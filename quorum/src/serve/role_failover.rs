//! Bounded provider failover for the daemon's restartable internal roles.
//!
//! A classifier or collector turn holds no lease, run capability, or provider
//! continuation, and its result is only accepted through the role's own
//! guarded store. Retrying one is a fresh turn, so these roles need none of the
//! authority rotation in `fallback.rs`. They do share its evidence: failed
//! routes become immutable `routing_attempts` rows scoped to the
//! responsibility, and `routing_attempts::select_alternate` picks the next
//! route. Each failover therefore adds an exclusion, which bounds automatic
//! retries by the size of the pool.
//!
//! This module never launches a provider. Callers launch outside any
//! transaction, after these calls have committed.

use super::runner::{FailureDisposition, RunnerFailure};
use quorum_core::error::{QuorumError, Result};
use quorum_core::role_assignments::{ModelProfile, RoleAssignment, ValidatedPool};
use quorum_core::routing_attempts::{self, AlternateRoute, RecordRoutingAttempt};
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::path::Path;

/// One executable choice for a responsibility: the assignment that owns it,
/// the exact pool generation, and the profile to run now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleRoute {
    pub assignment: RoleAssignment,
    pub pool: ValidatedPool,
    pub profile: ModelProfile,
}

impl RoleRoute {
    /// Whether evidence for this turn may link to the assignment. The evidence
    /// guard matches the assigned profile and canonical responsibility, so an
    /// alternate or re-keyed route is recorded without a link.
    pub fn links_assignment(&self, canonical_key: &str) -> bool {
        self.assignment.responsibility_key == canonical_key
            && self.profile == self.assignment.profile_snapshot()
    }
}

/// What a classified route failure leaves the caller to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteFailure {
    /// Not an availability failure; the role's existing retry policy applies.
    NotFailover,
    /// Run the turn on this eligible profile instead.
    Alternate(ModelProfile),
    /// No eligible profile remains. The owner has been alerted.
    Blocked,
}

/// Choose the route for the next turn. The assigned profile runs unless this
/// responsibility has evidence against it. With every route excluded the work
/// must still be able to recover from an outage, so `probe` rotates through the
/// pool: the caller's own backoff paces these turns.
pub fn resolve(
    conn: &Connection,
    assignment: RoleAssignment,
    pool: ValidatedPool,
    probe: u32,
) -> Result<RoleRoute> {
    let exclusions = routing_attempts::exclusions(conn, &assignment.responsibility_key)?;
    let assigned = assignment.profile_snapshot();
    let profile = if exclusions.excludes(&assigned) {
        match routing_attempts::select_alternate(&assignment, &pool, &exclusions)? {
            AlternateRoute::Selected(profile) => profile,
            AlternateRoute::Exhausted => probe_profile(&pool, probe),
            AlternateRoute::MismatchedGeneration => assigned,
        }
    } else {
        assigned
    };
    Ok(RoleRoute {
        assignment,
        pool,
        profile,
    })
}

fn probe_profile(pool: &ValidatedPool, probe: u32) -> ModelProfile {
    let mut profiles: Vec<_> = pool.profiles.iter().collect();
    profiles.sort_by(|left, right| {
        right
            .percent
            .cmp(&left.percent)
            .then_with(|| left.profile.id.cmp(&right.profile.id))
    });
    profiles[probe as usize % profiles.len()].profile.clone()
}

/// Record an availability failure and derive the next route. One evidence row
/// per profile is enough: a repeated failure of the same route adds nothing, and
/// an immutable replay would reject a changed disposition.
pub fn record_failure(
    conn: &mut Connection,
    route: &RoleRoute,
    failure: &RunnerFailure,
    subject: &str,
    now: i64,
) -> Result<RouteFailure> {
    if !matches!(
        failure.disposition(),
        FailureDisposition::ProviderUnavailable | FailureDisposition::ProfileUnavailable
    ) {
        return Ok(RouteFailure::NotFailover);
    }
    let assignment = &route.assignment;
    let key = assignment.responsibility_key.as_str();
    let tx = quorum_core::db::begin_immediate(conn)?;
    let attempted = routing_attempts::list(&tx, key)?
        .iter()
        .any(|attempt| attempt.profile.id == route.profile.id);
    if !attempted {
        routing_attempts::record_tx(
            &tx,
            &RecordRoutingAttempt {
                role_assignment_id: assignment.id,
                responsibility_key: key,
                profile: &route.profile,
                failure_disposition: Some(failure.disposition()),
                failed_agent_run_id: None,
                recorded_at: now,
            },
            &route.pool,
        )?;
    }
    let exclusions = routing_attempts::exclusions(&tx, key)?;
    let next = routing_attempts::select_alternate(assignment, &route.pool, &exclusions)?;
    tx.commit()?;
    match next {
        AlternateRoute::Selected(profile) => Ok(RouteFailure::Alternate(profile)),
        AlternateRoute::MismatchedGeneration => Ok(RouteFailure::NotFailover),
        AlternateRoute::Exhausted => {
            let (body, refs) = blocked_alert(route, failure, subject);
            quorum_core::feed::post_owner_alert_once(
                conn,
                "daemon",
                &body,
                &refs,
                quorum_core::feed::DEFAULT_MESSAGE_TTL_SECS,
                now,
            )?;
            Ok(RouteFailure::Blocked)
        }
    }
}

/// Alert text and its deduplication key. The key names the role's pool rather
/// than the blocked work, so an outage across many tasks raises one alert per
/// policy generation for each message TTL.
fn blocked_alert(route: &RoleRoute, failure: &RunnerFailure, subject: &str) -> (String, String) {
    let role = &route.assignment.role;
    let pool_key = &route.pool.pool_key;
    let provider = &route.profile.provider;
    let providers: BTreeSet<&str> = route
        .pool
        .profiles
        .iter()
        .map(|weighted| weighted.profile.provider.as_str())
        .collect();
    let evidence = format!("{}: {}", failure.disposition(), failure.detail());
    let (kind, body) = if providers.len() == 1 {
        (
            "single-provider",
            format!(
                "{role} blocked for {subject}: routing pool `{pool_key}` names only provider \
                 {provider}, which is unavailable ({evidence}). The work stays pending and \
                 retries with backoff. Add a second provider to [routing.{role}] and restart \
                 the daemon to route around this."
            ),
        )
    } else {
        (
            "all-providers",
            format!(
                "{role} blocked for {subject}: every provider in routing pool `{pool_key}` \
                 ({}) is unavailable; last failure on {provider} ({evidence}). The work stays \
                 pending and retries with backoff.",
                providers.into_iter().collect::<Vec<_>>().join(", ")
            ),
        )
    };
    let refs = format!(
        "role-blocked:{kind}:{pool_key}:{}",
        route.pool.policy_generation
    );
    (body, refs)
}

/// Settle a failed turn off the async runtime. Returns the profile to run next
/// when failover applies. Storage failures are logged and grant no failover, so
/// the caller falls back to its existing bounded retry.
pub async fn settle(
    db_path: &Path,
    route: &RoleRoute,
    failure: Option<&RunnerFailure>,
    subject: &str,
) -> RouteFailure {
    let Some(failure) = failure else {
        return RouteFailure::NotFailover;
    };
    let path = db_path.to_path_buf();
    let route = route.clone();
    let failure = failure.clone();
    let subject = subject.to_string();
    let role = route.assignment.role.clone();
    let settled = tokio::task::spawn_blocking(move || -> Result<RouteFailure> {
        let mut conn = quorum_core::db::open(&path)?;
        record_failure(&mut conn, &route, &failure, &subject, super::now_unix())
    })
    .await
    .map_err(|error| QuorumError::Io(format!("{role} failover join: {error}")));
    match settled.and_then(|settled| settled) {
        Ok(outcome) => outcome,
        Err(error) => {
            super::log(&format!("{role}: failover evidence not recorded: {error}"));
            if let Ok(conn) = quorum_core::db::open(db_path) {
                quorum_core::errlog::log_error(
                    &conn,
                    super::now_unix(),
                    &role,
                    &format!("failover evidence not recorded: {error}"),
                );
            }
            RouteFailure::NotFailover
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::role_assignments::{AssignmentRequest, WeightedProfile};

    fn profile(id: &str, provider: &str) -> ModelProfile {
        ModelProfile {
            id: id.into(),
            provider: provider.into(),
            runner: provider.into(),
            model: format!("{id}-model"),
            effort: "low".into(),
        }
    }

    fn pool(profiles: &[(&str, &str, u8)]) -> ValidatedPool {
        ValidatedPool {
            pool_key: "classifier".into(),
            policy_generation: "generation-1".into(),
            profiles: profiles
                .iter()
                .map(|(id, provider, percent)| WeightedProfile {
                    profile: profile(id, provider),
                    percent: *percent,
                })
                .collect(),
        }
    }

    fn failure(disposition: FailureDisposition) -> RunnerFailure {
        RunnerFailure::classified(disposition, "usage limit", std::io::ErrorKind::Other)
    }

    fn assigned(conn: &mut Connection, pool: &ValidatedPool, key: &str) -> RoleAssignment {
        quorum_core::role_assignments::assign_or_get_with_seed(
            conn,
            &AssignmentRequest {
                responsibility_key: key.into(),
                task_id: None,
                pr_number: None,
                role: "classifier".into(),
                review_stage: None,
                complexity: None,
            },
            pool,
            7,
            1,
        )
        .unwrap()
    }

    fn fixture() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = quorum_core::db::open(&dir.path().join("failover.db")).unwrap();
        (dir, conn)
    }

    fn owner_alerts(conn: &Connection) -> Vec<String> {
        let mut statement = conn
            .prepare("SELECT body FROM messages WHERE kind='alert' AND recipient='owner'")
            .unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap();
        rows.collect::<rusqlite::Result<Vec<_>>>().unwrap()
    }

    #[test]
    fn provider_unavailable_excludes_every_profile_of_that_provider() {
        let (_dir, mut conn) = fixture();
        let pool = pool(&[("a", "codex", 40), ("b", "codex", 40), ("c", "claude", 20)]);
        let assignment = assigned(&mut conn, &pool, "classifier:task:1:revision:1");
        let route = resolve(&conn, assignment, pool, 0).unwrap();

        let outcome = record_failure(
            &mut conn,
            &route,
            &failure(FailureDisposition::ProviderUnavailable),
            "task #1",
            2,
        )
        .unwrap();
        // Which profile the bag assigned does not matter when it is codex; a
        // claude assignment proves the same exclusion from the other side.
        let expected = if route.profile.provider == "codex" {
            RouteFailure::Alternate(profile("c", "claude"))
        } else {
            RouteFailure::Alternate(profile("a", "codex"))
        };
        assert_eq!(outcome, expected);

        let retried = resolve(&conn, route.assignment, route.pool, 0).unwrap();
        assert_ne!(retried.profile.provider, route.profile.provider);
        assert!(!retried.links_assignment("classifier:task:1:revision:1"));
    }

    #[test]
    fn profile_unavailable_keeps_other_profiles_of_the_provider() {
        let (_dir, mut conn) = fixture();
        let pool = pool(&[("a", "codex", 50), ("b", "codex", 50)]);
        let assignment = assigned(&mut conn, &pool, "classifier:task:2:revision:1");
        let route = resolve(&conn, assignment, pool, 0).unwrap();

        let outcome = record_failure(
            &mut conn,
            &route,
            &failure(FailureDisposition::ProfileUnavailable),
            "task #2",
            2,
        )
        .unwrap();
        let RouteFailure::Alternate(alternate) = outcome else {
            panic!("expected an alternate, got {outcome:?}");
        };
        assert_eq!(alternate.provider, "codex");
        assert_ne!(alternate.id, route.profile.id);
    }

    #[test]
    fn non_availability_failures_record_nothing_and_grant_no_failover() {
        let (_dir, mut conn) = fixture();
        let pool = pool(&[("a", "codex", 50), ("c", "claude", 50)]);
        let key = "classifier:task:3:revision:1";
        let assignment = assigned(&mut conn, &pool, key);
        let route = resolve(&conn, assignment, pool, 0).unwrap();

        for disposition in [
            FailureDisposition::RetryableSameRoute,
            FailureDisposition::NonFailover,
            FailureDisposition::Unclassified,
        ] {
            assert_eq!(
                record_failure(&mut conn, &route, &failure(disposition), "task #3", 2).unwrap(),
                RouteFailure::NotFailover
            );
        }
        assert!(routing_attempts::list(&conn, key).unwrap().is_empty());
    }

    #[test]
    fn single_provider_pool_blocks_with_one_alert_across_repeated_failures() {
        let (_dir, mut conn) = fixture();
        let pool = pool(&[("a", "codex", 100)]);
        let key = "classifier:task:4:revision:1";
        let assignment = assigned(&mut conn, &pool, key);
        let route = resolve(&conn, assignment, pool, 0).unwrap();

        for now in 2..6 {
            let outcome = record_failure(
                &mut conn,
                &route,
                &failure(FailureDisposition::ProviderUnavailable),
                "task #4",
                now,
            )
            .unwrap();
            assert_eq!(outcome, RouteFailure::Blocked);
        }
        assert_eq!(routing_attempts::list(&conn, key).unwrap().len(), 1);
        let alerts = owner_alerts(&conn);
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert!(
            alerts[0].contains("names only provider codex"),
            "{alerts:?}"
        );
        assert!(alerts[0].contains("[routing.classifier]"), "{alerts:?}");
    }

    #[test]
    fn exhausted_pool_alerts_once_per_role_and_probes_every_route() {
        let (_dir, mut conn) = fixture();
        let pool = pool(&[("a", "codex", 60), ("c", "claude", 40)]);
        for (task, now) in [(5, 2), (6, 3)] {
            let key = format!("classifier:task:{task}:revision:1");
            let assignment = assigned(&mut conn, &pool, &key);
            let first = resolve(&conn, assignment, pool.clone(), 0).unwrap();
            let unavailable = failure(FailureDisposition::ProviderUnavailable);
            let subject = format!("task #{task}");
            let RouteFailure::Alternate(_) =
                record_failure(&mut conn, &first, &unavailable, &subject, now).unwrap()
            else {
                panic!("the first failure must leave the second provider");
            };
            let second = resolve(&conn, first.assignment.clone(), first.pool.clone(), 0).unwrap();
            assert_ne!(second.profile, first.profile);
            assert_eq!(
                record_failure(&mut conn, &second, &unavailable, &subject, now).unwrap(),
                RouteFailure::Blocked
            );
            assert_eq!(routing_attempts::list(&conn, &key).unwrap().len(), 2);

            // A blocked responsibility keeps a paced way out of the outage
            // that reaches every provider, not only the assigned one.
            let probed: BTreeSet<String> = (0..2)
                .map(|probe| {
                    resolve(&conn, first.assignment.clone(), first.pool.clone(), probe)
                        .unwrap()
                        .profile
                        .id
                })
                .collect();
            assert_eq!(probed, BTreeSet::from(["a".into(), "c".into()]));
        }
        let alerts = owner_alerts(&conn);
        assert_eq!(alerts.len(), 1, "{alerts:?}");
        assert!(alerts[0].contains("every provider"), "{alerts:?}");
    }

    #[test]
    fn an_expired_alert_may_be_raised_again() {
        let (_dir, mut conn) = fixture();
        let pool = pool(&[("a", "codex", 100)]);
        let assignment = assigned(&mut conn, &pool, "classifier:task:7:revision:1");
        let route = resolve(&conn, assignment, pool, 0).unwrap();
        let unavailable = failure(FailureDisposition::ProviderUnavailable);

        record_failure(&mut conn, &route, &unavailable, "task #7", 2).unwrap();
        let later = 2 + quorum_core::feed::DEFAULT_MESSAGE_TTL_SECS;
        record_failure(&mut conn, &route, &unavailable, "task #7", later).unwrap();
        let live: i64 = conn
            .query_row(
                "SELECT count(*) FROM messages WHERE kind='alert' AND expires_at>?1",
                [later],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(live, 1);
    }

    #[test]
    fn links_assignment_requires_the_assigned_profile_and_canonical_key() {
        let (_dir, mut conn) = fixture();
        let pool = pool(&[("a", "codex", 50), ("c", "claude", 50)]);
        let key = "classifier:task:8:revision:1";
        let assignment = assigned(&mut conn, &pool, key);
        let route = resolve(&conn, assignment, pool, 0).unwrap();
        assert!(route.links_assignment(key));
        assert!(!route.links_assignment("classifier:task:8:revision:2"));
    }
}
