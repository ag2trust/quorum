//! Immutable responsibility-scoped provider-route attempt evidence.
//!
//! This module records evidence and derives read-only alternate-route choices.
//! It never launches routes or mutates role assignments, routing allocation, or
//! task lifecycle.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use crate::role_assignments::{AssignmentIdentity, ModelProfile, RoleAssignment, ValidatedPool};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeSet;

/// Closed, provider-neutral classification for runner failures observed before
/// a managed agent has produced an authoritative task or review outcome.
///
/// Only [`Self::ProviderUnavailable`] and [`Self::ProfileUnavailable`] derive
/// alternate-route exclusions. Every other variant is evidence without
/// fallback authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureDisposition {
    /// Authentication, account credit/quota, or a proved provider outage.
    ProviderUnavailable,
    /// The selected model/profile is unavailable; the provider may still work.
    ProfileUnavailable,
    /// A transport/startup interruption that may retry the exact same route.
    RetryableSameRoute,
    /// An execution or protocol boundary failure that must not trigger failover.
    NonFailover,
    /// Evidence is insufficient or internal; fail safe and grant no fallback.
    Unclassified,
}

impl FailureDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderUnavailable => "provider-unavailable",
            Self::ProfileUnavailable => "profile-unavailable",
            Self::RetryableSameRoute => "retryable-same-route",
            Self::NonFailover => "non-failover",
            Self::Unclassified => "unclassified",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "provider-unavailable" => Ok(Self::ProviderUnavailable),
            "profile-unavailable" => Ok(Self::ProfileUnavailable),
            "retryable-same-route" => Ok(Self::RetryableSameRoute),
            "non-failover" => Ok(Self::NonFailover),
            "unclassified" => Ok(Self::Unclassified),
            _ => Err(QuorumError::Io(
                "stored routing attempt has invalid failure disposition".into(),
            )),
        }
    }
}

impl std::fmt::Display for FailureDisposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingAttempt {
    pub id: i64,
    pub role_assignment_id: i64,
    pub responsibility_key: String,
    pub profile: ModelProfile,
    pub pool_key: String,
    pub policy_generation: String,
    /// `None` means the attempt has no classified pre-authoritative runner
    /// failure. Semantic outcomes and authoritative signals must use `None`.
    pub failure_disposition: Option<FailureDisposition>,
    pub recorded_at: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct RecordRoutingAttempt<'a> {
    pub role_assignment_id: i64,
    pub responsibility_key: &'a str,
    pub profile: &'a ModelProfile,
    pub failure_disposition: Option<FailureDisposition>,
    pub recorded_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordOutcome {
    Inserted(RoutingAttempt),
    Replayed(RoutingAttempt),
}

impl RecordOutcome {
    pub fn attempt(&self) -> &RoutingAttempt {
        match self {
            Self::Inserted(attempt) | Self::Replayed(attempt) => attempt,
        }
    }
}

/// Exclusions derived exclusively from immutable classified failure evidence.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteExclusions {
    providers: BTreeSet<String>,
    profiles: BTreeSet<String>,
}

/// Read-only result of selecting an alternate route from an immutable pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlternateRoute {
    Selected(ModelProfile),
    Exhausted,
    MismatchedGeneration,
}

/// All immutable evidence needed to validate one alternate-route attribution.
///
/// The caller supplies the exact role assignment, one persisted classified
/// routing attempt, and the identity of the run it intends to attribute. This
/// primitive only validates them; it never writes a run or capability.
#[derive(Debug, Clone, Copy)]
pub struct FallbackAttributionInput<'a> {
    pub assignment: &'a RoleAssignment,
    pub identity: AssignmentIdentity<'a>,
    pub eligible_pool: &'a ValidatedPool,
    pub attempt: &'a RoutingAttempt,
    pub exclusions: &'a RouteExclusions,
    pub selected_profile: &'a ModelProfile,
}

/// Opaque, bounded authority to attribute one validated fallback route.
///
/// It can only be constructed by [`validate_fallback_attribution`]. A future
/// run writer can use its accessors without accepting arbitrary profile or
/// lifecycle tuples from a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedFallbackAttribution {
    assignment_id: i64,
    task_id: Option<i64>,
    responsibility_key: String,
    role: String,
    pr_number: Option<i64>,
    review_stage: Option<String>,
    profile: ModelProfile,
}

impl ValidatedFallbackAttribution {
    pub fn assignment_id(&self) -> i64 {
        self.assignment_id
    }

    pub fn task_id(&self) -> Option<i64> {
        self.task_id
    }

    pub fn responsibility_key(&self) -> &str {
        &self.responsibility_key
    }

    pub fn role(&self) -> &str {
        &self.role
    }

    pub fn pr_number(&self) -> Option<i64> {
        self.pr_number
    }

    pub fn review_stage(&self) -> Option<&str> {
        self.review_stage.as_deref()
    }

    pub fn profile(&self) -> &ModelProfile {
        &self.profile
    }
}

impl RouteExclusions {
    pub fn excluded_providers(&self) -> &BTreeSet<String> {
        &self.providers
    }

    pub fn excluded_profiles(&self) -> &BTreeSet<String> {
        &self.profiles
    }

    pub fn excludes(&self, profile: &ModelProfile) -> bool {
        self.providers.contains(&profile.provider) || self.profiles.contains(&profile.id)
    }
}

/// Record one distinct configured route for an existing responsibility.
///
/// The supplied pool must be the exact pool generation that created the role
/// assignment, and both the assigned profile and attempted profile must remain
/// exact members. A replay of identical semantic evidence returns the original
/// row (including its original timestamp); conflicting evidence for the same
/// route fails closed instead of mutating history.
pub fn record(
    conn: &mut Connection,
    input: &RecordRoutingAttempt<'_>,
    eligible_pool: &ValidatedPool,
) -> Result<RecordOutcome> {
    eligible_pool.validate()?;
    validate_text("responsibility key", input.responsibility_key)?;
    if input.role_assignment_id <= 0 || input.recorded_at < 0 {
        return usage("routing attempt assignment must be positive and timestamp non-negative");
    }

    let tx = begin_immediate(conn)?;
    let assignment = crate::role_assignments::get(&tx, input.role_assignment_id)?
        .ok_or_else(|| QuorumError::Io("routing attempt role assignment is missing".into()))?;
    validate_assignment(&assignment, input, eligible_pool)?;

    if let Some(existing) = get_by_route(&tx, input.role_assignment_id, &input.profile.id)? {
        ensure_replay_matches(&existing, input, eligible_pool)?;
        tx.commit().map_err(map_sql_err)?;
        return Ok(RecordOutcome::Replayed(existing));
    }

    let distinct: i64 = tx.query_row(
        "SELECT count(*) FROM routing_attempts WHERE role_assignment_id=?1",
        [input.role_assignment_id],
        |row| row.get(0),
    )?;
    if distinct >= eligible_pool.profiles.len() as i64 {
        return Err(QuorumError::Io(
            "routing attempts exceed configured eligible routes".into(),
        ));
    }

    tx.execute(
        "INSERT INTO routing_attempts(
             role_assignment_id,responsibility_key,profile_id,provider,runner,model,effort,
             pool_key,policy_generation,failure_disposition,recorded_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            input.role_assignment_id,
            input.responsibility_key,
            input.profile.id,
            input.profile.provider,
            input.profile.runner,
            input.profile.model,
            input.profile.effort,
            eligible_pool.pool_key,
            eligible_pool.policy_generation,
            input.failure_disposition.map(FailureDisposition::as_str),
            input.recorded_at,
        ],
    )?;
    let inserted = get_by_route(&tx, input.role_assignment_id, &input.profile.id)?
        .ok_or_else(|| QuorumError::Io("recorded routing attempt is missing".into()))?;
    tx.commit().map_err(map_sql_err)?;
    Ok(RecordOutcome::Inserted(inserted))
}

/// Read immutable attempts for a responsibility in insertion order.
pub fn list(conn: &Connection, responsibility_key: &str) -> Result<Vec<RoutingAttempt>> {
    validate_text("responsibility key", responsibility_key)?;
    let mut statement = conn.prepare(
        "SELECT id,role_assignment_id,responsibility_key,profile_id,provider,runner,model,
                effort,pool_key,policy_generation,failure_disposition,recorded_at
         FROM routing_attempts WHERE responsibility_key=?1 ORDER BY id",
    )?;
    let attempts = statement
        .query_map([responsibility_key], row_to_attempt)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(QuorumError::from)?;
    Ok(attempts)
}

/// Derive the bounded exclusion set for exactly one responsibility.
pub fn exclusions(conn: &Connection, responsibility_key: &str) -> Result<RouteExclusions> {
    let attempts = list(conn, responsibility_key)?;
    let mut result = RouteExclusions::default();
    for attempt in attempts {
        match attempt.failure_disposition {
            Some(FailureDisposition::ProviderUnavailable) => {
                result.providers.insert(attempt.profile.provider);
            }
            Some(FailureDisposition::ProfileUnavailable) => {
                result.profiles.insert(attempt.profile.id);
            }
            Some(
                FailureDisposition::RetryableSameRoute
                | FailureDisposition::NonFailover
                | FailureDisposition::Unclassified,
            )
            | None => {}
        }
    }
    Ok(result)
}

/// Select the highest-priority executable profile from an assignment's exact pool generation.
///
/// This pure primitive never records an attempt, advances the weighted allocation cursor, or
/// mutates the assignment. Exclusions are the caller's persisted, responsibility-scoped
/// evidence from [`exclusions`]. All positive-weight profiles remain eligible candidates;
/// percentage only orders them, descending with profile ID as the stable tie-breaker.
pub fn select_alternate(
    assignment: &RoleAssignment,
    eligible_pool: &ValidatedPool,
    excluded: &RouteExclusions,
) -> Result<AlternateRoute> {
    eligible_pool.validate()?;
    if !assignment.matches_pool_generation(eligible_pool) {
        return Ok(AlternateRoute::MismatchedGeneration);
    }
    if !pool_contains_exact(eligible_pool, &assignment.profile_snapshot()) {
        return Err(QuorumError::Io(
            "routing pool does not contain the assigned profile snapshot".into(),
        ));
    }

    let mut candidates: Vec<_> = eligible_pool
        .profiles
        .iter()
        .filter(|weighted| !excluded.excludes(&weighted.profile))
        .collect();
    candidates.sort_by(|left, right| {
        right
            .percent
            .cmp(&left.percent)
            .then_with(|| left.profile.id.cmp(&right.profile.id))
    });
    Ok(candidates
        .first()
        .map(|weighted| AlternateRoute::Selected(weighted.profile.clone()))
        .unwrap_or(AlternateRoute::Exhausted))
}

/// Validate one deterministically selected fallback route for later durable
/// attribution.
///
/// `None` is the clean, fail-closed outcome for stale, cross-responsibility,
/// excluded, non-selected, or otherwise insufficient immutable evidence. An
/// invalid pool or identity is bad input and returns [`QuorumError::Usage`].
/// The returned token is bounded by the validated pool and assignment snapshot;
/// this function has no database access and mutates no assignment, cursor, run,
/// or capability state.
pub fn validate_fallback_attribution(
    input: &FallbackAttributionInput<'_>,
) -> Result<Option<ValidatedFallbackAttribution>> {
    input.eligible_pool.validate()?;
    input.identity.validate()?;

    let assignment = input.assignment;
    let attempt = input.attempt;
    if assignment.id <= 0
        || !assignment.matches_identity(&input.identity)
        || !assignment.matches_pool_generation(input.eligible_pool)
        || !pool_contains_exact(input.eligible_pool, &assignment.profile_snapshot())
        || attempt.id <= 0
        || attempt.role_assignment_id != assignment.id
        || attempt.responsibility_key != assignment.responsibility_key
        || attempt.pool_key != assignment.pool_key
        || attempt.policy_generation != assignment.policy_generation
        || !pool_contains_exact(input.eligible_pool, &attempt.profile)
        || !matches!(
            attempt.failure_disposition,
            Some(FailureDisposition::ProviderUnavailable | FailureDisposition::ProfileUnavailable)
        )
        || !input.exclusions.excludes(&attempt.profile)
        || input.exclusions.excludes(input.selected_profile)
    {
        return Ok(None);
    }

    match select_alternate(assignment, input.eligible_pool, input.exclusions)? {
        AlternateRoute::Selected(profile) if profile == *input.selected_profile => {
            Ok(Some(ValidatedFallbackAttribution {
                assignment_id: assignment.id,
                task_id: assignment.task_id,
                responsibility_key: assignment.responsibility_key.clone(),
                role: assignment.role.clone(),
                pr_number: assignment.pr_number,
                review_stage: assignment.review_stage.clone(),
                profile,
            }))
        }
        AlternateRoute::Selected(_)
        | AlternateRoute::Exhausted
        | AlternateRoute::MismatchedGeneration => Ok(None),
    }
}

fn validate_assignment(
    assignment: &RoleAssignment,
    input: &RecordRoutingAttempt<'_>,
    pool: &ValidatedPool,
) -> Result<()> {
    if assignment.responsibility_key != input.responsibility_key
        || assignment.pool_key != pool.pool_key
        || assignment.policy_generation != pool.policy_generation
    {
        return Err(QuorumError::Io(
            "routing attempt does not match role assignment".into(),
        ));
    }

    let assigned = assignment.profile_snapshot();
    if !pool_contains_exact(pool, &assigned) {
        return Err(QuorumError::Io(
            "routing pool does not contain the assigned profile snapshot".into(),
        ));
    }
    if !pool_contains_exact(pool, input.profile) {
        return usage("routing attempt is not a configured eligible route");
    }
    Ok(())
}

fn pool_contains_exact(pool: &ValidatedPool, profile: &ModelProfile) -> bool {
    pool.profiles
        .iter()
        .any(|eligible| eligible.profile == *profile)
}

fn ensure_replay_matches(
    existing: &RoutingAttempt,
    input: &RecordRoutingAttempt<'_>,
    pool: &ValidatedPool,
) -> Result<()> {
    if existing.responsibility_key != input.responsibility_key
        || existing.profile != *input.profile
        || existing.pool_key != pool.pool_key
        || existing.policy_generation != pool.policy_generation
        || existing.failure_disposition != input.failure_disposition
    {
        return Err(QuorumError::Io(
            "routing attempt replay conflicts with immutable evidence".into(),
        ));
    }
    Ok(())
}

fn get_by_route(
    conn: &Connection,
    assignment_id: i64,
    profile_id: &str,
) -> Result<Option<RoutingAttempt>> {
    conn.query_row(
        "SELECT id,role_assignment_id,responsibility_key,profile_id,provider,runner,model,
                effort,pool_key,policy_generation,failure_disposition,recorded_at
         FROM routing_attempts WHERE role_assignment_id=?1 AND profile_id=?2",
        params![assignment_id, profile_id],
        row_to_attempt,
    )
    .optional()
    .map_err(Into::into)
}

fn row_to_attempt(row: &rusqlite::Row<'_>) -> rusqlite::Result<RoutingAttempt> {
    let disposition: Option<String> = row.get(10)?;
    let failure_disposition = disposition
        .as_deref()
        .map(FailureDisposition::parse)
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                10,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    Ok(RoutingAttempt {
        id: row.get(0)?,
        role_assignment_id: row.get(1)?,
        responsibility_key: row.get(2)?,
        profile: ModelProfile {
            id: row.get(3)?,
            provider: row.get(4)?,
            runner: row.get(5)?,
            model: row.get(6)?,
            effort: row.get(7)?,
        },
        pool_key: row.get(8)?,
        policy_generation: row.get(9)?,
        failure_disposition,
        recorded_at: row.get(11)?,
    })
}

fn validate_text(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 1024 || value.contains('\0') {
        return usage(&format!("invalid {label}"));
    }
    Ok(())
}

fn usage<T>(message: &str) -> Result<T> {
    Err(QuorumError::Usage(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn pool() -> ValidatedPool {
        ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: "generation-1".into(),
            profiles: vec![
                crate::role_assignments::WeightedProfile {
                    profile: ModelProfile {
                        id: "opus".into(),
                        provider: "claude".into(),
                        runner: "claude".into(),
                        model: "claude-opus-4-8".into(),
                        effort: "high".into(),
                    },
                    percent: 34,
                },
                crate::role_assignments::WeightedProfile {
                    profile: ModelProfile {
                        id: "sonnet".into(),
                        provider: "claude".into(),
                        runner: "claude".into(),
                        model: "claude-sonnet-4-6".into(),
                        effort: "medium".into(),
                    },
                    percent: 33,
                },
                crate::role_assignments::WeightedProfile {
                    profile: ModelProfile {
                        id: "sol".into(),
                        provider: "codex".into(),
                        runner: "codex".into(),
                        model: "gpt-5.6-sol".into(),
                        effort: "high".into(),
                    },
                    percent: 33,
                },
            ],
        }
    }

    fn two_profile_pool(first_percent: u8, first_id: &str, second_id: &str) -> ValidatedPool {
        ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: "generation-1".into(),
            profiles: vec![
                crate::role_assignments::WeightedProfile {
                    profile: ModelProfile {
                        id: first_id.into(),
                        provider: "claude".into(),
                        runner: "claude".into(),
                        model: format!("{first_id}-model"),
                        effort: "high".into(),
                    },
                    percent: first_percent,
                },
                crate::role_assignments::WeightedProfile {
                    profile: ModelProfile {
                        id: second_id.into(),
                        provider: "codex".into(),
                        runner: "codex".into(),
                        model: format!("{second_id}-model"),
                        effort: "high".into(),
                    },
                    percent: 100 - first_percent,
                },
            ],
        }
    }

    fn insert_assignment(conn: &Connection, id: i64, responsibility: &str, task_id: i64) {
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (?1,'routing fixture','working','test',1,1)",
            [task_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,role,complexity,profile_id,provider,runner,
                 model,effort,pool_key,policy_generation,created_at)
             VALUES (?1,?2,?3,'worker','M','opus','claude','claude',
                     'claude-opus-4-8','high','worker.M','generation-1',1)",
            params![id, responsibility, task_id],
        )
        .unwrap();
    }

    fn insert_assignment_for_pool(
        conn: &Connection,
        id: i64,
        responsibility: &str,
        task_id: i64,
        pool: &ValidatedPool,
        profile_index: usize,
    ) {
        let profile = &pool.profiles[profile_index].profile;
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (?1,'routing fixture','working','test',1,1)",
            [task_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,role,complexity,profile_id,provider,runner,
                 model,effort,pool_key,policy_generation,created_at)
             VALUES (?1,?2,?3,'worker','M',?4,?5,?6,?7,?8,?9,?10,1)",
            params![
                id,
                responsibility,
                task_id,
                profile.id,
                profile.provider,
                profile.runner,
                profile.model,
                profile.effort,
                pool.pool_key,
                pool.policy_generation,
            ],
        )
        .unwrap();
    }

    fn record_profile(
        conn: &mut Connection,
        assignment_id: i64,
        responsibility: &str,
        pool: &ValidatedPool,
        profile_index: usize,
        disposition: Option<FailureDisposition>,
        recorded_at: i64,
    ) -> Result<RecordOutcome> {
        record(
            conn,
            &RecordRoutingAttempt {
                role_assignment_id: assignment_id,
                responsibility_key: responsibility,
                profile: &pool.profiles[profile_index].profile,
                failure_disposition: disposition,
                recorded_at,
            },
            pool,
        )
    }

    #[test]
    fn provider_wide_and_profile_only_failures_derive_exact_exclusions() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("routes.db")).unwrap();
        let pool = pool();
        insert_assignment(&conn, 1, "worker:task:1", 1);
        insert_assignment(&conn, 2, "worker:task:2", 2);

        record_profile(
            &mut conn,
            1,
            "worker:task:1",
            &pool,
            0,
            Some(FailureDisposition::ProviderUnavailable),
            10,
        )
        .unwrap();
        let provider = exclusions(&conn, "worker:task:1").unwrap();
        assert_eq!(
            provider.excluded_providers(),
            &BTreeSet::from(["claude".to_string()])
        );
        assert!(provider.excluded_profiles().is_empty());
        assert!(provider.excludes(&pool.profiles[0].profile));
        assert!(provider.excludes(&pool.profiles[1].profile));
        assert!(!provider.excludes(&pool.profiles[2].profile));

        record_profile(
            &mut conn,
            2,
            "worker:task:2",
            &pool,
            0,
            Some(FailureDisposition::ProfileUnavailable),
            11,
        )
        .unwrap();
        let profile = exclusions(&conn, "worker:task:2").unwrap();
        assert!(profile.excluded_providers().is_empty());
        assert_eq!(
            profile.excluded_profiles(),
            &BTreeSet::from(["opus".to_string()])
        );
        assert!(profile.excludes(&pool.profiles[0].profile));
        assert!(!profile.excludes(&pool.profiles[1].profile));
        assert!(!profile.excludes(&pool.profiles[2].profile));
    }

    #[test]
    fn alternate_selection_prioritizes_every_positive_weight_without_allocation_mutation() {
        for (first_percent, expected_initial) in [(99, "primary"), (1, "alternate")] {
            let dir = tempfile::tempdir().unwrap();
            let mut conn = crate::db::open(&dir.path().join("selection.db")).unwrap();
            let pool = two_profile_pool(first_percent, "primary", "alternate");
            insert_assignment_for_pool(&conn, 1, "worker:task:1", 1, &pool, 0);
            let assignment = crate::role_assignments::get(&conn, 1).unwrap().unwrap();
            let cursor_count: i64 = conn
                .query_row("SELECT count(*) FROM routing_cursors", [], |row| row.get(0))
                .unwrap();

            assert_eq!(
                select_alternate(&assignment, &pool, &RouteExclusions::default()).unwrap(),
                AlternateRoute::Selected(
                    pool.profiles
                        .iter()
                        .find(|entry| entry.profile.id == expected_initial)
                        .unwrap()
                        .profile
                        .clone()
                )
            );

            record_profile(
                &mut conn,
                1,
                "worker:task:1",
                &pool,
                0,
                Some(FailureDisposition::ProfileUnavailable),
                10,
            )
            .unwrap();
            assert_eq!(
                select_alternate(
                    &assignment,
                    &pool,
                    &exclusions(&conn, "worker:task:1").unwrap()
                )
                .unwrap(),
                AlternateRoute::Selected(pool.profiles[1].profile.clone()),
                "a positive one-percent profile remains a fallback candidate"
            );
            assert_eq!(
                crate::role_assignments::get(&conn, 1).unwrap(),
                Some(assignment)
            );
            assert_eq!(
                conn.query_row("SELECT count(*) FROM routing_cursors", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                cursor_count
            );
        }
    }

    #[test]
    fn fallback_attribution_requires_exact_selected_immutable_evidence_without_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("attribution.db")).unwrap();
        let pool = pool();
        insert_assignment_for_pool(&conn, 1, "worker:task:1", 1, &pool, 0);
        let assignment = crate::role_assignments::get(&conn, 1).unwrap().unwrap();
        let attempt = record_profile(
            &mut conn,
            assignment.id,
            &assignment.responsibility_key,
            &pool,
            0,
            Some(FailureDisposition::ProfileUnavailable),
            10,
        )
        .unwrap()
        .attempt()
        .clone();
        let excluded = exclusions(&conn, &assignment.responsibility_key).unwrap();
        let selected = match select_alternate(&assignment, &pool, &excluded).unwrap() {
            AlternateRoute::Selected(profile) => profile,
            other => panic!("expected selected alternate, got {other:?}"),
        };
        let before_assignment = crate::role_assignments::get(&conn, assignment.id).unwrap();
        let before_cursor: i64 = conn
            .query_row("SELECT count(*) FROM routing_cursors", [], |row| row.get(0))
            .unwrap();
        let before_runs: i64 = conn
            .query_row("SELECT count(*) FROM agent_runs", [], |row| row.get(0))
            .unwrap();
        let before_capabilities: i64 = conn
            .query_row("SELECT count(*) FROM run_capabilities", [], |row| {
                row.get(0)
            })
            .unwrap();

        let token = validate_fallback_attribution(&FallbackAttributionInput {
            assignment: &assignment,
            identity: AssignmentIdentity {
                task_id: Some(1),
                responsibility_key: "worker:task:1",
                role: "worker",
                pr_number: None,
                review_stage: None,
            },
            eligible_pool: &pool,
            attempt: &attempt,
            exclusions: &excluded,
            selected_profile: &selected,
        })
        .unwrap()
        .expect("exact fallback evidence must issue a token");
        assert_eq!(token.assignment_id(), assignment.id);
        assert_eq!(token.task_id(), Some(1));
        assert_eq!(token.responsibility_key(), "worker:task:1");
        assert_eq!(token.role(), "worker");
        assert_eq!(token.pr_number(), None);
        assert_eq!(token.review_stage(), None);
        assert_eq!(token.profile(), &selected);
        assert_eq!(
            crate::role_assignments::get(&conn, assignment.id).unwrap(),
            before_assignment
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM routing_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            before_cursor
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM agent_runs", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            before_runs
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM run_capabilities", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            before_capabilities
        );
    }

    #[test]
    fn fallback_attribution_fails_closed_for_non_members_stale_or_cross_identity_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("attribution-negative.db")).unwrap();
        let pool = pool();
        insert_assignment_for_pool(&conn, 1, "worker:task:1", 1, &pool, 0);
        let assignment = crate::role_assignments::get(&conn, 1).unwrap().unwrap();
        let attempt = record_profile(
            &mut conn,
            1,
            "worker:task:1",
            &pool,
            0,
            Some(FailureDisposition::ProfileUnavailable),
            10,
        )
        .unwrap()
        .attempt()
        .clone();
        let excluded = exclusions(&conn, "worker:task:1").unwrap();
        let selected = match select_alternate(&assignment, &pool, &excluded).unwrap() {
            AlternateRoute::Selected(profile) => profile,
            other => panic!("expected selected alternate, got {other:?}"),
        };
        let base = FallbackAttributionInput {
            assignment: &assignment,
            identity: AssignmentIdentity {
                task_id: Some(1),
                responsibility_key: "worker:task:1",
                role: "worker",
                pr_number: None,
                review_stage: None,
            },
            eligible_pool: &pool,
            attempt: &attempt,
            exclusions: &excluded,
            selected_profile: &selected,
        };
        assert!(validate_fallback_attribution(&base).unwrap().is_some());

        let non_member = ModelProfile {
            id: "arbitrary".into(),
            provider: "codex".into(),
            runner: "codex".into(),
            model: "arbitrary-model".into(),
            effort: "high".into(),
        };
        assert!(validate_fallback_attribution(&FallbackAttributionInput {
            selected_profile: &non_member,
            ..base
        })
        .unwrap()
        .is_none());

        assert!(validate_fallback_attribution(&FallbackAttributionInput {
            selected_profile: &attempt.profile,
            ..base
        })
        .unwrap()
        .is_none());

        let not_selected = pool
            .profiles
            .iter()
            .map(|weighted| &weighted.profile)
            .find(|profile| **profile != selected && !excluded.excludes(profile))
            .unwrap();
        assert!(validate_fallback_attribution(&FallbackAttributionInput {
            selected_profile: not_selected,
            ..base
        })
        .unwrap()
        .is_none());

        let stale = ValidatedPool {
            policy_generation: "generation-2".into(),
            ..pool.clone()
        };
        assert!(validate_fallback_attribution(&FallbackAttributionInput {
            eligible_pool: &stale,
            ..base
        })
        .unwrap()
        .is_none());

        for identity in [
            AssignmentIdentity {
                task_id: Some(2),
                ..base.identity
            },
            AssignmentIdentity {
                responsibility_key: "worker:task:2",
                ..base.identity
            },
            AssignmentIdentity {
                role: "planner",
                ..base.identity
            },
            AssignmentIdentity {
                pr_number: Some(7),
                ..base.identity
            },
        ] {
            assert!(
                validate_fallback_attribution(&FallbackAttributionInput { identity, ..base })
                    .unwrap()
                    .is_none()
            );
        }

        let mut cross_responsibility = attempt.clone();
        cross_responsibility.responsibility_key = "worker:task:2".into();
        assert!(validate_fallback_attribution(&FallbackAttributionInput {
            attempt: &cross_responsibility,
            ..base
        })
        .unwrap()
        .is_none());

        let zero_weight = ValidatedPool {
            profiles: vec![
                crate::role_assignments::WeightedProfile {
                    percent: 100,
                    ..pool.profiles[0].clone()
                },
                crate::role_assignments::WeightedProfile {
                    percent: 0,
                    ..pool.profiles[1].clone()
                },
            ],
            ..pool.clone()
        };
        assert!(matches!(
            validate_fallback_attribution(&FallbackAttributionInput {
                eligible_pool: &zero_weight,
                ..base
            }),
            Err(QuorumError::Usage(_))
        ));
    }

    #[test]
    fn fallback_attribution_preserves_reviewer_pr_and_stage_identity() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("reviewer-attribution.db")).unwrap();
        let pool = ValidatedPool {
            pool_key: "reviewer.M.r1".into(),
            ..pool()
        };
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (1,'routing fixture','working','test',1,1)",
            [],
        )
        .unwrap();
        let profile = &pool.profiles[0].profile;
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,pr_number,role,review_stage,complexity,
                 profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
             VALUES (1,'reviewer:task:1:pr:9:r1',1,9,'reviewer','r1','M',
                     ?1,?2,?3,?4,?5,?6,?7,1)",
            params![
                profile.id,
                profile.provider,
                profile.runner,
                profile.model,
                profile.effort,
                pool.pool_key,
                pool.policy_generation,
            ],
        )
        .unwrap();
        let assignment = crate::role_assignments::get(&conn, 1).unwrap().unwrap();
        let attempt = record_profile(
            &mut conn,
            1,
            &assignment.responsibility_key,
            &pool,
            0,
            Some(FailureDisposition::ProfileUnavailable),
            10,
        )
        .unwrap()
        .attempt()
        .clone();
        let excluded = exclusions(&conn, &assignment.responsibility_key).unwrap();
        let selected = match select_alternate(&assignment, &pool, &excluded).unwrap() {
            AlternateRoute::Selected(profile) => profile,
            other => panic!("expected selected alternate, got {other:?}"),
        };
        let input = FallbackAttributionInput {
            assignment: &assignment,
            identity: AssignmentIdentity {
                task_id: Some(1),
                responsibility_key: &assignment.responsibility_key,
                role: "reviewer",
                pr_number: Some(9),
                review_stage: Some("r1"),
            },
            eligible_pool: &pool,
            attempt: &attempt,
            exclusions: &excluded,
            selected_profile: &selected,
        };
        assert!(validate_fallback_attribution(&input).unwrap().is_some());
        assert!(validate_fallback_attribution(&FallbackAttributionInput {
            identity: AssignmentIdentity {
                review_stage: Some("r2"),
                ..input.identity
            },
            ..input
        })
        .unwrap()
        .is_none());
        assert!(validate_fallback_attribution(&FallbackAttributionInput {
            identity: AssignmentIdentity {
                pr_number: Some(10),
                ..input.identity
            },
            ..input
        })
        .unwrap()
        .is_none());
    }

    #[test]
    fn alternate_selection_uses_stable_profile_id_ties_and_exhaustion() {
        let pool = two_profile_pool(50, "zeta", "alpha");
        let assignment = RoleAssignment {
            id: 1,
            responsibility_key: "worker:task:1".into(),
            task_id: Some(1),
            pr_number: None,
            role: "worker".into(),
            review_stage: None,
            complexity: Some("M".into()),
            profile_id: "zeta".into(),
            provider: "claude".into(),
            runner: "claude".into(),
            model: "zeta-model".into(),
            effort: "high".into(),
            pool_key: pool.pool_key.clone(),
            policy_generation: pool.policy_generation.clone(),
            created_at: 1,
        };
        assert_eq!(
            select_alternate(&assignment, &pool, &RouteExclusions::default()).unwrap(),
            AlternateRoute::Selected(pool.profiles[1].profile.clone())
        );

        let excluded = RouteExclusions {
            providers: BTreeSet::from(["claude".into(), "codex".into()]),
            profiles: BTreeSet::new(),
        };
        assert_eq!(
            select_alternate(&assignment, &pool, &excluded).unwrap(),
            AlternateRoute::Exhausted
        );
    }

    #[test]
    fn alternate_selection_applies_provider_and_profile_exclusions_and_fails_closed() {
        let pool = ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: "generation-1".into(),
            profiles: vec![
                crate::role_assignments::WeightedProfile {
                    profile: ModelProfile {
                        id: "opus".into(),
                        provider: "claude".into(),
                        runner: "claude".into(),
                        model: "opus-model".into(),
                        effort: "high".into(),
                    },
                    percent: 50,
                },
                crate::role_assignments::WeightedProfile {
                    profile: ModelProfile {
                        id: "sonnet".into(),
                        provider: "claude".into(),
                        runner: "claude".into(),
                        model: "sonnet-model".into(),
                        effort: "high".into(),
                    },
                    percent: 30,
                },
                crate::role_assignments::WeightedProfile {
                    profile: ModelProfile {
                        id: "sol".into(),
                        provider: "codex".into(),
                        runner: "codex".into(),
                        model: "sol-model".into(),
                        effort: "high".into(),
                    },
                    percent: 20,
                },
            ],
        };
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("exclusions.db")).unwrap();
        insert_assignment_for_pool(&conn, 1, "worker:provider", 1, &pool, 0);
        insert_assignment_for_pool(&conn, 2, "worker:profile", 2, &pool, 0);
        let provider_assignment = crate::role_assignments::get(&conn, 1).unwrap().unwrap();
        let profile_assignment = crate::role_assignments::get(&conn, 2).unwrap().unwrap();

        record_profile(
            &mut conn,
            1,
            "worker:provider",
            &pool,
            0,
            Some(FailureDisposition::ProviderUnavailable),
            10,
        )
        .unwrap();
        record_profile(
            &mut conn,
            2,
            "worker:profile",
            &pool,
            0,
            Some(FailureDisposition::ProfileUnavailable),
            11,
        )
        .unwrap();
        assert_eq!(
            select_alternate(
                &provider_assignment,
                &pool,
                &exclusions(&conn, "worker:provider").unwrap()
            )
            .unwrap(),
            AlternateRoute::Selected(pool.profiles[2].profile.clone())
        );
        assert_eq!(
            select_alternate(
                &profile_assignment,
                &pool,
                &exclusions(&conn, "worker:profile").unwrap()
            )
            .unwrap(),
            AlternateRoute::Selected(pool.profiles[1].profile.clone())
        );

        let mismatched = ValidatedPool {
            policy_generation: "generation-2".into(),
            ..pool.clone()
        };
        assert_eq!(
            select_alternate(
                &provider_assignment,
                &mismatched,
                &RouteExclusions::default()
            )
            .unwrap(),
            AlternateRoute::MismatchedGeneration
        );

        let invalid = ValidatedPool {
            profiles: vec![
                crate::role_assignments::WeightedProfile {
                    percent: 100,
                    ..pool.profiles[0].clone()
                },
                crate::role_assignments::WeightedProfile {
                    percent: 0,
                    ..pool.profiles[1].clone()
                },
            ],
            ..pool.clone()
        };
        assert!(matches!(
            select_alternate(&provider_assignment, &invalid, &RouteExclusions::default()),
            Err(QuorumError::Usage(_))
        ));
    }

    #[test]
    fn replay_restart_and_distinct_route_bound_preserve_original_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restart.db");
        let pool = pool();
        {
            let mut conn = crate::db::open(&path).unwrap();
            insert_assignment(&conn, 7, "worker:task:7", 7);
            let original = crate::role_assignments::get(&conn, 7).unwrap().unwrap();

            let inserted = record_profile(
                &mut conn,
                7,
                "worker:task:7",
                &pool,
                0,
                Some(FailureDisposition::ProviderUnavailable),
                20,
            )
            .unwrap();
            assert!(matches!(inserted, RecordOutcome::Inserted(_)));
            let replay = record_profile(
                &mut conn,
                7,
                "worker:task:7",
                &pool,
                0,
                Some(FailureDisposition::ProviderUnavailable),
                999,
            )
            .unwrap();
            assert!(matches!(replay, RecordOutcome::Replayed(_)));
            assert_eq!(replay.attempt().recorded_at, 20);
            assert_eq!(
                crate::role_assignments::get(&conn, 7).unwrap(),
                Some(original)
            );
        }

        let mut restarted = crate::db::open(&path).unwrap();
        assert_eq!(list(&restarted, "worker:task:7").unwrap().len(), 1);
        assert!(exclusions(&restarted, "worker:task:7")
            .unwrap()
            .excludes(&pool.profiles[1].profile));
        for index in 1..pool.profiles.len() {
            record_profile(
                &mut restarted,
                7,
                "worker:task:7",
                &pool,
                index,
                Some(FailureDisposition::ProfileUnavailable),
                20 + index as i64,
            )
            .unwrap();
        }
        let unconfigured = ModelProfile {
            id: "other".into(),
            provider: "codex".into(),
            runner: "codex".into(),
            model: "gpt-other".into(),
            effort: "high".into(),
        };
        let rejected = record(
            &mut restarted,
            &RecordRoutingAttempt {
                role_assignment_id: 7,
                responsibility_key: "worker:task:7",
                profile: &unconfigured,
                failure_disposition: Some(FailureDisposition::ProfileUnavailable),
                recorded_at: 30,
            },
            &pool,
        );
        assert!(matches!(rejected, Err(QuorumError::Usage(_))));
        assert_eq!(list(&restarted, "worker:task:7").unwrap().len(), 3);

        assert!(restarted
            .execute(
                "UPDATE routing_attempts SET failure_disposition='unclassified' WHERE id=1",
                [],
            )
            .is_err());
        assert!(restarted
            .execute("DELETE FROM routing_attempts WHERE id=1", [])
            .is_err());
    }

    #[test]
    fn no_authority_dispositions_and_semantic_sources_never_exclude_or_consume_counters() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("negative.db")).unwrap();
        let pool = pool();

        for (offset, disposition) in [
            FailureDisposition::RetryableSameRoute,
            FailureDisposition::NonFailover,
            FailureDisposition::Unclassified,
        ]
        .into_iter()
        .enumerate()
        {
            let id = offset as i64 + 1;
            let responsibility = format!("worker:negative:{id}");
            insert_assignment(&conn, id, &responsibility, id);
            record_profile(
                &mut conn,
                id,
                &responsibility,
                &pool,
                0,
                Some(disposition),
                40 + id,
            )
            .unwrap();
            assert_eq!(
                exclusions(&conn, &responsibility).unwrap(),
                RouteExclusions::default()
            );
        }

        // Semantic agent outcomes, test failures, review findings, and
        // authoritative task signals have no #449 runner classification. They
        // are represented only as an attempted route with no disposition.
        for (offset, source) in [
            "semantic-agent-outcome",
            "test-failure",
            "review-finding",
            "authoritative-task-signal",
        ]
        .into_iter()
        .enumerate()
        {
            let id = offset as i64 + 10;
            let responsibility = format!("worker:{source}:{id}");
            insert_assignment(&conn, id, &responsibility, id);
            record_profile(&mut conn, id, &responsibility, &pool, 0, None, 50 + id).unwrap();
            assert_eq!(
                exclusions(&conn, &responsibility).unwrap(),
                RouteExclusions::default()
            );
            let counters: (i64, i64) = conn
                .query_row(
                    "SELECT rework_round,recovery_attempts FROM tasks WHERE id=?1",
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(counters, (0, 0));
        }
    }

    #[test]
    fn concurrent_duplicate_recording_converges_on_one_immutable_authority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concurrent.db");
        let pool = pool();
        {
            let conn = crate::db::open(&path).unwrap();
            insert_assignment(&conn, 70, "worker:task:70", 70);
        }

        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for attempt in 0..8 {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            let pool = pool.clone();
            handles.push(std::thread::spawn(move || {
                let mut conn = crate::db::open(&path).unwrap();
                barrier.wait();
                record_profile(
                    &mut conn,
                    70,
                    "worker:task:70",
                    &pool,
                    0,
                    Some(FailureDisposition::ProviderUnavailable),
                    100 + attempt,
                )
                .unwrap()
            }));
        }
        let outcomes: Vec<RecordOutcome> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, RecordOutcome::Inserted(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, RecordOutcome::Replayed(_)))
                .count(),
            7
        );

        let conn = crate::db::open(&path).unwrap();
        assert_eq!(list(&conn, "worker:task:70").unwrap().len(), 1);
        assert_eq!(
            exclusions(&conn, "worker:task:70")
                .unwrap()
                .excluded_providers(),
            &BTreeSet::from(["claude".to_string()])
        );
    }

    #[test]
    fn mismatched_assignment_and_conflicting_replay_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("mismatch.db")).unwrap();
        let pool = pool();
        insert_assignment(&conn, 80, "worker:task:80", 80);

        let wrong_responsibility = record_profile(
            &mut conn,
            80,
            "worker:task:81",
            &pool,
            0,
            Some(FailureDisposition::ProviderUnavailable),
            1,
        );
        assert!(matches!(wrong_responsibility, Err(QuorumError::Io(_))));
        record_profile(
            &mut conn,
            80,
            "worker:task:80",
            &pool,
            0,
            Some(FailureDisposition::ProviderUnavailable),
            2,
        )
        .unwrap();
        let conflict = record_profile(
            &mut conn,
            80,
            "worker:task:80",
            &pool,
            0,
            Some(FailureDisposition::ProfileUnavailable),
            3,
        );
        assert!(matches!(conflict, Err(QuorumError::Io(_))));
        assert_eq!(list(&conn, "worker:task:80").unwrap().len(), 1);
    }
}
