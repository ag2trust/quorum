//! Durable installation of one validated alternate fallback route.
//!
//! This step consumes only immutable evidence already established by the
//! preflight and retire steps. It does not retire the failed route, alter task
//! budgets, commit a caller-owned transaction, or launch a provider process.

use quorum_core::capabilities::{self, ReviewerLaunchEvidence, RunCapability};
use quorum_core::error::{QuorumError, Result};
use quorum_core::role_assignments::{
    AssignmentIdentity, ModelProfile, RoleAssignment, ValidatedPool,
};
use quorum_core::routing_attempts::{
    self, AlternateRoute, FallbackAttributionInput, RouteExclusions, RoutingAttempt,
};
use rusqlite::Transaction;

/// Immutable evidence and fresh identity for establishing one alternate run.
#[derive(Debug, Clone, Copy)]
pub struct FallbackEstablishInput<'a> {
    /// The exact assignment observed while the failed route was retired.
    pub assignment: &'a RoleAssignment,
    /// The exact configured-pool generation that created `assignment`.
    pub eligible_pool: &'a ValidatedPool,
    /// The persisted, classified failed-route attempt.
    pub failed_attempt: &'a RoutingAttempt,
    /// Exclusions derived from the persisted attempts at the same observation.
    pub exclusions: &'a RouteExclusions,
    /// Fresh agent identity for the alternate provider process.
    pub agent: &'a str,
    /// Fresh daemon-issued capability identity for that process.
    pub capability_run_id: &'a str,
    /// Exact reviewer PR/head authority, required for reviewer fallbacks.
    pub reviewer_launch: Option<ReviewerLaunchEvidence<'a>>,
    /// Timestamp for the immutable `agent_runs` row.
    pub spawned_at: i64,
    /// Timestamp for the fresh capability.
    pub issued_at: i64,
}

/// Result of attempting to establish one alternate fallback route.
#[derive(Debug, Clone)]
pub enum FallbackEstablishOutcome {
    /// One selected route was durably attributed to a fresh run and capability.
    Selected {
        profile: ModelProfile,
        agent_run_id: i64,
        capability: Box<RunCapability>,
    },
    /// No route may receive fallback authority. The caller maps this to its
    /// single fail-safe provider-block outcome without consuming a budget.
    Exhausted,
}

/// Select, validate, and atomically establish one alternate fallback route.
///
/// The supplied assignment, attempt, and exclusions are re-proved inside the
/// caller-owned fallback transaction. Thus evidence that became stale between
/// retirement and establishment cannot attribute a route. A clean routing
/// exhaustion, generation mismatch, or attribution/capability failure returns
/// [`FallbackEstablishOutcome::Exhausted`] and rolls back this step's writes
/// through an internal savepoint. The caller retains responsibility for
/// committing or rolling back its transaction. No provider or process
/// operation occurs here.
pub fn establish(
    tx: &Transaction<'_>,
    input: &FallbackEstablishInput<'_>,
) -> Result<FallbackEstablishOutcome> {
    tx.execute_batch("SAVEPOINT quorum_fallback_establish")?;
    match establish_inner(tx, input) {
        Ok(outcome @ FallbackEstablishOutcome::Selected { .. }) => {
            tx.execute_batch("RELEASE SAVEPOINT quorum_fallback_establish")?;
            Ok(outcome)
        }
        Ok(FallbackEstablishOutcome::Exhausted) => {
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT quorum_fallback_establish; \
                 RELEASE SAVEPOINT quorum_fallback_establish",
            )?;
            Ok(FallbackEstablishOutcome::Exhausted)
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT quorum_fallback_establish; \
                 RELEASE SAVEPOINT quorum_fallback_establish",
            )?;
            Err(error)
        }
    }
}

fn establish_inner(
    tx: &Transaction<'_>,
    input: &FallbackEstablishInput<'_>,
) -> Result<FallbackEstablishOutcome> {
    validate_input(input)?;

    // The caller's immutable observation must still be the persisted
    // assignment and attempt. In particular, do not reconstruct a token from
    // caller-supplied profile fields for a changed assignment.
    let Some(current_assignment) = quorum_core::role_assignments::get(tx, input.assignment.id)?
    else {
        return Ok(FallbackEstablishOutcome::Exhausted);
    };
    if current_assignment != *input.assignment {
        return Ok(FallbackEstablishOutcome::Exhausted);
    }
    let attempts = routing_attempts::list(tx, &input.assignment.responsibility_key)?;
    if !attempts
        .iter()
        .any(|attempt| attempt == input.failed_attempt)
    {
        return Ok(FallbackEstablishOutcome::Exhausted);
    }
    let current_exclusions =
        routing_attempts::exclusions(tx, &input.assignment.responsibility_key)?;
    if current_exclusions != *input.exclusions {
        return Ok(FallbackEstablishOutcome::Exhausted);
    }

    let selected = match routing_attempts::select_alternate(
        input.assignment,
        input.eligible_pool,
        input.exclusions,
    )? {
        AlternateRoute::Selected(profile) => profile,
        AlternateRoute::Exhausted | AlternateRoute::MismatchedGeneration => {
            return Ok(FallbackEstablishOutcome::Exhausted);
        }
    };
    let identity = AssignmentIdentity {
        task_id: input.assignment.task_id,
        responsibility_key: &input.assignment.responsibility_key,
        role: &input.assignment.role,
        pr_number: input.assignment.pr_number,
        review_stage: input.assignment.review_stage.as_deref(),
    };
    let Some(token) = routing_attempts::validate_fallback_attribution(&FallbackAttributionInput {
        assignment: input.assignment,
        identity,
        eligible_pool: input.eligible_pool,
        attempt: input.failed_attempt,
        exclusions: input.exclusions,
        selected_profile: &selected,
    })?
    else {
        return Ok(FallbackEstablishOutcome::Exhausted);
    };

    let agent_run_id = quorum_core::agent_runs::insert_alternate_with_attribution_tx(
        tx,
        &token,
        input.agent,
        input.spawned_at,
    )?;
    let Some(capability) = capabilities::issue_attributed_alternate_tx(
        tx,
        &token,
        agent_run_id,
        input.capability_run_id,
        input.agent,
        input.reviewer_launch,
        input.issued_at,
    )?
    else {
        // The savepoint removes the just-inserted run.
        return Ok(FallbackEstablishOutcome::Exhausted);
    };

    Ok(FallbackEstablishOutcome::Selected {
        profile: selected,
        agent_run_id,
        capability: Box::new(capability),
    })
}

fn validate_input(input: &FallbackEstablishInput<'_>) -> Result<()> {
    input.eligible_pool.validate()?;
    if input.assignment.id <= 0
        || input.assignment.task_id.unwrap_or_default() <= 0
        || !matches!(input.assignment.role.as_str(), "worker" | "reviewer")
        || input.agent.is_empty()
        || input.agent.len() > 1024
        || input.agent.contains('\0')
        || input.capability_run_id.is_empty()
        || input.capability_run_id.contains('\0')
        || input.spawned_at < 0
        || input.issued_at < 0
    {
        return Err(QuorumError::Usage(
            "invalid fallback establishment identity".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::role_assignments::WeightedProfile;
    use quorum_core::routing_attempts::{FailureDisposition, RecordRoutingAttempt};
    use rusqlite::{params, Connection};

    fn profile(id: &str, provider: &str, runner: &str, model: &str, effort: &str) -> ModelProfile {
        ModelProfile {
            id: id.into(),
            provider: provider.into(),
            runner: runner.into(),
            model: model.into(),
            effort: effort.into(),
        }
    }

    fn pool() -> ValidatedPool {
        ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: "generation-1".into(),
            profiles: vec![
                WeightedProfile {
                    profile: profile("primary", "codex", "codex", "gpt-5.6", "high"),
                    percent: 50,
                },
                WeightedProfile {
                    profile: profile(
                        "alternate",
                        "claude",
                        "claude",
                        "claude-sonnet-4-6",
                        "medium",
                    ),
                    percent: 50,
                },
            ],
        }
    }

    fn single_route_pool() -> ValidatedPool {
        ValidatedPool {
            profiles: vec![WeightedProfile {
                profile: profile("primary", "codex", "codex", "gpt-5.6", "high"),
                percent: 100,
            }],
            ..pool()
        }
    }

    fn fixture(pool: &ValidatedPool) -> (tempfile::TempDir, Connection, RoleAssignment, i64) {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = quorum_core::db::open(&dir.path().join("fallback-establish.db")).unwrap();
        let task_id = quorum_core::tasks::create(
            &mut conn,
            "owner",
            "fallback establish fixture",
            None,
            0,
            None,
            None,
            None,
            None,
            1,
        )
        .unwrap();
        let primary = &pool.profiles[0].profile;
        conn.execute(
            "INSERT INTO role_assignments(
                 responsibility_key,task_id,role,complexity,profile_id,provider,runner,model,
                 effort,pool_key,policy_generation,created_at)
             VALUES ('worker:task:1:revision:1',?1,'worker','M',?2,?3,?4,?5,?6,?7,?8,1)",
            params![
                task_id,
                primary.id,
                primary.provider,
                primary.runner,
                primary.model,
                primary.effort,
                pool.pool_key,
                pool.policy_generation,
            ],
        )
        .unwrap();
        let assignment = quorum_core::role_assignments::get(&conn, conn.last_insert_rowid())
            .unwrap()
            .unwrap();
        (dir, conn, assignment, task_id)
    }

    fn record_failure(
        conn: &mut Connection,
        assignment: &RoleAssignment,
        pool: &ValidatedPool,
        profile_index: usize,
        disposition: FailureDisposition,
    ) -> (RoutingAttempt, RouteExclusions) {
        let attempt = routing_attempts::record(
            conn,
            &RecordRoutingAttempt {
                role_assignment_id: assignment.id,
                responsibility_key: &assignment.responsibility_key,
                profile: &pool.profiles[profile_index].profile,
                failure_disposition: Some(disposition),
                recorded_at: 9,
            },
            pool,
        )
        .unwrap()
        .attempt()
        .clone();
        let exclusions =
            routing_attempts::exclusions(conn, &assignment.responsibility_key).unwrap();
        (attempt, exclusions)
    }

    fn input<'a>(
        assignment: &'a RoleAssignment,
        pool: &'a ValidatedPool,
        failed_attempt: &'a RoutingAttempt,
        exclusions: &'a RouteExclusions,
    ) -> FallbackEstablishInput<'a> {
        FallbackEstablishInput {
            assignment,
            eligible_pool: pool,
            failed_attempt,
            exclusions,
            agent: "Alternate-yg68",
            capability_run_id: "fallback-capability-1",
            reviewer_launch: None,
            spawned_at: 10,
            issued_at: 11,
        }
    }

    fn state(conn: &Connection, task_id: i64) -> (i64, i64, i64, i64) {
        conn.query_row(
            "SELECT
                 (SELECT count(*) FROM agent_runs),
                 (SELECT count(*) FROM run_capabilities),
                 (SELECT recovery_attempts FROM tasks WHERE id=?1),
                 (SELECT rework_round FROM tasks WHERE id=?1)",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
    }

    fn establish_and_commit(
        conn: &mut Connection,
        input: &FallbackEstablishInput<'_>,
    ) -> Result<FallbackEstablishOutcome> {
        let tx = quorum_core::db::begin_immediate(conn)?;
        let outcome = establish(&tx, input)?;
        tx.commit()?;
        Ok(outcome)
    }

    #[test]
    fn selected_route_atomically_inserts_fresh_attributed_run_and_capability() {
        let pool = pool();
        let (_dir, mut conn, assignment, task_id) = fixture(&pool);
        let (attempt, exclusions) = record_failure(
            &mut conn,
            &assignment,
            &pool,
            0,
            FailureDisposition::ProviderUnavailable,
        );
        let before = state(&conn, task_id);

        let tx = quorum_core::db::begin_immediate(&mut conn).unwrap();
        let outcome = establish(&tx, &input(&assignment, &pool, &attempt, &exclusions)).unwrap();
        let FallbackEstablishOutcome::Selected {
            profile,
            agent_run_id,
            capability,
        } = outcome
        else {
            panic!("a valid configured alternate must be established");
        };

        assert_eq!(profile, pool.profiles[1].profile);
        assert_eq!(capability.run_id, "fallback-capability-1");
        assert_eq!(capability.agent_run_id, Some(agent_run_id));
        assert_eq!(
            state(&tx, task_id),
            (before.0 + 1, before.1 + 1, before.2, before.3)
        );
        tx.commit().unwrap();
        let route: (String, String, String, String, i64) = conn
            .query_row(
                "SELECT configured_profile_id,configured_provider,provider,agent_name,
                        role_assignment_id
                 FROM agent_runs WHERE id=?1",
                [agent_run_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(route.0, "alternate");
        assert_eq!(route.1, "claude");
        assert_eq!(route.2, "claude");
        assert_eq!(route.3, "Alternate-yg68");
        assert_eq!(route.4, assignment.id);
    }

    #[test]
    fn exhausted_and_generation_mismatch_write_no_authority_or_budget() {
        let single = single_route_pool();
        let (_dir, mut conn, assignment, task_id) = fixture(&single);
        let (attempt, exclusions) = record_failure(
            &mut conn,
            &assignment,
            &single,
            0,
            FailureDisposition::ProviderUnavailable,
        );
        let before = state(&conn, task_id);
        assert!(matches!(
            establish_and_commit(
                &mut conn,
                &input(&assignment, &single, &attempt, &exclusions)
            )
            .unwrap(),
            FallbackEstablishOutcome::Exhausted
        ));
        assert_eq!(state(&conn, task_id), before);

        let pool = pool();
        let (_dir, mut conn, assignment, task_id) = fixture(&pool);
        let (attempt, exclusions) = record_failure(
            &mut conn,
            &assignment,
            &pool,
            0,
            FailureDisposition::ProviderUnavailable,
        );
        let stale_generation = ValidatedPool {
            policy_generation: "generation-stale".into(),
            ..pool.clone()
        };
        let before = state(&conn, task_id);
        assert!(matches!(
            establish_and_commit(
                &mut conn,
                &input(&assignment, &stale_generation, &attempt, &exclusions)
            )
            .unwrap(),
            FallbackEstablishOutcome::Exhausted
        ));
        assert_eq!(state(&conn, task_id), before);
    }

    #[test]
    fn stale_or_non_authorizing_evidence_fails_closed_without_partial_run() {
        let pool = pool();
        let (_dir, mut conn, assignment, task_id) = fixture(&pool);
        let (attempt, exclusions) = record_failure(
            &mut conn,
            &assignment,
            &pool,
            0,
            FailureDisposition::ProviderUnavailable,
        );
        // A subsequent immutable failure makes the caller's exclusions stale.
        record_failure(
            &mut conn,
            &assignment,
            &pool,
            1,
            FailureDisposition::ProfileUnavailable,
        );
        let before = state(&conn, task_id);
        assert!(matches!(
            establish_and_commit(&mut conn, &input(&assignment, &pool, &attempt, &exclusions))
                .unwrap(),
            FallbackEstablishOutcome::Exhausted
        ));
        assert_eq!(state(&conn, task_id), before);

        let (_dir, mut conn, assignment, task_id) = fixture(&pool);
        let (attempt, exclusions) = record_failure(
            &mut conn,
            &assignment,
            &pool,
            0,
            FailureDisposition::RetryableSameRoute,
        );
        let before = state(&conn, task_id);
        assert!(matches!(
            establish_and_commit(&mut conn, &input(&assignment, &pool, &attempt, &exclusions))
                .unwrap(),
            FallbackEstablishOutcome::Exhausted
        ));
        assert_eq!(state(&conn, task_id), before);
    }
}
