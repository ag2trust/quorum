//! Durable cleanup for one failed, fallback-authorizing managed route.
//!
//! This step records only immutable failure evidence and retires the failed
//! run's authority. It deliberately does not select, attribute, or launch an
//! alternate route.

use super::runner::FailureDisposition;
use quorum_core::error::{QuorumError, Result};
use quorum_core::role_assignments::{
    AssignmentIdentity, ModelProfile, RoleAssignment, ValidatedPool,
};
use quorum_core::routing_attempts::{self, RecordRoutingAttempt, RouteExclusions};
use rusqlite::Connection;

/// Exact managed-run evidence that must be retired with the failed route.
#[derive(Debug, Clone, Copy)]
pub struct FailedManagedRun<'a> {
    pub agent_run_id: i64,
    pub capability_run_id: &'a str,
    pub agent: &'a str,
    pub ended_at: i64,
    pub end_reason: &'a str,
}

/// Immutable failure evidence and exact authority identity for retirement.
#[derive(Debug, Clone, Copy)]
pub struct FallbackRetireInput<'a> {
    pub assignment: &'a RoleAssignment,
    pub responsibility: AssignmentIdentity<'a>,
    pub failed_route: &'a ModelProfile,
    pub disposition: FailureDisposition,
    pub eligible_pool: &'a ValidatedPool,
    pub failed_run: FailedManagedRun<'a>,
    pub recorded_at: i64,
}

/// Record an authorizing route failure and revoke its managed-run authority.
///
/// Every storage mutation shares one `BEGIN IMMEDIATE` transaction. Replays
/// reuse the original immutable routing-attempt row and leave recovery/rework
/// accounting untouched. This function performs no provider, process, or
/// network operation.
pub fn retire(conn: &mut Connection, input: &FallbackRetireInput<'_>) -> Result<RouteExclusions> {
    validate_input(input)?;
    let task_id = input
        .assignment
        .task_id
        .expect("validated task-scoped assignment");
    let tx = quorum_core::db::begin_immediate(conn)?;
    if !quorum_core::capabilities::attributed_retirement_target_tx(
        &tx,
        &quorum_core::capabilities::AttributedRetirementTarget {
            capability_run_id: input.failed_run.capability_run_id,
            agent_run_id: input.failed_run.agent_run_id,
            agent: input.failed_run.agent,
            assignment: input.assignment,
            failed_route: input.failed_route,
            ended_at: input.failed_run.ended_at,
            end_reason: input.failed_run.end_reason,
        },
    )? {
        return Err(QuorumError::Usage(
            "fallback retirement run authority does not match immutable evidence".into(),
        ));
    }
    routing_attempts::record_tx(
        &tx,
        &RecordRoutingAttempt {
            role_assignment_id: input.assignment.id,
            responsibility_key: input.responsibility.responsibility_key,
            profile: input.failed_route,
            failure_disposition: Some(input.disposition),
            recorded_at: input.recorded_at,
        },
        input.eligible_pool,
    )?;
    let exclusions = routing_attempts::exclusions(&tx, input.responsibility.responsibility_key)?;
    quorum_core::agent_runs::close_tx(
        &tx,
        input.failed_run.agent_run_id,
        input.failed_run.ended_at,
        input.failed_run.end_reason,
    )?;
    quorum_core::capabilities::revoke_managed_run_tx(
        &tx,
        input.failed_run.capability_run_id,
        input.failed_run.agent,
        task_id,
        &input.assignment.role,
        input.failed_run.ended_at,
    )?;
    tx.commit()?;
    Ok(exclusions)
}

fn validate_input(input: &FallbackRetireInput<'_>) -> Result<()> {
    input.eligible_pool.validate()?;
    if !matches!(
        input.disposition,
        FailureDisposition::ProviderUnavailable | FailureDisposition::ProfileUnavailable
    ) {
        return Err(QuorumError::Usage(
            "fallback retirement requires provider/profile-unavailable disposition".into(),
        ));
    }
    if input.assignment.id <= 0
        || input.assignment.task_id.unwrap_or_default() <= 0
        || !input.assignment.matches_identity(&input.responsibility)
        || !input
            .assignment
            .matches_pool_generation(input.eligible_pool)
        || !input
            .eligible_pool
            .profiles
            .iter()
            .any(|candidate| candidate.profile == *input.failed_route)
        || input.failed_run.agent_run_id <= 0
        || input.recorded_at < 0
        || input.failed_run.ended_at < 0
        || input.failed_run.capability_run_id.is_empty()
        || input.failed_run.capability_run_id.contains('\0')
        || input.failed_run.agent.is_empty()
        || input.failed_run.agent.contains('\0')
        || input.failed_run.end_reason.is_empty()
        || input.failed_run.end_reason.contains('\0')
    {
        return Err(QuorumError::Usage(
            "invalid fallback retirement evidence".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::role_assignments::WeightedProfile;
    use rusqlite::params;

    fn pool() -> ValidatedPool {
        ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: "generation-1".into(),
            profiles: vec![
                WeightedProfile {
                    profile: ModelProfile {
                        id: "primary".into(),
                        provider: "codex".into(),
                        runner: "codex".into(),
                        model: "gpt-5.6".into(),
                        effort: "high".into(),
                    },
                    percent: 50,
                },
                WeightedProfile {
                    profile: ModelProfile {
                        id: "alternate".into(),
                        provider: "claude".into(),
                        runner: "claude".into(),
                        model: "claude-sonnet-4-6".into(),
                        effort: "medium".into(),
                    },
                    percent: 50,
                },
            ],
        }
    }

    fn fixture() -> (
        tempfile::TempDir,
        Connection,
        RoleAssignment,
        ValidatedPool,
        i64,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = quorum_core::db::open(&dir.path().join("fallback-retire.db")).unwrap();
        let task_id = quorum_core::tasks::create(
            &mut conn,
            "owner",
            "fallback retire fixture",
            None,
            0,
            None,
            None,
            None,
            None,
            1,
        )
        .unwrap();
        let pool = pool();
        let route = &pool.profiles[0].profile;
        let responsibility = format!("worker:task:{task_id}:revision:1");
        conn.execute(
            "INSERT INTO role_assignments(
                 responsibility_key,task_id,role,complexity,profile_id,provider,runner,model,
                 effort,pool_key,policy_generation,created_at)
             VALUES (?1,?2,'worker','M',?3,?4,?5,?6,?7,?8,?9,1)",
            params![
                responsibility,
                task_id,
                route.id,
                route.provider,
                route.runner,
                route.model,
                route.effort,
                pool.pool_key,
                pool.policy_generation,
            ],
        )
        .unwrap();
        let assignment = quorum_core::role_assignments::get(&conn, conn.last_insert_rowid())
            .unwrap()
            .unwrap();
        conn.execute(
            "INSERT INTO agent_runs(
                 task_id,agent_name,role,model,effort,provider,role_assignment_id,spawned_at,
                 configured_profile_id,configured_provider,configured_model,configured_effort)
             VALUES (?1,'Wedge-gnua','worker',?2,?3,?4,?5,2,?6,?4,?2,?3)",
            params![
                task_id,
                route.model,
                route.effort,
                route.provider,
                assignment.id,
                route.id,
            ],
        )
        .unwrap();
        let agent_run_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at,agent_run_id)
             VALUES ('managed-run-1',?1,'Wedge-gnua','worker',2,?2)",
            [task_id, agent_run_id],
        )
        .unwrap();
        (dir, conn, assignment, pool, agent_run_id)
    }

    fn input<'a>(
        assignment: &'a RoleAssignment,
        pool: &'a ValidatedPool,
        agent_run_id: i64,
        disposition: FailureDisposition,
    ) -> FallbackRetireInput<'a> {
        FallbackRetireInput {
            assignment,
            responsibility: AssignmentIdentity {
                task_id: assignment.task_id,
                responsibility_key: &assignment.responsibility_key,
                role: &assignment.role,
                pr_number: assignment.pr_number,
                review_stage: assignment.review_stage.as_deref(),
            },
            failed_route: &pool.profiles[0].profile,
            disposition,
            eligible_pool: pool,
            failed_run: FailedManagedRun {
                agent_run_id,
                capability_run_id: "managed-run-1",
                agent: "Wedge-gnua",
                ended_at: 10,
                end_reason: "fallback-route-unavailable",
            },
            recorded_at: 9,
        }
    }

    #[test]
    fn records_once_revokes_authority_and_preserves_task_budgets() {
        let (_dir, mut conn, assignment, pool, agent_run_id) = fixture();
        let input = input(
            &assignment,
            &pool,
            agent_run_id,
            FailureDisposition::ProviderUnavailable,
        );
        let counters = conn
            .query_row(
                "SELECT recovery_attempts,rework_round FROM tasks WHERE id=?1",
                [assignment.task_id.unwrap()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap();

        let exclusions = retire(&mut conn, &input).unwrap();
        assert!(exclusions.excluded_providers().contains("codex"));
        let attempts = routing_attempts::list(&conn, &assignment.responsibility_key).unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].recorded_at, 9);
        assert_eq!(
            attempts[0].failure_disposition,
            Some(FailureDisposition::ProviderUnavailable)
        );
        assert_eq!(
            conn.query_row(
                "SELECT ended_at FROM agent_runs WHERE id=?1",
                [agent_run_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            10
        );
        assert_eq!(
            conn.query_row(
                "SELECT revoked_at FROM run_capabilities WHERE run_id='managed-run-1'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            10
        );

        let replay = retire(&mut conn, &input).unwrap();
        assert_eq!(replay, exclusions);
        assert_eq!(
            routing_attempts::list(&conn, &assignment.responsibility_key)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            conn.query_row(
                "SELECT recovery_attempts,rework_round FROM tasks WHERE id=?1",
                [assignment.task_id.unwrap()],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap(),
            counters
        );
    }

    #[test]
    fn profile_unavailability_excludes_only_the_failed_profile() {
        let (_dir, mut conn, assignment, pool, agent_run_id) = fixture();
        let exclusions = retire(
            &mut conn,
            &input(
                &assignment,
                &pool,
                agent_run_id,
                FailureDisposition::ProfileUnavailable,
            ),
        )
        .unwrap();
        assert!(exclusions.excluded_profiles().contains("primary"));
        assert!(!exclusions.excluded_providers().contains("codex"));
    }

    #[test]
    fn non_authorizing_failure_leaves_evidence_authority_and_budgets_unchanged() {
        let (_dir, mut conn, assignment, pool, agent_run_id) = fixture();
        let before = conn
            .query_row(
                "SELECT
                     (SELECT count(*) FROM routing_attempts),
                     (SELECT recovery_attempts FROM tasks WHERE id=?1),
                     (SELECT rework_round FROM tasks WHERE id=?1),
                     (SELECT revoked_at IS NOT NULL FROM run_capabilities
                      WHERE run_id='managed-run-1')",
                [assignment.task_id.unwrap()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, bool>(3)?,
                    ))
                },
            )
            .unwrap();

        assert!(retire(
            &mut conn,
            &input(
                &assignment,
                &pool,
                agent_run_id,
                FailureDisposition::RetryableSameRoute,
            ),
        )
        .is_err());
        let after = conn
            .query_row(
                "SELECT
                     (SELECT count(*) FROM routing_attempts),
                     (SELECT recovery_attempts FROM tasks WHERE id=?1),
                     (SELECT rework_round FROM tasks WHERE id=?1),
                     (SELECT revoked_at IS NOT NULL FROM run_capabilities
                      WHERE run_id='managed-run-1')",
                [assignment.task_id.unwrap()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, bool>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(after, before);
    }

    #[test]
    fn mismatched_capability_run_binding_rolls_back_every_retirement_mutation() {
        let (_dir, mut conn, assignment, pool, agent_run_id) = fixture();
        let unrelated_run = quorum_core::agent_runs::insert(
            &conn,
            assignment.task_id.unwrap(),
            "Wedge-gnua",
            "worker",
            "unrelated-model",
            "low",
            "codex",
            3,
        )
        .unwrap();
        let before = conn
            .query_row(
                "SELECT
                     (SELECT count(*) FROM routing_attempts),
                     (SELECT recovery_attempts FROM tasks WHERE id=?1),
                     (SELECT rework_round FROM tasks WHERE id=?1),
                     (SELECT count(*) FROM agent_runs WHERE id IN (?2,?3) AND ended_at IS NOT NULL),
                     (SELECT revoked_at IS NOT NULL FROM run_capabilities
                      WHERE run_id='managed-run-1')",
                params![assignment.task_id.unwrap(), agent_run_id, unrelated_run],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, bool>(4)?,
                    ))
                },
            )
            .unwrap();

        assert!(retire(
            &mut conn,
            &input(
                &assignment,
                &pool,
                unrelated_run,
                FailureDisposition::ProviderUnavailable,
            ),
        )
        .is_err());
        let after = conn
            .query_row(
                "SELECT
                     (SELECT count(*) FROM routing_attempts),
                     (SELECT recovery_attempts FROM tasks WHERE id=?1),
                     (SELECT rework_round FROM tasks WHERE id=?1),
                     (SELECT count(*) FROM agent_runs WHERE id IN (?2,?3) AND ended_at IS NOT NULL),
                     (SELECT revoked_at IS NOT NULL FROM run_capabilities
                      WHERE run_id='managed-run-1')",
                params![assignment.task_id.unwrap(), agent_run_id, unrelated_run],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, bool>(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(after, before);
    }
}
