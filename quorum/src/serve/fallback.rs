//! Dormant, durable installation of one alternate managed route.
//!
//! This module deliberately stops after committing and reconstructing the
//! launch descriptor. The worker and reviewer lifecycle own any later
//! provider/process call, and no lifecycle callsite is wired here.

use super::fallback_establish::{self, FallbackEstablishInput, FallbackEstablishOutcome};
use super::fallback_preflight::{
    self, FallbackPreflightInput, FallbackPreflightOutcome, ManagedTurnCurrency,
};
use super::fallback_retire::{self, FailedManagedRun, FallbackRetireInput};
use super::runner::FailureDisposition;
use quorum_core::capabilities::ReviewerLaunchEvidence;
use quorum_core::error::{QuorumError, Result};
use quorum_core::fallback_launch::{
    self, FallbackLaunchInput, FallbackLaunchIntent, PendingManagedTurn, PrHead,
};
use quorum_core::role_assignments::{
    AssignmentIdentity, ModelProfile, RoleAssignment, ValidatedPool,
};
use quorum_core::routing_attempts::{
    self, FallbackAttributionInput, RoutingAttempt, ValidatedFallbackAttribution,
};
use quorum_core::runner_state::{PendingTurn, ProviderBlock};
use rusqlite::Connection;

/// Immutable input for one possible fallback installation.
///
/// `current_currency` is the caller's fresh read of the exact lifecycle,
/// lease, and PR-head identity captured in `expected_currency`. The installer
/// only admits a matching, live snapshot; it never turns a runner failure into
/// lifecycle authority on its own.
#[derive(Debug, Clone, Copy)]
pub struct FallbackInstallInput<'a> {
    pub disposition: FailureDisposition,
    pub assignment: &'a RoleAssignment,
    pub responsibility: AssignmentIdentity<'a>,
    pub failed_route: &'a ModelProfile,
    pub pending_turn: &'a PendingTurn,
    pub eligible_pool: &'a ValidatedPool,
    pub expected_currency: &'a ManagedTurnCurrency,
    pub current_currency: &'a ManagedTurnCurrency,
    pub observed_at: i64,
    pub failed_run: FailedManagedRun<'a>,
    pub alternate_agent: &'a str,
    pub alternate_capability_run_id: &'a str,
    pub reviewer_launch: Option<ReviewerLaunchEvidence<'a>>,
    pub worktree: &'a str,
    pub recorded_at: i64,
    pub spawned_at: i64,
    pub issued_at: i64,
}

/// Closed result of a dormant fallback installation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FallbackInstallOutcome {
    /// The runner failure was valid evidence but is not eligible for fallback.
    NoFailover,
    /// Lifecycle, lease, head, assignment, route, or pool evidence was stale
    /// or contradictory. No database mutation was made.
    FailClosed,
    /// One alternate run, capability, and restart-safe descriptor were
    /// committed before this result was returned.
    Installed(Box<FallbackLaunchIntent>),
    /// The failed route was durably retired, but no alternate can receive
    /// authority. This changes neither recovery nor rework budget.
    ProviderBlocked(ProviderBlock),
}

/// Compose fallback preflight, retirement, establishment, and intent storage.
///
/// The preflight check occurs before opening a transaction. Once authorized,
/// all durable mutation shares one caller-owned `BEGIN IMMEDIATE` transaction:
/// the failed route is recorded and retired, one alternate is attributed, and
/// its launch descriptor is persisted. The transaction commits before the
/// descriptor is reconstructed and returned; this function has no provider,
/// process, network, or model call.
pub fn install(
    conn: &mut Connection,
    input: &FallbackInstallInput<'_>,
) -> Result<FallbackInstallOutcome> {
    match fallback_preflight::preflight(&FallbackPreflightInput {
        disposition: input.disposition,
        assignment: input.assignment,
        responsibility: input.responsibility,
        failed_route: input.failed_route,
        pending_turn: input.pending_turn,
        eligible_pool: input.eligible_pool,
        expected_currency: input.expected_currency,
        current_currency: input.current_currency,
        observed_at: input.observed_at,
    }) {
        FallbackPreflightOutcome::NoFailover => return Ok(FallbackInstallOutcome::NoFailover),
        FallbackPreflightOutcome::FailClosed => return Ok(FallbackInstallOutcome::FailClosed),
        FallbackPreflightOutcome::Authorized => {}
    }
    if !launch_identity_is_current(input) {
        return Ok(FallbackInstallOutcome::FailClosed);
    }

    let launch_head = input.expected_currency.head.as_ref().map(|head| PrHead {
        number: head.pr_number,
        head_sha: head.head_sha.clone(),
    });
    let tx = quorum_core::db::begin_immediate(conn)?;
    let exclusions = fallback_retire::retire_tx(
        &tx,
        &FallbackRetireInput {
            assignment: input.assignment,
            responsibility: input.responsibility,
            failed_route: input.failed_route,
            disposition: input.disposition,
            eligible_pool: input.eligible_pool,
            failed_run: input.failed_run,
            recorded_at: input.recorded_at,
        },
    )?;
    let failed_attempt = failed_attempt(&tx, input)?;
    if let Some(intent) = fallback_launch::reconstruct(
        &tx,
        input.responsibility.responsibility_key,
        failed_attempt.id,
    )? {
        if !replay_matches(input, &exclusions, launch_head.as_ref(), &intent)? {
            return Err(QuorumError::Io(
                "fallback launch intent replay conflicts with immutable evidence".into(),
            ));
        }
        tx.commit()?;
        return fallback_launch::reconstruct(
            conn,
            input.responsibility.responsibility_key,
            failed_attempt.id,
        )?
        .map(|intent| FallbackInstallOutcome::Installed(Box::new(intent)))
        .ok_or_else(|| QuorumError::Io("committed fallback launch intent is missing".into()));
    }

    let established = fallback_establish::establish(
        &tx,
        &FallbackEstablishInput {
            assignment: input.assignment,
            eligible_pool: input.eligible_pool,
            failed_attempt: &failed_attempt,
            exclusions: &exclusions,
            agent: input.alternate_agent,
            capability_run_id: input.alternate_capability_run_id,
            reviewer_launch: input.reviewer_launch,
            spawned_at: input.spawned_at,
            issued_at: input.issued_at,
        },
    )?;

    let (routing_attempt_id, outcome) = match established {
        FallbackEstablishOutcome::Exhausted => (
            failed_attempt.id,
            FallbackInstallOutcome::ProviderBlocked(ProviderBlock {
                provider: input.failed_route.provider.clone(),
                reason: "all configured fallback routes are unavailable".into(),
            }),
        ),
        FallbackEstablishOutcome::Selected {
            profile,
            agent_run_id,
            capability,
        } => {
            let attribution = attribution(input, &failed_attempt, &exclusions, &profile)?;
            let pending_turn =
                PendingManagedTurn::for_fallback_profile(input.pending_turn, &profile);
            let intent = fallback_launch::persist_tx(
                &tx,
                &FallbackLaunchInput {
                    attribution: &attribution,
                    routing_attempt_id: failed_attempt.id,
                    worktree: input.worktree,
                    pr_head: launch_head.as_ref(),
                    pending_turn: &pending_turn,
                    agent_run_id,
                    capability_run_id: &capability.run_id,
                    created_at: input.issued_at,
                },
            )?;
            (
                intent.routing_attempt_id,
                FallbackInstallOutcome::Installed(Box::new(intent)),
            )
        }
    };
    tx.commit()?;

    match outcome {
        FallbackInstallOutcome::Installed(_) => fallback_launch::reconstruct(
            conn,
            input.responsibility.responsibility_key,
            routing_attempt_id,
        )?
        .map(|intent| FallbackInstallOutcome::Installed(Box::new(intent)))
        .ok_or_else(|| QuorumError::Io("committed fallback launch intent is missing".into())),
        outcome => Ok(outcome),
    }
}

fn launch_identity_is_current(input: &FallbackInstallInput<'_>) -> bool {
    match (
        input.assignment.role.as_str(),
        &input.expected_currency.head,
    ) {
        ("worker", _) => input.reviewer_launch.is_none(),
        ("reviewer", Some(head)) => input
            .reviewer_launch
            .is_some_and(|launch| launch.pr == head.pr_number && launch.head_sha == head.head_sha),
        _ => false,
    }
}

fn replay_matches(
    input: &FallbackInstallInput<'_>,
    exclusions: &routing_attempts::RouteExclusions,
    launch_head: Option<&PrHead>,
    intent: &FallbackLaunchIntent,
) -> Result<bool> {
    let expected_profile = match routing_attempts::select_alternate(
        input.assignment,
        input.eligible_pool,
        exclusions,
    )? {
        routing_attempts::AlternateRoute::Selected(profile) => profile,
        routing_attempts::AlternateRoute::Exhausted
        | routing_attempts::AlternateRoute::MismatchedGeneration => return Ok(false),
    };
    Ok(
        intent.task_id == input.assignment.task_id.unwrap_or_default()
            && intent.responsibility_key == input.responsibility.responsibility_key
            && intent.role == input.assignment.role
            && intent.worktree == input.worktree
            && intent.pr_head.as_ref() == launch_head
            && intent.pending_turn
                == PendingManagedTurn::for_fallback_profile(input.pending_turn, &expected_profile),
    )
}

fn failed_attempt(conn: &Connection, input: &FallbackInstallInput<'_>) -> Result<RoutingAttempt> {
    routing_attempts::list(conn, input.responsibility.responsibility_key)?
        .into_iter()
        .find(|attempt| {
            attempt.role_assignment_id == input.assignment.id
                && attempt.profile == *input.failed_route
                && attempt.failure_disposition == Some(input.disposition)
        })
        .ok_or_else(|| QuorumError::Io("retired fallback route attempt is missing".into()))
}

fn attribution(
    input: &FallbackInstallInput<'_>,
    failed_attempt: &RoutingAttempt,
    exclusions: &routing_attempts::RouteExclusions,
    profile: &ModelProfile,
) -> Result<ValidatedFallbackAttribution> {
    routing_attempts::validate_fallback_attribution(&FallbackAttributionInput {
        assignment: input.assignment,
        identity: input.responsibility,
        eligible_pool: input.eligible_pool,
        attempt: failed_attempt,
        exclusions,
        selected_profile: profile,
    })?
    .ok_or_else(|| QuorumError::Io("established fallback attribution is no longer current".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::role_assignments::WeightedProfile;
    use rusqlite::{params, Connection};
    use std::sync::{Arc, Barrier};

    fn pool() -> ValidatedPool {
        ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: "fallback-install-test".into(),
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

    struct Fixture {
        dir: tempfile::TempDir,
        conn: Connection,
        assignment: RoleAssignment,
        pool: ValidatedPool,
        task_id: i64,
    }

    fn fixture(pool: ValidatedPool) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = quorum_core::db::open(&dir.path().join("fallback-install.db")).unwrap();
        let task_id = quorum_core::tasks::create(
            &mut conn,
            "owner",
            "fallback install fixture",
            None,
            0,
            None,
            None,
            None,
            None,
            1,
        )
        .unwrap();
        conn.execute("UPDATE tasks SET status='working' WHERE id=?1", [task_id])
            .unwrap();
        let responsibility = format!("worker:task:{task_id}:revision:1");
        let primary = &pool.profiles[0].profile;
        conn.execute(
            "INSERT INTO role_assignments(
                 responsibility_key,task_id,role,complexity,profile_id,provider,runner,model,
                 effort,pool_key,policy_generation,created_at)
             VALUES (?1,?2,'worker','M',?3,?4,?5,?6,?7,?8,?9,1)",
            params![
                responsibility,
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
        conn.execute(
            "INSERT INTO agent_runs(
                 task_id,agent_name,role,model,effort,provider,role_assignment_id,spawned_at,
                 configured_profile_id,configured_provider,configured_model,configured_effort)
             VALUES (?1,'failed-agent','worker',?2,?3,?4,?5,2,?6,?4,?2,?3)",
            params![
                task_id,
                primary.model,
                primary.effort,
                primary.provider,
                assignment.id,
                primary.id,
            ],
        )
        .unwrap();
        let failed_run_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at,agent_run_id)
             VALUES ('failed-capability',?1,'failed-agent','worker',2,?2)",
            [task_id, failed_run_id],
        )
        .unwrap();
        Fixture {
            dir,
            conn,
            assignment,
            pool,
            task_id,
        }
    }

    fn currency(task_id: i64) -> ManagedTurnCurrency {
        ManagedTurnCurrency {
            lifecycle: super::fallback_preflight::LifecycleCurrency {
                task_id,
                status: "working".into(),
                generation: 1,
            },
            lease: super::fallback_preflight::LeaseCurrency {
                claim_id: 1,
                target: format!("task#{task_id}"),
                holder: "fallback-installer".into(),
                expires_at: 100,
            },
            head: None,
        }
    }

    fn pending_turn() -> PendingTurn {
        PendingTurn {
            provider: "codex".into(),
            model: "gpt-5.6".into(),
            effort: "high".into(),
            prompt: "complete the managed turn".into(),
            turn_kind: "rework".into(),
            continuation_id: Some("failed-provider-thread".into()),
            requested: true,
        }
    }

    fn input<'a>(
        assignment: &'a RoleAssignment,
        pool: &'a ValidatedPool,
        disposition: FailureDisposition,
        expected: &'a ManagedTurnCurrency,
        current: &'a ManagedTurnCurrency,
        turn: &'a PendingTurn,
    ) -> FallbackInstallInput<'a> {
        FallbackInstallInput {
            disposition,
            assignment,
            responsibility: AssignmentIdentity {
                task_id: assignment.task_id,
                responsibility_key: &assignment.responsibility_key,
                role: &assignment.role,
                pr_number: assignment.pr_number,
                review_stage: assignment.review_stage.as_deref(),
            },
            failed_route: &pool.profiles[0].profile,
            pending_turn: turn,
            eligible_pool: pool,
            expected_currency: expected,
            current_currency: current,
            observed_at: 99,
            failed_run: FailedManagedRun {
                agent_run_id: 1,
                capability_run_id: "failed-capability",
                agent: "failed-agent",
                ended_at: 10,
                end_reason: "fallback-route-unavailable",
            },
            alternate_agent: "alternate-agent",
            alternate_capability_run_id: "alternate-capability",
            reviewer_launch: None,
            worktree: "/tmp/fallback-install-worktree",
            recorded_at: 9,
            spawned_at: 11,
            issued_at: 12,
        }
    }

    fn state(conn: &Connection, task_id: i64) -> (i64, i64, i64, i64, i64, i64) {
        conn.query_row(
            "SELECT
                 (SELECT count(*) FROM routing_attempts),
                 (SELECT count(*) FROM agent_runs),
                 (SELECT count(*) FROM run_capabilities),
                 (SELECT count(*) FROM fallback_launch_intents),
                 (SELECT recovery_attempts FROM tasks WHERE id=?1),
                 (SELECT rework_round FROM tasks WHERE id=?1)",
            [task_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap()
    }

    #[test]
    fn non_authorizing_or_stale_preflight_returns_without_mutation() {
        for disposition in [
            FailureDisposition::RetryableSameRoute,
            FailureDisposition::NonFailover,
            FailureDisposition::Unclassified,
        ] {
            let mut fixture = fixture(pool());
            let currency = currency(fixture.task_id);
            let turn = pending_turn();
            let before = state(&fixture.conn, fixture.task_id);
            assert_eq!(
                install(
                    &mut fixture.conn,
                    &input(
                        &fixture.assignment,
                        &fixture.pool,
                        disposition,
                        &currency,
                        &currency,
                        &turn,
                    )
                )
                .unwrap(),
                FallbackInstallOutcome::NoFailover
            );
            assert_eq!(state(&fixture.conn, fixture.task_id), before);
        }

        let mut fixture = fixture(pool());
        let expected = currency(fixture.task_id);
        let mut stale = expected.clone();
        stale.lease.expires_at = 99;
        let turn = pending_turn();
        let before = state(&fixture.conn, fixture.task_id);
        assert_eq!(
            install(
                &mut fixture.conn,
                &input(
                    &fixture.assignment,
                    &fixture.pool,
                    FailureDisposition::ProviderUnavailable,
                    &expected,
                    &stale,
                    &turn,
                )
            )
            .unwrap(),
            FallbackInstallOutcome::FailClosed
        );
        assert_eq!(state(&fixture.conn, fixture.task_id), before);
    }

    #[test]
    fn persists_replays_and_reconstructs_a_continuation_free_launch_descriptor() {
        let mut fixture = fixture(pool());
        let currency = currency(fixture.task_id);
        let turn = pending_turn();
        let first = install(
            &mut fixture.conn,
            &input(
                &fixture.assignment,
                &fixture.pool,
                FailureDisposition::ProviderUnavailable,
                &currency,
                &currency,
                &turn,
            ),
        )
        .unwrap();
        let FallbackInstallOutcome::Installed(intent) = first else {
            panic!("an alternate route must be installed");
        };
        let intent = *intent;
        assert_eq!(intent.task_id, fixture.task_id);
        assert_eq!(intent.role, "worker");
        assert_eq!(intent.worktree, "/tmp/fallback-install-worktree");
        assert_eq!(intent.pending_turn.provider, "claude");
        assert_eq!(intent.pending_turn.model, "claude-sonnet-4-6");
        assert_eq!(intent.pending_turn.prompt, turn.prompt);
        assert_eq!(intent.pending_turn.turn_kind, turn.turn_kind);
        assert_eq!(intent.pending_turn.requested, turn.requested);
        let committed = fallback_launch::reconstruct(
            &fixture.conn,
            &fixture.assignment.responsibility_key,
            intent.routing_attempt_id,
        )
        .unwrap();
        assert_eq!(committed, Some(intent.clone()));

        let mut replay_input = input(
            &fixture.assignment,
            &fixture.pool,
            FailureDisposition::ProviderUnavailable,
            &currency,
            &currency,
            &turn,
        );
        replay_input.alternate_agent = "new-agent-must-not-replace-the-persisted-run";
        replay_input.alternate_capability_run_id =
            "new-capability-must-not-replace-the-persisted-intent";
        let replay = install(&mut fixture.conn, &replay_input).unwrap();
        assert_eq!(replay, FallbackInstallOutcome::Installed(Box::new(intent)));
        assert_eq!(state(&fixture.conn, fixture.task_id), (1, 2, 2, 1, 0, 0));
    }

    #[test]
    fn profile_unavailability_excludes_only_the_failed_profile_before_installing() {
        let mut fixture = fixture(pool());
        let currency = currency(fixture.task_id);
        let turn = pending_turn();
        let outcome = install(
            &mut fixture.conn,
            &input(
                &fixture.assignment,
                &fixture.pool,
                FailureDisposition::ProfileUnavailable,
                &currency,
                &currency,
                &turn,
            ),
        )
        .unwrap();
        let FallbackInstallOutcome::Installed(intent) = outcome else {
            panic!("a different configured profile must remain eligible");
        };
        assert_eq!(intent.pending_turn.provider, "claude");
        let exclusions =
            routing_attempts::exclusions(&fixture.conn, &fixture.assignment.responsibility_key)
                .unwrap();
        assert!(exclusions.excluded_profiles().contains("primary"));
        assert!(!exclusions.excluded_providers().contains("codex"));
        assert_eq!(state(&fixture.conn, fixture.task_id), (1, 2, 2, 1, 0, 0));
    }

    #[test]
    fn reviewer_install_carries_the_current_pr_head_into_the_committed_descriptor() {
        let mut fixture = fixture(pool());
        fixture
            .conn
            .execute(
                "UPDATE tasks SET status='in-review' WHERE id=?1",
                [fixture.task_id],
            )
            .unwrap();
        fixture
            .conn
            .execute(
                "UPDATE role_assignments
                 SET role='reviewer',review_stage='r1',pr_number=7 WHERE id=?1",
                [fixture.assignment.id],
            )
            .unwrap();
        fixture
            .conn
            .execute("UPDATE agent_runs SET role='reviewer' WHERE id=1", [])
            .unwrap();
        fixture
            .conn
            .execute(
                "UPDATE run_capabilities SET role='reviewer' WHERE run_id='failed-capability'",
                [],
            )
            .unwrap();
        fixture.assignment =
            quorum_core::role_assignments::get(&fixture.conn, fixture.assignment.id)
                .unwrap()
                .unwrap();
        let head_sha = "a".repeat(40);
        let currency = ManagedTurnCurrency {
            lifecycle: super::fallback_preflight::LifecycleCurrency {
                task_id: fixture.task_id,
                status: "in-review".into(),
                generation: 1,
            },
            lease: super::fallback_preflight::LeaseCurrency {
                claim_id: 1,
                target: format!("task#{}", fixture.task_id),
                holder: "reviewer-installer".into(),
                expires_at: 100,
            },
            head: Some(super::fallback_preflight::HeadCurrency {
                pr_number: 7,
                head_ref: "daemon/task-review".into(),
                head_sha: head_sha.clone(),
            }),
        };
        let turn = pending_turn();
        let mut install_input = input(
            &fixture.assignment,
            &fixture.pool,
            FailureDisposition::ProviderUnavailable,
            &currency,
            &currency,
            &turn,
        );
        install_input.reviewer_launch = Some(ReviewerLaunchEvidence {
            pr: 7,
            head_sha: &head_sha,
        });
        let outcome = install(&mut fixture.conn, &install_input).unwrap();
        let FallbackInstallOutcome::Installed(intent) = outcome else {
            panic!("a current reviewer target must be installed");
        };
        assert_eq!(intent.role, "reviewer");
        assert_eq!(
            intent.pr_head,
            Some(PrHead {
                number: 7,
                head_sha,
            })
        );
    }

    #[test]
    fn exhaustion_commits_only_retirement_and_preserves_budgets() {
        let mut only_primary = pool();
        only_primary.profiles.pop();
        only_primary.profiles[0].percent = 100;
        let mut fixture = fixture(only_primary);
        let currency = currency(fixture.task_id);
        let turn = pending_turn();
        let before = state(&fixture.conn, fixture.task_id);
        assert_eq!(
            install(
                &mut fixture.conn,
                &input(
                    &fixture.assignment,
                    &fixture.pool,
                    FailureDisposition::ProviderUnavailable,
                    &currency,
                    &currency,
                    &turn,
                )
            )
            .unwrap(),
            FallbackInstallOutcome::ProviderBlocked(ProviderBlock {
                provider: "codex".into(),
                reason: "all configured fallback routes are unavailable".into(),
            })
        );
        assert_eq!(
            state(&fixture.conn, fixture.task_id),
            (before.0 + 1, before.1, before.2, before.3, 0, 0)
        );
        assert_eq!(
            fixture
                .conn
                .query_row(
                    "SELECT revoked_at FROM run_capabilities WHERE run_id='failed-capability'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            10
        );
    }

    #[test]
    fn concurrent_installers_converge_on_one_attempt_and_intent() {
        for _ in 0..4 {
            let fixture = fixture(pool());
            let path = fixture.dir.path().join("fallback-install.db");
            let task_id = fixture.task_id;
            let assignment_id = fixture.assignment.id;
            let Fixture { dir, conn, .. } = fixture;
            drop(conn);

            let barrier = Arc::new(Barrier::new(8));
            let handles = (0..8)
                .map(|_| {
                    let path = path.clone();
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let mut conn = quorum_core::db::open(&path).unwrap();
                        let assignment = quorum_core::role_assignments::get(&conn, assignment_id)
                            .unwrap()
                            .unwrap();
                        let pool = pool();
                        let currency = currency(assignment.task_id.unwrap());
                        let turn = pending_turn();
                        let input = FallbackInstallInput {
                            disposition: FailureDisposition::ProviderUnavailable,
                            responsibility: AssignmentIdentity {
                                task_id: assignment.task_id,
                                responsibility_key: &assignment.responsibility_key,
                                role: &assignment.role,
                                pr_number: assignment.pr_number,
                                review_stage: assignment.review_stage.as_deref(),
                            },
                            assignment: &assignment,
                            failed_route: &pool.profiles[0].profile,
                            pending_turn: &turn,
                            eligible_pool: &pool,
                            expected_currency: &currency,
                            current_currency: &currency,
                            observed_at: 99,
                            failed_run: FailedManagedRun {
                                agent_run_id: 1,
                                capability_run_id: "failed-capability",
                                agent: "failed-agent",
                                ended_at: 10,
                                end_reason: "fallback-route-unavailable",
                            },
                            alternate_agent: "alternate-agent",
                            alternate_capability_run_id: "alternate-capability",
                            reviewer_launch: None,
                            worktree: "/tmp/fallback-install-worktree",
                            recorded_at: 9,
                            spawned_at: 11,
                            issued_at: 12,
                        };
                        barrier.wait();
                        install(&mut conn, &input).unwrap()
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                assert!(matches!(
                    handle.join().unwrap(),
                    FallbackInstallOutcome::Installed(_)
                ));
            }
            let conn = quorum_core::db::open(&path).unwrap();
            assert_eq!(state(&conn, task_id), (1, 2, 2, 1, 0, 0));
            drop(dir);
        }
    }
}
