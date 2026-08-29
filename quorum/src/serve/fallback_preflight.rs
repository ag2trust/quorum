//! Read-only admission checks for one possible alternate provider route.
//!
//! This is deliberately only a preflight: recording the failed route,
//! choosing an alternate, issuing a capability, and launching a process all
//! belong to later, authoritative operations.

use super::runner::FailureDisposition;
use quorum_core::role_assignments::{
    AssignmentIdentity, ModelProfile, RoleAssignment, ValidatedPool,
};
use quorum_core::runner_state::{self, PendingTurn};

/// The closed result of checking whether a pre-authoritative failure may
/// proceed to alternate-route selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackPreflightOutcome {
    /// The failure is eligible for one later alternate-route selection.
    Authorized,
    /// The failure is valid evidence, but never grants fallback authority.
    NoFailover,
    /// Identity or currency is stale, incomplete, or contradictory.
    FailClosed,
}

/// Immutable lifecycle state observed for one managed turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleCurrency {
    pub task_id: i64,
    pub status: String,
    pub generation: i64,
}

/// Immutable identity of the task lease held while the turn was attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseCurrency {
    pub claim_id: i64,
    pub target: String,
    pub holder: String,
    pub expires_at: i64,
}

/// Immutable PR-head baseline for a managed turn that has a PR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadCurrency {
    pub pr_number: i64,
    pub head_ref: String,
    pub head_sha: String,
}

/// The complete lifecycle, lease, and PR-head currency for a managed turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedTurnCurrency {
    pub lifecycle: LifecycleCurrency,
    pub lease: LeaseCurrency,
    pub head: Option<HeadCurrency>,
}

/// All immutable evidence the preflight gate needs. `expected_currency` is
/// captured with the failed turn; `current_currency` is freshly read by the
/// caller. `observed_at` is the instant at which the current currency was
/// observed, so an otherwise matching lease is live only when
/// `expires_at > observed_at`. The gate never opens a database connection to
/// obtain this evidence.
#[derive(Debug, Clone)]
pub struct FallbackPreflightInput<'a> {
    pub disposition: FailureDisposition,
    pub assignment: &'a RoleAssignment,
    pub responsibility: AssignmentIdentity<'a>,
    pub failed_route: &'a ModelProfile,
    pub pending_turn: &'a PendingTurn,
    pub eligible_pool: &'a ValidatedPool,
    pub expected_currency: &'a ManagedTurnCurrency,
    pub current_currency: &'a ManagedTurnCurrency,
    pub observed_at: i64,
}

/// Authorize only a current, exact provider/profile-unavailable failure.
///
/// Every check is in-memory and this function has no fallible/side-effecting
/// operations. Invalid input is deliberately indistinguishable from stale
/// input at this boundary: both return [`FallbackPreflightOutcome::FailClosed`].
pub fn preflight(input: &FallbackPreflightInput<'_>) -> FallbackPreflightOutcome {
    if !currency_is_current(
        input.assignment,
        input.expected_currency,
        input.current_currency,
        input.observed_at,
    ) || input.eligible_pool.validate().is_err()
        || !input.assignment.matches_identity(&input.responsibility)
        || !input
            .assignment
            .matches_pool_generation(input.eligible_pool)
        || input.assignment.profile_snapshot() != *input.failed_route
        || !pool_contains_exact(input.eligible_pool, input.failed_route)
        || !pending_turn_matches_route(input.pending_turn, input.failed_route)
    {
        return FallbackPreflightOutcome::FailClosed;
    }

    match input.disposition {
        FailureDisposition::ProviderUnavailable | FailureDisposition::ProfileUnavailable => {
            FallbackPreflightOutcome::Authorized
        }
        FailureDisposition::RetryableSameRoute
        | FailureDisposition::NonFailover
        | FailureDisposition::Unclassified => FallbackPreflightOutcome::NoFailover,
    }
}

fn currency_is_current(
    assignment: &RoleAssignment,
    expected: &ManagedTurnCurrency,
    current: &ManagedTurnCurrency,
    observed_at: i64,
) -> bool {
    expected == current
        && assignment.id > 0
        && expected.lifecycle.task_id > 0
        && expected.lifecycle.task_id == assignment.task_id.unwrap_or_default()
        && lifecycle_is_live(assignment, &expected.lifecycle.status)
        && expected.lifecycle.generation >= 0
        && expected.lease.claim_id > 0
        && expected.lease.target == format!("task#{}", expected.lifecycle.task_id)
        && !expected.lease.holder.is_empty()
        && observed_at >= 0
        && expected.lease.expires_at > observed_at
        && match (&assignment.pr_number, &expected.head) {
            (None, None) => true,
            (Some(pr), Some(head)) => {
                *pr == head.pr_number
                    && head.pr_number > 0
                    && !head.head_ref.is_empty()
                    && !head.head_sha.is_empty()
            }
            _ => false,
        }
}

fn lifecycle_is_live(assignment: &RoleAssignment, status: &str) -> bool {
    match assignment.role.as_str() {
        "worker" => matches!(status, "working" | "rework"),
        "reviewer" => status == "in-review",
        _ => false,
    }
}

fn pool_contains_exact(pool: &ValidatedPool, route: &ModelProfile) -> bool {
    pool.profiles
        .iter()
        .any(|candidate| candidate.profile == *route)
}

fn pending_turn_matches_route(turn: &PendingTurn, route: &ModelProfile) -> bool {
    runner_state::pending_turn_is_complete(turn)
        && runner_state::pending_turn_is_resumable(turn)
        && turn.provider == route.provider
        && turn.model == route.model
        && turn.effort == route.effort
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::claims::{self, ClaimOutcome};
    use quorum_core::role_assignments::WeightedProfile;
    use quorum_core::tasks;

    fn profile() -> ModelProfile {
        ModelProfile {
            id: "codex-primary".into(),
            provider: "codex".into(),
            runner: "codex".into(),
            model: "gpt-5.6".into(),
            effort: "high".into(),
        }
    }

    fn assignment() -> RoleAssignment {
        let profile = profile();
        RoleAssignment {
            id: 7,
            responsibility_key: "task:42:worker".into(),
            task_id: Some(42),
            pr_number: Some(9),
            role: "worker".into(),
            review_stage: None,
            complexity: Some("M".into()),
            profile_id: profile.id,
            provider: profile.provider,
            runner: profile.runner,
            model: profile.model,
            effort: profile.effort,
            pool_key: "worker.M".into(),
            policy_generation: "generation-a".into(),
            created_at: 1,
        }
    }

    fn pool() -> ValidatedPool {
        ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: "generation-a".into(),
            profiles: vec![WeightedProfile {
                profile: profile(),
                percent: 100,
            }],
        }
    }

    fn currency() -> ManagedTurnCurrency {
        ManagedTurnCurrency {
            lifecycle: LifecycleCurrency {
                task_id: 42,
                status: "working".into(),
                generation: 3,
            },
            lease: LeaseCurrency {
                claim_id: 11,
                target: "task#42".into(),
                holder: "worker-a".into(),
                expires_at: 100,
            },
            head: Some(HeadCurrency {
                pr_number: 9,
                head_ref: "daemon/task-42".into(),
                head_sha: "head-a".into(),
            }),
        }
    }

    fn pending_turn() -> PendingTurn {
        PendingTurn {
            provider: "codex".into(),
            model: "gpt-5.6".into(),
            effort: "high".into(),
            prompt: "implement the task".into(),
            turn_kind: "initial".into(),
            continuation_id: None,
            requested: false,
        }
    }

    fn input<'a>(
        disposition: FailureDisposition,
        assignment: &'a RoleAssignment,
        pool: &'a ValidatedPool,
        expected_currency: &'a ManagedTurnCurrency,
        current_currency: &'a ManagedTurnCurrency,
        pending_turn: &'a PendingTurn,
        failed_route: &'a ModelProfile,
    ) -> FallbackPreflightInput<'a> {
        FallbackPreflightInput {
            disposition,
            assignment,
            responsibility: AssignmentIdentity {
                task_id: Some(42),
                responsibility_key: "task:42:worker",
                role: "worker",
                pr_number: Some(9),
                review_stage: None,
            },
            failed_route,
            pending_turn,
            eligible_pool: pool,
            expected_currency,
            current_currency,
            observed_at: 99,
        }
    }

    #[test]
    fn only_provider_and_profile_unavailability_authorize() {
        let assignment = assignment();
        let pool = pool();
        let currency = currency();
        let pending_turn = pending_turn();
        let failed_route = profile();
        for (disposition, expected) in [
            (
                FailureDisposition::ProviderUnavailable,
                FallbackPreflightOutcome::Authorized,
            ),
            (
                FailureDisposition::ProfileUnavailable,
                FallbackPreflightOutcome::Authorized,
            ),
            (
                FailureDisposition::RetryableSameRoute,
                FallbackPreflightOutcome::NoFailover,
            ),
            (
                FailureDisposition::NonFailover,
                FallbackPreflightOutcome::NoFailover,
            ),
            (
                FailureDisposition::Unclassified,
                FallbackPreflightOutcome::NoFailover,
            ),
        ] {
            assert_eq!(
                preflight(&input(
                    disposition,
                    &assignment,
                    &pool,
                    &currency,
                    &currency,
                    &pending_turn,
                    &failed_route,
                )),
                expected,
            );
        }
    }

    #[test]
    fn current_reviewer_provider_unavailability_authorizes() {
        let mut assignment = assignment();
        assignment.responsibility_key = "task:42:reviewer".into();
        assignment.role = "reviewer".into();
        assignment.review_stage = Some("r1".into());
        let pool = pool();
        let mut currency = currency();
        currency.lifecycle.status = "in-review".into();
        let pending_turn = pending_turn();
        let failed_route = profile();
        let mut reviewer_input = input(
            FailureDisposition::ProviderUnavailable,
            &assignment,
            &pool,
            &currency,
            &currency,
            &pending_turn,
            &failed_route,
        );
        reviewer_input.responsibility = AssignmentIdentity {
            task_id: Some(42),
            responsibility_key: "task:42:reviewer",
            role: "reviewer",
            pr_number: Some(9),
            review_stage: Some("r1"),
        };
        assert_eq!(
            preflight(&reviewer_input),
            FallbackPreflightOutcome::Authorized
        );
    }

    #[test]
    fn stale_currency_and_pool_generation_fail_closed_without_db_mutation() {
        let assignment = assignment();
        let pool = pool();
        let expected = currency();
        let pending_turn = pending_turn();
        let failed_route = profile();
        let temp = tempfile::tempdir().unwrap();
        let mut conn = quorum_core::db::open(&temp.path().join("quorum.db")).unwrap();
        tasks::create(
            &mut conn,
            "owner",
            "fallback preflight database sentinel",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null}"#),
            None,
            None,
            1,
        )
        .unwrap();
        let unrelated_claim = match claims::claim(&mut conn, "other", "pr#9", 100, 1).unwrap() {
            ClaimOutcome::Won(claim) => claim,
            ClaimOutcome::Lost { .. } => unreachable!("fresh claim must win"),
        };
        let snapshot = || {
            conn.query_row(
                "SELECT (SELECT count(*) FROM tasks),
                        (SELECT count(*) FROM claims),
                        (SELECT count(*) FROM role_assignments),
                        (SELECT count(*) FROM routing_attempts)",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap()
        };
        let before = snapshot();
        assert_eq!(before.0, 1);
        assert_eq!(before.1, 1);

        let mut unrelated_lease = expected.clone();
        unrelated_lease.lease.claim_id = unrelated_claim.id;
        unrelated_lease.lease.target = unrelated_claim.target;
        unrelated_lease.lease.holder = unrelated_claim.holder;
        unrelated_lease.lease.expires_at = unrelated_claim.expires_at;
        assert_eq!(
            preflight(&input(
                FailureDisposition::ProviderUnavailable,
                &assignment,
                &pool,
                &unrelated_lease,
                &unrelated_lease,
                &pending_turn,
                &failed_route,
            )),
            FallbackPreflightOutcome::FailClosed,
        );
        assert_eq!(snapshot(), before);

        let mut stale_lifecycle = expected.clone();
        stale_lifecycle.lifecycle.generation += 1;
        let mut stale_lease = expected.clone();
        stale_lease.lease.claim_id += 1;
        let mut stale_head = expected.clone();
        stale_head.head.as_mut().unwrap().head_sha = "head-b".into();
        for current in [&stale_lifecycle, &stale_lease, &stale_head] {
            assert_eq!(
                preflight(&input(
                    FailureDisposition::ProviderUnavailable,
                    &assignment,
                    &pool,
                    &expected,
                    current,
                    &pending_turn,
                    &failed_route,
                )),
                FallbackPreflightOutcome::FailClosed,
            );
            assert_eq!(snapshot(), before);
        }

        let mut mismatched_pool = pool.clone();
        mismatched_pool.policy_generation = "generation-b".into();
        assert_eq!(
            preflight(&input(
                FailureDisposition::ProviderUnavailable,
                &assignment,
                &mismatched_pool,
                &expected,
                &expected,
                &pending_turn,
                &failed_route,
            )),
            FallbackPreflightOutcome::FailClosed,
        );
        assert_eq!(snapshot(), before);

        let mut expired_lease = input(
            FailureDisposition::ProviderUnavailable,
            &assignment,
            &pool,
            &expected,
            &expected,
            &pending_turn,
            &failed_route,
        );
        expired_lease.observed_at = 100;
        assert_eq!(
            preflight(&expired_lease),
            FallbackPreflightOutcome::FailClosed,
        );
        assert_eq!(snapshot(), before);
    }
}
