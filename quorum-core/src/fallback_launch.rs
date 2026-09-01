//! Restart-safe, provider-neutral fallback launch intent.
//!
//! A fallback route first receives its ordinary attributed `agent_runs` row
//! and capability. This module then persists the bounded launch descriptor in
//! the same caller-owned `BEGIN IMMEDIATE` transaction, before any provider
//! launch. Recovery reads that descriptor verbatim; it never reconstructs a
//! launch from a failed provider continuation.

use crate::agent_runs;
use crate::capabilities;
use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use crate::role_assignments::ModelProfile;
use crate::routing_attempts::{self, FailureDisposition, ValidatedFallbackAttribution};
use crate::runner_state::PendingTurn;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

pub const MAX_RESPONSIBILITY_BYTES: usize = 1024;
pub const MAX_WORKTREE_BYTES: usize = 4096;
pub const MAX_CAPABILITY_RUN_ID_BYTES: usize = 1024;
pub const MAX_PENDING_TURN_JSON_BYTES: usize = 64 * 1024;

/// The exact PR revision a fallback launch is authorized to inspect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrHead {
    pub number: i64,
    pub head_sha: String,
}

/// The durable pending-turn descriptor for a fresh fallback launch.
///
/// This intentionally does not have a continuation-id field. A provider
/// continuation is valid only for the provider that issued it, so retaining a
/// failed-provider continuation would let restart recovery fork or corrupt a
/// fallback conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingManagedTurn {
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub prompt: String,
    pub turn_kind: String,
    #[serde(default)]
    pub requested: bool,
}

impl PendingManagedTurn {
    /// Preserve the managed turn's exact prompt/kind/requested identity while
    /// replacing the failed route's executable profile with the selected
    /// fallback profile. A continuation is deliberately not copied.
    pub fn for_fallback_profile(turn: &PendingTurn, profile: &ModelProfile) -> Self {
        Self {
            provider: profile.provider.clone(),
            model: profile.model.clone(),
            effort: profile.effort.clone(),
            prompt: turn.prompt.clone(),
            turn_kind: turn.turn_kind.clone(),
            requested: turn.requested,
        }
    }
}

/// The exact immutable descriptor recovery needs to launch an already
/// attributed fallback route. It contains no provider-session identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackLaunchIntent {
    pub id: i64,
    pub responsibility_key: String,
    pub routing_attempt_id: i64,
    pub task_id: i64,
    pub role: String,
    pub worktree: String,
    pub pr_head: Option<PrHead>,
    pub pending_turn: PendingManagedTurn,
    pub agent_run_id: i64,
    pub capability_run_id: String,
    pub created_at: i64,
}

/// All immutable evidence that must agree before a fallback launch intent can
/// be recorded. `attribution`, `agent_run_id`, and `capability_run_id` come
/// from the existing routing, `agent_runs`, and capability APIs; this module
/// only references and rechecks that evidence rather than recreating it.
#[derive(Debug, Clone)]
pub struct FallbackLaunchInput<'a> {
    pub attribution: &'a ValidatedFallbackAttribution,
    pub routing_attempt_id: i64,
    pub worktree: &'a str,
    pub pr_head: Option<&'a PrHead>,
    pub pending_turn: &'a PendingManagedTurn,
    pub agent_run_id: i64,
    pub capability_run_id: &'a str,
    pub created_at: i64,
}

/// Persist one fallback launch intent under a new `BEGIN IMMEDIATE`
/// transaction. Replaying the same immutable descriptor returns the original
/// row, including its original `created_at` value.
pub fn persist(
    conn: &mut Connection,
    input: &FallbackLaunchInput<'_>,
) -> Result<FallbackLaunchIntent> {
    let tx = begin_immediate(conn)?;
    let intent = persist_tx(&tx, input)?;
    tx.commit()?;
    Ok(intent)
}

/// Transactional form of [`persist`].
///
/// The caller owns the `BEGIN IMMEDIATE` transaction so it can create the
/// attributed `agent_runs` row, issue its capability, and record this intent
/// atomically before making any provider call.
pub fn persist_tx(
    tx: &Transaction<'_>,
    input: &FallbackLaunchInput<'_>,
) -> Result<FallbackLaunchIntent> {
    let task_id = validate_input(input)?;

    if let Some(existing) = reconstruct(
        tx,
        input.attribution.responsibility_key(),
        input.routing_attempt_id,
    )? {
        if same_descriptor(&existing, input, task_id) {
            return Ok(existing);
        }
        return Err(QuorumError::Io(
            "fallback launch intent replay conflicts with persisted evidence".into(),
        ));
    }

    validate_attribution_reference(tx, input, task_id)?;
    let pending_turn_json = encode_pending_turn(input.pending_turn)?;
    let pr_number = input.pr_head.map(|head| head.number);
    let head_sha = input.pr_head.map(|head| head.head_sha.as_str());

    tx.execute(
        "INSERT INTO fallback_launch_intents(
             responsibility_key,routing_attempt_id,task_id,role,worktree,
             pr_number,head_sha,pending_turn_json,agent_run_id,capability_run_id,created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            input.attribution.responsibility_key(),
            input.routing_attempt_id,
            task_id,
            input.attribution.role(),
            input.worktree,
            pr_number,
            head_sha,
            pending_turn_json,
            input.agent_run_id,
            input.capability_run_id,
            input.created_at,
        ],
    )?;

    Ok(FallbackLaunchIntent {
        id: tx.last_insert_rowid(),
        responsibility_key: input.attribution.responsibility_key().to_string(),
        routing_attempt_id: input.routing_attempt_id,
        task_id,
        role: input.attribution.role().to_string(),
        worktree: input.worktree.to_string(),
        pr_head: input.pr_head.cloned(),
        pending_turn: input.pending_turn.clone(),
        agent_run_id: input.agent_run_id,
        capability_run_id: input.capability_run_id.to_string(),
        created_at: input.created_at,
    })
}

/// Read the exact persisted descriptor for one responsibility/failure
/// attempt. This is the recovery/replay reader; it never derives a substitute
/// worktree, PR revision, pending turn, or provider continuation.
pub fn reconstruct(
    conn: &Connection,
    responsibility_key: &str,
    routing_attempt_id: i64,
) -> Result<Option<FallbackLaunchIntent>> {
    validate_text(
        "fallback responsibility",
        responsibility_key,
        MAX_RESPONSIBILITY_BYTES,
    )?;
    if routing_attempt_id <= 0 {
        return Err(QuorumError::Usage(
            "fallback routing attempt must be positive".into(),
        ));
    }

    let row = conn
        .query_row(
            "SELECT id,responsibility_key,routing_attempt_id,task_id,role,worktree,
                    pr_number,head_sha,pending_turn_json,agent_run_id,capability_run_id,created_at
             FROM fallback_launch_intents
             WHERE responsibility_key=?1 AND routing_attempt_id=?2",
            params![responsibility_key, routing_attempt_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, i64>(11)?,
                ))
            },
        )
        .optional()?;
    let Some((
        id,
        responsibility_key,
        routing_attempt_id,
        task_id,
        role,
        worktree,
        pr_number,
        head_sha,
        pending_turn_json,
        agent_run_id,
        capability_run_id,
        created_at,
    )) = row
    else {
        return Ok(None);
    };

    let pending_turn = decode_pending_turn(&pending_turn_json)?;
    let pr_head = match (pr_number, head_sha) {
        (None, None) => None,
        (Some(number), Some(head_sha)) => Some(PrHead { number, head_sha }),
        _ => {
            return Err(QuorumError::Io(
                "stored fallback launch intent has partial PR identity".into(),
            ))
        }
    };
    let intent = FallbackLaunchIntent {
        id,
        responsibility_key,
        routing_attempt_id,
        task_id,
        role,
        worktree,
        pr_head,
        pending_turn,
        agent_run_id,
        capability_run_id,
        created_at,
    };
    validate_stored_intent(&intent)?;
    Ok(Some(intent))
}

/// Alias for recovery callers that name the operation rather than its storage
/// reconstruction.
pub fn replay(
    conn: &Connection,
    responsibility_key: &str,
    routing_attempt_id: i64,
) -> Result<Option<FallbackLaunchIntent>> {
    reconstruct(conn, responsibility_key, routing_attempt_id)
}

fn validate_input(input: &FallbackLaunchInput<'_>) -> Result<i64> {
    let task_id = input.attribution.task_id().ok_or_else(|| {
        QuorumError::Usage("fallback launch intent requires a task-scoped attribution".into())
    })?;
    if task_id <= 0
        || input.routing_attempt_id <= 0
        || input.agent_run_id <= 0
        || input.created_at < 0
        || !matches!(input.attribution.role(), "worker" | "reviewer")
    {
        return Err(QuorumError::Usage(
            "invalid fallback launch intent identity".into(),
        ));
    }
    validate_text(
        "fallback responsibility",
        input.attribution.responsibility_key(),
        MAX_RESPONSIBILITY_BYTES,
    )?;
    validate_text("fallback worktree", input.worktree, MAX_WORKTREE_BYTES)?;
    validate_text(
        "fallback capability run id",
        input.capability_run_id,
        MAX_CAPABILITY_RUN_ID_BYTES,
    )?;
    validate_pending_turn(input.pending_turn)?;
    validate_pr_head(input.pr_head)?;

    let profile = input.attribution.profile();
    if input.pending_turn.provider != profile.provider
        || input.pending_turn.model != profile.model
        || input.pending_turn.effort != profile.effort
    {
        return Err(QuorumError::Usage(
            "fallback pending turn must use the attributed fallback profile".into(),
        ));
    }

    if input.attribution.role() == "reviewer"
        && input.attribution.pr_number() != input.pr_head.map(|head| head.number)
    {
        return Err(QuorumError::Usage(
            "reviewer fallback launch must retain its assigned PR identity".into(),
        ));
    }
    Ok(task_id)
}

fn validate_attribution_reference(
    tx: &Transaction<'_>,
    input: &FallbackLaunchInput<'_>,
    task_id: i64,
) -> Result<()> {
    let routing_attempt = routing_attempts::list(tx, input.attribution.responsibility_key())?
        .into_iter()
        .find(|attempt| attempt.id == input.routing_attempt_id)
        .ok_or_else(|| QuorumError::Io("fallback routing attempt is missing".into()))?;
    if routing_attempt.role_assignment_id != input.attribution.assignment_id()
        || routing_attempt.responsibility_key != input.attribution.responsibility_key()
        || !matches!(
            routing_attempt.failure_disposition,
            Some(FailureDisposition::ProviderUnavailable | FailureDisposition::ProfileUnavailable)
        )
    {
        return Err(QuorumError::Io(
            "fallback routing attempt does not authorize the attributed route".into(),
        ));
    }

    let attributed_run = agent_runs::fetch_configured_route(
        tx,
        input.attribution.assignment_id(),
        &input.attribution.profile().id,
    )?
    .ok_or_else(|| QuorumError::Io("attributed fallback run is missing".into()))?;
    let attributed_run_task_id: i64 = tx.query_row(
        "SELECT task_id FROM agent_runs WHERE id=?1",
        [attributed_run.id],
        |row| row.get(0),
    )?;
    if attributed_run.id != input.agent_run_id
        || attributed_run_task_id != task_id
        || attributed_run.role != input.attribution.role()
    {
        return Err(QuorumError::Io(
            "fallback launch run does not match attributed route".into(),
        ));
    }

    let capability = capabilities::validate(
        tx,
        input.capability_run_id,
        &attributed_run.agent,
        input.attribution.role(),
        Some(task_id),
    )?;
    if capability.agent_run_id != Some(input.agent_run_id) {
        return Err(QuorumError::Io(
            "fallback launch capability does not bind the attributed run".into(),
        ));
    }
    Ok(())
}

fn same_descriptor(
    existing: &FallbackLaunchIntent,
    input: &FallbackLaunchInput<'_>,
    task_id: i64,
) -> bool {
    existing.responsibility_key == input.attribution.responsibility_key()
        && existing.routing_attempt_id == input.routing_attempt_id
        && existing.task_id == task_id
        && existing.role == input.attribution.role()
        && existing.worktree == input.worktree
        && existing.pr_head == input.pr_head.cloned()
        && existing.pending_turn == *input.pending_turn
        && existing.agent_run_id == input.agent_run_id
        && existing.capability_run_id == input.capability_run_id
}

fn validate_stored_intent(intent: &FallbackLaunchIntent) -> Result<()> {
    if intent.id <= 0
        || intent.routing_attempt_id <= 0
        || intent.task_id <= 0
        || intent.agent_run_id <= 0
        || intent.created_at < 0
        || !matches!(intent.role.as_str(), "worker" | "reviewer")
    {
        return Err(QuorumError::Io(
            "stored fallback launch intent has invalid identity".into(),
        ));
    }
    validate_text(
        "stored fallback responsibility",
        &intent.responsibility_key,
        MAX_RESPONSIBILITY_BYTES,
    )
    .map_err(|_| QuorumError::Io("stored fallback launch intent is invalid".into()))?;
    validate_text(
        "stored fallback worktree",
        &intent.worktree,
        MAX_WORKTREE_BYTES,
    )
    .map_err(|_| QuorumError::Io("stored fallback launch intent is invalid".into()))?;
    validate_text(
        "stored fallback capability run id",
        &intent.capability_run_id,
        MAX_CAPABILITY_RUN_ID_BYTES,
    )
    .map_err(|_| QuorumError::Io("stored fallback launch intent is invalid".into()))?;
    validate_pr_head(intent.pr_head.as_ref())
        .map_err(|_| QuorumError::Io("stored fallback launch intent is invalid".into()))?;
    validate_pending_turn(&intent.pending_turn)
        .map_err(|_| QuorumError::Io("stored fallback launch intent is invalid".into()))?;
    Ok(())
}

fn validate_pr_head(pr_head: Option<&PrHead>) -> Result<()> {
    let Some(pr_head) = pr_head else {
        return Ok(());
    };
    if pr_head.number <= 0
        || pr_head.head_sha.len() != 40
        || !pr_head
            .head_sha
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(QuorumError::Usage("invalid fallback PR head".into()));
    }
    Ok(())
}

fn validate_pending_turn(turn: &PendingManagedTurn) -> Result<()> {
    for (label, value, max) in [
        ("provider", turn.provider.as_str(), MAX_RESPONSIBILITY_BYTES),
        ("model", turn.model.as_str(), MAX_RESPONSIBILITY_BYTES),
        ("effort", turn.effort.as_str(), MAX_RESPONSIBILITY_BYTES),
        ("prompt", turn.prompt.as_str(), MAX_PENDING_TURN_JSON_BYTES),
        (
            "turn kind",
            turn.turn_kind.as_str(),
            MAX_RESPONSIBILITY_BYTES,
        ),
    ] {
        validate_text(label, value, max)?;
    }
    let encoded = serde_json::to_string(turn)
        .map_err(|_| QuorumError::Usage("fallback pending turn cannot be serialized".into()))?;
    if encoded.len() > MAX_PENDING_TURN_JSON_BYTES {
        return Err(QuorumError::Usage(
            "fallback pending turn exceeds the storage bound".into(),
        ));
    }
    Ok(())
}

fn encode_pending_turn(turn: &PendingManagedTurn) -> Result<String> {
    validate_pending_turn(turn)?;
    serde_json::to_string(turn)
        .map_err(|_| QuorumError::Usage("fallback pending turn cannot be serialized".into()))
}

fn decode_pending_turn(encoded: &str) -> Result<PendingManagedTurn> {
    if encoded.len() > MAX_PENDING_TURN_JSON_BYTES {
        return Err(QuorumError::Io(
            "stored fallback pending turn exceeds the storage bound".into(),
        ));
    }
    let turn = serde_json::from_str::<PendingManagedTurn>(encoded)
        .map_err(|_| QuorumError::Io("stored fallback pending turn is invalid".into()))?;
    validate_pending_turn(&turn)
        .map_err(|_| QuorumError::Io("stored fallback pending turn is invalid".into()))?;
    Ok(turn)
}

fn validate_text(label: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty() || value.len() > max_bytes || value.contains('\0') {
        return Err(QuorumError::Usage(format!("invalid {label}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_runs;
    use crate::capabilities;
    use crate::role_assignments::{self, AssignmentIdentity, ValidatedPool, WeightedProfile};
    use crate::routing_attempts::{
        self, FailureDisposition, FallbackAttributionInput, RecordRoutingAttempt,
    };
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};

    fn pool() -> ValidatedPool {
        ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: "fallback-launch-test".into(),
            profiles: vec![
                WeightedProfile {
                    profile: ModelProfile {
                        id: "failed-route".into(),
                        provider: "claude".into(),
                        runner: "claude".into(),
                        model: "claude-opus".into(),
                        effort: "high".into(),
                    },
                    percent: 50,
                },
                WeightedProfile {
                    profile: ModelProfile {
                        id: "fallback-route".into(),
                        provider: "codex".into(),
                        runner: "codex".into(),
                        model: "gpt-5.6-sol".into(),
                        effort: "high".into(),
                    },
                    percent: 50,
                },
            ],
        }
    }

    fn prepare(conn: &mut Connection) -> (ValidatedFallbackAttribution, i64, i64, String) {
        let pool = pool();
        let responsibility = "worker:task:7:revision:1";
        conn.execute(
            "INSERT INTO tasks(id,title,status,assignee,created_by,created_at,updated_at)
             VALUES (7,'fallback launch fixture','working','Fallback','test',1,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,role,complexity,profile_id,provider,runner,
                 model,effort,pool_key,policy_generation,created_at)
             VALUES (11,?1,7,'worker','M','failed-route','claude','claude',
                     'claude-opus','high','worker.M','fallback-launch-test',1)",
            [responsibility],
        )
        .unwrap();
        let attempt = routing_attempts::record(
            conn,
            &RecordRoutingAttempt {
                role_assignment_id: 11,
                responsibility_key: responsibility,
                profile: &pool.profiles[0].profile,
                failure_disposition: Some(FailureDisposition::ProviderUnavailable),
                recorded_at: 10,
            },
            &pool,
        )
        .unwrap()
        .attempt()
        .clone();
        let assignment = role_assignments::get(conn, 11).unwrap().unwrap();
        let exclusions = routing_attempts::exclusions(conn, responsibility).unwrap();
        let token = routing_attempts::validate_fallback_attribution(&FallbackAttributionInput {
            assignment: &assignment,
            identity: AssignmentIdentity {
                task_id: Some(7),
                responsibility_key: responsibility,
                role: "worker",
                pr_number: None,
                review_stage: None,
            },
            eligible_pool: &pool,
            attempt: &attempt,
            exclusions: &exclusions,
            selected_profile: &pool.profiles[1].profile,
        })
        .unwrap()
        .unwrap();
        let tx = begin_immediate(conn).unwrap();
        let agent_run_id =
            agent_runs::insert_alternate_with_attribution_tx(&tx, &token, "Fallback", 11).unwrap();
        let capability = capabilities::issue_attributed_alternate_tx(
            &tx,
            &token,
            agent_run_id,
            "fallback-capability",
            "Fallback",
            None,
            12,
        )
        .unwrap()
        .unwrap();
        tx.commit().unwrap();
        (token, attempt.id, agent_run_id, capability.run_id)
    }

    fn pending_turn() -> PendingManagedTurn {
        PendingManagedTurn {
            provider: "codex".into(),
            model: "gpt-5.6-sol".into(),
            effort: "high".into(),
            prompt: "finish the exact managed turn".into(),
            turn_kind: "rework".into(),
            requested: true,
        }
    }

    #[test]
    fn persists_and_reconstructs_the_exact_continuation_free_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("fallback-launch.db")).unwrap();
        let (token, attempt_id, agent_run_id, capability_run_id) = prepare(&mut conn);
        let pending_turn = pending_turn();
        let input = FallbackLaunchInput {
            attribution: &token,
            routing_attempt_id: attempt_id,
            worktree: "/tmp/quorum-wt/fallback-launch",
            pr_head: None,
            pending_turn: &pending_turn,
            agent_run_id,
            capability_run_id: &capability_run_id,
            created_at: 13,
        };
        let intent = persist(&mut conn, &input).unwrap();
        let reconstructed = reconstruct(&conn, token.responsibility_key(), attempt_id)
            .unwrap()
            .unwrap();
        assert_eq!(reconstructed, intent);
        assert_eq!(reconstructed.task_id, 7);
        assert_eq!(reconstructed.role, "worker");
        assert_eq!(reconstructed.worktree, "/tmp/quorum-wt/fallback-launch");
        assert_eq!(reconstructed.pending_turn, pending_turn);
        let stored_json: String = conn
            .query_row(
                "SELECT pending_turn_json FROM fallback_launch_intents WHERE id=?1",
                [intent.id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!stored_json.contains("continuation_id"));
    }

    #[test]
    fn pending_turn_conversion_drops_a_failed_provider_continuation() {
        let source = PendingTurn {
            provider: "codex".into(),
            model: "gpt-5.6-sol".into(),
            effort: "high".into(),
            prompt: "retry exact work".into(),
            turn_kind: "rework".into(),
            continuation_id: Some("failed-provider-thread".into()),
            requested: true,
        };
        let fallback_profile = ModelProfile {
            id: "fallback-route".into(),
            provider: "claude".into(),
            runner: "claude".into(),
            model: "claude-sonnet".into(),
            effort: "medium".into(),
        };
        let descriptor = PendingManagedTurn::for_fallback_profile(&source, &fallback_profile);
        let encoded = encode_pending_turn(&descriptor).unwrap();
        assert!(!encoded.contains("failed-provider-thread"));
        assert!(!encoded.contains("continuation_id"));
        assert_eq!(descriptor.provider, "claude");
        assert_eq!(descriptor.model, "claude-sonnet");
    }

    #[test]
    fn replay_returns_the_original_intent_and_rejects_conflicting_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("fallback-replay.db")).unwrap();
        let (token, attempt_id, agent_run_id, capability_run_id) = prepare(&mut conn);
        let pending_turn = pending_turn();
        let first = persist(
            &mut conn,
            &FallbackLaunchInput {
                attribution: &token,
                routing_attempt_id: attempt_id,
                worktree: "/tmp/quorum-wt/fallback-replay",
                pr_head: None,
                pending_turn: &pending_turn,
                agent_run_id,
                capability_run_id: &capability_run_id,
                created_at: 13,
            },
        )
        .unwrap();
        let replay = persist(
            &mut conn,
            &FallbackLaunchInput {
                attribution: &token,
                routing_attempt_id: attempt_id,
                worktree: "/tmp/quorum-wt/fallback-replay",
                pr_head: None,
                pending_turn: &pending_turn,
                agent_run_id,
                capability_run_id: &capability_run_id,
                created_at: 99,
            },
        )
        .unwrap();
        assert_eq!(replay, first, "replay preserves the original durable row");

        let conflict = persist(
            &mut conn,
            &FallbackLaunchInput {
                attribution: &token,
                routing_attempt_id: attempt_id,
                worktree: "/tmp/quorum-wt/other",
                pr_head: None,
                pending_turn: &pending_turn,
                agent_run_id,
                capability_run_id: &capability_run_id,
                created_at: 100,
            },
        );
        assert!(conflict.is_err());
    }

    #[test]
    fn concurrent_persists_converge_on_one_intent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fallback-concurrent.db");
        let (token, attempt_id, agent_run_id, capability_run_id) = {
            let mut conn = crate::db::open(&path).unwrap();
            prepare(&mut conn)
        };
        let barrier = Arc::new(Barrier::new(8));
        let handles = (0..8)
            .map(|_| {
                let path = path.clone();
                let token = token.clone();
                let capability_run_id = capability_run_id.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    let pending_turn = pending_turn();
                    barrier.wait();
                    persist(
                        &mut conn,
                        &FallbackLaunchInput {
                            attribution: &token,
                            routing_attempt_id: attempt_id,
                            worktree: "/tmp/quorum-wt/fallback-concurrent",
                            pr_head: None,
                            pending_turn: &pending_turn,
                            agent_run_id,
                            capability_run_id: &capability_run_id,
                            created_at: 13,
                        },
                    )
                    .unwrap()
                    .id
                })
            })
            .collect::<Vec<_>>();
        let ids = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 1);
        let conn = crate::db::open(&path).unwrap();
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM fallback_launch_intents", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 1);
    }
}
