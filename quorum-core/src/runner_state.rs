//! Durable, provider-neutral state for turn-oriented coding runners.
//!
//! Task refs are the compatibility boundary. New writes use tagged `runner_*`
//! objects; readers also accept the historical flat `codex_*` keys so an
//! upgrade never strands an interrupted Codex run.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const CONTINUATION_REF: &str = "runner_continuation";
pub const REVIEWER_R1_CONTINUATION_REF: &str = "runner_reviewer_r1_continuation";
pub const REVIEWER_R2_CONTINUATION_REF: &str = "runner_reviewer_r2_continuation";
pub const PROVIDER_BLOCK_REF: &str = "runner_provider_block";
pub const RETRY_REF: &str = "runner_retry";
pub const INITIAL_WORKER_SESSION_REF: &str = "runner_initial_worker_session";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationIdentity {
    pub provider: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTurn {
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub prompt: String,
    pub turn_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub requested: bool,
}

/// Exact durable handoff for a turn-oriented worker whose provider issues its
/// continuation identity only at terminal success. This record is written in
/// the same transaction as [`CONTINUATION_REF`], after the immutable role
/// assignment and worker run have been revalidated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitialWorkerSession {
    pub task_id: i64,
    pub role_assignment_id: i64,
    pub responsibility_key: String,
    pub agent: String,
    pub role: String,
    pub provider: String,
    pub runner: String,
    pub model: String,
    pub effort: String,
    pub pending_turn: PendingTurn,
    pub session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderBlock {
    pub provider: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationSlot {
    Worker,
    ReviewerR1,
    ReviewerR2,
}

impl ContinuationSlot {
    pub fn ref_key(self) -> &'static str {
        match self {
            Self::Worker => CONTINUATION_REF,
            Self::ReviewerR1 => REVIEWER_R1_CONTINUATION_REF,
            Self::ReviewerR2 => REVIEWER_R2_CONTINUATION_REF,
        }
    }

    fn legacy_codex_ref_key(self) -> &'static str {
        match self {
            Self::Worker => "codex_thread_id",
            Self::ReviewerR1 => "codex_reviewer_r1_thread_id",
            Self::ReviewerR2 => "codex_reviewer_r2_thread_id",
        }
    }
}

pub fn set_continuation(refs: &mut Value, slot: ContinuationSlot, identity: &ContinuationIdentity) {
    refs[slot.ref_key()] = serde_json::to_value(identity).expect("continuation is serializable");
}

pub fn set_initial_worker_session(refs: &mut Value, session: &InitialWorkerSession) {
    refs[INITIAL_WORKER_SESSION_REF] =
        serde_json::to_value(session).expect("initial worker session is serializable");
}

pub fn initial_worker_session(refs: &Value) -> Option<InitialWorkerSession> {
    let session: InitialWorkerSession =
        serde_json::from_value(refs.get(INITIAL_WORKER_SESSION_REF)?.clone()).ok()?;
    (session.task_id > 0
        && session.role_assignment_id > 0
        && !session.responsibility_key.is_empty()
        && !session.agent.is_empty()
        && session.role == "worker"
        && !session.provider.is_empty()
        && session.provider == session.runner
        && session.provider == session.pending_turn.provider
        && session.model == session.pending_turn.model
        && session.effort == session.pending_turn.effort
        && session.pending_turn.turn_kind == "initial"
        && session.pending_turn.continuation_id.is_none()
        && !session.pending_turn.requested
        && pending_turn_is_complete(&session.pending_turn)
        && !session.session_id.is_empty()
        && session.session_id.len() <= 1024
        && session.session_id.trim() == session.session_id
        && !session.session_id.chars().any(char::is_control))
    .then_some(session)
}

/// Read a continuation only when its provider matches the selected runner.
/// A present neutral record is authoritative; legacy Codex fallback is used
/// only when no neutral record has been written for this slot.
pub fn continuation(
    refs: &Value,
    slot: ContinuationSlot,
    expected_provider: &str,
) -> Option<ContinuationIdentity> {
    if let Some(value) = refs.get(slot.ref_key()) {
        let identity: ContinuationIdentity = serde_json::from_value(value.clone()).ok()?;
        return (identity.provider == expected_provider && !identity.id.is_empty())
            .then_some(identity);
    }
    if expected_provider != "codex" {
        return None;
    }
    refs.get(slot.legacy_codex_ref_key())
        .or_else(|| {
            (slot == ContinuationSlot::Worker)
                .then(|| refs.get("codex_retry_thread_id"))
                .flatten()
        })
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(|id| ContinuationIdentity {
            provider: "codex".into(),
            id: id.into(),
        })
}

pub fn set_provider_block(refs: &mut Value, block: &ProviderBlock, turn: &PendingTurn) {
    refs[PROVIDER_BLOCK_REF] = serde_json::to_value(block).expect("provider block is serializable");
    refs[RETRY_REF] = serde_json::to_value(turn).expect("pending turn is serializable");
}

pub fn provider_block(refs: &Value) -> Option<ProviderBlock> {
    if let Some(value) = refs.get(PROVIDER_BLOCK_REF) {
        let block: ProviderBlock = serde_json::from_value(value.clone()).ok()?;
        return (!block.provider.is_empty() && !block.reason.is_empty()).then_some(block);
    }
    (refs.get("codex_provider_blocked").and_then(Value::as_bool) == Some(true)).then(|| {
        ProviderBlock {
            provider: "codex".into(),
            reason: refs
                .get("codex_provider_error")
                .and_then(Value::as_str)
                .unwrap_or("Codex provider failure")
                .into(),
        }
    })
}

/// Return a complete requested retry for the selected runner. Wrong-provider
/// and partial records fail closed. Historical Codex records remain readable.
pub fn requested_retry(refs: &Value, expected_provider: &str) -> Option<PendingTurn> {
    if let Some(value) = refs.get(RETRY_REF) {
        let turn: PendingTurn = serde_json::from_value(value.clone()).ok()?;
        return (valid_pending_turn(&turn, expected_provider, true)
            && pending_turn_is_resumable(&turn))
        .then_some(turn);
    }
    if expected_provider != "codex"
        || refs.get("codex_retry_requested").and_then(Value::as_bool) != Some(true)
    {
        return None;
    }
    let turn = PendingTurn {
        provider: "codex".into(),
        model: refs.get("codex_retry_model")?.as_str()?.into(),
        effort: refs.get("codex_retry_effort")?.as_str()?.into(),
        prompt: refs.get("codex_retry_prompt")?.as_str()?.into(),
        turn_kind: refs.get("codex_retry_turn_kind")?.as_str()?.into(),
        continuation_id: refs
            .get("codex_retry_thread_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        requested: true,
    };
    (valid_pending_turn(&turn, expected_provider, true) && pending_turn_is_resumable(&turn))
        .then_some(turn)
}

pub fn requested_retry_any(refs: &Value) -> Option<PendingTurn> {
    if let Some(provider) = refs
        .get(RETRY_REF)
        .and_then(|retry| retry.get("provider"))
        .and_then(Value::as_str)
    {
        return requested_retry(refs, provider);
    }
    requested_retry(refs, "codex")
}

pub fn retry_requested(refs: &Value) -> bool {
    if let Some(retry) = refs.get(RETRY_REF) {
        return retry.get("requested").and_then(Value::as_bool) == Some(true);
    }
    refs.get("codex_retry_requested").and_then(Value::as_bool) == Some(true)
}

pub fn pending_turn_is_complete(turn: &PendingTurn) -> bool {
    valid_pending_turn(turn, &turn.provider, false)
}

/// A fresh start is valid only for the initial turn, before the provider has
/// issued a continuation identity. Every later turn must carry that identity
/// or retry would silently fork the provider conversation.
pub fn pending_turn_is_resumable(turn: &PendingTurn) -> bool {
    pending_turn_is_complete(turn)
        && (turn.turn_kind == "initial"
            || turn
                .continuation_id
                .as_deref()
                .is_some_and(|id| !id.is_empty()))
}

/// Validate and activate a blocked turn. The provider block and pending turn
/// must agree, and all exact turn identity fields must already be durable.
pub fn request_retry(refs: &mut Value) -> bool {
    if refs.get(PROVIDER_BLOCK_REF).is_some() || refs.get(RETRY_REF).is_some() {
        let Some(block) = provider_block(refs) else {
            return false;
        };
        let Some(value) = refs.get(RETRY_REF) else {
            return false;
        };
        let Ok(mut turn) = serde_json::from_value::<PendingTurn>(value.clone()) else {
            return false;
        };
        if block.provider != turn.provider
            || !valid_pending_turn(&turn, &block.provider, false)
            || !pending_turn_is_resumable(&turn)
        {
            return false;
        }
        turn.requested = true;
        refs[RETRY_REF] = serde_json::to_value(turn).expect("pending turn is serializable");
        refs.as_object_mut()
            .expect("task refs object")
            .remove(PROVIDER_BLOCK_REF);
        return true;
    }

    if refs.get("codex_provider_blocked").and_then(Value::as_bool) != Some(true) {
        return false;
    }
    let turn = PendingTurn {
        provider: "codex".into(),
        model: refs
            .get("codex_retry_model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        effort: refs
            .get("codex_retry_effort")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        prompt: refs
            .get("codex_retry_prompt")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        turn_kind: refs
            .get("codex_retry_turn_kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        continuation_id: refs
            .get("codex_retry_thread_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        requested: true,
    };
    if !pending_turn_is_resumable(&turn) {
        return false;
    }
    refs[RETRY_REF] = serde_json::to_value(turn).expect("pending turn is serializable");
    let object = refs.as_object_mut().expect("task refs object");
    object.remove("codex_provider_blocked");
    object.remove("codex_provider_error");
    true
}

pub fn clear_provider_retry(refs: &mut Value) {
    // A successful retried turn can race the provider's repeated session event.
    // Preserve the already-durable retry continuation before removing the
    // round-scoped pending turn, so later remediation still resumes exactly.
    if refs.get(CONTINUATION_REF).is_none() {
        let retry_continuation = refs
            .get(RETRY_REF)
            .and_then(|value| serde_json::from_value::<PendingTurn>(value.clone()).ok())
            .and_then(|turn| {
                turn.continuation_id.map(|id| ContinuationIdentity {
                    provider: turn.provider,
                    id,
                })
            });
        if let Some(identity) = retry_continuation {
            set_continuation(refs, ContinuationSlot::Worker, &identity);
        }
    }
    let Some(object) = refs.as_object_mut() else {
        return;
    };
    for key in [
        PROVIDER_BLOCK_REF,
        RETRY_REF,
        "codex_provider_blocked",
        "codex_provider_error",
        "codex_retry_requested",
        "codex_retry_model",
        "codex_retry_effort",
        "codex_retry_prompt",
        "codex_retry_turn_kind",
        "codex_retry_thread_id",
    ] {
        object.remove(key);
    }
}

fn valid_pending_turn(turn: &PendingTurn, provider: &str, require_requested: bool) -> bool {
    turn.provider == provider
        && !turn.provider.is_empty()
        && !turn.model.is_empty()
        && !turn.effort.is_empty()
        && !turn.prompt.is_empty()
        && !turn.turn_kind.is_empty()
        && (!require_requested || turn.requested)
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_codex_retry_and_continuation_remain_readable() {
        let mut refs = serde_json::json!({
            "codex_thread_id": "thread-old",
            "codex_retry_requested": true,
            "codex_retry_model": "gpt-5",
            "codex_retry_effort": "high",
            "codex_retry_prompt": "finish",
            "codex_retry_turn_kind": "rework",
            "codex_retry_thread_id": "thread-old"
        });
        assert_eq!(
            continuation(&refs, ContinuationSlot::Worker, "codex")
                .unwrap()
                .id,
            "thread-old"
        );
        assert_eq!(
            requested_retry(&refs, "codex")
                .unwrap()
                .continuation_id
                .as_deref(),
            Some("thread-old")
        );

        refs.as_object_mut()
            .unwrap()
            .remove("codex_retry_requested");
        refs["codex_provider_blocked"] = Value::Bool(true);
        assert!(request_retry(&mut refs));
        assert!(refs.get("codex_retry_requested").is_none());
        assert_eq!(requested_retry(&refs, "codex").unwrap().prompt, "finish");
        assert_eq!(refs["codex_thread_id"], "thread-old");
    }

    #[test]
    fn neutral_state_rejects_wrong_provider_and_partial_retry() {
        let wrong = serde_json::json!({
            RETRY_REF: {
                "provider": "codex", "model": "gpt-5", "effort": "high",
                "prompt": "finish", "turn_kind": "rework", "requested": true
            }
        });
        assert!(requested_retry(&wrong, "claude").is_none());

        let partial = serde_json::json!({
            RETRY_REF: {"provider": "codex", "model": "gpt-5", "requested": true}
        });
        assert!(requested_retry(&partial, "codex").is_none());

        let missing_required_continuation = serde_json::json!({
            RETRY_REF: {
                "provider": "codex", "model": "gpt-5", "effort": "high",
                "prompt": "finish", "turn_kind": "rework", "requested": true
            }
        });
        assert!(requested_retry(&missing_required_continuation, "codex").is_none());

        let initial_without_continuation = serde_json::json!({
            RETRY_REF: {
                "provider": "codex", "model": "gpt-5", "effort": "high",
                "prompt": "start", "turn_kind": "initial", "requested": true
            }
        });
        assert!(requested_retry(&initial_without_continuation, "codex").is_some());

        let stale_legacy_request = serde_json::json!({
            RETRY_REF: {
                "provider": "codex", "model": "gpt-5", "effort": "high",
                "prompt": "finish", "turn_kind": "rework",
                "continuation_id": "thread-new", "requested": false
            },
            "codex_retry_requested": true
        });
        assert!(!retry_requested(&stale_legacy_request));
        assert!(requested_retry_any(&stale_legacy_request).is_none());

        let continuation_refs = serde_json::json!({
            CONTINUATION_REF: {"provider": "claude", "id": "session-new"},
            "codex_thread_id": "thread-stale"
        });
        assert!(continuation(&continuation_refs, ContinuationSlot::Worker, "codex").is_none());
        assert_eq!(
            continuation(&continuation_refs, ContinuationSlot::Worker, "claude")
                .unwrap()
                .id,
            "session-new"
        );

        let mut partial_block = serde_json::json!({
            PROVIDER_BLOCK_REF: {"provider": "codex", "reason": "quota"},
            RETRY_REF: {"provider": "codex", "model": "gpt-5"}
        });
        assert!(!request_retry(&mut partial_block));

        let mut missing_continuation_block = serde_json::json!({
            PROVIDER_BLOCK_REF: {"provider": "codex", "reason": "quota"},
            RETRY_REF: {
                "provider": "codex", "model": "gpt-5", "effort": "high",
                "prompt": "finish", "turn_kind": "rework"
            }
        });
        assert!(!request_retry(&mut missing_continuation_block));
    }

    #[test]
    fn initial_worker_session_preserves_complete_exact_turn_identity() {
        let session = InitialWorkerSession {
            task_id: 459,
            role_assignment_id: 88,
            responsibility_key: "worker:task:459:revision:1".into(),
            agent: "Tread-n2so".into(),
            role: "worker".into(),
            provider: "grok".into(),
            runner: "grok".into(),
            model: "grok-4.5".into(),
            effort: "high".into(),
            pending_turn: PendingTurn {
                provider: "grok".into(),
                model: "grok-4.5".into(),
                effort: "high".into(),
                prompt: "exact initial prompt".into(),
                turn_kind: "initial".into(),
                continuation_id: None,
                requested: false,
            },
            session_id: "provider-session-exact".into(),
        };
        let mut refs = serde_json::json!({});
        set_initial_worker_session(&mut refs, &session);
        assert_eq!(initial_worker_session(&refs), Some(session.clone()));

        let mut mismatched = session;
        mismatched.pending_turn.model = "grok-other".into();
        set_initial_worker_session(&mut refs, &mismatched);
        assert!(initial_worker_session(&refs).is_none());
    }
}
