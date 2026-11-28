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
    remove_keys(refs, RETRY_STATE_REFS);
}

/// Neutral and historical Codex keys that together hold one blocked or
/// requested pending turn.
const RETRY_STATE_REFS: &[&str] = &[
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
];

/// Drop a pending turn that the selected runner cannot resume. Unlike
/// [`clear_provider_retry`], the worker continuation is discarded too, in both
/// its neutral and historical Codex forms: the next worker turn must start
/// fresh rather than resume a conversation owned by the dropped provider.
pub fn discard_provider_retry(refs: &mut Value) {
    remove_keys(refs, RETRY_STATE_REFS);
    remove_keys(
        refs,
        &[
            ContinuationSlot::Worker.ref_key(),
            ContinuationSlot::Worker.legacy_codex_ref_key(),
        ],
    );
}

/// Task ref holding one external-runner session record per slot (spec
/// "Session record"). It is separate from the `runner_continuation*` refs and
/// is written only through the `external_session` helpers below.
pub const EXTERNAL_SESSIONS_REF: &str = "runner_external_sessions";

/// One slot's external-runner session record. Metadata only: it never holds
/// prompts, CLI output, or session file contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalSessionEntry {
    pub profile: String,
    pub fingerprint: String,
    pub usable: bool,
}

impl ContinuationSlot {
    /// Slot name used as the `runner_external_sessions` key and session
    /// directory component.
    pub fn slot_name(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::ReviewerR1 => "reviewer-r1",
            Self::ReviewerR2 => "reviewer-r2",
        }
    }
}

/// Read a slot's entry. A malformed or partial entry reads as absent, so the
/// resume decision fails closed to a fresh start.
pub fn external_session(refs: &Value, slot: ContinuationSlot) -> Option<ExternalSessionEntry> {
    let entry: ExternalSessionEntry = serde_json::from_value(
        refs.get(EXTERNAL_SESSIONS_REF)?
            .get(slot.slot_name())?
            .clone(),
    )
    .ok()?;
    (!entry.profile.is_empty() && !entry.fingerprint.is_empty()).then_some(entry)
}

/// Write or replace a slot's entry as usable. Other slots' entries are kept;
/// a non-object ref value is replaced.
pub fn set_external_session(
    refs: &mut Value,
    slot: ContinuationSlot,
    profile: &str,
    fingerprint: &str,
) {
    let entry = ExternalSessionEntry {
        profile: profile.into(),
        fingerprint: fingerprint.into(),
        usable: true,
    };
    if !refs
        .get(EXTERNAL_SESSIONS_REF)
        .is_some_and(Value::is_object)
    {
        refs[EXTERNAL_SESSIONS_REF] = Value::Object(serde_json::Map::new());
    }
    refs[EXTERNAL_SESSIONS_REF][slot.slot_name()] =
        serde_json::to_value(entry).expect("external session entry is serializable");
}

/// Mark a slot's entry `usable: false`. Returns whether an entry was marked.
pub fn mark_external_session_unusable(refs: &mut Value, slot: ContinuationSlot) -> bool {
    match refs
        .get_mut(EXTERNAL_SESSIONS_REF)
        .and_then(|sessions| sessions.get_mut(slot.slot_name()))
        .and_then(Value::as_object_mut)
    {
        Some(entry) => {
            entry.insert("usable".into(), Value::Bool(false));
            true
        }
        None => false,
    }
}

/// Remove a slot's entry; the ref itself is dropped once no slot remains.
pub fn remove_external_session(refs: &mut Value, slot: ContinuationSlot) {
    let Some(sessions) = refs
        .get_mut(EXTERNAL_SESSIONS_REF)
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    sessions.remove(slot.slot_name());
    if sessions.is_empty() {
        remove_keys(refs, &[EXTERNAL_SESSIONS_REF]);
    }
}

/// Remove a slot's continuation, in both its neutral and historical Codex
/// forms, when the slot launches on the external runner. Removing only the
/// neutral record would let the legacy reader fall back to a stale thread.
pub fn remove_slot_continuation(refs: &mut Value, slot: ContinuationSlot) {
    remove_keys(refs, &[slot.ref_key(), slot.legacy_codex_ref_key()]);
}

/// `fstatat(AT_SYMLINK_NOFOLLOW)` classification of a slot's session file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionFileState {
    Missing,
    /// Not a regular file (directory, symlink, or other).
    NonRegular,
    Empty,
    NonEmptyRegular,
}

impl SessionFileState {
    /// Classify no-follow metadata; `None` means the file does not exist.
    pub fn from_metadata(metadata: Option<&std::fs::Metadata>) -> Self {
        match metadata {
            None => Self::Missing,
            Some(m) if !m.file_type().is_file() => Self::NonRegular,
            Some(m) if m.len() == 0 => Self::Empty,
            Some(_) => Self::NonEmptyRegular,
        }
    }
}

/// What kind of run is launching on a slot (spec "Resume or fresh start",
/// condition 4). Continued turns carry the turn text Quorum feeds the
/// built-in turn-oriented runners today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalRunKind<'a> {
    /// Admission with `pending_resume.kind = "answer"`.
    AnsweredResume {
        identity: &'a str,
        text: &'a str,
    },
    /// Admission with `pending_resume.kind = "unanswered"`.
    UnansweredResume,
    Rework {
        turn_text: &'a str,
    },
    ReviewRemediation {
        turn_text: &'a str,
    },
    ReviewerContinuation {
        turn_text: &'a str,
    },
    ProviderRetry {
        turn_text: &'a str,
    },
    RebuiltDormantTurn {
        turn_text: &'a str,
    },
    /// Any other run: first run of a slot, a task reopened after
    /// `AgentFailed` or a stale lease, a restart without in-place rebuild.
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeDecision {
    Resume,
    Fresh,
}

#[derive(Debug, Clone, Copy)]
pub struct ResumeInputs<'a> {
    pub sessions_enabled: bool,
    pub slot: Option<ContinuationSlot>,
    pub entry: Option<&'a ExternalSessionEntry>,
    pub routed_profile: &'a str,
    pub fingerprint: &'a str,
    pub session_file: SessionFileState,
    pub run: ExternalRunKind<'a>,
}

/// Resume only when all four spec conditions hold; every other run is fresh.
pub fn resume_decision(inputs: &ResumeInputs<'_>) -> ResumeDecision {
    let has_slot = inputs.sessions_enabled && inputs.slot.is_some();
    let entry_matches = inputs.entry.is_some_and(|entry| {
        entry.usable
            && !inputs.routed_profile.is_empty()
            && !inputs.fingerprint.is_empty()
            && entry.profile == inputs.routed_profile
            && entry.fingerprint == inputs.fingerprint
    });
    let file_resumable = inputs.session_file == SessionFileState::NonEmptyRegular;
    let run_resumable = resumed_prompt(&inputs.run).is_some();
    if has_slot && entry_matches && file_resumable && run_resumable {
        ResumeDecision::Resume
    } else {
        ResumeDecision::Fresh
    }
}

/// Prompt for a resumed run (spec "Resumed-run prompts"). `None` for runs
/// that never resume; a fresh start uses the caller's full role prompt.
pub fn resumed_prompt(run: &ExternalRunKind<'_>) -> Option<String> {
    match *run {
        ExternalRunKind::AnsweredResume { identity, text } => {
            Some(format!("Answer from {identity}:\n{text}"))
        }
        ExternalRunKind::UnansweredResume => {
            Some("No answer was provided. Continue with your best judgment, or ask again.".into())
        }
        ExternalRunKind::Rework { turn_text }
        | ExternalRunKind::ReviewRemediation { turn_text }
        | ExternalRunKind::ReviewerContinuation { turn_text }
        | ExternalRunKind::ProviderRetry { turn_text }
        | ExternalRunKind::RebuiltDormantTurn { turn_text } => Some(turn_text.into()),
        ExternalRunKind::Other => None,
    }
}

fn remove_keys(refs: &mut Value, keys: &[&str]) {
    let Some(object) = refs.as_object_mut() else {
        return;
    };
    for key in keys {
        object.remove(*key);
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
    fn discard_drops_both_retry_representations_and_worker_continuation() {
        let mut refs = serde_json::json!({
            "pr": 7,
            RETRY_REF: {
                "provider": "codex", "model": "gpt-5", "effort": "high",
                "prompt": "finish", "turn_kind": "rework",
                "continuation_id": "thread-new", "requested": true
            },
            CONTINUATION_REF: {"provider": "codex", "id": "thread-new"},
            REVIEWER_R1_CONTINUATION_REF: {"provider": "claude", "id": "r1"},
            PROVIDER_BLOCK_REF: {"provider": "codex", "reason": "quota"},
            "codex_thread_id": "thread-old",
            "codex_reviewer_r1_thread_id": "r1-old",
            "codex_provider_blocked": true,
            "codex_provider_error": "quota",
            "codex_retry_requested": true,
            "codex_retry_model": "gpt-5",
            "codex_retry_effort": "high",
            "codex_retry_prompt": "finish",
            "codex_retry_turn_kind": "rework",
            "codex_retry_thread_id": "thread-old"
        });
        discard_provider_retry(&mut refs);
        assert_eq!(
            refs,
            serde_json::json!({
                "pr": 7,
                REVIEWER_R1_CONTINUATION_REF: {"provider": "claude", "id": "r1"},
                "codex_reviewer_r1_thread_id": "r1-old"
            })
        );
        assert!(requested_retry_any(&refs).is_none());
        assert!(!retry_requested(&refs));
        assert!(continuation(&refs, ContinuationSlot::Worker, "codex").is_none());
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

    const SLOTS: [ContinuationSlot; 3] = [
        ContinuationSlot::Worker,
        ContinuationSlot::ReviewerR1,
        ContinuationSlot::ReviewerR2,
    ];

    #[test]
    fn external_session_ref_round_trips_per_slot() {
        assert_eq!(EXTERNAL_SESSIONS_REF, "runner_external_sessions");
        let mut refs = serde_json::json!({"pr": 7});
        for (i, slot) in SLOTS.into_iter().enumerate() {
            set_external_session(&mut refs, slot, &format!("p{i}"), &format!("f{i}"));
        }
        assert_eq!(
            refs[EXTERNAL_SESSIONS_REF],
            serde_json::json!({
                "worker": {"profile": "p0", "fingerprint": "f0", "usable": true},
                "reviewer-r1": {"profile": "p1", "fingerprint": "f1", "usable": true},
                "reviewer-r2": {"profile": "p2", "fingerprint": "f2", "usable": true}
            })
        );
        for (i, slot) in SLOTS.into_iter().enumerate() {
            assert_eq!(
                external_session(&refs, slot),
                Some(ExternalSessionEntry {
                    profile: format!("p{i}"),
                    fingerprint: format!("f{i}"),
                    usable: true,
                })
            );
        }

        assert!(mark_external_session_unusable(
            &mut refs,
            ContinuationSlot::ReviewerR1
        ));
        assert!(
            !external_session(&refs, ContinuationSlot::ReviewerR1)
                .unwrap()
                .usable
        );
        assert!(
            external_session(&refs, ContinuationSlot::Worker)
                .unwrap()
                .usable
        );

        // Replace restores usable and the new identity.
        set_external_session(&mut refs, ContinuationSlot::ReviewerR1, "p9", "f9");
        assert_eq!(
            refs[EXTERNAL_SESSIONS_REF]["reviewer-r1"],
            serde_json::json!({"profile": "p9", "fingerprint": "f9", "usable": true})
        );

        remove_external_session(&mut refs, ContinuationSlot::Worker);
        assert!(external_session(&refs, ContinuationSlot::Worker).is_none());
        assert!(!mark_external_session_unusable(
            &mut refs,
            ContinuationSlot::Worker
        ));
        remove_external_session(&mut refs, ContinuationSlot::ReviewerR1);
        remove_external_session(&mut refs, ContinuationSlot::ReviewerR2);
        assert_eq!(refs, serde_json::json!({"pr": 7}));

        let partial = serde_json::json!({
            EXTERNAL_SESSIONS_REF: {"worker": {"profile": "p", "fingerprint": "f"}}
        });
        assert!(external_session(&partial, ContinuationSlot::Worker).is_none());
    }

    #[test]
    fn remove_slot_continuation_touches_only_that_slot() {
        let mut refs = serde_json::json!({
            CONTINUATION_REF: {"provider": "codex", "id": "w"},
            "codex_thread_id": "w-old",
            REVIEWER_R1_CONTINUATION_REF: {"provider": "claude", "id": "r1"},
            EXTERNAL_SESSIONS_REF: {"worker": {"profile": "p", "fingerprint": "f", "usable": true}}
        });
        remove_slot_continuation(&mut refs, ContinuationSlot::Worker);
        assert_eq!(
            refs,
            serde_json::json!({
                REVIEWER_R1_CONTINUATION_REF: {"provider": "claude", "id": "r1"},
                EXTERNAL_SESSIONS_REF: {"worker": {"profile": "p", "fingerprint": "f", "usable": true}}
            })
        );
        assert!(continuation(&refs, ContinuationSlot::Worker, "codex").is_none());
    }

    #[test]
    fn continuation_and_retry_helpers_leave_external_sessions_untouched() {
        let sessions = serde_json::json!({
            "worker": {"profile": "p", "fingerprint": "f", "usable": false},
            "reviewer-r2": {"profile": "q", "fingerprint": "g", "usable": true}
        });
        let mut refs = serde_json::json!({
            EXTERNAL_SESSIONS_REF: sessions.clone(),
            RETRY_REF: {
                "provider": "codex", "model": "gpt-5", "effort": "high",
                "prompt": "finish", "turn_kind": "rework",
                "continuation_id": "thread-new", "requested": true
            }
        });
        for slot in SLOTS {
            set_continuation(
                &mut refs,
                slot,
                &ContinuationIdentity {
                    provider: "codex".into(),
                    id: "t".into(),
                },
            );
        }
        assert_eq!(refs[EXTERNAL_SESSIONS_REF], sessions);
        clear_provider_retry(&mut refs);
        assert_eq!(refs[EXTERNAL_SESSIONS_REF], sessions);
        refs.as_object_mut().unwrap().remove(CONTINUATION_REF);
        refs[RETRY_REF] = serde_json::json!({
            "provider": "codex", "model": "gpt-5", "effort": "high",
            "prompt": "finish", "turn_kind": "rework",
            "continuation_id": "thread-new", "requested": true
        });
        clear_provider_retry(&mut refs);
        assert_eq!(refs[EXTERNAL_SESSIONS_REF], sessions);
        assert_eq!(refs[CONTINUATION_REF]["id"], "thread-new");
    }

    fn entry(usable: bool) -> ExternalSessionEntry {
        ExternalSessionEntry {
            profile: "prof".into(),
            fingerprint: "abc123".into(),
            usable,
        }
    }

    fn resumable<'a>(entry: &'a ExternalSessionEntry) -> ResumeInputs<'a> {
        ResumeInputs {
            sessions_enabled: true,
            slot: Some(ContinuationSlot::Worker),
            entry: Some(entry),
            routed_profile: "prof",
            fingerprint: "abc123",
            session_file: SessionFileState::NonEmptyRegular,
            run: ExternalRunKind::Rework { turn_text: "t" },
        }
    }

    #[test]
    fn resume_requires_all_four_conditions() {
        let good = entry(true);
        let unusable = entry(false);
        let base = resumable(&good);

        let resuming_runs = [
            ExternalRunKind::AnsweredResume {
                identity: "owner",
                text: "yes",
            },
            ExternalRunKind::UnansweredResume,
            ExternalRunKind::Rework { turn_text: "t" },
            ExternalRunKind::ReviewRemediation { turn_text: "t" },
            ExternalRunKind::ReviewerContinuation { turn_text: "t" },
            ExternalRunKind::ProviderRetry { turn_text: "t" },
            ExternalRunKind::RebuiltDormantTurn { turn_text: "t" },
        ];
        for run in resuming_runs {
            for slot in SLOTS {
                assert_eq!(
                    resume_decision(&ResumeInputs {
                        run,
                        slot: Some(slot),
                        ..base
                    }),
                    ResumeDecision::Resume,
                    "{run:?} {slot:?}"
                );
            }
        }

        let fresh_cases = [
            (
                "sessions disabled",
                ResumeInputs {
                    sessions_enabled: false,
                    ..base
                },
            ),
            ("no slot", ResumeInputs { slot: None, ..base }),
            (
                "missing entry",
                ResumeInputs {
                    entry: None,
                    ..base
                },
            ),
            (
                "unusable",
                ResumeInputs {
                    entry: Some(&unusable),
                    ..base
                },
            ),
            (
                "profile change",
                ResumeInputs {
                    routed_profile: "other",
                    ..base
                },
            ),
            (
                "fingerprint change",
                ResumeInputs {
                    fingerprint: "def456",
                    ..base
                },
            ),
            (
                "empty profile",
                ResumeInputs {
                    routed_profile: "",
                    ..base
                },
            ),
            (
                "missing file",
                ResumeInputs {
                    session_file: SessionFileState::Missing,
                    ..base
                },
            ),
            (
                "zero-byte file",
                ResumeInputs {
                    session_file: SessionFileState::Empty,
                    ..base
                },
            ),
            (
                "non-regular file",
                ResumeInputs {
                    session_file: SessionFileState::NonRegular,
                    ..base
                },
            ),
            (
                "other run kind",
                ResumeInputs {
                    run: ExternalRunKind::Other,
                    ..base
                },
            ),
        ];
        for (name, inputs) in fresh_cases {
            assert_eq!(resume_decision(&inputs), ResumeDecision::Fresh, "{name}");
        }
    }

    #[test]
    fn session_file_state_classifies_no_follow_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let classify = |name: &str| {
            SessionFileState::from_metadata(
                std::fs::symlink_metadata(dir.path().join(name))
                    .ok()
                    .as_ref(),
            )
        };
        std::fs::write(dir.path().join("full"), b"x").unwrap();
        std::fs::write(dir.path().join("empty"), b"").unwrap();
        std::fs::create_dir(dir.path().join("dir")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.path().join("full"), dir.path().join("link")).unwrap();

        assert_eq!(classify("missing"), SessionFileState::Missing);
        assert_eq!(classify("empty"), SessionFileState::Empty);
        assert_eq!(classify("dir"), SessionFileState::NonRegular);
        #[cfg(unix)]
        assert_eq!(classify("link"), SessionFileState::NonRegular);
        assert_eq!(classify("full"), SessionFileState::NonEmptyRegular);
    }

    #[test]
    fn resumed_prompts_match_spec_strings_exactly() {
        assert_eq!(
            resumed_prompt(&ExternalRunKind::AnsweredResume {
                identity: "Owner-1",
                text: "use option B\nthanks",
            })
            .as_deref(),
            Some("Answer from Owner-1:\nuse option B\nthanks")
        );
        assert_eq!(
            resumed_prompt(&ExternalRunKind::UnansweredResume).as_deref(),
            Some("No answer was provided. Continue with your best judgment, or ask again.")
        );
        let turn = "Review feedback:\n- fix the race\n";
        for run in [
            ExternalRunKind::Rework { turn_text: turn },
            ExternalRunKind::ReviewRemediation { turn_text: turn },
            ExternalRunKind::ReviewerContinuation { turn_text: turn },
            ExternalRunKind::ProviderRetry { turn_text: turn },
            ExternalRunKind::RebuiltDormantTurn { turn_text: turn },
        ] {
            assert_eq!(resumed_prompt(&run).as_deref(), Some(turn), "{run:?}");
        }
        assert_eq!(resumed_prompt(&ExternalRunKind::Other), None);
    }
}
