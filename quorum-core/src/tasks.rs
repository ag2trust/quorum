//! The shared work queue — lifecycle driven by the transition table in `lifecycle.rs`.
//!
//! Tasks walk through states defined in `lifecycle::Status`: open → working →
//! in-review → merging → done, with a rework loop (in-review ⇄ rework).
//! Terminal states: done, failed, cancelled.
//!
//! Every status change goes: build TaskView → build Event → lifecycle::transition()
//! → persist new status + execute DB-side effects, in one transaction. Process-side
//! effects are returned to the caller.

use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use crate::lifecycle::{Effect, Event, Status, TaskView};
use crate::runner_state::{self, PendingTurn, ProviderBlock};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction};
use serde::Serialize;
use std::process::Command;

pub const STATUSES: &[&str] = &[
    "open",
    "working",
    "in-review",
    "rework",
    "merging",
    "done",
    "failed",
    "cancelled",
];

pub const DEFAULT_LEASE_TTL_SECS: i64 = 3600;

pub const MAX_RECOVERY_ATTEMPTS: i64 = 3;

pub const PARKED_REF: &str = "daemon_parked";
pub const PARKED_REASON_REF: &str = "daemon_parked_reason";
pub const PARKED_RESUME_STATUS_REF: &str = "daemon_resume_status";
/// Durable "the dependency this park names cannot ever satisfy" bit (#473).
/// Only the dependency-sweep path sets it; every other park path clears it so
/// status's BLOCKED section renders no false unsatisfiable rows.
pub const PARKED_UNSATISFIABLE_REF: &str = "daemon_parked_unsatisfiable";
pub const CLASSIFIER_POLICY_PARKED_REF: &str = "classifier_policy_parked";
pub const PARKED_REWORK_RETRY_REF: &str = "daemon_rework_retry_requested";
/// Durable merge-call admission state. The CLI writes `requested` for an
/// explicit replay; the single daemon atomically advances it to `attempting`
/// before any GitHub/CI call. The live reviewed path also writes `attempting`
/// immediately before its first merge call. A crash or infrastructure failure
/// therefore cannot make an uncertain call eligible for automatic replay.
pub const MERGE_RETRY_REF: &str = "daemon_merge_retry";
pub const MERGE_RETRY_REQUESTED: &str = "requested";
pub const MERGE_RETRY_ATTEMPTING: &str = "attempting";
/// Maximum corrupt terminal retry rows reconciled in one daemon tick.  The
/// bounded batch makes restart cleanup converge without turning one tick into
/// an unbounded write transaction.
pub const TERMINAL_RETRY_RECONCILE_LIMIT: i64 = 8;
/// Legacy one-shot remediation head-check marker. New terminal parks never
/// write it; restart reconciliation removes it without reviving the task.
pub const PARKED_HEAD_CHECK_REF: &str = "daemon_parked_head_check";
pub const CI_REMEDIATION_REQUESTED_REF: &str = "ci_remediation_requested";
pub const CI_REMEDIATION_PR_REF: &str = "ci_remediation_pr";
pub const CI_REMEDIATION_HEAD_SHA_REF: &str = "ci_remediation_head_sha";
pub const CI_REMEDIATION_FEEDBACK_REF: &str = "ci_remediation_feedback";
pub const CI_REMEDIATION_CHECKS_REF: &str = "ci_remediation_checks";
pub const CI_REMEDIATION_ATTEMPTS_REF: &str = "ci_remediation_attempts";
pub const COMPLETION_PROVENANCE_MERGED: &str = "merged";
pub const COMPLETION_PROVENANCE_MANUAL: &str = "manual";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CiRemediationIntent {
    pub pr: i64,
    pub head_sha: String,
    pub feedback: String,
    pub checks: Vec<String>,
    pub attempts: i64,
}

/// Exact daemon-verified publication authority settled with a worker event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedWorkerPublication {
    pub pr: i64,
    pub source_sha: String,
    pub head_ref: String,
    pub expected_remote_sha: Option<String>,
}

pub fn ci_remediation_intent(refs: Option<&str>) -> Result<Option<CiRemediationIntent>> {
    let Some(raw) = refs else {
        return Ok(None);
    };
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| QuorumError::Io(format!("invalid persisted refs JSON: {error}")))?;
    if value
        .get(CI_REMEDIATION_REQUESTED_REF)
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return Ok(None);
    }
    let required_string = |key: &str| {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
            .ok_or_else(|| QuorumError::Io(format!("persisted CI remediation is missing {key}")))
    };
    let pr = value
        .get(CI_REMEDIATION_PR_REF)
        .and_then(serde_json::Value::as_i64)
        .filter(|pr| *pr > 0)
        .ok_or_else(|| QuorumError::Io("persisted CI remediation has invalid PR".into()))?;
    let checks = value
        .get(CI_REMEDIATION_CHECKS_REF)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            QuorumError::Io("persisted CI remediation is missing failing checks".into())
        })?
        .iter()
        .map(|check| {
            check
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| QuorumError::Io("persisted CI remediation check is not text".into()))
        })
        .collect::<Result<Vec<_>>>()?;
    if checks.is_empty() {
        return Err(QuorumError::Io(
            "persisted CI remediation has no failing checks".into(),
        ));
    }
    Ok(Some(CiRemediationIntent {
        pr,
        head_sha: required_string(CI_REMEDIATION_HEAD_SHA_REF)?,
        feedback: required_string(CI_REMEDIATION_FEEDBACK_REF)?,
        checks,
        attempts: value
            .get(CI_REMEDIATION_ATTEMPTS_REF)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
    }))
}

/// Body marker for review-only tasks whose approved PR failed to merge (e.g. conflicts).
/// The daemon's orphan-in-review handler detects this and retries merge when the PR
/// becomes MERGEABLE again.
pub const MERGE_BLOCKED_BODY: &str = "daemon:merge-blocked";

const KNOWN_EFFORTS: &[&str] = &["medium", "high"];
const KNOWN_COMPLEXITIES: &[&str] = &["1", "2", "3", "4", "5"];

pub fn lease_target(id: i64) -> String {
    format!("task#{id}")
}

fn deactivate_lease(tx: &rusqlite::Transaction, id: i64, now: i64) -> Result<()> {
    tx.execute(
        "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at > ?2",
        params![lease_target(id), now],
    )?;
    Ok(())
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Task {
    pub id: i64,
    pub title: String,
    pub body: Option<String>,
    pub status: String,
    pub priority: i64,
    pub labels: Option<String>,
    pub assignee: Option<String>,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub refs: Option<String>,
    pub depends_on: Option<String>,
    pub author: Option<String>,
    pub reviewer: Option<String>,
    pub rework_round: i64,
    pub review_only: bool,
    pub recovery_attempts: i64,
    pub revision: i64,
    pub edit_count: i64,
    pub continue_pr: Option<i64>,
    pub target_branch: Option<String>,
    /// Per-task rework ceiling, stamped from the daemon's `max_rework` config at
    /// first ownership. `None` means unstamped — see [`Task::effective_rework_cap`].
    pub rework_cap: Option<i64>,
    pub ready: bool,
}

impl Task {
    /// Resolved rework ceiling: the stamped per-task value, or the compiled
    /// [`crate::lifecycle::REWORK_CAP`] when unstamped (historic or unadopted rows).
    pub fn effective_rework_cap(&self) -> u32 {
        self.rework_cap
            .map(|c| c as u32)
            .unwrap_or(crate::lifecycle::REWORK_CAP)
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TaskBrief {
    pub id: i64,
    pub title: String,
    pub labels: Option<String>,
    pub priority: i64,
    pub status: String,
    pub assignee: Option<String>,
    pub ready: bool,
    pub depends_on: Option<String>,
    pub author: Option<String>,
    pub reviewer: Option<String>,
    pub rework_round: i64,
    pub recovery_attempts: i64,
    pub continue_pr: Option<i64>,
    pub target_branch: Option<String>,
}

impl From<&Task> for TaskBrief {
    fn from(t: &Task) -> Self {
        TaskBrief {
            id: t.id,
            title: t.title.clone(),
            labels: t.labels.clone(),
            priority: t.priority,
            status: t.status.clone(),
            assignee: t.assignee.clone(),
            ready: t.ready,
            depends_on: t.depends_on.clone(),
            author: t.author.clone(),
            reviewer: t.reviewer.clone(),
            rework_round: t.rework_round,
            recovery_attempts: t.recovery_attempts,
            continue_pr: t.continue_pr,
            target_branch: t.target_branch.clone(),
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TaskCompact {
    pub id: i64,
    pub status: String,
    pub assignee: Option<String>,
    pub refs: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continue_pr: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_worktree: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_exists: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub effects: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

impl From<&Task> for TaskCompact {
    fn from(t: &Task) -> Self {
        TaskCompact {
            id: t.id,
            status: t.status.clone(),
            assignee: t.assignee.clone(),
            refs: t.refs.clone(),
            continue_pr: t.continue_pr,
            lease_expires_at: None,
            note_id: None,
            suggested_branch: None,
            suggested_worktree: None,
            branch_exists: None,
            effects: Vec::new(),
            repo: None,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Note {
    pub id: i64,
    pub ts: i64,
    pub agent: String,
    pub body: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TaskDetail {
    #[serde(flatten)]
    pub task: Task,
    pub notes: Vec<Note>,
    pub agent_runs: Vec<crate::agent_runs::AgentRun>,
}

#[derive(Default)]
pub struct TaskUpdate<'a> {
    pub status: Option<&'a str>,
    pub body: Option<&'a str>,
    pub refs: Option<&'a str>,
    pub verdict: Option<&'a str>,
    pub depends_on: Option<&'a str>,
    /// Required compare-and-swap token for externally editable task fields.
    pub expected_revision: Option<i64>,
}

pub struct TransitionResult {
    pub task: Task,
    pub effects: Vec<Effect>,
}

pub fn effect_name(e: &Effect) -> String {
    match e {
        Effect::SetAuthor { .. } => "set_author".into(),
        Effect::SetReviewer { .. } => "set_reviewer".into(),
        Effect::SpawnReviewer => "spawn_reviewer".into(),
        Effect::ResumeReviewer => "resume_reviewer".into(),
        Effect::ResumeWorker => "resume_worker".into(),
        Effect::MergePr { .. } => "merge_pr".into(),
        Effect::IncrementReworkRound => "increment_rework_round".into(),
        Effect::NotifyOwner { .. } => "notify_owner".into(),
        Effect::ReleaseLease => "release_lease".into(),
        Effect::ClearAuthor => "clear_author".into(),
        Effect::PostFindingsNote => "post_findings_note".into(),
    }
}

const COLS: &str = "id, title, body, status, priority, labels, assignee, created_by, \
                    created_at, updated_at, refs, depends_on, author, reviewer, \
                    rework_round, review_only, recovery_attempts, revision, edit_count, \
                    continue_pr, target_branch, rework_cap";

const DEP_READY_CLAUSE: &str = "(depends_on IS NULL OR NOT EXISTS (
    SELECT 1 FROM json_each(depends_on) je
    WHERE NOT EXISTS (
        SELECT 1 FROM tasks d WHERE d.id = je.value AND d.status = 'done'
    )
))";

// Generated implementation work has additional graph authority. This exact
// predicate is used both by daemon-side candidate selection and by the
// authoritative claim transaction so the two paths cannot drift.
const GRAPH_IMPLEMENTATION_READY_CLAUSE: &str = "(NOT EXISTS (
    SELECT 1 FROM task_graph_members own_member
    WHERE own_member.task_id=tasks.id
) OR EXISTS (
    SELECT 1
    FROM task_graph_members own_member
    JOIN task_decompositions graph ON graph.id=own_member.graph_id
    JOIN tasks source ON source.id=graph.source_task_id
    WHERE own_member.task_id=tasks.id AND own_member.active=1
      AND graph.state='active' AND graph.active=1
      AND source.status='decomposed'
      AND NOT EXISTS (
          SELECT 1 FROM task_graph_members sibling_member
          JOIN tasks sibling ON sibling.id=sibling_member.task_id
          WHERE sibling_member.graph_id=own_member.graph_id
            AND sibling_member.active=1 AND sibling.status='failed'
      )
      AND 2 > (
          SELECT COUNT(*) FROM task_graph_members sibling_member
          JOIN tasks sibling ON sibling.id=sibling_member.task_id
          WHERE sibling_member.graph_id=own_member.graph_id
            AND sibling_member.active=1 AND sibling.status='working'
      )
))";

// SQL counterpart of the implementation branch in
// `classification_is_dispatchable`. Callers that need implementation work
// additionally require `review_only=0`; continuation tasks remain eligible at
// every classified size, just as they are in the Rust policy.
const DIRECT_DISPATCH_CLAUSE: &str = "(review_only=1 OR continue_pr IS NOT NULL OR (
    (json_extract(refs, '$.cx_size') IN ('S','M') OR (
        json_extract(refs, '$.cx_size')='L'
        AND json_extract(refs, '$.cx_est') <= 3
    ))
    AND NOT (json_extract(refs, '$.cx_est')=5
             AND json_extract(refs, '$.cx_size')='L')
))";

fn row_to_task(r: &Row) -> rusqlite::Result<Task> {
    Ok(Task {
        id: r.get(0)?,
        title: r.get(1)?,
        body: r.get(2)?,
        status: r.get(3)?,
        priority: r.get(4)?,
        labels: r.get(5)?,
        assignee: r.get(6)?,
        created_by: r.get(7)?,
        created_at: r.get(8)?,
        updated_at: r.get(9)?,
        refs: r.get(10)?,
        depends_on: r.get(11)?,
        author: r.get(12)?,
        reviewer: r.get(13)?,
        rework_round: r.get(14)?,
        review_only: r.get::<_, i64>(15)? != 0,
        recovery_attempts: r.get(16)?,
        revision: r.get(17)?,
        edit_count: r.get(18)?,
        continue_pr: r.get(19)?,
        target_branch: r.get(20)?,
        rework_cap: r.get(21)?,
        ready: false,
    })
}

fn validate_depends_on(s: &str) -> Result<()> {
    serde_json::from_str::<Vec<i64>>(s).map_err(|e| {
        QuorumError::Usage(format!(
            "--depends-on must be a JSON array of task ids (e.g. '[1,3]'): {e}"
        ))
    })?;
    Ok(())
}

fn validate_labels(s: &str) -> Result<()> {
    let labels: Vec<String> = serde_json::from_str(s).map_err(|e| {
        QuorumError::Usage(format!(
            "--labels must be a JSON array of strings (e.g. '[\"tier:opus-46\",\"effort:medium\"]'): {e}"
        ))
    })?;
    for label in &labels {
        if let Some(tier) = label.strip_prefix("tier:") {
            if !tier.is_empty() && crate::model_tiers::model_id_for_tier(tier).is_none() {
                return Err(QuorumError::Usage(format!(
                    "invalid tier '{tier}' in --labels; only {} are accepted",
                    crate::model_tiers::known_tiers(),
                )));
            }
        }
        if let Some(effort) = label.strip_prefix("effort:") {
            if !effort.is_empty() && !KNOWN_EFFORTS.contains(&effort) {
                return Err(QuorumError::Usage(format!(
                    "invalid effort '{effort}' in --labels; only {} are accepted",
                    KNOWN_EFFORTS.join(", ")
                )));
            }
        }
        if let Some(complexity) = label.strip_prefix("complexity:") {
            if !complexity.is_empty() && !KNOWN_COMPLEXITIES.contains(&complexity) {
                return Err(QuorumError::Usage(format!(
                    "invalid complexity '{complexity}' in --labels; only {} are accepted ({})",
                    KNOWN_COMPLEXITIES.join(", "),
                    crate::complexity::rubric_inline(),
                )));
            }
        }
    }
    Ok(())
}

/// Reject task-creator attempts to control daemon-owned routing. Kept separate
/// from structural label validation so migrations and internal compatibility
/// tests can still read and seed historical rows containing these labels.
pub fn validate_creator_labels(labels_json: Option<&str>) -> Result<()> {
    let Some(labels_json) = labels_json else {
        return Ok(());
    };
    let labels: Vec<String> = serde_json::from_str(labels_json).map_err(|e| {
        QuorumError::Usage(format!("--labels must be a JSON array of strings: {e}"))
    })?;
    if let Some(label) = labels.iter().find(|label| {
        ["tier:", "effort:", "complexity:"]
            .iter()
            .any(|prefix| label.starts_with(prefix))
    }) {
        return Err(QuorumError::Usage(format!(
            "label '{label}' is daemon-owned; task creators may not set \
             complexity, model tier, or effort"
        )));
    }
    Ok(())
}

pub fn validate_creator_refs(refs_json: Option<&str>) -> Result<()> {
    let Some(refs_json) = refs_json else {
        return Ok(());
    };
    let value: serde_json::Value = serde_json::from_str(refs_json)
        .map_err(|e| QuorumError::Usage(format!("--refs must be a JSON object: {e}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| QuorumError::Usage("--refs must be a JSON object".into()))?;
    if let Some(key) = object.keys().find(|key| key.starts_with("cx_")) {
        return Err(QuorumError::Usage(format!(
            "refs key '{key}' is classifier-owned; task creators may not set classification"
        )));
    }
    if let Some(key) = object
        .keys()
        .find(|key| key.starts_with("runner_") || key.starts_with("codex_"))
    {
        return Err(QuorumError::Usage(format!(
            "refs key '{key}' is runner-owned; task creators may not set provider state"
        )));
    }
    if object.contains_key("pr") {
        return Err(QuorumError::Usage(
            "refs key 'pr' is daemon-owned; use --review-pr or --continue-pr".into(),
        ));
    }
    if object.contains_key(MERGE_RETRY_REF) {
        return Err(QuorumError::Usage(format!(
            "refs key '{MERGE_RETRY_REF}' is daemon-owned; use task-retry"
        )));
    }
    Ok(())
}

fn preserve_classifier_refs(
    existing: &Option<String>,
    replacement: Option<&str>,
) -> Option<String> {
    preserve_protected_refs(existing, replacement, false)
}

/// Creator and assignee metadata replacement cannot mutate or erase durable
/// runner state. The daemon uses `preserve_classifier_refs` directly so its
/// authoritative refs path can still replace or clear these keys.
fn preserve_creator_protected_refs(
    existing: &Option<String>,
    replacement: Option<&str>,
) -> Option<String> {
    preserve_protected_refs(existing, replacement, true)
}

fn preserve_protected_refs(
    existing: &Option<String>,
    replacement: Option<&str>,
    preserve_runner_state: bool,
) -> Option<String> {
    let replacement = replacement?;
    let mut next: serde_json::Value =
        serde_json::from_str(replacement).unwrap_or_else(|_| serde_json::json!({}));
    let Some(next_map) = next.as_object_mut() else {
        return Some(replacement.to_string());
    };
    if let Some(existing_map) = existing
        .as_deref()
        .and_then(|refs| serde_json::from_str::<serde_json::Value>(refs).ok())
        .and_then(|value| value.as_object().cloned())
    {
        // PR association, classifier output, and (on creator/assignee paths)
        // runner state are daemon-owned. Metadata replacement may add caller
        // keys, but it cannot erase or rewrite established protected values.
        for (key, value) in existing_map {
            let classifier_or_pr = matches!(
                key.as_str(),
                "pr" | "cx_est"
                    | "cx_size"
                    | "cx_ready"
                    | "cx_not_ready_reason"
                    | "cx_by"
                    | "cx_dup_of"
                    | MERGE_RETRY_REF
            );
            let runner_state =
                preserve_runner_state && (key.starts_with("runner_") || key.starts_with("codex_"));
            if classifier_or_pr || runner_state {
                next_map.insert(key, value);
            }
        }
    }
    Some(next.to_string())
}

/// Remove the classifier-owned envelope while retaining unrelated caller and
/// daemon metadata.  Task content and dependency edits change the classifier
/// input, so this must happen in the same transaction as the edit; otherwise a
/// completed result for the old input could authorize dispatch.
fn invalidate_classifier_refs(
    existing: &Option<String>,
    replacement: Option<&str>,
) -> Option<String> {
    let refs =
        preserve_creator_protected_refs(existing, replacement).or_else(|| existing.clone())?;
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(&refs) else {
        return Some(refs);
    };
    let Some(object) = value.as_object_mut() else {
        return Some(refs);
    };
    for key in [
        "cx_est",
        "cx_size",
        "cx_ready",
        "cx_not_ready_reason",
        "cx_by",
        "cx_dup_of",
        "cx_flags",
        "cx_tags",
    ] {
        object.remove(key);
    }
    Some(value.to_string())
}

pub fn compute_ready(conn: &Connection, depends_on: &Option<String>) -> Result<bool> {
    let Some(json) = depends_on.as_deref() else {
        return Ok(true);
    };
    let unmet: i64 = conn.query_row(
        "SELECT count(*) FROM json_each(?1)
         WHERE NOT EXISTS (
             SELECT 1 FROM tasks d WHERE d.id = json_each.value AND d.status = 'done'
         )",
        params![json],
        |r| r.get(0),
    )?;
    Ok(unmet == 0)
}

/// Count tasks that have already started but not yet reached a terminal
/// state, excluding one task id (the planning source itself, which sits in
/// `planning` while draining and is not "started work" for this predicate).
///
/// Used by the decomposition drain-readiness gate: draining must wait for
/// every already-started task to run through review/rework/remediation/merge
/// to `done`/`failed`/`cancelled` before capturing the frozen base and
/// entering `planning`. `open` tasks never started; the freeze blocks new
/// claims, so the counted set can only shrink under drain.
pub fn count_started_non_terminal_excluding(conn: &Connection, exclude_id: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT count(*) FROM tasks
         WHERE status IN ('working','in-review','rework')
           AND id != ?1",
        params![exclude_id],
        |r| r.get(0),
    )?)
}

fn merge_pr_into_refs(existing: &Option<String>, pr: &str) -> String {
    let pr_val: serde_json::Value = pr
        .parse::<i64>()
        .map(|n| serde_json::json!(n))
        .unwrap_or_else(|_| serde_json::json!(pr));
    match existing.as_deref() {
        Some(s) => {
            if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(s) {
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("pr".to_string(), pr_val);
                    return v.to_string();
                }
            }
            serde_json::json!({"pr": pr_val}).to_string()
        }
        None => serde_json::json!({"pr": pr_val}).to_string(),
    }
}

fn extract_pr_from_refs(refs: &Option<String>) -> Option<String> {
    let s = refs.as_deref()?;
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    v.get("pr").and_then(|p| {
        if let Some(n) = p.as_i64() {
            Some(n.to_string())
        } else {
            p.as_str().map(|s| s.to_string())
        }
    })
}

pub fn extract_pr_number(refs: &Option<String>) -> Option<i64> {
    let s = refs.as_deref()?;
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    v.get("pr").and_then(|p| {
        p.as_i64()
            .or_else(|| p.as_str().and_then(|s| s.parse().ok()))
    })
}

pub fn extract_repo(refs: &Option<String>) -> Option<String> {
    let s = refs.as_deref()?;
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    v.get("repo")
        .and_then(|r| r.as_str())
        .map(|s| s.to_string())
}

/// Return the nonterminal task currently associated with a PR, if any.
///
/// Continuation creation calls the transaction variant under `BEGIN IMMEDIATE`, making the
/// check-and-insert atomic. Daemon paths that establish `refs.pr` later must use the same
/// transaction helper before granting publication authority.
pub fn active_pr_owner(conn: &Connection, pr: i64) -> Result<Option<i64>> {
    active_pr_owner_in(conn, pr, None)
}

pub fn active_pr_owner_in(
    conn: &Connection,
    pr: i64,
    excluding_task: Option<i64>,
) -> Result<Option<i64>> {
    Ok(conn
        .query_row(
            "SELECT id FROM tasks
             WHERE status NOT IN ('done', 'failed', 'cancelled')
               AND (?2 IS NULL OR id != ?2)
               AND (continue_pr = ?1 OR (
                    json_valid(COALESCE(refs, '{}'))
                    AND (json_extract(refs, '$.pr') = ?1
                         OR json_extract(refs, '$.pr') = CAST(?1 AS TEXT))))
             ORDER BY id LIMIT 1",
            params![pr, excluding_task],
            |row| row.get(0),
        )
        .optional()?)
}

// ── create ────────────────────────────────────────────────────────────────────

/// Maximum byte length for a task target's local branch name.
pub const MAX_TARGET_BRANCH_BYTES: usize = 255;

/// Validate a bounded local branch name with Git's ref validator.
///
/// Process arguments are passed directly to Git; branch text is never interpreted
/// by a shell. The pre-checks reject forms that Git accepts as shorthand or remote
/// qualifications but which cannot be this task's local target branch.
pub fn validate_target_branch(branch: &str) -> Result<()> {
    if branch.is_empty() {
        return Err(QuorumError::Usage("--base-branch must not be empty".into()));
    }
    if branch.len() > MAX_TARGET_BRANCH_BYTES {
        return Err(QuorumError::Usage(format!(
            "--base-branch must be at most {MAX_TARGET_BRANCH_BYTES} bytes"
        )));
    }
    if branch.starts_with('-') {
        return Err(QuorumError::Usage(
            "--base-branch must not be option-like".into(),
        ));
    }
    if branch == "@" || branch.chars().any(char::is_control) {
        return Err(QuorumError::Usage(
            "--base-branch must be a local branch name without control characters".into(),
        ));
    }
    if branch.starts_with("refs/")
        || branch.starts_with("remotes/")
        || branch.starts_with("origin/")
        || branch.starts_with("upstream/")
    {
        return Err(QuorumError::Usage(
            "--base-branch must not be remote-qualified".into(),
        ));
    }

    let output = Command::new("git")
        .args(["check-ref-format", "--branch", branch])
        .output()
        .map_err(|error| QuorumError::Io(format!("run git check-ref-format: {error}")))?;
    if !output.status.success() {
        return Err(QuorumError::Usage(format!(
            "invalid --base-branch {branch:?}"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn create(
    conn: &mut Connection,
    created_by: &str,
    title: &str,
    body: Option<&str>,
    priority: i64,
    labels: Option<&str>,
    refs: Option<&str>,
    depends_on: Option<&str>,
    review_pr: Option<i64>,
    now: i64,
) -> Result<i64> {
    create_with_continue_pr(
        conn, created_by, title, body, priority, labels, refs, depends_on, review_pr, None, now,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn create_with_continue_pr(
    conn: &mut Connection,
    created_by: &str,
    title: &str,
    body: Option<&str>,
    priority: i64,
    labels: Option<&str>,
    refs: Option<&str>,
    depends_on: Option<&str>,
    review_pr: Option<i64>,
    continue_pr: Option<i64>,
    now: i64,
) -> Result<i64> {
    create_with_continue_pr_and_target_branch(
        conn,
        created_by,
        title,
        body,
        priority,
        labels,
        refs,
        depends_on,
        review_pr,
        continue_pr,
        None,
        now,
    )
}

/// Create a task with an optional, authoritative target branch.
///
/// A supplied branch is validated and inserted in the same transaction as the
/// task, so a concurrent resolver cannot replace an explicit task-create target.
#[allow(clippy::too_many_arguments)]
pub fn create_with_continue_pr_and_target_branch(
    conn: &mut Connection,
    created_by: &str,
    title: &str,
    body: Option<&str>,
    priority: i64,
    labels: Option<&str>,
    refs: Option<&str>,
    depends_on: Option<&str>,
    review_pr: Option<i64>,
    continue_pr: Option<i64>,
    target_branch: Option<&str>,
    now: i64,
) -> Result<i64> {
    if review_pr.is_some() && continue_pr.is_some() {
        return Err(QuorumError::Usage(
            "--review-pr and --continue-pr are mutually exclusive".into(),
        ));
    }
    if review_pr.is_some_and(|pr| pr <= 0) {
        return Err(QuorumError::Usage("--review-pr must be positive".into()));
    }
    if continue_pr.is_some_and(|pr| pr <= 0) {
        return Err(QuorumError::Usage("--continue-pr must be positive".into()));
    }
    if let Some(branch) = target_branch {
        validate_target_branch(branch)?;
    }
    if let Some(s) = depends_on {
        validate_depends_on(s)?;
    }
    if let Some(s) = labels {
        validate_labels(s)?;
    }
    let (status, review_only, final_refs) = if let Some(pr) = review_pr {
        let r = match refs {
            Some(existing) => {
                let mut v: serde_json::Value = serde_json::from_str(existing)
                    .map_err(|e| QuorumError::Usage(format!("invalid refs JSON: {e}")))?;
                v.as_object_mut()
                    .ok_or_else(|| QuorumError::Usage("refs must be a JSON object".into()))?
                    .insert("pr".to_string(), serde_json::json!(pr));
                v.to_string()
            }
            None => serde_json::json!({"pr": pr}).to_string(),
        };
        ("in-review", 1_i64, Some(r))
    } else {
        ("open", 0_i64, refs.map(|s| s.to_string()))
    };
    let tx = begin_immediate(conn)?;
    if let Some(pr) = review_pr.or(continue_pr) {
        if let Some(owner) = active_pr_owner_in(&tx, pr, None)? {
            return Err(QuorumError::Usage(format!(
                "PR #{pr} is already associated with active task #{owner}"
            )));
        }
    }
    crate::agents::touch(&tx, created_by, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.execute(
        "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, \
         created_at, updated_at, refs, depends_on, review_only, continue_pr, target_branch) \
         VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            title,
            body,
            status,
            priority,
            labels,
            created_by,
            now,
            final_refs.as_deref(),
            depends_on,
            review_only,
            continue_pr,
            target_branch,
        ],
    )?;
    let id = tx.last_insert_rowid();
    let body_str = match labels {
        Some(l) => format!("created (prio {priority}, labels {l})"),
        None => format!("created (prio {priority})"),
    };
    crate::events::emit(&tx, "task_created", &lease_target(id), &body_str, now)?;
    tx.commit()?;
    Ok(id)
}

// ── claim ─────────────────────────────────────────────────────────────────────

pub fn claim(
    conn: &mut Connection,
    agent: &str,
    task_id: Option<i64>,
    match_labels: &[&str],
    ttl: i64,
    now: i64,
) -> Result<Option<Task>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;

    const CONTINUE_PR_UNOWNED_CLAUSE: &str = "(continue_pr IS NULL OR NOT EXISTS (
        SELECT 1 FROM tasks owner
        WHERE owner.id != tasks.id
          AND owner.status NOT IN ('done', 'failed', 'cancelled')
          AND (owner.continue_pr = tasks.continue_pr OR (
               json_valid(COALESCE(owner.refs, '{}'))
               AND (json_extract(owner.refs, '$.pr') = tasks.continue_pr
                    OR json_extract(owner.refs, '$.pr') = CAST(tasks.continue_pr AS TEXT))))
    ))";
    // The decomposition freeze blocks only NEW implementation starts, so this
    // clause is applied to the status='open' branch alone. Existing in-flight
    // work — reviewer attachment (status='in-review') here, plus rework and
    // remediation in claim_provider_retry_rework / claim_remediation_rework —
    // must still complete under the freeze, because the freeze's drain
    // predicate waits for workers==0 && reviewers==0 before capturing the
    // frozen base. Gating continuation on the freeze would deadlock it against
    // its own drain.
    const NO_PLANNING_FREEZE_CLAUSE: &str = "NOT EXISTS (
        SELECT 1 FROM task_decompositions WHERE freeze_active=1
    )";
    // Keep graph authority in this BEGIN IMMEDIATE transaction so sibling
    // claims, failures, and graph blockers cannot race provisioning.
    // Review/rework authority for an already-started child is intentionally not
    // gated: active children may finish after a sibling fails or blocks the graph.

    let mut task = match task_id {
        Some(id) => tx
            .query_row(
                &format!(
                    "UPDATE tasks SET
                        status = CASE WHEN status='open' THEN 'working' ELSE status END,
                        assignee = ?1,
                        author = CASE WHEN status='open' AND author IS NULL THEN ?1 ELSE author END,
                        reviewer = CASE WHEN status='in-review' THEN ?1 ELSE reviewer END,
                        updated_at = ?2
                     WHERE id = ?3
                       AND json_valid(refs)
                       AND json_type(refs, '$.cx_est')='integer'
                       AND json_extract(refs, '$.cx_est') BETWEEN 1 AND 5
                       AND json_type(refs, '$.cx_size')='text'
                       AND json_extract(refs, '$.cx_size') IN ('S','M','L','XL')
                       AND {DIRECT_DISPATCH_CLAUSE}
                       AND json_type(refs, '$.cx_ready')='true'
                       AND json_type(refs, '$.cx_not_ready_reason')='null'
                       AND {CONTINUE_PR_UNOWNED_CLAUSE}
                       AND (
                         (status='open' AND {NO_PLANNING_FREEZE_CLAUSE}
                            AND {DEP_READY_CLAUSE}
                            AND {GRAPH_IMPLEMENTATION_READY_CLAUSE})
                         OR (status='in-review' AND reviewer IS NULL \
                             AND (author IS NULL OR author != ?1))
                     )
                     RETURNING {COLS}"
                ),
                params![agent, now, id],
                row_to_task,
            )
            .optional()?,
        None => {
            let mut selector = format!(
                "SELECT id FROM tasks
                 WHERE json_valid(refs)
                   AND json_type(refs, '$.cx_est')='integer'
                   AND json_extract(refs, '$.cx_est') BETWEEN 1 AND 5
                   AND json_type(refs, '$.cx_size')='text'
                   AND json_extract(refs, '$.cx_size') IN ('S','M','L','XL')
                   AND {DIRECT_DISPATCH_CLAUSE}
                   AND json_type(refs, '$.cx_ready')='true'
                   AND json_type(refs, '$.cx_not_ready_reason')='null'
                   AND {CONTINUE_PR_UNOWNED_CLAUSE}
                   AND (
                    (status='open' AND {NO_PLANNING_FREEZE_CLAUSE}
                       AND {DEP_READY_CLAUSE}
                       AND {GRAPH_IMPLEMENTATION_READY_CLAUSE})
                    OR (status='in-review' AND reviewer IS NULL \
                        AND (author IS NULL OR author != ?1))
                )"
            );
            if !match_labels.is_empty() {
                use std::fmt::Write as _;
                selector.push_str(" AND (");
                for i in 0..match_labels.len() {
                    if i > 0 {
                        selector.push_str(" AND ");
                    }
                    let _ = write!(selector, "labels LIKE ?{}", i + 3);
                }
                selector.push(')');
            }
            selector.push_str(
                " ORDER BY
                    CASE WHEN status='open' AND EXISTS (
                        SELECT 1 FROM task_graph_members graph_priority
                        WHERE graph_priority.task_id=tasks.id AND graph_priority.active=1
                    ) THEN 0 ELSE 1 END,
                    priority DESC, id ASC LIMIT 1",
            );

            let sql = format!(
                "UPDATE tasks SET
                    status = CASE WHEN status='open' THEN 'working' ELSE status END,
                    assignee = ?1,
                    author = CASE WHEN status='open' AND author IS NULL THEN ?1 ELSE author END,
                    reviewer = CASE WHEN status='in-review' THEN ?1 ELSE reviewer END,
                    updated_at = ?2
                 WHERE id = ({selector}) RETURNING {COLS}"
            );
            let label_pats: Vec<String> =
                match_labels.iter().map(|l| format!("%\"{l}\"%")).collect();
            let mut bind: Vec<&dyn rusqlite::ToSql> = vec![&agent, &now];
            for p in &label_pats {
                bind.push(p);
            }
            tx.query_row(&sql, &bind[..], row_to_task).optional()?
        }
    };

    if let Some(t) = &mut task {
        t.ready = true;
        let target = lease_target(t.id);
        if t.status == "working" {
            tx.execute(
                "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at <= ?2",
                params![target, now],
            )?;
            tx.execute(
                "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1,?2,?3,?4,1)",
                params![target, agent, now, now + ttl],
            )?;
            crate::events::emit(&tx, "task_claimed", &target, &format!("by {agent}"), now)?;
        } else {
            crate::events::emit(
                &tx,
                "reviewer_attached",
                &target,
                &format!("by {agent}"),
                now,
            )?;
        }
    }
    tx.commit()?;
    Ok(task)
}

/// Reattach a worker to a provider-blocked rework task without erasing its
/// lifecycle phase. This is deliberately separate from [`claim`]: a rework
/// retry remains `rework`, but still needs a worker lease rather than reviewer
/// attachment semantics.
pub fn claim_provider_retry_rework(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    ttl: i64,
    now: i64,
) -> Result<Option<Task>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;

    let updated = tx.execute(
        &format!(
            "UPDATE tasks SET assignee=?1, updated_at=?2
         WHERE id=?3 AND status='rework' AND assignee IS NULL
           AND {DEP_READY_CLAUSE}
           AND CASE WHEN json_valid(refs) THEN
               json_type(refs, '$.cx_est')='integer'
               AND json_extract(refs, '$.cx_est') BETWEEN 1 AND 5
               AND json_type(refs, '$.cx_size')='text'
               AND json_extract(refs, '$.cx_size') IN ('S','M','L','XL')
               AND (review_only=1 OR continue_pr IS NOT NULL OR (
                   (json_extract(refs, '$.cx_size') IN ('S','M') OR (
                       json_extract(refs, '$.cx_size')='L'
                       AND json_extract(refs, '$.cx_est') <= 3
                   ))
                   AND NOT (json_extract(refs, '$.cx_est')=5
                            AND json_extract(refs, '$.cx_size')='L')
               ))
               AND json_type(refs, '$.cx_ready')='true'
               AND json_type(refs, '$.cx_not_ready_reason')='null'
               AND (
                   CASE WHEN json_type(refs, '$.runner_retry') IS NOT NULL
                       THEN COALESCE(json_type(refs, '$.runner_retry.requested')='true', 0)
                       ELSE COALESCE(json_type(refs, '$.codex_retry_requested')='true', 0)
                   END
                   OR json_type(refs, '$.daemon_rework_retry_requested')='true'
               )
               ELSE 0
           END"
        ),
        params![agent, now, id],
    )?;
    let mut task = if updated == 1 {
        Some(tx.query_row(
            &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
            params![id],
            row_to_task,
        )?)
    } else {
        None
    };

    if let Some(task) = &mut task {
        task.ready = true;
        let target = lease_target(task.id);
        tx.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at <= ?2",
            params![target, now],
        )?;
        tx.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1,?2,?3,?4,1)",
            params![target, agent, now, now + ttl],
        )?;
        crate::events::emit(&tx, "task_claimed", &target, &format!("by {agent}"), now)?;
    }

    // Sweep only after the replacement lease exists. Sweeping first would
    // classify this deliberately unleased retry as lapsed and erase `rework`.
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.commit()?;
    Ok(task)
}

/// Daemon-private: restore the lease for an exact dormant worker whose
/// awaiting-review lease expired while the daemon was down. The task must
/// still be in `in-review` or `merging`, and no other live holder may exist.
/// The caller validates the durable worker identity before entering this
/// atomic mutable-state check. Returns `false` if that state changed.
pub fn reclaim_dormant_review(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    ttl: i64,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let target = lease_target(id);
    let task_matches: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM tasks
             WHERE id=?1 AND status IN ('in-review','merging')
         )",
        [id],
        |row| row.get(0),
    )?;
    if !task_matches {
        tx.commit()?;
        return Ok(false);
    }

    let live_holder: Option<String> = tx
        .query_row(
            "SELECT holder FROM claims
             WHERE target=?1 AND active=1 AND expires_at>?2",
            params![target, now],
            |row| row.get(0),
        )
        .optional()?;
    match live_holder.as_deref() {
        Some(holder) if holder == agent => {
            tx.commit()?;
            return Ok(true);
        }
        Some(_) => {
            tx.commit()?;
            return Ok(false);
        }
        None => {}
    }

    crate::agents::touch(&tx, agent, now)?;
    tx.execute(
        "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at<=?2",
        params![target, now],
    )?;
    tx.execute(
        "INSERT INTO claims(target,holder,ts,expires_at,active) VALUES (?1,?2,?3,?4,1)",
        params![target, agent, now, now + ttl],
    )?;
    crate::events::emit(
        &tx,
        "task_claimed",
        &target,
        &format!("by {agent} (dormant recovery)"),
        now,
    )?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.commit()?;
    Ok(true)
}

// ── claim_remediation_rework ──────────────────────────────────────────────────

/// Daemon-private: atomically claim a rework task for a remediation worker.
/// Verifies the task is still in `rework`, installs the remediation assignee
/// and a live lease, then sweeps only after the lease exists. The original
/// author is retained: it is the durable identity of the managed PR branch
/// when GitHub target resolution is unavailable. Returns `None` if the task
/// left rework or another remediation agent already holds the claim (partial
/// unique index is the race authority).
pub fn claim_remediation_rework(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    ttl: i64,
    now: i64,
) -> Result<Option<Task>> {
    claim_remediation_rework_with_feedback(conn, agent, id, ttl, now, None)
}

/// Variant of [`claim_remediation_rework`] used by the daemon when it holds
/// accepted blocking feedback for the pending remediation turn. The feedback
/// is persisted in the same transaction before a dependency-triggered sweep
/// can terminally park the rework task.
pub fn claim_remediation_rework_with_feedback(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    ttl: i64,
    now: i64,
    feedback: Option<&str>,
) -> Result<Option<Task>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;

    if let Some(feedback) = feedback {
        tx.execute(
            "UPDATE tasks
             SET refs=json_set(COALESCE(refs, '{}'), '$.remediation_feedback', ?2),
                 updated_at=?3
             WHERE id=?1 AND status='rework'
               AND NOT EXISTS (
                   SELECT 1 FROM claims c
                   WHERE c.target='task#' || tasks.id
                     AND c.active=1 AND c.expires_at > ?3
               )",
            params![id, feedback, now],
        )?;
    }

    let status: Option<String> = tx
        .query_row(
            &format!("SELECT status FROM tasks WHERE id=?1 AND {DEP_READY_CLAUSE}"),
            params![id],
            |r| r.get(0),
        )
        .optional()?;

    let policy_parked: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM tasks
             WHERE id=?1 AND (NOT json_valid(refs)
                 OR json_type(refs, '$.cx_est') IS NOT 'integer'
                 OR COALESCE(json_extract(refs, '$.cx_est'), 0) NOT BETWEEN 1 AND 5
                 OR json_type(refs, '$.cx_size') IS NOT 'text'
                 OR COALESCE(json_extract(refs, '$.cx_size'), '') NOT IN ('S','M','L','XL')
                 OR json_type(refs, '$.cx_ready') IS NOT 'true'
                 OR json_type(refs, '$.cx_not_ready_reason') IS NOT 'null'
                 OR NOT (review_only=1 OR continue_pr IS NOT NULL OR (
                     (json_extract(refs, '$.cx_size') IN ('S','M') OR (
                         json_extract(refs, '$.cx_size')='L'
                         AND json_extract(refs, '$.cx_est') <= 3
                     ))
                     AND NOT (json_extract(refs, '$.cx_est')=5
                              AND json_extract(refs, '$.cx_size')='L')
                 )))
         )",
        params![id],
        |row| row.get(0),
    )?;
    if status.as_deref() != Some("rework") || policy_parked {
        crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
        tx.commit()?;
        return Ok(None);
    }

    let target = lease_target(id);

    // Deactivate expired claims only — the stale worker claim was released by
    // the ReleaseLease effect in the VerdictChanges/MergeConflict transition.
    tx.execute(
        "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at <= ?2",
        params![target, now],
    )?;

    // Partial unique index is the atomicity authority (invariant #1). If
    // another remediation agent already holds the active claim, this INSERT
    // fails with SQLITE_CONSTRAINT_UNIQUE — a normal lost race, not an error.
    let ins = tx.execute(
        "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1,?2,?3,?4,1)",
        params![target, agent, now, now + ttl],
    );

    match ins {
        Ok(_) => {}
        Err(ref e) if crate::claims::is_unique_violation_pub(e) => {
            crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
            tx.commit()?;
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    }

    tx.execute(
        "UPDATE tasks SET assignee=?1, updated_at=?2 WHERE id=?3",
        params![agent, now, id],
    )?;
    crate::events::emit(
        &tx,
        "task_claimed",
        &target,
        &format!("by {agent} (remediation)"),
        now,
    )?;

    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;

    // Sweep only after the replacement lease exists.
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.commit()?;
    Ok(Some(task))
}

/// Atomically reserve reviewer provisioning authority. The daemon must release
/// the opaque token after either attaching the reviewer or cleaning up a failed
/// external provision.
///
/// This deliberately does NOT gate on the repository decomposition freeze. A
/// freeze blocks only a new open-status worker start (see `claim`, where the
/// freeze clause sits inside the status='open' branch); existing in-flight
/// continuation — reviewer attachment, rework, and remediation — must still
/// complete. An already-published PR still needs its reviewer to finish. The
/// freeze's
/// quiescence contract is enforced by `decomposition_drain_ready`, which waits
/// for workers==0 && reviewers==0 before capturing the frozen base — a state
/// only reachable if in-flight reviews are allowed to run. Gating reservation
/// on the freeze would strand a retained worker awaiting review and deadlock the
/// freeze against its own drain.
pub fn reserve_reviewer_provision(
    conn: &mut Connection,
    task_id: i64,
    token: &str,
    role: &str,
    now: i64,
) -> Result<bool> {
    if token.is_empty() || token.len() > 128 || token.contains('\0') || !matches!(role, "r1" | "r2")
    {
        return Err(QuorumError::Usage(
            "invalid reviewer reservation token".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let eligible: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM tasks t
             WHERE t.id=?1
               AND ?2 IN ('r1','r2') AND t.status='in-review'
               AND json_valid(t.refs)
               AND json_type(t.refs,'$.cx_est')='integer'
               AND json_extract(t.refs,'$.cx_est') BETWEEN 1 AND 5
               AND json_type(t.refs,'$.cx_size')='text'
               AND json_extract(t.refs,'$.cx_size') IN ('S','M','L','XL')
               AND (t.review_only=1 OR t.continue_pr IS NOT NULL OR (
                   (json_extract(t.refs,'$.cx_size') IN ('S','M') OR (
                       json_extract(t.refs,'$.cx_size')='L'
                       AND json_extract(t.refs,'$.cx_est') <= 3
                   ))
                   AND NOT (json_extract(t.refs,'$.cx_est')=5
                            AND json_extract(t.refs,'$.cx_size')='L')
               ))
               AND json_type(t.refs,'$.cx_ready')='true'
               AND json_type(t.refs,'$.cx_not_ready_reason')='null'
               AND NOT EXISTS (SELECT 1 FROM reviewer_provision_reservations WHERE task_id=t.id)
         )",
        params![task_id, role],
        |row| row.get(0),
    )?;
    if !eligible {
        tx.commit()?;
        return Ok(false);
    }
    let inserted = tx.execute(
        "INSERT INTO reviewer_provision_reservations(task_id,token,role,created_at)
         VALUES (?1,?2,?3,?4)",
        params![task_id, token, role, now],
    );
    if matches!(&inserted, Err(error) if crate::claims::is_unique_violation_pub(error)) {
        return Ok(false);
    }
    inserted?;
    tx.commit()?;
    Ok(true)
}

pub fn release_reviewer_provision(
    conn: &mut Connection,
    task_id: i64,
    token: &str,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "DELETE FROM reviewer_provision_reservations WHERE task_id=?1 AND token=?2",
        params![task_id, token],
    )?;
    tx.commit()?;
    Ok(changed == 1)
}

/// Startup-only crash cleanup. The daemon lock guarantees there is no live
/// provisioning owner when the replacement daemon calls this.
pub fn clear_reviewer_provision_reservations(conn: &mut Connection) -> Result<usize> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute("DELETE FROM reviewer_provision_reservations", [])?;
    tx.commit()?;
    Ok(changed)
}

/// Daemon-private: check whether `agent` still holds an active, unexpired task
/// lease on `task#<id>`. Used by the final worker teardown to guard the
/// name-pool release: while the lease is live, the identity is still authoritative
/// and must not be recycled. Idempotent read; returns `false` once the lease has
/// been deactivated (released/transferred) or expired.
pub fn worker_lease_active_for(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let target = lease_target(id);
    let active = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM claims
              WHERE target=?1 AND holder=?2 AND active=1 AND expires_at > ?3
         )",
        params![target, agent, now],
        |row| row.get(0),
    )?;
    tx.commit()?;
    Ok(active)
}

/// Daemon-private: revalidate that a remediation worker still owns the exact
/// live rework lease it acquired before an awaited provisioning prerequisite.
///
/// The `BEGIN IMMEDIATE` snapshot serializes this check with creator
/// cancellation and other lifecycle writes. Logical expiry is part of the
/// predicate, so an expired claim is never treated as provisioning authority.
pub fn remediation_claim_still_owned(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let owned = tx.query_row(
        "SELECT EXISTS(
             SELECT 1
               FROM tasks t
               JOIN claims c ON c.target = 'task#' || t.id
              WHERE t.id=?1
                AND t.status='rework'
                AND t.assignee=?2
                AND c.holder=?2
                AND c.active=1
                AND c.expires_at > ?3
         )",
        params![id, agent, now],
        |row| row.get(0),
    )?;
    tx.commit()?;
    Ok(owned)
}

/// Release a remediation lease on provisioning failure. Deactivates the claim
/// and clears the assignee, leaving the task in rework for the next provisioning
/// attempt or reaper cycle.
pub fn release_remediation_lease(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    now: i64,
) -> Result<()> {
    let tx = begin_immediate(conn)?;
    let target = lease_target(id);
    tx.execute(
        "UPDATE claims SET active=0 WHERE target=?1 AND holder=?2 AND active=1",
        params![target, agent],
    )?;
    tx.execute(
        "UPDATE tasks SET assignee=NULL, updated_at=?1 WHERE id=?2 AND assignee=?3 AND status='rework'",
        params![now, id, agent],
    )?;
    crate::events::emit(
        &tx,
        "remediation_lease_released",
        &target,
        &format!("by {agent} (provision failed)"),
        now,
    )?;
    tx.commit()?;
    Ok(())
}

/// Suspend one managed run's authority during a controlled daemon shutdown.
///
/// The exact run capability, task lease, and audit event commit together. The
/// task lifecycle status and assignee are deliberately left unchanged so a
/// later restart can decide how to resume the preserved task phase.
///
/// Returns whether the exact capability was live and has now been revoked.
/// A previously revoked matching run is an idempotent `false` result. An
/// unknown or mismatched run/task/agent tuple is rejected without mutation.
pub fn suspend_run_for_controlled_shutdown(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    run_id: &str,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let capability_was_live =
        crate::capabilities::revoke_for_agent_task_tx(&tx, run_id, agent, id, now)?;
    deactivate_lease(&tx, id, now)?;
    let target = lease_target(id);
    crate::events::emit(
        &tx,
        "controlled_shutdown_suspended",
        &target,
        &format!("by {agent} (run {run_id})"),
        now,
    )?;
    tx.commit()?;
    Ok(capability_was_live)
}

// ── apply_event ───────────────────────────────────────────────────────────────

pub fn apply_event(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    event: &Event,
    now: i64,
) -> Result<TransitionResult> {
    let tx = begin_immediate(conn)?;
    apply_event_tx(tx, agent, id, event, now, |_| Ok(()))
}

/// Apply an actionable rework transition and persist the exact pending turn
/// in the same transaction. Sticky turn-oriented workers may be dormant when
/// the daemon dies, so the lifecycle destination must never become visible
/// without the prompt that restart will replay.
pub fn apply_actionable_rework_event(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    event: &Event,
    feedback: &str,
    now: i64,
) -> Result<TransitionResult> {
    if feedback.trim().is_empty() || feedback.contains('\0') {
        return Err(QuorumError::BadInput(
            "actionable rework feedback must be non-empty and contain no NUL".into(),
        ));
    }
    if !matches!(event, Event::VerdictChanges | Event::MergeConflict) {
        return Err(QuorumError::BadInput(
            "actionable rework persistence requires VerdictChanges or MergeConflict".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    apply_event_tx(tx, agent, id, event, now, |tx| {
        tx.execute(
            "UPDATE tasks
             SET refs=json_set(COALESCE(refs, '{}'), '$.remediation_feedback', ?2)
             WHERE id=?1 AND status='rework'",
            params![id, feedback],
        )?;
        Ok(())
    })
}

/// Complete an approved merge and consume its exact task/PR approvals
/// in the same lifecycle transaction.
pub fn complete_approved_merge(
    conn: &mut Connection,
    id: i64,
    pr_number: i64,
    now: i64,
) -> Result<TransitionResult> {
    let tx = begin_immediate(conn)?;
    apply_event_tx(tx, "daemon", id, &Event::MergeSucceeded, now, |tx| {
        tx.execute(
            "DELETE FROM approvals WHERE pr_number=?1",
            params![pr_number],
        )?;
        tx.execute(
            "UPDATE tasks SET refs=json_remove(refs, '$.daemon_merge_retry') WHERE id=?1",
            params![id],
        )?;
        Ok(())
    })
}

/// Fail closed from an admitted merge attempt to ordinary review. Only the
/// named stale roles are invalidated; valid same-head evidence for another
/// role is preserved for `next_needed_role`. A named stale sampling decision
/// is removed in the same transaction when R1 must recreate that authority.
/// The attempt marker is consumed there too so a later reviewed head can cross
/// a fresh boundary.
pub struct StaleMergeRetryEvidence<'a> {
    pub roles: &'a [&'a str],
    pub sampling_head: Option<&'a str>,
}

pub fn invalidate_merge_retry(
    conn: &mut Connection,
    id: i64,
    pr_number: i64,
    stale: StaleMergeRetryEvidence<'_>,
    reason: &str,
    now: i64,
) -> Result<TransitionResult> {
    let tx = begin_immediate(conn)?;
    let event = Event::MergeFailed {
        reason: reason.to_string(),
    };
    apply_event_tx(tx, "daemon", id, &event, now, |tx| {
        for role in stale.roles {
            tx.execute(
                "DELETE FROM approvals
                 WHERE pr_number=?1 AND review_role=?2",
                params![pr_number, role],
            )?;
        }
        if let Some(head_sha) = stale.sampling_head {
            tx.execute(
                "DELETE FROM r2_sampling_decisions
                 WHERE pr_number=?1 AND head_sha=?2",
                params![pr_number, head_sha],
            )?;
        }
        tx.execute(
            "UPDATE tasks SET refs=json_remove(refs, '$.daemon_merge_retry') WHERE id=?1",
            params![id],
        )?;
        Ok(())
    })
}

/// Atomically consume an admitted merge attempt into actionable remediation.
///
/// Worker-fixable merge outcomes must never expose an intermediate
/// `in-review` task after deleting approval authority: that state could
/// provision a fresh reviewer instead of the worker who must change the code.
/// The direct daemon-owned `MergeConflict` transition preserves the lifecycle
/// rework budget while approval invalidation, attempt consumption, and the
/// restart-replayable feedback commit in the same transaction.
pub fn rework_approved_merge(
    conn: &mut Connection,
    id: i64,
    pr_number: i64,
    feedback: &str,
    now: i64,
) -> Result<TransitionResult> {
    if feedback.trim().is_empty() || feedback.contains('\0') {
        return Err(QuorumError::BadInput(
            "approved merge rework feedback must be non-empty and contain no NUL".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    apply_event_tx(tx, "daemon", id, &Event::MergeConflict, now, |tx| {
        let changed = tx.execute(
            "UPDATE tasks
             SET refs=json_set(
                 json_remove(refs, '$.daemon_merge_retry'),
                 '$.remediation_feedback', ?3,
                 '$.daemon_rework_retry_requested', json('true')
             )
             WHERE id=?1 AND status IN ('rework','failed') AND json_valid(refs)
               AND json_extract(refs, '$.pr')=?2
               AND json_extract(refs, '$.daemon_merge_retry')='attempting'",
            params![id, pr_number, feedback],
        )?;
        if changed != 1 {
            return Err(QuorumError::Io(format!(
                "task #{id} lost admitted merge authority before rework disposition"
            )));
        }
        tx.execute(
            "DELETE FROM approvals WHERE pr_number=?1",
            params![pr_number],
        )?;
        Ok(())
    })
}

/// Atomically invalidate startup merge authority when the independent
/// sampled-R2 decision is missing or belongs to another task. R1 is the role
/// that creates that decision, so an exact-head R2 approval remains reusable.
/// A task already at `merging` returns directly to review; older recovered
/// lifecycle shapes retain their status for generic recovery.
pub fn invalidate_recovered_sampling_authority(
    conn: &mut Connection,
    id: i64,
    pr_number: i64,
    head_sha: &str,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let status: Option<String> = tx
        .query_row(
            "SELECT status FROM tasks
             WHERE id=?1 AND json_valid(refs) AND json_extract(refs, '$.pr')=?2",
            params![id, pr_number],
            |row| row.get(0),
        )
        .optional()?;
    let Some(status) = status else {
        tx.commit()?;
        return Ok(false);
    };
    let invalidate = |tx: &Transaction<'_>| -> Result<()> {
        tx.execute(
            "DELETE FROM approvals
             WHERE pr_number=?1 AND review_role='r1' AND task_id=?2",
            params![pr_number, id],
        )?;
        tx.execute(
            "DELETE FROM r2_sampling_decisions
             WHERE pr_number=?1 AND head_sha=?2",
            params![pr_number, head_sha],
        )?;
        Ok(())
    };
    if status == "merging" {
        apply_event_tx(
            tx,
            "daemon",
            id,
            &Event::MergeFailed {
                reason: "startup merge authority has no exact sampled-R2 decision".into(),
            },
            now,
            invalidate,
        )?;
    } else {
        invalidate(&tx)?;
        tx.commit()?;
    }
    Ok(true)
}

/// Remove non-authoritative optional-role evidence without consuming the
/// owner-authorized merge attempt.
///
/// The task must still own the exact `merging + attempting` boundary. Keeping
/// that marker makes a crash after this repair conservative: startup parks the
/// task as an uncertain attempt instead of replaying automatically. The live
/// caller may reread the complete authority once and issue at most one remote
/// merge call.
pub fn repair_merge_retry_evidence(
    conn: &mut Connection,
    id: i64,
    pr_number: i64,
    stale_roles: &[&str],
    reason: &str,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE tasks
         SET updated_at=?2
         WHERE id=?1 AND status='merging' AND json_valid(refs)
           AND json_extract(refs, '$.daemon_merge_retry')='attempting'
           AND json_extract(refs, '$.pr')=?3",
        params![id, now, pr_number],
    )?;
    if changed == 0 {
        tx.commit()?;
        return Ok(false);
    }
    for role in stale_roles {
        tx.execute(
            "DELETE FROM approvals
             WHERE pr_number=?1 AND review_role=?2",
            params![pr_number, role],
        )?;
    }
    crate::events::emit(
        &tx,
        "merge_retry_authority_repaired",
        &lease_target(id),
        reason,
        now,
    )?;
    tx.commit()?;
    Ok(true)
}

/// Apply a daemon-verified worker publication and retire its durable intent in
/// the same transaction as the lifecycle transition.
pub fn apply_published_worker_event(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    event: &Event,
    publication: &PublishedWorkerPublication,
    now: i64,
) -> Result<TransitionResult> {
    let tx = begin_immediate(conn)?;
    apply_event_tx(tx, agent, id, event, now, |tx| {
        settle_published_worker_tx(tx, id, publication, now)
    })
}

fn settle_published_worker_tx(
    tx: &Transaction<'_>,
    task_id: i64,
    publication: &PublishedWorkerPublication,
    now: i64,
) -> Result<()> {
    if publication.pr <= 0
        || publication.source_sha.is_empty()
        || publication.head_ref.is_empty()
        || publication
            .expected_remote_sha
            .as_deref()
            .is_some_and(str::is_empty)
    {
        return Err(QuorumError::BadInput(
            "published worker settlement requires a valid PR, source SHA, head ref, and lease baseline"
                .into(),
        ));
    }

    let recorded = tx
        .query_row(
            "SELECT json_extract(refs, '$.daemon_publication.pr'),
                    json_extract(refs, '$.daemon_publication.local_sha'),
                    json_extract(refs, '$.daemon_publication.branch'),
                    json_extract(refs, '$.daemon_publication.expected_remote_sha'),
                    json_extract(refs, '$.daemon_publication.stage')
             FROM tasks WHERE id=?1",
            params![task_id],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .optional()?;
    let expected_intent = (
        Some(publication.pr),
        Some(publication.source_sha.clone()),
        Some(publication.head_ref.clone()),
        publication.expected_remote_sha.clone(),
        Some("verified".to_string()),
    );
    if recorded.as_ref() != Some(&expected_intent) {
        return Err(QuorumError::Io(format!(
            "published worker settlement for task #{task_id} no longer matches its verified publication intent"
        )));
    }

    if let Some(prior_sha) = publication.expected_remote_sha.as_deref() {
        let rotated = tx.execute(
            "UPDATE pr_targets
             SET head_sha=?4, resolved_at=?6
             WHERE task_id=?1 AND pr_number=?2 AND head_ref=?3 AND is_fork=0
               AND (head_sha=?5 OR head_sha=?4)",
            params![
                task_id,
                publication.pr,
                publication.head_ref,
                publication.source_sha,
                prior_sha,
                now,
            ],
        )?;
        if rotated != 1 {
            return Err(QuorumError::Io(format!(
                "published PR #{} target authority changed before settlement: expected {} or already-published {} on {}",
                publication.pr,
                prior_sha,
                publication.source_sha,
                publication.head_ref,
            )));
        }
    }

    tx.execute(
        "UPDATE tasks
         SET refs=json_remove(COALESCE(refs, '{}'), '$.daemon_publication')
         WHERE id=?1",
        params![task_id],
    )?;
    Ok(())
}

/// Atomically enter rework for failed pre-review CI and persist the exact
/// remediation intent that restart recovery must replay on the same PR/head.
pub fn apply_checks_failed_with_remediation(
    conn: &mut Connection,
    id: i64,
    pr: i64,
    head_sha: &str,
    checks: &[String],
    feedback: &str,
    now: i64,
) -> Result<TransitionResult> {
    if pr <= 0 || head_sha.is_empty() || checks.is_empty() || feedback.is_empty() {
        return Err(QuorumError::BadInput(
            "CI remediation requires PR, head SHA, checks, and feedback".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let checks_json = serde_json::to_string(checks)
        .map_err(|error| QuorumError::Io(format!("serialize failing checks: {error}")))?;
    apply_event_tx(
        tx,
        "system",
        id,
        &Event::ChecksFailed {
            checks: checks.to_vec(),
        },
        now,
        |tx| {
            // The lifecycle update has already selected the destination. Only
            // rework receives retry intent; a rework-cap failure remains terminal.
            tx.execute(
                "UPDATE tasks SET refs=json_set(
                     COALESCE(refs, '{}'),
                     '$.ci_remediation_requested', json('true'),
                     '$.ci_remediation_pr', ?2,
                     '$.ci_remediation_head_sha', ?3,
                     '$.ci_remediation_feedback', ?4,
                     '$.ci_remediation_checks', json(?5),
                     '$.ci_remediation_attempts', 0
                 )
                 WHERE id=?1 AND status='rework'",
                params![id, pr, head_sha, feedback, checks_json],
            )?;
            tx.execute(
                "DELETE FROM approvals WHERE pr_number=?1 AND task_id=?2",
                params![pr, id],
            )?;
            Ok(())
        },
    )
}

/// Persist one failed attempt to provision a CI remediation worker.
/// Returns `None` if the task no longer owns this durable rework intent.
pub fn record_ci_remediation_attempt(
    conn: &mut Connection,
    id: i64,
    now: i64,
) -> Result<Option<i64>> {
    let tx = begin_immediate(conn)?;
    let attempts = tx
        .query_row(
            "UPDATE tasks
             SET refs=json_set(
                     refs,
                     '$.ci_remediation_attempts',
                     COALESCE(json_extract(refs, '$.ci_remediation_attempts'), 0) + 1
                 ),
                 updated_at=?2
             WHERE id=?1 AND status='rework' AND json_valid(refs)
               AND json_extract(refs, '$.ci_remediation_requested')=1
             RETURNING json_extract(refs, '$.ci_remediation_attempts')",
            params![id, now],
            |row| row.get(0),
        )
        .optional()?;
    tx.commit()?;
    Ok(attempts)
}

/// Clear stale runtime ownership for a durable CI remediation after daemon
/// restart while preserving its rework status and exact retry intent.
pub fn reset_ci_remediation_for_recovery(conn: &mut Connection, id: i64, now: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let preserved = tx.execute(
        "UPDATE tasks SET assignee=NULL, updated_at=?2
         WHERE id=?1 AND status='rework' AND json_valid(refs)
           AND json_extract(refs, '$.ci_remediation_requested')=1",
        params![id, now],
    )? == 1;
    if preserved {
        tx.execute(
            "UPDATE claims SET active=0
             WHERE target=?1 AND active=1",
            params![lease_target(id)],
        )?;
        crate::events::emit(
            &tx,
            "ci_remediation_recovered",
            &lease_target(id),
            "daemon restart preserved CI remediation intent",
            now,
        )?;
    }
    tx.commit()?;
    Ok(preserved)
}

/// Atomically fail a reviewer only while it still owns the active review phase.
///
/// Reviewer processes are turn-oriented and may exit after their verdict has
/// already transferred the task to remediation. The ownership predicate and
/// `AgentFailed` transition must therefore share one write transaction.
pub fn fail_reviewer_if_owner(
    conn: &mut Connection,
    reviewer: &str,
    id: i64,
    reason: &str,
    now: i64,
) -> Result<Option<TransitionResult>> {
    let tx = begin_immediate(conn)?;
    let still_owns_review = tx
        .query_row(
            "SELECT 1 FROM tasks
             WHERE id=?1 AND status='in-review' AND reviewer=?2",
            params![id, reviewer],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !still_owns_review {
        tx.commit()?;
        return Ok(None);
    }

    apply_event_tx(
        tx,
        reviewer,
        id,
        &Event::AgentFailed {
            reason: reason.to_string(),
        },
        now,
        |_| Ok(()),
    )
    .map(Some)
}

/// Verdict supplied to late-review recovery. R1/R2 is deliberately not an
/// input: it is derived from the durable `agent_runs.sub_role` identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LateReviewerVerdict {
    Approved,
    Changes,
}

/// Fold a worker submission that committed before a daemon restart into the
/// lifecycle before generic recovery can discard the journal identity that
/// proves its authority. The exact mailbox row is re-read and consumed in the
/// same transaction as the lifecycle transition.
pub fn recover_late_worker_completion(
    conn: &mut Connection,
    mailbox_id: i64,
    agent: &str,
    task_id: i64,
    pr: i64,
    publication: Option<&PublishedWorkerPublication>,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let status: Option<String> = tx
        .query_row(
            "SELECT t.status
             FROM mailbox m
             JOIN tasks t ON t.id=m.task_id
             JOIN journal j ON j.agent=m.agent AND j.role='worker'
                           AND j.task_id=m.task_id
             WHERE m.id=?1 AND m.consumed_at IS NULL AND m.kind='done'
               AND m.verdict IS NULL AND m.agent=?2 AND m.task_id=?3
               AND (m.pr=?4 OR m.pr IS NULL)
               AND t.status IN ('working','rework') AND t.assignee=m.agent
               AND (json_extract(t.refs, '$.pr') IS NULL
                    OR json_extract(t.refs, '$.pr')=?4)
               AND EXISTS (
                   SELECT 1 FROM agent_runs ar
                   WHERE ar.task_id=m.task_id AND ar.agent_name=m.agent
                     AND ar.role='worker'
               )",
            params![mailbox_id, agent, task_id, pr],
            |row| row.get(0),
        )
        .optional()?;
    let Some(status) = status else {
        tx.commit()?;
        return Ok(false);
    };
    let event = if status == "rework" {
        Event::ReworkPushed
    } else {
        Event::SignaledDone { pr: pr.to_string() }
    };
    apply_event_tx(tx, agent, task_id, &event, now, |tx| {
        if let Some(publication) = publication {
            settle_published_worker_tx(tx, task_id, publication, now)?;
        } else {
            tx.execute(
                "UPDATE tasks
                 SET refs=json_remove(COALESCE(refs, '{}'), '$.daemon_publication')
                 WHERE id=?1",
                params![task_id],
            )?;
        }
        consume_late_mailbox(tx, mailbox_id, now)
    })
    .map(|_| true)
}

/// Fold a reviewer verdict that committed before a daemon restart. Validation,
/// approval persistence/invalidation, transition, remediation feedback, and
/// mailbox consumption are deliberately one immediate transaction.
#[allow(clippy::too_many_arguments)]
pub fn recover_late_reviewer_verdict(
    conn: &mut Connection,
    mailbox_id: i64,
    agent: &str,
    task_id: i64,
    pr: i64,
    verdict: LateReviewerVerdict,
    blocking_count: i64,
    reviewed_head_sha: &str,
    remediation_feedback: Option<&str>,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let reviewer_state: Option<(String, String)> = tx
        .query_row(
            "SELECT CASE WHEN ar.sub_role='r2' THEN 'r2' WHEN ar.sub_role IS NULL THEN 'r1' END,
                    t.status
             FROM mailbox m
             JOIN tasks t ON t.id=m.task_id
             JOIN journal j ON j.agent=m.agent AND j.role='reviewer'
                           AND j.task_id=m.task_id AND j.pr=m.pr
             JOIN agent_runs ar ON ar.id=(
                 SELECT MAX(ar2.id) FROM agent_runs ar2
                 WHERE ar2.task_id=m.task_id AND ar2.agent_name=m.agent
                   AND ar2.role='reviewer'
             )
             WHERE m.id=?1 AND m.consumed_at IS NULL AND m.kind='done'
               AND m.agent=?2 AND m.task_id=?3 AND m.pr=?4
               AND ((t.status='in-review' AND t.reviewer=m.agent)
                    OR (?5='changes' AND t.status='rework'))
               AND (ar.sub_role IS NULL OR ar.sub_role='r2')
               AND ((?5='approved' AND m.verdict='approved')
                    OR (?5='changes' AND m.verdict='changes'))",
            params![
                mailbox_id,
                agent,
                task_id,
                pr,
                match verdict {
                    LateReviewerVerdict::Approved => "approved",
                    LateReviewerVerdict::Changes => "changes",
                }
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((role, task_status)) = reviewer_state else {
        tx.commit()?;
        return Ok(false);
    };

    let author: String = tx.query_row(
        "SELECT COALESCE(author, '') FROM tasks WHERE id=?1",
        params![task_id],
        |row| row.get(0),
    )?;

    match verdict {
        LateReviewerVerdict::Approved => {
            if reviewed_head_sha.is_empty() {
                tx.commit()?;
                return Ok(false);
            }
            if role == "r2" {
                let valid_r1 = tx
                    .query_row(
                        "SELECT 1 FROM approvals
                         WHERE pr_number=?1 AND review_role='r1' AND task_id=?2
                           AND verdict='approved' AND blocking_count=0
                           AND approved_head_sha=?3 AND reviewer != ?4",
                        params![pr, task_id, reviewed_head_sha, agent],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if !valid_r1 {
                    tx.commit()?;
                    return Ok(false);
                }
                return apply_event_tx(tx, agent, task_id, &Event::VerdictApprove, now, |tx| {
                    upsert_late_approval(
                        tx,
                        pr,
                        &role,
                        task_id,
                        &author,
                        agent,
                        blocking_count,
                        reviewed_head_sha,
                        now,
                    )?;
                    consume_late_mailbox(tx, mailbox_id, now)
                })
                .map(|_| true);
            }
            upsert_late_approval(
                &tx,
                pr,
                &role,
                task_id,
                &author,
                agent,
                blocking_count,
                reviewed_head_sha,
                now,
            )?;
            consume_late_mailbox(&tx, mailbox_id, now)?;
            tx.commit()?;
            Ok(true)
        }
        LateReviewerVerdict::Changes => {
            let remediation_feedback = remediation_feedback
                .map(str::trim)
                .filter(|feedback| !feedback.is_empty())
                .unwrap_or("Changes requested.");
            // The live path commits VerdictChanges before installing the
            // sticky worker's replacement lease. A daemon death in that gap
            // leaves this exact reviewer result unconsumed with the task
            // already in rework. Preserve the feedback and consume the
            // reviewer authority here; dormant recovery will re-install the
            // exact worker lease and resume its continuation.
            if task_status == "rework" {
                tx.execute(
                    "DELETE FROM approvals WHERE pr_number=?1 AND task_id=?2",
                    params![pr, task_id],
                )?;
                tx.execute(
                    "UPDATE tasks SET refs=json_set(COALESCE(refs, '{}'),
                       '$.remediation_feedback', ?2)
                     WHERE id=?1 AND status='rework'",
                    params![task_id, remediation_feedback],
                )?;
                consume_late_mailbox(&tx, mailbox_id, now)?;
                tx.commit()?;
                return Ok(true);
            }
            apply_event_tx(tx, agent, task_id, &Event::VerdictChanges, now, |tx| {
                tx.execute(
                    "DELETE FROM approvals WHERE pr_number=?1 AND task_id=?2",
                    params![pr, task_id],
                )?;
                tx.execute(
                    "UPDATE tasks SET assignee=NULL,
                     refs=json_set(COALESCE(refs, '{}'),
                       '$.daemon_rework_retry_requested', json('true'),
                       '$.remediation_feedback', ?2)
                     WHERE id=?1 AND status='rework'",
                    params![task_id, remediation_feedback],
                )?;
                consume_late_mailbox(tx, mailbox_id, now)
            })
            .map(|_| true)
        }
    }
}

fn consume_late_mailbox(tx: &Transaction<'_>, mailbox_id: i64, now: i64) -> Result<()> {
    let changed = tx.execute(
        "UPDATE mailbox SET consumed_at=?1 WHERE id=?2 AND consumed_at IS NULL",
        params![now, mailbox_id],
    )?;
    if changed != 1 {
        return Err(QuorumError::Io(format!(
            "late mailbox row {mailbox_id} was not consumed"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn upsert_late_approval(
    tx: &Transaction<'_>,
    pr: i64,
    role: &str,
    task_id: i64,
    author: &str,
    reviewer: &str,
    blocking_count: i64,
    reviewed_head_sha: &str,
    now: i64,
) -> Result<()> {
    tx.execute(
        "INSERT INTO approvals
           (pr_number, review_role, task_id, author, reviewer, verdict,
            blocking_count, approved_head_sha, created_at)
         VALUES (?1,?2,?3,?4,?5,'approved',?6,?7,?8)
         ON CONFLICT(pr_number, review_role) DO UPDATE SET
           task_id=excluded.task_id, author=excluded.author,
           reviewer=excluded.reviewer, verdict=excluded.verdict,
           blocking_count=excluded.blocking_count,
           approved_head_sha=excluded.approved_head_sha,
           created_at=excluded.created_at",
        params![
            pr,
            role,
            task_id,
            author,
            reviewer,
            blocking_count,
            reviewed_head_sha,
            now
        ],
    )?;
    Ok(())
}

fn apply_event_tx<F>(
    tx: Transaction<'_>,
    agent: &str,
    id: i64,
    event: &Event,
    now: i64,
    before_commit: F,
) -> Result<TransitionResult>
where
    F: FnOnce(&Transaction<'_>) -> Result<()>,
{
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;

    let task = tx
        .query_row(
            &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
            params![id],
            row_to_task,
        )
        .optional()?
        .ok_or_else(|| QuorumError::Usage(format!("task {id} not found")))?;

    let status = task.status.parse::<Status>().map_err(QuorumError::Usage)?;

    match event {
        Event::PlanningStarted
        | Event::PlanMaterialized
        | Event::PlanningBlocked { .. }
        | Event::GraphCompleted => {
            tx.commit()?;
            return Err(QuorumError::Usage(
                "decomposition lifecycle events require daemon authority".into(),
            ));
        }
        Event::SignaledDone { .. } | Event::ReworkPushed => {
            // Authorize by current assignee (fast path), then by active run
            // capability (handles replacement workers whose author field was
            // preserved for branch-naming provenance).
            let is_assignee = task.assignee.as_deref() == Some(agent);
            if !is_assignee {
                let has_cap =
                    crate::capabilities::active_for_agent_task(&tx, agent, id, "worker")?.is_some();
                if !has_cap {
                    tx.commit()?;
                    return Err(QuorumError::NotHolder);
                }
            }
        }
        Event::VerdictApprove | Event::VerdictChanges => {
            if task.reviewer.as_deref() != Some(agent) {
                tx.commit()?;
                return Err(QuorumError::NotHolder);
            }
        }
        Event::Claimed { .. }
        | Event::ReviewerAttached { .. }
        | Event::ChecksFailed { .. }
        | Event::LeaseExpired
        | Event::AgentFailed { .. }
        | Event::ControlledShutdown
        | Event::Cancelled { .. }
        | Event::MergeSucceeded
        | Event::MergeFailed { .. }
        | Event::MergeConflict
        | Event::PrFoundMerged
        | Event::PrFoundClosed => {}
    }

    let view = TaskView {
        status,
        author: task.author.clone(),
        reviewer: task.reviewer.clone(),
        rework_round: task.rework_round as u32,
        rework_cap: task.effective_rework_cap(),
        pr: extract_pr_from_refs(&task.refs),
        review_only: task.review_only,
    };

    let (mut new_status, mut effects) = crate::lifecycle::transition(&view, event)
        .map_err(|e| QuorumError::Usage(e.to_string()))?;

    // Recovery budget: crash-recovery transitions (Working/Rework → Open via
    // AgentFailed/LeaseExpired) are bounded. Park loudly in Failed when exhausted;
    // only an explicit caller may originate Cancelled.
    let is_crash_recovery = new_status == Status::Open
        && matches!(status, Status::Working | Status::Rework)
        && matches!(event, Event::AgentFailed { .. } | Event::LeaseExpired);

    let failure_cause = match event {
        Event::AgentFailed { reason } => reason.as_str(),
        Event::LeaseExpired => "lease expired",
        _ => "unknown",
    };

    if is_crash_recovery && task.recovery_attempts >= MAX_RECOVERY_ATTEMPTS {
        new_status = Status::Failed;
        effects.retain(|e| !matches!(e, Effect::NotifyOwner { .. }));
        if !effects.contains(&Effect::ReleaseLease) {
            effects.push(Effect::ReleaseLease);
        }
        effects.push(Effect::NotifyOwner {
            reason: format!(
                "recovery budget exhausted ({}/{MAX_RECOVERY_ATTEMPTS} attempts); \
                 last failure: {failure_cause}",
                task.recovery_attempts,
            ),
        });
    }

    // Reset recovery counter on meaningful lifecycle handoff.
    let reset_recovery = matches!(
        (&status, &new_status, event),
        (
            Status::Working,
            Status::InReview,
            Event::SignaledDone { .. }
        ) | (Status::Rework, Status::InReview, Event::ReworkPushed)
    );
    let recovery_attempts = if reset_recovery {
        0
    } else if is_crash_recovery && new_status == Status::Open {
        task.recovery_attempts + 1
    } else {
        task.recovery_attempts
    };

    let new_status_str = new_status.to_string();
    let mut author = task.author.clone();
    let mut reviewer = task.reviewer.clone();
    let mut rework_round = task.rework_round;
    let mut assignee = task.assignee.clone();
    let mut refs = task.refs.clone();
    if is_crash_recovery && new_status == Status::Failed {
        refs = Some(set_parked_refs(
            refs.as_deref(),
            failure_cause,
            if status == Status::Rework {
                "rework"
            } else {
                "open"
            },
        )?);
    }
    // Review-only remediation death: the lifecycle layer already chose Failed
    // (park, never bounce to review — a replacement reviewer on the unchanged
    // head burns a rework round with zero remediation applied). Write durable
    // owner-gated park markers only.
    // Every Failed park is owner-gated. In particular, daemon teardown must
    // not put a runnable retry marker on a terminal row: after restart the
    // pre-shutdown selection is no longer authoritative.
    let is_remediation_death_park = task.review_only
        && status == Status::Rework
        && new_status == Status::Failed
        && matches!(event, Event::AgentFailed { .. } | Event::LeaseExpired);
    if is_remediation_death_park {
        refs = Some(set_parked_refs(refs.as_deref(), failure_cause, "rework")?);
    }

    for eff in &effects {
        match eff {
            Effect::SetAuthor { agent } => {
                author = Some(agent.clone());
                assignee = Some(agent.clone());
            }
            Effect::SetReviewer { agent } => {
                reviewer = Some(agent.clone());
                assignee = Some(agent.clone());
            }
            Effect::ClearAuthor => {
                author = None;
            }
            Effect::IncrementReworkRound => {
                rework_round += 1;
            }
            Effect::ReleaseLease => {
                deactivate_lease(&tx, id, now)?;
                if new_status.is_terminal() || new_status == Status::Open {
                    assignee = None;
                } else if new_status == Status::InReview {
                    reviewer = None;
                    assignee = None;
                }
            }
            Effect::NotifyOwner { reason } => {
                let alert_body = format!("task #{id}: {reason}");
                let expires_at = now + crate::feed::DEFAULT_MESSAGE_TTL_SECS;
                tx.execute(
                    "INSERT INTO messages(ts, author, topic, kind, body, refs, expires_at, recipient)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        now,
                        agent,
                        crate::feed::DEFAULT_TOPIC,
                        "alert",
                        alert_body,
                        format!("task:{id}"),
                        expires_at,
                        "owner"
                    ],
                )?;
            }
            Effect::PostFindingsNote => {
                tx.execute(
                    "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        id,
                        now,
                        agent,
                        "review-only task failed: reviewer requested changes"
                    ],
                )?;
            }
            Effect::ResumeWorker => {
                // Rework phase: restore assignee from reviewer back to the
                // worker (author). If a replacement worker later pushes
                // rework, the assignee check may still miss — the capability
                // fallback in the authorization match covers that case.
                assignee = author.clone();
            }
            _ => {}
        }
    }

    if let Event::SignaledDone { pr } = event {
        refs = Some(merge_pr_into_refs(&task.refs, pr));
    }
    if matches!(event, Event::SignaledDone { .. } | Event::ReworkPushed)
        && new_status == Status::InReview
    {
        refs = clear_runner_retry_refs(refs.as_deref())?;
    }

    // Only daemon-observed merge events can establish merged completion
    // provenance through the lifecycle path. Other transitions preserve the
    // existing value; historical NULL remains unknown rather than inferred.
    let completion_provenance = matches!(event, Event::MergeSucceeded | Event::PrFoundMerged)
        .then_some(COMPLETION_PROVENANCE_MERGED);

    tx.execute(
        "UPDATE tasks SET status=?1, assignee=?2, author=?3, reviewer=?4, \
         rework_round=?5, refs=?6, updated_at=?7, recovery_attempts=?9, \
         completion_provenance=COALESCE(?10,completion_provenance) WHERE id=?8",
        params![
            new_status_str,
            assignee,
            author,
            reviewer,
            rework_round,
            refs,
            now,
            id,
            recovery_attempts,
            completion_provenance,
        ],
    )?;

    let event_kind = format!("task_{}", new_status_str.replace('-', "_"));
    crate::events::emit(
        &tx,
        &event_kind,
        &lease_target(id),
        &format!("by {agent}"),
        now,
    )?;

    // Generated children use the ordinary lifecycle, but the final transition
    // to done also owns the decomposition aggregate. Keep child completion,
    // graph completion, and source completion in this same write transaction
    // for every event that can reach done (currently MergeSucceeded and
    // PrFoundMerged).
    if new_status == Status::Done {
        crate::decomposition::complete_graph_if_final_child(&tx, id, now)?;
    }

    before_commit(&tx)?;

    let mut result_task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    result_task.ready = compute_ready(&tx, &result_task.depends_on)?;
    tx.commit()?;

    Ok(TransitionResult {
        task: result_task,
        effects,
    })
}

fn clear_runner_retry_refs(refs: Option<&str>) -> Result<Option<String>> {
    let Some(refs) = refs else {
        return Ok(None);
    };
    let mut value: serde_json::Value = serde_json::from_str(refs)
        .map_err(|error| QuorumError::Io(format!("invalid persisted refs JSON: {error}")))?;
    if value.is_object() {
        // The worker delivered a PR, so a staged provider retry is stale —
        // replaying it would re-run already-completed work. Clear both the
        // neutral representation and historical Codex state.
        runner_state::clear_provider_retry(&mut value);
        let object = value.as_object_mut().expect("checked task refs object");
        for key in [
            // Round-scoped remediation context becomes stale once the push
            // completes this round.
            "remediation_feedback",
            PARKED_HEAD_CHECK_REF,
            PARKED_REWORK_RETRY_REF,
            CI_REMEDIATION_REQUESTED_REF,
            CI_REMEDIATION_PR_REF,
            CI_REMEDIATION_HEAD_SHA_REF,
            CI_REMEDIATION_FEEDBACK_REF,
            CI_REMEDIATION_CHECKS_REF,
            CI_REMEDIATION_ATTEMPTS_REF,
        ] {
            object.remove(key);
        }
    }
    Ok(Some(value.to_string()))
}

// ── set_body (daemon post-event body annotation) ─────────────────────────────

pub fn set_body(conn: &mut Connection, id: i64, body: &str, now: i64) -> Result<()> {
    let tx = begin_immediate(conn)?;
    let current: Option<(Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT body, refs FROM tasks WHERE id=?1",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((existing_body, existing_refs)) = current else {
        tx.commit()?;
        return Ok(());
    };
    let refs = if existing_body.as_deref() == Some(body) {
        existing_refs
    } else {
        invalidate_classifier_refs(&existing_refs, None)
    };
    tx.execute(
        "UPDATE tasks SET body=?1, refs=?2, updated_at=?3 WHERE id=?4",
        params![body, refs, now, id],
    )?;
    tx.commit()?;
    Ok(())
}

// ── update (backward compat for serve/) ───────────────────────────────────────

pub fn update(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    fields: &TaskUpdate,
    now: i64,
) -> Result<Task> {
    let edit_requested =
        fields.body.is_some() || fields.refs.is_some() || fields.depends_on.is_some();
    if fields.status == Some("cancelled") && !edit_requested {
        match crate::decomposition::cancel_source_graph(
            conn,
            agent,
            id,
            fields.expected_revision,
            now,
        )? {
            crate::decomposition::SourceCancellation::Cancelled => {
                return get(conn, id)?.ok_or(QuorumError::NotHolder);
            }
            crate::decomposition::SourceCancellation::Rejected => {
                return Err(QuorumError::NotHolder);
            }
            crate::decomposition::SourceCancellation::NotGraphSource => {}
        }
    }
    struct EditableSnapshot {
        body: Option<String>,
        depends_on: Option<String>,
        refs: Option<String>,
        revision: i64,
        edit_count: i64,
    }

    if edit_requested && fields.expected_revision.is_none() {
        return Err(QuorumError::Usage(
            "task edits require --expected-revision".into(),
        ));
    }
    if let Some(s) = fields.status {
        if !STATUSES.contains(&s) {
            return Err(QuorumError::Usage(format!("invalid status: {s}")));
        }
        if s == "done" {
            return Err(QuorumError::Usage(
                "task-update cannot set status 'done'; use `quorum submit` (finished your part) or `quorum task-close` (manual close)".into()
            ));
        }
        let restricted = ["working", "in-review", "rework", "merging", "failed"];
        if restricted.contains(&s) {
            return Err(QuorumError::Usage(format!(
                "task-update cannot set status '{s}'; use lifecycle events instead"
            )));
        }
    }
    if let Some(dep_json) = fields.depends_on {
        validate_depends_on(dep_json)?;
    }

    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let existing: Option<EditableSnapshot> = tx
        .query_row(
            "SELECT body, depends_on, refs, revision, edit_count FROM tasks WHERE id=?1",
            params![id],
            |row| {
                Ok(EditableSnapshot {
                    body: row.get(0)?,
                    depends_on: row.get(1)?,
                    refs: row.get(2)?,
                    revision: row.get(3)?,
                    edit_count: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(existing) = existing else {
        return Err(QuorumError::NotHolder);
    };
    let existing_body = existing.body;
    let existing_depends_on = existing.depends_on;
    let existing_refs = existing.refs;
    let revision = existing.revision;
    let edit_count = existing.edit_count;
    let classifier_input_changed = fields
        .body
        .is_some_and(|body| existing_body.as_deref() != Some(body))
        || fields
            .depends_on
            .is_some_and(|depends_on| existing_depends_on.as_deref() != Some(depends_on));
    let preserved_refs = if classifier_input_changed {
        invalidate_classifier_refs(&existing_refs, fields.refs)
    } else {
        preserve_creator_protected_refs(&existing_refs, fields.refs)
    };
    let edit_changed = fields
        .body
        .is_some_and(|body| existing_body.as_deref() != Some(body))
        || fields
            .depends_on
            .is_some_and(|depends_on| existing_depends_on.as_deref() != Some(depends_on))
        || (fields.refs.is_some() && preserved_refs != existing_refs);

    if edit_requested
        && (fields.expected_revision != Some(revision) || !edit_changed || edit_count >= 3)
    {
        return Err(QuorumError::NotHolder);
    }

    let generated_member: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM task_graph_members WHERE task_id=?1)",
        [id],
        |row| row.get(0),
    )?;
    let graph_source: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM task_decompositions
         WHERE source_task_id=?1 AND state NOT IN ('completed','cancelled'))",
        [id],
        |row| row.get(0),
    )?;
    if fields.status == Some("cancelled") && (generated_member || graph_source) {
        return Err(QuorumError::Usage(
            "decomposed tasks must be cancelled through graph cancellation".into(),
        ));
    }
    if edit_requested
        && (generated_member
            || tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM task_decompositions
                 WHERE source_task_id=?1 AND accepted_plan_revision IS NOT NULL)",
                [id],
                |row| row.get::<_, bool>(0),
            )?)
    {
        return Err(QuorumError::Usage(
            "materialized decomposition tasks are immutable".into(),
        ));
    }

    let mut cancel_applied = false;
    let mut n = match fields.status {
        Some("open") => tx.execute(
            "UPDATE tasks SET
                    status='open', assignee=NULL,
                    body  = COALESCE(?3, body),
                    refs  = COALESCE(?4, refs),
                    updated_at = ?5
                 WHERE id=?1 AND assignee=?6 AND status='working'",
            params![
                id,
                "open",
                fields.body,
                preserved_refs.as_deref(),
                now,
                agent
            ],
        )?,
        Some("cancelled") => {
            let rows = tx.execute(
                "UPDATE tasks SET
                    status='cancelled',
                    body  = COALESCE(?3, body),
                    refs  = COALESCE(?4, refs),
                    updated_at = ?5
                 WHERE id=?1 AND (created_by=?6 OR assignee=?6)
                       AND status NOT IN ('done', 'failed', 'cancelled')",
                params![
                    id,
                    "cancelled",
                    fields.body,
                    preserved_refs.as_deref(),
                    now,
                    agent
                ],
            )?;
            cancel_applied = rows > 0;
            rows
        }
        _ => {
            let rows = tx.execute(
                "UPDATE tasks SET
                    status   = COALESCE(?2, status),
                    body     = COALESCE(?3, body),
                    refs     = COALESCE(?4, refs),
                    updated_at = ?5
                 WHERE id=?1 AND assignee=?6 AND status='working'",
                params![
                    id,
                    fields.status,
                    fields.body,
                    preserved_refs.as_deref(),
                    now,
                    agent
                ],
            )?;
            if rows == 0 && fields.status.is_none() {
                tx.execute(
                    "UPDATE tasks SET
                        body     = COALESCE(?2, body),
                        refs     = COALESCE(?3, refs),
                        updated_at = ?4
                     WHERE id=?1 AND created_by=?5 AND assignee IS NULL
                       AND status IN ('open','planning')",
                    params![id, fields.body, preserved_refs.as_deref(), now, agent],
                )?
            } else {
                rows
            }
        }
    };
    if n == 0 && edit_requested && graph_source && fields.status.is_none() {
        n = tx.execute(
            "UPDATE tasks SET body=COALESCE(?2,body),refs=COALESCE(?3,refs),
                    depends_on=COALESCE(?4,depends_on),updated_at=?5
             WHERE id=?1 AND created_by=?6 AND assignee IS NULL
               AND status IN ('planning','failed')",
            params![
                id,
                fields.body,
                preserved_refs.as_deref(),
                fields.depends_on,
                now,
                agent
            ],
        )?;
    }
    if n == 0 && fields.depends_on.is_none() {
        tx.commit()?;
        return Err(QuorumError::NotHolder);
    }

    if let Some(dep_json) = fields.depends_on {
        let dep_rows = tx.execute(
            "UPDATE tasks SET depends_on=?2, refs=COALESCE(?3, refs), updated_at=?4
             WHERE id=?1 AND (created_by=?5 OR assignee=?5)
                   AND status NOT IN ('done', 'cancelled')
                   AND (
                       status != 'failed'
                       OR (
                           json_valid(refs)
                           AND json_extract(refs, '$.daemon_parked')=1
                       )
                   )",
            params![id, dep_json, preserved_refs.as_deref(), now, agent],
        )?;
        if dep_rows == 0 && n == 0 {
            tx.commit()?;
            return Err(QuorumError::NotHolder);
        }
    }

    if edit_requested {
        let revision_rows = tx.execute(
            "UPDATE tasks SET revision=revision+1,edit_count=edit_count+1,updated_at=?3
             WHERE id=?1 AND revision=?2 AND edit_count < 3",
            params![id, revision, now],
        )?;
        if revision_rows != 1 {
            return Err(QuorumError::NotHolder);
        }
        // An accepted edit before materialization cancels the pending admission
        // aggregate and returns the new revision to ordinary classification.
        if graph_source {
            let graph_id: i64 = tx.query_row(
                "SELECT id FROM task_decompositions WHERE source_task_id=?1
                 AND accepted_plan_revision IS NULL AND active=0",
                [id],
                |row| row.get(0),
            )?;
            tx.execute(
                "DELETE FROM decomposition_attempts WHERE graph_id=?1",
                [graph_id],
            )?;
            tx.execute(
                "DELETE FROM decomposition_cleanup WHERE graph_id=?1",
                [graph_id],
            )?;
            tx.execute(
                "DELETE FROM task_graph_members WHERE graph_id=?1",
                [graph_id],
            )?;
            tx.execute("DELETE FROM task_decompositions WHERE id=?1", [graph_id])?;
            tx.execute(
                "UPDATE tasks SET status='open',assignee=NULL WHERE id=?1",
                [id],
            )?;
        }
    }

    if fields.status.is_none() && fields.body.is_some() {
        let is_unclaimed: bool = tx.query_row(
            "SELECT assignee IS NULL AND status='open' FROM tasks WHERE id=?1",
            params![id],
            |r| r.get(0),
        )?;
        if is_unclaimed {
            tx.execute(
                "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, ?3, ?4)",
                params![id, now, agent, format!("body replaced by creator {agent}")],
            )?;
        }
    }

    if fields.status == Some("open") {
        deactivate_lease(&tx, id, now)?;
        crate::events::emit(
            &tx,
            "task_released",
            &lease_target(id),
            &format!("released by {agent}"),
            now,
        )?;
    } else if fields.status == Some("cancelled") && cancel_applied {
        deactivate_lease(&tx, id, now)?;
        crate::events::emit(
            &tx,
            "task_cancelled",
            &lease_target(id),
            &format!("cancelled by {agent}"),
            now,
        )?;
        // Refresh durable park refs of parked dependents in the same tx,
        // so a following status read sees the unsatisfiable disposition
        // without waiting for another mutation to trigger a sweep.
        // Gated on `cancel_applied`: a combined status+depends_on edit whose
        // cancel UPDATE affects zero rows (e.g. task already 'failed') must
        // not falsely relabel dependents as cancelled — the guarded
        // depends_on branch may still have applied but the id is not
        // cancelled.
        converge_parked_dependents_of_cancelled(&tx, id, now)?;
    }

    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
    tx.commit()?;
    Ok(task)
}

/// Daemon-authoritative refs update — bypasses the assignee guard.
/// Used for internal bookkeeping (e.g. persisting runner continuation IDs)
/// where the daemon needs to write task metadata it doesn't "own" via
/// the normal agent-scoped update path.
pub fn update_refs_daemon(conn: &mut Connection, id: i64, refs: &str, now: i64) -> Result<()> {
    let tx = begin_immediate(conn)?;
    let existing: Option<String> = tx
        .query_row("SELECT refs FROM tasks WHERE id=?1", params![id], |r| {
            r.get(0)
        })
        .optional()?
        .flatten();
    let refs = preserve_classifier_refs(&existing, Some(refs)).unwrap_or_else(|| refs.to_string());
    tx.execute(
        "UPDATE tasks SET refs=?2, updated_at=?3 WHERE id=?1",
        params![id, refs, now],
    )?;
    tx.commit()?;
    Ok(())
}

/// Persist daemon-owned publication progress without replacing unrelated refs.
/// The JSON payload is validated by SQLite and survives daemon restarts between
/// remote push, PR creation, verification, and lifecycle transition.
pub fn set_publication_intent(
    conn: &Connection,
    id: i64,
    intent_json: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE tasks
         SET refs=json_set(COALESCE(refs, '{}'), '$.daemon_publication', json(?2)),
             updated_at=?3
         WHERE id=?1",
        params![id, intent_json, now],
    )?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeadTurnRunnerDisposition {
    DonePending,
    DeliveryRecorded,
    OwnershipTransferred,
    ProviderBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedRunRole {
    Worker,
    Reviewer,
}

pub enum ManagedExitDisposition {
    OutcomePending,
    OutcomeRecorded,
    OwnershipTransferred,
    AgentFailed(Box<TransitionResult>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedExitClassification {
    OutcomePending,
    OutcomeRecorded,
    OwnershipTransferred,
    ActiveWithoutOutcome,
}

fn classify_managed_exit_tx(
    tx: &Transaction<'_>,
    role: ManagedRunRole,
    agent: &str,
    id: i64,
    run_id: Option<&str>,
) -> Result<ManagedExitClassification> {
    let task = tx
        .query_row(
            "SELECT status, assignee, reviewer FROM tasks WHERE id=?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((status, assignee, reviewer)) = task else {
        return Ok(ManagedExitClassification::OwnershipTransferred);
    };

    let role_name = match role {
        ManagedRunRole::Worker => "worker",
        ManagedRunRole::Reviewer => "reviewer",
    };
    let exact_run_active = match run_id {
        Some(run_id) => {
            crate::capabilities::managed_run_is_active_tx(tx, run_id, agent, id, role_name)?
        }
        None => true,
    };
    let (owns_phase, outcome_predicate) = match role {
        ManagedRunRole::Worker => {
            let has_capability = if run_id.is_some() {
                exact_run_active
            } else {
                crate::capabilities::active_for_agent_task(tx, agent, id, "worker")?.is_some()
            };
            (
                matches!(status.as_str(), "working" | "rework")
                    && exact_run_active
                    && (assignee.as_deref() == Some(agent) || has_capability),
                // Initial daemon-owned publication deliberately submits without
                // a PR; the daemon resolves/creates it after consuming the row.
                // Ownership is checked before a consumed outcome can count, so
                // an earlier round still cannot hide a current worker failure.
                "verdict IS NULL",
            )
        }
        ManagedRunRole::Reviewer => (
            exact_run_active && status == "in-review" && reviewer.as_deref() == Some(agent),
            "verdict IS NOT NULL",
        ),
    };
    let has_pending_outcome = tx.query_row(
        &format!(
            "SELECT EXISTS(
                SELECT 1 FROM mailbox
                WHERE agent=?1 AND kind='done' AND task_id=?2
                  AND consumed_at IS NULL AND {outcome_predicate}
            )"
        ),
        params![agent, id],
        |row| row.get::<_, bool>(0),
    )?;
    if exact_run_active && has_pending_outcome {
        return Ok(ManagedExitClassification::OutcomePending);
    }
    if owns_phase {
        return Ok(ManagedExitClassification::ActiveWithoutOutcome);
    }
    let has_recorded_outcome = tx.query_row(
        &format!(
            "SELECT EXISTS(
                SELECT 1 FROM mailbox
                WHERE agent=?1 AND kind='done' AND task_id=?2
                  AND consumed_at IS NOT NULL AND {outcome_predicate}
            )"
        ),
        params![agent, id],
        |row| row.get::<_, bool>(0),
    )?;
    if exact_run_active && has_recorded_outcome {
        return Ok(ManagedExitClassification::OutcomeRecorded);
    }
    Ok(ManagedExitClassification::OwnershipTransferred)
}

/// Retire only this worker's task authority after its managed process can no
/// longer serve a later rework turn. A successor may already own the task by
/// the time a terminal provider event is drained, so the holder predicate is
/// load-bearing: cleanup for the old process must never deactivate the new
/// owner's lease.
fn settle_retiring_worker_lease_tx(
    tx: &Transaction<'_>,
    agent: &str,
    id: i64,
    run_id: Option<&str>,
) -> Result<()> {
    if let Some(run_id) = run_id {
        // The retiring capability has already been revoked in this
        // transaction. A later active capability for the same reusable
        // name/task identifies a successor, so its lease must survive; older
        // leaked capabilities do not block retirement of this lease.
        tx.execute(
            "UPDATE claims SET active=0
             WHERE target=?1 AND holder=?2 AND active=1
               AND NOT EXISTS (
                   SELECT 1
                   FROM run_capabilities successor
                   JOIN run_capabilities retiring ON retiring.run_id=?4
                   WHERE successor.agent=?2 AND successor.task_id=?3
                     AND successor.role='worker' AND successor.revoked_at IS NULL
                     AND successor.rowid > retiring.rowid
               )",
            params![lease_target(id), agent, id, run_id],
        )?;
    } else {
        tx.execute(
            "UPDATE claims SET active=0
             WHERE target=?1 AND holder=?2 AND active=1",
            params![lease_target(id), agent],
        )?;
    }
    Ok(())
}

/// Atomically classify a managed process exit and fail only a run that still
/// owns its lifecycle phase and has produced no durable outcome.
///
/// A pending matching mailbox row is authoritative. A consumed row is evidence
/// of completion only after the run no longer owns the active phase, so stale
/// rows from earlier rework/review rounds cannot hide a current failure. The
/// ownership check and `AgentFailed` transition share the same immediate
/// transaction. Cleanup-only worker dispositions retire that worker's exact
/// lease in the transaction as well, without deactivating a successor holder.
pub fn dispose_managed_exit(
    conn: &mut Connection,
    role: ManagedRunRole,
    agent: &str,
    id: i64,
    reason: &str,
    now: i64,
) -> Result<ManagedExitDisposition> {
    dispose_managed_exit_inner(conn, role, agent, id, None, reason, now)
}

/// Exact-run variant used by the daemon for all managed process teardown.
/// Classification, capability revocation, and any lifecycle mutation share
/// one immediate transaction.
pub fn dispose_managed_run_exit(
    conn: &mut Connection,
    role: ManagedRunRole,
    agent: &str,
    id: i64,
    run_id: &str,
    reason: &str,
    now: i64,
) -> Result<ManagedExitDisposition> {
    dispose_managed_exit_inner(conn, role, agent, id, Some(run_id), reason, now)
}

fn dispose_managed_exit_inner(
    conn: &mut Connection,
    role: ManagedRunRole,
    agent: &str,
    id: i64,
    run_id: Option<&str>,
    reason: &str,
    now: i64,
) -> Result<ManagedExitDisposition> {
    let tx = begin_immediate(conn)?;
    let role_name = match role {
        ManagedRunRole::Worker => "worker",
        ManagedRunRole::Reviewer => "reviewer",
    };
    match classify_managed_exit_tx(&tx, role, agent, id, run_id)? {
        ManagedExitClassification::OutcomePending => {
            tx.commit()?;
            return Ok(ManagedExitDisposition::OutcomePending);
        }
        ManagedExitClassification::OutcomeRecorded => {
            if let Some(run_id) = run_id {
                crate::capabilities::revoke_managed_run_tx(&tx, run_id, agent, id, role_name, now)?;
            }
            if role == ManagedRunRole::Worker {
                settle_retiring_worker_lease_tx(&tx, agent, id, run_id)?;
            }
            tx.commit()?;
            return Ok(ManagedExitDisposition::OutcomeRecorded);
        }
        ManagedExitClassification::OwnershipTransferred => {
            if let Some(run_id) = run_id {
                crate::capabilities::revoke_managed_run_tx(&tx, run_id, agent, id, role_name, now)?;
            }
            if role == ManagedRunRole::Worker {
                settle_retiring_worker_lease_tx(&tx, agent, id, run_id)?;
            }
            tx.commit()?;
            return Ok(ManagedExitDisposition::OwnershipTransferred);
        }
        ManagedExitClassification::ActiveWithoutOutcome => {}
    }

    apply_event_tx(
        tx,
        agent,
        id,
        &Event::AgentFailed {
            reason: reason.to_string(),
        },
        now,
        |tx| {
            if let Some(run_id) = run_id {
                crate::capabilities::revoke_managed_run_tx(tx, run_id, agent, id, role_name, now)?;
            }
            Ok(())
        },
    )
    .map(|transition| ManagedExitDisposition::AgentFailed(Box::new(transition)))
}

/// Atomically distinguish a submitted turn-oriented worker from a provider
/// failure.
///
/// The immediate transaction serializes the Done-row check with both mailbox
/// append and provider-block persistence, so committed work cannot be staged
/// for a duplicate retry through a check/write race.
pub fn dispose_dead_turn_runner(
    conn: &mut Connection,
    id: i64,
    agent: &str,
    block: &ProviderBlock,
    pending_turn: &PendingTurn,
    now: i64,
) -> Result<DeadTurnRunnerDisposition> {
    let tx = begin_immediate(conn)?;
    match classify_managed_exit_tx(&tx, ManagedRunRole::Worker, agent, id, None)? {
        ManagedExitClassification::OutcomePending => {
            tx.commit()?;
            return Ok(DeadTurnRunnerDisposition::DonePending);
        }
        ManagedExitClassification::OutcomeRecorded => {
            tx.commit()?;
            return Ok(DeadTurnRunnerDisposition::DeliveryRecorded);
        }
        ManagedExitClassification::OwnershipTransferred
        | ManagedExitClassification::ActiveWithoutOutcome => {}
    }
    let task = tx
        .query_row(
            "SELECT status, refs, author, assignee FROM tasks WHERE id=?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((status, refs_raw, author, assignee)) = task else {
        tx.commit()?;
        return Ok(DeadTurnRunnerDisposition::OwnershipTransferred);
    };
    // Match apply_event's worker submission authority: the current assignee
    // owns the phase directly, while a daemon-issued active worker capability
    // authorizes replacement/remediation workers whose preserved `author`
    // value still names the original branch author.
    let has_worker_capability =
        crate::capabilities::active_for_agent_task(&tx, agent, id, "worker")?.is_some();
    let owns_worker_phase = assignee.as_deref() == Some(agent) || has_worker_capability;
    if status != "working" && status != "rework" {
        let disposition = if status == "in-review"
            && (author.as_deref() == Some(agent) || has_worker_capability)
            && extract_pr_number(&refs_raw).is_some()
        {
            DeadTurnRunnerDisposition::DeliveryRecorded
        } else {
            DeadTurnRunnerDisposition::OwnershipTransferred
        };
        tx.commit()?;
        return Ok(disposition);
    }
    if !owns_worker_phase {
        tx.commit()?;
        return Ok(DeadTurnRunnerDisposition::OwnershipTransferred);
    }
    let mut refs: serde_json::Value = refs_raw
        .as_deref()
        .and_then(|refs| serde_json::from_str(refs).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    if block.provider.is_empty()
        || block.reason.is_empty()
        || block.provider != pending_turn.provider
        || !runner_state::pending_turn_is_complete(pending_turn)
    {
        return Err(QuorumError::Io(format!(
            "provider block '{}' does not match a complete pending turn '{}'",
            block.provider, pending_turn.provider
        )));
    }
    runner_state::set_provider_block(&mut refs, block, pending_turn);
    tx.execute(
        "UPDATE tasks SET refs=?2, updated_at=?3 WHERE id=?1",
        params![id, refs.to_string(), now],
    )?;
    tx.commit()?;
    Ok(DeadTurnRunnerDisposition::ProviderBlocked)
}

pub(crate) fn set_parked_refs(
    refs: Option<&str>,
    reason: &str,
    resume_status: &str,
) -> Result<String> {
    let mut value: serde_json::Value = match refs {
        Some(raw) => serde_json::from_str(raw)
            .map_err(|e| QuorumError::Io(format!("invalid task refs JSON: {e}")))?,
        None => serde_json::json!({}),
    };
    let object = value
        .as_object_mut()
        .ok_or_else(|| QuorumError::Io("task refs must be a JSON object".into()))?;
    object.insert(PARKED_REF.into(), serde_json::Value::Bool(true));
    object.insert(
        PARKED_REASON_REF.into(),
        serde_json::Value::String(reason.into()),
    );
    object.insert(
        PARKED_RESUME_STATUS_REF.into(),
        serde_json::Value::String(resume_status.into()),
    );
    // A generic park is never unsatisfiable — only the dependency-sweep path
    // sets this marker. Clear any stale bit left over from a prior park so
    // status's BLOCKED section never renders a false unsatisfiable row.
    object.remove(PARKED_UNSATISFIABLE_REF);
    serde_json::to_string(&value).map_err(|e| QuorumError::Io(format!("serialize task refs: {e}")))
}

/// Record the owner-facing half of a terminal daemon park. Callers perform
/// this in the same write transaction as the task, lease, note, and event
/// changes so a park can never become visible without its failure alert.
pub(crate) fn alert_owner_of_park(
    conn: &Connection,
    id: i64,
    reason: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO messages(ts, author, topic, kind, body, refs, expires_at, recipient)
         VALUES (?1, 'daemon', ?2, 'alert', ?3, ?4, ?5, 'owner')",
        params![
            now,
            crate::feed::DEFAULT_TOPIC,
            format!("task #{id}: {reason}; parked — resume with `quorum task-retry`"),
            format!("task:{id}"),
            now + crate::feed::DEFAULT_MESSAGE_TTL_SECS,
        ],
    )?;
    Ok(())
}

fn set_classifier_policy_parked_refs(
    refs: Option<&str>,
    reason: &str,
    resume_status: &str,
) -> Result<String> {
    let parked = set_parked_refs(refs, reason, resume_status)?;
    let mut value: serde_json::Value = serde_json::from_str(&parked)
        .map_err(|e| QuorumError::Io(format!("invalid task refs JSON: {e}")))?;
    value
        .as_object_mut()
        .ok_or_else(|| QuorumError::Io("task refs must be a JSON object".into()))?
        .insert(
            CLASSIFIER_POLICY_PARKED_REF.into(),
            serde_json::Value::Bool(true),
        );
    serde_json::to_string(&value).map_err(|e| QuorumError::Io(format!("serialize task refs: {e}")))
}

pub const COMPLEXITY_FIVE_PARK_REASON: &str =
    "complexity 5 exceeds automatic dispatch policy; split or rescope into a new task";
pub const LOW_COMPLEXITY_XL_PARK_REASON: &str =
    "size XL requires complexity 4 or 5 for decomposition; reclassify or rescope the task";

/// A complete v2 classification has the exact persisted types and
/// readiness/reason relationship required by dispatch policy.
pub fn classification_is_complete(refs: &Option<String>) -> bool {
    let Some(refs) = refs else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(refs) else {
        return false;
    };
    let Some(cx) = v.get("cx_est").and_then(serde_json::Value::as_i64) else {
        return false;
    };
    let Some(size) = v.get("cx_size").and_then(serde_json::Value::as_str) else {
        return false;
    };
    let Some(ready) = v.get("cx_ready").and_then(serde_json::Value::as_bool) else {
        return false;
    };
    let reason_is_valid = if ready {
        v.get("cx_not_ready_reason")
            .is_some_and(serde_json::Value::is_null)
    } else {
        v.get("cx_not_ready_reason")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|reason| !reason.trim().is_empty() && !reason.contains('\0'))
    };
    (1..=5).contains(&cx) && matches!(size, "S" | "M" | "L" | "XL") && reason_is_valid
}

/// Classification policy is intentionally separate from model routing: difficult
/// focused work may run, while unready or compound work is parked.
pub fn classification_is_dispatchable(
    refs: &Option<String>,
    review_only: bool,
    continue_pr: Option<i64>,
) -> bool {
    if !classification_is_complete(refs) {
        return false;
    }
    let Some(refs) = refs else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(refs) else {
        return false;
    };
    let Some(cx) = v.get("cx_est").and_then(|v| v.as_i64()) else {
        return false;
    };
    let Some(size) = v.get("cx_size").and_then(|v| v.as_str()) else {
        return false;
    };
    let ready = v.get("cx_ready").and_then(|v| v.as_bool()).unwrap_or(false);
    ready
        && (1..=5).contains(&cx)
        && (review_only
            || continue_pr.is_some()
            || matches!(size, "S" | "M")
            || (size == "L" && cx <= 3))
}

pub(crate) fn park_classified_task_tx(
    tx: &rusqlite::Transaction<'_>,
    id: i64,
    reason: &str,
    now: i64,
) -> Result<bool> {
    let current: Option<(String, Option<String>)> = tx.query_row(
        "SELECT status, refs FROM tasks WHERE id=?1 AND status NOT IN ('done','failed','cancelled')",
        params![id], |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional()?;
    let Some((status, refs)) = current else {
        return Ok(false);
    };
    let effective_reason = if reason == "classification outside automatic dispatch policy" {
        refs.as_deref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .and_then(|v| {
                if v.get("cx_ready").and_then(|b| b.as_bool()) == Some(false) {
                    return v
                        .get("cx_not_ready_reason")
                        .and_then(|s| s.as_str())
                        .map(|s| format!("task is not ready: {s}"));
                }
                if v.get("cx_size").and_then(|s| s.as_str()) == Some("XL")
                    && v.get("cx_est")
                        .and_then(|cx| cx.as_i64())
                        .is_some_and(|cx| cx <= 3)
                {
                    return Some(LOW_COMPLEXITY_XL_PARK_REASON.to_string());
                }
                None
            })
            .unwrap_or_else(|| reason.to_string())
    } else {
        reason.to_string()
    };
    let resume_status = match status.as_str() {
        "working" | "claimed" => "open",
        "merging" => "in-review",
        other => other,
    };
    let refs =
        set_classifier_policy_parked_refs(refs.as_deref(), &effective_reason, resume_status)?;
    tx.execute(
        "UPDATE tasks SET status='failed', assignee=NULL, refs=?2, updated_at=?3 WHERE id=?1",
        params![id, refs, now],
    )?;
    deactivate_lease(tx, id, now)?;
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, 'daemon', ?3)",
        params![id, now, format!("parked: {effective_reason}")],
    )?;
    crate::events::emit(tx, "task_parked", &lease_target(id), &effective_reason, now)?;
    alert_owner_of_park(tx, id, &effective_reason, now)?;
    Ok(true)
}

/// A newly complete, dispatchable classification may replace an older policy
/// park.  Restore only the status captured by that same park, inside the
/// classification write transaction.
pub(crate) fn restore_classified_task_tx(
    tx: &rusqlite::Transaction<'_>,
    id: i64,
    now: i64,
) -> Result<bool> {
    let row: Option<(String, Option<String>)> = tx
        .query_row(
            "SELECT status, refs FROM tasks
         WHERE id=?1 AND status='failed' AND json_valid(refs)
           AND json_extract(refs, '$.daemon_parked')=1
           AND json_extract(refs, '$.classifier_policy_parked')=1",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((_status, refs)) = row else {
        return Ok(false);
    };
    let Some(mut value) = refs.and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
    else {
        return Ok(false);
    };
    let resume = value
        .get(PARKED_RESUME_STATUS_REF)
        .and_then(|v| v.as_str())
        .unwrap_or("open")
        .to_string();
    if !matches!(resume.as_str(), "open" | "rework" | "in-review" | "merging") {
        return Ok(false);
    }
    let obj = value.as_object_mut().expect("refs object");
    obj.remove(PARKED_REF);
    obj.remove(PARKED_REASON_REF);
    obj.remove(PARKED_RESUME_STATUS_REF);
    obj.remove(CLASSIFIER_POLICY_PARKED_REF);
    tx.execute(
        "UPDATE tasks SET status=?2, refs=?3, updated_at=?4 WHERE id=?1",
        params![id, resume, value.to_string(), now],
    )?;
    tx.execute("INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, 'classifier', 'classification now dispatchable; restored from policy park')", params![id, now])?;
    Ok(true)
}

/// Park a classified category-5 task inside the caller's write transaction.
/// Classification and policy enforcement therefore become visible atomically.
#[allow(dead_code)] // compatibility helper retained for older callers/tests
pub(crate) fn park_complexity_five_tx(
    tx: &rusqlite::Transaction<'_>,
    id: i64,
    now: i64,
) -> Result<bool> {
    let current: Option<(String, Option<String>)> = tx
        .query_row(
            "SELECT status, refs FROM tasks
             WHERE id=?1
               AND status NOT IN ('done','failed','cancelled')
               AND json_valid(refs)
               AND json_extract(refs, '$.cx_est')=5",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((status, refs)) = current else {
        return Ok(false);
    };
    let resume_status = match status.as_str() {
        "working" | "claimed" => "open",
        "merging" => "in-review",
        other => other,
    };
    let refs = set_classifier_policy_parked_refs(
        refs.as_deref(),
        COMPLEXITY_FIVE_PARK_REASON,
        resume_status,
    )?;
    tx.execute(
        "UPDATE tasks SET status='failed', assignee=NULL, refs=?2, updated_at=?3 WHERE id=?1",
        params![id, refs, now],
    )?;
    deactivate_lease(tx, id, now)?;
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body)
         VALUES (?1, ?2, 'daemon', ?3)",
        params![id, now, format!("parked: {COMPLEXITY_FIVE_PARK_REASON}")],
    )?;
    crate::events::emit(
        tx,
        "task_parked",
        &lease_target(id),
        COMPLEXITY_FIVE_PARK_REASON,
        now,
    )?;
    alert_owner_of_park(tx, id, COMPLEXITY_FIVE_PARK_REASON, now)?;
    Ok(true)
}

/// Reconcile non-admissible classifications written by an older daemon or
/// changed while this daemon was stopped. Admission-ready review-only tasks and
/// L/XL implementation tasks with decomposition-range estimates are
/// intentionally left runnable; low-complexity non-continuation XL work is held.
pub fn park_classified_complexity_five(conn: &mut Connection, now: i64) -> Result<usize> {
    let tx = begin_immediate(conn)?;
    let ids: Vec<i64> = {
        let mut stmt = tx.prepare(
            "SELECT id FROM tasks
             WHERE status NOT IN ('done','failed','cancelled')
               AND json_valid(refs)
               AND (json_extract(refs, '$.cx_ready')!=1
                    OR (review_only=0 AND continue_pr IS NULL
                        AND json_extract(refs, '$.cx_size')='XL'
                        AND json_extract(refs, '$.cx_est') <= 3))
             ORDER BY id
             LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![SWEEP_LIMIT as i64], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        rows
    };
    let mut parked = 0;
    for id in ids {
        parked += usize::from(park_classified_task_tx(
            &tx,
            id,
            "classification outside automatic dispatch policy",
            now,
        )?);
    }
    tx.commit()?;
    Ok(parked)
}

/// Durably park an automatically blocked task. Failed is deliberately excluded
/// from daemon provisioning; the task can only continue through `task-retry`.
pub fn park(
    conn: &mut Connection,
    id: i64,
    reason: &str,
    resume_status: &str,
    now: i64,
) -> Result<Option<Task>> {
    if !matches!(resume_status, "open" | "rework" | "in-review" | "merging") {
        return Err(QuorumError::Usage(format!(
            "invalid parked resume status: {resume_status}"
        )));
    }
    let tx = begin_immediate(conn)?;
    let current: Option<Option<String>> = tx
        .query_row(
            "SELECT refs FROM tasks
             WHERE id=?1 AND status NOT IN ('done','failed','cancelled')",
            params![id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(refs) = current else {
        tx.commit()?;
        return Ok(None);
    };
    let refs = set_parked_refs(refs.as_deref(), reason, resume_status)?;
    tx.execute(
        "UPDATE tasks
         SET status='failed', assignee=NULL, refs=?2, updated_at=?3
         WHERE id=?1",
        params![id, refs, now],
    )?;
    deactivate_lease(&tx, id, now)?;
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body)
         VALUES (?1, ?2, 'daemon', ?3)",
        params![id, now, format!("parked: {reason}")],
    )?;
    crate::events::emit(&tx, "task_parked", &lease_target(id), reason, now)?;
    alert_owner_of_park(&tx, id, reason, now)?;
    crate::decomposition::block_graph_if_child_failed(&tx, id, reason, now)?;
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
    tx.commit()?;
    Ok(Some(task))
}

/// Atomically persist the round's blocking feedback on a remediation task.
/// Single-statement `json_set` — never read-modify-write — so a concurrent
/// park (sweep or apply_event) can't be overwritten by a stale refs snapshot.
/// This write is the recovery backbone: after any park, the durable-retry
/// reconciler can only rebuild the remediation turn from this key.
pub fn set_remediation_feedback(
    conn: &Connection,
    id: i64,
    feedback: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE tasks
         SET refs = json_set(COALESCE(refs, '{}'), '$.remediation_feedback', ?2),
             updated_at = ?3
         WHERE id = ?1",
        params![id, feedback, now],
    )?;
    Ok(())
}

/// Retain blocking feedback when a remediation claim loses solely because its
/// dependencies are not ready. The guarded write ensures a concurrent winner
/// keeps its lease and feedback, while the durable retry reconciler can resume
/// an unleased rework task once its dependencies complete.
pub fn retain_blocked_remediation_retry(
    conn: &mut Connection,
    id: i64,
    feedback: &str,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let updated = tx.execute(
        "UPDATE tasks
         SET refs = json_set(
                 COALESCE(refs, '{}'),
                 '$.remediation_feedback', ?2,
                 '$.daemon_rework_retry_requested', json('true')
             ),
             updated_at = ?3
         WHERE id = ?1
           AND status = 'rework'
           AND depends_on IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM claims c
               WHERE c.target = 'task#' || tasks.id
                 AND c.active = 1 AND c.expires_at > ?3
           )",
        params![id, feedback, now],
    )?;
    tx.commit()?;
    Ok(updated == 1)
}

/// Task #473: when a task is cancelled, refresh the durable park refs of
/// each daemon-parked dependent so refs stay consistent with the live dep
/// graph. Called inside the cancellation transaction so status readers see
/// the upgrade atomically with the cancel — no scheduling gap where the
/// dependent stays hidden from BLOCKED.
///
/// Bounded per call by [`CONVERGE_LIMIT`]. Cancellation installs a durable
/// cursor and examines at most one primary-key page of tasks. Ordinary
/// write-sweeps continue that cursor, so retained no-match history cannot
/// enlarge one `BEGIN IMMEDIATE` window and matches beyond the first page
/// are eventually repaired without another cancellation.
/// Correctness of the operator disposition queue does not depend on this
/// convergence: `stats::blocked_tasks` and `retry_parked` both infer the
/// unsatisfiable condition from the live dep graph, so any dependent that
/// exceeds the bound still surfaces in BLOCKED and still refuses retry —
/// only the durable `daemon_parked_reason`/marker refresh may lag while the
/// durable queue is drained by production sweep call sites.
///
/// Classifier-policy parks are skipped intentionally: their durable reason
/// is "classifier declined" and their retry path is reclassification, not
/// dep restoration — overwriting the reason would lose the classifier
/// cause. `stats::blocked_tasks` surfaces those rows via live dep-graph
/// inference instead, so the disposition signal is still present.
///
pub(crate) const CONVERGE_LIMIT: usize = 64;

pub(crate) fn converge_parked_dependents_of_cancelled(
    tx: &Connection,
    cancelled_id: i64,
    now: i64,
) -> Result<usize> {
    tx.execute(
        "INSERT INTO cancelled_dependency_reconciliation(
             cancelled_task_id, task_cursor, updated_at
         ) VALUES (?1, 0, ?2)
         ON CONFLICT(cancelled_task_id) DO UPDATE SET updated_at=excluded.updated_at",
        params![cancelled_id, now],
    )?;
    process_cancelled_dependency_reconciliation(tx, cancelled_id, now, CONVERGE_LIMIT)
}

/// Continue one durable cancelled-dependency cursor by examining at most
/// `limit` raw task rows. Paging only on the INTEGER PRIMARY KEY makes the
/// amount of candidate work independent of task status and JSON selectivity.
pub(crate) fn converge_cancelled_dependency_reconciliation(
    tx: &Connection,
    now: i64,
    limit: usize,
) -> Result<usize> {
    let Some(cancelled_id) = tx
        .query_row(
            "SELECT cancelled_task_id
             FROM cancelled_dependency_reconciliation
             ORDER BY cancelled_task_id
             LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
    else {
        return Ok(0);
    };
    process_cancelled_dependency_reconciliation(tx, cancelled_id, now, limit)
}

fn process_cancelled_dependency_reconciliation(
    tx: &Connection,
    cancelled_id: i64,
    now: i64,
    limit: usize,
) -> Result<usize> {
    let cursor: i64 = tx.query_row(
        "SELECT task_cursor FROM cancelled_dependency_reconciliation
         WHERE cancelled_task_id=?1",
        [cancelled_id],
        |row| row.get(0),
    )?;
    let page_limit = limit.clamp(1, CONVERGE_LIMIT);
    let page: Vec<(i64, String, Option<String>, Option<String>)> = {
        let mut stmt = tx.prepare(
            "SELECT id, status, depends_on, refs
             FROM tasks
             WHERE id > ?1
             ORDER BY id
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![cursor, page_limit as i64], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    if page.is_empty() {
        tx.execute(
            "DELETE FROM cancelled_dependency_reconciliation
             WHERE cancelled_task_id=?1",
            [cancelled_id],
        )?;
        return Ok(0);
    }
    let last_id = page.last().expect("non-empty page").0;
    let mut candidates = Vec::new();
    for (task_id, status, depends_on, refs) in &page {
        if status != "failed" {
            continue;
        }
        let Some(deps) = depends_on
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok())
        else {
            continue;
        };
        if !deps.contains(&cancelled_id) {
            continue;
        }
        let Some(refs) = refs
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        else {
            continue;
        };
        if refs.get(PARKED_REF).and_then(serde_json::Value::as_bool) == Some(true)
            && refs
                .get(CLASSIFIER_POLICY_PARKED_REF)
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            && refs
                .get(PARKED_UNSATISFIABLE_REF)
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            candidates.push(*task_id);
        }
    }
    let reason = format!("dependency #{cancelled_id} is cancelled — unsatisfiable");
    for task_id in &candidates {
        tx.execute(
            "UPDATE tasks
             SET refs = json_set(
                     refs,
                     '$.daemon_parked_unsatisfiable', json('true'),
                     '$.daemon_parked_reason', ?1
                 ),
                 updated_at=?2
             WHERE id=?3",
            params![reason, now, task_id],
        )?;
        tx.execute(
            "INSERT INTO task_notes(task_id, ts, agent, body)
             VALUES (?1, ?2, 'daemon',
                     'park upgraded to unsatisfiable: ' || ?3)",
            params![task_id, now, reason],
        )?;
        crate::events::emit(
            tx,
            "task_parked_upgraded",
            &format!("task#{task_id}"),
            &reason,
            now,
        )?;
    }
    if page.len() < page_limit {
        tx.execute(
            "DELETE FROM cancelled_dependency_reconciliation
             WHERE cancelled_task_id=?1",
            [cancelled_id],
        )?;
    } else {
        tx.execute(
            "UPDATE cancelled_dependency_reconciliation
             SET task_cursor=?2, updated_at=?3
             WHERE cancelled_task_id=?1",
            params![cancelled_id, last_id, now],
        )?;
    }
    Ok(candidates.len())
}

/// Cancelled dep ids in a task's `depends_on`. Cancelled is terminal, so
/// these are the ones a bare `task-retry` cannot re-satisfy; the operator
/// must edit `depends_on` or close the dependent. Empty when `depends_on`
/// is NULL/empty or no dep is cancelled.
pub fn cancelled_dep_ids(conn: &Connection, id: i64) -> Result<Vec<i64>> {
    let depends_on: Option<String> = conn
        .query_row(
            "SELECT depends_on FROM tasks WHERE id=?1",
            params![id],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let Some(json) = depends_on else {
        return Ok(vec![]);
    };
    let mut stmt = conn.prepare(
        "SELECT j.value FROM json_each(?1) j
         JOIN tasks d ON d.id = j.value
         WHERE d.status = 'cancelled'
         ORDER BY j.value",
    )?;
    let ids = stmt
        .query_map(params![json], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}

/// Explicitly resume the same task after an automatic park. PR, branch,
/// dependency, approval, author, and rework context remain untouched.
///
/// `reset_recovery_budget`: true for an explicit owner `task-retry` (fresh
/// budget); false for daemon-initiated auto-retries, whose respawns must
/// stay bounded by the recovery budget spent at park time.
///
/// Refuses to restore when `depends_on` contains a cancelled task — that
/// dependency is terminal-terminal, so the sweep would just re-park the
/// dependent on the next tick while the operator sees a "restored" outcome
/// and no signal that disposition (dep edit or close) is required. Callers
/// see `Ok(None)`; [`cancelled_dep_ids`] surfaces the specific ids so the
/// CLI can name them in the exit-1 payload.
pub fn retry_parked(
    conn: &mut Connection,
    id: i64,
    by: &str,
    reset_recovery_budget: bool,
    now: i64,
) -> Result<Option<Task>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, by, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let resume_status: Option<String> = tx
        .query_row(
            "SELECT json_extract(refs, '$.daemon_resume_status')
             FROM tasks
             WHERE id=?1 AND status='failed'
               AND json_valid(refs)
               AND json_extract(refs, '$.daemon_parked')=1",
            params![id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(resume_status) = resume_status else {
        tx.commit()?;
        return Ok(None);
    };
    // A parked task whose depends_on contains a cancelled id is unsatisfiable:
    // cancelled is terminal, so the sweep will just re-park immediately. Refuse
    // the restore so the operator has a clear disposition prompt via the CLI
    // exit-1 path instead of a false "restored" outcome.
    if !cancelled_dep_ids(&tx, id)?.is_empty() {
        tx.commit()?;
        return Ok(None);
    }
    let policy_parked: bool = tx.query_row(
        "SELECT COALESCE(
             json_valid(refs)
             AND json_extract(refs, '$.classifier_policy_parked')=1,
             0
         )
         FROM tasks WHERE id=?1",
        params![id],
        |row| row.get(0),
    )?;
    // Non-growth under a decomposition freeze: restoring a parked task to a
    // non-terminal status while `freeze_active=1` would add started work to
    // the drain-quiescence set that the frozen-base capture waits on. Refuse
    // the restore; the operator can rerun once planning materializes and the
    // freeze clears. The policy-parked branch keeps status='failed', so it
    // does not grow the counted set and is allowed under a freeze.
    if !policy_parked {
        let freeze_active: i64 = tx.query_row(
            "SELECT COUNT(*) FROM task_decompositions WHERE freeze_active=1",
            [],
            |row| row.get(0),
        )?;
        if freeze_active > 0 {
            return Err(QuorumError::Usage(format!(
                "cannot retry task #{id}: a decomposition freeze is active; \
                 wait for planning to materialize before retrying"
            )));
        }
    }
    if policy_parked {
        // Retry of a policy park is a request to estimate remaining work.  Keep
        // the durable park/resume context but make it a classifier candidate.
        // Also strip `daemon_parked_unsatisfiable` as defense in depth: the
        // sweep's convergence pass excludes classifier-policy parks, but any
        // future path that sets the marker on a policy park would otherwise
        // leave a stale `true` here (policy retry keeps status='failed').
        tx.execute(
            "UPDATE tasks
             SET refs=json_remove(
                     refs,
                     '$.cx_est',
                     '$.cx_size',
                     '$.cx_ready',
                     '$.cx_not_ready_reason',
                     '$.cx_by',
                     '$.cx_dup_of',
                     '$.daemon_parked_unsatisfiable'
                 ),
                 recovery_attempts=CASE WHEN ?3 THEN 0 ELSE recovery_attempts END,
                 updated_at=?2
             WHERE id=?1",
            params![id, now, reset_recovery_budget],
        )?;
        crate::events::emit(
            &tx,
            "task_retry",
            &lease_target(id),
            &format!("policy-parked task reclassification requested by {by}"),
            now,
        )?;
        let mut task = tx.query_row(
            &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
            params![id],
            row_to_task,
        )?;
        task.ready = compute_ready(&tx, &task.depends_on)?;
        tx.commit()?;
        return Ok(Some(task));
    }
    if !matches!(
        resume_status.as_str(),
        "open" | "rework" | "in-review" | "merging"
    ) {
        return Err(QuorumError::Io(format!(
            "invalid persisted resume status for task #{id}: {resume_status}"
        )));
    }
    let restored_status = resume_status.as_str();
    // A rejected initial branch push has no remote/PR authority to preserve.
    // Require both the parked publication reason and the most recent worker
    // outcome so an older rejected push cannot affect a later, unrelated
    // park. This is intentionally computed inside the retry transaction: the
    // same decision clears the stale source, continuation, author, and branch
    // allocation before another worker can claim the fresh delivery round.
    let fresh_initial_delivery: bool = tx.query_row(
        "SELECT COALESCE(
             json_extract(refs, '$.daemon_parked_reason')
                 LIKE 'daemon-owned publication failed:%'
             AND json_extract(refs, '$.daemon_publication.stage')='intent'
             AND json_type(refs, '$.daemon_publication.pr')='null'
             AND COALESCE((
                 SELECT end_reason
                 FROM agent_runs
                 WHERE task_id=tasks.id AND role='worker'
                 ORDER BY id DESC
                 LIMIT 1
             ), '')='daemon_push_failed',
             0
         )
         FROM tasks WHERE id=?1",
        params![id],
        |row| row.get(0),
    )?;
    let updated = tx.execute(
        "UPDATE tasks
         SET status=?2,
             assignee=NULL,
             author=CASE WHEN ?6 THEN NULL ELSE author END,
             recovery_attempts=CASE WHEN ?5 THEN 0 ELSE recovery_attempts END,
             refs=CASE
                  WHEN ?6
                  THEN json_remove(
                      refs,
                      '$.daemon_parked',
                      '$.daemon_parked_reason',
                      '$.daemon_parked_unsatisfiable',
                      '$.daemon_resume_status',
                      '$.daemon_rework_retry_requested',
                      '$.daemon_parked_head_check',
                      '$.daemon_merge_retry',
                      '$.daemon_publication',
                      '$.runner_continuation'
                  )
                  WHEN ?4='rework'
                  THEN json_set(
                      json_remove(
                          refs,
                          '$.daemon_parked',
                          '$.daemon_parked_reason',
                          '$.daemon_parked_unsatisfiable',
                          '$.daemon_resume_status',
                          '$.daemon_parked_head_check'
                      ),
                      '$.daemon_rework_retry_requested',
                      json('true')
                  )
                  WHEN ?4='merging'
                  THEN json_set(
                      json_remove(
                          refs,
                          '$.daemon_parked',
                          '$.daemon_parked_reason',
                          '$.daemon_parked_unsatisfiable',
                          '$.daemon_resume_status',
                          '$.daemon_rework_retry_requested',
                          '$.daemon_parked_head_check'
                      ),
                      '$.daemon_merge_retry',
                      'requested'
                  )
                  ELSE json_remove(
                      refs,
                      '$.daemon_parked',
                      '$.daemon_parked_reason',
                      '$.daemon_parked_unsatisfiable',
                      '$.daemon_resume_status',
                      '$.daemon_rework_retry_requested',
                      '$.daemon_parked_head_check',
                      '$.daemon_merge_retry'
                  )
             END,
             updated_at=?3
         WHERE id=?1 AND status='failed'
           AND json_valid(refs)
           AND json_extract(refs, '$.daemon_parked')=1
           AND json_extract(refs, '$.daemon_resume_status')=?4",
        params![
            id,
            restored_status,
            now,
            resume_status,
            reset_recovery_budget,
            fresh_initial_delivery,
        ],
    )?;
    if updated == 0 {
        tx.commit()?;
        return Ok(None);
    }
    if fresh_initial_delivery {
        tx.execute("DELETE FROM task_branches WHERE task_id=?1", params![id])?;
    }
    deactivate_lease(&tx, id, now)?;
    // Pre-structured generated-child holds from before this recovery path have
    // string summaries. They intentionally do not match: no migration or
    // backfill can safely infer which child those legacy rows name.
    let graph_reactivated = tx.execute(
        "UPDATE task_decompositions
         SET state='active',hold_code=NULL,hold_summary=NULL,updated_at=?2
         WHERE active=1 AND state='blocked' AND hold_code='generated-child-failed'
           AND CASE WHEN json_valid(hold_summary)
                    THEN json_type(hold_summary,'$.affected_task')='integer'
                         AND json_extract(hold_summary,'$.affected_task')=?1
                    ELSE 0
               END
           AND EXISTS(
               SELECT 1 FROM task_graph_members m
               WHERE m.graph_id=task_decompositions.id AND m.task_id=?1 AND m.active=1
           )",
        params![id, now],
    )?;
    crate::events::emit(
        &tx,
        "task_retry",
        &lease_target(id),
        &format!("parked task resumed by {by}"),
        now,
    )?;
    if graph_reactivated == 1 {
        crate::events::emit(
            &tx,
            "task_graph_unblocked",
            &lease_target(id),
            "generated child retry restored graph authority",
            now,
        )?;
    }
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
    tx.commit()?;
    Ok(Some(task))
}

/// Atomically consume at most one owner-authorized merge replay intent.
///
/// The transition to `attempting` commits before any network call. If the
/// daemon crashes after this point, startup parks the uncertain attempt and
/// requires fresh owner authority rather than issuing a duplicate merge call.
pub fn claim_merge_retry(conn: &mut Connection, now: i64) -> Result<Option<Task>> {
    let tx = begin_immediate(conn)?;
    let id: Option<i64> = tx
        .query_row(
            "SELECT id FROM tasks
             WHERE status='merging' AND json_valid(refs)
               AND json_extract(refs, '$.daemon_merge_retry')='requested'
             ORDER BY id LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let Some(id) = id else {
        tx.commit()?;
        return Ok(None);
    };
    let changed = tx.execute(
        "UPDATE tasks
         SET refs=json_set(refs, '$.daemon_merge_retry', 'attempting'),
             updated_at=?2
         WHERE id=?1 AND status='merging' AND json_valid(refs)
           AND json_extract(refs, '$.daemon_merge_retry')='requested'",
        params![id, now],
    )?;
    if changed == 0 {
        tx.commit()?;
        return Ok(None);
    }
    crate::events::emit(
        &tx,
        "merge_retry_started",
        &lease_target(id),
        "owner-authorized merge replay claimed by daemon",
        now,
    )?;
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
    tx.commit()?;
    Ok(Some(task))
}

/// Cross the durable boundary immediately before the ordinary reviewed merge
/// path makes its first remote merge call.
///
/// Only a merging task without an existing retry marker can be admitted. A
/// `requested` marker belongs to the explicit-retry reconciler, while an
/// `attempting` marker means a prior call may already have escaped. Both fail
/// closed here. Startup parks `attempting` tasks before approval recovery.
pub fn begin_approved_merge_attempt(conn: &mut Connection, id: i64, now: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE tasks
         SET refs=json_set(COALESCE(refs, '{}'), '$.daemon_merge_retry', 'attempting'),
             updated_at=?2
         WHERE id=?1 AND status='merging'
           AND (refs IS NULL OR json_valid(refs))
           AND json_extract(COALESCE(refs, '{}'), '$.daemon_merge_retry') IS NULL",
        params![id, now],
    )?;
    if changed == 1 {
        crate::events::emit(
            &tx,
            "merge_attempt_started",
            &lease_target(id),
            "approved merge call admitted by daemon",
            now,
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

/// Remove stale runnable retry state from terminal tasks in a bounded,
/// idempotent transaction.
///
/// `failed + daemon_parked` is the durable owner-retry representation, so its
/// park reason and resume target remain intact.  Only the incompatible
/// daemon-auto-retry flag is removed.  Fully terminal `done`/`cancelled` rows
/// lose all runnable park/resume state while retaining the textual park reason
/// and existing event/note history.  A failed row with an orphan resume marker
/// (no park authority) is cleaned as corrupt as well.
///
/// One audit note is inserted for each changed row in the same transaction.
/// Repeated ticks therefore make no further writes.  Returns the reconciled
/// task IDs for bounded daemon logging.
pub fn reconcile_terminal_retry_markers(conn: &mut Connection, now: i64) -> Result<Vec<i64>> {
    let tx = begin_immediate(conn)?;
    let ids: Vec<i64> = {
        let mut stmt = tx.prepare(
            "SELECT id FROM tasks INDEXED BY tasks_terminal_retry_id
             WHERE status IN ('done','failed','cancelled')
               AND json_valid(refs)
               AND (
                   json_type(refs, '$.daemon_rework_retry_requested')='true'
                   OR json_type(refs, '$.daemon_parked_head_check')='true'
                   OR (
                       status IN ('done','cancelled')
                       AND (
                           json_type(refs, '$.daemon_parked') IS NOT NULL
                           OR json_type(refs, '$.daemon_resume_status') IS NOT NULL
                       )
                   )
                   OR (
                       status='failed'
                       AND json_type(refs, '$.daemon_resume_status') IS NOT NULL
                       AND COALESCE(json_extract(refs, '$.daemon_parked'), 0) != 1
                   )
               )
             ORDER BY id
             LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![TERMINAL_RETRY_RECONCILE_LIMIT], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };

    for id in &ids {
        let changed = tx.execute(
            "UPDATE tasks
             SET refs=CASE
                   WHEN status='failed'
                    AND COALESCE(json_extract(refs, '$.daemon_parked'), 0) = 1
                   THEN json_remove(
                       refs,
                       '$.daemon_rework_retry_requested',
                       '$.daemon_parked_head_check'
                   )
                   WHEN status='failed' THEN json_remove(
                       refs,
                       '$.daemon_rework_retry_requested',
                       '$.daemon_parked_head_check',
                       '$.daemon_resume_status'
                   )
                   ELSE json_remove(
                       refs,
                       '$.daemon_parked',
                       '$.daemon_resume_status',
                       '$.daemon_rework_retry_requested',
                       '$.daemon_parked_head_check',
                       '$.classifier_policy_parked'
                   )
                 END,
                 updated_at=?2
             WHERE id=?1 AND status IN ('done','failed','cancelled')",
            params![id, now],
        )?;
        if changed == 1 {
            tx.execute(
                "INSERT INTO task_notes(task_id, ts, agent, body)
                 VALUES (?1, ?2, 'daemon',
                         'reconciled stale terminal daemon retry markers; lifecycle status preserved')",
                params![id, now],
            )?;
        }
    }
    tx.commit()?;
    Ok(ids)
}

/// Atomically retry a task parked after a bounded provider failure.
///
/// A `working` task returns to `open`. A true `rework` task remains unassigned
/// in `rework` until [`claim_provider_retry_rework`] atomically installs its
/// replacement worker lease. Reviewer retries are deliberately rejected until
/// provider-neutral R1/R2 support exists.
pub fn retry_provider_blocked(
    conn: &mut Connection,
    id: i64,
    by: &str,
    now: i64,
) -> Result<Option<Task>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, by, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;

    let current = tx
        .query_row(
            "SELECT status, refs FROM tasks WHERE id=?1 AND status IN ('working','rework')",
            params![id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let Some((status, Some(refs_raw))) = current else {
        tx.commit()?;
        return Ok(None);
    };
    let Ok(mut refs) = serde_json::from_str::<serde_json::Value>(&refs_raw) else {
        tx.commit()?;
        return Ok(None);
    };
    if !refs.is_object() || !runner_state::request_retry(&mut refs) {
        tx.commit()?;
        return Ok(None);
    }
    let next_status = if status == "working" {
        "open"
    } else {
        "rework"
    };
    tx.execute(
        "UPDATE tasks SET refs=?2, status=?3, assignee=NULL, updated_at=?4
         WHERE id=?1 AND status=?5",
        params![id, refs.to_string(), next_status, now, status],
    )?;
    deactivate_lease(&tx, id, now)?;
    crate::events::emit(
        &tx,
        "task_provider_retry",
        &format!("task#{id}"),
        &format!("provider retry requested by {by}"),
        now,
    )?;
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
    tx.commit()?;
    Ok(Some(task))
}

// ── target branch ────────────────────────────────────────────────────────────

/// One-time resolution of a task's target branch. Succeeds only when the
/// field is currently NULL; a populated value is immutable regardless of
/// task status. Returns `true` if the value was set, `false` if already
/// populated. Returns `Err` only on database errors — a missing task
/// returns `Ok(false)`.
pub fn resolve_target_branch(
    conn: &mut Connection,
    task_id: i64,
    branch: &str,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let n = tx.execute(
        "UPDATE tasks SET target_branch=?2, updated_at=?3 \
         WHERE id=?1 AND target_branch IS NULL",
        params![task_id, branch, now],
    )?;
    tx.commit()?;
    Ok(n > 0)
}

/// Stamp a task's per-task rework ceiling from the daemon's `max_rework` config,
/// immutable once populated (mirrors [`resolve_target_branch`]). Returns whether
/// a row was updated — `false` when the task is missing or already stamped.
pub fn stamp_rework_cap(conn: &mut Connection, task_id: i64, cap: u32, now: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let n = tx.execute(
        "UPDATE tasks SET rework_cap=?2, updated_at=?3 \
         WHERE id=?1 AND rework_cap IS NULL",
        params![task_id, i64::from(cap), now],
    )?;
    tx.commit()?;
    Ok(n > 0)
}

// ── close_after_merge ─────────────────────────────────────────────────────────

pub fn close_after_merge(conn: &mut Connection, id: i64, note: &str, now: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let n = tx.execute(
        "UPDATE tasks SET status='done', assignee=NULL, updated_at=?2,
                          completion_provenance=?3
         WHERE id=?1 AND status NOT IN ('done', 'failed', 'cancelled')",
        params![id, now, COMPLETION_PROVENANCE_MERGED],
    )?;
    if n == 0 {
        tx.commit()?;
        return Ok(false);
    }
    deactivate_lease(&tx, id, now)?;
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, 'daemon', ?3)",
        params![id, now, note],
    )?;
    crate::events::emit(
        &tx,
        "task_done",
        &lease_target(id),
        &format!("closed on merge (recovery): {note}"),
        now,
    )?;
    crate::decomposition::complete_graph_if_final_child(&tx, id, now)?;
    tx.commit()?;
    Ok(true)
}

/// Set the author field on a task (#159: remediation worker becomes the
/// managed author so routing/disqualification works correctly).
pub fn set_author(conn: &mut Connection, id: i64, author: &str) -> Result<()> {
    let now = crate::clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "UPDATE tasks SET author=?2, updated_at=?3 WHERE id=?1",
        params![id, author, now],
    )?;
    tx.commit()?;
    Ok(())
}

// ── close_manual ─────────────────────────────────────────────────────────────

/// `failed` is closable: a task whose PR merged outside the managed lifecycle
/// (rework cap exhausted, then landed by hand) has no other route to `done`,
/// and dependents stay parked until it gets there — `compute_ready` counts only
/// `done`. `done` and `cancelled` stay refused: neither is a wrong record.
pub fn close_manual(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    reason: &str,
    now: i64,
) -> Result<Option<Task>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let active_graph_source: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM task_decompositions
         WHERE source_task_id=?1 AND active=1 AND state IN ('active','blocked'))",
        [id],
        |row| row.get(0),
    )?;
    if active_graph_source {
        return Err(QuorumError::Usage(
            "active decomposition sources cannot be manually closed; cancel the source graph"
                .into(),
        ));
    }
    let n = tx.execute(
        "UPDATE tasks SET status='done', assignee=NULL, updated_at=?2,
                          completion_provenance=?3
         WHERE id=?1 AND status NOT IN ('done', 'cancelled')",
        params![id, now, COMPLETION_PROVENANCE_MANUAL],
    )?;
    if n == 0 {
        tx.commit()?;
        return Ok(None);
    }
    deactivate_lease(&tx, id, now)?;
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, ?3, ?4)",
        params![id, now, agent, format!("manually closed: {reason}")],
    )?;
    crate::events::emit(
        &tx,
        "task_closed_manual",
        &lease_target(id),
        &format!("by {agent}: {reason}"),
        now,
    )?;
    crate::decomposition::complete_graph_if_final_child(&tx, id, now)?;
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
    tx.commit()?;
    Ok(Some(task))
}

// ── list / get / notes ────────────────────────────────────────────────────────

pub fn list(
    conn: &Connection,
    status: Option<&str>,
    label: Option<&str>,
    assignee: Option<&str>,
) -> Result<Vec<Task>> {
    let label_pat = label.map(|l| format!("%\"{l}\"%"));
    let mut sql = format!("SELECT {COLS} FROM tasks WHERE 1=1");
    if status.is_some() {
        sql.push_str(" AND status=:status");
    }
    if label_pat.is_some() {
        sql.push_str(" AND labels LIKE :label");
    }
    if assignee.is_some() {
        sql.push_str(" AND assignee=:assignee");
    }
    sql.push_str(" ORDER BY priority DESC, id ASC");

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<(&str, &dyn rusqlite::ToSql)> = {
        let mut v: Vec<(&str, &dyn rusqlite::ToSql)> = Vec::new();
        if let Some(s) = &status {
            v.push((":status", s));
        }
        if let Some(p) = &label_pat {
            v.push((":label", p));
        }
        if let Some(a) = &assignee {
            v.push((":assignee", a));
        }
        v
    };
    let mut tasks: Vec<Task> = stmt
        .query_map(&params[..], row_to_task)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for t in &mut tasks {
        t.ready = compute_ready(conn, &t.depends_on)?;
    }
    Ok(tasks)
}

/// List open implementation candidates whose dependencies and decomposition
/// authority currently permit a claim. The claim transaction repeats these
/// predicates authoritatively; this read prevents stable graph holds from
/// becoming a repeated select/claim-reject loop.
pub fn list_implementation_ready_open(conn: &Connection) -> Result<Vec<Task>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM tasks
         WHERE status='open'
           AND {DEP_READY_CLAUSE}
           AND {GRAPH_IMPLEMENTATION_READY_CLAUSE}
         ORDER BY priority DESC, id ASC"
    ))?;
    let mut tasks: Vec<Task> = stmt
        .query_map([], row_to_task)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for task in &mut tasks {
        task.ready = true;
    }
    Ok(tasks)
}

/// Bounded dispatchable implementation projection for read-only pollers.
///
/// Keep the dependency, decomposition, and persisted classification predicates
/// in SQL so a dashboard does not materialize task bodies for every eligible
/// task before applying its display bound. The daemon scheduler intentionally
/// uses the unbounded candidate projection above because it must continue past
/// temporarily inadmissible candidates when selecting work.
pub fn list_implementation_ready_open_limited(conn: &Connection, limit: i64) -> Result<Vec<Task>> {
    if limit < 0 {
        return Err(QuorumError::Usage(
            "implementation-ready task limit must not be negative".into(),
        ));
    }
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM tasks
         WHERE status='open'
           AND {DEP_READY_CLAUSE}
           AND {GRAPH_IMPLEMENTATION_READY_CLAUSE}
           AND review_only=0
           AND CASE WHEN json_valid(refs) THEN
               json_type(refs, '$.cx_est')='integer'
               AND json_extract(refs, '$.cx_est') BETWEEN 1 AND 5
               AND json_type(refs, '$.cx_size')='text'
               AND json_extract(refs, '$.cx_size') IN ('S','M','L','XL')
               AND {DIRECT_DISPATCH_CLAUSE}
               AND json_type(refs, '$.cx_ready')='true'
               AND json_type(refs, '$.cx_not_ready_reason')='null'
           ELSE 0 END
         ORDER BY priority DESC, id ASC
         LIMIT ?1"
    ))?;
    let mut tasks: Vec<Task> = stmt
        .query_map(params![limit], row_to_task)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for task in &mut tasks {
        task.ready = true;
    }
    Ok(tasks)
}

/// List rework tasks whose dependencies satisfy the same SQL eligibility
/// predicate used by remediation claims. Durable remediation reconciliation
/// uses this read before provisioning so dependency-blocked retries retain
/// their marker without repeatedly attempting a claim.
pub fn list_dependency_ready_rework(conn: &Connection) -> Result<Vec<Task>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM tasks
         WHERE status='rework' AND {DEP_READY_CLAUSE}
         ORDER BY priority DESC, id ASC"
    ))?;
    let mut tasks: Vec<Task> = stmt
        .query_map([], row_to_task)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // The SQL predicate above is authoritative for every returned row.
    for task in &mut tasks {
        task.ready = true;
    }
    Ok(tasks)
}

/// Minimal, cursor-bounded input for publication-ref reconciliation.
///
/// Tasks never expire, so every daemon-created task-scoped ref has a task row.
/// Walking IDs newest-first lets the daemon reconcile current publication
/// intents and terminal/no-intent orphans without materializing task bodies,
/// notes, dependency readiness, or the unbounded historical task set.
#[derive(Debug, PartialEq, Eq)]
pub struct PublicationSourceReconcileRow {
    pub task_id: i64,
    pub source_sha: Option<String>,
}

pub fn publication_source_reconcile_batch(
    conn: &Connection,
    before_task_id: Option<i64>,
    limit: i64,
) -> Result<Vec<PublicationSourceReconcileRow>> {
    if limit <= 0 {
        return Err(QuorumError::Usage(
            "publication reconciliation limit must be positive".into(),
        ));
    }
    let mut stmt = conn.prepare(
        "SELECT id,
                CASE WHEN status NOT IN ('done', 'cancelled')
                           AND json_valid(COALESCE(refs, '{}'))
                  THEN CASE
                    WHEN json_type(refs, '$.daemon_publication.local_sha')='text'
                    THEN json_extract(refs, '$.daemon_publication.local_sha')
                    ELSE NULL
                  END
                  ELSE NULL
                END
         FROM tasks
         WHERE (?1 IS NULL OR id < ?1)
         ORDER BY id DESC
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(params![before_task_id, limit], |row| {
            Ok(PublicationSourceReconcileRow {
                task_id: row.get(0)?,
                source_sha: row.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Bounded read-only task listing for pollers such as the local web dashboard.
/// Unlike [`list`], this never materializes an unbounded historical task set.
pub fn list_limited(conn: &Connection, limit: i64) -> Result<Vec<Task>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM tasks ORDER BY updated_at DESC, id DESC LIMIT ?1"
    ))?;
    let mut tasks = stmt
        .query_map(params![limit], row_to_task)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for task in &mut tasks {
        task.ready = compute_ready(conn, &task.depends_on)?;
    }
    Ok(tasks)
}

pub fn get(conn: &Connection, id: i64) -> Result<Option<Task>> {
    let mut task = conn
        .query_row(
            &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
            params![id],
            row_to_task,
        )
        .optional()?;
    if let Some(t) = &mut task {
        t.ready = compute_ready(conn, &t.depends_on)?;
    }
    Ok(task)
}

pub fn get_with_notes(conn: &Connection, id: i64) -> Result<Option<TaskDetail>> {
    let Some(task) = get(conn, id)? else {
        return Ok(None);
    };
    let notes = notes_for(conn, id)?;
    let agent_runs = crate::agent_runs::runs_for_task(conn, id)?;
    Ok(Some(TaskDetail {
        task,
        notes,
        agent_runs,
    }))
}

pub fn add_note(
    conn: &mut Connection,
    agent: &str,
    task_id: i64,
    body: &str,
    now: i64,
) -> Result<Option<i64>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let id = add_note_tx(&tx, agent, task_id, body, now)?;
    tx.commit()?;
    Ok(id)
}

/// Append one task note inside a caller-owned write transaction.
///
/// Capability-bound endpoint operations use this boundary after deriving their
/// task and agent in the same transaction. It contains only the durable note
/// write: lifecycle housekeeping remains with standalone task-update calls.
pub fn add_note_tx(
    tx: &Transaction<'_>,
    agent: &str,
    task_id: i64,
    body: &str,
    now: i64,
) -> Result<Option<i64>> {
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
        params![task_id],
        |r| r.get(0),
    )?;
    if !exists {
        return Ok(None);
    }
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, ?3, ?4)",
        params![task_id, now, agent, body],
    )?;
    Ok(Some(tx.last_insert_rowid()))
}

fn notes_for(conn: &Connection, task_id: i64) -> Result<Vec<Note>> {
    let mut stmt = conn
        .prepare("SELECT id, ts, agent, body FROM task_notes WHERE task_id=?1 ORDER BY id ASC")?;
    let notes = stmt
        .query_map(params![task_id], |r| {
            Ok(Note {
                id: r.get(0)?,
                ts: r.get(1)?,
                agent: r.get(2)?,
                body: r.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(notes)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    /// Most lifecycle tests exercise work after the daemon has classified a
    /// task.  Keep that precondition explicit in one fixture wrapper; tests of
    /// classifier authority/absence call `super::create` directly.
    #[allow(clippy::too_many_arguments)]
    fn create(
        conn: &mut Connection,
        created_by: &str,
        title: &str,
        body: Option<&str>,
        priority: i64,
        labels: Option<&str>,
        refs: Option<&str>,
        depends_on: Option<&str>,
        review_pr: Option<i64>,
        now: i64,
    ) -> Result<i64> {
        let mut value = refs
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        let map = value.as_object_mut().expect("test refs object");
        map.entry("cx_est").or_insert_with(|| serde_json::json!(3));
        map.entry("cx_size")
            .or_insert_with(|| serde_json::json!("M"));
        map.entry("cx_ready")
            .or_insert_with(|| serde_json::json!(true));
        map.entry("cx_not_ready_reason")
            .or_insert(serde_json::Value::Null);
        map.entry("cx_by")
            .or_insert_with(|| serde_json::json!("test:v2"));
        let refs = value.to_string();
        super::create(
            conn,
            created_by,
            title,
            body,
            priority,
            labels,
            Some(&refs),
            depends_on,
            review_pr,
            now,
        )
    }

    #[test]
    fn merging_pr_refs_preserves_existing_classifier_provenance() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "already classified",
            None,
            0,
            None,
            Some(r#"{"cx_est":2,"cx_by":"haiku-45:v1","cx_tags":["kind:chore"]}"#),
            None,
            None,
            1_000,
        )
        .unwrap();
        claim(&mut conn, "worker", Some(task_id), &[], TTL, 1_001).unwrap();
        apply_event(
            &mut conn,
            "worker",
            task_id,
            &Event::SignaledDone { pr: "42".into() },
            1_002,
        )
        .unwrap();

        let task = get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();

        assert_eq!(refs["pr"], 42);
        assert_eq!(refs["cx_est"], 2);
        assert_eq!(refs["cx_by"], "haiku-45:v1");
        assert_eq!(refs["cx_tags"][0], "kind:chore");
    }

    fn late_worker_identity(c: &mut Connection, task_id: i64, agent: &str, pr: Option<i64>) {
        crate::journal::upsert(
            c,
            &crate::journal::JournalEntry {
                agent: agent.into(),
                role: "worker".into(),
                task_id: Some(task_id),
                session_id: "worker-run".into(),
                worktree: None,
                branch: None,
                phase: "working".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr,
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();
        crate::agent_runs::insert(c, task_id, agent, "worker", "model", "high", "codex", 1000)
            .unwrap();
    }

    fn late_reviewer_fixture(c: &mut Connection, r2: bool) -> (i64, i64) {
        let task_id = create(
            c,
            "owner",
            "late reviewer",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(c, "worker", Some(task_id), &[], TTL, 1000).unwrap();
        apply_event(
            c,
            "worker",
            task_id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        claim(c, "reviewer", Some(task_id), &[], TTL, 1002).unwrap();
        crate::journal::upsert(
            c,
            &crate::journal::JournalEntry {
                agent: "reviewer".into(),
                role: "reviewer".into(),
                task_id: Some(task_id),
                session_id: "review-run".into(),
                worktree: Some("/tmp/reviewer".into()),
                branch: None,
                phase: "reviewing".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: Some(42),
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();
        if r2 {
            crate::agent_runs::insert_r2(c, task_id, "reviewer", "model", "high", "codex", 1002)
                .unwrap();
        } else {
            crate::agent_runs::insert(
                c, task_id, "reviewer", "reviewer", "model", "high", "codex", 1002,
            )
            .unwrap();
        }
        let mailbox_id = crate::mailbox::append(
            c,
            &crate::mailbox::MailboxRow {
                agent: "reviewer".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(task_id),
                pr: Some(42),
                verdict: Some("approved".into()),
                feedback: None,
                note: None,
                to_agent: None,
                payload: Some(r#"{"blocking":0}"#.into()),
            },
        )
        .unwrap();
        (task_id, mailbox_id)
    }

    #[test]
    fn late_worker_completion_folds_and_consumes_atomically() {
        let (_d, mut c) = open_tmp();
        let task_id = create(
            &mut c,
            "owner",
            "late worker",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "worker", Some(task_id), &[], TTL, 1000).unwrap();
        late_worker_identity(&mut c, task_id, "worker", None);
        let mailbox_id = crate::mailbox::append(
            &mut c,
            &crate::mailbox::MailboxRow {
                agent: "worker".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(task_id),
                pr: None,
                verdict: None,
                feedback: None,
                note: None,
                to_agent: None,
                payload: None,
            },
        )
        .unwrap();
        assert!(recover_late_worker_completion(
            &mut c, mailbox_id, "worker", task_id, 42, None, 1001
        )
        .unwrap());
        assert_eq!(get(&c, task_id).unwrap().unwrap().status, "in-review");
        assert_eq!(crate::mailbox::poll_unconsumed(&c).unwrap().len(), 0);
        let events: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_in_review'",
                [lease_target(task_id)],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(events, 1);
    }

    #[test]
    fn late_replacement_completion_preserves_pr_and_uses_rework_pushed() {
        let (_d, mut c) = open_tmp();
        let task_id = create(
            &mut c,
            "owner",
            "late rework",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "original", Some(task_id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "original",
            task_id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        claim(&mut c, "reviewer", Some(task_id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "reviewer", task_id, &Event::VerdictChanges, 1003).unwrap();
        c.execute(
            "UPDATE tasks SET assignee='replacement' WHERE id=?1",
            [task_id],
        )
        .unwrap();
        late_worker_identity(&mut c, task_id, "replacement", Some(42));
        let mailbox_id = crate::mailbox::append(
            &mut c,
            &crate::mailbox::MailboxRow {
                agent: "replacement".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(task_id),
                pr: Some(42),
                verdict: None,
                feedback: None,
                note: None,
                to_agent: None,
                payload: None,
            },
        )
        .unwrap();
        assert!(recover_late_worker_completion(
            &mut c,
            mailbox_id,
            "replacement",
            task_id,
            42,
            None,
            1004
        )
        .unwrap());
        let task = get(&c, task_id).unwrap().unwrap();
        assert_eq!(task.status, "in-review");
        assert_eq!(extract_pr_number(&task.refs), Some(42));
        assert_eq!(task.author.as_deref(), Some("original"));
    }

    #[test]
    fn late_r1_approval_persists_once_without_merging() {
        let (_d, mut c) = open_tmp();
        let (task_id, mailbox_id) = late_reviewer_fixture(&mut c, false);
        assert!(recover_late_reviewer_verdict(
            &mut c,
            mailbox_id,
            "reviewer",
            task_id,
            42,
            LateReviewerVerdict::Approved,
            0,
            "head",
            None,
            1003
        )
        .unwrap());
        assert_eq!(get(&c, task_id).unwrap().unwrap().status, "in-review");
        assert_eq!(crate::approvals::get_for_pr(&c, 42).unwrap().len(), 1);
        assert!(crate::approvals::get(&c, 42, "r1").unwrap().is_some());
        assert_eq!(crate::mailbox::poll_unconsumed(&c).unwrap().len(), 0);
    }

    #[test]
    fn late_r2_requires_matching_distinct_r1_then_enters_merging() {
        let (_d, mut c) = open_tmp();
        let (task_id, mailbox_id) = late_reviewer_fixture(&mut c, true);
        crate::approvals::record(
            &mut c,
            &crate::approvals::Approval {
                pr_number: 42,
                review_role: "r1".into(),
                task_id,
                author: "worker".into(),
                reviewer: "r1".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "head".into(),
            },
        )
        .unwrap();
        assert!(recover_late_reviewer_verdict(
            &mut c,
            mailbox_id,
            "reviewer",
            task_id,
            42,
            LateReviewerVerdict::Approved,
            0,
            "head",
            None,
            1003
        )
        .unwrap());
        assert_eq!(get(&c, task_id).unwrap().unwrap().status, "merging");
        assert_eq!(crate::approvals::get_for_pr(&c, 42).unwrap().len(), 2);
        let events: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_merging'",
                [lease_target(task_id)],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(events, 1);
    }

    #[test]
    fn late_r2_rejects_missing_or_stale_r1_without_mutation() {
        let (_d, mut c) = open_tmp();
        let (task_id, mailbox_id) = late_reviewer_fixture(&mut c, true);
        assert!(!recover_late_reviewer_verdict(
            &mut c,
            mailbox_id,
            "reviewer",
            task_id,
            42,
            LateReviewerVerdict::Approved,
            0,
            "head",
            None,
            1003,
        )
        .unwrap());
        crate::approvals::record(
            &mut c,
            &crate::approvals::Approval {
                pr_number: 42,
                review_role: "r1".into(),
                task_id,
                author: "worker".into(),
                reviewer: "r1".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "old-head".into(),
            },
        )
        .unwrap();
        assert!(!recover_late_reviewer_verdict(
            &mut c,
            mailbox_id,
            "reviewer",
            task_id,
            42,
            LateReviewerVerdict::Approved,
            0,
            "head",
            None,
            1003,
        )
        .unwrap());
        assert_eq!(get(&c, task_id).unwrap().unwrap().status, "in-review");
        assert_eq!(crate::mailbox::poll_unconsumed(&c).unwrap().len(), 1);
    }

    #[test]
    fn late_r1_and_r2_changes_enter_rework_and_clear_approvals() {
        for r2 in [false, true] {
            let (_d, mut c) = open_tmp();
            let (task_id, mailbox_id) = late_reviewer_fixture(&mut c, r2);
            c.execute(
                "UPDATE mailbox SET verdict='changes', payload='{\"blocking\":1}' WHERE id=?1",
                [mailbox_id],
            )
            .unwrap();
            crate::approvals::record(
                &mut c,
                &crate::approvals::Approval {
                    pr_number: 42,
                    review_role: "r1".into(),
                    task_id,
                    author: "worker".into(),
                    reviewer: "prior-r1".into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "head".into(),
                },
            )
            .unwrap();
            assert!(recover_late_reviewer_verdict(
                &mut c,
                mailbox_id,
                "reviewer",
                task_id,
                42,
                LateReviewerVerdict::Changes,
                1,
                "",
                Some("fix the blocking finding"),
                1003,
            )
            .unwrap());
            let task = get(&c, task_id).unwrap().unwrap();
            assert_eq!(task.status, "rework");
            let refs: serde_json::Value =
                serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
            assert_eq!(refs[PARKED_REWORK_RETRY_REF], true);
            assert_eq!(refs["remediation_feedback"], "fix the blocking finding");
            assert!(crate::approvals::get_for_pr(&c, 42).unwrap().is_empty());
            assert_eq!(crate::mailbox::poll_unconsumed(&c).unwrap().len(), 0);
        }
    }

    #[test]
    fn late_changes_finishes_feedback_handoff_after_transition_already_committed() {
        let (_d, mut c) = open_tmp();
        let (task_id, mailbox_id) = late_reviewer_fixture(&mut c, false);
        c.execute(
            "UPDATE mailbox SET verdict='changes', payload='{\"blocking\":1}' WHERE id=?1",
            [mailbox_id],
        )
        .unwrap();
        apply_event(&mut c, "reviewer", task_id, &Event::VerdictChanges, 1002).unwrap();

        assert!(recover_late_reviewer_verdict(
            &mut c,
            mailbox_id,
            "reviewer",
            task_id,
            42,
            LateReviewerVerdict::Changes,
            1,
            "",
            Some("resume the exact sticky worker"),
            1003,
        )
        .unwrap());

        let task = get(&c, task_id).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(
            refs["remediation_feedback"],
            "resume the exact sticky worker"
        );
        assert!(refs.get(PARKED_REWORK_RETRY_REF).is_none());
        assert_eq!(crate::mailbox::poll_unconsumed(&c).unwrap().len(), 0);
    }

    #[test]
    fn late_reviewer_rejects_missing_journal_and_rolls_back_on_consume_failure() {
        let (_d, mut c) = open_tmp();
        let (task_id, mailbox_id) = late_reviewer_fixture(&mut c, false);
        c.execute("DELETE FROM journal WHERE agent='reviewer'", [])
            .unwrap();
        assert!(!recover_late_reviewer_verdict(
            &mut c,
            mailbox_id,
            "reviewer",
            task_id,
            42,
            LateReviewerVerdict::Approved,
            0,
            "head",
            None,
            1003
        )
        .unwrap());
        c.execute("INSERT INTO journal(agent,role,task_id,session_id,phase,cost_tokens,cost_usd,pr,rework_count,updated_at) VALUES ('reviewer','reviewer',?1,'run','reviewing',0,0,42,0,1000)", [task_id]).unwrap();
        c.execute_batch("CREATE TRIGGER reject_late_consume BEFORE UPDATE OF consumed_at ON mailbox BEGIN SELECT RAISE(ABORT, 'consume failed'); END;").unwrap();
        assert!(recover_late_reviewer_verdict(
            &mut c,
            mailbox_id,
            "reviewer",
            task_id,
            42,
            LateReviewerVerdict::Approved,
            0,
            "head",
            None,
            1003
        )
        .is_err());
        assert!(crate::approvals::get(&c, 42, "r1").unwrap().is_none());
        assert_eq!(get(&c, task_id).unwrap().unwrap().status, "in-review");
    }

    const TTL: i64 = 3600;

    fn release(conn: &mut Connection, agent: &str, id: i64, now: i64) -> Result<Task> {
        update(
            conn,
            agent,
            id,
            &TaskUpdate {
                status: Some("open"),
                ..Default::default()
            },
            now,
        )
    }

    fn cancel(conn: &mut Connection, agent: &str, id: i64, now: i64) -> Result<Task> {
        update(
            conn,
            agent,
            id,
            &TaskUpdate {
                status: Some("cancelled"),
                ..Default::default()
            },
            now,
        )
    }

    fn has_live_lease(c: &Connection, id: i64, now: i64) -> bool {
        c.query_row(
            "SELECT count(*) FROM claims WHERE target=?1 AND active=1 AND expires_at > ?2",
            params![lease_target(id), now],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    // ── basic claim lifecycle ────────────────────────────────────────────────

    #[test]
    fn create_then_claim_sets_working_and_author() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c, "boss", "fix bug", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", Some(id), &[], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "working");
        assert_eq!(t.assignee.as_deref(), Some("A"));
        assert_eq!(t.author.as_deref(), Some("A"));
        assert!(has_live_lease(&c, id, 1000));
    }

    #[test]
    fn second_claim_of_same_task_is_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, None, 1000).unwrap();
        assert!(claim(&mut c, "A", Some(id), &[], TTL, 1000)
            .unwrap()
            .is_some());
        assert!(claim(&mut c, "B", Some(id), &[], TTL, 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn claim_without_id_picks_highest_priority() {
        let (_d, mut c) = open_tmp();
        create(&mut c, "boss", "low", None, 1, None, None, None, None, 1000).unwrap();
        create(
            &mut c, "boss", "high", None, 9, None, None, None, None, 1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(t.title, "high");
    }

    #[test]
    fn claim_nothing_open_is_none() {
        let (_d, mut c) = open_tmp();
        assert!(claim(&mut c, "A", None, &[], TTL, 1000).unwrap().is_none());
    }

    // ── match-label ─────────────────────────────────────────────────────────

    #[test]
    fn match_label_filters_to_matching_task() {
        let (_d, mut c) = open_tmp();
        create(
            &mut c,
            "boss",
            "high-no-label",
            None,
            9,
            Some(r#"["tier:opus-46"]"#),
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let want = create(
            &mut c,
            "boss",
            "low-with-label",
            None,
            1,
            Some(r#"["tier:opus-47","lang:rust"]"#),
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["tier:opus-47"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want);
    }

    #[test]
    fn match_label_no_match_is_none() {
        let (_d, mut c) = open_tmp();
        create(
            &mut c,
            "boss",
            "rust",
            None,
            5,
            Some(r#"["lang:rust"]"#),
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        assert!(claim(&mut c, "A", None, &["lang:python"], TTL, 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn match_label_is_and_across_repeats() {
        let (_d, mut c) = open_tmp();
        create(
            &mut c,
            "boss",
            "rust-only",
            None,
            9,
            Some(r#"["lang:rust"]"#),
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        create(
            &mut c,
            "boss",
            "tier-only",
            None,
            9,
            Some(r#"["tier:opus-47"]"#),
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let want = create(
            &mut c,
            "boss",
            "rust-and-tier",
            None,
            5,
            Some(r#"["lang:rust","tier:opus-47"]"#),
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["lang:rust", "tier:opus-47"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want);
    }

    // ── reviewer attach ─────────────────────────────────────────────────────

    #[test]
    fn reviewer_attach_on_in_review_task() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1001,
        )
        .unwrap();
        let t = claim(&mut c, "reviewer", Some(id), &[], TTL, 1002)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "in-review");
        assert_eq!(t.reviewer.as_deref(), Some("reviewer"));
        assert_eq!(t.author.as_deref(), Some("author"));
    }

    #[test]
    fn self_review_blocked() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1001,
        )
        .unwrap();
        assert!(
            claim(&mut c, "author", Some(id), &[], TTL, 1002)
                .unwrap()
                .is_none(),
            "author must not be able to review their own task"
        );
    }

    #[test]
    fn reviewer_attach_via_auto_pick() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 5, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1001,
        )
        .unwrap();
        let t = claim(&mut c, "reviewer", None, &[], TTL, 1002)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, id);
        assert_eq!(t.status, "in-review");
        assert_eq!(t.reviewer.as_deref(), Some("reviewer"));
    }

    // ── apply_event lifecycle ───────────────────────────────────────────────

    #[test]
    fn apply_event_signaled_done_transitions_to_in_review() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let r = apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");
        assert!(r.effects.contains(&Effect::SpawnReviewer));
        let refs: serde_json::Value =
            serde_json::from_str(r.task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["pr"], 99);
    }

    #[test]
    fn apply_event_verdict_approve_to_merging() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        let r = apply_event(&mut c, "R", id, &Event::VerdictApprove, 1003).unwrap();
        assert_eq!(r.task.status, "merging");
        assert!(r
            .effects
            .iter()
            .any(|e| matches!(e, Effect::MergePr { .. })));
    }

    #[test]
    fn apply_event_verdict_changes_to_rework() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        let r = apply_event(&mut c, "R", id, &Event::VerdictChanges, 1003).unwrap();
        assert_eq!(r.task.status, "rework");
        assert_eq!(r.task.rework_round, 1);
        assert!(r.effects.contains(&Effect::IncrementReworkRound));
        assert!(r.effects.contains(&Effect::ResumeWorker));
    }

    #[test]
    fn actionable_rework_event_persists_exact_turn_atomically() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone { pr: "99".into() },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        let feedback = "Preserve the published head; merge main and never rebase.";
        let result =
            apply_actionable_rework_event(&mut c, "R", id, &Event::VerdictChanges, feedback, 1003)
                .unwrap();

        assert_eq!(result.task.status, "rework");
        let refs: serde_json::Value =
            serde_json::from_str(result.task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["remediation_feedback"], feedback);
        assert!(c
            .query_row(
                "SELECT 1 FROM claims WHERE target=?1 AND active=1",
                [lease_target(id)],
                |_| Ok(()),
            )
            .optional()
            .unwrap()
            .is_none());
    }

    #[test]
    fn stale_reviewer_failure_after_verdict_changes_is_atomic_noop() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "reviewer", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "reviewer", id, &Event::VerdictChanges, 1003).unwrap();
        claim_remediation_rework(&mut c, "remediation", id, TTL, 1004)
            .unwrap()
            .unwrap();

        let lease_before: (String, i64) = c
            .query_row(
                "SELECT holder, expires_at FROM claims WHERE target=?1 AND active=1",
                [lease_target(id)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let errors_before: i64 = c
            .query_row("SELECT COUNT(*) FROM errors", [], |row| row.get(0))
            .unwrap();

        let result =
            fail_reviewer_if_owner(&mut c, "reviewer", id, "reviewer exited", 1005).unwrap();
        assert!(result.is_none());

        let task = get(&c, id).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.assignee.as_deref(), Some("remediation"));
        let lease_after: (String, i64) = c
            .query_row(
                "SELECT holder, expires_at FROM claims WHERE target=?1 AND active=1",
                [lease_target(id)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(lease_after, lease_before);
        let errors_after: i64 = c
            .query_row("SELECT COUNT(*) FROM errors", [], |row| row.get(0))
            .unwrap();
        assert_eq!(errors_after, errors_before);

        let pushed = apply_event(&mut c, "remediation", id, &Event::ReworkPushed, 1006).unwrap();
        assert_eq!(pushed.task.status, "in-review");
    }

    #[test]
    fn current_reviewer_failure_still_triggers_recovery() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "reviewer", Some(id), &[], TTL, 1002).unwrap();

        let result = fail_reviewer_if_owner(&mut c, "reviewer", id, "reviewer process died", 1003)
            .unwrap()
            .expect("current reviewer failure must apply");
        assert_eq!(result.task.status, "in-review");
        assert!(result.effects.contains(&Effect::SpawnReviewer));
        assert!(result.task.reviewer.is_none());
        assert!(result.task.assignee.is_none());
    }

    #[test]
    fn managed_worker_exit_with_null_pr_submission_stays_pending() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "worker", Some(id), &[], TTL, 1000).unwrap();
        crate::mailbox::append(
            &mut c,
            &crate::mailbox::MailboxRow {
                agent: "worker".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(id),
                pr: None,
                verdict: None,
                feedback: None,
                note: None,
                to_agent: None,
                payload: None,
            },
        )
        .unwrap();

        let disposition = dispose_managed_exit(
            &mut c,
            ManagedRunRole::Worker,
            "worker",
            id,
            "status 0",
            1001,
        )
        .unwrap();
        assert!(matches!(
            disposition,
            ManagedExitDisposition::OutcomePending
        ));
        assert_eq!(get(&c, id).unwrap().unwrap().status, "working");
    }

    #[test]
    fn managed_reviewer_exit_with_pending_verdict_retains_review_phase() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        claim(&mut c, "reviewer", Some(id), &[], TTL, 1002).unwrap();
        crate::mailbox::append(
            &mut c,
            &crate::mailbox::MailboxRow {
                agent: "reviewer".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(id),
                pr: Some(42),
                verdict: Some("changes".into()),
                feedback: Some("fix it".into()),
                note: None,
                to_agent: None,
                payload: None,
            },
        )
        .unwrap();

        let disposition = dispose_managed_exit(
            &mut c,
            ManagedRunRole::Reviewer,
            "reviewer",
            id,
            "status 0",
            1003,
        )
        .unwrap();
        assert!(matches!(
            disposition,
            ManagedExitDisposition::OutcomePending
        ));
        assert_eq!(get(&c, id).unwrap().unwrap().status, "in-review");
        let review_events: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_in_review'",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            review_events, 1,
            "exit must not duplicate review transition"
        );
    }

    #[test]
    fn managed_reviewer_exit_after_consumed_verdict_is_cleanup_only() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        claim(&mut c, "reviewer", Some(id), &[], TTL, 1002).unwrap();
        let row_id = crate::mailbox::append(
            &mut c,
            &crate::mailbox::MailboxRow {
                agent: "reviewer".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(id),
                pr: Some(42),
                verdict: Some("changes".into()),
                feedback: Some("fix it".into()),
                note: None,
                to_agent: None,
                payload: None,
            },
        )
        .unwrap();
        apply_event(&mut c, "reviewer", id, &Event::VerdictChanges, 1003).unwrap();
        crate::mailbox::mark_consumed(&mut c, row_id).unwrap();

        let disposition = dispose_managed_exit(
            &mut c,
            ManagedRunRole::Reviewer,
            "reviewer",
            id,
            "status 0",
            1004,
        )
        .unwrap();
        assert!(matches!(
            disposition,
            ManagedExitDisposition::OutcomeRecorded
        ));
        assert_eq!(get(&c, id).unwrap().unwrap().status, "rework");
        let rework_events: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_rework'",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            rework_events, 1,
            "exit must not duplicate rework transition"
        );
    }

    #[test]
    fn historical_worker_submission_does_not_hide_current_rework_failure() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "worker", Some(id), &[], TTL, 1000).unwrap();
        let row_id = crate::mailbox::append(
            &mut c,
            &crate::mailbox::MailboxRow {
                agent: "worker".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(id),
                pr: None,
                verdict: None,
                feedback: None,
                note: None,
                to_agent: None,
                payload: None,
            },
        )
        .unwrap();
        apply_event(
            &mut c,
            "worker",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        crate::mailbox::mark_consumed(&mut c, row_id).unwrap();
        claim(&mut c, "reviewer", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "reviewer", id, &Event::VerdictChanges, 1003).unwrap();

        let disposition = dispose_managed_exit(
            &mut c,
            ManagedRunRole::Worker,
            "worker",
            id,
            "status 1: no rework submission",
            1004,
        )
        .unwrap();
        assert!(matches!(
            disposition,
            ManagedExitDisposition::AgentFailed(_)
        ));
        assert_eq!(get(&c, id).unwrap().unwrap().status, "open");
    }

    #[test]
    fn managed_reviewer_exit_without_verdict_recovers_once() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        claim(&mut c, "r1", Some(id), &[], TTL, 1002).unwrap();

        let disposition = dispose_managed_exit(
            &mut c,
            ManagedRunRole::Reviewer,
            "r1",
            id,
            "status 1: no verdict",
            1003,
        )
        .unwrap();
        let ManagedExitDisposition::AgentFailed(transition) = disposition else {
            panic!("owner without verdict must fail");
        };
        assert_eq!(
            transition
                .effects
                .iter()
                .filter(|effect| matches!(effect, Effect::SpawnReviewer))
                .count(),
            1,
            "one exit may request exactly one replacement reviewer"
        );
        let task = get(&c, id).unwrap().unwrap();
        assert_eq!(task.status, "in-review");
        assert!(task.reviewer.is_none());
        let recovery_events: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_in_review'",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(recovery_events, 2);
    }

    #[test]
    fn r2_pending_verdict_is_retained_without_duplicate_reviewer_request() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        claim(&mut c, "r2", Some(id), &[], TTL, 1002).unwrap();
        crate::mailbox::append(
            &mut c,
            &crate::mailbox::MailboxRow {
                agent: "r2".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(id),
                pr: Some(42),
                verdict: Some("approved".into()),
                feedback: None,
                note: None,
                to_agent: None,
                payload: Some(r#"{"blocking":0}"#.into()),
            },
        )
        .unwrap();

        assert!(matches!(
            dispose_managed_exit(&mut c, ManagedRunRole::Reviewer, "r2", id, "status 0", 1003,)
                .unwrap(),
            ManagedExitDisposition::OutcomePending
        ));
        let review_events: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_in_review'",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(review_events, 1);
    }

    #[test]
    fn remediation_submission_exit_is_pending_then_recorded_without_reopening() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "author", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "author",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        claim(&mut c, "reviewer", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "reviewer", id, &Event::VerdictChanges, 1003).unwrap();
        claim_remediation_rework(&mut c, "remediation", id, TTL, 1004).unwrap();
        let row_id = crate::mailbox::append(
            &mut c,
            &crate::mailbox::MailboxRow {
                agent: "remediation".into(),
                kind: crate::mailbox::MailboxKind::Done,
                task_id: Some(id),
                pr: Some(42),
                verdict: None,
                feedback: None,
                note: None,
                to_agent: None,
                payload: None,
            },
        )
        .unwrap();
        assert!(matches!(
            dispose_managed_exit(
                &mut c,
                ManagedRunRole::Worker,
                "remediation",
                id,
                "status 0",
                1005,
            )
            .unwrap(),
            ManagedExitDisposition::OutcomePending
        ));
        assert!(worker_lease_active_for(&mut c, "remediation", id, 1005).unwrap());

        apply_event(&mut c, "remediation", id, &Event::ReworkPushed, 1006).unwrap();
        crate::mailbox::mark_consumed(&mut c, row_id).unwrap();
        assert!(matches!(
            dispose_managed_exit(
                &mut c,
                ManagedRunRole::Worker,
                "remediation",
                id,
                "status 0",
                1007,
            )
            .unwrap(),
            ManagedExitDisposition::OutcomeRecorded
        ));
        assert!(
            !worker_lease_active_for(&mut c, "remediation", id, 1007).unwrap(),
            "recorded worker cleanup must retire its lease"
        );
        assert_eq!(get(&c, id).unwrap().unwrap().status, "in-review");
        let review_events: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_in_review'",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            review_events, 2,
            "remediation exit must not add a third review transition"
        );
    }

    #[test]
    fn transferred_worker_exit_preserves_successor_lease() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "retiring", Some(id), &[], TTL, 1000).unwrap();
        crate::capabilities::issue(&mut c, "run-retiring", id, "retiring", "worker", 1000).unwrap();
        apply_event(
            &mut c,
            "retiring",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        c.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND holder='retiring'",
            [lease_target(id)],
        )
        .unwrap();
        c.execute(
            "INSERT INTO claims(target,holder,ts,expires_at,active)
             VALUES (?1,'successor',1002,2002,1)",
            [lease_target(id)],
        )
        .unwrap();
        crate::capabilities::issue(&mut c, "run-successor", id, "successor", "worker", 1002)
            .unwrap();

        let disposition = dispose_managed_run_exit(
            &mut c,
            ManagedRunRole::Worker,
            "retiring",
            id,
            "run-retiring",
            "late terminal event",
            1003,
        )
        .unwrap();
        assert!(matches!(
            disposition,
            ManagedExitDisposition::OwnershipTransferred
        ));
        let successor: (String, i64) = c
            .query_row(
                "SELECT holder, expires_at FROM claims
                 WHERE target=?1 AND active=1",
                [lease_target(id)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(successor, ("successor".into(), 2002));
        assert!(
            crate::capabilities::validate(&c, "run-retiring", "retiring", "worker", Some(id))
                .is_err(),
            "retiring capability must be revoked atomically"
        );
        assert!(
            crate::capabilities::validate(&c, "run-successor", "successor", "worker", Some(id))
                .is_ok(),
            "successor capability must remain active"
        );
    }

    #[test]
    fn recycled_worker_name_cleanup_preserves_successor_authority() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "reused", Some(id), &[], TTL, 1000).unwrap();
        crate::capabilities::issue(&mut c, "run-old", id, "reused", "worker", 1000).unwrap();
        apply_event(
            &mut c,
            "reused",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1001,
        )
        .unwrap();
        c.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND holder='reused'",
            [lease_target(id)],
        )
        .unwrap();
        c.execute(
            "INSERT INTO claims(target,holder,ts,expires_at,active)
             VALUES (?1,'reused',1002,2002,1)",
            [lease_target(id)],
        )
        .unwrap();
        crate::capabilities::issue(&mut c, "run-new", id, "reused", "worker", 1002).unwrap();

        for reason in ["late terminal event", "duplicate cleanup"] {
            assert!(matches!(
                dispose_managed_run_exit(
                    &mut c,
                    ManagedRunRole::Worker,
                    "reused",
                    id,
                    "run-old",
                    reason,
                    1003,
                )
                .unwrap(),
                ManagedExitDisposition::OwnershipTransferred
            ));
        }

        let successor_lease: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM claims
                 WHERE target=?1 AND holder='reused' AND active=1",
                [lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(successor_lease, 1);
        assert!(
            crate::capabilities::validate(&c, "run-old", "reused", "worker", Some(id)).is_err()
        );
        assert!(crate::capabilities::validate(&c, "run-new", "reused", "worker", Some(id)).is_ok());
    }

    #[test]
    fn apply_event_rework_pushed_back_to_in_review() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "R", id, &Event::VerdictChanges, 1003).unwrap();
        let r = apply_event(&mut c, "A", id, &Event::ReworkPushed, 1004).unwrap();
        assert_eq!(r.task.status, "in-review");
        assert!(r.effects.contains(&Effect::ResumeReviewer));
    }

    #[test]
    fn apply_event_merge_succeeded_to_done() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "R", id, &Event::VerdictApprove, 1003).unwrap();
        let r = apply_event(&mut c, "system", id, &Event::MergeSucceeded, 1004).unwrap();
        assert_eq!(r.task.status, "done");
        assert!(r.effects.contains(&Effect::ReleaseLease));
    }

    #[test]
    fn apply_event_merging_agent_failed_to_in_review() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "R", id, &Event::VerdictApprove, 1003).unwrap();
        let r = apply_event(
            &mut c,
            "system",
            id,
            &Event::AgentFailed {
                reason: "force-kill during merge".into(),
            },
            1004,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");
        assert!(r.effects.contains(&Effect::ResumeReviewer));
        assert!(r.effects.iter().any(|e| matches!(e, Effect::NotifyOwner { reason } if reason.contains("force-kill during merge"))));
    }

    #[test]
    fn apply_event_illegal_transition_is_err() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        let err = apply_event(&mut c, "A", id, &Event::VerdictApprove, 1001);
        assert!(err.is_err());
    }

    #[test]
    fn apply_event_cancelled_from_working() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let r = apply_event(
            &mut c,
            "A",
            id,
            &Event::Cancelled {
                by: "boss".to_string(),
            },
            1001,
        )
        .unwrap();
        assert_eq!(r.task.status, "cancelled");
        assert!(r.effects.contains(&Effect::ReleaseLease));
        assert!(!has_live_lease(&c, id, 1001));
    }

    // ── apply_event caller authorization ──────────────────────────────────

    #[test]
    fn apply_event_stale_agent_signaled_done_rejected() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        // Stale agent B tries to fire SignaledDone on A's task
        let err = apply_event(
            &mut c,
            "B",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        );
        assert!(matches!(err, Err(QuorumError::NotHolder)));
    }

    #[test]
    fn apply_event_stale_agent_rework_pushed_rejected() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "R", id, &Event::VerdictChanges, 1003).unwrap();
        // Stale agent B tries ReworkPushed on A's task
        let err = apply_event(&mut c, "B", id, &Event::ReworkPushed, 1004);
        assert!(matches!(err, Err(QuorumError::NotHolder)));
    }

    #[test]
    fn apply_event_verdict_from_non_reviewer_rejected() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        // Non-reviewer X tries to approve
        let err = apply_event(&mut c, "X", id, &Event::VerdictApprove, 1003);
        assert!(matches!(err, Err(QuorumError::NotHolder)));
        // Non-reviewer X tries to request changes
        let err = apply_event(&mut c, "X", id, &Event::VerdictChanges, 1003);
        assert!(matches!(err, Err(QuorumError::NotHolder)));
    }

    #[test]
    fn apply_event_system_events_accept_any_caller() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        // System agent fires AgentFailed — should succeed even though "system" isn't the assignee
        let r = apply_event(
            &mut c,
            "system",
            id,
            &Event::AgentFailed {
                reason: "crashed".to_string(),
            },
            1001,
        )
        .unwrap();
        assert_eq!(r.task.status, "open");
    }

    #[test]
    fn apply_event_signaled_done_after_reclaim_rejected() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        // A claims and works
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        // A's lease expires (simulate via AgentFailed → open)
        apply_event(
            &mut c,
            "A",
            id,
            &Event::AgentFailed {
                reason: "lease expired".to_string(),
            },
            1001,
        )
        .unwrap();
        // B reclaims
        claim(&mut c, "B", Some(id), &[], TTL, 1002).unwrap();
        // Stale A fires SignaledDone — must be rejected
        let err = apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "50".to_string(),
            },
            1003,
        );
        assert!(matches!(err, Err(QuorumError::NotHolder)));
        // B's SignaledDone works
        let r = apply_event(
            &mut c,
            "B",
            id,
            &Event::SignaledDone {
                pr: "51".to_string(),
            },
            1004,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");
    }

    #[test]
    fn replacement_worker_signaled_done_with_preserved_author() {
        // Regression: task #9 — replacement worker with preserved authorship
        // (PR exists) must be able to signal done via assignee check.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();

        // A claims → author=A, assignee=A
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();

        // A signals done with PR
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "50".to_string(),
            },
            1001,
        )
        .unwrap();

        // Reviewer R claims and requests changes → rework
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "R", id, &Event::VerdictChanges, 1003).unwrap();

        // A fails during rework → open (author=A preserved because PR exists)
        apply_event(
            &mut c,
            "A",
            id,
            &Event::AgentFailed {
                reason: "crashed".to_string(),
            },
            1004,
        )
        .unwrap();

        // B reclaims → assignee=B, author=A (preserved)
        let t = claim(&mut c, "B", Some(id), &[], TTL, 1005)
            .unwrap()
            .unwrap();
        assert_eq!(t.assignee.as_deref(), Some("B"));
        assert_eq!(t.author.as_deref(), Some("A"), "author must be preserved");

        // B signals done — authorized by assignee, not author
        let r = apply_event(
            &mut c,
            "B",
            id,
            &Event::SignaledDone {
                pr: "51".to_string(),
            },
            1006,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");
        assert_eq!(
            r.task.author.as_deref(),
            Some("A"),
            "original author preserved through submit"
        );
    }

    #[test]
    fn replacement_worker_rework_pushed_via_capability() {
        // During Rework, assignee is restored to author by ResumeWorker.
        // A replacement worker (not the author) needs a capability to push rework.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();

        // A claims → author=A, assignee=A
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "50".to_string(),
            },
            1001,
        )
        .unwrap();

        // Reviewer R claims and gives changes → rework
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "R", id, &Event::VerdictChanges, 1003).unwrap();

        // ResumeWorker restored assignee to author=A.
        // B (replacement worker) pushes rework — needs capability.
        crate::capabilities::issue(&mut c, "run-b", id, "B", "worker", 1003).unwrap();
        let r = apply_event(&mut c, "B", id, &Event::ReworkPushed, 1004).unwrap();
        assert_eq!(r.task.status, "in-review");

        // Stale agent without capability must be rejected
        let err = apply_event(&mut c, "stale", id, &Event::ReworkPushed, 1005);
        assert!(err.is_err());
    }

    // ── review-only task ────────────────────────────────────────────────────

    #[test]
    fn create_with_review_pr_starts_in_review() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "review PR #50",
            None,
            100,
            None,
            None,
            None,
            Some(50),
            1000,
        )
        .unwrap();
        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "in-review");
        assert!(t.review_only);
        let refs: serde_json::Value = serde_json::from_str(t.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["pr"], 50);
    }

    #[test]
    fn create_with_continue_pr_starts_open_without_forging_refs() {
        let (_d, mut c) = open_tmp();
        let id = super::create_with_continue_pr(
            &mut c,
            "boss",
            "continue PR #50",
            None,
            100,
            None,
            Some(r#"{"ticket":"ABC-1"}"#),
            None,
            None,
            Some(50),
            1000,
        )
        .unwrap();
        let task = get(&c, id).unwrap().unwrap();
        assert_eq!(task.status, "open");
        assert!(!task.review_only);
        assert_eq!(task.continue_pr, Some(50));
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs, serde_json::json!({"ticket":"ABC-1"}));
    }

    #[test]
    fn review_only_verdict_changes_reworks() {
        // #159: review_only + changes → rework (remediation workers).
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "review PR #50",
            None,
            100,
            None,
            None,
            None,
            Some(50),
            1000,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1001).unwrap();
        let r = apply_event(&mut c, "R", id, &Event::VerdictChanges, 1002).unwrap();
        assert_eq!(r.task.status, "rework");
    }

    // ── update backward compat ──────────────────────────────────────────────

    #[test]
    fn release_from_working() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let t = release(&mut c, "A", id, 1001).unwrap();
        assert_eq!(t.status, "open");
        assert!(t.assignee.is_none());
        assert!(!has_live_lease(&c, id, 1001));
    }

    #[test]
    fn cancel_from_working() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let t = cancel(&mut c, "A", id, 1001).unwrap();
        assert_eq!(t.status, "cancelled");
        assert!(!has_live_lease(&c, id, 1001));
    }

    #[test]
    fn update_done_rejected() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1001,
        );
        assert!(matches!(err, Err(QuorumError::Usage(_))));
        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "working");
    }

    #[test]
    fn update_non_holder_fails() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "B",
            id,
            &TaskUpdate {
                status: Some("cancelled"),
                ..Default::default()
            },
            1001,
        );
        assert!(matches!(err, Err(QuorumError::NotHolder)));
    }

    #[test]
    fn update_restricted_status_rejected() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("in-review"),
                ..Default::default()
            },
            1001,
        );
        assert!(matches!(err, Err(QuorumError::Usage(_))));
    }

    #[test]
    fn cancelled_task_with_legacy_parked_body_is_terminal() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("cancelled"),
                body: Some("daemon:parked:merge-blocked"),
                expected_revision: Some(1),
                ..Default::default()
            },
            1001,
        )
        .unwrap();
        assert!(matches!(
            release(&mut c, "A", id, 1002),
            Err(QuorumError::NotHolder)
        ));
        assert_eq!(get(&c, id).unwrap().unwrap().status, "cancelled");
    }

    // ── deps ────────────────────────────────────────────────────────────────

    #[test]
    fn dep_blocks_claim_until_done() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let child = create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        assert!(
            claim(&mut c, "A", Some(child), &[], TTL, 1000)
                .unwrap()
                .is_none(),
            "child must not be claimable while dep is open"
        );
        claim(&mut c, "W", Some(dep), &[], TTL, 1000).unwrap();
        close_after_merge(&mut c, dep, "merged", 1001).unwrap();
        let t = claim(&mut c, "A", Some(child), &[], TTL, 1002)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "working");
    }

    #[test]
    fn cancelled_dependency_and_parked_child_stay_blocked() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let child = create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "W", Some(dep), &[], TTL, 1000).unwrap();
        cancel(&mut c, "W", dep, 1001).unwrap();
        // Child is blocked (dep is cancelled, not done)
        assert!(!get(&c, child).unwrap().unwrap().ready);
        assert!(matches!(
            release(&mut c, "boss", dep, 1003),
            Err(QuorumError::NotHolder)
        ));
        // Task #473: cancelled dep is unsatisfiable — `retry_parked` refuses
        // rather than silently restoring a task the sweep would re-park on
        // the next tick. The child stays failed/parked; only a `depends_on`
        // edit or explicit close clears the disposition.
        assert!(retry_parked(&mut c, child, "boss", true, 1004)
            .unwrap()
            .is_none());
        let child_row = get(&c, child).unwrap().unwrap();
        assert_eq!(child_row.status, "failed");
        assert!(claim(&mut c, "A", Some(child), &[], TTL, 1005)
            .unwrap()
            .is_none());
    }

    #[test]
    fn edit_depends_on_unblocks_stuck_child() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let child = create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "W", Some(dep), &[], TTL, 1000).unwrap();
        cancel(&mut c, "W", dep, 1001).unwrap();
        // child is blocked (dep is cancelled, not done)
        assert!(!get(&c, child).unwrap().unwrap().ready);
        // Corrective metadata edits are allowed while parked.
        let updated = update(
            &mut c,
            "boss",
            child,
            &TaskUpdate {
                depends_on: Some("[]"),
                expected_revision: Some(1),
                ..Default::default()
            },
            1003,
        )
        .unwrap();
        assert!(updated.ready);
        assert_eq!(updated.depends_on.as_deref(), Some("[]"));
        assert_eq!(updated.status, "failed");
        let resumed = retry_parked(&mut c, child, "boss", true, 1003)
            .unwrap()
            .unwrap();
        assert_eq!(resumed.status, "open");
        assert!(
            !classification_is_complete(&resumed.refs),
            "dependency edits must invalidate the old classifier envelope"
        );
        assert!(
            claim(&mut c, "A", Some(child), &[], TTL, 1004)
                .unwrap()
                .is_none(),
            "clearing dependencies does not bypass fresh classification"
        );
        crate::classify::store_classifications(
            &mut c,
            &[crate::classify::TaskClassification {
                task_id: child,
                cx_est: 3,
                size: "M".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: vec![],
            }],
            "test:v2",
            1004,
        )
        .unwrap();
        // Fresh classification restores eligibility after the dependency edit.
        let t = claim(&mut c, "A", Some(child), &[], TTL, 1004)
            .unwrap()
            .expect("freshly classified child with cleared deps should be claimable");
        assert_eq!(t.status, "working");
    }

    #[test]
    fn cancelled_task_cannot_be_reopened() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        cancel(&mut c, "A", id, 1001).unwrap();
        assert!(matches!(
            release(&mut c, "boss", id, 1002),
            Err(QuorumError::NotHolder)
        ));
        assert_eq!(get(&c, id).unwrap().unwrap().status, "cancelled");
    }

    #[test]
    fn update_depends_on_edits_deps() {
        let (_d, mut c) = open_tmp();
        let dep1 = create(
            &mut c, "boss", "dep1", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        let _dep2 = create(
            &mut c, "boss", "dep2", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        let child = create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep1}]")),
            None,
            1000,
        )
        .unwrap();
        // Child depends on dep1 — not ready
        let t = get(&c, child).unwrap().unwrap();
        assert!(!t.ready);
        // Creator edits deps to point at dep2 (already done? no, but let's clear deps)
        let t = update(
            &mut c,
            "boss",
            child,
            &TaskUpdate {
                depends_on: Some("[]"),
                expected_revision: Some(1),
                ..Default::default()
            },
            1001,
        )
        .unwrap();
        assert!(t.ready);
        assert_eq!(t.depends_on.as_deref(), Some("[]"));
    }

    #[test]
    fn update_depends_on_rejects_non_creator_non_assignee() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "t",
            None,
            0,
            None,
            None,
            Some("[999]"),
            None,
            1000,
        )
        .unwrap();
        let err = update(
            &mut c,
            "rando",
            id,
            &TaskUpdate {
                depends_on: Some("[]"),
                expected_revision: Some(1),
                ..Default::default()
            },
            1001,
        );
        assert!(matches!(err, Err(QuorumError::NotHolder)));
    }

    // ── close_after_merge ───────────────────────────────────────────────────

    #[test]
    fn close_after_merge_sets_done() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let changed = close_after_merge(&mut c, id, "merged", 1001).unwrap();
        assert!(changed);
        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "done");
        assert!(!has_live_lease(&c, id, 1001));
    }

    #[test]
    fn close_after_merge_preserves_body() {
        let (_d, mut c) = open_tmp();
        let original_body = "implement the flux capacitor with detailed design notes";
        let id = create(
            &mut c,
            "boss",
            "t",
            Some(original_body),
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let merge_note = "daemon: PR #999 merged on restart recovery (approved by R, #228)";
        let changed = close_after_merge(&mut c, id, merge_note, 1001).unwrap();
        assert!(changed);
        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "done");
        assert_eq!(
            t.body.as_deref(),
            Some(original_body),
            "body must be byte-exact preserved"
        );
        let td = get_with_notes(&c, id).unwrap().unwrap();
        assert!(
            td.notes.iter().any(|n| n.body.contains("PR #999")),
            "merge evidence must appear as a task note"
        );
    }

    #[test]
    fn close_after_merge_idempotent() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        close_after_merge(&mut c, id, "merged", 1001).unwrap();
        let changed = close_after_merge(&mut c, id, "merged", 1002).unwrap();
        assert!(!changed, "already done — should be idempotent");
    }

    #[test]
    fn close_after_merge_idempotent_no_duplicate_notes() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "t",
            Some("original"),
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        close_after_merge(&mut c, id, "merged", 1001).unwrap();
        close_after_merge(&mut c, id, "merged", 1002).unwrap();
        let td = get_with_notes(&c, id).unwrap().unwrap();
        let merge_notes: Vec<_> = td
            .notes
            .iter()
            .filter(|n| n.body.contains("merged"))
            .collect();
        assert_eq!(
            merge_notes.len(),
            1,
            "idempotent call must not duplicate notes"
        );
    }

    // ── close_manual ────────────────────────────────────────────────────────

    #[test]
    fn close_manual_from_working() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let t = close_manual(&mut c, "owner", id, "fixed elsewhere", 1001)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "done");
        assert!(t.assignee.is_none());
        assert!(!has_live_lease(&c, id, 1001));
        // Verify note was appended
        let td = get_with_notes(&c, id).unwrap().unwrap();
        assert!(td.notes.iter().any(|n| n.body.contains("fixed elsewhere")));
    }

    #[test]
    fn close_manual_from_open() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        let t = close_manual(&mut c, "owner", id, "obsolete", 1001)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "done");
    }

    #[test]
    fn close_manual_already_terminal_is_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        cancel(&mut c, "A", id, 1001).unwrap();
        assert!(
            close_manual(&mut c, "owner", id, "too late", 1002)
                .unwrap()
                .is_none(),
            "already cancelled — should return None"
        );
    }

    #[test]
    fn close_manual_from_failed_unblocks_dependents() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let child = create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        c.execute("UPDATE tasks SET status='failed' WHERE id=?1", params![dep])
            .unwrap();
        assert!(!get(&c, child).unwrap().unwrap().ready);

        let t = close_manual(&mut c, "owner", dep, "PR merged by hand", 1001)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "done");
        assert!(
            compute_ready(&c, &get(&c, child).unwrap().unwrap().depends_on).unwrap(),
            "dependent must become ready once the failed head closes to done"
        );
    }

    #[test]
    fn close_manual_emits_distinct_event() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        close_manual(&mut c, "owner", id, "merged by hand", 1001).unwrap();
        let events = crate::events::list(&c, 0, Some(&lease_target(id)), 100, 2000).unwrap();
        assert!(
            events.iter().any(|e| e.kind == "task_closed_manual"),
            "must emit task_closed_manual event"
        );
        assert!(
            !events.iter().any(|e| e.kind == "task_done" && e.ts == 1001),
            "must NOT emit task_done"
        );
    }

    // ── notes ───────────────────────────────────────────────────────────────

    #[test]
    fn add_note_and_get_with_notes() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        let nid = add_note(&mut c, "A", id, "hello", 1001).unwrap().unwrap();
        assert!(nid > 0);
        let td = get_with_notes(&c, id).unwrap().unwrap();
        assert_eq!(td.notes.len(), 1);
        assert_eq!(td.notes[0].body, "hello");
    }

    #[test]
    fn add_note_nonexistent_task_is_none() {
        let (_d, mut c) = open_tmp();
        assert!(add_note(&mut c, "A", 999, "x", 1000).unwrap().is_none());
    }

    // ── list ────────────────────────────────────────────────────────────────

    #[test]
    fn list_filters_by_status() {
        let (_d, mut c) = open_tmp();
        create(&mut c, "boss", "t1", None, 0, None, None, None, None, 1000).unwrap();
        let id2 = create(&mut c, "boss", "t2", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id2), &[], TTL, 1000).unwrap();
        let open = list(&c, Some("open"), None, None).unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].status, "open");
        let working = list(&c, Some("working"), None, None).unwrap();
        assert_eq!(working.len(), 1);
        assert_eq!(working[0].status, "working");
    }

    // ── full happy-path lifecycle ────────────────────────────────────────────

    #[test]
    fn full_lifecycle_open_to_done() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c, "boss", "feature", None, 5, None, None, None, None, 1000,
        )
        .unwrap();

        // open → working (claim)
        let t = claim(&mut c, "worker", Some(id), &[], TTL, 1001)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "working");
        assert_eq!(t.author.as_deref(), Some("worker"));

        // working → in-review (SignaledDone)
        let r = apply_event(
            &mut c,
            "worker",
            id,
            &Event::SignaledDone {
                pr: "100".to_string(),
            },
            1002,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");

        // attach reviewer
        let t = claim(&mut c, "reviewer", Some(id), &[], TTL, 1003)
            .unwrap()
            .unwrap();
        assert_eq!(t.reviewer.as_deref(), Some("reviewer"));

        // in-review → merging (VerdictApprove)
        let r = apply_event(&mut c, "reviewer", id, &Event::VerdictApprove, 1004).unwrap();
        assert_eq!(r.task.status, "merging");

        // merging → done (MergeSucceeded)
        let r = apply_event(&mut c, "system", id, &Event::MergeSucceeded, 1005).unwrap();
        assert_eq!(r.task.status, "done");
    }

    #[test]
    fn direct_claim_routes_moderate_l_and_all_continuation_sizes() {
        let (_d, mut conn) = open_tmp();
        let moderate_l = create(
            &mut conn,
            "owner",
            "moderate large",
            None,
            100,
            None,
            Some(r#"{"cx_est":3,"cx_size":"L"}"#),
            None,
            None,
            1,
        )
        .unwrap();
        let complex_l = create(
            &mut conn,
            "owner",
            "complex large",
            None,
            1,
            None,
            Some(r#"{"cx_est":4,"cx_size":"L"}"#),
            None,
            None,
            2,
        )
        .unwrap();

        let moderate_refs = get(&conn, moderate_l).unwrap().unwrap().refs;
        assert!(classification_is_complete(&moderate_refs));
        assert!(classification_is_dispatchable(&moderate_refs, false, None));
        assert!(claim(&mut conn, "moderate", Some(moderate_l), &[], TTL, 3)
            .unwrap()
            .is_some());
        assert!(claim(&mut conn, "complex", Some(complex_l), &[], TTL, 4)
            .unwrap()
            .is_none());

        let untouched = get(&conn, complex_l).unwrap().unwrap();
        assert_eq!(untouched.status, "open");
        assert_eq!(untouched.assignee, None);
        assert!(!has_live_lease(&conn, complex_l, 4));

        for (index, size) in ["S", "M", "L", "XL"].into_iter().enumerate() {
            let id = create_with_continue_pr(
                &mut conn,
                "owner",
                &format!("continue {size}"),
                None,
                1,
                None,
                Some(&format!(
                    r#"{{"cx_est":5,"cx_size":"{size}","cx_ready":true,"cx_not_ready_reason":null}}"#
                )),
                None,
                None,
                Some(100 + index as i64),
                10 + index as i64,
            )
            .unwrap();
            let task = get(&conn, id).unwrap().unwrap();
            assert!(classification_is_dispatchable(
                &task.refs,
                task.review_only,
                task.continue_pr
            ));
            assert!(claim(
                &mut conn,
                &format!("continue-{size}"),
                Some(id),
                &[],
                TTL,
                20 + index as i64,
            )
            .unwrap()
            .is_some());
        }
    }

    #[test]
    fn rework_cycle() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W",
            id,
            &Event::SignaledDone {
                pr: "1".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();

        // First rework
        let r = apply_event(&mut c, "R", id, &Event::VerdictChanges, 1003).unwrap();
        assert_eq!(r.task.status, "rework");
        assert_eq!(r.task.rework_round, 1);

        // Push rework
        let r = apply_event(&mut c, "W", id, &Event::ReworkPushed, 1004).unwrap();
        assert_eq!(r.task.status, "in-review");

        // Second round — approve
        let r = apply_event(&mut c, "R", id, &Event::VerdictApprove, 1005).unwrap();
        assert_eq!(r.task.status, "merging");
    }

    // ── validate inputs ─────────────────────────────────────────────────────

    #[test]
    fn invalid_depends_on_rejected() {
        let (_d, mut c) = open_tmp();
        let err = create(
            &mut c,
            "boss",
            "t",
            None,
            0,
            None,
            None,
            Some("not-json"),
            None,
            1000,
        );
        assert!(matches!(err, Err(QuorumError::Usage(_))));
    }

    #[test]
    fn invalid_labels_rejected() {
        let (_d, mut c) = open_tmp();
        let err = create(
            &mut c,
            "boss",
            "t",
            None,
            0,
            Some("not-json"),
            None,
            None,
            None,
            1000,
        );
        assert!(matches!(err, Err(QuorumError::Usage(_))));
    }

    // ── metadata update ─────────────────────────────────────────────────────

    // ── effect dispatch: NotifyOwner + PostFindingsNote ────────────────────

    #[test]
    fn rework_cap_exceeded_posts_alert_to_creator() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "99".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        c.execute(
            "UPDATE tasks SET rework_round=?1 WHERE id=?2",
            params![crate::lifecycle::REWORK_CAP as i64, id],
        )
        .unwrap();

        let r = apply_event(&mut c, "R", id, &Event::VerdictChanges, 1003).unwrap();
        assert_eq!(r.task.status, "failed");
        assert!(r
            .effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));

        let msgs = crate::feed::peek(&c, None, None, 10, 1003).unwrap();
        let alert = msgs
            .iter()
            .find(|m| m.kind == "alert" && m.recipient.as_deref() == Some("owner"))
            .expect("alert message to creator missing");
        assert!(
            alert.body.contains("rework cap"),
            "alert body should mention rework cap: {}",
            alert.body
        );
        assert!(
            alert.body.contains("rework cap (7) exceeded"),
            "alert body should report the configured cap: {}",
            alert.body
        );
        assert!(alert
            .refs
            .as_deref()
            .unwrap()
            .contains(&format!("task:{id}")));
    }

    #[test]
    fn review_only_verdict_changes_enters_rework() {
        // #159: review_only + changes → rework (remediation workers spawn).
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "review PR #50",
            None,
            100,
            None,
            None,
            None,
            Some(50),
            1000,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1001).unwrap();

        let r = apply_event(&mut c, "R", id, &Event::VerdictChanges, 1002).unwrap();
        assert_eq!(r.task.status, "rework");
        assert!(r.effects.contains(&Effect::IncrementReworkRound));
        assert!(r.effects.contains(&Effect::ResumeWorker));
    }

    /// Walk a review-only task into `rework` (claim + changes verdict) and
    /// return its id. rework_round is 1 afterwards.
    fn review_only_task_in_rework(c: &mut Connection) -> i64 {
        let id = create(
            c,
            "boss",
            "review PR #50",
            None,
            100,
            None,
            None,
            None,
            Some(50),
            1000,
        )
        .unwrap();
        claim(c, "R", Some(id), &[], TTL, 1001).unwrap();
        apply_event(c, "R", id, &Event::VerdictChanges, 1002).unwrap();
        id
    }

    #[test]
    fn remediation_agent_failed_parks_review_only_rework() {
        // D5b: a remediation worker lost at runtime must park the task, not
        // hand the unchanged PR head back to a fresh reviewer (whose changes
        // verdict would burn a rework round with zero remediation applied).
        let (_d, mut c) = open_tmp();
        let id = review_only_task_in_rework(&mut c);

        let r = apply_event(
            &mut c,
            "system",
            id,
            &Event::AgentFailed {
                reason: "worker killed by watchdog".into(),
            },
            1003,
        )
        .unwrap();

        assert_eq!(r.task.status, "failed");
        assert_eq!(
            r.task.rework_round, 1,
            "infra failure must not consume a rework round"
        );
        assert!(
            !r.effects.contains(&Effect::SpawnReviewer),
            "remediation death must not spawn a reviewer"
        );
        let refs: serde_json::Value =
            serde_json::from_str(r.task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[PARKED_REF], true);
        assert_eq!(refs[PARKED_RESUME_STATUS_REF], "rework");
        assert!(refs.get(PARKED_HEAD_CHECK_REF).is_none());
        assert!(
            refs.get(PARKED_REWORK_RETRY_REF).is_none(),
            "a genuine crash park must stay owner-gated — no auto-retry flag"
        );
        let active_claims: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target=?1 AND active=1",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_claims, 0, "park releases the remediation lease");
        // Failures are loud: owner alert delivered.
        let msgs = crate::feed::peek(&c, None, None, 10, 1003).unwrap();
        assert!(
            msgs.iter().any(|m| m.kind == "alert"
                && m.recipient.as_deref() == Some("owner")
                && m.body.contains("task-retry")),
            "owner alert with retry hint missing"
        );
    }

    #[test]
    fn remediation_lease_expired_parks_review_only_rework() {
        let (_d, mut c) = open_tmp();
        let id = review_only_task_in_rework(&mut c);

        let r = apply_event(&mut c, "system", id, &Event::LeaseExpired, 1003).unwrap();
        assert_eq!(r.task.status, "failed");
        assert_eq!(r.task.rework_round, 1);
        assert!(!r.effects.contains(&Effect::SpawnReviewer));
        let refs: serde_json::Value =
            serde_json::from_str(r.task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[PARKED_REF], true);
        assert_eq!(refs[PARKED_RESUME_STATUS_REF], "rework");
        assert!(refs.get(PARKED_HEAD_CHECK_REF).is_none());
    }

    #[test]
    fn daemon_caused_park_is_owner_gated_even_with_budget() {
        let (_d, mut c) = open_tmp();
        let id = review_only_task_in_rework(&mut c);

        let r = apply_event(
            &mut c,
            "system",
            id,
            &Event::AgentFailed {
                reason: "daemon draining".into(),
            },
            1003,
        )
        .unwrap();
        assert_eq!(r.task.status, "failed");
        let refs: serde_json::Value =
            serde_json::from_str(r.task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[PARKED_REF], true);
        assert!(refs.get(PARKED_REWORK_RETRY_REF).is_none());
        assert_eq!(
            r.task.recovery_attempts, 0,
            "owner-gated park must not spend an automatic retry budget"
        );

        let resumed = retry_parked(&mut c, id, "boss", true, 1004)
            .unwrap()
            .unwrap();
        assert_eq!(resumed.status, "rework");
        assert_eq!(
            resumed.recovery_attempts, 0,
            "owner retry refills the budget"
        );
    }

    #[test]
    fn daemon_caused_park_with_exhausted_budget_remains_owner_gated() {
        let (_d, mut c) = open_tmp();
        let id = review_only_task_in_rework(&mut c);
        c.execute(
            "UPDATE tasks SET recovery_attempts=?1 WHERE id=?2",
            params![MAX_RECOVERY_ATTEMPTS, id],
        )
        .unwrap();

        let r = apply_event(
            &mut c,
            "system",
            id,
            &Event::AgentFailed {
                reason: "daemon draining".into(),
            },
            1003,
        )
        .unwrap();
        assert_eq!(r.task.status, "failed", "still parks");
        let refs: serde_json::Value =
            serde_json::from_str(r.task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[PARKED_REF], true);
        assert!(
            refs.get(PARKED_REWORK_RETRY_REF).is_none(),
            "terminal park must never carry an auto-retry flag"
        );
        assert_eq!(
            r.task.recovery_attempts, MAX_RECOVERY_ATTEMPTS,
            "no budget spend without a flag"
        );
    }

    fn row_count(conn: &Connection, table: &str, task_id: i64) -> i64 {
        let sql = match table {
            "task_notes" => "SELECT COUNT(*) FROM task_notes WHERE task_id=?1",
            "agent_runs" => "SELECT COUNT(*) FROM agent_runs WHERE task_id=?1",
            "events" => "SELECT COUNT(*) FROM events WHERE subject=?1",
            other => panic!("unsupported test table {other}"),
        };
        if table == "events" {
            conn.query_row(sql, params![lease_target(task_id)], |row| row.get(0))
                .unwrap()
        } else {
            conn.query_row(sql, params![task_id], |row| row.get(0))
                .unwrap()
        }
    }

    #[test]
    fn terminal_retry_marker_reconciliation_converges_without_amplification() {
        let (dir, mut c) = open_tmp();
        let exact_261 = serde_json::json!({
            "cx_est": 3,
            "cx_size": "M",
            "cx_ready": true,
            "cx_not_ready_reason": null,
            "daemon_parked": true,
            "daemon_parked_reason": "remediation worker provisioning failed for PR #478",
            "daemon_resume_status": "rework",
            "daemon_rework_retry_requested": true,
            "remediation_feedback": "blocking feedback",
            "__quorum_noop": "retain",
            "pr": 478
        });
        let failed = create(
            &mut c,
            "boss",
            "legacy failed",
            None,
            0,
            None,
            Some(&exact_261.to_string()),
            None,
            None,
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed', refs=?2 WHERE id=?1",
            params![failed, exact_261.to_string()],
        )
        .unwrap();

        let terminal_ids =
            [("done", "legacy done"), ("cancelled", "legacy cancelled")].map(|(status, title)| {
                let id = create(
                    &mut c,
                    "boss",
                    title,
                    None,
                    0,
                    None,
                    Some(&exact_261.to_string()),
                    None,
                    None,
                    1000,
                )
                .unwrap();
                c.execute(
                    "UPDATE tasks SET status=?2, refs=?3 WHERE id=?1",
                    params![id, status, exact_261.to_string()],
                )
                .unwrap();
                (id, status)
            });

        let before_messages: i64 = c
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        let all_ids = [failed, terminal_ids[0].0, terminal_ids[1].0];
        let before_events = all_ids.map(|id| row_count(&c, "events", id));
        let first = reconcile_terminal_retry_markers(&mut c, 1001).unwrap();
        assert_eq!(first.len(), 3);
        assert!(reconcile_terminal_retry_markers(&mut c, 1002)
            .unwrap()
            .is_empty());

        // A restart (fresh SQLite connection) is also a no-op.
        drop(c);
        let mut c = crate::db::open(&dir.path().join("q.db")).unwrap();
        assert!(reconcile_terminal_retry_markers(&mut c, 1003)
            .unwrap()
            .is_empty());

        let failed_task = get(&c, failed).unwrap().unwrap();
        assert_eq!(failed_task.status, "failed");
        let failed_refs: serde_json::Value =
            serde_json::from_str(failed_task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(failed_refs[PARKED_REF], true);
        assert_eq!(failed_refs[PARKED_RESUME_STATUS_REF], "rework");
        assert_eq!(failed_refs[PARKED_REASON_REF], exact_261[PARKED_REASON_REF]);
        assert!(failed_refs.get(PARKED_REWORK_RETRY_REF).is_none());
        assert_eq!(failed_refs["pr"], 478);
        assert_eq!(failed_refs["remediation_feedback"], "blocking feedback");
        assert_eq!(
            failed_refs["__quorum_noop"], "retain",
            "reconciliation must preserve unrelated creator refs"
        );

        for (id, status) in terminal_ids {
            let task = get(&c, id).unwrap().unwrap();
            assert_eq!(task.status, status);
            let refs: serde_json::Value =
                serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
            assert!(refs.get(PARKED_REF).is_none());
            assert!(refs.get(PARKED_RESUME_STATUS_REF).is_none());
            assert!(refs.get(PARKED_REWORK_RETRY_REF).is_none());
            assert_eq!(refs[PARKED_REASON_REF], exact_261[PARKED_REASON_REF]);
        }

        for (index, id) in all_ids.into_iter().enumerate() {
            assert_eq!(row_count(&c, "task_notes", id), 1, "one cleanup note");
            assert_eq!(
                row_count(&c, "events", id),
                before_events[index],
                "no retry/park event"
            );
            assert_eq!(row_count(&c, "agent_runs", id), 0, "no worker run");
            let active_claims: i64 = c
                .query_row(
                    "SELECT COUNT(*) FROM claims WHERE target=?1 AND active=1",
                    params![lease_target(id)],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(active_claims, 0, "no remediation claim");
        }
        let after_messages: i64 = c
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            after_messages, before_messages,
            "no notifications or alerts"
        );
    }

    #[test]
    fn concurrent_terminal_retry_reconciliation_changes_each_row_once() {
        let (dir, mut conn) = open_tmp();
        let refs = serde_json::json!({
            "daemon_parked": true,
            "daemon_parked_reason": "remediation worker provisioning failed for PR #478",
            "daemon_resume_status": "rework",
            "daemon_rework_retry_requested": true,
            "remediation_feedback": "blocking feedback",
            "pr": 478
        });
        let id = create(
            &mut conn,
            "boss",
            "concurrent legacy retry",
            None,
            0,
            None,
            Some(&refs.to_string()),
            None,
            None,
            1000,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='failed', refs=?2 WHERE id=?1",
            params![id, refs.to_string()],
        )
        .unwrap();
        drop(conn);

        let db_path = dir.path().join("q.db");
        let contenders = 8;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(contenders));
        let handles: Vec<_> = (0..contenders)
            .map(|attempt| {
                let path = db_path.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    barrier.wait();
                    reconcile_terminal_retry_markers(&mut conn, 1100 + attempt as i64).unwrap()
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(
            outcomes.iter().filter(|ids| ids.as_slice() == [id]).count(),
            1,
            "exactly one serialized tick must consume the marker: {outcomes:?}"
        );
        assert_eq!(
            outcomes.iter().filter(|ids| ids.is_empty()).count(),
            contenders - 1
        );

        let conn = crate::db::open(&db_path).unwrap();
        let task = get(&conn, id).unwrap().unwrap();
        assert_eq!(task.status, "failed");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert!(refs.get(PARKED_REWORK_RETRY_REF).is_none());
        assert_eq!(refs[PARKED_REF], true);
        assert_eq!(refs[PARKED_RESUME_STATUS_REF], "rework");
        assert_eq!(row_count(&conn, "task_notes", id), 1);
    }

    #[test]
    fn terminal_race_after_retry_selection_is_a_clean_negative() {
        let (dir, mut selector) = open_tmp();
        let id = review_only_task_in_rework(&mut selector);
        selector
            .execute(
                "UPDATE tasks SET refs=json_set(refs,
                    '$.daemon_rework_retry_requested', json('true'),
                    '$.remediation_feedback', 'fix blocker',
                    '$.pr', 50)
                 WHERE id=?1",
                params![id],
            )
            .unwrap();

        // The daemon selected this rework row, then another lifecycle writer
        // made it terminal before the authoritative remediation claim.
        assert_eq!(
            list(&selector, Some("rework"), None, None).unwrap().len(),
            1
        );
        let terminal = crate::db::open(&dir.path().join("q.db")).unwrap();
        terminal
            .execute(
                "UPDATE tasks SET status='cancelled' WHERE id=?1",
                params![id],
            )
            .unwrap();

        for attempt in 0..32 {
            assert!(claim_remediation_rework(
                &mut selector,
                &format!("race-{attempt}"),
                id,
                TTL,
                1100 + attempt,
            )
            .unwrap()
            .is_none());
        }
        assert_eq!(get(&selector, id).unwrap().unwrap().status, "cancelled");
        let claimed_events: i64 = selector
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_claimed'",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claimed_events, 0);
        assert_eq!(row_count(&selector, "agent_runs", id), 0);
        let claims: i64 = selector
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target=?1",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claims, 0);
    }

    #[test]
    fn valid_owner_retried_remediation_claims_exactly_once_and_preserves_context() {
        let (_dir, mut c) = open_tmp();
        let id = review_only_task_in_rework(&mut c);
        c.execute(
            "UPDATE tasks SET recovery_attempts=2,
                 refs=json_set(refs, '$.pr', 50, '$.remediation_feedback', 'fix blocker')
             WHERE id=?1",
            params![id],
        )
        .unwrap();
        park(&mut c, id, "provisioning failed", "rework", 1003).unwrap();
        let retried = retry_parked(&mut c, id, "boss", true, 1004)
            .unwrap()
            .unwrap();
        assert_eq!(retried.status, "rework");
        assert_eq!(retried.recovery_attempts, 0);
        assert_eq!(retried.rework_round, 1);
        assert_eq!(extract_pr_number(&retried.refs), Some(50));
        let retried_refs: serde_json::Value =
            serde_json::from_str(retried.refs.as_deref().unwrap()).unwrap();
        assert_eq!(
            retried_refs["remediation_feedback"].as_str(),
            Some("fix blocker")
        );

        let claimed = claim_remediation_rework(&mut c, "replacement", id, TTL, 1005)
            .unwrap()
            .expect("valid retry must claim");
        assert_eq!(claimed.assignee.as_deref(), Some("replacement"));
        assert_eq!(claimed.author.as_deref(), None);
        assert_eq!(extract_pr_number(&claimed.refs), Some(50));
        assert!(has_live_lease(&c, id, 1005));
        assert!(claim_remediation_rework(&mut c, "loser", id, TTL, 1006)
            .unwrap()
            .is_none());
        let claimed_events: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_claimed'",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claimed_events, 1);
    }

    #[test]
    fn review_only_merge_failed_posts_alert_and_stays_in_review() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "review PR #50",
            None,
            100,
            None,
            None,
            None,
            Some(50),
            1000,
        )
        .unwrap();
        // Claim as reviewer, approve, then fail merge
        claim(&mut c, "R", Some(id), &[], TTL, 1001).unwrap();
        apply_event(&mut c, "R", id, &Event::VerdictApprove, 1002).unwrap();
        let r = apply_event(
            &mut c,
            "system",
            id,
            &Event::MergeFailed {
                reason: "PR #50 has conflicts with main".into(),
            },
            1003,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");
        assert!(r.task.review_only);
        assert!(r
            .effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));

        // Creator got an alert DM
        let msgs = crate::feed::peek(&c, None, None, 10, 1003).unwrap();
        let alert = msgs
            .iter()
            .find(|m| m.kind == "alert" && m.recipient.as_deref() == Some("owner"))
            .expect("alert DM to creator missing after merge failure");
        assert!(
            alert.body.contains("conflicts"),
            "alert should mention conflicts: {}",
            alert.body
        );

        // Reviewer column is still set (needed for merge retry)
        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.reviewer.as_deref(), Some("R"));
    }

    #[test]
    fn agent_failed_from_working_posts_alert_to_creator() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();

        let r = apply_event(
            &mut c,
            "A",
            id,
            &Event::AgentFailed {
                reason: "OOM killed".to_string(),
            },
            1001,
        )
        .unwrap();
        assert_eq!(r.task.status, "open");

        let msgs = crate::feed::peek(&c, None, None, 10, 1001).unwrap();
        let alert = msgs
            .iter()
            .find(|m| m.kind == "alert" && m.recipient.as_deref() == Some("owner"))
            .expect("alert message to creator missing");
        assert!(alert.body.contains("OOM killed"));
    }

    // ── metadata update ─────────────────────────────────────────────────────

    #[test]
    fn body_update_invalidates_classifier_refs_but_preserves_unrelated_refs() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let t = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                body: Some("new body"),
                refs: Some(r#"{"pr":42}"#),
                expected_revision: Some(1),
                ..Default::default()
            },
            1001,
        )
        .unwrap();
        assert_eq!(t.body.as_deref(), Some("new body"));
        let refs: serde_json::Value = serde_json::from_str(t.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["pr"], 42);
        assert!(refs.get("cx_est").is_none());
        assert!(refs.get("cx_size").is_none());
        assert!(refs.get("cx_ready").is_none());
        assert_eq!(t.status, "working");
    }

    #[test]
    fn creator_edits_unclaimed_task_body() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "t",
            Some("old"),
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let t = update(
            &mut c,
            "boss",
            id,
            &TaskUpdate {
                body: Some("revised spec"),
                expected_revision: Some(1),
                ..Default::default()
            },
            1001,
        )
        .unwrap();
        assert_eq!(t.body.as_deref(), Some("revised spec"));
        assert_eq!(t.status, "open");
        let notes = notes_for(&c, id).unwrap();
        assert_eq!(notes.len(), 1);
        assert!(notes[0].body.contains("body replaced by creator"));
    }

    #[test]
    fn non_creator_cannot_edit_unclaimed_task() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "t",
            Some("old"),
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let err = update(
            &mut c,
            "rando",
            id,
            &TaskUpdate {
                body: Some("hijack"),
                expected_revision: Some(1),
                ..Default::default()
            },
            1001,
        );
        assert!(matches!(err, Err(QuorumError::NotHolder)));
    }

    #[test]
    fn claimed_task_rejects_creator_body_edit() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "t",
            Some("old"),
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "worker", Some(id), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "boss",
            id,
            &TaskUpdate {
                body: Some("nope"),
                expected_revision: Some(1),
                ..Default::default()
            },
            1001,
        );
        assert!(matches!(err, Err(QuorumError::NotHolder)));
    }

    #[test]
    fn agent_failed_from_working_transitions_to_open() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W", Some(id), &[], TTL, 1000).unwrap();
        let r = apply_event(
            &mut c,
            "W",
            id,
            &Event::AgentFailed {
                reason: "worktree missing on recovery".into(),
            },
            1001,
        )
        .unwrap();
        assert_eq!(r.task.status, "open");
        assert!(r.task.assignee.is_none());
    }

    #[test]
    fn agent_failed_from_in_review_stays_in_review() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1001,
        )
        .unwrap();
        let r = apply_event(
            &mut c,
            "W",
            id,
            &Event::AgentFailed {
                reason: "worktree missing on recovery".into(),
            },
            1002,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");
        assert!(
            r.effects.iter().any(|e| matches!(e, Effect::SpawnReviewer)),
            "in-review AgentFailed must emit SpawnReviewer"
        );
    }

    #[test]
    fn reviewer_replacement_after_expiry() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        // Worker claims and signals done → in-review
        claim(&mut c, "W", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1001,
        )
        .unwrap();
        // R1 claims as reviewer
        claim(&mut c, "R1", Some(id), &[], TTL, 1002).unwrap();
        // R1's lease expires → sticky in-review, reviewer/assignee must be cleared
        let r = apply_event(&mut c, "system", id, &Event::LeaseExpired, 1003).unwrap();
        assert_eq!(r.task.status, "in-review");
        assert!(
            r.task.reviewer.is_none(),
            "reviewer must be cleared on sticky in-review release"
        );
        assert!(
            r.task.assignee.is_none(),
            "assignee must be cleared on sticky in-review release"
        );
        assert!(r.effects.contains(&Effect::SpawnReviewer));
        // R2 can now claim as the new reviewer
        let t = claim(&mut c, "R2", Some(id), &[], TTL, 1004)
            .unwrap()
            .unwrap();
        assert_eq!(t.reviewer, Some("R2".to_string()));
        assert_eq!(t.assignee, Some("R2".to_string()));
    }

    #[test]
    fn reviewer_replacement_after_agent_failed() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R1", Some(id), &[], TTL, 1002).unwrap();
        // R1 crashes → sticky in-review
        let r = apply_event(
            &mut c,
            "system",
            id,
            &Event::AgentFailed {
                reason: "crashed".to_string(),
            },
            1003,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");
        assert!(
            r.task.reviewer.is_none(),
            "reviewer must be cleared on agent failure"
        );
        assert!(
            r.task.assignee.is_none(),
            "assignee must be cleared on agent failure"
        );
        // R2 can claim
        let t = claim(&mut c, "R2", Some(id), &[], TTL, 1004)
            .unwrap()
            .unwrap();
        assert_eq!(t.reviewer, Some("R2".to_string()));
    }

    #[test]
    fn cancelled_event_emits_task_cancelled_row() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1001).unwrap();
        let r = apply_event(
            &mut c,
            "daemon",
            id,
            &Event::Cancelled {
                by: "daemon:provision-exhausted".to_string(),
            },
            1002,
        )
        .unwrap();
        assert_eq!(r.task.status, "cancelled");

        let event_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='task_cancelled' AND subject=?1",
                params![format!("task#{id}")],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 1, "expected exactly one task_cancelled event");
    }

    #[test]
    fn set_body_updates_task_body() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        set_body(&mut c, id, "daemon:parked:provision-exhausted", 1001).unwrap();
        let task: String = c
            .query_row("SELECT body FROM tasks WHERE id=?1", params![id], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(task, "daemon:parked:provision-exhausted");
    }

    #[test]
    fn extract_pr_number_from_refs() {
        assert_eq!(extract_pr_number(&Some(r#"{"pr":42}"#.into())), Some(42),);
        assert_eq!(extract_pr_number(&Some(r#"{"pr":"99"}"#.into())), Some(99),);
        assert_eq!(extract_pr_number(&None), None);
        assert_eq!(extract_pr_number(&Some(r#"{"branch":"foo"}"#.into())), None,);
    }

    #[test]
    fn extract_repo_from_refs() {
        assert_eq!(
            extract_repo(&Some(r#"{"pr":42,"repo":"ag2trust/quorum"}"#.into())),
            Some("ag2trust/quorum".to_string()),
        );
        assert_eq!(extract_repo(&Some(r#"{"pr":42}"#.into())), None);
        assert_eq!(extract_repo(&None), None);
        assert_eq!(extract_repo(&Some(r#"{"repo":123}"#.into())), None);
    }

    #[test]
    fn creator_label_validation_rejects_routing_authority() {
        for label in [
            "complexity:1",
            "complexity:5",
            "tier:luna",
            "tier:sol",
            "effort:medium",
            "effort:high",
        ] {
            let labels = format!(r#"["type:bug","{label}"]"#);
            let err = validate_creator_labels(Some(&labels)).unwrap_err();
            assert_eq!(err.exit_code(), 2, "{label}");
            assert!(
                matches!(&err, QuorumError::Usage(m) if m.contains("daemon-owned")),
                "task-create must reject {label}, got {err:?}"
            );
        }
    }

    #[test]
    fn validate_labels_accepts_non_routing_metadata() {
        assert!(validate_labels(r#"["type:bug","area:lifecycle","priority:high"]"#).is_ok());
        assert!(
            validate_creator_labels(Some(r#"["type:bug","area:lifecycle","priority:high"]"#))
                .is_ok()
        );
    }

    #[test]
    fn creator_refs_cannot_forge_classifier_authority() {
        for refs in [
            r#"{"cx_est":5}"#,
            r#"{"cx_by":"creator"}"#,
            r#"{"ticket":"ABC","cx_flags":[]}"#,
        ] {
            let err = validate_creator_refs(Some(refs)).unwrap_err();
            assert_eq!(err.exit_code(), 2);
            assert!(format!("{err}").contains("classifier-owned"));
        }
        let pr_err = validate_creator_refs(Some(r#"{"pr":42,"repo":"o/r"}"#)).unwrap_err();
        assert!(format!("{pr_err}").contains("--continue-pr"));
        let retry_err =
            validate_creator_refs(Some(r#"{"daemon_merge_retry":"requested"}"#)).unwrap_err();
        assert!(format!("{retry_err}").contains("task-retry"));
        assert!(validate_creator_refs(Some(r#"{"ticket":"ABC","repo":"o/r"}"#)).is_ok());
    }

    #[test]
    fn creator_refs_cannot_forge_runner_authority() {
        for refs in [
            r#"{"runner_retry":{"provider":"grok","model":"grok-4.5","effort":"high","prompt":"replace the daemon prompt","turn_kind":"initial","requested":true}}"#,
            r#"{"runner_continuation":{"provider":"grok","id":"session-forged"}}"#,
            r#"{"codex_retry_requested":true,"codex_retry_model":"gpt-5"}"#,
        ] {
            let error = validate_creator_refs(Some(refs)).unwrap_err();
            assert_eq!(error.exit_code(), 2, "{refs}: {error}");
            assert!(error.to_string().contains("runner-owned"), "{error}");
        }
    }

    #[test]
    fn metadata_update_preserves_daemon_pr_and_classifier_refs() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "classified",
            None,
            0,
            None,
            Some(r#"{"cx_est":5,"cx_by":"classifier:v1","pr":41}"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let fields = TaskUpdate {
            refs: Some(r#"{"pr":42,"ticket":"ABC"}"#),
            expected_revision: Some(1),
            ..Default::default()
        };
        let updated = update(&mut c, "boss", id, &fields, 1001).unwrap();
        let refs: serde_json::Value =
            serde_json::from_str(updated.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["pr"], 41);
        assert_eq!(refs["ticket"], "ABC");
        assert_eq!(refs["cx_est"], 5);
        assert_eq!(refs["cx_by"], "classifier:v1");
    }

    #[test]
    fn assignee_metadata_replacement_preserves_runner_state_but_daemon_can_replace_it() {
        let (_d, mut conn) = open_tmp();
        let id = create(
            &mut conn,
            "boss",
            "runner state",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let classifications = [crate::classify::TaskClassification {
            task_id: id,
            cx_est: 3,
            size: "M".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: Vec::new(),
        }];
        crate::classify::store_classifications(&mut conn, &classifications, "test:v2", 1001)
            .unwrap();
        claim(&mut conn, "worker", Some(id), &[], TTL, 1002).unwrap();
        update_refs_daemon(
            &mut conn,
            id,
            &serde_json::json!({
                "pr": 513,
                "old_metadata": "replace me",
                "runner_continuation": {"provider": "codex", "id": "thread-exact"},
                "runner_retry": {
                    "provider": "codex", "model": "gpt-5", "effort": "high",
                    "prompt": "resume exact turn", "turn_kind": "rework",
                    "continuation_id": "thread-exact", "requested": true
                },
                "runner_provider_block": {"provider": "codex", "reason": "quota"},
                "codex_thread_id": "thread-legacy",
                "codex_retry_requested": true
            })
            .to_string(),
            1003,
        )
        .unwrap();

        let updated = update(
            &mut conn,
            "worker",
            id,
            &TaskUpdate {
                refs: Some(r#"{"ticket":"ABC"}"#),
                expected_revision: Some(1),
                ..Default::default()
            },
            1004,
        )
        .unwrap();
        let refs: serde_json::Value =
            serde_json::from_str(updated.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["ticket"], "ABC");
        assert!(refs.get("old_metadata").is_none());
        assert_eq!(refs["runner_continuation"]["id"], "thread-exact");
        assert_eq!(refs["runner_retry"]["prompt"], "resume exact turn");
        assert_eq!(refs["runner_provider_block"]["reason"], "quota");
        assert_eq!(refs["codex_thread_id"], "thread-legacy");
        assert_eq!(refs["codex_retry_requested"], true);

        update_refs_daemon(
            &mut conn,
            id,
            r#"{"ticket":"daemon","runner_continuation":{"provider":"codex","id":"thread-new"}}"#,
            1005,
        )
        .unwrap();
        let task = get(&conn, id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["ticket"], "daemon");
        assert_eq!(refs["runner_continuation"]["id"], "thread-new");
        assert!(refs.get("runner_retry").is_none());
        assert!(refs.get("runner_provider_block").is_none());
        assert!(refs.get("codex_thread_id").is_none());
        assert!(refs.get("codex_retry_requested").is_none());
        assert_eq!(refs["pr"], 513);
        assert_eq!(refs["cx_by"], "test:v2");
    }

    // ── T6: lifecycle replay idempotency ──────────────────────────────────

    /// T6: replaying SignaledDone on a task already in in-review fails with
    /// InvalidTransition (Usage error), not corruption. This is the safety
    /// net for the mailbox consume-failure replay path.
    #[test]
    fn replay_signaled_done_is_harmless() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();

        // First apply: working → in-review.
        let r = apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1001,
        )
        .unwrap();
        assert_eq!(r.task.status, "in-review");

        // Replay: same event again (simulates consume failure + re-poll).
        let err = apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1002,
        );
        assert!(
            err.is_err(),
            "replay must be rejected, not silently applied"
        );

        // Task state is unchanged — no corruption.
        let task = get(&c, id).unwrap().unwrap();
        assert_eq!(task.status, "in-review");
    }

    /// T6: replaying VerdictApprove on a task already in merging fails
    /// harmlessly.
    #[test]
    fn replay_verdict_approve_is_harmless() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1001,
        )
        .unwrap();
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();

        // First: in-review → merging.
        let r = apply_event(&mut c, "R", id, &Event::VerdictApprove, 1003).unwrap();
        assert_eq!(r.task.status, "merging");

        // Replay: merging + VerdictApprove is invalid.
        let err = apply_event(&mut c, "R", id, &Event::VerdictApprove, 1004);
        assert!(err.is_err());

        let task = get(&c, id).unwrap().unwrap();
        assert_eq!(task.status, "merging");
    }

    /// T6: replaying a Done mailbox row after the task is cancelled fails
    /// harmlessly — the daemon's C3 path.
    #[test]
    fn replay_signaled_done_on_cancelled_task_is_harmless() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();

        apply_event(
            &mut c,
            "boss",
            id,
            &Event::Cancelled {
                by: "boss".to_string(),
            },
            1001,
        )
        .unwrap();

        // Worker tries SignaledDone on cancelled task (C3 replay scenario).
        let err = apply_event(
            &mut c,
            "A",
            id,
            &Event::SignaledDone {
                pr: "42".to_string(),
            },
            1002,
        );
        assert!(err.is_err());

        let task = get(&c, id).unwrap().unwrap();
        assert_eq!(task.status, "cancelled");
    }

    /// Rework re-claim by a different agent must preserve the original author
    /// and never overwrite it (#340). The branch (derived from author) stays
    /// stable across re-claims, preventing duplicate PRs.
    #[test]
    fn rework_reclaim_preserves_original_author() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();

        // Original author claims
        let t = claim(&mut c, "Optic-lo4x", Some(id), &[], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.author, Some("Optic-lo4x".to_string()));

        // Author signals done
        apply_event(
            &mut c,
            "Optic-lo4x",
            id,
            &Event::SignaledDone {
                pr: "3620".to_string(),
            },
            1001,
        )
        .unwrap();

        // Reviewer claims and sends back for rework
        claim(&mut c, "Optic-c6at", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "Optic-c6at", id, &Event::VerdictChanges, 1003).unwrap();

        // Author's lease lapsed — rework → open
        apply_event(&mut c, "system", id, &Event::LeaseExpired, 1004).unwrap();

        // Different agent claims the reopened task
        let t = claim(&mut c, "Lever-lx89", Some(id), &[], TTL, 1005)
            .unwrap()
            .unwrap();

        assert_eq!(
            t.author,
            Some("Optic-lo4x".to_string()),
            "author must be the original claimant, not the re-claimer"
        );
        assert_eq!(
            t.assignee,
            Some("Lever-lx89".to_string()),
            "assignee should be the new worker"
        );
    }

    /// Auto-select claim (task_id=None) also preserves author on rework re-claim (#340).
    #[test]
    fn rework_reclaim_auto_select_preserves_author() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();

        // Original author claims
        claim(&mut c, "Optic-lo4x", Some(id), &[], TTL, 1000).unwrap();

        // Author signals done → in-review
        apply_event(
            &mut c,
            "Optic-lo4x",
            id,
            &Event::SignaledDone {
                pr: "1".to_string(),
            },
            1001,
        )
        .unwrap();

        // Reviewer sends back for rework
        claim(&mut c, "R", Some(id), &[], TTL, 1002).unwrap();
        apply_event(&mut c, "R", id, &Event::VerdictChanges, 1003).unwrap();

        // Lease lapse → open
        apply_event(&mut c, "system", id, &Event::LeaseExpired, 1004).unwrap();

        // New agent auto-selects (no task_id)
        let t = claim(&mut c, "Lever-lx89", None, &[], TTL, 1005)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, id);
        assert_eq!(
            t.author,
            Some("Optic-lo4x".to_string()),
            "auto-select claim must also preserve original author"
        );
    }

    // #101: a working task whose refs carry a PR must NOT be direct-closed
    // when the done signal omits --pr. The daemon must extract the PR from
    // refs and route through the review lifecycle instead.
    #[test]
    fn done_without_pr_flag_must_not_close_task_with_refs_pr() {
        let (_dir, mut c) = open_tmp();

        // Create and claim a task with refs containing pr:343.
        let id = create(
            &mut c,
            "system",
            "review task with PR",
            None,
            0,
            None,
            Some(r#"{"pr":343}"#),
            None,
            None,
            1000,
        )
        .unwrap();

        claim(&mut c, "worker-1", Some(id), &[], TTL, 1001).unwrap();

        // Verify extract_pr_number finds the PR in refs.
        let task = get(&c, id).unwrap().unwrap();
        assert_eq!(
            extract_pr_number(&task.refs),
            Some(343),
            "refs.pr must be extractable for backfill"
        );

        // The lifecycle transition for SignaledDone must produce in-review,
        // not a terminal state — proving direct-close is wrong here.
        let view = crate::lifecycle::TaskView {
            status: crate::lifecycle::Status::Working,
            author: Some("worker-1".into()),
            reviewer: None,
            rework_round: 0,
            rework_cap: crate::lifecycle::REWORK_CAP,
            pr: Some("343".into()),
            review_only: false,
        };
        let (new_status, _effects) = crate::lifecycle::transition(
            &view,
            &crate::lifecycle::Event::SignaledDone { pr: "343".into() },
        )
        .unwrap();
        assert_eq!(
            new_status,
            crate::lifecycle::Status::InReview,
            "working task with PR must transition to in-review, not be direct-closed"
        );

        // Negative assertion: close_after_merge on a working task would
        // incorrectly skip the review lifecycle.
        assert_eq!(task.status, "working");
        assert!(
            extract_pr_number(&task.refs).is_some(),
            "daemon must check refs before direct-closing"
        );
    }

    #[test]
    fn get_with_notes_includes_agent_runs() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c, "boss", "run-test", None, 0, None, None, None, None, TTL,
        )
        .unwrap();

        // No runs yet — empty array, not error
        let detail = get_with_notes(&c, tid).unwrap().unwrap();
        assert!(detail.agent_runs.is_empty());

        // Insert worker + reviewer + R2
        crate::agent_runs::insert(&c, tid, "Alice", "worker", "opus-4", "high", "claude", 100)
            .unwrap();
        let rev_id = crate::agent_runs::insert(
            &c, tid, "Bob", "reviewer", "sonnet-5", "medium", "claude", 200,
        )
        .unwrap();
        crate::agent_runs::close(&c, rev_id, 300, "approved").unwrap();
        crate::agent_runs::insert_r2(&c, tid, "Carol", "opus-4", "high", "claude", 250).unwrap();

        let detail = get_with_notes(&c, tid).unwrap().unwrap();
        assert_eq!(detail.agent_runs.len(), 3);

        let w = &detail.agent_runs[0];
        assert_eq!(w.agent, "Alice");
        assert_eq!(w.role, "worker");
        assert_eq!(w.sub_role, None);
        assert_eq!(w.ended_at, None);

        let r = &detail.agent_runs[1];
        assert_eq!(r.agent, "Bob");
        assert_eq!(r.role, "reviewer");
        assert_eq!(r.end_reason.as_deref(), Some("approved"));
        assert_eq!(r.ended_at, Some(300));

        let r2 = &detail.agent_runs[2];
        assert_eq!(r2.agent, "Carol");
        assert_eq!(r2.role, "reviewer");
        assert_eq!(r2.sub_role.as_deref(), Some("r2"));
    }

    // ── Recovery budget tests ────────────────────────────────────────────

    #[test]
    fn recovery_budget_increments_on_agent_failed() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "crash-test",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        // Claim → working
        let t = claim(&mut c, "w1", None, &[], TTL, 200).unwrap().unwrap();
        assert_eq!(t.id, tid);
        assert_eq!(t.status, "working");

        // Worker dies → AgentFailed → open, recovery_attempts = 1
        let tr = apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "crashed".into(),
            },
            300,
        )
        .unwrap();
        assert_eq!(tr.task.status, "open");
        assert_eq!(tr.task.recovery_attempts, 1);
    }

    #[test]
    fn recovery_budget_increments_on_lease_expired() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "expire-test",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        claim(&mut c, "w1", None, &[], TTL, 200).unwrap();

        let tr = apply_event(&mut c, "daemon", tid, &Event::LeaseExpired, 300).unwrap();
        assert_eq!(tr.task.status, "open");
        assert_eq!(tr.task.recovery_attempts, 1);
    }

    #[test]
    fn recovery_budget_parks_on_exhaustion() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "exhaust-test",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        // Exhaust the budget: 3 crash cycles
        for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
            claim(&mut c, "w1", None, &[], TTL, attempt * 100).unwrap();
            let tr = apply_event(
                &mut c,
                "daemon",
                tid,
                &Event::AgentFailed {
                    reason: "crashed".into(),
                },
                attempt * 100 + 50,
            )
            .unwrap();
            assert_eq!(tr.task.status, "open", "attempt {attempt} should reopen");
            assert_eq!(tr.task.recovery_attempts, attempt);
        }

        // Attempt 4: claim again, crash → should park, not reopen
        claim(&mut c, "w1", None, &[], TTL, 500).unwrap();
        let tr = apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "crashed again".into(),
            },
            550,
        )
        .unwrap();
        assert_eq!(tr.task.status, "failed");
        assert!(
            tr.effects
                .iter()
                .any(|e| matches!(e, Effect::NotifyOwner { reason } if reason.contains("recovery budget exhausted"))),
            "must notify owner of budget exhaustion"
        );
    }

    #[test]
    fn recovery_budget_resets_on_signaled_done() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "reset-test",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        // Crash once → recovery_attempts = 1
        claim(&mut c, "w1", None, &[], TTL, 200).unwrap();
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "crash".into(),
            },
            300,
        )
        .unwrap();

        // Re-claim and succeed → SignaledDone → in-review, counter resets
        claim(&mut c, "w2", None, &[], TTL, 400).unwrap();
        let tr = apply_event(
            &mut c,
            "w2",
            tid,
            &Event::SignaledDone { pr: "42".into() },
            500,
        )
        .unwrap();
        assert_eq!(tr.task.status, "in-review");
        assert_eq!(tr.task.recovery_attempts, 0);
    }

    #[test]
    fn recovery_budget_resets_on_rework_pushed() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "rework-reset",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        // claim → submit → review → rework
        claim(&mut c, "w1", Some(tid), &[], TTL, 200).unwrap();
        apply_event(
            &mut c,
            "w1",
            tid,
            &Event::SignaledDone { pr: "10".into() },
            300,
        )
        .unwrap();
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::ReviewerAttached { agent: "r1".into() },
            400,
        )
        .unwrap();
        apply_event(&mut c, "r1", tid, &Event::VerdictChanges, 500).unwrap();

        // Install remediation lease (as the daemon would).
        claim_remediation_rework(&mut c, "w1", tid, TTL, 510).unwrap();

        // Rework crash → open, recovery_attempts = 1
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "rework crash".into(),
            },
            600,
        )
        .unwrap();
        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(t.recovery_attempts, 1);

        // Re-claim (w1 is still author since PR exists) → working → submit → in-review
        claim(&mut c, "w1", Some(tid), &[], TTL, 700).unwrap();
        apply_event(
            &mut c,
            "w1",
            tid,
            &Event::SignaledDone { pr: "10".into() },
            800,
        )
        .unwrap();
        // SignaledDone resets recovery_attempts
        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(t.recovery_attempts, 0, "SignaledDone resets counter");

        // New rework cycle to test ReworkPushed specifically:
        // Bump recovery_attempts via raw SQL to simulate prior crashes
        c.execute(
            "UPDATE tasks SET recovery_attempts = 2 WHERE id = ?1",
            params![tid],
        )
        .unwrap();

        // VerdictChanges → rework (does NOT touch recovery_attempts)
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::ReviewerAttached { agent: "r2".into() },
            900,
        )
        .unwrap();
        apply_event(&mut c, "r2", tid, &Event::VerdictChanges, 1000).unwrap();
        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(
            t.recovery_attempts, 2,
            "VerdictChanges must not touch counter"
        );

        // Install remediation lease (as the daemon would after VerdictChanges).
        claim_remediation_rework(&mut c, "w1", tid, TTL, 1010).unwrap();

        // ReworkPushed → in-review (resets counter)
        let tr = apply_event(&mut c, "w1", tid, &Event::ReworkPushed, 1100).unwrap();
        assert_eq!(tr.task.status, "in-review");
        assert_eq!(
            tr.task.recovery_attempts, 0,
            "ReworkPushed should reset recovery_attempts"
        );
    }

    #[test]
    fn rework_via_verdict_does_not_increment_recovery() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "rework-no-inc",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        claim(&mut c, "w1", None, &[], TTL, 200).unwrap();
        apply_event(
            &mut c,
            "w1",
            tid,
            &Event::SignaledDone { pr: "99".into() },
            300,
        )
        .unwrap();
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::ReviewerAttached { agent: "r1".into() },
            400,
        )
        .unwrap();

        // VerdictChanges → rework: must NOT touch recovery_attempts
        let tr = apply_event(&mut c, "r1", tid, &Event::VerdictChanges, 500).unwrap();
        assert_eq!(tr.task.status, "rework");
        assert_eq!(tr.task.recovery_attempts, 0);
    }

    #[test]
    fn recovery_budget_survives_daemon_restart_pattern() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "restart-test",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        // Crash twice via "daemon restart recovery" pattern (same as recovery.rs)
        for attempt in 1..=2 {
            claim(&mut c, "w1", None, &[], TTL, attempt * 100).unwrap();
            apply_event(
                &mut c,
                "daemon",
                tid,
                &Event::AgentFailed {
                    reason: "daemon restart recovery (working task)".to_string(),
                },
                attempt * 100 + 50,
            )
            .unwrap();
        }

        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(t.recovery_attempts, 2);

        // Third crash
        claim(&mut c, "w1", None, &[], TTL, 400).unwrap();
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "daemon restart recovery (working task)".into(),
            },
            450,
        )
        .unwrap();

        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(t.recovery_attempts, 3);

        // Fourth crash → parked
        claim(&mut c, "w1", None, &[], TTL, 500).unwrap();
        let tr = apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "daemon restart recovery (working task)".into(),
            },
            550,
        )
        .unwrap();
        assert_eq!(tr.task.status, "failed");
    }

    #[test]
    fn lifecycle_rejection_consumes_recovery_budget() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "reject-test",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        // Simulate the daemon's C3 path: worker submits, lifecycle rejects,
        // daemon fires AgentFailed with the rejection reason.
        for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
            claim(&mut c, "w1", None, &[], TTL, attempt * 100).unwrap();
            let tr = apply_event(
                &mut c,
                "daemon",
                tid,
                &Event::AgentFailed {
                    reason: "lifecycle transition rejected at done signal: not-holder".into(),
                },
                attempt * 100 + 50,
            )
            .unwrap();
            assert_eq!(tr.task.status, "open", "attempt {attempt} should reopen");
            assert_eq!(tr.task.recovery_attempts, attempt);
        }

        // Attempt 4: budget exhausted → parked, not reopened
        claim(&mut c, "w1", None, &[], TTL, 500).unwrap();
        let tr = apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "lifecycle transition rejected at done signal: not-holder".into(),
            },
            550,
        )
        .unwrap();
        assert_eq!(tr.task.status, "failed");

        // Verify the park includes the failure cause
        let has_cause = tr.effects.iter().any(|e| {
            matches!(e, Effect::NotifyOwner { reason }
                if reason.contains("recovery budget exhausted")
                    && reason.contains("not-holder"))
        });
        assert!(has_cause, "park must include the rejection cause");
    }

    #[test]
    fn no_fourth_spawn_after_budget_exhaustion() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "no-respawn",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        // Exhaust the budget with deterministic lifecycle rejections
        for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
            claim(&mut c, "w1", None, &[], TTL, attempt * 100).unwrap();
            apply_event(
                &mut c,
                "daemon",
                tid,
                &Event::AgentFailed {
                    reason: "lifecycle transition rejected at done signal: task is cancelled"
                        .into(),
                },
                attempt * 100 + 50,
            )
            .unwrap();
        }

        // Fourth crash → parked
        claim(&mut c, "w1", None, &[], TTL, 500).unwrap();
        let tr = apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "lifecycle transition rejected at done signal: task is cancelled".into(),
            },
            550,
        )
        .unwrap();
        assert_eq!(tr.task.status, "failed");

        // Task is now terminal — claim must fail (no 4th spawn possible)
        let claimed = claim(&mut c, "w2", None, &[], TTL, 600).unwrap();
        assert!(claimed.is_none(), "must not claim a parked task");
    }

    #[test]
    fn exhaustion_message_carries_last_failure_reason() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "cause-test",
            None,
            5,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();

        // Burn the budget with mixed reasons — the last one is what matters
        claim(&mut c, "w1", None, &[], TTL, 200).unwrap();
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "worker process died".into(),
            },
            250,
        )
        .unwrap();

        claim(&mut c, "w1", None, &[], TTL, 300).unwrap();
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "worker process died".into(),
            },
            350,
        )
        .unwrap();

        claim(&mut c, "w1", None, &[], TTL, 400).unwrap();
        apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "worker process died".into(),
            },
            450,
        )
        .unwrap();

        // Fourth attempt — the reason in this event is the one that shows up
        claim(&mut c, "w1", None, &[], TTL, 500).unwrap();
        let tr = apply_event(
            &mut c,
            "daemon",
            tid,
            &Event::AgentFailed {
                reason: "lifecycle transition rejected at done signal: not-holder".into(),
            },
            550,
        )
        .unwrap();
        assert_eq!(tr.task.status, "failed");

        let reason = tr
            .effects
            .iter()
            .find_map(|e| match e {
                Effect::NotifyOwner { reason } => Some(reason.as_str()),
                _ => None,
            })
            .expect("must have NotifyOwner effect");
        assert!(
            reason
                .contains("last failure: lifecycle transition rejected at done signal: not-holder"),
            "exhaustion message must carry the triggering failure reason, got: {reason}"
        );
    }

    #[test]
    fn parked_task_retry_preserves_pr_and_dependency_context() {
        let (_dir, mut conn) = open_tmp();
        let dep = create(
            &mut conn, "owner", "dep", None, 0, None, None, None, None, 10,
        )
        .unwrap();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            Some("original task context"),
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            Some(419),
            11,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks
             SET status='in-review', refs='{\"pr\":419,\"branch\":\"feature/x\"}',
                 author='worker', reviewer='reviewer', rework_round=2
             WHERE id=?1",
            params![task_id],
        )
        .unwrap();

        let parked = park(
            &mut conn,
            task_id,
            "reviewer repo mismatch",
            "in-review",
            12,
        )
        .unwrap()
        .unwrap();
        assert_eq!(parked.status, "failed");
        assert_eq!(parked.body.as_deref(), Some("original task context"));
        let expected_deps = format!("[{dep}]");
        assert_eq!(parked.depends_on.as_deref(), Some(expected_deps.as_str()));
        let refs: serde_json::Value =
            serde_json::from_str(parked.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["pr"], 419);
        assert_eq!(refs["branch"], "feature/x");
        assert_eq!(refs["daemon_parked_reason"], "reviewer repo mismatch");

        let retried = retry_parked(&mut conn, task_id, "operator", true, 13)
            .unwrap()
            .unwrap();
        assert_eq!(retried.status, "in-review");
        assert_eq!(retried.body.as_deref(), Some("original task context"));
        assert_eq!(retried.author.as_deref(), Some("worker"));
        assert_eq!(retried.reviewer.as_deref(), Some("reviewer"));
        assert_eq!(retried.rework_round, 2);
        assert_eq!(retried.depends_on.as_deref(), Some(expected_deps.as_str()));
        let refs: serde_json::Value =
            serde_json::from_str(retried.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["pr"], 419);
        assert!(refs.get("daemon_parked").is_none());
    }

    #[test]
    fn daemon_owned_push_rejection_park_alerts_owner() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn, "owner", "task", None, 0, None, None, None, None, 10,
        )
        .unwrap();
        let reason = "daemon-owned push rejected: worker signaled unbound PR #10; daemon creates initial PRs";

        park(&mut conn, task_id, reason, "open", 11)
            .unwrap()
            .expect("active task must park");

        let alert: (String, String, String, String, i64, String) = conn
            .query_row(
                "SELECT author, kind, body, refs, expires_at, recipient
                 FROM messages WHERE refs=?1",
                params![format!("task:{task_id}")],
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
            .expect("push-rejection park must alert the owner");
        assert_eq!(alert.0, "daemon");
        assert_eq!(alert.1, "alert");
        assert!(alert.2.contains(reason));
        assert!(alert.2.contains("quorum task-retry"));
        assert_eq!(alert.3, format!("task:{task_id}"));
        assert_eq!(alert.4, 11 + crate::feed::DEFAULT_MESSAGE_TTL_SECS);
        assert_eq!(alert.5, "owner");
    }

    #[test]
    fn daemon_push_rejection_park_blocks_parent_graph() {
        let (_dir, mut conn) = open_tmp();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('parent','open','owner',1,1)",
            [],
        )
        .unwrap();
        let graph = crate::decomposition::begin_planning(
            &mut conn,
            &crate::decomposition::BeginPlanning {
                source_task_id: 1,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        assert!(crate::decomposition::set_frozen_phase(
            &mut conn,
            graph,
            "freeze-requested",
            "preclassifying",
            None,
            2
        )
        .unwrap());
        let child = |key: &str| {
            crate::decomposition::PlannedChild {
            local_key: key.into(),
            title: key.into(),
            body: format!("deliver {key}"),
            labels: None,
            classification_refs: r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#.into(),
            prerequisite_keys: vec![],
            source_dependency_ids: vec![],
        }
        };
        let ids = crate::decomposition::materialize_graph(
            &mut conn,
            graph,
            1,
            &[child("a"), child("b")],
            4,
        )
        .unwrap()
        .unwrap();
        let reason = "daemon-owned push rejected: non-fast-forward";

        park(&mut conn, ids[0], reason, "open", 10)
            .unwrap()
            .expect("child must park");

        let (state, hold_code, hold_summary): (String, String, String) = conn
            .query_row(
                "SELECT state,hold_code,hold_summary
                 FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, "blocked");
        assert_eq!(hold_code, "generated-child-failed");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&hold_summary).unwrap(),
            serde_json::json!({"affected_task": ids[0], "reason": reason})
        );

        let event_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='task_graph_blocked'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 1);

        let graph_alert: String = conn
            .query_row(
                "SELECT body FROM messages
                 WHERE kind='alert' AND recipient='owner'
                   AND body LIKE '%task graph blocked%'",
                [],
                |row| row.get(0),
            )
            .expect("graph-blocked alert must exist");
        assert!(graph_alert.contains("task graph blocked"));
        assert!(graph_alert.contains(reason));

        assert!(
            !crate::decomposition_review::is_reviewable_graph_member(&conn, ids[1]).unwrap(),
            "a blocked graph must withhold sibling reviewer authority"
        );
        let retried = retry_parked(&mut conn, ids[0], "operator", true, 11)
            .unwrap()
            .expect("parked child must resume");
        assert_eq!(retried.status, "open");
        let graph_state: (String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT state,hold_code,hold_summary FROM task_decompositions WHERE id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(graph_state, ("active".into(), None, None));
        let unblocked: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE kind='task_graph_unblocked' AND subject=?1",
                [lease_target(ids[0])],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unblocked, 1);
        assert!(
            crate::decomposition_review::is_reviewable_graph_member(&conn, ids[1]).unwrap(),
            "reactivating the graph must restore sibling reviewer authority"
        );
    }

    fn blocked_graph_with_parked_children(
        conn: &mut Connection,
        hold_code: &str,
        hold_summary: &str,
        child_refs: &str,
    ) -> (i64, i64, i64) {
        let source = create(conn, "owner", "source", None, 0, None, None, None, None, 1).unwrap();
        let affected = create(
            conn, "owner", "affected", None, 0, None, None, None, None, 1,
        )
        .unwrap();
        let retried = create(conn, "owner", "retried", None, 0, None, None, None, None, 1).unwrap();
        conn.execute("UPDATE tasks SET status='decomposed' WHERE id=?1", [source])
            .unwrap();
        conn.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,
                 plan_revision,accepted_plan_revision,hold_code,hold_summary,created_at,updated_at)
             VALUES (?1,'blocked',1,0,1,1,1,?2,?3,1,1)",
            params![source, hold_code, hold_summary],
        )
        .unwrap();
        let graph = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
             VALUES (?1,?2,'affected',1,1),(?1,?3,'retried',1,1)",
            params![graph, affected, retried],
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='failed',refs=?2 WHERE id=?1",
            params![retried, child_refs],
        )
        .unwrap();
        (graph, affected, retried)
    }

    #[test]
    fn retry_parked_leaves_unrelated_or_legacy_graph_holds_blocked() {
        let parked = r#"{"daemon_parked":true,"daemon_resume_status":"open"}"#;
        for (name, hold_code, affected_task, policy_parked, legacy_summary) in [
            (
                "different child",
                "generated-child-failed",
                true,
                false,
                false,
            ),
            (
                "reviewer blocker",
                "boundary-violation",
                false,
                false,
                false,
            ),
            ("policy park", "generated-child-failed", false, true, false),
            (
                "legacy summary",
                "generated-child-failed",
                false,
                false,
                true,
            ),
        ] {
            let (_dir, mut conn) = open_tmp();
            let provisional_summary = serde_json::json!({"affected_task": 0, "reason": "failed"});
            let (graph, affected, retried) = blocked_graph_with_parked_children(
                &mut conn,
                hold_code,
                &provisional_summary.to_string(),
                if policy_parked {
                    r#"{"daemon_parked":true,"daemon_resume_status":"open","classifier_policy_parked":true}"#
                } else {
                    parked
                },
            );
            let summary = if legacy_summary {
                "generated child task #old failed: failed".to_owned()
            } else {
                serde_json::json!({
                    "affected_task": if affected_task { affected } else { retried },
                    "reason": "failed",
                })
                .to_string()
            };
            conn.execute(
                "UPDATE task_decompositions SET hold_summary=?2 WHERE id=?1",
                params![graph, summary],
            )
            .unwrap();

            let result = retry_parked(&mut conn, retried, "operator", true, 2)
                .unwrap()
                .expect("parked child retry must succeed");
            assert_eq!(
                result.status,
                if policy_parked { "failed" } else { "open" },
                "{name}"
            );
            let state: (String, String, String) = conn
                .query_row(
                    "SELECT state,hold_code,hold_summary FROM task_decompositions WHERE id=?1",
                    [graph],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(state.0, "blocked", "{name}");
            assert_eq!(state.1, hold_code, "{name}");
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM events WHERE kind='task_graph_unblocked'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                0,
                "{name} must not reactivate the graph"
            );
        }
    }

    #[test]
    fn daemon_push_rejection_park_skips_graph_block_with_retry_marker() {
        let (_dir, mut conn) = open_tmp();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('parent','open','owner',1,1)",
            [],
        )
        .unwrap();
        let graph = crate::decomposition::begin_planning(
            &mut conn,
            &crate::decomposition::BeginPlanning {
                source_task_id: 1,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        assert!(crate::decomposition::set_frozen_phase(
            &mut conn,
            graph,
            "freeze-requested",
            "preclassifying",
            None,
            2
        )
        .unwrap());
        let child = |key: &str| {
            crate::decomposition::PlannedChild {
            local_key: key.into(),
            title: key.into(),
            body: format!("deliver {key}"),
            labels: None,
            classification_refs: r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#.into(),
            prerequisite_keys: vec![],
            source_dependency_ids: vec![],
        }
        };
        let ids = crate::decomposition::materialize_graph(
            &mut conn,
            graph,
            1,
            &[child("a"), child("b")],
            4,
        )
        .unwrap()
        .unwrap();
        conn.execute(
            "UPDATE tasks SET refs=?2 WHERE id=?1",
            rusqlite::params![ids[0], r#"{"runner_retry":{"requested":true}}"#,],
        )
        .unwrap();
        let reason = "daemon-owned push rejected: non-fast-forward";

        park(&mut conn, ids[0], reason, "open", 10)
            .unwrap()
            .expect("child must park");

        let state: String = conn
            .query_row(
                "SELECT state FROM task_decompositions WHERE id=?1",
                [graph],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            state, "active",
            "graph stays active when retry marker present"
        );
    }

    #[test]
    fn parked_rework_retry_becomes_claimable_by_replacement_worker() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            Some("original task context"),
            0,
            None,
            None,
            None,
            Some(419),
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks
             SET status='rework', assignee='dead-worker', author='original-worker',
                 rework_round=2
             WHERE id=?1",
            params![task_id],
        )
        .unwrap();
        park(
            &mut conn,
            task_id,
            "recovery budget exhausted",
            "rework",
            11,
        )
        .unwrap()
        .unwrap();

        let retried = retry_parked(&mut conn, task_id, "operator", true, 12)
            .unwrap()
            .unwrap();
        assert_eq!(retried.status, "rework");
        let refs: serde_json::Value =
            serde_json::from_str(retried.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[PARKED_REWORK_RETRY_REF], true);

        let claimed = claim_provider_retry_rework(&mut conn, "replacement", task_id, TTL, 13)
            .unwrap()
            .expect("explicitly retried rework must be claimable");
        assert_eq!(claimed.status, "rework");
        assert_eq!(claimed.assignee.as_deref(), Some("replacement"));
        assert_eq!(claimed.author.as_deref(), Some("original-worker"));
        assert_eq!(claimed.rework_round, 2);
        let active_claims: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM claims
                 WHERE target=?1 AND holder='replacement' AND active=1 AND expires_at>13",
                params![lease_target(task_id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_claims, 1);
    }

    #[test]
    fn provider_retry_rework_claim_waits_for_dependencies() {
        let (_dir, mut conn) = open_tmp();
        let dependency = create(
            &mut conn,
            "owner",
            "dependency",
            None,
            0,
            None,
            None,
            None,
            None,
            10,
        )
        .unwrap();
        let dependencies = format!("[{dependency}]");
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            Some(r#"{"codex_retry_requested":true}"#),
            Some(&dependencies),
            None,
            11,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='rework' WHERE id=?1",
            params![task_id],
        )
        .unwrap();

        assert!(
            claim_provider_retry_rework(&mut conn, "replacement", task_id, TTL, 12)
                .unwrap()
                .is_none()
        );
        assert_eq!(get(&conn, task_id).unwrap().unwrap().assignee, None);

        conn.execute(
            "UPDATE tasks SET status='done' WHERE id=?1",
            params![dependency],
        )
        .unwrap();
        let claimed = claim_provider_retry_rework(&mut conn, "replacement", task_id, TTL, 13)
            .unwrap()
            .expect("done dependency must allow provider retry rework claim");
        assert_eq!(claimed.id, task_id);
    }

    #[test]
    fn dependency_ready_rework_listing_defers_retained_remediation_retry() {
        let (_dir, mut conn) = open_tmp();
        let dependency = create(
            &mut conn,
            "owner",
            "dependency",
            None,
            0,
            None,
            None,
            None,
            None,
            10,
        )
        .unwrap();
        let dependencies = format!("[{dependency}]");
        let task_id = create(
            &mut conn,
            "owner",
            "retained remediation retry",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null}"#),
            Some(&dependencies),
            None,
            11,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='rework' WHERE id=?1",
            params![task_id],
        )
        .unwrap();
        assert!(
            retain_blocked_remediation_retry(&mut conn, task_id, "fix the blocker", 12).unwrap()
        );

        assert!(
            list_dependency_ready_rework(&conn)
                .unwrap()
                .into_iter()
                .all(|task| task.id != task_id),
            "the retry scan must not select a dependency-blocked task"
        );
        let blocked = get(&conn, task_id).unwrap().unwrap();
        let blocked_refs: serde_json::Value =
            serde_json::from_str(blocked.refs.as_deref().unwrap()).unwrap();
        assert_eq!(blocked_refs[PARKED_REWORK_RETRY_REF], true);
        assert_eq!(blocked_refs["remediation_feedback"], "fix the blocker");
        let claims: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target=?1",
                params![lease_target(task_id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claims, 0, "the scan must not attempt a remediation claim");

        conn.execute(
            "UPDATE tasks SET status='done' WHERE id=?1",
            params![dependency],
        )
        .unwrap();
        let selected = list_dependency_ready_rework(&conn).unwrap();
        assert!(selected.iter().any(|task| task.id == task_id));
        assert!(claim_remediation_rework_with_feedback(
            &mut conn,
            "replacement",
            task_id,
            TTL,
            13,
            Some("fix the blocker"),
        )
        .unwrap()
        .is_some());
    }

    #[test]
    fn provider_retry_with_malformed_refs_is_a_clean_negative() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn, "owner", "task", None, 0, None, None, None, None, 10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='rework', refs='{' WHERE id=?1",
            params![task_id],
        )
        .unwrap();

        assert!(
            claim_provider_retry_rework(&mut conn, "replacement", task_id, TTL, 11)
                .unwrap()
                .is_none()
        );
        assert_eq!(get(&conn, task_id).unwrap().unwrap().assignee, None);
    }

    #[test]
    fn neutral_retry_marker_precedes_stale_codex_request_during_claim() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            Some(
                r#"{"runner_retry":{"provider":"codex","model":"gpt-5","effort":"high","prompt":"finish","turn_kind":"rework","continuation_id":"thread-new","requested":false},"codex_retry_requested":true}"#,
            ),
            None,
            None,
            10,
        )
        .unwrap();
        crate::classify::store_classifications(
            &mut conn,
            &[crate::classify::TaskClassification {
                task_id,
                cx_est: 3,
                size: "M".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: vec![],
            }],
            "unit-test:v2",
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='rework' WHERE id=?1",
            params![task_id],
        )
        .unwrap();

        assert!(
            claim_provider_retry_rework(&mut conn, "replacement", task_id, TTL, 11)
                .unwrap()
                .is_none()
        );
        assert_eq!(get(&conn, task_id).unwrap().unwrap().assignee, None);
    }

    #[test]
    fn parked_merging_retry_records_one_shot_merge_intent_with_pr_context() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            None,
            None,
            Some(419),
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks
             SET status='merging', author='worker', reviewer='reviewer'
             WHERE id=?1",
            params![task_id],
        )
        .unwrap();
        park(&mut conn, task_id, "merge policy blocked", "merging", 11)
            .unwrap()
            .unwrap();

        let retried = retry_parked(&mut conn, task_id, "operator", true, 12)
            .unwrap()
            .unwrap();
        assert_eq!(retried.status, "merging");
        assert_eq!(extract_pr_number(&retried.refs), Some(419));
        assert_eq!(retried.author.as_deref(), Some("worker"));
        assert_eq!(retried.reviewer.as_deref(), Some("reviewer"));
        let refs: serde_json::Value =
            serde_json::from_str(retried.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[MERGE_RETRY_REF], MERGE_RETRY_REQUESTED);

        let claimed = claim_merge_retry(&mut conn, 13).unwrap().unwrap();
        let refs: serde_json::Value =
            serde_json::from_str(claimed.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[MERGE_RETRY_REF], MERGE_RETRY_ATTEMPTING);
        assert!(claim_merge_retry(&mut conn, 14).unwrap().is_none());
    }

    #[test]
    fn approved_merge_attempt_is_durable_and_cannot_be_admitted_twice() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            Some(r#"{"pr":419}"#),
            None,
            None,
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='merging', author='worker', reviewer='reviewer' WHERE id=?1",
            [task_id],
        )
        .unwrap();

        assert!(begin_approved_merge_attempt(&mut conn, task_id, 11).unwrap());
        assert!(!begin_approved_merge_attempt(&mut conn, task_id, 12).unwrap());
        let task = get(&conn, task_id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[MERGE_RETRY_REF], MERGE_RETRY_ATTEMPTING);
        let starts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='merge_attempt_started' AND subject=?1",
                [lease_target(task_id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(starts, 1);
    }

    #[test]
    fn merge_retry_success_atomically_completes_and_consumes_approvals() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            Some(r#"{"pr":419,"daemon_merge_retry":"attempting"}"#),
            None,
            None,
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='merging',author='worker',reviewer='r2' WHERE id=?1",
            [task_id],
        )
        .unwrap();
        for role in ["r1", "r2"] {
            crate::approvals::record(
                &mut conn,
                &crate::approvals::Approval {
                    pr_number: 419,
                    review_role: role.into(),
                    task_id,
                    author: "worker".into(),
                    reviewer: role.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "head".into(),
                },
            )
            .unwrap();
        }

        let completed = complete_approved_merge(&mut conn, task_id, 419, 11).unwrap();
        assert_eq!(completed.task.status, "done");
        assert!(crate::approvals::get_for_pr(&conn, 419).unwrap().is_empty());
        let refs: serde_json::Value =
            serde_json::from_str(completed.task.refs.as_deref().unwrap()).unwrap();
        assert!(refs.get(MERGE_RETRY_REF).is_none());
    }

    #[test]
    fn merge_retry_invalidation_atomically_deletes_stale_role_and_sampling() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            Some(r#"{"pr":419,"daemon_merge_retry":"attempting"}"#),
            None,
            None,
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='merging',author='worker',reviewer='r2' WHERE id=?1",
            [task_id],
        )
        .unwrap();
        crate::review_audits::record_r2_requirement(&mut conn, task_id, 419, "head", true).unwrap();
        for role in ["r1", "r2"] {
            crate::approvals::record(
                &mut conn,
                &crate::approvals::Approval {
                    pr_number: 419,
                    review_role: role.into(),
                    task_id,
                    author: "worker".into(),
                    reviewer: role.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "head".into(),
                },
            )
            .unwrap();
        }

        let invalidated = invalidate_merge_retry(
            &mut conn,
            task_id,
            419,
            StaleMergeRetryEvidence {
                roles: &["r2"],
                sampling_head: Some("head"),
            },
            "R2 stale",
            11,
        )
        .unwrap();
        assert_eq!(invalidated.task.status, "in-review");
        assert!(crate::approvals::get(&conn, 419, "r1").unwrap().is_some());
        assert!(crate::approvals::get(&conn, 419, "r2").unwrap().is_none());
        assert_eq!(
            crate::review_audits::r2_requirement(&conn, task_id, 419, "head").unwrap(),
            None
        );
        let refs: serde_json::Value =
            serde_json::from_str(invalidated.task.refs.as_deref().unwrap()).unwrap();
        assert!(refs.get(MERGE_RETRY_REF).is_none());
    }

    #[test]
    fn worker_fixable_merge_retry_atomically_enters_actionable_rework() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            Some(r#"{"pr":419,"daemon_merge_retry":"attempting"}"#),
            None,
            None,
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='merging',author='worker',reviewer='r2' WHERE id=?1",
            [task_id],
        )
        .unwrap();
        for role in ["r1", "r2"] {
            crate::approvals::record(
                &mut conn,
                &crate::approvals::Approval {
                    pr_number: 419,
                    review_role: role.into(),
                    task_id,
                    author: "worker".into(),
                    reviewer: role.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "head".into(),
                },
            )
            .unwrap();
        }

        let feedback = "merge conflict\n\nMerge main into the PR branch.";
        let transition = rework_approved_merge(&mut conn, task_id, 419, feedback, 11).unwrap();
        assert_eq!(transition.task.status, "rework");
        assert_eq!(transition.task.rework_round, 1);
        assert_eq!(transition.task.assignee.as_deref(), Some("worker"));
        assert!(crate::approvals::get_for_pr(&conn, 419).unwrap().is_empty());
        let refs: serde_json::Value =
            serde_json::from_str(transition.task.refs.as_deref().unwrap()).unwrap();
        assert!(refs.get(MERGE_RETRY_REF).is_none());
        assert_eq!(refs[PARKED_REWORK_RETRY_REF], true);
        assert_eq!(refs["remediation_feedback"], feedback);
        let (rework_events, review_events): (i64, i64) = conn
            .query_row(
                "SELECT
                   SUM(kind='task_rework'),
                   SUM(kind='task_in_review')
                 FROM events WHERE subject=?1",
                [lease_target(task_id)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(rework_events, 1);
        assert_eq!(review_events, 0);
    }

    #[test]
    fn worker_fixable_merge_retry_rolls_back_when_attempt_marker_is_missing() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            Some(r#"{"pr":419}"#),
            None,
            None,
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='merging',author='worker',reviewer='r2' WHERE id=?1",
            [task_id],
        )
        .unwrap();
        crate::approvals::record(
            &mut conn,
            &crate::approvals::Approval {
                pr_number: 419,
                review_role: "r1".into(),
                task_id,
                author: "worker".into(),
                reviewer: "r1".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "head".into(),
            },
        )
        .unwrap();

        let error = match rework_approved_merge(&mut conn, task_id, 419, "fix merge", 11) {
            Ok(_) => panic!("missing attempt marker must reject rework disposition"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("lost admitted merge authority"));
        assert_eq!(get(&conn, task_id).unwrap().unwrap().status, "merging");
        assert!(crate::approvals::get(&conn, 419, "r1").unwrap().is_some());
        let rework_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_rework'",
                [lease_target(task_id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rework_events, 0);
    }

    #[test]
    fn merge_retry_optional_evidence_repair_retains_attempt_boundary() {
        let (_dir, mut conn) = open_tmp();
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            Some(r#"{"pr":419,"daemon_merge_retry":"attempting"}"#),
            None,
            None,
            10,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='merging',author='worker',reviewer='r1' WHERE id=?1",
            [task_id],
        )
        .unwrap();
        for role in ["r1", "r2"] {
            crate::approvals::record(
                &mut conn,
                &crate::approvals::Approval {
                    pr_number: 419,
                    review_role: role.into(),
                    task_id,
                    author: "worker".into(),
                    reviewer: role.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "head".into(),
                },
            )
            .unwrap();
        }

        assert!(repair_merge_retry_evidence(
            &mut conn,
            task_id,
            419,
            &["r2"],
            "optional R2 stale",
            11,
        )
        .unwrap());
        let task = get(&conn, task_id).unwrap().unwrap();
        assert_eq!(task.status, "merging");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[MERGE_RETRY_REF], MERGE_RETRY_ATTEMPTING);
        assert!(crate::approvals::get(&conn, 419, "r1").unwrap().is_some());
        assert!(crate::approvals::get(&conn, 419, "r2").unwrap().is_none());
        let events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE subject=?1 AND kind='merge_retry_authority_repaired'",
                [lease_target(task_id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(events, 1);
    }

    #[test]
    fn dependency_park_retry_restores_same_open_task_without_spawning_early() {
        let (_dir, mut conn) = open_tmp();
        let dep = create(
            &mut conn, "owner", "dep", None, 0, None, None, None, None, 10,
        )
        .unwrap();
        let child = create(
            &mut conn,
            "owner",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            11,
        )
        .unwrap();
        conn.execute("UPDATE tasks SET status='failed' WHERE id=?1", params![dep])
            .unwrap();
        crate::sweep::cascade_dead_deps(&conn, 12, 100).unwrap();
        assert_eq!(get(&conn, child).unwrap().unwrap().status, "failed");
        assert!(
            claim(&mut conn, "worker", None, &[], TTL, 13)
                .unwrap()
                .is_none(),
            "parked dependency task must not be provisioned"
        );

        let retried = retry_parked(&mut conn, child, "operator", true, 14)
            .unwrap()
            .unwrap();
        assert_eq!(retried.status, "open");
        assert!(!retried.ready, "failed dependency must still gate the task");
        assert!(
            claim(&mut conn, "worker", None, &[], TTL, 15)
                .unwrap()
                .is_none(),
            "retry must preserve dependency gating"
        );
    }

    #[test]
    fn update_refs_daemon_bypasses_assignee_guard() {
        let (_dir, mut conn) = open_tmp();
        let now = 1000;
        let id = create(
            &mut conn,
            "creator",
            "Test task",
            None,
            0,
            None,
            None,
            None,
            None,
            now,
        )
        .unwrap();
        // Claim as "worker-1" so assignee is set.
        claim(&mut conn, "worker-1", Some(id), &[], TTL, now).unwrap();
        // Normal update as "daemon" fails — not the assignee.
        let res = update(
            &mut conn,
            "daemon",
            id,
            &TaskUpdate {
                refs: Some(r#"{"thread":"abc"}"#),
                ..Default::default()
            },
            now,
        );
        assert!(res.is_err(), "regular update by non-assignee must fail");
        // Daemon-authoritative refs update succeeds.
        update_refs_daemon(&mut conn, id, r#"{"thread":"abc"}"#, now).unwrap();
        let t = get(&conn, id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(t.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["thread"], "abc");
        assert_eq!(refs["cx_est"], 3);
    }

    #[test]
    fn provider_retry_worker_is_atomic_and_requeues_implementation() {
        let (_dir, mut conn) = open_tmp();
        let id = create(
            &mut conn, "owner", "task", None, 0, None, None, None, None, 10,
        )
        .unwrap();
        claim(&mut conn, "worker", Some(id), &[], TTL, 11).unwrap();
        update_refs_daemon(
            &mut conn,
            id,
            r#"{"pr":419,"codex_thread_id":"thread-old","codex_provider_blocked":true,"codex_provider_error":"quota","codex_retry_model":"gpt","codex_retry_effort":"high","codex_retry_prompt":"fix blocker","codex_retry_turn_kind":"rework","codex_retry_thread_id":"thread-old","keep":"yes"}"#,
            12,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET rework_round=2, author='original-author' WHERE id=?1",
            params![id],
        )
        .unwrap();

        let retried = retry_provider_blocked(&mut conn, id, "operator", 13)
            .unwrap()
            .unwrap();
        assert_eq!(retried.status, "open");
        assert!(retried.assignee.is_none());
        let refs: serde_json::Value =
            serde_json::from_str(retried.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["keep"], "yes");
        assert_eq!(refs["pr"], 419);
        assert_eq!(refs["codex_thread_id"], "thread-old");
        assert!(crate::runner_state::requested_retry(&refs, "codex").is_some());
        assert!(refs.get("codex_retry_requested").is_none());
        assert_eq!(retried.rework_round, 2);
        assert_eq!(retried.author.as_deref(), Some("original-author"));
        let active_lease: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target=?1 AND active=1",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_lease, 0);
        let audit_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='task_provider_retry' AND subject=?1",
                params![format!("task#{id}")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(audit_count, 1);
        assert!(refs.get("codex_provider_blocked").is_none());
        assert!(refs.get("codex_provider_error").is_none());
        assert!(retry_provider_blocked(&mut conn, id, "operator", 14)
            .unwrap()
            .is_none());

        claim(&mut conn, "retry-worker", Some(id), &[], TTL, 15).unwrap();
        let submitted = apply_event(
            &mut conn,
            "retry-worker",
            id,
            &Event::SignaledDone { pr: "419".into() },
            16,
        )
        .unwrap()
        .task;
        assert_eq!(submitted.status, "in-review");
        let submitted_refs: serde_json::Value =
            serde_json::from_str(submitted.refs.as_deref().unwrap()).unwrap();
        assert!(submitted_refs.get("codex_retry_requested").is_none());
        assert!(submitted_refs.get("codex_retry_prompt").is_none());
        assert_eq!(submitted_refs["codex_thread_id"], "thread-old");
    }

    #[test]
    fn neutral_provider_retry_is_atomic_and_cleared_on_submit() {
        let (_dir, mut conn) = open_tmp();
        let id = create(
            &mut conn, "owner", "task", None, 0, None, None, None, None, 10,
        )
        .unwrap();
        claim(&mut conn, "worker", Some(id), &[], TTL, 11).unwrap();
        let refs = serde_json::json!({
            "pr": 420,
            "runner_provider_block": {
                "provider": "codex", "reason": "quota"
            },
            "runner_retry": {
                "provider": "codex", "model": "gpt-5", "effort": "high",
                "prompt": "finish exact turn", "turn_kind": "rework",
                "continuation_id": "thread-neutral"
            }
        });
        update_refs_daemon(&mut conn, id, &refs.to_string(), 12).unwrap();

        let retried = retry_provider_blocked(&mut conn, id, "operator", 13)
            .unwrap()
            .unwrap();
        assert_eq!(retried.status, "open");
        let queued: serde_json::Value =
            serde_json::from_str(retried.refs.as_deref().unwrap()).unwrap();
        let retry = crate::runner_state::requested_retry(&queued, "codex").unwrap();
        assert_eq!(retry.prompt, "finish exact turn");
        assert_eq!(retry.continuation_id.as_deref(), Some("thread-neutral"));
        assert!(queued
            .get(crate::runner_state::PROVIDER_BLOCK_REF)
            .is_none());

        claim(&mut conn, "retry-worker", Some(id), &[], TTL, 14).unwrap();
        let submitted = apply_event(
            &mut conn,
            "retry-worker",
            id,
            &Event::SignaledDone { pr: "420".into() },
            15,
        )
        .unwrap()
        .task;
        let submitted_refs: serde_json::Value =
            serde_json::from_str(submitted.refs.as_deref().unwrap()).unwrap();
        assert!(submitted_refs.get(crate::runner_state::RETRY_REF).is_none());
        assert_eq!(
            submitted_refs[crate::runner_state::CONTINUATION_REF]["id"],
            "thread-neutral"
        );
    }

    #[test]
    fn provider_retry_rejects_review_phase() {
        let (_dir, mut conn) = open_tmp();
        let id = create(
            &mut conn,
            "owner",
            "review",
            None,
            0,
            None,
            Some(r#"{"codex_provider_blocked":true,"codex_provider_error":"auth"}"#),
            None,
            Some(42),
            10,
        )
        .unwrap();
        assert!(retry_provider_blocked(&mut conn, id, "operator", 11)
            .unwrap()
            .is_none());
    }

    #[test]
    fn provider_retry_rejects_unblocked_and_terminal_tasks() {
        let (_dir, mut conn) = open_tmp();
        let id = create(
            &mut conn, "owner", "task", None, 0, None, None, None, None, 10,
        )
        .unwrap();
        assert!(retry_provider_blocked(&mut conn, id, "operator", 11)
            .unwrap()
            .is_none());
        update_refs_daemon(&mut conn, id, r#"{"codex_provider_blocked":true}"#, 12).unwrap();
        close_manual(&mut conn, "owner", id, "obsolete", 13).unwrap();
        assert!(retry_provider_blocked(&mut conn, id, "operator", 14)
            .unwrap()
            .is_none());
    }

    #[test]
    fn malformed_persisted_retry_refs_are_internal_error() {
        let error = clear_runner_retry_refs(Some("{")).unwrap_err();
        assert_eq!(error.exit_code(), 3);
        assert!(error.to_string().contains("invalid persisted refs JSON"));
    }

    // ── claim_remediation_rework (#199) ─────────────────────────────────────

    #[test]
    fn remediation_claim_installs_lease_and_preserves_original_author() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();
        // Working → InReview
        apply_event(
            &mut c,
            "W1",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1100,
        )
        .unwrap();
        // InReview → Rework (VerdictChanges releases the stale lease)
        apply_event(
            &mut c,
            "R1",
            id,
            &Event::ReviewerAttached { agent: "R1".into() },
            1200,
        )
        .unwrap();
        apply_event(&mut c, "R1", id, &Event::VerdictChanges, 1300).unwrap();

        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "rework");
        // The stale worker lease was released by VerdictChanges.
        assert!(!has_live_lease(&c, id, 1300));

        // Remediation claim.
        let claimed = claim_remediation_rework(&mut c, "REM1", id, TTL, 1400).unwrap();
        assert!(claimed.is_some());
        let t = claimed.unwrap();
        assert_eq!(t.status, "rework");
        assert_eq!(t.assignee.as_deref(), Some("REM1"));
        assert_eq!(
            t.author.as_deref(),
            Some("W1"),
            "original author identifies the managed PR branch across remediation retries"
        );
        assert!(has_live_lease(&c, id, 1400));

        // Lease survives sweep.
        crate::sweep::sweep_on_write(&c, 1500, 100).unwrap();
        let t2 = get(&c, id).unwrap().unwrap();
        assert_eq!(
            t2.status, "rework",
            "rework must survive sweep after lease installed"
        );
    }

    #[test]
    fn remediation_rework_claim_waits_for_dependencies() {
        let (_dir, mut conn) = open_tmp();
        let dependency = create(
            &mut conn,
            "owner",
            "dependency",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let dependencies = format!("[{dependency}]");
        let task_id = create(
            &mut conn,
            "owner",
            "task",
            None,
            0,
            None,
            None,
            Some(&dependencies),
            None,
            1001,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='rework' WHERE id=?1",
            params![task_id],
        )
        .unwrap();

        assert!(
            claim_remediation_rework(&mut conn, "remediation", task_id, TTL, 1002)
                .unwrap()
                .is_none()
        );
        assert_eq!(get(&conn, task_id).unwrap().unwrap().assignee, None);

        conn.execute(
            "UPDATE tasks SET status='done' WHERE id=?1",
            params![dependency],
        )
        .unwrap();
        let claimed = claim_remediation_rework(&mut conn, "remediation", task_id, TTL, 1003)
            .unwrap()
            .expect("done dependency must allow remediation rework claim");
        assert_eq!(claimed.id, task_id);
    }

    #[test]
    fn remediation_claim_verdict_then_sweep_regression() {
        // Regression for #199: VerdictChanges followed by an intervening
        // sweep_on_write must not erase rework when a remediation lease exists.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W1",
            id,
            &Event::SignaledDone { pr: "99".into() },
            1100,
        )
        .unwrap();
        apply_event(
            &mut c,
            "R1",
            id,
            &Event::ReviewerAttached { agent: "R1".into() },
            1200,
        )
        .unwrap();
        apply_event(&mut c, "R1", id, &Event::VerdictChanges, 1300).unwrap();

        // Remediation claim installs lease.
        claim_remediation_rework(&mut c, "REM1", id, TTL, 1400).unwrap();

        // Simulate multiple intervening writes triggering sweep.
        for t in 1500..1510 {
            crate::sweep::sweep_on_write(&c, t, 100).unwrap();
        }

        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "rework", "rework must survive repeated sweeps");
        assert_eq!(t.assignee.as_deref(), Some("REM1"));
    }

    #[test]
    fn remediation_claim_revalidation_rejects_creator_cancellation() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W1",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1100,
        )
        .unwrap();
        apply_event(
            &mut c,
            "R1",
            id,
            &Event::ReviewerAttached { agent: "R1".into() },
            1200,
        )
        .unwrap();
        apply_event(&mut c, "R1", id, &Event::VerdictChanges, 1300).unwrap();
        claim_remediation_rework(&mut c, "REM1", id, TTL, 1400)
            .unwrap()
            .expect("remediation claim");

        assert!(
            remediation_claim_still_owned(&mut c, "REM1", id, 1401).unwrap(),
            "fresh remediation lease should authorize provisioning"
        );

        cancel(&mut c, "boss", id, 1402).unwrap();

        assert_eq!(get(&c, id).unwrap().unwrap().status, "cancelled");
        assert!(
            !remediation_claim_still_owned(&mut c, "REM1", id, 1403).unwrap(),
            "terminal lifecycle state must revoke provisioning authority"
        );
    }

    #[test]
    fn remediation_lease_renewed_by_phase_4d() {
        // Active remediation worker's lease is renewed through renew_task_lease.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W1",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1100,
        )
        .unwrap();
        apply_event(
            &mut c,
            "R1",
            id,
            &Event::ReviewerAttached { agent: "R1".into() },
            1200,
        )
        .unwrap();
        apply_event(&mut c, "R1", id, &Event::VerdictChanges, 1300).unwrap();
        claim_remediation_rework(&mut c, "REM1", id, TTL, 1400).unwrap();

        // Original lease expires at 1400 + TTL = 5000. Renew at 4900.
        crate::agents::renew_task_lease(&c, "REM1", id, 4900).unwrap();

        // Check extended expiry.
        let exp: i64 = c
            .query_row(
                "SELECT expires_at FROM claims WHERE target=?1 AND holder='REM1' AND active=1",
                params![lease_target(id)],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(exp, 4900 + DEFAULT_LEASE_TTL_SECS);

        // Task survives sweep well past the original expiry.
        crate::sweep::sweep_on_write(&c, 5100, 100).unwrap();
        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(
            t.status, "rework",
            "renewed lease must protect rework from reaper"
        );
    }

    #[test]
    fn two_remediation_claimants_exactly_one_winner() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W1",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1100,
        )
        .unwrap();
        apply_event(
            &mut c,
            "R1",
            id,
            &Event::ReviewerAttached { agent: "R1".into() },
            1200,
        )
        .unwrap();
        apply_event(&mut c, "R1", id, &Event::VerdictChanges, 1300).unwrap();

        let first = claim_remediation_rework(&mut c, "REM1", id, TTL, 1400).unwrap();
        assert!(first.is_some(), "first claimant must win");

        let second = claim_remediation_rework(&mut c, "REM2", id, TTL, 1401).unwrap();
        assert!(
            second.is_none(),
            "second claimant must lose to partial unique index"
        );

        // First claimant's lease is still active.
        let holder: String = c
            .query_row(
                "SELECT holder FROM claims WHERE target=?1 AND active=1 AND expires_at > ?2",
                params![lease_target(id), 1401],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(holder, "REM1");

        // Zero errors for the normal lost race.
        let err_count: i64 = c
            .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
            .unwrap();
        assert_eq!(err_count, 0, "lost race must not produce error rows");
    }

    #[test]
    fn limited_implementation_ready_listing_bounds_materialized_tasks() {
        let (_dir, mut conn) = open_tmp();
        let mut ids = Vec::new();
        for priority in 0..=20 {
            let body = format!("ready body {priority}: {}", "x".repeat(1024));
            ids.push(
                create(
                    &mut conn,
                    "boss",
                    &format!("ready-{priority}"),
                    Some(&body),
                    priority,
                    None,
                    None,
                    None,
                    None,
                    1000,
                )
                .unwrap(),
            );
        }

        let listed = list_implementation_ready_open_limited(&conn, 20).unwrap();
        assert_eq!(listed.len(), 20);
        assert_eq!(
            listed.iter().map(|task| task.id).collect::<Vec<_>>(),
            ids.iter().rev().take(20).copied().collect::<Vec<_>>(),
        );
        assert!(listed.iter().all(|task| task.ready));
        assert!(listed.iter().all(|task| task.id != ids[0]));
        assert!(matches!(
            list_implementation_ready_open_limited(&conn, -1),
            Err(QuorumError::Usage(_))
        ));
    }

    #[test]
    fn limited_implementation_ready_listing_filters_before_its_limit() {
        const DASHBOARD_LIMIT: i64 = 20;

        let (_dir, mut conn) = open_tmp();
        for priority in 2..=DASHBOARD_LIMIT + 1 {
            let id = create(
                &mut conn,
                "boss",
                &format!("unready-{priority}"),
                None,
                priority,
                None,
                None,
                None,
                None,
                1000,
            )
            .unwrap();
            conn.execute(
                "UPDATE tasks SET refs=?1 WHERE id=?2",
                params![
                    r#"{"cx_est":2,"cx_size":"S","cx_ready":false,"cx_not_ready_reason":"waiting on design"}"#,
                    id
                ],
            )
            .unwrap();
        }
        let dispatchable = create(
            &mut conn,
            "boss",
            "dispatchable after unready prefix",
            None,
            1,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();

        assert_eq!(
            list_implementation_ready_open_limited(&conn, DASHBOARD_LIMIT)
                .unwrap()
                .into_iter()
                .map(|task| task.id)
                .collect::<Vec<_>>(),
            [dispatchable],
        );
    }

    #[test]
    fn two_selected_implementation_claimants_have_one_clean_winner() {
        use std::sync::{Arc, Barrier};

        let (dir, mut conn) = open_tmp();
        let id = create(
            &mut conn, "boss", "selected", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        assert_eq!(
            list_implementation_ready_open(&conn)
                .unwrap()
                .into_iter()
                .map(|task| task.id)
                .collect::<Vec<_>>(),
            [id]
        );
        drop(conn);

        let path = dir.path().join("q.db");
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = ["worker-a", "worker-b"]
            .into_iter()
            .map(|agent| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    barrier.wait();
                    claim(&mut conn, agent, Some(id), &[], TTL, 1001)
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect();
        assert_eq!(outcomes.iter().filter(|task| task.is_some()).count(), 1);
        assert_eq!(outcomes.iter().filter(|task| task.is_none()).count(), 1);

        let conn = crate::db::open(&path).unwrap();
        assert_eq!(get(&conn, id).unwrap().unwrap().status, "working");
        let err_count: i64 = conn
            .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
            .unwrap();
        assert_eq!(err_count, 0, "lost claim race must stay a clean negative");
    }

    #[test]
    fn remediation_claim_fails_if_task_not_in_rework() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        // Task is in open — claim must fail.
        let result = claim_remediation_rework(&mut c, "REM1", id, TTL, 1100).unwrap();
        assert!(result.is_none());

        // Task in working — claim must fail.
        claim(&mut c, "W1", Some(id), &[], TTL, 1200).unwrap();
        let result = claim_remediation_rework(&mut c, "REM1", id, TTL, 1300).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn release_remediation_lease_cleans_up() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W1",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1100,
        )
        .unwrap();
        apply_event(
            &mut c,
            "R1",
            id,
            &Event::ReviewerAttached { agent: "R1".into() },
            1200,
        )
        .unwrap();
        apply_event(&mut c, "R1", id, &Event::VerdictChanges, 1300).unwrap();
        claim_remediation_rework(&mut c, "REM1", id, TTL, 1400).unwrap();

        // Simulate provisioning failure.
        release_remediation_lease(&mut c, "REM1", id, 1500).unwrap();

        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "rework", "task stays in rework after release");
        assert!(t.assignee.is_none(), "assignee cleared on release");
        assert!(
            !has_live_lease(&c, id, 1500),
            "lease deactivated on release"
        );

        // Event emitted.
        let evs = crate::events::list(&c, 0, Some(&lease_target(id)), 20, 1500).unwrap();
        assert!(
            evs.iter().any(|e| e.kind == "remediation_lease_released"),
            "remediation_lease_released event must be emitted"
        );
    }

    #[test]
    fn controlled_shutdown_suspends_exact_run_lease_and_records_audit_event() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c, "owner", "shutdown", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        claim(&mut c, "worker", Some(id), &[], TTL, 1000).unwrap();
        crate::capabilities::issue(&mut c, "run-shutdown", id, "worker", "worker", 1000).unwrap();
        let status_before = get(&c, id).unwrap().unwrap().status;

        assert!(
            suspend_run_for_controlled_shutdown(&mut c, "worker", id, "run-shutdown", 1001,)
                .unwrap()
        );

        let revoked_at: Option<i64> = c
            .query_row(
                "SELECT revoked_at FROM run_capabilities WHERE run_id='run-shutdown'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revoked_at, Some(1001), "the exact run must be revoked");
        let active: i64 = c
            .query_row(
                "SELECT active FROM claims WHERE target=?1",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active, 0, "the task lease must be inactive");
        assert_eq!(get(&c, id).unwrap().unwrap().status, status_before);
        let events = crate::events::list(&c, 0, Some(&lease_target(id)), 20, 1001).unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.kind == "controlled_shutdown_suspended"),
            "controlled shutdown must have its own audit event"
        );
    }

    #[test]
    fn controlled_shutdown_revokes_only_the_named_run_for_an_agent() {
        let (_d, mut c) = open_tmp();
        let first = create(
            &mut c, "owner", "first", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        let second = create(
            &mut c, "owner", "second", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        claim(&mut c, "shared-agent", Some(first), &[], TTL, 1000).unwrap();
        claim(&mut c, "shared-agent", Some(second), &[], TTL, 1000).unwrap();
        crate::capabilities::issue(&mut c, "run-first", first, "shared-agent", "worker", 1000)
            .unwrap();
        crate::capabilities::issue(&mut c, "run-second", second, "shared-agent", "worker", 1000)
            .unwrap();

        assert!(suspend_run_for_controlled_shutdown(
            &mut c,
            "shared-agent",
            first,
            "run-first",
            1001,
        )
        .unwrap());

        let second_revoked_at: Option<i64> = c
            .query_row(
                "SELECT revoked_at FROM run_capabilities WHERE run_id='run-second'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            second_revoked_at.is_none(),
            "suspending one run must not revoke another run for the same agent"
        );
    }

    #[test]
    fn controlled_shutdown_rejects_mismatched_run_tuple_without_mutation() {
        let (_d, mut c) = open_tmp();
        let first = create(
            &mut c, "owner", "first", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        let second = create(
            &mut c, "owner", "second", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        claim(&mut c, "agent-a", Some(first), &[], TTL, 1000).unwrap();
        claim(&mut c, "agent-b", Some(second), &[], TTL, 1000).unwrap();
        crate::capabilities::issue(&mut c, "run-first", first, "agent-a", "worker", 1000).unwrap();
        crate::capabilities::issue(&mut c, "run-second", second, "agent-b", "worker", 1000)
            .unwrap();

        let wrong_task =
            suspend_run_for_controlled_shutdown(&mut c, "agent-a", first, "run-second", 1001)
                .unwrap_err();
        assert!(format!("{wrong_task}").contains("does not belong"));
        let wrong_agent =
            suspend_run_for_controlled_shutdown(&mut c, "agent-b", first, "run-first", 1001)
                .unwrap_err();
        assert!(format!("{wrong_agent}").contains("does not belong"));

        for run_id in ["run-first", "run-second"] {
            let revoked_at: Option<i64> = c
                .query_row(
                    "SELECT revoked_at FROM run_capabilities WHERE run_id=?1",
                    params![run_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(revoked_at.is_none(), "{run_id} must stay active");
        }
        assert!(
            has_live_lease(&c, first, 1001),
            "first lease must stay active"
        );
        assert!(
            has_live_lease(&c, second, 1001),
            "second lease must stay active"
        );
        let event_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='controlled_shutdown_suspended'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 0, "mismatches must not write audit events");
    }

    #[test]
    fn controlled_shutdown_rolls_back_when_lease_deactivation_fails() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c, "owner", "rollback", None, 0, None, None, None, None, 1000,
        )
        .unwrap();
        claim(&mut c, "worker", Some(id), &[], TTL, 1000).unwrap();
        crate::capabilities::issue(&mut c, "run-rollback", id, "worker", "worker", 1000).unwrap();
        c.execute_batch(&format!(
            "CREATE TRIGGER fail_shutdown_lease_deactivation
             BEFORE UPDATE OF active ON claims
             WHEN OLD.target = '{}' AND OLD.active = 1
             BEGIN SELECT RAISE(ABORT, 'forced lease failure'); END;",
            lease_target(id)
        ))
        .unwrap();

        let err = suspend_run_for_controlled_shutdown(&mut c, "worker", id, "run-rollback", 1001)
            .unwrap_err();
        assert!(format!("{err}").contains("forced lease failure"));

        let revoked_at: Option<i64> = c
            .query_row(
                "SELECT revoked_at FROM run_capabilities WHERE run_id='run-rollback'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(revoked_at.is_none(), "capability revocation must roll back");
        assert!(
            has_live_lease(&c, id, 1001),
            "lease deactivation must roll back"
        );
        let event_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind='controlled_shutdown_suspended'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 0, "audit event must not partially commit");
    }

    #[test]
    fn controlled_shutdown_is_idempotent_and_reports_already_revoked_run() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "owner",
            "idempotent",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "worker", Some(id), &[], TTL, 1000).unwrap();
        crate::capabilities::issue(&mut c, "run-idempotent", id, "worker", "worker", 1000).unwrap();

        assert!(
            suspend_run_for_controlled_shutdown(&mut c, "worker", id, "run-idempotent", 1001,)
                .unwrap()
        );
        assert!(
            !suspend_run_for_controlled_shutdown(&mut c, "worker", id, "run-idempotent", 1002,)
                .unwrap(),
            "the second suspension must report that the capability was already revoked"
        );
    }

    #[test]
    fn remediation_exact_expiry_boundary() {
        // At expires_at == now, the claim is DEAD (invariant: DEAD iff
        // expires_at <= now).
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W1",
            id,
            &Event::SignaledDone { pr: "42".into() },
            1100,
        )
        .unwrap();
        apply_event(
            &mut c,
            "R1",
            id,
            &Event::ReviewerAttached { agent: "R1".into() },
            1200,
        )
        .unwrap();
        apply_event(&mut c, "R1", id, &Event::VerdictChanges, 1300).unwrap();

        let short_ttl = 100;
        claim_remediation_rework(&mut c, "REM1", id, short_ttl, 1400).unwrap();

        // At exact expiry (1400 + 100 = 1500), claim is dead.
        assert!(!has_live_lease(&c, id, 1500), "claim dead at exact expiry");

        // Reaper fires (grace window has also passed: updated_at=1400,
        // grace=60, so 1400+60=1460 < 1500).
        crate::sweep::reap_lapsed_tasks(&c, 1500, 100).unwrap();
        let t = get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "open", "expired remediation lease must be reaped");
    }

    #[test]
    fn failed_ci_remediation_intent_is_atomic_recoverable_and_cleared_on_push() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "durable CI remediation",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();
        apply_event(
            &mut c,
            "W1",
            id,
            &Event::SignaledDone { pr: "453".into() },
            1100,
        )
        .unwrap();

        let checks = vec!["fmt".to_string(), "test".to_string()];
        let transition = apply_checks_failed_with_remediation(
            &mut c,
            id,
            453,
            "abc123",
            &checks,
            "fix the exact failed CI",
            1200,
        )
        .unwrap();
        assert_eq!(transition.task.status, "rework");
        let intent = ci_remediation_intent(transition.task.refs.as_deref())
            .unwrap()
            .unwrap();
        assert_eq!(
            intent,
            CiRemediationIntent {
                pr: 453,
                head_sha: "abc123".into(),
                feedback: "fix the exact failed CI".into(),
                checks,
                attempts: 0,
            }
        );
        assert_eq!(
            record_ci_remediation_attempt(&mut c, id, 1201).unwrap(),
            Some(1)
        );

        claim_remediation_rework(&mut c, "REM1", id, TTL, 1202)
            .unwrap()
            .unwrap();
        assert!(reset_ci_remediation_for_recovery(&mut c, id, 1203).unwrap());
        let recovered = get(&c, id).unwrap().unwrap();
        assert_eq!(recovered.status, "rework");
        assert_eq!(recovered.assignee, None);
        assert!(!has_live_lease(&c, id, 1203));

        claim_remediation_rework(&mut c, "REM2", id, TTL, 1204)
            .unwrap()
            .unwrap();
        apply_event(&mut c, "REM2", id, &Event::ReworkPushed, 1205).unwrap();
        let submitted = get(&c, id).unwrap().unwrap();
        assert_eq!(submitted.status, "in-review");
        assert_eq!(
            ci_remediation_intent(submitted.refs.as_deref()).unwrap(),
            None,
            "successful rework submission must consume durable CI intent"
        );
    }

    #[test]
    fn ready_large_review_only_gets_atomic_review_and_remediation_authority() {
        let (_d, mut c) = open_tmp();
        for (index, (size, cx_est)) in [("L", 5), ("XL", 2)].into_iter().enumerate() {
            let offset = index as i64 * 10;
            let id = create(
                &mut c,
                "owner",
                &format!("review {size}"),
                None,
                0,
                None,
                None,
                None,
                Some(500 + index as i64),
                1000 + index as i64,
            )
            .unwrap();
            c.execute(
                "UPDATE tasks
                 SET refs=json_object(
                    'pr', ?2, 'cx_est', ?4, 'cx_size', ?3, 'cx_ready', json('true'),
                    'cx_not_ready_reason', json('null'), 'cx_by', 'test:v2'
                 ) WHERE id=?1",
                params![id, 500 + index as i64, size, cx_est],
            )
            .unwrap();
            let task = get(&c, id).unwrap().unwrap();
            assert!(task.review_only);
            assert_eq!(task.status, "in-review");
            assert!(classification_is_dispatchable(
                &task.refs,
                task.review_only,
                task.continue_pr
            ));

            let token = format!("review-{size}");
            assert!(reserve_reviewer_provision(&mut c, id, &token, "r1", 1100 + offset,).unwrap());
            assert!(
                !reserve_reviewer_provision(
                    &mut c,
                    id,
                    &format!("loser-{size}"),
                    "r1",
                    1101 + offset,
                )
                .unwrap(),
                "the reviewer reservation remains single-holder for size {size}"
            );

            let reviewer = format!("reviewer-{size}");
            let attached = claim(&mut c, &reviewer, Some(id), &[], TTL, 1102 + offset)
                .unwrap()
                .expect("large review-only task must accept reviewer attachment");
            assert_eq!(attached.reviewer.as_deref(), Some(reviewer.as_str()));
            assert!(release_reviewer_provision(&mut c, id, &token).unwrap());

            let rework =
                apply_event(&mut c, &reviewer, id, &Event::VerdictChanges, 1103 + offset).unwrap();
            assert_eq!(rework.task.status, "rework");
            let remediation = format!("remediation-{size}");
            let claimed = claim_remediation_rework(&mut c, &remediation, id, TTL, 1104 + offset)
                .unwrap()
                .expect("large review-only rework must accept remediation authority");
            assert_eq!(claimed.assignee.as_deref(), Some(remediation.as_str()));
            assert!(has_live_lease(&c, id, 1104 + offset));
        }

        assert_eq!(park_classified_complexity_five(&mut c, 2000).unwrap(), 0);
        let parked: i64 = c
            .query_row(
                "SELECT count(*) FROM tasks WHERE status='failed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parked, 0);
    }

    #[test]
    fn ready_large_review_only_provider_retry_rework_gets_atomic_authority() {
        let (_d, mut c) = open_tmp();
        for (index, (size, cx_est)) in [("L", 5), ("XL", 2)].into_iter().enumerate() {
            let id = create(
                &mut c,
                "owner",
                &format!("provider retry {size}"),
                None,
                0,
                None,
                Some(&format!(
                    r#"{{"cx_est":{cx_est},"cx_size":"{size}","cx_ready":true,"cx_not_ready_reason":null}}"#
                )),
                None,
                Some(700 + index as i64),
                1200 + index as i64,
            )
            .unwrap();
            c.execute(
                "UPDATE tasks
                 SET status='rework',
                     refs=json_set(refs,'$.runner_retry',
                         json_object('requested',json('true')))
                 WHERE id=?1",
                [id],
            )
            .unwrap();

            let agent = format!("provider-retry-{size}");
            let claimed = claim_provider_retry_rework(&mut c, &agent, id, TTL, 1300 + index as i64)
                .unwrap()
                .expect("large review-only provider retry must acquire worker authority");
            assert_eq!(claimed.assignee.as_deref(), Some(agent.as_str()));
            assert!(has_live_lease(&c, id, 1300 + index as i64));
        }
    }

    #[test]
    fn review_provision_requires_complete_ready_classification_but_runs_under_decomposition_freeze()
    {
        let (_d, mut c) = open_tmp();
        let incomplete = create(
            &mut c,
            "owner",
            "incomplete review",
            None,
            0,
            None,
            None,
            None,
            Some(600),
            1000,
        )
        .unwrap();
        let unready = create(
            &mut c,
            "owner",
            "unready review",
            None,
            0,
            None,
            None,
            None,
            Some(601),
            1001,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET refs=json_object(
                'pr',600,'cx_est',4,'cx_size','XL','cx_ready',json('true'))
             WHERE id=?1",
            [incomplete],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET refs=json_object(
                'pr',601,'cx_est',4,'cx_size','XL','cx_ready',json('false'),
                'cx_not_ready_reason','scope is incomplete')
             WHERE id=?1",
            [unready],
        )
        .unwrap();

        for (id, label) in [(incomplete, "incomplete"), (unready, "unready")] {
            assert!(!reserve_reviewer_provision(&mut c, id, label, "r1", 1010).unwrap());
            assert!(claim(&mut c, label, Some(id), &[], TTL, 1011)
                .unwrap()
                .is_none());
            c.execute(
                "UPDATE tasks SET status='rework',assignee=NULL WHERE id=?1",
                [id],
            )
            .unwrap();
            assert!(claim_remediation_rework(&mut c, label, id, TTL, 1012)
                .unwrap()
                .is_none());
        }

        let source = create(
            &mut c,
            "owner",
            "planning source",
            None,
            0,
            None,
            None,
            None,
            None,
            1020,
        )
        .unwrap();
        let frozen_review = create(
            &mut c,
            "owner",
            "ready review under freeze",
            None,
            0,
            None,
            Some(
                r#"{"pr":602,"cx_est":5,"cx_size":"XL","cx_ready":true,"cx_not_ready_reason":null}"#,
            ),
            None,
            Some(602),
            1021,
        )
        .unwrap();
        crate::decomposition::begin_planning(
            &mut c,
            &crate::decomposition::BeginPlanning {
                source_task_id: source,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 1022,
            },
        )
        .unwrap()
        .expect("planning source must acquire the freeze");

        // Deadlock regression: an in-flight PR's reviewer MUST still provision
        // under an active decomposition freeze. The freeze's drain predicate
        // (decomposition_drain_ready) waits for reviewers==0; blocking this
        // reservation would strand the retained worker awaiting review and the
        // freeze would never drain to capture its frozen base.
        assert!(reserve_reviewer_provision(&mut c, frozen_review, "frozen", "r1", 1023,).unwrap());
        // Attaching that reviewer to the in-review PR is likewise allowed under
        // the freeze — reviewer attachment is in-flight continuation.
        assert!(
            claim(&mut c, "frozen", Some(frozen_review), &[], TTL, 1024,)
                .unwrap()
                .is_some()
        );
        // Existing rework/remediation, by contrast, MUST finish under the freeze
        // (same deadlock class as review): the retained worker's rework turn is
        // in-flight work the drain waits on, not a new start.
        c.execute(
            "UPDATE tasks SET status='rework',assignee=NULL WHERE id=?1",
            [frozen_review],
        )
        .unwrap();
        assert!(
            claim_remediation_rework(&mut c, "frozen", frozen_review, TTL, 1025)
                .unwrap()
                .is_some()
        );

        let reservations: i64 = c
            .query_row(
                "SELECT count(*) FROM reviewer_provision_reservations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let claims: i64 = c
            .query_row("SELECT count(*) FROM claims WHERE active=1", [], |row| {
                row.get(0)
            })
            .unwrap();
        // Reviewer reservation and the remediation claim both landed under the
        // freeze — the two continuation paths that let the freeze drain. The
        // only thing the freeze blocked was the new open-status worker start.
        assert_eq!(reservations, 1);
        assert_eq!(claims, 1);
    }

    #[test]
    fn legacy_low_complexity_xl_is_unclaimable_then_reconciled_once() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "legacy low complexity XL",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET refs=json_object('cx_est', 2, 'cx_size','XL','cx_ready',true,'cx_by', 'legacy:v1') WHERE id=?1",
            params![id],
        )
        .unwrap();

        assert!(claim(&mut c, "worker", Some(id), &[], TTL, 1001)
            .unwrap()
            .is_none());
        assert_eq!(park_classified_complexity_five(&mut c, 1002).unwrap(), 1);
        assert_eq!(park_classified_complexity_five(&mut c, 1003).unwrap(), 0);

        let task = get(&c, id).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(task.status, "failed");
        assert_eq!(refs["cx_est"], 2);
        assert_eq!(refs["cx_by"], "legacy:v1");
        assert_eq!(refs["daemon_parked"], true);
        let events: i64 = c
            .query_row(
                "SELECT count(*) FROM events WHERE kind='task_parked' AND subject=?1",
                params![lease_target(id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(events, 1, "restart reconciliation must be idempotent");
    }

    #[test]
    fn legacy_low_complexity_xl_reconciliation_progresses_in_bounded_batches() {
        let (_d, mut c) = open_tmp();
        let total = SWEEP_LIMIT + 1;
        for seq in 0..total {
            let id = create(
                &mut c,
                "boss",
                &format!("legacy low complexity XL {seq}"),
                None,
                0,
                None,
                None,
                None,
                None,
                1000 + seq as i64,
            )
            .unwrap();
            c.execute(
                "UPDATE tasks
                 SET refs=json_object('cx_est', 2, 'cx_size','XL','cx_ready',true,'cx_by', 'legacy:v1')
                 WHERE id=?1",
                params![id],
            )
            .unwrap();
        }

        assert_eq!(
            park_classified_complexity_five(&mut c, 2000).unwrap(),
            SWEEP_LIMIT
        );
        let still_live: i64 = c
            .query_row(
                "SELECT count(*) FROM tasks WHERE status NOT IN ('done','failed','cancelled')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(still_live, 1, "one row must remain for the next tick");

        assert_eq!(park_classified_complexity_five(&mut c, 2001).unwrap(), 1);
        assert_eq!(park_classified_complexity_five(&mut c, 2002).unwrap(), 0);
        let parked: i64 = c
            .query_row(
                "SELECT count(*) FROM tasks
                 WHERE status='failed'
                   AND json_extract(refs, '$.daemon_parked')=1
                   AND json_extract(refs, '$.cx_est')=2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let events: i64 = c
            .query_row(
                "SELECT count(*) FROM events WHERE kind='task_parked'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parked, total as i64);
        assert_eq!(
            events, total as i64,
            "repeated ticks must not duplicate audit events"
        );
    }

    #[test]
    fn external_edits_are_revision_bound_and_capped_without_counting_replays() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c, "owner", "editable", None, 0, None, None, None, None, 1,
        )
        .unwrap();

        for (revision, body) in [(1, "one"), (2, "two"), (3, "three")] {
            let task = update(
                &mut c,
                "owner",
                id,
                &TaskUpdate {
                    body: Some(body),
                    expected_revision: Some(revision),
                    ..Default::default()
                },
                10 + revision,
            )
            .unwrap();
            assert_eq!(task.revision, revision + 1);
            assert_eq!(task.edit_count, revision);
        }

        let replay = update(
            &mut c,
            "owner",
            id,
            &TaskUpdate {
                body: Some("three"),
                expected_revision: Some(3),
                ..Default::default()
            },
            20,
        );
        assert!(matches!(replay, Err(QuorumError::NotHolder)));
        let fourth = update(
            &mut c,
            "owner",
            id,
            &TaskUpdate {
                body: Some("four"),
                expected_revision: Some(4),
                ..Default::default()
            },
            21,
        );
        assert!(matches!(fourth, Err(QuorumError::NotHolder)));
        let task = get(&c, id).unwrap().unwrap();
        assert_eq!(task.body.as_deref(), Some("three"));
        assert_eq!((task.revision, task.edit_count), (4, 3));
    }

    #[test]
    fn accepted_planning_edit_atomically_restarts_admission() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "owner", "large", None, 0, None, None, None, None, 1).unwrap();
        let graph = crate::decomposition::begin_planning(
            &mut c,
            &crate::decomposition::BeginPlanning {
                source_task_id: id,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        for now in 3..=5 {
            crate::decomposition::record_attempt(
                &mut c, graph, "proposal", "invalid", "retry", now,
            )
            .unwrap();
        }
        assert_eq!(get(&c, id).unwrap().unwrap().status, "failed");

        let task = update(
            &mut c,
            "owner",
            id,
            &TaskUpdate {
                body: Some("clarified"),
                expected_revision: Some(1),
                ..Default::default()
            },
            6,
        )
        .unwrap();
        assert_eq!(
            (task.status.as_str(), task.revision, task.edit_count),
            ("open", 2, 1)
        );
        let graphs: i64 = c
            .query_row("SELECT count(*) FROM task_decompositions", [], |row| {
                row.get(0)
            })
            .unwrap();
        let attempts: i64 = c
            .query_row("SELECT count(*) FROM decomposition_attempts", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!((graphs, attempts), (0, 0));
    }

    #[test]
    fn generated_child_rejects_direct_cancel_and_scope_edit() {
        let (_d, mut c) = open_tmp();
        let source = create(&mut c, "owner", "large", None, 0, None, None, None, None, 1).unwrap();
        let graph = crate::decomposition::begin_planning(
            &mut c,
            &crate::decomposition::BeginPlanning {
                source_task_id: source,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        crate::decomposition::set_frozen_phase(
            &mut c,
            graph,
            "freeze-requested",
            "preclassifying",
            None,
            3,
        )
        .unwrap();
        let child = |key: &str| {
            crate::decomposition::PlannedChild {
            local_key: key.into(),
            title: key.into(),
            body: format!("deliver {key}"),
            labels: None,
            classification_refs: r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#.into(),
            prerequisite_keys: vec![],
            source_dependency_ids: vec![],
        }
        };
        let ids =
            crate::decomposition::materialize_graph(&mut c, graph, 1, &[child("a"), child("b")], 4)
                .unwrap()
                .unwrap();

        assert!(update(
            &mut c,
            "owner",
            ids[0],
            &TaskUpdate {
                status: Some("cancelled"),
                ..Default::default()
            },
            5,
        )
        .is_err());
        assert!(update(
            &mut c,
            "owner",
            ids[0],
            &TaskUpdate {
                body: Some("changed"),
                expected_revision: Some(1),
                ..Default::default()
            },
            6,
        )
        .is_err());
        assert_eq!(get(&c, ids[0]).unwrap().unwrap().status, "open");
    }

    #[test]
    fn creator_cancels_materialized_graph_through_source_update() {
        let (_d, mut c) = open_tmp();
        let source = create(&mut c, "owner", "large", None, 0, None, None, None, None, 1).unwrap();
        let graph = crate::decomposition::begin_planning(
            &mut c,
            &crate::decomposition::BeginPlanning {
                source_task_id: source,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        crate::decomposition::set_frozen_phase(
            &mut c,
            graph,
            "freeze-requested",
            "preclassifying",
            None,
            3,
        )
        .unwrap();
        let child = |key: &str| {
            crate::decomposition::PlannedChild {
            local_key: key.into(),
            title: key.into(),
            body: format!("deliver {key}"),
            labels: None,
            classification_refs: r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#.into(),
            prerequisite_keys: vec![],
            source_dependency_ids: vec![],
        }
        };
        let ids =
            crate::decomposition::materialize_graph(&mut c, graph, 1, &[child("a"), child("b")], 4)
                .unwrap()
                .unwrap();

        let cancelled = update(
            &mut c,
            "owner",
            source,
            &TaskUpdate {
                status: Some("cancelled"),
                expected_revision: Some(1),
                ..Default::default()
            },
            5,
        )
        .unwrap();
        assert_eq!(cancelled.status, "cancelled");
        assert!(ids
            .iter()
            .all(|id| get(&c, *id).unwrap().unwrap().status == "cancelled"));
        assert!(matches!(
            update(
                &mut c,
                "owner",
                source,
                &TaskUpdate {
                    status: Some("cancelled"),
                    expected_revision: Some(1),
                    ..Default::default()
                },
                6,
            ),
            Err(QuorumError::NotHolder)
        ));
    }

    #[test]
    fn planning_freeze_blocks_new_open_claims_but_allows_existing_continuation() {
        let (_dir, mut c) = open_tmp();
        let source = create(&mut c, "owner", "large", None, 1, None,
            Some(r#"{"cx_est":4,"cx_size":"XL","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#),
            None, None, 1).unwrap();
        let implementation = create(&mut c, "owner", "small", None, 1, None,
            Some(r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#),
            None, None, 1).unwrap();
        let review = create_with_continue_pr(&mut c, "owner", "review", None, 1, None,
            Some(r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#),
            None, Some(42), None, 1).unwrap();
        let remediation = create(&mut c, "owner", "small2", None, 1, None,
            Some(r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#),
            None, None, 1).unwrap();
        crate::decomposition::begin_planning(
            &mut c,
            &crate::decomposition::BeginPlanning {
                source_task_id: source,
                expected_revision: 1,
                provider: "codex",
                model: "sol",
                frozen_base_sha: "abc",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();

        // A new open-status worker start stays blocked under the freeze — no
        // new implementation work begins while draining.
        assert!(claim(&mut c, "worker", Some(implementation), &[], 60, 3)
            .unwrap()
            .is_none());
        // But reviewer attachment to an existing in-review PR is allowed: review
        // continuation is in-flight work the drain waits on, not a new start.
        assert!(claim(&mut c, "reviewer", Some(review), &[], 60, 3)
            .unwrap()
            .is_some());

        // Existing rework/remediation MUST still complete under the freeze:
        // these are in-flight continuations the drain predicate waits on
        // (workers==0 && reviewers==0). Blocking them would deadlock the freeze
        // against its own drain — the incident this regression guards.
        c.execute(
            "UPDATE tasks SET status='rework',assignee=NULL,
            refs=json_set(refs,'$.daemon_rework_retry_requested',json('true')) WHERE id=?1",
            [implementation],
        )
        .unwrap();
        assert!(
            claim_provider_retry_rework(&mut c, "retry", implementation, 60, 4)
                .unwrap()
                .is_some()
        );
        c.execute(
            "UPDATE tasks SET status='rework',assignee=NULL WHERE id=?1",
            [remediation],
        )
        .unwrap();
        assert!(
            claim_remediation_rework(&mut c, "remediation", remediation, 60, 4)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn worker_lease_active_for_flags_live_claim() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();

        assert!(
            worker_lease_active_for(&mut c, "W1", id, 1001).unwrap(),
            "live worker lease must block name-pool release"
        );
        // Idempotent: same-holder repeat check does not change state.
        assert!(worker_lease_active_for(&mut c, "W1", id, 1002).unwrap());
    }

    #[test]
    fn worker_lease_active_for_rejects_different_holder() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], TTL, 1000).unwrap();

        // A retired worker name (W2) never held this lease; the guard must not
        // treat a sibling holder's claim as a reason to retain a different name.
        assert!(!worker_lease_active_for(&mut c, "W2", id, 1001).unwrap());
    }

    #[test]
    fn worker_lease_active_for_ignores_expired_and_deactivated() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "W1", Some(id), &[], 100, 1000).unwrap();
        // Past expiry.
        assert!(!worker_lease_active_for(&mut c, "W1", id, 2000).unwrap());

        let id2 = create(&mut c, "boss", "u", None, 0, None, None, None, None, 3000).unwrap();
        claim(&mut c, "W2", Some(id2), &[], TTL, 3000).unwrap();
        // Terminal handoff would deactivate the lease.
        c.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND holder=?2",
            params![lease_target(id2), "W2"],
        )
        .unwrap();
        assert!(!worker_lease_active_for(&mut c, "W2", id2, 3001).unwrap());
    }

    #[test]
    fn worker_lease_active_for_missing_claim_is_false() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        // No claim was ever taken (e.g., worker died before insert).
        assert!(!worker_lease_active_for(&mut c, "W1", id, 1001).unwrap());
    }

    #[test]
    fn count_started_non_terminal_excludes_source_open_and_terminals() {
        let (_d, mut c) = open_tmp();
        let source = create(&mut c, "boss", "src", None, 0, None, None, None, None, 1000).unwrap();
        let working = create(&mut c, "boss", "w", None, 0, None, None, None, None, 1000).unwrap();
        let in_review = create(&mut c, "boss", "r", None, 0, None, None, None, None, 1000).unwrap();
        let rework = create(&mut c, "boss", "k", None, 0, None, None, None, None, 1000).unwrap();
        let open_task = create(&mut c, "boss", "o", None, 0, None, None, None, None, 1000).unwrap();
        let merging = create(&mut c, "boss", "m", None, 0, None, None, None, None, 1000).unwrap();
        let done = create(&mut c, "boss", "d", None, 0, None, None, None, None, 1000).unwrap();
        let failed = create(&mut c, "boss", "f", None, 0, None, None, None, None, 1000).unwrap();
        let cancelled = create(&mut c, "boss", "x", None, 0, None, None, None, None, 1000).unwrap();

        for (id, status) in [
            (working, "working"),
            (in_review, "in-review"),
            (rework, "rework"),
            (merging, "merging"),
            (done, "done"),
            (failed, "failed"),
            (cancelled, "cancelled"),
        ] {
            c.execute(
                "UPDATE tasks SET status=?2 WHERE id=?1",
                params![id, status],
            )
            .unwrap();
        }

        // working+in-review+rework block; source, open, merging, and terminals do not.
        assert_eq!(count_started_non_terminal_excluding(&c, source).unwrap(), 3);

        // Excluding an actual started task drops its count.
        assert_eq!(
            count_started_non_terminal_excluding(&c, in_review).unwrap(),
            2
        );

        // Move the last blocker to done → predicate releases (returns 0).
        for id in [working, rework] {
            c.execute("UPDATE tasks SET status='done' WHERE id=?1", params![id])
                .unwrap();
        }
        c.execute(
            "UPDATE tasks SET status='done' WHERE id=?1",
            params![in_review],
        )
        .unwrap();
        assert_eq!(count_started_non_terminal_excluding(&c, source).unwrap(), 0);

        let _ = open_task; // silence "unused" — open row also present in DB
    }

    #[test]
    fn retry_parked_refuses_under_a_decomposition_freeze() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        // Park the task with a non-terminal resume status so restoration would
        // grow the drain-quiescence set.
        c.execute(
            "UPDATE tasks SET status='failed',refs=json_set(
                json_object(
                    'cx_est',2,'cx_size','S','cx_ready',true,
                    'cx_not_ready_reason',NULL,'cx_by','test:v2'
                ),
                '$.daemon_parked', json('true'),
                '$.daemon_parked_reason','test',
                '$.daemon_resume_status','in-review'
             ) WHERE id=?1",
            params![id],
        )
        .unwrap();

        // No freeze: retry restores to in-review.
        let restored = retry_parked(&mut c, id, "operator", true, 1001)
            .unwrap()
            .expect("parked task restored when no freeze");
        assert_eq!(restored.status, "in-review");

        // Re-park then start a freeze; retry must fail Usage.
        c.execute(
            "UPDATE tasks SET status='failed',refs=json_set(
                refs,
                '$.daemon_parked', json('true'),
                '$.daemon_resume_status','in-review'
             ) WHERE id=?1",
            params![id],
        )
        .unwrap();
        let source = create(&mut c, "boss", "src", None, 0, None, None, None, None, 1000).unwrap();
        c.execute(
            "INSERT INTO task_decompositions(source_task_id,state,active,freeze_active,
                 planned_source_revision,created_at,updated_at)
             VALUES (?1,'draining',0,1,1,1000,1000)",
            params![source],
        )
        .unwrap();
        let err = retry_parked(&mut c, id, "operator", true, 1002).unwrap_err();
        match err {
            QuorumError::Usage(msg) => {
                assert!(msg.contains("decomposition freeze"), "unexpected: {msg}")
            }
            other => panic!("unexpected error variant: {other:?}"),
        }

        // Task stays failed — non-growth invariant preserved.
        let status: String = c
            .query_row("SELECT status FROM tasks WHERE id=?1", params![id], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(status, "failed");
    }

    /// Task #473: a parked task whose depends_on contains a cancelled id
    /// cannot be silently restored — the sweep would just re-park it. The
    /// operator's disposition (dep edit or close) is required. After a
    /// depends_on edit drops the cancelled id, retry restores normally.
    #[test]
    fn retry_parked_refuses_when_depends_on_contains_cancelled() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let id = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='cancelled' WHERE id=?1",
            params![dep],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_unsatisfiable', json('true'),
                 'daemon_parked_reason', 'dependency #' || ?2 || ' is cancelled — unsatisfiable',
                 'daemon_resume_status', 'open'
             ) WHERE id=?1",
            params![id, dep],
        )
        .unwrap();

        // Guard fires: no restore, no error.
        let out = retry_parked(&mut c, id, "operator", true, 1001).unwrap();
        assert!(
            out.is_none(),
            "retry must refuse silently; CLI surfaces the cancelled dep"
        );
        let status: String = c
            .query_row("SELECT status FROM tasks WHERE id=?1", params![id], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(status, "failed");

        // Operator edits depends_on to drop the cancelled id → retry proceeds.
        c.execute("UPDATE tasks SET depends_on='[]' WHERE id=?1", params![id])
            .unwrap();
        let restored = retry_parked(&mut c, id, "operator", true, 1002)
            .unwrap()
            .expect("dep edit clears the guard");
        assert_eq!(restored.status, "open");
    }

    /// Task #473 review blocker: a successful `retry_parked` must clear
    /// `daemon_parked_unsatisfiable`. Otherwise a later generic park (via
    /// `set_parked_refs`) preserves the stale `true`, and `quorum status`
    /// reports the unrelated failure as an unsatisfiable-dep row.
    #[test]
    fn retry_parked_clears_daemon_parked_unsatisfiable_marker() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let id = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        // Park with unsatisfiable=true, then simulate the operator dropping
        // the cancelled dep from depends_on (so retry passes the guard).
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_unsatisfiable', json('true'),
                 'daemon_parked_reason', 'stale unsatisfiable park',
                 'daemon_resume_status', 'open'
             ) WHERE id=?1",
            params![id],
        )
        .unwrap();
        c.execute("UPDATE tasks SET depends_on='[]' WHERE id=?1", params![id])
            .unwrap();

        let restored = retry_parked(&mut c, id, "operator", true, 1001)
            .unwrap()
            .expect("restored to open");
        assert_eq!(restored.status, "open");
        let refs: serde_json::Value =
            serde_json::from_str(restored.refs.as_deref().unwrap_or("{}")).unwrap();
        assert!(
            refs.get(PARKED_UNSATISFIABLE_REF).is_none(),
            "successful retry must strip the unsatisfiable marker; leftover refs: {refs}"
        );
    }

    #[test]
    fn retry_parked_discards_only_rejected_new_branch_publication_intent() {
        let (_d, mut c) = open_tmp();
        for (title, pr, stage, end_reason, clears_intent) in [
            (
                "rejected new branch",
                None,
                "intent",
                "daemon_push_failed",
                true,
            ),
            (
                "pr-backed publication",
                Some(482),
                "intent",
                "daemon_push_failed",
                false,
            ),
            (
                "pushed publication",
                None,
                "pushed",
                "daemon_push_failed",
                false,
            ),
            ("crash recovery", None, "intent", "daemon_crashed", false),
        ] {
            let id = create(
                &mut c, "owner", title, None, 0, None, None, None, None, 1000,
            )
            .unwrap();
            let refs = serde_json::json!({
                "daemon_publication": {
                    "branch": "daemon/first-worker-t89",
                    "local_sha": "sha-a",
                    "pr": pr,
                    "stage": stage
                },
                "runner_continuation": {"provider": "codex", "id": "old-turn"}
            });
            update_refs_daemon(&mut c, id, &refs.to_string(), 1001).unwrap();
            c.execute(
                "UPDATE tasks
                 SET status='working', assignee='FirstWorker', author='FirstWorker'
                 WHERE id=?1",
                params![id],
            )
            .unwrap();
            c.execute(
                "INSERT INTO task_branches(task_id,branch,worktree,allocated_by,allocated_at)
                 VALUES (?1,?2,?3,'FirstWorker',1001)",
                params![
                    id,
                    format!("daemon/first-worker-t{id}"),
                    format!("/tmp/first-worker-t{id}")
                ],
            )
            .unwrap();
            let run = crate::agent_runs::insert(
                &c,
                id,
                "FirstWorker",
                "worker",
                "model",
                "high",
                "codex",
                1002,
            )
            .unwrap();
            crate::agent_runs::close(&c, run, 1003, end_reason).unwrap();
            park(
                &mut c,
                id,
                "daemon-owned publication failed: remote rejected push",
                "open",
                1004,
            )
            .unwrap();

            let retried = retry_parked(&mut c, id, "operator", true, 1005)
                .unwrap()
                .expect("parked task must retry");
            assert_eq!(retried.status, "open");
            let refs: serde_json::Value =
                serde_json::from_str(retried.refs.as_deref().unwrap_or("{}")).unwrap();
            if clears_intent {
                assert!(
                    refs.get("daemon_publication").is_none(),
                    "rejected new-branch retry must discard stale publication: {refs}"
                );
                assert!(
                    refs.get("runner_continuation").is_none(),
                    "fresh delivery must not retain the previous worker continuation: {refs}"
                );
                assert_eq!(
                    retried.author, None,
                    "fresh delivery needs a new branch owner"
                );
            } else {
                assert_eq!(
                    refs["daemon_publication"]["local_sha"], "sha-a",
                    "non-rejected or PR-backed retries must replay their exact durable source"
                );
                assert_eq!(refs["daemon_publication"]["pr"], serde_json::json!(pr));
                assert_eq!(refs["runner_continuation"]["id"], "old-turn");
                assert_eq!(retried.author.as_deref(), Some("FirstWorker"));
            }
            let branch_allocations: i64 = c
                .query_row(
                    "SELECT COUNT(*) FROM task_branches WHERE task_id=?1",
                    params![id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(branch_allocations, i64::from(!clears_intent));
        }
    }

    /// Task #473 R4 defense: `retry_parked`'s policy branch keeps
    /// `status='failed'` (a policy retry is a reclassification request),
    /// so a stale `daemon_parked_unsatisfiable=true` from any prior path
    /// would persist across the retry and keep a false unsatisfiable row
    /// in status BLOCKED. The policy branch must strip the marker.
    #[test]
    fn retry_parked_policy_branch_clears_unsatisfiable_marker() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "policy-parked",
            None,
            0,
            None,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_reason', 'classifier declined',
                 'daemon_resume_status', 'open',
                 'classifier_policy_parked', json('true'),
                 'daemon_parked_unsatisfiable', json('true'),
                 'cx_est', 3
             ) WHERE id=?1",
            params![id],
        )
        .unwrap();
        let restored = retry_parked(&mut c, id, "operator", true, 1001)
            .unwrap()
            .expect("policy retry succeeds");
        assert_eq!(restored.status, "failed");
        let refs: serde_json::Value =
            serde_json::from_str(restored.refs.as_deref().unwrap()).unwrap();
        assert!(
            refs.get(PARKED_UNSATISFIABLE_REF).is_none(),
            "policy retry must strip the stale unsatisfiable marker: {refs}"
        );
        assert_eq!(refs[CLASSIFIER_POLICY_PARKED_REF], true);
        assert!(refs.get("cx_est").is_none());
    }

    /// Task #473 review blocker: `set_parked_refs` is the shared builder for
    /// every generic park path. It must NOT carry forward a stale
    /// `daemon_parked_unsatisfiable=true` from a prior park — only the
    /// dependency-sweep path is authorized to set that marker.
    #[test]
    fn generic_park_clears_stale_daemon_parked_unsatisfiable_marker() {
        let stale = r#"{"daemon_parked_unsatisfiable": true, "some": "context"}"#;
        let out = set_parked_refs(Some(stale), "generic failure", "rework").unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert!(
            parsed.get(PARKED_UNSATISFIABLE_REF).is_none(),
            "generic park must clear stale unsatisfiable marker: {parsed}"
        );
        assert_eq!(parsed[PARKED_REF], true);
        assert_eq!(parsed[PARKED_REASON_REF], "generic failure");
        assert_eq!(parsed[PARKED_RESUME_STATUS_REF], "rework");
        assert_eq!(parsed["some"], "context");
    }

    /// Task #473 R6 blocker 1: convergence must run at the cancellation
    /// transition, not later. `converge_parked_dependents_of_cancelled`
    /// upgrades the durable refs of every non-policy parked dependent of a
    /// just-cancelled task. Any subsequent status read sees the upgrade
    /// without waiting for another mutation to trigger a sweep.
    #[test]
    fn converge_parked_dependents_of_cancelled_upgrades_stale_park() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let child = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        // Recoverable park (dep is failed) — matches the primary cascade shape.
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_reason', 'dependency #' || ?2 || ' is terminal-not-done',
                 'daemon_parked_unsatisfiable', json('false'),
                 'daemon_resume_status', 'open'
             ) WHERE id=?1",
            params![child, dep],
        )
        .unwrap();
        // Now cancel the dep and run convergence in the same transaction.
        c.execute(
            "UPDATE tasks SET status='cancelled' WHERE id=?1",
            params![dep],
        )
        .unwrap();
        let n = converge_parked_dependents_of_cancelled(&c, dep, 2000).unwrap();
        assert_eq!(n, 1);
        let refs: serde_json::Value =
            serde_json::from_str(get(&c, child).unwrap().unwrap().refs.as_deref().unwrap())
                .unwrap();
        assert_eq!(refs["daemon_parked_unsatisfiable"], true);
        assert_eq!(
            refs["daemon_parked_reason"],
            format!("dependency #{dep} is cancelled — unsatisfiable")
        );
        // Idempotent: a second call finds nothing.
        assert_eq!(
            converge_parked_dependents_of_cancelled(&c, dep, 2001).unwrap(),
            0
        );
    }

    /// Task #473 R6: convergence must NOT overwrite the "classifier
    /// declined" reason of a policy park. Refs stay owned by the classifier
    /// path; `stats::blocked_tasks` surfaces the row via live dep-graph
    /// inference so the disposition signal is still present.
    #[test]
    fn converge_parked_dependents_of_cancelled_skips_policy_park() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let policy_park = create(
            &mut c,
            "boss",
            "policy-parked",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_reason', 'classifier declined',
                 'daemon_resume_status', 'open',
                 'classifier_policy_parked', json('true')
             ) WHERE id=?1",
            params![policy_park],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='cancelled' WHERE id=?1",
            params![dep],
        )
        .unwrap();
        assert_eq!(
            converge_parked_dependents_of_cancelled(&c, dep, 2000).unwrap(),
            0
        );
        let refs: serde_json::Value = serde_json::from_str(
            get(&c, policy_park)
                .unwrap()
                .unwrap()
                .refs
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        assert!(refs.get("daemon_parked_unsatisfiable").is_none());
        assert_eq!(refs["daemon_parked_reason"], "classifier declined");
    }

    /// Task #473 R7 blocker 3: the cancellation convergence hook must
    /// only fire when the cancel UPDATE actually applied. A combined
    /// status=cancelled + depends_on edit on a `failed` daemon-parked
    /// task has the cancel UPDATE affect zero rows (WHERE clause excludes
    /// failed) but the guarded depends_on branch may still succeed. The
    /// hook keying off `fields.status == Some("cancelled")` alone would
    /// falsely relabel the task's dependents as unsatisfiable even
    /// though the task itself is not cancelled.
    #[test]
    fn cancel_hook_gated_on_actual_transition_not_requested_status() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            1000,
        )
        .unwrap();
        // Park dependent recoverably on `dep`.
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_reason', 'dependency #' || ?2 || ' is terminal-not-done',
                 'daemon_parked_unsatisfiable', json('false'),
                 'daemon_resume_status', 'open'
             ) WHERE id=?1",
            params![dependent, dep],
        )
        .unwrap();
        // `dep` is itself failed + daemon-parked (guarded depends_on edit
        // path allows edits here).
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_reason', 'some failure',
                 'daemon_resume_status', 'open'
             ) WHERE id=?1",
            params![dep],
        )
        .unwrap();
        // Combined edit: requested status='cancelled' + depends_on edit.
        // The cancel UPDATE affects zero rows (dep is failed); the
        // depends_on UPDATE fires under the guarded failed+parked branch.
        let _ = update(
            &mut c,
            "boss",
            dep,
            &TaskUpdate {
                status: Some("cancelled"),
                depends_on: Some("[]"),
                expected_revision: Some(1),
                ..Default::default()
            },
            1002,
        );
        // dep must NOT have moved to cancelled.
        let dep_status: String = c
            .query_row("SELECT status FROM tasks WHERE id=?1", params![dep], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(dep_status, "failed");
        // Dependent's park refs must NOT have been relabeled.
        let refs: serde_json::Value = serde_json::from_str(
            get(&c, dependent)
                .unwrap()
                .unwrap()
                .refs
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(refs["daemon_parked_unsatisfiable"], false);
        assert!(
            refs["daemon_parked_reason"]
                .as_str()
                .unwrap()
                .contains("terminal-not-done"),
            "unexpected reason: {}",
            refs["daemon_parked_reason"]
        );
    }

    /// Task #473 final blockers: cancellation examines a raw-ID page rather
    /// than scanning to `LIMIT` matches, and ordinary write-side sweeping
    /// durably converges dependents beyond that page without a second helper
    /// or cancellation call.
    #[test]
    fn cancelled_dependency_reconciliation_is_bounded_and_continues_in_production() {
        let (_d, mut c) = open_tmp();
        // Retained no-match history precedes both the dependency and its
        // dependents. A post-filter LIMIT would scan all of this during the
        // cancellation transaction; the cursor page examines exactly the
        // first CONVERGE_LIMIT primary-key rows instead.
        for i in 0..(CONVERGE_LIMIT * 2) {
            create(
                &mut c,
                "boss",
                &format!("history-{i}"),
                None,
                0,
                None,
                None,
                None,
                None,
                1000,
            )
            .unwrap();
        }
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, None, 1000).unwrap();
        let extra = CONVERGE_LIMIT + 5;
        let mut ids = Vec::new();
        for i in 0..extra {
            let id = create(
                &mut c,
                "boss",
                &format!("d{i}"),
                None,
                0,
                None,
                None,
                Some(&format!("[{dep}]")),
                None,
                1000,
            )
            .unwrap();
            c.execute(
                "UPDATE tasks SET status='failed', refs=json_object(
                     'daemon_parked', json('true'),
                     'daemon_parked_reason', 'dependency #' || ?2 || ' is terminal-not-done',
                     'daemon_parked_unsatisfiable', json('false'),
                     'daemon_resume_status', 'open'
                 ) WHERE id=?1",
                params![id, dep],
            )
            .unwrap();
            ids.push(id);
        }
        update(
            &mut c,
            "boss",
            dep,
            &TaskUpdate {
                status: Some("cancelled"),
                expected_revision: Some(1),
                ..Default::default()
            },
            2000,
        )
        .unwrap();
        let cursor: i64 = c
            .query_row(
                "SELECT task_cursor FROM cancelled_dependency_reconciliation
                 WHERE cancelled_task_id=?1",
                [dep],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(cursor, CONVERGE_LIMIT as i64);
        assert!(ids.iter().all(|id| {
            let refs: serde_json::Value =
                serde_json::from_str(get(&c, *id).unwrap().unwrap().refs.as_deref().unwrap())
                    .unwrap();
            refs["daemon_parked_unsatisfiable"] == false
        }));

        // `create` is a normal mutation and therefore invokes
        // sweep_on_write. Repeated production writes drain the durable
        // cursor through history and then through every dependent page.
        for i in 0..4 {
            create(
                &mut c,
                "boss",
                &format!("sweep-trigger-{i}"),
                None,
                0,
                None,
                None,
                None,
                None,
                2001 + i as i64,
            )
            .unwrap();
        }
        let queued: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM cancelled_dependency_reconciliation
                 WHERE cancelled_task_id=?1",
                [dep],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(queued, 0, "production sweeps must drain the queue");
        let remaining: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM tasks
                 WHERE status='failed' AND json_valid(refs)
                   AND json_extract(refs,'$.daemon_parked')=1
                   AND COALESCE(json_extract(refs,'$.daemon_parked_unsatisfiable'),0)!=1
                   AND EXISTS(SELECT 1 FROM json_each(depends_on) j WHERE j.value=?1)",
                params![dep],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 0);

        let plan: Vec<String> = c
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT id, status, depends_on, refs FROM tasks
                 WHERE id > ?1 ORDER BY id LIMIT ?2",
            )
            .unwrap()
            .query_map(params![0, CONVERGE_LIMIT as i64], |row| row.get(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|detail| detail.contains("INTEGER PRIMARY KEY (rowid>?)")),
            "cursor page must use the task primary key: {plan:?}"
        );
    }

    #[test]
    fn cancelled_dep_ids_lists_only_cancelled_deps() {
        let (_d, mut c) = open_tmp();
        let a = create(&mut c, "boss", "a", None, 0, None, None, None, None, 1).unwrap();
        let b = create(&mut c, "boss", "b", None, 0, None, None, None, None, 1).unwrap();
        let d = create(&mut c, "boss", "d", None, 0, None, None, None, None, 1).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{a},{b},{d}]")),
            None,
            1,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='cancelled' WHERE id=?1",
            params![a],
        )
        .unwrap();
        c.execute("UPDATE tasks SET status='failed' WHERE id=?1", params![b])
            .unwrap();
        c.execute("UPDATE tasks SET status='done' WHERE id=?1", params![d])
            .unwrap();
        let ids = cancelled_dep_ids(&c, dependent).unwrap();
        assert_eq!(ids, vec![a]);
    }

    // ── target_branch ────────────────────────────────────────────────────────

    #[test]
    fn created_task_has_null_target_branch() {
        let (_dir, mut conn) = open_tmp();
        let id = create(&mut conn, "a", "t", None, 0, None, None, None, None, 1).unwrap();
        let task = get(&conn, id).unwrap().unwrap();
        assert!(task.target_branch.is_none());
    }

    #[test]
    fn target_branch_validation_rejects_control_nul_and_oversized_inputs() {
        for branch in ["main\0next", "main\u{1f}next", "@"] {
            assert!(
                validate_target_branch(branch).is_err(),
                "must reject {branch:?}"
            );
        }
        assert!(validate_target_branch(&"a".repeat(MAX_TARGET_BRANCH_BYTES + 1)).is_err());
    }

    #[test]
    fn create_with_target_branch_persists_it_atomically() {
        let (_dir, mut conn) = open_tmp();
        let id = create_with_continue_pr_and_target_branch(
            &mut conn,
            "a",
            "t",
            None,
            0,
            None,
            None,
            None,
            None,
            None,
            Some("develop"),
            1,
        )
        .unwrap();
        assert!(!resolve_target_branch(&mut conn, id, "main", 2).unwrap());
        assert_eq!(
            get(&conn, id).unwrap().unwrap().target_branch.as_deref(),
            Some("develop")
        );
    }

    #[test]
    fn resolve_target_branch_sets_once() {
        let (_dir, mut conn) = open_tmp();
        let id = create(&mut conn, "a", "t", None, 0, None, None, None, None, 1).unwrap();
        assert!(resolve_target_branch(&mut conn, id, "main", 2).unwrap());
        let task = get(&conn, id).unwrap().unwrap();
        assert_eq!(task.target_branch.as_deref(), Some("main"));
        assert_eq!(task.updated_at, 2);
    }

    #[test]
    fn resolve_target_branch_immutable_once_populated() {
        let (_dir, mut conn) = open_tmp();
        let id = create(&mut conn, "a", "t", None, 0, None, None, None, None, 1).unwrap();
        assert!(resolve_target_branch(&mut conn, id, "main", 2).unwrap());
        assert!(!resolve_target_branch(&mut conn, id, "develop", 3).unwrap());
        let task = get(&conn, id).unwrap().unwrap();
        assert_eq!(task.target_branch.as_deref(), Some("main"));
        assert_eq!(task.updated_at, 2);
    }

    #[test]
    fn resolve_target_branch_missing_task() {
        let (_dir, mut conn) = open_tmp();
        assert!(!resolve_target_branch(&mut conn, 999, "main", 1).unwrap());
    }

    #[test]
    fn target_branch_survives_lifecycle() {
        let (_dir, mut conn) = open_tmp();
        let id = create(&mut conn, "a", "t", None, 0, None, None, None, None, 1).unwrap();
        assert!(resolve_target_branch(&mut conn, id, "develop", 2).unwrap());
        claim(&mut conn, "w", Some(id), &[], TTL, 3).unwrap();
        let task = get(&conn, id).unwrap().unwrap();
        assert_eq!(task.target_branch.as_deref(), Some("develop"));
        assert_eq!(task.status, "working");
    }

    #[test]
    fn target_branch_in_brief() {
        let (_dir, mut conn) = open_tmp();
        let id = create(&mut conn, "a", "t", None, 0, None, None, None, None, 1).unwrap();
        assert!(resolve_target_branch(&mut conn, id, "main", 2).unwrap());
        let task = get(&conn, id).unwrap().unwrap();
        let brief = TaskBrief::from(&task);
        assert_eq!(brief.target_branch.as_deref(), Some("main"));
    }

    #[test]
    fn stamp_rework_cap_sets_once_and_defaults_before() {
        let (_dir, mut conn) = open_tmp();
        let id = create(&mut conn, "a", "t", None, 0, None, None, None, None, 1).unwrap();
        // Unstamped: NULL column, effective cap is the compiled default.
        let task = get(&conn, id).unwrap().unwrap();
        assert_eq!(task.rework_cap, None);
        assert_eq!(task.effective_rework_cap(), crate::lifecycle::REWORK_CAP);

        assert!(stamp_rework_cap(&mut conn, id, 10, 2).unwrap());
        let task = get(&conn, id).unwrap().unwrap();
        assert_eq!(task.rework_cap, Some(10));
        assert_eq!(task.effective_rework_cap(), 10);
        assert_eq!(task.updated_at, 2);
    }

    #[test]
    fn stamp_rework_cap_immutable_once_populated() {
        let (_dir, mut conn) = open_tmp();
        let id = create(&mut conn, "a", "t", None, 0, None, None, None, None, 1).unwrap();
        assert!(stamp_rework_cap(&mut conn, id, 10, 2).unwrap());
        // A second stamp is a no-op: the cap is frozen at first adoption.
        assert!(!stamp_rework_cap(&mut conn, id, 12, 3).unwrap());
        let task = get(&conn, id).unwrap().unwrap();
        assert_eq!(task.rework_cap, Some(10));
        assert_eq!(task.updated_at, 2);
    }

    #[test]
    fn stamp_rework_cap_missing_task() {
        let (_dir, mut conn) = open_tmp();
        assert!(!stamp_rework_cap(&mut conn, 999, 10, 1).unwrap());
    }

    #[test]
    fn target_branch_concurrent_resolve() {
        let contenders = 8;
        for round in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("q.db");
            {
                let mut conn = crate::db::open(&db_path).unwrap();
                create(&mut conn, "a", "t", None, 0, None, None, None, None, 1).unwrap();
            }
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(contenders));
            let handles: Vec<_> = (0..contenders)
                .map(|i| {
                    let path = db_path.clone();
                    let barrier = std::sync::Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let mut conn = crate::db::open(&path).unwrap();
                        barrier.wait();
                        resolve_target_branch(&mut conn, 1, &format!("branch-{i}"), 100 + i as i64)
                            .unwrap()
                    })
                })
                .collect();
            let outcomes: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            assert_eq!(
                outcomes.iter().filter(|&&won| won).count(),
                1,
                "round {round}: exactly one resolver must win: {outcomes:?}"
            );
            assert_eq!(outcomes.iter().filter(|&&won| !won).count(), contenders - 1);
            let conn = crate::db::open(&db_path).unwrap();
            let task = get(&conn, 1).unwrap().unwrap();
            assert!(
                task.target_branch
                    .as_deref()
                    .unwrap()
                    .starts_with("branch-"),
                "round {round}: durable value must be from one contender: {:?}",
                task.target_branch,
            );
        }
    }

    #[test]
    fn legacy_migration_adds_target_branch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch("PRAGMA journal_mode=WAL").unwrap();
            conn.execute_batch(
                "CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    body TEXT,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    labels TEXT,
                    assignee TEXT,
                    created_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    refs TEXT,
                    depends_on TEXT,
                    sticky_until INTEGER,
                    orig TEXT,
                    author TEXT,
                    reviewer TEXT,
                    rework_round INTEGER NOT NULL DEFAULT 0,
                    review_only INTEGER NOT NULL DEFAULT 0,
                    recovery_attempts INTEGER NOT NULL DEFAULT 0,
                    revision INTEGER NOT NULL DEFAULT 1,
                    edit_count INTEGER NOT NULL DEFAULT 0,
                    continue_pr INTEGER,
                    completion_provenance TEXT
                );
                PRAGMA user_version = 52;",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO tasks(title, status, created_by, created_at, updated_at)
                 VALUES ('old', 'open', 'x', 1, 1)",
                [],
            )
            .unwrap();
        }
        let conn = crate::db::open(&path).unwrap();
        let v: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, crate::db::SCHEMA_VERSION);
        let task = get(&conn, 1).unwrap().unwrap();
        assert!(task.target_branch.is_none());
        // v56: the added rework_cap column is nullable; legacy rows stay NULL and
        // fall back to the compiled cap, preserving historic behaviour.
        assert!(task.rework_cap.is_none());
        assert_eq!(task.effective_rework_cap(), crate::lifecycle::REWORK_CAP);
        assert_eq!(task.title, "old");
    }
}
