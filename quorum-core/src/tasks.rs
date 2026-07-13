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
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;

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

/// Body prefix for all daemon-parked tasks. Any cancelled task whose body starts with this
/// prefix is reopenable by creator or (former) assignee (#182).
pub const PARKED_BODY_PREFIX: &str = "daemon:parked:";

/// Body marker for review-only tasks whose approved PR failed to merge (e.g. conflicts).
/// The daemon's orphan-in-review handler detects this and retries merge when the PR
/// becomes MERGEABLE again.
pub const MERGE_BLOCKED_BODY: &str = "daemon:merge-blocked";

const KNOWN_TIERS: &[&str] = &["opus-46", "opus-47", "opus-48", "sonnet-5"];
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
    pub ready: bool,
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
}

#[derive(Default)]
pub struct TaskUpdate<'a> {
    pub status: Option<&'a str>,
    pub body: Option<&'a str>,
    pub refs: Option<&'a str>,
    pub verdict: Option<&'a str>,
    pub depends_on: Option<&'a str>,
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
                    rework_round, review_only";

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
            if !tier.is_empty() && !KNOWN_TIERS.contains(&tier) {
                eprintln!(
                    "warn: unrecognized tier '{tier}' in --labels (known: {}); \
                     serve will fall back to the global default model",
                    KNOWN_TIERS.join(", ")
                );
            }
        }
        if let Some(effort) = label.strip_prefix("effort:") {
            if !effort.is_empty() && !KNOWN_EFFORTS.contains(&effort) {
                return Err(QuorumError::Usage(format!(
                    "invalid effort '{effort}' in --labels; only {} are accepted \
                     (serve rejects anything else at dispatch)",
                    KNOWN_EFFORTS.join(", ")
                )));
            }
        }
        if let Some(complexity) = label.strip_prefix("complexity:") {
            if !complexity.is_empty() && !KNOWN_COMPLEXITIES.contains(&complexity) {
                return Err(QuorumError::Usage(format!(
                    "invalid complexity '{complexity}' in --labels; only {} are accepted \
                     (1=mechanical one-liner, 2=single-file, 3=multi-file, 4=cross-module, 5=cross-cutting)",
                    KNOWN_COMPLEXITIES.join(", ")
                )));
            }
        }
    }
    Ok(())
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

// ── create ────────────────────────────────────────────────────────────────────

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
    crate::agents::touch(&tx, created_by, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.execute(
        "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, \
         created_at, updated_at, refs, depends_on, review_only) \
         VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?7, ?8, ?9, ?10)",
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
            review_only
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

    const DEP_READY_CLAUSE: &str = "(depends_on IS NULL OR NOT EXISTS (
        SELECT 1 FROM json_each(depends_on) je
        WHERE NOT EXISTS (
            SELECT 1 FROM tasks d WHERE d.id = je.value AND d.status = 'done'
        )
    ))";

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
                     WHERE id = ?3 AND (
                         (status='open' AND {DEP_READY_CLAUSE})
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
                "SELECT id FROM tasks WHERE (
                    (status='open' AND {DEP_READY_CLAUSE})
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
            selector.push_str(" ORDER BY priority DESC, id ASC LIMIT 1");

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

// ── apply_event ───────────────────────────────────────────────────────────────

pub fn apply_event(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    event: &Event,
    now: i64,
) -> Result<TransitionResult> {
    let tx = begin_immediate(conn)?;
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
        Event::SignaledDone { .. } | Event::ReworkPushed => {
            if task.author.as_deref() != Some(agent) {
                tx.commit()?;
                return Err(QuorumError::NotHolder);
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
        | Event::LeaseExpired
        | Event::AgentFailed { .. }
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
        pr: extract_pr_from_refs(&task.refs),
        review_only: task.review_only,
    };

    let (new_status, effects) = crate::lifecycle::transition(&view, event)
        .map_err(|e| QuorumError::Usage(e.to_string()))?;

    let new_status_str = new_status.to_string();
    let mut author = task.author.clone();
    let mut reviewer = task.reviewer.clone();
    let mut rework_round = task.rework_round;
    let mut assignee = task.assignee.clone();
    let mut refs = task.refs.clone();

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
            _ => {}
        }
    }

    if let Event::SignaledDone { pr } = event {
        refs = Some(merge_pr_into_refs(&task.refs, pr));
    }

    tx.execute(
        "UPDATE tasks SET status=?1, assignee=?2, author=?3, reviewer=?4, \
         rework_round=?5, refs=?6, updated_at=?7 WHERE id=?8",
        params![
            new_status_str,
            assignee,
            author,
            reviewer,
            rework_round,
            refs,
            now,
            id
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

// ── set_body (daemon post-event body annotation) ─────────────────────────────

pub fn set_body(conn: &mut Connection, id: i64, body: &str, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE tasks SET body=?1, updated_at=?2 WHERE id=?3",
        params![body, now, id],
    )?;
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
    if let Some(s) = fields.status {
        if !STATUSES.contains(&s) {
            return Err(QuorumError::Usage(format!("invalid status: {s}")));
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

    let n = match fields.status {
        Some("open") => {
            let rows = tx.execute(
                "UPDATE tasks SET
                    status='open', assignee=NULL,
                    body  = COALESCE(?3, body),
                    refs  = COALESCE(?4, refs),
                    updated_at = ?5
                 WHERE id=?1 AND assignee=?6 AND status='working'",
                params![id, "open", fields.body, fields.refs, now, agent],
            )?;
            if rows == 0 {
                tx.execute(
                    "UPDATE tasks SET
                        status='open', assignee=NULL,
                        body  = COALESCE(?3, body),
                        refs  = COALESCE(?4, refs),
                        updated_at = ?5
                     WHERE id=?1 AND (created_by=?6 OR assignee=?6)
                           AND status='cancelled'",
                    params![id, "open", fields.body, fields.refs, now, agent],
                )?
            } else {
                rows
            }
        }
        Some("cancelled") => tx.execute(
            "UPDATE tasks SET
                status='cancelled',
                body  = COALESCE(?3, body),
                refs  = COALESCE(?4, refs),
                updated_at = ?5
             WHERE id=?1 AND (created_by=?6 OR assignee=?6)
                   AND status NOT IN ('done', 'failed', 'cancelled')",
            params![id, "cancelled", fields.body, fields.refs, now, agent],
        )?,
        _ => {
            let rows = tx.execute(
                "UPDATE tasks SET
                    status   = COALESCE(?2, status),
                    body     = COALESCE(?3, body),
                    refs     = COALESCE(?4, refs),
                    updated_at = ?5
                 WHERE id=?1 AND assignee=?6 AND status='working'",
                params![id, fields.status, fields.body, fields.refs, now, agent],
            )?;
            if rows == 0 && fields.status.is_none() {
                tx.execute(
                    "UPDATE tasks SET
                        body     = COALESCE(?2, body),
                        refs     = COALESCE(?3, refs),
                        updated_at = ?4
                     WHERE id=?1 AND created_by=?5 AND assignee IS NULL AND status='open'",
                    params![id, fields.body, fields.refs, now, agent],
                )?
            } else {
                rows
            }
        }
    };
    if n == 0 && fields.depends_on.is_none() {
        tx.commit()?;
        return Err(QuorumError::NotHolder);
    }

    if let Some(dep_json) = fields.depends_on {
        let dep_rows = tx.execute(
            "UPDATE tasks SET depends_on=?2, updated_at=?3
             WHERE id=?1 AND (created_by=?4 OR assignee=?4)
                   AND status NOT IN ('done', 'failed')",
            params![id, dep_json, now, agent],
        )?;
        if dep_rows == 0 && n == 0 {
            tx.commit()?;
            return Err(QuorumError::NotHolder);
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
    } else if fields.status == Some("cancelled") {
        deactivate_lease(&tx, id, now)?;
        crate::events::emit(
            &tx,
            "task_cancelled",
            &lease_target(id),
            &format!("cancelled by {agent}"),
            now,
        )?;
    } else if fields.status == Some("done") {
        deactivate_lease(&tx, id, now)?;
        crate::events::emit(
            &tx,
            "task_done",
            &lease_target(id),
            &format!("done by {agent}"),
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
    Ok(task)
}

// ── close_after_merge ─────────────────────────────────────────────────────────

pub fn close_after_merge(conn: &mut Connection, id: i64, note: &str, now: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let n = tx.execute(
        "UPDATE tasks SET status='done', assignee=NULL,
            body=?2, updated_at=?3
         WHERE id=?1 AND status NOT IN ('done', 'failed', 'cancelled')",
        params![id, note, now],
    )?;
    if n == 0 {
        tx.commit()?;
        return Ok(false);
    }
    deactivate_lease(&tx, id, now)?;
    crate::events::emit(
        &tx,
        "task_done",
        &lease_target(id),
        &format!("closed on merge (recovery): {note}"),
        now,
    )?;
    tx.commit()?;
    Ok(true)
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
    Ok(Some(TaskDetail { task, notes }))
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
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
        params![task_id],
        |r| r.get(0),
    )?;
    if !exists {
        tx.commit()?;
        return Ok(None);
    }
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, ?3, ?4)",
        params![task_id, now, agent, body],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(Some(id))
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
    fn review_only_verdict_changes_fails() {
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
        assert_eq!(r.task.status, "failed");
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
    fn update_done_from_working() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let t = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1001,
        )
        .unwrap();
        assert_eq!(t.status, "done");
        assert!(!has_live_lease(&c, id, 1001));
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
                status: Some("done"),
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
    fn reopen_parked_task() {
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
                ..Default::default()
            },
            1001,
        )
        .unwrap();
        let t = release(&mut c, "A", id, 1002).unwrap();
        assert_eq!(t.status, "open");
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
        update(
            &mut c,
            "W",
            dep,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1001,
        )
        .unwrap();
        let t = claim(&mut c, "A", Some(child), &[], TTL, 1002)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "working");
    }

    #[test]
    fn resurrect_cancelled_dep_unblocks_child() {
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
        // Resurrect the dep (creator reopens — no parked prefix needed)
        let reopened_dep = release(&mut c, "boss", dep, 1003).unwrap();
        assert_eq!(reopened_dep.status, "open");
        // Also resurrect child (cascade_dead_deps may have cancelled it on the
        // previous write; in the live system it fires on the next daemon tick)
        let child_task = get(&c, child).unwrap().unwrap();
        if child_task.status == "cancelled" {
            release(&mut c, "boss", child, 1003).unwrap();
        }
        // Complete the dep
        claim(&mut c, "W", Some(dep), &[], TTL, 1004)
            .unwrap()
            .unwrap();
        close_after_merge(&mut c, dep, "merged", 1005).unwrap();
        // Now child is unblocked
        let t = claim(&mut c, "A", Some(child), &[], TTL, 1006)
            .unwrap()
            .expect("child should be claimable now that dep is done");
        assert_eq!(t.status, "working");
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
        // Clear child's deps (works even on cancelled tasks)
        let updated = update(
            &mut c,
            "boss",
            child,
            &TaskUpdate {
                depends_on: Some("[]"),
                ..Default::default()
            },
            1002,
        )
        .unwrap();
        assert!(updated.ready);
        assert_eq!(updated.depends_on.as_deref(), Some("[]"));
        // If cascade cancelled the child, resurrect it (now safe — no dead deps)
        if updated.status == "cancelled" {
            let reopened = release(&mut c, "boss", child, 1003).unwrap();
            assert_eq!(reopened.status, "open");
        }
        // Now claimable
        let t = claim(&mut c, "A", Some(child), &[], TTL, 1004)
            .unwrap()
            .expect("child with cleared deps should be claimable");
        assert_eq!(t.status, "working");
    }

    #[test]
    fn reopen_cancelled_task_without_parked_prefix() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        cancel(&mut c, "A", id, 1001).unwrap();
        // Reopen by creator — no parked prefix required
        let t = release(&mut c, "boss", id, 1002).unwrap();
        assert_eq!(t.status, "open");
        assert!(t.assignee.is_none());
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
    fn close_after_merge_idempotent() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "t", None, 0, None, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        close_after_merge(&mut c, id, "merged", 1001).unwrap();
        let changed = close_after_merge(&mut c, id, "merged", 1002).unwrap();
        assert!(!changed, "already done — should be idempotent");
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
        assert!(alert
            .refs
            .as_deref()
            .unwrap()
            .contains(&format!("task:{id}")));
    }

    #[test]
    fn review_only_verdict_changes_posts_findings_note() {
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
        assert_eq!(r.task.status, "failed");
        assert!(r.effects.contains(&Effect::PostFindingsNote));

        let detail = get_with_notes(&c, id).unwrap().unwrap();
        let note = detail
            .notes
            .iter()
            .find(|n| n.body.contains("review-only"))
            .expect("findings note missing");
        assert!(note.body.contains("requested changes"));
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
    fn metadata_only_update() {
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
                ..Default::default()
            },
            1001,
        )
        .unwrap();
        assert_eq!(t.body.as_deref(), Some("new body"));
        assert_eq!(t.refs.as_deref(), Some(r#"{"pr":42}"#));
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
    fn validate_labels_accepts_known_efforts() {
        assert!(validate_labels(r#"["effort:medium"]"#).is_ok());
        assert!(validate_labels(r#"["effort:high"]"#).is_ok());
        assert!(validate_labels(r#"["tier:opus-46","effort:medium"]"#).is_ok());
    }

    #[test]
    fn validate_labels_rejects_effort_low() {
        let err = validate_labels(r#"["effort:low"]"#).unwrap_err();
        assert!(
            matches!(&err, QuorumError::Usage(m) if m.contains("invalid effort 'low'")),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_labels_rejects_effort_max() {
        assert!(validate_labels(r#"["effort:max"]"#).is_err());
    }

    #[test]
    fn create_rejects_effort_low_at_task_create() {
        let (_d, mut c) = open_tmp();
        let err = create(
            &mut c,
            "boss",
            "bad-effort",
            None,
            0,
            Some(r#"["effort:low"]"#),
            None,
            None,
            None,
            1000,
        )
        .unwrap_err();
        assert!(
            matches!(&err, QuorumError::Usage(m) if m.contains("invalid effort")),
            "task-create must reject effort:low, got {err:?}"
        );
    }

    #[test]
    fn validate_labels_accepts_known_complexities() {
        assert!(validate_labels(r#"["complexity:1"]"#).is_ok());
        assert!(validate_labels(r#"["complexity:3"]"#).is_ok());
        assert!(validate_labels(r#"["complexity:5"]"#).is_ok());
        assert!(validate_labels(r#"["tier:opus-46","effort:medium","complexity:2"]"#).is_ok());
    }

    #[test]
    fn validate_labels_rejects_invalid_complexity() {
        let err = validate_labels(r#"["complexity:0"]"#).unwrap_err();
        assert!(
            matches!(&err, QuorumError::Usage(m) if m.contains("invalid complexity '0'")),
            "got {err:?}"
        );
        assert!(validate_labels(r#"["complexity:6"]"#).is_err());
        assert!(validate_labels(r#"["complexity:easy"]"#).is_err());
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
}
