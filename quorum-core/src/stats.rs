//! Read-only health snapshot for `quorum status`. Every count applies the same logical
//! `expires_at > now` / presence read-filter as the rest of the system, so a snapshot never
//! reports expired rows or stale-as-online agents.
//!
//! Issue #77 enriched the snapshot into an operator dashboard:
//! - `agents` — per-online-agent view with derived tier + current task + last-seen age.
//! - `queue_by_tier` — open-task count grouped by `tier:*` label (untiered + review bucket).
//! - `recent_messages` — last 5 messages (from/kind/age/preview).
//! - `claim_ttls` — active claims with time-to-expiry.
//! - `throughput` — closed-last-hour + oldest-done-awaiting-review (catches review-loop stalls).

use crate::error::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

/// How many recent messages to surface on `status`. Bounded to keep the output cheap.
pub const RECENT_MSG_LIMIT: i64 = 5;
/// Body-preview length per recent message. Beyond this is truncated with an ellipsis.
pub const MSG_PREVIEW_CHARS: usize = 80;
/// A `done` task older than this is "stuck awaiting review" — surfaces stalled review loops.
pub const DONE_STUCK_THRESHOLD_SECS: i64 = 30 * 60;

/// Per-status task count.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

/// A recent error row.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ErrorRow {
    pub ts: i64,
    pub source: String,
    pub detail: String,
}

/// One online agent — what tier they operate at and what they're doing right now.
/// Tier is read from the persisted `agents.tier` column, set on each `sync --match-label
/// tier:*` call (#82). `unknown` when the agent has never synced with a tier label.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentView {
    pub id: String,
    pub tier: String,
    /// `Some({id,title})` if holding an active task; `None` = idle.
    pub current_task: Option<AgentCurrentTask>,
    /// Seconds since `last_seen`.
    pub last_seen_age_secs: i64,
    /// Issue #97 scoreboard: cumulative tasks the agent has reached `done`/`closed` on.
    /// Same accounting as `AgentLoadScore.tasks_completed` for this agent.
    pub tasks_completed: i64,
    /// Issue #97 scoreboard: cumulative active seconds across those completed tasks.
    /// Same accounting as `AgentLoadScore.total_active_secs` for this agent.
    pub total_active_secs: i64,
    /// Issue #97 retirement state: `active` / `retiring` / `retired`.
    pub retire_status: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentCurrentTask {
    pub id: i64,
    pub title: String,
}

/// Claimable-task count grouped by required tier label. `tier` is either a `tier:*` value
/// (e.g. `tier:opus-47`), `untiered` (open tasks with no `tier:` label), or `review`
/// (open `kind:review` tasks — they're tier-exempt and routed separately, see #73 fix).
///
/// Only counts `ready=true` tasks (deps satisfied) — blocked tasks appear in
/// [`Stats::blocked`] instead (#86).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TierQueueCount {
    pub tier: String,
    pub open: i64,
}

/// A task blocked by unmet dependencies, with the chain of blocking task ids.
/// Rendered in the `## blocked` section of `quorum status` (#86).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct BlockedTask {
    pub id: i64,
    pub title: String,
    pub waiting_on: Vec<i64>,
}

/// A recent feed message — last N rows, oldest-first within the window.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RecentMessage {
    pub seq: i64,
    pub ts: i64,
    pub age_secs: i64,
    pub author: String,
    pub kind: String,
    pub body_preview: String,
}

/// An active claim with time-to-expiry. Negative `expires_in_secs` means already-lapsed
/// (the reaper will clean it on the next sweep); flag in the renderer.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ClaimTtl {
    pub target: String,
    pub holder: String,
    pub expires_in_secs: i64,
}

/// One retired agent — surfaces in the dedicated `retired_agents` dashboard section
/// (issue #97) so the operator sees capacity drop in real time and knows when to re-spin.
/// Sorted by `retired_at` DESC (newest first); ties broken by `id` ascending.
///
/// `retired_age_secs` is computed against the same `now` the rest of the snapshot uses,
/// so the dashboard's "retired N ago" cell doesn't drift relative to `last_seen_age_secs`
/// on the online roster.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RetiredAgentView {
    pub id: String,
    pub tier: String,
    pub retired_at: i64,
    /// `max(0, now - retired_at)` at snapshot time. Convenience for renderers.
    pub retired_age_secs: i64,
    pub tasks_completed: i64,
    pub total_active_secs: i64,
}

/// Per-agent cumulative work signal — issue #95 Phase 1 (data only).
///
/// Now also consumed by the issue #97 retirement mechanic (server-side drain on score,
/// sticky carve-out, retire signal in `sync`).
///
/// `tasks_completed` counts distinct tasks the agent was assignee on when the task reached
/// `done`/`closed`. `total_active_secs` sums `(task.updated_at - latest_claim.ts)` per
/// completed task — the closest observable proxy for "context consumed" without
/// instrumenting the agent. Multi-round (changes-verdict) tasks count only the most recent
/// claim→done window, not the full rework history; that's an accepted Phase 1
/// simplification (issue #95 §1 calls for "cumulative active duration" without requiring
/// rework-perfect attribution).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentLoadScore {
    pub agent_id: String,
    pub tasks_completed: i64,
    pub total_active_secs: i64,
}

/// Throughput / queue-health metrics — surfaces review-loop stalls early.
#[derive(Debug, Serialize, PartialEq, Eq, Default)]
pub struct Throughput {
    /// Tasks transitioned to `closed` in the last hour (proxy for review-loop velocity).
    pub closed_last_hour: i64,
    /// Tasks currently `done` (submitted, awaiting review verdict).
    pub done_awaiting_review: i64,
    /// Age in seconds of the oldest `done`-status task (i.e. the worst review-loop stall).
    /// `None` when no `done` tasks exist.
    pub oldest_done_awaiting_review_secs: Option<i64>,
    /// Count of `done`-status tasks older than [`DONE_STUCK_THRESHOLD_SECS`].
    pub done_stuck_count: i64,
}

/// A point-in-time snapshot of the store.
#[derive(Debug, Serialize, PartialEq, Eq, Default)]
pub struct Stats {
    pub agents_total: i64,
    pub agents_online: i64,
    pub messages_live: i64,
    pub claims_active: i64,
    pub tasks: Vec<StatusCount>,
    pub errors_live: i64,
    pub last_errors: Vec<ErrorRow>,
    /// Issue #77: per-online-agent view (tier + current task + last_seen age).
    pub agents: Vec<AgentView>,
    /// Issue #77: claimable (ready) open-task count grouped by required tier.
    pub queue_by_tier: Vec<TierQueueCount>,
    /// Issue #86: open tasks blocked by unmet dependencies.
    pub blocked: Vec<BlockedTask>,
    /// Issue #77: last RECENT_MSG_LIMIT feed messages.
    pub recent_messages: Vec<RecentMessage>,
    /// Issue #77: active claims with time-to-expiry.
    pub claim_ttls: Vec<ClaimTtl>,
    /// Issue #77: throughput / review-loop-stall metrics.
    pub throughput: Throughput,
    /// Issue #95 Phase 1: per-agent cumulative work signal (tasks completed + active secs).
    /// Surfaced for owner-side fleet management; also consumed by the issue #97 retirement
    /// mechanic. Empty when no agent has completed work yet.
    pub agent_load_scores: Vec<AgentLoadScore>,
    /// Issue #97: agents whose `retire_status = 'retired'`, newest first. Surfaces in the
    /// retired-agents section of `quorum status` so the operator can see capacity drop and
    /// re-spin replacements.
    pub retired_agents: Vec<RetiredAgentView>,
    /// Issue #101 (experimental): per-agent activity summary from the
    /// PostToolUse hook. Stats-only; never read by routing/claim code.
    /// Empty when the hook isn't installed.
    pub activity: Vec<crate::activity::ActivityView>,
}

/// Gather a snapshot. Read-only.
pub fn stats(conn: &Connection, now: i64, online_window: i64) -> Result<Stats> {
    let one = |sql: &str, p: &[&dyn rusqlite::ToSql]| -> Result<i64> {
        Ok(conn.query_row(sql, p, |r| r.get(0))?)
    };

    let agents_total = one("SELECT count(*) FROM agents", &[])?;
    let agents_online = one(
        "SELECT count(*) FROM agents WHERE (?1 - last_seen) < ?2
           OR EXISTS (SELECT 1 FROM claims c
                      WHERE c.holder = agents.id AND c.active = 1 AND c.expires_at > ?1)",
        &[&now, &online_window],
    )?;
    let messages_live = one(
        "SELECT count(*) FROM messages WHERE expires_at > ?1",
        &[&now],
    )?;
    let claims_active = one(
        "SELECT count(*) FROM claims WHERE active=1 AND expires_at > ?1",
        &[&now],
    )?;
    let errors_live = one("SELECT count(*) FROM errors WHERE expires_at > ?1", &[&now])?;

    let mut tstmt =
        conn.prepare("SELECT status, count(*) FROM tasks GROUP BY status ORDER BY status")?;
    let tasks = tstmt
        .query_map([], |r| {
            Ok(StatusCount {
                status: r.get(0)?,
                count: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut estmt = conn.prepare(
        "SELECT ts, source, detail FROM errors WHERE expires_at > ?1 ORDER BY id DESC LIMIT 5",
    )?;
    let last_errors = estmt
        .query_map(params![now], |r| {
            Ok(ErrorRow {
                ts: r.get(0)?,
                source: r.get(1)?,
                detail: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    // Compute load scores once; both `online_agents_view` and `retired_agents_view` graft
    // them onto their rows, so a shared lookup avoids running the same JOIN three times.
    let agent_load_scores = agent_load_scores(conn)?;
    let scores_by_id: std::collections::HashMap<&str, &AgentLoadScore> = agent_load_scores
        .iter()
        .map(|s| (s.agent_id.as_str(), s))
        .collect();
    let agents = online_agents_view(conn, now, online_window, &scores_by_id)?;
    let queue_by_tier = queue_by_tier(conn)?;
    let blocked = blocked_tasks(conn)?;
    let recent_messages = recent_messages(conn, now)?;
    let claim_ttls = claim_ttls(conn, now)?;
    let throughput = throughput(conn, now)?;
    let retired_agents = retired_agents_view(conn, now, &scores_by_id)?;
    // Issue #101 (experimental): stats-only PostToolUse hook activity. Empty
    // vec when no events recorded — section is suppressed in the renderer.
    let activity = crate::activity::activity_summary(conn, now)?;

    Ok(Stats {
        agents_total,
        agents_online,
        messages_live,
        claims_active,
        tasks,
        errors_live,
        last_errors,
        agents,
        queue_by_tier,
        blocked,
        recent_messages,
        claim_ttls,
        throughput,
        agent_load_scores,
        retired_agents,
        activity,
    })
}

/// Per-online-agent view. Tier read from the stored `agents.tier` column (persisted on
/// each `sync --match-label tier:*`); falls back to `unknown` when NULL.
/// Sorted by tier ascending, then id ascending — deterministic so the watch loop's output
/// is stable frame-to-frame.
///
/// Issue #97: each row is enriched with the agent's scoreboard fields
/// (`tasks_completed`, `total_active_secs`, `retire_status`) by grafting on the caller's
/// pre-computed `scores_by_id` map — keeps the existing SQL simple, and avoids
/// re-running `agent_load_scores` here when `stats()` already has it.
fn online_agents_view(
    conn: &Connection,
    now: i64,
    online_window: i64,
    scores_by_id: &std::collections::HashMap<&str, &AgentLoadScore>,
) -> Result<Vec<AgentView>> {
    let mut stmt = conn.prepare(
        "SELECT a.id, a.last_seen, a.tier, a.retire_status, t.id, t.title
         FROM agents a
         LEFT JOIN claims c
           ON c.holder = a.id
          AND c.active = 1
          AND c.expires_at > ?1
          AND c.target LIKE 'task#%'
         LEFT JOIN tasks t
           ON t.id = CAST(SUBSTR(c.target, 6) AS INTEGER)
         WHERE ((?1 - a.last_seen) < ?2
                OR EXISTS (SELECT 1 FROM claims c2
                           WHERE c2.holder = a.id AND c2.active = 1 AND c2.expires_at > ?1))
           AND a.retire_status != 'retired'
         ORDER BY a.id ASC",
    )?;
    let mut views: Vec<AgentView> = stmt
        .query_map(params![now, online_window], |r| {
            let id: String = r.get(0)?;
            let last_seen: i64 = r.get(1)?;
            let stored_tier: Option<String> = r.get(2)?;
            let retire_status: String = r.get(3)?;
            let task_id: Option<i64> = r.get(4)?;
            let task_title: Option<String> = r.get(5)?;
            let current_task = task_id
                .zip(task_title)
                .map(|(id, title)| AgentCurrentTask { id, title });
            let tier = stored_tier.unwrap_or_else(|| "unknown".to_string());
            Ok(AgentView {
                id,
                tier,
                current_task,
                last_seen_age_secs: (now - last_seen).max(0),
                tasks_completed: 0,
                total_active_secs: 0,
                retire_status,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // Graft pre-computed scores onto the per-agent rows.
    for v in &mut views {
        if let Some(s) = scores_by_id.get(v.id.as_str()) {
            v.tasks_completed = s.tasks_completed;
            v.total_active_secs = s.total_active_secs;
        }
    }
    // Stable display order: by tier then id.
    views.sort_by(|a, b| a.tier.cmp(&b.tier).then_with(|| a.id.cmp(&b.id)));
    Ok(views)
}

/// Issue #97: retired-agents view — the agents who've signed off, with their final
/// scoreboard frozen at retirement. Sorted by `retired_at` DESC (newest first); ties broken
/// by id ASC for deterministic output. Caller passes the shared `scores_by_id` map
/// computed once in `stats()` so the load-score JOIN doesn't fire twice.
fn retired_agents_view(
    conn: &Connection,
    now: i64,
    scores_by_id: &std::collections::HashMap<&str, &AgentLoadScore>,
) -> Result<Vec<RetiredAgentView>> {
    let mut stmt = conn.prepare(
        "SELECT id, COALESCE(tier, 'unknown'), retired_at
         FROM agents
         WHERE retire_status = 'retired' AND retired_at IS NOT NULL
         ORDER BY retired_at DESC, id ASC",
    )?;
    let rows: Vec<(String, String, i64)> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .map(|(id, tier, retired_at)| {
            let (tasks_completed, total_active_secs) = scores_by_id
                .get(id.as_str())
                .map(|s| (s.tasks_completed, s.total_active_secs))
                .unwrap_or((0, 0));
            RetiredAgentView {
                id,
                tier,
                retired_at,
                retired_age_secs: (now - retired_at).max(0),
                tasks_completed,
                total_active_secs,
            }
        })
        .collect())
}

/// Per-agent slice of [`agent_load_scores`] — returns `(tasks_completed, total_active_secs)`
/// for `agent_id`, or `(0, 0)` when the agent has no completed work yet. Used by `sync` to
/// evaluate the retirement budget on every tick without scanning the whole fleet.
///
/// Same accounting as the fleet-wide version: distinct done/closed tasks where the agent
/// was assignee, joined with the latest matching claim for the working window.
pub fn load_score_for(conn: &Connection, agent_id: &str) -> Result<(i64, i64)> {
    // The COUNT/COALESCE(SUM) aggregate always returns exactly one row (zero matching
    // tasks → `(0, 0)`), so `query_row` never raises `QueryReturnedNoRows`.
    // `agent_id` is bound twice (?1 / ?2) because some SQLite driver bindings get touchy
    // about reusing a single positional placeholder across non-adjacent clauses inside a
    // CTE; both bindings are the same value.
    let row = conn.query_row(
        "WITH latest_claim AS (
             SELECT target, holder, MAX(ts) AS ts
             FROM claims
             WHERE holder = ?1
             GROUP BY target, holder
         )
         SELECT
             COUNT(DISTINCT t.id) AS tasks_completed,
             COALESCE(SUM(CASE
                 WHEN t.updated_at > lc.ts THEN t.updated_at - lc.ts
                 ELSE 0
             END), 0) AS total_active_secs
         FROM tasks t
         JOIN latest_claim lc
             ON lc.target = 'task#' || t.id
             AND lc.holder = t.assignee
         WHERE t.status IN ('done', 'closed')
             AND t.assignee = ?2",
        params![agent_id, agent_id],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
    )?;
    Ok(row)
}

/// Parse `tier:*` out of a JSON-array labels string. Returns the first matching label
/// verbatim (e.g. `tier:opus-47`), or `unknown` when none / no labels / unparseable.
/// Keeps the SQL path tier-agnostic — tier vocabulary lives in agent/CTO conventions.
pub fn extract_tier_from_labels(labels_json: Option<&str>) -> String {
    let s = match labels_json {
        Some(s) => s,
        None => return "unknown".to_string(),
    };
    let v: serde_json::Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return "unknown".to_string(),
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return "unknown".to_string(),
    };
    for item in arr {
        if let Some(t) = item.as_str() {
            if let Some(rest) = t.strip_prefix("tier:") {
                if !rest.is_empty() {
                    return t.to_string();
                }
            }
        }
    }
    "unknown".to_string()
}

/// Claimable (ready) open-task count grouped by required tier (#86). Uses
/// [`extract_tier_from_labels`] over each open task row in app-space. Only counts tasks
/// whose dependencies are all satisfied (`ready=true`); blocked tasks are surfaced
/// separately via [`blocked_tasks`]. `kind:review` open tasks land in a distinct `review`
/// bucket (tier-exempt at the matcher, #73 fix).
fn queue_by_tier(conn: &Connection) -> Result<Vec<TierQueueCount>> {
    let mut stmt = conn.prepare("SELECT id, labels, depends_on FROM tasks WHERE status='open'")?;
    let rows = stmt
        .query_map([], |r| {
            let id: i64 = r.get(0)?;
            let labels: Option<String> = r.get(1)?;
            let depends_on: Option<String> = r.get(2)?;
            Ok((id, labels, depends_on))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut counts: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    for (_id, labels, depends_on) in &rows {
        let ready = crate::tasks::compute_ready(conn, depends_on)?;
        if !ready {
            continue;
        }
        let bucket = if has_label(labels.as_deref(), "kind:review") {
            "review".to_string()
        } else {
            let t = extract_tier_from_labels(labels.as_deref());
            if t == "unknown" {
                "untiered".to_string()
            } else {
                t
            }
        };
        *counts.entry(bucket).or_insert(0) += 1;
    }
    Ok(counts
        .into_iter()
        .map(|(tier, open)| TierQueueCount { tier, open })
        .collect())
}

/// Open tasks blocked by unmet dependencies (#86). Returns each blocked task with the
/// list of dep ids it's waiting on (only deps that are NOT yet `closed`).
fn blocked_tasks(conn: &Connection) -> Result<Vec<BlockedTask>> {
    let mut stmt = conn.prepare(
        "SELECT id, title, depends_on FROM tasks WHERE status='open' AND depends_on IS NOT NULL",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let id: i64 = r.get(0)?;
            let title: String = r.get(1)?;
            let depends_on: Option<String> = r.get(2)?;
            Ok((id, title, depends_on))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut blocked = Vec::new();
    for (id, title, depends_on) in rows {
        let ready = crate::tasks::compute_ready(conn, &depends_on)?;
        if ready {
            continue;
        }
        let waiting_on = unmet_deps(conn, &depends_on)?;
        blocked.push(BlockedTask {
            id,
            title,
            waiting_on,
        });
    }
    Ok(blocked)
}

/// Return the subset of dep ids from `depends_on` that are NOT `closed`.
fn unmet_deps(conn: &Connection, depends_on: &Option<String>) -> Result<Vec<i64>> {
    let Some(json) = depends_on.as_deref() else {
        return Ok(vec![]);
    };
    let mut stmt = conn.prepare(
        "SELECT je.value FROM json_each(?1) je
         WHERE NOT EXISTS (
             SELECT 1 FROM tasks d WHERE d.id = je.value AND d.status = 'closed'
         )",
    )?;
    let ids = stmt
        .query_map(params![json], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}

/// Quick "does this labels JSON contain a given label string" helper.
pub fn has_label(labels_json: Option<&str>, target: &str) -> bool {
    let s = match labels_json {
        Some(s) => s,
        None => return false,
    };
    let v: serde_json::Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return false,
    };
    arr.iter().any(|x| x.as_str() == Some(target))
}

/// Last [`RECENT_MSG_LIMIT`] feed messages (newest first), with a bounded body preview.
///
/// **Broadcasts only.** Direct messages (`recipient IS NOT NULL`) are point-to-point per
/// issue #91 — the global `quorum status` dashboard renders only the fleet-wide feed.
/// Each agent's own direct messages are delivered via `quorum sync` instead. Without this
/// filter a `--to X` from A leaks into every agent's `status` view, making `--to` a
/// priority hint rather than a privacy boundary (verified leak on 2026-06-27, issue #91).
fn recent_messages(conn: &Connection, now: i64) -> Result<Vec<RecentMessage>> {
    let mut stmt = conn.prepare(
        "SELECT seq, ts, author, kind, body
         FROM messages
         WHERE expires_at > ?1 AND recipient IS NULL
         ORDER BY seq DESC
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(params![now, RECENT_MSG_LIMIT], |r| {
            let body: String = r.get(4)?;
            let preview: String = body
                .chars()
                .take(MSG_PREVIEW_CHARS)
                .collect::<String>()
                .replace(['\n', '\r'], " ");
            let trimmed = if body.chars().count() > MSG_PREVIEW_CHARS {
                format!("{preview}…")
            } else {
                preview
            };
            let ts: i64 = r.get(1)?;
            Ok(RecentMessage {
                seq: r.get(0)?,
                ts,
                age_secs: (now - ts).max(0),
                author: r.get(2)?,
                kind: r.get(3)?,
                body_preview: trimmed,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Active claims with time-to-expiry, ordered soonest-to-expire first (the dashboard's
/// most actionable angle — what's about to lapse).
fn claim_ttls(conn: &Connection, now: i64) -> Result<Vec<ClaimTtl>> {
    let mut stmt = conn.prepare(
        "SELECT target, holder, expires_at
         FROM claims
         WHERE active=1 AND expires_at > ?1
         ORDER BY expires_at ASC",
    )?;
    let rows = stmt
        .query_map(params![now], |r| {
            let expires_at: i64 = r.get(2)?;
            Ok(ClaimTtl {
                target: r.get(0)?,
                holder: r.get(1)?,
                expires_in_secs: expires_at - now,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Per-agent cumulative work signal — issue #95 Phase 1 (data only).
///
/// Joins `tasks` (status='done' or 'closed', assignee NOT NULL) against the agent's most
/// recent claim row for that task to derive `(claim.ts → task.updated_at)` as the working
/// window, then aggregates per-agent. Multi-round (changes-verdict) tasks expose only the
/// most recent window — see `AgentLoadScore` for the rationale.
///
/// Returns rows newest-by-volume first (highest total_active_secs first); ties broken by
/// tasks_completed descending then agent_id ascending so output is deterministic for tests.
fn agent_load_scores(conn: &Connection) -> Result<Vec<AgentLoadScore>> {
    let mut stmt = conn.prepare(
        "WITH latest_claim AS (
             SELECT target, holder, MAX(ts) AS ts
             FROM claims
             GROUP BY target, holder
         )
         SELECT
             t.assignee AS agent_id,
             COUNT(DISTINCT t.id) AS tasks_completed,
             COALESCE(SUM(CASE
                 WHEN t.updated_at > lc.ts THEN t.updated_at - lc.ts
                 ELSE 0
             END), 0) AS total_active_secs
         FROM tasks t
         JOIN latest_claim lc
             ON lc.target = 'task#' || t.id
             AND lc.holder = t.assignee
         WHERE t.status IN ('done', 'closed')
             AND t.assignee IS NOT NULL
         GROUP BY t.assignee
         ORDER BY total_active_secs DESC, tasks_completed DESC, agent_id ASC",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok(AgentLoadScore {
                agent_id: r.get(0)?,
                tasks_completed: r.get(1)?,
                total_active_secs: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Throughput / review-loop-stall metrics.
fn throughput(conn: &Connection, now: i64) -> Result<Throughput> {
    let hour_ago = now - 3600;
    let closed_last_hour: i64 = conn.query_row(
        "SELECT count(*) FROM tasks WHERE status='closed' AND updated_at > ?1",
        params![hour_ago],
        |r| r.get(0),
    )?;
    // Exclude kind:review tasks — their terminal state is `done` (they never transition
    // to `closed`), so they inflate "awaiting review" counters. See issue #81.
    let done_filter = "status='done' AND (labels IS NULL OR labels NOT LIKE '%\"kind:review\"%')";
    let done_awaiting_review: i64 = conn.query_row(
        &format!("SELECT count(*) FROM tasks WHERE {done_filter}"),
        [],
        |r| r.get(0),
    )?;
    let oldest_done_ts: Option<i64> = conn
        .query_row(
            &format!("SELECT MIN(updated_at) FROM tasks WHERE {done_filter}"),
            [],
            |r| r.get(0),
        )
        .ok();
    let oldest_done_awaiting_review_secs = oldest_done_ts.map(|ts| (now - ts).max(0));
    let stuck_threshold = now - DONE_STUCK_THRESHOLD_SECS;
    let done_stuck_count: i64 = conn.query_row(
        &format!("SELECT count(*) FROM tasks WHERE {done_filter} AND updated_at < ?1"),
        params![stuck_threshold],
        |r| r.get(0),
    )?;
    Ok(Throughput {
        closed_last_hour,
        done_awaiting_review,
        oldest_done_awaiting_review_secs,
        done_stuck_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    #[test]
    fn counts_exclude_expired_and_stale() {
        let (_d, mut c) = open_tmp();
        // Live message survives past now (TTL=4000); dead message expired long ago.
        crate::feed::post(
            &mut c, "A", "info", None, "live", None, None, false, 4000, 100,
        )
        .unwrap();
        crate::feed::post(&mut c, "A", "info", None, "dead", None, None, false, 5, 100).unwrap();
        crate::claims::claim(&mut c, "A", "pr#1", 1000, 100).unwrap();
        crate::tasks::create(&mut c, "A", "t", None, 0, None, None, None, 100).unwrap();

        // now=4000: agent last_seen=100 (3900s stale > 900 window), claim expired (3700 < 4000).
        let s = stats(&c, 4000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.messages_live, 1);
        assert_eq!(s.claims_active, 0);
        assert_eq!(s.agents_total, 1);
        assert_eq!(s.agents_online, 0);
        assert_eq!(
            s.tasks,
            vec![StatusCount {
                status: "open".into(),
                count: 1
            }]
        );
        assert_eq!(s.errors_live, 0);
    }

    // --- Issue #100: claim-holders count as online -------------------------

    #[test]
    fn claim_holder_counted_as_online_in_stats() {
        let (_d, mut c) = open_tmp();
        // ttl=5000 so claim expires at 5100 (well past now=2000).
        crate::claims::claim(&mut c, "A", "pr#1", 5000, 100).unwrap();
        // now=2000: last_seen=100 stale (1900 > 900 window), but claim active (5100>2000).
        let s = stats(&c, 2000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.agents_online, 1, "claim-holder must count as online");
        assert_eq!(s.agents.len(), 1, "claim-holder must appear in agents view");
    }

    #[test]
    fn claim_holder_with_task_shows_busy_in_agents_view() {
        let (_d, mut c) = open_tmp();
        let tid = crate::tasks::create(
            &mut c,
            "boss",
            "fix-presence",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::claim(&mut c, "Worker", Some(tid), &[], 3600, 100).unwrap();
        crate::agents::set_tier(&c, "Worker", Some("tier:opus-46")).unwrap();
        // now=2000: both agents last_seen=100 (1900s stale > 900 window).
        // "boss" has no claims → offline. "Worker" holds task claim (expires 3700 > 2000) → online.
        let s = stats(&c, 2000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(
            s.agents_online, 1,
            "only claim-holder Worker should be online"
        );
        assert_eq!(s.agents.len(), 1, "only online agents appear in the view");
        let worker = &s.agents[0];
        assert_eq!(worker.id, "Worker");
        assert!(
            worker.current_task.is_some(),
            "worker must show current task"
        );
        assert_eq!(worker.current_task.as_ref().unwrap().title, "fix-presence");
    }

    // --- Issue #77 dashboard fields ----------------------------------------

    #[test]
    fn extract_tier_finds_tier_label() {
        assert_eq!(
            extract_tier_from_labels(Some(r#"["foo","tier:opus-47","bar"]"#)),
            "tier:opus-47"
        );
        assert_eq!(
            extract_tier_from_labels(Some(r#"["foo","bar"]"#)),
            "unknown"
        );
        assert_eq!(extract_tier_from_labels(None), "unknown");
        assert_eq!(extract_tier_from_labels(Some("not json")), "unknown");
        assert_eq!(extract_tier_from_labels(Some(r#"["tier:"]"#)), "unknown");
    }

    #[test]
    fn has_label_matches_exactly() {
        assert!(has_label(
            Some(r#"["kind:review","tier:opus-47"]"#),
            "kind:review"
        ));
        assert!(!has_label(Some(r#"["kind:bug"]"#), "kind:review"));
        assert!(!has_label(None, "kind:review"));
    }

    #[test]
    fn agents_view_uses_stored_tier() {
        let (_d, mut c) = open_tmp();
        // Two agents with stored tiers and claimed tasks.
        let t46 = crate::tasks::create(
            &mut c,
            "boss",
            "t46",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        let t47 = crate::tasks::create(
            &mut c,
            "boss",
            "t47",
            None,
            0,
            Some("[\"tier:opus-47\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::claim(&mut c, "Alice", Some(t46), &[], 1000, 100).unwrap();
        crate::tasks::claim(&mut c, "Bob", Some(t47), &[], 1000, 100).unwrap();
        // Persist tiers on the agent rows (as sync would do).
        crate::agents::set_tier(&c, "Alice", Some("tier:opus-46")).unwrap();
        crate::agents::set_tier(&c, "Bob", Some("tier:opus-47")).unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let by_id: std::collections::HashMap<_, _> =
            s.agents.iter().map(|a| (a.id.as_str(), a)).collect();
        assert_eq!(by_id["Alice"].tier, "tier:opus-46");
        assert_eq!(by_id["Alice"].current_task.as_ref().unwrap().id, t46);
        assert_eq!(by_id["Bob"].tier, "tier:opus-47");
        assert_eq!(by_id["Bob"].current_task.as_ref().unwrap().id, t47);
    }

    #[test]
    fn agents_view_stored_tier_survives_idle() {
        let (_d, c) = open_tmp();
        // Agent synced with a tier, then released its task — tier should persist.
        crate::agents::touch(&c, "Idle", 100).unwrap();
        crate::agents::set_tier(&c, "Idle", Some("tier:opus-46")).unwrap();
        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let a = s.agents.iter().find(|a| a.id == "Idle").unwrap();
        assert_eq!(a.tier, "tier:opus-46");
        assert!(a.current_task.is_none());
    }

    #[test]
    fn agents_view_unknown_tier_when_never_synced_with_tier() {
        let (_d, mut c) = open_tmp();
        // Agent posts a message (touches presence) but never synced with --match-label.
        crate::feed::post(
            &mut c, "NoTier", "info", None, "hi", None, None, false, 1000, 100,
        )
        .unwrap();
        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let a = s.agents.iter().find(|a| a.id == "NoTier").unwrap();
        assert_eq!(a.tier, "unknown");
        assert!(a.current_task.is_none());
    }

    #[test]
    fn queue_by_tier_buckets_correctly() {
        let (_d, mut c) = open_tmp();
        crate::tasks::create(
            &mut c,
            "boss",
            "a",
            None,
            0,
            Some("[\"tier:opus-47\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "b",
            None,
            0,
            Some("[\"tier:opus-47\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "c",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::create(&mut c, "boss", "d", None, 0, None, None, None, 100).unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "r",
            None,
            1000,
            Some("[\"kind:review\"]"),
            None,
            None,
            100,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let by_tier: std::collections::HashMap<_, _> = s
            .queue_by_tier
            .iter()
            .map(|q| (q.tier.as_str(), q.open))
            .collect();
        assert_eq!(by_tier.get("tier:opus-47"), Some(&2));
        assert_eq!(by_tier.get("tier:opus-46"), Some(&1));
        assert_eq!(by_tier.get("untiered"), Some(&1));
        assert_eq!(by_tier.get("review"), Some(&1));
    }

    #[test]
    fn recent_messages_limit_and_preview() {
        let (_d, mut c) = open_tmp();
        for i in 0..(RECENT_MSG_LIMIT + 3) {
            crate::feed::post(
                &mut c,
                "A",
                "info",
                None,
                &format!("msg-{i}"),
                None,
                None,
                false,
                1000,
                100 + i,
            )
            .unwrap();
        }
        let long_body = "x".repeat(MSG_PREVIEW_CHARS + 50);
        crate::feed::post(
            &mut c, "A", "info", None, &long_body, None, None, false, 1000, 200,
        )
        .unwrap();

        let s = stats(&c, 300, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.recent_messages.len() as i64, RECENT_MSG_LIMIT);
        // Newest first (the long body).
        assert!(
            s.recent_messages[0].body_preview.ends_with('…'),
            "long body must be truncated with ellipsis, got: {}",
            s.recent_messages[0].body_preview
        );
        assert!(s.recent_messages[0].body_preview.chars().count() == MSG_PREVIEW_CHARS + 1);
        // +1 for ellipsis
    }

    #[test]
    fn recent_messages_excludes_direct_messages_issue_91() {
        // --to messages must be invisible in the global feed (privacy boundary).
        // Pre-#91 behavior leaked them; the recipient-IS-NULL filter pins the new contract.
        let (_d, mut c) = open_tmp();
        crate::feed::post(
            &mut c,
            "A",
            "info",
            None,
            "broadcast-1",
            None,
            None,
            false,
            1000,
            100,
        )
        .unwrap();
        crate::feed::post(
            &mut c,
            "A",
            "info",
            None,
            "to-Bob",
            None,
            Some("Bob"),
            false,
            1000,
            101,
        )
        .unwrap();
        crate::feed::post(
            &mut c,
            "A",
            "info",
            None,
            "broadcast-2",
            None,
            None,
            false,
            1000,
            102,
        )
        .unwrap();
        crate::feed::post(
            &mut c,
            "A",
            "critical",
            None,
            "critical-to-Bob",
            None,
            Some("Bob"),
            false,
            1000,
            103,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let bodies: Vec<&str> = s
            .recent_messages
            .iter()
            .map(|m| m.body_preview.as_str())
            .collect();
        assert!(
            bodies.contains(&"broadcast-1"),
            "broadcast must appear in global feed: {bodies:?}"
        );
        assert!(
            bodies.contains(&"broadcast-2"),
            "broadcast must appear in global feed: {bodies:?}"
        );
        assert!(
            !bodies.iter().any(|b| b.contains("to-Bob")),
            "direct message must NOT appear in global feed: {bodies:?}"
        );
    }

    #[test]
    fn claim_ttls_orders_soonest_first() {
        let (_d, mut c) = open_tmp();
        // Different holders so `agents::touch` auto-renewal of in-flight leases (which
        // happens inside every write — see CLAUDE.md "Leases auto-renew on touch") doesn't
        // cross-renew across our test claims and re-order the expected times.
        crate::claims::claim(&mut c, "A", "pr#1", 100, 1000).unwrap(); // expires 1100
        crate::claims::claim(&mut c, "B", "pr#2", 1000, 1000).unwrap(); // expires 2000
        crate::claims::claim(&mut c, "C", "pr#3", 500, 1000).unwrap(); // expires 1500

        let s = stats(&c, 1050, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let order: Vec<_> = s.claim_ttls.iter().map(|x| x.target.as_str()).collect();
        assert_eq!(order, vec!["pr#1", "pr#3", "pr#2"]);
        assert!(s.claim_ttls[0].expires_in_secs > 0);
    }

    fn done(status: &str) -> crate::tasks::TaskUpdate<'_> {
        crate::tasks::TaskUpdate {
            status: Some(status),
            body: None,
            refs: None,
            verdict: None,
        }
    }

    // ── Agent load score (#95 Phase 1) ─────────────────────────────────

    fn make_task(c: &mut Connection, title: &str, now: i64) -> i64 {
        crate::tasks::create(c, "boss", title, None, 0, None, None, None, now).unwrap()
    }

    /// Drive one task all the way through claim → done as `agent`, returning the
    /// task id. Uses distinct claim/done timestamps so the per-task active duration is
    /// non-zero.
    fn complete_task_as(c: &mut Connection, agent: &str, claim_ts: i64, done_ts: i64) -> i64 {
        let id = make_task(c, &format!("t-{claim_ts}"), claim_ts - 1);
        crate::tasks::claim(c, agent, Some(id), &[], 3600, claim_ts).unwrap();
        crate::tasks::update(c, agent, id, &done("done"), done_ts).unwrap();
        id
    }

    #[test]
    fn agent_load_scores_empty_when_no_completed_tasks() {
        let (_d, c) = open_tmp();
        let s = stats(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert!(s.agent_load_scores.is_empty());
    }

    #[test]
    fn agent_load_scores_sums_per_task_active_duration() {
        let (_d, mut c) = open_tmp();
        // Alice: 2 tasks, durations 30s + 60s = 90s; Bob: 1 task, 10s.
        complete_task_as(&mut c, "Alice", 100, 130);
        complete_task_as(&mut c, "Alice", 200, 260);
        complete_task_as(&mut c, "Bob", 300, 310);

        let s = stats(&c, 400, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        // Sorted by total_active_secs DESC: Alice (90) then Bob (10).
        assert_eq!(s.agent_load_scores.len(), 2);
        assert_eq!(s.agent_load_scores[0].agent_id, "Alice");
        assert_eq!(s.agent_load_scores[0].tasks_completed, 2);
        assert_eq!(s.agent_load_scores[0].total_active_secs, 90);
        assert_eq!(s.agent_load_scores[1].agent_id, "Bob");
        assert_eq!(s.agent_load_scores[1].tasks_completed, 1);
        assert_eq!(s.agent_load_scores[1].total_active_secs, 10);
    }

    #[test]
    fn agent_load_scores_excludes_in_flight_tasks() {
        let (_d, mut c) = open_tmp();
        // Alice has one completed task (30s) and one still claimed — the in-flight one
        // must NOT contribute to the score (the retire signal looks at completed work
        // only; in-flight time is the current lease's concern, not Phase 1's count).
        complete_task_as(&mut c, "Alice", 100, 130);
        let _in_flight = make_task(&mut c, "in-flight", 200);
        crate::tasks::claim(&mut c, "Alice", None, &[], 3600, 210).unwrap();

        let s = stats(&c, 300, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.agent_load_scores.len(), 1);
        assert_eq!(s.agent_load_scores[0].agent_id, "Alice");
        assert_eq!(s.agent_load_scores[0].tasks_completed, 1);
        assert_eq!(s.agent_load_scores[0].total_active_secs, 30);
    }

    #[test]
    fn agent_load_scores_orders_ties_deterministically() {
        let (_d, mut c) = open_tmp();
        // Bert and Anna both at 20s total — Anna sorts first by agent_id ASC tiebreaker
        // (after total_active_secs DESC, tasks_completed DESC). Output stability is
        // load-bearing for tests + #95-follow-up's scoreboard rendering.
        complete_task_as(&mut c, "Bert", 100, 120);
        complete_task_as(&mut c, "Anna", 200, 220);

        let s = stats(&c, 300, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let ids: Vec<&str> = s
            .agent_load_scores
            .iter()
            .map(|s| s.agent_id.as_str())
            .collect();
        assert_eq!(ids, vec!["Anna", "Bert"]);
    }

    #[test]
    fn throughput_counts_oldest_done_awaiting_review() {
        let (_d, mut c) = open_tmp();
        // Two tasks, both driven to `done` at different times. No closed transitions:
        // closing requires the review-verdict path (approve/changes), which is exercised
        // in tasks::tests. Stats only reads the resulting state — pin that here.
        let t1 =
            crate::tasks::create(&mut c, "boss", "t1", None, 0, None, None, None, 100).unwrap();
        let t2 =
            crate::tasks::create(&mut c, "boss", "t2", None, 0, None, None, None, 200).unwrap();
        let t3_open =
            crate::tasks::create(&mut c, "boss", "t3", None, 0, None, None, None, 300).unwrap();
        crate::tasks::claim(&mut c, "Alice", Some(t1), &[], 1000, 400).unwrap();
        crate::tasks::update(&mut c, "Alice", t1, &done("done"), 400).unwrap();
        crate::tasks::claim(&mut c, "Bob", Some(t2), &[], 1000, 500).unwrap();
        crate::tasks::update(&mut c, "Bob", t2, &done("done"), 500).unwrap();

        let now = 600;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.throughput.done_awaiting_review, 2);
        // t1 went done at 400; now=600 → age 200s. t1 is older than t2.
        assert_eq!(s.throughput.oldest_done_awaiting_review_secs, Some(200));
        // 200s < DONE_STUCK_THRESHOLD_SECS (30 min) → not stuck.
        assert_eq!(s.throughput.done_stuck_count, 0);
        assert!(crate::tasks::get(&c, t3_open).unwrap().is_some());
    }

    /// Insert a task directly with arbitrary status and labels — bypasses the claim/update
    /// lifecycle so stats tests can set up specific states without wiring the full review flow.
    fn insert_task_raw(
        c: &Connection,
        title: &str,
        status: &str,
        labels: Option<&str>,
        updated_at: i64,
    ) -> i64 {
        c.execute(
            "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, created_at, updated_at)
             VALUES (?1, NULL, ?2, 0, ?3, NULL, 'test', 100, ?4)",
            params![title, status, labels, updated_at],
        ).unwrap();
        c.last_insert_rowid()
    }

    #[test]
    fn throughput_excludes_review_tasks_from_done_counters() {
        let (_d, c) = open_tmp();
        // A work task in done (should count).
        insert_task_raw(&c, "work-task", "done", None, 300);
        // A review task in done (should NOT count).
        insert_task_raw(
            &c,
            "review-1",
            "done",
            Some(r#"["kind:review","tier:opus-46"]"#),
            250,
        );
        // A second review task in done (should NOT count).
        insert_task_raw(&c, "review-2", "done", Some(r#"["kind:review"]"#), 220);

        let now = 400;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        // Only the work task counts.
        assert_eq!(s.throughput.done_awaiting_review, 1);
        // Oldest done is the work task (updated_at=300), age = 100s.
        assert_eq!(s.throughput.oldest_done_awaiting_review_secs, Some(100));
        // 100s < 30min threshold → not stuck.
        assert_eq!(s.throughput.done_stuck_count, 0);
    }

    #[test]
    fn throughput_zero_when_only_review_tasks_done() {
        let (_d, c) = open_tmp();
        insert_task_raw(&c, "review-only", "done", Some(r#"["kind:review"]"#), 300);

        let now = 400;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.throughput.done_awaiting_review, 0);
        assert_eq!(s.throughput.oldest_done_awaiting_review_secs, None);
        assert_eq!(s.throughput.done_stuck_count, 0);
    }

    #[test]
    fn throughput_done_stuck_flagged_after_threshold() {
        let (_d, mut c) = open_tmp();
        let t =
            crate::tasks::create(&mut c, "boss", "stuck", None, 0, None, None, None, 100).unwrap();
        crate::tasks::claim(&mut c, "Alice", Some(t), &[], 10000, 100).unwrap();
        crate::tasks::update(&mut c, "Alice", t, &done("done"), 100).unwrap();
        // Now far enough in the future for it to be stuck.
        let now = 100 + DONE_STUCK_THRESHOLD_SECS + 60;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.throughput.done_stuck_count, 1);
        assert!(s.throughput.oldest_done_awaiting_review_secs.unwrap() > DONE_STUCK_THRESHOLD_SECS);
    }

    // --- Issue #86: claimable-only queue counts + blocked section ---------------

    #[test]
    fn queue_excludes_blocked_tasks() {
        let (_d, mut c) = open_tmp();
        // t1: open, no deps → claimable → counted in queue.
        crate::tasks::create(
            &mut c,
            "boss",
            "ready-task",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        // t2: open, depends on t1 (not closed) → blocked → NOT in queue.
        let t1 = 1; // t1's id
        crate::tasks::create(
            &mut c,
            "boss",
            "blocked-task",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            Some(&format!("[{t1}]")),
            100,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let by_tier: std::collections::HashMap<_, _> = s
            .queue_by_tier
            .iter()
            .map(|q| (q.tier.as_str(), q.open))
            .collect();
        // Only the ready task counts.
        assert_eq!(by_tier.get("tier:opus-46"), Some(&1));
    }

    #[test]
    fn blocked_section_lists_tasks_with_unmet_deps() {
        let (_d, mut c) = open_tmp();
        let t1 = crate::tasks::create(
            &mut c,
            "boss",
            "dep-task",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        let t2 = crate::tasks::create(
            &mut c,
            "boss",
            "blocked-by-t1",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            Some(&format!("[{t1}]")),
            100,
        )
        .unwrap();
        let t3 = crate::tasks::create(
            &mut c,
            "boss",
            "blocked-by-t2",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            Some(&format!("[{t2}]")),
            100,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.blocked.len(), 2);
        let b_ids: Vec<i64> = s.blocked.iter().map(|b| b.id).collect();
        assert!(b_ids.contains(&t2));
        assert!(b_ids.contains(&t3));
        let b2 = s.blocked.iter().find(|b| b.id == t2).unwrap();
        assert_eq!(b2.waiting_on, vec![t1]);
        let b3 = s.blocked.iter().find(|b| b.id == t3).unwrap();
        assert_eq!(b3.waiting_on, vec![t2]);
    }

    #[test]
    fn blocked_section_empty_when_deps_satisfied() {
        let (_d, mut c) = open_tmp();
        let t1 = crate::tasks::create(
            &mut c,
            "boss",
            "dep-to-close",
            None,
            0,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "depends-on-closed",
            None,
            0,
            None,
            None,
            Some(&format!("[{t1}]")),
            100,
        )
        .unwrap();
        // Close t1 via the full lifecycle: claim → done → approve.
        crate::tasks::claim(&mut c, "Alice", Some(t1), &[], 10000, 100).unwrap();
        crate::tasks::update(
            &mut c,
            "Alice",
            t1,
            &crate::tasks::TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            100,
        )
        .unwrap();
        // Directly mark closed via raw SQL (the full review flow is overkill for this test).
        c.execute("UPDATE tasks SET status='closed' WHERE id=?1", params![t1])
            .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert!(s.blocked.is_empty());
        // The dependent should now appear in the queue.
        assert!(!s.queue_by_tier.is_empty());
    }

    // -- Issue #97 scoreboard + retired list -----------------------------------

    #[test]
    fn online_agents_view_carries_load_score_and_retire_status() {
        let (_d, mut c) = open_tmp();
        // Alice: 2 completed tasks, 60s cumulative active. Still 'active' (default
        // retire_status from the schema default).
        complete_task_as(&mut c, "Alice", 100, 130);
        complete_task_as(&mut c, "Alice", 200, 230);
        let s = stats(&c, 300, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let a = s.agents.iter().find(|x| x.id == "Alice").unwrap();
        assert_eq!(a.tasks_completed, 2);
        assert_eq!(a.total_active_secs, 60);
        assert_eq!(a.retire_status, "active");
    }

    #[test]
    fn retired_agents_view_lists_retired_and_excludes_them_from_online() {
        let (_d, mut c) = open_tmp();
        complete_task_as(&mut c, "Done", 100, 130);
        // Touch + flip to retired the way sync's write txn would.
        crate::agents::touch(&c, "Done", 200).unwrap();
        crate::agents::mark_retired(&c, "Done", 250).unwrap();
        let s = stats(&c, 300, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        // Retired agent must NOT appear in the online list.
        assert!(s.agents.iter().all(|x| x.id != "Done"));
        // …but must appear in `retired_agents` with their final stats.
        let r = s
            .retired_agents
            .iter()
            .find(|x| x.id == "Done")
            .expect("Done must be in retired_agents");
        assert_eq!(r.retired_at, 250);
        assert_eq!(r.retired_age_secs, 50, "now=300 - retired_at=250 → 50s");
        assert_eq!(r.tasks_completed, 1);
        assert_eq!(r.total_active_secs, 30);
    }

    #[test]
    fn retired_agents_view_orders_newest_first_then_id() {
        let (_d, c) = open_tmp();
        // Three retired agents at three timestamps. Newest-first sort with id tiebreaker.
        crate::agents::touch(&c, "A", 100).unwrap();
        crate::agents::mark_retired(&c, "A", 100).unwrap();
        crate::agents::touch(&c, "B", 200).unwrap();
        crate::agents::mark_retired(&c, "B", 200).unwrap();
        crate::agents::touch(&c, "C", 200).unwrap();
        crate::agents::mark_retired(&c, "C", 200).unwrap();
        let s = stats(&c, 300, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let ids: Vec<&str> = s.retired_agents.iter().map(|r| r.id.as_str()).collect();
        // B (200) < C (200, tied → id asc) < A (100, oldest last).
        assert_eq!(ids, vec!["B", "C", "A"]);
    }

    #[test]
    fn load_score_for_returns_zero_for_unknown_agent() {
        let (_d, c) = open_tmp();
        let (tasks, secs) = load_score_for(&c, "Never").unwrap();
        assert_eq!((tasks, secs), (0, 0));
    }

    #[test]
    fn load_score_for_matches_per_agent_slice_of_agent_load_scores() {
        let (_d, mut c) = open_tmp();
        complete_task_as(&mut c, "X", 100, 145); // 45s
        complete_task_as(&mut c, "X", 200, 215); // 15s
        complete_task_as(&mut c, "Y", 300, 310); // 10s (other agent — must not leak)
        let (tasks, secs) = load_score_for(&c, "X").unwrap();
        assert_eq!(tasks, 2);
        assert_eq!(secs, 60);
    }
}
