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
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::io::Read;

/// How many recent messages to surface on `status`. Bounded to keep the output cheap.
pub const RECENT_MSG_LIMIT: i64 = 5;
/// Body-preview length per recent message. Beyond this is truncated with an ellipsis.
pub const MSG_PREVIEW_CHARS: usize = 80;
/// A `done` task older than this is "stuck awaiting review" — surfaces stalled review loops.
pub const DONE_STUCK_THRESHOLD_SECS: i64 = 30 * 60;
/// Alerts older than this stay in the feed but no longer affect the status snapshot.
pub const ALERT_WINDOW_SECS: i64 = 12 * 60 * 60;
const ALERT_DISPLAY_LIMIT: i64 = 10;
/// Durable post-submit tasks to surface in REVIEWING. Bounded so `status --watch`
/// remains cheap if review-only tasks accumulate.
pub const REVIEWING_TASK_LIMIT: i64 = 20;
/// Maximum completed milestones retained in a single task-progress projection.
/// The projection is deliberately a recent, bounded explanation, not an event log.
pub const TASK_PROGRESS_HISTORY_LIMIT: usize = 12;
/// Maximum milestones (history, current stage, and future path) returned per task.
pub const TASK_PROGRESS_MILESTONE_LIMIT: usize = 16;
const TASK_PROGRESS_TEXT_LIMIT: usize = 160;
const TASK_PROGRESS_DEPENDENCY_LIMIT: usize = 8;
/// Status reads only a bounded prefix of a planner stream. The projection keeps
/// counters, never provider payloads, and a planner's sanitized stream itself
/// is capped at this scale.
const PLANNER_STREAM_INSPECT_BYTES: u64 = 256 * 1024;
const PLANNER_STREAM_INSPECT_RECORDS: usize = 512;

/// Sidecar file written by the daemon per agent slot — carries live progress
/// stats that the status reader picks up without a DB schema change.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DaemonLiveStats {
    pub tools: u32,
    pub now: String,
    pub evm: f64,
    pub up_secs: u64,
    pub mid_turn_tok: i64,
    pub spawn_epoch: i64,
    #[serde(default)]
    pub error_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_text: Option<String>,
}

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
/// (tasks with `review_only=1`).
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
    /// Display identity from an explicit task tier, or `pending` when this
    /// read-only snapshot cannot know the daemon's configured default.
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    /// Legacy compact label retained for JSON consumers. It is not an
    /// execution identity and cockpit rendering must not use it.
    pub tier_eff: String,
    pub waiting_on: Vec<i64>,
    /// Dep ids that are cancelled — will never unblock without intervention.
    pub deadlocked_on: Vec<i64>,
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

/// Daemon in-flight agent view — read from the journal table. Shows workers
/// and reviewers currently managed by the daemon, with their phase, cost, and
/// self-reported agent state (blocked/failed/needs-info/note).
#[derive(Debug, Serialize, PartialEq)]
pub struct DaemonAgentView {
    pub agent: String,
    pub role: String,
    pub sub_role: Option<String>,
    pub task_id: Option<i64>,
    pub phase: String,
    pub cost_tokens: i64,
    pub agent_state: Option<String>,
    pub cost_usd: f64,
    pub log_dir: Option<String>,
    pub last_activity_age_secs: Option<i64>,
    pub task_title: Option<String>,
    /// Exact identity persisted for this managed run.
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub tier_eff: Option<String>,
    pub pr: Option<i64>,
    pub rework_count: i32,
    pub tool_count: u32,
    pub now_label: Option<String>,
    pub events_per_min: Option<f64>,
    pub uptime_secs: Option<i64>,
    pub live_error_count: u32,
    pub live_error_text: Option<String>,
}

/// Individual claimable task for the QUEUE section (#204 cockpit).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct QueueTask {
    pub id: i64,
    pub title: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub tier_eff: String,
    pub priority: i64,
    pub pr: Option<i64>,
}

/// A task in the post-submit review or merge band, shown in REVIEWING.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ReviewingTask {
    pub id: i64,
    pub title: String,
    pub pr: Option<i64>,
    /// The in-flight reviewer agent, if the daemon currently has one for this task.
    pub reviewer: Option<String>,
    /// `reviewing`, `awaiting reviewer`, or `merging`.
    pub state: String,
}

/// Task pipeline row: active daemon-owned coordinators plus tasks merged in the last hour (#204).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct PipelineTask {
    pub id: i64,
    pub title: String,
    /// Identity of the latest managed run for this task, ordered by
    /// `(spawned_at, id)` descending. This makes rework/R1/R2 history stable.
    pub provider: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub status: String,
    pub pr: Option<i64>,
    pub blocked: bool,
    /// The newest durable Arbiter verdict for this decomposed source, if one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arbiter: Option<ArbiterVerdict>,
}

/// Compact projection of the Arbiter's terminal verdict evidence.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ArbiterVerdict {
    pub verdict: Option<String>,
    pub reason_code: String,
    pub at: i64,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub duration_ms: Option<i64>,
}

/// Bounded child projection for the repository's current decomposition graph.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DecompositionMemberView {
    pub task_id: i64,
    pub local_key: String,
    pub title: String,
    pub status: String,
    pub prerequisites: Vec<i64>,
}

/// Read-only projection of the single current decomposition graph.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DecompositionStatusView {
    pub graph_id: i64,
    pub source_task_id: i64,
    pub source_title: String,
    pub source_status: String,
    pub graph_state: String,
    /// Stable reason new graph-member implementation work cannot dispatch.
    pub dispatch_hold: Option<String>,
    pub proposal_attempts: i64,
    pub provider_failures: i64,
    pub hold_code: Option<String>,
    pub retryable_planning_hold: bool,
    pub operator_retry_count: i64,
    pub operator_retry_cap: i64,
    pub planner_provider: Option<String>,
    pub planner_model: Option<String>,
    /// Assigned planner effort, resolved from its immutable role assignment.
    pub planner_effort: Option<String>,
    /// Current planner attempt's session directory, if logging is configured.
    pub planner_log_dir: Option<String>,
    /// Age of the current planner stream's latest write.
    pub planner_last_activity_age_secs: Option<i64>,
    /// Bounded count of recognized activity records in the current stream.
    pub planner_activity_count: Option<u32>,
    /// Bounded count of recognized tool records in the current stream.
    pub planner_tool_count: Option<u32>,
    pub accepted_plan_revision: Option<i64>,
    pub completed_children: i64,
    pub total_children: i64,
    pub child_statuses: Vec<StatusCount>,
    pub failed_children: Vec<i64>,
    pub reasons: Vec<String>,
    pub members: Vec<DecompositionMemberView>,
}

/// The server-owned task journey. Consumers render this projection and do not
/// reconstruct lifecycle meaning from task status in the browser.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TaskJourney {
    DirectImplementation,
    ReviewOnly,
    DecomposedSource,
}

/// The state of an ordered task-progress milestone.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TaskProgressMilestoneState {
    Completed,
    Current,
    Future,
    Terminal,
}

/// One bounded point in a task's journey. `role` is deliberately secondary to
/// the friendly stage label: e.g. `Plan review` / `Arbiter` and `First review` / `R1`.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskProgressMilestone {
    pub stage: String,
    pub role: Option<String>,
    pub state: TaskProgressMilestoneState,
    pub activity: Option<String>,
}

/// The task's present stage and the friendly description of the work underway.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskProgressStage {
    pub label: String,
    pub role: Option<String>,
    pub activity: String,
}

/// An expected action. `Possible next` is used whenever an external verdict,
/// scheduler choice, CI result, or operator decision can alter the path.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskProgressNextAction {
    pub label: String,
    pub action: String,
}

/// Bounded counters that explain planning and remediation loops.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct TaskProgressAttempts {
    pub proposal_attempts: i64,
    pub provider_failures: i64,
    pub arbiter_rounds: i64,
    pub rework_round: i64,
    pub rework_cap: i64,
    pub operator_retry_count: i64,
    pub operator_retry_cap: i64,
}

/// A bounded aggregate of a decomposed source's active child graph.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskProgressChildren {
    /// Friendly `N/M` representation, kept alongside numeric fields for simple renderers.
    pub summary: String,
    pub completed: i64,
    pub total: i64,
    /// Failed/cancelled child ids, bounded to [`TASK_PROGRESS_DEPENDENCY_LIMIT`].
    pub blocking_task_ids: Vec<i64>,
}

/// Read-only, request-scoped explanation of one task's durable lifecycle journey.
///
/// `history` retains completed loop rounds while `milestones` contains the same
/// completed history plus the current stage and a bounded future path. It is not
/// a lifecycle authority and never writes or controls the daemon.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TaskProgress {
    pub task_id: i64,
    pub journey: TaskJourney,
    pub milestones: Vec<TaskProgressMilestone>,
    pub history: Vec<TaskProgressMilestone>,
    pub stage: TaskProgressStage,
    /// An external or blocking fact, kept separate from the stage/activity.
    pub condition: Option<String>,
    pub next_action: Option<TaskProgressNextAction>,
    pub attempts: TaskProgressAttempts,
    pub children: Option<TaskProgressChildren>,
}

/// Deduped error for the ERRORS section — groups repeated messages.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DedupedError {
    pub detail: String,
    pub source: String,
    pub count: i64,
    pub latest_age_secs: i64,
}

/// An owner-alert feed message (kind = "alert" or "critical") for the ALERTS cockpit section.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AlertMessage {
    pub body: String,
    pub refs: Option<String>,
    pub age_secs: i64,
    pub kind: String,
}

/// A task stuck in the merge pipeline waiting on external conditions (CI, conflicts,
/// policy). Surfaced in the MERGE WAIT status section (#177). The underlying task
/// stays nonterminal — dependents remain blocked by the normal dependency rule.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct MergeBlockerView {
    pub task_id: i64,
    pub title: String,
    pub pr: Option<i64>,
    /// "conflict", "ci_pending", "policy"
    pub blocker_kind: String,
    /// Current task status (e.g. "in-review", "merging").
    pub status: String,
    /// Seconds since the block began (derived from task.updated_at).
    pub waiting_secs: i64,
    /// Rework rounds accumulated on this task.
    pub retry_count: i32,
}

/// True when `count` is 0 or a power of two — controls merge-wait alert dedup.
/// Alerts fire at retries 0, 1, 2, 4, 8, 16, … to avoid per-poll spam.
pub fn alert_due_at_retry(count: i64) -> bool {
    count == 0 || (count > 0 && (count & (count - 1)) == 0)
}

/// Daemon liveness snapshot read from `daemon_lock`.
/// Populated by the binary crate (which owns the pid-alive syscall).
#[derive(Debug, Serialize, PartialEq, Eq, Clone, Default)]
pub enum DaemonLiveness {
    #[default]
    /// No row in daemon_lock — daemon has never started.
    None,
    /// Daemon alive: heartbeat fresh AND pid exists.
    Alive { pid: i64, heartbeat_age_secs: i64 },
    /// Daemon stale: heartbeat old or pid dead.
    Stale {
        pid: i64,
        heartbeat_age_secs: i64,
        pid_dead: bool,
    },
}

/// Health verdict for the status header.
#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy, Default)]
pub enum HealthVerdict {
    #[default]
    OnTrack,
    Attention,
    Stalled,
}

/// A point-in-time snapshot of the store.
#[derive(Debug, Serialize, PartialEq, Default)]
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
    pub agent_load_scores: Vec<AgentLoadScore>,
    /// Issue #97: agents whose `retire_status = 'retired'`, newest first.
    pub retired_agents: Vec<RetiredAgentView>,
    /// Issue #101 (experimental): per-agent activity summary from the PostToolUse hook.
    pub activity: Vec<crate::activity::ActivityView>,
    /// M5: daemon in-flight agents from the journal table.
    pub daemon_agents: Vec<DaemonAgentView>,
    /// #204: individual claimable tasks for QUEUE section.
    pub queue_tasks: Vec<QueueTask>,
    /// Tasks in the durable post-submit review/merge band.
    pub reviewing: Vec<ReviewingTask>,
    /// #204: task pipeline view (all active + recently closed).
    pub pipeline: Vec<PipelineTask>,
    /// Current planning cycle or active/held decomposition graph, if any.
    pub decomposition: Option<DecompositionStatusView>,
    /// #204: deduped errors from last hour.
    pub recent_errors: Vec<DedupedError>,
    /// #204: count of older errors silenced (>1h).
    pub older_errors_silenced: i64,
    /// #204: health verdict.
    pub health: HealthVerdict,
    /// #204: stalled agent count (activity age > 2m).
    pub stalled_count: i64,
    /// #204: total session cost (sum of journal cost_usd).
    pub session_cost: f64,
    /// T59: open PRs with no task backing.
    pub unbacked_prs: Vec<crate::drift::UnbackedPr>,
    /// T59: tasks with multiple open PRs.
    pub twin_prs: Vec<crate::drift::TwinPr>,
    /// #88: owner-alert feed messages (kind = alert/critical), visible in ALERTS cockpit section.
    pub alerts: Vec<AlertMessage>,
    /// #177: tasks stuck in the merge pipeline waiting on external conditions.
    pub merge_blockers: Vec<MergeBlockerView>,
    /// #115: daemon liveness from daemon_lock (populated by binary crate).
    pub daemon: DaemonLiveness,
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
    let daemon_agents = daemon_agents_view(conn, now)?;
    let queue_tasks_list = queue_tasks(conn)?;
    let reviewing = reviewing_tasks(conn, &daemon_agents)?;
    let pipeline = pipeline_tasks(conn, now)?;
    let decomposition = decomposition_status(conn, now)?;
    let (recent_errors, older_errors_silenced) = deduped_errors(conn, now)?;
    let alerts = alert_messages(conn, now)?;
    let merge_blockers = merge_blockers(conn, now)?;
    let health = compute_health(
        &daemon_agents,
        !recent_errors.is_empty(),
        !alerts.is_empty(),
    );
    let stalled_count = daemon_agents
        .iter()
        .filter(|d| is_stall_eligible(d))
        .filter(|d| {
            matches!(d.last_activity_age_secs, Some(age) if age > 180)
                || d.last_activity_age_secs.is_none()
        })
        .count() as i64;
    let session_cost: f64 = daemon_agents.iter().map(|d| d.cost_usd).sum();
    let unbacked_prs = crate::drift::unbacked_pr_events(conn, now).unwrap_or_default();
    let twin_prs = crate::drift::twin_pr_events(conn, now).unwrap_or_default();

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
        daemon_agents,
        queue_tasks: queue_tasks_list,
        reviewing,
        pipeline,
        decomposition,
        recent_errors,
        older_errors_silenced,
        health,
        stalled_count,
        session_cost,
        unbacked_prs,
        twin_prs,
        alerts,
        merge_blockers,
        daemon: DaemonLiveness::default(),
    })
}

#[derive(Debug)]
struct ProgressGraph {
    id: i64,
    state: String,
    proposal_attempts: i64,
    provider_failures: i64,
    operator_retry_count: i64,
    hold_code: Option<String>,
    hold_summary: Option<String>,
    accepted_plan_revision: Option<i64>,
    completed_children: i64,
    total_children: i64,
    blocking_child_ids: Vec<i64>,
}

#[derive(Debug)]
struct ProgressAttempt {
    kind: String,
    reason_code: String,
}

#[derive(Debug)]
struct ProgressLiveRun {
    role: String,
    phase: String,
    agent_state: Option<String>,
}

#[derive(Debug, Default)]
struct DependencyProgress {
    blocked_ids: Vec<i64>,
    cancelled_ids: Vec<i64>,
}

/// Build the bounded server-owned progress projection for one task. The caller
/// supplies `now` for the same request-scoped snapshot semantics as [`stats`].
pub fn task_progress(conn: &Connection, task_id: i64, now: i64) -> Result<Option<TaskProgress>> {
    let Some(task) = crate::tasks::get(conn, task_id)? else {
        return Ok(None);
    };
    let graph = progress_graph(conn, task_id)?;
    let journey = if graph.is_some() || matches!(task.status.as_str(), "planning" | "decomposed") {
        TaskJourney::DecomposedSource
    } else if task.review_only {
        TaskJourney::ReviewOnly
    } else {
        TaskJourney::DirectImplementation
    };
    let runs = progress_runs(conn, task_id)?;
    let live = progress_live_runs(conn, task_id)?;
    let dependencies = unsatisfied_dependency_ids(conn, task.depends_on.as_deref())?;
    let r2_required = has_r2_evidence(conn, task_id, &runs)?;
    let attempts = progress_attempts(conn, graph.as_ref())?;

    let mut history = match journey {
        TaskJourney::DecomposedSource => decomposition_history(graph.as_ref(), &attempts),
        TaskJourney::DirectImplementation | TaskJourney::ReviewOnly => {
            execution_history(&task, &runs)
        }
    };
    history.truncate(TASK_PROGRESS_HISTORY_LIMIT);

    let task_attempts = TaskProgressAttempts {
        proposal_attempts: graph.as_ref().map_or(0, |g| g.proposal_attempts),
        provider_failures: graph.as_ref().map_or(0, |g| g.provider_failures),
        arbiter_rounds: attempts
            .iter()
            .filter(|attempt| attempt.reason_code.starts_with("arbiter-"))
            .count() as i64,
        rework_round: task.rework_round,
        rework_cap: task
            .rework_cap
            .unwrap_or(crate::lifecycle::REWORK_CAP as i64),
        operator_retry_count: graph.as_ref().map_or(0, |g| g.operator_retry_count),
        operator_retry_cap: crate::decomposition::MAX_OPERATOR_RETRIES,
    };
    let children = graph.as_ref().map(|g| TaskProgressChildren {
        summary: format!("{}/{}", g.completed_children, g.total_children),
        completed: g.completed_children,
        total: g.total_children,
        blocking_task_ids: g.blocking_child_ids.clone(),
    });

    let (stage, condition, next_action, future) = if is_terminal_status(&task.status) {
        terminal_progress(&task, graph.as_ref())
    } else {
        match journey {
            TaskJourney::DecomposedSource => {
                decomposition_current_progress(&task, graph.as_ref(), &attempts, &live)
            }
            TaskJourney::DirectImplementation | TaskJourney::ReviewOnly => {
                execution_current_progress(&task, journey, r2_required, &runs, &live, &dependencies)
            }
        }
    };

    let mut milestones = history.clone();
    milestones.push(TaskProgressMilestone {
        stage: stage.label.clone(),
        role: stage.role.clone(),
        state: if is_terminal_status(&task.status) {
            TaskProgressMilestoneState::Terminal
        } else {
            TaskProgressMilestoneState::Current
        },
        activity: Some(stage.activity.clone()),
    });
    milestones.extend(future);
    milestones.truncate(TASK_PROGRESS_MILESTONE_LIMIT);

    let _ = now; // Kept request-scoped for a stable public projection signature.
    Ok(Some(TaskProgress {
        task_id,
        journey,
        milestones,
        history,
        stage,
        condition,
        next_action,
        attempts: task_attempts,
        children,
    }))
}

fn progress_graph(conn: &Connection, task_id: i64) -> Result<Option<ProgressGraph>> {
    let graph = conn
        .query_row(
            "SELECT id,state,proposal_attempts,provider_failures,operator_retry_count,
                    hold_code,hold_summary,accepted_plan_revision
             FROM task_decompositions WHERE source_task_id=?1",
            [task_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((
        id,
        state,
        proposal_attempts,
        provider_failures,
        operator_retry_count,
        hold_code,
        hold_summary,
        accepted_plan_revision,
    )) = graph
    else {
        return Ok(None);
    };
    let total_children: i64 = conn.query_row(
        "SELECT count(*) FROM task_graph_members WHERE graph_id=?1 AND active=1",
        [id],
        |row| row.get(0),
    )?;
    let completed_children: i64 = conn.query_row(
        "SELECT count(*) FROM task_graph_members m JOIN tasks t ON t.id=m.task_id
         WHERE m.graph_id=?1 AND m.active=1 AND t.status='done'",
        [id],
        |row| row.get(0),
    )?;
    let blocking_child_ids = conn
        .prepare(
            "SELECT m.task_id FROM task_graph_members m JOIN tasks t ON t.id=m.task_id
             WHERE m.graph_id=?1 AND m.active=1 AND t.status IN ('failed','cancelled')
             ORDER BY m.task_id LIMIT ?2",
        )?
        .query_map(params![id, TASK_PROGRESS_DEPENDENCY_LIMIT as i64], |row| {
            row.get(0)
        })?
        .collect::<rusqlite::Result<Vec<i64>>>()?;
    Ok(Some(ProgressGraph {
        id,
        state,
        proposal_attempts,
        provider_failures,
        operator_retry_count,
        hold_code,
        hold_summary,
        accepted_plan_revision,
        completed_children,
        total_children,
        blocking_child_ids,
    }))
}

fn progress_attempts(
    conn: &Connection,
    graph: Option<&ProgressGraph>,
) -> Result<Vec<ProgressAttempt>> {
    let Some(graph) = graph else {
        return Ok(Vec::new());
    };
    let mut attempts = conn
        .prepare(
            "SELECT kind,reason_code FROM decomposition_attempts WHERE graph_id=?1
             ORDER BY id DESC LIMIT ?2",
        )?
        .query_map(
            params![graph.id, TASK_PROGRESS_HISTORY_LIMIT as i64],
            |row| {
                Ok(ProgressAttempt {
                    kind: row.get(0)?,
                    reason_code: row.get(1)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    attempts.reverse();
    Ok(attempts)
}

fn progress_runs(conn: &Connection, task_id: i64) -> Result<Vec<crate::agent_runs::AgentRun>> {
    let total: i64 = conn.query_row(
        "SELECT count(*) FROM agent_runs WHERE task_id=?1",
        [task_id],
        |row| row.get(0),
    )?;
    let mut runs = conn
        .prepare(
            "SELECT id,agent_name,role,sub_role,model,effort,provider,role_assignment_id,
                    spawned_at,ended_at,end_reason
             FROM agent_runs WHERE task_id=?1 ORDER BY id DESC LIMIT ?2",
        )?
        .query_map(
            params![task_id, TASK_PROGRESS_HISTORY_LIMIT as i64],
            |row| {
                Ok(crate::agent_runs::AgentRun {
                    id: row.get(0)?,
                    agent: row.get(1)?,
                    role: row.get(2)?,
                    sub_role: row.get(3)?,
                    model: row.get(4)?,
                    effort: row.get(5)?,
                    provider: row.get(6)?,
                    role_assignment_id: row.get(7)?,
                    spawned_at: row.get(8)?,
                    ended_at: row.get(9)?,
                    end_reason: row.get(10)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    runs.reverse();
    if total > TASK_PROGRESS_HISTORY_LIMIT as i64 {
        let omitted = total - TASK_PROGRESS_HISTORY_LIMIT as i64;
        runs.insert(
            0,
            crate::agent_runs::AgentRun {
                id: -omitted,
                agent: String::new(),
                role: "history".into(),
                sub_role: None,
                model: String::new(),
                effort: String::new(),
                provider: None,
                role_assignment_id: None,
                spawned_at: 0,
                ended_at: Some(0),
                end_reason: Some(format!("{omitted} earlier completed runs")),
            },
        );
    }
    Ok(runs)
}

fn progress_live_runs(conn: &Connection, task_id: i64) -> Result<Vec<ProgressLiveRun>> {
    conn.prepare(
        "SELECT role,phase,agent_state FROM journal WHERE task_id=?1
         ORDER BY updated_at DESC,agent LIMIT 4",
    )?
    .query_map([task_id], |row| {
        Ok(ProgressLiveRun {
            role: row.get(0)?,
            phase: row.get(1)?,
            agent_state: row.get(2)?,
        })
    })?
    .collect::<rusqlite::Result<Vec<_>>>()
    .map_err(Into::into)
}

fn has_r2_evidence(
    conn: &Connection,
    task_id: i64,
    runs: &[crate::agent_runs::AgentRun],
) -> Result<bool> {
    if runs.iter().any(|run| run.sub_role.as_deref() == Some("r2")) {
        return Ok(true);
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM role_assignments
         WHERE task_id=?1 AND role='reviewer' AND review_stage='r2')",
        [task_id],
        |row| row.get(0),
    )?)
}

fn unsatisfied_dependency_ids(conn: &Connection, raw: Option<&str>) -> Result<DependencyProgress> {
    let dependencies = raw
        .and_then(|json| serde_json::from_str::<Vec<i64>>(json).ok())
        .unwrap_or_default();
    let mut progress = DependencyProgress::default();
    for id in dependencies
        .into_iter()
        .take(TASK_PROGRESS_DEPENDENCY_LIMIT)
    {
        let status = conn
            .query_row("SELECT status FROM tasks WHERE id=?1", [id], |row| {
                row.get::<_, String>(0)
            })
            .optional()?;
        if status.as_deref() != Some("done") {
            if status.as_deref() == Some("cancelled") {
                progress.cancelled_ids.push(id);
            }
            progress.blocked_ids.push(id);
        }
    }
    Ok(progress)
}

fn execution_history(
    task: &crate::tasks::Task,
    runs: &[crate::agent_runs::AgentRun],
) -> Vec<TaskProgressMilestone> {
    let mut history = Vec::new();
    if task.status != "open" || !runs.is_empty() {
        push_completed(&mut history, "Queued", None, "Received");
    }
    for run in runs {
        if run.role == "history" {
            push_completed(
                &mut history,
                "Earlier activity",
                None,
                run.end_reason
                    .as_deref()
                    .unwrap_or("Earlier completed runs"),
            );
            continue;
        }
        if run.ended_at.is_none() {
            continue;
        }
        match (run.role.as_str(), run.sub_role.as_deref()) {
            ("worker", _) => push_completed(
                &mut history,
                "Implementation",
                Some("Worker"),
                run_outcome(run.end_reason.as_deref()),
            ),
            ("reviewer", Some("r2")) => push_completed(
                &mut history,
                "Final review",
                Some("R2"),
                run_outcome(run.end_reason.as_deref()),
            ),
            ("reviewer", _) => push_completed(
                &mut history,
                "First review",
                Some("R1"),
                run_outcome(run.end_reason.as_deref()),
            ),
            _ => {}
        }
    }
    let has_worker = history.iter().any(|m| m.stage == "Implementation");
    if !has_worker
        && (task.author.is_some()
            || matches!(
                task.status.as_str(),
                "in-review" | "rework" | "merging" | "done"
            ))
    {
        push_completed(
            &mut history,
            "Implementation",
            Some("Worker"),
            "Submitted for review",
        );
    }
    let has_review = history
        .iter()
        .any(|m| m.stage == "First review" || m.stage == "Final review");
    if !has_review && matches!(task.status.as_str(), "rework" | "merging" | "done") {
        push_completed(&mut history, "First review", Some("R1"), "Review completed");
    }
    history
}

fn decomposition_history(
    graph: Option<&ProgressGraph>,
    attempts: &[ProgressAttempt],
) -> Vec<TaskProgressMilestone> {
    let mut history = Vec::new();
    if graph.is_some() {
        push_completed(
            &mut history,
            "Intake / classification",
            None,
            "Classified for planning",
        );
    }
    for attempt in attempts {
        if attempt.reason_code.starts_with("arbiter-") {
            push_completed(
                &mut history,
                "Plan review",
                Some("Arbiter"),
                arbiter_outcome(&attempt.reason_code),
            );
        } else {
            push_completed(
                &mut history,
                "Planning",
                Some("Planner"),
                planning_attempt_outcome(attempt),
            );
        }
    }
    // Moving beyond `validating` is durable proof that the current proposal cleared
    // the Arbiter gate. A `planning` task alone is intentionally never such proof.
    if let Some(graph) = graph {
        if matches!(
            graph.state.as_str(),
            "preclassifying" | "active" | "blocked" | "completed"
        ) || graph.accepted_plan_revision.is_some()
        {
            let already_approved = history.iter().any(|milestone| {
                milestone.stage == "Plan review"
                    && milestone.activity.as_deref() == Some("Approved")
            });
            if !already_approved {
                push_completed(&mut history, "Plan review", Some("Arbiter"), "Approved");
            }
        }
    }
    history
}

fn execution_current_progress(
    task: &crate::tasks::Task,
    journey: TaskJourney,
    r2_required: bool,
    runs: &[crate::agent_runs::AgentRun],
    live: &[ProgressLiveRun],
    dependencies: &DependencyProgress,
) -> (
    TaskProgressStage,
    Option<String>,
    Option<TaskProgressNextAction>,
    Vec<TaskProgressMilestone>,
) {
    let live_worker = live.iter().find(|run| run.role == "worker");
    let live_reviewer = live.iter().find(|run| run.role == "reviewer");
    let r1_approved = runs.iter().rev().any(|run| {
        run.role == "reviewer"
            && run.sub_role.is_none()
            && run.end_reason.as_deref() == Some("verdict:approved")
    });
    let dependency_condition = if !dependencies.cancelled_ids.is_empty() {
        Some(deadlocked_by(&dependencies.cancelled_ids))
    } else if !dependencies.blocked_ids.is_empty() {
        Some(blocked_by("dependency tasks", &dependencies.blocked_ids))
    } else {
        None
    };
    let review_stage = if r2_required && r1_approved {
        ("Final review", Some("R2"), "Awaiting R2 review")
    } else {
        ("First review", Some("R1"), "Awaiting R1 review")
    };
    let (stage, condition, next) = match task.status.as_str() {
        "open" => {
            let activity = if journey == TaskJourney::ReviewOnly {
                "Received; awaiting review assignment"
            } else {
                "Waiting for worker assignment"
            };
            (
                TaskProgressStage {
                    label: "Queued".into(),
                    role: None,
                    activity: activity.into(),
                },
                dependency_condition,
                Some(possible_next(if journey == TaskJourney::ReviewOnly {
                    "A reviewer may be assigned"
                } else {
                    "A worker may be assigned"
                })),
            )
        }
        "working" | "rework" => {
            let activity = live_worker
                .map(|run| worker_activity(run, task.rework_round))
                .unwrap_or_else(|| {
                    if task.status == "rework" {
                        format!(
                            "Awaiting remediation for rework round {}",
                            task.rework_round
                        )
                    } else {
                        "Awaiting worker progress".into()
                    }
                });
            let condition = dependency_condition.or_else(|| live_worker.and_then(live_condition));
            (
                TaskProgressStage {
                    label: if journey == TaskJourney::ReviewOnly && task.status == "rework" {
                        "Remediation".into()
                    } else {
                        "Implementation".into()
                    },
                    role: Some("Worker".into()),
                    activity,
                },
                condition,
                Some(possible_next("The worker may submit work for review")),
            )
        }
        "in-review" => {
            let activity = live_reviewer
                .map(|run| reviewer_activity(run, review_stage.0))
                .unwrap_or_else(|| review_stage.2.into());
            let condition = match live_reviewer {
                Some(run) => live_condition(run),
                None => Some(if review_stage.1 == Some("R2") {
                    "Awaiting R2 assignment".into()
                } else {
                    "Awaiting R1 assignment".into()
                }),
            };
            (
                TaskProgressStage {
                    label: review_stage.0.into(),
                    role: review_stage.1.map(str::to_string),
                    activity,
                },
                condition,
                Some(possible_next("The reviewer may approve or request changes")),
            )
        }
        "merging" => {
            let waiting_conflict = task
                .body
                .as_deref()
                .is_some_and(|body| body.contains(crate::tasks::MERGE_BLOCKED_BODY));
            (
                TaskProgressStage {
                    label: "Merge".into(),
                    role: None,
                    activity: "Merge is in progress".into(),
                },
                Some(
                    if waiting_conflict {
                        "Waiting for merge conflict resolution"
                    } else {
                        "Waiting for CI"
                    }
                    .into(),
                ),
                Some(possible_next("CI and merge policy may permit completion")),
            )
        }
        _ => (
            TaskProgressStage {
                label: "Queued".into(),
                role: None,
                activity: "Awaiting lifecycle dispatch".into(),
            },
            dependency_condition,
            Some(possible_next(
                "The daemon may dispatch the next lifecycle step",
            )),
        ),
    };
    let future = execution_future(&stage.label, r2_required, journey);
    (stage, condition, next, future)
}

fn decomposition_current_progress(
    task: &crate::tasks::Task,
    graph: Option<&ProgressGraph>,
    attempts: &[ProgressAttempt],
    live: &[ProgressLiveRun],
) -> (
    TaskProgressStage,
    Option<String>,
    Option<TaskProgressNextAction>,
    Vec<TaskProgressMilestone>,
) {
    let Some(graph) = graph else {
        // A legacy/source row can be `planning` before its aggregate is visible. It is
        // planning, but makes no unsupported claim that an Arbiter review completed.
        let stage = TaskProgressStage {
            label: "Planning".into(),
            role: Some("Planner".into()),
            activity: "Awaiting decomposition record".into(),
        };
        return (
            stage.clone(),
            Some("Waiting for planning intake".into()),
            Some(possible_next("The planner may begin decomposition")),
            decomposition_future(&stage.label),
        );
    };
    let arbiter_live = live.iter().find(|run| run.role == "arbiter");
    let (stage, condition, next) = match graph.state.as_str() {
        "validating" => (
            TaskProgressStage {
                label: "Plan review".into(),
                role: Some("Arbiter".into()),
                activity: arbiter_live
                    .map(|_| "Arbiter is reviewing the plan".into())
                    .unwrap_or_else(|| "Awaiting Arbiter verdict".into()),
            },
            arbiter_live.and_then(live_condition),
            Some(possible_next(
                "The Arbiter may approve, request changes, or reject the plan",
            )),
        ),
        "preclassifying" => (
            TaskProgressStage {
                label: "Plan accepted".into(),
                role: None,
                activity: "Classifying accepted child work".into(),
            },
            None,
            Some(possible_next(
                "Accepted work may be materialized as child tasks",
            )),
        ),
        "active" | "blocked" | "completed" => {
            let condition = if !graph.blocking_child_ids.is_empty() {
                Some(blocked_by("child tasks", &graph.blocking_child_ids))
            } else if graph.state == "blocked" {
                Some(planning_hold_condition(graph))
            } else {
                None
            };
            let activity = if graph.total_children == 0 && graph.state == "completed" {
                "All child work completed".into()
            } else {
                format!(
                    "{}/{} child tasks complete",
                    graph.completed_children, graph.total_children
                )
            };
            (
                TaskProgressStage {
                    label: "Child execution".into(),
                    role: None,
                    activity,
                },
                condition.clone(),
                Some(if condition.is_some() {
                    possible_next("An operator may resolve or retry blocked child tasks")
                } else if graph.completed_children == graph.total_children {
                    deterministic_next("The source task completes")
                } else {
                    possible_next("Child tasks may complete")
                }),
            )
        }
        "held" => (
            TaskProgressStage {
                label: "Planning".into(),
                role: Some("Planner".into()),
                activity: "Planning is held".into(),
            },
            Some(planning_hold_condition(graph)),
            Some(possible_next(
                "An operator may retry or resolve the planning hold",
            )),
        ),
        "provider-backoff" => (
            TaskProgressStage {
                label: "Planning".into(),
                role: Some("Planner".into()),
                activity: "Waiting to retry the planner provider".into(),
            },
            Some("Waiting for planner provider".into()),
            Some(possible_next("The planner provider may be retried")),
        ),
        _ => (
            TaskProgressStage {
                label: "Planning".into(),
                role: Some("Planner".into()),
                activity: "Preparing a bounded task plan".into(),
            },
            planning_condition(graph, attempts),
            Some(possible_next("The planner may submit a proposal")),
        ),
    };
    let future = decomposition_future(&stage.label);
    let _ = task;
    (stage, condition, next, future)
}

fn terminal_progress(
    task: &crate::tasks::Task,
    graph: Option<&ProgressGraph>,
) -> (
    TaskProgressStage,
    Option<String>,
    Option<TaskProgressNextAction>,
    Vec<TaskProgressMilestone>,
) {
    match task.status.as_str() {
        "done" => (
            TaskProgressStage {
                label: "Complete".into(),
                role: None,
                activity: "Task completed".into(),
            },
            None,
            None,
            Vec::new(),
        ),
        "cancelled" => (
            TaskProgressStage {
                label: "Cancelled".into(),
                role: None,
                activity: "Task was cancelled".into(),
            },
            Some("Operator cancelled the task".into()),
            None,
            Vec::new(),
        ),
        _ => {
            let retry_eligible = retry_eligible(task, graph);
            (
                TaskProgressStage {
                    label: "Failed".into(),
                    role: None,
                    activity: "Task failed before completion".into(),
                },
                Some(
                    if retry_eligible {
                        "Retry eligible"
                    } else {
                        "Operator decision required"
                    }
                    .into(),
                ),
                retry_eligible.then(|| possible_next("An operator may retry the task")),
                Vec::new(),
            )
        }
    }
}

fn execution_future(
    current: &str,
    r2_required: bool,
    journey: TaskJourney,
) -> Vec<TaskProgressMilestone> {
    let mut labels: Vec<(&str, Option<&str>)> = match current {
        "Queued" => vec![
            ("Implementation", Some("Worker")),
            ("First review", Some("R1")),
        ],
        "Implementation" => vec![("First review", Some("R1"))],
        "Remediation" => vec![("First review", Some("R1"))],
        "First review" => Vec::new(),
        "Final review" => vec![("Merge", None), ("Complete", None)],
        "Merge" => vec![("Complete", None)],
        _ => Vec::new(),
    };
    if r2_required && current != "Final review" && current != "Merge" {
        labels.push(("Final review", Some("R2")));
    }
    if current != "Merge" && current != "Final review" {
        labels.push(("Merge", None));
        labels.push(("Complete", None));
    }
    if journey == TaskJourney::ReviewOnly {
        labels.retain(|(label, _)| *label != "Implementation");
    }
    future_milestones(labels)
}

fn decomposition_future(current: &str) -> Vec<TaskProgressMilestone> {
    let labels = match current {
        "Intake / classification" | "Planning" => vec![
            ("Plan review", Some("Arbiter")),
            ("Plan accepted", None),
            ("Child execution", None),
            ("Complete", None),
        ],
        "Plan review" => vec![
            ("Plan accepted", None),
            ("Child execution", None),
            ("Complete", None),
        ],
        "Plan accepted" => vec![("Child execution", None), ("Complete", None)],
        "Child execution" => vec![("Complete", None)],
        _ => Vec::new(),
    };
    future_milestones(labels)
}

fn future_milestones(labels: Vec<(&str, Option<&str>)>) -> Vec<TaskProgressMilestone> {
    labels
        .into_iter()
        .map(|(stage, role)| TaskProgressMilestone {
            stage: stage.into(),
            role: role.map(str::to_string),
            state: TaskProgressMilestoneState::Future,
            activity: None,
        })
        .collect()
}

fn push_completed(
    history: &mut Vec<TaskProgressMilestone>,
    stage: &str,
    role: Option<&str>,
    activity: &str,
) {
    if history.len() < TASK_PROGRESS_HISTORY_LIMIT {
        history.push(TaskProgressMilestone {
            stage: stage.into(),
            role: role.map(str::to_string),
            state: TaskProgressMilestoneState::Completed,
            activity: Some(bounded_progress_text(activity)),
        });
    }
}

fn run_outcome(end_reason: Option<&str>) -> &'static str {
    match end_reason {
        Some("verdict:approved") => "Approved",
        Some(reason) if reason.starts_with("verdict:changes") => "Changes requested",
        Some("cancelled") => "Cancelled",
        Some("drain") => "Paused for daemon drain",
        Some(_) => "Run completed",
        None => "Run completed",
    }
}

fn arbiter_outcome(code: &str) -> &'static str {
    match code {
        "arbiter-changes" => "Changes requested",
        "arbiter-reject-source" => "Rejected; operator decision required",
        "arbiter-provider" => "Provider failure",
        _ => "Arbiter outcome recorded",
    }
}

fn planning_attempt_outcome(attempt: &ProgressAttempt) -> &'static str {
    match attempt.kind.as_str() {
        "provider" => "Planner provider attempt ended",
        "blocker" => "Planning blocked",
        _ => "Proposal returned for revision",
    }
}

fn worker_activity(run: &ProgressLiveRun, rework_round: i64) -> String {
    if run.phase == "working" && rework_round > 0 {
        format!("Applying remediation for rework round {rework_round}")
    } else if run.phase == "working" {
        "Implementation in progress".into()
    } else {
        bounded_progress_text(&format!("Worker phase: {}", run.phase))
    }
}

fn reviewer_activity(run: &ProgressLiveRun, stage: &str) -> String {
    if run.phase == "reviewing" {
        format!("{stage} in progress")
    } else {
        bounded_progress_text(&format!("Reviewer phase: {}", run.phase))
    }
}

fn live_condition(run: &ProgressLiveRun) -> Option<String> {
    match run.agent_state.as_deref() {
        Some("blocked") => Some("Blocked by managed agent".into()),
        Some("needs-info") => Some("Waiting for operator input".into()),
        Some("failed") => Some("Managed agent failed; recovery pending".into()),
        Some(note) if !note.is_empty() => Some(bounded_progress_text(note)),
        _ => None,
    }
}

fn planning_condition(graph: &ProgressGraph, attempts: &[ProgressAttempt]) -> Option<String> {
    if graph.state == "freeze-requested" || graph.state == "draining" {
        return Some("Waiting for planning intake".into());
    }
    attempts
        .last()
        .filter(|attempt| attempt.reason_code == "arbiter-changes")
        .map(|_| "Planner is revising Arbiter feedback".into())
}

fn planning_hold_condition(graph: &ProgressGraph) -> String {
    graph
        .hold_summary
        .as_deref()
        .filter(|summary| !summary.is_empty())
        .map(|summary| render_graph_hold_summary(graph.hold_code.as_deref(), summary))
        .map(|summary| bounded_progress_text(&summary))
        .unwrap_or_else(|| {
            graph
                .hold_code
                .as_deref()
                .map(|code| format!("Planning held: {}", bounded_progress_text(code)))
                .unwrap_or_else(|| "Planning is held".into())
        })
}

/// Preserve a readable status surface while graph holds retain structured
/// identity for guarded recovery writes.
fn render_graph_hold_summary(hold_code: Option<&str>, summary: &str) -> String {
    if hold_code == Some("generated-child-failed") {
        if let Some((task_id, reason)) = serde_json::from_str::<serde_json::Value>(summary)
            .ok()
            .and_then(|value| {
                Some((
                    value.get("affected_task")?.as_i64()?,
                    value.get("reason")?.as_str()?.to_owned(),
                ))
            })
        {
            return format!("Generated child task #{task_id} failed: {reason}");
        }
    }
    summary.to_owned()
}

fn retry_eligible(task: &crate::tasks::Task, graph: Option<&ProgressGraph>) -> bool {
    let task_retry = task
        .refs
        .as_deref()
        .and_then(|refs| serde_json::from_str::<serde_json::Value>(refs).ok())
        .and_then(|refs| refs.get(crate::tasks::PARKED_RESUME_STATUS_REF).cloned())
        .and_then(|value| value.as_str().map(str::to_owned))
        .is_some();
    task_retry
        || graph.is_some_and(|graph| {
            matches!(
                graph.hold_code.as_deref(),
                Some("proposal-attempts-exhausted" | "provider-attempts-exhausted")
            ) && graph.operator_retry_count < crate::decomposition::MAX_OPERATOR_RETRIES
        })
}

fn possible_next(action: &str) -> TaskProgressNextAction {
    TaskProgressNextAction {
        label: "Possible next".into(),
        action: bounded_progress_text(action),
    }
}

fn deterministic_next(action: &str) -> TaskProgressNextAction {
    TaskProgressNextAction {
        label: "Next".into(),
        action: bounded_progress_text(action),
    }
}

fn blocked_by(noun: &str, ids: &[i64]) -> String {
    let ids = ids
        .iter()
        .take(TASK_PROGRESS_DEPENDENCY_LIMIT)
        .map(|id| format!("#{id}"))
        .collect::<Vec<_>>()
        .join(" and ");
    bounded_progress_text(&format!("Blocked by {noun} {ids}"))
}

fn deadlocked_by(ids: &[i64]) -> String {
    let ids = ids
        .iter()
        .take(TASK_PROGRESS_DEPENDENCY_LIMIT)
        .map(|id| format!("#{id}"))
        .collect::<Vec<_>>()
        .join(" and ");
    bounded_progress_text(&format!("Deadlocked by cancelled dependency tasks {ids}"))
}

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "done" | "failed" | "cancelled")
}

fn bounded_progress_text(text: &str) -> String {
    let mut chars = text.chars();
    let bounded: String = chars.by_ref().take(TASK_PROGRESS_TEXT_LIMIT).collect();
    if chars.next().is_some() {
        format!(
            "{}…",
            bounded
                .chars()
                .take(TASK_PROGRESS_TEXT_LIMIT.saturating_sub(1))
                .collect::<String>()
        )
    } else {
        bounded
    }
}

/// Small, bounded pieces of the status snapshot used by the polling web dashboard.
/// Keep this separate from [`stats`]: the terminal status command intentionally includes
/// complete persistent collections, which are unsuitable for a request made every 2s.
pub fn web_task_counts(conn: &Connection) -> Result<Vec<StatusCount>> {
    let mut stmt = conn
        .prepare("SELECT status, count(*) FROM tasks GROUP BY status ORDER BY status LIMIT 32")?;
    let counts = stmt
        .query_map([], |r| {
            Ok(StatusCount {
                status: r.get(0)?,
                count: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(counts)
}

/// The dashboard alert pane has the same bounded semantics as `status`.
pub fn web_alerts(conn: &Connection, now: i64) -> Result<Vec<AlertMessage>> {
    alert_messages(conn, now)
}

/// The dashboard error pane has the same bounded semantics as `status`.
pub fn web_recent_errors(conn: &Connection, now: i64) -> Result<Vec<DedupedError>> {
    Ok(deduped_errors(conn, now)?.0)
}

/// The dashboard's bounded live-agent view. Reuse the journal-backed projection
/// from `quorum status` so both surfaces agree about current daemon work.
pub fn web_daemon_agents(conn: &Connection, now: i64) -> Result<Vec<DaemonAgentView>> {
    daemon_agents_view(conn, now)
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

/// Claimable (ready) task count grouped by required tier (#86). Uses
/// [`extract_tier_from_labels`] over each task row in app-space. Only counts tasks
/// whose dependencies are all satisfied (`ready=true`); blocked tasks are surfaced
/// separately via [`blocked_tasks`]. Tasks with `review_only=1` land in a distinct
/// `review` bucket.
fn queue_by_tier(conn: &Connection) -> Result<Vec<TierQueueCount>> {
    let mut stmt = conn.prepare(
        "SELECT id, labels, depends_on, review_only FROM tasks WHERE status IN ('open', 'in-review')",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let id: i64 = r.get(0)?;
            let labels: Option<String> = r.get(1)?;
            let depends_on: Option<String> = r.get(2)?;
            let review_only: bool = r.get::<_, i64>(3)? != 0;
            Ok((id, labels, depends_on, review_only))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut counts: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    for (_id, labels, depends_on, review_only) in &rows {
        let ready = crate::tasks::compute_ready(conn, depends_on)?;
        if !ready {
            continue;
        }
        let bucket = if *review_only {
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

/// Open tasks blocked by unmet dependencies (#86) plus daemon-parked
/// (`status='failed'`) tasks whose `depends_on` currently contains any
/// cancelled dep. The latter are the operator disposition queue — task-retry
/// refuses until `depends_on` is edited or the dependent is closed.
///
/// Deliberately infers the unsatisfiable condition from the live dep graph
/// instead of gating on the durable `daemon_parked_unsatisfiable` marker.
/// Reason: cancellation writes commit before any subsequent sweep would set
/// that marker (sweep_on_write runs before the mutator writes), so a purely
/// marker-gated read would hide the row for an unbounded interval after
/// cancellation. Inference is authoritative regardless of when refs get
/// refreshed by `converge_parked_dependents_of_cancelled`. Also covers
/// classifier-policy parks (their `daemon_parked_reason` stays "classifier
/// declined" so refs never gain the marker, but the cancelled dep is still
/// the operator disposition and must surface).
fn blocked_tasks(conn: &Connection) -> Result<Vec<BlockedTask>> {
    let mut stmt = conn.prepare(
        "SELECT t.id, t.title, t.labels, t.depends_on, t.status FROM tasks t
         WHERE t.depends_on IS NOT NULL
           AND (
               t.status='open'
               OR (
                   t.status='failed'
                   AND json_valid(t.refs)
                   AND json_extract(t.refs, '$.daemon_parked')=1
                   AND EXISTS (
                       SELECT 1 FROM json_each(t.depends_on) j
                       JOIN tasks d ON d.id = j.value
                       WHERE d.status = 'cancelled'
                   )
               )
           )",
    )?;
    let rows = stmt
        .query_map([], |r| {
            let id: i64 = r.get(0)?;
            let title: String = r.get(1)?;
            let labels: Option<String> = r.get(2)?;
            let depends_on: Option<String> = r.get(3)?;
            let status: String = r.get(4)?;
            Ok((id, title, labels, depends_on, status))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut blocked = Vec::new();
    for (id, title, labels, depends_on, status) in rows {
        // Parked-unsatisfiable rows always render (they are the disposition
        // queue). Open rows only render when they are actually blocked.
        if status == "open" {
            let ready = crate::tasks::compute_ready(conn, &depends_on)?;
            if ready {
                continue;
            }
        }
        let waiting_on = unmet_deps(conn, &depends_on)?;
        let deadlocked_on = cancelled_deps(conn, &depends_on)?;
        blocked.push(BlockedTask {
            id,
            title,
            provider: Some(task_display_identity(labels.as_deref()).0),
            model: Some(task_display_identity(labels.as_deref()).1),
            effort: Some(task_display_identity(labels.as_deref()).2),
            tier_eff: tier_eff_label(labels.as_deref()),
            waiting_on,
            deadlocked_on,
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

fn cancelled_deps(conn: &Connection, depends_on: &Option<String>) -> Result<Vec<i64>> {
    let Some(json) = depends_on.as_deref() else {
        return Ok(vec![]);
    };
    let mut stmt = conn.prepare(
        "SELECT je.value FROM json_each(?1) je
         WHERE EXISTS (
             SELECT 1 FROM tasks d WHERE d.id = je.value AND d.status = 'cancelled'
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
        "SELECT count(*) FROM tasks WHERE status='done' AND updated_at > ?1",
        params![hour_ago],
        |r| r.get(0),
    )?;
    let done_awaiting_review: i64 = conn.query_row(
        "SELECT count(*) FROM tasks WHERE status='in-review'",
        [],
        |r| r.get(0),
    )?;
    let in_review_filter = "status='in-review'";
    let oldest_done_ts: Option<i64> = conn
        .query_row(
            &format!("SELECT MIN(updated_at) FROM tasks WHERE {in_review_filter}"),
            [],
            |r| r.get(0),
        )
        .ok();
    let oldest_done_awaiting_review_secs = oldest_done_ts.map(|ts| (now - ts).max(0));
    let stuck_threshold = now - DONE_STUCK_THRESHOLD_SECS;
    let done_stuck_count: i64 = conn.query_row(
        &format!("SELECT count(*) FROM tasks WHERE {in_review_filter} AND updated_at < ?1"),
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

fn daemon_agents_view(conn: &Connection, now: i64) -> Result<Vec<DaemonAgentView>> {
    let entries = crate::journal::list_in_flight(conn)?;
    let mut views = Vec::with_capacity(entries.len());
    for e in entries {
        let live = e.log_dir.as_deref().and_then(read_live_stats);
        let last_activity_age_secs = if live.is_some() {
            e.log_dir.as_deref().and_then(live_sidecar_age_secs)
        } else {
            e.log_dir.as_deref().and_then(stream_jsonl_age_secs)
        };
        let (task_title, tier_eff) = if let Some(tid) = e.task_id {
            match conn.query_row(
                "SELECT title, labels FROM tasks WHERE id=?1",
                params![tid],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?)),
            ) {
                Ok((title, labels)) => {
                    let te = tier_eff_label(labels.as_deref());
                    (Some(title), Some(te))
                }
                Err(_) => (None, None),
            }
        } else {
            (None, None)
        };
        let (provider, model, effort) = if e.role == "planner" {
            planner_journal_identity(conn, e.task_id, &e.session_id, e.provider.as_deref())
        } else {
            e.task_id.map_or((None, None, None), |tid| {
                conn.query_row(
                    "SELECT provider, model, effort FROM agent_runs
                     WHERE task_id=?1 AND agent_name=?2 AND role=?3
                     ORDER BY spawned_at DESC, id DESC LIMIT 1",
                    params![tid, e.agent, e.role],
                    |r| Ok((r.get::<_, Option<String>>(0)?, r.get(1)?, r.get(2)?)),
                )
                .ok()
                .map(|(provider, model, effort)| {
                    (
                        // v31 added this nullable column; NULL is the documented
                        // legacy-Claude meaning, not an unknown provider.
                        provider.or_else(|| Some("claude".into())),
                        Some(model),
                        Some(effort),
                    )
                })
                .unwrap_or((None, None, None))
            })
        };
        let stream_counters = e.log_dir.as_deref().and_then(planner_stream_counters);
        let (
            tool_count,
            now_label,
            events_per_min,
            uptime_secs,
            mid_turn_tok,
            error_count,
            error_text,
        ) = if let Some(ref ls) = live {
            let up = if ls.spawn_epoch > 0 {
                Some(now - ls.spawn_epoch)
            } else {
                None
            };
            (
                ls.tools,
                if ls.now.is_empty() {
                    None
                } else {
                    Some(ls.now.clone())
                },
                Some(ls.evm),
                up,
                ls.mid_turn_tok,
                ls.error_count,
                ls.error_text.clone(),
            )
        } else {
            let counters = if e.role == "planner" {
                stream_counters.unwrap_or_default()
            } else {
                PlannerStreamCounters::default()
            };
            (counters.tool_count, None, None, None, 0, 0, None)
        };
        let display_tokens = e.cost_tokens + mid_turn_tok;
        let sub_role = if e.phase == "auditing" {
            Some("r2".to_string())
        } else {
            None
        };
        views.push(DaemonAgentView {
            agent: e.agent,
            role: e.role,
            sub_role,
            task_id: e.task_id,
            phase: e.phase,
            cost_tokens: display_tokens,
            agent_state: e.agent_state,
            cost_usd: e.cost_usd,
            log_dir: e.log_dir,
            last_activity_age_secs,
            task_title,
            provider,
            model,
            effort,
            tier_eff,
            pr: e.pr,
            rework_count: e.rework_count,
            tool_count,
            now_label,
            events_per_min,
            uptime_secs,
            live_error_count: error_count,
            live_error_text: error_text,
        });
    }
    Ok(views)
}

/// Planner runs do not create `agent_runs`: their immutable executable identity
/// belongs to the decomposition's role assignment. Match the live journal's
/// session id so a retained graph never lends an earlier attempt its identity.
fn planner_journal_identity(
    conn: &Connection,
    task_id: Option<i64>,
    session_id: &str,
    journal_provider: Option<&str>,
) -> (Option<String>, Option<String>, Option<String>) {
    let Some(task_id) = task_id else {
        return (journal_provider.map(str::to_owned), None, None);
    };
    conn.query_row(
        "SELECT d.planner_provider, d.planner_model, a.effort
         FROM task_decompositions d
         LEFT JOIN role_assignments a ON a.id=d.planner_assignment_id AND a.role='planner'
         WHERE d.source_task_id=?1 AND d.planner_session_id=?2
         ORDER BY d.updated_at DESC, d.id DESC LIMIT 1",
        params![task_id, session_id],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        },
    )
    .ok()
    .map(|(provider, model, effort)| {
        (
            journal_provider.map(str::to_owned).or(provider),
            model,
            effort,
        )
    })
    .unwrap_or((journal_provider.map(str::to_owned), None, None))
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PlannerStreamCounters {
    activity_count: u32,
    tool_count: u32,
}

/// Derive only structural counters from a bounded prefix of a current planner
/// stream. No field values or provider payloads leave this function. It accepts
/// both the closed sanitized stream and the older planner JSONL shape while
/// migration is in progress.
fn planner_stream_counters(log_dir: &str) -> Option<PlannerStreamCounters> {
    let path = std::path::Path::new(log_dir).join("stream.jsonl");
    let file = std::fs::File::open(path).ok()?;
    let mut bytes = Vec::new();
    file.take(PLANNER_STREAM_INSPECT_BYTES)
        .read_to_end(&mut bytes)
        .ok()?;

    let mut counters = PlannerStreamCounters::default();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .take(PLANNER_STREAM_INSPECT_RECORDS)
    {
        let Ok(line) = std::str::from_utf8(line) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(event) = value.get("event").and_then(serde_json::Value::as_str) else {
            // Legacy planner JSONL has no closed event field. Count known
            // lifecycle/tool shapes only, and retain no provider text.
            let event_type = value.get("type").and_then(serde_json::Value::as_str);
            let item_type = value
                .get("item")
                .and_then(|item| item.get("type"))
                .and_then(serde_json::Value::as_str);
            if matches!(
                event_type,
                Some(
                    "thread.started"
                        | "turn.started"
                        | "turn.completed"
                        | "turn.failed"
                        | "item.started"
                        | "item.completed"
                )
            ) {
                counters.activity_count = counters.activity_count.saturating_add(1);
            }
            if matches!(
                (event_type, item_type),
                (
                    Some("item.started" | "item.completed"),
                    Some("command_execution" | "file_change" | "mcp_call")
                )
            ) {
                counters.tool_count = counters.tool_count.saturating_add(1);
            }
            continue;
        };
        if matches!(
            event,
            "provider_lifecycle"
                | "turn_lifecycle"
                | "command_summary"
                | "tool_summary"
                | "assistant_message"
                | "terminal_response"
                | "provider_failure"
                | "semantic_rejection"
                | "completion"
        ) {
            counters.activity_count = counters.activity_count.saturating_add(1);
        }
        // `assistant_message` is deliberately excluded: an assistant-only turn
        // is activity, not a tool action.
        if matches!(event, "command_summary" | "tool_summary") {
            counters.tool_count = counters.tool_count.saturating_add(1);
        }
    }
    Some(counters)
}

/// Build a compact `tier·eff` label from a task's JSON labels array.
/// e.g. `["tier:opus-46","effort:high"]` → `"opus46·hi"`.
pub fn tier_eff_label(labels_json: Option<&str>) -> String {
    let s = match labels_json {
        Some(s) => s,
        None => return "—".to_string(),
    };
    let v: serde_json::Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return "—".to_string(),
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return "—".to_string(),
    };
    let mut tier_part = String::new();
    let mut eff_part = String::new();
    let mut complexity: Option<&str> = None;
    for item in arr {
        if let Some(t) = item.as_str() {
            if let Some(rest) = t.strip_prefix("tier:") {
                tier_part = rest.replace('-', "");
            } else if let Some(rest) = t.strip_prefix("effort:") {
                eff_part = match rest {
                    "high" => "hi".to_string(),
                    "medium" | "med" => "md".to_string(),
                    other => other.to_string(),
                };
            } else if let Some(rest) = t.strip_prefix("complexity:") {
                complexity = Some(rest);
            }
        }
    }
    if tier_part.is_empty() && eff_part.is_empty() {
        return match complexity {
            Some(n) => format!("c{n}"),
            None => "—".to_string(),
        };
    }
    if eff_part.is_empty() {
        return tier_part;
    }
    if tier_part.is_empty() {
        return eff_part;
    }
    format!("{tier_part}·{eff_part}")
}

/// Individual claimable tasks for the QUEUE section (#204).
fn queue_tasks(conn: &Connection) -> Result<Vec<QueueTask>> {
    let mut stmt = conn.prepare(
        "SELECT id, title, labels, priority, refs, depends_on FROM tasks WHERE status='open'
         ORDER BY priority DESC, id ASC",
    )?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, Option<String>>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    for (id, title, labels, priority, refs, depends_on) in rows {
        if !crate::tasks::compute_ready(conn, &depends_on)? {
            continue;
        }
        let claimed: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM claims WHERE target='task#'||?1 AND active=1)",
                params![id],
                |r| r.get(0),
            )
            .unwrap_or(false);
        if claimed {
            continue;
        }
        result.push(QueueTask {
            id,
            title,
            provider: Some(task_display_identity(labels.as_deref()).0),
            model: Some(task_display_identity(labels.as_deref()).1),
            effort: Some(task_display_identity(labels.as_deref()).2),
            tier_eff: tier_eff_label(labels.as_deref()),
            priority,
            pr: extract_pr_from_refs(refs.as_deref()),
        });
    }
    Ok(result)
}

fn extract_pr_from_refs(refs_json: Option<&str>) -> Option<i64> {
    let s = refs_json?;
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    v.get("pr")?.as_i64()
}

/// Tasks that have been submitted and still require review or merging.
///
/// This is derived from durable task state so CI waits and reviewer-spawn failures
/// remain visible when no reviewer process is live.
fn reviewing_tasks(
    conn: &Connection,
    daemon_agents: &[DaemonAgentView],
) -> Result<Vec<ReviewingTask>> {
    let mut stmt = conn.prepare(
        "SELECT id, title, status, refs FROM (
             SELECT id, title, status, refs, updated_at FROM (
                 SELECT id, title, status, refs, updated_at FROM tasks
                 WHERE status='in-review'
                 ORDER BY updated_at DESC, id DESC
                 LIMIT ?1
             )
             UNION ALL
             SELECT id, title, status, refs, updated_at FROM (
                 SELECT id, title, status, refs, updated_at FROM tasks
                 WHERE status='merging'
                 ORDER BY updated_at DESC, id DESC
                 LIMIT ?1
             )
         )
         ORDER BY updated_at DESC, id DESC
         LIMIT ?1",
    )?;
    let rows = stmt
        .query_map(params![REVIEWING_TASK_LIMIT], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(rows
        .into_iter()
        .map(|(id, title, status, refs)| {
            let reviewer = daemon_agents
                .iter()
                .find(|agent| agent.role == "reviewer" && agent.task_id == Some(id))
                .map(|agent| agent.agent.clone());
            let state = if status == "merging" {
                "merging".to_string()
            } else if reviewer.is_some() {
                "reviewing".to_string()
            } else {
                "awaiting reviewer".to_string()
            };
            ReviewingTask {
                id,
                title,
                pr: extract_pr_from_refs(refs.as_deref()),
                reviewer,
                state,
            }
        })
        .collect())
}

/// Task pipeline view: daemon-owned source stages + recently done/closed (#204).
/// Done/closed tasks are time-windowed to the last hour to avoid unbounded growth.
/// Excludes `open` (already in QUEUE/BLOCKED), `cancelled`, and `parked`.
fn pipeline_tasks(conn: &Connection, now: i64) -> Result<Vec<PipelineTask>> {
    let hour_ago = now - 3600;
    let mut stmt = conn.prepare(
        "SELECT id, title, status, refs, depends_on FROM tasks
         WHERE status IN ('planning', 'decomposed')
         UNION ALL
         SELECT id, title, status, refs, depends_on FROM tasks
         WHERE status = 'done' AND updated_at > ?1
         UNION ALL
         SELECT id, title, status, refs, depends_on FROM tasks
         WHERE status = 'closed' AND updated_at > ?1
         ORDER BY id ASC",
    )?;
    let rows = stmt
        .query_map(params![hour_ago], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = Vec::new();
    for (id, title, status, refs, _depends_on) in rows {
        let pr = extract_pr_from_refs(refs.as_deref());
        let (provider, model, effort) = conn
            .query_row(
                "SELECT provider, model, effort FROM agent_runs WHERE task_id=?1
                 ORDER BY spawned_at DESC, id DESC LIMIT 1",
                params![id],
                |r| Ok((r.get::<_, Option<String>>(0)?, r.get(1)?, r.get(2)?)),
            )
            .ok()
            .map(|(provider, model, effort)| {
                (
                    // See the v31 migration: pre-existing rows are Claude.
                    Some(provider.unwrap_or_else(|| "claude".into())),
                    Some(model),
                    Some(effort),
                )
            })
            .unwrap_or_else(|| {
                (
                    Some("pending".into()),
                    Some("pending".into()),
                    Some("pending".into()),
                )
            });
        result.push(PipelineTask {
            id,
            title,
            provider,
            model,
            effort,
            status,
            pr,
            blocked: false,
            arbiter: arbiter_verdict(conn, id)?,
        });
    }
    Ok(result)
}

/// Read the newest verdict evidence for a decomposition source. Verdict rows are
/// observational, so select them by insertion order rather than their per-kind ordinal.
pub fn arbiter_verdict(conn: &Connection, source_task_id: i64) -> Result<Option<ArbiterVerdict>> {
    let row: Option<(String, i64, String)> = conn
        .query_row(
            "SELECT a.reason_code,a.created_at,a.summary
             FROM task_decompositions d
             JOIN decomposition_attempts a ON a.graph_id=d.id
             WHERE d.source_task_id=?1 AND a.kind='verdict'
             ORDER BY a.id DESC LIMIT 1",
            [source_task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    Ok(row.map(|(reason_code, at, summary)| {
        let summary: serde_json::Value = serde_json::from_str(&summary).unwrap_or_default();
        ArbiterVerdict {
            verdict: summary
                .get("verdict")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            reason_code,
            at,
            provider: summary
                .get("provider")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            model: summary
                .get("model")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            duration_ms: summary
                .get("duration_ms")
                .and_then(serde_json::Value::as_i64),
        }
    }))
}

/// The schema guarantees at most one active graph/freeze. A held planning result is
/// also useful owner-facing state, so fall back to the newest non-completed aggregate.
fn decomposition_status(conn: &Connection, now: i64) -> Result<Option<DecompositionStatusView>> {
    let graph = conn
        .query_row(
            "SELECT d.id, d.source_task_id, t.title, t.status, d.state, d.active,
                    d.proposal_attempts, d.provider_failures, d.planner_provider,
                    d.planner_model, a.effort, d.accepted_plan_revision, d.hold_summary,
                    d.hold_code,d.operator_retry_count
             FROM task_decompositions d
             JOIN tasks t ON t.id=d.source_task_id
             LEFT JOIN role_assignments a ON a.id=d.planner_assignment_id AND a.role='planner'
             WHERE d.active=1 OR d.freeze_active=1
                OR d.state NOT IN ('completed','cancelled')
             ORDER BY (d.active=1 OR d.freeze_active=1) DESC, d.updated_at DESC, d.id DESC
             LIMIT 1",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, bool>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, i64>(7)?,
                    r.get::<_, Option<String>>(8)?,
                    r.get::<_, Option<String>>(9)?,
                    r.get::<_, Option<String>>(10)?,
                    r.get::<_, Option<i64>>(11)?,
                    r.get::<_, Option<String>>(12)?,
                    r.get::<_, Option<String>>(13)?,
                    r.get::<_, i64>(14)?,
                ))
            },
        )
        .optional()?;
    let Some((
        graph_id,
        source_task_id,
        source_title,
        source_status,
        graph_state,
        graph_active,
        proposal_attempts,
        provider_failures,
        planner_provider,
        planner_model,
        planner_effort,
        accepted_plan_revision,
        hold_summary,
        hold_code,
        operator_retry_count,
    )) = graph
    else {
        return Ok(None);
    };
    let retryable_planning_hold =
        crate::decomposition::exhausted_planning_retry_is_eligible(conn, source_task_id, now)?;
    let planner_log_dir: Option<String> = conn
        .query_row(
            "SELECT log_dir FROM journal
             WHERE agent=?1 AND role='planner' AND task_id=?2
             LIMIT 1",
            params![format!("decomposition-planner-{graph_id}"), source_task_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let planner_counters = planner_log_dir.as_deref().and_then(planner_stream_counters);
    let planner_last_activity_age_secs = planner_log_dir.as_deref().and_then(stream_jsonl_age_secs);

    let mut stmt = conn.prepare(
        "SELECT m.task_id, m.local_key, t.title, t.status, t.depends_on
         FROM task_graph_members m JOIN tasks t ON t.id=m.task_id
         WHERE m.graph_id=?1 AND m.active=1
         ORDER BY m.local_key, m.task_id",
    )?;
    let members = stmt
        .query_map(params![graph_id], |r| {
            let depends_on: Option<String> = r.get(4)?;
            let prerequisites = depends_on
                .as_deref()
                .and_then(|json| serde_json::from_str::<Vec<i64>>(json).ok())
                .unwrap_or_default();
            Ok(DecompositionMemberView {
                task_id: r.get(0)?,
                local_key: r.get(1)?,
                title: r.get(2)?,
                status: r.get(3)?,
                prerequisites,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut counts = std::collections::BTreeMap::<String, i64>::new();
    let mut failed_children = Vec::new();
    let mut completed_children = 0;
    for member in &members {
        *counts.entry(member.status.clone()).or_default() += 1;
        if member.status == "done" || member.status == "closed" {
            completed_children += 1;
        }
        if member.status == "failed" || member.status == "cancelled" {
            failed_children.push(member.task_id);
        }
    }
    let child_statuses = counts
        .into_iter()
        .map(|(status, count)| StatusCount { status, count })
        .collect();

    // Keep reasons bounded independently of graph history. Summaries are already
    // length-bounded at their write boundary; no prompt or transcript is selected.
    let mut reasons = hold_summary
        .into_iter()
        .map(|summary| render_graph_hold_summary(hold_code.as_deref(), &summary))
        .collect::<Vec<_>>();
    let mut reason_stmt = conn.prepare(
        "SELECT summary FROM decomposition_attempts WHERE graph_id=?1
         ORDER BY id DESC LIMIT 6",
    )?;
    for reason in reason_stmt
        .query_map(params![graph_id], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
    {
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
    }
    reasons.truncate(6);

    let dispatch_hold = if graph_state != "active" || !graph_active {
        Some(format!(
            "implementation dispatch held: graph state={graph_state}, active={}",
            i64::from(graph_active)
        ))
    } else if source_status != "decomposed" {
        Some(format!(
            "implementation dispatch held: source status={source_status}"
        ))
    } else {
        None
    };

    Ok(Some(DecompositionStatusView {
        graph_id,
        source_task_id,
        source_title,
        source_status,
        graph_state,
        dispatch_hold,
        proposal_attempts,
        provider_failures,
        hold_code,
        retryable_planning_hold,
        operator_retry_count,
        operator_retry_cap: crate::decomposition::MAX_OPERATOR_RETRIES,
        planner_provider,
        planner_model,
        planner_effort,
        planner_log_dir,
        planner_last_activity_age_secs,
        planner_activity_count: planner_counters.map(|counters| counters.activity_count),
        planner_tool_count: planner_counters.map(|counters| counters.tool_count),
        accepted_plan_revision,
        completed_children,
        total_children: members.len() as i64,
        child_statuses,
        failed_children,
        reasons,
        members,
    }))
}

/// Resolve only an explicit task tier into a display identity. Queue/blocked status
/// is a read-only process and does not own the daemon's resolved serve config, so
/// missing values are deliberately marked pending rather than guessed.
fn task_display_identity(labels_json: Option<&str>) -> (String, String, String) {
    let labels: Vec<String> = labels_json
        .and_then(|value| serde_json::from_str(value).ok())
        .unwrap_or_default();
    let model = labels
        .iter()
        .filter_map(|label| label.strip_prefix("tier:"))
        .find_map(crate::model_tiers::model_id_for_tier)
        .map(str::to_string);
    let Some(model) = model else {
        return ("pending".into(), "pending".into(), "pending".into());
    };
    let provider = if model.starts_with("claude-") {
        "claude"
    } else {
        "codex"
    };
    let effort = labels
        .iter()
        .filter_map(|label| label.strip_prefix("effort:"))
        .find(|effort| matches!(*effort, "medium" | "high"))
        .unwrap_or("pending");
    (provider.to_string(), model, effort.to_string())
}

/// Deduped errors from the last hour, with a count of older silenced errors (#204).
fn deduped_errors(conn: &Connection, now: i64) -> Result<(Vec<DedupedError>, i64)> {
    let hour_ago = now - 3600;
    let mut stmt = conn.prepare(
        "SELECT detail, source, COUNT(*) as cnt, MAX(ts) as latest_ts
         FROM errors
         WHERE expires_at > ?1 AND ts > ?2
         GROUP BY detail, source
         ORDER BY latest_ts DESC
         LIMIT 10",
    )?;
    let recent = stmt
        .query_map(params![now, hour_ago], |r| {
            Ok(DedupedError {
                detail: r.get(0)?,
                source: r.get(1)?,
                count: r.get(2)?,
                latest_age_secs: (now - r.get::<_, i64>(3)?).max(0),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let older: i64 = conn.query_row(
        "SELECT COUNT(*) FROM errors WHERE expires_at > ?1 AND ts <= ?2",
        params![now, hour_ago],
        |r| r.get(0),
    )?;
    Ok((recent, older))
}

/// Owner-alert messages from the last 12 hours plus synthetic health alerts for
/// terminal tasks that still carry runnable daemon retry state.  The latter is
/// intentionally read-only so `quorum status` exposes latent corruption before
/// daemon startup reconciliation gets a chance to clean it.
fn alert_messages(conn: &Connection, now: i64) -> Result<Vec<AlertMessage>> {
    // Corrupt terminal retry authority is the signal this read-only surface
    // exists to expose before daemon reconciliation. Reserve display capacity
    // for it before ordinary persisted alerts, especially during alert-heavy
    // incidents.
    let mut terminal = conn.prepare(
        "SELECT id, status, updated_at
         FROM tasks INDEXED BY tasks_terminal_retry_recent
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
         ORDER BY updated_at DESC, id DESC
         LIMIT ?1",
    )?;
    let mut rows = terminal
        .query_map(params![ALERT_DISPLAY_LIMIT], |row| {
            let id = row.get::<_, i64>(0)?;
            let status = row.get::<_, String>(1)?;
            let updated_at = row.get::<_, i64>(2)?;
            Ok(AlertMessage {
                body: format!(
                    "task #{id} is terminal ({status}) but carries runnable daemon retry markers"
                ),
                refs: Some(format!("task#{id}")),
                age_secs: (now - updated_at).max(0),
                kind: "critical".into(),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let remaining = ALERT_DISPLAY_LIMIT - rows.len() as i64;
    if remaining > 0 {
        let window_start = now - ALERT_WINDOW_SECS;
        let mut stmt = conn.prepare(
            "SELECT body, refs, ts, kind
             FROM messages
             WHERE expires_at > ?1 AND ts > ?2 AND kind IN ('alert', 'critical')
             ORDER BY ts DESC
             LIMIT ?3",
        )?;
        let persisted = stmt
            .query_map(params![now, window_start, remaining], |r| {
                Ok(AlertMessage {
                    body: r.get(0)?,
                    refs: r.get(1)?,
                    age_secs: (now - r.get::<_, i64>(2)?).max(0),
                    kind: r.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.extend(persisted);
    }
    Ok(rows)
}

/// Tasks stuck waiting on external merge conditions (#177).
///
/// Two sources:
/// 1. Tasks with `body = MERGE_BLOCKED_BODY` — merge-blocked by conflict (review-only
///    tasks whose PR has conflicts; daemon parks them until mergeable again).
/// 2. Tasks in `merging` status — approved, waiting for CI/merge to complete.
///
/// Neither is terminal; dependents stay blocked by the normal dep rule.
fn merge_blockers(conn: &Connection, now: i64) -> Result<Vec<MergeBlockerView>> {
    let mut result = Vec::new();

    // 1. Conflict-blocked tasks (body = MERGE_BLOCKED_BODY, any non-terminal status).
    let mut stmt = conn.prepare(
        "SELECT id, title, refs, status, updated_at, rework_round
         FROM tasks
         WHERE body = ?1 AND status NOT IN ('done', 'failed', 'cancelled')
         ORDER BY updated_at ASC",
    )?;
    let rows = stmt
        .query_map(params![crate::tasks::MERGE_BLOCKED_BODY], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i32>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, title, refs, status, updated_at, rework_round) in rows {
        result.push(MergeBlockerView {
            task_id: id,
            title,
            pr: extract_pr_from_refs(refs.as_deref()),
            blocker_kind: "conflict".into(),
            status,
            waiting_secs: (now - updated_at).max(0),
            retry_count: rework_round,
        });
    }

    // 2. Tasks in 'merging' status (approved, pending CI / merge attempt).
    let mut stmt2 = conn.prepare(
        "SELECT id, title, refs, updated_at, rework_round
         FROM tasks
         WHERE status = 'merging'
         ORDER BY updated_at ASC",
    )?;
    let rows2 = stmt2
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i32>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, title, refs, updated_at, rework_round) in rows2 {
        if result.iter().any(|b| b.task_id == id) {
            continue;
        }
        result.push(MergeBlockerView {
            task_id: id,
            title,
            pr: extract_pr_from_refs(refs.as_deref()),
            blocker_kind: "ci_pending".into(),
            status: "merging".into(),
            waiting_secs: (now - updated_at).max(0),
            retry_count: rework_round,
        });
    }

    Ok(result)
}

/// Compute the health verdict (#204).
/// Thresholds: 60s silent → attention, 180s silent → stalled.
fn is_stall_eligible(d: &DaemonAgentView) -> bool {
    d.role == "worker" && d.phase != "awaiting-review"
}

fn compute_health(
    daemon_agents: &[DaemonAgentView],
    errors_recent: bool,
    alerts_present: bool,
) -> HealthVerdict {
    let has_dead = daemon_agents
        .iter()
        .filter(|d| is_stall_eligible(d))
        .any(|d| match d.last_activity_age_secs {
            Some(age) => age > 180,
            None => true,
        });
    if has_dead {
        return HealthVerdict::Stalled;
    }
    let has_stalling = daemon_agents
        .iter()
        .filter(|d| is_stall_eligible(d))
        .any(|d| match d.last_activity_age_secs {
            Some(age) => age > 60,
            None => false,
        });
    let has_live_error = daemon_agents.iter().any(|d| d.live_error_count > 0);
    if has_stalling || errors_recent || alerts_present || has_live_error {
        return HealthVerdict::Attention;
    }
    HealthVerdict::OnTrack
}

fn stream_jsonl_age_secs(log_dir: &str) -> Option<i64> {
    let path = std::path::Path::new(log_dir).join("stream.jsonl");
    let meta = std::fs::metadata(&path).ok()?;
    let mtime = meta.modified().ok()?;
    let age = mtime.elapsed().ok()?;
    Some(age.as_secs() as i64)
}

fn live_sidecar_age_secs(log_dir: &str) -> Option<i64> {
    let path = std::path::Path::new(log_dir).join("_daemon_live.json");
    let meta = std::fs::metadata(&path).ok()?;
    let mtime = meta.modified().ok()?;
    let age = mtime.elapsed().ok()?;
    Some(age.as_secs() as i64)
}

fn read_live_stats(log_dir: &str) -> Option<DaemonLiveStats> {
    let path = std::path::Path::new(log_dir).join("_daemon_live.json");
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn progress_task(c: &mut Connection, title: &str) -> i64 {
        crate::tasks::create(c, "owner", title, None, 0, None, None, None, None, 100).unwrap()
    }

    fn set_status(c: &Connection, task_id: i64, status: &str) {
        c.execute(
            "UPDATE tasks SET status=?2,updated_at=101 WHERE id=?1",
            params![task_id, status],
        )
        .unwrap();
    }

    fn progress_run(
        c: &Connection,
        task_id: i64,
        role: &str,
        sub_role: Option<&str>,
        end_reason: Option<&str>,
    ) {
        c.execute(
            "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at,
                 sub_role,ended_at,end_reason)
             VALUES(?1,'agent',?2,'model','high','codex',100,?3,
                    CASE WHEN ?4 IS NULL THEN NULL ELSE 101 END,?4)",
            params![task_id, role, sub_role, end_reason],
        )
        .unwrap();
    }

    fn progress_graph(c: &Connection, source_task_id: i64, state: &str) -> i64 {
        c.execute(
            "INSERT INTO task_decompositions(source_task_id,state,active,freeze_active,
                 planned_source_revision,created_at,updated_at)
             VALUES(?1,?2,0,0,1,100,100)",
            params![source_task_id, state],
        )
        .unwrap();
        c.last_insert_rowid()
    }

    fn projection(c: &Connection, task_id: i64) -> TaskProgress {
        task_progress(c, task_id, 200).unwrap().unwrap()
    }

    fn ready_claim(
        c: &mut Connection,
        agent: &str,
        task_id: Option<i64>,
        labels: &[&str],
        ttl: i64,
        now: i64,
    ) -> Result<Option<crate::tasks::Task>> {
        let ids = task_id.into_iter().collect::<Vec<_>>();
        for id in ids {
            crate::classify::store_classifications(
                c,
                &[crate::classify::TaskClassification {
                    task_id: id,
                    cx_est: 3,
                    size: "M".into(),
                    ready: true,
                    not_ready_reason: None,
                    duplicate_of: vec![],
                }],
                "unit-test:v2",
                now,
            )?;
        }
        crate::tasks::claim(c, agent, task_id, labels, ttl, now)
    }

    #[test]
    fn legacy_null_run_provider_displays_as_claude_live_and_pipeline() {
        let (_d, mut c) = open_tmp();
        let task_id = crate::tasks::create(
            &mut c,
            "boss",
            "legacy provider",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        c.execute(
            "INSERT INTO agent_runs (task_id, agent_name, role, model, effort, provider, spawned_at)
             VALUES (?1, 'W1', 'worker', 'claude-opus-4-6', 'high', NULL, 101)",
            params![task_id],
        )
        .unwrap();
        crate::journal::upsert(
            &mut c,
            &crate::journal::JournalEntry {
                agent: "W1".into(),
                role: "worker".into(),
                task_id: Some(task_id),
                session_id: "legacy-session".into(),
                worktree: None,
                branch: None,
                phase: "working".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: None,
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();
        let live = stats(&c, 102, crate::agents::ONLINE_WINDOW_SECS)
            .unwrap()
            .daemon_agents
            .pop()
            .unwrap();
        assert_eq!(live.provider.as_deref(), Some("claude"));

        c.execute(
            "UPDATE tasks SET status='done', updated_at=102 WHERE id=?1",
            params![task_id],
        )
        .unwrap();
        let pipeline = stats(&c, 103, crate::agents::ONLINE_WINDOW_SECS)
            .unwrap()
            .pipeline
            .pop()
            .unwrap();
        assert_eq!(pipeline.provider.as_deref(), Some("claude"));
        assert_eq!(pipeline.model.as_deref(), Some("claude-opus-4-6"));
        assert_eq!(pipeline.effort.as_deref(), Some("high"));
    }

    #[test]
    fn counts_exclude_expired_and_stale() {
        let (_d, mut c) = open_tmp();
        // Live message survives past now; dead message expired long ago.
        crate::feed::post(&mut c, "A", "info", None, "live", None, None, 4000, 100).unwrap();
        crate::feed::post(&mut c, "A", "info", None, "dead", None, None, 5, 100).unwrap();
        // Claim auto-renewed by touch to expires_at = MAX(1100, 100+3600) = 3700.
        crate::claims::claim(&mut c, "A", "pr#1", 1000, 100).unwrap();
        crate::tasks::create(&mut c, "A", "t", None, 0, None, None, None, None, 100).unwrap();

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
            None,
            100,
        )
        .unwrap();
        ready_claim(&mut c, "Worker", Some(tid), &[], 3600, 100).unwrap();
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
            Some(r#"["kind:bug","tier:opus-47"]"#),
            "kind:bug"
        ));
        assert!(!has_label(Some(r#"["kind:bug"]"#), "tier:opus-47"));
        assert!(!has_label(None, "tier:opus-47"));
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
            None,
            100,
        )
        .unwrap();
        ready_claim(&mut c, "Alice", Some(t46), &[], 1000, 100).unwrap();
        ready_claim(&mut c, "Bob", Some(t47), &[], 1000, 100).unwrap();
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
        crate::feed::post(&mut c, "NoTier", "info", None, "hi", None, None, 1000, 100).unwrap();
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
            None,
            100,
        )
        .unwrap();
        crate::tasks::create(&mut c, "boss", "d", None, 0, None, None, None, None, 100).unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "r",
            None,
            1000,
            None,
            None,
            None,
            Some(42),
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
                1000,
                100 + i,
            )
            .unwrap();
        }
        let long_body = "x".repeat(MSG_PREVIEW_CHARS + 50);
        crate::feed::post(&mut c, "A", "info", None, &long_body, None, None, 1000, 200).unwrap();

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

    // ── Agent load score (#95 Phase 1) ─────────────────────────────────

    fn make_task(c: &mut Connection, title: &str, now: i64) -> i64 {
        let id =
            crate::tasks::create(c, "boss", title, None, 0, None, None, None, None, now).unwrap();
        c.execute("UPDATE tasks SET refs=json_object('cx_est',3,'cx_size','M','cx_ready',true,'cx_by','test:v2') WHERE id=?1", [id]).unwrap();
        id
    }

    /// Drive one task through claim → done as `agent`, preserving assignee for
    /// load-score query testing. Uses direct SQL because the lifecycle paths
    /// (close_after_merge, close_manual) clear assignee — a separate issue (#114 note).
    fn complete_task_as(c: &mut Connection, agent: &str, claim_ts: i64, done_ts: i64) -> i64 {
        let id = make_task(c, &format!("t-{claim_ts}"), claim_ts - 1);
        ready_claim(c, agent, Some(id), &[], 3600, claim_ts).unwrap();
        c.execute(
            "UPDATE tasks SET status='done', updated_at=?2 WHERE id=?1",
            rusqlite::params![id, done_ts],
        )
        .unwrap();
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
    fn throughput_counts_oldest_in_review() {
        let (_d, mut c) = open_tmp();
        let t1 = crate::tasks::create(&mut c, "boss", "t1", None, 0, None, None, None, None, 100)
            .unwrap();
        let t2 = crate::tasks::create(&mut c, "boss", "t2", None, 0, None, None, None, None, 200)
            .unwrap();
        let t3_open =
            crate::tasks::create(&mut c, "boss", "t3", None, 0, None, None, None, None, 300)
                .unwrap();
        ready_claim(&mut c, "Alice", Some(t1), &[], 1000, 400).unwrap();
        crate::tasks::apply_event(
            &mut c,
            "Alice",
            t1,
            &crate::lifecycle::Event::SignaledDone { pr: "1".into() },
            400,
        )
        .unwrap();
        ready_claim(&mut c, "Bob", Some(t2), &[], 1000, 500).unwrap();
        crate::tasks::apply_event(
            &mut c,
            "Bob",
            t2,
            &crate::lifecycle::Event::SignaledDone { pr: "2".into() },
            500,
        )
        .unwrap();

        let now = 600;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.throughput.done_awaiting_review, 2);
        assert_eq!(s.throughput.oldest_done_awaiting_review_secs, Some(200));
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
    fn throughput_counts_in_review_tasks() {
        let (_d, c) = open_tmp();
        insert_task_raw(&c, "work-in-review", "in-review", None, 300);
        insert_task_raw(&c, "done-task", "done", None, 250);

        let now = 400;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.throughput.done_awaiting_review, 1);
        assert_eq!(s.throughput.oldest_done_awaiting_review_secs, Some(100));
        assert_eq!(s.throughput.done_stuck_count, 0);
    }

    #[test]
    fn throughput_zero_when_no_tasks_in_review() {
        let (_d, c) = open_tmp();
        insert_task_raw(&c, "done-task", "done", None, 300);

        let now = 400;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.throughput.done_awaiting_review, 0);
        assert_eq!(s.throughput.oldest_done_awaiting_review_secs, None);
        assert_eq!(s.throughput.done_stuck_count, 0);
    }

    #[test]
    fn throughput_in_review_stuck_flagged_after_threshold() {
        let (_d, mut c) = open_tmp();
        let t = crate::tasks::create(
            &mut c, "boss", "stuck", None, 0, None, None, None, None, 100,
        )
        .unwrap();
        ready_claim(&mut c, "Alice", Some(t), &[], 10000, 100).unwrap();
        crate::tasks::apply_event(
            &mut c,
            "Alice",
            t,
            &crate::lifecycle::Event::SignaledDone { pr: "1".into() },
            100,
        )
        .unwrap();
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
            None,
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
            None,
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
        assert_eq!(b2.tier_eff, "opus46");
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
            None,
            100,
        )
        .unwrap();
        ready_claim(&mut c, "Alice", Some(t1), &[], 10000, 100).unwrap();
        crate::tasks::close_after_merge(&mut c, t1, "merged", 100).unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert!(s.blocked.is_empty());
        // The dependent should now appear in the queue.
        assert!(!s.queue_by_tier.is_empty());
    }

    #[test]
    fn effort_only_labels_leave_queue_and_blocked_identity_pending() {
        let (_d, mut c) = open_tmp();
        let queued = crate::tasks::create(
            &mut c,
            "boss",
            "effort-only queued",
            None,
            0,
            Some(r#"["effort:high"]"#),
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let blocked = crate::tasks::create(
            &mut c,
            "boss",
            "effort-only blocked",
            None,
            0,
            Some(r#"["effort:high"]"#),
            None,
            Some(&format!("[{queued}]")),
            None,
            100,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let q = s.queue_tasks.iter().find(|task| task.id == queued).unwrap();
        let b = s.blocked.iter().find(|task| task.id == blocked).unwrap();
        for (provider, model, effort) in [
            (&q.provider, &q.model, &q.effort),
            (&b.provider, &b.model, &b.effort),
        ] {
            assert_eq!(provider.as_deref(), Some("pending"));
            assert_eq!(model.as_deref(), Some("pending"));
            assert_eq!(effort.as_deref(), Some("pending"));
        }
    }

    #[test]
    fn empty_labels_do_not_mask_later_queue_and_blocked_identity() {
        let (_d, mut c) = open_tmp();
        let labels = r#"["tier:","tier:opus-47","effort:","effort:high"]"#;
        let queued = crate::tasks::create(
            &mut c,
            "boss",
            "empty labels before valid queued identity",
            None,
            0,
            Some(labels),
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let blocked = crate::tasks::create(
            &mut c,
            "boss",
            "empty labels before valid blocked identity",
            None,
            0,
            Some(labels),
            None,
            Some(&format!("[{queued}]")),
            None,
            100,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let q = s.queue_tasks.iter().find(|task| task.id == queued).unwrap();
        let b = s.blocked.iter().find(|task| task.id == blocked).unwrap();
        for (provider, model, effort) in [
            (&q.provider, &q.model, &q.effort),
            (&b.provider, &b.model, &b.effort),
        ] {
            assert_eq!(provider.as_deref(), Some("claude"));
            assert_eq!(model.as_deref(), Some("claude-opus-4-7"));
            assert_eq!(effort.as_deref(), Some("high"));
        }
    }

    #[test]
    fn blocked_section_surfaces_deadlocked_cancelled_deps() {
        let (_d, mut c) = open_tmp();
        let dep = crate::tasks::create(
            &mut c,
            "boss",
            "will-cancel",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let child = crate::tasks::create(
            &mut c,
            "boss",
            "stuck-child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            100,
        )
        .unwrap();
        // Cancel the dep
        ready_claim(&mut c, "W", Some(dep), &[], 10000, 100).unwrap();
        crate::tasks::update(
            &mut c,
            "W",
            dep,
            &crate::tasks::TaskUpdate {
                status: Some("cancelled"),
                ..Default::default()
            },
            101,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.blocked.len(), 1);
        let b = &s.blocked[0];
        assert_eq!(b.id, child);
        assert_eq!(b.deadlocked_on, vec![dep]);
    }

    /// Task #473: a parked-unsatisfiable task (status='failed',
    /// daemon_parked_unsatisfiable=1) surfaces in the BLOCKED section with
    /// the cancelled dep id in `deadlocked_on`. Without this, the operator
    /// has to spelunk the DB to find the disposition queue after the sweep
    /// moves the task out of `status='open'`.
    #[test]
    fn blocked_section_surfaces_parked_unsatisfiable_tasks() {
        let (_d, mut c) = open_tmp();
        let dep = crate::tasks::create(&mut c, "boss", "dep", None, 0, None, None, None, None, 100)
            .unwrap();
        let parked = crate::tasks::create(
            &mut c,
            "boss",
            "parked-by-cancelled-dep",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='cancelled' WHERE id=?1",
            rusqlite::params![dep],
        )
        .unwrap();
        // Simulate the sweep parking the dependent with unsatisfiable=true.
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_unsatisfiable', json('true'),
                 'daemon_resume_status', 'open',
                 'daemon_parked_reason', 'dependency #' || ?2 || ' is cancelled — unsatisfiable'
             ) WHERE id=?1",
            rusqlite::params![parked, dep],
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let b = s
            .blocked
            .iter()
            .find(|task| task.id == parked)
            .expect("parked-unsatisfiable task must render in the BLOCKED section");
        assert_eq!(b.deadlocked_on, vec![dep]);
    }

    /// Task #473 R6 blocker 3: a classifier-policy park keeps its
    /// authoritative "classifier declined" reason (retry is a
    /// reclassification, not a dep restore), so the durable
    /// `daemon_parked_unsatisfiable` marker is never set on it. BLOCKED
    /// must still surface it when a dep is cancelled, otherwise the
    /// operator disposition signal disappears after the generic alert
    /// expires. Live dep-graph inference in `blocked_tasks` covers it.
    #[test]
    fn blocked_section_surfaces_policy_park_with_cancelled_dep() {
        let (_d, mut c) = open_tmp();
        let dep = crate::tasks::create(&mut c, "boss", "dep", None, 0, None, None, None, None, 100)
            .unwrap();
        let policy_parked = crate::tasks::create(
            &mut c,
            "boss",
            "policy-parked with cancelled dep",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='cancelled' WHERE id=?1",
            rusqlite::params![dep],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed', refs=json_object(
                 'daemon_parked', json('true'),
                 'daemon_parked_reason', 'classifier declined',
                 'daemon_resume_status', 'open',
                 'classifier_policy_parked', json('true')
             ) WHERE id=?1",
            rusqlite::params![policy_parked],
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let b = s
            .blocked
            .iter()
            .find(|task| task.id == policy_parked)
            .expect("policy park with cancelled dep must render in BLOCKED");
        assert_eq!(b.deadlocked_on, vec![dep]);
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
    fn daemon_agents_view_reads_journal_with_agent_state() {
        let (_d, mut c) = open_tmp();
        crate::journal::upsert(
            &mut c,
            &crate::journal::JournalEntry {
                agent: "W1".into(),
                role: "worker".into(),
                task_id: Some(10),
                session_id: "sess-1".into(),
                worktree: Some("/tmp/wt/w1".into()),
                branch: Some("feat/thing".into()),
                phase: "working".into(),
                cost_tokens: 500,
                agent_state: Some("blocked".into()),
                cost_usd: 0.05,
                log_dir: Some("/tmp/logs/W1-123".into()),
                pid: None,
                pr: None,
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();
        crate::journal::upsert(
            &mut c,
            &crate::journal::JournalEntry {
                agent: "R1".into(),
                role: "reviewer".into(),
                task_id: Some(10),
                session_id: "sess-2".into(),
                worktree: Some("/tmp/wt/r1".into()),
                branch: Some("review/thing".into()),
                phase: "reviewing".into(),
                cost_tokens: 200,
                agent_state: None,
                cost_usd: 0.01,
                log_dir: None,
                pid: None,
                pr: None,
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();

        let s = stats(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.daemon_agents.len(), 2);
        let w = s.daemon_agents.iter().find(|d| d.role == "worker").unwrap();
        assert_eq!(w.agent, "W1");
        assert_eq!(w.task_id, Some(10));
        assert_eq!(w.agent_state.as_deref(), Some("blocked"));
        assert!((w.cost_usd - 0.05).abs() < f64::EPSILON);
        assert_eq!(w.log_dir.as_deref(), Some("/tmp/logs/W1-123"));
        let r = s
            .daemon_agents
            .iter()
            .find(|d| d.role == "reviewer")
            .unwrap();
        assert_eq!(r.agent, "R1");
        assert_eq!(r.agent_state, None);
        assert!((r.cost_usd - 0.01).abs() < f64::EPSILON);
        assert_eq!(r.log_dir, None);
        // Non-existent log_dir → last_activity_age_secs is None
        assert_eq!(w.last_activity_age_secs, None);
        assert_eq!(r.last_activity_age_secs, None);
    }

    #[test]
    fn daemon_agents_view_reports_liveness_from_stream_mtime() {
        let (d, mut c) = open_tmp();
        let log_dir = d.path().join("Agent-1000");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::write(log_dir.join("stream.jsonl"), "{}").unwrap();

        crate::journal::upsert(
            &mut c,
            &crate::journal::JournalEntry {
                agent: "Agent".into(),
                role: "worker".into(),
                task_id: Some(1),
                session_id: "s".into(),
                worktree: None,
                branch: None,
                phase: "working".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: Some(log_dir.to_str().unwrap().to_string()),
                pid: None,
                pr: None,
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();

        let s = stats(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.daemon_agents.len(), 1);
        let age = s.daemon_agents[0].last_activity_age_secs.unwrap();
        assert!((0..5).contains(&age), "expected recent mtime, got {age}s");
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

    #[test]
    fn daemon_live_stats_serde_roundtrip() {
        let stats = DaemonLiveStats {
            tools: 27,
            now: "Bash: cargo test".into(),
            evm: 14.5,
            up_secs: 240,
            mid_turn_tok: 18000,
            spawn_epoch: 1720300000,
            error_count: 0,
            error_text: None,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let back: DaemonLiveStats = serde_json::from_str(&json).unwrap();
        assert_eq!(back.tools, 27);
        assert_eq!(back.now, "Bash: cargo test");
        assert!((back.evm - 14.5).abs() < f64::EPSILON);
        assert_eq!(back.up_secs, 240);
        assert_eq!(back.mid_turn_tok, 18000);
        assert_eq!(back.spawn_epoch, 1720300000);
    }

    #[test]
    fn read_live_stats_from_sidecar_file() {
        let dir = tempfile::tempdir().unwrap();
        let stats = DaemonLiveStats {
            tools: 5,
            now: "Read: foo.rs".into(),
            evm: 3.0,
            up_secs: 60,
            mid_turn_tok: 500,
            spawn_epoch: 1720300000,
            error_count: 0,
            error_text: None,
        };
        let json = serde_json::to_string(&stats).unwrap();
        std::fs::write(dir.path().join("_daemon_live.json"), json).unwrap();
        let read = read_live_stats(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(read.tools, 5);
        assert_eq!(read.now, "Read: foo.rs");
    }

    #[test]
    fn read_live_stats_missing_file_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_live_stats(dir.path().to_str().unwrap()).is_none());
    }

    #[test]
    fn compute_health_new_thresholds() {
        let worker = |age: Option<i64>| DaemonAgentView {
            agent: "W".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(1),
            phase: "working".into(),
            cost_tokens: 0,
            agent_state: None,
            cost_usd: 0.0,
            log_dir: None,
            last_activity_age_secs: age,
            task_title: None,
            tier_eff: None,
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: None,
            live_error_count: 0,
            live_error_text: None,
        };
        assert_eq!(
            compute_health(&[worker(Some(50))], false, false),
            HealthVerdict::OnTrack,
            "50s should be on-track (threshold is 60s)"
        );
        assert_eq!(
            compute_health(&[worker(Some(90))], false, false),
            HealthVerdict::Attention,
            "90s should be attention (was stalled at old 120s threshold)"
        );
        assert_eq!(
            compute_health(&[worker(Some(200))], false, false),
            HealthVerdict::Stalled,
            "200s should be stalled"
        );
    }

    fn make_daemon_agent(role: &str, phase: &str, age: Option<i64>) -> DaemonAgentView {
        DaemonAgentView {
            agent: format!("{role}-1"),
            role: role.into(),
            sub_role: None,
            task_id: Some(1),
            phase: phase.into(),
            cost_tokens: 0,
            agent_state: None,
            cost_usd: 0.0,
            log_dir: None,
            last_activity_age_secs: age,
            task_title: None,
            tier_eff: None,
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: None,
            live_error_count: 0,
            live_error_text: None,
        }
    }

    #[test]
    fn awaiting_review_worker_not_stalled() {
        let w = make_daemon_agent("worker", "awaiting-review", Some(300));
        assert_eq!(
            compute_health(&[w], false, false),
            HealthVerdict::OnTrack,
            "awaiting-review worker with 300s silence must not trigger stalled"
        );
    }

    #[test]
    fn working_worker_stalled_after_180s() {
        let w = make_daemon_agent("worker", "working", Some(200));
        assert_eq!(
            compute_health(&[w], false, false),
            HealthVerdict::Stalled,
            "working worker with 200s silence must be stalled"
        );
    }

    #[test]
    fn reviewer_not_counted_for_stall() {
        let r = make_daemon_agent("reviewer", "reviewing", Some(300));
        assert_eq!(
            compute_health(&[r], false, false),
            HealthVerdict::OnTrack,
            "reviewer with 300s silence must not trigger stalled (not a worker)"
        );
    }

    #[test]
    fn mixed_fleet_isolation() {
        let awaiting = make_daemon_agent("worker", "awaiting-review", Some(300));
        let active = make_daemon_agent("worker", "working", Some(30));
        let reviewer = make_daemon_agent("reviewer", "reviewing", Some(400));
        assert_eq!(
            compute_health(&[awaiting, active, reviewer], false, false),
            HealthVerdict::OnTrack,
            "only the active worker matters; awaiting-review and reviewer are excluded"
        );
    }

    #[test]
    fn tier_eff_label_maps_known_efforts() {
        assert_eq!(
            tier_eff_label(Some(r#"["tier:opus-46","effort:high"]"#)),
            "opus46·hi"
        );
        assert_eq!(
            tier_eff_label(Some(r#"["tier:opus-46","effort:medium"]"#)),
            "opus46·md"
        );
    }

    #[test]
    fn tier_eff_label_does_not_pretty_print_low() {
        // effort:low should never reach the DB (validate_labels rejects it), but if a stray one
        // slips through, we must NOT hide it behind a "lo" pretty label — surface the raw value
        // so the operator sees the corruption.
        let out = tier_eff_label(Some(r#"["effort:low"]"#));
        assert_eq!(out, "low", "want raw passthrough, got {out:?}");
    }

    #[test]
    fn tier_eff_label_complexity_fallback() {
        assert_eq!(tier_eff_label(Some(r#"["complexity:2"]"#)), "c2");
        assert_eq!(tier_eff_label(Some(r#"["complexity:5"]"#)), "c5");
        // tier/effort present → complexity ignored
        assert_eq!(
            tier_eff_label(Some(r#"["tier:opus-46","effort:high","complexity:3"]"#)),
            "opus46·hi"
        );
        // no labels at all → dash
        assert_eq!(tier_eff_label(None), "—");
        assert_eq!(tier_eff_label(Some(r#"[]"#)), "—");
    }

    #[test]
    fn stalled_count_skips_awaiting_review() {
        let awaiting = make_daemon_agent("worker", "awaiting-review", Some(300));
        let stalled_worker = make_daemon_agent("worker", "working", Some(200));
        let healthy_worker = make_daemon_agent("worker", "working", Some(30));
        let reviewer = make_daemon_agent("reviewer", "reviewing", Some(500));
        let agents = [awaiting, stalled_worker, healthy_worker, reviewer];
        let count = agents
            .iter()
            .filter(|d| is_stall_eligible(d))
            .filter(|d| {
                matches!(d.last_activity_age_secs, Some(age) if age > 180)
                    || d.last_activity_age_secs.is_none()
            })
            .count() as i64;
        assert_eq!(count, 1, "only the stalled working worker counts");
    }

    #[test]
    fn pipeline_tasks_time_windows_done() {
        let (_d, mut c) = open_tmp();
        let now = 10_000_i64;

        // Create two tasks, mark both done at different times.
        let t_recent =
            crate::tasks::create(&mut c, "A", "recent", None, 0, None, None, None, None, 100)
                .unwrap();
        let t_old =
            crate::tasks::create(&mut c, "A", "old", None, 0, None, None, None, None, 100).unwrap();

        // Mark both done: recent within the hour, old outside it.
        c.execute(
            "UPDATE tasks SET status='done', updated_at=?1 WHERE id=?2",
            rusqlite::params![now - 1800, t_recent], // 30 min ago — inside window
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='done', updated_at=?1 WHERE id=?2",
            rusqlite::params![now - 7200, t_old], // 2 hours ago — outside window
        )
        .unwrap();

        let tasks = pipeline_tasks(&c, now).unwrap();
        assert_eq!(tasks.len(), 1, "only recently-done task should appear");
        assert_eq!(tasks[0].id, t_recent);
    }

    #[test]
    fn pipeline_tasks_always_surface_decomposition_sources() {
        let (_d, mut c) = open_tmp();
        let now = 10_000_i64;
        let planning = crate::tasks::create(
            &mut c, "A", "planning", None, 0, None, None, None, None, 100,
        )
        .unwrap();
        let decomposed = crate::tasks::create(
            &mut c,
            "A",
            "decomposed",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='planning', updated_at=1 WHERE id=?1",
            rusqlite::params![planning],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='decomposed', updated_at=1 WHERE id=?1",
            rusqlite::params![decomposed],
        )
        .unwrap();

        let tasks = pipeline_tasks(&c, now).unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].id, planning);
        assert_eq!(tasks[0].status, "planning");
        assert_eq!(tasks[1].id, decomposed);
        assert_eq!(tasks[1].status, "decomposed");
    }

    #[test]
    fn decomposition_status_is_bounded_and_excludes_attempt_transcripts() {
        let (_d, mut c) = open_tmp();
        let source =
            crate::tasks::create(&mut c, "A", "source", None, 0, None, None, None, None, 100)
                .unwrap();
        let child_a =
            crate::tasks::create(&mut c, "A", "child a", None, 0, None, None, None, None, 100)
                .unwrap();
        let child_b = crate::tasks::create(
            &mut c,
            "A",
            "child b",
            None,
            0,
            None,
            None,
            Some(&format!("[{child_a}]")),
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='decomposed' WHERE id=?1",
            params![source],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed' WHERE id=?1",
            params![child_b],
        )
        .unwrap();
        c.execute(
            "INSERT INTO task_decompositions
             (source_task_id,state,active,planned_source_revision,proposal_attempts,
              provider_failures,planner_provider,planner_model,accepted_plan_revision,
              hold_summary,created_at,updated_at)
             VALUES (?1,'blocked',1,1,2,1,'codex','gpt-5.6-sol',3,'child failed',100,100)",
            params![source],
        )
        .unwrap();
        let graph_id = c.last_insert_rowid();
        for (task_id, local_key) in [(child_a, "a"), (child_b, "b")] {
            c.execute(
                "INSERT INTO task_graph_members
                 (graph_id,task_id,local_key,plan_revision) VALUES (?1,?2,?3,3)",
                params![graph_id, task_id, local_key],
            )
            .unwrap();
        }
        for ordinal in 1..=8 {
            c.execute(
                "INSERT INTO decomposition_attempts
                 (graph_id,source_revision,kind,ordinal,reason_code,summary,created_at)
                 VALUES (?1,1,'proposal',?2,'invalid',?3,100)",
                params![graph_id, ordinal, format!("reason {ordinal}")],
            )
            .unwrap();
        }

        let graph = decomposition_status(&c, 100).unwrap().unwrap();
        assert_eq!(graph.source_task_id, source);
        assert_eq!(
            graph.dispatch_hold.as_deref(),
            Some("implementation dispatch held: graph state=blocked, active=1")
        );
        assert_eq!(graph.completed_children, 0);
        assert_eq!(graph.total_children, 2);
        assert_eq!(graph.failed_children, vec![child_b]);
        assert_eq!(graph.members[1].prerequisites, vec![child_a]);
        assert_eq!(graph.reasons.len(), 6, "owner-facing reasons stay bounded");
        assert_eq!(graph.reasons[0], "child failed");
    }

    #[test]
    fn generated_child_failure_hold_renders_as_readable_status() {
        let summary = serde_json::json!({
            "affected_task": 42,
            "reason": "daemon push failed",
        })
        .to_string();
        assert_eq!(
            render_graph_hold_summary(Some("generated-child-failed"), &summary),
            "Generated child task #42 failed: daemon push failed"
        );
        assert_eq!(
            render_graph_hold_summary(Some("generated-child-failed"), "legacy child failure"),
            "legacy child failure"
        );
    }

    #[test]
    fn planner_stream_counters_do_not_count_assistant_messages_as_tools() {
        let dir = tempfile::tempdir().unwrap();
        // Sanitized shape: only assistant-message activity, no command or tool.
        std::fs::write(
            dir.path().join("stream.jsonl"),
            concat!(
                r#"{"event":"provider_lifecycle","provider":"codex","phase":"started"}"#,
                "\n",
                r#"{"event":"turn_lifecycle","turn":1,"phase":"started"}"#,
                "\n",
                r#"{"event":"assistant_message","details":{"summary":"structural","shape":"string","captured_bytes":8}}"#,
                "\n",
                r#"{"event":"assistant_message","details":{"summary":"structural","shape":"string","captured_bytes":8}}"#,
                "\n",
            ),
        )
        .unwrap();
        let counters = planner_stream_counters(dir.path().to_str().unwrap()).unwrap();
        assert!(
            counters.activity_count >= 3,
            "assistant messages must count as activity: {counters:?}"
        );
        assert_eq!(
            counters.tool_count, 0,
            "assistant-only stream must report zero tool actions"
        );
    }

    #[test]
    fn planner_stream_counters_count_command_and_tool_summaries_as_tools() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("stream.jsonl"),
            concat!(
                r#"{"event":"turn_lifecycle","turn":1,"phase":"started"}"#,
                "\n",
                r#"{"event":"assistant_message","details":{"summary":"structural","shape":"string","captured_bytes":4}}"#,
                "\n",
                r#"{"event":"command_summary","command":"shell","outcome":"succeeded","details":{"summary":"structural","shape":"object","captured_bytes":8}}"#,
                "\n",
                r#"{"event":"tool_summary","tool":"other","outcome":"started","details":{"summary":"structural","shape":"object","captured_bytes":8}}"#,
                "\n",
            ),
        )
        .unwrap();
        let counters = planner_stream_counters(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(
            counters.activity_count, 4,
            "each sanitized activity record counts once: {counters:?}"
        );
        assert_eq!(
            counters.tool_count, 2,
            "command_summary and tool_summary count as tools; assistant_message does not"
        );
    }

    #[test]
    fn live_planner_status_uses_assignment_and_bounded_stream_counters() {
        let (dir, mut c) = open_tmp();
        let source =
            crate::tasks::create(&mut c, "A", "source", None, 0, None, None, None, None, 100)
                .unwrap();
        c.execute(
            "INSERT INTO role_assignments(
                 responsibility_key,task_id,role,profile_id,provider,runner,model,effort,
                 pool_key,policy_generation,created_at
             ) VALUES ('planner:task:1:revision:1',?1,'planner','planner-profile','codex',
                 'codex','gpt-5.6-sol','high','pool','generation',100)",
            params![source],
        )
        .unwrap();
        let assignment_id = c.last_insert_rowid();
        c.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,
                 planner_provider,planner_model,planner_assignment_id,planner_session_id,
                 created_at,updated_at
             ) VALUES (?1,'planning',0,1,1,'codex','gpt-5.6-sol',?2,'planner-session',100,100)",
            params![source, assignment_id],
        )
        .unwrap();
        let graph_id = c.last_insert_rowid();
        let log_dir = dir.path().join("planner-live");
        std::fs::create_dir_all(&log_dir).unwrap();
        std::fs::write(
            log_dir.join("stream.jsonl"),
            concat!(
                r#"{"event":"turn_lifecycle","turn":1,"phase":"started"}"#,
                "\n",
                r#"{"event":"tool_summary","tool":"bash","outcome":"started","details":{"summary":"structural","shape":"object","captured_bytes":8}}"#,
                "\n",
                r#"{"type":"item.completed","item":{"type":"command_execution","command":"sk-status-must-not-leak"}}"#,
                "\n"
            ),
        )
        .unwrap();
        crate::journal::upsert(
            &mut c,
            &crate::journal::JournalEntry {
                agent: format!("decomposition-planner-{graph_id}"),
                role: "planner".into(),
                task_id: Some(source),
                session_id: "planner-session".into(),
                worktree: None,
                branch: None,
                phase: "planner".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: Some(log_dir.to_string_lossy().into_owned()),
                pid: None,
                pr: None,
                rework_count: 0,
                provider: Some("codex".into()),
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();

        let status = stats(&c, 100, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let graph = status.decomposition.as_ref().unwrap();
        assert_eq!(graph.planner_provider.as_deref(), Some("codex"));
        assert_eq!(graph.planner_model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(graph.planner_effort.as_deref(), Some("high"));
        assert_eq!(graph.planner_log_dir.as_deref(), log_dir.to_str());
        assert_eq!(graph.planner_activity_count, Some(3));
        assert_eq!(graph.planner_tool_count, Some(2));
        assert!(graph.planner_last_activity_age_secs.is_some());

        let planner = status
            .daemon_agents
            .iter()
            .find(|agent| agent.role == "planner")
            .unwrap();
        assert_eq!(planner.provider.as_deref(), Some("codex"));
        assert_eq!(planner.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(planner.effort.as_deref(), Some("high"));
        assert_eq!(planner.tool_count, 2);

        let json = serde_json::to_string(&status).unwrap();
        assert!(
            !json.contains("sk-status-must-not-leak"),
            "status must not expose provider payload text"
        );
    }

    #[test]
    fn decomposition_status_distinguishes_retryable_exhaustion_and_retry_count() {
        let (_d, mut c) = open_tmp();
        let source =
            crate::tasks::create(&mut c, "A", "source", None, 0, None, None, None, None, 100)
                .unwrap();
        c.execute(
            "UPDATE tasks SET status='failed' WHERE id=?1",
            params![source],
        )
        .unwrap();
        c.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,planned_source_revision,provider_failures,
                 operator_retry_count,hold_code,hold_summary,created_at,updated_at)
             VALUES (?1,'held',1,3,1,'provider-attempts-exhausted',
                     'bounded provider summary',100,100)",
            params![source],
        )
        .unwrap();
        let graph_id = c.last_insert_rowid();
        for ordinal in 1..=6 {
            c.execute(
                "INSERT INTO decomposition_attempts(
                     graph_id,source_revision,kind,ordinal,retry_generation,
                     reason_code,summary,created_at)
                 VALUES (?1,1,'provider',?2,?3,'provider-failure','bounded',100)",
                params![graph_id, ordinal, i64::from(ordinal > 3)],
            )
            .unwrap();
        }
        let graph = decomposition_status(&c, 100).unwrap().unwrap();
        assert_eq!(
            graph.hold_code.as_deref(),
            Some("provider-attempts-exhausted")
        );
        assert!(graph.retryable_planning_hold);
        assert_eq!(graph.operator_retry_count, 1);
        assert_eq!(graph.operator_retry_cap, 2);

        c.execute(
            "UPDATE tasks SET refs=json_object('daemon_publication',json_object(\
                 'pr',42,'branch','daemon/source','local_sha','abc',\
                 'expected_remote_sha','def','stage','push')) WHERE id=?1",
            [source],
        )
        .unwrap();
        assert!(
            !decomposition_status(&c, 100)
                .unwrap()
                .unwrap()
                .retryable_planning_hold
        );
        c.execute("UPDATE tasks SET refs=NULL WHERE id=?1", [source])
            .unwrap();

        c.execute(
            "UPDATE task_decompositions SET provider_failures=2 WHERE id=?1",
            [graph_id],
        )
        .unwrap();
        assert!(
            !decomposition_status(&c, 100)
                .unwrap()
                .unwrap()
                .retryable_planning_hold
        );

        c.execute(
            "UPDATE task_decompositions
             SET provider_failures=3,accepted_proposal_json='[]' WHERE id=?1",
            [graph_id],
        )
        .unwrap();
        assert!(
            !decomposition_status(&c, 100)
                .unwrap()
                .unwrap()
                .retryable_planning_hold
        );

        c.execute(
            "UPDATE task_decompositions SET accepted_proposal_json=NULL WHERE id=?1",
            [graph_id],
        )
        .unwrap();
        let child = crate::tasks::create(
            &mut c,
            "A",
            "materialized child",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        c.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision)
             VALUES (?1,?2,'child',1)",
            params![graph_id, child],
        )
        .unwrap();
        assert!(
            !decomposition_status(&c, 100)
                .unwrap()
                .unwrap()
                .retryable_planning_hold
        );

        c.execute(
            "DELETE FROM task_graph_members WHERE graph_id=?1",
            [graph_id],
        )
        .unwrap();
        c.execute(
            "UPDATE task_decompositions
             SET hold_code='scope-blocker',operator_retry_count=0 WHERE id=?1",
            [graph_id],
        )
        .unwrap();
        let semantic = decomposition_status(&c, 100).unwrap().unwrap();
        assert!(!semantic.retryable_planning_hold);
        assert_eq!(semantic.hold_code.as_deref(), Some("scope-blocker"));
    }

    #[test]
    fn reviewing_tasks_cover_post_submit_band_and_json() {
        let (_d, mut c) = open_tmp();
        let awaiting = crate::tasks::create(
            &mut c,
            "A",
            "await reviewer",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let live = crate::tasks::create(
            &mut c,
            "A",
            "live reviewer",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let merging = crate::tasks::create(
            &mut c,
            "A",
            "merge pending",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='in-review', refs='{\"pr\":101}' WHERE id=?1",
            params![awaiting],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='in-review', refs='{\"pr\":102}' WHERE id=?1",
            params![live],
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status='merging', refs='{\"pr\":103}' WHERE id=?1",
            params![merging],
        )
        .unwrap();
        crate::journal::upsert(
            &mut c,
            &crate::journal::JournalEntry {
                agent: "R1".into(),
                role: "reviewer".into(),
                task_id: Some(live),
                session_id: "reviewer-session".into(),
                worktree: None,
                branch: None,
                phase: "reviewing".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: Some(102),
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();

        let snapshot = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(snapshot.reviewing.len(), 3);
        assert_eq!(
            snapshot
                .reviewing
                .iter()
                .find(|task| task.id == awaiting)
                .unwrap()
                .state,
            "awaiting reviewer"
        );
        let live_row = snapshot
            .reviewing
            .iter()
            .find(|task| task.id == live)
            .unwrap();
        assert_eq!(live_row.reviewer.as_deref(), Some("R1"));
        assert_eq!(live_row.state, "reviewing");
        assert_eq!(
            snapshot
                .reviewing
                .iter()
                .find(|task| task.id == merging)
                .unwrap()
                .state,
            "merging"
        );

        let json = serde_json::to_value(&snapshot).unwrap();
        let rows = json["reviewing"].as_array().unwrap();
        assert_eq!(rows.len(), 3, "status --json serializes the reviewing rows");
        assert!(rows.iter().any(|row| {
            row["id"] == awaiting
                && row["state"] == "awaiting reviewer"
                && row["reviewer"].is_null()
        }));
    }

    #[test]
    fn reviewing_tasks_limit_newest_rows_when_review_only_tasks_accumulate() {
        let (_d, mut c) = open_tmp();
        let mut ids = Vec::new();
        for i in 0..=REVIEWING_TASK_LIMIT {
            let id = crate::tasks::create(
                &mut c,
                "A",
                &format!("review-only backlog {i}"),
                None,
                0,
                None,
                None,
                None,
                None,
                100 + i,
            )
            .unwrap();
            c.execute(
                "UPDATE tasks SET status='in-review', updated_at=?1 WHERE id=?2",
                params![100 + i, id],
            )
            .unwrap();
            ids.push(id);
        }

        let snapshot = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(snapshot.reviewing.len() as i64, REVIEWING_TASK_LIMIT);
        assert_eq!(snapshot.reviewing[0].id, *ids.last().unwrap());
        assert!(
            snapshot.reviewing.iter().all(|task| task.id != ids[0]),
            "the oldest overflow row must not be materialized"
        );
    }

    #[test]
    fn reviewing_tasks_query_uses_indexed_bounded_candidates() {
        let (_d, c) = open_tmp();
        let plan = c
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT id, title, status, refs FROM (
                     SELECT id, title, status, refs, updated_at FROM (
                         SELECT id, title, status, refs, updated_at FROM tasks
                         WHERE status='in-review'
                         ORDER BY updated_at DESC, id DESC LIMIT ?1
                     )
                     UNION ALL
                     SELECT id, title, status, refs, updated_at FROM (
                         SELECT id, title, status, refs, updated_at FROM tasks
                         WHERE status='merging'
                         ORDER BY updated_at DESC, id DESC LIMIT ?1
                     )
                 )
                 ORDER BY updated_at DESC, id DESC LIMIT ?1",
            )
            .unwrap()
            .query_map(params![REVIEWING_TASK_LIMIT], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(
            plan.contains("tasks_reviewing_newest"),
            "REVIEWING must use its bounded newest-first index: {plan}"
        );
        assert!(
            plan.matches("tasks_reviewing_newest").count() == 2,
            "REVIEWING must use the index for each bounded status candidate set: {plan}"
        );
    }

    #[test]
    fn alerts_older_than_twelve_hours_leave_status_and_health() {
        let (_d, mut c) = open_tmp();
        let now = 100_000_i64;
        let ttl = 7 * 24 * 60 * 60;

        crate::feed::post(
            &mut c,
            "daemon",
            "critical",
            None,
            "old alert",
            None,
            None,
            ttl,
            now - ALERT_WINDOW_SECS - 1,
        )
        .unwrap();
        crate::feed::post(
            &mut c,
            "daemon",
            "alert",
            None,
            "recent alert",
            None,
            None,
            ttl,
            now - ALERT_WINDOW_SECS + 1,
        )
        .unwrap();

        let with_recent = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(with_recent.alerts.len(), 1);
        assert_eq!(with_recent.alerts[0].body, "recent alert");
        assert_eq!(with_recent.health, HealthVerdict::Attention);

        let after_window = stats(&c, now + 2, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert!(after_window.alerts.is_empty());
        assert_eq!(after_window.health, HealthVerdict::OnTrack);
    }

    #[test]
    fn terminal_runnable_retry_marker_is_a_health_alert_until_reconciled() {
        let (_d, mut c) = open_tmp();
        let now = 100_000;
        let id = crate::tasks::create(
            &mut c,
            "boss",
            "legacy terminal retry",
            None,
            0,
            None,
            Some(
                r#"{"daemon_parked":true,"daemon_resume_status":"rework",
                    "daemon_rework_retry_requested":true}"#,
            ),
            None,
            None,
            now - 10,
        )
        .unwrap();
        c.execute("UPDATE tasks SET status='failed' WHERE id=?1", params![id])
            .unwrap();

        let before = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(before.health, HealthVerdict::Attention);
        assert!(before.alerts.iter().any(|alert| {
            alert.kind == "critical"
                && alert.body.contains(&format!("task #{id}"))
                && alert.body.contains("runnable daemon retry markers")
        }));

        crate::tasks::reconcile_terminal_retry_markers(&mut c, now + 1).unwrap();
        let after = stats(&c, now + 1, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert!(after.alerts.is_empty());
        assert_eq!(after.health, HealthVerdict::OnTrack);
    }

    #[test]
    fn terminal_retry_health_alert_is_not_starved_by_persisted_alert_cap() {
        let (_d, mut c) = open_tmp();
        let now = 100_000;
        for index in 0..ALERT_DISPLAY_LIMIT {
            crate::feed::post(
                &mut c,
                "daemon",
                "critical",
                None,
                &format!("persisted alert {index}"),
                None,
                None,
                ALERT_WINDOW_SECS,
                now - index,
            )
            .unwrap();
        }
        let id = crate::tasks::create(
            &mut c,
            "boss",
            "legacy terminal retry",
            None,
            0,
            None,
            Some(
                r#"{"daemon_parked":true,"daemon_resume_status":"rework",
                    "daemon_rework_retry_requested":true}"#,
            ),
            None,
            None,
            now - 20,
        )
        .unwrap();
        c.execute("UPDATE tasks SET status='failed' WHERE id=?1", params![id])
            .unwrap();

        let view = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let task_ref = format!("task#{id}");
        assert_eq!(view.alerts.len(), ALERT_DISPLAY_LIMIT as usize);
        assert!(view.alerts.iter().any(|alert| {
            alert.kind == "critical"
                && alert.refs.as_deref() == Some(task_ref.as_str())
                && alert.body.contains("runnable daemon retry markers")
        }));
        assert_eq!(
            view.alerts
                .iter()
                .filter(|alert| alert.body.starts_with("persisted alert"))
                .count(),
            ALERT_DISPLAY_LIMIT as usize - 1,
            "one persisted slot must yield to the terminal corruption signal"
        );
    }

    #[test]
    fn working_phase_worker_with_no_activity_counts_as_stalled() {
        let view = DaemonAgentView {
            agent: "W1".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(1),
            phase: "working".into(),
            cost_tokens: 500,
            agent_state: None,
            cost_usd: 0.05,
            log_dir: None,
            last_activity_age_secs: None,
            task_title: None,
            tier_eff: None,
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: None,
            live_error_count: 0,
            live_error_text: None,
        };
        assert!(is_stall_eligible(&view));
        let health = compute_health(&[view], false, false);
        assert_eq!(health, HealthVerdict::Stalled);
    }

    #[test]
    fn awaiting_review_worker_is_not_stall_eligible() {
        let view = DaemonAgentView {
            agent: "W1".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(1),
            phase: "awaiting-review".into(),
            cost_tokens: 500,
            agent_state: None,
            cost_usd: 0.05,
            log_dir: None,
            last_activity_age_secs: None,
            task_title: None,
            tier_eff: None,
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: None,
            live_error_count: 0,
            live_error_text: None,
        };
        assert!(!is_stall_eligible(&view));
    }

    // ── #177: merge blocker visibility ────────────────────────────────

    #[test]
    fn merge_blocker_conflict_surfaces_in_stats() {
        let (_d, mut c) = open_tmp();
        let t = crate::tasks::create(
            &mut c,
            "boss",
            "review PR #42",
            None,
            0,
            None,
            None,
            None,
            Some(42),
            100,
        )
        .unwrap();
        // Simulate merge-blocked: set body to MERGE_BLOCKED_BODY and status to in-review.
        c.execute(
            "UPDATE tasks SET body = ?1, status = 'in-review', updated_at = 500 WHERE id = ?2",
            rusqlite::params![crate::tasks::MERGE_BLOCKED_BODY, t],
        )
        .unwrap();

        let s = stats(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.merge_blockers.len(), 1);
        let b = &s.merge_blockers[0];
        assert_eq!(b.task_id, t);
        assert_eq!(b.blocker_kind, "conflict");
        assert_eq!(b.status, "in-review");
        assert_eq!(b.waiting_secs, 500);
        assert_eq!(b.pr, Some(42));
    }

    #[test]
    fn merge_blocker_ci_pending_surfaces_merging_tasks() {
        let (_d, mut c) = open_tmp();
        let t = crate::tasks::create(
            &mut c,
            "boss",
            "merge me",
            None,
            0,
            None,
            None,
            None,
            Some(99),
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET status = 'merging', updated_at = 800 WHERE id = ?1",
            rusqlite::params![t],
        )
        .unwrap();

        let s = stats(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.merge_blockers.len(), 1);
        let b = &s.merge_blockers[0];
        assert_eq!(b.task_id, t);
        assert_eq!(b.blocker_kind, "ci_pending");
        assert_eq!(b.status, "merging");
        assert_eq!(b.waiting_secs, 200);
        assert_eq!(b.pr, Some(99));
    }

    #[test]
    fn merge_blocker_not_counted_as_terminal_or_active_agent_work() {
        let (_d, mut c) = open_tmp();
        let t = crate::tasks::create(
            &mut c,
            "boss",
            "blocked",
            None,
            0,
            None,
            None,
            None,
            Some(10),
            100,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET body = ?1, status = 'in-review', updated_at = 200 WHERE id = ?2",
            rusqlite::params![crate::tasks::MERGE_BLOCKED_BODY, t],
        )
        .unwrap();

        let s = stats(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        // Must NOT be counted as done, failed, cancelled.
        let terminal_count: i64 = s
            .tasks
            .iter()
            .filter(|sc| sc.status == "done" || sc.status == "failed" || sc.status == "cancelled")
            .map(|sc| sc.count)
            .sum();
        assert_eq!(terminal_count, 0, "merge-blocked must not be terminal");
        // Must NOT appear as active daemon agent work.
        assert!(
            s.daemon_agents.is_empty(),
            "merge-blocked must not show as daemon agent"
        );
        // Must NOT count as stalled.
        assert_eq!(s.stalled_count, 0, "merge-blocked must not be stalled");
        // But IS in merge_blockers.
        assert_eq!(s.merge_blockers.len(), 1);
    }

    #[test]
    fn merge_blocker_done_task_excluded() {
        let (_d, mut c) = open_tmp();
        let t = crate::tasks::create(
            &mut c,
            "boss",
            "was blocked",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        // A done task that still has MERGE_BLOCKED_BODY (cleanup missed) should not surface.
        c.execute(
            "UPDATE tasks SET body = ?1, status = 'done', updated_at = 200 WHERE id = ?2",
            rusqlite::params![crate::tasks::MERGE_BLOCKED_BODY, t],
        )
        .unwrap();

        let s = stats(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert!(s.merge_blockers.is_empty());
    }

    #[test]
    fn merge_blocker_no_duplicate_when_merging_and_body_set() {
        let (_d, mut c) = open_tmp();
        let t = crate::tasks::create(
            &mut c,
            "boss",
            "both flags",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        // Edge case: task in merging AND has MERGE_BLOCKED_BODY. Should appear once.
        c.execute(
            "UPDATE tasks SET body = ?1, status = 'merging', updated_at = 200 WHERE id = ?2",
            rusqlite::params![crate::tasks::MERGE_BLOCKED_BODY, t],
        )
        .unwrap();

        let s = stats(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.merge_blockers.len(), 1, "should dedup");
        assert_eq!(s.merge_blockers[0].blocker_kind, "conflict");
    }

    #[test]
    fn existing_blocked_dependency_rendering_unaffected() {
        let (_d, mut c) = open_tmp();
        let dep = crate::tasks::create(&mut c, "boss", "dep", None, 0, None, None, None, None, 100)
            .unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "child",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            None,
            100,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.blocked.len(), 1, "dep-blocked task should still render");
        assert!(s.merge_blockers.is_empty(), "dep-blocked != merge-blocked");
    }

    // ── #177: alert dedup/backoff ─────────────────────────────────────

    #[test]
    fn alert_due_at_retry_fires_at_power_of_two() {
        assert!(alert_due_at_retry(0), "first alert must fire");
        assert!(alert_due_at_retry(1));
        assert!(alert_due_at_retry(2));
        assert!(!alert_due_at_retry(3));
        assert!(alert_due_at_retry(4));
        assert!(!alert_due_at_retry(5));
        assert!(!alert_due_at_retry(6));
        assert!(!alert_due_at_retry(7));
        assert!(alert_due_at_retry(8));
        assert!(alert_due_at_retry(16));
        assert!(!alert_due_at_retry(15));
    }

    #[test]
    fn alert_due_at_retry_negative_never_fires() {
        assert!(!alert_due_at_retry(-1));
        assert!(!alert_due_at_retry(-100));
    }

    // ── #182: live provider error fields ──────────────────────────────

    #[test]
    fn daemon_live_stats_round_trips_error_fields() {
        let stats = DaemonLiveStats {
            tools: 5,
            now: "Bash: ls".into(),
            evm: 3.0,
            up_secs: 120,
            mid_turn_tok: 0,
            spawn_epoch: 1000,
            error_count: 2,
            error_text: Some("session limit; resets 10:30am".into()),
        };
        let json = serde_json::to_string(&stats).unwrap();
        let parsed: DaemonLiveStats = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.error_count, 2);
        assert_eq!(
            parsed.error_text.as_deref(),
            Some("session limit; resets 10:30am")
        );
    }

    #[test]
    fn daemon_live_stats_defaults_error_fields_when_absent() {
        let json =
            r#"{"tools":1,"now":"","evm":0.0,"up_secs":10,"mid_turn_tok":0,"spawn_epoch":0}"#;
        let parsed: DaemonLiveStats = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.error_count, 0);
        assert!(parsed.error_text.is_none());
    }

    #[test]
    fn health_attention_when_live_error() {
        let agents = vec![DaemonAgentView {
            agent: "W1".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(1),
            phase: "working".into(),
            cost_tokens: 100,
            agent_state: None,
            cost_usd: 0.01,
            log_dir: None,
            last_activity_age_secs: Some(5),
            task_title: Some("task".into()),
            tier_eff: None,
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: Some(60),
            live_error_count: 1,
            live_error_text: Some("session limit".into()),
        }];
        let health = compute_health(&agents, false, false);
        assert_eq!(health, HealthVerdict::Attention);
    }

    #[test]
    fn health_on_track_when_no_live_error() {
        let agents = vec![DaemonAgentView {
            agent: "W1".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(1),
            phase: "working".into(),
            cost_tokens: 100,
            agent_state: None,
            cost_usd: 0.01,
            log_dir: None,
            last_activity_age_secs: Some(5),
            task_title: Some("task".into()),
            tier_eff: None,
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: Some(60),
            live_error_count: 0,
            live_error_text: None,
        }];
        let health = compute_health(&agents, false, false);
        assert_eq!(health, HealthVerdict::OnTrack);
    }

    // ── task-progress projection ──────────────────────────────────────

    #[test]
    fn progress_direct_r1_without_live_agent_keeps_history_and_waits_for_assignment() {
        let (_d, mut c) = open_tmp();
        let task_id = progress_task(&mut c, "direct");
        set_status(&c, task_id, "in-review");
        progress_run(&c, task_id, "worker", None, Some("turn-completed"));

        let progress = projection(&c, task_id);
        assert_eq!(progress.journey, TaskJourney::DirectImplementation);
        assert_eq!(progress.stage.label, "First review");
        assert_eq!(progress.stage.role.as_deref(), Some("R1"));
        assert_eq!(
            progress.condition.as_deref(),
            Some("Awaiting R1 assignment")
        );
        assert_eq!(
            progress.next_action.as_ref().unwrap().label,
            "Possible next"
        );
        assert_eq!(progress.history[0].stage, "Queued");
        assert_eq!(progress.history[1].stage, "Implementation");
    }

    #[test]
    fn progress_r2_uses_final_review_and_durable_r1_approval() {
        let (_d, mut c) = open_tmp();
        let task_id = progress_task(&mut c, "needs r2");
        set_status(&c, task_id, "in-review");
        progress_run(&c, task_id, "worker", None, Some("turn-completed"));
        progress_run(&c, task_id, "reviewer", None, Some("verdict:approved"));
        // The assigned but not live R2 run is durable evidence that this is the final gate.
        progress_run(&c, task_id, "reviewer", Some("r2"), None);

        let progress = projection(&c, task_id);
        assert_eq!(progress.stage.label, "Final review");
        assert_eq!(progress.stage.role.as_deref(), Some("R2"));
        assert_eq!(
            progress.condition.as_deref(),
            Some("Awaiting R2 assignment")
        );
        assert!(progress
            .history
            .iter()
            .any(|milestone| milestone.stage == "First review"
                && milestone.activity.as_deref() == Some("Approved")));
    }

    #[test]
    fn progress_rework_and_review_only_remediation_preserve_completed_review_rounds() {
        let (_d, mut c) = open_tmp();
        let direct = progress_task(&mut c, "direct rework");
        set_status(&c, direct, "rework");
        c.execute(
            "UPDATE tasks SET rework_round=2,rework_cap=3 WHERE id=?1",
            [direct],
        )
        .unwrap();
        progress_run(&c, direct, "worker", None, Some("turn-completed"));
        progress_run(&c, direct, "reviewer", None, Some("verdict:changes"));
        let direct_progress = projection(&c, direct);
        assert_eq!(direct_progress.stage.label, "Implementation");
        assert_eq!(direct_progress.attempts.rework_round, 2);
        assert_eq!(direct_progress.attempts.rework_cap, 3);
        assert!(direct_progress
            .history
            .iter()
            .any(|milestone| milestone.stage == "First review"));

        let review_only = progress_task(&mut c, "review remediation");
        c.execute(
            "UPDATE tasks SET review_only=1,status='rework',rework_round=1 WHERE id=?1",
            [review_only],
        )
        .unwrap();
        progress_run(&c, review_only, "reviewer", None, Some("verdict:changes"));
        let review_progress = projection(&c, review_only);
        assert_eq!(review_progress.journey, TaskJourney::ReviewOnly);
        assert_eq!(review_progress.stage.label, "Remediation");
        assert!(review_progress
            .milestones
            .iter()
            .any(|milestone| milestone.stage == "First review"
                && milestone.state == TaskProgressMilestoneState::Future));
    }

    #[test]
    fn progress_merge_wait_and_terminal_results_keep_completed_history() {
        let (_d, mut c) = open_tmp();
        let merge = progress_task(&mut c, "merge");
        set_status(&c, merge, "merging");
        progress_run(&c, merge, "worker", None, Some("turn-completed"));
        progress_run(&c, merge, "reviewer", None, Some("verdict:approved"));
        let merge_progress = projection(&c, merge);
        assert_eq!(merge_progress.stage.label, "Merge");
        assert_eq!(merge_progress.condition.as_deref(), Some("Waiting for CI"));
        assert_eq!(
            merge_progress.next_action.as_ref().unwrap().label,
            "Possible next"
        );

        let failed = progress_task(&mut c, "failed after review");
        set_status(&c, failed, "failed");
        c.execute(
            "UPDATE tasks SET refs=json_object('daemon_resume_status','rework') WHERE id=?1",
            [failed],
        )
        .unwrap();
        progress_run(&c, failed, "worker", None, Some("turn-completed"));
        progress_run(&c, failed, "reviewer", None, Some("verdict:changes"));
        let failed_progress = projection(&c, failed);
        assert_eq!(failed_progress.stage.label, "Failed");
        assert_eq!(failed_progress.condition.as_deref(), Some("Retry eligible"));
        assert_eq!(
            failed_progress.next_action.as_ref().unwrap().label,
            "Possible next"
        );
        assert!(failed_progress
            .history
            .iter()
            .any(|milestone| milestone.stage == "First review"));

        let cancelled = progress_task(&mut c, "cancelled after work");
        set_status(&c, cancelled, "cancelled");
        progress_run(&c, cancelled, "worker", None, Some("turn-completed"));
        let cancelled_progress = projection(&c, cancelled);
        assert_eq!(cancelled_progress.stage.label, "Cancelled");
        assert!(cancelled_progress
            .history
            .iter()
            .any(|milestone| milestone.stage == "Implementation"));
    }

    #[test]
    fn progress_planning_never_claims_arbiter_completion_from_task_status() {
        let (_d, mut c) = open_tmp();
        let source = progress_task(&mut c, "source");
        set_status(&c, source, "planning");
        let graph = progress_graph(&c, source, "planning");

        let planning = projection(&c, source);
        assert_eq!(planning.journey, TaskJourney::DecomposedSource);
        assert_eq!(planning.stage.label, "Planning");
        assert!(!planning
            .history
            .iter()
            .any(|milestone| milestone.stage == "Plan review"));

        c.execute(
            "UPDATE task_decompositions SET state='validating' WHERE id=?1",
            [graph],
        )
        .unwrap();
        let reviewing = projection(&c, source);
        assert_eq!(reviewing.stage.label, "Plan review");
        assert_eq!(reviewing.stage.role.as_deref(), Some("Arbiter"));
        assert_eq!(reviewing.stage.activity, "Awaiting Arbiter verdict");
        assert_eq!(
            reviewing.next_action.as_ref().unwrap().label,
            "Possible next"
        );
    }

    #[test]
    fn progress_uses_current_journal_evidence_for_activity_and_conditions() {
        let (_d, mut c) = open_tmp();
        let task_id = progress_task(&mut c, "live work");
        set_status(&c, task_id, "working");
        crate::journal::upsert(
            &mut c,
            &crate::journal::JournalEntry {
                agent: "worker".into(),
                role: "worker".into(),
                task_id: Some(task_id),
                session_id: "session".into(),
                worktree: None,
                branch: None,
                phase: "working".into(),
                cost_tokens: 0,
                agent_state: Some("needs-info".into()),
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: None,
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();
        let progress = projection(&c, task_id);
        assert_eq!(progress.stage.activity, "Implementation in progress");
        assert_eq!(
            progress.condition.as_deref(),
            Some("Waiting for operator input")
        );
    }

    #[test]
    fn progress_live_reviewer_has_no_awaiting_assignment_condition() {
        let (_d, mut c) = open_tmp();
        let task_id = progress_task(&mut c, "live review");
        set_status(&c, task_id, "in-review");
        crate::journal::upsert(
            &mut c,
            &crate::journal::JournalEntry {
                agent: "reviewer".into(),
                role: "reviewer".into(),
                task_id: Some(task_id),
                session_id: "session".into(),
                worktree: None,
                branch: None,
                phase: "reviewing".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: None,
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();

        let progress = projection(&c, task_id);
        assert_eq!(progress.stage.label, "First review");
        assert_eq!(progress.stage.activity, "First review in progress");
        assert_eq!(progress.condition, None);
    }

    #[test]
    fn progress_arbiter_outcomes_use_attempts_and_durable_advance() {
        let (_d, mut c) = open_tmp();
        let source = progress_task(&mut c, "arbiter source");
        set_status(&c, source, "planning");
        let graph = progress_graph(&c, source, "planning");
        for (kind, code) in [
            ("proposal", "arbiter-changes"),
            ("provider", "arbiter-provider"),
            ("blocker", "arbiter-reject-source"),
        ] {
            c.execute(
                "INSERT INTO decomposition_attempts(graph_id,source_revision,kind,ordinal,
                     reason_code,summary,created_at)
                 VALUES(?1,1,?2,(SELECT count(*)+1 FROM decomposition_attempts WHERE graph_id=?1 AND kind=?2),?3,'bounded',100)",
                params![graph, kind, code],
            )
            .unwrap();
        }
        let changed = projection(&c, source);
        let outcomes = changed
            .history
            .iter()
            .filter(|milestone| milestone.stage == "Plan review")
            .map(|milestone| milestone.activity.as_deref().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes,
            vec![
                "Changes requested",
                "Provider failure",
                "Rejected; operator decision required"
            ]
        );
        assert_eq!(changed.attempts.arbiter_rounds, 3);

        c.execute(
            "UPDATE task_decompositions SET state='preclassifying' WHERE id=?1",
            [graph],
        )
        .unwrap();
        let approved = projection(&c, source);
        assert_eq!(approved.stage.label, "Plan accepted");
        assert!(approved.history.iter().any(|milestone| {
            milestone.stage == "Plan review" && milestone.activity.as_deref() == Some("Approved")
        }));
    }

    #[test]
    fn progress_decomposed_children_surface_progress_and_failed_dependencies() {
        let (_d, mut c) = open_tmp();
        let source = progress_task(&mut c, "source");
        set_status(&c, source, "decomposed");
        let graph = progress_graph(&c, source, "active");
        let done = progress_task(&mut c, "done child");
        let failed = progress_task(&mut c, "failed child");
        let cancelled = progress_task(&mut c, "cancelled child");
        set_status(&c, done, "done");
        set_status(&c, failed, "failed");
        set_status(&c, cancelled, "cancelled");
        for (key, child) in [("a", done), ("b", failed), ("c", cancelled)] {
            c.execute(
                "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision)
                 VALUES(?1,?2,?3,1)",
                params![graph, child, key],
            )
            .unwrap();
        }

        let progress = projection(&c, source);
        assert_eq!(progress.stage.label, "Child execution");
        assert_eq!(progress.stage.activity, "1/3 child tasks complete");
        assert_eq!(progress.children.as_ref().unwrap().summary, "1/3");
        assert_eq!(
            progress.children.as_ref().unwrap().blocking_task_ids,
            vec![failed, cancelled]
        );
        let expected_condition = format!("Blocked by child tasks #{failed} and #{cancelled}");
        assert_eq!(
            progress.condition.as_deref(),
            Some(expected_condition.as_str())
        );
        assert_eq!(
            progress.next_action.as_ref().unwrap().label,
            "Possible next"
        );
    }

    #[test]
    fn progress_dependency_block_and_bounds_are_explicit() {
        let (_d, mut c) = open_tmp();
        let dependency = progress_task(&mut c, "dependency");
        let task = crate::tasks::create(
            &mut c,
            "owner",
            "blocked",
            None,
            0,
            None,
            None,
            Some(&format!("[{dependency}]")),
            None,
            100,
        )
        .unwrap();
        let blocked = projection(&c, task);
        let expected_condition = format!("Blocked by dependency tasks #{dependency}");
        assert_eq!(
            blocked.condition.as_deref(),
            Some(expected_condition.as_str())
        );

        set_status(&c, dependency, "cancelled");
        let deadlocked = projection(&c, task);
        let expected_deadlock = format!("Deadlocked by cancelled dependency tasks #{dependency}");
        assert_eq!(
            deadlocked.condition.as_deref(),
            Some(expected_deadlock.as_str())
        );

        for _ in 0..(TASK_PROGRESS_HISTORY_LIMIT + 4) {
            progress_run(&c, task, "worker", None, Some("turn-completed"));
        }
        let bounded = projection(&c, task);
        assert!(bounded.history.len() <= TASK_PROGRESS_HISTORY_LIMIT);
        assert!(bounded.milestones.len() <= TASK_PROGRESS_MILESTONE_LIMIT);
        assert!(bounded.history.iter().all(|milestone| milestone
            .activity
            .as_deref()
            .unwrap_or("")
            .chars()
            .count()
            <= TASK_PROGRESS_TEXT_LIMIT));
    }
}
