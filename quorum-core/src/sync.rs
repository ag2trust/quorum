//! `quorum sync` — the agent's "compass": one read-only call returning everything an agent
//! needs to orient, in strict priority order, as one JSON payload.
//!
//! See `docs/2026-06-26-sync-capstone-plan.md` for the design and issue #8 for the locked
//! contract. The cardinal rule: **`sync` orients, the agent acts.** No side effects except
//! the message-cursor advance (Phase 1b).
//!
//! Two public entry points:
//! - [`gather`] — **read-only** snapshot, no side effects. Useful for tests + ad-hoc
//!   introspection ("what does this agent's compass currently show?").
//! - [`tick`] — snapshot + **auto-ack the message cursor**. The CLI's `quorum sync` wires
//!   here; it's what an agent calls every loop iteration. Honors the single-write
//!   contract: the only mutation is advancing `cursors.last_seq` past what was returned.
//!
//! Phases 2/3/4 deferred per plan: `quorum sync --agent X` CLI flag wiring (Phase 2),
//! `stop` / `stop_cleared` (Phase 3, waits on #6 / PR #20), cheatsheet polish (Phase 4).

use crate::db::begin_immediate;
use crate::error::Result;
use crate::feed::DEFAULT_TOPIC;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

/// Default cap on returned events in `Snapshot::log`. Bounded so a noisy event stream
/// cannot make `sync` return a multi-megabyte payload (locked design: `log` is never the
/// global firehose).
pub const DEFAULT_LOG_LIMIT: i64 = 20;

/// Default cap on returned messages in `Snapshot::direct` / `Snapshot::critical`. The
/// `notifications.count` is unbounded — it's just `COUNT(*)`.
pub const DEFAULT_MSG_LIMIT: i64 = 20;

/// The task the agent currently holds (`status='claimed' AND assignee=agent`). Body is
/// intentionally omitted — the agent fetches it once at `task-claim` time and again via
/// `task-get` if needed. `sync` is the compass, not the cargo manifest.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct CurrentTaskView {
    pub id: i64,
    pub title: String,
    pub priority: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<String>,
    /// `task.updated_at` — the last time the row was modified (claim, body/refs edit, etc.).
    /// Named honestly: this is the row's current updated_at, not strictly "when claimed,"
    /// because a `task-update` after the claim transition shifts it. The agent can use it
    /// to detect stale own-tasks (e.g. "I claimed this 3 days ago and never touched it").
    pub last_updated_at: i64,
    /// `claim.expires_at` on `target='task#<id>'`. Agent renews before this.
    pub lease_expires_at: i64,
}

/// Lean view of the next claimable task — only emitted when `current_task` is `None`
/// (state-adaptive XOR, locked in the design session). Body omitted.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct NextTaskView {
    pub id: i64,
    pub title: String,
    pub priority: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<String>,
}

/// One direct or critical message rendered inline. `recipient` is implied by the bucket
/// (no need to echo it) — keeps the payload lean.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MsgView {
    pub seq: i64,
    pub ts: i64,
    pub author: String,
    pub kind: String,
    pub body: String,
}

/// Broadcast bucket: count of unread broadcasts plus any unread critical broadcasts
/// inlined verbatim. Token discipline — most ticks the agent only needs the count.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Notifications {
    pub count: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub critical: Vec<MsgView>,
}

/// A pinned standing notice — durable, cursor-independent, surfaced on every sync.
/// See `crate::pinned`. Distinct from `MsgView` because pins are not feed traffic.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct PinView {
    pub id: i64,
    pub ts: i64,
    pub author: String,
    pub body: String,
}

impl From<crate::pinned::Pin> for PinView {
    fn from(p: crate::pinned::Pin) -> Self {
        PinView {
            id: p.id,
            ts: p.ts,
            author: p.author,
            body: p.body,
        }
    }
}

/// One scoped event — `subject` matches a target the agent currently has skin in
/// (their current task and any claim they hold). Bounded by `DEFAULT_LOG_LIMIT`.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct LogEntry {
    pub seq: i64,
    pub ts: i64,
    pub kind: String,
    pub subject: String,
    pub body: String,
}

/// Retirement signal (issue #97) — present when the agent is in `retiring` or `retired`
/// state. Emits the load-score that triggered retirement so the agent (and operator) can
/// reason about why retirement fired, and the budgets so the threshold is self-documenting.
///
/// Semantics for the agent's work-loop:
/// - `status = "retiring"` → drain current/sticky work, then sign off. NO new claims of
///   non-sticky tasks. Reciprocity reviews are "new work" by this definition.
/// - `status = "retired"` → sign off immediately; `retired_at` is the canonical timestamp.
///
/// Companion `work-loop` skill changes (honoring this signal) ship in ag2trust separately —
/// see issue #97 trailing note.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RetireView {
    /// `"retiring"` or `"retired"` — mirrors `agents.retire_status`.
    pub status: String,
    /// Cumulative completed tasks for this agent.
    pub tasks_completed: i64,
    /// Cumulative active seconds across completed tasks.
    pub total_active_secs: i64,
    /// Budget for `total_active_secs` that triggered retirement (or would have, if the
    /// tasks-count budget tripped first). Self-documents the threshold every tick.
    pub budget_active_secs: i64,
    /// Budget for `tasks_completed` that triggered retirement (or would have, if the
    /// active-secs budget tripped first).
    pub budget_tasks: i64,
    /// Unix-ts when the agent was promoted to `retired`. `None` while still `retiring`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<i64>,
}

/// The HALT signal — present when the agent (or everybody, on a global stop) is stopped.
/// Field order matches the issue's lock: `{reason, scope, since, by}`. Global stops
/// take precedence over agent-targeted stops on the `is_stopped` query (broader signal —
/// the agent shouldn't have to merge two stop rows).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct StopView {
    pub reason: String,
    /// `global` or `agent:<id>`.
    pub scope: String,
    pub since: i64,
    pub by: String,
}

impl From<crate::control::Stop> for StopView {
    fn from(s: crate::control::Stop) -> Self {
        StopView {
            reason: s.reason,
            scope: s.scope,
            since: s.since,
            by: s.by,
        }
    }
}

/// How recent a `stop_cleared` event has to be for `sync` to surface it as the one-shot
/// affirmative resume signal. The locked design wants "next sync after resume shows
/// `stop_cleared: true`, subsequent syncs omit"; with no per-agent shown-set state
/// (CTO-greenlit Simple > schema-surface, hub 04:36), we approximate with a 2-minute
/// window after the event's `ts`. A halted agent polls cheaply, so the resume signal
/// is almost always seen within seconds; the 2 min absorbs realistic poll-cadence
/// variance without re-emitting the signal indefinitely (event TTL is 24h).
pub const STOP_CLEARED_WINDOW_SECS: i64 = 120;

/// The agent's one-call orientation payload. Empty sections are omitted on serialization —
/// a quiet tick (mid-task, no new messages, no stop) returns nearly nothing.
///
/// **Locked field order** (see issue #8): stop ▸ stop_cleared ▸ critical ▸ current_task ▸
/// next_task ▸ direct ▸ notifications ▸ log.
///
/// **STOP is absolute.** When `stop` is set, gather returns ONLY `stop` + `critical` (a
/// critical msg may explain the halt or be an even more urgent directive). All other
/// fields are omitted — the agent does nothing but cheap-poll for resume.
#[derive(Debug, Serialize, Default, PartialEq, Eq)]
pub struct Snapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<StopView>,
    /// One-shot affirmative resume signal — `true` for the first ~2 min after a stop
    /// clears, then drops out. See [`STOP_CLEARED_WINDOW_SECS`]. Always `Some(true)` or
    /// `None`; never `Some(false)`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_cleared: Option<bool>,
    /// Issue #97 retirement signal — present iff the agent is `retiring` or `retired`.
    /// Field ordering (after stop_cleared, before critical) puts it just below the HALT
    /// signals so a stop+retire collision keeps stop primacy without burying retirement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retire: Option<RetireView>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub critical: Vec<MsgView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_task: Option<CurrentTaskView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_task: Option<NextTaskView>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub direct: Vec<MsgView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notifications: Option<Notifications>,
    /// Durable standing notices (issue #78) — cursor-independent, no TTL, surfaced on
    /// every sync including the stop path. Removed only by explicit `unpin`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pinned: Vec<PinView>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub log: Vec<LogEntry>,
}

/// Assemble the orientation snapshot for `agent`. **Read-only** — no side effects, no
/// `BEGIN IMMEDIATE`, no presence bump. Use this for ad-hoc introspection ("what does
/// this agent's compass currently show?") or in tests. The CLI's `quorum sync` calls
/// [`tick`] instead — it auto-acks so the agent doesn't have to.
///
/// `match_labels` scopes `next_task` to tasks whose `labels` JSON-array contains every
/// entry; empty slice = no label filter (mirrors `tasks::claim`).
///
/// Running this mid-task is a non-event by design.
///
/// Convenience wrapper around [`gather_with_budget`] using the core retirement defaults
/// from `agents::DEFAULT_RETIRE_AFTER_*`. The CLI plumbs config-overridable budgets via
/// `gather_with_budget` directly.
pub fn gather(conn: &Connection, agent: &str, match_labels: &[&str], now: i64) -> Result<Snapshot> {
    gather_with_budget(
        conn,
        agent,
        match_labels,
        now,
        crate::agents::DEFAULT_RETIRE_AFTER_ACTIVE_SECS,
        crate::agents::DEFAULT_RETIRE_AFTER_TASKS,
    )
}

/// Same as [`gather`] but with explicit retire-budget parameters — used by the CLI to
/// honor config overrides on `retire_after_active_secs` / `retire_after_tasks` (issue #97).
pub fn gather_with_budget(
    conn: &Connection,
    agent: &str,
    match_labels: &[&str],
    now: i64,
    budget_active_secs: i64,
    budget_tasks: i64,
) -> Result<Snapshot> {
    // 0. STOP is first and absolute. If we're halted (globally or per-agent), return ONLY
    //    stop + critical and skip the rest — the agent does nothing but cheap-poll for
    //    resume. Critical msgs still come through (they may explain the halt or be a more
    //    urgent directive). This honors the locked "Nothing else matters" semantics while
    //    keeping the priority-hint surface live.
    if let Some(stop_row) = crate::control::is_stopped(conn, agent)? {
        let cursor = read_cursor(conn, agent, DEFAULT_TOPIC)?;
        let buckets = bucket_messages(conn, agent, cursor, now)?;
        // Pins are surfaced even during a stop — durable standing notices may explain
        // the halt or carry the "do X while halted" instruction. Same rationale as
        // critical: a halted agent benefits from the most context, not the least.
        let pinned = crate::pinned::list(conn)?
            .into_iter()
            .map(PinView::from)
            .collect();
        return Ok(Snapshot {
            stop: Some(stop_row.into()),
            critical: buckets.critical,
            pinned,
            ..Snapshot::default()
        });
    }

    // 0b. stop_cleared — one-shot affirmative resume signal. Only meaningful when we're
    //     NOT currently stopped (i.e., we just transitioned out). Derived from the event
    //     log: a recent `stop_cleared` event on `global` OR `agent:<me>` within the
    //     STOP_CLEARED_WINDOW_SECS window. Approximate "one-shot" without per-agent state
    //     per CTO 04:36 (Simple > schema-surface) — the agent's poll cadence almost always
    //     sees it within seconds, and the window absorbs realistic variance.
    let stop_cleared = recently_cleared(conn, agent, now)?.then_some(true);

    // 1a. Retirement signal (issue #97). Read the agent's persisted retire_status — it may
    //     already have been promoted to 'retiring'/'retired' on a prior tick. Also compute
    //     the load score once; both `retire` AND the next_task gate need it.
    let (tasks_completed, total_active_secs) = crate::stats::load_score_for(conn, agent)?;
    let (persisted_status, persisted_retired_at) = crate::agents::retire_state(conn, agent)?;
    // The *effective* status this tick: forward-only progression based on the persisted
    // state PLUS whether the budget is currently exceeded. We never demote (a returning
    // agent doesn't un-retire by losing tasks); only promote.
    let budget_exceeded =
        total_active_secs >= budget_active_secs.max(1) || tasks_completed >= budget_tasks.max(1);
    let effective_status: &str = if persisted_status == crate::agents::RETIRE_STATUS_RETIRED {
        crate::agents::RETIRE_STATUS_RETIRED
    } else if persisted_status == crate::agents::RETIRE_STATUS_RETIRING || budget_exceeded {
        crate::agents::RETIRE_STATUS_RETIRING
    } else {
        crate::agents::RETIRE_STATUS_ACTIVE
    };

    // 1b. State-adaptive XOR. If we hold a claimed task, surface it; do NOT also dangle
    //     next_task (locked in the design session — never show a second task to a busy agent).
    let current_task = current_task_view(conn, agent, now)?;
    let next_task = if current_task.is_some() {
        // Busy: never surface next_task, retiring or not.
        None
    } else if effective_status == crate::agents::RETIRE_STATUS_ACTIVE {
        next_task_view(conn, agent, match_labels, now)?
    } else {
        // Retiring/retired: no NEW work, but a sticky-pending rework belongs to this
        // agent and ONLY they can do it — surface it so the agent can drain before
        // signing off (issue #97 §2 "sticky carve-out"). Bypasses match_labels because
        // sticky-mine is by-name, not by-tier.
        sticky_mine_view(conn, agent, now)?
    };

    // Retirement signal is emitted whenever effective_status != active. Carries the budgets
    // so the value is self-documenting tick-to-tick.
    let retire = if effective_status == crate::agents::RETIRE_STATUS_ACTIVE {
        None
    } else {
        let no_remaining_work = current_task.is_none() && next_task.is_none();
        let will_be_retired =
            effective_status == crate::agents::RETIRE_STATUS_RETIRED || no_remaining_work;
        Some(RetireView {
            status: if will_be_retired {
                crate::agents::RETIRE_STATUS_RETIRED.to_string()
            } else {
                crate::agents::RETIRE_STATUS_RETIRING.to_string()
            },
            tasks_completed,
            total_active_secs,
            budget_active_secs,
            budget_tasks,
            // `retired_at` is whatever was persisted (None until tick writes it). The
            // first tick that transitions to 'retired' emits status='retired' but
            // retired_at=None; the next tick (after the write txn) has the timestamp.
            retired_at: persisted_retired_at,
        })
    };

    // 2. Message bucketing: direct full / critical full / broadcasts count + critical bodies.
    //    Reads from the cursor established by prior `read --ack-through` or `tick`.
    let cursor = read_cursor(conn, agent, DEFAULT_TOPIC)?;
    let buckets = bucket_messages(conn, agent, cursor, now)?;
    let notifications = if buckets.broadcast_count > 0 || !buckets.critical_broadcasts.is_empty() {
        Some(Notifications {
            count: buckets.broadcast_count,
            critical: buckets.critical_broadcasts,
        })
    } else {
        None
    };

    // 3. Scoped event log: events on the agent's task and any claim they hold. Bounded.
    //    Not the global firehose — token discipline.
    let current_task_id = current_task.as_ref().map(|c| c.id);
    let log = scoped_log(conn, agent, current_task_id, now)?;

    // Pinned notices — durable, cursor-independent, always surfaced. Read after the
    // event log because nothing else depends on them; ordering follows the locked
    // Snapshot field order (issue #8): stop ▸ stop_cleared ▸ critical ▸ current_task ▸
    // next_task ▸ direct ▸ notifications ▸ pinned ▸ log.
    let pinned = crate::pinned::list(conn)?
        .into_iter()
        .map(PinView::from)
        .collect();

    Ok(Snapshot {
        stop: None,
        stop_cleared,
        retire,
        critical: buckets.critical,
        current_task,
        next_task,
        direct: buckets.direct,
        notifications,
        pinned,
        log,
    })
}

/// Sticky-pending rework view (issue #97 sticky carve-out): an open task assigned to
/// `agent` with `sticky_until > now`. Returns `None` if no such task exists. Same
/// `NextTaskView` shape so the agent's loop doesn't need a new code path — it just claims
/// what `next_task` says.
fn sticky_mine_view(conn: &Connection, agent: &str, now: i64) -> Result<Option<NextTaskView>> {
    let row = conn
        .query_row(
            "SELECT id, title, priority, labels FROM tasks
             WHERE status = 'open'
               AND assignee = ?1
               AND sticky_until IS NOT NULL AND sticky_until > ?2
             ORDER BY priority DESC, id ASC
             LIMIT 1",
            params![agent, now],
            |r| {
                Ok(NextTaskView {
                    id: r.get(0)?,
                    title: r.get(1)?,
                    priority: r.get(2)?,
                    labels: r.get(3)?,
                })
            },
        )
        .optional()?;
    Ok(row)
}

/// `true` iff there's a `stop_cleared` event for the agent's scope (`global` or
/// `agent:<id>`) whose `ts` is within the last `STOP_CLEARED_WINDOW_SECS` seconds AND
/// the event itself isn't expired. Used by [`gather`] to surface the one-shot resume
/// signal without per-agent state. Caller must already have checked that the agent is
/// NOT currently stopped (otherwise we'd surface stop_cleared during an active stop —
/// nonsensical).
fn recently_cleared(conn: &Connection, agent: &str, now: i64) -> Result<bool> {
    let agent_scope = crate::control::agent_scope(agent);
    let threshold = now - STOP_CLEARED_WINDOW_SECS;
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM events
         WHERE kind = 'stop_cleared'
           AND expires_at > ?1
           AND ts > ?2
           AND (subject = ?3 OR subject = ?4)",
        params![now, threshold, crate::control::GLOBAL, agent_scope],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

/// Assemble the orientation snapshot AND advance the message cursor past everything that
/// was visible this tick — the one side effect the design allows. This is what the CLI's
/// `quorum sync` call wires to; an agent calls it every loop iteration and never has to
/// think about message acks.
///
/// **Cursor advance contract:** at the end of the call, `cursors.last_seq` is set to
/// `MAX(prior_last_seq, highest seq of any message read this tick)`. That includes
/// direct + critical msgs AND the broadcasts that fed into `notifications.count` — once
/// counted, they shouldn't be counted again. The advance is monotonic (a smaller cursor
/// never overwrites a larger one) and lives in the same transaction as the read so a
/// crash leaves either the old cursor (no msgs returned) or the new cursor (msgs
/// returned, agent saw them) — never a partial state.
///
/// **At-least-once vs at-most-once:** this is at-most-once per `tick()` call (the cursor
/// advances before the caller has a chance to "process" the snapshot). Agents that need
/// strict at-least-once should use [`gather`] plus explicit
/// `feed::read(..., ack_through=Some(seq))` after they've durably handled the payload.
/// For the agent-loop case the at-most-once weakening is intentional: the alternative
/// requires per-agent shown-but-not-acked state (a new schema column), which CTO has
/// ruled out as over-engineering — see plan doc + hub 04:36.
///
/// Same params as [`gather`]. Takes `&mut Connection` (the cursor advance needs a write
/// transaction).
pub fn tick(
    conn: &mut Connection,
    agent: &str,
    match_labels: &[&str],
    now: i64,
) -> Result<Snapshot> {
    tick_with_budget(
        conn,
        agent,
        match_labels,
        now,
        crate::agents::DEFAULT_RETIRE_AFTER_ACTIVE_SECS,
        crate::agents::DEFAULT_RETIRE_AFTER_TASKS,
    )
}

/// Same as [`tick`] but with explicit retire-budget parameters — the CLI calls this with
/// values from `~/.quorum/config.toml`. Issue #97.
pub fn tick_with_budget(
    conn: &mut Connection,
    agent: &str,
    match_labels: &[&str],
    now: i64,
    budget_active_secs: i64,
    budget_tasks: i64,
) -> Result<Snapshot> {
    let snap = gather_with_budget(
        conn,
        agent,
        match_labels,
        now,
        budget_active_secs,
        budget_tasks,
    )?;
    touch_and_advance_cursor(conn, agent, &snap, match_labels, now)?;
    Ok(snap)
}

/// Touch the agent (presence bump + auto-renew live leases per #55) AND advance the
/// message cursor past everything we just surfaced. Both writes happen inside the SAME
/// `begin_immediate` transaction — atomic by construction, and one connection write
/// rather than two.
///
/// **Why touch lives here instead of inside `gather`:** `gather` is the read-only entry
/// point. `tick` is where the agent's loop is — its purpose is the cursor advance + the
/// presence/renew side effects. Folding touch in the write txn means an agent that calls
/// only `quorum sync` (the design's whole point) still auto-renews its leases through
/// `agents::touch`'s renew clause (#55). This is the load-bearing piece that closes the
/// dogfood loop's "manual renew" hole.
///
/// **Cursor advance** is bounded by what `bucket_messages` actually read. In the current
/// design that's "everything > old_cursor that wasn't expired" — exactly what auto-ack
/// should cover. Even if a bucket's body was truncated by `DEFAULT_MSG_LIMIT`, we still
/// acked the seq beyond the truncation point (count is correct; just some bodies aren't
/// inlined). The cursor advance is a no-op when there's nothing new — but `touch` ALWAYS
/// runs (every `tick` is an `--agent` call by definition).
fn touch_and_advance_cursor(
    conn: &mut Connection,
    agent: &str,
    snap: &Snapshot,
    match_labels: &[&str],
    now: i64,
) -> Result<()> {
    // Highest seq across everything we surfaced (direct + critical + critical broadcasts).
    let cursor_now = read_cursor(conn, agent, DEFAULT_TOPIC)?;
    let mut max_seen: i64 = cursor_now;
    for m in snap.direct.iter().chain(snap.critical.iter()) {
        if m.seq > max_seen {
            max_seen = m.seq;
        }
    }
    if let Some(n) = &snap.notifications {
        for m in &n.critical {
            if m.seq > max_seen {
                max_seen = m.seq;
            }
        }
    }
    // Pick up any broadcast tail beyond what we inlined (notifications.count covers it).
    if let Some(broadcast_tail_max) = max_broadcast_seq_past_cursor(conn, cursor_now, now)? {
        if broadcast_tail_max > max_seen {
            max_seen = broadcast_tail_max;
        }
    }

    // ALWAYS write a txn — even on a quiet tick, `touch` runs (presence + auto-renew).
    // Three updates, one transaction:
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    // Persist the agent's declared tier from --match-label (#82).
    let tier = match_labels
        .iter()
        .find_map(|l| l.strip_prefix("tier:").map(|_| *l));
    crate::agents::set_tier(&tx, agent, tier)?;
    // Issue #97 retirement state transitions. The effective status this tick lives on the
    // snapshot's `RetireView` (computed by `gather_with_budget`). Each transition is
    // forward-only; the helpers are no-ops when the destination state is already set.
    if let Some(retire) = &snap.retire {
        if retire.status == crate::agents::RETIRE_STATUS_RETIRED {
            crate::agents::mark_retired(&tx, agent, now)?;
        } else {
            crate::agents::mark_retiring(&tx, agent)?;
        }
    }
    if max_seen > cursor_now {
        // Same shape as feed::read --ack-through: insert-or-update with MAX(...).
        tx.execute(
            "INSERT INTO cursors(agent_id, topic, last_seq) VALUES (?1, ?2, ?3)
             ON CONFLICT(agent_id, topic)
             DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)",
            params![agent, DEFAULT_TOPIC, max_seen],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Highest seq across un-expired broadcast messages with seq > cursor. None if no rows
/// match. Used by [`advance_cursor_past`] so the auto-ack covers broadcasts that only
/// contributed to `notifications.count`, not just inlined bodies.
fn max_broadcast_seq_past_cursor(conn: &Connection, cursor: i64, now: i64) -> Result<Option<i64>> {
    Ok(conn
        .query_row(
            "SELECT MAX(seq) FROM messages
             WHERE topic = ?1 AND seq > ?2 AND expires_at > ?3 AND recipient IS NULL",
            params![DEFAULT_TOPIC, cursor, now],
            |r| r.get::<_, Option<i64>>(0),
        )
        .optional()?
        .flatten())
}

// -- internal helpers --------------------------------------------------------------------

fn current_task_view(conn: &Connection, agent: &str, now: i64) -> Result<Option<CurrentTaskView>> {
    // Status + assignee together: a `claimed` row with `assignee=agent`. Joined with the
    // live lease (`target='task#<id>' AND active=1 AND expires_at > now`) for lease_expires.
    // LEFT JOIN so a momentarily-lease-less claimed row (e.g., reaper mid-flight) still
    // surfaces — better to show "your task" with lease_expires=updated_at than to omit it.
    let row = conn
        .query_row(
            "SELECT t.id, t.title, t.priority, t.labels, t.updated_at,
                    COALESCE(c.expires_at, t.updated_at) AS lease_expires
             FROM tasks t
             LEFT JOIN claims c
                ON c.target = 'task#' || t.id AND c.active = 1 AND c.expires_at > ?2
             WHERE t.status = 'claimed' AND t.assignee = ?1
             ORDER BY t.updated_at DESC
             LIMIT 1",
            params![agent, now],
            |r| {
                Ok(CurrentTaskView {
                    id: r.get(0)?,
                    title: r.get(1)?,
                    priority: r.get(2)?,
                    labels: r.get(3)?,
                    last_updated_at: r.get(4)?,
                    lease_expires_at: r.get(5)?,
                })
            },
        )
        .optional()?;
    Ok(row)
}

fn next_task_view(
    conn: &Connection,
    agent: &str,
    match_labels: &[&str],
    now: i64,
) -> Result<Option<NextTaskView>> {
    // Mirror `tasks::claim`'s selector EXACTLY: status='open' AND dep-ready AND
    // self-review-block AND sticky-eligible AND (untiered-review OR every match-label
    // present). The previous version of this function omitted the self-review and
    // sticky filters, causing an unclaimable head task (sticky-to-another or
    // self-review-blocked) to mask all claimable work beneath it — agents that hit
    // such a head correctly failed to claim it but `sync` did not fall through to
    // the next claimable task, leaving them idle with available work (issue #108).
    //
    // The fix is to apply the same three claim-eligibility clauses here that
    // `tasks::claim` uses, so a tier-matched, non-self-review, non-sticky-to-other
    // task surfaces instead of the unclaimable head. Tier gate stays exact-`==` per
    // owner — do NOT widen to `>=`.
    const DEP_READY_CLAUSE: &str = "(depends_on IS NULL OR NOT EXISTS (
        SELECT 1 FROM json_each(depends_on) je
        WHERE NOT EXISTS (
            SELECT 1 FROM tasks d WHERE d.id = je.value AND d.status = 'closed'
        )
    ))";
    // #10 / #108 self-review block: a review task (labels contain "kind:review")
    // whose `orig` equals the caller is invisible to that caller. Identical clause
    // to `tasks::claim` — sync MUST hide what claim would reject, otherwise the
    // agent sees a head task it cannot take and concludes "no work" (issue #108).
    // Bound as ?1 = agent.
    const SELF_REVIEW_BLOCK_CLAUSE: &str =
        "(labels IS NULL OR labels NOT LIKE '%\"kind:review\"%' OR orig IS NULL OR orig != ?1)";
    // #10 / #108 sticky-reopen gate: a task in its sticky window is claimable only
    // by its assignee (the original executor whose `changes`-verdict reopen set the
    // window). After expiry, anyone — eligibility narrows for `now < sticky_until`
    // only. Bound as ?2 = now. Identical clause to `tasks::claim`.
    const STICKY_CLAUSE: &str = "(sticky_until IS NULL OR sticky_until <= ?2 OR assignee = ?1)";
    // #105: only UNTIERED review tasks are tier-exempt. A review task that
    // inherited a tier label from the original task goes through normal tier
    // matching — so a weaker-tier agent doesn't review harder work. Untiered
    // reviews (legacy) remain visible to every tier-filtered sync (#73).
    const REVIEW_UNTIERED_EXEMPT: &str =
        "(labels LIKE '%\"kind:review\"%' AND labels NOT LIKE '%\"tier:%')";
    let mut sql = format!(
        "SELECT id, title, priority, labels FROM tasks
         WHERE status = 'open' AND {DEP_READY_CLAUSE}
           AND {SELF_REVIEW_BLOCK_CLAUSE} AND {STICKY_CLAUSE}"
    );
    if !match_labels.is_empty() {
        use std::fmt::Write as _;
        // (untiered-review OR (label1 AND label2 AND ...))
        let _ = write!(sql, " AND ({REVIEW_UNTIERED_EXEMPT} OR (");
        for i in 0..match_labels.len() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            // ?1 = agent, ?2 = now (used by SELF_REVIEW + STICKY clauses), so
            // label patterns start at ?3.
            let _ = write!(sql, "labels LIKE ?{}", i + 3);
        }
        sql.push_str("))");
    }
    sql.push_str(" ORDER BY priority DESC, id ASC LIMIT 1");

    let label_pats: Vec<String> = match_labels.iter().map(|l| format!("%\"{l}\"%")).collect();
    let mut bind: Vec<&dyn rusqlite::ToSql> = vec![&agent, &now];
    for p in &label_pats {
        bind.push(p);
    }
    let row = conn
        .query_row(&sql, &bind[..], |r| {
            Ok(NextTaskView {
                id: r.get(0)?,
                title: r.get(1)?,
                priority: r.get(2)?,
                labels: r.get(3)?,
            })
        })
        .optional()?;
    Ok(row)
}

fn read_cursor(conn: &Connection, agent: &str, topic: &str) -> Result<i64> {
    // Same cursor semantic as `feed::read` without `ack_through`. 0 = never acked anything.
    let c = conn
        .query_row(
            "SELECT last_seq FROM cursors WHERE agent_id = ?1 AND topic = ?2",
            params![agent, topic],
            |r| r.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0);
    Ok(c)
}

struct Buckets {
    direct: Vec<MsgView>,
    critical: Vec<MsgView>,
    broadcast_count: i64,
    critical_broadcasts: Vec<MsgView>,
}

fn bucket_messages(conn: &Connection, agent: &str, cursor: i64, now: i64) -> Result<Buckets> {
    // SQL-level bounding so a long-offline agent doesn't pull the full unread set into
    // memory before truncating. Three small targeted queries instead of one unbounded
    // fetch + in-memory partition:
    //   (1) direct-to-me (info + critical), LIMIT DEFAULT_MSG_LIMIT
    //   (2) critical broadcasts, LIMIT DEFAULT_MSG_LIMIT
    //   (3) total broadcast count (one COUNT, no rows)
    // critical bucket = direct-critical from (1) + all of (2).
    //
    // Each statement is index-friendly via messages_topic_seq. (3) is a COUNT over unread
    // broadcasts — one row, no body bytes — the only honest way to report "N unread
    // broadcasts" without inlining them (token discipline). #52 review: previously this
    // was one unbounded SELECT + in-memory truncate; an agent with 10k unread broadcasts
    // would fetch all 10k just to return ~20 inlined ones. Now bounded at the SQL layer.

    // (1) direct-to-me, full payload, bounded
    let mut stmt = conn.prepare(
        "SELECT seq, ts, author, kind, body FROM messages
         WHERE topic = ?1 AND seq > ?2 AND expires_at > ?3 AND recipient = ?4
         ORDER BY seq ASC LIMIT ?5",
    )?;
    let direct: Vec<MsgView> = stmt
        .query_map(
            params![DEFAULT_TOPIC, cursor, now, agent, DEFAULT_MSG_LIMIT],
            |r| {
                Ok(MsgView {
                    seq: r.get(0)?,
                    ts: r.get(1)?,
                    author: r.get(2)?,
                    kind: r.get(3)?,
                    body: r.get(4)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    // direct-critical (subset of `direct`) is surfaced in the critical bucket too — priority
    // hint so the agent doesn't have to scan both buckets to see a HALT-level msg.
    let critical_self: Vec<MsgView> = direct
        .iter()
        .filter(|m| m.kind == "critical")
        .cloned()
        .collect();

    // (2) critical broadcasts, full payload, bounded
    let mut stmt = conn.prepare(
        "SELECT seq, ts, author, kind, body FROM messages
         WHERE topic = ?1 AND seq > ?2 AND expires_at > ?3
           AND recipient IS NULL AND kind = 'critical'
         ORDER BY seq ASC LIMIT ?4",
    )?;
    let critical_broadcasts: Vec<MsgView> = stmt
        .query_map(
            params![DEFAULT_TOPIC, cursor, now, DEFAULT_MSG_LIMIT],
            |r| {
                Ok(MsgView {
                    seq: r.get(0)?,
                    ts: r.get(1)?,
                    author: r.get(2)?,
                    kind: r.get(3)?,
                    body: r.get(4)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    // critical bucket = direct-critical + critical-broadcast, bounded again so the merged
    // length never exceeds DEFAULT_MSG_LIMIT (each input is already capped at it).
    let mut critical: Vec<MsgView> = critical_self;
    critical.extend(critical_broadcasts.iter().cloned());
    if critical.len() > DEFAULT_MSG_LIMIT as usize {
        critical.truncate(DEFAULT_MSG_LIMIT as usize);
    }

    // (3) broadcast count — exact total, no row bodies fetched.
    let broadcast_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM messages
         WHERE topic = ?1 AND seq > ?2 AND expires_at > ?3 AND recipient IS NULL",
        params![DEFAULT_TOPIC, cursor, now],
        |r| r.get(0),
    )?;

    Ok(Buckets {
        direct,
        critical,
        broadcast_count,
        critical_broadcasts,
    })
}

fn scoped_log(
    conn: &Connection,
    agent: &str,
    current_task_id: Option<i64>,
    now: i64,
) -> Result<Vec<LogEntry>> {
    // Subject set = three sources, all about-the-agent:
    //   1. {task#<my-current-id>} — the task I'm working
    //   2. {target FROM claims WHERE holder=me AND active=1 AND expires_at>now} — non-task
    //      claims I currently hold (e.g. pr#2459)
    //   3. {task#<id> FROM tasks WHERE created_by=me AND status != 'closed'} — tasks I
    //      *created* (issue #62 / coordination-migration §6): a creator (typically the CTO)
    //      gets a live digest of every task they spun up without polling task-list. Bounded
    //      by status — once a task is `closed` it stops surfacing, otherwise an idle CTO's
    //      log fills with old terminal-state churn.
    //
    // If the union is empty, return [] (a fresh agent with no current/held/created tasks
    // has no scoped events — the global firehose is `quorum log`, not sync).
    let mut targets: Vec<String> = Vec::new();
    if let Some(id) = current_task_id {
        targets.push(format!("task#{id}"));
    }
    // Live claim targets I hold (includes the task lease implicitly, but the dedup below
    // keeps the in-clause minimal).
    let mut stmt = conn.prepare(
        "SELECT DISTINCT target FROM claims
         WHERE holder = ?1 AND active = 1 AND expires_at > ?2",
    )?;
    let claim_targets = stmt.query_map(params![agent, now], |r| r.get::<_, String>(0))?;
    for t in claim_targets {
        let t = t?;
        if !targets.contains(&t) {
            targets.push(t);
        }
    }
    // #62: tasks I created (non-terminal). `closed` is the only true terminal in the
    // 4-state lifecycle (`cancelled` reaches it via `task-cancel`); both stop surfacing so
    // an old CTO log doesn't drown the fresh signal.
    let mut stmt = conn.prepare(
        "SELECT id FROM tasks WHERE created_by = ?1 AND status NOT IN ('closed','cancelled')",
    )?;
    let created_ids = stmt.query_map(params![agent], |r| r.get::<_, i64>(0))?;
    for id in created_ids {
        let t = format!("task#{}", id?);
        if !targets.contains(&t) {
            targets.push(t);
        }
    }
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    // Build IN-clause with N placeholders. Targets come exclusively from db rows / the agent
    // string (no user-controlled raw SQL), but we still parameterize.
    let placeholders: Vec<String> = (0..targets.len()).map(|i| format!("?{}", i + 3)).collect();
    let sql = format!(
        "SELECT seq, ts, kind, subject, body FROM events
         WHERE expires_at > ?2 AND subject IN ({})
         ORDER BY seq DESC LIMIT ?1",
        placeholders.join(",")
    );
    let mut bind: Vec<&dyn rusqlite::ToSql> = vec![&DEFAULT_LOG_LIMIT, &now];
    for t in &targets {
        bind.push(t);
    }
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(&bind[..], |r| {
        Ok(LogEntry {
            seq: r.get(0)?,
            ts: r.get(1)?,
            kind: r.get(2)?,
            subject: r.get(3)?,
            body: r.get(4)?,
        })
    })?;
    let mut log: Vec<LogEntry> = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    // Return chronologically ascending — events are timeline data; reverse the DESC fetch
    // so the agent reads oldest-to-newest after the LIMIT cap took the most-recent N.
    log.reverse();
    Ok(log)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{claims, feed, tasks};

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn make_task(
        c: &mut Connection,
        title: &str,
        priority: i64,
        labels: Option<&str>,
        now: i64,
    ) -> i64 {
        tasks::create(c, "boss", title, None, priority, labels, None, None, now).unwrap()
    }

    // --- current_task XOR next_task ------------------------------------------------------

    #[test]
    fn snapshot_shows_current_task_when_agent_holds_one() {
        let (_d, mut c) = open_tmp();
        let id = make_task(&mut c, "do the thing", 5, Some("[\"rust\"]"), 100);
        tasks::claim(&mut c, "A", Some(id), &[], 1000, 100).unwrap();

        let snap = gather(&c, "A", &[], 200).unwrap();
        let cur = snap.current_task.as_ref().expect("current_task present");
        assert_eq!(cur.id, id);
        assert_eq!(cur.title, "do the thing");
        assert_eq!(cur.priority, 5);
        assert_eq!(cur.lease_expires_at, 100 + 1000);
        // XOR: must NOT also surface next_task.
        assert!(
            snap.next_task.is_none(),
            "next_task must be hidden when current_task is set"
        );
    }

    #[test]
    fn snapshot_shows_next_task_when_agent_idle() {
        let (_d, mut c) = open_tmp();
        let low = make_task(&mut c, "low", 1, None, 100);
        let high = make_task(&mut c, "high", 10, None, 100);
        // Make sure both exist; nothing claimed.
        assert!(tasks::get(&c, low).unwrap().is_some());
        assert!(tasks::get(&c, high).unwrap().is_some());

        let snap = gather(&c, "A", &[], 200).unwrap();
        assert!(snap.current_task.is_none());
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(nxt.id, high, "highest priority wins");
        assert_eq!(nxt.priority, 10);
    }

    #[test]
    fn snapshot_next_task_respects_match_label() {
        let (_d, mut c) = open_tmp();
        let _other = make_task(&mut c, "other", 10, Some("[\"python\"]"), 100);
        let mine = make_task(&mut c, "mine", 5, Some("[\"rust\",\"async\"]"), 100);
        // The higher-priority "other" doesn't match "rust" — must be skipped.
        let snap = gather(&c, "A", &["rust"], 200).unwrap();
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(nxt.id, mine);
        // Multi-label AND: requesting both should still match (labels has both).
        let snap2 = gather(&c, "A", &["rust", "async"], 200).unwrap();
        assert_eq!(snap2.next_task.as_ref().unwrap().id, mine);
        // Requesting an unmatched label hides it.
        let snap3 = gather(&c, "A", &["go"], 200).unwrap();
        assert!(snap3.next_task.is_none());
    }

    #[test]
    fn snapshot_untiered_review_is_tier_exempt_for_match_label_sync() {
        // #73: an UNTIERED `kind:review` task (only `["kind:review"]`, no tier)
        // remains tier-exempt so legacy reviews still surface.
        // #105 narrows: tiered reviews go through normal matching (see
        // `snapshot_tiered_review_obeys_tier_matching`).
        let (_d, mut c) = open_tmp();
        let _user = make_task(&mut c, "user-work", 50, Some("[\"tier:opus-47\"]"), 100);
        let review = make_task(
            &mut c,
            "review-pending",
            1000,
            Some("[\"kind:review\"]"),
            100,
        );

        let snap = gather(&c, "agent-X", &["tier:opus-47"], 200).unwrap();
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(
            nxt.id, review,
            "review task must surface to tier-filtered sync — priority 1000 should win over the user-work priority 50",
        );
    }

    #[test]
    fn snapshot_kind_review_still_visible_without_tier_filter() {
        // Sanity: with no --match-label, the existing behavior is unchanged —
        // the review task surfaces by priority just like before.
        let (_d, mut c) = open_tmp();
        let _user = make_task(&mut c, "user-work", 50, Some("[\"tier:opus-47\"]"), 100);
        let review = make_task(
            &mut c,
            "review-pending",
            1000,
            Some("[\"kind:review\"]"),
            100,
        );

        let snap = gather(&c, "agent-X", &[], 200).unwrap();
        assert_eq!(snap.next_task.as_ref().unwrap().id, review);
    }

    #[test]
    fn snapshot_kind_review_exemption_does_not_break_non_review_filtering() {
        // The tier-exempt OR must NOT widen the matcher for non-review tasks —
        // a user-work task without the tier label must still be hidden.
        let (_d, mut c) = open_tmp();
        let _foreign = make_task(&mut c, "foreign-tier", 100, Some("[\"tier:opus-46\"]"), 100);
        let snap = gather(&c, "agent-X", &["tier:opus-47"], 200).unwrap();
        assert!(
            snap.next_task.is_none(),
            "non-review task in a different tier must remain filtered out",
        );
    }

    #[test]
    fn snapshot_tiered_review_obeys_tier_matching() {
        // #105: a review task that carries a tier label is NOT tier-exempt —
        // a weaker-tier sync must not surface it.
        let (_d, mut c) = open_tmp();
        let review = make_task(
            &mut c,
            "review-47",
            1000,
            Some("[\"kind:review\",\"tier:opus-47\"]"),
            100,
        );
        // tier:opus-46 agent should NOT see this tiered review.
        let snap46 = gather(&c, "agent-46", &["tier:opus-46"], 200).unwrap();
        assert!(
            snap46.next_task.is_none(),
            "tier:opus-46 sync must NOT surface a tier:opus-47 review",
        );
        // tier:opus-47 agent SHOULD see it.
        let snap47 = gather(&c, "agent-47", &["tier:opus-47"], 200).unwrap();
        let nxt = snap47.next_task.as_ref().expect("next_task present");
        assert_eq!(nxt.id, review);
    }

    #[test]
    fn snapshot_next_task_respects_dep_gate() {
        let (_d, mut c) = open_tmp();
        let dep = make_task(&mut c, "dep", 1, None, 100);
        // dependent points at `dep` (not closed) — should be hidden until `dep` is closed.
        tasks::create(
            &mut c,
            "boss",
            "dependent",
            None,
            10,
            None,
            None,
            Some(&format!("[{dep}]")),
            100,
        )
        .unwrap();
        let snap = gather(&c, "A", &[], 200).unwrap();
        // `dep` (priority 1) is the next claimable; dependent (priority 10) is gated.
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(nxt.title, "dep");
    }

    // --- #108: next_task must skip unclaimable head ---------------------------------------
    //
    // Reproduces the live 2026-06-28 fleet incident: a sticky-to-another or
    // self-review-blocked head task at the top of the priority queue masked all
    // claimable work beneath it. The fix mirrors `tasks::claim`'s self-review +
    // sticky filters here so the surfaced next_task is one the requester can
    // actually claim.

    /// Set a sticky window on an open task with a specific assignee. Used to fabricate
    /// the "task is sticky to OfflineAgent" condition without driving a reviewer
    /// verdict (which would also reopen status and lift the lease — extra side effects
    /// beyond what this regression needs to test).
    fn set_sticky(c: &Connection, task_id: i64, assignee: &str, sticky_until: i64) {
        c.execute(
            "UPDATE tasks SET assignee=?1, sticky_until=?2 WHERE id=?3",
            rusqlite::params![assignee, sticky_until, task_id],
        )
        .unwrap();
    }

    #[test]
    fn next_task_skips_sticky_to_other_and_surfaces_claimable_below() {
        // Repro of #108: sticky head masks claimable work.
        let (_d, mut c) = open_tmp();
        let sticky_pri99 = make_task(&mut c, "sticky head", 99, None, 100);
        set_sticky(&c, sticky_pri99, "OfflineAgent", 9999);
        let claimable_pri50 = make_task(&mut c, "claimable below", 50, None, 100);
        // A non-sticky agent's sync must NOT be offered the sticky head (which they
        // can't claim) and MUST be offered the next claimable task.
        let snap = gather(&c, "BusyBee", &[], 200).unwrap();
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(
            nxt.id, claimable_pri50,
            "sticky head must be skipped for a non-sticky requester"
        );
    }

    #[test]
    fn next_task_still_surfaces_sticky_task_to_its_sticky_assignee() {
        // The sticky assignee is the one agent who CAN claim the sticky head —
        // they must still see it.
        let (_d, mut c) = open_tmp();
        let sticky_pri99 = make_task(&mut c, "sticky head", 99, None, 100);
        set_sticky(&c, sticky_pri99, "Griddle-7mR", 9999);
        let _below = make_task(&mut c, "claimable below", 50, None, 100);
        let snap = gather(&c, "Griddle-7mR", &[], 200).unwrap();
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(
            nxt.id, sticky_pri99,
            "sticky assignee must still see their sticky head"
        );
    }

    #[test]
    fn next_task_skips_self_review_and_surfaces_claimable_below() {
        // Repro the author-routing half of #108: a review task whose `orig` equals
        // the requester is unclaimable for that requester; sync must fall through.
        let (_d, mut c) = open_tmp();
        // Create the review task by hand — auto-spawn (via tasks::update --status
        // done) would also affect the original task's status and is more than we
        // need here.
        c.execute(
            "INSERT INTO tasks(title, status, priority, labels, created_by, created_at, updated_at, orig)
             VALUES ('review of mine', 'open', 1000, '[\"kind:review\"]', 'boss', 100, 100, 'Larkspur-q8X')",
            [],
        ).unwrap();
        let claimable = make_task(&mut c, "real work", 50, None, 100);
        // Larkspur-q8X is the PR author (`orig`) — must NOT see the self-review head.
        let snap = gather(&c, "Larkspur-q8X", &[], 200).unwrap();
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(
            nxt.id, claimable,
            "self-review head must be skipped for the orig author"
        );
    }

    #[test]
    fn next_task_still_surfaces_review_to_non_author() {
        // Inverse of the above: a different agent IS eligible to review.
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO tasks(title, status, priority, labels, created_by, created_at, updated_at, orig)
             VALUES ('review of someone else', 'open', 1000, '[\"kind:review\"]', 'boss', 100, 100, 'Velcro-m4D')",
            [],
        ).unwrap();
        let snap = gather(&c, "Larkspur-q8X", &[], 200).unwrap();
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(
            nxt.title, "review of someone else",
            "non-author must still see the review task"
        );
    }

    #[test]
    fn next_task_sticky_expired_is_visible_to_anyone() {
        // After the sticky window expires, the task returns to the regular queue and
        // any agent can pick it up. Boundary: `sticky_until <= now` ⇒ no longer sticky.
        let (_d, mut c) = open_tmp();
        let task = make_task(&mut c, "previously sticky", 99, None, 100);
        set_sticky(&c, task, "OfflineAgent", 150);
        // now=200 > sticky_until=150 → expired.
        let snap = gather(&c, "BusyBee", &[], 200).unwrap();
        let nxt = snap.next_task.as_ref().expect("next_task present");
        assert_eq!(nxt.id, task, "expired-sticky task is open to anyone");
    }

    #[test]
    fn next_task_self_review_filter_composes_with_tier_match_label() {
        // The self-review filter must compose with the match-label tier filter,
        // not bypass it: an agent whose tier doesn't match still doesn't see a
        // tier-mismatched review even if it isn't a self-review for them.
        let (_d, c) = open_tmp();
        // Review task carrying its own tier label (inherited per #105) — only
        // tier:opus-47 agents should see it.
        c.execute(
            "INSERT INTO tasks(title, status, priority, labels, created_by, created_at, updated_at, orig)
             VALUES ('opus-47 review', 'open', 1000, '[\"kind:review\",\"tier:opus-47\"]', 'boss', 100, 100, 'Velcro-m4D')",
            [],
        ).unwrap();
        // An opus-46 agent must NOT see this even though the orig != them.
        let snap_46 = gather(&c, "OpusFourSix", &["tier:opus-46"], 200).unwrap();
        assert!(
            snap_46.next_task.is_none(),
            "tier-mismatched review must be filtered even for non-author"
        );
        // An opus-47 non-author DOES see it.
        let snap_47 = gather(&c, "Larkspur-q8X", &["tier:opus-47"], 200).unwrap();
        assert_eq!(snap_47.next_task.as_ref().unwrap().title, "opus-47 review");
    }

    // --- message bucketing ---------------------------------------------------------------

    #[test]
    fn snapshot_buckets_direct_critical_and_broadcast_count() {
        let (_d, mut c) = open_tmp();
        // Direct to A (info + critical) and direct to B (must be hidden from A).
        feed::post(
            &mut c,
            "Z",
            "info",
            None,
            "hi-A",
            None,
            Some("A"),
            1000,
            100,
        )
        .unwrap();
        feed::post(
            &mut c,
            "Z",
            "critical",
            None,
            "halt-A",
            None,
            Some("A"),
            1000,
            100,
        )
        .unwrap();
        feed::post(
            &mut c,
            "Z",
            "info",
            None,
            "hi-B",
            None,
            Some("B"),
            1000,
            100,
        )
        .unwrap();
        // Broadcasts: 3 plain + 1 critical.
        for body in ["b1", "b2", "b3"] {
            feed::post(&mut c, "Z", "info", None, body, None, None, 1000, 100).unwrap();
        }
        feed::post(
            &mut c,
            "Z",
            "critical",
            None,
            "site-wide",
            None,
            None,
            1000,
            100,
        )
        .unwrap();

        let snap = gather(&c, "A", &[], 200).unwrap();
        // Direct: hi-A + halt-A; hi-B is hidden.
        assert_eq!(snap.direct.len(), 2);
        let bodies: Vec<&str> = snap.direct.iter().map(|m| m.body.as_str()).collect();
        assert!(bodies.contains(&"hi-A"));
        assert!(bodies.contains(&"halt-A"));
        assert!(!bodies.contains(&"hi-B"));
        // Critical bucket: the direct critical surfaces here too (priority hint).
        assert!(snap.critical.iter().any(|m| m.body == "halt-A"));
        // Notifications: 4 broadcasts (3 info + 1 critical), critical body inlined.
        let notif = snap.notifications.as_ref().expect("notifications present");
        assert_eq!(notif.count, 4);
        assert_eq!(notif.critical.len(), 1);
        assert_eq!(notif.critical[0].body, "site-wide");
    }

    #[test]
    fn snapshot_omits_notifications_when_inbox_empty() {
        let (_d, c) = open_tmp();
        let snap = gather(&c, "A", &[], 200).unwrap();
        assert!(snap.notifications.is_none());
        assert!(snap.direct.is_empty());
        assert!(snap.critical.is_empty());
    }

    #[test]
    fn snapshot_respects_message_cursor() {
        let (_d, mut c) = open_tmp();
        // Two direct msgs to A.
        let s1 = feed::post(&mut c, "Z", "info", None, "m1", None, Some("A"), 1000, 100)
            .unwrap()
            .seq;
        feed::post(&mut c, "Z", "info", None, "m2", None, Some("A"), 1000, 100).unwrap();
        // Agent A acks through s1 via the existing read path — sync must then only show m2.
        feed::read(&mut c, "A", None, Some(s1), feed::ReadFilter::All, 10, 200).unwrap();
        let snap = gather(&c, "A", &[], 200).unwrap();
        assert_eq!(snap.direct.len(), 1);
        assert_eq!(snap.direct[0].body, "m2");
    }

    // --- scoped log ----------------------------------------------------------------------

    #[test]
    fn snapshot_log_is_scoped_to_agent_targets() {
        let (_d, mut c) = open_tmp();
        // Two tasks: A's and B's. Only A's events should reach A's sync.log.
        let a_task = make_task(&mut c, "for-A", 1, None, 100);
        let b_task = make_task(&mut c, "for-B", 1, None, 100);
        tasks::claim(&mut c, "A", Some(a_task), &[], 1000, 100).unwrap();
        tasks::claim(&mut c, "B", Some(b_task), &[], 1000, 100).unwrap();
        // Pre-existing claim events from the claim() calls above also exist.
        // Verify A only sees A's events.
        let snap = gather(&c, "A", &[], 200).unwrap();
        assert!(!snap.log.is_empty(), "expected log entries on A's task");
        let a_subj = format!("task#{a_task}");
        let b_subj = format!("task#{b_task}");
        assert!(
            snap.log.iter().all(|e| e.subject == a_subj),
            "log leaked another agent's events: {:?}",
            snap.log
        );
        assert!(snap.log.iter().any(|e| e.subject == a_subj));
        // Sanity: B's snapshot only sees B's events.
        let snap_b = gather(&c, "B", &[], 200).unwrap();
        assert!(snap_b.log.iter().all(|e| e.subject == b_subj));
    }

    // --- #62 creator-routed events ------------------------------------------------------

    #[test]
    fn snapshot_log_includes_events_for_tasks_i_created() {
        // CTO-flavored use case: an agent that creates tasks (typically the CTO) sees
        // lifecycle events for every non-terminal task they spawned, without polling
        // task-list. Implemented by adding `created_by = me` task ids to the scoped-log
        // targets set in addition to current_task + held claims.
        let (_d, mut c) = open_tmp();
        // CTO creates 2 tasks; A claims one, B claims the other. Neither is `closed`.
        let t_a = tasks::create(&mut c, "CTO", "do-A", None, 0, None, None, None, 100).unwrap();
        let t_b = tasks::create(&mut c, "CTO", "do-B", None, 0, None, None, None, 100).unwrap();
        tasks::claim(&mut c, "A", Some(t_a), &[], 1000, 100).unwrap();
        tasks::claim(&mut c, "B", Some(t_b), &[], 1000, 100).unwrap();
        // CTO has no current task and no claims, but they CREATED both — sync.log must
        // include events for both.
        let snap = gather(&c, "CTO", &[], 200).unwrap();
        let a_subj = format!("task#{t_a}");
        let b_subj = format!("task#{t_b}");
        assert!(
            snap.log.iter().any(|e| e.subject == a_subj),
            "CTO must see events on task they created (t_a); got {:?}",
            snap.log
        );
        assert!(
            snap.log.iter().any(|e| e.subject == b_subj),
            "CTO must see events on task they created (t_b); got {:?}",
            snap.log
        );
    }

    #[test]
    fn snapshot_log_creator_view_excludes_other_creators() {
        // No leak: CTO sees their own created tasks; "other-CTO" tasks stay hidden from CTO.
        let (_d, mut c) = open_tmp();
        let mine = tasks::create(&mut c, "CTO", "mine", None, 0, None, None, None, 100).unwrap();
        let theirs = tasks::create(
            &mut c,
            "other-CTO",
            "theirs",
            None,
            0,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        tasks::claim(&mut c, "A", Some(mine), &[], 1000, 100).unwrap();
        tasks::claim(&mut c, "B", Some(theirs), &[], 1000, 100).unwrap();
        let snap = gather(&c, "CTO", &[], 200).unwrap();
        let my_subj = format!("task#{mine}");
        let their_subj = format!("task#{theirs}");
        assert!(snap.log.iter().any(|e| e.subject == my_subj));
        assert!(
            !snap.log.iter().any(|e| e.subject == their_subj),
            "CTO leaked another creator's task events"
        );
    }

    #[test]
    fn snapshot_log_creator_view_drops_closed_tasks() {
        // Once a task is `closed` (terminal), its events stop surfacing on the creator's
        // log — otherwise an idle CTO's log fills with old terminal-state churn. Use
        // direct SQL to set `closed` (no public path: that's the reviewer's via
        // #10 verdict=approve, which would also need an auto-spawned review etc. — too
        // much setup for this unit test).
        let (_d, mut c) = open_tmp();
        let t_closed =
            tasks::create(&mut c, "CTO", "closed-one", None, 0, None, None, None, 100).unwrap();
        let t_open =
            tasks::create(&mut c, "CTO", "open-one", None, 0, None, None, None, 100).unwrap();
        // Force-close the first task directly (events on it have been emitted up to here).
        c.execute(
            "UPDATE tasks SET status='closed' WHERE id=?1",
            rusqlite::params![t_closed],
        )
        .unwrap();
        let snap = gather(&c, "CTO", &[], 200).unwrap();
        let closed_subj = format!("task#{t_closed}");
        let open_subj = format!("task#{t_open}");
        assert!(
            snap.log.iter().any(|e| e.subject == open_subj),
            "CTO must see open task they created"
        );
        assert!(
            !snap.log.iter().any(|e| e.subject == closed_subj),
            "closed task should not surface on creator log: {:?}",
            snap.log
        );
    }

    #[test]
    fn snapshot_log_creator_view_drops_cancelled_tasks() {
        // Same as closed — cancelled is the other terminal state.
        let (_d, mut c) = open_tmp();
        let t_cancelled =
            tasks::create(&mut c, "CTO", "cnx", None, 0, None, None, None, 100).unwrap();
        // Cancel via the proper path (creator can cancel an open task).
        tasks::cancel(&mut c, "CTO", t_cancelled, 150).unwrap();
        let snap = gather(&c, "CTO", &[], 200).unwrap();
        let subj = format!("task#{t_cancelled}");
        assert!(
            !snap.log.iter().any(|e| e.subject == subj),
            "cancelled task should not surface on creator log"
        );
    }

    #[test]
    fn snapshot_log_includes_claim_target_events() {
        let (_d, mut c) = open_tmp();
        // A holds a non-task claim (e.g., pr#1). Events on that target should show up.
        claims::claim(&mut c, "A", "pr#1", 1000, 100).unwrap();
        let snap = gather(&c, "A", &[], 200).unwrap();
        // The claim() above emits a `claim_taken` event with subject=pr#1.
        assert!(
            snap.log
                .iter()
                .any(|e| e.subject == "pr#1" && e.kind == "claim_taken"),
            "expected the claim_taken event on pr#1 to surface; got {:?}",
            snap.log
        );
    }

    // --- omit-empty token discipline -----------------------------------------------------

    #[test]
    fn snapshot_quiet_tick_serializes_to_near_empty_object() {
        let (_d, c) = open_tmp();
        let snap = gather(&c, "A", &[], 200).unwrap();
        // No tasks, no claims, no messages: every optional/list field is empty → omitted.
        let json = serde_json::to_string(&snap).unwrap();
        assert_eq!(json, "{}", "quiet tick must serialize to {{}}, got {json}");
    }

    // --- Phase 3: stop / stop_cleared (#6 control) ---------------------------------------

    #[test]
    fn snapshot_surfaces_global_stop_and_omits_everything_else() {
        // STOP is absolute: when stopped, gather returns ONLY stop + critical. current_task,
        // next_task, direct, notifications, log are all omitted (the agent does nothing
        // but cheap-poll for resume).
        let (_d, mut c) = open_tmp();
        // Set up substantial unrelated state — should NOT bleed through.
        let id = make_task(&mut c, "in-flight", 5, None, 100);
        tasks::claim(&mut c, "A", Some(id), &[], 1000, 100).unwrap();
        feed::post(
            &mut c,
            "Z",
            "info",
            None,
            "to-A",
            None,
            Some("A"),
            1000,
            100,
        )
        .unwrap();
        for body in ["b1", "b2"] {
            feed::post(&mut c, "Z", "info", None, body, None, None, 1000, 100).unwrap();
        }
        crate::control::stop(&mut c, None, "deploy in flight", "cto", 100).unwrap();

        let snap = gather(&c, "A", &[], 200).unwrap();
        let stop = snap.stop.as_ref().expect("stop present under global halt");
        assert_eq!(stop.scope, "global");
        assert_eq!(stop.reason, "deploy in flight");
        assert_eq!(stop.by, "cto");
        // Nothing else surfaces.
        assert!(
            snap.current_task.is_none(),
            "current_task leaked under stop"
        );
        assert!(snap.next_task.is_none());
        assert!(snap.direct.is_empty(), "direct leaked under stop");
        assert!(snap.notifications.is_none());
        assert!(snap.log.is_empty());
        assert!(
            snap.stop_cleared.is_none(),
            "stop_cleared cannot be true during an active stop"
        );
    }

    #[test]
    fn snapshot_surfaces_targeted_stop_only_for_named_agent() {
        let (_d, mut c) = open_tmp();
        crate::control::stop(&mut c, Some("A"), "rate-limited", "cto", 100).unwrap();
        // A sees the stop.
        let snap_a = gather(&c, "A", &[], 200).unwrap();
        let stop = snap_a.stop.as_ref().expect("A is stopped");
        assert_eq!(stop.scope, "agent:A");
        // B is not affected — payload is normal (empty here since no state).
        let snap_b = gather(&c, "B", &[], 200).unwrap();
        assert!(snap_b.stop.is_none(), "B leaked A's targeted stop");
    }

    #[test]
    fn snapshot_critical_still_surfaces_under_stop() {
        // Critical messages are the priority-hint surface — they may explain the halt or
        // be an even-more-urgent directive, so they keep surfacing under stop.
        let (_d, mut c) = open_tmp();
        feed::post(
            &mut c,
            "Z",
            "critical",
            None,
            "EVAC-NOW",
            None,
            Some("A"),
            1000,
            100,
        )
        .unwrap();
        crate::control::stop(&mut c, None, "site outage", "cto", 100).unwrap();
        let snap = gather(&c, "A", &[], 200).unwrap();
        assert!(snap.stop.is_some());
        assert!(
            snap.critical.iter().any(|m| m.body == "EVAC-NOW"),
            "critical msg must surface under stop"
        );
    }

    #[test]
    fn snapshot_stop_cleared_fires_within_window_after_resume() {
        // resume() emits a stop_cleared event; gather sees it within STOP_CLEARED_WINDOW_SECS
        // and surfaces stop_cleared=true.
        let (_d, mut c) = open_tmp();
        crate::control::stop(&mut c, None, "deploy", "cto", 100).unwrap();
        crate::control::resume(&mut c, None, "cto", 110).unwrap();
        // Tight window: 110 + 5s = 115. resume emitted stop_cleared with ts=110, threshold
        // is now-WINDOW = 115-120 = -5 < 110 → match.
        let snap = gather(&c, "A", &[], 115).unwrap();
        assert!(snap.stop.is_none(), "stop should be cleared");
        assert_eq!(
            snap.stop_cleared,
            Some(true),
            "stop_cleared must fire after resume"
        );
    }

    #[test]
    fn snapshot_stop_cleared_drops_after_window() {
        // After STOP_CLEARED_WINDOW_SECS has elapsed since the resume, the signal goes
        // silent — the agent has had ample time to poll and see it (approximation of
        // one-shot semantics; the trade-off is documented on the constant).
        let (_d, mut c) = open_tmp();
        crate::control::stop(&mut c, None, "deploy", "cto", 100).unwrap();
        crate::control::resume(&mut c, None, "cto", 110).unwrap();
        // Past the window: now = 110 + WINDOW + 1 = past threshold.
        let snap = gather(&c, "A", &[], 110 + STOP_CLEARED_WINDOW_SECS + 1).unwrap();
        assert!(
            snap.stop_cleared.is_none(),
            "stop_cleared must drop after window"
        );
    }

    #[test]
    fn snapshot_stop_cleared_respects_scope() {
        // A resume for A's targeted stop emits on `agent:A`. Bob does NOT see it.
        let (_d, mut c) = open_tmp();
        crate::control::stop(&mut c, Some("A"), "rate-limited", "cto", 100).unwrap();
        crate::control::resume(&mut c, Some("A"), "cto", 110).unwrap();
        let snap_a = gather(&c, "A", &[], 115).unwrap();
        assert_eq!(
            snap_a.stop_cleared,
            Some(true),
            "A's resume must surface for A"
        );
        let snap_b = gather(&c, "B", &[], 115).unwrap();
        assert!(
            snap_b.stop_cleared.is_none(),
            "A's resume must NOT leak to B (different scope)"
        );
    }

    #[test]
    fn snapshot_global_resume_surfaces_for_every_agent() {
        // A global resume emits on `global` — every agent sees stop_cleared.
        let (_d, mut c) = open_tmp();
        crate::control::stop(&mut c, None, "deploy", "cto", 100).unwrap();
        crate::control::resume(&mut c, None, "cto", 110).unwrap();
        for agent in ["A", "B", "C"] {
            let snap = gather(&c, agent, &[], 115).unwrap();
            assert_eq!(
                snap.stop_cleared,
                Some(true),
                "global resume must fire for {agent}"
            );
        }
    }

    // --- Caps (per #52 review) ---------------------------------------------------------

    #[test]
    fn snapshot_direct_caps_at_default_msg_limit() {
        // Post 25 direct-to-A messages. The cap is DEFAULT_MSG_LIMIT (20); the SQL `LIMIT`
        // bounds it at the storage layer (no in-memory churn).
        let (_d, mut c) = open_tmp();
        for i in 0..25 {
            feed::post(
                &mut c,
                "Z",
                "info",
                None,
                &format!("m{i}"),
                None,
                Some("A"),
                1000,
                100,
            )
            .unwrap();
        }
        let snap = gather(&c, "A", &[], 200).unwrap();
        assert_eq!(
            snap.direct.len(),
            DEFAULT_MSG_LIMIT as usize,
            "direct must cap at DEFAULT_MSG_LIMIT, got {}",
            snap.direct.len()
        );
        // The first DEFAULT_MSG_LIMIT msgs (lowest seq) are kept — ORDER BY seq ASC.
        assert_eq!(snap.direct[0].body, "m0");
        assert_eq!(snap.direct[19].body, "m19");
    }

    #[test]
    fn snapshot_critical_caps_at_default_msg_limit() {
        // Post 25 critical broadcasts. critical bucket caps at DEFAULT_MSG_LIMIT (20).
        // The notifications.count is exact (25), the inlined `critical` is bounded.
        let (_d, mut c) = open_tmp();
        for i in 0..25 {
            feed::post(
                &mut c,
                "Z",
                "critical",
                None,
                &format!("c{i}"),
                None,
                None,
                1000,
                100,
            )
            .unwrap();
        }
        let snap = gather(&c, "A", &[], 200).unwrap();
        assert_eq!(snap.critical.len(), DEFAULT_MSG_LIMIT as usize);
        let notif = snap.notifications.as_ref().unwrap();
        assert_eq!(
            notif.count, 25,
            "broadcast COUNT must be exact (not capped)"
        );
        assert_eq!(notif.critical.len(), DEFAULT_MSG_LIMIT as usize);
    }

    #[test]
    fn snapshot_log_caps_at_default_log_limit() {
        // Stand up an agent's task + 30 events on its target. log bucket caps at
        // DEFAULT_LOG_LIMIT (20). The cap is SQL `LIMIT`-driven.
        let (_d, mut c) = open_tmp();
        let id = make_task(&mut c, "x", 1, None, 100);
        tasks::claim(&mut c, "A", Some(id), &[], 1000, 100).unwrap();
        let target = format!("task#{id}");
        // claim() emits one event; pad with 30 more to exceed the cap.
        for i in 0..30 {
            let tx = c.transaction().unwrap();
            crate::events::emit(&tx, "task_renewed", &target, &format!("note-{i}"), 100 + i)
                .unwrap();
            tx.commit().unwrap();
        }
        let snap = gather(&c, "A", &[], 200).unwrap();
        assert_eq!(
            snap.log.len(),
            DEFAULT_LOG_LIMIT as usize,
            "log must cap at DEFAULT_LOG_LIMIT, got {}",
            snap.log.len()
        );
    }

    // --- Phase 1b: tick() / auto-ack --------------------------------------------------

    #[test]
    fn tick_advances_cursor_past_returned_direct_msgs() {
        let (_d, mut c) = open_tmp();
        let s1 = feed::post(&mut c, "Z", "info", None, "m1", None, Some("A"), 1000, 100)
            .unwrap()
            .seq;
        feed::post(&mut c, "Z", "info", None, "m2", None, Some("A"), 1000, 100).unwrap();
        let snap = tick(&mut c, "A", &[], 200).unwrap();
        assert_eq!(snap.direct.len(), 2);
        // Cursor should now sit at or past the highest seq we showed (m2's seq).
        let cursor: i64 = c
            .query_row(
                "SELECT last_seq FROM cursors WHERE agent_id='A' AND topic='hub'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(cursor > s1, "cursor must advance past returned direct msgs");
    }

    #[test]
    fn tick_is_at_most_once_on_quiet_re_call() {
        // After a tick that returns direct msgs, a second tick (with no new posts) must
        // return zero direct/critical and the cursor must not regress. This pins the
        // at-most-once behavior the design accepts for token-cheap auto-ack.
        let (_d, mut c) = open_tmp();
        feed::post(&mut c, "Z", "info", None, "m1", None, Some("A"), 1000, 100).unwrap();
        feed::post(&mut c, "Z", "info", None, "m2", None, Some("A"), 1000, 100).unwrap();
        let first = tick(&mut c, "A", &[], 200).unwrap();
        assert_eq!(first.direct.len(), 2);
        let cursor_after_first: i64 = c
            .query_row(
                "SELECT last_seq FROM cursors WHERE agent_id='A' AND topic='hub'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let second = tick(&mut c, "A", &[], 200).unwrap();
        assert!(second.direct.is_empty());
        assert!(second.critical.is_empty());
        assert!(second.notifications.is_none());
        let cursor_after_second: i64 = c
            .query_row(
                "SELECT last_seq FROM cursors WHERE agent_id='A' AND topic='hub'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            cursor_after_first, cursor_after_second,
            "cursor must not regress on a quiet re-call"
        );
    }

    #[test]
    fn tick_acks_broadcasts_so_notifications_count_does_not_double_count() {
        // 3 broadcasts; first tick reports count=3; second tick should report no
        // notifications (the broadcast tail also gets acked, not just direct bodies).
        let (_d, mut c) = open_tmp();
        for body in ["b1", "b2", "b3"] {
            feed::post(&mut c, "Z", "info", None, body, None, None, 1000, 100).unwrap();
        }
        let first = tick(&mut c, "A", &[], 200).unwrap();
        assert_eq!(first.notifications.as_ref().unwrap().count, 3);
        let second = tick(&mut c, "A", &[], 200).unwrap();
        assert!(
            second.notifications.is_none(),
            "broadcasts must not be re-counted on the next tick — got {:?}",
            second.notifications
        );
    }

    #[test]
    fn tick_is_monotonic_under_concurrent_acks() {
        // If an external `feed::read --ack-through` advanced the cursor past where the
        // current tick's max-seen sits, tick's MAX(...) clause must NOT regress it.
        let (_d, mut c) = open_tmp();
        let s1 = feed::post(&mut c, "Z", "info", None, "m1", None, Some("A"), 1000, 100)
            .unwrap()
            .seq;
        let s2 = feed::post(&mut c, "Z", "info", None, "m2", None, Some("A"), 1000, 100)
            .unwrap()
            .seq;
        // Externally ack past s2 (somehow the agent or another caller advanced first).
        feed::read(&mut c, "A", None, Some(s2), feed::ReadFilter::All, 10, 200).unwrap();
        // Tick now: would compute max_seen <= s2 from a stale read, but the MAX clause
        // must keep the cursor at s2 (or higher).
        let _ = tick(&mut c, "A", &[], 200).unwrap();
        let cursor: i64 = c
            .query_row(
                "SELECT last_seq FROM cursors WHERE agent_id='A' AND topic='hub'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            cursor >= s2,
            "cursor regressed below external ack at s2 ({cursor} < {s2})"
        );
        let _ = s1; // silence unused
    }

    #[test]
    fn tick_no_advance_when_inbox_is_empty() {
        // A tick with nothing to read must NOT create a cursor row (we don't churn
        // sqlite writes on quiet ticks).
        let (_d, mut c) = open_tmp();
        let _ = tick(&mut c, "A", &[], 200).unwrap();
        let row: Option<i64> = c
            .query_row(
                "SELECT last_seq FROM cursors WHERE agent_id='A' AND topic='hub'",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        assert!(
            row.is_none(),
            "tick on empty inbox should not write a cursor row"
        );
    }

    #[test]
    fn tick_persists_tier_from_match_labels() {
        let (_d, mut c) = open_tmp();
        let _ = tick(&mut c, "Alice", &["tier:opus-46"], 200).unwrap();
        let tier: Option<String> = c
            .query_row("SELECT tier FROM agents WHERE id='Alice'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(tier, Some("tier:opus-46".to_string()));
    }

    #[test]
    fn tick_without_tier_label_does_not_clear_stored_tier() {
        let (_d, mut c) = open_tmp();
        let _ = tick(&mut c, "Bob", &["tier:opus-47"], 200).unwrap();
        let _ = tick(&mut c, "Bob", &[], 300).unwrap();
        let tier: Option<String> = c
            .query_row("SELECT tier FROM agents WHERE id='Bob'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            tier,
            Some("tier:opus-47".to_string()),
            "tier must not be cleared by a sync without --match-label"
        );
    }

    #[test]
    fn snapshot_gather_is_read_only() {
        let (_d, mut c) = open_tmp();
        // Set up some state so gather() has something to walk.
        let id = make_task(&mut c, "x", 1, None, 100);
        tasks::claim(&mut c, "A", Some(id), &[], 1000, 100).unwrap();
        feed::post(
            &mut c,
            "Z",
            "info",
            None,
            "to-A",
            None,
            Some("A"),
            1000,
            100,
        )
        .unwrap();
        // Snapshot before + after gather should be byte-identical for cursors (the one
        // mutable surface gather could touch via the message cursor — Phase 1b adds the
        // ack; Phase 1a guarantees NO advance).
        let before: Option<i64> = c
            .query_row(
                "SELECT last_seq FROM cursors WHERE agent_id='A' AND topic='hub'",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        let _ = gather(&c, "A", &[], 200).unwrap();
        let after: Option<i64> = c
            .query_row(
                "SELECT last_seq FROM cursors WHERE agent_id='A' AND topic='hub'",
                [],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(
            before, after,
            "gather() advanced the cursor in Phase 1a (should be read-only)"
        );
    }

    // --- pinned notices (issue #78) ------------------------------------------------------

    #[test]
    fn snapshot_surfaces_pinned_cursor_independent() {
        // A pin posted before any agent has ever synced must still appear on first sync —
        // no cursor exists, no ack ever happened.
        let (_d, mut c) = open_tmp();
        let pin = crate::pinned::pin(&mut c, "cto", "MIGRATION IN PROGRESS", 50).unwrap();

        let snap = gather(&c, "fresh-agent", &[], 100).unwrap();
        assert_eq!(snap.pinned.len(), 1);
        assert_eq!(snap.pinned[0].id, pin.id);
        assert_eq!(snap.pinned[0].author, "cto");
        assert_eq!(snap.pinned[0].body, "MIGRATION IN PROGRESS");
    }

    #[test]
    fn snapshot_pinned_persists_across_repeated_ticks_unlike_messages() {
        // Cursor-independent: tick() acks messages so they don't re-appear, but pinned
        // must keep showing on every tick.
        let (_d, mut c) = open_tmp();
        crate::pinned::pin(&mut c, "cto", "standing notice", 50).unwrap();

        let snap1 = tick(&mut c, "A", &[], 100).unwrap();
        assert_eq!(snap1.pinned.len(), 1);
        let snap2 = tick(&mut c, "A", &[], 200).unwrap();
        assert_eq!(snap2.pinned.len(), 1, "pinned must survive cursor advance");
        let snap3 = tick(&mut c, "A", &[], 300).unwrap();
        assert_eq!(snap3.pinned.len(), 1);
    }

    #[test]
    fn snapshot_pinned_visible_during_stop() {
        // The STOP-is-absolute path returns only stop + critical + pinned. Halted agents
        // benefit from the most context (the pin may explain the halt or carry the
        // "while halted do X" instruction).
        let (_d, mut c) = open_tmp();
        crate::pinned::pin(&mut c, "cto", "WHILE HALTED: do nothing", 50).unwrap();
        crate::control::stop(&mut c, None, "deploy", "cto", 100).unwrap();

        let snap = gather(&c, "anyone", &[], 200).unwrap();
        assert!(snap.stop.is_some(), "stop is set");
        assert_eq!(snap.pinned.len(), 1, "pinned must surface even on stop");
        // Other fields must be omitted per STOP-is-absolute.
        assert!(snap.current_task.is_none());
        assert!(snap.next_task.is_none());
    }

    #[test]
    fn snapshot_pinned_removed_after_unpin() {
        let (_d, mut c) = open_tmp();
        let p = crate::pinned::pin(&mut c, "cto", "transient", 50).unwrap();
        assert_eq!(gather(&c, "A", &[], 100).unwrap().pinned.len(), 1);
        crate::pinned::unpin(&mut c, p.id, "cto", 200).unwrap();
        assert!(gather(&c, "A", &[], 300).unwrap().pinned.is_empty());
    }

    #[test]
    fn snapshot_pinned_omitted_when_empty() {
        // Default state: no pins, sync payload omits the `pinned` field entirely
        // (skip_serializing_if = "Vec::is_empty"). Wire economy.
        let (_d, c) = open_tmp();
        let snap = gather(&c, "A", &[], 100).unwrap();
        assert!(snap.pinned.is_empty());
        let json = serde_json::to_string(&snap).unwrap();
        assert!(
            !json.contains("\"pinned\""),
            "empty pinned vec must be omitted on serialization, got: {json}"
        );
    }

    #[test]
    fn snapshot_pinned_ordered_oldest_first() {
        // Locked: deterministic order = insertion order (ascending id), oldest first.
        // Mirrors pinned::list. Lets callers render the same way every tick.
        let (_d, mut c) = open_tmp();
        let a = crate::pinned::pin(&mut c, "cto", "first", 50).unwrap();
        let b = crate::pinned::pin(&mut c, "cto", "second", 60).unwrap();
        let c_pin = crate::pinned::pin(&mut c, "cto", "third", 70).unwrap();
        let snap = gather(&c, "A", &[], 100).unwrap();
        let ids: Vec<i64> = snap.pinned.iter().map(|p| p.id).collect();
        assert_eq!(ids, vec![a.id, b.id, c_pin.id]);
    }

    // -- Agent retirement (issue #97) -----------------------------------------------------

    /// Drive a task to `done` as `agent`: create → claim → update(done). Returns the id.
    /// Mirrors stats.rs's complete_task_as helper but uses a very long claim lease so
    /// `sweep_on_write` (which fires inside every mutator's txn) does NOT delete the
    /// claim row before `load_score_for` joins against it — load_score reads `claims.ts`
    /// as the working-window start, so a swept claim silently zeros the score.
    /// 10⁹ seconds is well past any test's `now`.
    fn drive_done(c: &mut Connection, agent: &str, claim_ts: i64, done_ts: i64) -> i64 {
        let id = make_task(c, &format!("t-{claim_ts}"), 0, None, claim_ts - 1);
        tasks::claim(c, agent, Some(id), &[], 1_000_000_000, claim_ts).unwrap();
        tasks::update(
            c,
            agent,
            id,
            &tasks::TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            done_ts,
        )
        .unwrap();
        id
    }

    #[test]
    fn retire_under_budget_active_agent_gets_normal_next_task() {
        // Sanity: an agent with no load score and an open task in the queue gets a
        // normal next_task and no retire signal.
        let (_d, mut c) = open_tmp();
        let id = make_task(&mut c, "do thing", 1, None, 100);
        let snap = gather_with_budget(&c, "Fresh", &[], 200, 5400, 8).unwrap();
        assert!(snap.retire.is_none(), "fresh agent must not be retiring");
        assert_eq!(snap.next_task.as_ref().unwrap().id, id);
    }

    #[test]
    fn retire_active_secs_budget_suppresses_next_task_and_emits_signal() {
        // Agent with cumulative active-secs above the budget enters `retiring`: the
        // queue's open task is hidden (no new work), and the retire signal is emitted
        // carrying the load-score that triggered it.
        let (_d, mut c) = open_tmp();
        // 4 completed tasks, each 30 min long → 7200 sec cumulative.
        drive_done(&mut c, "Tired", 1000, 1000 + 1800);
        drive_done(&mut c, "Tired", 5000, 5000 + 1800);
        drive_done(&mut c, "Tired", 9000, 9000 + 1800);
        drive_done(&mut c, "Tired", 13000, 13000 + 1800);
        // An open task at the head of the queue — would normally be `next_task`.
        let _q = make_task(&mut c, "fresh-work", 5, None, 20000);

        let snap = gather_with_budget(&c, "Tired", &[], 20000, 5400, 8).unwrap();
        let r = snap.retire.as_ref().expect("retire signal present");
        assert_eq!(r.status, "retired"); // no current/sticky → straight to retired-effective
        assert_eq!(r.tasks_completed, 4);
        assert_eq!(r.total_active_secs, 4 * 1800);
        assert_eq!(r.budget_active_secs, 5400);
        assert_eq!(r.budget_tasks, 8);
        assert!(
            snap.next_task.is_none(),
            "retiring agent must NOT receive new queue work"
        );
    }

    #[test]
    fn retire_tasks_budget_alone_triggers_retirement() {
        // The tasks-count backstop fires even when each individual task is short
        // (total active secs well under the secs budget). 9 tasks at 1 sec each =
        // 9 sec cumulative, way under 5400 — but 9 ≥ 8 trips the count budget.
        let (_d, mut c) = open_tmp();
        let mut now = 100i64;
        for _ in 0..9 {
            drive_done(&mut c, "ManyShort", now, now + 1);
            now += 10;
        }
        let snap = gather_with_budget(&c, "ManyShort", &[], now + 10, 5400, 8).unwrap();
        assert!(
            snap.retire.is_some(),
            "tasks-count budget must trigger retire"
        );
        assert_eq!(snap.retire.as_ref().unwrap().tasks_completed, 9);
    }

    #[test]
    fn retiring_agent_with_sticky_mine_sees_sticky_in_next_task() {
        // Sticky carve-out: when retire would otherwise hide next_task, a sticky task
        // assigned to the retiring agent (own rework — only they can do it) MUST still
        // surface so they can drain before signing off.
        let (_d, mut c) = open_tmp();
        // Build up the load score so the agent is over budget.
        for i in 0..4 {
            drive_done(&mut c, "Dr", 1000 + i * 5000, 1000 + i * 5000 + 1800);
        }
        // Stamp a sticky-rework task assigned to "Dr" the same way `changes`-verdict
        // reopens do (open + assignee + sticky_until in the future).
        let sticky_id = make_task(&mut c, "rework-me", 100, None, 25000);
        c.execute(
            "UPDATE tasks SET status='open', assignee='Dr', sticky_until=?1 WHERE id=?2",
            params![26000_i64, sticky_id],
        )
        .unwrap();
        // Also drop a normal claimable task in the queue (must stay hidden under retire).
        let _open = make_task(&mut c, "new-work", 10, None, 25500);

        let snap = gather_with_budget(&c, "Dr", &[], 25500, 5400, 8).unwrap();
        let nxt = snap.next_task.as_ref().expect("sticky-mine must surface");
        assert_eq!(
            nxt.id, sticky_id,
            "sticky-mine carve-out must surface own rework"
        );
        let r = snap.retire.as_ref().expect("retire signal present");
        // Sticky-mine counts as remaining work → still 'retiring' (not yet 'retired').
        assert_eq!(r.status, "retiring");
    }

    #[test]
    fn tick_persists_retiring_then_retired_across_calls() {
        // First tick (over budget, no remaining work) writes `retired` + retired_at.
        // The next tick (with the timestamp now persisted) returns retired_at in the
        // signal — pinning the "stable across calls" guarantee the dashboard relies on.
        let (_d, mut c) = open_tmp();
        for i in 0..4 {
            drive_done(&mut c, "End", 1000 + i * 5000, 1000 + i * 5000 + 1800);
        }
        let snap1 = tick_with_budget(&mut c, "End", &[], 30_000, 5400, 8).unwrap();
        assert_eq!(snap1.retire.as_ref().unwrap().status, "retired");
        // First tick: retired_at not yet persisted at the read step (write follows).
        // Verify the persisted state.
        let (st, ts) = crate::agents::retire_state(&c, "End").unwrap();
        assert_eq!(st, "retired");
        let stamped = ts.expect("retired_at must be stamped");
        assert_eq!(stamped, 30_000);

        // Second tick at a later `now` — retire signal should still fire, carry the
        // ORIGINAL retired_at (idempotent: mark_retired must not re-stamp).
        let snap2 = tick_with_budget(&mut c, "End", &[], 31_000, 5400, 8).unwrap();
        let r = snap2.retire.as_ref().expect("retire signal still present");
        assert_eq!(r.status, "retired");
        assert_eq!(r.retired_at, Some(30_000));
        assert!(snap2.next_task.is_none());
    }

    #[test]
    fn tick_writes_retiring_status_when_current_task_remains() {
        // Over-budget agent still holding a claimed task → status persists as
        // 'retiring' (NOT retired — they have work to drain). Verify the write actually
        // hit the agents table.
        let (_d, mut c) = open_tmp();
        for i in 0..4 {
            drive_done(&mut c, "Busy", 1000 + i * 5000, 1000 + i * 5000 + 1800);
        }
        // Hold a current claim (open task → claim → still claimed).
        let active = make_task(&mut c, "in-flight", 1, None, 30_000);
        tasks::claim(&mut c, "Busy", Some(active), &[], 3600, 30_100).unwrap();

        let snap = tick_with_budget(&mut c, "Busy", &[], 30_200, 5400, 8).unwrap();
        let r = snap.retire.as_ref().expect("retire signal present");
        assert_eq!(r.status, "retiring");
        // Current task still surfaces (XOR with next_task; retirement doesn't hide it).
        assert!(snap.current_task.is_some());

        let (st, ts) = crate::agents::retire_state(&c, "Busy").unwrap();
        assert_eq!(st, "retiring");
        assert!(
            ts.is_none(),
            "retired_at must NOT be stamped while retiring"
        );
    }

    #[test]
    fn retire_default_budget_constants_match_agents_module() {
        // Pin: the budgets gather() uses without a config override are exactly the
        // ones exported by `agents`. A drift here would silently shift retirement
        // semantics for every test/CLI path that calls plain `gather`/`tick`.
        let (_d, mut c) = open_tmp();
        // Two completed tasks (well under both budgets).
        drive_done(&mut c, "X", 100, 200);
        drive_done(&mut c, "X", 300, 400);
        let snap = gather(&c, "X", &[], 500).unwrap();
        assert!(snap.retire.is_none());
        // Sanity: the constants are what the doc-comment claims.
        assert_eq!(crate::agents::DEFAULT_RETIRE_AFTER_ACTIVE_SECS, 5400);
        assert_eq!(crate::agents::DEFAULT_RETIRE_AFTER_TASKS, 8);
    }
}
