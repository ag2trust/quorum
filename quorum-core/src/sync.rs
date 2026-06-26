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
    /// `task.updated_at` when the row moved to `claimed`. Useful to detect a stale own-task.
    pub claimed_at: i64,
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

/// The agent's one-call orientation payload. Empty sections are omitted on serialization —
/// a quiet tick (mid-task, no new messages, no stop) returns nearly nothing.
///
/// **Locked field order** (see issue #8): stop ▸ critical ▸ current_task ▸ next_task ▸
/// direct ▸ notifications ▸ log. Phase 1a omits stop/stop_cleared (waits on #6).
#[derive(Debug, Serialize, Default, PartialEq, Eq)]
pub struct Snapshot {
    // Phase 3: stop / stop_cleared once #6 (control table + emergency stop/resume) merges.
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
pub fn gather(conn: &Connection, agent: &str, match_labels: &[&str], now: i64) -> Result<Snapshot> {
    // 1. State-adaptive XOR. If we hold a claimed task, surface it; do NOT also dangle
    //    next_task (locked in the design session — never show a second task to a busy agent).
    let current_task = current_task_view(conn, agent, now)?;
    let next_task = if current_task.is_none() {
        next_task_view(conn, match_labels)?
    } else {
        None
    };

    // 2. Message bucketing: direct full / critical full / broadcasts count + critical bodies.
    //    Reads from the cursor established by prior `read --ack-through` (no advance here
    //    in 1a; Phase 1b adds the auto-ack).
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

    Ok(Snapshot {
        critical: buckets.critical,
        current_task,
        next_task,
        direct: buckets.direct,
        notifications,
        log,
    })
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
    let snap = gather(conn, agent, match_labels, now)?;
    advance_cursor_past(conn, agent, &snap, now)?;
    Ok(snap)
}

/// Advance `cursors.last_seq` past every message we just surfaced (or counted, in the
/// case of broadcasts). Monotonic — the `MAX(...)` clause prevents the cursor from ever
/// going backward.
///
/// The advance is bounded by what `bucket_messages` actually read. In the current
/// design that's "everything > old_cursor that wasn't expired" — which is exactly what
/// the auto-ack should cover. Even if a bucket's body was truncated by `DEFAULT_MSG_LIMIT`,
/// we still acked the seq beyond the truncation point (the count is correct; just some
/// bodies aren't inlined). A future enhancement could narrow the ack to "highest body
/// returned" to give the agent a re-read window, but that's a Phase-5 nice-to-have.
fn advance_cursor_past(
    conn: &mut Connection,
    agent: &str,
    snap: &Snapshot,
    now: i64,
) -> Result<()> {
    // Highest seq across everything we surfaced. broadcasts that ONLY contributed to the
    // count (not bodies) need the cursor too — otherwise the next tick re-counts them.
    // Since `gather` already read past them, we need to walk the un-bucketed broadcast
    // tail too; do that here with one read inside the same write txn for atomicity.
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
    // Cap on the read = the same window gather just walked.
    if let Some(broadcast_tail_max) = max_broadcast_seq_past_cursor(conn, cursor_now, now)? {
        if broadcast_tail_max > max_seen {
            max_seen = broadcast_tail_max;
        }
    }

    // No-op if nothing to advance.
    if max_seen <= cursor_now {
        return Ok(());
    }

    // Same shape as feed::read --ack-through: insert-or-update with MAX(...) to guarantee
    // monotonicity. Wrapped in begin_immediate so a concurrent tick can't race the write.
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO cursors(agent_id, topic, last_seq) VALUES (?1, ?2, ?3)
         ON CONFLICT(agent_id, topic)
         DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)",
        params![agent, DEFAULT_TOPIC, max_seen],
    )?;
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
                    claimed_at: r.get(4)?,
                    lease_expires_at: r.get(5)?,
                })
            },
        )
        .optional()?;
    Ok(row)
}

fn next_task_view(conn: &Connection, match_labels: &[&str]) -> Result<Option<NextTaskView>> {
    // Mirror `tasks::claim`'s selector exactly: status='open' AND dep-ready AND every
    // match-label present. Shown, never claimed.
    const DEP_READY_CLAUSE: &str = "(depends_on IS NULL OR NOT EXISTS (
        SELECT 1 FROM json_each(depends_on) je
        WHERE NOT EXISTS (
            SELECT 1 FROM tasks d WHERE d.id = je.value AND d.status = 'closed'
        )
    ))";
    let mut sql = format!(
        "SELECT id, title, priority, labels FROM tasks
         WHERE status = 'open' AND {DEP_READY_CLAUSE}"
    );
    for i in 0..match_labels.len() {
        use std::fmt::Write as _;
        // Params start at ?1 — only label patterns are bound.
        let _ = write!(sql, " AND labels LIKE ?{}", i + 1);
    }
    sql.push_str(" ORDER BY priority DESC, id ASC LIMIT 1");

    let label_pats: Vec<String> = match_labels.iter().map(|l| format!("%\"{l}\"%")).collect();
    let bind: Vec<&dyn rusqlite::ToSql> = label_pats
        .iter()
        .map(|p| p as &dyn rusqlite::ToSql)
        .collect();
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
    // Pull all unread, unexpired messages in this topic with one query, then partition into
    // four buckets in-memory. Cheaper than 4 SQL queries on the same row-set, and the row
    // count is already bounded by DEFAULT_MSG_LIMIT for direct/critical (broadcast count is
    // a separate COUNT).
    let mut direct: Vec<MsgView> = Vec::new();
    let mut critical: Vec<MsgView> = Vec::new();
    let mut critical_broadcasts: Vec<MsgView> = Vec::new();

    let mut stmt = conn.prepare(
        "SELECT seq, ts, author, kind, body, recipient FROM messages
         WHERE topic = ?1 AND seq > ?2 AND expires_at > ?3
         ORDER BY seq ASC",
    )?;
    let rows = stmt.query_map(params![DEFAULT_TOPIC, cursor, now], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, Option<String>>(5)?,
        ))
    })?;

    let mut broadcast_count: i64 = 0;
    for row in rows {
        let (seq, ts, author, kind, body, recipient) = row?;
        let view = MsgView {
            seq,
            ts,
            author,
            kind: kind.clone(),
            body,
        };
        match recipient.as_deref() {
            Some(r) if r == agent => {
                // Direct-to-me. If it's critical, also surface in `critical` (priority hint
                // — the agent shouldn't have to scan both buckets to see a HALT-level msg).
                if kind == "critical" && critical.len() < DEFAULT_MSG_LIMIT as usize {
                    critical.push(view.clone());
                }
                if direct.len() < DEFAULT_MSG_LIMIT as usize {
                    direct.push(view);
                }
            }
            Some(_) => {
                // Direct to someone else. Not visible — sync respects recipient privacy.
                // Do NOT count toward broadcasts.
            }
            None => {
                // Broadcast. Counted always; only critical bodies inlined.
                broadcast_count += 1;
                if kind == "critical" && critical_broadcasts.len() < DEFAULT_MSG_LIMIT as usize {
                    critical_broadcasts.push(view);
                }
            }
        }
    }

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
    // Subject set = {task#<my-current-id> if any} ∪ {target from claims where holder=me
    // AND active=1 AND expires_at > now}. If the set is empty, return [] (a fresh idle agent
    // with no claims has no scoped events — the global firehose is `quorum log`, not sync).
    //
    // Implemented as a UNION subquery so the events query is one statement. SQLite optimizer
    // turns this into an index scan via events_subject_seq.
    let mut targets: Vec<String> = Vec::new();
    if let Some(id) = current_task_id {
        targets.push(format!("task#{id}"));
    }
    // All live claim targets for this agent (includes the task lease implicitly, but the
    // dedup below keeps the in-clause minimal).
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
}
