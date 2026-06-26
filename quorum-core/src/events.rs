//! The auto-emitted state-change stream.
//!
//! Events are how the system says "what just happened" without an agent having to narrate it
//! into [`crate::feed`]. The two streams are deliberately separate — auto-emit volume would
//! drown the rare agent-to-agent messages on the feed and waste agent tokens — and have
//! different read APIs (`quorum log` here, `quorum read`/`peek` for the feed).
//!
//! Every event is appended **inside the mutator's own `BEGIN IMMEDIATE` transaction**, so
//! the event and the state change it describes are committed atomically: a reader can never
//! see an event for a state that hasn't landed, or a state without its event. Emitters are
//! the state mutators in [`crate::tasks`], [`crate::claims`], and [`crate::sweep`] (the
//! reaper) — see [`emit`] for the contract.

use crate::error::Result;
use rusqlite::{params, Connection, Row};
use serde::Serialize;

/// Default event TTL — events are noise, not durable state. 24h is enough for `quorum log`
/// to give the day's picture; older events are swept like every other expiring row.
pub const EVENT_TTL_SECS: i64 = 24 * 3600;

/// One state-change event.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Event {
    pub seq: i64,
    pub ts: i64,
    /// Machine-readable category, e.g. `task_created`, `task_claimed`, `task_done`,
    /// `task_released`, `task_cancelled`, `task_reclaimed`, `task_renewed`, `claim_taken`,
    /// `claim_released`, `claim_renewed`. Stable enough for callers to branch on.
    pub kind: String,
    /// Filterable identifier — the entity the event is about (e.g. `task#42`, `pr#2459`).
    /// `quorum log --refs <s>` matches on this column.
    pub subject: String,
    /// Human-readable summary (also machine-parseable since it's short and conventional).
    pub body: String,
    pub expires_at: i64,
}

const COLS: &str = "seq, ts, kind, subject, body, expires_at";

fn row_to_event(r: &Row) -> rusqlite::Result<Event> {
    Ok(Event {
        seq: r.get(0)?,
        ts: r.get(1)?,
        kind: r.get(2)?,
        subject: r.get(3)?,
        body: r.get(4)?,
        expires_at: r.get(5)?,
    })
}

/// Append an event from inside a mutator's existing write context. The caller MUST already
/// hold the write lock (a `BEGIN IMMEDIATE` transaction, or — for the sweep reaper —
/// participation in its outer write) and MUST commit after; emitting outside the state
/// mutation's transaction is a bug (it would let an event and its state disagree).
///
/// Takes `&Connection` so a `&Transaction` callsite passes via deref coercion (rusqlite's
/// `Transaction: Deref<Target = Connection>`).
pub fn emit(conn: &Connection, kind: &str, subject: &str, body: &str, now: i64) -> Result<i64> {
    conn.execute(
        "INSERT INTO events(ts, kind, subject, body, expires_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![now, kind, subject, body, now + EVENT_TTL_SECS],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Read unexpired events with `seq > since`, oldest first. `subject_filter` matches the
/// `subject` column exactly (no LIKE), so callers don't accidentally expose a prefix path.
pub fn list(
    conn: &Connection,
    since: i64,
    subject_filter: Option<&str>,
    limit: i64,
    now: i64,
) -> Result<Vec<Event>> {
    match subject_filter {
        Some(s) => {
            let mut stmt = conn.prepare(&format!(
                "SELECT {COLS} FROM events
                 WHERE seq > ?1 AND expires_at > ?2 AND subject = ?3
                 ORDER BY seq ASC LIMIT ?4"
            ))?;
            let evs = stmt
                .query_map(params![since, now, s, limit], row_to_event)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(evs)
        }
        None => {
            let mut stmt = conn.prepare(&format!(
                "SELECT {COLS} FROM events
                 WHERE seq > ?1 AND expires_at > ?2
                 ORDER BY seq ASC LIMIT ?3"
            ))?;
            let evs = stmt
                .query_map(params![since, now, limit], row_to_event)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(evs)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::TransactionBehavior;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn emit_one(c: &mut Connection, kind: &str, subject: &str, body: &str, now: i64) -> i64 {
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let seq = emit(&tx, kind, subject, body, now).unwrap();
        tx.commit().unwrap();
        seq
    }

    #[test]
    fn emit_returns_increasing_seq_and_records_fields() {
        let (_d, mut c) = open_tmp();
        let s1 = emit_one(&mut c, "task_created", "task#1", "created", 100);
        let s2 = emit_one(&mut c, "task_claimed", "task#1", "taken by A", 101);
        assert!(s2 > s1);
        let evs = list(&c, 0, None, 10, 100).unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].kind, "task_created");
        assert_eq!(evs[0].subject, "task#1");
        assert_eq!(evs[0].body, "created");
        assert_eq!(evs[1].kind, "task_claimed");
    }

    #[test]
    fn list_filters_by_subject_exactly() {
        let (_d, mut c) = open_tmp();
        emit_one(&mut c, "task_created", "task#1", "a", 100);
        emit_one(&mut c, "task_created", "task#11", "b", 100);
        emit_one(&mut c, "claim_taken", "pr#2459", "by Z", 100);
        // exact-match: "task#1" must NOT pick up "task#11"
        let evs = list(&c, 0, Some("task#1"), 10, 100).unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].subject, "task#1");
        // pr filter
        let evs2 = list(&c, 0, Some("pr#2459"), 10, 100).unwrap();
        assert_eq!(evs2.len(), 1);
        assert_eq!(evs2[0].kind, "claim_taken");
        // unknown subject → empty
        assert!(list(&c, 0, Some("task#999"), 10, 100).unwrap().is_empty());
    }

    #[test]
    fn list_since_seq_is_a_strict_delta_filter() {
        let (_d, mut c) = open_tmp();
        let s1 = emit_one(&mut c, "task_created", "task#1", "a", 100);
        let s2 = emit_one(&mut c, "task_claimed", "task#1", "b", 100);
        let after_s1 = list(&c, s1, None, 10, 100).unwrap();
        assert_eq!(after_s1.len(), 1);
        assert_eq!(after_s1[0].seq, s2);
        let after_s2 = list(&c, s2, None, 10, 100).unwrap();
        assert!(after_s2.is_empty());
    }

    #[test]
    fn list_hides_expired_events_at_exact_boundary() {
        let (_d, mut c) = open_tmp();
        // emit at now=100, default TTL → expires at 100 + 24h. Force by writing past expiry.
        emit_one(&mut c, "task_created", "task#1", "a", 100);
        // expires at 100 + 24*3600 = 86500. Boundary: dead iff expires_at <= now.
        let live = list(&c, 0, None, 10, 86499).unwrap();
        assert_eq!(live.len(), 1);
        let dead = list(&c, 0, None, 10, 86500).unwrap();
        assert!(dead.is_empty(), "exact boundary: expires_at <= now is dead");
    }

    #[test]
    fn list_limit_caps_returned_rows() {
        let (_d, mut c) = open_tmp();
        for i in 0..5 {
            emit_one(&mut c, "task_created", &format!("task#{i}"), "x", 100);
        }
        let evs = list(&c, 0, None, 3, 100).unwrap();
        assert_eq!(evs.len(), 3);
    }

    #[test]
    fn body_roundtrips_byte_exact() {
        let (_d, mut c) = open_tmp();
        let body = "héllo \"world\"\n`$x`\n";
        emit_one(&mut c, "task_created", "task#1", body, 100);
        let evs = list(&c, 0, None, 10, 100).unwrap();
        assert_eq!(evs[0].body, body);
    }
}
