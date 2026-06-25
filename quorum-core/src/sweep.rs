//! Physical reclamation of expired rows.
//!
//! Expiry is *logical* first — every read filters `expires_at > now`, so expired rows are
//! invisible immediately. This module is housekeeping that reclaims the disk. [`sweep_on_write`]
//! is the bounded, opportunistic sweep every mutation runs; [`sweep_all`] is the unbounded
//! explicit sweep (`quorum sweep`) plus a WAL checkpoint.

use crate::error::Result;
use rusqlite::{params, Connection};

/// Done tasks are reclaimed this long after entering `done`. Default; Phase 6 config overrides.
pub const DONE_TASK_TTL_SECS: i64 = 7 * 24 * 3600;

/// Max rows reclaimed per table by an opportunistic sweep-on-write.
pub const SWEEP_LIMIT: usize = 100;

/// Reaper: return any `claimed` task whose lease has lapsed (no active, unexpired lease on
/// `task#<id>`) back to `open`, clearing the assignee, and emit a `task_reclaimed` event per
/// task to the event log (NOT to the message feed — events live separate from messaging per
/// issue #4 so auto-events don't drown agent-to-agent messages).
///
/// Runs inside the caller's write transaction as part of [`sweep_on_write`] — this is how a
/// lost agent's work re-enters the queue with no background daemon. Lease expiry boundary
/// matches the rest of the engine: a lease is live iff `expires_at > now`.
pub fn reap_lapsed_tasks(conn: &Connection, now: i64) -> Result<()> {
    // Snapshot lapsed-claimed tasks first (we need the about-to-be-cleared assignee for the
    // event body). The correlated `'task#' || tasks.id` rebuilds the lease target per row.
    let lapsed: Vec<(i64, Option<String>)> = {
        let mut stmt = conn.prepare(
            "SELECT id, assignee FROM tasks
             WHERE status='claimed' AND NOT EXISTS (
                 SELECT 1 FROM claims c
                 WHERE c.target = 'task#' || tasks.id AND c.active=1 AND c.expires_at > ?1
             )",
        )?;
        let rows = stmt
            .query_map(params![now], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };
    for (id, prev) in &lapsed {
        let target = format!("task#{id}");
        conn.execute(
            "UPDATE tasks SET status='open', assignee=NULL, updated_at=?1 WHERE id=?2",
            params![now, id],
        )?;
        // Clear any lingering (now-expired) lease row so the next claim starts clean.
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1",
            params![target],
        )?;
        // Emit to the event log. Body carries the prev_assignee so consumers can identify
        // whose work returned to the queue without parsing JSON; `subject = task#<id>` makes
        // it filterable via `quorum log --refs task#<id>`.
        let body = match prev {
            Some(a) => format!("reclaimed from {a} (lease lapsed) → open"),
            None => "reclaimed (lease lapsed) → open".to_string(),
        };
        crate::events::emit_conn(conn, "task_reclaimed", &target, &body, now)?;
    }
    Ok(())
}

fn delete_bounded(conn: &Connection, table: &str, now: i64, limit: usize) -> Result<()> {
    // `table` is always a string literal from this module — never user input.
    let sql = format!(
        "DELETE FROM {table} WHERE rowid IN \
         (SELECT rowid FROM {table} WHERE expires_at < ?1 LIMIT ?2)"
    );
    conn.execute(&sql, params![now, limit as i64])?;
    Ok(())
}

/// Bounded sweep run opportunistically inside every mutation's transaction. The `LIMIT`
/// keeps a large backlog from making one command's transaction pathologically long.
pub fn sweep_on_write(conn: &Connection, now: i64, limit: usize) -> Result<()> {
    // Correctness first: reclaim lost-agent tasks before the housekeeping deletes (a lapsed
    // `claimed` task must become re-claimable on the next write).
    reap_lapsed_tasks(conn, now)?;
    delete_bounded(conn, "messages", now, limit)?;
    delete_bounded(conn, "events", now, limit)?;
    delete_bounded(conn, "errors", now, limit)?;
    // Deletes expired claims of any `active` value: an expired `active=1` row is already
    // logically dead (the read-filter hid it), and removing it just frees the partial index.
    delete_bounded(conn, "claims", now, limit)?;
    conn.execute(
        "DELETE FROM tasks WHERE rowid IN \
         (SELECT rowid FROM tasks WHERE status='done' AND updated_at < ?1 LIMIT ?2)",
        params![now - DONE_TASK_TTL_SECS, limit as i64],
    )?;
    Ok(())
}

/// Unbounded sweep + `wal_checkpoint(TRUNCATE)`. Backs `quorum sweep`.
pub fn sweep_all(conn: &Connection, now: i64) -> Result<()> {
    reap_lapsed_tasks(conn, now)?;
    conn.execute("DELETE FROM messages WHERE expires_at < ?1", params![now])?;
    conn.execute("DELETE FROM events WHERE expires_at < ?1", params![now])?;
    conn.execute("DELETE FROM errors WHERE expires_at < ?1", params![now])?;
    conn.execute("DELETE FROM claims WHERE expires_at < ?1", params![now])?;
    conn.execute(
        "DELETE FROM tasks WHERE status='done' AND updated_at < ?1",
        params![now - DONE_TASK_TTL_SECS],
    )?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    Ok(())
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
    fn sweep_removes_expired_keeps_live() {
        let (_d, c) = open_tmp();
        c.execute(
            "INSERT INTO messages(ts,author,topic,kind,body,expires_at)
             VALUES (1,'a','hub','info','expired',10), (1,'a','hub','info','live',9999)",
            [],
        )
        .unwrap();
        sweep_on_write(&c, 100, 100).unwrap();
        let bodies: Vec<String> = c
            .prepare("SELECT body FROM messages ORDER BY seq")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(bodies, vec!["live".to_string()]);
    }

    #[test]
    fn reaper_returns_lapsed_claimed_task_to_open_with_event() {
        let (_d, mut c) = open_tmp();
        // A claimed task with a short lease (dead at 1100).
        let id = crate::tasks::create(&mut c, "boss", "x", None, 0, None, None, 1000).unwrap();
        crate::tasks::claim(&mut c, "A", Some(id), &[], 100, 1000).unwrap();
        // Before expiry: reaper leaves it alone.
        reap_lapsed_tasks(&c, 1050).unwrap();
        assert_eq!(
            crate::tasks::get(&c, id).unwrap().unwrap().status,
            "claimed"
        );
        // After the lease lapses: reaper returns it to open, clears assignee, emits a
        // `task_reclaimed` event to the EVENT LOG (not the message feed).
        reap_lapsed_tasks(&c, 1100).unwrap();
        let t = crate::tasks::get(&c, id).unwrap().unwrap();
        assert_eq!(t.status, "open");
        assert!(t.assignee.is_none());
        let target = format!("task#{id}");
        let evs = crate::events::list(&c, 0, Some(&target), 10, 1100).unwrap();
        let reclaimed = evs.iter().filter(|e| e.kind == "task_reclaimed").count();
        assert_eq!(reclaimed, 1, "exactly one task_reclaimed event");
        // The message feed is NOT polluted with auto-events.
        let msg_count: i64 = c
            .query_row(
                "SELECT count(*) FROM messages WHERE body LIKE '%reclaimed%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            msg_count, 0,
            "reaper events must NOT appear on the message feed"
        );
        // Idempotent: a now-open task is not reaped again (no duplicate event).
        reap_lapsed_tasks(&c, 1200).unwrap();
        let evs2 = crate::events::list(&c, 0, Some(&target), 10, 1200).unwrap();
        let reclaimed2 = evs2.iter().filter(|e| e.kind == "task_reclaimed").count();
        assert_eq!(
            reclaimed2, 1,
            "reaper must not re-fire on an already-open task"
        );
    }

    #[test]
    fn sweep_respects_limit() {
        let (_d, c) = open_tmp();
        for i in 0..5 {
            c.execute(
                "INSERT INTO messages(ts,author,topic,kind,body,expires_at)
                 VALUES (1,'a','hub','info',?1,10)",
                params![format!("m{i}")],
            )
            .unwrap();
        }
        sweep_on_write(&c, 100, 2).unwrap(); // only 2 of 5 expired rows reclaimed
        let n: i64 = c
            .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3);
    }
}
