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
    delete_bounded(conn, "messages", now, limit)?;
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
    conn.execute("DELETE FROM messages WHERE expires_at < ?1", params![now])?;
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
