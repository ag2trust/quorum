//! Single-daemon-per-DB guard.
//!
//! On `quorum serve` startup, the daemon acquires an exclusive lease in the
//! `daemon_lock` table (one-row-max). A second daemon on the same DB sees the
//! lease and either takes it over (stale holder) or exits loudly (live holder).
//!
//! The lease is refreshed on every tick (heartbeat_at updated) and released on
//! clean shutdown. Staleness is the caller's judgment — this module only reads
//! and writes the row.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

/// The current holder of the daemon lock (if any).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockHolder {
    pub pid: i64,
    pub heartbeat_at: i64,
}

/// Try to acquire the daemon lock, taking over from any existing holder.
/// Unconditional — callers must check staleness/liveness before calling.
pub fn force_acquire(conn: &mut Connection, pid: i64, now: i64) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO daemon_lock(id, pid, heartbeat_at) VALUES (1, ?1, ?2)
         ON CONFLICT(id) DO UPDATE SET pid = excluded.pid, heartbeat_at = excluded.heartbeat_at",
        params![pid, now],
    )?;
    tx.commit()?;
    Ok(())
}

/// Read the current lock holder, if any.
pub fn current_holder(conn: &Connection) -> Result<Option<LockHolder>> {
    let r = conn
        .query_row(
            "SELECT pid, heartbeat_at FROM daemon_lock WHERE id = 1",
            [],
            |r| {
                Ok(LockHolder {
                    pid: r.get(0)?,
                    heartbeat_at: r.get(1)?,
                })
            },
        )
        .optional()?;
    Ok(r)
}

/// Refresh the heartbeat timestamp. Called on every daemon tick.
pub fn refresh(conn: &Connection, pid: i64, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE daemon_lock SET heartbeat_at = ?1 WHERE id = 1 AND pid = ?2",
        params![now, pid],
    )?;
    Ok(())
}

/// Release the lock on clean shutdown.
pub fn release(conn: &Connection, pid: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM daemon_lock WHERE id = 1 AND pid = ?1",
        params![pid],
    )?;
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
    fn acquire_on_empty_db_succeeds() {
        let (_d, mut c) = open_tmp();
        force_acquire(&mut c, 12345, 1000).unwrap();
        let h = current_holder(&c).unwrap().unwrap();
        assert_eq!(h.pid, 12345);
        assert_eq!(h.heartbeat_at, 1000);
    }

    #[test]
    fn force_acquire_overwrites_existing() {
        let (_d, mut c) = open_tmp();
        force_acquire(&mut c, 100, 1000).unwrap();
        force_acquire(&mut c, 200, 2000).unwrap();
        let h = current_holder(&c).unwrap().unwrap();
        assert_eq!(h.pid, 200);
        assert_eq!(h.heartbeat_at, 2000);
    }

    #[test]
    fn release_clears_lock() {
        let (_d, mut c) = open_tmp();
        force_acquire(&mut c, 100, 1000).unwrap();
        release(&c, 100).unwrap();
        assert!(current_holder(&c).unwrap().is_none());
    }

    #[test]
    fn release_wrong_pid_is_noop() {
        let (_d, mut c) = open_tmp();
        force_acquire(&mut c, 100, 1000).unwrap();
        release(&c, 999).unwrap();
        assert!(current_holder(&c).unwrap().is_some());
    }

    #[test]
    fn refresh_updates_heartbeat() {
        let (_d, mut c) = open_tmp();
        force_acquire(&mut c, 100, 1000).unwrap();
        refresh(&c, 100, 5000).unwrap();
        let h = current_holder(&c).unwrap().unwrap();
        assert_eq!(h.heartbeat_at, 5000);
    }

    #[test]
    fn refresh_wrong_pid_is_noop() {
        let (_d, mut c) = open_tmp();
        force_acquire(&mut c, 100, 1000).unwrap();
        refresh(&c, 999, 5000).unwrap();
        let h = current_holder(&c).unwrap().unwrap();
        assert_eq!(h.heartbeat_at, 1000); // unchanged
    }

    #[test]
    fn current_holder_empty_returns_none() {
        let (_d, c) = open_tmp();
        assert!(current_holder(&c).unwrap().is_none());
    }
}
