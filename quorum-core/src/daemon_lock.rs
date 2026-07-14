//! Single-daemon-per-DB guard.
//!
//! On `quorum serve` startup, the daemon acquires an exclusive lease in the
//! `daemon_lock` table (one-row-max). A second daemon on the same DB sees the
//! lease and either takes it over (stale holder) or exits loudly (live holder).
//!
//! The lease is refreshed on every tick (heartbeat_at updated) and released on
//! clean shutdown. The check-and-acquire is atomic (single `BEGIN IMMEDIATE`
//! transaction) to prevent TOCTOU races between concurrent daemons.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

/// Outcome of a [`try_acquire`] attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum AcquireResult {
    /// Lock acquired (fresh, taken over from stale/dead, or same pid re-acquire).
    Acquired,
    /// Lock held by a live daemon — the caller must exit.
    Held { holder_pid: i64, heartbeat_age: i64 },
}

/// Atomically check and acquire the daemon lock in a single transaction.
///
/// `is_pid_alive` is called inside the write lock to decide whether the current
/// holder (if any) is stale. This prevents a TOCTOU race: two daemons starting
/// simultaneously both see "no/stale holder" and both write — under a single
/// `BEGIN IMMEDIATE`, only one proceeds at a time.
pub fn try_acquire(
    conn: &mut Connection,
    pid: i64,
    now: i64,
    stale_secs: i64,
    is_pid_alive: impl Fn(i64) -> bool,
) -> Result<AcquireResult> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let existing: Option<(i64, i64)> = tx
        .query_row(
            "SELECT pid, heartbeat_at FROM daemon_lock WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;

    if let Some((holder_pid, heartbeat_at)) = existing {
        let heartbeat_age = now - heartbeat_at;
        let alive = is_pid_alive(holder_pid);
        // Same pid re-acquiring is always allowed (daemon restart in-place).
        if holder_pid != pid && alive && heartbeat_age <= stale_secs {
            tx.commit()?;
            return Ok(AcquireResult::Held {
                holder_pid,
                heartbeat_age,
            });
        }
    }

    tx.execute(
        "INSERT INTO daemon_lock(id, pid, heartbeat_at) VALUES (1, ?1, ?2)
         ON CONFLICT(id) DO UPDATE SET pid = excluded.pid, heartbeat_at = excluded.heartbeat_at",
        params![pid, now],
    )?;
    tx.commit()?;
    Ok(AcquireResult::Acquired)
}

/// Refresh the heartbeat timestamp. Returns the number of rows updated —
/// 0 means the lock was stolen by another daemon (pid no longer matches).
pub fn refresh(conn: &Connection, pid: i64, now: i64) -> Result<usize> {
    let n = conn.execute(
        "UPDATE daemon_lock SET heartbeat_at = ?1 WHERE id = 1 AND pid = ?2",
        params![now, pid],
    )?;
    Ok(n)
}

/// Read the current lock row without taking the write lock.
/// Returns `None` when the table is empty (no daemon has ever started).
pub fn peek(conn: &Connection) -> Result<Option<(i64, i64)>> {
    let row = conn
        .query_row(
            "SELECT pid, heartbeat_at FROM daemon_lock WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(row)
}

/// Read daemon_lock and compute liveness. Same check `try_acquire` uses for
/// takeover: heartbeat < stale_secs AND pid alive via the caller's closure.
/// Pure read — does not take the write lock or bump last_seen.
pub fn liveness(
    conn: &Connection,
    now: i64,
    stale_secs: i64,
    is_pid_alive: impl Fn(i64) -> bool,
) -> Result<crate::stats::DaemonLiveness> {
    use crate::stats::DaemonLiveness;
    match peek(conn)? {
        None => Ok(DaemonLiveness::None),
        Some((pid, heartbeat_at)) => {
            let age = now - heartbeat_at;
            let alive = is_pid_alive(pid);
            if alive && age <= stale_secs {
                Ok(DaemonLiveness::Alive {
                    pid,
                    heartbeat_age_secs: age,
                })
            } else {
                Ok(DaemonLiveness::Stale {
                    pid,
                    heartbeat_age_secs: age,
                    pid_dead: !alive,
                })
            }
        }
    }
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

    const STALE: i64 = 30;

    #[test]
    fn acquire_on_empty_db_succeeds() {
        let (_d, mut c) = open_tmp();
        let r = try_acquire(&mut c, 12345, 1000, STALE, |_| false).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
        let hb: i64 = c
            .query_row(
                "SELECT heartbeat_at FROM daemon_lock WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hb, 1000);
    }

    #[test]
    fn same_pid_reacquire_succeeds() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        let r = try_acquire(&mut c, 100, 1001, STALE, |_| true).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
    }

    #[test]
    fn live_holder_is_rejected() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        // Different pid, holder alive, heartbeat fresh.
        let r = try_acquire(&mut c, 200, 1001, STALE, |pid| pid == 100).unwrap();
        assert_eq!(
            r,
            AcquireResult::Held {
                holder_pid: 100,
                heartbeat_age: 1
            }
        );
    }

    #[test]
    fn stale_heartbeat_allows_takeover() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        let r = try_acquire(&mut c, 200, 1000 + STALE + 1, STALE, |_| true).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
    }

    #[test]
    fn exact_stale_boundary_is_still_live() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        // heartbeat_age == STALE exactly — must still be considered live.
        let r = try_acquire(&mut c, 200, 1000 + STALE, STALE, |_| true).unwrap();
        assert_eq!(
            r,
            AcquireResult::Held {
                holder_pid: 100,
                heartbeat_age: STALE,
            }
        );
    }

    #[test]
    fn dead_pid_allows_takeover() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        // Holder pid is dead (is_pid_alive returns false), heartbeat fresh.
        let r = try_acquire(&mut c, 200, 1001, STALE, |_| false).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
    }

    #[test]
    fn release_clears_lock() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        release(&c, 100).unwrap();
        // Now a new daemon can acquire.
        let r = try_acquire(&mut c, 200, 1001, STALE, |_| true).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
    }

    #[test]
    fn release_wrong_pid_is_noop() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        release(&c, 999).unwrap();
        let count: i64 = c
            .query_row("SELECT COUNT(*) FROM daemon_lock", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn refresh_updates_heartbeat() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        let n = refresh(&c, 100, 5000).unwrap();
        assert_eq!(n, 1);
        let hb: i64 = c
            .query_row(
                "SELECT heartbeat_at FROM daemon_lock WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hb, 5000);
    }

    #[test]
    fn concurrent_try_acquire_exactly_one_winner() {
        use std::sync::{Arc, Barrier};
        for _ in 0..12 {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("q.db");
            {
                let c = crate::db::open(&db_path).unwrap();
                drop(c);
            }
            let n = 8;
            let barrier = Arc::new(Barrier::new(n));
            let path = Arc::new(db_path);
            let handles: Vec<_> = (0..n)
                .map(|i| {
                    let barrier = Arc::clone(&barrier);
                    let path = Arc::clone(&path);
                    std::thread::spawn(move || {
                        let mut conn = crate::db::open(&path).unwrap();
                        barrier.wait();
                        let pid = 1000 + i as i64;
                        try_acquire(&mut conn, pid, 5000, STALE, |_| true)
                    })
                })
                .collect();
            let results: Vec<_> = handles
                .into_iter()
                .map(|h| h.join().unwrap().unwrap())
                .collect();
            let acquired = results
                .iter()
                .filter(|r| matches!(r, AcquireResult::Acquired))
                .count();
            assert_eq!(acquired, 1, "exactly one thread must acquire the lock");
            let held = results
                .iter()
                .filter(|r| matches!(r, AcquireResult::Held { .. }))
                .count();
            assert_eq!(held, n - 1);
        }
    }

    #[test]
    fn refresh_wrong_pid_returns_zero() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, 1000, STALE, |_| true).unwrap();
        let n = refresh(&c, 999, 5000).unwrap();
        assert_eq!(n, 0);
        let hb: i64 = c
            .query_row(
                "SELECT heartbeat_at FROM daemon_lock WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hb, 1000); // unchanged
    }

    #[test]
    fn peek_empty_returns_none() {
        let (_d, c) = open_tmp();
        assert_eq!(peek(&c).unwrap(), None);
    }

    #[test]
    fn peek_returns_pid_and_heartbeat() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 42, 9000, STALE, |_| true).unwrap();
        assert_eq!(peek(&c).unwrap(), Some((42, 9000)));
    }

    #[test]
    fn liveness_empty_is_none() {
        use crate::stats::DaemonLiveness;
        let (_d, c) = open_tmp();
        let l = liveness(&c, 1000, STALE, |_| true).unwrap();
        assert_eq!(l, DaemonLiveness::None);
    }

    #[test]
    fn liveness_alive() {
        use crate::stats::DaemonLiveness;
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 42, 1000, STALE, |_| true).unwrap();
        let l = liveness(&c, 1004, STALE, |_| true).unwrap();
        assert_eq!(
            l,
            DaemonLiveness::Alive {
                pid: 42,
                heartbeat_age_secs: 4
            }
        );
    }

    #[test]
    fn liveness_stale_heartbeat() {
        use crate::stats::DaemonLiveness;
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 42, 1000, STALE, |_| true).unwrap();
        let l = liveness(&c, 1000 + STALE + 1, STALE, |_| true).unwrap();
        assert_eq!(
            l,
            DaemonLiveness::Stale {
                pid: 42,
                heartbeat_age_secs: STALE + 1,
                pid_dead: false
            }
        );
    }

    #[test]
    fn liveness_dead_pid() {
        use crate::stats::DaemonLiveness;
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 42, 1000, STALE, |_| true).unwrap();
        let l = liveness(&c, 1004, STALE, |_| false).unwrap();
        assert_eq!(
            l,
            DaemonLiveness::Stale {
                pid: 42,
                heartbeat_age_secs: 4,
                pid_dead: true
            }
        );
    }
}
