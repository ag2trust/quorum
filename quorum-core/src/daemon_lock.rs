//! Single-daemon-per-DB guard.
//!
//! On `quorum serve` startup, the daemon acquires an exclusive lease in the
//! `daemon_lock` table (one-row-max). A second daemon on the same DB either
//! takes the lease over (stale holder) or exits loudly (fresh holder under a
//! different identity).
//!
//! The authority is an opaque **instance identity** minted at daemon startup
//! ([`new_instance_id`]) and threaded through [`try_acquire`], [`refresh`],
//! and [`release`]. PID is stored and surfaced only as a human-readable
//! diagnostic — it never grants authority. This keeps the lock namespace-safe:
//! another process on this host may hold the same numeric PID (PID namespaces,
//! container restart, PID wraparound) without spoofing the previous daemon,
//! and the daemon's own PID may be locally invisible without breaking liveness.
//!
//! The check-and-acquire is atomic (single `BEGIN IMMEDIATE` transaction) so
//! two daemons starting simultaneously can never both write.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

/// Outcome of a [`try_acquire`] attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum AcquireResult {
    /// Lock acquired: fresh row, takeover of a stale holder, or idempotent
    /// reacquire by the same instance identity.
    Acquired,
    /// Lock held by a fresh daemon under a different instance identity — the
    /// caller must exit. `holder_pid` is the stored PID for diagnostics only
    /// and MUST NOT be treated as authoritative.
    Held { holder_pid: i64, heartbeat_age: i64 },
}

/// Mint a fresh opaque instance identifier for a new daemon lifetime.
///
/// A UUID v4 (128 bits of entropy, hex-encoded without hyphens) is
/// collision-resistant across any realistic number of concurrent daemon
/// lifetimes and round-trips through the TEXT column and JSON status output
/// without escaping surprises. Generation does not open a SQLite connection.
pub fn new_instance_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Atomically check and acquire the daemon lock in a single transaction.
///
/// Decision matrix (evaluated inside `BEGIN IMMEDIATE`):
///
/// | existing row              | outcome                                      |
/// |---------------------------|----------------------------------------------|
/// | none                      | INSERT with (pid, instance_id) → Acquired    |
/// | stale (age > stale_secs)  | UPSERT overwriting pid+instance → Acquired   |
/// | fresh, same instance_id   | UPSERT (idempotent reacquire)  → Acquired    |
/// | fresh, different instance | Held (rejects even if PID matches / invisible) |
/// | fresh, NULL instance_id   | Held (legacy row fails closed until stale)   |
///
/// PID liveness is not consulted: a locally-invisible holder PID (containers,
/// PID namespace) never causes a spurious takeover, and a locally-alive but
/// unrelated PID that happens to equal the stored PID never bypasses the
/// identity check.
pub fn try_acquire(
    conn: &mut Connection,
    pid: i64,
    instance_id: &str,
    now: i64,
    stale_secs: i64,
) -> Result<AcquireResult> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let existing: Option<(i64, i64, Option<String>)> = tx
        .query_row(
            "SELECT pid, heartbeat_at, instance_id FROM daemon_lock WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;

    if let Some((holder_pid, heartbeat_at, holder_instance)) = existing {
        let heartbeat_age = now - heartbeat_at;
        if heartbeat_age <= stale_secs {
            // Fresh holder — instance identity is authoritative.
            let same_instance = matches!(
                holder_instance.as_deref(),
                Some(id) if id == instance_id
            );
            if !same_instance {
                tx.commit()?;
                return Ok(AcquireResult::Held {
                    holder_pid,
                    heartbeat_age,
                });
            }
            // Same instance: allow idempotent reacquire (falls through to UPSERT).
        }
        // else: stale heartbeat — takeover permitted regardless of PID liveness
        // or of whether the legacy row carried a NULL instance_id.
    }

    tx.execute(
        "INSERT INTO daemon_lock(id, pid, heartbeat_at, instance_id)
              VALUES (1, ?1, ?2, ?3)
         ON CONFLICT(id) DO UPDATE SET pid          = excluded.pid,
                                       heartbeat_at = excluded.heartbeat_at,
                                       instance_id  = excluded.instance_id",
        params![pid, now, instance_id],
    )?;
    tx.commit()?;
    Ok(AcquireResult::Acquired)
}

/// Refresh the heartbeat timestamp for the calling instance. Returns the number
/// of rows updated — 0 means the lock was stolen by a different instance and
/// the caller must exit. Guarded by `instance_id` so a stale or superseded
/// process cannot revive its own lease.
pub fn refresh(conn: &Connection, instance_id: &str, now: i64) -> Result<usize> {
    let n = conn.execute(
        "UPDATE daemon_lock SET heartbeat_at = ?1 WHERE id = 1 AND instance_id = ?2",
        params![now, instance_id],
    )?;
    Ok(n)
}

/// Read the current lock row without taking the write lock. Returns `None`
/// when the table is empty (no daemon has ever started).
pub fn peek(conn: &Connection) -> Result<Option<(i64, i64, Option<String>)>> {
    let row = conn
        .query_row(
            "SELECT pid, heartbeat_at, instance_id FROM daemon_lock WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    Ok(row)
}

/// Read `daemon_lock` and compute liveness. Namespace-safe: heartbeat freshness
/// alone decides Alive vs Stale, so a live daemon whose PID happens to be
/// locally invisible still reads as Alive, and an expired heartbeat still reads
/// as Stale even when the stored numeric PID collides with an unrelated live
/// process. `is_pid_alive` populates the `pid_dead` diagnostic on `Stale` only
/// and never influences the verdict. Pure read — does not take the write lock.
pub fn liveness(
    conn: &Connection,
    now: i64,
    stale_secs: i64,
    is_pid_alive: impl Fn(i64) -> bool,
) -> Result<crate::stats::DaemonLiveness> {
    use crate::stats::DaemonLiveness;
    match peek(conn)? {
        None => Ok(DaemonLiveness::None),
        Some((pid, heartbeat_at, _instance_id)) => {
            let age = now - heartbeat_at;
            if age <= stale_secs {
                Ok(DaemonLiveness::Alive {
                    pid,
                    heartbeat_age_secs: age,
                })
            } else {
                Ok(DaemonLiveness::Stale {
                    pid,
                    heartbeat_age_secs: age,
                    pid_dead: !is_pid_alive(pid),
                })
            }
        }
    }
}

/// Release the lock on clean shutdown. No-op unless the row still belongs to
/// the calling `instance_id`, so a superseded process cannot delete the new
/// holder's row.
pub fn release(conn: &Connection, instance_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM daemon_lock WHERE id = 1 AND instance_id = ?1",
        params![instance_id],
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
    const INST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const INST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    /// Insert a legacy pre-v70 row (instance_id NULL) with a specific heartbeat_at
    /// to exercise the fresh-vs-stale legacy paths without invoking `try_acquire`.
    fn insert_legacy_null(conn: &Connection, pid: i64, heartbeat_at: i64) {
        conn.execute(
            "INSERT INTO daemon_lock(id, pid, heartbeat_at, instance_id)
                  VALUES (1, ?1, ?2, NULL)",
            params![pid, heartbeat_at],
        )
        .unwrap();
    }

    #[test]
    fn new_instance_id_is_opaque_and_unique() {
        let a = new_instance_id();
        let b = new_instance_id();
        assert_eq!(a.len(), 32, "16-byte hex encoding is 32 chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b, "two calls yield distinct ids");
    }

    #[test]
    fn acquire_on_empty_db_succeeds() {
        let (_d, mut c) = open_tmp();
        let r = try_acquire(&mut c, 12345, INST_A, 1000, STALE).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
        let (pid, hb, inst): (i64, i64, Option<String>) = c
            .query_row(
                "SELECT pid, heartbeat_at, instance_id FROM daemon_lock WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(pid, 12345);
        assert_eq!(hb, 1000);
        assert_eq!(inst.as_deref(), Some(INST_A));
    }

    #[test]
    fn same_instance_reacquire_is_idempotent() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, INST_A, 1000, STALE).unwrap();
        let r = try_acquire(&mut c, 100, INST_A, 1001, STALE).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
        let hb: i64 = c
            .query_row(
                "SELECT heartbeat_at FROM daemon_lock WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hb, 1001, "reacquire also refreshes the heartbeat");
    }

    #[test]
    fn equal_pid_different_instance_is_held() {
        // Regression: PID must never override instance identity. A namespaced or
        // recycled PID that happens to match the stored one may not steal the lock.
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, INST_A, 1000, STALE).unwrap();
        let r = try_acquire(&mut c, 100, INST_B, 1001, STALE).unwrap();
        assert_eq!(
            r,
            AcquireResult::Held {
                holder_pid: 100,
                heartbeat_age: 1
            }
        );
    }

    #[test]
    fn fresh_different_instance_with_invisible_pid_still_held() {
        // Even when the stored PID is not visible on this host (containers,
        // remote daemon, PID namespace), a fresh heartbeat under a different
        // instance identity must fail closed.
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, INST_A, 1000, STALE).unwrap();
        // Second acquire from a different instance; PID liveness is not consulted.
        let r = try_acquire(&mut c, 200, INST_B, 1001, STALE).unwrap();
        assert_eq!(
            r,
            AcquireResult::Held {
                holder_pid: 100,
                heartbeat_age: 1
            }
        );
    }

    #[test]
    fn stale_heartbeat_allows_takeover_regardless_of_pid_liveness() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, INST_A, 1000, STALE).unwrap();
        // Heartbeat expired — takeover permitted even if the holder PID would
        // still appear alive on this host.
        let r = try_acquire(&mut c, 200, INST_B, 1000 + STALE + 1, STALE).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
        let inst: Option<String> = c
            .query_row(
                "SELECT instance_id FROM daemon_lock WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(inst.as_deref(), Some(INST_B));
    }

    #[test]
    fn exact_stale_boundary_is_still_live() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, INST_A, 1000, STALE).unwrap();
        let r = try_acquire(&mut c, 200, INST_B, 1000 + STALE, STALE).unwrap();
        assert_eq!(
            r,
            AcquireResult::Held {
                holder_pid: 100,
                heartbeat_age: STALE,
            }
        );
    }

    #[test]
    fn fresh_legacy_null_row_fails_closed() {
        // A pre-v70 legacy row still under its stale window carries NULL
        // instance_id. Its identity is unknown, so any incoming id (which
        // cannot equal NULL) must fail closed rather than assume ownership.
        let (_d, mut c) = open_tmp();
        insert_legacy_null(&c, 100, 1000);
        let r = try_acquire(&mut c, 200, INST_B, 1005, STALE).unwrap();
        assert_eq!(
            r,
            AcquireResult::Held {
                holder_pid: 100,
                heartbeat_age: 5
            }
        );
    }

    #[test]
    fn stale_legacy_null_row_recovers_without_manual_edit() {
        // Once past stale_secs the legacy NULL row is treated as abandoned:
        // takeover proceeds via UPSERT and installs the new instance_id.
        let (_d, mut c) = open_tmp();
        insert_legacy_null(&c, 100, 1000);
        let r = try_acquire(&mut c, 200, INST_B, 1000 + STALE + 1, STALE).unwrap();
        assert_eq!(r, AcquireResult::Acquired);
        let inst: Option<String> = c
            .query_row(
                "SELECT instance_id FROM daemon_lock WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(inst.as_deref(), Some(INST_B));
    }

    #[test]
    fn release_only_by_owning_instance() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, INST_A, 1000, STALE).unwrap();
        // A superseded / stale instance cannot delete the current holder's row.
        release(&c, INST_B).unwrap();
        let count: i64 = c
            .query_row("SELECT COUNT(*) FROM daemon_lock", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "wrong-instance release is a no-op");

        release(&c, INST_A).unwrap();
        let count: i64 = c
            .query_row("SELECT COUNT(*) FROM daemon_lock", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn refresh_updates_only_owning_instance() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, INST_A, 1000, STALE).unwrap();
        let n = refresh(&c, INST_A, 5000).unwrap();
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
    fn old_instance_cannot_refresh_or_release_replacement() {
        // The A-instance acquires, then the row expires and B takes over. A must
        // not be able to revive its lease or delete B's row.
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 100, INST_A, 1000, STALE).unwrap();
        try_acquire(&mut c, 200, INST_B, 1000 + STALE + 1, STALE).unwrap();

        let n = refresh(&c, INST_A, 9999).unwrap();
        assert_eq!(n, 0, "old instance cannot refresh replacement's row");
        let hb: i64 = c
            .query_row(
                "SELECT heartbeat_at FROM daemon_lock WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            hb,
            1000 + STALE + 1,
            "heartbeat untouched by wrong instance"
        );

        release(&c, INST_A).unwrap();
        let (pid, inst): (i64, Option<String>) = c
            .query_row(
                "SELECT pid, instance_id FROM daemon_lock WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(pid, 200);
        assert_eq!(inst.as_deref(), Some(INST_B));
    }

    #[test]
    fn peek_empty_returns_none() {
        let (_d, c) = open_tmp();
        assert_eq!(peek(&c).unwrap(), None);
    }

    #[test]
    fn peek_returns_row() {
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 42, INST_A, 9000, STALE).unwrap();
        assert_eq!(
            peek(&c).unwrap(),
            Some((42, 9000, Some(INST_A.to_string())))
        );
    }

    #[test]
    fn liveness_empty_is_none() {
        use crate::stats::DaemonLiveness;
        let (_d, c) = open_tmp();
        let l = liveness(&c, 1000, STALE, |_| true).unwrap();
        assert_eq!(l, DaemonLiveness::None);
    }

    #[test]
    fn liveness_fresh_heartbeat_reports_alive_even_when_pid_invisible() {
        // Namespace-safe: a stored PID that isn't visible on this host is not
        // authoritative — a fresh heartbeat still means Alive.
        use crate::stats::DaemonLiveness;
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 424242, INST_A, 1000, STALE).unwrap();
        let l = liveness(&c, 1004, STALE, |_| false).unwrap();
        assert_eq!(
            l,
            DaemonLiveness::Alive {
                pid: 424242,
                heartbeat_age_secs: 4
            }
        );
    }

    #[test]
    fn liveness_expired_heartbeat_reports_stale_even_when_pid_alive() {
        // Namespace-safe: a stale heartbeat is Stale even if the numeric PID
        // matches an unrelated live local process. pid_dead is diagnostic only.
        use crate::stats::DaemonLiveness;
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 42, INST_A, 1000, STALE).unwrap();
        let l = liveness(&c, 1000 + STALE + 1, STALE, |_| true).unwrap();
        assert_eq!(
            l,
            DaemonLiveness::Stale {
                pid: 42,
                heartbeat_age_secs: STALE + 1,
                pid_dead: false,
            }
        );
    }

    #[test]
    fn liveness_stale_and_pid_dead_reports_both() {
        use crate::stats::DaemonLiveness;
        let (_d, mut c) = open_tmp();
        try_acquire(&mut c, 42, INST_A, 1000, STALE).unwrap();
        let l = liveness(&c, 1000 + STALE + 1, STALE, |_| false).unwrap();
        assert_eq!(
            l,
            DaemonLiveness::Stale {
                pid: 42,
                heartbeat_age_secs: STALE + 1,
                pid_dead: true,
            }
        );
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
                        let instance = new_instance_id();
                        barrier.wait();
                        let pid = 1000 + i as i64;
                        try_acquire(&mut conn, pid, &instance, 5000, STALE)
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
}
