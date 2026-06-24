//! Database connection setup: mandatory PRAGMAs + schema migration-on-open.
//!
//! Every connection applies the same PRAGMAs (they are per-connection in SQLite) and runs
//! [`migrate`] before use, so any short-lived `quorum` process self-heals the schema.

use crate::error::{QuorumError, Result};
use rusqlite::{Connection, Error as SqlErr, ErrorCode};
use std::path::Path;

/// Schema version this binary understands. Bump when adding a migration.
pub const SCHEMA_VERSION: i64 = 1;

/// The full schema. Every statement is idempotent (`IF NOT EXISTS`).
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Apply the mandatory per-connection PRAGMAs (see design spec §Concurrency & atomicity).
pub fn apply_pragmas(conn: &Connection) -> Result<()> {
    // busy_timeout MUST be first so every subsequent lock acquisition honors it.
    conn.pragma_update(None, "busy_timeout", 5000)?;
    set_journal_wal(conn)?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}

/// Switch the database to WAL mode, retrying briefly on transient lock contention.
///
/// Switching journal mode requires that no other connection is mid-switch, and the SQLite
/// busy-timeout handler does NOT cover journal-mode changes — so under concurrent
/// first-creation the switch can return `SQLITE_BUSY`/`SQLITE_LOCKED` even with the timeout
/// set. WAL is persistent on the file, so this race exists only on the very first switch;
/// a bounded retry resolves it. Subsequent opens see WAL already set (a no-op, no lock).
fn set_journal_wal(conn: &Connection) -> Result<()> {
    for _ in 0..100 {
        match conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get::<_, String>(0)) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => return Ok(()),
            Ok(_) => {} // not yet WAL (another switch in flight) — retry
            Err(SqlErr::SqliteFailure(e, _))
                if matches!(e.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {}
            Err(e) => return Err(e.into()),
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    Err(QuorumError::Busy)
}

/// Open the store at `path`, applying PRAGMAs and running migrations. The returned
/// connection is ready for use.
pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    apply_pragmas(&conn)?;
    migrate(&conn)?;
    Ok(conn)
}

/// Bring the on-disk schema up to [`SCHEMA_VERSION`].
///
/// Forward-only and idempotent. Runs under `BEGIN IMMEDIATE` so concurrent first-runs are
/// safe. Refuses (fails loud) if the DB was written by a newer binary.
pub fn migrate(conn: &Connection) -> Result<()> {
    let current: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if current > SCHEMA_VERSION {
        return Err(QuorumError::SchemaTooNew {
            db: current,
            bin: SCHEMA_VERSION,
        });
    }
    if current < SCHEMA_VERSION {
        // BEGIN IMMEDIATE + idempotent schema + bump user_version, atomically. SCHEMA_VERSION
        // is a compile-time constant, so the format! is injection-free.
        conn.execute_batch(&format!(
            "BEGIN IMMEDIATE;\n{SCHEMA_SQL}\nPRAGMA user_version = {SCHEMA_VERSION};\nCOMMIT;"
        ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pragmas_are_set() {
        let dir = tempfile::tempdir().unwrap();
        let c = open(&dir.path().join("q.db")).unwrap();
        let jm: String = c
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(jm.to_lowercase(), "wal");
        let bt: i64 = c
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        assert_eq!(bt, 5000);
    }

    #[test]
    fn migrate_creates_tables_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let _ = open(&p).unwrap();
        }
        let c = open(&p).unwrap(); // second open must not error
        for t in ["agents", "messages", "cursors", "claims", "tasks", "errors"] {
            let n: i64 = c
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [t],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "table {t} missing");
        }
        // partial unique index exists
        let idx: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='claims_one_active'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(idx, 1);
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
    }

    #[test]
    fn refuses_newer_db() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let c = open(&p).unwrap();
            c.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
                .unwrap();
        }
        match open(&p) {
            Err(QuorumError::SchemaTooNew { db, bin }) => {
                assert_eq!(db, SCHEMA_VERSION + 1);
                assert_eq!(bin, SCHEMA_VERSION);
            }
            other => panic!("expected SchemaTooNew, got {other:?}"),
        }
    }
}
