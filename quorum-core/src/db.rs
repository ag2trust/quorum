//! Database connection setup: mandatory PRAGMAs + schema migration-on-open.
//!
//! Every connection applies the same PRAGMAs (they are per-connection in SQLite) and runs
//! [`migrate`] before use, so any short-lived `quorum` process self-heals the schema.

use crate::error::{QuorumError, Result};
use rusqlite::Connection;
use std::path::Path;

/// Schema version this binary understands. Bump when adding a migration.
pub const SCHEMA_VERSION: i64 = 1;

/// The full schema. Every statement is idempotent (`IF NOT EXISTS`).
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Apply the mandatory per-connection PRAGMAs (see design spec §Concurrency & atomicity).
pub fn apply_pragmas(conn: &Connection) -> Result<()> {
    // journal_mode=WAL returns the applied mode as a row, so it can't go through the
    // no-result `pragma_update` path.
    conn.pragma_update_and_check(None, "journal_mode", "WAL", |_row| Ok(()))?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    Ok(())
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
        let bt: i64 = c.query_row("PRAGMA busy_timeout", [], |r| r.get(0)).unwrap();
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
        let v: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
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
