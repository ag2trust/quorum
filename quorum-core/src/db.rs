//! Database connection setup: mandatory PRAGMAs + schema migration-on-open.
//!
//! Every connection applies the same PRAGMAs (they are per-connection in SQLite) and runs
//! [`migrate`] before use, so any short-lived `quorum` process self-heals the schema.

use crate::error::{QuorumError, Result};
use rusqlite::{Connection, Error as SqlErr, ErrorCode};
use std::path::Path;

/// Schema version this binary understands. Bump when adding a migration.
pub const SCHEMA_VERSION: i64 = 2;

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
    if current == SCHEMA_VERSION {
        return Ok(());
    }
    // One atomic migration. SCHEMA_SQL is `CREATE TABLE IF NOT EXISTS`, so it builds a fresh
    // DB at the latest shape and is a no-op for existing tables — additive column changes
    // must therefore be ALTERed in below, guarded for idempotency since SQLite has no
    // `ADD COLUMN IF NOT EXISTS`. SCHEMA_VERSION is a compile-time constant (injection-free).
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let run = || -> Result<()> {
        conn.execute_batch(SCHEMA_SQL)?;
        if current < 2 && !column_exists(conn, "messages", "recipient")? {
            conn.execute("ALTER TABLE messages ADD COLUMN recipient TEXT", [])?;
        }
        conn.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
        Ok(())
    };
    match run() {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn column_exists(conn: &Connection, table: &str, col: &str) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2")?;
    Ok(stmt.exists(rusqlite::params![table, col])?)
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
    fn migrates_v1_messages_to_v2_recipient_column() {
        // Simulate a pre-existing v1 DB (no `recipient` column, user_version=1) with one
        // row, then re-open with the current binary and verify the column is added without
        // losing the row.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let c = Connection::open(&p).unwrap();
            apply_pragmas(&c).unwrap();
            c.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    author TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    body TEXT NOT NULL,
                    refs TEXT,
                    expires_at INTEGER NOT NULL
                 );
                 INSERT INTO messages(ts, author, topic, kind, body, refs, expires_at)
                 VALUES (1, 'A', 'hub', 'info', 'pre-migration', NULL, 9999);
                 PRAGMA user_version = 1;
                 COMMIT;",
            )
            .unwrap();
        }
        let c = open(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        assert!(column_exists(&c, "messages", "recipient").unwrap());
        // recipient is NULL for the pre-existing row (treated as a broadcast).
        let (body, recipient): (String, Option<String>) = c
            .query_row(
                "SELECT body, recipient FROM messages WHERE seq=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(body, "pre-migration");
        assert!(recipient.is_none());
    }

    #[test]
    fn migration_is_idempotent_when_already_at_latest() {
        // Calling open() twice must not re-run ALTER (which would fail on duplicate column).
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        let _ = open(&p).unwrap();
        let c = open(&p).unwrap();
        assert!(column_exists(&c, "messages", "recipient").unwrap());
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
