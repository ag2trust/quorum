//! Database connection setup: mandatory PRAGMAs + schema migration-on-open.
//!
//! Every connection applies the same PRAGMAs (they are per-connection in SQLite) and runs
//! [`migrate`] before use, so any short-lived `quorum` process self-heals the schema.

use crate::error::{QuorumError, Result};
use rusqlite::{Connection, Error as SqlErr, ErrorCode, Transaction, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

/// Schema version this binary understands. Bump when adding a migration.
pub const SCHEMA_VERSION: i64 = 10;

/// SQLite per-connection busy timeout: how long the engine sleeps on a held lock before
/// returning `SQLITE_BUSY`. 5s comfortably absorbs the BUSY window of any single in-process
/// write while still keeping pathological deadlocks from hanging the CLI indefinitely.
/// Load-bearing invariant: tests in [`tests::pragmas_are_set`] pin the deployed value.
pub const BUSY_TIMEOUT_MS: u32 = 5000;

/// Bounded retry budget for the first-open WAL-mode switch (see [`set_journal_wal`]). The
/// engine's busy-timeout handler doesn't cover journal-mode changes, so we re-try in
/// userspace. 100 × 20ms ≈ 2s — enough headroom for concurrent first-opens without making a
/// pathological lock hold a single command indefinitely.
const WAL_RETRY_MAX: usize = 100;
const WAL_RETRY_SLEEP: Duration = Duration::from_millis(20);

/// The full schema. Every statement is idempotent (`IF NOT EXISTS`).
const SCHEMA_SQL: &str = include_str!("schema.sql");

/// Apply the mandatory per-connection PRAGMAs (see design spec §Concurrency & atomicity).
pub fn apply_pragmas(conn: &Connection) -> Result<()> {
    // busy_timeout MUST be first so every subsequent lock acquisition honors it.
    conn.pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MS)?;
    set_journal_wal(conn)?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}

/// Begin a `BEGIN IMMEDIATE` transaction with BUSY-aware error mapping.
///
/// `BEGIN IMMEDIATE` takes the database's write lock up-front, so racing writers serialize on
/// the lock instead of discovering the contention at commit time. A post-timeout SQLITE_BUSY
/// here maps to [`QuorumError::Busy`] (clean exit 3, stable detail) rather than a raw
/// `Db(rusqlite::Error)` — every write path in the engine exits with the same string for the
/// same condition.
pub fn begin_immediate(conn: &mut Connection) -> Result<Transaction<'_>> {
    conn.transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(map_sql_err)
}

/// Map a raw SQLite error: a post-timeout BUSY becomes [`QuorumError::Busy`] (exit 3 with
/// stable detail at the CLI boundary); anything else stays a generic DB error.
pub(crate) fn map_sql_err(e: rusqlite::Error) -> QuorumError {
    if let rusqlite::Error::SqliteFailure(f, _) = &e {
        if f.code == ErrorCode::DatabaseBusy {
            return QuorumError::Busy;
        }
    }
    QuorumError::Db(e)
}

/// Switch the database to WAL mode, retrying briefly on transient lock contention.
///
/// Switching journal mode requires that no other connection is mid-switch, and the SQLite
/// busy-timeout handler does NOT cover journal-mode changes — so under concurrent
/// first-creation the switch can return `SQLITE_BUSY`/`SQLITE_LOCKED` even with the timeout
/// set. WAL is persistent on the file, so this race exists only on the very first switch;
/// a bounded retry resolves it. Subsequent opens see WAL already set (a no-op, no lock).
fn set_journal_wal(conn: &Connection) -> Result<()> {
    set_journal_wal_with(conn, WAL_RETRY_MAX, WAL_RETRY_SLEEP)
}

/// Inner: parameterized retry. Production uses (100, 20ms) → ~2s budget. Tests use tiny
/// values to deterministically hit the `Err(Busy)` exhaustion branch in <50ms without
/// changing the runtime semantics — extracting the constants is the smallest refactor that
/// makes the exhaustion path reachable from a unit test.
fn set_journal_wal_with(
    conn: &Connection,
    max_retries: usize,
    sleep: std::time::Duration,
) -> Result<()> {
    for _ in 0..max_retries {
        match conn.query_row("PRAGMA journal_mode=WAL", [], |r| r.get::<_, String>(0)) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => return Ok(()),
            Ok(_) => {} // not yet WAL (another switch in flight) — retry
            Err(SqlErr::SqliteFailure(e, _))
                if matches!(e.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {}
            Err(e) => return Err(e.into()),
        }
        std::thread::sleep(sleep);
    }
    Err(QuorumError::Busy)
}

/// Migration outcome: what version the DB was at before this binary opened it, and what
/// version it is now. Returned by [`migrate`] so callers (e.g. `quorum init`) can report
/// whether a retrofit happened.
pub struct MigrateResult {
    pub migrated_from: i64,
    pub schema_version: i64,
}

/// Open the store at `path`, applying PRAGMAs and running migrations. The returned
/// connection is ready for use.
pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    apply_pragmas(&conn)?;
    migrate(&conn)?;
    Ok(conn)
}

/// Like [`open`], but also returns the migration outcome so callers can report what changed.
pub fn open_init(path: &Path) -> Result<(Connection, MigrateResult)> {
    let conn = Connection::open(path)?;
    apply_pragmas(&conn)?;
    let info = migrate(&conn)?;
    Ok((conn, info))
}

/// Bring the on-disk schema up to [`SCHEMA_VERSION`].
///
/// Forward-only and idempotent. Runs under `BEGIN IMMEDIATE` so concurrent first-runs are
/// safe. Refuses (fails loud) if the DB was written by a newer binary.
pub fn migrate(conn: &Connection) -> Result<MigrateResult> {
    let current: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if current > SCHEMA_VERSION {
        return Err(QuorumError::SchemaTooNew {
            db: current,
            bin: SCHEMA_VERSION,
        });
    }
    if current == SCHEMA_VERSION {
        return Ok(MigrateResult {
            migrated_from: current,
            schema_version: SCHEMA_VERSION,
        });
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
        // v3 = events table addition (new CREATE TABLE in SCHEMA_SQL — no ALTER needed).
        if current < 4 && !column_exists(conn, "tasks", "depends_on")? {
            // Issue #2: JSON array of task ids; NULL = no deps. Filled via task-create
            // --depends-on (validated at the boundary). The claim auto-pick and explicit
            // --task-id both gate on every dep being `closed`.
            conn.execute("ALTER TABLE tasks ADD COLUMN depends_on TEXT", [])?;
        }
        // v5 = control table addition (issue #6 emergency stop). New CREATE TABLE in
        // SCHEMA_SQL handles it on fresh DBs and upgrades alike — no ALTER needed.
        // v6 = review-as-task columns (issue #10): tasks.sticky_until + tasks.orig.
        if current < 6 && !column_exists(conn, "tasks", "sticky_until")? {
            conn.execute("ALTER TABLE tasks ADD COLUMN sticky_until INTEGER", [])?;
        }
        if current < 6 && !column_exists(conn, "tasks", "orig")? {
            conn.execute("ALTER TABLE tasks ADD COLUMN orig TEXT", [])?;
        }
        // v8 = persist agent tier (issue #82).
        if current < 8 && !column_exists(conn, "agents", "tier")? {
            conn.execute("ALTER TABLE agents ADD COLUMN tier TEXT", [])?;
        }
        // v9 = per-(task, project) branch allocations (issue #98). Net-new table — the
        // CREATE TABLE IF NOT EXISTS in SCHEMA_SQL handles fresh DBs and upgrades alike;
        // no ALTER needed.
        // v10 = agent-retirement state machine (issue #97). Two additive columns on
        // `agents`; both safe to apply to a populated table (pre-existing rows default to
        // `'active'` / NULL). Forward-only — once a row reaches `'retired'` it stays there.
        if current < 10 && !column_exists(conn, "agents", "retire_status")? {
            conn.execute(
                "ALTER TABLE agents ADD COLUMN retire_status TEXT NOT NULL DEFAULT 'active'",
                [],
            )?;
        }
        if current < 10 && !column_exists(conn, "agents", "retired_at")? {
            conn.execute("ALTER TABLE agents ADD COLUMN retired_at INTEGER", [])?;
        }
        conn.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))?;
        Ok(())
    };
    match run() {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(MigrateResult {
                migrated_from: current,
                schema_version: SCHEMA_VERSION,
            })
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
        assert_eq!(bt, i64::from(BUSY_TIMEOUT_MS));
    }

    #[test]
    fn migrate_creates_tables_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let _ = open(&p).unwrap();
        }
        let c = open(&p).unwrap(); // second open must not error
        for t in [
            "agents",
            "messages",
            "cursors",
            "claims",
            "tasks",
            "errors",
            "events",
            "task_notes",
            "task_branches",
        ] {
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
    fn migrates_v2_to_v3_adds_events_table() {
        // Simulate a v2 DB (pre-events) with the v2 messages shape, then re-open and verify
        // the new `events` table is created without losing any existing rows.
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
                    expires_at INTEGER NOT NULL,
                    recipient TEXT
                 );
                 INSERT INTO messages(ts, author, topic, kind, body, refs, expires_at)
                 VALUES (1, 'A', 'hub', 'info', 'pre-events', NULL, 9999);
                 PRAGMA user_version = 2;
                 COMMIT;",
            )
            .unwrap();
        }
        let c = open(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        // events table exists and is empty
        let n: i64 = c
            .query_row("SELECT count(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        // existing message still there
        let body: String = c
            .query_row("SELECT body FROM messages WHERE seq=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(body, "pre-events");
    }

    #[test]
    fn migration_is_idempotent_when_already_at_latest() {
        // Calling open() twice must not re-run ALTER (which would fail on duplicate column).
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        let _ = open(&p).unwrap();
        let c = open(&p).unwrap();
        assert!(column_exists(&c, "messages", "recipient").unwrap());
        assert!(column_exists(&c, "tasks", "depends_on").unwrap());
        // v6 review-as-task columns are added by ALTER on upgrades and by SCHEMA_SQL on fresh.
        assert!(column_exists(&c, "tasks", "sticky_until").unwrap());
        assert!(column_exists(&c, "tasks", "orig").unwrap());
        // v5 control table is created via SCHEMA_SQL on every open — verify it exists.
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='control'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "control table missing");
    }

    #[test]
    fn migrates_v5_to_v6_adds_review_columns_without_disturbing_existing_rows() {
        // Simulate a v5 DB (control table present, sticky_until + orig absent, user_version=5)
        // with seeded rows; re-open and verify the new columns land NULL without losing data.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let c = Connection::open(&p).unwrap();
            apply_pragmas(&c).unwrap();
            // v5 tasks shape: depends_on present, sticky_until/orig absent.
            c.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0, labels TEXT, assignee TEXT,
                    created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL, refs TEXT, depends_on TEXT
                 );
                 INSERT INTO tasks(title, status, priority, created_by, created_at, updated_at)
                 VALUES ('pre-review-cols', 'open', 5, 'boss', 1, 1);
                 PRAGMA user_version = 5;
                 COMMIT;",
            )
            .unwrap();
        }
        let c = open(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        assert!(column_exists(&c, "tasks", "sticky_until").unwrap());
        assert!(column_exists(&c, "tasks", "orig").unwrap());
        // Pre-existing row: new columns are NULL, original data preserved.
        let (title, priority, sticky, orig): (String, i64, Option<i64>, Option<String>) = c
            .query_row(
                "SELECT title, priority, sticky_until, orig FROM tasks WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(title, "pre-review-cols");
        assert_eq!(priority, 5);
        assert!(sticky.is_none());
        assert!(orig.is_none());
    }

    #[test]
    fn migrates_v4_to_v5_adds_control_table_without_disturbing_existing_rows() {
        // Simulate a v4 DB (depends_on present, no control table, user_version=4) with a
        // seeded task + message; re-open and verify the control table is created and the
        // existing rows are preserved.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let c = Connection::open(&p).unwrap();
            apply_pragmas(&c).unwrap();
            c.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL, author TEXT NOT NULL, topic TEXT NOT NULL,
                    kind TEXT NOT NULL, body TEXT NOT NULL, refs TEXT,
                    expires_at INTEGER NOT NULL, recipient TEXT
                 );
                 CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0, labels TEXT, assignee TEXT,
                    created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL, refs TEXT, depends_on TEXT
                 );
                 INSERT INTO messages(ts, author, topic, kind, body, expires_at)
                 VALUES (1, 'A', 'hub', 'info', 'pre-control', 9999);
                 INSERT INTO tasks(title, status, priority, created_by, created_at, updated_at)
                 VALUES ('pre-control-task', 'open', 0, 'boss', 1, 1);
                 PRAGMA user_version = 4;
                 COMMIT;",
            )
            .unwrap();
        }
        let c = open(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        // control table now exists and starts empty.
        let n: i64 = c
            .query_row("SELECT count(*) FROM control", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        // Pre-existing rows untouched.
        let body: String = c
            .query_row("SELECT body FROM messages WHERE seq=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(body, "pre-control");
        let title: String = c
            .query_row("SELECT title FROM tasks WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(title, "pre-control-task");
    }

    #[test]
    fn migrates_v3_to_v4_adds_depends_on_column() {
        // Simulate a v3 DB (events table present, tasks.depends_on absent, user_version=3)
        // with a pre-existing task row; re-open and verify the column lands without losing
        // the row.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let c = Connection::open(&p).unwrap();
            apply_pragmas(&c).unwrap();
            // v3 shape: tasks WITHOUT depends_on. Re-CREATE at the v3 shape + seed.
            c.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL, body TEXT,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    labels TEXT, assignee TEXT,
                    created_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    refs TEXT
                 );
                 INSERT INTO tasks(title, status, priority, created_by, created_at, updated_at)
                 VALUES ('pre-existing', 'open', 0, 'boss', 1, 1);
                 PRAGMA user_version = 3;
                 COMMIT;",
            )
            .unwrap();
        }
        let c = open(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        assert!(column_exists(&c, "tasks", "depends_on").unwrap());
        // depends_on is NULL for the pre-existing row (treated as no-deps → ready).
        let (title, deps): (String, Option<String>) = c
            .query_row("SELECT title, depends_on FROM tasks WHERE id=1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(title, "pre-existing");
        assert!(deps.is_none());
    }

    #[test]
    fn migrates_v4_to_v5_adds_task_notes_table() {
        // Simulate a v4 DB (events + depends_on present, but task_notes absent,
        // user_version=4) with a pre-existing task; re-open and verify the new table is
        // created without losing data.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let c = Connection::open(&p).unwrap();
            apply_pragmas(&c).unwrap();
            c.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL, body TEXT,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    labels TEXT, assignee TEXT,
                    created_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    refs TEXT,
                    depends_on TEXT
                 );
                 INSERT INTO tasks(title, status, priority, created_by, created_at, updated_at)
                 VALUES ('pre-notes', 'open', 0, 'boss', 1, 1);
                 PRAGMA user_version = 4;
                 COMMIT;",
            )
            .unwrap();
        }
        let c = open(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        // task_notes table now exists and is empty.
        let n: i64 = c
            .query_row("SELECT count(*) FROM task_notes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        // existing task still there
        let title: String = c
            .query_row("SELECT title FROM tasks WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(title, "pre-notes");
    }

    #[test]
    fn migrates_v7_to_v8_adds_agents_tier_column_without_disturbing_existing_rows() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let c = Connection::open(&p).unwrap();
            apply_pragmas(&c).unwrap();
            c.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE agents (
                    id TEXT PRIMARY KEY,
                    first_seen INTEGER NOT NULL,
                    last_seen INTEGER NOT NULL
                 );
                 INSERT INTO agents(id, first_seen, last_seen) VALUES ('pre-tier', 100, 200);
                 PRAGMA user_version = 7;
                 COMMIT;",
            )
            .unwrap();
        }
        let c = open(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        assert!(column_exists(&c, "agents", "tier").unwrap());
        let (id, tier): (String, Option<String>) = c
            .query_row("SELECT id, tier FROM agents WHERE id='pre-tier'", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(id, "pre-tier");
        assert!(tier.is_none(), "tier must default NULL for existing rows");
    }

    #[test]
    fn migrates_v8_to_v9_adds_task_branches_table_without_disturbing_existing_rows() {
        // Issue #98: v9 is a net-new `task_branches` table. The migration is satisfied
        // entirely by SCHEMA_SQL's `CREATE TABLE IF NOT EXISTS` running on every open; no
        // ALTER is needed. Verify (a) the table exists post-open, (b) PRAGMA user_version
        // is bumped to SCHEMA_VERSION, and (c) a pre-existing v8 `tasks` row is untouched.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");
        {
            let c = Connection::open(&p).unwrap();
            apply_pragmas(&c).unwrap();
            // Minimal v8 shape: just enough for a tasks row to round-trip.
            c.execute_batch(
                "BEGIN IMMEDIATE;
                 CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    body TEXT,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    labels TEXT,
                    assignee TEXT,
                    created_by TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    refs TEXT,
                    depends_on TEXT,
                    sticky_until INTEGER,
                    orig TEXT
                 );
                 INSERT INTO tasks(title, status, created_by, created_at, updated_at)
                 VALUES ('pre-v9', 'open', 'boss', 100, 100);
                 PRAGMA user_version = 8;
                 COMMIT;",
            )
            .unwrap();
        }
        let c = open(&p).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        // (a) task_branches table now exists.
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='task_branches'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "task_branches table must exist after v8 → v9 migration"
        );
        // (b) UNIQUE indices present (the load-bearing invariants of #98).
        let idx_count: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='task_branches'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            idx_count >= 2,
            "expected ≥2 indices on task_branches (UNIQUE(task_id,repo), UNIQUE(repo,branch)); got {idx_count}"
        );
        // (c) Pre-existing tasks row untouched.
        let (title, status): (String, String) = c
            .query_row("SELECT title, status FROM tasks WHERE id=1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(title, "pre-v9");
        assert_eq!(status, "open");
    }

    #[test]
    fn set_journal_wal_returns_busy_when_lock_held() {
        // Exercises the previously-untested exhaustion branch (`db.rs::set_journal_wal_with`
        // returning `Err(QuorumError::Busy)`). A held EXCLUSIVE transaction on a second
        // connection blocks the WAL switch on the first, so the bounded retry loop drains
        // and returns `Err(Busy)` — the contract the production set_journal_wal relies on
        // when the 100×20ms budget is exceeded under genuinely-pathological contention.
        //
        // Uses a 3×1ms retry budget so the test runs in <50ms. Production semantics are
        // unchanged — set_journal_wal still calls set_journal_wal_with(100, 20ms).
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q.db");

        // Conn A: hold an EXCLUSIVE transaction. busy_timeout=0 so the BEGIN is immediate.
        let conn_a = Connection::open(&p).unwrap();
        conn_a.pragma_update(None, "busy_timeout", 0).unwrap();
        conn_a.execute_batch("BEGIN EXCLUSIVE").unwrap();

        // Conn B: tries to switch to WAL — can't acquire the exclusive lock A holds, every
        // retry sees BUSY/LOCKED, exhausts the budget, returns Err(Busy).
        let conn_b = Connection::open(&p).unwrap();
        conn_b.pragma_update(None, "busy_timeout", 0).unwrap();
        let result = set_journal_wal_with(&conn_b, 3, Duration::from_millis(1));

        // Cleanup: release A's lock so tempdir drops cleanly.
        let _ = conn_a.execute_batch("COMMIT");

        match result {
            Err(QuorumError::Busy) => {} // expected — the contract this test pins
            other => panic!("expected Err(QuorumError::Busy), got {other:?}"),
        }
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

    /// Issue #97: simulate a DB at v9 (current main's shape) and verify that the v10
    /// migration applies — adding both `agents.retire_status` and `agents.retired_at`,
    /// and bumping `user_version` to 10. This pins the fix for Gravel-m38's Critical
    /// review finding: an earlier draft of this PR declared v9 alongside main's #98
    /// branch-allocations v9, so the `if current < 9` guard never fired on existing
    /// databases and the retirement columns were silently skipped.
    #[test]
    fn migrates_v9_to_v10_adds_retire_columns() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        // Hand-craft a v9 database: the v9 `agents` table shape (no retire columns yet)
        // plus the branch-allocations table from PR #98, then stamp user_version=9.
        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE agents (
                 id         TEXT PRIMARY KEY,
                 first_seen INTEGER NOT NULL,
                 last_seen  INTEGER NOT NULL,
                 tier       TEXT
             );
             CREATE TABLE branch_allocations (
                 task_id INTEGER NOT NULL,
                 project TEXT NOT NULL,
                 branch  TEXT NOT NULL,
                 PRIMARY KEY (task_id, project)
             );
             INSERT INTO agents(id, first_seen, last_seen, tier)
                 VALUES ('Veteran', 100, 100, 'tier:opus-46');
             PRAGMA user_version = 9;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        // Now open via the production path — migrate() must lift v9 → v10 and ALTER
        // the agents table to add retire_status + retired_at.
        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 10, "user_version must advance to 10");
        assert!(
            column_exists(&c, "agents", "retire_status").unwrap(),
            "retire_status column missing — v9→v10 migration silently skipped"
        );
        assert!(
            column_exists(&c, "agents", "retired_at").unwrap(),
            "retired_at column missing — v9→v10 migration silently skipped"
        );

        // The pre-existing row must default to active / NULL.
        let (status, retired_at): (String, Option<i64>) = c
            .query_row(
                "SELECT retire_status, retired_at FROM agents WHERE id='Veteran'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "active");
        assert!(retired_at.is_none());
    }
}
