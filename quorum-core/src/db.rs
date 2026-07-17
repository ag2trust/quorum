//! Database connection setup: mandatory PRAGMAs + schema migration-on-open.
//!
//! Every connection applies the same PRAGMAs (they are per-connection in SQLite) and runs
//! [`migrate`] before use, so any short-lived `quorum` process self-heals the schema.

use crate::error::{QuorumError, Result};
use rusqlite::{Connection, Error as SqlErr, ErrorCode, Transaction, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

/// Schema version this binary understands. Bump when adding a migration.
pub const SCHEMA_VERSION: i64 = 25;

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
        // v11 = optional PostToolUse activity-hook stats (issue #101). Two net-new
        // tables (`agent_sessions`, `activity_events`) — the `CREATE TABLE IF NOT
        // EXISTS` in SCHEMA_SQL handles fresh DBs and upgrades alike; no ALTER
        // needed. EXPERIMENTAL / opt-in / stats-only — no existing query reads
        // these tables, so absence (or hook never installed) changes nothing.
        // v13 = M5 messaging + agent state: `journal.agent_state` column for daemon-
        // tracked agent reactions (blocked/failed/needs-info/note).
        if current < 13 && !column_exists(conn, "journal", "agent_state")? {
            conn.execute("ALTER TABLE journal ADD COLUMN agent_state TEXT", [])?;
        }
        // v14 = M6 logging: journal.cost_usd + journal.log_dir for live status display.
        if current < 14 && !column_exists(conn, "journal", "cost_usd")? {
            conn.execute(
                "ALTER TABLE journal ADD COLUMN cost_usd REAL NOT NULL DEFAULT 0.0",
                [],
            )?;
        }
        if current < 14 && !column_exists(conn, "journal", "log_dir")? {
            conn.execute("ALTER TABLE journal ADD COLUMN log_dir TEXT", [])?;
        }
        // v15 = M7 crash recovery: journal.pid, journal.pr, journal.rework_count
        // for process-group cleanup, PR tracking, and rework-limit enforcement
        // across daemon restarts.
        if current < 15 && !column_exists(conn, "journal", "pid")? {
            conn.execute("ALTER TABLE journal ADD COLUMN pid INTEGER", [])?;
        }
        if current < 15 && !column_exists(conn, "journal", "pr")? {
            conn.execute("ALTER TABLE journal ADD COLUMN pr INTEGER", [])?;
        }
        if current < 15 && !column_exists(conn, "journal", "rework_count")? {
            conn.execute(
                "ALTER TABLE journal ADD COLUMN rework_count INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        // v16 = #190 instance-scoped recovery: journal.instance_id so a daemon
        // restart never kills/reclaims a sibling instance's in-flight workers.
        // Nullable — pre-v16 rows have NULL and recovery falls back to worktree-prefix
        // matching. Index accelerates the per-instance list_in_flight query. The
        // index is created here (not in SCHEMA_SQL) because SCHEMA_SQL runs BEFORE
        // this ALTER — an index on a not-yet-present column would fail there.
        if current < 16 {
            if !column_exists(conn, "journal", "instance_id")? {
                conn.execute("ALTER TABLE journal ADD COLUMN instance_id TEXT", [])?;
            }
            conn.execute(
                "CREATE INDEX IF NOT EXISTS journal_instance ON journal(instance_id)",
                [],
            )?;
        }
        // v17 = #228 durable, instance-independent approval record: the
        // `approvals` table lets a self-update-drain restart reconstruct
        // "merge this approved PR" from persisted state instead of stranding it.
        // Net-new table + index — the `CREATE TABLE IF NOT EXISTS` /
        // `CREATE INDEX IF NOT EXISTS` in SCHEMA_SQL (which runs above) handles
        // fresh DBs and upgrades alike, so no explicit ALTER is needed here.

        // v18 = per-repo DB refactor (2/3): strip repo columns from
        // task_branches and instance_id from journal. With one DB per repo,
        // these columns are dead. Recreate task_branches without `repo`;
        // recreate journal without `instance_id`.
        if current < 18 {
            // task_branches: drop `repo`, UNIQUE(task_id,repo) → UNIQUE(task_id),
            // UNIQUE(repo,branch) → UNIQUE(branch).
            if column_exists(conn, "task_branches", "repo")? {
                conn.execute_batch(
                    "CREATE TABLE task_branches_new (
                         id           INTEGER PRIMARY KEY AUTOINCREMENT,
                         task_id      INTEGER NOT NULL UNIQUE,
                         branch       TEXT NOT NULL UNIQUE,
                         worktree     TEXT NOT NULL,
                         allocated_by TEXT NOT NULL,
                         allocated_at INTEGER NOT NULL
                     );
                     INSERT INTO task_branches_new(id, task_id, branch, worktree, allocated_by, allocated_at)
                         SELECT id, task_id, branch, worktree, allocated_by, allocated_at
                         FROM task_branches;
                     DROP TABLE task_branches;
                     ALTER TABLE task_branches_new RENAME TO task_branches;
                     CREATE INDEX IF NOT EXISTS task_branches_task ON task_branches(task_id);",
                )?;
            }
            // journal: drop `instance_id` + its index.
            if column_exists(conn, "journal", "instance_id")? {
                conn.execute("DROP INDEX IF EXISTS journal_instance", [])?;
                // Build the SELECT dynamically: `expected_signal` was never
                // added via ALTER (only in SCHEMA_SQL) so pre-v17 upgrades
                // may lack it. Use NULL in that case.
                let has_expected_signal = column_exists(conn, "journal", "expected_signal")?;
                let es_expr = if has_expected_signal {
                    "expected_signal"
                } else {
                    "NULL"
                };
                conn.execute_batch(&format!(
                    "CREATE TABLE journal_new (
                         agent           TEXT PRIMARY KEY,
                         role            TEXT NOT NULL,
                         task_id         INTEGER,
                         session_id      TEXT NOT NULL,
                         worktree        TEXT,
                         branch          TEXT,
                         phase           TEXT NOT NULL,
                         expected_signal TEXT,
                         cost_tokens     INTEGER NOT NULL DEFAULT 0,
                         agent_state     TEXT,
                         cost_usd        REAL NOT NULL DEFAULT 0.0,
                         log_dir         TEXT,
                         pid             INTEGER,
                         pr              INTEGER,
                         rework_count    INTEGER NOT NULL DEFAULT 0,
                         updated_at      INTEGER NOT NULL
                     );
                     INSERT INTO journal_new(agent, role, task_id, session_id, worktree, branch,
                         phase, expected_signal, cost_tokens, agent_state, cost_usd, log_dir,
                         pid, pr, rework_count, updated_at)
                         SELECT agent, role, task_id, session_id, worktree, branch,
                             phase, {es_expr}, cost_tokens, agent_state, cost_usd, log_dir,
                             pid, pr, rework_count, updated_at
                         FROM journal;
                     DROP TABLE journal;
                     ALTER TABLE journal_new RENAME TO journal;"
                ))?;
            }
        }
        // v19 = single-daemon-per-DB guard. Net-new `daemon_lock` table — the
        // `CREATE TABLE IF NOT EXISTS` in SCHEMA_SQL handles fresh DBs and
        // upgrades alike; no ALTER needed.

        // v20 = lifecycle columns on tasks: author, reviewer, rework_round,
        // review_only. Additive — pre-existing rows default to NULL/0.
        if current < 20 {
            if !column_exists(conn, "tasks", "author")? {
                conn.execute("ALTER TABLE tasks ADD COLUMN author TEXT", [])?;
            }
            if !column_exists(conn, "tasks", "reviewer")? {
                conn.execute("ALTER TABLE tasks ADD COLUMN reviewer TEXT", [])?;
            }
            if !column_exists(conn, "tasks", "rework_round")? {
                conn.execute(
                    "ALTER TABLE tasks ADD COLUMN rework_round INTEGER NOT NULL DEFAULT 0",
                    [],
                )?;
            }
            if !column_exists(conn, "tasks", "review_only")? {
                conn.execute(
                    "ALTER TABLE tasks ADD COLUMN review_only INTEGER NOT NULL DEFAULT 0",
                    [],
                )?;
            }
        }
        // v23 = R2 review audits (#92): `review_audits` table (net-new, via
        // SCHEMA_SQL) + `agent_runs.sub_role` to distinguish R2 from R1 runs.
        if current < 23 && !column_exists(conn, "agent_runs", "sub_role")? {
            conn.execute("ALTER TABLE agent_runs ADD COLUMN sub_role TEXT", [])?;
        }
        // v24 = #125 automatic post-merge review analytics collector: additive
        // columns on `review_findings` for addressed-status + evidence ids +
        // collector model/version, plus the net-new `review_collection_runs`
        // table (via SCHEMA_SQL) for per-PR audit rows.
        //
        // v25 = #127 crash-safe collector retry: net-new `review_interpret_jobs`
        // table as a durable retry queue with attempt/backoff state. The table
        // is created by SCHEMA_SQL above (CREATE TABLE IF NOT EXISTS) — no
        // ALTER needed. Splitting v25 from v24 is load-bearing: origin/main
        // already shipped SCHEMA_VERSION=24, so a live daemon DB at
        // user_version=24 would short-circuit `migrate` (see the early return
        // above at `current == SCHEMA_VERSION`) and never run SCHEMA_SQL,
        // leaving the new table absent. Bumping to 25 forces the SCHEMA_SQL
        // pass on those DBs and the CREATE TABLE IF NOT EXISTS lands the
        // table cleanly (invariant #8 — repo-vs-running-file drift).
        if current < 24 {
            if !column_exists(conn, "review_findings", "addressed_status")? {
                conn.execute(
                    "ALTER TABLE review_findings ADD COLUMN addressed_status TEXT",
                    [],
                )?;
            }
            if !column_exists(conn, "review_findings", "evidence_ids")? {
                conn.execute(
                    "ALTER TABLE review_findings ADD COLUMN evidence_ids TEXT",
                    [],
                )?;
            }
            if !column_exists(conn, "review_findings", "collector_model")? {
                conn.execute(
                    "ALTER TABLE review_findings ADD COLUMN collector_model TEXT",
                    [],
                )?;
            }
            if !column_exists(conn, "review_findings", "collector_version")? {
                conn.execute(
                    "ALTER TABLE review_findings ADD COLUMN collector_version TEXT",
                    [],
                )?;
            }
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
            "mailbox",
            "journal",
            "daemon_lock",
            "agent_runs",
            "task_messages",
            "task_message_deliveries",
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
        // (b) UNIQUE constraints present (the load-bearing invariants of #98).
        // After v18 migration, task_branches has UNIQUE(task_id) + UNIQUE(branch)
        // as inline column constraints, plus the explicit task_branches_task index.
        let idx_count: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND tbl_name='task_branches'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            idx_count >= 1,
            "expected ≥1 index on task_branches; got {idx_count}"
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

        // Now open via the production path — migrate() must lift v9 → SCHEMA_VERSION
        // (currently 11 after #101's activity-hook tables landed) and ALTER the
        // agents table along the way to add retire_status + retired_at.
        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            v, SCHEMA_VERSION,
            "user_version must advance to current SCHEMA_VERSION (this test specifically pins the v9→v10 retire-columns ALTER)"
        );
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

    #[test]
    fn migrates_v12_to_v13_adds_journal_agent_state() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        // Hand-craft a v12 database with a journal table missing agent_state.
        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE journal (
                 agent      TEXT PRIMARY KEY,
                 role       TEXT NOT NULL,
                 task_id    INTEGER,
                 session_id TEXT NOT NULL,
                 worktree   TEXT,
                 branch     TEXT,
                 phase      TEXT NOT NULL DEFAULT 'working',
                 cost_tokens INTEGER NOT NULL DEFAULT 0,
                 updated_at INTEGER NOT NULL
             );
             INSERT INTO journal(agent, role, task_id, session_id, phase, cost_tokens, updated_at)
                 VALUES ('W1', 'worker', 42, 'sess-1', 'working', 1000, 100);
             PRAGMA user_version = 12;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        assert!(
            column_exists(&c, "journal", "agent_state").unwrap(),
            "agent_state column missing — v12→v13 migration silently skipped"
        );

        let state: Option<String> = c
            .query_row(
                "SELECT agent_state FROM journal WHERE agent='W1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(state.is_none(), "pre-existing row must default to NULL");
    }

    #[test]
    fn migrates_v13_to_v14_adds_journal_cost_usd_and_log_dir() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE journal (
                 agent      TEXT PRIMARY KEY,
                 role       TEXT NOT NULL,
                 task_id    INTEGER,
                 session_id TEXT NOT NULL,
                 worktree   TEXT,
                 branch     TEXT,
                 phase      TEXT NOT NULL DEFAULT 'working',
                 expected_signal TEXT,
                 cost_tokens INTEGER NOT NULL DEFAULT 0,
                 agent_state TEXT,
                 updated_at INTEGER NOT NULL
             );
             INSERT INTO journal(agent, role, task_id, session_id, phase, cost_tokens, updated_at)
                 VALUES ('W1', 'worker', 42, 'sess-1', 'working', 1000, 100);
             PRAGMA user_version = 13;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        assert!(
            column_exists(&c, "journal", "cost_usd").unwrap(),
            "cost_usd column missing — v13→v14 migration silently skipped"
        );
        assert!(
            column_exists(&c, "journal", "log_dir").unwrap(),
            "log_dir column missing — v13→v14 migration silently skipped"
        );

        let (cost_usd, log_dir): (f64, Option<String>) = c
            .query_row(
                "SELECT cost_usd, log_dir FROM journal WHERE agent='W1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(
            (cost_usd - 0.0).abs() < f64::EPSILON,
            "pre-existing row defaults to 0.0"
        );
        assert!(log_dir.is_none(), "pre-existing row must default to NULL");
    }

    #[test]
    fn migrates_v14_to_v15_adds_journal_m7_columns() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE journal (
                 agent      TEXT PRIMARY KEY,
                 role       TEXT NOT NULL,
                 task_id    INTEGER,
                 session_id TEXT NOT NULL,
                 worktree   TEXT,
                 branch     TEXT,
                 phase      TEXT NOT NULL DEFAULT 'working',
                 expected_signal TEXT,
                 cost_tokens INTEGER NOT NULL DEFAULT 0,
                 agent_state TEXT,
                 cost_usd   REAL NOT NULL DEFAULT 0.0,
                 log_dir    TEXT,
                 updated_at INTEGER NOT NULL
             );
             INSERT INTO journal(agent, role, task_id, session_id, phase, cost_tokens, updated_at)
                 VALUES ('W1', 'worker', 42, 'sess-1', 'working', 1000, 100);
             PRAGMA user_version = 14;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        assert!(
            column_exists(&c, "journal", "pid").unwrap(),
            "pid column missing — v14→v15 migration silently skipped"
        );
        assert!(
            column_exists(&c, "journal", "pr").unwrap(),
            "pr column missing — v14→v15 migration silently skipped"
        );
        assert!(
            column_exists(&c, "journal", "rework_count").unwrap(),
            "rework_count column missing — v14→v15 migration silently skipped"
        );

        let (pid, pr, rework_count): (Option<i32>, Option<i64>, i32) = c
            .query_row(
                "SELECT pid, pr, rework_count FROM journal WHERE agent='W1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert!(pid.is_none(), "pre-existing row must default pid to NULL");
        assert!(pr.is_none(), "pre-existing row must default pr to NULL");
        assert_eq!(
            rework_count, 0,
            "pre-existing row must default rework_count to 0"
        );
    }

    #[test]
    fn migrates_v16_to_v17_adds_approvals_table() {
        // #228: a v16 DB (no `approvals` table, user_version=16) with a seeded
        // journal row must gain the durable approvals table on open, without
        // disturbing existing data.
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE journal (
                 agent TEXT PRIMARY KEY, role TEXT NOT NULL, task_id INTEGER,
                 session_id TEXT NOT NULL, worktree TEXT, branch TEXT,
                 phase TEXT NOT NULL DEFAULT 'working', expected_signal TEXT,
                 cost_tokens INTEGER NOT NULL DEFAULT 0, agent_state TEXT,
                 cost_usd REAL NOT NULL DEFAULT 0.0, log_dir TEXT, pid INTEGER,
                 pr INTEGER, rework_count INTEGER NOT NULL DEFAULT 0,
                 instance_id TEXT, updated_at INTEGER NOT NULL
             );
             INSERT INTO journal(agent, role, session_id, phase, updated_at)
                 VALUES ('W1', 'worker', 'sess-1', 'awaiting-review', 100);
             PRAGMA user_version = 16;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        // approvals table now exists and starts empty.
        let n: i64 = c
            .query_row("SELECT count(*) FROM approvals", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
        // Pre-existing journal row untouched.
        let phase: String = c
            .query_row("SELECT phase FROM journal WHERE agent='W1'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(phase, "awaiting-review");
    }

    #[test]
    fn migrates_v17_to_v18_strips_repo_and_instance_id() {
        // Per-repo DB refactor (2/3): a v17 DB must drop task_branches.repo
        // and journal.instance_id on open, preserving existing data.
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE task_branches (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 task_id INTEGER NOT NULL,
                 repo TEXT NOT NULL,
                 branch TEXT NOT NULL,
                 worktree TEXT NOT NULL,
                 allocated_by TEXT NOT NULL,
                 allocated_at INTEGER NOT NULL,
                 UNIQUE(task_id, repo),
                 UNIQUE(repo, branch)
             );
             INSERT INTO task_branches(task_id, repo, branch, worktree, allocated_by, allocated_at)
                 VALUES (1, 'ag2trust/quorum', 'feat/thing-w1', '/tmp/wt/thing-w1', 'W1', 100);
             CREATE TABLE journal (
                 agent TEXT PRIMARY KEY, role TEXT NOT NULL, task_id INTEGER,
                 session_id TEXT NOT NULL, worktree TEXT, branch TEXT,
                 phase TEXT NOT NULL DEFAULT 'working', expected_signal TEXT,
                 cost_tokens INTEGER NOT NULL DEFAULT 0, agent_state TEXT,
                 cost_usd REAL NOT NULL DEFAULT 0.0, log_dir TEXT, pid INTEGER,
                 pr INTEGER, rework_count INTEGER NOT NULL DEFAULT 0,
                 instance_id TEXT, updated_at INTEGER NOT NULL
             );
             INSERT INTO journal(agent, role, session_id, phase, instance_id, updated_at)
                 VALUES ('W1', 'worker', 'sess-1', 'working', '/tmp/wt', 100);
             PRAGMA user_version = 17;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        // task_branches: repo column gone, data preserved.
        assert!(
            !column_exists(&c, "task_branches", "repo").unwrap(),
            "repo column must be dropped from task_branches"
        );
        let (branch, worktree): (String, String) = c
            .query_row(
                "SELECT branch, worktree FROM task_branches WHERE task_id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(branch, "feat/thing-w1");
        assert_eq!(worktree, "/tmp/wt/thing-w1");

        // UNIQUE(task_id) enforced: duplicate task_id rejected.
        let dup = c.execute(
            "INSERT INTO task_branches(task_id, branch, worktree, allocated_by, allocated_at)
             VALUES (1, 'feat/other', '/tmp/wt/other', 'W2', 200)",
            [],
        );
        assert!(dup.is_err(), "UNIQUE(task_id) must reject duplicate");

        // UNIQUE(branch) enforced: duplicate branch rejected.
        let dup = c.execute(
            "INSERT INTO task_branches(task_id, branch, worktree, allocated_by, allocated_at)
             VALUES (2, 'feat/thing-w1', '/tmp/wt/other2', 'W2', 200)",
            [],
        );
        assert!(dup.is_err(), "UNIQUE(branch) must reject duplicate");

        // journal: instance_id column gone, data preserved.
        assert!(
            !column_exists(&c, "journal", "instance_id").unwrap(),
            "instance_id column must be dropped from journal"
        );
        let phase: String = c
            .query_row("SELECT phase FROM journal WHERE agent='W1'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(phase, "working");
    }

    #[test]
    fn migrates_v19_to_v20_adds_lifecycle_columns() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE tasks (
                 id          INTEGER PRIMARY KEY AUTOINCREMENT,
                 title       TEXT NOT NULL,
                 body        TEXT,
                 status      TEXT NOT NULL,
                 priority    INTEGER NOT NULL DEFAULT 0,
                 labels      TEXT,
                 assignee    TEXT,
                 created_by  TEXT NOT NULL,
                 created_at  INTEGER NOT NULL,
                 updated_at  INTEGER NOT NULL,
                 refs        TEXT,
                 depends_on  TEXT,
                 sticky_until INTEGER,
                 orig        TEXT
             );
             INSERT INTO tasks(title, status, created_by, created_at, updated_at)
                 VALUES ('seed task', 'open', 'test-agent', 100, 100);
             PRAGMA user_version = 19;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        assert!(column_exists(&c, "tasks", "author").unwrap());
        assert!(column_exists(&c, "tasks", "reviewer").unwrap());
        assert!(column_exists(&c, "tasks", "rework_round").unwrap());
        assert!(column_exists(&c, "tasks", "review_only").unwrap());

        let (author, reviewer, rework_round, review_only): (
            Option<String>,
            Option<String>,
            i64,
            i64,
        ) = c
            .query_row(
                "SELECT author, reviewer, rework_round, review_only FROM tasks WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert!(author.is_none(), "author default must be NULL");
        assert!(reviewer.is_none(), "reviewer default must be NULL");
        assert_eq!(rework_round, 0, "rework_round default must be 0");
        assert_eq!(review_only, 0, "review_only default must be 0");

        let title: String = c
            .query_row("SELECT title FROM tasks WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(title, "seed task");
    }

    #[test]
    fn migrates_v22_to_v23_adds_sub_role_and_review_audits() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE agent_runs (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 task_id INTEGER NOT NULL,
                 agent_name TEXT NOT NULL,
                 role TEXT NOT NULL CHECK(role IN ('worker','reviewer')),
                 model TEXT NOT NULL,
                 effort TEXT NOT NULL,
                 spawned_at INTEGER NOT NULL,
                 ended_at INTEGER,
                 end_reason TEXT
             );
             INSERT INTO agent_runs(task_id, agent_name, role, model, effort, spawned_at)
                 VALUES (1, 'Alice', 'worker', 'opus-46', 'high', 100);
             PRAGMA user_version = 22;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        assert!(
            column_exists(&c, "agent_runs", "sub_role").unwrap(),
            "sub_role column missing — v22→v23 migration silently skipped"
        );

        let sub_role: Option<String> = c
            .query_row("SELECT sub_role FROM agent_runs WHERE id=1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(sub_role.is_none(), "pre-existing row must default to NULL");

        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='review_audits'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "review_audits table must exist after v22→v23 migration"
        );
    }

    /// v23→v24: covers BOTH #125 (analytics collector — additive columns on
    /// `review_findings` + `review_collection_runs` table) and #127 (retry
    /// queue — `review_interpret_jobs` table). Pre-existing findings round-trip
    /// untouched and default to NULL for every new column.
    #[test]
    fn migrates_v23_to_v24_adds_collector_columns_and_new_tables() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE review_findings (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 pr_number INTEGER NOT NULL,
                 task_id INTEGER,
                 reviewer TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 author_pushback INTEGER NOT NULL DEFAULT 0,
                 pushback_accepted INTEGER,
                 severity TEXT,
                 text TEXT NOT NULL,
                 source_endpoint TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );
             INSERT INTO review_findings(pr_number, task_id, reviewer, kind, text,
                 source_endpoint, created_at)
                 VALUES (99, 5, 'rev', 'blocking', 'pre-v24 finding', 'pulls', 100);
             PRAGMA user_version = 23;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        for col in [
            "addressed_status",
            "evidence_ids",
            "collector_model",
            "collector_version",
        ] {
            assert!(
                column_exists(&c, "review_findings", col).unwrap(),
                "column '{col}' missing — v23→v24 migration silently skipped"
            );
        }

        // Pre-existing row must default to NULL for every new column.
        let (addr, ev, cm, cv): (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = c
            .query_row(
                "SELECT addressed_status, evidence_ids, collector_model, collector_version
                 FROM review_findings WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert!(addr.is_none() && ev.is_none() && cm.is_none() && cv.is_none());

        for tbl in ["review_collection_runs", "review_interpret_jobs"] {
            let n: i64 = c
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [tbl],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "{tbl} must exist after v23→v24 migration");
        }
        let count: i64 = c
            .query_row("SELECT count(*) FROM review_interpret_jobs", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    /// v24→v25 (#127 rebased): the real upgrade path for a daemon whose DB
    /// already shipped SCHEMA_VERSION=24 (from #125 before #127 rebased on
    /// top). The blast radius of getting this wrong: `migrate` early-returns
    /// on `current == SCHEMA_VERSION` WITHOUT running SCHEMA_SQL, so a v24
    /// DB would never learn about the net-new `review_interpret_jobs` table
    /// and every tick's Phase 7.5 (`list_all` / `list_ready`) would raise
    /// "no such table" out of the daemon loop. Bumping to v25 forces the
    /// SCHEMA_SQL pass on those DBs and the CREATE TABLE IF NOT EXISTS
    /// lands the table cleanly (invariant #8 — repo-vs-running-file drift).
    #[test]
    fn migrates_v24_to_v25_adds_review_interpret_jobs_table() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        // Seed a minimal v24-shaped DB: only the surface the new table
        // reads/writes (task rows) needs to exist for the migration itself
        // to succeed; the SCHEMA_SQL pass creates every missing table.
        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE review_collection_runs (
                 pr_number INTEGER PRIMARY KEY,
                 task_id INTEGER,
                 status TEXT NOT NULL,
                 error TEXT,
                 collector_model TEXT NOT NULL,
                 collector_version TEXT NOT NULL,
                 findings_count INTEGER NOT NULL DEFAULT 0,
                 attempted_at INTEGER NOT NULL,
                 completed_at INTEGER
             );
             PRAGMA user_version = 24;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        // Pin the pre-open state: table is absent.
        let raw = Connection::open(&path).unwrap();
        let pre: i64 = raw
            .query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='table' AND name='review_interpret_jobs'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pre, 0, "seed must not include the new table");
        drop(raw);

        // Open through the production path — migration runs.
        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        // Net-new table must exist and be usable.
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='table' AND name='review_interpret_jobs'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "review_interpret_jobs must exist after v24→v25 migration \
             — otherwise every daemon tick's Phase 7.5 raises 'no such table'"
        );
        // A read against it must succeed (not just the existence check).
        let count: i64 = c
            .query_row("SELECT count(*) FROM review_interpret_jobs", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }
}
