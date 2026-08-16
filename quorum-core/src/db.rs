//! Database connection setup: mandatory PRAGMAs + schema migration-on-open.
//!
//! Every connection applies the same PRAGMAs (they are per-connection in SQLite) and runs
//! [`migrate`] before use, so any short-lived `quorum` process self-heals the schema.

use crate::error::{QuorumError, Result};
use rusqlite::{Connection, Error as SqlErr, ErrorCode, Transaction, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

/// Schema version this binary understands. Bump when adding a migration.
pub const SCHEMA_VERSION: i64 = 52;

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
    apply_persistent_pragmas(conn)
}

/// Apply the PRAGMAs that follow `busy_timeout`. Kept separate so open can
/// perform its read-only schema compatibility check before the persistent WAL
/// switch without moving that check ahead of the mandatory timeout.
fn apply_persistent_pragmas(conn: &Connection) -> Result<()> {
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

/// Open the store at `path`, refusing an unsupported newer schema before applying
/// any persistent PRAGMA, then applying PRAGMAs and running migrations. The
/// returned connection is ready for use.
pub fn open(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MS)?;
    ensure_schema_supported(&conn)?;
    apply_persistent_pragmas(&conn)?;
    migrate(&conn)?;
    Ok(conn)
}

/// Like [`open`], but also returns the migration outcome so callers can report what changed.
pub fn open_init(path: &Path) -> Result<(Connection, MigrateResult)> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MS)?;
    ensure_schema_supported(&conn)?;
    apply_persistent_pragmas(&conn)?;
    let info = migrate(&conn)?;
    Ok((conn, info))
}

/// Read the on-disk version and reject unsupported schemas without changing
/// connection or database state. This must run before
/// [`apply_persistent_pragmas`]: switching `journal_mode` to WAL is persistent
/// and would otherwise mutate a newer database that this binary has no
/// authority to open.
fn ensure_schema_supported(conn: &Connection) -> Result<i64> {
    let current: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if current > SCHEMA_VERSION {
        return Err(QuorumError::SchemaTooNew {
            db: current,
            bin: SCHEMA_VERSION,
        });
    }
    Ok(current)
}

/// Bring the on-disk schema up to [`SCHEMA_VERSION`].
///
/// Forward-only and idempotent. Runs under `BEGIN IMMEDIATE` so concurrent first-runs are
/// safe. Refuses (fails loud) if the DB was written by a newer binary.
pub fn migrate(conn: &Connection) -> Result<MigrateResult> {
    let current = ensure_schema_supported(conn)?;
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
        // v26 = daemon-issued run capabilities (#130). Net-new
        // `run_capabilities` table via SCHEMA_SQL — no ALTER needed. Landing at
        // v26 (not v25) because main already shipped SCHEMA_VERSION=25 for the
        // post-merge review analytics collector (#127), and the `current ==
        // SCHEMA_VERSION` early-return above short-circuits SCHEMA_SQL — a live
        // DB stopped at user_version=25 would otherwise never see the new table.
        // v26 forces the migration path to run once.

        // v27 = prospective-only perf watermark (#158). Net-new
        // `perf_watermark` table via SCHEMA_SQL. On first migration past v26
        // we seed the single watermark row with the current unix timestamp —
        // every task that reached terminal status before this instant is
        // excluded from the default `quorum perf` report. INSERT OR IGNORE
        // is idempotent: re-running the migration (e.g. after a crash
        // between SCHEMA_SQL and the version stamp) never moves the boundary.
        if current < 27 {
            let now = crate::clock::now();
            conn.execute(
                "INSERT OR IGNORE INTO perf_watermark (id, watermark) VALUES (1, ?1)",
                [now],
            )?;
        }

        // v28 = mandatory dual-review approvals (#159). Recreate the approvals
        // table with a composite PK (pr_number, review_role) so R1 and R2
        // verdicts are stored independently. Existing single-row approvals
        // are migrated as role='r1'.
        if current < 28 {
            let has_old_approvals: bool = conn
                .query_row(
                    "SELECT count(*) > 0 FROM sqlite_master \
                     WHERE type='table' AND name='approvals'",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(false);
            if has_old_approvals && !column_exists(conn, "approvals", "review_role")? {
                conn.execute_batch(
                    "CREATE TABLE approvals_v28 (
                        pr_number         INTEGER NOT NULL,
                        review_role       TEXT NOT NULL DEFAULT 'r1',
                        task_id           INTEGER NOT NULL,
                        author            TEXT NOT NULL,
                        reviewer          TEXT NOT NULL,
                        verdict           TEXT NOT NULL,
                        blocking_count    INTEGER NOT NULL,
                        approved_head_sha TEXT NOT NULL,
                        created_at        INTEGER NOT NULL,
                        PRIMARY KEY (pr_number, review_role)
                    );
                    INSERT OR IGNORE INTO approvals_v28
                        (pr_number, review_role, task_id, author, reviewer, verdict,
                         blocking_count, approved_head_sha, created_at)
                        SELECT pr_number, 'r1', task_id, author, reviewer, verdict,
                               blocking_count, approved_head_sha, created_at
                        FROM approvals;
                    DROP TABLE approvals;
                    ALTER TABLE approvals_v28 RENAME TO approvals;
                    CREATE INDEX IF NOT EXISTS approvals_task ON approvals(task_id);",
                )?;
            }
        }

        // v29 = durable crash-recovery budget (#163). Additive column on
        // `tasks`; pre-existing rows default to 0 (no prior recovery attempts).
        if current < 29 && !column_exists(conn, "tasks", "recovery_attempts")? {
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN recovery_attempts INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }

        // v30 = durable reviewer-provision attempt tracking (#190). Net-new
        // `reviewer_provision_attempts` table via SCHEMA_SQL — no ALTER needed.
        // Landing at v30 because main shipped SCHEMA_VERSION=29 for the
        // recovery_attempts column; a live DB at user_version=29 would
        // short-circuit SCHEMA_SQL and miss the new table.

        // v31 = per-run provider column on agent_runs (#194). NULL for
        // pre-existing rows (implies Claude, the only prior provider).
        if current < 31 && !column_exists(conn, "agent_runs", "provider")? {
            conn.execute("ALTER TABLE agent_runs ADD COLUMN provider TEXT", [])?;
        }

        // v32 = durable PR target persistence (#201). Net-new `pr_targets`
        // table via SCHEMA_SQL — no ALTER needed. Landing at v32 because
        // main shipped SCHEMA_VERSION=31; a live DB at user_version=31
        // would short-circuit SCHEMA_SQL and miss the new table.

        // v33 = daemon-owned R2 sampling decisions (#224 remediation).
        // This table is intentionally separate from task refs: refs can be
        // supplied and updated through agent-facing task commands, whereas a
        // sampled R2 skip is merge-gate authority. Landing at v33 forces live
        // v32 databases through SCHEMA_SQL so the net-new table is present.

        // v34 = bounded REVIEWING status projection (#239). The partial index
        // matches each durable status and newest-first order. Status reads at
        // most REVIEWING_TASK_LIMIT candidates per status before merging, rather
        // than sorting every historical review-only task. Bumping is required
        // for live v33 databases, where SCHEMA_SQL would otherwise be skipped.

        // v36 reconciles the two independently shipped v35 additions: bounded task
        // decomposition and authoritative existing-PR implementation intent. Check
        // every column so a database created by either v35 lineage gains the other.
        // The aggregate/member/attempt/cleanup
        // tables are created by SCHEMA_SQL. Task revisions are additive so a
        // populated database preserves every existing task at revision 1 with
        // no accepted edits.
        if current < 36 {
            if !column_exists(conn, "tasks", "revision")? {
                conn.execute(
                    "ALTER TABLE tasks ADD COLUMN revision INTEGER NOT NULL DEFAULT 1",
                    [],
                )?;
            }
            if !column_exists(conn, "tasks", "edit_count")? {
                conn.execute(
                    "ALTER TABLE tasks ADD COLUMN edit_count INTEGER NOT NULL DEFAULT 0",
                    [],
                )?;
            }
            // Nullable preserves every historical task as ordinary/review-only;
            // the CLI/core boundary validates writes on upgraded databases.
            if !column_exists(conn, "tasks", "continue_pr")? {
                conn.execute("ALTER TABLE tasks ADD COLUMN continue_pr INTEGER", [])?;
            }
        }

        // v37 persists an accepted bounded proposal across daemon restarts.
        // The reservation table is created by SCHEMA_SQL.
        if current < 37 && !column_exists(conn, "task_decompositions", "accepted_proposal_json")? {
            conn.execute(
                "ALTER TABLE task_decompositions ADD COLUMN accepted_proposal_json TEXT",
                [],
            )?;
        }

        // v36 = indexed corrupt terminal retry candidates (#270). The two
        // partial indexes are created by SCHEMA_SQL: one supports the daemon's
        // oldest-first bounded reconciliation batch, and one supports the
        // newest-first bounded status projection. A live v35 database must run
        // SCHEMA_SQL once so both indexes are materialized.

        // v38 repairs the split v36 lineages. Decomposition's v36 added optimistic
        // task-edit columns, while main's independently shipped v36 did not. A main
        // database could therefore be stamped v37 by the integration binary without
        // receiving either column. Guard each ALTER independently so every historical
        // shape converges without changing existing task data.
        if current < 38 {
            if !column_exists(conn, "tasks", "revision")? {
                conn.execute(
                    "ALTER TABLE tasks ADD COLUMN revision INTEGER NOT NULL DEFAULT 1",
                    [],
                )?;
            }
            if !column_exists(conn, "tasks", "edit_count")? {
                conn.execute(
                    "ALTER TABLE tasks ADD COLUMN edit_count INTEGER NOT NULL DEFAULT 0",
                    [],
                )?;
            }
        }

        // v39 makes decomposition cleanup crash-recoverable. SQLite cannot
        // alter a CHECK constraint, so rebuild the table under the same write
        // lock. Historical `complete` rows remain terminal; historical
        // `failed` rows become retryable pending work.
        if current < 39 {
            conn.execute_batch(
                "CREATE TABLE decomposition_cleanup_v39 (
                     graph_id       INTEGER NOT NULL REFERENCES task_decompositions(id),
                     task_id        INTEGER NOT NULL REFERENCES tasks(id),
                     artifact_kind  TEXT NOT NULL,
                     artifact_ref   TEXT NOT NULL,
                     state          TEXT NOT NULL DEFAULT 'pending'
                                          CHECK(state IN ('pending','running','done','exhausted')),
                     attempts       INTEGER NOT NULL DEFAULT 0,
                     last_error     TEXT,
                     updated_at     INTEGER NOT NULL,
                     PRIMARY KEY (graph_id, task_id, artifact_kind, artifact_ref)
                 );
                 INSERT INTO decomposition_cleanup_v39(
                     graph_id,task_id,artifact_kind,artifact_ref,state,attempts,last_error,updated_at)
                 SELECT graph_id,task_id,artifact_kind,artifact_ref,
                        CASE state WHEN 'complete' THEN 'done'
                                   WHEN 'failed' THEN 'pending' ELSE state END,
                        attempts,last_error,updated_at
                 FROM decomposition_cleanup;
                 DROP TABLE decomposition_cleanup;
                 ALTER TABLE decomposition_cleanup_v39 RENAME TO decomposition_cleanup;",
            )?;
        }

        // v40 binds a task-owned branch allocation to the immutable commit it
        // was provisioned from. Historical allocations remain NULL and are
        // deliberately ineligible for destructive branch discovery.
        if current < 40 && !column_exists(conn, "task_branches", "provenance_sha")? {
            conn.execute(
                "ALTER TABLE task_branches ADD COLUMN provenance_sha TEXT",
                [],
            )?;
        }

        // v41 persists the immutable PR head assigned to an exact reviewer
        // capability. Restart recovery must never infer review authority from
        // a mutable worktree checkout.
        if current < 41 {
            if !column_exists(conn, "agent_runs", "review_cap_run_id")? {
                conn.execute(
                    "ALTER TABLE agent_runs ADD COLUMN review_cap_run_id TEXT",
                    [],
                )?;
            }
            if !column_exists(conn, "agent_runs", "review_pr")? {
                conn.execute("ALTER TABLE agent_runs ADD COLUMN review_pr INTEGER", [])?;
            }
            if !column_exists(conn, "agent_runs", "review_head_sha")? {
                conn.execute("ALTER TABLE agent_runs ADD COLUMN review_head_sha TEXT", [])?;
            }
        }
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS agent_runs_review_cap
             ON agent_runs(review_cap_run_id) WHERE review_cap_run_id IS NOT NULL",
            [],
        )?;

        // v42 = durable weighted model routing. Net-new authority tables are created by
        // SCHEMA_SQL; these nullable links extend existing canonical evidence without
        // reinterpreting historical rows.
        if current < 42 {
            if !column_exists(conn, "agent_runs", "role_assignment_id")? {
                conn.execute("ALTER TABLE agent_runs ADD COLUMN role_assignment_id INTEGER REFERENCES role_assignments(id)", [])?;
            }
            if !column_exists(conn, "task_decompositions", "planner_assignment_id")? {
                conn.execute("ALTER TABLE task_decompositions ADD COLUMN planner_assignment_id INTEGER REFERENCES role_assignments(id)", [])?;
            }
            if !column_exists(conn, "review_collection_runs", "role_assignment_id")? {
                conn.execute("ALTER TABLE review_collection_runs ADD COLUMN role_assignment_id INTEGER REFERENCES role_assignments(id)", [])?;
            }
            conn.execute_batch(
                "CREATE INDEX IF NOT EXISTS agent_runs_role_assignment
                     ON agent_runs(role_assignment_id);
                 CREATE INDEX IF NOT EXISTS review_collection_runs_role_assignment
                     ON review_collection_runs(role_assignment_id);
                 CREATE INDEX IF NOT EXISTS task_decompositions_planner_assignment
                     ON task_decompositions(planner_assignment_id);",
            )?;
        }

        // v43 adds dormant durable review follow-up batches and artifacts via
        // SCHEMA_SQL. The run-count column is additive; historical collection
        // rows retain the default zero without reinterpretation or backfill.
        if current < 43 && !column_exists(conn, "review_collection_runs", "followup_count")? {
            conn.execute(
                "ALTER TABLE review_collection_runs ADD COLUMN followup_count INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        // v44 adds dormant follow-up assessment aggregates and artifact
        // membership via SCHEMA_SQL. v45 adds counter-bound and membership-
        // immutability triggers, also via SCHEMA_SQL. v46 adds a durable,
        // irreversible membership seal. Existing assessments are sealed by
        // default; only the atomic materializer explicitly creates an open row.
        if current < 46 && !column_exists(conn, "review_followup_assessments", "membership_sealed")?
        {
            conn.execute(
                "ALTER TABLE review_followup_assessments
                 ADD COLUMN membership_sealed INTEGER NOT NULL DEFAULT 1
                 CHECK(membership_sealed IN (0,1))",
                [],
            )?;
        }
        if current < 46 {
            conn.execute_batch(
                "CREATE TRIGGER IF NOT EXISTS review_followup_membership_insert_unsealed
                 BEFORE INSERT ON review_followup_assessment_artifacts
                 WHEN NOT EXISTS (
                     SELECT 1 FROM review_followup_assessments
                     WHERE id=NEW.assessment_id AND membership_sealed=0
                       AND state='pending' AND active=0
                 )
                 BEGIN
                     SELECT RAISE(ABORT, 'follow-up assessment membership is sealed');
                 END;

                 CREATE TRIGGER IF NOT EXISTS review_followup_membership_no_unseal
                 BEFORE UPDATE OF membership_sealed ON review_followup_assessments
                 WHEN OLD.membership_sealed=1 AND NEW.membership_sealed!=1
                 BEGIN
                     SELECT RAISE(ABORT, 'follow-up assessment membership seal is irreversible');
                 END;",
            )?;
        }
        // v47 records daemon-owned terminal provenance without reinterpreting
        // history. Existing done rows remain NULL: refs.pr alone cannot prove
        // whether task-close represented a merge, an external fix, or obsolescence.
        // The column check deliberately extends through v48: v47 was also used
        // by PR #565 before main's completion-provenance migration landed, so
        // either v47 shape must converge safely when the lineages merge.
        if current < 48 && !column_exists(conn, "tasks", "completion_provenance")? {
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN completion_provenance TEXT
                 CHECK(completion_provenance IS NULL
                       OR completion_provenance IN ('merged','manual'))",
                [],
            )?;
        }
        // Guarded core write APIs remain dormant until later daemon activation
        // work.

        // v48 bounds sweep's REFERENCES tasks(id) guards (task #395). Schema 46
        // and main's independently shipped v47 left six durable FK columns
        // unindexed; sweep_on_write runs inside every mutation's write
        // transaction and would otherwise scan each retained provenance table
        // once per candidate task. SCHEMA_SQL (which runs at the top of
        // migrate) declares the six indexes and rotating sweep cursor so fresh
        // DBs get them, but the v39 block below drops+recreates
        // `decomposition_cleanup` via a rename that also drops its indexes;
        // recreate every durable-ref index here so both fresh and upgraded
        // databases end up with the same shape. `CREATE INDEX IF NOT EXISTS`
        // is idempotent so re-running the migration (crash between blocks) is
        // safe. Bumping SCHEMA_VERSION to 48 is load-bearing: the
        // `current == SCHEMA_VERSION` early-return above short-circuits
        // SCHEMA_SQL, so a live DB stopped at either v47 lineage would
        // otherwise retain an incomplete schema.
        if current < 48 {
            conn.execute_batch(
                "CREATE INDEX IF NOT EXISTS decomposition_cleanup_task
                     ON decomposition_cleanup(task_id);
                 CREATE INDEX IF NOT EXISTS review_followup_batches_task
                     ON review_followup_batches(task_id);
                 CREATE INDEX IF NOT EXISTS review_followup_batches_source_task
                     ON review_followup_batches(source_task_id);
                 CREATE INDEX IF NOT EXISTS review_followup_artifacts_linked_task
                     ON review_followup_artifacts(linked_task_id)
                     WHERE linked_task_id IS NOT NULL;
                 CREATE INDEX IF NOT EXISTS review_followup_artifacts_created_task
                     ON review_followup_artifacts(created_task_id)
                     WHERE created_task_id IS NOT NULL;
                 CREATE INDEX IF NOT EXISTS review_followup_assessments_source_task
                     ON review_followup_assessments(source_task_id);",
            )?;
        }
        // v49 adds immutable responsibility-scoped routing-attempt evidence.
        // The table, assignment guard, immutability triggers, and read index are
        // all additive and created idempotently by SCHEMA_SQL above.

        // v50 backfills task #473's durable `daemon_parked_unsatisfiable` bit
        // and the distinct "cancelled — unsatisfiable" reason onto rows already
        // parked before this binary shipped. Without this, live examples
        // (#308/#309, #313/#314, #318, #421/#422 in ag2trust/quorum on
        // 2026-08-15) stay hidden from the BLOCKED disposition queue and
        // indistinguishable from recoverable failed-dep parks — the exact
        // operator symptom task #473 was opened to fix. Idempotent: the guard
        // requires the marker to be missing/false, so a re-run at v50 is a
        // no-op. Data-only backfill; no schema shape change.
        // Run through v51 as well: task #426 independently shipped v50 for
        // dormant-worker journal identity, so that lineage has not applied
        // main's v50 data migration. The UPDATE remains idempotent by marker.
        if current < 52 && column_exists(conn, "tasks", "depends_on")? {
            conn.execute(
                "UPDATE tasks
                 SET refs = json_set(
                         refs,
                         '$.daemon_parked_unsatisfiable', json('true'),
                         '$.daemon_parked_reason',
                         'dependency #' ||
                         (SELECT j.value FROM json_each(tasks.depends_on) j
                          JOIN tasks d ON d.id = j.value
                          WHERE d.status IN ('cancelled')
                          ORDER BY j.value LIMIT 1)
                         || ' is cancelled — unsatisfiable'
                     )
                 WHERE status='failed'
                   AND depends_on IS NOT NULL
                   AND json_valid(refs)
                   AND json_extract(refs, '$.daemon_parked')=1
                   -- Same rule as `upgrade_stale_recoverable_parks` in
                   -- sweep.rs: classifier-policy parks own their own
                   -- lifecycle (reason 'classifier declined', retry stays
                   -- in `failed` as a reclassification request) and must
                   -- not be relabeled as unsatisfiable-dep parks.
                   -- Diverging here would make behavior depend on upgrade
                   -- timing.
                   AND COALESCE(
                       json_extract(refs, '$.classifier_policy_parked'), 0
                   ) != 1
                   AND COALESCE(
                       json_extract(refs, '$.daemon_parked_unsatisfiable'), 0
                   ) != 1
                   AND EXISTS (
                       SELECT 1 FROM json_each(tasks.depends_on) j
                       JOIN tasks d ON d.id = j.value
                       WHERE d.status IN ('cancelled')
                   )",
                [],
            )?;
        }
        // v51 adds a durable cursor queue for bounded cancelled-dependency
        // reconciliation. The idempotent table is declared in SCHEMA_SQL;
        // repeat it here to make the versioned shape explicit.
        if current < 51 {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS cancelled_dependency_reconciliation (
                     cancelled_task_id INTEGER PRIMARY KEY REFERENCES tasks(id),
                     task_cursor       INTEGER NOT NULL DEFAULT 0,
                     updated_at        INTEGER NOT NULL
                 );",
            )?;
        }
        // v52 converges main's v50/v51 migrations with task #426's
        // independently shipped v50 dormant-worker journal identity. Fresh
        // databases receive these nullable columns from SCHEMA_SQL; guarded
        // ALTERs upgrade either published lineage without rewriting history.
        if current < 52 {
            if !column_exists(conn, "journal", "provider")? {
                conn.execute("ALTER TABLE journal ADD COLUMN provider TEXT", [])?;
            }
            if !column_exists(conn, "journal", "continuation_id")? {
                conn.execute("ALTER TABLE journal ADD COLUMN continuation_id TEXT", [])?;
            }
            if !column_exists(conn, "journal", "local_branch")? {
                conn.execute("ALTER TABLE journal ADD COLUMN local_branch TEXT", [])?;
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
            "role_assignments",
            "routing_cursors",
            "routing_attempts",
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
    fn main_v49_migration_adds_dormant_journal_identity_without_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("main-v49-dormant-journal.db");
        {
            let conn = open(&path).unwrap();
            conn.execute(
                "INSERT INTO journal(agent,role,task_id,session_id,phase,updated_at)
                 VALUES ('legacy','worker',7,'session','working',1)",
                [],
            )
            .unwrap();
            conn.execute_batch(
                "ALTER TABLE journal DROP COLUMN provider;
                 ALTER TABLE journal DROP COLUMN continuation_id;
                 ALTER TABLE journal DROP COLUMN local_branch;
                 PRAGMA user_version=49;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        for column in ["provider", "continuation_id", "local_branch"] {
            assert!(column_exists(&conn, "journal", column).unwrap());
        }
        let legacy: (Option<String>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT provider,continuation_id,local_branch FROM journal WHERE agent='legacy'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(legacy, (None, None, None));
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn dormant_worker_v49_migration_adds_immutable_routing_attempts_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dormant-worker-v49.db");
        {
            let conn = Connection::open(&path).unwrap();
            apply_pragmas(&conn).unwrap();
            conn.execute_batch(
                "CREATE TABLE role_assignments (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     responsibility_key TEXT NOT NULL UNIQUE,
                     task_id INTEGER,
                     pr_number INTEGER,
                     role TEXT NOT NULL,
                     review_stage TEXT,
                     complexity TEXT,
                     profile_id TEXT NOT NULL,
                     provider TEXT NOT NULL,
                     runner TEXT NOT NULL,
                     model TEXT NOT NULL,
                     effort TEXT NOT NULL,
                     pool_key TEXT NOT NULL,
                     policy_generation TEXT NOT NULL,
                     created_at INTEGER NOT NULL
                 );
                 INSERT INTO role_assignments(
                     id,responsibility_key,task_id,role,complexity,profile_id,provider,
                     runner,model,effort,pool_key,policy_generation,created_at)
                 VALUES (9,'worker:task:9',9,'worker','M','opus','claude','claude',
                         'claude-opus-4-8','high','worker.M','generation-1',10);
                 PRAGMA user_version=49;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            conn.query_row(
                "SELECT responsibility_key,profile_id FROM role_assignments WHERE id=9",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
            ("worker:task:9".into(), "opus".into())
        );
        for object in [
            "routing_attempts",
            "routing_attempts_responsibility",
            "routing_attempts_assignment_guard",
            "routing_attempts_no_update",
            "routing_attempts_no_delete",
        ] {
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE name=?1",
                    [object],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                1,
                "missing migrated object {object}"
            );
        }
        drop(conn);
        let reopened = open(&path).unwrap();
        assert_eq!(
            reopened
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            reopened
                .query_row("SELECT count(*) FROM role_assignments", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn fresh_schema_has_exact_dormant_review_followup_shape_and_constraints() {
        fn columns(
            conn: &Connection,
            table: &str,
        ) -> Vec<(String, String, i64, Option<String>, i64)> {
            let mut stmt = conn
                .prepare(
                    "SELECT name,type,\"notnull\",dflt_value,pk
                     FROM pragma_table_info(?1) ORDER BY cid",
                )
                .unwrap();
            stmt.query_map([table], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
        }

        fn foreign_keys(conn: &Connection, table: &str) -> Vec<(String, String, String)> {
            let mut stmt = conn
                .prepare(
                    "SELECT \"from\",\"table\",\"to\"
                     FROM pragma_foreign_key_list(?1) ORDER BY \"from\"",
                )
                .unwrap();
            stmt.query_map([table], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let conn = open(&dir.path().join("followups.db")).unwrap();
        conn.pragma_update(None, "foreign_keys", true).unwrap();

        assert_eq!(
            columns(&conn, "review_followup_batches"),
            vec![
                ("pr_number".into(), "INTEGER".into(), 0, None, 1),
                ("task_id".into(), "INTEGER".into(), 1, None, 0),
                ("graph_id".into(), "INTEGER".into(), 0, None, 0),
                ("source_task_id".into(), "INTEGER".into(), 1, None, 0),
                ("collector_version".into(), "TEXT".into(), 1, None, 0),
                ("artifact_count".into(), "INTEGER".into(), 1, None, 0),
                ("state".into(), "TEXT".into(), 1, None, 0),
                ("created_at".into(), "INTEGER".into(), 1, None, 0),
                ("updated_at".into(), "INTEGER".into(), 1, None, 0),
            ]
        );
        assert_eq!(
            columns(&conn, "review_followup_artifacts"),
            vec![
                ("id".into(), "INTEGER".into(), 0, None, 1),
                ("pr_number".into(), "INTEGER".into(), 1, None, 0),
                ("ordinal".into(), "INTEGER".into(), 1, None, 0),
                ("technical_impact".into(), "TEXT".into(), 1, None, 0),
                ("scope_relationship".into(), "TEXT".into(), 1, None, 0),
                ("concern".into(), "TEXT".into(), 1, None, 0),
                ("non_blocking_reason".into(), "TEXT".into(), 1, None, 0),
                ("affected_behavior".into(), "TEXT".into(), 1, None, 0),
                ("desired_outcome".into(), "TEXT".into(), 1, None, 0),
                (
                    "verification_expectations".into(),
                    "TEXT".into(),
                    1,
                    None,
                    0,
                ),
                ("evidence_ids".into(), "TEXT".into(), 1, None, 0),
                ("disposition".into(), "TEXT".into(), 0, None, 0),
                ("disposition_reason".into(), "TEXT".into(), 0, None, 0),
                ("linked_task_id".into(), "INTEGER".into(), 0, None, 0),
                ("created_task_id".into(), "INTEGER".into(), 0, None, 0),
                ("created_at".into(), "INTEGER".into(), 1, None, 0),
                ("updated_at".into(), "INTEGER".into(), 1, None, 0),
            ]
        );
        assert_eq!(
            foreign_keys(&conn, "review_followup_batches"),
            vec![
                ("graph_id".into(), "task_decompositions".into(), "id".into()),
                ("source_task_id".into(), "tasks".into(), "id".into()),
                ("task_id".into(), "tasks".into(), "id".into()),
            ]
        );
        assert_eq!(
            foreign_keys(&conn, "review_followup_artifacts"),
            vec![
                ("created_task_id".into(), "tasks".into(), "id".into()),
                ("linked_task_id".into(), "tasks".into(), "id".into()),
                (
                    "pr_number".into(),
                    "review_followup_batches".into(),
                    "pr_number".into(),
                ),
            ]
        );
        assert_eq!(
            conn.query_row(
                "SELECT type,\"notnull\",dflt_value
                 FROM pragma_table_info('review_collection_runs')
                 WHERE name='followup_count'",
                [],
                |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?
                )),
            )
            .unwrap(),
            ("INTEGER".into(), 1, "0".into())
        );

        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at) VALUES
                 (1,'source','done','owner',1,1),
                 (2,'linked','open','owner',1,1),
                 (3,'created','open','owner',1,1);
             INSERT INTO task_decompositions(
                 id,source_task_id,state,active,freeze_active,planned_source_revision,
                 created_at,updated_at)
             VALUES (10,1,'completed',0,0,1,1,1);",
        )
        .unwrap();

        let insert_batch = |pr_number: i64,
                            task_id: i64,
                            graph_id: Option<i64>,
                            source_task_id: i64,
                            state: &str| {
            conn.execute(
                "INSERT INTO review_followup_batches(
                     pr_number,task_id,graph_id,source_task_id,collector_version,
                     artifact_count,state,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,'followups-v1',6,?5,1,1)",
                rusqlite::params![pr_number, task_id, graph_id, source_task_id, state],
            )
        };
        insert_batch(100, 1, Some(10), 1, "collected").unwrap();
        conn.execute(
            "UPDATE review_followup_batches SET state='assessing' WHERE pr_number=100",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE review_followup_batches SET state='resolved' WHERE pr_number=100",
            [],
        )
        .unwrap();
        assert!(insert_batch(101, 1, None, 1, "invalid").is_err());
        assert!(insert_batch(102, 999, None, 1, "collected").is_err());
        assert!(insert_batch(103, 1, Some(999), 1, "collected").is_err());
        assert!(insert_batch(104, 1, None, 999, "collected").is_err());

        let insert_artifact = |ordinal: i64,
                               impact: &str,
                               scope: &str,
                               disposition: Option<&str>,
                               linked_task_id: Option<i64>,
                               created_task_id: Option<i64>| {
            conn.execute(
                "INSERT INTO review_followup_artifacts(
                     pr_number,ordinal,technical_impact,scope_relationship,concern,
                     non_blocking_reason,affected_behavior,desired_outcome,
                     verification_expectations,evidence_ids,disposition,
                     disposition_reason,linked_task_id,created_task_id,created_at,updated_at)
                 VALUES (100,?1,?2,?3,'concern','reason','behavior','outcome',
                         '[\"verify\"]','[1]',?4,NULL,?5,?6,1,1)",
                rusqlite::params![
                    ordinal,
                    impact,
                    scope,
                    disposition,
                    linked_task_id,
                    created_task_id
                ],
            )
        };

        for (ordinal, impact, scope, disposition, linked, created) in [
            (0, "critical", "pre_existing", None, None, None),
            (1, "major", "out_of_scope", Some("linked"), Some(2), None),
            (
                2,
                "minor",
                "threat_model_expansion",
                Some("created"),
                None,
                Some(3),
            ),
            (3, "nit", "defense_in_depth", Some("dismissed"), None, None),
            (
                4,
                "critical",
                "future_requirement",
                Some("deferred"),
                None,
                None,
            ),
            (5, "major", "design_debt", None, None, None),
        ] {
            insert_artifact(ordinal, impact, scope, disposition, linked, created).unwrap();
        }

        assert!(insert_artifact(6, "invalid", "pre_existing", None, None, None).is_err());
        assert!(insert_artifact(6, "major", "invalid", None, None, None).is_err());
        assert!(insert_artifact(6, "major", "pre_existing", Some("invalid"), None, None).is_err());
        assert!(insert_artifact(6, "major", "pre_existing", None, Some(2), None).is_err());
        assert!(insert_artifact(6, "major", "pre_existing", Some("linked"), None, None).is_err());
        assert!(insert_artifact(
            6,
            "major",
            "pre_existing",
            Some("created"),
            Some(2),
            Some(3)
        )
        .is_err());
        assert!(
            insert_artifact(6, "major", "pre_existing", Some("dismissed"), None, Some(3)).is_err()
        );
        assert!(
            insert_artifact(6, "major", "pre_existing", Some("linked"), Some(999), None).is_err()
        );
        assert!(insert_artifact(0, "major", "pre_existing", None, None, None).is_err());
        assert!(conn
            .execute(
                "INSERT INTO review_followup_artifacts(
                     pr_number,ordinal,technical_impact,scope_relationship,concern,
                     non_blocking_reason,affected_behavior,desired_outcome,
                     verification_expectations,evidence_ids,created_at,updated_at)
                 VALUES (999,0,'major','pre_existing','c','r','b','o','[]','[]',1,1)",
                [],
            )
            .is_err());
    }

    #[test]
    fn populated_v42_migration_adds_dormant_followups_without_changing_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v42-followups.db");
        {
            let conn = open(&path).unwrap();
            conn.execute_batch(
                "INSERT INTO tasks(id,title,body,status,priority,labels,assignee,created_by,
                     created_at,updated_at,refs,author,reviewer,rework_round,review_only)
                 VALUES (41,'historical task','body','done',7,'[\"old\"]','worker','owner',
                         10,20,'{\"pr\":410}','worker','reviewer',2,0);
                 INSERT INTO review_findings(
                     id,pr_number,task_id,reviewer,kind,author_pushback,pushback_accepted,
                     severity,text,source_endpoint,created_at,addressed_status,evidence_ids,
                     collector_model,collector_version)
                 VALUES (51,410,41,'reviewer','suggestion',1,0,'minor','historical finding',
                         'pulls',30,'unaddressed','[{\"kind\":\"review\",\"id\":1}]',
                         'old-model','old-v1');
                 INSERT INTO review_collection_runs(
                     pr_number,task_id,status,error,collector_model,collector_version,
                     findings_count,attempted_at,completed_at,role_assignment_id)
                 VALUES (410,41,'success',NULL,'old-model','old-v1',1,30,31,NULL);
                 DROP TABLE review_followup_artifacts;
                 DROP TABLE review_followup_batches;
                 ALTER TABLE review_collection_runs DROP COLUMN followup_count;
                 PRAGMA user_version=42;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        assert!(column_exists(&conn, "review_collection_runs", "followup_count").unwrap());
        for table in ["review_followup_batches", "review_followup_artifacts"] {
            assert!(conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                    [table],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap());
        }
        let historical_json = |sql| {
            conn.query_row(sql, [], |row| row.get::<_, String>(0))
                .unwrap()
        };
        assert_eq!(
            historical_json(
                "SELECT json_array(id,title,body,status,priority,labels,assignee,created_by,
                        created_at,updated_at,refs,author,reviewer,rework_round,review_only)
                 FROM tasks WHERE id=41"
            ),
            r#"[41,"historical task","body","done",7,"[\"old\"]","worker","owner",10,20,"{\"pr\":410}","worker","reviewer",2,0]"#
        );
        assert_eq!(
            historical_json(
                "SELECT json_array(id,pr_number,task_id,reviewer,kind,author_pushback,
                        pushback_accepted,severity,text,source_endpoint,created_at,
                        addressed_status,evidence_ids,collector_model,collector_version)
                 FROM review_findings WHERE id=51"
            ),
            r#"[51,410,41,"reviewer","suggestion",1,0,"minor","historical finding","pulls",30,"unaddressed","[{\"kind\":\"review\",\"id\":1}]","old-model","old-v1"]"#
        );
        assert_eq!(
            historical_json(
                "SELECT json_array(pr_number,task_id,status,error,collector_model,
                        collector_version,findings_count,followup_count,attempted_at,
                        completed_at,role_assignment_id)
                 FROM review_collection_runs WHERE pr_number=410"
            ),
            r#"[410,41,"success",null,"old-model","old-v1",1,0,30,31,null]"#
        );
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        drop(conn);

        let reopened = open(&path).unwrap();
        assert_eq!(
            reopened
                .query_row(
                    "SELECT findings_count,followup_count FROM review_collection_runs
                     WHERE pr_number=410",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .unwrap(),
            (1, 0)
        );
    }

    #[test]
    fn populated_v43_migration_adds_assessment_tables_and_enforces_all_authority() {
        fn unique_indexes(conn: &Connection, table: &str) -> Vec<(String, bool, Vec<String>)> {
            let mut stmt = conn
                .prepare("SELECT name,partial FROM pragma_index_list(?1) WHERE \"unique\"=1")
                .unwrap();
            let indexes = stmt
                .query_map([table], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
                })
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            indexes
                .into_iter()
                .map(|(name, partial)| {
                    let columns = conn
                        .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")
                        .unwrap()
                        .query_map([&name], |row| row.get::<_, String>(0))
                        .unwrap()
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .unwrap();
                    (name, partial, columns)
                })
                .collect()
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v43-assessments.db");
        {
            let conn = open(&path).unwrap();
            conn.pragma_update(None, "foreign_keys", true).unwrap();
            conn.execute_batch(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at) VALUES
                     (1,'source one','done','owner',1,1),
                     (2,'source two','done','owner',1,1);
                 INSERT INTO review_followup_batches(
                     pr_number,task_id,source_task_id,collector_version,
                     artifact_count,state,created_at,updated_at)
                 VALUES (100,1,1,'followups-v1',2,'collected',1,1);
                 INSERT INTO review_followup_artifacts(
                     id,pr_number,ordinal,technical_impact,scope_relationship,concern,
                     non_blocking_reason,affected_behavior,desired_outcome,
                     verification_expectations,evidence_ids,created_at,updated_at)
                 VALUES
                     (11,100,0,'major','out_of_scope','one','reason','behavior','outcome',
                      '[\"verify\"]','[{\"kind\":\"review\",\"id\":1}]',1,1),
                     (12,100,1,'minor','design_debt','two','reason','behavior','outcome',
                      '[\"verify\"]','[{\"kind\":\"review\",\"id\":2}]',1,1);
                 DROP TABLE review_followup_assessment_artifacts;
                 DROP TABLE review_followup_assessments;
                 PRAGMA user_version=43;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        conn.pragma_update(None, "foreign_keys", true).unwrap();
        for table in [
            "review_followup_assessments",
            "review_followup_assessment_artifacts",
        ] {
            assert!(conn
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                    [table],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap());
        }

        let assessment_indexes = unique_indexes(&conn, "review_followup_assessments");
        assert!(assessment_indexes
            .iter()
            .any(|(_, partial, columns)| !partial && columns == &["scope_kind", "scope_id"]));
        assert!(assessment_indexes.iter().any(|(name, partial, columns)| {
            name == "one_active_followup_assessment" && *partial && columns == &["target"]
        }));
        let membership_indexes = unique_indexes(&conn, "review_followup_assessment_artifacts");
        assert!(membership_indexes
            .iter()
            .any(|(_, _, columns)| columns == &["artifact_id"]));

        let insert_assessment = |id: i64,
                                 target: &str,
                                 scope_kind: &str,
                                 scope_id: i64,
                                 source_task_id: i64,
                                 active: i64| {
            conn.execute(
                "INSERT INTO review_followup_assessments(
                     id,target,scope_kind,scope_id,source_task_id,state,active,
                     membership_sealed,created_at,updated_at)
                 VALUES (?1,?2,?3,?4,?5,'pending',?6,0,1,1)",
                rusqlite::params![id, target, scope_kind, scope_id, source_task_id, active],
            )
        };
        insert_assessment(21, "shared-authority", "task", 1, 1, 0).unwrap();
        assert!(insert_assessment(22, "another-target", "task", 1, 1, 0).is_err());
        insert_assessment(22, "shared-authority", "graph", 10, 1, 1).unwrap();
        assert!(conn
            .execute(
                "UPDATE review_followup_assessments SET active=1 WHERE id=21",
                [],
            )
            .is_err());
        insert_assessment(23, "followup:task:2", "task", 2, 2, 0).unwrap();
        assert!(insert_assessment(24, "invalid-scope", "repo", 3, 1, 0).is_err());
        assert!(conn
            .execute(
                "INSERT INTO review_followup_assessments(
                     id,target,scope_kind,scope_id,source_task_id,state,active,
                     created_at,updated_at)
                 VALUES (24,'invalid-state','graph',11,1,'retrying',0,1,1)",
                [],
            )
            .is_err());

        conn.execute(
            "INSERT INTO review_followup_assessment_artifacts(assessment_id,artifact_id)
             VALUES (21,11)",
            [],
        )
        .unwrap();
        assert!(conn
            .execute(
                "INSERT INTO review_followup_assessment_artifacts(assessment_id,artifact_id)
                 VALUES (23,11)",
                [],
            )
            .is_err());
        conn.execute(
            "INSERT INTO review_followup_assessment_artifacts(assessment_id,artifact_id)
             VALUES (23,12)",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE review_followup_assessments SET membership_sealed=1
             WHERE id IN (21,23)",
            [],
        )
        .unwrap();
        assert!(conn
            .execute(
                "UPDATE review_followup_assessments SET membership_sealed=0 WHERE id=21",
                [],
            )
            .is_err());
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn populated_v44_migration_seals_existing_membership_and_bounds_counters() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v44-assessment-guards.db");
        {
            let conn = open(&path).unwrap();
            conn.execute_batch(
                "DROP TRIGGER review_followup_membership_no_update;
                 DROP TRIGGER review_followup_membership_no_delete;
                 DROP TRIGGER review_followup_assessment_counter_insert_bound;
                 DROP TRIGGER review_followup_assessment_counter_update_bound;
                 DROP TRIGGER review_followup_membership_insert_unsealed;
                 DROP TRIGGER review_followup_membership_no_unseal;
                 ALTER TABLE review_followup_assessments DROP COLUMN membership_sealed;
                 INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
                 VALUES (1,'source','done','owner',1,1),
                        (2,'other','done','owner',1,1);
                 INSERT INTO review_followup_batches(
                     pr_number,task_id,source_task_id,collector_version,
                     artifact_count,state,created_at,updated_at)
                 VALUES (100,1,1,'followups-v1',1,'collected',1,1),
                        (200,2,2,'followups-v1',1,'collected',1,1);
                 INSERT INTO review_followup_artifacts(
                     id,pr_number,ordinal,technical_impact,scope_relationship,concern,
                     non_blocking_reason,affected_behavior,desired_outcome,
                     verification_expectations,evidence_ids,created_at,updated_at)
                 VALUES
                     (11,100,0,'major','out_of_scope','one','reason','behavior','outcome',
                      '[\"verify\"]','[{\"kind\":\"review\",\"id\":1}]',1,1),
                     (12,200,0,'minor','design_debt','two','reason','behavior','outcome',
                      '[\"verify\"]','[{\"kind\":\"review\",\"id\":2}]',1,1);
                 INSERT INTO review_followup_assessments(
                     id,target,scope_kind,scope_id,source_task_id,state,active,
                     proposal_attempts,provider_failures,created_at,updated_at)
                 VALUES (21,'followup:task:1','task',1,1,'pending',0,2,1,2,2);
                 INSERT INTO review_followup_assessment_artifacts(assessment_id,artifact_id)
                 VALUES (21,11);
                 PRAGMA user_version=44;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT state,membership_sealed,proposal_attempts,provider_failures,
                        (SELECT artifact_id FROM review_followup_assessment_artifacts
                         WHERE assessment_id=21)
                 FROM review_followup_assessments WHERE id=21",
                [],
                |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                )),
            )
            .unwrap(),
            ("pending".into(), 1, 2, 1, 11)
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='trigger' AND name IN (
                     'review_followup_membership_no_update',
                     'review_followup_membership_no_delete',
                     'review_followup_assessment_counter_insert_bound',
                     'review_followup_assessment_counter_update_bound',
                     'review_followup_membership_insert_unsealed',
                     'review_followup_membership_no_unseal')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            6
        );
        assert!(conn
            .execute(
                "UPDATE review_followup_assessment_artifacts
                 SET artifact_id=12 WHERE assessment_id=21",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "DELETE FROM review_followup_assessment_artifacts WHERE assessment_id=21",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "INSERT INTO review_followup_assessment_artifacts(assessment_id,artifact_id)
                 VALUES (21,12)",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "UPDATE review_followup_assessments SET membership_sealed=0 WHERE id=21",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "UPDATE review_followup_assessments SET proposal_attempts=4 WHERE id=21",
                [],
            )
            .is_err());
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn populated_v46_migration_adds_closed_completion_provenance_without_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v46-completion-provenance.db");
        {
            let conn = open(&path).unwrap();
            conn.execute_batch(
                "INSERT INTO tasks(
                     id,title,status,created_by,created_at,updated_at,refs,
                     completion_provenance)
                 VALUES
                     (71,'legacy done with PR','done','owner',1,1,'{\"pr\":701}','merged'),
                     (72,'legacy open','open','owner',1,1,NULL,NULL);
                 ALTER TABLE tasks DROP COLUMN completion_provenance;
                 PRAGMA user_version=46;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        assert!(column_exists(&conn, "tasks", "completion_provenance").unwrap());
        assert_eq!(
            conn.query_row(
                "SELECT completion_provenance FROM tasks WHERE id=71",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap(),
            None,
            "migration must not infer merge provenance from legacy done + refs.pr"
        );
        conn.execute(
            "UPDATE tasks SET completion_provenance='merged' WHERE id=71",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET completion_provenance='manual' WHERE id=72",
            [],
        )
        .unwrap();
        assert!(conn
            .execute(
                "UPDATE tasks SET completion_provenance='unknown' WHERE id=72",
                [],
            )
            .is_err());
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        drop(conn);

        let reopened = open(&path).unwrap();
        assert_eq!(
            reopened
                .query_row(
                    "SELECT json_array(
                         (SELECT completion_provenance FROM tasks WHERE id=71),
                         (SELECT completion_provenance FROM tasks WHERE id=72))",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            r#"["merged","manual"]"#
        );
    }

    #[test]
    fn v47_split_lineages_converge_on_complete_v48_schema() {
        // Main and PR #565 independently used schema version 47: main added
        // completion_provenance, while the PR added the durable-reference
        // indexes later consumed by the rotating task-sweep cursor. Model
        // both deployed shapes and prove either one upgrades to the complete
        // merged schema instead of short-circuiting at the shared version.
        let dir = tempfile::tempdir().unwrap();
        let main_v47 = dir.path().join("main-v47.db");
        {
            let conn = open(&main_v47).unwrap();
            conn.execute_batch(
                "DROP TABLE sweep_cursors;
                 DROP INDEX decomposition_cleanup_task;
                 DROP INDEX review_followup_batches_task;
                 DROP INDEX review_followup_batches_source_task;
                 DROP INDEX review_followup_artifacts_linked_task;
                 DROP INDEX review_followup_artifacts_created_task;
                 DROP INDEX review_followup_assessments_source_task;
                 PRAGMA user_version=47;",
            )
            .unwrap();
            assert!(column_exists(&conn, "tasks", "completion_provenance").unwrap());
        }

        let pr_v47 = dir.path().join("pr-v47.db");
        {
            let conn = open(&pr_v47).unwrap();
            conn.execute_batch(
                "ALTER TABLE tasks DROP COLUMN completion_provenance;
                 PRAGMA user_version=47;",
            )
            .unwrap();
        }

        for path in [&main_v47, &pr_v47] {
            let conn = open(path).unwrap();
            assert!(column_exists(&conn, "tasks", "completion_provenance").unwrap());
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM sqlite_master
                     WHERE type='table' AND name='sweep_cursors'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                1
            );
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM sqlite_master
                     WHERE type='index' AND name IN (
                         'decomposition_cleanup_task',
                         'review_followup_batches_task',
                         'review_followup_batches_source_task',
                         'review_followup_artifacts_linked_task',
                         'review_followup_artifacts_created_task',
                         'review_followup_assessments_source_task')",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                6
            );
            assert_eq!(
                conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                SCHEMA_VERSION
            );
            drop(conn);

            // The converged schema is stable on the normal equal-version path.
            assert_eq!(
                open(path)
                    .unwrap()
                    .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                SCHEMA_VERSION
            );
        }
    }

    #[test]
    fn v38_to_v42_migration_adds_routing_authority_and_nullable_evidence_links() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v38.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE agent_runs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL,
                    agent_name TEXT NOT NULL, role TEXT NOT NULL, model TEXT NOT NULL,
                    effort TEXT NOT NULL, spawned_at INTEGER NOT NULL, ended_at INTEGER,
                    end_reason TEXT, sub_role TEXT, provider TEXT
                 );
                 CREATE TABLE task_decompositions(
                    id INTEGER PRIMARY KEY, active INTEGER NOT NULL DEFAULT 0,
                    freeze_active INTEGER NOT NULL DEFAULT 0,
                    accepted_proposal_json TEXT
                 );
                 CREATE TABLE review_collection_runs(
                    pr_number INTEGER PRIMARY KEY, task_id INTEGER, status TEXT,
                    error TEXT, collector_model TEXT, collector_version TEXT,
                    findings_count INTEGER, attempted_at INTEGER, completed_at INTEGER
                 );
                 INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
                    VALUES (1,'old-worker','worker','old-model','high','codex',2);
                 PRAGMA user_version=38;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        for table in ["role_assignments", "routing_cursors"] {
            assert!(conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                    [table],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap());
        }
        assert!(column_exists(&conn, "agent_runs", "role_assignment_id").unwrap());
        assert!(column_exists(&conn, "task_decompositions", "planner_assignment_id").unwrap());
        assert!(column_exists(&conn, "review_collection_runs", "role_assignment_id").unwrap());
        let historical: (String, Option<i64>) = conn
            .query_row(
                "SELECT model,role_assignment_id FROM agent_runs WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(historical, ("old-model".into(), None));
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn v34_migration_adds_nullable_continue_pr_without_reinterpreting_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v34.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0, labels TEXT, assignee TEXT,
                    created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL, refs TEXT, depends_on TEXT,
                    sticky_until INTEGER, orig TEXT, author TEXT, reviewer TEXT,
                    rework_round INTEGER NOT NULL DEFAULT 0,
                    review_only INTEGER NOT NULL DEFAULT 0,
                    recovery_attempts INTEGER NOT NULL DEFAULT 0
                 );
                 INSERT INTO tasks
                    (title,status,created_by,created_at,updated_at,refs)
                    VALUES ('historical','failed','boss',1,1,'{\"pr\":19}');
                 PRAGMA user_version = 34;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let continue_pr: Option<i64> = conn
            .query_row("SELECT continue_pr FROM tasks WHERE id=1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(continue_pr, None);
        drop(conn);

        let reopened = open(&path).unwrap();
        assert!(column_exists(&reopened, "tasks", "continue_pr").unwrap());
    }

    #[test]
    fn migrates_v35_to_v36_adds_bounded_terminal_retry_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v35.db");
        {
            let conn = Connection::open(&path).unwrap();
            apply_pragmas(&conn).unwrap();
            conn.execute_batch(
                "BEGIN;
                 CREATE TABLE tasks (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     title TEXT NOT NULL,
                     status TEXT NOT NULL,
                     priority INTEGER NOT NULL DEFAULT 0,
                     created_by TEXT NOT NULL,
                     created_at INTEGER NOT NULL,
                     updated_at INTEGER NOT NULL,
                     refs TEXT
                 );
                 INSERT INTO tasks(title,status,created_by,created_at,updated_at,refs)
                     VALUES
                     ('clean terminal','done','boss',1,1,'{\"pr\":1}'),
                     ('corrupt terminal','failed','boss',2,2,
                      '{\"daemon_rework_retry_requested\":true}'),
                     ('malformed refs','cancelled','boss',3,3,'{');
                 PRAGMA user_version = 35;
                 COMMIT;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let index_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type='index'
                   AND name IN ('tasks_terminal_retry_id',
                                'tasks_terminal_retry_recent')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            index_count, 2,
            "v35 database must gain both candidate indexes"
        );

        let predicate = "status IN ('done','failed','cancelled')
             AND json_valid(refs)
             AND (
                 json_type(refs, '$.daemon_rework_retry_requested')='true'
                 OR json_type(refs, '$.daemon_parked_head_check')='true'
                 OR (
                     status IN ('done','cancelled')
                     AND (
                         json_type(refs, '$.daemon_parked') IS NOT NULL
                         OR json_type(refs, '$.daemon_resume_status') IS NOT NULL
                     )
                 )
                 OR (
                     status='failed'
                     AND json_type(refs, '$.daemon_resume_status') IS NOT NULL
                     AND COALESCE(json_extract(refs, '$.daemon_parked'), 0) != 1
                 )
             )";
        let plan = |sql: &str| {
            conn.prepare(sql)
                .unwrap()
                .query_map([], |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .join(" | ")
        };
        let reconcile_plan = plan(&format!(
            "EXPLAIN QUERY PLAN
             SELECT id FROM tasks INDEXED BY tasks_terminal_retry_id
             WHERE {predicate} ORDER BY id LIMIT 8"
        ));
        assert!(
            reconcile_plan.contains("tasks_terminal_retry_id"),
            "reconciliation must use its candidate-only index: {reconcile_plan}"
        );
        assert!(
            !reconcile_plan.contains("USE TEMP B-TREE"),
            "reconciliation must not sort the candidate set: {reconcile_plan}"
        );

        let status_plan = plan(&format!(
            "EXPLAIN QUERY PLAN
             SELECT id,status,updated_at
             FROM tasks INDEXED BY tasks_terminal_retry_recent
             WHERE {predicate}
             ORDER BY updated_at DESC,id DESC LIMIT 10"
        ));
        assert!(
            status_plan.contains("tasks_terminal_retry_recent"),
            "status must use its candidate-only index: {status_plan}"
        );
        assert!(
            !status_plan.contains("USE TEMP B-TREE"),
            "status must not sort the candidate set: {status_plan}"
        );

        let candidates: Vec<i64> = conn
            .prepare(&format!(
                "SELECT id FROM tasks INDEXED BY tasks_terminal_retry_id
                 WHERE {predicate} ORDER BY id"
            ))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            candidates,
            vec![2],
            "clean terminal history stays outside the index"
        );
    }

    #[test]
    fn migrates_split_lineage_v37_adds_task_revision_authority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("split-v37.db");
        {
            let conn = Connection::open(&path).unwrap();
            apply_pragmas(&conn).unwrap();
            conn.execute_batch(
                "BEGIN;
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
                     orig TEXT,
                     author TEXT,
                     reviewer TEXT,
                     rework_round INTEGER NOT NULL DEFAULT 0,
                     review_only INTEGER NOT NULL DEFAULT 0,
                     recovery_attempts INTEGER NOT NULL DEFAULT 0,
                     continue_pr INTEGER
                 );
                 INSERT INTO tasks(
                     title,body,status,priority,created_by,created_at,updated_at,refs
                 ) VALUES ('preserved','body','done',7,'owner',10,11,'{\"pr\":493}');
                 PRAGMA user_version = 37;
                 COMMIT;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        let row: (String, String, i64, i64, i64) = conn
            .query_row(
                "SELECT title,status,priority,revision,edit_count FROM tasks WHERE id=1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(row, ("preserved".into(), "done".into(), 7, 1, 0));
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );

        drop(conn);
        let reopened = open(&path).unwrap();
        assert!(column_exists(&reopened, "tasks", "revision").unwrap());
        assert!(column_exists(&reopened, "tasks", "edit_count").unwrap());
    }

    #[test]
    fn migrates_v39_branch_allocations_with_nullable_provenance() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v39-branch-provenance.db");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE task_branches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL UNIQUE,
                    branch TEXT NOT NULL UNIQUE,
                    worktree TEXT NOT NULL,
                    allocated_by TEXT NOT NULL,
                    allocated_at INTEGER NOT NULL
                 );
                 INSERT INTO task_branches(
                    task_id,branch,worktree,allocated_by,allocated_at)
                 VALUES (7,'daemon/legacy-t7','/tmp/legacy-t7','legacy',10);
                 PRAGMA user_version = 39;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        let row: (i64, String, String, Option<String>) = conn
            .query_row(
                "SELECT task_id,branch,worktree,provenance_sha FROM task_branches WHERE task_id=7",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (7, "daemon/legacy-t7".into(), "/tmp/legacy-t7".into(), None)
        );
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        drop(conn);
        let reopened = open(&path).unwrap();
        assert!(column_exists(&reopened, "task_branches", "provenance_sha").unwrap());
        assert!(reopened
            .query_row(
                "SELECT provenance_sha IS NULL FROM task_branches WHERE task_id=7",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap());
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
            let c = Connection::open(&p).unwrap();
            c.execute_batch(&format!(
                "CREATE TABLE newer_marker(value TEXT NOT NULL);
                 INSERT INTO newer_marker(value) VALUES ('untouched');
                 PRAGMA user_version={};",
                SCHEMA_VERSION + 1
            ))
            .unwrap();
            assert_eq!(
                c.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "delete"
            );
        }
        let bytes_before = std::fs::read(&p).unwrap();
        match open(&p) {
            Err(QuorumError::SchemaTooNew { db, bin }) => {
                assert_eq!(db, SCHEMA_VERSION + 1);
                assert_eq!(bin, SCHEMA_VERSION);
            }
            other => panic!("expected SchemaTooNew, got {other:?}"),
        }
        assert_eq!(
            std::fs::read(&p).unwrap(),
            bytes_before,
            "newer-schema refusal must not modify the database file"
        );
        let c = Connection::open(&p).unwrap();
        assert_eq!(
            c.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "delete",
            "newer-schema refusal must not persistently switch journal mode"
        );
        assert_eq!(
            c.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION + 1
        );
        assert_eq!(
            c.query_row("SELECT value FROM newer_marker", [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
            "untouched"
        );
        for table in [
            "review_followup_assessments",
            "review_followup_assessment_artifacts",
        ] {
            assert!(!c
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                    [table],
                    |row| row.get::<_, bool>(0),
                )
                .unwrap());
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

    #[test]
    fn migrates_v25_to_v26_adds_run_capabilities_table() {
        // Regression guard for the "already-at-latest short-circuit" trap:
        // main shipped SCHEMA_VERSION=25 without `run_capabilities`, so a
        // daemon DB stopped at user_version=25 would silently skip SCHEMA_SQL
        // if this PR reused v25. Landing at v26 forces the migration path.
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE review_interpret_jobs (
                 pr_number INTEGER PRIMARY KEY,
                 task_id INTEGER NOT NULL,
                 repo TEXT,
                 interpreter_version TEXT NOT NULL,
                 attempts INTEGER NOT NULL DEFAULT 0,
                 last_attempt_at INTEGER,
                 last_error TEXT,
                 created_at INTEGER NOT NULL
             );
             PRAGMA user_version = 25;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let raw = Connection::open(&path).unwrap();
        let pre: i64 = raw
            .query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='table' AND name='run_capabilities'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pre, 0, "seed must not include run_capabilities");
        drop(raw);

        // Production path: this is what the daemon does on every open.
        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='table' AND name='run_capabilities'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "run_capabilities must exist after v25→v26 migration \
             — otherwise capability issue/validate raise 'no such table'"
        );
        // The table must be writable via the module API (round-trip proves
        // the CHECK constraint and columns match the code path).
        let mut c = c;
        crate::capabilities::issue(&mut c, "run-v26", 1, "Agent-Upgrade", "worker", 1_000).unwrap();
        let cap = crate::capabilities::validate(&c, "run-v26", "Agent-Upgrade", "worker", Some(1))
            .unwrap();
        assert_eq!(cap.agent, "Agent-Upgrade");
    }

    #[test]
    fn migrates_v26_to_v27_adds_perf_watermark_table() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE run_capabilities (
                 run_id TEXT PRIMARY KEY, task_id INTEGER NOT NULL,
                 agent TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('worker','reviewer')),
                 created_at INTEGER NOT NULL, revoked_at INTEGER
             );
             PRAGMA user_version = 26;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        // Verify table absent before migration.
        let raw = Connection::open(&path).unwrap();
        let pre: i64 = raw
            .query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='table' AND name='perf_watermark'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pre, 0, "seed must not include perf_watermark");
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='table' AND name='perf_watermark'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "perf_watermark must exist after v26→v27 migration");

        // Watermark row seeded with a real timestamp.
        let wm: i64 = c
            .query_row(
                "SELECT watermark FROM perf_watermark WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            wm > 1_700_000_000,
            "watermark should be a recent unix timestamp, got {wm}"
        );
    }

    #[test]
    fn perf_watermark_idempotent_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        let wm1: i64;
        {
            let c = open(&path).unwrap();
            wm1 = c
                .query_row(
                    "SELECT watermark FROM perf_watermark WHERE id = 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
        }
        // Reopen — INSERT OR IGNORE must not overwrite.
        let c = open(&path).unwrap();
        let wm2: i64 = c
            .query_row(
                "SELECT watermark FROM perf_watermark WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(wm1, wm2, "watermark must not change on reopen");
    }

    #[test]
    fn migrates_v28_to_v29_adds_recovery_attempts_column() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE tasks (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
                 priority INTEGER NOT NULL DEFAULT 0, labels TEXT, assignee TEXT,
                 created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, refs TEXT, depends_on TEXT,
                 sticky_until INTEGER, orig TEXT,
                 author TEXT, reviewer TEXT,
                 rework_round INTEGER NOT NULL DEFAULT 0,
                 review_only INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO tasks(title, status, created_by, created_at, updated_at)
                 VALUES ('pre-v29', 'open', 'boss', 100, 100);
             PRAGMA user_version = 28;
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
            column_exists(&c, "tasks", "recovery_attempts").unwrap(),
            "recovery_attempts column missing — v28→v29 migration silently skipped"
        );

        let ra: i64 = c
            .query_row("SELECT recovery_attempts FROM tasks WHERE id=1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(ra, 0, "pre-existing row must default to 0");

        let title: String = c
            .query_row("SELECT title FROM tasks WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(title, "pre-v29");
    }

    #[test]
    fn migrates_v29_to_v30_adds_reviewer_provision_attempts_table() {
        use rusqlite::Connection;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE tasks (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
                 priority INTEGER NOT NULL DEFAULT 0, labels TEXT, assignee TEXT,
                 created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, refs TEXT, depends_on TEXT,
                 sticky_until INTEGER, orig TEXT,
                 author TEXT, reviewer TEXT,
                 rework_round INTEGER NOT NULL DEFAULT 0,
                 review_only INTEGER NOT NULL DEFAULT 0,
                 recovery_attempts INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO tasks(title, status, created_by, created_at, updated_at)
                 VALUES ('pre-v30', 'open', 'boss', 100, 100);
             PRAGMA user_version = 29;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='reviewer_provision_attempts'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "reviewer_provision_attempts table missing after v29→v30 migration"
        );

        let title: String = c
            .query_row("SELECT title FROM tasks WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(title, "pre-v30");
    }

    #[test]
    fn migrates_v31_to_v32_adds_pr_targets_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE tasks (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
                 priority INTEGER NOT NULL DEFAULT 0, labels TEXT, assignee TEXT,
                 created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, refs TEXT, depends_on TEXT,
                 sticky_until INTEGER, orig TEXT,
                 author TEXT, reviewer TEXT,
                 rework_round INTEGER NOT NULL DEFAULT 0,
                 review_only INTEGER NOT NULL DEFAULT 0,
                 recovery_attempts INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO tasks(title, status, created_by, created_at, updated_at)
                 VALUES ('pre-v32', 'open', 'boss', 100, 100);
             PRAGMA user_version = 31;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);

        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='pr_targets'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "pr_targets table missing after v31→v32 migration");

        let title: String = c
            .query_row("SELECT title FROM tasks WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(title, "pre-v32");
    }

    #[test]
    fn migrates_v32_to_v33_adds_daemon_owned_r2_sampling_decisions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");

        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE tasks (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
                 priority INTEGER NOT NULL DEFAULT 0, labels TEXT, assignee TEXT,
                 created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, refs TEXT, depends_on TEXT,
                 sticky_until INTEGER, orig TEXT,
                 author TEXT, reviewer TEXT,
                 rework_round INTEGER NOT NULL DEFAULT 0,
                 review_only INTEGER NOT NULL DEFAULT 0,
                 recovery_attempts INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO tasks(title, status, created_by, created_at, updated_at)
                 VALUES ('pre-v33', 'open', 'boss', 100, 100);
             PRAGMA user_version = 32;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let v: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION);
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master \
                 WHERE type='table' AND name='r2_sampling_decisions'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "r2_sampling_decisions table missing after v32→v33 migration"
        );
    }

    #[test]
    fn migrates_v33_to_v34_adds_reviewing_newest_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE tasks (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL,
                 status TEXT NOT NULL,
                 priority INTEGER NOT NULL DEFAULT 0,
                 created_by TEXT NOT NULL,
                 created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 refs TEXT
             );
             INSERT INTO tasks(title, status, created_by, created_at, updated_at)
                 VALUES ('pre-v34', 'in-review', 'boss', 100, 100);
             PRAGMA user_version = 33;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let c = open(&path).unwrap();
        let version: i64 = c
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let index: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_master \
                 WHERE type='index' AND name='tasks_reviewing_newest'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(index, 1, "v33 database must gain the REVIEWING index");

        let plan = c
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT id, title, status, refs FROM (
                     SELECT id, title, status, refs, updated_at FROM (
                         SELECT id, title, status, refs, updated_at FROM tasks
                         WHERE status='in-review'
                         ORDER BY updated_at DESC, id DESC LIMIT 20
                     )
                     UNION ALL
                     SELECT id, title, status, refs, updated_at FROM (
                         SELECT id, title, status, refs, updated_at FROM tasks
                         WHERE status='merging'
                         ORDER BY updated_at DESC, id DESC LIMIT 20
                     )
                 )
                 ORDER BY updated_at DESC, id DESC LIMIT 20",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert_eq!(
            plan.matches("tasks_reviewing_newest").count(),
            2,
            "plan: {plan}"
        );
    }

    #[test]
    fn migrates_v34_to_v35_adds_decomposition_authority_without_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        let raw = Connection::open(&path).unwrap();
        apply_pragmas(&raw).unwrap();
        raw.execute_batch(
            "BEGIN;
             CREATE TABLE tasks (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 title TEXT NOT NULL, body TEXT, status TEXT NOT NULL,
                 priority INTEGER NOT NULL DEFAULT 0, labels TEXT, assignee TEXT,
                 created_by TEXT NOT NULL, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, refs TEXT, depends_on TEXT,
                 sticky_until INTEGER, orig TEXT, author TEXT, reviewer TEXT,
                 rework_round INTEGER NOT NULL DEFAULT 0,
                 review_only INTEGER NOT NULL DEFAULT 0,
                 recovery_attempts INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                 VALUES ('existing', 'open', 'owner', 100, 100);
             PRAGMA user_version = 34;
             COMMIT;",
        )
        .unwrap();
        drop(raw);

        let conn = open(&path).unwrap();
        let task: (i64, i64) = conn
            .query_row(
                "SELECT revision,edit_count FROM tasks WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(task, (1, 0));
        for table in [
            "task_decompositions",
            "task_graph_members",
            "decomposition_attempts",
            "decomposition_cleanup",
            "reviewer_provision_reservations",
        ] {
            let present: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(present, "{table} missing after v34 migration");
        }
        assert!(column_exists(&conn, "task_decompositions", "accepted_proposal_json").unwrap());
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn populated_v36_to_v37_preserves_graph_and_matches_proposal_write_bound() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v36.db");
        {
            let conn = open(&path).unwrap();
            conn.execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                 VALUES ('large','open','owner',1,1)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO task_decompositions(
                    source_task_id,state,freeze_active,planned_source_revision,
                    planner_provider,planner_model,created_at,updated_at)
                 VALUES (1,'planning',1,1,'claude','opus',2,2)",
                [],
            )
            .unwrap();
        }
        {
            let raw = Connection::open(&path).unwrap();
            raw.execute_batch(
                "ALTER TABLE task_decompositions DROP COLUMN accepted_proposal_json;
                 PRAGMA user_version=36;",
            )
            .unwrap();
        }

        let mut upgraded = open(&path).unwrap();
        assert!(column_exists(&upgraded, "task_decompositions", "accepted_proposal_json").unwrap());
        let preserved: (String, String, String, i64) = upgraded
            .query_row(
                "SELECT state,planner_provider,planner_model,planned_source_revision
                 FROM task_decompositions WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            preserved,
            ("planning".into(), "claude".into(), "opus".into(), 1)
        );

        let exact = format!("\"{}\"", "a".repeat(65_534));
        assert_eq!(exact.len(), 65_536);
        assert!(crate::decomposition::accept_proposal(&mut upgraded, 1, &exact, 3).unwrap());
        upgraded
            .execute(
                "UPDATE task_decompositions SET state='planning',accepted_proposal_json=NULL WHERE id=1",
                [],
            )
            .unwrap();
        let over = format!("\"{}\"", "é".repeat(32_768));
        assert!(over.len() > 65_536);
        assert!(crate::decomposition::accept_proposal(&mut upgraded, 1, &over, 4).is_err());
        drop(upgraded);

        let reopened = open(&path).unwrap();
        assert_eq!(
            reopened
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        drop(reopened);

        let newer_path = dir.path().join("newer.db");
        let newer = Connection::open(&newer_path).unwrap();
        newer
            .execute_batch(&format!("PRAGMA user_version={}", SCHEMA_VERSION + 1))
            .unwrap();
        drop(newer);
        assert!(matches!(
            open(&newer_path),
            Err(QuorumError::SchemaTooNew { .. })
        ));
    }

    #[test]
    fn populated_v40_to_v41_adds_immutable_review_launch_authority_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v40-review-launch.db");
        {
            let conn = open(&path).unwrap();
            conn.execute(
                "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
                 VALUES (7,'historical','reviewer','model','high','codex',1)",
                [],
            )
            .unwrap();
            conn.execute_batch(
                "DROP INDEX IF EXISTS agent_runs_review_cap;
                 ALTER TABLE agent_runs DROP COLUMN review_cap_run_id;
                 ALTER TABLE agent_runs DROP COLUMN review_pr;
                 ALTER TABLE agent_runs DROP COLUMN review_head_sha;
                 PRAGMA user_version=40;",
            )
            .unwrap();
        }
        let conn = open(&path).unwrap();
        for column in ["review_cap_run_id", "review_pr", "review_head_sha"] {
            assert!(column_exists(&conn, "agent_runs", column).unwrap());
        }
        assert!(
            crate::agent_runs::review_launch_for_capability(&conn, "historical")
                .unwrap()
                .is_none()
        );
        let first =
            crate::agent_runs::insert(&conn, 7, "R1", "reviewer", "model", "high", "codex", 2)
                .unwrap();
        assert!(crate::agent_runs::bind_review_launch(
            &conn,
            first,
            "cap",
            71,
            "0123456789abcdef0123456789abcdef01234567"
        )
        .unwrap());
        let second =
            crate::agent_runs::insert(&conn, 8, "R2", "reviewer", "model", "high", "codex", 3)
                .unwrap();
        assert!(crate::agent_runs::bind_review_launch(
            &conn,
            second,
            "cap",
            72,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        .is_err());
        drop(conn);
        let reopened = open(&path).unwrap();
        assert_eq!(
            reopened
                .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(
            crate::agent_runs::review_launch_for_capability(&reopened, "cap")
                .unwrap()
                .unwrap()
                .pr,
            71
        );
    }

    /// Task #473: rows parked before this binary shipped carry the legacy
    /// `terminal-not-done` reason and lack the new `daemon_parked_unsatisfiable`
    /// bit, so `stats::blocked_tasks` (which requires the bit) would hide the
    /// exact operator disposition queue the task was opened to surface. The
    /// v50 migration must backfill both refs onto the pre-existing rows,
    /// leave recoverable failed-dep parks untouched, and be idempotent.
    #[test]
    fn migrates_v49_to_v50_backfills_cancelled_dependency_park_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v49-backfill.db");
        {
            let conn = open(&path).unwrap();
            conn.execute_batch(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
                     VALUES
                         (1,'cancelled-dep','cancelled','owner',1,1),
                         (2,'failed-dep','failed','owner',1,1),
                         (3,'legacy park cancelled','failed','owner',1,1),
                         (4,'legacy park failed only','failed','owner',1,1),
                         (5,'legacy park no deps','failed','owner',1,1),
                         (6,'open with cancelled dep','open','owner',1,1),
                         (7,'v49 policy park cancelled dep','failed','owner',1,1);
                 UPDATE tasks SET depends_on='[1,2]',
                     refs='{\"daemon_parked\": true,
                             \"daemon_parked_reason\": \"dependency #1 is terminal-not-done\",
                             \"daemon_resume_status\": \"open\"}'
                     WHERE id=3;
                 UPDATE tasks SET depends_on='[2]',
                     refs='{\"daemon_parked\": true,
                             \"daemon_parked_reason\": \"dependency #2 is terminal-not-done\",
                             \"daemon_resume_status\": \"open\"}'
                     WHERE id=4;
                 UPDATE tasks SET refs='{\"daemon_parked\": true,
                             \"daemon_parked_reason\": \"other reason\",
                             \"daemon_resume_status\": \"open\"}'
                     WHERE id=5;
                 UPDATE tasks SET depends_on='[1]' WHERE id=6;
                 UPDATE tasks SET depends_on='[1]',
                     refs='{\"daemon_parked\": true,
                             \"daemon_parked_reason\": \"classifier declined\",
                             \"daemon_resume_status\": \"open\",
                             \"classifier_policy_parked\": true}'
                     WHERE id=7;
                 PRAGMA user_version=49;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let refs3: String = conn
            .query_row("SELECT refs FROM tasks WHERE id=3", [], |r| r.get(0))
            .unwrap();
        let v3: serde_json::Value = serde_json::from_str(&refs3).unwrap();
        assert_eq!(v3["daemon_parked_unsatisfiable"], true);
        assert_eq!(
            v3["daemon_parked_reason"],
            "dependency #1 is cancelled — unsatisfiable"
        );

        // Recoverable failed-only park stays untouched: no marker, original
        // terminal-not-done reason preserved so operators still see the
        // recoverable text.
        let refs4: String = conn
            .query_row("SELECT refs FROM tasks WHERE id=4", [], |r| r.get(0))
            .unwrap();
        let v4: serde_json::Value = serde_json::from_str(&refs4).unwrap();
        assert!(v4.get("daemon_parked_unsatisfiable").is_none());
        assert_eq!(
            v4["daemon_parked_reason"],
            "dependency #2 is terminal-not-done"
        );

        // Parks without any depends_on aren't touched.
        let refs5: String = conn
            .query_row("SELECT refs FROM tasks WHERE id=5", [], |r| r.get(0))
            .unwrap();
        let v5: serde_json::Value = serde_json::from_str(&refs5).unwrap();
        assert!(v5.get("daemon_parked_unsatisfiable").is_none());
        assert_eq!(v5["daemon_parked_reason"], "other reason");

        // Open (non-failed) rows aren't touched by the parked-only backfill;
        // the cascade will park them via the new production path.
        let refs6: Option<String> = conn
            .query_row("SELECT refs FROM tasks WHERE id=6", [], |r| r.get(0))
            .unwrap();
        assert!(refs6.is_none());

        // Classifier-policy parks with a cancelled dep are NOT relabeled:
        // the migration and the runtime convergence pass must apply one
        // coherent rule, and policy parks own their own lifecycle.
        let refs7: String = conn
            .query_row("SELECT refs FROM tasks WHERE id=7", [], |r| r.get(0))
            .unwrap();
        let v7: serde_json::Value = serde_json::from_str(&refs7).unwrap();
        assert!(v7.get("daemon_parked_unsatisfiable").is_none());
        assert_eq!(v7["daemon_parked_reason"], "classifier declined");
        assert_eq!(v7["classifier_policy_parked"], true);

        // Idempotent: re-opening from the migrated DB does not double-write.
        drop(conn);
        let reopened = open(&path).unwrap();
        let refs3_again: String = reopened
            .query_row("SELECT refs FROM tasks WHERE id=3", [], |r| r.get(0))
            .unwrap();
        assert_eq!(refs3_again, refs3);
    }

    #[test]
    fn migrates_colliding_v50_to_v52_with_backfill_queue_and_journal_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("colliding-v50.db");
        {
            let conn = open(&path).unwrap();
            conn.execute_batch(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
                     VALUES
                         (1,'cancelled','cancelled','owner',1,1),
                         (2,'legacy park','failed','owner',1,1);
                 UPDATE tasks SET depends_on='[1]',
                     refs='{\"daemon_parked\": true,
                             \"daemon_parked_reason\": \"dependency #1 is terminal-not-done\",
                             \"daemon_resume_status\": \"open\"}'
                     WHERE id=2;
                 DROP TABLE cancelled_dependency_reconciliation;
                 PRAGMA user_version=50;",
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let refs: String = conn
            .query_row("SELECT refs FROM tasks WHERE id=2", [], |row| row.get(0))
            .unwrap();
        let refs: serde_json::Value = serde_json::from_str(&refs).unwrap();
        assert_eq!(refs["daemon_parked_unsatisfiable"], true);
        for column in ["provider", "continuation_id", "local_branch"] {
            assert!(column_exists(&conn, "journal", column).unwrap());
        }
        conn.execute(
            "INSERT INTO cancelled_dependency_reconciliation(
                 cancelled_task_id,task_cursor,updated_at
             ) VALUES (1,64,2)",
            [],
        )
        .unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT task_cursor FROM cancelled_dependency_reconciliation
                 WHERE cancelled_task_id=1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            64
        );
    }
}
