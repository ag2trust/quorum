//! Durable, daemon-internal branch synchronization requests.
//!
//! A branch sync is deliberately not a task on its clean path. This module
//! owns the small persistent state machine boundary: one active row per
//! directed branch pair, and compare-and-set phase advancement for the daemon
//! executor added later.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;

const COLS: &str = "id, source_branch, target_branch, source_sha, target_sha, sync_branch, \
                    merge_sha, pr, phase, task_id, active, requested_by, last_error, \
                    created_at, updated_at";

/// Terminal branch-sync phases release the active-pair slot in the statement
/// that records the phase, allowing a subsequent request for that pair.
pub fn is_terminal_phase(phase: &str) -> bool {
    matches!(
        phase,
        "done" | "noop" | "conflict" | "ci_failed" | "failed" | "cancelled"
    )
}

/// The complete closed vocabulary persisted in `branch_syncs.phase`.
pub fn is_valid_phase(phase: &str) -> bool {
    matches!(
        phase,
        "requested"
            | "pinned"
            | "prepared"
            | "published"
            | "checks"
            | "merging"
            | "done"
            | "noop"
            | "conflict"
            | "ci_failed"
            | "failed"
            | "cancelled"
    )
}

/// Durable synchronization state.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BranchSync {
    pub id: i64,
    pub source_branch: String,
    pub target_branch: String,
    pub source_sha: Option<String>,
    pub target_sha: Option<String>,
    pub sync_branch: Option<String>,
    pub merge_sha: Option<String>,
    pub pr: Option<i64>,
    pub phase: String,
    pub task_id: Option<i64>,
    pub active: bool,
    pub requested_by: String,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

fn row_to_branch_sync(row: &Row<'_>) -> rusqlite::Result<BranchSync> {
    Ok(BranchSync {
        id: row.get(0)?,
        source_branch: row.get(1)?,
        target_branch: row.get(2)?,
        source_sha: row.get(3)?,
        target_sha: row.get(4)?,
        sync_branch: row.get(5)?,
        merge_sha: row.get(6)?,
        pr: row.get(7)?,
        phase: row.get(8)?,
        task_id: row.get(9)?,
        active: row.get(10)?,
        requested_by: row.get(11)?,
        last_error: row.get(12)?,
        created_at: row.get(13)?,
        updated_at: row.get(14)?,
    })
}

/// Result of an atomic request attempt. `AlreadyActive` is an expected clean
/// negative for the CLI (exit 1), never an error or an `errors` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestOutcome {
    Requested(BranchSync),
    AlreadyActive(BranchSync),
}

fn validate_pair(from: &str, to: &str) -> Result<()> {
    crate::tasks::validate_target_branch(from)?;
    crate::tasks::validate_target_branch(to)?;
    if from == to {
        return Err(QuorumError::Usage(
            "branch-sync --from and --to must differ".into(),
        ));
    }
    Ok(())
}

/// Atomically enqueue a branch synchronization request.
///
/// The `BEGIN IMMEDIATE` transaction serializes concurrent writers, while the
/// partial unique index is the final cross-process backstop. A UNIQUE violation
/// is reread under that lock and returned as `AlreadyActive`.
pub fn request(
    conn: &mut Connection,
    from: &str,
    to: &str,
    by: &str,
    now: i64,
) -> Result<RequestOutcome> {
    validate_pair(from, to)?;
    if by.is_empty() || by.contains('\0') {
        return Err(QuorumError::Usage(
            "branch-sync --by must not be empty or contain NUL".into(),
        ));
    }

    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, by, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    match tx.execute(
        "INSERT INTO branch_syncs(
             source_branch, target_branch, phase, active, requested_by, created_at, updated_at
         ) VALUES (?1, ?2, 'requested', 1, ?3, ?4, ?4)",
        params![from, to, by, now],
    ) {
        Ok(_) => {
            let id = tx.last_insert_rowid();
            let sync = tx.query_row(
                &format!("SELECT {COLS} FROM branch_syncs WHERE id=?1"),
                [id],
                row_to_branch_sync,
            )?;
            crate::events::emit(
                &tx,
                "branch_sync_requested",
                &format!("branch_sync#{id}"),
                &format!("{from} -> {to} requested by {by}"),
                now,
            )?;
            tx.commit().map_err(map_sql_err)?;
            Ok(RequestOutcome::Requested(sync))
        }
        Err(ref error) if crate::claims::is_unique_violation_pub(error) => {
            let active = active_for_pair(&tx, from, to)?.ok_or_else(|| {
                QuorumError::Db(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE),
                    Some("branch sync unique race had no active row".into()),
                ))
            })?;
            tx.commit().map_err(map_sql_err)?;
            Ok(RequestOutcome::AlreadyActive(active))
        }
        Err(error) => Err(map_sql_err(error)),
    }
}

/// Compare-and-set a phase. A stale expected phase (or a terminal row) returns
/// `None`; callers must reread rather than overwrite newer executor progress.
/// Terminal phases clear `active` in this same UPDATE statement.
pub fn set_phase(
    conn: &mut Connection,
    id: i64,
    expected_phase: &str,
    next_phase: &str,
    now: i64,
) -> Result<Option<BranchSync>> {
    if !is_valid_phase(expected_phase) || !is_valid_phase(next_phase) {
        return Err(QuorumError::Usage("invalid branch sync phase".into()));
    }
    let tx = begin_immediate(conn)?;
    let terminal = i64::from(is_terminal_phase(next_phase));
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase=?1,
                     active=CASE WHEN ?2=1 THEN 0 ELSE active END,
                     updated_at=?3
                 WHERE id=?4 AND phase=?5 AND active=1
                 RETURNING {COLS}"
            ),
            params![next_phase, terminal, now, id, expected_phase],
            row_to_branch_sync,
        )
        .optional()?;
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Read the active synchronization for one directed branch pair.
pub fn active_for_pair(conn: &Connection, from: &str, to: &str) -> Result<Option<BranchSync>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {COLS} FROM branch_syncs
                 WHERE source_branch=?1 AND target_branch=?2 AND active=1"
            ),
            params![from, to],
            row_to_branch_sync,
        )
        .optional()?)
}

/// Read one branch synchronization row, including terminal history.
pub fn get(conn: &Connection, id: i64) -> Result<Option<BranchSync>> {
    Ok(conn
        .query_row(
            &format!("SELECT {COLS} FROM branch_syncs WHERE id=?1"),
            [id],
            row_to_branch_sync,
        )
        .optional()?)
}

/// List every active synchronization in creation order.
pub fn list_active(conn: &Connection) -> Result<Vec<BranchSync>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM branch_syncs WHERE active=1 ORDER BY created_at ASC, id ASC"
    ))?;
    let syncs = stmt
        .query_map([], row_to_branch_sync)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(syncs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("quorum.db")).unwrap();
        (dir, conn)
    }

    fn requested(outcome: RequestOutcome) -> BranchSync {
        match outcome {
            RequestOutcome::Requested(sync) => sync,
            RequestOutcome::AlreadyActive(_) => panic!("expected a new request"),
        }
    }

    #[test]
    fn second_active_request_is_a_clean_negative() {
        let (_dir, mut conn) = open_tmp();
        let first = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        let second = request(&mut conn, "main", "develop", "B", 101).unwrap();
        assert_eq!(second, RequestOutcome::AlreadyActive(first.clone()));
        assert_eq!(list_active(&conn).unwrap(), vec![first]);
        let errors: i64 = conn
            .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
            .unwrap();
        assert_eq!(errors, 0);
    }

    #[test]
    fn concurrent_requests_have_exactly_one_active_winner() {
        // Separate connections contend on the same real WAL database. This is
        // the same SQLite file-locking path as separate CLI processes.
        for round in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("quorum.db");
            drop(crate::db::open(&path).unwrap());
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(12));
            let handles = (0..12)
                .map(|agent| {
                    let path = path.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        let mut conn = crate::db::open(&path).unwrap();
                        barrier.wait();
                        request(
                            &mut conn,
                            "main",
                            "develop",
                            &format!("agent-{round}-{agent}"),
                            100 + round,
                        )
                        .unwrap()
                    })
                })
                .collect::<Vec<_>>();
            let outcomes = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, RequestOutcome::Requested(_)))
                    .count(),
                1,
                "round {round} must have exactly one winner"
            );

            let conn = crate::db::open(&path).unwrap();
            assert_eq!(list_active(&conn).unwrap().len(), 1);
            let errors: i64 = conn
                .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
                .unwrap();
            assert_eq!(errors, 0, "round {round} clean losers must not log errors");
        }
    }

    #[test]
    fn terminal_done_releases_the_pair_for_a_new_request() {
        let (_dir, mut conn) = open_tmp();
        let first = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        let done = set_phase(&mut conn, first.id, "requested", "done", 101)
            .unwrap()
            .expect("current phase must advance");
        assert_eq!(done.phase, "done");
        assert!(!done.active, "terminal update must release the pair");
        assert!(active_for_pair(&conn, "main", "develop").unwrap().is_none());

        let second = requested(request(&mut conn, "main", "develop", "B", 102).unwrap());
        assert!(second.id > first.id);
        assert_eq!(second.phase, "requested");
    }

    #[test]
    fn stale_expected_phase_returns_no_row() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        assert!(set_phase(&mut conn, sync.id, "requested", "pinned", 101)
            .unwrap()
            .is_some());
        assert!(set_phase(&mut conn, sync.id, "requested", "prepared", 102)
            .unwrap()
            .is_none());
        assert_eq!(get(&conn, sync.id).unwrap().unwrap().phase, "pinned");
    }
}
