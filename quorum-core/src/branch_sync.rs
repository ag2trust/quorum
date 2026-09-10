//! Durable, daemon-internal branch synchronization requests.
//!
//! A branch sync is deliberately not a task on its clean path. This module
//! owns the small persistent state machine boundary: one active row per
//! directed branch pair, and compare-and-set phase advancement for the daemon
//! executor added later.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension, Row};
use serde::Serialize;

const COLS: &str = "id, source_branch, target_branch, source_sha, target_sha, sync_branch, \
                    merge_sha, pr, phase, ci_attempts, ci_next_attempt_at, ci_wait_inflight, \
                    task_id, active, requested_by, last_error, created_at, updated_at";

/// Terminal branch-sync phases release the active-pair slot in the statement
/// that records the phase, allowing a subsequent request for that pair.
pub fn is_terminal_phase(phase: &str) -> bool {
    matches!(phase, "done" | "noop" | "failed" | "cancelled")
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

/// Whether `next_phase` is a legal one-way transition from `phase`.
///
/// The clean path advances one step at a time. Outcome terminals are admitted
/// only where their underlying operation occurs; `failed` and `cancelled` may
/// end any active phase. This prevents a restarted executor from replaying or
/// skipping durable work after it has observed a current row.
pub fn is_valid_transition(phase: &str, next_phase: &str) -> bool {
    matches!(
        (phase, next_phase),
        ("requested", "pinned")
            | ("pinned", "prepared" | "noop" | "conflict")
            | ("prepared", "published")
            | ("published", "checks")
            | ("checks", "merging" | "ci_failed")
            | ("merging", "done" | "conflict")
    ) || (!is_terminal_phase(phase) && matches!(next_phase, "failed" | "cancelled"))
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
    /// Durable count of full CI wait attempts admitted for this checks phase.
    pub ci_attempts: i64,
    /// Earliest unix timestamp at which a timed-out CI gate may be retried.
    pub ci_next_attempt_at: Option<i64>,
    /// A prior daemon admitted a CI wait but did not settle it before stopping.
    /// Restart may recover that one uncertain wait without spending another
    /// retry from the durable budget.
    pub ci_wait_inflight: bool,
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
        ci_attempts: row.get(9)?,
        ci_next_attempt_at: row.get(10)?,
        ci_wait_inflight: row.get(11)?,
        task_id: row.get(12)?,
        active: row.get(13)?,
        requested_by: row.get(14)?,
        last_error: row.get(15)?,
        created_at: row.get(16)?,
        updated_at: row.get(17)?,
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
    if !is_valid_transition(expected_phase, next_phase) {
        return Err(QuorumError::Usage(format!(
            "invalid branch sync transition: {expected_phase} -> {next_phase}"
        )));
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

/// Select one active row that still has daemon-owned clean-path work. A
/// conflict or CI failure remains active for its later judgment path, but must
/// not monopolize a daemon tick.
pub fn next_clean_path(conn: &Connection) -> Result<Option<BranchSync>> {
    next_clean_path_excluding_at(conn, &[], None, i64::MAX)
}

/// Select one runnable clean-path row while excluding in-flight external
/// operations owned by the daemon's in-memory coordinator. Exclusion is only
/// a scheduling concern: the durable `phase` remains the authority after a
/// restart, when no stale process-local handle survives.
pub fn next_clean_path_excluding(
    conn: &Connection,
    excluded_ids: &[i64],
) -> Result<Option<BranchSync>> {
    next_clean_path_excluding_at(conn, excluded_ids, None, i64::MAX)
}

/// Select one durable row due at `now`, optionally limiting `checks` selection
/// to the rows whose in-memory waits have already been admitted. The latter is
/// the global waiter-cap gate: it leaves excess durable rows untouched until a
/// retained waiter settles, rather than queueing an unbounded blocking task.
pub fn next_clean_path_excluding_at(
    conn: &Connection,
    excluded_ids: &[i64],
    admitted_check_ids: Option<&[i64]>,
    now: i64,
) -> Result<Option<BranchSync>> {
    let mut sql = format!(
        "SELECT {COLS} FROM branch_syncs
         WHERE active=1
           AND phase IN ('requested','pinned','prepared','published','checks','merging')
           AND (phase <> 'checks' OR ci_next_attempt_at IS NULL OR ci_next_attempt_at <= ?)"
    );
    let mut values = vec![now];
    if !excluded_ids.is_empty() {
        let placeholders = std::iter::repeat_n("?", excluded_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!(" AND id NOT IN ({placeholders})"));
        values.extend(excluded_ids);
    }
    if let Some(ids) = admitted_check_ids {
        debug_assert!(!ids.is_empty());
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!(
            " AND (phase <> 'checks' OR id IN ({placeholders}))"
        ));
        values.extend(ids);
    }
    sql.push_str(
        " ORDER BY CASE WHEN phase IN ('published','checks','merging') THEN 1 ELSE 0 END,
                  updated_at ASC, id ASC
          LIMIT 1",
    );
    Ok(conn
        .query_row(&sql, params_from_iter(values), row_to_branch_sync)
        .optional()?)
}

/// Admit the CI gate after the published PR's immutable head/base have been
/// revalidated. This is a separate durable boundary: checks must never be
/// queried with merge authority still implicit in `published`.
pub fn begin_checks(conn: &mut Connection, id: i64, now: i64) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='checks',ci_attempts=0,ci_next_attempt_at=NULL,ci_wait_inflight=0,
                     updated_at=?1
                 WHERE id=?2 AND phase='published' AND active=1
                 RETURNING {COLS}"
            ),
            params![now, id],
            row_to_branch_sync,
        )
        .optional()?;
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Cross the durable CI-wait admission boundary. A restarted daemon retains
/// an already-admitted uncertain wait without incrementing the finite retry
/// budget a second time; all ordinary starts increment it before spawning the
/// remote poll.
pub fn admit_check_wait(
    conn: &mut Connection,
    id: i64,
    max_attempts: i64,
    now: i64,
) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET ci_attempts=CASE WHEN ci_wait_inflight=1 THEN ci_attempts
                                      ELSE ci_attempts+1 END,
                     ci_next_attempt_at=NULL,ci_wait_inflight=1,updated_at=?1
                 WHERE id=?2 AND phase='checks' AND active=1
                   AND (ci_next_attempt_at IS NULL OR ci_next_attempt_at <= ?1)
                   AND (ci_wait_inflight=1 OR ci_attempts < ?3)
                 RETURNING {COLS}"
            ),
            params![now, id, max_attempts],
            row_to_branch_sync,
        )
        .optional()?;
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Persist the bounded retry schedule after one admitted CI wait remains
/// unresolved. This clears the in-flight marker before the next daemon can
/// select the row, so a restart honors both the count and cadence.
pub fn schedule_check_retry(
    conn: &mut Connection,
    id: i64,
    next_attempt_at: i64,
    now: i64,
) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET ci_next_attempt_at=?1,ci_wait_inflight=0,updated_at=?2
                 WHERE id=?3 AND phase='checks' AND active=1 AND ci_wait_inflight=1
                 RETURNING {COLS}"
            ),
            params![next_attempt_at, now, id],
            row_to_branch_sync,
        )
        .optional()?;
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Record a failed branch-sync CI gate without releasing the pair. A later
/// daemon-owned judgment task consumes this row, so treating it as a terminal
/// request would permit a second sync to race its remediation.
pub fn ci_failed(
    conn: &mut Connection,
    id: i64,
    detail: &str,
    now: i64,
) -> Result<Option<BranchSync>> {
    let detail = bounded_error(detail);
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='ci_failed',last_error=?1,ci_next_attempt_at=NULL,
                     ci_wait_inflight=0,updated_at=?2
                 WHERE id=?3 AND phase='checks' AND active=1
                 RETURNING {COLS}"
            ),
            params![detail, now, id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_ci_failed",
            &format!("branch_sync#{}", row.id),
            &detail,
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Cross the durable uncertainty boundary immediately before the daemon makes
/// its one branch-sync merge call. A crash after this transition is reconciled
/// from the pinned head/base evidence rather than replaying a blind merge.
pub fn begin_merge_attempt(conn: &mut Connection, id: i64, now: i64) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='merging',ci_next_attempt_at=NULL,ci_wait_inflight=0,updated_at=?1
                 WHERE id=?2 AND phase='checks' AND active=1
                 RETURNING {COLS}"
            ),
            params![now, id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_merge_attempt_started",
            &format!("branch_sync#{}", row.id),
            "branch-sync merge call admitted by daemon",
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Settle a verified GitHub merge. `merge_commit_sha` is the immutable remote
/// witness, intentionally distinct from the locally prepared `merge_sha`.
pub fn complete_merge(
    conn: &mut Connection,
    id: i64,
    merge_commit_sha: &str,
    now: i64,
) -> Result<Option<BranchSync>> {
    let merge_commit_sha = bounded_error(merge_commit_sha);
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='done',active=0,updated_at=?1
                 WHERE id=?2 AND phase='merging' AND active=1
                 RETURNING {COLS}"
            ),
            params![now, id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_merged",
            &format!("branch_sync#{}", row.id),
            &format!(
                "{} -> {} merged as {}",
                row.source_branch, row.target_branch, merge_commit_sha
            ),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Record one successful published-PR reconciliation so that multiple live
/// published rows share the bounded observation slot instead of the oldest
/// row being checked forever.
pub fn touch_published(conn: &mut Connection, id: i64, now: i64) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs SET updated_at=?1
                 WHERE id=?2 AND phase='published' AND active=1
                 RETURNING {COLS}"
            ),
            params![now, id],
            row_to_branch_sync,
        )
        .optional()?;
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Store the immutable remote tips and deterministic local branch name in the
/// same compare-and-set transition that makes the request pinned.
pub fn pin(
    conn: &mut Connection,
    id: i64,
    source_sha: &str,
    target_sha: &str,
    sync_branch: &str,
    now: i64,
) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET source_sha=?1,target_sha=?2,sync_branch=?3,phase='pinned',updated_at=?4
                 WHERE id=?5 AND phase='requested' AND active=1
                 RETURNING {COLS}"
            ),
            params![source_sha, target_sha, sync_branch, now, id],
            row_to_branch_sync,
        )
        .optional()?;
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Record the exact local merge commit after a clean pinned merge.
pub fn prepared(
    conn: &mut Connection,
    id: i64,
    sync_branch: &str,
    merge_sha: &str,
    now: i64,
) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET sync_branch=?1,merge_sha=?2,phase='prepared',updated_at=?3
                 WHERE id=?4 AND phase='pinned' AND active=1
                 RETURNING {COLS}"
            ),
            params![sync_branch, merge_sha, now, id],
            row_to_branch_sync,
        )
        .optional()?;
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Set a no-op terminal and emit its durable operator event.
pub fn noop(conn: &mut Connection, id: i64, now: i64) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='noop',active=0,updated_at=?1
                 WHERE id=?2 AND phase='pinned' AND active=1
                 RETURNING {COLS}"
            ),
            params![now, id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_noop",
            &format!("branch_sync#{}", row.id),
            &format!(
                "{} -> {} already contains pinned source",
                row.source_branch, row.target_branch
            ),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Keep a conflict active for the later judgment-required path and preserve
/// the in-progress worktree merge as its durable external evidence.
pub fn conflict(conn: &mut Connection, id: i64, now: i64) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='conflict',updated_at=?1
                 WHERE id=?2 AND phase='pinned' AND active=1
                 RETURNING {COLS}"
            ),
            params![now, id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_conflict",
            &format!("branch_sync#{}", row.id),
            &format!(
                "{} -> {} requires conflict resolution",
                row.source_branch, row.target_branch
            ),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Bind the published PR to the prepared merge SHA before later CI/merge work
/// receives authority.
pub fn published(conn: &mut Connection, id: i64, pr: i64, now: i64) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET pr=?1,phase='published',updated_at=?2
                 WHERE id=?3 AND phase='prepared' AND active=1
                 RETURNING {COLS}"
            ),
            params![pr, now, id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_published",
            &format!("branch_sync#{}", row.id),
            &format!(
                "{} -> {} published as PR #{}",
                row.source_branch, row.target_branch, pr
            ),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Fail one current active phase loudly. The compare-and-set avoids an old
/// executor overwriting newer progress after an external operation returns.
pub fn fail(
    conn: &mut Connection,
    id: i64,
    expected_phase: &str,
    error: &str,
    now: i64,
) -> Result<Option<BranchSync>> {
    if !is_valid_phase(expected_phase) || is_terminal_phase(expected_phase) {
        return Err(QuorumError::Usage(
            "invalid active branch sync failure phase".into(),
        ));
    }
    let detail = bounded_error(error);
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='failed',active=0,last_error=?1,ci_next_attempt_at=NULL,
                     ci_wait_inflight=0,updated_at=?2
                 WHERE id=?3 AND phase=?4 AND active=1
                 RETURNING {COLS}"
            ),
            params![detail, now, id, expected_phase],
            row_to_branch_sync,
        )
        .optional()?;
    if sync.is_some() {
        crate::errlog::log_error(&tx, now, "branch_sync", &detail);
        crate::events::emit(
            &tx,
            "branch_sync_failed",
            &format!("branch_sync#{id}"),
            &detail,
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

fn bounded_error(error: &str) -> String {
    let mut value = error.replace('\0', "<NUL>");
    const LIMIT: usize = 2048;
    if value.len() > LIMIT {
        let mut end = LIMIT;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value.truncate(end);
    }
    value
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
        let mut phase = "requested";
        for (next, now) in [
            ("pinned", 101),
            ("prepared", 102),
            ("published", 103),
            ("checks", 104),
            ("merging", 105),
            ("done", 106),
        ] {
            let advanced = set_phase(&mut conn, first.id, phase, next, now)
                .unwrap()
                .expect("current phase must advance");
            assert_eq!(advanced.phase, next);
            phase = next;
        }
        let done = get(&conn, first.id).unwrap().unwrap();
        assert_eq!(done.phase, "done");
        assert!(!done.active, "terminal update must release the pair");
        assert!(active_for_pair(&conn, "main", "develop").unwrap().is_none());

        let second = requested(request(&mut conn, "main", "develop", "B", 102).unwrap());
        assert!(second.id > first.id);
        assert_eq!(second.phase, "requested");
    }

    #[test]
    fn conflict_stays_active_and_clean_path_selection_skips_it() {
        let (_dir, mut conn) = open_tmp();
        let conflicted = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        let source = "a".repeat(40);
        let target = "b".repeat(40);
        pin(
            &mut conn,
            conflicted.id,
            &source,
            &target,
            "sync/main-into-develop-1",
            101,
        )
        .unwrap()
        .unwrap();
        let conflict = conflict(&mut conn, conflicted.id, 102).unwrap().unwrap();
        assert!(conflict.active, "a conflict awaits later judgment work");
        assert_eq!(conflict.phase, "conflict");

        let requested = requested(request(&mut conn, "release", "develop", "B", 103).unwrap());
        assert_eq!(next_clean_path(&conn).unwrap(), Some(requested));
        let event: String = conn
            .query_row(
                "SELECT kind FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", conflicted.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event, "branch_sync_conflict");
    }

    #[test]
    fn published_reconciliation_yields_to_later_clean_path_work() {
        let (_dir, mut conn) = open_tmp();
        let published_row = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        let source = "a".repeat(40);
        let target = "b".repeat(40);
        pin(
            &mut conn,
            published_row.id,
            &source,
            &target,
            "sync/main-into-develop-1",
            101,
        )
        .unwrap()
        .unwrap();
        prepared(
            &mut conn,
            published_row.id,
            "sync/main-into-develop-1",
            &"c".repeat(40),
            102,
        )
        .unwrap()
        .unwrap();
        published(&mut conn, published_row.id, 77, 103)
            .unwrap()
            .unwrap();
        assert_eq!(
            next_clean_path(&conn).unwrap(),
            Some(get(&conn, published_row.id).unwrap().unwrap())
        );

        let requested_row = requested(request(&mut conn, "release", "develop", "B", 104).unwrap());
        assert_eq!(next_clean_path(&conn).unwrap(), Some(requested_row));
        touch_published(&mut conn, published_row.id, 105)
            .unwrap()
            .unwrap();
        assert_eq!(
            get(&conn, published_row.id).unwrap().unwrap().updated_at,
            105,
            "published rows rotate only after a successful live verification"
        );
    }

    #[test]
    fn excluded_checks_row_yields_to_another_checks_row() {
        let (_dir, mut conn) = open_tmp();
        let source = "a".repeat(40);
        let target = "b".repeat(40);
        let first = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        pin(&mut conn, first.id, &source, &target, "sync/1", 101)
            .unwrap()
            .unwrap();
        prepared(&mut conn, first.id, "sync/1", &"c".repeat(40), 102)
            .unwrap()
            .unwrap();
        published(&mut conn, first.id, 41, 103).unwrap().unwrap();
        begin_checks(&mut conn, first.id, 104).unwrap().unwrap();

        let second = requested(request(&mut conn, "release", "develop", "B", 100).unwrap());
        pin(&mut conn, second.id, &source, &target, "sync/2", 101)
            .unwrap()
            .unwrap();
        prepared(&mut conn, second.id, "sync/2", &"d".repeat(40), 102)
            .unwrap()
            .unwrap();
        published(&mut conn, second.id, 42, 103).unwrap().unwrap();
        begin_checks(&mut conn, second.id, 104).unwrap().unwrap();

        assert_eq!(
            next_clean_path_excluding(&conn, &[first.id]).unwrap(),
            Some(get(&conn, second.id).unwrap().unwrap()),
            "an in-flight checks wait must not monopolize reconciliation"
        );
    }

    #[test]
    fn durable_check_retry_admission_honors_due_time_and_budget() {
        let (_dir, mut conn) = open_tmp();
        let row = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        let source = "a".repeat(40);
        let target = "b".repeat(40);
        pin(&mut conn, row.id, &source, &target, "sync/1", 101)
            .unwrap()
            .unwrap();
        prepared(&mut conn, row.id, "sync/1", &"c".repeat(40), 102)
            .unwrap()
            .unwrap();
        published(&mut conn, row.id, 41, 103).unwrap().unwrap();
        begin_checks(&mut conn, row.id, 104).unwrap().unwrap();

        let first = admit_check_wait(&mut conn, row.id, 2, 105)
            .unwrap()
            .unwrap();
        assert_eq!(first.ci_attempts, 1);
        assert!(first.ci_wait_inflight);
        schedule_check_retry(&mut conn, row.id, 120, 106)
            .unwrap()
            .unwrap();
        assert!(
            next_clean_path_excluding_at(&conn, &[], None, 119)
                .unwrap()
                .is_none(),
            "a restart must honor the durable retry cadence"
        );

        let second = admit_check_wait(&mut conn, row.id, 2, 120)
            .unwrap()
            .unwrap();
        assert_eq!(second.ci_attempts, 2);
        assert!(second.ci_wait_inflight);
        schedule_check_retry(&mut conn, row.id, 130, 121)
            .unwrap()
            .unwrap();
        assert!(
            admit_check_wait(&mut conn, row.id, 2, 130)
                .unwrap()
                .is_none(),
            "the durable cap must reject another full wait"
        );
    }

    #[test]
    fn stale_expected_phase_returns_no_row() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        assert!(set_phase(&mut conn, sync.id, "requested", "pinned", 101)
            .unwrap()
            .is_some());
        assert!(set_phase(&mut conn, sync.id, "requested", "pinned", 102)
            .unwrap()
            .is_none());
        assert_eq!(get(&conn, sync.id).unwrap().unwrap().phase, "pinned");
    }

    #[test]
    fn reverse_skipped_and_same_phase_transitions_are_rejected_without_mutation() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());

        let error = set_phase(&mut conn, sync.id, "requested", "prepared", 101).unwrap_err();
        assert!(matches!(error, QuorumError::Usage(_)), "skip must reject");
        assert_eq!(
            get(&conn, sync.id).unwrap().unwrap(),
            sync,
            "skip must leave the row unchanged"
        );

        let pinned = set_phase(&mut conn, sync.id, "requested", "pinned", 101)
            .unwrap()
            .expect("requested must advance to pinned");

        for next in ["requested", "pinned"] {
            let error = set_phase(&mut conn, sync.id, "pinned", next, 102).unwrap_err();
            assert!(matches!(error, QuorumError::Usage(_)), "{next} must reject");
            assert_eq!(
                get(&conn, sync.id).unwrap().unwrap(),
                pinned,
                "{next} must leave the row unchanged"
            );
        }
    }

    #[test]
    fn terminal_outcomes_follow_their_operation_phase() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());

        let error = set_phase(&mut conn, sync.id, "requested", "done", 101).unwrap_err();
        assert!(matches!(error, QuorumError::Usage(_)));
        assert_eq!(get(&conn, sync.id).unwrap().unwrap(), sync);

        let failed = set_phase(&mut conn, sync.id, "requested", "failed", 102)
            .unwrap()
            .expect("failed must end any active phase");
        assert_eq!(failed.phase, "failed");
        assert!(!failed.active);
    }
}
