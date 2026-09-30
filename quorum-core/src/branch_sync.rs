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

pub const FAILURE_COMMENT_MAX_ATTEMPTS: i64 = 3;
const FAILURE_COMMENT_RETRY_BASE_SECS: i64 = 30;

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
/// end any active phase. A judgment task's delivered merge publishes a
/// `conflict` row directly. This prevents a restarted executor from replaying
/// or skipping durable work after it has observed a current row.
pub fn is_valid_transition(phase: &str, next_phase: &str) -> bool {
    matches!(
        (phase, next_phase),
        ("requested", "pinned")
            | ("pinned", "prepared" | "noop" | "conflict")
            | ("prepared" | "conflict", "published")
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
/// A row bound to a judgment task is owned by that task's lifecycle, so its
/// published PR never receives the clean path's approval-free merge authority.
pub fn next_clean_path_excluding_at(
    conn: &Connection,
    excluded_ids: &[i64],
    admitted_check_ids: Option<&[i64]>,
    now: i64,
) -> Result<Option<BranchSync>> {
    let mut sql = format!(
        "SELECT {COLS} FROM branch_syncs
         WHERE active=1 AND task_id IS NULL
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

/// Bind a conflict judgment task's delivered merge to its sync row: the
/// worker's own merge commit becomes `merge_sha`, the daemon-created PR becomes
/// `pr`, and the row advances `conflict` → `published` in one compare-and-set.
/// A stale, terminal, or differently bound row returns `None` unchanged.
pub fn publish_from_conflict(
    conn: &mut Connection,
    id: i64,
    task_id: i64,
    merge_sha: &str,
    pr: i64,
    now: i64,
) -> Result<Option<BranchSync>> {
    if merge_sha.is_empty() || merge_sha.contains('\0') || pr <= 0 {
        return Err(QuorumError::Usage(
            "branch sync publication requires a merge SHA and positive PR".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET merge_sha=?1,pr=?2,phase='published',updated_at=?3
                 WHERE id=?4 AND task_id=?5 AND phase='conflict' AND active=1
                 RETURNING {COLS}"
            ),
            params![merge_sha, pr, now, id, task_id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_published",
            &format!("branch_sync#{}", row.id),
            &format!(
                "{} -> {} judgment task#{task_id} published as PR #{pr}",
                row.source_branch, row.target_branch
            ),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Settle a branch-sync judgment task's approved, daemon-merged delivery after
/// the caller has proven both pinned tips are ancestors of GitHub's immutable
/// merge commit. The compare-and-set requires the row still be the task's
/// active `published` conflict binding or `ci_failed` fix binding; a stale,
/// terminal, or differently bound row returns `None` unchanged.
pub fn resolve_conflict_done(
    conn: &mut Connection,
    id: i64,
    task_id: i64,
    merge_commit_sha: &str,
    now: i64,
) -> Result<Option<BranchSync>> {
    if merge_commit_sha.is_empty() || merge_commit_sha.contains('\0') {
        return Err(QuorumError::Usage(
            "branch sync completion requires a merge commit SHA".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='done',active=0,updated_at=?1
                 WHERE id=?2 AND task_id=?3
                   AND phase IN ('published','ci_failed') AND active=1
                 RETURNING {COLS}"
            ),
            params![now, id, task_id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_merged",
            &format!("branch_sync#{}", row.id),
            &format!(
                "{} -> {} judgment task#{task_id} merged as {merge_commit_sha}",
                row.source_branch, row.target_branch
            ),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// Select the oldest active row whose bound judgment task has been cancelled.
/// The daemon removes that row's `sync/<id>` branch before
/// [`cancel_for_cancelled_task`] releases the pair, so a crash between the two
/// steps replays the idempotent removal rather than leaking the branch.
pub fn next_cancelled_judgment(conn: &Connection) -> Result<Option<BranchSync>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {COLS} FROM branch_syncs
                 WHERE active=1 AND task_id IS NOT NULL
                   AND EXISTS (SELECT 1 FROM tasks t
                               WHERE t.id=branch_syncs.task_id AND t.status='cancelled')
                 ORDER BY updated_at ASC, id ASC
                 LIMIT 1"
            ),
            [],
            row_to_branch_sync,
        )
        .optional()?)
}

/// Cancel the active row bound to a cancelled judgment task and release the
/// pair. The compare-and-set re-proves the task binding and its `cancelled`
/// status in the same statement; any other state returns `None` unchanged.
/// Rows without a judgment task keep the coordinator [`cancel_request`] path.
pub fn cancel_for_cancelled_task(
    conn: &mut Connection,
    id: i64,
    task_id: i64,
    now: i64,
) -> Result<Option<BranchSync>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='cancelled',active=0,ci_next_attempt_at=NULL,ci_wait_inflight=0,
                     updated_at=?1
                 WHERE id=?2 AND task_id=?3 AND active=1
                   AND EXISTS (SELECT 1 FROM tasks t WHERE t.id=?3 AND t.status='cancelled')
                 RETURNING {COLS}"
            ),
            params![now, id, task_id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::events::emit(
            &tx,
            "branch_sync_cancelled",
            &format!("branch_sync#{}", row.id),
            &format!(
                "{} -> {} cancelled with judgment task#{task_id}",
                row.source_branch, row.target_branch
            ),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(sync)
}

/// The active sync row bound to a judgment task, if any.
pub fn active_for_task(conn: &Connection, task_id: i64) -> Result<Option<BranchSync>> {
    Ok(conn
        .query_row(
            &format!("SELECT {COLS} FROM branch_syncs WHERE task_id=?1 AND active=1"),
            [task_id],
            row_to_branch_sync,
        )
        .optional()?)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureCommentAttempt {
    pub sync: BranchSync,
    pub attempt: i64,
}

/// Admit one due terminal-comment attempt and durably spend that attempt
/// before the external GitHub call. Attempts live in the failed task's refs,
/// rather than the TTL'd event stream, so restart and event sweeping cannot
/// reset the bound. A failed oldest row is excluded until its backoff expires,
/// allowing later rows to make progress.
pub fn begin_failed_ci_fix_comment_attempt(
    conn: &mut Connection,
    now: i64,
) -> Result<Option<FailureCommentAttempt>> {
    let tx = begin_immediate(conn)?;
    let sync = tx
        .query_row(
            &format!(
                "SELECT {COLS} FROM branch_syncs
                 WHERE id=(
                     SELECT b.id FROM branch_syncs b
                     JOIN tasks t ON t.id=b.task_id AND t.continue_pr=b.pr
                     WHERE b.active=0 AND b.phase='failed'
                       AND b.task_id IS NOT NULL AND b.pr IS NOT NULL
                       AND t.status='failed' AND json_valid(t.refs)
                       AND COALESCE(json_extract(
                               t.refs,'$.branch_sync_failure_comment_posted'),0)!=1
                       AND COALESCE(json_extract(
                               t.refs,'$.branch_sync_failure_comment_attempts'),0)
                           BETWEEN 0 AND ?1 - 1
                       AND COALESCE(json_extract(
                               t.refs,'$.branch_sync_failure_comment_next_at'),0) <= ?2
                     ORDER BY COALESCE(json_extract(
                                  t.refs,'$.branch_sync_failure_comment_next_at'),b.updated_at),
                              b.updated_at,b.id
                     LIMIT 1
                 )"
            ),
            params![FAILURE_COMMENT_MAX_ATTEMPTS, now],
            row_to_branch_sync,
        )
        .optional()?;
    let Some(sync) = sync else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(None);
    };
    let task_id = sync.task_id.ok_or_else(|| {
        QuorumError::Io(format!(
            "branch sync #{} lost its CI-fix task binding during comment admission",
            sync.id
        ))
    })?;
    let prior_attempts: i64 = tx.query_row(
        "SELECT COALESCE(json_extract(
             refs,'$.branch_sync_failure_comment_attempts'),0)
         FROM tasks WHERE id=?1",
        [task_id],
        |row| row.get(0),
    )?;
    let attempt = prior_attempts + 1;
    let next_at = (attempt < FAILURE_COMMENT_MAX_ATTEMPTS)
        .then(|| now + FAILURE_COMMENT_RETRY_BASE_SECS * 4_i64.pow((attempt - 1) as u32));
    let changed = tx.execute(
        "UPDATE tasks
         SET refs=json_remove(
                 json_set(
                     refs,
                     '$.branch_sync_failure_comment_attempts',?1,
                     '$.branch_sync_failure_comment_next_at',?2
                 ),
                 '$.branch_sync_failure_comment_last_error',
                 '$.branch_sync_failure_comment_exhausted'
             ),
             updated_at=?3
         WHERE id=?4 AND status='failed' AND continue_pr=?5
           AND json_valid(refs)
           AND COALESCE(json_extract(
                   refs,'$.branch_sync_failure_comment_posted'),0)!=1
           AND COALESCE(json_extract(
                   refs,'$.branch_sync_failure_comment_attempts'),0)=?6",
        params![attempt, next_at, now, task_id, sync.pr, prior_attempts],
    )?;
    if changed != 1 {
        tx.commit().map_err(map_sql_err)?;
        return Ok(None);
    }
    crate::events::emit(
        &tx,
        "branch_sync_failure_comment_attempted",
        &format!("branch_sync#{}", sync.id),
        &format!(
            "terminal CI-fix failure comment attempt {attempt}/{FAILURE_COMMENT_MAX_ATTEMPTS} for task#{task_id}"
        ),
        now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(Some(FailureCommentAttempt { sync, attempt }))
}

/// Record successful publication of the terminal CI-fix failure comment.
/// A stale/nonmatching row is a clean negative so restart reconciliation can
/// race safely with administrative repair without inventing evidence.
pub fn record_failure_comment_posted(
    conn: &mut Connection,
    id: i64,
    task_id: i64,
    attempt: i64,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let eligible = tx.execute(
        "UPDATE tasks
         SET refs=json_remove(
                 json_set(refs,'$.branch_sync_failure_comment_posted',json('true')),
                 '$.branch_sync_failure_comment_next_at',
                 '$.branch_sync_failure_comment_last_error',
                 '$.branch_sync_failure_comment_exhausted'
             ),
             updated_at=?4
         WHERE id=?2 AND status='failed' AND json_valid(refs)
           AND COALESCE(json_extract(
                   refs,'$.branch_sync_failure_comment_attempts'),0)=?3
           AND COALESCE(json_extract(
                   refs,'$.branch_sync_failure_comment_posted'),0)!=1
           AND EXISTS (
               SELECT 1 FROM branch_syncs b
               WHERE b.id=?1 AND b.task_id=tasks.id AND b.pr=tasks.continue_pr
                 AND b.active=0 AND b.phase='failed'
           )",
        params![id, task_id, attempt, now],
    )? == 1;
    if eligible {
        crate::events::emit(
            &tx,
            "branch_sync_failure_commented",
            &format!("branch_sync#{id}"),
            &format!("terminal CI-fix failure comment posted for task#{task_id}"),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(eligible)
}

/// Record one failed external comment attempt. The next-at timestamp was
/// installed before the call, so this write only preserves bounded diagnostics
/// and marks the terminal attempt as exhausted.
pub fn record_failure_comment_failed(
    conn: &mut Connection,
    id: i64,
    task_id: i64,
    attempt: i64,
    error: &str,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let detail = bounded_error(error);
    let exhausted = attempt >= FAILURE_COMMENT_MAX_ATTEMPTS;
    let changed = tx.execute(
        "UPDATE tasks
         SET refs=json_set(
                 refs,
                 '$.branch_sync_failure_comment_last_error',?1,
                 '$.branch_sync_failure_comment_exhausted',?2
             ),
             updated_at=?3
         WHERE id=?4 AND status='failed' AND json_valid(refs)
           AND COALESCE(json_extract(
                   refs,'$.branch_sync_failure_comment_attempts'),0)=?5
           AND COALESCE(json_extract(
                   refs,'$.branch_sync_failure_comment_posted'),0)!=1
           AND EXISTS (
               SELECT 1 FROM branch_syncs b
               WHERE b.id=?6 AND b.task_id=tasks.id AND b.pr=tasks.continue_pr
                 AND b.active=0 AND b.phase='failed'
           )",
        params![detail, exhausted, now, task_id, attempt, id],
    )? == 1;
    if changed {
        crate::events::emit(
            &tx,
            if exhausted {
                "branch_sync_failure_comment_exhausted"
            } else {
                "branch_sync_failure_comment_deferred"
            },
            &format!("branch_sync#{id}"),
            &format!(
                "terminal CI-fix failure comment attempt {attempt}/{FAILURE_COMMENT_MAX_ATTEMPTS} failed: {detail}"
            ),
            now,
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(changed)
}

/// Fail the active sync row bound to a judgment task inside the caller's
/// write transaction, so the task's terminal failure and the row's `failed`
/// phase commit together. A task with no bound active row is a no-op.
pub fn fail_for_task_tx(
    conn: &Connection,
    task_id: i64,
    error: &str,
    now: i64,
) -> Result<Option<BranchSync>> {
    let detail = bounded_error(error);
    let sync = conn
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='failed',active=0,last_error=?1,ci_next_attempt_at=NULL,
                     ci_wait_inflight=0,updated_at=?2
                 WHERE task_id=?3 AND active=1
                 RETURNING {COLS}"
            ),
            params![detail, now, task_id],
            row_to_branch_sync,
        )
        .optional()?;
    if let Some(row) = &sync {
        crate::errlog::log_error(conn, now, "branch_sync", &detail);
        crate::events::emit(
            conn,
            "branch_sync_failed",
            &format!("branch_sync#{}", row.id),
            &detail,
            now,
        )?;
    }
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

/// Result of a coordinator-issued cancel attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOutcome {
    /// Cancellation succeeded; the row is now terminal and the pair released.
    Cancelled(BranchSync),
    /// No such id.
    NotFound,
    /// Row exists but is not in a cancellable phase, or a judgment task is
    /// already attached — the operator must intervene through the task itself.
    NotCancellable(BranchSync),
}

/// Phases a coordinator may cancel. `checks` and `merging` are excluded
/// because an in-flight external CI wait or `gh pr merge` call owns
/// authority; `conflict` and terminals cannot be cancelled from the CLI.
fn is_coordinator_cancellable(phase: &str) -> bool {
    matches!(
        phase,
        "requested" | "pinned" | "prepared" | "published" | "ci_failed"
    )
}

/// Atomically cancel one active row and release the pair. Rows with a
/// judgment task attached are rejected: the operator must cancel that task.
pub fn cancel_request(conn: &mut Connection, id: i64, by: &str, now: i64) -> Result<CancelOutcome> {
    if by.is_empty() || by.contains('\0') {
        return Err(QuorumError::Usage(
            "branch-sync --by must not be empty or contain NUL".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, by, now)?;
    let Some(current) = tx
        .query_row(
            &format!("SELECT {COLS} FROM branch_syncs WHERE id=?1"),
            [id],
            row_to_branch_sync,
        )
        .optional()?
    else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(CancelOutcome::NotFound);
    };
    if !current.active || !is_coordinator_cancellable(&current.phase) || current.task_id.is_some() {
        tx.commit().map_err(map_sql_err)?;
        return Ok(CancelOutcome::NotCancellable(current));
    }
    let cancelled = tx
        .query_row(
            &format!(
                "UPDATE branch_syncs
                 SET phase='cancelled',active=0,updated_at=?1
                 WHERE id=?2 AND phase=?3 AND active=1 AND task_id IS NULL
                 RETURNING {COLS}"
            ),
            params![now, id, current.phase],
            row_to_branch_sync,
        )
        .optional()?;
    let Some(row) = cancelled else {
        // A concurrent phase advance won the race; reread and report the current row.
        let reread = tx
            .query_row(
                &format!("SELECT {COLS} FROM branch_syncs WHERE id=?1"),
                [id],
                row_to_branch_sync,
            )
            .optional()?;
        tx.commit().map_err(map_sql_err)?;
        return Ok(match reread {
            Some(row) => CancelOutcome::NotCancellable(row),
            None => CancelOutcome::NotFound,
        });
    };
    crate::events::emit(
        &tx,
        "branch_sync_cancelled",
        &format!("branch_sync#{id}"),
        &format!(
            "{} -> {} cancelled by {} from {}",
            row.source_branch, row.target_branch, by, current.phase
        ),
        now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(CancelOutcome::Cancelled(row))
}

/// Select the oldest active `conflict` row that still lacks a bound judgment
/// task. The daemon's intake pass calls this once per tick and hands the id
/// to [`create_conflict_judgment_task`] for atomic provisioning, keeping the
/// pass fair (oldest first) and bounded (one row per tick) regardless of
/// how many sync pairs an operator has configured.
pub fn next_conflict_awaiting_task(conn: &Connection) -> Result<Option<BranchSync>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {COLS} FROM branch_syncs
                 WHERE active=1 AND phase='conflict' AND task_id IS NULL
                 ORDER BY updated_at ASC, id ASC
                 LIMIT 1"
            ),
            [],
            row_to_branch_sync,
        )
        .optional()?)
}

/// Select the oldest active CI-failed sync whose existing PR has not yet
/// received its one daemon-created fix-forward task.
pub fn next_ci_failed_awaiting_task(conn: &Connection) -> Result<Option<BranchSync>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {COLS} FROM branch_syncs
                 WHERE active=1 AND phase='ci_failed' AND task_id IS NULL
                 ORDER BY updated_at ASC, id ASC
                 LIMIT 1"
            ),
            [],
            row_to_branch_sync,
        )
        .optional()?)
}

/// Outcome of [`create_conflict_judgment_task`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictJudgmentOutcome {
    /// A fresh judgment task was created and its id bound to the sync row in
    /// one write transaction.
    Created(ConflictJudgmentTask),
    /// The row is missing, no longer active, no longer `conflict`, or already
    /// carries a `task_id`. A concurrent daemon or coordinator won the race.
    NotEligible,
}

/// The judgment task the daemon created for one conflicted branch sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictJudgmentTask {
    pub sync_id: i64,
    pub task_id: i64,
    pub target_branch: String,
    pub title: String,
    pub body: String,
    pub refs_json: String,
}

/// Outcome of [`create_ci_failure_fix_task`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CiFailureFixOutcome {
    /// A fresh continuation task was created and atomically bound to the sync.
    Created(CiFailureFixTask),
    /// The row is missing, no longer active/CI-failed, or already bound.
    NotEligible,
}

/// The continuation task created to fix CI on an existing branch-sync PR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CiFailureFixTask {
    pub sync_id: i64,
    pub task_id: i64,
    pub pr: i64,
    pub target_branch: String,
    pub title: String,
    pub body: String,
    pub refs_json: String,
}

fn conflict_task_title(sync_id: i64, from: &str, to: &str) -> String {
    format!("Resolve branch-sync conflict: {from} → {to} (#{sync_id})")
}

fn conflict_task_body(
    source_sha: &str,
    target_sha: &str,
    sync_branch: &str,
    conflicted_files: &[String],
) -> String {
    let mut body = String::new();
    body.push_str(&format!("source_sha: {source_sha}\n"));
    body.push_str(&format!("target_sha: {target_sha}\n"));
    body.push_str(&format!("sync_branch: {sync_branch}\n"));
    body.push_str("\nConflicted files:\n");
    if conflicted_files.is_empty() {
        body.push_str("(none reported by git diff --diff-filter=U)\n");
    } else {
        for file in conflicted_files {
            body.push_str(&format!("- {file}\n"));
        }
    }
    body.push_str(
        "\nResolve the merge conflicts and commit the merge in place. \
         Do not rebase, squash, or drop either side of the merge; both \
         parents must remain reachable from the resolved commit.\n",
    );
    body
}

fn ci_failure_task_title(sync_id: i64, from: &str, to: &str) -> String {
    format!("Fix CI for branch sync: {from} → {to} (#{sync_id})")
}

fn ci_failure_task_body(merge_sha: &str, pr: i64, failure_detail: &str) -> String {
    let checks = failure_detail
        .strip_prefix(&format!("PR #{pr} CI failed: "))
        .unwrap_or(failure_detail);
    let mut body = format!(
        "merge_sha: {merge_sha}\npull_request: #{pr}\n\nFailing checks from statusCheckRollup:\n"
    );
    if checks.trim().is_empty() {
        body.push_str("- (no check names reported)\n");
    } else {
        for check in checks.split(", ") {
            body.push_str(&format!("- {check}\n"));
        }
    }
    body.push_str(
        "\nFix forward on the existing sync branch. Do not rebase or force push, and do not \
         rewrite the merge commit. Commit only the changes needed to make CI pass.\n",
    );
    body
}

/// Atomically create the judgment task for one conflicted branch-sync row and
/// bind its id back onto that row. A row that no longer needs a task (missing,
/// inactive, non-`conflict`, or already bound) returns [`NotEligible`]
/// without side effects. This is the sole daemon-owned path that provisions
/// the conflict judgment task, so a restart or concurrent tick cannot double
/// up on task creation.
///
/// [`NotEligible`]: ConflictJudgmentOutcome::NotEligible
pub fn create_conflict_judgment_task(
    conn: &mut Connection,
    sync_id: i64,
    conflicted_files: &[String],
    now: i64,
) -> Result<ConflictJudgmentOutcome> {
    for file in conflicted_files {
        if file.contains('\0') {
            return Err(QuorumError::Usage(
                "conflicted file paths must not contain NUL".into(),
            ));
        }
    }
    let tx = begin_immediate(conn)?;
    let Some(row) = tx
        .query_row(
            &format!(
                "SELECT {COLS} FROM branch_syncs
                 WHERE id=?1 AND active=1 AND phase='conflict' AND task_id IS NULL"
            ),
            [sync_id],
            row_to_branch_sync,
        )
        .optional()?
    else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(ConflictJudgmentOutcome::NotEligible);
    };
    let source_sha = row
        .source_sha
        .as_deref()
        .ok_or_else(|| QuorumError::Usage("conflict row missing source_sha".into()))?;
    let target_sha = row
        .target_sha
        .as_deref()
        .ok_or_else(|| QuorumError::Usage("conflict row missing target_sha".into()))?;
    let sync_branch = row
        .sync_branch
        .as_deref()
        .ok_or_else(|| QuorumError::Usage("conflict row missing sync_branch".into()))?;
    let title = conflict_task_title(row.id, &row.source_branch, &row.target_branch);
    let body = conflict_task_body(source_sha, target_sha, sync_branch, conflicted_files);
    let refs_json = serde_json::json!({ "branch_sync": row.id }).to_string();

    let task_id = crate::tasks::create_task_tx(
        &tx,
        "daemon",
        &title,
        Some(&body),
        0,
        None,
        Some(&refs_json),
        None,
        None,
        None,
        Some(&row.target_branch),
        now,
    )?;

    let bound = tx.execute(
        "UPDATE branch_syncs
             SET task_id=?1, updated_at=?2
             WHERE id=?3 AND active=1 AND phase='conflict' AND task_id IS NULL",
        params![task_id, now, row.id],
    )?;
    if bound != 1 {
        // A concurrent writer took the slot; roll back everything we staged.
        return Err(QuorumError::Db(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_CHECK),
            Some("branch sync row lost task binding race".into()),
        )));
    }
    crate::events::emit(
        &tx,
        "branch_sync_judgment_task_created",
        &format!("branch_sync#{}", row.id),
        &format!(
            "conflict judgment task#{task_id} created for {} -> {}",
            row.source_branch, row.target_branch
        ),
        now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(ConflictJudgmentOutcome::Created(ConflictJudgmentTask {
        sync_id: row.id,
        task_id,
        target_branch: row.target_branch,
        title,
        body,
        refs_json,
    }))
}

/// Atomically create the single continuation task for a CI-failed branch-sync
/// PR and bind it to the sync row. The task continues the exact existing PR;
/// its normal continuation baseline and SHA lease protect the published head.
pub fn create_ci_failure_fix_task(
    conn: &mut Connection,
    sync_id: i64,
    now: i64,
) -> Result<CiFailureFixOutcome> {
    let tx = begin_immediate(conn)?;
    let Some(row) = tx
        .query_row(
            &format!(
                "SELECT {COLS} FROM branch_syncs
                 WHERE id=?1 AND active=1 AND phase='ci_failed' AND task_id IS NULL"
            ),
            [sync_id],
            row_to_branch_sync,
        )
        .optional()?
    else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(CiFailureFixOutcome::NotEligible);
    };
    let merge_sha = row
        .merge_sha
        .as_deref()
        .filter(|sha| !sha.is_empty())
        .ok_or_else(|| QuorumError::Usage("CI-failed sync row missing merge_sha".into()))?;
    let pr = row
        .pr
        .filter(|pr| *pr > 0)
        .ok_or_else(|| QuorumError::Usage("CI-failed sync row missing PR".into()))?;
    let failure_detail = row
        .last_error
        .as_deref()
        .ok_or_else(|| QuorumError::Usage("CI-failed sync row missing failing checks".into()))?;
    let title = ci_failure_task_title(row.id, &row.source_branch, &row.target_branch);
    let body = ci_failure_task_body(merge_sha, pr, failure_detail);
    let refs_json = serde_json::json!({ "branch_sync": row.id }).to_string();
    let task_id = crate::tasks::create_task_tx(
        &tx,
        "daemon",
        &title,
        Some(&body),
        0,
        None,
        Some(&refs_json),
        None,
        None,
        Some(pr),
        Some(&row.target_branch),
        now,
    )?;
    let bound = tx.execute(
        "UPDATE branch_syncs
         SET task_id=?1,updated_at=?2
         WHERE id=?3 AND active=1 AND phase='ci_failed' AND task_id IS NULL",
        params![task_id, now, row.id],
    )?;
    if bound != 1 {
        return Err(QuorumError::Db(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_CHECK),
            Some("branch sync row lost CI-fix task binding race".into()),
        )));
    }
    crate::events::emit(
        &tx,
        "branch_sync_ci_fix_task_created",
        &format!("branch_sync#{}", row.id),
        &format!(
            "CI-fix task#{task_id} created for PR #{pr} ({} -> {})",
            row.source_branch, row.target_branch
        ),
        now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(CiFailureFixOutcome::Created(CiFailureFixTask {
        sync_id: row.id,
        task_id,
        pr,
        target_branch: row.target_branch,
        title,
        body,
        refs_json,
    }))
}

/// Return the most recent terminal rows, newest first. Bounded by `limit` so
/// a long history stays cheap for a short read.
pub fn list_recent_terminal(conn: &Connection, limit: i64) -> Result<Vec<BranchSync>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM branch_syncs WHERE active=0
         ORDER BY updated_at DESC, id DESC LIMIT ?1"
    ))?;
    let syncs = stmt
        .query_map(params![limit], row_to_branch_sync)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(syncs)
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

    #[test]
    fn cancel_request_releases_pair_and_emits_event() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        let outcome = cancel_request(&mut conn, sync.id, "coordinator", 101).unwrap();
        match outcome {
            CancelOutcome::Cancelled(row) => {
                assert_eq!(row.phase, "cancelled");
                assert!(!row.active);
            }
            other => panic!("expected Cancelled, got {other:?}"),
        }
        assert!(active_for_pair(&conn, "main", "develop").unwrap().is_none());
        let event: String = conn
            .query_row(
                "SELECT kind FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", sync.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event, "branch_sync_cancelled");
    }

    #[test]
    fn cancel_request_rejects_checks_merging_and_conflict() {
        let (_dir, mut conn) = open_tmp();
        let source = "a".repeat(40);
        let target = "b".repeat(40);

        // checks phase — not cancellable
        let a = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        pin(&mut conn, a.id, &source, &target, "sync/1", 101)
            .unwrap()
            .unwrap();
        prepared(&mut conn, a.id, "sync/1", &"c".repeat(40), 102)
            .unwrap()
            .unwrap();
        published(&mut conn, a.id, 11, 103).unwrap().unwrap();
        begin_checks(&mut conn, a.id, 104).unwrap().unwrap();
        assert!(matches!(
            cancel_request(&mut conn, a.id, "coordinator", 105).unwrap(),
            CancelOutcome::NotCancellable(row) if row.phase == "checks"
        ));

        // conflict phase — not cancellable via CLI
        let b = requested(request(&mut conn, "release", "develop", "A", 100).unwrap());
        pin(&mut conn, b.id, &source, &target, "sync/2", 101)
            .unwrap()
            .unwrap();
        conflict(&mut conn, b.id, 102).unwrap().unwrap();
        assert!(matches!(
            cancel_request(&mut conn, b.id, "coordinator", 103).unwrap(),
            CancelOutcome::NotCancellable(row) if row.phase == "conflict"
        ));
    }

    #[test]
    fn cancel_request_of_missing_row_is_a_clean_negative() {
        let (_dir, mut conn) = open_tmp();
        assert_eq!(
            cancel_request(&mut conn, 999, "coordinator", 100).unwrap(),
            CancelOutcome::NotFound
        );
    }

    #[test]
    fn cancel_request_of_terminal_row_reports_not_cancellable() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        set_phase(&mut conn, sync.id, "requested", "failed", 101)
            .unwrap()
            .unwrap();
        assert!(matches!(
            cancel_request(&mut conn, sync.id, "coordinator", 102).unwrap(),
            CancelOutcome::NotCancellable(row) if row.phase == "failed"
        ));
    }

    fn pin_and_conflict(conn: &mut Connection, id: i64) {
        let source = "a".repeat(40);
        let target = "b".repeat(40);
        pin(conn, id, &source, &target, "sync/1", 101)
            .unwrap()
            .unwrap();
        conflict(conn, id, 102).unwrap().unwrap();
    }

    #[test]
    fn create_conflict_judgment_task_links_row_atomically() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        pin_and_conflict(&mut conn, sync.id);
        let files = vec!["src/lib.rs".to_string(), "Cargo.toml".to_string()];
        let outcome = create_conflict_judgment_task(&mut conn, sync.id, &files, 200).unwrap();
        let task = match outcome {
            ConflictJudgmentOutcome::Created(task) => task,
            other => panic!("expected Created, got {other:?}"),
        };
        assert_eq!(task.sync_id, sync.id);
        assert_eq!(task.target_branch, "develop");
        assert_eq!(
            task.title,
            format!(
                "Resolve branch-sync conflict: main → develop (#{})",
                sync.id
            )
        );
        assert!(task.body.contains(&"a".repeat(40)), "body lists source_sha");
        assert!(task.body.contains(&"b".repeat(40)), "body lists target_sha");
        assert!(task.body.contains("src/lib.rs"), "body lists conflict file");
        assert!(task.body.contains("Cargo.toml"), "body lists conflict file");
        assert!(
            task.body.contains("without rebasing") || task.body.contains("Do not rebase"),
            "body carries the resolve-without-rebasing instruction"
        );

        let row = get(&conn, sync.id).unwrap().unwrap();
        assert_eq!(row.task_id, Some(task.task_id));
        assert!(
            row.active,
            "conflict row remains active for the judgment task"
        );
        assert_eq!(row.phase, "conflict");

        let (created_by, target_branch, refs, body): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT created_by, target_branch, refs, body FROM tasks WHERE id=?1",
                [task.task_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(created_by, "daemon");
        assert_eq!(target_branch.as_deref(), Some("develop"));
        assert!(body.is_some());
        let refs_value: serde_json::Value = serde_json::from_str(refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs_value["branch_sync"], serde_json::json!(sync.id));

        let event: String = conn
            .query_row(
                "SELECT kind FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", sync.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event, "branch_sync_judgment_task_created");
    }

    #[test]
    fn create_conflict_judgment_task_is_a_clean_negative_when_already_bound() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        pin_and_conflict(&mut conn, sync.id);
        let first = create_conflict_judgment_task(&mut conn, sync.id, &[], 200).unwrap();
        let task_id = match first {
            ConflictJudgmentOutcome::Created(task) => task.task_id,
            other => panic!("expected Created, got {other:?}"),
        };
        let second = create_conflict_judgment_task(&mut conn, sync.id, &[], 201).unwrap();
        assert_eq!(second, ConflictJudgmentOutcome::NotEligible);
        let row = get(&conn, sync.id).unwrap().unwrap();
        assert_eq!(row.task_id, Some(task_id), "task binding must not change");
    }

    #[test]
    fn create_ci_failure_fix_task_is_atomic_idempotent_continue_pr_intake() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "develop", "main", "A", 100).unwrap());
        pin(
            &mut conn,
            sync.id,
            &"a".repeat(40),
            &"b".repeat(40),
            &format!("sync/{}", sync.id),
            101,
        )
        .unwrap()
        .unwrap();
        let merge_sha = "c".repeat(40);
        prepared(
            &mut conn,
            sync.id,
            &format!("sync/{}", sync.id),
            &merge_sha,
            102,
        )
        .unwrap()
        .unwrap();
        published(&mut conn, sync.id, 77, 103).unwrap().unwrap();
        begin_checks(&mut conn, sync.id, 104).unwrap().unwrap();
        ci_failed(
            &mut conn,
            sync.id,
            "PR #77 CI failed: unit-tests, clippy",
            105,
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            next_ci_failed_awaiting_task(&conn)
                .unwrap()
                .map(|row| row.id),
            Some(sync.id)
        );
        let created = match create_ci_failure_fix_task(&mut conn, sync.id, 106).unwrap() {
            CiFailureFixOutcome::Created(task) => task,
            CiFailureFixOutcome::NotEligible => panic!("CI-failed row must be eligible"),
        };
        assert_eq!(
            created.title,
            format!("Fix CI for branch sync: develop → main (#{}", sync.id) + ")"
        );
        assert!(created.body.contains(&format!("merge_sha: {merge_sha}")));
        assert!(created.body.contains("- unit-tests"));
        assert!(created.body.contains("- clippy"));
        assert!(created.body.contains("Do not rebase or force push"));
        assert!(created.body.contains("do not rewrite the merge commit"));

        let task = crate::tasks::get(&conn, created.task_id)
            .unwrap()
            .expect("task exists");
        assert_eq!(task.created_by, "daemon");
        assert_eq!(task.continue_pr, Some(77));
        assert_eq!(task.target_branch.as_deref(), Some("main"));
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["branch_sync"], serde_json::json!(sync.id));
        assert_eq!(
            create_ci_failure_fix_task(&mut conn, sync.id, 107).unwrap(),
            CiFailureFixOutcome::NotEligible
        );
        assert_eq!(
            conn.query_row::<i64, _, _>(
                "SELECT count(*) FROM tasks WHERE json_extract(refs, '$.branch_sync')=?1",
                [sync.id],
                |row| row.get(0),
            )
            .unwrap(),
            1
        );
    }

    fn failed_ci_fix_task(
        conn: &mut Connection,
        source: &str,
        target: &str,
        pr: i64,
        now: i64,
    ) -> (BranchSync, i64) {
        let sync = requested(request(conn, source, target, "A", now).unwrap());
        pin(
            conn,
            sync.id,
            &"a".repeat(40),
            &"b".repeat(40),
            &format!("sync/{}", sync.id),
            now + 1,
        )
        .unwrap()
        .unwrap();
        prepared(
            conn,
            sync.id,
            &format!("sync/{}", sync.id),
            &"c".repeat(40),
            now + 2,
        )
        .unwrap()
        .unwrap();
        published(conn, sync.id, pr, now + 3).unwrap().unwrap();
        begin_checks(conn, sync.id, now + 4).unwrap().unwrap();
        ci_failed(conn, sync.id, &format!("PR #{pr} CI failed: test"), now + 5)
            .unwrap()
            .unwrap();
        let task_id = match create_ci_failure_fix_task(conn, sync.id, now + 6).unwrap() {
            CiFailureFixOutcome::Created(task) => task.task_id,
            CiFailureFixOutcome::NotEligible => unreachable!(),
        };
        conn.execute(
            "UPDATE tasks SET refs=json_set(refs,
                '$.cx_est',1,'$.cx_size','S','$.cx_ready',json('true'),
                '$.cx_not_ready_reason',json('null'),'$.cx_risk_flags',json('[]'))
             WHERE id=?1",
            [task_id],
        )
        .unwrap();
        crate::tasks::claim(
            conn,
            "worker",
            Some(task_id),
            &[],
            crate::tasks::DEFAULT_LEASE_TTL_SECS,
            now + 7,
        )
        .unwrap()
        .unwrap();
        let transition = crate::tasks::apply_event(
            conn,
            "worker",
            task_id,
            &crate::lifecycle::Event::AgentFailed {
                reason: "worker crashed".into(),
            },
            now + 8,
        )
        .unwrap();
        assert_eq!(transition.task.status, "failed");
        assert!(!transition
            .effects
            .iter()
            .any(|effect| matches!(effect, crate::lifecycle::Effect::ResumeWorker)));
        (sync, task_id)
    }

    #[test]
    fn ci_failure_fix_worker_failure_fails_sync_without_retry_and_queues_pr_comment() {
        let (_dir, mut conn) = open_tmp();
        let (sync, task_id) = failed_ci_fix_task(&mut conn, "develop", "main", 77, 100);
        let failed = get(&conn, sync.id).unwrap().unwrap();
        assert_eq!(failed.phase, "failed");
        assert!(!failed.active);
        let admitted = begin_failed_ci_fix_comment_attempt(&mut conn, 109)
            .unwrap()
            .expect("failed worker queues a comment attempt");
        assert_eq!(admitted.sync.id, sync.id);
        assert_eq!(admitted.attempt, 1);
        assert!(record_failure_comment_posted(&mut conn, sync.id, task_id, 1, 110).unwrap());
        assert!(begin_failed_ci_fix_comment_attempt(&mut conn, 200)
            .unwrap()
            .is_none());
        assert!(!record_failure_comment_posted(&mut conn, sync.id, task_id, 1, 111).unwrap());
    }

    #[test]
    fn failed_ci_fix_comment_retries_are_bounded_backed_off_and_fair() {
        let (_dir, mut conn) = open_tmp();
        let (oldest, oldest_task) = failed_ci_fix_task(&mut conn, "develop", "main", 77, 100);
        let (later, later_task) = failed_ci_fix_task(&mut conn, "release", "main", 78, 200);

        let first = begin_failed_ci_fix_comment_attempt(&mut conn, 300)
            .unwrap()
            .unwrap();
        assert_eq!(first.sync.id, oldest.id);
        assert!(record_failure_comment_failed(
            &mut conn,
            oldest.id,
            oldest_task,
            first.attempt,
            "permission denied",
            300,
        )
        .unwrap());

        let second = begin_failed_ci_fix_comment_attempt(&mut conn, 300)
            .unwrap()
            .expect("the backed-off oldest row must not starve a later row");
        assert_eq!(second.sync.id, later.id);
        assert!(record_failure_comment_posted(
            &mut conn,
            later.id,
            later_task,
            second.attempt,
            301,
        )
        .unwrap());

        assert!(begin_failed_ci_fix_comment_attempt(&mut conn, 329)
            .unwrap()
            .is_none());
        for (at, expected_attempt) in [(330, 2), (450, 3)] {
            let retry = begin_failed_ci_fix_comment_attempt(&mut conn, at)
                .unwrap()
                .unwrap();
            assert_eq!(retry.sync.id, oldest.id);
            assert_eq!(retry.attempt, expected_attempt);
            assert!(record_failure_comment_failed(
                &mut conn,
                oldest.id,
                oldest_task,
                retry.attempt,
                "permission denied",
                at,
            )
            .unwrap());
        }
        assert!(begin_failed_ci_fix_comment_attempt(&mut conn, 10_000)
            .unwrap()
            .is_none());
        let (attempts, exhausted): (i64, bool) = conn
            .query_row(
                "SELECT json_extract(refs,'$.branch_sync_failure_comment_attempts'),
                        json_extract(refs,'$.branch_sync_failure_comment_exhausted')
                 FROM tasks WHERE id=?1",
                [oldest_task],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(attempts, FAILURE_COMMENT_MAX_ATTEMPTS);
        assert!(exhausted);
    }

    #[test]
    fn failed_sync_does_not_mislabel_done_or_cancelled_ci_fix_tasks() {
        let (_dir, mut conn) = open_tmp();
        let (_, done_task) = failed_ci_fix_task(&mut conn, "develop", "main", 77, 100);
        let (_, cancelled_task) = failed_ci_fix_task(&mut conn, "release", "main", 78, 200);
        conn.execute("UPDATE tasks SET status='done' WHERE id=?1", [done_task])
            .unwrap();
        conn.execute(
            "UPDATE tasks SET status='cancelled' WHERE id=?1",
            [cancelled_task],
        )
        .unwrap();

        assert!(begin_failed_ci_fix_comment_attempt(&mut conn, 300)
            .unwrap()
            .is_none());
    }

    #[test]
    fn ci_failure_fix_approved_merge_completes_bound_sync_without_rewriting_merge_sha() {
        let (_dir, mut conn) = open_tmp();
        let sync = requested(request(&mut conn, "develop", "main", "A", 100).unwrap());
        pin(
            &mut conn,
            sync.id,
            &"a".repeat(40),
            &"b".repeat(40),
            &format!("sync/{}", sync.id),
            101,
        )
        .unwrap()
        .unwrap();
        let original_merge = "c".repeat(40);
        prepared(
            &mut conn,
            sync.id,
            &format!("sync/{}", sync.id),
            &original_merge,
            102,
        )
        .unwrap()
        .unwrap();
        published(&mut conn, sync.id, 77, 103).unwrap().unwrap();
        begin_checks(&mut conn, sync.id, 104).unwrap().unwrap();
        ci_failed(&mut conn, sync.id, "PR #77 CI failed: test", 105)
            .unwrap()
            .unwrap();
        let task_id = match create_ci_failure_fix_task(&mut conn, sync.id, 106).unwrap() {
            CiFailureFixOutcome::Created(task) => task.task_id,
            CiFailureFixOutcome::NotEligible => unreachable!(),
        };
        let remote_merge = "d".repeat(40);
        let done = resolve_conflict_done(&mut conn, sync.id, task_id, &remote_merge, 107)
            .unwrap()
            .expect("bound CI-fix task completes the sync");
        assert_eq!(done.phase, "done");
        assert!(!done.active);
        assert_eq!(done.merge_sha.as_deref(), Some(original_merge.as_str()));
    }

    #[test]
    fn next_conflict_awaiting_task_returns_oldest_unbound_row() {
        let (_dir, mut conn) = open_tmp();
        let a = requested(request(&mut conn, "main", "develop", "A", 100).unwrap());
        pin_and_conflict(&mut conn, a.id);
        let b = requested(request(&mut conn, "release", "develop", "A", 100).unwrap());
        pin_and_conflict(&mut conn, b.id);
        // Nothing bound yet — oldest row wins.
        let first = next_conflict_awaiting_task(&conn).unwrap().unwrap();
        assert_eq!(first.id, a.id);
        create_conflict_judgment_task(&mut conn, a.id, &[], 200).unwrap();
        // Once bound, the next tick moves to the next oldest unbound row.
        let second = next_conflict_awaiting_task(&conn).unwrap().unwrap();
        assert_eq!(second.id, b.id);
        create_conflict_judgment_task(&mut conn, b.id, &[], 201).unwrap();
        assert!(next_conflict_awaiting_task(&conn).unwrap().is_none());
    }

    fn bound_judgment_task(conn: &mut Connection) -> (BranchSync, i64) {
        let sync = requested(request(conn, "main", "develop", "A", 100).unwrap());
        pin_and_conflict(conn, sync.id);
        let task_id = match create_conflict_judgment_task(conn, sync.id, &[], 200).unwrap() {
            ConflictJudgmentOutcome::Created(task) => task.task_id,
            other => panic!("expected Created, got {other:?}"),
        };
        (sync, task_id)
    }

    #[test]
    fn publish_from_conflict_records_merge_and_pr_in_one_cas() {
        let (_dir, mut conn) = open_tmp();
        let (sync, task_id) = bound_judgment_task(&mut conn);
        let merge_sha = "c".repeat(40);

        // Only the bound task may publish the row.
        assert!(
            publish_from_conflict(&mut conn, sync.id, task_id + 1, &merge_sha, 7, 300)
                .unwrap()
                .is_none()
        );
        assert!(publish_from_conflict(&mut conn, sync.id, task_id, "", 7, 300).is_err());
        assert!(publish_from_conflict(&mut conn, sync.id, task_id, &merge_sha, 0, 300).is_err());
        assert_eq!(get(&conn, sync.id).unwrap().unwrap().phase, "conflict");

        let published = publish_from_conflict(&mut conn, sync.id, task_id, &merge_sha, 7, 301)
            .unwrap()
            .expect("conflict row publishes");
        assert_eq!(published.phase, "published");
        assert_eq!(published.merge_sha.as_deref(), Some(merge_sha.as_str()));
        assert_eq!(published.pr, Some(7));
        assert_eq!(published.task_id, Some(task_id));
        assert!(published.active);
        assert_eq!(published.sync_branch.as_deref(), Some("sync/1"));
        let event: String = conn
            .query_row(
                "SELECT kind FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", sync.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event, "branch_sync_published");

        // A stale replay cannot rewrite the recorded merge or PR.
        assert!(
            publish_from_conflict(&mut conn, sync.id, task_id, &"d".repeat(40), 8, 302)
                .unwrap()
                .is_none()
        );
        let row = get(&conn, sync.id).unwrap().unwrap();
        assert_eq!(row.merge_sha.as_deref(), Some(merge_sha.as_str()));
        assert_eq!(row.pr, Some(7));

        // The task lifecycle, not the approval-free clean path, owns the PR.
        assert!(next_clean_path(&conn).unwrap().is_none());
        assert!(is_valid_transition("conflict", "published"));
    }

    #[test]
    fn resolve_conflict_done_requires_bound_published_row() {
        let (_dir, mut conn) = open_tmp();
        let (sync, task_id) = bound_judgment_task(&mut conn);
        let merged = "e".repeat(40);
        // Still `conflict`: nothing has been published or merged yet.
        assert!(
            resolve_conflict_done(&mut conn, sync.id, task_id, &merged, 300)
                .unwrap()
                .is_none()
        );
        publish_from_conflict(&mut conn, sync.id, task_id, &"c".repeat(40), 7, 301)
            .unwrap()
            .unwrap();
        assert!(resolve_conflict_done(&mut conn, sync.id, task_id, "", 302).is_err());
        assert!(
            resolve_conflict_done(&mut conn, sync.id, task_id + 1, &merged, 302)
                .unwrap()
                .is_none(),
            "only the bound judgment task may complete the row"
        );
        assert!(get(&conn, sync.id).unwrap().unwrap().active);

        let done = resolve_conflict_done(&mut conn, sync.id, task_id, &merged, 303)
            .unwrap()
            .expect("published judgment row completes");
        assert_eq!(done.phase, "done");
        assert!(!done.active, "completion must release the pair");
        assert!(active_for_pair(&conn, "main", "develop").unwrap().is_none());
        let event: String = conn
            .query_row(
                "SELECT kind FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", sync.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event, "branch_sync_merged");
        // A replay after completion is a clean negative.
        assert!(
            resolve_conflict_done(&mut conn, sync.id, task_id, &merged, 304)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cancel_for_cancelled_task_requires_cancelled_bound_task() {
        let (_dir, mut conn) = open_tmp();
        let (sync, task_id) = bound_judgment_task(&mut conn);
        // An open judgment task does not authorize cancelling its row.
        assert!(next_cancelled_judgment(&conn).unwrap().is_none());
        assert!(cancel_for_cancelled_task(&mut conn, sync.id, task_id, 300)
            .unwrap()
            .is_none());
        assert!(get(&conn, sync.id).unwrap().unwrap().active);

        conn.execute("UPDATE tasks SET status='cancelled' WHERE id=?1", [task_id])
            .unwrap();
        let pending = next_cancelled_judgment(&conn).unwrap().unwrap();
        assert_eq!(pending.id, sync.id);
        assert!(
            cancel_for_cancelled_task(&mut conn, sync.id, task_id + 1, 301)
                .unwrap()
                .is_none(),
            "a different task binding cannot cancel the row"
        );
        let cancelled = cancel_for_cancelled_task(&mut conn, sync.id, task_id, 302)
            .unwrap()
            .expect("cancelled judgment task cancels its row");
        assert_eq!(cancelled.phase, "cancelled");
        assert!(!cancelled.active, "cancellation must release the pair");
        assert!(next_cancelled_judgment(&conn).unwrap().is_none());
        assert!(cancel_for_cancelled_task(&mut conn, sync.id, task_id, 303)
            .unwrap()
            .is_none());
        // The coordinator path still refuses rows that carry a judgment task.
        let unbound = requested(request(&mut conn, "main", "develop", "A", 304).unwrap());
        assert!(matches!(
            cancel_request(&mut conn, sync.id, "A", 305).unwrap(),
            CancelOutcome::NotCancellable(_)
        ));
        assert!(
            cancel_for_cancelled_task(&mut conn, unbound.id, task_id, 306)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn publish_from_conflict_rejects_terminal_rows() {
        let (_dir, mut conn) = open_tmp();
        let (sync, task_id) = bound_judgment_task(&mut conn);
        fail(&mut conn, sync.id, "conflict", "boom", 300)
            .unwrap()
            .unwrap();
        assert!(
            publish_from_conflict(&mut conn, sync.id, task_id, &"c".repeat(40), 7, 301)
                .unwrap()
                .is_none()
        );
        let row = get(&conn, sync.id).unwrap().unwrap();
        assert_eq!(row.phase, "failed");
        assert_eq!(row.pr, None);
    }

    fn classify(conn: &Connection, task_id: i64) {
        conn.execute(
            "UPDATE tasks SET refs=json_set(COALESCE(refs,'{}'),
                 '$.cx_est',2,'$.cx_size','S','$.cx_ready',json('true'),
                 '$.cx_not_ready_reason',json('null'))
             WHERE id=?1",
            [task_id],
        )
        .unwrap();
    }

    fn claimed_judgment_task(conn: &mut Connection) -> (BranchSync, i64) {
        let (sync, task_id) = bound_judgment_task(conn);
        classify(conn, task_id);
        crate::tasks::claim(conn, "Judge", Some(task_id), &[], 3600, 250)
            .unwrap()
            .expect("judgment worker claims");
        (sync, task_id)
    }

    fn assert_failed_without_retry(conn: &Connection, sync_id: i64, task_id: i64, cause: &str) {
        let row = get(conn, sync_id).unwrap().unwrap();
        assert_eq!(row.phase, "failed");
        assert!(!row.active, "failed row releases the pair");
        let last_error = row.last_error.expect("failed row records last_error");
        assert!(last_error.contains(cause), "{last_error}");
        let task = crate::tasks::get(conn, task_id).unwrap().unwrap();
        assert_eq!(
            task.status, "failed",
            "no reopen/retry for the judgment task"
        );
        assert_eq!(task.assignee, None);
        assert!(crate::tasks::list_implementation_ready_open(conn)
            .unwrap()
            .iter()
            .all(|open| open.id != task_id));
        let errors: i64 = conn
            .query_row(
                "SELECT count(*) FROM errors WHERE source='branch_sync'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(errors, 1);
    }

    #[test]
    fn judgment_worker_failure_fails_task_and_sync_row_without_retry() {
        let (_dir, mut conn) = open_tmp();
        let (sync, task_id) = claimed_judgment_task(&mut conn);
        let result = crate::tasks::apply_event(
            &mut conn,
            "daemon",
            task_id,
            &crate::lifecycle::Event::AgentFailed {
                reason: "provider exited".into(),
            },
            300,
        )
        .unwrap();
        assert_eq!(result.task.status, "failed");
        assert!(!result
            .effects
            .contains(&crate::lifecycle::Effect::ResumeWorker));
        assert_failed_without_retry(&conn, sync.id, task_id, "provider exited");
    }

    #[test]
    fn judgment_worker_lease_expiry_fails_task_and_sync_row_without_retry() {
        let (_dir, mut conn) = open_tmp();
        let (sync, task_id) = claimed_judgment_task(&mut conn);
        crate::sweep::reap_lapsed_tasks(&conn, 250 + 3600, SWEEP_LIMIT).unwrap();
        assert_failed_without_retry(&conn, sync.id, task_id, "lease lapsed");
        // A later sweep has nothing left to reclaim.
        crate::sweep::reap_lapsed_tasks(&conn, 9000, SWEEP_LIMIT).unwrap();
        assert_eq!(
            crate::tasks::get(&conn, task_id).unwrap().unwrap().status,
            "failed"
        );
    }

    #[test]
    fn judgment_rework_awaiting_lease_keeps_its_recovered_retry() {
        // After VerdictChanges the lease is released and the task waits in
        // `rework` for the remediation worker. That healthy wait is not a
        // worker lapse: past the provisioning grace it must recover as rework
        // and leave the published sync row alone.
        let (_dir, mut conn) = open_tmp();
        let (sync, task_id) = claimed_judgment_task(&mut conn);
        publish_from_conflict(&mut conn, sync.id, task_id, &"c".repeat(40), 7, 300)
            .unwrap()
            .expect("conflict row publishes");
        conn.execute(
            "UPDATE tasks SET status='rework', assignee=NULL, rework_round=1,
                 refs=json_set(COALESCE(refs,'{}'),'$.pr',7), updated_at=301
             WHERE id=?1",
            [task_id],
        )
        .unwrap();
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1",
            [format!("task#{task_id}")],
        )
        .unwrap();

        let now = 301 + crate::sweep::REWORK_PROVISIONING_GRACE_SECS + 1;
        crate::sweep::reap_lapsed_tasks(&conn, now, SWEEP_LIMIT).unwrap();

        let task = crate::tasks::get(&conn, task_id).unwrap().unwrap();
        assert_eq!(task.status, "rework", "no terminal failure for the wait");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(
            refs[crate::tasks::RECOVERED_REMEDIATION_RETRY_REF],
            serde_json::json!(true),
            "ordinary recovered-retry marker is written"
        );
        let row = get(&conn, sync.id).unwrap().unwrap();
        assert_eq!(row.phase, "published");
        assert!(row.active);
        assert_eq!(row.last_error, None);
        let errors: i64 = conn
            .query_row(
                "SELECT count(*) FROM errors WHERE source='branch_sync'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(errors, 0);

        // A remediation lease that is installed (consuming the retry marker)
        // and then lapses is a real worker lapse: it fails the task and the
        // sync row without retry.
        let unmarked = crate::tasks::clear_recovered_remediation_retry(task.refs.as_deref())
            .unwrap()
            .unwrap();
        conn.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active)
             VALUES (?1, 'Judge2', ?2, ?3, 1)",
            params![format!("task#{task_id}"), now + 1, now + 61],
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET assignee='Judge2', refs=?2, updated_at=?3 WHERE id=?1",
            params![task_id, unmarked, now + 1],
        )
        .unwrap();
        crate::sweep::reap_lapsed_tasks(&conn, now + 61, SWEEP_LIMIT).unwrap();
        assert_failed_without_retry(&conn, sync.id, task_id, "lease lapsed");
    }

    #[test]
    fn ordinary_worker_failure_keeps_its_recovery_retry() {
        let (_dir, mut conn) = open_tmp();
        let task_id = crate::tasks::create(
            &mut conn, "A", "ordinary", None, 0, None, None, None, None, 100,
        )
        .unwrap();
        classify(&conn, task_id);
        crate::tasks::claim(&mut conn, "W", Some(task_id), &[], 3600, 101)
            .unwrap()
            .expect("ordinary claim");
        let result = crate::tasks::apply_event(
            &mut conn,
            "daemon",
            task_id,
            &crate::lifecycle::Event::AgentFailed {
                reason: "provider exited".into(),
            },
            102,
        )
        .unwrap();
        assert_eq!(result.task.status, "open");
    }

    #[test]
    fn list_recent_terminal_returns_newest_first_bounded() {
        let (_dir, mut conn) = open_tmp();
        for (i, (src, dst)) in [
            ("main", "develop"),
            ("release", "develop"),
            ("hotfix", "main"),
        ]
        .into_iter()
        .enumerate()
        {
            let sync = requested(request(&mut conn, src, dst, "A", 100 + i as i64).unwrap());
            set_phase(
                &mut conn,
                sync.id,
                "requested",
                "failed",
                200 + i as i64 * 10,
            )
            .unwrap()
            .unwrap();
        }
        let all = list_recent_terminal(&conn, 10).unwrap();
        assert_eq!(all.len(), 3);
        assert!(all[0].updated_at >= all[1].updated_at);
        assert!(all[1].updated_at >= all[2].updated_at);
        let limited = list_recent_terminal(&conn, 2).unwrap();
        assert_eq!(limited.len(), 2);
    }
}
