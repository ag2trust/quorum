//! Serving-layer glue for worker-work recover refs: release with
//! preservation, the durable trust record, resume notes, owner alerts, and
//! bounded retention. Git mechanics live in `worktree/recover.rs`.

use super::worktree::{self, RecoverIdentity, RecoverRef, RecoverResume, ReleaseError};
use super::{branch_sync_worktree_root, log, now_unix, truncate_utf8_bytes, ServeConfig};
use crate::serve::worktree::WorktreeManager;
use quorum_core::error::Result;
use quorum_core::recover_refs::{self, NewRecoverRef, RecoverRefRow};
use quorum_core::tasks;
use rusqlite::OptionalExtension;
use std::path::Path;

pub(crate) const WORKTREE_RECOVER_TEXT_MAX: usize = 1024;
/// Restored untracked paths listed in a resume note.
const RESTORED_PATHS_MAX: usize = 20;
/// Bound on recover refs retired per housekeeping pass.
const RECOVER_REF_HOUSEKEEPING_MAX_DELETES: i64 = 64;
/// Recover refs of done/cancelled tasks are kept this long after the task's
/// last update so an owner can still recover work after a cancel.
pub(crate) const RECOVER_REF_TERMINAL_GRACE_SECS: i64 = 7 * 24 * 60 * 60;

fn row_to_ref(row: &RecoverRefRow) -> RecoverRef {
    RecoverRef {
        refname: row.refname.clone(),
        sha: row.sha.clone(),
        snapshot_parent: row.parent_sha.clone(),
        tracked_only: row.tracked_only,
        untracked_skip: None,
    }
}

/// Recover refs the daemon recorded for `task_id`, newest first.
pub(crate) async fn trusted_recover_refs(
    db_path: &Path,
    task_id: i64,
) -> std::result::Result<Vec<RecoverRef>, String> {
    let path = db_path.to_path_buf();
    let rows = tokio::task::spawn_blocking(move || -> Result<Vec<RecoverRefRow>> {
        let conn = quorum_core::db::open(&path)?;
        recover_refs::for_task(&conn, task_id)
    })
    .await
    .map_err(|e| format!("recover ref lookup join failure: {e}"))?
    .map_err(|e| format!("recover ref lookup failed: {e}"))?;
    Ok(rows.iter().map(row_to_ref).collect())
}

/// Newest recorded recover ref for the task. Refs in the task's bucket that
/// the daemon did not record (e.g. written by a worker) are logged and never
/// returned.
pub(crate) async fn latest_trusted_recover(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    task_id: i64,
) -> std::result::Result<Option<RecoverRef>, String> {
    let trusted = trusted_recover_refs(&config.db_path, task_id).await?;
    if let Ok(listed) = wt_mgr.list_recover_refs(&config.repo_dir).await {
        for recover in listed {
            if worktree::recover_ref_task_id(&recover.refname) == Some(task_id)
                && !trusted
                    .iter()
                    .any(|t| t.refname == recover.refname && t.sha == recover.sha)
            {
                log(&format!(
                    "task #{task_id}: ignoring unrecorded recover ref {} ({}) — not daemon-created",
                    recover.refname, recover.sha
                ));
            }
        }
    }
    Ok(trusted.into_iter().next())
}

/// Durably record refs the daemon just created, BEFORE anything is
/// destroyed. Unattributed refs are never recorded (never trusted). Any
/// failure is returned so the caller destroys nothing.
pub(crate) async fn record_recover_rows(
    db_path: &Path,
    identity: &RecoverIdentity,
    created: &[RecoverRef],
) -> std::result::Result<(), String> {
    let Some(task_id) = identity.task_id else {
        return Ok(());
    };
    let path = db_path.to_path_buf();
    let rows: Vec<RecoverRef> = created.to_vec();
    let (agent, reason) = (identity.agent.clone(), identity.reason.clone());
    let recorded = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
        let mut conn = quorum_core::db::open(&path)?;
        let mut refused = Vec::new();
        for recover in &rows {
            if !recover_refs::record(
                &mut conn,
                &NewRecoverRef {
                    refname: &recover.refname,
                    task_id,
                    sha: &recover.sha,
                    parent_sha: recover.snapshot_parent.as_deref(),
                    tracked_only: recover.tracked_only,
                    agent: &agent,
                    reason: &reason,
                },
                now_unix(),
            )? {
                refused.push(recover.refname.clone());
            }
        }
        Ok(refused)
    })
    .await
    .map_err(|e| format!("recover ref record join failure: {e}"))?
    .map_err(|e| format!("recover ref record failed: {e}"))?;
    if recorded.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "recover ref name(s) already recorded with another SHA: {}",
            recorded.join(", ")
        ))
    }
}

/// After a successful preserve+destroy: log, surface tracked-only snapshots
/// (errors row, owner alert, daemon note), and enforce bounded retention.
pub(crate) async fn after_recover_refs_created(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    identity: &RecoverIdentity,
    created: &[RecoverRef],
) {
    let Some(task_id) = identity.task_id else {
        for recover in created {
            log(&format!(
                "preserved unattributed worker work at {} ({}) — never auto-resumed",
                recover.refname, recover.sha
            ));
        }
        return;
    };
    for recover in created {
        log(&format!(
            "preserved worker work for task #{task_id} ({}, {}) at {} ({})",
            identity.agent, identity.reason, recover.refname, recover.sha
        ));
        if let Some(skip) = recover.untracked_skip.as_deref() {
            let body = format!(
                "teardown of worker {} ({}) preserved TRACKED changes only at {}: untracked \
                 files could not be captured ({skip}). They were not preserved.",
                identity.agent, identity.reason, recover.refname
            );
            record_worktree_recover_error(&config.db_path, Some(task_id), &recover.refname, &body)
                .await;
            add_daemon_note(&config.db_path, task_id, &body).await;
        }
    }
    if !created.is_empty() {
        enforce_recover_retention(config, wt_mgr, task_id).await;
    }
}

async fn enforce_recover_retention(config: &ServeConfig, wt_mgr: &WorktreeManager, task_id: i64) {
    let rows = match trusted_recover_refs(&config.db_path, task_id).await {
        Ok(rows) => rows,
        Err(error) => {
            log(&format!(
                "task #{task_id}: recover retention skipped: {error}"
            ));
            return;
        }
    };
    let excess: Vec<RecoverRef> = rows
        .into_iter()
        .skip(worktree::RECOVER_REFS_PER_TASK)
        .collect();
    retire_recorded(config, wt_mgr, &excess).await;
}

/// Delete each ref (if it still names the recorded SHA) and its row.
async fn retire_recorded(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    refs: &[RecoverRef],
) -> usize {
    let mut retired = 0;
    for recover in refs {
        if let Err(error) = wt_mgr.delete_recover_ref(&config.repo_dir, recover).await {
            log(&format!("recover ref retirement: {error}"));
        }
        let path = config.db_path.clone();
        let (refname, sha) = (recover.refname.clone(), recover.sha.clone());
        let deleted = tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut conn = quorum_core::db::open(&path)?;
            recover_refs::delete(&mut conn, &refname, &sha)
        })
        .await;
        if matches!(deleted, Ok(Ok(true))) {
            retired += 1;
        }
    }
    retired
}

/// Release a worker slot's worktree and local branch. An adopted kept sync
/// merge is owned by the branch-sync reconciler, not the slot: removing it
/// would erase MERGE_HEAD and delete `sync/<id>`, so every retry of the
/// judgment task would fail adoption.
///
/// Every other worker checkout is snapshotted to a `refs/quorum-recover/`
/// ref before the force removal / branch deletion whenever that would lose
/// uncommitted or unpushed work, and the ref is recorded as daemon-created.
/// A failed snapshot destroys nothing: the worktree and branch stay for a
/// later attempt (the next provisioning of the task or startup GC), and the
/// failure is recorded in `errors` with a bounded owner alert.
pub(crate) async fn release_worker_worktree(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    worktree: &Path,
    branch: &str,
    delete_branch: bool,
    identity: &RecoverIdentity,
) -> std::result::Result<(), ReleaseError> {
    if worktree.starts_with(branch_sync_worktree_root(config)) {
        log(&format!(
            "preserving adopted branch-sync worktree {} (branch {branch})",
            worktree.display()
        ));
        return Ok(());
    }
    let mut identity = identity.clone();
    if let Some(task_id) = identity.task_id {
        match trusted_recover_refs(&config.db_path, task_id).await {
            Ok(trusted) => identity.trusted = trusted,
            Err(error) => log(&format!(
                "task #{task_id}: trusted recover refs unavailable ({error}); snapshotting without dedupe"
            )),
        }
    }
    let db_path = config.db_path.clone();
    let record_identity = identity.clone();
    match wt_mgr
        .release_preserving(
            &config.repo_dir,
            worktree,
            branch,
            delete_branch,
            &identity,
            |created| async move { record_recover_rows(&db_path, &record_identity, &created).await },
        )
        .await
    {
        Ok(created) => {
            after_recover_refs_created(config, wt_mgr, &identity, &created).await;
            Ok(())
        }
        Err(error) => {
            let detail = format!(
                "task #{} worker {} teardown ({}) of {} [{branch}]: {error}",
                identity.task_id.unwrap_or(0),
                identity.agent,
                identity.reason,
                worktree.display()
            );
            log(&detail);
            if matches!(error, ReleaseError::Preserve(_)) {
                record_worktree_recover_error(
                    &config.db_path,
                    identity.task_id,
                    &worktree.to_string_lossy(),
                    &detail,
                )
                .await;
            }
            Err(error)
        }
    }
}

/// Preserve and release a worker checkout an earlier run of this task left
/// at `worktree` (its teardown snapshot failed, or the daemon died first).
/// Called only after this process claimed the task, so no live worker owns
/// it. Destroys nothing and returns an error if preservation fails.
///
/// The snapshot is attributed to the task (and so trusted for resume) only
/// when the checkout's registered branch AND path equal the task's daemon
/// allocation record; a worker-forged checkout at this path is preserved
/// unattributed and never resumed. An unregistered directory is left to
/// provisioning exactly as before.
pub(crate) async fn release_leftover_worker_checkout(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    worktree: &Path,
    branch: &str,
    delete_branch: bool,
    task_id: i64,
) -> std::result::Result<(), String> {
    match std::fs::symlink_metadata(worktree) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "cannot inspect leftover worker checkout {}: {error}",
                worktree.display()
            ))
        }
        Ok(_) => {}
    }
    let Some(registered) = wt_mgr.registered_branch(&config.repo_dir, worktree).await? else {
        return Ok(());
    };
    let path = config.db_path.clone();
    let allocation = tokio::task::spawn_blocking(move || -> Result<Option<(String, String)>> {
        let conn = quorum_core::db::open(&path)?;
        Ok(conn
            .query_row(
                "SELECT branch, worktree FROM task_branches WHERE task_id=?1",
                [task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?)
    })
    .await
    .map_err(|e| format!("leftover allocation lookup join failure: {e}"))?
    .map_err(|e| format!("leftover allocation lookup failed: {e}"))?;
    let allocations: Vec<(i64, String, String)> = allocation
        .into_iter()
        .map(|(branch, worktree)| (task_id, branch, worktree))
        .collect();
    let attributed = worktree::daemon_attribution(worktree, registered.as_deref(), &allocations);
    let agent = worktree
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let identity = match attributed {
        Some(id) if id == task_id => RecoverIdentity::new(task_id, &agent, "leftover_checkout"),
        _ => {
            log(&format!(
                "task #{task_id}: leftover checkout {} ({registered:?}) does not match the task's \
                 allocation; preserving it unattributed",
                worktree.display()
            ));
            RecoverIdentity {
                task_id: None,
                agent,
                reason: "leftover_checkout".into(),
                trusted: Vec::new(),
            }
        }
    };
    release_worker_worktree(config, wt_mgr, worktree, branch, delete_branch, &identity)
        .await
        .map_err(|error| format!("leftover worker checkout {}: {error}", worktree.display()))
}

/// Loud, bounded record of a work-preservation problem: an `errors` row and
/// at most one live owner alert per (task, subject).
pub(crate) async fn record_worktree_recover_error(
    db_path: &Path,
    task_id: Option<i64>,
    subject: &str,
    detail: &str,
) {
    let path = db_path.to_path_buf();
    let detail = truncate_utf8_bytes(detail, WORKTREE_RECOVER_TEXT_MAX).to_string();
    let refs = truncate_utf8_bytes(
        &format!(
            "task:{},worktree_recover:{subject}",
            task_id.map_or_else(|| "unattributed".to_string(), |id| id.to_string())
        ),
        512,
    )
    .to_string();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&path)?;
        let now = now_unix();
        quorum_core::errlog::log_error(&conn, now, "worktree_recover", &detail);
        let already: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM messages WHERE kind='alert' AND refs=?1 AND expires_at > ?2)",
            rusqlite::params![refs, now],
            |row| row.get(0),
        )?;
        if !already {
            quorum_core::feed::post(
                &mut conn,
                "daemon",
                "alert",
                None,
                &format!("worker work preservation: {detail}"),
                Some(&refs),
                Some("owner"),
                quorum_core::feed::DEFAULT_MESSAGE_TTL_SECS,
                now,
            )?;
        }
        Ok(())
    })
    .await;
    if !matches!(result, Ok(Ok(()))) {
        log(&format!("worktree recover error record failed: {result:?}"));
    }
}

/// Append a bounded daemon note unless an identical one already exists.
async fn add_daemon_note(db_path: &Path, task_id: i64, body: &str) {
    let path = db_path.to_path_buf();
    let body = truncate_utf8_bytes(body, WORKTREE_RECOVER_TEXT_MAX).to_string();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&path)?;
        let tx = quorum_core::db::begin_immediate(&mut conn)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM task_notes WHERE task_id=?1 AND agent='daemon' AND body=?2)",
            rusqlite::params![task_id, body],
            |row| row.get(0),
        )?;
        if !exists {
            tasks::add_note_tx(&tx, "daemon", task_id, &body, now_unix())?;
        }
        tx.commit()?;
        Ok(())
    })
    .await;
    if !matches!(result, Ok(Ok(()))) {
        log(&format!(
            "task #{task_id}: recover note write failed: {result:?}"
        ));
    }
}

/// Restore a resumed snapshot's uncommitted changes into the provisioned
/// worktree. `Ok(None)` when the outcome needs no restore; `Err` when the
/// worktree could not be returned to a clean state (fail provisioning).
pub(crate) async fn restore_resumed_snapshot(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    worktree: &Path,
    outcome: Option<&std::result::Result<RecoverResume, String>>,
) -> std::result::Result<Option<std::result::Result<Vec<String>, String>>, String> {
    match outcome {
        Some(Ok(RecoverResume::Resumed {
            recover,
            restore_uncommitted: true,
            ..
        })) => wt_mgr
            .restore_snapshot_as_uncommitted(&config.repo_dir, worktree, recover)
            .await
            .map(Some),
        _ => Ok(None),
    }
}

/// Task-note body (bounded) describing a resume decision for the next
/// worker. `None` for outcomes that need no note.
pub(crate) fn recover_resume_note(
    agent: &str,
    start: &str,
    outcome: &std::result::Result<RecoverResume, String>,
    restored: Option<&std::result::Result<Vec<String>, String>>,
) -> Option<String> {
    let body = match outcome {
        Ok(RecoverResume::None) | Ok(RecoverResume::AlreadyIncorporated(_)) => return None,
        Ok(RecoverResume::Resumed { recover, .. }) => {
            let (prior, reason) = recover.provenance();
            let mut body = format!(
                "worker {agent} resumed from {} ({}), which preserves prior run {prior}'s \
                 work (teardown reason: {reason}). Its commits are on the branch.",
                recover.refname, recover.sha
            );
            match restored {
                Some(Ok(added)) => {
                    body.push_str(
                        " Its uncommitted changes were restored as UNCOMMITTED working-tree \
                         changes (nothing was committed for you). Review them, especially \
                         restored untracked files, before committing.",
                    );
                    if recover.tracked_only {
                        body.push_str(" Untracked files were NOT captured at teardown.");
                    }
                    if !added.is_empty() {
                        let shown: Vec<&str> = added
                            .iter()
                            .take(RESTORED_PATHS_MAX)
                            .map(String::as_str)
                            .collect();
                        body.push_str(&format!(" Restored new files: {}", shown.join(", ")));
                        if added.len() > RESTORED_PATHS_MAX {
                            body.push_str(&format!(
                                " (+{} more)",
                                added.len() - RESTORED_PATHS_MAX
                            ));
                        }
                        body.push('.');
                    }
                }
                Some(Err(error)) => body.push_str(&format!(
                    " Restoring its uncommitted changes FAILED ({error}); they remain in {} for \
                     manual recovery.",
                    recover.refname
                )),
                None => {}
            }
            body
        }
        Ok(RecoverResume::Skipped { recover, reason }) => format!(
            "recover ref {} ({}) was NOT applied: {reason}. It is preserved for manual \
             recovery; this run starts without it.",
            recover.refname, recover.sha
        ),
        Ok(RecoverResume::Stale { recover, onto }) => {
            let (prior, reason) = recover.provenance();
            format!(
                "recover ref {} ({}) from prior run {prior} (teardown reason: {reason}) was NOT \
                 applied: it does not descend from the {start} ({onto}). It is preserved for \
                 manual recovery; this run starts without it.",
                recover.refname, recover.sha
            )
        }
        Err(error) => format!(
            "resume from a recover ref failed: {error}. Any recover refs are preserved; \
             this run starts without them."
        ),
    };
    Some(truncate_utf8_bytes(&body, WORKTREE_RECOVER_TEXT_MAX).to_string())
}

/// Record a resume decision as a daemon task note (deduplicated by body so a
/// repeatedly stale ref does not grow notes); failures also go to `errors`.
pub(crate) async fn record_recover_resume(
    db_path: &Path,
    task_id: i64,
    agent: &str,
    start: &str,
    outcome: &std::result::Result<RecoverResume, String>,
    restored: Option<&std::result::Result<Vec<String>, String>>,
) {
    let Some(body) = recover_resume_note(agent, start, outcome, restored) else {
        return;
    };
    log(&format!("task #{task_id}: {body}"));
    if outcome.is_err() || matches!(restored, Some(Err(_))) {
        record_worktree_recover_error(
            db_path,
            Some(task_id),
            "resume",
            &format!("task #{task_id}: {body}"),
        )
        .await;
    }
    add_daemon_note(db_path, task_id, &body).await;
}

/// Retire recover refs and records of tasks that reached `done`/`cancelled`
/// more than the grace period ago, including unrecorded refs in those tasks'
/// buckets. Refs of unknown or
/// live tasks, and every non-recover ref, are left untouched.
pub(crate) async fn prune_terminal_recover_refs(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
) -> std::result::Result<usize, String> {
    let path = config.db_path.clone();
    let rows = tokio::task::spawn_blocking(move || -> Result<Vec<RecoverRefRow>> {
        let conn = quorum_core::db::open(&path)?;
        recover_refs::terminal_rows(
            &conn,
            now_unix() - RECOVER_REF_TERMINAL_GRACE_SECS,
            RECOVER_REF_HOUSEKEEPING_MAX_DELETES,
        )
    })
    .await
    .map_err(|e| format!("recover ref housekeeping join failure: {e}"))?
    .map_err(|e| format!("recover ref housekeeping DB read failed: {e}"))?;
    let recorded: Vec<RecoverRef> = rows.iter().map(row_to_ref).collect();
    let mut retired = retire_recorded(config, wt_mgr, &recorded).await;

    // Unrecorded refs in terminal tasks' buckets (never trusted) go too.
    let mut by_task: std::collections::BTreeMap<i64, Vec<RecoverRef>> =
        std::collections::BTreeMap::new();
    for recover in wt_mgr.list_recover_refs(&config.repo_dir).await? {
        if let Some(task_id) = worktree::recover_ref_task_id(&recover.refname) {
            by_task.entry(task_id).or_default().push(recover);
        }
    }
    if !by_task.is_empty() {
        let path = config.db_path.clone();
        let task_ids: Vec<i64> = by_task.keys().copied().collect();
        let terminal = tokio::task::spawn_blocking(move || -> Result<Vec<i64>> {
            let conn = quorum_core::db::open(&path)?;
            let mut stmt = conn.prepare(
                "SELECT 1 FROM tasks WHERE id=?1 AND status IN ('done','cancelled') \
                 AND updated_at <= ?2",
            )?;
            let cutoff = now_unix() - RECOVER_REF_TERMINAL_GRACE_SECS;
            let mut terminal = Vec::new();
            for task_id in task_ids {
                if stmt.exists(rusqlite::params![task_id, cutoff])? {
                    terminal.push(task_id);
                }
            }
            Ok(terminal)
        })
        .await
        .map_err(|e| format!("recover ref housekeeping join failure: {e}"))?
        .map_err(|e| format!("recover ref housekeeping DB read failed: {e}"))?;
        for recover in terminal
            .into_iter()
            .flat_map(|task_id| by_task.remove(&task_id).unwrap_or_default())
            .take(RECOVER_REF_HOUSEKEEPING_MAX_DELETES as usize)
        {
            match wt_mgr.delete_recover_ref(&config.repo_dir, &recover).await {
                Ok(()) => retired += 1,
                Err(error) => log(&format!("recover ref housekeeping: {error}")),
            }
        }
    }
    if retired > 0 {
        log(&format!(
            "retired {retired} recover ref(s) of done/cancelled tasks"
        ));
    }
    Ok(retired)
}
