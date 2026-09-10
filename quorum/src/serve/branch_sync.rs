//! Daemon-owned execution and restart reconciliation for branch sync rows.
//!
//! Each pass reads one durable row, does all GitHub/Git work without a
//! database transaction, then settles exactly the phase that operation earned.

use super::merge::{self, MergeCommitStatus};
use super::worktree::{SyncMerge, WorktreeManager};
use super::{
    log, parse_created_pr_number, parse_initial_pr_list, resolve_pr_target_with_program,
    run_publication_gh_command, validate_initial_pr_target, PrTarget, ServeConfig,
};
use quorum_core::branch_sync::{self, BranchSync};
use quorum_core::error::{QuorumError, Result};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

const PR_OPEN: &str = "OPEN";
/// Branch-sync CI gets the same finite policy-retry budget as a merge. The
/// first complete wait is followed by at most this many durable retries.
const MAX_BRANCH_SYNC_CHECK_ATTEMPTS: i64 = merge::MAX_POLICY_RETRIES as i64 + 1;
/// A small global cap leaves Tokio's blocking pool available for SQLite and
/// lifecycle work even when an owner configures many independent sync pairs.
const MAX_BRANCH_SYNC_CHECK_WAITS: usize = 2;
const CHECK_RETRY_LIMIT_ERROR: &str = "branch sync CI wait retry limit exceeded";

/// Retained branch-sync CI work. At most [`MAX_BRANCH_SYNC_CHECK_WAITS`] full
/// waits exist in this map; retry count and cadence are durable row fields.
#[derive(Default)]
pub struct BranchSyncChecks {
    waits: HashMap<i64, BranchSyncCheckWait>,
}

struct BranchSyncCheckWait {
    pr: i64,
    attempts: i64,
    handle: JoinHandle<merge::ChecksOutcome>,
}

impl BranchSyncChecks {
    fn excluded_ids(&self) -> Vec<i64> {
        self.waits
            .iter()
            .filter_map(|(id, wait)| (!wait.handle.is_finished()).then_some(*id))
            .collect()
    }

    /// Once the cap is full, only already-admitted rows may be selected. A
    /// completed handle remains selectable for settlement, while an excess
    /// durable `checks` row waits fairly for that slot to be released.
    fn admitted_check_ids_if_full(&self) -> Option<Vec<i64>> {
        (self.waits.len() >= MAX_BRANCH_SYNC_CHECK_WAITS)
            .then(|| self.waits.keys().copied().collect())
    }

    fn cancel(&mut self, id: i64) {
        if let Some(wait) = self.waits.remove(&id) {
            wait.handle.abort();
        }
    }
}

impl Drop for BranchSyncChecks {
    fn drop(&mut self) {
        for wait in self.waits.values_mut() {
            wait.handle.abort();
        }
    }
}

/// Reconcile one active clean-path row. Invoking this on startup and once per
/// normal tick gives crash recovery without an unbounded non-task scan.
pub async fn reconcile_one(
    config: &ServeConfig,
    worktrees: &WorktreeManager,
    checks: &mut BranchSyncChecks,
) -> Result<()> {
    let db_path = config.db_path.clone();
    let excluded = checks.excluded_ids();
    let admitted_check_ids = checks.admitted_check_ids_if_full();
    let now = quorum_core::clock::now();
    let sync = tokio::task::spawn_blocking(move || -> Result<Option<BranchSync>> {
        let conn = quorum_core::db::open(&db_path)?;
        branch_sync::next_clean_path_excluding_at(
            &conn,
            &excluded,
            admitted_check_ids.as_deref(),
            now,
        )
    })
    .await
    .map_err(|error| QuorumError::Io(format!("branch sync selection join: {error}")))??;
    let Some(sync) = sync else {
        return Ok(());
    };

    let result = match sync.phase.as_str() {
        "requested" => pin_requested(config, worktrees, &sync).await,
        "pinned" => prepare_pinned(config, worktrees, &sync).await,
        "prepared" => publish_prepared(config, worktrees, &sync).await,
        "published" => begin_published_checks(config, &sync).await,
        "checks" => run_checks(config, &sync, checks).await,
        "merging" => reconcile_merge(config, worktrees, &sync).await,
        // `next_clean_path` is intentionally narrower than the persisted
        // vocabulary, so this means the database query and row parser no
        // longer agree rather than silently ignoring a future phase.
        phase => Err(format!("unsupported active branch sync phase {phase}")),
    };
    if let Err(error) = result {
        fail(config, &sync, &error).await?;
        log(&format!(
            "branch sync #{} {} failed: {error}",
            sync.id, sync.phase
        ));
    }
    Ok(())
}

fn sync_branch(sync: &BranchSync) -> String {
    // The row ID is globally unique and durable, which makes it sufficient for
    // restart reconciliation without concatenating owner-controlled branch
    // names into a filesystem-backed ref component. Source/target remain in
    // the row and PR title, while this name stays far below Git's component
    // limit even when both configured branches are individually maximal.
    format!("sync/{}", sync.id)
}

fn sync_worktree(config: &ServeConfig, sync: &BranchSync) -> std::path::PathBuf {
    // Task recovery garbage-collects only direct children of worktree_base.
    // Keep a judgment-pending sync merge in its own daemon namespace so that
    // task cleanup cannot erase MERGE_HEAD before the conflict path adopts it.
    let root = config
        .worktree_base
        .parent()
        .unwrap_or(config.worktree_base.as_path());
    root.join("branch-sync-worktrees").join(sync.id.to_string())
}

async fn pin_requested(
    config: &ServeConfig,
    worktrees: &WorktreeManager,
    sync: &BranchSync,
) -> std::result::Result<(), String> {
    let (source_sha, target_sha) = worktrees
        .fetch_sync_tips(&config.repo_dir, &sync.source_branch, &sync.target_branch)
        .await?;
    let branch = sync_branch(sync);
    let db_path = config.db_path.clone();
    let id = sync.id;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&db_path)?;
        let _ = branch_sync::pin(
            &mut conn,
            id,
            &source_sha,
            &target_sha,
            &branch,
            quorum_core::clock::now(),
        )?;
        Ok(())
    })
    .await
    .map_err(|error| format!("branch sync pin settlement join: {error}"))?
    .map_err(|error| error.to_string())
}

async fn prepare_pinned(
    config: &ServeConfig,
    worktrees: &WorktreeManager,
    sync: &BranchSync,
) -> std::result::Result<(), String> {
    let source_sha = required(sync.source_sha.as_deref(), "source_sha")?;
    let target_sha = required(sync.target_sha.as_deref(), "target_sha")?;
    let branch = required(sync.sync_branch.as_deref(), "sync_branch")?;

    if worktrees
        .source_is_ancestor_of_target(&config.repo_dir, source_sha, target_sha)
        .await?
    {
        let db_path = config.db_path.clone();
        let id = sync.id;
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&db_path)?;
            let _ = branch_sync::noop(&mut conn, id, quorum_core::clock::now())?;
            Ok(())
        })
        .await
        .map_err(|error| format!("branch sync noop settlement join: {error}"))?
        .map_err(|error| error.to_string())?;
        return Ok(());
    }

    match worktrees
        .prepare_sync_merge(
            &config.repo_dir,
            &sync_worktree(config, sync),
            branch,
            source_sha,
            target_sha,
        )
        .await?
    {
        SyncMerge::Clean { merge_sha } => {
            let db_path = config.db_path.clone();
            let id = sync.id;
            let branch = branch.to_string();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = quorum_core::db::open(&db_path)?;
                let _ = branch_sync::prepared(
                    &mut conn,
                    id,
                    &branch,
                    &merge_sha,
                    quorum_core::clock::now(),
                )?;
                Ok(())
            })
            .await
            .map_err(|error| format!("branch sync prepared settlement join: {error}"))?
            .map_err(|error| error.to_string())
        }
        SyncMerge::Conflicted => {
            let db_path = config.db_path.clone();
            let id = sync.id;
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = quorum_core::db::open(&db_path)?;
                let _ = branch_sync::conflict(&mut conn, id, quorum_core::clock::now())?;
                Ok(())
            })
            .await
            .map_err(|error| format!("branch sync conflict settlement join: {error}"))?
            .map_err(|error| error.to_string())
        }
    }
}

async fn publish_prepared(
    config: &ServeConfig,
    worktrees: &WorktreeManager,
    sync: &BranchSync,
) -> std::result::Result<(), String> {
    let branch = required(sync.sync_branch.as_deref(), "sync_branch")?;
    let merge_sha = required(sync.merge_sha.as_deref(), "merge_sha")?;
    let worktree = sync_worktree(config, sync);
    // Prepared state may recover a missing worktree, but never a missing
    // local branch: that would be a different merge than the recorded SHA.
    worktrees
        .open_existing_sync_worktree(&config.repo_dir, &worktree, branch)
        .await?;
    worktrees.verify_head_sha(&worktree, merge_sha).await?;
    let pushed = worktrees
        .push_new_branch(&worktree, branch, merge_sha)
        .await?;
    if pushed != merge_sha {
        return Err(format!(
            "branch sync push returned unexpected SHA: expected {merge_sha}, got {pushed}"
        ));
    }

    // A crash after PR creation but before the durable phase update has an
    // already-open PR. Adopt only the uniquely matching open branch, then
    // validate its exact head/base before recording it.
    let pr = match find_branch_sync_pr(config, branch).await? {
        Some(pr) => pr,
        None => create_pr(config, sync, branch).await?,
    };
    let target = resolve_pr_target(config, pr).await?;
    validate_initial_pr_target(&target, branch, merge_sha, &sync.target_branch)?;
    if target.state.as_deref() != Some(PR_OPEN) {
        return Err(format!("branch sync PR #{pr} is not open"));
    }

    let db_path = config.db_path.clone();
    let id = sync.id;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&db_path)?;
        let _ = branch_sync::published(&mut conn, id, pr, quorum_core::clock::now())?;
        Ok(())
    })
    .await
    .map_err(|error| format!("branch sync published settlement join: {error}"))?
    .map_err(|error| error.to_string())
}

async fn find_branch_sync_pr(
    config: &ServeConfig,
    branch: &str,
) -> std::result::Result<Option<i64>, String> {
    let mut args = vec![
        "pr".to_string(),
        "list".to_string(),
        "--state".to_string(),
        "all".to_string(),
        "--head".to_string(),
        branch.to_string(),
        "--limit".to_string(),
        "2".to_string(),
        "--json".to_string(),
        "number,state".to_string(),
    ];
    if !config.repo.is_empty() {
        args.push("--repo".to_string());
        args.push(config.repo.clone());
    }
    let output = run_branch_sync_gh(config, &args, "gh pr list").await?;
    if !output.status.success() {
        return Err(format!(
            "gh pr list failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    parse_initial_pr_list(&output.stdout, branch)
}

async fn begin_published_checks(
    config: &ServeConfig,
    sync: &BranchSync,
) -> std::result::Result<(), String> {
    let pr = sync
        .pr
        .filter(|pr| *pr > 0)
        .ok_or_else(|| "missing pr".to_string())?;
    let merge_sha = required(sync.merge_sha.as_deref(), "merge_sha")?;
    let target = resolve_pr_target(config, pr).await?;
    validate_published_target(&target, merge_sha, &sync.target_branch)?;
    let db_path = config.db_path.clone();
    let id = sync.id;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&db_path)?;
        let _ = branch_sync::begin_checks(&mut conn, id, quorum_core::clock::now())?;
        Ok(())
    })
    .await
    .map_err(|error| format!("branch sync checks admission join: {error}"))?
    .map_err(|error| error.to_string())
}

const REQUIRED_JOBS_ERROR: &str = "branch sync requires a non-empty required_jobs gate";

async fn run_checks(
    config: &ServeConfig,
    sync: &BranchSync,
    waits: &mut BranchSyncChecks,
) -> std::result::Result<(), String> {
    if config.required_jobs.is_empty() {
        waits.cancel(sync.id);
        return Err(REQUIRED_JOBS_ERROR.to_string());
    }
    let pr = required_pr(sync)?;
    if waits.waits.get(&sync.id).is_some_and(|wait| wait.pr != pr) {
        waits.cancel(sync.id);
    }

    let wait = waits.waits.remove(&sync.id);
    let Some(wait) = wait else {
        return admit_checks_wait(config, sync, waits, pr).await;
    };
    if !wait.handle.is_finished() {
        waits.waits.insert(sync.id, wait);
        return Ok(());
    }
    let checks = wait
        .handle
        .await
        .map_err(|error| format!("branch sync checks join: {error}"))?;

    settle_checks(config, sync, pr, wait.attempts, checks).await
}

async fn admit_checks_wait(
    config: &ServeConfig,
    sync: &BranchSync,
    waits: &mut BranchSyncChecks,
    pr: i64,
) -> std::result::Result<(), String> {
    if waits.waits.len() >= MAX_BRANCH_SYNC_CHECK_WAITS {
        return Ok(());
    }
    if !sync.ci_wait_inflight && sync.ci_attempts >= MAX_BRANCH_SYNC_CHECK_ATTEMPTS {
        return Err(check_retry_limit_error(pr, sync.ci_attempts));
    }

    let db_path = config.db_path.clone();
    let id = sync.id;
    let admitted = tokio::task::spawn_blocking(move || -> Result<Option<BranchSync>> {
        let mut conn = quorum_core::db::open(&db_path)?;
        branch_sync::admit_check_wait(
            &mut conn,
            id,
            MAX_BRANCH_SYNC_CHECK_ATTEMPTS,
            quorum_core::clock::now(),
        )
    })
    .await
    .map_err(|error| format!("branch sync checks admission join: {error}"))?
    .map_err(|error| error.to_string())?;
    let Some(admitted) = admitted else {
        return Ok(());
    };

    let repo = config.repo_dir.clone();
    let executor = Arc::clone(&config.merge_executor);
    let timeout = config.merge_checks_timeout_secs;
    let poll = config.merge_checks_poll_secs;
    let handle = tokio::task::spawn_blocking(move || {
        executor.wait_for_branch_sync_checks(pr, &repo, timeout, poll)
    });
    waits.waits.insert(
        sync.id,
        BranchSyncCheckWait {
            pr,
            attempts: admitted.ci_attempts,
            handle,
        },
    );
    Ok(())
}

fn check_retry_limit_error(pr: i64, attempts: i64) -> String {
    format!("{CHECK_RETRY_LIMIT_ERROR} after {attempts} attempts for PR #{pr}")
}

fn check_retry_delay_secs(config: &ServeConfig, attempts: i64) -> u64 {
    let exponent = u32::try_from(attempts.saturating_sub(1))
        .unwrap_or(u32::MAX)
        .min(63);
    config
        .merge_checks_poll_secs
        .saturating_mul(1_u64 << exponent)
        .min(config.merge_checks_timeout_secs)
}

async fn settle_checks(
    config: &ServeConfig,
    sync: &BranchSync,
    pr: i64,
    attempts: i64,
    mut checks: merge::ChecksOutcome,
) -> std::result::Result<(), String> {
    if matches!(checks, merge::ChecksOutcome::Ready) {
        let required_jobs = {
            let repo = config.repo_dir.clone();
            let executor = Arc::clone(&config.merge_executor);
            let jobs = config.required_jobs.clone();
            tokio::task::spawn_blocking(move || executor.check_required_jobs(pr, &repo, &jobs))
                .await
                .map_err(|error| format!("branch sync required-jobs join: {error}"))?
        };
        checks = merge::apply_required_jobs_gate(checks, required_jobs);
    }

    match checks {
        merge::ChecksOutcome::Ready => {
            let db_path = config.db_path.clone();
            let id = sync.id;
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = quorum_core::db::open(&db_path)?;
                let _ = branch_sync::begin_merge_attempt(&mut conn, id, quorum_core::clock::now())?;
                Ok(())
            })
            .await
            .map_err(|error| format!("branch sync merge admission join: {error}"))?
            .map_err(|error| error.to_string())
        }
        merge::ChecksOutcome::Failed { failing_checks } => {
            let detail = format!("PR #{pr} CI failed: {}", failing_checks.join(", "));
            let db_path = config.db_path.clone();
            let id = sync.id;
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = quorum_core::db::open(&db_path)?;
                let _ = branch_sync::ci_failed(&mut conn, id, &detail, quorum_core::clock::now())?;
                Ok(())
            })
            .await
            .map_err(|error| format!("branch sync CI failure settlement join: {error}"))?
            .map_err(|error| error.to_string())
        }
        merge::ChecksOutcome::Pending { .. } | merge::ChecksOutcome::TimedOut => {
            if attempts >= MAX_BRANCH_SYNC_CHECK_ATTEMPTS {
                return Err(check_retry_limit_error(pr, attempts));
            }
            let delay = check_retry_delay_secs(config, attempts);
            let db_path = config.db_path.clone();
            let id = sync.id;
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = quorum_core::db::open(&db_path)?;
                let now = quorum_core::clock::now();
                let next_attempt_at = now.saturating_add(delay.try_into().unwrap_or(i64::MAX));
                let _ = branch_sync::schedule_check_retry(&mut conn, id, next_attempt_at, now)?;
                Ok(())
            })
            .await
            .map_err(|error| format!("branch sync CI retry settlement join: {error}"))?
            .map_err(|error| error.to_string())
        }
    }
}

/// Reconcile the durable merge boundary. A live `merging` row may represent a
/// crash before or after the one remote call, so query GitHub first; only an
/// open PR receives the bounded single retry with the original pinned head.
async fn reconcile_merge(
    config: &ServeConfig,
    worktrees: &WorktreeManager,
    sync: &BranchSync,
) -> std::result::Result<(), String> {
    let pr = required_pr(sync)?;
    let status = {
        let repo = config.repo_dir.clone();
        let executor = Arc::clone(&config.merge_executor);
        tokio::task::spawn_blocking(move || executor.merge_commit_status(pr, &repo))
            .await
            .map_err(|error| format!("branch sync merge state lookup join: {error}"))?
    };
    match status {
        MergeCommitStatus::Merged { .. } => {
            complete_verified_merge(config, worktrees, sync, pr).await
        }
        MergeCommitStatus::Open => {
            let merge_sha = required(sync.merge_sha.as_deref(), "merge_sha")?.to_string();
            let base = sync.target_branch.clone();
            let repo = config.repo_dir.clone();
            let executor = Arc::clone(&config.merge_executor);
            let context = merge::MergeContext {
                reviewer_name: "daemon branch sync".to_string(),
                review_task_id: sync.id,
                expected_base_branch: base,
                expected_head_sha: merge_sha,
            };
            let result = tokio::task::spawn_blocking(move || {
                executor.merge_without_approval(pr, &repo, &context)
            })
            .await
            .map_err(|error| format!("branch sync merge execution join: {error}"))?;
            if !result.success {
                return Err(format!(
                    "branch sync merge PR #{pr} failed: {}",
                    result.message
                ));
            }
            complete_verified_merge(config, worktrees, sync, pr).await
        }
        MergeCommitStatus::Closed => Err(format!("branch sync PR #{pr} is closed without merge")),
        MergeCommitStatus::Unknown => Err(format!(
            "branch sync PR #{pr} merge state could not be determined during merging reconciliation"
        )),
    }
}

async fn complete_verified_merge(
    config: &ServeConfig,
    worktrees: &WorktreeManager,
    sync: &BranchSync,
    pr: i64,
) -> std::result::Result<(), String> {
    worktrees
        .fetch_branch_sync_target(&config.repo_dir, &sync.target_branch)
        .await?;
    let merge_commit_sha = {
        let repo = config.repo_dir.clone();
        let executor = Arc::clone(&config.merge_executor);
        tokio::task::spawn_blocking(move || executor.merge_commit_sha(pr, &repo))
            .await
            .map_err(|error| format!("branch sync merge commit lookup join: {error}"))?
    }
    .filter(|sha| !sha.is_empty())
    .ok_or_else(|| format!("branch sync PR #{pr} is merged but has no merge_commit_sha"))?;
    let source_sha = required(sync.source_sha.as_deref(), "source_sha")?;
    let target_sha = required(sync.target_sha.as_deref(), "target_sha")?;
    worktrees
        .verify_branch_sync_merge_ancestry(
            &config.repo_dir,
            &merge_commit_sha,
            source_sha,
            target_sha,
        )
        .await?;
    let db_path = config.db_path.clone();
    let id = sync.id;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&db_path)?;
        let _ = branch_sync::complete_merge(
            &mut conn,
            id,
            &merge_commit_sha,
            quorum_core::clock::now(),
        )?;
        Ok(())
    })
    .await
    .map_err(|error| format!("branch sync completion settlement join: {error}"))?
    .map_err(|error| error.to_string())
}

fn required_pr(sync: &BranchSync) -> std::result::Result<i64, String> {
    sync.pr
        .filter(|pr| *pr > 0)
        .ok_or_else(|| "missing pr".to_string())
}

fn validate_published_target(
    target: &PrTarget,
    merge_sha: &str,
    target_branch: &str,
) -> std::result::Result<(), String> {
    if target.state.as_deref() != Some(PR_OPEN)
        || target.head_sha != merge_sha
        || target.base_ref.as_deref() != Some(target_branch)
    {
        return Err(format!(
            "branch sync PR #{} is stale: state={:?}, head={}, base={:?}; expected open head={} base={}",
            target.pr, target.state, target.head_sha, target.base_ref, merge_sha, target_branch
        ));
    }
    Ok(())
}

async fn create_pr(
    config: &ServeConfig,
    sync: &BranchSync,
    branch: &str,
) -> std::result::Result<i64, String> {
    let mut args = vec![
        "pr".to_string(),
        "create".to_string(),
        "--base".to_string(),
        sync.target_branch.clone(),
        "--head".to_string(),
        branch.to_string(),
        "--title".to_string(),
        format!(
            "Branch sync: {} → {} (#{})",
            sync.source_branch, sync.target_branch, sync.id
        ),
        "--body".to_string(),
        format!("Daemon-owned branch synchronization #{}.", sync.id),
    ];
    if !config.repo.is_empty() {
        args.push("--repo".to_string());
        args.push(config.repo.clone());
    }
    let output = run_branch_sync_gh(config, &args, "gh pr create").await?;
    if !output.status.success() {
        return Err(format!(
            "gh pr create failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    parse_created_pr_number(&output.stdout)
        .ok_or_else(|| "gh pr create succeeded but did not return a pull-request URL".to_string())
}

async fn resolve_pr_target(config: &ServeConfig, pr: i64) -> std::result::Result<PrTarget, String> {
    let program = config
        .pr_target_program
        .as_deref()
        .unwrap_or(Path::new("gh"));
    resolve_pr_target_with_program(
        pr,
        &config.repo_dir,
        (!config.repo.is_empty()).then_some(config.repo.as_str()),
        super::PUBLICATION_GH_TIMEOUT,
        program,
    )
    .await
}

async fn run_branch_sync_gh(
    config: &ServeConfig,
    args: &[String],
    label: &str,
) -> std::result::Result<std::process::Output, String> {
    let program = config
        .pr_target_program
        .as_deref()
        .unwrap_or(Path::new("gh"));
    let mut command = tokio::process::Command::new(program);
    command.args(args).current_dir(&config.repo_dir);
    run_publication_gh_command(command, super::PUBLICATION_GH_TIMEOUT, label).await
}

async fn fail(config: &ServeConfig, sync: &BranchSync, error: &str) -> Result<()> {
    let db_path = config.db_path.clone();
    let id = sync.id;
    let phase = sync.phase.clone();
    let error = error.to_string();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&db_path)?;
        let _ = branch_sync::fail(&mut conn, id, &phase, &error, quorum_core::clock::now())?;
        Ok(())
    })
    .await
    .map_err(|join| QuorumError::Io(format!("branch sync failure settlement join: {join}")))??;
    Ok(())
}

fn required<'a>(value: Option<&'a str>, field: &str) -> std::result::Result<&'a str, String> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("branch sync durable row is missing {field}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::{Arc, Mutex};

    fn target(state: &str, head: &str, base: &str) -> PrTarget {
        PrTarget {
            pr: 77,
            head_ref: "sync/main-into-develop-1".into(),
            head_sha: head.into(),
            is_fork: false,
            base_ref: Some(base.into()),
            state: Some(state.into()),
        }
    }

    #[test]
    fn published_reconciliation_rejects_stale_pr_state_head_and_base() {
        for stale in [
            target("CLOSED", "merge", "develop"),
            target("OPEN", "other", "develop"),
            target("OPEN", "merge", "main"),
        ] {
            assert!(validate_published_target(&stale, "merge", "develop").is_err());
        }
        assert!(
            validate_published_target(&target("OPEN", "merge", "develop"), "merge", "develop")
                .is_ok()
        );
    }

    #[test]
    fn sync_branch_is_bounded_and_cannot_match_task_branch_grammar() {
        let row = BranchSync {
            id: 42,
            source_branch: "main".into(),
            target_branch: "develop".into(),
            source_sha: None,
            target_sha: None,
            sync_branch: None,
            merge_sha: None,
            pr: None,
            phase: "requested".into(),
            ci_attempts: 0,
            ci_next_attempt_at: None,
            ci_wait_inflight: false,
            task_id: None,
            active: true,
            requested_by: "owner".into(),
            last_error: None,
            created_at: 0,
            updated_at: 0,
        };
        assert_eq!(sync_branch(&row), "sync/42");
        assert!(!sync_branch(&row).starts_with("daemon/"));

        let long_row = BranchSync {
            source_branch: "a".repeat(quorum_core::tasks::MAX_TARGET_BRANCH_BYTES),
            target_branch: "b".repeat(quorum_core::tasks::MAX_TARGET_BRANCH_BYTES),
            ..row
        };
        quorum_core::tasks::validate_target_branch(&long_row.source_branch).unwrap();
        quorum_core::tasks::validate_target_branch(&long_row.target_branch).unwrap();
        let branch = sync_branch(&long_row);
        assert_eq!(branch, "sync/42");
        assert!(branch.len() <= quorum_core::tasks::MAX_TARGET_BRANCH_BYTES);
        let check = Command::new("git")
            .args(["check-ref-format", "--branch", &branch])
            .output()
            .unwrap();
        assert!(check.status.success());
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed in {}", dir.display());
    }

    fn sync_test_config(
        db_path: PathBuf,
        repo_dir: PathBuf,
        worktree_base: PathBuf,
        gh: PathBuf,
    ) -> ServeConfig {
        let profile = crate::serve_config::ModelProfile {
            runner: "codex".into(),
            model: "test".into(),
            effort: "medium".into(),
        };
        let pool = std::collections::BTreeMap::from([("test".to_string(), 100)]);
        ServeConfig {
            db_path,
            cap: 1,
            model_profiles: std::collections::BTreeMap::from([("test".to_string(), profile)]),
            routing: crate::serve_config::RoutingPolicy {
                classifier: pool.clone(),
                planner: pool.clone(),
                arbiter: pool.clone(),
                collector: pool.clone(),
                worker: (1..=5)
                    .map(|level| (level.to_string(), pool.clone()))
                    .collect(),
                reviewer: (1..=5)
                    .map(|level| (level.to_string(), pool.clone()))
                    .collect(),
            },
            repo_dir,
            worktree_base,
            names_file: None,
            agent_bin: None,
            merge_executor: Arc::new(super::super::merge::CommandMergeExecutor {
                command: "true".into(),
                checks_cmd: None,
                mergeability_cmd: None,
            }),
            bare_agent: true,
            limits: super::super::CostLimits::default(),
            log_dir: None,
            self_update_drain: false,
            drain_timeout_secs: 1,
            self_repo: None,
            sha_poll_interval_secs: 60,
            merge_checks_timeout_secs: 1,
            merge_checks_poll_secs: 1,
            repo: "owner/repo".into(),
            base_branch: "main".into(),
            self_update_branch: "main".into(),
            exit_when_gone: None,
            required_jobs: Vec::new(),
            master_ci_gate: false,
            master_ci_timeout_secs: 1,
            allowed_tools: None,
            doctor_enabled: false,
            resource_monitor: crate::resource_health::ResourceMonitorConfig::default(),
            r2_enabled: false,
            r2_target_per_stratum: 0,
            r2_steady_state_p: 0.0,
            max_rework: quorum_core::lifecycle::REWORK_CAP,
            codex_sandbox: "danger-full-access".into(),
            grok: Default::default(),
            pr_target_program: Some(gh),
        }
    }

    struct SyncExecutor {
        checks: merge::ChecksOutcome,
        required_jobs: merge::RequiredJobsOutcome,
        merge_status: MergeCommitStatus,
        merge_commit_sha: Option<String>,
        merge_success: bool,
        merge_calls: std::sync::atomic::AtomicUsize,
        merge_heads: Mutex<Vec<String>>,
        wait_calls: std::sync::atomic::AtomicUsize,
        wait_delay: Option<Duration>,
    }

    impl SyncExecutor {
        fn new(
            checks: merge::ChecksOutcome,
            required_jobs: merge::RequiredJobsOutcome,
            merge_status: MergeCommitStatus,
            merge_commit_sha: Option<String>,
            merge_success: bool,
        ) -> Self {
            Self {
                checks,
                required_jobs,
                merge_status,
                merge_commit_sha,
                merge_success,
                merge_calls: std::sync::atomic::AtomicUsize::new(0),
                merge_heads: Mutex::new(Vec::new()),
                wait_calls: std::sync::atomic::AtomicUsize::new(0),
                wait_delay: None,
            }
        }

        fn with_wait_delay(mut self, delay: Duration) -> Self {
            self.wait_delay = Some(delay);
            self
        }
    }

    impl merge::MergeExecutor for SyncExecutor {
        fn merge(
            &self,
            _pr: i64,
            _repo_dir: &Path,
            context: &merge::MergeContext,
        ) -> merge::MergeResult {
            self.merge_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.merge_heads
                .lock()
                .unwrap()
                .push(context.expected_head_sha.clone());
            merge::MergeResult {
                success: self.merge_success,
                message: "fake merge".to_string(),
                failure_kind: (!self.merge_success)
                    .then_some(merge::MergeFailureKind::PolicyBlocked),
            }
        }

        fn merge_commit_sha(&self, _pr: i64, _repo_dir: &Path) -> Option<String> {
            self.merge_commit_sha.clone()
        }

        fn merge_commit_status(&self, _pr: i64, _repo_dir: &Path) -> MergeCommitStatus {
            self.merge_status.clone()
        }

        fn wait_for_branch_sync_checks(
            &self,
            _pr: i64,
            _repo_dir: &Path,
            _timeout_secs: u64,
            _poll_interval_secs: u64,
        ) -> merge::ChecksOutcome {
            self.wait_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(delay) = self.wait_delay {
                std::thread::sleep(delay);
            }
            self.checks.clone()
        }

        fn check_required_jobs(
            &self,
            _pr: i64,
            _repo_dir: &Path,
            _required_jobs: &[String],
        ) -> merge::RequiredJobsOutcome {
            self.required_jobs.clone()
        }
    }

    fn sync_at_phase(
        db_path: &Path,
        source_sha: &str,
        target_sha: &str,
        phase: &str,
    ) -> BranchSync {
        sync_at_phase_for_pair(db_path, "develop", "main", source_sha, target_sha, phase)
    }

    fn sync_at_phase_for_pair(
        db_path: &Path,
        source_branch: &str,
        target_branch: &str,
        source_sha: &str,
        target_sha: &str,
        phase: &str,
    ) -> BranchSync {
        let mut conn = quorum_core::db::open(db_path).unwrap();
        let row = match branch_sync::request(&mut conn, source_branch, target_branch, "owner", 1)
            .unwrap()
        {
            branch_sync::RequestOutcome::Requested(row) => row,
            branch_sync::RequestOutcome::AlreadyActive(_) => unreachable!(),
        };
        let sync_branch = format!("sync/{}", row.id);
        branch_sync::pin(&mut conn, row.id, source_sha, target_sha, &sync_branch, 2)
            .unwrap()
            .unwrap();
        branch_sync::prepared(&mut conn, row.id, &sync_branch, &"c".repeat(40), 3)
            .unwrap()
            .unwrap();
        branch_sync::published(&mut conn, row.id, 42, 4)
            .unwrap()
            .unwrap();
        if phase == "checks" || phase == "merging" {
            branch_sync::begin_checks(&mut conn, row.id, 5)
                .unwrap()
                .unwrap();
        }
        if phase == "merging" {
            branch_sync::begin_merge_attempt(&mut conn, row.id, 6)
                .unwrap()
                .unwrap();
        }
        branch_sync::get(&conn, row.id).unwrap().unwrap()
    }

    async fn reconcile_until_phase(
        config: &ServeConfig,
        worktrees: &WorktreeManager,
        checks: &mut BranchSyncChecks,
        db_path: &Path,
        id: i64,
        phase: &str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            reconcile_one(config, worktrees, checks).await.unwrap();
            let conn = quorum_core::db::open(db_path).unwrap();
            if branch_sync::get(&conn, id).unwrap().unwrap().phase == phase {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("branch sync #{id} did not reach {phase}");
    }

    fn config_with_executor(
        root: &Path,
        db_path: PathBuf,
        executor: Arc<dyn merge::MergeExecutor>,
        required_jobs: Vec<String>,
    ) -> ServeConfig {
        let mut config = sync_test_config(
            db_path,
            root.join("repo"),
            root.join("worktrees"),
            root.join("fake-gh"),
        );
        config.merge_executor = executor;
        config.required_jobs = required_jobs;
        config
    }

    #[tokio::test]
    async fn checks_fail_closed_when_required_jobs_are_empty() {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("quorum.db");
        let row = sync_at_phase(&db_path, &"a".repeat(40), &"b".repeat(40), "checks");
        let executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::Ready,
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Open,
            None,
            true,
        ));
        let config = config_with_executor(root.path(), db_path.clone(), executor, Vec::new());

        let mut checks = BranchSyncChecks::default();
        reconcile_one(&config, &WorktreeManager::new(), &mut checks)
            .await
            .unwrap();

        let conn = quorum_core::db::open(&db_path).unwrap();
        let failed = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(failed.phase, "failed");
        assert!(!failed.active);
        assert_eq!(failed.last_error.as_deref(), Some(REQUIRED_JOBS_ERROR));
        let error_rows: i64 = conn
            .query_row(
                "SELECT count(*) FROM errors WHERE source='branch_sync'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(error_rows, 1);
        let event: String = conn
            .query_row(
                "SELECT kind FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", row.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event, "branch_sync_failed");
    }

    #[tokio::test]
    async fn failed_checks_stay_active_for_later_judgment() {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("quorum.db");
        let row = sync_at_phase(&db_path, &"a".repeat(40), &"b".repeat(40), "checks");
        let executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::Failed {
                failing_checks: vec!["ci".to_string()],
            },
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Open,
            None,
            true,
        ));
        let config = config_with_executor(
            root.path(),
            db_path.clone(),
            executor,
            vec!["ci".to_string()],
        );

        let manager = WorktreeManager::new();
        let mut checks = BranchSyncChecks::default();
        reconcile_until_phase(
            &config,
            &manager,
            &mut checks,
            &db_path,
            row.id,
            "ci_failed",
        )
        .await;

        let conn = quorum_core::db::open(&db_path).unwrap();
        let failed = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(failed.phase, "ci_failed");
        assert!(failed.active);
        assert!(failed.last_error.as_deref().unwrap().contains("ci"));
        let event: String = conn
            .query_row(
                "SELECT kind FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", row.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event, "branch_sync_ci_failed");
    }

    #[tokio::test]
    async fn green_checks_cross_durable_merging_admission_before_remote_call() {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("quorum.db");
        let row = sync_at_phase(&db_path, &"a".repeat(40), &"b".repeat(40), "checks");
        let executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::Ready,
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Open,
            None,
            true,
        ));
        let config = config_with_executor(
            root.path(),
            db_path.clone(),
            executor.clone(),
            vec!["ci".to_string()],
        );

        let manager = WorktreeManager::new();
        let mut checks = BranchSyncChecks::default();
        reconcile_until_phase(&config, &manager, &mut checks, &db_path, row.id, "merging").await;

        let conn = quorum_core::db::open(&db_path).unwrap();
        let merging = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(merging.phase, "merging");
        assert!(merging.active);
        assert_eq!(
            executor
                .merge_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn pending_checks_waits_are_nonblocking_and_globally_capped() {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("quorum.db");
        let first = sync_at_phase_for_pair(
            &db_path,
            "develop",
            "main",
            &"a".repeat(40),
            &"b".repeat(40),
            "checks",
        );
        let second = sync_at_phase_for_pair(
            &db_path,
            "release",
            "main",
            &"c".repeat(40),
            &"d".repeat(40),
            "checks",
        );
        let third = sync_at_phase_for_pair(
            &db_path,
            "stable",
            "main",
            &"e".repeat(40),
            &"f".repeat(40),
            "checks",
        );
        let executor = Arc::new(
            SyncExecutor::new(
                merge::ChecksOutcome::TimedOut,
                merge::RequiredJobsOutcome::AllSucceeded,
                MergeCommitStatus::Open,
                None,
                true,
            )
            .with_wait_delay(Duration::from_millis(500)),
        );
        let mut config = config_with_executor(
            root.path(),
            db_path,
            executor.clone(),
            vec!["ci".to_string()],
        );
        config.merge_checks_poll_secs = 30;
        let manager = WorktreeManager::new();
        let mut checks = BranchSyncChecks::default();

        let first_tick = Instant::now();
        reconcile_one(&config, &manager, &mut checks).await.unwrap();
        assert!(
            first_tick.elapsed() < Duration::from_millis(200),
            "the synchronous CI waiter must not hold the daemon tick"
        );

        let second_tick = Instant::now();
        reconcile_one(&config, &manager, &mut checks).await.unwrap();
        assert!(
            second_tick.elapsed() < Duration::from_millis(200),
            "an in-flight checks row must not delay another row"
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        while executor
            .wait_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            < 2
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            executor
                .wait_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "each checks row owns one retained wait instead of reselecting the oldest row"
        );
        assert!(checks.waits.contains_key(&first.id));
        assert!(checks.waits.contains_key(&second.id));

        let capped_tick = Instant::now();
        reconcile_one(&config, &manager, &mut checks).await.unwrap();
        assert!(
            capped_tick.elapsed() < Duration::from_millis(200),
            "a saturated branch-sync CI cap must leave the daemon tick free"
        );
        assert_eq!(checks.waits.len(), MAX_BRANCH_SYNC_CHECK_WAITS);
        assert_eq!(
            executor
                .wait_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the third durable checks row must wait for a global admission slot"
        );

        tokio::time::sleep(Duration::from_millis(600)).await;
        let deadline = Instant::now() + Duration::from_secs(1);
        while checks.waits.len() == MAX_BRANCH_SYNC_CHECK_WAITS && Instant::now() < deadline {
            reconcile_one(&config, &manager, &mut checks).await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            checks.waits.len() < MAX_BRANCH_SYNC_CHECK_WAITS,
            "a completed wait must release capacity for the next durable row"
        );

        while executor
            .wait_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            < 3
            && Instant::now() < deadline
        {
            reconcile_one(&config, &manager, &mut checks).await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            executor
                .wait_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            3,
            "the oldest unadmitted checks row must receive the released slot"
        );
        assert!(checks.waits.contains_key(&third.id));
        assert!(checks.waits.len() <= MAX_BRANCH_SYNC_CHECK_WAITS);
    }

    #[tokio::test]
    async fn timed_out_checks_wait_is_cadenced_outside_the_tick_loop() {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("quorum.db");
        let row = sync_at_phase(&db_path, &"a".repeat(40), &"b".repeat(40), "checks");
        let executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::TimedOut,
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Open,
            None,
            true,
        ));
        let mut config = config_with_executor(
            root.path(),
            db_path.clone(),
            executor.clone(),
            vec!["ci".to_string()],
        );
        config.merge_checks_poll_secs = 30;
        let manager = WorktreeManager::new();
        let mut checks = BranchSyncChecks::default();

        reconcile_one(&config, &manager, &mut checks).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            reconcile_one(&config, &manager, &mut checks).await.unwrap();
            let conn = quorum_core::db::open(&db_path).unwrap();
            let scheduled = branch_sync::get(&conn, row.id).unwrap().unwrap();
            if scheduled.ci_attempts == 1 && scheduled.ci_next_attempt_at.is_some() {
                assert!(!scheduled.ci_wait_inflight);
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let conn = quorum_core::db::open(&db_path).unwrap();
        let scheduled = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(scheduled.ci_attempts, 1);
        assert!(scheduled.ci_next_attempt_at.is_some());
        assert!(!scheduled.ci_wait_inflight);
        assert!(checks.waits.is_empty());

        for _ in 0..20 {
            reconcile_one(&config, &manager, &mut checks).await.unwrap();
        }
        assert_eq!(
            executor
                .wait_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a completed timeout must wait for its scheduled retry instead of polling every tick"
        );
    }

    #[tokio::test]
    async fn timed_out_checks_have_a_finite_durable_retry_budget_across_restarts() {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("quorum.db");
        let row = sync_at_phase(&db_path, &"a".repeat(40), &"b".repeat(40), "checks");
        let executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::TimedOut,
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Open,
            None,
            true,
        ));
        let mut config = config_with_executor(
            root.path(),
            db_path.clone(),
            executor.clone(),
            vec!["ci".to_string()],
        );
        config.merge_checks_poll_secs = 1;
        config.merge_checks_timeout_secs = 8;
        let manager = WorktreeManager::new();

        for attempt in 1..=MAX_BRANCH_SYNC_CHECK_ATTEMPTS {
            // A fresh coordinator models a daemon restart. The row's attempt
            // count and scheduled time, rather than process memory, govern
            // the next admission.
            let mut checks = BranchSyncChecks::default();
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                reconcile_one(&config, &manager, &mut checks).await.unwrap();
                let conn = quorum_core::db::open(&db_path).unwrap();
                let current = branch_sync::get(&conn, row.id).unwrap().unwrap();
                if attempt < MAX_BRANCH_SYNC_CHECK_ATTEMPTS
                    && current.phase == "checks"
                    && current.ci_attempts == attempt
                    && current.ci_next_attempt_at.is_some()
                {
                    let expected_delay = check_retry_delay_secs(&config, attempt) as i64;
                    assert!(
                        current.ci_next_attempt_at.unwrap() >= current.updated_at + expected_delay,
                        "retry {attempt} must persist exponential backoff"
                    );
                    drop(conn);
                    let conn = quorum_core::db::open(&db_path).unwrap();
                    conn.execute(
                        "UPDATE branch_syncs SET ci_next_attempt_at=0 WHERE id=?1",
                        [row.id],
                    )
                    .unwrap();
                    break;
                }
                if attempt == MAX_BRANCH_SYNC_CHECK_ATTEMPTS && current.phase == "failed" {
                    assert_eq!(current.ci_attempts, attempt);
                    assert!(!current.active);
                    assert!(current
                        .last_error
                        .as_deref()
                        .unwrap()
                        .contains(CHECK_RETRY_LIMIT_ERROR));
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "attempt {attempt} did not settle"
                );
                drop(conn);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        assert_eq!(
            executor
                .wait_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            MAX_BRANCH_SYNC_CHECK_ATTEMPTS as usize
        );
        let conn = quorum_core::db::open(&db_path).unwrap();
        let event: String = conn
            .query_row(
                "SELECT kind FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", row.id)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event, "branch_sync_failed");
    }

    #[cfg(unix)]
    fn git_sha(dir: &Path) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    #[cfg(unix)]
    fn init_merge_reconciliation_repo(root: &Path, divergent: bool) -> (PathBuf, String, String) {
        let bare = root.join("origin.git");
        let repo = root.join("repo");
        std::fs::create_dir(&repo).unwrap();
        assert!(Command::new("git")
            .args(["init", "--bare", "-b", "main", &bare.to_string_lossy()])
            .status()
            .unwrap()
            .success());
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "test@example.com"]);
        git(&repo, &["config", "user.name", "Test"]);
        git(&repo, &["commit", "--allow-empty", "-m", "initial"]);
        let source_sha = git_sha(&repo);
        git(&repo, &["remote", "add", "origin", &bare.to_string_lossy()]);
        git(&repo, &["push", "-u", "origin", "main"]);
        if divergent {
            git(&repo, &["checkout", "-b", "develop"]);
            git(&repo, &["commit", "--allow-empty", "-m", "source"]);
            let source_sha = git_sha(&repo);
            git(&repo, &["push", "origin", "develop"]);
            git(&repo, &["checkout", "main"]);
            git(&repo, &["commit", "--allow-empty", "-m", "target"]);
            let target_sha = git_sha(&repo);
            git(&repo, &["push", "origin", "main"]);
            return (repo, source_sha, target_sha);
        }
        git(&repo, &["commit", "--allow-empty", "-m", "target"]);
        let target_sha = git_sha(&repo);
        git(&repo, &["push", "origin", "main"]);
        (repo, source_sha, target_sha)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ancestry_failure_after_merge_fails_loudly() {
        let root = tempfile::tempdir().unwrap();
        let (_repo, source_sha, target_sha) = init_merge_reconciliation_repo(root.path(), true);
        let db_path = root.path().join("quorum.db");
        let row = sync_at_phase(&db_path, &source_sha, &target_sha, "merging");
        let executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::Ready,
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Merged {
                merge_commit_sha: Some(target_sha.clone()),
            },
            Some(target_sha),
            true,
        ));
        let config = config_with_executor(
            root.path(),
            db_path.clone(),
            executor,
            vec!["ci".to_string()],
        );

        let mut checks = BranchSyncChecks::default();
        reconcile_one(&config, &WorktreeManager::new(), &mut checks)
            .await
            .unwrap();

        let conn = quorum_core::db::open(&db_path).unwrap();
        let failed = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(failed.phase, "failed");
        assert!(failed
            .last_error
            .as_deref()
            .unwrap()
            .contains("does not contain pinned source tip"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn merging_restart_reconciles_merged_open_and_closed_pr_states() {
        let root = tempfile::tempdir().unwrap();
        let (_repo, source_sha, target_sha) = init_merge_reconciliation_repo(root.path(), false);

        let merged_db = root.path().join("merged.db");
        let merged_row = sync_at_phase(&merged_db, &source_sha, &target_sha, "merging");
        let merged_executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::Ready,
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Merged {
                merge_commit_sha: Some(target_sha.clone()),
            },
            Some(target_sha.clone()),
            true,
        ));
        let merged_config = config_with_executor(
            root.path(),
            merged_db.clone(),
            merged_executor.clone(),
            vec!["ci".to_string()],
        );
        let mut merged_checks = BranchSyncChecks::default();
        reconcile_one(&merged_config, &WorktreeManager::new(), &mut merged_checks)
            .await
            .unwrap();
        let merged_conn = quorum_core::db::open(&merged_db).unwrap();
        assert_eq!(
            branch_sync::get(&merged_conn, merged_row.id)
                .unwrap()
                .unwrap()
                .phase,
            "done"
        );
        let merged_event: (String, String) = merged_conn
            .query_row(
                "SELECT kind, body FROM events WHERE subject=?1 ORDER BY seq DESC LIMIT 1",
                [format!("branch_sync#{}", merged_row.id)],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(merged_event.0, "branch_sync_merged");
        assert!(merged_event.1.contains(&target_sha));
        assert_eq!(
            merged_executor
                .merge_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );

        let open_db = root.path().join("open.db");
        let open_row = sync_at_phase(&open_db, &source_sha, &target_sha, "merging");
        let open_executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::Ready,
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Open,
            None,
            false,
        ));
        let open_config = config_with_executor(
            root.path(),
            open_db.clone(),
            open_executor.clone(),
            vec!["ci".to_string()],
        );
        let mut open_checks = BranchSyncChecks::default();
        reconcile_one(&open_config, &WorktreeManager::new(), &mut open_checks)
            .await
            .unwrap();
        let open_conn = quorum_core::db::open(&open_db).unwrap();
        assert_eq!(
            branch_sync::get(&open_conn, open_row.id)
                .unwrap()
                .unwrap()
                .phase,
            "failed"
        );
        assert_eq!(
            open_executor
                .merge_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        {
            let open_heads = open_executor.merge_heads.lock().unwrap();
            assert_eq!(open_heads.as_slice(), &["c".repeat(40)]);
        }

        let closed_db = root.path().join("closed.db");
        let closed_row = sync_at_phase(&closed_db, &source_sha, &target_sha, "merging");
        let closed_executor = Arc::new(SyncExecutor::new(
            merge::ChecksOutcome::Ready,
            merge::RequiredJobsOutcome::AllSucceeded,
            MergeCommitStatus::Closed,
            None,
            true,
        ));
        let closed_config = config_with_executor(
            root.path(),
            closed_db.clone(),
            closed_executor.clone(),
            vec!["ci".to_string()],
        );
        let mut closed_checks = BranchSyncChecks::default();
        reconcile_one(&closed_config, &WorktreeManager::new(), &mut closed_checks)
            .await
            .unwrap();
        let closed_conn = quorum_core::db::open(&closed_db).unwrap();
        assert_eq!(
            branch_sync::get(&closed_conn, closed_row.id)
                .unwrap()
                .unwrap()
                .phase,
            "failed"
        );
        assert_eq!(
            closed_executor
                .merge_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[cfg(unix)]
    fn write_gh(program: &Path, head: &str, state: &str) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(
            program,
            format!(
                "#!/bin/sh\ncase \"$1 $2\" in\n  'pr list') echo '[]' ;;\n  'pr create') echo 'https://github.test/owner/repo/pull/42' ;;\n  'pr view') echo '{{\"headRefName\":\"sync/1\",\"headRefOid\":\"{head}\",\"isCrossRepository\":false,\"baseRefName\":\"main\",\"state\":\"{state}\"}}' ;;\n  *) exit 2 ;;\nesac\n"
            ),
        )
        .unwrap();
        std::fs::set_permissions(program, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconciliation_advances_each_clean_phase_and_fails_a_stale_published_pr() {
        let root = tempfile::tempdir().unwrap();
        let bare = root.path().join("origin.git");
        let repo = root.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        assert!(Command::new("git")
            .args(["init", "--bare", "-b", "main", &bare.to_string_lossy()])
            .status()
            .unwrap()
            .success());
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "test@example.com"]);
        git(&repo, &["config", "user.name", "Test"]);
        git(&repo, &["commit", "--allow-empty", "-m", "initial"]);
        git(&repo, &["remote", "add", "origin", &bare.to_string_lossy()]);
        git(&repo, &["push", "-u", "origin", "main"]);
        git(&repo, &["checkout", "-b", "develop"]);
        git(&repo, &["commit", "--allow-empty", "-m", "source"]);
        git(&repo, &["push", "origin", "develop"]);
        git(&repo, &["checkout", "main"]);

        let db_path = root.path().join("quorum.db");
        let gh = root.path().join("fake-gh");
        let config = sync_test_config(
            db_path.clone(),
            repo.clone(),
            root.path().join("worktrees"),
            gh.clone(),
        );
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let row = match branch_sync::request(&mut conn, "develop", "main", "owner", 1).unwrap() {
            branch_sync::RequestOutcome::Requested(row) => row,
            branch_sync::RequestOutcome::AlreadyActive(_) => unreachable!(),
        };
        drop(conn);
        let manager = WorktreeManager::new();
        let mut checks = BranchSyncChecks::default();

        reconcile_one(&config, &manager, &mut checks).await.unwrap();
        let conn = quorum_core::db::open(&db_path).unwrap();
        let pinned = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(pinned.phase, "pinned");
        assert!(pinned.source_sha.is_some() && pinned.target_sha.is_some());
        drop(conn);

        reconcile_one(&config, &manager, &mut checks).await.unwrap();
        let conn = quorum_core::db::open(&db_path).unwrap();
        let prepared = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(prepared.phase, "prepared");
        let merge_sha = prepared.merge_sha.clone().unwrap();
        drop(conn);
        assert!(
            !sync_worktree(&config, &prepared).starts_with(&config.worktree_base),
            "task recovery GC must not see the preserved sync worktree"
        );

        write_gh(&gh, &merge_sha, "OPEN");
        reconcile_one(&config, &manager, &mut checks).await.unwrap();
        let conn = quorum_core::db::open(&db_path).unwrap();
        let published = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(published.phase, "published");
        assert_eq!(published.pr, Some(42));
        drop(conn);

        write_gh(&gh, "stale", "OPEN");
        reconcile_one(&config, &manager, &mut checks).await.unwrap();
        let conn = quorum_core::db::open(&db_path).unwrap();
        let failed = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(failed.phase, "failed");
        assert!(
            !failed.active
                && failed
                    .last_error
                    .as_deref()
                    .unwrap_or_default()
                    .contains("stale")
        );
        let errors: i64 = conn
            .query_row(
                "SELECT count(*) FROM errors WHERE source='branch_sync'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(errors, 1);
    }
}
