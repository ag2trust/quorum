//! Executes durable decomposition cleanup intents.
//!
//! Claims and settlement are short DB transactions. Identity validation and
//! destructive process/GitHub/git/filesystem work happen between them.

use super::{names::Pool, ServeConfig, SlotState};
use crate::serve::worktree::WorktreeManager;
use quorum_core::decomposition_cleanup::{self, CleanupWork};
use quorum_core::error::{QuorumError, Result};
use serde::Deserialize;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

pub const CLEANUP_DRAIN_LIMIT: usize = 8;
const PROCESS_REAP_TIMEOUT: Duration = Duration::from_secs(2);

fn configured_remote_url(repo: &str) -> String {
    format!("https://github.com/{repo}.git")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessRef {
    agent: String,
    session_id: String,
    pid: i32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProposedChangeRef {
    pr_number: i64,
    head_ref: String,
    head_sha: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorktreeRef {
    path: PathBuf,
    branch: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchRef {
    name: String,
    expected_sha: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchDiscoveryRef {
    allocation_id: i64,
    allocated_at: i64,
    allocated_by: String,
    name: String,
    path: String,
    provenance_sha: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BranchDeleteRef {
    allocation_id: i64,
    name: String,
    expected_sha: String,
    tombstone_ref: String,
}

pub async fn startup(config: &ServeConfig, wt: &WorktreeManager) -> Result<usize> {
    let path = config.db_path.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = quorum_core::db::open(&path)?;
        decomposition_cleanup::requeue_interrupted(&mut conn, quorum_core::clock::now())
    })
    .await
    .map_err(|e| QuorumError::Io(format!("cleanup startup join: {e}")))??;
    drain_startup(config, wt, None).await
}

pub struct LiveSlots<'a> {
    pub workers: &'a mut Vec<SlotState>,
    pub reviewers: &'a mut Vec<SlotState>,
    pub names: &'a mut Pool,
}

pub async fn drain_startup(
    config: &ServeConfig,
    wt: &WorktreeManager,
    mut live: Option<LiveSlots<'_>>,
) -> Result<usize> {
    let mut total = 0;
    loop {
        retire_settled_tombstones(config, wt, CLEANUP_DRAIN_LIMIT).await?;
        let count = drain_batch(config, wt, CLEANUP_DRAIN_LIMIT, false, live.as_mut()).await?;
        total += count;
        if count < CLEANUP_DRAIN_LIMIT {
            retire_settled_tombstones(config, wt, CLEANUP_DRAIN_LIMIT).await?;
            return Ok(total);
        }
        tokio::task::yield_now().await;
    }
}

pub async fn drain_tick(
    config: &ServeConfig,
    wt: &WorktreeManager,
    mut live: LiveSlots<'_>,
) -> Result<usize> {
    retire_settled_tombstones(config, wt, CLEANUP_DRAIN_LIMIT).await?;
    drain_batch(config, wt, CLEANUP_DRAIN_LIMIT, true, Some(&mut live)).await
}

async fn retire_settled_tombstones(
    config: &ServeConfig,
    wt: &WorktreeManager,
    limit: usize,
) -> Result<usize> {
    let path = config.db_path.clone();
    let rows = tokio::task::spawn_blocking(move || {
        let conn = quorum_core::db::open(&path)?;
        let mut stmt = conn.prepare("SELECT graph_id,task_id,artifact_ref FROM decomposition_cleanup WHERE artifact_kind='branch-delete' AND state='done' ORDER BY updated_at,graph_id,task_id LIMIT ?1")?;
        let refs = stmt.query_map([limit as i64], |row| Ok((row.get::<_, i64>(0)?,row.get::<_, i64>(1)?,row.get::<_, String>(2)?)))?.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok::<_, QuorumError>(refs)
    }).await.map_err(|e| QuorumError::Io(format!("settled tombstone scan join: {e}")))??;
    for (graph_id, task_id, artifact) in &rows {
        let intent: BranchDeleteRef = serde_json::from_str(artifact)
            .map_err(|e| QuorumError::BadInput(format!("invalid settled branch-delete: {e}")))?;
        wt.retire_cleanup_tombstone(
            &config.repo_dir,
            &configured_remote_url(&config.repo),
            &intent.tombstone_ref,
            &intent.expected_sha,
        )
        .await
        .map_err(QuorumError::Io)?;
        let path = config.db_path.clone();
        let artifact = artifact.clone();
        let graph_id = *graph_id;
        let task_id = *task_id;
        tokio::task::spawn_blocking(move || {
            let mut conn = quorum_core::db::open(&path)?;
            let tx = quorum_core::db::begin_immediate(&mut conn)?;
            tx.execute("DELETE FROM decomposition_cleanup WHERE graph_id=?1 AND task_id=?2 AND artifact_kind='branch-delete' AND artifact_ref=?3 AND state='done'", rusqlite::params![graph_id,task_id,artifact])?;
            tx.commit()?;
            Ok::<_, QuorumError>(())
        }).await.map_err(|e| QuorumError::Io(format!("tombstone retirement settlement join: {e}")))??;
    }
    Ok(rows.len())
}

async fn drain_batch(
    config: &ServeConfig,
    wt: &WorktreeManager,
    limit: usize,
    stop_after_failure: bool,
    mut live: Option<&mut LiveSlots<'_>>,
) -> Result<usize> {
    let mut completed = 0;
    for _ in 0..limit {
        let path = config.db_path.clone();
        let work = tokio::task::spawn_blocking(move || {
            let mut conn = quorum_core::db::open(&path)?;
            decomposition_cleanup::claim_next(&mut conn, quorum_core::clock::now())
        })
        .await
        .map_err(|e| QuorumError::Io(format!("cleanup claim join: {e}")))??;
        let Some(work) = work else {
            break;
        };
        let outcome = execute(config, wt, &work, live.as_deref_mut()).await;
        let failed = outcome.is_err();
        let path = config.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = quorum_core::db::open(&path)?;
            match outcome {
                Ok(()) => {
                    decomposition_cleanup::complete(&mut conn, &work, quorum_core::clock::now())?;
                }
                Err(error) => {
                    let bounded: String = error.chars().take(1024).collect();
                    decomposition_cleanup::fail(
                        &mut conn,
                        &work,
                        &bounded,
                        quorum_core::clock::now(),
                    )?;
                }
            }
            Ok::<_, QuorumError>(())
        })
        .await
        .map_err(|e| QuorumError::Io(format!("cleanup settlement join: {e}")))??;
        completed += 1;
        if failed && stop_after_failure {
            break;
        }
    }
    Ok(completed)
}

async fn execute(
    config: &ServeConfig,
    wt: &WorktreeManager,
    work: &CleanupWork,
    live: Option<&mut LiveSlots<'_>>,
) -> std::result::Result<(), String> {
    match work.key.artifact_kind.as_str() {
        "process" => cleanup_process(config, work, live).await,
        "proposed-change" => cleanup_pr(config, work).await,
        "worktree" => cleanup_worktree(config, wt, work).await,
        "branch" => cleanup_branch(config, wt, work).await,
        "branch-discovery" => cleanup_branch_discovery(config, wt, work).await,
        "branch-delete" => cleanup_branch_delete(config, wt, work).await,
        kind => Err(format!("unsupported cleanup artifact kind {kind}")),
    }
}

async fn cleanup_process(
    config: &ServeConfig,
    work: &CleanupWork,
    live: Option<&mut LiveSlots<'_>>,
) -> std::result::Result<(), String> {
    let identity: ProcessRef =
        serde_json::from_str(&work.key.artifact_ref).map_err(|e| e.to_string())?;
    if let Some(live) = live {
        if kill_matching_live_slot(live, work.key.task_id, &identity).await? {
            settle_process_identity(&config.db_path, work.key.task_id, &identity).await?;
            return Ok(());
        }
    }
    cleanup_process_at(&config.db_path, work).await
}

async fn kill_matching_live_slot(
    live: &mut LiveSlots<'_>,
    task_id: i64,
    identity: &ProcessRef,
) -> std::result::Result<bool, String> {
    for slots in [&mut *live.workers, &mut *live.reviewers] {
        if let Some(index) = slots
            .iter()
            .position(|slot| slot.pid() == Some(identity.pid))
        {
            let slot = &slots[index];
            if slot.task_id != task_id
                || slot.agent_name != identity.agent
                || slot.session_id != identity.session_id
            {
                return Err("live process slot identity mismatch".into());
            }
            let slot = slots.swap_remove(index);
            live.names.release(&slot.agent_name);
            slot.kill_and_reap().await;
            return Ok(true);
        }
    }
    Ok(false)
}

async fn settle_process_identity(
    db_path: &Path,
    task: i64,
    identity: &ProcessRef,
) -> std::result::Result<(), String> {
    let path = db_path.to_path_buf();
    let agent = identity.agent.clone();
    let session = identity.session_id.clone();
    let pid = identity.pid;
    tokio::task::spawn_blocking(move || {
        let mut conn = quorum_core::db::open(&path)?;
        let tx = quorum_core::db::begin_immediate(&mut conn)?;
        let role = tx
            .query_row(
                "SELECT role FROM journal
                 WHERE agent=?1 AND task_id=?2 AND session_id=?3 AND pid=?4",
                rusqlite::params![agent, task, session, pid],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(role) = role else {
            tx.commit()?;
            return Ok::<_, QuorumError>(());
        };
        tx.execute(
            "UPDATE agent_runs SET ended_at=?1,end_reason='cancelled'
             WHERE task_id=?2 AND agent_name=?3 AND role=?4 AND ended_at IS NULL",
            rusqlite::params![quorum_core::clock::now(), task, agent, role],
        )?;
        tx.execute(
            "DELETE FROM journal WHERE agent=?1 AND task_id=?2 AND session_id=?3 AND pid=?4",
            rusqlite::params![agent, task, session, pid],
        )?;
        tx.commit()?;
        Ok::<_, QuorumError>(())
    })
    .await
    .map_err(|e| format!("process journal cleanup join: {e}"))?
    .map_err(|e| e.to_string())
}

async fn cleanup_process_at(db_path: &Path, work: &CleanupWork) -> std::result::Result<(), String> {
    let identity: ProcessRef =
        serde_json::from_str(&work.key.artifact_ref).map_err(|e| e.to_string())?;
    let path = db_path.to_path_buf();
    let expected = identity.agent.clone();
    let session = identity.session_id.clone();
    let pid = identity.pid;
    let task = work.key.task_id;
    let live = tokio::task::spawn_blocking(move || {
        let conn = quorum_core::db::open(&path)?;
        let identity = conn
            .query_row(
                "SELECT task_id,session_id,pid FROM journal WHERE agent=?1",
                [expected],
                |r| {
                    Ok((
                        r.get::<_, Option<i64>>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<i32>>(2)?,
                    ))
                },
            )
            .optional()?;
        Ok::<_, QuorumError>(identity)
    })
    .await
    .map_err(|e| format!("process identity join: {e}"))?
    .map_err(|e| e.to_string())?;
    let Some(live) = live else {
        return Ok(());
    };
    if live != (Some(task), session, Some(pid)) {
        return Err("process journal identity mismatch".into());
    }
    let killed = unsafe { libc::killpg(pid, libc::SIGKILL) };
    if killed != 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
        return Err(format!(
            "killpg({pid}) failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    let deadline = tokio::time::Instant::now() + PROCESS_REAP_TIMEOUT;
    loop {
        let alive = unsafe { libc::kill(pid, 0) == 0 };
        if !alive {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("process {pid} remained alive after SIGKILL"));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    settle_process_identity(db_path, task, &identity).await
}

async fn cleanup_pr(config: &ServeConfig, work: &CleanupWork) -> std::result::Result<(), String> {
    let expected: ProposedChangeRef =
        serde_json::from_str(&work.key.artifact_ref).map_err(|e| e.to_string())?;
    let Some(live) = resolve_cleanup_pr(config, expected.pr_number).await? else {
        return Ok(());
    };
    if validate_cleanup_pr_target(&live, &expected, &config.repo, &config.base_branch)? {
        return Ok(());
    }
    if live.state != "open" {
        return Err(format!(
            "PR #{} has unknown state {:?}",
            expected.pr_number, live.state
        ));
    }
    close_cleanup_pr(config, expected.pr_number).await
}

/// Returns true when the PR is already terminal and therefore must be
/// preserved. Repository/base provenance remains auditable after closure, but
/// mutable terminal head identity is never a prerequisite for idempotence.
fn validate_cleanup_pr_target(
    live: &CleanupPrTarget,
    expected: &ProposedChangeRef,
    repo: &str,
    base: &str,
) -> std::result::Result<bool, String> {
    if live.head_repo != repo || live.base_repo != repo || live.base_ref != base {
        return Err(format!(
            "PR #{} repository/base identity mismatch",
            expected.pr_number
        ));
    }
    if live.merged || live.state == "closed" {
        return Ok(true);
    }
    if live.head_ref != expected.head_ref || live.head_sha != expected.head_sha {
        return Err(format!(
            "PR #{} open head identity mismatch",
            expected.pr_number
        ));
    }
    Ok(false)
}

async fn close_cleanup_pr(config: &ServeConfig, pr: i64) -> std::result::Result<(), String> {
    close_cleanup_pr_with_program(&config.repo_dir, &config.repo, pr, Path::new("gh")).await
}

async fn close_cleanup_pr_with_program(
    repo_dir: &Path,
    repo: &str,
    pr: i64,
    program: &Path,
) -> std::result::Result<(), String> {
    let args = vec![
        "pr".to_string(),
        "close".to_string(),
        pr.to_string(),
        "--repo".to_string(),
        repo.to_string(),
    ];
    let mut command = tokio::process::Command::new(program);
    command.args(&args).current_dir(repo_dir);
    let out = super::run_publication_gh_command(
        command,
        super::PUBLICATION_GH_TIMEOUT,
        "gh pr close cleanup",
    )
    .await?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "gh pr close failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

#[derive(Deserialize, Debug)]
struct ApiRepo {
    full_name: String,
}
#[derive(Deserialize, Debug)]
struct ApiRef {
    #[serde(rename = "ref")]
    name: String,
    sha: String,
    repo: ApiRepo,
}
#[derive(Deserialize, Debug)]
struct ApiBase {
    #[serde(rename = "ref")]
    name: String,
    repo: ApiRepo,
}
#[derive(Deserialize, Debug)]
struct ApiPull {
    state: String,
    merged: bool,
    head: ApiRef,
    base: ApiBase,
}

struct CleanupPrTarget {
    head_ref: String,
    head_sha: String,
    head_repo: String,
    base_ref: String,
    base_repo: String,
    state: String,
    merged: bool,
}

async fn resolve_cleanup_pr(
    config: &ServeConfig,
    pr: i64,
) -> std::result::Result<Option<CleanupPrTarget>, String> {
    let endpoint = format!("repos/{}/pulls/{pr}", config.repo);
    let args = vec!["api".to_string(), "--include".to_string(), endpoint];
    let out = super::run_publication_gh(&args, &config.repo_dir, "gh api cleanup PR").await?;
    parse_cleanup_pr_response(&out)
}

fn parse_cleanup_pr_response(
    output: &std::process::Output,
) -> std::result::Result<Option<CleanupPrTarget>, String> {
    let bytes = &output.stdout;
    let header_end = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .or_else(|| bytes.windows(2).position(|w| w == b"\n\n").map(|i| i + 2))
        .ok_or_else(|| "GitHub API response omitted HTTP headers".to_string())?;
    let header = std::str::from_utf8(&bytes[..header_end])
        .map_err(|_| "GitHub API returned non-UTF-8 headers")?;
    let status = header
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u16>().ok())
        .ok_or_else(|| "GitHub API response omitted numeric status".to_string())?;
    if status == 404 {
        return Ok(None);
    }
    if status != 200 || !output.status.success() {
        return Err(format!("GitHub API PR lookup returned HTTP {status}"));
    }
    let api: ApiPull = serde_json::from_slice(&bytes[header_end..])
        .map_err(|e| format!("GitHub API PR JSON: {e}"))?;
    Ok(Some(CleanupPrTarget {
        head_ref: api.head.name,
        head_sha: api.head.sha,
        head_repo: api.head.repo.full_name,
        base_ref: api.base.name,
        base_repo: api.base.repo.full_name,
        state: api.state,
        merged: api.merged,
    }))
}

fn validate_worktree_path(base: &Path, path: &Path) -> std::result::Result<(), String> {
    if path
        .components()
        .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
        || path.parent() != Some(base)
    {
        return Err("cleanup worktree is not an immediate child of configured base".into());
    }
    let canonical_base = std::fs::canonicalize(base)
        .map_err(|e| format!("canonicalize cleanup worktree base: {e}"))?;
    let canonical_parent = std::fs::canonicalize(path.parent().expect("parent checked above"))
        .map_err(|e| format!("canonicalize cleanup worktree parent: {e}"))?;
    if canonical_parent != canonical_base {
        return Err("cleanup worktree parent resolves outside configured base".into());
    }
    match std::fs::symlink_metadata(path) {
        Ok(_) => {
            let canonical_path = std::fs::canonicalize(path)
                .map_err(|e| format!("canonicalize cleanup worktree path: {e}"))?;
            let basename = path
                .file_name()
                .ok_or_else(|| "cleanup worktree path has no basename".to_string())?;
            if canonical_path != canonical_base.join(basename) {
                return Err("cleanup worktree resolves to a different canonical child".into());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("inspect cleanup worktree path: {error}")),
    }
    Ok(())
}

async fn cleanup_worktree(
    config: &ServeConfig,
    wt: &WorktreeManager,
    work: &CleanupWork,
) -> std::result::Result<(), String> {
    let expected: WorktreeRef =
        serde_json::from_str(&work.key.artifact_ref).map_err(|e| e.to_string())?;
    validate_worktree_path(&config.worktree_base, &expected.path)?;
    let owned = task_branch_binding(config, work.key.task_id).await?;
    validate_worktree_registration(owned.as_ref(), &expected)?;
    wt.remove_exact(&config.repo_dir, &expected.path, &expected.branch)
        .await
}

fn validate_worktree_registration(
    owned: Option<&(String, String)>,
    expected: &WorktreeRef,
) -> std::result::Result<(), String> {
    let exact = (
        expected.branch.clone(),
        expected.path.to_string_lossy().into_owned(),
    );
    if owned != Some(&exact) {
        return Err("worktree no longer matches daemon task allocation".into());
    }
    Ok(())
}

async fn cleanup_branch(
    config: &ServeConfig,
    _wt: &WorktreeManager,
    work: &CleanupWork,
) -> std::result::Result<(), String> {
    let expected: BranchRef =
        serde_json::from_str(&work.key.artifact_ref).map_err(|e| e.to_string())?;
    if expected.name == config.base_branch {
        return Err("refusing to delete configured base branch".into());
    }
    let owned = task_branch_authority(config, work.key.task_id).await?;
    if owned.as_ref().map(|v| v.1.as_str()) != Some(expected.name.as_str()) {
        return Err("branch no longer matches daemon task allocation".into());
    }
    let allocation_id = owned.expect("allocation checked above").0;
    let tombstone = format!(
        "refs/quorum/cleanup/{}/{}/{}",
        work.key.graph_id, work.key.task_id, allocation_id
    );
    let path = config.db_path.clone();
    let work = work.clone();
    let finalized = tokio::task::spawn_blocking(move || {
        let mut conn = quorum_core::db::open(&path)?;
        decomposition_cleanup::finalize_known_branch(
            &mut conn,
            &work,
            allocation_id,
            &expected.name,
            &expected.expected_sha,
            &tombstone,
            quorum_core::clock::now(),
        )
    })
    .await
    .map_err(|e| format!("known branch finalize join: {e}"))?
    .map_err(|e| e.to_string())?;
    finalized
        .then_some(())
        .ok_or_else(|| "known branch authority changed before finalization".into())
}

async fn cleanup_branch_discovery(
    config: &ServeConfig,
    wt: &WorktreeManager,
    work: &CleanupWork,
) -> std::result::Result<(), String> {
    let expected: BranchDiscoveryRef =
        serde_json::from_str(&work.key.artifact_ref).map_err(|e| e.to_string())?;
    if expected.name == config.base_branch {
        return Err("refusing to discover configured base branch".into());
    }
    let allocation = task_branch_authority(config, work.key.task_id).await?;
    if allocation
        != Some((
            expected.allocation_id,
            expected.name.clone(),
            expected.path.clone(),
            expected.allocated_by.clone(),
            expected.allocated_at,
            Some(expected.provenance_sha.clone()),
        ))
    {
        return Err("branch discovery allocation identity mismatch".into());
    }
    let Some(current) = wt
        .discover_branch_head(&config.repo_dir, &expected.name, &expected.provenance_sha)
        .await?
    else {
        return Ok(());
    };
    let tombstone = format!(
        "refs/quorum/cleanup/{}/{}/{}",
        work.key.graph_id, work.key.task_id, expected.allocation_id
    );
    let path = config.db_path.clone();
    let work = work.clone();
    let finalized = tokio::task::spawn_blocking(move || {
        let mut conn = quorum_core::db::open(&path)?;
        decomposition_cleanup::finalize_branch_discovery(
            &mut conn,
            &work,
            &current,
            &tombstone,
            quorum_core::clock::now(),
        )
    })
    .await
    .map_err(|e| format!("branch discovery finalize join: {e}"))?
    .map_err(|e| e.to_string())?;
    if finalized {
        Ok(())
    } else {
        Err("branch discovery authority changed before finalization".into())
    }
}

async fn cleanup_branch_delete(
    config: &ServeConfig,
    wt: &WorktreeManager,
    work: &CleanupWork,
) -> std::result::Result<(), String> {
    let expected: BranchDeleteRef =
        serde_json::from_str(&work.key.artifact_ref).map_err(|e| e.to_string())?;
    if expected.name == config.base_branch {
        return Err("refusing to delete configured base branch".into());
    }
    let allocation = task_branch_authority(config, work.key.task_id).await?;
    if allocation.as_ref().map(|v| (v.0, v.1.as_str()))
        != Some((expected.allocation_id, expected.name.as_str()))
    {
        return Err("branch delete allocation identity mismatch".into());
    }
    let tombstone = format!(
        "refs/quorum/cleanup/{}/{}/{}",
        work.key.graph_id, work.key.task_id, expected.allocation_id
    );
    if expected.tombstone_ref != tombstone {
        return Err("branch delete tombstone identity mismatch".into());
    }
    wt.delete_branch_with_tombstone(
        &config.repo_dir,
        &configured_remote_url(&config.repo),
        &expected.name,
        &expected.expected_sha,
        &expected.tombstone_ref,
    )
    .await
}

async fn task_branch_binding(
    config: &ServeConfig,
    task_id: i64,
) -> std::result::Result<Option<(String, String)>, String> {
    let path = config.db_path.clone();
    tokio::task::spawn_blocking(move || {
        let conn = quorum_core::db::open(&path)?;
        let value = conn
            .query_row(
                "SELECT branch,worktree FROM task_branches WHERE task_id=?1",
                [task_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        Ok::<_, QuorumError>(value)
    })
    .await
    .map_err(|e| format!("task branch identity join: {e}"))?
    .map_err(|e| e.to_string())
}

type TaskBranchAuthority = (i64, String, String, String, i64, Option<String>);
async fn task_branch_authority(
    config: &ServeConfig,
    task_id: i64,
) -> std::result::Result<Option<TaskBranchAuthority>, String> {
    let path = config.db_path.clone();
    tokio::task::spawn_blocking(move || {
        let conn = quorum_core::db::open(&path)?;
        let value = conn.query_row("SELECT id,branch,worktree,allocated_by,allocated_at,provenance_sha FROM task_branches WHERE task_id=?1", [task_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
        Ok::<_, QuorumError>(value)
    }).await.map_err(|e| format!("task branch authority join: {e}"))?.map_err(|e| e.to_string())
}

use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::decomposition_cleanup::CleanupKey;
    use std::os::unix::process::ExitStatusExt;

    fn restart_config(db_path: PathBuf, repo_dir: PathBuf, worktree_base: PathBuf) -> ServeConfig {
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
            merge_executor: std::sync::Arc::new(super::super::merge::CommandMergeExecutor {
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
            exit_when_gone: None,
            required_jobs: Vec::new(),
            master_ci_gate: false,
            master_ci_timeout_secs: 1,
            allowed_tools: None,
            doctor_enabled: false,
            r2_enabled: false,
            r2_target_per_stratum: 0,
            r2_steady_state_p: 0.0,
            max_rework: quorum_core::lifecycle::REWORK_CAP,
            codex_sandbox: "danger-full-access".into(),
            grok: Default::default(),
            pr_target_program: None,
        }
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn response(status: i32, http: u16, body: &str) -> std::process::Output {
        std::process::Output {
            status: std::process::ExitStatus::from_raw(status),
            stdout: format!("HTTP/2 {http}\r\ncontent-type: application/json\r\n\r\n{body}")
                .into_bytes(),
            stderr: Vec::new(),
        }
    }

    #[test]
    fn pr_lookup_distinguishes_absent_from_transport_and_preserves_terminal() {
        assert!(
            parse_cleanup_pr_response(&response(256, 404, r#"{"message":"Not Found"}"#))
                .unwrap()
                .is_none()
        );
        assert!(parse_cleanup_pr_response(&response(256, 500, r#"{"message":"no"}"#)).is_err());
        let merged = parse_cleanup_pr_response(&response(0, 200,
            r#"{"state":"closed","merged":true,"head":{"ref":"q/task","sha":"abc","repo":{"full_name":"o/r"}},"base":{"ref":"develop","repo":{"full_name":"o/r"}}}"#))
            .unwrap().unwrap();
        assert!(merged.merged);
        assert_eq!(merged.head_repo, "o/r");
        assert_eq!(merged.base_ref, "develop");
        let expected = ProposedChangeRef {
            pr_number: 42,
            head_ref: "old-head".into(),
            head_sha: "old-sha".into(),
        };
        assert!(
            validate_cleanup_pr_target(&merged, &expected, "o/r", "develop").unwrap(),
            "terminal PR must survive a moved mutable head"
        );
    }
    #[test]
    fn path_prefix_and_traversal_are_not_children() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("quorum-wt");
        std::fs::create_dir(&base).unwrap();
        assert!(validate_worktree_path(&base, &base.join("task-1")).is_ok());
        assert!(validate_worktree_path(&base, &dir.path().join("quorum-wt-evil/task-1")).is_err());
        assert!(validate_worktree_path(&base, &base.join("../victim")).is_err());
        assert!(validate_worktree_path(&base, &base).is_err());
    }

    #[test]
    fn symlinked_worktree_cannot_escape_configured_base() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("quorum-wt");
        let outside = dir.path().join("outside");
        std::fs::create_dir(&base).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let escaped = base.join("task-1");
        symlink(&outside, &escaped).unwrap();
        assert!(validate_worktree_path(&base, &escaped)
            .unwrap_err()
            .contains("different canonical child"));
    }

    #[test]
    fn symlinked_worktree_cannot_alias_another_child() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("quorum-wt");
        let other = base.join("task-2");
        std::fs::create_dir(&base).unwrap();
        std::fs::create_dir(&other).unwrap();
        let aliased = base.join("task-1");
        symlink(&other, &aliased).unwrap();
        assert!(validate_worktree_path(&base, &aliased)
            .unwrap_err()
            .contains("different canonical child"));
    }

    #[test]
    fn existing_unregistered_worktree_fails_loud() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("existing");
        std::fs::create_dir(&path).unwrap();
        let expected = WorktreeRef {
            path,
            branch: "daemon/task".into(),
        };
        assert!(validate_worktree_registration(None, &expected)
            .unwrap_err()
            .contains("allocation"));
    }

    #[tokio::test]
    async fn mismatched_process_identity_never_signals_live_process() {
        use std::os::unix::process::CommandExt;
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let conn = quorum_core::db::open(&db_path).unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        conn.execute(
            "INSERT INTO journal(agent,role,task_id,session_id,phase,pid,updated_at)
             VALUES ('worker-a','worker',22,'new-session','working',?1,1)",
            [pid],
        )
        .unwrap();
        let work = CleanupWork {
            key: CleanupKey {
                graph_id: 1,
                task_id: 22,
                artifact_kind: "process".into(),
                artifact_ref:
                    serde_json::json!({"agent":"worker-a","session_id":"old-session","pid":pid})
                        .to_string(),
            },
            attempt: 1,
        };
        let error = cleanup_process_at(&db_path, &work).await.unwrap_err();
        assert!(error.contains("identity mismatch"));
        assert!(
            child.try_wait().unwrap().is_none(),
            "mismatched PID must remain alive"
        );
        unsafe {
            libc::killpg(pid, libc::SIGKILL);
        }
        child.wait().unwrap();
    }

    #[tokio::test]
    async fn exact_process_kill_reaps_deletes_journal_and_replays() {
        use std::os::unix::process::CommandExt;
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (22,'cancelled child','cancelled','owner',1,1)",
            [],
        )
        .unwrap();
        let run_id = quorum_core::agent_runs::insert(
            &conn, 22, "worker-a", "worker", "test", "medium", "codex", 1,
        )
        .unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        conn.execute("INSERT INTO journal(agent,role,task_id,session_id,phase,pid,updated_at) VALUES ('worker-a','worker',22,'session-a','working',?1,1)", [pid]).unwrap();
        let work = CleanupWork {
            key: CleanupKey {
                graph_id: 1,
                task_id: 22,
                artifact_kind: "process".into(),
                artifact_ref:
                    serde_json::json!({"agent":"worker-a","session_id":"session-a","pid":pid})
                        .to_string(),
            },
            attempt: 1,
        };
        let reaper = tokio::task::spawn_blocking(move || child.wait().unwrap());
        cleanup_process_at(&db_path, &work).await.unwrap();
        assert!(!reaper.await.unwrap().success());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM journal", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row(
                "SELECT ended_at IS NOT NULL,end_reason FROM agent_runs WHERE id=?1",
                [run_id],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?))
            )
            .unwrap(),
            (true, "cancelled".into())
        );
        cleanup_process_at(&db_path, &work).await.unwrap();
    }

    #[tokio::test]
    async fn live_process_settlement_closes_only_matching_role_before_journal_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (22,'cancelled child','cancelled','owner',1,1)",
            [],
        )
        .unwrap();
        let worker_run = quorum_core::agent_runs::insert(
            &conn, 22, "agent-a", "worker", "test", "medium", "codex", 1,
        )
        .unwrap();
        let reviewer_run = quorum_core::agent_runs::insert(
            &conn, 22, "agent-a", "reviewer", "test", "medium", "codex", 2,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO journal(agent,role,task_id,session_id,phase,pid,updated_at)
             VALUES ('agent-a','worker',22,'session-a','working',12345,1)",
            [],
        )
        .unwrap();
        let identity = ProcessRef {
            agent: "agent-a".into(),
            session_id: "session-a".into(),
            pid: 12345,
        };
        settle_process_identity(&db_path, 22, &identity)
            .await
            .unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT ended_at IS NOT NULL,end_reason FROM agent_runs WHERE id=?1",
                [worker_run],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?))
            )
            .unwrap(),
            (true, "cancelled".into())
        );
        assert!(conn
            .query_row(
                "SELECT ended_at IS NULL FROM agent_runs WHERE id=?1",
                [reviewer_run],
                |row| row.get::<_, bool>(0)
            )
            .unwrap());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM journal", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn exact_open_pr_close_uses_bounded_cli_boundary() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let program = dir.path().join("fake-gh");
        let log = dir.path().join("args");
        std::fs::write(
            &program,
            format!("#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n", log.display()),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&program).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&program, permissions).unwrap();
        close_cleanup_pr_with_program(dir.path(), "owner/repo", 42, &program)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(log).unwrap(),
            "pr\nclose\n42\n--repo\nowner/repo\n"
        );
    }

    #[tokio::test]
    async fn startup_replays_interrupted_cancel_cleanup_and_preserves_done_history() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let remote = dir.path().join("remote.git");
        let worktree_base = dir.path().join("worktrees");
        let worker_tree = worktree_base.join("task-2");
        std::fs::create_dir(&repo).unwrap();
        std::fs::create_dir(&worktree_base).unwrap();
        git(dir.path(), &["init", "--bare", remote.to_str().unwrap()]);
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "test@example.com"]);
        git(&repo, &["config", "user.name", "Test"]);
        std::fs::write(repo.join("README"), "base\n").unwrap();
        git(&repo, &["add", "README"]);
        git(&repo, &["commit", "-m", "base"]);
        git(&repo, &["branch", "daemon/task-2"]);
        git(
            &repo,
            &[
                "worktree",
                "add",
                worker_tree.to_str().unwrap(),
                "daemon/task-2",
            ],
        );
        std::fs::write(worker_tree.join("result"), "finished\n").unwrap();
        git(&worker_tree, &["add", "result"]);
        git(&worker_tree, &["commit", "-m", "finished result"]);
        let worker_sha = git(&worker_tree, &["rev-parse", "HEAD"]);
        git(
            &repo,
            &[
                "config",
                &format!("url.{}.insteadOf", remote.display()),
                "https://github.com/owner/repo.git",
            ],
        );
        git(
            &repo,
            &[
                "push",
                "https://github.com/owner/repo.git",
                &format!("{worker_sha}:refs/heads/daemon/task-2"),
            ],
        );

        let db_path = dir.path().join("quorum.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at) VALUES
               (1,'source','cancelled','owner',1,10),
               (2,'completed child','done','owner',1,9),
               (3,'unfinished child','cancelled','owner',1,10);
             INSERT INTO task_decompositions(id,source_task_id,state,active,freeze_active,planned_source_revision,created_at,updated_at)
               VALUES (1,1,'cancelled',0,0,1,1,10);
             INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
               VALUES (1,2,'done-child',1,0),(1,3,'unfinished-child',1,0);
             INSERT INTO events(ts,kind,subject,body,expires_at) VALUES (9,'merge_succeeded','task#2','historical merge',9999999999);
             INSERT INTO approvals(pr_number,review_role,task_id,author,reviewer,verdict,blocking_count,approved_head_sha,created_at)
               VALUES (42,'r1',2,'author','reviewer','approve',0,'historical-head',9);
             INSERT INTO review_findings(pr_number,task_id,reviewer,kind,text,source_endpoint,created_at)
               VALUES (42,2,'reviewer','suggestion','historical review','pulls',9);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_branches(task_id,branch,worktree,allocated_by,allocated_at,provenance_sha)
             VALUES (2,'daemon/task-2',?1,'daemon',2,?2)",
            rusqlite::params![worker_tree.to_string_lossy(), worker_sha],
        )
        .unwrap();
        quorum_core::pr_targets::upsert(&mut conn, 2, 42, "daemon/task-2", &worker_sha, false)
            .unwrap();
        use std::os::unix::process::CommandExt;
        let mut stale_child = std::process::Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let stale_pid = stale_child.id() as i32;
        conn.execute("INSERT INTO journal(agent,role,task_id,session_id,phase,pid,updated_at) VALUES ('worker-a','worker',2,'session-a','working',?1,9)", [stale_pid]).unwrap();
        let stale_run_id = quorum_core::agent_runs::insert(
            &conn, 2, "worker-a", "worker", "test", "medium", "codex", 9,
        )
        .unwrap();
        let process_ref = serde_json::json!({
            "agent":"worker-a", "session_id":"session-a", "pid":stale_pid
        })
        .to_string();
        let pr_ref = serde_json::json!({
            "pr_number":42, "head_ref":"daemon/task-2", "head_sha":worker_sha
        })
        .to_string();
        let worktree_ref = serde_json::json!({
            "path":worker_tree, "branch":"daemon/task-2"
        })
        .to_string();
        let branch_ref = serde_json::json!({
            "name":"daemon/task-2", "expected_sha":worker_sha
        })
        .to_string();
        for (kind, artifact, state, attempts, updated) in [
            ("process", process_ref, "running", 1, 1),
            // A merged/terminal proposed change is history, not destructive work.
            ("proposed-change", pr_ref, "done", 1, 2),
            ("worktree", worktree_ref, "pending", 0, 3),
            ("branch", branch_ref, "pending", 0, 4),
        ] {
            conn.execute(
                "INSERT INTO decomposition_cleanup(graph_id,task_id,artifact_kind,artifact_ref,state,attempts,updated_at)
                 VALUES (1,2,?1,?2,?3,?4,?5)",
                rusqlite::params![kind, artifact, state, attempts, updated],
            )
            .unwrap();
        }
        drop(conn); // daemon crash/restart boundary

        let config = restart_config(db_path.clone(), repo.clone(), worktree_base);
        let manager = WorktreeManager::new();
        let stale_reaper = tokio::task::spawn_blocking(move || stale_child.wait().unwrap());
        assert!(startup(&config, &manager).await.unwrap() >= 3);
        assert!(
            !stale_reaper.await.unwrap().success(),
            "stale process must be killed and reaped"
        );
        // A second restart proves terminal replay and tombstone retirement are idempotent.
        assert_eq!(startup(&config, &manager).await.unwrap(), 0);

        let conn = quorum_core::db::open(&db_path).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM decomposition_cleanup WHERE state NOT IN ('done','exhausted')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT status FROM tasks WHERE id=2", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "done"
        );
        assert_eq!(
            conn.query_row("SELECT status FROM tasks WHERE id=3", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "cancelled"
        );
        assert_eq!(
            conn.query_row("SELECT status FROM tasks WHERE id=1", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "cancelled"
        );
        assert_eq!(
            conn.query_row(
                "SELECT state FROM task_decompositions WHERE id=1",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "cancelled"
        );
        assert_eq!(
            conn.query_row(
                "SELECT pr_number FROM pr_targets WHERE task_id=2",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            42
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM events WHERE subject='task#2'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM approvals WHERE pr_number=42",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM review_findings WHERE pr_number=42",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT count(*) FROM journal", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row(
                "SELECT ended_at IS NOT NULL,end_reason FROM agent_runs WHERE id=?1",
                [stale_run_id],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, String>(1)?))
            )
            .unwrap(),
            (true, "cancelled".into())
        );
        assert_eq!(
            conn.query_row(
                "SELECT state||':'||attempts FROM decomposition_cleanup
                 WHERE graph_id=1 AND task_id=2 AND artifact_kind='proposed-change'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "done:1",
            "terminal merged-PR cleanup history must not be replayed"
        );
        assert_eq!(
            conn.query_row(
                "SELECT branch FROM task_branches WHERE task_id=2",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "daemon/task-2"
        );
        assert!(!worker_tree.exists());
        assert!(!std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["show-ref", "--verify", "refs/heads/daemon/task-2"])
            .output()
            .unwrap()
            .status
            .success());
        assert!(!std::process::Command::new("git")
            .arg("-C")
            .arg(&remote)
            .args(["show-ref", "--verify", "refs/heads/daemon/task-2"])
            .output()
            .unwrap()
            .status
            .success());
    }

    #[test]
    fn exhausted_failure_does_not_block_independent_cleanup_progress() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        quorum_core::db::migrate(&conn).unwrap();
        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at) VALUES
               (1,'source','cancelled','owner',1,1),(2,'a','cancelled','owner',1,1),(3,'b','cancelled','owner',1,1);
             INSERT INTO task_decompositions(id,source_task_id,state,active,freeze_active,planned_source_revision,created_at,updated_at)
               VALUES (1,1,'cancelled',0,0,1,1,1);
             INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active) VALUES
               (1,2,'a',1,0),(1,3,'b',1,0);",
        ).unwrap();
        for (task, agent, pid, updated) in [(2, "a", 2, 1), (3, "b", 3, 2)] {
            let artifact =
                serde_json::json!({"agent":agent,"pid":pid,"session_id":"s"}).to_string();
            conn.execute("INSERT INTO decomposition_cleanup(graph_id,task_id,artifact_kind,artifact_ref,state,updated_at) VALUES (1,?1,'process',?2,'pending',?3)", rusqlite::params![task, artifact, updated]).unwrap();
        }
        let failed = decomposition_cleanup::claim_next(&mut conn, 10)
            .unwrap()
            .unwrap();
        assert_eq!(failed.key.task_id, 2);
        decomposition_cleanup::fail(&mut conn, &failed, "identity mismatch", 20).unwrap();
        let independent = decomposition_cleanup::claim_next(&mut conn, 21)
            .unwrap()
            .unwrap();
        assert_eq!(independent.key.task_id, 3);
        decomposition_cleanup::complete(&mut conn, &independent, 22).unwrap();
        for now in [23, 24] {
            let retry = decomposition_cleanup::claim_next(&mut conn, now)
                .unwrap()
                .unwrap();
            assert_eq!(retry.key.task_id, 2);
            decomposition_cleanup::fail(&mut conn, &retry, "identity mismatch", now).unwrap();
        }
    }
}
