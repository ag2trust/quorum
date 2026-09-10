//! Daemon-owned execution and restart reconciliation for branch sync rows.
//!
//! Each pass reads one durable row, does all GitHub/Git work without a
//! database transaction, then settles exactly the phase that operation earned.

use super::worktree::{SyncMerge, WorktreeManager};
use super::{
    log, parse_created_pr_number, parse_initial_pr_list, resolve_pr_target_with_program,
    run_publication_gh_command, validate_initial_pr_target, PrTarget, ServeConfig,
};
use quorum_core::branch_sync::{self, BranchSync};
use quorum_core::error::{QuorumError, Result};
use std::path::Path;

const PR_OPEN: &str = "OPEN";

/// Reconcile one active clean-path row. Invoking this on startup and once per
/// normal tick gives crash recovery without an unbounded non-task scan.
pub async fn reconcile_one(config: &ServeConfig, worktrees: &WorktreeManager) -> Result<()> {
    let db_path = config.db_path.clone();
    let sync = tokio::task::spawn_blocking(move || -> Result<Option<BranchSync>> {
        let conn = quorum_core::db::open(&db_path)?;
        branch_sync::next_clean_path(&conn)
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
        "published" => verify_published(config, &sync).await,
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
    format!(
        "sync/{}-into-{}-{}",
        sync.source_branch, sync.target_branch, sync.id
    )
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

async fn verify_published(
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
        let _ = branch_sync::touch_published(&mut conn, id, quorum_core::clock::now())?;
        Ok(())
    })
    .await
    .map_err(|error| format!("branch sync published refresh join: {error}"))?
    .map_err(|error| error.to_string())
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
    use std::sync::Arc;

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
    fn sync_branch_cannot_match_task_branch_grammar() {
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
            task_id: None,
            active: true,
            requested_by: "owner".into(),
            last_error: None,
            created_at: 0,
            updated_at: 0,
        };
        assert_eq!(sync_branch(&row), "sync/main-into-develop-42");
        assert!(!sync_branch(&row).starts_with("daemon/"));
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

    #[cfg(unix)]
    fn write_gh(program: &Path, head: &str, state: &str) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(
            program,
            format!(
                "#!/bin/sh\ncase \"$1 $2\" in\n  'pr list') echo '[]' ;;\n  'pr create') echo 'https://github.test/owner/repo/pull/42' ;;\n  'pr view') echo '{{\"headRefName\":\"sync/develop-into-main-1\",\"headRefOid\":\"{head}\",\"isCrossRepository\":false,\"baseRefName\":\"main\",\"state\":\"{state}\"}}' ;;\n  *) exit 2 ;;\nesac\n"
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

        reconcile_one(&config, &manager).await.unwrap();
        let conn = quorum_core::db::open(&db_path).unwrap();
        let pinned = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(pinned.phase, "pinned");
        assert!(pinned.source_sha.is_some() && pinned.target_sha.is_some());
        drop(conn);

        reconcile_one(&config, &manager).await.unwrap();
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
        reconcile_one(&config, &manager).await.unwrap();
        let conn = quorum_core::db::open(&db_path).unwrap();
        let published = branch_sync::get(&conn, row.id).unwrap().unwrap();
        assert_eq!(published.phase, "published");
        assert_eq!(published.pr, Some(42));
        drop(conn);

        reconcile_one(&config, &manager).await.unwrap();
        write_gh(&gh, "stale", "OPEN");
        reconcile_one(&config, &manager).await.unwrap();
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
