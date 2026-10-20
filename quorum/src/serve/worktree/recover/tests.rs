use super::*;
use std::path::PathBuf;
use std::process::Command as StdCommand;

fn git(dir: &Path, args: &[&str]) -> String {
    let out = StdCommand::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn git_ok(dir: &Path, args: &[&str]) -> bool {
    StdCommand::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap()
        .status
        .success()
}

/// Repo with a bare origin; `main` is pushed so base commits are on a remote.
struct Fixture {
    _tmp: tempfile::TempDir,
    repo: PathBuf,
    base: PathBuf,
}

fn fixture() -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let origin = tmp.path().join("origin.git");
    let repo = tmp.path().join("repo");
    let base = tmp.path().join("worktrees");
    std::fs::create_dir_all(&base).unwrap();
    git(
        tmp.path(),
        &["init", "--bare", "-b", "main", &origin.to_string_lossy()],
    );
    git(tmp.path(), &["init", "-b", "main", &repo.to_string_lossy()]);
    git(&repo, &["config", "user.email", "t@t"]);
    git(&repo, &["config", "user.name", "T"]);
    std::fs::write(repo.join("tracked.txt"), "base\n").unwrap();
    std::fs::write(repo.join(".gitignore"), "ignored.log\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "base"]);
    git(
        &repo,
        &["remote", "add", "origin", &origin.to_string_lossy()],
    );
    git(&repo, &["push", "origin", "main"]);
    git(&repo, &["fetch", "origin"]);
    Fixture {
        _tmp: tmp,
        repo,
        base,
    }
}

async fn provision_worker(mgr: &WorktreeManager, f: &Fixture, task: i64) -> (PathBuf, String) {
    let branch = format!("daemon/anvil-t{task}");
    let wt = f.base.join(format!("anvil-t{task}"));
    mgr.provision(&f.repo, &branch, &wt, "origin/main")
        .await
        .unwrap();
    (wt, branch)
}

fn ident(task: i64) -> RecoverIdentity {
    RecoverIdentity::new(task, "Anvil", "idle_reaped")
}

fn recover_refs(repo: &Path, task: i64) -> Vec<String> {
    let out = git(
        repo,
        &[
            "for-each-ref",
            "--format=%(refname)",
            &format!("refs/quorum-recover/t{task}"),
        ],
    );
    out.lines().map(str::to_string).collect()
}

#[cfg(unix)]
fn failing_git_shim(dir: &Path, fail_on: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let real = String::from_utf8(
        StdCommand::new("sh")
            .args(["-c", "command -v git"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    let shim = dir.join("git-shim");
    std::fs::write(
        &shim,
        format!(
            "#!/bin/sh\nfor a in \"$@\"; do [ \"$a\" = \"{fail_on}\" ] && {{ echo injected >&2; exit 1; }}; done\nexec \"{real}\" \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    shim
}

#[tokio::test]
async fn release_snapshots_uncommitted_tracked_and_untracked_before_force_remove() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 412).await;
    let head = git(&wt, &["rev-parse", "HEAD"]);
    std::fs::write(wt.join("tracked.txt"), "finished work\n").unwrap();
    std::fs::write(wt.join("new_module.rs"), "pub fn done() {}\n").unwrap();
    std::fs::write(wt.join("ignored.log"), "noise\n").unwrap();
    // The worker's own index is not consulted or modified.
    let index_before = git(&wt, &["diff", "--cached", "--name-only"]);

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(412))
        .await
        .unwrap();

    assert_eq!(created.len(), 1, "{created:?}");
    assert!(!wt.exists(), "worktree must be removed after snapshot");
    assert!(!git_ok(
        &f.repo,
        &["rev-parse", "--verify", &format!("refs/heads/{branch}")]
    ));
    let snap = &created[0];
    assert!(snap.refname.starts_with("refs/quorum-recover/t412/"));
    assert_eq!(snap.provenance(), ("anvil".into(), "idle_reaped".into()));
    assert_eq!(recover_refs(&f.repo, 412), vec![snap.refname.clone()]);
    assert_eq!(
        git(&f.repo, &["rev-parse", &format!("{}^", snap.sha)]),
        head
    );
    assert_eq!(
        git(&f.repo, &["show", &format!("{}:tracked.txt", snap.sha)]),
        "finished work"
    );
    assert_eq!(
        git(&f.repo, &["show", &format!("{}:new_module.rs", snap.sha)]),
        "pub fn done() {}"
    );
    assert!(!git_ok(
        &f.repo,
        &["cat-file", "-e", &format!("{}:ignored.log", snap.sha)]
    ));
    assert!(index_before.is_empty());
}

#[tokio::test]
async fn release_snapshots_committed_unpushed_work_before_branch_delete() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 7).await;
    std::fs::write(wt.join("feature.rs"), "x\n").unwrap();
    git(&wt, &["add", "feature.rs"]);
    git(
        &wt,
        &[
            "-c",
            "user.email=w@w",
            "-c",
            "user.name=W",
            "commit",
            "-m",
            "work",
        ],
    );
    let tip = git(&wt, &["rev-parse", "HEAD"]);

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(7))
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].sha, tip, "clean committed work is pinned as-is");
    assert!(!git_ok(
        &f.repo,
        &["rev-parse", "--verify", &format!("refs/heads/{branch}")]
    ));
}

#[tokio::test]
async fn release_clean_worktree_without_unique_commits_creates_no_ref() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 8).await;

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(8))
        .await
        .unwrap();

    assert!(created.is_empty());
    assert!(recover_refs(&f.repo, 8).is_empty());
    assert!(!wt.exists());
}

#[tokio::test]
async fn release_keeping_branch_does_not_pin_commits_the_branch_still_holds() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 9).await;
    std::fs::write(wt.join("feature.rs"), "x\n").unwrap();
    git(&wt, &["add", "feature.rs"]);
    git(
        &wt,
        &[
            "-c",
            "user.email=w@w",
            "-c",
            "user.name=W",
            "commit",
            "-m",
            "work",
        ],
    );

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, false, &ident(9))
        .await
        .unwrap();

    assert!(created.is_empty(), "kept branch already preserves commits");
    assert!(git_ok(
        &f.repo,
        &["rev-parse", "--verify", &format!("refs/heads/{branch}")]
    ));
    assert!(!wt.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn release_destroys_nothing_when_snapshot_fails_then_retry_preserves() {
    let f = fixture();
    let shim_dir = tempfile::tempdir().unwrap();
    let failing = WorktreeManager::with_config(
        failing_git_shim(shim_dir.path(), "commit-tree"),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(30),
    );
    let (wt, branch) = provision_worker(&failing, &f, 10).await;
    std::fs::write(wt.join("tracked.txt"), "precious\n").unwrap();

    let error = failing
        .release_preserving(&f.repo, &wt, &branch, true, &ident(10))
        .await
        .unwrap_err();

    assert!(matches!(error, ReleaseError::Preserve(_)), "{error:?}");
    assert!(wt.exists(), "worktree must survive a failed snapshot");
    assert_eq!(
        std::fs::read_to_string(wt.join("tracked.txt")).unwrap(),
        "precious\n"
    );
    assert!(git_ok(
        &f.repo,
        &["rev-parse", "--verify", &format!("refs/heads/{branch}")]
    ));
    assert!(recover_refs(&f.repo, 10).is_empty(), "no partial ref");

    // A later attempt (e.g. after a daemon restart) completes the release.
    let created = WorktreeManager::new()
        .release_preserving(&f.repo, &wt, &branch, true, &ident(10))
        .await
        .unwrap();
    assert_eq!(created.len(), 1);
    assert!(!wt.exists());
}

#[tokio::test]
async fn concurrent_and_repeated_releases_create_one_snapshot() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 11).await;
    std::fs::write(wt.join("tracked.txt"), "work\n").unwrap();

    let id = ident(11);
    let (a, b) = tokio::join!(
        mgr.release_preserving(&f.repo, &wt, &branch, true, &id),
        mgr.release_preserving(&f.repo, &wt, &branch, true, &id),
    );
    a.unwrap();
    b.unwrap();
    mgr.release_preserving(&f.repo, &wt, &branch, true, &ident(11))
        .await
        .unwrap();

    assert_eq!(recover_refs(&f.repo, 11).len(), 1);
}

#[tokio::test]
async fn identical_dirty_state_is_not_snapshotted_twice() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 12).await;
    std::fs::write(wt.join("tracked.txt"), "same\n").unwrap();
    let _g = mgr.lock.lock().await;
    let first = mgr
        .preserve_unlocked(&f.repo, Some(&wt), None, &ident(12))
        .await
        .unwrap();
    let second = mgr
        .preserve_unlocked(&f.repo, Some(&wt), None, &ident(12))
        .await
        .unwrap();
    drop(_g);
    assert_eq!(first.len(), 1);
    assert!(second.is_empty());
    let _ = branch;
}

#[tokio::test]
async fn retention_keeps_newest_per_task_and_never_touches_other_refs() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let base = git(&f.repo, &["rev-parse", "HEAD"]);
    // A human-made recover branch and another task's ref must survive.
    git(&f.repo, &["branch", "recover/t20-manual", &base]);
    let (wt, branch) = provision_worker(&mgr, &f, 21).await;
    std::fs::write(wt.join("other.txt"), "other task\n").unwrap();
    mgr.release_preserving(&f.repo, &wt, &branch, true, &ident(21))
        .await
        .unwrap();

    let mut made = Vec::new();
    for round in 0..3 {
        let (wt, branch) = provision_worker(&mgr, &f, 20).await;
        std::fs::write(wt.join("tracked.txt"), format!("round {round}\n")).unwrap();
        let created = mgr
            .release_preserving(&f.repo, &wt, &branch, true, &ident(20))
            .await
            .unwrap();
        made.extend(created);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let kept = recover_refs(&f.repo, 20);
    assert_eq!(kept.len(), RECOVER_REFS_PER_TASK);
    assert!(!kept.contains(&made[0].refname), "oldest pruned");
    assert!(kept.contains(&made[2].refname), "newest kept");
    assert_eq!(recover_refs(&f.repo, 21).len(), 1);
    assert!(git_ok(
        &f.repo,
        &["rev-parse", "--verify", "refs/heads/recover/t20-manual"]
    ));

    let latest = mgr.latest_recover_ref(&f.repo, 20).await.unwrap().unwrap();
    assert_eq!(latest.refname, made[2].refname);
    let error = mgr
        .delete_recover_refs(
            &f.repo,
            &[RecoverRef {
                refname: "refs/heads/recover/t20-manual".into(),
                sha: base,
            }],
        )
        .await
        .unwrap_err();
    assert!(error.contains("non-recover"), "{error}");
}

#[tokio::test]
async fn gc_snapshots_dirty_orphan_before_removal() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, _branch) = provision_worker(&mgr, &f, 30).await;
    std::fs::write(wt.join("untracked.rs"), "keep me\n").unwrap();

    let gc = mgr.gc_orphaned_detailed(&f.repo, &f.base, &[]).await;

    assert_eq!(gc.removed.len(), 1);
    assert!(gc.preserve_failures.is_empty());
    assert!(!wt.exists());
    let refs = recover_refs(&f.repo, 30);
    assert_eq!(refs.len(), 1, "branch-attributed task bucket");
    assert_eq!(
        git(&f.repo, &["show", &format!("{}:untracked.rs", refs[0])]),
        "keep me"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn gc_keeps_orphan_when_snapshot_fails() {
    let f = fixture();
    let shim_dir = tempfile::tempdir().unwrap();
    let failing = WorktreeManager::with_config(
        failing_git_shim(shim_dir.path(), "write-tree"),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(30),
    );
    let (wt, _branch) = provision_worker(&failing, &f, 31).await;
    std::fs::write(wt.join("tracked.txt"), "precious\n").unwrap();

    let gc = failing.gc_orphaned_detailed(&f.repo, &f.base, &[]).await;

    assert!(gc.removed.is_empty());
    assert_eq!(gc.preserve_failures.len(), 1);
    assert!(wt.exists());
}

#[tokio::test]
async fn remove_exact_snapshots_dirty_worktree() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 32).await;
    std::fs::write(wt.join("tracked.txt"), "cleanup intent\n").unwrap();

    mgr.remove_exact(&f.repo, &wt, &branch).await.unwrap();

    assert!(!wt.exists());
    assert_eq!(recover_refs(&f.repo, 32).len(), 1);
}

#[tokio::test]
async fn resume_branch_creates_deleted_branch_from_recover_ref() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let base = git(&f.repo, &["rev-parse", "origin/main"]);
    let (wt, branch) = provision_worker(&mgr, &f, 40).await;
    std::fs::write(wt.join("tracked.txt"), "half done\n").unwrap();
    let snap = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(40))
        .await
        .unwrap()
        .remove(0);

    let outcome = mgr
        .resume_branch_from_recover(&f.repo, 40, &branch, &base)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RecoverResume::Resumed {
            recover: snap.clone(),
            previous: None
        }
    );
    let (wt, _) = provision_worker(&mgr, &f, 40).await;
    assert_eq!(git(&wt, &["rev-parse", "HEAD"]), snap.sha);
    assert_eq!(
        std::fs::read_to_string(wt.join("tracked.txt")).unwrap(),
        "half done\n"
    );

    // Provisioning the same start again is a no-op.
    assert_eq!(
        mgr.resume_branch_from_recover(&f.repo, 40, "daemon/other-t40", &snap.sha)
            .await
            .unwrap(),
        RecoverResume::AlreadyIncorporated(snap)
    );
}

#[tokio::test]
async fn resume_branch_fast_forwards_kept_branch() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let base = git(&f.repo, &["rev-parse", "origin/main"]);
    let (wt, branch) = provision_worker(&mgr, &f, 41).await;
    std::fs::write(wt.join("tracked.txt"), "uncommitted\n").unwrap();
    let snap = mgr
        .release_preserving(&f.repo, &wt, &branch, false, &ident(41))
        .await
        .unwrap()
        .remove(0);

    let outcome = mgr
        .resume_branch_from_recover(&f.repo, 41, &branch, &base)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RecoverResume::Resumed {
            recover: snap.clone(),
            previous: Some(base)
        }
    );
    assert_eq!(git(&f.repo, &["rev-parse", &branch]), snap.sha);
}

#[tokio::test]
async fn resume_branch_leaves_stale_recover_ref_alone() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 42).await;
    std::fs::write(wt.join("tracked.txt"), "old\n").unwrap();
    let snap = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(42))
        .await
        .unwrap()
        .remove(0);
    // An unrelated base the snapshot does not descend from.
    git(&f.repo, &["checkout", "-q", "--orphan", "other"]);
    git(
        &f.repo,
        &["commit", "-q", "--allow-empty", "-m", "unrelated"],
    );
    let other = git(&f.repo, &["rev-parse", "HEAD"]);

    let outcome = mgr
        .resume_branch_from_recover(&f.repo, 42, &branch, &other)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RecoverResume::Stale {
            recover: snap.clone(),
            onto: other
        }
    );
    assert!(!git_ok(
        &f.repo,
        &["rev-parse", "--verify", &format!("refs/heads/{branch}")]
    ));
    assert_eq!(recover_refs(&f.repo, 42), vec![snap.refname]);
}

#[tokio::test]
async fn resume_without_recover_ref_is_none() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let base = git(&f.repo, &["rev-parse", "origin/main"]);
    assert_eq!(
        mgr.resume_branch_from_recover(&f.repo, 43, "daemon/anvil-t43", &base)
            .await
            .unwrap(),
        RecoverResume::None
    );
}

#[tokio::test]
async fn resume_worktree_fast_forwards_continuation_checkout() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 44).await;
    std::fs::write(wt.join("tracked.txt"), "rework in progress\n").unwrap();
    let snap = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(44))
        .await
        .unwrap()
        .remove(0);
    let wt2 = f.base.join("remediation-t44");
    mgr.provision(&f.repo, "remediation/forge-t44", &wt2, "origin/main")
        .await
        .unwrap();

    let outcome = mgr
        .resume_worktree_from_recover(&f.repo, &wt2, 44)
        .await
        .unwrap();

    assert!(
        matches!(outcome, RecoverResume::Resumed { .. }),
        "{outcome:?}"
    );
    assert_eq!(git(&wt2, &["rev-parse", "HEAD"]), snap.sha);
    assert_eq!(
        std::fs::read_to_string(wt2.join("tracked.txt")).unwrap(),
        "rework in progress\n"
    );
    assert!(git(&wt2, &["status", "--porcelain"]).is_empty());
}

#[test]
fn task_id_parsing_from_daemon_names() {
    assert_eq!(task_id_from_name("daemon/anvil-x6tt-t318"), Some(318));
    assert_eq!(task_id_from_name("remediation/forge-t4"), Some(4));
    assert_eq!(task_id_from_name("Anvil-t12"), Some(12));
    assert_eq!(task_id_from_name("review/pr-55-rev-1"), None);
    assert_eq!(task_id_from_name("sync/3"), None);
    assert_eq!(task_id_from_name("x-t"), None);
    assert_eq!(task_id_from_name("x-t0"), None);
}
