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

async fn noop(_: Vec<RecoverRef>) -> Result<(), String> {
    Ok(())
}

async fn noop2(_: RecoverIdentity, _: Vec<RecoverRef>) -> Result<(), String> {
    Ok(())
}

fn commit_in(wt: &Path, file: &str, body: &str) -> String {
    std::fs::write(wt.join(file), body).unwrap();
    git(wt, &["add", file]);
    git(
        wt,
        &[
            "-c",
            "user.email=w@w",
            "-c",
            "user.name=W",
            "commit",
            "-qm",
            "work",
        ],
    );
    git(wt, &["rev-parse", "HEAD"])
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

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(412), noop)
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
    assert_eq!(snap.snapshot_parent.as_deref(), Some(head.as_str()));
    assert!(!snap.tracked_only);
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
}

#[tokio::test]
async fn release_snapshots_committed_unpushed_work_before_branch_delete() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 7).await;
    let tip = commit_in(&wt, "feature.rs", "x\n");

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(7), noop)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    assert_eq!(created[0].sha, tip, "clean committed work is pinned as-is");
    assert_eq!(created[0].snapshot_parent, None);
    assert!(!git_ok(
        &f.repo,
        &["rev-parse", "--verify", &format!("refs/heads/{branch}")]
    ));
}

#[tokio::test]
async fn forged_recover_ref_does_not_count_as_preserving_commits() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 70).await;
    let tip = commit_in(&wt, "feature.rs", "x\n");
    // A worker forges a recover ref at its own commit.
    git(
        &f.repo,
        &[
            "update-ref",
            "refs/quorum-recover/t70/9999999999999/evil/forged",
            &tip,
        ],
    );

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(70), noop)
        .await
        .unwrap();

    assert_eq!(created.len(), 1, "a daemon ref must still be created");
    assert_ne!(
        created[0].refname,
        "refs/quorum-recover/t70/9999999999999/evil/forged"
    );
    assert_eq!(created[0].sha, tip);
}

#[tokio::test]
async fn release_clean_worktree_without_unique_commits_creates_no_ref() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 8).await;

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(8), noop)
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
    commit_in(&wt, "feature.rs", "x\n");

    let created = mgr
        .release_preserving(&f.repo, &wt, &branch, false, &ident(9), noop)
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
        .release_preserving(&f.repo, &wt, &branch, true, &ident(10), noop)
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

    let created = WorktreeManager::new()
        .release_preserving(&f.repo, &wt, &branch, true, &ident(10), noop)
        .await
        .unwrap();
    assert_eq!(created.len(), 1);
    assert!(!wt.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn untracked_capture_failure_falls_back_to_tracked_only_snapshot() {
    let f = fixture();
    let shim_dir = tempfile::tempdir().unwrap();
    let failing = WorktreeManager::with_config(
        failing_git_shim(shim_dir.path(), "-A"),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(30),
    );
    let (wt, branch) = provision_worker(&failing, &f, 11).await;
    std::fs::write(wt.join("tracked.txt"), "tracked edit\n").unwrap();
    std::fs::write(wt.join("untracked.rs"), "lost\n").unwrap();

    let created = failing
        .release_preserving(&f.repo, &wt, &branch, true, &ident(11), noop)
        .await
        .unwrap();

    assert_eq!(created.len(), 1);
    let snap = &created[0];
    assert!(snap.tracked_only);
    assert!(snap.untracked_skip.as_deref().unwrap().contains("injected"));
    assert!(
        snap.refname.ends_with("/idle_reaped-tracked-only"),
        "{}",
        snap.refname
    );
    assert_eq!(
        git(&f.repo, &["show", &format!("{}:tracked.txt", snap.sha)]),
        "tracked edit"
    );
    assert!(!git_ok(
        &f.repo,
        &["cat-file", "-e", &format!("{}:untracked.rs", snap.sha)]
    ));
    assert!(!wt.exists());
}

#[tokio::test]
async fn concurrent_and_repeated_releases_create_one_snapshot() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 12).await;
    std::fs::write(wt.join("tracked.txt"), "work\n").unwrap();

    let id = ident(12);
    let (a, b) = tokio::join!(
        mgr.release_preserving(&f.repo, &wt, &branch, true, &id, noop),
        mgr.release_preserving(&f.repo, &wt, &branch, true, &id, noop),
    );
    a.unwrap();
    b.unwrap();
    mgr.release_preserving(&f.repo, &wt, &branch, true, &id, noop)
        .await
        .unwrap();

    assert_eq!(recover_refs(&f.repo, 12).len(), 1);
}

#[tokio::test]
async fn identical_dirty_state_dedupes_only_against_trusted_snapshots() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, _branch) = provision_worker(&mgr, &f, 13).await;
    std::fs::write(wt.join("tracked.txt"), "same\n").unwrap();
    let guard = mgr.lock.lock().await;
    let first = mgr
        .preserve_unlocked(&f.repo, Some(&wt), None, &ident(13))
        .await
        .unwrap();
    let trusted = mgr
        .preserve_unlocked(
            &f.repo,
            Some(&wt),
            None,
            &ident(13).with_trusted(first.clone()),
        )
        .await
        .unwrap();
    // A forged identical ref (same tree/parent) is not trusted: no dedupe.
    let forged = vec![RecoverRef {
        refname: "refs/quorum-recover/t13/0000000000000/evil/forged".into(),
        ..first[0].clone()
    }];
    let untrusted = mgr
        .preserve_unlocked(&f.repo, Some(&wt), None, &ident(13).with_trusted(forged))
        .await
        .unwrap();
    drop(guard);
    assert_eq!(first.len(), 1);
    assert!(trusted.is_empty());
    assert_eq!(untrusted.len(), 1);
}

#[tokio::test]
async fn gc_snapshots_dirty_orphan_before_removal_with_daemon_attribution() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 30).await;
    std::fs::write(wt.join("untracked.rs"), "keep me\n").unwrap();
    let allocations = vec![(30, branch, wt.to_string_lossy().into_owned())];

    let gc = mgr
        .gc_orphaned_detailed(&f.repo, &f.base, &[], &allocations, noop2)
        .await;

    assert_eq!(gc.removed.len(), 1);
    assert!(gc.preserve_failures.is_empty());
    assert!(!wt.exists());
    let refs = recover_refs(&f.repo, 30);
    assert_eq!(
        refs.len(),
        1,
        "allocation-matched path and branch attribute to the task"
    );
    assert_eq!(gc.preserved[0].refname, refs[0]);
    assert_eq!(
        git(&f.repo, &["show", &format!("{}:untracked.rs", refs[0])]),
        "keep me"
    );
}

#[tokio::test]
async fn gc_never_attributes_from_a_worker_chosen_branch() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 31).await;
    let allocations = vec![(31, branch, wt.to_string_lossy().into_owned())];
    // The worker switches its checkout to a branch naming another task.
    git(&wt, &["checkout", "-q", "-b", "daemon/anvil-t99"]);
    std::fs::write(wt.join("tracked.txt"), "injected\n").unwrap();

    let gc = mgr
        .gc_orphaned_detailed(&f.repo, &f.base, &[], &allocations, noop2)
        .await;

    assert_eq!(gc.removed.len(), 1);
    assert!(
        recover_refs(&f.repo, 99).is_empty(),
        "never credited to task 99"
    );
    assert!(
        recover_refs(&f.repo, 31).is_empty(),
        "mismatched branch is unattributed"
    );
    let unattributed = git(
        &f.repo,
        &[
            "for-each-ref",
            "--format=%(refname)",
            "refs/quorum-recover/unattributed",
        ],
    );
    assert_eq!(unattributed.lines().count(), 1);
}

#[tokio::test]
async fn gc_never_attributes_a_forged_daemon_shaped_worktree() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (real_wt, real_branch) = provision_worker(&mgr, &f, 34).await;
    let allocations = vec![(34, real_branch, real_wt.to_string_lossy().into_owned())];
    // A worker forges a perfectly daemon-shaped checkout for task 99.
    let forged = f.base.join("anvil-t99");
    git(
        &f.repo,
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            "daemon/anvil-t99",
            &forged.to_string_lossy(),
            "origin/main",
        ],
    );
    std::fs::write(forged.join("backdoor.rs"), "evil\n").unwrap();

    let gc = mgr
        .gc_orphaned_detailed(
            &f.repo,
            &f.base,
            &[real_wt.to_str().unwrap()],
            &allocations,
            noop2,
        )
        .await;

    assert_eq!(gc.removed.len(), 1);
    assert!(
        recover_refs(&f.repo, 99).is_empty(),
        "never credited to task 99"
    );
    assert_eq!(gc.preserved.len(), 1);
    assert!(gc.preserved[0]
        .refname
        .starts_with("refs/quorum-recover/unattributed/"));
}

#[test]
fn daemon_attribution_requires_a_matching_allocation_record() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("Anvil-t5");
    std::fs::create_dir(&dir).unwrap();
    let alloc = vec![(
        5,
        "daemon/anvil-t5".to_string(),
        dir.to_string_lossy().into_owned(),
    )];
    assert_eq!(
        daemon_attribution(&dir, Some("refs/heads/daemon/anvil-t5"), &alloc),
        Some(5)
    );
    // Daemon-shaped names alone are never enough.
    assert_eq!(
        daemon_attribution(&dir, Some("refs/heads/daemon/anvil-t5"), &[]),
        None
    );
    assert_eq!(
        daemon_attribution(&dir, Some("refs/heads/remediation/anvil-t5"), &alloc),
        None
    );
    assert_eq!(
        daemon_attribution(&dir, Some("refs/heads/daemon/anvil-t6"), &alloc),
        None
    );
    assert_eq!(daemon_attribution(&dir, None, &alloc), None);
    let other = tmp.path().join("Anvil-t6");
    std::fs::create_dir(&other).unwrap();
    assert_eq!(
        daemon_attribution(&other, Some("refs/heads/daemon/anvil-t5"), &alloc),
        None
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
    let (wt, _branch) = provision_worker(&failing, &f, 32).await;
    std::fs::write(wt.join("tracked.txt"), "precious\n").unwrap();

    let gc = failing
        .gc_orphaned_detailed(&f.repo, &f.base, &[], &[], noop2)
        .await;

    assert!(gc.removed.is_empty());
    assert_eq!(gc.preserve_failures.len(), 1);
    assert!(wt.exists());
}

#[tokio::test]
async fn remove_exact_snapshots_dirty_worktree() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 33).await;
    std::fs::write(wt.join("tracked.txt"), "cleanup intent\n").unwrap();

    let created = mgr
        .remove_exact(&f.repo, &wt, &branch, &ident(33), noop)
        .await
        .unwrap();

    assert!(!wt.exists());
    assert_eq!(created.len(), 1);
    assert_eq!(recover_refs(&f.repo, 33).len(), 1);
}

async fn dirty_snapshot(
    mgr: &WorktreeManager,
    f: &Fixture,
    task: i64,
    keep_branch: bool,
) -> RecoverRef {
    let (wt, branch) = provision_worker(mgr, f, task).await;
    commit_in(&wt, "committed.rs", "c\n");
    std::fs::write(wt.join("tracked.txt"), "half done\n").unwrap();
    std::fs::write(wt.join("new_file.rs"), "new\n").unwrap();
    mgr.release_preserving(&f.repo, &wt, &branch, !keep_branch, &ident(task), noop)
        .await
        .unwrap()
        .remove(0)
}

#[tokio::test]
async fn resume_branch_restores_snapshot_as_uncommitted_changes() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let base = git(&f.repo, &["rev-parse", "origin/main"]);
    let snap = dirty_snapshot(&mgr, &f, 40, false).await;
    let parent = snap.snapshot_parent.clone().unwrap();

    let outcome = mgr
        .resume_branch_from_recover(&f.repo, &snap, "daemon/anvil-t40", &base)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RecoverResume::Resumed {
            recover: snap.clone(),
            previous: None,
            restore_uncommitted: true
        }
    );
    assert_eq!(
        git(&f.repo, &["rev-parse", "daemon/anvil-t40"]),
        parent,
        "worker commits only"
    );

    let (wt, _) = provision_worker(&mgr, &f, 40).await;
    let added = mgr
        .restore_snapshot_as_uncommitted(&f.repo, &wt, &snap)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        git(&wt, &["rev-parse", "HEAD"]),
        parent,
        "no daemon commit on the branch"
    );
    assert!(wt.join("committed.rs").exists());
    assert_eq!(
        std::fs::read_to_string(wt.join("tracked.txt")).unwrap(),
        "half done\n"
    );
    assert_eq!(added, vec!["new_file.rs".to_string()]);
    let status = git(&wt, &["status", "--porcelain"]);
    assert!(status.contains("M tracked.txt"), "{status}");
    assert!(status.contains("?? new_file.rs"), "{status}");
    assert!(
        git(&wt, &["diff", "--cached", "--name-only"]).is_empty(),
        "nothing staged"
    );
}

#[tokio::test]
async fn resume_branch_fast_forwards_kept_branch_to_pinned_commits() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let base = git(&f.repo, &["rev-parse", "origin/main"]);
    let (wt, branch) = provision_worker(&mgr, &f, 41).await;
    let tip = commit_in(&wt, "committed.rs", "c\n");
    let pinned = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(41), noop)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(pinned.sha, tip);
    git(&f.repo, &["branch", &branch, &base]);

    let outcome = mgr
        .resume_branch_from_recover(&f.repo, &pinned, &branch, &base)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RecoverResume::Resumed {
            recover: pinned,
            previous: Some(base),
            restore_uncommitted: false
        }
    );
    assert_eq!(git(&f.repo, &["rev-parse", &branch]), tip);
}

#[tokio::test]
async fn resume_refuses_a_recorded_ref_a_worker_moved() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let base = git(&f.repo, &["rev-parse", "origin/main"]);
    let snap = dirty_snapshot(&mgr, &f, 42, false).await;
    // A worker repoints the recorded ref at its own injected commit.
    let (wt, _) = provision_worker(&mgr, &f, 142).await;
    let injected = commit_in(&wt, "evil.rs", "evil\n");
    git(&f.repo, &["update-ref", &snap.refname, &injected]);

    let error = mgr
        .resume_branch_from_recover(&f.repo, &snap, "daemon/anvil-t42", &base)
        .await
        .unwrap_err();

    assert!(error.contains("no longer names recorded commit"), "{error}");
    assert!(!git_ok(
        &f.repo,
        &["rev-parse", "--verify", "refs/heads/daemon/anvil-t42"]
    ));
}

#[tokio::test]
async fn resume_refuses_a_snapshot_that_is_not_the_recorded_daemon_commit() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let base = git(&f.repo, &["rev-parse", "origin/main"]);
    let (wt, _) = provision_worker(&mgr, &f, 43).await;
    let worker_commit = commit_in(&wt, "evil.rs", "evil\n");
    let forged = RecoverRef {
        refname: "refs/quorum-recover/t43/0000000000001/anvil/idle_reaped".into(),
        sha: worker_commit.clone(),
        snapshot_parent: Some(base.clone()),
        tracked_only: false,
        untracked_skip: None,
    };
    git(&f.repo, &["update-ref", &forged.refname, &worker_commit]);

    let error = mgr
        .resume_branch_from_recover(&f.repo, &forged, "daemon/other-t43", &base)
        .await
        .unwrap_err();

    assert!(
        error.contains("not the recorded daemon snapshot"),
        "{error}"
    );
}

#[tokio::test]
async fn resume_branch_leaves_stale_recover_ref_alone() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let snap = dirty_snapshot(&mgr, &f, 44, false).await;
    git(&f.repo, &["checkout", "-q", "--orphan", "other"]);
    git(
        &f.repo,
        &["commit", "-q", "--allow-empty", "-m", "unrelated"],
    );
    let other = git(&f.repo, &["rev-parse", "HEAD"]);

    let outcome = mgr
        .resume_branch_from_recover(&f.repo, &snap, "daemon/anvil-t44", &other)
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
        &["rev-parse", "--verify", "refs/heads/daemon/anvil-t44"]
    ));
    assert_eq!(recover_refs(&f.repo, 44), vec![snap.refname]);
}

#[tokio::test]
async fn resume_worktree_fast_forwards_then_restores_uncommitted() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let snap = dirty_snapshot(&mgr, &f, 45, false).await;
    let parent = snap.snapshot_parent.clone().unwrap();
    let wt2 = f.base.join("forge-t45");
    mgr.provision(&f.repo, "remediation/forge-t45", &wt2, "origin/main")
        .await
        .unwrap();

    let outcome = mgr
        .resume_worktree_from_recover(&f.repo, &wt2, &snap)
        .await
        .unwrap();
    assert!(
        matches!(
            outcome,
            RecoverResume::Resumed {
                restore_uncommitted: true,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(git(&wt2, &["rev-parse", "HEAD"]), parent);
    mgr.restore_snapshot_as_uncommitted(&f.repo, &wt2, &snap)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(wt2.join("tracked.txt")).unwrap(),
        "half done\n"
    );
    assert_eq!(git(&wt2, &["rev-parse", "HEAD"]), parent);
}

#[test]
fn recover_ref_task_id_parsing() {
    assert_eq!(
        recover_ref_task_id("refs/quorum-recover/t12/1/a/b"),
        Some(12)
    );
    assert_eq!(
        recover_ref_task_id("refs/quorum-recover/unattributed/1/a/b"),
        None
    );
}

#[cfg(unix)]
#[tokio::test]
async fn untracked_only_work_is_never_destroyed_when_capture_fails() {
    let f = fixture();
    let shim_dir = tempfile::tempdir().unwrap();
    let failing = WorktreeManager::with_config(
        failing_git_shim(shim_dir.path(), "-A"),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(30),
    );
    let (wt, branch) = provision_worker(&failing, &f, 50).await;
    // Only new files; no tracked change and nothing committed.
    std::fs::write(wt.join("brand_new.rs"), "only copy\n").unwrap();

    let error = failing
        .release_preserving(&f.repo, &wt, &branch, true, &ident(50), noop)
        .await
        .unwrap_err();

    assert!(matches!(error, ReleaseError::Preserve(_)), "{error:?}");
    assert!(error
        .to_string()
        .contains("untracked files could not be captured"));
    assert!(
        wt.join("brand_new.rs").exists(),
        "the only copy must survive"
    );
    assert!(git_ok(
        &f.repo,
        &["rev-parse", "--verify", &format!("refs/heads/{branch}")]
    ));
    assert!(recover_refs(&f.repo, 50).is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn untracked_capture_failure_with_already_preserved_tracked_state_keeps_worktree() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 51).await;
    std::fs::write(wt.join("tracked.txt"), "tracked edit\n").unwrap();
    let guard = mgr.lock.lock().await;
    let trusted = mgr
        .preserve_unlocked(&f.repo, Some(&wt), None, &ident(51))
        .await
        .unwrap();
    drop(guard);
    std::fs::write(wt.join("later_untracked.rs"), "new\n").unwrap();
    let shim_dir = tempfile::tempdir().unwrap();
    let failing = WorktreeManager::with_config(
        failing_git_shim(shim_dir.path(), "-A"),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(30),
    );

    let error = failing
        .release_preserving(
            &f.repo,
            &wt,
            &branch,
            true,
            &ident(51).with_trusted(trusted),
            noop,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, ReleaseError::Preserve(_)), "{error:?}");
    assert!(wt.join("later_untracked.rs").exists());
}

#[tokio::test]
async fn recording_failure_destroys_nothing() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let (wt, branch) = provision_worker(&mgr, &f, 52).await;
    std::fs::write(wt.join("tracked.txt"), "work\n").unwrap();

    let error = mgr
        .release_preserving(&f.repo, &wt, &branch, true, &ident(52), |_| async {
            Err("db unavailable".to_string())
        })
        .await
        .unwrap_err();

    assert!(matches!(error, ReleaseError::Preserve(_)), "{error:?}");
    assert!(wt.exists(), "write ref -> record row -> destroy");
    assert!(git_ok(
        &f.repo,
        &["rev-parse", "--verify", &format!("refs/heads/{branch}")]
    ));
}

/// The snapshot conflicts with the checkout it is restored into (e.g. the
/// base integration touched the same lines): the worktree must come back
/// verified clean, never half-applied.
#[tokio::test]
async fn conflicting_restore_reverts_to_a_verified_clean_worktree() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let snap = dirty_snapshot(&mgr, &f, 53, false).await;
    let parent = snap.snapshot_parent.clone().unwrap();
    let wt = f.base.join("forge-t53");
    mgr.provision(&f.repo, "remediation/forge-t53", &wt, &parent)
        .await
        .unwrap();
    let conflicting = commit_in(&wt, "tracked.txt", "base changed these lines\n");

    let restored = mgr
        .restore_snapshot_as_uncommitted(&f.repo, &wt, &snap)
        .await
        .expect("worktree must be provably clean")
        .unwrap_err();

    assert!(restored.contains("does not apply cleanly"), "{restored}");
    assert_eq!(git(&wt, &["rev-parse", "HEAD"]), conflicting);
    assert!(git(&wt, &["status", "--porcelain", "--untracked-files=all"]).is_empty());
    assert!(!wt.join("new_file.rs").exists());
    assert_eq!(
        std::fs::read_to_string(wt.join("tracked.txt")).unwrap(),
        "base changed these lines\n"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn conflicting_restore_that_cannot_be_reverted_fails_hard() {
    let f = fixture();
    let mgr = WorktreeManager::new();
    let snap = dirty_snapshot(&mgr, &f, 54, false).await;
    let parent = snap.snapshot_parent.clone().unwrap();
    let wt = f.base.join("forge-t54");
    mgr.provision(&f.repo, "remediation/forge-t54", &wt, &parent)
        .await
        .unwrap();
    commit_in(&wt, "tracked.txt", "base changed these lines\n");
    let shim_dir = tempfile::tempdir().unwrap();
    let failing = WorktreeManager::with_config(
        failing_git_shim(shim_dir.path(), "--hard"),
        std::time::Duration::from_secs(30),
        std::time::Duration::from_secs(30),
    );

    let fatal = failing
        .restore_snapshot_as_uncommitted(&f.repo, &wt, &snap)
        .await
        .unwrap_err();

    assert!(fatal.contains("cannot reset worktree"), "{fatal}");
}
