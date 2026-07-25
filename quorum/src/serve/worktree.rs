//! Serialized git worktree operations for agent isolation.

use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::Mutex;

const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_LOCAL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct WorktreeManager {
    lock: Mutex<()>,
    git_bin: PathBuf,
    fetch_timeout: Duration,
    local_timeout: Duration,
}

/// Run a git subprocess with a bounded timeout. On timeout the child is killed
/// via `kill_on_drop` (SIGKILL) before this function returns, so the caller's
/// mutex guard remains held until the child is dead.
async fn run_git(
    mut cmd: Command,
    timeout: Duration,
    label: &str,
) -> Result<std::process::Output, String> {
    cmd.kill_on_drop(true);
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => Err(format!("{label}: {e}")),
        Err(_) => Err(format!("{label}: timed out after {}s", timeout.as_secs())),
    }
}

impl WorktreeManager {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            git_bin: PathBuf::from("git"),
            fetch_timeout: DEFAULT_FETCH_TIMEOUT,
            local_timeout: DEFAULT_LOCAL_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn with_config(git_bin: PathBuf, fetch_timeout: Duration, local_timeout: Duration) -> Self {
        Self {
            lock: Mutex::new(()),
            git_bin,
            fetch_timeout,
            local_timeout,
        }
    }

    fn git_cmd(&self, repo_dir: &Path) -> Command {
        let mut cmd = Command::new(&self.git_bin);
        cmd.arg("-C").arg(repo_dir);
        cmd
    }

    /// Check if a local branch exists. Caller MUST hold `self.lock`.
    async fn branch_exists_unlocked(&self, repo_dir: &Path, branch: &str) -> bool {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["rev-parse", "--verify", &format!("refs/heads/{branch}")]);
        match run_git(cmd, self.local_timeout, "git rev-parse --verify").await {
            Ok(out) => out.status.success(),
            Err(_) => false,
        }
    }

    /// Find the worktree path that has `branch` checked out, if any.
    /// Returns `None` when the branch is free or on any git error.
    /// Caller MUST hold `self.lock`.
    async fn find_worktree_for_branch_unlocked(
        &self,
        repo_dir: &Path,
        branch: &str,
    ) -> Option<String> {
        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["worktree", "list", "--porcelain"]);
        let out = match run_git(cmd, self.local_timeout, "git worktree list").await {
            Ok(out) if out.status.success() => out,
            _ => return None,
        };
        let stdout = String::from_utf8_lossy(&out.stdout);
        let target_ref = format!("branch refs/heads/{branch}");
        let mut current_wt: Option<String> = None;
        for line in stdout.lines() {
            if let Some(path) = line.strip_prefix("worktree ") {
                current_wt = Some(path.to_string());
            } else if line == target_ref {
                return current_wt;
            } else if line.is_empty() {
                current_wt = None;
            }
        }
        None
    }

    pub async fn provision(
        &self,
        repo_dir: &Path,
        branch: &str,
        worktree_dir: &Path,
        base_ref: &str,
    ) -> Result<PathBuf, String> {
        let _guard = self.lock.lock().await;

        let wt_path = worktree_dir.to_path_buf();

        if self.branch_exists_unlocked(repo_dir, branch).await {
            if let Some(existing_wt) = self
                .find_worktree_for_branch_unlocked(repo_dir, branch)
                .await
            {
                return Err(format!(
                    "branch collision: '{branch}' already checked out in worktree '{existing_wt}'"
                ));
            }
            // Reuse existing branch — preserves commits from a prior incarnation
            let mut cmd = self.git_cmd(repo_dir);
            cmd.args(["worktree", "add"]);
            cmd.arg(&wt_path).arg(branch);
            let add = run_git(cmd, self.local_timeout, "git worktree add (reuse branch)").await?;
            if !add.status.success() {
                return Err(format!(
                    "git worktree add (reuse branch) failed: {}",
                    String::from_utf8_lossy(&add.stderr)
                ));
            }
        } else {
            let mut cmd = self.git_cmd(repo_dir);
            cmd.args(["worktree", "add", "-b", branch]);
            cmd.arg(&wt_path).arg(base_ref);
            let add = run_git(cmd, self.local_timeout, "git worktree add").await?;
            if !add.status.success() {
                return Err(format!(
                    "git worktree add failed: {}",
                    String::from_utf8_lossy(&add.stderr)
                ));
            }
        }

        Ok(wt_path)
    }

    pub async fn fetch_and_provision(
        &self,
        repo_dir: &Path,
        branch: &str,
        worktree_dir: &Path,
        remote_branch: &str,
    ) -> Result<PathBuf, String> {
        let _guard = self.lock.lock().await;

        let mut fetch_cmd = self.git_cmd(repo_dir);
        fetch_cmd.args(["fetch", "origin", remote_branch]);
        let fetch = run_git(fetch_cmd, self.fetch_timeout, "git fetch").await?;

        if !fetch.status.success() {
            return Err(format!(
                "git fetch origin {remote_branch} failed: {}",
                String::from_utf8_lossy(&fetch.stderr)
            ));
        }

        let base_ref = format!("origin/{remote_branch}");
        let wt_path = worktree_dir.to_path_buf();

        if self.branch_exists_unlocked(repo_dir, branch).await {
            if let Some(existing_wt) = self
                .find_worktree_for_branch_unlocked(repo_dir, branch)
                .await
            {
                return Err(format!(
                    "branch collision: '{branch}' already checked out in worktree '{existing_wt}'"
                ));
            }
            // Delete stale review branch so it can be recreated at the remote head
            let mut del_cmd = self.git_cmd(repo_dir);
            del_cmd.args(["branch", "-D", branch]);
            let _ = run_git(del_cmd, self.local_timeout, "git branch -D (stale)").await;
        }

        let mut add_cmd = self.git_cmd(repo_dir);
        add_cmd.args(["worktree", "add", "-b", branch]);
        add_cmd.arg(&wt_path).arg(&base_ref);
        let add = run_git(add_cmd, self.local_timeout, "git worktree add").await?;

        if !add.status.success() {
            return Err(format!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&add.stderr)
            ));
        }

        Ok(wt_path)
    }

    /// Fetch a PR head via `refs/pull/<pr>/head` and provision a worktree.
    /// Works for both same-repo and fork PRs (GitHub exposes this ref
    /// regardless of head repository).
    pub async fn fetch_pr_and_provision(
        &self,
        repo_dir: &Path,
        branch: &str,
        worktree_dir: &Path,
        pr: i64,
    ) -> Result<PathBuf, String> {
        let _guard = self.lock.lock().await;

        let refspec = format!("+refs/pull/{pr}/head:refs/quorum-pr/{pr}");
        let mut fetch_cmd = self.git_cmd(repo_dir);
        fetch_cmd.args(["fetch", "origin", &refspec]);
        let fetch = run_git(fetch_cmd, self.fetch_timeout, "git fetch pr ref").await?;
        if !fetch.status.success() {
            return Err(format!(
                "git fetch origin {refspec} failed: {}",
                String::from_utf8_lossy(&fetch.stderr)
            ));
        }

        let base_ref = format!("refs/quorum-pr/{pr}");
        let wt_path = worktree_dir.to_path_buf();

        if self.branch_exists_unlocked(repo_dir, branch).await {
            if let Some(existing_wt) = self
                .find_worktree_for_branch_unlocked(repo_dir, branch)
                .await
            {
                return Err(format!(
                    "branch collision: '{branch}' already checked out in worktree '{existing_wt}'"
                ));
            }
            let mut del_cmd = self.git_cmd(repo_dir);
            del_cmd.args(["branch", "-D", branch]);
            let _ = run_git(del_cmd, self.local_timeout, "git branch -D (stale)").await;
        }

        let mut add_cmd = self.git_cmd(repo_dir);
        add_cmd.args(["worktree", "add", "-b", branch]);
        add_cmd.arg(&wt_path).arg(&base_ref);
        let add = run_git(add_cmd, self.local_timeout, "git worktree add").await?;
        if !add.status.success() {
            return Err(format!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&add.stderr)
            ));
        }

        Ok(wt_path)
    }

    /// Verify that a worktree HEAD matches an expected SHA. Does not take the
    /// serialization lock — this is a read-only check on the worktree dir.
    pub async fn verify_head_sha(
        &self,
        worktree_dir: &Path,
        expected_sha: &str,
    ) -> Result<(), String> {
        let mut cmd = Command::new(&self.git_bin);
        cmd.arg("-C").arg(worktree_dir).args(["rev-parse", "HEAD"]);
        let out = run_git(cmd, self.local_timeout, "git rev-parse HEAD").await?;
        if !out.status.success() {
            return Err(format!(
                "git rev-parse HEAD failed in {}: {}",
                worktree_dir.display(),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        let actual = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if actual != expected_sha {
            return Err(format!(
                "worktree HEAD SHA mismatch: expected {expected_sha}, got {actual}"
            ));
        }
        Ok(())
    }

    pub async fn gc_orphaned(
        &self,
        repo_dir: &Path,
        worktree_base: &Path,
        active_worktrees: &[&str],
    ) -> Vec<String> {
        let _guard = self.lock.lock().await;

        let mut prune_cmd = self.git_cmd(repo_dir);
        prune_cmd.args(["worktree", "prune"]);
        if let Err(e) = run_git(prune_cmd, self.local_timeout, "git worktree prune").await {
            eprintln!("warn: {e}");
        }

        let entries = match std::fs::read_dir(worktree_base) {
            Ok(e) => e,
            Err(_) => return Vec::new(),
        };

        let mut removed = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let path_str = path.to_string_lossy().to_string();
            if active_worktrees.iter().any(|a| *a == path_str) {
                continue;
            }

            let mut rm_cmd = self.git_cmd(repo_dir);
            rm_cmd.args(["worktree", "remove", &path_str, "--force"]);
            let git_ok = match run_git(rm_cmd, self.local_timeout, "git worktree remove").await {
                Ok(out) if out.status.success() => true,
                Ok(_) => false,
                Err(e) => {
                    eprintln!("warn: {e}");
                    false
                }
            };

            if git_ok || std::fs::remove_dir_all(&path).is_ok() {
                removed.push(path_str);
            }
        }
        removed
    }

    pub async fn remove(&self, repo_dir: &Path, worktree_dir: &Path) -> Result<(), String> {
        let _guard = self.lock.lock().await;

        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["worktree", "remove"]);
        cmd.arg(worktree_dir).arg("--force");
        let rm = run_git(cmd, self.local_timeout, "git worktree remove").await?;

        if !rm.status.success() {
            return Err(format!(
                "git worktree remove failed: {}",
                String::from_utf8_lossy(&rm.stderr)
            ));
        }

        Ok(())
    }

    /// Delete a local branch. Best-effort: logs but does not propagate errors
    /// (the branch may not exist if the worktree was never fully provisioned).
    pub async fn delete_branch(&self, repo_dir: &Path, branch: &str) {
        let _guard = self.lock.lock().await;

        let mut cmd = self.git_cmd(repo_dir);
        cmd.args(["branch", "-D", branch]);
        match run_git(cmd, self.local_timeout, "git branch -D").await {
            Ok(o) if o.status.success() => {}
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                if !stderr.contains("not found") {
                    eprintln!("warn: git branch -D {branch} failed: {stderr}");
                }
            }
            Err(e) => {
                eprintln!("warn: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;
    use std::sync::Arc;

    fn init_git_repo(dir: &Path) {
        let d = dir.to_string_lossy();
        let init = StdCommand::new("git")
            .args(["-C", &d, "init", "-b", "main"])
            .output()
            .unwrap();
        assert!(init.status.success(), "git init failed");
        StdCommand::new("git")
            .args(["-C", &d, "config", "user.email", "test@test.com"])
            .output()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "config", "user.name", "Test"])
            .output()
            .unwrap();
        let commit = StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "init"])
            .output()
            .unwrap();
        assert!(
            commit.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&commit.stderr)
        );
    }

    #[cfg(unix)]
    fn create_hanging_shim(dir: &Path) -> PathBuf {
        let shim = dir.join("git-hang");
        std::fs::write(&shim, "#!/bin/sh\nexec sleep 3600\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
        shim
    }

    #[cfg(unix)]
    fn short_timeouts() -> (Duration, Duration) {
        (Duration::from_millis(300), Duration::from_millis(300))
    }

    #[tokio::test]
    async fn provision_and_remove() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("test-wt");

        let mgr = WorktreeManager::new();
        let result = mgr
            .provision(repo_dir.path(), "test-branch", &wt_path, "main")
            .await;
        assert!(result.is_ok(), "provision failed: {:?}", result.err());
        assert!(wt_path.exists());

        let rm_result = mgr.remove(repo_dir.path(), &wt_path).await;
        assert!(rm_result.is_ok(), "remove failed: {:?}", rm_result.err());
        assert!(!wt_path.exists());
    }

    fn git_rev_parse(dir: &Path, rev: &str) -> String {
        let out = StdCommand::new("git")
            .args(["-C", &dir.to_string_lossy(), "rev-parse", rev])
            .output()
            .unwrap();
        assert!(out.status.success(), "rev-parse {rev} failed");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[tokio::test]
    async fn fetch_and_provision_uses_branch_head_not_main() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let d = repo_dir.path().to_string_lossy().to_string();

        // Add origin pointing to self (mirrors integration test setup)
        StdCommand::new("git")
            .args(["-C", &d, "remote", "add", "origin", &d])
            .status()
            .unwrap();

        // Create a feature branch with an extra commit ahead of main
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", "feature/test-pr"])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "pr commit"])
            .status()
            .unwrap();

        let feature_head = git_rev_parse(repo_dir.path(), "feature/test-pr");
        let main_head = git_rev_parse(repo_dir.path(), "main");
        assert_ne!(feature_head, main_head, "feature must be ahead of main");

        // Switch back to main so the worktree can check out the feature branch
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("reviewer-wt");

        let mgr = WorktreeManager::new();
        let result = mgr
            .fetch_and_provision(
                repo_dir.path(),
                "review/pr-1-TestReviewer",
                &wt_path,
                "feature/test-pr",
            )
            .await;
        assert!(
            result.is_ok(),
            "fetch_and_provision failed: {:?}",
            result.err()
        );

        let wt_head = git_rev_parse(&wt_path, "HEAD");
        assert_eq!(
            wt_head, feature_head,
            "reviewer worktree HEAD should be at the PR branch head, not main"
        );

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }

    #[tokio::test]
    async fn provision_remove_delete_branch_then_reprovision() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("worker-wt");
        let branch = "daemon/bellows-t42";

        let mgr = WorktreeManager::new();

        // First cycle: provision, remove worktree, delete branch
        mgr.provision(repo_dir.path(), branch, &wt_path, "main")
            .await
            .expect("first provision should succeed");
        assert!(wt_path.exists());

        mgr.remove(repo_dir.path(), &wt_path)
            .await
            .expect("remove should succeed");
        mgr.delete_branch(repo_dir.path(), branch).await;
        assert!(!wt_path.exists());

        // Second cycle: same branch name must succeed
        let result = mgr
            .provision(repo_dir.path(), branch, &wt_path, "main")
            .await;
        assert!(
            result.is_ok(),
            "re-provision with same branch should succeed after delete_branch: {:?}",
            result.err()
        );
        assert!(wt_path.exists());

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
        mgr.delete_branch(repo_dir.path(), branch).await;
    }

    /// Recovery scenario: worktree removed but branch survives. provision()
    /// must reuse the existing branch and preserve its commits.
    #[tokio::test]
    async fn provision_reuses_existing_branch_preserving_commits() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("worker-wt");
        let branch = "daemon/alloy-t9";

        let mgr = WorktreeManager::new();

        // First cycle: provision, make a commit in the worktree, then remove
        // worktree WITHOUT deleting branch (simulates recovery GC).
        mgr.provision(repo_dir.path(), branch, &wt_path, "main")
            .await
            .expect("first provision");
        let d = wt_path.to_string_lossy().to_string();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "worker commit"])
            .status()
            .unwrap();
        let worker_head = git_rev_parse(&wt_path, "HEAD");

        mgr.remove(repo_dir.path(), &wt_path).await.unwrap();
        // Branch still exists (recovery doesn't delete branches)
        let branch_check = StdCommand::new("git")
            .args([
                "-C",
                &repo_dir.path().to_string_lossy(),
                "rev-parse",
                "--verify",
                &format!("refs/heads/{branch}"),
            ])
            .output()
            .unwrap();
        assert!(branch_check.status.success(), "branch must survive removal");

        // Second cycle: provision should reuse the existing branch
        let wt_path2 = wt_dir.path().join("worker-wt-2");
        let result = mgr
            .provision(repo_dir.path(), branch, &wt_path2, "main")
            .await;
        assert!(
            result.is_ok(),
            "provision with existing branch should succeed: {:?}",
            result.err()
        );

        // Worker commit must be preserved
        let head_after = git_rev_parse(&wt_path2, "HEAD");
        assert_eq!(
            head_after, worker_head,
            "reused branch must preserve the worker's commits"
        );

        mgr.remove(repo_dir.path(), &wt_path2).await.ok();
        mgr.delete_branch(repo_dir.path(), branch).await;
    }

    /// Collision: provision must fail when the branch is checked out in
    /// another worktree.
    #[tokio::test]
    async fn provision_detects_worktree_collision() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt1 = wt_dir.path().join("wt-1");
        let wt2 = wt_dir.path().join("wt-2");
        let branch = "daemon/collide-t1";

        let mgr = WorktreeManager::new();
        mgr.provision(repo_dir.path(), branch, &wt1, "main")
            .await
            .unwrap();

        let result = mgr.provision(repo_dir.path(), branch, &wt2, "main").await;
        assert!(result.is_err(), "should detect collision");
        let err = result.unwrap_err();
        assert!(
            err.contains("branch collision"),
            "error should mention collision, got: {err}"
        );

        mgr.remove(repo_dir.path(), &wt1).await.ok();
        mgr.delete_branch(repo_dir.path(), branch).await;
    }

    /// fetch_and_provision must handle a stale local branch left from a
    /// prior reviewer incarnation (delete + recreate at remote head).
    #[tokio::test]
    async fn fetch_and_provision_cleans_stale_branch() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());
        let d = repo_dir.path().to_string_lossy().to_string();

        StdCommand::new("git")
            .args(["-C", &d, "remote", "add", "origin", &d])
            .status()
            .unwrap();

        // Create a feature branch
        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", "feature/pr-branch"])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "pr work"])
            .status()
            .unwrap();
        let feature_head = git_rev_parse(repo_dir.path(), "feature/pr-branch");

        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();

        // Create a stale local review branch (simulates surviving branch)
        StdCommand::new("git")
            .args(["-C", &d, "branch", "review/pr-1-Rev0", "main"])
            .status()
            .unwrap();
        let stale_head = git_rev_parse(repo_dir.path(), "review/pr-1-Rev0");
        assert_ne!(stale_head, feature_head, "stale branch should differ");

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("reviewer-wt");

        let mgr = WorktreeManager::new();
        let result = mgr
            .fetch_and_provision(
                repo_dir.path(),
                "review/pr-1-Rev0",
                &wt_path,
                "feature/pr-branch",
            )
            .await;
        assert!(
            result.is_ok(),
            "fetch_and_provision with stale branch should succeed: {:?}",
            result.err()
        );

        // Must point at the remote head, not the stale branch
        let wt_head = git_rev_parse(&wt_path, "HEAD");
        assert_eq!(
            wt_head, feature_head,
            "reviewer worktree must be at remote head, not stale branch"
        );

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }

    #[tokio::test]
    async fn delete_branch_nonexistent_does_not_error() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let mgr = WorktreeManager::new();
        // Should not panic or error — best-effort
        mgr.delete_branch(repo_dir.path(), "no-such-branch").await;
    }

    #[tokio::test]
    async fn gc_removes_orphaned_worktrees() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_base = tempfile::tempdir().unwrap();
        let mgr = WorktreeManager::new();

        // Create two worktrees
        let wt1 = wt_base.path().join("active-wt");
        let wt2 = wt_base.path().join("orphan-wt");
        mgr.provision(repo_dir.path(), "branch-active", &wt1, "main")
            .await
            .unwrap();
        mgr.provision(repo_dir.path(), "branch-orphan", &wt2, "main")
            .await
            .unwrap();
        assert!(wt1.exists());
        assert!(wt2.exists());

        // GC with only wt1 as active — wt2 should be removed
        let active = [wt1.to_string_lossy().to_string()];
        let active_refs: Vec<&str> = active.iter().map(|s| s.as_str()).collect();
        let removed = mgr
            .gc_orphaned(repo_dir.path(), wt_base.path(), &active_refs)
            .await;

        assert_eq!(removed.len(), 1);
        assert!(wt1.exists(), "active worktree should NOT be removed");
        assert!(!wt2.exists(), "orphaned worktree should be removed");

        // Clean up
        mgr.remove(repo_dir.path(), &wt1).await.ok();
    }

    // --- Timeout / reap tests (require Unix shims) ---

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_hanging_subprocess() {
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let mgr = WorktreeManager::with_config(shim, ft, lt);
        let tmp = tempfile::tempdir().unwrap();
        let wt = tmp.path().join("wt");

        let start = std::time::Instant::now();
        let result = mgr.provision(tmp.path(), "branch", &wt, "main").await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("timed out"),
            "expected timeout error, got: {err}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "should have timed out quickly, took {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_git_timeout_kills_and_returns() {
        // Directly test run_git: a `sleep 3600` must not block beyond the timeout.
        // If kill_on_drop failed, this call would hang for an hour.
        let mut cmd = Command::new("sleep");
        cmd.arg("3600");

        let start = std::time::Instant::now();
        let result = run_git(cmd, Duration::from_millis(300), "sleep").await;
        let elapsed = start.elapsed();

        assert!(result.unwrap_err().contains("timed out"));
        assert!(
            elapsed < Duration::from_secs(5),
            "run_git should return on timeout, took {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_releases_mutex_for_subsequent_operations() {
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let mgr = WorktreeManager::with_config(shim, ft, lt);
        let tmp = tempfile::tempdir().unwrap();

        // First call times out
        let r1 = mgr
            .provision(tmp.path(), "b1", &tmp.path().join("wt1"), "main")
            .await;
        assert!(r1.unwrap_err().contains("timed out"));

        // Second call also times out — proves mutex was released after first timeout
        let r2 = mgr
            .provision(tmp.path(), "b2", &tmp.path().join("wt2"), "main")
            .await;
        assert!(r2.unwrap_err().contains("timed out"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_concurrent_callers_progress() {
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let mgr = Arc::new(WorktreeManager::with_config(shim, ft, lt));
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().to_path_buf();

        let m1 = mgr.clone();
        let p1 = base.clone();
        let t1 =
            tokio::spawn(async move { m1.provision(&p1, "b1", &p1.join("wt1"), "main").await });

        let m2 = mgr.clone();
        let p2 = base.clone();
        let t2 =
            tokio::spawn(async move { m2.provision(&p2, "b2", &p2.join("wt2"), "main").await });

        let (r1, r2) = tokio::join!(t1, t2);
        assert!(r1.unwrap().unwrap_err().contains("timed out"));
        assert!(r2.unwrap().unwrap_err().contains("timed out"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_no_orphan_worktree_corruption() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("test-wt");

        // Provision with real git
        let real_mgr = WorktreeManager::new();
        real_mgr
            .provision(repo_dir.path(), "test-branch", &wt_path, "main")
            .await
            .unwrap();
        assert!(wt_path.exists());

        // Try to remove with hanging shim — will timeout
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let hang_mgr = WorktreeManager::with_config(shim, ft, lt);
        let result = hang_mgr.remove(repo_dir.path(), &wt_path).await;
        assert!(result.unwrap_err().contains("timed out"));

        // Worktree still intact — no corruption from killed subprocess
        assert!(wt_path.exists());

        // Real git can still clean up successfully
        real_mgr.remove(repo_dir.path(), &wt_path).await.unwrap();
        assert!(!wt_path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn gc_orphaned_timeout_falls_through_to_fs_cleanup() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_base = tempfile::tempdir().unwrap();
        let orphan = wt_base.path().join("orphan-wt");
        std::fs::create_dir(&orphan).unwrap();

        // Hanging shim — git commands timeout, but fs fallback still works
        let shim_dir = tempfile::tempdir().unwrap();
        let shim = create_hanging_shim(shim_dir.path());
        let (ft, lt) = short_timeouts();
        let mgr = WorktreeManager::with_config(shim, ft, lt);

        let removed = mgr.gc_orphaned(repo_dir.path(), wt_base.path(), &[]).await;
        assert!(
            removed.contains(&orphan.to_string_lossy().to_string()),
            "orphan should be cleaned up via fs fallback"
        );
        assert!(!orphan.exists());
    }

    #[tokio::test]
    async fn verify_head_sha_matches() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("verify-wt");

        let mgr = WorktreeManager::new();
        mgr.provision(repo_dir.path(), "verify-branch", &wt_path, "main")
            .await
            .unwrap();

        let expected_sha = git_rev_parse(&wt_path, "HEAD");
        assert!(mgr.verify_head_sha(&wt_path, &expected_sha).await.is_ok());

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }

    #[tokio::test]
    async fn verify_head_sha_mismatch_fails() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("mismatch-wt");

        let mgr = WorktreeManager::new();
        mgr.provision(repo_dir.path(), "mismatch-branch", &wt_path, "main")
            .await
            .unwrap();

        let result = mgr
            .verify_head_sha(&wt_path, "0000000000000000000000000000000000000000")
            .await;
        assert!(result.is_err(), "must fail on SHA mismatch");
        let err = result.unwrap_err();
        assert!(
            err.contains("mismatch"),
            "error should mention mismatch: {err}"
        );

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }

    #[tokio::test]
    async fn fetch_pr_and_provision_same_repo() {
        let repo_dir = tempfile::tempdir().unwrap();
        init_git_repo(repo_dir.path());
        let d = repo_dir.path().to_string_lossy().to_string();

        StdCommand::new("git")
            .args(["-C", &d, "remote", "add", "origin", &d])
            .status()
            .unwrap();

        StdCommand::new("git")
            .args(["-C", &d, "checkout", "-b", "feature/pr-42"])
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "pr commit"])
            .status()
            .unwrap();
        let feature_head = git_rev_parse(repo_dir.path(), "feature/pr-42");

        // Simulate GitHub's refs/pull/<pr>/head by creating the ref manually
        StdCommand::new("git")
            .args(["-C", &d, "update-ref", "refs/pull/42/head", &feature_head])
            .status()
            .unwrap();

        StdCommand::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();

        let wt_dir = tempfile::tempdir().unwrap();
        let wt_path = wt_dir.path().join("pr-review-wt");

        let mgr = WorktreeManager::new();
        let result = mgr
            .fetch_pr_and_provision(repo_dir.path(), "review/pr-42-test", &wt_path, 42)
            .await;
        assert!(
            result.is_ok(),
            "fetch_pr_and_provision failed: {:?}",
            result.err()
        );

        let wt_head = git_rev_parse(&wt_path, "HEAD");
        assert_eq!(wt_head, feature_head, "worktree must be at PR head");

        mgr.remove(repo_dir.path(), &wt_path).await.ok();
    }
}
