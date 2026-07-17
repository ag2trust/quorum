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

    pub async fn provision(
        &self,
        repo_dir: &Path,
        branch: &str,
        worktree_dir: &Path,
        base_ref: &str,
    ) -> Result<PathBuf, String> {
        let _guard = self.lock.lock().await;

        let wt_path = worktree_dir.to_path_buf();
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
}
