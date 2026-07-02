//! Serialized git worktree operations for agent isolation.

use std::path::{Path, PathBuf};
use tokio::process::Command;
use tokio::sync::Mutex;

pub struct WorktreeManager {
    lock: Mutex<()>,
}

impl WorktreeManager {
    pub fn new() -> Self {
        Self {
            lock: Mutex::new(()),
        }
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
        let add = Command::new("git")
            .args([
                "-C",
                &repo_dir.to_string_lossy(),
                "worktree",
                "add",
                "-b",
                branch,
                &wt_path.to_string_lossy(),
                base_ref,
            ])
            .output()
            .await
            .map_err(|e| format!("git worktree add failed: {e}"))?;

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

        let fetch = Command::new("git")
            .args([
                "-C",
                &repo_dir.to_string_lossy(),
                "fetch",
                "origin",
                remote_branch,
            ])
            .output()
            .await
            .map_err(|e| format!("git fetch failed: {e}"))?;

        if !fetch.status.success() {
            return Err(format!(
                "git fetch origin {remote_branch} failed: {}",
                String::from_utf8_lossy(&fetch.stderr)
            ));
        }

        let base_ref = format!("origin/{remote_branch}");
        let wt_path = worktree_dir.to_path_buf();
        let add = Command::new("git")
            .args([
                "-C",
                &repo_dir.to_string_lossy(),
                "worktree",
                "add",
                "-b",
                branch,
                &wt_path.to_string_lossy(),
                &base_ref,
            ])
            .output()
            .await
            .map_err(|e| format!("git worktree add failed: {e}"))?;

        if !add.status.success() {
            return Err(format!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&add.stderr)
            ));
        }

        Ok(wt_path)
    }

    pub async fn remove(&self, repo_dir: &Path, worktree_dir: &Path) -> Result<(), String> {
        let _guard = self.lock.lock().await;

        let rm = Command::new("git")
            .args([
                "-C",
                &repo_dir.to_string_lossy(),
                "worktree",
                "remove",
                &worktree_dir.to_string_lossy(),
                "--force",
            ])
            .output()
            .await
            .map_err(|e| format!("git worktree remove failed: {e}"))?;

        if !rm.status.success() {
            return Err(format!(
                "git worktree remove failed: {}",
                String::from_utf8_lossy(&rm.stderr)
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;

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
}
