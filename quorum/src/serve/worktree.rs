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
}
