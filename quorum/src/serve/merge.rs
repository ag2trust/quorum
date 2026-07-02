//! MergeExecutor — seam for PR merge so tests can mock the `gh` call.

use std::path::Path;

#[derive(Debug)]
pub struct MergeResult {
    pub success: bool,
    pub message: String,
}

/// Trait for executing PR merges. The default implementation calls `gh pr merge`.
/// Tests inject a mock via `command_override`.
pub trait MergeExecutor: Send + Sync {
    fn merge(&self, pr: i64, repo_dir: &Path) -> MergeResult;
}

/// Production executor: runs `gh pr merge <pr> --merge --delete-branch`.
/// If `token_file` is set, reads the token at call time and passes it via
/// `GH_TOKEN` env var. The token is never exposed to agent processes.
pub struct GhMergeExecutor {
    pub token_file: Option<std::path::PathBuf>,
}

impl GhMergeExecutor {
    fn read_token(&self) -> Option<String> {
        self.token_file.as_ref().and_then(|p| {
            std::fs::read_to_string(p)
                .ok()
                .map(|s| s.trim().to_string())
        })
    }
}

impl MergeExecutor for GhMergeExecutor {
    fn merge(&self, pr: i64, repo_dir: &Path) -> MergeResult {
        let mut cmd = std::process::Command::new("gh");
        cmd.args(["pr", "merge", &pr.to_string(), "--merge", "--delete-branch"]);
        cmd.current_dir(repo_dir);

        if let Some(token) = self.read_token() {
            cmd.env("GH_TOKEN", token);
        }

        match cmd.output() {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                let message = if output.status.success() {
                    stdout
                } else {
                    stderr
                };
                MergeResult {
                    success: output.status.success(),
                    message: message.trim().to_string(),
                }
            }
            Err(e) => MergeResult {
                success: false,
                message: format!("failed to run gh: {e}"),
            },
        }
    }
}

/// Mock executor controlled by an env var or a command string.
/// Used in integration tests: QUORUM_MERGE_CMD="true" → always succeeds,
/// QUORUM_MERGE_CMD="false" → always fails.
pub struct CommandMergeExecutor {
    pub command: String,
}

impl MergeExecutor for CommandMergeExecutor {
    fn merge(&self, pr: i64, repo_dir: &Path) -> MergeResult {
        let expanded = self.command.replace("{pr}", &pr.to_string());
        let output = std::process::Command::new("sh")
            .args(["-c", &expanded])
            .current_dir(repo_dir)
            .output();

        match output {
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr).to_string();
                let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                MergeResult {
                    success: out.status.success(),
                    message: if out.status.success() {
                        stdout.trim().to_string()
                    } else {
                        stderr.trim().to_string()
                    },
                }
            }
            Err(e) => MergeResult {
                success: false,
                message: format!("merge command failed: {e}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_merge_executor_success() {
        let exec = CommandMergeExecutor {
            command: "echo merged PR {pr}".into(),
        };
        let result = exec.merge(42, Path::new("/tmp"));
        assert!(result.success);
        assert!(result.message.contains("merged PR 42"));
    }

    #[test]
    fn command_merge_executor_failure() {
        let exec = CommandMergeExecutor {
            command: "echo 'conflict' >&2 && exit 1".into(),
        };
        let result = exec.merge(7, Path::new("/tmp"));
        assert!(!result.success);
        assert!(result.message.contains("conflict"));
    }
}
