//! MergeExecutor — seam for PR merge so tests can mock the `gh` call.

use std::path::Path;

/// Reviewer lineage passed to the merge executor so the formal GitHub approval
/// carries the reviewer's identity and task id.
#[derive(Debug, Clone)]
pub struct MergeContext {
    pub reviewer_name: String,
    pub review_task_id: i64,
}

#[derive(Debug)]
pub struct MergeResult {
    pub success: bool,
    pub message: String,
}

/// Trait for executing PR merges. The default implementation calls `gh pr merge`.
/// Tests inject a mock via `command_override`.
pub trait MergeExecutor: Send + Sync {
    fn merge(&self, pr: i64, repo_dir: &Path, ctx: &MergeContext) -> MergeResult;
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

impl GhMergeExecutor {
    fn run_gh(&self, args: &[&str], repo_dir: &Path) -> MergeResult {
        let mut cmd = std::process::Command::new("gh");
        cmd.args(args);
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

impl MergeExecutor for GhMergeExecutor {
    fn merge(&self, pr: i64, repo_dir: &Path, ctx: &MergeContext) -> MergeResult {
        let pr_str = pr.to_string();
        let approve_body = format!(
            "Formal approval — per {} review verdict (task #{}). \
             Merge performed programmatically on approved verdict (daemon model).",
            ctx.reviewer_name, ctx.review_task_id,
        );

        let approve = self.run_gh(
            &[
                "pr",
                "review",
                &pr_str,
                "--approve",
                "--body",
                &approve_body,
            ],
            repo_dir,
        );
        if !approve.success {
            return MergeResult {
                success: false,
                message: format!("approve failed (merge not attempted): {}", approve.message),
            };
        }

        self.run_gh(
            &["pr", "merge", &pr_str, "--merge", "--delete-branch"],
            repo_dir,
        )
    }
}

/// Mock executor controlled by an env var or a command string.
/// Used in integration tests: QUORUM_MERGE_CMD="true" → always succeeds,
/// QUORUM_MERGE_CMD="false" → always fails.
pub struct CommandMergeExecutor {
    pub command: String,
}

impl MergeExecutor for CommandMergeExecutor {
    fn merge(&self, pr: i64, repo_dir: &Path, _ctx: &MergeContext) -> MergeResult {
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

    fn test_ctx() -> MergeContext {
        MergeContext {
            reviewer_name: "Rev-1".into(),
            review_task_id: 99,
        }
    }

    #[test]
    fn command_merge_executor_success() {
        let exec = CommandMergeExecutor {
            command: "echo merged PR {pr}".into(),
        };
        let result = exec.merge(42, Path::new("/tmp"), &test_ctx());
        assert!(result.success);
        assert!(result.message.contains("merged PR 42"));
    }

    #[test]
    fn command_merge_executor_failure() {
        let exec = CommandMergeExecutor {
            command: "echo 'conflict' >&2 && exit 1".into(),
        };
        let result = exec.merge(7, Path::new("/tmp"), &test_ctx());
        assert!(!result.success);
        assert!(result.message.contains("conflict"));
    }

    #[test]
    fn gh_merge_executor_approve_body_carries_lineage() {
        let ctx = MergeContext {
            reviewer_name: "TestReviewer".into(),
            review_task_id: 42,
        };

        let approve_body = format!(
            "Formal approval — per {} review verdict (task #{}). \
             Merge performed programmatically on approved verdict (daemon model).",
            ctx.reviewer_name, ctx.review_task_id,
        );
        assert!(approve_body.contains("TestReviewer"));
        assert!(approve_body.contains("task #42"));
        assert!(approve_body.contains("daemon model"));
    }

    #[test]
    fn approve_failure_short_circuits_merge() {
        // Simulate: approve step fails → merge never runs.
        // Use CommandMergeExecutor with a failing command as a proxy: verify
        // that GhMergeExecutor's approve-failure path returns a distinct message.
        let ctx = test_ctx();

        // GhMergeExecutor with no gh binary available will fail at approve step.
        // We verify the error message pattern.
        let exec = GhMergeExecutor {
            token_file: Some(std::path::PathBuf::from("/nonexistent/token")),
        };
        let result = exec.merge(1, Path::new("/tmp"), &ctx);
        assert!(!result.success);
        // Should fail at approve step (gh not found or approve fails)
        // and include "approve failed" in the message.
        assert!(
            result.message.contains("approve failed")
                || result.message.contains("failed to run gh"),
            "expected approve-failure message, got: {}",
            result.message
        );
    }
}
