//! MergeExecutor — seam for PR merge so tests can mock the `gh` call.

use std::path::Path;
use std::process::Command;

/// Reviewer lineage passed to the merge executor so the formal GitHub approval
/// carries the reviewer's identity and task id.
#[derive(Debug, Clone)]
pub struct MergeContext {
    pub reviewer_name: String,
    pub review_task_id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeFailureKind {
    /// Worker can fix (merge conflict, branch behind base).
    Retryable,
    /// Not worker-fixable (policy blocked, auth failure, infra).
    PolicyBlocked,
}

pub fn classify_merge_failure(message: &str) -> MergeFailureKind {
    let lower = message.to_lowercase();
    let retryable_patterns = [
        "merge conflict",
        "head branch was modified",
        "branch is behind",
        "not up to date",
        "out of date",
    ];
    for pat in &retryable_patterns {
        if lower.contains(pat) {
            return MergeFailureKind::Retryable;
        }
    }
    MergeFailureKind::PolicyBlocked
}

#[derive(Debug)]
pub struct MergeResult {
    pub success: bool,
    pub message: String,
    pub failure_kind: Option<MergeFailureKind>,
}

/// Trait for executing PR merges. The default implementation posts a formal
/// GitHub approval review then calls `gh pr merge`.
/// Tests inject a mock via `command_override`.
pub trait MergeExecutor: Send + Sync {
    fn merge(&self, pr: i64, repo_dir: &Path, ctx: &MergeContext) -> MergeResult;
}

fn gh_pr_state_is_merged(json_output: &str) -> bool {
    json_output.contains("\"MERGED\"")
}

/// Production executor: posts a formal GitHub approval review, then runs
/// `gh pr merge <pr> --merge --delete-branch`. If `token_file` is set,
/// reads the token at call time and passes it via `GH_TOKEN` env var.
/// The token is never exposed to agent processes.
pub struct GhMergeExecutor {
    pub token_file: Option<std::path::PathBuf>,
    /// `owner/repo` slug for `-R` flag — avoids cwd git dependency.
    pub gh_repo: Option<String>,
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
    fn build_gh_cmd(&self, args: &[&str], repo_dir: &Path) -> Command {
        let mut cmd = Command::new("gh");
        cmd.args(args);
        if let Some(ref nwo) = self.gh_repo {
            cmd.args(["-R", nwo]);
        } else {
            cmd.current_dir(repo_dir);
        }
        if let Some(token) = self.read_token() {
            cmd.env("GH_TOKEN", token);
        }
        cmd
    }

    fn run_gh(&self, args: &[&str], repo_dir: &Path) -> MergeResult {
        let mut cmd = self.build_gh_cmd(args, repo_dir);

        match cmd.output() {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                let message = if output.status.success() {
                    stdout
                } else {
                    stderr
                };
                let trimmed = message.trim().to_string();
                let failure_kind = if output.status.success() {
                    None
                } else {
                    Some(classify_merge_failure(&trimmed))
                };
                MergeResult {
                    success: output.status.success(),
                    message: trimmed,
                    failure_kind,
                }
            }
            Err(e) => MergeResult {
                success: false,
                message: format!("failed to run gh: {e}"),
                failure_kind: Some(MergeFailureKind::PolicyBlocked),
            },
        }
    }

    fn verify_pr_merged(&self, pr: i64, repo_dir: &Path) -> bool {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(&["pr", "view", &pr_str, "--json", "state"], repo_dir);
        let output = match cmd.output() {
            Ok(o) if o.status.success() => o,
            _ => return false,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        gh_pr_state_is_merged(&stdout)
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
                failure_kind: Some(MergeFailureKind::PolicyBlocked),
            };
        }

        let result = self.run_gh(
            &["pr", "merge", &pr_str, "--merge", "--delete-branch"],
            repo_dir,
        );

        if !result.success && self.verify_pr_merged(pr, repo_dir) {
            return MergeResult {
                success: true,
                message: format!(
                    "merge succeeded (post-merge cleanup failed: {})",
                    result.message,
                ),
                failure_kind: None,
            };
        }

        result
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
                let message = if out.status.success() {
                    stdout.trim().to_string()
                } else {
                    stderr.trim().to_string()
                };
                let failure_kind = if out.status.success() {
                    None
                } else {
                    Some(classify_merge_failure(&message))
                };
                MergeResult {
                    success: out.status.success(),
                    message,
                    failure_kind,
                }
            }
            Err(e) => MergeResult {
                success: false,
                message: format!("merge command failed: {e}"),
                failure_kind: Some(MergeFailureKind::PolicyBlocked),
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
        assert!(result.failure_kind.is_none());
        assert!(result.message.contains("merged PR 42"));
    }

    #[test]
    fn command_merge_executor_failure() {
        let exec = CommandMergeExecutor {
            command: "echo 'conflict' >&2 && exit 1".into(),
        };
        let result = exec.merge(7, Path::new("/tmp"), &test_ctx());
        assert!(!result.success);
        assert!(result.failure_kind.is_some());
        assert!(result.message.contains("conflict"));
    }

    #[test]
    fn command_merge_executor_retryable_conflict() {
        let exec = CommandMergeExecutor {
            command: "echo 'merge conflict on file.rs' >&2 && exit 1".into(),
        };
        let result = exec.merge(7, Path::new("/tmp"), &test_ctx());
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::Retryable));
    }

    #[test]
    fn command_merge_executor_policy_blocked() {
        let exec = CommandMergeExecutor {
            command:
                "echo 'not mergeable: the base branch policy prohibits the merge' >&2 && exit 1"
                    .into(),
        };
        let result = exec.merge(7, Path::new("/tmp"), &test_ctx());
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
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
        let ctx = test_ctx();
        let exec = GhMergeExecutor {
            token_file: Some(std::path::PathBuf::from("/nonexistent/token")),
            gh_repo: None,
        };
        let result = exec.merge(1, Path::new("/tmp"), &ctx);
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
        assert!(
            result.message.contains("approve failed")
                || result.message.contains("failed to run gh"),
            "expected approve-failure message, got: {}",
            result.message
        );
    }

    #[test]
    fn classify_retryable_merge_conflict() {
        assert_eq!(
            classify_merge_failure("Merge conflict in src/main.rs"),
            MergeFailureKind::Retryable,
        );
    }

    #[test]
    fn classify_retryable_branch_behind() {
        assert_eq!(
            classify_merge_failure("the branch is behind the base branch"),
            MergeFailureKind::Retryable,
        );
    }

    #[test]
    fn classify_retryable_head_modified() {
        assert_eq!(
            classify_merge_failure("Head branch was modified. Review and try the merge again."),
            MergeFailureKind::Retryable,
        );
    }

    #[test]
    fn classify_retryable_out_of_date() {
        assert_eq!(
            classify_merge_failure("This branch is out of date with the base branch"),
            MergeFailureKind::Retryable,
        );
    }

    #[test]
    fn classify_policy_blocked() {
        assert_eq!(
            classify_merge_failure("not mergeable: the base branch policy prohibits the merge"),
            MergeFailureKind::PolicyBlocked,
        );
    }

    #[test]
    fn classify_policy_unknown_error() {
        assert_eq!(
            classify_merge_failure("some unknown gh error"),
            MergeFailureKind::PolicyBlocked,
        );
    }

    #[test]
    fn classify_policy_failed_to_run() {
        assert_eq!(
            classify_merge_failure("failed to run gh: No such file or directory"),
            MergeFailureKind::PolicyBlocked,
        );
    }

    #[test]
    fn gh_pr_state_detects_merged() {
        assert!(gh_pr_state_is_merged(r#"{"state":"MERGED"}"#));
        assert!(gh_pr_state_is_merged(
            r#"{"state":"MERGED","mergedAt":"2026-07-03T12:00:00Z"}"#
        ));
    }

    #[test]
    fn gh_pr_state_rejects_open() {
        assert!(!gh_pr_state_is_merged(r#"{"state":"OPEN"}"#));
        assert!(!gh_pr_state_is_merged(r#"{"state":"CLOSED"}"#));
    }

    #[test]
    fn gh_pr_state_rejects_empty() {
        assert!(!gh_pr_state_is_merged(""));
        assert!(!gh_pr_state_is_merged("{}"));
    }

    /// Simulates the #156 scenario: merge exit non-zero due to cleanup failure,
    /// but PR is actually MERGED. A mock executor that checks post-merge state
    /// should report success.
    struct VerifyingMockExecutor {
        merge_result: MergeResult,
        pr_state_json: String,
    }

    impl MergeExecutor for VerifyingMockExecutor {
        fn merge(&self, _pr: i64, _repo_dir: &Path, _ctx: &MergeContext) -> MergeResult {
            if !self.merge_result.success && gh_pr_state_is_merged(&self.pr_state_json) {
                return MergeResult {
                    success: true,
                    message: format!(
                        "merge succeeded (post-merge cleanup failed: {})",
                        self.merge_result.message,
                    ),
                    failure_kind: None,
                };
            }
            MergeResult {
                success: self.merge_result.success,
                message: self.merge_result.message.clone(),
                failure_kind: self.merge_result.failure_kind,
            }
        }
    }

    #[test]
    fn merge_failure_with_merged_state_reports_success() {
        let exec = VerifyingMockExecutor {
            merge_result: MergeResult {
                success: false,
                message: "could not determine current branch: failed to run git: not on any branch"
                    .into(),
                failure_kind: Some(MergeFailureKind::PolicyBlocked),
            },
            pr_state_json: r#"{"state":"MERGED"}"#.into(),
        };
        let result = exec.merge(155, Path::new("/tmp"), &test_ctx());
        assert!(result.success);
        assert!(result.failure_kind.is_none());
        assert!(result.message.contains("merge succeeded"));
        assert!(result.message.contains("post-merge cleanup failed"));
    }

    #[test]
    fn merge_failure_with_open_state_stays_failure() {
        let exec = VerifyingMockExecutor {
            merge_result: MergeResult {
                success: false,
                message: "not mergeable: the base branch policy prohibits the merge".into(),
                failure_kind: Some(MergeFailureKind::PolicyBlocked),
            },
            pr_state_json: r#"{"state":"OPEN"}"#.into(),
        };
        let result = exec.merge(155, Path::new("/tmp"), &test_ctx());
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
    }

    #[test]
    fn merge_success_not_rechecked() {
        let exec = VerifyingMockExecutor {
            merge_result: MergeResult {
                success: true,
                message: "merged".into(),
                failure_kind: None,
            },
            pr_state_json: r#"{"state":"OPEN"}"#.into(),
        };
        let result = exec.merge(155, Path::new("/tmp"), &test_ctx());
        assert!(result.success);
    }
}
