//! MergeExecutor — seam for PR merge so tests can mock the `gh` call.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

/// Reviewer lineage passed to the merge executor so the formal GitHub approval
/// carries the reviewer's identity and task id.
#[derive(Debug, Clone)]
pub struct MergeContext {
    pub reviewer_name: String,
    pub review_task_id: i64,
    /// Immutable task target. The production executor revalidates the live
    /// PR base against this value before every approval/merge attempt.
    pub expected_base_branch: String,
    /// Exact immutable PR head approved by the managed review. The production
    /// executor revalidates this SHA before formal approval and asks GitHub to
    /// reject the merge if the head changes before the irreversible call.
    pub expected_head_sha: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeFailureKind {
    /// The PR head moved after managed approval. This is stale review
    /// authority, not a worker-fixable merge conflict.
    StaleAuthority,
    /// Worker can fix (merge conflict, branch behind base).
    Retryable,
    /// Merge prerequisites not yet met (checks/approval still propagating).
    /// Daemon should re-enter the wait+merge cycle, not cancel or rework.
    PolicyPending,
    /// Not worker-fixable (auth failure, infra, unknown).
    PolicyBlocked,
}

pub fn classify_merge_failure(message: &str) -> MergeFailureKind {
    let lower = message.to_lowercase();
    let stale_authority_patterns = [
        "head sha does not match",
        "head commit does not match",
        "head branch was modified",
        "does not match the expected head",
        "expected head commit",
    ];
    for pat in &stale_authority_patterns {
        if lower.contains(pat) {
            return MergeFailureKind::StaleAuthority;
        }
    }
    let retryable_patterns = [
        "merge conflict",
        "branch is behind",
        "not up to date",
        "out of date",
        "cannot be cleanly created",
    ];
    for pat in &retryable_patterns {
        if lower.contains(pat) {
            return MergeFailureKind::Retryable;
        }
    }
    if lower.contains("policy prohibits") {
        return MergeFailureKind::PolicyPending;
    }
    MergeFailureKind::PolicyBlocked
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeabilityState {
    /// PR can be merged (or state is unknown/not checked).
    Mergeable,
    /// PR has conflicts with the base branch.
    Conflicting,
    /// PR was already merged.
    AlreadyMerged,
    /// PR was closed without merging.
    Closed,
}

#[derive(Debug)]
pub struct MergeResult {
    pub success: bool,
    pub message: String,
    pub failure_kind: Option<MergeFailureKind>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChecksOutcome {
    /// All required checks passed — safe to merge.
    Ready,
    /// One or more checks failed — names of failing checks included.
    Failed { failing_checks: Vec<String> },
    /// Checks still running — retry later without entering rework.
    Pending { reason: String },
    /// Timed out waiting for checks to complete.
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequiredJobsOutcome {
    /// All required jobs completed with conclusion SUCCESS.
    AllSucceeded,
    /// Some required jobs are SKIPPED, absent, or have a non-SUCCESS conclusion.
    NotReady { issues: Vec<(String, String)> },
    /// Some required jobs haven't completed yet.
    Pending { pending_jobs: Vec<String> },
}

pub fn apply_required_jobs_gate(
    checks: ChecksOutcome,
    required_jobs: RequiredJobsOutcome,
) -> ChecksOutcome {
    if !matches!(checks, ChecksOutcome::Ready) {
        return checks;
    }
    match required_jobs {
        RequiredJobsOutcome::AllSucceeded => ChecksOutcome::Ready,
        RequiredJobsOutcome::NotReady { issues } => ChecksOutcome::Failed {
            failing_checks: issues
                .into_iter()
                .map(|(name, status)| format!("{name} ({status})"))
                .collect(),
        },
        RequiredJobsOutcome::Pending { pending_jobs } => ChecksOutcome::Pending {
            reason: format!("required jobs still pending: {}", pending_jobs.join(", ")),
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultBranchStatus {
    Green,
    Red { details: String },
    Pending,
    Unknown,
}

/// Trait for executing PR merges. The default implementation posts a formal
/// GitHub approval review (`gh pr review --approve`) then calls `gh pr merge`.
/// Tests inject a mock via [`CommandMergeExecutor::command`].
pub trait MergeExecutor: Send + Sync {
    fn merge(&self, pr: i64, repo_dir: &Path, ctx: &MergeContext) -> MergeResult;

    /// GitHub's immutable merge commit for a completed PR. The production
    /// implementation asks the PR API after merge; unavailable metadata is
    /// intentionally represented as `None` so callers can keep the lifecycle
    /// conservative rather than inventing a commit identity.
    fn merge_commit_sha(&self, _pr: i64, _repo_dir: &Path) -> Option<String> {
        None
    }

    fn wait_for_checks(
        &self,
        _pr: i64,
        _repo_dir: &Path,
        _timeout_secs: u64,
        _poll_interval_secs: u64,
    ) -> ChecksOutcome {
        ChecksOutcome::Ready
    }

    /// Names of checks that are still pending, used only to make a prolonged
    /// pre-review CI wait actionable for the owner. Implementations may return
    /// an empty list when the provider cannot determine them.
    fn pending_check_names(&self, _pr: i64, _repo_dir: &Path) -> Vec<String> {
        Vec::new()
    }

    /// Pre-merge mergeability check. Returns `Conflicting` if the PR has
    /// conflicts with the base branch, allowing the daemon to skip the
    /// approve+checks cycle and go straight to a rework round.
    fn check_mergeability(&self, _pr: i64, _repo_dir: &Path) -> MergeabilityState {
        MergeabilityState::Mergeable
    }

    /// Current head commit SHA of the PR's branch (`headRefOid`). Used by #228
    /// restart recovery to SHA-bind a durable approval to the exact diff the
    /// reviewer approved: a moved head auto-invalidates the approval. Returns
    /// `None` if the head can't be determined (e.g. PR gone / gh error) — the
    /// caller then leaves the record for a later startup rather than merging
    /// blind. Default `None` so mock executors opt in explicitly.
    fn head_sha(&self, _pr: i64, _repo_dir: &Path) -> Option<String> {
        None
    }

    /// Verify that each required job in the PR's check suite has conclusion
    /// SUCCESS (not SKIPPED, NEUTRAL, or absent). Default: gate off (all succeed).
    fn check_required_jobs(
        &self,
        _pr: i64,
        _repo_dir: &Path,
        _required_jobs: &[String],
    ) -> RequiredJobsOutcome {
        RequiredJobsOutcome::AllSucceeded
    }

    /// Check the CI status of the default branch's latest workflow run.
    /// Default: Green (gate off).
    fn check_default_branch_ci(&self, _repo_dir: &Path, _branch: &str) -> DefaultBranchStatus {
        DefaultBranchStatus::Green
    }

    /// #159: post or verify the daemon-owned REQUEST_CHANGES review carrying the
    /// blocking findings. Managed reviewer agents share the PR author's GitHub
    /// identity, so formal review state stays with the daemon's merge account.
    fn ensure_changes_requested(&self, _pr: i64, _repo_dir: &Path, _body: &str) {
        // Default no-op; real executor posts via `gh pr review --request-changes`.
    }
}

fn gh_pr_state_is_merged(json_output: &str) -> bool {
    json_output.contains("\"MERGED\"")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ChecksQueryResult {
    AllPassed,
    SomeFailed(Vec<String>),
    Pending,
    /// Valid empty statusCheckRollup — repo may have no CI, or checks
    /// may not have registered yet after a push. Caller must require
    /// consecutive polls before trusting this as Ready.
    NoChecksConfigured,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SingleCheckStatus {
    Passed,
    Failed,
    Pending,
}

/// A missing or JSON-null conclusion is normalized to the empty conclusion
/// used by the check parser. Only an explicitly completed check with no
/// conclusion fails closed; all other empty-conclusion checks are pending.
fn is_pending_check(status: &str, conclusion: &str) -> bool {
    conclusion.is_empty() && status != "COMPLETED"
}

/// Returns `(mergeStateStatus, checks)` where `checks` is:
/// - `Some(vec![...])` when `statusCheckRollup` is a valid JSON array (possibly empty),
/// - `None` when the field is missing, not an array, or JSON is malformed.
fn parse_checks_json(json_str: &str) -> (Option<String>, Option<Vec<(String, SingleCheckStatus)>>) {
    let val: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return (None, None),
    };

    let merge_state = val
        .get("mergeStateStatus")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let checks = val
        .get("statusCheckRollup")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|entry| {
                    let name = entry
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let conclusion = entry
                        .get("conclusion")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let status = entry.get("status").and_then(|v| v.as_str()).unwrap_or("");

                    // GitHub only supplies a conclusion for a terminal check-run, but its
                    // status field can lag behind. Treat a non-empty conclusion as
                    // authoritative so a phantom IN_PROGRESS/SUCCESS run cannot hold a
                    // gate open forever. An empty conclusion remains pending unless the
                    // status is explicitly completed, in which case it is fail-closed.
                    let check_status = match conclusion {
                        "SUCCESS" | "NEUTRAL" | "SKIPPED" => SingleCheckStatus::Passed,
                        _ if is_pending_check(status, conclusion) => SingleCheckStatus::Pending,
                        _ => SingleCheckStatus::Failed,
                    };

                    (name, check_status)
                })
                .collect()
        });

    (merge_state, checks)
}

fn pending_check_names_from_json(json_str: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str) else {
        return Vec::new();
    };
    let Some(checks) = value
        .get("statusCheckRollup")
        .and_then(|value| value.as_array())
    else {
        return Vec::new();
    };
    checks
        .iter()
        .filter(|check| {
            let conclusion = check
                .get("conclusion")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let status = check
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            is_pending_check(status, conclusion)
        })
        .filter_map(|check| check.get("name").and_then(|value| value.as_str()))
        .map(str::to_owned)
        .collect()
}

fn checks_query_from_parsed(
    merge_state: &Option<String>,
    checks: &Option<Vec<(String, SingleCheckStatus)>>,
) -> ChecksQueryResult {
    if merge_state.as_deref() == Some("CLEAN") {
        return ChecksQueryResult::AllPassed;
    }

    let Some(checks) = checks else {
        // Field absent or not an array — unknown state, wait.
        return ChecksQueryResult::Pending;
    };

    if checks.is_empty() {
        // Valid empty array — might be no CI, or checks haven't registered
        // yet after a push. Caller must poll again to disambiguate.
        return ChecksQueryResult::NoChecksConfigured;
    }

    let mut failing = Vec::new();
    let mut any_pending = false;

    for (name, status) in checks {
        match status {
            SingleCheckStatus::Failed => failing.push(name.clone()),
            SingleCheckStatus::Pending => any_pending = true,
            SingleCheckStatus::Passed => {}
        }
    }

    if !failing.is_empty() {
        return ChecksQueryResult::SomeFailed(failing);
    }

    if any_pending {
        return ChecksQueryResult::Pending;
    }

    ChecksQueryResult::AllPassed
}

/// Check whether each required job in the PR's `statusCheckRollup` has conclusion
/// SUCCESS specifically — SKIPPED and NEUTRAL do NOT satisfy a required job.
pub fn validate_required_jobs(checks_json: &str, required_jobs: &[String]) -> RequiredJobsOutcome {
    if required_jobs.is_empty() {
        return RequiredJobsOutcome::AllSucceeded;
    }

    let val: serde_json::Value = match serde_json::from_str(checks_json) {
        Ok(v) => v,
        Err(_) => {
            let issues = required_jobs
                .iter()
                .map(|j| (j.clone(), "unknown (parse error)".into()))
                .collect();
            return RequiredJobsOutcome::NotReady { issues };
        }
    };

    let checks: Vec<(&str, &str, &str)> = val
        .get("statusCheckRollup")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|entry| {
                    let name = entry.get("name")?.as_str()?;
                    let status = entry.get("status").and_then(|v| v.as_str()).unwrap_or("");
                    let conclusion = entry
                        .get("conclusion")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    Some((name, status, conclusion))
                })
                .collect()
        })
        .unwrap_or_default();

    let mut issues = Vec::new();
    let mut pending = Vec::new();

    for job in required_jobs {
        match checks.iter().find(|(name, _, _)| *name == job.as_str()) {
            None => issues.push((job.clone(), "absent".to_string())),
            Some((_, status, _)) if *status != "COMPLETED" => pending.push(job.clone()),
            Some((_, _, conclusion)) if *conclusion != "SUCCESS" => {
                issues.push((job.clone(), conclusion.to_string()));
            }
            Some(_) => {}
        }
    }

    if !pending.is_empty() {
        return RequiredJobsOutcome::Pending {
            pending_jobs: pending,
        };
    }
    if !issues.is_empty() {
        return RequiredJobsOutcome::NotReady { issues };
    }
    RequiredJobsOutcome::AllSucceeded
}

/// Parse the output of `gh run list --json conclusion,status` for the default
/// branch's latest CI run.
pub fn parse_default_branch_ci(json: &str) -> DefaultBranchStatus {
    let val: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return DefaultBranchStatus::Unknown,
    };

    let arr = match val.as_array() {
        Some(a) if !a.is_empty() => a,
        _ => return DefaultBranchStatus::Unknown,
    };

    let entry = &arr[0];
    let status = entry.get("status").and_then(|v| v.as_str()).unwrap_or("");
    let conclusion = entry
        .get("conclusion")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if status != "completed" {
        return DefaultBranchStatus::Pending;
    }

    match conclusion {
        "success" => DefaultBranchStatus::Green,
        other => DefaultBranchStatus::Red {
            details: format!("latest run conclusion: {other}"),
        },
    }
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
        self.token_file
            .as_ref()
            .and_then(|p| match std::fs::read_to_string(p) {
                Ok(s) => Some(s.trim().to_string()),
                Err(e) => {
                    eprintln!("warn: failed to read merge-token file {}: {e}", p.display());
                    None
                }
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

    fn live_base_branch(&self, pr: i64, repo_dir: &Path) -> std::result::Result<String, String> {
        let pr_str = pr.to_string();
        let mut cmd =
            self.build_gh_cmd(&["pr", "view", &pr_str, "--json", "baseRefName"], repo_dir);
        let output = cmd
            .output()
            .map_err(|error| format!("failed to run gh: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "gh pr view failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        parse_pr_base_branch(&output.stdout)
            .ok_or_else(|| "gh pr view returned no baseRefName".to_string())
    }

    fn live_head_sha(&self, pr: i64, repo_dir: &Path) -> std::result::Result<String, String> {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(&["pr", "view", &pr_str, "--json", "headRefOid"], repo_dir);
        let output = cmd
            .output()
            .map_err(|error| format!("failed to run gh: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "gh pr view failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        parse_pr_head_sha(&output.stdout)
            .ok_or_else(|| "gh pr view returned no headRefOid".to_string())
    }

    fn reject_base_drift(
        &self,
        pr: i64,
        repo_dir: &Path,
        expected_base_branch: &str,
    ) -> Option<MergeResult> {
        base_validation_result(
            pr,
            self.live_base_branch(pr, repo_dir),
            expected_base_branch,
        )
    }

    fn reject_head_drift(
        &self,
        pr: i64,
        repo_dir: &Path,
        expected_head_sha: &str,
    ) -> Option<MergeResult> {
        head_validation_result(pr, self.live_head_sha(pr, repo_dir), expected_head_sha)
    }

    /// Fetch `reviews` + `headRefOid` in one `gh` call. Returns `None` on any
    /// query failure so the caller can fail-open (post the approval).
    fn fetch_reviews_and_head(&self, pr: i64, repo_dir: &Path) -> Option<(String, String)> {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "reviews,headRefOid"],
            repo_dir,
        );
        let output = cmd.output().ok()?;
        if !output.status.success() {
            return None;
        }
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let head = serde_json::from_str::<serde_json::Value>(&stdout)
            .ok()?
            .get("headRefOid")?
            .as_str()?
            .to_string();
        Some((stdout, head))
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

    fn query_checks(&self, pr: i64, repo_dir: &Path) -> ChecksQueryResult {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(
            &[
                "pr",
                "view",
                &pr_str,
                "--json",
                "statusCheckRollup,mergeStateStatus",
            ],
            repo_dir,
        );
        let output = match cmd.output() {
            Ok(o) if o.status.success() => o,
            _ => {
                return ChecksQueryResult::Pending;
            }
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        let (merge_state, checks) = parse_checks_json(&stdout);
        checks_query_from_parsed(&merge_state, &checks)
    }

    fn is_draft(&self, pr: i64, repo_dir: &Path) -> bool {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(
            &[
                "pr", "view", &pr_str, "--json", "isDraft", "--jq", ".isDraft",
            ],
            repo_dir,
        );
        match cmd.output() {
            Ok(o) if o.status.success() => parse_is_draft(&String::from_utf8_lossy(&o.stdout)),
            _ => false,
        }
    }
}

fn base_validation_result(
    pr: i64,
    actual_base_branch: std::result::Result<String, String>,
    expected_base_branch: &str,
) -> Option<MergeResult> {
    match actual_base_branch {
        Ok(actual) if actual == expected_base_branch => None,
        Ok(actual) => Some(MergeResult {
            success: false,
            message: format!(
                "PR #{pr} base drifted to {actual}, expected {expected_base_branch}; \
                 approval and merge not attempted"
            ),
            failure_kind: Some(MergeFailureKind::PolicyBlocked),
        }),
        Err(error) => Some(MergeResult {
            success: false,
            message: format!(
                "PR #{pr} base could not be validated against {expected_base_branch}: \
                 {error}; approval and merge not attempted"
            ),
            failure_kind: Some(MergeFailureKind::PolicyBlocked),
        }),
    }
}

fn head_validation_result(
    pr: i64,
    actual_head_sha: std::result::Result<String, String>,
    expected_head_sha: &str,
) -> Option<MergeResult> {
    match actual_head_sha {
        Ok(actual) if !expected_head_sha.is_empty() && actual == expected_head_sha => None,
        Ok(actual) => Some(MergeResult {
            success: false,
            message: format!(
                "PR #{pr} head drifted to {actual}, expected {expected_head_sha}; \
                 approval and merge not attempted"
            ),
            failure_kind: Some(MergeFailureKind::StaleAuthority),
        }),
        Err(error) => Some(MergeResult {
            success: false,
            message: format!(
                "PR #{pr} head could not be validated against {expected_head_sha}: \
                 {error}; approval and merge not attempted"
            ),
            failure_kind: Some(MergeFailureKind::PolicyBlocked),
        }),
    }
}

fn parse_pr_base_branch(output: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(output)
        .ok()?
        .get("baseRefName")?
        .as_str()
        .map(str::to_owned)
}

fn parse_pr_head_sha(output: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(output)
        .ok()?
        .get("headRefOid")?
        .as_str()
        .filter(|sha| !sha.is_empty())
        .map(str::to_owned)
}

fn merge_command_args<'a>(pr: &'a str, expected_head_sha: &'a str) -> [&'a str; 7] {
    [
        "pr",
        "merge",
        pr,
        "--merge",
        "--delete-branch",
        "--match-head-commit",
        expected_head_sha,
    ]
}

/// Return true iff the `reviews` array (from `gh pr view --json reviews`)
/// contains an APPROVED review whose `commit.oid` matches `head_sha`. Any
/// parse failure returns false (fail-open — caller posts the approval).
fn head_has_approval(reviews_json: &str, head_sha: &str) -> bool {
    let val: serde_json::Value = match serde_json::from_str(reviews_json) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let Some(reviews) = val.get("reviews").and_then(|v| v.as_array()) else {
        return false;
    };
    reviews.iter().any(|r| {
        r.get("state").and_then(|v| v.as_str()) == Some("APPROVED")
            && r.get("commit")
                .and_then(|c| c.get("oid"))
                .and_then(|o| o.as_str())
                == Some(head_sha)
    })
}

fn parse_mergeability(json_str: &str) -> MergeabilityState {
    let val: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return MergeabilityState::Mergeable,
    };
    match val.get("state").and_then(|v| v.as_str()) {
        Some("MERGED") => return MergeabilityState::AlreadyMerged,
        Some("CLOSED") => return MergeabilityState::Closed,
        _ => {}
    }
    match val.get("mergeStateStatus").and_then(|v| v.as_str()) {
        Some("DIRTY") => MergeabilityState::Conflicting,
        _ => MergeabilityState::Mergeable,
    }
}

fn parse_merge_commit_sha(json_str: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json_str)
        .ok()?
        .get("mergeCommit")?
        .get("oid")?
        .as_str()
        .filter(|sha| !sha.is_empty() && !sha.contains('\0'))
        .map(str::to_owned)
}

/// Parse the `--jq .isDraft` output of `gh pr view` (bare `true`/`false`).
/// Anything that isn't exactly `true` is treated as not-draft (fail-safe:
/// a query hiccup skips the undraft and behaves as it did before this guard).
fn parse_is_draft(jq_out: &str) -> bool {
    jq_out.trim() == "true"
}

impl MergeExecutor for GhMergeExecutor {
    fn merge(&self, pr: i64, repo_dir: &Path, ctx: &MergeContext) -> MergeResult {
        let pr_str = pr.to_string();

        // Validate before any mutation (including undrafting). `baseRefName`
        // can change without moving the reviewed head SHA.
        if let Some(rejected) = self.reject_base_drift(pr, repo_dir, &ctx.expected_base_branch) {
            return rejected;
        }
        if let Some(rejected) = self.reject_head_drift(pr, repo_dir, &ctx.expected_head_sha) {
            return rejected;
        }

        // A worker that opened its PR as a draft hard-blocks the merge:
        // `gh pr merge` (GraphQL mergePullRequest) refuses drafts, that failure
        // classifies as PolicyBlocked, and the daemon cancels the task —
        // leaving an approved, green, orphaned PR. Undraft first, then let
        // mergeability re-settle (undrafting recomputes mergeStateStatus, which
        // flips to UNKNOWN/BLOCKED for a few seconds before returning CLEAN).
        if self.is_draft(pr, repo_dir) {
            let ready = self.run_gh(&["pr", "ready", &pr_str], repo_dir);
            if !ready.success {
                return MergeResult {
                    success: false,
                    message: format!("undraft failed (merge not attempted): {}", ready.message),
                    failure_kind: Some(MergeFailureKind::PolicyBlocked),
                };
            }
            // Only a genuine check failure aborts; TimedOut falls through to the
            // merge attempt, whose own failure the caller's merge-gate classifies.
            if let ChecksOutcome::Failed { failing_checks } =
                self.wait_for_checks(pr, repo_dir, 180, 5)
            {
                return MergeResult {
                    success: false,
                    message: format!(
                        "checks not green after undraft: {}",
                        failing_checks.join(", ")
                    ),
                    failure_kind: Some(MergeFailureKind::PolicyBlocked),
                };
            }
        }

        // #49: on a PolicyPending retry the daemon calls merge() again — without
        // dedupe every retry posts another `--approve` stub, cluttering the PR
        // review history (4 identical approvals in 7s observed on task #45).
        // Skip the approve step if an APPROVED review already exists on the
        // current head SHA. Fail-open: query hiccup falls through to the
        // approve call and behaves as before this guard.
        let already_approved = self
            .fetch_reviews_and_head(pr, repo_dir)
            .map(|(reviews_json, head)| head_has_approval(&reviews_json, &head))
            .unwrap_or(false);

        // Recheck after draft/check/review queries and immediately before the
        // formal approval. Every policy retry enters merge() again and repeats
        // this fail-closed validation.
        if let Some(rejected) = self.reject_base_drift(pr, repo_dir, &ctx.expected_base_branch) {
            return rejected;
        }
        if let Some(rejected) = self.reject_head_drift(pr, repo_dir, &ctx.expected_head_sha) {
            return rejected;
        }

        if !already_approved {
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
        }

        // Approval and merge are separate GitHub calls. Close the remaining
        // retarget window before invoking the irreversible merge operation.
        if let Some(rejected) = self.reject_base_drift(pr, repo_dir, &ctx.expected_base_branch) {
            return rejected;
        }
        if let Some(rejected) = self.reject_head_drift(pr, repo_dir, &ctx.expected_head_sha) {
            return rejected;
        }

        let result = self.run_gh(
            &merge_command_args(&pr_str, &ctx.expected_head_sha),
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

    fn merge_commit_sha(&self, pr: i64, repo_dir: &Path) -> Option<String> {
        let pr = pr.to_string();
        let mut cmd = self.build_gh_cmd(&["pr", "view", &pr, "--json", "mergeCommit"], repo_dir);
        let output = cmd.output().ok()?;
        if !output.status.success() {
            return None;
        }
        parse_merge_commit_sha(&String::from_utf8_lossy(&output.stdout))
    }

    fn wait_for_checks(
        &self,
        pr: i64,
        repo_dir: &Path,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> ChecksOutcome {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        // #181: after a push, GitHub transiently returns statusCheckRollup: []
        // before CI registers. Require 2 consecutive empty polls to distinguish
        // "no CI configured" from "checks haven't registered yet."
        let mut consecutive_no_checks = 0u32;
        loop {
            match self.query_checks(pr, repo_dir) {
                ChecksQueryResult::AllPassed => return ChecksOutcome::Ready,
                ChecksQueryResult::SomeFailed(names) => {
                    return ChecksOutcome::Failed {
                        failing_checks: names,
                    }
                }
                ChecksQueryResult::NoChecksConfigured => {
                    consecutive_no_checks += 1;
                    if consecutive_no_checks >= 2 {
                        return ChecksOutcome::Ready;
                    }
                }
                ChecksQueryResult::Pending => {
                    consecutive_no_checks = 0;
                }
            }
            if Instant::now() + Duration::from_secs(poll_interval_secs) > deadline {
                return ChecksOutcome::TimedOut;
            }
            std::thread::sleep(Duration::from_secs(poll_interval_secs));
        }
    }

    fn pending_check_names(&self, pr: i64, repo_dir: &Path) -> Vec<String> {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "statusCheckRollup"],
            repo_dir,
        );
        let output = match cmd.output() {
            Ok(output) if output.status.success() => output,
            _ => return Vec::new(),
        };
        pending_check_names_from_json(&String::from_utf8_lossy(&output.stdout))
    }

    fn check_mergeability(&self, pr: i64, repo_dir: &Path) -> MergeabilityState {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "mergeStateStatus,state"],
            repo_dir,
        );
        let output = match cmd.output() {
            Ok(o) if o.status.success() => o,
            _ => return MergeabilityState::Mergeable,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_mergeability(&stdout)
    }

    fn head_sha(&self, pr: i64, repo_dir: &Path) -> Option<String> {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(
            &[
                "pr",
                "view",
                &pr_str,
                "--json",
                "headRefOid",
                "--jq",
                ".headRefOid",
            ],
            repo_dir,
        );
        let output = cmd.output().ok()?;
        if !output.status.success() {
            return None;
        }
        let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }

    fn check_required_jobs(
        &self,
        pr: i64,
        repo_dir: &Path,
        required_jobs: &[String],
    ) -> RequiredJobsOutcome {
        if required_jobs.is_empty() {
            return RequiredJobsOutcome::AllSucceeded;
        }
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "statusCheckRollup"],
            repo_dir,
        );
        let output = match cmd.output() {
            Ok(o) if o.status.success() => o,
            _ => {
                let issues = required_jobs
                    .iter()
                    .map(|j| (j.clone(), "query failed".into()))
                    .collect();
                return RequiredJobsOutcome::NotReady { issues };
            }
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        validate_required_jobs(&stdout, required_jobs)
    }

    fn check_default_branch_ci(&self, repo_dir: &Path, branch: &str) -> DefaultBranchStatus {
        let mut cmd = self.build_gh_cmd(
            &[
                "run",
                "list",
                "--branch",
                branch,
                "--limit",
                "1",
                "--json",
                "conclusion,status",
            ],
            repo_dir,
        );
        let output = match cmd.output() {
            Ok(o) if o.status.success() => o,
            _ => return DefaultBranchStatus::Unknown,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_default_branch_ci(&stdout)
    }

    fn ensure_changes_requested(&self, pr: i64, repo_dir: &Path, body: &str) {
        let pr_str = pr.to_string();
        let mut cmd = self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "reviewDecision"],
            repo_dir,
        );
        let output = match cmd.output() {
            Ok(o) if o.status.success() => o,
            _ => {
                // Can't check — post defensively.
                let _ = self
                    .build_gh_cmd(
                        &["pr", "review", &pr_str, "--request-changes", "--body", body],
                        repo_dir,
                    )
                    .output();
                return;
            }
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains("CHANGES_REQUESTED") {
            let _ = self
                .build_gh_cmd(
                    &["pr", "review", &pr_str, "--request-changes", "--body", body],
                    repo_dir,
                )
                .output();
        }
    }
}

/// Mock executor controlled by an env var or a command string.
/// Used in integration tests: QUORUM_MERGE_CMD="true" → always succeeds,
/// QUORUM_MERGE_CMD="false" → always fails.
pub struct CommandMergeExecutor {
    pub command: String,
    pub checks_cmd: Option<String>,
    pub mergeability_cmd: Option<String>,
}

impl CommandMergeExecutor {
    fn run_checks_cmd(&self, pr: i64) -> Option<ChecksOutcome> {
        let cmd_str = self.checks_cmd.as_ref()?;
        let expanded = cmd_str.replace("{pr}", &pr.to_string());
        let output = std::process::Command::new("sh")
            .args(["-c", &expanded])
            .output()
            .ok()?;

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();

        if !output.status.success() {
            return Some(ChecksOutcome::Ready);
        }

        let first_line = stdout.lines().next().unwrap_or("");
        match first_line {
            "ready" => Some(ChecksOutcome::Ready),
            "pending" => None,
            "failed" => {
                let failing: Vec<String> = stdout.lines().skip(1).map(|s| s.to_string()).collect();
                Some(ChecksOutcome::Failed {
                    failing_checks: failing,
                })
            }
            _ => Some(ChecksOutcome::Ready),
        }
    }
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

    fn wait_for_checks(
        &self,
        pr: i64,
        _repo_dir: &Path,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> ChecksOutcome {
        if self.checks_cmd.is_none() {
            return ChecksOutcome::Ready;
        }
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            if let Some(outcome) = self.run_checks_cmd(pr) {
                return outcome;
            }
            if Instant::now() + Duration::from_secs(poll_interval_secs) > deadline {
                return ChecksOutcome::TimedOut;
            }
            std::thread::sleep(Duration::from_secs(poll_interval_secs));
        }
    }

    fn head_sha(&self, _pr: i64, repo_dir: &Path) -> Option<String> {
        let output = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo_dir)
            .output()
            .ok()?;
        if output.status.success() {
            Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            None
        }
    }

    fn check_mergeability(&self, pr: i64, _repo_dir: &Path) -> MergeabilityState {
        let Some(ref cmd_str) = self.mergeability_cmd else {
            return MergeabilityState::Mergeable;
        };
        let expanded = cmd_str.replace("{pr}", &pr.to_string());
        let output = std::process::Command::new("sh")
            .args(["-c", &expanded])
            .output();
        match output {
            Ok(out) if out.status.success() => {
                let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
                match stdout.as_str() {
                    "conflicting" => MergeabilityState::Conflicting,
                    "merged" => MergeabilityState::AlreadyMerged,
                    "closed" => MergeabilityState::Closed,
                    _ => MergeabilityState::Mergeable,
                }
            }
            _ => MergeabilityState::Mergeable,
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
            expected_base_branch: "main".into(),
            expected_head_sha: "abc123".into(),
        }
    }

    #[test]
    fn merge_base_validation_rejects_retarget_and_lookup_failure() {
        assert!(base_validation_result(42, Ok("develop".into()), "develop").is_none());

        for rejected in [
            base_validation_result(42, Ok("main".into()), "develop").unwrap(),
            base_validation_result(42, Err("network unavailable".into()), "develop").unwrap(),
        ] {
            assert!(!rejected.success);
            assert_eq!(rejected.failure_kind, Some(MergeFailureKind::PolicyBlocked));
            assert!(
                rejected.message.contains("expected develop")
                    || rejected.message.contains("validated against develop")
            );
        }
    }

    #[test]
    fn merge_head_validation_rejects_drift_and_lookup_failure() {
        assert!(head_validation_result(42, Ok("approved".into()), "approved").is_none());

        let drifted = head_validation_result(42, Ok("moved".into()), "approved").unwrap();
        assert!(!drifted.success);
        assert_eq!(drifted.failure_kind, Some(MergeFailureKind::StaleAuthority));
        assert!(drifted.message.contains("approval and merge not attempted"));

        let unknown = head_validation_result(42, Err("network".into()), "approved").unwrap();
        assert!(!unknown.success);
        assert_eq!(unknown.failure_kind, Some(MergeFailureKind::PolicyBlocked));
    }

    #[test]
    fn production_merge_command_pins_the_approved_head() {
        assert_eq!(
            merge_command_args("42", "approved-sha"),
            [
                "pr",
                "merge",
                "42",
                "--merge",
                "--delete-branch",
                "--match-head-commit",
                "approved-sha",
            ]
        );
    }

    #[test]
    fn merge_base_query_requires_base_ref_name() {
        assert_eq!(
            parse_pr_base_branch(br#"{"baseRefName":"develop"}"#).as_deref(),
            Some("develop")
        );
        assert!(parse_pr_base_branch(br#"{}"#).is_none());
        assert!(parse_pr_base_branch(b"not-json").is_none());
    }

    #[test]
    fn head_has_approval_matches_current_head() {
        let json = r#"{
            "reviews": [
                {"state":"COMMENTED","commit":{"oid":"aaa"}},
                {"state":"APPROVED","commit":{"oid":"bbb"}}
            ]
        }"#;
        assert!(head_has_approval(json, "bbb"));
    }

    #[test]
    fn head_has_approval_stale_approval_ignored() {
        // Approval was on an old commit; head has since moved.
        let json = r#"{"reviews":[{"state":"APPROVED","commit":{"oid":"old"}}]}"#;
        assert!(!head_has_approval(json, "new"));
    }

    #[test]
    fn head_has_approval_non_approved_ignored() {
        let json = r#"{"reviews":[{"state":"CHANGES_REQUESTED","commit":{"oid":"same"}}]}"#;
        assert!(!head_has_approval(json, "same"));
    }

    #[test]
    fn head_has_approval_empty_and_malformed_are_false() {
        assert!(!head_has_approval("", "abc"));
        assert!(!head_has_approval("not json", "abc"));
        assert!(!head_has_approval(r#"{"reviews":[]}"#, "abc"));
        assert!(!head_has_approval(r#"{}"#, "abc"));
    }

    #[test]
    fn command_merge_executor_success() {
        let exec = CommandMergeExecutor {
            command: "echo merged PR {pr}".into(),
            checks_cmd: None,
            mergeability_cmd: None,
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
            checks_cmd: None,
            mergeability_cmd: None,
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
            checks_cmd: None,
            mergeability_cmd: None,
        };
        let result = exec.merge(7, Path::new("/tmp"), &test_ctx());
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::Retryable));
    }

    #[test]
    fn command_merge_executor_policy_pending() {
        let exec = CommandMergeExecutor {
            command:
                "echo 'not mergeable: the base branch policy prohibits the merge' >&2 && exit 1"
                    .into(),
            checks_cmd: None,
            mergeability_cmd: None,
        };
        let result = exec.merge(7, Path::new("/tmp"), &test_ctx());
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyPending));
    }

    #[test]
    fn gh_merge_executor_approve_body_carries_lineage() {
        let ctx = MergeContext {
            reviewer_name: "TestReviewer".into(),
            review_task_id: 42,
            expected_base_branch: "main".into(),
            expected_head_sha: "abc123".into(),
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
    fn base_validation_failure_short_circuits_approval_and_merge() {
        let ctx = test_ctx();
        let exec = GhMergeExecutor {
            token_file: Some(std::path::PathBuf::from("/nonexistent/token")),
            gh_repo: None,
        };
        let result = exec.merge(1, Path::new("/tmp"), &ctx);
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
        assert!(
            result.message.contains("base could not be validated")
                && result.message.contains("approval and merge not attempted"),
            "expected pre-approval base-validation failure, got: {}",
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
    fn classify_head_modified_as_stale_authority() {
        assert_eq!(
            classify_merge_failure("Head branch was modified. Review and try the merge again."),
            MergeFailureKind::StaleAuthority,
        );
    }

    #[test]
    fn classify_expected_head_mismatch_as_stale_authority() {
        assert_eq!(
            classify_merge_failure("pull request head SHA does not match expected head commit"),
            MergeFailureKind::StaleAuthority,
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
    fn classify_retryable_cannot_be_cleanly_created() {
        assert_eq!(
            classify_merge_failure("not mergeable: the merge commit cannot be cleanly created"),
            MergeFailureKind::Retryable,
        );
    }

    #[test]
    fn classify_retryable_cannot_be_cleanly_created_with_pr_prefix() {
        assert_eq!(
            classify_merge_failure(
                "Pull request #42 is not mergeable: \
                 the merge commit cannot be cleanly created"
            ),
            MergeFailureKind::Retryable,
        );
    }

    #[test]
    fn classify_policy_pending() {
        assert_eq!(
            classify_merge_failure("not mergeable: the base branch policy prohibits the merge"),
            MergeFailureKind::PolicyPending,
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
    fn parse_is_draft_variants() {
        assert!(parse_is_draft("true"));
        assert!(parse_is_draft("true\n"));
        assert!(parse_is_draft("  true  "));
        assert!(!parse_is_draft("false"));
        assert!(!parse_is_draft(""));
        assert!(!parse_is_draft("null"));
        assert!(!parse_is_draft("TRUE"));
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

    #[test]
    fn parse_checks_all_success() {
        let json = r#"{
            "mergeStateStatus": "CLEAN",
            "statusCheckRollup": [
                {"name": "fmt", "status": "COMPLETED", "conclusion": "SUCCESS"},
                {"name": "test", "status": "COMPLETED", "conclusion": "SUCCESS"}
            ]
        }"#;
        let (state, checks) = parse_checks_json(json);
        assert_eq!(state.as_deref(), Some("CLEAN"));
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(result, ChecksQueryResult::AllPassed);
    }

    #[test]
    fn parse_checks_conclusion_is_terminal_even_when_status_lags() {
        let json = r#"{
            "mergeStateStatus": "BLOCKED",
            "statusCheckRollup": [
                {"name": "passed", "status": "IN_PROGRESS", "conclusion": "SUCCESS"},
                {"name": "failed", "status": "IN_PROGRESS", "conclusion": "FAILURE"},
                {"name": "pending", "status": "IN_PROGRESS", "conclusion": ""}
            ]
        }"#;
        let (_, checks) = parse_checks_json(json);
        assert_eq!(
            checks,
            Some(vec![
                ("passed".to_string(), SingleCheckStatus::Passed),
                ("failed".to_string(), SingleCheckStatus::Failed),
                ("pending".to_string(), SingleCheckStatus::Pending),
            ])
        );
    }

    #[test]
    fn pending_check_names_normalizes_null_and_missing_conclusions() {
        let json = r#"{
            "statusCheckRollup": [
                {"name": "null-conclusion", "status": "IN_PROGRESS", "conclusion": null},
                {"name": "missing-conclusion", "status": "QUEUED"},
                {"name": "empty-conclusion", "status": "IN_PROGRESS", "conclusion": ""},
                {"name": "completed-empty", "status": "COMPLETED", "conclusion": null},
                {"name": "terminal", "status": "IN_PROGRESS", "conclusion": "SUCCESS"}
            ]
        }"#;
        assert_eq!(
            pending_check_names_from_json(json),
            vec![
                "null-conclusion".to_string(),
                "missing-conclusion".to_string(),
                "empty-conclusion".to_string(),
            ]
        );
    }

    #[test]
    fn parse_checks_some_pending() {
        let json = r#"{
            "mergeStateStatus": "BLOCKED",
            "statusCheckRollup": [
                {"name": "fmt", "status": "COMPLETED", "conclusion": "SUCCESS"},
                {"name": "test", "status": "IN_PROGRESS", "conclusion": ""}
            ]
        }"#;
        let (state, checks) = parse_checks_json(json);
        assert_eq!(state.as_deref(), Some("BLOCKED"));
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(result, ChecksQueryResult::Pending);
    }

    #[test]
    fn parse_checks_some_failed() {
        let json = r#"{
            "mergeStateStatus": "BLOCKED",
            "statusCheckRollup": [
                {"name": "fmt", "status": "COMPLETED", "conclusion": "SUCCESS"},
                {"name": "clippy", "status": "COMPLETED", "conclusion": "FAILURE"},
                {"name": "test", "status": "COMPLETED", "conclusion": "FAILURE"}
            ]
        }"#;
        let (state, checks) = parse_checks_json(json);
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(
            result,
            ChecksQueryResult::SomeFailed(vec!["clippy".to_string(), "test".to_string()])
        );
    }

    #[test]
    fn parse_checks_empty_rollup_is_no_checks_configured() {
        let json = r#"{
            "mergeStateStatus": "BLOCKED",
            "statusCheckRollup": []
        }"#;
        let (state, checks) = parse_checks_json(json);
        assert!(checks.as_ref().unwrap().is_empty());
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(result, ChecksQueryResult::NoChecksConfigured);
    }

    #[test]
    fn parse_checks_clean_with_empty_rollup_is_all_passed() {
        let json = r#"{
            "mergeStateStatus": "CLEAN",
            "statusCheckRollup": []
        }"#;
        let (state, checks) = parse_checks_json(json);
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(result, ChecksQueryResult::AllPassed);
    }

    #[test]
    fn parse_checks_no_rollup_field_is_pending() {
        let json = r#"{"mergeStateStatus": "BLOCKED"}"#;
        let (state, checks) = parse_checks_json(json);
        assert!(checks.is_none());
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(result, ChecksQueryResult::Pending);
    }

    #[test]
    fn parse_checks_invalid_json() {
        let (state, checks) = parse_checks_json("not json");
        assert!(state.is_none());
        assert!(checks.is_none());
    }

    #[test]
    fn parse_checks_non_array_rollup_is_pending() {
        let json = r#"{
            "mergeStateStatus": "BLOCKED",
            "statusCheckRollup": "not an array"
        }"#;
        let (state, checks) = parse_checks_json(json);
        assert!(checks.is_none());
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(result, ChecksQueryResult::Pending);
    }

    #[test]
    fn parse_checks_null_rollup_is_pending() {
        let json = r#"{
            "mergeStateStatus": "BLOCKED",
            "statusCheckRollup": null
        }"#;
        let (state, checks) = parse_checks_json(json);
        assert!(checks.is_none());
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(result, ChecksQueryResult::Pending);
    }

    #[test]
    fn parse_checks_skipped_and_neutral_pass() {
        let json = r#"{
            "mergeStateStatus": "BLOCKED",
            "statusCheckRollup": [
                {"name": "optional", "status": "COMPLETED", "conclusion": "SKIPPED"},
                {"name": "neutral", "status": "COMPLETED", "conclusion": "NEUTRAL"}
            ]
        }"#;
        let (state, checks) = parse_checks_json(json);
        let result = checks_query_from_parsed(&state, &checks);
        assert_eq!(result, ChecksQueryResult::AllPassed);
    }

    #[test]
    fn command_checks_cmd_ready() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: Some("echo ready".into()),
            mergeability_cmd: None,
        };
        let outcome = exec.wait_for_checks(1, Path::new("/tmp"), 5, 1);
        assert_eq!(outcome, ChecksOutcome::Ready);
    }

    #[test]
    fn command_checks_cmd_failed() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: Some("printf 'failed\\nclipper\\ntest'".into()),
            mergeability_cmd: None,
        };
        let outcome = exec.wait_for_checks(1, Path::new("/tmp"), 5, 1);
        assert_eq!(
            outcome,
            ChecksOutcome::Failed {
                failing_checks: vec!["clipper".to_string(), "test".to_string()]
            }
        );
    }

    #[test]
    fn command_checks_cmd_timeout() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: Some("echo pending".into()),
            mergeability_cmd: None,
        };
        let outcome = exec.wait_for_checks(1, Path::new("/tmp"), 1, 1);
        assert_eq!(outcome, ChecksOutcome::TimedOut);
    }

    #[test]
    fn command_no_checks_cmd_returns_ready() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: None,
            mergeability_cmd: None,
        };
        let outcome = exec.wait_for_checks(1, Path::new("/tmp"), 5, 1);
        assert_eq!(outcome, ChecksOutcome::Ready);
    }

    #[test]
    fn parse_mergeability_dirty() {
        assert_eq!(
            parse_mergeability(r#"{"mergeStateStatus":"DIRTY"}"#),
            MergeabilityState::Conflicting,
        );
    }

    #[test]
    fn parse_mergeability_clean() {
        assert_eq!(
            parse_mergeability(r#"{"mergeStateStatus":"CLEAN"}"#),
            MergeabilityState::Mergeable,
        );
    }

    #[test]
    fn parse_mergeability_blocked() {
        assert_eq!(
            parse_mergeability(r#"{"mergeStateStatus":"BLOCKED"}"#),
            MergeabilityState::Mergeable,
        );
    }

    #[test]
    fn parse_mergeability_behind() {
        assert_eq!(
            parse_mergeability(r#"{"mergeStateStatus":"BEHIND"}"#),
            MergeabilityState::Mergeable,
        );
    }

    #[test]
    fn parse_mergeability_invalid_json() {
        assert_eq!(parse_mergeability("not json"), MergeabilityState::Mergeable,);
    }

    #[test]
    fn parse_mergeability_missing_field() {
        assert_eq!(parse_mergeability(r#"{}"#), MergeabilityState::Mergeable,);
    }

    #[test]
    fn read_token_returns_none_on_missing_file() {
        let exec = GhMergeExecutor {
            token_file: Some(std::path::PathBuf::from("/nonexistent/path/token")),
            gh_repo: None,
        };
        assert!(exec.read_token().is_none());
    }

    #[test]
    fn read_token_reads_and_trims_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "  my-secret-token \n").unwrap();
        let exec = GhMergeExecutor {
            token_file: Some(token_path),
            gh_repo: None,
        };
        assert_eq!(exec.read_token().unwrap(), "my-secret-token");
    }

    #[test]
    fn read_token_returns_none_when_no_file_configured() {
        let exec = GhMergeExecutor {
            token_file: None,
            gh_repo: None,
        };
        assert!(exec.read_token().is_none());
    }

    #[test]
    fn command_mergeability_conflicting() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: None,
            mergeability_cmd: Some("echo conflicting".into()),
        };
        assert_eq!(
            exec.check_mergeability(1, Path::new("/tmp")),
            MergeabilityState::Conflicting,
        );
    }

    #[test]
    fn command_mergeability_mergeable() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: None,
            mergeability_cmd: Some("echo mergeable".into()),
        };
        assert_eq!(
            exec.check_mergeability(1, Path::new("/tmp")),
            MergeabilityState::Mergeable,
        );
    }

    #[test]
    fn command_mergeability_none_returns_mergeable() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: None,
            mergeability_cmd: None,
        };
        assert_eq!(
            exec.check_mergeability(1, Path::new("/tmp")),
            MergeabilityState::Mergeable,
        );
    }

    #[test]
    fn command_mergeability_merged() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: None,
            mergeability_cmd: Some("echo merged".into()),
        };
        assert_eq!(
            exec.check_mergeability(1, Path::new("/tmp")),
            MergeabilityState::AlreadyMerged,
        );
    }

    #[test]
    fn command_mergeability_closed() {
        let exec = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: None,
            mergeability_cmd: Some("echo closed".into()),
        };
        assert_eq!(
            exec.check_mergeability(1, Path::new("/tmp")),
            MergeabilityState::Closed,
        );
    }

    #[test]
    fn parse_mergeability_state_merged() {
        assert_eq!(
            parse_mergeability(r#"{"state":"MERGED","mergeStateStatus":"CLEAN"}"#),
            MergeabilityState::AlreadyMerged,
        );
    }

    #[test]
    fn parse_mergeability_state_closed() {
        assert_eq!(
            parse_mergeability(r#"{"state":"CLOSED","mergeStateStatus":"DIRTY"}"#),
            MergeabilityState::Closed,
        );
    }

    #[test]
    fn parse_mergeability_state_open_dirty() {
        assert_eq!(
            parse_mergeability(r#"{"state":"OPEN","mergeStateStatus":"DIRTY"}"#),
            MergeabilityState::Conflicting,
        );
    }

    // ── Required jobs gate tests ──────────────────────────────────────

    #[test]
    fn required_jobs_empty_list_always_succeeds() {
        assert_eq!(
            validate_required_jobs("{}", &[]),
            RequiredJobsOutcome::AllSucceeded,
        );
    }

    #[test]
    fn required_jobs_all_success() {
        let json = r#"{
            "statusCheckRollup": [
                {"name": "python-tests", "status": "COMPLETED", "conclusion": "SUCCESS"},
                {"name": "frontend-build", "status": "COMPLETED", "conclusion": "SUCCESS"},
                {"name": "ci-gate", "status": "COMPLETED", "conclusion": "SUCCESS"}
            ]
        }"#;
        assert_eq!(
            validate_required_jobs(json, &["python-tests".into(), "frontend-build".into()],),
            RequiredJobsOutcome::AllSucceeded,
        );
    }

    #[test]
    fn required_jobs_skipped_blocks() {
        let json = r#"{
            "statusCheckRollup": [
                {"name": "python-tests", "status": "COMPLETED", "conclusion": "SKIPPED"},
                {"name": "pii-service-tests", "status": "COMPLETED", "conclusion": "SKIPPED"},
                {"name": "ci-gate", "status": "COMPLETED", "conclusion": "SUCCESS"}
            ]
        }"#;
        let result =
            validate_required_jobs(json, &["python-tests".into(), "pii-service-tests".into()]);
        match result {
            RequiredJobsOutcome::NotReady { issues } => {
                assert_eq!(issues.len(), 2);
                assert!(issues
                    .iter()
                    .any(|(n, s)| n == "python-tests" && s == "SKIPPED"));
                assert!(issues
                    .iter()
                    .any(|(n, s)| n == "pii-service-tests" && s == "SKIPPED"));
            }
            other => panic!("expected NotReady, got {other:?}"),
        }
    }

    #[test]
    fn required_jobs_absent_blocks() {
        let json = r#"{
            "statusCheckRollup": [
                {"name": "ci-gate", "status": "COMPLETED", "conclusion": "SUCCESS"}
            ]
        }"#;
        let result = validate_required_jobs(json, &["python-tests".into()]);
        match result {
            RequiredJobsOutcome::NotReady { issues } => {
                assert_eq!(issues.len(), 1);
                assert_eq!(issues[0], ("python-tests".into(), "absent".into()));
            }
            other => panic!("expected NotReady, got {other:?}"),
        }
    }

    #[test]
    fn required_jobs_neutral_blocks() {
        let json = r#"{
            "statusCheckRollup": [
                {"name": "python-tests", "status": "COMPLETED", "conclusion": "NEUTRAL"}
            ]
        }"#;
        let result = validate_required_jobs(json, &["python-tests".into()]);
        match result {
            RequiredJobsOutcome::NotReady { issues } => {
                assert_eq!(issues[0].1, "NEUTRAL");
            }
            other => panic!("expected NotReady, got {other:?}"),
        }
    }

    #[test]
    fn required_jobs_pending_returns_pending() {
        let json = r#"{
            "statusCheckRollup": [
                {"name": "python-tests", "status": "IN_PROGRESS", "conclusion": ""},
                {"name": "ci-gate", "status": "COMPLETED", "conclusion": "SUCCESS"}
            ]
        }"#;
        let result = validate_required_jobs(json, &["python-tests".into()]);
        match result {
            RequiredJobsOutcome::Pending { pending_jobs } => {
                assert_eq!(pending_jobs, vec!["python-tests".to_string()]);
            }
            other => panic!("expected Pending, got {other:?}"),
        }
    }

    #[test]
    fn required_jobs_mixed_skipped_and_success() {
        let json = r#"{
            "statusCheckRollup": [
                {"name": "python-tests", "status": "COMPLETED", "conclusion": "SUCCESS"},
                {"name": "pii-service-tests", "status": "COMPLETED", "conclusion": "SKIPPED"},
                {"name": "frontend-build", "status": "COMPLETED", "conclusion": "SUCCESS"}
            ]
        }"#;
        let result = validate_required_jobs(
            json,
            &[
                "python-tests".into(),
                "pii-service-tests".into(),
                "frontend-build".into(),
            ],
        );
        match result {
            RequiredJobsOutcome::NotReady { issues } => {
                assert_eq!(issues.len(), 1);
                assert_eq!(issues[0], ("pii-service-tests".into(), "SKIPPED".into()));
            }
            other => panic!("expected NotReady, got {other:?}"),
        }
    }

    #[test]
    fn required_jobs_invalid_json() {
        let result = validate_required_jobs("not json", &["test".into()]);
        assert!(matches!(result, RequiredJobsOutcome::NotReady { .. }));
    }

    #[test]
    fn required_jobs_absent_from_empty_rollup() {
        let json = r#"{"statusCheckRollup": []}"#;
        let result = validate_required_jobs(json, &["ci-gate".into()]);
        match result {
            RequiredJobsOutcome::NotReady { issues } => {
                assert_eq!(issues.len(), 1);
                assert_eq!(issues[0], ("ci-gate".into(), "absent".into()));
            }
            other => panic!("expected NotReady, got {other:?}"),
        }
    }

    #[test]
    fn required_jobs_gate_preserves_pending_and_failed_semantics() {
        assert_eq!(
            apply_required_jobs_gate(
                ChecksOutcome::Ready,
                RequiredJobsOutcome::Pending {
                    pending_jobs: vec!["test".into()]
                }
            ),
            ChecksOutcome::Pending {
                reason: "required jobs still pending: test".into()
            }
        );
        assert_eq!(
            apply_required_jobs_gate(
                ChecksOutcome::Ready,
                RequiredJobsOutcome::NotReady {
                    issues: vec![("fmt".into(), "SKIPPED".into())]
                }
            ),
            ChecksOutcome::Failed {
                failing_checks: vec!["fmt (SKIPPED)".into()]
            }
        );
        assert_eq!(
            apply_required_jobs_gate(
                ChecksOutcome::Failed {
                    failing_checks: vec!["clippy".into()]
                },
                RequiredJobsOutcome::AllSucceeded
            ),
            ChecksOutcome::Failed {
                failing_checks: vec!["clippy".into()]
            },
            "required-job success must not erase a general check failure"
        );
    }

    // ── Default branch CI gate tests ──────────────────────────────────

    #[test]
    fn default_branch_ci_green() {
        let json = r#"[{"conclusion":"success","status":"completed"}]"#;
        assert_eq!(parse_default_branch_ci(json), DefaultBranchStatus::Green);
    }

    #[test]
    fn default_branch_ci_red() {
        let json = r#"[{"conclusion":"failure","status":"completed"}]"#;
        match parse_default_branch_ci(json) {
            DefaultBranchStatus::Red { details } => {
                assert!(details.contains("failure"), "details: {details}");
            }
            other => panic!("expected Red, got {other:?}"),
        }
    }

    #[test]
    fn default_branch_ci_pending() {
        let json = r#"[{"conclusion":"","status":"in_progress"}]"#;
        assert_eq!(parse_default_branch_ci(json), DefaultBranchStatus::Pending);
    }

    #[test]
    fn default_branch_ci_empty_array() {
        assert_eq!(parse_default_branch_ci("[]"), DefaultBranchStatus::Unknown);
    }

    #[test]
    fn default_branch_ci_invalid_json() {
        assert_eq!(
            parse_default_branch_ci("not json"),
            DefaultBranchStatus::Unknown,
        );
    }

    #[test]
    fn default_branch_ci_cancelled() {
        let json = r#"[{"conclusion":"cancelled","status":"completed"}]"#;
        match parse_default_branch_ci(json) {
            DefaultBranchStatus::Red { details } => {
                assert!(details.contains("cancelled"), "details: {details}");
            }
            other => panic!("expected Red, got {other:?}"),
        }
    }
}
