//! MergeExecutor — seam for PR merge so tests can mock the `gh` call.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::github_credentials::{
    GithubCredentialError, GithubCredentialSource, LocalDefaultCredentialSource,
};

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

/// Bounded retries after a merge policy reports a transient pending state.
/// Branch-sync CI uses the same retry budget after its initial full wait.
pub const MAX_POLICY_RETRIES: u32 = 3;

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
            reason: format!("{REQUIRED_JOBS_PENDING_PREFIX}{}", pending_jobs.join(", ")),
        },
    }
}

const REQUIRED_JOBS_PENDING_PREFIX: &str = "required jobs still pending: ";

/// Issue status [`validate_required_jobs`] records for a required job missing
/// from the rollup.
const REQUIRED_JOB_ABSENT: &str = "absent";

/// Pre-review classification of an exact-head required-jobs snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreReviewRequiredJobs {
    Ready,
    /// A required job reached a terminal non-SUCCESS conclusion (or the
    /// snapshot could not be read); names exclude merely absent jobs.
    Failed {
        failing_checks: Vec<String>,
    },
    /// Some required jobs are still running, or the rollup is for another head.
    Pending {
        reason: String,
    },
    /// Every issue is a required job missing from the rollup: CI has not
    /// registered it yet. Absence is not worker-actionable, so it never fails.
    Unregistered {
        missing_jobs: Vec<String>,
    },
}

/// Pre-review counterpart of [`apply_required_jobs_gate`]. GitHub can report a
/// CLEAN or empty rollup seconds after a push, before CI registers its jobs, so
/// an absent required job is pending here rather than failed. A real terminal
/// non-SUCCESS conclusion still fails, even when other jobs are absent. The
/// merge-time and branch-sync gates keep [`apply_required_jobs_gate`].
pub fn classify_pre_review_required_jobs(required: RequiredJobsOutcome) -> PreReviewRequiredJobs {
    match required {
        RequiredJobsOutcome::AllSucceeded => PreReviewRequiredJobs::Ready,
        RequiredJobsOutcome::Pending { pending_jobs } => PreReviewRequiredJobs::Pending {
            reason: format!("{REQUIRED_JOBS_PENDING_PREFIX}{}", pending_jobs.join(", ")),
        },
        RequiredJobsOutcome::NotReady { issues } => {
            let (missing, failing): (Vec<_>, Vec<_>) = issues
                .into_iter()
                .partition(|(_, status)| status == REQUIRED_JOB_ABSENT);
            if failing.is_empty() {
                PreReviewRequiredJobs::Unregistered {
                    missing_jobs: missing.into_iter().map(|(name, _)| name).collect(),
                }
            } else {
                PreReviewRequiredJobs::Failed {
                    failing_checks: failing
                        .into_iter()
                        .map(|(name, status)| format!("{name} ({status})"))
                        .collect(),
                }
            }
        }
    }
}

/// Pending required-job names recovered from a `Pending` reason produced by
/// [`apply_required_jobs_gate`]. Returns `None` for any other pending reason
/// (e.g. a timed-out rollup wait) so the caller can ask the provider instead.
pub fn required_jobs_pending_names(reason: &str) -> Option<Vec<String>> {
    let names = reason.strip_prefix(REQUIRED_JOBS_PENDING_PREFIX)?;
    Some(
        names
            .split(", ")
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultBranchStatus {
    Green,
    Red { details: String },
    Pending,
    Unknown,
}

/// The observable PR state used by recovery paths that need to distinguish a
/// genuinely unmerged PR from transiently unavailable merge metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeCommitStatus {
    Merged { merge_commit_sha: Option<String> },
    Open,
    Closed,
    Unknown,
}

/// Trait for executing PR merges. The default implementation posts a formal
/// GitHub approval review (`gh pr review --approve`) then calls `gh pr merge`.
/// Tests inject a mock via [`CommandMergeExecutor::command`].
pub trait MergeExecutor: Send + Sync {
    fn merge(&self, pr: i64, repo_dir: &Path, ctx: &MergeContext) -> MergeResult;

    /// Daemon-internal branch syncs have no reviewer, so they must not post a
    /// formal approval. Implementations still validate the pinned head/base
    /// before the irreversible merge call. The default keeps lightweight test
    /// executors source-compatible; the GitHub executor overrides it.
    fn merge_without_approval(&self, pr: i64, repo_dir: &Path, ctx: &MergeContext) -> MergeResult {
        self.merge(pr, repo_dir, ctx)
    }

    /// GitHub's immutable merge commit for a completed PR. The production
    /// implementation asks the PR API after merge; unavailable metadata is
    /// intentionally represented as `None` so callers can keep the lifecycle
    /// conservative rather than inventing a commit identity.
    fn merge_commit_sha(&self, _pr: i64, _repo_dir: &Path) -> Option<String> {
        None
    }

    /// Resolve whether a PR has merged before attempting to recover missing
    /// dependency provenance. Test executors that only supply a merge SHA keep
    /// their established meaning; the GitHub executor reports the real state.
    fn merge_commit_status(&self, pr: i64, repo_dir: &Path) -> MergeCommitStatus {
        match self.merge_commit_sha(pr, repo_dir) {
            Some(merge_commit_sha) => MergeCommitStatus::Merged {
                merge_commit_sha: Some(merge_commit_sha),
            },
            None => MergeCommitStatus::Unknown,
        }
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

    /// Exact-head variant of [`MergeExecutor::wait_for_checks`]. A check
    /// snapshot GitHub reports for any PR head other than `expected_head`
    /// (read-after-write lag right after a push) counts as pending, never as
    /// failed or ready. The default delegates so lightweight test executors
    /// stay source-compatible; the GitHub executor overrides it.
    fn wait_for_checks_at_head(
        &self,
        pr: i64,
        repo_dir: &Path,
        _expected_head: &str,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> ChecksOutcome {
        self.wait_for_checks(pr, repo_dir, timeout_secs, poll_interval_secs)
    }

    /// CI wait for a branch sync. Unlike ordinary task merges, an empty
    /// status-check rollup never establishes readiness: the sync path has a
    /// mandatory non-empty required-jobs gate. The production executor
    /// overrides this while test executors can share `wait_for_checks`.
    fn wait_for_branch_sync_checks(
        &self,
        pr: i64,
        repo_dir: &Path,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> ChecksOutcome {
        self.wait_for_checks(pr, repo_dir, timeout_secs, poll_interval_secs)
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

    /// [`MergeExecutor::check_mergeability`] plus the `headRefOid` GitHub
    /// computed that state for, read in the same response. The pre-review gate
    /// acts on `Conflicting` only when this head equals the recorded head, so a
    /// lagging DIRTY report for an older head never triggers conflict rework.
    /// Default: no head, which never authorizes pre-review conflict routing.
    fn check_mergeability_with_head(
        &self,
        pr: i64,
        repo_dir: &Path,
    ) -> (MergeabilityState, Option<String>) {
        (self.check_mergeability(pr, repo_dir), None)
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

    /// Exact-head variant of [`MergeExecutor::check_required_jobs`]: a rollup
    /// for another PR head is pending, never `NotReady`.
    fn check_required_jobs_at_head(
        &self,
        pr: i64,
        repo_dir: &Path,
        _expected_head: &str,
        required_jobs: &[String],
    ) -> RequiredJobsOutcome {
        self.check_required_jobs(pr, repo_dir, required_jobs)
    }

    /// Check the CI status of the default branch's latest workflow run.
    /// Default: Green (gate off).
    fn check_default_branch_ci(&self, _repo_dir: &Path, _branch: &str) -> DefaultBranchStatus {
        DefaultBranchStatus::Green
    }

    /// #159: post or verify the daemon-owned REQUEST_CHANGES review carrying the
    /// blocking findings. Managed reviewer agents share the PR author's GitHub
    /// identity, so formal review state stays with the daemon's merge account.
    /// Returns `true` when the review is known to exist (verified or posted).
    fn ensure_changes_requested(&self, _pr: i64, _repo_dir: &Path, _body: &str) -> bool {
        // Default no-op; real executor posts via `gh pr review --request-changes`.
        true
    }

    /// The daemon's one shared GitHub credential source, reachable by other
    /// daemon modules through `ServeConfig.merge_executor`. The default is the
    /// local default source with no token file (the token `gh` would use);
    /// the GitHub executor returns its injected source.
    fn github_credentials(&self) -> Arc<dyn GithubCredentialSource> {
        // Test builds hand every mock executor a fixed fake token so unit
        // fixtures never depend on ambient `gh auth` on the test host (CI
        // runners have none). Production keeps the local default source.
        #[cfg(test)]
        {
            Arc::new(crate::github_credentials::FakeCredentialSource::token(
                "test-fixture-token",
            ))
        }
        #[cfg(not(test))]
        {
            Arc::new(LocalDefaultCredentialSource::new(None))
        }
    }

    /// The daemon's PR-authoring (publishing) identity: publication,
    /// PR-target lookup, drift, branch-sync, and cleanup `gh` children.
    /// Configuring a merge token file distinct from this source keeps the two
    /// identities apart, so the merge account's formal APPROVE/REQUEST_CHANGES
    /// review is never a self-review. The default is the merge default (both
    /// are the local default source with no token file); GitHub then rejects a
    /// formal self-review, so `MergeExecutor::merge` and
    /// `ensure_changes_requested` detect that per PR (`viewerDidAuthor`) and
    /// fall back to posting a `COMMENT` verdict review instead — one-identity
    /// mode.
    fn publication_credentials(&self) -> Arc<dyn GithubCredentialSource> {
        self.github_credentials()
    }
}

/// Merge failure for a credential source that produced no token. No `gh`
/// child was spawned; the failure classifies like any other GitHub failure
/// that is not worker-fixable.
fn credential_failure(error: &GithubCredentialError) -> MergeResult {
    MergeResult {
        success: false,
        message: format!("GitHub credential unavailable (gh not run): {error}"),
        failure_kind: Some(MergeFailureKind::PolicyBlocked),
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

/// Attribute a `gh pr view --json headRefOid,...` snapshot to an exact head.
/// Returns `None` when the snapshot's `headRefOid` is `expected_head`, or a
/// pending reason when GitHub reports another (or no) head: check results in
/// that snapshot belong to a different commit and must not gate this one.
fn checks_snapshot_head_mismatch(json_str: &str, expected_head: &str) -> Option<String> {
    let reported = serde_json::from_str::<serde_json::Value>(json_str)
        .ok()
        .and_then(|value| {
            value
                .get("headRefOid")
                .and_then(|head| head.as_str())
                .map(str::to_owned)
        });
    match reported.as_deref() {
        Some(head) if head == expected_head => None,
        Some(head) => Some(format!(
            "GitHub reports PR head {head}, not expected head {expected_head}"
        )),
        None => Some(format!(
            "GitHub reported no PR head while waiting for expected head {expected_head}"
        )),
    }
}

/// Interpret one `gh pr view` checks snapshot. With `expected_head`, a
/// snapshot attributed to any other PR head is pending, never failed or ready.
fn checks_query_for_head(json_str: &str, expected_head: Option<&str>) -> ChecksQueryResult {
    if expected_head
        .is_some_and(|expected| checks_snapshot_head_mismatch(json_str, expected).is_some())
    {
        return ChecksQueryResult::Pending;
    }
    let (merge_state, checks) = parse_checks_json(json_str);
    checks_query_from_parsed(&merge_state, &checks)
}

/// Required-jobs counterpart of [`checks_query_for_head`]: a rollup for
/// another head is `Pending`, never `NotReady`.
fn required_jobs_for_head(
    json_str: &str,
    expected_head: Option<&str>,
    required_jobs: &[String],
) -> RequiredJobsOutcome {
    if let Some(reason) =
        expected_head.and_then(|expected| checks_snapshot_head_mismatch(json_str, expected))
    {
        return RequiredJobsOutcome::Pending {
            pending_jobs: vec![reason],
        };
    }
    validate_required_jobs(json_str, required_jobs)
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

/// Branch synchronization is fail-closed for an empty rollup. Ordinary task
/// merges retain their two-poll no-CI compatibility rule; this path can only
/// advance after actual checks report green and the configured named jobs are
/// subsequently verified.
fn branch_sync_checks_query_outcome(query: ChecksQueryResult) -> Option<ChecksOutcome> {
    match query {
        ChecksQueryResult::AllPassed => Some(ChecksOutcome::Ready),
        ChecksQueryResult::SomeFailed(failing_checks) => {
            Some(ChecksOutcome::Failed { failing_checks })
        }
        ChecksQueryResult::Pending | ChecksQueryResult::NoChecksConfigured => None,
    }
}

fn branch_sync_checks_query_from_parsed(
    merge_state: &Option<String>,
    checks: &Option<Vec<(String, SingleCheckStatus)>>,
) -> ChecksQueryResult {
    if checks.as_ref().is_some_and(Vec::is_empty) {
        return ChecksQueryResult::NoChecksConfigured;
    }
    checks_query_from_parsed(merge_state, checks)
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
            None => issues.push((job.clone(), REQUIRED_JOB_ABSENT.to_string())),
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
/// `gh pr merge <pr> --merge`, deleting only non-protected head branches.
/// Every `gh` child asks `credentials` for a token at call time and receives
/// it via `GH_TOKEN` on that child only; a source failure returns before any
/// child spawns. The token is never exposed to agent processes.
pub struct GhMergeExecutor {
    /// Merge-authority source (`--merge-token-file`): approve, request
    /// changes, merge, and other merge-gate `gh` children.
    pub credentials: Arc<dyn GithubCredentialSource>,
    /// PR-authoring source (local default, no merge token file), returned by
    /// [`MergeExecutor::publication_credentials`]. Never the merge token, so
    /// a distinct `credentials` source keeps daemon-authored PRs reviewable
    /// by the merge account. Equal to `credentials` by default (no merge
    /// token file configured), which puts every PR in one-identity mode:
    /// `merge`/`ensure_changes_requested` detect that per PR and post a
    /// `COMMENT` verdict instead of a formal review.
    pub publication_credentials: Arc<dyn GithubCredentialSource>,
    /// `owner/repo` slug for `-R` flag — avoids cwd git dependency.
    pub gh_repo: Option<String>,
    /// Long-lived daemon branches that must never be deleted after a merge.
    pub base_branch: String,
    pub self_update_branch: String,
    /// `gh` program every child runs. Production is `gh` (PATH lookup);
    /// tests point it at a fake script.
    pub gh_program: PathBuf,
}

impl GhMergeExecutor {
    /// Build one `gh` child with `GH_TOKEN` from the credential source. The
    /// token is fetched first, so a source failure spawns nothing.
    fn build_gh_cmd(
        &self,
        args: &[&str],
        repo_dir: &Path,
    ) -> std::result::Result<Command, GithubCredentialError> {
        let token = self.credentials.token()?;
        let mut cmd = Command::new(&self.gh_program);
        cmd.args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_OPTIONAL_LOCKS", "0");
        if let Some(ref nwo) = self.gh_repo {
            cmd.args(["-R", nwo]);
        } else {
            cmd.current_dir(repo_dir);
        }
        token.apply_to_command(&mut cmd);
        Ok(cmd)
    }

    fn run_gh(&self, args: &[&str], repo_dir: &Path) -> MergeResult {
        let mut cmd = match self.build_gh_cmd(args, repo_dir) {
            Ok(cmd) => cmd,
            Err(error) => return credential_failure(&error),
        };

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
        let mut cmd = self
            .build_gh_cmd(&["pr", "view", &pr_str, "--json", "baseRefName"], repo_dir)
            .map_err(|error| error.to_string())?;
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
        let mut cmd = self
            .build_gh_cmd(&["pr", "view", &pr_str, "--json", "headRefOid"], repo_dir)
            .map_err(|error| error.to_string())?;
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

    fn live_head_ref(&self, pr: i64, repo_dir: &Path) -> Option<String> {
        let pr_str = pr.to_string();
        let mut cmd = self
            .build_gh_cmd(&["pr", "view", &pr_str, "--json", "headRefName"], repo_dir)
            .ok()?;
        let output = cmd.output().ok()?;
        if !output.status.success() {
            return None;
        }
        parse_pr_head_ref(&output.stdout)
    }

    /// Build the `gh repo view` command for the repository default-branch lookup.
    ///
    /// `gh repo view` takes the repo *positionally* and has no `-R` flag, so it
    /// cannot route through `build_gh_cmd` (which appends `-R <nwo>` and fails with
    /// `unknown shorthand flag: 'R' in -R`). This replicates `build_gh_cmd`'s token +
    /// cwd behavior, with the only difference being the slug is passed positionally.
    fn repo_view_default_branch_cmd(
        &self,
        repo_dir: &Path,
    ) -> std::result::Result<Command, GithubCredentialError> {
        let token = self.credentials.token()?;
        let mut cmd = Command::new(&self.gh_program);
        cmd.args(["repo", "view"])
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_OPTIONAL_LOCKS", "0");
        if let Some(ref nwo) = self.gh_repo {
            cmd.arg(nwo);
        } else {
            cmd.current_dir(repo_dir);
        }
        cmd.args(["--json", "defaultBranchRef"]);
        token.apply_to_command(&mut cmd);
        Ok(cmd)
    }

    fn repository_default_branch(&self, repo_dir: &Path) -> Option<String> {
        let mut cmd = match self.repo_view_default_branch_cmd(repo_dir) {
            Ok(cmd) => cmd,
            Err(error) => {
                eprintln!("warn: gh repo view not run: {error}");
                return None;
            }
        };
        let output = cmd.output().ok()?;
        if !output.status.success() {
            eprintln!(
                "warn: gh repo view failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
            return None;
        }
        parse_repository_default_branch(&output.stdout)
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
        let mut cmd = self
            .build_gh_cmd(
                &["pr", "view", &pr_str, "--json", "reviews,headRefOid"],
                repo_dir,
            )
            .ok()?;
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

    /// Build the identity probe: `gh api graphql` with the constant
    /// [`PR_IDENTITY_QUERY`] and only daemon-derived owner, name, and number
    /// as variables. `gh api` rejects `-R`, so a configured slug travels as
    /// raw `-f` strings; without one, `-F` expands gh's `{owner}`/`{repo}`
    /// placeholders from `repo_dir`. The token is fetched first, so a source
    /// failure spawns nothing.
    fn identity_probe_cmd(
        &self,
        pr: i64,
        repo_dir: &Path,
    ) -> std::result::Result<Command, GithubCredentialError> {
        let token = self.credentials.token()?;
        let mut cmd = Command::new(&self.gh_program);
        cmd.args(["api", "graphql", "-f"])
            .arg(format!("query={PR_IDENTITY_QUERY}"))
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_OPTIONAL_LOCKS", "0");
        match self
            .gh_repo
            .as_deref()
            .and_then(|slug| slug.split_once('/'))
        {
            Some((owner, name)) => {
                cmd.args(["-f", &format!("owner={owner}")])
                    .args(["-f", &format!("name={name}")]);
            }
            None => {
                cmd.args(["-F", "owner={owner}", "-F", "name={repo}"])
                    .current_dir(repo_dir);
            }
        }
        cmd.args(["-F", &format!("number={pr}")]);
        token.apply_to_command(&mut cmd);
        Ok(cmd)
    }

    /// Ask GitHub, with the merge credential, whether that credential's
    /// identity authored the PR. `None` (never one-identity) on credential
    /// failure, gh failure, or any malformed/partial response; each failure
    /// is logged.
    fn probe_pr_identity(&self, pr: i64, repo_dir: &Path) -> Option<PrIdentity> {
        let mut cmd = match self.identity_probe_cmd(pr, repo_dir) {
            Ok(cmd) => cmd,
            Err(error) => {
                eprintln!("warn: PR #{pr} identity probe not run: {error}");
                return None;
            }
        };
        let output = match cmd.output() {
            Ok(output) => output,
            Err(error) => {
                eprintln!("warn: PR #{pr} identity probe failed to run gh: {error}");
                return None;
            }
        };
        if !output.status.success() {
            eprintln!(
                "warn: PR #{pr} identity probe failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
            return None;
        }
        let parsed = parse_pr_identity(&String::from_utf8_lossy(&output.stdout));
        if parsed.is_none() {
            eprintln!("warn: PR #{pr} identity probe returned an unusable response");
        }
        parsed
    }

    /// Fresh `reviewDecision` read (`gh pr view <n> --json reviewDecision`).
    /// `None` on credential/gh failure or a malformed response; `Some("")`
    /// when GitHub reports no decision.
    fn live_review_decision(&self, pr: i64, repo_dir: &Path) -> Option<String> {
        let pr_str = pr.to_string();
        let mut cmd = match self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "reviewDecision"],
            repo_dir,
        ) {
            Ok(cmd) => cmd,
            Err(error) => {
                eprintln!("warn: PR #{pr} reviewDecision read not run: {error}");
                return None;
            }
        };
        let output = cmd.output().ok()?;
        if !output.status.success() {
            eprintln!(
                "warn: PR #{pr} reviewDecision read failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
            return None;
        }
        parse_review_decision(&String::from_utf8_lossy(&output.stdout))
    }

    /// One-identity changes verdict: true once a review on the current head
    /// carries the changes marker for that head, found or just posted. Never
    /// posts `--request-changes`. Without a readable head, nothing is posted
    /// (false) because the marker cannot be bound.
    fn ensure_changes_comment(&self, pr: i64, repo_dir: &Path, body: &str) -> bool {
        let Some((reviews, head)) = self.fetch_reviews_and_head(pr, repo_dir) else {
            eprintln!("warn: PR #{pr} changes comment not posted: reviews/head read failed");
            return false;
        };
        if head.is_empty() {
            eprintln!("warn: PR #{pr} changes comment not posted: empty head");
            return false;
        }
        if head_has_verdict_marker(&reviews, &head, VerdictKind::Changes) {
            return true;
        }
        let pr_str = pr.to_string();
        let marked = format!("{body}\n\n{}", verdict_marker(VerdictKind::Changes, &head));
        let result = self.run_gh(
            &["pr", "review", &pr_str, "--comment", "--body", &marked],
            repo_dir,
        );
        if !result.success {
            eprintln!("warn: PR #{pr} changes comment failed: {}", result.message);
        }
        result.success
    }

    /// True only when a fresh `reviewDecision` read is `REVIEW_REQUIRED`; a
    /// failed read or any other decision leaves today's merge path in charge.
    fn review_required(&self, pr: i64, repo_dir: &Path) -> bool {
        self.live_review_decision(pr, repo_dir).as_deref() == Some("REVIEW_REQUIRED")
    }

    fn verify_pr_merged(&self, pr: i64, repo_dir: &Path) -> bool {
        let pr_str = pr.to_string();
        let Ok(mut cmd) = self.build_gh_cmd(&["pr", "view", &pr_str, "--json", "state"], repo_dir)
        else {
            return false;
        };
        let output = match cmd.output() {
            Ok(o) if o.status.success() => o,
            _ => return false,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        gh_pr_state_is_merged(&stdout)
    }

    /// Poll one PR's check rollup until it settles or `timeout_secs` passes.
    /// With `expected_head`, a snapshot for another PR head is pending.
    fn poll_checks(
        &self,
        pr: i64,
        repo_dir: &Path,
        expected_head: Option<&str>,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> ChecksOutcome {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        // #181: after a push, GitHub transiently returns statusCheckRollup: []
        // before CI registers. Require 2 consecutive empty polls to distinguish
        // "no CI configured" from "checks haven't registered yet."
        let mut consecutive_no_checks = 0u32;
        loop {
            match self.query_checks(pr, repo_dir, expected_head) {
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

    /// Required-jobs snapshot. With `expected_head`, the same `gh` call reads
    /// `headRefOid`; a rollup for another head is pending, never `NotReady`.
    fn query_required_jobs(
        &self,
        pr: i64,
        repo_dir: &Path,
        expected_head: Option<&str>,
        required_jobs: &[String],
    ) -> RequiredJobsOutcome {
        if required_jobs.is_empty() {
            return RequiredJobsOutcome::AllSucceeded;
        }
        let pr_str = pr.to_string();
        let fields = if expected_head.is_some() {
            "statusCheckRollup,headRefOid"
        } else {
            "statusCheckRollup"
        };
        let output = match self
            .build_gh_cmd(&["pr", "view", &pr_str, "--json", fields], repo_dir)
            .map(|mut cmd| cmd.output())
        {
            Ok(Ok(o)) if o.status.success() => o,
            _ => {
                let issues = required_jobs
                    .iter()
                    .map(|j| (j.clone(), "query failed".into()))
                    .collect();
                return RequiredJobsOutcome::NotReady { issues };
            }
        };
        required_jobs_for_head(
            &String::from_utf8_lossy(&output.stdout),
            expected_head,
            required_jobs,
        )
    }

    /// One rollup snapshot. With `expected_head`, the same `gh` call also reads
    /// `headRefOid` so the rollup is attributed to that exact head; a snapshot
    /// for any other head is pending.
    fn query_checks(
        &self,
        pr: i64,
        repo_dir: &Path,
        expected_head: Option<&str>,
    ) -> ChecksQueryResult {
        let pr_str = pr.to_string();
        let fields = if expected_head.is_some() {
            "statusCheckRollup,mergeStateStatus,headRefOid"
        } else {
            "statusCheckRollup,mergeStateStatus"
        };
        let output = match self
            .build_gh_cmd(&["pr", "view", &pr_str, "--json", fields], repo_dir)
            .map(|mut cmd| cmd.output())
        {
            Ok(Ok(o)) if o.status.success() => o,
            _ => {
                return ChecksQueryResult::Pending;
            }
        };
        checks_query_for_head(&String::from_utf8_lossy(&output.stdout), expected_head)
    }

    /// Branch sync has a stricter interpretation than an ordinary task merge:
    /// GitHub may call the PR CLEAN even while its rollup is an empty array,
    /// but that array still cannot satisfy the mandatory named-jobs gate.
    fn query_branch_sync_checks(&self, pr: i64, repo_dir: &Path) -> ChecksQueryResult {
        let pr_str = pr.to_string();
        let cmd = self.build_gh_cmd(
            &[
                "pr",
                "view",
                &pr_str,
                "--json",
                "statusCheckRollup,mergeStateStatus",
            ],
            repo_dir,
        );
        let output = match cmd.map(|mut cmd| cmd.output()) {
            Ok(Ok(o)) if o.status.success() => o,
            _ => return ChecksQueryResult::Pending,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        let (merge_state, checks) = parse_checks_json(&stdout);
        branch_sync_checks_query_from_parsed(&merge_state, &checks)
    }

    fn is_draft(&self, pr: i64, repo_dir: &Path) -> bool {
        let pr_str = pr.to_string();
        let cmd = self.build_gh_cmd(
            &[
                "pr", "view", &pr_str, "--json", "isDraft", "--jq", ".isDraft",
            ],
            repo_dir,
        );
        match cmd.map(|mut cmd| cmd.output()) {
            Ok(Ok(o)) if o.status.success() => parse_is_draft(&String::from_utf8_lossy(&o.stdout)),
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

fn parse_pr_head_ref(output: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(output)
        .ok()?
        .get("headRefName")?
        .as_str()
        .filter(|branch| !branch.is_empty())
        .map(str::to_owned)
}

fn parse_repository_default_branch(output: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(output)
        .ok()?
        .get("defaultBranchRef")?
        .get("name")?
        .as_str()
        .filter(|branch| !branch.is_empty())
        .map(str::to_owned)
}

#[derive(Debug)]
struct MergeBranchMetadata {
    head_ref: String,
    pr_base_branch: String,
    default_branch: String,
}

fn merge_branch_metadata(
    head_ref: Option<String>,
    pr_base_branch: Option<String>,
    default_branch: Option<String>,
) -> std::result::Result<MergeBranchMetadata, MergeResult> {
    let head_ref = head_ref.ok_or_else(|| MergeResult {
        success: false,
        message: "PR head branch could not be resolved; merge not attempted".into(),
        failure_kind: Some(MergeFailureKind::PolicyBlocked),
    })?;
    let pr_base_branch = pr_base_branch.ok_or_else(|| MergeResult {
        success: false,
        message: "PR base branch could not be resolved; merge not attempted".into(),
        failure_kind: Some(MergeFailureKind::PolicyBlocked),
    })?;
    let default_branch = default_branch.ok_or_else(|| MergeResult {
        success: false,
        message: "repository default branch could not be resolved; merge not attempted".into(),
        failure_kind: Some(MergeFailureKind::PolicyBlocked),
    })?;

    Ok(MergeBranchMetadata {
        head_ref,
        pr_base_branch,
        default_branch,
    })
}

/// One-identity park reason: branch protection needs a person's approval the
/// merge identity (the PR author) cannot give.
fn self_approval_required(pr: i64, head_sha: &str) -> MergeResult {
    MergeResult {
        success: false,
        message: format!(
            "PR #{pr}: branch protection requires an approving review from someone \
             other than the pull request author; the merge identity authored this PR \
             and cannot approve it. A person must approve head {head_sha}; merge not attempted"
        ),
        failure_kind: Some(MergeFailureKind::PolicyBlocked),
    }
}

fn merge_command_args<'a>(
    pr: &'a str,
    expected_head_sha: &'a str,
    head_ref: &str,
    protected_branches: [&str; 4],
) -> Vec<&'a str> {
    let delete_branch = protected_branches
        .iter()
        .all(|protected| head_ref != *protected);

    let mut args = vec!["pr", "merge", pr, "--merge"];
    if delete_branch {
        args.push("--delete-branch");
    }
    args.extend(["--match-head-commit", expected_head_sha]);
    args
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

/// Fixed GraphQL document for the merge-credential identity probe. Only its
/// variables carry the daemon-derived repository and PR number.
const PR_IDENTITY_QUERY: &str = "query($owner: String!, $name: String!, $number: Int!) { \
repository(owner: $owner, name: $name) { pullRequest(number: $number) { \
viewerDidAuthor reviewDecision } } }";

/// Identity-probe result for one PR, as seen by the merge credential.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrIdentity {
    /// The merge credential's identity authored the PR (one-identity mode).
    pub viewer_did_author: bool,
    /// `reviewDecision`; `None` when GitHub reports `null`.
    pub review_decision: Option<String>,
}

/// Parse the identity-probe response. `None` on malformed JSON, a GraphQL
/// `errors` entry, a missing `pullRequest`, a non-boolean/missing
/// `viewerDidAuthor`, or a missing/non-string `reviewDecision` key, so no
/// partial response ever selects one-identity mode.
fn parse_pr_identity(json_str: &str) -> Option<PrIdentity> {
    let val: serde_json::Value = serde_json::from_str(json_str).ok()?;
    match val.get("errors") {
        None | Some(serde_json::Value::Null) => {}
        Some(serde_json::Value::Array(errors)) if errors.is_empty() => {}
        Some(_) => return None,
    }
    let pull = val
        .get("data")?
        .get("repository")?
        .get("pullRequest")?
        .as_object()?;
    let viewer_did_author = pull.get("viewerDidAuthor")?.as_bool()?;
    let review_decision = match pull.get("reviewDecision")? {
        serde_json::Value::Null => None,
        serde_json::Value::String(decision) => Some(decision.clone()),
        _ => return None,
    };
    Some(PrIdentity {
        viewer_did_author,
        review_decision,
    })
}

/// True only for a well-formed probe that reports `viewerDidAuthor=true`.
fn is_one_identity(identity: Option<&PrIdentity>) -> bool {
    identity.is_some_and(|identity| identity.viewer_did_author)
}

/// Parse `gh pr view --json reviewDecision`. `null` reads as `""`;
/// malformed JSON or a missing/non-string field is `None`.
fn parse_review_decision(json_str: &str) -> Option<String> {
    match serde_json::from_str::<serde_json::Value>(json_str)
        .ok()?
        .get("reviewDecision")?
    {
        serde_json::Value::Null => Some(String::new()),
        serde_json::Value::String(decision) => Some(decision.clone()),
        _ => None,
    }
}

/// Verdict kinds the daemon marks on a review body for duplicate
/// suppression. The marker never feeds a merge or lifecycle decision.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerdictKind {
    Approved,
    Changes,
}

/// Hidden marker binding a verdict kind to one head SHA, e.g.
/// `<!-- quorum-verdict:approved head=<sha> -->`.
fn verdict_marker(kind: VerdictKind, head_sha: &str) -> String {
    let kind = match kind {
        VerdictKind::Approved => "approved",
        VerdictKind::Changes => "changes",
    };
    format!("<!-- quorum-verdict:{kind} head={head_sha} -->")
}

/// True iff a review in `reviews_json` (from `fetch_reviews_and_head`) whose
/// `commit.oid` is `head_sha` carries the exact marker for `kind` and
/// `head_sha`. Parse failure or an empty head is false. Dedupe only.
fn head_has_verdict_marker(reviews_json: &str, head_sha: &str, kind: VerdictKind) -> bool {
    if head_sha.is_empty() {
        return false;
    }
    let Ok(val) = serde_json::from_str::<serde_json::Value>(reviews_json) else {
        return false;
    };
    let Some(reviews) = val.get("reviews").and_then(|v| v.as_array()) else {
        return false;
    };
    let marker = verdict_marker(kind, head_sha);
    reviews.iter().any(|r| {
        r.get("commit")
            .and_then(|c| c.get("oid"))
            .and_then(|o| o.as_str())
            == Some(head_sha)
            && r.get("body")
                .and_then(|b| b.as_str())
                .is_some_and(|body| body.contains(&marker))
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

fn parse_head_ref_oid(json_str: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json_str)
        .ok()?
        .get("headRefOid")?
        .as_str()
        .map(str::trim)
        .filter(|sha| !sha.is_empty())
        .map(str::to_string)
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

fn parse_merge_commit_status(json_str: &str) -> MergeCommitStatus {
    let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) else {
        return MergeCommitStatus::Unknown;
    };
    match json.get("state").and_then(serde_json::Value::as_str) {
        Some("MERGED") => MergeCommitStatus::Merged {
            merge_commit_sha: json
                .get("mergeCommit")
                .and_then(|value| value.get("oid"))
                .and_then(serde_json::Value::as_str)
                .filter(|sha| !sha.is_empty() && !sha.contains('\0'))
                .map(str::to_owned),
        },
        Some("OPEN") => MergeCommitStatus::Open,
        Some("CLOSED") => MergeCommitStatus::Closed,
        _ => MergeCommitStatus::Unknown,
    }
}

/// Parse the `--jq .isDraft` output of `gh pr view` (bare `true`/`false`).
/// Anything that isn't exactly `true` is treated as not-draft (fail-safe:
/// a query hiccup skips the undraft and behaves as it did before this guard).
fn parse_is_draft(jq_out: &str) -> bool {
    jq_out.trim() == "true"
}

impl MergeExecutor for GhMergeExecutor {
    fn github_credentials(&self) -> Arc<dyn GithubCredentialSource> {
        Arc::clone(&self.credentials)
    }

    fn publication_credentials(&self) -> Arc<dyn GithubCredentialSource> {
        Arc::clone(&self.publication_credentials)
    }

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

        // Probe once, before any review write. Only a well-formed
        // `viewerDidAuthor=true` selects one-identity mode; every probe failure
        // keeps the two-identity `--approve` path.
        let one_identity = is_one_identity(self.probe_pr_identity(pr, repo_dir).as_ref());

        // #49: on a PolicyPending retry the daemon calls merge() again — without
        // dedupe every retry posts another `--approve` stub, cluttering the PR
        // review history (4 identical approvals in 7s observed on task #45).
        // Skip the approve step if an APPROVED review already exists on the
        // current head SHA (one-identity: a review on the current head carrying
        // the approved marker). Fail-open: query hiccup falls through to the
        // review call and behaves as before this guard.
        let already_approved = self
            .fetch_reviews_and_head(pr, repo_dir)
            .map(|(reviews_json, head)| {
                if one_identity {
                    head_has_verdict_marker(&reviews_json, &head, VerdictKind::Approved)
                } else {
                    head_has_approval(&reviews_json, &head)
                }
            })
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

        if one_identity {
            // GitHub refuses a self-approval, so the merge identity posts its
            // verdict as a COMMENT. The marker only suppresses a duplicate
            // comment; it never stands in for an approval.
            if !already_approved {
                let comment_body = format!(
                    "Approved verdict — per {} review verdict (task #{}). \
                     The merge identity authored this PR, so this is a comment, \
                     not a formal approval (daemon model).\n\n{}",
                    ctx.reviewer_name,
                    ctx.review_task_id,
                    verdict_marker(VerdictKind::Approved, &ctx.expected_head_sha),
                );
                let comment = self.run_gh(
                    &[
                        "pr",
                        "review",
                        &pr_str,
                        "--comment",
                        "--body",
                        &comment_body,
                    ],
                    repo_dir,
                );
                if !comment.success {
                    return MergeResult {
                        success: false,
                        message: format!(
                            "verdict comment failed (merge not attempted): {}",
                            comment.message
                        ),
                        failure_kind: Some(MergeFailureKind::PolicyBlocked),
                    };
                }
            }
            if self.review_required(pr, repo_dir) {
                return self_approval_required(pr, &ctx.expected_head_sha);
            }
        } else if !already_approved {
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

        let branch_metadata = match merge_branch_metadata(
            self.live_head_ref(pr, repo_dir),
            self.live_base_branch(pr, repo_dir).ok(),
            self.repository_default_branch(repo_dir),
        ) {
            Ok(metadata) => metadata,
            Err(result) => return result,
        };
        let result = self.run_gh(
            &merge_command_args(
                &pr_str,
                &ctx.expected_head_sha,
                &branch_metadata.head_ref,
                [
                    &self.base_branch,
                    &self.self_update_branch,
                    &branch_metadata.pr_base_branch,
                    &branch_metadata.default_branch,
                ],
            ),
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
        if !result.success && one_identity && self.review_required(pr, repo_dir) {
            return self_approval_required(pr, &ctx.expected_head_sha);
        }

        result
    }

    fn merge_without_approval(&self, pr: i64, repo_dir: &Path, ctx: &MergeContext) -> MergeResult {
        let pr_str = pr.to_string();

        // Branch syncs have no reviewer whose verdict can authorize a formal
        // approval, but their deterministic PR still gets the same base/head
        // drift protection immediately before the GitHub write.
        if let Some(rejected) = self.reject_base_drift(pr, repo_dir, &ctx.expected_base_branch) {
            return rejected;
        }
        if let Some(rejected) = self.reject_head_drift(pr, repo_dir, &ctx.expected_head_sha) {
            return rejected;
        }

        let branch_metadata = match merge_branch_metadata(
            self.live_head_ref(pr, repo_dir),
            self.live_base_branch(pr, repo_dir).ok(),
            self.repository_default_branch(repo_dir),
        ) {
            Ok(metadata) => metadata,
            Err(result) => return result,
        };

        // Close the metadata-query window before the irreversible merge.
        if let Some(rejected) = self.reject_base_drift(pr, repo_dir, &ctx.expected_base_branch) {
            return rejected;
        }
        if let Some(rejected) = self.reject_head_drift(pr, repo_dir, &ctx.expected_head_sha) {
            return rejected;
        }

        let result = self.run_gh(
            &merge_command_args(
                &pr_str,
                &ctx.expected_head_sha,
                &branch_metadata.head_ref,
                [
                    &self.base_branch,
                    &self.self_update_branch,
                    &branch_metadata.pr_base_branch,
                    &branch_metadata.default_branch,
                ],
            ),
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
        let mut cmd = self
            .build_gh_cmd(&["pr", "view", &pr, "--json", "mergeCommit"], repo_dir)
            .ok()?;
        let output = cmd.output().ok()?;
        if !output.status.success() {
            return None;
        }
        parse_merge_commit_sha(&String::from_utf8_lossy(&output.stdout))
    }

    fn merge_commit_status(&self, pr: i64, repo_dir: &Path) -> MergeCommitStatus {
        let pr = pr.to_string();
        let cmd = self.build_gh_cmd(
            &["pr", "view", &pr, "--json", "state,mergeCommit"],
            repo_dir,
        );
        let Ok(mut cmd) = cmd else {
            return MergeCommitStatus::Unknown;
        };
        let Ok(output) = cmd.output() else {
            return MergeCommitStatus::Unknown;
        };
        if !output.status.success() {
            return MergeCommitStatus::Unknown;
        }
        parse_merge_commit_status(&String::from_utf8_lossy(&output.stdout))
    }

    fn wait_for_checks(
        &self,
        pr: i64,
        repo_dir: &Path,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> ChecksOutcome {
        self.poll_checks(pr, repo_dir, None, timeout_secs, poll_interval_secs)
    }

    fn wait_for_checks_at_head(
        &self,
        pr: i64,
        repo_dir: &Path,
        expected_head: &str,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> ChecksOutcome {
        self.poll_checks(
            pr,
            repo_dir,
            Some(expected_head),
            timeout_secs,
            poll_interval_secs,
        )
    }

    fn wait_for_branch_sync_checks(
        &self,
        pr: i64,
        repo_dir: &Path,
        timeout_secs: u64,
        poll_interval_secs: u64,
    ) -> ChecksOutcome {
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            if let Some(outcome) =
                branch_sync_checks_query_outcome(self.query_branch_sync_checks(pr, repo_dir))
            {
                return outcome;
            }
            if Instant::now() + Duration::from_secs(poll_interval_secs) > deadline {
                return ChecksOutcome::TimedOut;
            }
            std::thread::sleep(Duration::from_secs(poll_interval_secs));
        }
    }

    fn pending_check_names(&self, pr: i64, repo_dir: &Path) -> Vec<String> {
        let pr_str = pr.to_string();
        let cmd = self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "statusCheckRollup"],
            repo_dir,
        );
        let output = match cmd.map(|mut cmd| cmd.output()) {
            Ok(Ok(output)) if output.status.success() => output,
            _ => return Vec::new(),
        };
        pending_check_names_from_json(&String::from_utf8_lossy(&output.stdout))
    }

    fn check_mergeability(&self, pr: i64, repo_dir: &Path) -> MergeabilityState {
        let pr_str = pr.to_string();
        let cmd = self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "mergeStateStatus,state"],
            repo_dir,
        );
        let output = match cmd.map(|mut cmd| cmd.output()) {
            Ok(Ok(o)) if o.status.success() => o,
            _ => return MergeabilityState::Mergeable,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_mergeability(&stdout)
    }

    fn check_mergeability_with_head(
        &self,
        pr: i64,
        repo_dir: &Path,
    ) -> (MergeabilityState, Option<String>) {
        let pr_str = pr.to_string();
        let cmd = self.build_gh_cmd(
            &[
                "pr",
                "view",
                &pr_str,
                "--json",
                "mergeStateStatus,state,headRefOid",
            ],
            repo_dir,
        );
        let output = match cmd.map(|mut cmd| cmd.output()) {
            Ok(Ok(o)) if o.status.success() => o,
            _ => return (MergeabilityState::Mergeable, None),
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        (parse_mergeability(&stdout), parse_head_ref_oid(&stdout))
    }

    fn head_sha(&self, pr: i64, repo_dir: &Path) -> Option<String> {
        let pr_str = pr.to_string();
        let mut cmd = self
            .build_gh_cmd(
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
            )
            .ok()?;
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
        self.query_required_jobs(pr, repo_dir, None, required_jobs)
    }

    fn check_required_jobs_at_head(
        &self,
        pr: i64,
        repo_dir: &Path,
        expected_head: &str,
        required_jobs: &[String],
    ) -> RequiredJobsOutcome {
        self.query_required_jobs(pr, repo_dir, Some(expected_head), required_jobs)
    }

    fn check_default_branch_ci(&self, repo_dir: &Path, branch: &str) -> DefaultBranchStatus {
        let cmd = self.build_gh_cmd(
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
        let output = match cmd.map(|mut cmd| cmd.output()) {
            Ok(Ok(o)) if o.status.success() => o,
            _ => return DefaultBranchStatus::Unknown,
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        parse_default_branch_ci(&stdout)
    }

    fn ensure_changes_requested(&self, pr: i64, repo_dir: &Path, body: &str) -> bool {
        let pr_str = pr.to_string();
        // One identity: GitHub refuses --request-changes on your own PR, so
        // post a COMMENT carrying the head-bound changes marker instead. A
        // failed probe falls through to the two-identity path below.
        if is_one_identity(self.probe_pr_identity(pr, repo_dir).as_ref()) {
            return self.ensure_changes_comment(pr, repo_dir, body);
        }
        let post = || {
            self.build_gh_cmd(
                &["pr", "review", &pr_str, "--request-changes", "--body", body],
                repo_dir,
            )
            .is_ok_and(|mut cmd| cmd.output().is_ok_and(|output| output.status.success()))
        };
        let cmd = self.build_gh_cmd(
            &["pr", "view", &pr_str, "--json", "reviewDecision"],
            repo_dir,
        );
        let output = match cmd.map(|mut cmd| cmd.output()) {
            Ok(Ok(o)) if o.status.success() => o,
            // Can't check — post defensively.
            _ => return post(),
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.contains("CHANGES_REQUESTED") {
            true
        } else {
            post()
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

    fn merge_commit_sha(&self, _pr: i64, repo_dir: &Path) -> Option<String> {
        // `--merge-cmd` is a hidden local-test seam rather than a GitHub
        // executor. Its successful command has no GraphQL mergeCommit to
        // query, so use the immutable repository object the fixture exposes
        // as its simulated merge witness. Production uses GhMergeExecutor and
        // must still obtain GitHub's real mergeCommit above.
        let output = quorum_core::git::git_command("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo_dir)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
        (!sha.is_empty()).then_some(sha)
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
        let output = quorum_core::git::git_command("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo_dir)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_OPTIONAL_LOCKS", "0")
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
    use crate::github_credentials::{FakeCredentialSource, GithubToken};

    fn checks_snapshot(head: &str, conclusion: &str) -> String {
        format!(
            r#"{{"headRefOid":"{head}","mergeStateStatus":"BLOCKED","statusCheckRollup":[{{"__typename":"CheckRun","name":"container","status":"COMPLETED","conclusion":"{conclusion}"}}]}}"#
        )
    }

    #[test]
    fn exact_head_checks_treat_another_heads_failure_as_pending() {
        // #402: right after the daemon's push GitHub can still serve the old
        // head's failed rollup. It must never fail the newly published head.
        let stale = checks_snapshot("old-head", "FAILURE");
        assert_eq!(
            checks_query_for_head(&stale, Some("new-head")),
            ChecksQueryResult::Pending
        );
        assert_eq!(
            checks_query_for_head(&stale, Some("old-head")),
            ChecksQueryResult::SomeFailed(vec!["container".into()])
        );
        // A snapshot without any head attribution is not evidence either.
        let unattributed = r#"{"mergeStateStatus":"CLEAN","statusCheckRollup":[]}"#;
        assert_eq!(
            checks_query_for_head(unattributed, Some("new-head")),
            ChecksQueryResult::Pending
        );
        // Unattributed waits keep their established meaning.
        assert_eq!(
            checks_query_for_head(&stale, None),
            ChecksQueryResult::SomeFailed(vec!["container".into()])
        );
    }

    #[test]
    fn exact_head_required_jobs_treat_another_heads_rollup_as_pending() {
        let jobs = vec!["container".to_string()];
        let stale = checks_snapshot("old-head", "FAILURE");
        assert!(matches!(
            required_jobs_for_head(&stale, Some("new-head"), &jobs),
            RequiredJobsOutcome::Pending { .. }
        ));
        assert!(matches!(
            required_jobs_for_head(&stale, Some("old-head"), &jobs),
            RequiredJobsOutcome::NotReady { .. }
        ));
        let current = checks_snapshot("new-head", "SUCCESS");
        assert_eq!(
            required_jobs_for_head(&current, Some("new-head"), &jobs),
            RequiredJobsOutcome::AllSucceeded
        );
    }

    fn test_ctx() -> MergeContext {
        MergeContext {
            reviewer_name: "Rev-1".into(),
            review_task_id: 99,
            expected_base_branch: "main".into(),
            expected_head_sha: "abc123".into(),
        }
    }

    #[test]
    fn branch_sync_empty_rollup_is_not_ready() {
        let (merge_state, checks) =
            parse_checks_json(r#"{"mergeStateStatus":"CLEAN","statusCheckRollup":[]}"#);
        assert_eq!(
            branch_sync_checks_query_outcome(branch_sync_checks_query_from_parsed(
                &merge_state,
                &checks,
            )),
            None
        );
        assert_eq!(
            branch_sync_checks_query_outcome(ChecksQueryResult::Pending),
            None
        );
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
    fn production_merge_command_deletes_unprotected_daemon_head() {
        assert_eq!(
            merge_command_args(
                "42",
                "approved-sha",
                "daemon/worker-t1",
                ["configured-base", "self-update", "pr-base", "default"],
            ),
            vec![
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
    fn production_merge_command_retains_protected_heads() {
        for (protected_head, label) in [
            ("configured-base", "configured base branch"),
            ("self-update", "self-update branch"),
            ("pr-base", "PR base branch"),
            ("default", "repository default branch"),
        ] {
            assert_eq!(
                merge_command_args(
                    "42",
                    "approved-sha",
                    protected_head,
                    ["configured-base", "self-update", "pr-base", "default"],
                ),
                vec![
                    "pr",
                    "merge",
                    "42",
                    "--merge",
                    "--match-head-commit",
                    "approved-sha"
                ],
                "{label} must not be deleted",
            );
        }
    }

    #[test]
    fn branch_metadata_lookup_failures_block_merge_before_mutation() {
        for (head_ref, pr_base_branch, default_branch, missing) in [
            (
                None,
                Some("pr-base".into()),
                Some("default".into()),
                "PR head branch",
            ),
            (
                Some("head".into()),
                None,
                Some("default".into()),
                "PR base branch",
            ),
            (
                Some("head".into()),
                Some("pr-base".into()),
                None,
                "repository default branch",
            ),
        ] {
            let result = merge_branch_metadata(head_ref, pr_base_branch, default_branch)
                .expect_err("missing branch metadata must block the merge");
            assert!(!result.success);
            assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
            assert!(result.message.contains(missing));
            assert!(result.message.contains("merge not attempted"));
        }
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
        // A configured-but-missing token file is a typed credential failure,
        // never a silent fallback to ambient gh auth.
        let exec = gh_exec(
            Arc::new(LocalDefaultCredentialSource::new(Some(
                std::path::PathBuf::from("/nonexistent/token"),
            ))),
            None,
        );
        let result = exec.merge(1, Path::new("/tmp"), &ctx);
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
        assert!(
            result.message.contains("base could not be validated")
                && result.message.contains("approval and merge not attempted")
                && result.message.contains("GitHub token file"),
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
        assert_eq!(
            parse_mergeability(r#"{"mergeStateStatus":"UNKNOWN"}"#),
            MergeabilityState::Mergeable,
            "UNKNOWN just after a push is never a conflict"
        );
        assert_eq!(
            parse_mergeability(r#"{"mergeStateStatus":"UNSTABLE"}"#),
            MergeabilityState::Mergeable
        );
        assert_eq!(parse_head_ref_oid("not json"), None);
        assert_eq!(parse_head_ref_oid(r#"{"headRefOid":""}"#), None);
        assert_eq!(
            parse_head_ref_oid(r#"{"mergeStateStatus":"DIRTY","headRefOid":"8aadb5bc"}"#),
            Some("8aadb5bc".to_string())
        );
    }

    #[test]
    fn parse_mergeability_missing_field() {
        assert_eq!(parse_mergeability(r#"{}"#), MergeabilityState::Mergeable,);
    }

    fn gh_exec(
        credentials: Arc<dyn GithubCredentialSource>,
        gh_repo: Option<String>,
    ) -> GhMergeExecutor {
        GhMergeExecutor {
            credentials,
            publication_credentials: Arc::new(FakeCredentialSource::token("publication-fixture")),
            gh_repo,
            base_branch: "main".into(),
            self_update_branch: "main".into(),
            gh_program: PathBuf::from("gh"),
        }
    }

    fn fake_exec(secret: &str) -> (Arc<FakeCredentialSource>, GhMergeExecutor) {
        let source = Arc::new(FakeCredentialSource::token(secret));
        let exec = gh_exec(source.clone(), None);
        (source, exec)
    }

    fn gh_token_env(cmd: &Command) -> Option<String> {
        cmd.get_envs()
            .find(|(key, _)| *key == std::ffi::OsStr::new("GH_TOKEN"))
            .and_then(|(_, value)| value)
            .map(|value| value.to_string_lossy().into_owned())
    }

    fn argv(cmd: &Command) -> Vec<String> {
        cmd.get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn gh_commands_fail_before_spawn_on_missing_token_file() {
        let exec = gh_exec(
            Arc::new(LocalDefaultCredentialSource::new(Some(
                std::path::PathBuf::from("/nonexistent/path/token"),
            ))),
            None,
        );
        assert!(matches!(
            exec.build_gh_cmd(&["pr", "view", "1"], Path::new("/tmp")),
            Err(GithubCredentialError::TokenFileUnreadable { .. })
        ));
        assert!(matches!(
            exec.repo_view_default_branch_cmd(Path::new("/tmp")),
            Err(GithubCredentialError::TokenFileUnreadable { .. })
        ));
    }

    #[test]
    fn gh_commands_read_and_trim_token_file_at_call_time() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "  my-secret-token \n").unwrap();
        let exec = gh_exec(
            Arc::new(LocalDefaultCredentialSource::new(Some(token_path.clone()))),
            None,
        );
        let cmd = exec
            .build_gh_cmd(&["pr", "view", "1"], Path::new("/tmp"))
            .unwrap();
        assert_eq!(gh_token_env(&cmd).as_deref(), Some("my-secret-token"));

        std::fs::write(&token_path, "rotated-token\n").unwrap();
        let cmd = exec
            .repo_view_default_branch_cmd(Path::new("/tmp"))
            .unwrap();
        assert_eq!(gh_token_env(&cmd).as_deref(), Some("rotated-token"));
    }

    #[test]
    fn gh_commands_get_gh_token_from_injected_source_per_call() {
        const SECRET: &str = "ghs_injected_secret_value";
        let (source, exec) = fake_exec(SECRET);
        let pr_cmd = exec
            .build_gh_cmd(&["pr", "view", "1"], Path::new("/tmp"))
            .unwrap();
        let repo_cmd = exec
            .repo_view_default_branch_cmd(Path::new("/tmp"))
            .unwrap();
        assert_eq!(source.calls(), 2, "token is fetched once per gh child");
        for cmd in [&pr_cmd, &repo_cmd] {
            assert_eq!(gh_token_env(cmd).as_deref(), Some(SECRET));
            assert!(
                !argv(cmd).iter().any(|arg| arg.contains(SECRET)),
                "token must never enter argv"
            );
        }
    }

    #[test]
    fn failing_source_yields_classified_failure_without_spawning_gh() {
        const SECRET: &str = "ghs_never_rendered_secret";
        let source = Arc::new(FakeCredentialSource::failing(
            GithubCredentialError::LookupTimedOut {
                timeout: Duration::from_secs(10),
            },
        ));
        // A slug keeps every builder independent of cwd; the failure must
        // surface before any gh child exists.
        let exec = gh_exec(source.clone(), Some("owner/repo".into()));
        assert!(matches!(
            exec.build_gh_cmd(&["pr", "view", "1"], Path::new("/tmp")),
            Err(GithubCredentialError::LookupTimedOut { .. })
        ));
        assert!(matches!(
            exec.repo_view_default_branch_cmd(Path::new("/tmp")),
            Err(GithubCredentialError::LookupTimedOut { .. })
        ));

        let run = exec.run_gh(&["pr", "ready", "1"], Path::new("/tmp"));
        assert!(!run.success);
        assert_eq!(run.failure_kind, Some(MergeFailureKind::PolicyBlocked));
        assert!(run.message.contains("gh not run"), "{}", run.message);

        let ctx = test_ctx();
        for result in [
            exec.merge(1, Path::new("/tmp"), &ctx),
            exec.merge_without_approval(1, Path::new("/tmp"), &ctx),
        ] {
            assert!(!result.success);
            assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
            assert!(
                result.message.contains("base could not be validated")
                    && result.message.contains("timed out"),
                "{}",
                result.message
            );
            assert!(!result.message.contains(SECRET));
        }
        assert!(exec.live_head_sha(1, Path::new("/tmp")).is_err());
        assert_eq!(exec.live_head_ref(1, Path::new("/tmp")), None);
        assert_eq!(exec.repository_default_branch(Path::new("/tmp")), None);
        assert_eq!(exec.merge_commit_sha(1, Path::new("/tmp")), None);
        assert_eq!(
            exec.merge_commit_status(1, Path::new("/tmp")),
            MergeCommitStatus::Unknown
        );
        assert_eq!(exec.head_sha(1, Path::new("/tmp")), None);
        assert!(!exec.ensure_changes_requested(1, Path::new("/tmp"), "body"));
        assert_eq!(
            exec.check_default_branch_ci(Path::new("/tmp"), "main"),
            DefaultBranchStatus::Unknown
        );
        assert_eq!(
            exec.check_required_jobs(1, Path::new("/tmp"), &["ci".to_string()]),
            RequiredJobsOutcome::NotReady {
                issues: vec![("ci".into(), "query failed".into())]
            }
        );
        assert!(source.calls() > 0);
    }

    #[test]
    fn merge_failure_messages_never_contain_the_token() {
        const SECRET: &str = "ghp_file_token_should_stay_hidden";
        let dir = tempfile::tempdir().unwrap();
        // An empty file is a typed failure; then a source that does yield the
        // token must still keep it out of every rendered failure.
        let empty = dir.path().join("empty");
        std::fs::write(&empty, "\n").unwrap();
        let exec = gh_exec(
            Arc::new(LocalDefaultCredentialSource::new(Some(empty))),
            Some("owner/repo".into()),
        );
        let result = exec.run_gh(&["pr", "view", "1"], Path::new("/tmp"));
        assert!(result.message.contains("is empty"), "{}", result.message);

        let error = GithubCredentialError::LookupFailed {
            reason: "gh exited with exit status: 1".into(),
        };
        let rendered = credential_failure(&error);
        assert!(!rendered.message.contains(SECRET));
        assert!(!format!("{rendered:?}").contains(SECRET));
    }

    #[test]
    fn merge_executor_exposes_its_credential_source() {
        const SECRET: &str = "ghs_shared_source";
        let (source, exec) = fake_exec(SECRET);
        let shared = MergeExecutor::github_credentials(&exec);
        assert_eq!(shared.token().unwrap().expose_secret(), SECRET);
        assert_eq!(
            source.calls(),
            1,
            "the executor hands out its injected source"
        );

        // Other implementors inherit the default without code changes; in
        // test builds that default is hermetic (no ambient `gh auth`).
        let command = CommandMergeExecutor {
            command: "true".into(),
            checks_cmd: None,
            mergeability_cmd: None,
        };
        let default: Arc<dyn GithubCredentialSource> = command.github_credentials();
        assert_eq!(
            default.token().unwrap().expose_secret(),
            "test-fixture-token"
        );
    }

    fn repo_view_argv(exec: &GhMergeExecutor, repo_dir: &Path) -> Vec<String> {
        let cmd = exec.repo_view_default_branch_cmd(repo_dir).unwrap();
        cmd.get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    /// Regression: `gh repo view` takes the repo positionally and rejects `-R`
    /// (`unknown shorthand flag: 'R' in -R`). Routing it through `build_gh_cmd`
    /// appended `-R <nwo>`, so the lookup always failed and the fail-closed gate
    /// parked every merge. The argv must pass the slug positionally, never via -R.
    #[test]
    fn repo_view_default_branch_passes_slug_positionally_never_with_r_flag() {
        let (_, mut exec) = fake_exec("t");
        exec.gh_repo = Some("owner/repo".into());
        let cmd = exec
            .repo_view_default_branch_cmd(Path::new("/tmp"))
            .unwrap();
        assert_eq!(cmd.get_program().to_string_lossy(), "gh");

        let argv = repo_view_argv(&exec, Path::new("/tmp"));
        assert_eq!(
            argv,
            vec!["repo", "view", "owner/repo", "--json", "defaultBranchRef"],
        );
        assert!(
            !argv.iter().any(|a| a == "-R"),
            "gh repo view must not receive -R: {argv:?}"
        );
    }

    /// With no configured slug the command falls back to running in `repo_dir`
    /// (no positional slug), matching `build_gh_cmd`'s cwd fallback.
    #[test]
    fn repo_view_default_branch_falls_back_to_cwd_without_slug() {
        let (_, exec) = fake_exec("t");
        let repo_dir = Path::new("/tmp/some-repo");
        let cmd = exec.repo_view_default_branch_cmd(repo_dir).unwrap();
        assert_eq!(
            cmd.get_current_dir(),
            Some(repo_dir),
            "no-slug lookup must run in repo_dir",
        );

        let argv = repo_view_argv(&exec, repo_dir);
        assert_eq!(argv, vec!["repo", "view", "--json", "defaultBranchRef"]);
        assert!(!argv.iter().any(|a| a == "-R"));
    }

    #[test]
    fn gh_commands_disable_git_prompts_and_optional_locks() {
        let (_, exec) = fake_exec("t");
        for cmd in [
            exec.build_gh_cmd(&["pr", "view", "1"], Path::new("/tmp"))
                .unwrap(),
            exec.repo_view_default_branch_cmd(Path::new("/tmp"))
                .unwrap(),
        ] {
            let env: std::collections::HashMap<_, _> = cmd
                .get_envs()
                .filter_map(|(key, value)| value.map(|value| (key, value)))
                .collect();
            assert_eq!(
                env.get(std::ffi::OsStr::new("GIT_TERMINAL_PROMPT")),
                Some(&std::ffi::OsStr::new("0"))
            );
            assert_eq!(
                env.get(std::ffi::OsStr::new("GIT_OPTIONAL_LOCKS")),
                Some(&std::ffi::OsStr::new("0"))
            );
        }
    }

    fn gh_available() -> bool {
        std::process::Command::new("gh")
            .args(["auth", "status"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    /// Real-binary contract test: spawns the installed, authed `gh` against a
    /// configured slug and asserts a default branch is actually resolved. This
    /// is the boundary the parser-only tests could not see — the old `-R` argv
    /// failed here at runtime. Skipped when `gh` is absent or unauthenticated.
    #[test]
    fn repo_view_default_branch_resolves_against_real_gh() {
        if !gh_available() {
            eprintln!("skipping: gh not available/authenticated");
            return;
        }
        // A configured slug (`gh_repo = Some`) is the exact case the old `-R` argv
        // broke: `gh repo view -R <slug>` errored, so the lookup returned None.
        let exec = gh_exec(
            Arc::new(LocalDefaultCredentialSource::new(None)),
            Some("cli/cli".into()),
        );
        let branch = exec.repository_default_branch(Path::new("."));
        assert!(
            branch.as_deref().is_some_and(|b| !b.is_empty()),
            "gh repo view must resolve a non-empty default branch for a configured slug, got {branch:?}"
        );
    }

    // ── Identity probe, reviewDecision, verdict markers ─────────────────

    fn probe_json(viewer_did_author: &str, review_decision: &str) -> String {
        format!(
            r#"{{"data":{{"repository":{{"pullRequest":{{"viewerDidAuthor":{viewer_did_author},"reviewDecision":{review_decision}}}}}}}}}"#
        )
    }

    #[test]
    fn parse_pr_identity_true_selects_one_identity() {
        let parsed = parse_pr_identity(&probe_json("true", r#""APPROVED""#));
        assert_eq!(
            parsed,
            Some(PrIdentity {
                viewer_did_author: true,
                review_decision: Some("APPROVED".into()),
            })
        );
        assert!(is_one_identity(parsed.as_ref()));
        let null_decision = parse_pr_identity(&probe_json("true", "null")).unwrap();
        assert_eq!(null_decision.review_decision, None);
        assert!(is_one_identity(Some(&null_decision)));
    }

    #[test]
    fn parse_pr_identity_false_selects_two_identity() {
        let parsed = parse_pr_identity(&probe_json("false", r#""REVIEW_REQUIRED""#));
        assert_eq!(
            parsed.as_ref().map(|p| p.viewer_did_author),
            Some(false),
            "{parsed:?}"
        );
        assert!(!is_one_identity(parsed.as_ref()));
    }

    #[test]
    fn parse_pr_identity_errors_entry_is_not_one_identity() {
        // A GraphQL error alongside otherwise-valid data must not count.
        let with_errors = r#"{"data":{"repository":{"pullRequest":{"viewerDidAuthor":true,"reviewDecision":null}}},"errors":[{"message":"boom"}]}"#;
        assert_eq!(parse_pr_identity(with_errors), None);
        let errors_object = r#"{"data":{"repository":{"pullRequest":{"viewerDidAuthor":true,"reviewDecision":null}}},"errors":{"message":"boom"}}"#;
        assert_eq!(parse_pr_identity(errors_object), None);
        let errors_only = r#"{"data":null,"errors":[{"type":"NOT_FOUND"}]}"#;
        assert_eq!(parse_pr_identity(errors_only), None);
        assert!(!is_one_identity(parse_pr_identity(with_errors).as_ref()));
    }

    #[test]
    fn parse_pr_identity_malformed_or_missing_field_is_not_one_identity() {
        for body in [
            "",
            "not json",
            "true",
            r#"{}"#,
            r#"{"data":{}}"#,
            r#"{"data":{"repository":null}}"#,
            r#"{"data":{"repository":{"pullRequest":null}}}"#,
            r#"{"data":{"repository":{"pullRequest":{"reviewDecision":"APPROVED"}}}}"#,
            r#"{"data":{"repository":{"pullRequest":{"viewerDidAuthor":true}}}}"#,
            r#"{"data":{"repository":{"pullRequest":{"viewerDidAuthor":"true","reviewDecision":null}}}}"#,
            r#"{"data":{"repository":{"pullRequest":{"viewerDidAuthor":1,"reviewDecision":null}}}}"#,
            r#"{"data":{"repository":{"pullRequest":{"viewerDidAuthor":true,"reviewDecision":7}}}}"#,
        ] {
            let parsed = parse_pr_identity(body);
            assert_eq!(parsed, None, "{body}");
            assert!(!is_one_identity(parsed.as_ref()), "{body}");
        }
        assert!(!is_one_identity(None));
    }

    #[test]
    fn parse_review_decision_reads_string_null_and_rejects_malformed() {
        assert_eq!(
            parse_review_decision(r#"{"reviewDecision":"CHANGES_REQUESTED"}"#).as_deref(),
            Some("CHANGES_REQUESTED")
        );
        assert_eq!(
            parse_review_decision(r#"{"reviewDecision":""}"#).as_deref(),
            Some("")
        );
        assert_eq!(
            parse_review_decision(r#"{"reviewDecision":null}"#).as_deref(),
            Some("")
        );
        for body in ["", "nope", "{}", r#"{"reviewDecision":3}"#] {
            assert_eq!(parse_review_decision(body), None, "{body}");
        }
    }

    #[test]
    fn verdict_marker_binds_kind_and_head() {
        assert_eq!(
            verdict_marker(VerdictKind::Approved, "abc123"),
            "<!-- quorum-verdict:approved head=abc123 -->"
        );
        assert_eq!(
            verdict_marker(VerdictKind::Changes, "abc123"),
            "<!-- quorum-verdict:changes head=abc123 -->"
        );
    }

    fn reviews_with(entries: &[(&str, &str, &str)]) -> String {
        let reviews: Vec<serde_json::Value> = entries
            .iter()
            .map(|(state, oid, body)| {
                serde_json::json!({"state": state, "commit": {"oid": oid}, "body": body})
            })
            .collect();
        serde_json::json!({ "reviews": reviews, "headRefOid": "head1" }).to_string()
    }

    #[test]
    fn head_has_verdict_marker_matches_only_exact_kind_and_current_head() {
        let approved = verdict_marker(VerdictKind::Approved, "head1");
        let changes = verdict_marker(VerdictKind::Changes, "head1");
        let body = format!("Looks good.\n\n{approved}");
        let json = reviews_with(&[("COMMENTED", "head1", &body)]);
        assert!(head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Approved
        ));
        // The other verdict kind does not dedupe.
        assert!(!head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Changes
        ));
        let json = reviews_with(&[("CHANGES_REQUESTED", "head1", &changes)]);
        assert!(head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Changes
        ));
        assert!(!head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Approved
        ));
    }

    #[test]
    fn head_has_verdict_marker_rejects_other_head_and_missing_marker() {
        // Marker for an older head on an older-head review.
        let old = verdict_marker(VerdictKind::Approved, "head0");
        let json = reviews_with(&[("COMMENTED", "head0", &old)]);
        assert!(!head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Approved
        ));
        // Marker naming another head, carried by a current-head review.
        let json = reviews_with(&[("COMMENTED", "head1", &old)]);
        assert!(!head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Approved
        ));
        // A current-head marker carried by a review on another commit.
        let current = verdict_marker(VerdictKind::Approved, "head1");
        let json = reviews_with(&[("COMMENTED", "head0", &current)]);
        assert!(!head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Approved
        ));
        // A head that is a prefix of the marked head does not match.
        let long = verdict_marker(VerdictKind::Approved, "head10");
        let json = reviews_with(&[("COMMENTED", "head1", &long)]);
        assert!(!head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Approved
        ));
        // Reviews without the marker, malformed JSON, and an empty head.
        let json = reviews_with(&[("APPROVED", "head1", "LGTM")]);
        assert!(!head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Approved
        ));
        let no_body = r#"{"reviews":[{"state":"APPROVED","commit":{"oid":"head1"}}]}"#;
        assert!(!head_has_verdict_marker(
            no_body,
            "head1",
            VerdictKind::Approved
        ));
        assert!(!head_has_verdict_marker(
            "nope",
            "head1",
            VerdictKind::Approved
        ));
        let empty_head = verdict_marker(VerdictKind::Approved, "");
        let json = reviews_with(&[("COMMENTED", "", &empty_head)]);
        assert!(!head_has_verdict_marker(&json, "", VerdictKind::Approved));
    }

    /// Negative case 9: a COMMENTED review is never an approval, even when it
    /// carries the daemon's approved marker for the current head.
    #[test]
    fn head_has_approval_false_for_commented_review_with_marker() {
        let marker = verdict_marker(VerdictKind::Approved, "head1");
        let json = reviews_with(&[("COMMENTED", "head1", &marker)]);
        assert!(head_has_verdict_marker(
            &json,
            "head1",
            VerdictKind::Approved
        ));
        assert!(!head_has_approval(&json, "head1"));
        let json = reviews_with(&[("COMMENTED", "head1", "plain comment")]);
        assert!(!head_has_approval(&json, "head1"));
    }

    /// Reusable fake `gh`: records argv and `GH_TOKEN` per call and serves
    /// scripted stdout/stderr/exit per route (`probe`, `reviews`, `decision`,
    /// `review`, `merge`, `view`, `other`).
    #[cfg(unix)]
    struct FakeGh {
        dir: tempfile::TempDir,
        program: PathBuf,
    }

    #[cfg(unix)]
    #[derive(Debug, PartialEq, Eq)]
    struct FakeGhCall {
        argv: Vec<String>,
        gh_token: String,
    }

    #[cfg(unix)]
    impl FakeGh {
        fn new() -> Self {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().display().to_string();
            let script = format!(
                r#"#!/bin/sh
d='{root}'
printf 'CALL\t%s\n' "$GH_TOKEN" >> "$d/calls.log"
for a in "$@"; do printf 'ARG\t%s\n' "$(printf '%s' "$a" | tr '\n' '\036')" >> "$d/calls.log"; done
case "$1 $2" in
  "api graphql") route=probe ;;
  "pr review") route=review ;;
  "pr merge") route=merge ;;
  "pr view")
    route=view
    for a in "$@"; do
      case "$a" in
        reviews,headRefOid) route=reviews ;;
        reviewDecision) route=decision ;;
      esac
    done ;;
  *) route=other ;;
esac
n=1
[ -f "$d/$route.n" ] && n=$(( $(cat "$d/$route.n") + 1 ))
echo "$n" > "$d/$route.n"
out="$d/$route.out"
[ -f "$out.$n" ] && out="$out.$n"
[ -f "$out" ] && cat "$out"
[ -f "$d/$route.err" ] && cat "$d/$route.err" >&2
code=0
[ -f "$d/$route.code" ] && code=$(cat "$d/$route.code")
exit "$code"
"#
            );
            let program = dir.path().join("fake-gh");
            std::fs::write(&program, script).unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
            Self { dir, program }
        }

        fn respond(&self, route: &str, stdout: &str, code: i32) {
            let dir = self.dir.path();
            std::fs::write(dir.join(format!("{route}.out")), stdout).unwrap();
            std::fs::write(dir.join(format!("{route}.code")), code.to_string()).unwrap();
            let _ = std::fs::remove_file(dir.join(format!("{route}.err")));
        }

        /// Override stdout for only the `nth` (1-based) call on `route`.
        fn respond_nth(&self, route: &str, nth: usize, stdout: &str) {
            std::fs::write(self.dir.path().join(format!("{route}.out.{nth}")), stdout).unwrap();
        }

        fn fail(&self, route: &str, stderr: &str, code: i32) {
            self.respond(route, "", code);
            std::fs::write(self.dir.path().join(format!("{route}.err")), stderr).unwrap();
        }

        fn calls(&self) -> Vec<FakeGhCall> {
            let Ok(log) = std::fs::read_to_string(self.dir.path().join("calls.log")) else {
                return Vec::new();
            };
            let mut calls: Vec<FakeGhCall> = Vec::new();
            for line in log.lines() {
                if let Some(token) = line.strip_prefix("CALL\t") {
                    calls.push(FakeGhCall {
                        argv: Vec::new(),
                        gh_token: token.to_string(),
                    });
                } else if let Some(arg) = line.strip_prefix("ARG\t") {
                    // The script encodes newlines as RS (0x1e) to keep one arg per line.
                    calls
                        .last_mut()
                        .unwrap()
                        .argv
                        .push(arg.replace('\u{1e}', "\n"));
                }
            }
            calls
        }

        fn exec(
            &self,
            credentials: Arc<dyn GithubCredentialSource>,
            gh_repo: Option<&str>,
        ) -> GhMergeExecutor {
            let mut exec = gh_exec(credentials, gh_repo.map(str::to_owned));
            exec.gh_program = self.program.clone();
            exec
        }
    }

    #[test]
    fn production_gh_program_default_is_gh() {
        let (_, exec) = fake_exec("t");
        let cmd = exec
            .build_gh_cmd(&["pr", "view", "1"], Path::new("/tmp"))
            .unwrap();
        assert_eq!(cmd.get_program(), std::ffi::OsStr::new("gh"));
        let cmd = exec.identity_probe_cmd(1, Path::new("/tmp")).unwrap();
        assert_eq!(cmd.get_program(), std::ffi::OsStr::new("gh"));
    }

    #[cfg(unix)]
    #[test]
    fn identity_probe_argv_carries_constant_query_and_variables_only() {
        const SECRET: &str = "ghs_probe_secret_value";
        let gh = FakeGh::new();
        gh.respond("probe", &probe_json("true", r#""APPROVED""#), 0);
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        let exec = gh.exec(source.clone(), Some("owner/repo"));

        let identity = exec.probe_pr_identity(42, Path::new("/tmp"));
        assert!(is_one_identity(identity.as_ref()), "{identity:?}");
        assert_eq!(
            identity.unwrap().review_decision.as_deref(),
            Some("APPROVED")
        );

        let calls = gh.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(source.calls(), 1);
        assert_eq!(calls[0].gh_token, SECRET, "token travels via GH_TOKEN");
        assert_eq!(
            calls[0].argv,
            vec![
                "api".to_string(),
                "graphql".into(),
                "-f".into(),
                format!("query={PR_IDENTITY_QUERY}"),
                "-f".into(),
                "owner=owner".into(),
                "-f".into(),
                "name=repo".into(),
                "-F".into(),
                "number=42".into(),
            ]
        );
        assert!(!calls[0].argv.iter().any(|a| a == "-R"));
        assert!(
            !calls[0].argv.iter().any(|a| a.contains(SECRET)),
            "token must never enter argv"
        );
    }

    #[cfg(unix)]
    #[test]
    fn identity_probe_without_slug_uses_placeholders_in_repo_dir() {
        let gh = FakeGh::new();
        gh.respond("probe", &probe_json("false", "null"), 0);
        let exec = gh.exec(Arc::new(FakeCredentialSource::token("t")), None);
        let repo_dir = tempfile::tempdir().unwrap();
        let cmd = exec.identity_probe_cmd(7, repo_dir.path()).unwrap();
        assert_eq!(cmd.get_current_dir(), Some(repo_dir.path()));

        let identity = exec.probe_pr_identity(7, repo_dir.path());
        assert!(!is_one_identity(identity.as_ref()), "{identity:?}");
        assert_eq!(
            gh.calls()[0].argv[4..],
            ["-F", "owner={owner}", "-F", "name={repo}", "-F", "number=7"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn identity_probe_failures_never_select_one_identity() {
        let gh = FakeGh::new();
        let exec = gh.exec(Arc::new(FakeCredentialSource::token("t")), Some("o/r"));
        // gh failure, even with a one-identity body on stdout.
        gh.respond("probe", &probe_json("true", "null"), 1);
        assert_eq!(exec.probe_pr_identity(1, Path::new("/tmp")), None);
        gh.fail("probe", "HTTP 502", 1);
        assert_eq!(exec.probe_pr_identity(1, Path::new("/tmp")), None);
        // GraphQL errors entry with exit 0.
        gh.respond(
            "probe",
            r#"{"data":{"repository":{"pullRequest":{"viewerDidAuthor":true,"reviewDecision":null}}},"errors":[{"message":"x"}]}"#,
            0,
        );
        assert_eq!(exec.probe_pr_identity(1, Path::new("/tmp")), None);
        // Malformed JSON and missing field.
        gh.respond("probe", "{not json", 0);
        assert_eq!(exec.probe_pr_identity(1, Path::new("/tmp")), None);
        gh.respond(
            "probe",
            r#"{"data":{"repository":{"pullRequest":{"reviewDecision":null}}}}"#,
            0,
        );
        assert_eq!(exec.probe_pr_identity(1, Path::new("/tmp")), None);
        assert_eq!(gh.calls().len(), 5);
        // An unspawnable program is a failure too.
        let mut missing = gh.exec(Arc::new(FakeCredentialSource::token("t")), Some("o/r"));
        missing.gh_program = PathBuf::from("/nonexistent/bin/gh");
        assert_eq!(missing.probe_pr_identity(1, Path::new("/tmp")), None);
    }

    /// Negative case 11: a missing merge token file spawns no gh child.
    #[cfg(unix)]
    #[test]
    fn identity_probe_missing_credential_spawns_nothing() {
        let gh = FakeGh::new();
        gh.respond("probe", &probe_json("true", "null"), 0);
        let exec = gh.exec(
            Arc::new(LocalDefaultCredentialSource::new(Some(PathBuf::from(
                "/nonexistent/path/token",
            )))),
            Some("o/r"),
        );
        assert!(matches!(
            exec.identity_probe_cmd(1, Path::new("/tmp")),
            Err(GithubCredentialError::TokenFileUnreadable { .. })
        ));
        assert_eq!(exec.probe_pr_identity(1, Path::new("/tmp")), None);
        assert_eq!(exec.live_review_decision(1, Path::new("/tmp")), None);
        assert!(gh.calls().is_empty(), "no gh child may spawn");
    }

    #[cfg(unix)]
    #[test]
    fn fake_gh_serves_review_decision_reviews_review_and_merge() {
        const SECRET: &str = "ghs_merge_secret";
        let gh = FakeGh::new();
        let exec = gh.exec(Arc::new(FakeCredentialSource::token(SECRET)), Some("o/r"));

        gh.respond("decision", r#"{"reviewDecision":"APPROVED"}"#, 0);
        assert_eq!(
            exec.live_review_decision(5, Path::new("/tmp")).as_deref(),
            Some("APPROVED")
        );
        gh.fail("decision", "boom", 1);
        assert_eq!(exec.live_review_decision(5, Path::new("/tmp")), None);

        let marker = verdict_marker(VerdictKind::Approved, "head1");
        gh.respond(
            "reviews",
            &reviews_with(&[("COMMENTED", "head1", &marker)]),
            0,
        );
        let (reviews, head) = exec.fetch_reviews_and_head(5, Path::new("/tmp")).unwrap();
        assert_eq!(head, "head1");
        assert!(head_has_verdict_marker(
            &reviews,
            &head,
            VerdictKind::Approved
        ));
        assert!(!head_has_approval(&reviews, &head));

        gh.respond("review", "", 0);
        assert!(
            exec.run_gh(&["pr", "review", "5", "--comment"], Path::new("/tmp"))
                .success
        );
        gh.fail("merge", "merge blocked", 1);
        let merged = exec.run_gh(&["pr", "merge", "5", "--merge"], Path::new("/tmp"));
        assert!(!merged.success);
        assert_eq!(merged.message, "merge blocked");

        let calls = gh.calls();
        assert_eq!(calls.len(), 5);
        for call in &calls {
            assert_eq!(call.gh_token, SECRET);
            assert!(!call.argv.iter().any(|a| a.contains(SECRET)));
        }
        assert_eq!(
            calls[0].argv,
            ["pr", "view", "5", "--json", "reviewDecision", "-R", "o/r"]
        );
        assert_eq!(
            calls[2].argv,
            [
                "pr",
                "view",
                "5",
                "--json",
                "reviews,headRefOid",
                "-R",
                "o/r"
            ]
        );
        assert_eq!(calls[3].argv[..2], ["pr", "review"]);
        assert_eq!(calls[4].argv[..2], ["pr", "merge"]);
    }

    // ── ensure_changes_requested: one- vs two-identity ──────────────────

    #[cfg(unix)]
    const CHANGES_SECRET: &str = "ghs_changes_secret";

    #[cfg(unix)]
    fn reviews_at(head: &str, entries: &[(&str, &str, &str)]) -> String {
        let reviews: Vec<serde_json::Value> = entries
            .iter()
            .map(|(state, oid, body)| {
                serde_json::json!({"state": state, "commit": {"oid": oid}, "body": body})
            })
            .collect();
        serde_json::json!({ "reviews": reviews, "headRefOid": head }).to_string()
    }

    #[cfg(unix)]
    fn changes_gh() -> (FakeGh, GhMergeExecutor) {
        let gh = FakeGh::new();
        let exec = gh.exec(
            Arc::new(FakeCredentialSource::token(CHANGES_SECRET)),
            Some("o/r"),
        );
        gh.respond("review", "", 0);
        (gh, exec)
    }

    #[cfg(unix)]
    fn review_calls(gh: &FakeGh) -> Vec<Vec<String>> {
        gh.calls()
            .into_iter()
            .inspect(|call| {
                assert_eq!(call.gh_token, CHANGES_SECRET, "token via GH_TOKEN");
                assert!(
                    !call.argv.iter().any(|a| a.contains(CHANGES_SECRET)),
                    "token must never enter argv: {:?}",
                    call.argv
                );
            })
            .filter(|call| call.argv[..2] == ["pr", "review"])
            .map(|call| call.argv)
            .collect()
    }

    #[cfg(unix)]
    fn request_changes_argv(body: &str) -> Vec<String> {
        [
            "pr",
            "review",
            "9",
            "--request-changes",
            "--body",
            body,
            "-R",
            "o/r",
        ]
        .map(str::to_owned)
        .to_vec()
    }

    /// Negative case 1: viewerDidAuthor=false keeps today's argv and dedupe.
    #[cfg(unix)]
    #[test]
    fn ensure_changes_two_identity_posts_request_changes_unchanged() {
        let (gh, exec) = changes_gh();
        gh.respond("probe", &probe_json("false", "null"), 0);
        gh.respond("decision", r#"{"reviewDecision":"REVIEW_REQUIRED"}"#, 0);
        assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        assert_eq!(review_calls(&gh), vec![request_changes_argv("fix it")]);
        let routes: Vec<String> = gh.calls().iter().map(|c| c.argv[0].clone()).collect();
        assert_eq!(routes, ["api", "pr", "pr"], "probe, decision, review");

        // Existing CHANGES_REQUESTED dedupe still short-circuits.
        let (gh, exec) = changes_gh();
        gh.respond("probe", &probe_json("false", "null"), 0);
        gh.respond("decision", r#"{"reviewDecision":"CHANGES_REQUESTED"}"#, 0);
        assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        assert!(review_calls(&gh).is_empty());

        // A refused --request-changes still fails as today.
        let (gh, exec) = changes_gh();
        gh.respond("probe", &probe_json("false", "null"), 0);
        gh.respond("decision", r#"{"reviewDecision":""}"#, 0);
        gh.fail("review", "refused", 1);
        assert!(!exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        assert_eq!(review_calls(&gh), vec![request_changes_argv("fix it")]);
    }

    /// Negative case 2: a failed probe takes the two-identity path.
    #[cfg(unix)]
    #[test]
    fn ensure_changes_failed_probe_falls_back_to_two_identity() {
        let probes: [fn(&FakeGh); 2] = [
            |gh| gh.fail("probe", "HTTP 502", 1),
            |gh| gh.respond("probe", "{not json", 0),
        ];
        for fail_probe in probes {
            let (gh, exec) = changes_gh();
            fail_probe(&gh);
            gh.respond("decision", r#"{"reviewDecision":""}"#, 0);
            assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
            assert_eq!(review_calls(&gh), vec![request_changes_argv("fix it")]);
        }
    }

    /// One identity posts a marker COMMENT bound to the current head and
    /// never `--request-changes`.
    #[cfg(unix)]
    #[test]
    fn ensure_changes_one_identity_posts_marker_comment() {
        let (gh, exec) = changes_gh();
        gh.respond("probe", &probe_json("true", "null"), 0);
        gh.respond("reviews", &reviews_at("head1", &[]), 0);
        // Even a CHANGES_REQUESTED decision is not consulted in this mode.
        gh.respond("decision", r#"{"reviewDecision":"CHANGES_REQUESTED"}"#, 0);
        assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        let marked = format!(
            "fix it\n\n{}",
            verdict_marker(VerdictKind::Changes, "head1")
        );
        assert_eq!(
            review_calls(&gh),
            vec![[
                "pr",
                "review",
                "9",
                "--comment",
                "--body",
                &marked,
                "-R",
                "o/r"
            ]
            .map(str::to_owned)
            .to_vec()]
        );
        assert!(!gh
            .calls()
            .iter()
            .any(|c| c.argv.iter().any(|a| a == "--request-changes")));

        // A failed comment post is a failure, still never --request-changes.
        let (gh, exec) = changes_gh();
        gh.respond("probe", &probe_json("true", "null"), 0);
        gh.respond("reviews", &reviews_at("head1", &[]), 0);
        gh.fail("review", "boom", 1);
        assert!(!exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        let reviews = review_calls(&gh);
        assert_eq!(reviews.len(), 1);
        assert_eq!(reviews[0][3], "--comment");

        // An unreadable head posts nothing.
        let (gh, exec) = changes_gh();
        gh.respond("probe", &probe_json("true", "null"), 0);
        gh.fail("reviews", "boom", 1);
        assert!(!exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        assert!(review_calls(&gh).is_empty());
    }

    /// Negative case 4: a retry on the same head finds the marker, posts no
    /// second comment, and returns true.
    #[cfg(unix)]
    #[test]
    fn ensure_changes_one_identity_retry_same_head_posts_once() {
        let (gh, exec) = changes_gh();
        gh.respond("probe", &probe_json("true", "null"), 0);
        gh.respond("reviews", &reviews_at("head1", &[]), 0);
        assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        let posted = review_calls(&gh)[0][5].clone();
        gh.respond(
            "reviews",
            &reviews_at("head1", &[("COMMENTED", "head1", &posted)]),
            0,
        );
        assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "other"));
        assert_eq!(review_calls(&gh).len(), 1, "exactly one marker comment");
    }

    /// Negative case 5: after the head moves, the old marker does not dedupe.
    #[cfg(unix)]
    #[test]
    fn ensure_changes_one_identity_head_move_posts_new_marker() {
        let (gh, exec) = changes_gh();
        gh.respond("probe", &probe_json("true", "null"), 0);
        let old = format!("x\n\n{}", verdict_marker(VerdictKind::Changes, "head1"));
        gh.respond(
            "reviews",
            &reviews_at("head2", &[("COMMENTED", "head1", &old)]),
            0,
        );
        assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        let reviews = review_calls(&gh);
        assert_eq!(reviews.len(), 1);
        assert!(reviews[0][5].ends_with(&verdict_marker(VerdictKind::Changes, "head2")));
    }

    /// Negative case 10: a current-head comment quoting another head's
    /// marker, the approved marker, or no marker does not dedupe.
    #[cfg(unix)]
    #[test]
    fn ensure_changes_one_identity_ignores_quoted_or_missing_marker() {
        let other_head = format!(
            "as the daemon said: {}",
            verdict_marker(VerdictKind::Changes, "head0")
        );
        let approved = verdict_marker(VerdictKind::Approved, "head1");
        for body in [other_head.as_str(), approved.as_str(), "please fix"] {
            let (gh, exec) = changes_gh();
            gh.respond("probe", &probe_json("true", "null"), 0);
            gh.respond(
                "reviews",
                &reviews_at("head1", &[("COMMENTED", "head1", body)]),
                0,
            );
            assert!(exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
            let reviews = review_calls(&gh);
            assert_eq!(reviews.len(), 1, "{body}");
            assert!(reviews[0][5].ends_with(&verdict_marker(VerdictKind::Changes, "head1")));
        }
    }

    /// Negative cases 11/12: a missing credential spawns no gh child for the
    /// probe or any review; the token never enters argv (asserted per call by
    /// `review_calls` in the other tests).
    #[cfg(unix)]
    #[test]
    fn ensure_changes_missing_credential_spawns_nothing() {
        let gh = FakeGh::new();
        gh.respond("probe", &probe_json("true", "null"), 0);
        gh.respond("reviews", &reviews_at("head1", &[]), 0);
        gh.respond("review", "", 0);
        let exec = gh.exec(
            Arc::new(LocalDefaultCredentialSource::new(Some(PathBuf::from(
                "/nonexistent/path/token",
            )))),
            Some("o/r"),
        );
        assert!(!exec.ensure_changes_requested(9, Path::new("/tmp"), "fix it"));
        assert!(gh.calls().is_empty(), "no gh child may spawn");
    }

    // ── merge(): one-identity approved-verdict path ─────────────────────

    const MERGE_SECRET: &str = "ghs_merge_path_secret";
    const VIEW_AT_HEAD: &str =
        r#"{"baseRefName":"main","headRefOid":"abc123","headRefName":"feature","state":"OPEN"}"#;

    /// A fake gh serving a mergeable PR #1 at head `abc123` (ctx head) with
    /// no reviews, the given probe stdout, and the given reviewDecision.
    #[cfg(unix)]
    fn merge_fixture(probe: &str, decision: &str) -> FakeGh {
        let gh = FakeGh::new();
        gh.respond("view", VIEW_AT_HEAD, 0);
        gh.respond("other", r#"{"defaultBranchRef":{"name":"main"}}"#, 0);
        gh.respond("reviews", r#"{"reviews":[],"headRefOid":"abc123"}"#, 0);
        gh.respond("probe", probe, 0);
        gh.respond(
            "decision",
            &format!(r#"{{"reviewDecision":{decision}}}"#),
            0,
        );
        gh.respond("review", "", 0);
        gh.respond("merge", "", 0);
        gh
    }

    #[cfg(unix)]
    fn merge_exec(gh: &FakeGh) -> GhMergeExecutor {
        gh.exec(
            Arc::new(FakeCredentialSource::token(MERGE_SECRET)),
            Some("o/r"),
        )
    }

    #[cfg(unix)]
    fn calls_for(calls: &[FakeGhCall], verb: &str) -> Vec<Vec<String>> {
        calls
            .iter()
            .filter(|c| c.argv.len() >= 2 && c.argv[0] == "pr" && c.argv[1] == verb)
            .map(|c| c.argv.clone())
            .collect()
    }

    fn expected_merge_argv() -> Vec<String> {
        [
            "pr",
            "merge",
            "1",
            "--merge",
            "--delete-branch",
            "--match-head-commit",
            "abc123",
            "-R",
            "o/r",
        ]
        .map(String::from)
        .to_vec()
    }

    fn expected_approve_argv(ctx: &MergeContext) -> Vec<String> {
        let body = format!(
            "Formal approval — per {} review verdict (task #{}). \
             Merge performed programmatically on approved verdict (daemon model).",
            ctx.reviewer_name, ctx.review_task_id,
        );
        [
            "pr",
            "review",
            "1",
            "--approve",
            "--body",
            &body,
            "-R",
            "o/r",
        ]
        .map(String::from)
        .to_vec()
    }

    #[cfg(unix)]
    fn assert_token_only_in_env(calls: &[FakeGhCall]) {
        assert!(!calls.is_empty());
        for call in calls {
            assert_eq!(call.gh_token, MERGE_SECRET, "{:?}", call.argv);
            assert!(
                !call.argv.iter().any(|a| a.contains(MERGE_SECRET)),
                "token must never enter argv: {:?}",
                call.argv
            );
        }
    }

    /// Negative case 1: viewerDidAuthor=false keeps today's argv exactly.
    #[cfg(unix)]
    #[test]
    fn merge_two_identity_keeps_approve_and_merge_argv() {
        let ctx = test_ctx();
        let gh = merge_fixture(
            &probe_json("false", r#""REVIEW_REQUIRED""#),
            r#""REVIEW_REQUIRED""#,
        );
        let result = merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx);
        assert!(result.success, "{result:?}");

        let calls = gh.calls();
        assert_eq!(
            calls_for(&calls, "review"),
            vec![expected_approve_argv(&ctx)]
        );
        assert_eq!(calls_for(&calls, "merge"), vec![expected_merge_argv()]);
        assert!(
            !calls
                .iter()
                .any(|c| c.argv.iter().any(|a| a == "reviewDecision")),
            "two-identity mode never reads reviewDecision"
        );
        assert_token_only_in_env(&calls);

        // #49 dedupe unchanged: an APPROVED review on the head skips --approve.
        let gh = merge_fixture(&probe_json("false", "null"), "null");
        gh.respond(
            "reviews",
            &reviews_with_head(&[("APPROVED", "abc123", "ok")], "abc123"),
            0,
        );
        assert!(merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx).success);
        let calls = gh.calls();
        assert!(calls_for(&calls, "review").is_empty());
        assert_eq!(calls_for(&calls, "merge"), vec![expected_merge_argv()]);
    }

    /// Negative case 2: every failed probe keeps the two-identity path.
    #[cfg(unix)]
    #[test]
    fn merge_failed_probe_keeps_two_identity_path() {
        let ctx = test_ctx();
        let one_identity = probe_json("true", r#""REVIEW_REQUIRED""#);
        let errors_entry = r#"{"data":{"repository":{"pullRequest":{"viewerDidAuthor":true,"reviewDecision":null}}},"errors":[{"message":"x"}]}"#;
        let missing_field = r#"{"data":{"repository":{"pullRequest":{"reviewDecision":null}}}}"#;
        for (stdout, code) in [
            (one_identity.as_str(), 1),
            (errors_entry, 0),
            ("{not json", 0),
            (missing_field, 0),
        ] {
            let gh = merge_fixture(stdout, r#""REVIEW_REQUIRED""#);
            gh.respond("probe", stdout, code);
            let result = merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx);
            assert!(result.success, "{stdout}: {result:?}");
            let calls = gh.calls();
            assert_eq!(
                calls
                    .iter()
                    .filter(|c| c.argv[..2] == ["api", "graphql"])
                    .count(),
                1,
                "probe runs once per merge() call"
            );
            assert_eq!(
                calls_for(&calls, "review"),
                vec![expected_approve_argv(&ctx)],
                "{stdout}"
            );
            assert_eq!(
                calls_for(&calls, "merge"),
                vec![expected_merge_argv()],
                "{stdout}"
            );
        }
    }

    fn reviews_with_head(entries: &[(&str, &str, &str)], head: &str) -> String {
        let mut json: serde_json::Value = serde_json::from_str(&reviews_with(entries)).unwrap();
        json["headRefOid"] = serde_json::Value::String(head.into());
        json.to_string()
    }

    /// Negative cases 3 and 7: one marker COMMENT per head, then the
    /// unchanged merge argv when reviewDecision is not REVIEW_REQUIRED.
    #[cfg(unix)]
    #[test]
    fn merge_one_identity_posts_one_marker_comment_per_head_and_merges() {
        let ctx = test_ctx();
        let gh = merge_fixture(&probe_json("true", "null"), r#""APPROVED""#);
        let exec = merge_exec(&gh);
        let result = exec.merge(1, Path::new("/tmp"), &ctx);
        assert!(result.success, "{result:?}");

        let calls = gh.calls();
        let reviews = calls_for(&calls, "review");
        assert_eq!(reviews.len(), 1);
        let comment = &reviews[0];
        assert_eq!(comment[..5], ["pr", "review", "1", "--comment", "--body"]);
        assert_eq!(comment[6..], ["-R", "o/r"]);
        assert!(!comment.iter().any(|a| a == "--approve"));
        let body = &comment[5];
        assert!(
            body.contains("Rev-1") && body.contains("task #99"),
            "{body}"
        );
        assert!(
            body.contains(&verdict_marker(VerdictKind::Approved, "abc123")),
            "{body}"
        );
        assert_eq!(calls_for(&calls, "merge"), vec![expected_merge_argv()]);
        assert_token_only_in_env(&calls);

        // Re-entry on the same head: GitHub now lists the daemon's COMMENT.
        gh.respond(
            "reviews",
            &reviews_with_head(&[("COMMENTED", "abc123", body)], "abc123"),
            0,
        );
        assert!(exec.merge(1, Path::new("/tmp"), &ctx).success);
        let calls = gh.calls();
        assert_eq!(
            calls_for(&calls, "review").len(),
            1,
            "no second marker comment"
        );
        assert_eq!(calls_for(&calls, "merge").len(), 2);

        // A null decision is "anything else": merge is attempted unchanged.
        let gh = merge_fixture(&probe_json("true", "null"), "null");
        assert!(merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx).success);
        assert_eq!(calls_for(&gh.calls(), "merge"), vec![expected_merge_argv()]);
    }

    /// Negative case 6: REVIEW_REQUIRED never spawns `gh pr merge`.
    #[cfg(unix)]
    #[test]
    fn merge_one_identity_review_required_parks_without_merging() {
        let ctx = test_ctx();
        let gh = merge_fixture(&probe_json("true", "null"), r#""REVIEW_REQUIRED""#);
        let result = merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx);
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
        assert_eq!(result.message, self_approval_required(1, "abc123").message);
        assert!(
            result
                .message
                .contains("someone other than the pull request author")
                && result.message.contains("A person must approve head abc123")
                && result.message.contains("merge not attempted"),
            "{}",
            result.message
        );
        let calls = gh.calls();
        assert_eq!(calls_for(&calls, "review").len(), 1);
        assert!(
            calls_for(&calls, "merge").is_empty(),
            "gh pr merge must not spawn"
        );
    }

    /// Negative case 5: a moved head ignores the old marker and the existing
    /// drift rejection fires before any comment or merge.
    #[cfg(unix)]
    #[test]
    fn merge_one_identity_moved_head_rejects_before_comment() {
        let ctx = test_ctx();
        let gh = merge_fixture(&probe_json("true", "null"), r#""APPROVED""#);
        let old = verdict_marker(VerdictKind::Approved, "abc123");
        gh.respond(
            "reviews",
            &reviews_with_head(&[("COMMENTED", "abc123", &old)], "def456"),
            0,
        );
        // View calls: base, head, isDraft pass; the post-dedupe base/head
        // recheck sees the moved head.
        let moved = VIEW_AT_HEAD.replace("abc123", "def456");
        for nth in 4..=8 {
            gh.respond_nth("view", nth, &moved);
        }
        let result = merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx);
        assert!(!result.success);
        assert_eq!(result.failure_kind, Some(MergeFailureKind::StaleAuthority));
        assert!(
            result.message.contains("head drifted to def456"),
            "{}",
            result.message
        );
        let calls = gh.calls();
        assert!(
            calls_for(&calls, "review").is_empty(),
            "no comment after head moved"
        );
        assert!(calls_for(&calls, "merge").is_empty());
    }

    /// Negative case 8: a failed merge with a fresh non-REVIEW_REQUIRED
    /// decision is classified exactly as today; REVIEW_REQUIRED names the
    /// missing person's approval.
    #[cfg(unix)]
    #[test]
    fn merge_one_identity_failed_merge_classification() {
        let ctx = test_ctx();
        let stderr = "Pull request is not mergeable: the base branch policy prohibits the merge";
        for (fresh, expect_specific) in [
            (r#""APPROVED""#, false),
            ("null", false),
            (r#""REVIEW_REQUIRED""#, true),
        ] {
            let gh = merge_fixture(&probe_json("true", "null"), r#""APPROVED""#);
            gh.fail("merge", stderr, 1);
            gh.respond_nth("decision", 2, &format!(r#"{{"reviewDecision":{fresh}}}"#));
            let result = merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx);
            assert!(!result.success);
            assert_eq!(calls_for(&gh.calls(), "merge"), vec![expected_merge_argv()]);
            if expect_specific {
                assert_eq!(result.message, self_approval_required(1, "abc123").message);
                assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
            } else {
                assert_eq!(result.message, stderr, "{fresh}");
                assert_eq!(result.failure_kind, Some(classify_merge_failure(stderr)));
            }
        }
        // A failed fresh read keeps today's classification too.
        let gh = merge_fixture(&probe_json("true", "null"), r#""APPROVED""#);
        gh.fail("merge", "merge conflict", 1);
        gh.respond_nth("decision", 2, "{not json");
        let result = merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx);
        assert_eq!(result.message, "merge conflict");
        assert_eq!(
            result.failure_kind,
            Some(classify_merge_failure("merge conflict"))
        );
    }

    /// Negative case 10: agent-authored comments quoting another head's
    /// marker or none do not dedupe; a copied current-head marker at most
    /// suppresses the comment and never changes the merge decision.
    #[cfg(unix)]
    #[test]
    fn merge_one_identity_forged_markers_never_change_merge_outcome() {
        let ctx = test_ctx();
        let other_head = verdict_marker(VerdictKind::Approved, "zzz999");
        for body in ["LGTM, approved!", other_head.as_str()] {
            let gh = merge_fixture(&probe_json("true", "null"), r#""APPROVED""#);
            gh.respond(
                "reviews",
                &reviews_with_head(&[("COMMENTED", "abc123", body)], "abc123"),
                0,
            );
            assert!(merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx).success);
            assert_eq!(calls_for(&gh.calls(), "review").len(), 1, "{body}");
        }

        let copied = verdict_marker(VerdictKind::Approved, "abc123");
        for (decision, merges) in [(r#""APPROVED""#, true), (r#""REVIEW_REQUIRED""#, false)] {
            let gh = merge_fixture(&probe_json("true", "null"), decision);
            gh.respond(
                "reviews",
                &reviews_with_head(&[("COMMENTED", "abc123", &copied)], "abc123"),
                0,
            );
            let result = merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx);
            let calls = gh.calls();
            assert!(
                calls_for(&calls, "review").is_empty(),
                "duplicate comment suppressed"
            );
            assert_eq!(result.success, merges, "{decision}: {result:?}");
            assert_eq!(calls_for(&calls, "merge").len(), usize::from(merges));
            if !merges {
                assert_eq!(result.message, self_approval_required(1, "abc123").message);
            }
        }
    }

    /// Succeeds for the first `ok` token fetches, then fails.
    struct CountdownSource {
        ok: usize,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl GithubCredentialSource for CountdownSource {
        fn token(&self) -> std::result::Result<GithubToken, GithubCredentialError> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.ok {
                FakeCredentialSource::token(MERGE_SECRET).token()
            } else {
                Err(GithubCredentialError::LookupTimedOut {
                    timeout: Duration::from_secs(10),
                })
            }
        }
    }

    /// Negative cases 11/12: a credential failure at the probe or at the
    /// `--comment` review spawns no gh child for it, and nothing merges.
    #[cfg(unix)]
    #[test]
    fn merge_one_identity_credential_failure_spawns_no_probe_or_comment() {
        let ctx = test_ctx();
        // Baseline: every gh child fetches exactly one token first.
        let gh = merge_fixture(&probe_json("true", "null"), r#""APPROVED""#);
        assert!(merge_exec(&gh).merge(1, Path::new("/tmp"), &ctx).success);
        let baseline = gh.calls();
        let probe_at = baseline
            .iter()
            .position(|c| c.argv[..2] == ["api", "graphql"])
            .unwrap();
        let comment_at = baseline
            .iter()
            .position(|c| c.argv[..2] == ["pr", "review"])
            .unwrap();

        for (ok, spawned) in [(probe_at, probe_at), (comment_at, comment_at)] {
            let gh = merge_fixture(&probe_json("true", "null"), r#""APPROVED""#);
            let exec = gh.exec(
                Arc::new(CountdownSource {
                    ok,
                    calls: Default::default(),
                }),
                Some("o/r"),
            );
            let result = exec.merge(1, Path::new("/tmp"), &ctx);
            assert!(!result.success);
            assert_eq!(result.failure_kind, Some(MergeFailureKind::PolicyBlocked));
            assert!(!result.message.contains(MERGE_SECRET));
            let calls = gh.calls();
            assert_eq!(
                calls.len(),
                spawned,
                "no child after the credential failure"
            );
            assert!(calls_for(&calls, "review").is_empty());
            assert!(calls_for(&calls, "merge").is_empty());
            assert_token_only_in_env(&calls);
        }

        // A missing token file spawns nothing at all.
        let gh = merge_fixture(&probe_json("true", "null"), r#""APPROVED""#);
        let exec = gh.exec(
            Arc::new(LocalDefaultCredentialSource::new(Some(PathBuf::from(
                "/nonexistent/path/token",
            )))),
            Some("o/r"),
        );
        assert!(!exec.merge(1, Path::new("/tmp"), &ctx).success);
        assert!(gh.calls().is_empty());
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

    fn pre_review_jobs(json: &str, head: &str, jobs: &[&str]) -> PreReviewRequiredJobs {
        let jobs: Vec<String> = jobs.iter().map(|job| job.to_string()).collect();
        classify_pre_review_required_jobs(required_jobs_for_head(json, Some(head), &jobs))
    }

    #[test]
    fn pre_review_required_jobs_clean_empty_rollup_is_unregistered_not_failed() {
        // #474: GitHub reported CLEAN with an empty rollup seconds after push.
        let json = r#"{"mergeStateStatus":"CLEAN","headRefOid":"h1","statusCheckRollup":[]}"#;
        assert_eq!(
            checks_query_for_head(json, Some("h1")),
            ChecksQueryResult::AllPassed,
            "precondition: the CLEAN short-circuit still reaches the required-jobs gate"
        );
        assert_eq!(
            pre_review_jobs(json, "h1", &["fmt", "test"]),
            PreReviewRequiredJobs::Unregistered {
                missing_jobs: vec!["fmt".into(), "test".into()]
            }
        );
    }

    #[test]
    fn pre_review_required_jobs_in_progress_and_absent_is_pending() {
        let json = r#"{"headRefOid":"h1","statusCheckRollup":[
            {"name":"fmt","status":"IN_PROGRESS","conclusion":""}
        ]}"#;
        assert_eq!(
            pre_review_jobs(json, "h1", &["fmt", "test"]),
            PreReviewRequiredJobs::Pending {
                reason: "required jobs still pending: fmt".into()
            }
        );
    }

    #[test]
    fn pre_review_required_jobs_failure_is_not_hidden_by_absence() {
        let json = r#"{"headRefOid":"h1","statusCheckRollup":[
            {"name":"fmt","status":"COMPLETED","conclusion":"FAILURE"}
        ]}"#;
        assert_eq!(
            pre_review_jobs(json, "h1", &["fmt", "test"]),
            PreReviewRequiredJobs::Failed {
                failing_checks: vec!["fmt (FAILURE)".into()]
            }
        );
    }

    #[test]
    fn pre_review_required_jobs_skipped_or_neutral_still_fails() {
        for conclusion in ["SKIPPED", "NEUTRAL"] {
            let json = format!(
                r#"{{"headRefOid":"h1","statusCheckRollup":[
                    {{"name":"fmt","status":"COMPLETED","conclusion":"{conclusion}"}},
                    {{"name":"test","status":"COMPLETED","conclusion":"SUCCESS"}}
                ]}}"#
            );
            assert_eq!(
                pre_review_jobs(&json, "h1", &["fmt", "test"]),
                PreReviewRequiredJobs::Failed {
                    failing_checks: vec![format!("fmt ({conclusion})")]
                }
            );
        }
    }

    #[test]
    fn pre_review_required_jobs_success_other_head_and_query_failure() {
        let green = r#"{"headRefOid":"h1","statusCheckRollup":[
            {"name":"fmt","status":"COMPLETED","conclusion":"SUCCESS"}
        ]}"#;
        assert_eq!(
            pre_review_jobs(green, "h1", &["fmt"]),
            PreReviewRequiredJobs::Ready
        );
        assert!(
            matches!(
                pre_review_jobs(green, "h2", &["fmt"]),
                PreReviewRequiredJobs::Pending { .. }
            ),
            "a rollup for another head is pending, never ready or failed"
        );
        assert!(
            matches!(
                pre_review_jobs(
                    r#"{"headRefOid":"h1","statusCheckRollup":[]}"#,
                    "h2",
                    &["fmt"]
                ),
                PreReviewRequiredJobs::Pending { .. }
            ),
            "an empty rollup for another head is pending, not unregistered"
        );
        assert_eq!(
            classify_pre_review_required_jobs(RequiredJobsOutcome::NotReady {
                issues: vec![("fmt".into(), "query failed".into())]
            }),
            PreReviewRequiredJobs::Failed {
                failing_checks: vec!["fmt (query failed)".into()]
            },
            "only absence is reclassified"
        );
    }

    #[test]
    fn required_jobs_pending_names_round_trips_only_gate_reasons() {
        let ChecksOutcome::Pending { reason } = apply_required_jobs_gate(
            ChecksOutcome::Ready,
            RequiredJobsOutcome::Pending {
                pending_jobs: vec!["test".into(), "lint".into()],
            },
        ) else {
            panic!("required-job pending must stay pending");
        };
        assert_eq!(
            required_jobs_pending_names(&reason),
            Some(vec!["test".to_string(), "lint".to_string()])
        );
        assert_eq!(
            required_jobs_pending_names("CI checks timed out after 60s for PR #7"),
            None
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
