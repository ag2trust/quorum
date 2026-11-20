//! Bounded, daemon-owned IPC for managed agent runs.
//!
//! The endpoint is deliberately not a general daemon RPC surface. One Unix
//! socket accepts one length-prefixed JSON request per connection, resolves
//! all authority from the supplied run capability, and exposes only the
//! closed operations below. Repository identity is daemon configuration.

use super::repository_service::{
    PullRequestNumber, RepositoryRequest, RepositoryResponse, RepositoryService, RepositoryTarget,
    MAX_COMMENT_BODY_BYTES,
};
use quorum_core::capabilities::{LiveRunContext, LiveRunPhase};
use quorum_core::collaboration_admission::{
    self, ClientRequestId, CollaborationAdmissionError, CollaborationAgent, CollaborationAttemptId,
    CollaborationAttemptTransitionResult, CollaborationRole, CreateCollaborationAttemptRequest,
    EnqueueGithubOperationRequest, GithubOperationAdmissionResult, GithubOperationId,
    GithubOperationKind, GithubOperationReadResult, GithubOperationRequestJson,
    GithubOperationState, GithubOperationStatus, GithubSendState, ReviewPublicationFailure,
    ReviewVerdictEvidence, ReviewerHeadSha, RunAttemptResolution, RunCapabilityId,
};
use quorum_core::db::begin_immediate;
use quorum_core::error::{QuorumError, Result};
use quorum_core::planner_submissions::{self, CallOutcome, RejectionCategory, SubmitOutcome};
use quorum_core::review_anchor::{AnchorError, InlineAnchor, LaunchDiff, MAX_ANCHOR_PATH_BYTES};
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

/// Four-byte big-endian length prefix followed by at most one JSON document.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// Larger than a request so one `github_operation_read` answer fits: a
/// retained result is at most `MAX_OPERATION_JSON_BYTES` of canonical JSON,
/// plus a fully escaped error summary and the fixed envelope.
pub const MAX_RESPONSE_BYTES: usize = 96 * 1024;
/// Bound on one submitted plan document. Deliberately below
/// `MAX_REQUEST_BYTES` so a plan at the limit still fits its request frame:
/// the bound is the operative one, rejecting the payload as `invalid_plan`
/// against the run's rejection budget instead of letting the frame reader
/// answer `request_too_large` with nothing counted.
pub const MAX_PLAN_SUBMISSION_BYTES: usize = 60 * 1024;
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);
pub const PROCESS_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONNECTIONS: usize = 32;
const PROTOCOL_VERSION: u8 = 1;
const SOCKET_FILE: &str = "endpoint.sock";
/// Bound on one owner alert body. The feed itself is unbounded; this endpoint
/// is not, so the alert uses the same ceiling as a reaction reason.
pub const MAX_OWNER_ALERT_BYTES: usize = 8 * 1024;
const PROCESSING: u8 = 0;
const CANCELLED: u8 = 1;
const COMMITTING: u8 = 2;
const COMPLETE: u8 = 3;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u8,
    capability: String,
    operation: Operation,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Operation {
    AppendNote {
        /// Optional identity constraints. When present they must match the
        /// capability; when absent the capability's own task and agent apply.
        #[serde(default)]
        task_id: Option<i64>,
        #[serde(default)]
        agent: Option<String>,
        note: String,
    },
    /// Reviewer-only critical feed message addressed to the owner. Authority
    /// and authorship come from a live `reviewer` run capability.
    AlertOwner {
        body: String,
    },
    Submit {
        #[serde(default)]
        summary: Option<String>,
        #[serde(default)]
        verdict: Option<String>,
        #[serde(default)]
        feedback: Option<String>,
        #[serde(default)]
        feedback_json: Option<String>,
        #[serde(default)]
        blocking: Option<u32>,
        /// Reviewer only: the explicit typed reason a verdict is submitted
        /// although the run's pending COMMENT review did not fully publish.
        /// The only accepted value is `operation_failed`, and the verdict
        /// guard admits it only when durable outbox state bears it out.
        #[serde(default)]
        publication_failure: Option<String>,
    },
    React {
        state: String,
        /// Required for `blocked` / `needs-info`; absent from older clients.
        #[serde(default)]
        reason: Option<String>,
    },
    /// Planner-only plan delivery. Authority comes from a `planner` run
    /// capability; every other operation rejects that role, and this one
    /// rejects every other role.
    SubmitPlan {
        response: serde_json::Value,
    },
    Inventory,
    Protocol {
        operation: ProtocolOperation,
        /// Required by `github_operation_read`; ignored by every operation
        /// that is still unavailable.
        #[serde(default)]
        attempt_id: Option<String>,
        #[serde(default)]
        operation_id: Option<String>,
        /// The tool's own arguments object for `pull_request_read`,
        /// `add_issue_comment`, `pull_request_review_write`, and
        /// `add_comment_to_pending_review`, decoded here against each closed
        /// schema.
        #[serde(default)]
        arguments: Option<serde_json::Value>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolOperation {
    PullRequestRead,
    AddIssueComment,
    PullRequestReviewWrite,
    AddCommentToPendingReview,
    AddReplyToPullRequestComment,
    ResolveReviewThread,
    GithubOperationRead,
    DeliveryReportWrite,
}

impl ProtocolOperation {
    fn allowed_in(self, phase: LiveRunPhase) -> bool {
        match phase {
            LiveRunPhase::InitialWorker => matches!(self, Self::DeliveryReportWrite),
            LiveRunPhase::ReworkWorker => matches!(
                self,
                Self::PullRequestRead
                    | Self::AddIssueComment
                    | Self::AddReplyToPullRequestComment
                    | Self::GithubOperationRead
                    | Self::DeliveryReportWrite
            ),
            LiveRunPhase::Reviewer => !matches!(self, Self::DeliveryReportWrite),
        }
    }
}

const ALL_PROTOCOL_OPERATIONS: [ProtocolOperation; 8] = [
    ProtocolOperation::PullRequestRead,
    ProtocolOperation::AddIssueComment,
    ProtocolOperation::PullRequestReviewWrite,
    ProtocolOperation::AddCommentToPendingReview,
    ProtocolOperation::AddReplyToPullRequestComment,
    ProtocolOperation::ResolveReviewThread,
    ProtocolOperation::GithubOperationRead,
    ProtocolOperation::DeliveryReportWrite,
];

#[derive(Debug, Serialize)]
struct Response {
    version: u8,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<ResponseResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ResponseError>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseResult {
    TaskNote {
        note_id: i64,
    },
    Mailbox {
        mailbox_id: i64,
    },
    PlanAccepted {
        graph_id: i64,
    },
    Message {
        seq: i64,
    },
    Inventory {
        repository: String,
        task_id: i64,
        role: String,
        pr: Option<i64>,
        review_revision: Option<String>,
        phase: InventoryPhase,
        operations: Vec<ProtocolOperation>,
    },
    /// The closed, bounded view of one operation owned by the caller's run.
    /// Request JSON, the hidden marker, and credentials are never included.
    GithubOperation {
        attempt_id: String,
        operation_id: String,
        state: GithubOperationState,
        send_state: GithubSendState,
        remote_object_id: Option<String>,
        result: Option<serde_json::Value>,
        error_summary: Option<String>,
        /// Typed failure class once the operation failed or a retry is
        /// pending after a GitHub failure; `None` while healthy or done.
        error_code: Option<&'static str>,
    },
}

/// The run phase an inventory advertises.
///
/// It mirrors `LiveRunPhase` and adds the planner, whose entire tool surface is
/// `submit_plan`. Keeping it distinct from `LiveRunPhase` means no
/// `ProtocolOperation` gate can ever be asked about a planner: authority for
/// every GitHub and lifecycle operation still comes from
/// `resolve_live_run_context`, which rejects the planner role outright.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum InventoryPhase {
    InitialWorker,
    ReworkWorker,
    Reviewer,
    Planner,
}

impl From<LiveRunPhase> for InventoryPhase {
    fn from(phase: LiveRunPhase) -> Self {
        match phase {
            LiveRunPhase::InitialWorker => Self::InitialWorker,
            LiveRunPhase::ReworkWorker => Self::ReworkWorker,
            LiveRunPhase::Reviewer => Self::Reviewer,
        }
    }
}

#[derive(Debug, Serialize)]
struct ResponseError {
    code: &'static str,
    message: Cow<'static, str>,
}

impl Response {
    fn success(result: ResponseResult) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    fn failure(code: &'static str, message: Cow<'static, str>) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            ok: false,
            result: None,
            error: Some(ResponseError { code, message }),
        }
    }
}

#[derive(Debug)]
struct EndpointFailure {
    code: &'static str,
    /// Owned so a validator's own rejection text reaches the agent verbatim.
    /// Every non-validating failure keeps a fixed, borrowed message.
    message: Cow<'static, str>,
}

impl EndpointFailure {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self {
            code,
            message: Cow::Borrowed(message),
        }
    }

    /// A failure whose message is produced at call time, not a fixed constant.
    fn detailed(code: &'static str, message: String) -> Self {
        Self {
            code,
            message: Cow::Owned(message),
        }
    }

    /// Only daemon-side faults warrant an operator-visible log entry.
    fn is_abnormal(&self) -> bool {
        match self.code {
            "internal" => true,
            "unauthorized"
            | "malformed_frame"
            | "request_too_large"
            | "malformed_request"
            | "unsupported_version"
            | "timeout"
            | "invalid_operation"
            | "forbidden_operation"
            | "operation_unavailable"
            | "operation_not_found"
            | "operation_rejected"
            | "operation_limit_reached"
            | "invalid_plan"
            | "already_submitted"
            | "submit_budget_exhausted" => false,
            code if is_review_refusal(code) => false,
            _ => true,
        }
    }

    fn response(self) -> Response {
        Response::failure(self.code, self.message)
    }
}

/// Closed refusal codes of the pending-review tools and the reviewer verdict
/// guard. Each is an expected clean negative: nothing was admitted or
/// recorded, and the agent can correct its call. `review_diff_unavailable`
/// and `review_head_unavailable` are deliberately absent — they report a
/// daemon-side or GitHub fault and are logged as abnormal.
pub const REVIEW_REFUSAL_CODES: [&str; 20] = [
    "review_method_unsupported",
    "review_event_unsupported",
    "review_id_unsupported",
    "review_commit_mismatch",
    "review_not_created",
    "review_already_created",
    "review_sealed",
    "review_inline_limit_reached",
    "review_body_too_large",
    "review_not_published",
    "review_head_moved",
    "review_publication_not_failed",
    "anchor_stale_sha",
    "anchor_unsupported_subject_type",
    "anchor_path_not_in_diff",
    "anchor_missing_line",
    "anchor_invalid_side",
    "anchor_line_outside_diff",
    "anchor_side_mismatch",
    "anchor_invalid_range",
];

fn is_review_refusal(code: &str) -> bool {
    REVIEW_REFUSAL_CODES.contains(&code)
}

/// The typed endpoint code for each closed anchor-validator rejection.
fn anchor_failure(error: AnchorError) -> EndpointFailure {
    let code = match error {
        AnchorError::StaleSha => "anchor_stale_sha",
        AnchorError::UnsupportedSubjectType => "anchor_unsupported_subject_type",
        AnchorError::PathNotInDiff => "anchor_path_not_in_diff",
        AnchorError::MissingLine => "anchor_missing_line",
        AnchorError::InvalidSide => "anchor_invalid_side",
        AnchorError::LineOutsideDiff => "anchor_line_outside_diff",
        AnchorError::SideMismatch => "anchor_side_mismatch",
        AnchorError::InvalidRange => "anchor_invalid_range",
    };
    EndpointFailure::detailed(code, format!("inline anchor rejected: {error}"))
}

/// Daemon-side evidence the pending-review tools and the verdict guard need.
/// Both calls run with no database transaction open, and every caller bounds
/// them with its own deadline.
pub type ReviewEvidenceFuture<'a, T> =
    Pin<Box<dyn Future<Output = std::result::Result<T, String>> + Send + 'a>>;

pub trait ReviewEvidence: Send + Sync {
    /// A bounded unified `git diff` of the pull request at `launch_sha`
    /// against `base_branch`, with the head SHA the diff was computed for.
    fn launch_diff<'a>(
        &'a self,
        base_branch: &'a str,
        launch_sha: &'a str,
    ) -> ReviewEvidenceFuture<'a, (String, Vec<u8>)>;

    /// The remote pull request head SHA, freshly fetched from GitHub.
    fn pull_request_head(&self, pr: i64) -> ReviewEvidenceFuture<'_, String>;
}

/// Wall-clock bound on computing one launch-SHA diff.
const REVIEW_DIFF_TIMEOUT: Duration = Duration::from_secs(3);
/// Wall-clock bound on the fresh PR-head fetch before a verdict proof. It
/// leaves the proof transaction room inside `PROCESS_TIMEOUT`.
const REVIEW_HEAD_TIMEOUT: Duration = Duration::from_secs(3);

/// The production evidence: the diff from the daemon's own repository clone
/// and the head from the daemon's Repository Service.
pub struct RepositoryReviewEvidence {
    repo_dir: PathBuf,
    target: Option<RepositoryTarget>,
    service: Arc<dyn RepositoryService>,
}

impl RepositoryReviewEvidence {
    /// An invalid configured repository leaves head fetches failing closed;
    /// the verdict guard then refuses rather than skipping its proof.
    pub fn new(repo_dir: &Path, repository: &str, service: Arc<dyn RepositoryService>) -> Self {
        Self {
            repo_dir: repo_dir.to_path_buf(),
            target: RepositoryTarget::new(repo_dir.to_path_buf(), Some(repository)).ok(),
            service,
        }
    }
}

impl ReviewEvidence for RepositoryReviewEvidence {
    fn launch_diff<'a>(
        &'a self,
        base_branch: &'a str,
        launch_sha: &'a str,
    ) -> ReviewEvidenceFuture<'a, (String, Vec<u8>)> {
        Box::pin(async move {
            let base_ref = format!("refs/remotes/origin/{base_branch}");
            quorum_core::git::validate_ref_name(&base_ref).map_err(|error| error.to_string())?;
            quorum_core::git::validate_sha(launch_sha).map_err(|error| error.to_string())?;
            // GitHub's pull request diff: merge base to head, three lines of
            // context, unmerged hunks, and default prefixes. Repository
            // configuration that would change any of those is overridden.
            let mut command = tokio::process::Command::from(quorum_core::git::git_command("git"));
            command
                .arg("-C")
                .arg(&self.repo_dir)
                .args([
                    "-c",
                    "diff.noprefix=false",
                    "-c",
                    "diff.mnemonicPrefix=false",
                    "-c",
                    "diff.relative=false",
                    "-c",
                    "diff.suppressBlankEmpty=false",
                    "-c",
                    "core.quotePath=true",
                    "diff",
                    "--no-color",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--find-renames",
                    "--unified=3",
                    "--inter-hunk-context=0",
                    "--diff-algorithm=myers",
                    "--src-prefix=a/",
                    "--dst-prefix=b/",
                ])
                .arg(format!("{base_ref}...{launch_sha}"))
                .arg("--");
            let output = super::worktree::run_git_with_limit(
                command,
                REVIEW_DIFF_TIMEOUT,
                quorum_core::review_anchor::MAX_DIFF_BYTES,
                "git diff launch SHA",
            )
            .await?;
            if !output.status.success() {
                return Err("git diff of the launch SHA failed".into());
            }
            Ok((launch_sha.to_owned(), output.stdout))
        })
    }

    fn pull_request_head(&self, pr: i64) -> ReviewEvidenceFuture<'_, String> {
        Box::pin(async move {
            let target = self
                .target
                .as_ref()
                .ok_or("the configured repository is invalid")?;
            let pr = PullRequestNumber::new(pr).ok_or("the run has no valid pull request")?;
            match self
                .service
                .execute(target, pr, &RepositoryRequest::ReadPrMetadata {})
                .await
            {
                Ok(RepositoryResponse::PrMetadata(metadata)) => Ok(metadata.head_sha),
                Ok(_) => Err("unexpected pull request metadata response".into()),
                Err(error) => Err(error.to_string()),
            }
        })
    }
}

/// The stable locator injected into managed launches for this database.
pub fn locator(db_path: &Path) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    db_path.hash(&mut hasher);
    std::env::temp_dir()
        .join(format!("quorum-agent-{:016x}", hasher.finish()))
        .join(SOCKET_FILE)
}

/// Filesystem authority the plan validator needs.
///
/// `WritablePathResolver` is a *process-local single-slot admission gate*
/// (`planner.rs`): at most one dedicated OS resolver thread runs at a time and
/// an occupied slot fails later resolutions closed until the stalled
/// filesystem call really returns. That bound only holds if the whole process
/// shares one instance, so the daemon constructs it once and hands the same
/// clone to this endpoint and to the decomposition coordinator.
#[derive(Clone)]
struct PlanValidation {
    repo_dir: PathBuf,
    path_resolver: super::planner::WritablePathResolver,
}

pub struct AgentEndpoint {
    locator: PathBuf,
    artifact_dir: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl AgentEndpoint {
    pub async fn start(
        db_path: &Path,
        repository: &str,
        repo_dir: &Path,
        path_resolver: super::planner::WritablePathResolver,
        review_evidence: Arc<dyn ReviewEvidence>,
    ) -> Result<Self> {
        let locator = locator(db_path);
        let artifact_dir = locator
            .parent()
            .expect("endpoint locator always has a parent")
            .to_path_buf();
        if let Err(error) = prepare_artifact_dir(&artifact_dir, &locator) {
            cleanup_artifacts(&locator, &artifact_dir);
            return Err(error);
        }

        let listener = match UnixListener::bind(&locator) {
            Ok(listener) => listener,
            Err(error) => {
                cleanup_artifacts(&locator, &artifact_dir);
                return Err(QuorumError::Io(format!(
                    "failed to bind agent endpoint: {error}"
                )));
            }
        };
        if let Err(error) = fs::set_permissions(&locator, fs::Permissions::from_mode(0o600)) {
            drop(listener);
            cleanup_artifacts(&locator, &artifact_dir);
            return Err(QuorumError::Io(format!(
                "failed to restrict agent endpoint permissions: {error}"
            )));
        }

        // Only the location is resolved here. The file is read per owner
        // alert, as `quorum post` reads it per post, so starting the endpoint
        // neither depends on nor caches the operator config's contents.
        let operator_config = match crate::paths::config_path() {
            Ok(path) => path,
            Err(error) => {
                drop(listener);
                cleanup_artifacts(&locator, &artifact_dir);
                return Err(error);
            }
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let db_path = db_path.to_path_buf();
        let repository = repository.to_string();
        let plan_validation = PlanValidation {
            repo_dir: repo_dir.to_path_buf(),
            path_resolver,
        };
        let task = tokio::spawn(run_listener(
            listener,
            db_path,
            repository,
            plan_validation,
            operator_config,
            review_evidence,
            shutdown_rx,
        ));
        super::log("agent endpoint ready");
        Ok(Self {
            locator,
            artifact_dir,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        })
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        cleanup_artifacts(&self.locator, &self.artifact_dir);
        super::log("agent endpoint stopped");
    }
}

impl Drop for AgentEndpoint {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        cleanup_artifacts(&self.locator, &self.artifact_dir);
    }
}

fn prepare_artifact_dir(dir: &Path, socket: &Path) -> Result<()> {
    match fs::create_dir(dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(dir).map_err(|metadata_error| {
                QuorumError::Io(format!(
                    "failed to inspect agent endpoint directory: {metadata_error}"
                ))
            })?;
            if !metadata.file_type().is_dir() || metadata.uid() != unsafe { libc::geteuid() } {
                return Err(QuorumError::Io(
                    "agent endpoint directory is not an owner-controlled directory".into(),
                ));
            }
        }
        Err(error) => {
            return Err(QuorumError::Io(format!(
                "failed to create agent endpoint directory: {error}"
            )));
        }
    }
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(|error| {
        QuorumError::Io(format!(
            "failed to restrict agent endpoint directory permissions: {error}"
        ))
    })?;
    match fs::remove_file(socket) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(QuorumError::Io(format!(
                "failed to remove stale agent endpoint: {error}"
            )));
        }
    }
    Ok(())
}

fn cleanup_artifacts(socket: &Path, dir: &Path) {
    let _ = fs::remove_file(socket);
    let _ = fs::remove_dir(dir);
}

async fn run_listener(
    listener: UnixListener,
    db_path: PathBuf,
    repository: String,
    plan_validation: PlanValidation,
    operator_config: PathBuf,
    review_evidence: Arc<dyn ReviewEvidence>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                match accepted {
                    Ok((stream, _)) => {
                        connections.spawn(serve_connection(
                            stream,
                            db_path.clone(),
                            repository.clone(),
                            plan_validation.clone(),
                            operator_config.clone(),
                            Arc::clone(&review_evidence),
                        ));
                    }
                    Err(_) => super::log("agent endpoint accept failed"),
                }
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    drop(listener);
    // Stateful spawn_blocking work is not abortable. Drain every admitted
    // connection so endpoint teardown cannot precede its commit or rollback.
    while connections.join_next().await.is_some() {}
}

async fn serve_connection(
    mut stream: UnixStream,
    db_path: PathBuf,
    repository: String,
    plan_validation: PlanValidation,
    operator_config: PathBuf,
    review_evidence: Arc<dyn ReviewEvidence>,
) {
    let response = match read_request(&mut stream).await {
        Ok(request) => match process_request(
            db_path,
            repository,
            plan_validation,
            operator_config,
            review_evidence,
            request,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                if error.is_abnormal() {
                    super::log(error.code);
                }
                error.response()
            }
        },
        Err(error) => {
            if error.is_abnormal() {
                super::log(error.code);
            }
            error.response()
        }
    };
    let _ = write_response(&mut stream, &response).await;
}

async fn read_request(stream: &mut UnixStream) -> std::result::Result<Request, EndpointFailure> {
    let mut prefix = [0_u8; 4];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut prefix))
        .await
        .map_err(|_| EndpointFailure::new("timeout", "request read timed out"))?
        .map_err(|_| EndpointFailure::new("malformed_frame", "malformed request frame"))?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length == 0 {
        return Err(EndpointFailure::new(
            "malformed_frame",
            "malformed request frame",
        ));
    }
    if length > MAX_REQUEST_BYTES {
        return Err(EndpointFailure::new(
            "request_too_large",
            "request frame exceeds the limit",
        ));
    }
    let mut body = vec![0; length];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut body))
        .await
        .map_err(|_| EndpointFailure::new("timeout", "request read timed out"))?
        .map_err(|_| EndpointFailure::new("malformed_frame", "malformed request frame"))?;
    serde_json::from_slice(&body)
        .map_err(|_| EndpointFailure::new("malformed_request", "malformed request"))
}

async fn write_response(stream: &mut UnixStream, response: &Response) -> std::io::Result<()> {
    let body = serde_json::to_vec(response).map_err(std::io::Error::other)?;
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(std::io::Error::other("agent endpoint response overflow"));
    }
    let length = (body.len() as u32).to_be_bytes();
    tokio::time::timeout(IO_TIMEOUT, async {
        stream.write_all(&length).await?;
        stream.write_all(&body).await?;
        stream.shutdown().await
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "response write timed out"))?
}

async fn process_request(
    db_path: PathBuf,
    repository: String,
    plan_validation: PlanValidation,
    operator_config: PathBuf,
    review_evidence: Arc<dyn ReviewEvidence>,
    request: Request,
) -> std::result::Result<Response, EndpointFailure> {
    process_request_with_timeout(
        db_path,
        repository,
        plan_validation,
        operator_config,
        review_evidence,
        request,
        PROCESS_TIMEOUT,
    )
    .await
}

async fn process_request_with_timeout(
    db_path: PathBuf,
    repository: String,
    plan_validation: PlanValidation,
    operator_config: PathBuf,
    review_evidence: Arc<dyn ReviewEvidence>,
    request: Request,
    process_timeout: Duration,
) -> std::result::Result<Response, EndpointFailure> {
    if request.version != PROTOCOL_VERSION {
        let failure = EndpointFailure::new("unsupported_version", "unsupported protocol version");
        if matches!(request.operation, Operation::SubmitPlan { .. })
            && capability_has_valid_shape(&request.capability)
        {
            return Err(record_refused_call(
                db_path,
                request.capability,
                RejectionCategory::UnsupportedVersion,
                quorum_core::clock::now(),
                failure,
            )
            .await);
        }
        return Err(failure);
    }
    if !capability_has_valid_shape(&request.capability) {
        return Err(EndpointFailure::new(
            "unauthorized",
            "run authority rejected",
        ));
    }

    // Plan validation is async and touches the filesystem, so it cannot run on
    // the shared blocking path that opens `BEGIN IMMEDIATE` up front. This
    // branch validates with no write transaction open and records afterwards.
    let request = match request.operation {
        Operation::Protocol {
            operation:
                operation @ (ProtocolOperation::PullRequestRead | ProtocolOperation::AddIssueComment),
            arguments,
            ..
        } => {
            // Admission is several short guarded transactions (attempt, then
            // operation), each idempotent under retry, so it runs to
            // completion rather than being abandoned at a deadline.
            let capability = request.capability;
            return tokio::task::spawn_blocking(move || {
                admit_github_operation(&db_path, &repository, &capability, operation, arguments)
            })
            .await
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
        }
        Operation::Protocol {
            operation:
                operation @ (ProtocolOperation::PullRequestReviewWrite
                | ProtocolOperation::AddCommentToPendingReview),
            arguments,
            ..
        } => {
            // Like the operations above, admission runs to completion: the
            // only slow step (the launch-SHA diff) is bounded on its own and
            // runs before any write transaction opens.
            return admit_review_operation(
                db_path,
                repository,
                request.capability,
                operation,
                arguments,
                review_evidence,
            )
            .await;
        }
        Operation::SubmitPlan { response } => {
            return submit_plan(
                db_path,
                plan_validation,
                request.capability,
                response,
                process_timeout,
            )
            .await;
        }
        operation => Request {
            version: request.version,
            capability: request.capability,
            operation,
        },
    };

    // A reviewer verdict whose run began a pending review needs the remote PR
    // head for its proof. The fetch is network I/O, so it happens here, with
    // no transaction open; its time comes out of the request's own budget.
    let started = Instant::now();
    let verdict_head = match &request.operation {
        Operation::Submit {
            verdict: Some(_),
            publication_failure: None,
            ..
        } => verdict_head_evidence(&db_path, &request.capability, review_evidence).await?,
        _ => None,
    };
    let process_timeout = process_timeout.saturating_sub(started.elapsed());

    let state = Arc::new(AtomicU8::new(PROCESSING));
    let deadline = Instant::now() + process_timeout;
    let blocking_state = Arc::clone(&state);
    let mut task = tokio::task::spawn_blocking(move || {
        process_request_blocking(
            &db_path,
            &repository,
            &operator_config,
            request,
            verdict_head,
            &blocking_state,
            deadline,
        )
    });

    match tokio::time::timeout(process_timeout, &mut task).await {
        Ok(result) => {
            result.map_err(|_| EndpointFailure::new("internal", "request processing failed"))?
        }
        Err(_) => {
            // Cancellation is permitted only before the blocking transaction owns
            // commit. If commit already won the race, wait for and report its real
            // result. Otherwise join the rollback before returning a timeout so no
            // authoritative write can appear after the bounded failure.
            let cancelled = state
                .compare_exchange(PROCESSING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
            let result = task
                .await
                .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
            if cancelled {
                Err(EndpointFailure::new(
                    "timeout",
                    "request processing timed out",
                ))
            } else {
                result
            }
        }
    }
}

fn process_request_blocking(
    db_path: &Path,
    repository: &str,
    operator_config: &Path,
    request: Request,
    verdict_head: Option<String>,
    state: &AtomicU8,
    deadline: Instant,
) -> std::result::Result<Response, EndpointFailure> {
    // The alert TTL is the operator's configured message TTL, exactly what
    // `quorum post` applies. Reading it is file I/O, which must not happen
    // under the database write lock, so it is read before the transaction.
    // Authority and the body are decided first, on a short read, so a caller
    // with no reviewer authority is answered `unauthorized` whatever state
    // the config file is in. The write transaction below resolves authority
    // again and is the one that counts.
    let owner_alert_ttl = match &request.operation {
        Operation::AlertOwner { body } => {
            {
                let conn = quorum_core::db::open(db_path)
                    .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
                resolve(&conn, &request.capability, "reviewer")?;
            }
            validate_owner_alert(body)?;
            Some(configured_message_ttl(operator_config)?)
        }
        _ => None,
    };
    let mut conn = quorum_core::db::open(db_path)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let tx = begin_immediate(&mut conn)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;

    let response = match request.operation {
        Operation::AppendNote {
            task_id,
            agent,
            note,
        } => {
            let context = resolve_any_role(&tx, &request.capability)?;
            if task_id.is_some_and(|task_id| task_id != context.task_id)
                || agent.is_some_and(|agent| agent != context.agent)
            {
                return Err(EndpointFailure::new(
                    "invalid_operation",
                    "task identity does not match run authority",
                ));
            }
            validate_progress_note(&note)?;
            let note_id = quorum_core::tasks::add_note_tx(
                &tx,
                &context.agent,
                context.task_id,
                &note,
                quorum_core::clock::now(),
            )
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?
            .ok_or_else(|| EndpointFailure::new("internal", "request processing failed"))?;
            Response::success(ResponseResult::TaskNote { note_id })
        }
        Operation::AlertOwner { body } => {
            let context = resolve(&tx, &request.capability, "reviewer")?;
            validate_owner_alert(&body)?;
            let ttl = owner_alert_ttl
                .ok_or_else(|| EndpointFailure::new("internal", "request processing failed"))?;
            let posted = quorum_core::feed::post_tx(
                &tx,
                &context.agent,
                "critical",
                None,
                &body,
                None,
                Some("owner"),
                ttl,
                quorum_core::clock::now(),
            )
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
            Response::success(ResponseResult::Message { seq: posted.seq })
        }
        Operation::Submit {
            summary,
            verdict,
            feedback,
            feedback_json,
            blocking,
            publication_failure,
        } => {
            let role = if verdict.is_some() {
                "reviewer"
            } else {
                "worker"
            };
            let context = resolve(&tx, &request.capability, role)?;
            validate_text(summary.as_deref())?;
            validate_text(feedback.as_deref())?;
            validate_text(feedback_json.as_deref())?;
            if role == "worker" && publication_failure.is_some() {
                return Err(EndpointFailure::new(
                    "invalid_operation",
                    "worker submission has reviewer fields",
                ));
            }
            let payload = validate_submission(
                &request.capability,
                &context,
                verdict.as_deref(),
                feedback.as_deref(),
                feedback_json.as_deref(),
                blocking,
            )?;
            if role == "reviewer" {
                // The proof and the verdict's mailbox row share this one
                // `BEGIN IMMEDIATE`: a refusal rolls back with nothing
                // recorded, and an admission completes the attempt.
                review_verdict_guard(
                    &tx,
                    &context,
                    publication_failure.as_deref(),
                    verdict_head.as_deref(),
                )?;
            }
            let mailbox_id = insert_mailbox(
                &tx,
                &context,
                "done",
                context.pr,
                verdict.as_deref(),
                feedback.as_deref(),
                summary.as_deref(),
                payload.as_deref(),
            )?;
            Response::success(ResponseResult::Mailbox { mailbox_id })
        }
        Operation::React { state, reason } => {
            let context = resolve(&tx, &request.capability, "worker")?;
            if !matches!(state.as_str(), "blocked" | "failed" | "needs-info" | "note") {
                return Err(EndpointFailure::new(
                    "invalid_operation",
                    "invalid reaction state",
                ));
            }
            let reason = quorum_core::tasks::validate_react_reason(&state, reason.as_deref())
                .map_err(|_| {
                    EndpointFailure::new(
                        "invalid_operation",
                        "react requires a non-empty reason (max 8192 bytes, no NUL) for blocked/needs-info",
                    )
                })?;
            let mailbox_id = insert_mailbox(
                &tx,
                &context,
                "task_update",
                None,
                None,
                reason.as_deref(),
                Some(&state),
                None,
            )?;
            Response::success(ResponseResult::Mailbox { mailbox_id })
        }
        // Routed to `submit_plan` before this write transaction opened.
        Operation::SubmitPlan { .. } => {
            return Err(EndpointFailure::new(
                "internal",
                "request processing failed",
            ));
        }
        // A live planner holds no `ProtocolOperation` authority at all, so its
        // inventory exists only so the MCP shell can advertise `submit_plan`
        // and nothing else. A capability that is not the live planner's falls
        // through to the live-run resolver, which answers `unauthorized`.
        Operation::Inventory => {
            match quorum_core::capabilities::resolve_planner_context(&tx, &request.capability) {
                Ok(context) => Response::success(ResponseResult::Inventory {
                    repository: repository.to_string(),
                    task_id: context.task_id,
                    role: "planner".into(),
                    pr: None,
                    review_revision: None,
                    phase: InventoryPhase::Planner,
                    operations: Vec::new(),
                }),
                Err(_) => {
                    let context = resolve_any_role(&tx, &request.capability)?;
                    let operations = ALL_PROTOCOL_OPERATIONS
                        .into_iter()
                        .filter(|operation| operation.allowed_in(context.phase))
                        .collect();
                    Response::success(ResponseResult::Inventory {
                        repository: repository.to_string(),
                        task_id: context.task_id,
                        role: context.role,
                        pr: context.pr,
                        review_revision: context.review_revision,
                        phase: context.phase.into(),
                        operations,
                    })
                }
            }
        }
        Operation::Protocol {
            operation,
            attempt_id,
            operation_id,
            ..
        } => {
            let context = resolve_any_role(&tx, &request.capability)?;
            if !operation.allowed_in(context.phase) {
                return Err(EndpointFailure::new(
                    "forbidden_operation",
                    "operation is not available to this run",
                ));
            }
            // The admitted GitHub tools were routed before this transaction
            // opened; what remains here is the operation read and the tools
            // whose delivery slices have not landed.
            if operation != ProtocolOperation::GithubOperationRead {
                return Err(EndpointFailure::new(
                    "operation_unavailable",
                    "operation is not implemented",
                ));
            }
            github_operation_read(&tx, &context, attempt_id, operation_id)?
        }
    };
    if Instant::now() >= deadline {
        let _ = state.compare_exchange(PROCESSING, CANCELLED, Ordering::AcqRel, Ordering::Acquire);
    }
    state
        .compare_exchange(PROCESSING, COMMITTING, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| EndpointFailure::new("timeout", "request processing timed out"))?;
    if tx.commit().is_err() {
        state.store(COMPLETE, Ordering::Release);
        return Err(EndpointFailure::new(
            "internal",
            "request processing failed",
        ));
    }
    state.store(COMPLETE, Ordering::Release);
    Ok(response)
}

/// Answer `github_operation_read` for the caller's own run.
///
/// Ownership is decided by `read_operation` from the run id the capability
/// resolved to, never from anything the caller names. Every rejection maps to
/// one fixed code so a non-owner learns nothing about the operation.
fn github_operation_read(
    conn: &rusqlite::Connection,
    context: &LiveRunContext,
    attempt_id: Option<String>,
    operation_id: Option<String>,
) -> std::result::Result<Response, EndpointFailure> {
    let invalid = || {
        EndpointFailure::new(
            "invalid_operation",
            "github_operation_read requires a bounded attempt_id and operation_id",
        )
    };
    let attempt_id = attempt_id
        .and_then(|value| CollaborationAttemptId::new(value).ok())
        .ok_or_else(invalid)?;
    let operation_id = operation_id
        .and_then(|value| GithubOperationId::new(value).ok())
        .ok_or_else(invalid)?;
    let read =
        collaboration_admission::read_operation(conn, &context.run_id, &attempt_id, &operation_id)
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    match read {
        GithubOperationReadResult::Found(status) => operation_response(conn, &attempt_id, status),
        GithubOperationReadResult::NotFound => Err(EndpointFailure::new(
            "operation_not_found",
            "operation not found",
        )),
        GithubOperationReadResult::Rejected(_) => Err(EndpointFailure::new(
            "operation_rejected",
            "operation read rejected",
        )),
    }
}

/// The fixed shared retention of one collaboration attempt and its rows.
pub(crate) const ATTEMPT_RETENTION_SECS: i64 = 7 * 24 * 60 * 60;
/// Wall-clock bound on one operation, from admission.
const OPERATION_DEADLINE_SECS: i64 = 60 * 60;
/// GitHub's own default page size, used when `perPage` is absent.
const DEFAULT_READ_PER_PAGE: u32 = 30;

/// Closed `pull_request_read` arguments. `owner`, `repo`, and `pullNumber`
/// are assertions only: the daemon derives the target and rejects any
/// mismatch.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PullRequestReadArguments {
    method: String,
    owner: String,
    repo: String,
    #[serde(rename = "pullNumber")]
    pull_number: i64,
    #[serde(default)]
    page: Option<u32>,
    #[serde(default, rename = "perPage")]
    per_page: Option<u32>,
}

/// Closed `add_issue_comment` arguments; the target fields are assertions.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AddIssueCommentArguments {
    owner: String,
    repo: String,
    issue_number: i64,
    body: String,
    #[serde(default, rename = "clientRequestId")]
    client_request_id: Option<String>,
}

fn decode_arguments<T: serde::de::DeserializeOwned>(
    tool: &str,
    arguments: Option<serde_json::Value>,
) -> std::result::Result<T, EndpointFailure> {
    let arguments = arguments.ok_or_else(|| {
        EndpointFailure::detailed("invalid_operation", format!("{tool} requires arguments"))
    })?;
    serde_json::from_value(arguments).map_err(|error| {
        let mut message = format!("invalid {tool} arguments: {error}");
        message.truncate(200);
        EndpointFailure::detailed("invalid_operation", message)
    })
}

/// The agent-named target must be exactly the run's daemon-derived one.
fn check_target(
    repository: &str,
    owner: &str,
    repo: &str,
    number: i64,
    pr: i64,
) -> std::result::Result<(), EndpointFailure> {
    let same_repository = repository
        .split_once('/')
        .is_some_and(|(o, r)| o.eq_ignore_ascii_case(owner) && r.eq_ignore_ascii_case(repo));
    if same_repository && number == pr {
        Ok(())
    } else {
        Err(EndpointFailure::new(
            "forbidden_operation",
            "target is not the pull request bound to this run",
        ))
    }
}

/// True when the Markdown leaves a fenced code block open, which would
/// swallow the appended daemon marker into authored code.
fn has_unclosed_fence(body: &str) -> bool {
    let mut open: Option<(char, usize)> = None;
    for line in body.lines() {
        let rest = line.trim_start_matches(' ');
        if line.len() - rest.len() > 3 {
            continue;
        }
        let Some(fence) = rest.chars().next().filter(|c| matches!(c, '`' | '~')) else {
            continue;
        };
        let run = rest.chars().take_while(|c| *c == fence).count();
        if run < 3 {
            continue;
        }
        let info = &rest[run..];
        match open {
            None if fence == '`' && info.contains('`') => {}
            None => open = Some((fence, run)),
            Some((open_fence, open_run))
                if fence == open_fence && run >= open_run && info.trim().is_empty() =>
            {
                open = None;
            }
            Some(_) => {}
        }
    }
    open.is_some()
}

/// One authored Markdown body: non-blank, NUL-free, within the per-body
/// bound, and with every code fence closed so the appended daemon marker
/// cannot be swallowed into authored code.
fn validate_authored_body(body: &str) -> std::result::Result<(), EndpointFailure> {
    if body.trim().is_empty() || body.contains('\0') {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "comment body must be non-empty text without NUL",
        ));
    }
    if body.len() > MAX_COMMENT_BODY_BYTES {
        return Err(EndpointFailure::detailed(
            "invalid_operation",
            format!("comment body exceeds {MAX_COMMENT_BODY_BYTES} bytes"),
        ));
    }
    if has_unclosed_fence(body) {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "comment body leaves a fenced code block open",
        ));
    }
    Ok(())
}

fn parse_client_request_id(
    value: Option<String>,
) -> std::result::Result<Option<ClientRequestId>, EndpointFailure> {
    value
        .map(|value| {
            ClientRequestId::new(value)
                .ok()
                .filter(|id| id.as_str().chars().all(|c| c.is_ascii_graphic()))
                .ok_or_else(|| {
                    EndpointFailure::new(
                        "invalid_operation",
                        "clientRequestId must be 1-128 printable ASCII characters",
                    )
                })
        })
        .transpose()
}

/// Closed `pull_request_review_write` arguments. `owner`, `repo`, and
/// `pullNumber` are assertions; a review ID is accepted only so that naming
/// one is a typed refusal rather than a generic unknown field.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PullRequestReviewWriteArguments {
    method: String,
    owner: String,
    repo: String,
    #[serde(rename = "pullNumber")]
    pull_number: i64,
    #[serde(default, rename = "commitID")]
    commit_id: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default, rename = "clientRequestId")]
    client_request_id: Option<String>,
    #[serde(default, rename = "reviewID", alias = "reviewId", alias = "review_id")]
    review_id: Option<serde_json::Value>,
}

/// Closed `add_comment_to_pending_review` arguments. Anchor fields stay raw
/// so every malformed anchor reaches the typed validator.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AddCommentToPendingReviewArguments {
    owner: String,
    repo: String,
    #[serde(rename = "pullNumber")]
    pull_number: i64,
    path: String,
    #[serde(default)]
    line: Option<u64>,
    #[serde(default)]
    side: Option<String>,
    #[serde(default, rename = "startLine")]
    start_line: Option<u64>,
    #[serde(default, rename = "startSide")]
    start_side: Option<String>,
    #[serde(default, rename = "subjectType")]
    subject_type: Option<String>,
    body: String,
    #[serde(default, rename = "clientRequestId")]
    client_request_id: Option<String>,
}

/// One validated tool call, ready for durable admission.
struct PreparedOperation {
    kind: GithubOperationKind,
    request_json: GithubOperationRequestJson,
    client_request_id: Option<ClientRequestId>,
}

/// Every write of one attempt's pending review shares one identity per step,
/// so a repeated `create` resumes the attempt's review and a repeated
/// `submit_pending` returns the sealing submit instead of a second review.
const REVIEW_CREATE_CLIENT_ID: &str = "pending-review-create";
const REVIEW_SUBMIT_CLIENT_ID: &str = "pending-review-submit";

fn review_request_json(
    value: serde_json::Value,
) -> std::result::Result<GithubOperationRequestJson, EndpointFailure> {
    GithubOperationRequestJson::new(&value.to_string()).map_err(|_| {
        EndpointFailure::new(
            "invalid_operation",
            "request is outside the supported bounds",
        )
    })
}

/// Validate a `pull_request_review_write` call. Only `create` (for exactly
/// the immutable launch SHA) and `submit_pending` (with `event=COMMENT`)
/// exist; nothing here can approve, request changes, dismiss, or delete.
fn prepare_review_write(
    repository: &str,
    pr: i64,
    launch_sha: &str,
    arguments: Option<serde_json::Value>,
) -> std::result::Result<PreparedOperation, EndpointFailure> {
    let arguments: PullRequestReviewWriteArguments =
        decode_arguments("pull_request_review_write", arguments)?;
    check_target(
        repository,
        &arguments.owner,
        &arguments.repo,
        arguments.pull_number,
        pr,
    )?;
    if arguments.review_id.is_some() {
        return Err(EndpointFailure::new(
            "review_id_unsupported",
            "a review ID cannot be named; the daemon owns this run's one pending review",
        ));
    }
    if arguments.client_request_id.is_some() {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "pull_request_review_write takes no clientRequestId; a repeated call resumes the same review step",
        ));
    }
    if let Some(body) = &arguments.body {
        validate_authored_body(body)?;
    }
    let (request, client_id) = match arguments.method.as_str() {
        "create" => {
            if arguments.event.is_some() {
                return Err(EndpointFailure::new(
                    "review_event_unsupported",
                    "create takes no event; the review stays pending until submit_pending",
                ));
            }
            if arguments.commit_id.as_deref() != Some(launch_sha) {
                return Err(EndpointFailure::new(
                    "review_commit_mismatch",
                    "commitID must equal this review run's launch SHA",
                ));
            }
            let mut request = json!({"method": "create", "commitID": launch_sha});
            if let Some(body) = arguments.body {
                request["body"] = json!(body);
            }
            (request, REVIEW_CREATE_CLIENT_ID)
        }
        "submit_pending" => {
            if arguments.event.as_deref() != Some("COMMENT") {
                return Err(EndpointFailure::new(
                    "review_event_unsupported",
                    "submit_pending accepts only event=COMMENT; the verdict is submitted separately",
                ));
            }
            if arguments.commit_id.is_some() {
                return Err(EndpointFailure::new(
                    "invalid_operation",
                    "submit_pending takes no commitID",
                ));
            }
            let Some(body) = arguments.body else {
                return Err(EndpointFailure::new(
                    "invalid_operation",
                    "submit_pending requires the complete review summary body",
                ));
            };
            (
                json!({"method": "submit_pending", "event": "COMMENT", "body": body}),
                REVIEW_SUBMIT_CLIENT_ID,
            )
        }
        _ => {
            return Err(EndpointFailure::new(
                "review_method_unsupported",
                "only method=create and method=submit_pending are available",
            ))
        }
    };
    Ok(PreparedOperation {
        kind: GithubOperationKind::PullRequestReviewWrite,
        request_json: review_request_json(request)?,
        client_request_id: Some(
            ClientRequestId::new(client_id)
                .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?,
        ),
    })
}

/// The pure half of `add_comment_to_pending_review`: target, body, path, and
/// client id. The anchor itself is validated against the launch-SHA diff.
fn decode_pending_comment(
    repository: &str,
    pr: i64,
    arguments: Option<serde_json::Value>,
) -> std::result::Result<(InlineAnchor, String, Option<ClientRequestId>), EndpointFailure> {
    let arguments: AddCommentToPendingReviewArguments =
        decode_arguments("add_comment_to_pending_review", arguments)?;
    check_target(
        repository,
        &arguments.owner,
        &arguments.repo,
        arguments.pull_number,
        pr,
    )?;
    validate_authored_body(&arguments.body)?;
    if arguments.path.is_empty()
        || arguments.path.len() > MAX_ANCHOR_PATH_BYTES
        || arguments.path.contains('\0')
    {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "path must be a non-empty repository path without NUL",
        ));
    }
    let client_request_id = parse_client_request_id(arguments.client_request_id)?;
    let anchor = InlineAnchor {
        path: arguments.path,
        line: arguments.line,
        side: arguments.side,
        start_line: arguments.start_line,
        start_side: arguments.start_side,
        subject_type: arguments.subject_type,
    };
    Ok((anchor, arguments.body, client_request_id))
}

/// Validate one inline anchor against the launch-SHA diff and build the
/// stored request. Only a fully validated anchor is ever admitted; an
/// unparseable diff refuses the call rather than skipping validation.
fn prepare_pending_comment(
    launch_sha: &str,
    diff_sha: &str,
    diff: &[u8],
    anchor: &InlineAnchor,
    body: String,
    client_request_id: Option<ClientRequestId>,
) -> std::result::Result<PreparedOperation, EndpointFailure> {
    let diff = LaunchDiff::parse(diff_sha, diff).map_err(|error| {
        EndpointFailure::detailed(
            "review_diff_unavailable",
            format!("the launch SHA diff could not be read: {}", error.code()),
        )
    })?;
    let validated = diff.validate(launch_sha, anchor).map_err(anchor_failure)?;
    let mut request = serde_json::to_value(&validated)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    request["subjectType"] = json!("LINE");
    request["body"] = json!(body);
    Ok(PreparedOperation {
        kind: GithubOperationKind::AddCommentToPendingReview,
        request_json: review_request_json(request)?,
        client_request_id,
    })
}

/// Validate one tool call against its closed schema and the run's target,
/// and build the stored closed Repository Service request.
fn prepare_github_request(
    repository: &str,
    pr: i64,
    operation: ProtocolOperation,
    arguments: Option<serde_json::Value>,
) -> std::result::Result<PreparedOperation, EndpointFailure> {
    let (kind, request, client_request_id) = match operation {
        ProtocolOperation::PullRequestRead => {
            let arguments: PullRequestReadArguments =
                decode_arguments("pull_request_read", arguments)?;
            check_target(
                repository,
                &arguments.owner,
                &arguments.repo,
                arguments.pull_number,
                pr,
            )?;
            let page = arguments.page.unwrap_or(1);
            let per_page = arguments.per_page.unwrap_or(DEFAULT_READ_PER_PAGE);
            let paged = |kind: &str| json!({"kind": kind, "page": page, "per_page": per_page});
            let request = match arguments.method.as_str() {
                "get" => json!({"kind": "get_pull_request"}),
                "get_diff" => json!({"kind": "get_diff", "page": page}),
                "get_files" => paged("get_files"),
                "get_review_comments" => paged("get_review_comments"),
                "get_reviews" => paged("get_reviews"),
                "get_comments" => paged("get_comments"),
                _ => {
                    return Err(EndpointFailure::new(
                        "invalid_operation",
                        "unsupported pull_request_read method",
                    ))
                }
            };
            (GithubOperationKind::PullRequestRead, request, None)
        }
        ProtocolOperation::AddIssueComment => {
            let arguments: AddIssueCommentArguments =
                decode_arguments("add_issue_comment", arguments)?;
            check_target(
                repository,
                &arguments.owner,
                &arguments.repo,
                arguments.issue_number,
                pr,
            )?;
            let body = arguments.body;
            validate_authored_body(&body)?;
            let client_request_id = parse_client_request_id(arguments.client_request_id)?;
            (
                GithubOperationKind::AddIssueComment,
                json!({"kind": "add_issue_comment", "body": body}),
                client_request_id,
            )
        }
        _ => {
            return Err(EndpointFailure::new(
                "internal",
                "request processing failed",
            ))
        }
    };
    // The executor decodes exactly this document; refuse here anything it
    // would refuse there, so a bad request is never admitted.
    let text = request.to_string();
    RepositoryRequest::decode_agent_json(text.as_bytes()).map_err(|_| {
        EndpointFailure::new(
            "invalid_operation",
            "request is outside the supported bounds",
        )
    })?;
    let request_json = GithubOperationRequestJson::new(&text).map_err(|_| {
        EndpointFailure::new(
            "invalid_operation",
            "request is outside the supported bounds",
        )
    })?;
    Ok(PreparedOperation {
        kind,
        request_json,
        client_request_id,
    })
}

fn admission_code(error: CollaborationAdmissionError) -> String {
    serde_json::to_value(error)
        .ok()
        .and_then(|value| {
            value
                .get("code")
                .and_then(|code| code.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "rejected".to_owned())
}

/// Admit one `pull_request_read` or `add_issue_comment` call into the
/// durable outbox for the daemon executor. Authority, phase, the target, and
/// the run's collaboration attempt all come from daemon state; nothing here
/// contacts GitHub. The answer is the operation's current bounded status.
fn admit_github_operation(
    db_path: &Path,
    repository: &str,
    capability: &str,
    operation: ProtocolOperation,
    arguments: Option<serde_json::Value>,
) -> std::result::Result<Response, EndpointFailure> {
    let mut conn = quorum_core::db::open(db_path)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let (context, pr) = resolve_github_context(&mut conn, capability, operation)?;
    let prepared = prepare_github_request(repository, pr, operation, arguments)?;
    enqueue_operation(&mut conn, &context, pr, prepared)
}

/// A short authority read for one GitHub tool call: the live run, its phase
/// gate, and its bound pull request. Every write re-derives authority in its
/// own guarded transaction.
fn resolve_github_context(
    conn: &mut rusqlite::Connection,
    capability: &str,
    operation: ProtocolOperation,
) -> std::result::Result<(LiveRunContext, i64), EndpointFailure> {
    let context = {
        let tx = conn
            .transaction()
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
        resolve_any_role(&tx, capability)?
    };
    if !operation.allowed_in(context.phase) {
        return Err(EndpointFailure::new(
            "forbidden_operation",
            "operation is not available to this run",
        ));
    }
    let pr = context.pr.ok_or(EndpointFailure::new(
        "forbidden_operation",
        "this run has no bound pull request",
    ))?;
    Ok((context, pr))
}

/// Durably admit one validated call for the run's collaboration attempt.
fn enqueue_operation(
    conn: &mut rusqlite::Connection,
    context: &LiveRunContext,
    pr: i64,
    prepared: PreparedOperation,
) -> std::result::Result<Response, EndpointFailure> {
    let internal = || EndpointFailure::new("internal", "request processing failed");
    let PreparedOperation {
        kind,
        request_json,
        client_request_id,
    } = prepared;
    let role: CollaborationRole = context.role.parse().map_err(|_| internal())?;
    let run_id = RunCapabilityId::new(context.run_id.clone()).map_err(|_| internal())?;
    let now = quorum_core::clock::now();
    let fresh_attempt = CollaborationAttemptId::new(format!("att-{}", uuid::Uuid::new_v4()))
        .map_err(|_| internal())?;
    let attempt = match collaboration_admission::ensure_run_attempt(
        conn,
        &run_id,
        role,
        &fresh_attempt,
        ATTEMPT_RETENTION_SECS,
        now,
    )
    .map_err(|_| internal())?
    {
        RunAttemptResolution::Bound(status) => status,
        RunAttemptResolution::Rejected(CollaborationAdmissionError::CapabilityRejected) => {
            return Err(EndpointFailure::new(
                "unauthorized",
                "run authority rejected",
            ))
        }
        RunAttemptResolution::Rejected(rejection) => {
            return Err(EndpointFailure::detailed(
                "operation_unavailable",
                format!(
                    "no collaboration attempt is available for this run: {}",
                    admission_code(rejection)
                ),
            ))
        }
    };

    // Reads are side-effect free, so each call is its own operation. A write
    // without a caller id derives one from its content, so an identical retry
    // returns the original operation instead of posting twice.
    let client_request_id = match client_request_id {
        Some(id) => id,
        None if kind == GithubOperationKind::PullRequestRead => {
            ClientRequestId::new(format!("read-{}", uuid::Uuid::new_v4()))
                .map_err(|_| internal())?
        }
        None => collaboration_admission::derive_client_request_id(kind, &request_json),
    };
    let reviewer_head_sha = match role {
        CollaborationRole::Reviewer => Some(
            context
                .review_revision
                .clone()
                .and_then(|sha| ReviewerHeadSha::new(sha).ok())
                .ok_or_else(internal)?,
        ),
        CollaborationRole::Worker => None,
    };
    let operation_id =
        collaboration_admission::derive_operation_id(&attempt.attempt_id, &client_request_id);
    let marker = collaboration_admission::derive_github_marker(
        &attempt.attempt_id,
        &client_request_id,
        kind,
    );
    let request = EnqueueGithubOperationRequest::new(
        operation_id,
        client_request_id,
        attempt.attempt_id.clone(),
        run_id,
        context.task_id,
        CollaborationAgent::new(context.agent.clone()).map_err(|_| internal())?,
        role,
        pr,
        reviewer_head_sha,
        kind,
        request_json,
        marker,
        now.saturating_add(OPERATION_DEADLINE_SECS)
            .min(attempt.expires_at),
        now,
        attempt.expires_at,
    )
    .map_err(|_| internal())?;
    match collaboration_admission::admit_operation(conn, &context.run_id, &request, now)
        .map_err(|_| internal())?
    {
        GithubOperationAdmissionResult::Enqueued(status)
        | GithubOperationAdmissionResult::Existing(status) => {
            operation_response(conn, &attempt.attempt_id, status)
        }
        GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::CapabilityRejected,
        ) => Err(EndpointFailure::new(
            "unauthorized",
            "run authority rejected",
        )),
        GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::RunOperationLimitReached
            | CollaborationAdmissionError::AttemptOperationLimitReached
            | CollaborationAdmissionError::TaskOperationLimitReached
            | CollaborationAdmissionError::RepositoryOperationLimitReached,
        ) => Err(EndpointFailure::new(
            "operation_limit_reached",
            "the retained GitHub operation limit for this run is reached",
        )),
        GithubOperationAdmissionResult::Rejected(rejection) => Err(review_rejection(rejection)
            .unwrap_or_else(|| {
                EndpointFailure::detailed(
                    "operation_rejected",
                    format!(
                        "operation admission rejected: {}",
                        admission_code(rejection)
                    ),
                )
            })),
    }
}

/// The typed endpoint code for a review-sequence admission refusal, or
/// `None` for a rejection outside the pending-review vocabulary.
fn review_rejection(rejection: CollaborationAdmissionError) -> Option<EndpointFailure> {
    use CollaborationAdmissionError as E;
    let (code, message) = match rejection {
        E::ReviewRoleRejected => (
            "forbidden_operation",
            "pending-review tools belong to the reviewer run",
        ),
        E::ReviewNotCreated => (
            "review_not_created",
            "create the pending review for the launch SHA first",
        ),
        E::ReviewAlreadyCreated => (
            "review_already_created",
            "this run's pending review already exists",
        ),
        E::ReviewSealed => (
            "review_sealed",
            "the pending review was already submitted; no further findings are accepted",
        ),
        E::ReviewInlineLimitReached => (
            "review_inline_limit_reached",
            "the pending review already holds its maximum number of inline findings",
        ),
        E::ReviewBodyTooLarge => (
            "review_body_too_large",
            "the review body exceeds its byte bound",
        ),
        _ => return None,
    };
    Some(EndpointFailure::new(code, message))
}

/// The typed endpoint code for a verdict-proof refusal. Each one recorded
/// nothing: the verdict may be submitted again once its cause is resolved.
fn verdict_refusal(rejection: CollaborationAdmissionError) -> EndpointFailure {
    use CollaborationAdmissionError as E;
    let detail = admission_code(rejection);
    match rejection {
        E::CapabilityRejected => EndpointFailure::new("unauthorized", "run authority rejected"),
        E::ReviewNotSealed
        | E::ReviewFinalOperationInvalid
        | E::ReviewSequenceIncomplete
        | E::ReviewPublicationPending => EndpointFailure::detailed(
            "review_not_published",
            format!(
                "the verdict waits until this run's COMMENT review is fully published ({detail})"
            ),
        ),
        E::ReviewHeadMismatch => EndpointFailure::new(
            "review_head_moved",
            "the pull request head no longer equals this review's launch SHA",
        ),
        E::ReviewPublicationNotFailed => EndpointFailure::new(
            "review_publication_not_failed",
            "publication_failure requires a failed pending-review operation and none in flight",
        ),
        _ => EndpointFailure::detailed(
            "operation_rejected",
            format!("reviewer verdict rejected: {detail}"),
        ),
    }
}

/// The binding of this run's live collaboration attempt, when that attempt
/// has begun a pending review (its `create` was admitted).
fn started_review_attempt(
    conn: &rusqlite::Connection,
    run_id: &str,
    now: i64,
) -> std::result::Result<Option<CreateCollaborationAttemptRequest>, EndpointFailure> {
    let internal = || EndpointFailure::new("internal", "request processing failed");
    type Row = (
        String,
        i64,
        String,
        String,
        i64,
        Option<String>,
        i64,
        i64,
        i64,
    );
    let row: Option<Row> = conn
        .query_row(
            "SELECT attempt_id,task_id,agent,role,pr_number,head_sha,lifecycle_generation,
                    created_at,expires_at
             FROM github_collaboration_attempts
             WHERE active_run_id=?1 AND expires_at>?2 AND next_review_sequence>0",
            params![run_id, now],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .optional()
        .map_err(|_| internal())?;
    let Some((attempt_id, task_id, agent, role, pr, head_sha, generation, created_at, expires_at)) =
        row
    else {
        return Ok(None);
    };
    CreateCollaborationAttemptRequest::new(
        CollaborationAttemptId::new(attempt_id).map_err(|_| internal())?,
        task_id,
        CollaborationAgent::new(agent).map_err(|_| internal())?,
        role.parse().map_err(|_| internal())?,
        pr,
        head_sha
            .map(ReviewerHeadSha::new)
            .transpose()
            .map_err(|_| internal())?,
        generation,
        RunCapabilityId::new(run_id).map_err(|_| internal())?,
        created_at,
        expires_at,
    )
    .map(Some)
    .map_err(|_| internal())
}

/// Fetch the remote PR head for a reviewer verdict, outside any transaction,
/// when the run's attempt has begun a pending review. A run with no started
/// review needs no proof (see `review_verdict_guard`), so it costs no fetch.
/// Authority read here is advisory: the write transaction decides it again.
async fn verdict_head_evidence(
    db_path: &Path,
    capability: &str,
    review_evidence: Arc<dyn ReviewEvidence>,
) -> std::result::Result<Option<String>, EndpointFailure> {
    let (db_path, capability) = (db_path.to_path_buf(), capability.to_owned());
    let pr = in_blocking(move || {
        let conn = quorum_core::db::open(&db_path)
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
        let Ok(context) = resolve(&conn, &capability, "reviewer") else {
            return Ok(None);
        };
        Ok(
            started_review_attempt(&conn, &context.run_id, quorum_core::clock::now())?
                .map(|attempt| attempt.pr_number()),
        )
    })
    .await?;
    let Some(pr) = pr else {
        return Ok(None);
    };
    match tokio::time::timeout(REVIEW_HEAD_TIMEOUT, review_evidence.pull_request_head(pr)).await {
        Ok(Ok(head)) => Ok(Some(head)),
        Ok(Err(_)) | Err(_) => Err(EndpointFailure::new(
            "review_head_unavailable",
            "the pull request head could not be fetched for the verdict proof; nothing was recorded",
        )),
    }
}

/// The reviewer verdict guard, inside the verdict's own `BEGIN IMMEDIATE`.
///
/// Once this run's attempt has begun a pending review, its verdict is
/// admitted only by the core proof: the attempt is sealed, every review step
/// through the final COMMENT submit succeeded, and the launch SHA still
/// equals the freshly fetched head — or, with an explicit
/// `publication_failure=operation_failed`, nothing is in flight and a step
/// failed. Admission completes the attempt in the same transaction.
///
/// A reviewer run that never began a pending review is admitted unchanged.
/// Reviewer prompts do not yet teach the pending-review sequence and not
/// every provider has the review tools, so requiring a published review from
/// every reviewer now would refuse all of their verdicts; the prompt cutover
/// is where that requirement becomes unconditional.
fn review_verdict_guard(
    tx: &Transaction<'_>,
    context: &LiveRunContext,
    publication_failure: Option<&str>,
    fetched_head: Option<&str>,
) -> std::result::Result<(), EndpointFailure> {
    let internal = || EndpointFailure::new("internal", "request processing failed");
    let reason = match publication_failure {
        None => None,
        Some("operation_failed") => Some(ReviewPublicationFailure::OperationFailed),
        Some(_) => {
            return Err(EndpointFailure::new(
                "invalid_operation",
                "publication_failure must be operation_failed",
            ))
        }
    };
    let now = quorum_core::clock::now();
    let Some(attempt) = started_review_attempt(tx, &context.run_id, now)? else {
        return match reason {
            Some(_) => Err(EndpointFailure::new(
                "review_publication_not_failed",
                "publication_failure requires a pending review that this run began",
            )),
            None => Ok(()),
        };
    };
    let fetched_head = fetched_head
        .map(ReviewerHeadSha::new)
        .transpose()
        .map_err(|_| {
            EndpointFailure::new(
                "review_head_unavailable",
                "the fetched pull request head is invalid; nothing was recorded",
            )
        })?;
    let evidence = match (reason, fetched_head.as_ref()) {
        (Some(reason), _) => ReviewVerdictEvidence::PublicationFailed { reason },
        (None, Some(fetched_head_sha)) => ReviewVerdictEvidence::Published { fetched_head_sha },
        // The review began after the pre-transaction head check: it cannot
        // have published yet, and there is no fresh head to prove against.
        (None, None) => {
            return Err(EndpointFailure::new(
                "review_not_published",
                "the verdict waits until this run's COMMENT review is fully published",
            ))
        }
    };
    match collaboration_admission::admit_review_verdict_in_tx(tx, &attempt, evidence, now)
        .map_err(|_| internal())?
    {
        CollaborationAttemptTransitionResult::Transitioned(_)
        | CollaborationAttemptTransitionResult::Existing(_) => Ok(()),
        CollaborationAttemptTransitionResult::Rejected(rejection) => {
            Err(verdict_refusal(rejection))
        }
    }
}

/// Admit one `pull_request_review_write` or `add_comment_to_pending_review`
/// call. Authority, the phase gate (reviewer only), the target, and the
/// immutable launch SHA come from daemon state. An inline anchor is validated
/// against the launch-SHA diff, obtained with no transaction open, before
/// anything is enqueued; the review-sequence store then assigns the step its
/// durable order under its own bounds.
async fn admit_review_operation(
    db_path: PathBuf,
    repository: String,
    capability: String,
    operation: ProtocolOperation,
    arguments: Option<serde_json::Value>,
    review_evidence: Arc<dyn ReviewEvidence>,
) -> std::result::Result<Response, EndpointFailure> {
    let read_db = db_path.clone();
    let (context, pr, base_branch) = in_blocking(move || {
        let mut conn = quorum_core::db::open(&read_db)
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
        let (context, pr) = resolve_github_context(&mut conn, &capability, operation)?;
        let base_branch: Option<String> = conn
            .query_row(
                "SELECT target_branch FROM tasks WHERE id=?1",
                [context.task_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?
            .flatten();
        Ok((context, pr, base_branch))
    })
    .await?;
    let launch_sha = context
        .review_revision
        .clone()
        .ok_or_else(|| EndpointFailure::new("internal", "request processing failed"))?;
    let prepared = match operation {
        ProtocolOperation::PullRequestReviewWrite => {
            prepare_review_write(&repository, pr, &launch_sha, arguments)?
        }
        ProtocolOperation::AddCommentToPendingReview => {
            let (anchor, body, client_request_id) =
                decode_pending_comment(&repository, pr, arguments)?;
            let unavailable = |detail: &str| {
                EndpointFailure::detailed(
                    "review_diff_unavailable",
                    format!("the launch SHA diff is unavailable: {detail}"),
                )
            };
            let base_branch =
                base_branch.ok_or_else(|| unavailable("the task has no target branch"))?;
            let (diff_sha, diff) = match tokio::time::timeout(
                REVIEW_DIFF_TIMEOUT,
                review_evidence.launch_diff(&base_branch, &launch_sha),
            )
            .await
            {
                Ok(Ok(diff)) => diff,
                Ok(Err(_)) => return Err(unavailable("git diff failed")),
                Err(_) => return Err(unavailable("git diff timed out")),
            };
            // Parsing a diff at its bound is CPU work; keep it off the runtime.
            in_blocking(move || {
                prepare_pending_comment(
                    &launch_sha,
                    &diff_sha,
                    &diff,
                    &anchor,
                    body,
                    client_request_id,
                )
            })
            .await?
        }
        _ => {
            return Err(EndpointFailure::new(
                "internal",
                "request processing failed",
            ))
        }
    };
    in_blocking(move || {
        let mut conn = quorum_core::db::open(&db_path)
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
        enqueue_operation(&mut conn, &context, pr, prepared)
    })
    .await
}

/// The closed status answer for one operation the caller is authorized to
/// see, with its typed failure class.
fn operation_response(
    conn: &rusqlite::Connection,
    attempt_id: &CollaborationAttemptId,
    status: GithubOperationStatus,
) -> std::result::Result<Response, EndpointFailure> {
    let internal = || EndpointFailure::new("internal", "request processing failed");
    let result = status
        .response_json
        .as_ref()
        .map(|json| serde_json::from_str(json.as_str()))
        .transpose()
        .map_err(|_| internal())?;
    let error_kind: Option<String> = conn
        .query_row(
            "SELECT error_kind FROM github_agent_operations WHERE operation_id=?1",
            [status.operation_id.as_str()],
            |row| row.get(0),
        )
        .map_err(|_| internal())?;
    let error_code = github_error_code(status.state, status.send_state, error_kind.as_deref());
    Ok(Response::success(ResponseResult::GithubOperation {
        attempt_id: attempt_id.as_str().to_owned(),
        operation_id: status.operation_id.as_str().to_owned(),
        state: status.state,
        send_state: status.send_state,
        remote_object_id: status
            .remote_object_id
            .map(|value| value.as_str().to_owned()),
        result,
        error_summary: status.error_summary.map(|value| value.as_str().to_owned()),
        error_code,
    }))
}

/// Classify a failed (or failing-and-retrying) operation into the closed
/// error vocabulary agents see. Outages and rate limits are distinct from
/// GitHub's definitive rejections and from a write whose effect is unknown.
fn github_error_code(
    state: GithubOperationState,
    send_state: GithubSendState,
    error_kind: Option<&str>,
) -> Option<&'static str> {
    match state {
        GithubOperationState::Succeeded => return None,
        GithubOperationState::Cancelled => return Some("github_operation_cancelled"),
        GithubOperationState::Failed if send_state == GithubSendState::Ambiguous => {
            return Some("github_outcome_unknown")
        }
        GithubOperationState::Failed => {}
        GithubOperationState::Queued | GithubOperationState::Running => {
            error_kind?;
        }
    }
    Some(match error_kind {
        Some("rate_limited") => "github_rate_limited",
        Some(
            "transient_failure"
            | "timed_out"
            | "spawn_failed"
            | "credential_unavailable"
            | "deadline_exceeded"
            | "attempts_exhausted"
            | "interrupted",
        ) => "github_unavailable",
        Some("permanent_failure") => "github_rejected",
        Some("response_too_large" | "invalid_response" | "result_too_large" | "invalid_result") => {
            "github_invalid_response"
        }
        _ => "github_operation_failed",
    })
}

/// Authoritative facts a plan submission needs before it can be validated.
struct PlanSubmissionContext {
    graph_id: i64,
    source_dependency_ids: Vec<i64>,
}

/// One plan submission that survived the read-and-validate phase.
enum PreparedPlan {
    /// The plan is valid; `serialized` is what gets recorded.
    Valid { graph_id: i64, serialized: String },
    /// The plan was rejected; `message` is the validator's own text.
    Invalid { graph_id: i64, message: String },
}

/// Accept one planner plan submission.
///
/// Ordering is load-bearing. Authority, the once-only guard, and the rejection
/// budget are answered from a short read before any work is done; validation
/// then runs with no transaction open because `validate_for_source` is async
/// and canonicalizes filesystem paths; only the outcome is recorded inside
/// `BEGIN IMMEDIATE`. The already-accepted check precedes the budget check so a
/// run whose plan already stands reports `already_submitted` rather than a
/// budget failure it can do nothing about.
///
/// Bounding differs from the other operations by exactly one step. The
/// read-and-validate phase writes nothing, so abandoning it is safe and it
/// carries the same `PROCESS_TIMEOUT` deadline every operation has. The
/// recording phase is deliberately **not** deadlined: `spawn_blocking` work
/// cannot be aborted, so a cancelled wait could report a timeout for a commit
/// that already landed. It is bounded only by `busy_timeout` on its
/// connection, so a submission can outlive the client's own read timeout. That
/// is safe but can mislead: acceptance is one atomic guarded write, so a client
/// that gives up must treat the outcome as unknown and re-check, never as
/// "failed, resubmit" — a corrected resubmission would receive
/// `already_submitted` while the first plan stands.
///
/// Every call for an issued run lands exactly one `planner_submission_calls`
/// row. A write-phase outcome and its row share one transaction; a refusal from
/// the non-writing phase records its row afterwards through
/// `record_refused_call`, so an abandoned prepare can never add a second one.
async fn submit_plan(
    db_path: PathBuf,
    plan_validation: PlanValidation,
    capability: String,
    response: serde_json::Value,
    process_timeout: Duration,
) -> std::result::Result<Response, EndpointFailure> {
    let received_at = quorum_core::clock::now();
    // The deadline, not timer-vs-task scheduling, decides a timeout: a prepare
    // that resolves at or after the deadline is a timeout even if it won the
    // poll race against the timer (which only fires on a later driver tick).
    let deadline = tokio::time::Instant::now() + process_timeout;
    let prepared = match tokio::time::timeout_at(
        deadline,
        prepare_and_validate_plan(
            db_path.clone(),
            plan_validation,
            capability.clone(),
            response,
        ),
    )
    .await
    {
        Ok(prepared) if tokio::time::Instant::now() < deadline => prepared,
        _ => Err(EndpointFailure::new(
            "timeout",
            "request processing timed out",
        )),
    };
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(failure) => {
            let category = prepare_failure_category(failure.code);
            return Err(
                record_refused_call(db_path, capability, category, received_at, failure).await,
            );
        }
    };

    match prepared {
        PreparedPlan::Invalid { graph_id, message } => {
            in_write_phase(
                db_path,
                capability,
                received_at,
                move |db_path, capability| {
                    record_plan_rejection(db_path, capability, graph_id, received_at)
                },
            )
            .await?;
            Err(EndpointFailure::detailed("invalid_plan", message))
        }
        PreparedPlan::Valid {
            graph_id,
            serialized,
        } => {
            let outcome = in_write_phase(
                db_path,
                capability,
                received_at,
                move |db_path, capability| {
                    record_plan_acceptance(db_path, capability, graph_id, &serialized, received_at)
                },
            )
            .await?;
            match outcome {
                SubmitOutcome::Accepted => {
                    Ok(Response::success(ResponseResult::PlanAccepted { graph_id }))
                }
                SubmitOutcome::AlreadySubmitted => Err(EndpointFailure::new(
                    "already_submitted",
                    "a plan was already accepted for this run",
                )),
                SubmitOutcome::BudgetExhausted => Err(EndpointFailure::new(
                    "submit_budget_exhausted",
                    "plan submission budget is exhausted",
                )),
            }
        }
    }
}

/// The ledger category of a refusal from the non-writing phase. Closed over the
/// failure code, so no message text can choose a category.
fn prepare_failure_category(code: &str) -> RejectionCategory {
    match code {
        "unauthorized" => RejectionCategory::Unauthorized,
        "already_submitted" => RejectionCategory::AlreadySubmitted,
        "submit_budget_exhausted" => RejectionCategory::BudgetExhausted,
        "timeout" => RejectionCategory::Timeout,
        _ => RejectionCategory::Internal,
    }
}

/// Run one recording step. An `internal` failure means its transaction rolled
/// back, taking the call's ledger row with it, so the call gets one attempt at
/// an `internal` row instead. An `unauthorized` failure already committed its row.
async fn in_write_phase<T, F>(
    db_path: PathBuf,
    capability: String,
    received_at: i64,
    work: F,
) -> std::result::Result<T, EndpointFailure>
where
    F: FnOnce(&Path, &str) -> std::result::Result<T, EndpointFailure> + Send + 'static,
    T: Send + 'static,
{
    let (write_db_path, write_capability) = (db_path.clone(), capability.clone());
    match in_blocking(move || work(&write_db_path, &write_capability)).await {
        Err(failure) if failure.code == "internal" => Err(record_refused_call(
            db_path,
            capability,
            RejectionCategory::Internal,
            received_at,
            failure,
        )
        .await),
        result => result,
    }
}

/// Record a call that no write-phase transaction covers, then return `failure`.
/// If the row cannot be written the call fails as `internal`, never as the
/// clean refusal it would otherwise have reported.
async fn record_refused_call(
    db_path: PathBuf,
    capability: String,
    category: RejectionCategory,
    received_at: i64,
    failure: EndpointFailure,
) -> EndpointFailure {
    let recorded =
        in_blocking(move || record_call_row(&db_path, &capability, category, received_at)).await;
    match recorded {
        Ok(()) => failure,
        Err(internal) => internal,
    }
}

/// Append one refused call in its own transaction. A capability matching no
/// issued run writes nothing: `record_call` keeps it out of the ledger.
fn record_call_row(
    db_path: &Path,
    capability: &str,
    category: RejectionCategory,
    received_at: i64,
) -> std::result::Result<(), EndpointFailure> {
    let mut conn = quorum_core::db::open(db_path)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let tx = begin_immediate(&mut conn)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    planner_submissions::record_call(
        &tx,
        capability,
        CallOutcome::Rejected(category),
        received_at,
    )
    .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    tx.commit()
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))
}

/// The whole non-writing phase: the short authority read, then full plan
/// validation. Nothing here writes, so the caller may abandon it on a deadline.
async fn prepare_and_validate_plan(
    db_path: PathBuf,
    plan_validation: PlanValidation,
    capability: String,
    response: serde_json::Value,
) -> std::result::Result<PreparedPlan, EndpointFailure> {
    let context = {
        let capability = capability.clone();
        in_blocking(move || prepare_plan_submission(&db_path, &capability)).await?
    };
    let serialized = serde_json::to_string(&response)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    Ok(
        match validate_plan_submission(
            &serialized,
            &plan_validation,
            &context.source_dependency_ids,
        )
        .await
        {
            Err(message) => PreparedPlan::Invalid {
                graph_id: context.graph_id,
                message,
            },
            Ok(()) => PreparedPlan::Valid {
                graph_id: context.graph_id,
                serialized,
            },
        },
    )
}

/// Run one bounded blocking database step. `spawn_blocking` work is not
/// abortable, so joining it always reports the step's real outcome.
async fn in_blocking<T, F>(work: F) -> std::result::Result<T, EndpointFailure>
where
    F: FnOnce() -> std::result::Result<T, EndpointFailure> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?
}

/// Short read: authority, the once-only guard, the rejection budget, and the
/// source dependency set the plan is validated against.
fn prepare_plan_submission(
    db_path: &Path,
    capability: &str,
) -> std::result::Result<PlanSubmissionContext, EndpointFailure> {
    let conn = quorum_core::db::open(db_path)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let context = quorum_core::capabilities::resolve_planner_context(&conn, capability)
        .map_err(|_| EndpointFailure::new("unauthorized", "run authority rejected"))?;
    if planner_submissions::accepted_response(&conn, capability)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?
        .is_some()
    {
        return Err(EndpointFailure::new(
            "already_submitted",
            "a plan was already accepted for this run",
        ));
    }
    if planner_submissions::budget_exhausted(&conn, capability)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?
    {
        return Err(EndpointFailure::new(
            "submit_budget_exhausted",
            "plan submission budget is exhausted",
        ));
    }
    let source_dependency_ids = source_dependency_ids(&conn, context.task_id)?;
    Ok(PlanSubmissionContext {
        graph_id: context.graph_id,
        source_dependency_ids,
    })
}

/// The planning source's declared dependencies, in the same form the
/// coordinator loads before its own `validate_for_source` call.
fn source_dependency_ids(
    conn: &rusqlite::Connection,
    task_id: i64,
) -> std::result::Result<Vec<i64>, EndpointFailure> {
    use rusqlite::OptionalExtension;
    let depends_on: Option<String> = conn
        .query_row(
            "SELECT depends_on FROM tasks WHERE id=?1",
            [task_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?
        .flatten();
    depends_on
        .as_deref()
        .map(serde_json::from_str::<Vec<i64>>)
        .transpose()
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))
        .map(Option::unwrap_or_default)
}

/// Full plan validation. The error string is the validator's own text so the
/// planner can correct the plan and resubmit within its turn.
async fn validate_plan_submission(
    serialized: &str,
    plan_validation: &PlanValidation,
    source_dependency_ids: &[i64],
) -> std::result::Result<(), String> {
    if serialized.len() > MAX_PLAN_SUBMISSION_BYTES {
        return Err(format!("plan exceeds {MAX_PLAN_SUBMISSION_BYTES} bytes"));
    }
    let response: super::planner::PlannerResponse =
        serde_json::from_str(serialized).map_err(|error| format!("invalid plan: {error}"))?;
    if let super::planner::PlannerResponse::Plan { tasks } = &response {
        reject_compile_atomic_signature_seam_split(tasks)?;
    }
    super::planner::validate_semantics(&response).map_err(|error| error.to_string())?;
    if let super::planner::PlannerResponse::Plan { tasks } = &response {
        super::planner::validate_for_source(
            tasks,
            source_dependency_ids,
            &plan_validation.repo_dir,
            &plan_validation.path_resolver,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Reject the mechanically visible subset of compile-closure violations at
/// the planner submission boundary. The proposal itself declares both the API
/// change and the caller path it reserves for a sibling, so no repository
/// inspection or inference is needed here. Broader compile-closure review
/// remains the Arbiter's responsibility.
fn reject_compile_atomic_signature_seam_split(
    tasks: &[super::planner::ProposedTask],
) -> std::result::Result<(), String> {
    for signature_task in tasks {
        if !changes_compile_interface(signature_task) {
            continue;
        }
        for caller_task in tasks {
            if caller_task.key == signature_task.key {
                continue;
            }
            for caller_path in caller_task.deliverables.writable_paths() {
                let named_in_deliverables = signature_task.deliverables.0.iter().any(|deliverable| {
                    matches!(deliverable,
                        quorum_core::decomposition::ChildDeliverable::Write { path }
                        | quorum_core::decomposition::ChildDeliverable::ReadOnlyReference { path }
                        if path == caller_path
                    )
                });
                let reserved_in_non_goal = signature_task
                    .non_goals
                    .iter()
                    .any(|non_goal| names_path(non_goal, caller_path));
                if named_in_deliverables || reserved_in_non_goal {
                    let caller_path = bounded_path_for_message(caller_path);
                    return Err(format!(
                        "child {} changes a compile-atomic signature seam but names caller path `{caller_path}` owned by child {}; a child changing an API must own its callers",
                        signature_task.key, caller_task.key,
                    ));
                }
            }
        }
    }
    Ok(())
}

fn changes_compile_interface(task: &super::planner::ProposedTask) -> bool {
    let body = format!(
        "{} {} {}",
        task.title, task.implementation_delta, task.observable_outcome
    )
    .to_ascii_lowercase();
    [
        "signature",
        "public api",
        "api change",
        "api changes",
        "api surface",
        "api contract",
        "public function",
        "pub fn",
        "function parameter",
        "function argument",
        "method parameter",
        "method argument",
        "return type",
        "trait shape",
        "struct shape",
        "type shape",
        "shared trait",
        "shared struct",
        "argument through",
        "parameter through",
    ]
    .iter()
    .any(|needle| body.contains(needle))
}

fn names_path(text: &str, path: &str) -> bool {
    if text.contains(path) {
        return true;
    }
    let Some(file_name) = path.rsplit('/').next() else {
        return false;
    };
    // A basename such as `recovery.rs` is an explicit reservation; generic
    // `mod.rs` is too ambiguous for this deterministic guard.
    file_name != "mod.rs" && text.contains(file_name)
}

fn bounded_path_for_message(path: &str) -> String {
    const MAX_PATH_BYTES: usize = 512;
    const ELLIPSIS: &str = "…";
    if path.len() <= MAX_PATH_BYTES {
        return path.into();
    }
    let mut end = MAX_PATH_BYTES - ELLIPSIS.len();
    while !path.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &path[..end], ELLIPSIS)
}

/// Record a validation rejection and its ledger row in one transaction. The
/// counter's own guard leaves a settled run's `rejections` alone; the call is
/// still recorded because it was still received.
fn record_plan_rejection(
    db_path: &Path,
    capability: &str,
    graph_id: i64,
    received_at: i64,
) -> std::result::Result<(), EndpointFailure> {
    let mut conn = quorum_core::db::open(db_path)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let tx = begin_immediate(&mut conn)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    planner_submissions::record_rejection(&tx, capability, graph_id)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    record_call_in_tx(
        &tx,
        capability,
        CallOutcome::Rejected(RejectionCategory::Validation),
        received_at,
    )?;
    tx.commit()
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))
}

/// Record an accepted plan. Authority is re-resolved inside the write
/// transaction because validation ran outside it and the graph may have moved
/// on; the guarded acceptance in `record_accepted` decides the outcome, and its
/// ledger row commits with it. An authority failure commits only its own row.
fn record_plan_acceptance(
    db_path: &Path,
    capability: &str,
    graph_id: i64,
    serialized: &str,
    received_at: i64,
) -> std::result::Result<SubmitOutcome, EndpointFailure> {
    let mut conn = quorum_core::db::open(db_path)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let tx = begin_immediate(&mut conn)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let authorized = quorum_core::capabilities::resolve_planner_context(&tx, capability)
        .is_ok_and(|context| context.graph_id == graph_id);
    if !authorized {
        planner_submissions::record_call(
            &tx,
            capability,
            CallOutcome::Rejected(RejectionCategory::Unauthorized),
            received_at,
        )
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
        tx.commit()
            .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
        return Err(EndpointFailure::new(
            "unauthorized",
            "run authority rejected",
        ));
    }
    let outcome = planner_submissions::record_accepted(
        &tx,
        capability,
        graph_id,
        serialized,
        quorum_core::clock::now(),
    )
    .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let call = match outcome {
        SubmitOutcome::Accepted => CallOutcome::Accepted,
        SubmitOutcome::AlreadySubmitted => {
            CallOutcome::Rejected(RejectionCategory::AlreadySubmitted)
        }
        SubmitOutcome::BudgetExhausted => CallOutcome::Rejected(RejectionCategory::BudgetExhausted),
    };
    record_call_in_tx(&tx, capability, call, received_at)?;
    tx.commit()
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    Ok(outcome)
}

/// Append the row for an outcome being recorded in `tx`. That outcome's run was
/// already resolved, so a missing `run_capabilities` row fails the transaction
/// rather than letting the outcome commit without its ledger row.
fn record_call_in_tx(
    tx: &Transaction<'_>,
    capability: &str,
    outcome: CallOutcome,
    received_at: i64,
) -> std::result::Result<(), EndpointFailure> {
    match planner_submissions::record_call(tx, capability, outcome, received_at) {
        Ok(Some(_)) => Ok(()),
        Ok(None) | Err(_) => Err(EndpointFailure::new(
            "internal",
            "request processing failed",
        )),
    }
}

fn resolve(
    tx: &rusqlite::Connection,
    capability: &str,
    role: &str,
) -> std::result::Result<LiveRunContext, EndpointFailure> {
    quorum_core::capabilities::resolve_live_run_context(tx, capability, role)
        .map_err(|_| EndpointFailure::new("unauthorized", "run authority rejected"))
}

fn resolve_any_role(
    tx: &Transaction<'_>,
    capability: &str,
) -> std::result::Result<LiveRunContext, EndpointFailure> {
    match resolve(tx, capability, "worker") {
        Ok(context) => Ok(context),
        Err(_) => resolve(tx, capability, "reviewer"),
    }
}

/// Run ids are daemon-minted, so a capability failing these rules cannot name
/// an issued run.
fn capability_has_valid_shape(capability: &str) -> bool {
    !capability.is_empty() && capability.len() <= 128 && !capability.contains('\0')
}

fn validate_text(value: Option<&str>) -> std::result::Result<(), EndpointFailure> {
    if value.is_some_and(|value| value.contains('\0')) {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "operation contains invalid text",
        ));
    }
    Ok(())
}

fn validate_progress_note(note: &str) -> std::result::Result<(), EndpointFailure> {
    if note.is_empty() || note.len() > MAX_REQUEST_BYTES || note.contains('\0') {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "invalid progress note",
        ));
    }
    Ok(())
}

fn validate_owner_alert(body: &str) -> std::result::Result<(), EndpointFailure> {
    if body.is_empty() || body.len() > MAX_OWNER_ALERT_BYTES || body.contains('\0') {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "invalid owner alert",
        ));
    }
    Ok(())
}

/// The message TTL `quorum post` uses when `--ttl` is omitted: the operator
/// config's `message_ttl_secs`, or its default when no config file exists.
fn configured_message_ttl(operator_config: &Path) -> std::result::Result<i64, EndpointFailure> {
    crate::config::load(operator_config)
        .map(|config| config.message_ttl_secs)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))
}

fn validate_submission(
    capability: &str,
    context: &LiveRunContext,
    verdict: Option<&str>,
    feedback: Option<&str>,
    feedback_json: Option<&str>,
    blocking: Option<u32>,
) -> std::result::Result<Option<String>, EndpointFailure> {
    if context.role == "worker" {
        if verdict.is_some() || feedback.is_some() || feedback_json.is_some() || blocking.is_some()
        {
            return Err(EndpointFailure::new(
                "invalid_operation",
                "worker submission has reviewer fields",
            ));
        }
        return Ok(None);
    }

    if !matches!(verdict, Some("approved" | "changes" | "graph-blocker")) {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "invalid reviewer verdict",
        ));
    }
    if verdict == Some("graph-blocker") {
        if feedback.is_some() || blocking.is_some() {
            return Err(EndpointFailure::new(
                "invalid_operation",
                "invalid graph-blocker fields",
            ));
        }
        let raw = feedback_json.ok_or_else(|| {
            EndpointFailure::new("invalid_operation", "graph-blocker feedback is required")
        })?;
        let graph_feedback = crate::graph_blocker::parse_feedback(raw).map_err(|_| {
            EndpointFailure::new("invalid_operation", "invalid graph-blocker feedback")
        })?;
        if graph_feedback.affected_task != context.task_id {
            return Err(EndpointFailure::new(
                "invalid_operation",
                "graph-blocker task does not match this run",
            ));
        }
        return crate::graph_blocker::encode(capability.to_string(), graph_feedback)
            .map(Some)
            .map_err(|_| {
                EndpointFailure::new("invalid_operation", "invalid graph-blocker feedback")
            });
    }
    if feedback_json.is_some() {
        return Err(EndpointFailure::new(
            "invalid_operation",
            "feedback_json requires graph-blocker verdict",
        ));
    }
    crate::verdict::validate(verdict, blocking, feedback, true)
        .map_err(|_| EndpointFailure::new("invalid_operation", "invalid reviewer submission"))?;
    Ok(crate::verdict::attestation_payload(blocking))
}

#[allow(clippy::too_many_arguments)]
fn insert_mailbox(
    tx: &Transaction<'_>,
    context: &LiveRunContext,
    kind: &str,
    pr: Option<i64>,
    verdict: Option<&str>,
    feedback: Option<&str>,
    note: Option<&str>,
    payload: Option<&str>,
) -> std::result::Result<i64, EndpointFailure> {
    tx.query_row(
        "INSERT INTO mailbox
         (agent,kind,task_id,pr,verdict,feedback,note,to_agent,payload,created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,NULL,?8,?9)
         RETURNING id",
        params![
            context.agent,
            kind,
            context.task_id,
            pr,
            verdict,
            feedback,
            note,
            payload,
            quorum_core::clock::now(),
        ],
        |row| row.get(0),
    )
    .map_err(|_| EndpointFailure::new("internal", "request processing failed"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::mpsc;

    /// A frozen planning graph whose live planner run holds `planner-cap`,
    /// plus a repo root that exists on disk so writable-path resolution can
    /// canonicalize it. The source task declares dependency 7 so the plan may
    /// legitimately carry a `source:7` prerequisite.
    struct PlannerFixture {
        _dir: tempfile::TempDir,
        db_path: PathBuf,
        plan_validation: PlanValidation,
        /// Inside the fixture's own directory and absent unless a test
        /// writes it, so no test reads the developer's real config.
        operator_config: PathBuf,
        evidence: Arc<FakeEvidence>,
    }

    /// Scripted launch-SHA diff and PR head. `None` fails the call.
    #[derive(Default)]
    struct FakeEvidence {
        diff: std::sync::Mutex<Option<(String, Vec<u8>)>>,
        head: std::sync::Mutex<Option<String>>,
        diff_calls: std::sync::atomic::AtomicUsize,
        head_calls: std::sync::atomic::AtomicUsize,
    }

    impl ReviewEvidence for FakeEvidence {
        fn launch_diff<'a>(
            &'a self,
            base_branch: &'a str,
            _launch_sha: &'a str,
        ) -> ReviewEvidenceFuture<'a, (String, Vec<u8>)> {
            self.diff_calls.fetch_add(1, Ordering::SeqCst);
            let diff = self.diff.lock().unwrap().clone();
            assert_eq!(base_branch, "main");
            Box::pin(async move { diff.ok_or_else(|| "no diff".to_string()) })
        }

        fn pull_request_head(&self, pr: i64) -> ReviewEvidenceFuture<'_, String> {
            self.head_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(pr, 77);
            let head = self.head.lock().unwrap().clone();
            Box::pin(async move { head.ok_or_else(|| "github down".to_string()) })
        }
    }

    impl PlannerFixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join("quorum.db");
            let repo_dir = dir.path().join("repo");
            std::fs::create_dir(&repo_dir).unwrap();
            let conn = quorum_core::db::open(&db_path).unwrap();
            conn.execute(
                "INSERT INTO tasks
                 (id,title,status,assignee,created_by,created_at,updated_at,depends_on)
                 VALUES (1,'planner source','open','Planner','test',10,10,'[7]')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
                 VALUES ('planner-cap',1,'Planner','planner',10)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
                 VALUES ('worker-cap',1,'Worker','worker',10)",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO task_decompositions
                 (id,source_task_id,state,active,freeze_active,planner_session_id,
                  planned_source_revision,created_at,updated_at)
                 VALUES (5,1,'planning',0,1,'planner-cap',3,10,10)",
                [],
            )
            .unwrap();
            drop(conn);
            Self {
                operator_config: dir.path().join("config.toml"),
                evidence: Arc::new(FakeEvidence::default()),
                _dir: dir,
                db_path,
                plan_validation: PlanValidation {
                    repo_dir,
                    path_resolver: super::super::planner::WritablePathResolver::default(),
                },
            }
        }

        async fn call(
            &self,
            capability: &str,
            operation: Operation,
        ) -> std::result::Result<Response, EndpointFailure> {
            self.request(PROTOCOL_VERSION, capability, operation, PROCESS_TIMEOUT)
                .await
        }

        async fn request(
            &self,
            version: u8,
            capability: &str,
            operation: Operation,
            process_timeout: Duration,
        ) -> std::result::Result<Response, EndpointFailure> {
            process_request_with_timeout(
                self.db_path.clone(),
                "test/repo".into(),
                self.plan_validation.clone(),
                self.operator_config.clone(),
                self.evidence.clone(),
                Request {
                    version,
                    capability: capability.into(),
                    operation,
                },
                process_timeout,
            )
            .await
        }

        async fn submit_plan(
            &self,
            capability: &str,
            response: serde_json::Value,
        ) -> std::result::Result<Response, EndpointFailure> {
            self.call(capability, Operation::SubmitPlan { response })
                .await
        }

        fn submission_row(&self) -> (Option<String>, i64) {
            quorum_core::db::open(&self.db_path)
                .unwrap()
                .query_row(
                    "SELECT response_json,rejections FROM planner_submissions WHERE run_id=?1",
                    ["planner-cap"],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap()
        }

        fn submission_count(&self) -> i64 {
            quorum_core::db::open(&self.db_path)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM planner_submissions", [], |row| {
                    row.get(0)
                })
                .unwrap()
        }

        /// `(seq, accepted, rejection_category)` for `run_id`, in `seq` order.
        fn ledger(&self, run_id: &str) -> Vec<(i64, i64, Option<String>)> {
            quorum_core::db::open(&self.db_path)
                .unwrap()
                .prepare(
                    "SELECT seq,accepted,rejection_category FROM planner_submission_calls
                     WHERE run_id=?1 ORDER BY seq",
                )
                .unwrap()
                .query_map([run_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        }

        fn ledger_total(&self) -> i64 {
            quorum_core::db::open(&self.db_path)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM planner_submission_calls", [], |row| {
                    row.get(0)
                })
                .unwrap()
        }

        fn execute(&self, sql: &str) {
            quorum_core::db::open(&self.db_path)
                .unwrap()
                .execute_batch(sql)
                .unwrap();
        }

        fn revoke(&self, run_id: &str) {
            let mut conn = quorum_core::db::open(&self.db_path).unwrap();
            quorum_core::capabilities::revoke(&mut conn, run_id, 20).unwrap();
        }
    }

    /// Fails every ledger insert, so no call can land a row.
    const LEDGER_DOWN: &str = "CREATE TRIGGER ledger_down BEFORE INSERT ON planner_submission_calls
         BEGIN SELECT RAISE(ABORT, 'ledger down'); END";

    fn accepted_row(seq: i64) -> (i64, i64, Option<String>) {
        (seq, 1, None)
    }

    fn rejected_row(seq: i64, category: &str) -> (i64, i64, Option<String>) {
        (seq, 0, Some(category.to_string()))
    }

    fn plan_task(key: &str, path: &str, prerequisites: Vec<&str>) -> serde_json::Value {
        json!({
            "key": key,
            "title": format!("implement {key}"),
            "implementation_delta": format!("edit {path}"),
            "affected_paths": [path],
            "observable_outcome": format!("{path} exposes the new behavior"),
            "deliverables": [{"kind": "write", "path": path}],
            "acceptance_criteria": ["the new behavior is covered by a test"],
            "source_constraints": ["do not change unrelated modules"],
            "verification_expectations": ["cargo test passes"],
            "non_goals": ["no unrelated refactors"],
            "prerequisites": prerequisites,
        })
    }

    fn valid_plan() -> serde_json::Value {
        json!({
            "outcome": "plan",
            "tasks": [
                plan_task("first", "src/first.rs", vec!["source:7"]),
                plan_task("second", "src/second.rs", vec!["first"]),
            ],
        })
    }

    /// One task is below `validate_plan_tasks`' two-task minimum.
    fn undersized_plan() -> serde_json::Value {
        json!({"outcome": "plan", "tasks": [plan_task("only", "src/only.rs", vec![])]})
    }

    /// Mirrors the rejected #179/#180 shape: one child changes a public core
    /// signature but explicitly leaves its callers to a dependent sibling.
    fn compile_atomic_signature_seam_plan() -> serde_json::Value {
        let mut core = plan_task("lock-api", "quorum-core/src/daemon_lock.rs", vec![]);
        core["implementation_delta"] = json!(
            "change public function signatures and thread instance_id through acquire, refresh, and release"
        );
        core["non_goals"] = json!([
            "Do not change quorum/src/serve/mod.rs callers; that wiring belongs to serve-wiring"
        ]);

        json!({
            "outcome": "plan",
            "tasks": [
                core,
                plan_task("serve-wiring", "quorum/src/serve/mod.rs", vec!["lock-api"]),
            ],
        })
    }

    fn no_safe_split_blocker() -> serde_json::Value {
        json!({
            "outcome": "blocker",
            "category": "no_safe_split",
            "evidence": [
                "changing daemon_lock public signatures requires the serve callers to change together"
            ],
            "required_decision": "allow one compile-atomic child",
            "why_no_safe_split": "the signature and caller wiring cannot pass preflight independently"
        })
    }

    fn result_value(response: &Response) -> serde_json::Value {
        serde_json::to_value(response).unwrap()
    }

    #[test]
    fn compile_atomic_signature_seam_guard_rejects_reserved_callers() {
        let response: super::super::planner::PlannerResponse =
            serde_json::from_value(compile_atomic_signature_seam_plan()).unwrap();
        let super::super::planner::PlannerResponse::Plan { tasks } = response else {
            panic!("fixture must be a plan");
        };

        let error = reject_compile_atomic_signature_seam_split(&tasks).unwrap_err();
        assert!(error.contains("compile-atomic signature seam"));
        assert!(error.contains("serve-wiring"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_plan_accepts_valid_plan_once() {
        let fixture = PlannerFixture::new();
        let plan = valid_plan();

        let accepted = fixture
            .submit_plan("planner-cap", plan.clone())
            .await
            .unwrap();
        let value = result_value(&accepted);
        assert_eq!(value["ok"], true);
        assert_eq!(value["result"]["type"], "plan_accepted");
        assert_eq!(value["result"]["graph_id"], 5);

        let (stored, rejections) = fixture.submission_row();
        assert_eq!(rejections, 0);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored.unwrap()).unwrap(),
            plan
        );

        // A second valid submission never overwrites the accepted plan.
        let error = fixture.submit_plan("planner-cap", plan).await.unwrap_err();
        assert_eq!(error.code, "already_submitted");
        assert_eq!(fixture.submission_row().1, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_plan_returns_validator_message_and_counts_rejection() {
        let fixture = PlannerFixture::new();
        let error = fixture
            .submit_plan("planner-cap", undersized_plan())
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_plan");
        assert!(
            error
                .message
                .contains("plan must contain between 2 and 8 tasks"),
            "unexpected message: {}",
            error.message
        );
        let (stored, rejections) = fixture.submission_row();
        assert_eq!(stored, None);
        assert_eq!(rejections, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_plan_rejects_compile_atomic_signature_split_and_accepts_no_safe_split() {
        let fixture = PlannerFixture::new();
        let error = fixture
            .submit_plan("planner-cap", compile_atomic_signature_seam_plan())
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_plan");
        assert!(
            error.message.contains("compile-atomic signature seam"),
            "unexpected message: {}",
            error.message
        );
        assert!(error.message.contains("serve-wiring"));
        assert_eq!(fixture.submission_row(), (None, 1));

        let blocker = no_safe_split_blocker();
        let accepted = fixture
            .submit_plan("planner-cap", blocker.clone())
            .await
            .unwrap();
        assert_eq!(result_value(&accepted)["result"]["type"], "plan_accepted");
        let (stored, rejections) = fixture.submission_row();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored.unwrap()).unwrap(),
            blocker
        );
        assert_eq!(rejections, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_plan_exhausts_rejection_budget() {
        let fixture = PlannerFixture::new();
        for expected in 1..=quorum_core::planner_submissions::MAX_PLAN_SUBMIT_REJECTIONS {
            let error = fixture
                .submit_plan("planner-cap", undersized_plan())
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid_plan", "rejection {expected}");
            assert_eq!(fixture.submission_row().1, expected);
        }

        // The budget refuses even a plan that would otherwise be accepted.
        let error = fixture
            .submit_plan("planner-cap", valid_plan())
            .await
            .unwrap_err();
        assert_eq!(error.code, "submit_budget_exhausted");
        let (stored, rejections) = fixture.submission_row();
        assert_eq!(stored, None);
        assert_eq!(
            rejections,
            quorum_core::planner_submissions::MAX_PLAN_SUBMIT_REJECTIONS
        );
    }

    /// The plan byte bound must be the operative one, and it must be the
    /// reason this payload is refused.
    ///
    /// Every individually length-checked field stays well inside its own
    /// limit: the padding is 8 extra `affected_paths` entries of 7,600 bytes
    /// each — under `validate_text`'s 8 KiB per-item bound and under
    /// `validate_list`'s 32-item bound — so only the size of the whole
    /// document is out of range. Delete the byte bound and this plan validates
    /// and is accepted, which is what makes the test fail when the behavior
    /// breaks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_plan_rejects_oversized_payload() {
        let fixture = PlannerFixture::new();
        let mut plan = valid_plan();
        let padding = "p".repeat(7_600);
        let paths = plan["tasks"][0]["affected_paths"].as_array_mut().unwrap();
        for index in 0..8 {
            paths.push(json!(format!("src/pad-{index}-{padding}.rs")));
        }

        let serialized = serde_json::to_string(&plan).unwrap();
        assert!(
            serialized.len() > MAX_PLAN_SUBMISSION_BYTES,
            "payload must exceed the plan bound: {}",
            serialized.len()
        );
        // The bound is reachable over the wire: a plan just past it still fits
        // inside a request frame, so the endpoint answers `invalid_plan` and
        // counts a rejection instead of the frame reader answering
        // `request_too_large` with nothing counted.
        let framed = serde_json::to_vec(&serde_json::json!({
            "version": PROTOCOL_VERSION,
            "capability": "planner-cap",
            "operation": {"type": "submit_plan", "response": plan},
        }))
        .unwrap();
        assert!(
            framed.len() <= MAX_REQUEST_BYTES,
            "oversized plan must still fit its request frame: {}",
            framed.len()
        );

        let error = fixture.submit_plan("planner-cap", plan).await.unwrap_err();
        assert_eq!(error.code, "invalid_plan");
        assert_eq!(
            error.message,
            format!("plan exceeds {MAX_PLAN_SUBMISSION_BYTES} bytes"),
            "the byte bound, not a per-field limit, must reject this plan"
        );
        let (stored, rejections) = fixture.submission_row();
        assert_eq!(stored, None);
        assert_eq!(rejections, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_plan_rejects_worker_capability() {
        let fixture = PlannerFixture::new();
        let error = fixture
            .submit_plan("worker-cap", valid_plan())
            .await
            .unwrap_err();
        assert_eq!(error.code, "unauthorized");
        let recorded: i64 = quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM planner_submissions", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(recorded, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn planner_capability_rejected_on_other_operations() {
        let fixture = PlannerFixture::new();
        for operation in [
            Operation::Submit {
                summary: Some("planner should not submit".into()),
                verdict: None,
                feedback: None,
                feedback_json: None,
                blocking: None,
                publication_failure: None,
            },
            Operation::React {
                state: "blocked".into(),
                reason: Some("planner should not react".into()),
            },
            Operation::AppendNote {
                task_id: Some(1),
                agent: Some("Planner".into()),
                note: "planner should not append".into(),
            },
            Operation::AppendNote {
                task_id: None,
                agent: None,
                note: "planner should not append".into(),
            },
            Operation::AlertOwner {
                body: "planner should not alert".into(),
            },
            Operation::Protocol {
                operation: ProtocolOperation::PullRequestRead,
                attempt_id: None,
                operation_id: None,
                arguments: None,
            },
        ] {
            let error = fixture.call("planner-cap", operation).await.unwrap_err();
            assert_eq!(error.code, "unauthorized");
        }
        let mailbox_count: i64 = quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM mailbox", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mailbox_count, 0);
    }

    /// A live worker (task 100) and a live reviewer (task 101, PR 77), plus a
    /// revoked reviewer capability, with no planner involved.
    fn live_run_fixture() -> PlannerFixture {
        let fixture = PlannerFixture::new();
        fixture.execute(
            "INSERT INTO tasks
               (id,title,status,assignee,reviewer,created_by,created_at,updated_at,refs)
             VALUES (100,'worker fixture','working','Worker',NULL,'test',1000,1000,NULL),
                    (101,'review fixture','in-review',NULL,'Reviewer','test',1000,1000,'{\"pr\":77}'),
                    (102,'revoked fixture','in-review',NULL,'Gone','test',1000,1000,'{\"pr\":78}');
             INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at,revoked_at)
             VALUES ('live-worker',100,'Worker','worker',1001,NULL),
                    ('live-reviewer',101,'Reviewer','reviewer',1001,NULL),
                    ('revoked-reviewer',102,'Gone','reviewer',1001,1002);
             INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
             VALUES (100,'Worker','worker','test','high','test',1001);
             INSERT INTO agent_runs
               (task_id,agent_name,role,model,effort,provider,spawned_at,
                review_cap_run_id,review_pr,review_head_sha)
             VALUES (101,'Reviewer','reviewer','test','high','test',1001,'live-reviewer',77,
                     'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
                    (102,'Gone','reviewer','test','high','test',1001,'revoked-reviewer',78,
                     'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');",
        );
        fixture
    }

    fn count(fixture: &PlannerFixture, table: &str) -> i64 {
        quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn note_row(fixture: &PlannerFixture, note_id: i64) -> (i64, String, String) {
        quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .query_row(
                "SELECT task_id,agent,body FROM task_notes WHERE id=?1",
                [note_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    /// Without identity fields the note lands on the capability's own task as
    /// its own agent, for a worker and for a reviewer alike.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_note_without_identity_uses_the_capability() {
        let fixture = live_run_fixture();
        for (capability, task_id, agent) in [
            ("live-worker", 100, "Worker"),
            ("live-reviewer", 101, "Reviewer"),
        ] {
            let response = fixture
                .call(
                    capability,
                    Operation::AppendNote {
                        task_id: None,
                        agent: None,
                        note: format!("progress from {agent}"),
                    },
                )
                .await
                .unwrap();
            let value = result_value(&response)["result"].clone();
            assert_eq!(value["type"], "task_note");
            assert_eq!(
                note_row(&fixture, value["note_id"].as_i64().unwrap()),
                (task_id, agent.to_string(), format!("progress from {agent}"))
            );
        }
        assert_eq!(count(&fixture, "task_notes"), 2);
    }

    /// Identity fields stay constraints: a full or partial match is accepted,
    /// and either field disagreeing with the capability writes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_note_identity_fields_must_match_when_present() {
        let fixture = live_run_fixture();
        for (task_id, agent) in [
            (Some(100), Some("Worker")),
            (Some(100), None),
            (None, Some("Worker")),
        ] {
            let response = fixture
                .call(
                    "live-worker",
                    Operation::AppendNote {
                        task_id,
                        agent: agent.map(str::to_string),
                        note: "matching identity".into(),
                    },
                )
                .await
                .unwrap();
            let note_id = result_value(&response)["result"]["note_id"]
                .as_i64()
                .unwrap();
            assert_eq!(
                note_row(&fixture, note_id),
                (100, "Worker".to_string(), "matching identity".to_string())
            );
        }
        assert_eq!(count(&fixture, "task_notes"), 3);

        for (task_id, agent) in [
            (Some(101), Some("Worker")),
            (Some(100), Some("Reviewer")),
            (Some(101), None),
            (None, Some("Reviewer")),
        ] {
            let error = fixture
                .call(
                    "live-worker",
                    Operation::AppendNote {
                        task_id,
                        agent: agent.map(str::to_string),
                        note: "mismatched identity".into(),
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid_operation");
            assert_eq!(error.message, "task identity does not match run authority");
        }
        assert_eq!(count(&fixture, "task_notes"), 3);
    }

    /// Older clients always send both fields, and newer ones may omit them;
    /// both wire shapes must decode, and unknown fields stay rejected.
    #[test]
    fn append_note_wire_shape_accepts_optional_identity() {
        let with: Request = serde_json::from_value(json!({
            "version": 1, "capability": "c",
            "operation": {"type":"append_note","task_id":7,"agent":"A","note":"n"}
        }))
        .unwrap();
        assert!(matches!(
            with.operation,
            Operation::AppendNote { task_id: Some(7), agent: Some(ref agent), .. } if agent == "A"
        ));
        let without: Request = serde_json::from_value(json!({
            "version": 1, "capability": "c",
            "operation": {"type":"append_note","note":"n"}
        }))
        .unwrap();
        assert!(matches!(
            without.operation,
            Operation::AppendNote {
                task_id: None,
                agent: None,
                ..
            }
        ));
        assert!(serde_json::from_value::<Request>(json!({
            "version": 1, "capability": "c",
            "operation": {"type":"append_note","note":"n","task":7}
        }))
        .is_err());
        assert!(serde_json::from_value::<Request>(json!({
            "version": 1, "capability": "c",
            "operation": {"type":"alert_owner","body":"b","to":"someone"}
        }))
        .is_err());
    }

    /// A live reviewer's alert is one critical feed message to the owner,
    /// authored by the capability's agent, with a live expiry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alert_owner_posts_one_critical_message_as_the_reviewer() {
        let fixture = live_run_fixture();
        let response = fixture
            .call(
                "live-reviewer",
                Operation::AlertOwner {
                    body: "merge is unsafe".into(),
                },
            )
            .await
            .unwrap();
        let value = result_value(&response)["result"].clone();
        assert_eq!(value["type"], "message");
        let seq = value["seq"].as_i64().unwrap();
        assert!(seq > 0);

        type MessageShape = (
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            i64,
            i64,
        );
        let row: MessageShape = quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .query_row(
                "SELECT author,kind,topic,body,recipient,refs,ts,expires_at
                 FROM messages WHERE seq=?1",
                [seq],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            (&row.0[..], &row.1[..], &row.2[..], &row.3[..]),
            (
                "Reviewer",
                "critical",
                quorum_core::feed::DEFAULT_TOPIC,
                "merge is unsafe"
            )
        );
        assert_eq!(row.4.as_deref(), Some("owner"));
        assert_eq!(row.5, None);
        assert_eq!(
            row.7 - row.6,
            quorum_core::feed::DEFAULT_MESSAGE_TTL_SECS,
            "with no operator config the alert uses the default message TTL"
        );
        assert_eq!(count(&fixture, "messages"), 1);
        assert_eq!(count(&fixture, "mailbox"), 0);
    }

    /// The operator config supplies the alert TTL, and it is consulted only
    /// after authority and the body are settled: with a malformed config an
    /// unauthorized or invalid call still gets its own refusal, and only a
    /// call that would otherwise post fails as `internal`. Nothing is written.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alert_owner_reads_the_operator_config_only_after_authorization() {
        let fixture = live_run_fixture();
        std::fs::write(&fixture.operator_config, "message_ttl_secs = 777\n").unwrap();
        let response = fixture
            .call(
                "live-reviewer",
                Operation::AlertOwner {
                    body: "configured ttl".into(),
                },
            )
            .await
            .unwrap();
        let seq = result_value(&response)["result"]["seq"].as_i64().unwrap();
        let ttl: i64 = quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .query_row(
                "SELECT expires_at - ts FROM messages WHERE seq=?1",
                [seq],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ttl, 777);

        std::fs::write(&fixture.operator_config, "message_ttl_secs = [not toml").unwrap();
        for capability in ["live-worker", "revoked-reviewer", "planner-cap", "unknown"] {
            let error = fixture
                .call(
                    capability,
                    Operation::AlertOwner {
                        body: "not allowed".into(),
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "unauthorized", "{capability}");
        }
        let invalid = fixture
            .call(
                "live-reviewer",
                Operation::AlertOwner {
                    body: String::new(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(invalid.code, "invalid_operation");
        let unreadable = fixture
            .call(
                "live-reviewer",
                Operation::AlertOwner {
                    body: "would post".into(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(unreadable.code, "internal");
        assert_eq!(count(&fixture, "messages"), 1);
    }

    /// Only a live reviewer may alert the owner, and only with a bounded,
    /// NUL-free, non-empty body. Every refusal leaves the feed untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alert_owner_rejections_write_nothing() {
        let fixture = live_run_fixture();
        for capability in ["live-worker", "revoked-reviewer", "planner-cap", "unknown"] {
            let error = fixture
                .call(
                    capability,
                    Operation::AlertOwner {
                        body: "not allowed".into(),
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "unauthorized", "{capability}");
            assert_eq!(error.message, "run authority rejected");
        }
        for body in [
            String::new(),
            "nul\0inside".to_string(),
            "x".repeat(MAX_OWNER_ALERT_BYTES + 1),
        ] {
            let error = fixture
                .call("live-reviewer", Operation::AlertOwner { body })
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid_operation");
            assert_eq!(error.message, "invalid owner alert");
        }
        assert_eq!(count(&fixture, "messages"), 0);

        fixture
            .call(
                "live-reviewer",
                Operation::AlertOwner {
                    body: "x".repeat(MAX_OWNER_ALERT_BYTES),
                },
            )
            .await
            .unwrap();
        assert_eq!(count(&fixture, "messages"), 1);
    }

    /// The planner's inventory is what lets its MCP shell advertise
    /// `submit_plan`. It must carry no protocol operation and no PR target: the
    /// planner's authority stops at the one plan submission.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn planner_inventory_advertises_no_protocol_operations() {
        let fixture = PlannerFixture::new();
        let value = result_value(
            &fixture
                .call("planner-cap", Operation::Inventory)
                .await
                .unwrap(),
        );
        assert_eq!(value["ok"], true);
        assert_eq!(value["result"]["type"], "inventory");
        assert_eq!(value["result"]["role"], "planner");
        assert_eq!(value["result"]["phase"], "planner");
        assert_eq!(value["result"]["task_id"], 1);
        assert_eq!(value["result"]["pr"], serde_json::Value::Null);
        assert_eq!(value["result"]["review_revision"], serde_json::Value::Null);
        assert_eq!(value["result"]["operations"], json!([]));
    }

    /// A revoked planner is not the live planner, so its inventory falls
    /// through to the live-run resolver rather than advertising a tool.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revoked_planner_inventory_is_unauthorized() {
        let fixture = PlannerFixture::new();
        let mut conn = quorum_core::db::open(&fixture.db_path).unwrap();
        quorum_core::capabilities::revoke(&mut conn, "planner-cap", 20).unwrap();
        drop(conn);

        let error = fixture
            .call("planner-cap", Operation::Inventory)
            .await
            .unwrap_err();
        assert_eq!(error.code, "unauthorized");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ledger_records_rejection_acceptance_and_duplicate() {
        let fixture = PlannerFixture::new();
        let invalid = fixture
            .submit_plan("planner-cap", undersized_plan())
            .await
            .unwrap_err();
        assert_eq!(invalid.code, "invalid_plan");
        fixture
            .submit_plan("planner-cap", valid_plan())
            .await
            .unwrap();
        let duplicate = fixture
            .submit_plan("planner-cap", valid_plan())
            .await
            .unwrap_err();
        assert_eq!(duplicate.code, "already_submitted");
        assert_eq!(
            duplicate.message,
            "a plan was already accepted for this run"
        );

        assert_eq!(
            fixture.ledger("planner-cap"),
            vec![
                rejected_row(1, "validation"),
                accepted_row(2),
                rejected_row(3, "already_submitted"),
            ]
        );
        let conn = quorum_core::db::open(&fixture.db_path).unwrap();
        let summary = planner_submissions::call_summary(&conn, "planner-cap").unwrap();
        assert_eq!((summary.total, summary.accepted), (3, 1));
        assert_eq!(fixture.submission_row().1, 1, "only validation counts");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ledger_records_budget_exhaustion_without_spending_budget() {
        let fixture = PlannerFixture::new();
        let budget = planner_submissions::MAX_PLAN_SUBMIT_REJECTIONS;
        for _ in 0..budget {
            fixture
                .submit_plan("planner-cap", undersized_plan())
                .await
                .unwrap_err();
        }
        let error = fixture
            .submit_plan("planner-cap", valid_plan())
            .await
            .unwrap_err();
        assert_eq!(error.code, "submit_budget_exhausted");

        let ledger = fixture.ledger("planner-cap");
        assert_eq!(ledger.len() as i64, budget + 1);
        assert_eq!(
            ledger[ledger.len() - 1],
            rejected_row(budget + 1, "budget_exhausted")
        );
        assert!(ledger[..ledger.len() - 1]
            .iter()
            .all(|row| row.2.as_deref() == Some("validation")));
        assert_eq!(fixture.submission_row().1, budget);
    }

    /// The write phase reaches these outcomes only after losing a race with
    /// another call, so drive it directly past the prepare-phase refusals.
    #[test]
    fn ledger_row_shares_the_write_phase_outcome() {
        let fixture = PlannerFixture::new();
        let accept = |plan: &str| {
            record_plan_acceptance(&fixture.db_path, "planner-cap", 5, plan, 40).unwrap()
        };
        assert_eq!(accept("{\"first\":1}"), SubmitOutcome::Accepted);
        assert_eq!(accept("{\"second\":2}"), SubmitOutcome::AlreadySubmitted);
        assert_eq!(
            fixture.ledger("planner-cap"),
            vec![accepted_row(1), rejected_row(2, "already_submitted")]
        );
        assert_eq!(fixture.submission_row().0.as_deref(), Some("{\"first\":1}"));

        let fixture = PlannerFixture::new();
        let budget = planner_submissions::MAX_PLAN_SUBMIT_REJECTIONS;
        for _ in 0..budget {
            record_plan_rejection(&fixture.db_path, "planner-cap", 5, 40).unwrap();
        }
        assert_eq!(
            record_plan_acceptance(&fixture.db_path, "planner-cap", 5, "{}", 41).unwrap(),
            SubmitOutcome::BudgetExhausted
        );
        assert_eq!(
            fixture.ledger("planner-cap").last(),
            Some(&rejected_row(budget + 1, "budget_exhausted"))
        );
        let conn = quorum_core::db::open(&fixture.db_path).unwrap();
        let received_at: i64 = conn
            .query_row(
                "SELECT MAX(received_at) FROM planner_submission_calls",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(received_at, 41, "the row keeps the call's receipt time");
    }

    /// Validation runs outside the write transaction, so a rejection can commit
    /// behind another call's acceptance.
    #[test]
    fn late_rejection_is_recorded_without_touching_the_accepted_run() {
        let fixture = PlannerFixture::new();
        record_plan_acceptance(&fixture.db_path, "planner-cap", 5, "{\"plan\":1}", 40).unwrap();
        record_plan_rejection(&fixture.db_path, "planner-cap", 5, 41).unwrap();

        assert_eq!(
            fixture.ledger("planner-cap"),
            vec![accepted_row(1), rejected_row(2, "validation")]
        );
        assert_eq!(
            fixture.submission_row(),
            (Some("{\"plan\":1}".to_string()), 0)
        );
    }

    /// An outcome may not commit without its row, even if the capability that
    /// passed prepare is gone by the time the rejection records.
    #[test]
    fn outcome_without_a_ledger_row_rolls_back() {
        let fixture = PlannerFixture::new();
        let error = record_plan_rejection(&fixture.db_path, "never-issued", 5, 40).unwrap_err();
        assert_eq!(error.code, "internal");
        assert_eq!(fixture.submission_count(), 0);
        assert_eq!(fixture.ledger_total(), 0);
    }

    /// Authority can change between prepare and record, including the graph
    /// moving on while the capability stays live.
    #[test]
    fn write_phase_authority_failure_commits_only_its_ledger_row() {
        let wrong_graph = PlannerFixture::new();
        let error =
            record_plan_acceptance(&wrong_graph.db_path, "planner-cap", 6, "{}", 40).unwrap_err();
        assert_eq!(error.code, "unauthorized");
        assert_eq!(
            wrong_graph.ledger("planner-cap"),
            vec![rejected_row(1, "unauthorized")]
        );
        assert_eq!(wrong_graph.submission_count(), 0);

        let revoked = PlannerFixture::new();
        revoked.revoke("planner-cap");
        let error =
            record_plan_acceptance(&revoked.db_path, "planner-cap", 5, "{}", 40).unwrap_err();
        assert_eq!(error.code, "unauthorized");
        assert_eq!(
            revoked.ledger("planner-cap"),
            vec![rejected_row(1, "unauthorized")]
        );
        assert_eq!(revoked.submission_count(), 0);

        let unknown = PlannerFixture::new();
        let error =
            record_plan_acceptance(&unknown.db_path, "never-issued", 5, "{}", 40).unwrap_err();
        assert_eq!(error.code, "unauthorized");
        assert_eq!(unknown.ledger_total(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wrong_version_records_only_an_issued_submit_plan_capability() {
        let fixture = PlannerFixture::new();
        let wrong_version = PROTOCOL_VERSION + 1;
        let submit = || Operation::SubmitPlan {
            response: valid_plan(),
        };
        let refuse = |capability: String, operation: Operation| {
            let fixture = &fixture;
            async move {
                let error = fixture
                    .request(wrong_version, &capability, operation, PROCESS_TIMEOUT)
                    .await
                    .unwrap_err();
                assert_eq!(error.code, "unsupported_version");
                assert_eq!(error.message, "unsupported protocol version");
                assert!(!error.is_abnormal());
            }
        };

        refuse("never-issued".into(), submit()).await;
        refuse(String::new(), submit()).await;
        refuse("x".repeat(129), submit()).await;
        refuse("planner\0cap".into(), submit()).await;
        refuse("planner-cap".into(), Operation::Inventory).await;
        assert_eq!(fixture.ledger_total(), 0);

        refuse("planner-cap".into(), submit()).await;
        refuse("planner-cap".into(), submit()).await;
        assert_eq!(
            fixture.ledger("planner-cap"),
            vec![
                rejected_row(1, "unsupported_version"),
                rejected_row(2, "unsupported_version"),
            ]
        );
        assert_eq!(fixture.submission_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prepare_deadline_records_exactly_one_timeout_row() {
        let fixture = PlannerFixture::new();
        for capability in ["planner-cap", "never-issued"] {
            let error = fixture
                .request(
                    PROTOCOL_VERSION,
                    capability,
                    Operation::SubmitPlan {
                        response: valid_plan(),
                    },
                    Duration::ZERO,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "timeout");
        }
        // The abandoned prepare is still finishing; it must add nothing.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            fixture.ledger("planner-cap"),
            vec![rejected_row(1, "timeout")]
        );
        assert_eq!(fixture.ledger_total(), 1);
        assert_eq!(fixture.submission_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_authority_records_an_unauthorized_row_for_issued_runs() {
        let fixture = PlannerFixture::new();
        fixture.revoke("planner-cap");
        fixture.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('moved-on-cap',1,'Planner','planner',10)",
        );
        for capability in ["worker-cap", "planner-cap", "moved-on-cap"] {
            let error = fixture
                .submit_plan(capability, valid_plan())
                .await
                .unwrap_err();
            assert_eq!(error.code, "unauthorized", "{capability}");
            assert_eq!(error.message, "run authority rejected");
            assert_eq!(
                fixture.ledger(capability),
                vec![rejected_row(1, "unauthorized")],
                "{capability}"
            );
        }
        assert_eq!(fixture.submission_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capabilities_naming_no_issued_run_write_no_row() {
        let fixture = PlannerFixture::new();
        for capability in [
            "never-issued".to_string(),
            String::new(),
            "x".repeat(129),
            "planner\0cap".to_string(),
        ] {
            let error = fixture
                .submit_plan(&capability, valid_plan())
                .await
                .unwrap_err();
            assert_eq!(error.code, "unauthorized");
        }
        assert_eq!(fixture.ledger_total(), 0);
        assert_eq!(fixture.submission_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn internal_prepare_failure_records_an_internal_row() {
        let fixture = PlannerFixture::new();
        fixture.execute("UPDATE tasks SET depends_on='not json' WHERE id=1");
        let error = fixture
            .submit_plan("planner-cap", valid_plan())
            .await
            .unwrap_err();
        assert_eq!(error.code, "internal");
        assert_eq!(
            fixture.ledger("planner-cap"),
            vec![rejected_row(1, "internal")]
        );
    }

    /// A failed ledger write must surface as abnormal on every path, with no
    /// outcome left behind, rather than as the refusal the call would have got.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ledger_write_failure_is_abnormal_on_every_path() {
        let fixture = PlannerFixture::new();
        fixture.execute(LEDGER_DOWN);
        let assert_internal = |result: std::result::Result<Response, EndpointFailure>| {
            let error = result.unwrap_err();
            assert_eq!(error.code, "internal");
            assert_eq!(error.message, "request processing failed");
            assert!(error.is_abnormal());
        };

        assert_internal(fixture.submit_plan("planner-cap", valid_plan()).await);
        assert_internal(fixture.submit_plan("planner-cap", undersized_plan()).await);
        assert_internal(fixture.submit_plan("worker-cap", valid_plan()).await);
        assert_internal(
            fixture
                .request(
                    PROTOCOL_VERSION + 1,
                    "planner-cap",
                    Operation::SubmitPlan {
                        response: valid_plan(),
                    },
                    PROCESS_TIMEOUT,
                )
                .await,
        );
        assert_internal(
            fixture
                .request(
                    PROTOCOL_VERSION,
                    "planner-cap",
                    Operation::SubmitPlan {
                        response: valid_plan(),
                    },
                    Duration::ZERO,
                )
                .await,
        );
        let unauthorized = record_plan_acceptance(&fixture.db_path, "worker-cap", 5, "{}", 40);
        assert_eq!(unauthorized.unwrap_err().code, "internal");

        assert_eq!(fixture.ledger_total(), 0);
        assert_eq!(fixture.submission_count(), 0, "outcomes rolled back");
    }

    /// Either half of a write-phase transaction failing discards both halves,
    /// and the one retained record of the call is its `internal` row.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rolled_back_write_phase_records_one_internal_row() {
        for trigger in [
            "CREATE TRIGGER ledger_refused BEFORE INSERT ON planner_submission_calls
             WHEN NEW.rejection_category IS NOT 'internal'
             BEGIN SELECT RAISE(ABORT, 'ledger refused'); END",
            "CREATE TRIGGER outcome_refused BEFORE UPDATE ON planner_submissions
             BEGIN SELECT RAISE(ABORT, 'outcome refused'); END",
        ] {
            for plan in [valid_plan(), undersized_plan()] {
                let fixture = PlannerFixture::new();
                fixture.execute(trigger);
                let error = fixture.submit_plan("planner-cap", plan).await.unwrap_err();
                assert_eq!(error.code, "internal");
                assert!(error.is_abnormal());
                assert_eq!(
                    fixture.ledger("planner-cap"),
                    vec![rejected_row(1, "internal")]
                );
                assert_eq!(fixture.submission_count(), 0, "outcome rolled back");
            }
        }
    }

    /// Concurrent calls race through the prepare and write phases on their own
    /// connections. However they interleave, each lands exactly one row, `seq`
    /// stays contiguous, and only the first accepted plan stands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_calls_each_land_exactly_one_ledger_row() {
        const VALID_CALLS: usize = 4;
        const INVALID_CALLS: usize = 4;
        const ROUNDS: usize = 10;

        for _ in 0..ROUNDS {
            let fixture = Arc::new(PlannerFixture::new());
            let mut calls = JoinSet::new();
            for call in 0..VALID_CALLS + INVALID_CALLS {
                let fixture = Arc::clone(&fixture);
                calls.spawn(async move {
                    let plan = if call < VALID_CALLS {
                        valid_plan()
                    } else {
                        undersized_plan()
                    };
                    // The shared resolver admits one call and fails the rest
                    // closed, so each call brings its own to reach the ledger race.
                    let plan_validation = PlanValidation {
                        repo_dir: fixture.plan_validation.repo_dir.clone(),
                        path_resolver: super::super::planner::WritablePathResolver::default(),
                    };
                    process_request_with_timeout(
                        fixture.db_path.clone(),
                        "test/repo".into(),
                        plan_validation,
                        fixture.operator_config.clone(),
                        Arc::new(FakeEvidence::default()),
                        Request {
                            version: PROTOCOL_VERSION,
                            capability: "planner-cap".into(),
                            operation: Operation::SubmitPlan { response: plan },
                        },
                        PROCESS_TIMEOUT,
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| error.code)
                });
            }
            let mut accepted = 0;
            while let Some(outcome) = calls.join_next().await {
                match outcome.unwrap() {
                    Ok(()) => accepted += 1,
                    Err(code) => {
                        assert!(
                            matches!(code, "invalid_plan" | "already_submitted"),
                            "{code}"
                        )
                    }
                }
            }
            assert_eq!(accepted, 1);

            let ledger = fixture.ledger("planner-cap");
            let seqs: Vec<i64> = ledger.iter().map(|row| row.0).collect();
            assert_eq!(
                seqs,
                (1..=(VALID_CALLS + INVALID_CALLS) as i64).collect::<Vec<_>>()
            );
            assert_eq!(ledger.iter().filter(|row| row.1 == 1).count(), 1);
            assert!(fixture.submission_row().0.is_some());
        }
    }

    #[test]
    fn prepare_failure_category_is_a_closed_mapping() {
        for (code, category) in [
            ("unauthorized", RejectionCategory::Unauthorized),
            ("already_submitted", RejectionCategory::AlreadySubmitted),
            (
                "submit_budget_exhausted",
                RejectionCategory::BudgetExhausted,
            ),
            ("timeout", RejectionCategory::Timeout),
            ("internal", RejectionCategory::Internal),
            ("invalid_plan", RejectionCategory::Internal),
            ("unsupported_version", RejectionCategory::Internal),
            ("anything else", RejectionCategory::Internal),
        ] {
            assert_eq!(prepare_failure_category(code), category, "{code}");
        }
    }

    #[test]
    fn only_abnormal_endpoint_failures_are_logged() {
        for code in [
            "unauthorized",
            "malformed_frame",
            "request_too_large",
            "malformed_request",
            "unsupported_version",
            "timeout",
            "invalid_operation",
            "forbidden_operation",
            "operation_unavailable",
            "operation_not_found",
            "operation_rejected",
            "operation_limit_reached",
            "invalid_plan",
            "already_submitted",
            "submit_budget_exhausted",
        ] {
            assert!(!EndpointFailure::new(code, "expected rejection").is_abnormal());
        }
        assert!(EndpointFailure::new("internal", "processing failed").is_abnormal());
    }

    #[test]
    fn largest_operation_read_fits_one_response_frame() {
        use quorum_core::collaboration_admission::{
            MAX_COLLABORATION_ID_BYTES, MAX_OPERATION_ERROR_BYTES, MAX_OPERATION_JSON_BYTES,
        };
        // A canonical retained result at its bound re-serializes byte for
        // byte; every other string is maximally escaped control characters.
        let result = serde_json::Value::String("x".repeat(MAX_OPERATION_JSON_BYTES - 2));
        assert_eq!(result.to_string().len(), MAX_OPERATION_JSON_BYTES);
        let escaped = |len| "\u{1}".repeat(len);
        let response = Response::success(ResponseResult::GithubOperation {
            attempt_id: escaped(MAX_COLLABORATION_ID_BYTES),
            error_code: Some("github_invalid_response"),
            operation_id: escaped(MAX_COLLABORATION_ID_BYTES),
            state: GithubOperationState::Succeeded,
            send_state: GithubSendState::Confirmed,
            remote_object_id: Some(escaped(MAX_COLLABORATION_ID_BYTES)),
            result: Some(result),
            error_summary: Some(escaped(MAX_OPERATION_ERROR_BYTES)),
        });
        let body = serde_json::to_vec(&response).unwrap();
        assert!(body.len() <= MAX_RESPONSE_BYTES, "{}", body.len());
    }

    #[test]
    fn inventories_are_phase_scoped() {
        let names = |phase| {
            ALL_PROTOCOL_OPERATIONS
                .into_iter()
                .filter(|operation| operation.allowed_in(phase))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(LiveRunPhase::InitialWorker),
            vec![ProtocolOperation::DeliveryReportWrite]
        );
        assert_eq!(names(LiveRunPhase::ReworkWorker).len(), 5);
        assert_eq!(names(LiveRunPhase::Reviewer).len(), 7);
        assert!(!names(LiveRunPhase::Reviewer).contains(&ProtocolOperation::DeliveryReportWrite));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_out_transaction_rolls_back_before_failure_returns() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("quorum.db");
        let conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO tasks
             (id,title,status,assignee,created_by,created_at,updated_at)
             VALUES (1,'endpoint timeout','working','Worker','test',10,10)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('timeout-cap',1,'Worker','worker',10)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agent_runs
             (task_id,agent_name,role,model,effort,provider,spawned_at)
             VALUES (1,'Worker','worker','test','high','test',10)",
            [],
        )
        .unwrap();
        drop(conn);

        let (locked_tx, locked_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let lock_db = db_path.clone();
        let holder = std::thread::spawn(move || {
            let mut conn = quorum_core::db::open(&lock_db).unwrap();
            let tx = begin_immediate(&mut conn).unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(tx);
        });
        locked_rx.recv().unwrap();

        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            release_tx.send(()).unwrap();
        });
        let error = process_request_with_timeout(
            db_path.clone(),
            "test/repo".into(),
            PlanValidation {
                repo_dir: dir.path().to_path_buf(),
                path_resolver: super::super::planner::WritablePathResolver::default(),
            },
            dir.path().join("config.toml"),
            Arc::new(FakeEvidence::default()),
            Request {
                version: PROTOCOL_VERSION,
                capability: "timeout-cap".into(),
                operation: Operation::Submit {
                    summary: Some("must roll back".into()),
                    verdict: None,
                    feedback: None,
                    feedback_json: None,
                    blocking: None,
                    publication_failure: None,
                },
            },
            Duration::from_millis(20),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "timeout");
        releaser.join().unwrap();
        holder.join().unwrap();

        let mailbox_count: i64 = quorum_core::db::open(&db_path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM mailbox", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mailbox_count, 0);
    }

    use crate::serve::github_outbox::{compose_comment_body, OutboxExecutor};
    use crate::serve::repository_service::{
        CreatedComment, FakeOutcome, FakeRepositoryService, PullRequestNumber, RepositoryFuture,
        RepositoryResponse, RepositoryService, RepositoryTarget,
    };

    /// Answers every request by kind and records each daemon-supplied call.
    #[derive(Default)]
    struct KindService {
        calls: std::sync::Mutex<Vec<(RepositoryTarget, PullRequestNumber, RepositoryRequest)>>,
    }

    impl RepositoryService for KindService {
        fn execute<'a>(
            &'a self,
            target: &'a RepositoryTarget,
            pr: PullRequestNumber,
            request: &'a RepositoryRequest,
        ) -> RepositoryFuture<'a> {
            self.calls
                .lock()
                .unwrap()
                .push((target.clone(), pr, request.clone()));
            let response = match request {
                RepositoryRequest::AddIssueComment { .. } => {
                    RepositoryResponse::IssueComment(CreatedComment {
                        id: 555,
                        html_url: "https://github.com/test/repo/pull/77#issuecomment-555".into(),
                    })
                }
                _ => RepositoryResponse::PullRequestRead(json!({
                    "method": "get_files", "items": [], "truncated": false
                })),
            };
            Box::pin(async move { Ok(response) })
        }
    }

    fn github(operation: ProtocolOperation, arguments: serde_json::Value) -> Operation {
        Operation::Protocol {
            operation,
            attempt_id: None,
            operation_id: None,
            arguments: Some(arguments),
        }
    }

    fn read_operation_request(attempt_id: &str, operation_id: &str) -> Operation {
        Operation::Protocol {
            operation: ProtocolOperation::GithubOperationRead,
            attempt_id: Some(attempt_id.into()),
            operation_id: Some(operation_id.into()),
            arguments: None,
        }
    }

    #[derive(Debug)]
    struct OperationAnswer {
        attempt_id: String,
        operation_id: String,
        state: GithubOperationState,
        send_state: GithubSendState,
        remote_object_id: Option<String>,
        result: Option<serde_json::Value>,
        error_code: Option<&'static str>,
    }

    fn answer(response: Response) -> OperationAnswer {
        match response.result {
            Some(ResponseResult::GithubOperation {
                attempt_id,
                operation_id,
                state,
                send_state,
                remote_object_id,
                result,
                error_code,
                ..
            }) => OperationAnswer {
                attempt_id,
                operation_id,
                state,
                send_state,
                remote_object_id,
                result,
                error_code,
            },
            other => panic!("expected an operation answer: {other:?}"),
        }
    }

    const MARKDOWN: &str = "## Finding `one`\n\nSaid \"quoted\" and 'single' \\ back\\slash.\n\n\
        ```rust\nlet x = \"y\"; // `tick`\n```\n\n- item\n  - nested `code`\n\n> quote\n\
        | a | b |\n|---|---|\n| 1 | 2 |\n\n~~~suggestion\nfixed();\n~~~\ntrailing spaces  \n";

    fn comment_arguments(body: &str) -> serde_json::Value {
        json!({"owner":"Test","repo":"Repo","issue_number":77,"body":body})
    }

    async fn run_executor(fixture: &PlannerFixture, service: Arc<dyn RepositoryService>) {
        let target =
            RepositoryTarget::new(fixture.plan_validation.repo_dir.clone(), Some("test/repo"))
                .unwrap();
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = OutboxExecutor::new(fixture.db_path.clone(), target, service)
            .with_timing(Duration::from_millis(20), Duration::from_secs(5));
        let handle = tokio::spawn(executor.run(shutdown.clone()));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let pending: i64 = quorum_core::db::open(&fixture.db_path)
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM github_agent_operations
                     WHERE state='running' OR (state='queued' AND attempts=0)",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            if pending == 0 {
                break;
            }
            assert!(Instant::now() < deadline, "executor did not settle");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        shutdown.store(true, Ordering::SeqCst);
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_bound_read_and_comment_execute_through_the_outbox() {
        let fixture = live_run_fixture();
        let read = answer(
            fixture
                .call(
                    "live-reviewer",
                    github(
                        ProtocolOperation::PullRequestRead,
                        json!({"method":"get_files","owner":"test","repo":"repo",
                               "pullNumber":77,"page":2,"perPage":5}),
                    ),
                )
                .await
                .unwrap(),
        );
        assert_eq!(read.state, GithubOperationState::Queued);
        assert!(read.attempt_id.starts_with("att-"));
        let comment = answer(
            fixture
                .call(
                    "live-reviewer",
                    github(
                        ProtocolOperation::AddIssueComment,
                        comment_arguments(MARKDOWN),
                    ),
                )
                .await
                .unwrap(),
        );
        assert_eq!(comment.attempt_id, read.attempt_id, "one attempt per run");
        assert_eq!(comment.state, GithubOperationState::Queued);

        let service = Arc::new(KindService::default());
        run_executor(&fixture, service.clone()).await;

        let calls = service.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        let marker: String = quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .query_row(
                "SELECT github_marker FROM github_agent_operations WHERE operation_id=?1",
                [&comment.operation_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(marker.starts_with("<!-- quorum-op:"));
        for (target, pr, request) in &calls {
            assert_eq!(target.gh_repo(), Some("test/repo"));
            assert_eq!(pr.get(), 77);
            match request {
                RepositoryRequest::GetFiles { page, per_page } => {
                    assert_eq!((*page, *per_page), (2, 5));
                }
                RepositoryRequest::AddIssueComment { body } => {
                    // Authored Markdown survives byte for byte, then the marker.
                    assert!(body.starts_with(MARKDOWN));
                    assert_eq!(body, &compose_comment_body(MARKDOWN, &marker));
                    assert_eq!(body, &format!("{MARKDOWN}\n{marker}"));
                }
                other => panic!("unexpected request {other:?}"),
            }
        }

        let settled = answer(
            fixture
                .call(
                    "live-reviewer",
                    read_operation_request(&comment.attempt_id, &comment.operation_id),
                )
                .await
                .unwrap(),
        );
        assert_eq!(settled.state, GithubOperationState::Succeeded);
        assert_eq!(settled.send_state, GithubSendState::Confirmed);
        assert_eq!(settled.remote_object_id.as_deref(), Some("555"));
        assert_eq!(settled.result.unwrap()["id"], 555);
        assert_eq!(settled.error_code, None);
        // An identical retry returns the original comment, never a second post.
        let retried = answer(
            fixture
                .call(
                    "live-reviewer",
                    github(
                        ProtocolOperation::AddIssueComment,
                        comment_arguments(MARKDOWN),
                    ),
                )
                .await
                .unwrap(),
        );
        assert_eq!(retried.operation_id, comment.operation_id);
        assert_eq!(retried.state, GithubOperationState::Succeeded);
        assert_eq!(count(&fixture, "github_agent_operations"), 2);
        assert_eq!(count(&fixture, "github_collaboration_attempts"), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outages_and_unknown_write_outcomes_are_typed_errors() {
        let fixture = live_run_fixture();
        let comment = answer(
            fixture
                .call(
                    "live-reviewer",
                    github(
                        ProtocolOperation::AddIssueComment,
                        comment_arguments("hello"),
                    ),
                )
                .await
                .unwrap(),
        );
        run_executor(
            &fixture,
            Arc::new(FakeRepositoryService::scripted([FakeOutcome::ambiguous()])),
        )
        .await;
        let settled = answer(
            fixture
                .call(
                    "live-reviewer",
                    read_operation_request(&comment.attempt_id, &comment.operation_id),
                )
                .await
                .unwrap(),
        );
        assert_eq!(settled.state, GithubOperationState::Failed);
        assert_eq!(settled.send_state, GithubSendState::Ambiguous);
        assert_eq!(settled.error_code, Some("github_outcome_unknown"));

        let read = answer(
            fixture
                .call(
                    "live-reviewer",
                    github(
                        ProtocolOperation::PullRequestRead,
                        json!({"method":"get","owner":"test","repo":"repo","pullNumber":77}),
                    ),
                )
                .await
                .unwrap(),
        );
        run_executor(
            &fixture,
            Arc::new(FakeRepositoryService::scripted([FakeOutcome::Fail(
                crate::serve::repository_service::RepositoryErrorKind::RateLimited,
            )])),
        )
        .await;
        let retrying = answer(
            fixture
                .call(
                    "live-reviewer",
                    read_operation_request(&read.attempt_id, &read.operation_id),
                )
                .await
                .unwrap(),
        );
        assert_eq!(retrying.state, GithubOperationState::Queued);
        assert_eq!(retrying.error_code, Some("github_rate_limited"));
    }

    #[tokio::test]
    async fn foreign_targets_bad_bodies_and_wrong_phases_admit_nothing() {
        let fixture = live_run_fixture();
        let read =
            |arguments: serde_json::Value| github(ProtocolOperation::PullRequestRead, arguments);
        let comment =
            |arguments: serde_json::Value| github(ProtocolOperation::AddIssueComment, arguments);
        let cases: Vec<(&str, Operation, &str)> = vec![
            (
                "live-reviewer",
                read(json!({"method":"get","owner":"test","repo":"repo","pullNumber":78})),
                "forbidden_operation",
            ),
            (
                "live-reviewer",
                read(json!({"method":"get","owner":"evil","repo":"repo","pullNumber":77})),
                "forbidden_operation",
            ),
            (
                "live-reviewer",
                read(json!({"method":"get","owner":"test","repo":"other","pullNumber":77})),
                "forbidden_operation",
            ),
            (
                "live-reviewer",
                comment(json!({"owner":"test","repo":"repo","issue_number":1,"body":"x"})),
                "forbidden_operation",
            ),
            (
                "live-reviewer",
                comment(json!({"owner":"evil","repo":"repo","issue_number":77,"body":"x"})),
                "forbidden_operation",
            ),
            (
                "live-reviewer",
                comment(comment_arguments(&"x".repeat(MAX_COMMENT_BODY_BYTES + 1))),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                comment(comment_arguments("a\0b")),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                comment(comment_arguments("   \n")),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                comment(comment_arguments("```\nunclosed")),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                comment(
                    json!({"owner":"test","repo":"repo","issue_number":77,"body":"x","clientRequestId":"has space"}),
                ),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                comment(
                    json!({"owner":"test","repo":"repo","issue_number":77,"body":"x","repo_override":"evil/x"}),
                ),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                read(
                    json!({"method":"get_check_runs","owner":"test","repo":"repo","pullNumber":77}),
                ),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                read(
                    json!({"method":"get_files","owner":"test","repo":"repo","pullNumber":77,"perPage":101}),
                ),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                read(
                    json!({"method":"get","owner":"test","repo":"repo","pullNumber":77,"after":"cursor"}),
                ),
                "invalid_operation",
            ),
            (
                "live-reviewer",
                Operation::Protocol {
                    operation: ProtocolOperation::PullRequestRead,
                    attempt_id: None,
                    operation_id: None,
                    arguments: None,
                },
                "invalid_operation",
            ),
            (
                "live-worker",
                read(json!({"method":"get","owner":"test","repo":"repo","pullNumber":77})),
                "forbidden_operation",
            ),
            (
                "revoked-reviewer",
                read(json!({"method":"get","owner":"test","repo":"repo","pullNumber":78})),
                "unauthorized",
            ),
        ];
        for (capability, operation, code) in cases {
            let error = fixture.call(capability, operation).await.unwrap_err();
            assert_eq!(error.code, code, "{capability}: {}", error.message);
        }
        assert_eq!(count(&fixture, "github_agent_operations"), 0);
        assert_eq!(count(&fixture, "github_collaboration_attempts"), 0);
        assert_eq!(count(&fixture, "errors"), 0);
        // A body at the bound is accepted.
        let at_bound = fixture
            .call(
                "live-reviewer",
                comment(comment_arguments(&"x".repeat(MAX_COMMENT_BODY_BYTES))),
            )
            .await
            .unwrap();
        assert_eq!(answer(at_bound).state, GithubOperationState::Queued);
    }

    #[test]
    fn unclosed_fences_are_detected() {
        for open in [
            "```\nx",
            "~~~~\nx\n~~~",
            "  ```rust\nx\n``` trailing",
            "```\nx\n~~~",
        ] {
            assert!(has_unclosed_fence(open), "{open:?}");
        }
        for closed in [
            "plain",
            "```\nx\n```",
            "~~~~\nx\n~~~~~",
            "inline ``` not a fence",
            "``` `inline` ```",
            "    ```\nindented code",
            "```a\n```\n~~~\n~~~",
        ] {
            assert!(!has_unclosed_fence(closed), "{closed:?}");
        }
    }

    #[test]
    fn github_error_codes_are_a_closed_classification() {
        use GithubOperationState::*;
        use GithubSendState::*;
        for (state, send, kind, code) in [
            (Succeeded, Confirmed, None, None),
            (Queued, NotStarted, None, None),
            (Running, Ambiguous, None, None),
            (
                Queued,
                NotStarted,
                Some("rate_limited"),
                Some("github_rate_limited"),
            ),
            (
                Queued,
                NotStarted,
                Some("transient_failure"),
                Some("github_unavailable"),
            ),
            (
                Failed,
                NotStarted,
                Some("timed_out"),
                Some("github_unavailable"),
            ),
            (
                Failed,
                NotStarted,
                Some("deadline_exceeded"),
                Some("github_unavailable"),
            ),
            (
                Failed,
                DefinitelyUnsent,
                Some("permanent_failure"),
                Some("github_rejected"),
            ),
            (
                Failed,
                Ambiguous,
                Some("timed_out"),
                Some("github_outcome_unknown"),
            ),
            (
                Failed,
                NotStarted,
                Some("response_too_large"),
                Some("github_invalid_response"),
            ),
            (
                Failed,
                NotStarted,
                Some("unsupported_request"),
                Some("github_operation_failed"),
            ),
            (
                Cancelled,
                NotStarted,
                Some("attempt_revoked"),
                Some("github_operation_cancelled"),
            ),
        ] {
            assert_eq!(
                github_error_code(state, send, kind),
                code,
                "{state:?}/{send:?}/{kind:?}"
            );
        }
    }

    const LAUNCH_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OTHER_SHA: &str = "cccccccccccccccccccccccccccccccccccccccc";
    /// Old side lines 1-3, new side lines 1-4, one hunk.
    const LAUNCH_DIFF: &str = "diff --git a/src/lib.rs b/src/lib.rs\n\
        index 1111111..2222222 100644\n\
        --- a/src/lib.rs\n\
        +++ b/src/lib.rs\n\
        @@ -1,3 +1,4 @@\n \
        fn a() {}\n\
        -fn b() {}\n\
        +fn b2() {}\n\
        +fn c() {}\n \
        fn d() {}\n";

    /// The live reviewer fixture with a target branch and a scripted diff
    /// for its launch SHA.
    fn review_fixture() -> PlannerFixture {
        let fixture = live_run_fixture();
        fixture.execute("UPDATE tasks SET target_branch='main' WHERE id IN (100,101)");
        *fixture.evidence.diff.lock().unwrap() =
            Some((LAUNCH_SHA.to_owned(), LAUNCH_DIFF.as_bytes().to_vec()));
        fixture
    }

    fn review_write(arguments: serde_json::Value) -> Operation {
        let mut target = json!({"owner":"test","repo":"repo","pullNumber":77});
        target
            .as_object_mut()
            .unwrap()
            .extend(arguments.as_object().unwrap().clone());
        github(ProtocolOperation::PullRequestReviewWrite, target)
    }

    fn inline(anchor: serde_json::Value) -> Operation {
        let mut arguments = json!({"owner":"test","repo":"repo","pullNumber":77,
            "subjectType":"LINE","body":"Finding `x`"});
        arguments
            .as_object_mut()
            .unwrap()
            .extend(anchor.as_object().unwrap().clone());
        github(ProtocolOperation::AddCommentToPendingReview, arguments)
    }

    fn create() -> Operation {
        review_write(json!({"method":"create","commitID":LAUNCH_SHA}))
    }

    fn submit_comment() -> Operation {
        review_write(json!({"method":"submit_pending","event":"COMMENT","body":"## Summary\n\nok"}))
    }

    fn verdict(publication_failure: Option<&str>) -> Operation {
        Operation::Submit {
            summary: None,
            verdict: Some("approved".into()),
            feedback: None,
            feedback_json: None,
            blocking: Some(0),
            publication_failure: publication_failure.map(str::to_owned),
        }
    }

    async fn refused(fixture: &PlannerFixture, capability: &str, operation: Operation) -> String {
        let error = fixture.call(capability, operation).await.unwrap_err();
        assert!(!error.is_abnormal() || error.code.ends_with("_unavailable"));
        error.code.to_owned()
    }

    /// `(kind, review_sequence, request_json)` of every stored operation.
    fn stored(fixture: &PlannerFixture) -> Vec<(String, Option<i64>, serde_json::Value)> {
        quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .prepare(
                "SELECT kind,review_sequence,request_json FROM github_agent_operations
                 ORDER BY review_sequence",
            )
            .unwrap()
            .query_map([], |row| {
                let json: String = row.get(2)?;
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    serde_json::from_str(&json).unwrap(),
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    fn attempt_state(fixture: &PlannerFixture) -> (String, Option<String>, i64) {
        quorum_core::db::open(&fixture.db_path)
            .unwrap()
            .query_row(
                "SELECT state,active_run_id,review_sealed FROM github_collaboration_attempts",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    fn settle_review_rows(fixture: &PlannerFixture, state: &str) {
        fixture.execute(&format!(
            "UPDATE github_agent_operations SET state='{state}',
               send_state=CASE WHEN '{state}'='succeeded' THEN 'confirmed'
                               ELSE 'definitely_unsent' END
             WHERE review_sequence IS NOT NULL"
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pull_request_review_write_admits_only_launch_sha_create_and_comment_submit() {
        let fixture = review_fixture();
        let cases = [
            (
                review_write(json!({"method":"create","commitID":OTHER_SHA})),
                "review_commit_mismatch",
            ),
            (
                review_write(json!({"method":"create"})),
                "review_commit_mismatch",
            ),
            (
                review_write(json!({"method":"create","commitID":LAUNCH_SHA,"event":"APPROVE"})),
                "review_event_unsupported",
            ),
            (
                review_write(json!({"method":"submit_pending","event":"APPROVE","body":"x"})),
                "review_event_unsupported",
            ),
            (
                review_write(
                    json!({"method":"submit_pending","event":"REQUEST_CHANGES","body":"x"}),
                ),
                "review_event_unsupported",
            ),
            (
                review_write(json!({"method":"submit_pending","body":"x"})),
                "review_event_unsupported",
            ),
            (
                review_write(json!({"method":"delete_pending"})),
                "review_method_unsupported",
            ),
            (
                review_write(json!({"method":"dismiss","body":"x"})),
                "review_method_unsupported",
            ),
            (
                review_write(
                    json!({"method":"submit_pending","event":"COMMENT","body":"x",
                                    "reviewID":12}),
                ),
                "review_id_unsupported",
            ),
            (
                review_write(json!({"method":"create","commitID":LAUNCH_SHA,"review_id":"PRR_1"})),
                "review_id_unsupported",
            ),
            (
                review_write(json!({"method":"submit_pending","event":"COMMENT"})),
                "invalid_operation",
            ),
            (
                review_write(json!({"method":"create","commitID":LAUNCH_SHA,
                                    "clientRequestId":"mine"})),
                "invalid_operation",
            ),
            // The core sequence store refuses a submit before its create.
            (submit_comment(), "review_not_created"),
        ];
        for (operation, code) in cases {
            assert_eq!(refused(&fixture, "live-reviewer", operation).await, code);
        }
        let worker = fixture.call("live-worker", create()).await.unwrap_err();
        assert_eq!(worker.code, "forbidden_operation");
        assert!(stored(&fixture).is_empty());

        let created = answer(fixture.call("live-reviewer", create()).await.unwrap());
        assert_eq!(created.state, GithubOperationState::Queued);
        // A repeated create resumes the attempt's one review.
        let resumed = answer(
            fixture
                .call(
                    "live-reviewer",
                    review_write(json!({"method":"create","commitID":LAUNCH_SHA,"body":"draft"})),
                )
                .await
                .unwrap(),
        );
        assert_eq!(resumed.operation_id, created.operation_id);
        let submitted = answer(
            fixture
                .call("live-reviewer", submit_comment())
                .await
                .unwrap(),
        );
        let again = answer(
            fixture
                .call("live-reviewer", submit_comment())
                .await
                .unwrap(),
        );
        assert_eq!(again.operation_id, submitted.operation_id);
        assert_eq!(
            stored(&fixture),
            vec![
                (
                    "pull_request_review_write".into(),
                    Some(0),
                    json!({"method":"create","commitID":LAUNCH_SHA})
                ),
                (
                    "pull_request_review_write".into(),
                    Some(1),
                    json!({"method":"submit_pending","event":"COMMENT","body":"## Summary\n\nok"})
                ),
            ]
        );
        assert_eq!(attempt_state(&fixture).2, 1, "submit sealed the review");
        assert_eq!(count(&fixture, "errors"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inline_anchors_are_validated_against_the_launch_sha_before_enqueue() {
        let fixture = review_fixture();
        let invalid = [
            (
                json!({"path":"src/other.rs","line":2,"side":"RIGHT"}),
                "anchor_path_not_in_diff",
            ),
            (
                json!({"path":"src/lib.rs","line":40,"side":"RIGHT"}),
                "anchor_line_outside_diff",
            ),
            (
                json!({"path":"src/lib.rs","line":4,"side":"LEFT"}),
                "anchor_side_mismatch",
            ),
            (
                json!({"path":"src/lib.rs","side":"RIGHT"}),
                "anchor_missing_line",
            ),
            (
                json!({"path":"src/lib.rs","line":2,"side":"UP"}),
                "anchor_invalid_side",
            ),
            (
                json!({"path":"src/lib.rs","line":2,"side":"RIGHT","startLine":3}),
                "anchor_invalid_range",
            ),
            (
                json!({"path":"src/lib.rs","line":3,"side":"RIGHT","startLine":1,
                       "startSide":"LEFT"}),
                "anchor_invalid_range",
            ),
            (
                json!({"path":"src/lib.rs","line":2,"side":"RIGHT","subjectType":"FILE"}),
                "anchor_unsupported_subject_type",
            ),
            (
                json!({"path":"src/lib.rs","line":2,"side":"RIGHT","body":"```\nopen"}),
                "invalid_operation",
            ),
        ];
        for (anchor, code) in invalid {
            assert_eq!(
                refused(&fixture, "live-reviewer", inline(anchor.clone())).await,
                code,
                "{anchor}"
            );
        }
        // A diff computed for any other SHA is stale; a missing diff refuses.
        *fixture.evidence.diff.lock().unwrap() =
            Some((OTHER_SHA.to_owned(), LAUNCH_DIFF.as_bytes().to_vec()));
        let single = json!({"path":"src/lib.rs","line":3,"side":"RIGHT"});
        assert_eq!(
            refused(&fixture, "live-reviewer", inline(single.clone())).await,
            "anchor_stale_sha"
        );
        *fixture.evidence.diff.lock().unwrap() = None;
        assert_eq!(
            refused(&fixture, "live-reviewer", inline(single.clone())).await,
            "review_diff_unavailable"
        );
        *fixture.evidence.diff.lock().unwrap() =
            Some((LAUNCH_SHA.to_owned(), b"garbage\n".to_vec()));
        assert_eq!(
            refused(&fixture, "live-reviewer", inline(single.clone())).await,
            "review_diff_unavailable"
        );
        assert!(
            stored(&fixture).is_empty(),
            "no invalid anchor was enqueued"
        );
        assert_eq!(
            count(&fixture, "github_collaboration_attempts"),
            0,
            "a refused anchor never reached admission"
        );
        let worker = fixture
            .call("live-worker", inline(single.clone()))
            .await
            .unwrap_err();
        assert_eq!(worker.code, "forbidden_operation");

        *fixture.evidence.diff.lock().unwrap() =
            Some((LAUNCH_SHA.to_owned(), LAUNCH_DIFF.as_bytes().to_vec()));
        // Findings need their review first.
        assert_eq!(
            refused(&fixture, "live-reviewer", inline(single.clone())).await,
            "review_not_created"
        );
        answer(fixture.call("live-reviewer", create()).await.unwrap());
        answer(
            fixture
                .call("live-reviewer", inline(single.clone()))
                .await
                .unwrap(),
        );
        let multi = json!({"path":"src/lib.rs","line":4,"side":"RIGHT","startLine":2,
                           "startSide":"RIGHT"});
        answer(fixture.call("live-reviewer", inline(multi)).await.unwrap());
        let left = json!({"path":"src/lib.rs","line":2,"side":"LEFT"});
        answer(fixture.call("live-reviewer", inline(left)).await.unwrap());
        answer(
            fixture
                .call("live-reviewer", submit_comment())
                .await
                .unwrap(),
        );
        assert_eq!(
            refused(
                &fixture,
                "live-reviewer",
                inline(json!({"path":"src/lib.rs","line":1,"side":"RIGHT"}))
            )
            .await,
            "review_sealed"
        );
        let rows = stored(&fixture);
        let inline_rows: Vec<_> = rows
            .iter()
            .filter(|(kind, _, _)| kind == "add_comment_to_pending_review")
            .map(|(_, sequence, json)| (*sequence, json.clone()))
            .collect();
        assert_eq!(
            inline_rows,
            vec![
                (
                    Some(1),
                    json!({"path":"src/lib.rs","line":3,"side":"RIGHT","subjectType":"LINE",
                           "body":"Finding `x`"})
                ),
                (
                    Some(2),
                    json!({"path":"src/lib.rs","line":4,"side":"RIGHT","startLine":2,
                           "startSide":"RIGHT","subjectType":"LINE","body":"Finding `x`"})
                ),
                (
                    Some(3),
                    json!({"path":"src/lib.rs","line":2,"side":"LEFT","subjectType":"LINE",
                           "body":"Finding `x`"})
                ),
            ]
        );
        assert_eq!(rows.len(), 5);
        assert_eq!(count(&fixture, "errors"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verdict_is_refused_until_the_comment_review_is_published() {
        let fixture = review_fixture();
        answer(fixture.call("live-reviewer", create()).await.unwrap());

        // Head unknown: the verdict cannot be proven, so nothing is recorded.
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(None)).await,
            "review_head_unavailable"
        );
        *fixture.evidence.head.lock().unwrap() = Some(LAUNCH_SHA.to_owned());
        // Created but not submitted.
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(None)).await,
            "review_not_published"
        );
        // A failure reason is refused while a review step is in flight.
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(Some("operation_failed"))).await,
            "review_not_published"
        );
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(Some("github_down"))).await,
            "invalid_operation"
        );
        answer(
            fixture
                .call("live-reviewer", submit_comment())
                .await
                .unwrap(),
        );
        // Sealed, but the steps are still queued.
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(None)).await,
            "review_not_published"
        );
        settle_review_rows(&fixture, "succeeded");
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(Some("operation_failed"))).await,
            "review_publication_not_failed"
        );
        *fixture.evidence.head.lock().unwrap() = Some(OTHER_SHA.to_owned());
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(None)).await,
            "review_head_moved"
        );
        assert_eq!(
            count(&fixture, "mailbox"),
            0,
            "every refusal recorded nothing"
        );
        assert_eq!(attempt_state(&fixture).0, "active");

        *fixture.evidence.head.lock().unwrap() = Some(LAUNCH_SHA.to_owned());
        let admitted = fixture.call("live-reviewer", verdict(None)).await.unwrap();
        assert!(matches!(
            admitted.result,
            Some(ResponseResult::Mailbox { .. })
        ));
        assert_eq!(count(&fixture, "mailbox"), 1);
        // The proof's transaction completed the attempt with the verdict.
        let (state, active_run, _) = attempt_state(&fixture);
        assert_eq!((state.as_str(), active_run), ("completed", None));
        assert_eq!(count(&fixture, "errors"), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verdict_after_a_failed_publication_needs_the_typed_reason() {
        let fixture = review_fixture();
        *fixture.evidence.head.lock().unwrap() = Some(LAUNCH_SHA.to_owned());
        // No review begun: a failure reason has nothing to stand on.
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(Some("operation_failed"))).await,
            "review_publication_not_failed"
        );
        let worker = fixture
            .call(
                "live-worker",
                Operation::Submit {
                    summary: None,
                    verdict: None,
                    feedback: None,
                    feedback_json: None,
                    blocking: None,
                    publication_failure: Some("operation_failed".into()),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(worker.code, "invalid_operation");

        answer(fixture.call("live-reviewer", create()).await.unwrap());
        settle_review_rows(&fixture, "failed");
        // Without the reason the proof still requires full publication.
        assert_eq!(
            refused(&fixture, "live-reviewer", verdict(None)).await,
            "review_not_published"
        );
        let heads_before = fixture.evidence.head_calls.load(Ordering::SeqCst);
        fixture
            .call("live-reviewer", verdict(Some("operation_failed")))
            .await
            .unwrap();
        assert_eq!(
            fixture.evidence.head_calls.load(Ordering::SeqCst),
            heads_before,
            "the publication-failed path needs no head fetch"
        );
        assert_eq!(count(&fixture, "mailbox"), 1);
        assert_eq!(attempt_state(&fixture).0, "completed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verdict_without_a_started_review_is_unguarded_and_fetches_nothing() {
        let fixture = review_fixture();
        fixture.call("live-reviewer", verdict(None)).await.unwrap();
        assert_eq!(count(&fixture, "mailbox"), 1);
        assert_eq!(fixture.evidence.head_calls.load(Ordering::SeqCst), 0);
        assert_eq!(count(&fixture, "github_collaboration_attempts"), 0);
    }

    #[test]
    fn review_refusals_are_typed_clean_negatives() {
        for code in REVIEW_REFUSAL_CODES {
            assert!(
                !EndpointFailure::new(code, "refused").is_abnormal(),
                "{code}"
            );
        }
        for code in ["review_diff_unavailable", "review_head_unavailable"] {
            assert!(EndpointFailure::new(code, "fault").is_abnormal(), "{code}");
        }
        use AnchorError::*;
        let codes: std::collections::BTreeSet<_> = [
            StaleSha,
            UnsupportedSubjectType,
            PathNotInDiff,
            MissingLine,
            InvalidSide,
            LineOutsideDiff,
            SideMismatch,
            InvalidRange,
        ]
        .into_iter()
        .map(|error| {
            let failure = anchor_failure(error);
            assert_eq!(failure.code, format!("anchor_{}", error.code()));
            assert!(is_review_refusal(failure.code));
            failure.code
        })
        .collect();
        assert_eq!(codes.len(), 8, "each validator error has its own code");
    }

    #[tokio::test]
    async fn production_evidence_diffs_the_launch_sha_against_its_base() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success(), "{args:?}: {output:?}");
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        git(&["init", "-q", "-b", "main"]);
        git(&["config", "user.email", "t@example.com"]);
        git(&["config", "user.name", "T"]);
        std::fs::write(repo.join("lib.rs"), "a\nb\nc\nd\ne\nf\ng\nh\n").unwrap();
        git(&["add", "lib.rs"]);
        git(&["commit", "-q", "-m", "base"]);
        git(&["update-ref", "refs/remotes/origin/main", "HEAD"]);
        git(&["checkout", "-q", "-b", "feature"]);
        std::fs::write(repo.join("lib.rs"), "a\nb\nc\nd\nE\nf\ng\nh\n").unwrap();
        git(&["commit", "-q", "-am", "change"]);
        let launch = git(&["rev-parse", "HEAD"]);
        // A base advanced after the fork does not enter the PR diff.
        git(&["checkout", "-q", "main"]);
        std::fs::write(repo.join("other.rs"), "x\n").unwrap();
        git(&["add", "other.rs"]);
        git(&["commit", "-q", "-m", "base moves"]);
        git(&["update-ref", "refs/remotes/origin/main", "HEAD"]);

        let evidence = RepositoryReviewEvidence::new(
            repo,
            "test/repo",
            Arc::new(FakeRepositoryService::scripted([])),
        );
        let (sha, diff) = evidence.launch_diff("main", &launch).await.unwrap();
        assert_eq!(sha, launch);
        let parsed = LaunchDiff::parse(&sha, &diff).unwrap();
        let anchor = |path: &str, line| InlineAnchor {
            path: path.into(),
            line: Some(line),
            side: Some("RIGHT".into()),
            ..InlineAnchor::default()
        };
        assert!(parsed.validate(&launch, &anchor("lib.rs", 5)).is_ok());
        assert_eq!(
            parsed.validate(&launch, &anchor("other.rs", 1)),
            Err(AnchorError::PathNotInDiff)
        );
        assert!(evidence.launch_diff("main", OTHER_SHA).await.is_err());
        assert!(evidence.launch_diff("bad..ref", &launch).await.is_err());
    }
}
