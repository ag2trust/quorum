//! Bounded, daemon-owned IPC for managed agent runs.
//!
//! The endpoint is deliberately not a general daemon RPC surface. One Unix
//! socket accepts one length-prefixed JSON request per connection, resolves
//! all authority from the supplied run capability, and exposes only the
//! closed operations below. Repository identity is daemon configuration.

use quorum_core::capabilities::{LiveRunContext, LiveRunPhase};
use quorum_core::db::begin_immediate;
use quorum_core::error::{QuorumError, Result};
use quorum_core::planner_submissions::{self, CallOutcome, RejectionCategory, SubmitOutcome};
use rusqlite::{params, Transaction};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
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
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024;
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
        task_id: i64,
        agent: String,
        note: String,
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
    Inventory {
        repository: String,
        task_id: i64,
        role: String,
        pr: Option<i64>,
        review_revision: Option<String>,
        phase: InventoryPhase,
        operations: Vec<ProtocolOperation>,
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
            | "invalid_plan"
            | "already_submitted"
            | "submit_budget_exhausted" => false,
            _ => true,
        }
    }

    fn response(self) -> Response {
        Response::failure(self.code, self.message)
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
) {
    let response = match read_request(&mut stream).await {
        Ok(request) => match process_request(db_path, repository, plan_validation, request).await {
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
    request: Request,
) -> std::result::Result<Response, EndpointFailure> {
    process_request_with_timeout(
        db_path,
        repository,
        plan_validation,
        request,
        PROCESS_TIMEOUT,
    )
    .await
}

async fn process_request_with_timeout(
    db_path: PathBuf,
    repository: String,
    plan_validation: PlanValidation,
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

    let state = Arc::new(AtomicU8::new(PROCESSING));
    let deadline = Instant::now() + process_timeout;
    let blocking_state = Arc::clone(&state);
    let mut task = tokio::task::spawn_blocking(move || {
        process_request_blocking(&db_path, &repository, request, &blocking_state, deadline)
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
    request: Request,
    state: &AtomicU8,
    deadline: Instant,
) -> std::result::Result<Response, EndpointFailure> {
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
            if task_id != context.task_id || agent != context.agent {
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
        Operation::Submit {
            summary,
            verdict,
            feedback,
            feedback_json,
            blocking,
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
            let payload = validate_submission(
                &request.capability,
                &context,
                verdict.as_deref(),
                feedback.as_deref(),
                feedback_json.as_deref(),
                blocking,
            )?;
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
        Operation::Protocol { operation } => {
            let context = resolve_any_role(&tx, &request.capability)?;
            if !operation.allowed_in(context.phase) {
                return Err(EndpointFailure::new(
                    "forbidden_operation",
                    "operation is not available to this run",
                ));
            }
            return Err(EndpointFailure::new(
                "operation_unavailable",
                "operation is not implemented",
            ));
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
    let prepared = tokio::time::timeout(
        process_timeout,
        prepare_and_validate_plan(
            db_path.clone(),
            plan_validation,
            capability.clone(),
            response,
        ),
    )
    .await
    .unwrap_or_else(|_| {
        Err(EndpointFailure::new(
            "timeout",
            "request processing timed out",
        ))
    });
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
    tx: &Transaction<'_>,
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
            },
            Operation::React {
                state: "blocked".into(),
                reason: Some("planner should not react".into()),
            },
            Operation::AppendNote {
                task_id: 1,
                agent: "Planner".into(),
                note: "planner should not append".into(),
            },
            Operation::Protocol {
                operation: ProtocolOperation::PullRequestRead,
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
            "invalid_plan",
            "already_submitted",
            "submit_budget_exhausted",
        ] {
            assert!(!EndpointFailure::new(code, "expected rejection").is_abnormal());
        }
        assert!(EndpointFailure::new("internal", "processing failed").is_abnormal());
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
            Request {
                version: PROTOCOL_VERSION,
                capability: "timeout-cap".into(),
                operation: Operation::Submit {
                    summary: Some("must roll back".into()),
                    verdict: None,
                    feedback: None,
                    feedback_json: None,
                    blocking: None,
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
}
