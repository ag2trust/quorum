//! Bounded, daemon-owned IPC for managed agent runs.
//!
//! The endpoint is deliberately not a general daemon RPC surface. One Unix
//! socket accepts one length-prefixed JSON request per connection, resolves
//! all authority from the supplied run capability, and exposes only the
//! closed operations below. Repository identity is daemon configuration.

use quorum_core::capabilities::{LiveRunContext, LiveRunPhase};
use quorum_core::db::begin_immediate;
use quorum_core::error::{QuorumError, Result};
use quorum_core::planner_submissions::{self, SubmitOutcome};
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
        phase: LiveRunPhase,
        operations: Vec<ProtocolOperation>,
    },
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

pub struct AgentEndpoint {
    locator: PathBuf,
    artifact_dir: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl AgentEndpoint {
    pub async fn start(db_path: &Path, repository: &str, repo_dir: &Path) -> Result<Self> {
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
        let repo_dir = repo_dir.to_path_buf();
        let task = tokio::spawn(run_listener(
            listener,
            db_path,
            repository,
            repo_dir,
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
    repo_dir: PathBuf,
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
                            repo_dir.clone(),
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
    repo_dir: PathBuf,
) {
    let response = match read_request(&mut stream).await {
        Ok(request) => match process_request(db_path, repository, repo_dir, request).await {
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
    repo_dir: PathBuf,
    request: Request,
) -> std::result::Result<Response, EndpointFailure> {
    process_request_with_timeout(db_path, repository, repo_dir, request, PROCESS_TIMEOUT).await
}

async fn process_request_with_timeout(
    db_path: PathBuf,
    repository: String,
    repo_dir: PathBuf,
    request: Request,
    process_timeout: Duration,
) -> std::result::Result<Response, EndpointFailure> {
    if request.version != PROTOCOL_VERSION {
        return Err(EndpointFailure::new(
            "unsupported_version",
            "unsupported protocol version",
        ));
    }
    if request.capability.is_empty()
        || request.capability.len() > 128
        || request.capability.contains('\0')
    {
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
            return submit_plan(db_path, repo_dir, request.capability, response).await;
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
        Operation::React { state } => {
            let context = resolve(&tx, &request.capability, "worker")?;
            if !matches!(state.as_str(), "blocked" | "failed" | "needs-info" | "note") {
                return Err(EndpointFailure::new(
                    "invalid_operation",
                    "invalid reaction state",
                ));
            }
            let mailbox_id = insert_mailbox(
                &tx,
                &context,
                "task_update",
                None,
                None,
                None,
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
        Operation::Inventory => {
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
                phase: context.phase,
                operations,
            })
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
/// This path does not use the cancellation state machine that guards the
/// blocking operations: `spawn_blocking` work cannot be aborted, so a cancelled
/// wait could report a timeout for a commit that already landed. Each step is
/// bounded instead — `busy_timeout` on every connection, and
/// `validate_for_source`'s own resolution timeout.
async fn submit_plan(
    db_path: PathBuf,
    repo_dir: PathBuf,
    capability: String,
    response: serde_json::Value,
) -> std::result::Result<Response, EndpointFailure> {
    let context = {
        let db_path = db_path.clone();
        let capability = capability.clone();
        in_blocking(move || prepare_plan_submission(&db_path, &capability)).await?
    };

    let serialized = serde_json::to_string(&response)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    if let Err(message) =
        validate_plan_submission(&serialized, &repo_dir, &context.source_dependency_ids).await
    {
        let graph_id = context.graph_id;
        let rejection_db = db_path.clone();
        let rejection_capability = capability.clone();
        in_blocking(move || record_plan_rejection(&rejection_db, &rejection_capability, graph_id))
            .await?;
        return Err(EndpointFailure::detailed("invalid_plan", message));
    }

    let graph_id = context.graph_id;
    let outcome =
        in_blocking(move || record_plan_acceptance(&db_path, &capability, graph_id, &serialized))
            .await?;
    match outcome {
        SubmitOutcome::Accepted => Ok(Response::success(ResponseResult::PlanAccepted { graph_id })),
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
    repo_dir: &Path,
    source_dependency_ids: &[i64],
) -> std::result::Result<(), String> {
    if serialized.len() > super::planner::MAX_RESPONSE_BYTES {
        return Err(format!(
            "plan exceeds {} bytes",
            super::planner::MAX_RESPONSE_BYTES
        ));
    }
    let response: super::planner::PlannerResponse =
        serde_json::from_str(serialized).map_err(|error| format!("invalid plan: {error}"))?;
    super::planner::validate_semantics(&response).map_err(|error| error.to_string())?;
    if let super::planner::PlannerResponse::Plan { tasks } = &response {
        super::planner::validate_for_source(
            tasks,
            source_dependency_ids,
            repo_dir,
            &super::planner::WritablePathResolver::default(),
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn record_plan_rejection(
    db_path: &Path,
    capability: &str,
    graph_id: i64,
) -> std::result::Result<(), EndpointFailure> {
    let mut conn = quorum_core::db::open(db_path)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let tx = begin_immediate(&mut conn)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    planner_submissions::record_rejection(&tx, capability, graph_id)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    tx.commit()
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))
}

/// Record an accepted plan. Authority is re-resolved inside the write
/// transaction because validation ran outside it and the graph may have moved
/// on; the guarded acceptance in `record_accepted` decides the outcome.
fn record_plan_acceptance(
    db_path: &Path,
    capability: &str,
    graph_id: i64,
    serialized: &str,
) -> std::result::Result<SubmitOutcome, EndpointFailure> {
    let mut conn = quorum_core::db::open(db_path)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let tx = begin_immediate(&mut conn)
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    let context = quorum_core::capabilities::resolve_planner_context(&tx, capability)
        .map_err(|_| EndpointFailure::new("unauthorized", "run authority rejected"))?;
    if context.graph_id != graph_id {
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
    tx.commit()
        .map_err(|_| EndpointFailure::new("internal", "request processing failed"))?;
    Ok(outcome)
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
        repo_dir: PathBuf,
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
                repo_dir,
            }
        }

        async fn call(
            &self,
            capability: &str,
            operation: Operation,
        ) -> std::result::Result<Response, EndpointFailure> {
            process_request_with_timeout(
                self.db_path.clone(),
                "test/repo".into(),
                self.repo_dir.clone(),
                Request {
                    version: PROTOCOL_VERSION,
                    capability: capability.into(),
                    operation,
                },
                PROCESS_TIMEOUT,
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

    fn result_value(response: &Response) -> serde_json::Value {
        serde_json::to_value(response).unwrap()
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_plan_rejects_oversized_payload() {
        let fixture = PlannerFixture::new();
        let mut plan = valid_plan();
        plan["tasks"][0]["observable_outcome"] =
            json!("x".repeat(super::super::planner::MAX_RESPONSE_BYTES + 1));

        let error = fixture.submit_plan("planner-cap", plan).await.unwrap_err();
        assert_eq!(error.code, "invalid_plan");
        assert!(
            error.message.contains("exceeds"),
            "unexpected message: {}",
            error.message
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
            },
            Operation::AppendNote {
                task_id: 1,
                agent: "Planner".into(),
                note: "planner should not append".into(),
            },
            Operation::Protocol {
                operation: ProtocolOperation::PullRequestRead,
            },
            Operation::Inventory,
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
            dir.path().to_path_buf(),
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
