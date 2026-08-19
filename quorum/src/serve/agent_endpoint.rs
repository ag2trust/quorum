//! Bounded, daemon-owned IPC for managed agent runs.
//!
//! The endpoint is deliberately not a general daemon RPC surface. One Unix
//! socket accepts one length-prefixed JSON request per connection, resolves
//! all authority from the supplied run capability, and exposes only the
//! closed operations below. Repository identity is daemon configuration.

use quorum_core::capabilities::{LiveRunContext, LiveRunPhase};
use quorum_core::db::begin_immediate;
use quorum_core::error::{QuorumError, Result};
use rusqlite::{params, Transaction};
use serde::{Deserialize, Serialize};
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
    Mailbox {
        mailbox_id: i64,
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
    message: &'static str,
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

    fn failure(code: &'static str, message: &'static str) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            ok: false,
            result: None,
            error: Some(ResponseError { code, message }),
        }
    }
}

struct EndpointFailure {
    code: &'static str,
    message: &'static str,
}

impl EndpointFailure {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
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
    pub async fn start(db_path: &Path, repository: &str) -> Result<Self> {
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
        let task = tokio::spawn(run_listener(listener, db_path, repository, shutdown_rx));
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

async fn serve_connection(mut stream: UnixStream, db_path: PathBuf, repository: String) {
    let response = match read_request(&mut stream).await {
        Ok(request) => match process_request(db_path, repository, request).await {
            Ok(response) => response,
            Err(error) => {
                super::log(error.code);
                error.response()
            }
        },
        Err(error) => {
            super::log(error.code);
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
    request: Request,
) -> std::result::Result<Response, EndpointFailure> {
    process_request_with_timeout(db_path, repository, request, PROCESS_TIMEOUT).await
}

async fn process_request_with_timeout(
    db_path: PathBuf,
    repository: String,
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
    use std::sync::mpsc;

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
