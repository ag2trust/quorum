//! Credentialless, tools-only MCP adapter for one daemon-managed run.
//!
//! This process has no database or GitHub execution path. Every inventory and
//! known tool call is authorized by the daemon endpoint using only the injected
//! run capability. Tool arguments deliberately stop at this shell until the
//! durable operation slices implement their closed endpoint schemas.
//!
//! Two transports share this shell. Over stdio it serves the one run whose
//! capability was injected into its environment. With `--listen` it serves
//! loopback Streamable HTTP statelessly: every request names its own run with
//! an `Authorization: Bearer <run id>` header, nothing about a run is kept
//! between requests, and the signalling tools below are advertised.

use crate::agent_client::{SignalOutcome, SubmitPlanOutcome};
use axum::http::{header, request::Parts, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use quorum_core::error::{QuorumError, Result};
use rmcp::{
    model::{
        CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock,
        ErrorCode, Implementation, ListPromptsResult, ListResourceTemplatesResult,
        ListResourcesResult, ListToolsResult, PaginatedRequestParams, ServerCapabilities,
        ServerInfo, Tool, ToolAnnotations,
    },
    service::{Peer, RequestContext},
    transport::streamable_http_server::{
        session::never::NeverSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    },
    ErrorData as McpError, RoleServer, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

const PROTOCOL_VERSION: u8 = 1;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
/// The MCP adapter is a credentialless child of a persistent provider, so it
/// learns daemon phase transitions by asking the endpoint for its derived
/// inventory. This keeps the adapter out of the database while promptly
/// invalidating clients' cached tool lists.
const PHASE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_ENV_BYTES: usize = 4096;
/// The only path the HTTP transport serves.
const HTTP_MCP_PATH: &str = "/mcp";
/// Bound on one HTTP request body. An endpoint request is at most
/// `MAX_FRAME_BYTES`; the allowance covers the JSON-RPC envelope and string
/// escaping around the largest payload the endpoint would accept.
pub(crate) const MAX_HTTP_BODY_BYTES: usize = 4 * MAX_FRAME_BYTES;
/// Open HTTP connections served at once. HTTP/1.1 carries one request per
/// connection at a time, so this is also the bound on concurrent requests.
/// Further connections wait in the kernel's listen backlog. It is kept well
/// under the daemon endpoint's 32 connection slots so this listener cannot
/// starve the CLI path that shares them.
pub(crate) const MAX_HTTP_CONNECTIONS: usize = 8;
/// Endpoint exchanges in flight at once, counted separately from connections
/// because a blocking exchange outlives a client that hung up on it.
const MAX_ENDPOINT_EXCHANGES: usize = MAX_HTTP_CONNECTIONS;
/// A connection must deliver a complete request head within this bound,
/// whether it is new or idle between requests.
const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// One request, from its head to the start of its response, including
/// reading its body and waiting for the daemon endpoint.
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// After SIGINT/SIGTERM, open connections get this long to finish.
const HTTP_SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);

/// Where a request's run capability comes from.
#[derive(Debug, Clone)]
enum Authority {
    /// stdio: the one capability injected into this process's environment.
    Run(String),
    /// HTTP: the bearer token of each request. Nothing is stored.
    Bearer,
}

#[derive(Debug, Clone)]
struct QuorumServer {
    endpoint: PathBuf,
    authority: Authority,
    repository: String,
    // `tools/list` is the only inventory a client can cache. Track the phase
    // actually returned there instead of taking an asynchronous watcher
    // baseline, which could miss a transition before its first poll.
    advertised_phase: Arc<Mutex<Option<RunPhase>>>,
    /// HTTP only: bounds concurrent endpoint exchanges across all requests.
    /// stdio serves one client and keeps its exchanges unbounded as before.
    endpoint_slots: Option<Arc<tokio::sync::Semaphore>>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ProtocolOperation {
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
    const fn name(self) -> &'static str {
        match self {
            Self::PullRequestRead => "pull_request_read",
            Self::AddIssueComment => "add_issue_comment",
            Self::PullRequestReviewWrite => "pull_request_review_write",
            Self::AddCommentToPendingReview => "add_comment_to_pending_review",
            Self::AddReplyToPullRequestComment => "add_reply_to_pull_request_comment",
            Self::ResolveReviewThread => "resolve_review_thread",
            Self::GithubOperationRead => "github_operation_read",
            Self::DeliveryReportWrite => "delivery_report_write",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "pull_request_read" => Some(Self::PullRequestRead),
            "add_issue_comment" => Some(Self::AddIssueComment),
            "pull_request_review_write" => Some(Self::PullRequestReviewWrite),
            "add_comment_to_pending_review" => Some(Self::AddCommentToPendingReview),
            "add_reply_to_pull_request_comment" => Some(Self::AddReplyToPullRequestComment),
            "resolve_review_thread" => Some(Self::ResolveReviewThread),
            "github_operation_read" => Some(Self::GithubOperationRead),
            "delivery_report_write" => Some(Self::DeliveryReportWrite),
            _ => None,
        }
    }

    fn tool(self) -> Tool {
        let (description, schema, read_only) = match self {
            Self::PullRequestRead => (
                "Read the pull request bound to this managed run using an approved read method.",
                github_schema(
                    json!({
                        "method": {"type":"string","enum":["get","get_diff","get_files","get_review_comments","get_reviews","get_comments"]},
                        "owner": bounded_string(1, 100),
                        "repo": bounded_string(1, 100),
                        "pullNumber": positive_integer(),
                        "after": bounded_string(1, 512),
                        "page": {"type":"integer","minimum":1,"maximum":100},
                        "perPage": {"type":"integer","minimum":1,"maximum":100}
                    }),
                    &["method", "owner", "repo", "pullNumber"],
                ),
                true,
            ),
            Self::AddIssueComment => (
                "Add a general Markdown comment to the pull request bound to this managed run.",
                github_schema(
                    json!({
                        "owner": bounded_string(1, 100),
                        "repo": bounded_string(1, 100),
                        "issue_number": positive_integer(),
                        "body": bounded_string(1, 32768),
                        "clientRequestId": bounded_string(1, 128)
                    }),
                    &["owner", "repo", "issue_number", "body"],
                ),
                false,
            ),
            Self::PullRequestReviewWrite => (
                "Create or submit the current reviewer's pending COMMENT review for the bound pull request.",
                pending_review_schema(),
                false,
            ),
            Self::AddCommentToPendingReview => (
                "Add one validated inline Markdown finding to the current pending review.",
                github_schema(
                    json!({
                        "owner": bounded_string(1, 100),
                        "repo": bounded_string(1, 100),
                        "pullNumber": positive_integer(),
                        "path": bounded_string(1, 4096),
                        "line": positive_integer(),
                        "side": {"type":"string","enum":["LEFT","RIGHT"]},
                        "startLine": positive_integer(),
                        "startSide": {"type":"string","enum":["LEFT","RIGHT"]},
                        "subjectType": {"type":"string","enum":["LINE","FILE"]},
                        "body": bounded_string(1, 32768),
                        "clientRequestId": bounded_string(1, 128)
                    }),
                    &["owner", "repo", "pullNumber", "path", "subjectType", "body"],
                ),
                false,
            ),
            Self::AddReplyToPullRequestComment => (
                "Reply in an existing review-comment thread on the bound pull request.",
                github_schema(
                    json!({
                        "owner": bounded_string(1, 100),
                        "repo": bounded_string(1, 100),
                        "pullNumber": positive_integer(),
                        "commentId": positive_integer(),
                        "body": bounded_string(1, 32768),
                        "clientRequestId": bounded_string(1, 128)
                    }),
                    &["owner", "repo", "pullNumber", "commentId", "body"],
                ),
                false,
            ),
            Self::ResolveReviewThread => (
                "Resolve a review thread on the bound pull request as the current reviewer.",
                github_schema(
                    json!({
                        "threadID": bounded_string(1, 256),
                        "clientRequestId": bounded_string(1, 128)
                    }),
                    &["threadID"],
                ),
                false,
            ),
            Self::GithubOperationRead => (
                "Read the bounded status of a GitHub operation owned by this exact managed run.",
                github_schema(
                    json!({"operationId": bounded_string(1, 128)}),
                    &["operationId"],
                ),
                true,
            ),
            Self::DeliveryReportWrite => (
                "Record bounded structured delivery evidence for the current worker run.",
                github_schema(
                    json!({
                        "summary": bounded_string(1, 32768),
                        "changes": {
                            "type":"array","maxItems":64,
                            "items": bounded_string(1, 4096)
                        },
                        "verification": {
                            "type":"array","maxItems":64,
                            "items": {
                                "type":"object","additionalProperties":false,
                                "properties": {
                                    "command": bounded_string(1, 4096),
                                    "outcome": bounded_string(1, 4096)
                                },
                                "required":["command","outcome"]
                            }
                        },
                        "risks_or_notes": {
                            "type":"array","maxItems":64,
                            "items": bounded_string(1, 4096)
                        }
                    }),
                    &["summary", "changes", "verification"],
                ),
                false,
            ),
        };
        Tool::new(self.name(), description, schema).with_annotations(
            ToolAnnotations::new()
                .read_only(read_only)
                .destructive(false)
                .idempotent(false)
                .open_world(!read_only),
        )
    }
}

/// The planner's single tool. It is not a `ProtocolOperation`: it performs no
/// GitHub work and is proxied straight to the daemon endpoint's `SubmitPlan`.
const SUBMIT_PLAN: &str = "submit_plan";

fn submit_plan_tool() -> Tool {
    let description = format!(
        "Submit this planning run's response to the daemon. Call it exactly once with the PLAN \
         or BLOCKER object as `response`. The daemon validates the plan and answers in this \
         turn, so a rejection can be corrected and submitted again immediately; the plan is \
         never reported as text. {shapes} If this call times out or the transport fails, call \
         it again — `already_submitted` means your first plan stands; never resubmit a \
         corrected plan after a transport failure. `request_too_large` means the plan itself \
         must be shrunk; it is not a transport failure.",
        shapes = crate::serve::planner::RESPONSE_SHAPES,
    );
    let schema = json!({
        "type": "object",
        "required": ["response"],
        "properties": {
            "response": {
                "type": "object",
                "description": "Exactly the PLAN or BLOCKER object: {\"outcome\":\"plan\",\"tasks\":[...]} or {\"outcome\":\"blocker\",...}. Unknown fields are rejected."
            }
        }
    });
    Tool::new(
        SUBMIT_PLAN,
        description,
        Arc::new(
            schema
                .as_object()
                .expect("static tool schema is an object")
                .clone(),
        ),
    )
    .with_annotations(
        ToolAnnotations::new()
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            // The daemon endpoint is the only world this tool touches.
            .open_world(false),
    )
}

/// One tool result, always a single JSON text block so the planner reads the
/// daemon's own verdict rather than prose.
pub(crate) fn json_result(value: Value, is_error: bool) -> CallToolResult {
    let content = vec![ContentBlock::text(value.to_string())];
    if is_error {
        CallToolResult::error(content)
    } else {
        CallToolResult::success(content)
    }
}

const SUBMIT_WORK: &str = "submit_work";
const SUBMIT_REVIEW: &str = "submit_review";
const REACT: &str = "react";
const TASK_NOTE: &str = "task_note";
const ALERT_OWNER: &str = "alert_owner";

/// Shared by every signalling tool description. The endpoint appends one row
/// per accepted call and deduplicates nothing, so a retry is never free.
const SIGNAL_RETRY: &str = "Not idempotent: every accepted call records another row, so call it \
     once. A rejection ({\"code\",\"message\"}) recorded nothing and may be corrected and \
     repeated. After `transport` the outcome is unknown and a repeat may record a duplicate.";

/// The signalling tools advertised over HTTP for a run phase. Like
/// `submit_plan` they are not `ProtocolOperation`s; the daemon endpoint
/// re-derives authority from the capability on every call.
fn signal_tools(phase: RunPhase) -> Vec<Tool> {
    match phase {
        RunPhase::Planner => Vec::new(),
        RunPhase::InitialWorker | RunPhase::ReworkWorker => {
            vec![submit_work_tool(), react_tool(), task_note_tool()]
        }
        RunPhase::Reviewer => vec![submit_review_tool(), task_note_tool(), alert_owner_tool()],
    }
}

fn signal_tool(
    name: &'static str,
    description: String,
    properties: Value,
    required: &[&str],
) -> Tool {
    Tool::new(name, description, github_schema(properties, required)).with_annotations(
        ToolAnnotations::new()
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            // The daemon endpoint is the only world these tools touch.
            .open_world(false),
    )
}

fn submit_work_tool() -> Tool {
    signal_tool(
        SUBMIT_WORK,
        format!(
            "Signal that this worker run's delivery (initial or rework) is complete, with an \
             optional summary. The daemon alone decides what happens next. Success is \
             {{\"ok\":true,\"mailbox_id\":N}}. {SIGNAL_RETRY}"
        ),
        json!({"summary": bounded_string(1, 32768)}),
        &[],
    )
}

fn submit_review_tool() -> Tool {
    signal_tool(
        SUBMIT_REVIEW,
        format!(
            "Submit this reviewer run's verdict. `approved` requires `blocking: 0` and takes no \
             feedback. `changes` requires `blocking` >= 1 and non-empty `feedback`. \
             `graph-blocker` takes only `feedback_json` (the graph-blocker feedback object). \
             The daemon re-validates and alone acts on the verdict. Success is \
             {{\"ok\":true,\"mailbox_id\":N}}. {SIGNAL_RETRY}"
        ),
        json!({
            "verdict": {"type":"string","enum":["approved","changes","graph-blocker"]},
            "blocking": {"type":"integer","minimum":0},
            "feedback": bounded_string(1, 32768),
            "feedback_json": {"type":"object"}
        }),
        &["verdict"],
    )
}

fn react_tool() -> Tool {
    signal_tool(
        REACT,
        format!(
            "Signal a worker run's state to the daemon: `blocked`, `failed`, `needs-info`, or \
             `note`. `blocked` and `needs-info` require a non-empty `reason` stating the blocker \
             or the full question. Success is {{\"ok\":true,\"mailbox_id\":N}}. {SIGNAL_RETRY}"
        ),
        json!({
            "state": {"type":"string","enum":["blocked","failed","needs-info","note"]},
            "reason": bounded_string(1, quorum_core::tasks::REACT_REASON_MAX_BYTES)
        }),
        &["state"],
    )
}

fn task_note_tool() -> Tool {
    signal_tool(
        TASK_NOTE,
        format!(
            "Append a progress note to this run's own task, authored as this run's agent. \
             Success is {{\"ok\":true,\"note_id\":N}}. {SIGNAL_RETRY}"
        ),
        json!({"note": bounded_string(1, 32768)}),
        &["note"],
    )
}

fn alert_owner_tool() -> Tool {
    signal_tool(
        ALERT_OWNER,
        format!(
            "Post a critical feed message addressed to the owner, authored as this reviewer \
             run's agent. It changes no task state. Success is {{\"ok\":true,\"seq\":N}}. \
             {SIGNAL_RETRY}"
        ),
        json!({
            "body": bounded_string(1, crate::serve::agent_endpoint::MAX_OWNER_ALERT_BYTES)
        }),
        &["body"],
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitWorkArguments {
    #[serde(default)]
    summary: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitReviewArguments {
    verdict: String,
    #[serde(default)]
    blocking: Option<u32>,
    #[serde(default)]
    feedback: Option<String>,
    #[serde(default)]
    feedback_json: Option<Map<String, Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReactArguments {
    state: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskNoteArguments {
    note: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertOwnerArguments {
    body: String,
}

pub(crate) fn invalid_arguments(message: impl Into<String>) -> CallToolResult {
    json_result(
        json!({"code": "invalid_arguments", "message": message.into()}),
        true,
    )
}

/// Decode a signalling tool's arguments against its closed shape. Absent
/// arguments are an empty object; an unknown or mistyped field is refused here
/// and never reaches the endpoint.
pub(crate) fn signal_arguments<T: serde::de::DeserializeOwned>(
    tool: &str,
    arguments: Option<Map<String, Value>>,
) -> std::result::Result<T, CallToolResult> {
    serde_json::from_value(Value::Object(arguments.unwrap_or_default()))
        .map_err(|error| invalid_arguments(format!("invalid {tool} arguments: {error}")))
}

/// The client-side checks `quorum submit` applies to a reviewer verdict. On
/// success, returns the serialized graph-blocker feedback when there is one.
fn validate_review(
    arguments: &SubmitReviewArguments,
) -> std::result::Result<Option<String>, String> {
    let verdict = arguments.verdict.as_str();
    if !matches!(verdict, "approved" | "changes" | "graph-blocker") {
        return Err("verdict must be 'approved', 'changes', or 'graph-blocker'".into());
    }
    if verdict == "graph-blocker" {
        if arguments.blocking.is_some() || arguments.feedback.is_some() {
            return Err(
                "graph-blocker uses only feedback_json, not ordinary review feedback or blocking"
                    .into(),
            );
        }
        let Some(feedback_json) = arguments.feedback_json.as_ref() else {
            return Err("verdict graph-blocker requires feedback_json".into());
        };
        let raw = Value::Object(feedback_json.clone()).to_string();
        // The shared validator words its errors for the CLI flag.
        crate::graph_blocker::parse_feedback(&raw)
            .map_err(|error| error.replace("--feedback-json", "feedback_json"))?;
        return Ok(Some(raw));
    }
    if arguments.feedback_json.is_some() {
        return Err("feedback_json requires verdict graph-blocker".into());
    }
    if arguments.feedback.is_some() && verdict != "changes" {
        return Err("feedback requires verdict changes".into());
    }
    // The shared validator decides; its own text names CLI flags, so the
    // refusal restates the rule in this tool's terms instead.
    crate::verdict::validate(
        Some(verdict),
        arguments.blocking,
        arguments.feedback.as_deref(),
        true,
    )
    .map_err(|_| {
        "approved requires blocking 0; changes requires blocking >= 1 and non-empty feedback"
            .to_string()
    })?;
    Ok(None)
}

pub(crate) fn bounded_string(min: usize, max: usize) -> Value {
    json!({"type":"string","minLength":min,"maxLength":max})
}

pub(crate) fn positive_integer() -> Value {
    json!({"type":"integer","minimum":1})
}

pub(crate) fn github_schema(properties: Value, required: &[&str]) -> Arc<Map<String, Value>> {
    let value = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": required,
    });
    Arc::new(
        value
            .as_object()
            .expect("static tool schema is an object")
            .clone(),
    )
}

fn pending_review_schema() -> Arc<Map<String, Value>> {
    let mut schema = github_schema(
        json!({
            "method": {"type":"string","enum":["create","submit_pending"]},
            "owner": bounded_string(1, 100),
            "repo": bounded_string(1, 100),
            "pullNumber": positive_integer(),
            "commitID": bounded_string(40, 40),
            "event": {"type":"string","enum":["COMMENT"]},
            "body": bounded_string(1, 32768),
            "clientRequestId": bounded_string(1, 128)
        }),
        &["method", "owner", "repo", "pullNumber"],
    );
    Arc::make_mut(&mut schema).insert(
        "oneOf".into(),
        json!([
            {
                "properties": {"method": {"const": "create"}},
                "required": ["method", "commitID"]
            },
            {
                "properties": {"method": {"const": "submit_pending"}},
                "required": ["method", "event", "body"]
            }
        ]),
    );
    schema
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct EndpointRequest<'a> {
    version: u8,
    capability: &'a str,
    operation: EndpointOperation,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EndpointOperation {
    Inventory,
    Protocol { operation: ProtocolOperation },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointResponse {
    version: u8,
    ok: bool,
    #[serde(default)]
    result: Option<EndpointResult>,
    #[serde(default)]
    error: Option<EndpointError>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum EndpointResult {
    Inventory {
        repository: String,
        task_id: i64,
        role: String,
        pr: Option<i64>,
        review_revision: Option<String>,
        phase: RunPhase,
        operations: Vec<ProtocolOperation>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointError {
    code: String,
    message: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum RunPhase {
    InitialWorker,
    ReworkWorker,
    Reviewer,
    Planner,
}

#[derive(Debug)]
struct Inventory {
    phase: RunPhase,
    operations: Vec<ProtocolOperation>,
}

#[derive(Debug)]
enum EndpointFailure {
    Rejected(String),
    Unavailable,
}

impl QuorumServer {
    /// The run capability authorizing one request.
    ///
    /// Over HTTP it is read from that request's own `Authorization` header on
    /// every call and never retained, so two runs sharing one listener cannot
    /// observe or borrow each other's authority.
    fn capability(
        &self,
        context: &RequestContext<RoleServer>,
    ) -> std::result::Result<String, McpError> {
        match &self.authority {
            Authority::Run(capability) => Ok(capability.clone()),
            Authority::Bearer => context
                .extensions
                .get::<Parts>()
                .and_then(|parts| bearer_capability(&parts.headers))
                .map(str::to_owned)
                .ok_or_else(|| McpError::invalid_request("run authority rejected", None)),
        }
    }

    /// Wait for one of the bounded endpoint exchange slots. The permit is
    /// owned so blocking work can carry it until that work really ends.
    async fn endpoint_slot(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        match &self.endpoint_slots {
            // The semaphore is never closed, so acquisition cannot fail.
            Some(slots) => Arc::clone(slots).acquire_owned().await.ok(),
            None => None,
        }
    }

    fn is_http(&self) -> bool {
        matches!(self.authority, Authority::Bearer)
    }

    async fn inventory(&self, capability: &str) -> std::result::Result<Inventory, McpError> {
        let response = self
            .exchange(capability, EndpointOperation::Inventory)
            .await
            .map_err(inventory_error)?;
        let EndpointResult::Inventory {
            repository,
            task_id,
            role,
            pr,
            review_revision,
            phase,
            operations,
        } = response;
        if repository != self.repository
            || repository.len() > 255
            || task_id <= 0
            || role.len() > 16
            || review_revision
                .as_ref()
                .is_some_and(|value| value.len() > 128)
        {
            return Err(McpError::internal_error(
                "agent endpoint returned invalid inventory",
                None,
            ));
        }
        let expected = expected_inventory(phase);
        let target_valid = match phase {
            RunPhase::Planner => role == "planner" && pr.is_none() && review_revision.is_none(),
            RunPhase::InitialWorker => {
                role == "worker" && pr.is_none() && review_revision.is_none()
            }
            RunPhase::ReworkWorker => {
                role == "worker" && pr.is_some_and(|value| value > 0) && review_revision.is_none()
            }
            RunPhase::Reviewer => {
                role == "reviewer"
                    && pr.is_some_and(|value| value > 0)
                    && review_revision
                        .as_ref()
                        .is_some_and(|value| !value.is_empty())
            }
        };
        if !target_valid || operations != expected {
            return Err(McpError::internal_error(
                "agent endpoint returned invalid inventory",
                None,
            ));
        }
        Ok(Inventory { phase, operations })
    }

    async fn exchange(
        &self,
        capability: &str,
        operation: EndpointOperation,
    ) -> std::result::Result<EndpointResult, EndpointFailure> {
        let request = EndpointRequest {
            version: PROTOCOL_VERSION,
            capability,
            operation,
        };
        let body = serde_json::to_vec(&request).map_err(|_| EndpointFailure::Unavailable)?;
        if body.is_empty() || body.len() > MAX_FRAME_BYTES {
            return Err(EndpointFailure::Unavailable);
        }
        let exchange = async {
            let _slot = self.endpoint_slot().await;
            let mut stream = UnixStream::connect(&self.endpoint).await?;
            stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
            stream.write_all(&body).await?;
            let mut prefix = [0_u8; 4];
            stream.read_exact(&mut prefix).await?;
            let length = u32::from_be_bytes(prefix) as usize;
            if length == 0 || length > MAX_FRAME_BYTES {
                return Err(std::io::Error::other("invalid response frame"));
            }
            let mut response_body = vec![0; length];
            stream.read_exact(&mut response_body).await?;
            Ok::<_, std::io::Error>(response_body)
        };
        let response_body = tokio::time::timeout(IO_TIMEOUT, exchange)
            .await
            .map_err(|_| EndpointFailure::Unavailable)?
            .map_err(|_| EndpointFailure::Unavailable)?;
        let response: EndpointResponse =
            serde_json::from_slice(&response_body).map_err(|_| EndpointFailure::Unavailable)?;
        if response.version != PROTOCOL_VERSION {
            return Err(EndpointFailure::Unavailable);
        }
        match (response.ok, response.result, response.error) {
            (true, Some(result), None) => Ok(result),
            (false, None, Some(error))
                if !error.code.is_empty()
                    && error.code.len() <= 64
                    && !error.message.is_empty()
                    && error.message.len() <= 256 =>
            {
                Err(EndpointFailure::Rejected(error.code))
            }
            _ => Err(EndpointFailure::Unavailable),
        }
    }

    /// Proxy one plan submission to the daemon endpoint.
    ///
    /// The endpoint owns every validation decision, so its own rejection code
    /// and message are relayed verbatim and the planner corrects the cited
    /// defect within the same turn. Acceptance is one atomic guarded write, so
    /// a transport failure leaves the outcome unknown rather than failed; the
    /// tool description tells the planner to repeat the identical call, which
    /// answers `already_submitted` when the first plan already landed.
    ///
    /// A plan too large for one endpoint request is refused by the client
    /// before it connects, so nothing was submitted and repeating the identical
    /// call cannot succeed. That case is reported as `request_too_large` — the
    /// one failure whose remedy is a smaller plan rather than a retry.
    async fn submit_plan(
        &self,
        capability: String,
        arguments: Option<Map<String, Value>>,
    ) -> CallToolResult {
        let Some(response) = arguments
            .as_ref()
            .and_then(|arguments| arguments.get("response"))
            .filter(|response| response.is_object())
            .cloned()
        else {
            return json_result(
                json!({
                    "code": "invalid_arguments",
                    "message": "submit_plan requires a `response` object"
                }),
                true,
            );
        };
        let endpoint = self.endpoint.clone();
        let slot = self.endpoint_slot().await;
        match tokio::task::spawn_blocking(move || {
            let _slot = slot;
            crate::agent_client::submit_plan(&endpoint, &capability, &response)
        })
        .await
        {
            Ok(Ok(SubmitPlanOutcome::Accepted { graph_id })) => {
                json_result(json!({"accepted": true, "graph_id": graph_id}), false)
            }
            Ok(Ok(SubmitPlanOutcome::Rejected { code, message })) => {
                json_result(json!({"code": code, "message": message}), true)
            }
            // The client rejects an oversized frame before it connects, so the
            // plan never reached the daemon and a retry would fail identically.
            Ok(Err(QuorumError::Usage(_))) => json_result(
                json!({
                    "code": "request_too_large",
                    "message": "the plan does not fit one endpoint request; submit a smaller plan"
                }),
                true,
            ),
            Ok(Err(_)) | Err(_) => json_result(
                json!({
                    "code": "transport",
                    "message": "the agent endpoint did not answer; call submit_plan again to \
                                learn whether the plan was accepted"
                }),
                true,
            ),
        }
    }

    /// Route one HTTP signalling tool, or `None` when `name` is not one.
    ///
    /// Arguments are checked against the tool's closed shape and the same
    /// rules the CLI applies; the daemon endpoint then re-validates and is the
    /// only authority on whether this run may send the signal at all.
    async fn call_signal(
        &self,
        name: &str,
        capability: String,
        arguments: Option<Map<String, Value>>,
    ) -> Option<CallToolResult> {
        Some(match name {
            SUBMIT_WORK => {
                let arguments: SubmitWorkArguments = match signal_arguments(name, arguments) {
                    Ok(arguments) => arguments,
                    Err(refusal) => return Some(refusal),
                };
                self.signal("mailbox_id", move |endpoint| {
                    crate::agent_client::submit_at(
                        endpoint,
                        crate::agent_client::Submit {
                            capability: &capability,
                            summary: arguments.summary.as_deref(),
                            verdict: None,
                            feedback: None,
                            feedback_json: None,
                            blocking: None,
                        },
                    )
                })
                .await
            }
            SUBMIT_REVIEW => {
                let arguments: SubmitReviewArguments = match signal_arguments(name, arguments) {
                    Ok(arguments) => arguments,
                    Err(refusal) => return Some(refusal),
                };
                let feedback_json = match validate_review(&arguments) {
                    Ok(feedback_json) => feedback_json,
                    Err(message) => return Some(invalid_arguments(message)),
                };
                self.signal("mailbox_id", move |endpoint| {
                    crate::agent_client::submit_at(
                        endpoint,
                        crate::agent_client::Submit {
                            capability: &capability,
                            summary: None,
                            verdict: Some(&arguments.verdict),
                            feedback: arguments.feedback.as_deref(),
                            feedback_json: feedback_json.as_deref(),
                            blocking: arguments.blocking,
                        },
                    )
                })
                .await
            }
            REACT => {
                let arguments: ReactArguments = match signal_arguments(name, arguments) {
                    Ok(arguments) => arguments,
                    Err(refusal) => return Some(refusal),
                };
                if !matches!(
                    arguments.state.as_str(),
                    "blocked" | "failed" | "needs-info" | "note"
                ) {
                    return Some(invalid_arguments(
                        "state must be 'blocked', 'failed', 'needs-info', or 'note'",
                    ));
                }
                let Ok(reason) = quorum_core::tasks::validate_react_reason(
                    &arguments.state,
                    arguments.reason.as_deref(),
                ) else {
                    return Some(invalid_arguments(format!(
                        "blocked and needs-info require a non-empty reason; a reason is at \
                         most {} bytes with no NUL",
                        quorum_core::tasks::REACT_REASON_MAX_BYTES
                    )));
                };
                self.signal("mailbox_id", move |endpoint| {
                    crate::agent_client::react_at(
                        endpoint,
                        &capability,
                        &arguments.state,
                        reason.as_deref(),
                    )
                })
                .await
            }
            TASK_NOTE => {
                let arguments: TaskNoteArguments = match signal_arguments(name, arguments) {
                    Ok(arguments) => arguments,
                    Err(refusal) => return Some(refusal),
                };
                self.signal("note_id", move |endpoint| {
                    crate::agent_client::append_note_at(endpoint, &capability, &arguments.note)
                })
                .await
            }
            ALERT_OWNER => {
                let arguments: AlertOwnerArguments = match signal_arguments(name, arguments) {
                    Ok(arguments) => arguments,
                    Err(refusal) => return Some(refusal),
                };
                self.signal("seq", move |endpoint| {
                    crate::agent_client::alert_owner_at(endpoint, &capability, &arguments.body)
                })
                .await
            }
            _ => return None,
        })
    }

    /// Send one signalling operation and shape its result. `id_key` names the
    /// row the daemon wrote. A daemon rejection is relayed verbatim; a request
    /// too large for one endpoint frame never left this process.
    async fn signal<F>(&self, id_key: &'static str, send: F) -> CallToolResult
    where
        F: FnOnce(&Path) -> Result<SignalOutcome> + Send + 'static,
    {
        let endpoint = self.endpoint.clone();
        let slot = self.endpoint_slot().await;
        let sent = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            send(&endpoint)
        });
        match sent.await {
            Ok(Ok(SignalOutcome::Accepted(id))) => {
                let mut result = Map::new();
                result.insert("ok".into(), Value::Bool(true));
                result.insert(id_key.into(), Value::from(id));
                json_result(Value::Object(result), false)
            }
            Ok(Ok(SignalOutcome::Rejected { code, message })) => {
                json_result(json!({"code": code, "message": message}), true)
            }
            Ok(Err(QuorumError::Usage(_))) => json_result(
                json!({
                    "code": "request_too_large",
                    "message": "the arguments do not fit one endpoint request; send less text"
                }),
                true,
            ),
            Ok(Err(_)) | Err(_) => json_result(
                json!({
                    "code": "transport",
                    "message": "the agent endpoint did not answer; whether the signal was \
                                recorded is unknown"
                }),
                true,
            ),
        }
    }

    fn record_advertised_phase(&self, phase: RunPhase) {
        *self
            .advertised_phase
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(phase);
    }

    /// Whether `submit_plan` belongs to the inventory this client last received.
    ///
    /// The tool is advertised to a planner only, so a run that has been told
    /// any other phase must see the name as unknown, exactly like any tool
    /// outside its inventory. A run that has not listed tools yet is not
    /// refused here: the endpoint authorizes the call itself, and a cache this
    /// shell has never filled must not take away a planner's only door.
    fn planner_tool_advertised(&self) -> bool {
        matches!(
            *self
                .advertised_phase
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            None | Some(RunPhase::Planner)
        )
    }

    fn phase_changed_since_advertisement(&self, phase: RunPhase) -> bool {
        self.advertised_phase
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some_and(|advertised| advertised != phase)
    }
}

fn expected_inventory(phase: RunPhase) -> Vec<ProtocolOperation> {
    use ProtocolOperation::*;
    match phase {
        // The planner's only tool is `submit_plan`, which is not a
        // `ProtocolOperation`: it performs no GitHub work.
        RunPhase::Planner => Vec::new(),
        RunPhase::InitialWorker => vec![DeliveryReportWrite],
        RunPhase::ReworkWorker => vec![
            PullRequestRead,
            AddIssueComment,
            AddReplyToPullRequestComment,
            GithubOperationRead,
            DeliveryReportWrite,
        ],
        RunPhase::Reviewer => vec![
            PullRequestRead,
            AddIssueComment,
            PullRequestReviewWrite,
            AddCommentToPendingReview,
            AddReplyToPullRequestComment,
            ResolveReviewThread,
            GithubOperationRead,
        ],
    }
}

fn inventory_error(error: EndpointFailure) -> McpError {
    match error {
        EndpointFailure::Rejected(code) if code == "unauthorized" => {
            McpError::invalid_request("run authority rejected", None)
        }
        EndpointFailure::Rejected(_) => McpError::invalid_request("tool inventory rejected", None),
        EndpointFailure::Unavailable => {
            McpError::internal_error("agent endpoint unavailable", None)
        }
    }
}

fn tool_error(code: &str) -> CallToolResult {
    let message = match code {
        "unauthorized" => "run authority rejected",
        "forbidden_operation" => "tool is forbidden for the current run phase",
        "operation_unavailable" => "tool execution is not available in this delivery",
        "invalid_operation" => "tool request rejected",
        _ => "agent endpoint rejected the tool request",
    };
    CallToolResult::error(vec![ContentBlock::text(message)])
}

impl ServerHandler for QuorumServer {
    fn get_info(&self) -> ServerInfo {
        let tools = ServerCapabilities::builder().enable_tools();
        // A stateless HTTP request has no channel for a later notification,
        // so that transport must not promise `tools/list_changed`.
        let capabilities = if self.is_http() {
            tools.build()
        } else {
            tools.enable_tool_list_changed().build()
        };
        ServerInfo::new(capabilities)
            .with_server_info(Implementation::new("quorum", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Only tools authorized for this exact managed run are exposed. Workers may write only the assigned worktree and repository.",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, McpError> {
        let capability = self.capability(&context)?;
        let inventory = self.inventory(&capability).await?;
        let phase = inventory.phase;
        // The advertised phase is per-run state, which only the single-run
        // stdio transport keeps.
        if !self.is_http() {
            self.record_advertised_phase(phase);
        }
        let mut tools: Vec<Tool> = inventory
            .operations
            .into_iter()
            .map(ProtocolOperation::tool)
            .collect();
        if phase == RunPhase::Planner {
            tools.push(submit_plan_tool());
        }
        if self.is_http() {
            tools.extend(signal_tools(phase));
        }
        // MCP 2026-07-28 requires `ttlMs` and `cacheScope` on list results;
        // rmcp leaves both unset, and Claude Code then rejects the whole
        // inventory (the planner loses `submit_plan`). The inventory is
        // per-run and phase-derived, so it is never fresh and never shared.
        Ok(ListToolsResult::with_all_items(tools)
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, McpError> {
        let capability = self.capability(&context)?;
        if self.is_http() {
            // No per-run state exists to consult here: the daemon endpoint
            // authorizes each of these calls from the capability alone.
            if request.name == SUBMIT_PLAN {
                return Ok(self.submit_plan(capability, request.arguments).await.into());
            }
            if let Some(result) = self
                .call_signal(&request.name, capability.clone(), request.arguments.clone())
                .await
            {
                return Ok(result.into());
            }
        } else if request.name == SUBMIT_PLAN && self.planner_tool_advertised() {
            return Ok(self.submit_plan(capability, request.arguments).await.into());
        }
        let operation = ProtocolOperation::from_name(&request.name)
            .ok_or_else(|| McpError::new(ErrorCode::METHOD_NOT_FOUND, "unknown tool", None))?;
        match self
            .exchange(&capability, EndpointOperation::Protocol { operation })
            .await
        {
            Ok(_) => Err(McpError::internal_error(
                "agent endpoint returned invalid tool response",
                None,
            )),
            Err(EndpointFailure::Rejected(code)) => Ok(tool_error(&code).into()),
            Err(EndpointFailure::Unavailable) => {
                Err(McpError::internal_error("agent endpoint unavailable", None))
            }
        }
    }

    fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListPromptsResult, McpError>> + Send + '_ {
        std::future::ready(Err(McpError::new(
            ErrorCode::METHOD_NOT_FOUND,
            "method not found",
            None,
        )))
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListResourcesResult, McpError>> + Send + '_ {
        std::future::ready(Err(McpError::new(
            ErrorCode::METHOD_NOT_FOUND,
            "method not found",
            None,
        )))
    }

    fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListResourceTemplatesResult, McpError>> + Send + '_
    {
        std::future::ready(Err(McpError::new(
            ErrorCode::METHOD_NOT_FOUND,
            "method not found",
            None,
        )))
    }
}

/// Keep a persistent provider's cached MCP tool list aligned with the
/// daemon-derived phase. The endpoint remains the authority for every call;
/// this only tells clients to rediscover the changed inventory.
async fn notify_phase_changes(server: QuorumServer, capability: String, peer: Peer<RoleServer>) {
    let mut interval = tokio::time::interval(PHASE_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        interval.tick().await;
        let Ok(inventory) = server.inventory(&capability).await else {
            continue;
        };
        if server.phase_changed_since_advertisement(inventory.phase) {
            if peer.notify_tool_list_changed().await.is_err() {
                return;
            }
            server.record_advertised_phase(inventory.phase);
        }
    }
}

pub fn run(listen: Option<SocketAddr>) -> Result<()> {
    match listen {
        Some(addr) => run_http(addr),
        None => run_stdio(),
    }
}

fn run_stdio() -> Result<()> {
    let endpoint = required_env("QUORUM_AGENT_ENDPOINT", MAX_ENV_BYTES)?;
    let capability = required_env("QUORUM_RUN_ID", MAX_CAPABILITY_BYTES)?;
    let repository = required_env("QUORUM_REPO", 255)?;
    // Validate the complete managed-run envelope even though agent identity is
    // resolved exclusively by the daemon and never sent as request authority.
    let _agent = required_env("QUORUM_AGENT", 128)?;
    let server = QuorumServer {
        endpoint: PathBuf::from(endpoint),
        authority: Authority::Run(capability.clone()),
        repository,
        advertised_phase: Arc::default(),
        endpoint_slots: None,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| QuorumError::Io("failed to start agent MCP runtime".into()))?;
    runtime.block_on(async move {
        let service = server
            .clone()
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|_| QuorumError::Io("agent MCP initialization failed".into()))?;
        let watcher = tokio::spawn(notify_phase_changes(
            server,
            capability,
            service.peer().clone(),
        ));
        let result = service
            .waiting()
            .await
            .map(|_| ())
            .map_err(|_| QuorumError::Io("agent MCP service failed".into()));
        watcher.abort();
        result
    })
}

/// Serve Streamable HTTP MCP on a loopback address until SIGINT or SIGTERM.
///
/// The listener holds no run: it needs only the daemon endpoint locator and
/// the repository identity, and deliberately never reads `QUORUM_RUN_ID`.
fn run_http(addr: SocketAddr) -> Result<()> {
    validate_loopback(addr)?;
    let endpoint = required_env("QUORUM_AGENT_ENDPOINT", MAX_ENV_BYTES)?;
    let repository = required_env("QUORUM_REPO", 255)?;
    let server = QuorumServer {
        endpoint: PathBuf::from(endpoint),
        authority: Authority::Bearer,
        repository,
        advertised_phase: Arc::default(),
        endpoint_slots: Some(Arc::new(tokio::sync::Semaphore::new(
            MAX_ENDPOINT_EXCHANGES,
        ))),
    };
    // The default allowed hosts are the loopback names.
    serve_http(
        HttpListener {
            command: "agent-mcp",
            title: "agent MCP",
            max_connections: MAX_HTTP_CONNECTIONS,
            header_read_timeout: HTTP_HEADER_READ_TIMEOUT,
            max_admitted_requests: None,
        },
        addr,
        stateless_http_config(),
        server,
        |headers: &HeaderMap| match bearer_capability(headers) {
            Some(_) => Ok(()),
            None => Err(StatusCode::UNAUTHORIZED),
        },
    )
}

/// Stateless Streamable HTTP: no sessions, no server-initiated stream, one
/// bounded JSON body per request.
pub(crate) fn stateless_http_config() -> StreamableHttpServerConfig {
    StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_max_request_body_bytes(MAX_HTTP_BODY_BYTES)
}

/// What distinguishes one HTTP listener from another. The request timeout,
/// body cap, shutdown drain, and per-request service are shared.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HttpListener {
    /// Subcommand name, printed in the listening line.
    pub command: &'static str,
    /// Name used in startup errors.
    pub title: &'static str,
    /// Open connections served at once; further ones wait in the backlog.
    pub max_connections: usize,
    /// A connection must deliver a complete request head within this bound,
    /// whether it is new or idle between requests.
    pub header_read_timeout: Duration,
    /// `None`: a connection slot is the only admission bound. `Some(n)`: at
    /// most `n` requests the gate admitted are in flight at once, and a
    /// request the gate refuses also closes its connection, so a peer that
    /// cannot pass the gate holds a connection slot only until its refusal
    /// or the head timeout and never holds a request slot.
    pub max_admitted_requests: Option<usize>,
}

/// Serve one tools-only MCP server over Streamable HTTP at `/mcp` until
/// SIGINT or SIGTERM, with this module's time and body bounds.
///
/// `gate` sees every request's headers before MCP runs and before the body
/// is read; the status it returns refuses the request.
pub(crate) fn serve_http<S, G>(
    listener_bounds: HttpListener,
    addr: SocketAddr,
    config: StreamableHttpServerConfig,
    server: S,
    gate: G,
) -> Result<()>
where
    S: ServerHandler + Clone + Send + Sync + 'static,
    G: Fn(&HeaderMap) -> std::result::Result<(), StatusCode> + Clone + Send + Sync + 'static,
{
    let HttpListener { command, title, .. } = listener_bounds;
    let admitted = listener_bounds
        .max_admitted_requests
        .map(|requests| Arc::new(tokio::sync::Semaphore::new(requests)));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| QuorumError::Io(format!("failed to start {title} runtime")))?;
    runtime.block_on(async move {
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .map_err(|e| QuorumError::Io(format!("failed to register SIGINT handler: {e}")))?;
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| QuorumError::Io(format!("failed to register SIGTERM handler: {e}")))?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| QuorumError::Io(format!("failed to bind {title} listener: {e}")))?;
        let local = listener
            .local_addr()
            .map_err(|e| QuorumError::Io(format!("failed to bind {title} listener: {e}")))?;
        let cancel = config.cancellation_token.clone();
        let app = axum::Router::new()
            .route(
                HTTP_MCP_PATH,
                axum::routing::any(move |request: axum::extract::Request| {
                    // A fresh rmcp service per request. rmcp caches one tool
                    // schema entry per distinct `tools/call` name for the
                    // life of a service and never evicts, so a shared service
                    // would let any caller grow memory without bound. Nothing
                    // else in a stateless service outlives a request.
                    let service = StreamableHttpService::new(
                        {
                            let server = server.clone();
                            move || Ok(server.clone())
                        },
                        Arc::new(NeverSessionManager::default()),
                        config.clone(),
                    );
                    async move { service.handle(request).await }
                }),
            )
            .layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let gate = gate.clone();
                    let admitted = admitted.clone();
                    async move { bound_request(gate, admitted, request, next).await }
                },
            ));
        // The one line a launcher needs to learn a port-0 bind.
        eprintln!("quorum {command} listening on http://{local}{HTTP_MCP_PATH}");

        let slots = Arc::new(tokio::sync::Semaphore::new(listener_bounds.max_connections));
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                // Signals first, then reaping, so finished connections never
                // accumulate behind a steady stream of new ones.
                biased;
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
                accepted = accept_with_slot(&listener, &slots) => {
                    if let Some((stream, slot)) = accepted {
                        connections.spawn(serve_http_connection(
                            stream,
                            listener_bounds.header_read_timeout,
                            app.clone(),
                            shutdown.subscribe(),
                            slot,
                        ));
                    }
                }
            }
        }
        // Signals only start shutdown. Stop accepting, tell rmcp and every
        // connection to finish, and wait a bounded time. A connection still
        // open after that (a half-sent request, a stalled peer) is dropped:
        // an operation already in flight is one atomic operation whose
        // outcome its owner alone decides.
        drop(listener);
        cancel.cancel();
        let _ = shutdown.send(true);
        let _ = tokio::time::timeout(HTTP_SHUTDOWN_DRAIN, async {
            while connections.join_next().await.is_some() {}
        })
        .await;
        connections.shutdown().await;
        Ok(())
    })
}

/// Accept one connection, but only once a connection slot is free, so the
/// excess waits in the kernel's bounded listen backlog rather than in memory.
/// Cancel-safe: a dropped call releases its slot and leaves the backlog alone.
async fn accept_with_slot(
    listener: &tokio::net::TcpListener,
    slots: &Arc<tokio::sync::Semaphore>,
) -> Option<(tokio::net::TcpStream, tokio::sync::OwnedSemaphorePermit)> {
    // The semaphore is never closed, so acquisition cannot fail.
    let slot = Arc::clone(slots).acquire_owned().await.ok()?;
    match listener.accept().await {
        Ok((stream, _)) => Some((stream, slot)),
        Err(_) => {
            // Usually transient (the peer reset, or descriptors ran out).
            // Pause so a persistent failure cannot spin this loop.
            tokio::time::sleep(Duration::from_millis(50)).await;
            None
        }
    }
}

/// Serve one HTTP/1.1 connection until it closes, its request head times
/// out, or shutdown is signalled. `_slot` is held for the connection's life.
async fn serve_http_connection(
    stream: tokio::net::TcpStream,
    header_read_timeout: Duration,
    app: axum::Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    _slot: tokio::sync::OwnedSemaphorePermit,
) {
    let connection = hyper::server::conn::http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(header_read_timeout)
        .serve_connection(
            hyper_util::rt::TokioIo::new(stream),
            hyper_util::service::TowerToHyperService::new(app),
        );
    tokio::pin!(connection);
    tokio::select! {
        _ = connection.as_mut() => {}
        _ = shutdown.changed() => {
            connection.as_mut().graceful_shutdown();
            let _ = connection.await;
        }
    }
}

fn validate_loopback(addr: SocketAddr) -> Result<()> {
    if addr.ip().is_loopback() {
        return Ok(());
    }
    Err(QuorumError::Usage(
        "agent-mcp --listen is loopback-only; it serves plain HTTP authorized by run capability"
            .into(),
    ))
}

/// The run capability carried by exactly one well-formed
/// `Authorization: Bearer <run id>` header, or `None`. The shape rules are the
/// endpoint's own plus no whitespace, so a malformed credential is refused
/// here without being forwarded, stored, or echoed.
fn bearer_capability(headers: &HeaderMap) -> Option<&str> {
    bearer_token(headers, MAX_CAPABILITY_BYTES)
}

/// The credential of exactly one well-formed `Authorization: Bearer <token>`
/// header of at most `max_bytes`, or `None`.
pub(crate) fn bearer_token(headers: &HeaderMap, max_bytes: usize) -> Option<&str> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let (scheme, token) = value.to_str().ok()?.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.len() > max_bytes
        || token
            .chars()
            .any(|character| character == '\0' || character.is_whitespace())
    {
        return None;
    }
    Some(token)
}

/// Refuse a request the gate rejects before it reaches MCP, and bound how
/// long an admitted one may take to produce its response. The bound covers
/// waiting for a request slot and reading the body, which rmcp does inside
/// the service.
async fn bound_request<G>(
    gate: G,
    admitted: Option<Arc<tokio::sync::Semaphore>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response
where
    G: Fn(&HeaderMap) -> std::result::Result<(), StatusCode>,
{
    if let Err(status) = gate(request.headers()) {
        let mut response = if status == StatusCode::UNAUTHORIZED {
            (
                status,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                "Unauthorized",
            )
                .into_response()
        } else {
            (status, status.canonical_reason().unwrap_or("Refused")).into_response()
        };
        // Its body is never read. Where admitted requests are counted, the
        // connection is closed too, so a refused peer keeps nothing open.
        if admitted.is_some() {
            response.headers_mut().insert(
                header::CONNECTION,
                axum::http::HeaderValue::from_static("close"),
            );
        }
        return response;
    }
    let admitted_request = async {
        // The semaphore is never closed, so acquisition cannot fail.
        let _slot = match admitted {
            Some(slots) => slots.acquire_owned().await.ok(),
            None => None,
        };
        next.run(request).await
    };
    match tokio::time::timeout(HTTP_REQUEST_TIMEOUT, admitted_request).await {
        Ok(response) => response,
        // Dropping the request future cancels its handler. An operation
        // already sent is not undone, so its outcome is unknown.
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            [(header::CONNECTION, "close")],
            "Request Timeout",
        )
            .into_response(),
    }
}

fn required_env(name: &str, max_bytes: usize) -> Result<String> {
    let value = std::env::var(name)
        .map_err(|_| QuorumError::Usage(format!("agent-mcp requires {name}")))?;
    if value.is_empty() || value.len() > max_bytes || value.contains('\0') {
        return Err(QuorumError::Usage(format!(
            "agent-mcp received invalid {name}"
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delayed_watcher_snapshot_detects_change_after_tools_list() {
        let server = QuorumServer {
            endpoint: PathBuf::from("/tmp/agent-endpoint"),
            authority: Authority::Run("run-capability".into()),
            repository: "owner/repository".into(),
            advertised_phase: Arc::default(),
            endpoint_slots: None,
        };

        // This models the client caching its initial list before the task
        // enters rework, while the watcher has not yet completed any poll.
        server.record_advertised_phase(RunPhase::InitialWorker);
        assert!(server.phase_changed_since_advertisement(RunPhase::ReworkWorker));

        server.record_advertised_phase(RunPhase::ReworkWorker);
        assert!(!server.phase_changed_since_advertisement(RunPhase::ReworkWorker));
    }
}
