//! Credentialless, tools-only MCP adapter for one daemon-managed run.
//!
//! This process has no database or GitHub execution path. Every inventory and
//! known tool call is authorized by the daemon endpoint using only the injected
//! run capability. Tool arguments deliberately stop at this shell until the
//! durable operation slices implement their closed endpoint schemas.

use quorum_core::error::{QuorumError, Result};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ErrorCode,
        Implementation, ListPromptsResult, ListResourceTemplatesResult, ListResourcesResult,
        ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
        ToolAnnotations,
    },
    service::{Peer, RequestContext},
    ErrorData as McpError, RoleServer, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::future::Future;
use std::path::PathBuf;
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

#[derive(Debug, Clone)]
struct GithubServer {
    endpoint: PathBuf,
    capability: String,
    repository: String,
    // `tools/list` is the only inventory a client can cache. Track the phase
    // actually returned there instead of taking an asynchronous watcher
    // baseline, which could miss a transition before its first poll.
    advertised_phase: Arc<Mutex<Option<RunPhase>>>,
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

fn bounded_string(min: usize, max: usize) -> Value {
    json!({"type":"string","minLength":min,"maxLength":max})
}

fn positive_integer() -> Value {
    json!({"type":"integer","minimum":1})
}

fn github_schema(properties: Value, required: &[&str]) -> Arc<Map<String, Value>> {
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

impl GithubServer {
    async fn inventory(&self) -> std::result::Result<Inventory, McpError> {
        let response = self
            .exchange(EndpointOperation::Inventory)
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
        operation: EndpointOperation,
    ) -> std::result::Result<EndpointResult, EndpointFailure> {
        let request = EndpointRequest {
            version: PROTOCOL_VERSION,
            capability: &self.capability,
            operation,
        };
        let body = serde_json::to_vec(&request).map_err(|_| EndpointFailure::Unavailable)?;
        if body.is_empty() || body.len() > MAX_FRAME_BYTES {
            return Err(EndpointFailure::Unavailable);
        }
        let exchange = async {
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

    fn record_advertised_phase(&self, phase: RunPhase) {
        *self
            .advertised_phase
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(phase);
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

impl ServerHandler for GithubServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .build(),
        )
            .with_server_info(Implementation::new("github", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Only tools authorized for this exact managed run are exposed. Workers may write only the assigned worktree and repository.",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, McpError> {
        let inventory = self.inventory().await?;
        self.record_advertised_phase(inventory.phase);
        let tools = inventory
            .operations
            .into_iter()
            .map(ProtocolOperation::tool)
            .collect();
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, McpError> {
        let operation = ProtocolOperation::from_name(&request.name)
            .ok_or_else(|| McpError::new(ErrorCode::METHOD_NOT_FOUND, "unknown tool", None))?;
        match self
            .exchange(EndpointOperation::Protocol { operation })
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
async fn notify_phase_changes(server: GithubServer, peer: Peer<RoleServer>) {
    let mut interval = tokio::time::interval(PHASE_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        interval.tick().await;
        let Ok(inventory) = server.inventory().await else {
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

pub fn run() -> Result<()> {
    let endpoint = required_env("QUORUM_AGENT_ENDPOINT", MAX_ENV_BYTES)?;
    let capability = required_env("QUORUM_RUN_ID", MAX_CAPABILITY_BYTES)?;
    let repository = required_env("QUORUM_REPO", 255)?;
    // Validate the complete managed-run envelope even though agent identity is
    // resolved exclusively by the daemon and never sent as request authority.
    let _agent = required_env("QUORUM_AGENT", 128)?;
    let server = GithubServer {
        endpoint: PathBuf::from(endpoint),
        capability,
        repository,
        advertised_phase: Arc::default(),
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
        let watcher = tokio::spawn(notify_phase_changes(server, service.peer().clone()));
        let result = service
            .waiting()
            .await
            .map(|_| ())
            .map_err(|_| QuorumError::Io("agent MCP service failed".into()));
        watcher.abort();
        result
    })
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
        let server = GithubServer {
            endpoint: PathBuf::from("/tmp/agent-endpoint"),
            capability: "run-capability".into(),
            repository: "owner/repository".into(),
            advertised_phase: Arc::default(),
        };

        // This models the client caching its initial list before the task
        // enters rework, while the watcher has not yet completed any poll.
        server.record_advertised_phase(RunPhase::InitialWorker);
        assert!(server.phase_changed_since_advertisement(RunPhase::ReworkWorker));

        server.record_advertised_phase(RunPhase::ReworkWorker);
        assert!(!server.phase_changed_since_advertisement(RunPhase::ReworkWorker));
    }
}
