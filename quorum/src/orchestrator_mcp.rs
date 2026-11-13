//! Tools-only MCP server for an outside coordinating agent.
//!
//! `quorum orchestrator-mcp --listen <SOCKETADDR>` serves Streamable HTTP MCP
//! at `/mcp` and exposes the coordinator subset of the CLI for one repository:
//! create, list, inspect, cancel, and retry tasks, and message a managed
//! agent. Every tool runs the operation its CLI command runs, as one
//! short-lived SQLite connection per call, acting as the single coordinator
//! identity the operator bound at startup. Nothing here transitions
//! lifecycle, talks to GitHub, or controls the daemon process.
//!
//! It is a separate listener from the managed-agent adapter and shares no
//! authority with it: the only credential it accepts is the operator's bearer
//! token, a run capability is never consulted, and none of the adapter's tools
//! exist here. The HTTP listener, its bounds, and the tool-result conventions
//! are the adapter's own (`agent_mcp`).

use crate::agent_mcp::{
    bearer_token, bounded_string, github_schema, invalid_arguments, json_result, positive_integer,
    serve_http, signal_arguments, stateless_http_config, HttpListener,
};
use axum::http::{header, HeaderMap, StatusCode};
use quorum_core::error::{QuorumError, Result};
use rmcp::{
    model::{
        CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ErrorCode,
        Implementation, ListPromptsResult, ListResourceTemplatesResult, ListResourcesResult,
        ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
        ToolAnnotations,
    },
    service::RequestContext,
    ErrorData as McpError, RoleServer, ServerHandler,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Not a token source. An operator who exports it anyway is refused at
/// startup, and the daemon removes it from every process it starts.
pub(crate) const TOKEN_ENV: &str = "QUORUM_ORCHESTRATOR_TOKEN";
/// A token shorter than this is refused at startup: it is the only secret
/// guarding task intake, possibly on a non-loopback address.
const MIN_TOKEN_BYTES: usize = 32;
const MAX_TOKEN_BYTES: usize = 512;
const MAX_ALLOWED_HOSTS: usize = 16;

/// Requests the gate admitted that may be in flight at once, and so also the
/// bound on concurrent database operations.
const MAX_ADMITTED_REQUESTS: usize = 8;
/// Open connections, authenticated or not. Deliberately larger than the
/// admitted-request bound: a peer without the token can occupy these, each
/// only until its request head is refused or times out, and never a request
/// slot.
const MAX_CONNECTIONS: usize = 32;
/// A request head must arrive within this bound on a new or idle connection.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(2);

const MAX_IDENTITY_BYTES: usize = 128;
const MAX_TITLE_BYTES: usize = 512;
const MAX_BODY_BYTES: usize = 64 * 1024;
const MAX_NOTE_BYTES: usize = 32 * 1024;
const MAX_MESSAGE_BYTES: usize = 32 * 1024;
const MAX_LABELS: usize = 16;
const MAX_LABEL_BYTES: usize = 64;
const MAX_DEPENDENCIES: usize = 64;
const MAX_REFS_BYTES: usize = 4096;
const MAX_FILTER_BYTES: usize = 128;

/// Rows returned by a listing tool unless the caller asks for fewer.
const DEFAULT_PAGE: usize = 100;
const MAX_PAGE: usize = 200;
/// Newest notes and runs kept in one `task_get` result.
const MAX_DETAIL_ROWS: usize = 50;
/// Bound on one tool result's JSON text. Larger results are trimmed and say so.
const MAX_RESULT_BYTES: usize = 128 * 1024;
const MAX_ERROR_MESSAGE_BYTES: usize = 512;

const TASK_CREATE: &str = "task_create";
const TASK_LIST: &str = "task_list";
const TASK_GET: &str = "task_get";
const TASK_LOG: &str = "task_log";
const TASK_CANCEL: &str = "task_cancel";
const TASK_RETRY: &str = "task_retry";
const MESSAGE_AGENT: &str = "message_agent";

#[derive(Debug, Clone)]
struct Orchestrator {
    repo: String,
    /// The one coordinator identity every tool acts as.
    identity: Arc<str>,
    db: PathBuf,
    /// Bounds blocking database work, counted separately from connections
    /// because a blocking operation outlives a client that hung up on it.
    slots: Arc<tokio::sync::Semaphore>,
}

fn tool(
    name: &'static str,
    description: &'static str,
    properties: Value,
    required: &[&str],
    read_only: bool,
    destructive: bool,
) -> Tool {
    Tool::new(name, description, github_schema(properties, required)).with_annotations(
        ToolAnnotations::new()
            .read_only(read_only)
            .destructive(destructive)
            .idempotent(read_only)
            // The repository's Quorum database is the only world these touch.
            .open_world(false),
    )
}

fn tools() -> Vec<Tool> {
    vec![
        tool(
            TASK_CREATE,
            "Create one execution-ready task (the `quorum task-create` operation). Give observed \
             and expected behavior, relevant paths, constraints, and verification in `body`. At \
             most one of `continue_pr` (continue implementation from that PR's current head), \
             `review_pr` (review; requested changes get a managed remediation worker), or \
             `review_only` (review; never reworked) may be set; with none, new work starts from \
             the base branch. The daemon chooses complexity, model, and effort and alone moves \
             the task through its lifecycle. Not idempotent: every accepted call creates a task. \
             Success is {\"id\":N,\"repo\":\"owner/name\"}.",
            json!({
                "title": bounded_string(1, MAX_TITLE_BYTES),
                "body": bounded_string(1, MAX_BODY_BYTES),
                "priority": {"type":"integer"},
                "labels": {
                    "type":"array","maxItems":MAX_LABELS,
                    "items": bounded_string(1, MAX_LABEL_BYTES)
                },
                "refs": {
                    "type":"object",
                    "description":"Non-authoritative external references. `pr` and classifier keys are refused."
                },
                "depends_on": {
                    "type":"array","maxItems":MAX_DEPENDENCIES,
                    "items": positive_integer()
                },
                "continue_pr": positive_integer(),
                "review_pr": positive_integer(),
                "review_only": positive_integer(),
                "base_branch": bounded_string(1, quorum_core::tasks::MAX_TARGET_BRANCH_BYTES)
            }),
            &["title"],
            false,
            false,
        ),
        tool(
            TASK_LIST,
            "List tasks as summary rows without bodies (`quorum task-list --brief`), highest \
             priority first, optionally filtered by `status`, `label`, or `assignee`. Returns \
             {\"tasks\":[...],\"total\":N,\"offset\":N,\"truncated\":bool} and, when rows remain, \
             `next_offset` to pass as `offset`. Pages are separate reads, so a queue that \
             changes between them can repeat or skip a row.",
            json!({
                "status": bounded_string(1, MAX_FILTER_BYTES),
                "label": bounded_string(1, MAX_FILTER_BYTES),
                "assignee": bounded_string(1, MAX_IDENTITY_BYTES),
                "limit": {"type":"integer","minimum":1,"maximum":MAX_PAGE},
                "offset": {"type":"integer","minimum":0}
            }),
            &[],
            true,
            false,
        ),
        tool(
            TASK_GET,
            "Read one task (`quorum task-get`): status, assignee, reviewer, refs (including the \
             PR number and any recorded outcome), revision, body, and its newest notes and agent \
             runs. An oversized result drops the oldest notes, then shortens the body, and says \
             so in `notes_omitted`, `agent_runs_omitted`, or `body_truncated`.",
            json!({"task_id": positive_integer()}),
            &["task_id"],
            true,
            false,
        ),
        tool(
            TASK_LOG,
            "Read one task's lifecycle events in order (`quorum log --refs task#N`). Pass the \
             last `seq` seen as `since` to read on. Returns {\"events\":[...],\"truncated\":bool}; \
             when truncated, `next_since` is the `since` that continues.",
            json!({
                "task_id": positive_integer(),
                "since": {"type":"integer","minimum":0},
                "limit": {"type":"integer","minimum":1,"maximum":MAX_PAGE}
            }),
            &["task_id"],
            true,
            false,
        ),
        tool(
            TASK_CANCEL,
            "Cancel one task that has not finished (`quorum task-update --status cancelled`). \
             This server's coordinator identity must be the task's creator or current assignee, \
             exactly as on the command line. A decomposed source task \
             also needs `expected_revision` (from task_get) and cancels its whole graph. The \
             daemon stops the task's managed agents on its next tick. `note` records why. A task \
             that is unknown, already done, failed, or cancelled, or that this identity may not cancel, \
             is refused with code `rejected` and nothing changes.",
            json!({
                "task_id": positive_integer(),
                "expected_revision": {"type":"integer","minimum":0},
                "note": bounded_string(1, MAX_NOTE_BYTES)
            }),
            &["task_id"],
            false,
            true,
        ),
        tool(
            TASK_RETRY,
            "Resume a task the daemon parked after a bounded failure or that is provider-blocked, \
             or retry an eligible exhausted decomposition plan (`quorum task-retry`). It keeps \
             the task's PR, branch, dependencies, and rework context. It is not a general retry \
             for terminal failures: any other task is refused with code `rejected`, carrying the \
             command's own `outcome`, `reason`, and any `cancelled_deps` or `retry_count`.",
            json!({"task_id": positive_integer()}),
            &["task_id"],
            false,
            false,
        ),
        tool(
            MESSAGE_AGENT,
            "Queue a message for one daemon-managed agent (`quorum message`). The daemon delivers \
             it as a turn when that agent is idle, from this server's coordinator identity. A \
             message never changes task lifecycle. Not \
             idempotent. Success is {\"ok\":true,\"mailbox_id\":N}.",
            json!({
                "to": bounded_string(1, MAX_IDENTITY_BYTES),
                "body": bounded_string(1, MAX_MESSAGE_BYTES)
            }),
            &["to", "body"],
            false,
            false,
        ),
    ]
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskCreateArguments {
    title: String,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    labels: Option<Vec<String>>,
    #[serde(default)]
    refs: Option<Map<String, Value>>,
    #[serde(default)]
    depends_on: Option<Vec<i64>>,
    #[serde(default)]
    continue_pr: Option<i64>,
    #[serde(default)]
    review_pr: Option<i64>,
    #[serde(default)]
    review_only: Option<i64>,
    #[serde(default)]
    base_branch: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskListArguments {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    assignee: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskGetArguments {
    task_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskLogArguments {
    task_id: i64,
    #[serde(default)]
    since: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskCancelArguments {
    task_id: i64,
    #[serde(default)]
    expected_revision: Option<i64>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskRetryArguments {
    task_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageAgentArguments {
    to: String,
    body: String,
}

type Checked<T> = std::result::Result<T, CallToolResult>;

/// The text-safety rule for every free-text argument: non-empty, bounded, and
/// free of NUL. JSON decoding has already established valid UTF-8.
fn text(name: &str, value: &str, max_bytes: usize) -> Checked<()> {
    if value.trim().is_empty() {
        return Err(invalid_arguments(format!("{name} must not be empty")));
    }
    if value.len() > max_bytes {
        return Err(invalid_arguments(format!(
            "{name} is at most {max_bytes} bytes"
        )));
    }
    if value.contains('\0') {
        return Err(invalid_arguments(format!("{name} must not contain NUL")));
    }
    Ok(())
}

/// A name or single-line field: text with no control characters at all.
fn line(name: &str, value: &str, max_bytes: usize) -> Checked<()> {
    text(name, value, max_bytes)?;
    if value.chars().any(char::is_control) {
        return Err(invalid_arguments(format!(
            "{name} must not contain control characters"
        )));
    }
    Ok(())
}

fn positive(name: &str, value: i64) -> Checked<()> {
    if value <= 0 {
        return Err(invalid_arguments(format!("{name} must be positive")));
    }
    Ok(())
}

fn page(limit: Option<usize>) -> Checked<usize> {
    match limit {
        None => Ok(DEFAULT_PAGE),
        Some(limit) if (1..=MAX_PAGE).contains(&limit) => Ok(limit),
        Some(_) => Err(invalid_arguments(format!(
            "limit must be between 1 and {MAX_PAGE}"
        ))),
    }
}

fn refusal(code: &str, message: impl Into<String>) -> CallToolResult {
    let mut message = message.into();
    if message.len() > MAX_ERROR_MESSAGE_BYTES {
        let mut end = MAX_ERROR_MESSAGE_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    json_result(json!({"code": code, "message": message}), true)
}

fn internal() -> CallToolResult {
    refusal(
        "internal",
        "internal failure; the operator's error log has the detail",
    )
}

/// Shape one failed operation by the CLI's exit-code classes: an expected
/// clean negative (exit 1) and bad input (exit 2) are relayed and record
/// nothing; anything else (exit 3) is logged for the operator, as the CLI
/// logs it, and reported without its detail.
fn failure(db: &Path, tool: &str, error: &QuorumError) -> CallToolResult {
    match error.exit_code() {
        1 => refusal("rejected", error.to_string()),
        2 => invalid_arguments(error.to_string()),
        _ => {
            if let Ok(conn) = quorum_core::db::open(db) {
                quorum_core::errlog::log_error(
                    &conn,
                    quorum_core::clock::now(),
                    "orchestrator-mcp",
                    &format!("{tool}: {error}"),
                );
            }
            internal()
        }
    }
}

fn json_len(value: &Value) -> usize {
    value.to_string().len()
}

/// Drop rows from the end of `rows` until they fit one result beside
/// `overhead` bytes of other fields. Returns whether any row was dropped.
fn fit_rows(rows: &mut Vec<Value>, overhead: usize) -> bool {
    let mut size: usize = overhead + rows.iter().map(|row| json_len(row) + 1).sum::<usize>();
    let mut dropped = false;
    while size > MAX_RESULT_BYTES {
        let Some(row) = rows.pop() else {
            break;
        };
        size -= json_len(&row) + 1;
        dropped = true;
    }
    dropped
}

/// Room left in a listing result for its few scalar fields.
const LISTING_OVERHEAD_BYTES: usize = 256;

/// Keep the newest `MAX_DETAIL_ROWS` entries of the array at `key`.
fn keep_newest(view: &mut Map<String, Value>, key: &str, omitted_key: &str) {
    let Some(Value::Array(rows)) = view.get_mut(key) else {
        return;
    };
    let omitted = rows.len().saturating_sub(MAX_DETAIL_ROWS);
    if omitted > 0 {
        rows.drain(..omitted);
        view.insert(omitted_key.into(), Value::from(omitted));
    }
}

/// Fit one task view into a result: newest notes and runs only, then fewer
/// notes, then a shortened body. Each reduction is named in the result.
fn bounded_task_view(view: Value) -> Option<Value> {
    let Value::Object(mut view) = view else {
        return None;
    };
    keep_newest(&mut view, "notes", "notes_omitted");
    keep_newest(&mut view, "agent_runs", "agent_runs_omitted");
    let size = |view: &Map<String, Value>| json_len(&Value::Object(view.clone()));
    while size(&view) > MAX_RESULT_BYTES {
        let Some(Value::Array(notes)) = view.get_mut("notes") else {
            break;
        };
        if notes.is_empty() {
            break;
        }
        notes.remove(0);
        let omitted = view
            .get("notes_omitted")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        view.insert("notes_omitted".into(), Value::from(omitted + 1));
    }
    let excess = size(&view).saturating_sub(MAX_RESULT_BYTES);
    if excess > 0 {
        let Some(Value::String(body)) = view.get_mut("body") else {
            return None;
        };
        // Escaping can make the encoded body longer than its bytes, so drop
        // the excess plus a margin, then confirm the result fits.
        let mut end = body.len().saturating_sub(excess + 64);
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
        view.insert("body_truncated".into(), Value::Bool(true));
        if size(&view) > MAX_RESULT_BYTES {
            return None;
        }
    }
    Some(Value::Object(view))
}

impl Orchestrator {
    /// Run one blocking database operation on a bounded slot. `operation`
    /// receives the database path and opens its own short-lived connection.
    async fn blocking<F>(&self, tool: &'static str, operation: F) -> CallToolResult
    where
        F: FnOnce(&Path) -> Result<CallToolResult> + Send + 'static,
    {
        // The semaphore is never closed, so acquisition cannot fail.
        let slot = Arc::clone(&self.slots).acquire_owned().await.ok();
        let db = self.db.clone();
        let done = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            operation(&db).unwrap_or_else(|error| failure(&db, tool, &error))
        });
        done.await.unwrap_or_else(|_| internal())
    }

    async fn task_create(&self, arguments: TaskCreateArguments) -> Checked<CallToolResult> {
        line("title", &arguments.title, MAX_TITLE_BYTES)?;
        if let Some(body) = &arguments.body {
            text("body", body, MAX_BODY_BYTES)?;
        }
        let labels = match &arguments.labels {
            Some(labels) => {
                if labels.len() > MAX_LABELS {
                    return Err(invalid_arguments(format!(
                        "labels holds at most {MAX_LABELS} entries"
                    )));
                }
                for label in labels {
                    line("a label", label, MAX_LABEL_BYTES)?;
                }
                Some(json!(labels).to_string())
            }
            None => None,
        };
        let refs = match arguments.refs {
            Some(refs) => {
                let refs = Value::Object(refs).to_string();
                if refs.len() > MAX_REFS_BYTES {
                    return Err(invalid_arguments(format!(
                        "refs is at most {MAX_REFS_BYTES} bytes of JSON"
                    )));
                }
                if refs.contains("\\u0000") {
                    return Err(invalid_arguments("refs must not contain NUL"));
                }
                Some(refs)
            }
            None => None,
        };
        let depends_on = match &arguments.depends_on {
            Some(ids) => {
                if ids.len() > MAX_DEPENDENCIES {
                    return Err(invalid_arguments(format!(
                        "depends_on holds at most {MAX_DEPENDENCIES} task ids"
                    )));
                }
                for id in ids {
                    positive("a depends_on task id", *id)?;
                }
                Some(json!(ids).to_string())
            }
            None => None,
        };
        let entry_modes = [
            ("continue_pr", arguments.continue_pr),
            ("review_pr", arguments.review_pr),
            ("review_only", arguments.review_only),
        ];
        for (name, pr) in entry_modes {
            if let Some(pr) = pr {
                positive(name, pr)?;
            }
        }
        if entry_modes.iter().filter(|(_, pr)| pr.is_some()).count() > 1 {
            return Err(invalid_arguments(
                "continue_pr, review_pr, and review_only are mutually exclusive",
            ));
        }
        if let Some(branch) = &arguments.base_branch {
            line(
                "base_branch",
                branch,
                quorum_core::tasks::MAX_TARGET_BRANCH_BYTES,
            )?;
        }
        let repo = self.repo.clone();
        let identity = Arc::clone(&self.identity);
        Ok(self
            .blocking(TASK_CREATE, move |_db| {
                let task = crate::coordinator::NewTask {
                    created_by: &identity,
                    title: &arguments.title,
                    body: arguments.body.as_deref(),
                    priority: arguments.priority,
                    labels: labels.as_deref(),
                    refs: refs.as_deref(),
                    depends_on: depends_on.as_deref(),
                    review_pr: arguments.review_pr,
                    review_only: arguments.review_only,
                    continue_pr: arguments.continue_pr,
                    base_branch: arguments.base_branch.as_deref(),
                };
                crate::coordinator::validate_new_task(&task)?;
                // Task intake reads the operator's serve configuration. A
                // failure to read it is the operator's to see, never the
                // caller's: its text names and can quote that file.
                crate::serve_config::task_create_base_branch(&repo)
                    .map_err(|error| QuorumError::Io(error.to_string()))?;
                let id = crate::coordinator::task_create(&repo, &task, quorum_core::clock::now())?;
                Ok(json_result(json!({"id": id, "repo": repo}), false))
            })
            .await)
    }

    async fn task_list(&self, arguments: TaskListArguments) -> Checked<CallToolResult> {
        for (name, value) in [("status", &arguments.status), ("label", &arguments.label)] {
            if let Some(value) = value {
                line(name, value, MAX_FILTER_BYTES)?;
            }
        }
        if let Some(assignee) = &arguments.assignee {
            line("assignee", assignee, MAX_IDENTITY_BYTES)?;
        }
        let limit = page(arguments.limit)?;
        let offset = arguments.offset.unwrap_or(0);
        if offset < 0 {
            return Err(invalid_arguments("offset must be >= 0"));
        }
        Ok(self
            .blocking(TASK_LIST, move |db| {
                let (page, total) = {
                    let conn = quorum_core::db::open(db)?;
                    quorum_core::tasks::list_brief_page(
                        &conn,
                        arguments.status.as_deref(),
                        arguments.label.as_deref(),
                        arguments.assignee.as_deref(),
                        limit as i64,
                        offset,
                    )?
                };
                let mut rows = page
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| QuorumError::Io(error.to_string()))?;
                fit_rows(&mut rows, LISTING_OVERHEAD_BYTES);
                let next = offset.saturating_add(rows.len() as i64);
                let mut result = json!({"total": total, "offset": offset});
                // No progress is possible past a single row too large to
                // return; `truncated` without `next_offset` says so.
                let more = next < total && offset < total;
                if more && !rows.is_empty() {
                    result["next_offset"] = Value::from(next);
                }
                result["truncated"] = Value::Bool(more);
                result["tasks"] = Value::Array(rows);
                Ok(json_result(result, false))
            })
            .await)
    }

    async fn task_get(&self, arguments: TaskGetArguments) -> Checked<CallToolResult> {
        positive("task_id", arguments.task_id)?;
        Ok(self
            .blocking(TASK_GET, move |db| {
                let Some(view) = crate::coordinator::task_get(db, arguments.task_id)? else {
                    return Ok(refusal("not_found", "no such task"));
                };
                let view = serde_json::to_value(&view)
                    .map_err(|error| QuorumError::Io(error.to_string()))?;
                Ok(match bounded_task_view(view) {
                    Some(view) => json_result(view, false),
                    None => refusal(
                        "result_too_large",
                        "the task does not fit one result even without its notes and body",
                    ),
                })
            })
            .await)
    }

    async fn task_log(&self, arguments: TaskLogArguments) -> Checked<CallToolResult> {
        positive("task_id", arguments.task_id)?;
        let since = arguments.since.unwrap_or(0);
        if since < 0 {
            return Err(invalid_arguments("since must be >= 0"));
        }
        let limit = page(arguments.limit)?;
        Ok(self
            .blocking(TASK_LOG, move |db| {
                // One row past the page tells a full page from a last page.
                let mut events = {
                    let conn = quorum_core::db::open(db)?;
                    quorum_core::events::list(
                        &conn,
                        since,
                        Some(&format!("task#{}", arguments.task_id)),
                        limit as i64 + 1,
                        quorum_core::clock::now(),
                    )?
                };
                let mut more = events.len() > limit;
                events.truncate(limit);
                let mut rows = events
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| QuorumError::Io(error.to_string()))?;
                more |= fit_rows(&mut rows, LISTING_OVERHEAD_BYTES);
                let mut result = json!({"truncated": more});
                if let (true, Some(last)) = (more, rows.last()) {
                    result["next_since"] = last["seq"].clone();
                }
                result["events"] = Value::Array(rows);
                Ok(json_result(result, false))
            })
            .await)
    }

    async fn task_cancel(&self, arguments: TaskCancelArguments) -> Checked<CallToolResult> {
        positive("task_id", arguments.task_id)?;
        if arguments.expected_revision.is_some_and(|value| value < 0) {
            return Err(invalid_arguments("expected_revision must be >= 0"));
        }
        if let Some(note) = &arguments.note {
            text("note", note, MAX_NOTE_BYTES)?;
        }
        let identity = Arc::clone(&self.identity);
        Ok(self
            .blocking(TASK_CANCEL, move |db| {
                let now = quorum_core::clock::now();
                let mut conn = quorum_core::db::open(db)?;
                // The guarded UPDATE inside `update` is the whole decision,
                // including the creator-or-assignee rule for this identity:
                // zero rows is a clean negative, whoever else is racing.
                let fields = quorum_core::tasks::TaskUpdate {
                    status: Some("cancelled"),
                    expected_revision: arguments.expected_revision,
                    ..Default::default()
                };
                let task = match quorum_core::tasks::update(
                    &mut conn,
                    &identity,
                    arguments.task_id,
                    &fields,
                    now,
                ) {
                    Ok(task) => task,
                    Err(QuorumError::NotHolder) => {
                        return Ok(refusal(
                            "rejected",
                            "not cancelled: the task is unknown, already finished, or this \
                             coordinator identity is neither its creator nor its assignee",
                        ));
                    }
                    Err(error) => return Err(error),
                };
                // As on the command line, the note is a second transaction
                // after the cancellation has committed.
                let mut compact = quorum_core::tasks::TaskCompact::from(&task);
                if let Some(note) = &arguments.note {
                    compact.note_id = quorum_core::tasks::add_note(
                        &mut conn,
                        &identity,
                        arguments.task_id,
                        note,
                        now,
                    )?;
                }
                let compact = serde_json::to_value(&compact)
                    .map_err(|error| QuorumError::Io(error.to_string()))?;
                Ok(json_result(compact, false))
            })
            .await)
    }

    async fn task_retry(&self, arguments: TaskRetryArguments) -> Checked<CallToolResult> {
        positive("task_id", arguments.task_id)?;
        let identity = Arc::clone(&self.identity);
        Ok(self
            .blocking(TASK_RETRY, move |db| {
                let (emitted, exit) = crate::coordinator::task_retry(
                    db,
                    arguments.task_id,
                    &identity,
                    quorum_core::clock::now(),
                )?;
                let mut value: Value = serde_json::from_str(&emitted)
                    .map_err(|error| QuorumError::Io(error.to_string()))?;
                if exit == 0 {
                    return Ok(json_result(value, false));
                }
                // A refusal keeps every field the command emits (`outcome`,
                // `reason`, `cancelled_deps`, `retry_count`, `retry_cap`)
                // beside this server's `code` and `message`.
                let message = value["reason"]
                    .as_str()
                    .unwrap_or("the task is not eligible for retry")
                    .to_string();
                value["code"] = Value::from("rejected");
                value["message"] = Value::from(message);
                Ok(json_result(value, true))
            })
            .await)
    }

    async fn message_agent(&self, arguments: MessageAgentArguments) -> Checked<CallToolResult> {
        line("to", &arguments.to, MAX_IDENTITY_BYTES)?;
        text("body", &arguments.body, MAX_MESSAGE_BYTES)?;
        let identity = self.identity.to_string();
        Ok(self
            .blocking(MESSAGE_AGENT, move |db| {
                let id = crate::coordinator::message(db, identity, arguments.to, arguments.body)?;
                Ok(json_result(json!({"ok": true, "mailbox_id": id}), false))
            })
            .await)
    }

    /// Decode one tool's closed argument shape and run it. `None` when `name`
    /// is not a tool of this server.
    async fn call(
        &self,
        name: &str,
        arguments: Option<Map<String, Value>>,
    ) -> Option<Checked<CallToolResult>> {
        macro_rules! route {
            ($method:ident) => {
                match signal_arguments(name, arguments) {
                    Ok(arguments) => self.$method(arguments).await,
                    Err(refused) => Err(refused),
                }
            };
        }
        Some(match name {
            TASK_CREATE => route!(task_create),
            TASK_LIST => route!(task_list),
            TASK_GET => route!(task_get),
            TASK_LOG => route!(task_log),
            TASK_CANCEL => route!(task_cancel),
            TASK_RETRY => route!(task_retry),
            MESSAGE_AGENT => route!(message_agent),
            _ => return None,
        })
    }
}

fn method_not_found<T>() -> std::result::Result<T, McpError> {
    Err(McpError::new(
        ErrorCode::METHOD_NOT_FOUND,
        "method not found",
        None,
    ))
}

impl ServerHandler for Orchestrator {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "quorum-orchestrator",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Coordinator tools for one repository's Quorum task queue: create, list, \
                 inspect, cancel, and retry tasks, and message a managed agent. Create only \
                 execution-ready tasks. The daemon alone claims work, transitions lifecycle, \
                 reviews, and merges; nothing here does, and nothing here controls the daemon.",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(tools())
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, McpError> {
        match self.call(&request.name, request.arguments).await {
            Some(Ok(result)) | Some(Err(result)) => Ok(result.into()),
            None => Err(McpError::new(
                ErrorCode::METHOD_NOT_FOUND,
                "unknown tool",
                None,
            )),
        }
    }

    fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListPromptsResult, McpError>> + Send + '_ {
        std::future::ready(method_not_found())
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListResourcesResult, McpError>> + Send + '_ {
        std::future::ready(method_not_found())
    }

    fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = std::result::Result<ListResourceTemplatesResult, McpError>> + Send + '_
    {
        std::future::ready(method_not_found())
    }
}

/// Compare a presented token with the configured one without an early exit,
/// so the time taken does not reveal how much of a guess was right. Every
/// presented byte is examined whatever the lengths.
fn token_matches(presented: &[u8], expected: &[u8]) -> bool {
    let mut difference = presented.len() ^ expected.len();
    for (index, byte) in presented.iter().enumerate() {
        // `expected` is never empty: startup refuses a short token.
        difference |= usize::from(byte ^ expected[index % expected.len()]);
    }
    std::hint::black_box(difference) == 0
}

/// The request gate. A request without exactly the operator's token is `401`
/// whatever else it carries. An authenticated request naming an `Origin` is
/// `403`: no browser client is supported, so there is no origin to allow.
fn gate(headers: &HeaderMap, token: &[u8]) -> std::result::Result<(), StatusCode> {
    match bearer_token(headers, MAX_TOKEN_BYTES) {
        Some(presented) if token_matches(presented.as_bytes(), token) => {}
        _ => return Err(StatusCode::UNAUTHORIZED),
    }
    if headers.contains_key(header::ORIGIN) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

fn valid_token(token: &str) -> bool {
    (MIN_TOKEN_BYTES..=MAX_TOKEN_BYTES).contains(&token.len())
        && token.bytes().all(|byte| byte.is_ascii_graphic())
}

/// Load the operator's bearer token from `--token-file`, the only source.
///
/// The file must be a regular file owned by this user with no group or other
/// permission bits, so no other user can read it. That does not hide it from
/// processes of the same user; see the design record's trust model.
fn load_token(token_file: &Path) -> Result<Vec<u8>> {
    let token = crate::secret_file::read_owner_only(token_file, unsafe { libc::geteuid() })
        .map_err(|reason| QuorumError::Usage(format!("--token-file {reason}")))?;
    if !valid_token(&token) {
        return Err(QuorumError::Usage(format!(
            "the orchestrator token must be {MIN_TOKEN_BYTES} to {MAX_TOKEN_BYTES} printable \
             ASCII characters with no spaces"
        )));
    }
    Ok(token.into_bytes())
}

/// The `Host` names a request may carry. A loopback bind accepts the loopback
/// names plus any the operator adds. A non-loopback bind has no safe default,
/// so the operator must name at least one host.
fn allowed_hosts(addr: SocketAddr, configured: Vec<String>) -> Result<Vec<String>> {
    if configured.len() > MAX_ALLOWED_HOSTS {
        return Err(QuorumError::Usage(format!(
            "at most {MAX_ALLOWED_HOSTS} --allowed-host values are accepted"
        )));
    }
    for host in &configured {
        if host.is_empty()
            || host.len() > 255
            || host.parse::<axum::http::uri::Authority>().is_err()
        {
            return Err(QuorumError::Usage(format!(
                "--allowed-host must be a host or host:port (got {host:?})"
            )));
        }
    }
    if addr.ip().is_loopback() {
        let mut hosts = vec!["localhost".to_string(), "127.0.0.1".into(), "::1".into()];
        hosts.extend(configured);
        return Ok(hosts);
    }
    if configured.is_empty() {
        return Err(QuorumError::Usage(
            "orchestrator-mcp on a non-loopback address requires --allowed-host: name each \
             host (or host:port) that clients use to reach it"
                .into(),
        ));
    }
    Ok(configured)
}

pub fn run(
    listen: SocketAddr,
    token_file: &Path,
    identity: String,
    hosts: Vec<String>,
    repo: Option<String>,
) -> Result<()> {
    // The environment is not a token source: it is inherited by child
    // processes and outlives the reason it was set. Refuse rather than let an
    // operator believe an exported token is in use.
    if std::env::var_os(TOKEN_ENV).is_some() {
        return Err(QuorumError::Usage(format!(
            "{TOKEN_ENV} is not read; put the token in a file and pass --token-file"
        )));
    }
    let token = load_token(token_file)?;
    // Advisory only: a managed run normally has no database to serve, and a
    // process that controls its own environment can unset this. The token
    // file and database permissions are the boundary, not this check.
    if std::env::var_os("QUORUM_RUN_ID").is_some() {
        return Err(QuorumError::Usage(
            "orchestrator-mcp cannot be started from a daemon-managed run".into(),
        ));
    }
    if line("identity", &identity, MAX_IDENTITY_BYTES).is_err() {
        return Err(QuorumError::Usage(format!(
            "--identity must be 1 to {MAX_IDENTITY_BYTES} bytes with no control characters"
        )));
    }
    let hosts = allowed_hosts(listen, hosts)?;
    let repo = crate::resolve_repo_override(repo.as_deref())?;
    // Serve only a repository that already has a database. Opening a missing
    // path would create an empty one for a mistyped name.
    let db = crate::paths::db_path_for_repo(&repo)?;
    if !db.is_file() {
        return Err(QuorumError::Usage(format!(
            "no Quorum database for {repo}; run `quorum init` for that repository first"
        )));
    }
    // Fail at startup, not on the first call, when the database is unusable
    // or newer than this binary.
    drop(quorum_core::db::open(&db)?);
    let server = Orchestrator {
        repo,
        identity: identity.into(),
        db,
        slots: Arc::new(tokio::sync::Semaphore::new(MAX_ADMITTED_REQUESTS)),
    };
    let token: Arc<[u8]> = token.into();
    serve_http(
        HttpListener {
            command: "orchestrator-mcp",
            title: "orchestrator MCP",
            max_connections: MAX_CONNECTIONS,
            header_read_timeout: HEADER_READ_TIMEOUT,
            max_admitted_requests: Some(MAX_ADMITTED_REQUESTS),
        },
        listen,
        stateless_http_config().with_allowed_hosts(hosts),
        server,
        move |headers: &HeaderMap| gate(headers, &token),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_requires_an_exact_match() {
        let expected = b"0123456789abcdef0123456789abcdef";
        assert!(token_matches(expected, expected));
        assert!(!token_matches(b"", expected));
        assert!(!token_matches(&expected[..31], expected));
        assert!(!token_matches(
            b"0123456789abcdef0123456789abcdeX",
            expected
        ));
        // A repeat of the token must not match through the wrapped index.
        assert!(!token_matches(
            &[&expected[..], &expected[..]].concat(),
            expected
        ));
    }

    #[test]
    fn token_shape_is_bounded_printable_ascii() {
        assert!(valid_token(&"a".repeat(32)));
        assert!(valid_token(&"a".repeat(512)));
        assert!(!valid_token(&"a".repeat(31)));
        assert!(!valid_token(&"a".repeat(513)));
        assert!(!valid_token(&format!(
            "{} {}",
            "a".repeat(20),
            "b".repeat(20)
        )));
        assert!(!valid_token(&format!("{}\n", "a".repeat(40))));
        assert!(!valid_token(&"é".repeat(32)));
    }

    #[test]
    fn non_loopback_binds_need_an_explicit_host() {
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let any: SocketAddr = "0.0.0.0:0".parse().unwrap();
        assert_eq!(allowed_hosts(loopback, vec![]).unwrap().len(), 3);
        assert!(allowed_hosts(any, vec![]).is_err());
        assert_eq!(
            allowed_hosts(any, vec!["quorum.internal:8811".into()]).unwrap(),
            vec!["quorum.internal:8811".to_string()]
        );
        assert!(allowed_hosts(any, vec!["bad host".into()]).is_err());
        assert!(allowed_hosts(any, vec![String::new()]).is_err());
    }

    #[test]
    fn an_oversized_task_view_sheds_old_notes_then_body() {
        let note = |id: usize| json!({"id": id, "ts": 1, "agent": "a", "body": "n".repeat(8000)});
        let view = json!({
            "id": 1,
            "body": "b".repeat(1000),
            "notes": (0..80).map(note).collect::<Vec<_>>(),
            "agent_runs": [],
        });
        let bounded = bounded_task_view(view).unwrap();
        assert!(json_len(&bounded) <= MAX_RESULT_BYTES);
        let notes = bounded["notes"].as_array().unwrap();
        // The newest note survives; the count of dropped notes is exact.
        assert_eq!(notes.last().unwrap()["id"], 79);
        assert_eq!(bounded["notes_omitted"], 80 - notes.len());
        assert!(bounded.get("body_truncated").is_none());

        let view = json!({"id": 1, "body": "\"é".repeat(120_000), "notes": [], "agent_runs": []});
        let bounded = bounded_task_view(view).unwrap();
        assert!(json_len(&bounded) <= MAX_RESULT_BYTES);
        assert_eq!(bounded["body_truncated"], true);
    }

    #[test]
    fn listed_rows_are_dropped_from_the_end_until_they_fit() {
        let mut rows: Vec<Value> = (0..40)
            .map(|id| json!({"id": id, "t": "x".repeat(8000)}))
            .collect();
        assert!(fit_rows(&mut rows, LISTING_OVERHEAD_BYTES));
        assert!(json_len(&Value::Array(rows.clone())) + LISTING_OVERHEAD_BYTES <= MAX_RESULT_BYTES);
        assert_eq!(rows[0]["id"], 0);
        assert!(!rows.is_empty() && rows.len() < 40);
        let mut small = vec![json!({"id": 1})];
        assert!(!fit_rows(&mut small, LISTING_OVERHEAD_BYTES));
        assert_eq!(small.len(), 1);
    }
}
