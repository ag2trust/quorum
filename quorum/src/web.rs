//! Read-only local dashboard. Connections are intentionally request-scoped so a browser
//! left open for days never pins SQLite's WAL through a held read transaction.

use axum::{
    extract::{
        rejection::{PathRejection, QueryRejection},
        Path, Query, State,
    },
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use rusqlite::OptionalExtension;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    net::{IpAddr, SocketAddr},
    path::{Path as FsPath, PathBuf},
};

const PAGE: &str = include_str!("web.html");
const CLIENT: &str = include_str!("web.js");
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 100;
const DEFAULT_STREAM_BYTES: u64 = 2 * 1024 * 1024;
const MAX_STREAM_BYTES: u64 = 8 * 1024 * 1024;
// Keep the transport bounded by the same record budget the browser normalizer uses. The
// first record is retained for a continuation from the preceding poll; the rest are the
// newest suffix so an active stream remains useful when a byte window is dense.
const MAX_STREAM_RECORDS: usize = 2_000;
const DASHBOARD_AGENT_LIMIT: i64 = 100;
const STATE_BAND_LIMIT: usize = 20;
const TASK_LIST_DEFAULT_LIMIT: usize = 50;
const TASK_LIST_MAX_LIMIT: usize = 100;
const DETAIL_EVENT_LIMIT: i64 = 30;
const DETAIL_NOTE_LIMIT: i64 = 30;
const DETAIL_RUN_LIMIT: i64 = 30;
const DETAIL_DEPENDENCY_LIMIT: usize = 32;
const DETAIL_CHILD_LIMIT: i64 = 32;
const MAX_QUERY_TEXT_CHARS: usize = 256;
const MAX_TITLE_CHARS: usize = 512;
const MAX_BODY_CHARS: usize = 64 * 1024;
const MAX_DETAIL_TEXT_CHARS: usize = 2 * 1024;
const MAX_IDENTITY_CHARS: usize = 256;

#[derive(Clone)]
struct AppState {
    db_path: PathBuf,
    logs_root: PathBuf,
    online_window: i64,
}

pub fn serve(
    db_path: PathBuf,
    logs_root: PathBuf,
    bind: &str,
    port: u16,
    online_window: i64,
) -> quorum_core::error::Result<()> {
    let ip: IpAddr = bind.parse().map_err(|e| {
        quorum_core::error::QuorumError::Usage(format!("invalid --bind/--port: {e}"))
    })?;
    let addr = SocketAddr::new(ip, port);
    validate_loopback(addr)?;
    let state = AppState {
        db_path,
        logs_root,
        online_window,
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/web.js", get(client))
        .route("/api/state", get(api_state))
        .route("/api/tasks", get(api_tasks))
        .route("/api/tasks/:id", get(api_task))
        .route("/api/runs", get(api_runs))
        .route("/api/runs/:dir/stream", get(api_stream))
        .route("/api/runs/:dir/transcript", get(api_transcript))
        .with_state(state);
    eprintln!("quorum web listening on http://{addr}");
    tokio::runtime::Runtime::new()
        .map_err(|e| quorum_core::error::QuorumError::Io(e.to_string()))?
        .block_on(async move {
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| quorum_core::error::QuorumError::Io(e.to_string()))?;
            axum::serve(listener, app)
                .await
                .map_err(|e| quorum_core::error::QuorumError::Io(e.to_string()))
        })
}

fn validate_loopback(addr: SocketAddr) -> quorum_core::error::Result<()> {
    if addr.ip().is_loopback() {
        return Ok(());
    }
    Err(quorum_core::error::QuorumError::Usage(
        "quorum web is loopback-only; remote serving requires a separately designed authenticated transport".into(),
    ))
}

async fn index() -> Html<&'static str> {
    Html(PAGE)
}

async fn client() -> Response {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        CLIENT,
    )
        .into_response()
}

async fn api_state(State(state): State<AppState>) -> Response {
    match state_payload(&state) {
        Ok(value) => Json(value).into_response(),
        Err(error) => server_error(error),
    }
}

/// Opens and drops the connection in this function. Do not move it into AppState: doing so
/// would allow a long-lived request loop to retain a SQLite reader and pin the WAL.
fn state_payload(state: &AppState) -> quorum_core::error::Result<Value> {
    let now = quorum_core::clock::now();
    let conn = quorum_core::db::open(&state.db_path)?;
    let roster = quorum_core::agents::roster_limited(
        &conn,
        now,
        state.online_window,
        DASHBOARD_AGENT_LIMIT,
    )?;
    let live_agents = quorum_core::stats::web_daemon_agents(&conn, now)?
        .into_iter()
        .take(DASHBOARD_AGENT_LIMIT as usize)
        .map(|agent| {
            let run_dir = agent
                .log_dir
                .as_deref()
                .and_then(|path| FsPath::new(path).file_name())
                .and_then(|name| name.to_str())
                .filter(|name| run_entry((*name).to_owned()).is_some());
            json!({
                "name": bounded_text(&agent.agent, MAX_IDENTITY_CHARS),
                "role": bounded_text(&agent.role, MAX_IDENTITY_CHARS),
                "task_id": agent.task_id,
                "task_title": bounded_option(agent.task_title.as_deref(), MAX_TITLE_CHARS),
                "phase": bounded_text(&agent.phase, MAX_IDENTITY_CHARS),
                "provider": bounded_option(agent.provider.as_deref(), MAX_IDENTITY_CHARS),
                "model": bounded_option(agent.model.as_deref(), MAX_IDENTITY_CHARS),
                "effort": bounded_option(agent.effort.as_deref(), MAX_IDENTITY_CHARS),
                "pr": agent.pr,
                "run_dir": bounded_option(run_dir, MAX_IDENTITY_CHARS),
                "last_activity_age_secs": agent.last_activity_age_secs,
                "agent_state": bounded_option(agent.agent_state.as_deref(), MAX_IDENTITY_CHARS),
                "live_error_count": agent.live_error_count,
                "live_error_text": agent.live_error_text.map(|text| bounded_text(&text, MAX_DETAIL_TEXT_CHARS)),
                "now": bounded_option(agent.now_label.as_deref(), MAX_DETAIL_TEXT_CHARS),
            })
        })
        .collect::<Vec<_>>();
    let held_by_agent: std::collections::HashMap<String, Value> = live_agents
        .iter()
        .filter_map(|agent| {
            agent["task_id"].as_i64().map(|id| {
                (
                    agent["name"].as_str().unwrap_or_default().to_owned(),
                    json!({"id": id, "title": agent["task_title"]}),
                )
            })
        })
        .collect();
    let agents: Vec<Value> = roster
        .into_iter()
        .map(|agent| {
            json!({"name": bounded_text(&agent.id, MAX_IDENTITY_CHARS), "last_seen": agent.last_seen, "online": agent.online,
            "task_held": held_by_agent.get(&agent.id), "run_dir": Value::Null})
        })
        .collect();
    let counts = quorum_core::stats::web_task_counts(&conn)?;
    let alert_rows = quorum_core::stats::web_alerts(&conn, now)?;
    let error_rows = quorum_core::stats::web_recent_errors(&conn, now)?;
    let alerts = alert_rows.iter().map(alert_value).collect::<Vec<_>>();
    let errors = error_rows.iter().map(error_value).collect::<Vec<_>>();
    // Start with the task-core projection that repeats the dependency and graph predicates
    // used by a claim, then apply the same persisted classification admission the daemon uses
    // before it attempts an implementation claim. Do not substitute a recent/open task page.
    let mut ready_all =
        quorum_core::tasks::list_implementation_ready_open_limited(&conn, STATE_BAND_LIMIT as i64)?;
    ready_all.retain(|task| {
        !task.review_only
            && quorum_core::tasks::classification_is_dispatchable(
                &task.refs,
                task.review_only,
                task.continue_pr,
            )
    });
    let ready_count = ready_all.len();
    let ready = ready_all
        .into_iter()
        .map(|task| task_summary(&conn, &task, now))
        .collect::<quorum_core::error::Result<Vec<_>>>()?;
    let planning = state_band(&conn, "status IN ('planning', 'decomposed')", now)?;
    let working = state_band(&conn, "status IN ('working', 'rework')", now)?;
    let reviewing = state_band(&conn, "status='in-review'", now)?;
    let merging = state_band(&conn, "status='merging'", now)?;
    let blocked = blocked_task_summaries(&conn, now)?;
    let planning_holds = state_band(
        &conn,
        "status='planning' AND EXISTS (SELECT 1 FROM task_decompositions d WHERE d.source_task_id=tasks.id AND (d.state != 'active' OR d.active=0))",
        now,
    )?;
    let merge_waits = state_band(
        &conn,
        "(status='merging' OR (body='daemon:merge-blocked' AND status NOT IN ('done', 'failed', 'cancelled')))",
        now,
    )?;
    let attention_outcomes = state_band(&conn, "status IN ('failed', 'cancelled')", now)?;
    let stalled_agents: Vec<Value> = live_agents
        .iter()
        .filter(|agent| {
            agent["role"] == "worker"
                && agent["phase"] != "awaiting-review"
                && agent["last_activity_age_secs"]
                    .as_i64()
                    .map(|age| age > 60)
                    .unwrap_or(true)
        })
        .cloned()
        .collect();
    let error_agents: Vec<Value> = live_agents
        .iter()
        .filter(|agent| {
            agent["live_error_count"].as_u64().unwrap_or_default() > 0
                || agent["agent_state"]
                    .as_str()
                    .is_some_and(|state| matches!(state, "blocked" | "failed" | "needs-info"))
        })
        .cloned()
        .collect();
    let health = health_value(&live_agents, &alert_rows, &error_rows, now);
    let terminal_counts = terminal_counts(&counts);
    let tasks = ready.clone();
    drop(conn);
    Ok(json!({
        "now": now,
        "health": health,
        "working_now": live_agents,
            "queue_bands": {
            "planning": planning,
            "ready": {"count": ready_count, "tasks": ready},
            "working": working,
            "reviewing": reviewing,
            "merging": merging,
                "terminal": terminal_counts,
                "attention": attention_outcomes,
        },
        "needs_attention": {
            "blocked_tasks": blocked,
            "planning_holds": planning_holds,
            "stalled_agents": stalled_agents,
            "error_agents": error_agents,
            "merge_waits": merge_waits,
            "alerts": alerts,
            "recent_errors": errors,
        },
        "counts": counts,
        "tasks": tasks,
        "agents": agents,
        "live_agents": live_agents,
        "alerts": alerts,
        "errors": errors,
    }))
}

/// A summary row deliberately excludes task bodies, notes, events, and run history. The
/// browser polls this representation frequently; `/api/tasks/:id` owns those on-demand reads.
fn task_summary(
    conn: &rusqlite::Connection,
    task: &quorum_core::tasks::Task,
    now: i64,
) -> quorum_core::error::Result<Value> {
    let run = quorum_core::agent_runs::latest_for_task(conn, task.id)?;
    let refs = refs_value(task.refs.as_deref());
    Ok(json!({
        "id": task.id,
        "title": bounded_text(&task.title, MAX_TITLE_CHARS),
        "status": bounded_text(&task.status, MAX_IDENTITY_CHARS),
        // Retained for the current dashboard client while callers migrate to `status`.
        "state": bounded_text(&task.status, MAX_IDENTITY_CHARS),
        "priority": task.priority,
        "labels": bounded_labels(task.labels.as_deref()),
        "assignee": bounded_option(task.assignee.as_deref(), MAX_IDENTITY_CHARS),
        "author": bounded_option(task.author.as_deref(), MAX_IDENTITY_CHARS),
        "reviewer": bounded_option(task.reviewer.as_deref(), MAX_IDENTITY_CHARS),
        "created_at": task.created_at,
        "updated_at": task.updated_at,
        "age_secs": (now - task.created_at).max(0),
        "ready": task.ready,
        "pr": quorum_core::tasks::extract_pr_number(&task.refs),
        "branch": task_branch(task, &refs),
        "provider": run.as_ref().and_then(|run| run.provider.as_deref()).map(|value| bounded_text(value, MAX_IDENTITY_CHARS)),
        "model": run.as_ref().map(|run| bounded_text(&run.model, MAX_IDENTITY_CHARS)),
    }))
}

/// `predicate` is always a fixed, local SQL fragment below; no request text is interpolated.
fn state_band(
    conn: &rusqlite::Connection,
    predicate: &str,
    now: i64,
) -> quorum_core::error::Result<Value> {
    let count: i64 = conn.query_row(
        &format!("SELECT count(*) FROM tasks WHERE {predicate}"),
        [],
        |row| row.get(0),
    )?;
    let mut stmt = conn.prepare(&format!(
        "SELECT id FROM tasks WHERE {predicate} ORDER BY priority DESC, updated_at DESC, id DESC LIMIT ?1"
    ))?;
    let ids = stmt
        .query_map([STATE_BAND_LIMIT as i64], |row| row.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let tasks = ids
        .into_iter()
        .map(|id| {
            quorum_core::tasks::get(conn, id)?
                .map(|task| task_summary(conn, &task, now))
                .transpose()
        })
        .collect::<quorum_core::error::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    Ok(json!({"count": count, "tasks": tasks}))
}

fn blocked_task_summaries(
    conn: &rusqlite::Connection,
    now: i64,
) -> quorum_core::error::Result<Value> {
    // This mirrors the durable status projection rather than inferring "blocked" from a
    // recent task page. A missing dependency is also unsatisfied, just as `compute_ready` is;
    // failed-but-parked rows with a cancelled prerequisite are retained as disposition work.
    state_band(
        conn,
        "depends_on IS NOT NULL AND ((status='open' AND EXISTS (SELECT 1 FROM json_each(depends_on) dependency WHERE NOT EXISTS (SELECT 1 FROM tasks prerequisite WHERE prerequisite.id=dependency.value AND prerequisite.status='done'))) OR (status='failed' AND json_valid(refs) AND json_extract(refs, '$.daemon_parked')=1 AND EXISTS (SELECT 1 FROM json_each(depends_on) dependency JOIN tasks prerequisite ON prerequisite.id=dependency.value WHERE prerequisite.status='cancelled')))",
        now,
    )
}

fn terminal_counts(counts: &[quorum_core::stats::StatusCount]) -> Value {
    let count = |status| {
        counts
            .iter()
            .find(|row| row.status == status)
            .map(|row| row.count)
            .unwrap_or_default()
    };
    json!({"done": count("done"), "failed": count("failed"), "cancelled": count("cancelled")})
}

fn health_value(
    live_agents: &[Value],
    alerts: &[quorum_core::stats::AlertMessage],
    errors: &[quorum_core::stats::DedupedError],
    now: i64,
) -> Value {
    let eligible =
        |agent: &&Value| agent["role"] == "worker" && agent["phase"] != "awaiting-review";
    let stalled = live_agents.iter().filter(eligible).any(|agent| {
        agent["last_activity_age_secs"]
            .as_i64()
            .map(|age| age > 180)
            .unwrap_or(true)
    });
    let attention = live_agents.iter().filter(eligible).any(|agent| {
        agent["last_activity_age_secs"]
            .as_i64()
            .map(|age| age > 60)
            .unwrap_or(false)
    }) || live_agents
        .iter()
        .any(|agent| agent["live_error_count"].as_u64().unwrap_or_default() > 0)
        || !alerts.is_empty()
        || !errors.is_empty();
    let verdict = if stalled {
        "stalled"
    } else if attention {
        "attention"
    } else {
        "on-track"
    };
    json!({"verdict": verdict, "refreshed_at": now, "stale_after_secs": 10})
}

fn alert_value(alert: &quorum_core::stats::AlertMessage) -> Value {
    json!({
        "body": bounded_text(&alert.body, MAX_DETAIL_TEXT_CHARS),
        "refs": bounded_option(alert.refs.as_deref(), MAX_DETAIL_TEXT_CHARS),
        "age_secs": alert.age_secs,
        "kind": bounded_text(&alert.kind, MAX_IDENTITY_CHARS),
    })
}

fn error_value(error: &quorum_core::stats::DedupedError) -> Value {
    json!({
        "detail": bounded_text(&error.detail, MAX_DETAIL_TEXT_CHARS),
        "source": bounded_text(&error.source, MAX_IDENTITY_CHARS),
        "count": error.count,
        "latest_age_secs": error.latest_age_secs,
    })
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let bounded = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        if max_chars == 0 {
            String::new()
        } else {
            format!(
                "{}…",
                bounded.chars().take(max_chars - 1).collect::<String>()
            )
        }
    } else {
        bounded
    }
}

fn bounded_option(value: Option<&str>, max_chars: usize) -> Option<String> {
    value.map(|value| bounded_text(value, max_chars))
}

fn bounded_labels(raw: Option<&str>) -> Vec<String> {
    raw.and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .unwrap_or_default()
        .into_iter()
        .take(DETAIL_DEPENDENCY_LIMIT)
        .map(|label| bounded_text(&label, MAX_IDENTITY_CHARS))
        .collect()
}

fn refs_value(raw: Option<&str>) -> Value {
    raw.and_then(|raw| serde_json::from_str(raw).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}))
}

fn ref_string(refs: &Value, key: &str) -> Option<String> {
    refs.get(key)
        .and_then(Value::as_str)
        .map(|value| bounded_text(value, MAX_DETAIL_TEXT_CHARS))
}

fn task_branch(task: &quorum_core::tasks::Task, refs: &Value) -> Option<String> {
    bounded_option(task.target_branch.as_deref(), MAX_IDENTITY_CHARS).or_else(|| {
        refs.get("branch")
            .or_else(|| refs.get("head_ref"))
            .and_then(Value::as_str)
            .map(|value| bounded_text(value, MAX_IDENTITY_CHARS))
    })
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TaskListQuery {
    cursor: Option<i64>,
    limit: Option<usize>,
    status: Option<String>,
    label: Option<String>,
    assignee: Option<String>,
}

async fn api_tasks(
    State(state): State<AppState>,
    query: Result<Query<TaskListQuery>, QueryRejection>,
) -> Response {
    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => return bad_request("invalid task list query"),
    };
    let query = match validate_task_list_query(query) {
        Ok(query) => query,
        Err(error) => return bad_request(&error),
    };
    match task_list_payload(&state, query) {
        Ok(value) => Json(value).into_response(),
        Err(error) => server_error(error),
    }
}

async fn api_task(
    State(state): State<AppState>,
    id: Result<Path<String>, PathRejection>,
) -> Response {
    let Path(id) = match id {
        Ok(id) => id,
        Err(_) => return bad_request("invalid task id"),
    };
    let id = match id.parse::<i64>().ok().filter(|id| *id > 0) {
        Some(id) => id,
        None => return bad_request("invalid task id"),
    };
    match task_detail_payload(&state, id) {
        Ok(Some(value)) => Json(value).into_response(),
        Ok(None) => not_found("task not found"),
        Err(error) => server_error(error),
    }
}

/// Opens and closes a connection for this one paginated list request. Cursor ordering is by
/// task id descending, which is stable even while priority and timestamps change underneath it.
fn task_list_payload(state: &AppState, query: TaskListQuery) -> quorum_core::error::Result<Value> {
    let limit = query.limit.unwrap_or(TASK_LIST_DEFAULT_LIMIT);
    let label = query.label.as_deref();
    let conn = quorum_core::db::open(&state.db_path)?;
    let mut stmt = conn.prepare(
        "SELECT id FROM tasks
             WHERE (?1 IS NULL OR id < ?1)
               AND (?2 IS NULL OR status = ?2)
               AND (?3 IS NULL OR (json_valid(COALESCE(labels, '[]'))
                    AND EXISTS (SELECT 1 FROM json_each(labels) WHERE value = ?3)))
               AND (?4 IS NULL OR assignee = ?4)
             ORDER BY id DESC LIMIT ?5",
    )?;
    let mut ids = stmt
        .query_map(
            rusqlite::params![
                query.cursor,
                query.status.as_deref(),
                label,
                query.assignee.as_deref(),
                (limit + 1) as i64,
            ],
            |row| row.get::<_, i64>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    let has_next = ids.len() > limit;
    ids.truncate(limit);
    let now = quorum_core::clock::now();
    let tasks = ids
        .iter()
        .map(|id| {
            quorum_core::tasks::get(&conn, *id)?
                .map(|task| task_summary(&conn, &task, now))
                .transpose()
        })
        .collect::<quorum_core::error::Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let next_cursor = has_next.then(|| ids.last().copied()).flatten();
    drop(conn);
    Ok(json!({"tasks": tasks, "next_cursor": next_cursor}))
}

fn validate_task_list_query(mut query: TaskListQuery) -> Result<TaskListQuery, String> {
    if query.cursor.is_some_and(|cursor| cursor <= 0) {
        return Err("cursor must be a positive task id".into());
    }
    if query.limit.is_some_and(|limit| limit == 0) {
        return Err("limit must be positive".into());
    }
    query.limit = Some(
        query
            .limit
            .unwrap_or(TASK_LIST_DEFAULT_LIMIT)
            .min(TASK_LIST_MAX_LIMIT),
    );
    for (name, value) in [
        ("status", query.status.as_deref()),
        ("label", query.label.as_deref()),
        ("assignee", query.assignee.as_deref()),
    ] {
        if value.is_some_and(|value| {
            value.is_empty() || value.contains('\0') || value.chars().count() > MAX_QUERY_TEXT_CHARS
        }) {
            return Err(format!("invalid {name} filter"));
        }
    }
    if let Some(status) = query.status.as_deref() {
        const TASK_STATUSES: &[&str] = &[
            "open",
            "planning",
            "decomposed",
            "working",
            "in-review",
            "rework",
            "merging",
            "done",
            "failed",
            "cancelled",
            "closed",
        ];
        if !TASK_STATUSES.contains(&status) {
            return Err("invalid status filter".into());
        }
    }
    Ok(query)
}

/// Opens and closes a connection for one detail request. All historical collections are read
/// newest-first with a fixed SQL limit before being serialized back into chronological order.
fn task_detail_payload(state: &AppState, id: i64) -> quorum_core::error::Result<Option<Value>> {
    let now = quorum_core::clock::now();
    let conn = quorum_core::db::open(&state.db_path)?;
    let Some(task) = quorum_core::tasks::get(&conn, id)? else {
        drop(conn);
        return Ok(None);
    };
    let progress = quorum_core::stats::task_progress(&conn, id, now)?;
    let detail = task_detail_value(&conn, &task)?;
    let timeline = task_events(&conn, id, now)?;
    let notes = task_notes(&conn, id)?;
    let runs = task_runs(&conn, id)?;
    drop(conn);
    Ok(Some(json!({
        "task": detail,
        "progress": progress,
        "timeline": timeline,
        "notes": notes,
        "runs": runs,
    })))
}

fn task_detail_value(
    conn: &rusqlite::Connection,
    task: &quorum_core::tasks::Task,
) -> quorum_core::error::Result<Value> {
    let refs = refs_value(task.refs.as_deref());
    let dependencies = task
        .depends_on
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok())
        .unwrap_or_default()
        .into_iter()
        .take(DETAIL_DEPENDENCY_LIMIT)
        .collect::<Vec<_>>();
    let generated_children = generated_children(conn, task.id)?;
    let generated_from = generated_from(conn, task.id)?;
    let failure_reason = [
        "failure_reason",
        "daemon_failure_reason",
        "provider_error",
        "codex_provider_error",
    ]
    .into_iter()
    .find_map(|key| ref_string(&refs, key));
    Ok(json!({
        "id": task.id,
        "title": bounded_text(&task.title, MAX_TITLE_CHARS),
        "body": bounded_option(task.body.as_deref(), MAX_BODY_CHARS),
        "status": bounded_text(&task.status, MAX_IDENTITY_CHARS),
        "labels": bounded_labels(task.labels.as_deref()),
        "priority": task.priority,
        "assignee": bounded_option(task.assignee.as_deref(), MAX_IDENTITY_CHARS),
        "created_by": bounded_text(&task.created_by, MAX_IDENTITY_CHARS),
        "author": bounded_option(task.author.as_deref(), MAX_IDENTITY_CHARS),
        "reviewer": bounded_option(task.reviewer.as_deref(), MAX_IDENTITY_CHARS),
        "created_at": task.created_at,
        "updated_at": task.updated_at,
        "timestamps": {"created_at": task.created_at, "updated_at": task.updated_at},
        "branch": task_branch(task, &refs),
        "pr": quorum_core::tasks::extract_pr_number(&task.refs),
        "readiness": {
            "ready": task.ready,
            "classification_ready": refs.get("cx_ready").and_then(Value::as_bool),
            "reason": ref_string(&refs, "cx_not_ready_reason"),
        },
        "rework": {"round": task.rework_round, "cap": task.effective_rework_cap()},
        "failure_reason": failure_reason,
        "park_reason": ref_string(&refs, quorum_core::tasks::PARKED_REASON_REF),
        "dependencies": dependencies,
        "generated_children": generated_children,
        "generated_from": generated_from,
    }))
}

fn generated_children(
    conn: &rusqlite::Connection,
    task_id: i64,
) -> quorum_core::error::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT m.task_id, m.local_key, t.title, t.status
         FROM task_decompositions d
         JOIN task_graph_members m ON m.graph_id=d.id
         JOIN tasks t ON t.id=m.task_id
         WHERE d.source_task_id=?1 AND m.active=1
         ORDER BY m.local_key, m.task_id LIMIT ?2",
    )?;
    let children = stmt
        .query_map(rusqlite::params![task_id, DETAIL_CHILD_LIMIT], |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "key": bounded_text(&row.get::<_, String>(1)?, MAX_IDENTITY_CHARS),
                "title": bounded_text(&row.get::<_, String>(2)?, MAX_TITLE_CHARS),
                "status": bounded_text(&row.get::<_, String>(3)?, MAX_IDENTITY_CHARS),
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(children)
}

fn generated_from(
    conn: &rusqlite::Connection,
    task_id: i64,
) -> quorum_core::error::Result<Option<Value>> {
    conn.query_row(
        "SELECT d.id, d.source_task_id, source.title, source.status, m.local_key
         FROM task_graph_members m
         JOIN task_decompositions d ON d.id=m.graph_id
         JOIN tasks source ON source.id=d.source_task_id
         WHERE m.task_id=?1 ORDER BY m.active DESC, d.id DESC LIMIT 1",
        [task_id],
        |row| {
            Ok(json!({
                "graph_id": row.get::<_, i64>(0)?,
                "task_id": row.get::<_, i64>(1)?,
                "title": bounded_text(&row.get::<_, String>(2)?, MAX_TITLE_CHARS),
                "status": bounded_text(&row.get::<_, String>(3)?, MAX_IDENTITY_CHARS),
                "key": bounded_text(&row.get::<_, String>(4)?, MAX_IDENTITY_CHARS),
            }))
        },
    )
    .optional()
    .map_err(Into::into)
}

fn task_events(
    conn: &rusqlite::Connection,
    task_id: i64,
    now: i64,
) -> quorum_core::error::Result<Vec<Value>> {
    let subject = format!("task#{task_id}");
    let mut stmt = conn.prepare(
        "SELECT seq, ts, kind, subject, body FROM (
             SELECT seq, ts, kind, subject, body FROM events
             WHERE subject=?1 AND expires_at > ?2
             ORDER BY seq DESC LIMIT ?3
         ) ORDER BY seq ASC",
    )?;
    let events = stmt
        .query_map(rusqlite::params![subject, now, DETAIL_EVENT_LIMIT], |row| {
            Ok(json!({
                "seq": row.get::<_, i64>(0)?,
                "ts": row.get::<_, i64>(1)?,
                "kind": bounded_text(&row.get::<_, String>(2)?, MAX_IDENTITY_CHARS),
                "subject": bounded_text(&row.get::<_, String>(3)?, MAX_IDENTITY_CHARS),
                "body": bounded_text(&row.get::<_, String>(4)?, MAX_DETAIL_TEXT_CHARS),
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(events)
}

fn task_notes(conn: &rusqlite::Connection, task_id: i64) -> quorum_core::error::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT id, ts, agent, body FROM (
             SELECT id, ts, agent, body FROM task_notes WHERE task_id=?1
             ORDER BY id DESC LIMIT ?2
         ) ORDER BY id ASC",
    )?;
    let notes = stmt
        .query_map(rusqlite::params![task_id, DETAIL_NOTE_LIMIT], |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "ts": row.get::<_, i64>(1)?,
                "agent": bounded_text(&row.get::<_, String>(2)?, MAX_IDENTITY_CHARS),
                "body": bounded_text(&row.get::<_, String>(3)?, MAX_DETAIL_TEXT_CHARS),
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(notes)
}

fn task_runs(conn: &rusqlite::Connection, task_id: i64) -> quorum_core::error::Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT id, agent_name, role, sub_role, model, effort, provider, spawned_at, ended_at, end_reason FROM (
             SELECT id, agent_name, role, sub_role, model, effort, provider, spawned_at, ended_at, end_reason
             FROM agent_runs WHERE task_id=?1 ORDER BY spawned_at DESC, id DESC LIMIT ?2
         ) ORDER BY spawned_at ASC, id ASC",
    )?;
    let runs = stmt
        .query_map(rusqlite::params![task_id, DETAIL_RUN_LIMIT], |row| {
        Ok(json!({
            "id": row.get::<_, i64>(0)?,
            "agent": bounded_text(&row.get::<_, String>(1)?, MAX_IDENTITY_CHARS),
            "role": bounded_text(&row.get::<_, String>(2)?, MAX_IDENTITY_CHARS),
            "sub_role": row.get::<_, Option<String>>(3)?.map(|value| bounded_text(&value, MAX_IDENTITY_CHARS)),
            "model": bounded_text(&row.get::<_, String>(4)?, MAX_IDENTITY_CHARS),
            "effort": bounded_text(&row.get::<_, String>(5)?, MAX_IDENTITY_CHARS),
            "provider": row.get::<_, Option<String>>(6)?.map(|value| bounded_text(&value, MAX_IDENTITY_CHARS)),
            "spawned_at": row.get::<_, i64>(7)?,
            "ended_at": row.get::<_, Option<i64>>(8)?,
            "end_reason": row.get::<_, Option<String>>(9)?.map(|value| bounded_text(&value, MAX_DETAIL_TEXT_CHARS)),
        }))
    })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(runs)
}

#[derive(Deserialize)]
struct RunsQuery {
    before: Option<String>,
    limit: Option<usize>,
}

async fn api_runs(State(state): State<AppState>, Query(query): Query<RunsQuery>) -> Response {
    match list_runs(
        &state.logs_root,
        query.before.as_deref(),
        query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT),
    ) {
        Ok(page) => {
            Json(json!({"runs": page.runs, "next_before": page.next_before})).into_response()
        }
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

struct RunPage {
    runs: Vec<Value>,
    next_before: Option<String>,
}

fn run_entry(name: String) -> Option<(i64, String)> {
    let epoch = name.rsplit_once('-')?.1.parse::<i64>().ok()?;
    Some((epoch, name))
}

fn list_runs(root: &FsPath, before: Option<&str>, limit: usize) -> std::io::Result<RunPage> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RunPage {
                runs: Vec::new(),
                next_before: None,
            })
        }
        Err(error) => return Err(error),
    };
    let cursor = before.and_then(|value| run_entry(value.to_owned()));
    // `read_dir` has no ordering guarantee. Scan names (never metadata) but retain only
    // this page's newest candidates, so pagination is complete without unbounded memory.
    // Keep one extra candidate: its presence is what proves an older page exists, so the
    // last real page reports `next_before: None` instead of pointing at an empty page.
    let mut dirs: BinaryHeap<Reverse<(i64, String)>> = BinaryHeap::with_capacity(limit + 2);
    for entry in entries.filter_map(Result::ok) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !entry.file_type().ok().is_some_and(|kind| kind.is_dir()) {
            continue;
        }
        let Some(entry) = run_entry(name) else {
            continue;
        };
        if !cursor
            .as_ref()
            .map(|cursor| entry.cmp(cursor).is_lt())
            .unwrap_or(true)
        {
            continue;
        }
        dirs.push(Reverse(entry));
        if dirs.len() > limit + 1 {
            dirs.pop();
        }
    }
    let mut selected: Vec<_> = dirs.into_iter().map(|Reverse(entry)| entry).collect();
    selected.sort_unstable_by(|a, b| b.cmp(a));
    let has_older = selected.len() > limit;
    selected.truncate(limit);
    let next_before = has_older
        .then(|| selected.last().map(|(_, dir)| dir.clone()))
        .flatten();
    let runs = selected
        .into_iter()
        .map(|(epoch, dir)| {
            let meta = fs::read_to_string(root.join(&dir).join("meta.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .unwrap_or(Value::Null);
            Ok(json!({"dir": dir, "epoch": epoch, "meta": meta}))
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    Ok(RunPage { runs, next_before })
}

#[derive(Deserialize)]
struct StreamQuery {
    from: Option<u64>,
    max: Option<u64>,
}

async fn api_stream(
    State(state): State<AppState>,
    Path(dir): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Response {
    let max = query
        .max
        .unwrap_or(DEFAULT_STREAM_BYTES)
        .min(MAX_STREAM_BYTES);
    match stream_payload(&state.logs_root, &dir, query.from, max) {
        Ok(value) => Json(value).into_response(),
        Err(StreamError::BadPath) => {
            (StatusCode::BAD_REQUEST, "invalid run directory").into_response()
        }
        Err(StreamError::Io(error)) => (StatusCode::NOT_FOUND, error.to_string()).into_response(),
    }
}

async fn api_transcript(
    State(state): State<AppState>,
    Path(dir): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Response {
    let max = query
        .max
        .unwrap_or(DEFAULT_STREAM_BYTES)
        .min(MAX_STREAM_BYTES);
    match text_payload(&state.logs_root, &dir, "transcript.md", query.from, max) {
        Ok(value) => Json(value).into_response(),
        Err(StreamError::BadPath) => {
            (StatusCode::BAD_REQUEST, "invalid run directory").into_response()
        }
        Err(StreamError::Io(error)) => (StatusCode::NOT_FOUND, error.to_string()).into_response(),
    }
}

#[derive(Debug)]
enum StreamError {
    BadPath,
    Io(std::io::Error),
}

fn run_dir(root: &FsPath, dir: &str) -> Result<PathBuf, StreamError> {
    if dir.contains('/') || dir.contains("..") || dir.contains('\0') {
        return Err(StreamError::BadPath);
    }
    let root = root.canonicalize().map_err(StreamError::Io)?;
    let path = root.join(dir).canonicalize().map_err(StreamError::Io)?;
    if !path.starts_with(&root) || !path.is_dir() {
        return Err(StreamError::BadPath);
    }
    Ok(path)
}

fn stream_payload(
    root: &FsPath,
    dir: &str,
    from: Option<u64>,
    max: u64,
) -> Result<Value, StreamError> {
    let path = run_dir(root, dir)?.join("stream.jsonl");
    let mut file = File::open(&path).map_err(StreamError::Io)?;
    let len = file.metadata().map_err(StreamError::Io)?.len();
    let start = from
        .unwrap_or_else(|| len.saturating_sub(DEFAULT_STREAM_BYTES))
        .min(len);
    // A nonzero initial offset is not necessarily in the middle of a record: it can
    // land immediately after a newline. Only discard the first chunk when the byte
    // before the tail window proves it is a fragment.
    let starts_mid_line = if from.is_none() && start > 0 {
        file.seek(SeekFrom::Start(start - 1))
            .map_err(StreamError::Io)?;
        let mut previous = [0_u8; 1];
        file.read_exact(&mut previous).map_err(StreamError::Io)?;
        previous[0] != b'\n'
    } else {
        false
    };
    file.seek(SeekFrom::Start(start)).map_err(StreamError::Io)?;
    let mut bytes = vec![0; max as usize];
    let read = file.read(&mut bytes).map_err(StreamError::Io)?;
    bytes.truncate(read);
    let next = start + read as u64;
    let complete_records = bytes.iter().filter(|byte| **byte == b'\n').count();
    let partial = (!bytes.ends_with(b"\n")).then(|| {
        let start = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        hex_bytes(&bytes[start..])
    });
    let retained_start = if complete_records > MAX_STREAM_RECORDS {
        let skipped = complete_records - MAX_STREAM_RECORDS + 1;
        bytes
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'\n')
            .nth(skipped - 1)
            .map_or(0, |(index, _)| index + 1)
    } else {
        0
    };
    let mut lines = Vec::with_capacity(complete_records.min(MAX_STREAM_RECORDS));
    if complete_records > MAX_STREAM_RECORDS {
        let first_end = bytes.iter().position(|byte| *byte == b'\n').unwrap();
        lines.push(hex_bytes(&bytes[..first_end]));
    }
    let mut line_start = retained_start;
    for (index, byte) in bytes.iter().enumerate().skip(retained_start) {
        if *byte == b'\n' {
            lines.push(hex_bytes(&bytes[line_start..index]));
            line_start = index + 1;
        }
    }
    let omitted = complete_records.saturating_sub(lines.len());
    // The initial tail can begin in the middle of a record. The client discards that
    // first completed fragment before it begins retaining suffixes for later requests.
    Ok(
        json!({"lines": lines, "omitted": omitted, "partial": partial, "starts_mid_line": starts_mid_line,
        "next_offset": next, "eof": next >= len}),
    )
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 15) as usize] as char);
    }
    encoded
}

fn text_payload(
    root: &FsPath,
    dir: &str,
    filename: &str,
    from: Option<u64>,
    max: u64,
) -> Result<Value, StreamError> {
    let path = run_dir(root, dir)?.join(filename);
    let mut file = File::open(&path).map_err(StreamError::Io)?;
    let len = file.metadata().map_err(StreamError::Io)?.len();
    let start = from
        .unwrap_or_else(|| len.saturating_sub(DEFAULT_STREAM_BYTES))
        .min(len);
    file.seek(SeekFrom::Start(start)).map_err(StreamError::Io)?;
    let mut bytes = vec![0; max as usize];
    let read = file.read(&mut bytes).map_err(StreamError::Io)?;
    bytes.truncate(read);
    let next = start + read as u64;
    Ok(json!({
        "text": String::from_utf8_lossy(&bytes),
        "next_offset": next,
        "eof": next >= len,
    }))
}

fn server_error(error: quorum_core::error::QuorumError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": bounded_text(&error.to_string(), MAX_DETAIL_TEXT_CHARS)})),
    )
        .into_response()
}

fn bad_request(message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": bounded_text(message, MAX_DETAIL_TEXT_CHARS)})),
    )
        .into_response()
}

fn not_found(message: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": bounded_text(message, MAX_DETAIL_TEXT_CHARS)})),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_traversal() {
        let root = tempfile::tempdir().unwrap();
        assert!(matches!(
            run_dir(root.path(), "../oops"),
            Err(StreamError::BadPath)
        ));
    }

    #[test]
    fn rejects_non_loopback_bind() {
        assert!(validate_loopback("0.0.0.0:8080".parse().unwrap()).is_err());
    }

    #[test]
    fn accepts_ipv6_loopback_bind() {
        let ip: IpAddr = "::1".parse().unwrap();
        assert!(validate_loopback(SocketAddr::new(ip, 8080)).is_ok());
    }

    #[test]
    fn page_never_interpolates_stored_values_as_html_or_inline_handlers() {
        assert!(!PAGE.contains("innerHTML"));
        assert!(!PAGE.contains(" onclick="));
        assert!(!CLIENT.contains("innerHTML"));
        assert!(CLIENT.contains("textContent"));
        assert!(CLIENT.contains("MAX_RENDERED_TAIL_CHARS"));
        assert!(CLIENT.contains("slice(-MAX_RENDERED_TAIL_CHARS)"));
    }

    #[test]
    fn client_pure_functions_pass_their_node_tests() {
        // The documented workflow installs only Rust. Node is a bonus gate where it exists
        // (CI has it), never a hard prerequisite of `cargo test`.
        let output = match std::process::Command::new("node")
            .arg("quorum/src/web.test.js")
            .current_dir(env!("CARGO_MANIFEST_DIR").rsplit_once('/').unwrap().0)
            .output()
        {
            Ok(output) => output,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("skipping web client tests: node not installed");
                return;
            }
            Err(error) => panic!("failed to run the web client tests: {error}"),
        };
        assert!(
            output.status.success(),
            "web client tests failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn stream_offsets_only_return_new_bytes() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("A-100");
        fs::create_dir(&dir).unwrap();
        fs::write(dir.join("stream.jsonl"), "one\ntwo\n").unwrap();
        let first = stream_payload(root.path(), "A-100", Some(0), 4).unwrap();
        let next = first["next_offset"].as_u64().unwrap();
        let second = stream_payload(root.path(), "A-100", Some(next), 20).unwrap();
        assert_eq!(second["lines"], json!(["74776f"]));
    }

    #[test]
    fn stream_payload_keeps_a_record_spanning_the_byte_cap_as_a_suffix() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("A-100");
        fs::create_dir(&dir).unwrap();
        let record = format!(
            "{{\"type\":\"assistant\",\"message\":{{\"content\":\"{}\"}}}}\n",
            "x".repeat(DEFAULT_STREAM_BYTES as usize + 128)
        );
        fs::write(dir.join("stream.jsonl"), &record).unwrap();

        let first = stream_payload(root.path(), "A-100", Some(0), DEFAULT_STREAM_BYTES).unwrap();
        assert_eq!(first["lines"], json!([]));
        assert_eq!(
            first["partial"].as_str().unwrap().len(),
            DEFAULT_STREAM_BYTES as usize * 2
        );

        let second = stream_payload(
            root.path(),
            "A-100",
            first["next_offset"].as_u64(),
            DEFAULT_STREAM_BYTES,
        )
        .unwrap();
        let reassembled = format!(
            "{}{}",
            first["partial"].as_str().unwrap(),
            second["lines"][0].as_str().unwrap()
        );
        assert_eq!(reassembled, hex_bytes(record.trim_end().as_bytes()));
    }

    #[test]
    fn stream_payload_bounds_dense_record_fanout_before_json() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("A-100");
        fs::create_dir(&dir).unwrap();
        let record_count = MAX_STREAM_RECORDS * 3;
        fs::write(dir.join("stream.jsonl"), "{}\n".repeat(record_count)).unwrap();

        let payload = stream_payload(root.path(), "A-100", Some(0), DEFAULT_STREAM_BYTES).unwrap();
        let lines = payload["lines"].as_array().unwrap();
        assert_eq!(lines.len(), MAX_STREAM_RECORDS);
        assert_eq!(payload["omitted"], json!(record_count - MAX_STREAM_RECORDS));
        assert_eq!(lines.first().unwrap(), "7b7d");
        assert_eq!(lines.last().unwrap(), "7b7d");
        assert_eq!(payload["next_offset"], json!((record_count * 3) as u64));
    }

    #[test]
    fn initial_tail_at_a_record_boundary_keeps_its_first_record() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("A-100");
        fs::create_dir(&dir).unwrap();
        let record = format!(
            "{{\"type\":\"assistant\",\"message\":{{\"content\":\"{}\"}}}}\n",
            "x".repeat(DEFAULT_STREAM_BYTES as usize - 46)
        );
        assert_eq!(record.len(), DEFAULT_STREAM_BYTES as usize);
        fs::write(dir.join("stream.jsonl"), format!("discarded\n{record}")).unwrap();

        let tail = stream_payload(root.path(), "A-100", None, DEFAULT_STREAM_BYTES).unwrap();
        assert_eq!(tail["starts_mid_line"], json!(false));
        assert_eq!(
            tail["lines"],
            json!([hex_bytes(record.trim_end().as_bytes())])
        );
    }

    #[test]
    fn transcript_offsets_only_return_new_text() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("A-100");
        fs::create_dir(&dir).unwrap();
        fs::write(dir.join("transcript.md"), "first\nsecond\n").unwrap();
        let first = text_payload(root.path(), "A-100", "transcript.md", Some(0), 6).unwrap();
        assert_eq!(first["text"], "first\n");
        let next = first["next_offset"].as_u64().unwrap();
        let second = text_payload(root.path(), "A-100", "transcript.md", Some(next), 20).unwrap();
        assert_eq!(second["text"], "second\n");
    }

    #[test]
    fn state_opens_a_short_lived_connection_and_serializes_json() {
        let temp = tempfile::tempdir().unwrap();
        let db = temp.path().join("q.db");
        let conn = quorum_core::db::open(&db).unwrap();
        drop(conn);
        let state = AppState {
            db_path: db.clone(),
            logs_root: temp.path().join("logs"),
            online_window: 900,
        };
        let payload = state_payload(&state).unwrap();
        assert!(payload.get("tasks").is_some());
        assert_eq!(payload["health"]["stale_after_secs"], json!(10));
        assert!(payload["queue_bands"]["ready"].is_object());
        assert!(payload["needs_attention"].is_object());
        // The handler owns no connection; a checkpoint can immediately truncate a WAL.
        let conn = quorum_core::db::open(&db).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
        assert_eq!(
            fs::metadata(format!("{}-wal", db.display()))
                .map(|m| m.len())
                .unwrap_or(0),
            0
        );
    }

    fn test_state(temp: &tempfile::TempDir) -> AppState {
        AppState {
            db_path: temp.path().join("q.db"),
            logs_root: temp.path().join("logs"),
            online_window: 900,
        }
    }

    fn create_test_task(
        conn: &mut rusqlite::Connection,
        title: &str,
        labels: Option<&str>,
        depends_on: Option<&str>,
    ) -> i64 {
        quorum_core::tasks::create(
            conn,
            "owner",
            title,
            None,
            1,
            labels,
            None,
            depends_on,
            None,
            quorum_core::clock::now(),
        )
        .unwrap()
    }

    fn assert_checkpoint_released(db: &FsPath) {
        let conn = quorum_core::db::open(db).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        drop(conn);
        assert_eq!(
            fs::metadata(format!("{}-wal", db.display()))
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn state_uses_the_claimable_projection_and_surfaces_dependency_blocks() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let mut conn = quorum_core::db::open(&state.db_path).unwrap();
        let ready = create_test_task(&mut conn, "ready", Some(r#"["kind:ready"]"#), None);
        conn.execute(
            "UPDATE tasks SET refs=?1 WHERE id=?2",
            rusqlite::params![
                r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null}"#,
                ready
            ],
        )
        .unwrap();
        let blocked = create_test_task(
            &mut conn,
            "blocked",
            Some(r#"["kind:blocked"]"#),
            Some(&format!("[{ready}]")),
        );
        drop(conn);

        let payload = state_payload(&state).unwrap();
        let ready_ids = payload["queue_bands"]["ready"]["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|task| task["id"].as_i64())
            .collect::<Vec<_>>();
        assert!(ready_ids.contains(&ready));
        assert!(!ready_ids.contains(&blocked));
        assert_eq!(
            payload["needs_attention"]["blocked_tasks"]["count"],
            json!(1)
        );
        assert_checkpoint_released(&state.db_path);
    }

    #[test]
    fn state_ready_band_uses_the_limited_ready_projection() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let mut conn = quorum_core::db::open(&state.db_path).unwrap();
        for index in 0..=STATE_BAND_LIMIT {
            let id = create_test_task(&mut conn, &format!("ready-{index}"), None, None);
            conn.execute(
                "UPDATE tasks SET refs=?1 WHERE id=?2",
                rusqlite::params![
                    r#"{"cx_est":2,"cx_size":"S","cx_ready":true,"cx_not_ready_reason":null}"#,
                    id
                ],
            )
            .unwrap();
        }
        drop(conn);

        let payload = state_payload(&state).unwrap();
        assert_eq!(
            payload["queue_bands"]["ready"]["tasks"]
                .as_array()
                .unwrap()
                .len(),
            STATE_BAND_LIMIT
        );
        assert_eq!(
            payload["queue_bands"]["ready"]["count"],
            json!(STATE_BAND_LIMIT)
        );
        assert_checkpoint_released(&state.db_path);
    }

    #[test]
    fn task_list_paginates_filters_and_releases_its_connection() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let mut conn = quorum_core::db::open(&state.db_path).unwrap();
        let first = create_test_task(&mut conn, "first", Some(r#"["kind:keep"]"#), None);
        let second = create_test_task(&mut conn, "second", Some(r#"["kind:keep"]"#), None);
        let third = create_test_task(&mut conn, "third", Some(r#"["kind:skip"]"#), None);
        drop(conn);

        let page = task_list_payload(
            &state,
            TaskListQuery {
                limit: Some(2),
                ..TaskListQuery::default()
            },
        )
        .unwrap();
        assert_eq!(page["tasks"][0]["id"], json!(third));
        assert_eq!(page["tasks"][1]["id"], json!(second));
        assert_eq!(page["next_cursor"], json!(second));

        let older = task_list_payload(
            &state,
            TaskListQuery {
                cursor: Some(second),
                limit: Some(2),
                label: Some("kind:keep".into()),
                ..TaskListQuery::default()
            },
        )
        .unwrap();
        assert_eq!(older["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(older["tasks"][0]["id"], json!(first));
        assert_eq!(older["tasks"][0]["labels"], json!(["kind:keep"]));
        assert_eq!(older["next_cursor"], Value::Null);
        assert!(validate_task_list_query(TaskListQuery {
            limit: Some(0),
            ..TaskListQuery::default()
        })
        .is_err());
        assert_checkpoint_released(&state.db_path);
    }

    #[test]
    fn task_detail_is_bounded_and_keeps_untrusted_content_as_json() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let mut conn = quorum_core::db::open(&state.db_path).unwrap();
        let title = "</script><img src=x onerror=alert(1)>";
        let body = "x".repeat(MAX_BODY_CHARS + 100);
        let task_id = quorum_core::tasks::create(
            &mut conn,
            "owner",
            title,
            Some(&body),
            1,
            Some(r#"["kind:detail"]"#),
            None,
            Some("[1,2,3]"),
            None,
            quorum_core::clock::now(),
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET refs=?1 WHERE id=?2",
            rusqlite::params![r#"{"pr":42,"branch":"feature/detail","cx_ready":false,"cx_not_ready_reason":"needs <review>","daemon_parked_reason":"waiting for <owner>"}"#, task_id],
        )
        .unwrap();
        let now = quorum_core::clock::now();
        for index in 0..(DETAIL_NOTE_LIMIT + 4) {
            conn.execute(
                "INSERT INTO task_notes(task_id,ts,agent,body) VALUES (?1,?2,'agent',?3)",
                rusqlite::params![task_id, now + index, format!("note {index} <unsafe>")],
            )
            .unwrap();
        }
        for index in 0..(DETAIL_EVENT_LIMIT + 4) {
            quorum_core::events::emit(
                &conn,
                "task_changed",
                &format!("task#{task_id}"),
                &format!("event {index} <unsafe>"),
                now + index,
            )
            .unwrap();
        }
        for index in 0..(DETAIL_RUN_LIMIT + 4) {
            quorum_core::agent_runs::insert(
                &conn,
                task_id,
                "agent",
                "worker",
                "model",
                "high",
                "codex",
                now + index,
            )
            .unwrap();
        }
        drop(conn);

        let payload = task_detail_payload(&state, task_id).unwrap().unwrap();
        assert_eq!(payload["task"]["title"], json!(title));
        assert!(payload["task"]["body"].as_str().unwrap().chars().count() <= MAX_BODY_CHARS);
        assert_eq!(payload["task"]["pr"], json!(42));
        assert_eq!(payload["task"]["branch"], json!("feature/detail"));
        assert_eq!(
            payload["task"]["readiness"]["reason"],
            json!("needs <review>")
        );
        assert_eq!(payload["task"]["park_reason"], json!("waiting for <owner>"));
        assert_eq!(payload["task"]["dependencies"], json!([1, 2, 3]));
        assert!(payload["progress"].is_object());
        assert_eq!(
            payload["timeline"].as_array().unwrap().len(),
            DETAIL_EVENT_LIMIT as usize
        );
        assert_eq!(
            payload["notes"].as_array().unwrap().len(),
            DETAIL_NOTE_LIMIT as usize
        );
        assert_eq!(
            payload["runs"].as_array().unwrap().len(),
            DETAIL_RUN_LIMIT as usize
        );
        assert_checkpoint_released(&state.db_path);
    }

    #[test]
    fn task_detail_includes_bounded_generated_task_references() {
        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let mut conn = quorum_core::db::open(&state.db_path).unwrap();
        let source = create_test_task(&mut conn, "source", None, None);
        let child = create_test_task(&mut conn, "child", None, None);
        let now = quorum_core::clock::now();
        conn.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,created_at,updated_at
             ) VALUES (?1,'active',1,0,1,?2,?2)",
            rusqlite::params![source, now],
        )
        .unwrap();
        let graph = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
             VALUES (?1,?2,'child-a',1,1)",
            rusqlite::params![graph, child],
        )
        .unwrap();
        drop(conn);

        let source_payload = task_detail_payload(&state, source).unwrap().unwrap();
        assert_eq!(
            source_payload["task"]["generated_children"][0]["id"],
            json!(child)
        );
        let child_payload = task_detail_payload(&state, child).unwrap().unwrap();
        assert_eq!(
            child_payload["task"]["generated_from"]["task_id"],
            json!(source)
        );
        assert_checkpoint_released(&state.db_path);
    }

    #[tokio::test]
    async fn task_handler_returns_safe_json_for_invalid_and_missing_ids() {
        async fn response_json(response: Response) -> Value {
            let bytes = axum::body::to_bytes(response.into_body(), 8 * 1024)
                .await
                .unwrap();
            serde_json::from_slice(&bytes).unwrap()
        }

        let temp = tempfile::tempdir().unwrap();
        let state = test_state(&temp);
        let conn = quorum_core::db::open(&state.db_path).unwrap();
        drop(conn);

        let invalid = api_task(
            State(state.clone()),
            Ok::<_, PathRejection>(Path("not-a-task".into())),
        )
        .await;
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(invalid).await["error"],
            json!("invalid task id")
        );

        let bad_query = api_tasks(
            State(state.clone()),
            Ok::<_, QueryRejection>(Query(TaskListQuery {
                limit: Some(0),
                ..TaskListQuery::default()
            })),
        )
        .await;
        assert_eq!(bad_query.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(bad_query).await["error"],
            json!("limit must be positive")
        );

        let missing = api_task(State(state), Ok::<_, PathRejection>(Path("999".into()))).await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_json(missing).await["error"],
            json!("task not found")
        );
    }

    #[test]
    fn run_listing_is_empty_when_the_configured_root_is_absent() {
        let root = tempfile::tempdir().unwrap().path().join("absent");
        assert!(list_runs(&root, None, 50).unwrap().runs.is_empty());
    }

    #[test]
    fn run_pages_are_chronological_and_cursor_complete() {
        let root = tempfile::tempdir().unwrap();
        for dir in ["zeta-100", "alpha-300", "beta-200", "gamma-200"] {
            fs::create_dir(root.path().join(dir)).unwrap();
        }
        let first = list_runs(root.path(), None, 2).unwrap();
        assert_eq!(first.runs[0]["dir"], "alpha-300");
        assert_eq!(first.runs[1]["dir"], "gamma-200");
        assert_eq!(first.next_before.as_deref(), Some("gamma-200"));
        let second = list_runs(root.path(), first.next_before.as_deref(), 2).unwrap();
        assert_eq!(second.runs[0]["dir"], "beta-200");
        assert_eq!(second.runs[1]["dir"], "zeta-100");
        // The final page must not advertise a cursor; following it lands on an empty page
        // with no in-view route back to the newest runs.
        assert_eq!(second.next_before, None);
    }

    #[test]
    fn an_exactly_full_final_page_reports_no_cursor() {
        let root = tempfile::tempdir().unwrap();
        for dir in ["alpha-300", "beta-200"] {
            fs::create_dir(root.path().join(dir)).unwrap();
        }
        let page = list_runs(root.path(), None, 2).unwrap();
        assert_eq!(page.runs.len(), 2);
        assert_eq!(page.next_before, None);
    }

    #[test]
    fn run_page_considers_entries_beyond_the_old_scan_cap() {
        let root = tempfile::tempdir().unwrap();
        for epoch in 0..1_002 {
            fs::create_dir(root.path().join(format!("run-{epoch}"))).unwrap();
        }
        let page = list_runs(root.path(), None, 1).unwrap();
        assert_eq!(page.runs[0]["dir"], "run-1001");
    }
}
