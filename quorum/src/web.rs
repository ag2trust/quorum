//! Read-only local dashboard. Connections are intentionally request-scoped so a browser
//! left open for days never pins SQLite's WAL through a held read transaction.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
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
const DASHBOARD_TASK_LIMIT: i64 = 100;
const DASHBOARD_AGENT_LIMIT: i64 = 100;

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
        .route("/api/runs", get(api_runs))
        .route("/api/runs/:dir/stream", get(api_stream))
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
    let tasks = quorum_core::tasks::list_limited(&conn, DASHBOARD_TASK_LIMIT)?;
    let roster = quorum_core::agents::roster_limited(
        &conn,
        now,
        state.online_window,
        DASHBOARD_AGENT_LIMIT,
    )?;
    let events = quorum_core::events::list(&conn, 0, None, 30, now)?;
    let mut held_by_agent = std::collections::HashMap::new();
    let tasks: Vec<Value> = tasks.into_iter().map(|task| -> quorum_core::error::Result<Value> {
        // One bounded task page; ask SQLite for one latest row rather than loading each
        // task's complete historical run list.
        let run = quorum_core::agent_runs::latest_for_task(&conn, task.id)?;
        let (provider, model) = run
            .map(|run| (run.provider, Some(run.model)))
            .unwrap_or((None, None));
        if let Some(agent) = task.assignee.as_ref() {
            held_by_agent.insert(agent.clone(), json!({"id": task.id, "title": task.title}));
        }
        let pr = task.refs.as_deref().and_then(|refs| serde_json::from_str::<Value>(refs).ok())
            .and_then(|refs| refs.get("pr").and_then(Value::as_i64));
        Ok(json!({"id": task.id, "title": task.title, "state": task.status, "provider": provider,
            "model": model, "pr": pr, "age_secs": (now - task.created_at).max(0),
            "priority": task.priority, "labels": task.labels, "assignee": task.assignee, "ready": task.ready}))
    }).collect::<quorum_core::error::Result<_>>()?;
    let agents: Vec<Value> = roster
        .into_iter()
        .map(|agent| {
            json!({"name": agent.id, "last_seen": agent.last_seen, "online": agent.online,
            "task_held": held_by_agent.get(&agent.id), "run_dir": Value::Null})
        })
        .collect();
    let counts = quorum_core::stats::web_task_counts(&conn)?;
    let alerts = quorum_core::stats::web_alerts(&conn, now)?;
    let errors = quorum_core::stats::web_recent_errors(&conn, now)?;
    drop(conn);
    Ok(
        json!({"now": now, "counts": counts, "tasks": tasks, "agents": agents,
        "recent_events": events, "alerts": alerts, "errors": errors}),
    )
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
    let mut dirs: BinaryHeap<Reverse<(i64, String)>> = BinaryHeap::with_capacity(limit + 1);
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
        if dirs.len() > limit {
            dirs.pop();
        }
    }
    let mut selected: Vec<_> = dirs.into_iter().map(|Reverse(entry)| entry).collect();
    selected.sort_unstable_by(|a, b| b.cmp(a));
    let next_before = selected.last().map(|(_, dir)| dir.clone());
    let runs = selected
        .into_iter()
        .take(limit)
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
    file.seek(SeekFrom::Start(start)).map_err(StreamError::Io)?;
    let mut bytes = vec![0; max as usize];
    let read = file.read(&mut bytes).map_err(StreamError::Io)?;
    bytes.truncate(read);
    let next = start + read as u64;
    let lines = String::from_utf8_lossy(&bytes)
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    Ok(json!({"lines": lines, "next_offset": next, "eof": next >= len}))
}

fn server_error(error: quorum_core::error::QuorumError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": error.to_string()})),
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
        let output = std::process::Command::new("node")
            .arg("quorum/src/web.test.js")
            .current_dir(env!("CARGO_MANIFEST_DIR").rsplit_once('/').unwrap().0)
            .output()
            .expect("node is required to run the web client unit tests");
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
        assert_eq!(second["lines"], json!(["two"]));
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
        let second = list_runs(root.path(), first.next_before.as_deref(), 2).unwrap();
        assert_eq!(second.runs[0]["dir"], "beta-200");
        assert_eq!(second.runs[1]["dir"], "zeta-100");
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
