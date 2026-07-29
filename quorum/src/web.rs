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
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
};

const PAGE: &str = include_str!("web.html");
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 100;
const DEFAULT_STREAM_BYTES: u64 = 2 * 1024 * 1024;
const MAX_STREAM_BYTES: u64 = 8 * 1024 * 1024;

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
    let addr: SocketAddr = format!("{bind}:{port}").parse().map_err(|e| {
        quorum_core::error::QuorumError::Usage(format!("invalid --bind/--port: {e}"))
    })?;
    let state = AppState {
        db_path,
        logs_root,
        online_window,
    };
    let app = Router::new()
        .route("/", get(index))
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

async fn index() -> Html<&'static str> {
    Html(PAGE)
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
    let mut snapshot = quorum_core::stats::stats(&conn, now, state.online_window)?;
    snapshot.daemon = quorum_core::daemon_lock::liveness(&conn, now, 30, super::pid_is_alive)?;
    let tasks = quorum_core::tasks::list(&conn, None, None, None)?;
    let roster = quorum_core::agents::roster(&conn, now, state.online_window)?;
    let events = quorum_core::events::list(&conn, 0, None, 30, now)?;
    let agents: Vec<Value> = roster
        .into_iter()
        .map(|agent| {
            let held = snapshot
                .agents
                .iter()
                .find(|a| a.id == agent.id)
                .and_then(|a| a.current_task.as_ref())
                .map(|task| json!({"id": task.id, "title": task.title}));
            let run_dir = snapshot
                .daemon_agents
                .iter()
                .find(|run| run.agent == agent.id)
                .and_then(|run| run.log_dir.as_deref())
                .and_then(|path| FsPath::new(path).file_name())
                .and_then(|name| name.to_str());
            json!({"name": agent.id, "last_seen": agent.last_seen, "online": agent.online,
            "task_held": held, "run_dir": run_dir})
        })
        .collect();
    let tasks: Vec<Value> = tasks.into_iter().map(|task| -> quorum_core::error::Result<Value> {
        let run = quorum_core::agent_runs::runs_for_task(&conn, task.id)?.pop();
        let (provider, model) = run.map(|r| (r.provider, Some(r.model))).unwrap_or((None, None));
        let pr = task.refs.as_deref().and_then(|refs| serde_json::from_str::<Value>(refs).ok())
            .and_then(|refs| refs.get("pr").and_then(Value::as_i64));
        Ok(json!({"id": task.id, "title": task.title, "state": task.status, "provider": provider,
            "model": model, "pr": pr, "age_secs": (now - task.created_at).max(0),
            "priority": task.priority, "labels": task.labels, "assignee": task.assignee, "ready": task.ready}))
    }).collect::<quorum_core::error::Result<_>>()?;
    drop(conn);
    Ok(
        json!({"now": now, "counts": snapshot.tasks, "tasks": tasks, "agents": agents,
        "recent_events": events, "alerts": snapshot.alerts, "errors": snapshot.recent_errors,
        "stats": snapshot}),
    )
}

#[derive(Deserialize)]
struct RunsQuery {
    before: Option<i64>,
    limit: Option<usize>,
}

async fn api_runs(State(state): State<AppState>, Query(query): Query<RunsQuery>) -> Response {
    match list_runs(
        &state.logs_root,
        query.before,
        query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT),
    ) {
        Ok(runs) => Json(json!({"runs": runs})).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

fn list_runs(root: &FsPath, before: Option<i64>, limit: usize) -> std::io::Result<Vec<Value>> {
    let mut dirs: Vec<(String, i64)> = fs::read_dir(root)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            entry.file_type().ok()?.is_dir().then_some(())?;
            let epoch = name.rsplit_once('-')?.1.parse::<i64>().ok()?;
            (before.map(|b| epoch < b).unwrap_or(true)).then_some((name, epoch))
        })
        .collect();
    dirs.sort_unstable_by(|a, b| b.0.cmp(&a.0));
    dirs.into_iter()
        .take(limit)
        .map(|(dir, epoch)| {
            let meta = fs::read_to_string(root.join(&dir).join("meta.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .unwrap_or(Value::Null);
            Ok(json!({"dir": dir, "epoch": epoch, "meta": meta}))
        })
        .collect()
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
}
