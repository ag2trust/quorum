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
    let live_agents = quorum_core::stats::web_daemon_agents(&conn, now)?
        .into_iter()
        .map(|agent| {
            let run_dir = agent
                .log_dir
                .as_deref()
                .and_then(|path| FsPath::new(path).file_name())
                .and_then(|name| name.to_str())
                .filter(|name| run_entry((*name).to_owned()).is_some());
            json!({
                "name": agent.agent,
                "role": agent.role,
                "task_id": agent.task_id,
                "task_title": agent.task_title,
                "phase": agent.phase,
                "provider": agent.provider,
                "model": agent.model,
                "effort": agent.effort,
                "pr": agent.pr,
                "run_dir": run_dir,
                "last_activity_age_secs": agent.last_activity_age_secs,
                "now": agent.now_label,
            })
        })
        .collect::<Vec<_>>();
    let counts = quorum_core::stats::web_task_counts(&conn)?;
    let alerts = quorum_core::stats::web_alerts(&conn, now)?;
    let errors = quorum_core::stats::web_recent_errors(&conn, now)?;
    drop(conn);
    Ok(
        json!({"now": now, "counts": counts, "tasks": tasks, "agents": agents,
        "live_agents": live_agents,
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
    let mut chunks = bytes.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    let partial = if bytes.ends_with(b"\n") {
        chunks.pop();
        None
    } else {
        chunks.pop().map(hex_bytes)
    };
    // The initial tail can begin in the middle of a record. The client discards that
    // first completed fragment before it begins retaining suffixes for later requests.
    Ok(
        json!({"lines": chunks.into_iter().map(hex_bytes).collect::<Vec<_>>(), "partial": partial, "starts_mid_line": starts_mid_line,
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
