#![cfg(unix)]

//! Real-process coverage for `quorum inference-forwarder`: run-capability
//! authority (against a real `quorum serve` endpoint and a stand-in), the
//! request policy, per-run token accounting, and upstream failure handling,
//! all against a local fake upstream. No real network and no real API key.

use rusqlite::params;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// The operator's upstream credential. It must reach the fake upstream and
/// nothing else: no response and no log line.
const UPSTREAM_KEY: &str = "upstream-secret-4f9a";
/// Text that stands for a prompt and a completion; neither may be logged.
const PROMPT: &str = "PROMPT-SENTINEL";
const COMPLETION: &str = "COMPLETION-SENTINEL";

fn quorum_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
}

// ---------------------------------------------------------------- upstream

#[derive(Debug, Clone)]
struct Seen {
    request_line: String,
    headers: Vec<(String, String)>,
    body: Value,
    raw_body: String,
}

impl Seen {
    fn header(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
            .collect()
    }
}

/// A local stand-in for the model upstream. Its behaviour is chosen by the
/// `model` of each request it receives.
struct FakeUpstream {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    /// Sending on this lets a `model-gated-stream` response finish.
    release: mpsc::Sender<()>,
}

fn sse(event: &str, data: Value) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

fn stream_start() -> String {
    sse(
        "message_start",
        json!({"type": "message_start", "message": {"id": "msg_1", "usage":
            {"input_tokens": 7, "cache_read_input_tokens": 3, "output_tokens": 1}}}),
    )
}

fn stream_rest() -> String {
    sse(
        "content_block_delta",
        json!({"type": "content_block_delta", "index": 0,
               "delta": {"type": "text_delta", "text": COMPLETION}}),
    ) + &sse(
        "message_delta",
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"},
               "usage": {"output_tokens": 20}}),
    ) + &sse("message_stop", json!({"type": "message_stop"}))
}

fn write_chunk(stream: &mut TcpStream, data: &str) {
    let _ = write!(stream, "{:x}\r\n{data}\r\n", data.len());
    let _ = stream.flush();
}

const STREAM_HEAD: &str = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
    Transfer-Encoding: chunked\r\nrequest-id: req_stream\r\n\r\n";

impl FakeUpstream {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (release, gate) = mpsc::channel();
        let gate = Arc::new(Mutex::new(gate));
        let recorded = Arc::clone(&seen);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let recorded = Arc::clone(&recorded);
                let gate = Arc::clone(&gate);
                thread::spawn(move || serve_upstream(stream, &recorded, &gate));
            }
        });
        Self {
            addr,
            seen,
            release,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

fn serve_upstream(
    mut stream: TcpStream,
    recorded: &Mutex<Vec<Seen>>,
    gate: &Mutex<mpsc::Receiver<()>>,
) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').unwrap();
        headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
    }
    let length: usize = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .map_or(0, |(_, value)| value.parse().unwrap());
    let mut raw = vec![0; length];
    if reader.read_exact(&mut raw).is_err() {
        return;
    }
    let raw_body = String::from_utf8(raw).unwrap();
    let body: Value = serde_json::from_str(&raw_body).unwrap_or(Value::Null);
    let seen = Seen {
        request_line: request_line.trim_end().to_string(),
        headers,
        body: body.clone(),
        raw_body,
    };
    let key = seen.header("x-api-key").join(",");
    recorded.lock().unwrap().push(seen);

    let respond = |stream: &mut TcpStream, status: &str, body: &Value| {
        let body = body.to_string();
        let _ = write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nrequest-id: req_1\r\n\
             x-upstream-account: acct-1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    };
    if request_line.contains("/count_tokens") {
        respond(&mut stream, "200 OK", &json!({"input_tokens": 42}));
        return;
    }
    match body["model"].as_str().unwrap_or("") {
        "model-gated-stream" => {
            let _ = stream.write_all(STREAM_HEAD.as_bytes());
            write_chunk(&mut stream, &stream_start());
            // Hold the rest until the test has read the first event.
            let _ = gate.lock().unwrap().recv_timeout(Duration::from_secs(20));
            write_chunk(&mut stream, &stream_rest());
            let _ = stream.write_all(b"0\r\n\r\n");
        }
        "model-stream" => {
            let _ = stream.write_all(STREAM_HEAD.as_bytes());
            write_chunk(&mut stream, &stream_start());
            write_chunk(&mut stream, &stream_rest());
            let _ = stream.write_all(b"0\r\n\r\n");
        }
        "model-stalled-stream" => {
            let _ = stream.write_all(STREAM_HEAD.as_bytes());
            write_chunk(&mut stream, &stream_start());
            thread::sleep(Duration::from_secs(20));
        }
        "model-slow" => thread::sleep(Duration::from_secs(20)),
        "model-refused-key" => respond(
            &mut stream,
            "401 Unauthorized",
            &json!({"type": "error", "error": {"type": "authentication_error",
                    "message": "invalid x-api-key"}}),
        ),
        "model-echo-key" => respond(
            &mut stream,
            "400 Bad Request",
            &json!({"type": "error", "error": {"type": "invalid_request_error",
                    "message": format!("bad key {key}")}}),
        ),
        "model-overloaded" => respond(
            &mut stream,
            "529 Overloaded",
            &json!({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}),
        ),
        _ => respond(
            &mut stream,
            "200 OK",
            &json!({"id": "msg_1", "type": "message", "role": "assistant",
                    "content": [{"type": "text", "text": COMPLETION}],
                    "usage": {"input_tokens": 10, "output_tokens": 5}}),
        ),
    }
}

// ---------------------------------------------------------------- endpoint

/// A length-prefixed JSON stand-in for the daemon endpoint. It answers as a
/// live worker for every capability currently in `live`.
struct FakeEndpoint {
    _dir: tempfile::TempDir,
    path: PathBuf,
    live: Arc<Mutex<HashSet<String>>>,
}

impl FakeEndpoint {
    fn start(live: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("endpoint.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let live: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(
            live.iter().map(|run| (*run).to_string()).collect(),
        ));
        let known = Arc::clone(&live);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut prefix = [0_u8; 4];
                if stream.read_exact(&mut prefix).is_err() {
                    continue;
                }
                let mut body = vec![0; u32::from_be_bytes(prefix) as usize];
                if stream.read_exact(&mut body).is_err() {
                    continue;
                }
                let request: Value = serde_json::from_slice(&body).unwrap();
                let capability = request["capability"].as_str().unwrap();
                let response = if request["operation"]["type"] == "inventory"
                    && known.lock().unwrap().contains(capability)
                {
                    json!({"version": 1, "ok": true, "result": {
                        "type": "inventory", "repository": "test/repo", "task_id": 7,
                        "role": "worker", "pr": null, "review_revision": null,
                        "phase": "initial-worker", "operations": []}})
                } else {
                    json!({"version": 1, "ok": false, "error":
                        {"code": "unauthorized", "message": "run authority rejected"}})
                };
                let response = serde_json::to_vec(&response).unwrap();
                let _ = stream.write_all(&(response.len() as u32).to_be_bytes());
                let _ = stream.write_all(&response);
            }
        });
        Self {
            _dir: dir,
            path,
            live,
        }
    }

    fn finish(&self, run: &str) {
        self.live.lock().unwrap().remove(run);
    }
}

// --------------------------------------------------------------- forwarder

/// One `quorum inference-forwarder --listen 127.0.0.1:0` process, the address
/// it reported binding, and everything it writes to stderr.
struct Forwarder {
    child: Child,
    addr: SocketAddr,
    lines: mpsc::Receiver<String>,
}

fn forwarder_command(endpoint: &Path, upstream: &str, env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(quorum_bin());
    command
        .args(["inference-forwarder", "--listen", "127.0.0.1:0"])
        .env("QUORUM_AGENT_ENDPOINT", endpoint)
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_INFERENCE_UPSTREAM_URL", upstream)
        .env("QUORUM_INFERENCE_UPSTREAM_API_KEY", UPSTREAM_KEY)
        .env_remove("QUORUM_INFERENCE_ALLOWED_MODELS")
        .env_remove("QUORUM_INFERENCE_ALLOWED_BETAS")
        .env_remove("QUORUM_INFERENCE_RUN_TOKEN_CAP")
        .env_remove("QUORUM_INFERENCE_UPSTREAM_TIMEOUT_SECS")
        .env_remove("QUORUM_RUN_ID")
        .env_remove("QUORUM_AGENT")
        .env_remove("QUORUM_HOME")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for (name, value) in env {
        command.env(name, value);
    }
    command
}

impl Forwarder {
    fn start(endpoint: &Path, upstream: &str, env: &[(&str, &str)]) -> Self {
        let mut child = forwarder_command(endpoint, upstream, env).spawn().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = sender.send(line);
            }
        });
        let line = lines
            .recv_timeout(Duration::from_secs(15))
            .expect("inference-forwarder reported no address");
        let addr = line
            .strip_prefix("quorum inference-forwarder listening on http://")
            .unwrap_or_else(|| panic!("unexpected listener line: {line:?}"))
            .parse()
            .unwrap();
        Self { child, addr, lines }
    }

    /// SIGTERM must end the listener cleanly with exit 0. Returns every log
    /// line written after the listening line, checked for secrets.
    fn stop(mut self) -> Vec<String> {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        let status = self
            .child
            .try_wait()
            .unwrap()
            .expect("inference-forwarder did not exit after SIGTERM");
        assert_eq!(status.code(), Some(0), "forwarder exit: {status}");
        let mut logs = Vec::new();
        while let Ok(line) = self.lines.recv_timeout(Duration::from_secs(2)) {
            logs.push(line);
        }
        for line in &logs {
            for secret in [UPSTREAM_KEY, PROMPT, COMPLETION, "run-a", "worker-cap"] {
                assert!(!line.contains(secret), "log line leaks {secret:?}: {line}");
            }
        }
        logs
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

// ------------------------------------------------------------------ client

struct Reply {
    status: u16,
    head: String,
    body: String,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|error| panic!("{error}: {:?}", self.body))
    }

    fn error_message(&self) -> String {
        let value = self.json();
        assert_eq!(value["type"], "error", "{value}");
        value["error"]["message"].as_str().unwrap().to_string()
    }

    fn header(&self, name: &str) -> Option<String> {
        self.head.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name)
                .then(|| value.trim().to_string())
        })
    }
}

fn dechunk(mut raw: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = raw.split_once("\r\n") {
        let size = usize::from_str_radix(size.trim(), 16).unwrap();
        if size == 0 {
            break;
        }
        out.push_str(&rest[..size]);
        raw = &rest[size + 2..];
    }
    out
}

/// One HTTP/1.1 exchange on its own connection. No response, in any test,
/// may contain the upstream credential.
fn send(addr: SocketAddr, method: &str, path: &str, headers: &[(&str, &str)], body: &str) -> Reply {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("host"))
    {
        request.push_str(&format!("Host: {addr}\r\n"));
    }
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let raw = String::from_utf8(raw).unwrap();
    assert!(!raw.contains(UPSTREAM_KEY), "response leaks the credential");
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed HTTP response: {raw:?}"));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("malformed HTTP status line: {head:?}"));
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(body)
    } else {
        body.to_string()
    };
    Reply {
        status,
        head: head.to_string(),
        body,
    }
}

fn message(model: &str) -> Value {
    json!({
        "model": model,
        "max_tokens": 64,
        "messages": [{"role": "user", "content": PROMPT}]
    })
}

fn post(addr: SocketAddr, run: &str, body: &Value) -> Reply {
    send(
        addr,
        "POST",
        "/v1/messages",
        &[("x-api-key", run), ("Content-Type", "application/json")],
        &body.to_string(),
    )
}

// ------------------------------------------------------------------- tests

#[test]
fn forwards_with_the_upstream_credential_in_place_of_the_run_capability() {
    let endpoint = FakeEndpoint::start(&["run-a"]);
    let upstream = FakeUpstream::start();
    let forwarder = Forwarder::start(
        &endpoint.path,
        &upstream.url(),
        &[("QUORUM_INFERENCE_ALLOWED_BETAS", "beta-a, beta-b")],
    );
    let addr = forwarder.addr;

    // x-api-key, as a model client configured with a base URL sends it. A
    // query is accepted and not forwarded.
    let reply = send(
        addr,
        "POST",
        "/v1/messages?beta=true",
        &[
            ("x-api-key", "run-a"),
            ("Content-Type", "application/json"),
            ("anthropic-version", "2023-06-01"),
            ("anthropic-beta", "beta-b"),
            ("Cookie", "session=1"),
            ("X-Forwarded-For", "192.0.2.9"),
        ],
        &message("model-json").to_string(),
    );
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.json()["content"][0]["text"], COMPLETION);
    assert_eq!(reply.header("request-id").as_deref(), Some("req_1"));
    assert_eq!(reply.header("x-upstream-account"), None);

    // Authorization: Bearer is accepted too, and count_tokens is mirrored.
    let reply = send(
        addr,
        "POST",
        "/v1/messages/count_tokens",
        &[("Authorization", "Bearer run-a")],
        &message("model-json").to_string(),
    );
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.json()["input_tokens"], 42);

    // A duplicated key is forwarded once, with the value that was checked.
    let duplicated = concat!(
        r#"{"model":"model-json","tools":[{"type":"web_search_20250305","name":"w"}],"#,
        r#""tools":[],"max_tokens":1,"messages":[]}"#
    );
    let reply = send(
        addr,
        "POST",
        "/v1/messages",
        &[("x-api-key", "run-a")],
        duplicated,
    );
    assert_eq!(reply.status, 200, "{}", reply.body);

    let seen = upstream.seen();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].request_line, "POST /v1/messages HTTP/1.1");
    assert_eq!(seen[0].body, message("model-json"));
    assert_eq!(seen[0].header("anthropic-beta"), ["beta-b"]);
    assert_eq!(seen[0].header("anthropic-version"), ["2023-06-01"]);
    assert_eq!(
        seen[1].request_line,
        "POST /v1/messages/count_tokens HTTP/1.1"
    );
    assert_eq!(seen[1].header("anthropic-version"), ["2023-06-01"]);
    assert!(
        !seen[2].raw_body.contains("web_search"),
        "{}",
        seen[2].raw_body
    );
    for request in &seen {
        // The inbound credential is replaced, never passed along.
        assert_eq!(request.header("x-api-key"), [UPSTREAM_KEY]);
        assert!(request.header("authorization").is_empty());
        assert!(request.header("cookie").is_empty());
        assert!(request.header("x-forwarded-for").is_empty());
        assert!(!format!("{:?}", request.headers).contains("run-a"));
    }

    let logs = forwarder.stop();
    let line = logs
        .iter()
        .find(|line| line.contains("route=messages"))
        .unwrap_or_else(|| panic!("no request log line: {logs:?}"));
    for field in [
        "task=7",
        "role=worker",
        "model=model-json",
        "status=200",
        "input_tokens=10",
        "output_tokens=5",
        "run_total_tokens=15",
    ] {
        assert!(line.contains(field), "missing {field}: {line}");
    }
}

#[test]
fn requests_without_a_live_run_capability_get_401_and_nothing_is_forwarded() {
    let endpoint = FakeEndpoint::start(&["run-a"]);
    let upstream = FakeUpstream::start();
    let forwarder = Forwarder::start(&endpoint.path, &upstream.url(), &[]);
    let addr = forwarder.addr;
    let body = message("model-json").to_string();

    let cases: [&[(&str, &str)]; 8] = [
        &[],
        &[("x-api-key", "run-unknown")],
        &[("Authorization", "Bearer run-unknown")],
        &[("Authorization", "Basic run-a")],
        &[("x-api-key", "run-a"), ("Authorization", "Bearer run-b")],
        &[("x-api-key", "run-a"), ("x-api-key", "run-a")],
        // Presenting the upstream credential itself earns nothing.
        &[("x-api-key", UPSTREAM_KEY)],
        &[("x-api-key", "run a")],
    ];
    for headers in cases {
        for path in ["/v1/messages", "/v1/messages/count_tokens"] {
            let reply = send(addr, "POST", path, headers, &body);
            assert_eq!(reply.status, 401, "{headers:?}: {}", reply.body);
            assert_eq!(reply.json()["error"]["type"], "authentication_error");
            assert_eq!(reply.header("x-should-retry").as_deref(), Some("false"));
        }
    }
    assert!(upstream.seen().is_empty());

    // A capability stops working the moment its run is no longer live.
    assert_eq!(post(addr, "run-a", &message("model-json")).status, 200);
    endpoint.finish("run-a");
    assert_eq!(post(addr, "run-a", &message("model-json")).status, 401);
    assert_eq!(upstream.seen().len(), 1);
    forwarder.stop();
}

#[test]
fn only_the_messages_routes_exist() {
    let endpoint = FakeEndpoint::start(&["run-a"]);
    let upstream = FakeUpstream::start();
    let forwarder = Forwarder::start(&endpoint.path, &upstream.url(), &[]);
    let addr = forwarder.addr;
    let body = message("model-json").to_string();
    for (method, path) in [
        ("GET", "/v1/messages"),
        ("PUT", "/v1/messages"),
        ("DELETE", "/v1/messages"),
        ("POST", "/v1/messages/"),
        ("POST", "/v1/messages/batches"),
        ("POST", "/v1/complete"),
        ("POST", "/v1/files"),
        ("GET", "/v1/models"),
        ("POST", "/v1/messages/../files"),
        ("POST", "/mcp"),
        ("POST", "/"),
    ] {
        let reply = send(addr, method, path, &[("x-api-key", "run-a")], &body);
        assert_eq!(reply.status, 404, "{method} {path}: {}", reply.body);
    }

    // DNS-rebinding guard: only loopback host names are served.
    for (host, status) in [
        ("evil.example", 403),
        ("192.0.2.1", 403),
        ("localhost", 200),
    ] {
        let reply = send(
            addr,
            "POST",
            "/v1/messages",
            &[("Host", host), ("x-api-key", "run-a")],
            &body,
        );
        assert_eq!(reply.status, status, "Host: {host}");
    }
    assert_eq!(upstream.seen().len(), 1);
    forwarder.stop();
}

#[test]
fn requests_outside_the_policy_get_400_and_nothing_is_forwarded() {
    let endpoint = FakeEndpoint::start(&["run-a"]);
    let upstream = FakeUpstream::start();
    let forwarder = Forwarder::start(
        &endpoint.path,
        &upstream.url(),
        &[
            ("QUORUM_INFERENCE_ALLOWED_MODELS", "model-json,model-stream"),
            ("QUORUM_INFERENCE_ALLOWED_BETAS", "beta-a"),
        ],
    );
    let addr = forwarder.addr;
    let with = |field: &str, value: Value| {
        let mut body = message("model-json");
        body[field] = value;
        body
    };
    let url_image = json!([{"role": "user", "content": [
        {"type": "image", "source": {"type": "url", "url": "http://169.254.169.254/"}}]}]);
    let file_document = json!([{"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": "t", "content": [
            {"type": "document", "source": {"type": "file", "file_id": "file_1"}}]}]}]);
    for (body, expected) in [
        (
            with(
                "tools",
                json!([{"type": "web_search_20250305", "name": "web_search"}]),
            ),
            "tool type web_search_20250305",
        ),
        (
            with(
                "tools",
                json!([{"type": "code_execution_20250825", "name": "c"}]),
            ),
            "tool type code_execution_20250825",
        ),
        (
            with(
                "tools",
                json!([{"type": "mcp_toolset", "mcp_server_name": "m"}]),
            ),
            "tool type mcp_toolset",
        ),
        (
            with("tools", json!([{"type": "bash_20250124", "name": "bash"}])),
            "tool type bash_20250124",
        ),
        (
            with(
                "mcp_servers",
                json!([{"type": "url", "url": "https://mcp.example", "name": "m"}]),
            ),
            "mcp_servers is not available",
        ),
        (
            with("container", json!("container_1")),
            "container is not available",
        ),
        (with("messages", url_image), "url sources are not available"),
        (
            with("messages", file_document),
            "file sources are not available",
        ),
        (message("model-other"), "model model-other is not allowed"),
        (with("model", json!(null)), "model must be"),
        (json!([message("model-json")]), "must be a JSON object"),
    ] {
        let reply = post(addr, "run-a", &body);
        assert_eq!(reply.status, 400, "{expected}: {}", reply.body);
        assert_eq!(reply.json()["error"]["type"], "invalid_request_error");
        let text = reply.error_message();
        assert!(text.contains(expected), "{expected}: {text}");
        assert!(!reply.body.contains(PROMPT));
    }
    let reply = send(
        addr,
        "POST",
        "/v1/messages",
        &[("x-api-key", "run-a")],
        "{not json",
    );
    assert_eq!(reply.status, 400);

    // Betas outside the allowlist refuse the request, however they arrive.
    for betas in ["code-execution-2025-08-25", "beta-a,mcp-client-2025-11-20"] {
        for path in ["/v1/messages", "/v1/messages/count_tokens"] {
            let reply = send(
                addr,
                "POST",
                path,
                &[("x-api-key", "run-a"), ("anthropic-beta", betas)],
                &message("model-json").to_string(),
            );
            assert_eq!(reply.status, 400, "{betas}: {}", reply.body);
            assert!(reply.error_message().contains("is not allowed"));
        }
    }
    let reply = send(
        addr,
        "POST",
        "/v1/messages",
        &[
            ("x-api-key", "run-a"),
            ("anthropic-beta", "beta-a"),
            ("anthropic-beta", "files-api-2025-04-14"),
        ],
        &message("model-json").to_string(),
    );
    assert_eq!(reply.status, 400);
    assert!(upstream.seen().is_empty(), "{:?}", upstream.seen());

    // A custom tool with a cache breakpoint is ordinary and goes through.
    let tools = json!([{"name": "read_file", "description": "d",
        "input_schema": {"type": "object"}, "cache_control": {"type": "ephemeral"}}]);
    assert_eq!(post(addr, "run-a", &with("tools", tools)).status, 200);
    assert_eq!(upstream.seen().len(), 1);
    let logs = forwarder.stop();
    assert!(
        logs.iter().any(|line| line.contains("status=400")),
        "{logs:?}"
    );
}

#[test]
fn a_run_over_its_token_cap_gets_429_until_the_forwarder_restarts() {
    let endpoint = FakeEndpoint::start(&["run-a", "run-b"]);
    let upstream = FakeUpstream::start();
    let env = [("QUORUM_INFERENCE_RUN_TOKEN_CAP", "40")];
    let forwarder = Forwarder::start(&endpoint.path, &upstream.url(), &env);
    let addr = forwarder.addr;

    // 15 tokens whole, then 30 streamed (7 + 3 input, 20 output): 45 >= 40.
    // The request that crosses the cap completes; the next is refused.
    assert_eq!(post(addr, "run-a", &message("model-json")).status, 200);
    // Token counting is free upstream and is not charged here.
    for _ in 0..3 {
        let reply = send(
            addr,
            "POST",
            "/v1/messages/count_tokens",
            &[("x-api-key", "run-a")],
            &message("model-json").to_string(),
        );
        assert_eq!(reply.status, 200);
    }
    // An upstream error reports no usage and costs the run nothing.
    assert_eq!(
        post(addr, "run-a", &message("model-overloaded")).status,
        529
    );
    let mut streaming = message("model-stream");
    streaming["stream"] = json!(true);
    let reply = post(addr, "run-a", &streaming);
    assert_eq!(reply.status, 200);
    assert!(reply.body.contains("message_stop"), "{}", reply.body);

    let forwarded = upstream.seen().len();
    for model in ["model-json", "model-stream"] {
        let reply = post(addr, "run-a", &message(model));
        assert_eq!(reply.status, 429, "{}", reply.body);
        assert_eq!(reply.json()["error"]["type"], "rate_limit_error");
        assert!(reply.error_message().contains("token allowance"));
        assert_eq!(reply.header("x-should-retry").as_deref(), Some("false"));
    }
    assert_eq!(
        upstream.seen().len(),
        forwarded,
        "a capped request was forwarded"
    );
    // The cap is per run.
    assert_eq!(post(addr, "run-b", &message("model-json")).status, 200);

    let logs = forwarder.stop();
    assert!(
        logs.iter()
            .any(|line| line.contains("model=model-stream") && line.contains("run_total_tokens=45")),
        "{logs:?}"
    );
    assert!(
        logs.iter().any(|line| line.contains("status=429")),
        "{logs:?}"
    );

    // Counts are held in the forwarder's memory only: a restarted forwarder
    // starts a still-live run from zero.
    let forwarder = Forwarder::start(&endpoint.path, &upstream.url(), &env);
    assert_eq!(
        post(forwarder.addr, "run-a", &message("model-json")).status,
        200
    );
    forwarder.stop();
}

#[test]
fn a_streamed_response_is_passed_through_as_it_arrives_and_is_counted() {
    let endpoint = FakeEndpoint::start(&["run-a"]);
    let upstream = FakeUpstream::start();
    let forwarder = Forwarder::start(
        &endpoint.path,
        &upstream.url(),
        &[("QUORUM_INFERENCE_RUN_TOKEN_CAP", "30")],
    );
    let addr = forwarder.addr;
    let mut body = message("model-gated-stream");
    body["stream"] = json!(true);
    let body = body.to_string();

    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        stream,
        "POST /v1/messages HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\
         x-api-key: run-a\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    // The first event arrives while the upstream is still holding the rest
    // back: nothing is buffered until the end of the response.
    let mut received = String::new();
    let mut buffer = [0_u8; 4096];
    while !received.contains("message_start") {
        let read = stream.read(&mut buffer).unwrap();
        assert!(read > 0, "stream closed early: {received:?}");
        received.push_str(std::str::from_utf8(&buffer[..read]).unwrap());
    }
    assert!(received.starts_with("HTTP/1.1 200"), "{received}");
    assert!(received
        .to_ascii_lowercase()
        .contains("content-type: text/event-stream"));
    assert!(!received.contains("message_stop"), "{received}");

    upstream.release.send(()).unwrap();
    let mut rest = String::new();
    stream.read_to_string(&mut rest).unwrap();
    received.push_str(&rest);
    for expected in [COMPLETION, "message_delta", "message_stop"] {
        assert!(
            received.contains(expected),
            "missing {expected}: {received}"
        );
    }
    assert!(!received.contains(UPSTREAM_KEY));

    // Streamed usage (10 input + 20 output) reached the cap of 30.
    assert_eq!(post(addr, "run-a", &message("model-json")).status, 429);
    let logs = forwarder.stop();
    assert!(
        logs.iter().any(|line| line.contains("input_tokens=10")
            && line.contains("output_tokens=20")
            && line.contains("run_total_tokens=30")),
        "{logs:?}"
    );
}

#[test]
fn upstream_failures_are_bounded_and_never_expose_the_credential() {
    let endpoint = FakeEndpoint::start(&["run-a"]);
    let upstream = FakeUpstream::start();
    let env = [
        ("QUORUM_INFERENCE_UPSTREAM_TIMEOUT_SECS", "1"),
        ("QUORUM_INFERENCE_RUN_TOKEN_CAP", "10"),
    ];
    let forwarder = Forwarder::start(&endpoint.path, &upstream.url(), &env);
    let addr = forwarder.addr;

    // An upstream that accepts and then says nothing: 504 at the bound.
    let started = Instant::now();
    let reply = post(addr, "run-a", &message("model-slow"));
    assert_eq!(reply.status, 504, "{}", reply.body);
    assert_eq!(reply.json()["error"]["type"], "api_error");
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );

    // The upstream rejecting the operator's credential is not the run's
    // error to see, and a body repeating the credential is not relayed.
    for model in ["model-refused-key", "model-echo-key"] {
        let reply = post(addr, "run-a", &message(model));
        assert_eq!(reply.status, 502, "{model}: {}", reply.body);
        assert_eq!(
            reply.error_message(),
            "upstream refused the forwarder's own request"
        );
    }
    // Other upstream errors are relayed as they are.
    let reply = post(addr, "run-a", &message("model-overloaded"));
    assert_eq!(reply.status, 529);
    assert_eq!(reply.json()["error"]["type"], "overloaded_error");

    // A stream that stalls is ended with an error event at the bound, and
    // the usage it had already reported (10 input, 1 output) is kept.
    let started = Instant::now();
    let reply = post(addr, "run-a", &message("model-stalled-stream"));
    assert_eq!(reply.status, 200);
    assert!(reply.body.contains("message_start"), "{}", reply.body);
    assert!(reply.body.contains("event: error"), "{}", reply.body);
    assert!(
        reply.body.contains("upstream stream timed out"),
        "{}",
        reply.body
    );
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(post(addr, "run-a", &message("model-json")).status, 429);
    forwarder.stop();

    // No upstream listening at all: a clean 502.
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let down = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let forwarder = Forwarder::start(&endpoint.path, &down, &[]);
    let reply = post(forwarder.addr, "run-a", &message("model-json"));
    assert_eq!(reply.status, 502, "{}", reply.body);
    assert_eq!(reply.error_message(), "upstream could not be reached");
    forwarder.stop();

    // The daemon endpoint being down is not a rejection: 503, not 401.
    let dir = tempfile::tempdir().unwrap();
    let forwarder = Forwarder::start(&dir.path().join("absent.sock"), &upstream.url(), &[]);
    let forwarded = upstream.seen().len();
    let reply = post(forwarder.addr, "run-a", &message("model-json"));
    assert_eq!(reply.status, 503, "{}", reply.body);
    assert_eq!(upstream.seen().len(), forwarded);
    forwarder.stop();
}

/// A raw request on its own connection; `None` when the server closed or
/// reset the connection before an answer could be read.
fn raw_exchange(addr: SocketAddr, request: &[u8], read_timeout: Duration) -> Option<u16> {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(read_timeout)).unwrap();
    stream.write_all(request).unwrap();
    let mut raw = Vec::new();
    if let Err(error) = stream.read_to_end(&mut raw) {
        // A server that closes with request bytes unread resets the peer.
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset,
            "no response and no close: {error}"
        );
    }
    String::from_utf8_lossy(&raw)
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
}

#[test]
fn oversized_and_stalled_requests_are_cut_off() {
    let endpoint = FakeEndpoint::start(&["run-a"]);
    let upstream = FakeUpstream::start();
    let forwarder = Forwarder::start(&endpoint.path, &upstream.url(), &[]);
    let addr = forwarder.addr;
    let head = |extra: &str| {
        format!("POST /v1/messages HTTP/1.1\r\nHost: {addr}\r\nx-api-key: run-a\r\n{extra}\r\n")
    };

    // A body declared over the 32 MiB bound is refused before it is read.
    let declared = head(&format!("Content-Length: {}\r\n", 32 * 1024 * 1024 + 1));
    assert_eq!(
        raw_exchange(addr, declared.as_bytes(), Duration::from_secs(10)),
        Some(413)
    );
    // An undeclared (chunked) body is refused once it passes the bound. The
    // peer may see the 413 or a reset, depending on how much it had queued.
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    stream
        .write_all(head("Transfer-Encoding: chunked\r\n").as_bytes())
        .unwrap();
    let chunk = format!("{:x}\r\n{}\r\n", 1024 * 1024, "x".repeat(1024 * 1024));
    let mut refused = false;
    for _ in 0..40 {
        if stream.write_all(chunk.as_bytes()).is_err() {
            refused = true;
            break;
        }
    }
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let raw = String::from_utf8_lossy(&raw);
    assert!(refused || raw.starts_with("HTTP/1.1 413"), "{raw}");
    assert!(!raw.starts_with("HTTP/1.1 200"), "{raw}");

    // A request head well over the 64 KiB read-buffer bound is refused.
    let big_head = head(&format!("X-Padding: {}\r\n", "p".repeat(256 * 1024)));
    let status = raw_exchange(addr, big_head.as_bytes(), Duration::from_secs(10));
    assert!(matches!(status, Some(431) | None), "{status:?}");

    // An unauthorized caller's body is never waited for.
    let started = Instant::now();
    let unauthorized = format!(
        "POST /v1/messages HTTP/1.1\r\nHost: {addr}\r\nx-api-key: nobody\r\n\
         Content-Length: 100\r\n\r\n"
    );
    assert_eq!(
        raw_exchange(addr, unauthorized.as_bytes(), Duration::from_secs(10)),
        Some(401)
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(upstream.seen().is_empty());

    // SIGTERM ends the listener within its drain bound even with a request
    // head half sent.
    let mut stalled = TcpStream::connect(addr).unwrap();
    stalled
        .write_all(b"POST /v1/messages HTTP/1.1\r\nHost: 127.0.0.1\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(300));
    let started = Instant::now();
    forwarder.stop();
    assert!(started.elapsed() < Duration::from_secs(8));
    drop(stalled);
}

#[test]
fn configuration_errors_are_usage_errors_that_never_print_the_credential() {
    let dir = tempfile::tempdir().unwrap();
    let endpoint = dir.path().join("endpoint.sock");
    let run = |mut command: Command| {
        let output = command.output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        assert!(!stderr.contains(UPSTREAM_KEY), "{stderr}");
        (output.status.code(), stderr)
    };

    let mut command = forwarder_command(&endpoint, "http://127.0.0.1:9", &[]);
    command.env_remove("QUORUM_INFERENCE_UPSTREAM_API_KEY");
    let (code, stderr) = run(command);
    assert_eq!(code, Some(2), "{stderr}");
    assert!(
        stderr.contains("QUORUM_INFERENCE_UPSTREAM_API_KEY"),
        "{stderr}"
    );

    // A credential that cannot be a header value is refused without echo.
    let bad_key = format!("{UPSTREAM_KEY}\nx-injected: 1");
    let (code, stderr) = run(forwarder_command(
        &endpoint,
        "http://127.0.0.1:9",
        &[("QUORUM_INFERENCE_UPSTREAM_API_KEY", &bad_key)],
    ));
    assert_eq!(code, Some(2), "{stderr}");

    for (upstream, env) in [
        // The credential may not cross a network in clear.
        ("http://gateway.example", vec![]),
        ("ftp://127.0.0.1", vec![]),
        ("https://user:pw@gateway.example", vec![]),
        (
            "http://127.0.0.1:9",
            vec![("QUORUM_INFERENCE_RUN_TOKEN_CAP", "0")],
        ),
        (
            "http://127.0.0.1:9",
            vec![("QUORUM_INFERENCE_RUN_TOKEN_CAP", "lots")],
        ),
        (
            "http://127.0.0.1:9",
            vec![("QUORUM_INFERENCE_UPSTREAM_TIMEOUT_SECS", "0")],
        ),
        (
            "http://127.0.0.1:9",
            vec![("QUORUM_INFERENCE_ALLOWED_MODELS", "model a")],
        ),
    ] {
        let (code, stderr) = run(forwarder_command(&endpoint, upstream, &env));
        assert_eq!(code, Some(2), "{upstream} {env:?}: {stderr}");
    }

    for listen in ["0.0.0.0:0", "192.0.2.1:0"] {
        let mut command = forwarder_command(&endpoint, "http://127.0.0.1:9", &[]);
        command.args(["--listen", listen]);
        let (code, stderr) = run(command);
        assert_eq!(code, Some(2), "{listen}: {stderr}");
    }
    let mut command = Command::new(quorum_bin());
    command.arg("inference-forwarder");
    let (code, _) = run(command);
    assert_eq!(code, Some(2));
}

// ------------------------------------------------------------- real daemon

fn endpoint_path(db: &Path) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    db.hash(&mut hasher);
    std::env::temp_dir()
        .join(format!("quorum-agent-{:016x}", hasher.finish()))
        .join("endpoint.sock")
}

fn git(dir: &Path, args: &[&str]) {
    assert!(Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .status()
        .unwrap()
        .success());
}

struct Daemon {
    child: Child,
}

impl Daemon {
    fn start(root: &Path) -> (Self, PathBuf) {
        let home = root.join("home");
        let repo = root.join("repo");
        let worktrees = root.join("worktrees");
        for dir in [&home, &repo, &worktrees] {
            std::fs::create_dir_all(dir).unwrap();
        }
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "test@example.com"]);
        git(&repo, &["config", "user.name", "Forwarder Test"]);
        git(&repo, &["commit", "--allow-empty", "-m", "initial"]);
        git(&repo, &["remote", "add", "origin", repo.to_str().unwrap()]);
        git(&repo, &["fetch", "origin"]);
        let names = root.join("names.txt");
        std::fs::write(&names, "AgentOne\nAgentTwo\n").unwrap();
        let config = root.join("serve.toml");
        let mut text = String::from(
            "[model_profiles.test]\nrunner = \"claude\"\nmodel = \"claude-opus-4-6\"\neffort = \"high\"\n\
             [routing.classifier]\ntest = 100\n[routing.planner]\ntest = 100\n\
             [routing.collector]\ntest = 100\n",
        );
        for role in ["worker", "reviewer"] {
            for level in 1..=5 {
                text.push_str(&format!("[routing.{role}.{level}]\ntest = 100\n"));
            }
        }
        std::fs::write(&config, text).unwrap();
        let mut child = Command::new(quorum_bin())
            .env("QUORUM_HOME", &home)
            .env("QUORUM_REPO", "test/repo")
            .args(["serve", "--config"])
            .arg(&config)
            .args(["--repo", "test/repo", "--cap", "1", "--repo-dir"])
            .arg(&repo)
            .arg("--worktree-base")
            .arg(&worktrees)
            .arg("--names-file")
            .arg(&names)
            .arg("--agent-bin")
            .arg(assert_cmd::cargo::cargo_bin("fake-agent"))
            .args(["--merge-cmd", "true"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = sender.send(line);
            }
        });
        let daemon = Self { child };
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut seen = Vec::new();
        loop {
            match lines.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(line) if line.contains("serving (cap=1)") => break,
                Ok(line) => seen.push(line),
                Err(error) => panic!("daemon did not start: {error}; output={seen:?}"),
            }
        }
        (daemon, home.join("repos/test__repo/quorum.db"))
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}

/// A live worker run (`worker-cap`), a live reviewer run (`reviewer-cap`),
/// and a worker run whose capability was revoked (`revoked-cap`).
fn install_runs(db: &Path) {
    let mut conn = quorum_core::db::open(db).unwrap();
    let tx = quorum_core::db::begin_immediate(&mut conn).unwrap();
    for (id, status, assignee, reviewer, refs) in [
        (200, "working", Some("Worker"), None, None),
        (
            201,
            "in-review",
            None,
            Some("Reviewer"),
            Some(r#"{"pr":77}"#),
        ),
        (202, "working", Some("Revoked"), None, None),
    ] {
        tx.execute(
            "INSERT INTO tasks (id,title,status,assignee,reviewer,created_by,created_at,updated_at,refs)
             VALUES (?1,'Forwarder fixture',?2,?3,?4,'test',1000,1000,?5)",
            params![id, status, assignee, reviewer, refs],
        )
        .unwrap();
    }
    for (capability, task_id, agent, role, revoked_at) in [
        ("worker-cap", 200, "Worker", "worker", None),
        ("reviewer-cap", 201, "Reviewer", "reviewer", None),
        ("revoked-cap", 202, "Revoked", "worker", Some(1002)),
    ] {
        tx.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at,revoked_at)
             VALUES (?1,?2,?3,?4,1001,?5)",
            params![capability, task_id, agent, role, revoked_at],
        )
        .unwrap();
    }
    for (task_id, agent) in [(200, "Worker"), (202, "Revoked")] {
        tx.execute(
            "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
             VALUES (?1,?2,'worker','test','high','test',1001)",
            params![task_id, agent],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO claims(target,holder,ts,expires_at,active)
             VALUES ('task#' || ?1, ?2, 1000, ?3, 1)",
            params![task_id, agent, quorum_core::clock::now() + 86_400],
        )
        .unwrap();
    }
    tx.execute(
        "INSERT INTO agent_runs
         (task_id,agent_name,role,model,effort,provider,spawned_at,review_cap_run_id,review_pr,review_head_sha)
         VALUES (201,'Reviewer','reviewer','test','high','test',1001,'reviewer-cap',77,?1)",
        ["a".repeat(40)],
    )
    .unwrap();
    tx.commit().unwrap();
}

#[test]
fn run_authority_comes_from_the_real_daemon_endpoint() {
    let root = tempfile::tempdir().unwrap();
    let (daemon, db) = Daemon::start(root.path());
    install_runs(&db);
    let upstream = FakeUpstream::start();
    let forwarder = Forwarder::start(
        &endpoint_path(&db),
        &upstream.url(),
        &[("QUORUM_INFERENCE_RUN_TOKEN_CAP", "20")],
    );
    let addr = forwarder.addr;

    for run in ["revoked-cap", "no-such-run", ""] {
        let reply = post(addr, run, &message("model-json"));
        assert_eq!(reply.status, 401, "{run:?}: {}", reply.body);
    }
    assert!(upstream.seen().is_empty());

    // Live worker and reviewer runs are served, each under its own count.
    assert_eq!(post(addr, "worker-cap", &message("model-json")).status, 200);
    assert_eq!(
        post(addr, "reviewer-cap", &message("model-json")).status,
        200
    );
    assert_eq!(post(addr, "worker-cap", &message("model-json")).status, 200);
    assert_eq!(post(addr, "worker-cap", &message("model-json")).status, 429);
    assert_eq!(
        post(addr, "reviewer-cap", &message("model-json")).status,
        200
    );
    assert_eq!(upstream.seen().len(), 4);

    // Revoking the capability ends its access on the next request, before
    // its cap is even consulted.
    let conn = quorum_core::db::open(&db).unwrap();
    conn.execute(
        "UPDATE run_capabilities SET revoked_at=2000 WHERE run_id='reviewer-cap'",
        [],
    )
    .unwrap();
    drop(conn);
    assert_eq!(
        post(addr, "reviewer-cap", &message("model-json")).status,
        401
    );
    assert_eq!(upstream.seen().len(), 4);

    let logs = forwarder.stop();
    assert!(
        logs.iter()
            .any(|line| line.contains("task=200") && line.contains("role=worker")),
        "{logs:?}"
    );
    assert!(
        logs.iter().any(|line| line.contains("task=201")),
        "{logs:?}"
    );
    drop(daemon);
}
