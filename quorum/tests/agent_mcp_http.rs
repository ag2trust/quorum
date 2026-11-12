#![cfg(unix)]

//! Real-process coverage for `quorum agent-mcp --listen`: the loopback
//! Streamable HTTP transport, bearer-derived run authority, and the signalling
//! tools it advertises, driven against a real `quorum serve` endpoint.

use rusqlite::params;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// The message TTL written to the daemon's operator config. Deliberately not
/// the built-in default, so an alert's expiry proves the configured value.
const CONFIGURED_MESSAGE_TTL_SECS: i64 = 4321;

fn quorum_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
}

fn endpoint_path(db: &Path) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    db.hash(&mut hasher);
    std::env::temp_dir()
        .join(format!("quorum-agent-{:016x}", hasher.finish()))
        .join("endpoint.sock")
}

fn init_git_repo(dir: &Path) {
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.email", "test@example.com"],
        vec!["config", "user.name", "Agent MCP Test"],
        vec!["commit", "--allow-empty", "-m", "initial"],
    ] {
        assert!(Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .unwrap()
            .success());
    }
    assert!(Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["remote", "add", "origin"])
        .arg(dir)
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["fetch", "origin"])
        .status()
        .unwrap()
        .success());
}

fn write_config(path: &Path) {
    let mut config = String::from(
        "[model_profiles.test]\nrunner = \"claude\"\nmodel = \"claude-opus-4-6\"\neffort = \"high\"\n\
         [routing.classifier]\ntest = 100\n[routing.planner]\ntest = 100\n\
         [routing.collector]\ntest = 100\n",
    );
    for role in ["worker", "reviewer"] {
        for level in 1..=5 {
            config.push_str(&format!("[routing.{role}.{level}]\ntest = 100\n"));
        }
    }
    std::fs::write(path, config).unwrap();
}

struct Daemon {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Daemon {
    fn start(home: &Path, repo: &Path, worktrees: &Path, names: &Path, config: &Path) -> Self {
        let mut child = Command::new(quorum_bin())
            .env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .args(["serve", "--config"])
            .arg(config)
            .args(["--repo", "test/repo", "--cap", "1", "--repo-dir"])
            .arg(repo)
            .arg("--worktree-base")
            .arg(worktrees)
            .arg("--names-file")
            .arg(names)
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
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self { child, lines }
    }

    fn wait_for(&self, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            match self.lines.recv_timeout(deadline - Instant::now()) {
                Ok(line) if line.contains(needle) => return,
                Ok(line) => seen.push(line),
                Err(error) => panic!("daemon did not log {needle:?}: {error}; output={seen:?}"),
            }
        }
        panic!("daemon did not log {needle:?}; output={seen:?}");
    }

    fn stop(mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        if self.child.try_wait().unwrap().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}

/// Live runs: an initial worker (200), a reviewer (201, PR 77), a rework
/// worker (204, PR 42), and a revoked worker (202).
fn install_fixtures(db: &Path) {
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
        (204, "working", Some("Rework"), None, Some(r#"{"pr":42}"#)),
    ] {
        tx.execute(
            "INSERT INTO tasks (id,title,status,assignee,reviewer,created_by,created_at,updated_at,refs)
             VALUES (?1,'MCP HTTP fixture',?2,?3,?4,'test',1000,1000,?5)",
            params![id, status, assignee, reviewer, refs],
        )
        .unwrap();
    }
    for (capability, task_id, agent, role, revoked_at) in [
        ("worker-cap", 200, "Worker", "worker", None),
        ("reviewer-cap", 201, "Reviewer", "reviewer", None),
        ("revoked-cap", 202, "Revoked", "worker", Some(1002)),
        ("rework-cap", 204, "Rework", "worker", None),
    ] {
        tx.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at,revoked_at)
             VALUES (?1,?2,?3,?4,1001,?5)",
            params![capability, task_id, agent, role, revoked_at],
        )
        .unwrap();
    }
    for (task_id, agent) in [(200, "Worker"), (202, "Revoked"), (204, "Rework")] {
        tx.execute(
            "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
             VALUES (?1,?2,'worker','test','high','test',1001)",
            params![task_id, agent],
        )
        .unwrap();
        // A working task holds a live lease. Without one, the feed's
        // sweep-on-write (which `alert_owner` runs, like `quorum post`) would
        // reap the fixture back to `open` and end the worker's authority.
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

/// One `quorum agent-mcp --listen 127.0.0.1:0` process and the address it
/// reported binding.
struct HttpShell {
    child: Child,
    addr: SocketAddr,
}

impl HttpShell {
    /// The listener gets only the endpoint locator and the repository. The
    /// run-scoped variables are removed (or poisoned by `extra_env`) so any
    /// dependence on them fails a test.
    fn start(endpoint: &Path, extra_env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(quorum_bin());
        command
            .args(["agent-mcp", "--listen", "127.0.0.1:0"])
            .env("QUORUM_AGENT_ENDPOINT", endpoint)
            .env("QUORUM_REPO", "test/repo")
            .env_remove("QUORUM_RUN_ID")
            .env_remove("QUORUM_AGENT")
            .env_remove("QUORUM_HOME")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        for (name, value) in extra_env {
            command.env(name, value);
        }
        let mut child = command.spawn().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                let _ = sender.send(line);
            }
        });
        let line = lines
            .recv_timeout(Duration::from_secs(15))
            .expect("agent-mcp --listen reported no address");
        let addr = line
            .strip_prefix("quorum agent-mcp listening on http://")
            .and_then(|rest| rest.strip_suffix("/mcp"))
            .unwrap_or_else(|| panic!("unexpected listener line: {line:?}"))
            .parse()
            .unwrap();
        Self { child, addr }
    }

    /// SIGTERM must end the listener cleanly with exit 0.
    fn stop(mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        let status = self
            .child
            .try_wait()
            .unwrap()
            .expect("agent-mcp --listen did not exit after SIGTERM");
        assert_eq!(status.code(), Some(0), "listener exit: {status}");
    }
}

impl Drop for HttpShell {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// One HTTP/1.1 exchange on its own connection. Returns the status code and
/// the response body.
fn http(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let raw = String::from_utf8(raw).unwrap();
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed HTTP response: {raw:?}"));
    assert!(
        !head.to_ascii_lowercase().contains("transfer-encoding"),
        "unexpected streamed response: {head}"
    );
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("malformed HTTP status line: {head:?}"));
    (status, body.to_string())
}

const MCP_HEADERS: [(&str, &str); 2] = [
    ("Content-Type", "application/json"),
    ("Accept", "application/json, text/event-stream"),
];

fn post_mcp(addr: SocketAddr, authorization: &[&str], body: &Value) -> (u16, String) {
    let mut headers = MCP_HEADERS.to_vec();
    for value in authorization {
        headers.push(("Authorization", value));
    }
    http(addr, "POST", "/mcp", &headers, &body.to_string())
}

/// One JSON-RPC request authorized by `capability` as a bearer token.
fn rpc(addr: SocketAddr, capability: &str, method: &str, params: Value) -> Value {
    let (status, body) = post_mcp(
        addr,
        &[&format!("Bearer {capability}")],
        &json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
    );
    assert_eq!(status, 200, "{method}: {body}");
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("{method}: {error}: {body:?}"))
}

fn list_tools(addr: SocketAddr, capability: &str) -> Value {
    rpc(addr, capability, "tools/list", json!({}))
}

fn tool_names(response: &Value) -> Vec<&str> {
    response["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tool list: {response:#}"))
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect()
}

/// Call one tool and decode its single JSON text block.
fn call(addr: SocketAddr, capability: &str, name: &str, arguments: Value) -> (bool, Value) {
    let response = rpc(
        addr,
        capability,
        "tools/call",
        json!({"name": name, "arguments": arguments}),
    );
    let result = &response["result"];
    let content = result["content"]
        .as_array()
        .unwrap_or_else(|| panic!("{name}: no tool result: {response:#}"));
    assert_eq!(content.len(), 1, "{name}: {response:#}");
    let text = content[0]["text"].as_str().unwrap();
    (
        result["isError"] == true,
        serde_json::from_str(text).unwrap_or_else(|error| panic!("{name}: {error}: {text:?}")),
    )
}

/// A successful call's row id under `key`, asserting the exact result shape.
fn accepted(addr: SocketAddr, capability: &str, name: &str, arguments: Value, key: &str) -> i64 {
    let (is_error, value) = call(addr, capability, name, arguments);
    assert!(!is_error, "{name}: {value}");
    let id = value[key]
        .as_i64()
        .unwrap_or_else(|| panic!("{name}: {value}"));
    assert_eq!(value, json!({"ok": true, key: id}), "{name}");
    assert!(id > 0);
    id
}

/// A refused call's `code`, asserting the `{code,message}` error shape.
fn refused(addr: SocketAddr, capability: &str, name: &str, arguments: Value) -> (String, String) {
    let (is_error, value) = call(addr, capability, name, arguments);
    assert!(is_error, "{name} unexpectedly succeeded: {value}");
    let object = value.as_object().unwrap();
    assert_eq!(
        object.keys().collect::<Vec<_>>(),
        vec!["code", "message"],
        "{name}: {value}"
    );
    (
        value["code"].as_str().unwrap().to_string(),
        value["message"].as_str().unwrap().to_string(),
    )
}

/// Row counts of every table a signalling tool can write.
fn written_rows(db: &Path) -> (i64, i64, i64) {
    let conn = quorum_core::db::open(db).unwrap();
    let count = |table: &str| -> i64 {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    };
    (count("mailbox"), count("task_notes"), count("messages"))
}

type MailboxShape = (
    String,
    String,
    i64,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// `(agent, kind, task_id, pr, verdict, feedback, note, payload)`.
fn mailbox_row(db: &Path, id: i64) -> MailboxShape {
    quorum_core::db::open(db)
        .unwrap()
        .query_row(
            "SELECT agent,kind,task_id,pr,verdict,feedback,note,payload FROM mailbox WHERE id=?1",
            [id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .unwrap()
}

fn note_row(db: &Path, id: i64) -> (i64, String, String) {
    quorum_core::db::open(db)
        .unwrap()
        .query_row(
            "SELECT task_id,agent,body FROM task_notes WHERE id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

const WORKER_SIGNALS: [&str; 3] = ["submit_work", "react", "task_note"];

#[test]
fn http_listener_derives_authority_per_request_and_signals_the_daemon() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let repo = root.path().join("repo");
    let worktrees = root.path().join("worktrees");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::create_dir_all(&worktrees).unwrap();
    init_git_repo(&repo);
    let names = root.path().join("names.txt");
    std::fs::write(&names, "AgentOne\nAgentTwo\n").unwrap();
    let config = root.path().join("serve.toml");
    write_config(&config);
    std::fs::write(
        home.join("config.toml"),
        format!("message_ttl_secs = {CONFIGURED_MESSAGE_TTL_SECS}\n"),
    )
    .unwrap();
    let daemon = Daemon::start(&home, &repo, &worktrees, &names, &config);
    daemon.wait_for("serving (cap=1)");
    let db = home.join("repos/test__repo/quorum.db");
    let endpoint = endpoint_path(&db);
    install_fixtures(&db);

    // The listener's own environment names a different live run. No request
    // below may be served under it.
    let shell = HttpShell::start(
        &endpoint,
        &[("QUORUM_RUN_ID", "worker-cap"), ("QUORUM_AGENT", "Worker")],
    );
    let addr = shell.addr;
    assert!(addr.ip().is_loopback());

    // Stateless HTTP advertises tools and nothing it cannot deliver.
    let initialized = rpc(
        addr,
        "worker-cap",
        "initialize",
        json!({
            "protocolVersion":"2025-06-18",
            "capabilities":{},
            "clientInfo":{"name":"agent-mcp-http-test","version":"1"}
        }),
    );
    assert_eq!(initialized["result"]["serverInfo"]["name"], "quorum");
    let capabilities = initialized["result"]["capabilities"].as_object().unwrap();
    assert_eq!(capabilities.keys().collect::<Vec<_>>(), vec!["tools"]);
    assert_ne!(
        capabilities["tools"]["listChanged"], true,
        "{initialized:#}"
    );

    // Inventory per phase: the daemon-derived operations, then the signals.
    let worker_tools = list_tools(addr, "worker-cap");
    assert_eq!(
        tool_names(&worker_tools),
        [&["delivery_report_write"][..], &WORKER_SIGNALS[..]].concat()
    );
    assert_eq!(
        tool_names(&list_tools(addr, "rework-cap")),
        [
            &[
                "pull_request_read",
                "add_issue_comment",
                "add_reply_to_pull_request_comment",
                "github_operation_read",
                "delivery_report_write"
            ][..],
            &WORKER_SIGNALS[..]
        ]
        .concat()
    );
    let reviewer_tools = list_tools(addr, "reviewer-cap");
    assert_eq!(
        tool_names(&reviewer_tools),
        vec![
            "pull_request_read",
            "add_issue_comment",
            "pull_request_review_write",
            "add_comment_to_pending_review",
            "add_reply_to_pull_request_comment",
            "resolve_review_thread",
            "github_operation_read",
            "submit_review",
            "task_note",
            "alert_owner"
        ]
    );
    for (listed, signals) in [
        (&worker_tools, &WORKER_SIGNALS[..]),
        (
            &reviewer_tools,
            &["submit_review", "task_note", "alert_owner"][..],
        ),
    ] {
        for name in signals {
            let schema = listed["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .find(|tool| tool["name"] == *name)
                .unwrap()["inputSchema"]
                .clone();
            assert_eq!(schema["type"], "object", "{name}");
            assert_eq!(schema["additionalProperties"], false, "{name}");
            for (property, shape) in schema["properties"].as_object().unwrap() {
                if shape["type"] == "string" && shape.get("enum").is_none() {
                    assert!(
                        shape["maxLength"].as_u64().unwrap() > 0,
                        "{name}.{property}"
                    );
                }
            }
        }
    }
    for capability in ["unknown-cap", "revoked-cap"] {
        let rejected = list_tools(addr, capability);
        assert_eq!(rejected["error"]["code"], -32600, "{rejected:#}");
        assert_eq!(rejected["error"]["message"], "run authority rejected");
        assert!(!rejected.to_string().contains(capability));
    }

    // Wrong-role, unknown, and revoked authority: refused by the daemon with
    // nothing written anywhere.
    let before = written_rows(&db);
    for (capability, name, arguments) in [
        (
            "worker-cap",
            "submit_review",
            json!({"verdict":"approved","blocking":0}),
        ),
        (
            "worker-cap",
            "alert_owner",
            json!({"body":"not a reviewer"}),
        ),
        (
            "rework-cap",
            "alert_owner",
            json!({"body":"not a reviewer"}),
        ),
        (
            "reviewer-cap",
            "submit_work",
            json!({"summary":"not a worker"}),
        ),
        (
            "reviewer-cap",
            "react",
            json!({"state":"failed","reason":"not a worker"}),
        ),
        ("unknown-cap", "task_note", json!({"note":"nobody"})),
        ("unknown-cap", "alert_owner", json!({"body":"nobody"})),
        ("revoked-cap", "submit_work", json!({})),
        ("revoked-cap", "task_note", json!({"note":"revoked"})),
    ] {
        let (code, message) = refused(addr, capability, name, arguments);
        assert_eq!(code, "unauthorized", "{capability} {name}");
        assert_eq!(message, "run authority rejected", "{capability} {name}");
    }
    // Arguments outside a tool's closed shape or the CLI's rules never reach
    // the daemon.
    for (capability, name, arguments) in [
        ("worker-cap", "submit_work", json!({"summary":"x","pr":7})),
        ("worker-cap", "submit_work", json!({"summary":7})),
        ("worker-cap", "react", json!({"state":"blocked"})),
        (
            "worker-cap",
            "react",
            json!({"state":"blocked","reason":"  "}),
        ),
        ("worker-cap", "react", json!({"state":"done"})),
        ("worker-cap", "task_note", json!({})),
        ("worker-cap", "task_note", json!({"note":"n","task_id":201})),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"approved"}),
        ),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"approved","blocking":1}),
        ),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"approved","blocking":0,"feedback":"extra"}),
        ),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"changes","blocking":1}),
        ),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"changes","blocking":0,"feedback":"contradiction"}),
        ),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"changes","blocking":-1,"feedback":"negative"}),
        ),
        ("reviewer-cap", "submit_review", json!({"verdict":"merge"})),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"graph-blocker"}),
        ),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"graph-blocker","feedback_json":{"category":"nope"}}),
        ),
        (
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"changes","blocking":1,"feedback":"f","feedback_json":{}}),
        ),
        (
            "reviewer-cap",
            "alert_owner",
            json!({"body":"b","to":"other"}),
        ),
    ] {
        let (code, _) = refused(addr, capability, name, arguments.clone());
        assert_eq!(code, "invalid_arguments", "{capability} {name} {arguments}");
    }
    // These pass the client-side shape and are refused by the daemon's own
    // re-validation, relayed verbatim.
    let graph_blocker = json!({
        "category": quorum_core::decomposition::GRAPH_BLOCKER_CATEGORY_BOUNDARY_VIOLATION,
        "affected_task": 999,
        "violated_assigned_boundary": "src/other.rs",
        "evidence": ["the diff edits src/other.rs"]
    });
    assert_eq!(
        refused(
            addr,
            "reviewer-cap",
            "submit_review",
            json!({"verdict":"graph-blocker","feedback_json":graph_blocker}),
        ),
        (
            "invalid_operation".to_string(),
            "graph-blocker task does not match this run".to_string()
        )
    );
    assert_eq!(
        refused(addr, "reviewer-cap", "alert_owner", json!({"body":""})),
        (
            "invalid_operation".to_string(),
            "invalid owner alert".to_string()
        )
    );
    assert_eq!(
        refused(addr, "worker-cap", "task_note", json!({"note":""})),
        (
            "invalid_operation".to_string(),
            "invalid progress note".to_string()
        )
    );
    // Larger than one endpoint frame: refused before anything is sent.
    assert_eq!(
        refused(
            addr,
            "worker-cap",
            "task_note",
            json!({"note":"n".repeat(70 * 1024)}),
        )
        .0,
        "request_too_large"
    );
    // The same runs over stdio keep exactly their pre-existing inventories:
    // no signalling tool is advertised or routed there.
    assert_stdio_inventory(
        &endpoint,
        "worker-cap",
        "Worker",
        &["delivery_report_write"],
    );
    assert_stdio_inventory(
        &endpoint,
        "reviewer-cap",
        "Reviewer",
        &[
            "pull_request_read",
            "add_issue_comment",
            "pull_request_review_write",
            "add_comment_to_pending_review",
            "add_reply_to_pull_request_comment",
            "resolve_review_thread",
            "github_operation_read",
        ],
    );
    // The stub operations still answer exactly as they do over stdio.
    let stub = rpc(
        addr,
        "worker-cap",
        "tools/call",
        json!({
            "name":"delivery_report_write",
            "arguments":{"summary":"done","changes":[],"verification":[]}
        }),
    );
    assert_eq!(stub["result"]["isError"], true);
    assert_eq!(
        stub["result"]["content"][0]["text"],
        "tool execution is not available in this delivery"
    );
    let unknown = rpc(
        addr,
        "worker-cap",
        "tools/call",
        json!({"name":"raw_http","arguments":{}}),
    );
    assert_eq!(unknown["error"]["code"], -32601);
    assert_eq!(
        written_rows(&db),
        before,
        "a refused call must write no mailbox, note, or message row"
    );

    // task_note: each run's note lands on its own task as its own agent.
    for (capability, task_id, agent) in [
        ("worker-cap", 200, "Worker"),
        ("rework-cap", 204, "Rework"),
        ("reviewer-cap", 201, "Reviewer"),
    ] {
        let body = format!("progress from {capability}");
        let note_id = accepted(
            addr,
            capability,
            "task_note",
            json!({"note": body}),
            "note_id",
        );
        assert_eq!(note_row(&db, note_id), (task_id, agent.to_string(), body));
    }

    // Two runs interleaved on one listener, concurrently: every note must
    // carry the identity of the bearer that sent it, never the other's.
    let rounds = 12;
    let handles: Vec<_> = [
        ("worker-cap", 200_i64, "Worker"),
        ("reviewer-cap", 201_i64, "Reviewer"),
    ]
    .into_iter()
    .map(|(capability, task_id, agent)| {
        let db = db.clone();
        thread::spawn(move || {
            for round in 0..rounds {
                let body = format!("isolation {capability} {round}");
                let note_id = accepted(
                    addr,
                    capability,
                    "task_note",
                    json!({"note": body}),
                    "note_id",
                );
                assert_eq!(note_row(&db, note_id), (task_id, agent.to_string(), body));
                let names: Vec<String> = tool_names(&list_tools(addr, capability))
                    .into_iter()
                    .map(str::to_string)
                    .collect();
                assert_eq!(
                    names.iter().any(|name| name == "submit_review"),
                    agent == "Reviewer",
                    "{capability} saw another run's inventory: {names:?}"
                );
            }
        })
    })
    .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    let conn = quorum_core::db::open(&db).unwrap();
    for (task_id, agent, other) in [
        (200, "Worker", "reviewer-cap"),
        (201, "Reviewer", "worker-cap"),
    ] {
        let (own, foreign): (i64, i64) = conn
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(agent<>?2 OR body LIKE ?3),0)
                 FROM task_notes WHERE task_id=?1 AND body LIKE 'isolation %'",
                params![task_id, agent, format!("isolation {other} %")],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((own, foreign), (rounds, 0), "task {task_id}");
    }
    drop(conn);

    // alert_owner: one critical message to the owner, authored by the
    // reviewer, expiring after the operator's configured message TTL.
    let seq = accepted(
        addr,
        "reviewer-cap",
        "alert_owner",
        json!({"body":"merge is unsafe"}),
        "seq",
    );
    type MessageShape = (String, String, String, Option<String>, i64, i64);
    let message: MessageShape = quorum_core::db::open(&db)
        .unwrap()
        .query_row(
            "SELECT author,kind,body,recipient,ts,expires_at FROM messages WHERE seq=?1",
            [seq],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        (&message.0[..], &message.1[..], &message.2[..]),
        ("Reviewer", "critical", "merge is unsafe")
    );
    assert_eq!(message.3.as_deref(), Some("owner"));
    assert_eq!(message.5 - message.4, CONFIGURED_MESSAGE_TTL_SECS);

    // react: a worker-phase reaction, for the initial and the rework worker.
    for (capability, task_id, agent) in
        [("worker-cap", 200, "Worker"), ("rework-cap", 204, "Rework")]
    {
        let id = accepted(
            addr,
            capability,
            "react",
            json!({"state":"note","reason":"  still going  "}),
            "mailbox_id",
        );
        assert_eq!(
            mailbox_row(&db, id),
            (
                agent.to_string(),
                "task_update".to_string(),
                task_id,
                None,
                None,
                Some("still going".to_string()),
                Some("note".to_string()),
                None,
            )
        );
    }

    // submit_review: the verdict, its feedback, and the blocking attestation.
    let review = accepted(
        addr,
        "reviewer-cap",
        "submit_review",
        json!({"verdict":"changes","blocking":2,"feedback":"Two blocking issues"}),
        "mailbox_id",
    );
    assert_eq!(
        mailbox_row(&db, review),
        (
            "Reviewer".to_string(),
            "done".to_string(),
            201,
            Some(77),
            Some("changes".to_string()),
            Some("Two blocking issues".to_string()),
            None,
            Some(r#"{"blocking":2}"#.to_string()),
        )
    );

    // submit_work: initial and rework worker, with and without a summary.
    let initial = accepted(
        addr,
        "worker-cap",
        "submit_work",
        json!({"summary":"Delivered over HTTP"}),
        "mailbox_id",
    );
    assert_eq!(
        mailbox_row(&db, initial),
        (
            "Worker".to_string(),
            "done".to_string(),
            200,
            None,
            None,
            None,
            Some("Delivered over HTTP".to_string()),
            None,
        )
    );
    let rework = accepted(addr, "rework-cap", "submit_work", json!({}), "mailbox_id");
    assert_eq!(
        mailbox_row(&db, rework),
        (
            "Rework".to_string(),
            "done".to_string(),
            204,
            Some(42),
            None,
            None,
            None,
            None,
        )
    );

    shell.stop();
    daemon.stop();
}

/// A length-prefixed JSON stand-in for the daemon endpoint that answers as a
/// live planner for `planner-cap` and rejects every other capability. A real
/// daemon's planner scheduler would immediately claim a planner fixture.
struct FakeEndpoint {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl FakeEndpoint {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("endpoint.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
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
                let response = serde_json::to_vec(&fake_response(&request)).unwrap();
                let _ = stream.write_all(&(response.len() as u32).to_be_bytes());
                let _ = stream.write_all(&response);
            }
        });
        Self { _dir: dir, path }
    }
}

fn fake_response(request: &Value) -> Value {
    let unauthorized = json!({
        "version": 1,
        "ok": false,
        "error": {"code": "unauthorized", "message": "run authority rejected"}
    });
    if request["capability"] != "planner-cap" {
        return unauthorized;
    }
    match request["operation"]["type"].as_str().unwrap() {
        "inventory" => json!({
            "version": 1,
            "ok": true,
            "result": {
                "type": "inventory",
                "repository": "test/repo",
                "task_id": 1,
                "role": "planner",
                "pr": null,
                "review_revision": null,
                "phase": "planner",
                "operations": []
            }
        }),
        "submit_plan" => {
            json!({"version":1,"ok":true,"result":{"type":"plan_accepted","graph_id":5}})
        }
        _ => unauthorized,
    }
}

#[test]
fn http_planner_inventory_is_submit_plan_only() {
    let endpoint = FakeEndpoint::start();
    let shell = HttpShell::start(&endpoint.path, &[]);
    let addr = shell.addr;

    assert_eq!(
        tool_names(&list_tools(addr, "planner-cap")),
        vec!["submit_plan"]
    );
    let (is_error, value) = call(
        addr,
        "planner-cap",
        "submit_plan",
        json!({"response":{"outcome":"plan","tasks":[]}}),
    );
    assert!(!is_error, "{value}");
    assert_eq!(value, json!({"accepted": true, "graph_id": 5}));

    // A planner holds no signalling authority, and no other bearer borrows
    // the planner's door.
    for name in ["submit_work", "task_note", "alert_owner"] {
        let arguments = match name {
            "task_note" => json!({"note":"n"}),
            "alert_owner" => json!({"body":"b"}),
            _ => json!({}),
        };
        assert_eq!(
            refused(addr, "planner-cap", name, arguments).0,
            "unauthorized",
            "{name}"
        );
    }
    assert_eq!(
        refused(
            addr,
            "someone-else",
            "submit_plan",
            json!({"response":{"outcome":"plan","tasks":[]}}),
        )
        .0,
        "unauthorized"
    );
    shell.stop();
}

#[test]
fn http_requests_without_a_well_formed_bearer_get_401() {
    // No endpoint is listening: a request that passes the bearer gate reaches
    // MCP and reports the dead endpoint, which distinguishes it from a 401.
    let dir = tempfile::tempdir().unwrap();
    let dead_endpoint = dir.path().join("absent.sock");
    let shell = HttpShell::start(&dead_endpoint, &[]);
    let addr = shell.addr;
    let list = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});

    let oversized = format!("Bearer {}", "c".repeat(129));
    let cases: Vec<Vec<&str>> = vec![
        vec![],
        vec!["worker-cap"],
        vec!["Basic d29ya2VyLWNhcDo="],
        vec!["Bearer"],
        vec!["Bearer "],
        vec!["Bearer  worker-cap"],
        vec!["Bearer worker cap"],
        vec!["Bearer worker\tcap"],
        vec!["Bearerworker-cap"],
        vec![&oversized],
        vec!["Bearer worker-cap", "Bearer reviewer-cap"],
    ];
    for authorization in cases {
        let (status, body) = post_mcp(addr, &authorization, &list);
        assert_eq!(status, 401, "{authorization:?}: {body}");
        assert_eq!(body, "Unauthorized", "{authorization:?}");
    }
    // The gate covers every method and path, not only a well-formed POST.
    assert_eq!(http(addr, "GET", "/mcp", &[], "").0, 401);
    assert_eq!(http(addr, "POST", "/", &MCP_HEADERS, "{}").0, 401);

    // A well-formed bearer at the size limit passes the gate.
    let at_limit = format!("Bearer {}", "c".repeat(128));
    let (status, body) = post_mcp(addr, &[&at_limit], &list);
    assert_eq!(status, 200, "{body}");
    let response: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(response["error"]["message"], "agent endpoint unavailable");
    assert!(!body.contains(&"c".repeat(128)));
    let (status, _) = post_mcp(addr, &["bearer worker-cap"], &list);
    assert_eq!(status, 200, "the Bearer scheme is case-insensitive");
    // Only `/mcp` is served, and only by POST.
    let authorized = [
        MCP_HEADERS[0],
        MCP_HEADERS[1],
        ("Authorization", "Bearer x"),
    ];
    assert_eq!(http(addr, "POST", "/", &authorized, "{}").0, 404);
    assert_eq!(http(addr, "GET", "/mcp", &authorized, "").0, 405);

    // With no daemon answering, a signal's outcome is reported as unknown.
    let (code, message) = refused(addr, "worker-cap", "task_note", json!({"note":"n"}));
    assert_eq!(code, "transport");
    assert!(!message.contains("worker-cap"));
    shell.stop();
}

#[test]
fn listen_refuses_non_loopback_and_malformed_addresses() {
    for addr in ["0.0.0.0:0", "192.0.2.1:8080", "[::]:0", "localhost", "8080"] {
        let output = Command::new(quorum_bin())
            .args(["agent-mcp", "--listen", addr])
            .env("QUORUM_AGENT_ENDPOINT", "/nonexistent/endpoint.sock")
            .env("QUORUM_REPO", "test/repo")
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{addr}: {output:?}");
        assert!(output.stdout.is_empty(), "{addr}");
    }
    // HTTP mode needs the endpoint locator and repository, and only those.
    for missing in ["QUORUM_AGENT_ENDPOINT", "QUORUM_REPO"] {
        let output = Command::new(quorum_bin())
            .args(["agent-mcp", "--listen", "127.0.0.1:0"])
            .env("QUORUM_AGENT_ENDPOINT", "/nonexistent/endpoint.sock")
            .env("QUORUM_REPO", "test/repo")
            .env_remove(missing)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{missing}: {output:?}");
    }
}

const SIGNAL_TOOLS: [&str; 5] = [
    "submit_work",
    "submit_review",
    "react",
    "task_note",
    "alert_owner",
];

/// Drive one stdio shell for `capability`: it must advertise
/// `tools/list_changed`, list exactly `expected`, and treat every signalling
/// tool name as unknown.
fn assert_stdio_inventory(endpoint: &Path, capability: &str, agent: &str, expected: &[&str]) {
    let mut child = Command::new(quorum_bin())
        .arg("agent-mcp")
        .env("QUORUM_AGENT_ENDPOINT", endpoint)
        .env("QUORUM_RUN_ID", capability)
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_AGENT", agent)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut request = |id: i64, method: &str, params: Value| -> Value {
        let message = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        writeln!(input, "{message}").unwrap();
        input.flush().unwrap();
        loop {
            let mut line = String::new();
            assert!(
                output.read_line(&mut line).unwrap() > 0,
                "stdio shell exited"
            );
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["id"] == id {
                return value;
            }
        }
    };
    let initialized = request(
        1,
        "initialize",
        json!({
            "protocolVersion":"2025-06-18",
            "capabilities":{},
            "clientInfo":{"name":"agent-mcp-http-test","version":"1"}
        }),
    );
    assert_eq!(
        initialized["result"]["capabilities"]["tools"]["listChanged"], true,
        "{capability}"
    );
    let listed = request(2, "tools/list", json!({}));
    assert_eq!(tool_names(&listed), expected, "{capability}");
    for (id, name) in SIGNAL_TOOLS.into_iter().enumerate() {
        let response = request(
            10 + id as i64,
            "tools/call",
            json!({"name": name, "arguments": {}}),
        );
        assert_eq!(response["error"]["code"], -32601, "{name}: {response:#}");
        assert_eq!(response["error"]["message"], "unknown tool", "{name}");
    }
    drop(input);
    assert!(child.wait().unwrap().success());
}

/// The signalling tools exist only over HTTP. A stdio planner shell
/// advertises exactly its pre-existing inventory and treats the new names as
/// unknown. The worker and reviewer stdio inventories are asserted against
/// the real daemon in the listener test above.
#[test]
fn stdio_inventory_and_routing_are_unchanged() {
    let endpoint = FakeEndpoint::start();
    assert_stdio_inventory(&endpoint.path, "planner-cap", "Planner", &["submit_plan"]);
}

/// A raw request on its own connection; `None` when the server closed the
/// connection without answering.
fn raw_exchange(addr: SocketAddr, request: &[u8], read_timeout: Duration) -> Option<u16> {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(read_timeout)).unwrap();
    stream.write_all(request).unwrap();
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .expect("no response and no close");
    String::from_utf8_lossy(&raw)
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
}

const PARTIAL_HEAD: &[u8] = b"POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\n";
const PARTIAL_BODY: &[u8] = b"POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\n\
    Authorization: Bearer x\r\nContent-Type: application/json\r\n\
    Accept: application/json, text/event-stream\r\nContent-Length: 100\r\n\r\n{";

#[test]
fn http_rejects_foreign_hosts_and_oversized_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let shell = HttpShell::start(&dir.path().join("absent.sock"), &[]);
    let addr = shell.addr;
    let list = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;

    // DNS-rebinding guard: a request naming a non-loopback host is refused
    // even with a well-formed bearer; the loopback names are accepted.
    for (host, status) in [
        ("evil.example", 403),
        ("192.0.2.1", 403),
        (&format!("evil.example:{}", addr.port())[..], 403),
        (&format!("localhost:{}", addr.port())[..], 200),
        (&addr.to_string()[..], 200),
    ] {
        let request = format!(
            "POST /mcp HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
             Authorization: Bearer x\r\nContent-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\nContent-Length: {}\r\n\r\n{list}",
            list.len()
        );
        assert_eq!(
            raw_exchange(addr, request.as_bytes(), Duration::from_secs(10)),
            Some(status),
            "Host: {host}"
        );
    }

    // The body cap is 256 KiB. Just under it is read and parsed; over it is
    // refused with 413 whatever it contains.
    let body = |bytes: usize| {
        json!({
            "jsonrpc":"2.0","id":1,"method":"tools/call",
            "params":{"name":"task_note","arguments":{"note":"n".repeat(bytes)}}
        })
    };
    let (status, _) = post_mcp(addr, &["Bearer x"], &body(255 * 1024));
    assert_eq!(status, 200);
    let (status, text) = post_mcp(addr, &["Bearer x"], &body(257 * 1024));
    assert_eq!(status, 413, "{}", &text[..text.len().min(200)]);
    shell.stop();
}

/// Without a bound, one peer that stops mid-request would keep the listener
/// alive forever after SIGTERM. Shutdown must finish within the drain bound
/// (5s) and still exit 0.
#[test]
fn sigterm_exits_within_the_drain_bound_with_half_sent_requests_open() {
    for payload in [&b""[..], PARTIAL_HEAD, PARTIAL_BODY] {
        let dir = tempfile::tempdir().unwrap();
        let shell = HttpShell::start(&dir.path().join("absent.sock"), &[]);
        let mut stalled = TcpStream::connect(shell.addr).unwrap();
        stalled.write_all(payload).unwrap();
        thread::sleep(Duration::from_millis(300));
        let started = Instant::now();
        // `stop` asserts exit code 0.
        shell.stop();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(8),
            "shutdown took {elapsed:?} with {} stalled bytes",
            payload.len()
        );
        drop(stalled);
    }
}

/// At most eight connections are served at once. A ninth is not refused and
/// not served: it waits in the listen backlog until a slot frees.
#[test]
fn connections_beyond_the_cap_wait_for_a_free_slot() {
    let dir = tempfile::tempdir().unwrap();
    let shell = HttpShell::start(&dir.path().join("absent.sock"), &[]);
    let addr = shell.addr;
    // Eight stalled request heads hold every slot (for up to the 10s
    // header-read bound, well beyond this test's own waits).
    let mut holders: Vec<TcpStream> = (0..8)
        .map(|_| {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.write_all(PARTIAL_HEAD).unwrap();
            stream
        })
        .collect();
    thread::sleep(Duration::from_millis(300));

    let list = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#;
    let mut ninth = TcpStream::connect(addr).unwrap();
    ninth
        .write_all(
            format!(
                "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\
                 Authorization: Bearer x\r\nContent-Type: application/json\r\n\
                 Accept: application/json, text/event-stream\r\nContent-Length: {}\r\n\r\n{list}",
                list.len()
            )
            .as_bytes(),
        )
        .unwrap();
    ninth
        .set_read_timeout(Some(Duration::from_millis(1500)))
        .unwrap();
    let mut byte = [0_u8; 1];
    let waiting = ninth.read(&mut byte).unwrap_err();
    assert!(
        matches!(
            waiting.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ),
        "the ninth connection was answered or closed while all slots were held: {waiting}"
    );

    // Freeing one slot lets the queued request through, complete and intact.
    drop(holders.pop());
    ninth
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut raw = Vec::new();
    ninth.read_to_end(&mut raw).unwrap();
    let raw = String::from_utf8(raw).unwrap();
    assert!(raw.starts_with("HTTP/1.1 200"), "{raw}");
    assert!(raw.contains("agent endpoint unavailable"), "{raw}");
    drop(holders);
    shell.stop();
}

/// A connection that never completes its request head is closed after the
/// 10s header-read bound, and a request whose body never arrives is answered
/// 408 after the 15s request bound; neither holds its slot indefinitely.
#[test]
fn stalled_requests_are_cut_off_at_their_time_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let shell = HttpShell::start(&dir.path().join("absent.sock"), &[]);
    let addr = shell.addr;
    let started = Instant::now();
    let head = thread::spawn(move || {
        let status = raw_exchange(addr, PARTIAL_HEAD, Duration::from_secs(25));
        (status, started.elapsed())
    });
    let body = thread::spawn(move || {
        let status = raw_exchange(addr, PARTIAL_BODY, Duration::from_secs(30));
        (status, started.elapsed())
    });
    let (head_status, head_elapsed) = head.join().unwrap();
    assert_ne!(head_status, Some(200));
    assert!(
        (Duration::from_secs(9)..Duration::from_secs(14)).contains(&head_elapsed),
        "stalled head ended after {head_elapsed:?} with {head_status:?}"
    );
    let (body_status, body_elapsed) = body.join().unwrap();
    assert_eq!(body_status, Some(408));
    assert!(
        (Duration::from_secs(14)..Duration::from_secs(20)).contains(&body_elapsed),
        "stalled body ended after {body_elapsed:?}"
    );
    shell.stop();
}
