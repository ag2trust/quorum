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
        Self::start_with_env(home, repo, worktrees, names, config, &[])
    }

    fn start_with_env(
        home: &Path,
        repo: &Path,
        worktrees: &Path,
        names: &Path,
        config: &Path,
        env: &[(&str, String)],
    ) -> Self {
        let mut command = Command::new(quorum_bin());
        for (name, value) in env {
            command.env(name, value);
        }
        let mut child = command
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

    // github_operation_read: the bearer's own run reads its operation; any
    // other run is refused without content, whatever transport carried it.
    {
        const FAR: i64 = 4_102_444_800;
        let conn = quorum_core::db::open(&db).unwrap();
        conn.execute(
            "INSERT INTO github_collaboration_attempts(
                 attempt_id,task_id,agent,role,pr_number,lifecycle_generation,
                 active_run_id,state,created_at,updated_at,expires_at)
             VALUES ('attempt-rw',204,'Rework','worker',42,0,'rework-cap','active',1001,1001,?1)",
            [FAR],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO github_agent_operations(
                 operation_id,client_request_id,attempt_id,created_by_run_id,task_id,agent,
                 role,pr_number,kind,request_json,state,send_state,attempts,deadline_at,
                 github_marker,remote_object_id,response_json,created_at,updated_at,expires_at)
             VALUES ('op-rw','op-rw','attempt-rw','rework-cap',204,'Rework','worker',42,
                     'add_issue_comment','{\"body\":\"must-not-leak\"}','succeeded',
                     'confirmed',1,?1,'marker-must-not-leak','IC_rework','{\"id\":9}',
                     1001,1001,?1)",
            [FAR],
        )
        .unwrap();
        // A write whose send timed out after it may have reached GitHub.
        conn.execute(
            "INSERT INTO github_agent_operations(
                 operation_id,client_request_id,attempt_id,created_by_run_id,task_id,agent,
                 role,pr_number,kind,request_json,state,send_state,attempts,deadline_at,
                 error_kind,error_summary,created_at,updated_at,expires_at)
             VALUES ('op-rw-timeout','op-rw-timeout','attempt-rw','rework-cap',204,'Rework',
                     'worker',42,'add_issue_comment','{\"body\":\"b\"}','failed',
                     'ambiguous',1,?1,'timed_out','write timed out',1001,1001,?1)",
            [FAR],
        )
        .unwrap();
    }
    let read_arguments = json!({"attemptId":"attempt-rw","operationId":"op-rw"});
    let (is_error, owned) = call(
        addr,
        "rework-cap",
        "github_operation_read",
        read_arguments.clone(),
    );
    assert!(!is_error, "{owned:#}");
    assert_eq!(
        owned,
        json!({
            "attempt_id":"attempt-rw",
            "operation_id":"op-rw",
            "state":"succeeded",
            "send_state":"confirmed",
            "remote_object_id":"IC_rework",
            "result":{"id":9}
        })
    );
    assert!(!owned.to_string().contains("must-not-leak"));
    // A settled failure read later is a typed tool error, never success: the
    // pending path of a write that outlived the admitting call's wait.
    let (is_error, timed_out) = call(
        addr,
        "rework-cap",
        "github_operation_read",
        json!({"attemptId":"attempt-rw","operationId":"op-rw-timeout"}),
    );
    assert!(is_error, "{timed_out:#}");
    assert_eq!(timed_out["code"], "github_outcome_unknown", "{timed_out:#}");
    assert_eq!(timed_out["operation_id"], "op-rw-timeout");
    assert_eq!(timed_out["state"], "failed");
    assert_eq!(timed_out["send_state"], "ambiguous");
    assert_eq!(timed_out["retrying"], false);
    assert_eq!(timed_out["error_summary"], "write timed out");
    for (capability, arguments, message) in [
        (
            "reviewer-cap",
            read_arguments.clone(),
            "operation read rejected",
        ),
        (
            "worker-cap",
            read_arguments.clone(),
            "tool is forbidden for the current run phase",
        ),
        (
            "rework-cap",
            json!({"attemptId":"attempt-rw","operationId":"op-unknown"}),
            "operation not found",
        ),
    ] {
        let refused = rpc(
            addr,
            capability,
            "tools/call",
            json!({"name":"github_operation_read","arguments":arguments}),
        );
        assert_eq!(refused["result"]["isError"], true, "{refused:#}");
        assert_eq!(
            refused["result"]["content"][0]["text"], message,
            "{capability}: {refused:#}"
        );
    }

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

/// A `gh` stand-in for the daemon's Repository Service: it answers the
/// credential lookup, one files page for PR 77, and one comment POST whose
/// stdin it records. Anything else fails like an unsupported call.
fn install_fake_gh(dir: &Path) {
    let script = format!(
        r#"#!/bin/sh
if [ "$1" = auth ] && [ "$2" = token ]; then echo fake-gh-token; exit 0; fi
[ "$GH_TOKEN" = fake-gh-token ] || {{ echo "missing token" >&2; exit 7; }}
echo "$@" >> '{dir}/gh-args.log'
if [ "$1" = api ] && [ "$2" = --method ] && [ "$4" = repos/test/repo/issues/77/comments ]; then
  cat > '{dir}/comment-stdin.json'
  printf '%s' '{{"id":4242,"html_url":"https://github.com/test/repo/pull/77#issuecomment-4242"}}'
  exit 0
fi
if [ "$1" = api ] && [ "$2" = 'repos/test/repo/pulls/77/files?per_page=10&page=1' ]; then
  printf '%s' '[{{"filename":"src/lib.rs","status":"modified","additions":1,"deletions":0,"changes":1,"patch":"@@ -1 +1 @@\n+x"}}]'
  exit 0
fi
echo "fake gh: unsupported $*" >&2
exit 1
"#,
        dir = dir.display()
    );
    let path = dir.join("gh");
    std::fs::write(&path, script).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn pull_request_read_and_comment_run_through_the_daemon_executor() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let repo = root.path().join("repo");
    let worktrees = root.path().join("worktrees");
    let bin = root.path().join("bin");
    for dir in [&home, &repo, &worktrees, &bin] {
        std::fs::create_dir_all(dir).unwrap();
    }
    init_git_repo(&repo);
    install_fake_gh(&bin);
    let names = root.path().join("names.txt");
    std::fs::write(&names, "AgentOne\nAgentTwo\n").unwrap();
    let config = root.path().join("serve.toml");
    write_config(&config);
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let daemon =
        Daemon::start_with_env(&home, &repo, &worktrees, &names, &config, &[("PATH", path)]);
    daemon.wait_for("serving (cap=1)");
    let db = home.join("repos/test__repo/quorum.db");
    let endpoint = endpoint_path(&db);
    install_fixtures(&db);
    // The adapter holds no credential and never runs `gh` itself.
    let shell = HttpShell::start(&endpoint, &[("GH_TOKEN", "must-not-be-used")]);
    let addr = shell.addr;

    let (is_error, read) = call(
        addr,
        "reviewer-cap",
        "pull_request_read",
        json!({"method":"get_files","owner":"test","repo":"repo","pullNumber":77,
               "page":1,"perPage":10}),
    );
    assert!(!is_error, "{read:#}");
    assert_eq!(read["state"], "succeeded", "{read:#}");
    assert_eq!(read["result"]["method"], "get_files");
    assert_eq!(read["result"]["items"][0]["filename"], "src/lib.rs");
    assert_eq!(read["result"]["items"][0]["patch"], "@@ -1 +1 @@\n+x");
    assert_eq!(read["result"]["truncated"], false);

    let markdown = "## Finding\n\n`code` with \"quotes\" and 'single' $HOME $(id)\n\n\
                    ```rust\nlet x = \"y\";\n```\n\n- one\n  - two\n";
    let arguments = json!({"owner":"test","repo":"repo","issue_number":77,"body":markdown});
    let (is_error, comment) = call(addr, "reviewer-cap", "add_issue_comment", arguments.clone());
    assert!(!is_error, "{comment:#}");
    assert_eq!(comment["state"], "succeeded", "{comment:#}");
    assert_eq!(comment["send_state"], "confirmed");
    assert_eq!(comment["remote_object_id"], "4242");
    assert_eq!(comment["result"]["id"], 4242);

    let (marker, operations, attempts): (String, i64, i64) = {
        let conn = quorum_core::db::open(&db).unwrap();
        (
            conn.query_row(
                "SELECT github_marker FROM github_agent_operations WHERE kind='add_issue_comment'",
                [],
                |row| row.get(0),
            )
            .unwrap(),
            conn.query_row("SELECT COUNT(*) FROM github_agent_operations", [], |row| {
                row.get(0)
            })
            .unwrap(),
            conn.query_row(
                "SELECT COUNT(*) FROM github_collaboration_attempts",
                [],
                |row| row.get(0),
            )
            .unwrap(),
        )
    };
    let sent: Value =
        serde_json::from_slice(&std::fs::read(bin.join("comment-stdin.json")).unwrap()).unwrap();
    assert_eq!(sent, json!({"body": format!("{markdown}\n{marker}")}));
    assert!(marker.starts_with("<!-- quorum-op:"));
    let args = std::fs::read_to_string(bin.join("gh-args.log")).unwrap();
    assert!(
        !args.contains("Finding") && !args.contains("quotes"),
        "{args}"
    );
    assert_eq!((operations, attempts), (2, 1));

    // An identical retry returns the posted comment without a second POST.
    let (is_error, again) = call(addr, "reviewer-cap", "add_issue_comment", arguments);
    assert!(!is_error, "{again:#}");
    assert_eq!(again["operation_id"], comment["operation_id"]);
    let posts = std::fs::read_to_string(bin.join("gh-args.log"))
        .unwrap()
        .lines()
        .filter(|line| line.contains("--method POST"))
        .count();
    assert_eq!(posts, 1);

    // A foreign target is refused by the daemon before anything is queued.
    let (is_error, foreign) = call(
        addr,
        "reviewer-cap",
        "pull_request_read",
        json!({"method":"get","owner":"test","repo":"repo","pullNumber":78}),
    );
    assert!(is_error);
    assert_eq!(foreign["code"], "forbidden_operation", "{foreign:#}");
    let (is_error, initial) = call(
        addr,
        "worker-cap",
        "add_issue_comment",
        json!({"owner":"test","repo":"repo","issue_number":77,"body":"x"}),
    );
    assert!(is_error);
    assert_eq!(initial["code"], "forbidden_operation", "{initial:#}");
    let operations: i64 = quorum_core::db::open(&db)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM github_agent_operations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(operations, 2);

    shell.stop();
    daemon.stop();
}

/// A `gh` stand-in that holds one publisher's pending review on PR 77 at
/// `sha`: it answers the credential lookup, a files page (which creates the
/// run's collaboration attempt), the review create, the inline GraphQL
/// thread, the COMMENT submit, and the verdict's PR head fetch. Request
/// documents are recorded per step; the create and submit echo their bodies
/// back the way GitHub does.
fn install_fake_review_gh(dir: &Path, sha: &str) {
    let review = format!(
        r#""id":9001,"node_id":"PRR_kwDOfake9001","pull_request_url":"https://api.github.com/repos/test/repo/pulls/77","commit_id":"{sha}""#
    );
    let script = format!(
        r#"#!/bin/sh
if [ "$1" = auth ] && [ "$2" = token ]; then echo fake-gh-token; exit 0; fi
[ "$GH_TOKEN" = fake-gh-token ] || {{ echo "missing token" >&2; exit 7; }}
echo "$@" >> '{dir}/gh-args.log'
if [ "$1" = api ] && [ "$2" = 'repos/test/repo/pulls/77/files?per_page=10&page=1' ]; then
  printf '%s' '[]'
  exit 0
fi
if [ "$1" = api ] && [ "$2" = --method ] && [ "$4" = repos/test/repo/pulls/77/reviews ]; then
  cat > '{dir}/create-stdin.json'
  printf '%s' '{{{review},"state":"PENDING",'
  tail -c +2 '{dir}/create-stdin.json' | sed -e 's/"commit_id":"[0-9a-f]*"//' -e 's/^,//' -e 's/,}}$/}}/'
  exit 0
fi
if [ "$1" = api ] && [ "$2" = graphql ]; then
  cat > '{dir}/inline-stdin.json'
  printf '%s' '{{"data":{{"addPullRequestReviewThread":{{"thread":{{"id":"PRRT_kwDOfake1","pullRequest":{{"number":77}},"comments":{{"nodes":[{{"id":"PRRC_kwDOfake1","databaseId":5001,"body":"x","pullRequestReview":{{"id":"PRR_kwDOfake9001"}}}}]}}}}}}}}}}'
  exit 0
fi
if [ "$1" = api ] && [ "$2" = --method ] && [ "$4" = repos/test/repo/pulls/77/reviews/9001/events ]; then
  cat > '{dir}/submit-stdin.json'
  printf '%s' '{{{review},"state":"COMMENTED",'
  tail -c +2 '{dir}/submit-stdin.json'
  exit 0
fi
if [ "$1" = pr ] && [ "$2" = view ] && [ "$3" = 77 ]; then
  printf '%s' '{{"number":77,"state":"OPEN","headRefName":"feature","headRefOid":"{sha}","baseRefName":"main","isCrossRepository":false}}'
  exit 0
fi
echo "fake gh: unsupported $*" >&2
exit 1
"#,
        dir = dir.display()
    );
    let path = dir.join("gh");
    std::fs::write(&path, script).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Commit `src/lib.rs` on a `feature` branch (left checked out on `main`)
/// and return the commit SHA: the reviewer's immutable launch SHA.
fn commit_feature(repo: &Path) -> String {
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?}: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    git(&["checkout", "-q", "-b", "feature"]);
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src/lib.rs"), "one\ntwo\nthree\n").unwrap();
    git(&["add", "src/lib.rs"]);
    git(&["commit", "-q", "-m", "feature"]);
    let sha = git(&["rev-parse", "HEAD"]);
    git(&["checkout", "-q", "main"]);
    sha
}

fn stdin_json(dir: &Path, name: &str) -> Value {
    serde_json::from_slice(&std::fs::read(dir.join(name)).unwrap()).unwrap()
}

#[test]
fn pending_review_publishes_one_comment_review_through_the_daemon_executor() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let repo = root.path().join("repo");
    let worktrees = root.path().join("worktrees");
    let bin = root.path().join("bin");
    for dir in [&home, &repo, &worktrees, &bin] {
        std::fs::create_dir_all(dir).unwrap();
    }
    init_git_repo(&repo);
    let sha = commit_feature(&repo);
    install_fake_review_gh(&bin, &sha);
    let names = root.path().join("names.txt");
    std::fs::write(&names, "AgentOne\nAgentTwo\n").unwrap();
    let config = root.path().join("serve.toml");
    write_config(&config);
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let daemon =
        Daemon::start_with_env(&home, &repo, &worktrees, &names, &config, &[("PATH", path)]);
    daemon.wait_for("serving (cap=1)");
    let db = home.join("repos/test__repo/quorum.db");
    let endpoint = endpoint_path(&db);
    install_fixtures(&db);
    {
        // The reviewer was launched at the feature commit against `main`.
        let conn = quorum_core::db::open(&db).unwrap();
        conn.execute(
            "UPDATE agent_runs SET review_head_sha=?1 WHERE review_cap_run_id='reviewer-cap'",
            [&sha],
        )
        .unwrap();
        conn.execute("UPDATE tasks SET target_branch='main' WHERE id=201", [])
            .unwrap();
    }
    let shell = HttpShell::start(&endpoint, &[("GH_TOKEN", "must-not-be-used")]);
    let addr = shell.addr;

    // A first GitHub call binds the run's collaboration attempt.
    let (is_error, read) = call(
        addr,
        "reviewer-cap",
        "pull_request_read",
        json!({"method":"get_files","owner":"test","repo":"repo","pullNumber":77,
               "page":1,"perPage":10}),
    );
    assert!(!is_error && read["state"] == "succeeded", "{read:#}");
    let attempt_id = read["attempt_id"].as_str().unwrap().to_string();

    // Stand-in for the provisioning barrier (not this delivery): the real
    // probe primitives give the attempt the slot after an empty listing.
    {
        use quorum_core::review_publication_slots as slots;
        let mut conn = quorum_core::db::open(&db).unwrap();
        let now = quorum_core::clock::now();
        let scope = slots::PublisherScope::new("test-publisher").unwrap();
        let slots::ProbeStart::Claimed(claim) =
            slots::begin_probe(&mut conn, &scope, 77, 201, now).unwrap()
        else {
            panic!("probe not claimed");
        };
        let owned = slots::resolve_probe(
            &mut conn,
            &claim,
            slots::ProbeResult::Empty {
                new_attempt_id: &attempt_id,
            },
            now,
        )
        .unwrap();
        assert!(matches!(owned, slots::ProbeOutcome::Owned(_)), "{owned:?}");
    }
    let owner_marker = quorum_core::review_publication_slots::owner_marker_for_attempt(
        &quorum_core::db::open(&db).unwrap(),
        &attempt_id,
    )
    .unwrap()
    .unwrap()
    .as_str()
    .to_string();

    let target = json!({"owner":"test","repo":"repo","pullNumber":77});
    let with = |extra: Value| {
        let mut arguments = target.clone();
        arguments
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        arguments
    };
    let (is_error, create) = call(
        addr,
        "reviewer-cap",
        "pull_request_review_write",
        with(json!({"method":"create","commitID":sha})),
    );
    assert!(!is_error, "{create:#}");
    assert_eq!(create["state"], "succeeded", "{create:#}");
    assert_eq!(create["send_state"], "confirmed");
    assert_eq!(create["remote_object_id"], "9001");
    assert_eq!(
        stdin_json(&bin, "create-stdin.json"),
        json!({"commit_id": sha, "body": owner_marker})
    );

    // The verdict waits for the published COMMENT review.
    let (is_error, early) = call(
        addr,
        "reviewer-cap",
        "submit_review",
        json!({"verdict":"approved","blocking":0}),
    );
    assert!(is_error, "verdict admitted before publication: {early:#}");

    let (is_error, inline) = call(
        addr,
        "reviewer-cap",
        "add_comment_to_pending_review",
        with(
            json!({"path":"src/lib.rs","line":2,"side":"RIGHT","subjectType":"LINE",
                    "body":"Finding: `two` is wrong."}),
        ),
    );
    assert!(!is_error, "{inline:#}");
    assert_eq!(inline["state"], "succeeded", "{inline:#}");
    assert_eq!(inline["remote_object_id"], "5001");
    let inline_marker: String = quorum_core::db::open(&db)
        .unwrap()
        .query_row(
            "SELECT github_marker FROM github_agent_operations
             WHERE kind='add_comment_to_pending_review'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let thread = stdin_json(&bin, "inline-stdin.json");
    let input = &thread["variables"]["input"];
    assert_eq!(input["pullRequestReviewId"], "PRR_kwDOfake9001");
    assert_eq!(
        (
            &input["path"],
            &input["line"],
            &input["side"],
            &input["subjectType"]
        ),
        (
            &json!("src/lib.rs"),
            &json!(2),
            &json!("RIGHT"),
            &json!("LINE")
        )
    );
    assert_eq!(
        input["body"],
        format!("Finding: `two` is wrong.\n\n{inline_marker}")
    );

    let summary = "## Summary\n\nOne finding.";
    let (is_error, submit) = call(
        addr,
        "reviewer-cap",
        "pull_request_review_write",
        with(json!({"method":"submit_pending","event":"COMMENT","body":summary})),
    );
    assert!(!is_error, "{submit:#}");
    assert_eq!(submit["state"], "succeeded", "{submit:#}");
    assert_eq!(submit["result"]["state"], "COMMENTED");
    assert_eq!(submit["result"]["commit_id"], sha.as_str());
    assert_eq!(
        stdin_json(&bin, "submit-stdin.json"),
        json!({"event":"COMMENT","body": format!("{summary}\n\n{owner_marker}")})
    );

    // Each mutation was sent exactly once, in sequence order; the slot was
    // released by the submit.
    let args = std::fs::read_to_string(bin.join("gh-args.log")).unwrap();
    let mutations = args
        .lines()
        .filter(|line| line.contains("--method POST") || line.contains("graphql"))
        .collect::<Vec<_>>();
    assert_eq!(
        mutations,
        [
            "api --method POST repos/test/repo/pulls/77/reviews --input -",
            "api graphql --input -",
            "api --method POST repos/test/repo/pulls/77/reviews/9001/events --input -",
        ]
    );
    let slots: i64 = quorum_core::db::open(&db)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM github_review_publication_slots",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(slots, 0);

    // The fully published review admits the verdict.
    accepted(
        addr,
        "reviewer-cap",
        "submit_review",
        json!({"verdict":"approved","blocking":0}),
        "mailbox_id",
    );

    shell.stop();
    daemon.stop();
}
