#![cfg(unix)]

//! Real-process coverage for `quorum orchestrator-mcp --listen`: the
//! token-gated Streamable HTTP listener and its coordinator tools, driven
//! against a real repository database and, for the separation between the two
//! MCP listeners, a real `quorum serve` endpoint.

use rusqlite::params;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const TOKEN: &str = "orchestrator-test-token-0123456789abcdef";
const REPO: &str = "test/repo";

fn quorum_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
}

fn db_path(home: &Path) -> PathBuf {
    home.join("repos/test__repo/quorum.db")
}

/// A `quorum orchestrator-mcp` command with a clean authority environment.
fn orchestrator_command(home: &Path, listen: &str) -> Command {
    let mut command = Command::new(quorum_bin());
    command
        .args(["orchestrator-mcp", "--listen", listen])
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", REPO)
        .env_remove("QUORUM_RUN_ID")
        .env_remove("QUORUM_AGENT")
        .env_remove("QUORUM_AGENT_ENDPOINT")
        .env_remove("QUORUM_ORCHESTRATOR_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

/// An owner-only token file, as the listener requires.
fn write_token(dir: &Path, token: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("orchestrator.token");
    std::fs::write(&path, format!("{token}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    path
}

/// The listener serves only a repository that already has a database.
fn init(home: &Path) {
    if !db_path(home).exists() {
        let output = cli(home, &["init"]);
        assert_eq!(output.status.code(), Some(0), "{output:?}");
    }
}

/// One running listener process and the address it reported binding.
struct Listener {
    child: Child,
    addr: SocketAddr,
}

impl Listener {
    fn spawn(mut command: Command, label: &str) -> Self {
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
            .unwrap_or_else(|_| panic!("{label} reported no address"));
        let reported: SocketAddr = line
            .strip_prefix(&format!("quorum {label} listening on http://"))
            .and_then(|rest| rest.strip_suffix("/mcp"))
            .unwrap_or_else(|| panic!("unexpected listener line: {line:?}"))
            .parse()
            .unwrap();
        // A wildcard bind is reached through loopback.
        let addr = if reported.ip().is_unspecified() {
            SocketAddr::from(([127, 0, 0, 1], reported.port()))
        } else {
            reported
        };
        Self { child, addr }
    }

    /// The orchestrator listener on loopback, acting as `Coordinator`.
    fn orchestrator(home: &Path) -> Self {
        Self::orchestrator_as(home, "Coordinator")
    }

    fn orchestrator_as(home: &Path, identity: &str) -> Self {
        init(home);
        let mut command = orchestrator_command(home, "127.0.0.1:0");
        command
            .arg("--token-file")
            .arg(write_token(home, TOKEN))
            .args(["--identity", identity]);
        Self::spawn(command, "orchestrator-mcp")
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
            .expect("listener did not exit after SIGTERM");
        assert_eq!(status.code(), Some(0), "listener exit: {status}");
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// One HTTP/1.1 exchange on its own connection with an explicit `Host`.
fn http_as(
    addr: SocketAddr,
    host: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    let mut request = request.into_bytes();
    request.extend_from_slice(body);
    // A refused oversized body can be answered before it is fully written.
    let _ = stream.write_all(&request);
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed HTTP response: {raw:?}"));
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
    http_as(
        addr,
        &addr.to_string(),
        "POST",
        "/mcp",
        &headers,
        body.to_string().as_bytes(),
    )
}

/// One JSON-RPC request authorized by `token` as a bearer.
fn rpc(addr: SocketAddr, token: &str, method: &str, params: Value) -> Value {
    let (status, body) = post_mcp(
        addr,
        &[&format!("Bearer {token}")],
        &json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
    );
    assert_eq!(status, 200, "{method}: {body}");
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("{method}: {error}: {body:?}"))
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
fn call_as(addr: SocketAddr, token: &str, name: &str, arguments: Value) -> (bool, Value) {
    let response = rpc(
        addr,
        token,
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

fn ok(addr: SocketAddr, name: &str, arguments: Value) -> Value {
    let (is_error, value) = call_as(addr, TOKEN, name, arguments);
    assert!(!is_error, "{name}: {value}");
    value
}

/// A refused call's `code`, asserting the `{code,message}` error shape.
fn refused(addr: SocketAddr, name: &str, arguments: Value) -> (String, String) {
    let (is_error, value) = call_as(addr, TOKEN, name, arguments);
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

fn count(db: &Path, sql: &str) -> i64 {
    quorum_core::db::open(db)
        .unwrap()
        .query_row(sql, [], |row| row.get(0))
        .unwrap()
}

fn status_of(db: &Path, task_id: i64) -> String {
    quorum_core::db::open(db)
        .unwrap()
        .query_row("SELECT status FROM tasks WHERE id=?1", [task_id], |row| {
            row.get(0)
        })
        .unwrap()
}

fn cli(home: &Path, args: &[&str]) -> Output {
    Command::new(quorum_bin())
        .args(args)
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", REPO)
        .env_remove("QUORUM_RUN_ID")
        .env_remove("QUORUM_AGENT_ENDPOINT")
        .stdin(Stdio::null())
        .output()
        .unwrap()
}

const TOOLS: [&str; 7] = [
    "task_create",
    "task_list",
    "task_get",
    "task_log",
    "task_cancel",
    "task_retry",
    "message_agent",
];

#[test]
fn tools_run_the_coordinator_operations_against_a_real_database() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    let listener = Listener::orchestrator(home);
    let addr = listener.addr;
    let db = db_path(home);

    let initialized = rpc(
        addr,
        TOKEN,
        "initialize",
        json!({
            "protocolVersion":"2025-06-18",
            "capabilities":{},
            "clientInfo":{"name":"orchestrator-mcp-test","version":"1"}
        }),
    );
    assert_eq!(
        initialized["result"]["serverInfo"]["name"],
        "quorum-orchestrator"
    );
    let capabilities = initialized["result"]["capabilities"].as_object().unwrap();
    assert_eq!(capabilities.keys().collect::<Vec<_>>(), vec!["tools"]);
    assert_eq!(
        tool_names(&rpc(addr, TOKEN, "tools/list", json!({}))),
        TOOLS
    );

    // Intake writes exactly the row `quorum task-create` writes.
    let body = "Observed: \"x\" fails\nExpected: `y` — héllo $HOME\n";
    let created = ok(
        addr,
        "task_create",
        json!({
            "title": "Fix the thing",
            "body": body,
            "priority": 3,
            "labels": ["backend"],
            "refs": {"ticket": "T-1"},
        }),
    );
    assert_eq!(created["repo"], REPO);
    let id = created["id"].as_i64().unwrap();
    assert_eq!(created, json!({"id": id, "repo": REPO}));
    let row: (String, String, String, String, i64, String) = quorum_core::db::open(&db)
        .unwrap()
        .query_row(
            "SELECT created_by,title,body,status,priority,target_branch FROM tasks WHERE id=?1",
            [id],
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
        row,
        (
            "Coordinator".into(),
            "Fix the thing".into(),
            body.into(),
            "open".into(),
            3,
            "main".into()
        )
    );

    // Inspection agrees with the CLI reading the same database.
    let got = ok(addr, "task_get", json!({"task_id": id}));
    let from_cli = cli(home, &["task-get", "--task-id", &id.to_string()]);
    assert_eq!(from_cli.status.code(), Some(0));
    let from_cli: Value = serde_json::from_slice(&from_cli.stdout).unwrap();
    assert_eq!(got, from_cli);
    assert_eq!(got["body"], body);
    assert_eq!(got["status"], "open");

    // A review-only task carries its PR and starts in review.
    let review = ok(
        addr,
        "task_create",
        json!({"title": "Review PR #7", "review_only": 7}),
    );
    let review_id = review["id"].as_i64().unwrap();
    let review_task = ok(addr, "task_get", json!({"task_id": review_id}));
    assert_eq!(review_task["status"], "in-review");
    let refs: Value = serde_json::from_str(review_task["refs"].as_str().unwrap()).unwrap();
    assert_eq!(refs["pr"], 7);

    let listed = ok(addr, "task_list", json!({}));
    assert_eq!(listed["total"], 2);
    assert_eq!(listed["truncated"], false);
    assert_eq!(listed["tasks"][0]["id"], id, "highest priority first");
    assert!(listed["tasks"][0].get("body").is_none());
    let page = ok(addr, "task_list", json!({"limit": 1}));
    assert_eq!(page["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(page["truncated"], true);
    let open = ok(addr, "task_list", json!({"status": "open"}));
    assert_eq!(open["total"], 1);

    let log = ok(addr, "task_log", json!({"task_id": id}));
    let events = log["events"].as_array().unwrap();
    assert!(
        events.iter().any(|event| event["kind"] == "task_created"),
        "{log}"
    );
    assert!(events
        .iter()
        .all(|event| event["subject"] == format!("task#{id}")));

    // A message is one mailbox row for the daemon to deliver; nothing else.
    let sent = ok(
        addr,
        "message_agent",
        json!({"to": "AgentOne", "body": "status?"}),
    );
    let mailbox_id = sent["mailbox_id"].as_i64().unwrap();
    let message: (String, String, String, String) = quorum_core::db::open(&db)
        .unwrap()
        .query_row(
            "SELECT agent,kind,to_agent,payload FROM mailbox WHERE id=?1",
            [mailbox_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        message,
        (
            "Coordinator".into(),
            "message".into(),
            "AgentOne".into(),
            "status?".into()
        )
    );
    assert_eq!(status_of(&db, id), "open");

    // Cancellation is the guarded storage update: only the creator or the
    // assignee, only while unfinished. Every refusal leaves state alone.
    // A second listener bound to another identity is neither the creator
    // nor the assignee, so it cannot cancel this task or attach a note to it.
    let stranger = Listener::orchestrator_as(home, "Stranger");
    let notes = count(&db, "SELECT COUNT(*) FROM task_notes");
    let (code, _) = refused(
        stranger.addr,
        "task_cancel",
        json!({"task_id": id, "note": "not mine"}),
    );
    assert_eq!(code, "rejected");
    assert_eq!(status_of(&db, id), "open");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM task_notes"), notes);
    // What it creates and sends carries its own identity, not the caller's.
    let theirs = ok(stranger.addr, "task_create", json!({"title": "theirs"}))["id"]
        .as_i64()
        .unwrap();
    assert_eq!(
        ok(stranger.addr, "task_get", json!({"task_id": theirs}))["created_by"],
        "Stranger"
    );
    let (code, _) = refused(addr, "task_cancel", json!({"task_id": theirs}));
    assert_eq!(
        code, "rejected",
        "the first identity cannot cancel it either"
    );
    assert_eq!(
        ok(stranger.addr, "task_cancel", json!({"task_id": theirs}))["status"],
        "cancelled"
    );
    stranger.stop();
    let (code, _) = refused(addr, "task_cancel", json!({"task_id": 999_999}));
    assert_eq!(code, "rejected");
    // A task that is not parked is not retryable. The refusal carries the
    // command's own fields beside `code` and `message`.
    let (is_error, retry) = call_as(addr, TOKEN, "task_retry", json!({"task_id": id}));
    assert!(is_error, "{retry}");
    let from_cli = cli(
        home,
        &[
            "task-retry",
            "--task-id",
            &id.to_string(),
            "--by",
            "Coordinator",
        ],
    );
    assert_eq!(from_cli.status.code(), Some(1));
    let mut expected: Value = serde_json::from_slice(&from_cli.stdout).unwrap();
    assert_eq!(expected["outcome"], "task-retry-rejected");
    expected["code"] = json!("rejected");
    expected["message"] = expected["reason"].clone();
    assert_eq!(retry, expected);
    assert_eq!(status_of(&db, id), "open");

    let cancelled = ok(
        addr,
        "task_cancel",
        json!({"task_id": id, "note": "superseded"}),
    );
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(status_of(&db, id), "cancelled");
    let note: (String, String) = quorum_core::db::open(&db)
        .unwrap()
        .query_row(
            "SELECT agent,body FROM task_notes WHERE id=?1",
            [cancelled["note_id"].as_i64().unwrap()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(note, ("Coordinator".into(), "superseded".into()));

    // Already finished: a clean negative, and no second note.
    let notes = count(&db, "SELECT COUNT(*) FROM task_notes");
    let (code, _) = refused(addr, "task_cancel", json!({"task_id": id, "note": "again"}));
    assert_eq!(code, "rejected");
    assert_eq!(count(&db, "SELECT COUNT(*) FROM task_notes"), notes);
    let (is_error, retry) = call_as(addr, TOKEN, "task_retry", json!({"task_id": id}));
    assert!(is_error && retry["code"] == "rejected", "{retry}");
    assert_eq!(status_of(&db, id), "cancelled");

    let (code, _) = refused(addr, "task_get", json!({"task_id": 999_999}));
    assert_eq!(code, "not_found");
    let log = ok(addr, "task_log", json!({"task_id": id}));
    assert!(log["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["kind"] == "task_cancelled"));

    // No negative above is an error to the operator.
    assert_eq!(count(&db, "SELECT COUNT(*) FROM errors"), 0);
    listener.stop();
}

#[test]
fn every_request_needs_exactly_the_operator_token() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    let listener = Listener::orchestrator(home);
    let addr = listener.addr;
    let db = db_path(home);
    // A live managed-run capability in this very database.
    quorum_core::db::open(&db)
        .unwrap()
        .execute_batch(
            "INSERT INTO tasks (id,title,status,assignee,created_by,created_at,updated_at)
             VALUES (200,'fixture','working','Worker','test',1000,1000);
             INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('worker-capability-0123456789abcdef0123',200,'Worker','worker',1001);",
        )
        .unwrap();
    let list = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    let create = json!({
        "jsonrpc":"2.0","id":1,"method":"tools/call",
        "params":{"name":"task_create","arguments":{"title":"t"}}
    });

    let same_length_wrong = format!("Bearer {}X", &TOKEN[..TOKEN.len() - 1]);
    let prefix = format!("Bearer {}", &TOKEN[..TOKEN.len() - 1]);
    let extended = format!("Bearer {TOKEN}x");
    let doubled = format!("Bearer {TOKEN}{TOKEN}");
    let good = format!("Bearer {TOKEN}");
    let cases: Vec<Vec<&str>> = vec![
        vec![],
        vec![TOKEN],
        vec!["Bearer"],
        vec!["Bearer "],
        vec!["Basic b3JjaGVzdHJhdG9y"],
        vec![&same_length_wrong],
        vec![&prefix],
        vec![&extended],
        vec![&doubled],
        // A run capability is not a credential here, live or not.
        vec!["Bearer worker-capability-0123456789abcdef0123"],
        // Two headers are refused even when one is right.
        vec![&good, &good],
        vec![&good, "Bearer worker-capability-0123456789abcdef0123"],
    ];
    for authorization in cases {
        for body in [&list, &create] {
            let (status, text) = post_mcp(addr, &authorization, body);
            assert_eq!(status, 401, "{authorization:?}: {text}");
            assert_eq!(text, "Unauthorized", "{authorization:?}");
        }
    }
    // Authentication is decided first: without the token, a foreign Host or
    // a browser Origin learns nothing more than 401.
    let listed = list.to_string();
    let mut with_origin = MCP_HEADERS.to_vec();
    with_origin.push(("Origin", "http://localhost:3000"));
    for (host, headers) in [
        ("evil.example", &MCP_HEADERS[..]),
        (&addr.to_string()[..], &with_origin[..]),
        ("evil.example", &with_origin[..]),
    ] {
        let (status, text) = http_as(addr, host, "POST", "/mcp", headers, listed.as_bytes());
        assert_eq!((status, text.as_str()), (401, "Unauthorized"), "{host}");
    }
    // The gate covers every method and path.
    let host = addr.to_string();
    assert_eq!(http_as(addr, &host, "GET", "/mcp", &[], b"").0, 401);
    assert_eq!(
        http_as(addr, &host, "POST", "/", &MCP_HEADERS, b"{}").0,
        401
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM tasks"),
        1,
        "nothing was created"
    );

    // The token works, case-insensitive scheme included, and is never echoed.
    let (status, text) = post_mcp(addr, &[&format!("bearer {TOKEN}")], &list);
    assert_eq!(status, 200, "{text}");
    assert!(!text.contains(TOKEN));

    // An authenticated request from a browser origin is refused; so is a
    // host name the listener was not told to answer to.
    let mut with_origin = MCP_HEADERS.to_vec();
    with_origin.push(("Authorization", &good));
    with_origin.push(("Origin", "http://localhost:3000"));
    let (status, _) = http_as(
        addr,
        &host,
        "POST",
        "/mcp",
        &with_origin,
        list.to_string().as_bytes(),
    );
    assert_eq!(status, 403);
    let mut authorized = MCP_HEADERS.to_vec();
    authorized.push(("Authorization", &good));
    let (status, _) = http_as(
        addr,
        "evil.example",
        "POST",
        "/mcp",
        &authorized,
        list.to_string().as_bytes(),
    );
    assert_eq!(status, 403);
    // Only `/mcp` is served, and only by POST.
    assert_eq!(http_as(addr, &host, "POST", "/", &authorized, b"{}").0, 404);
    assert_eq!(http_as(addr, &host, "GET", "/mcp", &authorized, b"").0, 405);
    listener.stop();
}

#[test]
fn startup_refuses_unsafe_or_incomplete_configuration() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    init(home);
    let token_file = write_token(home, TOKEN);
    let token_file = token_file.to_str().unwrap();
    let private = |name: &str, contents: &str, mode: u32| {
        let path = home.join(name);
        std::fs::write(&path, contents).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path.to_str().unwrap().to_string()
    };
    let short = private("short.token", "too-short", 0o600);
    let spaced = private(
        "spaced.token",
        &format!("{} {}", &TOKEN[..20], &TOKEN[20..]),
        0o600,
    );
    let group_readable = private("group.token", TOKEN, 0o640);
    let world_readable = private("world.token", TOKEN, 0o604);
    let absent = home.join("absent.token");
    let absent = absent.to_str().unwrap();
    let directory = home.to_str().unwrap();

    let refused = |listen: &str, args: &[&str], env: &[(&str, &str)]| {
        let mut command = orchestrator_command(home, listen);
        command.args(args);
        for (name, value) in env {
            command.env(name, value);
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(2),
            "{listen} {args:?} {env:?}: {output:?}"
        );
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(!stderr.contains(TOKEN), "{stderr}");
        stderr
    };
    let id = ["--identity", "Coordinator"];

    // No token: refused on loopback and on every other address.
    for listen in ["127.0.0.1:0", "0.0.0.0:0", "[::]:0", "192.0.2.1:8811"] {
        refused(listen, &id, &[]);
        refused(
            listen,
            &["--identity", "Coordinator", "--allowed-host", "quorum.test"],
            &[],
        );
    }
    // The token file must be a well-formed secret only its owner can read.
    for file in [&short[..], &spaced, absent, directory] {
        refused(
            "127.0.0.1:0",
            &["--identity", "Coordinator", "--token-file", file],
            &[],
        );
    }
    for file in [&group_readable, &world_readable] {
        let stderr = refused(
            "127.0.0.1:0",
            &["--identity", "Coordinator", "--token-file", file],
            &[],
        );
        assert!(stderr.contains("group or others"), "{stderr}");
    }
    // The token is never an argument, and the environment is not a source:
    // an exported token is refused even beside a valid file.
    refused(
        "127.0.0.1:0",
        &["--identity", "Coordinator", "--token", TOKEN],
        &[],
    );
    refused("127.0.0.1:0", &id, &[("QUORUM_ORCHESTRATOR_TOKEN", TOKEN)]);
    let stderr = refused(
        "127.0.0.1:0",
        &["--identity", "Coordinator", "--token-file", token_file],
        &[("QUORUM_ORCHESTRATOR_TOKEN", TOKEN)],
    );
    assert!(stderr.contains("is not read"), "{stderr}");
    // One coordinator identity is required and must be a plain name.
    refused("127.0.0.1:0", &["--token-file", token_file], &[]);
    for identity in ["", "  ", "two\nlines"] {
        refused(
            "127.0.0.1:0",
            &["--token-file", token_file, "--identity", identity],
            &[],
        );
    }
    // A token alone does not open a non-loopback bind: its hosts are named.
    refused(
        "0.0.0.0:0",
        &["--identity", "Coordinator", "--token-file", token_file],
        &[],
    );
    refused(
        "0.0.0.0:0",
        &[
            "--identity",
            "Coordinator",
            "--token-file",
            token_file,
            "--allowed-host",
            "bad host",
        ],
        &[],
    );
    // A managed run cannot start a coordinator listener.
    refused(
        "127.0.0.1:0",
        &["--identity", "Coordinator", "--token-file", token_file],
        &[("QUORUM_RUN_ID", "worker-cap")],
    );
    refused(
        "localhost",
        &["--identity", "Coordinator", "--token-file", token_file],
        &[],
    );
    // A repository without a database is refused, and none is created.
    for repo in ["other/repo", "not-a-repo"] {
        refused(
            "127.0.0.1:0",
            &[
                "--identity",
                "Coordinator",
                "--token-file",
                token_file,
                "--repo",
                repo,
            ],
            &[],
        );
    }
    assert!(!home.join("repos/other__repo").exists());

    // With a token and a named host, a non-loopback bind serves that host only.
    let mut command = orchestrator_command(home, "0.0.0.0:0");
    command.args([
        "--identity",
        "Coordinator",
        "--token-file",
        token_file,
        "--allowed-host",
        "quorum.test",
    ]);
    let listener = Listener::spawn(command, "orchestrator-mcp");
    let addr = listener.addr;
    let list = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}).to_string();
    let good = format!("Bearer {TOKEN}");
    let mut authorized = MCP_HEADERS.to_vec();
    authorized.push(("Authorization", &good));
    for (host, expected) in [
        ("quorum.test", 200),
        (&format!("quorum.test:{}", addr.port())[..], 200),
        (&addr.to_string()[..], 403),
        ("localhost", 403),
    ] {
        let (status, _) = http_as(addr, host, "POST", "/mcp", &authorized, list.as_bytes());
        assert_eq!(status, expected, "Host: {host}");
    }
    let (status, _) = http_as(
        addr,
        "quorum.test",
        "POST",
        "/mcp",
        &MCP_HEADERS,
        list.as_bytes(),
    );
    assert_eq!(status, 401);
    listener.stop();
}

#[test]
fn free_text_is_bounded_and_text_safe() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    let listener = Listener::orchestrator(home);
    let addr = listener.addr;
    let db = db_path(home);
    let create = |extra: Value| {
        let mut arguments = json!({"title": "t"});
        for (key, value) in extra.as_object().unwrap() {
            arguments[key] = value.clone();
        }
        arguments
    };

    let invalid = [
        create(json!({"title": "t".repeat(513)})),
        create(json!({"title": ""})),
        create(json!({"title": "line\nbreak"})),
        create(json!({"title": "nul\u{0}"})),
        // The identity is the listener's, never an argument.
        create(json!({"created_by": "Someone"})),
        create(json!({"body": "b".repeat(64 * 1024 + 1)})),
        create(json!({"body": "has\u{0}nul"})),
        create(json!({"body": "   \n"})),
        create(json!({"labels": ["l".repeat(65)]})),
        create(json!({"labels": ["complexity:1"]})),
        create(json!({"labels": (0..17).map(|n| n.to_string()).collect::<Vec<_>>()})),
        create(json!({"refs": {"pr": 5}})),
        create(json!({"refs": {"k": "v".repeat(5000)}})),
        create(json!({"refs": {"k": "nul\u{0}"}})),
        create(json!({"depends_on": [0]})),
        create(json!({"review_pr": 1, "continue_pr": 2})),
        create(json!({"review_only": 0})),
        create(json!({"base_branch": "b".repeat(256)})),
        create(json!({"unknown": true})),
        create(json!({"title": 5})),
        json!({"body": "no title"}),
    ];
    for arguments in invalid {
        let (code, message) = refused(addr, "task_create", arguments.clone());
        assert_eq!(code, "invalid_arguments", "{arguments}: {message}");
    }
    for (tool, arguments) in [
        (
            "message_agent",
            json!({"to": "b", "body": "m".repeat(32 * 1024 + 1)}),
        ),
        ("message_agent", json!({"to": "b", "body": "x\u{0}"})),
        ("message_agent", json!({"to": "b\u{0}", "body": "x"})),
        ("task_cancel", json!({"task_id": 1, "note": "n\u{0}"})),
        (
            "task_cancel",
            json!({"task_id": 1, "note": "n".repeat(32 * 1024 + 1)}),
        ),
        ("task_cancel", json!({"task_id": -1})),
        ("task_cancel", json!({"task_id": 1, "agent": "Someone"})),
        (
            "message_agent",
            json!({"from": "Someone", "to": "b", "body": "x"}),
        ),
        ("task_list", json!({"offset": -1})),
        ("task_list", json!({"limit": 0})),
        ("task_list", json!({"limit": 201})),
        ("task_list", json!({"status": "x\u{0}"})),
        ("task_log", json!({"task_id": 1, "since": -1})),
        ("task_get", json!({"task_id": 0})),
        ("task_get", json!({})),
        ("task_retry", json!({"task_id": 1, "by": "Someone"})),
    ] {
        let (code, message) = refused(addr, tool, arguments.clone());
        assert_eq!(code, "invalid_arguments", "{tool} {arguments}: {message}");
    }

    // Bytes that are not UTF-8 never become a request, let alone a row.
    let mut raw = br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"task_create","arguments":{"title":""#.to_vec();
    raw.extend_from_slice(&[0xff, 0xfe, 0xc3, 0x28]);
    raw.extend_from_slice(br#""}}}"#);
    let good = format!("Bearer {TOKEN}");
    let mut authorized = MCP_HEADERS.to_vec();
    authorized.push(("Authorization", &good));
    let (status, text) = http_as(addr, &addr.to_string(), "POST", "/mcp", &authorized, &raw);
    assert_eq!(status, 415, "{text}");
    assert!(
        text.starts_with("fail to deserialize request body"),
        "{text}"
    );

    // The HTTP body cap is the agent adapter's: 256 KiB.
    let oversized = json!({
        "jsonrpc":"2.0","id":1,"method":"tools/call",
        "params":{"name":"task_create","arguments":create(json!({"body":"b".repeat(257 * 1024)}))}
    });
    assert_eq!(post_mcp(addr, &[&good], &oversized).0, 413);

    assert_eq!(count(&db, "SELECT COUNT(*) FROM tasks"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM mailbox"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM errors"), 0);

    // Text at the limits is stored byte for byte.
    let body = "é".repeat(32 * 1024);
    assert_eq!(body.len(), 64 * 1024);
    let title = "t".repeat(512);
    let created = ok(
        addr,
        "task_create",
        create(json!({"title": title, "body": body})),
    );
    let stored: (String, String) = quorum_core::db::open(&db)
        .unwrap()
        .query_row(
            "SELECT title,body FROM tasks WHERE id=?1",
            [created["id"].as_i64().unwrap()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored, (title, body));
    listener.stop();
}

#[test]
fn concurrent_creates_and_cancels_are_decided_by_storage() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    let listener = Listener::orchestrator(home);
    let addr = listener.addr;
    let db = db_path(home);

    for round in 0..5 {
        // More callers than the listener serves at once.
        let creators: Vec<_> = (0..12)
            .map(|n| {
                thread::spawn(move || {
                    ok(addr, "task_create", json!({"title": format!("task {n}")}))["id"]
                        .as_i64()
                        .unwrap()
                })
            })
            .collect();
        let mut ids: Vec<i64> = creators
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 12, "round {round}: every create got its own id");

        let target = ids[0];
        let cancellers: Vec<_> = (0..12)
            .map(|_| {
                thread::spawn(move || {
                    call_as(addr, TOKEN, "task_cancel", json!({"task_id": target}))
                })
            })
            .collect();
        let outcomes: Vec<(bool, Value)> = cancellers
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        let won = outcomes.iter().filter(|(is_error, _)| !is_error).count();
        assert_eq!(won, 1, "round {round}: {outcomes:?}");
        for (is_error, value) in &outcomes {
            if *is_error {
                assert_eq!(value["code"], "rejected", "round {round}: {value}");
            }
        }
        assert_eq!(status_of(&db, target), "cancelled");
        assert_eq!(
            count(
                &db,
                &format!(
                    "SELECT COUNT(*) FROM events WHERE kind='task_cancelled' AND subject='task#{target}'"
                )
            ),
            1,
            "round {round}"
        );
    }
    assert_eq!(count(&db, "SELECT COUNT(*) FROM tasks"), 60);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM errors"), 0);
    listener.stop();
}

/// The text of one tool result exactly as the listener sent it.
fn result_text(addr: SocketAddr, name: &str, arguments: Value) -> String {
    let response = rpc(
        addr,
        TOKEN,
        "tools/call",
        json!({"name": name, "arguments": arguments}),
    );
    assert_ne!(response["result"]["isError"], true, "{response:#}");
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_string()
}

const RESULT_CAP: usize = 128 * 1024;

#[test]
fn results_are_bounded_and_paged_through_the_listener() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    let listener = Listener::orchestrator(home);
    let addr = listener.addr;
    let db = db_path(home);

    // task_list: summary rows only, and every row reachable by offset.
    let mut created = Vec::new();
    for n in 0..5 {
        created.push(
            ok(
                addr,
                "task_create",
                json!({"title": format!("task {n}"), "body": "b".repeat(60_000)}),
            )["id"]
                .as_i64()
                .unwrap(),
        );
    }
    let mut seen = Vec::new();
    let mut offset = 0;
    loop {
        let text = result_text(addr, "task_list", json!({"limit": 2, "offset": offset}));
        assert!(text.len() < 4096, "a page of summaries carries no body");
        let page: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(page["total"], 5);
        assert_eq!(page["offset"], offset);
        for task in page["tasks"].as_array().unwrap() {
            assert!(task.get("body").is_none());
            seen.push(task["id"].as_i64().unwrap());
        }
        match page["next_offset"].as_i64() {
            Some(next) => {
                assert_eq!(page["truncated"], true);
                offset = next;
            }
            None => {
                assert_eq!(page["truncated"], false);
                break;
            }
        }
    }
    assert_eq!(seen, created);
    let past = ok(addr, "task_list", json!({"offset": 50}));
    assert_eq!(past["tasks"], json!([]));
    assert_eq!(past["truncated"], false);

    // task_get: 80 notes of 8000 bytes cannot fit; the newest survive and the
    // result says how many were left out.
    let id = created[0];
    {
        let conn = quorum_core::db::open(&db).unwrap();
        for n in 0..80 {
            conn.execute(
                "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, 'Writer', ?3)",
                params![id, 2000 + n, format!("{n:04}{}", "n".repeat(7996))],
            )
            .unwrap();
        }
    }
    let text = result_text(addr, "task_get", json!({"task_id": id}));
    assert!(text.len() <= RESULT_CAP, "{}", text.len());
    let view: Value = serde_json::from_str(&text).unwrap();
    let notes = view["notes"].as_array().unwrap();
    assert!(!notes.is_empty() && notes.len() < 80, "{}", notes.len());
    assert!(notes.last().unwrap()["body"]
        .as_str()
        .unwrap()
        .starts_with("0079"));
    assert_eq!(view["notes_omitted"], 80 - notes.len());
    assert_eq!(view["body"].as_str().unwrap().len(), 60_000);
    assert!(view.get("body_truncated").is_none());

    // A body too large for one result on its own is shortened and flagged.
    quorum_core::db::open(&db)
        .unwrap()
        .execute(
            "UPDATE tasks SET body=?2 WHERE id=?1",
            params![created[1], "é".repeat(150_000)],
        )
        .unwrap();
    let text = result_text(addr, "task_get", json!({"task_id": created[1]}));
    assert!(text.len() <= RESULT_CAP, "{}", text.len());
    let view: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(view["body_truncated"], true);
    assert!(view["body"].as_str().unwrap().chars().all(|c| c == 'é'));

    // task_log: 250 events of 2000 bytes are read in bounded pages, in order,
    // by following `next_since`.
    {
        let conn = quorum_core::db::open(&db).unwrap();
        let expires = quorum_core::clock::now() + 86_400;
        for n in 0..250 {
            conn.execute(
                "INSERT INTO events(ts, kind, subject, body, expires_at)
                 VALUES (?1, 'test_event', ?2, ?3, ?4)",
                params![3000 + n, format!("task#{id}"), "e".repeat(2000), expires],
            )
            .unwrap();
        }
    }
    let mut since = 0;
    let mut sequence = Vec::new();
    let mut pages = 0;
    loop {
        let text = result_text(addr, "task_log", json!({"task_id": id, "since": since}));
        assert!(text.len() <= RESULT_CAP, "{}", text.len());
        let page: Value = serde_json::from_str(&text).unwrap();
        let events = page["events"].as_array().unwrap();
        assert!(events.len() <= 100);
        sequence.extend(events.iter().map(|event| event["seq"].as_i64().unwrap()));
        pages += 1;
        if page["truncated"] == true {
            since = page["next_since"].as_i64().unwrap();
            assert_eq!(Some(&since), sequence.last());
        } else {
            assert!(page.get("next_since").is_none());
            break;
        }
        assert!(pages < 20, "paging did not advance");
    }
    assert!(pages > 3, "200 KiB per hundred events cannot fit one page");
    assert_eq!(sequence.len(), 251, "task_created plus the 250 inserted");
    assert!(sequence.windows(2).all(|pair| pair[0] < pair[1]));
    listener.stop();
}

#[test]
fn an_internal_failure_is_logged_for_the_operator_and_hidden_from_the_caller() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    let listener = Listener::orchestrator(home);
    let addr = listener.addr;
    let db = db_path(home);
    // Task intake reads the operator's serve configuration; break it.
    let config = home.join("serve/test__repo.toml");
    std::fs::create_dir_all(config.parent().unwrap()).unwrap();
    std::fs::write(
        &config,
        "merge_token_file = \"/secret/path\"\nbase_branch = [broken\n",
    )
    .unwrap();

    for arguments in [
        json!({"title": "t"}),
        json!({"title": "t", "base_branch": "main"}),
    ] {
        let (code, message) = refused(addr, "task_create", arguments);
        assert_eq!(code, "internal");
        for leaked in ["serve", "toml", "secret", home.to_str().unwrap()] {
            assert!(!message.contains(leaked), "{message}");
        }
    }
    assert_eq!(count(&db, "SELECT COUNT(*) FROM tasks"), 0);
    let logged: Vec<(String, String)> = {
        let conn = quorum_core::db::open(&db).unwrap();
        let mut statement = conn.prepare("SELECT source, detail FROM errors").unwrap();
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows
    };
    assert_eq!(logged.len(), 2, "{logged:?}");
    for (source, detail) in &logged {
        assert_eq!(source, "orchestrator-mcp");
        assert!(detail.starts_with("task_create: "), "{detail}");
        assert!(detail.contains("bad serve config"), "{detail}");
    }
    // Reads do not depend on that file and still work.
    assert_eq!(ok(addr, "task_list", json!({}))["total"], 0);
    listener.stop();
}

/// One connection that has sent `payload` and nothing more.
fn stalled(addr: SocketAddr, payload: &[u8]) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write_all(payload).unwrap();
    stream
}

#[test]
fn peers_without_the_token_cannot_hold_the_listener() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path();
    let listener = Listener::orchestrator(home);
    let addr = listener.addr;
    let list = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    let good = format!("Bearer {TOKEN}");

    // A refused request is answered before its body is read and its
    // connection is closed: it never waits on, or holds, a request slot.
    let refused_head = format!(
        "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer wrong-token-wrong-token-wrong-token\r\n\
         Content-Type: application/json\r\nContent-Length: 200000\r\n\r\n"
    );
    for _ in 0..40 {
        let mut stream = stalled(addr, refused_head.as_bytes());
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .expect("a refused connection must be closed, not held open");
        let raw = String::from_utf8_lossy(&raw).to_ascii_lowercase();
        assert!(raw.starts_with("http/1.1 401"), "{raw}");
        assert!(raw.contains("connection: close"), "{raw}");
    }

    // Peers that never finish a request head fill every connection slot (32)
    // and more. They are cut off at the 2 second head timeout, so a caller
    // with the token is served shortly after, not when they choose to leave.
    let partial = format!("POST /mcp HTTP/1.1\r\nHost: {addr}\r\n");
    let squatters: Vec<TcpStream> = (0..48)
        .map(|n| stalled(addr, if n % 2 == 0 { b"" } else { partial.as_bytes() }))
        .collect();
    thread::sleep(Duration::from_millis(200));
    let started = Instant::now();
    let (status, body) = post_mcp(addr, &[&good], &list);
    assert_eq!(status, 200, "{body}");
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "served after {:?}",
        started.elapsed()
    );
    // Every squatter has been disconnected by the listener.
    for mut squatter in squatters {
        squatter
            .set_read_timeout(Some(Duration::from_secs(8)))
            .unwrap();
        let mut raw = Vec::new();
        squatter
            .read_to_end(&mut raw)
            .expect("an unfinished request head must be cut off");
    }
    assert_eq!(post_mcp(addr, &[&good], &list).0, 200);
    listener.stop();
}

// --- separation from the managed-agent MCP, against a real daemon ---

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
        vec!["config", "user.name", "Orchestrator MCP Test"],
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

fn write_serve_config(path: &Path) {
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
            .env("QUORUM_REPO", REPO)
            .args(["serve", "--config"])
            .arg(config)
            .args(["--repo", REPO, "--cap", "1", "--repo-dir"])
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
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
            let deadline = Instant::now() + Duration::from_secs(10);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(20));
            }
            if self.child.try_wait().ok().flatten().is_none() {
                unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            }
            let _ = self.child.wait();
        }
    }
}

/// One live worker run (task 200) the daemon endpoint will authorize.
fn install_worker_run(db: &Path) {
    let mut conn = quorum_core::db::open(db).unwrap();
    let tx = quorum_core::db::begin_immediate(&mut conn).unwrap();
    tx.execute(
        "INSERT INTO tasks (id,title,status,assignee,created_by,created_at,updated_at)
         VALUES (200,'fixture','working','Worker','test',1000,1000)",
        [],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
         VALUES ('worker-cap',200,'Worker','worker',1001)",
        [],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
         VALUES (200,'Worker','worker','test','high','test',1001)",
        [],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO claims(target,holder,ts,expires_at,active)
         VALUES ('task#200','Worker',1000,?1,1)",
        params![quorum_core::clock::now() + 86_400],
    )
    .unwrap();
    tx.commit().unwrap();
}

#[test]
fn agent_and_orchestrator_tools_never_cross_listeners() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let repo = root.path().join("repo");
    let worktrees = root.path().join("worktrees");
    for dir in [&home, &repo, &worktrees] {
        std::fs::create_dir_all(dir).unwrap();
    }
    init_git_repo(&repo);
    let names = root.path().join("names.txt");
    std::fs::write(&names, "AgentOne\nAgentTwo\n").unwrap();
    let config = root.path().join("serve.toml");
    write_serve_config(&config);
    let daemon = Daemon::start(&home, &repo, &worktrees, &names, &config);
    daemon.wait_for("serving (cap=1)");
    let db = db_path(&home);
    install_worker_run(&db);

    let mut agent_command = Command::new(quorum_bin());
    agent_command
        .args(["agent-mcp", "--listen", "127.0.0.1:0"])
        .env("QUORUM_AGENT_ENDPOINT", endpoint_path(&db))
        .env("QUORUM_REPO", REPO)
        .env_remove("QUORUM_RUN_ID")
        .env_remove("QUORUM_AGENT")
        .env_remove("QUORUM_HOME")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let agent = Listener::spawn(agent_command, "agent-mcp");
    let orchestrator = Listener::orchestrator(&home);
    let list = json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}});
    let tasks_before = count(&db, "SELECT COUNT(*) FROM tasks");
    let mailbox_before = count(&db, "SELECT COUNT(*) FROM mailbox");

    // The two inventories are disjoint.
    let agent_tools = rpc(agent.addr, "worker-cap", "tools/list", json!({}));
    let agent_tools = tool_names(&agent_tools);
    assert!(agent_tools.contains(&"submit_work"), "{agent_tools:?}");
    let orchestrator_tools = rpc(orchestrator.addr, TOKEN, "tools/list", json!({}));
    let orchestrator_tools = tool_names(&orchestrator_tools);
    assert_eq!(orchestrator_tools, TOOLS);
    assert!(agent_tools
        .iter()
        .all(|name| !orchestrator_tools.contains(name)));

    // A live run capability does not authenticate to the orchestrator.
    let (status, _) = post_mcp(orchestrator.addr, &["Bearer worker-cap"], &list);
    assert_eq!(status, 401);
    // The operator token is not a run: the agent listener's daemon refuses it.
    let as_run = rpc(agent.addr, TOKEN, "tools/list", json!({}));
    assert_eq!(
        as_run["error"]["message"], "run authority rejected",
        "{as_run:#}"
    );

    // Orchestrator tools do not exist on the agent listener, for a live run.
    for (name, arguments) in [
        ("task_create", json!({"title": "escalate"})),
        ("task_cancel", json!({"task_id": 200})),
        ("task_list", json!({})),
        ("message_agent", json!({"to": "AgentOne", "body": "x"})),
    ] {
        let response = rpc(
            agent.addr,
            "worker-cap",
            "tools/call",
            json!({"name": name, "arguments": arguments}),
        );
        assert_eq!(response["error"]["code"], -32601, "{name}: {response:#}");
    }
    // Agent tools do not exist on the orchestrator listener.
    for name in [
        "submit_work",
        "submit_review",
        "submit_plan",
        "react",
        "task_note",
        "alert_owner",
        "pull_request_read",
    ] {
        let response = rpc(
            orchestrator.addr,
            TOKEN,
            "tools/call",
            json!({"name": name, "arguments": {}}),
        );
        assert_eq!(response["error"]["code"], -32601, "{name}: {response:#}");
        assert_eq!(response["error"]["message"], "unknown tool", "{name}");
    }
    assert_eq!(count(&db, "SELECT COUNT(*) FROM tasks"), tasks_before);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM mailbox"), mailbox_before);
    assert_eq!(status_of(&db, 200), "working");

    // With the daemon running, an orchestrator creates a task held behind an
    // unfinished dependency, inspects it, and cancels it. The cancellation is
    // the same row state the daemon observes, and the daemon leaves it be.
    let id = ok(
        orchestrator.addr,
        "task_create",
        json!({"title": "held", "depends_on": [200]}),
    )["id"]
        .as_i64()
        .unwrap();
    let listed = ok(orchestrator.addr, "task_list", json!({}));
    assert!(listed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|task| task["id"] == id));
    let cancelled = ok(orchestrator.addr, "task_cancel", json!({"task_id": id}));
    assert_eq!(cancelled["status"], "cancelled");
    thread::sleep(Duration::from_millis(2500));
    assert_eq!(status_of(&db, id), "cancelled");
    assert_eq!(
        ok(orchestrator.addr, "task_get", json!({"task_id": id}))["status"],
        "cancelled"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM errors WHERE source='orchestrator-mcp'"
        ),
        0
    );
    orchestrator.stop();
    agent.stop();
}
