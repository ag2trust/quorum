#![cfg(unix)]

//! Real-process protocol coverage for the credentialless tools-only MCP shell.

use rusqlite::params;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
    std::fs::write(
        path,
        r#"
[model_profiles.test]
runner = "claude"
model = "claude-opus-4-6"
effort = "high"
[routing.classifier]
test = 100
[routing.planner]
test = 100
[routing.collector]
test = 100
[routing.worker.1]
test = 100
[routing.worker.2]
test = 100
[routing.worker.3]
test = 100
[routing.worker.4]
test = 100
[routing.worker.5]
test = 100
[routing.reviewer.1]
test = 100
[routing.reviewer.2]
test = 100
[routing.reviewer.3]
test = 100
[routing.reviewer.4]
test = 100
[routing.reviewer.5]
test = 100
"#,
    )
    .unwrap();
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
        (203, "working", Some("Ended"), None, None),
    ] {
        tx.execute(
            "INSERT INTO tasks (id,title,status,assignee,reviewer,created_by,created_at,updated_at,refs)
             VALUES (?1,'MCP fixture',?2,?3,?4,'test',1000,1000,?5)",
            params![id, status, assignee, reviewer, refs],
        )
        .unwrap();
    }
    for (capability, task_id, agent, role, revoked_at) in [
        ("worker-cap", 200, "Worker", "worker", None),
        ("reviewer-cap", 201, "Reviewer", "reviewer", None),
        ("revoked-cap", 202, "Revoked", "worker", Some(1002)),
        ("ended-cap", 203, "Ended", "worker", None),
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
    }
    tx.execute(
        "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at,ended_at,end_reason)
         VALUES (203,'Ended','worker','test','high','test',1001,1002,'completed')",
        [],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO agent_runs
         (task_id,agent_name,role,model,effort,provider,spawned_at,review_cap_run_id,review_pr,review_head_sha)
         VALUES (201,'Reviewer','reviewer','test','high','test',1001,'reviewer-cap',77,?1)",
        ["a".repeat(40)],
    )
    .unwrap();
    tx.commit().unwrap();
}

struct McpProcess {
    child: Child,
    input: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    stdout_reader: Option<thread::JoinHandle<Vec<String>>>,
    stderr_reader: Option<thread::JoinHandle<Vec<u8>>>,
}

impl McpProcess {
    fn start(endpoint: &Path, capability: &str, agent: &str) -> Self {
        let mut child = Command::new(quorum_bin())
            .arg("agent-mcp")
            .env("QUORUM_AGENT_ENDPOINT", endpoint)
            .env("QUORUM_RUN_ID", capability)
            .env("QUORUM_REPO", "test/repo")
            .env("QUORUM_AGENT", agent)
            .env("GH_TOKEN", "must-not-leak")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (sender, lines) = mpsc::channel();
        let stdout_reader = thread::spawn(move || {
            let mut all = Vec::new();
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                all.push(line.clone());
                if sender.send(line).is_err() {
                    break;
                }
            }
            all
        });
        let stderr_reader = thread::spawn(move || {
            let mut bytes = Vec::new();
            BufReader::new(stderr).read_to_end(&mut bytes).unwrap();
            bytes
        });
        Self {
            child,
            input: Some(input),
            lines,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
        }
    }

    fn send(&mut self, value: Value) {
        let input = self.input.as_mut().unwrap();
        serde_json::to_writer(&mut *input, &value).unwrap();
        input.write_all(b"\n").unwrap();
        input.flush().unwrap();
    }

    fn request(&mut self, id: i64, method: &str, params: Value) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|error| panic!("no MCP response for {method}: {error}"));
            let value: Value = serde_json::from_str(&line)
                .unwrap_or_else(|error| panic!("stdout was not MCP JSON: {error}: {line:?}"));
            if value["id"] == id {
                return value;
            }
        }
    }

    fn wait_for_notification(&self, method: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|error| panic!("no MCP notification for {method}: {error}"));
            let value: Value = serde_json::from_str(&line)
                .unwrap_or_else(|error| panic!("stdout was not MCP JSON: {error}: {line:?}"));
            if value["method"] == method {
                return value;
            }
        }
    }

    fn initialize(&mut self) -> Value {
        let response = self.request(
            1,
            "initialize",
            json!({
                "protocolVersion":"2025-06-18",
                "capabilities":{},
                "clientInfo":{"name":"agent-mcp-test","version":"1"}
            }),
        );
        self.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        response
    }

    fn finish(mut self) -> Vec<u8> {
        drop(self.input.take());
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
        }
        let status = self.child.wait().unwrap();
        assert!(status.success(), "agent-mcp exited with {status}");
        let lines = self.stdout_reader.take().unwrap().join().unwrap();
        for line in lines {
            serde_json::from_str::<Value>(&line)
                .unwrap_or_else(|error| panic!("stdout was not MCP JSON: {error}: {line:?}"));
        }
        self.stderr_reader.take().unwrap().join().unwrap()
    }
}

impl Drop for McpProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn tool_names(response: &Value) -> Vec<&str> {
    response["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect()
}

fn tool<'a>(response: &'a Value, name: &str) -> &'a Value {
    response["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == name)
        .unwrap_or_else(|| panic!("missing advertised tool {name}: {response:#}"))
}

fn assert_tool_error(response: &Value, message: &str) {
    assert_eq!(response["result"]["isError"], true, "{response:#}");
    assert_eq!(
        response["result"]["content"][0]["text"], message,
        "{response:#}"
    );
}

#[test]
fn stdio_shell_is_tools_only_live_scoped_and_performs_no_github_work() {
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
    let daemon = Daemon::start(&home, &repo, &worktrees, &names, &config);
    daemon.wait_for("serving (cap=1)");
    let db = home.join("repos/test__repo/quorum.db");
    let endpoint = endpoint_path(&db);
    install_fixtures(&db);

    let mut worker = McpProcess::start(&endpoint, "worker-cap", "Worker");
    let initialized = worker.initialize();
    assert_eq!(initialized["result"]["serverInfo"]["name"], "github");
    let capabilities = initialized["result"]["capabilities"].as_object().unwrap();
    assert_eq!(capabilities.keys().collect::<Vec<_>>(), vec!["tools"]);
    assert_eq!(capabilities["tools"]["listChanged"], true);

    let initial = worker.request(2, "tools/list", json!({}));
    assert_eq!(tool_names(&initial), vec!["delivery_report_write"]);
    let schema = &initial["result"]["tools"][0]["inputSchema"];
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(
        schema["required"],
        json!(["summary", "changes", "verification"])
    );

    let valid_shell = worker.request(
        3,
        "tools/call",
        json!({
            "name":"delivery_report_write",
            "arguments":{"summary":"done","changes":[],"verification":[]}
        }),
    );
    assert_tool_error(
        &valid_shell,
        "tool execution is not available in this delivery",
    );

    let forbidden = worker.request(
        4,
        "tools/call",
        json!({"name":"pull_request_review_write","arguments":{}}),
    );
    assert_tool_error(&forbidden, "tool is forbidden for the current run phase");

    let unknown = worker.request(
        5,
        "tools/call",
        json!({"name":"raw_http","arguments":{"secret":"must-not-leak"}}),
    );
    assert_eq!(unknown["error"]["code"], -32601);
    assert_eq!(unknown["error"]["message"], "unknown tool");

    // `submit_plan` is advertised to a planner only, so to any other run the
    // name is outside its inventory and never reaches the endpoint.
    let not_a_planner = worker.request(
        9,
        "tools/call",
        json!({"name":"submit_plan","arguments":{"response":{"outcome":"blocker"}}}),
    );
    assert_eq!(not_a_planner["error"]["code"], -32601);
    assert_eq!(not_a_planner["error"]["message"], "unknown tool");

    let resources = worker.request(6, "resources/list", json!({}));
    assert_eq!(resources["error"]["code"], -32601);
    let prompts = worker.request(7, "prompts/list", json!({}));
    assert_eq!(prompts["error"]["code"], -32601);

    quorum_core::db::open(&db)
        .unwrap()
        .execute(
            r#"UPDATE tasks SET refs='{"pr":42}', updated_at=1002 WHERE id=200"#,
            [],
        )
        .unwrap();
    let notification = worker.wait_for_notification("notifications/tools/list_changed");
    assert!(notification.get("id").is_none());
    let rework = worker.request(8, "tools/list", json!({}));
    assert_eq!(
        tool_names(&rework),
        vec![
            "pull_request_read",
            "add_issue_comment",
            "add_reply_to_pull_request_comment",
            "github_operation_read",
            "delivery_report_write"
        ]
    );

    let mut reviewer = McpProcess::start(&endpoint, "reviewer-cap", "Reviewer");
    reviewer.initialize();
    let reviewer_tools = reviewer.request(2, "tools/list", json!({}));
    assert_eq!(
        tool_names(&reviewer_tools),
        vec![
            "pull_request_read",
            "add_issue_comment",
            "pull_request_review_write",
            "add_comment_to_pending_review",
            "add_reply_to_pull_request_comment",
            "resolve_review_thread",
            "github_operation_read"
        ]
    );
    let pull_request_read = &tool(&reviewer_tools, "pull_request_read")["inputSchema"];
    assert_eq!(
        pull_request_read["properties"]["after"],
        json!({"type":"string","minLength":1,"maxLength":512})
    );
    assert_eq!(
        pull_request_read["properties"]["page"],
        json!({"type":"integer","minimum":1,"maximum":100})
    );
    let pending_review = &tool(&reviewer_tools, "pull_request_review_write")["inputSchema"];
    assert_eq!(
        pending_review["oneOf"],
        json!([
            {
                "properties":{"method":{"const":"create"}},
                "required":["method","commitID"]
            },
            {
                "properties":{"method":{"const":"submit_pending"}},
                "required":["method","event","body"]
            }
        ])
    );
    let wrong_role = reviewer.request(
        3,
        "tools/call",
        json!({"name":"delivery_report_write","arguments":{}}),
    );
    assert_tool_error(&wrong_role, "tool is forbidden for the current run phase");

    let mut revoked = McpProcess::start(&endpoint, "revoked-cap", "Revoked");
    revoked.initialize();
    let revoked_list = revoked.request(2, "tools/list", json!({}));
    assert_eq!(revoked_list["error"]["code"], -32600);
    assert_eq!(revoked_list["error"]["message"], "run authority rejected");
    let revoked_call = revoked.request(
        3,
        "tools/call",
        json!({"name":"delivery_report_write","arguments":{}}),
    );
    assert_tool_error(&revoked_call, "run authority rejected");

    let mut ended = McpProcess::start(&endpoint, "ended-cap", "Ended");
    ended.initialize();
    let ended_list = ended.request(2, "tools/list", json!({}));
    assert_eq!(ended_list["error"]["code"], -32600);
    assert_eq!(ended_list["error"]["message"], "run authority rejected");
    let ended_call = ended.request(
        3,
        "tools/call",
        json!({"name":"delivery_report_write","arguments":{}}),
    );
    assert_tool_error(&ended_call, "run authority rejected");

    for stderr in [
        worker.finish(),
        reviewer.finish(),
        revoked.finish(),
        ended.finish(),
    ] {
        assert!(stderr.is_empty(), "unexpected MCP diagnostics: {stderr:?}");
        assert!(!String::from_utf8_lossy(&stderr).contains("must-not-leak"));
    }
    assert_eq!(
        quorum_core::db::open(&db)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM mailbox", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0,
        "MCP shell calls must not perform lifecycle or GitHub work"
    );
    daemon.stop();
}

/// The validator text a rejected plan must reach the planner with, unaltered.
/// It deliberately carries quoting and punctuation that any reformatting of the
/// endpoint's message would disturb.
const PLANNER_REJECTION: &str =
    r#"plan must contain between 2 and 8 tasks; "only" is the single task in this response"#;

/// A length-prefixed JSON stand-in for the daemon endpoint.
///
/// A planner capability authorizes `submit_plan` only while its graph is the
/// live frozen planning graph, which a running daemon's own planner scheduler
/// would immediately claim and re-key. The real endpoint's wire contract for
/// this operation is covered end-to-end by `agent_client`'s round-trip test, so
/// the MCP shell's proxying is driven against this fake instead.
struct FakeEndpoint {
    _dir: tempfile::TempDir,
    path: PathBuf,
    submitted: Arc<Mutex<Vec<Value>>>,
}

impl FakeEndpoint {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("endpoint.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let submitted = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&submitted);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let Some(request) = read_frame(&mut stream) else {
                    continue;
                };
                write_frame(&mut stream, &fake_response(&request, &recorder));
            }
        });
        Self {
            _dir: dir,
            path,
            submitted,
        }
    }

    fn submitted(&self) -> Vec<Value> {
        self.submitted.lock().unwrap().clone()
    }
}

fn read_frame(stream: &mut std::os::unix::net::UnixStream) -> Option<Value> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).ok()?;
    let mut body = vec![0; u32::from_be_bytes(prefix) as usize];
    stream.read_exact(&mut body).ok()?;
    serde_json::from_slice(&body).ok()
}

fn write_frame(stream: &mut std::os::unix::net::UnixStream, value: &Value) {
    let body = serde_json::to_vec(value).unwrap();
    let _ = stream.write_all(&(body.len() as u32).to_be_bytes());
    let _ = stream.write_all(&body);
    let _ = stream.flush();
}

fn fake_response(request: &Value, submitted: &Mutex<Vec<Value>>) -> Value {
    assert_eq!(request["version"], 1, "{request:#}");
    assert_eq!(request["capability"], "planner-cap", "{request:#}");
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
            let response = request["operation"]["response"].clone();
            submitted.lock().unwrap().push(response.clone());
            if response["tasks"].as_array().map_or(0, Vec::len) >= 2 {
                json!({"version":1,"ok":true,"result":{"type":"plan_accepted","graph_id":5}})
            } else {
                json!({
                    "version": 1,
                    "ok": false,
                    "error": {"code": "invalid_plan", "message": PLANNER_REJECTION}
                })
            }
        }
        other => panic!("unexpected endpoint operation {other}"),
    }
}

fn plan_task(key: &str) -> Value {
    json!({
        "key": key,
        "title": format!("implement {key}"),
        "implementation_delta": format!("edit src/{key}.rs"),
        "affected_paths": [format!("src/{key}.rs")],
        "observable_outcome": "the new behavior is observable",
        "deliverables": [{"kind": "write", "path": format!("src/{key}.rs")}],
        "acceptance_criteria": ["the new behavior is covered by a test"],
        "source_constraints": ["do not change unrelated modules"],
        "verification_expectations": ["cargo test passes"],
        "non_goals": ["no unrelated refactors"],
        "prerequisites": [],
    })
}

/// A planner shell that has completed discovery, as every MCP client does
/// before its first call: `submit_plan` is routed only for a run whose last
/// advertised inventory was a planner's.
fn planner_shell(endpoint: &FakeEndpoint) -> McpProcess {
    let mut planner = McpProcess::start(&endpoint.path, "planner-cap", "Planner");
    planner.initialize();
    assert_eq!(
        tool_names(&planner.request(2, "tools/list", json!({}))),
        vec!["submit_plan"]
    );
    planner
}

fn tool_text(response: &Value) -> &str {
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool result had no text content: {response:#}"))
}

#[test]
fn tools_list_exposes_submit_plan() {
    let endpoint = FakeEndpoint::start();
    let mut planner = McpProcess::start(&endpoint.path, "planner-cap", "Planner");
    planner.initialize();

    let listed = planner.request(2, "tools/list", json!({}));
    assert_eq!(tool_names(&listed), vec!["submit_plan"]);
    let advertised = tool(&listed, "submit_plan");
    let schema = &advertised["inputSchema"];
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["response"]));
    assert_eq!(schema["properties"]["response"]["type"], "object");

    // The closed shapes are the planner prompt's own, and the retry rule is
    // load-bearing: acceptance is one atomic write, so a corrected resubmission
    // after a transport failure would silently lose to the first plan.
    let description = advertised["description"].as_str().unwrap();
    assert!(
        description.contains(
            r#"BLOCKER={"outcome":"blocker","category":"<ambiguous_scope|missing_decision|external_constraint|no_safe_split>""#
        ),
        "tool description omits the BLOCKER shape: {description}"
    );
    assert!(
        description.contains(r#"PLAN={"outcome":"plan","tasks":"#),
        "tool description omits the PLAN shape: {description}"
    );
    assert!(
        description.contains(
            "If this call times out or the transport fails, call it again — `already_submitted` \
             means your first plan stands; never resubmit a corrected plan after a transport \
             failure."
        ),
        "tool description omits the resubmission rule: {description}"
    );

    assert!(planner.finish().is_empty());
}

#[test]
fn submit_plan_proxies_and_returns_endpoint_error_verbatim() {
    let endpoint = FakeEndpoint::start();
    let mut planner = planner_shell(&endpoint);

    let rejected = planner.request(
        3,
        "tools/call",
        json!({
            "name": "submit_plan",
            "arguments": {"response": {"outcome": "plan", "tasks": [plan_task("only")]}}
        }),
    );
    assert_eq!(rejected["result"]["isError"], true, "{rejected:#}");
    assert_eq!(
        serde_json::from_str::<Value>(tool_text(&rejected)).unwrap(),
        json!({"code": "invalid_plan", "message": PLANNER_REJECTION}),
        "{rejected:#}"
    );

    assert!(planner.finish().is_empty());
}

#[test]
fn submit_plan_success_returns_graph_id() {
    let endpoint = FakeEndpoint::start();
    let mut planner = planner_shell(&endpoint);
    let plan = json!({"outcome": "plan", "tasks": [plan_task("first"), plan_task("second")]});

    let accepted = planner.request(
        3,
        "tools/call",
        json!({"name": "submit_plan", "arguments": {"response": plan.clone()}}),
    );
    assert_ne!(accepted["result"]["isError"], true, "{accepted:#}");
    assert_eq!(
        serde_json::from_str::<Value>(tool_text(&accepted)).unwrap(),
        json!({"accepted": true, "graph_id": 5}),
        "{accepted:#}"
    );
    // The shell relays the plan unchanged; it never inspects or rewrites it.
    assert_eq!(endpoint.submitted(), vec![plan]);

    assert!(planner.finish().is_empty());
}

/// A plan too large for one endpoint request is refused before the client
/// connects, so nothing was submitted and repeating the identical call could
/// only fail again. It must therefore never be reported as a transport failure,
/// whose documented remedy is exactly that retry.
#[test]
fn submit_plan_reports_an_oversized_plan_as_request_too_large() {
    let endpoint = FakeEndpoint::start();
    let mut planner = planner_shell(&endpoint);
    let mut first = plan_task("first");
    first["implementation_delta"] = json!("x".repeat(70 * 1024));

    let oversized = planner.request(
        3,
        "tools/call",
        json!({
            "name": "submit_plan",
            "arguments": {
                "response": {"outcome": "plan", "tasks": [first, plan_task("second")]}
            }
        }),
    );
    assert_eq!(oversized["result"]["isError"], true, "{oversized:#}");
    assert_eq!(
        serde_json::from_str::<Value>(tool_text(&oversized)).unwrap(),
        json!({
            "code": "request_too_large",
            "message": "the plan does not fit one endpoint request; submit a smaller plan"
        }),
        "{oversized:#}"
    );
    assert!(
        endpoint.submitted().is_empty(),
        "an oversized plan must never reach the endpoint: {:?}",
        endpoint.submitted()
    );

    assert!(planner.finish().is_empty());
}

#[test]
fn lockfile_pins_reviewed_rmcp_release() {
    let lock = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.lock"))
        .unwrap();
    let package: toml::Value = toml::from_str(&lock).unwrap();
    let versions: Vec<_> = package["package"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["name"].as_str() == Some("rmcp"))
        .map(|entry| entry["version"].as_str().unwrap())
        .collect();
    assert_eq!(versions, vec!["3.0.1"]);
}
