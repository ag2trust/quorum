#![cfg(unix)]

//! Real-process coverage for how `quorum serve` attaches a managed Claude run
//! to the daemon-embedded agent MCP: one Streamable HTTP server named
//! `quorum`, authenticated by the run bearer through Claude's `${VAR}`
//! expansion, so the capability never enters the provider argv and no stdio
//! `quorum agent-mcp` child is spawned.
//!
//! The real-binary test drives the installed `claude` CLI against a local
//! fake Messages API (`ANTHROPIC_BASE_URL`), so it spends no tokens yet
//! exercises the real CLI's MCP-config parsing, header expansion, HTTP
//! transport, tool listing, and `submit_work` call end to end. It is skipped
//! when no `claude` is on PATH, like the CLI-boundary tests in
//! `serve/agent.rs`.

mod common;

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

const REPO: &str = "test/repo";
const SUMMARY: &str = "delivered by the real claude CLI over HTTP MCP";

fn quorum_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
}

fn claude_available() -> bool {
    Command::new("claude")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn init_git_repo(dir: &Path) {
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.email", "test@test.com"],
        vec!["config", "user.name", "Test"],
        vec!["commit", "--allow-empty", "-m", "init"],
    ] {
        assert!(Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .unwrap()
            .success());
    }
    for args in [vec!["remote", "add", "origin"], vec!["fetch", "origin"]] {
        let mut command = Command::new("git");
        command.arg("-C").arg(dir).args(&args);
        if args[0] == "remote" {
            command.arg(dir);
        }
        assert!(command.status().unwrap().success());
    }
}

fn write_names_file(dir: &Path) -> PathBuf {
    let path = dir.join("names.txt");
    let names: String = (0..20).map(|i| format!("Agent{i}\n")).collect();
    std::fs::write(&path, names).unwrap();
    path
}

/// `quorum init` writes the default serve config, whose every role routes to
/// the `claude` runner — so `--agent-bin` is launched through the Claude
/// adapter.
fn init_home(home: &Path) {
    let status = Command::new(quorum_bin())
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", REPO)
        .arg("init")
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "init failed");
}

fn db_path(home: &Path) -> PathBuf {
    home.join("repos").join("test__repo").join("quorum.db")
}

fn seed_task(home: &Path, title: &str) -> i64 {
    let out = Command::new(quorum_bin())
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", REPO)
        .args([
            "task-create",
            "--title",
            title,
            "--created-by",
            "TestCreator",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "task-create failed: {out:?}");
    let json: Value = serde_json::from_slice(&out.stdout).unwrap();
    json["id"].as_i64().unwrap()
}

fn serve_command(home: &Path, repo: &Path, worktrees: &Path, agent_bin: &Path) -> Command {
    let mut command = common::test_daemon_command(quorum_bin());
    command
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", REPO)
        .env_remove("QUORUM_RUN_ID")
        .env_remove("QUORUM_AGENT")
        .env_remove("QUORUM_AGENT_ENDPOINT")
        .env_remove("QUORUM_AGENT_MCP_URL")
        .arg("serve")
        .arg("--repo")
        .arg(REPO)
        .arg("--cap")
        .arg("1")
        .arg("--repo-dir")
        .arg(repo)
        .arg("--worktree-base")
        .arg(worktrees)
        .arg("--names-file")
        .arg(write_names_file(home))
        .arg("--agent-bin")
        .arg(agent_bin)
        .arg("--merge-cmd")
        .arg("true");
    command
}

struct Serve {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl Serve {
    fn spawn(mut command: Command) -> Self {
        let mut child = command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self { child, lines }
    }

    fn stderr(&self) -> Vec<String> {
        self.lines.try_iter().collect()
    }
}

impl Drop for Serve {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}

/// Every provider invocation records, in its own file, its `QUORUM_RUN_ID`,
/// its `QUORUM_AGENT_MCP_URL`, then its argv one argument per line (the
/// MCP config is single-line JSON and the prompt travels on stdin).
fn recording_prelude(captures: &Path) -> String {
    format!(
        "capture='{dir}/argv.'$$\n\
         {{ printf '%s\\n' \"$QUORUM_RUN_ID\" \"$QUORUM_AGENT_MCP_URL\"; printf '%s\\n' \"$@\"; }} \
         > \"$capture.tmp\" && mv \"$capture.tmp\" \"$capture\"\n",
        dir = captures.display()
    )
}

struct Invocation {
    run_id: String,
    mcp_url: String,
    args: Vec<String>,
}

fn invocations(captures: &Path) -> Vec<Invocation> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(captures).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "tmp") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let mut lines = text.lines().map(str::to_string);
        found.push(Invocation {
            run_id: lines.next().unwrap_or_default(),
            mcp_url: lines.next().unwrap_or_default(),
            args: lines.collect(),
        });
    }
    found
}

fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    let index = args.iter().position(|arg| arg == flag)?;
    args.get(index + 1).map(String::as_str)
}

/// The managed-run contract on one recorded invocation: an HTTP `quorum`
/// server at the run's URL with an env-expanded bearer, under
/// `--strict-mcp-config`, and the capability value nowhere in argv.
fn assert_http_mcp_invocation(invocation: &Invocation) {
    let args = &invocation.args;
    assert!(
        !invocation.run_id.is_empty(),
        "managed run lacks a capability"
    );
    assert!(
        invocation.mcp_url.starts_with("http://127.0.0.1:") && invocation.mcp_url.ends_with("/mcp"),
        "{}",
        invocation.mcp_url
    );
    let config: Value = serde_json::from_str(
        flag_value(args, "--mcp-config")
            .unwrap_or_else(|| panic!("managed run has no --mcp-config: {args:?}")),
    )
    .unwrap();
    assert_eq!(
        config,
        json!({"mcpServers": {"quorum": {
            "type": "http",
            "url": invocation.mcp_url,
            "headers": {"Authorization": "Bearer ${QUORUM_RUN_ID}"},
        }}}),
        "{args:?}"
    );
    assert!(
        args.iter().any(|arg| arg == "--strict-mcp-config"),
        "{args:?}"
    );
    assert!(
        !args.iter().any(|arg| arg.contains(&invocation.run_id)),
        "the run capability leaked into the provider argv: {args:?}"
    );
    assert!(
        !args.iter().any(|arg| arg.contains("agent-mcp")),
        "a stdio agent-mcp server is configured: {args:?}"
    );
}

/// Command lines of every live descendant of `root`.
#[cfg(target_os = "linux")]
fn descendant_cmdlines(root: u32) -> Vec<String> {
    let mut parents = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        // `pid (comm) state ppid …`: comm may hold spaces or parens.
        let Some(ppid) = stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().nth(1))
            .and_then(|ppid| ppid.parse::<u32>().ok())
        else {
            continue;
        };
        parents.push((pid, ppid));
    }
    let mut frontier = vec![root];
    let mut cmdlines = Vec::new();
    while let Some(parent) = frontier.pop() {
        for &(pid, _) in parents.iter().filter(|(_, ppid)| *ppid == parent) {
            frontier.push(pid);
            if let Ok(raw) = std::fs::read(format!("/proc/{pid}/cmdline")) {
                cmdlines.push(String::from_utf8_lossy(&raw).replace('\0', " "));
            }
        }
    }
    cmdlines
}

#[cfg(not(target_os = "linux"))]
fn descendant_cmdlines(_root: u32) -> Vec<String> {
    Vec::new()
}

/// Fail if any descendant of the daemon is a stdio agent MCP server.
fn assert_no_stdio_agent_mcp(serve: &Serve) {
    let stdio: Vec<String> = descendant_cmdlines(serve.child.id())
        .into_iter()
        .filter(|cmdline| cmdline.contains("agent-mcp"))
        .collect();
    assert!(stdio.is_empty(), "stdio agent-mcp child spawned: {stdio:?}");
}

/// A recording stub stands in for Claude: it captures what the Claude adapter
/// hands the provider through the real daemon, then waits on stdin.
#[test]
fn serve_launches_claude_worker_with_http_mcp_and_no_capability_in_argv() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let worktrees = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let captures = scratch.path().join("captures");
    std::fs::create_dir(&captures).unwrap();
    init_git_repo(repo.path());
    init_home(home.path());
    seed_task(home.path(), "record the managed Claude argv");
    common::classify_open_tasks(home.path());
    let stub = scratch.path().join("agent-bin");
    std::fs::write(
        &stub,
        format!(
            "#!/bin/sh\n{}while IFS= read -r _line; do :; done\n",
            recording_prelude(&captures)
        ),
    )
    .unwrap();
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();

    let serve = Serve::spawn(serve_command(
        home.path(),
        repo.path(),
        worktrees.path(),
        &stub,
    ));
    let invocation =
        common::wait_until("the managed worker launch", Duration::from_secs(60), || {
            match invocations(&captures).pop() {
                Some(invocation) => common::WaitState::Ready(invocation),
                None => common::WaitState::Pending(format!("stderr: {:?}", serve.stderr())),
            }
        });
    assert_http_mcp_invocation(&invocation);
    assert_no_stdio_agent_mcp(&serve);
}

/// Minimal Anthropic Messages API stand-in. While the request offers
/// `mcp__quorum__submit_work` and carries no tool result yet, it answers
/// with a `submit_work` tool call; otherwise it ends the turn with text.
struct FakeMessagesApi {
    base_url: String,
    offered_tools: Arc<Mutex<Vec<Vec<String>>>>,
}

impl FakeMessagesApi {
    fn bind() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let offered_tools = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&offered_tools);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let recorded = Arc::clone(&recorded);
                std::thread::spawn(move || {
                    let _ = Self::answer(stream, &recorded);
                });
            }
        });
        Self {
            base_url,
            offered_tools,
        }
    }

    fn answer(
        mut stream: std::net::TcpStream,
        offered_tools: &Mutex<Vec<Vec<String>>>,
    ) -> std::io::Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut request_line = String::new();
        reader.read_line(&mut request_line)?;
        let path = request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or_default()
            .to_string();
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line)? == 0 || line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse().unwrap_or(0);
                }
            }
        }
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body)?;
        let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);

        let (content_type, response) =
            if path.starts_with("/v1/messages") && !path.contains("count_tokens") {
                let tools: Vec<String> = request["tools"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|tool| tool["name"].as_str().map(str::to_string))
                    .collect();
                let answered = request["messages"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|message| message["content"].as_array())
                    .flatten()
                    .any(|block| block["type"] == "tool_result");
                let call = !answered && tools.iter().any(|tool| tool == "mcp__quorum__submit_work");
                offered_tools.lock().unwrap().push(tools);
                Self::message(&request, call)
            } else {
                ("application/json", json!({"input_tokens": 1}).to_string())
            };
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{response}",
            response.len()
        )
    }

    fn message(request: &Value, call: bool) -> (&'static str, String) {
        let (block, delta, stop) = if call {
            (
                json!({"type":"tool_use","id":"toolu_quorum_1","name":"mcp__quorum__submit_work","input":{}}),
                json!({"type":"input_json_delta","partial_json": json!({"summary": SUMMARY}).to_string()}),
                "tool_use",
            )
        } else {
            (
                json!({"type":"text","text":""}),
                json!({"type":"text_delta","text":"done"}),
                "end_turn",
            )
        };
        let message = json!({
            "id": "msg_quorum_test", "type": "message", "role": "assistant",
            "model": request["model"], "content": [], "stop_reason": null,
            "stop_sequence": null, "usage": {"input_tokens": 1, "output_tokens": 1}
        });
        if request["stream"] != true {
            let mut complete = message;
            let mut block = block;
            if call {
                block["input"] = json!({"summary": SUMMARY});
            } else {
                block["text"] = json!("done");
            }
            complete["content"] = json!([block]);
            complete["stop_reason"] = json!(stop);
            return ("application/json", complete.to_string());
        }
        let events = [
            json!({"type":"message_start","message":message}),
            json!({"type":"content_block_start","index":0,"content_block":block}),
            json!({"type":"content_block_delta","index":0,"delta":delta}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":stop,"stop_sequence":null},"usage":{"output_tokens":1}}),
            json!({"type":"message_stop"}),
        ];
        let stream = events
            .iter()
            .map(|event| {
                format!(
                    "event: {}\ndata: {event}\n\n",
                    event["type"].as_str().unwrap()
                )
            })
            .collect();
        ("text/event-stream", stream)
    }
}

/// The real Claude CLI, launched by the daemon as a managed worker, reaches
/// the embedded agent MCP listener over Streamable HTTP with its run bearer:
/// it lists the quorum tools and completes `submit_work`, which lands in the
/// daemon mailbox as this task's delivery. No stdio `agent-mcp` child exists.
#[test]
fn real_claude_worker_submits_work_over_http_agent_mcp() {
    if !claude_available() {
        eprintln!("skipped: no claude binary on PATH");
        return;
    }
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let worktrees = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let captures = scratch.path().join("captures");
    let claude_config = scratch.path().join("claude-config");
    std::fs::create_dir(&captures).unwrap();
    std::fs::create_dir(&claude_config).unwrap();
    init_git_repo(repo.path());
    init_home(home.path());
    let task_id = seed_task(home.path(), "deliver through the real claude CLI");
    common::classify_open_tasks(home.path());
    let api = FakeMessagesApi::bind();

    // Point the real CLI at the fake API with an empty config dir and a
    // throwaway key: no account credential is reachable, so no token spend.
    // Tool-search deferral is disabled so the scripted model sees the quorum
    // tools directly instead of behind `ToolSearch`.
    let wrapper = scratch.path().join("agent-bin");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\n{prelude}exec env CLAUDE_CONFIG_DIR='{config}' \
             ANTHROPIC_BASE_URL='{base}' ANTHROPIC_API_KEY=quorum-test-fake-key \
             ANTHROPIC_AUTH_TOKEN= CLAUDE_CODE_OAUTH_TOKEN= ENABLE_TOOL_SEARCH=false claude \"$@\"\n",
            prelude = recording_prelude(&captures),
            config = claude_config.display(),
            base = api.base_url,
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

    let serve = Serve::spawn(serve_command(
        home.path(),
        repo.path(),
        worktrees.path(),
        &wrapper,
    ));
    let db = db_path(home.path());
    let mut stdio_children = Vec::new();
    let (agent, note) = common::wait_until(
        "the real claude submit_work delivery",
        Duration::from_secs(120),
        || {
            stdio_children.extend(
                descendant_cmdlines(serve.child.id())
                    .into_iter()
                    .filter(|cmdline| cmdline.contains("agent-mcp")),
            );
            let row = quorum_core::db::open(&db).ok().and_then(|conn| {
                conn.query_row(
                    "SELECT agent, note FROM mailbox WHERE kind='done' AND task_id=?1",
                    [task_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .ok()
            });
            match row {
                Some(row) => common::WaitState::Ready(row),
                None => common::WaitState::Pending(format!(
                    "offered tools: {:?}; stderr: {:?}",
                    api.offered_tools.lock().unwrap(),
                    serve.stderr()
                )),
            }
        },
    );
    assert_eq!(note.as_deref(), Some(SUMMARY));
    assert!(
        stdio_children.is_empty(),
        "stdio agent-mcp child spawned: {stdio_children:?}"
    );
    assert_no_stdio_agent_mcp(&serve);

    // The quorum tools were listed to the model from the HTTP server.
    let offered = api.offered_tools.lock().unwrap().clone();
    assert!(
        offered
            .iter()
            .any(|tools| tools.iter().any(|tool| tool == "mcp__quorum__submit_work")),
        "{offered:?}"
    );

    // The delivering run is the recorded worker launch, whose argv carried
    // the HTTP config and never the capability.
    let launches = invocations(&captures);
    assert!(!launches.is_empty());
    for invocation in &launches {
        assert_http_mcp_invocation(invocation);
    }
    let conn = quorum_core::db::open(&db).unwrap();
    let worker: String = conn
        .query_row(
            "SELECT agent_name FROM agent_runs WHERE task_id=?1 ORDER BY id LIMIT 1",
            [task_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(agent, worker);
}
