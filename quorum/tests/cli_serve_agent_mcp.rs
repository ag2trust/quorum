#![cfg(unix)]

//! Real-process coverage for the agent MCP listener `quorum serve` embeds:
//! startup validation of its address, the `QUORUM_AGENT_MCP_URL` contract an
//! external `agent_bin` receives, and the fail-closed behaviour when the
//! listener cannot bind.

mod common;

use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const REPO: &str = "test/repo";

fn quorum_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
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

/// `quorum init`, then prepend `top_level` keys to the generated serve config
/// (top-level keys must precede its tables).
fn init_home(home: &Path, top_level: &str) {
    let status = Command::new(quorum_bin())
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", REPO)
        .arg("init")
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "init failed");
    let config = home.join("serve").join("test__repo.toml");
    let generated = std::fs::read_to_string(&config).unwrap();
    std::fs::write(&config, format!("{top_level}\n{generated}")).unwrap();
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
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
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

/// Run `serve` to completion with a config that startup must refuse.
fn refused_serve(top_level: &str) -> Output {
    let home = tempfile::tempdir().unwrap();
    init_home(home.path(), top_level);
    serve_command(
        home.path(),
        Path::new("/tmp/x"),
        Path::new("/tmp/y"),
        Path::new("/bin/false"),
    )
    .output()
    .unwrap()
}

#[test]
fn serve_refuses_an_agent_mcp_address_equal_to_the_orchestrator_address() {
    let output = refused_serve(
        "agent_mcp_listen = \"127.0.0.1:48811\"\norchestrator_mcp_listen = \"127.0.0.1:48811\"",
    );
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("orchestrator_mcp_listen"),
        "{output:?}"
    );
}

#[test]
fn serve_refuses_a_non_loopback_agent_mcp_bind_without_allowed_hosts() {
    for top_level in [
        "agent_mcp_listen = \"0.0.0.0:0\"",
        "agent_mcp_listen = \"[::]:0\"",
        "agent_mcp_listen = \"0.0.0.0:0\"\nagent_mcp_allowed_hosts = []",
    ] {
        let output = refused_serve(top_level);
        assert_eq!(output.status.code(), Some(2), "{top_level}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("allowed host"),
            "{top_level}: {output:?}"
        );
    }
}

/// An `agent_bin` stub: records its environment, then calls `react` on the
/// agent MCP listener over plain HTTP with `Authorization: Bearer
/// $QUORUM_RUN_ID`, using only bash's `/dev/tcp`.
fn http_stub(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let capture = dir.join("env");
    let response = dir.join("response");
    let stub = dir.join("agent-bin");
    let script = format!(
        r#"#!/bin/bash
printf '%s\n%s\n%s\n' "$QUORUM_AGENT_MCP_URL" "$QUORUM_RUN_ID" "$$" > '{capture}.tmp'
mv '{capture}.tmp' '{capture}'
hostport=${{QUORUM_AGENT_MCP_URL#http://}}
hostport=${{hostport%/mcp}}
body='{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"react","arguments":{{"state":"note","reason":"reached over http"}}}}}}'
exec 3<>"/dev/tcp/${{hostport%:*}}/${{hostport##*:}}"
printf 'POST /mcp HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nConnection: close\r\nContent-Length: %s\r\n\r\n%s' \
  "$hostport" "$QUORUM_RUN_ID" "${{#body}}" "$body" >&3
cat <&3 > '{response}.tmp'
mv '{response}.tmp' '{response}'
exec sleep 30
"#,
        capture = capture.display(),
        response = response.display(),
    );
    std::fs::write(&stub, script).unwrap();
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    (stub, capture, response)
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

#[test]
fn external_agent_bin_receives_the_agent_mcp_url_and_reacts_over_http() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let worktrees = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    init_git_repo(repo.path());
    init_home(home.path(), "");
    let task_id = seed_task(home.path(), "reach the agent MCP over HTTP");
    common::classify_open_tasks(home.path());
    let (stub, capture, response) = http_stub(scratch.path());

    let serve = Serve::spawn(serve_command(
        home.path(),
        repo.path(),
        worktrees.path(),
        &stub,
    ));
    let raw_response = common::wait_until(
        "the stub's agent MCP response",
        Duration::from_secs(60),
        || match std::fs::read_to_string(&response) {
            Ok(body) => common::WaitState::Ready(body),
            Err(_) => common::WaitState::Pending(format!("stderr: {:?}", serve.stderr())),
        },
    );
    let env = std::fs::read_to_string(&capture).unwrap();
    let mut env = env.lines();
    let url = env.next().unwrap().to_string();
    let run_id = env.next().unwrap().to_string();
    let stub_pid: libc::pid_t = env.next().unwrap().parse().unwrap();

    let port = url
        .strip_prefix("http://127.0.0.1:")
        .and_then(|rest| rest.strip_suffix("/mcp"))
        .unwrap_or_else(|| panic!("QUORUM_AGENT_MCP_URL must be loopback /mcp: {url:?}"));
    assert!(port.parse::<u16>().unwrap() > 0, "{url}");
    assert!(!run_id.is_empty(), "QUORUM_RUN_ID must be set");

    let (head, body) = raw_response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("malformed HTTP response: {raw_response:?}"));
    assert!(head.starts_with("HTTP/1.1 200"), "{raw_response}");
    let reply: serde_json::Value = serde_json::from_str(body).unwrap();
    let result = &reply["result"];
    assert_ne!(result["isError"], true, "{reply:#}");
    let text: serde_json::Value =
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(text["ok"], true, "{reply:#}");
    let mailbox_id = text["mailbox_id"].as_i64().unwrap();

    // The reaction reached the daemon as this run's worker.
    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let row: (String, i64, Option<String>, Option<String>) = conn
        .query_row(
            "SELECT kind, task_id, note, feedback FROM mailbox WHERE id=?1",
            [mailbox_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        (
            "task_update".to_string(),
            task_id,
            Some("note".to_string()),
            Some("reached over http".to_string())
        )
    );

    // Daemon shutdown stops the listener: a signal only sets the flag, and
    // once serve has exited the port no longer accepts connections.
    unsafe { libc::kill(stub_pid, libc::SIGKILL) };
    let mut serve = serve;
    unsafe { libc::kill(serve.child.id() as libc::pid_t, libc::SIGINT) };
    let status = common::wait_until("serve to exit", Duration::from_secs(60), || {
        match serve.child.try_wait().unwrap() {
            Some(status) => common::WaitState::Ready(status),
            None => common::WaitState::Pending(format!("stderr: {:?}", serve.stderr())),
        }
    });
    assert!(status.success(), "{status}");
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    assert!(
        std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)).is_err(),
        "the agent MCP listener must stop with the daemon"
    );
}

#[test]
fn a_taken_agent_mcp_port_launches_no_managed_run_and_is_recorded_loudly() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = occupied.local_addr().unwrap();
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let worktrees = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    init_git_repo(repo.path());
    init_home(home.path(), &format!("agent_mcp_listen = \"{addr}\""));
    let task_id = seed_task(home.path(), "must not be launched");
    common::classify_open_tasks(home.path());
    let marker = scratch.path().join("launched");
    let stub = scratch.path().join("agent-bin");
    std::fs::write(
        &stub,
        format!("#!/bin/sh\ntouch '{}'\nexec sleep 30\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();

    let output = serve_command(home.path(), repo.path(), worktrees.path(), &stub)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("agent MCP listener"),
        "{output:?}"
    );
    drop(occupied);

    assert!(!marker.exists(), "no managed run may be launched");
    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let error: String = conn
        .query_row(
            "SELECT detail FROM errors WHERE source='agent_mcp'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(error.contains(&addr.to_string()), "{error}");
    let alerts: i64 = conn
        .query_row(
            "SELECT count(*) FROM messages
             WHERE kind='alert' AND recipient='owner' AND refs='agent_mcp:listener'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(alerts, 1);
    let (status, runs): (String, i64) = conn
        .query_row(
            "SELECT status, (SELECT count(*) FROM agent_runs) FROM tasks WHERE id=?1",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((status.as_str(), runs), ("open", 0));
    // The daemon lock was released on the way out.
    let locks: i64 = conn
        .query_row("SELECT count(*) FROM daemon_lock", [], |row| row.get(0))
        .unwrap();
    assert_eq!(locks, 0);
}
