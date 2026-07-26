//! Deterministic production-path coverage for mixed provider lifecycle routing.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn cargo_bin(name: &str) -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin(name)
}

fn init_git_repo(dir: &std::path::Path) {
    let d = dir.to_string_lossy();
    for args in [
        vec!["-C", &d, "init", "-b", "main"],
        vec!["-C", &d, "config", "user.email", "test@test.com"],
        vec!["-C", &d, "config", "user.name", "Test"],
        vec!["-C", &d, "commit", "--allow-empty", "-m", "init"],
        vec!["-C", &d, "remote", "add", "origin", &d],
        vec!["-C", &d, "fetch", "origin"],
    ] {
        assert!(Command::new("git").args(args).status().unwrap().success());
    }
}

fn write_names(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("names.txt");
    let mut file = std::fs::File::create(&path).unwrap();
    for i in 0..20 {
        writeln!(file, "Agent{i}").unwrap();
    }
    path
}

fn write_dual_protocol_runner(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("dual-runner.sh");
    std::fs::write(
        &path,
        r#"#!/bin/sh
printf '%s|%s\n' "${QUORUM_AGENT:-none}" "$*" >> "$RUNNER_LOG"
if [ "$1" = "exec" ]; then
  printf '{"type":"thread.started","thread_id":"thread-%s"}\n' "${QUORUM_AGENT:-none}"
  printf '{"type":"turn.started"}\n'
  printf '{"type":"item.completed","item":{"id":"i","type":"agent_message","text":"done"}}\n'
  printf '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}\n'
  sleep 30
else
  while IFS= read -r line; do
    printf '{"type":"assistant","message":{"content":"done"}}\n'
    printf '{"type":"result","result":"done","usage":{"input_tokens":10,"output_tokens":5},"total_cost_usd":0.001,"is_error":false}\n'
  done
fi
"#,
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    path
}

struct ServeHandle {
    child: std::process::Child,
    rx: mpsc::Receiver<String>,
    lines: Vec<String>,
    _sentinel: tempfile::TempDir,
}

impl Drop for ServeHandle {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}

impl ServeHandle {
    fn wait_for(&mut self, needle: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < deadline {
            match self.rx.recv_timeout(deadline - std::time::Instant::now()) {
                Ok(line) => {
                    let found = line.contains(needle);
                    self.lines.push(line);
                    if found {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        panic!("did not see {needle:?}: {:?}", self.lines);
    }

    fn agent_after(&self, marker: &str) -> String {
        self.lines
            .iter()
            .rev()
            .find_map(|line| {
                line.split(marker)
                    .nth(1)
                    .and_then(|rest| rest.split_whitespace().next())
            })
            .unwrap_or_else(|| panic!("no agent after {marker:?}: {:?}", self.lines))
            .to_string()
    }

    fn stop(mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGINT) };
        assert!(self.child.wait().unwrap().success());
    }
}

struct Case {
    home: tempfile::TempDir,
    _repo: tempfile::TempDir,
    _worktrees: tempfile::TempDir,
    runner_log: std::path::PathBuf,
    handle: ServeHandle,
}

impl Case {
    fn start(default_provider: &str, model: &str, labels: Option<&str>) -> Self {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let worktrees = tempfile::tempdir().unwrap();
        init_git_repo(repo.path());
        let names = write_names(home.path());
        let runner = write_dual_protocol_runner(home.path());
        let runner_log = home.path().join("runner.log");
        std::fs::write(&runner_log, "").unwrap();

        assert!(Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .arg("init")
            .status()
            .unwrap()
            .success());
        let mut create = Command::new(cargo_bin("quorum"));
        create
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .args([
                "task-create",
                "--title",
                "provider lifecycle",
                "--created-by",
                "test",
                "--refs",
                r#"{"cx_by":"test:v1","cx_est":2}"#,
            ]);
        if let Some(labels) = labels {
            create.args(["--labels", labels]);
        }
        assert!(create.status().unwrap().success());

        let sentinel = tempfile::tempdir().unwrap();
        let mut child = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("RUNNER_LOG", &runner_log)
            .args([
                "serve",
                "--repo",
                "test/repo",
                "--cap",
                "1",
                "--repo-dir",
                &repo.path().to_string_lossy(),
                "--worktree-base",
                &worktrees.path().to_string_lossy(),
                "--names-file",
                &names.to_string_lossy(),
                "--agent",
                default_provider,
                "--model",
                model,
                "--agent-bin",
                &runner.to_string_lossy(),
                "--merge-cmd",
                "true",
                "--merge-checks-cmd",
                "echo ready",
                "--merge-checks-timeout-secs",
                "10",
                "--merge-checks-poll-secs",
                "1",
                "--exit-when-gone",
                &sentinel.path().to_string_lossy(),
            ])
            .stderr(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            home,
            _repo: repo,
            _worktrees: worktrees,
            runner_log,
            handle: ServeHandle {
                child,
                rx,
                lines: Vec::new(),
                _sentinel: sentinel,
            },
        }
    }

    fn db(&self) -> rusqlite::Connection {
        quorum_core::db::open(&self.home.path().join("repos/test__repo/quorum.db")).unwrap()
    }

    fn done(&self, agent: &str, args: &[&str]) {
        let mut conn = self.db();
        let run_id = match quorum_core::capabilities::active_for_agent(&conn, agent).unwrap() {
            Some(cap) => cap.run_id,
            None => {
                let run_id = format!("test-{agent}-{}", std::process::id());
                let role = if args.contains(&"--verdict") {
                    "reviewer"
                } else {
                    "worker"
                };
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                quorum_core::capabilities::issue(&mut conn, &run_id, 1, agent, role, now).unwrap();
                run_id
            }
        };
        let mut command = Command::new(cargo_bin("quorum"));
        command
            .env("QUORUM_HOME", self.home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("QUORUM_RUN_ID", run_id)
            .args(["done", "--agent", agent])
            .args(args);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "done failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn approve_current_reviewer(&mut self, marker: &str) {
        self.handle.wait_for(marker);
        let reviewer = self.handle.agent_after(marker);
        self.handle.wait_for("turn");
        self.done(
            &reviewer,
            &["--pr", "1", "--verdict", "approved", "--blocking", "0"],
        );
    }

    fn finish(mut self) -> Vec<quorum_core::agent_runs::AgentRun> {
        self.handle.wait_for("spawning agent");
        let worker = self.handle.agent_after("spawning agent ");
        self.handle.wait_for("turn");
        self.done(&worker, &["--pr", "1"]);
        self.approve_current_reviewer("spawning reviewer ");
        self.approve_current_reviewer("R2: pre-merge reviewer ");
        self.handle.wait_for("checks passed");
        self.handle.wait_for("merged — firing MergeSucceeded");
        self.handle.wait_for("lifecycle: task #1 -> done");
        let conn = self.db();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(task.status, "done");
        let claims: i64 = conn
            .query_row(
                "SELECT count(*) FROM claims WHERE active=1 AND target='task:1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claims, 0);
        let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
        drop(conn);
        self.handle.stop();
        runs
    }
}

fn providers(runs: &[quorum_core::agent_runs::AgentRun]) -> Vec<&str> {
    runs.iter()
        .filter(|run| run.role == "worker" || run.role == "reviewer")
        .map(|run| run.provider.as_deref().unwrap())
        .collect()
}

#[test]
fn production_lifecycle_routes_claude_default_all_codex_and_mixed() {
    let claude = Case::start("claude", "claude-opus-4-6", None).finish();
    assert_eq!(providers(&claude), ["claude", "claude", "claude"]);

    let codex = Case::start("codex", "o3", None).finish();
    assert_eq!(providers(&codex), ["codex", "codex", "codex"]);

    let mixed = Case::start(
        "claude",
        "claude-opus-4-6",
        Some(r#"["tier:o3","effort:high"]"#),
    )
    .finish();
    assert_eq!(providers(&mixed), ["codex", "claude", "claude"]);
}

#[test]
fn changes_reuses_codex_thread_then_runs_fresh_reviews_and_merges() {
    let mut case = Case::start(
        "claude",
        "claude-opus-4-6",
        Some(r#"["tier:o3","effort:high"]"#),
    );
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");
    case.done(&worker, &["--pr", "1"]);

    case.handle.wait_for("spawning reviewer ");
    let r1 = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for("result");
    case.done(
        &r1,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "fix it",
        ],
    );
    case.handle.wait_for("rework #1 started");
    case.handle.wait_for("turn");
    let log = std::fs::read_to_string(&case.runner_log).unwrap();
    assert!(
        log.contains("exec resume thread-"),
        "Codex remediation must resume the provider thread: {log}"
    );
    case.done(&worker, &["--pr", "1"]);
    case.handle.wait_for("ResumeReviewer: fed re-review turn");
    let reviewer_pid: i32 = case
        .db()
        .query_row("SELECT pid FROM journal WHERE agent=?1", [&r1], |row| {
            row.get(0)
        })
        .unwrap();
    unsafe { libc::kill(reviewer_pid, libc::SIGKILL) };
    case.handle.wait_for(&format!("reviewer {r1} died"));
    case.approve_current_reviewer("spawning reviewer ");
    case.approve_current_reviewer("R2: pre-merge reviewer ");
    case.handle.wait_for("merged — firing MergeSucceeded");
    case.handle.wait_for("lifecycle: task #1 -> done");

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "done");
    let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
    let workers: Vec<_> = runs.iter().filter(|run| run.role == "worker").collect();
    assert_eq!(
        workers.len(),
        1,
        "Codex continuation stays within the original durable worker run"
    );
    assert!(workers.iter().all(|run| run.agent == worker
        && run.model == "o3"
        && run.provider.as_deref() == Some("codex")));
    assert_eq!(
        runs.iter()
            .filter(|run| run.role == "reviewer" && run.sub_role.is_none())
            .count(),
        2,
        "changes must require a fresh R1"
    );
    drop(conn);
    case.handle.stop();
}
