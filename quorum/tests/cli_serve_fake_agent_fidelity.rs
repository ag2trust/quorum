//! T11: fake_agent fidelity — test that `--with-side-effects` (FAKE_AGENT_SIDE_EFFECTS)
//! produces a real `quorum submit` mailbox row, and `--emit-tool-use` (FAKE_AGENT_EMIT_TOOL_USE)
//! emits tool_use stream events that exercise the daemon's tool_count/now_label tracking.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn cargo_bin(name: &str) -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin(name)
}

fn write_names_file(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("names.txt");
    let mut f = std::fs::File::create(&path).unwrap();
    for i in 0..20 {
        writeln!(f, "Agent{i}").unwrap();
    }
    path
}

fn init_git_repo(dir: &std::path::Path) {
    let d = dir.to_string_lossy();
    Command::new("git")
        .args(["-C", &d, "init", "-b", "main"])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "config", "user.email", "test@test.com"])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "config", "user.name", "Test"])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "commit", "--allow-empty", "-m", "init"])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "remote", "add", "origin", &*d])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "fetch", "origin"])
        .status()
        .unwrap();
}

fn seed_task(home: &std::path::Path, title: &str) {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            title,
            "--created-by",
            "TestCreator",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "task-create failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

struct ServeHandle {
    child: std::process::Child,
    rx: mpsc::Receiver<String>,
    lines: Vec<String>,
    _sentinel: Option<tempfile::TempDir>,
}

impl Drop for ServeHandle {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let pid = self.child.id() as libc::pid_t;
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
            let _ = self.child.wait();
        }
    }
}

impl ServeHandle {
    fn start(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        log_dir: &std::path::Path,
        extra_env: &[(&str, &str)],
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let sentinel_path = sentinel.path().to_string_lossy().to_string();
        let fake_agent = cargo_bin("fake-agent");
        let mut cmd = Command::new(cargo_bin("quorum"));
        cmd.env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .args([
                "serve",
                "--repo",
                "test/repo",
                "--cap",
                "1",
                "--repo-dir",
                &repo.to_string_lossy(),
                "--worktree-base",
                &wt_base.to_string_lossy(),
                "--names-file",
                &names.to_string_lossy(),
                "--agent-bin",
                &fake_agent.to_string_lossy(),
                "--log-dir",
                &log_dir.to_string_lossy(),
                "--exit-when-gone",
                &sentinel_path,
            ])
            .stderr(Stdio::piped())
            .stdout(Stdio::null());
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().unwrap();

        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });

        ServeHandle {
            child,
            rx,
            lines: Vec::new(),
            _sentinel: Some(sentinel),
        }
    }

    fn wait_for(&mut self, needle: &str, timeout_secs: u64) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
        while std::time::Instant::now() < deadline {
            let remaining = deadline - std::time::Instant::now();
            match self.rx.recv_timeout(remaining) {
                Ok(line) => {
                    let found = line.contains(needle);
                    self.lines.push(line);
                    if found {
                        return true;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => return false,
                Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            }
        }
        false
    }

    fn stop(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGINT);
        }
        let _ = self.child.wait();
    }
}

/// FAKE_AGENT_SIDE_EFFECTS=1 causes the fake agent to call `quorum submit --agent <name>`
/// as a real subprocess, creating a mailbox row that the daemon then processes.
#[test]
fn side_effects_mode_creates_real_submit_mailbox_row() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Side-effects done test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        log_dir.path(),
        &[("FAKE_AGENT_SIDE_EFFECTS", "1")],
    );

    // The daemon should process the submit mailbox row from the fake agent's
    // subprocess call to `quorum submit`. The log will contain "done (pr=None"
    // because the fake agent calls submit without --pr.
    assert!(
        handle.wait_for("done (pr=None", 20),
        "daemon should have processed a done mailbox row created by fake_agent \
         side-effect subprocess. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// FAKE_AGENT_EMIT_TOOL_USE=1 causes the fake agent to emit tool_use stream events.
/// The daemon should parse them and update tool_count/now_label in the live sidecar.
#[test]
fn emit_tool_use_mode_exercises_tool_count_and_now_label() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Tool-use emission test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        log_dir.path(),
        &[("FAKE_AGENT_EMIT_TOOL_USE", "1")],
    );

    // Wait for the agent to produce its result (turn 1 completes).
    assert!(
        handle.wait_for("result", 15),
        "agent should have produced a result event. Lines: {:?}",
        handle.lines
    );

    // Give the daemon a moment to write the live sidecar.
    std::thread::sleep(Duration::from_millis(500));

    // Find the _daemon_live.json in the log directory. The session log creates
    // a subdirectory like {agent}-{timestamp}/ containing _daemon_live.json.
    let mut live_json_found = false;
    for entry in std::fs::read_dir(log_dir.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            let live_path = entry.path().join("_daemon_live.json");
            if live_path.exists() {
                let content = std::fs::read_to_string(&live_path).unwrap();
                let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
                let tools = parsed["tools"].as_u64().unwrap_or(0);
                assert!(
                    tools >= 3,
                    "tool_count should be >= 3 (Bash + Read + Grep emitted per turn), got {tools}. \
                     live json: {content}"
                );
                let now = parsed["now"].as_str().unwrap_or("");
                assert!(
                    !now.is_empty(),
                    "now_label should be set after tool_use events, got empty. live json: {content}"
                );
                live_json_found = true;
                break;
            }
        }
    }
    assert!(
        live_json_found,
        "expected _daemon_live.json in log_dir. log_dir contents: {:?}",
        std::fs::read_dir(log_dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect::<Vec<_>>()
    );

    // Also verify the stream.jsonl contains tool_use events.
    let mut stream_has_tool_use = false;
    for entry in std::fs::read_dir(log_dir.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            let stream_path = entry.path().join("stream.jsonl");
            if stream_path.exists() {
                let content = std::fs::read_to_string(&stream_path).unwrap();
                let tool_use_count = content
                    .lines()
                    .filter(|l| l.contains("\"tool_use\""))
                    .count();
                assert!(
                    tool_use_count >= 3,
                    "stream.jsonl should contain >= 3 tool_use events, found {tool_use_count}"
                );
                stream_has_tool_use = true;
                break;
            }
        }
    }
    assert!(
        stream_has_tool_use,
        "stream.jsonl should exist and contain tool_use events"
    );

    handle.stop();
}

/// FAKE_AGENT_ERROR_RESULT=1: turn 1 emits is_error=true, the daemon auto-refeeds,
/// turn 2 succeeds normally.
#[test]
fn error_result_triggers_auto_refeed() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Error refeed test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        log_dir.path(),
        &[("FAKE_AGENT_ERROR_RESULT", "1")],
    );

    assert!(
        handle.wait_for("auto-refeed worker", 20),
        "daemon should auto-refeed after error-terminated turn. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("result", 15),
        "agent should produce a second (successful) result after refeed. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// FAKE_AGENT_ERROR_RESULT=99: every turn errors. After MAX_ERROR_RETRIES (3) the
/// daemon fires AgentFailed and the task returns to open.
#[test]
fn repeated_error_results_fire_agent_failed() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    let log_dir = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Error cap test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        log_dir.path(),
        &[("FAKE_AGENT_ERROR_RESULT", "99")],
    );

    assert!(
        handle.wait_for("exhausted error retries", 30),
        "daemon should fire AgentFailed after max error retries. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}
