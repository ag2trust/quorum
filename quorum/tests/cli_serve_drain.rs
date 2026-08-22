//! Integration tests for self-update drain mode (issue #159).
//!
//! Verifies: Trigger A (self-repo merge → drain → exit 75), negative path
//! (other-repo merge does NOT trigger drain), drain timeout path,
//! queued tasks survive restart, and T3: drain timeout with merge-in-progress.

use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn cargo_bin(name: &str) -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin(name)
}

fn agent_endpoint(home: &std::path::Path) -> std::path::PathBuf {
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let mut hasher = DefaultHasher::new();
    db.hash(&mut hasher);
    std::env::temp_dir()
        .join(format!("quorum-agent-{:016x}", hasher.finish()))
        .join("endpoint.sock")
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
    // Align the fixture's initial origin/main with the executable under test.
    // The daemon now compares origin directly to its embedded build SHA rather
    // than taking a startup baseline, so a fresh fixture must represent a
    // current build before a test advances it.
    let source_repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("quorum crate has repository parent");
    Command::new("git")
        .args([
            "-C",
            &d,
            "fetch",
            "--update-shallow",
            &source_repo.to_string_lossy(),
            "HEAD",
        ])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "reset", "--hard", "FETCH_HEAD"])
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

struct ServeHandle {
    child: std::process::Child,
    rx: mpsc::Receiver<String>,
    lines: Vec<String>,
    _sentinel: Option<tempfile::TempDir>,
    _gh_shim: Option<tempfile::TempDir>,
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
        merge_cmd: &str,
        extra_args: &[&str],
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let sentinel_path = sentinel.path().to_string_lossy().to_string();
        let gh_shim = tempfile::tempdir().unwrap();
        let gh_state = gh_shim.path().join("state");
        std::fs::create_dir_all(&gh_state).unwrap();
        let gh_path = gh_shim.path().join("gh");
        std::fs::write(
            &gh_path,
            r#"#!/bin/sh
set -eu
cmd="${1:-} ${2:-}"
if [ "$cmd" = "pr create" ]; then
  shift 2
  head=""
  while [ "$#" -gt 0 ]; do
    if [ "$1" = "--head" ]; then head="$2"; shift 2; else shift; fi
  done
  pr="${head##*-t}"
  printf '%s' "$head" > "$QUORUM_TEST_GH_STATE/$pr"
  printf 'https://github.com/test/repo/pull/%s\n' "$pr"
elif [ "$cmd" = "pr list" ]; then
  shift 2
  head=""
  while [ "$#" -gt 0 ]; do
    if [ "$1" = "--head" ]; then head="$2"; shift 2; else shift; fi
  done
  pr="${head##*-t}"
  if [ -f "$QUORUM_TEST_GH_STATE/$pr" ]; then
    printf '[{"number":%s,"state":"OPEN"}]\n' "$pr"
  else
    printf '[]\n'
  fi
elif [ "$cmd" = "pr view" ]; then
  pr="$3"
  branch="$(cat "$QUORUM_TEST_GH_STATE/$pr")"
  sha="$(git -C "$QUORUM_TEST_REPO" rev-parse "refs/heads/$branch")"
  printf '{"headRefName":"%s","headRefOid":"%s","isCrossRepository":false,"baseRefName":"main","state":"OPEN"}\n' "$branch" "$sha"
else
  printf 'unsupported gh invocation: %s\n' "$*" >&2
  exit 1
fi
"#,
        )
        .unwrap();
        std::fs::set_permissions(&gh_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = format!(
            "{}:{}",
            gh_shim.path().display(),
            env::var("PATH").unwrap_or_default()
        );
        let fake_agent = cargo_bin("fake-agent");
        let mut args: Vec<String> = vec![
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
            "--merge-cmd",
            merge_cmd,
            "--exit-when-gone",
            &sentinel_path,
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();
        for a in extra_args {
            args.push(a.to_string());
        }

        let mut child = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .env("PATH", path)
            .env("QUORUM_TEST_GH_STATE", &gh_state)
            .env("QUORUM_TEST_REPO", repo)
            .args(&args)
            .stderr(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();

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
            _gh_shim: Some(gh_shim),
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

    fn wait_exit(&mut self, timeout_secs: u64) -> Option<std::process::ExitStatus> {
        let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            match self.child.try_wait().unwrap() {
                Some(status) => return Some(status),
                None => {
                    if std::time::Instant::now() > deadline {
                        self.child.kill().ok();
                        return self.child.wait().ok();
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn extract_agent_name(&self, prefix: &str) -> Option<String> {
        for line in &self.lines {
            if let Some(rest) = line.split(prefix).nth(1) {
                return Some(rest.split_whitespace().next().unwrap_or("").to_string());
            }
        }
        None
    }

    fn stop(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGINT);
        }
        let _ = self.child.wait();
    }
}

fn seed_task_with_refs(home: &std::path::Path, title: &str, refs: &str) {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            title,
            "--created-by",
            "TestCreator",
            "--refs",
            refs,
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "task-create failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn task_json(home: &std::path::Path, task_id: i64) -> serde_json::Value {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", &task_id.to_string()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "task-get #{task_id} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

fn resolve_run_id(home: &std::path::Path, agent: &str, role: &str) -> String {
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let mut conn = quorum_core::db::open(&db).unwrap();
    match quorum_core::capabilities::active_for_agent(&conn, agent).unwrap() {
        Some(cap) => cap.run_id,
        None => {
            let rid = format!("test-{agent}-{}", std::process::id());
            quorum_core::capabilities::issue(&mut conn, &rid, 0, agent, role, 1000).unwrap();
            rid
        }
    }
}

fn quorum_done(home: &std::path::Path, args: &[&str]) {
    let agent = args
        .iter()
        .zip(args.iter().skip(1))
        .find(|(k, _)| **k == "--agent")
        .map(|(_, v)| *v)
        .expect("quorum_done requires --agent");
    let role = if args.contains(&"--verdict") {
        "reviewer"
    } else {
        "worker"
    };
    let run_id = resolve_run_id(home, agent, role);
    let mut cmd_args = vec!["done"];
    cmd_args.extend_from_slice(args);
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_AGENT_ENDPOINT", agent_endpoint(home))
        .env("QUORUM_RUN_ID", &run_id)
        .args(&cmd_args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "done failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Self-repo merge triggers drain → roster empties → exit 75.
#[test]
fn self_repo_merge_drains_and_exits_75() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task_with_refs(
        home.path(),
        "Self-repo task",
        r#"{"repo":"test-owner/test-repo"}"#,
    );

    // merge-cmd: "true" — always succeeds
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--self-update-drain",
            "--self-repo",
            "test-owner/test-repo",
            "--drain-timeout-secs",
            "10",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "did not spawn agent: {:?}",
        handle.lines
    );

    let agent_name = handle
        .extract_agent_name("spawning agent ")
        .expect("could not extract agent name");

    assert!(
        handle.wait_for("result", 15),
        "agent did not produce result: {:?}",
        handle.lines
    );

    // Agent commits and signals; the daemon publishes and creates the PR.
    quorum_done(home.path(), &["--agent", &agent_name]);

    // Wait for reviewer to be spawned
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer was not spawned: {:?}",
        handle.lines
    );

    // Get reviewer name from "spawning reviewer" line
    let reviewer_name = handle
        .extract_agent_name("spawning reviewer ")
        .expect("could not extract reviewer name");

    assert!(
        handle.wait_for("result", 15),
        "reviewer did not produce result: {:?}",
        handle.lines
    );

    // R1 reviewer done with approved verdict — triggers mandatory R2.
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    // #159: mandatory dual review — wait for R2 reviewer.
    assert!(
        handle.wait_for("R2: pre-merge reviewer", 15),
        "R2 reviewer was not spawned: {:?}",
        handle.lines
    );
    let r2_name = handle
        .extract_agent_name("R2: pre-merge reviewer ")
        .expect("could not extract R2 reviewer name");
    // R2 name has trailing " spawned..." — take first word.
    let r2_name = r2_name.split_whitespace().next().unwrap().to_string();

    assert!(
        handle.wait_for("result", 15),
        "R2 reviewer did not produce result: {:?}",
        handle.lines
    );

    // R2 reviewer approves — completes dual review.
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r2_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    // Should see drain log and exit 75
    assert!(
        handle.wait_for("DRAIN:", 15),
        "did not see DRAIN log: {:?}",
        handle.lines
    );

    let status = handle
        .wait_exit(10)
        .expect("serve did not exit after drain");

    assert_eq!(
        status.code(),
        Some(75),
        "expected exit 75, got {:?}. Lines: {:?}",
        status.code(),
        handle.lines
    );
}

/// Non-self-repo merge does NOT trigger drain.
#[test]
fn other_repo_merge_does_not_drain() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    // Task refs point to a DIFFERENT repo
    seed_task_with_refs(
        home.path(),
        "Other-repo task",
        r#"{"repo":"other-owner/other-repo"}"#,
    );

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--self-update-drain",
            "--self-repo",
            "test-owner/test-repo",
            "--drain-timeout-secs",
            "5",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "did not spawn agent: {:?}",
        handle.lines
    );

    let agent_name = handle
        .extract_agent_name("spawning agent ")
        .expect("could not extract agent name");

    assert!(
        handle.wait_for("result", 15),
        "agent did not produce result: {:?}",
        handle.lines
    );

    quorum_done(home.path(), &["--agent", &agent_name]);

    // #75: cross-repo tasks are detected and parked immediately — no reviewer
    // spawn, no drain.
    assert!(
        handle.wait_for("REPO MISMATCH", 15),
        "did not see repo mismatch detection: {:?}",
        handle.lines
    );

    // Wait 2 ticks to confirm no drain was triggered
    std::thread::sleep(Duration::from_millis(600));

    let drain_found = handle.lines.iter().any(|l| l.contains("DRAIN:"));
    assert!(
        !drain_found,
        "DRAIN was triggered for a non-self-repo merge! Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// Trigger B treats an origin matching the running build as current.
#[test]
fn build_sha_matching_origin_does_not_drain() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &["--self-update-drain"],
    );

    assert!(
        handle.wait_for("decision=Current", 15),
        "did not observe current build-staleness decision: {:?}",
        handle.lines
    );
    assert!(
        handle.child.try_wait().unwrap().is_none(),
        "current build SHA must not stop the daemon: {:?}",
        handle.lines
    );
    assert!(
        !handle.lines.iter().any(|line| line.contains("DRAIN:")),
        "current build SHA must not start a drain: {:?}",
        handle.lines
    );
    handle.stop();
}

/// Trigger B's idle path: the daemon starts at build SHA A, origin/main
/// advances to B, then the empty roster completes the self-update drain.
/// The exit status, rather than a log line, is the supervisor handoff contract.
#[test]
fn build_sha_advance_drains_and_exits_75() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &["--self-update-drain", "--sha-poll-interval-secs", "30"],
    );

    assert!(
        handle.wait_for("decision=Current", 15),
        "did not observe initial current build SHA: {:?}",
        handle.lines
    );

    let repo = repo_dir.path().to_string_lossy().to_string();
    Command::new("git")
        .args(["-C", &repo, "commit", "--allow-empty", "-m", "advance main"])
        .status()
        .unwrap();

    assert!(
        handle.wait_for("decision=Behind", 45),
        "did not observe build SHA advancement: {:?}",
        handle.lines
    );
    let status = handle
        .wait_exit(10)
        .expect("serve did not exit after self-update drain");
    assert_eq!(
        status.code(),
        Some(75),
        "self-update drain must hand off to the supervisor with exit 75; lines: {:?}",
        handle.lines
    );
}

/// A failed staleness check is explicitly non-fatal so offline supervisors do
/// not enter a restart loop.
#[test]
fn unreachable_origin_logs_warning_and_daemon_keeps_running() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let d = repo_dir.path().to_string_lossy().to_string();
    Command::new("git")
        .args(["-C", &d, "remote", "set-url", "origin", "/does/not/exist"])
        .status()
        .unwrap();
    let names_file = write_names_file(home.path());
    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &["--self-update-drain"],
    );

    assert!(
        handle.wait_for("WARN: self-update-drain build staleness check failed", 15),
        "did not observe staleness warning: {:?}",
        handle.lines
    );
    assert!(
        handle.child.try_wait().unwrap().is_none(),
        "unreachable origin must not stop the daemon: {:?}",
        handle.lines
    );
    handle.stop();
}

/// Drain timeout force-kills remaining agents and still exits 75.
/// Uses Trigger B (sha change) to start drain while agent is mid-turn.
#[test]
fn drain_timeout_force_kills_and_exits_75() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task_with_refs(
        home.path(),
        "Long running task",
        r#"{"repo":"test-owner/test-repo"}"#,
    );
    seed_task_with_refs(
        home.path(),
        "Second task queued",
        r#"{"repo":"test-owner/test-repo"}"#,
    );

    // fake-agent completes its turn quickly, but we won't send `done`, so the agent
    // stays alive between turns. However the slot's `draining` becomes false after result.
    // The Phase 4a-drain logic will tear down idle agents during drain. To test the
    // timeout path, we need an agent that's still mid-turn (draining=true on the slot).
    //
    // The fake-agent auto-completes its turn, so we rely on timing: if drain triggers
    // BEFORE the result event drains, the slot is still draining=true. With
    // sha-poll-interval-secs=30, so the next build-SHA check can take up to
    // one configured interval after we advance main.
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--self-update-drain",
            "--self-repo",
            "test-owner/test-repo",
            "--drain-timeout-secs",
            "3",
            "--sha-poll-interval-secs",
            "30",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "did not spawn agent: {:?}",
        handle.lines
    );

    // Advance origin/main to trigger Trigger B
    let d = repo_dir.path().to_string_lossy().to_string();
    Command::new("git")
        .args(["-C", &d, "commit", "--allow-empty", "-m", "advance main"])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "fetch", "origin"])
        .status()
        .unwrap();

    // The daemon should either:
    // (a) drain the idle agent immediately via Phase 4a-drain, or
    // (b) timeout after 3s if the agent is still mid-turn
    // Either way, it exits 75.
    assert!(
        handle.wait_for("DRAIN: all agents finished", 45)
            || handle.wait_for("DRAIN: exiting 75", 5),
        "did not see drain exit log: {:?}",
        handle.lines
    );

    let status = handle
        .wait_exit(10)
        .expect("serve did not exit after drain");

    assert_eq!(
        status.code(),
        Some(75),
        "expected exit 75, got {:?}. Lines: {:?}",
        status.code(),
        handle.lines
    );
    assert!(
        handle
            .lines
            .iter()
            .any(|line| line.contains("decision=Behind")),
        "advanced origin/main did not record a behind decision: {:?}",
        handle.lines
    );

    // State evidence: once the behind decision is made, task #2 was neither
    // claimed nor spawned. It remains wholly available to the next daemon.
    let queued = task_json(home.path(), 2);
    assert_eq!(queued["status"], "open");
    assert!(queued["assignee"].is_null());
    assert_eq!(queued["agent_runs"].as_array().unwrap().len(), 0);

    // The in-flight task remains durable and recoverable after the bounded
    // drain; the daemon must not silently lose it while stopping.
    let in_flight = task_json(home.path(), 1);
    assert_eq!(in_flight["status"], "open");
    assert!(!in_flight["agent_runs"].as_array().unwrap().is_empty());
}

/// T3 regression: drain timeout must be honored even when tick() is blocked
/// inside wait_for_checks. With drain_timeout_secs=2 and
/// merge_checks_timeout_secs=30, the daemon must exit within the drain window,
/// not after the merge-checks timeout.
#[test]
fn drain_timeout_honored_during_merge_checks() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("drain_merge_checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task_with_refs(
        home.path(),
        "Task with long merge checks",
        r#"{"repo":"test-owner/test-repo"}"#,
    );

    // CI starts green so R1 and R2 can run. Before R2 submits, the test flips
    // it to pending so the merge-time wait blocks for up to 30s.
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--self-update-drain",
            "--self-repo",
            "test-owner/test-repo",
            "--drain-timeout-secs",
            "2",
            "--merge-checks-cmd",
            &checks_cmd,
            "--merge-checks-timeout-secs",
            "30",
            "--merge-checks-poll-secs",
            "30",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned: {:?}",
        handle.lines
    );

    let agent_name = handle
        .extract_agent_name("spawning agent ")
        .expect("could not extract agent name");

    assert!(
        handle.wait_for("result", 15),
        "worker result not seen: {:?}",
        handle.lines
    );

    quorum_done(home.path(), &["--agent", &agent_name]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned: {:?}",
        handle.lines
    );

    let reviewer_name = handle
        .extract_agent_name("spawning reviewer ")
        .expect("could not extract reviewer name");

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen: {:?}",
        handle.lines
    );

    // R1 approves → triggers mandatory R2 (#159).
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    // Wait for R2 reviewer.
    assert!(
        handle.wait_for("R2: pre-merge reviewer", 15),
        "R2 reviewer was not spawned: {:?}",
        handle.lines
    );
    let r2_name = handle
        .extract_agent_name("R2: pre-merge reviewer ")
        .expect("could not extract R2 reviewer name");
    let r2_name = r2_name.split_whitespace().next().unwrap().to_string();

    assert!(
        handle.wait_for("result", 15),
        "R2 reviewer did not produce result: {:?}",
        handle.lines
    );

    // R2 approves → daemon enters wait_for_checks (blocks up to 30s).
    std::fs::write(&checks_state, "pending").unwrap();
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r2_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    assert!(
        handle.wait_for("waiting for checks", 15),
        "daemon did not enter merge-checks wait: {:?}",
        handle.lines
    );

    // Now trigger drain via SIGINT while wait_for_checks is blocking tick().
    let drain_start = std::time::Instant::now();
    unsafe {
        libc::kill(handle.child.id() as libc::pid_t, libc::SIGINT);
    }

    // The daemon must exit within drain_timeout (2s) + margin (6s for tick
    // latency and teardown). If wait_for_checks blocks the drain check, it
    // won't exit for ~30s → test fails.
    let status = handle
        .wait_exit(8)
        .expect("daemon did not exit within 8s (drain timeout is 2s)");

    // Drain remaining stderr for diagnostics
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    let elapsed = drain_start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "daemon took {elapsed:?} to exit — drain_timeout_secs=2 was violated \
         (wait_for_checks blocked the tick loop). All lines: {:?}",
        handle.lines
    );

    assert_eq!(
        status.code(),
        Some(0),
        "expected exit 0 (signal drain), got {:?}. Lines: {:?}",
        status.code(),
        handle.lines
    );
}

/// Negative path: when no drain is active, pending checks that time out should
/// follow the normal rework path (MergeFailed + VerdictChanges) — the merge
/// must not be lost or silently swallowed.
#[test]
fn pending_checks_timeout_without_drain_enters_merge_wait() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("pending_merge_checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task_with_refs(
        home.path(),
        "Task with short checks timeout",
        r#"{"repo":"test/repo"}"#,
    );

    // CI starts green so R1 and R2 can run, then becomes pending before the
    // merge-time check wait and times out after 3s.
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            &checks_cmd,
            "--merge-checks-timeout-secs",
            "3",
            "--merge-checks-poll-secs",
            "30",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned: {:?}",
        handle.lines
    );

    let agent_name = handle
        .extract_agent_name("spawning agent ")
        .expect("could not extract agent name");

    assert!(
        handle.wait_for("result", 15),
        "worker result not seen: {:?}",
        handle.lines
    );

    quorum_done(home.path(), &["--agent", &agent_name]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned: {:?}",
        handle.lines
    );

    let reviewer_name = handle
        .extract_agent_name("spawning reviewer ")
        .expect("could not extract reviewer name");

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen: {:?}",
        handle.lines
    );

    // R1 approves → triggers mandatory R2 (#159).
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    // Wait for R2 reviewer.
    assert!(
        handle.wait_for("R2: pre-merge reviewer", 15),
        "R2 reviewer was not spawned: {:?}",
        handle.lines
    );
    let r2_name = handle
        .extract_agent_name("R2: pre-merge reviewer ")
        .expect("could not extract R2 reviewer name");
    let r2_name = r2_name.split_whitespace().next().unwrap().to_string();

    assert!(
        handle.wait_for("result", 15),
        "R2 reviewer did not produce result: {:?}",
        handle.lines
    );

    // R2 approves → daemon enters wait_for_checks (times out after 3s).
    std::fs::write(&checks_state, "pending").unwrap();
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r2_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    assert!(
        handle.wait_for("waiting for checks", 15),
        "daemon did not enter merge-checks wait: {:?}",
        handle.lines
    );

    // No drain signal — let the timeout expire naturally.
    // #174: checks timeout → durable merge-wait (not rework).
    assert!(
        handle.wait_for("merge wait", 20),
        "checks did not time out and enter merge-wait as expected: {:?}",
        handle.lines
    );

    // No rework should be triggered.
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT fire for pending checks timeout (#174): {:?}",
        handle.lines
    );

    handle.stop();
}
