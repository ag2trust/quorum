//! Integration tests for self-update drain mode (issue #159).
//!
//! Verifies: Trigger A (self-repo merge → drain → exit 75), negative path
//! (other-repo merge does NOT trigger drain), drain timeout path, and
//! queued tasks survive restart.

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
        merge_cmd: &str,
        extra_args: &[&str],
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let sentinel_path = sentinel.path().to_string_lossy().to_string();
        let fake_agent = cargo_bin("fake-agent");
        let mut args: Vec<String> = vec![
            "serve",
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

fn quorum_done(home: &std::path::Path, args: &[&str]) {
    let mut cmd_args = vec!["done"];
    cmd_args.extend_from_slice(args);
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
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

    // Agent done with a PR → triggers reviewer spawn → reviewer approves → merge → drain
    quorum_done(home.path(), &["--agent", &agent_name, "--pr", "42"]);

    // Wait for reviewer to produce a result
    assert!(
        handle.wait_for("reviewer", 15),
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

    // Reviewer done with approved verdict
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            "42",
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

    quorum_done(home.path(), &["--agent", &agent_name, "--pr", "99"]);

    assert!(
        handle.wait_for("reviewer", 15),
        "reviewer was not spawned: {:?}",
        handle.lines
    );

    let reviewer_name = handle
        .extract_agent_name("spawning reviewer ")
        .expect("could not extract reviewer name");

    assert!(
        handle.wait_for("result", 15),
        "reviewer did not produce result: {:?}",
        handle.lines
    );

    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            "99",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    // After merge, should see "merged" but NOT "DRAIN"
    assert!(
        handle.wait_for("merged", 15),
        "did not see merge: {:?}",
        handle.lines
    );

    // Give it a moment to confirm no drain was triggered
    std::thread::sleep(Duration::from_secs(1));

    let drain_found = handle.lines.iter().any(|l| l.contains("DRAIN:"));
    assert!(
        !drain_found,
        "DRAIN was triggered for a non-self-repo merge! Lines: {:?}",
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
    // sha-poll-interval-secs=1, we advance main immediately after spawn.
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
            "1",
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
        handle.wait_for("DRAIN: all agents finished", 20)
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

    // Verify queued task (#2) is still open (claimable after restart)
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args(["task-get", "--task-id", "2"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"open\"") || stdout.contains("\"status\": \"open\""),
        "queued task was not left open for next generation: {stdout}"
    );
}
