//! Integration tests for merge status-check waiting (#166).
//!
//! Verifies that the daemon waits for required CI checks before merging:
//! - checks pass → merge proceeds
//! - checks fail → Retryable rework with failing check names
//! - checks timeout → rework (recoverable, not terminal cancel)
//! - merge is NOT attempted while checks are pending (negative path)

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
        let mut args = vec![
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
        .collect::<Vec<_>>();
        for a in extra_args {
            args.push(a.to_string());
        }

        let mut child = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
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

/// #159: after R1 approves, wait for mandatory R2 and post R2's approval.
fn complete_r2_review(home: &std::path::Path, handle: &mut ServeHandle, pr: &str) {
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

    quorum_done(
        home,
        &[
            "--agent",
            &r2_name,
            "--pr",
            pr,
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
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

/// Checks pass immediately → merge proceeds.
#[test]
fn checks_pass_then_merge_succeeds() {
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

    seed_task(home.path(), "Task for checks-pass test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "echo ready",
            "--merge-checks-timeout-secs",
            "10",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    assert!(
        handle.wait_for("checks passed", 15),
        "checks-passed log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 15),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// Checks fail → Retryable rework with failing check names.
#[test]
fn checks_fail_sends_rework() {
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

    seed_task(home.path(), "Task for checks-fail test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "printf 'failed\\nclipper\\ntest'",
            "--merge-checks-timeout-secs",
            "10",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    assert!(
        handle.wait_for("checks failed", 15),
        "checks-failed log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework #1 (checks failure)", 15),
        "rework turn not sent. Lines: {:?}",
        handle.lines
    );

    let saw_merged = handle.lines.iter().any(|l| l.contains("merged"));
    assert!(
        !saw_merged,
        "merge should NOT happen when checks fail. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #174: Checks timeout → durable merge-wait (no rework, no merge, no
/// agent spawn, no counter changes). Task stays in merging.
#[test]
fn checks_timeout_enters_merge_wait() {
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

    seed_task(home.path(), "Task for checks-timeout merge-wait test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "echo pending",
            "--merge-checks-timeout-secs",
            "1",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    // Should see merge-wait log, NOT rework/MERGE BLOCKED.
    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.lines.iter().any(|l| l.contains("timed out")),
        "timeout reason not in merge-wait log. Lines: {:?}",
        handle.lines
    );

    // Negative: no rework, no merge, no cancel, no new agent spawn.
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT fire during merge-wait. Lines: {:?}",
        handle.lines
    );
    let saw_merged = handle
        .lines
        .iter()
        .any(|l| l.contains("merged") && !l.contains("BLOCKED"));
    assert!(
        !saw_merged,
        "merge should NOT happen during merge-wait. Lines: {:?}",
        handle.lines
    );
    let saw_cancelled = handle.lines.iter().any(|l| l.contains("cancelling"));
    assert!(
        !saw_cancelled,
        "task should NOT be cancelled during merge-wait. Lines: {:?}",
        handle.lines
    );

    // Task should still be in merging status.
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"merging\"") || stdout.contains("\"status\": \"merging\""),
        "task should stay in merging during merge-wait, got: {stdout}"
    );

    // Counters should not have changed.
    assert!(
        stdout.contains("\"rework_round\":0") || stdout.contains("\"rework_round\": 0"),
        "rework_round should not increment during merge-wait, got: {stdout}"
    );

    handle.stop();
}

/// #174: Pending → Ready → merge proceeds (merge-wait retries and succeeds).
#[test]
fn checks_pending_then_ready_merges_via_merge_wait() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let state_file = home.path().join("checks_state");
    std::fs::write(&state_file, "pending").unwrap();
    let checks_cmd = format!("cat {}", state_file.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for pending-then-ready merge-wait test");

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
            "1",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    // First timeout enters merge-wait.
    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait log not seen. Lines: {:?}",
        handle.lines
    );

    // Flip checks to ready — next tick should merge.
    std::fs::write(&state_file, "ready").unwrap();

    assert!(
        handle.wait_for("checks passed", 20),
        "checks-passed log not seen after state change. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 20),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    // No rework was triggered.
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT fire for pending→ready path. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #174: Pending → Failed → enters rework (merge-wait retries, checks fail).
#[test]
fn checks_pending_then_failed_enters_rework() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let state_file = home.path().join("checks_state");
    std::fs::write(&state_file, "pending").unwrap();
    let checks_cmd = format!("cat {}", state_file.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for pending-then-failed test");

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
            "1",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    // First timeout enters merge-wait.
    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait log not seen. Lines: {:?}",
        handle.lines
    );

    // Flip checks to failed — next tick should enter rework.
    std::fs::write(&state_file, "failed\nclipper\ntest").unwrap();

    assert!(
        handle.wait_for("checks failed", 20),
        "checks-failed log not seen after state change. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework #1 (checks failure)", 15),
        "rework not triggered after checks failure. Lines: {:?}",
        handle.lines
    );

    let saw_merged = handle.lines.iter().any(|l| l.contains("merged"));
    assert!(
        !saw_merged,
        "merge should NOT happen when checks fail. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #174: Restart test — pending before restart remains pending after restart,
/// and later Ready merges exactly once.
#[test]
fn checks_pending_survives_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let checks_state = home.path().join("checks_state");
    std::fs::write(&checks_state, "pending").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for restart merge-wait test");

    // Phase 1: start daemon, enter merge-wait.
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
            "1",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait log not seen. Lines: {:?}",
        handle.lines
    );

    // Phase 2: stop daemon. Shutdown teardown resets merging → in-review
    // via AgentFailed, but durable approvals survive.
    handle.stop();

    // Phase 3: flip checks to ready before restart so approval-recovery
    // can verify CI and merge.
    std::fs::write(&checks_state, "ready").unwrap();

    // Phase 4: restart daemon. Approval recovery detects dual approval,
    // verifies CI (now ready), and merges from durable state.
    let mut handle2 = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            &checks_cmd,
            "--merge-checks-timeout-secs",
            "10",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle2.wait_for("merged from durable approval", 30),
        "approval-recovery merge not seen after restart. Lines: {:?}",
        handle2.lines
    );

    // Verify task is done.
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"done\"") || stdout.contains("\"status\": \"done\""),
        "task should be done after restart merge, got: {stdout}"
    );

    // No rework should have been triggered.
    let saw_rework = handle2.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT fire after restart merge. Lines: {:?}",
        handle2.lines
    );

    handle2.stop();
}

/// Negative path: checks pending → transition to ready → merge proceeds.
/// Verifies merge is NOT attempted while checks are pending.
#[test]
fn checks_pending_then_ready_merges_after_wait() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let state_file = home.path().join("checks_state");
    std::fs::write(&state_file, "pending").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for pending-then-ready test");

    let checks_cmd = format!("cat {}", state_file.to_string_lossy());

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
            "15",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    assert!(
        handle.wait_for("waiting for checks", 10),
        "waiting-for-checks log not seen. Lines: {:?}",
        handle.lines
    );

    let merged_before = handle
        .lines
        .iter()
        .any(|l| l.contains("merged") || l.contains("proceeding to merge"));
    assert!(
        !merged_before,
        "merge should NOT happen while checks pending. Lines: {:?}",
        handle.lines
    );

    std::fs::write(&state_file, "ready").unwrap();

    assert!(
        handle.wait_for("checks passed", 15),
        "checks-passed log not seen after state change. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 15),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// Empty check rollup (no check runs yet for new head SHA) is treated as
/// pending, not passing. Verifies the daemon waits instead of merging
/// prematurely on a rework push with stale/absent checks.
#[test]
fn empty_checks_treated_as_pending() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    // Checks command returns "pending" (simulating empty check rollup that
    // transitions to ready after 2 polls).
    let state_file = home.path().join("checks_state");
    std::fs::write(&state_file, "pending").unwrap();
    let checks_cmd = format!("cat {}", state_file.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for empty-checks test");

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
            "15",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    // Wait long enough that the old (buggy) code would have merged immediately.
    assert!(
        handle.wait_for("waiting for checks", 10),
        "waiting-for-checks log not seen. Lines: {:?}",
        handle.lines
    );

    // Verify no merge happened while pending.
    let merged_before = handle
        .lines
        .iter()
        .any(|l| l.contains("proceeding to merge") || l.contains("merged"));
    assert!(
        !merged_before,
        "merge should NOT happen while checks are pending/empty. Lines: {:?}",
        handle.lines
    );

    // Now transition checks to ready.
    std::fs::write(&state_file, "ready").unwrap();

    assert!(
        handle.wait_for("checks passed", 15),
        "checks-passed log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 15),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// Policy-pending merge retries until checks pass then merges successfully.
/// Simulates rework-push race: first merge attempt hits "policy prohibits"
/// (checks not propagated), retry wait sees checks become ready, second
/// merge attempt succeeds.
#[test]
fn policy_pending_retries_then_merges() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let merge_state_file = home.path().join("merge_state");
    std::fs::write(&merge_state_file, "fail").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for policy-pending retry test");

    // Merge command: fails with "policy prohibits" when state is "fail",
    // succeeds when state is "pass".
    let merge_cmd = format!(
        "state=$(cat {}); if [ \"$state\" = \"pass\" ]; then echo merged; else \
         echo 'not mergeable: the base branch policy prohibits the merge' >&2; exit 1; fi",
        merge_state_file.to_string_lossy()
    );

    // Checks cmd starts pending, then transitions to ready (which also
    // flips the merge state so the retry merge succeeds).
    let checks_state_file = home.path().join("checks_state");
    std::fs::write(&checks_state_file, "pending").unwrap();

    let checks_cmd = format!("cat {}", checks_state_file.to_string_lossy());

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &merge_cmd,
        &[
            "--merge-checks-cmd",
            &checks_cmd,
            "--merge-checks-timeout-secs",
            "15",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

    // Initial checks pass (simulating stale check state from old HEAD).
    std::fs::write(&checks_state_file, "ready").unwrap();

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
    complete_r2_review(home.path(), &mut handle, "1");

    // First merge attempt hits policy-pending.
    assert!(
        handle.wait_for("policy-pending", 15),
        "policy-pending log not seen. Lines: {:?}",
        handle.lines
    );

    // Flip merge state so retry succeeds.
    std::fs::write(&merge_state_file, "pass").unwrap();

    // Should retry and succeed.
    assert!(
        handle.wait_for("merged", 20),
        "merge-success log not seen after policy-pending retry. Lines: {:?}",
        handle.lines
    );

    // No cancel, no rework.
    let saw_cancelled = handle.lines.iter().any(|l| l.contains("cancelling task"));
    assert!(
        !saw_cancelled,
        "task should NOT be cancelled on policy-pending retry success. Lines: {:?}",
        handle.lines
    );
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT be sent for policy-pending merge. Lines: {:?}",
        handle.lines
    );

    handle.stop();

    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"done\"") || stdout.contains("\"status\": \"done\""),
        "task should be done after policy-pending retry merge, got: {stdout}"
    );
}

/// #223: approved verdict with no PR number must warn and skip merge,
/// not call merge with PR #0.
#[test]
fn approved_without_pr_skips_merge() {
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

    seed_task(home.path(), "Task for missing-PR test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "echo ready",
            "--merge-checks-timeout-secs",
            "10",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

    // Reviewer signals approved WITHOUT --pr (the bug trigger).
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    assert!(
        handle.wait_for("missing PR number", 15),
        "expected missing-PR warning log. Lines: {:?}",
        handle.lines
    );

    let saw_merge_attempt = handle
        .lines
        .iter()
        .any(|l| l.contains("proceeding to merge") || l.contains("waiting for checks"));
    assert!(
        !saw_merge_attempt,
        "merge should NOT be attempted without PR number. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #153: PR is mergeable at initial check, becomes conflicting during the
/// checks wait, and checks time out. Task must transition to rework (not
/// cancelled), and the worker must receive a rework turn with conflict
/// resolution instructions.
#[test]
fn conflict_during_checks_wait_triggers_rework_not_cancel() {
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

    seed_task(home.path(), "Task for conflict-during-checks test");

    // mergeability-cmd: returns "mergeable" for the first N calls (Phase 5
    // worker-needs-reviewer check + pre-merge check), then "conflicting"
    // on the post-timeout recheck. Uses a counter file.
    let counter_file = home.path().join("mergeability_counter");
    std::fs::write(&counter_file, "0").unwrap();
    let mergeability_script = format!(
        "n=$(cat {f}); n=$((n + 1)); echo $n > {f}; \
         if [ $n -le 2 ]; then echo mergeable; else echo conflicting; fi",
        f = counter_file.display()
    );

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "echo pending",
            "--merge-checks-timeout-secs",
            "1",
            "--merge-checks-poll-secs",
            "1",
            "--merge-mergeability-cmd",
            &mergeability_script,
        ],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    // Should see the conflict detected after timeout, NOT a cancel.
    assert!(
        handle.wait_for("CONFLICTING during checks", 20),
        "conflict-during-checks log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework", 15),
        "rework not triggered for conflict during checks. Lines: {:?}",
        handle.lines
    );

    let saw_cancelled = handle.lines.iter().any(|l| l.contains("cancelling"));
    assert!(
        !saw_cancelled,
        "task should NOT be cancelled for conflict during checks. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #181: No CI configured (no checks_cmd) → merge proceeds immediately
/// without entering merge-wait or timeout.
#[test]
fn no_ci_checks_merges_immediately() {
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

    seed_task(home.path(), "Task for no-CI-checks test");

    // No --merge-checks-cmd: CommandMergeExecutor returns Ready immediately.
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

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
    complete_r2_review(home.path(), &mut handle, "1");

    assert!(
        handle.wait_for("merged", 15),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    // Negative: no merge-wait, no timeout, no rework.
    let saw_merge_wait = handle.lines.iter().any(|l| l.contains("merge wait"));
    assert!(
        !saw_merge_wait,
        "merge-wait should NOT occur with no CI checks. Lines: {:?}",
        handle.lines
    );
    let saw_timeout = handle.lines.iter().any(|l| l.contains("timed out"));
    assert!(
        !saw_timeout,
        "timeout should NOT occur with no CI checks. Lines: {:?}",
        handle.lines
    );
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT occur with no CI checks. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}
