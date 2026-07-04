//! M2/M3 tests: reviewer spawn + verdict loop + daemon-owned merge.
//!
//! Scenarios using fake-agent:
//! 1. approve flow: worker done → reviewer spawns → approved → daemon merges → both torn down
//! 2. changes flow: worker done → reviewer spawns → changes → reviewer killed,
//!    worker re-fed same PID (warm rework)
//! 3. merge failure: approved verdict but merge fails → treated as changes,
//!    worker gets rework turn with merge error

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
}

impl ServeHandle {
    fn start(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
    ) -> Self {
        Self::start_with_merge(home, repo, wt_base, names, "true")
    }

    fn start_with_merge(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        merge_cmd: &str,
    ) -> Self {
        let fake_agent = cargo_bin("fake-agent");
        let mut child = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home)
            .args([
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
            ])
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

#[test]
fn approve_flow_tears_down_both_agents() {
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

    seed_task(home.path(), "Task for approve flow");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Wait for worker to spawn and produce a result
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

    // Worker signals "done with PR" — triggers reviewer spawn
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Wait for reviewer to spawn
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );

    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    // Wait for reviewer result
    assert!(
        handle.wait_for("reviewer", 15),
        "reviewer activity not seen. Lines: {:?}",
        handle.lines
    );
    // Give a moment for draining to complete
    std::thread::sleep(Duration::from_secs(1));

    // Reviewer signals approved verdict
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
        ],
    );

    // Daemon should merge, then tear down both
    assert!(
        handle.wait_for("merged", 15),
        "merge log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("tearing down worker", 15),
        "worker teardown not seen. Lines: {:?}",
        handle.lines
    );

    // Verify task is closed (done → review auto-resolved → closed, #162)
    std::thread::sleep(Duration::from_millis(500));
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"closed\"") || stdout.contains("\"status\": \"closed\""),
        "task not closed after in-cycle merge + auto-resolve: {stdout}"
    );

    handle.stop();
}

#[test]
fn changes_verdict_feeds_rework_to_same_warm_worker() {
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

    seed_task(home.path(), "Task for rework flow");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Wait for worker to spawn and produce a result
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

    // Worker signals "done with PR"
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Wait for reviewer to spawn
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );

    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    // Wait for reviewer to finish its turn
    std::thread::sleep(Duration::from_secs(2));

    // Reviewer signals changes verdict with feedback
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--feedback",
            "Fix the error handling in main.rs",
        ],
    );

    // Worker should get rework — wait for the fake-agent to respond
    assert!(
        handle.wait_for("rework", 15),
        "rework not seen. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("Fixing", 15),
        "worker rework response not seen. Lines: {:?}",
        handle.lines
    );

    // Drain remaining lines
    std::thread::sleep(Duration::from_millis(500));
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    // ── State assertions (F12) ──

    // Invariant: same warm worker — exactly 1 worker spawn, 0 worker teardowns
    let worker_spawns = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent"))
        .count();
    assert_eq!(
        worker_spawns, 1,
        "expected exactly 1 worker spawn (warm rework, no re-spawn), got {worker_spawns}. Lines: {:?}",
        handle.lines
    );

    let worker_teardowns = handle
        .lines
        .iter()
        .filter(|l| l.contains("tearing down worker"))
        .count();
    assert_eq!(
        worker_teardowns, 0,
        "worker should not be torn down during rework. Lines: {:?}",
        handle.lines
    );

    // Reviewer must be torn down
    let reviewer_teardowns = handle
        .lines
        .iter()
        .filter(|l| l.contains("tearing down reviewer"))
        .count();
    assert_eq!(
        reviewer_teardowns, 1,
        "reviewer should be torn down after changes verdict. Lines: {:?}",
        handle.lines
    );

    // Task status: must still be claimed by the same worker (not done, not open)
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    let task: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        task["status"].as_str(),
        Some("claimed"),
        "task must remain claimed during rework, got: {stdout}"
    );
    assert_eq!(
        task["assignee"].as_str(),
        Some(worker_name.as_str()),
        "task must remain assigned to the same worker during rework, got: {stdout}"
    );

    handle.stop();
}

#[test]
fn merge_failure_feeds_rework_to_worker() {
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

    seed_task(home.path(), "Task for merge failure flow");

    // Use a merge command that always fails
    let mut handle = ServeHandle::start_with_merge(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "echo 'merge conflict: base branch was updated' >&2 && exit 1",
    );

    // Wait for worker to spawn and produce a result
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

    // Worker signals "done with PR"
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Wait for reviewer to spawn
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );

    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    // Wait for reviewer to finish its turn
    std::thread::sleep(Duration::from_secs(2));

    // Reviewer signals approved verdict — but merge will fail
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
        ],
    );

    // Merge should fail and daemon should log the failure
    assert!(
        handle.wait_for("merge failed", 15),
        "merge failure not logged. Lines: {:?}",
        handle.lines
    );

    // Worker should get rework turn (merge failure is treated as changes)
    assert!(
        handle.wait_for("rework", 15),
        "rework not seen after merge failure. Lines: {:?}",
        handle.lines
    );

    // The reviewer should be torn down
    let saw_reviewer_teardown = handle
        .lines
        .iter()
        .any(|l| l.contains("tearing down reviewer"));
    assert!(
        saw_reviewer_teardown,
        "reviewer teardown not seen. Lines: {:?}",
        handle.lines
    );

    // Task should NOT be done (merge failed, worker is reworking)
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        !stdout.contains("\"status\":\"done\"") && !stdout.contains("\"status\": \"done\""),
        "task should not be done after merge failure: {stdout}"
    );

    handle.stop();
}

#[test]
fn no_verdict_done_clears_pr_no_respawn_loop() {
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

    seed_task(home.path(), "Task for no-verdict flow");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

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

    // Worker signals "done with PR" — triggers reviewer spawn
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );

    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    // Wait for reviewer to produce its result
    std::thread::sleep(Duration::from_secs(2));

    // Reviewer signals done WITHOUT a verdict — the `_ =>` branch
    quorum_done(home.path(), &["--agent", &reviewer_name, "--pr", "1"]);

    // Daemon should tear down the reviewer and clear w.pr
    assert!(
        handle.wait_for("clearing PR", 15),
        "clearing PR log not seen. Lines: {:?}",
        handle.lines
    );

    let saw_reviewer_teardown = handle
        .lines
        .iter()
        .any(|l| l.contains("tearing down reviewer"));
    assert!(
        saw_reviewer_teardown,
        "reviewer teardown not seen. Lines: {:?}",
        handle.lines
    );

    // Wait 3 seconds (6+ ticks) — if w.pr was NOT cleared, a second reviewer
    // would spawn within one or two ticks.
    std::thread::sleep(Duration::from_secs(3));

    // Drain any remaining log lines
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    // Count reviewer spawn messages — should be exactly 1
    let spawn_count = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning reviewer"))
        .count();
    assert_eq!(
        spawn_count, 1,
        "expected exactly 1 reviewer spawn (no respawn loop), got {spawn_count}. Lines: {:?}",
        handle.lines
    );

    // Worker should still be alive (not torn down)
    let worker_teardowns = handle
        .lines
        .iter()
        .filter(|l| l.contains("tearing down worker"))
        .count();
    assert_eq!(
        worker_teardowns, 0,
        "worker should not be torn down on no-verdict. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #180: reviewer provision must resolve refs.repo via --repo-dir-map instead
/// of always fetching from the daemon's base --repo-dir. When a task's refs.repo
/// maps to a different directory, the reviewer worktree must be provisioned from
/// that directory — otherwise `git fetch origin <branch>` fails because the branch
/// doesn't exist on the default repo's remote.
#[test]
fn reviewer_provision_uses_repo_dir_map_for_cross_repo_task() {
    let home = tempfile::tempdir().unwrap();
    // Two separate repos: daemon base repo and the task's target repo.
    let daemon_repo = tempfile::tempdir().unwrap();
    let task_repo = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(daemon_repo.path());
    init_git_repo(task_repo.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .status()
        .unwrap();

    // Create task with refs.repo pointing to "test/cross-repo"
    seed_task_with_refs(
        home.path(),
        "Cross-repo task",
        r#"{"repo":"test/cross-repo"}"#,
    );

    // Start daemon with --repo-dir-map mapping "test/cross-repo" to task_repo's path
    let fake_agent = cargo_bin("fake-agent");
    let repo_dir_map_val = format!("test/cross-repo={}", task_repo.path().display());
    let mut child = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args([
            "serve",
            "--cap",
            "1",
            "--repo-dir",
            &daemon_repo.path().to_string_lossy(),
            "--worktree-base",
            &wt_base.path().to_string_lossy(),
            "--names-file",
            &names_file.to_string_lossy(),
            "--agent-bin",
            &fake_agent.to_string_lossy(),
            "--merge-cmd",
            "true",
            "--repo-dir-map",
            &repo_dir_map_val,
        ])
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
    let mut lines: Vec<String> = Vec::new();

    let wait_for = |lines: &mut Vec<String>, rx: &mpsc::Receiver<String>, needle: &str| -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            let remaining = deadline - std::time::Instant::now();
            match rx.recv_timeout(remaining) {
                Ok(line) => {
                    let found = line.contains(needle);
                    lines.push(line);
                    if found {
                        return true;
                    }
                }
                Err(_) => return false,
            }
        }
        false
    };

    // Wait for worker to spawn (should provision from task_repo, not daemon_repo)
    assert!(
        wait_for(&mut lines, &rx, "spawning agent"),
        "worker not spawned. Lines: {:?}",
        lines
    );
    assert!(
        wait_for(&mut lines, &rx, "result"),
        "worker result not seen. Lines: {:?}",
        lines
    );

    // Extract worker name
    let worker_name = lines
        .iter()
        .find_map(|l| l.split("spawning agent ").nth(1))
        .map(|rest| rest.split_whitespace().next().unwrap_or("").to_string())
        .unwrap();

    // The worker's branch was created inside task_repo. Signal done with PR.
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Reviewer must provision its worktree from task_repo (where the branch exists).
    // If it tries daemon_repo, the fetch will fail and we'd see provision strikes.
    assert!(
        wait_for(&mut lines, &rx, "reviewer worktree provisioned"),
        "reviewer worktree not provisioned — likely failed due to wrong repo dir. Lines: {:?}",
        lines
    );

    // Verify: no provision failure messages
    let provision_failures = lines
        .iter()
        .filter(|l| l.contains("reviewer worktree provision failed"))
        .count();
    assert_eq!(
        provision_failures, 0,
        "reviewer provision should not fail with correct repo-dir-map. Lines: {:?}",
        lines
    );

    // Clean up
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGINT);
    }
    let _ = child.wait();
}
