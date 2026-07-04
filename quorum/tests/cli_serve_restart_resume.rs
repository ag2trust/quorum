//! #178: daemon restart resumes done-awaiting-review at the review stage.
//!
//! Before this fix, a daemon that restarted while workers were awaiting review
//! (PR delivered, reviewer not yet spawned) would revert the tasks to `open`
//! and let fresh workers re-execute them from scratch — producing duplicate
//! PRs for already-delivered work. The acceptance test lives here.
//!
//! Scenarios:
//! 1. hard kill (SIGKILL) after `done --pr`: restart provisions a reviewer
//!    for the recorded PR and does NOT spawn a new worker for the task.
//! 2. drain-mode shutdown after `done --pr`: same guarantee across a clean
//!    exit path.

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
        .args(["-C", &d, "remote", "add", "origin", &d])
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
                "true",
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

    fn any_line_contains(&self, needle: &str) -> bool {
        self.lines.iter().any(|l| l.contains(needle))
    }

    fn extract_agent_name(&self, prefix: &str) -> Option<String> {
        for line in &self.lines {
            if let Some(rest) = line.split(prefix).nth(1) {
                return Some(rest.split_whitespace().next().unwrap_or("").to_string());
            }
        }
        None
    }

    fn hard_kill(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL);
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

fn task_status(home: &std::path::Path, task_id: i64) -> String {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .args(["task-get", "--task-id", &task_id.to_string()])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    for prefix in ["\"status\":\"", "\"status\": \""] {
        if let Some(rest) = stdout.split(prefix).nth(1) {
            if let Some(v) = rest.split('"').next() {
                return v.to_string();
            }
        }
    }
    String::new()
}

/// #178 acceptance test: worker signals done --pr; kill daemon; relaunch;
/// assert NO new worker spawns for that task and a reviewer is provisioned
/// against the recorded PR.
#[test]
fn hard_kill_after_done_pr_restart_resumes_at_review_stage() {
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

    seed_task(home.path(), "Task for restart-resume");

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
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "42"]);

    // Wait for the daemon to process the done --pr row and mark the worker
    // as awaiting review (before it spawns a reviewer that we then race).
    assert!(
        handle.wait_for("ready for review", 15),
        "PR ready-for-review not seen. Lines: {:?}",
        handle.lines
    );

    // Hard-kill to simulate a crash — journal preserved, no drain cleanup.
    handle.hard_kill();

    // Confirm task is still "claimed" — the delivered PR state must survive.
    let status_before = task_status(home.path(), 1);
    assert_eq!(
        status_before, "claimed",
        "task lost claimed state before restart (was {status_before})"
    );

    // Relaunch daemon.
    let mut handle2 = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Recovery must classify the entry as an awaiting-review resume.
    assert!(
        handle2.wait_for("resuming at review stage", 20),
        "restart did not resume at review stage. Lines: {:?}",
        handle2.lines
    );

    // Reviewer must be provisioned for the recorded PR (42).
    assert!(
        handle2.wait_for("spawning reviewer", 30),
        "reviewer not spawned after restart. Lines: {:?}",
        handle2.lines
    );
    assert!(
        handle2.any_line_contains("PR #42"),
        "reviewer not tied to recorded PR #42. Lines: {:?}",
        handle2.lines
    );

    // Critical assertion from the issue: no worker respawn for task #1.
    assert!(
        !handle2.any_line_contains("spawning agent"),
        "restart re-executed task with a fresh worker. Lines: {:?}",
        handle2.lines
    );

    handle2.hard_kill();
}

/// Drain-mode variant: SIGINT while a worker is awaiting review should shelve
/// (preserve journal, keep task claimed) rather than revert-to-open.
#[test]
fn drain_after_done_pr_shelves_awaiting_review_worker() {
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

    seed_task(home.path(), "Task for drain-shelve");

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
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "99"]);

    assert!(
        handle.wait_for("ready for review", 15),
        "PR ready-for-review not seen. Lines: {:?}",
        handle.lines
    );

    // Send second SIGINT quickly to force teardown path.
    unsafe {
        libc::kill(handle.child.id() as libc::pid_t, libc::SIGINT);
        libc::kill(handle.child.id() as libc::pid_t, libc::SIGINT);
    }
    // Give the daemon time to process signals and shelve.
    let mut shelved = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        if handle.wait_for("shelved", 1) || handle.wait_for("shelving", 1) {
            shelved = true;
            break;
        }
    }
    assert!(
        shelved,
        "worker not shelved on drain. Lines: {:?}",
        handle.lines
    );

    // Wait for daemon exit.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        match handle.child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => std::thread::sleep(Duration::from_millis(100)),
            Err(_) => break,
        }
    }
    let _ = handle.child.wait();

    // Task must still be claimed post-drain (PR state preserved).
    let status_after = task_status(home.path(), 1);
    assert_eq!(
        status_after, "claimed",
        "drain reverted task to {status_after} — PR info lost"
    );
}
