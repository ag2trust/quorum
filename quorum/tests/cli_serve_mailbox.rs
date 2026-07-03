//! Mailbox consumption correctness tests (audit F8/F9/F13).
//!
//! Scenarios:
//! 1. Unmatched Done row is consumed (not re-polled forever) — F9
//! 2. Non-Done mailbox kinds don't interfere with daemon operation
//! 3. Stale Done row for a reused name is consumed at spawn time,
//!    preventing phantom verdict — F9
//! 4. Rework feed failure tears down the broken worker and releases
//!    the task back to open — F8

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

/// Insert a raw mailbox row directly via the quorum DB (bypasses CLI).
fn insert_mailbox_row(home: &std::path::Path, agent: &str, kind: &str) {
    let db_path = home.join("quorum.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO mailbox (agent, kind, created_at) VALUES (?1, ?2, strftime('%s','now'))",
        rusqlite::params![agent, kind],
    )
    .unwrap();
}

/// Count unconsumed mailbox rows for a given agent.
fn count_unconsumed(home: &std::path::Path, agent: &str) -> usize {
    let db_path = home.join("quorum.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM mailbox WHERE agent = ?1 AND consumed_at IS NULL",
        rusqlite::params![agent],
        |r| r.get::<_, usize>(0),
    )
    .unwrap()
}

// ── F9: Unmatched Done row is consumed ──────────────────────────────────

#[test]
fn unmatched_done_row_consumed() {
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

    seed_task(home.path(), "Task for unmatched row test");

    // Write a Done row for an agent name that will never be assigned.
    quorum_done(home.path(), &["--agent", "GhostAgent"]);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Wait for the daemon to process the unmatched row.
    assert!(
        handle.wait_for("consuming unmatched Done row from GhostAgent", 15),
        "daemon did not log consuming unmatched Done row. Lines: {:?}",
        handle.lines
    );

    handle.stop();

    // The GhostAgent row should now be consumed.
    assert_eq!(
        count_unconsumed(home.path(), "GhostAgent"),
        0,
        "unmatched Done row was not consumed"
    );
}

// ── Non-Done mailbox kinds are consumed gracefully ────────────────────

#[test]
fn non_done_mailbox_rows_consumed_gracefully() {
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

    seed_task(home.path(), "Task for non-Done kind test");

    // Insert M5 mailbox rows for an agent with no active worker — the
    // daemon should consume them (task_update: "no active worker",
    // message: "no to_agent") and still spawn a worker for the task.
    insert_mailbox_row(home.path(), "SomeAgent", "task_update");
    insert_mailbox_row(home.path(), "SomeAgent", "message");

    assert_eq!(count_unconsumed(home.path(), "SomeAgent"), 2);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Daemon should spawn a worker normally — non-Done rows don't block it.
    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned despite non-Done rows. Lines: {:?}",
        handle.lines
    );

    handle.stop();

    assert_eq!(
        count_unconsumed(home.path(), "SomeAgent"),
        0,
        "non-Done mailbox rows were not consumed"
    );
}

// ── F9: Phantom verdict prevention via stale-row drain at spawn ────────

#[test]
fn stale_done_row_drained_on_name_reuse() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());

    // Names pool requires >2*cap entries. Agent0 is first, so it will be
    // acquired for the worker.
    let names_file = home.path().join("names.txt");
    std::fs::write(&names_file, "Agent0\nAgent1\nAgent2\nAgent3\n").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for phantom verdict test");

    // Plant a stale Done+approved row for "Agent0" BEFORE serve starts.
    // If this isn't drained, the daemon would apply the verdict to the
    // new worker that acquires "Agent0".
    quorum_done(
        home.path(),
        &["--agent", "Agent0", "--verdict", "approved", "--pr", "99"],
    );

    assert_eq!(count_unconsumed(home.path(), "Agent0"), 1);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // The daemon should consume the stale row — either as "unmatched" in
    // Phase 2 (before any worker is spawned) or drained at spawn time.
    // Both are valid defenses; Phase 2 typically fires first.
    assert!(
        handle.wait_for("Agent0", 15),
        "daemon did not process Agent0 rows. Lines: {:?}",
        handle.lines
    );

    let stale_consumed = handle.lines.iter().any(|l| {
        l.contains("consuming unmatched Done row from Agent0")
            || l.contains("consumed") && l.contains("stale") && l.contains("Agent0")
    });
    assert!(
        stale_consumed,
        "stale Done row was not consumed. Lines: {:?}",
        handle.lines
    );

    // The worker should spawn normally (not be affected by the stale verdict).
    assert!(
        handle.wait_for("spawning agent", 15),
        "worker was not spawned. Lines: {:?}",
        handle.lines
    );

    // Wait for the worker to produce a result (normal turn 1).
    assert!(
        handle.wait_for("result", 15),
        "worker did not produce a result. Lines: {:?}",
        handle.lines
    );

    // Task should NOT be marked done (the stale approved verdict was drained,
    // not applied).
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        !stdout.contains("\"status\":\"done\"") && !stdout.contains("\"status\": \"done\""),
        "task was marked done by stale verdict (phantom): {stdout}"
    );

    handle.stop();
}

// ── F8: Rework feed failure tears down broken worker ───────────────────

#[test]
fn rework_feed_failure_releases_task() {
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

    seed_task(home.path(), "Task for rework feed failure");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Wait for worker to spawn and produce a result.
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

    // Worker signals "done with PR" — triggers reviewer spawn.
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );

    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    // Wait for reviewer to finish its turn.
    std::thread::sleep(Duration::from_secs(2));

    // Kill the worker process directly so its stdin pipe breaks.
    // We read the worker PID from /proc or use the agent name to find it.
    // Simpler: use the daemon's own serve process to find the child.
    // Actually, the easiest way: look up the process. Since fake-agent
    // is our child's child, we can find it by name and kill it.
    let pgrep_out = Command::new("pgrep")
        .args(["-f", "fake-agent.*--session-id"])
        .output();
    if let Ok(out) = pgrep_out {
        let pids = String::from_utf8_lossy(&out.stdout);
        for pid_str in pids.lines() {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
    }

    // Give the worker a moment to actually die.
    std::thread::sleep(Duration::from_secs(1));

    // Reviewer signals "changes" verdict — daemon will try to feed rework
    // to the (now dead) worker, which should fail.
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
            "Fix the tests",
        ],
    );

    // The daemon should detect the feed failure and tear down the worker.
    // Either: "rework feed_turn failed" + "tearing down broken worker"
    // or: the death detection in Phase 4b catches it first and logs
    // "died mid-task".
    let saw_feed_failure = handle.wait_for("tearing down", 15);
    let saw_death = handle.lines.iter().any(|l| {
        l.contains("tearing down broken worker")
            || l.contains("died mid-task")
            || l.contains("tearing down worker")
    });
    assert!(
        saw_feed_failure || saw_death,
        "daemon did not handle dead worker. Lines: {:?}",
        handle.lines
    );

    // Wait for cleanup.
    std::thread::sleep(Duration::from_secs(1));

    handle.stop();

    // Task must be back to open (not stranded in claimed/in-progress).
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"open\"") || stdout.contains("\"status\": \"open\""),
        "task was not released back to open after rework feed failure: {stdout}"
    );
}
