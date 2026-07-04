//! Mailbox consumption correctness tests (audit F8/F9/F13 + issue #181).
//!
//! Scenarios:
//! 1. Foreign Done row (agent not in this instance's lifetime roster) is LEFT
//!    unconsumed — #181 preserves sibling daemon signals.
//! 2. Phantom Done row for an agent this daemon HAS owned is still consumed —
//!    F9 phantom-verdict guarantee preserved within a single instance.
//! 3. Two-daemon regression: daemon B never eats daemon A's live signal (#181).
//! 4. Non-Done mailbox kinds don't block the daemon.
//! 5. Stale Done row for a reused name is consumed at spawn time via the
//!    at-spawn drain, preventing phantom verdict — F9.
//! 6. Rework feed failure tears down the broken worker and releases the task
//!    back to open — F8.

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

// ── #181: Foreign Done row (name never owned by this instance) is LEFT ────
//
// Under two-instance topology (shared SQLite queue), a Done row whose agent
// name has never been in this daemon's lifetime roster is assumed to belong
// to a sibling instance's worker — leave it for the owner. Consuming would
// destroy the sibling's lifecycle signal (issue #181).

#[test]
fn unmatched_done_row_left_for_other_instance() {
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

    // Write a Done row for a name that this daemon will never acquire.
    // (Its names_file only contains Agent0..Agent19; "GhostAgent" is
    // outside the pool, so this daemon can never claim it.)
    quorum_done(home.path(), &["--agent", "GhostAgent"]);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Daemon should log that the row is being left (not consumed).
    assert!(
        handle.wait_for(
            "leaving Done row from GhostAgent unconsumed (not in this instance's roster)",
            15
        ),
        "daemon did not log leaving GhostAgent row. Lines: {:?}",
        handle.lines
    );

    // Give the daemon a couple more ticks to (not) consume the row.
    std::thread::sleep(Duration::from_millis(1500));

    handle.stop();

    // The GhostAgent row must still be unconsumed — leaving it for TTL/owner.
    assert_eq!(
        count_unconsumed(home.path(), "GhostAgent"),
        1,
        "foreign Done row was consumed (would destroy sibling daemon's signal)"
    );
}

// ── #181: F9 phantom-row GC preserved WITHIN the same instance ─────────
//
// A row for an agent name we HAVE owned (spawned) previously must still be
// consumed if it doesn't match a live slot — otherwise it would re-poll every
// tick and, on name reuse, apply as a phantom verdict (the #133 guarantee).

#[test]
fn phantom_done_row_for_owned_name_still_consumed() {
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

    seed_task(home.path(), "Task for phantom-in-own-roster test");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Wait for daemon to spawn a worker (registers the name in the roster).
    assert!(
        handle.wait_for("spawning agent ", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();

    // Simulate the worker being killed by an external actor while a stray
    // Done row is left in the queue for the same (now dead) worker.
    // We plant a Done row and rely on the daemon's Phase-4b death detection
    // to remove the worker slot — then the stray row is "unmatched but ours".
    //
    // Easier probe: write another done row directly for the SAME name via CLI,
    // wait for it to be processed. The first message consumes the live slot
    // (worker done — no PR = teardown). The second row lands with no matching
    // live slot but is in our lifetime roster → must be consumed.
    quorum_done(home.path(), &["--agent", &worker_name]);

    // Wait for worker teardown from first done.
    assert!(
        handle.wait_for(&format!("worker {} done", worker_name), 15),
        "daemon did not process first done. Lines: {:?}",
        handle.lines
    );

    // Plant a second done row post-teardown — this simulates a phantom.
    quorum_done(home.path(), &["--agent", &worker_name]);

    // Assert the daemon consumes this row (does NOT leave it).
    assert!(
        handle.wait_for(
            &format!("consuming unmatched Done row from {worker_name}"),
            15
        ),
        "daemon did not consume phantom row for its own retired agent. Lines: {:?}",
        handle.lines
    );

    handle.stop();
    assert_eq!(
        count_unconsumed(home.path(), &worker_name),
        0,
        "phantom row for owned name was left unconsumed"
    );
}

// ── #181: Two daemons, one queue — neither eats the other's rows ───────
//
// Regression for the bug reported in #181: two per-repo daemons share a
// SQLite queue; each has its own worktree base and names file. A Done row
// for daemon A's live agent must NOT be consumed by daemon B — which polls
// at 500ms ticks and would otherwise coin-flip who eats the signal.

#[test]
fn two_daemons_do_not_consume_sibling_signals() {
    let home = tempfile::tempdir().unwrap();
    let repo_a = tempfile::tempdir().unwrap();
    let repo_b = tempfile::tempdir().unwrap();
    let wt_b = tempfile::tempdir().unwrap();

    init_git_repo(repo_a.path());
    init_git_repo(repo_b.path());

    // Two disjoint name pools so daemons cannot acquire each other's names.
    let names_a = home.path().join("names_a.txt");
    let names_b = home.path().join("names_b.txt");
    let mut fa = std::fs::File::create(&names_a).unwrap();
    let mut fb = std::fs::File::create(&names_b).unwrap();
    for i in 0..10 {
        writeln!(fa, "Aardvark{i}").unwrap();
        writeln!(fb, "Beluga{i}").unwrap();
    }
    drop(fa);
    drop(fb);

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .status()
        .unwrap();

    // Plant a Done row for one of daemon A's agent names BEFORE daemon B starts.
    // Note: this is the same shape as A's Cleat-d12 done row in issue #181.
    quorum_done(home.path(), &["--agent", "Aardvark0"]);
    quorum_done(home.path(), &["--agent", "Beluga0"]);

    // Start daemon B ONLY (not A). B polls; sees rows for Aardvark0 (not in
    // B's roster) and Beluga0 (also not in B's roster since B hasn't spawned
    // yet). Both must be LEFT unconsumed.
    let mut handle_b = ServeHandle::start(home.path(), repo_b.path(), wt_b.path(), &names_b);

    // Wait for B to log "leaving Done row from Aardvark0" — the sibling signal.
    assert!(
        handle_b.wait_for(
            "leaving Done row from Aardvark0 unconsumed (not in this instance's roster)",
            15
        ),
        "daemon B did not leave Aardvark0 (sibling) row unconsumed. Lines: {:?}",
        handle_b.lines
    );

    // Give B extra ticks to (not) consume.
    std::thread::sleep(Duration::from_millis(1500));

    // Sibling row must survive.
    assert_eq!(
        count_unconsumed(home.path(), "Aardvark0"),
        1,
        "daemon B consumed daemon A's sibling done signal (#181 regression)"
    );

    handle_b.stop();
}

// ── Non-Done mailbox kinds are consumed gracefully ────────────────────

#[test]
fn non_done_mailbox_rows_dont_block_daemon() {
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

    // Insert M5 mailbox rows for an agent this daemon will never own.
    // Message row has no `to_agent` — the daemon considers this malformed
    // and consumes it regardless of roster (they are unroutable). task_update
    // for a foreign name is left for the owning instance (#181).
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

    // Give the daemon a moment to finish processing the two mailbox rows.
    std::thread::sleep(Duration::from_millis(1500));

    handle.stop();

    // Message with no to_agent is unroutable — consumed regardless (#181).
    // task_update for a name outside this daemon's roster is left for the
    // owning instance (or TTL sweep).
    assert_eq!(
        count_unconsumed(home.path(), "SomeAgent"),
        1,
        "expected exactly 1 unconsumed row (the foreign task_update); \
         message-with-no-to_agent must still be consumed"
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

    // Under #181 semantics, the daemon may either LEAVE the stale row (name
    // not yet in roster) or CONSUME it at spawn time when Agent0 is acquired.
    // The at-spawn drain (`consumed N stale mailbox row(s) for Agent0`) is
    // the mandatory defense — it MUST fire because Agent0 is the first name
    // in the pool.
    assert!(
        handle.wait_for("consumed 1 stale mailbox row(s) for Agent0", 15),
        "at-spawn drain did not consume stale Agent0 row. Lines: {:?}",
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
