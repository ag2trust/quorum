//! Mailbox consumption correctness tests (audit F8/F9/F13).
//!
//! Scenarios:
//! 1. Non-roster Done row (passive agent, no working task) is consumed as
//!    phantom — daemon_lock (invariant 11) guarantees single daemon per DB.
//! 2. Phantom Done row for an agent this daemon HAS owned is still consumed —
//!    F9 phantom-verdict guarantee preserved within a single instance.
//! 3. Non-roster Done rows are consumed (no sibling daemon to defer to).
//! 4. Non-Done mailbox kinds don't block the daemon.
//! 5. Stale Done row for a name is consumed (passive agent phantom path or
//!    at-spawn drain), preventing phantom verdict — F9.
//! 6. Rework feed failure tears down the broken worker and releases the task
//!    back to open — F8.

use std::env;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
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
  printf '{"headRefName":"%s","headRefOid":"%s","isCrossRepository":false,"baseRefName":"main"}\n' "$branch" "$sha"
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
        let mut child = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .env("PATH", path)
            .env("QUORUM_TEST_GH_STATE", &gh_state)
            .env("QUORUM_TEST_REPO", repo)
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
                "--merge-cmd",
                "true",
                "--exit-when-gone",
                &sentinel_path,
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

/// Insert a raw mailbox row directly via the quorum DB (bypasses CLI).
fn insert_mailbox_row(home: &std::path::Path, agent: &str, kind: &str) {
    let db_path = home.join("repos/test__repo/quorum.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO mailbox (agent, kind, created_at) VALUES (?1, ?2, strftime('%s','now'))",
        rusqlite::params![agent, kind],
    )
    .unwrap();
}

/// Count unconsumed mailbox rows for a given agent.
fn count_unconsumed(home: &std::path::Path, agent: &str) -> usize {
    let db_path = home.join("repos/test__repo/quorum.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM mailbox WHERE agent = ?1 AND consumed_at IS NULL",
        rusqlite::params![agent],
        |r| r.get::<_, usize>(0),
    )
    .unwrap()
}

// ── Unmatched Done row is consumed (#130) ────────────────────────────
//
// daemon_lock (invariant 11) guarantees one daemon per DB. A Done row from
// an agent with no active slot is consumed as an unmatched phantom.

#[test]
fn unmatched_done_row_consumed_as_passive_phantom() {
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

    seed_task(home.path(), "Task for unmatched row test");

    // Write a Done row for a name outside the daemon's pool.
    quorum_done(home.path(), &["--agent", "GhostAgent"]);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Daemon should process the row as a passive agent phantom (no working task).
    assert!(
        handle.wait_for("consuming unmatched Done row from GhostAgent", 15),
        "daemon did not process GhostAgent row. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_secs(1));

    handle.stop();

    // Row must be consumed — single daemon per DB, no sibling to defer to.
    assert_eq!(
        count_unconsumed(home.path(), "GhostAgent"),
        0,
        "non-roster Done row was not consumed"
    );
}

// ── #130 negative-path: unmatched Done does NOT drive lifecycle ────────
//
// R1 advisory #4: replaces the deleted passive-agent submit coverage. Asserts
// that a Done row from an agent with no active slot is consumed as a phantom
// AND that no lifecycle transition occurs — task stays `open`, no
// `SignaledDone` event is emitted. Locks the invariant against silent
// regressions where a stray Done could still drive the state machine.

#[test]
fn unmatched_done_row_does_not_drive_lifecycle() {
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

    // No tasks seeded — the daemon idles, no worker gets spawned. Any lifecycle
    // event that shows up can only be a phantom driven by the ghost Done row.
    quorum_done(home.path(), &["--agent", "GhostAgent", "--pr", "42"]);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Daemon must consume the row (invariant: single-daemon-per-DB).
    assert!(
        handle.wait_for("consuming unmatched Done row from GhostAgent", 15),
        "daemon did not consume ghost row. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_secs(1));
    handle.stop();

    // Row consumed as phantom.
    assert_eq!(count_unconsumed(home.path(), "GhostAgent"), 0);

    // No lifecycle transition event may have fired: no task_in_review,
    // no task_working, no task_done. The event kind is emitted as
    // `task_{new_status.replace('-', '_')}` (tasks.rs:653) — assert the
    // full family stayed empty since no worker was ever spawned.
    let db_path = home.path().join("repos/test__repo/quorum.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let lifecycle_events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE kind IN ('task_in_review', 'task_working', 'task_done',
                            'task_merging', 'task_rework')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    assert_eq!(
        lifecycle_events, 0,
        "unmatched Done row triggered a lifecycle event (phantom drove state machine)"
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
        .env("QUORUM_REPO", "test/repo")
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
    // Retire the live slot through an external lifecycle move, then plant the
    // stray Done row for the same lifetime-roster name.
    let cancel = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-update",
            "--task-id",
            "1",
            "--agent",
            "TestCreator",
            "--status",
            "cancelled",
        ])
        .output()
        .unwrap();
    assert!(
        cancel.status.success(),
        "task cancellation failed: {}",
        String::from_utf8_lossy(&cancel.stderr)
    );
    assert!(
        handle.wait_for("externally moved to cancelled", 15),
        "daemon did not retire worker. Lines: {:?}",
        handle.lines
    );

    // Plant a done row post-teardown — this simulates a phantom.
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

// ── Single daemon consumes all non-roster Done rows ──────────────────
//
// daemon_lock (invariant 11) guarantees one daemon per DB. Non-roster
// Done rows without working tasks are consumed as passive agent phantoms.

#[test]
fn non_roster_done_rows_consumed_as_phantoms() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());

    let names_file = home.path().join("names.txt");
    {
        let mut f = std::fs::File::create(&names_file).unwrap();
        for i in 0..10 {
            writeln!(f, "Beluga{i}").unwrap();
        }
    }

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    // Plant Done rows for names outside the daemon's pool.
    quorum_done(home.path(), &["--agent", "Aardvark0"]);
    quorum_done(home.path(), &["--agent", "Beluga0"]);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Both rows consumed as passive agent phantoms (no working task found).
    assert!(
        handle.wait_for("consuming unmatched Done row from Aardvark0", 15),
        "Aardvark0 row not processed. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_secs(1));
    handle.stop();

    assert_eq!(
        count_unconsumed(home.path(), "Aardvark0"),
        0,
        "non-roster Done row for Aardvark0 was not consumed"
    );
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
        .env("QUORUM_REPO", "test/repo")
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

    // Wait 2 ticks for the daemon to finish processing the two mailbox rows.
    std::thread::sleep(Duration::from_secs(1));

    handle.stop();

    // Both rows consumed: single daemon per DB (invariant 11), no sibling
    // to defer to. Message with no to_agent is unroutable; task_update for
    // a passive agent has no worker slot but is still consumed.
    assert_eq!(
        count_unconsumed(home.path(), "SomeAgent"),
        0,
        "non-Done mailbox rows for non-roster agent were not consumed"
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
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for phantom verdict test");

    // Plant a stale Done+approved row for "Agent0" BEFORE serve starts.
    // If this isn't drained, the daemon would apply the verdict to the
    // new worker that acquires "Agent0".
    quorum_done(
        home.path(),
        &[
            "--agent",
            "Agent0",
            "--verdict",
            "approved",
            "--blocking",
            "0",
            "--pr",
            "99",
        ],
    );

    assert_eq!(count_unconsumed(home.path(), "Agent0"), 1);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // The stale Done row is consumed via the passive agent phantom path
    // (no working task found for Agent0) before the at-spawn drain fires.
    assert!(
        handle.wait_for("consuming unmatched Done row from Agent0", 15),
        "stale Agent0 row not consumed. Lines: {:?}",
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
        .env("QUORUM_REPO", "test/repo")
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
        .env("QUORUM_REPO", "test/repo")
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

    // Worker signals completion; the daemon publishes and creates the PR.
    quorum_done(home.path(), &["--agent", &worker_name]);

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

    // Kill ALL fake-agent processes so the worker's stdin pipe breaks.
    // This also kills the reviewer, so Phase 4b fires AgentFailed on both.
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

    handle.stop();

    // With lifecycle, AgentFailed from in-review stays in-review (the code
    // was pushed, task just needs a new reviewer). The pgrep kills both
    // agents, so the task lands in in-review — not open.
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"in-review\"")
            || stdout.contains("\"status\": \"in-review\"")
            || stdout.contains("\"status\":\"open\"")
            || stdout.contains("\"status\": \"open\""),
        "task was not in a recoverable state after rework feed failure: {stdout}"
    );
}
