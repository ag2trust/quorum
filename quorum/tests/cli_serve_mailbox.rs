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

mod common;

use std::env;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use common::{wait_until, WaitState};

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

    fn suspend(&mut self) {
        let pid = self.child.id() as libc::pid_t;
        assert_eq!(
            unsafe { libc::kill(pid, libc::SIGSTOP) },
            0,
            "failed to suspend daemon process {pid}"
        );
        wait_for_process_suspended(pid);
    }

    fn resume(&mut self) {
        let pid = self.child.id() as libc::pid_t;
        assert_eq!(
            unsafe { libc::kill(pid, libc::SIGCONT) },
            0,
            "failed to resume daemon process {pid}"
        );
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

fn append_phase4c_barrier(conn: &mut rusqlite::Connection, target: &str) {
    let row = quorum_core::mailbox::MailboxRow {
        agent: "ReworkPhaseBarrier".to_string(),
        kind: quorum_core::mailbox::MailboxKind::Message,
        task_id: None,
        pr: None,
        verdict: None,
        feedback: None,
        note: None,
        to_agent: Some(target.to_string()),
        payload: Some("phase barrier".to_string()),
    };
    quorum_core::mailbox::append(conn, &row).unwrap();
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

fn wait_for_unconsumed_count(home: &std::path::Path, agent: &str, expected: usize) {
    let db_path = home.join("repos/test__repo/quorum.db");
    wait_until(
        &format!("{expected} unconsumed mailbox row(s) for {agent}"),
        Duration::from_secs(15),
        || match quorum_core::db::open(&db_path) {
            Ok(conn) => {
                let actual = conn.query_row(
                    "SELECT COUNT(*) FROM mailbox WHERE agent=?1 AND consumed_at IS NULL",
                    [agent],
                    |row| row.get::<_, usize>(0),
                );
                match actual {
                    Ok(actual) if actual == expected => WaitState::Ready(()),
                    Ok(actual) => WaitState::Pending(format!(
                        "agent {agent} still had {actual} unconsumed mailbox row(s)"
                    )),
                    Err(error) => WaitState::Pending(format!(
                        "could not query {}: {error}",
                        db_path.display()
                    )),
                }
            }
            Err(error) => {
                WaitState::Pending(format!("could not open {}: {error}", db_path.display()))
            }
        },
    );
}

fn managed_pid(home: &std::path::Path, role: &str) -> i32 {
    let db_path = home.join("repos/test__repo/quorum.db");
    let conn = quorum_core::db::open(&db_path).unwrap();
    conn.query_row(
        "SELECT pid FROM journal WHERE role=?1 AND pid IS NOT NULL",
        [role],
        |row| row.get(0),
    )
    .unwrap()
}

fn wait_for_process_terminated(pid: i32) {
    wait_until(
        &format!("managed process {pid} to terminate"),
        Duration::from_secs(15),
        || {
            let output = Command::new("ps")
                .args(["-o", "stat=", "-p", &pid.to_string()])
                .output();
            match output {
                Ok(output) if !output.status.success() || output.stdout.is_empty() => {
                    WaitState::Ready(())
                }
                Ok(output) => {
                    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if state.starts_with('Z') {
                        WaitState::Ready(())
                    } else {
                        WaitState::Pending(format!("process {pid} state was {state:?}"))
                    }
                }
                Err(error) => {
                    WaitState::Pending(format!("could not inspect process {pid} state: {error}"))
                }
            }
        },
    );
}

fn wait_for_process_suspended(pid: i32) {
    wait_until(
        &format!("daemon process {pid} to suspend"),
        Duration::from_secs(15),
        || {
            let output = Command::new("ps")
                .args(["-o", "stat=", "-p", &pid.to_string()])
                .output();
            match output {
                Ok(output) if !output.status.success() || output.stdout.is_empty() => {
                    WaitState::Pending(format!("daemon process {pid} was absent"))
                }
                Ok(output) => {
                    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if state.starts_with('T') {
                        WaitState::Ready(())
                    } else {
                        WaitState::Pending(format!("daemon process {pid} state was {state:?}"))
                    }
                }
                Err(error) => WaitState::Pending(format!(
                    "could not inspect daemon process {pid} state: {error}"
                )),
            }
        },
    );
}

fn wait_for_task_status(home: &std::path::Path, task_id: i64, expected: &str) {
    let db_path = home.join("repos/test__repo/quorum.db");
    wait_until(
        &format!("task #{task_id} status {expected:?}"),
        Duration::from_secs(15),
        || match quorum_core::db::open(&db_path) {
            Ok(conn) => {
                let status = conn
                    .query_row("SELECT status FROM tasks WHERE id=?1", [task_id], |row| {
                        row.get::<_, String>(0)
                    })
                    .ok();
                if status.as_deref() == Some(expected) {
                    WaitState::Ready(())
                } else {
                    WaitState::Pending(format!("task #{task_id} status was {status:?}"))
                }
            }
            Err(error) => {
                WaitState::Pending(format!("could not open {}: {error}", db_path.display()))
            }
        },
    );
}

fn wait_for_journal_absent(home: &std::path::Path, agent: &str) {
    let db_path = home.join("repos/test__repo/quorum.db");
    wait_until(
        &format!("journal teardown for agent {agent}"),
        Duration::from_secs(15),
        || match quorum_core::db::open(&db_path) {
            Ok(conn) => {
                let count = conn.query_row(
                    "SELECT COUNT(*) FROM journal WHERE agent=?1",
                    [agent],
                    |row| row.get::<_, i64>(0),
                );
                match count {
                    Ok(0) => WaitState::Ready(()),
                    Ok(count) => {
                        WaitState::Pending(format!("journal still has {count} row(s) for {agent}"))
                    }
                    Err(error) => WaitState::Pending(format!(
                        "could not query {}: {error}",
                        db_path.display()
                    )),
                }
            }
            Err(error) => {
                WaitState::Pending(format!("could not open {}: {error}", db_path.display()))
            }
        },
    );
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

    wait_for_unconsumed_count(home.path(), "GhostAgent", 0);

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

    wait_for_unconsumed_count(home.path(), "GhostAgent", 0);
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

    wait_for_unconsumed_count(home.path(), "Aardvark0", 0);
    wait_for_unconsumed_count(home.path(), "Beluga0", 0);
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

    wait_for_unconsumed_count(home.path(), "SomeAgent", 0);

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

    // A message to an absent agent is handled in Phase 4c, after Phase 4b.
    // Its log precedes the marker-consumption write, so first observe the log,
    // then serialize behind that write before suspending the daemon.
    let phase_barrier_target = "MissingReworkPhaseBarrier";
    let db_path = home.path().join("repos/test__repo/quorum.db");
    let mut barrier_conn = quorum_core::db::open(&db_path).unwrap();
    append_phase4c_barrier(&mut barrier_conn, phase_barrier_target);
    assert!(
        handle.wait_for(
            &format!("consuming message to {phase_barrier_target} (no active worker)"),
            15,
        ),
        "daemon did not reach the post-death-scan barrier. Lines: {:?}",
        handle.lines
    );

    // BEGIN IMMEDIATE cannot succeed until marker consumption commits. Once it
    // does, no daemon thread owns a write transaction, and the barrier prevents
    // any later writer from starting before SIGSTOP becomes observable.
    let db_write_barrier =
        quorum_core::db::begin_immediate(&mut barrier_conn).unwrap_or_else(|error| {
            panic!(
                "could not acquire post-consumption SQLite writer barrier before daemon \
                 suspension: {error}"
            )
        });
    handle.suspend();
    db_write_barrier.commit().unwrap_or_else(|error| {
        panic!("could not release Phase 4c SQLite writer barrier: {error}")
    });
    drop(barrier_conn);

    // The paused daemon retains this worker slot while the process dies and
    // the reviewer verdict is atomically appended to the mailbox.
    let worker_pid = managed_pid(home.path(), "worker");
    assert_eq!(unsafe { libc::kill(worker_pid, libc::SIGKILL) }, 0);
    wait_for_process_terminated(worker_pid);

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
    handle.resume();

    // The specific failure proves the changes verdict reached the dead
    // worker's rework-feed path, rather than a Phase 4b teardown winning first.
    assert!(
        handle.wait_for("rework feed_turn failed", 15),
        "daemon did not exercise the failed rework-feed path. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for(&format!("tearing down worker {worker_name}"), 15),
        "dead worker teardown not seen after rework-feed failure. Lines: {:?}",
        handle.lines
    );

    // VerdictChanges moves the task to rework, then AgentFailed from the
    // failed feed releases it to open. Journal absence proves cleanup finished.
    wait_for_task_status(home.path(), 1, "open");
    wait_for_journal_absent(home.path(), &worker_name);

    handle.stop();

    // The failed rework delivery releases the task for a replacement worker.
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    let task: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(
        task["status"].as_str(),
        Some("open"),
        "task was not released after rework feed failure: {stdout}"
    );
}
