//! Recovery acceptance tests: after a daemon restart, in-review tasks must
//! get reviewers provisioned (via Phase 5b) without re-executing the worker.
//!
//! Stateless recovery (2026-07-09 refactor) kills stale processes, wipes the
//! journal, GCs worktrees, and resets non-terminal tasks. In-review tasks are
//! left as-is — the normal tick loop's Phase 5b queries the DB for in-review
//! tasks with a PR but no live worker/reviewer and provisions a reviewer.

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
  if [ -f "$QUORUM_TEST_GH_STATE/$pr" ]; then
    branch="$(cat "$QUORUM_TEST_GH_STATE/$pr")"
  else
    branch="daemon/agent0-t$pr"
  fi
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

    fn drain_pending_lines(&mut self) {
        while let Ok(line) = self.rx.try_recv() {
            self.lines.push(line);
        }
    }

    /// Wait until the daemon has completed at least one full scheduling tick.
    ///
    /// Phase 2 consumes each mailbox marker. Inserting the second marker only
    /// after the first is consumed makes its observation prove that the prior
    /// tick reached the later recovery and provisioning phases.
    fn wait_for_completed_tick(&mut self, home: &std::path::Path) {
        let db_path = home.join("repos/test__repo/quorum.db");
        for marker in ["RecoveryTick0", "RecoveryTick1"] {
            let mut conn = quorum_core::db::open(&db_path).unwrap();
            quorum_core::mailbox::append(
                &mut conn,
                &quorum_core::mailbox::MailboxRow {
                    agent: marker.into(),
                    kind: quorum_core::mailbox::MailboxKind::TaskUpdate,
                    task_id: None,
                    pr: None,
                    verdict: None,
                    feedback: None,
                    note: Some("readiness barrier".into()),
                    to_agent: None,
                    payload: None,
                },
            )
            .unwrap();
            drop(conn);
            assert!(
                self.wait_for(
                    &format!("consuming unmatched task_update from {marker}"),
                    15,
                ),
                "daemon did not consume readiness marker {marker}. Lines: {:?}",
                self.lines
            );
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

    fn start_with_agent_bin(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        agent_bin: &str,
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let sentinel_path = sentinel.path().to_string_lossy().to_string();
        let mut child = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home)
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
                agent_bin,
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
            _gh_shim: None,
        }
    }

    fn sigkill(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL);
        }
        let _ = self.child.wait();
        while let Ok(line) = self.rx.try_recv() {
            self.lines.push(line);
        }
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
    if role == "worker" {
        let mut index = 0;
        while index < args.len() {
            if args[index] == "--pr" {
                index += 2;
            } else {
                cmd_args.push(args[index]);
                index += 1;
            }
        }
    } else {
        cmd_args.extend_from_slice(args);
    }
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

/// #178: worker signals done --pr N, daemon is SIGKILL'd before verdict,
/// then relaunched — the restart MUST resume at the review stage
/// (provision reviewer against recorded PR) rather than re-execute the
/// task from scratch and produce a duplicate PR.
///
/// Post-refactor: stateless recovery wipes the journal and leaves the
/// in-review task as-is. Phase 5b of the tick loop detects the orphan
/// in-review task (has PR, no live worker/reviewer) and provisions a
/// reviewer.
#[test]
fn restart_resumes_awaiting_review_at_review_stage_no_re_execution() {
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

    seed_task(home.path(), "Task for #178 restart-at-review acceptance");

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

    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("PR #1 ready for review", 15),
        "daemon did not acknowledge PR #1 before SIGKILL. Lines: {:?}",
        handle.lines
    );
    handle.drain_pending_lines();

    // ── Kill the daemon hard (mimics operator hitting the process). ──
    handle.sigkill();

    // ── Relaunch the daemon. ──
    let mut handle2 = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Recovery must complete (stateless: kill stale, wipe journal, GC worktrees).
    assert!(
        handle2.wait_for("recovery: complete", 30),
        "recovery did not complete. Lines: {:?}",
        handle2.lines
    );

    // Phase 5b must detect the orphan in-review task and provision a reviewer.
    assert!(
        handle2.wait_for("spawning reviewer", 30),
        "reviewer not provisioned for in-review task after restart. Lines: {:?}",
        handle2.lines
    );
    handle2.drain_pending_lines();

    // ── Invariant: NO fresh worker spawn for task #1 after restart. ──
    let fresh_worker_spawns = handle2
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent") && l.contains("task #1"))
        .count();
    assert_eq!(
        fresh_worker_spawns, 0,
        "restart re-executed the task from scratch (found 'spawning agent for task #1'). \
         Lines: {:?}",
        handle2.lines
    );

    handle2.sigkill();
}

/// In-review task with no journal row — Phase 5b must detect it and
/// provision a reviewer on the first tick after startup.
#[test]
fn orphan_in_review_task_gets_reviewer_on_startup() {
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

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let now = 1000;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "Orphan task",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test-classifier:v2"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        assert_eq!(id, 1);
        quorum_core::tasks::claim(&mut conn, "W1", Some(id), &[], 3600, now).unwrap();
        quorum_core::tasks::apply_event(
            &mut conn,
            "W1",
            id,
            &quorum_core::lifecycle::Event::SignaledDone {
                pr: "42".to_string(),
            },
            now + 1,
        )
        .unwrap();
        let task = quorum_core::tasks::get(&conn, id).unwrap().unwrap();
        assert_eq!(task.status, "in-review");
    }

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // Phase 5b provisions a reviewer for the orphan in-review task.
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not provisioned for orphan in-review task. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Double-restart idempotent: task stays in-review across two restarts.
#[test]
fn double_restart_in_review_stays_in_review() {
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

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let now = 1000;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "Orphan task 2",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test-classifier:v2"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        quorum_core::tasks::claim(&mut conn, "W1", Some(id), &[], 3600, now).unwrap();
        quorum_core::tasks::apply_event(
            &mut conn,
            "W1",
            id,
            &quorum_core::lifecycle::Event::SignaledDone {
                pr: "42".to_string(),
            },
            now + 1,
        )
        .unwrap();
    }

    // First daemon start — recovery completes, Phase 5b handles reviewer.
    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);
    assert!(
        handle.wait_for("recovery: complete", 15),
        "first-start recovery did not complete. Lines: {:?}",
        handle.lines
    );
    handle.sigkill();

    // Second daemon start — recovery again, task still in-review.
    let mut handle2 = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);
    assert!(
        handle2.wait_for("recovery: complete", 15),
        "second start did not complete recovery. Lines: {:?}",
        handle2.lines
    );

    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(
        task.status, "in-review",
        "task must remain in-review across double restart. Lines: {:?}",
        handle2.lines
    );
    drop(conn);

    handle2.sigkill();
}

/// Awaiting-review journal row must survive shutdown teardown — the task
/// stays in-review (not silently reset to open) across a graceful stop.
#[test]
fn in_review_journal_row_survives_shutdown() {
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

    seed_task(home.path(), "Task for shutdown survival");

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

    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    assert!(
        handle.wait_for("PR #1 ready for review", 15),
        "PR not acknowledged. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(
        task.status, "in-review",
        "task should remain in-review after shutdown, not silently reset to open"
    );
}

/// Exit-75 (self-update) recovery: journal row survives, task stays in-review,
/// Phase 5b provisions a reviewer — no duplicate worker execution.
#[test]
fn exit75_in_review_recovered_without_worker_respawn() {
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

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");

    let fake_wt = wt_base.path().join("Agent0");
    std::fs::create_dir_all(&fake_wt).unwrap();
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let now = 1000;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "Exit-75 lease expiry test",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test-classifier:v2"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        assert_eq!(id, 1);

        quorum_core::tasks::claim(&mut conn, "Agent0", Some(id), &[], 100, now).unwrap();

        quorum_core::tasks::apply_event(
            &mut conn,
            "Agent0",
            id,
            &quorum_core::lifecycle::Event::SignaledDone {
                pr: "99".to_string(),
            },
            now + 1,
        )
        .unwrap();

        let task = quorum_core::tasks::get(&conn, id).unwrap().unwrap();
        assert_eq!(task.status, "in-review");

        let entry = quorum_core::journal::JournalEntry {
            agent: "Agent0".into(),
            role: "worker".into(),
            task_id: Some(id),
            session_id: "sess-exit75".into(),
            worktree: Some(fake_wt.to_string_lossy().into()),
            branch: Some("feat/test-exit75".into()),
            phase: "awaiting-review".into(),
            cost_tokens: 500,
            agent_state: None,
            cost_usd: 0.01,
            log_dir: None,
            pid: None,
            pr: Some(99),
            rework_count: 0,
        };
        quorum_core::journal::upsert(&mut conn, &entry).unwrap();
    }

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Stateless recovery: wipes journal, GCs worktrees, leaves in-review as-is.
    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );
    handle.wait_for_completed_tick(home.path());

    // Task stays in-review.
    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(
        task.status, "in-review",
        "task must stay in-review despite expired claim lease. Lines: {:?}",
        handle.lines
    );
    drop(conn);

    // No fresh worker for task #1 — Phase 5b handles it via reviewer provisioning.
    let fresh_spawns = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent") && l.contains("task #1"))
        .count();
    assert_eq!(
        fresh_spawns, 0,
        "exit-75 recovery must not re-execute the task. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

// ── Crash-matrix integration tests ──────────────────────────────────────────

struct TestEnv {
    home: tempfile::TempDir,
    repo_dir: tempfile::TempDir,
    wt_base: tempfile::TempDir,
    names_file: std::path::PathBuf,
    db_path: std::path::PathBuf,
}

impl TestEnv {
    fn new() -> Self {
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

        let db_path = home
            .path()
            .join("repos")
            .join("test__repo")
            .join("quorum.db");

        TestEnv {
            home,
            repo_dir,
            wt_base,
            names_file,
            db_path,
        }
    }

    fn start_serve(&self) -> ServeHandle {
        ServeHandle::start(
            self.home.path(),
            self.repo_dir.path(),
            self.wt_base.path(),
            &self.names_file,
        )
    }

    fn start_serve_with_agent_bin(&self, bin: &str) -> ServeHandle {
        ServeHandle::start_with_agent_bin(
            self.home.path(),
            self.repo_dir.path(),
            self.wt_base.path(),
            &self.names_file,
            bin,
        )
    }

    fn seed_claimed_task(&self, title: &str, agent: &str) -> i64 {
        let mut conn = quorum_core::db::open(&self.db_path).unwrap();
        let now = quorum_core::clock::now();
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            title,
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test-classifier:v2"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        quorum_core::tasks::claim(&mut conn, agent, Some(id), &[], 86400, now).unwrap();
        id
    }

    fn seed_journal(&self, entry: &quorum_core::journal::JournalEntry) {
        let mut conn = quorum_core::db::open(&self.db_path).unwrap();
        quorum_core::journal::upsert(&mut conn, entry).unwrap();
    }

    fn task_status(&self, id: i64) -> String {
        let conn = quorum_core::db::open(&self.db_path).unwrap();
        quorum_core::tasks::get(&conn, id).unwrap().unwrap().status
    }

    fn journal_exists(&self, agent: &str) -> bool {
        let conn = quorum_core::db::open(&self.db_path).unwrap();
        let entries = quorum_core::journal::list_in_flight(&conn).unwrap();
        entries.iter().any(|e| e.agent == agent)
    }
}

fn make_journal_entry(
    agent: &str,
    role: &str,
    phase: &str,
    task_id: Option<i64>,
    worktree: Option<&str>,
    pr: Option<i64>,
) -> quorum_core::journal::JournalEntry {
    quorum_core::journal::JournalEntry {
        agent: agent.into(),
        role: role.into(),
        task_id,
        session_id: "sess-test".into(),
        worktree: worktree.map(Into::into),
        branch: Some("feat/test-branch".into()),
        phase: phase.into(),
        cost_tokens: 100,
        agent_state: None,
        cost_usd: 0.01,
        log_dir: None,
        pid: None,
        pr,
        rework_count: 0,
    }
}

/// Stateless recovery: working task with journal entry → AgentFailed → open,
/// journal wiped, worktree GC'd, Phase 6 re-spawns a fresh worker.
#[test]
fn recovery_working_task_reset_to_open_and_respawned() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Working task with worktree", "Agent0");

    let wt = env.wt_base.path().join("Agent0");
    std::fs::create_dir_all(&wt).unwrap();

    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "working",
        Some(id),
        Some(&wt.to_string_lossy()),
        None,
    ));

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("working task #1 -> open via AgentFailed", 15),
        "recovery did not reset working task to open. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // Journal must be wiped.
    assert!(
        !env.journal_exists("Agent0"),
        "journal entry should be deleted by stateless recovery"
    );

    // Phase 6 should re-spawn a fresh worker for the open task.
    assert!(
        handle.wait_for("spawning agent", 15),
        "Phase 6 did not re-spawn a worker for the open task. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Stateless recovery: working task with missing worktree → same behavior
/// as with existing worktree (journal wiped, task → open).
#[test]
fn recovery_working_task_missing_worktree_reset_to_open() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Working task missing worktree", "Agent0");

    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "working",
        Some(id),
        Some("/nonexistent/worktree/path"),
        None,
    ));

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    assert_eq!(
        env.task_status(id),
        "open",
        "task should revert to open via AgentFailed"
    );

    assert!(
        !env.journal_exists("Agent0"),
        "journal entry should be deleted by stateless recovery"
    );

    handle.sigkill();
}

/// Stateless recovery: all journal entries are wiped regardless of role.
#[test]
fn recovery_all_journal_entries_wiped() {
    let env = TestEnv::new();

    env.seed_journal(&make_journal_entry(
        "Rev0",
        "reviewer",
        "reviewing",
        Some(42),
        None,
        None,
    ));
    env.seed_journal(&make_journal_entry(
        "X0", "observer", "watching", None, None, None,
    ));

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    assert!(
        !env.journal_exists("Rev0"),
        "reviewer journal entry should be deleted"
    );
    assert!(
        !env.journal_exists("X0"),
        "unknown-role journal entry should be deleted"
    );

    handle.sigkill();
}

/// Phase 3: orphaned worktree directories are GC'd (all worktrees are orphans
/// after journal wipe).
#[test]
fn recovery_orphaned_worktree_gc() {
    let env = TestEnv::new();

    let orphan_dir = env.wt_base.path().join("orphan-stale-wt");
    std::fs::create_dir_all(&orphan_dir).unwrap();
    assert!(orphan_dir.exists());

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("recovery: GC'd 1 worktree(s)", 15),
        "recovery did not GC orphaned worktree. Lines: {:?}",
        handle.lines
    );

    assert!(
        !orphan_dir.exists(),
        "orphaned worktree directory should have been removed"
    );

    handle.sigkill();
}

/// F9: stale mailbox rows for a re-spawned agent name are drained during
/// Phase 6 spawn_worker (not during recovery, but before the worker's
/// first turn).
#[test]
fn recovery_stale_mailbox_drained_on_respawn() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Mailbox drain task", "Agent0");

    let wt = env.wt_base.path().join("Agent0");
    std::fs::create_dir_all(&wt).unwrap();

    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "working",
        Some(id),
        Some(&wt.to_string_lossy()),
        None,
    ));

    // Seed stale mailbox rows for Agent0 (as if the agent signaled before crash).
    {
        let mut conn = quorum_core::db::open(&env.db_path).unwrap();
        let row = quorum_core::mailbox::MailboxRow {
            agent: "Agent0".into(),
            kind: quorum_core::mailbox::MailboxKind::Message,
            task_id: Some(id),
            pr: None,
            verdict: None,
            feedback: None,
            note: Some("stale msg".into()),
            to_agent: None,
            payload: None,
        };
        quorum_core::mailbox::append(&mut conn, &row).unwrap();
        quorum_core::mailbox::append(&mut conn, &row).unwrap();
    }

    let mut handle = env.start_serve();

    // Recovery resets task to open. With no live Agent0 slot yet, Phase 4c
    // consumes both stale messages before Phase 6 logs the replacement spawn.
    for _ in 0..2 {
        assert!(
            handle.wait_for("consuming message from Agent0 with no to_agent", 15),
            "stale mailbox row was not consumed. Lines: {:?}",
            handle.lines
        );
    }
    assert!(
        handle.wait_for("spawning agent Agent0", 15),
        "Phase 6 did not re-spawn Agent0. Lines: {:?}",
        handle.lines
    );

    // Verify the stale mailbox rows are consumed (no unconsumed rows remain).
    {
        let conn = quorum_core::db::open(&env.db_path).unwrap();
        let unconsumed = quorum_core::mailbox::poll_unconsumed(&conn).unwrap();
        let agent0_unconsumed: Vec<_> = unconsumed
            .iter()
            .filter(|(_, r)| r.agent == "Agent0")
            .collect();
        assert!(
            agent0_unconsumed.is_empty(),
            "stale mailbox rows should be consumed, found {} unconsumed",
            agent0_unconsumed.len()
        );
    }

    handle.sigkill();
}

/// Stateless recovery resets working task to open; with a bad agent binary
/// Phase 6's spawn attempt fails but the task stays open (not stuck).
#[test]
fn recovery_bad_agent_binary_task_stays_open() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Spawn failure task", "Agent0");

    let wt = env.wt_base.path().join("Agent0");
    std::fs::create_dir_all(&wt).unwrap();

    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "working",
        Some(id),
        Some(&wt.to_string_lossy()),
        None,
    ));

    let mut handle = env.start_serve_with_agent_bin("/nonexistent/agent/binary");

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // Task should be open (reset by recovery's AgentFailed).
    assert_eq!(
        env.task_status(id),
        "open",
        "task should be open after recovery"
    );

    handle.sigkill();
}

/// After stateless recovery, a worker spawned by Phase 6 that exits
/// immediately is detected by the tick loop and torn down.
#[test]
fn recovery_respawned_agent_dies_detected_by_tick_loop() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Die-after-respawn task", "Agent0");

    let wt = env.wt_base.path().join("Agent0");
    std::fs::create_dir_all(&wt).unwrap();

    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "working",
        Some(id),
        Some(&wt.to_string_lossy()),
        None,
    ));

    let bad_agent = env.home.path().join("exit-agent.sh");
    std::fs::write(&bad_agent, "#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bad_agent, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let mut handle = env.start_serve_with_agent_bin(&bad_agent.to_string_lossy());

    // Recovery resets task to open, Phase 6 spawns worker, worker dies immediately.
    assert!(
        handle.wait_for("died mid-task", 15),
        "tick loop did not detect dead worker. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("tearing down worker", 15),
        "worker teardown not seen. Lines: {:?}",
        handle.lines
    );

    handle.drain_pending_lines();
    let status = env.task_status(id);
    assert!(
        status == "open" || status == "cancelled",
        "task should be open or cancelled after agent dies, got: {status}. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Stateless recovery: multiple journal entries of different roles are all
/// wiped, and working tasks are reset to open.
#[test]
fn recovery_mixed_entries_all_wiped() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Mixed recovery task", "Agent0");

    let wt = env.wt_base.path().join("Agent0");
    std::fs::create_dir_all(&wt).unwrap();

    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "working",
        Some(id),
        Some(&wt.to_string_lossy()),
        None,
    ));
    env.seed_journal(&make_journal_entry(
        "Rev0",
        "reviewer",
        "reviewing",
        Some(99),
        None,
        None,
    ));

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("recovery: complete", 30),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // Both journal entries wiped.
    assert!(!env.journal_exists("Agent0"));
    assert!(!env.journal_exists("Rev0"));

    // Working task reset to open.
    assert_eq!(env.task_status(id), "open");

    handle.sigkill();
}

/// Stateless recovery: journal phase (awaiting-review) is irrelevant — what
/// matters is the DB task status. A working task with an awaiting-review
/// journal entry is still reset to open via AgentFailed.
#[test]
fn recovery_journal_phase_irrelevant_db_status_drives_reset() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Awaiting review no PR", "Agent0");

    let wt = env.wt_base.path().join("Agent0");
    std::fs::create_dir_all(&wt).unwrap();

    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "awaiting-review",
        Some(id),
        Some(&wt.to_string_lossy()),
        None,
    ));

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // Journal wiped regardless of phase.
    assert!(
        !env.journal_exists("Agent0"),
        "journal entry should be wiped by stateless recovery"
    );

    // Task was "working" in DB → open via AgentFailed → Phase 6 re-spawns.
    assert!(
        handle.wait_for("spawning agent", 15),
        "Phase 6 did not re-spawn a worker. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Recovery with surviving branch: worktree GC'd but branch persists.
/// Phase 6 must reuse the existing branch (not fail on `-b`).
#[test]
fn recovery_surviving_branch_reused_on_respawn() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Surviving branch task", "Agent0");

    // Create a real git branch in the repo (simulates what the prior worker created).
    let d = env.repo_dir.path().to_string_lossy().to_string();
    let branch = format!("daemon/agent0-t{id}");
    Command::new("git")
        .args(["-C", &d, "checkout", "-b", &branch])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "commit", "--allow-empty", "-m", "worker WIP"])
        .status()
        .unwrap();
    // Switch back to main
    Command::new("git")
        .args(["-C", &d, "checkout", "main"])
        .status()
        .unwrap();

    // Create orphan worktree dir (will be GC'd) and journal entry
    let wt = env.wt_base.path().join(format!("Agent0-t{id}"));
    std::fs::create_dir_all(&wt).unwrap();

    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "working",
        Some(id),
        Some(&wt.to_string_lossy()),
        None,
    ));

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // The branch survives recovery (only worktrees are GC'd).
    // Phase 6 re-spawns: provision() must reuse the branch, not fail.
    assert!(
        handle.wait_for("worktree provisioned", 20),
        "worker worktree should be provisioned despite surviving branch. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}
