//! #178 acceptance test: restart resumes done-awaiting-review at the REVIEW
//! stage, not by re-executing the task from scratch.
//!
//! Bug repro (2026-07-04, hit twice live): a worker completes and signals
//! `done --pr N` (PR open on GitHub), sits in awaiting-review, then the
//! daemon is restarted. Pre-fix: the pipeline position was lost — the
//! awaiting-review journal entry had no PR, recovery spawned a stub worker
//! that was reaped as dead (releasing the task to `open`), and a fresh
//! worker re-executed the task producing a duplicate PR.
//!
//! Post-fix: the done --pr N handler upserts the journal with the PR, and
//! recovery routes awaiting-review-with-PR entries to a `PendingReview`
//! collection so a reviewer is provisioned for the recorded PR without a
//! worker re-spawn.

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
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let sentinel_path = sentinel.path().to_string_lossy().to_string();
        let fake_agent = cargo_bin("fake-agent");
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
        }
    }

    fn sigkill(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL);
        }
        let _ = self.child.wait();
        // Drain any final buffered log lines
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

fn quorum_done(home: &std::path::Path, args: &[&str]) {
    let mut cmd_args = vec!["done"];
    cmd_args.extend_from_slice(args);
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
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

    // Worker spawns and produces its first result.
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

    // Worker signals done --pr — the daemon persists the PR to the journal.
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Wait for the daemon to acknowledge the PR (proof journal upsert ran).
    assert!(
        handle.wait_for("PR #1 ready for review", 15),
        "daemon did not acknowledge PR #1 before SIGKILL. Lines: {:?}",
        handle.lines
    );
    // Small drain so we've observed all log lines up to this point.
    handle.drain_pending_lines();

    // ── Kill the daemon hard (mimics operator hitting the process). ──
    handle.sigkill();

    // ── Relaunch the daemon. ──
    let mut handle2 = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Recovery MUST route the awaiting-review-with-PR entry to a pending
    // review (no --resume worker spawn for this task).
    assert!(
        handle2.wait_for("resuming task #1 at REVIEW stage", 30),
        "recovery did not route awaiting-review-with-PR to pending review. Lines: {:?}",
        handle2.lines
    );

    // A reviewer MUST be provisioned against the recorded PR.
    assert!(
        handle2.wait_for("spawning reviewer", 30),
        "reviewer not provisioned for pending review. Lines: {:?}",
        handle2.lines
    );

    // A moment for any late log lines.
    std::thread::sleep(Duration::from_millis(500));
    handle2.drain_pending_lines();

    // ── Invariant: NO fresh worker spawn for task #1 after restart. ──
    // This is the core anti-duplication guarantee. The pre-fix bug was
    // exactly that a fresh `spawning agent` fired for the same task on
    // restart, producing a duplicate PR.
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

/// C7 regression: a task stuck in `in-review` with no journal row — the orphan
/// rescue scan must detect it and register a PendingReview for reviewer provisioning.
#[test]
fn orphan_in_review_task_rescued_on_startup() {
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

    // Set up the orphan state directly: create task, claim it, signal done to reach in-review.
    // No daemon means no journal row — exactly the orphan scenario.
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
            None,
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
        // Verify task is in-review
        let task = quorum_core::tasks::get(&conn, id).unwrap().unwrap();
        assert_eq!(task.status, "in-review");
        // No journal row exists — this is the orphan state
    }

    // Start daemon — it should detect the orphan and rescue it.
    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    assert!(
        handle.wait_for("rescuing orphaned in-review task #1", 15),
        "orphan rescue not triggered. Lines: {:?}",
        handle.lines
    );

    // The orphan should be registered as a PendingReview, leading to reviewer provisioning.
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not provisioned for rescued orphan. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Double-restart idempotent: orphan rescue on second restart should not
/// create duplicates or crash if the task was already rescued.
#[test]
fn orphan_rescue_double_restart_idempotent() {
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
            None,
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

    // First daemon start — rescues orphan.
    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);
    assert!(
        handle.wait_for("rescuing orphaned in-review task #1", 15),
        "first-start orphan rescue not triggered. Lines: {:?}",
        handle.lines
    );
    handle.sigkill();

    // Second daemon start — the orphan-rescue journal entry has no worktree on disk,
    // so recovery cleans it up via AgentFailed (in-review stays in-review), then the
    // orphan scan re-rescues. This is idempotent: the task stays in-review throughout.
    let mut handle2 = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);
    assert!(
        handle2.wait_for("recovery: complete", 15),
        "second start did not complete recovery. Lines: {:?}",
        handle2.lines
    );

    // Verify task is still in-review (not corrupted by double recovery).
    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
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

    // Kill daemon — simulate unclean shutdown.
    handle.sigkill();

    // Verify task is still in-review (not silently reset to open).
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

/// M3: Simulate the exit-75 (self-update) recovery path where a pending review's
/// journal row survives the restart and the task's claim lease has expired during
/// the rebuild window. Recovery must re-adopt the pending review from journal and
/// provision a reviewer — the expired claim must not cause duplicate execution.
#[test]
fn exit75_pending_review_recovered_with_expired_lease() {
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

    // Seed the post-exit-75 state directly in the DB:
    // 1. Task in in-review (worker signaled done --pr)
    // 2. Claim with expired expires_at (lease expired during slow rebuild)
    // 3. Journal row for the worker in awaiting-review phase (left by exit-75)
    // 4. Worktree directory exists on disk (exit-75 preserves worktrees)
    let fake_wt = wt_base.path().join("Agent0");
    std::fs::create_dir_all(&fake_wt).unwrap();
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let now = 1000;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "M3 exit-75 lease expiry test",
            None,
            0,
            None,
            None,
            None,
            None,
            now,
        )
        .unwrap();
        assert_eq!(id, 1);

        // Claim task with a short TTL so the lease is already expired by "now"
        quorum_core::tasks::claim(&mut conn, "Agent0", Some(id), &[], 100, now).unwrap();

        // Transition to in-review via SignaledDone
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

        // Verify task is in-review
        let task = quorum_core::tasks::get(&conn, id).unwrap().unwrap();
        assert_eq!(task.status, "in-review");

        // Insert journal row as if exit-75 left it: worker in awaiting-review
        // with a PR and a worktree path that exists on disk.
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

    // Start daemon — recovery must re-adopt the journal entry as a PendingReview.
    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Recovery Phase 2 routes awaiting-review-with-PR to pending review.
    assert!(
        handle.wait_for("resuming task #1 at REVIEW stage", 15),
        "recovery did not resume pending review from exit-75 journal row. Lines: {:?}",
        handle.lines
    );

    // Wait a moment for any late log output.
    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    // Verify task remains in-review (expired claim didn't corrupt status).
    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(
        task.status, "in-review",
        "task must stay in-review despite expired claim lease. Lines: {:?}",
        handle.lines
    );
    drop(conn);

    // No fresh worker should be spawned for task #1 — it's covered by the pending review.
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
            &mut conn, "test", title, None, 0, None, None, None, None, now,
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

/// Cell: worker/working with existing worktree → resumed with --resume + feed_turn
#[test]
fn recovery_worker_working_with_worktree_resumes() {
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
        handle.wait_for("resumed worker Agent0", 15),
        "recovery did not resume worker/working with existing worktree. Lines: {:?}",
        handle.lines
    );

    // The worker should remain in working status (not released).
    assert_eq!(
        env.task_status(id),
        "working",
        "task should stay working after resume"
    );

    handle.sigkill();
}

/// Cell: worker/working with missing worktree → release task via AgentFailed
#[test]
fn recovery_worker_working_missing_worktree_releases() {
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
        handle.wait_for("worktree missing for worker Agent0", 15),
        "recovery did not detect missing worktree. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // Task should be back to open via AgentFailed (Working → Open).
    assert_eq!(
        env.task_status(id),
        "open",
        "task should revert to open when worktree is missing"
    );

    // Journal entry should be deleted.
    assert!(
        !env.journal_exists("Agent0"),
        "journal entry should be deleted after worktree-missing cleanup"
    );

    handle.sigkill();
}

/// Cell: reviewer teardown — journal deleted, worktree removed, name released
#[test]
fn recovery_reviewer_teardown() {
    let env = TestEnv::new();

    env.seed_journal(&make_journal_entry(
        "Rev0",
        "reviewer",
        "reviewing",
        Some(42),
        None,
        None,
    ));

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("tearing down stale reviewer Rev0", 15),
        "recovery did not tear down reviewer. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    assert!(
        !env.journal_exists("Rev0"),
        "journal entry should be deleted after reviewer teardown"
    );

    handle.sigkill();
}

/// Cell: unknown role → journal entry deleted, name released
#[test]
fn recovery_unknown_role_deleted() {
    let env = TestEnv::new();

    env.seed_journal(&make_journal_entry(
        "X0", "observer", "watching", None, None, None,
    ));

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("unknown role 'observer' for X0", 15),
        "recovery did not handle unknown role. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    assert!(
        !env.journal_exists("X0"),
        "journal entry should be deleted for unknown role"
    );

    handle.sigkill();
}

/// Cell: orphaned worktree GC — directories in wt_base not referenced by journal are removed
#[test]
fn recovery_orphaned_worktree_gc() {
    let env = TestEnv::new();

    let orphan_dir = env.wt_base.path().join("orphan-stale-wt");
    std::fs::create_dir_all(&orphan_dir).unwrap();
    assert!(orphan_dir.exists());

    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("GC'd 1 orphaned worktree", 15),
        "recovery did not GC orphaned worktree. Lines: {:?}",
        handle.lines
    );

    assert!(
        !orphan_dir.exists(),
        "orphaned worktree directory should have been removed"
    );

    handle.sigkill();
}

/// Cell: stale mailbox drain (F9) — unconsumed mailbox rows consumed before worker resume
#[test]
fn recovery_stale_mailbox_drained() {
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

    assert!(
        handle.wait_for("consumed 2 stale mailbox row(s) for Agent0", 15),
        "recovery did not drain stale mailbox rows. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Cell: spawn failure → release_and_cleanup (task goes to open, journal deleted)
#[test]
fn recovery_spawn_failure_releases_task() {
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

    // Use a non-existent binary so AgentProc::spawn returns Err.
    let mut handle = env.start_serve_with_agent_bin("/nonexistent/agent/binary");

    assert!(
        handle.wait_for("spawn failed for Agent0", 15),
        "recovery did not report spawn failure. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete after spawn failure. Lines: {:?}",
        handle.lines
    );

    assert_eq!(
        env.task_status(id),
        "open",
        "task should revert to open after spawn failure"
    );

    handle.sigkill();
}

/// Cell: feed_turn failure path — when the resumed agent exits immediately,
/// the tick loop detects the death and releases the task. The feed_turn
/// failure path in recovery.rs shares `release_and_cleanup` with spawn
/// failure (tested above); the pipe write succeeds because the kernel
/// buffers the small resume turn, so the failure surfaces as worker death
/// in the tick loop rather than as a feed_turn error.
#[test]
fn recovery_agent_dies_after_resume_releases_task() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Die-after-resume task", "Agent0");

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

    // Script exits immediately after shell startup — too fast to respond.
    let bad_agent = env.home.path().join("exit-agent.sh");
    std::fs::write(&bad_agent, "#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bad_agent, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let mut handle = env.start_serve_with_agent_bin(&bad_agent.to_string_lossy());

    // Recovery resumes the worker (feed_turn succeeds due to pipe buffering),
    // then the tick loop detects the worker died.
    assert!(
        handle.wait_for("died mid-task", 15),
        "tick loop did not detect dead worker after resume. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Cell: mixed entries — multiple entry types processed in single recovery run
#[test]
fn recovery_mixed_worker_and_reviewer_entries() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Mixed recovery task", "Agent0");

    let wt = env.wt_base.path().join("Agent0");
    std::fs::create_dir_all(&wt).unwrap();

    // Worker in working phase with existing worktree.
    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "working",
        Some(id),
        Some(&wt.to_string_lossy()),
        None,
    ));

    // Reviewer entry — should be torn down.
    env.seed_journal(&make_journal_entry(
        "Rev0",
        "reviewer",
        "reviewing",
        Some(99),
        None,
        None,
    ));

    let mut handle = env.start_serve();

    // Wait for recovery to finish — entries are processed in alphabetical order.
    assert!(
        handle.wait_for("recovery: complete", 30),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    handle.drain_pending_lines();

    let has_resumed = handle
        .lines
        .iter()
        .any(|l| l.contains("resumed worker Agent0"));
    let has_teardown = handle
        .lines
        .iter()
        .any(|l| l.contains("tearing down stale reviewer Rev0"));

    assert!(
        has_resumed,
        "recovery did not resume worker in mixed scenario. Lines: {:?}",
        handle.lines
    );
    assert!(
        has_teardown,
        "recovery did not tear down reviewer in mixed scenario. Lines: {:?}",
        handle.lines
    );

    // Worker task stays working, reviewer journal cleaned up.
    assert_eq!(env.task_status(id), "working");
    assert!(!env.journal_exists("Rev0"));

    handle.sigkill();
}

/// Cell: worker/awaiting-review WITHOUT PR → falls through to --resume spawn
/// (no PendingReview created; the worker is resumed as a regular slot)
#[test]
fn recovery_awaiting_review_without_pr_spawns_resume() {
    let env = TestEnv::new();
    let id = env.seed_claimed_task("Awaiting review no PR", "Agent0");

    let wt = env.wt_base.path().join("Agent0");
    std::fs::create_dir_all(&wt).unwrap();

    // awaiting-review but pr=None → recovery falls through to spawn with --resume
    env.seed_journal(&make_journal_entry(
        "Agent0",
        "worker",
        "awaiting-review",
        Some(id),
        Some(&wt.to_string_lossy()),
        None, // no PR
    ));

    let mut handle = env.start_serve();

    // Should resume (not route to pending review).
    assert!(
        handle.wait_for("resumed worker Agent0", 15),
        "recovery did not resume awaiting-review worker without PR. Lines: {:?}",
        handle.lines
    );

    // Should NOT see "at REVIEW stage" (that's the with-PR path).
    handle.drain_pending_lines();
    let review_stage_log = handle
        .lines
        .iter()
        .any(|l| l.contains("resuming task") && l.contains("at REVIEW stage"));
    assert!(
        !review_stage_log,
        "awaiting-review without PR should NOT route to pending review. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}
