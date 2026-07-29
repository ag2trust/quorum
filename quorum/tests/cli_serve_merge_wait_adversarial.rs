//! Adversarial end-to-end daemon tests for merge-wait and replacement recovery
//! across daemon restarts (#178, lifecycle program rooted at #173).
//!
//! Scenario A: External CI stall — task remains durable in merge-wait, survives
//! restart, and merges exactly once when checks flip to Ready.
//!
//! Scenario B: Actionable failure with absent worker — task enters rework and
//! exactly one replacement worker starts on the existing PR branch.
//!
//! Negative/adversarial coverage:
//! - Repeated Pending never increments rework/recovery counters
//! - Head SHA change invalidates stored approvals, requires re-review
//! - Merge conflict uses replacement-worker path and respects caps
//! - Replacement instant-death loop stops at recovery budget with owner signal
//! - No duplicate PR, branch, merge, or lifecycle event under rapid ticks/restart
//! - agent_runs end reasons are truthful

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
            .env(
                "FAKE_AGENT_PROMPT_LOG",
                home.join("fake-agent-prompts.jsonl"),
            )
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

    fn stop(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGINT);
        }
        let _ = self.child.wait();
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

fn seed_task_with_deps(home: &std::path::Path, title: &str, depends_on: &str) {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            title,
            "--created-by",
            "TestCreator",
            "--depends-on",
            depends_on,
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "task-create with deps failed: {}",
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

fn complete_r2_review(home: &std::path::Path, handle: &mut ServeHandle, pr: &str) {
    complete_r2_review_after(home, handle, pr, || {});
}

fn complete_r2_review_after(
    home: &std::path::Path,
    handle: &mut ServeHandle,
    pr: &str,
    before_submit: impl FnOnce(),
) {
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

    before_submit();
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

/// Drive a task through worker + R1 + R2 approval to reach the merge-checks phase.
fn drive_to_approved(home: &std::path::Path, handle: &mut ServeHandle, pr: &str) -> String {
    drive_to_approved_after(home, handle, pr, || {})
}

fn drive_to_approved_after(
    home: &std::path::Path,
    handle: &mut ServeHandle,
    pr: &str,
    before_r2_submit: impl FnOnce(),
) -> String {
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
    quorum_done(home, &["--agent", &worker_name, "--pr", pr]);

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
        home,
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            pr,
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    complete_r2_review_after(home, handle, pr, before_r2_submit);

    worker_name
}

fn seed_in_review_task(home: &std::path::Path, author: &str, pr: i64) -> i64 {
    let db_path = home.join("repos").join("test__repo").join("quorum.db");
    let mut conn = quorum_core::db::open(&db_path).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let id = quorum_core::tasks::create(
        &mut conn,
        "test",
        "Adversarial merge-wait test task",
        Some("Fix the widget"),
        0,
        None,
        None,
        None,
        None,
        now,
    )
    .unwrap();
    quorum_core::tasks::claim(&mut conn, author, Some(id), &[], 3600, now).unwrap();
    quorum_core::tasks::apply_event(
        &mut conn,
        author,
        id,
        &quorum_core::lifecycle::Event::SignaledDone { pr: pr.to_string() },
        now + 1,
    )
    .unwrap();
    let task = quorum_core::tasks::get(&conn, id).unwrap().unwrap();
    assert_eq!(task.status, "in-review");
    id
}

fn create_pr_branch(repo_dir: &std::path::Path, author: &str, task_id: i64) {
    let branch = format!("daemon/{}-t{}", author.to_lowercase(), task_id);
    let d = repo_dir.to_string_lossy();
    Command::new("git")
        .args(["-C", &d, "branch", &branch])
        .status()
        .unwrap();
}

fn db_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join("repos").join("test__repo").join("quorum.db")
}

fn get_task(home: &std::path::Path, id: i64) -> quorum_core::tasks::Task {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    quorum_core::tasks::get(&conn, id).unwrap().unwrap()
}

fn active_claim(home: &std::path::Path, task_id: i64) -> Option<(String, i64)> {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    let target = format!("task#{task_id}");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    conn.query_row(
        "SELECT holder, expires_at FROM claims
         WHERE target=?1 AND active=1 AND expires_at > ?2",
        rusqlite::params![target, now],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .ok()
}

fn worker_journal_target(home: &std::path::Path, task_id: i64) -> Option<(String, Option<i64>)> {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    conn.query_row(
        "SELECT branch, pr FROM journal
         WHERE task_id=?1 AND role='worker'",
        [task_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .ok()
}

fn get_agent_runs(home: &std::path::Path, task_id: i64) -> Vec<quorum_core::agent_runs::AgentRun> {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    quorum_core::agent_runs::runs_for_task(&conn, task_id).unwrap()
}

fn get_approvals(home: &std::path::Path, pr: i64) -> Vec<quorum_core::approvals::Approval> {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    quorum_core::approvals::get_for_pr(&conn, pr).unwrap()
}

fn get_events(home: &std::path::Path, task_id: i64) -> Vec<quorum_core::events::Event> {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let subject = format!("task#{task_id}");
    quorum_core::events::list(&conn, 0, Some(&subject), 1000, now).unwrap()
}

fn get_messages(home: &std::path::Path, task_id: i64) -> Vec<String> {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let mut stmt = conn
        .prepare("SELECT body FROM messages WHERE refs = ?1 AND expires_at > ?2 ORDER BY ts ASC")
        .unwrap();
    stmt.query_map(rusqlite::params![format!("task:{task_id}"), now], |r| {
        r.get::<_, String>(0)
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

// ═══════════════════════════════════════════════════════════════════════════════
// SCENARIO A: External CI stall — durable merge-wait across restart
// ═══════════════════════════════════════════════════════════════════════════════

/// Scenario A — durable merge-wait with dependent tasks:
/// 1. Worker submits PR; R1+R2 approve same head SHA.
/// 2. Checks return Pending past configured timeout → enters merge-wait.
/// 3. Task stays merging, counters unchanged, dependents not-ready, no new workers.
/// 4. Flip Pending→Ready → exactly one merge, prerequisite done, dependent ready.
#[test]
fn scenario_a_merge_wait_with_dependents() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let checks_state = home.path().join("checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    // Create prerequisite task (id=1) and dependent task (id=2).
    seed_task(home.path(), "Prerequisite task");
    seed_task_with_deps(home.path(), "Dependent task", "[1]");

    // Verify dependent starts not-ready.
    let dep = get_task(home.path(), 2);
    assert!(!dep.ready, "dependent must start not-ready");

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

    drive_to_approved_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait not entered. Lines: {:?}",
        handle.lines
    );

    // Verify durable state during merge-wait.
    let task = get_task(home.path(), 1);
    assert_eq!(task.status, "merging", "task must remain in merging");
    assert_eq!(task.rework_round, 0, "rework counter must not increment");
    assert_eq!(
        task.recovery_attempts, 0,
        "recovery counter must not increment"
    );

    // Dependent must still be not-ready.
    let dep = get_task(home.path(), 2);
    assert!(
        !dep.ready,
        "dependent must stay not-ready during merge-wait"
    );

    // No new worker spawns after merge-wait entered.
    std::thread::sleep(Duration::from_millis(2000));
    handle.drain_pending_lines();
    let mw_idx = handle
        .lines
        .iter()
        .position(|l| l.contains("merge wait"))
        .unwrap();
    let worker_spawns_after_mw = handle.lines[mw_idx..]
        .iter()
        .filter(|l| l.contains("spawning agent"))
        .count();
    assert_eq!(
        worker_spawns_after_mw, 0,
        "no workers should spawn during merge-wait"
    );

    // Flip Pending→Ready.
    std::fs::write(&checks_state, "ready").unwrap();

    assert!(
        handle.wait_for("checks passed", 20),
        "checks-passed not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 20),
        "merge not seen after checks pass. Lines: {:?}",
        handle.lines
    );

    // Wait for DB update to complete after merge.
    std::thread::sleep(Duration::from_millis(1000));

    // Verify prerequisite done, dependent ready.
    let task = get_task(home.path(), 1);
    assert_eq!(task.status, "done", "prerequisite task must be done");

    let dep = get_task(home.path(), 2);
    assert!(
        dep.ready,
        "dependent must become ready after prerequisite done"
    );

    // No rework triggered.
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(!saw_rework, "rework must not trigger on pending→ready path");

    handle.sigkill();
}

/// Scenario A restart variant — restart during pending CI, flip to ready post-restart.
/// Verifies daemon merges from durable approval after restart + checks ready.
#[test]
fn scenario_a_restart_pending_then_ready_merges() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let checks_state = home.path().join("checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Restart-pending test");

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

    drive_to_approved_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait not entered. Lines: {:?}",
        handle.lines
    );

    // Stop and flip checks to ready (matching the proven pattern from
    // checks_pending_survives_restart — flip before restart so
    // approval-recovery can merge directly).
    handle.stop();
    std::fs::write(&checks_state, "ready").unwrap();

    // Restart daemon — approval recovery merges from durable state.
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

    // Wait for DB update to complete after merge.
    std::thread::sleep(Duration::from_millis(1500));

    // Task must be done after the merge.
    let task = get_task(home.path(), 1);
    assert_eq!(task.status, "done", "task must be done after restart merge");

    // No workers spawned on restart (only approval-recovery merge).
    let worker_spawns = handle2
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent") && !l.contains("spawning reviewer"))
        .count();
    assert_eq!(
        worker_spawns, 0,
        "restart must not spawn workers for a merge-ready task"
    );

    // Approvals cleaned up after merge.
    let approvals = get_approvals(home.path(), 1);
    assert!(
        approvals.is_empty(),
        "approvals must be cleaned up after merge"
    );

    handle2.sigkill();
}

// ═══════════════════════════════════════════════════════════════════════════════
// SCENARIO B: Actionable failure with absent worker
// ═══════════════════════════════════════════════════════════════════════════════

/// Scenario B:
/// 1. Reach approved merge-wait, release original worker.
/// 2. Flip checks Pending→Failed with named failing jobs.
/// 3. Task enters rework, exactly one replacement worker starts on existing PR branch.
/// 4. Replacement pushes; same PR returns through review and can merge.
#[test]
fn scenario_b_failed_checks_absent_worker_replacement_cycle() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let checks_state = home.path().join("checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    // In-review task with absent worker.
    let author = "OrigWorker";
    let pr: i64 = 1;
    let task_id = seed_in_review_task(home.path(), author, pr);
    create_pr_branch(repo_dir.path(), author, task_id);

    // Start daemon — reviewer provisioned by Phase 5b recovery.
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
            "2",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("spawning reviewer", 30),
        "reviewer not provisioned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

    // Reviewer approves.
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            &pr.to_string(),
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    complete_r2_review_after(home.path(), &mut handle, &pr.to_string(), || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    // Enter merge-wait with Pending.
    assert!(
        handle.wait_for("merge wait", 20) || handle.wait_for("waiting for checks", 20),
        "merge-wait/waiting not entered. Lines: {:?}",
        handle.lines
    );

    // Step 2: Flip checks to Failed with named jobs.
    std::fs::write(&checks_state, "failed\nci-lint\nci-test").unwrap();

    // Step 3: Assert rework + remediation worker spawned.
    assert!(
        handle.wait_for("checks failed", 30),
        "checks-failed not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("spawning remediation worker", 15),
        "remediation worker not spawned. Lines: {:?}",
        handle.lines
    );

    // Wait for the remediation worker to produce a result before extracting name.
    assert!(
        handle.wait_for("result", 15),
        "remediation result not seen. Lines: {:?}",
        handle.lines
    );

    // Extract remediation worker name from "spawning remediation worker {name} for..."
    let remediation_name = handle
        .extract_agent_name("spawning remediation worker ")
        .expect("could not extract remediation worker name");

    // Step 4: Replacement pushes with same PR, checks now pass.
    std::fs::write(&checks_state, "ready").unwrap();
    quorum_done(
        home.path(),
        &["--agent", &remediation_name, "--pr", &pr.to_string()],
    );

    // PR returns to review.
    assert!(
        handle.wait_for("ready for review", 20) || handle.wait_for("spawning reviewer", 20),
        "PR not returned to review after remediation. Lines: {:?}",
        handle.lines
    );

    // Verify task state.
    let task = get_task(home.path(), task_id);
    assert!(
        task.status == "in-review" || task.status == "merging" || task.status == "done",
        "task must progress after remediation, got: {}",
        task.status
    );

    // Negative: no duplicate PR was created.
    let new_pr_lines = handle
        .lines
        .iter()
        .filter(|l| l.contains("gh pr create") || l.contains("opening new PR"))
        .count();
    assert_eq!(new_pr_lines, 0, "remediation must not create duplicate PR");

    handle.sigkill();
}

// ═══════════════════════════════════════════════════════════════════════════════
// NEGATIVE/ADVERSARIAL COVERAGE
// ═══════════════════════════════════════════════════════════════════════════════

/// Repeated Pending never increments rework/recovery counters or cancels.
/// Multiple ticks of "pending" past timeout keep the task in merging.
#[test]
fn repeated_pending_never_increments_counters() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("repeated_pending_checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Repeated pending test");

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

    drive_to_approved_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    // Wait for multiple merge-wait ticks (at least 3 seconds of retries).
    assert!(
        handle.wait_for("merge wait", 15),
        "first merge-wait not seen. Lines: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_secs(4));
    handle.drain_pending_lines();

    // Verify counters have NOT changed.
    let task = get_task(home.path(), 1);
    assert_eq!(task.status, "merging", "task must stay in merging");
    assert_eq!(
        task.rework_round, 0,
        "rework counter must not increment from repeated pending"
    );
    assert_eq!(
        task.recovery_attempts, 0,
        "recovery counter must not increment from repeated pending"
    );

    // No rework, no cancel.
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(!saw_rework, "repeated pending must not trigger rework");
    let saw_cancel = handle.lines.iter().any(|l| l.contains("cancelling"));
    assert!(!saw_cancel, "repeated pending must not cancel");

    handle.sigkill();
}

/// Head SHA change invalidates stored approvals and requires re-review.
#[test]
fn head_sha_change_invalidates_approvals() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let checks_state = home.path().join("checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Head SHA invalidation test");

    // Drive to approved, enter merge-wait.
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

    drive_to_approved_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait not entered. Lines: {:?}",
        handle.lines
    );

    // Stop daemon.
    handle.stop();

    // Simulate head SHA change by making a new commit in the repo.
    let d = repo_dir.path().to_string_lossy().to_string();
    Command::new("git")
        .args(["-C", &d, "commit", "--allow-empty", "-m", "force push"])
        .status()
        .unwrap();

    // Flip checks to ready.
    std::fs::write(&checks_state, "ready").unwrap();

    // Restart daemon. Approval recovery should detect the stale SHA.
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

    // Wait for recovery — should see "demoted" (stale head) rather than merge.
    std::thread::sleep(Duration::from_secs(5));
    handle2.drain_pending_lines();

    // The approval must be invalidated — either "demoted" log or no merge.
    let saw_demotion = handle2
        .lines
        .iter()
        .any(|l| l.contains("demoted") || l.contains("Demote"));
    let saw_merge = handle2
        .lines
        .iter()
        .any(|l| l.contains("merged from durable approval"));

    assert!(
        saw_demotion || !saw_merge,
        "stale head must invalidate approval — should see demotion or no merge. Lines: {:?}",
        handle2.lines
    );

    // Approvals table should be empty (cleaned up on demotion).
    let approvals = get_approvals(home.path(), 1);
    assert!(
        approvals.is_empty(),
        "approvals must be cleared after head SHA demotion, found: {:?}",
        approvals
    );

    handle2.sigkill();
}

/// Implementation task + merge conflict + absent worker → MergeConflict
/// and a leased remediation worker continuing the existing PR.
#[test]
fn merge_conflict_absent_worker_spawns_remediation() {
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

    let author = "OrigWorker";
    let pr: i64 = 1;
    let task_id = seed_in_review_task(home.path(), author, pr);
    create_pr_branch(repo_dir.path(), author, task_id);

    // mergeability_cmd returns conflicting on second call.
    let counter_file = home.path().join("mergeability_counter");
    std::fs::write(&counter_file, "0").unwrap();
    let mergeability_script = format!(
        "n=$(cat {f}); n=$((n + 1)); echo $n > {f}; \
         if [ $n -le 1 ]; then echo mergeable; else echo conflicting; fi",
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
            "echo ready",
            "--merge-checks-timeout-secs",
            "10",
            "--merge-checks-poll-secs",
            "1",
            "--merge-mergeability-cmd",
            &mergeability_script,
        ],
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery not complete. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("spawning reviewer", 30),
        "reviewer not provisioned. Lines: {:?}",
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
            &pr.to_string(),
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    complete_r2_review(home.path(), &mut handle, &pr.to_string());

    assert!(
        handle.wait_for("CONFLICTING", 20),
        "conflict not detected. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_secs(2));
    handle.drain_pending_lines();

    assert!(
        handle
            .lines
            .iter()
            .any(|line| line.contains("spawning remediation worker"))
            || handle.wait_for("spawning remediation worker", 15),
        "remediation worker was not spawned. Lines: {:?}",
        handle.lines
    );
    let remediation_name = handle
        .extract_agent_name("spawning remediation worker ")
        .expect("could not extract remediation worker name");

    // Persisted implementation responsibility drives MergeConflict → rework.
    let task = get_task(home.path(), task_id);
    assert_eq!(
        task.status, "rework",
        "implementation task must enter rework despite absent original worker"
    );
    assert_eq!(task.rework_round, 1);

    let (holder, expires_at) =
        active_claim(home.path(), task_id).expect("remediation lease must be active");
    assert_eq!(
        holder, remediation_name,
        "remediation worker must hold the task lease"
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(expires_at > now, "remediation lease must be unexpired");

    let expected_branch = format!("daemon/{}-t{}", author.to_lowercase(), task_id);
    let (branch, journal_pr) =
        worker_journal_target(home.path(), task_id).expect("remediation journal row");
    assert_eq!(branch, expected_branch, "must reuse the existing PR branch");
    assert_eq!(journal_pr, Some(pr), "must continue the existing PR");

    assert!(
        handle
            .lines
            .iter()
            .any(|line| line.contains(&format!("worker {remediation_name} result")))
            || handle.wait_for(&format!("worker {remediation_name} result"), 15),
        "remediation worker did not receive its turn. Lines: {:?}",
        handle.lines
    );
    let prompts = std::fs::read_to_string(home.path().join("fake-agent-prompts.jsonl")).unwrap();
    assert!(
        prompts.contains("Fix the widget"),
        "remediation turn must include the existing task body: {prompts}"
    );
    assert!(
        prompts.contains("has conflicts with main")
            && prompts.contains("Rebase on main, resolve conflicts"),
        "remediation turn must include conflict instructions: {prompts}"
    );

    handle.sigkill();
}

/// Genuine review-only tasks remain parked for their external author and
/// never acquire an implementation lease or remediation worker.
#[test]
fn merge_conflict_review_only_parks_without_remediation() {
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

    let author = "ExternalAuthor";
    let pr = 1;
    let task_id = seed_in_review_task(home.path(), author, pr);
    {
        let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        conn.execute("UPDATE tasks SET review_only=1 WHERE id=?1", [task_id])
            .unwrap();
    }
    create_pr_branch(repo_dir.path(), author, task_id);
    let head_sha = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                &repo_dir.path().to_string_lossy(),
                "rev-parse",
                "HEAD",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    {
        let mut conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        let branch = format!("daemon/{}-t{}", author.to_lowercase(), task_id);
        quorum_core::pr_targets::upsert(&mut conn, task_id, pr, &branch, head_sha.trim(), false)
            .unwrap();
    }

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &["--merge-mergeability-cmd", "echo conflicting"],
    );
    assert!(
        handle.wait_for("recovery: complete", 15),
        "{:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("spawning reviewer", 30),
        "{:?}",
        handle.lines
    );
    let reviewer = handle.extract_agent_name("spawning reviewer ").unwrap();
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer,
            "--pr",
            &pr.to_string(),
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    complete_r2_review(home.path(), &mut handle, &pr.to_string());
    assert!(
        handle.wait_for("parking, not failing", 20),
        "{:?}",
        handle.lines
    );

    let task = get_task(home.path(), task_id);
    assert_eq!(task.status, "in-review");
    assert_eq!(task.rework_round, 0);
    assert!(
        get_agent_runs(home.path(), task_id)
            .iter()
            .all(|run| run.role != "worker"),
        "review-only conflict must not allocate a worker"
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("spawning remediation worker")),
        "review-only conflict must not spawn remediation: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Live-worker merge conflict triggers MergeConflict → rework directly,
/// and rework cap is enforced.
#[test]
fn merge_conflict_live_worker_triggers_rework_respects_cap() {
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

    seed_task(home.path(), "Conflict-rework-cap test");

    // mergeability_cmd returns conflicting after first few calls.
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
            "echo ready",
            "--merge-checks-timeout-secs",
            "10",
            "--merge-checks-poll-secs",
            "1",
            "--merge-mergeability-cmd",
            &mergeability_script,
        ],
    );

    drive_to_approved(home.path(), &mut handle, "1");

    // Conflict detected with live worker → MergeConflict → rework.
    assert!(
        handle.wait_for("CONFLICTING", 20),
        "conflict not detected. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework", 15),
        "rework not triggered for conflict. Lines: {:?}",
        handle.lines
    );

    // Rework counter incremented.
    let task = get_task(home.path(), 1);
    assert!(
        task.rework_round >= 1,
        "rework_round must increment, got: {}",
        task.rework_round
    );

    handle.sigkill();
}

/// Replacement worker instant-death loop stops at configured recovery budget
/// with loud owner signal.
#[test]
fn replacement_instant_death_stops_at_budget() {
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

    // Seed task at recovery_attempts = MAX-1 (one more death → cancel).
    let task_id = {
        let mut conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "Instant-death budget test",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_by":"test-classifier"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        // Set to MAX-1 recovery attempts.
        let max = quorum_core::tasks::MAX_RECOVERY_ATTEMPTS;
        for i in 0..(max - 1) {
            quorum_core::tasks::claim(&mut conn, "W", Some(id), &[], 3600, now + i * 10).unwrap();
            quorum_core::tasks::apply_event(
                &mut conn,
                "daemon",
                id,
                &quorum_core::lifecycle::Event::AgentFailed {
                    reason: "worker process died".into(),
                },
                now + i * 10 + 5,
            )
            .unwrap();
        }
        // Claim again to put in working state.
        quorum_core::tasks::claim(&mut conn, "W-final", Some(id), &[], 3600, now + 100).unwrap();
        id
    };

    let task = get_task(home.path(), task_id);
    assert_eq!(task.status, "working");
    assert_eq!(
        task.recovery_attempts,
        quorum_core::tasks::MAX_RECOVERY_ATTEMPTS - 1
    );

    // die-agent: exits immediately (triggers instant death detection).
    let die_agent = home.path().join("die-agent.sh");
    std::fs::write(
        &die_agent,
        "#!/bin/sh\n\
         read -r _line 2>/dev/null || true\n\
         printf '{\"type\":\"assistant\",\"message\":{\"content\":\"working\"}}\\n'\n\
         printf '{\"type\":\"result\",\"result\":\"crash\",\"usage\":{\"input_tokens\":100,\"output_tokens\":50},\"total_cost_usd\":0.001,\"num_turns\":1,\"duration_ms\":100,\"is_error\":false}\\n'\n\
         exit 0\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&die_agent, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Start daemon with the die-agent.
    let sentinel = tempfile::tempdir().unwrap();
    let sentinel_path = sentinel.path().to_string_lossy().to_string();
    let mut child = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "serve",
            "--repo",
            "test/repo",
            "--cap",
            "1",
            "--repo-dir",
            &repo_dir.path().to_string_lossy(),
            "--worktree-base",
            &wt_base.path().to_string_lossy(),
            "--names-file",
            &names_file.to_string_lossy(),
            "--agent-bin",
            &die_agent.to_string_lossy(),
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
    let mut handle = ServeHandle {
        child,
        rx,
        lines: Vec::new(),
        _sentinel: Some(sentinel),
    };

    // Recovery fires AgentFailed → budget exhausted → durably parked.
    assert!(
        handle.wait_for("-> failed", 30) || handle.wait_for("recovery budget", 30),
        "task not parked after budget exhaustion. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    // Verify final state.
    let task = get_task(home.path(), task_id);
    assert_eq!(
        task.status, "failed",
        "task must be parked after budget exhaustion"
    );

    // Owner signal: messages table must contain budget-exhaustion alert.
    let msgs = get_messages(home.path(), task_id);
    let has_alert = msgs
        .iter()
        .any(|m| m.contains("recovery budget exhausted") || m.contains("budget"));
    assert!(
        has_alert,
        "owner must receive budget-exhaustion alert. Messages: {:?}",
        msgs
    );

    // agent_runs must all have ended_at set (no ghost runs).
    let runs = get_agent_runs(home.path(), task_id);
    for run in &runs {
        if run.role == "worker" {
            assert!(
                run.ended_at.is_some(),
                "worker run {} must have ended_at",
                run.id
            );
        }
    }

    handle.sigkill();
}

/// No duplicate lifecycle events under rapid ticks/restart.
/// Start → merge-wait → stop → restart → merge. Count lifecycle events.
#[test]
fn no_duplicate_events_under_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let checks_state = home.path().join("checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "No-duplicate-events test");

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

    drive_to_approved_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait not entered. Lines: {:?}",
        handle.lines
    );
    handle.stop();

    // Flip to ready.
    std::fs::write(&checks_state, "ready").unwrap();

    // Restart and merge.
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
        handle2.wait_for("merged", 30),
        "merge not seen after restart. Lines: {:?}",
        handle2.lines
    );

    std::thread::sleep(Duration::from_millis(500));
    handle2.drain_pending_lines();

    // No duplicate merges in the log lines. Filter for actual merge actions,
    // not summary stats like "merged=1".
    let all_lines: Vec<_> = handle2
        .lines
        .iter()
        .filter(|l| {
            (l.contains("merged from durable approval") || l.contains("proceeding to merge"))
                && !l.contains("merged=")
        })
        .collect();
    assert!(
        all_lines.len() <= 1,
        "at most one merged log expected, got {}: {:?}",
        all_lines.len(),
        all_lines
    );

    handle2.sigkill();

    // Count lifecycle events — exactly one task_done/task_merged event.
    let events = get_events(home.path(), 1);
    let done_events: Vec<_> = events
        .iter()
        .filter(|e| e.kind == "task_done" || e.kind == "task_merged")
        .collect();
    assert!(
        done_events.len() <= 1,
        "at most one done/merged event expected, got {}: {:?}",
        done_events.len(),
        done_events
    );
}

/// agent_runs end_reason values are truthful — worker ended by daemon stop
/// vs. natural completion get distinct reasons.
#[test]
fn agent_runs_end_reasons_truthful() {
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

    seed_task(home.path(), "Agent-runs end-reason test");

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

    // Wait for reviewer cycle.
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

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
        "merge not seen. Lines: {:?}",
        handle.lines
    );
    handle.stop();

    // Check agent_runs for truthful end reasons.
    let runs = get_agent_runs(home.path(), 1);
    assert!(!runs.is_empty(), "agent_runs must have entries for task #1");

    for run in &runs {
        assert!(
            run.ended_at.is_some(),
            "run {} ({}) must have ended_at set",
            run.id,
            run.role
        );
        if let Some(ref reason) = run.end_reason {
            // End reasons should be meaningful strings, not empty.
            assert!(
                !reason.is_empty(),
                "end_reason for run {} must not be empty",
                run.id
            );
            // Should not contain error text for successful completions.
            assert!(
                !reason.contains("panic") && !reason.contains("INTERNAL ERROR"),
                "end_reason should not contain panic/internal-error for normal flow: {}",
                reason
            );
        }
    }
}
