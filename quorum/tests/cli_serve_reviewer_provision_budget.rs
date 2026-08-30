//! Reviewer provisioning budget regressions.
//!
//! Every failure that happens after a reviewer identity and worktree exist must
//! burn the durable `reviewer_provision_attempts` budget and park at the cap;
//! a generated child whose graph plan is stale must be held instead, without
//! ever allocating one. Split from `cli_serve_review_only_orphan.rs` only to
//! keep each test binary inside the preflight per-binary time budget.
//!
//! Asserts DB state (attempts rows, task status/refs), not just console lines.

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

fn write_named_pool(dir: &std::path::Path, names: &[String]) -> std::path::PathBuf {
    let path = dir.join("names.txt");
    let mut f = std::fs::File::create(&path).unwrap();
    for name in names {
        writeln!(f, "{name}").unwrap();
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
        merge_cmd: &str,
        extra_args: &[&str],
    ) -> Self {
        let fake_agent = cargo_bin("fake-agent");
        Self::start_with_agent_bin(
            home,
            repo,
            wt_base,
            names,
            merge_cmd,
            extra_args,
            &fake_agent,
        )
    }

    fn start_with_agent_bin(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        merge_cmd: &str,
        extra_args: &[&str],
        agent_bin: &std::path::Path,
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let sentinel_path = sentinel.path().to_string_lossy().to_string();
        let gh_shim = tempfile::tempdir().unwrap();
        let gh_path = gh_shim.path().join("gh");
        std::fs::write(
            &gh_path,
            r#"#!/bin/sh
set -eu
cmd="${1:-} ${2:-}"
if [ "$cmd" = "pr list" ]; then
  printf '[]\n'
elif [ "$cmd" = "pr view" ]; then
  pr="$3"
  branch="daemon/origworker-t1"
  sha="$(git -C "$QUORUM_TEST_REPO" ls-remote origin "refs/heads/$branch" | awk '{print $1}')"
  if [ -z "$sha" ]; then sha="$(git -C "$QUORUM_TEST_REPO" rev-parse "refs/heads/$branch")"; fi
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
            &agent_bin.to_string_lossy(),
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
            .env("PATH", path)
            .env("QUORUM_TEST_REPO", repo)
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
}

fn db_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join("repos").join("test__repo").join("quorum.db")
}

fn get_task(home: &std::path::Path, task_id: i64) -> quorum_core::tasks::Task {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    quorum_core::tasks::get(&conn, task_id).unwrap().unwrap()
}

fn seed_in_review_task(home: &std::path::Path, author: &str, pr: i64) -> i64 {
    let mut conn = quorum_core::db::open(&db_path(home)).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let id = quorum_core::tasks::create(
        &mut conn,
        "test",
        "PR #3806 orphan regression task",
        Some("Regression for the review-only orphan incident"),
        0,
        None,
        None,
        None,
        None,
        now,
    )
    .unwrap();
    quorum_core::classify::store_classifications(
        &mut conn,
        &[quorum_core::classify::TaskClassification {
            task_id: id,
            cx_est: 3,
            size: "M".into(),
            size_reason: "bounded test classification rationale".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: vec![],
        }],
        "test:v2",
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

fn create_author_branch(repo_dir: &std::path::Path, author: &str, task_id: i64) {
    let branch = format!("daemon/{}-t{}", author.to_lowercase(), task_id);
    let d = repo_dir.to_string_lossy();
    Command::new("git")
        .args(["-C", &d, "branch", &branch])
        .status()
        .unwrap();
}

fn record_closed_run(home: &std::path::Path, task_id: i64, agent: &str, role: &str) {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    let run_id = quorum_core::agent_runs::insert(
        &conn,
        task_id,
        agent,
        role,
        "claude-opus-4-6",
        "high",
        "claude",
        100,
    )
    .unwrap();
    quorum_core::agent_runs::close(&conn, run_id, 101, "test teardown").unwrap();
}

/// A generated child whose graph is no longer the current active plan can never
/// be issued reviewer authority. It is HELD in-review: no reviewer identity, no
/// worktree, no strike, and no park — the daemon waits for an operator to
/// unblock or cancel the graph. Every provisioning path applies the same hold,
/// so a restart (which turns a tracked worker into an orphan) cannot escalate a
/// held child into a parked one.
#[test]
fn stale_graph_child_is_held_not_provisioned_or_parked() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let author = "Worker";
    let task_id = seed_in_review_task(home.path(), author, 42);
    create_author_branch(repo_dir.path(), author, task_id);
    record_closed_run(home.path(), task_id, author, "worker");
    let names = write_named_pool(home.path(), &["Reviewer".into()]);
    {
        let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        conn.execute(
            "INSERT INTO tasks(id,title,body,status,created_by,created_at,updated_at)
             VALUES (9001,'graph source','source outcome','decomposed','owner',1,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_decompositions(id,source_task_id,state,active,freeze_active,
                 planned_source_revision,plan_revision,accepted_plan_revision,created_at,updated_at)
             VALUES (9,9001,'completed',0,0,1,2,2,1,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
             VALUES (9,?1,'parser',2,1)",
            rusqlite::params![task_id],
        )
        .unwrap();
    }

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("holding in-review, not provisioning a reviewer", 60),
        "stale graph child was not held: {:?}",
        handle.lines
    );
    // Several more ticks: the hold must be stable, not a slow path to a park.
    std::thread::sleep(Duration::from_secs(5));
    handle.drain_pending_lines();

    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("spawning reviewer")),
        "held child must not allocate a reviewer identity or worktree: {:?}",
        handle.lines
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("provision strike")),
        "held child must not burn the provision budget: {:?}",
        handle.lines
    );
    // The rate limiter keeps a permanently held child from logging every tick.
    let hold_logs = handle
        .lines
        .iter()
        .filter(|line| line.contains("holding in-review, not provisioning a reviewer"))
        .count();
    assert_eq!(
        hold_logs, 1,
        "hold notice must be rate limited: {:?}",
        handle.lines
    );

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let attempts: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(attempts), 0) FROM reviewer_provision_attempts WHERE task_id=?1",
            rusqlite::params![task_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        attempts, 0,
        "a held child must not record provision strikes"
    );
    let held = get_task(home.path(), task_id);
    assert_eq!(held.status, "in-review", "held child must stay in-review");
    assert_eq!(held.reviewer, None);
    drop(handle);
}

/// The strike path itself must stay covered for a task that IS reviewable: an
/// authority failure that the pre-flight hold cannot predict (here, capability
/// issuance failing inside the same transaction) still burns the durable budget
/// and parks at the cap.
#[test]
fn authority_failure_for_reviewable_task_stops_after_provision_budget() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let author = "Worker";
    let task_id = seed_in_review_task(home.path(), author, 42);
    create_author_branch(repo_dir.path(), author, task_id);
    record_closed_run(home.path(), task_id, author, "worker");
    let names = write_named_pool(home.path(), &["Reviewer".into()]);
    {
        // Not a graph member, so the hold does not apply — authority issuance
        // itself fails, after the worktree already exists.
        let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_reviewer_capability
             BEFORE INSERT ON run_capabilities
             WHEN NEW.role = 'reviewer'
             BEGIN
               SELECT RAISE(ABORT, 'capability issuance unavailable');
             END;",
        )
        .unwrap();
    }

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("orphan in-review task #1 PR #42: provision exhausted", 90),
        "authority failure did not exhaust and park: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    let authority_failures = handle
        .lines
        .iter()
        .filter(|line| line.contains("reviewer authority validation failed"))
        .count();
    assert_eq!(
        authority_failures, 3,
        "daemon must stop re-provisioning at the durable strike cap: {:?}",
        handle.lines
    );
    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let attempts: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(attempts), 0) FROM reviewer_provision_attempts WHERE task_id=?1",
            rusqlite::params![task_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        attempts, 3,
        "authority failure must record provision strikes"
    );
    let parked = get_task(home.path(), task_id);
    assert_eq!(parked.status, "failed", "task was not parked");
    assert!(
        parked
            .refs
            .as_deref()
            .unwrap_or_default()
            .contains("reviewer provision exhausted"),
        "park reason missing from refs: {:?}",
        parked.refs
    );
    assert_eq!(parked.reviewer, None);
    drop(handle);
}

/// A `ReviewerAttached` transition that keeps being rejected fails after the
/// reviewer process is already running. It must burn the budget too — and the
/// durable strikes are only cleared once the transition actually succeeds, so a
/// persistent rejection cannot wipe its own accounting on every attempt.
#[test]
fn persistent_attachment_rejection_stops_after_provision_budget() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let author = "Worker";
    let task_id = seed_in_review_task(home.path(), author, 42);
    create_author_branch(repo_dir.path(), author, task_id);
    record_closed_run(home.path(), task_id, author, "worker");
    let names = write_named_pool(home.path(), &["Reviewer".into()]);
    {
        let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_reviewer_attach
             BEFORE UPDATE ON tasks
             WHEN NEW.reviewer IS NOT NULL
                  AND (OLD.reviewer IS NULL OR OLD.reviewer <> NEW.reviewer)
             BEGIN
               SELECT RAISE(ABORT, 'reviewer attachment unavailable');
             END;",
        )
        .unwrap();
    }

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("orphan in-review task #1 PR #42: provision exhausted", 90),
        "persistent attachment rejection did not exhaust and park: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    let rejections = handle
        .lines
        .iter()
        .filter(|line| line.contains("ReviewerAttached was rejected after provisioning"))
        .count();
    assert_eq!(
        rejections, 3,
        "daemon must stop re-provisioning at the durable strike cap: {:?}",
        handle.lines
    );
    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let attempts: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(attempts), 0) FROM reviewer_provision_attempts WHERE task_id=?1",
            rusqlite::params![task_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        attempts, 3,
        "a rejected attachment must not clear its own strikes"
    );
    let parked = get_task(home.path(), task_id);
    assert_eq!(parked.status, "failed", "task was not parked");
    assert_eq!(parked.reviewer, None);
    drop(handle);
}

/// A provider CLI that cannot be spawned at all (missing binary, bad path,
/// authentication failure) fails after the worktree, branch, journal row, and
/// capability have all been created. It is the most expensive provisioning
/// failure there is, and callers treat `Failed` as "retry next tick", so it
/// must burn the durable budget and park like every other post-worktree failure.
#[test]
fn persistent_reviewer_spawn_failure_stops_after_provision_budget() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let author = "Worker";
    let task_id = seed_in_review_task(home.path(), author, 42);
    create_author_branch(repo_dir.path(), author, task_id);
    record_closed_run(home.path(), task_id, author, "worker");
    let names = write_named_pool(home.path(), &["Reviewer".into()]);

    // A path that cannot be executed makes `RunnerProc::launch` fail before any
    // provider protocol exists — `fake_agent` cannot reproduce this.
    let missing_dir = tempfile::tempdir().unwrap();
    let missing_agent = missing_dir.path().join("no-such-provider-cli");

    let mut handle = ServeHandle::start_with_agent_bin(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
        &missing_agent,
    );
    assert!(
        handle.wait_for("orphan in-review task #1 PR #42: provision exhausted", 90),
        "persistent spawn failure did not exhaust and park: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    let spawn_failures = handle
        .lines
        .iter()
        .filter(|line| line.contains("failed to spawn reviewer"))
        .count();
    assert_eq!(
        spawn_failures, 3,
        "daemon must stop re-provisioning at the durable strike cap: {:?}",
        handle.lines
    );
    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let attempts: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(attempts), 0) FROM reviewer_provision_attempts WHERE task_id=?1",
            rusqlite::params![task_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempts, 3, "spawn failure must record provision strikes");
    let parked = get_task(home.path(), task_id);
    assert_eq!(parked.status, "failed", "task was not parked");
    assert_eq!(parked.reviewer, None);
    drop(handle);
}

/// If the durable strike cannot be written, the durable budget can never be
/// reached — but an unrecordable strike is a DB fault, not a lifecycle verdict.
/// The daemon must stay loud and keep going (a transient write-lock holder must
/// not park a task, which for a generated child would block its whole graph),
/// and only the in-memory backstop parks after three consecutive failures.
#[test]
fn unrecordable_provision_strikes_park_only_via_the_in_memory_backstop() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let author = "Worker";
    let task_id = seed_in_review_task(home.path(), author, 42);
    create_author_branch(repo_dir.path(), author, task_id);
    record_closed_run(home.path(), task_id, author, "worker");
    let names = write_named_pool(home.path(), &["Reviewer".into()]);
    {
        let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_reviewer_run_forever
             BEFORE INSERT ON agent_runs
             WHEN NEW.role = 'reviewer'
             BEGIN
               SELECT RAISE(ABORT, 'persistent reviewer run failure');
             END;
             CREATE TRIGGER fail_provision_strike
             BEFORE INSERT ON reviewer_provision_attempts
             BEGIN
               SELECT RAISE(ABORT, 'strike recording unavailable');
             END;",
        )
        .unwrap();
    }

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("PARKED: task #1", 90),
        "in-memory backstop did not park after repeated unrecordable strikes: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    // The park must come only after the third consecutive unrecordable strike —
    // one DB hiccup must not be a lifecycle transition.
    let park_index = handle
        .lines
        .iter()
        .position(|line| line.contains("PARKED: task #1"))
        .expect("park line present");
    let backstop_before_park = handle.lines[..park_index]
        .iter()
        .filter(|line| line.contains("could not record provision strike"))
        .count();
    assert_eq!(
        backstop_before_park, 3,
        "backstop must park only on the third consecutive unrecordable strike: {:?}",
        handle.lines
    );
    assert!(
        handle
            .lines
            .iter()
            .any(|line| line.contains("in-memory backstop 1/3")),
        "unrecordable strikes must be logged as in-memory, not durable: {:?}",
        handle.lines
    );
    // Errors stay contained in the per-task path, not the whole tick.
    assert!(
        !handle.lines.iter().any(|line| line.contains("tick error")),
        "an unrecordable strike must not abort the tick: {:?}",
        handle.lines
    );

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let attempts: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(attempts), 0) FROM reviewer_provision_attempts WHERE task_id=?1",
            rusqlite::params![task_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempts, 0, "no strike could be recorded durably");
    let parked = get_task(home.path(), task_id);
    assert_eq!(parked.status, "failed", "task was not parked");
    assert!(
        parked
            .refs
            .as_deref()
            .unwrap_or_default()
            .contains("could not be recorded"),
        "park reason missing from refs: {:?}",
        parked.refs
    );
    drop(handle);
}
