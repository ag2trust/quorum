//! End-to-end regression for the PR #3806 review-only orphan incident.
//!
//! Walks a review-only task through:
//! 1. R1 changes verdict → rework
//! 2. Remediation worker provisioned with lease
//! 3. Intervening sweep does NOT reap the leased rework task
//! 4. Remediation worker pushes → in-review + re-review fed to reviewer
//!
//! Unit-level tests cover:
//! - Expired-lease review-only rework recovers to in-review (not open)
//! - Healthy remediation lease survives reaper
//! - Claim race produces no errors rows
//!
//! Negative assertions:
//! - No review-only path reaches open
//! - Healthy remediation lease survives sweep
//! - No errors rows from normal operation
//! - ReworkPushed is never rejected

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
    write_named_pool(
        dir,
        &(0..20).map(|i| format!("Agent{i}")).collect::<Vec<_>>(),
    )
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

    fn extract_agent_name(&self, prefix: &str) -> Option<String> {
        for line in &self.lines {
            if let Some(rest) = line.split(prefix).nth(1) {
                return Some(rest.split_whitespace().next().unwrap_or("").to_string());
            }
        }
        None
    }
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

fn agent_endpoint(home: &std::path::Path) -> std::path::PathBuf {
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&db, &mut hasher);
    std::env::temp_dir()
        .join(format!(
            "quorum-agent-{:016x}",
            std::hash::Hasher::finish(&hasher)
        ))
        .join("endpoint.sock")
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
        .env("QUORUM_AGENT_ENDPOINT", agent_endpoint(home))
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

fn db_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join("repos").join("test__repo").join("quorum.db")
}

fn get_task(home: &std::path::Path, task_id: i64) -> quorum_core::tasks::Task {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    quorum_core::tasks::get(&conn, task_id).unwrap().unwrap()
}

fn active_claim(home: &std::path::Path, task_id: i64) -> Option<(String, i64)> {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    let target = format!("task#{task_id}");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    conn.query_row(
        "SELECT holder, expires_at FROM claims WHERE target=?1 AND active=1 AND expires_at > ?2",
        rusqlite::params![target, now],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
    )
    .ok()
}

fn events_for_task(home: &std::path::Path, task_id: i64) -> Vec<(String, String)> {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    let subject = format!("task#{task_id}");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let mut stmt = conn
        .prepare("SELECT kind, body FROM events WHERE subject=?1 AND expires_at > ?2 ORDER BY seq")
        .unwrap();
    stmt.query_map(rusqlite::params![subject, now], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })
    .unwrap()
    .collect::<rusqlite::Result<Vec<_>>>()
    .unwrap()
}

fn errors_count(home: &std::path::Path) -> i64 {
    let conn = quorum_core::db::open(&db_path(home)).unwrap();
    conn.query_row("SELECT COUNT(*) FROM errors", [], |r| r.get(0))
        .unwrap()
}

/// Seed a review-only task directly in the DB with a known author and PR.
fn seed_review_only_task(home: &std::path::Path, pr: i64) -> i64 {
    let mut conn = quorum_core::db::open(&db_path(home)).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let id = quorum_core::tasks::create(
        &mut conn,
        "TestCreator",
        "Review-only orphan regression task",
        Some("Regression for PR #3806 orphan incident"),
        0,
        None,
        None,
        None,
        Some(pr),
        now,
    )
    .unwrap();
    quorum_core::classify::store_classifications(
        &mut conn,
        &[quorum_core::classify::TaskClassification {
            task_id: id,
            cx_est: 3,
            size: "M".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: vec![],
        }],
        "test:v2",
        now,
    )
    .unwrap();
    id
}

/// Seed an in-review task with a known author (daemon can derive branch).
/// Uses the same pattern as `cli_serve_remediation.rs` — normal task, claimed,
/// signaled done with PR — so `orphan_worker_branch` resolves without GitHub.
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

#[test]
fn orphan_reviewer_waits_for_complete_v2_classification() {
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
    {
        let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        conn.execute(
            "UPDATE tasks
             SET refs=json_object('pr', 42, 'cx_est', 3, 'cx_by', 'legacy:v1')
             WHERE id=?1",
            rusqlite::params![task_id],
        )
        .unwrap();
    }
    let names = write_named_pool(home.path(), &["Reviewer".into()]);

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("spawning reviewer", 30),
        "reviewer was not provisioned after reclassification: {:?}",
        handle.lines
    );

    let gated = handle
        .lines
        .iter()
        .position(|line| {
            line.contains("awaiting complete dispatchable classification before review dispatch")
        })
        .expect("legacy partial classification was not gated");
    let classified = handle
        .lines
        .iter()
        .position(|line| line.contains("classifier: stored 1 classification"))
        .expect("v2 classification was not persisted");
    let spawned = handle
        .lines
        .iter()
        .position(|line| line.contains("spawning reviewer"))
        .expect("reviewer spawn log missing");
    assert!(
        gated < classified && classified < spawned,
        "reviewer must not spawn before v2 classification: {:?}",
        handle.lines
    );

    let task = get_task(home.path(), task_id);
    let refs: serde_json::Value =
        serde_json::from_str(task.refs.as_deref().expect("classified refs")).unwrap();
    assert_eq!(refs["cx_by"], "claude-opus-4-6:v2");
    assert_eq!(refs["cx_size"], "S");
    assert_eq!(refs["cx_ready"], true);
    drop(handle);
}

#[test]
fn terminal_orphan_pr_reconciliation_ignores_incomplete_classification() {
    for (pr_state, expected_status, log_needle) in [
        ("merged", "done", "already merged (orphan in-review)"),
        (
            "closed",
            "failed",
            "closed without merge (orphan in-review)",
        ),
    ] {
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
        record_closed_run(home.path(), task_id, author, "worker");
        {
            let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
            conn.execute(
                "UPDATE tasks
                 SET refs=json_object('pr', 42, 'cx_est', 3, 'cx_by', 'legacy:v1')
                 WHERE id=?1",
                rusqlite::params![task_id],
            )
            .unwrap();
        }

        let unavailable_dir = tempfile::tempdir().unwrap();
        let unavailable_agent = unavailable_dir.path().join("unavailable-agent");
        std::fs::write(&unavailable_agent, "#!/bin/sh\nexit 1\n").unwrap();
        std::fs::set_permissions(&unavailable_agent, std::fs::Permissions::from_mode(0o755))
            .unwrap();
        let names = write_named_pool(home.path(), &["Reviewer".into()]);
        let mergeability_cmd = format!("echo {pr_state}");
        let extra_args = ["--merge-mergeability-cmd", mergeability_cmd.as_str()];
        let mut handle = ServeHandle::start_with_agent_bin(
            home.path(),
            repo_dir.path(),
            wt_base.path(),
            &names,
            "true",
            &extra_args,
            &unavailable_agent,
        );

        assert!(
            handle.wait_for(log_needle, 15),
            "terminal {pr_state} PR was not reconciled without classification: {:?}",
            handle.lines
        );
        handle.drain_pending_lines();
        assert!(
            !handle
                .lines
                .iter()
                .any(|line| line.contains("spawning reviewer")),
            "terminal {pr_state} PR must not spawn a reviewer: {:?}",
            handle.lines
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let task = loop {
            let task = get_task(home.path(), task_id);
            if task.status == expected_status {
                break task;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "terminal {pr_state} PR did not persist {expected_status}: {:?}",
                handle.lines
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        assert_eq!(task.status, expected_status);
        let refs: serde_json::Value =
            serde_json::from_str(task.refs.as_deref().expect("legacy refs retained")).unwrap();
        assert!(
            refs.get("cx_size").is_none(),
            "test requires terminal reconciliation before unavailable classification"
        );
        drop(handle);
    }
}

#[test]
fn orphan_r1_does_not_reuse_torn_down_worker_name() {
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
    let names = write_named_pool(home.path(), &["Worker".into(), "R1".into()]);

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("spawning reviewer", 30),
        "R1 reviewer not provisioned: {:?}",
        handle.lines
    );
    let reviewer = handle.extract_agent_name("spawning reviewer ").unwrap();
    assert_eq!(reviewer, "R1");
    assert_ne!(reviewer, author);
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("reviewer must differ from author")),
        "lifecycle rejected a reused worker identity: {:?}",
        handle.lines
    );
    drop(handle);
}

#[test]
fn orphan_r1_restart_excludes_reviewer_persisted_before_attachment() {
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
    let interrupted_reviewer = "InterruptedR1";
    let task_id = seed_in_review_task(home.path(), author, 42);
    create_author_branch(repo_dir.path(), author, task_id);
    record_closed_run(home.path(), task_id, author, "worker");
    // Simulate a daemon crash after the fail-safe run insert and before
    // ReviewerAttached: there is durable run history but no task reviewer.
    record_closed_run(home.path(), task_id, interrupted_reviewer, "reviewer");
    assert_eq!(get_task(home.path(), task_id).reviewer, None);
    let names = write_named_pool(
        home.path(),
        &[interrupted_reviewer.into(), "FreshR1".into()],
    );

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("spawning reviewer", 30),
        "replacement R1 reviewer not provisioned: {:?}",
        handle.lines
    );
    let reviewer = handle.extract_agent_name("spawning reviewer ").unwrap();
    assert_eq!(reviewer, "FreshR1");
    assert_ne!(reviewer, interrupted_reviewer);
    drop(handle);
}

#[test]
fn reviewer_run_insert_error_never_attaches_and_restart_recovers() {
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
            "CREATE TRIGGER fail_reviewer_run
             BEFORE INSERT ON agent_runs
             WHEN NEW.role = 'reviewer'
             BEGIN
               SELECT RAISE(ABORT, 'injected reviewer run failure');
             END;",
        )
        .unwrap();
    }

    let mut failed = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        failed.wait_for("reviewer run persistence failed before attachment", 30),
        "injected persistence failure was not surfaced: {:?}",
        failed.lines
    );
    // The failure is contained to this task: it is logged and the tick keeps
    // going, instead of aborting the whole tick with `tick error`.
    assert!(
        failed.wait_for("orphan provision error for task #1 PR #42", 10),
        "daemon did not finish fail-safe cleanup: {:?}",
        failed.lines
    );
    failed.drain_pending_lines();
    assert!(
        !failed.lines.iter().any(|line| line.contains("tick error")),
        "a single task's provision failure must not abort the tick: {:?}",
        failed.lines
    );
    drop(failed);

    let task = get_task(home.path(), task_id);
    assert_eq!(task.reviewer, None, "failed persistence must not attach");
    {
        let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        let reviewer_runs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE task_id=?1 AND role='reviewer'",
                rusqlite::params![task_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reviewer_runs, 0);
        conn.execute_batch("DROP TRIGGER fail_reviewer_run;")
            .unwrap();
    }

    let retry_wt_base = tempfile::tempdir().unwrap();
    let mut recovered = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        retry_wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        recovered.wait_for("R1: reviewer Reviewer spawned", 30),
        "restart did not recover reviewer provisioning: {:?}",
        recovered.lines
    );
    assert_eq!(
        get_task(home.path(), task_id).reviewer.as_deref(),
        Some("Reviewer")
    );
    drop(recovered);
}

/// A generated child whose graph is no longer the current active plan fails
/// reviewer-authority validation on every attempt. That failure happens after
/// the reviewer identity and worktree exist, so it must burn the same durable
/// provision budget as any other post-allocation failure and park the task
/// instead of re-provisioning a reviewer on every tick forever.
#[test]
fn stale_graph_authority_failure_stops_after_provision_budget() {
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
        handle.wait_for("orphan in-review task #1 PR #42: provision exhausted", 90),
        "stale graph authority failure did not exhaust and park: {:?}",
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

/// If the durable strike itself cannot be recorded, the retry budget can never
/// be reached. Failing open there would re-provision a reviewer every tick
/// forever, so the daemon must park on the first unrecordable strike.
#[test]
fn unrecordable_provision_strike_parks_immediately() {
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
        "unrecordable strike did not park the task: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    let provision_failures = handle
        .lines
        .iter()
        .filter(|line| line.contains("reviewer run persistence failed before attachment"))
        .count();
    assert_eq!(
        provision_failures, 1,
        "an unrecordable strike must stop provisioning at once: {:?}",
        handle.lines
    );
    let parked = get_task(home.path(), task_id);
    assert_eq!(parked.status, "failed", "task was not parked");
    assert!(
        parked
            .refs
            .as_deref()
            .unwrap_or_default()
            .contains("provision strike"),
        "park reason missing from refs: {:?}",
        parked.refs
    );
    drop(handle);
}

#[test]
fn persistent_reviewer_run_insert_error_stops_after_provision_budget() {
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
        "persistent failure did not exhaust and park: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    let persistence_failures = handle
        .lines
        .iter()
        .filter(|line| line.contains("reviewer run persistence failed before attachment"))
        .count();
    assert_eq!(
        persistence_failures, 3,
        "daemon must stop provisioning at the durable strike cap: {:?}",
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
    assert_eq!(attempts, 3);
    assert_eq!(get_task(home.path(), task_id).reviewer, None);
    drop(handle);
}

#[test]
fn orphan_r2_does_not_reuse_torn_down_worker_or_r1_name() {
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
    let r1 = "R1";
    let pr = 42;
    let task_id = seed_in_review_task(home.path(), author, pr);
    create_author_branch(repo_dir.path(), author, task_id);
    record_closed_run(home.path(), task_id, author, "worker");
    record_closed_run(home.path(), task_id, r1, "reviewer");

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
    .unwrap()
    .trim()
    .to_string();
    {
        let mut conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        quorum_core::approvals::record(
            &mut conn,
            &quorum_core::approvals::Approval {
                pr_number: pr,
                review_role: "r1".into(),
                task_id,
                author: author.into(),
                reviewer: r1.into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: head_sha.clone(),
            },
        )
        .unwrap();
        quorum_core::review_audits::record_r2_requirement(&mut conn, task_id, pr, &head_sha, true)
            .unwrap();
    }
    let names = write_named_pool(home.path(), &["Worker".into(), "R1".into(), "R2".into()]);

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("R2: pre-merge reviewer", 30),
        "R2 reviewer not provisioned: {:?}",
        handle.lines
    );
    let reviewer = handle
        .extract_agent_name("R2: pre-merge reviewer ")
        .unwrap();
    assert_eq!(reviewer, "R2");
    assert_ne!(reviewer, author);
    assert_ne!(reviewer, r1);
    drop(handle);
}

/// PR #3806 regression: in-review task through R1 changes → remediation →
/// sweep survival → rework push → re-review feed.
///
/// Exercises #199 (remediation lease), #200 (status-aware recovery), and the
/// sweep-on-write interaction that caused the original orphan. Uses a
/// daemon-resolvable (impl-authored) in-review task for deterministic branch
/// resolution; review-only-specific reaper paths are covered by the unit-level
/// tests in this file. Follows the established `rework_resignal_feeds_rereview_turn`
/// pattern: assert re-review feed happened, verify DB state, stop.
#[test]
fn review_only_orphan_full_lifecycle() {
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

    // ── Step 1: Seed in-review task (worker absent — daemon startup) ──
    let author = "OrigWorker";
    let pr: i64 = 42;
    let task_id = seed_in_review_task(home.path(), author, pr);
    create_author_branch(repo_dir.path(), author, task_id);
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

    let task = get_task(home.path(), task_id);
    assert_eq!(task.status, "in-review", "task must start in-review");

    // ── Step 2: Daemon starts, Phase 5b provisions R1 reviewer ──
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[],
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("spawning reviewer", 30),
        "R1 reviewer not provisioned: {:?}",
        handle.lines
    );
    let r1_name = handle
        .extract_agent_name("spawning reviewer ")
        .expect("could not extract R1 reviewer name");

    assert!(
        handle.wait_for("result", 15),
        "R1 reviewer result not seen: {:?}",
        handle.lines
    );

    // ── Step 3: R1 gives changes verdict → task enters rework ──
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r1_name,
            "--pr",
            &pr.to_string(),
            "--verdict",
            "changes",
            "--blocking",
            "2",
            "--feedback",
            "Fix error handling and add tests",
        ],
    );

    // Wait for remediation worker (no warm worker since original is absent).
    assert!(
        handle.wait_for("spawning remediation worker", 15),
        "remediation worker was not spawned after R1 changes: {:?}",
        handle.lines
    );

    // ── Step 4: Verify DB state during remediation ──
    std::thread::sleep(Duration::from_millis(500));
    let task = get_task(home.path(), task_id);
    assert_eq!(
        task.status, "rework",
        "task must be in rework after changes verdict"
    );
    assert_eq!(task.rework_round, 1, "rework_round must be 1");

    // Verify remediation lease exists (#199 fix).
    let claim = active_claim(home.path(), task_id);
    assert!(
        claim.is_some(),
        "remediation worker must hold an active lease (#199)"
    );
    let (holder, expiry) = claim.unwrap();
    assert!(
        !holder.is_empty(),
        "lease holder must be the remediation agent"
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(
        expiry > now,
        "lease must not be expired (expires_at={expiry}, now={now})"
    );

    // ── Step 5: Force a sweep — the lease must protect the task ──
    // The original bug: sweep saw rework + no lease → reaped to open.
    {
        let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        let sweep_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        quorum_core::sweep::reap_lapsed_tasks(&conn, sweep_now, 100).unwrap();
    }

    let task_after_sweep = get_task(home.path(), task_id);
    assert_eq!(
        task_after_sweep.status, "rework",
        "sweep must NOT reap a rework task with an active remediation lease"
    );

    // ── Step 6: Remediation worker completes and pushes ──
    assert!(
        handle.wait_for("remediation worker", 15),
        "remediation worker name log not seen: {:?}",
        handle.lines
    );
    let remediation_name = handle
        .extract_agent_name("remediation worker ")
        .expect("could not extract remediation worker name");
    let remediation_name = remediation_name
        .split_whitespace()
        .next()
        .unwrap()
        .to_string();

    assert!(
        handle.wait_for("result", 15),
        "remediation result not seen: {:?}",
        handle.lines
    );

    quorum_done(
        home.path(),
        &["--agent", &remediation_name, "--pr", &pr.to_string()],
    );

    // ── Step 7: ReworkPushed → in-review + re-review fed to reviewer ──
    assert!(
        handle.wait_for("ready for review", 15),
        "task did not return to review after remediation push: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("fed re-review turn", 15),
        "reviewer was not fed re-review turn after CI passed: {:?}",
        handle.lines
    );

    // ── Final DB state assertions ──
    let task = get_task(home.path(), task_id);
    assert_eq!(
        task.status, "in-review",
        "task must be in-review after ReworkPushed"
    );
    assert_eq!(
        task.rework_round, 1,
        "rework_round must still be 1 after first remediation cycle"
    );

    // Lifecycle events: verify the full sequence.
    let events = events_for_task(home.path(), task_id);
    let event_kinds: Vec<&str> = events.iter().map(|(k, _)| k.as_str()).collect();
    assert!(
        event_kinds.contains(&"task_rework"),
        "must have task_rework event (changes verdict), got: {event_kinds:?}"
    );
    assert!(
        event_kinds
            .iter()
            .filter(|k| **k == "task_in_review")
            .count()
            >= 2,
        "must have at least 2 task_in_review events (initial + ReworkPushed), got: {event_kinds:?}"
    );

    // No errors rows from normal operation.
    let errs = errors_count(home.path());
    assert_eq!(errs, 0, "normal operation must produce zero errors rows");

    handle.drain_pending_lines();

    // ReworkPushed was never rejected.
    let rework_pushed_rejected = handle
        .lines
        .iter()
        .any(|l| l.contains("ReworkPushed") && l.contains("rejected"));
    assert!(
        !rework_pushed_rejected,
        "ReworkPushed must never be rejected (the #3806 bug shape)"
    );

    // Task was never reaped — the core #200 invariant: no reclaim event.
    let was_reclaimed = events.iter().any(|(k, _)| k == "task_reclaimed");
    assert!(
        !was_reclaimed,
        "task must never be reaped during normal remediation lifecycle"
    );
}

/// Negative: an expired remediation lease parks the review-only task (never
/// bounces to in-review) — a replacement reviewer on the unchanged PR head
/// would burn a rework round with zero remediation applied (D5b).
#[test]
fn review_only_rework_expired_lease_parks_for_retry() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let pr: i64 = 99;
    let task_id = seed_review_only_task(home.path(), pr);

    // Manually walk the task to rework with an expired lease.
    {
        let mut conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let past = now - 7200; // 2 hours ago

        // Attach a reviewer, then give changes to enter rework.
        quorum_core::tasks::apply_event(
            &mut conn,
            "reviewer1",
            task_id,
            &quorum_core::lifecycle::Event::ReviewerAttached {
                agent: "reviewer1".into(),
            },
            past,
        )
        .unwrap();
        quorum_core::tasks::apply_event(
            &mut conn,
            "reviewer1",
            task_id,
            &quorum_core::lifecycle::Event::VerdictChanges,
            past + 1,
        )
        .unwrap();

        let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
        assert_eq!(task.status, "rework");

        // Install an expired lease (simulates remediation that died).
        let target = format!("task#{task_id}");
        conn.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1, 'dead-agent', ?2, ?3, 1)",
            rusqlite::params![target, past, past + 100],
        )
        .unwrap();

        // Manually set updated_at far enough in the past to be outside the
        // provisioning grace window.
        conn.execute(
            "UPDATE tasks SET updated_at=?1 WHERE id=?2",
            rusqlite::params![past, task_id],
        )
        .unwrap();

        // Run the reaper.
        quorum_core::sweep::reap_lapsed_tasks(&conn, now, 100).unwrap();
    }

    // Verify: parked (failed + daemon_parked, resume rework), never in-review.
    let task = get_task(home.path(), task_id);
    assert_eq!(
        task.status, "failed",
        "expired-lease review-only rework must park, got: {}",
        task.status
    );
    assert_eq!(
        task.rework_round, 1,
        "infra failure must not consume a rework round"
    );
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["daemon_parked"], true);
    assert_eq!(refs["daemon_resume_status"], "rework");
    assert!(
        refs.get("daemon_parked_head_check").is_none(),
        "terminal park must not carry automatic head-check authority"
    );
    assert!(
        refs.get("daemon_rework_retry_requested").is_none(),
        "terminal park must stay owner-gated until explicit task-retry"
    );

    let events = events_for_task(home.path(), task_id);
    let parked = events
        .iter()
        .find(|(k, _)| k == "task_parked")
        .map(|(_, b)| b.clone());
    assert!(parked.is_some(), "reaper must emit task_parked event");
    assert!(
        parked.as_ref().unwrap().contains("lease lapsed"),
        "park event must say 'lease lapsed' (not 'no lease installed'), got: {:?}",
        parked
    );
    assert!(
        !events.iter().any(|(k, _)| k == "task_reclaimed"),
        "parked task must not also emit task_reclaimed"
    );
}

/// Negative: a healthy (unexpired) remediation lease is never reaped,
/// even when the provisioning grace window has passed.
#[test]
fn healthy_remediation_lease_survives_reaper() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let pr: i64 = 100;
    let task_id = seed_review_only_task(home.path(), pr);

    {
        let mut conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Walk to rework.
        quorum_core::tasks::apply_event(
            &mut conn,
            "reviewer1",
            task_id,
            &quorum_core::lifecycle::Event::ReviewerAttached {
                agent: "reviewer1".into(),
            },
            now,
        )
        .unwrap();
        quorum_core::tasks::apply_event(
            &mut conn,
            "reviewer1",
            task_id,
            &quorum_core::lifecycle::Event::VerdictChanges,
            now + 1,
        )
        .unwrap();

        // Install a LIVE lease (not expired).
        let target = format!("task#{task_id}");
        conn.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1, 'remediation-agent', ?2, ?3, 1)",
            rusqlite::params![target, now, now + 3600],
        )
        .unwrap();

        // Push updated_at far back so the grace window does not protect it —
        // only the lease should.
        conn.execute(
            "UPDATE tasks SET updated_at=?1 WHERE id=?2",
            rusqlite::params![now - 120, task_id],
        )
        .unwrap();

        // Reap.
        quorum_core::sweep::reap_lapsed_tasks(&conn, now + 5, 100).unwrap();
    }

    let task = get_task(home.path(), task_id);
    assert_eq!(
        task.status, "rework",
        "task with active remediation lease must NOT be reaped, got: {}",
        task.status
    );

    let events = events_for_task(home.path(), task_id);
    let was_reclaimed = events.iter().any(|(k, _)| k == "task_reclaimed");
    assert!(
        !was_reclaimed,
        "healthy lease must prevent reaper from emitting task_reclaimed"
    );
}

/// Negative: claim races during remediation produce no errors rows.
#[test]
fn remediation_claim_race_no_errors() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let pr: i64 = 200;
    let task_id = seed_review_only_task(home.path(), pr);

    {
        let mut conn = quorum_core::db::open(&db_path(home.path())).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Walk to rework.
        quorum_core::tasks::apply_event(
            &mut conn,
            "reviewer1",
            task_id,
            &quorum_core::lifecycle::Event::ReviewerAttached {
                agent: "reviewer1".into(),
            },
            now,
        )
        .unwrap();
        quorum_core::tasks::apply_event(
            &mut conn,
            "reviewer1",
            task_id,
            &quorum_core::lifecycle::Event::VerdictChanges,
            now + 1,
        )
        .unwrap();

        // First claim wins.
        let r1 = quorum_core::tasks::claim_remediation_rework(
            &mut conn,
            "agent-a",
            task_id,
            3600,
            now + 2,
        )
        .unwrap();
        assert!(r1.is_some(), "first remediation claim must succeed");

        // Second claim loses — normal race, not an error.
        let r2 = quorum_core::tasks::claim_remediation_rework(
            &mut conn,
            "agent-b",
            task_id,
            3600,
            now + 3,
        )
        .unwrap();
        assert!(r2.is_none(), "second remediation claim must lose the race");
    }

    let errs = errors_count(home.path());
    assert_eq!(
        errs, 0,
        "lost claim race must NOT produce errors rows (invariant #4)"
    );
}
