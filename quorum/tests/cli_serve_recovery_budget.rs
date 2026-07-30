//! Recovery budget regression: lock the incident recovery behavior across daemon restarts.
//!
//! Exercises:
//! 1. Repeated worker failures with a daemon restart between attempts.
//! 2. SQLite recovery_attempts persists across restarts.
//! 3. Existing task branch is resumed, not recreated from base — branch-exists must not
//!    cause an instant-death loop.
//! 4. Third reopen → fourth failure cancels with durable diagnostics, no fourth worker.
//! 5. Success case: replacement resumes branch, reaches in-review, resets recovery state.
//!
//! Asserts task/event/error/agent-run state in the DB, not only console lines.

use std::env;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

mod common;

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
    fn start_with_agent_bin(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        agent_bin: &str,
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

struct TestEnv {
    home: tempfile::TempDir,
    repo_dir: tempfile::TempDir,
    wt_base: tempfile::TempDir,
    names_file: std::path::PathBuf,
    db_path: std::path::PathBuf,
    die_agent: std::path::PathBuf,
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

        // Agent binary that exits immediately with code 1 (simulates crash).
        // Produces some stdout so it doesn't trigger the instant-death (poison)
        // path — we want it to go through the normal AgentFailed recovery path.
        let die_agent = home.path().join("die-agent.sh");
        std::fs::write(
            &die_agent,
            "#!/bin/sh\n\
             # Read one line from stdin (the prompt) so the daemon sees a turn start\n\
             read -r _line 2>/dev/null || true\n\
             # Emit a minimal result with nonzero cost_tokens to avoid poison detection\n\
             printf '{\"type\":\"assistant\",\"message\":{\"content\":\"working\"}}\\n'\n\
             printf '{\"type\":\"result\",\"result\":\"crash\",\"usage\":{\"input_tokens\":100,\"output_tokens\":50},\"total_cost_usd\":0.001,\"num_turns\":1,\"duration_ms\":100,\"is_error\":false}\\n'\n\
             # Now exit — daemon will see EOF on stdin and detect death on next tick\n\
             exit 0\n",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&die_agent, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        TestEnv {
            home,
            repo_dir,
            wt_base,
            names_file,
            db_path,
            die_agent,
        }
    }

    fn start_serve(&self) -> ServeHandle {
        ServeHandle::start_with_agent_bin(
            self.home.path(),
            self.repo_dir.path(),
            self.wt_base.path(),
            &self.names_file,
            &self.die_agent.to_string_lossy(),
        )
    }

    fn start_serve_with_bin(&self, bin: &str) -> ServeHandle {
        ServeHandle::start_with_agent_bin(
            self.home.path(),
            self.repo_dir.path(),
            self.wt_base.path(),
            &self.names_file,
            bin,
        )
    }

    fn task(&self, id: i64) -> quorum_core::tasks::Task {
        let conn = quorum_core::db::open(&self.db_path).unwrap();
        quorum_core::tasks::get(&conn, id).unwrap().unwrap()
    }

    fn agent_runs(&self, task_id: i64) -> Vec<quorum_core::agent_runs::AgentRun> {
        let conn = quorum_core::db::open(&self.db_path).unwrap();
        quorum_core::agent_runs::runs_for_task(&conn, task_id).unwrap()
    }

    fn events_for_task(&self, task_id: i64) -> Vec<quorum_core::events::Event> {
        let conn = quorum_core::db::open(&self.db_path).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let subject = format!("task#{task_id}");
        quorum_core::events::list(&conn, 0, Some(&subject), 1000, now).unwrap()
    }

    fn messages_for_task(&self, task_id: i64) -> Vec<String> {
        let conn = quorum_core::db::open(&self.db_path).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut stmt = conn
            .prepare(
                "SELECT body FROM messages WHERE refs = ?1 AND expires_at > ?2 ORDER BY ts ASC",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![format!("task:{task_id}"), now], |r| {
            r.get::<_, String>(0)
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }

    fn create_task_branch(&self, task_id: i64) {
        let d = self.repo_dir.path().to_string_lossy().to_string();
        let branch = format!("daemon/agent0-t{task_id}");
        Command::new("git")
            .args(["-C", &d, "checkout", "-b", &branch])
            .status()
            .unwrap();
        Command::new("git")
            .args(["-C", &d, "commit", "--allow-empty", "-m", "worker WIP"])
            .status()
            .unwrap();
        Command::new("git")
            .args(["-C", &d, "checkout", "main"])
            .status()
            .unwrap();
    }

    fn branch_exists(&self, branch: &str) -> bool {
        let d = self.repo_dir.path().to_string_lossy().to_string();
        Command::new("git")
            .args(["-C", &d, "rev-parse", "--verify", branch])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
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
    let db = home.join("repos/test__repo/quorum.db");
    let mut conn = quorum_core::db::open(&db).unwrap();
    let task_id: i64 = conn
        .query_row("SELECT MAX(id) FROM tasks", [], |row| row.get(0))
        .unwrap();
    quorum_core::classify::store_classifications(
        &mut conn,
        &[quorum_core::classify::TaskClassification {
            task_id,
            cx_est: 3,
            size: "M".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: vec![],
        }],
        "test-classifier:v2",
        quorum_core::clock::now(),
    )
    .unwrap();
}

/// Core regression test: recovery budget persists across daemon restarts and
/// exactly three recovery attempts occur before cancellation.
///
/// Strategy: seed a task at recovery_attempts=1 (one prior crash), put it in
/// "working" state, then SIGKILL the daemon. On restart, recovery fires
/// AgentFailed (count→2). The die-agent spawns and dies (count→3), then
/// spawns again and dies (count=3 + check >=3 → cancelled).
///
/// This exercises the full restart-spanning path: persistence of the counter
/// in SQLite, branch survival across SIGKILL (no teardown ran), branch reuse
/// on respawn, and eventual budget exhaustion.
#[test]
fn recovery_budget_exhaustion_across_daemon_restart() {
    let env = TestEnv::new();

    // Seed task with 1 prior crash, currently in "working" state
    let task_id = {
        let mut conn = quorum_core::db::open(&env.db_path).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "Budget exhaustion test",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_by":"test-classifier:v2"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        // One prior crash: recovery_attempts = 1
        quorum_core::tasks::claim(&mut conn, "W-prior", Some(id), &[], 3600, now).unwrap();
        quorum_core::tasks::apply_event(
            &mut conn,
            "daemon",
            id,
            &quorum_core::lifecycle::Event::AgentFailed {
                reason: "worker process died".into(),
            },
            now + 1,
        )
        .unwrap();
        // Claim again to put in "working" (simulates a worker that was running
        // when the daemon was killed)
        quorum_core::tasks::claim(&mut conn, "W-active", Some(id), &[], 86400, now + 2).unwrap();
        id
    };

    // Pre-conditions
    let task = env.task(task_id);
    assert_eq!(task.recovery_attempts, 1);
    assert_eq!(task.status, "working");

    // Create a branch (simulates the worker's commit before daemon crash)
    let branch_name = format!("daemon/agent0-t{task_id}");
    env.create_task_branch(task_id);
    assert!(env.branch_exists(&branch_name));

    // ── SIGKILL scenario: daemon never ran teardown ─────────────────────
    // (We just directly set up state as if a daemon had been killed.)
    // The recovery_attempts=1 and status=working already simulate this.
    // Verify persistence before starting a daemon.

    // ── Start daemon: recovery fires AgentFailed on working task ─────────
    let mut handle = env.start_serve();

    assert!(
        handle.wait_for("recovery: complete", 20),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // Branch must survive recovery (recovery GCs worktrees, not branches)
    assert!(
        env.branch_exists(&branch_name),
        "branch must survive recovery (only worktrees are GC'd)"
    );

    // Recovery incremented counter to 2, task is now open. Die-agent spawns
    // and dies: count→3, task reopens. Spawns again and dies: check 3>=3 →
    // failed/parked. The daemon logs the lifecycle transition.
    assert!(
        handle.wait_for("-> failed", 30),
        "task not parked after restart + budget exhaustion. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    // ── Assertions on final DB state ────────────────────────────────────

    let task = env.task(task_id);
    assert_eq!(
        task.status, "failed",
        "task must be parked after budget exhaustion"
    );
    assert_eq!(
        task.recovery_attempts,
        quorum_core::tasks::MAX_RECOVERY_ATTEMPTS,
        "final recovery_attempts must equal MAX_RECOVERY_ATTEMPTS ({})",
        quorum_core::tasks::MAX_RECOVERY_ATTEMPTS
    );

    // No further worker spawns after parking.
    let park_idx = handle.lines.iter().position(|l| l.contains("-> failed"));
    if let Some(idx) = park_idx {
        let spawns_after = handle.lines[idx..]
            .iter()
            .filter(|l| l.contains("spawning agent") && l.contains(&format!("task #{task_id}")))
            .count();
        assert_eq!(
            spawns_after, 0,
            "no worker must spawn after budget exhaustion"
        );
    }

    // Events table records failure, never cancellation.
    let events = env.events_for_task(task_id);
    let failed_events: Vec<_> = events.iter().filter(|e| e.kind == "task_failed").collect();
    assert_eq!(
        failed_events.len(),
        1,
        "exactly one task_failed event expected, got {}",
        failed_events.len()
    );
    assert!(!events.iter().any(|e| e.kind == "task_cancelled"));

    // Messages table must contain the owner alert with failure cause
    let msgs = env.messages_for_task(task_id);
    let exhaustion_msgs: Vec<_> = msgs
        .iter()
        .filter(|m| m.contains("recovery budget exhausted"))
        .collect();
    assert!(
        !exhaustion_msgs.is_empty(),
        "owner alert with 'recovery budget exhausted' must be in messages table"
    );

    // Agent runs: workers that spawned are recorded
    let runs = env.agent_runs(task_id);
    let worker_runs: Vec<_> = runs.iter().filter(|r| r.role == "worker").collect();
    assert!(
        !worker_runs.is_empty(),
        "agent_runs must have entries for spawned workers"
    );
    for run in &worker_runs {
        assert!(
            run.ended_at.is_some(),
            "all worker runs must have ended_at set, run {} missing it",
            run.id
        );
    }

    handle.sigkill();
}

#[test]
fn explicit_retry_of_parked_rework_spawns_replacement_worker() {
    let env = TestEnv::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let task_id = {
        let mut conn = quorum_core::db::open(&env.db_path).unwrap();
        let id = quorum_core::tasks::create(
            &mut conn,
            "owner",
            "Parked rework retry",
            Some("preserve original context"),
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_by":"test-classifier:v2"}"#),
            None,
            Some(1),
            now,
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks
             SET status='rework', assignee='dead-worker', author='Agent0',
                 rework_round=1, review_only=0
             WHERE id=?1",
            rusqlite::params![id],
        )
        .unwrap();
        quorum_core::tasks::park(
            &mut conn,
            id,
            "recovery budget exhausted",
            "rework",
            now + 1,
        )
        .unwrap()
        .unwrap();
        id
    };

    let retry = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", env.home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-retry",
            "--task-id",
            &task_id.to_string(),
            "--by",
            "operator",
        ])
        .output()
        .unwrap();
    assert!(
        retry.status.success(),
        "task-retry failed: {}",
        String::from_utf8_lossy(&retry.stderr)
    );

    let fake_agent = cargo_bin("fake-agent");
    let mut handle = env.start_serve_with_bin(&fake_agent.to_string_lossy());
    let spawned = handle.wait_for("spawning agent", 15);
    let after_wait = env.task(task_id);
    assert!(
        spawned,
        "retried rework never reached worker spawn; task={after_wait:?}; lines={:?}",
        handle.lines,
    );
    assert!(
        handle.lines.iter().any(|line| {
            line.contains("spawning agent") && line.contains(&format!("task #{task_id}"))
        }),
        "retried rework must spawn a replacement worker: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("worktree provisioned", 15),
        "replacement worker never completed claim/provision: {:?}",
        handle.lines
    );

    let task = env.task(task_id);
    assert!(
        task.assignee.is_some() || task.status == "in-review",
        "replacement must claim or complete the same task, got: {task:?}"
    );
    let events = env.events_for_task(task_id);
    assert!(
        events.iter().any(|event| event.kind == "task_claimed"),
        "replacement claim must be durable: {events:?}"
    );
    handle.sigkill();
}

/// Success case: replacement worker resumes the existing branch, reaches
/// in-review, and the recovery counter resets to 0.
#[test]
fn recovery_budget_resets_on_successful_replacement() {
    let env = TestEnv::new();
    seed_task(env.home.path(), "Recovery reset test");

    // Pre-create a branch for the task
    env.create_task_branch(1);

    // Seed the task into a state with recovery_attempts = 2 (two prior crashes)
    // by directly manipulating the DB — simulating two prior worker deaths.
    {
        let mut conn = quorum_core::db::open(&env.db_path).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Claim and crash twice to set recovery_attempts = 2
        quorum_core::tasks::claim(&mut conn, "W-prev", Some(1), &[], 3600, now).unwrap();
        quorum_core::tasks::apply_event(
            &mut conn,
            "daemon",
            1,
            &quorum_core::lifecycle::Event::AgentFailed {
                reason: "worker process died".into(),
            },
            now + 1,
        )
        .unwrap();
        quorum_core::tasks::claim(&mut conn, "W-prev2", Some(1), &[], 3600, now + 2).unwrap();
        quorum_core::tasks::apply_event(
            &mut conn,
            "daemon",
            1,
            &quorum_core::lifecycle::Event::AgentFailed {
                reason: "worker process died".into(),
            },
            now + 3,
        )
        .unwrap();

        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(task.recovery_attempts, 2, "pre-condition: 2 prior crashes");
        assert_eq!(task.status, "open");
    }

    // Use the real fake-agent that stays alive and produces valid results
    let fake_agent = cargo_bin("fake-agent");
    let mut handle = env.start_serve_with_bin(&fake_agent.to_string_lossy());

    // Worker spawns, does work, produces result
    assert!(
        handle.wait_for("spawning agent", 20),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 20),
        "worker result not seen. Lines: {:?}",
        handle.lines
    );
    assert!(Command::new("git")
        .args([
            "-C",
            &env.wt_base.path().join("Agent0-t1").to_string_lossy(),
            "commit",
            "--allow-empty",
            "-m",
            "replacement completed",
        ])
        .status()
        .unwrap()
        .success());

    // Extract agent name for the done signal
    let agent_name = handle
        .lines
        .iter()
        .find_map(|l| {
            l.split("spawning agent ")
                .nth(1)
                .map(|rest| rest.split_whitespace().next().unwrap_or("").to_string())
        })
        .expect("could not extract agent name");

    // Signal done with a PR (moves to in-review)
    let run_id = {
        let conn = quorum_core::db::open(&env.db_path).unwrap();
        match quorum_core::capabilities::active_for_agent(&conn, &agent_name).unwrap() {
            Some(cap) => cap.run_id,
            None => {
                let mut conn2 = quorum_core::db::open(&env.db_path).unwrap();
                let rid = format!("test-{agent_name}-reset");
                quorum_core::capabilities::issue(&mut conn2, &rid, 0, &agent_name, "worker", 1000)
                    .unwrap();
                rid
            }
        }
    };

    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", env.home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", &run_id)
        .args(["done", "--agent", &agent_name])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "done --pr failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Wait for daemon to acknowledge
    assert!(
        handle.wait_for("PR #1 ready for review", 15),
        "daemon did not acknowledge PR. Lines: {:?}",
        handle.lines
    );

    // ── Assertions ──────────────────────────────────────────────────────
    let task = env.task(1);
    assert_eq!(
        task.status, "in-review",
        "task must be in-review after done"
    );
    assert_eq!(
        task.recovery_attempts, 0,
        "recovery_attempts must reset to 0 on successful transition to in-review"
    );

    // Events: should have task_in_review event
    let events = env.events_for_task(1);
    let in_review_events: Vec<_> = events
        .iter()
        .filter(|e| e.kind == "task_in_review")
        .collect();
    assert!(
        !in_review_events.is_empty(),
        "task_in_review event must exist after successful submission"
    );

    // Branch is preserved (reused by worker, not recreated)
    assert!(
        env.branch_exists("daemon/agent0-t1"),
        "task branch must still exist"
    );

    handle.sigkill();
}

/// Branch-exists does not cause instant-death loop: when an existing branch
/// is present from a prior worker, the replacement worker's worktree provision
/// reuses it without triggering the poison tracker's instant-death detection.
#[test]
fn existing_branch_does_not_trigger_instant_death_loop() {
    let env = TestEnv::new();
    seed_task(env.home.path(), "Branch-exists stability test");

    // Create the branch that would normally come from a prior worker
    env.create_task_branch(1);

    // Use the standard fake-agent (stays alive, emits valid result)
    let fake_agent = cargo_bin("fake-agent");
    let mut handle = env.start_serve_with_bin(&fake_agent.to_string_lossy());

    // Worker must spawn successfully
    assert!(
        handle.wait_for("spawning agent", 20),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );

    // Worktree must be provisioned (proves branch-exists path works)
    assert!(
        handle.wait_for("worktree provisioned", 15),
        "worktree not provisioned. Lines: {:?}",
        handle.lines
    );

    // Worker must produce a result (not die instantly)
    assert!(
        handle.wait_for("result", 20),
        "worker result not seen — may have instant-death'd. Lines: {:?}",
        handle.lines
    );

    // Give time for any poison detection to fire
    std::thread::sleep(Duration::from_millis(1000));
    handle.drain_pending_lines();

    // No poison cancellation
    let poison_lines: Vec<_> = handle
        .lines
        .iter()
        .filter(|l| l.contains("poison") || l.contains("instant death"))
        .collect();
    assert!(
        poison_lines.is_empty(),
        "no poison/instant-death detection should fire. Lines: {:?}",
        poison_lines
    );

    // Task must NOT be cancelled
    let task = env.task(1);
    assert_ne!(
        task.status, "cancelled",
        "task must not be cancelled by branch-exists confusion"
    );

    handle.sigkill();
}

/// The daemon restart recovery path increments recovery_attempts for each
/// working task — verify this persists and accumulates across two consecutive
/// restarts without any worker needing to spawn in between.
///
/// Strategy: seed a task in "working" state, start+kill daemon twice in
/// rapid succession (so recovery fires AgentFailed each time). Verify the
/// counter increments on each restart.
#[test]
fn consecutive_restarts_accumulate_recovery_count() {
    let env = TestEnv::new();

    let db_path = &env.db_path;
    let task_id = {
        let mut conn = quorum_core::db::open(db_path).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "Consecutive restart test",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_by":"test-classifier:v2"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        quorum_core::tasks::claim(&mut conn, "W1", Some(id), &[], 86400, now).unwrap();
        id
    };

    assert_eq!(env.task(task_id).status, "working");
    assert_eq!(env.task(task_id).recovery_attempts, 0);

    // ── Restart 1: working → open (recovery_attempts 0 → 1) ────────────
    let mut handle = env.start_serve();
    assert!(
        handle.wait_for("recovery: complete", 15),
        "restart 1 recovery failed. Lines: {:?}",
        handle.lines
    );
    // Recovery fires AgentFailed, task goes to open, counter = 1
    // SIGKILL immediately before the die-agent can do further damage
    handle.sigkill();

    let task = env.task(task_id);
    assert_eq!(
        task.recovery_attempts, 1,
        "after restart 1, recovery_attempts should be 1"
    );
    // Task should be open (or working if the die-agent already spawned)
    // Either way, we need it in "working" for the next restart.
    // Force it back to working if needed.
    if task.status == "open" {
        let mut conn = quorum_core::db::open(db_path).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        quorum_core::tasks::claim(&mut conn, "W2", Some(task_id), &[], 86400, now).unwrap();
    }
    assert_eq!(env.task(task_id).status, "working");

    // ── Restart 2: working → open (recovery_attempts 1 → 2) ────────────
    let mut handle2 = env.start_serve();
    assert!(
        handle2.wait_for("recovery: complete", 15),
        "restart 2 recovery failed. Lines: {:?}",
        handle2.lines
    );
    handle2.sigkill();

    let task = env.task(task_id);
    assert_eq!(
        task.recovery_attempts, 2,
        "after restart 2, recovery_attempts should be 2"
    );
    // Task is open again (not cancelled — budget is 3, we're at 2)
    assert_ne!(
        task.status, "cancelled",
        "task should NOT be cancelled at recovery_attempts=2 (budget is 3)"
    );
}

/// Third failure produces cancellation with durable diagnostics:
/// task_notes row, messages alert, events entry — all queryable after the
/// fact (not just console output).
#[test]
fn exhaustion_produces_durable_diagnostics() {
    let env = TestEnv::new();

    // Directly seed a task at recovery_attempts = 2 (one attempt away from
    // exhaustion) in "working" state.
    let task_id = {
        let mut conn = quorum_core::db::open(&env.db_path).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "Diagnostics test",
            None,
            0,
            None,
            Some(r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_by":"test-classifier:v2"}"#),
            None,
            None,
            now,
        )
        .unwrap();
        // Simulate 3 prior crashes to set recovery_attempts = 3
        for i in 0..3 {
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
        // Now recovery_attempts = 3, task is open
        let task = quorum_core::tasks::get(&conn, id).unwrap().unwrap();
        assert_eq!(task.recovery_attempts, 3);
        assert_eq!(task.status, "open");

        // Claim it to put in "working" — next AgentFailed will exhaust budget
        quorum_core::tasks::claim(&mut conn, "W-final", Some(id), &[], 3600, now + 50).unwrap();
        id
    };

    // Start daemon — the die-agent will exit, daemon detects death and fires
    // the 4th AgentFailed → budget exhausted → failed/parked
    let mut handle = env.start_serve();

    // Recovery fires AgentFailed for the working task (it was working when
    // the daemon started and has no journal entry, so recovery resets it)
    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // The recovery AgentFailed should park it (attempts=3, check >=3).
    std::thread::sleep(Duration::from_millis(1000));
    handle.drain_pending_lines();

    let task = env.task(task_id);
    assert_eq!(
        task.status, "failed",
        "task must be parked after budget exhaustion. Lines: {:?}",
        handle.lines
    );

    // Durable diagnostics: owner alert in messages table
    let msgs = env.messages_for_task(task_id);
    let has_budget_msg = msgs.iter().any(|m| m.contains("recovery budget exhausted"));
    assert!(
        has_budget_msg,
        "messages table must contain 'recovery budget exhausted' alert. Found: {:?}",
        msgs
    );

    // Durable diagnostics: failed event and no cancellation event.
    let events = env.events_for_task(task_id);
    let failed_events: Vec<_> = events.iter().filter(|e| e.kind == "task_failed").collect();
    assert!(
        !failed_events.is_empty(),
        "events table must contain task_failed event"
    );
    assert!(!events.iter().any(|e| e.kind == "task_cancelled"));

    // No worker must spawn for a parked task.
    let spawns_for_task: Vec<_> = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent") && l.contains(&format!("task #{task_id}")))
        .collect();
    // The task was parked during recovery, so no spawn should happen at all.
    assert!(
        spawns_for_task.is_empty(),
        "no worker should spawn for a task parked during recovery. Found: {:?}",
        spawns_for_task
    );

    handle.sigkill();
}
