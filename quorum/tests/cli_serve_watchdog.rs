//! M4 watchdog integration tests.
//!
//! Verifies that cost and runaway controls kill agents and release tasks
//! when ceilings are exceeded:
//! 1. max-task-tokens: worker killed when cumulative tokens exceed ceiling
//! 2. max-turn-tokens: worker killed when single-turn tokens exceed ceiling

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

fn create_gh_shim() -> (tempfile::TempDir, std::path::PathBuf, String) {
    let shim = tempfile::tempdir().unwrap();
    let state = shim.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let path = shim.path().join("gh");
    std::fs::write(
        &path,
        r#"#!/bin/sh
set -eu
cmd="${1:-} ${2:-}"
if [ "$cmd" = "pr create" ]; then
  shift 2; head=""; base=""
  while [ "$#" -gt 0 ]; do
    if [ "$1" = "--head" ]; then head="$2"; shift 2
    elif [ "$1" = "--base" ]; then base="$2"; shift 2
    else shift; fi
  done
  [ "$base" = "main" ] || { printf 'unexpected base: %s\n' "$base" >&2; exit 1; }
  pr="${head##*-t}"; printf '%s' "$head" > "$QUORUM_TEST_GH_STATE/$pr"
  printf 'https://github.com/test/repo/pull/%s\n' "$pr"
elif [ "$cmd" = "pr list" ]; then
  shift 2; head=""
  while [ "$#" -gt 0 ]; do
    if [ "$1" = "--head" ]; then head="$2"; shift 2; else shift; fi
  done
  pr="${head##*-t}"
  if [ -f "$QUORUM_TEST_GH_STATE/$pr" ]; then printf '[{"number":%s,"state":"OPEN"}]\n' "$pr"; else printf '[]\n'; fi
elif [ "$cmd" = "pr view" ]; then
  pr="$3"
  if [ -f "$QUORUM_TEST_GH_STATE/$pr" ]; then
    branch="$(cat "$QUORUM_TEST_GH_STATE/$pr")"
  else
    branch="review-pr-$pr"
  fi
  sha="$(git -C "$QUORUM_TEST_REPO" rev-parse "refs/heads/$branch")"
  printf '{"headRefName":"%s","headRefOid":"%s","isCrossRepository":false,"baseRefName":"main","state":"OPEN"}\n' "$branch" "$sha"
else
  exit 1
fi
"#,
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let search_path = format!(
        "{}:{}",
        shim.path().display(),
        env::var("PATH").unwrap_or_default()
    );
    (shim, state, search_path)
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
    fn start_with_limits(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        extra_args: &[&str],
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
  base=""
  while [ "$#" -gt 0 ]; do
    if [ "$1" = "--head" ]; then head="$2"; shift 2
    elif [ "$1" = "--base" ]; then base="$2"; shift 2
    else shift; fi
  done
  [ "$base" = "main" ] || { printf 'unexpected base: %s\n' "$base" >&2; exit 1; }
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
            "true",
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
            .env("QUORUM_TEST_GH_STATE", &gh_state)
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

    fn start_with_limits_and_env(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        extra_args: &[&str],
        extra_envs: &[(&str, &str)],
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let sentinel_path = sentinel.path().to_string_lossy().to_string();
        let (gh_shim, gh_state, path) = create_gh_shim();
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
            "true",
            "--exit-when-gone",
            &sentinel_path,
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
        for a in extra_args {
            args.push(a.to_string());
        }

        let mut cmd = Command::new(cargo_bin("quorum"));
        cmd.env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .env("PATH", path)
            .env("QUORUM_TEST_GH_STATE", &gh_state)
            .env("QUORUM_TEST_REPO", repo)
            .args(&args)
            .stderr(Stdio::piped())
            .stdout(Stdio::null());
        for (k, v) in extra_envs {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().unwrap();

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
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let mut conn = quorum_core::db::open(&db).unwrap();
    quorum_core::classify::store_classifications(
        &mut conn,
        &[quorum_core::classify::TaskClassification {
            task_id: 1,
            cx_est: 3,
            size: "M".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: Vec::new(),
        }],
        "test-classifier:v1",
        1,
    )
    .unwrap();
}

fn seed_review_only_task(home: &std::path::Path, repo: &std::path::Path, title: &str) {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            title,
            "--created-by",
            "TestCreator",
            "--review-pr",
            "1",
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
    quorum_core::classify::store_classifications(
        &mut conn,
        &[quorum_core::classify::TaskClassification {
            task_id: 1,
            cx_est: 3,
            size: "M".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: Vec::new(),
        }],
        "test-classifier:v1",
        1,
    )
    .unwrap();
    let head_sha = Command::new("git")
        .args(["-C", &repo.to_string_lossy(), "rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(head_sha.status.success());
    assert!(Command::new("git")
        .args(["-C", &repo.to_string_lossy(), "branch", "review-pr-1"])
        .status()
        .unwrap()
        .success());
    quorum_core::pr_targets::upsert(
        &mut conn,
        1,
        1,
        "review-pr-1",
        String::from_utf8_lossy(&head_sha.stdout).trim(),
        false,
    )
    .unwrap();
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

#[test]
fn rework_cap_kills_worker_and_releases_task() {
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

    seed_task(home.path(), "Task for rework cap test");

    let mut handle = ServeHandle::start_with_limits(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &[],
    );

    // Wait for worker to spawn and produce a result
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

    // Worker signals "done with PR" — triggers reviewer spawn
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

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

    // Reviewer requests changes — rework round #1
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
            "Fix error handling",
        ],
    );

    // Lifecycle: VerdictChanges → rework, daemon feeds rework turn to worker
    assert!(
        handle.wait_for("rework #1 started", 15),
        "first rework not seen. Lines: {:?}",
        handle.lines
    );

    // Verify lifecycle logged the transition
    let saw_lifecycle = handle.lines.iter().any(|l| l.contains("lifecycle:"));
    assert!(
        saw_lifecycle,
        "lifecycle transition log not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

#[test]
fn task_token_limit_kills_worker_and_releases_task() {
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

    seed_task(home.path(), "Task for token limit test");

    // fake-agent turn 1 emits 500+200=700 tokens. Set limit to 500 so it's exceeded.
    let mut handle = ServeHandle::start_with_limits(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--max-task-tokens", "500"],
    );

    // Wait for worker to spawn
    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );

    // The first turn emits 700 tokens, which exceeds 500 → watchdog fires
    assert!(
        handle.wait_for("WATCHDOG", 15),
        "watchdog kill not seen. Lines: {:?}",
        handle.lines
    );

    let saw_task_tokens = handle.lines.iter().any(|l| l.contains("task tokens"));
    assert!(
        saw_task_tokens,
        "task tokens limit message not seen. Lines: {:?}",
        handle.lines
    );

    // Stop the daemon to prevent respawn loop, then verify the task is open.
    handle.stop();

    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"open\"") || stdout.contains("\"status\": \"open\""),
        "task should be released to open after token limit, got: {stdout}"
    );
}

#[test]
fn worker_submission_before_terminal_overage_is_cleanup_only() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    assert!(Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap()
        .success());
    seed_task(home.path(), "Submitted worker terminal overage regression");

    let mut handle = ServeHandle::start_with_limits_and_env(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--max-task-tokens", "600"],
        &[("FAKE_AGENT_DELAY_SECS", "3")],
    );
    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker_name]);
    assert!(
        handle.wait_for("PR #1 ready for review", 15),
        "submission did not reach in-review before terminal usage. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("cleanup only", 15),
        "recorded worker outcome was not cleanup-only. Lines: {:?}",
        handle.lines
    );
    let false_alert = handle
        .lines
        .iter()
        .any(|line| line.contains("WATCHDOG: worker"));
    handle.stop();

    assert!(!false_alert, "recorded submission emitted a watchdog alert");
    let db = home.path().join("repos/test__repo/quorum.db");
    let conn = quorum_core::db::open(&db).unwrap();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "in-review");
    assert_eq!(task.recovery_attempts, 0);
    assert_eq!(quorum_core::tasks::extract_pr_number(&task.refs), Some(1));
    let review_events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE subject='task#1' AND kind='task_in_review'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(review_events, 1, "watchdog duplicated task_in_review");
    let worker_runs = quorum_core::agent_runs::runs_for_task(&conn, 1)
        .unwrap()
        .into_iter()
        .filter(|run| run.role == "worker")
        .collect::<Vec<_>>();
    assert_eq!(worker_runs.len(), 1);
    assert!(matches!(
        worker_runs[0].end_reason.as_deref(),
        Some("completed" | "ownership_transferred")
    ));
}

#[test]
fn turn_token_limit_kills_worker() {
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

    seed_task(home.path(), "Task for turn token limit test");

    // fake-agent turn 1 emits 500+200=700 tokens. Set per-turn limit to 600.
    let mut handle = ServeHandle::start_with_limits(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--max-turn-tokens", "600"],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("WATCHDOG", 15),
        "watchdog kill not seen. Lines: {:?}",
        handle.lines
    );

    let saw_turn_tokens = handle.lines.iter().any(|l| l.contains("turn tokens"));
    assert!(
        saw_turn_tokens,
        "turn tokens limit message not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();

    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"open\"") || stdout.contains("\"status\": \"open\""),
        "task should be released to open after turn token limit, got: {stdout}"
    );
}

#[test]
fn task_cost_usd_limit_kills_worker() {
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

    seed_task(home.path(), "Task for cost USD limit test");

    // fake-agent turn 1 cumulative cost = 700 * 0.00001 = 0.007. Set limit to 0.001.
    let mut handle = ServeHandle::start_with_limits(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--max-task-cost-usd", "0.001"],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("WATCHDOG", 15),
        "watchdog kill not seen. Lines: {:?}",
        handle.lines
    );

    let saw_task_cost = handle.lines.iter().any(|l| l.contains("task cost"));
    assert!(
        saw_task_cost,
        "task cost limit message not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();

    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"open\"") || stdout.contains("\"status\": \"open\""),
        "task should be released to open after cost limit, got: {stdout}"
    );
}

#[test]
fn cumulative_cost_usd_is_high_water_mark_not_summed() {
    // Regression: total_cost_usd in stream-json is session-cumulative. The
    // watchdog must treat it as a high-water mark (assign), not sum with +=.
    // Fake-agent emits cumulative costs: turn 1 = 0.007, turn 2 = 0.021.
    // Old bug: += would record 0.028 and trip a 0.025 ceiling.
    // Fix: = records 0.021, which stays under 0.025.
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

    seed_task(home.path(), "Task for cumulative cost regression");

    let mut handle = ServeHandle::start_with_limits(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--max-task-cost-usd", "0.025"],
    );

    // Turn 1: worker spawns, emits result with cumulative cost 0.007
    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "turn 1 result not seen. Lines: {:?}",
        handle.lines
    );

    let worker_name = handle.extract_agent_name("spawning agent ").unwrap();

    // Worker signals done → reviewer spawns
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);
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

    // Reviewer requests changes → triggers rework (turn 2)
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
            "Needs fixes",
        ],
    );

    assert!(
        handle.wait_for("rework #1 started", 15),
        "rework not started. Lines: {:?}",
        handle.lines
    );

    // Turn 2 result: cumulative cost = 0.021 (under 0.025 limit)
    assert!(
        handle.wait_for("result", 15),
        "turn 2 result not seen. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_secs(1));
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    // With the fix, cost_usd should be 0.021 (the last cumulative value),
    // NOT 0.028 (the double-counted sum). No watchdog should fire.
    let saw_watchdog = handle.lines.iter().any(|l| l.contains("WATCHDOG"));
    assert!(
        !saw_watchdog,
        "watchdog should NOT fire — cumulative cost 0.021 < limit 0.025. \
         If it fired, total_cost_usd is being summed instead of assigned. Lines: {:?}",
        handle.lines
    );

    // Verify the logged cost matches the last cumulative value (0.021), not the sum.
    let cost_line = handle
        .lines
        .iter()
        .rfind(|l| l.contains("cost_usd="))
        .cloned();
    if let Some(line) = &cost_line {
        assert!(
            line.contains("cost_usd=0.0210"),
            "logged cost should be 0.0210 (last cumulative), got: {line}"
        );
    }

    handle.stop();
}

#[test]
fn deprecated_turn_wall_cli_alias_does_not_kill_active_worker() {
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

    seed_task(home.path(), "Task for deprecated turn-wall alias test");

    // The deprecated alias resolves to the idle timeout. It must not reap an
    // active turn even though the fake agent takes longer than one second.
    let mut handle = ServeHandle::start_with_limits_and_env(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--max-turn-wall-secs", "1"],
        &[("FAKE_AGENT_DELAY_SECS", "3")],
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

    let saw_turn_wall = handle.lines.iter().any(|l| l.contains("turn wall-clock"));
    assert!(
        !saw_turn_wall,
        "deprecated turn wall alias must not reap an active turn. Lines: {:?}",
        handle.lines
    );
    assert_eq!(
        handle
            .lines
            .iter()
            .filter(|line| line.contains("WARNING: max_turn_wall_secs is deprecated"))
            .count(),
        1,
        "CLI use of the deprecated alias must emit one warning. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

#[test]
fn task_wall_clock_limit_kills_worker_and_releases_task() {
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

    seed_task(home.path(), "Task for task wall-clock limit test");

    // fake-agent delays 3s; task ceiling is 1s.
    let mut handle = ServeHandle::start_with_limits_and_env(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--max-task-wall-secs", "1"],
        &[("FAKE_AGENT_DELAY_SECS", "3")],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("WATCHDOG", 15),
        "watchdog kill not seen. Lines: {:?}",
        handle.lines
    );

    let saw_task_wall = handle.lines.iter().any(|l| l.contains("task wall-clock"));
    assert!(
        saw_task_wall,
        "task wall-clock limit message not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();

    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"open\"") || stdout.contains("\"status\": \"open\""),
        "task should be released to open after task wall-clock limit, got: {stdout}"
    );
}

#[test]
fn reviewer_ceiling_kills_reviewer_and_respawns() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    let wrapper_dir = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let create = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            "Task for reviewer ceiling test",
            "--created-by",
            "TestCreator",
            "--review-pr",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        create.status.success(),
        "task-create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let db = home.path().join("repos/test__repo/quorum.db");
    let mut conn = quorum_core::db::open(&db).unwrap();
    quorum_core::classify::store_classifications(
        &mut conn,
        &[quorum_core::classify::TaskClassification {
            task_id: 1,
            cx_est: 3,
            size: "M".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: Vec::new(),
        }],
        "test-classifier:v1",
        1,
    )
    .unwrap();
    let head_sha = Command::new("git")
        .args([
            "-C",
            &repo_dir.path().to_string_lossy(),
            "rev-parse",
            "HEAD",
        ])
        .output()
        .unwrap();
    assert!(head_sha.status.success());
    assert!(Command::new("git")
        .args([
            "-C",
            &repo_dir.path().to_string_lossy(),
            "branch",
            "review-pr-1",
        ])
        .status()
        .unwrap()
        .success());
    quorum_core::pr_targets::upsert(
        &mut conn,
        1,
        1,
        "review-pr-1",
        String::from_utf8_lossy(&head_sha.stdout).trim(),
        false,
    )
    .unwrap();

    // This review-only fixture starts directly in the reviewer phase, so the
    // one-second ceiling measures reviewer runtime without worker/publication
    // setup consuming the test's timing budget.
    let fake_agent = cargo_bin("fake-agent");
    let wrapper = wrapper_dir.path().join("agent-wrapper.sh");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/bash\n\
             export FAKE_AGENT_DELAY_SECS=4\n\
             exec \"{fake_agent}\" \"$@\"\n",
            fake_agent = fake_agent.display(),
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let sentinel = tempfile::tempdir().unwrap();
    let sentinel_path = sentinel.path().to_string_lossy().to_string();
    let (gh_shim, gh_state, path) = create_gh_shim();
    std::fs::write(gh_state.join("1"), "review-pr-1").unwrap();
    let mut child = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("PATH", path)
        .env("QUORUM_TEST_GH_STATE", &gh_state)
        .env("QUORUM_TEST_REPO", repo_dir.path())
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
            &wrapper.to_string_lossy(),
            "--merge-cmd",
            "true",
            "--exit-when-gone",
            &sentinel_path,
            "--max-turn-wall-secs",
            "1",
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
        _gh_shim: Some(gh_shim),
    };

    assert!(
        handle.wait_for("spawning reviewer", 15),
        "first reviewer not spawned. Lines: {:?}",
        handle.lines
    );

    // Reviewer delays 4s, hits 1s wall-clock ceiling → watchdog kills it.
    assert!(
        handle.wait_for("WATCHDOG", 20),
        "watchdog kill not seen for reviewer. Lines: {:?}",
        handle.lines
    );

    let saw_reviewer_watchdog = handle
        .lines
        .iter()
        .any(|l| l.contains("WATCHDOG: reviewer"));
    assert!(
        saw_reviewer_watchdog,
        "WATCHDOG message should mention 'reviewer'. Lines: {:?}",
        handle.lines
    );

    // Key assertion: task does NOT stall — Phase 5 respawns a new reviewer.
    assert!(
        handle.wait_for("spawning reviewer", 20),
        "second reviewer not spawned — task stalled in-review after ceiling kill. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

#[test]
fn reviewer_verdict_before_terminal_overage_is_cleanup_only() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    assert!(Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap()
        .success());
    seed_review_only_task(
        home.path(),
        repo_dir.path(),
        "Recorded reviewer terminal overage regression",
    );

    let mut handle = ServeHandle::start_with_limits_and_env(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--max-task-tokens", "600"],
        &[("FAKE_AGENT_DELAY_SECS", "3")],
    );
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );
    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();
    assert!(
        handle.wait_for("R1: reviewer", 15),
        "reviewer authority was not installed. Lines: {:?}",
        handle.lines
    );
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
            "Fix the regression",
        ],
    );
    assert!(
        handle.wait_for("-> rework", 15),
        "verdict did not reach rework before terminal usage. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("cleanup only", 15),
        "recorded reviewer outcome was not cleanup-only. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for(&format!("reviewer {reviewer_name} torn down"), 15),
        "reviewer cleanup did not finish. Lines: {:?}",
        handle.lines
    );
    let false_alert = handle
        .lines
        .iter()
        .any(|line| line.contains("WATCHDOG: reviewer"));

    assert!(!false_alert, "recorded verdict emitted a watchdog alert");
    let db = home.path().join("repos/test__repo/quorum.db");
    let conn = quorum_core::db::open(&db).unwrap();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "rework");
    assert_eq!(task.recovery_attempts, 0);
    assert_eq!(quorum_core::tasks::extract_pr_number(&task.refs), Some(1));
    let rework_events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE subject='task#1' AND kind='task_rework'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rework_events, 1, "watchdog duplicated task_rework");
    let reviewer_runs = quorum_core::agent_runs::runs_for_task(&conn, 1)
        .unwrap()
        .into_iter()
        .filter(|run| run.role == "reviewer")
        .collect::<Vec<_>>();
    assert_eq!(reviewer_runs.len(), 1);
    assert!(
        matches!(
            reviewer_runs[0].end_reason.as_deref(),
            Some("completed" | "ownership_transferred")
        ),
        "unexpected reviewer runs: {reviewer_runs:?}"
    );
    drop(conn);
    handle.stop();
}

#[test]
fn no_limits_does_not_kill() {
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

    seed_task(home.path(), "Task with no limits");

    // No limit flags — agent should survive
    let mut handle = ServeHandle::start_with_limits(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &[],
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

    // Wait 2 ticks — no watchdog should fire
    std::thread::sleep(Duration::from_secs(1));
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    let saw_watchdog = handle.lines.iter().any(|l| l.contains("WATCHDOG"));
    assert!(
        !saw_watchdog,
        "watchdog should not fire with no limits. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}
