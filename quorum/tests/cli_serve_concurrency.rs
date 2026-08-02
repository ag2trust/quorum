//! M3 concurrency tests: multi-worker spawn, priority ordering,
//! independent lifecycle, SIGINT teardown with multiple workers.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use std::{env, os::unix::fs::PermissionsExt};

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
    /// #201: sentinel file whose presence keeps the serve process alive.
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
        cap: usize,
    ) -> Self {
        Self::start_with_merge(home, repo, wt_base, names, cap, "true")
    }

    fn start_with_merge(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        cap: usize,
        merge_cmd: &str,
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
                &cap.to_string(),
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

    fn wait_for_n(&mut self, needle: &str, n: usize, timeout_secs: u64) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
        // Earlier waits may already have consumed matching lines. Count the
        // complete observed stream so call ordering cannot hide an event.
        let mut count = self
            .lines
            .iter()
            .filter(|line| line.contains(needle))
            .count();
        if count >= n {
            return true;
        }
        while std::time::Instant::now() < deadline {
            let remaining = deadline - std::time::Instant::now();
            match self.rx.recv_timeout(remaining) {
                Ok(line) => {
                    if line.contains(needle) {
                        count += 1;
                    }
                    self.lines.push(line);
                    if count >= n {
                        return true;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => return false,
                Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            }
        }
        false
    }

    fn extract_all_agent_names(&self, prefix: &str) -> Vec<String> {
        let mut names = Vec::new();
        for line in &self.lines {
            if let Some(rest) = line.split(prefix).nth(1) {
                if let Some(name) = rest.split_whitespace().next() {
                    names.push(name.to_string());
                }
            }
        }
        names
    }

    fn drain_available(&mut self) {
        while let Ok(line) = self.rx.try_recv() {
            self.lines.push(line);
        }
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

fn seed_task_with_priority(home: &std::path::Path, title: &str, priority: i64) {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            title,
            "--created-by",
            "TestCreator",
            "--priority",
            &priority.to_string(),
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

fn get_task_status(home: &std::path::Path, task_id: i64) -> String {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", &task_id.to_string()])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let task: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    task["status"].as_str().unwrap_or("unknown").to_string()
}

// ── M3: Multi-worker spawn ─────────────────────────────────────────────

#[test]
fn cap_2_spawns_two_workers_concurrently() {
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

    seed_task(home.path(), "Task A");
    seed_task(home.path(), "Task B");

    let mut handle =
        ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file, 2);

    // Wait for two workers to spawn.
    assert!(
        handle.wait_for_n("spawning agent", 2, 20),
        "expected 2 worker spawns. Lines: {:?}",
        handle.lines
    );

    // Both workers should produce results.
    assert!(
        handle.wait_for_n("result", 2, 20),
        "expected 2 worker results. Lines: {:?}",
        handle.lines
    );

    let agents = handle.extract_all_agent_names("spawning agent ");
    assert_eq!(
        agents.len(),
        2,
        "expected 2 distinct agents, got: {agents:?}"
    );
    assert_ne!(
        agents[0], agents[1],
        "two workers must have different names"
    );

    handle.stop();
}

// ── M3: Cap limits concurrency ─────────────────────────────────────────

#[test]
fn cap_limits_concurrent_workers() {
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

    // Seed 3 tasks but cap=2 — only 2 should spawn initially.
    seed_task(home.path(), "Task A");
    seed_task(home.path(), "Task B");
    seed_task(home.path(), "Task C");

    let mut handle =
        ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file, 2);

    // Wait for two workers to spawn.
    assert!(
        handle.wait_for_n("spawning agent", 2, 20),
        "expected 2 worker spawns. Lines: {:?}",
        handle.lines
    );

    // Wait 2 ticks to see if a third spawns (it shouldn't).
    std::thread::sleep(Duration::from_secs(1));
    handle.drain_available();

    let spawn_count = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent"))
        .count();
    assert_eq!(
        spawn_count, 2,
        "cap=2 should limit to 2 concurrent workers, got {spawn_count}. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

// ── M3: Priority ordering ──────────────────────────────────────────────

#[test]
fn higher_priority_task_spawns_first() {
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

    // Seed low-priority task first, high-priority second.
    seed_task_with_priority(home.path(), "Low-prio task", 10);
    seed_task_with_priority(home.path(), "High-prio task", 90);

    // cap=1 — only one worker. The high-priority task should be picked.
    let mut handle =
        ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file, 1);

    assert!(
        handle.wait_for("spawning agent", 15),
        "worker not spawned. Lines: {:?}",
        handle.lines
    );

    // The spawn log includes the task title — verify it's the high-prio one.
    let spawn_line = handle
        .lines
        .iter()
        .find(|l| l.contains("spawning agent"))
        .unwrap()
        .clone();
    assert!(
        spawn_line.contains("High-prio task"),
        "expected high-priority task to be picked first, got: {spawn_line}"
    );

    handle.stop();
}

// ── M3: Independent lifecycle ──────────────────────────────────────────

#[test]
fn one_worker_done_does_not_affect_other() {
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

    seed_task(home.path(), "Task A");
    seed_task(home.path(), "Task B");

    let mut handle =
        ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file, 2);

    // Wait for both workers to spawn and produce results.
    assert!(
        handle.wait_for_n("spawning agent", 2, 20),
        "expected 2 spawns. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for_n("result", 2, 20),
        "expected 2 results. Lines: {:?}",
        handle.lines
    );

    let agents = handle.extract_all_agent_names("spawning agent ");
    let worker_a = &agents[0];
    let worker_b = &agents[1];

    // Worker A signals done. The daemon publishes its commit and advances only
    // that task into review.
    quorum_done(home.path(), &["--agent", worker_a]);

    // A reviewer replaces worker A in the slot.
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "worker A reviewer not seen. Lines: {:?}",
        handle.lines
    );

    // Worker B should still own its independent task.
    assert_eq!(
        get_task_status(home.path(), 2),
        "working",
        "worker B task should remain working when A enters review"
    );
    assert_ne!(worker_a, worker_b);

    handle.stop();
}

// ── M3: SIGINT tears down all workers ──────────────────────────────────

#[test]
fn sigint_releases_all_workers_to_open() {
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

    seed_task(home.path(), "Task A");
    seed_task(home.path(), "Task B");

    let mut handle =
        ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file, 2);

    // Wait for both workers to spawn.
    assert!(
        handle.wait_for_n("spawning agent", 2, 20),
        "expected 2 spawns. Lines: {:?}",
        handle.lines
    );

    // SIGINT mid-work.
    unsafe {
        libc::kill(handle.child.id() as libc::pid_t, libc::SIGINT);
    }
    handle.child.wait().unwrap();

    // Both tasks must be released back to open.
    assert_eq!(
        get_task_status(home.path(), 1),
        "open",
        "task 1 was not released to open after SIGINT"
    );
    assert_eq!(
        get_task_status(home.path(), 2),
        "open",
        "task 2 was not released to open after SIGINT"
    );
}

// ── M3: Slot freed on done, third task starts ──────────────────────────

#[test]
fn freed_slot_picks_up_next_task() {
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

    // 3 tasks, cap=2 — third should start after one finishes.
    seed_task(home.path(), "Task A");
    seed_task(home.path(), "Task B");
    seed_task(home.path(), "Task C");

    let mut handle =
        ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file, 2);

    // Wait for 2 workers.
    assert!(
        handle.wait_for_n("spawning agent", 2, 20),
        "expected 2 initial spawns. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for_n("result", 2, 20),
        "expected 2 results. Lines: {:?}",
        handle.lines
    );

    let agents = handle.extract_all_agent_names("spawning agent ");
    let worker_a = &agents[0];

    // Worker A signals done; daemon publication advances it to review.
    quorum_done(home.path(), &["--agent", worker_a]);

    // Complete A's reviewer turn so the lifecycle slot is actually free.
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "worker A reviewer not seen. Lines: {:?}",
        handle.lines
    );
    let reviewer = handle
        .extract_all_agent_names("spawning reviewer ")
        .into_iter()
        .next()
        .expect("reviewer name");
    assert!(
        handle.wait_for(&format!("reviewer {reviewer} result"), 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    assert!(
        handle.wait_for("R2: pre-merge reviewer", 15),
        "R2 reviewer not seen. Lines: {:?}",
        handle.lines
    );
    let r2_reviewer = handle
        .extract_all_agent_names("R2: pre-merge reviewer ")
        .into_iter()
        .next()
        .expect("R2 reviewer name");
    assert!(
        handle.wait_for("result", 15),
        "R2 reviewer result not seen. Lines: {:?}",
        handle.lines
    );
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r2_reviewer,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    // Third worker should spawn for Task C.
    assert!(
        handle.wait_for("spawning agent", 15),
        "third worker not spawned after slot freed. Lines: {:?}",
        handle.lines
    );

    // Verify we now have 3 total spawns.
    handle.drain_available();
    let total_spawns = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent"))
        .count();
    assert_eq!(
        total_spawns, 3,
        "expected 3 total spawns (2 initial + 1 after slot freed). Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

// ── M3: Each worker gets its own reviewer ──────────────────────────────

#[test]
fn each_worker_gets_own_reviewer() {
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

    seed_task(home.path(), "Task A");
    seed_task(home.path(), "Task B");

    let mut handle =
        ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file, 2);

    // Wait for both workers.
    assert!(
        handle.wait_for_n("spawning agent", 2, 20),
        "expected 2 worker spawns. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for_n("result", 2, 20),
        "expected 2 worker results. Lines: {:?}",
        handle.lines
    );

    let agents = handle.extract_all_agent_names("spawning agent ");

    // Initial workers commit and signal only; the daemon publishes and creates
    // one PR per task through the GitHub boundary.
    quorum_done(home.path(), &["--agent", &agents[0]]);
    quorum_done(home.path(), &["--agent", &agents[1]]);

    // Wait for two reviewers to spawn.
    assert!(
        handle.wait_for_n("spawning reviewer", 2, 20),
        "expected 2 reviewer spawns. Lines: {:?}",
        handle.lines
    );

    let reviewer_names = handle.extract_all_agent_names("spawning reviewer ");
    assert_eq!(
        reviewer_names.len(),
        2,
        "expected 2 reviewers, got: {reviewer_names:?}"
    );
    assert_ne!(
        reviewer_names[0], reviewer_names[1],
        "reviewers must have different names"
    );

    handle.stop();
}

// ── #201: Sentinel self-termination ───────────────────────────────────

#[test]
fn sentinel_gone_terminates_serve_and_children() {
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

    seed_task(home.path(), "Task A");

    // Create a separate sentinel dir we can drop manually.
    let sentinel = tempfile::tempdir().unwrap();
    let sentinel_path = sentinel.path().to_string_lossy().to_string();

    let fake_agent = cargo_bin("fake-agent");
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

    // Wait for serve to boot.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut booted = false;
    while std::time::Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(line) => {
                if line.contains("serving") {
                    booted = true;
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(_) => break,
        }
    }
    assert!(booted, "serve did not boot");

    // Simulate parent death: remove the sentinel directory.
    drop(sentinel);

    // Serve should exit within a few seconds (tick interval is 500ms).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match child.try_wait().unwrap() {
            Some(_status) => return,
            None => {
                if std::time::Instant::now() > deadline {
                    child.kill().ok();
                    let _ = child.wait();
                    panic!("serve did not exit within 10s after sentinel removal");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}
