//! M2/M3 tests: reviewer spawn + verdict loop + daemon-owned merge.
//!
//! Scenarios using fake-agent:
//! 1. approve flow: worker done → reviewer spawns → approved → daemon merges → both torn down
//! 2. changes flow: worker done → reviewer spawns → changes → reviewer killed,
//!    worker re-fed same PID (warm rework)
//! 3. merge failure: approved verdict but merge fails → treated as changes,
//!    worker gets rework turn with merge error

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

/// Poll every worker/reviewer session log under `{home}/logs/*/stream.jsonl`
/// for `needle` until it matches or the timeout elapses. Agent *text* is
/// written to the per-session log, NOT echoed to daemon stderr, so agent
/// output (like the fake-agent's "Fixing…" rework response) is observed here.
fn wait_session_log(home: &std::path::Path, needle: &str, timeout_secs: u64) -> bool {
    let logs = home.join("logs");
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        if let Ok(entries) = std::fs::read_dir(&logs) {
            for entry in entries.flatten() {
                let stream = entry.path().join("stream.jsonl");
                if let Ok(content) = std::fs::read_to_string(&stream) {
                    if content.contains(needle) {
                        return true;
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

fn wait_for_task_status(
    home: &std::path::Path,
    task_id: i64,
    expected: &str,
    timeout_secs: u64,
) -> bool {
    let db = home.join("repos/test__repo/quorum.db");
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        if let Ok(conn) = rusqlite::Connection::open(&db) {
            let status: Option<String> = conn
                .query_row("SELECT status FROM tasks WHERE id=?1", [task_id], |r| {
                    r.get(0)
                })
                .ok();
            if status.as_deref() == Some(expected) {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
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
        Self::start_with_merge(home, repo, wt_base, names, "true")
    }

    fn start_with_merge(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        merge_cmd: &str,
    ) -> Self {
        Self::start_with_options(home, repo, wt_base, names, merge_cmd, None)
    }

    fn start_with_options(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        merge_cmd: &str,
        mergeability_cmd: Option<&str>,
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
        let mut args = vec![
            "serve".to_string(),
            "--repo".to_string(),
            "test/repo".to_string(),
            "--cap".to_string(),
            "1".to_string(),
            "--repo-dir".to_string(),
            repo.to_string_lossy().to_string(),
            "--worktree-base".to_string(),
            wt_base.to_string_lossy().to_string(),
            "--names-file".to_string(),
            names.to_string_lossy().to_string(),
            "--agent-bin".to_string(),
            fake_agent.to_string_lossy().to_string(),
            "--merge-cmd".to_string(),
            merge_cmd.to_string(),
            "--exit-when-gone".to_string(),
            sentinel_path,
        ];
        if let Some(m_cmd) = mergeability_cmd {
            args.push("--merge-mergeability-cmd".to_string());
            args.push(m_cmd.to_string());
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

fn configure_r2_sampling(home: &std::path::Path, target: i64, probability: f64) {
    let path = home.join("serve").join("test__repo.toml");
    let routing = std::fs::read_to_string(&path).unwrap();
    let config = format!(
        "r2_enabled = true\nr2_target_per_stratum = {target}\nr2_steady_state_p = {probability}\n{routing}"
    );
    std::fs::write(path, config).unwrap();
}

fn r2_run_count(home: &std::path::Path, task_id: i64) -> i64 {
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let conn = quorum_core::db::open(&db).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM agent_runs WHERE task_id=?1 AND sub_role='r2'",
        [task_id],
        |row| row.get(0),
    )
    .unwrap()
}

fn complete_r2_review(home: &std::path::Path, handle: &mut ServeHandle, pr: &str) {
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
fn approve_flow_tears_down_both_agents() {
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

    seed_task(home.path(), "Task for approve flow");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

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

    // Wait for reviewer to spawn
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned. Lines: {:?}",
        handle.lines
    );

    let reviewer_name = handle.extract_agent_name("spawning reviewer ").unwrap();

    // Wait for reviewer to finish its turn
    assert!(
        handle.wait_for("result", 15),
        "reviewer result not seen. Lines: {:?}",
        handle.lines
    );

    // Reviewer signals approved verdict
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

    // No R2 config is present: the default must preserve the mandatory gate.
    assert!(
        handle.wait_for("R2: pre-merge reviewer", 15),
        "default R1 approval must provision R2: {:?}",
        handle.lines
    );
    assert_eq!(r2_run_count(home.path(), 1), 1, "R2 run must be recorded");
    assert!(
        !handle.wait_for("merged", 1),
        "R1 approval alone must not merge while mandatory R2 is pending"
    );
    let r2_name = handle
        .extract_agent_name("R2: pre-merge reviewer ")
        .expect("could not extract R2 reviewer name");
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r2_name,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    // Daemon should merge, then tear down both
    assert!(
        handle.wait_for("merged", 15),
        "merge log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("tearing down worker", 15),
        "worker teardown not seen. Lines: {:?}",
        handle.lines
    );

    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"done\"") || stdout.contains("\"status\": \"done\""),
        "task not done after in-cycle merge + auto-resolve: {stdout}"
    );

    handle.stop();
}

#[test]
fn sampled_skip_merges_once_without_an_r2_slot() {
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
    configure_r2_sampling(home.path(), 0, 0.0);
    seed_task(home.path(), "sampled skip");
    let merge_marker = home.path().join("merged-count");
    let merge_cmd = format!("printf merged >> {}", merge_marker.display());
    let mut handle = ServeHandle::start_with_merge(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &merge_cmd,
    );

    assert!(handle.wait_for("spawning agent", 15));
    assert!(handle.wait_for("result", 15));
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);
    assert!(handle.wait_for("spawning reviewer", 15));
    assert!(handle.wait_for("result", 15));
    let r1 = handle.extract_agent_name("spawning reviewer ").unwrap();
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r1,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    assert!(handle.wait_for("merged", 15), "{:?}", handle.lines);
    assert_eq!(r2_run_count(home.path(), 1), 0, "skip must not create R2");
    assert_eq!(
        std::fs::read_to_string(&merge_marker).unwrap(),
        "merged",
        "R1-only sampled skip must issue exactly one merge"
    );
    handle.stop();
}

#[test]
fn coverage_floor_forces_r2_even_when_steady_state_sampling_is_zero() {
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
    configure_r2_sampling(home.path(), 1, 0.0);
    seed_task(home.path(), "coverage floor");
    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    assert!(handle.wait_for("spawning agent", 15));
    assert!(handle.wait_for("result", 15));
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);
    assert!(handle.wait_for("spawning reviewer", 15));
    assert!(handle.wait_for("result", 15));
    let r1 = handle.extract_agent_name("spawning reviewer ").unwrap();
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r1,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    assert!(handle.wait_for("R2: pre-merge reviewer", 15));
    assert_eq!(r2_run_count(home.path(), 1), 1);
    assert!(
        !handle.wait_for("merged", 1),
        "coverage-floor R2 must block merge until it approves"
    );
    handle.stop();
}

#[test]
fn configured_always_sample_still_blocks_merge_until_r2_approves() {
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
    configure_r2_sampling(home.path(), 0, 1.0);
    seed_task(home.path(), "always sample");
    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    assert!(handle.wait_for("spawning agent", 15));
    assert!(handle.wait_for("result", 15));
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);
    assert!(handle.wait_for("spawning reviewer", 15));
    assert!(handle.wait_for("result", 15));
    let r1 = handle.extract_agent_name("spawning reviewer ").unwrap();
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r1,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    assert!(handle.wait_for("R2: pre-merge reviewer", 15));
    assert_eq!(r2_run_count(home.path(), 1), 1);
    assert!(
        !handle.wait_for("merged", 1),
        "configured always-sample R2 must block merge until its verdict"
    );
    handle.stop();
}

#[test]
fn exhausted_rework_budget_skips_r2_even_when_sampling_requires_it() {
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
    configure_r2_sampling(home.path(), 0, 1.0);
    seed_task(home.path(), "exhausted rework budget");
    let db = home.path().join("repos/test__repo/quorum.db");
    let conn = quorum_core::db::open(&db).unwrap();
    conn.execute(
        "UPDATE tasks SET rework_round=?1 WHERE id=1",
        [quorum_core::lifecycle::REWORK_CAP],
    )
    .unwrap();
    drop(conn);

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);
    assert!(handle.wait_for("spawning agent", 15));
    assert!(handle.wait_for("result", 15));
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);
    assert!(handle.wait_for("spawning reviewer", 15));
    assert!(handle.wait_for("result", 15));
    let r1 = handle.extract_agent_name("spawning reviewer ").unwrap();
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r1,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    assert!(
        handle.wait_for("exhausted rework budget skipped R2", 15),
        "{:?}",
        handle.lines
    );
    assert!(handle.wait_for("merged", 15), "{:?}", handle.lines);
    assert_eq!(r2_run_count(home.path(), 1), 0);
    handle.stop();
}

#[test]
fn changes_verdict_feeds_rework_to_same_warm_worker() {
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

    seed_task(home.path(), "Task for rework flow");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

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

    // Worker signals "done with PR"
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Wait for reviewer to spawn
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

    // Reviewer signals changes verdict with feedback
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
            "Fix the error handling in main.rs",
        ],
    );

    // Worker should get rework — wait for the fake-agent to respond
    assert!(
        handle.wait_for("rework", 15),
        "rework not seen. Lines: {:?}",
        handle.lines
    );

    assert!(
        wait_session_log(home.path(), "Fixing", 15),
        "worker rework response not seen in session log"
    );

    // ── State assertions (F12) ──
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    // Invariant: same warm worker — exactly 1 worker spawn, 0 worker teardowns
    let worker_spawns = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent"))
        .count();
    assert_eq!(
        worker_spawns, 1,
        "expected exactly 1 worker spawn (warm rework, no re-spawn), got {worker_spawns}. Lines: {:?}",
        handle.lines
    );

    let worker_teardowns = handle
        .lines
        .iter()
        .filter(|l| l.contains("tearing down worker"))
        .count();
    assert_eq!(
        worker_teardowns, 0,
        "worker should not be torn down during rework. Lines: {:?}",
        handle.lines
    );

    // Sticky-agent policy: reviewer stays alive during rework.
    let reviewer_teardowns = handle
        .lines
        .iter()
        .filter(|l| l.contains("tearing down reviewer"))
        .count();
    assert_eq!(
        reviewer_teardowns, 0,
        "reviewer should stay alive (sticky-agent policy). Lines: {:?}",
        handle.lines
    );

    // Task #124: the daemon must NOT mirror the reviewer's `changes` verdict
    // into a duplicate generic GitHub REQUEST_CHANGES review. Reviewer agents
    // own their GitHub review interactions on the PR; the daemon retains only
    // final formal APPROVE + merge. Prior to #124 the daemon logged
    // `posted REQUEST_CHANGES on PR #N` (or a failure/join-error variant) on
    // this path; the absence of any REQUEST_CHANGES log is the runtime
    // guardrail that the mirror is dead, while the rework transition + fed
    // feedback above prove the lifecycle + worker-context path is preserved.
    let request_changes_mentions = handle
        .lines
        .iter()
        .filter(|l| l.contains("REQUEST_CHANGES"))
        .count();
    assert_eq!(
        request_changes_mentions, 0,
        "daemon must not log any REQUEST_CHANGES mirror activity on a changes verdict. \
         Lines: {:?}",
        handle.lines
    );

    // Task status: lifecycle transitions to "rework" during rework.
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
        Some("rework"),
        "task must be in rework during rework cycle, got: {stdout}"
    );
    assert_eq!(
        task["assignee"].as_str(),
        task["author"].as_str(),
        "during rework, assignee must be the worker (restored by ResumeWorker), got: {stdout}"
    );

    handle.stop();
}

#[test]
fn merge_failure_feeds_rework_to_worker() {
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

    seed_task(home.path(), "Task for merge failure flow");

    // Use a merge command that always fails
    let mut handle = ServeHandle::start_with_merge(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "echo 'merge conflict: base branch was updated' >&2 && exit 1",
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

    // Worker signals "done with PR"
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Wait for reviewer to spawn
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

    // Reviewer signals approved verdict — but merge will fail
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

    // Merge should fail and daemon should log the failure
    assert!(
        handle.wait_for("merge failed", 15),
        "merge failure not logged. Lines: {:?}",
        handle.lines
    );

    // Worker should get rework turn (merge failure is treated as changes)
    assert!(
        handle.wait_for("rework", 15),
        "rework not seen after merge failure. Lines: {:?}",
        handle.lines
    );

    // Sticky-agent policy: reviewer stays alive during merge-failure rework.
    // Task should be in rework (lifecycle: merging → in-review → rework).
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
        "task should not be done after merge failure: {stdout}"
    );

    handle.stop();
}

#[test]
fn no_verdict_done_clears_pr_no_respawn_loop() {
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

    seed_task(home.path(), "Task for no-verdict flow");

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

    // Reviewer signals done WITHOUT a verdict — the `_ =>` branch.
    // The CLI now rejects this (role mismatch: reviewer cap, worker-shaped
    // submit), so write the malformed Done row directly to test daemon handling.
    {
        let db = home
            .path()
            .join("repos")
            .join("test__repo")
            .join("quorum.db");
        let mut conn = quorum_core::db::open(&db).unwrap();
        let row = quorum_core::mailbox::MailboxRow {
            agent: reviewer_name.clone(),
            kind: quorum_core::mailbox::MailboxKind::Done,
            task_id: None,
            pr: Some(1),
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        quorum_core::mailbox::append(&mut conn, &row).unwrap();
    }

    // Daemon should fire AgentFailed and tear down the reviewer
    assert!(
        handle.wait_for("without verdict", 15),
        "no-verdict handling not seen. Lines: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("tearing down reviewer", 15),
        "reviewer teardown not seen. Lines: {:?}",
        handle.lines
    );

    // Wait 2 ticks — if w.pr was NOT cleared, a second reviewer would spawn.
    std::thread::sleep(Duration::from_secs(1));

    // Drain any remaining log lines
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    // Count reviewer spawn messages — should be exactly 1
    let spawn_count = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning reviewer"))
        .count();
    assert_eq!(
        spawn_count, 1,
        "expected exactly 1 reviewer spawn (no respawn loop), got {spawn_count}. Lines: {:?}",
        handle.lines
    );

    // Worker should still be alive (not torn down)
    let worker_teardowns = handle
        .lines
        .iter()
        .filter(|l| l.contains("tearing down worker"))
        .count();
    assert_eq!(
        worker_teardowns, 0,
        "worker should not be torn down on no-verdict. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #206: an `approved` verdict that did NOT come through the validated CLI —
/// no zero-blocking attestation payload on the mailbox row — must be demoted
/// to `changes` at the daemon boundary and fed back as rework, never merged.
/// This replays the #198 shape (a merge-caliber verdict the review's own
/// findings contradicted) at the mailbox level.
#[test]
fn unattested_approved_verdict_is_demoted_to_changes() {
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

    seed_task(home.path(), "Task for verdict-gate flow");

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

    // Bypass the CLI's #206 validation: write the approved-verdict mailbox row
    // directly, with NO attestation payload.
    let conn = rusqlite::Connection::open(home.path().join("repos/test__repo/quorum.db")).unwrap();
    conn.execute(
        "INSERT INTO mailbox (agent, kind, pr, verdict, created_at) \
         VALUES (?1, 'done', 1, 'approved', 1700000000)",
        rusqlite::params![reviewer_name],
    )
    .unwrap();

    // The gate must demote and route to rework instead of merging.
    assert!(
        handle.wait_for("VERDICT GATE", 15),
        "verdict-gate demotion log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework", 15),
        "demoted verdict did not produce a rework turn. Lines: {:?}",
        handle.lines
    );
    assert!(
        wait_for_task_status(home.path(), 1, "rework", 15),
        "demoted verdict rework turn arrived before the task state transition"
    );

    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }
    assert!(
        !handle.lines.iter().any(|l| l.contains("merged")),
        "unattested approved verdict must never reach merge. Lines: {:?}",
        handle.lines
    );

    // State assertion (review #226 finding 2): merge-absence must hold in the
    // DB, not just the log stream — a merged task would be closed; a demoted
    // one stays claimed by the same worker for the rework round.
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
        Some("rework"),
        "demoted verdict must leave the task in rework, not done: {stdout}"
    );
    assert_eq!(
        task["assignee"].as_str(),
        task["author"].as_str(),
        "during rework, assignee must be the worker (restored by ResumeWorker): {stdout}"
    );

    handle.stop();
}

/// C6 regression: after a rework cycle, the worker re-signals done (ReworkPushed).
/// The lifecycle emits ResumeReviewer. The daemon must tear down the existing
/// reviewer (which stayed alive per sticky-agent policy) so Phase 5 respawns a
/// fresh one — otherwise the idle reviewer blocks respawn and the task deadlocks.
#[test]
fn rework_resignal_feeds_rereview_turn() {
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

    seed_task(home.path(), "Task for rework re-signal flow");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Worker spawns and produces a result
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

    // Worker signals done with PR
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Reviewer spawns
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

    // Reviewer signals changes verdict → triggers rework
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

    // Worker gets rework turn and responds
    assert!(
        handle.wait_for("rework", 15),
        "rework not initiated. Lines: {:?}",
        handle.lines
    );
    assert!(
        wait_session_log(home.path(), "Fixing", 15),
        "worker rework response not seen in session log"
    );

    assert!(
        handle.wait_for("result", 15),
        "worker rework result not seen. Lines: {:?}",
        handle.lines
    );

    // Worker re-signals done with PR (rework pushed)
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // ResumeReviewer feeds a re-review turn to the existing reviewer
    // (no teardown + respawn — the reviewer keeps its session context).
    assert!(
        handle.wait_for("fed re-review turn", 15),
        "reviewer not fed re-review turn after rework re-signal. Lines: {:?}",
        handle.lines
    );

    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    // Exactly 1 reviewer spawn: the original. Re-review is a feed_turn,
    // not a fresh spawn.
    let reviewer_spawns = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning reviewer"))
        .count();
    assert_eq!(
        reviewer_spawns, 1,
        "expected 1 reviewer spawn (original only, re-review is feed_turn), got {reviewer_spawns}. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// C3 regression: if a task is externally cancelled while the worker is still
/// active, the worker's done signal must NOT set worker.pr or spawn a reviewer.
/// The daemon must detect the rejected lifecycle transition and clean up the slot.
#[test]
fn cancelled_task_done_signal_no_reviewer_spawn() {
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

    seed_task(home.path(), "Task for cancelled-done flow");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Worker spawns and produces a result
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

    // Cancel the task externally (creator can cancel)
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-update",
            "--agent",
            "TestCreator",
            "--task-id",
            "1",
            "--status",
            "cancelled",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "task cancel failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Worker signals done with PR (doesn't know task was cancelled)
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Daemon should detect the cancelled task and clean up.  Two valid
    // orderings: (a) done signal arrives first → "lifecycle rejected", or
    // (b) tick detects cancellation first → "externally moved to cancelled"
    // and the done row lands as "unmatched Done".  Both tear down the worker.
    assert!(
        handle.wait_for("tearing down worker", 15),
        "worker teardown not seen after cancelled task. Lines: {:?}",
        handle.lines
    );
    let saw_rejection = handle
        .lines
        .iter()
        .any(|l| l.contains("lifecycle rejected"));
    let saw_external = handle
        .lines
        .iter()
        .any(|l| l.contains("externally moved to cancelled"));
    assert!(
        saw_rejection || saw_external,
        "expected either 'lifecycle rejected' or 'externally moved to cancelled'. Lines: {:?}",
        handle.lines
    );

    // Wait 2 ticks to confirm no reviewer spawns
    std::thread::sleep(Duration::from_secs(1));
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    // No reviewer should have been spawned
    let reviewer_spawns = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning reviewer"))
        .count();
    assert_eq!(
        reviewer_spawns, 0,
        "no reviewer should spawn for a cancelled task, got {reviewer_spawns}. Lines: {:?}",
        handle.lines
    );

    // Task should remain cancelled
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
        Some("cancelled"),
        "task must remain cancelled, got: {stdout}"
    );

    handle.stop();
}

/// T5: Worker signals done with a PR that is already merged externally.
/// The daemon should detect the merged state via check_mergeability, fire
/// PrFoundMerged, transition the task to done, and never spawn a reviewer.
#[test]
fn already_merged_pr_closes_task_without_reviewer() {
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

    seed_task(home.path(), "Task for already-merged PR");

    let mut handle = ServeHandle::start_with_options(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        Some("echo merged"),
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

    assert!(
        handle.wait_for("already merged", 15),
        "PR-already-merged detection not logged. Lines: {:?}",
        handle.lines
    );

    // Wait 2 ticks to confirm no reviewer spawns
    std::thread::sleep(Duration::from_secs(1));
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }

    let reviewer_spawns = handle
        .lines
        .iter()
        .filter(|l| l.contains("spawning reviewer"))
        .count();
    assert_eq!(
        reviewer_spawns, 0,
        "no reviewer should spawn for an already-merged PR, got {reviewer_spawns}. Lines: {:?}",
        handle.lines
    );

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
        Some("done"),
        "task must be done after PrFoundMerged, got: {stdout}"
    );

    handle.stop();
}
