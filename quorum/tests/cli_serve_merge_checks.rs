//! Integration tests for merge status-check waiting (#166).
//!
//! Verifies that the daemon waits for required CI checks before merging:
//! - checks pass → merge proceeds
//! - checks fail → Retryable rework with failing check names
//! - checks timeout → rework (recoverable, not terminal cancel)
//! - merge is NOT attempted while checks are pending (negative path)

mod common;

use std::env;
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

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
    gh_state: std::path::PathBuf,
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
  case "$*" in
    *headRefName*)
      if [ -f "$QUORUM_TEST_GH_STATE/fail_target" ]; then
        printf 'target lookup unavailable\n' >&2
        exit 1
      fi
      if [ -f "$QUORUM_TEST_GH_STATE/move_on_target" ]; then
        rm "$QUORUM_TEST_GH_STATE/move_on_target"
        git -C "$QUORUM_TEST_REPO" commit --allow-empty -m reviewer-target-moved >/dev/null
        moved_sha="$(git -C "$QUORUM_TEST_REPO" rev-parse HEAD)"
        git -C "$QUORUM_TEST_REPO" update-ref "refs/heads/$branch" "$moved_sha"
        printf '%s' "$moved_sha" > "$QUORUM_TEST_GH_STATE/moved_sha"
        if [ -f "$QUORUM_TEST_GH_STATE/checks_path" ]; then
          printf 'pending\n' > "$(cat "$QUORUM_TEST_GH_STATE/checks_path")"
        fi
      fi
      ;;
  esac
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
            gh_state,
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

    fn crash(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL);
        }
        let _ = self.child.wait();
    }

    fn force_stop(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGINT);
        }
        std::thread::sleep(Duration::from_millis(100));
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

/// #159: after R1 approves, wait for mandatory R2 and post R2's approval.
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

fn resolve_run_id(home: &std::path::Path, agent: &str, role: &str) -> String {
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let conn = quorum_core::db::open(&db).unwrap();
        if let Some(cap) = quorum_core::capabilities::active_for_agent(&conn, agent).unwrap() {
            if cap.role == role
                && quorum_core::capabilities::resolve_live_run_context(&conn, &cap.run_id, role)
                    .is_ok()
            {
                return cap.run_id;
            }
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for live {role} run capability for {agent}");
        }
        std::thread::sleep(Duration::from_millis(25));
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

fn task_state(home: &std::path::Path) -> (String, i64, i64) {
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let conn = quorum_core::db::open(&db).unwrap();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    let reviewer_runs = conn
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE task_id=1 AND role='reviewer'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    (task.status, task.rework_round, reviewer_runs)
}

fn persisted_pr_target(home: &std::path::Path) -> (i64, String, String, i64, i64) {
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let conn = quorum_core::db::open(&db).unwrap();
    conn.query_row(
        "SELECT pr_number,head_ref,head_sha,is_fork,resolved_at
         FROM pr_targets WHERE task_id=1",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )
    .unwrap()
}

fn reviewer_resource_counts(home: &std::path::Path) -> (i64, i64, i64) {
    let db = home.join("repos").join("test__repo").join("quorum.db");
    let conn = quorum_core::db::open(&db).unwrap();
    conn.query_row(
        "SELECT
           (SELECT count(*) FROM agent_runs WHERE task_id=1 AND role='reviewer'),
           (SELECT count(*) FROM journal WHERE task_id=1 AND role='reviewer'),
           (SELECT count(*) FROM run_capabilities
            WHERE task_id=1 AND role='reviewer' AND revoked_at IS NULL)",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

fn drive_to_rework(home: &std::path::Path, handle: &mut ServeHandle) -> (String, String) {
    assert!(handle.wait_for("spawning agent", 15), "{:?}", handle.lines);
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home, &["--agent", &worker, "--pr", "1"]);
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "R1 did not spawn: {:?}",
        handle.lines
    );
    let reviewer = handle.extract_agent_name("spawning reviewer ").unwrap();
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);
    quorum_done(
        home,
        &[
            "--agent",
            &reviewer,
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--feedback",
            "Fix the blocking behavior",
        ],
    );
    assert!(
        handle.wait_for("rework #1 started", 15),
        "worker did not enter rework: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("result", 15),
        "worker rework result not seen: {:?}",
        handle.lines
    );
    (worker, reviewer)
}

#[test]
fn rereview_pending_does_not_feed_then_ready_resumes_exactly_once() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("rereview_checks");
    std::fs::write(&checks_state, "ready").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "re-review pending gate");

    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());
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
            "30",
        ],
    );
    let (worker, _) = drive_to_rework(home.path(), &mut handle);
    std::fs::write(&checks_state, "pending").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);

    assert!(
        handle.wait_for("ResumeReviewer: CI pending", 15),
        "sticky reviewer did not enter CI wait: {:?}",
        handle.lines
    );
    assert!(
        !handle.wait_for("ResumeReviewer: fed re-review turn", 3),
        "pending CI must not feed the sticky reviewer: {:?}",
        handle.lines
    );
    assert_eq!(task_state(home.path()).0, "in-review");

    std::fs::write(&checks_state, "ready").unwrap();
    assert!(
        handle.wait_for("ResumeReviewer: fed re-review turn", 45),
        "green CI did not resume the sticky reviewer: {:?}",
        handle.lines
    );
    while let Ok(line) = handle.rx.try_recv() {
        handle.lines.push(line);
    }
    assert_eq!(
        handle
            .lines
            .iter()
            .filter(|line| line.contains("ResumeReviewer: fed re-review turn"))
            .count(),
        1,
        "the pending resume intent must be consumed exactly once"
    );
    handle.force_stop();
}

#[test]
fn rereview_failed_ci_reenters_rework_without_feeding_reviewer() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("rereview_failed_checks");
    std::fs::write(&checks_state, "ready").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "re-review failed gate");

    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());
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
            "30",
        ],
    );
    let (worker, _) = drive_to_rework(home.path(), &mut handle);
    std::fs::write(&checks_state, "failed\nrereview-ci").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);

    assert!(
        handle.wait_for("failed (rereview-ci)", 15),
        "red CI did not enter failed-CI rework: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework #2 (pre-review CI failure)", 15),
        "failed-CI lifecycle transition did not complete: {:?}",
        handle.lines
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("ResumeReviewer: fed re-review turn")),
        "red CI must not feed the sticky reviewer: {:?}",
        handle.lines
    );
    assert_eq!(
        task_state(home.path()).0,
        "rework",
        "red re-review CI must return to rework"
    );
    assert_eq!(task_state(home.path()).1, 2);
    handle.force_stop();
}

#[test]
fn head_move_during_ci_wait_discards_old_gate_before_reviewer_spawn() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let moved_marker = home.path().join("head_moved_once");
    let worker_wt = wt_base.path().join("Agent0-t1");

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "head move during CI wait");

    let checks_cmd = format!(
        "if [ ! -e '{marker}' ]; then \
           touch '{marker}'; \
           git -C '{repo}' commit --allow-empty -m moved >/dev/null; \
           sha=$(git -C '{repo}' rev-parse HEAD); \
           git -C '{worker_wt}' reset --hard \"$sha\" >/dev/null; \
           git -C '{worker_wt}' push --force origin HEAD:daemon/agent0-t1 >/dev/null; \
         fi; printf 'ready\\n'",
        marker = moved_marker.to_string_lossy(),
        repo = repo_dir.path().to_string_lossy(),
        worker_wt = worker_wt.to_string_lossy(),
    );
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
            "30",
        ],
    );
    assert!(handle.wait_for("spawning agent", 15), "{:?}", handle.lines);
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);

    assert!(
        handle.wait_for("head moved after CI gate", 15),
        "daemon did not invalidate the old gated SHA: {:?}",
        handle.lines
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("spawning reviewer")),
        "reviewer spawned for the ungated moved head: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("spawning reviewer", 45),
        "new head was not gated and reviewed: {:?}",
        handle.lines
    );
    let reviewer = handle.extract_agent_name("spawning reviewer ").unwrap();
    assert!(
        handle.wait_for("reviewer worktree provisioned", 15),
        "reviewer worktree was not provisioned: {:?}",
        handle.lines
    );
    let reviewer_wt = wt_base.path().join(format!("pr-1-{reviewer}"));
    let reviewed_sha = Command::new("git")
        .args(["-C", &reviewer_wt.to_string_lossy(), "rev-parse", "HEAD"])
        .output()
        .unwrap();
    let current_sha = Command::new("git")
        .args([
            "-C",
            &repo_dir.path().to_string_lossy(),
            "rev-parse",
            "HEAD",
        ])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&reviewed_sha.stdout).trim(),
        String::from_utf8_lossy(&current_sha.stdout).trim(),
        "reviewer worktree must be the newly gated head"
    );
    handle.force_stop();
}

#[test]
fn reviewer_target_move_after_exact_head_check_preserves_durable_target() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("reviewer_target_checks");
    std::fs::write(&checks_state, "pending\n").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "reviewer target TOCTOU");

    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());
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
            "30",
        ],
    );
    assert!(handle.wait_for("spawning agent", 15), "{:?}", handle.lines);
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);
    assert!(
        handle.wait_for("waiting for checks before reviewer provisioning", 15),
        "initial reviewer gate did not start: {:?}",
        handle.lines
    );

    let accepted_branch = std::fs::read_to_string(handle.gh_state.join("1")).unwrap();
    let accepted_sha = Command::new("git")
        .args([
            "-C",
            &repo_dir.path().to_string_lossy(),
            "rev-parse",
            &format!("refs/heads/{accepted_branch}"),
        ])
        .output()
        .unwrap();
    let accepted_sha = String::from_utf8_lossy(&accepted_sha.stdout)
        .trim()
        .to_string();
    let db = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    let mut conn = quorum_core::db::open(&db).unwrap();
    quorum_core::pr_targets::upsert(&mut conn, 1, 1, &accepted_branch, &accepted_sha, false)
        .unwrap();
    drop(conn);
    let accepted = persisted_pr_target(home.path());
    std::fs::write(
        handle.gh_state.join("checks_path"),
        checks_state.to_string_lossy().as_bytes(),
    )
    .unwrap();
    std::fs::write(handle.gh_state.join("move_on_target"), b"1").unwrap();
    std::fs::write(&checks_state, "ready\n").unwrap();

    assert!(
        handle.wait_for("resolved target moved after CI gate", 45),
        "target resolution did not deterministically move after the exact head check: {:?}",
        handle.lines
    );
    let moved_sha = std::fs::read_to_string(handle.gh_state.join("moved_sha")).unwrap();
    assert_ne!(accepted.2, moved_sha);
    assert_eq!(
        persisted_pr_target(home.path()),
        accepted,
        "the rejected moved target must not change any durable tuple byte"
    );
    assert_eq!(
        reviewer_resource_counts(home.path()),
        (0, 0, 0),
        "the rejected moved target must acquire no reviewer resources"
    );

    handle.force_stop();
}

#[test]
fn pre_review_pending_waits_without_reviewer_then_ready_spawns() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("pre_review_checks");
    std::fs::write(&checks_state, "pending").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "pre-review pending gate");

    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());
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
            "30",
        ],
    );
    assert!(handle.wait_for("spawning agent", 15), "{:?}", handle.lines);
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);

    assert!(
        handle.wait_for("PRE-REVIEW CI GATE", 15),
        "gate did not start: {:?}",
        handle.lines
    );
    assert!(
        !handle.wait_for("spawning reviewer", 3),
        "pending checks must not consume a reviewer: {:?}",
        handle.lines
    );
    assert_eq!(
        task_state(home.path()),
        ("in-review".into(), 0, 0),
        "pending checks must be lifecycle-inert"
    );

    std::fs::write(&checks_state, "ready").unwrap();
    assert!(
        handle.wait_for("spawning reviewer", 45),
        "green checks did not release the reviewer gate: {:?}",
        handle.lines
    );
    handle.force_stop();
}

#[test]
fn pre_review_failed_enters_rework_without_reviewer() {
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
    seed_task(home.path(), "pre-review failed gate");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "printf 'failed\\nfmt\\n'",
            "--merge-checks-timeout-secs",
            "1",
            "--merge-checks-poll-secs",
            "30",
        ],
    );
    assert!(handle.wait_for("spawning agent", 15), "{:?}", handle.lines);
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);

    assert!(
        handle.wait_for("entering rework without spawning a reviewer", 15),
        "failed checks did not enter rework: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("pre-review CI failure", 15),
        "worker did not receive CI rework: {:?}",
        handle.lines
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("spawning reviewer")),
        "failed checks must not consume a reviewer: {:?}",
        handle.lines
    );
    assert_eq!(
        task_state(home.path()),
        ("rework".into(), 1, 0),
        "failed checks must use the normal rework budget without a reviewer run"
    );
    assert!(
        handle.lines.iter().any(|line| line.contains("fmt")),
        "failing check names must reach the rework path: {:?}",
        handle.lines
    );
    handle.force_stop();
}

#[test]
fn pre_review_pending_survives_daemon_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("restart_pre_review_checks");
    std::fs::write(&checks_state, "pending").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "pre-review restart gate");

    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());
    let args = [
        "--merge-checks-cmd",
        checks_cmd.as_str(),
        "--merge-checks-timeout-secs",
        "10",
        "--merge-checks-poll-secs",
        "30",
    ];
    let mut first = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &args,
    );
    assert!(first.wait_for("spawning agent", 15), "{:?}", first.lines);
    assert!(first.wait_for("result", 15), "{:?}", first.lines);
    let worker = first.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);
    assert!(
        first.wait_for("waiting for checks before reviewer provisioning", 15),
        "{:?}",
        first.lines
    );
    assert!(
        !first
            .lines
            .iter()
            .any(|line| line.contains("spawning reviewer")),
        "reviewer spawned before crash: {:?}",
        first.lines
    );
    first.crash();
    // SIGKILL + immediate restart is Held until stale or cleared (instance-id
    // authority); tests clear the leftover row instead of waiting stale_secs.
    common::clear_daemon_lock(&home.path().join("repos/test__repo/quorum.db"));

    std::fs::write(&checks_state, "ready").unwrap();
    let mut restarted = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &args,
    );
    assert!(
        restarted.wait_for("spawning reviewer", 30),
        "restart did not re-poll and release the durable in-review task: {:?}",
        restarted.lines
    );
    assert_eq!(task_state(home.path()).0, "in-review");
    restarted.force_stop();
}

#[test]
fn r2_rechecks_ci_before_spawning() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("r2_pre_review_checks");
    std::fs::write(&checks_state, "ready").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "R2 pre-review gate");

    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());
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
            "30",
        ],
    );
    assert!(handle.wait_for("spawning agent", 15), "{:?}", handle.lines);
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "R1 did not spawn: {:?}",
        handle.lines
    );
    let r1 = handle.extract_agent_name("spawning reviewer ").unwrap();
    assert!(handle.wait_for("result", 15), "{:?}", handle.lines);

    std::fs::write(&checks_state, "pending").unwrap();
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
        handle.wait_for("CI pending for current head", 15),
        "R2 CI gate did not hold: {:?}",
        handle.lines
    );
    assert!(
        !handle.wait_for("R2: pre-merge reviewer", 3),
        "R2 must not spawn while current-head CI is pending: {:?}",
        handle.lines
    );

    std::fs::write(&checks_state, "ready").unwrap();
    assert!(
        handle.wait_for("R2: pre-merge reviewer", 45),
        "R2 did not spawn after current-head CI became green: {:?}",
        handle.lines
    );
    handle.force_stop();
}

/// Checks pass immediately → merge proceeds.
#[test]
fn checks_pass_then_merge_succeeds() {
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

    seed_task(home.path(), "Task for checks-pass test");

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
            "30",
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
        handle.wait_for("checks passed", 15),
        "checks-passed log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 15),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// Checks fail → Retryable rework with failing check names.
#[test]
fn checks_fail_sends_rework() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("checks_fail_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for checks-fail test");

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
            "10",
            "--merge-checks-poll-secs",
            "30",
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
    complete_r2_review_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "failed\nclipper\ntest").unwrap();
    });

    assert!(
        handle.wait_for("checks failed", 15),
        "checks-failed log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework #1 (checks failure)", 15),
        "rework turn not sent. Lines: {:?}",
        handle.lines
    );

    let saw_merged = handle.lines.iter().any(|l| l.contains("merged"));
    assert!(
        !saw_merged,
        "merge should NOT happen when checks fail. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #174: Checks timeout → durable merge-wait (no rework, no merge, no
/// agent spawn, no counter changes). Task stays in merging.
#[test]
fn checks_timeout_enters_merge_wait() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("checks_timeout_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for checks-timeout merge-wait test");

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
            "30",
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
    complete_r2_review_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    // Should see merge-wait log, NOT rework/MERGE BLOCKED.
    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.lines.iter().any(|l| l.contains("timed out")),
        "timeout reason not in merge-wait log. Lines: {:?}",
        handle.lines
    );

    // Negative: no rework, no merge, no cancel, no new agent spawn.
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT fire during merge-wait. Lines: {:?}",
        handle.lines
    );
    let saw_merged = handle
        .lines
        .iter()
        .any(|l| l.contains("merged") && !l.contains("BLOCKED"));
    assert!(
        !saw_merged,
        "merge should NOT happen during merge-wait. Lines: {:?}",
        handle.lines
    );
    let saw_cancelled = handle.lines.iter().any(|l| l.contains("cancelling"));
    assert!(
        !saw_cancelled,
        "task should NOT be cancelled during merge-wait. Lines: {:?}",
        handle.lines
    );

    // Task should still be in merging status.
    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"merging\"") || stdout.contains("\"status\": \"merging\""),
        "task should stay in merging during merge-wait, got: {stdout}"
    );

    // Counters should not have changed.
    assert!(
        stdout.contains("\"rework_round\":0") || stdout.contains("\"rework_round\": 0"),
        "rework_round should not increment during merge-wait, got: {stdout}"
    );

    handle.stop();
}

/// #174: Pending → Ready → merge proceeds (merge-wait retries and succeeds).
#[test]
fn checks_pending_then_ready_merges_via_merge_wait() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let state_file = home.path().join("checks_state");
    std::fs::write(&state_file, "ready").unwrap();
    let checks_cmd = format!("cat {}", state_file.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for pending-then-ready merge-wait test");

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
            "30",
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
    complete_r2_review_after(home.path(), &mut handle, "1", || {
        std::fs::write(&state_file, "pending").unwrap();
    });

    // First timeout enters merge-wait.
    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait log not seen. Lines: {:?}",
        handle.lines
    );

    // Flip checks to ready — next tick should merge.
    std::fs::write(&state_file, "ready").unwrap();

    assert!(
        handle.wait_for("checks passed", 20),
        "checks-passed log not seen after state change. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 20),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    // No rework was triggered.
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT fire for pending→ready path. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #174: Pending → Failed → enters rework (merge-wait retries, checks fail).
#[test]
fn checks_pending_then_failed_enters_rework() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let state_file = home.path().join("checks_state");
    std::fs::write(&state_file, "ready").unwrap();
    let checks_cmd = format!("cat {}", state_file.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for pending-then-failed test");

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
            "30",
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
    complete_r2_review_after(home.path(), &mut handle, "1", || {
        std::fs::write(&state_file, "pending").unwrap();
    });

    // First timeout enters merge-wait.
    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait log not seen. Lines: {:?}",
        handle.lines
    );

    // Flip checks to failed — next tick should enter rework.
    std::fs::write(&state_file, "failed\nclipper\ntest").unwrap();

    assert!(
        handle.wait_for("checks failed", 20),
        "checks-failed log not seen after state change. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework #1 (checks failure)", 15),
        "rework not triggered after checks failure. Lines: {:?}",
        handle.lines
    );

    let saw_merged = handle.lines.iter().any(|l| l.contains("merged"));
    assert!(
        !saw_merged,
        "merge should NOT happen when checks fail. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #174: Restart test — pending before restart remains pending after restart,
/// and later Ready merges exactly once.
#[test]
fn checks_pending_survives_restart() {
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

    seed_task(home.path(), "Task for restart merge-wait test");

    // Phase 1: start daemon, enter merge-wait.
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
            "30",
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
    complete_r2_review_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    assert!(
        handle.wait_for("merge wait", 15),
        "merge-wait log not seen. Lines: {:?}",
        handle.lines
    );

    // Phase 2: stop daemon. Shutdown teardown resets merging → in-review
    // via AgentFailed, but durable approvals survive.
    handle.stop();

    // Phase 3: flip checks to ready before restart so approval-recovery
    // can verify CI and merge.
    std::fs::write(&checks_state, "ready").unwrap();

    // Phase 4: restart daemon. Approval recovery detects dual approval,
    // verifies CI (now ready), and merges from durable state.
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
            "30",
        ],
    );

    assert!(
        handle2.wait_for("merged from durable approval", 30),
        "approval-recovery merge not seen after restart. Lines: {:?}",
        handle2.lines
    );

    // Verify task is done.
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
        "task should be done after restart merge, got: {stdout}"
    );

    // No rework should have been triggered.
    let saw_rework = handle2.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT fire after restart merge. Lines: {:?}",
        handle2.lines
    );

    handle2.stop();
}

/// Negative path: checks pending → transition to ready → merge proceeds.
/// Verifies merge is NOT attempted while checks are pending.
#[test]
fn checks_pending_then_ready_merges_after_wait() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let state_file = home.path().join("checks_state");
    std::fs::write(&state_file, "ready").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for pending-then-ready test");

    let checks_cmd = format!("cat {}", state_file.to_string_lossy());

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
            "15",
            "--merge-checks-poll-secs",
            "30",
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
    complete_r2_review_after(home.path(), &mut handle, "1", || {
        std::fs::write(&state_file, "pending").unwrap();
    });

    assert!(
        handle.wait_for("waiting for checks", 10),
        "waiting-for-checks log not seen. Lines: {:?}",
        handle.lines
    );

    let merged_before = handle
        .lines
        .iter()
        .any(|l| l.contains("merged") || l.contains("proceeding to merge"));
    assert!(
        !merged_before,
        "merge should NOT happen while checks pending. Lines: {:?}",
        handle.lines
    );

    std::fs::write(&state_file, "ready").unwrap();

    assert!(
        handle.wait_for("checks passed", 15),
        "checks-passed log not seen after state change. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 15),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// Empty check rollup (no check runs yet for new head SHA) is treated as
/// pending, not passing. Verifies the daemon waits instead of merging
/// prematurely on a rework push with stale/absent checks.
#[test]
fn empty_checks_treated_as_pending() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    // Checks command returns "pending" (simulating empty check rollup that
    // transitions to ready after 2 polls).
    let state_file = home.path().join("checks_state");
    std::fs::write(&state_file, "ready").unwrap();
    let checks_cmd = format!("cat {}", state_file.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for empty-checks test");

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
            "15",
            "--merge-checks-poll-secs",
            "30",
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
    complete_r2_review_after(home.path(), &mut handle, "1", || {
        std::fs::write(&state_file, "pending").unwrap();
    });

    // Wait long enough that the old (buggy) code would have merged immediately.
    assert!(
        handle.wait_for("waiting for checks", 10),
        "waiting-for-checks log not seen. Lines: {:?}",
        handle.lines
    );

    // Verify no merge happened while pending.
    let merged_before = handle
        .lines
        .iter()
        .any(|l| l.contains("proceeding to merge") || l.contains("merged"));
    assert!(
        !merged_before,
        "merge should NOT happen while checks are pending/empty. Lines: {:?}",
        handle.lines
    );

    // Now transition checks to ready.
    std::fs::write(&state_file, "ready").unwrap();

    assert!(
        handle.wait_for("checks passed", 15),
        "checks-passed log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("merged", 15),
        "merge-success log not seen. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// Policy-pending merge retries until checks pass then merges successfully.
/// Simulates rework-push race: first merge attempt hits "policy prohibits"
/// (checks not propagated), retry wait sees checks become ready, second
/// merge attempt succeeds.
#[test]
fn policy_pending_retries_then_merges() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let merge_state_file = home.path().join("merge_state");
    std::fs::write(&merge_state_file, "fail").unwrap();

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for policy-pending retry test");

    // Merge command: fails with "policy prohibits" when state is "fail",
    // succeeds when state is "pass".
    let merge_cmd = format!(
        "state=$(cat {}); if [ \"$state\" = \"pass\" ]; then echo merged; else \
         echo 'not mergeable: the base branch policy prohibits the merge' >&2; exit 1; fi",
        merge_state_file.to_string_lossy()
    );

    // Checks cmd starts pending, then transitions to ready (which also
    // flips the merge state so the retry merge succeeds).
    let checks_state_file = home.path().join("checks_state");
    std::fs::write(&checks_state_file, "ready").unwrap();

    let checks_cmd = format!("cat {}", checks_state_file.to_string_lossy());

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &merge_cmd,
        &[
            "--merge-checks-cmd",
            &checks_cmd,
            "--merge-checks-timeout-secs",
            "15",
            "--merge-checks-poll-secs",
            "30",
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

    // Initial checks pass (simulating stale check state from old HEAD).
    std::fs::write(&checks_state_file, "ready").unwrap();

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

    // First merge attempt hits policy-pending.
    assert!(
        handle.wait_for("policy-pending", 15),
        "policy-pending log not seen. Lines: {:?}",
        handle.lines
    );

    // Flip merge state so retry succeeds.
    std::fs::write(&merge_state_file, "pass").unwrap();

    // Should retry and succeed.
    assert!(
        handle.wait_for("merged", 20),
        "merge-success log not seen after policy-pending retry. Lines: {:?}",
        handle.lines
    );

    // No cancel, no rework.
    let saw_cancelled = handle.lines.iter().any(|l| l.contains("cancelling task"));
    assert!(
        !saw_cancelled,
        "task should NOT be cancelled on policy-pending retry success. Lines: {:?}",
        handle.lines
    );
    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT be sent for policy-pending merge. Lines: {:?}",
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
        stdout.contains("\"status\":\"done\"") || stdout.contains("\"status\": \"done\""),
        "task should be done after policy-pending retry merge, got: {stdout}"
    );
}

/// #223: approved verdict with no PR number must warn and skip merge,
/// not call merge with PR #0.
#[test]
fn approved_without_pr_skips_merge() {
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

    seed_task(home.path(), "Task for missing-PR test");

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
            "30",
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

    // The endpoint derives the daemon-owned PR when the reviewer omits --pr.
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );

    assert!(
        handle.wait_for("R2 GATE", 15),
        "endpoint-derived PR did not reach the post-review gate. Lines: {:?}",
        handle.lines
    );

    let saw_merge_attempt = handle.lines.iter().any(|l| {
        l.contains("proceeding to merge") || l.contains("verdict: approved — waiting for checks")
    });
    assert!(
        !saw_merge_attempt,
        "the pending R2 gate must still prevent a merge. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

/// #153: PR is mergeable at initial check, becomes conflicting during the
/// checks wait, and checks time out. Task must transition to rework (not
/// cancelled), and the worker must receive a rework turn with conflict
/// resolution instructions.
#[test]
fn conflict_during_checks_wait_triggers_rework_not_cancel() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("conflict_checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Task for conflict-during-checks test");

    // mergeability-cmd: stays mergeable while checks are ready. Once the test
    // flips checks to pending, the first mergeability check (before waiting) is
    // mergeable and the second (after timeout) is conflicting.
    let counter_file = home.path().join("mergeability_counter");
    std::fs::write(&counter_file, "0").unwrap();
    let mergeability_script = format!(
        "if [ \"$(cat {s})\" != pending ]; then echo mergeable; \
         else n=$(cat {f}); n=$((n + 1)); echo $n > {f}; \
         if [ $n -le 1 ]; then echo mergeable; else echo conflicting; fi; fi",
        f = counter_file.display(),
        s = checks_state.display()
    );

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
            "30",
            "--merge-mergeability-cmd",
            &mergeability_script,
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
    complete_r2_review_after(home.path(), &mut handle, "1", || {
        std::fs::write(&checks_state, "pending").unwrap();
    });

    // Should see the conflict detected after timeout, NOT a cancel.
    assert!(
        handle.wait_for("CONFLICTING during checks", 20),
        "conflict-during-checks log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework", 15),
        "rework not triggered for conflict during checks. Lines: {:?}",
        handle.lines
    );

    let saw_cancelled = handle.lines.iter().any(|l| l.contains("cancelling"));
    assert!(
        !saw_cancelled,
        "task should NOT be cancelled for conflict during checks. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

// NOTE: no-CI path coverage for parse_checks_json / checks_query_from_parsed
// is in unit tests (parse_checks_empty_rollup_is_no_checks_configured,
// parse_checks_no_rollup_field_is_pending, etc.). E2E tests use
// CommandMergeExecutor which bypasses GhMergeExecutor::query_checks.
// See #181 review discussion.
