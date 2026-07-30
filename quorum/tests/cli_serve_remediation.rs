//! Integration tests for #175: remediation worker provisioning when the
//! original worker is absent during actionable rework (failed CI, merge
//! conflict, reviewer changes).
//!
//! Scenario: daemon restarts after the worker submitted a PR. On restart
//! the in-memory workers vector is empty but the task is still in-review.
//! When the reviewer approves and merge handling encounters a failure, the
//! daemon must spawn a remediation worker instead of firing AgentFailed.

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
  printf '[]\n'
elif [ "$cmd" = "pr view" ]; then
  pr="$3"
  if [ -f "$QUORUM_TEST_GH_STATE/$pr" ]; then
    branch="$(cat "$QUORUM_TEST_GH_STATE/$pr")"
  else
    branch="daemon/origworker-t$pr"
  fi
  sha="$(git -C "$QUORUM_TEST_REPO" ls-remote origin "refs/heads/$branch" | awk '{print $1}')"
  if [ -z "$sha" ]; then
    sha="$(git -C "$QUORUM_TEST_REPO" rev-parse "refs/heads/$branch")"
  fi
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

/// Seed an in-review task directly in the DB with a known author, so
/// the daemon can derive the PR branch without `gh`.
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
        "Remediation test task",
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

/// Create the git branch that the remediation worker's worktree provision
/// expects (daemon convention: `daemon/{author}-t{task_id}`).
fn create_pr_branch(repo_dir: &std::path::Path, author: &str, task_id: i64) {
    let branch = format!("daemon/{}-t{}", author.to_lowercase(), task_id);
    let d = repo_dir.to_string_lossy();
    Command::new("git")
        .args(["-C", &d, "branch", &branch])
        .status()
        .unwrap();
}

fn stage_failed_ci_remediation(
    home: &std::path::Path,
    repo_dir: &std::path::Path,
    task_id: i64,
    pr: i64,
) -> String {
    let head_sha = String::from_utf8(
        Command::new("git")
            .args(["-C", &repo_dir.to_string_lossy(), "rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    let db_path = home.join("repos").join("test__repo").join("quorum.db");
    let mut conn = quorum_core::db::open(&db_path).unwrap();
    quorum_core::tasks::apply_checks_failed_with_remediation(
        &mut conn,
        task_id,
        pr,
        &head_sha,
        &["ci-test".into()],
        "CI checks failed before review for PR #1: ci-test\n\nFix the failing checks.",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    )
    .unwrap();
    head_sha
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

/// #175: Failed CI checks + absent worker → remediation worker spawned
/// on the same task and PR branch (no duplicate PR/branch).
#[test]
fn failed_checks_absent_worker_spawns_remediation() {
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

    // Seed an in-review task directly (worker is absent from the start).
    let author = "OrigWorker";
    let pr: i64 = 1;
    let task_id = seed_in_review_task(home.path(), author, pr);
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
        let mut conn =
            quorum_core::db::open(&home.path().join("repos/test__repo/quorum.db")).unwrap();
        let branch = format!("daemon/{}-t{}", author.to_lowercase(), task_id);
        quorum_core::pr_targets::upsert(&mut conn, task_id, pr, &branch, head_sha.trim(), false)
            .unwrap();
    }

    // Start daemon with failing checks command.
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "printf 'failed\\nci-lint\\nci-test'",
            "--merge-checks-timeout-secs",
            "10",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // The daemon detects failed CI before reviewer provisioning and enters the
    // normal remediation path directly.
    assert!(
        handle.wait_for("entering rework without spawning a reviewer", 30),
        "pre-review CI failure not detected. Lines: {:?}",
        handle.lines
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("spawning reviewer")),
        "failed CI must not consume a reviewer. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("spawning remediation worker", 15),
        "remediation worker was not spawned. Lines: {:?}",
        handle.lines
    );

    // Verify the old AgentFailed("no worker for rework") path did NOT fire.
    // The new path logs "spawning remediation worker" — that's correct, not an error.
    let old_error = handle
        .lines
        .iter()
        .any(|l| l.contains("AgentFailed") && l.contains("no worker for rework"));
    assert!(
        !old_error,
        "old AgentFailed('no worker for rework') fired — remediation should have handled it. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    // Negative: no duplicate PR created (remediation works on existing PR).
    let new_pr_lines = handle
        .lines
        .iter()
        .filter(|l| l.contains("gh pr create") || l.contains("opening new PR"))
        .count();
    assert_eq!(
        new_pr_lines, 0,
        "remediation should NOT create a new PR. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// Regression (2026-07-29): a PR head branch checked out in an unrelated
/// worktree (a human's `~/dev/*-wt/...`) used to make remediation provisioning
/// fail instantly with "branch collision", burning rework rounds without ever
/// running a remediation agent. The daemon must check out a run-unique local
/// branch and only fetch the PR head.
#[test]
fn remediation_provisions_when_pr_branch_held_by_external_worktree() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    // Outside wt_base so daemon GC never touches it.
    let external_wt = tempfile::tempdir().unwrap();

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
    let pr_branch = format!("daemon/{}-t{}", author.to_lowercase(), task_id);

    // Someone else holds the PR branch checked out locally.
    let held = external_wt.path().join("human-checkout");
    let add = Command::new("git")
        .args([
            "-C",
            &repo_dir.path().to_string_lossy(),
            "worktree",
            "add",
            &held.to_string_lossy(),
            &pr_branch,
        ])
        .output()
        .unwrap();
    assert!(
        add.status.success(),
        "external worktree add failed: {}",
        String::from_utf8_lossy(&add.stderr)
    );

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "printf 'failed\\nci-test'",
            "--merge-checks-timeout-secs",
            "10",
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
        handle.wait_for("spawning remediation worker", 30),
        "remediation worker was not spawned. Lines: {:?}",
        handle.lines
    );
    let agent = handle
        .extract_agent_name("spawning remediation worker ")
        .expect("remediation agent name");
    assert!(
        handle.wait_for("spawned for task", 20),
        "remediation worker never finished provisioning. Lines: {:?}",
        handle.lines
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|l| l.contains("provision failed") || l.contains("branch collision")),
        "provisioning must not collide with the externally held PR branch. Lines: {:?}",
        handle.lines
    );

    // State, not logs: the remediation worktree exists on a namespaced local
    // branch whose upstream is the PR branch on origin.
    let wt_path = wt_base.path().join(format!("{agent}-t{task_id}"));
    assert!(
        wt_path.is_dir(),
        "remediation worktree missing at {}",
        wt_path.display()
    );
    let git = |args: &[&str]| -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(&wt_path)
            .args(args)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    let local_branch = git(&["rev-parse", "--abbrev-ref", "HEAD"]);
    assert_eq!(
        local_branch,
        format!("remediation/{}-t{task_id}", agent.to_lowercase()),
        "remediation worktree must use a daemon-owned local branch name"
    );
    assert_eq!(
        git(&["config", "--get", &format!("branch.{local_branch}.merge")]),
        format!("refs/heads/{pr_branch}"),
        "plain `git push` must target the PR branch"
    );
    assert_eq!(
        git(&["config", "--get", "push.default"]),
        "",
        "remediation workers must not receive agent-side push defaults"
    );
    assert_eq!(
        git(&["config", "--get", "remote.origin.push"]),
        "",
        "remediation workers must not receive agent-side push refspecs"
    );
    // The external checkout is untouched.
    assert_eq!(
        String::from_utf8_lossy(
            &Command::new("git")
                .args([
                    "-C",
                    &held.to_string_lossy(),
                    "rev-parse",
                    "--abbrev-ref",
                    "HEAD"
                ])
                .output()
                .unwrap()
                .stdout
        )
        .trim(),
        pr_branch
    );

    handle.sigkill();
}

#[test]
fn restart_after_checks_failed_recovers_exact_same_pr_remediation() {
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
    let pr = 1;
    let task_id = seed_in_review_task(home.path(), author, pr);
    create_pr_branch(repo_dir.path(), author, task_id);
    let staged_head = stage_failed_ci_remediation(home.path(), repo_dir.path(), task_id, pr);

    // This DB state is the crash window: ChecksFailed and its exact intent
    // committed, but no remediation process or journal row exists.
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("preserving exact CI remediation", 15),
        "restart did not preserve the staged intent: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("remediation worker", 15),
        "restart did not provision same-PR remediation: {:?}",
        handle.lines
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("spawning agent")),
        "recovery must not launch a generic implementation worker: {:?}",
        handle.lines
    );

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
    assert_eq!(task.status, "rework");
    let intent = quorum_core::tasks::ci_remediation_intent(task.refs.as_deref())
        .unwrap()
        .unwrap();
    assert_eq!(intent.pr, pr);
    assert_eq!(intent.head_sha, staged_head);
    assert!(intent.feedback.contains("ci-test"));
    handle.sigkill();
}

/// A fork PR head names a branch in the fork, not in origin, and the daemon
/// holds no fork credentials. Remediation must park instead of provisioning a
/// worktree aimed at a same-named origin branch.
#[test]
fn fork_pr_remediation_parks_without_configuring_origin_upstream() {
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

    let author = "ForkAuthor";
    let pr: i64 = 1;
    let task_id = seed_in_review_task(home.path(), author, pr);
    // The branch exists locally and on origin, so a collision is NOT the
    // reason provisioning must fail — the fork guard is.
    create_pr_branch(repo_dir.path(), author, task_id);
    let head_sha = stage_failed_ci_remediation(home.path(), repo_dir.path(), task_id, pr);
    {
        let db_path = home
            .path()
            .join("repos")
            .join("test__repo")
            .join("quorum.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        // A fork PR whose head ref collides with a base-repo branch name.
        quorum_core::pr_targets::upsert(&mut conn, task_id, pr, "main", &head_sha, true).unwrap();
    }

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("fork PR: no pushable remote", 20),
        "fork remediation was not refused. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("PARKED", 20),
        "refused fork remediation did not park. Lines: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
    assert_eq!(task.status, "failed", "fork remediation must park the task");
    let active_claims: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM claims WHERE target=?1 AND active=1",
            rusqlite::params![format!("task:{task_id}")],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_claims, 0, "parking releases the remediation claim");
    drop(conn);

    // Nothing was configured to push at origin, and no worktree survives.
    let upstream = Command::new("git")
        .args([
            "-C",
            &repo_dir.path().to_string_lossy(),
            "config",
            "--get-regexp",
            "^branch\\.remediation/",
        ])
        .output()
        .unwrap();
    assert!(
        !upstream.status.success(),
        "fork remediation must not configure an origin upstream: {}",
        String::from_utf8_lossy(&upstream.stdout)
    );
    assert_eq!(
        std::fs::read_dir(wt_base.path()).unwrap().count(),
        0,
        "fork remediation must not leave a worktree behind"
    );

    handle.sigkill();
}

#[test]
fn repeated_ci_remediation_provision_failure_parks_without_open_fallback() {
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
    let author = "MissingBranchWorker";
    let pr = 1;
    let task_id = seed_in_review_task(home.path(), author, pr);
    let staged_head = stage_failed_ci_remediation(home.path(), repo_dir.path(), task_id, pr);
    // Deliberately do not create the expected PR branch: provisioning must
    // fail repeatedly, remain same-PR rework, then park loudly.

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[],
    );
    assert!(
        handle.wait_for("CI remediation provision strike 3/3", 20),
        "bounded provision failures not observed: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("PARKED", 10),
        "exhausted CI remediation did not park loudly: {:?}",
        handle.lines
    );
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("spawning agent")),
        "failed remediation must not fall through to a generic worker: {:?}",
        handle.lines
    );

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
    assert_eq!(task.status, "failed", "parked task must not become open");
    let intent = quorum_core::tasks::ci_remediation_intent(task.refs.as_deref())
        .unwrap()
        .unwrap();
    assert_eq!(intent.pr, pr);
    assert_eq!(intent.head_sha, staged_head);
    assert_eq!(intent.attempts, 3);
    handle.sigkill();
}

/// #175: Remediation worker submits → same PR returns to review.
#[test]
fn remediation_worker_resubmits_same_pr() {
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
        let mut conn =
            quorum_core::db::open(&home.path().join("repos/test__repo/quorum.db")).unwrap();
        let branch = format!("daemon/{}-t{}", author.to_lowercase(), task_id);
        quorum_core::pr_targets::upsert(&mut conn, task_id, pr, &branch, head_sha.trim(), false)
            .unwrap();
    }

    // Reviewer gives changes verdict directly (no merge-checks path).
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

    // Reviewer returns changes verdict → rework → no worker → remediation.
    quorum_done(
        home.path(),
        &[
            "--agent",
            &reviewer_name,
            "--pr",
            &pr.to_string(),
            "--verdict",
            "changes",
            "--blocking",
            "2",
            "--feedback",
            "Fix the error handling in main.rs",
        ],
    );

    assert!(
        handle.wait_for("spawning remediation worker", 15),
        "remediation worker was not spawned. Lines: {:?}",
        handle.lines
    );

    // Remediation worker gets spawned, completes, submits same PR.
    assert!(
        handle.wait_for("remediation worker", 15),
        "remediation worker log not seen. Lines: {:?}",
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

    // Regression: a turn-oriented reviewer exits after its accepted changes
    // verdict. Once remediation owns the rework phase, that exit must not fire
    // reviewer AgentFailed and reopen the task.
    let db_path = home.path().join("repos/test__repo/quorum.db");
    let conn = quorum_core::db::open(&db_path).unwrap();
    let reviewer_pid: i32 = conn
        .query_row(
            "SELECT pid FROM journal WHERE agent=?1",
            [&reviewer_name],
            |row| row.get(0),
        )
        .unwrap();
    drop(conn);
    let kill_result = unsafe { libc::kill(reviewer_pid, libc::SIGKILL) };
    assert_eq!(
        kill_result,
        0,
        "failed to SIGKILL reviewer pid {reviewer_pid}: {}",
        std::io::Error::last_os_error()
    );
    assert!(
        handle.wait_for(&format!("reviewer {reviewer_name} process exited"), 15),
        "reviewer process exit not observed. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("exited after recorded verdict", 15),
        "reviewer exit was not treated as teardown-only. Lines: {:?}",
        handle.lines
    );
    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
    assert_eq!(task.status, "rework");
    assert_eq!(task.assignee.as_deref(), Some(remediation_name.as_str()));
    let remediation_runs: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM agent_runs
             WHERE task_id=?1 AND role='worker' AND agent_name=?2",
            rusqlite::params![task_id, remediation_name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remediation_runs, 1, "must not spawn duplicate remediation");
    let reviewer_runs: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM agent_runs
             WHERE task_id=?1 AND role='reviewer'",
            [task_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        reviewer_runs, 1,
        "rework must not spawn a second reviewer before remediation resubmits"
    );
    drop(conn);

    let remediation_result_seen = handle
        .lines
        .iter()
        .any(|line| line.contains(&format!("worker {remediation_name} result")))
        || handle.wait_for(&format!("worker {remediation_name} result"), 15);
    assert!(
        remediation_result_seen,
        "remediation result not seen. Lines: {:?}",
        handle.lines
    );

    // Remediation worker commits and signals; the daemon updates the same PR.
    quorum_done(home.path(), &["--agent", &remediation_name]);

    // Task should transition back to in-review (rework pushed).
    assert!(
        handle.wait_for("ready for review", 15),
        "PR not returned to review after remediation submit. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// #175 negative: pending/timed-out CI does NOT spawn a remediation worker.
#[test]
fn pending_checks_no_remediation() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());
    let checks_state = home.path().join("pending_checks_state");
    std::fs::write(&checks_state, "ready").unwrap();
    let checks_cmd = format!("cat {}", checks_state.to_string_lossy());

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

    // Checks always return "pending" → will timeout.
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

    // Wait for timeout to fire.
    assert!(
        handle.wait_for("timed out", 30) || handle.wait_for("checks timeout", 30),
        "checks timeout not detected. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_millis(500));
    handle.drain_pending_lines();

    // No remediation worker should be spawned for timeout.
    let remediation_spawned = handle
        .lines
        .iter()
        .any(|l| l.contains("spawning remediation worker"));
    assert!(
        !remediation_spawned,
        "remediation worker should NOT be spawned for timed-out CI. Lines: {:?}",
        handle.lines
    );

    handle.sigkill();
}

/// #175: rework cap bounds replacement attempts. After cap exhaustion,
/// task goes to failed — no unbounded respawn loop.
#[test]
fn rework_cap_bounds_remediation_attempts() {
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

    // Set rework_round to the cap directly so the next VerdictChanges
    // exceeds it and transitions to Failed.
    {
        let db_path = home
            .path()
            .join("repos")
            .join("test__repo")
            .join("quorum.db");
        let conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute(
            "UPDATE tasks SET rework_round=?1 WHERE id=?2",
            rusqlite::params![quorum_core::lifecycle::REWORK_CAP, task_id],
        )
        .unwrap();
        let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
        assert_eq!(
            task.rework_round,
            i64::from(quorum_core::lifecycle::REWORK_CAP)
        );
    }

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &[
            "--merge-checks-cmd",
            "printf 'failed\\nci-lint'",
            "--merge-checks-timeout-secs",
            "10",
            "--merge-checks-poll-secs",
            "1",
        ],
    );

    assert!(
        handle.wait_for("recovery: complete", 15),
        "recovery did not complete. Lines: {:?}",
        handle.lines
    );

    // Checks fail before review, but the rework cap is exhausted: the task
    // fails directly without consuming a reviewer or spawning remediation.
    assert!(
        handle.wait_for("lifecycle: task #1 -> failed", 30),
        "capped pre-review CI failure not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        !handle
            .lines
            .iter()
            .any(|line| line.contains("spawning reviewer")),
        "capped failed CI must not consume a reviewer. Lines: {:?}",
        handle.lines
    );

    std::thread::sleep(Duration::from_millis(1500));
    handle.drain_pending_lines();

    let remediation_spawned = handle
        .lines
        .iter()
        .any(|l| l.contains("spawning remediation worker"));
    assert!(
        !remediation_spawned,
        "remediation should NOT be spawned when rework cap is exhausted. Lines: {:?}",
        handle.lines
    );

    // Verify the task transitions to failed (rework cap exceeded path).
    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    let conn = quorum_core::db::open(&db_path).unwrap();
    let task = quorum_core::tasks::get(&conn, task_id).unwrap().unwrap();
    assert_eq!(
        task.status, "failed",
        "task should be failed after rework cap exhaustion, got: {}",
        task.status
    );

    handle.sigkill();
}
