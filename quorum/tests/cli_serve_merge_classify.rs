//! Integration tests for merge-failure classification (#149).
//!
//! Verifies that policy-blocked merge failures park the task (no rework,
//! no re-claim) and retryable merge failures send exactly one rework turn.

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

fn init_git_repo_with_bare_origin(dir: &std::path::Path) -> tempfile::TempDir {
    let origin = tempfile::tempdir().unwrap();
    Command::new("git")
        .args(["init", "--bare", &origin.path().to_string_lossy()])
        .status()
        .unwrap();
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
        .args([
            "-C",
            &d,
            "remote",
            "add",
            "origin",
            &origin.path().to_string_lossy(),
        ])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "push", "-u", "origin", "main"])
        .status()
        .unwrap();
    origin
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
        Self::start_with_env(home, repo, wt_base, names, merge_cmd, extra_args, &[])
    }

    fn start_with_env(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        merge_cmd: &str,
        extra_args: &[&str],
        extra_env: &[(&str, &std::path::Path)],
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
  sha="$(git -C "$QUORUM_TEST_REPO" ls-remote origin "refs/heads/$branch" | awk 'NR == 1 { print $1 }')"
  test -n "$sha"
  printf '{"headRefName":"%s","headRefOid":"%s","isCrossRepository":false,"baseRefName":"main","state":"OPEN"}\n' "$branch" "$sha"
else
  printf 'unsupported gh invocation: %s\n' "$*" >&2
  exit 1
fi
"#,
        )
        .unwrap();
        std::fs::set_permissions(&gh_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let real_git =
            String::from_utf8(Command::new("which").arg("git").output().unwrap().stdout).unwrap();
        let git_path = gh_shim.path().join("git");
        std::fs::write(
            &git_path,
            format!(
                "#!/bin/sh\n\
                 if [ \"${{1:-}}\" = ls-remote ] && [ \"${{2:-}}\" = origin ] && \
                    [ \"${{3:-}}\" = refs/heads/main ] && \
                    [ -n \"${{QUORUM_TEST_LS_REMOTE_STATE:-}}\" ]; then\n\
                   cat \"$QUORUM_TEST_LS_REMOTE_STATE\"\n\
                   exit 0\n\
                 fi\n\
                 exec '{}' \"$@\"\n",
                real_git.trim()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&git_path, std::fs::Permissions::from_mode(0o755)).unwrap();
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

        let mut command = Command::new(cargo_bin("quorum"));
        command
            .env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .env("PATH", path)
            .env("QUORUM_TEST_GH_STATE", &gh_state)
            .env("QUORUM_TEST_REPO", repo)
            .args(&args)
            .stderr(Stdio::piped())
            .stdout(Stdio::null());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let mut child = command.spawn().unwrap();

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

    fn wait_for_exit(&mut self, timeout_secs: u64) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
        while std::time::Instant::now() < deadline {
            if let Some(status) = self.child.try_wait().unwrap() {
                while let Ok(line) = self.rx.try_recv() {
                    self.lines.push(line);
                }
                return status;
            }
            while let Ok(line) = self.rx.try_recv() {
                self.lines.push(line);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "daemon did not exit within {timeout_secs}s: {:?}",
            self.lines
        )
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

fn ready_r2_review(handle: &mut ServeHandle) -> String {
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

    r2_name
}

fn complete_r2_review(home: &std::path::Path, handle: &mut ServeHandle, pr: &str) {
    let r2_name = ready_r2_review(handle);
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

fn test_db(home: &std::path::Path) -> std::path::PathBuf {
    home.join("repos/test__repo/quorum.db")
}

fn managed_worker_pid(home: &std::path::Path, agent: &str) -> i32 {
    let db = test_db(home);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let conn = quorum_core::db::open(&db).unwrap();
        if let Ok(pid) = conn.query_row(
            "SELECT pid FROM journal WHERE role='worker' AND agent=?1 AND pid IS NOT NULL",
            [agent],
            |row| row.get(0),
        ) {
            return pid;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("managed worker {agent} pid was not persisted")
}

fn append_reviewer_approval(home: &std::path::Path, reviewer: &str, pr: i64) -> i64 {
    let mut conn = quorum_core::db::open(&test_db(home)).unwrap();
    quorum_core::mailbox::append(
        &mut conn,
        &quorum_core::mailbox::MailboxRow {
            agent: reviewer.into(),
            kind: quorum_core::mailbox::MailboxKind::Done,
            task_id: Some(1),
            pr: Some(pr),
            verdict: Some("approved".into()),
            feedback: None,
            note: None,
            to_agent: None,
            payload: Some(r#"{"blocking":0}"#.into()),
        },
    )
    .unwrap()
}

fn wait_for_gate(gate: &std::path::Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if std::fs::read(gate).ok().as_deref() == Some(b"captured") {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("mailbox snapshot gate was not captured")
}

fn wait_for_worker_cleanup(home: &std::path::Path, agent: &str) {
    let db = test_db(home);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let conn = quorum_core::db::open(&db).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM journal WHERE role='worker' AND agent=?1",
                [agent],
                |row| row.get(0),
            )
            .unwrap();
        if count == 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("managed worker {agent} was not cleaned up")
}

fn obstruct_unused_worktree_paths(wt_base: &std::path::Path) {
    for i in 0..20 {
        let path = wt_base.join(format!("Agent{i}-t1"));
        if path.exists() {
            continue;
        }
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("blocker"), b"force git worktree add failure").unwrap();
    }
}

#[test]
fn policy_blocked_merge_parks_task_no_rework() {
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

    seed_task(home.path(), "Task for merge-blocked test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "echo 'not mergeable: the base branch policy prohibits the merge' >&2 && exit 1",
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
        handle.wait_for("MERGE BLOCKED", 15),
        "MERGE BLOCKED log not seen. Lines: {:?}",
        handle.lines
    );

    let saw_rework = handle.lines.iter().any(|l| l.contains("rework"));
    assert!(
        !saw_rework,
        "rework should NOT be sent for policy-blocked merge. Lines: {:?}",
        handle.lines
    );

    handle.stop();

    let retry_branch = format!("daemon/{}-t1", worker_name.to_lowercase());
    assert!(Command::new("git")
        .args([
            "-C",
            &repo_dir.path().to_string_lossy(),
            "branch",
            "-f",
            &retry_branch,
            "main",
        ])
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .args([
            "-C",
            &repo_dir.path().to_string_lossy(),
            "push",
            "origin",
            &retry_branch,
        ])
        .status()
        .unwrap()
        .success());

    let get_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", "1"])
        .output()
        .unwrap();
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("\"status\":\"failed\"") || stdout.contains("\"status\": \"failed\""),
        "task should be parked after policy-blocked merge, got: {stdout}"
    );
    assert!(stdout.contains("daemon_resume_status"));
    assert!(stdout.contains("merging"));
    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    {
        let conn = quorum_core::db::open(&db_path).unwrap();
        assert_eq!(
            quorum_core::approvals::get_for_pr(&conn, 1).unwrap().len(),
            2,
            "policy park must retain exact-head R1/R2 authority"
        );
    }

    let retry = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-retry", "--task-id", "1", "--by", "operator"])
        .output()
        .unwrap();
    assert!(retry.status.success());
    let retry_stdout = String::from_utf8_lossy(&retry.stdout);
    assert!(
        retry_stdout.contains("\"status\":\"merging\""),
        "retry must request one daemon-owned merge replay: {retry_stdout}"
    );

    std::fs::write(
        &names_file,
        (0..20)
            .map(|i| format!("RetryAgent{i}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let mut retry_handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "exit 0",
        &[],
    );
    std::fs::write(
        retry_handle
            ._gh_shim
            .as_ref()
            .unwrap()
            .path()
            .join("state/1"),
        &retry_branch,
    )
    .unwrap();
    assert!(
        retry_handle.wait_for("PR #1 merged from explicit durable-approval retry", 15),
        "retried task never reached a second merge attempt: {:?}",
        retry_handle.lines
    );
    assert!(
        !retry_handle
            .lines
            .iter()
            .any(|line| line.contains("spawning reviewer")),
        "unchanged-head retry must not provision another reviewer: {:?}",
        retry_handle.lines
    );
    retry_handle.stop();
    let conn = quorum_core::db::open(&db_path).unwrap();
    assert_eq!(
        quorum_core::tasks::get(&conn, 1).unwrap().unwrap().status,
        "done"
    );
    assert!(quorum_core::approvals::get_for_pr(&conn, 1)
        .unwrap()
        .is_empty());
}

#[test]
fn conflicting_pr_skips_merge_sends_rework() {
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

    seed_task(home.path(), "Task for conflicting PR test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "true",
        &["--merge-mergeability-cmd", "echo conflicting"],
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
        handle.wait_for("CONFLICTING", 15),
        "CONFLICTING log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework #1 (pre-merge conflict)", 15),
        "rework turn not sent for pre-merge conflict. Lines: {:?}",
        handle.lines
    );

    // Merge was never attempted (merge-cmd is "true" — if it ran, we'd
    // see a merge log line but no CONFLICTING skip).
    let saw_merge = handle.lines.iter().any(|l| l.contains("merged"));
    assert!(
        !saw_merge,
        "merge should NOT have been attempted. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

#[test]
fn retryable_merge_sends_rework_turn() {
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

    seed_task(home.path(), "Task for retryable merge test");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        "echo 'merge conflict in src/main.rs' >&2 && exit 1",
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
        handle.wait_for("retryable", 15),
        "retryable merge log not seen. Lines: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("rework #1 (merge failure)", 15),
        "rework turn not sent. Lines: {:?}",
        handle.lines
    );

    handle.stop();
}

#[test]
fn graceful_drain_defers_merge_remediation_until_restart() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    let gh_state = tempfile::tempdir().unwrap();
    let names_file = write_names_file(home.path());
    let snapshot_gate = home.path().join("mailbox-snapshot.gate");
    let sha_state = home.path().join("origin-main-sha");
    let prompt_log = home.path().join("remediation-prompts.jsonl");
    let merge_calls = home.path().join("merge-calls");
    let merge_cmd = format!(
        "printf 'call\\n' >> '{}'; echo 'merge conflict in src/main.rs' >&2; exit 1",
        merge_calls.display()
    );

    let _origin = init_git_repo_with_bare_origin(repo_dir.path());
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let running_sha = Command::new("git")
        .args(["-C", &source.to_string_lossy(), "rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(running_sha.status.success());
    let running_sha = String::from_utf8(running_sha.stdout).unwrap();
    std::fs::write(
        &sha_state,
        format!("{}\trefs/heads/main\n", running_sha.trim()),
    )
    .unwrap();
    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "Task for drained merge remediation");

    let first_env = [
        ("QUORUM_TEST_MAILBOX_SNAPSHOT_GATE", snapshot_gate.as_path()),
        ("QUORUM_TEST_GH_STATE", gh_state.path()),
        ("QUORUM_TEST_LS_REMOTE_STATE", sha_state.as_path()),
    ];
    let mut first = ServeHandle::start_with_env(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &merge_cmd,
        &[
            "--self-update-drain",
            "--self-repo",
            "test/repo",
            "--sha-poll-interval-secs",
            "30",
        ],
        &first_env,
    );
    assert!(
        first.wait_for("spawning agent", 15) && first.wait_for("result", 15),
        "worker did not run: {:?}",
        first.lines
    );
    let worker = first.extract_agent_name("spawning agent ").unwrap();
    let worker_pid = managed_worker_pid(home.path(), &worker);
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);

    assert!(
        first.wait_for("spawning reviewer", 15) && first.wait_for("result", 15),
        "R1 did not run: {:?}",
        first.lines
    );
    let r1 = first.extract_agent_name("spawning reviewer ").unwrap();
    assert_eq!(unsafe { libc::kill(worker_pid, libc::SIGKILL) }, 0);
    assert!(
        first.wait_for("exited after recorded submission", 15),
        "submitted worker exit was not cleanup-only: {:?}",
        first.lines
    );
    wait_for_worker_cleanup(home.path(), &worker);
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
    let r2 = ready_r2_review(&mut first);

    // Pause one tick after its empty mailbox snapshot. The approval appended
    // while paused is therefore processed only by the next tick, after the
    // base-advance detection has entered graceful drain at the outer boundary.
    std::fs::write(&snapshot_gate, b"waiting").unwrap();
    wait_for_gate(&snapshot_gate);
    let approval_mailbox = append_reviewer_approval(home.path(), &r2, 1);
    std::fs::write(&sha_state, format!("{}\trefs/heads/main\n", "f".repeat(40))).unwrap();
    std::thread::sleep(Duration::from_secs(31));
    std::fs::remove_file(&snapshot_gate).unwrap();

    assert!(
        first.wait_for("DRAIN: entering drain mode source=self-update", 15),
        "base advance did not enter self-update drain: {:?}",
        first.lines
    );
    assert!(
        first.wait_for("deferring durable merge-failure remediation", 15),
        "drain did not defer remediation: {:?}",
        first.lines
    );
    assert!(
        first.wait_for("DRAIN: all agents finished", 15),
        "first daemon did not drain: {:?}",
        first.lines
    );
    assert_eq!(
        first.wait_for_exit(15).code(),
        Some(75),
        "self-update drain must request supervisor restart"
    );

    let expected_feedback = "Merge of PR #1 failed: merge conflict in src/main.rs\n\n\
Preserve the published PR head, merge main into the PR branch, resolve conflicts, commit, and submit without pushing. Never rebase.";
    {
        let conn = quorum_core::db::open(&test_db(home.path())).unwrap();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.rework_round, 1);
        assert_eq!(task.author.as_deref(), Some(worker.as_str()));
        assert_eq!(quorum_core::tasks::extract_pr_number(&task.refs), Some(1));
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["remediation_feedback"], expected_feedback);
        assert_eq!(refs["daemon_rework_retry_requested"], true);
        assert!(refs.get("daemon_merge_retry").is_none());
        assert!(quorum_core::approvals::get_for_pr(&conn, 1)
            .unwrap()
            .is_empty());
        let active_claims: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target='task#1' AND active=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_claims, 0, "drain must not claim remediation");
        let remediation_runs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE task_id=1 AND role='worker'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remediation_runs, 1, "no remediation run may exist yet");
        let event_sequence = conn
            .prepare(
                "SELECT kind FROM events
                 WHERE subject='task#1'
                   AND kind IN ('merge_attempt_started','task_rework','task_open')
                 ORDER BY seq",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            event_sequence,
            ["merge_attempt_started", "task_rework"],
            "drain must preserve the admitted merge -> rework sequence"
        );
        let consumed: bool = conn
            .query_row(
                "SELECT consumed_at IS NOT NULL FROM mailbox WHERE id=?1",
                [approval_mailbox],
                |row| row.get(0),
            )
            .unwrap();
        assert!(consumed);
    }
    assert_eq!(
        std::fs::read_to_string(&merge_calls)
            .unwrap()
            .lines()
            .count(),
        1
    );

    let restart_env = [
        ("QUORUM_TEST_GH_STATE", gh_state.path()),
        ("FAKE_AGENT_PROMPT_LOG", prompt_log.as_path()),
    ];
    let mut restarted = ServeHandle::start_with_env(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &merge_cmd,
        &[],
        &restart_env,
    );
    assert!(
        restarted.wait_for("durable remediation retry: provisioning task #1", 15),
        "restart did not select durable remediation: {:?}",
        restarted.lines
    );
    assert!(
        restarted.wait_for("spawning remediation worker Agent", 15)
            && restarted.wait_for("result", 15),
        "restart did not run remediation: {:?}",
        restarted.lines
    );
    let remediation = restarted
        .extract_agent_name("spawning remediation worker ")
        .unwrap();
    let prompts = std::fs::read_to_string(&prompt_log).unwrap();
    let prompt_text = prompts
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|value| {
            value
                .pointer("/message/content")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        prompt_text.contains(expected_feedback),
        "remediation prompt lost exact merge feedback: {prompts}"
    );
    {
        let conn = quorum_core::db::open(&test_db(home.path())).unwrap();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.rework_round, 1);
        assert_eq!(task.assignee.as_deref(), Some(remediation.as_str()));
        let active_claims: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE target='task#1' AND active=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_claims, 1, "marker must be claimed exactly once");
        let worker_runs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE task_id=1 AND role='worker'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(worker_runs, 2, "one original plus one remediation worker");
        let reviewer_runs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE task_id=1 AND role='reviewer'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reviewer_runs, 2, "restart must not duplicate a reviewer");
    }

    // The established bounded path consumes the one-shot marker when the
    // remediation worker submits its completed turn and returns to review.
    quorum_done(home.path(), &["--agent", &remediation, "--pr", "1"]);
    assert!(
        restarted.wait_for("lifecycle: task #1 -> in-review", 15),
        "remediation submission did not return the task to review: {:?}",
        restarted.lines
    );
    {
        let conn = quorum_core::db::open(&test_db(home.path())).unwrap();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(task.status, "in-review");
        assert_eq!(task.rework_round, 1);
        assert_eq!(quorum_core::tasks::extract_pr_number(&task.refs), Some(1));
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert!(refs.get("daemon_rework_retry_requested").is_none());
        assert!(refs.get("remediation_feedback").is_none());
        let worker_runs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE task_id=1 AND role='worker'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let reviewer_runs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE task_id=1 AND role='reviewer'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!((worker_runs, reviewer_runs), (2, 2));
    }
    assert_eq!(
        restarted
            .lines
            .iter()
            .filter(|line| line.contains("spawning remediation worker Agent"))
            .count(),
        1
    );
    assert!(
        restarted
            .lines
            .iter()
            .all(|line| !line.contains("spawning reviewer")),
        "restart must not duplicate reviewer provisioning: {:?}",
        restarted.lines
    );
    assert_eq!(
        std::fs::read_to_string(&merge_calls)
            .unwrap()
            .lines()
            .count(),
        1,
        "restart must not replay the admitted merge call"
    );
    assert_eq!(
        unsafe { libc::kill(restarted.child.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    assert!(
        restarted.wait_for("DRAIN: entering drain mode source=signal", 15),
        "restart did not enter drain: {:?}",
        restarted.lines
    );
    assert!(restarted.wait_for_exit(15).success());
    assert!(
        restarted.lines.iter().any(|line| line
            .contains("shutting down (signal, no in-flight agents)")
            || line.contains("DRAIN: all agents finished")),
        "restart did not drain cleanly: {:?}",
        restarted.lines
    );
}

#[test]
fn non_drain_remediation_provision_failure_still_parks() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();
    let names_file = write_names_file(home.path());
    let merge_calls = home.path().join("merge-calls");
    let merge_cmd = format!(
        "printf 'call\\n' >> '{}'; echo 'merge conflict in src/main.rs' >&2; exit 1",
        merge_calls.display()
    );

    let _origin = init_git_repo_with_bare_origin(repo_dir.path());
    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    seed_task(home.path(), "Task for failed remediation provisioning");
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &merge_cmd,
        &[],
    );
    assert!(handle.wait_for("spawning agent", 15) && handle.wait_for("result", 15));
    let worker = handle.extract_agent_name("spawning agent ").unwrap();
    let worker_pid = managed_worker_pid(home.path(), &worker);
    quorum_done(home.path(), &["--agent", &worker, "--pr", "1"]);
    assert!(handle.wait_for("spawning reviewer", 15) && handle.wait_for("result", 15));
    let r1 = handle.extract_agent_name("spawning reviewer ").unwrap();
    assert_eq!(unsafe { libc::kill(worker_pid, libc::SIGKILL) }, 0);
    assert!(handle.wait_for("exited after recorded submission", 15));
    wait_for_worker_cleanup(home.path(), &worker);
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
    let r2 = ready_r2_review(&mut handle);
    obstruct_unused_worktree_paths(wt_base.path());
    quorum_done(
        home.path(),
        &[
            "--agent",
            &r2,
            "--pr",
            "1",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ],
    );
    assert!(
        handle.wait_for("remediation: worktree provision failed", 15),
        "provision failure was not observed: {:?}",
        handle.lines
    );
    assert!(
        handle.wait_for("PARKED: task #1", 15),
        "non-drain provision failure did not park: {:?}",
        handle.lines
    );

    let conn = quorum_core::db::open(&test_db(home.path())).unwrap();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    assert_eq!(task.rework_round, 1);
    assert_eq!(quorum_core::tasks::extract_pr_number(&task.refs), Some(1));
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["daemon_parked"], true);
    assert_eq!(refs["daemon_resume_status"], "rework");
    assert!(refs["daemon_parked_reason"]
        .as_str()
        .unwrap()
        .contains("remediation provisioning failed for PR #1"));
    assert!(refs["remediation_feedback"]
        .as_str()
        .unwrap()
        .contains("merge conflict in src/main.rs"));
    assert!(quorum_core::approvals::get_for_pr(&conn, 1)
        .unwrap()
        .is_empty());
    let worker_runs: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE task_id=1 AND role='worker'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(worker_runs, 1, "failed provisioning must not create a run");
    let active_claims: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM claims WHERE target='task#1' AND active=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_claims, 0);
    drop(conn);
    assert_eq!(
        std::fs::read_to_string(&merge_calls)
            .unwrap()
            .lines()
            .count(),
        1
    );
    handle.stop();
}
