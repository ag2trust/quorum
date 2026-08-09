//! Deterministic production-path coverage for mixed provider lifecycle routing.

mod common;

use common::{wait_until, WaitState};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn cargo_bin(name: &str) -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin(name)
}

fn init_git_repo(dir: &std::path::Path) {
    let d = dir.to_string_lossy();
    for args in [
        vec!["-C", &d, "init", "-b", "main"],
        vec!["-C", &d, "config", "user.email", "test@test.com"],
        vec!["-C", &d, "config", "user.name", "Test"],
        vec!["-C", &d, "commit", "--allow-empty", "-m", "init"],
        vec!["-C", &d, "remote", "add", "origin", &d],
        vec!["-C", &d, "fetch", "origin"],
    ] {
        assert!(Command::new("git").args(args).status().unwrap().success());
    }
}

fn write_names(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("names.txt");
    let mut file = std::fs::File::create(&path).unwrap();
    for i in 0..20 {
        writeln!(file, "Agent{i}").unwrap();
    }
    path
}

fn routing_config(provider: &str, model: &str) -> String {
    let (runner, worker_model, reviewer_model, planner_profile) = if provider == "codex" {
        ("codex", "gpt-5.6-terra", "gpt-5.6-terra", "planner")
    } else {
        ("claude", model, "claude-opus-4-7", "primary")
    };
    let planner = if provider == "codex" {
        "[model_profiles.planner]\nrunner = \"claude\"\nmodel = \"claude-opus-4-8\"\neffort = \"high\"\n"
    } else {
        ""
    };
    format!(
        "[model_profiles.primary]\nrunner = \"{runner}\"\nmodel = \"{worker_model}\"\neffort = \"high\"\n\
         [model_profiles.reviewer]\nrunner = \"{runner}\"\nmodel = \"{reviewer_model}\"\neffort = \"high\"\n\
         {planner}\
         [routing.classifier]\nprimary = 100\n[routing.planner]\n{planner_profile} = 100\n\
         [routing.collector]\nprimary = 100\n\
         [routing.worker.1]\nprimary = 100\n[routing.worker.2]\nprimary = 100\n[routing.worker.3]\nprimary = 100\n[routing.worker.4]\nprimary = 100\n[routing.worker.5]\nprimary = 100\n\
         [routing.reviewer.1]\nreviewer = 100\n[routing.reviewer.2]\nreviewer = 100\n[routing.reviewer.3]\nreviewer = 100\n[routing.reviewer.4]\nreviewer = 100\n[routing.reviewer.5]\nreviewer = 100\n"
    )
}

fn write_dual_protocol_runner(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("dual-runner.sh");
    std::fs::write(
        &path,
        r#"#!/bin/sh
printf '%s|%s\n' "${QUORUM_AGENT:-none}" "$*" >> "$RUNNER_LOG"
if [ "$1" = "exec" ]; then
  printf '{"type":"thread.started","thread_id":"thread-%s"}\n' "${QUORUM_AGENT:-none}"
  printf '{"type":"turn.started"}\n'
  printf '{"type":"item.completed","item":{"id":"i","type":"agent_message","text":"done"}}\n'
  printf '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}}\n'
  printf 'hold-pid|%s\n' "$$" >> "$RUNNER_LOG"
  read -r _ < "/dev/fd/$RUNNER_HOLD_FD" || true
else
  while IFS= read -r line; do
    printf '{"type":"assistant","message":{"content":"done"}}\n'
    printf '{"type":"result","result":"done","usage":{"input_tokens":10,"output_tokens":5},"total_cost_usd":0.001,"is_error":false}\n'
  done
fi
"#,
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    path
}

fn runner_hold_pipe() -> (File, File) {
    let mut fds = [-1; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);

    // The daemon and every provider process inherit only the read end. The
    // fixture retains the close-on-exec writer, so dropping the ServeHandle
    // broadcasts EOF to every sticky fake provider without a timer.
    let writer_flags = unsafe { libc::fcntl(fds[1], libc::F_GETFD) };
    assert_ne!(writer_flags, -1);
    assert_ne!(
        unsafe { libc::fcntl(fds[1], libc::F_SETFD, writer_flags | libc::FD_CLOEXEC) },
        -1
    );

    unsafe { (File::from_raw_fd(fds[0]), File::from_raw_fd(fds[1])) }
}

struct ServeHandle {
    child: std::process::Child,
    rx: mpsc::Receiver<String>,
    lines: Vec<String>,
    _sentinel: tempfile::TempDir,
    runner_hold_writer: Option<File>,
}

impl Drop for ServeHandle {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
        self.release_runner_hold();
    }
}

impl ServeHandle {
    fn release_runner_hold(&mut self) {
        drop(self.runner_hold_writer.take());
    }

    fn wait_for(&mut self, needle: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while std::time::Instant::now() < deadline {
            match self.rx.recv_timeout(deadline - std::time::Instant::now()) {
                Ok(line) => {
                    let found = line.contains(needle);
                    self.lines.push(line);
                    if found {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        panic!("did not see {needle:?}: {:?}", self.lines);
    }

    fn agent_after(&self, marker: &str) -> String {
        self.lines
            .iter()
            .rev()
            .find_map(|line| {
                line.split(marker)
                    .nth(1)
                    .and_then(|rest| rest.split_whitespace().next())
            })
            .unwrap_or_else(|| panic!("no agent after {marker:?}: {:?}", self.lines))
            .to_string()
    }

    fn stop(mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGINT) };
        assert!(self.child.wait().unwrap().success());
        self.release_runner_hold();
    }

    fn stop_mut(&mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGINT) };
        assert!(self.child.wait().unwrap().success());
        self.release_runner_hold();
    }

    fn crash_mut(&mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
        let status = self.child.wait().unwrap();
        assert!(!status.success());
        self.release_runner_hold();
    }
}

struct Case {
    home: tempfile::TempDir,
    _repo: tempfile::TempDir,
    _worktrees: tempfile::TempDir,
    gh_shim: tempfile::TempDir,
    runner_log: std::path::PathBuf,
    handle: ServeHandle,
}

impl Case {
    fn start(default_provider: &str, model: &str, labels: Option<&str>) -> Self {
        Self::start_with_role_config(default_provider, model, labels, None)
    }

    fn start_with_role_config(
        default_provider: &str,
        model: &str,
        labels: Option<&str>,
        role_config: Option<&str>,
    ) -> Self {
        Self::start_with_pr_assignment(default_provider, model, labels, role_config, None, None)
    }

    fn start_review_only(default_provider: &str, model: &str) -> Self {
        Self::start_with_pr_assignment(default_provider, model, None, None, Some(1), None)
    }

    fn start_continue(default_provider: &str, model: &str, pr: i64) -> Self {
        Self::start_with_pr_assignment(default_provider, model, None, None, None, Some(pr))
    }

    fn start_with_pr_assignment(
        default_provider: &str,
        model: &str,
        labels: Option<&str>,
        role_config: Option<&str>,
        review_pr: Option<i64>,
        continue_pr: Option<i64>,
    ) -> Self {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let worktrees = tempfile::tempdir().unwrap();
        init_git_repo(repo.path());
        let names = write_names(home.path());
        let runner = write_dual_protocol_runner(home.path());
        let runner_log = home.path().join("runner.log");
        std::fs::write(&runner_log, "").unwrap();
        let config_path = home.path().join("serve.toml");
        let config_contents = role_config
            .map(|contents| {
                if contents == CHATGPT_ONLY_ROLE_CONFIG {
                    routing_config("codex", "gpt-5.6-terra")
                } else {
                    contents.to_owned()
                }
            })
            .unwrap_or_else(|| routing_config(default_provider, model));
        std::fs::write(&config_path, &config_contents).unwrap();

        assert!(Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .arg("init")
            .status()
            .unwrap()
            .success());
        let mut create = Command::new(cargo_bin("quorum"));
        create
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .args([
                "task-create",
                "--title",
                "provider lifecycle",
                "--created-by",
                "test",
            ]);
        if let Some(pr) = review_pr {
            let pr_string = pr.to_string();
            create.args(["--review-pr", &pr_string]);
        } else if let Some(pr) = continue_pr {
            let pr_string = pr.to_string();
            create.args(["--continue-pr", &pr_string]);
        }
        assert!(create.status().unwrap().success());
        let db_path = home.path().join("repos/test__repo/quorum.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        quorum_core::classify::store_classifications(
            &mut conn,
            &[quorum_core::classify::TaskClassification {
                task_id: 1,
                cx_est: 2,
                size: "S".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: Vec::new(),
            }],
            "test-classifier:v1",
            1,
        )
        .unwrap();
        if let Some(labels) = labels {
            conn.execute("UPDATE tasks SET labels = ?1 WHERE id = 1", [labels])
                .unwrap();
        }

        // A review-only task has no managed branch or worker. Persist the
        // resolved PR identity before the daemon begins orphan review
        // provisioning so the test exercises the same verified-PR path as
        // production rather than a branch-name fallback.
        if let Some(pr) = review_pr.or(continue_pr) {
            let head_ref = if review_pr.is_some() {
                format!("review-pr-{pr}")
            } else {
                format!("continue-pr-{pr}")
            };
            assert!(Command::new("git")
                .args(["-C", &repo.path().to_string_lossy(), "branch", &head_ref])
                .status()
                .unwrap()
                .success());
            let head_sha = String::from_utf8(
                Command::new("git")
                    .args(["-C", &repo.path().to_string_lossy(), "rev-parse", "HEAD"])
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap();
            if review_pr.is_some() {
                let mut conn =
                    quorum_core::db::open(&home.path().join("repos/test__repo/quorum.db")).unwrap();
                quorum_core::pr_targets::upsert(
                    &mut conn,
                    1,
                    pr,
                    &head_ref,
                    head_sha.trim(),
                    false,
                )
                .unwrap();
            }
        }

        let gh_shim = tempfile::tempdir().unwrap();
        let gh_state = gh_shim.path().join("state");
        std::fs::create_dir_all(&gh_state).unwrap();
        if let Some(pr) = review_pr.or(continue_pr) {
            let head_ref = if review_pr.is_some() {
                format!("review-pr-{pr}")
            } else {
                format!("continue-pr-{pr}")
            };
            std::fs::write(gh_state.join(pr.to_string()), head_ref).unwrap();
        }
        let gh_path = gh_shim.path().join("gh");
        std::fs::write(
            &gh_path,
            r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$QUORUM_TEST_GH_STATE/calls"
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
  sha="$(git -C "$QUORUM_TEST_REPO" ls-remote origin "refs/heads/$branch" | awk '{print $1}')"
  if [ -z "$sha" ]; then
    sha="$(git -C "$QUORUM_TEST_REPO" rev-parse "refs/heads/$branch")"
  fi
  base=main
  state=OPEN
  if [ -f "$QUORUM_TEST_GH_STATE/base-$pr" ]; then
    base="$(cat "$QUORUM_TEST_GH_STATE/base-$pr")"
  fi
  if [ -f "$QUORUM_TEST_GH_STATE/state-$pr" ]; then
    state="$(cat "$QUORUM_TEST_GH_STATE/state-$pr")"
  fi
  printf '{"headRefName":"%s","headRefOid":"%s","isCrossRepository":false,"baseRefName":"%s","state":"%s"}\n' "$branch" "$sha" "$base" "$state"
else
  printf 'unsupported gh invocation: %s\n' "$*" >&2
  exit 1
fi
"#,
        )
        .unwrap();
        std::fs::set_permissions(&gh_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let codex_path = gh_shim.path().join("codex");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&runner, &codex_path).unwrap();
        #[cfg(not(unix))]
        {
            std::fs::copy(&runner, &codex_path).unwrap();
        }
        let path = format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", gh_shim.path().display());
        let sentinel = tempfile::tempdir().unwrap();
        let (runner_hold_reader, runner_hold_writer) = runner_hold_pipe();
        let mut serve = Command::new(cargo_bin("quorum"));
        serve
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("RUNNER_LOG", &runner_log)
            .env("RUNNER_HOLD_FD", runner_hold_reader.as_raw_fd().to_string())
            .env("PATH", path)
            .env("QUORUM_TEST_GH_STATE", &gh_state)
            .env("QUORUM_TEST_REPO", repo.path())
            .args([
                "serve",
                "--repo",
                "test/repo",
                "--cap",
                "1",
                "--repo-dir",
                &repo.path().to_string_lossy(),
                "--worktree-base",
                &worktrees.path().to_string_lossy(),
                "--names-file",
                &names.to_string_lossy(),
                "--merge-cmd",
                "true",
                "--merge-checks-cmd",
                "echo ready",
                "--merge-checks-timeout-secs",
                "10",
                "--merge-checks-poll-secs",
                "1",
                "--exit-when-gone",
                &sentinel.path().to_string_lossy(),
            ]);
        serve.args(["--config", &config_path.to_string_lossy()]);
        if !config_contents.contains("runner = \"codex\"") {
            serve.args(["--agent-bin", &runner.to_string_lossy()]);
        }
        let mut child = serve
            .stderr(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            home,
            _repo: repo,
            _worktrees: worktrees,
            gh_shim,
            runner_log,
            handle: ServeHandle {
                child,
                rx,
                lines: Vec::new(),
                _sentinel: sentinel,
                runner_hold_writer: Some(runner_hold_writer),
            },
        }
    }

    fn db(&self) -> rusqlite::Connection {
        quorum_core::db::open(&self.home.path().join("repos/test__repo/quorum.db")).unwrap()
    }

    /// Wait until the daemon has completed at least one full scheduling tick.
    ///
    /// A single mailbox marker is observed in Phase 2, before reviewer and
    /// worker provisioning. Appending the second marker only after the first
    /// is consumed forces it into a later tick, proving that every phase of
    /// the prior tick ran without relying on its 500ms cadence.
    fn wait_for_completed_tick(&mut self) {
        for marker in ["ProviderLifecycleTick0", "ProviderLifecycleTick1"] {
            let mut conn = self.db();
            quorum_core::mailbox::append(
                &mut conn,
                &quorum_core::mailbox::MailboxRow {
                    agent: marker.into(),
                    kind: quorum_core::mailbox::MailboxKind::TaskUpdate,
                    task_id: None,
                    pr: None,
                    verdict: None,
                    feedback: None,
                    note: Some("readiness barrier".into()),
                    to_agent: None,
                    payload: None,
                },
            )
            .unwrap();
            drop(conn);
            self.handle
                .wait_for(&format!("consuming unmatched task_update from {marker}"));
        }
    }

    fn restart(&mut self, default_provider: &str, model: &str) {
        self.restart_with_role_config(default_provider, model, None);
    }

    fn restart_with_role_config(
        &mut self,
        default_provider: &str,
        model: &str,
        role_config: Option<&str>,
    ) {
        self.handle.stop_mut();
        self.restart_after_stop(default_provider, model, role_config);
    }

    fn restart_after_stop(
        &mut self,
        default_provider: &str,
        model: &str,
        role_config: Option<&str>,
    ) {
        let names = self.home.path().join("names.txt");
        let runner = self.home.path().join("dual-runner.sh");
        let config_path = self.home.path().join("restart-serve.toml");
        let config_contents = role_config
            .map(|contents| {
                if contents == CHATGPT_ONLY_ROLE_CONFIG {
                    routing_config("codex", "gpt-5.6-terra")
                } else {
                    contents.to_owned()
                }
            })
            .unwrap_or_else(|| routing_config(default_provider, model));
        std::fs::write(&config_path, &config_contents).unwrap();
        let sentinel = tempfile::tempdir().unwrap();
        let (runner_hold_reader, runner_hold_writer) = runner_hold_pipe();
        let mut serve = Command::new(cargo_bin("quorum"));
        serve
            .env("QUORUM_HOME", self.home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("RUNNER_LOG", &self.runner_log)
            .env("RUNNER_HOLD_FD", runner_hold_reader.as_raw_fd().to_string())
            .env(
                "PATH",
                format!(
                    "{}:/usr/bin:/bin:/usr/sbin:/sbin",
                    self.gh_shim.path().display()
                ),
            )
            .env("QUORUM_TEST_GH_STATE", self.gh_shim.path().join("state"))
            .env("QUORUM_TEST_REPO", self._repo.path())
            .args([
                "serve",
                "--repo",
                "test/repo",
                "--cap",
                "1",
                "--repo-dir",
                &self._repo.path().to_string_lossy(),
                "--worktree-base",
                &self._worktrees.path().to_string_lossy(),
                "--names-file",
                &names.to_string_lossy(),
                "--merge-cmd",
                "true",
                "--merge-checks-cmd",
                "echo ready",
                "--merge-checks-timeout-secs",
                "10",
                "--merge-checks-poll-secs",
                "1",
                "--exit-when-gone",
                &sentinel.path().to_string_lossy(),
            ]);
        serve.args(["--config", &config_path.to_string_lossy()]);
        if !config_contents.contains("runner = \"codex\"") {
            serve.args(["--agent-bin", &runner.to_string_lossy()]);
        }
        let mut child = serve
            .stderr(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        self.handle = ServeHandle {
            child,
            rx,
            lines: Vec::new(),
            _sentinel: sentinel,
            runner_hold_writer: Some(runner_hold_writer),
        };
    }

    fn done(&self, agent: &str, args: &[&str]) {
        let mut conn = self.db();
        let run_id = match quorum_core::capabilities::active_for_agent(&conn, agent).unwrap() {
            Some(cap) => cap.run_id,
            None => {
                let run_id = format!("test-{agent}-{}", std::process::id());
                let role = if args.contains(&"--verdict") {
                    "reviewer"
                } else {
                    "worker"
                };
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64;
                quorum_core::capabilities::issue(&mut conn, &run_id, 1, agent, role, now).unwrap();
                run_id
            }
        };
        let mut command = Command::new(cargo_bin("quorum"));
        let mut done_args = Vec::new();
        let worker_has_bound_pr = args.contains(&"--verdict");
        let mut index = 0;
        while index < args.len() {
            if !worker_has_bound_pr && args[index] == "--pr" {
                index += 2;
            } else {
                done_args.push(args[index]);
                index += 1;
            }
        }
        command
            .env("QUORUM_HOME", self.home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("QUORUM_RUN_ID", run_id)
            .args(["done", "--agent", agent])
            .args(done_args);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "done failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn retry_parked(&self) {
        let output = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", self.home.path())
            .env("QUORUM_REPO", "test/repo")
            .args(["task-retry", "--task-id", "1", "--by", "operator"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "task-retry failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn approve_current_reviewer(&mut self, marker: &str) {
        self.handle.wait_for(marker);
        let reviewer = self.handle.agent_after(marker);
        self.handle.wait_for("turn");
        self.done(
            &reviewer,
            &["--pr", "1", "--verdict", "approved", "--blocking", "0"],
        );
    }

    fn finish(mut self) -> Vec<quorum_core::agent_runs::AgentRun> {
        self.handle.wait_for("spawning agent");
        let worker = self.handle.agent_after("spawning agent ");
        self.handle.wait_for("turn");
        self.done(&worker, &["--pr", "1"]);
        self.approve_current_reviewer("spawning reviewer ");
        self.approve_current_reviewer("R2: pre-merge reviewer ");
        self.handle.wait_for("checks passed");
        self.handle.wait_for("merged — firing MergeSucceeded");
        self.handle.wait_for("lifecycle: task #1 -> done");
        let conn = self.db();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(task.status, "done");
        let claims: i64 = conn
            .query_row(
                "SELECT count(*) FROM claims WHERE active=1 AND target='task:1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claims, 0);
        let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
        drop(conn);
        self.handle.stop();
        runs
    }
}

fn run_routes(runs: &[quorum_core::agent_runs::AgentRun]) -> Vec<(&str, Option<&str>, &str, &str)> {
    runs.iter()
        .filter(|run| run.role == "worker" || run.role == "reviewer")
        .map(|run| {
            (
                run.role.as_str(),
                run.sub_role.as_deref(),
                run.model.as_str(),
                run.provider.as_deref().unwrap(),
            )
        })
        .collect()
}

fn run_routes_with_effort(
    runs: &[quorum_core::agent_runs::AgentRun],
) -> Vec<(&str, Option<&str>, &str, &str, &str)> {
    runs.iter()
        .filter(|run| run.role == "worker" || run.role == "reviewer")
        .map(|run| {
            (
                run.role.as_str(),
                run.sub_role.as_deref(),
                run.model.as_str(),
                run.effort.as_str(),
                run.provider.as_deref().unwrap(),
            )
        })
        .collect()
}

const CHATGPT_ONLY_ROLE_CONFIG: &str = "chatgpt-only-routing";

#[test]
fn continuation_worker_without_pr_recovers_pre_fix_intent_with_spawn_lease() {
    let mut case = Case::start_continue("codex", "gpt-5.6-terra", 10);
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");

    let (worktree, remote_branch, journal_pr, baseline_sha) = {
        let conn = case.db();
        let journal: (String, String, Option<i64>) = conn
            .query_row(
                "SELECT worktree, branch, pr FROM journal WHERE agent=?1",
                [&worker],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let target = quorum_core::pr_targets::get(&conn, 1, 10)
            .unwrap()
            .expect("spawn must persist the immutable continuation baseline");
        (journal.0, journal.1, journal.2, target.head_sha)
    };
    assert_eq!(journal_pr, Some(10));
    assert_eq!(remote_branch, "continue-pr-10");

    assert!(Command::new("git")
        .args([
            "-C",
            &worktree,
            "commit",
            "--allow-empty",
            "-m",
            "continuation work",
        ])
        .status()
        .unwrap()
        .success());
    let worker_sha = String::from_utf8(
        Command::new("git")
            .args(["-C", &worktree, "rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert_ne!(worker_sha, baseline_sha);

    // Exact task #9 incident shape: the old routing bug persisted a
    // new-branch publication intent before push_new_branch rejected the
    // already-existing continuation branch. A retry retains this intent.
    {
        let conn = case.db();
        let pre_fix_intent = serde_json::json!({
            "branch": remote_branch.clone(),
            "local_sha": worker_sha.clone(),
            "pr": null,
            "stage": "intent",
            "expected_remote_sha": null,
        });
        quorum_core::tasks::set_publication_intent(&conn, 1, &pre_fix_intent.to_string(), 2)
            .unwrap();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert!(refs["daemon_publication"]["pr"].is_null());
        assert!(refs["daemon_publication"]["expected_remote_sha"].is_null());
    }

    // Managed workers normally omit --pr. The live slot must retain PR #10,
    // then repair the missing lease only from the persisted pr_targets row.
    case.done(&worker, &[]);
    case.handle.wait_for("PR #10 ready for review");

    let published_sha = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                &case._repo.path().to_string_lossy(),
                "rev-parse",
                "refs/heads/continue-pr-10",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert_eq!(published_sha, worker_sha, "existing PR head must advance");

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "in-review");
    assert_eq!(quorum_core::tasks::extract_pr_number(&task.refs), Some(10));
    let target = quorum_core::pr_targets::get(&conn, 1, 10).unwrap().unwrap();
    assert_eq!(
        target.head_sha, baseline_sha,
        "publication must retain the spawn-time lease baseline"
    );
    drop(conn);

    let gh_calls = std::fs::read_to_string(case.gh_shim.path().join("state/calls")).unwrap();
    assert!(gh_calls.lines().any(|line| line.starts_with("pr view 10")));
    assert!(
        !gh_calls.lines().any(|line| line.starts_with("pr create")
            || (line.starts_with("pr list") && line.contains("--head"))),
        "continuation publication must never enter initial-PR routing: {gh_calls}"
    );
}

#[test]
fn dropping_serve_handle_releases_sticky_codex_runner() {
    let mut case = Case::start_continue("codex", "gpt-5.6-terra", 10);
    case.handle.wait_for("spawning agent");
    case.handle.wait_for("turn");

    let runner_pid = wait_until("sticky Codex runner PID", Duration::from_secs(5), || {
        let log = std::fs::read_to_string(&case.runner_log).unwrap();
        match log.lines().find_map(|line| {
            line.strip_prefix("hold-pid|")
                .and_then(|pid| pid.parse::<libc::pid_t>().ok())
        }) {
            Some(pid) => WaitState::Ready(pid),
            None => WaitState::Pending(log),
        }
    });
    assert_eq!(unsafe { libc::kill(runner_pid, 0) }, 0);

    // Implicit fixture teardown hard-kills only the daemon. Closing the
    // fixture-owned hold pipe must also let the provider process exit.
    drop(case);
    wait_until(
        "sticky Codex runner cleanup after ServeHandle drop",
        Duration::from_secs(5),
        || {
            if unsafe { libc::kill(runner_pid, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                WaitState::Ready(())
            } else {
                WaitState::Pending(format!("runner PID {runner_pid} is still alive"))
            }
        },
    );
}

#[test]
fn restart_recovery_retains_journal_pr_when_worker_omits_pr() {
    let mut case = Case::start_continue("codex", "gpt-5.6-terra", 10);
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");

    let (worktree, baseline_sha) = {
        let conn = case.db();
        let (worktree, journal_pr): (String, Option<i64>) = conn
            .query_row(
                "SELECT worktree, pr FROM journal WHERE agent=?1",
                [&worker],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(journal_pr, Some(10));
        let target = quorum_core::pr_targets::get(&conn, 1, 10).unwrap().unwrap();
        (worktree, target.head_sha)
    };
    assert!(Command::new("git")
        .args([
            "-C",
            &worktree,
            "commit",
            "--allow-empty",
            "-m",
            "late continuation work",
        ])
        .status()
        .unwrap()
        .success());
    let worker_sha = String::from_utf8(
        Command::new("git")
            .args(["-C", &worktree, "rev-parse", "HEAD"])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert_ne!(worker_sha, baseline_sha);

    // Crash before the worker signal is observed. Startup must recover the
    // exact journal identity even though the durable mailbox row has no PR.
    case.handle.crash_mut();
    case.done(&worker, &[]);
    case.restart_after_stop("codex", "gpt-5.6-terra", None);
    case.handle.wait_for("startup worker recovery: folded");

    let published_sha = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                &case._repo.path().to_string_lossy(),
                "rev-parse",
                "refs/heads/continue-pr-10",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert_eq!(published_sha, worker_sha);

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "in-review");
    assert_eq!(quorum_core::tasks::extract_pr_number(&task.refs), Some(10));
}

#[test]
fn continuation_publication_rejects_a_head_moved_after_spawn() {
    let mut case = Case::start_continue("codex", "gpt-5.6-terra", 10);
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");

    let (worktree, baseline_sha) = {
        let conn = case.db();
        let worktree: String = conn
            .query_row(
                "SELECT worktree FROM journal WHERE agent=?1",
                [&worker],
                |row| row.get(0),
            )
            .unwrap();
        let target = quorum_core::pr_targets::get(&conn, 1, 10).unwrap().unwrap();
        (worktree, target.head_sha)
    };
    assert!(Command::new("git")
        .args([
            "-C",
            &worktree,
            "commit",
            "--allow-empty",
            "-m",
            "continuation work",
        ])
        .status()
        .unwrap()
        .success());

    let tree = format!("{baseline_sha}^{{tree}}");
    let moved_sha = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                &case._repo.path().to_string_lossy(),
                "commit-tree",
                &tree,
                "-p",
                &baseline_sha,
                "-m",
                "racing writer",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert!(!moved_sha.is_empty());
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "update-ref",
            "refs/heads/continue-pr-10",
            &moved_sha,
            &baseline_sha,
        ])
        .status()
        .unwrap()
        .success());

    case.done(&worker, &[]);
    case.handle.wait_for("outside publication lease");
    case.handle.wait_for("PARKED: task #1");

    let remote_sha = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                &case._repo.path().to_string_lossy(),
                "rev-parse",
                "refs/heads/continue-pr-10",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert_eq!(remote_sha, moved_sha, "moved PR head must be preserved");

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["daemon_parked"], true);
    drop(conn);

    let gh_calls = std::fs::read_to_string(case.gh_shim.path().join("state/calls")).unwrap();
    assert!(
        !gh_calls.lines().any(|line| line.starts_with("pr create")
            || (line.starts_with("pr list") && line.contains("--head"))),
        "a lease failure must not fall back to initial publication: {gh_calls}"
    );
}

fn assert_continuation_publication_rejects_live_target_change(
    metadata: &str,
    value: &str,
    expected_error: &str,
) {
    let mut case = Case::start_continue("codex", "gpt-5.6-terra", 10);
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");

    let (worktree, baseline_sha) = {
        let conn = case.db();
        let worktree: String = conn
            .query_row(
                "SELECT worktree FROM journal WHERE agent=?1",
                [&worker],
                |row| row.get(0),
            )
            .unwrap();
        let target = quorum_core::pr_targets::get(&conn, 1, 10).unwrap().unwrap();
        (worktree, target.head_sha)
    };
    assert!(Command::new("git")
        .args([
            "-C",
            &worktree,
            "commit",
            "--allow-empty",
            "-m",
            "continuation work",
        ])
        .status()
        .unwrap()
        .success());

    std::fs::write(
        case.gh_shim.path().join(format!("state/{metadata}-10")),
        value,
    )
    .unwrap();
    case.done(&worker, &[]);
    case.handle.wait_for(expected_error);
    case.handle.wait_for("PARKED: task #1");

    let remote_sha = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                &case._repo.path().to_string_lossy(),
                "rev-parse",
                "refs/heads/continue-pr-10",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_string();
    assert_eq!(
        remote_sha, baseline_sha,
        "live PR metadata drift must prevent any remote update"
    );

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["daemon_parked"], true);
}

#[test]
fn continuation_publication_rejects_same_sha_base_retarget() {
    assert_continuation_publication_rejects_live_target_change("base", "develop", "targets base");
}

#[test]
fn continuation_publication_rejects_same_sha_closed_pr() {
    assert_continuation_publication_rejects_live_target_change("state", "CLOSED", "is not open");
}

#[test]
fn configurable_chatgpt_only_lifecycle_persists_role_models_and_efforts() {
    let runs = Case::start_with_role_config(
        "claude",
        "claude-opus-4-6",
        None,
        Some(CHATGPT_ONLY_ROLE_CONFIG),
    )
    .finish();

    assert_eq!(
        run_routes_with_effort(&runs),
        [
            ("worker", None, "gpt-5.6-terra", "high", "codex"),
            ("reviewer", None, "gpt-5.6-terra", "high", "codex"),
            ("reviewer", Some("r2"), "gpt-5.6-terra", "high", "codex"),
        ],
        "role-specific configuration must override legacy global Claude defaults"
    );
}

#[test]
fn configurable_chatgpt_only_ignores_legacy_claude_task_tier() {
    let runs = Case::start_with_role_config(
        "claude",
        "claude-opus-4-6",
        Some(r#"["tier:opus-46"]"#),
        Some(CHATGPT_ONLY_ROLE_CONFIG),
    )
    .finish();

    assert_eq!(
        run_routes_with_effort(&runs),
        [
            ("worker", None, "gpt-5.6-terra", "high", "codex"),
            ("reviewer", None, "gpt-5.6-terra", "high", "codex"),
            ("reviewer", Some("r2"), "gpt-5.6-terra", "high", "codex"),
        ],
        "legacy task routing labels must not override classifier-owned routing"
    );
}

#[test]
fn production_lifecycle_routes_claude_default_all_codex_and_mixed() {
    let claude = Case::start("claude", "claude-opus-4-6", None).finish();
    assert_eq!(
        run_routes(&claude),
        [
            ("worker", None, "claude-opus-4-6", "claude"),
            ("reviewer", None, "claude-opus-4-7", "claude"),
            ("reviewer", Some("r2"), "claude-opus-4-7", "claude"),
        ]
    );

    let codex = Case::start("codex", "o3", None).finish();
    assert_eq!(
        run_routes(&codex),
        [
            ("worker", None, "gpt-5.6-terra", "codex"),
            ("reviewer", None, "gpt-5.6-terra", "codex"),
            ("reviewer", Some("r2"), "gpt-5.6-terra", "codex"),
        ]
    );

    let mixed = Case::start(
        "claude",
        "claude-opus-4-6",
        Some(r#"["tier:terra","effort:high"]"#),
    )
    .finish();
    assert_eq!(
        run_routes(&mixed),
        [
            ("worker", None, "claude-opus-4-6", "claude"),
            ("reviewer", None, "claude-opus-4-7", "claude"),
            ("reviewer", Some("r2"), "claude-opus-4-7", "claude"),
        ]
    );
}

#[test]
fn changes_reuses_codex_thread_then_runs_fresh_reviews_and_merges() {
    let mut case = Case::start_with_role_config(
        "codex",
        "gpt-5.6-terra",
        None,
        Some(CHATGPT_ONLY_ROLE_CONFIG),
    );
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");
    case.done(&worker, &["--pr", "1"]);

    case.handle.wait_for("spawning reviewer ");
    let r1 = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for("result");
    case.done(
        &r1,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "fix it",
        ],
    );
    case.handle.wait_for("rework #1 started");
    case.handle.wait_for("turn");
    let expected_thread = format!("thread-{worker}");
    let log = std::fs::read_to_string(&case.runner_log).unwrap();
    let expected_resume = format!("{worker}|exec resume {expected_thread} --json ");
    assert!(
        log.lines().any(|line| line.starts_with(&expected_resume)),
        "Codex remediation must resume the original worker's exact provider thread \
         ({expected_resume:?}): {log}"
    );
    case.done(&worker, &["--pr", "1"]);
    case.approve_current_reviewer("spawning reviewer ");
    case.approve_current_reviewer("R2: pre-merge reviewer ");
    case.handle.wait_for("merged — firing MergeSucceeded");
    case.handle.wait_for("lifecycle: task #1 -> done");

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "done");
    let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
    assert_eq!(
        run_routes(&runs),
        [
            ("worker", None, "gpt-5.6-terra", "codex"),
            ("reviewer", None, "gpt-5.6-terra", "codex"),
            ("reviewer", None, "gpt-5.6-terra", "codex"),
            ("reviewer", Some("r2"), "gpt-5.6-terra", "codex"),
        ]
    );
    let workers: Vec<_> = runs.iter().filter(|run| run.role == "worker").collect();
    assert_eq!(
        workers.len(),
        1,
        "Codex continuation stays within the original durable worker run"
    );
    assert!(workers.iter().all(|run| run.agent == worker
        && run.model == "gpt-5.6-terra"
        && run.provider.as_deref() == Some("codex")));
    assert_eq!(
        runs.iter()
            .filter(|run| run.role == "reviewer" && run.sub_role.is_none())
            .count(),
        2,
        "changes must require a fresh R1"
    );
    drop(conn);
    case.handle.stop();
}

#[test]
fn workerless_review_only_changes_start_fresh_codex_remediation_on_verified_pr() {
    let mut case = Case::start_review_only("codex", "gpt-5.6-terra");

    case.handle.wait_for("spawning reviewer ");
    let reviewer = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for("result");
    case.done(
        &reviewer,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "fix the review finding",
        ],
    );

    case.handle.wait_for("spawning remediation worker ");
    let remediation = case.handle.agent_after("spawning remediation worker ");
    case.handle
        .wait_for(&format!("worker {remediation} result"));

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "rework");
    assert!(task.review_only);
    assert_eq!(task.rework_round, 1);
    assert_eq!(task.assignee.as_deref(), Some(remediation.as_str()));
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(
        refs["runner_continuation"]["id"].as_str(),
        Some(format!("thread-{remediation}").as_str()),
        "the fresh remediation thread must be durable before any later continuation"
    );
    let active_claims: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM claims WHERE target='task:1' AND active=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        active_claims, 0,
        "a completed remediation turn must not leave a dangling active claim"
    );
    let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
    assert_eq!(
        run_routes(&runs),
        [
            ("reviewer", None, "gpt-5.6-terra", "codex"),
            ("worker", None, "gpt-5.6-terra", "codex"),
        ],
        "review-only remediation gets one new worker run without inventing an original one"
    );
    let target: (String, String) = conn
        .query_row(
            "SELECT head_ref, head_sha FROM pr_targets WHERE task_id=1 AND pr_number=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(target.0, "review-pr-1");
    assert!(!target.1.is_empty());
    drop(conn);

    let log = std::fs::read_to_string(&case.runner_log).unwrap();
    let fresh = format!("{remediation}|exec --json --model gpt-5.6-terra ");
    assert!(
        log.lines().any(|line| line.starts_with(&fresh)),
        "workerless review-only remediation must start a fresh Codex turn: {log}"
    );
    assert!(
        !log.lines()
            .any(|line| line.starts_with(&format!("{remediation}|exec resume "))),
        "workerless review-only remediation must not fabricate a continuation: {log}"
    );

    case.done(&remediation, &["--pr", "1"]);
    case.handle.wait_for("lifecycle: task #1 -> in-review");
    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "in-review");
    assert_eq!(task.rework_round, 1);
    drop(conn);
    case.handle.stop();
}

#[test]
fn remediation_provision_failure_parks_review_only_rework_without_reviewer_loop() {
    let mut case = Case::start_review_only("codex", "gpt-5.6-terra");
    case.handle.wait_for("spawning reviewer ");
    let reviewer = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for("result");

    // Remove the executable only after the initial reviewer is running. The
    // next process creation is the remediation worker, so this forces a
    // deterministic spawn-time provisioning failure rather than an agent
    // runtime failure.
    std::fs::remove_file(case.home.path().join("dual-runner.sh")).unwrap();
    case.done(
        &reviewer,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "still blocked",
        ],
    );
    case.handle.wait_for("PARKED: task #1");
    case.wait_for_completed_tick();

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    assert_eq!(task.rework_round, 1, "the failed spawn consumes one round");
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["daemon_parked"], true);
    assert_eq!(refs["daemon_resume_status"], "rework");
    assert_eq!(refs["remediation_feedback"], "still blocked");
    let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
    assert_eq!(
        runs.iter().filter(|run| run.role == "reviewer").count(),
        1,
        "the task must not provision a replacement reviewer after a failed remediation spawn"
    );
    assert!(
        runs.iter().all(|run| run.role != "worker"),
        "a spawn-time failure must not create a worker run"
    );
    let active_claims: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM claims WHERE target='task:1' AND active=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_claims, 0, "parking releases the remediation claim");
    drop(conn);

    // An explicit retry must be schedulable through the review-only
    // remediation path, not generic worker provisioning (which would create
    // a daemon branch rather than reuse the adopted PR branch).
    write_dual_protocol_runner(case.home.path());
    // The failed remediation attempt only owned its namespaced local branch —
    // the adopted PR branch must survive untouched.
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "rev-parse",
            "--verify",
            "refs/heads/review-pr-1",
        ])
        .status()
        .unwrap()
        .success());
    case.retry_parked();
    case.handle
        .wait_for("durable remediation retry: provisioning task #1");
    case.handle.wait_for("spawning remediation worker ");
    let remediation = case.handle.agent_after("spawning remediation worker ");
    case.handle
        .wait_for(&format!("worker {remediation} result"));
    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "rework");
    assert_eq!(task.assignee.as_deref(), Some(remediation.as_str()));
    let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
    assert_eq!(
        runs.iter().filter(|run| run.role == "worker").count(),
        1,
        "one explicit retry must create exactly one remediation worker"
    );
    drop(conn);
    case.done(&remediation, &["--pr", "1"]);
    case.handle.wait_for("lifecycle: task #1 -> in-review");
    case.handle.stop();
}

/// D5b: a remediation worker that spawns fine but dies at runtime WITHOUT
/// pushing must park the task — never hand the unchanged PR head back to a
/// fresh reviewer (whose changes verdict would burn a rework round with zero
/// remediation applied). The terminal park is immediately owner-gated; an
/// explicit `task-retry` resumes the remediation flow with persisted feedback.
#[test]
fn remediation_runtime_death_parks_review_only_rework_without_reviewer_loop() {
    let mut case = Case::start_review_only("claude", "claude-opus-4-6");
    case.handle.wait_for("spawning reviewer ");
    let reviewer = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for("result");

    // Swap in a runner that accepts the initial turn, then dies without any
    // protocol output. The next spawn is the remediation worker, so this
    // forces a deterministic RUNTIME death (post-spawn), not a provisioning
    // failure — the case the provision-failure park cannot cover.
    std::fs::write(
        case.home.path().join("dual-runner.sh"),
        r#"#!/bin/sh
printf '%s|%s\n' "${QUORUM_AGENT:-none}" "$*" >> "$RUNNER_LOG"
IFS= read -r line
exit 1
"#,
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            case.home.path().join("dual-runner.sh"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
    }

    case.done(
        &reviewer,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "fix the blocker",
        ],
    );
    case.handle.wait_for("spawning remediation worker ");
    let remediation = case.handle.agent_after("spawning remediation worker ");
    case.handle.wait_for("lifecycle: task #1 -> failed");

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    assert_eq!(
        task.rework_round, 1,
        "runtime death must not consume a rework round"
    );
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["daemon_parked"], true);
    assert_eq!(refs["daemon_resume_status"], "rework");
    assert!(
        refs.get("daemon_parked_head_check").is_none(),
        "new terminal parks must not create automatic head-check authority"
    );
    assert!(
        refs.get("daemon_rework_retry_requested").is_none(),
        "a genuine crash park must stay owner-gated — no auto-retry flag"
    );
    assert_eq!(
        refs["remediation_feedback"], "fix the blocker",
        "feedback must be durable at spawn so retry can rebuild the turn"
    );
    let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
    assert_eq!(
        runs.iter().filter(|run| run.role == "reviewer").count(),
        1,
        "the task must not provision a replacement reviewer after a runtime death"
    );
    let active_claims: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM claims WHERE target='task:1' AND active=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_claims, 0, "parking releases the remediation claim");
    drop(conn);

    // Explicit retry resumes the remediation flow (not generic provisioning).
    write_dual_protocol_runner(case.home.path());
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "branch",
            "-f",
            "review-pr-1",
        ])
        .status()
        .unwrap()
        .success());
    case.retry_parked();
    case.handle
        .wait_for("durable remediation retry: provisioning task #1");
    case.handle.wait_for("spawning remediation worker ");
    let retry_worker = case.handle.agent_after("spawning remediation worker ");
    assert_ne!(
        retry_worker, remediation,
        "retry must provision a fresh remediation worker"
    );
    case.handle
        .wait_for(&format!("worker {retry_worker} result"));
    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "rework");
    assert_eq!(
        task.rework_round, 1,
        "retry must not consume a rework round"
    );
    drop(conn);
    case.done(&retry_worker, &["--pr", "1"]);
    case.handle.wait_for("lifecycle: task #1 -> in-review");
    case.handle.stop();
}

/// #270: even if the PR head moves after a remediation worker dies, the
/// terminal task remains owner-gated. A stale pre-terminal head-check marker
/// must never revive it; explicit retry returns to remediation exactly once.
#[test]
fn remediation_death_after_push_remains_owner_gated() {
    let mut case = Case::start_review_only("claude", "claude-opus-4-6");
    case.handle.wait_for("spawning reviewer ");
    let reviewer = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for("result");

    // Dying runner: accepts the initial turn, then exits without protocol
    // output — a runtime death after spawn.
    std::fs::write(
        case.home.path().join("dual-runner.sh"),
        r#"#!/bin/sh
printf '%s|%s\n' "${QUORUM_AGENT:-none}" "$*" >> "$RUNNER_LOG"
IFS= read -r line
exit 1
"#,
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            case.home.path().join("dual-runner.sh"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
    }

    case.done(
        &reviewer,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "fix the blocker",
        ],
    );
    case.handle.wait_for("spawning remediation worker ");
    case.handle.wait_for("lifecycle: task #1 -> failed");

    // Simulate the dead worker's push landing after the terminal transition.
    write_dual_protocol_runner(case.home.path());
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "checkout",
            "review-pr-1",
        ])
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "commit",
            "--allow-empty",
            "-m",
            "remediation push",
        ])
        .status()
        .unwrap()
        .success());

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(
        task.status, "failed",
        "terminal task must not revive merely because its PR head moved"
    );
    assert_eq!(
        task.rework_round, 1,
        "the terminal park must not consume a rework round"
    );
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["daemon_parked"], true);
    assert_eq!(refs["daemon_resume_status"], "rework");
    assert!(refs.get("daemon_parked_head_check").is_none());
    assert!(refs.get("daemon_rework_retry_requested").is_none());
    let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
    assert_eq!(
        runs.iter().filter(|run| run.role == "reviewer").count(),
        1,
        "head movement must not provision a replacement reviewer"
    );
    drop(conn);

    // Explicit owner retry returns to the remediation path on the moved head.
    let new_head = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                &case._repo.path().to_string_lossy(),
                "rev-parse",
                "review-pr-1",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "checkout",
            "main"
        ])
        .status()
        .unwrap()
        .success());
    let conn = case.db();
    conn.execute(
        "UPDATE pr_targets SET head_sha=?1 WHERE task_id=1 AND pr_number=1",
        [new_head.trim()],
    )
    .unwrap();
    drop(conn);
    case.retry_parked();
    case.handle
        .wait_for("durable remediation retry: provisioning task #1");
    case.handle.wait_for("spawning remediation worker ");
    let retry_worker = case.handle.agent_after("spawning remediation worker ");
    case.handle
        .wait_for(&format!("worker {retry_worker} result"));
    let task = quorum_core::tasks::get(&case.db(), 1).unwrap().unwrap();
    assert_eq!(task.status, "rework");
    assert_eq!(task.rework_round, 1);
    case.handle.stop();
}

/// #270: drain teardown parks remediation work without leaving durable
/// automatic-retry authority. Restart keeps the terminal row owner-gated;
/// explicit `task-retry` resumes it without spending recovery budget.
#[test]
fn drain_park_of_remediation_stays_owner_gated_on_restart() {
    let mut case = Case::start_review_only("claude", "claude-opus-4-6");
    case.handle.wait_for("spawning reviewer ");
    let reviewer = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for("result");
    case.done(
        &reviewer,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "fix the blocker",
        ],
    );
    case.handle.wait_for("spawning remediation worker ");
    let remediation = case.handle.agent_after("spawning remediation worker ");
    case.handle
        .wait_for(&format!("worker {remediation} result"));

    // Drain: the idle remediation worker is torn down with AgentFailed
    // ("daemon draining") and parked for an explicit owner retry.
    case.handle.stop_mut();
    {
        let conn = case.db();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(task.status, "failed", "drain must park, not bounce");
        assert_eq!(task.rework_round, 1, "drain must not consume a round");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["daemon_parked"], true);
        assert!(
            refs.get("daemon_rework_retry_requested").is_none(),
            "terminal park must not carry automatic-retry authority"
        );
        assert_eq!(
            task.recovery_attempts, 0,
            "owner-gated park must not spend recovery budget"
        );
    }

    // The PR branch was torn down with the dead slot; recreate it so the
    // respawned remediation worker can provision (same as the manual-retry
    // tests).
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "branch",
            "-f",
            "review-pr-1",
        ])
        .status()
        .unwrap()
        .success());

    // Restart must leave the terminal task inert and provision nothing.
    let runner_log_before_restart = std::fs::read_to_string(&case.runner_log).unwrap();
    case.restart_after_stop("claude", "claude-opus-4-6", None);
    case.handle.wait_for("recovery: complete");
    case.wait_for_completed_tick();
    let runner_log_after_restart = std::fs::read_to_string(&case.runner_log).unwrap();
    assert_eq!(
        runner_log_after_restart, runner_log_before_restart,
        "restart must not provision a worker for a terminal park"
    );

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    assert_eq!(task.rework_round, 1);
    assert_eq!(task.recovery_attempts, 0);
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["daemon_parked"], true);
    assert!(refs.get("daemon_rework_retry_requested").is_none());
    drop(conn);

    // Explicit owner action resumes the preserved remediation request once.
    case.retry_parked();
    case.handle
        .wait_for("durable remediation retry: provisioning task #1");
    case.handle.wait_for("spawning remediation worker ");
    let retry_worker = case.handle.agent_after("spawning remediation worker ");
    case.handle
        .wait_for(&format!("worker {retry_worker} result"));

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "rework");
    assert_eq!(task.rework_round, 1, "respawn must not consume a round");
    assert_eq!(
        task.recovery_attempts, 0,
        "explicit retry must not alter the recovery budget"
    );
    let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
    assert_eq!(
        runs.iter().filter(|run| run.role == "reviewer").count(),
        1,
        "no replacement reviewer across drain + restart"
    );
    drop(conn);
    case.done(&retry_worker, &["--pr", "1"]);
    case.handle.wait_for("lifecycle: task #1 -> in-review");
    case.handle.stop();
}

#[test]
fn remediation_retry_for_implementation_task_preserves_feedback_and_codex_thread() {
    let mut case = Case::start("codex", "gpt-5.6-terra", None);
    case.handle.wait_for("spawning agent ");
    let original_worker = case.handle.agent_after("spawning agent ");
    case.handle
        .wait_for(&format!("worker {original_worker} result"));
    let original_thread = format!("thread-{original_worker}");
    case.done(&original_worker, &["--pr", "1"]);

    case.handle.wait_for("spawning reviewer ");
    let reviewer = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for(&format!("reviewer {reviewer} result"));

    // A daemon restart drops the submitted worker slot while recovering the
    // durable reviewer. The subsequent changes verdict must therefore create
    // a replacement remediation worker rather than feed a live worker.
    case.handle.stop_mut();
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "branch",
            "-f",
            "daemon/agent0-t1",
        ])
        .status()
        .unwrap()
        .success());
    case.restart_after_stop("codex", "gpt-5.6-terra", None);
    case.handle.wait_for(&format!(
        "recovering R1 reviewer {reviewer} with persisted provider codex model gpt-5.6-terra"
    ));
    case.handle.wait_for(&format!("reviewer {reviewer} result"));

    // Force the replacement remediation process spawn to fail after the
    // original worker and its exact Codex thread have been persisted.
    std::fs::remove_file(case.home.path().join("dual-runner.sh")).unwrap();
    case.done(
        &reviewer,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "preserve this exact blocker",
        ],
    );
    case.handle.wait_for("PARKED: task #1");

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["remediation_feedback"], "preserve this exact blocker");
    assert_eq!(refs["runner_continuation"]["provider"], "codex");
    assert_eq!(refs["runner_continuation"]["id"], original_thread);
    drop(conn);

    write_dual_protocol_runner(case.home.path());
    assert!(Command::new("git")
        .args([
            "-C",
            &case._repo.path().to_string_lossy(),
            "branch",
            "-f",
            "daemon/agent0-t1",
        ])
        .status()
        .unwrap()
        .success());
    case.retry_parked();
    case.handle
        .wait_for("durable remediation retry: provisioning task #1");
    case.handle.wait_for("spawning remediation worker ");
    let replacement = case.handle.agent_after("spawning remediation worker ");
    case.handle
        .wait_for(&format!("worker {replacement} result"));

    let log = std::fs::read_to_string(&case.runner_log).unwrap();
    let resume_prefix = format!("{replacement}|exec resume {original_thread} --json ");
    let resume = log
        .lines()
        .find(|line| line.starts_with(&resume_prefix))
        .unwrap_or_else(|| panic!("replacement must resume the original thread: {log}"));
    assert!(
        log.contains("preserve this exact blocker"),
        "replacement prompt must contain the accepted reviewer feedback: {log}"
    );
    assert!(
        !log.lines()
            .any(|line| line.starts_with(&format!("{replacement}|exec --json "))),
        "implementation remediation must not start a fresh Codex thread: {log}"
    );
    assert!(
        !resume.contains(&format!("You are agent {replacement}.")),
        "replacement must not receive the initial-task prompt: {resume}"
    );

    case.done(&replacement, &["--pr", "1"]);
    case.handle.wait_for("lifecycle: task #1 -> in-review");
    case.handle.stop();
}

#[test]
fn restart_resumes_codex_reviewer_with_persisted_identity_model_and_thread() {
    let mut case = Case::start("codex", "gpt-5.6-terra", None);
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");
    case.done(&worker, &["--pr", "1"]);
    case.handle.wait_for("spawning reviewer ");
    let reviewer = case.handle.agent_after("spawning reviewer ");
    assert_ne!(
        reviewer, worker,
        "fresh reviewer provisioning must exclude the durable worker identity"
    );
    case.handle.wait_for(&format!("reviewer {reviewer} result"));

    let expected_thread = format!("thread-{reviewer}");
    let task = quorum_core::tasks::get(&case.db(), 1).unwrap().unwrap();
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(
        refs["runner_reviewer_r1_continuation"]["id"].as_str(),
        Some(expected_thread.as_str())
    );
    let head_sha = String::from_utf8(
        Command::new("git")
            .args([
                "-C",
                &case._repo.path().to_string_lossy(),
                "rev-parse",
                "HEAD",
            ])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    let mut conn = case.db();
    conn.execute("UPDATE tasks SET author='' WHERE id=1", [])
        .unwrap();
    quorum_core::pr_targets::upsert(&mut conn, 1, 1, "main", head_sha.trim(), false).unwrap();
    drop(conn);

    // Change daemon defaults across restart. Orphan provisioning must recover
    // the interrupted reviewer's durable identity instead of reclassifying.
    case.restart("claude", "claude-opus-4-6");
    case.handle.wait_for(&format!(
        "recovering R1 reviewer {reviewer} with persisted provider codex model gpt-5.6-terra"
    ));
    case.handle.wait_for(&format!("reviewer {reviewer} result"));

    let log = std::fs::read_to_string(&case.runner_log).unwrap();
    let expected_resume = format!("{reviewer}|exec resume {expected_thread} --json ");
    assert!(
        log.lines().any(|line| {
            line.starts_with(&expected_resume) && line.contains("--model gpt-5.6-terra")
        }),
        "reviewer restart must resume the exact persisted Codex identity: {log}"
    );
    let runs = quorum_core::agent_runs::runs_for_task(&case.db(), 1).unwrap();
    let recovered = runs.last().unwrap();
    assert_eq!(recovered.agent, reviewer);
    assert_ne!(
        recovered.agent, worker,
        "restart recovery must preserve the reviewer identity without weakening task exclusions"
    );
    assert_eq!(recovered.model, "gpt-5.6-terra");
    assert_eq!(recovered.provider.as_deref(), Some("codex"));
    case.handle.stop();
}

#[test]
fn strict_codex_restart_does_not_resume_interrupted_claude_reviewer() {
    let mut case = Case::start("claude", "claude-opus-4-6", None);
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");
    case.done(&worker, &["--pr", "1"]);
    case.handle.wait_for("spawning reviewer ");
    case.handle.wait_for("result");

    let runner_log_before_restart = std::fs::read_to_string(&case.runner_log).unwrap();
    case.restart_with_role_config("codex", "gpt-5.6-terra", Some(CHATGPT_ONLY_ROLE_CONFIG));
    case.handle.wait_for("recovery: complete");
    case.wait_for_completed_tick();

    let runner_log_after_restart = std::fs::read_to_string(&case.runner_log).unwrap();
    assert_eq!(
        runner_log_after_restart, runner_log_before_restart,
        "strict Codex recovery must not invoke the persisted Claude reviewer"
    );
    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert!(
        task.status == "in-review" || task.status == "failed",
        "incompatible reviewer recovery must stay safely reviewable or park: {task:?}"
    );
    let claims: i64 = conn
        .query_row(
            "SELECT count(*) FROM claims WHERE active=1 AND target='task:1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        claims, 0,
        "incompatible reviewer recovery must not retain a claim"
    );
    drop(conn);
    case.handle.stop_mut();
}

#[test]
fn codex_remediation_resumes_with_persisted_worker_effort() {
    let mut case = Case::start_with_role_config(
        "codex",
        "gpt-5.6-terra",
        None,
        Some(CHATGPT_ONLY_ROLE_CONFIG),
    );
    case.handle.wait_for("spawning agent");
    let worker = case.handle.agent_after("spawning agent ");
    case.handle.wait_for("turn");
    case.done(&worker, &["--pr", "1"]);
    case.handle.wait_for("spawning reviewer ");
    let reviewer = case.handle.agent_after("spawning reviewer ");
    case.handle.wait_for("result");
    case.done(
        &reviewer,
        &[
            "--pr",
            "1",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "preserve effort",
        ],
    );
    case.handle.wait_for("rework #1 started");
    case.handle.wait_for("turn");

    let expected_thread = format!("thread-{worker}");
    let log = std::fs::read_to_string(&case.runner_log).unwrap();
    let resume = log
        .lines()
        .find(|line| line.starts_with(&format!("{worker}|exec resume {expected_thread} ")))
        .unwrap_or_else(|| panic!("missing exact Codex remediation resume: {log}"));
    assert!(
        resume.contains("model_reasoning_effort=high"),
        "remediation must reuse persisted worker effort, not review effort: {resume}"
    );
    case.handle.stop_mut();
}
