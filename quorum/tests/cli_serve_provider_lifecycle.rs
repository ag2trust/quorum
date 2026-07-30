//! Deterministic production-path coverage for mixed provider lifecycle routing.

use std::io::{BufRead, BufReader, Write};
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
  sleep 30
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

struct ServeHandle {
    child: std::process::Child,
    rx: mpsc::Receiver<String>,
    lines: Vec<String>,
    _sentinel: tempfile::TempDir,
}

impl Drop for ServeHandle {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}

impl ServeHandle {
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
    }

    fn stop_mut(&mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGINT) };
        assert!(self.child.wait().unwrap().success());
    }
}

struct Case {
    home: tempfile::TempDir,
    _repo: tempfile::TempDir,
    _worktrees: tempfile::TempDir,
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
        Self::start_with_review_pr(default_provider, model, labels, role_config, None)
    }

    fn start_review_only(default_provider: &str, model: &str) -> Self {
        Self::start_with_review_pr(default_provider, model, None, None, Some(1))
    }

    fn start_with_review_pr(
        default_provider: &str,
        model: &str,
        labels: Option<&str>,
        role_config: Option<&str>,
        review_pr: Option<i64>,
    ) -> Self {
        let home = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        let worktrees = tempfile::tempdir().unwrap();
        init_git_repo(repo.path());
        let names = write_names(home.path());
        let runner = write_dual_protocol_runner(home.path());
        let runner_log = home.path().join("runner.log");
        std::fs::write(&runner_log, "").unwrap();
        let config_path = role_config.map(|contents| {
            let path = home.path().join("serve.toml");
            std::fs::write(&path, contents).unwrap();
            path
        });

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
        }
        assert!(create.status().unwrap().success());
        let db_path = home.path().join("repos/test__repo/quorum.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        quorum_core::classify::store_classifications(
            &mut conn,
            &[quorum_core::classify::TaskClassification {
                task_id: 1,
                cx_est: 2,
                cx_flags: Vec::new(),
                cx_tags: Vec::new(),
                cx_dup_of: Vec::new(),
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
        if let Some(pr) = review_pr {
            let head_ref = format!("review-pr-{pr}");
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
            let mut conn =
                quorum_core::db::open(&home.path().join("repos/test__repo/quorum.db")).unwrap();
            quorum_core::pr_targets::upsert(&mut conn, 1, pr, &head_ref, head_sha.trim(), false)
                .unwrap();
        }

        let sentinel = tempfile::tempdir().unwrap();
        let codex_only = role_config.is_some_and(|config| config.contains(r#"provider = "codex""#));
        let cli_provider = if codex_only {
            "codex"
        } else {
            default_provider
        };
        let cli_model = if codex_only { "gpt-5.6-terra" } else { model };
        let mut serve = Command::new(cargo_bin("quorum"));
        serve
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("RUNNER_LOG", &runner_log)
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
                "--agent",
                cli_provider,
                "--model",
                cli_model,
                "--agent-bin",
                &runner.to_string_lossy(),
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
        if let Some(path) = config_path {
            serve.args(["--config", &path.to_string_lossy()]);
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
            runner_log,
            handle: ServeHandle {
                child,
                rx,
                lines: Vec::new(),
                _sentinel: sentinel,
            },
        }
    }

    fn db(&self) -> rusqlite::Connection {
        quorum_core::db::open(&self.home.path().join("repos/test__repo/quorum.db")).unwrap()
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
        let config_path = role_config.map(|contents| {
            let path = self.home.path().join("restart-serve.toml");
            std::fs::write(&path, contents).unwrap();
            path
        });
        let sentinel = tempfile::tempdir().unwrap();
        let cli_provider = if role_config.is_some() {
            "codex"
        } else {
            default_provider
        };
        let cli_model = if role_config.is_some() {
            "gpt-5.6-terra"
        } else {
            model
        };
        let mut serve = Command::new(cargo_bin("quorum"));
        serve
            .env("QUORUM_HOME", self.home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("RUNNER_LOG", &self.runner_log)
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
                "--agent",
                cli_provider,
                "--model",
                cli_model,
                "--agent-bin",
                &runner.to_string_lossy(),
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
        if let Some(path) = config_path {
            serve.args(["--config", &path.to_string_lossy()]);
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
        command
            .env("QUORUM_HOME", self.home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("QUORUM_RUN_ID", run_id)
            .args(["done", "--agent", agent])
            .args(args);
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

const CHATGPT_ONLY_ROLE_CONFIG: &str = r#"
provider = "codex"
worker_model = "gpt-5.6-terra"
worker_effort = "medium"
review_model = "gpt-5.6-terra"
review_effort = "high"
classifier_model = "gpt-5.6-terra"
classifier_effort = "medium"
"#;

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
            ("reviewer", None, "o3", "codex"),
            ("reviewer", Some("r2"), "o3", "codex"),
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
        refs["codex_thread_id"].as_str(),
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
    std::thread::sleep(Duration::from_millis(500));

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
/// remediation applied). The park owes one PR-head check (settled as
/// "staying parked" here, since the head never moved), and an explicit
/// `task-retry` resumes the remediation flow with the persisted feedback.
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

    // The head is unchanged (executor answers with the repo HEAD, which still
    // equals the seeded spawn baseline), and a YOUNG unchanged park must stay
    // pending — a slow in-flight push could still land. Backdate the park past
    // the remediation lease TTL to let the check settle as "staying parked".
    {
        let conn = case.db();
        conn.execute(
            "UPDATE tasks SET updated_at = updated_at - 3700 WHERE id = 1",
            [],
        )
        .unwrap();
    }
    case.handle
        .wait_for("head check settled for task #1 — staying parked");

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
        "one-shot head check must be settled, not pending"
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

/// D5b pushed-then-died rescue: the remediation worker pushes its fix, then
/// dies before signaling `ReworkPushed`. The head check must observe the
/// moved PR head (executor = repo HEAD vs the seeded spawn baseline) and
/// resume the task straight to in-review — no manual retry, no rework round.
#[test]
fn remediation_death_after_push_resumes_to_in_review() {
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

    // Simulate the dead worker's push landing: advance the repo HEAD past the
    // seeded spawn baseline. Restore a healthy runner first so the reviewer
    // the resume triggers can actually run.
    write_dual_protocol_runner(case.home.path());
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

    // A moved head settles at ANY park age — no backdating needed.
    case.handle
        .wait_for("head check: task #1 head advanced before worker death — resumed to in-review");

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(
        task.status, "in-review",
        "pushed work must go to review, not sit parked behind task-retry"
    );
    assert_eq!(
        task.rework_round, 1,
        "the rescue must not consume a rework round"
    );
    assert!(task.reviewer.is_none(), "reviewer cleared for reattachment");
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert!(refs.get("daemon_parked").is_none(), "park markers cleared");
    assert!(refs.get("daemon_parked_head_check").is_none());

    // Production re-resolves the PR target from GitHub before reviewer
    // provisioning; the harness has no gh, so refresh the stored target to
    // the pushed head by hand (the head check deliberately never upserts —
    // it must preserve the spawn-time baseline until settled).
    let new_head = String::from_utf8(
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
    conn.execute(
        "UPDATE pr_targets SET head_sha=?1 WHERE task_id=1 AND pr_number=1",
        [new_head.trim()],
    )
    .unwrap();
    drop(conn);

    // Phase 5b picks the orphaned in-review task back up with a reviewer.
    case.handle.wait_for("spawning reviewer ");
    case.handle.stop();
}

/// A1: a park the daemon itself caused (drain teardown of a healthy
/// remediation worker) auto-retries on the next daemon run — the owner is
/// never asked to `task-retry` an event the daemon caused. Bounded by the
/// recovery budget spent at park time.
#[test]
fn drain_park_of_remediation_auto_retries_on_restart() {
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
    // ("daemon draining") → parked WITH the auto-retry flag.
    case.handle.stop_mut();
    {
        let conn = case.db();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(task.status, "failed", "drain must park, not bounce");
        assert_eq!(task.rework_round, 1, "drain must not consume a round");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["daemon_parked"], true);
        assert_eq!(
            refs["daemon_rework_retry_requested"], true,
            "daemon-caused park must carry the auto-retry flag"
        );
        assert_eq!(
            task.recovery_attempts, 1,
            "auto-retry park spends recovery budget"
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

    // Restart: the daemon owes the respawn — no owner task-retry involved.
    case.restart_after_stop("claude", "claude-opus-4-6", None);
    case.handle
        .wait_for("auto-retrying daemon-caused remediation park for task #1");
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
        task.recovery_attempts, 1,
        "daemon auto-retry must not refill the recovery budget"
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
    std::thread::sleep(Duration::from_millis(250));

    let conn = case.db();
    let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
    assert_eq!(task.status, "failed");
    let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
    assert_eq!(refs["remediation_feedback"], "preserve this exact blocker");
    assert_eq!(refs["codex_thread_id"], original_thread);
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
        refs["codex_reviewer_r1_thread_id"].as_str(),
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
    std::thread::sleep(Duration::from_millis(750));

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
