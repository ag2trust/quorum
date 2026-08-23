#![cfg(unix)]

//! Real-process tests for managed completion requests routed through the daemon.

use assert_cmd::Command;
use rusqlite::params;
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

type MailboxShape = (
    String,
    String,
    i64,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn quorum_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
}

fn endpoint_path(db: &Path) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    db.hash(&mut hasher);
    std::env::temp_dir()
        .join(format!("quorum-agent-{:016x}", hasher.finish()))
        .join("endpoint.sock")
}

fn init_git_repo(dir: &Path) {
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.email", "test@example.com"],
        vec!["config", "user.name", "Endpoint CLI Test"],
        vec!["commit", "--allow-empty", "-m", "initial"],
    ] {
        assert!(ProcessCommand::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .unwrap()
            .success());
    }
    assert!(ProcessCommand::new("git")
        .arg("-C")
        .arg(dir)
        .args(["remote", "add", "origin"])
        .arg(dir)
        .status()
        .unwrap()
        .success());
    assert!(ProcessCommand::new("git")
        .arg("-C")
        .arg(dir)
        .args(["fetch", "origin"])
        .status()
        .unwrap()
        .success());
}

fn write_config(path: &Path) {
    std::fs::write(
        path,
        r#"
[model_profiles.test]
runner = "claude"
model = "claude-opus-4-6"
effort = "high"
[routing.classifier]
test = 100
[routing.planner]
test = 100
[routing.collector]
test = 100
[routing.worker.1]
test = 100
[routing.worker.2]
test = 100
[routing.worker.3]
test = 100
[routing.worker.4]
test = 100
[routing.worker.5]
test = 100
[routing.reviewer.1]
test = 100
[routing.reviewer.2]
test = 100
[routing.reviewer.3]
test = 100
[routing.reviewer.4]
test = 100
[routing.reviewer.5]
test = 100
"#,
    )
    .unwrap();
}

struct ServeProcess {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl ServeProcess {
    fn start(home: &Path, repo: &Path, worktrees: &Path, names: &Path, config: &Path) -> Self {
        let mut child = ProcessCommand::new(quorum_bin())
            .env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .args(["serve", "--config"])
            .arg(config)
            .args(["--repo", "test/repo", "--cap", "1", "--repo-dir"])
            .arg(repo)
            .arg("--worktree-base")
            .arg(worktrees)
            .arg("--names-file")
            .arg(names)
            .arg("--agent-bin")
            .arg(assert_cmd::cargo::cargo_bin("fake-agent"))
            .args(["--merge-cmd", "true"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self { child, lines }
    }

    fn wait_for(&self, needle: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            match self.lines.recv_timeout(deadline - Instant::now()) {
                Ok(line) if line.contains(needle) => return,
                Ok(line) => seen.push(line),
                Err(error) => panic!("daemon did not log {needle:?}: {error}; output={seen:?}"),
            }
        }
        panic!("daemon did not log {needle:?}; output={seen:?}");
    }

    fn stop(mut self) {
        unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        if self.child.try_wait().unwrap().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}

impl Drop for ServeProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe { libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL) };
            let _ = self.child.wait();
        }
    }
}

fn install_fixtures(db: &Path) {
    let mut conn = quorum_core::db::open(db).unwrap();
    let tx = quorum_core::db::begin_immediate(&mut conn).unwrap();
    for (id, status, assignee, reviewer, refs) in [
        (100, "working", Some("Worker"), None, Some(r#"{"pr":42}"#)),
        (
            101,
            "in-review",
            None,
            Some("Reviewer"),
            Some(r#"{"pr":77}"#),
        ),
        (
            102,
            "in-review",
            None,
            Some("GraphReviewer"),
            Some(r#"{"pr":78}"#),
        ),
        (103, "working", Some("Reactor"), None, None),
        (104, "working", Some("Revoked"), None, None),
        (105, "working", Some("Ended"), None, None),
    ] {
        tx.execute(
            "INSERT INTO tasks (id,title,status,assignee,reviewer,created_by,created_at,updated_at,refs)
             VALUES (?1,'CLI endpoint fixture',?2,?3,?4,'test',1000,1000,?5)",
            params![id, status, assignee, reviewer, refs],
        )
        .unwrap();
    }
    for (capability, task_id, agent, role, revoked) in [
        ("worker-cap", 100, "Worker", "worker", None),
        ("reviewer-cap", 101, "Reviewer", "reviewer", None),
        ("graph-cap", 102, "GraphReviewer", "reviewer", None),
        ("reactor-cap", 103, "Reactor", "worker", None),
        ("revoked-cap", 104, "Revoked", "worker", Some(1002)),
        ("ended-cap", 105, "Ended", "worker", None),
    ] {
        tx.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at,revoked_at)
             VALUES (?1,?2,?3,?4,1001,?5)",
            params![capability, task_id, agent, role, revoked],
        )
        .unwrap();
    }
    for (task_id, agent) in [(100, "Worker"), (103, "Reactor"), (104, "Revoked")] {
        tx.execute(
            "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
             VALUES (?1,?2,'worker','test','high','test',1001)",
            params![task_id, agent],
        )
        .unwrap();
    }
    tx.execute(
        "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at,ended_at,end_reason)
         VALUES (105,'Ended','worker','test','high','test',1001,1002,'completed')",
        [],
    )
    .unwrap();
    for (task_id, agent, capability, pr) in [
        (101, "Reviewer", "reviewer-cap", 77),
        (102, "GraphReviewer", "graph-cap", 78),
    ] {
        tx.execute(
            "INSERT INTO agent_runs
             (task_id,agent_name,role,model,effort,provider,spawned_at,review_cap_run_id,review_pr,review_head_sha)
             VALUES (?1,?2,'reviewer','test','high','test',1001,?3,?4,?5)",
            params![task_id, agent, capability, pr, "a".repeat(40)],
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

fn command(
    home: &Path,
    endpoint: &Path,
    capability: &str,
    args: &[&str],
) -> assert_cmd::assert::Assert {
    let mut command = Command::new(quorum_bin());
    command
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_AGENT_ENDPOINT", endpoint)
        .env("QUORUM_RUN_ID", capability)
        .args(args)
        .assert()
}

fn command_without_quorum_home(
    private_home: &Path,
    endpoint: &Path,
    capability: &str,
    args: &[&str],
) -> assert_cmd::assert::Assert {
    let mut command = Command::new(quorum_bin());
    command
        .env_remove("QUORUM_HOME")
        .env("HOME", private_home)
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_AGENT_ENDPOINT", endpoint)
        .env("QUORUM_RUN_ID", capability)
        .args(args)
        .assert()
}

fn mailbox_count(db: &Path) -> i64 {
    quorum_core::db::open(db)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM mailbox", [], |row| row.get(0))
        .unwrap()
}

fn task_note_count(db: &Path) -> i64 {
    quorum_core::db::open(db)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM task_notes", [], |row| row.get(0))
        .unwrap()
}

fn success_id(assertion: assert_cmd::assert::Assert) -> i64 {
    let output = assertion.success().get_output().clone();
    assert!(output.stderr.is_empty(), "unexpected stderr: {output:?}");
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["ok"], true);
    value["mailbox_id"].as_i64().unwrap()
}

fn success_note_id(assertion: assert_cmd::assert::Assert) -> i64 {
    let output = assertion.success().get_output().clone();
    assert!(output.stderr.is_empty(), "unexpected stderr: {output:?}");
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["ok"], true);
    assert_eq!(value.as_object().unwrap().len(), 2);
    let id = value["note_id"].as_i64().unwrap();
    assert!(id > 0);
    id
}

#[test]
fn managed_completion_is_endpoint_only_and_authoritative() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let repo = root.path().join("repo");
    let worktrees = root.path().join("worktrees");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::create_dir_all(&worktrees).unwrap();
    init_git_repo(&repo);
    let names = root.path().join("names.txt");
    std::fs::write(&names, "AgentOne\nAgentTwo\n").unwrap();
    let config = root.path().join("serve.toml");
    write_config(&config);
    let daemon = ServeProcess::start(&home, &repo, &worktrees, &names, &config);
    daemon.wait_for("serving (cap=1)");
    let db = home.join("repos/test__repo/quorum.db");
    let endpoint = endpoint_path(&db);
    install_fixtures(&db);
    let before = mailbox_count(&db);

    let worker = success_id(command(
        &home,
        &endpoint,
        "worker-cap",
        &[
            "submit",
            "--agent",
            "Impostor",
            "--pr",
            "999",
            "--summary",
            "delivered",
        ],
    ));
    let reviewer = success_id(command(
        &home,
        &endpoint,
        "reviewer-cap",
        &[
            "submit",
            "--agent",
            "Impostor",
            "--pr",
            "999",
            "--verdict",
            "changes",
            "--blocking",
            "1",
            "--feedback",
            "fix it",
        ],
    ));
    let graph = success_id(command(
        &home,
        &endpoint,
        "graph-cap",
        &[
            "submit",
            "--agent",
            "GraphReviewer",
            "--pr",
            "999",
            "--verdict",
            "graph-blocker",
            "--feedback-json",
            r#"{"category":"boundary-violation","affected_task":102,"violated_assigned_boundary":"worker-only file","evidence":["changed another task"]}"#,
        ],
    ));
    let reaction = success_id(command(
        &home,
        &endpoint,
        "reactor-cap",
        &[
            "react",
            "--agent",
            "Impostor",
            "--task-id",
            "999",
            "--state",
            "blocked",
        ],
    ));
    let worker_note_file = root.path().join("worker-note.txt");
    std::fs::write(&worker_note_file, "worker progress").unwrap();
    // Grok runs with an invocation-private HOME and no QUORUM_HOME. The note
    // must still reach the daemon-owned database through its endpoint.
    let private_home = root.path().join("private-home");
    std::fs::create_dir_all(&private_home).unwrap();
    let worker_note = success_note_id(command_without_quorum_home(
        &private_home,
        &endpoint,
        "worker-cap",
        &[
            "task-update",
            "--agent",
            "Worker",
            "--task-id",
            "100",
            "--note-file",
            worker_note_file.to_str().unwrap(),
        ],
    ));
    let reviewer_note_file = root.path().join("reviewer-note.txt");
    std::fs::write(&reviewer_note_file, "review progress").unwrap();
    let reviewer_note = success_note_id(command(
        &home,
        &endpoint,
        "reviewer-cap",
        &[
            "task-update",
            "--agent",
            "Reviewer",
            "--task-id",
            "101",
            "--note-file",
            reviewer_note_file.to_str().unwrap(),
        ],
    ));

    let conn = quorum_core::db::open(&db).unwrap();
    let rows: Vec<MailboxShape> = conn
        .prepare(
            "SELECT agent,kind,task_id,pr,verdict,feedback,note,payload FROM mailbox
             WHERE id IN (?1,?2,?3,?4) ORDER BY id",
        )
        .unwrap()
        .query_map(params![worker, reviewer, graph, reaction], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let graph_payload: Value = serde_json::from_str(rows[2].7.as_deref().unwrap()).unwrap();
    assert_eq!(
        rows,
        vec![
            (
                "Worker".into(),
                "done".into(),
                100,
                Some(42),
                None,
                None,
                Some("delivered".into()),
                None
            ),
            (
                "Reviewer".into(),
                "done".into(),
                101,
                Some(77),
                Some("changes".into()),
                Some("fix it".into()),
                None,
                Some(r#"{"blocking":1}"#.into())
            ),
            (
                "GraphReviewer".into(),
                "done".into(),
                102,
                Some(78),
                Some("graph-blocker".into()),
                None,
                None,
                Some(r#"{"run_id":"graph-cap","feedback":{"category":"boundary-violation","affected_task":102,"violated_assigned_boundary":"worker-only file","evidence":["changed another task"]}}"#.into())
            ),
            (
                "Reactor".into(),
                "task_update".into(),
                103,
                None,
                None,
                None,
                Some("blocked".into()),
                None
            ),
        ]
    );
    assert_eq!(graph_payload["run_id"], "graph-cap");
    assert_eq!(graph_payload["feedback"]["affected_task"], 102);
    let notes: Vec<(i64, String, String)> = conn
        .prepare("SELECT task_id,agent,body FROM task_notes WHERE id IN (?1,?2) ORDER BY id")
        .unwrap()
        .query_map(params![worker_note, reviewer_note], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        notes,
        vec![
            (100, "Worker".into(), "worker progress".into()),
            (101, "Reviewer".into(), "review progress".into()),
        ]
    );
    drop(conn);
    assert!(
        !private_home.join(".quorum").exists(),
        "managed note command opened a private Quorum home"
    );

    let notes_before_rejections = task_note_count(&db);
    for (capability, args) in [
        (
            "worker-cap",
            vec![
                "task-update",
                "--agent",
                "Impostor",
                "--task-id",
                "100",
                "--note-file",
                worker_note_file.to_str().unwrap(),
            ],
        ),
        (
            "worker-cap",
            vec![
                "task-update",
                "--agent",
                "Worker",
                "--task-id",
                "999",
                "--note-file",
                worker_note_file.to_str().unwrap(),
            ],
        ),
        (
            "revoked-cap",
            vec![
                "task-update",
                "--agent",
                "Revoked",
                "--task-id",
                "104",
                "--note-file",
                worker_note_file.to_str().unwrap(),
            ],
        ),
        (
            "ended-cap",
            vec![
                "task-update",
                "--agent",
                "Ended",
                "--task-id",
                "105",
                "--note-file",
                worker_note_file.to_str().unwrap(),
            ],
        ),
    ] {
        command_without_quorum_home(&private_home, &endpoint, capability, &args)
            .code(2)
            .stderr(predicates::str::contains("agent endpoint rejected"));
    }

    let body_file = root.path().join("body.txt");
    std::fs::write(&body_file, "new body").unwrap();
    for extra in [
        vec!["--status", "working"],
        vec!["--refs", r#"{"issue":1}"#],
        vec!["--body-file", body_file.to_str().unwrap()],
        vec!["--depends-on", "[]"],
    ] {
        let mut args = vec!["task-update", "--agent", "Worker", "--task-id", "100"];
        args.extend(extra);
        args.extend(["--note-file", worker_note_file.to_str().unwrap()]);
        command_without_quorum_home(&private_home, &endpoint, "worker-cap", &args)
            .code(2)
            .stderr(predicates::str::contains(
                "daemon-managed task-update supports only",
            ));
    }
    assert_eq!(
        task_note_count(&db),
        notes_before_rejections,
        "rejected managed updates appended notes"
    );

    for (capability, args) in [
        ("revoked-cap", vec!["submit", "--agent", "Revoked"]),
        ("ended-cap", vec!["submit", "--agent", "Ended"]),
        (
            "reviewer-cap",
            vec![
                "react",
                "--agent",
                "Reviewer",
                "--task-id",
                "101",
                "--state",
                "blocked",
            ],
        ),
        (
            "worker-cap",
            vec![
                "submit",
                "--agent",
                "Worker",
                "--verdict",
                "approved",
                "--blocking",
                "0",
            ],
        ),
    ] {
        command(&home, &endpoint, capability, &args)
            .code(2)
            .stderr(predicates::str::contains("agent endpoint rejected"));
    }
    assert_eq!(
        mailbox_count(&db),
        before + 4,
        "rejected calls wrote mailbox rows"
    );
    daemon.stop();
}

#[test]
fn unavailable_timed_out_and_malformed_endpoints_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let private_home = root.path().join("private-home");
    std::fs::create_dir_all(&private_home).unwrap();
    let note_file = root.path().join("note.txt");
    std::fs::write(&note_file, "progress").unwrap();
    let missing = root.path().join("missing.sock");
    command_without_quorum_home(
        &private_home,
        &missing,
        "capability",
        &[
            "task-update",
            "--agent",
            "Worker",
            "--task-id",
            "1",
            "--note-file",
            note_file.to_str().unwrap(),
        ],
    )
    .code(3)
    .stderr(predicates::str::contains("agent endpoint request failed"));

    let timeout_socket = root.path().join("timeout.sock");
    let listener = UnixListener::bind(&timeout_socket).unwrap();
    let timeout_server = thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        thread::sleep(Duration::from_secs(6));
    });
    command_without_quorum_home(
        &private_home,
        &timeout_socket,
        "capability",
        &[
            "task-update",
            "--agent",
            "Worker",
            "--task-id",
            "1",
            "--note-file",
            note_file.to_str().unwrap(),
        ],
    )
    .code(3)
    .stderr(predicates::str::contains("request timed out"));
    timeout_server.join().unwrap();

    let malformed_socket = root.path().join("malformed.sock");
    let listener = UnixListener::bind(&malformed_socket).unwrap();
    let malformed_server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).unwrap();
        let mut body = vec![0; u32::from_be_bytes(prefix) as usize];
        stream.read_exact(&mut body).unwrap();
        stream.write_all(&4_u32.to_be_bytes()).unwrap();
        stream.write_all(b"nope").unwrap();
    });
    command_without_quorum_home(
        &private_home,
        &malformed_socket,
        "capability",
        &[
            "task-update",
            "--agent",
            "Worker",
            "--task-id",
            "1",
            "--note-file",
            note_file.to_str().unwrap(),
        ],
    )
    .code(3)
    .stderr(predicates::str::contains("malformed response"));
    malformed_server.join().unwrap();
    assert!(
        !private_home.join(".quorum").exists(),
        "failed managed calls opened a private Quorum home"
    );
}
