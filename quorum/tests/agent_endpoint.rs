#![cfg(unix)]

use rusqlite::params;
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const MAX_REQUEST_BYTES: usize = 64 * 1024;
type MailboxShape = (
    String,
    String,
    i64,
    Option<i64>,
    Option<String>,
    Option<String>,
);

fn cargo_bin(name: &str) -> PathBuf {
    assert_cmd::cargo::cargo_bin(name)
}

fn init_git_repo(dir: &Path) {
    for args in [
        vec!["init", "-b", "main"],
        vec!["config", "user.email", "test@example.com"],
        vec!["config", "user.name", "Endpoint Test"],
        vec!["commit", "--allow-empty", "-m", "initial"],
    ] {
        assert!(Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .unwrap()
            .success());
    }
    assert!(Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["remote", "add", "origin"])
        .arg(dir)
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["fetch", "origin"])
        .status()
        .unwrap()
        .success());
}

struct ServeProcess {
    child: Child,
    lines: mpsc::Receiver<String>,
}

impl ServeProcess {
    fn start(home: &Path, repo: &Path, worktrees: &Path, names: &Path, config: &Path) -> Self {
        let fake_agent = cargo_bin("fake-agent");
        let mut child = Command::new(cargo_bin("quorum"))
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
            .arg(fake_agent)
            .args(["--merge-cmd", "true"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
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
                Err(error) => {
                    panic!("daemon did not log {needle:?}: {error}; output={seen:?}")
                }
            }
        }
        panic!("daemon did not log {needle:?}; output={seen:?}");
    }

    fn stop(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self.child.try_wait().unwrap().is_some() {
                return;
            }
            if Instant::now() >= deadline {
                unsafe {
                    libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
                }
                let _ = self.child.wait().unwrap();
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ServeProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            unsafe {
                libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL);
            }
            let _ = self.child.wait();
        }
    }
}

fn endpoint_path(db: &Path) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    db.hash(&mut hasher);
    std::env::temp_dir()
        .join(format!("quorum-agent-{:016x}", hasher.finish()))
        .join("endpoint.sock")
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

fn exchange(endpoint: &Path, request: &Value) -> Value {
    let body = serde_json::to_vec(request).unwrap();
    let mut stream = UnixStream::connect(endpoint).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(6)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(6)))
        .unwrap();
    stream
        .write_all(&(body.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&body).unwrap();
    read_response(&mut stream)
}

fn exchange_raw(endpoint: &Path, length: u32, body: &[u8]) -> Value {
    let mut stream = UnixStream::connect(endpoint).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(6)))
        .unwrap();
    stream.write_all(&length.to_be_bytes()).unwrap();
    stream.write_all(body).unwrap();
    read_response(&mut stream)
}

fn read_response(stream: &mut UnixStream) -> Value {
    let mut prefix = [0; 4];
    stream.read_exact(&mut prefix).unwrap();
    let length = u32::from_be_bytes(prefix) as usize;
    assert!(length <= MAX_REQUEST_BYTES);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    serde_json::from_slice(&body).unwrap()
}

fn request(capability: &str, operation: Value) -> Value {
    json!({"version": 1, "capability": capability, "operation": operation})
}

fn install_authority_fixtures(db: &Path) {
    let mut conn = quorum_core::db::open(db).unwrap();
    let tx = quorum_core::db::begin_immediate(&mut conn).unwrap();
    let tasks = [
        (100, "working", Some("Worker"), None, None),
        (
            101,
            "in-review",
            None,
            Some("Reviewer"),
            Some(r#"{"pr":77}"#),
        ),
        (102, "working", Some("Revoked"), None, None),
        (103, "working", Some("Ended"), None, None),
        (104, "working", Some("Reused"), None, None),
    ];
    for (id, status, assignee, reviewer, refs) in tasks {
        tx.execute(
            "INSERT INTO tasks
             (id,title,status,assignee,reviewer,created_by,created_at,updated_at,refs)
             VALUES (?1,'endpoint fixture',?2,?3,?4,'test',1000,1000,?5)",
            params![id, status, assignee, reviewer, refs],
        )
        .unwrap();
    }
    for (capability, task, agent, role, revoked) in [
        ("worker-cap", 100, "Worker", "worker", None),
        ("reviewer-cap", 101, "Reviewer", "reviewer", None),
        ("revoked-cap", 102, "Revoked", "worker", Some(1002_i64)),
        ("ended-cap", 103, "Ended", "worker", None),
        ("old-cap", 104, "Reused", "worker", None),
    ] {
        tx.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at,revoked_at)
             VALUES (?1,?2,?3,?4,1001,?5)",
            params![capability, task, agent, role, revoked],
        )
        .unwrap();
    }
    tx.execute(
        "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
         VALUES (100,'Worker','worker','test','high','test',1001)",
        [],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO agent_runs
         (task_id,agent_name,role,model,effort,provider,spawned_at,review_cap_run_id,review_pr,review_head_sha)
         VALUES (101,'Reviewer','reviewer','test','high','test',1001,'reviewer-cap',77,?1)",
        ["a".repeat(40)],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
         VALUES (102,'Revoked','worker','test','high','test',1001)",
        [],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO agent_runs
         (task_id,agent_name,role,model,effort,provider,spawned_at,ended_at,end_reason)
         VALUES (103,'Ended','worker','test','high','test',1001,1002,'completed')",
        [],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO agent_runs
         (task_id,agent_name,role,model,effort,provider,spawned_at,ended_at,end_reason)
         VALUES (104,'Reused','worker','test','high','test',1001,1002,'completed')",
        [],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,spawned_at)
         VALUES (104,'Reused','worker','test','high','test',1003)",
        [],
    )
    .unwrap();
    tx.commit().unwrap();
}

fn mailbox_count(db: &Path) -> i64 {
    quorum_core::db::open(db)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM mailbox", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn daemon_endpoint_is_bounded_authoritative_and_torn_down() {
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
    let metadata = std::fs::metadata(&endpoint).unwrap();
    assert!(metadata.file_type().is_socket());
    assert!(
        !include_str!("../src/serve/agent_endpoint.rs").contains(&["Tcp", "Listener"].concat()),
        "agent endpoint must not construct a TCP listener"
    );
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(
        std::fs::metadata(endpoint.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );

    install_authority_fixtures(&db);
    let before = mailbox_count(&db);

    let inventory = exchange(
        &endpoint,
        &request("worker-cap", json!({"type":"inventory"})),
    );
    assert_eq!(inventory["ok"], true);
    assert_eq!(inventory["result"]["repository"], "test/repo");
    assert_eq!(inventory["result"]["task_id"], 100);
    assert_eq!(inventory["result"]["phase"], "initial-worker");
    assert_eq!(
        inventory["result"]["operations"],
        json!(["delivery_report_write"])
    );
    let reviewer_inventory = exchange(
        &endpoint,
        &request("reviewer-cap", json!({"type":"inventory"})),
    );
    assert_eq!(reviewer_inventory["result"]["phase"], "reviewer");
    assert_eq!(reviewer_inventory["result"]["pr"], 77);
    assert_eq!(
        reviewer_inventory["result"]["review_revision"],
        "a".repeat(40)
    );
    assert_eq!(
        reviewer_inventory["result"]["operations"]
            .as_array()
            .unwrap()
            .len(),
        7
    );

    for denied in [
        exchange(&endpoint, &request("missing", json!({"type":"inventory"}))),
        exchange(
            &endpoint,
            &request("revoked-cap", json!({"type":"inventory"})),
        ),
        exchange(
            &endpoint,
            &request("ended-cap", json!({"type":"inventory"})),
        ),
        exchange(&endpoint, &request("old-cap", json!({"type":"inventory"}))),
        exchange(
            &endpoint,
            &request("reviewer-cap", json!({"type":"react","state":"blocked"})),
        ),
        exchange(
            &endpoint,
            &request(
                "worker-cap",
                json!({"type":"protocol","operation":"pull_request_review_write"}),
            ),
        ),
        exchange(
            &endpoint,
            &request(
                "reviewer-cap",
                json!({"type":"protocol","operation":"delivery_report_write"}),
            ),
        ),
        exchange(
            &endpoint,
            &request(
                "worker-cap",
                json!({"type":"protocol","operation":"delivery_report_write"}),
            ),
        ),
        exchange(
            &endpoint,
            &json!({"version":1,"capability":"worker-cap","operation":{"type":"raw_sql"}}),
        ),
        exchange_raw(&endpoint, 5, b"nope!"),
        exchange_raw(&endpoint, (MAX_REQUEST_BYTES + 1) as u32, &[]),
    ] {
        assert_eq!(denied["ok"], false, "unexpected response: {denied}");
    }
    assert_eq!(mailbox_count(&db), before);

    // A bounded processing failure must own the blocking transaction through
    // rollback. Releasing the contended write lock after the failure cannot
    // allow detached work to append a late lifecycle signal.
    let mut lock_conn = quorum_core::db::open(&db).unwrap();
    let lock_tx = quorum_core::db::begin_immediate(&mut lock_conn).unwrap();
    let timeout_endpoint = endpoint.clone();
    let timed_out = std::thread::spawn(move || {
        exchange(
            &timeout_endpoint,
            &request(
                "worker-cap",
                json!({"type":"submit","summary":"must not commit late"}),
            ),
        )
    });
    let denied = timed_out.join().unwrap();
    assert_eq!(denied["ok"], false, "unexpected response: {denied}");
    drop(lock_tx);
    drop(lock_conn);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(mailbox_count(&db), before);

    let reaction = exchange(
        &endpoint,
        &request("worker-cap", json!({"type":"react","state":"note"})),
    );
    assert_eq!(reaction["ok"], true);
    let reviewer_submit = exchange(
        &endpoint,
        &request(
            "reviewer-cap",
            json!({
                "type":"submit",
                "verdict":"changes",
                "feedback":"One blocking issue",
                "blocking":1
            }),
        ),
    );
    assert_eq!(reviewer_submit["ok"], true);
    let worker_submit = exchange(
        &endpoint,
        &request(
            "worker-cap",
            json!({"type":"submit","summary":"Delivered endpoint"}),
        ),
    );
    assert_eq!(worker_submit["ok"], true);

    let conn = quorum_core::db::open(&db).unwrap();
    let rows: Vec<MailboxShape> = conn
        .prepare(
            "SELECT agent,kind,task_id,pr,verdict,note FROM mailbox
             WHERE id IN (?1,?2,?3) ORDER BY id",
        )
        .unwrap()
        .query_map(
            params![
                reaction["result"]["mailbox_id"].as_i64().unwrap(),
                reviewer_submit["result"]["mailbox_id"].as_i64().unwrap(),
                worker_submit["result"]["mailbox_id"].as_i64().unwrap(),
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                "Worker".into(),
                "task_update".into(),
                100,
                None,
                None,
                Some("note".into())
            ),
            (
                "Reviewer".into(),
                "done".into(),
                101,
                Some(77),
                Some("changes".into()),
                None,
            ),
            (
                "Worker".into(),
                "done".into(),
                100,
                None,
                None,
                Some("Delivered endpoint".into()),
            ),
        ]
    );
    drop(conn);

    std::thread::sleep(Duration::from_millis(50));
    let endpoint_logs = daemon.lines.try_iter().collect::<Vec<_>>().join("\n");
    for secret in [
        "worker-cap",
        "reviewer-cap",
        "One blocking issue",
        "Delivered endpoint",
        "raw_sql",
    ] {
        assert!(
            !endpoint_logs.contains(secret),
            "endpoint log exposed capability or request content: {secret}"
        );
    }

    daemon.stop();
    assert!(!endpoint.exists(), "socket survived normal shutdown");
    assert!(
        !endpoint.parent().unwrap().exists(),
        "endpoint artifact directory survived normal shutdown"
    );
}
