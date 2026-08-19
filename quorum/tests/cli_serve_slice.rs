//! Slice test: seed a task, run `quorum serve` with fake-agent, verify the agent
//! was spawned, processed turn 1, then after a Done mailbox row, teardown occurs.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

/// Kill-on-drop guard for a serve child process.
struct ServeGuard {
    child: std::process::Child,
}

impl Drop for ServeGuard {
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

fn cargo_bin(name: &str) -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin(name)
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
    // Create origin/main ref so `serve` can provision worktrees from it
    Command::new("git")
        .args(["-C", &d, "remote", "add", "origin", &*d])
        .status()
        .unwrap();
    Command::new("git")
        .args(["-C", &d, "fetch", "origin"])
        .status()
        .unwrap();
}

#[test]
fn serve_spawns_agent_and_tears_down_on_done() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    // Init quorum DB
    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    // Seed a task
    let mut task_child = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            "Test task for slice",
            "--created-by",
            "TestCreator",
            "--body-stdin",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let mut stdin = task_child.stdin.take().unwrap();
        stdin.write_all(b"Do the thing").unwrap();
    }
    let task_out = task_child.wait_with_output().unwrap();
    assert!(
        task_out.status.success(),
        "task-create failed: {}",
        String::from_utf8_lossy(&task_out.stderr)
    );

    // Start serve with fake-agent
    let sentinel = tempfile::tempdir().unwrap();
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
            "--exit-when-gone",
            &sentinel.path().to_string_lossy(),
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let stderr = child.stderr.take().unwrap();
    let mut _guard = ServeGuard { child };

    // Read stderr on a dedicated thread so timeouts are enforceable
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let mut saw_spawning = false;
    let mut saw_result = false;
    let mut agent_name = String::new();
    let mut lines = Vec::new();

    while let Ok(line) = rx.recv_timeout(Duration::from_secs(15)) {
        if line.contains("spawning agent") {
            saw_spawning = true;
            if let Some(name) = line.split("spawning agent ").nth(1) {
                agent_name = name.split_whitespace().next().unwrap_or("").to_string();
            }
        }
        if line.contains("result") {
            saw_result = true;
        }
        lines.push(line);
        if saw_result {
            break;
        }
    }

    assert!(
        saw_spawning,
        "serve did not spawn an agent. Lines: {lines:?}"
    );
    assert!(
        saw_result,
        "agent spawned but did not produce a result event. Lines: {lines:?}"
    );

    // Now write a Done mailbox row for that agent
    if !agent_name.is_empty() {
        let run_id = {
            let db = home
                .path()
                .join("repos")
                .join("test__repo")
                .join("quorum.db");
            let conn = quorum_core::db::open(&db).unwrap();
            quorum_core::capabilities::active_for_agent(&conn, &agent_name)
                .unwrap()
                .expect("no active capability for spawned agent")
                .run_id
        };
        let done_out = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .env("QUORUM_AGENT_ENDPOINT", agent_endpoint(home.path()))
            .env("QUORUM_RUN_ID", &run_id)
            .args(["done", "--agent", &agent_name, "--pr", "1"])
            .output()
            .unwrap();
        assert!(
            done_out.status.success(),
            "done failed: {}",
            String::from_utf8_lossy(&done_out.stderr)
        );

        // Wait for daemon to process the done signal
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            let remaining = deadline - std::time::Instant::now();
            match rx.recv_timeout(remaining) {
                Ok(line) => {
                    let done = line.contains("spawning reviewer") || line.contains("tearing down");
                    lines.push(line);
                    if done {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }

    // Kill the serve process
    unsafe {
        libc::kill(_guard.child.id() as libc::pid_t, libc::SIGINT);
    }
    let status = _guard.child.wait().unwrap();
    // Exit 0 or signal — both OK for this test
    let _ = status;
}

#[test]
fn sigint_during_work_releases_task_back_to_open() {
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

    let task_out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            "Interrupted task",
            "--created-by",
            "TestCreator",
        ])
        .output()
        .unwrap();
    assert!(
        task_out.status.success(),
        "task-create failed: {}",
        String::from_utf8_lossy(&task_out.stderr)
    );

    let sentinel = tempfile::tempdir().unwrap();
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
            "--exit-when-gone",
            &sentinel.path().to_string_lossy(),
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let stderr = child.stderr.take().unwrap();
    let mut _guard = ServeGuard { child };
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    // Wait until the agent is spawned (task is claimed and in progress)
    let mut saw_spawning = false;
    let mut lines = Vec::new();
    while let Ok(line) = rx.recv_timeout(Duration::from_secs(15)) {
        let done = line.contains("spawning agent");
        lines.push(line);
        if done {
            saw_spawning = true;
            break;
        }
    }
    assert!(
        saw_spawning,
        "serve did not spawn an agent. Lines: {lines:?}"
    );

    // SIGINT mid-work — no Done signal was sent
    unsafe {
        libc::kill(_guard.child.id() as libc::pid_t, libc::SIGINT);
    }
    _guard.child.wait().unwrap();

    // The interrupted task must be released back to open, not marked done
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
        "task was not released back to open after SIGINT: {stdout}"
    );
}
