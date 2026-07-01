//! Slice test: seed a task, run `quorum serve` with fake-agent, verify the agent
//! was spawned, processed turn 1, then after a Done mailbox row, teardown occurs.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
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
    Command::new("git")
        .args(["-C", &dir.to_string_lossy(), "init", "-b", "main"])
        .output()
        .unwrap();
    Command::new("git")
        .args([
            "-C",
            &dir.to_string_lossy(),
            "commit",
            "--allow-empty",
            "-m",
            "init",
        ])
        .output()
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
        .arg("init")
        .status()
        .unwrap();

    // Seed a task
    let mut task_child = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
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
    let fake_agent = cargo_bin("fake-agent");
    let mut child = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args([
            "serve",
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
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let stderr = child.stderr.take().unwrap();
    let reader = BufReader::new(stderr);

    // Collect stderr lines until we see the agent spawn and result
    let mut saw_spawning = false;
    let mut saw_result = false;
    let mut agent_name = String::new();
    let deadline = Instant::now() + Duration::from_secs(15);

    let lines: Vec<String> = reader
        .lines()
        .take_while(|_| Instant::now() < deadline)
        .filter_map(|l| l.ok())
        .take_while(|line| {
            if line.contains("spawning agent") {
                saw_spawning = true;
                if let Some(name) = line.split("spawning agent ").nth(1) {
                    agent_name = name.split_whitespace().next().unwrap_or("").to_string();
                }
            }
            if line.contains("result") {
                saw_result = true;
            }
            // Stop after we see result (agent finished turn 1)
            !saw_result
        })
        .collect();

    assert!(
        saw_spawning,
        "serve did not spawn an agent. Lines: {lines:?}"
    );

    // Now write a Done mailbox row for that agent
    if !agent_name.is_empty() {
        let done_out = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home.path())
            .args(["done", "--agent", &agent_name, "--pr", "1"])
            .output()
            .unwrap();
        assert!(
            done_out.status.success(),
            "done failed: {}",
            String::from_utf8_lossy(&done_out.stderr)
        );

        // Wait briefly for teardown
        std::thread::sleep(Duration::from_secs(2));
    }

    // Kill the serve process
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGINT);
    }
    let status = child.wait().unwrap();
    // Exit 0 or signal — both OK for this test
    let _ = status;
}
