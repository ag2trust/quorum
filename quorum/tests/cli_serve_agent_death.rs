//! Slice test: fake worker agent exits mid-task without signaling done.
//! The daemon must detect the death, release the task back to `open`,
//! remove the worktree, and release the name so it can be re-used.
//!
//! Guards audit finding F6: prior to death-detection, a crashed child
//! pinned the slot forever (draining stayed true, task never released,
//! worktree leaked, Phase 5 never fired).

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

#[test]
fn worker_dying_mid_task_releases_task_and_slot() {
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

    // Seed a task whose body triggers fake-agent to exit(1) mid-turn.
    let mut task_child = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--title",
            "Agent-death test",
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
        // Marker recognized by fake_agent: emit assistant then exit(1).
        stdin.write_all(b"DIE_MID_TURN please crash me").unwrap();
    }
    let task_out = task_child.wait_with_output().unwrap();
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

    // Watch for the specific death-detection log line.
    let mut saw_spawn = false;
    let mut saw_death = false;
    let mut agent_name = String::new();
    let mut worktree_path: Option<std::path::PathBuf> = None;
    let mut lines = Vec::new();

    while let Ok(line) = rx.recv_timeout(Duration::from_secs(20)) {
        if line.contains("spawning agent") {
            saw_spawn = true;
            if let Some(name) = line.split("spawning agent ").nth(1) {
                agent_name = name.split_whitespace().next().unwrap_or("").to_string();
            }
        }
        if line.contains("worktree provisioned at ") {
            if let Some(p) = line.split("worktree provisioned at ").nth(1) {
                worktree_path = Some(std::path::PathBuf::from(p.trim()));
            }
        }
        if line.contains("died mid-task") {
            saw_death = true;
        }
        lines.push(line);
        if saw_death {
            break;
        }
    }

    // Kill the serve process so nothing races against our post-conditions.
    unsafe {
        libc::kill(_guard.child.id() as libc::pid_t, libc::SIGINT);
    }
    let _ = _guard.child.wait();

    assert!(saw_spawn, "serve did not spawn a worker. Lines: {lines:?}");
    assert!(
        saw_death,
        "serve did not log death detection for the crashed agent. Lines: {lines:?}"
    );
    assert!(
        !agent_name.is_empty(),
        "could not extract agent name from log lines: {lines:?}"
    );

    // Task must be back to open (not stuck claimed).
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
        "task was not released back to open after agent death: {stdout}"
    );

    // The worker's worktree must be gone.
    if let Some(wt) = worktree_path {
        assert!(
            !wt.exists(),
            "worktree still exists after agent death: {}",
            wt.display()
        );
    }
}
