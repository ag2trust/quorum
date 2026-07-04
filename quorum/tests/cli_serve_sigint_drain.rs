//! Integration tests for SIGINT/SIGTERM drain mode (issue #173).
//!
//! Verifies:
//! 1. First SIGINT enters drain (no new claims, in-flight agents finish), exits 0.
//! 2. Second SIGINT during drain forces immediate teardown, exits 0.
//! 3. SIGINT-drain exit code (0) does NOT trigger supervisor relaunch (only 75 does).

use std::io::{BufRead, BufReader, Write};
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
}

impl ServeHandle {
    fn start(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        extra_args: &[&str],
    ) -> Self {
        let fake_agent = cargo_bin("fake-agent");
        let mut args = vec![
            "serve",
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
            "true",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
        for a in extra_args {
            args.push(a.to_string());
        }

        let mut child = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home)
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

    fn wait_exit(&mut self, timeout_secs: u64) -> Option<std::process::ExitStatus> {
        let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            match self.child.try_wait().unwrap() {
                Some(status) => {
                    self.drain_remaining();
                    return Some(status);
                }
                None => {
                    if std::time::Instant::now() > deadline {
                        self.child.kill().ok();
                        self.drain_remaining();
                        return self.child.wait().ok();
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn drain_remaining(&mut self) {
        while let Ok(line) = self.rx.recv_timeout(Duration::from_millis(200)) {
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

    fn send_sigint(&self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGINT);
        }
    }

    fn has_line_containing(&self, needle: &str) -> bool {
        self.lines.iter().any(|l| l.contains(needle))
    }
}

fn seed_task(home: &std::path::Path, title: &str) {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
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

fn quorum_done(home: &std::path::Path, args: &[&str]) {
    let mut cmd_args = vec!["done"];
    cmd_args.extend_from_slice(args);
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .args(&cmd_args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "done failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// First SIGINT with an in-flight agent enters drain mode. The agent finishes
/// its turn, then the daemon tears it down and exits 0 (not 75).
#[test]
fn sigint_drains_in_flight_agent_and_exits_0() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Drain test task");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &[],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "did not spawn agent: {:?}",
        handle.lines
    );

    let agent_name = handle
        .extract_agent_name("spawning agent ")
        .expect("could not extract agent name");

    assert!(
        handle.wait_for("result", 15),
        "agent did not produce result: {:?}",
        handle.lines
    );

    // Agent is mid-task (fake-agent stays alive between turns). Send SIGINT.
    handle.send_sigint();

    assert!(
        handle.wait_for("draining", 5),
        "did not see drain message: {:?}",
        handle.lines
    );

    // Complete the agent's task so it becomes idle → drain tears it down.
    quorum_done(home.path(), &["--agent", &agent_name]);

    // Daemon should tear down the idle worker and exit.
    assert!(
        handle.wait_for("DRAIN: tearing down idle worker", 10),
        "did not see idle worker teardown: {:?}",
        handle.lines
    );

    let status = handle.wait_exit(10).expect("serve did not exit");
    assert_eq!(
        status.code(),
        Some(0),
        "expected exit 0, got {:?}. Lines: {:?}",
        status.code(),
        handle.lines
    );

    assert!(
        !handle.has_line_containing("exiting 75"),
        "SIGINT drain must NOT exit 75 (supervisor would relaunch). Lines: {:?}",
        handle.lines
    );
}

/// Second SIGINT while already draining forces immediate teardown.
#[test]
fn double_sigint_forces_immediate_teardown() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .status()
        .unwrap();

    seed_task(home.path(), "Double SIGINT task");

    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &[],
    );

    assert!(
        handle.wait_for("spawning agent", 15),
        "did not spawn agent: {:?}",
        handle.lines
    );

    assert!(
        handle.wait_for("result", 15),
        "agent did not produce result: {:?}",
        handle.lines
    );

    // First SIGINT → drain
    handle.send_sigint();

    assert!(
        handle.wait_for("draining", 5),
        "did not see drain message after first SIGINT: {:?}",
        handle.lines
    );

    // Brief pause so the daemon processes the drain state before second signal.
    std::thread::sleep(Duration::from_millis(200));

    // Second SIGINT → force shutdown
    handle.send_sigint();

    let status = handle
        .wait_exit(5)
        .expect("serve did not exit after double SIGINT");
    assert_eq!(
        status.code(),
        Some(0),
        "expected exit 0 on double SIGINT, got {:?}. Lines: {:?}",
        status.code(),
        handle.lines
    );

    assert!(
        handle.has_line_containing("force shutdown"),
        "expected 'force shutdown' log. Lines: {:?}",
        handle.lines
    );
}

/// SIGINT with no in-flight agents exits immediately with 0.
#[test]
fn sigint_no_agents_exits_immediately() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .status()
        .unwrap();

    // No tasks seeded — daemon will be idle.
    let mut handle = ServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &[],
    );

    assert!(
        handle.wait_for("serving", 5),
        "did not see 'serving' banner: {:?}",
        handle.lines
    );

    handle.send_sigint();

    let status = handle
        .wait_exit(5)
        .expect("serve did not exit after SIGINT");
    assert!(
        status.success(),
        "expected exit 0 with no agents, got {:?}. Lines: {:?}",
        status.code(),
        handle.lines
    );
}
