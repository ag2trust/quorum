//! #178 acceptance test: restart resumes done-awaiting-review at the REVIEW
//! stage, not by re-executing the task from scratch.
//!
//! Bug repro (2026-07-04, hit twice live): a worker completes and signals
//! `done --pr N` (PR open on GitHub), sits in awaiting-review, then the
//! daemon is restarted. Pre-fix: the pipeline position was lost — the
//! awaiting-review journal entry had no PR, recovery spawned a stub worker
//! that was reaped as dead (releasing the task to `open`), and a fresh
//! worker re-executed the task producing a duplicate PR.
//!
//! Post-fix: the done --pr N handler upserts the journal with the PR, and
//! recovery routes awaiting-review-with-PR entries to a `PendingReview`
//! collection so a reviewer is provisioned for the recorded PR without a
//! worker re-spawn.

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
        .args(["-C", &d, "remote", "add", "origin", &d])
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
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let sentinel_path = sentinel.path().to_string_lossy().to_string();
        let fake_agent = cargo_bin("fake-agent");
        let mut child = Command::new(cargo_bin("quorum"))
            .env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .args([
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
                "true",
                "--exit-when-gone",
                &sentinel_path,
            ])
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
        // Drain any final buffered log lines
        while let Ok(line) = self.rx.try_recv() {
            self.lines.push(line);
        }
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

fn quorum_done(home: &std::path::Path, args: &[&str]) {
    let mut cmd_args = vec!["done"];
    cmd_args.extend_from_slice(args);
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args(&cmd_args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "done failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// #178: worker signals done --pr N, daemon is SIGKILL'd before verdict,
/// then relaunched — the restart MUST resume at the review stage
/// (provision reviewer against recorded PR) rather than re-execute the
/// task from scratch and produce a duplicate PR.
#[test]
fn restart_resumes_awaiting_review_at_review_stage_no_re_execution() {
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

    seed_task(home.path(), "Task for #178 restart-at-review acceptance");

    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Worker spawns and produces its first result.
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

    // Worker signals done --pr — the daemon persists the PR to the journal.
    quorum_done(home.path(), &["--agent", &worker_name, "--pr", "1"]);

    // Wait for the daemon to acknowledge the PR (proof journal upsert ran).
    assert!(
        handle.wait_for("PR #1 ready for review", 15),
        "daemon did not acknowledge PR #1 before SIGKILL. Lines: {:?}",
        handle.lines
    );
    // Small drain so we've observed all log lines up to this point.
    handle.drain_pending_lines();

    // ── Kill the daemon hard (mimics operator hitting the process). ──
    handle.sigkill();

    // ── Relaunch the daemon. ──
    let mut handle2 = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Recovery MUST route the awaiting-review-with-PR entry to a pending
    // review (no --resume worker spawn for this task).
    assert!(
        handle2.wait_for("resuming task #1 at REVIEW stage", 30),
        "recovery did not route awaiting-review-with-PR to pending review. Lines: {:?}",
        handle2.lines
    );

    // A reviewer MUST be provisioned against the recorded PR.
    assert!(
        handle2.wait_for("spawning reviewer", 30),
        "reviewer not provisioned for pending review. Lines: {:?}",
        handle2.lines
    );

    // A moment for any late log lines.
    std::thread::sleep(Duration::from_millis(500));
    handle2.drain_pending_lines();

    // ── Invariant: NO fresh worker spawn for task #1 after restart. ──
    // This is the core anti-duplication guarantee. The pre-fix bug was
    // exactly that a fresh `spawning agent` fired for the same task on
    // restart, producing a duplicate PR.
    let fresh_worker_spawns = handle2
        .lines
        .iter()
        .filter(|l| l.contains("spawning agent") && l.contains("task #1"))
        .count();
    assert_eq!(
        fresh_worker_spawns, 0,
        "restart re-executed the task from scratch (found 'spawning agent for task #1'). \
         Lines: {:?}",
        handle2.lines
    );

    handle2.sigkill();
}
