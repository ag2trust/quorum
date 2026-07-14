//! Passive/interactive agent submit: a non-roster agent claims a task, pushes
//! a PR, runs `quorum submit --pr N` → daemon fires SignaledDone, task goes
//! working → in-review, Phase 5b spawns reviewer.

use std::io::BufRead;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn cargo_bin(name: &str) -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin(name)
}

fn write_names_file(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("names.txt");
    let mut f = std::fs::File::create(&path).unwrap();
    for i in 0..10 {
        writeln!(f, "DaemonAgent{i}").unwrap();
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
            let reader = std::io::BufReader::new(stderr);
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

    fn sigkill(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGKILL);
        }
        let _ = self.child.wait();
        while let Ok(line) = self.rx.try_recv() {
            self.lines.push(line);
        }
    }
}

/// Passive agent claims a task, pushes PR, runs `quorum submit --pr N` →
/// daemon fires SignaledDone → task goes in-review → Phase 5b spawns reviewer.
#[test]
fn passive_agent_submit_enters_review_lifecycle() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    // Init quorum DB.
    Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");

    // Seed: create task, claim it as a passive agent (not in daemon roster).
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let id = quorum_core::tasks::create(
            &mut conn,
            "test",
            "Passive agent task",
            None,
            0,
            None,
            None,
            None,
            None,
            now,
        )
        .unwrap();
        assert_eq!(id, 1);
        quorum_core::tasks::claim(&mut conn, "PassiveHuman", Some(id), &[], 7200, now).unwrap();

        let task = quorum_core::tasks::get(&conn, id).unwrap().unwrap();
        assert_eq!(task.status, "working");
        assert_eq!(task.assignee.as_deref(), Some("PassiveHuman"));
    }

    // Passive agent runs `quorum submit --agent PassiveHuman --pr 99`.
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["submit", "--agent", "PassiveHuman", "--pr", "99"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "submit failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Start daemon — it should consume the passive agent's submit row.
    let mut handle = ServeHandle::start(home.path(), repo_dir.path(), wt_base.path(), &names_file);

    // Daemon should log passive agent submit processing.
    assert!(
        handle.wait_for("passive agent PassiveHuman submit", 15),
        "daemon did not process passive agent submit row. Lines: {:?}",
        handle.lines
    );

    // Verify lifecycle: task should transition to in-review.
    assert!(
        handle.wait_for("in-review", 10),
        "task did not transition to in-review. Lines: {:?}",
        handle.lines
    );

    // Phase 5b should spawn a reviewer for the now in-review task.
    assert!(
        handle.wait_for("spawning reviewer", 15),
        "reviewer not spawned for passive agent's in-review task. Lines: {:?}",
        handle.lines
    );

    // Verify DB state.
    {
        let conn = quorum_core::db::open(&db_path).unwrap();
        let task = quorum_core::tasks::get(&conn, 1).unwrap().unwrap();
        assert_eq!(
            task.status, "in-review",
            "task status should be in-review, got: {}",
            task.status
        );
    }

    handle.sigkill();
}
