//! Test: `quorum serve` does NOT pass `--bare` to spawned agents by default
//! (inherits operator's Claude login), and a config with `no_bare_agent = false`
//! re-enables bare mode.

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

struct BareServeHandle {
    child: std::process::Child,
    rx: mpsc::Receiver<String>,
    lines: Vec<String>,
    _sentinel: Option<tempfile::TempDir>,
}

impl Drop for BareServeHandle {
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

impl BareServeHandle {
    fn start(
        home: &std::path::Path,
        repo: &std::path::Path,
        wt_base: &std::path::Path,
        names: &std::path::Path,
        extra_args: &[&str],
    ) -> Self {
        let sentinel = tempfile::tempdir().unwrap();
        let fake_agent = cargo_bin("fake-agent");
        let mut cmd = Command::new(cargo_bin("quorum"));
        cmd.env("QUORUM_HOME", home)
            .env("QUORUM_REPO", "test/repo")
            .arg("serve")
            .arg("--repo")
            .arg("test/repo")
            .arg("--cap")
            .arg("1")
            .arg("--repo-dir")
            .arg(repo)
            .arg("--worktree-base")
            .arg(wt_base)
            .arg("--names-file")
            .arg(names)
            .arg("--agent-bin")
            .arg(&fake_agent)
            .arg("--exit-when-gone")
            .arg(sentinel.path());
        for a in extra_args {
            cmd.arg(a);
        }
        let mut child = cmd
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

        BareServeHandle {
            child,
            rx,
            lines: Vec::new(),
            _sentinel: Some(sentinel),
        }
    }

    /// Wait for a daemon-decision line (e.g. "classifier: stored …") on serve's
    /// stderr. NOTE: agent *text* is NOT on stderr — observe that via
    /// `wait_session_log` / `session_log_contains` instead.
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

    fn stop(mut self) {
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGINT);
        }
        let _ = self.child.wait();
    }
}

/// Scan every worker/reviewer session log under `{home}/logs/*/stream.jsonl`
/// for `needle`. Agent text (like the fake-agent's `[bare]` marker) is written
/// to the per-session log, NOT echoed to daemon stderr — so this is where e2e
/// tests must observe it.
fn session_log_contains(home: &std::path::Path, needle: &str) -> bool {
    let logs = home.join("logs");
    let Ok(entries) = std::fs::read_dir(&logs) else {
        return false;
    };
    for entry in entries.flatten() {
        let stream = entry.path().join("stream.jsonl");
        if let Ok(content) = std::fs::read_to_string(&stream) {
            if content.contains(needle) {
                return true;
            }
        }
    }
    false
}

/// Poll `session_log_contains` until it matches or the timeout elapses.
fn wait_session_log(home: &std::path::Path, needle: &str, timeout_secs: u64) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        if session_log_contains(home, needle) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

#[test]
fn serve_uses_system_login_by_default() {
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

    seed_task(home.path(), "Test no-bare default");

    let handle = BareServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &[],
    );

    assert!(
        wait_session_log(home.path(), "Working on task", 15),
        "agent turn-1 should have landed in the session log"
    );

    assert!(
        !session_log_contains(home.path(), "[bare]"),
        "agent should NOT get --bare by default (system login mode)"
    );

    handle.stop();
}

#[test]
fn serve_omits_bare_with_no_bare_agent_flag() {
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

    seed_task(home.path(), "Test no-bare flag");

    let handle = BareServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--no-bare-agent"],
    );

    // Sync on turn-1 landing in the session log (both variants emit the
    // "Working on task..." prefix; only the bare variant appends "[bare]").
    assert!(
        wait_session_log(home.path(), "Working on task", 15),
        "agent turn-1 should have landed in the session log"
    );

    // The agent's turn-1 message must NOT contain [bare] under --no-bare-agent.
    assert!(
        !session_log_contains(home.path(), "[bare]"),
        "agent turn-1 session log should NOT contain [bare] with --no-bare-agent"
    );

    handle.stop();
}

fn task_get_json(home: &std::path::Path, task_id: u32) -> String {
    let out = Command::new(cargo_bin("quorum"))
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .args(["task-get", "--task-id", &task_id.to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// 2026-07-10 live incident: spawn_classifier hardcoded bare:true, so on
/// subscription-auth machines every classifier turn failed "Not logged in"
/// and the daemon respawn-looped. These two tests pin the classifier spawn
/// to the daemon's bare_agent config end-to-end: fake-agent answers the
/// classifier prompt with an `area:fake-bare` / `area:fake-nobare` cx tag
/// depending on whether it was spawned with --bare, and we assert the tag
/// that lands in the task's refs.
///
/// #180: default changed from bare to system-login (no_bare_agent=true).
#[test]
fn classifier_spawn_uses_system_login_by_default() {
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

    seed_task(home.path(), "Classify me (system login default)");

    let mut handle = BareServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &[],
    );

    assert!(
        handle.wait_for("classifier: stored 1 classification(s)", 30),
        "classifier never stored a classification. Lines: {:?}",
        handle.lines
    );
    handle.stop();

    let refs = task_get_json(home.path(), 1);
    assert!(
        refs.contains("area:fake-nobare"),
        "classifier should NOT get --bare by default (system login); task refs: {refs}"
    );
}

#[test]
fn classifier_spawn_respects_no_bare_agent() {
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

    seed_task(home.path(), "Classify me (no-bare)");

    let mut handle = BareServeHandle::start(
        home.path(),
        repo_dir.path(),
        wt_base.path(),
        &names_file,
        &["--no-bare-agent"],
    );

    assert!(
        handle.wait_for("classifier: stored 1 classification(s)", 30),
        "classifier never stored a classification. Lines: {:?}",
        handle.lines
    );
    handle.stop();

    let refs = task_get_json(home.path(), 1);
    assert!(
        refs.contains("area:fake-nobare"),
        "classifier agent must NOT get --bare under --no-bare-agent; task refs: {refs}"
    );
}
