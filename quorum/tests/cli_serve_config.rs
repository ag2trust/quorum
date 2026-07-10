//! Integration tests for `quorum serve --config`.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

fn cargo_bin() -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
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

/// Read stderr lines until we see the banner or timeout.
fn collect_stderr_until(
    reader: &mut BufReader<std::process::ChildStderr>,
    marker: &str,
    timeout: Duration,
) -> Vec<String> {
    let mut lines = Vec::new();
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::time::Instant::now() > deadline {
            break;
        }
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                let found = line.contains(marker);
                lines.push(line);
                if found {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    lines
}

#[test]
fn serve_config_file_with_flag_override() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let init_status = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    assert!(init_status.success(), "init failed");

    // Write a config file with cap=8 and model=opus-48
    let config_path = home.path().join("serve-test.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
repo = "test/repo"
repo_dir = "{repo_dir}"
worktree_base = "{wt_base}"
cap = 8
model = "opus-48"
effort = "max"
max_turn_wall_secs = 2700
"#,
            repo_dir = repo_dir.path().to_string_lossy(),
            wt_base = wt_base.path().to_string_lossy(),
        ),
    )
    .unwrap();

    let sentinel = tempfile::tempdir().unwrap();
    let sentinel_path = sentinel.path().to_string_lossy().to_string();

    // Launch serve with --config, overriding cap=2 via flag
    let mut child = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "serve",
            "--config",
            &config_path.to_string_lossy(),
            "--cap",
            "2",
            "--names-file",
            &names_file.to_string_lossy(),
            "--exit-when-gone",
            &sentinel_path,
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let mut reader = BufReader::new(child.stderr.take().unwrap());

    // Collect stderr lines until "serving" banner (post-recovery startup)
    let lines = collect_stderr_until(&mut reader, "serving", Duration::from_secs(10));
    let stderr_text = lines.join("");

    // The resolved config banner should appear before "serving"
    assert!(
        stderr_text.contains("resolved serve config"),
        "banner missing from stderr:\n{stderr_text}"
    );
    // Config file path should be shown
    assert!(
        stderr_text.contains("serve-test.toml"),
        "config path missing from banner:\n{stderr_text}"
    );
    // cap=2 from flag should override cap=8 from file
    assert!(
        stderr_text.contains("2 (flag)"),
        "cap should show '2 (flag)':\n{stderr_text}"
    );
    // model=opus-48 from file (no flag override)
    assert!(
        stderr_text.contains("opus-48 (file)"),
        "model should show 'opus-48 (file)':\n{stderr_text}"
    );
    // effort=max from file (no flag override)
    assert!(
        stderr_text.contains("max (file)"),
        "effort should show 'max (file)':\n{stderr_text}"
    );
    // max_turn_wall_secs=2700 from file
    assert!(
        stderr_text.contains("2700 (file)"),
        "max_turn_wall_secs should show '2700 (file)':\n{stderr_text}"
    );
    // base_branch defaults
    assert!(
        stderr_text.contains("main (default)"),
        "base_branch should show 'main (default)':\n{stderr_text}"
    );

    // Clean shutdown
    drop(sentinel);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                let code = status.code().unwrap_or(-1);
                assert!(
                    code == 0 || code == 1,
                    "serve exited with unexpected code: {status}"
                );
                return;
            }
            None => {
                if std::time::Instant::now() > deadline {
                    child.kill().ok();
                    panic!("serve did not exit within 5s");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

#[test]
fn serve_config_rejects_unknown_keys() {
    let home = tempfile::tempdir().unwrap();

    let init_status = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    assert!(init_status.success(), "init failed");

    let config_path = home.path().join("bad.toml");
    std::fs::write(&config_path, "typo_key = 42\nrepo = \"test/repo\"\n").unwrap();

    let output = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "serve",
            "--config",
            &config_path.to_string_lossy(),
            "--repo-dir",
            "/tmp/x",
            "--worktree-base",
            "/tmp/y",
        ])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "serve should fail on unknown config key"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("typo_key"),
        "error should name the unknown key: {stderr}"
    );
}

#[test]
fn serve_config_explicit_missing_file_fails() {
    let home = tempfile::tempdir().unwrap();

    let init_status = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    assert!(init_status.success(), "init failed");

    let output = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "serve",
            "--config",
            "/nonexistent/path/typo.toml",
            "--repo",
            "test/repo",
            "--repo-dir",
            "/tmp/x",
            "--worktree-base",
            "/tmp/y",
        ])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "serve should fail when --config points to a missing file"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not found"),
        "error should say 'not found': {stderr}"
    );
}

#[test]
fn serve_config_default_path_loaded() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let init_status = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    assert!(init_status.success(), "init failed");

    // Create default config path: ~/.quorum/serve/test__repo.toml
    let serve_dir = home.path().join("serve");
    std::fs::create_dir_all(&serve_dir).unwrap();
    let default_config = serve_dir.join("test__repo.toml");
    std::fs::write(
        &default_config,
        format!(
            r#"
repo_dir = "{repo_dir}"
worktree_base = "{wt_base}"
model = "fable-5"
"#,
            repo_dir = repo_dir.path().to_string_lossy(),
            wt_base = wt_base.path().to_string_lossy(),
        ),
    )
    .unwrap();

    let sentinel = tempfile::tempdir().unwrap();
    let sentinel_path = sentinel.path().to_string_lossy().to_string();

    // Launch with --repo only — config should be auto-discovered
    let mut child = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "serve",
            "--repo",
            "test/repo",
            "--names-file",
            &names_file.to_string_lossy(),
            "--exit-when-gone",
            &sentinel_path,
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let mut reader = BufReader::new(child.stderr.take().unwrap());
    let lines = collect_stderr_until(&mut reader, "serving", Duration::from_secs(10));
    let stderr_text = lines.join("");

    assert!(
        stderr_text.contains("test__repo.toml"),
        "should auto-discover default config path:\n{stderr_text}"
    );
    assert!(
        stderr_text.contains("fable-5 (file)"),
        "model should come from auto-discovered config:\n{stderr_text}"
    );

    drop(sentinel);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                let code = status.code().unwrap_or(-1);
                assert!(
                    code == 0 || code == 1,
                    "serve exited with unexpected code: {status}"
                );
                return;
            }
            None => {
                if std::time::Instant::now() > deadline {
                    child.kill().ok();
                    panic!("serve did not exit within 5s");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}
