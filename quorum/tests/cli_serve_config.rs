//! Integration tests for `quorum serve --config`.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

fn cargo_bin() -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
}

const ROUTING_POLICY: &str = r#"
[model_profiles.primary]
runner = "codex"
model = "gpt-5.6-sol"
effort = "high"
[model_profiles.planner]
runner = "claude"
model = "claude-opus-4-8"
effort = "high"
[routing.classifier]
primary = 100
[routing.planner]
planner = 100
[routing.collector]
primary = 100
[routing.worker.1]
primary = 100
[routing.worker.2]
primary = 100
[routing.worker.3]
primary = 100
[routing.worker.4]
primary = 100
[routing.worker.5]
primary = 100
[routing.reviewer.1]
primary = 100
[routing.reviewer.2]
primary = 100
[routing.reviewer.3]
primary = 100
[routing.reviewer.4]
primary = 100
[routing.reviewer.5]
primary = 100
"#;

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

    // Write a config file with cap=8 and a complete routing policy.
    let config_path = home.path().join("serve-test.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
repo = "test/repo"
repo_dir = "{repo_dir}"
worktree_base = "{wt_base}"
cap = 8
max_turn_wall_secs = 2700
base_branch = "develop"
self_update_branch = "main"
{ROUTING_POLICY}
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
    // The routing policy is summarized without presenting a legacy fixed model.
    assert!(
        stderr_text.contains("model_profiles:            2"),
        "profile count missing from banner:\n{stderr_text}"
    );
    // The deprecated config key maps to max_idle_secs and warns once.
    assert!(
        stderr_text.contains("WARNING: max_turn_wall_secs is deprecated; it now sets the max_idle_secs idle timeout when max_idle_secs is unset"),
        "deprecated max_turn_wall_secs warning missing:\n{stderr_text}"
    );
    assert_eq!(
        stderr_text
            .matches("WARNING: max_turn_wall_secs is deprecated")
            .count(),
        1,
        "deprecated max_turn_wall_secs warning must be one line:\n{stderr_text}"
    );
    assert!(
        stderr_text.contains("max_idle_secs:             2700 (file)"),
        "deprecated max_turn_wall_secs should resolve as max_idle_secs:\n{stderr_text}"
    );
    // Task/PR and self-update branches resolve independently from the file.
    assert!(
        stderr_text.contains("base_branch:               develop (file)"),
        "base_branch should show 'develop (file)':\n{stderr_text}"
    );
    assert!(
        stderr_text.contains("self_update_branch:        main (file)"),
        "self_update_branch should show 'main (file)':\n{stderr_text}"
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
fn serve_accepts_self_update_branch_flag() {
    let output = Command::new(cargo_bin())
        .args(["serve", "--self-update-branch", "main"])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("unexpected argument '--self-update-branch'"),
        "self-update branch flag should parse before configuration validation: {stderr}"
    );
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
fn serve_config_rejects_external_poll_intervals_below_30_seconds() {
    let home = tempfile::tempdir().unwrap();

    for key in ["merge_checks_poll_secs", "sha_poll_interval_secs"] {
        let config_path = home.path().join(format!("{key}.toml"));
        std::fs::write(&config_path, format!("{key} = 10\n{ROUTING_POLICY}")).unwrap();

        let output = Command::new(cargo_bin())
            .env("QUORUM_HOME", home.path())
            .args([
                "serve",
                "--config",
                &config_path.to_string_lossy(),
                "--repo",
                "test/repo",
                "--repo-dir",
                "/tmp/x",
                "--worktree-base",
                "/tmp/y",
            ])
            .output()
            .unwrap();

        assert_eq!(output.status.code(), Some(2), "{key}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains(&format!("{key} must be at least 30 seconds")),
            "{key}: {output:?}"
        );
    }
}

#[test]
fn serve_rejects_removed_fixed_routing_flags() {
    for flag in ["--agent", "--model", "--effort"] {
        let output = Command::new(cargo_bin())
            .args(["serve", flag, "legacy-value"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{flag}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("unexpected argument"),
            "{flag} should be rejected by CLI parsing: {output:?}"
        );
    }
}

#[test]
fn serve_without_routing_policy_fails_closed() {
    let home = tempfile::tempdir().unwrap();
    let output = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .args([
            "serve",
            "--repo",
            "test/repo",
            "--repo-dir",
            "/tmp/x",
            "--worktree-base",
            "/tmp/y",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("model_profiles"),
        "missing routing policy must fail before work can be claimed: {output:?}"
    );
}

#[test]
fn serve_rejects_cli_usd_limit_when_routing_can_select_codex() {
    let home = tempfile::tempdir().unwrap();
    let config_path = home.path().join("routing.toml");
    std::fs::write(&config_path, ROUTING_POLICY).unwrap();

    let output = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "serve",
            "--config",
            &config_path.to_string_lossy(),
            "--repo",
            "test/repo",
            "--repo-dir",
            "/tmp/x",
            "--worktree-base",
            "/tmp/y",
            "--max-turn-cost-usd",
            "1.0",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("USD cost limits"),
        "CLI override must fail before daemon startup: {output:?}"
    );
}

#[test]
fn serve_config_rejects_invalid_r2_sampling_values_with_exit_2() {
    let home = tempfile::tempdir().unwrap();
    let init_status = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    assert!(init_status.success(), "init failed");

    for (name, contents, expected) in [
        (
            "bad-r2-p.toml",
            "r2_steady_state_p = 1.5\n",
            "r2_steady_state_p",
        ),
        (
            "bad-r2-target.toml",
            "r2_target_per_stratum = -1\n",
            "r2_target_per_stratum",
        ),
    ] {
        let config_path = home.path().join(name);
        std::fs::write(&config_path, format!("{contents}{ROUTING_POLICY}")).unwrap();
        let output = Command::new(cargo_bin())
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .args([
                "serve",
                "--config",
                &config_path.to_string_lossy(),
                "--repo",
                "test/repo",
                "--repo-dir",
                "/tmp/x",
                "--worktree-base",
                "/tmp/y",
            ])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "error must name {expected}: {output:?}"
        );
    }
}

#[test]
fn legacy_fixed_routing_keys_are_rejected_at_startup() {
    let home = tempfile::tempdir().unwrap();

    let init_status = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .status()
        .unwrap();
    assert!(init_status.success(), "init failed");

    let config_path = home.path().join("codex-with-claude-floor.toml");
    std::fs::write(
        &config_path,
        "repo = \"test/repo\"\nprovider = \"codex\"\nmin_model = \"opus-47\"\n",
    )
    .unwrap();

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

    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("legacy routing key"),
        "startup error should explain the hard cutover: {stderr}"
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
base_branch = "develop"
{ROUTING_POLICY}
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
        stderr_text.contains("model_profiles:            2"),
        "routing policy should come from auto-discovered config:\n{stderr_text}"
    );
    assert!(
        stderr_text.contains("base_branch:               develop (file)"),
        "base_branch should come from the config file:\n{stderr_text}"
    );
    assert!(
        stderr_text.contains("self_update_branch:        develop (file)"),
        "self_update_branch should inherit the resolved base_branch when omitted:\n{stderr_text}"
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
