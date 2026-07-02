//! Integration tests for `quorum serve`.

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
fn serve_boots_and_stops_on_sigint() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let wt_base = tempfile::tempdir().unwrap();

    init_git_repo(repo_dir.path());
    let names_file = write_names_file(home.path());

    let init_status = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .status()
        .unwrap();
    assert!(init_status.success(), "init failed");

    let mut child = Command::new(cargo_bin())
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
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let stderr = child.stderr.take().unwrap();
    let mut reader = BufReader::new(stderr);

    let mut banner_found = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if std::time::Instant::now() > deadline {
            child.kill().ok();
            panic!("serve did not print 'serving' banner within 5s");
        }
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if line.contains("serving") {
                    banner_found = true;
                    break;
                }
            }
            Err(_) => break,
        }
    }
    assert!(banner_found, "serve did not print 'serving' banner");

    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGINT);
    }

    let drain = std::thread::spawn(move || {
        let mut sink = String::new();
        loop {
            match reader.read_line(&mut sink) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                drain.join().ok();
                assert!(status.success(), "serve exited with non-zero: {status}");
                return;
            }
            None => {
                if std::time::Instant::now() > deadline {
                    child.kill().ok();
                    drain.join().ok();
                    panic!("serve did not exit within 3s after SIGINT");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}
