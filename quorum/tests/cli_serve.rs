//! Integration tests for `quorum serve`.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

fn cargo_bin() -> std::path::PathBuf {
    assert_cmd::cargo::cargo_bin("quorum")
}

#[test]
fn serve_boots_and_stops_on_sigint() {
    let home = tempfile::tempdir().unwrap();

    let init_status = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .status()
        .unwrap();
    assert!(init_status.success(), "init failed");

    let mut child = Command::new(cargo_bin())
        .env("QUORUM_HOME", home.path())
        .args(["serve", "--cap", "1"])
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

    // Keep reading stderr so the child doesn't get EPIPE on its writes.
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
