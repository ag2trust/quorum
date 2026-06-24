//! Integration tests for `quorum init`.

use assert_cmd::Command;

#[test]
fn init_creates_db() {
    let home = tempfile::tempdir().unwrap();
    Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .assert()
        .success();
    assert!(home.path().join("quorum.db").exists());
}

#[test]
fn init_is_idempotent() {
    let home = tempfile::tempdir().unwrap();
    for _ in 0..2 {
        Command::cargo_bin("quorum")
            .unwrap()
            .env("QUORUM_HOME", home.path())
            .arg("init")
            .assert()
            .success();
    }
}

#[test]
fn concurrent_init_is_safe() {
    // N separate processes running `init` at once must all succeed against one DB —
    // migration runs under BEGIN IMMEDIATE, so first-runs serialize safely.
    let home = tempfile::tempdir().unwrap();
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let p = home.path().to_path_buf();
            std::thread::spawn(move || {
                Command::cargo_bin("quorum")
                    .unwrap()
                    .env("QUORUM_HOME", &p)
                    .arg("init")
                    .assert()
                    .success();
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert!(home.path().join("quorum.db").exists());
}
