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
    //
    // Loop-stressed per quorum CLAUDE.md ("Always stress concurrency tests in a loop;
    // a single green run hides flakiness"). The first-creation WAL-switch race is the
    // documented flaky path (busy_timeout doesn't cover journal-mode changes — see
    // `db::set_journal_wal`), so a single round can pass while the bounded retry is
    // silently broken. Each iteration uses a fresh tempdir so every round re-races the
    // initial WAL switch from scratch (a reused DB would already be in WAL, no race).
    for _ in 0..12 {
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
}
