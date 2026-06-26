//! Integration tests for `quorum init` and `quorum reset`.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

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

// -- reset (#59) --------------------------------------------------------------------------

#[test]
fn reset_without_yes_refuses_and_preserves_state() {
    let home = tempfile::tempdir().unwrap();
    // Seed a task so we can prove nothing was wiped.
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "keep me"])
        .assert()
        .success();
    // `reset` with no --yes must refuse: exit 2 (usage) and name the confirm flag.
    quorum(home.path())
        .args(["reset"])
        .assert()
        .code(2)
        .stderr(predicates::str::contains("--yes"));
    // State is intact — the refusal did not touch the DB.
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("keep me"));
}

#[test]
fn reset_yes_wipes_to_clean_db() {
    let home = tempfile::tempdir().unwrap();
    // Seed a task (also registers agent "boss" via touch) so there's state to wipe.
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "wipe me"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("wipe me"));
    // Wipe with confirmation.
    quorum(home.path())
        .args(["reset", "--yes"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"reset\":true"));
    // Clean DB: no tasks, no agents, and the file is recreated (usable).
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
    quorum(home.path())
        .args(["roster"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
    assert!(home.path().join("quorum.db").exists());
}

#[test]
fn reset_yes_on_fresh_home_succeeds() {
    // reset --yes before any DB exists must not error on the missing-file removal — it
    // should just create a clean DB (the sidecar removals are NotFound-tolerant).
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["reset", "--yes"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"reset\":true"));
    assert!(home.path().join("quorum.db").exists());
}
