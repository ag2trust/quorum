//! End-to-end tests for the auto-emitted event log: state changes generate events on the
//! `events` stream (not the message feed), `quorum log` reads them, `--refs` filters by
//! subject, `--since <seq>` is a strict delta, and `post`/`read` are NOT affected.

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn state_changes_auto_emit_events_on_log() {
    let home = tempfile::tempdir().unwrap();

    // create + claim + done → three events on the log
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-claim", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--status",
            "done",
        ])
        .assert()
        .success();

    quorum(home.path())
        .arg("log")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"kind\":\"task_created\""))
        .stdout(predicates::str::contains("\"kind\":\"task_claimed\""))
        .stdout(predicates::str::contains("\"kind\":\"task_done\""))
        .stdout(predicates::str::contains("\"subject\":\"task#1\""));
}

#[test]
fn refs_filter_matches_subject_exactly() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "a"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "b"])
        .assert()
        .success();
    // task#1 filter must NOT pick up task#11 if it existed; task#1 events only.
    quorum(home.path())
        .args(["log", "--refs", "task#1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"subject\":\"task#1\""))
        .stdout(predicates::str::contains("\"subject\":\"task#2\"").not());
}

#[test]
fn since_seq_is_strict_delta() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "a"])
        .assert()
        .success();
    // The first event has seq=1; --since 1 must skip it.
    quorum(home.path())
        .args(["log", "--since", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"seq\":1").not());
    // Adding another state change appears for --since 1.
    quorum(home.path())
        .args(["task-claim", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();
    quorum(home.path())
        .args(["log", "--since", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"kind\":\"task_claimed\""));
}

#[test]
fn message_feed_carries_no_auto_events() {
    // Acceptance: state changes auto-emit ONLY to the event log. The agent-to-agent message
    // feed (`read`/`peek`) MUST NOT carry any system-generated event.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-claim", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-cancel", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();

    // Reading the feed as a fresh agent: nothing on it (no auto-emits, no manual posts).
    quorum(home.path())
        .args(["read", "--agent", "Z"])
        .assert()
        .success()
        .stdout(predicates::str::contains("task_created").not())
        .stdout(predicates::str::contains("task_claimed").not())
        .stdout(predicates::str::contains("task_cancelled").not());
}

#[test]
fn limit_caps_returned_rows() {
    let home = tempfile::tempdir().unwrap();
    for _ in 0..5 {
        quorum(home.path())
            .args(["task-create", "--created-by", "boss", "--title", "x"])
            .assert()
            .success();
    }
    // 5 task_created events; --limit 2 returns only the first 2 (oldest).
    quorum(home.path())
        .args(["log", "--limit", "2"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"subject\":\"task#1\""))
        .stdout(predicates::str::contains("\"subject\":\"task#2\""))
        .stdout(predicates::str::contains("\"subject\":\"task#3\"").not());
}

#[test]
fn negative_limit_is_usage_error() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["log", "--limit", "-1"])
        .assert()
        .code(2);
}
