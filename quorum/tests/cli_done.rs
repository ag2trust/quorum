//! Tests for `quorum done` (mailbox write).

use assert_cmd::Command;
use predicates::prelude::*;

fn quorum() -> Command {
    Command::cargo_bin("quorum").unwrap()
}

#[test]
fn done_writes_mailbox_row() {
    let home = tempfile::tempdir().unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .assert()
        .success();

    quorum()
        .env("QUORUM_HOME", home.path())
        .args(["done", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"))
        .stdout(predicate::str::contains("\"mailbox_id\""));
}

#[test]
fn done_with_verdict() {
    let home = tempfile::tempdir().unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .assert()
        .success();

    quorum()
        .env("QUORUM_HOME", home.path())
        .args([
            "done",
            "--agent",
            "Reviewer-1",
            "--pr",
            "55",
            "--verdict",
            "approved",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"));
}

#[test]
fn done_with_changes_and_feedback() {
    let home = tempfile::tempdir().unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .assert()
        .success();

    quorum()
        .env("QUORUM_HOME", home.path())
        .args([
            "done",
            "--agent",
            "Reviewer-2",
            "--pr",
            "60",
            "--verdict",
            "changes",
            "--feedback",
            "Fix the error handling in main.rs",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"));
}
