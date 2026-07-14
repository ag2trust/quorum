//! Tests for `quorum submit` (mailbox write) and its deprecated `done` alias.

use assert_cmd::Command;
use predicates::prelude::*;

fn quorum() -> Command {
    Command::cargo_bin("quorum").unwrap()
}

fn init(home: &std::path::Path) {
    quorum()
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();
}

// --- `quorum submit` (canonical verb) ---

#[test]
fn submit_writes_mailbox_row() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["submit", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"))
        .stdout(predicate::str::contains("\"mailbox_id\""));
}

#[test]
fn submit_with_verdict() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "Reviewer-1",
            "--pr",
            "55",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"));
}

#[test]
fn submit_with_changes_and_feedback() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
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

// --- #206 verdict discipline ---

#[test]
fn submit_approved_with_blocking_findings_is_refused() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "Reviewer-1",
            "--pr",
            "198",
            "--verdict",
            "approved",
            "--blocking",
            "2",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--verdict changes"));
}

#[test]
fn submit_approved_without_blocking_attestation_is_refused() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "Reviewer-1",
            "--pr",
            "55",
            "--verdict",
            "approved",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--blocking 0"));
}

#[test]
fn submit_changes_without_feedback_is_refused() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "Reviewer-1",
            "--pr",
            "60",
            "--verdict",
            "changes",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--feedback"));
}

// --- `quorum done` (deprecated alias, must still work) ---

#[test]
fn done_alias_writes_mailbox_row() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["done", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"))
        .stdout(predicate::str::contains("\"mailbox_id\""));
}

#[test]
fn done_alias_with_verdict() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "done",
            "--agent",
            "Reviewer-1",
            "--pr",
            "55",
            "--verdict",
            "approved",
            "--blocking",
            "0",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"));
}
