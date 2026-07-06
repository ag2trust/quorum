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
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();

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
fn done_with_verdict() {
    let home = tempfile::tempdir().unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();

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

#[test]
fn done_with_changes_and_feedback() {
    let home = tempfile::tempdir().unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
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

// --- #206 verdict discipline: the verdict must match the review's findings ---

/// #198 regression: the reviewer wrote two BLOCKING findings and then signaled
/// `approved`; the merge shipped the bugs (#205). An approve carrying a
/// nonzero blocking count must be refused at the CLI.
#[test]
fn done_approved_with_blocking_findings_is_refused() {
    let home = tempfile::tempdir().unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "done",
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
fn done_approved_without_blocking_attestation_is_refused() {
    let home = tempfile::tempdir().unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();

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
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--blocking 0"));
}

#[test]
fn done_changes_without_feedback_is_refused() {
    let home = tempfile::tempdir().unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "done",
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
