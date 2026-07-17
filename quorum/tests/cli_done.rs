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

// --- #130 capability validation ---

#[test]
fn submit_with_unknown_run_id_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "TestAgent",
            "--pr",
            "42",
            "--run-id",
            "nonexistent-run-id",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown run_id"));
}

#[test]
fn submit_with_revoked_run_id_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        quorum_core::capabilities::issue(&mut conn, "run-revoked", 1, "TestAgent", "worker", 1000)
            .unwrap();
        quorum_core::capabilities::revoke(&mut conn, "run-revoked", 2000).unwrap();
    }

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "TestAgent",
            "--pr",
            "42",
            "--run-id",
            "run-revoked",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("revoked"));
}

#[test]
fn submit_with_mismatched_agent_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        quorum_core::capabilities::issue(
            &mut conn,
            "run-mismatch",
            1,
            "OtherAgent",
            "worker",
            1000,
        )
        .unwrap();
    }

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "TestAgent",
            "--pr",
            "42",
            "--run-id",
            "run-mismatch",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("does not match"));
}

#[test]
fn submit_with_valid_run_id_succeeds() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    let db_path = home
        .path()
        .join("repos")
        .join("test__repo")
        .join("quorum.db");
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        quorum_core::capabilities::issue(&mut conn, "run-valid", 1, "TestAgent", "worker", 1000)
            .unwrap();
    }

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "TestAgent",
            "--pr",
            "42",
            "--run-id",
            "run-valid",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"));
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
