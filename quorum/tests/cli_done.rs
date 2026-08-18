//! Tests for `quorum submit` and `quorum react` — daemon run identity is required.

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

fn db_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join("repos").join("test__repo").join("quorum.db")
}

fn issue_cap(home: &std::path::Path, run_id: &str, task_id: i64, agent: &str, role: &str) {
    let db = db_path(home);
    let mut conn = quorum_core::db::open(&db).unwrap();
    quorum_core::capabilities::issue(&mut conn, run_id, task_id, agent, role, 1000).unwrap();
}

fn revoke_cap(home: &std::path::Path, run_id: &str) {
    let db = db_path(home);
    let mut conn = quorum_core::db::open(&db).unwrap();
    quorum_core::capabilities::revoke(&mut conn, run_id, 2000).unwrap();
}

// ---------------------------------------------------------------------------
// submit — happy paths
// ---------------------------------------------------------------------------

#[test]
fn submit_writes_mailbox_row() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-w1", 1, "TestAgent", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-w1")
        .args(["submit", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"))
        .stdout(predicate::str::contains("\"mailbox_id\""));

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let rows = quorum_core::mailbox::poll_unconsumed(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.task_id,
        Some(1),
        "submit must bind the Done row to its validated run capability"
    );
    assert_eq!(rows[0].1.agent, "TestAgent");
    assert_eq!(rows[0].1.kind, quorum_core::mailbox::MailboxKind::Done);
}

#[test]
fn submit_with_verdict() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-r1", 1, "Reviewer-1", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-r1")
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
fn submit_graph_blocker_writes_closed_capability_bound_payload() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "graph-run", 7, "BoundaryReviewer", "reviewer");
    let feedback = r#"{"category":"boundary-violation","affected_task":7,"violated_assigned_boundary":"parser-only child","evidence":["diff changes sibling-owned schema.sql"]}"#;

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "graph-run")
        .args([
            "submit",
            "--agent",
            "BoundaryReviewer",
            "--pr",
            "71",
            "--verdict",
            "graph-blocker",
            "--feedback-json",
            feedback,
        ])
        .assert()
        .success();

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let rows = quorum_core::mailbox::poll_unconsumed(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0].1;
    assert_eq!(row.task_id, Some(7));
    assert_eq!(row.verdict.as_deref(), Some("graph-blocker"));
    let payload: serde_json::Value = serde_json::from_str(row.payload.as_deref().unwrap()).unwrap();
    assert_eq!(payload["run_id"], "graph-run");
    assert_eq!(payload["feedback"]["affected_task"], 7);
    assert_eq!(payload["feedback"]["category"], "boundary-violation");
}

#[test]
fn submit_graph_blocker_rejects_wrong_task_unknown_fields_and_wrong_role() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "graph-review", 7, "Reviewer", "reviewer");
    issue_cap(home.path(), "graph-worker", 7, "Worker", "worker");
    let valid = r#"{"category":"boundary-violation","affected_task":8,"violated_assigned_boundary":"parser-only child","evidence":["diff changes schema.sql"]}"#;

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "graph-review")
        .args([
            "submit",
            "--agent",
            "Reviewer",
            "--pr",
            "71",
            "--verdict",
            "graph-blocker",
            "--feedback-json",
            valid,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("does not match reviewer task"));

    let unknown = valid.replace("}", ",\"unknown\":true}");
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "graph-review")
        .args([
            "submit",
            "--agent",
            "Reviewer",
            "--pr",
            "71",
            "--verdict",
            "graph-blocker",
            "--feedback-json",
            &unknown,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid --feedback-json"));

    let worker_feedback = valid.replace(":8", ":7");
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "graph-worker")
        .args([
            "submit",
            "--agent",
            "Worker",
            "--pr",
            "71",
            "--verdict",
            "graph-blocker",
            "--feedback-json",
            &worker_feedback,
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("role mismatch"));

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    assert!(quorum_core::mailbox::poll_unconsumed(&conn)
        .unwrap()
        .is_empty());
}

#[test]
fn submit_with_changes_and_feedback() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-r2", 1, "Reviewer-2", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-r2")
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

#[test]
fn submit_with_changes_and_feedback_file() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-r2-file", 1, "Reviewer-2", "reviewer");
    let feedback = home.path().join("feedback.txt");
    std::fs::write(
        &feedback,
        "Fix the quoted `$value` handling\nand its negative path.",
    )
    .unwrap();

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-r2-file")
        .args([
            "submit",
            "--agent",
            "Reviewer-2",
            "--pr",
            "60",
            "--verdict",
            "changes",
            "--feedback-file",
        ])
        .arg(&feedback)
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"));

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let rows = quorum_core::mailbox::poll_unconsumed(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.feedback.as_deref(),
        Some("Fix the quoted `$value` handling\nand its negative path."),
        "feedback-file text must reach the mailbox unchanged"
    );
}

#[test]
fn submit_explicit_run_id_flag_overrides_env() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-flag", 1, "TestAgent", "worker");
    issue_cap(home.path(), "run-env", 2, "TestAgent", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-env")
        .args([
            "submit",
            "--agent",
            "TestAgent",
            "--pr",
            "42",
            "--run-id",
            "run-flag",
        ])
        .assert()
        .success();
}

// ---------------------------------------------------------------------------
// submit — #206 verdict discipline (still checked before capability)
// ---------------------------------------------------------------------------

#[test]
fn submit_approved_with_blocking_findings_is_refused() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-vd1", 1, "Reviewer-1", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-vd1")
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
    issue_cap(home.path(), "run-vd2", 1, "Reviewer-1", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-vd2")
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
    issue_cap(home.path(), "run-vd3", 1, "Reviewer-1", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-vd3")
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

#[test]
fn submit_feedback_file_requires_changes_verdict_before_file_io() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    let missing = home.path().join("missing-feedback.txt");

    for verdict_args in [vec![], vec!["--verdict", "approved"]] {
        let mut args = vec![
            "submit",
            "--agent",
            "Reviewer-1",
            "--feedback-file",
            missing.to_str().unwrap(),
        ];
        args.extend(verdict_args);
        quorum()
            .env("QUORUM_HOME", home.path())
            .env("QUORUM_REPO", "test/repo")
            .args(args)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "--feedback-file/--feedback requires --verdict changes",
            ))
            .stderr(predicate::str::contains("failed to read").not());
    }

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "submit",
            "--agent",
            "Reviewer-1",
            "--verdict",
            "invalid",
            "--feedback-file",
            missing.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "--verdict must be 'approved', 'changes', or 'graph-blocker'",
        ))
        .stderr(predicate::str::contains("failed to read").not());
}

// ---------------------------------------------------------------------------
// submit — identity failures (exit 2)
// ---------------------------------------------------------------------------

#[test]
fn submit_without_run_identity_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_RUN_ID")
        .args(["submit", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("requires daemon run identity"));
}

#[test]
fn submit_with_unknown_run_id_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "nonexistent-run-id")
        .args(["submit", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown run_id"));
}

#[test]
fn submit_with_revoked_run_id_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-revoked", 1, "TestAgent", "worker");
    revoke_cap(home.path(), "run-revoked");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-revoked")
        .args(["submit", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("revoked"));
}

#[test]
fn submit_with_mismatched_agent_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-mismatch", 1, "OtherAgent", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-mismatch")
        .args(["submit", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent mismatch"));
}

#[test]
fn submit_with_mismatched_role_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-wrong-role", 1, "TestAgent", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-wrong-role")
        .args(["submit", "--agent", "TestAgent", "--pr", "42"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("role mismatch"));
}

#[test]
fn submit_with_valid_run_id_succeeds() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-valid", 1, "TestAgent", "worker");

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

// ---------------------------------------------------------------------------
// review-draft — non-authoritative reviewer continuation signal
// ---------------------------------------------------------------------------

fn write_draft_feedback(home: &std::path::Path, body: &[u8]) -> std::path::PathBuf {
    let path = home.join("draft-feedback.txt");
    std::fs::write(&path, body).unwrap();
    path
}

#[test]
fn review_draft_requires_matching_reviewer_run_identity() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    let feedback = write_draft_feedback(home.path(), b"Need another analysis pass.");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_RUN_ID")
        .args([
            "review-draft",
            "--agent",
            "Reviewer",
            "--pr",
            "60",
            "--blocking",
            "1",
            "--feedback-file",
        ])
        .arg(&feedback)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("requires daemon run identity"));

    issue_cap(home.path(), "worker-run", 8, "Worker", "worker");
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "worker-run")
        .args([
            "review-draft",
            "--agent",
            "Worker",
            "--pr",
            "60",
            "--blocking",
            "1",
            "--feedback-file",
        ])
        .arg(&feedback)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("role mismatch"));

    issue_cap(
        home.path(),
        "foreign-review-run",
        9,
        "OtherReviewer",
        "reviewer",
    );
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "foreign-review-run")
        .args([
            "review-draft",
            "--agent",
            "Reviewer",
            "--pr",
            "60",
            "--blocking",
            "1",
            "--feedback-file",
        ])
        .arg(&feedback)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("agent mismatch"));

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    assert!(quorum_core::mailbox::poll_unconsumed(&conn)
        .unwrap()
        .is_empty());
}

#[test]
fn review_draft_rejects_zero_or_invalid_feedback_before_mailbox_write() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "review-run", 8, "Reviewer", "reviewer");
    let valid = write_draft_feedback(home.path(), b"Need another analysis pass.");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "review-run")
        .args([
            "review-draft",
            "--agent",
            "Reviewer",
            "--pr",
            "60",
            "--blocking",
            "0",
            "--feedback-file",
        ])
        .arg(&valid)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("positive --blocking"));

    let missing = home.path().join("missing-feedback.txt");
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "review-run")
        .args([
            "review-draft",
            "--agent",
            "Reviewer",
            "--pr",
            "60",
            "--blocking",
            "1",
            "--feedback-file",
        ])
        .arg(&missing)
        .assert()
        .code(2);

    let invalid = write_draft_feedback(home.path(), b"bad\0summary");
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "review-run")
        .args([
            "review-draft",
            "--agent",
            "Reviewer",
            "--pr",
            "60",
            "--blocking",
            "1",
            "--feedback-file",
        ])
        .arg(&invalid)
        .assert()
        .code(2)
        .stderr(predicate::str::contains("embedded NUL"));

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    assert!(quorum_core::mailbox::poll_unconsumed(&conn)
        .unwrap()
        .is_empty());
}

#[test]
fn review_draft_writes_capability_bound_distinct_mailbox_row() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "review-run", 8, "Reviewer", "reviewer");
    let feedback =
        write_draft_feedback(home.path(), b"Check the cancellation path before deciding.");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "review-run")
        .args([
            "review-draft",
            "--agent",
            "Reviewer",
            "--pr",
            "60",
            "--blocking",
            "2",
            "--feedback-file",
        ])
        .arg(&feedback)
        .assert()
        .success();

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    let rows = quorum_core::mailbox::poll_unconsumed(&conn).unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0].1;
    assert_eq!(row.kind, quorum_core::mailbox::MailboxKind::ReviewDraft);
    assert_eq!(row.task_id, Some(8));
    assert_eq!(row.pr, Some(60));
    assert_eq!(row.verdict, None);
    assert_eq!(
        row.feedback.as_deref(),
        Some("Check the cancellation path before deciding.")
    );
    assert_eq!(row.payload.as_deref(), Some("{\"blocking\":2}"));
}

// ---------------------------------------------------------------------------
// `quorum done` (deprecated alias, must still work)
// ---------------------------------------------------------------------------

#[test]
fn done_alias_writes_mailbox_row() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-done1", 1, "TestAgent", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-done1")
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
    issue_cap(home.path(), "run-done2", 1, "Reviewer-1", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-done2")
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

// ---------------------------------------------------------------------------
// react — happy path
// ---------------------------------------------------------------------------

#[test]
fn react_with_valid_capability_succeeds() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-react1", 42, "Worker-1", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-react1")
        .args([
            "react",
            "--agent",
            "Worker-1",
            "--task-id",
            "42",
            "--state",
            "blocked",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"));
}

#[test]
fn react_explicit_run_id_flag() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-react-flag", 10, "Worker-1", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_RUN_ID")
        .args([
            "react",
            "--agent",
            "Worker-1",
            "--task-id",
            "10",
            "--state",
            "note",
            "--run-id",
            "run-react-flag",
        ])
        .assert()
        .success();
}

// ---------------------------------------------------------------------------
// react — identity failures (exit 2)
// ---------------------------------------------------------------------------

#[test]
fn react_without_run_identity_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_RUN_ID")
        .args([
            "react",
            "--agent",
            "Worker-1",
            "--task-id",
            "42",
            "--state",
            "blocked",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("requires daemon run identity"));
}

#[test]
fn react_with_unknown_run_id_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "ghost-run")
        .args([
            "react",
            "--agent",
            "Worker-1",
            "--task-id",
            "42",
            "--state",
            "blocked",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown run_id"));
}

#[test]
fn react_with_revoked_run_id_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-react-rev", 42, "Worker-1", "worker");
    revoke_cap(home.path(), "run-react-rev");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-react-rev")
        .args([
            "react",
            "--agent",
            "Worker-1",
            "--task-id",
            "42",
            "--state",
            "failed",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("revoked"));
}

#[test]
fn react_with_wrong_agent_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-react-ag", 42, "RealWorker", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-react-ag")
        .args([
            "react",
            "--agent",
            "Impostor",
            "--task-id",
            "42",
            "--state",
            "blocked",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("agent mismatch"));
}

#[test]
fn react_with_wrong_task_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-react-tid", 42, "Worker-1", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-react-tid")
        .args([
            "react",
            "--agent",
            "Worker-1",
            "--task-id",
            "99",
            "--state",
            "blocked",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("task mismatch"));
}

#[test]
fn react_with_reviewer_role_fails() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-react-role", 42, "Rev-1", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-react-role")
        .args([
            "react",
            "--agent",
            "Rev-1",
            "--task-id",
            "42",
            "--state",
            "blocked",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("role mismatch"));
}

#[test]
fn react_invalid_state_still_rejected() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-react-st", 42, "Worker-1", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-react-st")
        .args([
            "react",
            "--agent",
            "Worker-1",
            "--task-id",
            "42",
            "--state",
            "invalid",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--state must be"));
}
