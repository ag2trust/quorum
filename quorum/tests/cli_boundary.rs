//! End-to-end daemon-only execution boundary tests.
//!
//! Locks the invariant that the public CLI surface cannot perform daemon-internal
//! operations (claim, release, renew, task-claim) and that submit/react require a
//! valid, non-revoked run capability with matching identity — failing loud (exit 2)
//! without mutating any DB state.
//!
//! The multi-process atomic task-selection race canary (race.rs) exercises the
//! internal library path that replaced the retired CLI commands; these tests verify
//! the public surface is sealed.

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

fn mailbox_count(home: &std::path::Path) -> i64 {
    let db = db_path(home);
    let conn = quorum_core::db::open(&db).unwrap();
    conn.query_row("SELECT count(*) FROM mailbox", [], |r| r.get(0))
        .unwrap()
}

// ===========================================================================
// §1 — Removed commands rejected at CLI parse (exit 2)
//
// These commands existed in v1 and were retired when the daemon took over
// task selection/claiming. They must not reappear as valid subcommands.
// ===========================================================================

#[test]
fn claim_rejected_as_unknown_subcommand() {
    quorum()
        .args([
            "claim", "--agent", "x", "--target", "task#1", "--ttl", "300",
        ])
        .assert()
        .code(2);
}

#[test]
fn release_rejected_as_unknown_subcommand() {
    quorum()
        .args(["release", "--agent", "x", "--target", "task#1"])
        .assert()
        .code(2);
}

#[test]
fn renew_rejected_as_unknown_subcommand() {
    quorum()
        .args([
            "renew", "--agent", "x", "--target", "task#1", "--ttl", "300",
        ])
        .assert()
        .code(2);
}

#[test]
fn claims_rejected_as_unknown_subcommand() {
    quorum().args(["claims"]).assert().code(2);
}

#[test]
fn task_claim_rejected_as_unknown_subcommand() {
    quorum()
        .args(["task-claim", "--agent", "x"])
        .assert()
        .code(2);
}

// ===========================================================================
// §2 — submit/react without run identity: exit 2, no mailbox mutation
// ===========================================================================

#[test]
fn submit_no_identity_writes_no_mailbox() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_RUN_ID")
        .args(["submit", "--agent", "Ghost", "--pr", "1"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("requires daemon run identity"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn react_no_identity_writes_no_mailbox() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_RUN_ID")
        .args([
            "react",
            "--agent",
            "Ghost",
            "--task-id",
            "1",
            "--state",
            "blocked",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("requires daemon run identity"));

    assert_eq!(mailbox_count(home.path()), 0);
}

// ===========================================================================
// §3 — Valid capability: worker submits assigned task
// ===========================================================================

#[test]
fn worker_with_valid_cap_can_submit() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-ok", 7, "Alpha", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-ok")
        .args(["submit", "--agent", "Alpha", "--pr", "100"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"ok\":true"));

    assert_eq!(mailbox_count(home.path()), 1);
}

#[test]
fn worker_react_enforces_task_scope() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-task7", 7, "Alpha", "worker");

    // react for assigned task (7) succeeds
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-task7")
        .args([
            "react",
            "--agent",
            "Alpha",
            "--task-id",
            "7",
            "--state",
            "note",
        ])
        .assert()
        .success();

    // react for different task (99) fails
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-task7")
        .args([
            "react",
            "--agent",
            "Alpha",
            "--task-id",
            "99",
            "--state",
            "blocked",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("task mismatch"));

    // only the first (successful) react wrote a mailbox row
    assert_eq!(mailbox_count(home.path()), 1);
}

// ===========================================================================
// §4 — Identity failures: no mutation on wrong-agent, wrong-role, unknown,
//       revoked, wrong-task, and replayed (post-revocation) run identities
// ===========================================================================

#[test]
fn submit_unknown_run_id_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "fabricated-id")
        .args(["submit", "--agent", "X", "--pr", "1"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("unknown run_id"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn submit_wrong_agent_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-real", 1, "RealAgent", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-real")
        .args(["submit", "--agent", "Impostor", "--pr", "1"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("agent mismatch"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn submit_wrong_role_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    // Capability issued as reviewer; submit without --verdict implies worker role
    issue_cap(home.path(), "run-rev", 1, "Agent-R", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-rev")
        .args(["submit", "--agent", "Agent-R", "--pr", "1"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("role mismatch"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn submit_revoked_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-dead", 1, "Agent-D", "worker");
    revoke_cap(home.path(), "run-dead");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-dead")
        .args(["submit", "--agent", "Agent-D", "--pr", "1"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("revoked"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn react_unknown_run_id_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "invented")
        .args([
            "react",
            "--agent",
            "X",
            "--task-id",
            "1",
            "--state",
            "blocked",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("unknown run_id"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn react_wrong_agent_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-w", 5, "TrueWorker", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-w")
        .args([
            "react",
            "--agent",
            "FakeWorker",
            "--task-id",
            "5",
            "--state",
            "failed",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("agent mismatch"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn react_revoked_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-rr", 5, "W", "worker");
    revoke_cap(home.path(), "run-rr");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-rr")
        .args(["react", "--agent", "W", "--task-id", "5", "--state", "note"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("revoked"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn react_wrong_task_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-t1", 1, "W", "worker");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-t1")
        .args([
            "react",
            "--agent",
            "W",
            "--task-id",
            "99",
            "--state",
            "blocked",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("task mismatch"));

    assert_eq!(mailbox_count(home.path()), 0);
}

#[test]
fn react_reviewer_role_rejected_no_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-reviewer", 10, "Rev", "reviewer");

    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-reviewer")
        .args([
            "react",
            "--agent",
            "Rev",
            "--task-id",
            "10",
            "--state",
            "blocked",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("role mismatch"));

    assert_eq!(mailbox_count(home.path()), 0);
}

// ===========================================================================
// §5 — Replayed identity: submit succeeds, then revoke, then replay fails
// ===========================================================================

#[test]
fn replayed_run_id_after_revocation_fails_without_mutation() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());
    issue_cap(home.path(), "run-once", 3, "Worker-1", "worker");

    // first submit succeeds
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-once")
        .args(["submit", "--agent", "Worker-1", "--pr", "50"])
        .assert()
        .success();

    assert_eq!(mailbox_count(home.path()), 1);

    // daemon revokes the cap (simulating post-process cleanup)
    revoke_cap(home.path(), "run-once");

    // replay attempt fails
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .env("QUORUM_RUN_ID", "run-once")
        .args(["submit", "--agent", "Worker-1", "--pr", "50"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("revoked"));

    // mailbox count unchanged from the one successful write
    assert_eq!(mailbox_count(home.path()), 1);
}

// ===========================================================================
// §6 — Race canary coexistence: internal claim API works, no public CLI path
//
// The race canary (race.rs) exercises quorum_core::tasks::claim via library
// calls with separate connections — identical to what the daemon does. This
// test verifies the library path is reachable from integration tests while
// the CLI surface remains sealed (§1 above confirms task-claim is gone).
// ===========================================================================

#[test]
fn internal_claim_api_available_without_cli_surface() {
    let home = tempfile::tempdir().unwrap();
    init(home.path());

    // Create a task via CLI (the only public way)
    quorum()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "boundary-task",
        ])
        .assert()
        .success();

    // Claim via internal API (what the daemon does)
    let db = db_path(home.path());
    let mut conn = quorum_core::db::open(&db).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let result =
        quorum_core::tasks::claim(&mut conn, "daemon-internal", Some(1), &[], 300, now).unwrap();
    assert!(result.is_some(), "internal claim API must succeed");

    // Second claim for same task loses (atomic)
    let result2 =
        quorum_core::tasks::claim(&mut conn, "second-agent", Some(1), &[], 300, now).unwrap();
    assert!(result2.is_none(), "second claim must lose the race");
}
