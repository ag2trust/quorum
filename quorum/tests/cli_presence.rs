//! Integration tests for presence semantics.
//!
//! Presence is derived from `agents::touch`, which every write-taking command calls inside
//! its txn. These tests verify the three edge branches documented in the design notes:
//! (1) a lost race still bumps presence, (2) a pre-write usage error leaves no trace,
//! (3) pure-read commands never create/bump an agent row.

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn roster_empty_on_fresh_db() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .arg("roster")
        .assert()
        .success()
        .stdout(predicates::str::diff("[]\n"));
}

#[test]
fn name_collision_merges_presence_silently() {
    // Two separate CLI invocations using the same agent id should merge into one roster
    // entry (ON CONFLICT on agents.id), not create duplicates or error.
    let home = tempfile::tempdir().unwrap();
    // First invocation registering "A" (via a task-create — created_by bumps presence).
    quorum(home.path())
        .args(["task-create", "--created-by", "A", "--title", "x"])
        .assert()
        .success();
    // Second invocation as "A" (via a post) — same id, different "session."
    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "info", "--body-stdin"])
        .write_stdin("hello from session 2")
        .assert()
        .success();
    // Roster should show exactly one agent "A", not two.
    let out = quorum(home.path()).arg("roster").output().unwrap();
    let roster: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(roster.len(), 1, "expected 1 agent, got {}", roster.len());
    assert_eq!(roster[0]["id"], "A");
}

#[test]
fn lost_claim_still_marks_agent_online() {
    let home = tempfile::tempdir().unwrap();

    // One task for two agents to contend for.
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();

    // Winner takes the task lease.
    quorum(home.path())
        .args(["task-claim", "--agent", "Winner", "--task-id", "1"])
        .assert()
        .success();

    // Loser loses (exit 1, task already claimed) — but touch ran inside the txn first.
    quorum(home.path())
        .args(["task-claim", "--agent", "Loser", "--task-id", "1"])
        .assert()
        .code(1);

    // Both agents must appear in the roster.
    quorum(home.path())
        .arg("roster")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"Loser\""))
        .stdout(predicates::str::contains("\"Winner\""));
}

#[test]
fn pre_write_usage_error_leaves_no_trace() {
    let home = tempfile::tempdir().unwrap();

    // `post --kind bogus` fails validation (exit 2) before the txn/touch.
    quorum(home.path())
        .args(["post", "--agent", "Ghost", "--kind", "bogus"])
        .arg("--body-stdin")
        .write_stdin("hello\n")
        .assert()
        .code(2);

    // Ghost must NOT appear in the roster — touch never ran.
    quorum(home.path())
        .arg("roster")
        .assert()
        .success()
        .stdout(predicates::str::diff("[]\n"));
}

#[test]
fn pure_reads_do_not_create_agent_row() {
    let home = tempfile::tempdir().unwrap();

    // Seed the DB with one write so there's data to read.
    quorum(home.path())
        .args(["post", "--agent", "Writer", "--kind", "info"])
        .arg("--body-stdin")
        .write_stdin("seed message\n")
        .assert()
        .success();

    // A different agent performs every pure-read command.
    // read WITHOUT --ack-through (pure read, no cursor write).
    quorum(home.path())
        .args(["read", "--agent", "Reader"])
        .assert()
        .success();

    // peek (always read-only, no agent param).
    quorum(home.path()).args(["peek"]).assert().success();

    // task-list (read-only listing).
    quorum(home.path()).args(["task-list"]).assert().success();

    // status --json (read-only snapshot).
    quorum(home.path())
        .args(["status", "--json"])
        .assert()
        .success();

    // log (read-only event log).
    quorum(home.path()).args(["log"]).assert().success();

    // roster itself (read-only).
    quorum(home.path()).args(["roster"]).assert().success();

    // Only "Writer" should be in the roster — "Reader" must NOT appear.
    quorum(home.path())
        .arg("roster")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"Writer\""))
        .stdout(predicates::str::contains("\"Reader\"").not());
}
