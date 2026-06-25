//! Integration tests for `quorum roster` and agent presence.

use assert_cmd::Command;

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
    // First invocation as "A" (via a claim).
    quorum(home.path())
        .args(["claim", "--agent", "A", "--target", "pr#1", "--ttl", "1h"])
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
