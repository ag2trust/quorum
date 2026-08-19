//! Local validation for managed completion commands.
//!
//! Endpoint-backed authority and mailbox effects are covered by
//! `agent_cli_endpoint.rs`; these checks deliberately run before any endpoint
//! connection is attempted.

use assert_cmd::Command;
use predicates::prelude::*;

fn quorum() -> Command {
    Command::cargo_bin("quorum").unwrap()
}

#[test]
fn submit_validates_verdict_before_connecting_to_the_endpoint() {
    quorum()
        .args([
            "submit",
            "--agent",
            "Reviewer",
            "--verdict",
            "approved",
            "--blocking",
            "1",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--verdict changes"));
}

#[test]
fn submit_requires_a_run_identity_before_connecting_to_the_endpoint() {
    quorum()
        .env_remove("QUORUM_RUN_ID")
        .args(["submit", "--agent", "Worker"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("requires daemon run identity"));
}

#[test]
fn react_validates_state_before_connecting_to_the_endpoint() {
    quorum()
        .args([
            "react",
            "--agent",
            "Worker",
            "--task-id",
            "1",
            "--state",
            "invalid",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--state must be"));
}
