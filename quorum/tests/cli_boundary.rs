//! Public command boundary checks not specific to the managed endpoint protocol.
//!
//! Endpoint authority, invalid capabilities, and mailbox non-mutation are
//! exercised with a live daemon in `agent_cli_endpoint.rs`.

use assert_cmd::Command;
use predicates::prelude::*;

fn quorum() -> Command {
    Command::cargo_bin("quorum").unwrap()
}

#[test]
fn retired_daemon_internal_commands_are_not_public() {
    for command in ["claim", "release", "renew", "claims", "task-claim"] {
        quorum().arg(command).assert().code(2);
    }
}

#[test]
fn managed_commands_require_run_identity_before_endpoint_io() {
    quorum()
        .env_remove("QUORUM_RUN_ID")
        .args(["submit", "--agent", "Worker"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("requires daemon run identity"));
    quorum()
        .env_remove("QUORUM_RUN_ID")
        .args([
            "react",
            "--agent",
            "Worker",
            "--task-id",
            "1",
            "--state",
            "note",
        ])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("requires daemon run identity"));
}
