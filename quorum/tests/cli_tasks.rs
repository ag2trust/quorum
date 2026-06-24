//! Integration tests for the task commands, including the concurrent-claim single-winner
//! property and the body-via-stdin text path.

use assert_cmd::Command;
use std::process::{Command as Proc, Stdio};

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn create_claim_update_flow() {
    let home = tempfile::tempdir().unwrap();

    // create with a body piped on stdin (exercises the text-safety path)
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "fix bug"])
        .arg("--body-stdin")
        .write_stdin("multi\nline \"body\" with $vars\n")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"id\":1"));

    // claim highest-priority open
    quorum(home.path())
        .args(["task-claim", "--agent", "A"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"assignee\":\"A\""));

    // update by assignee
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--status",
            "in_progress",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"in_progress\""));

    // update by non-assignee fails loud (exit 1)
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "B",
            "--task-id",
            "1",
            "--status",
            "done",
        ])
        .assert()
        .code(1);

    // body round-tripped byte-exact
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains(
            "multi\\nline \\\"body\\\" with $vars",
        ));
}

#[test]
fn concurrent_task_claim_one_winner() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "single"])
        .assert()
        .success();

    let bin = assert_cmd::cargo::cargo_bin("quorum");
    let children: Vec<_> = (0..12)
        .map(|i| {
            Proc::new(&bin)
                .env("QUORUM_HOME", home.path())
                .args(["task-claim", "--agent", &format!("a{i}"), "--task-id", "1"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    let wins = children
        .into_iter()
        .map(|c| c.wait_with_output().unwrap())
        .filter(|o| o.status.success())
        .count();
    assert_eq!(wins, 1, "exactly one process may claim the task");
}
