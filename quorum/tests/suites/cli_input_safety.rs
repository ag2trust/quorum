//! Integration tests for text-safety validation across all body-accepting paths.
//!
//! The `validate` function in `input.rs` rejects embedded NUL bytes and invalid UTF-8. This
//! is unit-tested, but never integration-tested through the CLI stdin path. These tests pipe
//! bad bytes via `--body-stdin` to `post`, `task-create`, and `task-update`, asserting exit 2.

use crate::common;

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_AGENT")
        .env_remove("QUORUM_RUN_ID")
        .env_remove("QUORUM_AGENT_ENDPOINT");
    c
}

#[test]
fn post_rejects_nul_via_stdin() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "info", "--body-stdin"])
        .write_stdin("hello\0world")
        .assert()
        .code(2);
}

#[test]
fn post_rejects_invalid_utf8_via_stdin() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "info", "--body-stdin"])
        .write_stdin(vec![0xff_u8])
        .assert()
        .code(2);
}

#[test]
fn task_create_rejects_nul_via_stdin() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "t",
            "--body-stdin",
        ])
        .write_stdin("a\0b")
        .assert()
        .code(2);
}

#[test]
fn task_create_rejects_empty_and_whitespace_only_body_stdin() {
    // Explicit --body-stdin that resolves empty/whitespace must fail loudly (exit 2)
    // and must not create a task row. Omitting the body flag remains valid.
    let home = tempfile::tempdir().unwrap();
    for body in ["", "   ", "\n\t  \n"] {
        quorum(home.path())
            .args([
                "task-create",
                "--created-by",
                "boss",
                "--title",
                "empty-body",
                "--body-stdin",
            ])
            .write_stdin(body)
            .assert()
            .code(2);
    }
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
}

#[test]
fn task_create_rejects_empty_body_file() {
    let home = tempfile::tempdir().unwrap();
    let empty = home.path().join("empty-body.txt");
    std::fs::write(&empty, "  \n").unwrap();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "empty-body-file",
            "--body-file",
            empty.to_str().unwrap(),
        ])
        .assert()
        .code(2);
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
}

#[test]
fn task_create_allows_omitted_body() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "no body"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"id\":1"));
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"body\":null"));
}

#[test]
fn task_create_rejects_invalid_utf8_via_stdin() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "t",
            "--body-stdin",
        ])
        .write_stdin(vec![0xff_u8])
        .assert()
        .code(2);
}

#[test]
fn task_update_rejects_nul_via_body_stdin() {
    let home = tempfile::tempdir().unwrap();
    // Create + claim a task so task-update has a valid target.
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "t"])
        .assert()
        .success();
    common::claim_task(home.path(), "A", Some(1), 3600);
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--body-stdin",
        ])
        .write_stdin("a\0b")
        .assert()
        .code(2);
}

#[test]
fn task_update_rejects_invalid_utf8_via_body_stdin() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "t"])
        .assert()
        .success();
    common::claim_task(home.path(), "A", Some(1), 3600);
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--body-stdin",
        ])
        .write_stdin(vec![0xff_u8])
        .assert()
        .code(2);
}

#[test]
fn task_update_rejects_nul_via_note_stdin() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "t"])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--note-stdin",
        ])
        .write_stdin("note\0here")
        .assert()
        .code(2);
}
