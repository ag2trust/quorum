//! Integration tests for text-safety validation across all body-accepting paths.
//!
//! The `validate` function in `input.rs` rejects embedded NUL bytes and invalid UTF-8. This
//! is unit-tested, but never integration-tested through the CLI stdin path. These tests pipe
//! bad bytes via `--body-stdin` to `post`, `task-create`, and `task-update`, asserting exit 2.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
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
    quorum(home.path())
        .args(["task-claim", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();
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
    quorum(home.path())
        .args(["task-claim", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();
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
