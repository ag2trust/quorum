//! Integration tests for the feed: delta reads, ack advancement, text safety, and the
//! NUL/invalid-UTF-8 rejection on `--body-file`.

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;
use std::io::Write;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn post_read_ack_delta_flow() {
    let home = tempfile::tempdir().unwrap();

    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "info"])
        .arg("--body-stdin")
        .write_stdin("first \"msg\" $x\n")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"seq\":1"));

    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "info"])
        .arg("--body-stdin")
        .write_stdin("second\n")
        .assert()
        .success();

    // B reads both (no ack), byte-exact body preserved
    quorum(home.path())
        .args(["read", "--agent", "B"])
        .assert()
        .success()
        .stdout(predicates::str::contains("first \\\"msg\\\" $x"))
        .stdout(predicates::str::contains("second"));

    // B acks through seq 1 → next read returns only the second message
    quorum(home.path())
        .args(["read", "--agent", "B", "--ack-through", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("second"))
        .stdout(predicates::str::contains("first").not());
}

#[test]
fn post_rejects_invalid_kind() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "shout"])
        .arg("--body-stdin")
        .write_stdin("x")
        .assert()
        .code(2);
}

#[test]
fn body_file_rejects_nul_and_bad_utf8() {
    let home = tempfile::tempdir().unwrap();

    let mut nul = tempfile::NamedTempFile::new().unwrap();
    nul.write_all(&[b'a', 0, b'b']).unwrap();
    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "info", "--body-file"])
        .arg(nul.path())
        .assert()
        .code(2);

    let mut bad = tempfile::NamedTempFile::new().unwrap();
    bad.write_all(&[0xff, 0xfe]).unwrap();
    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "info", "--body-file"])
        .arg(bad.path())
        .assert()
        .code(2);
}

#[test]
fn post_without_body_is_usage_error() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["post", "--agent", "A", "--kind", "info"])
        .assert()
        .code(2);
}
