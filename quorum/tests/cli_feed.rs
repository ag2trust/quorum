//! Integration tests for the feed: delta reads, ack advancement, text safety, and the
//! NUL/invalid-UTF-8 rejection on `--body-file`.

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;
use std::io::Write;
use std::process::{Command as Proc, Stdio};

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

#[test]
fn negative_limit_is_usage_error() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["read", "--agent", "A", "--limit", "-1"])
        .assert()
        .code(2);
}

#[test]
fn concurrent_acks_leave_cursor_at_max() {
    // Project bar: stress concurrency, don't trust single-threaded green. N processes ack
    // out-of-order values; the monotonic MAX upsert must leave the cursor at the largest.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();

    let bin = assert_cmd::cargo::cargo_bin("quorum");
    let acks = [3i64, 1, 9, 2, 7, 4, 10, 5, 8, 6];
    let children: Vec<_> = acks
        .iter()
        .map(|a| {
            Proc::new(&bin)
                .env("QUORUM_HOME", home.path())
                .args(["read", "--agent", "B", "--ack-through", &a.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();
    for c in children {
        c.wait_with_output().unwrap();
    }

    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
    let last: i64 = conn
        .query_row(
            "SELECT last_seq FROM cursors WHERE agent_id='B' AND topic='hub'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        last, 10,
        "cursor must settle at the max ack despite concurrency"
    );
}
