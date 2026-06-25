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

    // update by assignee: the executor submits `done`
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--status",
            "done",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"done\""));

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
fn normal_misses_do_not_log_errors() {
    let home = tempfile::tempdir().unwrap();
    // claim with nothing open → exit 1
    quorum(home.path())
        .args(["task-claim", "--agent", "A"])
        .assert()
        .code(1);
    // create + claim, then a non-assignee update → exit 1
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
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
            "B",
            "--task-id",
            "1",
            "--status",
            "done",
        ])
        .assert()
        .code(1);
    // none of those normal misses are errors
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0, "normal exit-1 misses must not log errors");
}

#[test]
fn release_then_reclaim_hands_off_task() {
    // Hand-off under the lease model: the holder releases (→ open), then another agent claims.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-claim", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();
    // A gives it up → back to open, assignee cleared.
    quorum(home.path())
        .args(["task-release", "--agent", "A", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"open\""))
        .stdout(predicates::str::contains("\"assignee\":null"));
    // A no longer holds it → a second release is a clean miss (exit 1).
    quorum(home.path())
        .args(["task-release", "--agent", "A", "--task-id", "1"])
        .assert()
        .code(1);
    // B claims the now-open task and submits done; A (not assignee) cannot.
    quorum(home.path())
        .args(["task-claim", "--agent", "B", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"assignee\":\"B\""));
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
        .success();
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--status",
            "done",
        ])
        .assert()
        .code(1);
}

#[test]
fn renew_and_cancel_lifecycle() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "task-claim",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--ttl",
            "1h",
        ])
        .assert()
        .success();
    // Holder renews; a non-holder cannot.
    quorum(home.path())
        .args(["task-renew", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-renew", "--agent", "B", "--task-id", "1"])
        .assert()
        .code(1);
    // A stranger (neither creator nor assignee) cannot cancel...
    quorum(home.path())
        .args(["task-cancel", "--agent", "C", "--task-id", "1"])
        .assert()
        .code(1);
    // ...but the creator can. Terminal → a second cancel is a clean miss.
    quorum(home.path())
        .args(["task-cancel", "--agent", "boss", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"cancelled\""));
    quorum(home.path())
        .args(["task-cancel", "--agent", "boss", "--task-id", "1"])
        .assert()
        .code(1);
}

#[test]
fn reaper_reclaims_lapsed_lease_via_cli() {
    // End-to-end (real binary, real clock): a claimed task whose lease lapses is returned to
    // `open` by the next write's sweep-on-write reaper, with a `reclaimed` event on the feed.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "task-claim",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--ttl",
            "1s",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"claimed\""));
    // Let the 1s lease lapse, then make any write to trigger sweep-on-write.
    std::thread::sleep(std::time::Duration::from_millis(2100));
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "y"])
        .assert()
        .success();
    // Task 1 is back to open, assignee cleared.
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"open\""))
        .stdout(predicates::str::contains("\"assignee\":null"));
    // A reclaimed event was posted to the feed by the reaper.
    // (body is JSON-inside-JSON, so assert on quote-free substrings that survive escaping)
    quorum(home.path())
        .args(["peek"])
        .assert()
        .success()
        .stdout(predicates::str::contains("reclaimed"))
        .stdout(predicates::str::contains("lease lapsed"));
    // No errors logged (reaping is normal operation).
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0, "reaping must not log errors");
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

// -- --match-label (issue #1) -------------------------------------------------------------

#[test]
fn match_label_end_to_end() {
    let home = tempfile::tempdir().unwrap();
    // A high-priority task without the label and a low-priority one with it.
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "no-label",
            "--priority",
            "9",
        ])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "with-label",
            "--priority",
            "1",
            "--labels",
            r#"["tier:opus-47"]"#,
        ])
        .assert()
        .success();

    // --match-label restricts to the labeled task even though the other is higher-priority.
    quorum(home.path())
        .args([
            "task-claim",
            "--agent",
            "A",
            "--match-label",
            "tier:opus-47",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("with-label"))
        .stdout(predicates::str::contains("\"status\":\"claimed\""));

    // No more labeled tasks open → exit 1, clean reason.
    quorum(home.path())
        .args([
            "task-claim",
            "--agent",
            "B",
            "--match-label",
            "tier:opus-47",
        ])
        .assert()
        .code(1)
        .stdout(predicates::str::contains("no claimable task"));
}

#[test]
fn match_label_and_task_id_are_mutually_exclusive() {
    // clap rejects --task-id + --match-label at parse time (exit 2 = usage error). An explicit
    // --task-id is already a more specific selector than any label filter.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "task-claim",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--match-label",
            "k",
        ])
        .assert()
        .code(2);
}

#[test]
fn concurrent_match_label_claim_one_winner() {
    // Project bar (CLAUDE.md): stress concurrency. Spawn 12 processes all racing for the same
    // label-filtered task; the partial unique index + BEGIN IMMEDIATE must still give exactly
    // one winner — the WHERE label-filter doesn't change the atomicity gate.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "labeled",
            "--labels",
            r#"["k"]"#,
        ])
        .assert()
        .success();

    let bin = assert_cmd::cargo::cargo_bin("quorum");
    let children: Vec<_> = (0..12)
        .map(|i| {
            Proc::new(&bin)
                .env("QUORUM_HOME", home.path())
                .args([
                    "task-claim",
                    "--agent",
                    &format!("a{i}"),
                    "--match-label",
                    "k",
                ])
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
    assert_eq!(
        wins, 1,
        "label-filtered claim must still grant to exactly one process"
    );
}
