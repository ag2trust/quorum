//! Integration tests for the task commands, including the concurrent-claim single-winner
//! property and the body-via-stdin text path.
//!
//! `task-claim` was removed from the public CLI (#161); tests now claim via the internal
//! `quorum_core::tasks::claim` function through a shared helper.

mod common;

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home).env("QUORUM_REPO", "test/repo");
    c
}

// -- removed CLI entry points (#161) -------------------------------------------------

#[test]
fn task_claim_cli_rejected_at_parse() {
    // task-claim was removed from the public CLI surface (#161). The binary must reject it
    // at arg parsing (exit 2), not silently accept or route it.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-claim", "--agent", "A"])
        .assert()
        .code(2);
}

#[test]
fn legacy_claim_commands_all_rejected() {
    // All legacy claim entry points (claim, release, renew, claims, task-claim) removed by
    // PR #85 and #161 — verify they all fail at parse (exit 2).
    let home = tempfile::tempdir().unwrap();
    for cmd in ["claim", "release", "renew", "claims", "task-claim"] {
        quorum(home.path()).arg(cmd).assert().code(2);
    }
}

// -- task lifecycle -------------------------------------------------------------------

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

    // claim highest-priority open (via internal lib — task-claim CLI removed #161)
    common::claim_task(home.path(), "A", None, 3600);

    // `done` is lifecycle-only — task-update rejects it with exit 2
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
        .code(2);

    // close manually via task-close
    quorum(home.path())
        .args([
            "task-close",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--reason-stdin",
        ])
        .write_stdin("done manually\n")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"done\""));

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
    // claim with nothing open → None (no claimable task)
    assert!(common::try_claim_task(home.path(), "A", None, 3600).is_none());
    // create + claim, then a non-assignee cancel → exit 1 (not holder/creator)
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    common::claim_task(home.path(), "A", Some(1), 3600);
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "B",
            "--task-id",
            "1",
            "--status",
            "cancelled",
        ])
        .assert()
        .code(1);
    // none of those normal misses are errors
    let conn = quorum_core::db::open(&home.path().join("repos/test__repo/quorum.db")).unwrap();
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
    common::claim_task(home.path(), "A", Some(1), 3600);
    // A gives it up → back to open, assignee cleared.
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--status",
            "open",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"open\""))
        .stdout(predicates::str::contains("\"assignee\":null"));
    // A no longer holds it → a second release is a clean miss (exit 1).
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--status",
            "open",
        ])
        .assert()
        .code(1);
    // B claims the now-open task and cancels it; A (not assignee/creator) cannot.
    common::claim_task(home.path(), "B", Some(1), 3600);
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "B",
            "--task-id",
            "1",
            "--status",
            "cancelled",
        ])
        .assert()
        .success();
}

#[test]
fn cancel_lifecycle() {
    // `task-renew` was removed in #55 (auto-renew on agent touch). The lease-extend path
    // is now exercised by any --agent command (covered in agents::touch unit tests). This
    // test focuses on the cancel half of the original lifecycle test.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    common::claim_task(home.path(), "A", Some(1), 3600);
    // A stranger (neither creator nor assignee) cannot cancel...
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "C",
            "--task-id",
            "1",
            "--status",
            "cancelled",
        ])
        .assert()
        .code(1);
    // ...but the creator can. Terminal → a second cancel is a clean miss.
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "boss",
            "--task-id",
            "1",
            "--status",
            "cancelled",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"cancelled\""));
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "boss",
            "--task-id",
            "1",
            "--status",
            "cancelled",
        ])
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
    common::claim_task(home.path(), "A", Some(1), 1);
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
    // A `task_reclaimed` event was posted to the EVENT LOG by the reaper (not the message
    // feed — events live separate from messaging per issue #4).
    quorum(home.path())
        .args(["log", "--refs", "task#1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"kind\":\"task_reclaimed\""))
        .stdout(predicates::str::contains("lease lapsed"));
    // And the message feed is NOT polluted with auto-events.
    quorum(home.path())
        .args(["read"])
        .assert()
        .success()
        .stdout(predicates::str::contains("reclaimed").not());
    // No errors logged (reaping is normal operation).
    let conn = quorum_core::db::open(&home.path().join("repos/test__repo/quorum.db")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0, "reaping must not log errors");
}

#[test]
fn notes_round_trip_byte_exact_and_any_agent_can_add() {
    let home = tempfile::tempdir().unwrap();
    // create + claim by A
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    common::claim_task(home.path(), "A", Some(1), 3600);

    // A leaves a note via stdin — heredoc-style content with $vars + backticks + newlines
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--note-stdin",
        ])
        .write_stdin("step 1: $hello\n`backtick`\nmulti\n")
        .assert()
        .success();

    // B (NOT the assignee) can still leave a note — no assignee guard on notes (the
    // contract differentiator vs `--status done` which IS assignee-gated under #14).
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "B",
            "--task-id",
            "1",
            "--note-stdin",
        ])
        .write_stdin("watcher sees rough edge in step 1\n")
        .assert()
        .success();

    // task-get returns both notes in insertion order, byte-exact
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"notes\":["))
        .stdout(predicates::str::contains("\"agent\":\"A\""))
        .stdout(predicates::str::contains("\"agent\":\"B\""))
        .stdout(predicates::str::contains(
            "step 1: $hello\\n`backtick`\\nmulti",
        ))
        .stdout(predicates::str::contains(
            "watcher sees rough edge in step 1",
        ));
}

#[test]
fn note_combinable_with_cancel() {
    // --note-* IS combinable with --status cancelled, so the agent can cancel + leave a
    // breadcrumb in one call.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
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
            "--status",
            "cancelled",
            "--note-stdin",
        ])
        .write_stdin("won't do: see PR #123\n")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"cancelled\""));
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("won't do: see PR #123"));
}

#[test]
fn note_with_status_done_rejected_before_note() {
    // --status done is rejected at validation (exit 2) before the note is applied.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
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
            "--status",
            "done",
            "--note-stdin",
        ])
        .write_stdin("shouldnt land\n")
        .assert()
        .code(2);
    // Verify: the note was not added, the task is still working.
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"working\""))
        .stdout(predicates::str::contains("\"notes\":[]"))
        .stdout(predicates::str::contains("shouldnt land").not());
}

#[test]
fn note_on_missing_task_is_exit_1_not_an_error() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "9999",
            "--note-stdin",
        ])
        .write_stdin("into the void\n")
        .assert()
        .code(1);
    // and nothing logged to errors
    let conn = quorum_core::db::open(&home.path().join("repos/test__repo/quorum.db")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn task_update_without_any_change_is_usage_error() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "x"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-update", "--agent", "A", "--task-id", "1"])
        .assert()
        .code(2);
}

#[test]
fn body_stdin_and_note_stdin_conflict_at_parse() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "task-update",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--body-stdin",
            "--note-stdin",
        ])
        .assert()
        .code(2);
}

#[test]
fn concurrent_task_claim_one_winner() {
    // Multi-threaded variant: each thread opens its own connection and races
    // quorum_core::tasks::claim. SQLite file locking guarantees exactly-one-winner
    // the same way the former multi-process CLI test did.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "single"])
        .assert()
        .success();

    let db_path = home.path().join("repos/test__repo/quorum.db");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let handles: Vec<_> = (0..12)
        .map(|i| {
            let db = db_path.clone();
            std::thread::spawn(move || {
                let mut conn = quorum_core::db::open(&db).unwrap();
                quorum_core::tasks::claim(&mut conn, &format!("a{i}"), Some(1), &[], 300, now)
                    .unwrap()
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wins = results.iter().filter(|r| r.is_some()).count();
    assert_eq!(wins, 1, "exactly one thread may claim the task");

    let conn = quorum_core::db::open(&db_path).unwrap();
    let active: i64 = conn
        .query_row(
            "SELECT count(*) FROM claims WHERE target='task#1' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 1, "exactly one active lease row for task#1");

    let errs: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(errs, 0, "a normal race must not log any errors");
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
    let claimed = common::claim_task_with_labels(home.path(), "A", &["tier:opus-47"], 3600);
    assert!(claimed.is_some(), "label-matched claim should succeed");
    quorum(home.path())
        .args(["task-get", "--task-id", "2"])
        .assert()
        .success()
        .stdout(predicates::str::contains("with-label"));

    // No more labeled tasks open → None.
    let miss = common::claim_task_with_labels(home.path(), "B", &["tier:opus-47"], 3600);
    assert!(miss.is_none(), "no more labeled tasks open");
}

#[test]
fn concurrent_match_label_claim_one_winner() {
    // Multi-threaded variant: 12 threads race label-filtered claim on one task.
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

    let db_path = home.path().join("repos/test__repo/quorum.db");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let handles: Vec<_> = (0..12)
        .map(|i| {
            let db = db_path.clone();
            std::thread::spawn(move || {
                let mut conn = quorum_core::db::open(&db).unwrap();
                quorum_core::tasks::claim(&mut conn, &format!("a{i}"), None, &["k"], 300, now)
                    .unwrap()
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wins = results.iter().filter(|r| r.is_some()).count();
    assert_eq!(
        wins, 1,
        "label-filtered claim must still grant to exactly one thread"
    );

    let conn = quorum_core::db::open(&db_path).unwrap();
    let active: i64 = conn
        .query_row(
            "SELECT count(*) FROM claims WHERE target='task#1' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 1, "exactly one active lease row for task#1");
    let errs: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(errs, 0, "a normal race must not log any errors");
}

// -- task dependencies (issue #2) ---------------------------------------------------------

#[test]
fn task_create_rejects_malformed_depends_on() {
    // Cobble-x7M's blocking finding on #18 v1, asserted at the CLI boundary: a typo like
    // `"1,2"` (no brackets) MUST exit non-zero AND not create the row. Otherwise the bad
    // row would poison every subsequent task-list/task-get/task-cancel.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "bad",
            "--depends-on",
            "1,2",
        ])
        .assert()
        .code(2)
        .stderr(predicates::str::contains("depends-on"));
    // task-list still works (proves the queue isn't poisoned) and shows no rows.
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
}

#[test]
fn depends_on_gates_claim_end_to_end() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "dep"])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "dependent",
            "--depends-on",
            "[1]",
        ])
        .assert()
        .success();

    // Auto-pick claims the dep (id 1); dependent stays gated.
    let picked = common::try_claim_task(home.path(), "A", None, 3600);
    assert_eq!(picked.as_ref().unwrap().id, 1);

    // No more claimable tasks: dependent is gated, dep is claimed.
    assert!(common::try_claim_task(home.path(), "B", None, 3600).is_none());

    // Even an explicit task-id can't pull the gated dependent.
    assert!(common::try_claim_task(home.path(), "B", Some(2), 3600).is_none());

    // Mark dep as done via task-close — `done` ungates dependents.
    quorum(home.path())
        .args([
            "task-close",
            "--agent",
            "A",
            "--task-id",
            "1",
            "--reason-stdin",
        ])
        .write_stdin("completed\n")
        .assert()
        .success();
    // Now the dependent (task 2) is unblocked — B can claim it.
    let unblocked = common::try_claim_task(home.path(), "B", None, 3600);
    assert_eq!(unblocked.as_ref().unwrap().id, 2);
}

#[test]
fn task_get_surfaces_depends_on_and_ready() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "dep"])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "dependent",
            "--depends-on",
            "[1]",
        ])
        .assert()
        .success();

    // No-deps task → ready=true, depends_on=null.
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"ready\":true"))
        .stdout(predicates::str::contains("\"depends_on\":null"));

    // With unmet dep → ready=false, depends_on="[1]".
    quorum(home.path())
        .args(["task-get", "--task-id", "2"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"ready\":false"))
        .stdout(predicates::str::contains("\"depends_on\":\"[1]\""));
}

// -- task-list --brief (issue #57) --------------------------------------------------------

#[test]
fn task_list_brief_omits_body_full_get_keeps_it() {
    let home = tempfile::tempdir().unwrap();

    // A task whose body carries a sentinel a brief scan must never pay for.
    const SENTINEL: &str = "SENTINEL_BODY_should_not_appear_in_brief";
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "fix bug"])
        .arg("--body-stdin")
        .write_stdin(SENTINEL)
        .assert()
        .success();

    // --brief: summary fields present, body (and other non-summary fields) gone.
    quorum(home.path())
        .args(["task-list", "--brief"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"id\":1"))
        .stdout(predicates::str::contains("\"title\":\"fix bug\""))
        .stdout(predicates::str::contains("\"ready\":true"))
        .stdout(predicates::str::contains("\"assignee\":null"))
        .stdout(predicates::str::contains(SENTINEL).not())
        .stdout(predicates::str::contains("\"body\"").not())
        .stdout(predicates::str::contains("\"created_at\"").not())
        // depends_on is intentionally included in --brief (#86).
        .stdout(predicates::str::contains("\"depends_on\""));

    // Plain task-list (no --brief) is unchanged: full body still present.
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains(SENTINEL))
        .stdout(predicates::str::contains("\"body\""));

    // task-get still returns the full body + notes view.
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains(SENTINEL))
        .stdout(predicates::str::contains("\"notes\""));
}
