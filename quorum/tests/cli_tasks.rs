//! Integration tests for the task commands, including the concurrent-claim single-winner
//! property and the body-via-stdin text path.

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;
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
fn cancel_lifecycle() {
    // `task-renew` was removed in #55 (auto-renew on agent touch). The lease-extend path
    // is now exercised by any --agent command (covered in agents::touch unit tests). This
    // test focuses on the cancel half of the original lifecycle test.
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
        .args(["peek"])
        .assert()
        .success()
        .stdout(predicates::str::contains("reclaimed").not());
    // No errors logged (reaping is normal operation).
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
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
    quorum(home.path())
        .args(["task-claim", "--agent", "A", "--task-id", "1"])
        .assert()
        .success();

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
fn note_combinable_with_done_submit() {
    // Post-#14 reconciliation: --note-* IS combinable with --status done (the only field
    // update an executor performs), so the executor can submit + leave a final breadcrumb
    // in one call. The field update runs first under the assignee guard; the note follows.
    let home = tempfile::tempdir().unwrap();
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
            "A",
            "--task-id",
            "1",
            "--status",
            "done",
            "--note-stdin",
        ])
        .write_stdin("submitted: see PR #123\n")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"done\""));
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("submitted: see PR #123"));
}

#[test]
fn note_with_status_done_from_nonassignee_aborts_before_adding_note() {
    // Coherence: field update runs first; non-assignee --status done fails NotHolder (exit 1)
    // and the note is NOT added. No half-applied state.
    let home = tempfile::tempdir().unwrap();
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
            "--note-stdin",
        ])
        .write_stdin("shouldnt land\n")
        .assert()
        .code(1);
    // Verify: the note was not added, the task is still claimed by A.
    quorum(home.path())
        .args(["task-get", "--task-id", "1"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"status\":\"claimed\""))
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
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
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
    let outputs: Vec<_> = children
        .into_iter()
        .map(|c| c.wait_with_output().unwrap())
        .collect();
    let wins = outputs.iter().filter(|o| o.status.success()).count();
    assert_eq!(wins, 1, "exactly one process may claim the task");

    // Mirror the claims canary (race.rs): the *quality* of the race matters too, not just
    // the count of winners. Losers must exit 1 (clean lost-race), never 3 (abnormal). A
    // post-#9 lease-insert boundary-corpse regression would surface as exit 3 here — the
    // win-count check alone would miss it because the status-UPDATE still gates correctly.
    for o in &outputs {
        let code = o.status.code().unwrap_or(-1);
        assert!(
            code == 0 || code == 1,
            "loser must exit 1 (clean lost-race), got {code} — exit 3 means tasks::claim hit an abnormal DB error"
        );
    }

    // Storage agrees: exactly one active lease row for task#1, the same property the claims
    // canary asserts for an arbitrary target. Catches a half-applied claim (status UPDATE
    // wins, lease INSERT fails) that the win-count check can't see.
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
    let active: i64 = conn
        .query_row(
            "SELECT count(*) FROM claims WHERE target='task#1' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 1, "exactly one active lease row for task#1");

    // And no errors logged — a normal race is not a failure mode.
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
        // #64 compact write response: title is no longer in the success JSON. Verify
        // the right task was claimed via `task-get` (full record) instead.
        .stdout(predicates::str::contains("\"status\":\"claimed\""));
    quorum(home.path())
        .args(["task-get", "--task-id", "2"])
        .assert()
        .success()
        .stdout(predicates::str::contains("with-label"));

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
    let outputs: Vec<_> = children
        .into_iter()
        .map(|c| c.wait_with_output().unwrap())
        .collect();
    let wins = outputs.iter().filter(|o| o.status.success()).count();
    assert_eq!(
        wins, 1,
        "label-filtered claim must still grant to exactly one process"
    );

    // Same canary-grade guards as the task-id variant — the label filter is just an extra
    // AND on the selector and must not change exit-code or lease-row semantics.
    for o in &outputs {
        let code = o.status.code().unwrap_or(-1);
        assert!(
            code == 0 || code == 1,
            "loser must exit 1 (clean lost-race), got {code}"
        );
    }
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
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
    quorum(home.path())
        .args(["task-claim", "--agent", "A"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"id\":1"));

    // No more claimable tasks: dependent is gated, dep is claimed.
    quorum(home.path())
        .args(["task-claim", "--agent", "B"])
        .assert()
        .code(1)
        .stdout(predicates::str::contains("no claimable task"));

    // Even an explicit --task-id can't pull the gated dependent.
    quorum(home.path())
        .args(["task-claim", "--agent", "B", "--task-id", "2"])
        .assert()
        .code(1);

    // Submitting dep as `done` is NOT enough — gate is on `closed` per #9/#10 alignment.
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
        .success();
    // `done` auto-spawns a review task (issue #10 Phase 2). B (not the orig) sees and claims
    // it — that's the new top-priority work, REVIEW_PRIORITY=1000 beats the dep's priority.
    // #64 compact write response omits labels; we verify `refs.review_of` (which IS in the
    // compact shape since refs stays) and use `task-get` to check the label.
    quorum(home.path())
        .args(["task-claim", "--agent", "B"])
        .assert()
        .success()
        .stdout(predicates::str::contains("review_of"));
    // Find the just-claimed review task id (B should be the assignee).
    quorum(home.path())
        .args(["task-list", "--assignee", "B"])
        .assert()
        .success()
        .stdout(predicates::str::contains("kind:review"));
    // With the review claimed and `dep` still `done` (not `closed`), the dependent stays
    // gated for everyone — the dep-gate's "closed-not-done" boundary holds even after the
    // review-as-task layer.
    quorum(home.path())
        .args(["task-claim", "--agent", "C"])
        .assert()
        .code(1)
        .stdout(predicates::str::contains("no claimable task"));
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
        .stdout(predicates::str::contains("\"depends_on\"").not());

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
