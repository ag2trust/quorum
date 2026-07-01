//! Integration tests for the reviewer's verdict path (issue #10 + #89).
//!
//! The verdict mechanics live in `tasks::update` → `apply_verdict` and chain a
//! state change on the original task in the same transaction as the review
//! task's `done`. Two terminal verdicts:
//! - `approve` → original → `closed` (terminal).
//! - `changes` → original → `open` + sticky-to-`orig` + `rework` label.
//!
//! Issue #89 reported the `changes` path was leaving the original in `done`
//! with the sticky/label/assignee set — making the task UNCLAIMABLE (claim
//! requires `status='open'`). These tests pin the contract end-to-end so the
//! regression cannot return silently.

use assert_cmd::Command;
use serde_json::Value;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

/// Run a quorum subcommand and parse stdout as JSON. Asserts success.
fn quorum_json(home: &std::path::Path, args: &[&str]) -> Value {
    let out = quorum(home).args(args).assert().success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("failed to parse JSON from {args:?}: {e}\n--- stdout ---\n{stdout}")
    })
}

/// End-to-end: author marks a task done → review auto-spawns → reviewer marks
/// the review done with `--verdict changes` → original MUST be claimable by
/// the sticky author. Pinned because #89 regressed this — the apply_verdict
/// changes branch was leaving status='done' which silently de-fanged claim.
#[test]
fn verdict_changes_reopens_original_as_open_claimable_by_sticky_author() {
    let home = tempfile::tempdir().unwrap();

    let create = quorum_json(
        home.path(),
        &[
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "ship-the-thing",
            "--labels",
            r#"["tier:opus-47"]"#,
        ],
    );
    let task_id = create["id"].as_i64().unwrap();

    quorum_json(
        home.path(),
        &[
            "task-claim",
            "--agent",
            "Alice",
            "--task-id",
            &task_id.to_string(),
        ],
    );

    let done = quorum_json(
        home.path(),
        &[
            "task-update",
            "--agent",
            "Alice",
            "--task-id",
            &task_id.to_string(),
            "--status",
            "done",
        ],
    );
    assert_eq!(done["status"], "done");

    // Auto-spawn surfaces a kind:review task whose refs.review_of points at our task.
    let list = quorum_json(home.path(), &["task-list", "--status", "open"]);
    let review = list
        .as_array()
        .expect("task-list returns an array")
        .iter()
        .find(|t| {
            t["labels"]
                .as_str()
                .map(|s| s.contains("kind:review"))
                .unwrap_or(false)
                && t["refs"]
                    .as_str()
                    .map(|s| s.contains(&format!("\"review_of\":{task_id}")))
                    .unwrap_or(false)
        })
        .expect("verdict-changes flow requires auto-spawned review task");
    let review_id = review["id"].as_i64().unwrap();

    quorum_json(
        home.path(),
        &[
            "task-claim",
            "--agent",
            "Bob",
            "--task-id",
            &review_id.to_string(),
        ],
    );
    quorum_json(
        home.path(),
        &[
            "task-update",
            "--agent",
            "Bob",
            "--task-id",
            &review_id.to_string(),
            "--status",
            "done",
            "--verdict",
            "changes",
        ],
    );

    let after = quorum_json(
        home.path(),
        &["task-get", "--task-id", &task_id.to_string()],
    );

    // — The load-bearing contract from issue #89: status MUST be `open`. —
    assert_eq!(
        after["status"], "open",
        "verdict=changes must leave the original task in status='open' so the \
         sticky author can re-claim it (issue #89). Got: {after:?}"
    );
    assert_eq!(
        after["assignee"], "Alice",
        "verdict=changes must set assignee=orig (the original author)"
    );
    assert!(
        after["sticky_until"].as_i64().unwrap_or(0) > 0,
        "verdict=changes must set a sticky_until window"
    );
    let labels = after["labels"].as_str().unwrap_or("");
    assert!(
        labels.contains("\"rework\""),
        "verdict=changes must append the rework label (got: {labels})"
    );

    // — The downstream behavior the contract enables: sticky author can claim. —
    // (Without the status='open' fix this errors with NotClaimable on status=done.)
    let reclaimed = quorum_json(
        home.path(),
        &[
            "task-claim",
            "--agent",
            "Alice",
            "--task-id",
            &task_id.to_string(),
        ],
    );
    assert_eq!(reclaimed["status"], "claimed");
    assert_eq!(reclaimed["assignee"], "Alice");
}

/// Non-author attempting to claim during the sticky window must be REJECTED.
/// Pinned alongside the open-status fix because the two together are the
/// "sticky-to-original-author" contract from spec §10.
#[test]
fn verdict_changes_rejects_non_author_claim_during_sticky_window() {
    let home = tempfile::tempdir().unwrap();

    let create = quorum_json(
        home.path(),
        &[
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "ship-the-thing",
            "--labels",
            r#"["tier:opus-47"]"#,
        ],
    );
    let task_id = create["id"].as_i64().unwrap();

    quorum_json(
        home.path(),
        &[
            "task-claim",
            "--agent",
            "Alice",
            "--task-id",
            &task_id.to_string(),
        ],
    );
    quorum_json(
        home.path(),
        &[
            "task-update",
            "--agent",
            "Alice",
            "--task-id",
            &task_id.to_string(),
            "--status",
            "done",
        ],
    );

    let list = quorum_json(home.path(), &["task-list", "--status", "open"]);
    let review = list
        .as_array()
        .unwrap()
        .iter()
        .find(|t| {
            t["labels"]
                .as_str()
                .map(|s| s.contains("kind:review"))
                .unwrap_or(false)
                && t["refs"]
                    .as_str()
                    .map(|s| s.contains(&format!("\"review_of\":{task_id}")))
                    .unwrap_or(false)
        })
        .expect("verdict-changes flow requires auto-spawned review task");
    let review_id = review["id"].as_i64().unwrap();

    quorum_json(
        home.path(),
        &[
            "task-claim",
            "--agent",
            "Bob",
            "--task-id",
            &review_id.to_string(),
        ],
    );
    quorum_json(
        home.path(),
        &[
            "task-update",
            "--agent",
            "Bob",
            "--task-id",
            &review_id.to_string(),
            "--status",
            "done",
            "--verdict",
            "changes",
        ],
    );

    // Carol is neither the author nor the reviewer — she must be locked out of
    // the sticky window. Quorum returns a non-zero exit for failed claims, so
    // assert failure rather than parsing JSON.
    quorum(home.path())
        .args([
            "task-claim",
            "--agent",
            "Carol",
            "--task-id",
            &task_id.to_string(),
        ])
        .assert()
        .failure();
}

/// Issue #122: cancelling a review task must respawn a fresh review for the
/// original (which stays in `done`). End-to-end CLI test.
#[test]
fn cancel_review_task_respawns_review_for_original() {
    let home = tempfile::tempdir().unwrap();

    let create = quorum_json(
        home.path(),
        &[
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "implement-feature",
        ],
    );
    let task_id = create["id"].as_i64().unwrap();

    quorum_json(
        home.path(),
        &[
            "task-claim",
            "--agent",
            "Alice",
            "--task-id",
            &task_id.to_string(),
        ],
    );
    quorum_json(
        home.path(),
        &[
            "task-update",
            "--agent",
            "Alice",
            "--task-id",
            &task_id.to_string(),
            "--status",
            "done",
        ],
    );

    // Find R1 (auto-spawned review).
    let list1 = quorum_json(home.path(), &["task-list", "--status", "open"]);
    let r1 = list1
        .as_array()
        .unwrap()
        .iter()
        .find(|t| {
            t["labels"]
                .as_str()
                .map(|s| s.contains("kind:review"))
                .unwrap_or(false)
        })
        .expect("auto-spawned review must exist");
    let r1_id = r1["id"].as_i64().unwrap();

    // Reviewer claims then cancels R1.
    quorum_json(
        home.path(),
        &[
            "task-claim",
            "--agent",
            "Bob",
            "--task-id",
            &r1_id.to_string(),
        ],
    );
    quorum_json(
        home.path(),
        &[
            "task-update",
            "--agent",
            "Bob",
            "--task-id",
            &r1_id.to_string(),
            "--status",
            "cancelled",
        ],
    );

    // R1 is cancelled.
    let r1_after = quorum_json(
        home.path(),
        &["task-get", "--task-id", &r1_id.to_string()],
    );
    assert_eq!(r1_after["status"], "cancelled");

    // A fresh R2 must have been respawned.
    let list2 = quorum_json(home.path(), &["task-list", "--status", "open"]);
    let r2 = list2
        .as_array()
        .unwrap()
        .iter()
        .find(|t| {
            t["labels"]
                .as_str()
                .map(|s| s.contains("kind:review"))
                .unwrap_or(false)
                && t["id"].as_i64() != Some(r1_id)
        })
        .expect("cancelling R1 must respawn a fresh review R2");

    // R2 points at the same original task.
    let r2_refs: Value = serde_json::from_str(r2["refs"].as_str().unwrap()).unwrap();
    assert_eq!(
        r2_refs["review_of"].as_i64(),
        Some(task_id),
        "R2 must point at the original task"
    );

    // Original task T is still in `done` (not stranded — it has a live review).
    let t = quorum_json(
        home.path(),
        &["task-get", "--task-id", &task_id.to_string()],
    );
    assert_eq!(t["status"], "done");
}
