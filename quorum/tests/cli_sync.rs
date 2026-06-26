//! Integration tests for `quorum sync` — the agent's compass.
//!
//! Core `sync::gather()` / `sync::tick()` semantics are pinned in `quorum-core::sync` unit
//! tests; these tests verify the CLI surface: argument parsing, dispatch, JSON output
//! shape, and the at-most-once cursor advance via the public `quorum read` path so a
//! second `sync` invocation doesn't re-show the same direct messages.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn sync_on_fresh_db_returns_empty_object() {
    // Locked design: a quiet tick (no current_task, no msgs, no claims) serializes to {}.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    let out = quorum(home.path())
        .args(["sync", "--agent", "A"])
        .output()
        .unwrap();
    assert!(out.status.success(), "sync exit: {:?}", out.status);
    let stdout = String::from_utf8(out.stdout).unwrap();
    let trimmed = stdout.trim();
    assert_eq!(
        trimmed, "{}",
        "quiet tick must serialize to {{}}, got: {trimmed}"
    );
}

#[test]
fn sync_returns_next_task_when_agent_idle() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "do-thing",
            "--priority",
            "5",
        ])
        .assert()
        .success();
    let out = quorum(home.path())
        .args(["sync", "--agent", "A"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let next = v.get("next_task").expect("next_task present on idle");
    assert_eq!(next["title"], "do-thing");
    assert_eq!(next["priority"], 5);
    assert!(
        v.get("current_task").is_none(),
        "XOR violated: current_task should be omitted when idle"
    );
}

#[test]
fn sync_returns_current_task_when_agent_holds_one() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "mine",
            "--priority",
            "1",
        ])
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
    let out = quorum(home.path())
        .args(["sync", "--agent", "A"])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let cur = v.get("current_task").expect("current_task present");
    assert_eq!(cur["title"], "mine");
    assert!(
        v.get("next_task").is_none(),
        "XOR violated: next_task should be omitted when holding one"
    );
}

#[test]
fn sync_match_label_restricts_next_task() {
    let home = tempfile::tempdir().unwrap();
    // Two tasks: high-prio without rust label, low-prio with rust label.
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "high",
            "--priority",
            "10",
            "--labels",
            r#"["python"]"#,
        ])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "boss",
            "--title",
            "low",
            "--priority",
            "1",
            "--labels",
            r#"["rust"]"#,
        ])
        .assert()
        .success();
    // Without filter: high-prio wins.
    let unfiltered = quorum(home.path())
        .args(["sync", "--agent", "A"])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&unfiltered.stdout).unwrap();
    assert_eq!(v["next_task"]["title"], "high");
    // With --match-label rust: low-prio wins (only label-matched).
    let filtered = quorum(home.path())
        .args(["sync", "--agent", "A", "--match-label", "rust"])
        .output()
        .unwrap();
    let v2: serde_json::Value = serde_json::from_slice(&filtered.stdout).unwrap();
    assert_eq!(v2["next_task"]["title"], "low");
}

#[test]
fn sync_auto_acks_messages_so_second_call_is_quiet() {
    // Post a direct msg to A, sync once, then sync again — second call must NOT re-show it.
    // This is the at-most-once contract per CTO direction (cursor.last_seq advance).
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "post",
            "--agent",
            "Z",
            "--kind",
            "info",
            "--to",
            "A",
            "--body-stdin",
        ])
        .write_stdin("hello-A")
        .assert()
        .success();
    let first = quorum(home.path())
        .args(["sync", "--agent", "A"])
        .output()
        .unwrap();
    let v1: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    let direct = v1["direct"].as_array().expect("direct present");
    assert_eq!(direct.len(), 1);
    assert_eq!(direct[0]["body"], "hello-A");
    // Second call: cursor has advanced; no direct returned.
    let second = quorum(home.path())
        .args(["sync", "--agent", "A"])
        .output()
        .unwrap();
    let v2: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert!(
        v2.get("direct").is_none(),
        "auto-ack failed: direct still present on re-call: {v2:?}"
    );
}

#[test]
fn sync_notifications_count_is_exact() {
    let home = tempfile::tempdir().unwrap();
    // Post 5 broadcasts.
    for body in ["b1", "b2", "b3", "b4", "b5"] {
        quorum(home.path())
            .args(["post", "--agent", "Z", "--kind", "info", "--body-stdin"])
            .write_stdin(body)
            .assert()
            .success();
    }
    let out = quorum(home.path())
        .args(["sync", "--agent", "A"])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let notif = v.get("notifications").expect("notifications present");
    assert_eq!(notif["count"], 5);
    // No critical broadcasts here → bodies omitted (empty `critical` skipped per omit-empty).
    assert!(
        notif.get("critical").is_none(),
        "critical bodies should be omitted when none are critical-kind: {notif:?}"
    );
}
