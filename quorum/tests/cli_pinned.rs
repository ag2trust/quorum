//! End-to-end CLI tests for pinned notices (issue #78):
//! `quorum pin`, `quorum unpin`, `quorum pins`, and the `sync.pinned` payload field.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn pin_then_pins_lists_then_unpin_clears() {
    let home = tempfile::tempdir().unwrap();
    // pin a notice — body via stdin per Invariant #10
    let out = quorum(home.path())
        .args(["pin", "--agent", "cto"])
        .arg("--body-stdin")
        .write_stdin("MIGRATION IN PROGRESS — use new flow\n")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"author\":\"cto\""))
        .stdout(predicates::str::contains("MIGRATION IN PROGRESS"))
        .get_output()
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = json["id"].as_i64().expect("id present");
    assert!(id > 0);

    quorum(home.path())
        .args(["pins"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"author\":\"cto\""));

    quorum(home.path())
        .args(["unpin", "--agent", "cto", "--id", &id.to_string()])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"ok\":true"))
        .stdout(predicates::str::contains("\"cleared\""));

    quorum(home.path())
        .args(["pins"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
}

#[test]
fn pin_requires_body_via_stdin_or_file() {
    // Invariant #10: free text never as a flag. No body source → exit 2.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    quorum(home.path())
        .args(["pin", "--agent", "cto"])
        .assert()
        .failure()
        .code(2);
}

#[test]
fn unpin_by_non_creator_exits_1_with_reason() {
    let home = tempfile::tempdir().unwrap();
    let out = quorum(home.path())
        .args(["pin", "--agent", "cto"])
        .arg("--body-stdin")
        .write_stdin("guarded\n")
        .assert()
        .success()
        .get_output()
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let id = json["id"].as_i64().unwrap();

    quorum(home.path())
        .args(["unpin", "--agent", "intruder", "--id", &id.to_string()])
        .assert()
        .failure()
        .code(1)
        .stdout(predicates::str::contains("\"ok\":false"))
        .stdout(predicates::str::contains("creator-only"))
        .stdout(predicates::str::contains("\"author\":\"cto\""));

    // Pin still present.
    quorum(home.path())
        .args(["pins"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"author\":\"cto\""));
}

#[test]
fn unpin_on_missing_id_exits_1_clean() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    quorum(home.path())
        .args(["unpin", "--agent", "cto", "--id", "99999"])
        .assert()
        .failure()
        .code(1)
        .stdout(predicates::str::contains("\"ok\":false"))
        .stdout(predicates::str::contains("no pin at that id"));
}

#[test]
fn sync_surfaces_pinned_section() {
    // The motivating use case: a new agent's first sync must surface the standing notice.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["pin", "--agent", "cto"])
        .arg("--body-stdin")
        .write_stdin("STANDING NOTICE — read me\n")
        .assert()
        .success();

    let out = quorum(home.path())
        .args(["sync", "--agent", "fresh-agent"])
        .assert()
        .success()
        .get_output()
        .clone();
    let snap: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let pinned = snap["pinned"].as_array().expect("pinned array present");
    assert_eq!(pinned.len(), 1, "expected 1 pin, got: {pinned:?}");
    assert_eq!(pinned[0]["author"], "cto");
    // stdin preserves the trailing newline that `write_stdin` adds — body comparison
    // strips it so we test the round-trip without coupling to harness quirks.
    let body = pinned[0]["body"].as_str().unwrap().trim_end();
    assert_eq!(body, "STANDING NOTICE — read me");
}

#[test]
fn sync_pinned_field_omitted_when_no_pins() {
    // Wire economy: empty pinned array isn't serialized.
    let home = tempfile::tempdir().unwrap();
    let out = quorum(home.path())
        .args(["sync", "--agent", "A"])
        .assert()
        .success()
        .get_output()
        .clone();
    let raw = String::from_utf8_lossy(&out.stdout);
    assert!(
        !raw.contains("\"pinned\""),
        "pinned field must be omitted when empty, got: {raw}"
    );
}

#[test]
fn sync_pinned_persists_across_multiple_ticks() {
    // Cursor-independent: tick acks messages but pinned must re-surface every time.
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["pin", "--agent", "cto"])
        .arg("--body-stdin")
        .write_stdin("persistent\n")
        .assert()
        .success();

    for _ in 0..3 {
        let out = quorum(home.path())
            .args(["sync", "--agent", "A"])
            .assert()
            .success()
            .get_output()
            .clone();
        let snap: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(snap["pinned"].as_array().unwrap().len(), 1);
    }
}
