//! Integration tests for the claim commands, including the exit-code contract and the
//! "normal lost-race does not write an errors row" invariant.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn claim_then_second_loses_with_no_error_logged() {
    let home = tempfile::tempdir().unwrap();

    // First claim wins (exit 0).
    quorum(home.path())
        .args(["claim", "--agent", "A", "--target", "pr#1", "--ttl", "45m"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"ok\":true"));

    // Second claim by another agent loses: exit 1, {ok:false, holder:A}.
    quorum(home.path())
        .args(["claim", "--agent", "B", "--target", "pr#1", "--ttl", "45m"])
        .assert()
        .code(1)
        .stdout(predicates::str::contains("\"ok\":false"))
        .stdout(predicates::str::contains("\"holder\":\"A\""));

    // A normal lost race is NOT an error → no errors row written.
    let conn = quorum_core::db::open(&home.path().join("quorum.db")).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0, "lost race must not log an error");
}

#[test]
fn release_then_reclaimable() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["claim", "--agent", "A", "--target", "pr#9", "--ttl", "1h"])
        .assert()
        .success();
    quorum(home.path())
        .args(["release", "--agent", "A", "--target", "pr#9"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"released\":true"));
    quorum(home.path())
        .args(["claim", "--agent", "B", "--target", "pr#9", "--ttl", "1h"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"ok\":true"));
}

#[test]
fn release_by_nonholder_exits_1() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["claim", "--agent", "A", "--target", "pr#2", "--ttl", "1h"])
        .assert()
        .success();
    quorum(home.path())
        .args(["release", "--agent", "B", "--target", "pr#2"])
        .assert()
        .code(1);
}

#[test]
fn claim_bumps_presence_in_roster() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "claim", "--agent", "Worker1", "--target", "pr#3", "--ttl", "1h",
        ])
        .assert()
        .success();
    quorum(home.path())
        .arg("roster")
        .assert()
        .success()
        .stdout(predicates::str::contains("Worker1"));
}

#[test]
fn release_requires_exactly_one_selector() {
    let home = tempfile::tempdir().unwrap();
    // neither selector → usage error (exit 2)
    quorum(home.path())
        .args(["release", "--agent", "A"])
        .assert()
        .code(2);
}

#[test]
fn renew_output_matches_claim_won_shape() {
    let home = tempfile::tempdir().unwrap();
    // claim emits {ok:true, claim_id:...}
    let claim_out = quorum(home.path())
        .args(["claim", "--agent", "A", "--target", "pr#7", "--ttl", "1h"])
        .output()
        .unwrap()
        .stdout;
    let claim_json: serde_json::Value = serde_json::from_slice(&claim_out).unwrap();
    assert_eq!(claim_json["ok"], true);
    assert!(claim_json["claim_id"].is_number());

    let claim_id = claim_json["claim_id"].to_string();

    // renew must emit the same shape: {ok:true, claim_id:...}
    let renew_out = quorum(home.path())
        .args([
            "renew",
            "--agent",
            "A",
            "--claim-id",
            &claim_id,
            "--ttl",
            "2h",
        ])
        .output()
        .unwrap()
        .stdout;
    let renew_json: serde_json::Value = serde_json::from_slice(&renew_out).unwrap();
    assert_eq!(renew_json["ok"], true, "renew must emit ok:true");
    assert_eq!(
        renew_json["claim_id"], claim_json["claim_id"],
        "renew must emit claim_id, not id"
    );
    assert!(renew_json["target"].is_string());
    assert!(renew_json["holder"].is_string());
    assert!(renew_json["expires_at"].is_number());
    // The old field name 'id' must NOT appear.
    assert!(
        renew_json.get("id").is_none(),
        "renew must not emit bare 'id'"
    );
}
