//! Public branch-sync enqueue boundary: configured-pair validation, clean
//! active-pair loss, and the request event.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut command = Command::cargo_bin("quorum").unwrap();
    command
        .env("QUORUM_HOME", home)
        .env("QUORUM_REPO", "test/repo")
        .env_remove("QUORUM_AGENT")
        .env_remove("QUORUM_RUN_ID")
        .env_remove("QUORUM_AGENT_ENDPOINT");
    command
}

fn configure_pair(home: &std::path::Path, pairs: &str) {
    let serve_dir = home.join("serve");
    std::fs::create_dir_all(&serve_dir).unwrap();
    std::fs::write(serve_dir.join("test__repo.toml"), pairs).unwrap();
}

#[test]
fn branch_sync_rejects_unconfigured_pair_with_usage_exit() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args([
            "branch-sync",
            "--by",
            "owner",
            "--from",
            "main",
            "--to",
            "develop",
        ])
        .assert()
        .code(2);

    configure_pair(home.path(), "sync_pairs = [[\"main\", \"develop\"]]\n");
    quorum(home.path())
        .args([
            "branch-sync",
            "--by",
            "owner",
            "--from",
            "main",
            "--to",
            "main",
        ])
        .assert()
        .code(2);
}

#[test]
fn branch_sync_list_reports_active_and_recent_terminal_rows() {
    let home = tempfile::tempdir().unwrap();
    configure_pair(
        home.path(),
        "sync_pairs = [[\"main\", \"develop\"], [\"release\", \"develop\"]]\n",
    );

    // Empty list is valid — both arrays present but empty.
    let out = quorum(home.path())
        .args(["branch-sync", "--list"])
        .assert()
        .success()
        .get_output()
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["active"].as_array().unwrap().len(), 0);
    assert_eq!(json["recent_terminal"].as_array().unwrap().len(), 0);

    // One active row and one cancelled row via the CLI.
    quorum(home.path())
        .args([
            "branch-sync",
            "--by",
            "owner",
            "--from",
            "main",
            "--to",
            "develop",
        ])
        .assert()
        .success();
    quorum(home.path())
        .args([
            "branch-sync",
            "--by",
            "owner",
            "--from",
            "release",
            "--to",
            "develop",
        ])
        .assert()
        .success();
    quorum(home.path())
        .args(["branch-sync", "--cancel", "2", "--by", "owner"])
        .assert()
        .success();

    let out = quorum(home.path())
        .args(["branch-sync", "--list"])
        .assert()
        .success()
        .get_output()
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let active = json["active"].as_array().unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0]["id"], 1);
    assert_eq!(active[0]["from"], "main");
    assert_eq!(active[0]["to"], "develop");
    assert_eq!(active[0]["phase"], "requested");
    let recent = json["recent_terminal"].as_array().unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0]["id"], 2);
    assert_eq!(recent[0]["phase"], "cancelled");
}

#[test]
fn branch_sync_cancel_of_missing_or_terminal_row_is_a_clean_negative() {
    let home = tempfile::tempdir().unwrap();
    configure_pair(home.path(), "sync_pairs = [[\"main\", \"develop\"]]\n");

    // Missing id → exit 1, ok:false.
    let out = quorum(home.path())
        .args(["branch-sync", "--cancel", "99", "--by", "owner"])
        .assert()
        .code(1)
        .get_output()
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["ok"], false);

    // Cancel twice — second call finds a terminal row.
    quorum(home.path())
        .args([
            "branch-sync",
            "--by",
            "owner",
            "--from",
            "main",
            "--to",
            "develop",
        ])
        .assert()
        .success();
    quorum(home.path())
        .args(["branch-sync", "--cancel", "1", "--by", "owner"])
        .assert()
        .success();
    quorum(home.path())
        .args(["branch-sync", "--cancel", "1", "--by", "owner"])
        .assert()
        .code(1);
}

#[test]
fn branch_sync_cancel_requires_by_and_rejects_unknown_flag_combos() {
    let home = tempfile::tempdir().unwrap();
    configure_pair(home.path(), "sync_pairs = [[\"main\", \"develop\"]]\n");
    // Missing --by fails with usage exit.
    quorum(home.path())
        .args(["branch-sync", "--cancel", "1"])
        .assert()
        .code(2);
    // --list with --from is a clap conflict.
    quorum(home.path())
        .args(["branch-sync", "--list", "--from", "main", "--to", "develop"])
        .assert()
        .code(2);
}

#[test]
fn branch_sync_status_json_reports_active_summary() {
    let home = tempfile::tempdir().unwrap();
    configure_pair(home.path(), "sync_pairs = [[\"main\", \"develop\"]]\n");
    quorum(home.path())
        .args([
            "branch-sync",
            "--by",
            "owner",
            "--from",
            "main",
            "--to",
            "develop",
        ])
        .assert()
        .success();
    let out = quorum(home.path())
        .args(["status", "--json"])
        .assert()
        .success()
        .get_output()
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let syncs = json["branch_syncs"].as_array().unwrap();
    assert_eq!(syncs.len(), 1);
    assert_eq!(syncs[0]["id"], 1);
    assert_eq!(syncs[0]["from"], "main");
    assert_eq!(syncs[0]["to"], "develop");
    assert_eq!(syncs[0]["phase"], "requested");
    assert!(syncs[0]["updated_at"].is_i64());
}

#[test]
fn branch_sync_enqueues_configured_pair_and_loses_cleanly_when_active() {
    let home = tempfile::tempdir().unwrap();
    configure_pair(home.path(), "sync_pairs = [[\"main\", \"develop\"]]\n");

    let first = quorum(home.path())
        .args([
            "branch-sync",
            "--by",
            "owner",
            "--from",
            "main",
            "--to",
            "develop",
        ])
        .assert()
        .success()
        .get_output()
        .clone();
    let json: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(json["id"], 1);
    assert_eq!(json["phase"], "requested");

    quorum(home.path())
        .args([
            "branch-sync",
            "--by",
            "another-owner",
            "--from",
            "main",
            "--to",
            "develop",
        ])
        .assert()
        .code(1);

    let db_path = home.path().join("repos/test__repo/quorum.db");
    let conn = quorum_core::db::open(&db_path).unwrap();
    let errors: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
        .unwrap();
    assert_eq!(errors, 0, "a lost active-pair race is not an error");
    let event: (String, String) = conn
        .query_row(
            "SELECT kind, subject FROM events WHERE subject='branch_sync#1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        event,
        ("branch_sync_requested".into(), "branch_sync#1".into())
    );
}
