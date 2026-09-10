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
