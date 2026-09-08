//! CLI contract tests for the read-only performance facts report.

use assert_cmd::Command;
use predicates::prelude::*;
use rusqlite::Connection;
use std::collections::BTreeMap;

fn quorum(home: &std::path::Path) -> Command {
    let mut command = Command::cargo_bin("quorum").unwrap();
    command.env("QUORUM_HOME", home);
    command.env("QUORUM_REPO", "test/repo");
    command
}

fn db_path(home: &std::path::Path) -> std::path::PathBuf {
    home.join("repos/test__repo/quorum.db")
}

/// Snapshot all durable row counts plus SQLite's cross-connection write
/// indicator, so this CLI-level test catches data and schema mutations.
fn snapshot_db_state(conn: &Connection) -> (BTreeMap<String, i64>, i64) {
    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .unwrap();
    let tables = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let counts = tables
        .into_iter()
        .map(|table| {
            let count = conn
                .query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |row| {
                    row.get(0)
                })
                .unwrap();
            (table, count)
        })
        .collect();
    let data_version = conn
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap();
    (counts, data_version)
}

#[test]
fn perf_facts_json_emits_versioned_report_honors_all_and_does_not_write() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    quorum(home.path())
        .args([
            "task-create",
            "--created-by",
            "owner",
            "--title",
            "historical",
        ])
        .assert()
        .success();

    let conn = quorum_core::db::open(&db_path(home.path())).unwrap();
    conn.execute(
        "UPDATE tasks \
         SET status = 'done', \
             completion_provenance = 'merged', \
             refs = '{\"merge_commit_sha\":\"0123456789abcdef0123456789abcdef01234567\"}' \
         WHERE id = 1",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE perf_watermark SET watermark = ?1 WHERE id = 1",
        [i64::MAX],
    )
    .unwrap();
    let before = snapshot_db_state(&conn);

    let prospective = quorum(home.path())
        .args(["perf", "--facts", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let prospective: serde_json::Value = serde_json::from_slice(&prospective).unwrap();
    assert_eq!(prospective["facts_version"], "perf-facts-v1");
    assert_eq!(prospective["cohort"]["prospective_only"], true);
    assert_eq!(prospective["counts"]["included"], 0);
    assert!(prospective.get("coverage").is_some());

    let all = quorum(home.path())
        .args(["perf", "--facts", "--json", "--all"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let all: serde_json::Value = serde_json::from_slice(&all).unwrap();
    assert_eq!(all["cohort"]["include_all"], true);
    assert_eq!(all["counts"]["included"], 1);

    assert_eq!(
        snapshot_db_state(&conn),
        before,
        "perf facts must not write"
    );
}

#[test]
fn perf_facts_rejects_incompatible_flags_as_usage_errors() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["perf", "--facts"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--json"));
    quorum(home.path())
        .args(["perf", "--facts", "--json", "--by", "complexity"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot be used"));
}

#[test]
fn legacy_perf_empty_output_remains_byte_for_byte_compatible() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();

    for args in [
        ["perf"].as_slice(),
        ["perf", "--all"].as_slice(),
        ["perf", "--by", "complexity"].as_slice(),
        ["perf", "--by", "reviewer"].as_slice(),
    ] {
        let output = quorum(home.path())
            .args(args)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        assert_eq!(output, b"No terminal tasks found.\n", "args: {args:?}");
    }
    for args in [
        ["perf", "--json"].as_slice(),
        ["perf", "--json", "--all"].as_slice(),
        ["perf", "--json", "--by", "complexity"].as_slice(),
        ["perf", "--json", "--by", "reviewer"].as_slice(),
    ] {
        let output = quorum(home.path())
            .args(args)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        assert_eq!(output, b"{\"rows\":[]}\n", "args: {args:?}");
    }
}
