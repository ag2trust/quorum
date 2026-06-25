//! Integration tests for ops commands: status, sweep, help (and the help-agent alias),
//! config handling, migration refusal, and the WAL-health property (short-lived connections
//! self-checkpoint).

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn init_writes_default_config() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    assert!(home.path().join("config.toml").exists());
}

#[test]
fn status_json_and_table() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["claim", "--agent", "A", "--target", "pr#1", "--ttl", "1h"])
        .assert()
        .success();

    quorum(home.path())
        .args(["status", "--json"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"claims_active\":1"))
        .stdout(predicates::str::contains("\"agents_online\":1"));

    quorum(home.path())
        .arg("status")
        .assert()
        .success()
        .stdout(predicates::str::contains("claims     : 1 active"));
}

#[test]
fn sweep_runs() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    quorum(home.path())
        .arg("sweep")
        .assert()
        .success()
        .stdout(predicates::str::contains("\"ok\":true"));
}

#[test]
fn help_lists_commands_and_safety() {
    quorum(tempfile::tempdir().unwrap().path())
        .arg("help")
        .assert()
        .success()
        .stdout(predicates::str::contains("--body-stdin"))
        .stdout(predicates::str::contains("EXIT CODES"))
        .stdout(predicates::str::contains("quorum claim"));
}

#[test]
fn help_agent_alias_still_works() {
    // `help-agent` was the v1 spelling; keep it as a back-compat alias so existing
    // agent prompts/scripts/cheatsheets continue to work after the rename.
    quorum(tempfile::tempdir().unwrap().path())
        .arg("help-agent")
        .assert()
        .success()
        .stdout(predicates::str::contains("EXIT CODES"));
}

#[test]
fn help_works_despite_malformed_config() {
    // help is the recovery command — it must work even when config is broken
    // (and without ~/.quorum existing at all).
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    std::fs::write(home.path().join("config.toml"), "= not valid =").unwrap();
    quorum(home.path())
        .arg("help")
        .assert()
        .success()
        .stdout(predicates::str::contains("EXIT CODES"));
}

#[test]
fn help_works_with_no_quorum_home() {
    // Acceptance: no ~/.quorum at all — help must still print without touching disk.
    let home = tempfile::tempdir().unwrap();
    // do NOT run `init`; QUORUM_HOME points at an empty dir
    quorum(home.path())
        .arg("help")
        .assert()
        .success()
        .stdout(predicates::str::contains("quorum claim"));
}

#[test]
fn malformed_config_fails_loud() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    std::fs::write(
        home.path().join("config.toml"),
        "this is not = valid = toml =",
    )
    .unwrap();
    quorum(home.path()).arg("roster").assert().code(3);
}

#[test]
fn wal_stays_small_with_short_lived_connections() {
    // 50 separate post processes; each opens+closes, so SQLite checkpoints on last-close and
    // the -wal must NOT grow unbounded. (No explicit sweep.)
    let home = tempfile::tempdir().unwrap();
    for i in 0..50 {
        let mut cmd = quorum(home.path());
        cmd.args(["post", "--agent", "A", "--kind", "info", "--body-stdin"])
            .write_stdin(format!("m{i}\n"))
            .assert()
            .success();
    }
    let wal = home.path().join("quorum.db-wal");
    let size = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    assert!(
        size < 100_000,
        "WAL grew to {size} bytes — not checkpointing"
    );
}

#[test]
fn migration_refusal_exits_3_on_newer_schema() {
    let home = tempfile::tempdir().unwrap();
    // Create a valid DB first...
    quorum(home.path()).arg("init").assert().success();
    // ...then bump user_version past what this binary understands.
    let db_path = home.path().join("quorum.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("PRAGMA user_version = 999").unwrap();
    drop(conn);
    // Any command that opens the DB should fail with exit 3.
    quorum(home.path())
        .arg("roster")
        .assert()
        .code(3)
        .stderr(predicates::str::contains("schema version 999"));
}

#[test]
fn missing_config_falls_back_to_defaults() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    // Delete config.toml — commands should still work with built-in defaults.
    std::fs::remove_file(home.path().join("config.toml")).unwrap();
    quorum(home.path())
        .arg("roster")
        .assert()
        .success()
        .stdout(predicates::str::diff("[]\n"));
    quorum(home.path())
        .args(["status", "--json"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"agents_total\":0"));
}

#[test]
fn status_watch_emits_output_before_kill() {
    let home = tempfile::tempdir().unwrap();
    // Seed data so the status table has something visible.
    quorum(home.path())
        .args(["claim", "--agent", "A", "--target", "pr#1", "--ttl", "1h"])
        .assert()
        .success();

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("quorum"))
        .env("QUORUM_HOME", home.path())
        .args(["status", "--watch"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn status --watch");

    // One tick is 1.5s; wait for two ticks so at least one full render completes.
    std::thread::sleep(std::time::Duration::from_millis(3500));

    child.kill().expect("failed to kill watch process");
    let output = child.wait_with_output().expect("failed to wait");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("agents"),
        "expected status table, got: {stdout}"
    );
    assert!(
        stdout.contains("claims"),
        "expected claims line in status table"
    );
}
