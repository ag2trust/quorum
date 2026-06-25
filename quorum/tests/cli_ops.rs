//! Integration tests for ops commands: status, sweep, help (and the help-agent alias),
//! config handling, and the WAL-health property (short-lived connections self-checkpoint).

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
