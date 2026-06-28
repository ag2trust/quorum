//! Integration tests for `quorum init` and `quorum reset`.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c
}

#[test]
fn init_creates_db() {
    let home = tempfile::tempdir().unwrap();
    Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .arg("init")
        .assert()
        .success();
    assert!(home.path().join("quorum.db").exists());
}

#[test]
fn init_reports_schema_version() {
    let home = tempfile::tempdir().unwrap();
    let out = quorum(home.path()).arg("init").output().unwrap();
    assert!(out.status.success());
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["ok"], true);
    assert!(
        json["schema_version"].is_number(),
        "init must report schema_version"
    );
    assert!(json["schema_version"].as_i64().unwrap() > 0);
    // Fresh DB: no migrated_from (already at latest on creation).
    assert!(
        json.get("migrated_from").is_none(),
        "fresh init should not report migrated_from"
    );
}

#[test]
fn init_on_drifted_db_reports_migrated_from() {
    // Simulate the cutover incident: DB at v4 (no control table, no sticky_until/orig).
    // Running init with the current binary must retrofit the schema and report the migration.
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("quorum.db");
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             BEGIN IMMEDIATE;
             CREATE TABLE agents (id TEXT PRIMARY KEY, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL);
             CREATE TABLE messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL,
                 author TEXT NOT NULL, topic TEXT NOT NULL, kind TEXT NOT NULL, body TEXT NOT NULL,
                 refs TEXT, expires_at INTEGER NOT NULL, recipient TEXT);
             CREATE TABLE cursors (agent_id TEXT NOT NULL, topic TEXT NOT NULL, last_seq INTEGER NOT NULL, PRIMARY KEY (agent_id, topic));
             CREATE TABLE claims (id INTEGER PRIMARY KEY AUTOINCREMENT, target TEXT NOT NULL,
                 holder TEXT NOT NULL, ts INTEGER NOT NULL, expires_at INTEGER NOT NULL,
                 active INTEGER NOT NULL DEFAULT 0);
             CREATE UNIQUE INDEX claims_one_active ON claims(target) WHERE active = 1;
             CREATE TABLE tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,
                 body TEXT, status TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
                 labels TEXT, assignee TEXT, created_by TEXT NOT NULL,
                 created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, refs TEXT,
                 depends_on TEXT);
             CREATE TABLE errors (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL,
                 source TEXT NOT NULL, detail TEXT NOT NULL, expires_at INTEGER NOT NULL);
             CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL,
                 kind TEXT NOT NULL, subject TEXT NOT NULL, body TEXT NOT NULL,
                 expires_at INTEGER NOT NULL);
             INSERT INTO tasks(title, status, priority, created_by, created_at, updated_at)
                 VALUES ('pre-existing', 'open', 5, 'boss', 1, 1);
             PRAGMA user_version = 4;
             COMMIT;",
        ).unwrap();
    }
    let out = quorum(home.path()).arg("init").output().unwrap();
    assert!(out.status.success(), "init on drifted DB must succeed");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["ok"], true);
    assert_eq!(json["migrated_from"], 4, "must report migrated_from=4");
    assert!(json["schema_version"].as_i64().unwrap() >= 6);
    // Verify the retrofit actually worked: control table + sticky_until/orig exist.
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("SELECT 1 FROM control LIMIT 0").unwrap();
        let (title, sticky, orig): (String, Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT title, sticky_until, orig FROM tasks WHERE id=1",
                [],
                |r| Ok((r.get(0).unwrap(), r.get(1).unwrap(), r.get(2).unwrap())),
            )
            .unwrap();
        assert_eq!(title, "pre-existing");
        assert!(sticky.is_none());
        assert!(orig.is_none());
    }
}

#[test]
fn init_is_idempotent() {
    let home = tempfile::tempdir().unwrap();
    for _ in 0..2 {
        Command::cargo_bin("quorum")
            .unwrap()
            .env("QUORUM_HOME", home.path())
            .arg("init")
            .assert()
            .success();
    }
}

#[test]
fn concurrent_init_is_safe() {
    // N separate processes running `init` at once must all succeed against one DB —
    // migration runs under BEGIN IMMEDIATE, so first-runs serialize safely.
    //
    // Loop-stressed per quorum CLAUDE.md ("Always stress concurrency tests in a loop;
    // a single green run hides flakiness"). The first-creation WAL-switch race is the
    // documented flaky path (busy_timeout doesn't cover journal-mode changes — see
    // `db::set_journal_wal`), so a single round can pass while the bounded retry is
    // silently broken. Each iteration uses a fresh tempdir so every round re-races the
    // initial WAL switch from scratch (a reused DB would already be in WAL, no race).
    for _ in 0..12 {
        let home = tempfile::tempdir().unwrap();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let p = home.path().to_path_buf();
                std::thread::spawn(move || {
                    Command::cargo_bin("quorum")
                        .unwrap()
                        .env("QUORUM_HOME", &p)
                        .arg("init")
                        .assert()
                        .success();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(home.path().join("quorum.db").exists());
    }
}

// -- reset (#59) --------------------------------------------------------------------------

#[test]
fn reset_without_yes_refuses_and_preserves_state() {
    let home = tempfile::tempdir().unwrap();
    // Seed a task so we can prove nothing was wiped.
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "keep me"])
        .assert()
        .success();
    // `reset` with no --yes must refuse: exit 2 (usage) and name the confirm flag.
    quorum(home.path())
        .args(["reset"])
        .assert()
        .code(2)
        .stderr(predicates::str::contains("--yes"));
    // State is intact — the refusal did not touch the DB.
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("keep me"));
}

#[test]
fn reset_yes_wipes_to_clean_db() {
    let home = tempfile::tempdir().unwrap();
    // Seed a task (also registers agent "boss" via touch) so there's state to wipe.
    quorum(home.path())
        .args(["task-create", "--created-by", "boss", "--title", "wipe me"])
        .assert()
        .success();
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("wipe me"));
    // Wipe with confirmation.
    quorum(home.path())
        .args(["reset", "--yes"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"reset\":true"));
    // Clean DB: no tasks, no agents, and the file is recreated (usable).
    quorum(home.path())
        .args(["task-list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
    quorum(home.path())
        .args(["status", "--agents"])
        .assert()
        .success()
        .stdout(predicates::str::contains("[]"));
    assert!(home.path().join("quorum.db").exists());
}

#[test]
fn reset_yes_on_fresh_home_succeeds() {
    // reset --yes before any DB exists must not error on the missing-file removal — it
    // should just create a clean DB (the sidecar removals are NotFound-tolerant).
    let home = tempfile::tempdir().unwrap();
    quorum(home.path())
        .args(["reset", "--yes"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"reset\":true"));
    assert!(home.path().join("quorum.db").exists());
}
