//! Integration tests for `quorum init` and `quorum reset`.

use assert_cmd::Command;

fn quorum(home: &std::path::Path) -> Command {
    let mut c = Command::cargo_bin("quorum").unwrap();
    c.env("QUORUM_HOME", home);
    c.env("QUORUM_REPO", "test/repo");
    c
}

#[test]
fn init_creates_db() {
    let home = tempfile::tempdir().unwrap();
    Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();
    assert!(home.path().join("repos/test__repo/quorum.db").exists());
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
    let db_path = home.path().join("repos/test__repo/quorum.db");
    std::fs::create_dir_all(db_path.parent().unwrap()).unwrap();
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
            .env("QUORUM_REPO", "test/repo")
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
                        .env("QUORUM_REPO", "test/repo")
                        .arg("init")
                        .assert()
                        .success();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(home.path().join("repos/test__repo/quorum.db").exists());
    }
}

// -- repo provisioning (task #106) --------------------------------------------------------

#[test]
fn init_creates_serve_config_scaffold() {
    let home = tempfile::tempdir().unwrap();
    quorum(home.path()).arg("init").assert().success();
    let toml_path = home.path().join("serve/test__repo.toml");
    assert!(toml_path.exists(), "serve config scaffold must be created");
    let content = std::fs::read_to_string(&toml_path).unwrap();
    assert!(
        content.contains("R2 pre-merge review sampling"),
        "scaffold must document R2 sampling"
    );
    assert!(
        content.contains("[model_profiles.primary]")
            && content.contains("[routing.classifier]")
            && content.contains("[routing.reviewer.\"1\"]"),
        "scaffold must document the required hard-cutover routing policy"
    );
}

#[test]
fn init_does_not_clobber_existing_serve_config() {
    let home = tempfile::tempdir().unwrap();
    let toml_path = home.path().join("serve/test__repo.toml");
    std::fs::create_dir_all(toml_path.parent().unwrap()).unwrap();
    let custom = "cap = 16\n";
    std::fs::write(&toml_path, custom).unwrap();
    quorum(home.path()).arg("init").assert().success();
    let after = std::fs::read_to_string(&toml_path).unwrap();
    assert_eq!(after, custom, "user edits must be preserved byte-for-byte");
}

#[test]
fn init_no_git_repo_skips_skill_cleanly() {
    let home = tempfile::tempdir().unwrap();
    let non_git = tempfile::tempdir().unwrap();
    Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .current_dir(non_git.path())
        .arg("init")
        .assert()
        .success();
    // No .claude dir created in the non-git directory.
    assert!(
        !non_git.path().join(".claude").exists(),
        "skill must not be written outside a git repo"
    );
}

#[test]
fn init_idempotent_serve_config_and_skill() {
    let home = tempfile::tempdir().unwrap();
    // First run creates everything.
    quorum(home.path()).arg("init").assert().success();
    let toml_path = home.path().join("serve/test__repo.toml");
    let toml_before = std::fs::read_to_string(&toml_path).unwrap();
    let mtime_before = std::fs::metadata(&toml_path).unwrap().modified().unwrap();
    // Small sleep so mtime would differ if file were rewritten.
    std::thread::sleep(std::time::Duration::from_millis(50));
    // Second run: no changes.
    let out = quorum(home.path()).arg("init").output().unwrap();
    assert!(out.status.success());
    let toml_after = std::fs::read_to_string(&toml_path).unwrap();
    assert_eq!(toml_before, toml_after);
    let mtime_after = std::fs::metadata(&toml_path).unwrap().modified().unwrap();
    assert_eq!(
        mtime_before, mtime_after,
        "serve toml must not be rewritten on idempotent run"
    );
    // JSON output should not contain serve_config action on second run.
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(
        json.get("serve_config").is_none(),
        "second init must not report serve_config action"
    );
}

#[test]
fn init_installs_current_embedded_recovery_skill() {
    let repo = tempfile::tempdir().unwrap();
    std::process::Command::new("git")
        .args(["init", "--initial-branch=main"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    let home = tempfile::tempdir().unwrap();

    let out = Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .current_dir(repo.path())
        .arg("init")
        .output()
        .unwrap();

    assert!(out.status.success(), "init must install the embedded skill");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["skill"]["action"], "created");
    let skill =
        std::fs::read_to_string(repo.path().join(".claude/skills/quorum/SKILL.md")).unwrap();
    assert!(
        skill.contains("task-retry")
            && skill.contains("decomposition-adopt-recovery")
            && skill.contains("task-close"),
        "fresh installs must include recovery decision procedures"
    );
    assert!(
        skill.contains("--continue-pr <PR>")
            && skill.contains("review-only task has no")
            && skill.contains("changes verdict fails"),
        "fresh installs must distinguish continuations from review-only tasks"
    );
    assert!(
        skill.contains("--repo owner/name")
            && skill.contains("QUORUM_REPO")
            && skill.contains("outside-repository change is not a managed deliverable"),
        "fresh installs must preserve repository targeting and write boundaries"
    );
    assert!(
        skill.contains("no public successor-task creation interface")
            && skill.contains("Do not manufacture `refs.source_task`"),
        "until task-create ships source-task, the skill must reject invented provenance"
    );
}

// -- upgrade command (task #106 amendment) -------------------------------------------------

#[test]
fn upgrade_replaces_stale_skill() {
    let home = tempfile::tempdir().unwrap();
    // init creates the skill in the *current* git repo's toplevel.
    // For this test we use a fake git repo so we control the skill file.
    let repo = tempfile::tempdir().unwrap();
    std::process::Command::new("git")
        .args(["init", "--initial-branch=main"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    // Seed a stale skill.
    let skill_dir = repo.path().join(".claude/skills/quorum");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "old content\n").unwrap();
    // upgrade should replace it.
    let out = Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .current_dir(repo.path())
        .args(["upgrade"])
        .output()
        .unwrap();
    assert!(out.status.success(), "upgrade must exit 0");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["status"], "upgraded");
    let after = std::fs::read_to_string(skill_dir.join("SKILL.md")).unwrap();
    assert_ne!(after, "old content\n", "stale file must be replaced");
    assert!(
        after.contains("quorum"),
        "embedded skill must mention quorum"
    );
    assert!(
        after.contains("cancel that task before external")
            && after.contains("create a new `--review-pr` task"),
        "embedded skill must document external transfer protocol"
    );
    assert!(
        after.contains("init` installs the embedded")
            && after.contains("upgrade` publishes the embedded skill artifact")
            && after.contains("upgrade --check` to detect drift without writing"),
        "upgraded skill must explain how init and upgrade publish the artifact"
    );
}

#[test]
fn upgrade_check_reports_stale_without_writing() {
    let repo = tempfile::tempdir().unwrap();
    std::process::Command::new("git")
        .args(["init", "--initial-branch=main"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    let skill_dir = repo.path().join(".claude/skills/quorum");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(skill_dir.join("SKILL.md"), "old\n").unwrap();
    let home = tempfile::tempdir().unwrap();
    let out = Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .current_dir(repo.path())
        .args(["upgrade", "--check"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1), "check on stale must exit 1");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["status"], "stale");
    // File unchanged.
    let after = std::fs::read_to_string(skill_dir.join("SKILL.md")).unwrap();
    assert_eq!(after, "old\n", "check must not write");
}

#[test]
fn upgrade_current_is_noop() {
    let repo = tempfile::tempdir().unwrap();
    std::process::Command::new("git")
        .args(["init", "--initial-branch=main"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    let home = tempfile::tempdir().unwrap();
    // Use init to create the skill (it will embed the current copy).
    Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .current_dir(repo.path())
        .arg("init")
        .assert()
        .success();
    // upgrade should report current.
    let out = Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .current_dir(repo.path())
        .args(["upgrade"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["status"], "current");
}

#[test]
fn init_stale_skill_reports_drift_without_overwriting() {
    let repo = tempfile::tempdir().unwrap();
    std::process::Command::new("git")
        .args(["init", "--initial-branch=main"])
        .current_dir(repo.path())
        .output()
        .unwrap();
    let skill_dir = repo.path().join(".claude/skills/quorum");
    std::fs::create_dir_all(&skill_dir).unwrap();
    let stale = "# old skill content\n";
    std::fs::write(skill_dir.join("SKILL.md"), stale).unwrap();
    let home = tempfile::tempdir().unwrap();
    let out = Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .current_dir(repo.path())
        .arg("init")
        .output()
        .unwrap();
    assert!(out.status.success());
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["skill"], "stale", "init must report drift");
    // File must be untouched.
    let after = std::fs::read_to_string(skill_dir.join("SKILL.md")).unwrap();
    assert_eq!(after, stale, "init must not overwrite stale skill");
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
    assert!(home.path().join("repos/test__repo/quorum.db").exists());
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
    assert!(home.path().join("repos/test__repo/quorum.db").exists());
}
