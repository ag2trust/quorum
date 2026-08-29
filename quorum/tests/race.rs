//! The load-bearing invariant: N concurrent callers racing `tasks::claim` on one task produce
//! exactly one winner. The task lease reuses the same atomic claims primitive
//! (`UNIQUE(target) WHERE active=1`) the queue is built on. This is the canary — if it ever
//! flakes, stop and investigate before anything else.
//!
//! Converted from multi-process CLI to multi-threaded library calls (#161: task-claim CLI
//! removed). SQLite file locking is process-agnostic — threads with separate connections
//! contend on the same WAL/lock, so the atomicity guarantee is identical.

#[test]
fn n_threads_exactly_one_winner() {
    let home = tempfile::tempdir().unwrap();

    assert_cmd::Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .arg("init")
        .assert()
        .success();
    assert_cmd::Command::cargo_bin("quorum")
        .unwrap()
        .env("QUORUM_HOME", home.path())
        .env("QUORUM_REPO", "test/repo")
        .args(["task-create", "--created-by", "boss", "--title", "race-me"])
        .assert()
        .success();

    let db_path = home.path().join("repos/test__repo/quorum.db");
    let n = 20;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    {
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        quorum_core::classify::store_classifications(
            &mut conn,
            &[quorum_core::classify::TaskClassification {
                task_id: 1,
                cx_est: 3,
                size: "M".into(),
                size_reason: "bounded test classification rationale".into(),
                ready: true,
                not_ready_reason: None,
                duplicate_of: vec![],
            }],
            "test:v2",
            now,
        )
        .unwrap();
    }

    let handles: Vec<_> = (0..n)
        .map(|i| {
            let db = db_path.clone();
            std::thread::spawn(move || {
                let mut conn = quorum_core::db::open(&db).unwrap();
                quorum_core::tasks::claim(&mut conn, &format!("a{i}"), Some(1), &[], 300, now)
                    .unwrap()
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wins = results.iter().filter(|r| r.is_some()).count();

    assert_eq!(wins, 1, "exactly one thread must win the claim");

    let conn = quorum_core::db::open(&db_path).unwrap();
    let active: i64 = conn
        .query_row(
            "SELECT count(*) FROM claims WHERE target='task#1' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 1, "exactly one active claim row");

    let errs: i64 = conn
        .query_row("SELECT count(*) FROM errors", [], |r| r.get(0))
        .unwrap();
    assert_eq!(errs, 0, "a normal race must not log any errors");
}
