#![allow(dead_code)]

use std::path::Path;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn open_db(home: &Path) -> rusqlite::Connection {
    let db = home.join("repos/test__repo/quorum.db");
    std::fs::create_dir_all(db.parent().unwrap()).unwrap();
    quorum_core::db::open(&db).unwrap()
}

pub fn claim_task(home: &Path, agent: &str, task_id: Option<i64>, ttl_secs: i64) {
    let mut conn = open_db(home);
    let result =
        quorum_core::tasks::claim(&mut conn, agent, task_id, &[], ttl_secs, now()).unwrap();
    assert!(result.is_some(), "claim_task expected to win");
}

pub fn claim_task_with_labels(
    home: &Path,
    agent: &str,
    match_labels: &[&str],
    ttl_secs: i64,
) -> Option<quorum_core::tasks::Task> {
    let mut conn = open_db(home);
    quorum_core::tasks::claim(&mut conn, agent, None, match_labels, ttl_secs, now()).unwrap()
}

pub fn try_claim_task(
    home: &Path,
    agent: &str,
    task_id: Option<i64>,
    ttl_secs: i64,
) -> Option<quorum_core::tasks::Task> {
    let mut conn = open_db(home);
    quorum_core::tasks::claim(&mut conn, agent, task_id, &[], ttl_secs, now()).unwrap()
}
