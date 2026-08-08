#![allow(dead_code)]

use std::path::Path;
use std::time::{Duration, Instant};

pub enum WaitState<T> {
    Ready(T),
    Pending(String),
}

/// Poll an observable test state until it is ready or fail with the last value seen.
///
/// Integration tests use this for SQLite and process state, where no notification
/// primitive crosses the daemon boundary. Keeping the deadline here makes every
/// wait bounded and gives timeouts a useful missing-state diagnostic.
pub fn wait_until<T>(
    description: &str,
    timeout: Duration,
    mut observe: impl FnMut() -> WaitState<T>,
) -> T {
    let deadline = Instant::now() + timeout;

    loop {
        let last_observation = match observe() {
            WaitState::Ready(value) => return value,
            WaitState::Pending(observation) => observation,
        };

        let now = Instant::now();
        if now >= deadline {
            panic!(
                "timed out after {timeout:?} waiting for {description}; \
                 last observation: {last_observation}"
            );
        }
        std::thread::park_timeout((deadline - now).min(Duration::from_millis(25)));
    }
}

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

fn classify_claim_candidates(conn: &mut rusqlite::Connection, task_id: Option<i64>) {
    let ids = if let Some(id) = task_id {
        vec![id]
    } else {
        let mut stmt = conn
            .prepare("SELECT id FROM tasks WHERE status IN ('open','in-review','rework')")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<i64>>>()
            .unwrap()
    };
    let classifications = ids
        .into_iter()
        .map(|task_id| quorum_core::classify::TaskClassification {
            task_id,
            cx_est: 3,
            size: "M".into(),
            ready: true,
            not_ready_reason: None,
            duplicate_of: vec![],
        })
        .collect::<Vec<_>>();
    quorum_core::classify::store_classifications(
        conn,
        &classifications,
        "integration-test:v2",
        now(),
    )
    .unwrap();
}

pub fn classify_open_tasks(home: &Path) {
    let mut conn = open_db(home);
    classify_claim_candidates(&mut conn, None);
}

pub fn claim_task(home: &Path, agent: &str, task_id: Option<i64>, ttl_secs: i64) {
    let mut conn = open_db(home);
    classify_claim_candidates(&mut conn, task_id);
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
    classify_claim_candidates(&mut conn, None);
    quorum_core::tasks::claim(&mut conn, agent, None, match_labels, ttl_secs, now()).unwrap()
}

pub fn try_claim_task(
    home: &Path,
    agent: &str,
    task_id: Option<i64>,
    ttl_secs: i64,
) -> Option<quorum_core::tasks::Task> {
    let mut conn = open_db(home);
    classify_claim_candidates(&mut conn, task_id);
    quorum_core::tasks::claim(&mut conn, agent, task_id, &[], ttl_secs, now()).unwrap()
}
