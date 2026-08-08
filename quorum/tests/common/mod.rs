#![allow(dead_code)]

use std::path::Path;
use std::process::Child;
use std::sync::mpsc;
use std::time::Duration;

pub fn record_process_state(child: &mut Child, lines: &mut Vec<String>, context: &str) {
    let state = match child.try_wait() {
        Ok(Some(status)) => format!("exited with {status}"),
        Ok(None) => "still running".to_string(),
        Err(error) => format!("status unavailable: {error}"),
    };
    lines.push(format!("[test diagnostic] {context}; daemon {state}"));
}

pub fn drain_pending_lines(rx: &mpsc::Receiver<String>, lines: &mut Vec<String>) {
    while let Ok(line) = rx.try_recv() {
        lines.push(line);
    }
}

pub fn wait_for_count(
    child: &mut Child,
    rx: &mpsc::Receiver<String>,
    lines: &mut Vec<String>,
    needle: &str,
    count: usize,
    timeout_secs: u64,
) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        drain_pending_lines(rx, lines);
        if lines.iter().filter(|line| line.contains(needle)).count() >= count {
            return true;
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            record_process_state(child, lines, "output-count wait deadline elapsed");
            return false;
        }
        match rx.recv_timeout(deadline - now) {
            Ok(line) => lines.push(line),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                record_process_state(child, lines, "output-count wait timed out");
                return false;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                record_process_state(child, lines, "stderr disconnected");
                return false;
            }
        }
    }
}

pub fn wait_until<F>(
    child: &mut Child,
    rx: &mpsc::Receiver<String>,
    lines: &mut Vec<String>,
    description: &str,
    timeout_secs: u64,
    mut ready: F,
) where
    F: FnMut() -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        drain_pending_lines(rx, lines);
        if ready() {
            return;
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            record_process_state(child, lines, "observable-state wait deadline elapsed");
            panic!("timed out waiting for {description}; process/output diagnostics: {lines:?}");
        }
        match rx.recv_timeout((deadline - now).min(Duration::from_millis(50))) {
            Ok(line) => lines.push(line),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                record_process_state(child, lines, "stderr disconnected");
                panic!(
                    "daemon exited while waiting for {description}; process/output diagnostics: {lines:?}"
                );
            }
        }
    }
}

pub fn terminate_and_drain(
    child: &mut Child,
    rx: &mpsc::Receiver<String>,
    lines: &mut Vec<String>,
) {
    if child.try_wait().unwrap().is_none() {
        unsafe {
            libc::kill(child.id() as libc::pid_t, libc::SIGKILL);
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll terminated daemon") {
                break status;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                drain_pending_lines(rx, lines);
                panic!("daemon did not terminate within 5s after SIGKILL; output: {lines:?}");
            }
            match rx.recv_timeout((deadline - now).min(Duration::from_millis(50))) {
                Ok(line) => lines.push(line),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {}
            }
        };
        lines.push(format!("[test diagnostic] daemon terminated with {status}"));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
            Ok(line) => lines.push(line),
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("timed out draining daemon stderr after termination: {lines:?}");
            }
        }
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
