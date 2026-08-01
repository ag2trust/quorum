//! Durable state machine for daemon-owned decomposition artifact cleanup.
//!
//! This module only leases and settles bounded cleanup work. External process,
//! GitHub, and git operations must happen after `claim_next` commits.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::{Map, Value};

pub const MAX_CLEANUP_ATTEMPTS: i64 = 3;
pub const MAX_CLEANUP_ARTIFACT_BYTES: usize = 4096;
pub const MAX_CLEANUP_ERROR_BYTES: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupKey {
    pub graph_id: i64,
    pub task_id: i64,
    pub artifact_kind: String,
    pub artifact_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupWork {
    pub key: CleanupKey,
    /// Lease generation. Settlement must present this exact value.
    pub attempt: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureOutcome {
    RetryPending,
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RequeueResult {
    pub pending: usize,
    pub exhausted: usize,
}

/// Recover leases left by a crashed daemon. Attempts are consumed at claim,
/// so a lease at the cap is terminal rather than being executed again.
pub fn requeue_interrupted(conn: &mut Connection, now: i64) -> Result<RequeueResult> {
    let tx = begin_immediate(conn)?;
    let exhausted = tx.execute(
        "UPDATE decomposition_cleanup
         SET state='exhausted',last_error='cleanup interrupted at retry limit',updated_at=?1
         WHERE state='running' AND attempts>=?2",
        params![now, MAX_CLEANUP_ATTEMPTS],
    )?;
    let pending = tx.execute(
        "UPDATE decomposition_cleanup
         SET state='pending',last_error='cleanup interrupted before completion',updated_at=?1
         WHERE state='running' AND attempts<?2",
        params![now, MAX_CLEANUP_ATTEMPTS],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(RequeueResult { pending, exhausted })
}

/// Atomically leases the oldest eligible intent. Invalid persisted intents are
/// marked exhausted in the same transaction and are never returned for I/O.
pub fn claim_next(conn: &mut Connection, now: i64) -> Result<Option<CleanupWork>> {
    let tx = begin_immediate(conn)?;
    tx.execute(
        "UPDATE decomposition_cleanup
         SET state='exhausted',last_error='cleanup retry limit reached before claim',updated_at=?1
         WHERE state='pending' AND attempts>=?2",
        params![now, MAX_CLEANUP_ATTEMPTS],
    )?;
    loop {
        let key = oldest_candidate(&tx)?;
        let Some(key) = key else {
            tx.commit().map_err(map_sql_err)?;
            return Ok(None);
        };
        if let Err(reason) = validate_key(&key) {
            tx.execute(
                "UPDATE decomposition_cleanup
                 SET state='exhausted',last_error=?5,updated_at=?6
                 WHERE graph_id=?1 AND task_id=?2 AND artifact_kind=?3 AND artifact_ref=?4
                   AND state='pending'",
                params![
                    key.graph_id,
                    key.task_id,
                    key.artifact_kind,
                    key.artifact_ref,
                    reason,
                    now
                ],
            )?;
            continue;
        }
        let attempt: Option<i64> = tx
            .query_row(
                "UPDATE decomposition_cleanup SET state='running',attempts=attempts+1,
                    last_error=NULL,updated_at=?5
             WHERE graph_id=?1 AND task_id=?2 AND artifact_kind=?3 AND artifact_ref=?4
               AND state='pending' AND attempts<?6
             RETURNING attempts",
                params![
                    key.graph_id,
                    key.task_id,
                    key.artifact_kind,
                    key.artifact_ref,
                    now,
                    MAX_CLEANUP_ATTEMPTS
                ],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(attempt) = attempt {
            tx.commit().map_err(map_sql_err)?;
            return Ok(Some(CleanupWork { key, attempt }));
        }
    }
}

pub fn complete(conn: &mut Connection, work: &CleanupWork, now: i64) -> Result<bool> {
    settle(conn, work, "done", None, now).map(|outcome| outcome.is_some())
}

pub fn fail(
    conn: &mut Connection,
    work: &CleanupWork,
    error: &str,
    now: i64,
) -> Result<Option<FailureOutcome>> {
    if error.is_empty() || error.contains('\0') || error.len() > MAX_CLEANUP_ERROR_BYTES {
        return Err(QuorumError::Usage("invalid bounded cleanup error".into()));
    }
    let state = if work.attempt >= MAX_CLEANUP_ATTEMPTS {
        "exhausted"
    } else {
        "pending"
    };
    settle(conn, work, state, Some(error), now).map(|changed| {
        changed.map(|_| {
            if state == "exhausted" {
                FailureOutcome::Exhausted
            } else {
                FailureOutcome::RetryPending
            }
        })
    })
}

fn settle(
    conn: &mut Connection,
    work: &CleanupWork,
    state: &str,
    error: Option<&str>,
    now: i64,
) -> Result<Option<()>> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "UPDATE decomposition_cleanup SET state=?6,last_error=?7,updated_at=?8
         WHERE graph_id=?1 AND task_id=?2 AND artifact_kind=?3 AND artifact_ref=?4
           AND state='running' AND attempts=?5",
        params![
            work.key.graph_id,
            work.key.task_id,
            work.key.artifact_kind,
            work.key.artifact_ref,
            work.attempt,
            state,
            error,
            now
        ],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok((changed == 1).then_some(()))
}

fn oldest_candidate(tx: &Transaction<'_>) -> Result<Option<CleanupKey>> {
    tx.query_row(
        "SELECT c.graph_id,c.task_id,c.artifact_kind,c.artifact_ref
         FROM decomposition_cleanup c
         JOIN task_decompositions d ON d.id=c.graph_id
         JOIN task_graph_members m ON m.graph_id=c.graph_id AND m.task_id=c.task_id
         WHERE c.state='pending' AND c.attempts<?1
           AND d.state='cancelled' AND d.active=0
           AND NOT EXISTS (
               SELECT 1 FROM decomposition_cleanup prior
               WHERE prior.graph_id=c.graph_id AND prior.task_id=c.task_id
                 AND prior.state IN ('pending','running')
                 AND CASE prior.artifact_kind
                       WHEN 'process' THEN 1 WHEN 'proposed-change' THEN 2
                       WHEN 'worktree' THEN 3 WHEN 'branch' THEN 4 ELSE 0 END
                     < CASE c.artifact_kind
                       WHEN 'process' THEN 1 WHEN 'proposed-change' THEN 2
                       WHEN 'worktree' THEN 3 WHEN 'branch' THEN 4 ELSE 0 END)
         ORDER BY c.updated_at,c.graph_id,c.task_id,
                  CASE c.artifact_kind WHEN 'process' THEN 1 WHEN 'proposed-change' THEN 2
                       WHEN 'worktree' THEN 3 WHEN 'branch' THEN 4 ELSE 0 END,
                  c.artifact_ref
         LIMIT 1",
        [MAX_CLEANUP_ATTEMPTS],
        |row| {
            Ok(CleanupKey {
                graph_id: row.get(0)?,
                task_id: row.get(1)?,
                artifact_kind: row.get(2)?,
                artifact_ref: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn validate_key(key: &CleanupKey) -> std::result::Result<(), &'static str> {
    if key.artifact_ref.contains('\0') || key.artifact_ref.len() > MAX_CLEANUP_ARTIFACT_BYTES {
        return Err("invalid or oversized cleanup artifact reference");
    }
    let value: Value = serde_json::from_str(&key.artifact_ref)
        .map_err(|_| "malformed cleanup artifact reference")?;
    let object = value
        .as_object()
        .ok_or("cleanup artifact reference is not an object")?;
    let valid = match key.artifact_kind.as_str() {
        "process" => {
            exact(object, &["agent", "pid", "session_id"])
                && strings(object, &["agent", "session_id"])
                && object
                    .get("pid")
                    .and_then(Value::as_i64)
                    .is_some_and(|pid| pid > 0)
        }
        "proposed-change" => {
            exact(object, &["head_ref", "head_sha", "pr_number"])
                && strings(object, &["head_ref", "head_sha"])
                && object
                    .get("pr_number")
                    .and_then(Value::as_i64)
                    .is_some_and(|n| n > 0)
        }
        "worktree" => exact(object, &["branch", "path"]) && strings(object, &["branch", "path"]),
        "branch" => {
            exact(object, &["expected_sha", "name"]) && strings(object, &["expected_sha", "name"])
        }
        _ => return Err("unknown cleanup artifact kind"),
    };
    valid.then_some(()).ok_or("invalid cleanup artifact fields")
}

fn exact(object: &Map<String, Value>, keys: &[&str]) -> bool {
    object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
}

fn strings(object: &Map<String, Value>, keys: &[&str]) -> bool {
    keys.iter().all(|key| {
        object
            .get(*key)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty() && !value.contains('\0'))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::process::Stdio;

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::db::migrate(&conn).unwrap();
        seed(&conn);
        conn
    }

    fn seed(conn: &Connection) {
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (1,'source','cancelled','owner',1,1),(2,'child','cancelled','owner',1,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_decompositions(id,source_task_id,state,active,freeze_active,
                 planned_source_revision,created_at,updated_at)
             VALUES (1,1,'cancelled',0,0,1,1,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)
             VALUES (1,2,'child',1,0)",
            [],
        )
        .unwrap();
    }

    fn insert(conn: &Connection, kind: &str, artifact_ref: &str, updated_at: i64) {
        conn.execute(
            "INSERT INTO decomposition_cleanup(graph_id,task_id,artifact_kind,artifact_ref,updated_at)
             VALUES (1,2,?1,?2,?3)",
            params![kind, artifact_ref, updated_at],
        ).unwrap();
    }

    fn process_ref() -> &'static str {
        r#"{"agent":"a","pid":42,"session_id":"s"}"#
    }

    #[test]
    fn claim_settlement_is_attempt_guarded_and_retries_to_cap() {
        let mut conn = setup();
        insert(&conn, "process", process_ref(), 1);
        let first = claim_next(&mut conn, 2).unwrap().unwrap();
        assert_eq!(first.attempt, 1);
        let mut stale = first.clone();
        assert_eq!(
            fail(&mut conn, &first, "retry", 3).unwrap(),
            Some(FailureOutcome::RetryPending)
        );
        let second = claim_next(&mut conn, 4).unwrap().unwrap();
        assert_eq!(second.attempt, 2);
        assert!(!complete(&mut conn, &stale, 5).unwrap());
        assert_eq!(
            fail(&mut conn, &second, "retry", 6).unwrap(),
            Some(FailureOutcome::RetryPending)
        );
        let third = claim_next(&mut conn, 7).unwrap().unwrap();
        assert_eq!(
            fail(&mut conn, &third, "final", 8).unwrap(),
            Some(FailureOutcome::Exhausted)
        );
        assert!(claim_next(&mut conn, 9).unwrap().is_none());
        let state: (String, i64) = conn
            .query_row(
                "SELECT state,attempts FROM decomposition_cleanup",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(state, ("exhausted".into(), 3));
        stale.attempt = 3;
        assert!(!complete(&mut conn, &stale, 10).unwrap());
    }

    #[test]
    fn interrupted_work_requeues_below_cap_and_exhausts_at_cap() {
        let mut conn = setup();
        insert(&conn, "process", process_ref(), 1);
        let first = claim_next(&mut conn, 2).unwrap().unwrap();
        assert_eq!(
            requeue_interrupted(&mut conn, 3).unwrap(),
            RequeueResult {
                pending: 1,
                exhausted: 0
            }
        );
        let second = claim_next(&mut conn, 4).unwrap().unwrap();
        assert_eq!(second.attempt, first.attempt + 1);
        fail(&mut conn, &second, "retry", 5).unwrap();
        let third = claim_next(&mut conn, 6).unwrap().unwrap();
        assert_eq!(
            requeue_interrupted(&mut conn, 7).unwrap(),
            RequeueResult {
                pending: 0,
                exhausted: 1
            }
        );
        assert_eq!(third.attempt, MAX_CLEANUP_ATTEMPTS);
    }

    #[test]
    fn invalid_intents_exhaust_loudly_without_being_claimed() {
        let mut conn = setup();
        insert(&conn, "mystery", "{}", 1);
        insert(&conn, "process", "not-json", 2);
        insert(
            &conn,
            "branch",
            &format!(
                r#"{{"expected_sha":"x","name":"{}"}}"#,
                "x".repeat(MAX_CLEANUP_ARTIFACT_BYTES)
            ),
            3,
        );
        assert!(claim_next(&mut conn, 4).unwrap().is_none());
        let rows: Vec<(String, String)> = conn.prepare(
            "SELECT state,last_error FROM decomposition_cleanup ORDER BY updated_at,artifact_kind"
        ).unwrap().query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
            .collect::<rusqlite::Result<_>>().unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows
            .iter()
            .all(|(state, error)| state == "exhausted" && !error.is_empty()));
    }

    #[test]
    fn graph_member_gates_and_per_task_action_order_hold() {
        let mut conn = setup();
        insert(&conn, "branch", r#"{"expected_sha":"abc","name":"b"}"#, 1);
        insert(&conn, "worktree", r#"{"branch":"b","path":"/tmp/w"}"#, 1);
        insert(
            &conn,
            "proposed-change",
            r#"{"head_ref":"b","head_sha":"abc","pr_number":7}"#,
            1,
        );
        insert(&conn, "process", process_ref(), 1);
        for expected in ["process", "proposed-change", "worktree", "branch"] {
            let work = claim_next(&mut conn, 2).unwrap().unwrap();
            assert_eq!(work.key.artifact_kind, expected);
            assert!(complete(&mut conn, &work, 3).unwrap());
        }
        conn.execute("UPDATE decomposition_cleanup SET state='pending'", [])
            .unwrap();
        conn.execute("UPDATE task_decompositions SET state='completed'", [])
            .unwrap();
        assert!(claim_next(&mut conn, 4).unwrap().is_none());
        conn.execute("UPDATE task_decompositions SET state='cancelled'", [])
            .unwrap();
        conn.execute("DELETE FROM task_graph_members", []).unwrap();
        assert!(claim_next(&mut conn, 5).unwrap().is_none());
    }

    #[test]
    #[ignore]
    fn process_claim_helper() {
        let Ok(path) = std::env::var("QUORUM_CLEANUP_CLAIM_DB") else {
            return;
        };
        let mut conn = crate::db::open(std::path::Path::new(&path)).unwrap();
        let won = claim_next(&mut conn, 20).unwrap().is_some();
        println!("CLEANUP_CLAIM_WIN={}", i32::from(won));
    }

    #[test]
    fn concurrent_processes_have_exactly_one_claim_winner() {
        let binary = std::env::current_exe().unwrap();
        for iteration in 0..8 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("quorum-{iteration}.db"));
            let conn = crate::db::open(&path).unwrap();
            seed(&conn);
            insert(&conn, "process", process_ref(), 1);
            drop(conn);
            let spawn = || {
                Command::new(&binary)
                    .args([
                        "--ignored",
                        "--exact",
                        "decomposition_cleanup::tests::process_claim_helper",
                        "--nocapture",
                    ])
                    .env("QUORUM_CLEANUP_CLAIM_DB", &path)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap()
            };
            let a = spawn();
            let b = spawn();
            let outputs = [a.wait_with_output().unwrap(), b.wait_with_output().unwrap()];
            assert!(outputs.iter().all(|output| output.status.success()));
            let winners = outputs
                .iter()
                .filter(|output| {
                    String::from_utf8_lossy(&output.stdout).contains("CLEANUP_CLAIM_WIN=1")
                })
                .count();
            assert_eq!(winners, 1, "iteration {iteration}");
            let conn = crate::db::open(&path).unwrap();
            let state: (String, i64) = conn
                .query_row(
                    "SELECT state,attempts FROM decomposition_cleanup",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(state, ("running".into(), 1), "iteration {iteration}");
        }
    }

    #[test]
    fn v39_migration_preserves_terminal_and_requeues_legacy_failed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let conn = crate::db::open(&path).unwrap();
        seed(&conn);
        conn.execute_batch(
            r#"PRAGMA foreign_keys=OFF;
             DROP TABLE decomposition_cleanup;
             CREATE TABLE decomposition_cleanup (
                 graph_id INTEGER NOT NULL REFERENCES task_decompositions(id),
                 task_id INTEGER NOT NULL REFERENCES tasks(id),
                 artifact_kind TEXT NOT NULL, artifact_ref TEXT NOT NULL,
                 state TEXT NOT NULL DEFAULT 'pending'
                     CHECK(state IN ('pending','complete','failed')),
                 attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY(graph_id,task_id,artifact_kind,artifact_ref));
             INSERT INTO decomposition_cleanup VALUES
                 (1,2,'process','{"agent":"done","pid":1,"session_id":"s"}',
                    'complete',1,NULL,10),
                 (1,2,'branch','{"expected_sha":"abc","name":"retry"}',
                    'failed',1,'old retryable failure',11),
                 (1,2,'worktree','{"branch":"cap","path":"/tmp/cap"}',
                    'failed',3,'old capped failure',12);
             PRAGMA user_version=38;
             PRAGMA foreign_keys=ON;"#,
        )
        .unwrap();
        drop(conn);

        let mut conn = crate::db::open(&path).unwrap();
        type LegacyRow = (String, String, String, i64, Option<String>, i64);
        let rows: Vec<LegacyRow> = conn
            .prepare(
                "SELECT artifact_kind,artifact_ref,state,attempts,last_error,updated_at
                      FROM decomposition_cleanup ORDER BY updated_at",
            )
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "process".into(),
                    r#"{"agent":"done","pid":1,"session_id":"s"}"#.into(),
                    "done".into(),
                    1,
                    None,
                    10
                ),
                (
                    "branch".into(),
                    r#"{"expected_sha":"abc","name":"retry"}"#.into(),
                    "pending".into(),
                    1,
                    Some("old retryable failure".into()),
                    11
                ),
                (
                    "worktree".into(),
                    r#"{"branch":"cap","path":"/tmp/cap"}"#.into(),
                    "pending".into(),
                    3,
                    Some("old capped failure".into()),
                    12
                ),
            ]
        );
        assert_eq!(
            conn.query_row("PRAGMA foreign_key_check", [], |_| Ok(0))
                .optional()
                .unwrap(),
            None
        );
        assert!(conn.execute(
            "INSERT INTO decomposition_cleanup(graph_id,task_id,artifact_kind,artifact_ref,state,updated_at)
             VALUES (999,2,'process','{}','pending',1)", []).is_err());
        assert!(conn.execute(
            "INSERT INTO decomposition_cleanup(graph_id,task_id,artifact_kind,artifact_ref,state,updated_at)
             VALUES (1,2,'process','{}','failed',1)", []).is_err());

        let retry = claim_next(&mut conn, 20).unwrap().unwrap();
        assert_eq!(retry.key.artifact_kind, "branch");
        assert_eq!(retry.attempt, 2);
        assert_eq!(
            fail(&mut conn, &retry, "still failing", 21).unwrap(),
            Some(FailureOutcome::RetryPending)
        );
        let final_retry = claim_next(&mut conn, 22).unwrap().unwrap();
        assert_eq!(final_retry.attempt, 3);
        assert!(complete(&mut conn, &final_retry, 23).unwrap());
        assert!(claim_next(&mut conn, 24).unwrap().is_none());
        let capped: (String, i64, String) = conn.query_row(
            "SELECT state,attempts,last_error FROM decomposition_cleanup WHERE artifact_kind='worktree'",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        assert_eq!(
            capped,
            (
                "exhausted".into(),
                3,
                "cleanup retry limit reached before claim".into()
            )
        );
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            crate::db::SCHEMA_VERSION
        );
        drop(conn);

        let reopened = crate::db::open(&path).unwrap();
        assert_eq!(
            reopened
                .query_row("SELECT count(*) FROM decomposition_cleanup", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            reopened
                .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            crate::db::SCHEMA_VERSION
        );
    }
}
