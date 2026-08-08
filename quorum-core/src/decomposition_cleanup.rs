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

/// Atomically converts a validated non-destructive branch discovery into an
/// immutable CAS deletion. The caller resolves and validates `expected_sha`
/// outside the transaction; this transaction revalidates allocation authority.
pub fn finalize_branch_discovery(
    conn: &mut Connection,
    work: &CleanupWork,
    expected_sha: &str,
    tombstone_ref: &str,
    now: i64,
) -> Result<bool> {
    if work.key.artifact_kind != "branch-discovery"
        || !matches!(expected_sha.len(), 40 | 64)
        || !expected_sha.bytes().all(|b| b.is_ascii_hexdigit())
        || !tombstone_ref.starts_with("refs/quorum/cleanup/")
        || tombstone_ref.contains('\0')
    {
        return Err(QuorumError::Usage(
            "invalid branch discovery finalization".into(),
        ));
    }
    let value: Value = serde_json::from_str(&work.key.artifact_ref)
        .map_err(|_| QuorumError::Usage("invalid branch discovery intent".into()))?;
    let allocation_id = value["allocation_id"]
        .as_i64()
        .ok_or_else(|| QuorumError::Usage("invalid allocation id".into()))?;
    let allocated_at = value["allocated_at"]
        .as_i64()
        .ok_or_else(|| QuorumError::Usage("invalid allocation time".into()))?;
    let allocated_by = value["allocated_by"]
        .as_str()
        .ok_or_else(|| QuorumError::Usage("invalid allocator".into()))?;
    let name = value["name"]
        .as_str()
        .ok_or_else(|| QuorumError::Usage("invalid branch".into()))?;
    let path = value["path"]
        .as_str()
        .ok_or_else(|| QuorumError::Usage("invalid worktree".into()))?;
    let provenance = value["provenance_sha"]
        .as_str()
        .ok_or_else(|| QuorumError::Usage("invalid provenance".into()))?;
    let tx = begin_immediate(conn)?;
    let authoritative: Option<i64> = tx
        .query_row(
            "SELECT id FROM task_branches WHERE id=?1 AND task_id=?2 AND branch=?3 AND worktree=?4
           AND allocated_by=?5 AND allocated_at=?6 AND provenance_sha=?7",
            params![
                allocation_id,
                work.key.task_id,
                name,
                path,
                allocated_by,
                allocated_at,
                provenance
            ],
            |row| row.get(0),
        )
        .optional()?;
    if authoritative.is_none() {
        tx.commit().map_err(map_sql_err)?;
        return Ok(false);
    }
    let running: Option<i64> = tx.query_row(
        "SELECT 1 FROM decomposition_cleanup WHERE graph_id=?1 AND task_id=?2 AND artifact_kind=?3
           AND artifact_ref=?4 AND state='running' AND attempts=?5",
        params![work.key.graph_id, work.key.task_id, work.key.artifact_kind, work.key.artifact_ref, work.attempt],
        |row| row.get(0),
    ).optional()?;
    if running.is_none() {
        tx.commit().map_err(map_sql_err)?;
        return Ok(false);
    }
    let delete_ref = serde_json::json!({"allocation_id":allocation_id,"expected_sha":expected_sha.to_ascii_lowercase(),"name":name,"tombstone_ref":tombstone_ref}).to_string();
    tx.execute("INSERT OR IGNORE INTO decomposition_cleanup(graph_id,task_id,artifact_kind,artifact_ref,state,attempts,updated_at) VALUES (?1,?2,'branch-delete',?3,'pending',0,?4)", params![work.key.graph_id, work.key.task_id, delete_ref, now])?;
    tx.execute("UPDATE decomposition_cleanup SET state='done',last_error=NULL,updated_at=?6 WHERE graph_id=?1 AND task_id=?2 AND artifact_kind=?3 AND artifact_ref=?4 AND state='running' AND attempts=?5",
        params![work.key.graph_id, work.key.task_id, work.key.artifact_kind, work.key.artifact_ref, work.attempt, now])?;
    tx.commit().map_err(map_sql_err)?;
    Ok(true)
}

/// Convert a definitive known-SHA branch lease to the same tombstoned CAS
/// deletion used by discovery, so crash/recreation safety is uniform.
pub fn finalize_known_branch(
    conn: &mut Connection,
    work: &CleanupWork,
    allocation_id: i64,
    name: &str,
    expected_sha: &str,
    tombstone_ref: &str,
    now: i64,
) -> Result<bool> {
    if work.key.artifact_kind != "branch"
        || allocation_id <= 0
        || !matches!(expected_sha.len(), 40 | 64)
        || !expected_sha.bytes().all(|b| b.is_ascii_hexdigit())
        || !tombstone_ref.starts_with("refs/quorum/cleanup/")
    {
        return Err(QuorumError::Usage(
            "invalid known branch finalization".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let authority: Option<i64> = tx
        .query_row(
            "SELECT 1 FROM task_branches WHERE id=?1 AND task_id=?2 AND branch=?3",
            params![allocation_id, work.key.task_id, name],
            |r| r.get(0),
        )
        .optional()?;
    let running: Option<i64> = tx.query_row("SELECT 1 FROM decomposition_cleanup WHERE graph_id=?1 AND task_id=?2 AND artifact_kind='branch' AND artifact_ref=?3 AND state='running' AND attempts=?4",
        params![work.key.graph_id,work.key.task_id,work.key.artifact_ref,work.attempt], |r| r.get(0)).optional()?;
    if authority.is_none() || running.is_none() {
        tx.commit().map_err(map_sql_err)?;
        return Ok(false);
    }
    let delete_ref = serde_json::json!({"allocation_id":allocation_id,"expected_sha":expected_sha,"name":name,"tombstone_ref":tombstone_ref}).to_string();
    tx.execute("INSERT OR IGNORE INTO decomposition_cleanup(graph_id,task_id,artifact_kind,artifact_ref,state,attempts,updated_at) VALUES (?1,?2,'branch-delete',?3,'pending',0,?4)", params![work.key.graph_id,work.key.task_id,delete_ref,now])?;
    tx.execute("UPDATE decomposition_cleanup SET state='done',last_error=NULL,updated_at=?5 WHERE graph_id=?1 AND task_id=?2 AND artifact_kind='branch' AND artifact_ref=?3 AND state='running' AND attempts=?4",
        params![work.key.graph_id,work.key.task_id,work.key.artifact_ref,work.attempt,now])?;
    tx.commit().map_err(map_sql_err)?;
    Ok(true)
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
                 AND prior.state != 'done'
                 AND CASE prior.artifact_kind
                       WHEN 'process' THEN 1 WHEN 'proposed-change' THEN 2
                       WHEN 'worktree' THEN 3 WHEN 'branch-discovery' THEN 4
                       WHEN 'branch' THEN 5 WHEN 'branch-delete' THEN 5 ELSE 0 END
                     < CASE c.artifact_kind
                       WHEN 'process' THEN 1 WHEN 'proposed-change' THEN 2
                       WHEN 'worktree' THEN 3 WHEN 'branch-discovery' THEN 4
                       WHEN 'branch' THEN 5 WHEN 'branch-delete' THEN 5 ELSE 0 END)
         ORDER BY c.updated_at,c.graph_id,c.task_id,
                  CASE c.artifact_kind WHEN 'process' THEN 1 WHEN 'proposed-change' THEN 2
                       WHEN 'worktree' THEN 3 WHEN 'branch-discovery' THEN 4
                       WHEN 'branch' THEN 5 WHEN 'branch-delete' THEN 5 ELSE 0 END,
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
                && oid(object, "head_sha")
                && object
                    .get("pr_number")
                    .and_then(Value::as_i64)
                    .is_some_and(|n| n > 0)
        }
        "worktree" => exact(object, &["branch", "path"]) && strings(object, &["branch", "path"]),
        "branch" => {
            exact(object, &["expected_sha", "name"])
                && strings(object, &["expected_sha", "name"])
                && oid(object, "expected_sha")
                && branch_name(object, "name")
        }
        "branch-discovery" => {
            exact(
                object,
                &[
                    "allocated_at",
                    "allocated_by",
                    "allocation_id",
                    "name",
                    "path",
                    "provenance_sha",
                ],
            ) && strings(object, &["allocated_by", "name", "path", "provenance_sha"])
                && oid(object, "provenance_sha")
                && branch_name(object, "name")
                && object
                    .get("allocation_id")
                    .and_then(Value::as_i64)
                    .is_some_and(|v| v > 0)
                && object
                    .get("allocated_at")
                    .and_then(Value::as_i64)
                    .is_some_and(|v| v > 0)
        }
        "branch-delete" => {
            exact(
                object,
                &["allocation_id", "expected_sha", "name", "tombstone_ref"],
            ) && strings(object, &["expected_sha", "name", "tombstone_ref"])
                && oid(object, "expected_sha")
                && branch_name(object, "name")
                && object
                    .get("allocation_id")
                    .and_then(Value::as_i64)
                    .is_some_and(|v| v > 0)
                && deterministic_tombstone(key, object)
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

fn oid(object: &Map<String, Value>, key: &str) -> bool {
    object
        .get(key)
        .and_then(Value::as_str)
        .is_some_and(|value| {
            matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn branch_name(object: &Map<String, Value>, key: &str) -> bool {
    object.get(key).and_then(Value::as_str).is_some_and(|name| {
        !name.is_empty()
            && name.len() <= 255
            && name != "@"
            && !name.starts_with('-')
            && !name.starts_with('/')
            && !name.ends_with('/')
            && !name.ends_with('.')
            && !name.contains("//")
            && !name.contains("..")
            && !name.contains("@{")
            && !name
                .bytes()
                .any(|byte| byte <= b' ' || byte == 0x7f || b"~^:?*[\\".contains(&byte))
            && name
                .split('/')
                .all(|part| !part.is_empty() && !part.starts_with('.') && !part.ends_with(".lock"))
    })
}

fn deterministic_tombstone(key: &CleanupKey, object: &Map<String, Value>) -> bool {
    if key.graph_id <= 0 || key.task_id <= 0 {
        return false;
    }
    let Some(allocation_id) = object
        .get("allocation_id")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
    else {
        return false;
    };
    let expected = format!(
        "refs/quorum/cleanup/{}/{}/{}",
        key.graph_id, key.task_id, allocation_id
    );
    object.get("tombstone_ref").and_then(Value::as_str) == Some(expected.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let rows: Vec<(String, Option<String>)> = conn.prepare(
            "SELECT state,last_error FROM decomposition_cleanup ORDER BY updated_at,artifact_kind"
        ).unwrap().query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
            .collect::<rusqlite::Result<_>>().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter()
                .filter(|(state, _)| state == "exhausted")
                .count(),
            1
        );
        assert!(rows.iter().any(|(state, error)| state == "exhausted"
            && error.as_deref().is_some_and(|error| !error.is_empty())));
        assert_eq!(
            rows.iter()
                .filter(|(state, error)| state == "pending" && error.is_none())
                .count(),
            2
        );
    }

    #[test]
    fn corrupt_earlier_branch_artifact_blocks_later_external_execution() {
        let mut conn = setup();
        insert(
            &conn,
            "branch",
            r#"{"expected_sha":"not-an-oid","name":"daemon/x"}"#,
            1,
        );
        insert(
            &conn,
            "branch-discovery",
            &serde_json::json!({
                "allocated_at":1,"allocated_by":"agent","allocation_id":7,
                "name":"refs/heads/../victim","path":"/tmp/w","provenance_sha":"a".repeat(40)
            })
            .to_string(),
            2,
        );
        insert(
            &conn,
            "branch-delete",
            &serde_json::json!({
                "allocation_id":7,"expected_sha":"b".repeat(40),"name":"daemon/x",
                "tombstone_ref":"refs/quorum/cleanup/999/2/7"
            })
            .to_string(),
            3,
        );
        assert!(
            claim_next(&mut conn, 4).unwrap().is_none(),
            "corrupt rows must never escape to an external runner"
        );
        let rows: Vec<(String, i64, Option<String>)> = conn
            .prepare(
                "SELECT state,attempts,last_error FROM decomposition_cleanup ORDER BY updated_at",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter()
                .filter(|(state, attempts, error)| state == "exhausted"
                    && *attempts == 0
                    && error.as_deref().is_some_and(|error| !error.is_empty()))
                .count(),
            1
        );
        assert_eq!(
            rows.iter()
                .filter(|(state, attempts, error)| state == "pending"
                    && *attempts == 0
                    && error.is_none())
                .count(),
            2
        );
    }

    #[test]
    fn graph_member_gates_and_per_task_action_order_hold() {
        let mut conn = setup();
        insert(
            &conn,
            "branch",
            &serde_json::json!({"expected_sha":"a".repeat(40),"name":"b"}).to_string(),
            1,
        );
        insert(&conn, "worktree", r#"{"branch":"b","path":"/tmp/w"}"#, 1);
        insert(
            &conn,
            "proposed-change",
            &serde_json::json!({"head_ref":"b","head_sha":"a".repeat(40),"pr_number":7})
                .to_string(),
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
    fn exhausted_earlier_action_blocks_later_destructive_cleanup() {
        let mut conn = setup();
        insert(&conn, "process", process_ref(), 1);
        insert(&conn, "worktree", r#"{"branch":"b","path":"/tmp/w"}"#, 2);
        for now in [3, 4, 5] {
            let process = claim_next(&mut conn, now).unwrap().unwrap();
            assert_eq!(process.key.artifact_kind, "process");
            fail(&mut conn, &process, "process identity mismatch", now + 10).unwrap();
        }
        assert!(claim_next(&mut conn, 20).unwrap().is_none());
        assert_eq!(
            conn.query_row(
                "SELECT state FROM decomposition_cleanup WHERE artifact_kind='worktree'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "pending"
        );
    }

    #[test]
    fn malformed_proposed_change_oid_exhausts_before_execution() {
        let mut conn = setup();
        insert(
            &conn,
            "proposed-change",
            r#"{"head_ref":"daemon/task","head_sha":"not-an-oid","pr_number":7}"#,
            1,
        );
        assert!(claim_next(&mut conn, 2).unwrap().is_none());
        assert_eq!(
            conn.query_row(
                "SELECT state FROM decomposition_cleanup WHERE artifact_kind='proposed-change'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "exhausted"
        );
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
                 (1,2,'branch','{"expected_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"retry"}',
                    'failed',1,'old retryable failure',11),
                 (1,2,'branch','{"expected_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","name":"cap"}',
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
                    r#"{"expected_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"retry"}"#
                        .into(),
                    "pending".into(),
                    1,
                    Some("old retryable failure".into()),
                    11
                ),
                (
                    "branch".into(),
                    r#"{"expected_sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","name":"cap"}"#
                        .into(),
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
        let capped: (String, i64, String) = conn
            .query_row(
                "SELECT state,attempts,last_error FROM decomposition_cleanup
             WHERE artifact_kind='branch' AND artifact_ref LIKE '%\"name\":\"cap\"%'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
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
