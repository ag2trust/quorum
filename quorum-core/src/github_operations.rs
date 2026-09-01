//! Durable outbox storage for GitHub agent operations (task #126).
//!
//! This module owns the low-level read/write shape of the
//! `github_agent_operations` table. The higher-level admission surface added in
//! task #104 (`collaboration_admission::admit_operation`) enforces caller /
//! attempt / capacity checks on top of the same table; the primitives here are
//! deliberately narrower: they persist one queued row and read it back, with
//! no daemon loop, no claim, no retry scheduling, and no reconciliation.
//!
//! Idempotency: every row is keyed on the deterministic `operation_id`
//! (`derive_operation_id` in `collaboration_admission`). [`enqueue`] runs
//! inside a single `BEGIN IMMEDIATE` and re-issues that stay bit-identical
//! observe the existing row: `INSERT ... ON CONFLICT(operation_id) DO NOTHING`
//! preserves any progress the executor (once it exists) has made — attempts,
//! send_state, response, error, and claim/execution stamps.
//!
//! Text safety: every free-text field is validated for embedded NUL. `&str`
//! already guarantees valid UTF-8 at the type boundary. Length and structural
//! validation (JSON shape, kind vocabulary, etc.) belongs to the admission
//! layer above — this module does not re-invent it.

use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

/// One row of `github_agent_operations`, read-only. Callers project the columns
/// they need — every non-request field is present so the future claim / retry
/// / reconciliation children can round-trip a row without a second query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GithubAgentOperation {
    pub id: i64,
    pub operation_id: String,
    pub client_request_id: String,
    pub attempt_id: String,
    pub created_by_run_id: String,
    pub task_id: i64,
    pub agent: String,
    pub role: String,
    pub pr_number: i64,
    pub head_sha: Option<String>,
    pub reviewer_launch_sha: Option<String>,
    pub lifecycle_generation: Option<i64>,
    pub kind: String,
    pub request_json: String,
    pub state: String,
    pub send_state: String,
    pub attempts: i64,
    pub next_attempt_at: Option<i64>,
    pub deadline_at: i64,
    pub review_sequence: Option<i64>,
    pub group_key: Option<String>,
    pub github_marker: Option<String>,
    pub remote_object_id: Option<String>,
    pub response_json: Option<String>,
    pub error_kind: Option<String>,
    pub error_summary: Option<String>,
    pub completed_after_revocation: bool,
    pub claimed_at: Option<i64>,
    pub execution_started_at: Option<i64>,
    pub active: bool,
    pub created_at: i64,
    pub updated_at: i64,
    pub expires_at: i64,
}

/// Column projection order shared by every read path so `row_from_sql` stays
/// tied to a single SELECT clause. Extending the row means editing this
/// constant and `row_from_sql` in lock-step.
const SELECT_COLUMNS: &str = "id, operation_id, client_request_id, attempt_id, created_by_run_id, \
     task_id, agent, role, pr_number, head_sha, reviewer_launch_sha, \
     lifecycle_generation, kind, request_json, state, send_state, attempts, \
     next_attempt_at, deadline_at, review_sequence, group_key, github_marker, \
     remote_object_id, response_json, error_kind, error_summary, \
     completed_after_revocation, claimed_at, execution_started_at, active, \
     created_at, updated_at, expires_at";

pub(crate) fn row_from_sql(r: &rusqlite::Row<'_>) -> rusqlite::Result<GithubAgentOperation> {
    Ok(GithubAgentOperation {
        id: r.get(0)?,
        operation_id: r.get(1)?,
        client_request_id: r.get(2)?,
        attempt_id: r.get(3)?,
        created_by_run_id: r.get(4)?,
        task_id: r.get(5)?,
        agent: r.get(6)?,
        role: r.get(7)?,
        pr_number: r.get(8)?,
        head_sha: r.get(9)?,
        reviewer_launch_sha: r.get(10)?,
        lifecycle_generation: r.get(11)?,
        kind: r.get(12)?,
        request_json: r.get(13)?,
        state: r.get(14)?,
        send_state: r.get(15)?,
        attempts: r.get(16)?,
        next_attempt_at: r.get(17)?,
        deadline_at: r.get(18)?,
        review_sequence: r.get(19)?,
        group_key: r.get(20)?,
        github_marker: r.get(21)?,
        remote_object_id: r.get(22)?,
        response_json: r.get(23)?,
        error_kind: r.get(24)?,
        error_summary: r.get(25)?,
        completed_after_revocation: r.get::<_, i64>(26)? != 0,
        claimed_at: r.get(27)?,
        execution_started_at: r.get(28)?,
        active: r.get::<_, i64>(29)? != 0,
        created_at: r.get(30)?,
        updated_at: r.get(31)?,
        expires_at: r.get(32)?,
    })
}

/// One prepared outbox request. Every field is a snapshot of the immutable
/// contract the enqueue path writes: nothing here is inferred from live state
/// at insert time, and every field re-issued by an idempotent retry is
/// verified to match what already sits in the row via the `operation_id`
/// UNIQUE constraint.
#[derive(Debug, Clone)]
pub struct NewGithubOperation<'a> {
    pub operation_id: &'a str,
    pub client_request_id: &'a str,
    pub attempt_id: &'a str,
    pub created_by_run_id: &'a str,
    pub task_id: i64,
    pub agent: &'a str,
    pub role: &'a str,
    pub pr_number: i64,
    pub head_sha: Option<&'a str>,
    pub reviewer_launch_sha: Option<&'a str>,
    pub lifecycle_generation: Option<i64>,
    pub kind: &'a str,
    pub request_json: &'a str,
    pub deadline_at: i64,
    pub review_sequence: Option<i64>,
    pub group_key: Option<&'a str>,
    pub github_marker: Option<&'a str>,
    pub created_at: i64,
    pub expires_at: i64,
}

fn reject_nul(value: &str, field: &'static str) -> Result<()> {
    if value.as_bytes().contains(&0) {
        return Err(QuorumError::Usage(format!(
            "github operation {field} contains an embedded NUL"
        )));
    }
    Ok(())
}

fn reject_nul_opt(value: Option<&str>, field: &'static str) -> Result<()> {
    match value {
        Some(v) => reject_nul(v, field),
        None => Ok(()),
    }
}

fn validate(request: &NewGithubOperation<'_>) -> Result<()> {
    // Empty required identifiers would otherwise land as blank strings and
    // silently confuse downstream reconciliation — reject at the boundary.
    for (value, field) in [
        (request.operation_id, "operation_id"),
        (request.client_request_id, "client_request_id"),
        (request.attempt_id, "attempt_id"),
        (request.created_by_run_id, "created_by_run_id"),
        (request.agent, "agent"),
        (request.role, "role"),
        (request.kind, "kind"),
        (request.request_json, "request_json"),
    ] {
        if value.is_empty() {
            return Err(QuorumError::Usage(format!(
                "github operation {field} must not be empty"
            )));
        }
        reject_nul(value, field)?;
    }
    reject_nul_opt(request.head_sha, "head_sha")?;
    reject_nul_opt(request.reviewer_launch_sha, "reviewer_launch_sha")?;
    reject_nul_opt(request.group_key, "group_key")?;
    reject_nul_opt(request.github_marker, "github_marker")?;
    Ok(())
}

/// Insert a queued outbox row. Idempotent on `operation_id`: a second call
/// with the same `operation_id` observes the existing row and does not
/// overwrite `attempts`, `state`, `send_state`, `next_attempt_at`,
/// `response_json`, `error_kind`, `error_summary`, `claimed_at`, or `active`.
/// Runs inside one `BEGIN IMMEDIATE`, so racing enqueues serialize on the
/// database write lock and land in a deterministic order rather than
/// clobbering each other.
///
/// Returns `true` when this call inserted a fresh row, `false` when the
/// existing row was reused. `false` is the load-bearing outcome for
/// idempotent retries — the caller learns the operation was already queued
/// without having to re-select it.
pub fn enqueue(conn: &mut Connection, request: &NewGithubOperation<'_>) -> Result<bool> {
    validate(request)?;
    let tx = begin_immediate(conn)?;
    let changed = tx.execute(
        "INSERT INTO github_agent_operations (
             operation_id, client_request_id, attempt_id, created_by_run_id,
             task_id, agent, role, pr_number, head_sha, reviewer_launch_sha,
             lifecycle_generation, kind, request_json, state, send_state,
             attempts, deadline_at, review_sequence, group_key, github_marker,
             completed_after_revocation, active, created_at, updated_at, expires_at
         ) VALUES (
             ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,'queued','not_started',
             0,?14,?15,?16,?17,0,0,?18,?18,?19
         )
         ON CONFLICT(operation_id) DO NOTHING",
        params![
            request.operation_id,
            request.client_request_id,
            request.attempt_id,
            request.created_by_run_id,
            request.task_id,
            request.agent,
            request.role,
            request.pr_number,
            request.head_sha,
            request.reviewer_launch_sha,
            request.lifecycle_generation,
            request.kind,
            request.request_json,
            request.deadline_at,
            request.review_sequence,
            request.group_key,
            request.github_marker,
            request.created_at,
            request.expires_at,
        ],
    )?;
    tx.commit()?;
    Ok(changed > 0)
}

/// Fetch one row by its deterministic `operation_id`. Non-transactional read;
/// callers do not need to hold the write lock to poll for a queued row.
pub fn get_by_operation_id(
    conn: &Connection,
    operation_id: &str,
) -> Result<Option<GithubAgentOperation>> {
    let sql = format!(
        "SELECT {SELECT_COLUMNS}
         FROM github_agent_operations WHERE operation_id = ?1"
    );
    let row = conn
        .query_row(&sql, params![operation_id], row_from_sql)
        .optional()?;
    Ok(row)
}

/// List every outbox row, oldest-first by insertion order. Debug / inspection
/// helper — the executor children will paginate through their own filters
/// (state, next_attempt_at, group_key) rather than list-all.
pub fn list_all(conn: &Connection) -> Result<Vec<GithubAgentOperation>> {
    let sql = format!(
        "SELECT {SELECT_COLUMNS}
         FROM github_agent_operations ORDER BY id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], row_from_sql)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    /// Seed the FK targets the outbox row needs: one task, one run capability,
    /// one collaboration attempt bound to that capability. Every enqueue in
    /// these tests reuses the same triple so idempotency-by-`operation_id`
    /// is the axis under test rather than incidental FK plumbing.
    fn seed_dependencies(conn: &mut Connection) {
        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (1,'src','working','owner',1,1);
             INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('run-1',1,'Worker','worker',1);
             INSERT INTO github_collaboration_attempts(
                 attempt_id,task_id,agent,role,pr_number,lifecycle_generation,
                 active_run_id,state,created_at,updated_at,expires_at
             ) VALUES ('att-1',1,'Worker','worker',7,3,'run-1','active',1,1,999);",
        )
        .unwrap();
    }

    fn sample_request<'a>() -> NewGithubOperation<'a> {
        NewGithubOperation {
            operation_id: "op-1",
            client_request_id: "req-1",
            attempt_id: "att-1",
            created_by_run_id: "run-1",
            task_id: 1,
            agent: "Worker",
            role: "worker",
            pr_number: 7,
            head_sha: None,
            reviewer_launch_sha: None,
            lifecycle_generation: Some(3),
            kind: "pull_request_read",
            request_json: r#"{"pr":7}"#,
            deadline_at: 500,
            review_sequence: None,
            group_key: Some("pr:7:pull_request_read"),
            github_marker: None,
            created_at: 100,
            expires_at: 999,
        }
    }

    fn test_conn() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = db::open(&dir.path().join("q.db")).unwrap();
        seed_dependencies(&mut conn);
        (conn, dir)
    }

    #[test]
    fn enqueue_inserts_queued_row_readable_by_operation_id() {
        let (mut conn, _dir) = test_conn();
        assert!(get_by_operation_id(&conn, "op-1").unwrap().is_none());
        assert!(enqueue(&mut conn, &sample_request()).unwrap());

        let got = get_by_operation_id(&conn, "op-1").unwrap().unwrap();
        assert_eq!(got.operation_id, "op-1");
        assert_eq!(got.attempt_id, "att-1");
        assert_eq!(got.created_by_run_id, "run-1");
        assert_eq!(got.task_id, 1);
        assert_eq!(got.pr_number, 7);
        assert_eq!(got.kind, "pull_request_read");
        assert_eq!(got.state, "queued");
        assert_eq!(got.send_state, "not_started");
        assert_eq!(got.attempts, 0);
        assert_eq!(got.deadline_at, 500);
        assert_eq!(got.lifecycle_generation, Some(3));
        assert_eq!(got.group_key.as_deref(), Some("pr:7:pull_request_read"));
        assert_eq!(got.expires_at, 999);
        assert_eq!(got.created_at, 100);
        assert_eq!(got.updated_at, 100);
        assert!(!got.completed_after_revocation);
        assert!(!got.active);
        assert!(got.claimed_at.is_none());
        assert!(got.execution_started_at.is_none());

        // list_all surfaces the same row.
        let all = list_all(&conn).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].operation_id, "op-1");
    }

    #[test]
    fn enqueue_is_idempotent_and_preserves_executor_progress() {
        let (mut conn, _dir) = test_conn();
        assert!(enqueue(&mut conn, &sample_request()).unwrap());

        // Simulate a downstream executor advancing the row's mutable state:
        // attempts, send_state, and error_summary — none of which enqueue
        // must ever overwrite on an idempotent retry.
        conn.execute(
            "UPDATE github_agent_operations
             SET attempts = 2, send_state = 'ambiguous',
                 error_kind = 'transient', error_summary = 'gh api 502',
                 updated_at = 250
             WHERE operation_id = 'op-1'",
            [],
        )
        .unwrap();

        // Second enqueue is a no-op: returns false, does not clobber progress.
        assert!(!enqueue(&mut conn, &sample_request()).unwrap());

        let got = get_by_operation_id(&conn, "op-1").unwrap().unwrap();
        assert_eq!(got.attempts, 2);
        assert_eq!(got.send_state, "ambiguous");
        assert_eq!(got.error_kind.as_deref(), Some("transient"));
        assert_eq!(got.error_summary.as_deref(), Some("gh api 502"));
        assert_eq!(got.updated_at, 250);

        // A drifted retry (different client_request_id, same operation_id)
        // is likewise a no-op — the row stays as it was.
        let mut drifted = sample_request();
        drifted.client_request_id = "req-drifted";
        assert!(!enqueue(&mut conn, &drifted).unwrap());
        let after = get_by_operation_id(&conn, "op-1").unwrap().unwrap();
        assert_eq!(after.client_request_id, "req-1");
    }

    #[test]
    fn enqueue_rejects_embedded_nul() {
        let (mut conn, _dir) = test_conn();
        let mut bad = sample_request();
        // Embedded NUL in the free-text kind. Rust already rejects invalid
        // UTF-8 at the `&str` boundary; the module-owned NUL guard closes
        // the remaining hole in the text-safety invariant.
        let with_nul = "pull_request_read\0";
        bad.kind = with_nul;
        let err = enqueue(&mut conn, &bad).unwrap_err();
        assert!(
            matches!(err, QuorumError::Usage(_)),
            "expected Usage: {err:?}"
        );
        assert!(get_by_operation_id(&conn, "op-1").unwrap().is_none());
    }

    #[test]
    fn enqueue_rejects_empty_required_field() {
        let (mut conn, _dir) = test_conn();
        let mut bad = sample_request();
        bad.operation_id = "";
        let err = enqueue(&mut conn, &bad).unwrap_err();
        assert!(
            matches!(err, QuorumError::Usage(_)),
            "expected Usage: {err:?}"
        );
    }
}
