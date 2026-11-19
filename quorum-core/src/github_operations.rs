//! Durable outbox storage for GitHub agent operations (task #126).
//!
//! This module owns the low-level read/write shape of the
//! `github_agent_operations` table. The higher-level admission surface added in
//! task #104 (`collaboration_admission::admit_operation`) enforces caller /
//! attempt / capacity checks on top of the same table; the primitives here are
//! deliberately narrower: they persist and read rows, and (task #435) expose
//! the storage-level executor primitives — claim, execution start, settle,
//! retry scheduling, and startup reconciliation — with no daemon loop and no
//! network I/O. Every primitive here is one short `BEGIN IMMEDIATE`; callers
//! perform GitHub I/O only *between* primitives, never inside one.
//!
//! State vocabulary (no schema extension needed): a claim is
//! `queued -> running` with `active = 1`; terminal states are `succeeded`
//! (always with `send_state = confirmed`) and `failed`. A failed write whose
//! remote disposition is unknown keeps `send_state = ambiguous` — the closed
//! "settled ambiguous" form the technical spec permits — and is never
//! re-queued by these primitives. Reads (`pull_request_read`) produce no
//! remote effect, so their `send_state` stays `not_started` until success.
//!
//! Claim fencing: [`OperationClaim`] pairs the row id with the post-claim
//! `attempts` value. Every follow-up primitive is guarded on
//! `active = 1 AND state = 'running' AND attempts = claim.attempt`, so a stale
//! claimant (e.g. one superseded by restart reconciliation and a re-claim)
//! affects zero rows and observes a clean negative.
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

use crate::claims::is_unique_violation_pub;
use crate::collaboration_admission::{
    GithubOperationKind, GithubOperationResponseJson, OperationErrorSummary, RemoteObjectId,
    MAX_OPERATION_ATTEMPTS,
};
use crate::db::{begin_immediate, map_sql_err};
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

/// Hard cap on claims per operation. A claim increments `attempts`, so an
/// operation is executed at most this many times before it settles `failed`.
pub const MAX_OPERATION_CLAIMS: i64 = MAX_OPERATION_ATTEMPTS as i64;

/// Byte cap on the closed `error_kind` token recorded at settle/retry.
pub const MAX_OPERATION_ERROR_KIND_BYTES: usize = 64;

/// The only kind with no remote effect. Every other kind (including any kind
/// this binary does not recognise) is treated as a write: fail-safe toward
/// "never blindly re-send".
const READ_KIND: &str = GithubOperationKind::PullRequestRead.as_str();

/// `send_state` expression for an ambiguous settle: writes become ambiguous,
/// reads keep their (effect-free) send state. Must stay in lock-step with
/// [`READ_KIND`].
const AMBIGUOUS_SEND_SQL: &str =
    "CASE WHEN kind='pull_request_read' THEN send_state ELSE 'ambiguous' END";

const ERR_DEADLINE: &str = "deadline_exceeded";
const ERR_ATTEMPT_REVOKED: &str = "attempt_revoked";
const ERR_ATTEMPTS: &str = "attempts_exhausted";
const ERR_INTERRUPTED: &str = "interrupted";
const ERR_INTERRUPTED_AMBIGUOUS: &str = "interrupted_ambiguous";

/// Fencing token for one claim. `attempt` is the row's `attempts` value
/// immediately after this claim incremented it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct OperationClaim {
    pub id: i64,
    pub attempt: i64,
}

/// A won claim: the fencing token plus the row as it stood after the claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClaimedOperation {
    pub claim: OperationClaim,
    pub operation: GithubAgentOperation,
}

/// Terminal result recorded by [`settle`].
#[derive(Debug, Clone, Copy)]
pub enum Settlement<'a> {
    /// The requested effect is confirmed (or the read completed):
    /// `succeeded` with `send_state = confirmed`.
    Succeeded {
        response_json: Option<&'a GithubOperationResponseJson>,
        remote_object_id: Option<&'a RemoteObjectId>,
    },
    /// Terminal failure with a closed proof that no request was handed off.
    /// A write whose send had started moves `ambiguous -> definitely_unsent`.
    FailedUnsent {
        error_kind: &'a str,
        error_summary: &'a OperationErrorSummary,
    },
    /// Terminal failure whose remote disposition is unknown (timeout, lost
    /// response, unrecognised result). Writes keep/enter `send_state =
    /// ambiguous`; reads have no remote effect and keep their send state.
    Ambiguous {
        error_kind: &'a str,
        error_summary: &'a OperationErrorSummary,
    },
}

/// Outcome of [`release_for_retry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryOutcome {
    /// Claim released; the row is `queued` and due at `next_attempt_at`.
    Requeued { next_attempt_at: i64 },
    /// Attempt cap or deadline reached; the row settled `failed` instead.
    Exhausted,
    /// The claim is no longer current (settled, reconciled, or stale token).
    Lost,
}

/// Counts from [`reconcile_on_start`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ReconcileReport {
    /// Reads and never-sent writes returned to `queued`.
    pub requeued: usize,
    /// Writes whose send may have reached GitHub, settled `failed/ambiguous`.
    pub ambiguous: usize,
    /// Never-sent rows that were out of attempts or past deadline.
    pub failed: usize,
}

fn validate_error_kind(kind: &str) -> Result<()> {
    if kind.is_empty()
        || kind.len() > MAX_OPERATION_ERROR_KIND_BYTES
        || !kind
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(QuorumError::Usage(
            "invalid bounded GitHub operation error kind".into(),
        ));
    }
    Ok(())
}

/// Claim the oldest due queued operation.
///
/// One `BEGIN IMMEDIATE`: first terminalize queued rows that can never run
/// (deadline passed or claim cap reached) as `failed`, then pick the oldest
/// due candidate whose parent collaboration attempt is `active`, and flip it
/// `queued -> running`, `active 0 -> 1`, stamping `claimed_at` and
/// incrementing `attempts` via a guarded `UPDATE ... RETURNING`.
///
/// `Ok(None)` is the clean negative: nothing due, a lost race (zero guarded
/// rows), or `SQLITE_CONSTRAINT_UNIQUE` on `github_agent_operations_one_active`.
/// A post-timeout `SQLITE_BUSY` surfaces as [`QuorumError::Busy`].
///
/// A queued row with `send_state = ambiguous` may still be claimed; it is
/// reconciliation-only and [`mark_execution_started`] refuses to start a send
/// for it.
pub fn claim_next(conn: &mut Connection, now: i64) -> Result<Option<ClaimedOperation>> {
    let tx = begin_immediate(conn)?;
    tx.execute(
        "UPDATE github_agent_operations
         SET state='failed', next_attempt_at=NULL, error_kind=?2,
             error_summary='operation deadline passed before claim', updated_at=?1
         WHERE state='queued' AND active=0 AND deadline_at<=?1",
        params![now, ERR_DEADLINE],
    )
    .map_err(map_sql_err)?;
    tx.execute(
        "UPDATE github_agent_operations
         SET state='failed', next_attempt_at=NULL, error_kind=?2,
             error_summary='operation claim limit reached before claim', updated_at=?1
         WHERE state='queued' AND active=0 AND attempts>=?3",
        params![now, ERR_ATTEMPTS, MAX_OPERATION_CLAIMS],
    )
    .map_err(map_sql_err)?;
    // A revoked run's attempt is terminal: terminalize it and cancel its
    // provably unsent rows before picking, so its queued work is never sent.
    crate::collaboration_admission::revoke_dead_run_attempts(
        &tx,
        now,
        crate::collaboration_admission::UNBOUNDED_GROUP_SWEEP,
    )?;
    let candidate: Option<i64> = tx
        .query_row(
            "SELECT o.id FROM github_agent_operations o
             JOIN github_collaboration_attempts a ON a.attempt_id = o.attempt_id
             WHERE o.state='queued' AND o.active=0
               AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=?1)
               AND o.deadline_at>?1 AND o.attempts<?2
               AND a.state='active'
               AND EXISTS (
                   SELECT 1 FROM run_capabilities c
                   WHERE c.run_id=a.active_run_id AND c.revoked_at IS NULL
               )
             ORDER BY COALESCE(o.next_attempt_at, o.created_at), o.id
             LIMIT 1",
            params![now, MAX_OPERATION_CLAIMS],
            |r| r.get(0),
        )
        .optional()
        .map_err(map_sql_err)?;
    let Some(id) = candidate else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(None);
    };
    let sql = format!(
        "UPDATE github_agent_operations
         SET state='running', active=1, claimed_at=?2, execution_started_at=NULL,
             attempts=attempts+1, next_attempt_at=NULL, updated_at=?2
         WHERE id=?1 AND state='queued' AND active=0
         RETURNING {SELECT_COLUMNS}"
    );
    let claimed = match tx
        .query_row(&sql, params![id, now], row_from_sql)
        .optional()
    {
        Ok(row) => row,
        Err(e) if is_unique_violation_pub(&e) => None,
        Err(e) => return Err(map_sql_err(e)),
    };
    let Some(operation) = claimed else {
        // Lost race: drop the transaction (rollback) — the terminalization
        // sweeps above are idempotent and will re-run on the next claim.
        return Ok(None);
    };
    tx.commit().map_err(map_sql_err)?;
    Ok(Some(ClaimedOperation {
        claim: OperationClaim {
            id: operation.id,
            attempt: operation.attempts,
        },
        operation,
    }))
}

/// Record that execution is about to begin, *before* any network send.
///
/// Stamps `execution_started_at` and, for writes, moves
/// `not_started|definitely_unsent -> ambiguous`, so a crash after this point
/// is reconciled as "may have reached GitHub". Guarded on the current claim
/// and on a not-yet-started, sendable row: `Ok(false)` is the clean negative
/// (stale/lost claim, already started, or a reconciliation-only ambiguous
/// row that must not be re-sent).
pub fn mark_execution_started(
    conn: &mut Connection,
    claim: &OperationClaim,
    now: i64,
) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    // The run may have been revoked between claim and send. A row that has
    // provably not been sent is cancelled instead of started; the caller's
    // later settle of this claim is then a guarded no-op.
    tx.execute(
        "UPDATE github_agent_operations
         SET state='cancelled', active=0, next_attempt_at=NULL, error_kind=?3,
             error_summary='collaboration attempt revoked before the send started',
             updated_at=?4
         WHERE id=?1 AND attempts=?2 AND active=1 AND state='running'
           AND execution_started_at IS NULL
           AND send_state IN ('not_started','definitely_unsent')
           AND NOT EXISTS (
               SELECT 1 FROM github_collaboration_attempts a
               JOIN run_capabilities c ON c.run_id=a.active_run_id
               WHERE a.attempt_id=github_agent_operations.attempt_id
                 AND a.state='active' AND c.revoked_at IS NULL
           )",
        params![claim.id, claim.attempt, ERR_ATTEMPT_REVOKED, now],
    )
    .map_err(map_sql_err)?;
    let changed = tx
        .execute(
            "UPDATE github_agent_operations
             SET execution_started_at=?3,
                 send_state=CASE WHEN kind=?4 THEN send_state ELSE 'ambiguous' END,
                 updated_at=?3
             WHERE id=?1 AND attempts=?2 AND active=1 AND state='running'
               AND execution_started_at IS NULL
               AND send_state IN ('not_started','definitely_unsent')",
            params![claim.id, claim.attempt, now, READ_KIND],
        )
        .map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(changed > 0)
}

/// Record the terminal result of the current claim exactly once and clear
/// `active`. `Ok(false)` is the clean negative: the claim is stale, the row
/// was already settled, or reconciliation already resolved it. Text inputs
/// arrive as bounded, NUL-free newtypes; `error_kind` is a closed
/// `[a-z0-9_]` token of at most [`MAX_OPERATION_ERROR_KIND_BYTES`].
pub fn settle(
    conn: &mut Connection,
    claim: &OperationClaim,
    settlement: Settlement<'_>,
    now: i64,
) -> Result<bool> {
    let (state, send_sql, response, remote, error_kind, error_summary) = match settlement {
        Settlement::Succeeded {
            response_json,
            remote_object_id,
        } => (
            "succeeded",
            "'confirmed'",
            response_json.map(GithubOperationResponseJson::as_str),
            remote_object_id.map(|r| r.as_str()),
            None,
            None,
        ),
        Settlement::FailedUnsent {
            error_kind,
            error_summary,
        } => {
            validate_error_kind(error_kind)?;
            (
                "failed",
                "CASE WHEN send_state='ambiguous' THEN 'definitely_unsent' ELSE send_state END",
                None,
                None,
                Some(error_kind),
                Some(error_summary.as_str()),
            )
        }
        Settlement::Ambiguous {
            error_kind,
            error_summary,
        } => {
            validate_error_kind(error_kind)?;
            (
                "failed",
                AMBIGUOUS_SEND_SQL,
                None,
                None,
                Some(error_kind),
                Some(error_summary.as_str()),
            )
        }
    };
    let sql = format!(
        "UPDATE github_agent_operations
         SET state=?3, send_state={send_sql}, active=0, next_attempt_at=NULL,
             response_json=?4, remote_object_id=?5, error_kind=?6, error_summary=?7,
             updated_at=?8
         WHERE id=?1 AND attempts=?2 AND active=1 AND state='running'"
    );
    let tx = begin_immediate(conn)?;
    let changed = tx
        .execute(
            &sql,
            params![
                claim.id,
                claim.attempt,
                state,
                response,
                remote,
                error_kind,
                error_summary,
                now
            ],
        )
        .map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(changed > 0)
}

/// Release the current claim back to `queued`, due at `retry_at`.
///
/// A write whose send has started (`send_state = ambiguous`) may be retried
/// only with `proven_unsent = true` — a closed transport proof that no
/// request was handed off — which records `definitely_unsent`. Without that
/// proof the call is refused with a usage error and the claim is left
/// untouched: the caller must [`settle`] it [`Settlement::Ambiguous`].
///
/// When the claim cap is already reached or `retry_at` is at/after the
/// deadline, the row settles `failed` ([`RetryOutcome::Exhausted`]).
pub fn release_for_retry(
    conn: &mut Connection,
    claim: &OperationClaim,
    retry_at: i64,
    error_kind: &str,
    error_summary: &OperationErrorSummary,
    proven_unsent: bool,
    now: i64,
) -> Result<RetryOutcome> {
    validate_error_kind(error_kind)?;
    if retry_at < now {
        return Err(QuorumError::Usage(
            "GitHub operation retry must not be scheduled in the past".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let current: Option<(String, String, i64)> = tx
        .query_row(
            "SELECT kind, send_state, deadline_at FROM github_agent_operations
             WHERE id=?1 AND attempts=?2 AND active=1 AND state='running'",
            params![claim.id, claim.attempt],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(map_sql_err)?;
    let Some((kind, send_state, deadline_at)) = current else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(RetryOutcome::Lost);
    };
    let is_read = kind == READ_KIND;
    let send_ambiguous =
        !is_read && send_state != "not_started" && send_state != "definitely_unsent";
    if send_ambiguous && !proven_unsent {
        return Err(QuorumError::Usage(
            "GitHub write whose send may have reached GitHub cannot be retried without unsent proof"
                .into(),
        ));
    }
    let next_send_state = if send_ambiguous {
        "definitely_unsent"
    } else {
        send_state.as_str()
    };
    let exhausted = claim.attempt >= MAX_OPERATION_CLAIMS || retry_at >= deadline_at;
    let changed = if exhausted {
        tx.execute(
            "UPDATE github_agent_operations
             SET state='failed', send_state=?3, active=0, next_attempt_at=NULL,
                 error_kind=?4, error_summary=?5, updated_at=?6
             WHERE id=?1 AND attempts=?2 AND active=1 AND state='running'",
            params![
                claim.id,
                claim.attempt,
                next_send_state,
                error_kind,
                error_summary.as_str(),
                now
            ],
        )
    } else {
        tx.execute(
            "UPDATE github_agent_operations
             SET state='queued', send_state=?3, active=0, next_attempt_at=?7,
                 error_kind=?4, error_summary=?5, updated_at=?6
             WHERE id=?1 AND attempts=?2 AND active=1 AND state='running'",
            params![
                claim.id,
                claim.attempt,
                next_send_state,
                error_kind,
                error_summary.as_str(),
                now,
                retry_at
            ],
        )
    }
    .map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(match (changed > 0, exhausted) {
        (false, _) => RetryOutcome::Lost,
        (true, true) => RetryOutcome::Exhausted,
        (true, false) => RetryOutcome::Requeued {
            next_attempt_at: retry_at,
        },
    })
}

/// Resolve every claim left `active = 1` by a previous daemon. Call once at
/// startup while holding `daemon_lock`, before the first [`claim_next`].
///
/// One `BEGIN IMMEDIATE`:
/// - writes whose send may have reached GitHub (`execution_started_at` set or
///   `send_state` past `not_started|definitely_unsent`) settle
///   `failed` with `send_state = ambiguous` and are never re-queued;
/// - reads and never-sent writes return to `queued`, due now, unless their
///   claim cap or deadline is spent, in which case they settle `failed`.
pub fn reconcile_on_start(conn: &mut Connection, now: i64) -> Result<ReconcileReport> {
    let tx = begin_immediate(conn)?;
    let ambiguous = tx
        .execute(
            "UPDATE github_agent_operations
             SET state='failed', send_state='ambiguous', active=0, next_attempt_at=NULL,
                 error_kind=?2,
                 error_summary='daemon restarted after the send may have reached GitHub',
                 updated_at=?1
             WHERE active=1 AND kind<>?3
               AND (execution_started_at IS NOT NULL
                    OR send_state NOT IN ('not_started','definitely_unsent'))",
            params![now, ERR_INTERRUPTED_AMBIGUOUS, READ_KIND],
        )
        .map_err(map_sql_err)?;
    let failed = tx
        .execute(
            "UPDATE github_agent_operations
             SET state='failed', active=0, next_attempt_at=NULL, error_kind=?2,
                 error_summary='daemon restarted with no remaining attempts or deadline',
                 updated_at=?1
             WHERE active=1 AND (attempts>=?3 OR deadline_at<=?1)",
            params![now, ERR_INTERRUPTED, MAX_OPERATION_CLAIMS],
        )
        .map_err(map_sql_err)?;
    let requeued = tx
        .execute(
            "UPDATE github_agent_operations
             SET state='queued', active=0, next_attempt_at=?1, error_kind=?2,
                 error_summary='daemon restarted before the send started',
                 updated_at=?1
             WHERE active=1",
            params![now, ERR_INTERRUPTED],
        )
        .map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(ReconcileReport {
        requeued,
        ambiguous,
        failed,
    })
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

    // ---- task #435: claim / start / settle / retry / reconcile ----

    fn enqueue_op(conn: &mut Connection, operation_id: &str, kind: &str) {
        let mut req = sample_request();
        req.operation_id = operation_id;
        req.client_request_id = operation_id;
        req.kind = kind;
        req.group_key = None;
        assert!(enqueue(conn, &req).unwrap());
    }

    const WRITE: &str = "add_issue_comment";

    fn summary(text: &str) -> OperationErrorSummary {
        OperationErrorSummary::new(text).unwrap()
    }

    fn claim(conn: &mut Connection, now: i64) -> ClaimedOperation {
        claim_next(conn, now).unwrap().expect("a due operation")
    }

    fn get(conn: &Connection, op: &str) -> GithubAgentOperation {
        get_by_operation_id(conn, op).unwrap().unwrap()
    }

    fn errors_rows(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM errors", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn ambiguous_send_sql_tracks_read_kind() {
        assert!(AMBIGUOUS_SEND_SQL.contains(&format!("'{READ_KIND}'")));
    }

    #[test]
    fn claim_sets_running_active_and_second_claim_is_clean_negative() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        let won = claim(&mut conn, 110);
        assert_eq!(won.claim.attempt, 1);
        assert_eq!(won.operation.state, "running");
        assert!(won.operation.active);
        assert_eq!(won.operation.claimed_at, Some(110));
        assert_eq!(won.operation.attempts, 1);
        assert_eq!(won.operation.send_state, "not_started");
        assert_eq!(get(&conn, "op-w"), won.operation);
        // Already claimed: nothing else is due.
        assert!(claim_next(&mut conn, 111).unwrap().is_none());
        assert_eq!(errors_rows(&conn), 0);
    }

    #[test]
    fn claim_skips_not_due_and_inactive_attempts_and_fails_overdue() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-later", WRITE);
        conn.execute(
            "UPDATE github_agent_operations SET next_attempt_at=200 WHERE operation_id='op-later'",
            [],
        )
        .unwrap();
        assert!(claim_next(&mut conn, 150).unwrap().is_none());
        assert_eq!(claim(&mut conn, 200).operation.operation_id, "op-later");

        // Parent attempt not active: never claimable.
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        conn.execute(
            "UPDATE github_collaboration_attempts SET state='awaiting_resume', active_run_id=NULL",
            [],
        )
        .unwrap();
        assert!(claim_next(&mut conn, 150).unwrap().is_none());
        assert_eq!(get(&conn, "op-w").state, "queued");

        // Deadline passed (deadline_at=500): settles failed, never claimed.
        assert!(claim_next(&mut conn, 500).unwrap().is_none());
        let row = get(&conn, "op-w");
        assert_eq!(row.state, "failed");
        assert_eq!(row.error_kind.as_deref(), Some(ERR_DEADLINE));
        assert_eq!(row.attempts, 0);
        assert!(!row.active);
    }

    #[test]
    fn revoked_run_queued_write_is_cancelled_never_claimed_or_started() {
        // Revoked before claim: the attempt terminalizes and the queued
        // comment is cancelled, never handed to the executor.
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        crate::capabilities::revoke(&mut conn, "run-1", 105).unwrap();
        assert!(claim_next(&mut conn, 110).unwrap().is_none());
        let row = get(&conn, "op-w");
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str(), row.attempts),
            ("cancelled", "not_started", 0)
        );
        assert_eq!(row.error_kind.as_deref(), Some(ERR_ATTEMPT_REVOKED));
        let attempt: (String, Option<String>, i64) = conn
            .query_row(
                "SELECT state,active_run_id,expires_at FROM github_collaboration_attempts
                 WHERE attempt_id='att-1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(attempt, ("revoked".to_string(), None, 110));

        // Revoked between claim and send start: the claimed row is cancelled
        // instead of started, and the executor's later settle is a no-op.
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        let won = claim(&mut conn, 110);
        crate::capabilities::revoke(&mut conn, "run-1", 111).unwrap();
        assert!(!mark_execution_started(&mut conn, &won.claim, 112).unwrap());
        let row = get(&conn, "op-w");
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str(), row.active),
            ("cancelled", "not_started", false)
        );
        assert_eq!(row.execution_started_at, None);
        let s = summary("late");
        assert!(!settle(
            &mut conn,
            &won.claim,
            Settlement::Ambiguous {
                error_kind: "not_resent",
                error_summary: &s
            },
            113
        )
        .unwrap());
        assert_eq!(get(&conn, "op-w"), row);
        assert_eq!(errors_rows(&conn), 0);
    }

    #[test]
    fn settle_records_result_exactly_once() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        let won = claim(&mut conn, 110);
        assert!(mark_execution_started(&mut conn, &won.claim, 111).unwrap());
        let response = GithubOperationResponseJson::new(r#"{"id":9}"#).unwrap();
        let remote = RemoteObjectId::new("IC_9").unwrap();
        let ok = Settlement::Succeeded {
            response_json: Some(&response),
            remote_object_id: Some(&remote),
        };
        assert!(settle(&mut conn, &won.claim, ok, 112).unwrap());
        let row = get(&conn, "op-w");
        assert_eq!(row.state, "succeeded");
        assert_eq!(row.send_state, "confirmed");
        assert_eq!(row.response_json.as_deref(), Some(r#"{"id":9}"#));
        assert_eq!(row.remote_object_id.as_deref(), Some("IC_9"));
        assert!(!row.active);

        // Second settle (any variant) on the same claim: zero rows, no change.
        let s = summary("late");
        assert!(!settle(&mut conn, &won.claim, ok, 113).unwrap());
        assert!(!settle(
            &mut conn,
            &won.claim,
            Settlement::Ambiguous {
                error_kind: "timeout",
                error_summary: &s
            },
            113
        )
        .unwrap());
        assert_eq!(get(&conn, "op-w"), row);
        assert!(!mark_execution_started(&mut conn, &won.claim, 113).unwrap());
        assert_eq!(
            release_for_retry(&mut conn, &won.claim, 120, "timeout", &s, false, 113).unwrap(),
            RetryOutcome::Lost
        );
        assert_eq!(errors_rows(&conn), 0);
    }

    #[test]
    fn settle_by_stale_or_never_claimed_token_is_clean_negative() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        let id = get(&conn, "op-w").id;
        let s = summary("x");
        let fail = Settlement::FailedUnsent {
            error_kind: "rejected",
            error_summary: &s,
        };
        // Inactive (never claimed) row.
        let phantom = OperationClaim { id, attempt: 0 };
        assert!(!settle(&mut conn, &phantom, fail, 110).unwrap());
        assert!(!mark_execution_started(&mut conn, &phantom, 110).unwrap());
        assert_eq!(get(&conn, "op-w").state, "queued");

        let won = claim(&mut conn, 110);
        let stale = OperationClaim {
            id,
            attempt: won.claim.attempt + 1,
        };
        assert!(!settle(&mut conn, &stale, fail, 111).unwrap());
        assert!(get(&conn, "op-w").active);
        assert!(settle(&mut conn, &won.claim, fail, 111).unwrap());
        let row = get(&conn, "op-w");
        assert_eq!(row.state, "failed");
        assert_eq!(row.send_state, "not_started");
        assert_eq!(row.error_kind.as_deref(), Some("rejected"));
    }

    #[test]
    fn execution_start_marks_writes_ambiguous_and_leaves_reads_unsent() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        let w = claim(&mut conn, 110);
        assert!(mark_execution_started(&mut conn, &w.claim, 111).unwrap());
        let row = get(&conn, "op-w");
        assert_eq!(row.send_state, "ambiguous");
        assert_eq!(row.execution_started_at, Some(111));
        // Started once only.
        assert!(!mark_execution_started(&mut conn, &w.claim, 112).unwrap());

        enqueue_op(&mut conn, "op-r", READ_KIND);
        let r = claim(&mut conn, 113);
        assert!(mark_execution_started(&mut conn, &r.claim, 114).unwrap());
        assert_eq!(get(&conn, "op-r").send_state, "not_started");
        assert!(!mark_execution_started(&mut conn, &r.claim, 115).unwrap());

        // A failed-unsent settle with proof records definitely_unsent.
        let s = summary("connect refused");
        assert!(settle(
            &mut conn,
            &w.claim,
            Settlement::FailedUnsent {
                error_kind: "connect_refused",
                error_summary: &s
            },
            116
        )
        .unwrap());
        assert_eq!(get(&conn, "op-w").send_state, "definitely_unsent");
        // An ambiguous settle on a read keeps its effect-free send state.
        assert!(settle(
            &mut conn,
            &r.claim,
            Settlement::Ambiguous {
                error_kind: "timeout",
                error_summary: &s
            },
            116
        )
        .unwrap());
        let read = get(&conn, "op-r");
        assert_eq!(
            (read.state.as_str(), read.send_state.as_str()),
            ("failed", "not_started")
        );
    }

    #[test]
    fn reconcile_settles_started_writes_ambiguous_and_requeues_reads_and_unsent_writes() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-sent", WRITE);
        enqueue_op(&mut conn, "op-unsent", WRITE);
        enqueue_op(&mut conn, "op-read", READ_KIND);
        enqueue_op(&mut conn, "op-idle", WRITE);
        let sent = claim(&mut conn, 110);
        assert_eq!(sent.operation.operation_id, "op-sent");
        assert!(mark_execution_started(&mut conn, &sent.claim, 110).unwrap());
        let unsent = claim(&mut conn, 110);
        assert_eq!(unsent.operation.operation_id, "op-unsent");
        let read = claim(&mut conn, 110);
        assert_eq!(read.operation.operation_id, "op-read");
        assert!(mark_execution_started(&mut conn, &read.claim, 110).unwrap());

        let report = reconcile_on_start(&mut conn, 120).unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                requeued: 2,
                ambiguous: 1,
                failed: 0
            }
        );
        let row = get(&conn, "op-sent");
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str(), row.active),
            ("failed", "ambiguous", false)
        );
        assert_eq!(row.error_kind.as_deref(), Some(ERR_INTERRUPTED_AMBIGUOUS));
        for op in ["op-unsent", "op-read"] {
            let row = get(&conn, op);
            assert_eq!((row.state.as_str(), row.active), ("queued", false), "{op}");
            assert_eq!(row.send_state, "not_started", "{op}");
            assert_eq!(row.next_attempt_at, Some(120), "{op}");
        }
        assert_eq!(get(&conn, "op-idle").state, "queued");
        // Idempotent: nothing left active.
        assert_eq!(
            reconcile_on_start(&mut conn, 121).unwrap(),
            ReconcileReport::default()
        );

        // The ambiguous write is never re-claimed; the others are.
        let mut reclaimed = Vec::new();
        while let Some(won) = claim_next(&mut conn, 130).unwrap() {
            reclaimed.push(won.operation.operation_id);
        }
        reclaimed.sort();
        assert_eq!(reclaimed, ["op-idle", "op-read", "op-unsent"]);
        // The pre-restart claimant of the read is stale now.
        let s = summary("late");
        assert!(!settle(
            &mut conn,
            &read.claim,
            Settlement::Ambiguous {
                error_kind: "timeout",
                error_summary: &s
            },
            131
        )
        .unwrap());
        assert_eq!(get(&conn, "op-read").attempts, 2);
        assert_eq!(errors_rows(&conn), 0);
    }

    #[test]
    fn reconcile_fails_unsent_rows_with_no_attempts_or_deadline_left() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-r", READ_KIND);
        claim(&mut conn, 110);
        // deadline_at=500 has passed by the restart.
        let report = reconcile_on_start(&mut conn, 600).unwrap();
        assert_eq!(report.failed, 1);
        let row = get(&conn, "op-r");
        assert_eq!((row.state.as_str(), row.active), ("failed", false));
        assert_eq!(row.error_kind.as_deref(), Some(ERR_INTERRUPTED));
    }

    #[test]
    fn retry_requeues_until_attempt_cap_then_settles_failed() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-r", READ_KIND);
        let s = summary("gh api 502");
        let mut now = 110;
        for attempt in 1..MAX_OPERATION_CLAIMS {
            let won = claim(&mut conn, now);
            assert_eq!(won.claim.attempt, attempt);
            assert!(mark_execution_started(&mut conn, &won.claim, now).unwrap());
            let outcome =
                release_for_retry(&mut conn, &won.claim, now + 5, "transient", &s, false, now)
                    .unwrap();
            assert_eq!(
                outcome,
                RetryOutcome::Requeued {
                    next_attempt_at: now + 5
                }
            );
            let row = get(&conn, "op-r");
            assert_eq!((row.state.as_str(), row.active), ("queued", false));
            // Not due until the scheduled time.
            assert!(claim_next(&mut conn, now + 4).unwrap().is_none());
            now += 5;
        }
        let last = claim(&mut conn, now);
        assert_eq!(last.claim.attempt, MAX_OPERATION_CLAIMS);
        assert_eq!(
            release_for_retry(&mut conn, &last.claim, now + 5, "transient", &s, false, now)
                .unwrap(),
            RetryOutcome::Exhausted
        );
        let row = get(&conn, "op-r");
        assert_eq!((row.state.as_str(), row.active), ("failed", false));
        assert_eq!(row.attempts, MAX_OPERATION_CLAIMS);
        assert!(claim_next(&mut conn, now + 10).unwrap().is_none());
    }

    #[test]
    fn retry_past_deadline_settles_failed() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-r", READ_KIND);
        let won = claim(&mut conn, 110);
        let s = summary("slow");
        assert_eq!(
            release_for_retry(&mut conn, &won.claim, 500, "transient", &s, false, 110).unwrap(),
            RetryOutcome::Exhausted
        );
        assert_eq!(get(&conn, "op-r").state, "failed");
        // Scheduling in the past is a usage error.
        enqueue_op(&mut conn, "op-r2", READ_KIND);
        let won = claim(&mut conn, 120);
        let err =
            release_for_retry(&mut conn, &won.claim, 119, "transient", &s, false, 120).unwrap_err();
        assert!(matches!(err, QuorumError::Usage(_)), "{err:?}");
    }

    #[test]
    fn retry_of_started_write_requires_unsent_proof() {
        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        let won = claim(&mut conn, 110);
        assert!(mark_execution_started(&mut conn, &won.claim, 111).unwrap());
        let s = summary("timeout");
        let err =
            release_for_retry(&mut conn, &won.claim, 120, "timeout", &s, false, 112).unwrap_err();
        assert!(matches!(err, QuorumError::Usage(_)), "{err:?}");
        let row = get(&conn, "op-w");
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str(), row.active),
            ("running", "ambiguous", true)
        );
        assert_eq!(
            release_for_retry(&mut conn, &won.claim, 120, "connect_refused", &s, true, 112)
                .unwrap(),
            RetryOutcome::Requeued {
                next_attempt_at: 120
            }
        );
        let row = get(&conn, "op-w");
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str()),
            ("queued", "definitely_unsent")
        );
        // The next send must re-enter ambiguous before I/O.
        let again = claim(&mut conn, 120);
        assert!(mark_execution_started(&mut conn, &again.claim, 121).unwrap());
        assert_eq!(get(&conn, "op-w").send_state, "ambiguous");
    }

    #[test]
    fn settle_text_is_bounded_and_nul_free() {
        use crate::collaboration_admission::{MAX_OPERATION_ERROR_BYTES, MAX_OPERATION_JSON_BYTES};
        assert!(OperationErrorSummary::new("x".repeat(MAX_OPERATION_ERROR_BYTES)).is_ok());
        assert!(matches!(
            OperationErrorSummary::new("x".repeat(MAX_OPERATION_ERROR_BYTES + 1)),
            Err(QuorumError::Usage(_))
        ));
        assert!(matches!(
            OperationErrorSummary::new("bad\0summary"),
            Err(QuorumError::Usage(_))
        ));
        let big = format!(r#"{{"b":"{}"}}"#, "x".repeat(MAX_OPERATION_JSON_BYTES));
        assert!(matches!(
            GithubOperationResponseJson::new(&big),
            Err(QuorumError::Usage(_))
        ));
        assert!(matches!(
            GithubOperationResponseJson::new("{\"a\":\"\\u0000\"}"),
            Err(QuorumError::Usage(_))
        ));
        assert!(matches!(
            RemoteObjectId::new("x".repeat(129)),
            Err(QuorumError::Usage(_))
        ));

        let (mut conn, _dir) = test_conn();
        enqueue_op(&mut conn, "op-w", WRITE);
        let won = claim(&mut conn, 110);
        let s = summary("ok");
        let oversize_kind = "k".repeat(MAX_OPERATION_ERROR_KIND_BYTES + 1);
        for kind in ["", "bad\0kind", "Upper", oversize_kind.as_str()] {
            let err = settle(
                &mut conn,
                &won.claim,
                Settlement::Ambiguous {
                    error_kind: kind,
                    error_summary: &s,
                },
                111,
            )
            .unwrap_err();
            assert!(matches!(err, QuorumError::Usage(_)), "{kind:?}: {err:?}");
            let err =
                release_for_retry(&mut conn, &won.claim, 120, kind, &s, false, 111).unwrap_err();
            assert!(matches!(err, QuorumError::Usage(_)), "{kind:?}: {err:?}");
        }
        // Rejected input left the claim untouched.
        let row = get(&conn, "op-w");
        assert!(row.active);
        assert_eq!(row.state, "running");
        assert!(row.error_kind.is_none());
    }
}
