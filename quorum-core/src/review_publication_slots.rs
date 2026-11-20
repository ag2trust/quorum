//! Daemon-owned pending-review publication slots (task #447).
//!
//! One `github_review_publication_slots` row serializes every pending-review
//! draft a publisher may hold on one PR: `PRIMARY KEY(publisher_scope,
//! pr_number)` admits one slot per scope/PR and `UNIQUE(attempt_id)` lets an
//! attempt own at most one. This module owns the slot state machine; it makes
//! no Repository Service call and holds no transaction across GitHub I/O.
//! Every primitive is one short `BEGIN IMMEDIATE` (or an `_in_tx` helper the
//! caller composes into its own), and the daemon performs remote reads and
//! deletes only *between* primitives.
//!
//! ```text
//!   begin_probe ─► probing ──empty listing──────────► owned ──submit_pending ok──► (deleted)
//!                     │  └─exact terminal-attempt draft─► cleanup_required ◄─┘ terminal/revoked/
//!                     │                                    │  ▲              failed predecessor
//!                     └─foreign/multiple/active/exhausted─► blocked          │
//!                                                          ▼  │ reschedule
//!                                                   cleanup_running ──proof──► (deleted)
//! ```
//!
//! Fencing: a probe or cleanup claim increments `cleanup_attempts`; every
//! follow-up is guarded on that exact cycle and on the claimed state, so a
//! stale claimant (for example one superseded by [`reconcile_in_tx`]) matches
//! zero rows and observes [`ProbeOutcome::Lost`] / [`CleanupOutcome::Lost`].
//!
//! Recovery generation: probing and cleanup share [`MAX_SLOT_CYCLES`] claims
//! and [`SLOT_RECOVERY_WINDOW_SECS`] per generation. A generation starts when a
//! probe is inserted or when an owned slot moves to `cleanup_required`.
//! Exhaustion sets `blocked` and touches nothing else: no verdict, lifecycle
//! transition, rework, `errors` row, or budget charge.
//!
//! Create coupling: the pending-review create (review sequence 0) and the
//! slot's `create_send_state` form one state machine. `github_operations`
//! claims a create only while its slot is `owned` by the same attempt and moves
//! both send states to `ambiguous` in that claim transaction; every later
//! transition of the operation is mirrored by [`couple_create_in_tx`], and any
//! disagreement blocks the slot as a storage fault ([`sync_in_tx`]).
//!
//! Owner marker: [`ReviewOwnerMarker`] carries 244 bits of OS randomness. It is
//! stored on the attempt and slot, handed only to daemon cleanup claims, is
//! never serialized, and is redacted from `Debug`.

use crate::claims::is_unique_violation_pub;
use crate::collaboration_admission::{GithubSendState, MAX_GITHUB_MARKER_BYTES};
use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::fmt;

/// Shared probe + cleanup claim bound per recovery generation.
pub const MAX_SLOT_CYCLES: i64 = 8;
/// Wall-clock bound per recovery generation.
pub const SLOT_RECOVERY_WINDOW_SECS: i64 = 3_600;
/// First retry delay; doubles per consumed cycle up to [`SLOT_BACKOFF_MAX_SECS`].
pub const SLOT_BACKOFF_BASE_SECS: i64 = 15;
pub const SLOT_BACKOFF_MAX_SECS: i64 = 300;
/// Slot admission caps (technical spec: 16 per task, 1,024 per repository).
pub const MAX_SLOTS_PER_TASK: i64 = 16;
pub const MAX_SLOTS_PER_REPOSITORY: i64 = 1_024;
/// Upper bound on pending-review candidates one observation may carry.
pub const MAX_PENDING_REVIEW_CANDIDATES: usize = 16;
pub const MAX_PUBLISHER_SCOPE_BYTES: usize = 128;
const MAX_REVIEW_ID_BYTES: usize = 128;
const MAX_COMMIT_SHA_BYTES: usize = 128;

const ERR_CLEANUP_CANCELLED: &str = "review_cleanup";

/// Opaque daemon-derived Repository Service namespace. Never agent input.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct PublisherScope(String);

impl PublisherScope {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.contains('\0') || value.len() > MAX_PUBLISHER_SCOPE_BYTES {
            return Err(QuorumError::Usage("invalid bounded publisher scope".into()));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Unguessable per-attempt pending-review owner marker. Deliberately not
/// `Serialize` and redacted from `Debug`, so it cannot leak through any JSON
/// or log surface; only daemon cleanup and the create/submit executor read it.
#[derive(Clone, PartialEq, Eq)]
pub struct ReviewOwnerMarker(String);

impl ReviewOwnerMarker {
    /// Two UUIDv4 values: 2 × 122 = 244 bits from the OS CSPRNG.
    pub(crate) fn generate() -> Self {
        let a = uuid::Uuid::new_v4().simple();
        let b = uuid::Uuid::new_v4().simple();
        Self(format!("<!-- quorum-review-owner:{a}{b} -->"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ReviewOwnerMarker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReviewOwnerMarker(<redacted>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotState {
    Probing,
    Owned,
    CleanupRequired,
    CleanupRunning,
    Blocked,
}

impl SlotState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Probing => "probing",
            Self::Owned => "owned",
            Self::CleanupRequired => "cleanup_required",
            Self::CleanupRunning => "cleanup_running",
            Self::Blocked => "blocked",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "probing" => Self::Probing,
            "owned" => Self::Owned,
            "cleanup_required" => Self::CleanupRequired,
            "cleanup_running" => Self::CleanupRunning,
            "blocked" => Self::Blocked,
            _ => {
                return Err(QuorumError::Io(
                    "stored publication-slot state is invalid".into(),
                ))
            }
        })
    }
}

fn parse_send_state(value: &str) -> Result<GithubSendState> {
    value
        .parse()
        .map_err(|_| QuorumError::Io("stored publication-slot send state is invalid".into()))
}

/// Closed reasons a slot fails closed. Stored as `error_kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotBlockReason {
    /// A pending review with a missing or unknown owner marker.
    ForeignDraft,
    /// More than one pending-review candidate.
    MultipleDrafts,
    /// Our marker, but the PR, launch SHA, or known review ID differs.
    MismatchedDraft,
    /// The marker names an attempt that is still live.
    ActiveAttempt,
    /// The create operation and slot disagree on the create send state.
    SendStateDisagreement,
    /// The shared cycle or wall-clock bound of the recovery generation.
    RetryExhausted,
}

impl SlotBlockReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ForeignDraft => "foreign_draft",
            Self::MultipleDrafts => "multiple_drafts",
            Self::MismatchedDraft => "mismatched_draft",
            Self::ActiveAttempt => "active_attempt",
            Self::SendStateDisagreement => "send_state_disagreement",
            Self::RetryExhausted => "retry_exhausted",
        }
    }

    const fn summary(self) -> &'static str {
        match self {
            Self::ForeignDraft => "pending review has a missing or foreign owner marker",
            Self::MultipleDrafts => "publisher holds more than one pending review on the PR",
            Self::MismatchedDraft => {
                "owner marker matches but PR, launch SHA, or review ID differs"
            }
            Self::ActiveAttempt => "pending review belongs to a different live attempt",
            Self::SendStateDisagreement => "create operation and slot disagree on send state",
            Self::RetryExhausted => "pending-review recovery exhausted its cycle or time bound",
        }
    }
}

/// Read-only slot projection. Omits the owner marker by construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SlotStatus {
    pub publisher_scope: String,
    pub pr_number: i64,
    pub task_id: i64,
    pub attempt_id: Option<String>,
    pub state: SlotState,
    pub pending_review_id: Option<String>,
    pub create_send_state: GithubSendState,
    pub cleanup_attempts: i64,
    pub next_cleanup_at: Option<i64>,
    pub cleanup_deadline_at: Option<i64>,
    pub error_kind: Option<String>,
}

const STATUS_COLUMNS: &str = "publisher_scope, pr_number, task_id, attempt_id, state, \
     pending_review_id, create_send_state, cleanup_attempts, next_cleanup_at, \
     cleanup_deadline_at, error_kind";

fn status_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SlotStatusRaw> {
    Ok(SlotStatusRaw {
        publisher_scope: row.get(0)?,
        pr_number: row.get(1)?,
        task_id: row.get(2)?,
        attempt_id: row.get(3)?,
        state: row.get(4)?,
        pending_review_id: row.get(5)?,
        create_send_state: row.get(6)?,
        cleanup_attempts: row.get(7)?,
        next_cleanup_at: row.get(8)?,
        cleanup_deadline_at: row.get(9)?,
        error_kind: row.get(10)?,
    })
}

struct SlotStatusRaw {
    publisher_scope: String,
    pr_number: i64,
    task_id: i64,
    attempt_id: Option<String>,
    state: String,
    pending_review_id: Option<String>,
    create_send_state: String,
    cleanup_attempts: i64,
    next_cleanup_at: Option<i64>,
    cleanup_deadline_at: Option<i64>,
    error_kind: Option<String>,
}

impl SlotStatusRaw {
    fn into_status(self) -> Result<SlotStatus> {
        Ok(SlotStatus {
            state: SlotState::parse(&self.state)?,
            create_send_state: parse_send_state(&self.create_send_state)?,
            publisher_scope: self.publisher_scope,
            pr_number: self.pr_number,
            task_id: self.task_id,
            attempt_id: self.attempt_id,
            pending_review_id: self.pending_review_id,
            cleanup_attempts: self.cleanup_attempts,
            next_cleanup_at: self.next_cleanup_at,
            cleanup_deadline_at: self.cleanup_deadline_at,
            error_kind: self.error_kind,
        })
    }
}

fn load_status(conn: &Connection, scope: &str, pr_number: i64) -> Result<Option<SlotStatus>> {
    let sql = format!(
        "SELECT {STATUS_COLUMNS} FROM github_review_publication_slots
         WHERE publisher_scope=?1 AND pr_number=?2"
    );
    conn.query_row(&sql, params![scope, pr_number], status_from_row)
        .optional()
        .map_err(map_sql_err)?
        .map(SlotStatusRaw::into_status)
        .transpose()
}

/// Non-transactional read of one slot.
pub fn get(
    conn: &Connection,
    scope: &PublisherScope,
    pr_number: i64,
) -> Result<Option<SlotStatus>> {
    load_status(conn, scope.as_str(), pr_number)
}

/// Daemon-only read of the owner marker an attempt's create/submit body
/// carries. Never route the result to an agent-facing surface.
pub fn owner_marker_for_attempt(
    conn: &Connection,
    attempt_id: &str,
) -> Result<Option<ReviewOwnerMarker>> {
    let marker: Option<Option<String>> = conn
        .query_row(
            "SELECT review_owner_marker FROM github_collaboration_attempts WHERE attempt_id=?1",
            [attempt_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_sql_err)?;
    Ok(marker.flatten().map(ReviewOwnerMarker))
}

/// One pending review the Repository Service reported for `(scope, PR)`.
/// `marker` is the hidden owner marker extracted from its body, if any.
#[derive(Debug, Clone, Copy)]
pub struct PendingReviewCandidate<'a> {
    pub review_id: &'a str,
    pub marker: Option<&'a str>,
    pub commit_sha: &'a str,
}

fn validate_text(value: &str, max: usize, label: &str) -> Result<()> {
    if value.is_empty() || value.contains('\0') || value.len() > max {
        return Err(QuorumError::Usage(format!(
            "invalid bounded pending-review {label}"
        )));
    }
    Ok(())
}

fn validate_candidates(candidates: &[PendingReviewCandidate<'_>]) -> Result<()> {
    if candidates.len() > MAX_PENDING_REVIEW_CANDIDATES {
        return Err(QuorumError::Usage(
            "too many pending-review candidates in one observation".into(),
        ));
    }
    for candidate in candidates {
        validate_text(candidate.review_id, MAX_REVIEW_ID_BYTES, "review id")?;
        validate_text(candidate.commit_sha, MAX_COMMIT_SHA_BYTES, "commit sha")?;
        if let Some(marker) = candidate.marker {
            validate_text(marker, MAX_GITHUB_MARKER_BYTES, "marker")?;
        }
    }
    Ok(())
}

fn validate_now(now: i64) -> Result<()> {
    if now < 0 {
        return Err(QuorumError::Usage(
            "publication-slot timestamp is invalid".into(),
        ));
    }
    Ok(())
}

/// Delay before the next cycle after `cycle` claims were consumed.
pub fn backoff_secs(cycle: i64) -> i64 {
    let exponent = (cycle.max(1) - 1).min(16) as u32;
    SLOT_BACKOFF_BASE_SECS
        .saturating_mul(1_i64 << exponent)
        .min(SLOT_BACKOFF_MAX_SECS)
}

fn exhausted(cycles: i64, deadline: Option<i64>, now: i64) -> bool {
    cycles >= MAX_SLOT_CYCLES || deadline.is_some_and(|deadline| deadline <= now)
}

fn block_slot(
    tx: &Connection,
    scope: &str,
    pr_number: i64,
    reason: SlotBlockReason,
    now: i64,
) -> Result<()> {
    tx.execute(
        "UPDATE github_review_publication_slots
         SET state='blocked', next_cleanup_at=NULL, error_kind=?3, error_summary=?4,
             updated_at=?5
         WHERE publisher_scope=?1 AND pr_number=?2",
        params![scope, pr_number, reason.as_str(), reason.summary(), now],
    )
    .map_err(map_sql_err)?;
    Ok(())
}

/// Result of a backoff decision shared by probe and cleanup.
enum Backoff {
    Rescheduled(i64),
    Blocked,
}

/// Give back a claimed cycle after a negative or inconclusive observation.
/// The caller has already proven the guarded claim in this transaction.
#[allow(clippy::too_many_arguments)]
fn reschedule(
    tx: &Connection,
    scope: &str,
    pr_number: i64,
    cycle: i64,
    deadline: Option<i64>,
    to_state: SlotState,
    error_kind: &str,
    now: i64,
) -> Result<Backoff> {
    let next = now.saturating_add(backoff_secs(cycle));
    if cycle >= MAX_SLOT_CYCLES || deadline.is_some_and(|deadline| next >= deadline) {
        block_slot(tx, scope, pr_number, SlotBlockReason::RetryExhausted, now)?;
        return Ok(Backoff::Blocked);
    }
    tx.execute(
        "UPDATE github_review_publication_slots
         SET state=?3, next_cleanup_at=?4, error_kind=?5, error_summary=NULL, updated_at=?6
         WHERE publisher_scope=?1 AND pr_number=?2",
        params![scope, pr_number, to_state.as_str(), next, error_kind, now],
    )
    .map_err(map_sql_err)?;
    Ok(Backoff::Rescheduled(next))
}

// ---------------------------------------------------------------------------
// Probe
// ---------------------------------------------------------------------------

/// Fencing token for one probe cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProbeClaim {
    pub publisher_scope: PublisherScope,
    pub pr_number: i64,
    pub cycle: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProbeStart {
    /// The caller holds this probe cycle and may read remote state.
    Claimed(ProbeClaim),
    /// The slot exists in another state, is in flight, not yet due, or waits
    /// for a different task. Clean negative.
    Occupied(SlotStatus),
    /// Per-task or repository slot cap reached; no row created.
    CapacityExceeded,
}

/// Insert the unique `(scope, PR)` slot in `probing` (starting a recovery
/// generation), or claim the next cycle of an existing due probing slot for
/// the same task. Commits before any remote read. Exhausting the generation
/// on re-claim sets `blocked` and returns `Occupied`.
pub fn begin_probe(
    conn: &mut Connection,
    scope: &PublisherScope,
    pr_number: i64,
    task_id: i64,
    now: i64,
) -> Result<ProbeStart> {
    validate_now(now)?;
    if pr_number <= 0 || task_id <= 0 {
        return Err(QuorumError::Usage(
            "publication-slot PR and task must be positive".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let claim = |cycle| {
        ProbeStart::Claimed(ProbeClaim {
            publisher_scope: scope.clone(),
            pr_number,
            cycle,
        })
    };
    if let Some(existing) = load_status(&tx, scope.as_str(), pr_number)? {
        let due = existing.next_cleanup_at.is_some_and(|next| next <= now);
        if existing.state != SlotState::Probing || existing.task_id != task_id || !due {
            tx.commit().map_err(map_sql_err)?;
            return Ok(ProbeStart::Occupied(existing));
        }
        if exhausted(existing.cleanup_attempts, existing.cleanup_deadline_at, now) {
            block_slot(
                &tx,
                scope.as_str(),
                pr_number,
                SlotBlockReason::RetryExhausted,
                now,
            )?;
            let blocked = load_status(&tx, scope.as_str(), pr_number)?
                .ok_or_else(|| QuorumError::Io("publication slot vanished".into()))?;
            tx.commit().map_err(map_sql_err)?;
            return Ok(ProbeStart::Occupied(blocked));
        }
        let cycle: i64 = tx
            .query_row(
                "UPDATE github_review_publication_slots
                 SET cleanup_attempts=cleanup_attempts+1, next_cleanup_at=NULL, updated_at=?4
                 WHERE publisher_scope=?1 AND pr_number=?2 AND state='probing'
                   AND cleanup_attempts=?3
                 RETURNING cleanup_attempts",
                params![scope.as_str(), pr_number, existing.cleanup_attempts, now],
                |row| row.get(0),
            )
            .map_err(map_sql_err)?;
        tx.commit().map_err(map_sql_err)?;
        return Ok(claim(cycle));
    }
    let (per_task, total): (i64, i64) = tx
        .query_row(
            "SELECT COALESCE(SUM(task_id=?1),0), COUNT(*) FROM github_review_publication_slots",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(map_sql_err)?;
    if per_task >= MAX_SLOTS_PER_TASK || total >= MAX_SLOTS_PER_REPOSITORY {
        tx.commit().map_err(map_sql_err)?;
        return Ok(ProbeStart::CapacityExceeded);
    }
    match tx.execute(
        "INSERT INTO github_review_publication_slots(
             publisher_scope, pr_number, task_id, state, create_send_state,
             cleanup_attempts, next_cleanup_at, cleanup_deadline_at, created_at, updated_at
         ) VALUES (?1,?2,?3,'probing','not_started',1,NULL,?4,?5,?5)",
        params![
            scope.as_str(),
            pr_number,
            task_id,
            now.saturating_add(SLOT_RECOVERY_WINDOW_SECS),
            now
        ],
    ) {
        Ok(_) => {}
        Err(error) if is_unique_violation_pub(&error) => return Ok(ProbeStart::CapacityExceeded),
        Err(error) => return Err(map_sql_err(error)),
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(claim(1))
}

/// What the remote probe read observed.
#[derive(Debug, Clone, Copy)]
pub enum ProbeResult<'a> {
    /// The publisher has no pending review on the PR: the slot becomes owned
    /// by `new_attempt_id`, which must be a live reviewer attempt of the
    /// slot's task and PR that holds no marker or slot yet.
    Empty { new_attempt_id: &'a str },
    /// One or more pending reviews (non-empty).
    Pending {
        candidates: &'a [PendingReviewCandidate<'a>],
    },
    /// The read failed or was cut off; nothing is known.
    Inconclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProbeOutcome {
    Owned(SlotStatus),
    CleanupRequired(SlotStatus),
    Blocked(SlotBlockReason),
    Rescheduled {
        next_probe_at: i64,
    },
    /// `Empty` named an attempt that cannot own the slot; nothing changed.
    AttemptRejected,
    /// The probe claim is no longer current. Clean negative.
    Lost,
}

/// Resolve a probe claim in its own `BEGIN IMMEDIATE`.
pub fn resolve_probe(
    conn: &mut Connection,
    claim: &ProbeClaim,
    result: ProbeResult<'_>,
    now: i64,
) -> Result<ProbeOutcome> {
    let tx = begin_immediate(conn)?;
    let outcome = resolve_probe_in_tx(&tx, claim, result, now)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(outcome)
}

/// [`resolve_probe`] for a caller that holds `BEGIN IMMEDIATE` (for example
/// one that inserts the new collaboration attempt and capability in the same
/// transaction). The caller commits whatever this returns.
pub fn resolve_probe_in_tx(
    tx: &Connection,
    claim: &ProbeClaim,
    result: ProbeResult<'_>,
    now: i64,
) -> Result<ProbeOutcome> {
    validate_now(now)?;
    if let ProbeResult::Pending { candidates } = result {
        validate_candidates(candidates)?;
        if candidates.is_empty() {
            return Err(QuorumError::Usage(
                "an empty pending-review listing is ProbeResult::Empty".into(),
            ));
        }
    }
    let scope = claim.publisher_scope.as_str();
    let held: Option<(i64, Option<i64>)> = tx
        .query_row(
            "SELECT task_id, cleanup_deadline_at FROM github_review_publication_slots
             WHERE publisher_scope=?1 AND pr_number=?2 AND state='probing'
               AND cleanup_attempts=?3 AND next_cleanup_at IS NULL",
            params![scope, claim.pr_number, claim.cycle],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(map_sql_err)?;
    let Some((task_id, deadline)) = held else {
        return Ok(ProbeOutcome::Lost);
    };
    match result {
        ProbeResult::Empty { new_attempt_id } => own_in_tx(tx, claim, task_id, new_attempt_id, now),
        ProbeResult::Inconclusive => Ok(
            match reschedule(
                tx,
                scope,
                claim.pr_number,
                claim.cycle,
                deadline,
                SlotState::Probing,
                "probe_inconclusive",
                now,
            )? {
                Backoff::Rescheduled(next_probe_at) => ProbeOutcome::Rescheduled { next_probe_at },
                Backoff::Blocked => ProbeOutcome::Blocked(SlotBlockReason::RetryExhausted),
            },
        ),
        ProbeResult::Pending { candidates } => {
            probe_pending_in_tx(tx, claim, deadline, candidates, now)
        }
    }
}

fn own_in_tx(
    tx: &Connection,
    claim: &ProbeClaim,
    task_id: i64,
    attempt_id: &str,
    now: i64,
) -> Result<ProbeOutcome> {
    let eligible: bool = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM github_collaboration_attempts a
                 JOIN run_capabilities c ON c.run_id=a.active_run_id AND c.revoked_at IS NULL
                 WHERE a.attempt_id=?1 AND a.role='reviewer' AND a.state='active'
                   AND a.task_id=?2 AND a.pr_number=?3 AND a.head_sha IS NOT NULL
                   AND a.review_owner_marker IS NULL AND a.expires_at>?4
                   AND NOT EXISTS (SELECT 1 FROM github_review_publication_slots s
                                   WHERE s.attempt_id=a.attempt_id))",
            params![attempt_id, task_id, claim.pr_number, now],
            |row| row.get(0),
        )
        .map_err(map_sql_err)?;
    if !eligible {
        return Ok(ProbeOutcome::AttemptRejected);
    }
    let marker = ReviewOwnerMarker::generate();
    let marked = tx
        .execute(
            "UPDATE github_collaboration_attempts SET review_owner_marker=?2, updated_at=?3
             WHERE attempt_id=?1 AND review_owner_marker IS NULL",
            params![attempt_id, marker.as_str(), now],
        )
        .map_err(map_sql_err)?;
    let owned = tx
        .execute(
            "UPDATE github_review_publication_slots
             SET state='owned', attempt_id=?4, review_owner_marker=?5, pending_review_id=NULL,
                 create_send_state='not_started', cleanup_attempts=0, next_cleanup_at=NULL,
                 cleanup_deadline_at=NULL, error_kind=NULL, error_summary=NULL, updated_at=?6
             WHERE publisher_scope=?1 AND pr_number=?2 AND state='probing'
               AND cleanup_attempts=?3 AND next_cleanup_at IS NULL",
            params![
                claim.publisher_scope.as_str(),
                claim.pr_number,
                claim.cycle,
                attempt_id,
                marker.as_str(),
                now
            ],
        )
        .map_err(map_sql_err)?;
    if marked != 1 || owned != 1 {
        return Err(QuorumError::Io(
            "publication slot changed inside its ownership transaction".into(),
        ));
    }
    let status = load_status(tx, claim.publisher_scope.as_str(), claim.pr_number)?
        .ok_or_else(|| QuorumError::Io("publication slot vanished".into()))?;
    Ok(ProbeOutcome::Owned(status))
}

/// The create operation (review sequence 0) of an attempt: `(state,
/// send_state, remote_object_id)`.
fn create_operation(
    tx: &Connection,
    attempt_id: &str,
) -> Result<Option<(String, String, Option<String>)>> {
    tx.query_row(
        "SELECT state, send_state, remote_object_id FROM github_agent_operations
         WHERE attempt_id=?1 AND review_sequence=0",
        [attempt_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
    .map_err(map_sql_err)
}

/// A network child of the attempt is still running or reconciliation-only.
fn attempt_has_inflight(tx: &Connection, attempt_id: &str) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM github_agent_operations
                       WHERE attempt_id=?1
                         AND (state='running' OR (state='queued' AND send_state='ambiguous')))",
        [attempt_id],
        |row| row.get(0),
    )
    .map_err(map_sql_err)
}

/// Record exact marker evidence for an attempt's create: the remote review ID
/// and `confirmed`, on both the operation (if still ambiguous) and the slot.
fn record_marker_evidence(
    tx: &Connection,
    scope: &str,
    pr_number: i64,
    attempt_id: &str,
    review_id: &str,
    now: i64,
) -> Result<()> {
    tx.execute(
        "UPDATE github_agent_operations
         SET send_state='confirmed', remote_object_id=COALESCE(remote_object_id, ?2),
             updated_at=?3
         WHERE attempt_id=?1 AND review_sequence=0 AND send_state='ambiguous'
           AND state IN ('succeeded','failed','cancelled')",
        params![attempt_id, review_id, now],
    )
    .map_err(map_sql_err)?;
    tx.execute(
        "UPDATE github_review_publication_slots
         SET pending_review_id=?3, create_send_state='confirmed', updated_at=?4
         WHERE publisher_scope=?1 AND pr_number=?2",
        params![scope, pr_number, review_id, now],
    )
    .map_err(map_sql_err)?;
    Ok(())
}

fn probe_pending_in_tx(
    tx: &Connection,
    claim: &ProbeClaim,
    deadline: Option<i64>,
    candidates: &[PendingReviewCandidate<'_>],
    now: i64,
) -> Result<ProbeOutcome> {
    let scope = claim.publisher_scope.as_str();
    let pr_number = claim.pr_number;
    let blocked = |reason| -> Result<ProbeOutcome> {
        block_slot(tx, scope, pr_number, reason, now)?;
        Ok(ProbeOutcome::Blocked(reason))
    };
    let [candidate] = candidates else {
        return blocked(SlotBlockReason::MultipleDrafts);
    };
    let Some(marker) = candidate.marker else {
        return blocked(SlotBlockReason::ForeignDraft);
    };
    let owner: Option<(String, String, String, i64, Option<String>)> = tx
        .query_row(
            "SELECT attempt_id, state, role, pr_number, head_sha
             FROM github_collaboration_attempts WHERE review_owner_marker=?1",
            [marker],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(map_sql_err)?;
    let Some((attempt_id, state, role, owner_pr, head_sha)) = owner else {
        return blocked(SlotBlockReason::ForeignDraft);
    };
    if role != "reviewer"
        || owner_pr != pr_number
        || head_sha.as_deref() != Some(candidate.commit_sha)
    {
        return blocked(SlotBlockReason::MismatchedDraft);
    }
    if state != "completed" && state != "revoked" {
        return blocked(SlotBlockReason::ActiveAttempt);
    }
    let owns_other_slot: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM github_review_publication_slots WHERE attempt_id=?1)",
            [&attempt_id],
            |row| row.get(0),
        )
        .map_err(map_sql_err)?;
    if owns_other_slot {
        return blocked(SlotBlockReason::MismatchedDraft);
    }
    if let Some((_, send_state, remote_id)) = create_operation(tx, &attempt_id)? {
        if remote_id
            .as_deref()
            .is_some_and(|known| known != candidate.review_id)
        {
            return blocked(SlotBlockReason::MismatchedDraft);
        }
        if send_state == "not_started" || send_state == "definitely_unsent" {
            return blocked(SlotBlockReason::SendStateDisagreement);
        }
    }
    if attempt_has_inflight(tx, &attempt_id)? {
        // The old attempt's child must settle before cleanup can own the
        // draft; re-probe after backoff rather than racing it.
        return Ok(
            match reschedule(
                tx,
                scope,
                pr_number,
                claim.cycle,
                deadline,
                SlotState::Probing,
                "probe_owner_inflight",
                now,
            )? {
                Backoff::Rescheduled(next_probe_at) => ProbeOutcome::Rescheduled { next_probe_at },
                Backoff::Blocked => ProbeOutcome::Blocked(SlotBlockReason::RetryExhausted),
            },
        );
    }
    let changed = tx
        .execute(
            "UPDATE github_review_publication_slots
             SET state='cleanup_required', attempt_id=?4, review_owner_marker=?5,
                 cleanup_attempts=0, next_cleanup_at=?6, cleanup_deadline_at=?7,
                 error_kind=NULL, error_summary=NULL, updated_at=?6
             WHERE publisher_scope=?1 AND pr_number=?2 AND state='probing'
               AND cleanup_attempts=?3 AND next_cleanup_at IS NULL",
            params![
                scope,
                pr_number,
                claim.cycle,
                attempt_id,
                marker,
                now,
                now.saturating_add(SLOT_RECOVERY_WINDOW_SECS)
            ],
        )
        .map_err(map_sql_err)?;
    if changed != 1 {
        return Err(QuorumError::Io(
            "publication slot changed inside its probe transaction".into(),
        ));
    }
    record_marker_evidence(tx, scope, pr_number, &attempt_id, candidate.review_id, now)?;
    let status = load_status(tx, scope, pr_number)?
        .ok_or_else(|| QuorumError::Io("publication slot vanished".into()))?;
    Ok(ProbeOutcome::CleanupRequired(status))
}

// ---------------------------------------------------------------------------
// Coupling and owned -> cleanup_required
// ---------------------------------------------------------------------------

/// Mirror a settled create operation's send state into its attempt's slot.
/// Only `ambiguous -> confirmed|definitely_unsent` is a legal follow-up of a
/// claimed create, so only an ambiguous slot moves; a late result persists
/// the review ID even when the slot already left `owned`, and never touches a
/// slot owned by a different attempt.
pub(crate) fn couple_create_in_tx(tx: &Connection, attempt_id: &str, now: i64) -> Result<()> {
    let Some((_, send_state, remote_id)) = create_operation(tx, attempt_id)? else {
        return Ok(());
    };
    if send_state != "confirmed" && send_state != "definitely_unsent" {
        return Ok(());
    }
    tx.execute(
        "UPDATE github_review_publication_slots
         SET create_send_state=?2,
             pending_review_id=CASE WHEN ?2='confirmed' THEN COALESCE(?3, pending_review_id)
                                    ELSE pending_review_id END,
             updated_at=?4
         WHERE attempt_id=?1 AND create_send_state='ambiguous'",
        params![attempt_id, send_state, remote_id, now],
    )
    .map_err(map_sql_err)?;
    Ok(())
}

/// Storage-level slot housekeeping run inside every lifecycle/result
/// transaction that can change an attempt or review-sequence row:
///
/// 1. an `owned`/`cleanup_required` slot whose create operation disagrees with
///    its `create_send_state` is `blocked` as a storage fault;
/// 2. an `owned` slot whose attempt is terminal (`completed`/`revoked`, which
///    covers task cancellation, head invalidation, and non-resumable
///    teardown), or that has a failed, cancelled, or completed-after-revocation
///    review-sequence row, becomes `cleanup_required` with a fresh recovery
///    generation. `awaiting_resume` keeps ownership for exact continuation.
///
/// Returns the number of slots changed.
pub(crate) fn sync_in_tx(tx: &Connection, now: i64) -> Result<usize> {
    let blocked = tx
        .execute(
            "UPDATE github_review_publication_slots
             SET state='blocked', next_cleanup_at=NULL, error_kind=?2, error_summary=?3,
                 updated_at=?1
             WHERE state IN ('owned','cleanup_required') AND EXISTS (
                 SELECT 1 FROM github_agent_operations o
                 WHERE o.attempt_id=github_review_publication_slots.attempt_id
                   AND o.review_sequence=0
                   AND o.send_state<>github_review_publication_slots.create_send_state)",
            params![
                now,
                SlotBlockReason::SendStateDisagreement.as_str(),
                SlotBlockReason::SendStateDisagreement.summary()
            ],
        )
        .map_err(map_sql_err)?;
    let cleanup = tx
        .execute(
            "UPDATE github_review_publication_slots
             SET state='cleanup_required', cleanup_attempts=0, next_cleanup_at=?1,
                 cleanup_deadline_at=?2, error_kind=NULL, error_summary=NULL, updated_at=?1
             WHERE state='owned' AND (
                 NOT EXISTS (
                     SELECT 1 FROM github_collaboration_attempts a
                     WHERE a.attempt_id=github_review_publication_slots.attempt_id
                       AND a.state IN ('active','awaiting_resume'))
                 OR EXISTS (
                     SELECT 1 FROM github_agent_operations o
                     WHERE o.attempt_id=github_review_publication_slots.attempt_id
                       AND o.review_sequence IS NOT NULL
                       AND (o.state IN ('failed','cancelled')
                            OR o.completed_after_revocation=1)))",
            params![now, now.saturating_add(SLOT_RECOVERY_WINDOW_SECS)],
        )
        .map_err(map_sql_err)?;
    Ok(blocked + cleanup)
}

/// Explicit `owned -> cleanup_required` for an attempt whose authority the
/// daemon invalidated out of band (for example a launch-head move). Returns
/// `false` (clean negative) when the attempt owns no slot in `owned`.
pub fn require_cleanup(conn: &mut Connection, attempt_id: &str, now: i64) -> Result<bool> {
    validate_now(now)?;
    let tx = begin_immediate(conn)?;
    let changed = tx
        .execute(
            "UPDATE github_review_publication_slots
             SET state='cleanup_required', cleanup_attempts=0, next_cleanup_at=?2,
                 cleanup_deadline_at=?3, error_kind=NULL, error_summary=NULL, updated_at=?2
             WHERE attempt_id=?1 AND state='owned'",
            params![
                attempt_id,
                now,
                now.saturating_add(SLOT_RECOVERY_WINDOW_SECS)
            ],
        )
        .map_err(map_sql_err)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(changed > 0)
}

/// A successful `submit_pending` released the review from pending: delete the
/// attempt's slot while it is still `owned`. Called in the settle transaction.
pub(crate) fn release_after_submit_in_tx(tx: &Connection, attempt_id: &str) -> Result<bool> {
    let changed = tx
        .execute(
            "DELETE FROM github_review_publication_slots WHERE attempt_id=?1 AND state='owned'",
            [attempt_id],
        )
        .map_err(map_sql_err)?;
    Ok(changed > 0)
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

/// A won cleanup cycle. Daemon-internal: not `Serialize`, and the marker is
/// redacted from `Debug`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupClaim {
    pub publisher_scope: String,
    pub pr_number: i64,
    pub cycle: i64,
    pub attempt_id: String,
    pub launch_sha: String,
    pub pending_review_id: Option<String>,
    pub create_send_state: GithubSendState,
    pub owner_marker: ReviewOwnerMarker,
}

/// Claim the oldest due `cleanup_required` slot as `cleanup_running`.
///
/// One `BEGIN IMMEDIATE`: run [`sync_in_tx`], block every slot whose
/// generation is exhausted or that lacks a marker/launch SHA, then pick a due
/// slot whose attempt has no running or reconciliation-only operation, cancel
/// that attempt's still-queued create rows (retaining their send disposition),
/// and increment the cycle. `Ok(None)` is the clean negative.
pub fn claim_cleanup(conn: &mut Connection, now: i64) -> Result<Option<CleanupClaim>> {
    validate_now(now)?;
    let tx = begin_immediate(conn)?;
    sync_in_tx(&tx, now)?;
    let reason = SlotBlockReason::RetryExhausted;
    tx.execute(
        "UPDATE github_review_publication_slots
         SET state='blocked', next_cleanup_at=NULL, error_kind=?3, error_summary=?4,
             updated_at=?1
         WHERE (state='cleanup_required'
                OR (state='probing' AND next_cleanup_at IS NOT NULL AND next_cleanup_at<=?1))
           AND (cleanup_attempts>=?2 OR cleanup_deadline_at<=?1)",
        params![now, MAX_SLOT_CYCLES, reason.as_str(), reason.summary()],
    )
    .map_err(map_sql_err)?;
    let mismatch = SlotBlockReason::MismatchedDraft;
    tx.execute(
        "UPDATE github_review_publication_slots
         SET state='blocked', next_cleanup_at=NULL, error_kind=?2, error_summary=?3,
             updated_at=?1
         WHERE state='cleanup_required' AND (
             review_owner_marker IS NULL OR NOT EXISTS (
                 SELECT 1 FROM github_collaboration_attempts a
                 WHERE a.attempt_id=github_review_publication_slots.attempt_id
                   AND a.head_sha IS NOT NULL))",
        params![now, mismatch.as_str(), mismatch.summary()],
    )
    .map_err(map_sql_err)?;
    let candidate: Option<(String, i64, String)> = tx
        .query_row(
            "SELECT s.publisher_scope, s.pr_number, s.attempt_id
             FROM github_review_publication_slots s
             WHERE s.state='cleanup_required'
               AND (s.next_cleanup_at IS NULL OR s.next_cleanup_at<=?1)
               AND NOT EXISTS (
                   SELECT 1 FROM github_agent_operations o
                   WHERE o.attempt_id=s.attempt_id
                     AND (o.state='running' OR (o.state='queued' AND o.send_state='ambiguous')))
             ORDER BY COALESCE(s.next_cleanup_at, s.updated_at), s.publisher_scope, s.pr_number
             LIMIT 1",
            [now],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(map_sql_err)?;
    let Some((scope, pr_number, attempt_id)) = candidate else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(None);
    };
    tx.execute(
        "UPDATE github_agent_operations
         SET state='cancelled', next_attempt_at=NULL, error_kind=?3,
             error_summary='pending-review cleanup claimed before the create was sent',
             updated_at=?2
         WHERE attempt_id=?1 AND review_sequence=0 AND state='queued' AND active=0
           AND execution_started_at IS NULL
           AND send_state IN ('not_started','definitely_unsent')",
        params![attempt_id, now, ERR_CLEANUP_CANCELLED],
    )
    .map_err(map_sql_err)?;
    let claimed: (i64, Option<String>, String, String) = tx
        .query_row(
            "UPDATE github_review_publication_slots
             SET state='cleanup_running', cleanup_attempts=cleanup_attempts+1,
                 next_cleanup_at=NULL, updated_at=?3
             WHERE publisher_scope=?1 AND pr_number=?2 AND state='cleanup_required'
             RETURNING cleanup_attempts, pending_review_id, create_send_state,
                       review_owner_marker",
            params![scope, pr_number, now],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(map_sql_err)?;
    let launch_sha: String = tx
        .query_row(
            "SELECT head_sha FROM github_collaboration_attempts WHERE attempt_id=?1",
            [&attempt_id],
            |row| row.get(0),
        )
        .map_err(map_sql_err)?;
    let (cycle, pending_review_id, send_state, marker) = claimed;
    let claim = CleanupClaim {
        publisher_scope: scope,
        pr_number,
        cycle,
        attempt_id,
        launch_sha,
        pending_review_id,
        create_send_state: parse_send_state(&send_state)?,
        owner_marker: ReviewOwnerMarker(marker),
    };
    tx.commit().map_err(map_sql_err)?;
    Ok(Some(claim))
}

/// Closed Repository Service result for one cleanup step.
#[derive(Debug, Clone, Copy)]
pub enum CleanupObservation<'a> {
    /// The publisher's current pending reviews on the PR. `fenced` is a
    /// closed publisher-scope/PR ordering fence proving every earlier create
    /// reached a terminal remote disposition before this read; a generic GET
    /// is never fenced.
    Listing {
        candidates: &'a [PendingReviewCandidate<'a>],
        fenced: bool,
    },
    /// Exact delete of the stored review ID succeeded, or 404 after it.
    ExactDeleted { review_id: &'a str },
    /// Authoritative exact-ID lookup shows the stored review is no longer
    /// pending.
    ExactNotPending { review_id: &'a str },
    /// Transport failure, timeout, or unrecognized result.
    Inconclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CleanupOutcome {
    /// Proof of no pending draft: the slot row was deleted.
    Released,
    /// The exact owned draft was found and recorded `confirmed`; the claim
    /// stays `cleanup_running` and the daemon may delete exactly this ID.
    DeleteAuthorized {
        review_id: String,
    },
    /// Negative/inconclusive observation: back to `cleanup_required`.
    Rescheduled {
        next_cleanup_at: i64,
    },
    Blocked(SlotBlockReason),
    /// The claim is no longer current. Clean negative.
    Lost,
}

/// `(pending_review_id, create_send_state, review_owner_marker,
/// cleanup_deadline_at)` of a still-held cleanup claim.
type HeldCleanup = (Option<String>, String, Option<String>, Option<i64>);

/// Apply one cleanup observation to the current claim.
///
/// Release proofs are exactly: a marker-verified exact delete or exact-ID
/// non-pending lookup of the recorded `confirmed` review; an empty listing for
/// `not_started`/`definitely_unsent` (the claim already made every queued
/// create unclaimable); or a fenced empty listing. An unfenced empty listing
/// for `ambiguous` or `confirmed` is one negative observation: it consumes the
/// cycle and reschedules without releasing the slot or authorizing a create.
/// A foreign, multiple, or mismatched draft is never deleted and blocks.
pub fn complete_cleanup(
    conn: &mut Connection,
    claim: &CleanupClaim,
    observation: CleanupObservation<'_>,
    now: i64,
) -> Result<CleanupOutcome> {
    validate_now(now)?;
    match observation {
        CleanupObservation::Listing { candidates, .. } => validate_candidates(candidates)?,
        CleanupObservation::ExactDeleted { review_id }
        | CleanupObservation::ExactNotPending { review_id } => {
            validate_text(review_id, MAX_REVIEW_ID_BYTES, "review id")?
        }
        CleanupObservation::Inconclusive => {}
    }
    let tx = begin_immediate(conn)?;
    let scope = claim.publisher_scope.as_str();
    let pr_number = claim.pr_number;
    let held: Option<HeldCleanup> = tx
        .query_row(
            "SELECT pending_review_id, create_send_state, review_owner_marker,
                    cleanup_deadline_at
             FROM github_review_publication_slots
             WHERE publisher_scope=?1 AND pr_number=?2 AND state='cleanup_running'
               AND cleanup_attempts=?3 AND attempt_id=?4",
            params![scope, pr_number, claim.cycle, claim.attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(map_sql_err)?;
    let Some((pending_id, send_state, marker, deadline)) = held else {
        tx.commit().map_err(map_sql_err)?;
        return Ok(CleanupOutcome::Lost);
    };
    let send_state = parse_send_state(&send_state)?;
    let release = |fenced_ambiguous: bool| -> Result<CleanupOutcome> {
        if fenced_ambiguous {
            // The fence is the closed no-effect proof for the create too.
            tx.execute(
                "UPDATE github_agent_operations
                 SET send_state='definitely_unsent', updated_at=?2
                 WHERE attempt_id=?1 AND review_sequence=0 AND send_state='ambiguous'
                   AND state IN ('failed','cancelled')",
                params![claim.attempt_id, now],
            )
            .map_err(map_sql_err)?;
        }
        tx.execute(
            "DELETE FROM github_review_publication_slots
             WHERE publisher_scope=?1 AND pr_number=?2 AND state='cleanup_running'
               AND cleanup_attempts=?3",
            params![scope, pr_number, claim.cycle],
        )
        .map_err(map_sql_err)?;
        Ok(CleanupOutcome::Released)
    };
    let blocked = |reason| -> Result<CleanupOutcome> {
        block_slot(&tx, scope, pr_number, reason, now)?;
        Ok(CleanupOutcome::Blocked(reason))
    };
    let rescheduled = |kind: &str| -> Result<CleanupOutcome> {
        Ok(
            match reschedule(
                &tx,
                scope,
                pr_number,
                claim.cycle,
                deadline,
                SlotState::CleanupRequired,
                kind,
                now,
            )? {
                Backoff::Rescheduled(next_cleanup_at) => {
                    CleanupOutcome::Rescheduled { next_cleanup_at }
                }
                Backoff::Blocked => CleanupOutcome::Blocked(SlotBlockReason::RetryExhausted),
            },
        )
    };
    let outcome = match observation {
        CleanupObservation::Inconclusive => rescheduled("cleanup_inconclusive")?,
        CleanupObservation::Listing { candidates, fenced } => match candidates {
            [] => match send_state {
                GithubSendState::NotStarted | GithubSendState::DefinitelyUnsent => release(false)?,
                GithubSendState::Ambiguous if fenced => release(true)?,
                GithubSendState::Confirmed if fenced => release(false)?,
                GithubSendState::Ambiguous | GithubSendState::Confirmed => {
                    rescheduled("cleanup_empty_read")?
                }
            },
            [candidate] => {
                let ours = marker.is_some() && candidate.marker == marker.as_deref();
                if !ours {
                    blocked(SlotBlockReason::ForeignDraft)?
                } else if candidate.commit_sha != claim.launch_sha
                    || pending_id
                        .as_deref()
                        .is_some_and(|known| known != candidate.review_id)
                {
                    blocked(SlotBlockReason::MismatchedDraft)?
                } else if matches!(
                    send_state,
                    GithubSendState::NotStarted | GithubSendState::DefinitelyUnsent
                ) {
                    blocked(SlotBlockReason::SendStateDisagreement)?
                } else {
                    record_marker_evidence(
                        &tx,
                        scope,
                        pr_number,
                        &claim.attempt_id,
                        candidate.review_id,
                        now,
                    )?;
                    CleanupOutcome::DeleteAuthorized {
                        review_id: candidate.review_id.to_owned(),
                    }
                }
            }
            _ => blocked(SlotBlockReason::MultipleDrafts)?,
        },
        CleanupObservation::ExactDeleted { review_id }
        | CleanupObservation::ExactNotPending { review_id } => {
            if send_state == GithubSendState::Confirmed && pending_id.as_deref() == Some(review_id)
            {
                release(false)?
            } else {
                blocked(SlotBlockReason::MismatchedDraft)?
            }
        }
    };
    tx.commit().map_err(map_sql_err)?;
    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

/// Startup reconciliation inside the caller's write transaction: an in-flight
/// `probing` slot becomes due for a fresh remote read, and `cleanup_running`
/// returns to `cleanup_required`, both retaining marker, review ID, cycle
/// count, and deadline. Any claim token from the previous daemon is fenced.
/// Then [`sync_in_tx`]. Returns the number of slots changed.
pub fn reconcile_in_tx(tx: &Connection, now: i64) -> Result<usize> {
    validate_now(now)?;
    let probing = tx
        .execute(
            "UPDATE github_review_publication_slots
             SET next_cleanup_at=?1, error_kind='interrupted', updated_at=?1
             WHERE state='probing' AND next_cleanup_at IS NULL",
            [now],
        )
        .map_err(map_sql_err)?;
    let cleanup = tx
        .execute(
            "UPDATE github_review_publication_slots
             SET state='cleanup_required', next_cleanup_at=?1, error_kind='interrupted',
                 updated_at=?1
             WHERE state='cleanup_running'",
            [now],
        )
        .map_err(map_sql_err)?;
    Ok(probing + cleanup + sync_in_tx(tx, now)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collaboration_admission::{OperationErrorSummary, RemoteObjectId};
    use crate::db;
    use crate::github_operations::{self, NewGithubOperation, Settlement};

    const NOW: i64 = 100;
    const SHA: &str = "sha-1";

    fn setup() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open(&dir.path().join("q.db")).unwrap();
        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (1,'review','in-review','owner',1,1);
             INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('run-a',1,'Reviewer','reviewer',1),('run-b',1,'Reviewer','reviewer',1);
             INSERT INTO github_collaboration_attempts(
                 attempt_id,task_id,agent,role,pr_number,head_sha,lifecycle_generation,
                 active_run_id,state,created_at,updated_at,expires_at)
             VALUES ('att-a',1,'Reviewer','reviewer',7,'sha-1',1,'run-a','active',1,1,99999),
                    ('att-b',1,'Reviewer','reviewer',7,'sha-1',1,'run-b','active',1,1,99999);",
        )
        .unwrap();
        (dir, conn)
    }

    fn scope() -> PublisherScope {
        PublisherScope::new("scope-1").unwrap()
    }

    fn probe(conn: &mut Connection, now: i64) -> ProbeClaim {
        match begin_probe(conn, &scope(), 7, 1, now).unwrap() {
            ProbeStart::Claimed(claim) => claim,
            other => panic!("expected a probe claim, got {other:?}"),
        }
    }

    fn own(conn: &mut Connection, attempt: &str) -> SlotStatus {
        let claim = probe(conn, NOW);
        let empty = ProbeResult::Empty {
            new_attempt_id: attempt,
        };
        match resolve_probe(conn, &claim, empty, NOW).unwrap() {
            ProbeOutcome::Owned(status) => status,
            other => panic!("expected owned, got {other:?}"),
        }
    }

    fn slot(conn: &Connection) -> Option<SlotStatus> {
        get(conn, &scope(), 7).unwrap()
    }

    fn enqueue_review(conn: &mut Connection, attempt: &str, op: &str, seq: i64, json: &str) {
        let run = if attempt == "att-a" { "run-a" } else { "run-b" };
        let request = NewGithubOperation {
            operation_id: op,
            client_request_id: op,
            attempt_id: attempt,
            created_by_run_id: run,
            task_id: 1,
            agent: "Reviewer",
            role: "reviewer",
            pr_number: 7,
            head_sha: Some(SHA),
            reviewer_launch_sha: Some(SHA),
            lifecycle_generation: Some(1),
            kind: "pull_request_review_write",
            request_json: json,
            deadline_at: 50_000,
            review_sequence: Some(seq),
            group_key: None,
            github_marker: None,
            created_at: 10,
            expires_at: 99_999,
        };
        assert!(github_operations::enqueue(conn, &request).unwrap());
    }

    fn enqueue_create(conn: &mut Connection, attempt: &str) {
        enqueue_review(conn, attempt, "op-create", 0, r#"{"method":"create"}"#);
    }

    fn op(conn: &Connection, id: &str) -> github_operations::GithubAgentOperation {
        github_operations::get_by_operation_id(conn, id)
            .unwrap()
            .unwrap()
    }

    fn summary() -> OperationErrorSummary {
        OperationErrorSummary::new("timed out").unwrap()
    }

    fn errors(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM errors", [], |r| r.get(0))
            .unwrap()
    }

    fn task_status(conn: &Connection) -> String {
        conn.query_row("SELECT status FROM tasks WHERE id=1", [], |r| r.get(0))
            .unwrap()
    }

    fn attempt_marker(conn: &Connection, attempt: &str) -> Option<String> {
        conn.query_row(
            "SELECT review_owner_marker FROM github_collaboration_attempts WHERE attempt_id=?1",
            [attempt],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn set_attempt_state(conn: &Connection, attempt: &str, state: &str) {
        conn.execute(
            "UPDATE github_collaboration_attempts SET state=?2, active_run_id=NULL
             WHERE attempt_id=?1",
            params![attempt, state],
        )
        .unwrap();
    }

    /// Own the slot with att-a, claim + start its create, and settle it
    /// ambiguous (timeout after hand-off).
    fn ambiguous_create(conn: &mut Connection) {
        own(conn, "att-a");
        enqueue_create(conn, "att-a");
        let won = github_operations::claim_next(conn, NOW).unwrap().unwrap();
        assert!(github_operations::mark_execution_started(conn, &won.claim, NOW).unwrap());
        let s = summary();
        let ambiguous = Settlement::Ambiguous {
            error_kind: "timed_out",
            error_summary: &s,
        };
        assert!(github_operations::settle(conn, &won.claim, ambiguous, NOW).unwrap());
    }

    fn cleanup(conn: &mut Connection, now: i64) -> CleanupClaim {
        claim_cleanup(conn, now).unwrap().expect("cleanup claim")
    }

    fn empty_listing() -> CleanupObservation<'static> {
        CleanupObservation::Listing {
            candidates: &[],
            fenced: false,
        }
    }

    #[test]
    fn probe_inserts_one_slot_per_scope_and_pr() {
        let (_dir, mut conn) = setup();
        let claim = probe(&mut conn, NOW);
        assert_eq!(claim.cycle, 1);
        let status = slot(&conn).unwrap();
        assert_eq!(status.state, SlotState::Probing);
        assert_eq!(status.cleanup_attempts, 1);
        assert_eq!(
            status.cleanup_deadline_at,
            Some(NOW + SLOT_RECOVERY_WINDOW_SECS)
        );
        // In flight: a second probe of the same scope/PR is a clean negative.
        assert!(matches!(
            begin_probe(&mut conn, &scope(), 7, 1, NOW).unwrap(),
            ProbeStart::Occupied(_)
        ));
        // A different PR or scope is a distinct slot.
        assert!(matches!(
            begin_probe(&mut conn, &scope(), 8, 1, NOW).unwrap(),
            ProbeStart::Claimed(_)
        ));
        let other = PublisherScope::new("scope-2").unwrap();
        assert!(matches!(
            begin_probe(&mut conn, &other, 7, 1, NOW).unwrap(),
            ProbeStart::Claimed(_)
        ));
        assert!(PublisherScope::new("").is_err());
        assert!(PublisherScope::new("a\0b").is_err());
    }

    #[test]
    fn slot_admission_is_capped_per_task() {
        let (_dir, mut conn) = setup();
        for pr in 1..=MAX_SLOTS_PER_TASK {
            assert!(matches!(
                begin_probe(&mut conn, &scope(), pr, 1, NOW).unwrap(),
                ProbeStart::Claimed(_)
            ));
        }
        assert_eq!(
            begin_probe(&mut conn, &scope(), 999, 1, NOW).unwrap(),
            ProbeStart::CapacityExceeded
        );
    }

    #[test]
    fn empty_probe_owns_slot_with_hidden_random_marker() {
        let (_dir, mut conn) = setup();
        let owned = own(&mut conn, "att-a");
        assert_eq!(owned.state, SlotState::Owned);
        assert_eq!(owned.attempt_id.as_deref(), Some("att-a"));
        assert_eq!(owned.create_send_state, GithubSendState::NotStarted);
        assert_eq!(
            (owned.cleanup_attempts, owned.cleanup_deadline_at),
            (0, None)
        );

        let marker = attempt_marker(&conn, "att-a").unwrap();
        let stored: String = conn
            .query_row(
                "SELECT review_owner_marker FROM github_review_publication_slots",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(marker, stored);
        let hex: String = marker.chars().filter(char::is_ascii_hexdigit).collect();
        assert!(hex.len() >= 64, "at least 128 random bits: {hex}");
        assert_eq!(
            owner_marker_for_attempt(&conn, "att-a")
                .unwrap()
                .unwrap()
                .as_str(),
            marker
        );

        // Never on an agent-facing or serializable surface.
        let json = serde_json::to_string(&owned).unwrap();
        assert!(!json.contains(&marker) && !json.contains("quorum-review-owner"));
        let debug = format!("{:?}", owner_marker_for_attempt(&conn, "att-a").unwrap());
        assert!(!debug.contains(&marker));
        let rows = serde_json::to_string(&github_operations::list_all(&conn).unwrap()).unwrap();
        assert!(!rows.contains(&marker));

        // Fresh randomness per marker.
        assert_ne!(
            ReviewOwnerMarker::generate().as_str(),
            ReviewOwnerMarker::generate().as_str()
        );
    }

    #[test]
    fn slot_cannot_be_owned_by_two_attempts() {
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        // The same scope/PR is occupied while owned.
        assert!(matches!(
            begin_probe(&mut conn, &scope(), 7, 1, NOW).unwrap(),
            ProbeStart::Occupied(SlotStatus {
                state: SlotState::Owned,
                ..
            })
        ));
        // An attempt that already owns a slot cannot own a second one.
        let other = PublisherScope::new("scope-2").unwrap();
        let ProbeStart::Claimed(claim) = begin_probe(&mut conn, &other, 7, 1, NOW).unwrap() else {
            panic!("expected claim");
        };
        let empty = ProbeResult::Empty {
            new_attempt_id: "att-a",
        };
        assert_eq!(
            resolve_probe(&mut conn, &claim, empty, NOW).unwrap(),
            ProbeOutcome::AttemptRejected
        );
        // Storage backs it: UNIQUE(attempt_id).
        assert!(conn
            .execute(
                "UPDATE github_review_publication_slots SET attempt_id='att-a'
                 WHERE publisher_scope='scope-2'",
                [],
            )
            .is_err());
        // A terminal attempt cannot own a fresh slot.
        set_attempt_state(&conn, "att-b", "revoked");
        let empty = ProbeResult::Empty {
            new_attempt_id: "att-b",
        };
        assert_eq!(
            resolve_probe(&mut conn, &claim, empty, NOW).unwrap(),
            ProbeOutcome::AttemptRejected
        );
    }

    #[test]
    fn create_claim_persists_ambiguous_before_send_then_confirms() {
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        enqueue_create(&mut conn, "att-a");
        let won = github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .unwrap();
        // The claim commit alone (no execution start yet) already persisted
        // ambiguity on both coupled rows.
        assert_eq!(won.operation.send_state, "ambiguous");
        assert_eq!(op(&conn, "op-create").send_state, "ambiguous");
        assert!(op(&conn, "op-create").execution_started_at.is_none());
        assert_eq!(
            slot(&conn).unwrap().create_send_state,
            GithubSendState::Ambiguous
        );
        assert!(github_operations::mark_execution_started(&mut conn, &won.claim, NOW).unwrap());
        let id = RemoteObjectId::new("PRR_1").unwrap();
        let ok = Settlement::Succeeded {
            response_json: None,
            remote_object_id: Some(&id),
        };
        assert!(github_operations::settle(&mut conn, &won.claim, ok, NOW).unwrap());
        let status = slot(&conn).unwrap();
        assert_eq!(status.state, SlotState::Owned);
        assert_eq!(status.create_send_state, GithubSendState::Confirmed);
        assert_eq!(status.pending_review_id.as_deref(), Some("PRR_1"));
    }

    #[test]
    fn create_is_never_claimable_from_ambiguous_confirmed_or_without_owned_slot() {
        for send_state in ["ambiguous", "confirmed"] {
            let (_dir, mut conn) = setup();
            own(&mut conn, "att-a");
            enqueue_create(&mut conn, "att-a");
            conn.execute(
                "UPDATE github_agent_operations SET send_state=?1",
                [send_state],
            )
            .unwrap();
            conn.execute(
                "UPDATE github_review_publication_slots SET create_send_state=?1",
                [send_state],
            )
            .unwrap();
            assert!(
                github_operations::claim_next(&mut conn, NOW)
                    .unwrap()
                    .is_none(),
                "{send_state}"
            );
            assert_eq!(op(&conn, "op-create").attempts, 0);
        }
        // No slot at all: never claimable.
        let (_dir, mut conn) = setup();
        enqueue_create(&mut conn, "att-a");
        assert!(github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .is_none());
    }

    #[test]
    fn cleanup_transition_first_refuses_the_create_claim_and_cancels_it() {
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        enqueue_create(&mut conn, "att-a");
        assert!(require_cleanup(&mut conn, "att-a", NOW).unwrap());
        assert!(!require_cleanup(&mut conn, "att-a", NOW).unwrap());
        assert!(github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .is_none());
        let claim = cleanup(&mut conn, NOW);
        assert_eq!(claim.create_send_state, GithubSendState::NotStarted);
        let create = op(&conn, "op-create");
        assert_eq!(
            (
                create.state.as_str(),
                create.send_state.as_str(),
                create.attempts
            ),
            ("cancelled", "not_started", 0)
        );
        assert!(!format!("{claim:?}").contains(claim.owner_marker.as_str()));
        // Absence with not_started is idempotent cleanup success.
        assert_eq!(
            complete_cleanup(&mut conn, &claim, empty_listing(), NOW).unwrap(),
            CleanupOutcome::Released
        );
        assert!(slot(&conn).is_none());
        assert_eq!(errors(&conn), 0);
    }

    #[test]
    fn slot_lost_between_claim_and_start_cancels_create_as_definitely_unsent() {
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        enqueue_create(&mut conn, "att-a");
        let won = github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .unwrap();
        assert!(require_cleanup(&mut conn, "att-a", NOW).unwrap());
        // Cleanup cannot run while the create is claimed.
        assert!(claim_cleanup(&mut conn, NOW).unwrap().is_none());
        assert!(!github_operations::mark_execution_started(&mut conn, &won.claim, NOW).unwrap());
        let create = op(&conn, "op-create");
        assert_eq!(
            (create.state.as_str(), create.send_state.as_str()),
            ("cancelled", "definitely_unsent")
        );
        assert_eq!(
            slot(&conn).unwrap().create_send_state,
            GithubSendState::DefinitelyUnsent
        );
        let claim = cleanup(&mut conn, NOW);
        assert_eq!(
            complete_cleanup(&mut conn, &claim, empty_listing(), NOW).unwrap(),
            CleanupOutcome::Released
        );
    }

    #[test]
    fn ambiguous_empty_read_never_releases_and_exhaustion_blocks_without_side_effects() {
        let (_dir, mut conn) = setup();
        ambiguous_create(&mut conn);
        let status = slot(&conn).unwrap();
        // A failed review-sequence row moved the owned slot to cleanup.
        assert_eq!(status.state, SlotState::CleanupRequired);
        assert_eq!(status.create_send_state, GithubSendState::Ambiguous);
        assert_eq!(status.cleanup_attempts, 0);
        let attempt_before: String = conn
            .query_row(
                "SELECT state FROM github_collaboration_attempts WHERE attempt_id='att-a'",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let mut now = NOW;
        let mut outcome = CleanupOutcome::Lost;
        for cycle in 1..=MAX_SLOT_CYCLES {
            let claim = cleanup(&mut conn, now);
            assert_eq!(claim.cycle, cycle);
            outcome = complete_cleanup(&mut conn, &claim, empty_listing(), now).unwrap();
            // Never released, never re-sent.
            let status = slot(&conn).unwrap();
            assert_eq!(status.create_send_state, GithubSendState::Ambiguous);
            assert!(github_operations::claim_next(&mut conn, now)
                .unwrap()
                .is_none());
            assert_eq!(op(&conn, "op-create").attempts, 1);
            match outcome {
                CleanupOutcome::Rescheduled { next_cleanup_at } => {
                    assert_eq!(status.state, SlotState::CleanupRequired);
                    // Not due before its backoff.
                    assert!(claim_cleanup(&mut conn, next_cleanup_at - 1)
                        .unwrap()
                        .is_none());
                    now = next_cleanup_at;
                }
                CleanupOutcome::Blocked(_) => break,
                ref other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(
            outcome,
            CleanupOutcome::Blocked(SlotBlockReason::RetryExhausted)
        );
        let status = slot(&conn).unwrap();
        assert_eq!(status.state, SlotState::Blocked);
        assert_eq!(status.error_kind.as_deref(), Some("retry_exhausted"));
        assert!(claim_cleanup(&mut conn, now + 10_000).unwrap().is_none());
        // No verdict, lifecycle, rework, or error side effect.
        assert_eq!(task_status(&conn), "in-review");
        assert_eq!(errors(&conn), 0);
        let attempt_after: String = conn
            .query_row(
                "SELECT state FROM github_collaboration_attempts WHERE attempt_id='att-a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(attempt_before, attempt_after);
    }

    #[test]
    fn wall_clock_bound_blocks_cleanup_and_probe() {
        let (_dir, mut conn) = setup();
        ambiguous_create(&mut conn);
        let deadline = slot(&conn).unwrap().cleanup_deadline_at.unwrap();
        assert!(claim_cleanup(&mut conn, deadline).unwrap().is_none());
        let status = slot(&conn).unwrap();
        assert_eq!(status.state, SlotState::Blocked);
        assert_eq!(status.cleanup_attempts, 0);

        let (_dir, mut conn) = setup();
        let claim = probe(&mut conn, NOW);
        let ProbeOutcome::Rescheduled { next_probe_at } =
            resolve_probe(&mut conn, &claim, ProbeResult::Inconclusive, NOW).unwrap()
        else {
            panic!("expected reschedule");
        };
        assert!(matches!(
            begin_probe(&mut conn, &scope(), 7, 1, next_probe_at - 1).unwrap(),
            ProbeStart::Occupied(_)
        ));
        let late = NOW + SLOT_RECOVERY_WINDOW_SECS;
        assert!(matches!(
            begin_probe(&mut conn, &scope(), 7, 1, late).unwrap(),
            ProbeStart::Occupied(SlotStatus {
                state: SlotState::Blocked,
                ..
            })
        ));
        assert_eq!(errors(&conn), 0);
    }

    #[test]
    fn probe_cycles_share_the_eight_cycle_bound() {
        let (_dir, mut conn) = setup();
        let mut now = NOW;
        let mut claim = probe(&mut conn, now);
        loop {
            match resolve_probe(&mut conn, &claim, ProbeResult::Inconclusive, now).unwrap() {
                ProbeOutcome::Rescheduled { next_probe_at } => {
                    now = next_probe_at;
                    claim = probe(&mut conn, now);
                }
                ProbeOutcome::Blocked(reason) => {
                    assert_eq!(reason, SlotBlockReason::RetryExhausted);
                    break;
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(claim.cycle, MAX_SLOT_CYCLES);
        assert_eq!(slot(&conn).unwrap().state, SlotState::Blocked);
    }

    #[test]
    fn fenced_empty_read_releases_ambiguous_and_resolves_the_create() {
        let (_dir, mut conn) = setup();
        ambiguous_create(&mut conn);
        let claim = cleanup(&mut conn, NOW);
        let fenced = CleanupObservation::Listing {
            candidates: &[],
            fenced: true,
        };
        assert_eq!(
            complete_cleanup(&mut conn, &claim, fenced, NOW).unwrap(),
            CleanupOutcome::Released
        );
        assert!(slot(&conn).is_none());
        assert_eq!(op(&conn, "op-create").send_state, "definitely_unsent");
    }

    #[test]
    fn marker_found_is_recorded_before_exact_delete() {
        let (_dir, mut conn) = setup();
        ambiguous_create(&mut conn);
        let claim = cleanup(&mut conn, NOW);
        let marker = claim.owner_marker.as_str().to_owned();
        let found = [PendingReviewCandidate {
            review_id: "PRR_9",
            marker: Some(&marker),
            commit_sha: SHA,
        }];
        let listing = CleanupObservation::Listing {
            candidates: &found,
            fenced: false,
        };
        assert_eq!(
            complete_cleanup(&mut conn, &claim, listing, NOW).unwrap(),
            CleanupOutcome::DeleteAuthorized {
                review_id: "PRR_9".into()
            }
        );
        let status = slot(&conn).unwrap();
        assert_eq!(status.state, SlotState::CleanupRunning);
        assert_eq!(status.create_send_state, GithubSendState::Confirmed);
        assert_eq!(status.pending_review_id.as_deref(), Some("PRR_9"));
        let create = op(&conn, "op-create");
        assert_eq!(create.send_state, "confirmed");
        assert_eq!(create.remote_object_id.as_deref(), Some("PRR_9"));

        // Crash before the delete result: restart fences the old claim and
        // the exact ID survives.
        let report = github_operations::reconcile_on_start(&mut conn, NOW + 1).unwrap();
        assert_eq!(report.slots, 1);
        let deleted = CleanupObservation::ExactDeleted { review_id: "PRR_9" };
        assert_eq!(
            complete_cleanup(&mut conn, &claim, deleted, NOW + 1).unwrap(),
            CleanupOutcome::Lost
        );
        let claim = cleanup(&mut conn, NOW + 1);
        assert_eq!(claim.cycle, 2);
        assert_eq!(claim.pending_review_id.as_deref(), Some("PRR_9"));
        // A confirmed create never trusts an empty listing.
        assert!(matches!(
            complete_cleanup(&mut conn, &claim, empty_listing(), NOW + 1).unwrap(),
            CleanupOutcome::Rescheduled { .. }
        ));
        let claim = cleanup(&mut conn, NOW + 1_000);
        let not_pending = CleanupObservation::ExactNotPending { review_id: "PRR_9" };
        assert_eq!(
            complete_cleanup(&mut conn, &claim, not_pending, NOW + 1_000).unwrap(),
            CleanupOutcome::Released
        );
        assert!(slot(&conn).is_none());
    }

    #[test]
    fn exact_delete_of_an_unrecorded_id_blocks() {
        let (_dir, mut conn) = setup();
        ambiguous_create(&mut conn);
        let claim = cleanup(&mut conn, NOW);
        let deleted = CleanupObservation::ExactDeleted { review_id: "PRR_X" };
        assert_eq!(
            complete_cleanup(&mut conn, &claim, deleted, NOW).unwrap(),
            CleanupOutcome::Blocked(SlotBlockReason::MismatchedDraft)
        );
    }

    #[test]
    fn foreign_multiple_or_mismatched_drafts_block_cleanup_and_are_never_deleted() {
        let foreign = [PendingReviewCandidate {
            review_id: "PRR_F",
            marker: Some("<!-- someone-else -->"),
            commit_sha: SHA,
        }];
        let unmarked = [PendingReviewCandidate {
            review_id: "PRR_F",
            marker: None,
            commit_sha: SHA,
        }];
        for (label, candidates, expected) in [
            ("foreign", &foreign[..], SlotBlockReason::ForeignDraft),
            ("unmarked", &unmarked[..], SlotBlockReason::ForeignDraft),
        ] {
            let (_dir, mut conn) = setup();
            ambiguous_create(&mut conn);
            let claim = cleanup(&mut conn, NOW);
            let listing = CleanupObservation::Listing {
                candidates,
                fenced: true,
            };
            assert_eq!(
                complete_cleanup(&mut conn, &claim, listing, NOW).unwrap(),
                CleanupOutcome::Blocked(expected),
                "{label}"
            );
            let status = slot(&conn).unwrap();
            assert_eq!(status.state, SlotState::Blocked);
            assert_eq!(status.pending_review_id, None, "{label}");
        }
        // Our marker on the wrong SHA, or alongside another draft.
        let (_dir, mut conn) = setup();
        ambiguous_create(&mut conn);
        let claim = cleanup(&mut conn, NOW);
        let marker = claim.owner_marker.as_str().to_owned();
        let wrong_sha = [PendingReviewCandidate {
            review_id: "PRR_9",
            marker: Some(&marker),
            commit_sha: "sha-2",
        }];
        let listing = CleanupObservation::Listing {
            candidates: &wrong_sha,
            fenced: false,
        };
        assert_eq!(
            complete_cleanup(&mut conn, &claim, listing, NOW).unwrap(),
            CleanupOutcome::Blocked(SlotBlockReason::MismatchedDraft)
        );

        let (_dir, mut conn) = setup();
        ambiguous_create(&mut conn);
        let claim = cleanup(&mut conn, NOW);
        let marker = claim.owner_marker.as_str().to_owned();
        let two = [
            PendingReviewCandidate {
                review_id: "PRR_9",
                marker: Some(&marker),
                commit_sha: SHA,
            },
            foreign[0],
        ];
        let listing = CleanupObservation::Listing {
            candidates: &two,
            fenced: false,
        };
        assert_eq!(
            complete_cleanup(&mut conn, &claim, listing, NOW).unwrap(),
            CleanupOutcome::Blocked(SlotBlockReason::MultipleDrafts)
        );
        assert_eq!(op(&conn, "op-create").send_state, "ambiguous");
    }

    /// att-a owned, created, and terminalized; its slot then released so a
    /// fresh distinct-attempt probe starts from an empty table.
    fn terminal_prior_attempt(conn: &mut Connection, terminal_state: &str) -> String {
        own(conn, "att-a");
        let marker = attempt_marker(conn, "att-a").unwrap();
        conn.execute("DELETE FROM github_review_publication_slots", [])
            .unwrap();
        set_attempt_state(conn, "att-a", terminal_state);
        marker
    }

    #[test]
    fn probe_dispositions_for_existing_drafts() {
        // Exact marker/PR/SHA of a terminal attempt -> cleanup_required.
        let (_dir, mut conn) = setup();
        let marker = terminal_prior_attempt(&mut conn, "revoked");
        let claim = probe(&mut conn, NOW);
        let exact = [PendingReviewCandidate {
            review_id: "PRR_OLD",
            marker: Some(&marker),
            commit_sha: SHA,
        }];
        let ProbeOutcome::CleanupRequired(status) = resolve_probe(
            &mut conn,
            &claim,
            ProbeResult::Pending { candidates: &exact },
            NOW,
        )
        .unwrap() else {
            panic!("expected cleanup_required");
        };
        assert_eq!(status.attempt_id.as_deref(), Some("att-a"));
        assert_eq!(status.pending_review_id.as_deref(), Some("PRR_OLD"));
        assert_eq!(status.create_send_state, GithubSendState::Confirmed);
        assert_eq!(status.cleanup_attempts, 0);
        // The stale probe claim is spent.
        assert_eq!(
            resolve_probe(&mut conn, &claim, ProbeResult::Inconclusive, NOW).unwrap(),
            ProbeOutcome::Lost
        );
        let cleanup_claim = cleanup(&mut conn, NOW);
        let deleted = CleanupObservation::ExactDeleted {
            review_id: "PRR_OLD",
        };
        assert_eq!(
            complete_cleanup(&mut conn, &cleanup_claim, deleted, NOW).unwrap(),
            CleanupOutcome::Released
        );

        // Blocked dispositions: delete nothing, provision nothing.
        let cases: [(&str, Option<&str>, &str, SlotBlockReason); 4] = [
            (
                "foreign",
                Some("<!-- other -->"),
                SHA,
                SlotBlockReason::ForeignDraft,
            ),
            ("missing", None, SHA, SlotBlockReason::ForeignDraft),
            (
                "sha",
                Some("MARKER"),
                "sha-2",
                SlotBlockReason::MismatchedDraft,
            ),
            (
                "active",
                Some("ACTIVE"),
                SHA,
                SlotBlockReason::ActiveAttempt,
            ),
        ];
        for (label, marker_case, sha, expected) in cases {
            let (_dir, mut conn) = setup();
            let terminal = terminal_prior_attempt(&mut conn, "completed");
            let marker = match marker_case {
                Some("MARKER") => Some(terminal.clone()),
                Some("ACTIVE") => {
                    // att-b is live and owns the marker on the draft.
                    conn.execute(
                        "UPDATE github_collaboration_attempts SET review_owner_marker='live-m'
                         WHERE attempt_id='att-b'",
                        [],
                    )
                    .unwrap();
                    Some("live-m".to_owned())
                }
                other => other.map(str::to_owned),
            };
            let claim = probe(&mut conn, NOW);
            let candidates = [PendingReviewCandidate {
                review_id: "PRR_X",
                marker: marker.as_deref(),
                commit_sha: sha,
            }];
            assert_eq!(
                resolve_probe(
                    &mut conn,
                    &claim,
                    ProbeResult::Pending {
                        candidates: &candidates
                    },
                    NOW
                )
                .unwrap(),
                ProbeOutcome::Blocked(expected),
                "{label}"
            );
            let status = slot(&conn).unwrap();
            assert_eq!(status.state, SlotState::Blocked, "{label}");
            assert_eq!(status.attempt_id, None, "{label}");
            assert_eq!(errors(&conn), 0);
        }

        // Multiple candidates.
        let (_dir, mut conn) = setup();
        let claim = probe(&mut conn, NOW);
        let two = [
            PendingReviewCandidate {
                review_id: "A",
                marker: None,
                commit_sha: SHA,
            },
            PendingReviewCandidate {
                review_id: "B",
                marker: None,
                commit_sha: SHA,
            },
        ];
        assert_eq!(
            resolve_probe(
                &mut conn,
                &claim,
                ProbeResult::Pending { candidates: &two },
                NOW
            )
            .unwrap(),
            ProbeOutcome::Blocked(SlotBlockReason::MultipleDrafts)
        );
    }

    #[test]
    fn known_review_id_mismatch_blocks_the_probe() {
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        enqueue_create(&mut conn, "att-a");
        let won = github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .unwrap();
        let id = RemoteObjectId::new("PRR_1").unwrap();
        let ok = Settlement::Succeeded {
            response_json: None,
            remote_object_id: Some(&id),
        };
        assert!(github_operations::settle(&mut conn, &won.claim, ok, NOW).unwrap());
        let marker = attempt_marker(&conn, "att-a").unwrap();
        conn.execute("DELETE FROM github_review_publication_slots", [])
            .unwrap();
        set_attempt_state(&conn, "att-a", "completed");
        let claim = probe(&mut conn, NOW);
        let other_id = [PendingReviewCandidate {
            review_id: "PRR_2",
            marker: Some(&marker),
            commit_sha: SHA,
        }];
        assert_eq!(
            resolve_probe(
                &mut conn,
                &claim,
                ProbeResult::Pending {
                    candidates: &other_id
                },
                NOW
            )
            .unwrap(),
            ProbeOutcome::Blocked(SlotBlockReason::MismatchedDraft)
        );
    }

    #[test]
    fn successful_submit_releases_the_owned_slot() {
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        enqueue_create(&mut conn, "att-a");
        enqueue_review(
            &mut conn,
            "att-a",
            "op-submit",
            1,
            r#"{"method":"submit_pending","event":"COMMENT","body":"done"}"#,
        );
        for expected in ["op-create", "op-submit"] {
            let won = github_operations::claim_next(&mut conn, NOW)
                .unwrap()
                .unwrap();
            assert_eq!(won.operation.operation_id, expected);
            assert!(github_operations::mark_execution_started(&mut conn, &won.claim, NOW).unwrap());
            let id = RemoteObjectId::new("PRR_1").unwrap();
            let ok = Settlement::Succeeded {
                response_json: None,
                remote_object_id: Some(&id),
            };
            assert!(github_operations::settle(&mut conn, &won.claim, ok, NOW).unwrap());
            if expected == "op-create" {
                assert_eq!(slot(&conn).unwrap().state, SlotState::Owned);
            }
        }
        assert!(slot(&conn).is_none());
    }

    #[test]
    fn owned_slot_moves_to_cleanup_on_terminalization_or_failed_predecessor() {
        // Capability revocation (task cancellation / teardown) via claim_next.
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        crate::capabilities::revoke(&mut conn, "run-a", NOW).unwrap();
        assert!(github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .is_none());
        let status = slot(&conn).unwrap();
        assert_eq!(status.state, SlotState::CleanupRequired);
        assert_eq!(
            status.cleanup_deadline_at,
            Some(NOW + SLOT_RECOVERY_WINDOW_SECS)
        );

        // A failed create (definitely unsent) is a failed predecessor.
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        enqueue_create(&mut conn, "att-a");
        let won = github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .unwrap();
        let s = summary();
        let unsent = Settlement::FailedUnsent {
            error_kind: "github_rejected",
            error_summary: &s,
        };
        assert!(github_operations::settle(&mut conn, &won.claim, unsent, NOW).unwrap());
        let status = slot(&conn).unwrap();
        assert_eq!(status.state, SlotState::CleanupRequired);
        assert_eq!(status.create_send_state, GithubSendState::DefinitelyUnsent);
        let claim = cleanup(&mut conn, NOW);
        assert_eq!(
            complete_cleanup(&mut conn, &claim, empty_listing(), NOW).unwrap(),
            CleanupOutcome::Released
        );

        // awaiting_resume keeps ownership for exact continuation.
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        conn.execute(
            "UPDATE github_collaboration_attempts SET state='awaiting_resume', active_run_id=NULL
             WHERE attempt_id='att-a'",
            [],
        )
        .unwrap();
        github_operations::claim_next(&mut conn, NOW).unwrap();
        assert_eq!(slot(&conn).unwrap().state, SlotState::Owned);
    }

    #[test]
    fn proven_unsent_retry_re_enters_ambiguous_on_the_next_claim() {
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        enqueue_create(&mut conn, "att-a");
        let won = github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .unwrap();
        assert!(github_operations::mark_execution_started(&mut conn, &won.claim, NOW).unwrap());
        let s = summary();
        // Without proof the ambiguous create cannot be retried.
        assert!(github_operations::release_for_retry(
            &mut conn,
            &won.claim,
            NOW + 5,
            "dns",
            &s,
            false,
            NOW
        )
        .is_err());
        assert!(matches!(
            github_operations::release_for_retry(
                &mut conn,
                &won.claim,
                NOW + 5,
                "dns",
                &s,
                true,
                NOW
            )
            .unwrap(),
            github_operations::RetryOutcome::Requeued { .. }
        ));
        assert_eq!(op(&conn, "op-create").send_state, "definitely_unsent");
        let status = slot(&conn).unwrap();
        assert_eq!(
            (status.state, status.create_send_state),
            (SlotState::Owned, GithubSendState::DefinitelyUnsent)
        );
        let again = github_operations::claim_next(&mut conn, NOW + 5)
            .unwrap()
            .unwrap();
        assert_eq!(again.operation.send_state, "ambiguous");
        assert_eq!(
            slot(&conn).unwrap().create_send_state,
            GithubSendState::Ambiguous
        );
    }

    #[test]
    fn send_state_disagreement_blocks_the_slot() {
        let (_dir, mut conn) = setup();
        own(&mut conn, "att-a");
        enqueue_create(&mut conn, "att-a");
        conn.execute(
            "UPDATE github_review_publication_slots SET create_send_state='ambiguous'",
            [],
        )
        .unwrap();
        assert!(github_operations::claim_next(&mut conn, NOW)
            .unwrap()
            .is_none());
        let status = slot(&conn).unwrap();
        assert_eq!(status.state, SlotState::Blocked);
        assert_eq!(
            status.error_kind.as_deref(),
            Some("send_state_disagreement")
        );
        assert_eq!(op(&conn, "op-create").attempts, 0);
    }

    #[test]
    fn startup_makes_probing_and_cleanup_running_reconciliation_required() {
        let (_dir, mut conn) = setup();
        let stale_probe = probe(&mut conn, NOW);
        let other = PublisherScope::new("scope-2").unwrap();
        conn.execute(
            "INSERT INTO github_review_publication_slots(
                 publisher_scope,pr_number,task_id,attempt_id,state,review_owner_marker,
                 create_send_state,cleanup_attempts,cleanup_deadline_at,pending_review_id,
                 created_at,updated_at)
             VALUES ('scope-2',7,1,'att-b','cleanup_running','m','confirmed',3,9999,'PRR_7',1,1)",
            [],
        )
        .unwrap();
        let report = github_operations::reconcile_on_start(&mut conn, NOW + 1).unwrap();
        assert_eq!(report.slots, 2);
        assert_eq!(
            resolve_probe(
                &mut conn,
                &stale_probe,
                ProbeResult::Empty {
                    new_attempt_id: "att-a"
                },
                NOW + 1
            )
            .unwrap(),
            ProbeOutcome::Lost
        );
        let fresh = probe(&mut conn, NOW + 1);
        assert_eq!(fresh.cycle, 2);
        let running = get(&conn, &other, 7).unwrap().unwrap();
        assert_eq!(running.state, SlotState::CleanupRequired);
        assert_eq!(
            (
                running.cleanup_attempts,
                running.cleanup_deadline_at,
                running.pending_review_id.as_deref()
            ),
            (3, Some(9999), Some("PRR_7"))
        );
        assert!(attempt_marker(&conn, "att-a").is_none());
    }

    #[test]
    fn backoff_is_exponential_and_bounded() {
        assert_eq!(backoff_secs(1), SLOT_BACKOFF_BASE_SECS);
        assert_eq!(backoff_secs(2), 2 * SLOT_BACKOFF_BASE_SECS);
        assert_eq!(backoff_secs(MAX_SLOT_CYCLES), SLOT_BACKOFF_MAX_SECS);
        assert_eq!(backoff_secs(i64::MAX), SLOT_BACKOFF_MAX_SECS);
    }

    #[test]
    fn observations_are_bounded_and_nul_free() {
        let (_dir, mut conn) = setup();
        let claim = probe(&mut conn, NOW);
        let bad = [PendingReviewCandidate {
            review_id: "a\0b",
            marker: None,
            commit_sha: SHA,
        }];
        assert!(resolve_probe(
            &mut conn,
            &claim,
            ProbeResult::Pending { candidates: &bad },
            NOW
        )
        .is_err());
        assert!(resolve_probe(
            &mut conn,
            &claim,
            ProbeResult::Pending { candidates: &[] },
            NOW
        )
        .is_err());
        // Usage errors leave the claim intact.
        assert_eq!(slot(&conn).unwrap().state, SlotState::Probing);
    }
}
