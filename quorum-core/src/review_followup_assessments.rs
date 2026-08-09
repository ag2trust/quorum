//! Dormant storage authority for review follow-up assessments.
//!
//! The types in this module map every column in the assessment and membership
//! tables while keeping persisted enum and boolean values closed. Assessment
//! materialization and every state change are guarded core writes. Nothing in
//! this module schedules work, invokes a provider, applies dispositions, or
//! otherwise activates follow-up planning.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use crate::review_followups::{MAX_FOLLOWUP_ARTIFACTS, MAX_FOLLOWUP_TEXT_BYTES};
use rusqlite::{params, Connection, Error as SqlError, OptionalExtension, Row, Transaction};
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

/// Independent retry caps. The attempt that reaches the cap moves the
/// assessment to `held`; a fourth attempt can never be persisted.
pub const MAX_SEMANTIC_PROPOSALS: usize = 3;
pub const MAX_ASSESSMENT_PROVIDER_FAILURES: usize = 3;

const MAX_HOLD_CODE_BYTES: usize = 128;
const MAX_HOLD_SUMMARY_BYTES: usize = 2048;

macro_rules! closed_text_enum {
    ($name:ident, $label:literal, {$($variant:ident => $value:literal),+ $(,)?}) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = QuorumError;

            fn from_str(value: &str) -> Result<Self> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(QuorumError::Usage(format!(
                        "invalid {}: {value}", $label
                    ))),
                }
            }
        }
    };
}

closed_text_enum!(FollowupScopeKind, "follow-up assessment scope kind", {
    Task => "task",
    Graph => "graph",
});

closed_text_enum!(FollowupAssessmentState, "follow-up assessment state", {
    Pending => "pending",
    Planning => "planning",
    ProviderBackoff => "provider-backoff",
    Held => "held",
    Completed => "completed",
});

fn validate_optional_text(field: &str, value: Option<&str>) -> Result<()> {
    if value.is_some_and(|value| {
        value.is_empty() || value.contains('\0') || value.len() > MAX_FOLLOWUP_TEXT_BYTES
    }) {
        return Err(QuorumError::Usage(format!(
            "invalid bounded follow-up assessment {field}"
        )));
    }
    Ok(())
}

/// Canonical SQLite authority target for one assessment scope.
pub fn assessment_target(scope_kind: FollowupScopeKind, scope_id: i64) -> Result<String> {
    if scope_id <= 0 {
        return Err(QuorumError::Usage(
            "follow-up assessment scope id must be positive".into(),
        ));
    }
    Ok(format!("followup:{}:{scope_id}", scope_kind.as_str()))
}

/// One fully typed stored row from `review_followup_assessments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewFollowupAssessment {
    id: i64,
    target: String,
    scope_kind: FollowupScopeKind,
    scope_id: i64,
    source_task_id: i64,
    state: FollowupAssessmentState,
    active: bool,
    proposal_attempts: usize,
    provider_failures: usize,
    planner_provider: Option<String>,
    planner_model: Option<String>,
    planner_assignment_id: Option<i64>,
    base_sha: Option<String>,
    hold_code: Option<String>,
    hold_summary: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl ReviewFollowupAssessment {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i64,
        target: String,
        scope_kind: FollowupScopeKind,
        scope_id: i64,
        source_task_id: i64,
        state: FollowupAssessmentState,
        active: bool,
        proposal_attempts: usize,
        provider_failures: usize,
        planner_provider: Option<String>,
        planner_model: Option<String>,
        planner_assignment_id: Option<i64>,
        base_sha: Option<String>,
        hold_code: Option<String>,
        hold_summary: Option<String>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<Self> {
        if id <= 0 || source_task_id <= 0 {
            return Err(QuorumError::Usage(
                "follow-up assessment relationship ids must be positive".into(),
            ));
        }
        if planner_assignment_id.is_some_and(|id| id <= 0) {
            return Err(QuorumError::Usage(
                "follow-up assessment planner assignment id must be positive".into(),
            ));
        }
        if target != assessment_target(scope_kind, scope_id)? {
            return Err(QuorumError::Usage(
                "follow-up assessment target does not match its scope".into(),
            ));
        }
        for (field, value) in [
            ("planner provider", planner_provider.as_deref()),
            ("planner model", planner_model.as_deref()),
            ("base SHA", base_sha.as_deref()),
            ("hold code", hold_code.as_deref()),
            ("hold summary", hold_summary.as_deref()),
        ] {
            validate_optional_text(field, value)?;
        }
        if created_at < 0 || updated_at < created_at {
            return Err(QuorumError::Usage(
                "follow-up assessment timestamps are inconsistent".into(),
            ));
        }
        if proposal_attempts > MAX_SEMANTIC_PROPOSALS {
            return Err(QuorumError::Usage(
                "follow-up proposal attempts exceed the retry bound".into(),
            ));
        }
        if provider_failures > MAX_ASSESSMENT_PROVIDER_FAILURES {
            return Err(QuorumError::Usage(
                "follow-up provider failures exceed the retry bound".into(),
            ));
        }
        Ok(Self {
            id,
            target,
            scope_kind,
            scope_id,
            source_task_id,
            state,
            active,
            proposal_attempts,
            provider_failures,
            planner_provider,
            planner_model,
            planner_assignment_id,
            base_sha,
            hold_code,
            hold_summary,
            created_at,
            updated_at,
        })
    }

    /// Reconstruct a row from SQLite, rejecting open-ended text enums, invalid
    /// boolean sentinels, and negative counters before they reach callers.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored(
        id: i64,
        target: String,
        scope_kind: &str,
        scope_id: i64,
        source_task_id: i64,
        state: &str,
        active: i64,
        proposal_attempts: i64,
        provider_failures: i64,
        planner_provider: Option<String>,
        planner_model: Option<String>,
        planner_assignment_id: Option<i64>,
        base_sha: Option<String>,
        hold_code: Option<String>,
        hold_summary: Option<String>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<Self> {
        let active = match active {
            0 => false,
            1 => true,
            _ => {
                return Err(QuorumError::Usage(
                    "invalid follow-up assessment active sentinel".into(),
                ))
            }
        };
        let proposal_attempts = usize::try_from(proposal_attempts).map_err(|_| {
            QuorumError::Usage("follow-up proposal attempts cannot be negative".into())
        })?;
        let provider_failures = usize::try_from(provider_failures).map_err(|_| {
            QuorumError::Usage("follow-up provider failures cannot be negative".into())
        })?;
        Self::new(
            id,
            target,
            scope_kind.parse()?,
            scope_id,
            source_task_id,
            state.parse()?,
            active,
            proposal_attempts,
            provider_failures,
            planner_provider,
            planner_model,
            planner_assignment_id,
            base_sha,
            hold_code,
            hold_summary,
            created_at,
            updated_at,
        )
    }

    pub fn id(&self) -> i64 {
        self.id
    }
    pub fn target(&self) -> &str {
        &self.target
    }
    pub fn scope_kind(&self) -> FollowupScopeKind {
        self.scope_kind
    }
    pub fn scope_id(&self) -> i64 {
        self.scope_id
    }
    pub fn source_task_id(&self) -> i64 {
        self.source_task_id
    }
    pub fn state(&self) -> FollowupAssessmentState {
        self.state
    }
    pub fn active(&self) -> bool {
        self.active
    }
    pub fn proposal_attempts(&self) -> usize {
        self.proposal_attempts
    }
    pub fn provider_failures(&self) -> usize {
        self.provider_failures
    }
    pub fn planner_provider(&self) -> Option<&str> {
        self.planner_provider.as_deref()
    }
    pub fn planner_model(&self) -> Option<&str> {
        self.planner_model.as_deref()
    }
    pub fn planner_assignment_id(&self) -> Option<i64> {
        self.planner_assignment_id
    }
    pub fn base_sha(&self) -> Option<&str> {
        self.base_sha.as_deref()
    }
    pub fn hold_code(&self) -> Option<&str> {
        self.hold_code.as_deref()
    }
    pub fn hold_summary(&self) -> Option<&str> {
        self.hold_summary.as_deref()
    }
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
    pub fn updated_at(&self) -> i64 {
        self.updated_at
    }
}

/// One fully typed immutable membership row from
/// `review_followup_assessment_artifacts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReviewFollowupAssessmentArtifact {
    assessment_id: i64,
    artifact_id: i64,
}

impl ReviewFollowupAssessmentArtifact {
    pub fn new(assessment_id: i64, artifact_id: i64) -> Result<Self> {
        if assessment_id <= 0 || artifact_id <= 0 {
            return Err(QuorumError::Usage(
                "follow-up assessment membership ids must be positive".into(),
            ));
        }
        Ok(Self {
            assessment_id,
            artifact_id,
        })
    }

    pub fn assessment_id(self) -> i64 {
        self.assessment_id
    }

    pub fn artifact_id(self) -> i64 {
        self.artifact_id
    }
}

/// Validated input for one absent-only assessment and its complete immutable
/// artifact membership. Eligibility queries live outside this write boundary;
/// callers pass the artifact IDs selected by such a read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewReviewFollowupAssessment {
    scope_kind: FollowupScopeKind,
    scope_id: i64,
    source_task_id: i64,
    artifact_ids: Vec<i64>,
    created_at: i64,
}

impl NewReviewFollowupAssessment {
    pub fn new(
        scope_kind: FollowupScopeKind,
        scope_id: i64,
        source_task_id: i64,
        artifact_ids: Vec<i64>,
        created_at: i64,
    ) -> Result<Self> {
        assessment_target(scope_kind, scope_id)?;
        if source_task_id <= 0 {
            return Err(QuorumError::Usage(
                "follow-up assessment source task id must be positive".into(),
            ));
        }
        if artifact_ids.is_empty() || artifact_ids.len() > MAX_FOLLOWUP_ARTIFACTS {
            return Err(QuorumError::Usage(format!(
                "follow-up assessment membership must contain 1..={MAX_FOLLOWUP_ARTIFACTS} artifacts"
            )));
        }
        let mut unique = HashSet::with_capacity(artifact_ids.len());
        if artifact_ids
            .iter()
            .any(|artifact_id| *artifact_id <= 0 || !unique.insert(*artifact_id))
        {
            return Err(QuorumError::Usage(
                "follow-up assessment artifact ids must be positive and unique".into(),
            ));
        }
        if created_at < 0 {
            return Err(QuorumError::Usage(
                "follow-up assessment creation time cannot be negative".into(),
            ));
        }
        Ok(Self {
            scope_kind,
            scope_id,
            source_task_id,
            artifact_ids,
            created_at,
        })
    }

    pub fn scope_kind(&self) -> FollowupScopeKind {
        self.scope_kind
    }
    pub fn scope_id(&self) -> i64 {
        self.scope_id
    }
    pub fn source_task_id(&self) -> i64 {
        self.source_task_id
    }
    pub fn artifact_ids(&self) -> &[i64] {
        &self.artifact_ids
    }
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
}

/// Provider evidence bound to one guarded transition into `planning`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowupPlanningInput<'a> {
    pub provider: &'a str,
    pub model: &'a str,
    pub assignment_id: Option<i64>,
    pub base_sha: &'a str,
    pub now: i64,
}

impl FollowupPlanningInput<'_> {
    fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("planner provider", self.provider),
            ("planner model", self.model),
        ] {
            if value.is_empty() || value.contains('\0') || value.len() > MAX_FOLLOWUP_TEXT_BYTES {
                return Err(QuorumError::Usage(format!(
                    "invalid bounded follow-up assessment {field}"
                )));
            }
        }
        if self.assignment_id.is_some_and(|id| id <= 0) {
            return Err(QuorumError::Usage(
                "follow-up assessment planner assignment id must be positive".into(),
            ));
        }
        if !matches!(self.base_sha.len(), 40 | 64)
            || !self.base_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(QuorumError::Usage(
                "invalid follow-up assessment base SHA".into(),
            ));
        }
        validate_now(self.now)
    }
}

/// Atomically create one pending assessment and every immutable membership row.
///
/// A duplicate scope, an artifact already assigned elsewhere, or an artifact
/// that is absent/disposed is an expected clean negative. In all three cases
/// the transaction leaves neither the new assessment nor any partial membership.
pub fn materialize_assessment(
    conn: &mut Connection,
    value: &NewReviewFollowupAssessment,
) -> Result<Option<ReviewFollowupAssessment>> {
    let target = assessment_target(value.scope_kind, value.scope_id)?;
    let tx = begin_immediate(conn)?;
    let inserted = tx
        .execute(
            "INSERT INTO review_followup_assessments(
                 target,scope_kind,scope_id,source_task_id,state,active,
                 proposal_attempts,provider_failures,created_at,updated_at)
             SELECT ?1,?2,?3,id,'pending',0,0,0,?5,?5 FROM tasks WHERE id=?4
             ON CONFLICT DO NOTHING",
            params![
                target,
                value.scope_kind.as_str(),
                value.scope_id,
                value.source_task_id,
                value.created_at,
            ],
        )
        .map_err(map_sql_err)?;
    if inserted == 0 {
        tx.commit().map_err(map_sql_err)?;
        return Ok(None);
    }

    let assessment_id = tx.last_insert_rowid();
    for artifact_id in &value.artifact_ids {
        let inserted = tx.execute(
            "INSERT INTO review_followup_assessment_artifacts(assessment_id,artifact_id)
             SELECT ?1,id FROM review_followup_artifacts
             WHERE id=?2 AND disposition IS NULL
             ON CONFLICT DO NOTHING",
            params![assessment_id, artifact_id],
        );
        match inserted {
            Ok(1) => {}
            Ok(0) => {
                tx.rollback().map_err(map_sql_err)?;
                return Ok(None);
            }
            Ok(_) => unreachable!("one artifact id can insert at most one membership"),
            Err(error) if is_unique_constraint(&error) => {
                tx.rollback().map_err(map_sql_err)?;
                return Ok(None);
            }
            Err(error) => return Err(map_sql_err(error)),
        }
    }

    let assessment = read_assessment_tx(&tx, assessment_id)?.ok_or_else(|| {
        QuorumError::Usage("new follow-up assessment disappeared before commit".into())
    })?;
    tx.commit().map_err(map_sql_err)?;
    Ok(Some(assessment))
}

/// Read one closed assessment record, including both independent counters.
pub fn get_assessment(
    conn: &Connection,
    assessment_id: i64,
) -> Result<Option<ReviewFollowupAssessment>> {
    if assessment_id <= 0 {
        return Err(QuorumError::Usage(
            "follow-up assessment id must be positive".into(),
        ));
    }
    read_assessment(conn, assessment_id)
}

/// Read the complete immutable membership in deterministic artifact-id order.
pub fn assessment_artifact_ids(conn: &Connection, assessment_id: i64) -> Result<Vec<i64>> {
    if assessment_id <= 0 {
        return Err(QuorumError::Usage(
            "follow-up assessment id must be positive".into(),
        ));
    }
    let limit = i64::try_from(MAX_FOLLOWUP_ARTIFACTS + 1)
        .map_err(|_| QuorumError::Usage("follow-up artifact bound is invalid".into()))?;
    let mut stmt = conn.prepare(
        "SELECT artifact_id FROM review_followup_assessment_artifacts
         WHERE assessment_id=?1 ORDER BY artifact_id LIMIT ?2",
    )?;
    let ids = stmt
        .query_map(params![assessment_id, limit], |row| row.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if ids.len() > MAX_FOLLOWUP_ARTIFACTS {
        return Err(QuorumError::Usage(
            "stored follow-up assessment membership exceeds its bound".into(),
        ));
    }
    Ok(ids)
}

/// Guard `pending|provider-backoff -> planning` and acquire active authority.
/// A stale state, inconsistent active sentinel, exhausted budget, or lost
/// authority-index race is an expected clean negative.
pub fn begin_planning(
    conn: &mut Connection,
    assessment_id: i64,
    input: &FollowupPlanningInput<'_>,
) -> Result<Option<ReviewFollowupAssessment>> {
    validate_assessment_id(assessment_id)?;
    input.validate()?;
    let tx = begin_immediate(conn)?;
    let result = tx.query_row(
        &format!(
            "UPDATE review_followup_assessments
             SET state='planning',active=1,planner_provider=?2,planner_model=?3,
                 planner_assignment_id=?4,base_sha=?5,hold_code=NULL,hold_summary=NULL,
                 updated_at=?6
             WHERE id=?1 AND state IN ('pending','provider-backoff') AND active=0
               AND proposal_attempts < {MAX_SEMANTIC_PROPOSALS}
               AND provider_failures < {MAX_ASSESSMENT_PROVIDER_FAILURES}
               AND (?4 IS NULL OR EXISTS(
                   SELECT 1 FROM role_assignments WHERE id=?4))
               AND updated_at <= ?6
             RETURNING {ASSESSMENT_COLUMNS}"
        ),
        params![
            assessment_id,
            input.provider,
            input.model,
            input.assignment_id,
            input.base_sha,
            input.now,
        ],
        assessment_from_row,
    );
    finish_guarded_write(tx, result)
}

/// Record one semantic proposal rejection. Only an active planning owner may
/// consume this budget. The first two rejections return to `pending`; the third
/// atomically holds the assessment.
pub fn record_semantic_rejection(
    conn: &mut Connection,
    assessment_id: i64,
    reason_code: &str,
    summary: &str,
    now: i64,
) -> Result<Option<ReviewFollowupAssessment>> {
    validate_attempt(assessment_id, reason_code, summary, now)?;
    let tx = begin_immediate(conn)?;
    let result = tx.query_row(
        &format!(
            "UPDATE review_followup_assessments
             SET proposal_attempts=proposal_attempts+1,
                 state=CASE WHEN proposal_attempts+1={MAX_SEMANTIC_PROPOSALS}
                            THEN 'held' ELSE 'pending' END,
                 active=0,
                 hold_code=CASE WHEN proposal_attempts+1={MAX_SEMANTIC_PROPOSALS}
                                THEN ?2 ELSE NULL END,
                 hold_summary=CASE WHEN proposal_attempts+1={MAX_SEMANTIC_PROPOSALS}
                                   THEN ?3 ELSE NULL END,
                 updated_at=?4
             WHERE id=?1 AND state='planning' AND active=1
               AND proposal_attempts < {MAX_SEMANTIC_PROPOSALS}
               AND updated_at <= ?4
             RETURNING {ASSESSMENT_COLUMNS}"
        ),
        params![assessment_id, reason_code, summary, now],
        assessment_from_row,
    );
    finish_guarded_write(tx, result)
}

/// Record one provider/protocol/sandbox failure without consuming the semantic
/// proposal budget. The first two failures enter backoff; the third holds.
pub fn record_provider_failure(
    conn: &mut Connection,
    assessment_id: i64,
    reason_code: &str,
    summary: &str,
    now: i64,
) -> Result<Option<ReviewFollowupAssessment>> {
    validate_attempt(assessment_id, reason_code, summary, now)?;
    let tx = begin_immediate(conn)?;
    let result = tx.query_row(
        &format!(
            "UPDATE review_followup_assessments
             SET provider_failures=provider_failures+1,
                 state=CASE WHEN provider_failures+1={MAX_ASSESSMENT_PROVIDER_FAILURES}
                            THEN 'held' ELSE 'provider-backoff' END,
                 active=0,
                 hold_code=CASE WHEN provider_failures+1={MAX_ASSESSMENT_PROVIDER_FAILURES}
                                THEN ?2 ELSE NULL END,
                 hold_summary=CASE WHEN provider_failures+1={MAX_ASSESSMENT_PROVIDER_FAILURES}
                                   THEN ?3 ELSE NULL END,
                 updated_at=?4
             WHERE id=?1 AND state='planning' AND active=1
               AND provider_failures < {MAX_ASSESSMENT_PROVIDER_FAILURES}
               AND updated_at <= ?4
             RETURNING {ASSESSMENT_COLUMNS}"
        ),
        params![assessment_id, reason_code, summary, now],
        assessment_from_row,
    );
    finish_guarded_write(tx, result)
}

/// Guard one explicit nonterminal state into `held`. Supplying the expected
/// state makes a concurrent phase change a clean negative. This recovery path
/// deliberately accepts either active sentinel so inconsistent stored state can
/// be made inert without granting authority.
pub fn hold_assessment(
    conn: &mut Connection,
    assessment_id: i64,
    expected: FollowupAssessmentState,
    reason_code: &str,
    summary: &str,
    now: i64,
) -> Result<Option<ReviewFollowupAssessment>> {
    validate_attempt(assessment_id, reason_code, summary, now)?;
    if matches!(
        expected,
        FollowupAssessmentState::Held | FollowupAssessmentState::Completed
    ) {
        return Err(QuorumError::Usage(
            "only a nonterminal follow-up assessment can be held".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let result = tx.query_row(
        &format!(
            "UPDATE review_followup_assessments
             SET state='held',active=0,hold_code=?3,hold_summary=?4,updated_at=?5
             WHERE id=?1 AND state=?2 AND updated_at <= ?5
             RETURNING {ASSESSMENT_COLUMNS}"
        ),
        params![assessment_id, expected.as_str(), reason_code, summary, now],
        assessment_from_row,
    );
    finish_guarded_write(tx, result)
}

/// Guard the sole successful terminal transition: active `planning -> completed`.
pub fn complete_assessment(
    conn: &mut Connection,
    assessment_id: i64,
    now: i64,
) -> Result<Option<ReviewFollowupAssessment>> {
    validate_assessment_id(assessment_id)?;
    validate_now(now)?;
    let tx = begin_immediate(conn)?;
    let result = tx.query_row(
        &format!(
            "UPDATE review_followup_assessments
             SET state='completed',active=0,hold_code=NULL,hold_summary=NULL,updated_at=?2
             WHERE id=?1 AND state='planning' AND active=1 AND updated_at <= ?2
             RETURNING {ASSESSMENT_COLUMNS}"
        ),
        params![assessment_id, now],
        assessment_from_row,
    );
    finish_guarded_write(tx, result)
}

const ASSESSMENT_COLUMNS: &str = "id,target,scope_kind,scope_id,source_task_id,state,active,
    proposal_attempts,provider_failures,planner_provider,planner_model,
    planner_assignment_id,base_sha,hold_code,hold_summary,created_at,updated_at";

fn read_assessment(
    conn: &Connection,
    assessment_id: i64,
) -> Result<Option<ReviewFollowupAssessment>> {
    conn.query_row(
        &format!("SELECT {ASSESSMENT_COLUMNS} FROM review_followup_assessments WHERE id=?1"),
        [assessment_id],
        assessment_from_row,
    )
    .optional()
    .map_err(map_sql_err)
}

fn read_assessment_tx(
    tx: &Transaction<'_>,
    assessment_id: i64,
) -> Result<Option<ReviewFollowupAssessment>> {
    read_assessment(tx, assessment_id)
}

fn assessment_from_row(row: &Row<'_>) -> rusqlite::Result<ReviewFollowupAssessment> {
    ReviewFollowupAssessment::from_stored(
        row.get(0)?,
        row.get(1)?,
        &row.get::<_, String>(2)?,
        row.get(3)?,
        row.get(4)?,
        &row.get::<_, String>(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
        row.get(12)?,
        row.get(13)?,
        row.get(14)?,
        row.get(15)?,
        row.get(16)?,
    )
    .map_err(|error| {
        SqlError::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn finish_guarded_write(
    tx: Transaction<'_>,
    result: rusqlite::Result<ReviewFollowupAssessment>,
) -> Result<Option<ReviewFollowupAssessment>> {
    match result {
        Ok(value) => {
            tx.commit().map_err(map_sql_err)?;
            Ok(Some(value))
        }
        Err(SqlError::QueryReturnedNoRows) => {
            tx.commit().map_err(map_sql_err)?;
            Ok(None)
        }
        Err(error) if is_unique_constraint(&error) => {
            tx.rollback().map_err(map_sql_err)?;
            Ok(None)
        }
        Err(error) => Err(map_sql_err(error)),
    }
}

fn is_unique_constraint(error: &SqlError) -> bool {
    matches!(
        error,
        SqlError::SqliteFailure(failure, _)
            if matches!(
                failure.extended_code,
                rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
                    | rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
            )
    )
}

fn validate_assessment_id(assessment_id: i64) -> Result<()> {
    if assessment_id <= 0 {
        return Err(QuorumError::Usage(
            "follow-up assessment id must be positive".into(),
        ));
    }
    Ok(())
}

fn validate_now(now: i64) -> Result<()> {
    if now < 0 {
        return Err(QuorumError::Usage(
            "follow-up assessment timestamp cannot be negative".into(),
        ));
    }
    Ok(())
}

fn validate_attempt(assessment_id: i64, reason_code: &str, summary: &str, now: i64) -> Result<()> {
    validate_assessment_id(assessment_id)?;
    validate_now(now)?;
    if reason_code.is_empty()
        || reason_code.contains('\0')
        || reason_code.len() > MAX_HOLD_CODE_BYTES
        || summary.is_empty()
        || summary.contains('\0')
        || summary.len() > MAX_HOLD_SUMMARY_BYTES
    {
        return Err(QuorumError::Usage(
            "invalid bounded follow-up assessment reason".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const BASE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn database() -> (TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("assessments.db")).unwrap();
        conn.pragma_update(None, "foreign_keys", true).unwrap();
        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at) VALUES
                 (1,'source one','done','owner',1,1),
                 (2,'source two','done','owner',1,1);
             INSERT INTO review_followup_batches(
                 pr_number,task_id,source_task_id,collector_version,
                 artifact_count,state,created_at,updated_at)
             VALUES (100,1,1,'followups-v1',3,'collected',1,1);
             INSERT INTO review_followup_artifacts(
                 id,pr_number,ordinal,technical_impact,scope_relationship,concern,
                 non_blocking_reason,affected_behavior,desired_outcome,
                 verification_expectations,evidence_ids,created_at,updated_at)
             VALUES
                 (11,100,0,'major','out_of_scope','one','reason','behavior','outcome',
                  '[\"verify\"]','[{\"kind\":\"review\",\"id\":1}]',1,1),
                 (12,100,1,'minor','design_debt','two','reason','behavior','outcome',
                  '[\"verify\"]','[{\"kind\":\"review\",\"id\":2}]',1,1),
                 (13,100,2,'minor','future_requirement','three','reason','behavior','outcome',
                  '[\"verify\"]','[{\"kind\":\"review\",\"id\":3}]',1,1);",
        )
        .unwrap();
        (dir, conn)
    }

    fn new_assessment(
        scope_kind: FollowupScopeKind,
        scope_id: i64,
        source_task_id: i64,
        artifact_ids: Vec<i64>,
    ) -> NewReviewFollowupAssessment {
        NewReviewFollowupAssessment::new(scope_kind, scope_id, source_task_id, artifact_ids, 10)
            .unwrap()
    }

    fn planning(now: i64) -> FollowupPlanningInput<'static> {
        FollowupPlanningInput {
            provider: "codex",
            model: "planner",
            assignment_id: None,
            base_sha: BASE_SHA,
            now,
        }
    }

    fn counts(conn: &Connection) -> (i64, i64, i64) {
        conn.query_row(
            "SELECT
                 (SELECT count(*) FROM review_followup_assessments),
                 (SELECT count(*) FROM review_followup_assessment_artifacts),
                 (SELECT count(*) FROM errors)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
    }

    fn state(conn: &Connection, id: i64) -> (String, i64, i64, i64, Option<String>, i64) {
        conn.query_row(
            "SELECT state,active,proposal_attempts,provider_failures,hold_code,updated_at
             FROM review_followup_assessments WHERE id=?1",
            [id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap()
    }

    fn stored() -> ReviewFollowupAssessment {
        ReviewFollowupAssessment::from_stored(
            1,
            "followup:graph:7".into(),
            "graph",
            7,
            3,
            "provider-backoff",
            1,
            2,
            1,
            Some("openai".into()),
            Some("planner".into()),
            Some(9),
            Some("0123456789abcdef".into()),
            Some("provider-failure".into()),
            Some("retry after the bounded backoff".into()),
            10,
            11,
        )
        .unwrap()
    }

    #[test]
    fn stored_assessment_maps_every_column_to_closed_values() {
        let value = stored();
        assert_eq!(value.id(), 1);
        assert_eq!(value.target(), "followup:graph:7");
        assert_eq!(value.scope_kind(), FollowupScopeKind::Graph);
        assert_eq!(value.scope_id(), 7);
        assert_eq!(value.source_task_id(), 3);
        assert_eq!(value.state(), FollowupAssessmentState::ProviderBackoff);
        assert!(value.active());
        assert_eq!(value.proposal_attempts(), 2);
        assert_eq!(value.provider_failures(), 1);
        assert_eq!(value.planner_provider(), Some("openai"));
        assert_eq!(value.planner_model(), Some("planner"));
        assert_eq!(value.planner_assignment_id(), Some(9));
        assert_eq!(value.base_sha(), Some("0123456789abcdef"));
        assert_eq!(value.hold_code(), Some("provider-failure"));
        assert_eq!(
            value.hold_summary(),
            Some("retry after the bounded backoff")
        );
        assert_eq!(value.created_at(), 10);
        assert_eq!(value.updated_at(), 11);
    }

    #[test]
    fn stored_assessment_rejects_open_or_inconsistent_values() {
        assert!("repository".parse::<FollowupScopeKind>().is_err());
        assert!("retrying".parse::<FollowupAssessmentState>().is_err());

        let mut value = stored();
        value.target = "followup:task:7".into();
        assert!(ReviewFollowupAssessment::new(
            value.id,
            value.target,
            value.scope_kind,
            value.scope_id,
            value.source_task_id,
            value.state,
            value.active,
            value.proposal_attempts,
            value.provider_failures,
            value.planner_provider,
            value.planner_model,
            value.planner_assignment_id,
            value.base_sha,
            value.hold_code,
            value.hold_summary,
            value.created_at,
            value.updated_at,
        )
        .is_err());

        let make = |active, attempts| {
            ReviewFollowupAssessment::from_stored(
                1,
                "followup:task:7".into(),
                "task",
                7,
                7,
                "pending",
                active,
                attempts,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                1,
                1,
            )
        };
        assert!(make(2, 0).is_err());
        assert!(make(0, -1).is_err());
        assert!(make(0, 4).is_err());
    }

    #[test]
    fn membership_requires_positive_relationship_ids() {
        let membership = ReviewFollowupAssessmentArtifact::new(2, 3).unwrap();
        assert_eq!(membership.assessment_id(), 2);
        assert_eq!(membership.artifact_id(), 3);
        assert!(ReviewFollowupAssessmentArtifact::new(0, 3).is_err());
        assert!(ReviewFollowupAssessmentArtifact::new(2, -1).is_err());
    }

    #[test]
    fn materialization_commits_one_assessment_with_complete_immutable_membership() {
        let (_dir, mut conn) = database();
        let input = new_assessment(FollowupScopeKind::Task, 1, 1, vec![12, 11]);
        let assessment = materialize_assessment(&mut conn, &input).unwrap().unwrap();

        assert_eq!(assessment.state(), FollowupAssessmentState::Pending);
        assert!(!assessment.active());
        assert_eq!(assessment.proposal_attempts(), 0);
        assert_eq!(assessment.provider_failures(), 0);
        assert_eq!(
            assessment_artifact_ids(&conn, assessment.id()).unwrap(),
            vec![11, 12]
        );
        assert_eq!(counts(&conn), (1, 2, 0));

        assert!(conn
            .execute(
                "UPDATE review_followup_assessment_artifacts SET artifact_id=13
                 WHERE assessment_id=?1 AND artifact_id=12",
                [assessment.id()],
            )
            .is_err());
        assert!(conn
            .execute(
                "DELETE FROM review_followup_assessment_artifacts
                 WHERE assessment_id=?1 AND artifact_id=12",
                [assessment.id()],
            )
            .is_err());
        assert_eq!(
            assessment_artifact_ids(&conn, assessment.id()).unwrap(),
            vec![11, 12]
        );
        assert_eq!(counts(&conn), (1, 2, 0));
    }

    #[test]
    fn materialization_uniqueness_losses_and_incomplete_membership_roll_back_cleanly() {
        let (_dir, mut conn) = database();
        let first = new_assessment(FollowupScopeKind::Task, 1, 1, vec![11, 12]);
        materialize_assessment(&mut conn, &first).unwrap().unwrap();
        assert!(materialize_assessment(&mut conn, &first).unwrap().is_none());
        assert_eq!(counts(&conn), (1, 2, 0));

        let overlapping = new_assessment(FollowupScopeKind::Task, 2, 2, vec![12, 13]);
        assert!(materialize_assessment(&mut conn, &overlapping)
            .unwrap()
            .is_none());
        assert_eq!(counts(&conn), (1, 2, 0));

        let missing = new_assessment(FollowupScopeKind::Graph, 9, 1, vec![13, 999]);
        assert!(materialize_assessment(&mut conn, &missing)
            .unwrap()
            .is_none());
        assert_eq!(counts(&conn), (1, 2, 0));

        conn.execute(
            "UPDATE review_followup_artifacts
             SET disposition='dismissed',disposition_reason='not actionable'
             WHERE id=13",
            [],
        )
        .unwrap();
        let disposed = new_assessment(FollowupScopeKind::Graph, 10, 1, vec![13]);
        assert!(materialize_assessment(&mut conn, &disposed)
            .unwrap()
            .is_none());
        assert_eq!(counts(&conn), (1, 2, 0));
    }

    #[test]
    fn materialization_missing_source_is_a_clean_negative_without_logging() {
        let (_dir, mut conn) = database();
        conn.pragma_update(None, "foreign_keys", false).unwrap();
        let input = new_assessment(FollowupScopeKind::Task, 404, 404, vec![11]);
        assert!(materialize_assessment(&mut conn, &input).unwrap().is_none());
        assert_eq!(counts(&conn), (0, 0, 0));
    }

    #[test]
    fn guarded_transitions_reject_illegal_states_without_mutation() {
        let (_dir, mut conn) = database();
        let assessment = materialize_assessment(
            &mut conn,
            &new_assessment(FollowupScopeKind::Task, 1, 1, vec![11]),
        )
        .unwrap()
        .unwrap();
        let id = assessment.id();

        assert!(complete_assessment(&mut conn, id, 11).unwrap().is_none());
        assert_eq!(state(&conn, id), ("pending".into(), 0, 0, 0, None, 10));

        conn.pragma_update(None, "foreign_keys", false).unwrap();
        let missing_assignment = FollowupPlanningInput {
            assignment_id: Some(404),
            ..planning(11)
        };
        assert!(begin_planning(&mut conn, id, &missing_assignment)
            .unwrap()
            .is_none());
        assert_eq!(state(&conn, id), ("pending".into(), 0, 0, 0, None, 10));

        let planning_row = begin_planning(&mut conn, id, &planning(11))
            .unwrap()
            .unwrap();
        assert_eq!(planning_row.state(), FollowupAssessmentState::Planning);
        assert!(planning_row.active());
        assert_eq!(state(&conn, id), ("planning".into(), 1, 0, 0, None, 11));
        assert!(begin_planning(&mut conn, id, &planning(12))
            .unwrap()
            .is_none());
        assert_eq!(state(&conn, id), ("planning".into(), 1, 0, 0, None, 11));

        let failed = record_provider_failure(&mut conn, id, "provider", "failed", 12)
            .unwrap()
            .unwrap();
        assert_eq!(failed.state(), FollowupAssessmentState::ProviderBackoff);
        assert_eq!(failed.provider_failures(), 1);
        assert_eq!(failed.proposal_attempts(), 0);
        assert_eq!(
            state(&conn, id),
            ("provider-backoff".into(), 0, 0, 1, None, 12)
        );
        assert!(
            record_semantic_rejection(&mut conn, id, "semantic", "bad", 13)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            state(&conn, id),
            ("provider-backoff".into(), 0, 0, 1, None, 12)
        );

        begin_planning(&mut conn, id, &planning(13))
            .unwrap()
            .unwrap();
        let rejected = record_semantic_rejection(&mut conn, id, "semantic", "bad", 14)
            .unwrap()
            .unwrap();
        assert_eq!(rejected.state(), FollowupAssessmentState::Pending);
        assert_eq!(rejected.proposal_attempts(), 1);
        assert_eq!(rejected.provider_failures(), 1);
        assert_eq!(state(&conn, id), ("pending".into(), 0, 1, 1, None, 14));

        begin_planning(&mut conn, id, &planning(15))
            .unwrap()
            .unwrap();
        let completed = complete_assessment(&mut conn, id, 16).unwrap().unwrap();
        assert_eq!(completed.state(), FollowupAssessmentState::Completed);
        assert!(!completed.active());
        let terminal = state(&conn, id);
        assert_eq!(terminal, ("completed".into(), 0, 1, 1, None, 16));
        assert!(complete_assessment(&mut conn, id, 17).unwrap().is_none());
        assert!(
            record_provider_failure(&mut conn, id, "provider", "late", 17)
                .unwrap()
                .is_none()
        );
        assert_eq!(state(&conn, id), terminal);
        assert_eq!(counts(&conn), (1, 1, 0));
    }

    #[test]
    fn semantic_and_provider_budgets_are_independent_and_bounded() {
        let (_dir, mut conn) = database();
        let semantic = materialize_assessment(
            &mut conn,
            &new_assessment(FollowupScopeKind::Task, 1, 1, vec![11]),
        )
        .unwrap()
        .unwrap();
        for ordinal in 1..=MAX_SEMANTIC_PROPOSALS {
            begin_planning(
                &mut conn,
                semantic.id(),
                &planning(10 + ordinal as i64 * 2 - 1),
            )
            .unwrap()
            .unwrap();
            let row = record_semantic_rejection(
                &mut conn,
                semantic.id(),
                "semantic-rejection",
                "proposal did not cover every artifact",
                10 + ordinal as i64 * 2,
            )
            .unwrap()
            .unwrap();
            assert_eq!(row.proposal_attempts(), ordinal);
            assert_eq!(row.provider_failures(), 0);
            assert_eq!(
                row.state(),
                if ordinal == MAX_SEMANTIC_PROPOSALS {
                    FollowupAssessmentState::Held
                } else {
                    FollowupAssessmentState::Pending
                }
            );
        }
        let semantic_terminal = state(&conn, semantic.id());
        assert_eq!(
            semantic_terminal,
            (
                "held".into(),
                0,
                MAX_SEMANTIC_PROPOSALS as i64,
                0,
                Some("semantic-rejection".into()),
                16,
            )
        );
        assert!(begin_planning(&mut conn, semantic.id(), &planning(17))
            .unwrap()
            .is_none());
        assert_eq!(state(&conn, semantic.id()), semantic_terminal);

        let provider = materialize_assessment(
            &mut conn,
            &new_assessment(FollowupScopeKind::Task, 2, 2, vec![12]),
        )
        .unwrap()
        .unwrap();
        for ordinal in 1..=MAX_ASSESSMENT_PROVIDER_FAILURES {
            begin_planning(
                &mut conn,
                provider.id(),
                &planning(20 + ordinal as i64 * 2 - 1),
            )
            .unwrap()
            .unwrap();
            let row = record_provider_failure(
                &mut conn,
                provider.id(),
                "provider-failure",
                "provider process exited",
                20 + ordinal as i64 * 2,
            )
            .unwrap()
            .unwrap();
            assert_eq!(row.proposal_attempts(), 0);
            assert_eq!(row.provider_failures(), ordinal);
            assert_eq!(
                row.state(),
                if ordinal == MAX_ASSESSMENT_PROVIDER_FAILURES {
                    FollowupAssessmentState::Held
                } else {
                    FollowupAssessmentState::ProviderBackoff
                }
            );
        }
        assert_eq!(
            state(&conn, provider.id()),
            (
                "held".into(),
                0,
                0,
                MAX_ASSESSMENT_PROVIDER_FAILURES as i64,
                Some("provider-failure".into()),
                26,
            )
        );
        assert!(conn
            .execute(
                "UPDATE review_followup_assessments SET provider_failures=4 WHERE id=?1",
                [provider.id()],
            )
            .is_err());
        assert_eq!(
            get_assessment(&conn, provider.id())
                .unwrap()
                .unwrap()
                .provider_failures(),
            3
        );
    }

    #[test]
    fn expected_state_hold_handles_inconsistency_without_stomping_a_race() {
        let (_dir, mut conn) = database();
        let assessment = materialize_assessment(
            &mut conn,
            &new_assessment(FollowupScopeKind::Task, 1, 1, vec![11]),
        )
        .unwrap()
        .unwrap();
        conn.execute(
            "UPDATE review_followup_assessments SET active=1 WHERE id=?1",
            [assessment.id()],
        )
        .unwrap();
        let inconsistent = state(&conn, assessment.id());
        assert!(begin_planning(&mut conn, assessment.id(), &planning(11))
            .unwrap()
            .is_none());
        assert_eq!(state(&conn, assessment.id()), inconsistent);
        assert!(hold_assessment(
            &mut conn,
            assessment.id(),
            FollowupAssessmentState::Planning,
            "inconsistent",
            "wrong expected state",
            12,
        )
        .unwrap()
        .is_none());
        assert_eq!(state(&conn, assessment.id()), inconsistent);
        let held = hold_assessment(
            &mut conn,
            assessment.id(),
            FollowupAssessmentState::Pending,
            "inconsistent-active",
            "pending row retained planning authority",
            12,
        )
        .unwrap()
        .unwrap();
        assert_eq!(held.state(), FollowupAssessmentState::Held);
        assert!(!held.active());
        assert_eq!(
            state(&conn, assessment.id()),
            (
                "held".into(),
                0,
                0,
                0,
                Some("inconsistent-active".into()),
                12
            )
        );
    }

    #[test]
    fn authority_uniqueness_loss_is_a_clean_guarded_negative() {
        let (_dir, mut conn) = database();
        let assessment = materialize_assessment(
            &mut conn,
            &new_assessment(FollowupScopeKind::Task, 1, 1, vec![11]),
        )
        .unwrap()
        .unwrap();
        conn.execute(
            "INSERT INTO review_followup_assessments(
                 target,scope_kind,scope_id,source_task_id,state,active,created_at,updated_at)
             VALUES ('followup:task:1','graph',99,2,'planning',1,10,10)",
            [],
        )
        .unwrap();
        let before = state(&conn, assessment.id());
        assert!(begin_planning(&mut conn, assessment.id(), &planning(11))
            .unwrap()
            .is_none());
        assert_eq!(state(&conn, assessment.id()), before);
        assert_eq!(counts(&conn), (2, 1, 0));
    }
}
