//! Closed, bounded vocabulary for the canonical GitHub collaboration storage.
//!
//! This module owns the capability-bound admission and lifecycle of canonical
//! `github_collaboration_attempts`. GitHub operation admission/execution stays
//! separate: attempts only establish the durable, exact managed-turn identity
//! on which those later paths rely.

use crate::capabilities::{self, LiveCollaborationContext, LiveCollaborationContextResolution};
use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

pub const MAX_COLLABORATION_ID_BYTES: usize = 128;
pub const MAX_COLLABORATION_AGENT_BYTES: usize = 256;
pub const MAX_GITHUB_MARKER_BYTES: usize = 512;
pub const MAX_OPERATION_JSON_BYTES: usize = 64 * 1024;
pub const MAX_OPERATION_ERROR_BYTES: usize = 2 * 1024;
pub const MAX_OPERATION_ATTEMPTS: u8 = 8;
pub const MAX_NONEXPIRED_ATTEMPTS_PER_TASK: i64 = 16;
pub const MAX_NONEXPIRED_ATTEMPTS_PER_REPOSITORY: i64 = 1_024;

macro_rules! bounded_text {
    ($name:ident, $label:literal, $max:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                if value.is_empty() || value.contains('\0') || value.len() > $max {
                    return Err(QuorumError::Usage(format!(
                        "invalid bounded collaboration {}",
                        $label
                    )));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

macro_rules! closed_text_enum {
    ($name:ident, $label:literal, {$($variant:ident => $value:literal),+ $(,)?}) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
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
                    _ => Err(QuorumError::Usage(format!("invalid {}", $label))),
                }
            }
        }
    };
}

bounded_text!(
    CollaborationAttemptId,
    "attempt id",
    MAX_COLLABORATION_ID_BYTES
);
bounded_text!(
    GithubOperationId,
    "operation id",
    MAX_COLLABORATION_ID_BYTES
);
bounded_text!(
    ClientRequestId,
    "client request id",
    MAX_COLLABORATION_ID_BYTES
);
bounded_text!(
    RunCapabilityId,
    "run capability id",
    MAX_COLLABORATION_ID_BYTES
);
bounded_text!(CollaborationAgent, "agent", MAX_COLLABORATION_AGENT_BYTES);
bounded_text!(
    ReviewerHeadSha,
    "reviewer head SHA",
    MAX_COLLABORATION_ID_BYTES
);
bounded_text!(GithubMarker, "GitHub marker", MAX_GITHUB_MARKER_BYTES);
bounded_text!(
    RemoteObjectId,
    "remote object id",
    MAX_COLLABORATION_ID_BYTES
);
bounded_text!(
    OperationErrorSummary,
    "operation error summary",
    MAX_OPERATION_ERROR_BYTES
);

closed_text_enum!(CollaborationRole, "collaboration role", {
    Worker => "worker",
    Reviewer => "reviewer",
});

closed_text_enum!(CollaborationAttemptState, "collaboration attempt state", {
    Active => "active",
    AwaitingResume => "awaiting_resume",
    Completed => "completed",
    Revoked => "revoked",
});

closed_text_enum!(GithubOperationKind, "GitHub operation kind", {
    PullRequestRead => "pull_request_read",
    AddIssueComment => "add_issue_comment",
    PullRequestReviewWrite => "pull_request_review_write",
    AddCommentToPendingReview => "add_comment_to_pending_review",
    AddReplyToPullRequestComment => "add_reply_to_pull_request_comment",
    ResolveReviewThread => "resolve_review_thread",
});

closed_text_enum!(GithubOperationState, "GitHub operation state", {
    Queued => "queued",
    Running => "running",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});

closed_text_enum!(GithubSendState, "GitHub send state", {
    NotStarted => "not_started",
    DefinitelyUnsent => "definitely_unsent",
    Ambiguous => "ambiguous",
    Confirmed => "confirmed",
});

/// The bounded fields an admission boundary can reject without accepting a
/// request. Keeping this closed prevents free-form errors from becoming an
/// endpoint protocol or durable state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationAdmissionField {
    AttemptId,
    OperationId,
    ClientRequestId,
    RunCapabilityId,
    Agent,
    PrNumber,
    ReviewerHeadSha,
    LifecycleGeneration,
    RequestJson,
    GithubMarker,
    Deadline,
    Expiry,
}

/// Closed error outcomes for future atomic attempt/operation admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum CollaborationAdmissionError {
    InvalidField { field: CollaborationAdmissionField },
    CapabilityRejected,
    AttemptNotFound,
    AttemptNotActive,
    AttemptBindingMismatch,
    ActiveAttemptExists,
    AttemptExpired,
    AttemptLimitReached,
    RepositoryAttemptLimitReached,
    ContinuationMismatch,
    OperationAlreadyExists,
    OperationLimitReached,
}

/// JSON text valid for a closed `github_agent_operations` request. The value is
/// parsed before it is re-serialized, so durable storage receives SQL-bound
/// canonical JSON rather than opaque caller-controlled text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct GithubOperationRequestJson(String);

impl GithubOperationRequestJson {
    pub fn new(value: &str) -> Result<Self> {
        if value.is_empty() || value.contains('\0') || value.len() > MAX_OPERATION_JSON_BYTES {
            return Err(QuorumError::Usage(
                "invalid bounded GitHub operation request JSON".into(),
            ));
        }
        let parsed: serde_json::Value = serde_json::from_str(value).map_err(|_| {
            QuorumError::Usage("invalid bounded GitHub operation request JSON".into())
        })?;
        if !parsed.is_object() || json_has_nul(&parsed) {
            return Err(QuorumError::Usage(
                "invalid bounded GitHub operation request JSON".into(),
            ));
        }
        let canonical = serde_json::to_string(&parsed).map_err(|error| {
            QuorumError::Io(format!("serialize GitHub operation request JSON: {error}"))
        })?;
        if canonical.len() > MAX_OPERATION_JSON_BYTES {
            return Err(QuorumError::Usage(
                "invalid bounded GitHub operation request JSON".into(),
            ));
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Bounded JSON retained from a completed GitHub operation. Responses may be
/// any JSON value, unlike operation requests, which are objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct GithubOperationResponseJson(String);

impl GithubOperationResponseJson {
    pub fn new(value: &str) -> Result<Self> {
        if value.is_empty() || value.contains('\0') || value.len() > MAX_OPERATION_JSON_BYTES {
            return Err(QuorumError::Usage(
                "invalid bounded GitHub operation response JSON".into(),
            ));
        }
        let parsed: serde_json::Value = serde_json::from_str(value).map_err(|_| {
            QuorumError::Usage("invalid bounded GitHub operation response JSON".into())
        })?;
        if json_has_nul(&parsed) {
            return Err(QuorumError::Usage(
                "invalid bounded GitHub operation response JSON".into(),
            ));
        }
        let canonical = serde_json::to_string(&parsed).map_err(|error| {
            QuorumError::Io(format!("serialize GitHub operation response JSON: {error}"))
        })?;
        if canonical.len() > MAX_OPERATION_JSON_BYTES {
            return Err(QuorumError::Usage(
                "invalid bounded GitHub operation response JSON".into(),
            ));
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn json_has_nul(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(value) => value.contains('\0'),
        serde_json::Value::Array(values) => values.iter().any(json_has_nul),
        serde_json::Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains('\0') || json_has_nul(value)),
        _ => false,
    }
}

/// A prepared insert for one canonical `github_collaboration_attempts` logical
/// turn, bound to an exact task/agent/role/PR/generation and reviewer launch
/// SHA.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateCollaborationAttemptRequest {
    attempt_id: CollaborationAttemptId,
    task_id: i64,
    agent: CollaborationAgent,
    role: CollaborationRole,
    pr_number: i64,
    reviewer_head_sha: Option<ReviewerHeadSha>,
    lifecycle_generation: i64,
    active_run_id: RunCapabilityId,
    created_at: i64,
    expires_at: i64,
}

impl CreateCollaborationAttemptRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attempt_id: CollaborationAttemptId,
        task_id: i64,
        agent: CollaborationAgent,
        role: CollaborationRole,
        pr_number: i64,
        reviewer_head_sha: Option<ReviewerHeadSha>,
        lifecycle_generation: i64,
        active_run_id: RunCapabilityId,
        created_at: i64,
        expires_at: i64,
    ) -> Result<Self> {
        if lifecycle_generation < 0 {
            return Err(QuorumError::Usage(
                "collaboration lifecycle generation is invalid".into(),
            ));
        }
        validate_collaboration_context(
            task_id,
            role,
            pr_number,
            reviewer_head_sha.as_ref(),
            created_at,
            expires_at,
        )?;
        Ok(Self {
            attempt_id,
            task_id,
            agent,
            role,
            pr_number,
            reviewer_head_sha,
            lifecycle_generation,
            active_run_id,
            created_at,
            expires_at,
        })
    }

    pub fn attempt_id(&self) -> &CollaborationAttemptId {
        &self.attempt_id
    }
    pub fn task_id(&self) -> i64 {
        self.task_id
    }
    pub fn agent(&self) -> &CollaborationAgent {
        &self.agent
    }
    pub fn role(&self) -> CollaborationRole {
        self.role
    }
    pub fn pr_number(&self) -> i64 {
        self.pr_number
    }
    pub fn reviewer_head_sha(&self) -> Option<&ReviewerHeadSha> {
        self.reviewer_head_sha.as_ref()
    }
    pub fn lifecycle_generation(&self) -> i64 {
        self.lifecycle_generation
    }
    pub fn active_run_id(&self) -> &RunCapabilityId {
        &self.active_run_id
    }
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
    pub fn expires_at(&self) -> i64 {
        self.expires_at
    }
}

/// One immutable prepared `github_agent_operations` enqueue request. All target
/// fields repeat the attempt binding so future storage can reject cross-row
/// disagreement in its one admission transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnqueueGithubOperationRequest {
    operation_id: GithubOperationId,
    client_request_id: ClientRequestId,
    attempt_id: CollaborationAttemptId,
    created_by_run_id: RunCapabilityId,
    task_id: i64,
    agent: CollaborationAgent,
    role: CollaborationRole,
    pr_number: i64,
    reviewer_head_sha: Option<ReviewerHeadSha>,
    kind: GithubOperationKind,
    request_json: GithubOperationRequestJson,
    github_marker: Option<GithubMarker>,
    deadline_at: i64,
    created_at: i64,
    expires_at: i64,
}

impl EnqueueGithubOperationRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation_id: GithubOperationId,
        client_request_id: ClientRequestId,
        attempt_id: CollaborationAttemptId,
        created_by_run_id: RunCapabilityId,
        task_id: i64,
        agent: CollaborationAgent,
        role: CollaborationRole,
        pr_number: i64,
        reviewer_head_sha: Option<ReviewerHeadSha>,
        kind: GithubOperationKind,
        request_json: GithubOperationRequestJson,
        github_marker: Option<GithubMarker>,
        deadline_at: i64,
        created_at: i64,
        expires_at: i64,
    ) -> Result<Self> {
        validate_collaboration_context(
            task_id,
            role,
            pr_number,
            reviewer_head_sha.as_ref(),
            created_at,
            expires_at,
        )?;
        if deadline_at < created_at || deadline_at > expires_at {
            return Err(QuorumError::Usage(
                "GitHub operation deadline is outside its retention interval".into(),
            ));
        }
        let marker_matches_kind = match kind {
            GithubOperationKind::PullRequestRead => github_marker.is_none(),
            GithubOperationKind::AddIssueComment
            | GithubOperationKind::PullRequestReviewWrite
            | GithubOperationKind::AddCommentToPendingReview
            | GithubOperationKind::AddReplyToPullRequestComment
            | GithubOperationKind::ResolveReviewThread => github_marker.is_some(),
        };
        if !marker_matches_kind {
            return Err(QuorumError::Usage(
                "GitHub operation marker does not match operation kind".into(),
            ));
        }
        Ok(Self {
            operation_id,
            client_request_id,
            attempt_id,
            created_by_run_id,
            task_id,
            agent,
            role,
            pr_number,
            reviewer_head_sha,
            kind,
            request_json,
            github_marker,
            deadline_at,
            created_at,
            expires_at,
        })
    }

    pub fn operation_id(&self) -> &GithubOperationId {
        &self.operation_id
    }
    pub fn client_request_id(&self) -> &ClientRequestId {
        &self.client_request_id
    }
    pub fn attempt_id(&self) -> &CollaborationAttemptId {
        &self.attempt_id
    }
    pub fn created_by_run_id(&self) -> &RunCapabilityId {
        &self.created_by_run_id
    }
    pub fn task_id(&self) -> i64 {
        self.task_id
    }
    pub fn agent(&self) -> &CollaborationAgent {
        &self.agent
    }
    pub fn role(&self) -> CollaborationRole {
        self.role
    }
    pub fn pr_number(&self) -> i64 {
        self.pr_number
    }
    pub fn reviewer_head_sha(&self) -> Option<&ReviewerHeadSha> {
        self.reviewer_head_sha.as_ref()
    }
    pub fn kind(&self) -> GithubOperationKind {
        self.kind
    }
    pub fn request_json(&self) -> &GithubOperationRequestJson {
        &self.request_json
    }
    pub fn github_marker(&self) -> Option<&GithubMarker> {
        self.github_marker.as_ref()
    }
    pub fn deadline_at(&self) -> i64 {
        self.deadline_at
    }
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
    pub fn expires_at(&self) -> i64 {
        self.expires_at
    }
}

fn validate_collaboration_context(
    task_id: i64,
    role: CollaborationRole,
    pr_number: i64,
    reviewer_head_sha: Option<&ReviewerHeadSha>,
    created_at: i64,
    expires_at: i64,
) -> Result<()> {
    if task_id <= 0 || pr_number <= 0 {
        return Err(QuorumError::Usage(
            "collaboration attempt identifiers are invalid".into(),
        ));
    }
    if created_at < 0 || expires_at <= created_at {
        return Err(QuorumError::Usage(
            "collaboration attempt timestamps are inconsistent".into(),
        ));
    }
    match (role, reviewer_head_sha) {
        (CollaborationRole::Reviewer, Some(_)) | (CollaborationRole::Worker, None) => Ok(()),
        _ => Err(QuorumError::Usage(
            "collaboration role and reviewer SHA are inconsistent".into(),
        )),
    }
}

/// A single endpoint-shaped request. This is intentionally closed: no generic
/// SQL, filesystem, transport, or arbitrary GitHub method is expressible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CollaborationAdmissionRequest {
    CreateAttempt(CreateCollaborationAttemptRequest),
    EnqueueGithubOperation(EnqueueGithubOperationRequest),
}

/// Persisted attempt status exposed by a future admission/read boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CollaborationAttemptStatus {
    pub attempt_id: CollaborationAttemptId,
    pub state: CollaborationAttemptState,
    pub lifecycle_generation: i64,
    pub expires_at: i64,
}

/// Persisted operation status exposed by a future admission/read boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GithubOperationStatus {
    pub operation_id: GithubOperationId,
    pub state: GithubOperationState,
    pub send_state: GithubSendState,
    pub remote_object_id: Option<RemoteObjectId>,
    pub response_json: Option<GithubOperationResponseJson>,
    pub error_summary: Option<OperationErrorSummary>,
}

/// Closed idempotent outcomes for attempt admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationAttemptAdmissionResult {
    Created(CollaborationAttemptStatus),
    Existing(CollaborationAttemptStatus),
    Rejected(CollaborationAdmissionError),
}

/// Closed result of an exact-continuation handoff or terminal attempt change.
/// Rejections are expected authority/race outcomes; they intentionally do not
/// cross the error boundary and therefore cannot produce an `errors` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationAttemptTransitionResult {
    Transitioned(CollaborationAttemptStatus),
    Existing(CollaborationAttemptStatus),
    Rejected(CollaborationAdmissionError),
}

/// Closed idempotent outcomes for operation admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GithubOperationAdmissionResult {
    Enqueued(GithubOperationStatus),
    Existing(GithubOperationStatus),
}

/// One closed result envelope for either collaboration admission operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CollaborationAdmissionResult {
    Attempt(CollaborationAttemptAdmissionResult),
    Operation(GithubOperationAdmissionResult),
}

#[derive(Debug)]
struct StoredAttempt {
    task_id: i64,
    agent: String,
    role: String,
    pr_number: i64,
    head_sha: Option<String>,
    lifecycle_generation: i64,
    turn_provider: Option<String>,
    turn_continuation_id: Option<String>,
    pending_turn_json: Option<String>,
    active_run_id: Option<String>,
    state: CollaborationAttemptState,
    expires_at: i64,
}

fn stored_attempt_status(
    attempt_id: &str,
    state: &str,
    lifecycle_generation: i64,
    expires_at: i64,
) -> rusqlite::Result<CollaborationAttemptStatus> {
    let state = state.parse().map_err(|_| {
        rusqlite::Error::InvalidColumnType(1, "state".into(), rusqlite::types::Type::Text)
    })?;
    let attempt_id = CollaborationAttemptId::new(attempt_id).map_err(|_| {
        rusqlite::Error::InvalidColumnType(0, "attempt_id".into(), rusqlite::types::Type::Text)
    })?;
    Ok(CollaborationAttemptStatus {
        attempt_id,
        state,
        lifecycle_generation,
        expires_at,
    })
}

fn row_to_status(row: &rusqlite::Row<'_>) -> rusqlite::Result<CollaborationAttemptStatus> {
    stored_attempt_status(
        &row.get::<_, String>(0)?,
        &row.get::<_, String>(1)?,
        row.get(2)?,
        row.get(3)?,
    )
}

fn get_attempt(
    conn: &Connection,
    attempt_id: &CollaborationAttemptId,
) -> Result<Option<StoredAttempt>> {
    conn.query_row(
        "SELECT task_id,agent,role,pr_number,head_sha,lifecycle_generation,turn_provider,
                turn_continuation_id,pending_turn_json,active_run_id,state,expires_at
         FROM github_collaboration_attempts WHERE attempt_id=?1",
        [attempt_id.as_str()],
        |row| {
            let state: String = row.get(10)?;
            let state = state.parse().map_err(|_| {
                rusqlite::Error::InvalidColumnType(10, "state".into(), rusqlite::types::Type::Text)
            })?;
            Ok(StoredAttempt {
                task_id: row.get(0)?,
                agent: row.get(1)?,
                role: row.get(2)?,
                pr_number: row.get(3)?,
                head_sha: row.get(4)?,
                lifecycle_generation: row.get(5)?,
                turn_provider: row.get(6)?,
                turn_continuation_id: row.get(7)?,
                pending_turn_json: row.get(8)?,
                active_run_id: row.get(9)?,
                state,
                expires_at: row.get(11)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn request_matches_attempt(
    request: &CreateCollaborationAttemptRequest,
    attempt: &StoredAttempt,
) -> bool {
    attempt.task_id == request.task_id()
        && attempt.agent == request.agent().as_str()
        && attempt.role == request.role().as_str()
        && attempt.pr_number == request.pr_number()
        && attempt.head_sha.as_deref() == request.reviewer_head_sha().map(ReviewerHeadSha::as_str)
        && attempt.lifecycle_generation == request.lifecycle_generation()
}

enum AttemptAuthority {
    Authorized(Box<LiveCollaborationContext>),
    Rejected(CollaborationAdmissionError),
}

/// Revalidate request-shaped context against daemon-owned authority. The
/// lifecycle generation and provider-turn identity are derived from durable
/// daemon state, never accepted from the request.
fn resolve_attempt_authority(
    conn: &Connection,
    request: &CreateCollaborationAttemptRequest,
) -> Result<AttemptAuthority> {
    let context = match capabilities::resolve_live_collaboration_context_for_admission(
        conn,
        request.active_run_id().as_str(),
        request.role().as_str(),
    )? {
        LiveCollaborationContextResolution::Live(context) => *context,
        LiveCollaborationContextResolution::Rejected => {
            return Ok(AttemptAuthority::Rejected(
                CollaborationAdmissionError::CapabilityRejected,
            ));
        }
    };
    let run = &context.run;
    Ok(
        if run.task_id != request.task_id()
            || run.agent != request.agent().as_str()
            || run.role != request.role().as_str()
            || run.pr != Some(request.pr_number())
            || run.review_revision.as_deref()
                != request.reviewer_head_sha().map(ReviewerHeadSha::as_str)
            || context.lifecycle_generation != request.lifecycle_generation()
        {
            AttemptAuthority::Rejected(CollaborationAdmissionError::AttemptBindingMismatch)
        } else {
            AttemptAuthority::Authorized(Box::new(context))
        },
    )
}

fn attempt_matches_turn(attempt: &StoredAttempt, authority: &LiveCollaborationContext) -> bool {
    attempt.turn_provider.as_deref() == Some(authority.turn_provider.as_str())
        && attempt.turn_continuation_id.as_deref() == authority.turn_continuation_id.as_deref()
        && attempt.pending_turn_json.as_deref() == authority.pending_turn_json.as_deref()
}

fn logical_attempt_exists(
    conn: &Connection,
    request: &CreateCollaborationAttemptRequest,
    now: i64,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM github_collaboration_attempts
             WHERE task_id=?1 AND agent=?2 AND role=?3 AND pr_number=?4
               AND head_sha IS ?5 AND lifecycle_generation=?6
               AND state IN ('active','awaiting_resume')
               AND expires_at>?7
         )",
        params![
            request.task_id(),
            request.agent().as_str(),
            request.role().as_str(),
            request.pr_number(),
            request.reviewer_head_sha().map(ReviewerHeadSha::as_str),
            request.lifecycle_generation(),
            now,
        ],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

/// Physical release of a logically dead unique run binding. Retention rows
/// remain for audit; clearing this pointer only prevents SQLite's unconditional
/// `UNIQUE(active_run_id)` from overriding logical expiry on a new issuance.
fn release_expired_run_binding(
    conn: &Connection,
    run_id: &RunCapabilityId,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE github_collaboration_attempts SET active_run_id=NULL
         WHERE active_run_id=?1 AND expires_at<=?2",
        params![run_id.as_str(), now],
    )?;
    Ok(())
}

fn capacity_rejection(
    conn: &Connection,
    task_id: i64,
    now: i64,
) -> Result<Option<CollaborationAdmissionError>> {
    let task_attempts: i64 = conn.query_row(
        "SELECT count(*) FROM github_collaboration_attempts
         WHERE task_id=?1 AND expires_at>?2",
        params![task_id, now],
        |row| row.get(0),
    )?;
    if task_attempts >= MAX_NONEXPIRED_ATTEMPTS_PER_TASK {
        return Ok(Some(CollaborationAdmissionError::AttemptLimitReached));
    }
    let repository_attempts: i64 = conn.query_row(
        "SELECT count(*) FROM github_collaboration_attempts WHERE expires_at>?1",
        [now],
        |row| row.get(0),
    )?;
    Ok(
        (repository_attempts >= MAX_NONEXPIRED_ATTEMPTS_PER_REPOSITORY)
            .then_some(CollaborationAdmissionError::RepositoryAttemptLimitReached),
    )
}

/// Issue the one active collaboration attempt for a daemon-created logical
/// turn. The supplied binding is only an assertion: task, agent, role, PR,
/// reviewer SHA, lifecycle generation, and provider-turn identity are derived
/// again from daemon-owned state inside the same `BEGIN IMMEDIATE` transaction
/// as the insert. `now` is the daemon's authoritative admission time.
pub fn issue_attempt(
    conn: &mut Connection,
    request: &CreateCollaborationAttemptRequest,
    now: i64,
) -> Result<CollaborationAttemptAdmissionResult> {
    if now < 0 {
        return Err(QuorumError::Usage(
            "collaboration admission timestamp is invalid".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let authority = match resolve_attempt_authority(&tx, request)? {
        AttemptAuthority::Authorized(authority) => *authority,
        AttemptAuthority::Rejected(rejection) => {
            tx.commit()?;
            return Ok(CollaborationAttemptAdmissionResult::Rejected(rejection));
        }
    };
    if request.expires_at() <= now {
        tx.commit()?;
        return Ok(CollaborationAttemptAdmissionResult::Rejected(
            CollaborationAdmissionError::AttemptExpired,
        ));
    }

    if let Some(existing) = get_attempt(&tx, request.attempt_id())? {
        let result = if existing.expires_at <= now {
            CollaborationAttemptAdmissionResult::Rejected(
                CollaborationAdmissionError::AttemptExpired,
            )
        } else if request_matches_attempt(request, &existing)
            && existing.state == CollaborationAttemptState::Active
            && existing.active_run_id.as_deref() == Some(request.active_run_id().as_str())
            && attempt_matches_turn(&existing, &authority)
        {
            CollaborationAttemptAdmissionResult::Existing(CollaborationAttemptStatus {
                attempt_id: request.attempt_id().clone(),
                state: existing.state,
                lifecycle_generation: existing.lifecycle_generation,
                expires_at: existing.expires_at,
            })
        } else {
            CollaborationAttemptAdmissionResult::Rejected(
                CollaborationAdmissionError::AttemptBindingMismatch,
            )
        };
        tx.commit()?;
        return Ok(result);
    }

    if logical_attempt_exists(&tx, request, now)? {
        tx.commit()?;
        return Ok(CollaborationAttemptAdmissionResult::Rejected(
            CollaborationAdmissionError::ActiveAttemptExists,
        ));
    }
    release_expired_run_binding(&tx, request.active_run_id(), now)?;
    let run_already_bound: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM github_collaboration_attempts
             WHERE active_run_id=?1 AND expires_at>?2
         )",
        params![request.active_run_id().as_str(), now],
        |row| row.get(0),
    )?;
    if run_already_bound {
        tx.commit()?;
        return Ok(CollaborationAttemptAdmissionResult::Rejected(
            CollaborationAdmissionError::ActiveAttemptExists,
        ));
    }
    if let Some(rejection) = capacity_rejection(&tx, request.task_id(), now)? {
        tx.commit()?;
        return Ok(CollaborationAttemptAdmissionResult::Rejected(rejection));
    }

    let created = tx.query_row(
        "INSERT INTO github_collaboration_attempts(
             attempt_id,task_id,agent,role,pr_number,head_sha,lifecycle_generation,
             turn_provider,turn_continuation_id,pending_turn_json,active_run_id,state,
             created_at,updated_at,expires_at
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'active',?12,?12,?13)
         RETURNING attempt_id,state,lifecycle_generation,expires_at",
        params![
            request.attempt_id().as_str(),
            request.task_id(),
            request.agent().as_str(),
            request.role().as_str(),
            request.pr_number(),
            request.reviewer_head_sha().map(ReviewerHeadSha::as_str),
            request.lifecycle_generation(),
            authority.turn_provider,
            authority.turn_continuation_id,
            authority.pending_turn_json,
            request.active_run_id().as_str(),
            now,
            request.expires_at(),
        ],
        row_to_status,
    )?;
    tx.commit()?;
    Ok(CollaborationAttemptAdmissionResult::Created(created))
}

/// Adopt an interrupted attempt only for a freshly issued live capability that
/// carries the same complete logical-turn binding. This never inserts a second
/// row: the guarded update moves `awaiting_resume` back to `active`.
pub fn adopt_exact_continuation(
    conn: &mut Connection,
    request: &CreateCollaborationAttemptRequest,
    now: i64,
) -> Result<CollaborationAttemptTransitionResult> {
    if now < 0 {
        return Err(QuorumError::Usage(
            "collaboration transition timestamp is invalid".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let authority = match resolve_attempt_authority(&tx, request)? {
        AttemptAuthority::Authorized(authority) => *authority,
        AttemptAuthority::Rejected(rejection) => {
            tx.commit()?;
            return Ok(CollaborationAttemptTransitionResult::Rejected(rejection));
        }
    };
    let Some(turn_continuation_id) = authority.turn_continuation_id.as_deref() else {
        tx.commit()?;
        return Ok(CollaborationAttemptTransitionResult::Rejected(
            CollaborationAdmissionError::ContinuationMismatch,
        ));
    };
    release_expired_run_binding(&tx, request.active_run_id(), now)?;
    let run_already_bound: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM github_collaboration_attempts
             WHERE active_run_id=?1 AND attempt_id<>?2 AND expires_at>?3
         )",
        params![
            request.active_run_id().as_str(),
            request.attempt_id().as_str(),
            now,
        ],
        |row| row.get(0),
    )?;
    if run_already_bound {
        tx.commit()?;
        return Ok(CollaborationAttemptTransitionResult::Rejected(
            CollaborationAdmissionError::ActiveAttemptExists,
        ));
    }

    let adopted = tx
        .query_row(
            "UPDATE github_collaboration_attempts
             SET active_run_id=?2,state='active',updated_at=?12
             WHERE attempt_id=?1 AND state='awaiting_resume' AND active_run_id IS NULL
               AND task_id=?3 AND agent=?4 AND role=?5 AND pr_number=?6
               AND head_sha IS ?7 AND lifecycle_generation=?8
               AND turn_provider=?9 AND turn_continuation_id=?10
               AND pending_turn_json IS ?11 AND expires_at>?12
             RETURNING attempt_id,state,lifecycle_generation,expires_at",
            params![
                request.attempt_id().as_str(),
                request.active_run_id().as_str(),
                request.task_id(),
                request.agent().as_str(),
                request.role().as_str(),
                request.pr_number(),
                request.reviewer_head_sha().map(ReviewerHeadSha::as_str),
                request.lifecycle_generation(),
                authority.turn_provider,
                turn_continuation_id,
                authority.pending_turn_json,
                now,
            ],
            row_to_status,
        )
        .optional()?;
    if let Some(adopted) = adopted {
        tx.commit()?;
        return Ok(CollaborationAttemptTransitionResult::Transitioned(adopted));
    }

    let rejection = match get_attempt(&tx, request.attempt_id())? {
        Some(existing) if existing.expires_at <= now => CollaborationAdmissionError::AttemptExpired,
        Some(existing)
            if request_matches_attempt(request, &existing)
                && existing.state == CollaborationAttemptState::Active
                && existing.active_run_id.as_deref() == Some(request.active_run_id().as_str())
                && attempt_matches_turn(&existing, &authority) =>
        {
            let status = CollaborationAttemptStatus {
                attempt_id: request.attempt_id().clone(),
                state: existing.state,
                lifecycle_generation: existing.lifecycle_generation,
                expires_at: existing.expires_at,
            };
            tx.commit()?;
            return Ok(CollaborationAttemptTransitionResult::Existing(status));
        }
        Some(_) => CollaborationAdmissionError::ContinuationMismatch,
        None => CollaborationAdmissionError::AttemptNotFound,
    };
    tx.commit()?;
    Ok(CollaborationAttemptTransitionResult::Rejected(rejection))
}

fn transition_active_attempt(
    conn: &mut Connection,
    request: &CreateCollaborationAttemptRequest,
    target: CollaborationAttemptState,
    now: i64,
) -> Result<CollaborationAttemptTransitionResult> {
    debug_assert!(matches!(
        target,
        CollaborationAttemptState::AwaitingResume
            | CollaborationAttemptState::Completed
            | CollaborationAttemptState::Revoked
    ));
    if now < 0 {
        return Err(QuorumError::Usage(
            "collaboration transition timestamp is invalid".into(),
        ));
    }
    let tx = begin_immediate(conn)?;
    let authority = match resolve_attempt_authority(&tx, request)? {
        AttemptAuthority::Authorized(authority) => *authority,
        AttemptAuthority::Rejected(rejection) => {
            tx.commit()?;
            return Ok(CollaborationAttemptTransitionResult::Rejected(rejection));
        }
    };
    if target == CollaborationAttemptState::AwaitingResume
        && authority.turn_continuation_id.is_none()
    {
        tx.commit()?;
        return Ok(CollaborationAttemptTransitionResult::Rejected(
            CollaborationAdmissionError::ContinuationMismatch,
        ));
    }
    let transitioned = tx
        .query_row(
            "UPDATE github_collaboration_attempts
             SET active_run_id=NULL,state=?9,turn_provider=?10,
                 turn_continuation_id=?11,pending_turn_json=?12,updated_at=?13
             WHERE attempt_id=?1 AND active_run_id=?2 AND state='active'
               AND task_id=?3 AND agent=?4 AND role=?5 AND pr_number=?6
               AND head_sha IS ?7 AND lifecycle_generation=?8 AND expires_at>?13
             RETURNING attempt_id,state,lifecycle_generation,expires_at",
            params![
                request.attempt_id().as_str(),
                request.active_run_id().as_str(),
                request.task_id(),
                request.agent().as_str(),
                request.role().as_str(),
                request.pr_number(),
                request.reviewer_head_sha().map(ReviewerHeadSha::as_str),
                request.lifecycle_generation(),
                target.as_str(),
                authority.turn_provider,
                authority.turn_continuation_id,
                authority.pending_turn_json,
                now,
            ],
            row_to_status,
        )
        .optional()?;
    if let Some(transitioned) = transitioned {
        tx.commit()?;
        return Ok(CollaborationAttemptTransitionResult::Transitioned(
            transitioned,
        ));
    }
    let rejection = match get_attempt(&tx, request.attempt_id())? {
        Some(existing) if existing.expires_at <= now => CollaborationAdmissionError::AttemptExpired,
        Some(existing) if !request_matches_attempt(request, &existing) => {
            CollaborationAdmissionError::AttemptBindingMismatch
        }
        Some(_) => CollaborationAdmissionError::AttemptNotActive,
        None => CollaborationAdmissionError::AttemptNotFound,
    };
    tx.commit()?;
    Ok(CollaborationAttemptTransitionResult::Rejected(rejection))
}

/// Suspend a live logical turn after interruption. The active capability is
/// detached in the same guarded transaction, so no mutation can race into a
/// resumable attempt after this returns.
pub fn mark_awaiting_resume(
    conn: &mut Connection,
    request: &CreateCollaborationAttemptRequest,
    now: i64,
) -> Result<CollaborationAttemptTransitionResult> {
    transition_active_attempt(
        conn,
        request,
        CollaborationAttemptState::AwaitingResume,
        now,
    )
}

/// Complete an admitted logical turn. Lifecycle callers may compose their own
/// task transition later; this API deliberately changes only the attempt.
pub fn complete_attempt(
    conn: &mut Connection,
    request: &CreateCollaborationAttemptRequest,
    now: i64,
) -> Result<CollaborationAttemptTransitionResult> {
    transition_active_attempt(conn, request, CollaborationAttemptState::Completed, now)
}

/// Revoke the live attempt when its authority is no longer usable. A loser of
/// a concurrent completion/revocation gets a typed clean negative and cannot
/// cause a task lifecycle side effect.
pub fn revoke_attempt(
    conn: &mut Connection,
    request: &CreateCollaborationAttemptRequest,
    now: i64,
) -> Result<CollaborationAttemptTransitionResult> {
    transition_active_attempt(conn, request, CollaborationAttemptState::Revoked, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    const WORKER_PR: i64 = 42;
    const REVIEW_SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, conn)
    }

    fn insert_worker_task(conn: &Connection, task_id: i64, agent: &str, pr: i64) {
        conn.execute(
            "INSERT INTO tasks(
                 id,title,status,assignee,author,created_by,created_at,updated_at,refs
             ) VALUES (?1,'collaboration test','rework',?2,?2,'owner',1,1,?3)",
            params![
                task_id,
                agent,
                serde_json::json!({
                    "pr": pr,
                    "runner_retry": {
                        "provider": "codex",
                        "model": "model",
                        "effort": "high",
                        "prompt": "resume exact collaboration turn",
                        "turn_kind": "rework",
                        "continuation_id": "turn-exact",
                        "requested": true
                    }
                })
                .to_string()
            ],
        )
        .unwrap();
    }

    fn insert_live_worker(
        conn: &mut Connection,
        task_id: i64,
        agent: &str,
        run_id: &str,
        now: i64,
    ) {
        crate::capabilities::issue(conn, run_id, task_id, agent, "worker", now).unwrap();
        crate::agent_runs::insert(
            conn, task_id, agent, "worker", "model", "high", "codex", now,
        )
        .unwrap();
    }

    fn worker_request(
        attempt_id: &str,
        run_id: &str,
        task_id: i64,
        agent: &str,
        generation: i64,
    ) -> CreateCollaborationAttemptRequest {
        CreateCollaborationAttemptRequest::new(
            CollaborationAttemptId::new(attempt_id).unwrap(),
            task_id,
            CollaborationAgent::new(agent).unwrap(),
            CollaborationRole::Worker,
            WORKER_PR,
            None,
            generation,
            RunCapabilityId::new(run_id).unwrap(),
            20,
            1_000,
        )
        .unwrap()
    }

    fn reviewer_request(
        attempt_id: &str,
        run_id: &str,
        reviewer_sha: &str,
    ) -> CreateCollaborationAttemptRequest {
        CreateCollaborationAttemptRequest::new(
            CollaborationAttemptId::new(attempt_id).unwrap(),
            3,
            CollaborationAgent::new("Reviewer").unwrap(),
            CollaborationRole::Reviewer,
            WORKER_PR,
            Some(ReviewerHeadSha::new(reviewer_sha).unwrap()),
            0,
            RunCapabilityId::new(run_id).unwrap(),
            20,
            1_000,
        )
        .unwrap()
    }

    fn insert_live_reviewer(conn: &mut Connection, run_id: &str) {
        conn.execute(
            "INSERT INTO tasks(
                 id,title,status,assignee,reviewer,created_by,created_at,updated_at,refs
             ) VALUES (3,'review test','in-review','Reviewer','Reviewer','owner',1,1,?1)",
            [serde_json::json!({"pr": WORKER_PR}).to_string()],
        )
        .unwrap();
        crate::capabilities::issue(conn, run_id, 3, "Reviewer", "reviewer", 10).unwrap();
        crate::agent_runs::insert_reviewer_with_launch(
            conn, 3, "Reviewer", "model", "high", "codex", None, 10, None, run_id, WORKER_PR,
            REVIEW_SHA,
        )
        .unwrap()
        .unwrap();
    }

    #[test]
    fn operation_request_json_is_canonical_bounded_and_nul_safe() {
        let json = GithubOperationRequestJson::new(r#"{ "body": "hello", "n": 1 }"#).unwrap();
        assert_eq!(json.as_str(), r#"{"body":"hello","n":1}"#);
        assert!(GithubOperationRequestJson::new("[]").is_err());
        assert!(GithubOperationRequestJson::new("{\"body\":\"\\u0000\"}").is_err());
        assert!(GithubOperationRequestJson::new(&format!(
            "{{\"body\":\"{}\"}}",
            "x".repeat(MAX_OPERATION_JSON_BYTES)
        ))
        .is_err());
    }

    #[test]
    fn reviewer_context_and_mutation_marker_are_required() {
        let attempt_id = CollaborationAttemptId::new("attempt").unwrap();
        let agent = CollaborationAgent::new("Reviewer").unwrap();
        let run = RunCapabilityId::new("run").unwrap();
        assert!(CreateCollaborationAttemptRequest::new(
            attempt_id.clone(),
            1,
            agent.clone(),
            CollaborationRole::Reviewer,
            42,
            None,
            0,
            run.clone(),
            1,
            2,
        )
        .is_err());

        let request_json = GithubOperationRequestJson::new(r#"{"body":"hello"}"#).unwrap();
        assert!(EnqueueGithubOperationRequest::new(
            GithubOperationId::new("operation").unwrap(),
            ClientRequestId::new("request").unwrap(),
            attempt_id,
            run,
            1,
            agent,
            CollaborationRole::Worker,
            42,
            None,
            GithubOperationKind::AddIssueComment,
            request_json,
            None,
            2,
            1,
            3,
        )
        .is_err());
    }

    #[test]
    fn closed_vocabulary_rejects_unknown_values() {
        assert_eq!(
            "awaiting_resume"
                .parse::<CollaborationAttemptState>()
                .unwrap(),
            CollaborationAttemptState::AwaitingResume
        );
        assert!("unknown".parse::<GithubOperationKind>().is_err());
        assert_eq!(MAX_OPERATION_ATTEMPTS, 8);
    }

    #[test]
    fn exact_continuation_adoption_reuses_one_attempt_after_fresh_capability() {
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", "run-original", 10);
        let original = worker_request("attempt-1", "run-original", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &original, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Created(_)
        ));
        assert!(matches!(
            mark_awaiting_resume(&mut conn, &original, 21).unwrap(),
            CollaborationAttemptTransitionResult::Transitioned(status)
                if status.state == CollaborationAttemptState::AwaitingResume
        ));
        crate::capabilities::revoke(&mut conn, "run-original", 22).unwrap();
        conn.execute(
            "UPDATE agent_runs SET ended_at=22 WHERE task_id=1 AND agent_name='Worker'",
            [],
        )
        .unwrap();
        insert_live_worker(&mut conn, 1, "Worker", "run-resume", 30);
        let resumed = worker_request("attempt-1", "run-resume", 1, "Worker", 0);

        let adopted = adopt_exact_continuation(&mut conn, &resumed, 31).unwrap();
        assert!(matches!(
            adopted,
            CollaborationAttemptTransitionResult::Transitioned(status)
                if status.attempt_id.as_str() == "attempt-1"
                    && status.state == CollaborationAttemptState::Active
        ));
        assert!(matches!(
            adopt_exact_continuation(&mut conn, &resumed, 32).unwrap(),
            CollaborationAttemptTransitionResult::Existing(status)
                if status.attempt_id.as_str() == "attempt-1"
        ));
        let rows: i64 = conn
            .query_row(
                "SELECT count(*) FROM github_collaboration_attempts WHERE task_id=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1, "exact continuation must not mint a second attempt");
    }

    #[test]
    fn exact_continuation_rejects_changed_lifecycle_generation() {
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", "run-original", 10);
        let original = worker_request("attempt-generation", "run-original", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &original, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Created(_)
        ));
        assert!(matches!(
            mark_awaiting_resume(&mut conn, &original, 21).unwrap(),
            CollaborationAttemptTransitionResult::Transitioned(_)
        ));
        crate::capabilities::revoke(&mut conn, "run-original", 22).unwrap();
        conn.execute(
            "UPDATE agent_runs SET ended_at=22 WHERE task_id=1 AND agent_name='Worker'",
            [],
        )
        .unwrap();
        conn.execute("UPDATE tasks SET rework_round=1 WHERE id=1", [])
            .unwrap();
        insert_live_worker(&mut conn, 1, "Worker", "run-new-generation", 30);
        let resumed = worker_request("attempt-generation", "run-new-generation", 1, "Worker", 0);

        assert!(matches!(
            adopt_exact_continuation(&mut conn, &resumed, 31).unwrap(),
            CollaborationAttemptTransitionResult::Rejected(
                CollaborationAdmissionError::AttemptBindingMismatch
            )
        ));
    }

    #[test]
    fn exact_continuation_rejects_changed_daemon_pending_turn() {
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", "run-original", 10);
        let original = worker_request("attempt-turn", "run-original", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &original, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Created(_)
        ));
        assert!(matches!(
            mark_awaiting_resume(&mut conn, &original, 21).unwrap(),
            CollaborationAttemptTransitionResult::Transitioned(_)
        ));
        crate::capabilities::revoke(&mut conn, "run-original", 22).unwrap();
        conn.execute(
            "UPDATE agent_runs SET ended_at=22 WHERE task_id=1 AND agent_name='Worker'",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET refs=?1 WHERE id=1",
            [serde_json::json!({
                "pr": WORKER_PR,
                "runner_retry": {
                    "provider": "codex",
                    "model": "model",
                    "effort": "high",
                    "prompt": "a different daemon-owned turn",
                    "turn_kind": "rework",
                    "continuation_id": "turn-other",
                    "requested": true
                }
            })
            .to_string()],
        )
        .unwrap();
        insert_live_worker(&mut conn, 1, "Worker", "run-other-turn", 30);
        let resumed = worker_request("attempt-turn", "run-other-turn", 1, "Worker", 0);

        assert!(matches!(
            adopt_exact_continuation(&mut conn, &resumed, 31).unwrap(),
            CollaborationAttemptTransitionResult::Rejected(
                CollaborationAdmissionError::ContinuationMismatch
            )
        ));
    }

    #[test]
    fn issuance_ignores_expired_resumable_attempt_and_releases_expired_run_binding() {
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", "run-live", 10);
        let expired = worker_request("attempt-expired", "run-live", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &expired, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Created(_)
        ));
        conn.execute(
            "UPDATE github_collaboration_attempts SET expires_at=20 WHERE attempt_id='attempt-expired'",
            [],
        )
        .unwrap();
        let fresh = worker_request("attempt-fresh", "run-live", 1, "Worker", 0);

        assert!(matches!(
            issue_attempt(&mut conn, &fresh, 21).unwrap(),
            CollaborationAttemptAdmissionResult::Created(_)
        ));
        let expired_run: Option<String> = conn
            .query_row(
                "SELECT active_run_id FROM github_collaboration_attempts WHERE attempt_id='attempt-expired'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            expired_run, None,
            "expired run binding must not block re-issuance"
        );
    }

    #[test]
    fn issuance_rejects_foreign_stale_revoked_and_mismatched_pr_capabilities() {
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", "run-live", 10);

        insert_worker_task(&conn, 2, "Other", WORKER_PR);
        insert_live_worker(&mut conn, 2, "Other", "run-foreign", 10);
        let foreign = worker_request("foreign", "run-foreign", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &foreign, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Rejected(
                CollaborationAdmissionError::AttemptBindingMismatch
            )
        ));

        let wrong_pr = CreateCollaborationAttemptRequest::new(
            CollaborationAttemptId::new("wrong-pr").unwrap(),
            1,
            CollaborationAgent::new("Worker").unwrap(),
            CollaborationRole::Worker,
            WORKER_PR + 1,
            None,
            0,
            RunCapabilityId::new("run-live").unwrap(),
            20,
            1_000,
        )
        .unwrap();
        assert!(matches!(
            issue_attempt(&mut conn, &wrong_pr, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Rejected(
                CollaborationAdmissionError::AttemptBindingMismatch
            )
        ));

        conn.execute(
            "UPDATE agent_runs SET ended_at=11 WHERE task_id=1 AND agent_name='Worker'",
            [],
        )
        .unwrap();
        let stale = worker_request("stale", "run-live", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &stale, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Rejected(
                CollaborationAdmissionError::CapabilityRejected
            )
        ));

        crate::capabilities::revoke(&mut conn, "run-live", 12).unwrap();
        let revoked = worker_request("revoked", "run-live", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &revoked, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Rejected(
                CollaborationAdmissionError::CapabilityRejected
            )
        ));
        let errors: i64 = conn
            .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
            .unwrap();
        assert_eq!(errors, 0, "clean negatives must not write errors");
    }

    #[test]
    fn issuance_rejects_reviewer_launch_sha_mismatch_without_writing_an_attempt() {
        let (_dir, mut conn) = open_tmp();
        insert_live_reviewer(&mut conn, "review-run");
        let mismatched = reviewer_request(
            "review-attempt",
            "review-run",
            "ffffffffffffffffffffffffffffffffffffffff",
        );
        assert!(matches!(
            issue_attempt(&mut conn, &mismatched, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Rejected(
                CollaborationAdmissionError::AttemptBindingMismatch
            )
        ));
        let rows: i64 = conn
            .query_row(
                "SELECT count(*) FROM github_collaboration_attempts",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn completion_and_revocation_race_has_one_clean_winner_without_lifecycle_change() {
        use std::sync::{Arc, Barrier};

        let (dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", "race-run", 10);
        let request = worker_request("attempt-terminal", "race-run", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &request, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Created(_)
        ));
        let path = dir.path().join("q.db");
        drop(conn);

        let barrier = Arc::new(Barrier::new(2));
        let complete_path = path.clone();
        let complete_request = request.clone();
        let complete_barrier = Arc::clone(&barrier);
        let complete = std::thread::spawn(move || {
            let mut conn = crate::db::open(&complete_path).unwrap();
            complete_barrier.wait();
            complete_attempt(&mut conn, &complete_request, 30).unwrap()
        });
        let revoke_path = path.clone();
        let revoke_request = request.clone();
        let revoke = std::thread::spawn(move || {
            let mut conn = crate::db::open(&revoke_path).unwrap();
            barrier.wait();
            revoke_attempt(&mut conn, &revoke_request, 30).unwrap()
        });
        let results = [complete.join().unwrap(), revoke.join().unwrap()];
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    matches!(
                        result,
                        CollaborationAttemptTransitionResult::Transitioned(_)
                    )
                })
                .count(),
            1
        );
        assert!(results.iter().any(|result| {
            matches!(
                result,
                CollaborationAttemptTransitionResult::Rejected(
                    CollaborationAdmissionError::AttemptNotActive
                )
            )
        }));

        let conn = crate::db::open(&path).unwrap();
        let status: String = conn
            .query_row("SELECT status FROM tasks WHERE id=1", [], |row| row.get(0))
            .unwrap();
        let events: i64 = conn
            .query_row(
                "SELECT count(*) FROM events WHERE subject='task#1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let errors: i64 = conn
            .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
            .unwrap();
        assert_eq!(status, "rework");
        assert_eq!(events, 0);
        assert_eq!(errors, 0);
    }

    #[test]
    fn attempt_capacity_loss_is_a_clean_negative_without_lifecycle_change() {
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", "capacity-run", 10);
        for ordinal in 0..MAX_NONEXPIRED_ATTEMPTS_PER_TASK {
            conn.execute(
                "INSERT INTO github_collaboration_attempts(
                     attempt_id,task_id,agent,role,pr_number,lifecycle_generation,state,
                     created_at,updated_at,expires_at
                 ) VALUES (?1,1,'retained','worker',?2,0,'completed',1,1,1000)",
                params![format!("retained-{ordinal}"), 100 + ordinal],
            )
            .unwrap();
        }
        let request = worker_request("over-capacity", "capacity-run", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &request, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Rejected(
                CollaborationAdmissionError::AttemptLimitReached
            )
        ));
        let status: String = conn
            .query_row("SELECT status FROM tasks WHERE id=1", [], |row| row.get(0))
            .unwrap();
        let events: i64 = conn
            .query_row(
                "SELECT count(*) FROM events WHERE subject='task#1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let errors: i64 = conn
            .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
            .unwrap();
        assert_eq!(status, "rework");
        assert_eq!(events, 0);
        assert_eq!(errors, 0);
    }

    #[test]
    fn real_process_attempt_issuance_race_has_one_holder_and_clean_losers() {
        const CHILD_DB: &str = "QUORUM_COLLAB_ADMISSION_RACE_DB";
        const CHILD_ATTEMPT: &str = "QUORUM_COLLAB_ADMISSION_RACE_ATTEMPT";
        if let (Ok(path), Ok(attempt_id)) = (std::env::var(CHILD_DB), std::env::var(CHILD_ATTEMPT))
        {
            let mut conn = crate::db::open(std::path::Path::new(&path)).unwrap();
            let request = worker_request(&attempt_id, "race-run", 1, "Worker", 0);
            let result = issue_attempt(&mut conn, &request, 20).unwrap();
            assert!(matches!(
                result,
                CollaborationAttemptAdmissionResult::Created(_)
                    | CollaborationAttemptAdmissionResult::Rejected(
                        CollaborationAdmissionError::ActiveAttemptExists
                    )
            ));
            println!("{result:?}");
            return;
        }

        for round in 0..6 {
            let (dir, mut conn) = open_tmp();
            insert_worker_task(&conn, 1, "Worker", WORKER_PR);
            insert_live_worker(&mut conn, 1, "Worker", "race-run", 10);
            let db_path = dir.path().join("q.db");
            drop(conn);

            let test_name = "collaboration_admission::tests::real_process_attempt_issuance_race_has_one_holder_and_clean_losers";
            let mut children = Vec::new();
            for contender in 0..2 {
                children.push(
                    Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", test_name, "--nocapture"])
                        .env(CHILD_DB, &db_path)
                        .env(CHILD_ATTEMPT, format!("race-{round}-{contender}"))
                        .stdout(Stdio::piped())
                        .spawn()
                        .unwrap(),
                );
            }
            let outputs: Vec<_> = children
                .into_iter()
                .map(|child| child.wait_with_output().unwrap())
                .collect();
            assert!(outputs.iter().all(|output| output.status.success()));
            let created = outputs
                .iter()
                .filter(|output| String::from_utf8_lossy(&output.stdout).contains("Created("))
                .count();
            assert_eq!(created, 1, "round {round} must have one issuer");

            let conn = crate::db::open(&db_path).unwrap();
            let active: i64 = conn
                .query_row(
                    "SELECT count(*) FROM github_collaboration_attempts WHERE state='active'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let errors: i64 = conn
                .query_row("SELECT count(*) FROM errors", [], |row| row.get(0))
                .unwrap();
            assert_eq!(active, 1, "round {round} must retain one active holder");
            assert_eq!(errors, 0, "round {round} race loss must stay clean");
        }
    }
}
