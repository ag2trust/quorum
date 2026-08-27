//! Closed, bounded vocabulary and admission for canonical GitHub collaboration
//! storage.
//!
//! This module owns the capability-bound admission and lifecycle of canonical
//! `github_collaboration_attempts` and admission/reads for the durable
//! `github_agent_operations` outbox. GitHub operation *execution* stays
//! separate: attempts establish the durable, exact managed-turn identity, and
//! `admit_operation` / `read_operation` decide admission and expose bounded
//! closed status only. Nothing here executes a GitHub call or contacts the
//! network.

use crate::capabilities::{
    self, LiveCollaborationContext, LiveCollaborationContextResolution, LiveRunContext,
    LiveRunContextResolution,
};
use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
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

/// Fixed cap on retained GitHub operations created by one run capability.
pub const MAX_NONEXPIRED_OPERATIONS_PER_RUN: i64 = 64;
/// Fixed cap on retained GitHub operations per collaboration attempt.
pub const MAX_NONEXPIRED_OPERATIONS_PER_ATTEMPT: i64 = 128;
/// Fixed cap on retained GitHub operations per task.
pub const MAX_NONEXPIRED_OPERATIONS_PER_TASK: i64 = 512;
/// Fixed cap on retained GitHub operations in the repository database.
pub const MAX_NONEXPIRED_OPERATIONS_PER_REPOSITORY: i64 = 4_096;

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

/// Closed error outcomes for atomic attempt/operation admission and reads.
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
    OperationIdentityMismatch,
    OperationBindingMismatch,
    RunOperationLimitReached,
    AttemptOperationLimitReached,
    TaskOperationLimitReached,
    RepositoryOperationLimitReached,
    OperationDeadlineInvalid,
    OperationNotFound,
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
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GithubOperationAdmissionResult {
    Enqueued(GithubOperationStatus),
    Existing(GithubOperationStatus),
    Rejected(CollaborationAdmissionError),
}

/// Closed outcomes for an authorized operation read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GithubOperationReadResult {
    Found(GithubOperationStatus),
    NotFound,
    Rejected(CollaborationAdmissionError),
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
    let turn_continuation_id = authority.turn_continuation_id.as_deref();
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
               AND turn_provider=?9 AND turn_continuation_id IS ?10
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

/// FNV-1a 128-bit hash. Not cryptographic; the derived identity is scoped by
/// the attempt owner so pre-image resistance is not a requirement, only
/// deterministic collision-free identity across identical retries.
fn fnv1a_128(chunks: &[&[u8]]) -> u128 {
    let mut hash: u128 = 0x6c62272e_07bb0142_62b82175_6295c58d;
    const PRIME: u128 = 0x00000000_01000000_00000000_0000013b;
    for chunk in chunks {
        for &byte in chunk.iter() {
            hash ^= u128::from(byte);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    hash
}

/// Deterministically derive an operation id from the immutable identity fields
/// an admission request cannot mutate: the collaboration attempt and the
/// caller-scoped client request id. A repeated identical enqueue therefore
/// collides on the same `operation_id` and is deduplicated by the unique index
/// on `(attempt_id, client_request_id)`.
pub fn derive_operation_id(
    attempt_id: &CollaborationAttemptId,
    client_request_id: &ClientRequestId,
) -> GithubOperationId {
    let digest = fnv1a_128(&[
        b"quorum-op\x00",
        attempt_id.as_str().as_bytes(),
        b"\x00",
        client_request_id.as_str().as_bytes(),
    ]);
    GithubOperationId::new(format!("op-{digest:032x}"))
        .expect("derived operation id is bounded ASCII")
}

/// Deterministically derive the hidden GitHub marker attached to a mutation
/// body. Reads do not carry a marker; the closed operation vocabulary makes
/// the presence/absence coupling to `kind` an authoritative validation.
pub fn derive_github_marker(
    attempt_id: &CollaborationAttemptId,
    client_request_id: &ClientRequestId,
    kind: GithubOperationKind,
) -> Option<GithubMarker> {
    match kind {
        GithubOperationKind::PullRequestRead => None,
        GithubOperationKind::AddIssueComment
        | GithubOperationKind::PullRequestReviewWrite
        | GithubOperationKind::AddCommentToPendingReview
        | GithubOperationKind::AddReplyToPullRequestComment
        | GithubOperationKind::ResolveReviewThread => {
            let digest = fnv1a_128(&[
                b"quorum-marker\x00",
                kind.as_str().as_bytes(),
                b"\x00",
                attempt_id.as_str().as_bytes(),
                b"\x00",
                client_request_id.as_str().as_bytes(),
            ]);
            Some(
                GithubMarker::new(format!("<!-- quorum-op:{digest:032x} -->"))
                    .expect("derived marker is bounded ASCII"),
            )
        }
    }
}

/// The subset of a collaboration attempt loaded during operation admission.
/// Kept separate from the top-level `StoredAttempt` used by attempt lifecycle
/// so operation admission only reads what it needs.
#[derive(Debug, Clone)]
struct AttemptBinding {
    task_id: i64,
    agent: String,
    role: CollaborationRole,
    pr_number: i64,
    head_sha: Option<String>,
    active_run_id: Option<String>,
    state: CollaborationAttemptState,
    expires_at: i64,
}

fn load_attempt_binding(
    conn: &Connection,
    attempt_id: &CollaborationAttemptId,
) -> Result<Option<AttemptBinding>> {
    conn.query_row(
        "SELECT task_id, agent, role, pr_number, head_sha, active_run_id, state, expires_at
         FROM github_collaboration_attempts WHERE attempt_id=?1",
        [attempt_id.as_str()],
        |row| {
            let role_raw: String = row.get(2)?;
            let state_raw: String = row.get(6)?;
            let role = role_raw.parse().map_err(|_| {
                rusqlite::Error::InvalidColumnType(2, "role".into(), rusqlite::types::Type::Text)
            })?;
            let state = state_raw.parse().map_err(|_| {
                rusqlite::Error::InvalidColumnType(6, "state".into(), rusqlite::types::Type::Text)
            })?;
            Ok(AttemptBinding {
                task_id: row.get(0)?,
                agent: row.get(1)?,
                role,
                pr_number: row.get(3)?,
                head_sha: row.get(4)?,
                active_run_id: row.get(5)?,
                state,
                expires_at: row.get(7)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn row_to_operation_status(
    operation_id: GithubOperationId,
    state_raw: String,
    send_state_raw: String,
    remote_object_id: Option<String>,
    response_json: Option<String>,
    error_summary: Option<String>,
) -> Result<GithubOperationStatus> {
    let state = state_raw.parse::<GithubOperationState>()?;
    let send_state = send_state_raw.parse::<GithubSendState>()?;
    let remote_object_id = remote_object_id.map(RemoteObjectId::new).transpose()?;
    let response_json = response_json
        .as_deref()
        .map(GithubOperationResponseJson::new)
        .transpose()?;
    let error_summary = error_summary.map(OperationErrorSummary::new).transpose()?;
    Ok(GithubOperationStatus {
        operation_id,
        state,
        send_state,
        remote_object_id,
        response_json,
        error_summary,
    })
}

struct StoredOperationRow {
    operation_id: String,
    state: String,
    send_state: String,
    remote_object_id: Option<String>,
    response_json: Option<String>,
    error_summary: Option<String>,
}

fn load_operation_by_dedup_key(
    conn: &Connection,
    attempt_id: &CollaborationAttemptId,
    client_request_id: &ClientRequestId,
) -> Result<Option<GithubOperationStatus>> {
    let raw: Option<StoredOperationRow> = conn
        .query_row(
            "SELECT operation_id, state, send_state, remote_object_id, response_json, error_summary
             FROM github_agent_operations
             WHERE attempt_id=?1 AND client_request_id=?2",
            params![attempt_id.as_str(), client_request_id.as_str()],
            |row| {
                Ok(StoredOperationRow {
                    operation_id: row.get(0)?,
                    state: row.get(1)?,
                    send_state: row.get(2)?,
                    remote_object_id: row.get(3)?,
                    response_json: row.get(4)?,
                    error_summary: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let operation_id = GithubOperationId::new(raw.operation_id)?;
    Ok(Some(row_to_operation_status(
        operation_id,
        raw.state,
        raw.send_state,
        raw.remote_object_id,
        raw.response_json,
        raw.error_summary,
    )?))
}

/// Delete every attempt group whose parent is terminal, unpinned, and past its
/// shared retention. Operations are only ever deleted as part of their parent
/// attempt: this preserves the "sweeper never deletes an operation
/// independently" contract. The transaction guards ensure a live parent, an
/// unresolved child, or a pending publication slot pins the group.
fn sweep_expired_terminal_groups(tx: &Transaction<'_>, now: i64) -> Result<()> {
    let attempt_ids: Vec<String> = {
        let mut statement = tx.prepare(
            "SELECT a.attempt_id FROM github_collaboration_attempts a
             WHERE a.state IN ('completed','revoked')
               AND a.expires_at <= ?1
               AND a.active_run_id IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM github_review_publication_slots s
                   WHERE s.attempt_id = a.attempt_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM github_agent_operations o
                   WHERE o.attempt_id = a.attempt_id
                     AND (
                         o.state NOT IN ('succeeded','failed','cancelled')
                         OR o.send_state = 'ambiguous'
                     )
               )",
        )?;
        let rows = statement
            .query_map([now], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows
    };
    for attempt_id in attempt_ids {
        tx.execute(
            "DELETE FROM github_agent_operations WHERE attempt_id=?1",
            [&attempt_id],
        )?;
        tx.execute(
            "DELETE FROM github_collaboration_attempts WHERE attempt_id=?1",
            [&attempt_id],
        )?;
    }
    Ok(())
}

fn operation_capacity_rejection(
    tx: &Transaction<'_>,
    request: &EnqueueGithubOperationRequest,
) -> Result<Option<CollaborationAdmissionError>> {
    // `admit_operation` sweeps every reclaimable expired terminal group before
    // reaching this point. Every remaining row is therefore still owned by an
    // attempt, including a complete expired group pinned by an ambiguous send
    // disposition, and must consume capacity until reconciliation releases it.
    let per_run: i64 = tx.query_row(
        "SELECT COUNT(*) FROM github_agent_operations o
         JOIN github_collaboration_attempts a ON a.attempt_id=o.attempt_id
         WHERE o.created_by_run_id=?1",
        [request.created_by_run_id().as_str()],
        |row| row.get(0),
    )?;
    if per_run >= MAX_NONEXPIRED_OPERATIONS_PER_RUN {
        return Ok(Some(CollaborationAdmissionError::RunOperationLimitReached));
    }
    let per_attempt: i64 = tx.query_row(
        "SELECT COUNT(*) FROM github_agent_operations o
         JOIN github_collaboration_attempts a ON a.attempt_id=o.attempt_id
         WHERE o.attempt_id=?1",
        [request.attempt_id().as_str()],
        |row| row.get(0),
    )?;
    if per_attempt >= MAX_NONEXPIRED_OPERATIONS_PER_ATTEMPT {
        return Ok(Some(
            CollaborationAdmissionError::AttemptOperationLimitReached,
        ));
    }
    let per_task: i64 = tx.query_row(
        "SELECT COUNT(*) FROM github_agent_operations o
         JOIN github_collaboration_attempts a ON a.attempt_id=o.attempt_id
         WHERE a.task_id=?1",
        [request.task_id()],
        |row| row.get(0),
    )?;
    if per_task >= MAX_NONEXPIRED_OPERATIONS_PER_TASK {
        return Ok(Some(CollaborationAdmissionError::TaskOperationLimitReached));
    }
    let per_repo: i64 = tx.query_row(
        "SELECT COUNT(*) FROM github_agent_operations o
         JOIN github_collaboration_attempts a ON a.attempt_id=o.attempt_id",
        [],
        |row| row.get(0),
    )?;
    if per_repo >= MAX_NONEXPIRED_OPERATIONS_PER_REPOSITORY {
        return Ok(Some(
            CollaborationAdmissionError::RepositoryOperationLimitReached,
        ));
    }
    Ok(None)
}

fn attempt_binding_matches_request(
    attempt: &AttemptBinding,
    request: &EnqueueGithubOperationRequest,
) -> bool {
    attempt.task_id == request.task_id()
        && attempt.agent == request.agent().as_str()
        && attempt.role == request.role()
        && attempt.pr_number == request.pr_number()
        && attempt.head_sha.as_deref() == request.reviewer_head_sha().map(ReviewerHeadSha::as_str)
}

fn live_context_matches_request(
    context: &LiveRunContext,
    request: &EnqueueGithubOperationRequest,
) -> bool {
    context.task_id == request.task_id()
        && context.agent == request.agent().as_str()
        && context.role == request.role().as_str()
        && context.pr == Some(request.pr_number())
        && context.review_revision.as_deref()
            == request.reviewer_head_sha().map(ReviewerHeadSha::as_str)
}

fn live_context_matches_attempt(context: &LiveRunContext, attempt: &AttemptBinding) -> bool {
    context.task_id == attempt.task_id
        && context.agent == attempt.agent
        && context.role == attempt.role.as_str()
        && context.pr == Some(attempt.pr_number)
        && context.review_revision.as_deref() == attempt.head_sha.as_deref()
}

fn insert_operation(
    tx: &Transaction<'_>,
    request: &EnqueueGithubOperationRequest,
    attempt_expires_at: i64,
    now: i64,
) -> Result<GithubOperationStatus> {
    tx.execute(
        "INSERT INTO github_agent_operations(
             operation_id, client_request_id, attempt_id, created_by_run_id,
             task_id, agent, role, pr_number, head_sha, kind, request_json,
             state, send_state, attempts, deadline_at, github_marker,
             created_at, updated_at, expires_at
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'queued','not_started',
                   0,?12,?13,?14,?14,?15)",
        params![
            request.operation_id().as_str(),
            request.client_request_id().as_str(),
            request.attempt_id().as_str(),
            request.created_by_run_id().as_str(),
            request.task_id(),
            request.agent().as_str(),
            request.role().as_str(),
            request.pr_number(),
            request.reviewer_head_sha().map(ReviewerHeadSha::as_str),
            request.kind().as_str(),
            request.request_json().as_str(),
            request.deadline_at(),
            request.github_marker().map(GithubMarker::as_str),
            now,
            attempt_expires_at,
        ],
    )?;
    Ok(GithubOperationStatus {
        operation_id: request.operation_id().clone(),
        state: GithubOperationState::Queued,
        send_state: GithubSendState::NotStarted,
        remote_object_id: None,
        response_json: None,
        error_summary: None,
    })
}

/// Admit one GitHub operation into the durable outbox. Every check and the
/// insert run inside a single `BEGIN IMMEDIATE` transaction, so admission
/// serializes on the database write lock. Nothing here executes a GitHub call
/// or contacts the network; the row is stored `queued/not_started` for a
/// separate executor to claim later.
///
/// The admission steps, in order:
///  1. Reject the caller if `caller_run_id` does not match the request's
///     immutable `created_by_run_id`.
///  2. Recompute `operation_id` and (for mutations) the hidden GitHub marker
///     from the immutable attempt/client-request identity and reject any
///     drifted request.
///  3. Sweep every eligible expired terminal attempt group so its rows do not
///     wrongly consume capacity or shadow a fresh admission.
///  4. Revalidate the caller's live capability against the current task phase
///     via `capabilities::resolve_live_run_context` and reject any binding
///     mismatch.
///  5. Load the collaboration attempt row and reject if it is missing,
///     expired, not `active`, or not bound to the caller's capability with
///     the request's immutable target.
///  6. Return the existing row if one already matches the deterministic
///     `(attempt_id, client_request_id)` identity: identical retries are
///     idempotent by design.
///  7. Enforce per-run, per-attempt, per-task and per-repository retained
///     operation caps and reject with a typed capacity outcome if any fires.
///  8. Insert the durable row.
pub fn admit_operation(
    conn: &mut Connection,
    caller_run_id: &str,
    request: &EnqueueGithubOperationRequest,
    now: i64,
) -> Result<GithubOperationAdmissionResult> {
    if now < 0 {
        return Err(QuorumError::Usage(
            "collaboration admission timestamp is invalid".into(),
        ));
    }
    if caller_run_id.is_empty()
        || caller_run_id.contains('\0')
        || caller_run_id.len() > MAX_COLLABORATION_ID_BYTES
    {
        return Err(QuorumError::Usage(
            "collaboration admission caller run id is invalid".into(),
        ));
    }
    if caller_run_id != request.created_by_run_id().as_str() {
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::CapabilityRejected,
        ));
    }
    if request.expires_at() <= now || request.deadline_at() <= now {
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::OperationDeadlineInvalid,
        ));
    }

    let expected_operation_id =
        derive_operation_id(request.attempt_id(), request.client_request_id());
    if request.operation_id() != &expected_operation_id {
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::OperationIdentityMismatch,
        ));
    }
    let expected_marker = derive_github_marker(
        request.attempt_id(),
        request.client_request_id(),
        request.kind(),
    );
    if request.github_marker() != expected_marker.as_ref() {
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::OperationIdentityMismatch,
        ));
    }

    let tx = begin_immediate(conn)?;

    sweep_expired_terminal_groups(&tx, now)?;

    let context = match capabilities::resolve_live_run_context_for_admission(
        &tx,
        caller_run_id,
        request.role().as_str(),
    )? {
        LiveRunContextResolution::Live(context) => context,
        LiveRunContextResolution::Rejected => {
            tx.commit()?;
            return Ok(GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::CapabilityRejected,
            ));
        }
    };
    if !live_context_matches_request(&context, request) {
        tx.commit()?;
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::AttemptBindingMismatch,
        ));
    }

    let Some(attempt) = load_attempt_binding(&tx, request.attempt_id())? else {
        tx.commit()?;
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::AttemptNotFound,
        ));
    };
    if attempt.expires_at <= now {
        tx.commit()?;
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::AttemptExpired,
        ));
    }
    if attempt.state != CollaborationAttemptState::Active {
        tx.commit()?;
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::AttemptNotActive,
        ));
    }
    if attempt.active_run_id.as_deref() != Some(caller_run_id) {
        tx.commit()?;
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::CapabilityRejected,
        ));
    }
    if !attempt_binding_matches_request(&attempt, request) {
        tx.commit()?;
        return Ok(GithubOperationAdmissionResult::Rejected(
            CollaborationAdmissionError::AttemptBindingMismatch,
        ));
    }

    if let Some(existing) =
        load_operation_by_dedup_key(&tx, request.attempt_id(), request.client_request_id())?
    {
        // The unique index makes the derived id necessarily equal to the
        // existing row's id, but re-check defensively: a drifted stored
        // identity is a storage fault, not an admission outcome.
        if existing.operation_id.as_str() != expected_operation_id.as_str() {
            tx.commit()?;
            return Ok(GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::OperationIdentityMismatch,
            ));
        }
        tx.commit()?;
        return Ok(GithubOperationAdmissionResult::Existing(existing));
    }

    if let Some(rejection) = operation_capacity_rejection(&tx, request)? {
        tx.commit()?;
        return Ok(GithubOperationAdmissionResult::Rejected(rejection));
    }

    let status = match insert_operation(&tx, request, attempt.expires_at, now) {
        Ok(status) => status,
        Err(QuorumError::Db(rusqlite::Error::SqliteFailure(failure, _)))
            if failure.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            // A race lost the UNIQUE(attempt_id, client_request_id) or
            // UNIQUE(operation_id) tie. Reload and admit as Existing.
            if let Some(existing) =
                load_operation_by_dedup_key(&tx, request.attempt_id(), request.client_request_id())?
            {
                tx.commit()?;
                return Ok(GithubOperationAdmissionResult::Existing(existing));
            }
            tx.commit()?;
            return Ok(GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::OperationAlreadyExists,
            ));
        }
        Err(error) => return Err(error),
    };
    tx.commit()?;
    Ok(GithubOperationAdmissionResult::Enqueued(status))
}

/// Read a stored operation only when the caller owns the operation's
/// collaboration attempt as its current active run. Adoption of an interrupted
/// attempt rebinds `active_run_id` to the fresh capability, so an adopted
/// caller passes the same authority gate as the original live caller. This
/// function returns bounded closed status only and never exposes the hidden
/// marker, request JSON, or transport credentials.
pub fn read_operation(
    conn: &Connection,
    caller_run_id: &str,
    attempt_id: &CollaborationAttemptId,
    operation_id: &GithubOperationId,
) -> Result<GithubOperationReadResult> {
    if caller_run_id.is_empty()
        || caller_run_id.contains('\0')
        || caller_run_id.len() > MAX_COLLABORATION_ID_BYTES
    {
        return Err(QuorumError::Usage(
            "collaboration operation read caller run id is invalid".into(),
        ));
    }
    struct ReadJoinRow {
        attempt_id: String,
        op_state: String,
        op_send_state: String,
        remote_object_id: Option<String>,
        response_json: Option<String>,
        error_summary: Option<String>,
        attempt_active_run_id: Option<String>,
        attempt_state: String,
        attempt_task_id: i64,
        attempt_agent: String,
        attempt_role: String,
        attempt_pr_number: i64,
        attempt_head_sha: Option<String>,
        attempt_expires_at: i64,
    }
    let raw: Option<ReadJoinRow> = conn
        .query_row(
            "SELECT o.attempt_id, o.state, o.send_state,
                    o.remote_object_id, o.response_json, o.error_summary,
                    a.active_run_id, a.state, a.task_id, a.agent, a.role,
                    a.pr_number, a.head_sha, a.expires_at
             FROM github_agent_operations o
             JOIN github_collaboration_attempts a ON a.attempt_id = o.attempt_id
             WHERE o.operation_id = ?1",
            [operation_id.as_str()],
            |row| {
                Ok(ReadJoinRow {
                    attempt_id: row.get(0)?,
                    op_state: row.get(1)?,
                    op_send_state: row.get(2)?,
                    remote_object_id: row.get(3)?,
                    response_json: row.get(4)?,
                    error_summary: row.get(5)?,
                    attempt_active_run_id: row.get(6)?,
                    attempt_state: row.get(7)?,
                    attempt_task_id: row.get(8)?,
                    attempt_agent: row.get(9)?,
                    attempt_role: row.get(10)?,
                    attempt_pr_number: row.get(11)?,
                    attempt_head_sha: row.get(12)?,
                    attempt_expires_at: row.get(13)?,
                })
            },
        )
        .optional()?;
    let Some(row) = raw else {
        return Ok(GithubOperationReadResult::NotFound);
    };
    if row.attempt_id != attempt_id.as_str() {
        return Ok(GithubOperationReadResult::Rejected(
            CollaborationAdmissionError::OperationBindingMismatch,
        ));
    }
    let attempt_state = row.attempt_state.parse::<CollaborationAttemptState>()?;
    if attempt_state != CollaborationAttemptState::Active {
        return Ok(GithubOperationReadResult::Rejected(
            CollaborationAdmissionError::AttemptNotActive,
        ));
    }
    if row.attempt_active_run_id.as_deref() != Some(caller_run_id) {
        return Ok(GithubOperationReadResult::Rejected(
            CollaborationAdmissionError::CapabilityRejected,
        ));
    }
    let attempt_role = row.attempt_role.parse::<CollaborationRole>()?;
    let context = match capabilities::resolve_live_run_context_for_admission(
        conn,
        caller_run_id,
        attempt_role.as_str(),
    )? {
        LiveRunContextResolution::Live(context) => context,
        LiveRunContextResolution::Rejected => {
            return Ok(GithubOperationReadResult::Rejected(
                CollaborationAdmissionError::CapabilityRejected,
            ));
        }
    };
    let attempt = AttemptBinding {
        task_id: row.attempt_task_id,
        agent: row.attempt_agent,
        role: attempt_role,
        pr_number: row.attempt_pr_number,
        head_sha: row.attempt_head_sha,
        active_run_id: row.attempt_active_run_id,
        state: attempt_state,
        expires_at: row.attempt_expires_at,
    };
    if !live_context_matches_attempt(&context, &attempt) {
        return Ok(GithubOperationReadResult::Rejected(
            CollaborationAdmissionError::CapabilityRejected,
        ));
    }
    let status = row_to_operation_status(
        operation_id.clone(),
        row.op_state,
        row.op_send_state,
        row.remote_object_id,
        row.response_json,
        row.error_summary,
    )?;
    Ok(GithubOperationReadResult::Found(status))
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
    fn initial_rework_continuation_without_provider_id_reuses_one_attempt() {
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        conn.execute(
            "UPDATE tasks SET refs=?1 WHERE id=1",
            [serde_json::json!({
                "pr": WORKER_PR,
                "runner_retry": {
                    "provider": "codex",
                    "model": "model",
                    "effort": "high",
                    "prompt": "start exact collaboration turn",
                    "turn_kind": "initial",
                    "requested": true
                }
            })
            .to_string()],
        )
        .unwrap();
        insert_live_worker(&mut conn, 1, "Worker", "run-initial", 10);
        let original = worker_request("attempt-initial", "run-initial", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &original, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Created(_)
        ));
        let continuation: Option<String> = conn
            .query_row(
                "SELECT turn_continuation_id FROM github_collaboration_attempts
                 WHERE attempt_id='attempt-initial'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(continuation, None);
        assert!(matches!(
            mark_awaiting_resume(&mut conn, &original, 21).unwrap(),
            CollaborationAttemptTransitionResult::Transitioned(status)
                if status.state == CollaborationAttemptState::AwaitingResume
        ));
        crate::capabilities::revoke(&mut conn, "run-initial", 22).unwrap();
        conn.execute(
            "UPDATE agent_runs SET ended_at=22 WHERE task_id=1 AND agent_name='Worker'",
            [],
        )
        .unwrap();
        insert_live_worker(&mut conn, 1, "Worker", "run-initial-resume", 30);
        let resumed = worker_request("attempt-initial", "run-initial-resume", 1, "Worker", 0);

        assert!(matches!(
            adopt_exact_continuation(&mut conn, &resumed, 31).unwrap(),
            CollaborationAttemptTransitionResult::Transitioned(status)
                if status.attempt_id.as_str() == "attempt-initial"
                    && status.state == CollaborationAttemptState::Active
        ));
        assert!(matches!(
            adopt_exact_continuation(&mut conn, &resumed, 32).unwrap(),
            CollaborationAttemptTransitionResult::Existing(status)
                if status.attempt_id.as_str() == "attempt-initial"
        ));
        let rows: i64 = conn
            .query_row(
                "SELECT count(*) FROM github_collaboration_attempts WHERE task_id=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1, "exact initial turn must not mint a second attempt");
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

    // ---- Operation admission integration tests ----

    const OP_NOW: i64 = 1_000;
    const OP_EXPIRES: i64 = 10_000;

    #[allow(clippy::too_many_arguments)]
    fn insert_attempt_row(
        conn: &Connection,
        attempt_id: &str,
        task_id: i64,
        agent: &str,
        role: &str,
        pr: i64,
        head_sha: Option<&str>,
        active_run_id: Option<&str>,
        state: &str,
        expires_at: i64,
    ) {
        conn.execute(
            "INSERT INTO github_collaboration_attempts(
                 attempt_id, task_id, agent, role, pr_number, head_sha,
                 lifecycle_generation, active_run_id, state,
                 created_at, updated_at, expires_at
             ) VALUES (?1,?2,?3,?4,?5,?6,0,?7,?8,1,1,?9)",
            params![
                attempt_id,
                task_id,
                agent,
                role,
                pr,
                head_sha,
                active_run_id,
                state,
                expires_at,
            ],
        )
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    fn build_op_request(
        attempt_id: &str,
        client_request_id: &str,
        run_id: &str,
        task_id: i64,
        agent: &str,
        pr: i64,
        kind: GithubOperationKind,
        body: &str,
        now: i64,
        expires_at: i64,
    ) -> EnqueueGithubOperationRequest {
        let attempt = CollaborationAttemptId::new(attempt_id).unwrap();
        let client = ClientRequestId::new(client_request_id).unwrap();
        let op_id = derive_operation_id(&attempt, &client);
        let marker = derive_github_marker(&attempt, &client, kind);
        EnqueueGithubOperationRequest::new(
            op_id,
            client,
            attempt,
            RunCapabilityId::new(run_id).unwrap(),
            task_id,
            CollaborationAgent::new(agent).unwrap(),
            CollaborationRole::Worker,
            pr,
            None,
            kind,
            GithubOperationRequestJson::new(body).unwrap(),
            marker,
            expires_at - 1,
            now,
            expires_at,
        )
        .unwrap()
    }

    fn setup_authorized_worker(task_id: i64) -> (tempfile::TempDir, Connection, &'static str) {
        const RUN: &str = "run-worker-op";
        let (dir, mut conn) = open_tmp();
        insert_worker_task(&conn, task_id, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, task_id, "Worker", RUN, OP_NOW);
        insert_attempt_row(
            &conn,
            "attempt-1",
            task_id,
            "Worker",
            "worker",
            WORKER_PR,
            None,
            Some(RUN),
            "active",
            OP_EXPIRES,
        );
        (dir, conn, RUN)
    }

    fn count_operations(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM github_agent_operations", [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn count_errors(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM errors", [], |row| row.get(0))
            .unwrap()
    }

    fn task_status(conn: &Connection, task_id: i64) -> String {
        conn.query_row("SELECT status FROM tasks WHERE id=?1", [task_id], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn insert_recovery_pinned_group(
        conn: &mut Connection,
        attempt_id: &str,
        run_id: &str,
        task_id: i64,
        pr_number: i64,
        operation_count: i64,
    ) {
        insert_attempt_row(
            conn,
            attempt_id,
            task_id,
            "Worker",
            "worker",
            pr_number,
            None,
            None,
            "revoked",
            OP_NOW - 1,
        );
        crate::capabilities::issue(conn, run_id, task_id, "Worker", "worker", 1).unwrap();
        for ordinal in 0..operation_count {
            let (state, send_state) = if ordinal == 0 {
                ("failed", "ambiguous")
            } else {
                ("succeeded", "confirmed")
            };
            conn.execute(
                "INSERT INTO github_agent_operations(
                     operation_id, client_request_id, attempt_id, created_by_run_id,
                     task_id, agent, role, pr_number, kind, request_json,
                     state, send_state, attempts, deadline_at,
                     created_at, updated_at, expires_at
                 ) VALUES (?1,?2,?3,?4,?5,'Worker','worker',?6,'pull_request_read','{}',
                           ?7,?8,1,?9,1,1,?10)",
                params![
                    format!("op-{attempt_id}-{ordinal}"),
                    format!("req-{attempt_id}-{ordinal}"),
                    attempt_id,
                    run_id,
                    task_id,
                    pr_number,
                    state,
                    send_state,
                    OP_NOW - 1,
                    OP_NOW - 1,
                ],
            )
            .unwrap();
        }
        crate::capabilities::revoke(conn, run_id, 2).unwrap();
    }

    #[test]
    fn operation_admission_caps_are_defined() {
        assert_eq!(MAX_NONEXPIRED_OPERATIONS_PER_RUN, 64);
        assert_eq!(MAX_NONEXPIRED_OPERATIONS_PER_ATTEMPT, 128);
        assert_eq!(MAX_NONEXPIRED_OPERATIONS_PER_TASK, 512);
        assert_eq!(MAX_NONEXPIRED_OPERATIONS_PER_REPOSITORY, 4_096);
    }

    #[test]
    fn derived_identity_is_stable_and_scoped() {
        let attempt = CollaborationAttemptId::new("attempt-A").unwrap();
        let other_attempt = CollaborationAttemptId::new("attempt-B").unwrap();
        let request = ClientRequestId::new("req-1").unwrap();
        let other_request = ClientRequestId::new("req-2").unwrap();

        assert_eq!(
            derive_operation_id(&attempt, &request),
            derive_operation_id(&attempt, &request)
        );
        assert_ne!(
            derive_operation_id(&attempt, &request),
            derive_operation_id(&other_attempt, &request)
        );
        assert_ne!(
            derive_operation_id(&attempt, &request),
            derive_operation_id(&attempt, &other_request)
        );

        assert_eq!(
            derive_github_marker(&attempt, &request, GithubOperationKind::PullRequestRead),
            None
        );
        let marker =
            derive_github_marker(&attempt, &request, GithubOperationKind::AddIssueComment).unwrap();
        assert_ne!(
            Some(marker),
            derive_github_marker(&attempt, &request, GithubOperationKind::ResolveReviewThread)
        );
    }

    #[test]
    fn admit_operation_enqueues_and_is_idempotent_on_retry() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let request = build_op_request(
            "attempt-1",
            "req-first",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::AddIssueComment,
            r#"{"body":"hi"}"#,
            OP_NOW,
            OP_EXPIRES,
        );

        let first = admit_operation(&mut conn, run, &request, OP_NOW).unwrap();
        assert!(matches!(first, GithubOperationAdmissionResult::Enqueued(_)));
        assert_eq!(count_operations(&conn), 1);

        let second = admit_operation(&mut conn, run, &request, OP_NOW + 1).unwrap();
        match second {
            GithubOperationAdmissionResult::Existing(existing) => {
                let GithubOperationAdmissionResult::Enqueued(original) = first else {
                    panic!("first admission must be enqueued");
                };
                assert_eq!(existing.operation_id, original.operation_id);
            }
            other => panic!("expected Existing on retry, got {other:?}"),
        }
        assert_eq!(count_operations(&conn), 1);
        assert_eq!(count_errors(&conn), 0);
        assert_eq!(task_status(&conn, 1), "rework");
    }

    #[test]
    fn admit_operation_rejects_when_caller_run_does_not_match_created_by() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let request = build_op_request(
            "attempt-1",
            "req-1",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"note":"read"}"#,
            OP_NOW,
            OP_EXPIRES,
        );

        let result = admit_operation(&mut conn, "run-other", &request, OP_NOW).unwrap();
        assert_eq!(
            result,
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::CapabilityRejected
            )
        );
        assert_eq!(count_operations(&conn), 0);
        assert_eq!(count_errors(&conn), 0);
    }

    #[test]
    fn admit_operation_rejects_when_identity_is_forged() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let attempt = CollaborationAttemptId::new("attempt-1").unwrap();
        let client = ClientRequestId::new("req-1").unwrap();
        let forged_op = GithubOperationId::new("op-not-derived").unwrap();
        let request = EnqueueGithubOperationRequest::new(
            forged_op,
            client.clone(),
            attempt.clone(),
            RunCapabilityId::new(run).unwrap(),
            1,
            CollaborationAgent::new("Worker").unwrap(),
            CollaborationRole::Worker,
            WORKER_PR,
            None,
            GithubOperationKind::AddIssueComment,
            GithubOperationRequestJson::new(r#"{"body":"x"}"#).unwrap(),
            derive_github_marker(&attempt, &client, GithubOperationKind::AddIssueComment),
            OP_EXPIRES - 1,
            OP_NOW,
            OP_EXPIRES,
        )
        .unwrap();

        let result = admit_operation(&mut conn, run, &request, OP_NOW).unwrap();
        assert_eq!(
            result,
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::OperationIdentityMismatch
            )
        );
        assert_eq!(count_operations(&conn), 0);
    }

    #[test]
    fn admit_operation_rejects_when_marker_does_not_match_derivation() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let attempt = CollaborationAttemptId::new("attempt-1").unwrap();
        let client = ClientRequestId::new("req-1").unwrap();
        let op_id = derive_operation_id(&attempt, &client);
        let request = EnqueueGithubOperationRequest::new(
            op_id,
            client,
            attempt,
            RunCapabilityId::new(run).unwrap(),
            1,
            CollaborationAgent::new("Worker").unwrap(),
            CollaborationRole::Worker,
            WORKER_PR,
            None,
            GithubOperationKind::AddIssueComment,
            GithubOperationRequestJson::new(r#"{"body":"x"}"#).unwrap(),
            Some(GithubMarker::new("<!-- forged -->").unwrap()),
            OP_EXPIRES - 1,
            OP_NOW,
            OP_EXPIRES,
        )
        .unwrap();

        let result = admit_operation(&mut conn, run, &request, OP_NOW).unwrap();
        assert_eq!(
            result,
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::OperationIdentityMismatch
            )
        );
        assert_eq!(count_operations(&conn), 0);
    }

    #[test]
    fn admit_operation_rejects_when_attempt_is_absent() {
        const RUN: &str = "run-worker-op";
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", RUN, OP_NOW);
        let request = build_op_request(
            "missing-attempt",
            "req-1",
            RUN,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );

        let result = admit_operation(&mut conn, RUN, &request, OP_NOW).unwrap();
        assert_eq!(
            result,
            GithubOperationAdmissionResult::Rejected(CollaborationAdmissionError::AttemptNotFound)
        );
    }

    #[test]
    fn admit_operation_rejects_when_attempt_is_not_active() {
        const RUN: &str = "run-worker-op";
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", RUN, OP_NOW);
        insert_attempt_row(
            &conn,
            "attempt-completed",
            1,
            "Worker",
            "worker",
            WORKER_PR,
            None,
            None,
            "completed",
            OP_EXPIRES,
        );
        let request = build_op_request(
            "attempt-completed",
            "req-1",
            RUN,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );

        let result = admit_operation(&mut conn, RUN, &request, OP_NOW).unwrap();
        assert_eq!(
            result,
            GithubOperationAdmissionResult::Rejected(CollaborationAdmissionError::AttemptNotActive)
        );
        assert_eq!(count_operations(&conn), 0);
    }

    #[test]
    fn admit_operation_rejects_when_attempt_binding_differs() {
        const RUN: &str = "run-worker-op";
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "OtherWorker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "OtherWorker", RUN, OP_NOW);
        insert_attempt_row(
            &conn,
            "attempt-1",
            1,
            "OtherWorker",
            "worker",
            WORKER_PR,
            None,
            Some(RUN),
            "active",
            OP_EXPIRES,
        );

        let request = build_op_request(
            "attempt-1",
            "req-1",
            RUN,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let result = admit_operation(&mut conn, RUN, &request, OP_NOW).unwrap();
        assert_eq!(
            result,
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::AttemptBindingMismatch
            )
        );
    }

    #[test]
    fn admit_operation_rejects_when_attempt_is_not_owned_by_caller() {
        const RUN: &str = "run-worker-op";
        const OTHER_RUN: &str = "run-other-op";
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", RUN, OP_NOW);
        insert_worker_task(&conn, 2, "Worker", WORKER_PR);
        crate::capabilities::issue(&mut conn, OTHER_RUN, 2, "Worker", "worker", OP_NOW).unwrap();
        crate::agent_runs::insert(
            &conn, 2, "Worker", "worker", "model", "high", "codex", OP_NOW,
        )
        .unwrap();
        insert_attempt_row(
            &conn,
            "attempt-1",
            1,
            "Worker",
            "worker",
            WORKER_PR,
            None,
            Some(OTHER_RUN),
            "active",
            OP_EXPIRES,
        );
        let request = build_op_request(
            "attempt-1",
            "req-1",
            RUN,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let result = admit_operation(&mut conn, RUN, &request, OP_NOW).unwrap();
        assert_eq!(
            result,
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::CapabilityRejected
            )
        );
    }

    #[test]
    fn admit_operation_rejects_after_per_run_cap() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        for index in 0..MAX_NONEXPIRED_OPERATIONS_PER_RUN {
            let request = build_op_request(
                "attempt-1",
                &format!("req-{index}"),
                run,
                1,
                "Worker",
                WORKER_PR,
                GithubOperationKind::PullRequestRead,
                r#"{"pr":42}"#,
                OP_NOW,
                OP_EXPIRES,
            );
            let outcome = admit_operation(&mut conn, run, &request, OP_NOW).unwrap();
            assert!(matches!(
                outcome,
                GithubOperationAdmissionResult::Enqueued(_)
            ));
        }

        let over_cap = build_op_request(
            "attempt-1",
            "req-over-cap",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let result = admit_operation(&mut conn, run, &over_cap, OP_NOW).unwrap();
        assert_eq!(
            result,
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::RunOperationLimitReached
            )
        );
        assert_eq!(count_operations(&conn), MAX_NONEXPIRED_OPERATIONS_PER_RUN);
        assert_eq!(count_errors(&conn), 0);
        assert_eq!(task_status(&conn, 1), "rework");
    }

    #[test]
    fn admit_operation_counts_rows_until_their_attempt_expires() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        for index in 0..MAX_NONEXPIRED_OPERATIONS_PER_RUN {
            let now = OP_NOW + (index * 3);
            let request = build_op_request(
                "attempt-1",
                &format!("req-expiring-{index}"),
                run,
                1,
                "Worker",
                WORKER_PR,
                GithubOperationKind::PullRequestRead,
                r#"{"pr":42}"#,
                now,
                now + 2,
            );
            assert!(matches!(
                admit_operation(&mut conn, run, &request, now).unwrap(),
                GithubOperationAdmissionResult::Enqueued(_)
            ));
        }

        let retention: (i64, i64) = conn
            .query_row(
                "SELECT MIN(expires_at), MAX(expires_at) FROM github_agent_operations",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(retention, (OP_EXPIRES, OP_EXPIRES));

        let now = OP_NOW + (MAX_NONEXPIRED_OPERATIONS_PER_RUN * 3);
        let over_cap = build_op_request(
            "attempt-1",
            "req-expiring-over-cap",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            now,
            now + 2,
        );
        assert_eq!(
            admit_operation(&mut conn, run, &over_cap, now).unwrap(),
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::RunOperationLimitReached
            )
        );
        assert_eq!(count_operations(&conn), MAX_NONEXPIRED_OPERATIONS_PER_RUN);
        assert_eq!(count_errors(&conn), 0);
    }

    #[test]
    fn admit_operation_propagates_capability_database_failures() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let request = build_op_request(
            "attempt-1",
            "req-storage-failure",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        conn.execute_batch("DROP TABLE agent_runs").unwrap();

        let error = admit_operation(&mut conn, run, &request, OP_NOW).unwrap_err();
        assert!(matches!(error, QuorumError::Db(_)));
        assert_eq!(count_operations(&conn), 0);
        assert_eq!(count_errors(&conn), 0);
        assert_eq!(task_status(&conn, 1), "rework");
    }

    #[test]
    fn admit_operation_sweeps_expired_terminal_groups_before_capacity_check() {
        const RUN: &str = "run-worker-op";
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", RUN, OP_NOW);
        insert_attempt_row(
            &conn,
            "attempt-live",
            1,
            "Worker",
            "worker",
            WORKER_PR,
            None,
            Some(RUN),
            "active",
            OP_EXPIRES,
        );

        insert_attempt_row(
            &conn,
            "attempt-expired",
            1,
            "Worker",
            "worker",
            WORKER_PR,
            None,
            None,
            "completed",
            OP_NOW - 1,
        );
        crate::capabilities::issue(&mut conn, "run-old", 1, "Worker", "worker", 1).unwrap();
        crate::capabilities::revoke(&mut conn, "run-old", 2).unwrap();
        conn.execute(
            "INSERT INTO github_agent_operations(
                 operation_id, client_request_id, attempt_id, created_by_run_id,
                 task_id, agent, role, pr_number, kind, request_json,
                 state, send_state, attempts, deadline_at,
                 created_at, updated_at, expires_at
             ) VALUES ('op-stale','req-stale','attempt-expired','run-old',
                       1,'Worker','worker',?1,'pull_request_read','{}',
                       'succeeded','confirmed',1,?2,1,1,?3)",
            params![WORKER_PR, OP_NOW - 1, OP_NOW - 1],
        )
        .unwrap();
        assert_eq!(count_operations(&conn), 1);

        let request = build_op_request(
            "attempt-live",
            "req-fresh",
            RUN,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let outcome = admit_operation(&mut conn, RUN, &request, OP_NOW).unwrap();
        assert!(matches!(
            outcome,
            GithubOperationAdmissionResult::Enqueued(_)
        ));
        let attempts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM github_collaboration_attempts",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempts, 1);
        assert_eq!(count_operations(&conn), 1);
    }

    #[test]
    fn admit_operation_does_not_sweep_terminal_group_pinned_by_ambiguous_child() {
        const RUN: &str = "run-worker-op";
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", RUN, OP_NOW);
        insert_attempt_row(
            &conn,
            "attempt-live",
            1,
            "Worker",
            "worker",
            WORKER_PR,
            None,
            Some(RUN),
            "active",
            OP_EXPIRES,
        );

        insert_attempt_row(
            &conn,
            "attempt-pinned",
            1,
            "Worker",
            "worker",
            WORKER_PR,
            None,
            None,
            "revoked",
            OP_NOW - 1,
        );
        crate::capabilities::issue(&mut conn, "run-old", 1, "Worker", "worker", 1).unwrap();
        crate::capabilities::revoke(&mut conn, "run-old", 2).unwrap();
        conn.execute(
            "INSERT INTO github_agent_operations(
                 operation_id, client_request_id, attempt_id, created_by_run_id,
                 task_id, agent, role, pr_number, kind, request_json,
                 state, send_state, attempts, deadline_at,
                 created_at, updated_at, expires_at
             ) VALUES ('op-pinned','req-pinned','attempt-pinned','run-old',
                       1,'Worker','worker',?1,'add_issue_comment','{}',
                       'failed','ambiguous',1,?2,1,1,?3)",
            params![WORKER_PR, OP_NOW - 1, OP_NOW - 1],
        )
        .unwrap();

        let request = build_op_request(
            "attempt-live",
            "req-fresh",
            RUN,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let outcome = admit_operation(&mut conn, RUN, &request, OP_NOW).unwrap();
        assert!(matches!(
            outcome,
            GithubOperationAdmissionResult::Enqueued(_)
        ));
        let pinned_alive: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM github_collaboration_attempts
                 WHERE attempt_id='attempt-pinned'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pinned_alive, 1);
        let pinned_op_alive: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM github_agent_operations WHERE operation_id='op-pinned'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pinned_op_alive, 1);
    }

    #[test]
    fn admit_operation_counts_complete_recovery_pinned_groups_toward_task_capacity() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        for group in 0..(MAX_NONEXPIRED_OPERATIONS_PER_TASK / MAX_NONEXPIRED_OPERATIONS_PER_ATTEMPT)
        {
            insert_recovery_pinned_group(
                &mut conn,
                &format!("attempt-pinned-task-{group}"),
                &format!("run-pinned-task-{group}"),
                1,
                WORKER_PR,
                MAX_NONEXPIRED_OPERATIONS_PER_ATTEMPT,
            );
        }

        let request = build_op_request(
            "attempt-1",
            "req-task-cap",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        assert_eq!(
            admit_operation(&mut conn, run, &request, OP_NOW).unwrap(),
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::TaskOperationLimitReached
            )
        );
        assert_eq!(count_operations(&conn), MAX_NONEXPIRED_OPERATIONS_PER_TASK);
    }

    #[test]
    fn admit_operation_counts_complete_recovery_pinned_groups_toward_repository_capacity() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let groups_per_task =
            MAX_NONEXPIRED_OPERATIONS_PER_TASK / MAX_NONEXPIRED_OPERATIONS_PER_ATTEMPT;
        let task_count =
            MAX_NONEXPIRED_OPERATIONS_PER_REPOSITORY / MAX_NONEXPIRED_OPERATIONS_PER_TASK;
        assert_eq!(
            groups_per_task * task_count * MAX_NONEXPIRED_OPERATIONS_PER_ATTEMPT,
            MAX_NONEXPIRED_OPERATIONS_PER_REPOSITORY
        );
        for task_offset in 0..task_count {
            let task_id = task_offset + 2;
            let pr_number = WORKER_PR + task_id;
            insert_worker_task(&conn, task_id, "Worker", pr_number);
            for group in 0..groups_per_task {
                insert_recovery_pinned_group(
                    &mut conn,
                    &format!("attempt-pinned-repo-{task_id}-{group}"),
                    &format!("run-pinned-repo-{task_id}-{group}"),
                    task_id,
                    pr_number,
                    MAX_NONEXPIRED_OPERATIONS_PER_ATTEMPT,
                );
            }
        }

        let request = build_op_request(
            "attempt-1",
            "req-repository-cap",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        assert_eq!(
            admit_operation(&mut conn, run, &request, OP_NOW).unwrap(),
            GithubOperationAdmissionResult::Rejected(
                CollaborationAdmissionError::RepositoryOperationLimitReached
            )
        );
        assert_eq!(
            count_operations(&conn),
            MAX_NONEXPIRED_OPERATIONS_PER_REPOSITORY
        );
    }

    #[test]
    fn read_operation_returns_status_only_for_authorized_caller() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let request = build_op_request(
            "attempt-1",
            "req-1",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let GithubOperationAdmissionResult::Enqueued(enqueued) =
            admit_operation(&mut conn, run, &request, OP_NOW).unwrap()
        else {
            panic!("admission must succeed");
        };
        let attempt = CollaborationAttemptId::new("attempt-1").unwrap();

        let read = read_operation(&conn, run, &attempt, &enqueued.operation_id).unwrap();
        match read {
            GithubOperationReadResult::Found(status) => {
                assert_eq!(status.operation_id, enqueued.operation_id);
                assert_eq!(status.state, GithubOperationState::Queued);
                assert_eq!(status.send_state, GithubSendState::NotStarted);
            }
            other => panic!("expected Found, got {other:?}"),
        }

        let read = read_operation(&conn, "run-stranger", &attempt, &enqueued.operation_id).unwrap();
        assert_eq!(
            read,
            GithubOperationReadResult::Rejected(CollaborationAdmissionError::CapabilityRejected)
        );

        let unknown = GithubOperationId::new("op-does-not-exist").unwrap();
        let read = read_operation(&conn, run, &attempt, &unknown).unwrap();
        assert_eq!(read, GithubOperationReadResult::NotFound);

        let wrong_attempt = CollaborationAttemptId::new("attempt-other").unwrap();
        let read = read_operation(&conn, run, &wrong_attempt, &enqueued.operation_id).unwrap();
        assert_eq!(
            read,
            GithubOperationReadResult::Rejected(
                CollaborationAdmissionError::OperationBindingMismatch
            )
        );
    }

    #[test]
    fn read_operation_rejects_reads_after_attempt_leaves_active() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let request = build_op_request(
            "attempt-1",
            "req-1",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let GithubOperationAdmissionResult::Enqueued(enqueued) =
            admit_operation(&mut conn, run, &request, OP_NOW).unwrap()
        else {
            panic!("admission must succeed");
        };
        conn.execute(
            "UPDATE github_collaboration_attempts SET state='awaiting_resume',active_run_id=NULL
             WHERE attempt_id='attempt-1'",
            [],
        )
        .unwrap();
        let attempt = CollaborationAttemptId::new("attempt-1").unwrap();
        let read = read_operation(&conn, run, &attempt, &enqueued.operation_id).unwrap();
        assert_eq!(
            read,
            GithubOperationReadResult::Rejected(CollaborationAdmissionError::AttemptNotActive)
        );

        conn.execute(
            "UPDATE github_collaboration_attempts
             SET state='active', active_run_id=?1
             WHERE attempt_id='attempt-1'",
            [run],
        )
        .unwrap();
        let read = read_operation(&conn, run, &attempt, &enqueued.operation_id).unwrap();
        assert!(matches!(read, GithubOperationReadResult::Found(_)));
    }

    #[test]
    fn read_operation_rejects_a_revoked_caller() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let request = build_op_request(
            "attempt-1",
            "req-revoked",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let GithubOperationAdmissionResult::Enqueued(enqueued) =
            admit_operation(&mut conn, run, &request, OP_NOW).unwrap()
        else {
            panic!("admission must succeed");
        };
        crate::capabilities::revoke(&mut conn, run, OP_NOW + 1).unwrap();

        assert_eq!(
            read_operation(
                &conn,
                run,
                &CollaborationAttemptId::new("attempt-1").unwrap(),
                &enqueued.operation_id,
            )
            .unwrap(),
            GithubOperationReadResult::Rejected(CollaborationAdmissionError::CapabilityRejected)
        );
    }

    #[test]
    fn read_operation_rejects_an_ended_caller_run() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let request = build_op_request(
            "attempt-1",
            "req-ended",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let GithubOperationAdmissionResult::Enqueued(enqueued) =
            admit_operation(&mut conn, run, &request, OP_NOW).unwrap()
        else {
            panic!("admission must succeed");
        };
        conn.execute(
            "UPDATE agent_runs SET ended_at=?1 WHERE task_id=1 AND agent_name='Worker'",
            [OP_NOW + 1],
        )
        .unwrap();

        assert_eq!(
            read_operation(
                &conn,
                run,
                &CollaborationAttemptId::new("attempt-1").unwrap(),
                &enqueued.operation_id,
            )
            .unwrap(),
            GithubOperationReadResult::Rejected(CollaborationAdmissionError::CapabilityRejected)
        );
    }

    #[test]
    fn read_operation_rejects_after_caller_leaves_the_task_phase() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let request = build_op_request(
            "attempt-1",
            "req-phase-exit",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let GithubOperationAdmissionResult::Enqueued(enqueued) =
            admit_operation(&mut conn, run, &request, OP_NOW).unwrap()
        else {
            panic!("admission must succeed");
        };
        conn.execute("UPDATE tasks SET status='in-review' WHERE id=1", [])
            .unwrap();

        assert_eq!(
            read_operation(
                &conn,
                run,
                &CollaborationAttemptId::new("attempt-1").unwrap(),
                &enqueued.operation_id,
            )
            .unwrap(),
            GithubOperationReadResult::Rejected(CollaborationAdmissionError::CapabilityRejected)
        );
    }

    #[test]
    fn read_operation_allows_the_exact_adopted_caller() {
        let (_dir, mut conn) = open_tmp();
        insert_worker_task(&conn, 1, "Worker", WORKER_PR);
        insert_live_worker(&mut conn, 1, "Worker", "run-original", 10);
        let original = worker_request("attempt-adopted", "run-original", 1, "Worker", 0);
        assert!(matches!(
            issue_attempt(&mut conn, &original, 20).unwrap(),
            CollaborationAttemptAdmissionResult::Created(_)
        ));
        let request = build_op_request(
            "attempt-adopted",
            "req-adopted",
            "run-original",
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            30,
            100,
        );
        let GithubOperationAdmissionResult::Enqueued(enqueued) =
            admit_operation(&mut conn, "run-original", &request, 30).unwrap()
        else {
            panic!("admission must succeed");
        };
        assert!(matches!(
            mark_awaiting_resume(&mut conn, &original, 31).unwrap(),
            CollaborationAttemptTransitionResult::Transitioned(_)
        ));
        crate::capabilities::revoke(&mut conn, "run-original", 32).unwrap();
        conn.execute(
            "UPDATE agent_runs SET ended_at=32 WHERE task_id=1 AND agent_name='Worker'",
            [],
        )
        .unwrap();
        insert_live_worker(&mut conn, 1, "Worker", "run-resume", 40);
        let resumed = worker_request("attempt-adopted", "run-resume", 1, "Worker", 0);
        assert!(matches!(
            adopt_exact_continuation(&mut conn, &resumed, 41).unwrap(),
            CollaborationAttemptTransitionResult::Transitioned(_)
        ));

        assert!(matches!(
            read_operation(
                &conn,
                "run-resume",
                &CollaborationAttemptId::new("attempt-adopted").unwrap(),
                &enqueued.operation_id,
            )
            .unwrap(),
            GithubOperationReadResult::Found(_)
        ));
    }

    #[test]
    fn admit_operation_dedup_survives_two_serial_races_on_same_identity() {
        let (_dir, mut conn, run) = setup_authorized_worker(1);
        let a = build_op_request(
            "attempt-1",
            "req-shared",
            run,
            1,
            "Worker",
            WORKER_PR,
            GithubOperationKind::PullRequestRead,
            r#"{"pr":42}"#,
            OP_NOW,
            OP_EXPIRES,
        );
        let first = admit_operation(&mut conn, run, &a, OP_NOW).unwrap();
        let second = admit_operation(&mut conn, run, &a, OP_NOW + 1).unwrap();
        assert_eq!(count_operations(&conn), 1);
        match (first, second) {
            (
                GithubOperationAdmissionResult::Enqueued(one),
                GithubOperationAdmissionResult::Existing(two),
            ) => assert_eq!(one.operation_id, two.operation_id),
            other => panic!("expected Enqueued then Existing, got {other:?}"),
        }
    }

    #[test]
    fn real_process_operation_enqueue_race_produces_one_row_and_clean_losers() {
        const CHILD_DB: &str = "QUORUM_OP_ADMISSION_RACE_DB";
        const CHILD_ROUND: &str = "QUORUM_OP_ADMISSION_RACE_ROUND";

        if let (Ok(path), Ok(round)) = (std::env::var(CHILD_DB), std::env::var(CHILD_ROUND)) {
            let mut conn = crate::db::open(std::path::Path::new(&path)).unwrap();
            let request = build_op_request(
                "attempt-1",
                &format!("req-race-{round}"),
                "run-worker-op",
                1,
                "Worker",
                WORKER_PR,
                GithubOperationKind::PullRequestRead,
                r#"{"pr":42}"#,
                OP_NOW,
                OP_EXPIRES,
            );
            let outcome = admit_operation(&mut conn, "run-worker-op", &request, OP_NOW).unwrap();
            let tag = match outcome {
                GithubOperationAdmissionResult::Enqueued(_) => "ENQUEUED",
                GithubOperationAdmissionResult::Existing(_) => "EXISTING",
                GithubOperationAdmissionResult::Rejected(_) => "REJECTED",
            };
            println!("child-outcome:{tag}");
            return;
        }

        for round in 0..3 {
            let (dir, _conn, _run) = setup_authorized_worker(1);
            let db_path = dir.path().join("q.db");

            let test_name =
                "collaboration_admission::tests::real_process_operation_enqueue_race_produces_one_row_and_clean_losers";
            let mut children = Vec::new();
            for _ in 0..3 {
                children.push(
                    Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", test_name, "--nocapture"])
                        .env(CHILD_DB, &db_path)
                        .env(CHILD_ROUND, round.to_string())
                        .stdout(Stdio::piped())
                        .spawn()
                        .unwrap(),
                );
            }
            let outputs: Vec<_> = children
                .into_iter()
                .map(|child| child.wait_with_output().unwrap())
                .collect();
            for output in &outputs {
                assert!(
                    output.status.success(),
                    "child failed in round {round}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            let enqueued = outputs
                .iter()
                .filter(|out| {
                    String::from_utf8_lossy(&out.stdout).contains("child-outcome:ENQUEUED")
                })
                .count();
            let existing = outputs
                .iter()
                .filter(|out| {
                    String::from_utf8_lossy(&out.stdout).contains("child-outcome:EXISTING")
                })
                .count();
            let rejected = outputs
                .iter()
                .filter(|out| {
                    String::from_utf8_lossy(&out.stdout).contains("child-outcome:REJECTED")
                })
                .count();
            assert_eq!(enqueued, 1, "round {round} must have exactly one enqueuer");
            assert_eq!(
                existing,
                outputs.len() - 1,
                "round {round} losers must be Existing"
            );
            assert_eq!(
                rejected, 0,
                "round {round} race loss must never be a rejection"
            );

            let conn = crate::db::open(&db_path).unwrap();
            let rows: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM github_agent_operations
                     WHERE attempt_id='attempt-1' AND client_request_id=?1",
                    [format!("req-race-{round}")],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(rows, 1, "round {round} must retain one operation row");
            let errors: i64 = conn
                .query_row("SELECT COUNT(*) FROM errors", [], |row| row.get(0))
                .unwrap();
            assert_eq!(errors, 0, "round {round} race loss must stay clean");
            let status: String = conn
                .query_row("SELECT status FROM tasks WHERE id=1", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                status, "rework",
                "round {round} task lifecycle must not move"
            );
        }
    }
}
