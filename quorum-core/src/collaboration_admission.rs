//! Closed, bounded vocabulary for collaboration attempts and GitHub operations.
//!
//! This module deliberately contains no admission, transition, or persistence
//! logic. It gives the eventual endpoint and executor one validated vocabulary
//! for request construction and one closed set of durable result/error states.

use crate::error::{QuorumError, Result};
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

pub const MAX_COLLABORATION_ID_BYTES: usize = 128;
pub const MAX_COLLABORATION_AGENT_BYTES: usize = 256;
pub const MAX_GITHUB_MARKER_BYTES: usize = 512;
pub const MAX_OPERATION_JSON_BYTES: usize = 64 * 1024;
pub const MAX_OPERATION_ERROR_BYTES: usize = 2 * 1024;
pub const MAX_OPERATION_ATTEMPTS: u8 = 8;

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
    AttemptNotFound,
    AttemptNotActive,
    AttemptBindingMismatch,
    ActiveAttemptExists,
    AttemptExpired,
    OperationAlreadyExists,
    OperationLimitReached,
}

/// JSON text valid for a closed GitHub-operation request. The value is parsed
/// before it is re-serialized, so durable storage receives SQL-bound canonical
/// JSON rather than opaque caller-controlled text.
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

/// A new daemon-owned logical turn. The active attempt is bound to one exact
/// task/agent/role/PR/generation, plus the immutable reviewer launch SHA.
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

/// One immutable GitHub-operation enqueue request. All target fields repeat
/// the attempt binding so future storage can reject cross-row disagreement in
/// its one admission transaction.
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
