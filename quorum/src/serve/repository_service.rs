//! Closed, daemon-owned Repository Service boundary for GitHub collaboration.
//!
//! The request vocabulary is a closed enum of typed operations. No variant
//! carries an endpoint, HTTP method, URL, GraphQL document, query string,
//! owner/repository, or raw API passthrough, so an agent-supplied request
//! cannot express an arbitrary repository or GitHub API call. The target
//! repository ([`RepositoryTarget`]) and pull request ([`PullRequestNumber`])
//! are separate parameters supplied by the daemon from its repository config
//! and the run's capability; neither type implements `Deserialize`, and agent
//! request JSON that names a target field is rejected at decode.
//!
//! [`GhRepositoryService`] executes through the installed `gh` binary with a
//! token fetched from the daemon credential source once per operation, a
//! per-call timeout, and a response byte cap. Failures are typed and carry
//! whether the request was provably not sent, definitively rejected, or
//! possibly sent.

use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use super::{publication_github_token, run_bounded_gh_command_with_input, BoundedGhError};
use crate::github_credentials::GithubCredentialSource;

/// Per-call deadline for one Repository Service `gh` child (spec: reuse the
/// 30-second publication timeout).
pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);
/// Byte cap for each of the response stdout/stderr pipes (spec: 1 MiB).
pub const DEFAULT_RESPONSE_BYTE_LIMIT: usize = 1024 * 1024;
/// Largest agent request document accepted for decode.
pub const MAX_AGENT_REQUEST_BYTES: usize = 64 * 1024;
/// Largest diagnostic detail retained in a [`RepositoryError`].
const MAX_ERROR_DETAIL_BYTES: usize = 512;
/// Largest Git ref name accepted from a GitHub response.
const MAX_REF_BYTES: usize = 255;
/// Largest `OWNER/REPO` slug accepted from daemon config.
const MAX_REPO_SLUG_BYTES: usize = 200;
/// Largest authored general-comment body, before the daemon marker.
pub const MAX_COMMENT_BODY_BYTES: usize = 32 * 1024;
/// Largest page number and page size a pull request read may request.
pub const MAX_READ_PAGE: u32 = 100;
pub const MAX_READ_PER_PAGE: u32 = 100;
/// Serialized-JSON budget for one bounded read result. It sits below the
/// outbox's retained-result cap so a result at the budget is always stored.
pub const READ_RESULT_BUDGET_BYTES: usize = 48 * 1024;
/// Serialized-JSON budget for one `get_diff` page of diff text.
pub const DIFF_PAGE_BUDGET_BYTES: usize = 40 * 1024;
/// Serialized-JSON bounds for individual text fields in read results.
const MAX_BODY_FIELD_BYTES: usize = 8 * 1024;
const MAX_PATCH_FIELD_BYTES: usize = 16 * 1024;
const MAX_HUNK_FIELD_BYTES: usize = 4 * 1024;
const MAX_SHORT_FIELD_BYTES: usize = 1024;
/// Largest GraphQL node ID accepted in a request or response.
const MAX_NODE_ID_BYTES: usize = 256;
/// Largest repository path an inline comment may anchor to.
const MAX_PATH_BYTES: usize = 4096;
/// Largest review/comment body accepted back from GitHub. It covers an
/// authored body at its bound plus the daemon marker and attribution, and
/// is never clipped, so marker reconciliation sees the whole body.
pub const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;
/// Inline comments one pending review may carry.
#[cfg_attr(not(test), allow(dead_code))]
pub const MAX_PENDING_REVIEW_COMMENTS: usize = 50;
/// Pending reviews one listing may return before it fails closed. GitHub
/// keeps at most one per publisher per PR; more is an anomaly.
pub const MAX_LISTED_PENDING_REVIEWS: usize = 10;
/// Fixed GraphQL document for `add_comment_to_pending_review`. Only its
/// variables carry request fields.
const ADD_REVIEW_THREAD_MUTATION: &str = "mutation($input: AddPullRequestReviewThreadInput!) { \
addPullRequestReviewThread(input: $input) { thread { id pullRequest { number } \
comments(first: 1) { nodes { id databaseId body pullRequestReview { id databaseId } } } } } }";
/// Fixed GraphQL document listing the target PR's review threads, paged by
/// `gh --paginate` through `$endCursor`. Its variables carry only the
/// daemon-derived repository and PR number.
const REVIEW_THREADS_QUERY: &str = "query($owner: String!, $name: String!, $number: Int!, \
$endCursor: String) { repository(owner: $owner, name: $name) { pullRequest(number: $number) { \
reviewThreads(first: 50, after: $endCursor) { pageInfo { hasNextPage endCursor } nodes { \
id isResolved isOutdated path line originalLine startLine diffSide subjectType \
ids: comments(first: 100) { nodes { databaseId } } \
root: comments(first: 1) { nodes { databaseId diffHunk commit { oid } originalCommit { oid } } } \
} } } } }";
/// Fixed `--jq` projection streaming one compact object per thread: comment
/// IDs flattened, the root comment unwrapped, and its diff hunk cut to the
/// lines nearest the anchor so the listing stays within the response cap.
const REVIEW_THREADS_FILTER: &str = ".data.repository.pullRequest.reviewThreads.nodes[] \
| .ids = [.ids.nodes[].databaseId] | .root = (.root.nodes[0] // {}) \
| .root.diffHunk = ((.root.diffHunk // \"\") | .[-1000:])";
/// Fixed GraphQL document for `resolve_review_thread`; only its variables
/// carry the thread node ID.
const RESOLVE_THREAD_MUTATION: &str = "mutation($threadId: ID!) { \
resolveReviewThread(input: {threadId: $threadId}) { thread { id isResolved \
pullRequest { number } } } }";
/// Review threads one listing may return before it fails closed.
pub const MAX_LISTED_REVIEW_THREADS: usize = 1000;
/// Comments one thread may carry in a listing (GitHub page bound).
const MAX_THREAD_COMMENTS: usize = 100;
/// Replies one reconciliation listing may return before it fails closed.
pub const MAX_LISTED_REPLIES: usize = 200;
/// Largest diff-hunk context kept per thread (1000 characters at most).
const MAX_THREAD_HUNK_BYTES: usize = 4 * 1000;
/// Fixed `--jq` filter that keeps only pending reviews from every page.
const PENDING_REVIEW_FILTER: &str = r#".[] | select(.state == "PENDING")"#;
/// Fixed `--jq` filter that streams every item of every page.
const EACH_ITEM_FILTER: &str = ".[]";

/// The daemon-configured repository an operation targets. Built only by
/// daemon code from repository config; never decoded from agent input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryTarget {
    repo_dir: PathBuf,
    gh_repo: Option<String>,
}

impl RepositoryTarget {
    /// `gh_repo` is the optional `OWNER/REPO` override from daemon config;
    /// without it `gh` resolves the repository from `repo_dir`.
    pub fn new(repo_dir: PathBuf, gh_repo: Option<&str>) -> Result<Self, String> {
        if let Some(slug) = gh_repo {
            validate_repo_slug(slug)?;
        }
        Ok(Self {
            repo_dir,
            gh_repo: gh_repo.map(str::to_string),
        })
    }

    pub fn repo_dir(&self) -> &Path {
        &self.repo_dir
    }

    pub fn gh_repo(&self) -> Option<&str> {
        self.gh_repo.as_deref()
    }
}

fn validate_repo_slug(slug: &str) -> Result<(), String> {
    let valid_part = |part: &str| {
        !part.is_empty()
            && !part.starts_with(['-', '.'])
            && part
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    };
    match slug.split_once('/') {
        Some((owner, repo))
            if slug.len() <= MAX_REPO_SLUG_BYTES && valid_part(owner) && valid_part(repo) =>
        {
            Ok(())
        }
        _ => Err("repository slug must be OWNER/REPO".to_string()),
    }
}

/// The pull request an operation targets, derived by the daemon from the
/// run's capability. Never decoded from agent input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullRequestNumber(u64);

impl PullRequestNumber {
    pub fn new(pr: i64) -> Option<Self> {
        u64::try_from(pr).ok().filter(|pr| *pr > 0).map(Self)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

/// Whether an operation can change remote state. Governs how an ambiguous
/// transport outcome is classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationEffect {
    Read,
    Write,
}

/// Closed Repository Service operation vocabulary. Variants carry only typed,
/// bounded operation fields; the target is always a separate daemon parameter.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RepositoryRequest {
    /// Read the target pull request's identity, state, and head/base refs.
    ReadPrMetadata {},
    /// `pull_request_read method=get`: bounded pull request details.
    GetPullRequest {},
    /// `pull_request_read method=get_diff`: one bounded page of the diff.
    GetDiff { page: u32 },
    /// `pull_request_read method=get_files`.
    GetFiles { page: u32, per_page: u32 },
    /// `pull_request_read method=get_review_comments`.
    GetReviewComments { page: u32, per_page: u32 },
    /// `pull_request_read method=get_reviews`.
    GetReviews { page: u32, per_page: u32 },
    /// `pull_request_read method=get_comments` (general PR comments).
    GetComments { page: u32, per_page: u32 },
    /// `add_issue_comment`: one general Markdown comment on the target PR.
    /// The executor appends the operation's hidden marker before the call.
    AddIssueComment { body: String },
    /// `pull_request_review_write method=create`: open the publisher's
    /// pending review on `commit_id` (the immutable launch SHA). No event is
    /// sent, so GitHub leaves the review `PENDING`.
    CreatePendingReview { commit_id: String, body: String },
    /// `add_comment_to_pending_review`: one inline thread on the pending
    /// review identified by its GraphQL node ID.
    AddPendingReviewComment {
        review_node_id: String,
        path: String,
        line: u32,
        side: DiffSide,
        #[serde(default)]
        start_line: Option<u32>,
        #[serde(default)]
        start_side: Option<DiffSide>,
        subject_type: ReviewSubjectType,
        body: String,
    },
    /// `pull_request_review_write method=submit_pending`. The event is fixed
    /// to `COMMENT` in the request document; there is no field to name
    /// another event.
    SubmitPendingReview { review_id: u64, body: String },
    /// `add_reply_to_pull_request_comment`: one reply in the review thread
    /// whose top-level comment is `comment_id`, through GitHub's reply
    /// endpoint. The executor resolves the agent's comment ID to that root
    /// on the target PR and appends the operation's hidden marker.
    ReplyToReviewComment { comment_id: u64, body: String },
    /// `resolve_review_thread`: resolve one review thread by node ID. The
    /// executor first proves the thread belongs to the target PR.
    ResolveReviewThread { thread_node_id: String },
    /// Daemon-only: every review thread of the target PR with its comment
    /// IDs, root anchor, and resolved/outdated state.
    #[serde(skip_deserializing)]
    ListReviewThreads {},
    /// Daemon-only reconciliation: the replies to one top-level review
    /// comment, with complete bodies, so a lost reply is found by marker.
    #[serde(skip_deserializing)]
    ListCommentReplies { comment_id: u64 },
    /// Daemon-only housekeeping: the publisher's current pending review(s)
    /// on the target PR. Never decodable from agent JSON.
    #[serde(skip_deserializing)]
    #[cfg_attr(not(test), allow(dead_code))]
    ListPendingReviews {},
    /// Daemon-only housekeeping: look up one exact review ID.
    #[serde(skip_deserializing)]
    #[cfg_attr(not(test), allow(dead_code))]
    GetReview { review_id: u64 },
    /// Daemon-only housekeeping: delete one exact pending review ID.
    #[serde(skip_deserializing)]
    #[cfg_attr(not(test), allow(dead_code))]
    DeletePendingReview { review_id: u64 },
    /// Daemon-only reconciliation: the inline comments of one exact review
    /// ID, with complete bodies so a lost inline response is found by its
    /// hidden operation marker.
    #[serde(skip_deserializing)]
    ListReviewComments { review_id: u64 },
}

/// Diff side of an inline comment anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum DiffSide {
    #[serde(rename = "LEFT")]
    Left,
    #[serde(rename = "RIGHT")]
    Right,
}

impl DiffSide {
    fn as_str(self) -> &'static str {
        match self {
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
        }
    }
}

/// Whether an inline comment targets diff lines or the whole file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewSubjectType {
    Line,
    File,
}

impl RepositoryRequest {
    /// Decode an agent-supplied request. Unknown kinds, unknown fields
    /// (including any repository or PR target override), oversize input,
    /// invalid UTF-8, and embedded NUL are rejected.
    pub fn decode_agent_json(bytes: &[u8]) -> Result<Self, RequestDecodeError> {
        if bytes.len() > MAX_AGENT_REQUEST_BYTES {
            return Err(RequestDecodeError(format!(
                "request exceeds {MAX_AGENT_REQUEST_BYTES}-byte limit"
            )));
        }
        let text = std::str::from_utf8(bytes)
            .map_err(|_| RequestDecodeError("request is not valid UTF-8".to_string()))?;
        if text.contains('\0') {
            return Err(RequestDecodeError("request contains NUL".to_string()));
        }
        let request: Self = serde_json::from_str(text)
            .map_err(|error| RequestDecodeError(bounded(&error.to_string())))?;
        request.validate()?;
        Ok(request)
    }

    fn validate(&self) -> Result<(), RequestDecodeError> {
        let page_ok = |page: u32| (1..=MAX_READ_PAGE).contains(&page);
        let per_page_ok = |per_page: u32| (1..=MAX_READ_PER_PAGE).contains(&per_page);
        let valid = match self {
            Self::ReadPrMetadata {} | Self::GetPullRequest {} => true,
            Self::GetDiff { page } => page_ok(*page),
            Self::GetFiles { page, per_page }
            | Self::GetReviewComments { page, per_page }
            | Self::GetReviews { page, per_page }
            | Self::GetComments { page, per_page } => page_ok(*page) && per_page_ok(*per_page),
            Self::AddIssueComment { body } => body_ok(body),
            Self::CreatePendingReview { commit_id, body } => {
                is_object_id(commit_id) && body_ok(body)
            }
            Self::AddPendingReviewComment {
                review_node_id,
                path,
                line,
                side: _,
                start_line,
                start_side,
                subject_type,
                body,
            } => {
                let anchor_ok = match subject_type {
                    ReviewSubjectType::Line => {
                        *line >= 1
                            && start_line
                                .map_or(start_side.is_none(), |start| (1..=*line).contains(&start))
                    }
                    // A file-level comment has no line range.
                    ReviewSubjectType::File => {
                        *line >= 1 && start_line.is_none() && start_side.is_none()
                    }
                };
                is_node_id(review_node_id) && path_ok(path) && anchor_ok && body_ok(body)
            }
            Self::SubmitPendingReview { review_id, body } => *review_id >= 1 && body_ok(body),
            Self::ReplyToReviewComment { comment_id, body } => *comment_id >= 1 && body_ok(body),
            Self::ResolveReviewThread { thread_node_id } => is_node_id(thread_node_id),
            Self::ListPendingReviews {} | Self::ListReviewThreads {} => true,
            Self::ListCommentReplies { comment_id } => *comment_id >= 1,
            Self::GetReview { review_id }
            | Self::DeletePendingReview { review_id }
            | Self::ListReviewComments { review_id } => *review_id >= 1,
        };
        if valid {
            Ok(())
        } else {
            Err(RequestDecodeError(
                "request field is outside its bounds".to_string(),
            ))
        }
    }

    pub fn effect(&self) -> OperationEffect {
        match self {
            Self::AddIssueComment { .. }
            | Self::CreatePendingReview { .. }
            | Self::AddPendingReviewComment { .. }
            | Self::SubmitPendingReview { .. }
            | Self::ReplyToReviewComment { .. }
            | Self::ResolveReviewThread { .. }
            | Self::DeletePendingReview { .. } => OperationEffect::Write,
            Self::ReadPrMetadata {}
            | Self::ListReviewThreads {}
            | Self::ListCommentReplies { .. }
            | Self::GetPullRequest {}
            | Self::GetDiff { .. }
            | Self::GetFiles { .. }
            | Self::GetReviewComments { .. }
            | Self::GetReviews { .. }
            | Self::GetComments { .. }
            | Self::ListPendingReviews {}
            | Self::GetReview { .. }
            | Self::ListReviewComments { .. } => OperationEffect::Read,
        }
    }
}

/// A non-blank authored body within the per-body byte bound.
fn body_ok(body: &str) -> bool {
    !body.trim().is_empty() && body.len() <= MAX_COMMENT_BODY_BYTES
}

/// A full lowercase hex Git object ID (SHA-1 or SHA-256).
fn is_object_id(sha: &str) -> bool {
    matches!(sha.len(), 40 | 64) && sha.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// A bounded GitHub GraphQL node ID.
fn is_node_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_NODE_ID_BYTES
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'='))
}

/// A bounded, relative repository path with no control characters.
fn path_ok(path: &str) -> bool {
    !path.trim().is_empty()
        && path.len() <= MAX_PATH_BYTES
        && !path.starts_with('/')
        && !path.chars().any(char::is_control)
}

/// An agent request that does not fit the closed vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestDecodeError(String);

impl fmt::Display for RequestDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid repository request: {}", self.0)
    }
}

impl std::error::Error for RequestDecodeError {}

/// Closed result vocabulary, one variant per request kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepositoryResponse {
    PrMetadata(PrMetadata),
    /// One bounded `pull_request_read` result, built by the daemon from the
    /// typed GitHub response. Its serialized form fits
    /// [`READ_RESULT_BUDGET_BYTES`] and carries a `truncated` indicator.
    PullRequestRead(Value),
    /// The general comment GitHub created.
    IssueComment(CreatedComment),
    /// The review created, submitted, looked up, or deleted.
    Review(ReviewRecord),
    /// The publisher's current pending reviews on the target PR.
    PendingReviews(Vec<ReviewRecord>),
    /// An exact-ID lookup or delete answered 404: the review does not exist
    /// (for a delete, it is already gone).
    ReviewAbsent {
        review_id: u64,
    },
    /// The inline thread added to a pending review.
    PendingReviewComment(CreatedReviewComment),
    /// The inline comments of one exact review.
    ReviewComments(Vec<ReviewCommentRecord>),
    /// Every review thread of the target PR.
    ReviewThreads(Vec<ReviewThreadRecord>),
    /// The reply GitHub created in a review thread.
    ReviewReply(ReviewReplyRecord),
    /// The replies to one top-level review comment.
    ReviewReplies(Vec<ReviewReplyRecord>),
    /// The thread GitHub reports resolved on the target PR.
    ThreadResolved {
        thread_node_id: String,
    },
}

/// One validated review thread of the target PR. `comment_ids` lists the
/// thread's comments in order, starting with the top-level comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewThreadRecord {
    pub node_id: String,
    pub resolved: bool,
    pub outdated: bool,
    pub path: String,
    pub line: Option<u64>,
    pub original_line: Option<u64>,
    pub start_line: Option<u64>,
    pub side: Option<String>,
    pub subject_type: Option<String>,
    pub root_comment_id: u64,
    pub comment_ids: Vec<u64>,
    /// The commit the thread's anchor currently applies to, when GitHub has one.
    pub commit_id: Option<String>,
    /// The commit the top-level comment was made on.
    pub original_commit_id: Option<String>,
    /// The tail of the top-level comment's diff hunk: the lines nearest the anchor.
    pub diff_hunk: String,
}

/// One validated reply in a review thread on the target PR. `body` is
/// complete so a lost reply is found by its hidden marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewReplyRecord {
    pub id: u64,
    pub node_id: String,
    pub in_reply_to_id: u64,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedComment {
    pub id: u64,
    pub html_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewState {
    Pending,
    Commented,
    Approved,
    ChangesRequested,
    Dismissed,
}

impl ReviewState {
    fn parse(state: &str) -> Option<Self> {
        Some(match state {
            "PENDING" => Self::Pending,
            "COMMENTED" => Self::Commented,
            "APPROVED" => Self::Approved,
            "CHANGES_REQUESTED" => Self::ChangesRequested,
            "DISMISSED" => Self::Dismissed,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Commented => "COMMENTED",
            Self::Approved => "APPROVED",
            Self::ChangesRequested => "CHANGES_REQUESTED",
            Self::Dismissed => "DISMISSED",
        }
    }
}

/// A validated review on the target PR. `body` is complete (never clipped)
/// so the daemon can reconcile by its hidden marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewRecord {
    pub id: u64,
    pub node_id: String,
    pub state: ReviewState,
    pub commit_id: Option<String>,
    pub body: String,
}

impl ReviewRecord {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn has_marker(&self, marker: &str) -> bool {
        !marker.is_empty() && self.body.contains(marker)
    }
}

/// A validated inline thread GitHub added to a pending review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedReviewComment {
    pub id: u64,
    pub node_id: String,
    pub thread_node_id: String,
    pub review_node_id: String,
    pub body: String,
}

/// One validated inline comment of an exact review. `body` is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewCommentRecord {
    pub id: u64,
    pub node_id: String,
    pub body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrState {
    Open,
    Closed,
    Merged,
}

/// Validated pull request metadata. Refs are non-empty and bounded; the head
/// SHA is a full lowercase hex object ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrMetadata {
    pub number: PullRequestNumber,
    pub state: PrState,
    pub head_ref: String,
    pub head_sha: String,
    pub base_ref: String,
    pub is_cross_repository: bool,
}

/// What is known about whether a failed request reached GitHub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Provably never handed to `gh`/GitHub.
    NotSent,
    /// GitHub answered with a definitive failure; no effect occurred.
    Rejected,
    /// The request may have reached GitHub; its effect is unknown.
    PossiblySent,
}

/// How a caller must treat a failed operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeClass {
    /// Safe to retry within the caller's fixed attempt bounds.
    Retryable,
    /// Fails closed; retrying cannot help.
    NonRetryable,
    /// A write may have taken effect: reconcile, never resend from absence.
    PossiblySent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepositoryErrorKind {
    /// The daemon credential source produced no token; nothing was run.
    CredentialUnavailable,
    /// The `gh` child could not be spawned; nothing was sent.
    SpawnFailed,
    /// The call exceeded its deadline and the child was killed.
    TimedOut,
    /// A response pipe exceeded the byte cap and the child was killed.
    ResponseTooLarge,
    /// A transport/5xx style failure after spawn: GitHub is unavailable.
    TransientFailure,
    /// GitHub answered with a primary or secondary rate limit.
    RateLimited,
    /// GitHub definitively rejected the request (e.g. 4xx, not found).
    PermanentFailure,
    /// `gh` reported success but the response failed validation.
    InvalidResponse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryError {
    pub kind: RepositoryErrorKind,
    detail: String,
}

impl RepositoryError {
    pub fn new(kind: RepositoryErrorKind, detail: &str) -> Self {
        Self {
            kind,
            detail: bounded(detail),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn delivery(&self) -> Delivery {
        match self.kind {
            RepositoryErrorKind::CredentialUnavailable | RepositoryErrorKind::SpawnFailed => {
                Delivery::NotSent
            }
            RepositoryErrorKind::PermanentFailure => Delivery::Rejected,
            // A rate-limit answer is classified from stderr text, which is
            // not a closed proof of no effect: writes stay fenced.
            RepositoryErrorKind::TimedOut
            | RepositoryErrorKind::ResponseTooLarge
            | RepositoryErrorKind::TransientFailure
            | RepositoryErrorKind::RateLimited
            | RepositoryErrorKind::InvalidResponse => Delivery::PossiblySent,
        }
    }

    /// Classify this failure for an operation with `effect`. A possibly-sent
    /// write is never retryable; a possibly-sent read is retryable only when
    /// the failure is transient.
    pub fn outcome(&self, effect: OperationEffect) -> OutcomeClass {
        match (self.delivery(), effect) {
            (Delivery::NotSent | Delivery::Rejected, _) => OutcomeClass::NonRetryable,
            (Delivery::PossiblySent, OperationEffect::Write) => OutcomeClass::PossiblySent,
            (Delivery::PossiblySent, OperationEffect::Read) => match self.kind {
                RepositoryErrorKind::TimedOut
                | RepositoryErrorKind::TransientFailure
                | RepositoryErrorKind::RateLimited => OutcomeClass::Retryable,
                _ => OutcomeClass::NonRetryable,
            },
        }
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "repository service {:?}: {}", self.kind, self.detail)
    }
}

impl std::error::Error for RepositoryError {}

pub type RepositoryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<RepositoryResponse, RepositoryError>> + Send + 'a>>;

/// The daemon-owned Repository Service. Callers pass the daemon-derived
/// target and PR; the request names only the closed operation.
pub trait RepositoryService: Send + Sync {
    fn execute<'a>(
        &'a self,
        target: &'a RepositoryTarget,
        pr: PullRequestNumber,
        request: &'a RepositoryRequest,
    ) -> RepositoryFuture<'a>;
}

/// Real implementation over the installed `gh` binary.
pub struct GhRepositoryService {
    credentials: Arc<dyn GithubCredentialSource>,
    program: PathBuf,
    leading_args: Vec<PathBuf>,
    timeout: Duration,
    response_byte_limit: usize,
}

impl GhRepositoryService {
    pub fn new(credentials: Arc<dyn GithubCredentialSource>) -> Self {
        Self {
            credentials,
            program: PathBuf::from("gh"),
            leading_args: Vec::new(),
            timeout: DEFAULT_CALL_TIMEOUT,
            response_byte_limit: DEFAULT_RESPONSE_BYTE_LIMIT,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_bounds(mut self, timeout: Duration, response_byte_limit: usize) -> Self {
        self.timeout = timeout;
        self.response_byte_limit = response_byte_limit;
        self
    }

    /// Test hook: run `sh <script>` in place of `gh` (avoids exec of a
    /// freshly written file, which can race to ETXTBSY).
    #[cfg(test)]
    fn with_gh_script(mut self, script: PathBuf) -> Self {
        self.program = PathBuf::from("sh");
        self.leading_args = vec![script];
        self
    }

    #[cfg(test)]
    fn with_program(mut self, program: PathBuf) -> Self {
        self.program = program;
        self.leading_args = Vec::new();
        self
    }

    /// `get_review_comments` also lists the PR's review threads first, so
    /// each comment carries its thread node ID and resolved/outdated state
    /// within the same bounded page.
    async fn run(
        &self,
        target: &RepositoryTarget,
        pr: PullRequestNumber,
        request: &RepositoryRequest,
    ) -> Result<RepositoryResponse, RepositoryError> {
        let threads = match request {
            RepositoryRequest::GetReviewComments { .. } => {
                match self
                    .run_with(target, pr, &RepositoryRequest::ListReviewThreads {}, &[])
                    .await?
                {
                    RepositoryResponse::ReviewThreads(threads) => threads,
                    _ => {
                        return Err(RepositoryError::new(
                            RepositoryErrorKind::InvalidResponse,
                            "review thread listing returned another response",
                        ))
                    }
                }
            }
            _ => Vec::new(),
        };
        self.run_with(target, pr, request, &threads).await
    }

    async fn run_with(
        &self,
        target: &RepositoryTarget,
        pr: PullRequestNumber,
        request: &RepositoryRequest,
        threads: &[ReviewThreadRecord],
    ) -> Result<RepositoryResponse, RepositoryError> {
        let args = gh_args(target, pr, request);
        let input = gh_input(request);
        // One fresh token per operation; a source failure returns before any
        // child is spawned. The token lives only in this frame and the child env.
        let token = publication_github_token(&self.credentials, "repository service")
            .await
            .map_err(|error| {
                RepositoryError::new(RepositoryErrorKind::CredentialUnavailable, &error)
            })?;
        let mut command = tokio::process::Command::new(&self.program);
        command
            .args(&self.leading_args)
            .args(&args)
            .current_dir(target.repo_dir());
        token.apply_to_tokio_command(&mut command);
        let secret = token.expose_secret();
        let redact = |text: &str| text.replace(secret, "<redacted>");
        let output = run_bounded_gh_command_with_input(
            command,
            input,
            self.timeout,
            self.response_byte_limit,
        )
        .await
        .map_err(|error| {
            let kind = match &error {
                BoundedGhError::Spawn(_) => RepositoryErrorKind::SpawnFailed,
                BoundedGhError::PipeOverflow { .. } => RepositoryErrorKind::ResponseTooLarge,
                BoundedGhError::TimedOut { .. } | BoundedGhError::CollectionTimedOut => {
                    RepositoryErrorKind::TimedOut
                }
                BoundedGhError::Io(_) => RepositoryErrorKind::TransientFailure,
            };
            let detail = error.describe("gh", self.timeout, self.response_byte_limit);
            RepositoryError::new(kind, &redact(&detail))
        })?;
        if !output.status.success() {
            let stderr = redact(&String::from_utf8_lossy(&output.stderr));
            // Only the daemon's exact-ID housekeeping reads a 404 as an
            // answer: the review is absent (or a delete already landed).
            if let RepositoryRequest::GetReview { review_id }
            | RepositoryRequest::DeletePendingReview { review_id } = request
            {
                if stderr.to_ascii_lowercase().contains("http 404") {
                    return Ok(RepositoryResponse::ReviewAbsent {
                        review_id: *review_id,
                    });
                }
            }
            return Err(RepositoryError::new(classify_gh_failure(&stderr), &stderr));
        }
        parse_response_with_threads(pr, request, &output.stdout, threads).map_err(|reason| {
            RepositoryError::new(RepositoryErrorKind::InvalidResponse, &redact(&reason))
        })
    }
}

impl RepositoryService for GhRepositoryService {
    fn execute<'a>(
        &'a self,
        target: &'a RepositoryTarget,
        pr: PullRequestNumber,
        request: &'a RepositoryRequest,
    ) -> RepositoryFuture<'a> {
        Box::pin(self.run(target, pr, request))
    }
}

/// The only argv this service can build: a fixed command per closed variant,
/// with the daemon-supplied PR and optional repository slug. Integers are the
/// only request fields that reach argv; authored text never does.
fn gh_args(
    target: &RepositoryTarget,
    pr: PullRequestNumber,
    request: &RepositoryRequest,
) -> Vec<String> {
    let n = pr.get();
    // `gh api` resolves the `{owner}/{repo}` placeholders from the working
    // directory's repository when daemon config names no slug.
    let repo = target.gh_repo().unwrap_or("{owner}/{repo}");
    let api = |path: String| vec!["api".to_string(), format!("repos/{repo}/{path}")];
    let post = |endpoint: String| {
        vec![
            "api".to_string(),
            "--method".to_string(),
            "POST".to_string(),
            endpoint,
            "--input".to_string(),
            "-".to_string(),
        ]
    };
    let paged = |path: &str, page: &u32, per_page: &u32| {
        api(format!("{path}?per_page={per_page}&page={page}"))
    };
    match request {
        RepositoryRequest::ReadPrMetadata {} => {
            let mut args = vec![
                "pr".to_string(),
                "view".to_string(),
                n.to_string(),
                "--json".to_string(),
                "number,state,headRefName,headRefOid,baseRefName,isCrossRepository".to_string(),
            ];
            if let Some(slug) = target.gh_repo() {
                args.push("--repo".to_string());
                args.push(slug.to_string());
            }
            args
        }
        RepositoryRequest::GetPullRequest {} => api(format!("pulls/{n}")),
        RepositoryRequest::GetDiff { .. } => {
            let mut args = vec![
                "api".to_string(),
                "-H".to_string(),
                "Accept: application/vnd.github.diff".to_string(),
            ];
            args.push(format!("repos/{repo}/pulls/{n}"));
            args
        }
        RepositoryRequest::GetFiles { page, per_page } => {
            paged(&format!("pulls/{n}/files"), page, per_page)
        }
        RepositoryRequest::GetReviewComments { page, per_page } => {
            paged(&format!("pulls/{n}/comments"), page, per_page)
        }
        RepositoryRequest::GetReviews { page, per_page } => {
            paged(&format!("pulls/{n}/reviews"), page, per_page)
        }
        RepositoryRequest::GetComments { page, per_page } => {
            paged(&format!("issues/{n}/comments"), page, per_page)
        }
        RepositoryRequest::AddIssueComment { .. } => {
            post(format!("repos/{repo}/issues/{n}/comments"))
        }
        RepositoryRequest::CreatePendingReview { .. } => {
            post(format!("repos/{repo}/pulls/{n}/reviews"))
        }
        RepositoryRequest::SubmitPendingReview { review_id, .. } => {
            post(format!("repos/{repo}/pulls/{n}/reviews/{review_id}/events"))
        }
        // The GraphQL mutation names the pending review by node ID; the
        // response is checked against the target PR.
        RepositoryRequest::AddPendingReviewComment { .. } => vec![
            "api".to_string(),
            "graphql".to_string(),
            "--input".to_string(),
            "-".to_string(),
        ],
        RepositoryRequest::ListPendingReviews {} => vec![
            "api".to_string(),
            "--paginate".to_string(),
            format!("repos/{repo}/pulls/{n}/reviews?per_page=100"),
            "--jq".to_string(),
            PENDING_REVIEW_FILTER.to_string(),
        ],
        RepositoryRequest::GetReview { review_id } => api(format!("pulls/{n}/reviews/{review_id}")),
        RepositoryRequest::DeletePendingReview { review_id } => vec![
            "api".to_string(),
            "--method".to_string(),
            "DELETE".to_string(),
            format!("repos/{repo}/pulls/{n}/reviews/{review_id}"),
        ],
        RepositoryRequest::ListReviewComments { review_id } => vec![
            "api".to_string(),
            "--paginate".to_string(),
            format!("repos/{repo}/pulls/{n}/reviews/{review_id}/comments?per_page=100"),
            "--jq".to_string(),
            EACH_ITEM_FILTER.to_string(),
        ],
        // The true reply endpoint keeps the reply inside the thread.
        RepositoryRequest::ReplyToReviewComment { comment_id, .. } => post(format!(
            "repos/{repo}/pulls/{n}/comments/{comment_id}/replies"
        )),
        RepositoryRequest::ResolveReviewThread { .. } => vec![
            "api".to_string(),
            "graphql".to_string(),
            "--input".to_string(),
            "-".to_string(),
        ],
        RepositoryRequest::ListReviewThreads {} => {
            // `-F` expands gh's `{owner}`/`{repo}` placeholders from the
            // working directory; a configured slug is passed as raw strings.
            let (owner, name) = match target.gh_repo().and_then(|slug| slug.split_once('/')) {
                Some((owner, name)) => (format!("owner={owner}"), format!("name={name}")),
                None => ("owner={owner}".to_string(), "name={repo}".to_string()),
            };
            let field = if target.gh_repo().is_some() {
                "-f"
            } else {
                "-F"
            };
            vec![
                "api".to_string(),
                "graphql".to_string(),
                "--paginate".to_string(),
                "-f".to_string(),
                format!("query={REVIEW_THREADS_QUERY}"),
                field.to_string(),
                owner,
                field.to_string(),
                name,
                "-F".to_string(),
                format!("number={n}"),
                "--jq".to_string(),
                REVIEW_THREADS_FILTER.to_string(),
            ]
        }
        RepositoryRequest::ListCommentReplies { comment_id } => vec![
            "api".to_string(),
            "--paginate".to_string(),
            format!("repos/{repo}/pulls/{n}/comments?per_page=100"),
            "--jq".to_string(),
            format!(".[] | select(.in_reply_to_id == {comment_id})"),
        ],
    }
}

/// The request document streamed to `gh` on stdin. Authored text travels
/// only here, as JSON strings, byte for byte.
fn gh_input(request: &RepositoryRequest) -> Option<Vec<u8>> {
    let document = match request {
        RepositoryRequest::AddIssueComment { body }
        | RepositoryRequest::ReplyToReviewComment { body, .. } => json!({ "body": body }),
        RepositoryRequest::ResolveReviewThread { thread_node_id } => json!({
            "query": RESOLVE_THREAD_MUTATION,
            "variables": { "threadId": thread_node_id },
        }),
        // No `event`: GitHub creates the review as PENDING.
        RepositoryRequest::CreatePendingReview { commit_id, body } => {
            json!({ "commit_id": commit_id, "body": body })
        }
        // The only event this service can send.
        RepositoryRequest::SubmitPendingReview { body, .. } => {
            json!({ "body": body, "event": "COMMENT" })
        }
        RepositoryRequest::AddPendingReviewComment {
            review_node_id,
            path,
            line,
            side,
            start_line,
            start_side,
            subject_type,
            body,
        } => {
            let mut input = json!({
                "pullRequestReviewId": review_node_id,
                "path": path,
                "body": body,
            });
            match subject_type {
                ReviewSubjectType::Line => {
                    input["subjectType"] = json!("LINE");
                    input["line"] = json!(line);
                    input["side"] = json!(side.as_str());
                    if let Some(start_line) = start_line {
                        input["startLine"] = json!(start_line);
                        input["startSide"] = json!(start_side.unwrap_or(*side).as_str());
                    }
                }
                ReviewSubjectType::File => input["subjectType"] = json!("FILE"),
            }
            json!({
                "query": ADD_REVIEW_THREAD_MUTATION,
                "variables": { "input": input },
            })
        }
        _ => return None,
    };
    Some(document.to_string().into_bytes())
}

/// Nonzero `gh` exits whose stderr names a transport, 5xx, or rate-limit
/// condition are transient; anything else is a definitive rejection.
fn classify_gh_failure(stderr: &str) -> RepositoryErrorKind {
    const RATE_LIMIT_MARKERS: &[&str] = &["rate limit", "http 429"];
    const TRANSIENT_MARKERS: &[&str] = &[
        "http 500",
        "http 502",
        "http 503",
        "http 504",
        "timeout",
        "timed out",
        "connection",
        "tls handshake",
        "unexpected eof",
        "temporarily unavailable",
        "something went wrong",
    ];
    let lower = stderr.to_ascii_lowercase();
    if RATE_LIMIT_MARKERS
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return RepositoryErrorKind::RateLimited;
    }
    if TRANSIENT_MARKERS
        .iter()
        .any(|marker| lower.contains(marker))
    {
        RepositoryErrorKind::TransientFailure
    } else {
        RepositoryErrorKind::PermanentFailure
    }
}

fn parse_pr_metadata(pr: PullRequestNumber, stdout: &[u8]) -> Result<PrMetadata, String> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct GhPrView {
        number: u64,
        state: String,
        head_ref_name: String,
        head_ref_oid: String,
        base_ref_name: String,
        is_cross_repository: bool,
    }
    let view: GhPrView =
        serde_json::from_slice(stdout).map_err(|error| format!("gh pr view JSON: {error}"))?;
    if view.number != pr.get() {
        return Err(format!(
            "gh pr view returned PR #{} for requested PR #{}",
            view.number,
            pr.get()
        ));
    }
    let state = match view.state.as_str() {
        "OPEN" => PrState::Open,
        "CLOSED" => PrState::Closed,
        "MERGED" => PrState::Merged,
        _ => return Err("gh pr view returned an unknown state".to_string()),
    };
    let valid_ref = |name: &str| {
        !name.is_empty() && name.len() <= MAX_REF_BYTES && !name.chars().any(|c| c.is_control())
    };
    if !valid_ref(&view.head_ref_name) || !valid_ref(&view.base_ref_name) {
        return Err("gh pr view returned an invalid ref name".to_string());
    }
    let sha = &view.head_ref_oid;
    if !matches!(sha.len(), 40 | 64) || !sha.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err("gh pr view returned an invalid head SHA".to_string());
    }
    Ok(PrMetadata {
        number: pr,
        state,
        head_ref: view.head_ref_name,
        head_sha: view.head_ref_oid,
        base_ref: view.base_ref_name,
        is_cross_repository: view.is_cross_repository,
    })
}

#[cfg(test)]
fn parse_response(
    pr: PullRequestNumber,
    request: &RepositoryRequest,
    stdout: &[u8],
) -> Result<RepositoryResponse, String> {
    parse_response_with_threads(pr, request, stdout, &[])
}

/// Parse one `gh` answer. `threads` is the PR's review-thread listing that
/// `get_review_comments` joins onto each comment (empty otherwise).
fn parse_response_with_threads(
    pr: PullRequestNumber,
    request: &RepositoryRequest,
    stdout: &[u8],
    threads: &[ReviewThreadRecord],
) -> Result<RepositoryResponse, String> {
    let list = |method: &str, page: u32, per_page: u32, items: Result<Vec<Item>, String>| {
        items.map(|items| {
            RepositoryResponse::PullRequestRead(fit_page(method, page, per_page, items))
        })
    };
    match request {
        RepositoryRequest::ReadPrMetadata {} => {
            parse_pr_metadata(pr, stdout).map(RepositoryResponse::PrMetadata)
        }
        RepositoryRequest::GetPullRequest {} => {
            parse_pull_request(pr, stdout).map(RepositoryResponse::PullRequestRead)
        }
        RepositoryRequest::GetDiff { page } => Ok(RepositoryResponse::PullRequestRead(diff_page(
            &String::from_utf8_lossy(stdout),
            *page,
        ))),
        RepositoryRequest::GetFiles { page, per_page } => list(
            "get_files",
            *page,
            *per_page,
            parse_list::<GhFile>(stdout).map(|v| v.into_iter().map(file_item).collect()),
        ),
        RepositoryRequest::GetReviewComments { page, per_page } => list(
            "get_review_comments",
            *page,
            *per_page,
            parse_list::<GhReviewComment>(stdout).map(|v| {
                v.into_iter()
                    .map(|comment| review_comment_item(comment, threads))
                    .collect()
            }),
        ),
        RepositoryRequest::GetReviews { page, per_page } => list(
            "get_reviews",
            *page,
            *per_page,
            parse_list::<GhReview>(stdout).map(|v| v.into_iter().map(review_item).collect()),
        ),
        RepositoryRequest::GetComments { page, per_page } => list(
            "get_comments",
            *page,
            *per_page,
            parse_list::<GhIssueComment>(stdout)
                .map(|v| v.into_iter().map(issue_comment_item).collect()),
        ),
        RepositoryRequest::AddIssueComment { .. } => {
            let created: GhIssueComment = serde_json::from_slice(stdout)
                .map_err(|error| format!("gh api comment JSON: {error}"))?;
            if created.id == 0 || created.html_url.len() > MAX_SHORT_FIELD_BYTES {
                return Err("gh api returned an invalid comment".to_string());
            }
            Ok(RepositoryResponse::IssueComment(CreatedComment {
                id: created.id,
                html_url: created.html_url,
            }))
        }
        RepositoryRequest::CreatePendingReview { commit_id, .. } => {
            let review = parse_review(pr, stdout)?;
            if review.state != ReviewState::Pending
                || review.commit_id.as_deref() != Some(commit_id.as_str())
            {
                return Err("gh api did not create a pending review on the commit".to_string());
            }
            Ok(RepositoryResponse::Review(review))
        }
        RepositoryRequest::SubmitPendingReview { review_id, .. } => {
            let review = parse_review(pr, stdout)?;
            if review.id != *review_id || review.state != ReviewState::Commented {
                return Err("gh api did not submit the review as a comment".to_string());
            }
            Ok(RepositoryResponse::Review(review))
        }
        RepositoryRequest::GetReview { review_id }
        | RepositoryRequest::DeletePendingReview { review_id } => {
            let review = parse_review(pr, stdout)?;
            if review.id != *review_id {
                return Err("gh api returned a different review".to_string());
            }
            Ok(RepositoryResponse::Review(review))
        }
        RepositoryRequest::ListPendingReviews {} => {
            let mut reviews = Vec::new();
            for value in serde_json::Deserializer::from_slice(stdout).into_iter::<Value>() {
                let value = value.map_err(|error| format!("gh api review JSON: {error}"))?;
                let review = review_record(pr, value)?;
                if review.state != ReviewState::Pending {
                    return Err("gh api listed a non-pending review".to_string());
                }
                reviews.push(review);
                if reviews.len() > MAX_LISTED_PENDING_REVIEWS {
                    return Err("gh api listed too many pending reviews".to_string());
                }
            }
            Ok(RepositoryResponse::PendingReviews(reviews))
        }
        RepositoryRequest::AddPendingReviewComment { review_node_id, .. } => {
            parse_review_thread(pr, review_node_id, stdout)
                .map(RepositoryResponse::PendingReviewComment)
        }
        RepositoryRequest::ListReviewComments { review_id } => {
            let mut comments = Vec::new();
            for value in serde_json::Deserializer::from_slice(stdout).into_iter::<Value>() {
                let value =
                    value.map_err(|error| format!("gh api review comment JSON: {error}"))?;
                comments.push(review_comment_record(pr, *review_id, value)?);
                if comments.len() > MAX_PENDING_REVIEW_COMMENTS {
                    return Err("gh api listed too many review comments".to_string());
                }
            }
            Ok(RepositoryResponse::ReviewComments(comments))
        }
        RepositoryRequest::ReplyToReviewComment { comment_id, .. } => {
            let value: Value = serde_json::from_slice(stdout)
                .map_err(|error| format!("gh api reply JSON: {error}"))?;
            review_reply_record(pr, *comment_id, value).map(RepositoryResponse::ReviewReply)
        }
        RepositoryRequest::ListCommentReplies { comment_id } => {
            let mut replies = Vec::new();
            for value in serde_json::Deserializer::from_slice(stdout).into_iter::<Value>() {
                let value = value.map_err(|error| format!("gh api reply JSON: {error}"))?;
                replies.push(review_reply_record(pr, *comment_id, value)?);
                if replies.len() > MAX_LISTED_REPLIES {
                    return Err("gh api listed too many replies".to_string());
                }
            }
            Ok(RepositoryResponse::ReviewReplies(replies))
        }
        RepositoryRequest::ListReviewThreads {} => {
            let mut threads = Vec::new();
            for value in serde_json::Deserializer::from_slice(stdout).into_iter::<Value>() {
                let value = value.map_err(|error| format!("gh api graphql JSON: {error}"))?;
                threads.push(review_thread_record(value)?);
                if threads.len() > MAX_LISTED_REVIEW_THREADS {
                    return Err("gh api listed too many review threads".to_string());
                }
            }
            Ok(RepositoryResponse::ReviewThreads(threads))
        }
        RepositoryRequest::ResolveReviewThread { thread_node_id } => {
            parse_resolved_thread(pr, thread_node_id, stdout)
        }
    }
}

/// Validate one REST reply: it must be in reply to exactly `comment_id` on
/// the target PR and carry a complete bounded body.
fn review_reply_record(
    pr: PullRequestNumber,
    comment_id: u64,
    value: Value,
) -> Result<ReviewReplyRecord, String> {
    #[derive(Deserialize)]
    struct GhReply {
        id: u64,
        node_id: String,
        in_reply_to_id: Option<u64>,
        body: Option<String>,
        pull_request_url: String,
    }
    let reply: GhReply =
        serde_json::from_value(value).map_err(|error| format!("gh api reply JSON: {error}"))?;
    if !reply
        .pull_request_url
        .ends_with(&format!("/pulls/{}", pr.get()))
        || reply.in_reply_to_id != Some(comment_id)
    {
        return Err("gh api returned a reply outside the requested thread".to_string());
    }
    let body = reply.body.unwrap_or_default();
    if reply.id == 0 || !is_node_id(&reply.node_id) || body.len() > MAX_RESPONSE_BODY_BYTES {
        return Err("gh api returned an invalid reply".to_string());
    }
    Ok(ReviewReplyRecord {
        id: reply.id,
        node_id: reply.node_id,
        in_reply_to_id: comment_id,
        body,
    })
}

/// Validate one projected review thread from [`REVIEW_THREADS_FILTER`].
fn review_thread_record(value: Value) -> Result<ReviewThreadRecord, String> {
    #[derive(Deserialize)]
    struct Oid {
        oid: String,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Root {
        database_id: Option<u64>,
        diff_hunk: Option<String>,
        commit: Option<Oid>,
        original_commit: Option<Oid>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Thread {
        id: String,
        is_resolved: bool,
        is_outdated: bool,
        path: String,
        line: Option<u64>,
        original_line: Option<u64>,
        start_line: Option<u64>,
        diff_side: Option<String>,
        subject_type: Option<String>,
        ids: Vec<Option<u64>>,
        root: Root,
    }
    let thread: Thread = serde_json::from_value(value)
        .map_err(|error| format!("gh api review thread JSON: {error}"))?;
    let comment_ids: Vec<u64> = thread.ids.into_iter().flatten().collect();
    let root_comment_id = thread.root.database_id.unwrap_or(0);
    let commit = |oid: Option<Oid>| -> Result<Option<String>, String> {
        match oid {
            Some(Oid { oid }) if !is_object_id(&oid) => {
                Err("gh api returned an invalid thread commit".to_string())
            }
            oid => Ok(oid.map(|oid| oid.oid)),
        }
    };
    let enumerated = |value: &Option<String>, allowed: &[&str]| {
        value
            .as_deref()
            .is_none_or(|value| allowed.contains(&value))
    };
    let diff_hunk = thread.root.diff_hunk.unwrap_or_default();
    if !is_node_id(&thread.id)
        || thread.path.is_empty()
        || thread.path.len() > MAX_PATH_BYTES
        || root_comment_id == 0
        || comment_ids.first() != Some(&root_comment_id)
        || comment_ids.len() > MAX_THREAD_COMMENTS
        || diff_hunk.len() > MAX_THREAD_HUNK_BYTES
        || !enumerated(&thread.diff_side, &["LEFT", "RIGHT"])
        || !enumerated(&thread.subject_type, &["LINE", "FILE"])
    {
        return Err("gh api returned an invalid review thread".to_string());
    }
    Ok(ReviewThreadRecord {
        node_id: thread.id,
        resolved: thread.is_resolved,
        outdated: thread.is_outdated,
        path: thread.path,
        line: thread.line,
        original_line: thread.original_line,
        start_line: thread.start_line,
        side: thread.diff_side,
        subject_type: thread.subject_type,
        root_comment_id,
        comment_ids,
        commit_id: commit(thread.root.commit)?,
        original_commit_id: commit(thread.root.original_commit)?,
        diff_hunk,
    })
}

fn parse_resolved_thread(
    pr: PullRequestNumber,
    thread_node_id: &str,
    stdout: &[u8],
) -> Result<RepositoryResponse, String> {
    #[derive(Deserialize)]
    struct Response {
        data: Data,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Data {
        resolve_review_thread: Payload,
    }
    #[derive(Deserialize)]
    struct Payload {
        thread: Thread,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Thread {
        id: String,
        is_resolved: bool,
        pull_request: Number,
    }
    #[derive(Deserialize)]
    struct Number {
        number: u64,
    }
    let response: Response =
        serde_json::from_slice(stdout).map_err(|error| format!("gh api graphql JSON: {error}"))?;
    let thread = response.data.resolve_review_thread.thread;
    if thread.id != thread_node_id || thread.pull_request.number != pr.get() || !thread.is_resolved
    {
        return Err("gh api graphql did not resolve the requested thread".to_string());
    }
    Ok(RepositoryResponse::ThreadResolved {
        thread_node_id: thread.id,
    })
}

/// Validate one REST review comment: it must belong to the exact review on
/// the target PR and carry a complete bounded body.
fn review_comment_record(
    pr: PullRequestNumber,
    review_id: u64,
    value: Value,
) -> Result<ReviewCommentRecord, String> {
    #[derive(Deserialize)]
    struct GhListedReviewComment {
        id: u64,
        node_id: String,
        body: Option<String>,
        pull_request_review_id: Option<u64>,
        pull_request_url: String,
    }
    let comment: GhListedReviewComment = serde_json::from_value(value)
        .map_err(|error| format!("gh api review comment JSON: {error}"))?;
    if !comment
        .pull_request_url
        .ends_with(&format!("/pulls/{}", pr.get()))
        || comment.pull_request_review_id != Some(review_id)
    {
        return Err("gh api returned a comment of another review".to_string());
    }
    let body = comment.body.unwrap_or_default();
    if comment.id == 0 || !is_node_id(&comment.node_id) || body.len() > MAX_RESPONSE_BODY_BYTES {
        return Err("gh api returned an invalid review comment".to_string());
    }
    Ok(ReviewCommentRecord {
        id: comment.id,
        node_id: comment.node_id,
        body,
    })
}

fn parse_review(pr: PullRequestNumber, stdout: &[u8]) -> Result<ReviewRecord, String> {
    let value: Value =
        serde_json::from_slice(stdout).map_err(|error| format!("gh api review JSON: {error}"))?;
    review_record(pr, value)
}

/// Validate one REST review object: it must belong to the target PR, have a
/// known state, and carry a complete bounded body.
fn review_record(pr: PullRequestNumber, value: Value) -> Result<ReviewRecord, String> {
    #[derive(Deserialize)]
    struct GhPendingReview {
        id: u64,
        node_id: String,
        state: String,
        body: Option<String>,
        commit_id: Option<String>,
        pull_request_url: String,
    }
    let review: GhPendingReview =
        serde_json::from_value(value).map_err(|error| format!("gh api review JSON: {error}"))?;
    if !review
        .pull_request_url
        .ends_with(&format!("/pulls/{}", pr.get()))
    {
        return Err("gh api returned a review for another PR".to_string());
    }
    let state = ReviewState::parse(&review.state)
        .ok_or_else(|| "gh api returned an unknown review state".to_string())?;
    let body = review.body.unwrap_or_default();
    if review.id == 0
        || !is_node_id(&review.node_id)
        || body.len() > MAX_RESPONSE_BODY_BYTES
        || review
            .commit_id
            .as_deref()
            .is_some_and(|sha| !is_object_id(sha))
    {
        return Err("gh api returned an invalid review".to_string());
    }
    Ok(ReviewRecord {
        id: review.id,
        node_id: review.node_id,
        state,
        commit_id: review.commit_id,
        body,
    })
}

fn parse_review_thread(
    pr: PullRequestNumber,
    review_node_id: &str,
    stdout: &[u8],
) -> Result<CreatedReviewComment, String> {
    #[derive(Deserialize)]
    struct Response {
        data: Data,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Data {
        add_pull_request_review_thread: Payload,
    }
    #[derive(Deserialize)]
    struct Payload {
        thread: Thread,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Thread {
        id: String,
        pull_request: Number,
        comments: Nodes,
    }
    #[derive(Deserialize)]
    struct Number {
        number: u64,
    }
    #[derive(Deserialize)]
    struct Nodes {
        nodes: Vec<Comment>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Comment {
        id: String,
        database_id: u64,
        body: String,
        pull_request_review: Review,
    }
    #[derive(Deserialize)]
    struct Review {
        id: String,
    }
    let response: Response =
        serde_json::from_slice(stdout).map_err(|error| format!("gh api graphql JSON: {error}"))?;
    let thread = response.data.add_pull_request_review_thread.thread;
    if thread.pull_request.number != pr.get() {
        return Err("gh api graphql added a thread on another PR".to_string());
    }
    let [comment] = <[Comment; 1]>::try_from(thread.comments.nodes)
        .map_err(|_| "gh api graphql returned no single thread comment".to_string())?;
    if comment.pull_request_review.id != review_node_id
        || comment.database_id == 0
        || !is_node_id(&comment.id)
        || !is_node_id(&thread.id)
        || comment.body.len() > MAX_RESPONSE_BODY_BYTES
    {
        return Err("gh api graphql returned an invalid review thread".to_string());
    }
    Ok(CreatedReviewComment {
        id: comment.database_id,
        node_id: comment.id,
        thread_node_id: thread.id,
        review_node_id: comment.pull_request_review.id,
        body: comment.body,
    })
}

/// One rendered list item and whether any of its text fields was clipped.
type Item = (Value, bool);

#[derive(Deserialize)]
struct GhUser {
    login: String,
}

#[derive(Deserialize)]
struct GhRef {
    #[serde(rename = "ref")]
    name: String,
    sha: String,
}

#[derive(Deserialize)]
struct GhPull {
    number: u64,
    title: String,
    body: Option<String>,
    state: String,
    draft: Option<bool>,
    merged: Option<bool>,
    user: Option<GhUser>,
    head: GhRef,
    base: GhRef,
    html_url: String,
    created_at: Option<String>,
    updated_at: Option<String>,
    additions: Option<u64>,
    deletions: Option<u64>,
    changed_files: Option<u64>,
    commits: Option<u64>,
}

#[derive(Deserialize)]
struct GhFile {
    filename: String,
    status: String,
    additions: u64,
    deletions: u64,
    changes: u64,
    patch: Option<String>,
    previous_filename: Option<String>,
}

#[derive(Deserialize)]
struct GhReviewComment {
    id: u64,
    node_id: Option<String>,
    pull_request_review_id: Option<u64>,
    path: String,
    commit_id: Option<String>,
    original_commit_id: Option<String>,
    side: Option<String>,
    start_side: Option<String>,
    line: Option<u64>,
    start_line: Option<u64>,
    original_line: Option<u64>,
    original_start_line: Option<u64>,
    subject_type: Option<String>,
    diff_hunk: Option<String>,
    body: Option<String>,
    user: Option<GhUser>,
    in_reply_to_id: Option<u64>,
    created_at: Option<String>,
    updated_at: Option<String>,
    html_url: Option<String>,
}

#[derive(Deserialize)]
struct GhReview {
    id: u64,
    user: Option<GhUser>,
    state: String,
    body: Option<String>,
    commit_id: Option<String>,
    submitted_at: Option<String>,
    html_url: Option<String>,
}

#[derive(Deserialize)]
struct GhIssueComment {
    id: u64,
    user: Option<GhUser>,
    body: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    html_url: String,
}

fn parse_list<T: for<'de> Deserialize<'de>>(stdout: &[u8]) -> Result<Vec<T>, String> {
    serde_json::from_slice(stdout).map_err(|error| format!("gh api list JSON: {error}"))
}

/// JSON-escaped length of one character inside a string literal.
fn escaped_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        c if (c as u32) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

/// The longest prefix of `text` whose JSON-escaped form fits `budget` bytes,
/// cut on a character boundary.
fn escaped_prefix(text: &str, budget: usize) -> &str {
    let mut used = 0;
    for (index, c) in text.char_indices() {
        used += escaped_len(c);
        if used > budget {
            return &text[..index];
        }
    }
    text
}

/// Clip one text field to its serialized bound, recording any clipping.
fn clip(text: Option<&str>, budget: usize, clipped: &mut bool) -> Value {
    match text {
        None => Value::Null,
        Some(text) => {
            let kept = escaped_prefix(text, budget);
            if kept.len() < text.len() {
                *clipped = true;
            }
            Value::String(kept.to_string())
        }
    }
}

fn login(user: &Option<GhUser>, clipped: &mut bool) -> Value {
    clip(
        user.as_ref().map(|user| user.login.as_str()),
        MAX_SHORT_FIELD_BYTES,
        clipped,
    )
}

fn parse_pull_request(pr: PullRequestNumber, stdout: &[u8]) -> Result<Value, String> {
    let pull: GhPull =
        serde_json::from_slice(stdout).map_err(|error| format!("gh api pull JSON: {error}"))?;
    if pull.number != pr.get() {
        return Err(format!(
            "gh api returned PR #{} for requested PR #{}",
            pull.number,
            pr.get()
        ));
    }
    let mut clipped = false;
    let short = MAX_SHORT_FIELD_BYTES;
    let pull_request = json!({
        "number": pull.number,
        "title": clip(Some(&pull.title), short, &mut clipped),
        "body": clip(pull.body.as_deref(), MAX_BODY_FIELD_BYTES, &mut clipped),
        "state": clip(Some(&pull.state), short, &mut clipped),
        "draft": pull.draft,
        "merged": pull.merged,
        "author": login(&pull.user, &mut clipped),
        "head_ref": clip(Some(&pull.head.name), short, &mut clipped),
        "head_sha": clip(Some(&pull.head.sha), short, &mut clipped),
        "base_ref": clip(Some(&pull.base.name), short, &mut clipped),
        "base_sha": clip(Some(&pull.base.sha), short, &mut clipped),
        "html_url": clip(Some(&pull.html_url), short, &mut clipped),
        "created_at": clip(pull.created_at.as_deref(), short, &mut clipped),
        "updated_at": clip(pull.updated_at.as_deref(), short, &mut clipped),
        "additions": pull.additions,
        "deletions": pull.deletions,
        "changed_files": pull.changed_files,
        "commits": pull.commits,
    });
    Ok(json!({
        "method": "get",
        "pull_request": pull_request,
        "truncated": clipped,
    }))
}

fn file_item(file: GhFile) -> Item {
    let mut clipped = false;
    let short = MAX_SHORT_FIELD_BYTES * 4;
    let value = json!({
        "filename": clip(Some(&file.filename), short, &mut clipped),
        "previous_filename": clip(file.previous_filename.as_deref(), short, &mut clipped),
        "status": clip(Some(&file.status), MAX_SHORT_FIELD_BYTES, &mut clipped),
        "additions": file.additions,
        "deletions": file.deletions,
        "changes": file.changes,
        "patch": clip(file.patch.as_deref(), MAX_PATCH_FIELD_BYTES, &mut clipped),
    });
    (value, clipped)
}

/// One REST review comment joined to its review thread, when the listing
/// holds it: the thread node ID (for `resolve_review_thread`) and GitHub's
/// own resolved/outdated state. Without a thread, `outdated` falls back to
/// the missing current-diff line.
fn review_comment_item(comment: GhReviewComment, threads: &[ReviewThreadRecord]) -> Item {
    let thread = threads
        .iter()
        .find(|thread| thread.comment_ids.contains(&comment.id));
    let outdated = match thread {
        Some(thread) => thread.outdated,
        // GitHub drops the current-diff anchor of an outdated comment.
        None => comment.line.is_none() && comment.subject_type.as_deref() != Some("file"),
    };
    let mut clipped = false;
    let short = MAX_SHORT_FIELD_BYTES;
    let value = json!({
        "thread_id": thread.map(|thread| thread.node_id.as_str()),
        "resolved": thread.map(|thread| thread.resolved),
        "id": comment.id,
        "node_id": clip(comment.node_id.as_deref(), short, &mut clipped),
        "review_id": comment.pull_request_review_id,
        "path": clip(Some(&comment.path), short * 4, &mut clipped),
        "commit_id": clip(comment.commit_id.as_deref(), short, &mut clipped),
        "original_commit_id": clip(comment.original_commit_id.as_deref(), short, &mut clipped),
        "side": clip(comment.side.as_deref(), short, &mut clipped),
        "start_side": clip(comment.start_side.as_deref(), short, &mut clipped),
        "line": comment.line,
        "start_line": comment.start_line,
        "original_line": comment.original_line,
        "original_start_line": comment.original_start_line,
        "subject_type": clip(comment.subject_type.as_deref(), short, &mut clipped),
        "outdated": outdated,
        "in_reply_to_id": comment.in_reply_to_id,
        "author": login(&comment.user, &mut clipped),
        "body": clip(comment.body.as_deref(), MAX_BODY_FIELD_BYTES, &mut clipped),
        "diff_hunk": clip(comment.diff_hunk.as_deref(), MAX_HUNK_FIELD_BYTES, &mut clipped),
        "created_at": clip(comment.created_at.as_deref(), short, &mut clipped),
        "updated_at": clip(comment.updated_at.as_deref(), short, &mut clipped),
        "html_url": clip(comment.html_url.as_deref(), short, &mut clipped),
    });
    (value, clipped)
}

fn review_item(review: GhReview) -> Item {
    let mut clipped = false;
    let short = MAX_SHORT_FIELD_BYTES;
    let value = json!({
        "id": review.id,
        "author": login(&review.user, &mut clipped),
        "state": clip(Some(&review.state), short, &mut clipped),
        "body": clip(review.body.as_deref(), MAX_BODY_FIELD_BYTES, &mut clipped),
        "commit_id": clip(review.commit_id.as_deref(), short, &mut clipped),
        "submitted_at": clip(review.submitted_at.as_deref(), short, &mut clipped),
        "html_url": clip(review.html_url.as_deref(), short, &mut clipped),
    });
    (value, clipped)
}

fn issue_comment_item(comment: GhIssueComment) -> Item {
    let mut clipped = false;
    let short = MAX_SHORT_FIELD_BYTES;
    let value = json!({
        "id": comment.id,
        "author": login(&comment.user, &mut clipped),
        "body": clip(comment.body.as_deref(), MAX_BODY_FIELD_BYTES, &mut clipped),
        "created_at": clip(comment.created_at.as_deref(), short, &mut clipped),
        "updated_at": clip(comment.updated_at.as_deref(), short, &mut clipped),
        "html_url": clip(Some(&comment.html_url), short, &mut clipped),
    });
    (value, clipped)
}

/// Fit one GitHub page into [`READ_RESULT_BUDGET_BYTES`]. Items are kept in
/// order while they fit; `items_omitted` counts the rest so the caller can
/// re-read with a smaller `perPage`. `has_next_page` reports that GitHub
/// returned a full page.
fn fit_page(method: &str, page: u32, per_page: u32, items: Vec<Item>) -> Value {
    let returned = items.len();
    let envelope = |kept: Vec<Value>, omitted: usize, truncated: bool| {
        json!({
            "method": method,
            "page": page,
            "per_page": per_page,
            "items": kept,
            "items_omitted": omitted,
            "has_next_page": returned >= per_page as usize,
            "truncated": truncated,
        })
    };
    // Worst-case envelope with every counter at its widest.
    let mut used = envelope(Vec::new(), usize::MAX, true).to_string().len();
    let mut kept = Vec::new();
    let mut clipped = false;
    for (item, item_clipped) in items {
        let size = item.to_string().len() + 1;
        if used + size > READ_RESULT_BUDGET_BYTES {
            break;
        }
        used += size;
        clipped |= item_clipped;
        kept.push(item);
    }
    let omitted = returned - kept.len();
    envelope(kept, omitted, clipped || omitted > 0)
}

/// Split `diff` into consecutive pages whose escaped text fits
/// [`DIFF_PAGE_BUDGET_BYTES`], preferring to end each page after a newline,
/// and return page `page` (1-based).
fn diff_page(diff: &str, page: u32) -> Value {
    let mut start = 0;
    let mut current = 1;
    let mut pages = 0;
    let mut selected = "";
    while start < diff.len() {
        let rest = &diff[start..];
        let mut chunk = escaped_prefix(rest, DIFF_PAGE_BUDGET_BYTES);
        if chunk.len() < rest.len() {
            if let Some(newline) = chunk.rfind('\n') {
                chunk = &chunk[..=newline];
            }
        }
        pages += 1;
        if current == page {
            selected = chunk;
        }
        current += 1;
        start += chunk.len();
    }
    json!({
        "method": "get_diff",
        "page": page,
        "total_pages": pages,
        "total_bytes": diff.len(),
        "diff": selected,
        "has_next_page": page < pages,
        "truncated": pages > 1,
    })
}

fn bounded(text: &str) -> String {
    let text = text.trim();
    if text.len() <= MAX_ERROR_DETAIL_BYTES {
        return text.to_string();
    }
    let mut end = MAX_ERROR_DETAIL_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// Scriptable Repository Service for executor tests. Outcomes are consumed
/// in order; every call is recorded with its daemon-supplied target.
#[cfg(test)]
pub struct FakeRepositoryService {
    script: std::sync::Mutex<std::collections::VecDeque<FakeOutcome>>,
    calls: std::sync::Mutex<Vec<FakeCall>>,
    reviews: std::sync::Mutex<FakeReviewStore>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub enum FakeOutcome {
    Succeed(RepositoryResponse),
    /// Fails with no remote effect (for an ambiguous kind, the effect is
    /// unknown to the caller but did not happen).
    Fail(RepositoryErrorKind),
    /// Apply a pending-review request to the fake's review store and
    /// return GitHub's answer.
    Apply,
    /// Apply the request to the store, then lose the response: the effect
    /// happened but the caller sees this possibly-sent failure kind.
    ApplyThenLose(RepositoryErrorKind),
}

/// One review held by the fake, as GitHub would hold it for the publisher.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeReview {
    pub pr: PullRequestNumber,
    pub record: ReviewRecord,
    pub comments: Vec<CreatedReviewComment>,
}

/// The fake's remote review state. It outlives a fake instance via
/// [`FakeRepositoryService::reviews`] / [`FakeRepositoryService::with_reviews`],
/// so a test can carry an orphan pending review across a daemon restart.
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FakeReviewStore {
    pub reviews: Vec<FakeReview>,
    next_id: u64,
}

#[cfg(test)]
impl FakeReviewStore {
    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        1000 + self.next_id
    }

    /// Seed a pending review as if a prior daemon created it and crashed.
    pub fn seed_pending(&mut self, pr: PullRequestNumber, commit_id: &str, body: &str) -> u64 {
        let id = self.next_id();
        self.reviews.push(FakeReview {
            pr,
            record: ReviewRecord {
                id,
                node_id: format!("PRR_fake{id}"),
                state: ReviewState::Pending,
                commit_id: Some(commit_id.to_string()),
                body: body.to_string(),
            },
            comments: Vec::new(),
        });
        id
    }

    fn rejected(detail: &str) -> RepositoryError {
        RepositoryError::new(RepositoryErrorKind::PermanentFailure, detail)
    }

    /// GitHub's behaviour for the pending-review requests: one pending
    /// review per publisher per PR; inline comments and submit only on a
    /// pending review; delete only of a pending review.
    fn apply(
        &mut self,
        pr: PullRequestNumber,
        request: &RepositoryRequest,
    ) -> Result<RepositoryResponse, RepositoryError> {
        let find = |store: &mut Self, id: u64| {
            store
                .reviews
                .iter()
                .position(|review| review.pr == pr && review.record.id == id)
        };
        match request {
            RepositoryRequest::CreatePendingReview { commit_id, body } => {
                if self.pending(pr).next().is_some() {
                    return Err(Self::rejected(
                        "HTTP 422: user can only have one pending review per pull request",
                    ));
                }
                let id = self.seed_pending(pr, commit_id, body);
                let index = find(self, id).unwrap();
                Ok(RepositoryResponse::Review(
                    self.reviews[index].record.clone(),
                ))
            }
            RepositoryRequest::AddPendingReviewComment {
                review_node_id,
                body,
                ..
            } => {
                let id = self.next_id();
                let review = self
                    .reviews
                    .iter_mut()
                    .find(|review| {
                        review.pr == pr
                            && review.record.node_id == *review_node_id
                            && review.record.state == ReviewState::Pending
                    })
                    .ok_or_else(|| Self::rejected("review is not pending"))?;
                if review.comments.len() >= MAX_PENDING_REVIEW_COMMENTS {
                    return Err(Self::rejected("too many inline comments"));
                }
                let comment = CreatedReviewComment {
                    id,
                    node_id: format!("PRRC_fake{id}"),
                    thread_node_id: format!("PRRT_fake{id}"),
                    review_node_id: review_node_id.clone(),
                    body: body.clone(),
                };
                review.comments.push(comment.clone());
                Ok(RepositoryResponse::PendingReviewComment(comment))
            }
            RepositoryRequest::SubmitPendingReview { review_id, body } => {
                let index = find(self, *review_id)
                    .filter(|&i| self.reviews[i].record.state == ReviewState::Pending)
                    .ok_or_else(|| Self::rejected("review is not pending"))?;
                let record = &mut self.reviews[index].record;
                record.state = ReviewState::Commented;
                record.body = body.clone();
                Ok(RepositoryResponse::Review(record.clone()))
            }
            RepositoryRequest::ListPendingReviews {} => Ok(RepositoryResponse::PendingReviews(
                self.pending(pr).cloned().collect(),
            )),
            RepositoryRequest::GetReview { review_id } => Ok(match find(self, *review_id) {
                Some(index) => RepositoryResponse::Review(self.reviews[index].record.clone()),
                None => RepositoryResponse::ReviewAbsent {
                    review_id: *review_id,
                },
            }),
            RepositoryRequest::DeletePendingReview { review_id } => match find(self, *review_id) {
                None => Ok(RepositoryResponse::ReviewAbsent {
                    review_id: *review_id,
                }),
                Some(index) if self.reviews[index].record.state == ReviewState::Pending => Ok(
                    RepositoryResponse::Review(self.reviews.remove(index).record),
                ),
                Some(_) => Err(Self::rejected("HTTP 422: review is not pending")),
            },
            RepositoryRequest::ListReviewComments { review_id } => match find(self, *review_id) {
                None => Err(Self::rejected("HTTP 404: review not found")),
                Some(index) => Ok(RepositoryResponse::ReviewComments(
                    self.reviews[index]
                        .comments
                        .iter()
                        .map(|comment| ReviewCommentRecord {
                            id: comment.id,
                            node_id: comment.node_id.clone(),
                            body: comment.body.clone(),
                        })
                        .collect(),
                )),
            },
            other => panic!("FakeReviewStore cannot apply {other:?}"),
        }
    }

    fn pending(&self, pr: PullRequestNumber) -> impl Iterator<Item = &ReviewRecord> {
        self.reviews
            .iter()
            .filter(move |review| review.pr == pr && review.record.state == ReviewState::Pending)
            .map(|review| &review.record)
    }
}

#[cfg(test)]
impl FakeOutcome {
    pub fn failure() -> Self {
        Self::Fail(RepositoryErrorKind::PermanentFailure)
    }
    pub fn timeout() -> Self {
        Self::Fail(RepositoryErrorKind::TimedOut)
    }
    pub fn oversize() -> Self {
        Self::Fail(RepositoryErrorKind::ResponseTooLarge)
    }
    /// The request was handed off and its effect is unknown.
    pub fn ambiguous() -> Self {
        Self::Fail(RepositoryErrorKind::TransientFailure)
    }
    pub fn credential_unavailable() -> Self {
        Self::Fail(RepositoryErrorKind::CredentialUnavailable)
    }
    /// The write landed on GitHub but its response was lost to a timeout.
    pub fn lost_response() -> Self {
        Self::ApplyThenLose(RepositoryErrorKind::TimedOut)
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeCall {
    pub target: RepositoryTarget,
    pub pr: PullRequestNumber,
    pub request: RepositoryRequest,
}

#[cfg(test)]
impl FakeRepositoryService {
    pub fn scripted(outcomes: impl IntoIterator<Item = FakeOutcome>) -> Self {
        Self {
            script: std::sync::Mutex::new(outcomes.into_iter().collect()),
            calls: Default::default(),
            reviews: Default::default(),
        }
    }

    /// Start from existing remote review state (e.g. after a restart).
    pub fn with_reviews(self, reviews: FakeReviewStore) -> Self {
        *self.reviews.lock().unwrap() = reviews;
        self
    }

    pub fn calls(&self) -> Vec<FakeCall> {
        self.calls.lock().unwrap().clone()
    }

    /// Snapshot of the remote review state.
    pub fn reviews(&self) -> FakeReviewStore {
        self.reviews.lock().unwrap().clone()
    }
}

#[cfg(test)]
impl RepositoryService for FakeRepositoryService {
    fn execute<'a>(
        &'a self,
        target: &'a RepositoryTarget,
        pr: PullRequestNumber,
        request: &'a RepositoryRequest,
    ) -> RepositoryFuture<'a> {
        self.calls.lock().unwrap().push(FakeCall {
            target: target.clone(),
            pr,
            request: request.clone(),
        });
        let outcome = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("FakeRepositoryService: call with no scripted outcome");
        let result = match outcome {
            FakeOutcome::Succeed(response) => Ok(response),
            FakeOutcome::Fail(kind) => Err(RepositoryError::new(kind, "fake outcome")),
            FakeOutcome::Apply => self.reviews.lock().unwrap().apply(pr, request),
            FakeOutcome::ApplyThenLose(kind) => {
                let _ = self.reviews.lock().unwrap().apply(pr, request);
                Err(RepositoryError::new(kind, "fake lost response"))
            }
        };
        Box::pin(async move { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::github_credentials::{FakeCredentialSource, GithubCredentialError};

    const SECRET: &str = "ghp_repositoryservicesecret42";
    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn pr(n: i64) -> PullRequestNumber {
        PullRequestNumber::new(n).unwrap()
    }

    fn target(dir: &Path) -> RepositoryTarget {
        RepositoryTarget::new(dir.to_path_buf(), Some("acme/widgets")).unwrap()
    }

    fn metadata(n: i64) -> PrMetadata {
        PrMetadata {
            number: pr(n),
            state: PrState::Open,
            head_ref: "feature".to_string(),
            head_sha: SHA.to_string(),
            base_ref: "main".to_string(),
            is_cross_repository: false,
        }
    }

    fn pr_json(n: u64) -> String {
        format!(
            r#"{{"number":{n},"state":"OPEN","headRefName":"feature","headRefOid":"{SHA}","baseRefName":"main","isCrossRepository":false}}"#
        )
    }

    fn script(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("gh.sh");
        std::fs::write(&path, format!("{body}\n")).unwrap();
        path
    }

    fn service(
        source: &Arc<FakeCredentialSource>,
        script: PathBuf,
        timeout: Duration,
        limit: usize,
    ) -> GhRepositoryService {
        let credentials: Arc<dyn GithubCredentialSource> = source.clone();
        GhRepositoryService::new(credentials)
            .with_bounds(timeout, limit)
            .with_gh_script(script)
    }

    #[test]
    fn decode_accepts_only_the_closed_vocabulary() {
        assert_eq!(
            RepositoryRequest::decode_agent_json(br#"{"kind":"read_pr_metadata"}"#).unwrap(),
            RepositoryRequest::ReadPrMetadata {}
        );
        for rejected in [
            r#"{"kind":"read_pr_metadata","repo":"evil/repo"}"#,
            r#"{"kind":"read_pr_metadata","pr_number":7}"#,
            r#"{"kind":"read_pr_metadata","owner":"evil"}"#,
            r#"{"kind":"read_pr_metadata","path":"/repos/x/y/pulls"}"#,
            r#"{"kind":"api","method":"DELETE","url":"https://api.github.com/repos/x/y"}"#,
            r#"{"kind":"graphql","query":"mutation{}"}"#,
            r#"{"kind":"READ_PR_METADATA"}"#,
            r#"{"repo":"x/y"}"#,
            r#"{}"#,
            r#"[]"#,
            r#""read_pr_metadata""#,
        ] {
            assert!(
                RepositoryRequest::decode_agent_json(rejected.as_bytes()).is_err(),
                "accepted {rejected}"
            );
        }
        assert!(
            RepositoryRequest::decode_agent_json(b"{\"kind\":\"read_pr_metadata\"\0}").is_err()
        );
        assert!(RepositoryRequest::decode_agent_json(b"{\"kind\":\"\xff\"}").is_err());
        let oversize = format!(
            r#"{{"kind":"read_pr_metadata"{}}}"#,
            " ".repeat(MAX_AGENT_REQUEST_BYTES)
        );
        assert!(RepositoryRequest::decode_agent_json(oversize.as_bytes()).is_err());
    }

    #[test]
    fn targets_are_daemon_constructed_and_validated() {
        assert!(PullRequestNumber::new(0).is_none());
        assert!(PullRequestNumber::new(-3).is_none());
        assert_eq!(PullRequestNumber::new(12).unwrap().get(), 12);
        let dir = PathBuf::from("/tmp");
        assert!(RepositoryTarget::new(dir.clone(), None).is_ok());
        assert!(RepositoryTarget::new(dir.clone(), Some("acme/widgets.rs")).is_ok());
        for bad in [
            "acme", "/widgets", "acme/", "-x/y", "a/b/c", "a b/c", "a/--repo",
        ] {
            assert!(
                RepositoryTarget::new(dir.clone(), Some(bad)).is_err(),
                "{bad}"
            );
        }
        let args = gh_args(
            &RepositoryTarget::new(dir, Some("acme/widgets")).unwrap(),
            pr(9),
            &RepositoryRequest::ReadPrMetadata {},
        );
        assert_eq!(&args[..3], ["pr", "view", "9"]);
        assert_eq!(&args[args.len() - 2..], ["--repo", "acme/widgets"]);
    }

    #[test]
    fn outcome_classification_distinguishes_possibly_sent_writes() {
        use OperationEffect::{Read, Write};
        use OutcomeClass::*;
        use RepositoryErrorKind::*;
        let cases = [
            (
                CredentialUnavailable,
                Delivery::NotSent,
                NonRetryable,
                NonRetryable,
            ),
            (SpawnFailed, Delivery::NotSent, NonRetryable, NonRetryable),
            (
                PermanentFailure,
                Delivery::Rejected,
                NonRetryable,
                NonRetryable,
            ),
            (TimedOut, Delivery::PossiblySent, Retryable, PossiblySent),
            (
                TransientFailure,
                Delivery::PossiblySent,
                Retryable,
                PossiblySent,
            ),
            (
                ResponseTooLarge,
                Delivery::PossiblySent,
                NonRetryable,
                PossiblySent,
            ),
            (
                InvalidResponse,
                Delivery::PossiblySent,
                NonRetryable,
                PossiblySent,
            ),
            (RateLimited, Delivery::PossiblySent, Retryable, PossiblySent),
        ];
        for (kind, delivery, read, write) in cases {
            let error = RepositoryError::new(kind, "x");
            assert_eq!(error.delivery(), delivery, "{kind:?}");
            assert_eq!(error.outcome(Read), read, "{kind:?}");
            assert_eq!(error.outcome(Write), write, "{kind:?}");
        }
        assert_eq!(
            RepositoryRequest::ReadPrMetadata {}.effect(),
            OperationEffect::Read
        );
    }

    #[test]
    fn error_detail_is_bounded() {
        let error = RepositoryError::new(RepositoryErrorKind::PermanentFailure, &"é".repeat(2000));
        assert!(error.detail().len() <= MAX_ERROR_DETAIL_BYTES + '…'.len_utf8());
    }

    #[tokio::test]
    async fn fake_service_scripts_outcomes_in_order_and_records_targets() {
        let dir = tempfile::tempdir().unwrap();
        let fake = FakeRepositoryService::scripted([
            FakeOutcome::Succeed(RepositoryResponse::PrMetadata(metadata(4))),
            FakeOutcome::failure(),
            FakeOutcome::timeout(),
            FakeOutcome::oversize(),
            FakeOutcome::ambiguous(),
            FakeOutcome::credential_unavailable(),
        ]);
        let service: &dyn RepositoryService = &fake;
        let request = RepositoryRequest::ReadPrMetadata {};
        let target = target(dir.path());
        assert_eq!(
            service.execute(&target, pr(4), &request).await.unwrap(),
            RepositoryResponse::PrMetadata(metadata(4))
        );
        let mut kinds = Vec::new();
        for _ in 0..5 {
            kinds.push(service.execute(&target, pr(4), &request).await.unwrap_err());
        }
        let kinds: Vec<_> = kinds
            .iter()
            .map(|e| (e.kind, e.outcome(OperationEffect::Write)))
            .collect();
        assert_eq!(
            kinds,
            [
                (
                    RepositoryErrorKind::PermanentFailure,
                    OutcomeClass::NonRetryable
                ),
                (RepositoryErrorKind::TimedOut, OutcomeClass::PossiblySent),
                (
                    RepositoryErrorKind::ResponseTooLarge,
                    OutcomeClass::PossiblySent
                ),
                (
                    RepositoryErrorKind::TransientFailure,
                    OutcomeClass::PossiblySent
                ),
                (
                    RepositoryErrorKind::CredentialUnavailable,
                    OutcomeClass::NonRetryable
                ),
            ]
        );
        let calls = fake.calls();
        assert_eq!(calls.len(), 6);
        assert!(calls
            .iter()
            .all(|call| call.target == target && call.pr == pr(4) && call.request == request));
    }

    #[tokio::test]
    async fn real_service_reads_metadata_with_a_fresh_token_per_operation() {
        let dir = tempfile::tempdir().unwrap();
        let args_log = dir.path().join("args");
        let body = format!(
            "[ \"$GH_TOKEN\" = \"{SECRET}\" ] || {{ echo bad token >&2; exit 9; }}\n\
             echo \"$@\" >> '{}'\n\
             printf '%s' '{}'",
            args_log.display(),
            pr_json(7)
        );
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        let service = service(
            &source,
            script(dir.path(), &body),
            Duration::from_secs(10),
            DEFAULT_RESPONSE_BYTE_LIMIT,
        );
        let request = RepositoryRequest::ReadPrMetadata {};
        let target = target(dir.path());
        for expected_calls in 1..=2 {
            let response = service.execute(&target, pr(7), &request).await.unwrap();
            assert_eq!(response, RepositoryResponse::PrMetadata(metadata(7)));
            assert_eq!(source.calls(), expected_calls);
        }
        let logged = std::fs::read_to_string(&args_log).unwrap();
        assert_eq!(
            logged.lines().next().unwrap(),
            "pr view 7 --json number,state,headRefName,headRefOid,baseRefName,isCrossRepository --repo acme/widgets"
        );
    }

    #[tokio::test]
    async fn credential_failure_is_not_sent_and_spawns_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("ran");
        let source = Arc::new(FakeCredentialSource::failing(
            GithubCredentialError::LookupFailed {
                reason: "no auth".to_string(),
            },
        ));
        let service = service(
            &source,
            script(dir.path(), &format!("touch '{}'", marker.display())),
            Duration::from_secs(10),
            DEFAULT_RESPONSE_BYTE_LIMIT,
        );
        let error = service
            .execute(
                &target(dir.path()),
                pr(1),
                &RepositoryRequest::ReadPrMetadata {},
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, RepositoryErrorKind::CredentialUnavailable);
        assert_eq!(error.delivery(), Delivery::NotSent);
        assert_eq!(
            error.outcome(OperationEffect::Write),
            OutcomeClass::NonRetryable
        );
        assert_eq!(source.calls(), 1);
        assert!(!marker.exists(), "gh child ran despite credential failure");
    }

    #[tokio::test]
    async fn missing_program_is_a_not_sent_spawn_failure() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        let credentials: Arc<dyn GithubCredentialSource> = source.clone();
        let service =
            GhRepositoryService::new(credentials).with_program(dir.path().join("no-such-gh"));
        let error = service
            .execute(
                &target(dir.path()),
                pr(1),
                &RepositoryRequest::ReadPrMetadata {},
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, RepositoryErrorKind::SpawnFailed);
        assert_eq!(error.delivery(), Delivery::NotSent);
    }

    #[tokio::test]
    async fn timeout_kills_the_call_and_is_possibly_sent() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        let service = service(
            &source,
            script(dir.path(), "exec sleep 30"),
            Duration::from_millis(300),
            DEFAULT_RESPONSE_BYTE_LIMIT,
        );
        let started = std::time::Instant::now();
        let error = service
            .execute(
                &target(dir.path()),
                pr(1),
                &RepositoryRequest::ReadPrMetadata {},
            )
            .await
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(error.kind, RepositoryErrorKind::TimedOut);
        assert_eq!(error.delivery(), Delivery::PossiblySent);
        assert_eq!(
            error.outcome(OperationEffect::Read),
            OutcomeClass::Retryable
        );
        assert_eq!(
            error.outcome(OperationEffect::Write),
            OutcomeClass::PossiblySent
        );
    }

    #[tokio::test]
    async fn oversize_response_is_capped_and_typed() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        let service = service(
            &source,
            script(dir.path(), "head -c 100000 /dev/zero"),
            Duration::from_secs(10),
            4096,
        );
        let error = service
            .execute(
                &target(dir.path()),
                pr(1),
                &RepositoryRequest::ReadPrMetadata {},
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, RepositoryErrorKind::ResponseTooLarge);
        assert!(
            error.detail().contains("4096-byte limit"),
            "{}",
            error.detail()
        );
        assert_eq!(
            error.outcome(OperationEffect::Read),
            OutcomeClass::NonRetryable
        );
        assert_eq!(
            error.outcome(OperationEffect::Write),
            OutcomeClass::PossiblySent
        );
    }

    #[tokio::test]
    async fn failed_exits_are_classified_and_never_echo_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        for (stderr, expected) in [
            (
                "HTTP 502: Bad Gateway",
                RepositoryErrorKind::TransientFailure,
            ),
            ("API rate limit exceeded", RepositoryErrorKind::RateLimited),
            (
                "You have exceeded a secondary rate limit (HTTP 403)",
                RepositoryErrorKind::RateLimited,
            ),
            (
                "HTTP 503: Service Unavailable",
                RepositoryErrorKind::TransientFailure,
            ),
            (
                "GraphQL: Could not resolve to a PullRequest (HTTP 404)",
                RepositoryErrorKind::PermanentFailure,
            ),
        ] {
            let body = format!("echo \"{stderr} $GH_TOKEN\" >&2; exit 1");
            let service = service(
                &source,
                script(dir.path(), &body),
                Duration::from_secs(10),
                DEFAULT_RESPONSE_BYTE_LIMIT,
            );
            let error = service
                .execute(
                    &target(dir.path()),
                    pr(1),
                    &RepositoryRequest::ReadPrMetadata {},
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind, expected, "{stderr}");
            assert!(!error.detail().contains(SECRET));
            assert!(!error.to_string().contains(SECRET));
            assert!(error.detail().contains("<redacted>"));
        }
    }

    #[tokio::test]
    async fn invalid_or_mismatched_responses_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        let wrong_sha = pr_json(1).replace(SHA, "not-a-sha");
        let bad_state = pr_json(1).replace("OPEN", "DRAFT");
        for stdout in [pr_json(2), wrong_sha, bad_state, "not json".to_string()] {
            let service = service(
                &source,
                script(dir.path(), &format!("printf '%s' '{stdout}'")),
                Duration::from_secs(10),
                DEFAULT_RESPONSE_BYTE_LIMIT,
            );
            let error = service
                .execute(
                    &target(dir.path()),
                    pr(1),
                    &RepositoryRequest::ReadPrMetadata {},
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind, RepositoryErrorKind::InvalidResponse, "{stdout}");
        }
    }

    #[test]
    fn decode_accepts_bounded_read_methods_and_comments() {
        for (json, expected) in [
            (
                r#"{"kind":"get_pull_request"}"#,
                RepositoryRequest::GetPullRequest {},
            ),
            (
                r#"{"kind":"get_diff","page":3}"#,
                RepositoryRequest::GetDiff { page: 3 },
            ),
            (
                r#"{"kind":"get_files","page":1,"per_page":100}"#,
                RepositoryRequest::GetFiles {
                    page: 1,
                    per_page: 100,
                },
            ),
            (
                r#"{"kind":"get_review_comments","page":100,"per_page":1}"#,
                RepositoryRequest::GetReviewComments {
                    page: 100,
                    per_page: 1,
                },
            ),
            (
                r#"{"kind":"get_reviews","page":2,"per_page":30}"#,
                RepositoryRequest::GetReviews {
                    page: 2,
                    per_page: 30,
                },
            ),
            (
                r#"{"kind":"get_comments","page":2,"per_page":30}"#,
                RepositoryRequest::GetComments {
                    page: 2,
                    per_page: 30,
                },
            ),
            (
                r#"{"kind":"add_issue_comment","body":"hi"}"#,
                RepositoryRequest::AddIssueComment { body: "hi".into() },
            ),
        ] {
            assert_eq!(
                RepositoryRequest::decode_agent_json(json.as_bytes()).unwrap(),
                expected
            );
        }
        let oversize = serde_json::json!({
            "kind": "add_issue_comment",
            "body": "x".repeat(MAX_COMMENT_BODY_BYTES + 1)
        })
        .to_string();
        for rejected in [
            r#"{"kind":"get_diff","page":0}"#,
            r#"{"kind":"get_files","page":1,"per_page":101}"#,
            r#"{"kind":"get_files","page":101,"per_page":1}"#,
            r#"{"kind":"get_files","page":1}"#,
            r#"{"kind":"get_reviews","page":1,"per_page":1,"pr":9}"#,
            r#"{"kind":"add_issue_comment","body":"  "}"#,
            r#"{"kind":"add_issue_comment","body":"x","issue_number":3}"#,
            r#"{"kind":"get_check_runs"}"#,
            oversize.as_str(),
        ] {
            assert!(
                RepositoryRequest::decode_agent_json(rejected.as_bytes()).is_err(),
                "accepted {rejected}"
            );
        }
        assert_eq!(
            RepositoryRequest::AddIssueComment { body: "x".into() }.effect(),
            OperationEffect::Write
        );
        assert_eq!(
            RepositoryRequest::GetDiff { page: 1 }.effect(),
            OperationEffect::Read
        );
    }

    #[test]
    fn argv_names_only_fixed_paths_and_integers() {
        let dir = PathBuf::from("/tmp");
        let target = RepositoryTarget::new(dir.clone(), Some("acme/widgets")).unwrap();
        let cases = [
            (
                RepositoryRequest::GetPullRequest {},
                vec!["api", "repos/acme/widgets/pulls/9"],
            ),
            (
                RepositoryRequest::GetDiff { page: 2 },
                vec![
                    "api",
                    "-H",
                    "Accept: application/vnd.github.diff",
                    "repos/acme/widgets/pulls/9",
                ],
            ),
            (
                RepositoryRequest::GetFiles {
                    page: 2,
                    per_page: 5,
                },
                vec!["api", "repos/acme/widgets/pulls/9/files?per_page=5&page=2"],
            ),
            (
                RepositoryRequest::GetReviewComments {
                    page: 1,
                    per_page: 30,
                },
                vec![
                    "api",
                    "repos/acme/widgets/pulls/9/comments?per_page=30&page=1",
                ],
            ),
            (
                RepositoryRequest::GetReviews {
                    page: 1,
                    per_page: 30,
                },
                vec![
                    "api",
                    "repos/acme/widgets/pulls/9/reviews?per_page=30&page=1",
                ],
            ),
            (
                RepositoryRequest::GetComments {
                    page: 1,
                    per_page: 30,
                },
                vec![
                    "api",
                    "repos/acme/widgets/issues/9/comments?per_page=30&page=1",
                ],
            ),
            (
                RepositoryRequest::AddIssueComment {
                    body: "--repo evil/x `rm`".into(),
                },
                vec![
                    "api",
                    "--method",
                    "POST",
                    "repos/acme/widgets/issues/9/comments",
                    "--input",
                    "-",
                ],
            ),
        ];
        for (request, expected) in cases {
            assert_eq!(gh_args(&target, pr(9), &request), expected, "{request:?}");
        }
        let unpinned = RepositoryTarget::new(dir, None).unwrap();
        assert_eq!(
            gh_args(&unpinned, pr(9), &RepositoryRequest::GetPullRequest {}),
            ["api", "repos/{owner}/{repo}/pulls/9"]
        );
        assert!(gh_input(&RepositoryRequest::GetPullRequest {}).is_none());
    }

    #[tokio::test]
    async fn comment_body_reaches_gh_on_stdin_byte_for_byte() {
        let dir = tempfile::tempdir().unwrap();
        let stdin_log = dir.path().join("stdin");
        let args_log = dir.path().join("args");
        let body = format!(
            "cat > '{}'\necho \"$@\" > '{}'\nprintf '%s' '{{\"id\":321,\"html_url\":\"https://x/1\"}}'",
            stdin_log.display(),
            args_log.display()
        );
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        let service = service(
            &source,
            script(dir.path(), &body),
            Duration::from_secs(10),
            DEFAULT_RESPONSE_BYTE_LIMIT,
        );
        let markdown =
            "## `x` \"q\" 'single' $HOME $(id)\n\n```sh\necho \"hi\"\n```\n\\n\t\u{e9}\n";
        let response = service
            .execute(
                &target(dir.path()),
                pr(7),
                &RepositoryRequest::AddIssueComment {
                    body: markdown.into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response,
            RepositoryResponse::IssueComment(CreatedComment {
                id: 321,
                html_url: "https://x/1".into()
            })
        );
        let sent: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&stdin_log).unwrap()).unwrap();
        assert_eq!(sent, serde_json::json!({ "body": markdown }));
        let args = std::fs::read_to_string(&args_log).unwrap();
        assert_eq!(
            args.trim_end(),
            "api --method POST repos/acme/widgets/issues/7/comments --input -"
        );
        assert!(!args.contains("HOME"));
    }

    #[test]
    fn pull_request_get_is_bounded_and_matches_the_target() {
        let pull = |number: u64, body: &str| {
            serde_json::json!({
                "number": number, "title": "T", "body": body, "state": "open",
                "draft": false, "merged": false, "user": {"login": "alice"},
                "head": {"ref": "feature", "sha": SHA}, "base": {"ref": "main", "sha": SHA},
                "html_url": "https://x/pull/7", "additions": 1, "deletions": 2,
                "changed_files": 3, "commits": 1, "unrelated": {"nested": true}
            })
            .to_string()
        };
        let request = RepositoryRequest::GetPullRequest {};
        let RepositoryResponse::PullRequestRead(value) =
            parse_response(pr(7), &request, pull(7, "short").as_bytes()).unwrap()
        else {
            panic!("expected a read result")
        };
        assert_eq!(value["method"], "get");
        assert_eq!(value["pull_request"]["author"], "alice");
        assert_eq!(value["pull_request"]["head_sha"], SHA);
        assert_eq!(value["truncated"], false);
        assert!(value["pull_request"].get("unrelated").is_none());

        let long = "\u{1}".repeat(MAX_BODY_FIELD_BYTES);
        let RepositoryResponse::PullRequestRead(value) =
            parse_response(pr(7), &request, pull(7, &long).as_bytes()).unwrap()
        else {
            panic!("expected a read result")
        };
        assert_eq!(value["truncated"], true);
        assert!(value.to_string().len() <= READ_RESULT_BUDGET_BYTES);
        assert!(parse_response(pr(7), &request, pull(8, "x").as_bytes()).is_err());
    }

    #[test]
    fn list_pages_fit_the_budget_and_report_omissions() {
        let comments: Vec<serde_json::Value> = (0..60)
            .map(|id| {
                serde_json::json!({
                    "id": id + 1, "user": {"login": "bob"}, "body": "b".repeat(4_000),
                    "created_at": "2026-01-01T00:00:00Z", "html_url": "https://x"
                })
            })
            .collect();
        let stdout = serde_json::to_vec(&comments).unwrap();
        let request = RepositoryRequest::GetComments {
            page: 1,
            per_page: 60,
        };
        let RepositoryResponse::PullRequestRead(value) =
            parse_response(pr(7), &request, &stdout).unwrap()
        else {
            panic!("expected a read result")
        };
        assert!(value.to_string().len() <= READ_RESULT_BUDGET_BYTES);
        let kept = value["items"].as_array().unwrap().len();
        assert!(kept > 0 && kept < 60, "{kept}");
        assert_eq!(value["items_omitted"], 60 - kept);
        assert_eq!(value["truncated"], true);
        assert_eq!(value["has_next_page"], true);
        assert_eq!(value["items"][0]["id"], 1);

        let review_comment = serde_json::json!([{
            "id": 5, "node_id": "PRRC_x", "path": "src/a.rs", "line": null,
            "original_line": 4, "side": "RIGHT", "body": "b", "diff_hunk": "@@",
            "user": null, "subject_type": "line"
        }]);
        let RepositoryResponse::PullRequestRead(value) = parse_response(
            pr(7),
            &RepositoryRequest::GetReviewComments {
                page: 1,
                per_page: 30,
            },
            review_comment.to_string().as_bytes(),
        )
        .unwrap() else {
            panic!("expected a read result")
        };
        assert_eq!(value["items"][0]["outdated"], true);
        assert_eq!(value["items"][0]["author"], serde_json::Value::Null);
        assert_eq!(value["has_next_page"], false);
        assert_eq!(value["truncated"], false);
        assert!(parse_response(
            pr(7),
            &RepositoryRequest::GetReviews {
                page: 1,
                per_page: 30
            },
            b"{\"not\":\"a list\"}"
        )
        .is_err());
    }

    #[test]
    fn diff_pages_cover_the_whole_diff_within_the_budget() {
        let line = "+    let value = \"quoted\\tvalue\";\n";
        let diff = line.repeat(4_000);
        let first = diff_page(&diff, 1);
        let pages = first["total_pages"].as_u64().unwrap();
        assert!(pages > 1);
        let mut joined = String::new();
        for page in 1..=pages {
            let value = diff_page(&diff, page as u32);
            assert!(value.to_string().len() <= READ_RESULT_BUDGET_BYTES);
            let text = value["diff"].as_str().unwrap();
            assert!(text.ends_with('\n'), "pages end on a line boundary");
            joined.push_str(text);
            assert_eq!(value["has_next_page"], page < pages);
        }
        assert_eq!(joined, diff);
        assert_eq!(diff_page(&diff, pages as u32 + 1)["diff"], "");
        let small = diff_page("+x\n", 1);
        assert_eq!(
            (small["total_pages"].as_u64(), small["truncated"].as_bool()),
            (Some(1), Some(false))
        );
    }

    fn review_json(id: u64, n: u64, state: &str, body: &str) -> String {
        json!({
            "id": id,
            "node_id": format!("PRR_{id}"),
            "state": state,
            "body": body,
            "commit_id": SHA,
            "pull_request_url": format!("https://api.github.com/repos/acme/widgets/pulls/{n}"),
        })
        .to_string()
    }

    fn inline(review_node_id: &str) -> RepositoryRequest {
        RepositoryRequest::AddPendingReviewComment {
            review_node_id: review_node_id.into(),
            path: "src/lib.rs".into(),
            line: 12,
            side: DiffSide::Right,
            start_line: Some(10),
            start_side: None,
            subject_type: ReviewSubjectType::Line,
            body: "Finding `x`".into(),
        }
    }

    #[test]
    fn decode_accepts_pending_review_writes_with_comment_only_submit() {
        let decode = |text: &str| RepositoryRequest::decode_agent_json(text.as_bytes());
        assert_eq!(
            decode(&format!(
                r#"{{"kind":"create_pending_review","commit_id":"{SHA}","body":"m"}}"#
            ))
            .unwrap(),
            RepositoryRequest::CreatePendingReview {
                commit_id: SHA.into(),
                body: "m".into()
            }
        );
        assert_eq!(
            decode(r#"{"kind":"submit_pending_review","review_id":9,"body":"Summary"}"#).unwrap(),
            RepositoryRequest::SubmitPendingReview {
                review_id: 9,
                body: "Summary".into()
            }
        );
        assert_eq!(
            decode(
                r#"{"kind":"add_pending_review_comment","review_node_id":"PRR_x","path":"src/lib.rs","line":12,"side":"RIGHT","start_line":10,"subject_type":"line","body":"Finding `x`"}"#
            )
            .unwrap(),
            inline("PRR_x")
        );
        let big = "x".repeat(MAX_COMMENT_BODY_BYTES + 1);
        let comment = |fields: &str| {
            format!(
                r#"{{"kind":"add_pending_review_comment","review_node_id":"PRR_x","path":"a.rs","subject_type":"line","body":"b",{fields}}}"#
            )
        };
        for rejected in [
            // No event other than the fixed COMMENT can be expressed.
            r#"{"kind":"submit_pending_review","review_id":9,"body":"s","event":"APPROVE"}"#.to_string(),
            r#"{"kind":"submit_pending_review","review_id":9,"body":"s","event":"REQUEST_CHANGES"}"#.to_string(),
            r#"{"kind":"submit_pending_review","review_id":9,"body":"s","event":"COMMENT"}"#.to_string(),
            r#"{"kind":"approve_pull_request","review_id":9}"#.to_string(),
            r#"{"kind":"dismiss_review","review_id":9}"#.to_string(),
            r#"{"kind":"submit_pending_review","review_id":0,"body":"s"}"#.to_string(),
            r#"{"kind":"submit_pending_review","review_id":9,"body":"  "}"#.to_string(),
            format!(r#"{{"kind":"submit_pending_review","review_id":9,"body":"{big}"}}"#),
            // Daemon-only housekeeping never decodes from agent JSON.
            r#"{"kind":"list_pending_reviews"}"#.to_string(),
            r#"{"kind":"get_review","review_id":9}"#.to_string(),
            r#"{"kind":"delete_pending_review","review_id":9}"#.to_string(),
            // Create bounds.
            r#"{"kind":"create_pending_review","commit_id":"main","body":"m"}"#.to_string(),
            format!(r#"{{"kind":"create_pending_review","commit_id":"{}","body":"m"}}"#, SHA.to_uppercase()),
            format!(r#"{{"kind":"create_pending_review","commit_id":"{SHA}","body":""}}"#),
            format!(r#"{{"kind":"create_pending_review","commit_id":"{SHA}","body":"m","event":"APPROVE"}}"#),
            format!(r#"{{"kind":"create_pending_review","commit_id":"{SHA}","body":"m","pr":3}}"#),
            // Inline anchor bounds.
            comment(r#""line":0,"side":"RIGHT""#),
            comment(r#""line":3,"side":"BOTH""#),
            comment(r#""line":3,"side":"right""#),
            comment(r#""line":3,"side":"RIGHT","start_line":4"#),
            comment(r#""line":3,"side":"RIGHT","start_line":0"#),
            comment(r#""line":3,"side":"RIGHT","start_side":"LEFT""#),
            comment(r#""line":3,"side":"RIGHT","extra":1"#),
            comment(r#""line":-1,"side":"RIGHT""#),
            r#"{"kind":"add_pending_review_comment","review_node_id":"PRR_x","path":"a.rs","subject_type":"file","line":3,"side":"RIGHT","start_line":1,"body":"b"}"#.to_string(),
            r#"{"kind":"add_pending_review_comment","review_node_id":"PRR_x","path":"a.rs","subject_type":"hunk","line":3,"side":"RIGHT","body":"b"}"#.to_string(),
            r#"{"kind":"add_pending_review_comment","review_node_id":"a b","path":"a.rs","subject_type":"line","line":3,"side":"RIGHT","body":"b"}"#.to_string(),
            r#"{"kind":"add_pending_review_comment","review_node_id":"PRR_x","path":"/etc/passwd","subject_type":"line","line":3,"side":"RIGHT","body":"b"}"#.to_string(),
            r#"{"kind":"add_pending_review_comment","review_node_id":"PRR_x","path":"a\nb","subject_type":"line","line":3,"side":"RIGHT","body":"b"}"#.to_string(),
            r#"{"kind":"add_pending_review_comment","review_node_id":"PRR_x","path":"a.rs","subject_type":"line","line":3,"side":"RIGHT","body":""}"#.to_string(),
        ] {
            assert!(decode(&rejected).is_err(), "accepted {rejected}");
        }
        assert!(RepositoryRequest::decode_agent_json(
            b"{\"kind\":\"submit_pending_review\",\"review_id\":9,\"body\":\"a\0\"}"
        )
        .is_err());
        assert!(RepositoryRequest::decode_agent_json(
            b"{\"kind\":\"submit_pending_review\",\"review_id\":9,\"body\":\"\xff\"}"
        )
        .is_err());
        // A same-line range start and a file-level comment are in bounds.
        assert!(decode(&comment(
            r#""line":3,"side":"LEFT","start_line":3,"start_side":"LEFT""#
        ))
        .is_ok());
        assert!(decode(r#"{"kind":"add_pending_review_comment","review_node_id":"PRR_x","path":"a.rs","subject_type":"file","line":1,"side":"RIGHT","body":"b"}"#).is_ok());
    }

    #[test]
    fn pending_review_effects_fence_possibly_sent_writes() {
        let writes = [
            RepositoryRequest::CreatePendingReview {
                commit_id: SHA.into(),
                body: "m".into(),
            },
            inline("PRR_x"),
            RepositoryRequest::SubmitPendingReview {
                review_id: 9,
                body: "s".into(),
            },
            RepositoryRequest::DeletePendingReview { review_id: 9 },
        ];
        let lost = RepositoryError::new(RepositoryErrorKind::TimedOut, "lost");
        for request in &writes {
            assert_eq!(request.effect(), OperationEffect::Write, "{request:?}");
            assert_eq!(lost.outcome(request.effect()), OutcomeClass::PossiblySent);
        }
        for request in [
            RepositoryRequest::ListPendingReviews {},
            RepositoryRequest::GetReview { review_id: 9 },
        ] {
            assert_eq!(request.effect(), OperationEffect::Read, "{request:?}");
            assert_eq!(request.validate(), Ok(()));
        }
        assert!(RepositoryRequest::GetReview { review_id: 0 }
            .validate()
            .is_err());
        assert!(RepositoryRequest::DeletePendingReview { review_id: 0 }
            .validate()
            .is_err());
    }

    #[test]
    fn pending_review_argv_is_fixed_and_bodies_travel_on_stdin() {
        let target = RepositoryTarget::new(PathBuf::from("/tmp"), Some("acme/widgets")).unwrap();
        let body = "Body $(id) --method DELETE";
        let create = RepositoryRequest::CreatePendingReview {
            commit_id: SHA.into(),
            body: body.into(),
        };
        let submit = RepositoryRequest::SubmitPendingReview {
            review_id: 41,
            body: body.into(),
        };
        let cases: [(RepositoryRequest, &[&str]); 7] = [
            (
                create.clone(),
                &[
                    "api",
                    "--method",
                    "POST",
                    "repos/acme/widgets/pulls/7/reviews",
                    "--input",
                    "-",
                ],
            ),
            (
                submit.clone(),
                &[
                    "api",
                    "--method",
                    "POST",
                    "repos/acme/widgets/pulls/7/reviews/41/events",
                    "--input",
                    "-",
                ],
            ),
            (inline("PRR_x"), &["api", "graphql", "--input", "-"]),
            (
                RepositoryRequest::ListPendingReviews {},
                &[
                    "api",
                    "--paginate",
                    "repos/acme/widgets/pulls/7/reviews?per_page=100",
                    "--jq",
                    PENDING_REVIEW_FILTER,
                ],
            ),
            (
                RepositoryRequest::GetReview { review_id: 41 },
                &["api", "repos/acme/widgets/pulls/7/reviews/41"],
            ),
            (
                RepositoryRequest::DeletePendingReview { review_id: 41 },
                &[
                    "api",
                    "--method",
                    "DELETE",
                    "repos/acme/widgets/pulls/7/reviews/41",
                ],
            ),
            (
                RepositoryRequest::ListReviewComments { review_id: 41 },
                &[
                    "api",
                    "--paginate",
                    "repos/acme/widgets/pulls/7/reviews/41/comments?per_page=100",
                    "--jq",
                    EACH_ITEM_FILTER,
                ],
            ),
        ];
        for (request, expected) in &cases {
            let args = gh_args(&target, pr(7), request);
            assert_eq!(args, *expected, "{request:?}");
            assert!(!args.iter().any(|arg| arg.contains("$(id)")));
        }
        let input = |request: &RepositoryRequest| -> Value {
            serde_json::from_slice(&gh_input(request).unwrap()).unwrap()
        };
        // Create sends no event, so the review stays PENDING.
        assert_eq!(input(&create), json!({ "commit_id": SHA, "body": body }));
        // Submit can only ever send COMMENT.
        assert_eq!(input(&submit), json!({ "body": body, "event": "COMMENT" }));
        let graphql = input(&inline("PRR_x"));
        assert_eq!(graphql["query"], ADD_REVIEW_THREAD_MUTATION);
        assert_eq!(
            graphql["variables"]["input"],
            json!({
                "pullRequestReviewId": "PRR_x",
                "path": "src/lib.rs",
                "body": "Finding `x`",
                "subjectType": "LINE",
                "line": 12,
                "side": "RIGHT",
                "startLine": 10,
                "startSide": "RIGHT",
            })
        );
        let file = RepositoryRequest::AddPendingReviewComment {
            review_node_id: "PRR_x".into(),
            path: "a.rs".into(),
            line: 1,
            side: DiffSide::Left,
            start_line: None,
            start_side: None,
            subject_type: ReviewSubjectType::File,
            body: "b".into(),
        };
        assert_eq!(
            input(&file)["variables"]["input"],
            json!({ "pullRequestReviewId": "PRR_x", "path": "a.rs", "body": "b", "subjectType": "FILE" })
        );
        for request in [
            RepositoryRequest::ListPendingReviews {},
            RepositoryRequest::GetReview { review_id: 41 },
            RepositoryRequest::DeletePendingReview { review_id: 41 },
        ] {
            assert!(gh_input(&request).is_none());
        }
    }

    #[test]
    fn pending_review_responses_return_ids_and_full_bodies() {
        let create = RepositoryRequest::CreatePendingReview {
            commit_id: SHA.into(),
            body: "<!-- m -->".into(),
        };
        let parsed = parse_response(
            pr(7),
            &create,
            review_json(41, 7, "PENDING", "<!-- m -->").as_bytes(),
        )
        .unwrap();
        let RepositoryResponse::Review(review) = parsed else {
            panic!("{parsed:?}")
        };
        assert_eq!(
            (review.id, review.node_id.as_str(), review.state),
            (41, "PRR_41", ReviewState::Pending)
        );
        assert!(review.has_marker("<!-- m -->") && !review.has_marker(""));
        for bad in [
            review_json(41, 8, "PENDING", "m"),
            review_json(41, 7, "COMMENTED", "m"),
            review_json(41, 7, "WEIRD", "m"),
            review_json(0, 7, "PENDING", "m"),
            review_json(41, 7, "PENDING", &"x".repeat(MAX_RESPONSE_BODY_BYTES + 1)),
            "{}".to_string(),
        ] {
            assert!(
                parse_response(pr(7), &create, bad.as_bytes()).is_err(),
                "{bad}"
            );
        }
        let submit = RepositoryRequest::SubmitPendingReview {
            review_id: 41,
            body: "s".into(),
        };
        assert!(parse_response(
            pr(7),
            &submit,
            review_json(41, 7, "COMMENTED", "s").as_bytes()
        )
        .is_ok());
        assert!(parse_response(
            pr(7),
            &submit,
            review_json(41, 7, "APPROVED", "s").as_bytes()
        )
        .is_err());
        assert!(parse_response(
            pr(7),
            &submit,
            review_json(42, 7, "COMMENTED", "s").as_bytes()
        )
        .is_err());
        assert!(parse_response(
            pr(7),
            &RepositoryRequest::DeletePendingReview { review_id: 41 },
            review_json(42, 7, "PENDING", "s").as_bytes()
        )
        .is_err());

        let list = RepositoryRequest::ListPendingReviews {};
        let stream = format!(
            "{}\n{}\n",
            review_json(41, 7, "PENDING", "a"),
            review_json(42, 7, "PENDING", "b")
        );
        let RepositoryResponse::PendingReviews(reviews) =
            parse_response(pr(7), &list, stream.as_bytes()).unwrap()
        else {
            panic!()
        };
        assert_eq!(reviews.iter().map(|r| r.id).collect::<Vec<_>>(), [41, 42]);
        assert_eq!(
            parse_response(pr(7), &list, b"").unwrap(),
            RepositoryResponse::PendingReviews(Vec::new())
        );
        assert!(parse_response(
            pr(7),
            &list,
            review_json(41, 7, "COMMENTED", "a").as_bytes()
        )
        .is_err());
        let many: String = (1..=MAX_LISTED_PENDING_REVIEWS as u64 + 1)
            .map(|id| review_json(id, 7, "PENDING", "a"))
            .collect();
        assert!(parse_response(pr(7), &list, many.as_bytes()).is_err());

        let thread = |n: u64, review: &str| {
            json!({"data": {"addPullRequestReviewThread": {"thread": {
                "id": "PRRT_1", "pullRequest": {"number": n},
                "comments": {"nodes": [{"id": "PRRC_1", "databaseId": 77, "body": "Finding <!-- m -->",
                    "pullRequestReview": {"id": review, "databaseId": 41}}]}}}}})
            .to_string()
        };
        assert_eq!(
            parse_response(pr(7), &inline("PRR_41"), thread(7, "PRR_41").as_bytes()).unwrap(),
            RepositoryResponse::PendingReviewComment(CreatedReviewComment {
                id: 77,
                node_id: "PRRC_1".into(),
                thread_node_id: "PRRT_1".into(),
                review_node_id: "PRR_41".into(),
                body: "Finding <!-- m -->".into(),
            })
        );
        assert!(parse_response(pr(7), &inline("PRR_41"), thread(8, "PRR_41").as_bytes()).is_err());
        assert!(parse_response(pr(7), &inline("PRR_41"), thread(7, "PRR_99").as_bytes()).is_err());
        assert!(parse_response(pr(7), &inline("PRR_41"), br#"{"data":null}"#).is_err());
    }

    #[test]
    fn review_comment_listing_returns_full_bodies_of_the_exact_review_only() {
        let list = RepositoryRequest::ListReviewComments { review_id: 41 };
        assert_eq!(list.effect(), OperationEffect::Read);
        assert!(RepositoryRequest::decode_agent_json(
            br#"{"kind":"list_review_comments","review_id":41}"#
        )
        .is_err());
        let comment = |id: u64, review: u64, pr_number: u64, body: &str| {
            json!({
                "id": id, "node_id": format!("PRRC_{id}"), "body": body,
                "pull_request_review_id": review,
                "pull_request_url": format!("https://api.github.com/repos/acme/widgets/pulls/{pr_number}"),
            })
            .to_string()
        };
        let long = format!("{}<!-- quorum-op:1 -->", "x".repeat(MAX_BODY_FIELD_BYTES));
        let stream = format!("{}\n{}", comment(5, 41, 7, "a"), comment(6, 41, 7, &long));
        let parsed = parse_response(pr(7), &list, stream.as_bytes()).unwrap();
        let RepositoryResponse::ReviewComments(comments) = parsed else {
            panic!("{parsed:?}")
        };
        assert_eq!(comments.len(), 2);
        assert_eq!(
            (comments[1].id, comments[1].body.as_str()),
            (6, long.as_str())
        );
        assert!(parse_response(pr(7), &list, b"").is_ok());
        for bad in [
            comment(5, 40, 7, "a"),
            comment(5, 41, 8, "a"),
            comment(0, 41, 7, "a"),
        ] {
            assert!(
                parse_response(pr(7), &list, bad.as_bytes()).is_err(),
                "{bad}"
            );
        }
        let many: String = (1..=MAX_PENDING_REVIEW_COMMENTS as u64 + 1)
            .map(|id| comment(id, 41, 7, "a"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(parse_response(pr(7), &list, many.as_bytes()).is_err());
    }

    #[tokio::test]
    async fn exact_review_404_is_absence_only_for_housekeeping() {
        let dir = tempfile::tempdir().unwrap();
        let body = "echo 'gh: Not Found (HTTP 404)' >&2\nexit 1";
        let source = Arc::new(FakeCredentialSource::token(SECRET));
        let service = service(
            &source,
            script(dir.path(), body),
            Duration::from_secs(10),
            DEFAULT_RESPONSE_BYTE_LIMIT,
        );
        let target = target(dir.path());
        for request in [
            RepositoryRequest::GetReview { review_id: 41 },
            RepositoryRequest::DeletePendingReview { review_id: 41 },
        ] {
            assert_eq!(
                service.execute(&target, pr(7), &request).await.unwrap(),
                RepositoryResponse::ReviewAbsent { review_id: 41 }
            );
        }
        let error = service
            .execute(
                &target,
                pr(7),
                &RepositoryRequest::SubmitPendingReview {
                    review_id: 41,
                    body: "s".into(),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, RepositoryErrorKind::PermanentFailure);
    }

    #[tokio::test]
    async fn fake_service_simulates_pending_review_lifecycle_and_ambiguity() {
        let dir = tempfile::tempdir().unwrap();
        let target = target(dir.path());
        let create = RepositoryRequest::CreatePendingReview {
            commit_id: SHA.into(),
            body: "<!-- owner -->".into(),
        };
        let list = RepositoryRequest::ListPendingReviews {};
        // A create whose response is lost still lands; a second create is
        // refused because GitHub allows one pending review per publisher.
        let fake = FakeRepositoryService::scripted([
            FakeOutcome::lost_response(),
            FakeOutcome::Apply,
            FakeOutcome::Apply,
        ]);
        let error = fake.execute(&target, pr(7), &create).await.unwrap_err();
        assert_eq!(error.outcome(create.effect()), OutcomeClass::PossiblySent);
        assert_eq!(
            fake.execute(&target, pr(7), &create)
                .await
                .unwrap_err()
                .kind,
            RepositoryErrorKind::PermanentFailure
        );
        let RepositoryResponse::PendingReviews(found) =
            fake.execute(&target, pr(7), &list).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(found.len(), 1);
        assert!(found[0].has_marker("<!-- owner -->"));

        // The orphan draft survives a daemon restart (a fresh fake over the
        // same remote state); the new daemon reconciles and deletes it.
        let remote = fake.reviews();
        let orphan = found[0].clone();
        let restarted = FakeRepositoryService::scripted([
            FakeOutcome::Apply,
            FakeOutcome::Apply,
            FakeOutcome::ambiguous(),
            FakeOutcome::Apply,
            FakeOutcome::Apply,
            FakeOutcome::Apply,
        ])
        .with_reviews(remote);
        assert_eq!(
            restarted.execute(&target, pr(7), &list).await.unwrap(),
            RepositoryResponse::PendingReviews(vec![orphan.clone()])
        );
        assert_eq!(
            restarted
                .execute(
                    &target,
                    pr(7),
                    &RepositoryRequest::GetReview {
                        review_id: orphan.id
                    }
                )
                .await
                .unwrap(),
            RepositoryResponse::Review(orphan.clone())
        );
        let delete = RepositoryRequest::DeletePendingReview {
            review_id: orphan.id,
        };
        // An ambiguous delete has no effect in the fake but is fenced.
        let error = restarted
            .execute(&target, pr(7), &delete)
            .await
            .unwrap_err();
        assert_eq!(error.outcome(delete.effect()), OutcomeClass::PossiblySent);
        assert_eq!(restarted.reviews().reviews.len(), 1);
        assert_eq!(
            restarted.execute(&target, pr(7), &delete).await.unwrap(),
            RepositoryResponse::Review(orphan.clone())
        );
        assert_eq!(
            restarted.execute(&target, pr(7), &delete).await.unwrap(),
            RepositoryResponse::ReviewAbsent {
                review_id: orphan.id
            }
        );
        assert_eq!(
            restarted.execute(&target, pr(7), &list).await.unwrap(),
            RepositoryResponse::PendingReviews(Vec::new())
        );

        // Full create → inline → submit on a clean slot; a submitted review
        // takes no more inline comments and cannot be deleted.
        let fake =
            FakeRepositoryService::scripted(std::iter::repeat_with(|| FakeOutcome::Apply).take(6));
        let RepositoryResponse::Review(review) =
            fake.execute(&target, pr(7), &create).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(review.state, ReviewState::Pending);
        let RepositoryResponse::PendingReviewComment(comment) = fake
            .execute(&target, pr(7), &inline(&review.node_id))
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(comment.review_node_id, review.node_id);
        let submit = RepositoryRequest::SubmitPendingReview {
            review_id: review.id,
            body: "Summary <!-- owner -->".into(),
        };
        let RepositoryResponse::Review(submitted) =
            fake.execute(&target, pr(7), &submit).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(submitted.state, ReviewState::Commented);
        assert!(submitted.has_marker("<!-- owner -->"));
        assert!(fake
            .execute(&target, pr(7), &inline(&review.node_id))
            .await
            .is_err());
        assert!(fake
            .execute(
                &target,
                pr(7),
                &RepositoryRequest::DeletePendingReview {
                    review_id: review.id
                }
            )
            .await
            .is_err());
        assert_eq!(
            fake.execute(&target, pr(7), &list).await.unwrap(),
            RepositoryResponse::PendingReviews(Vec::new())
        );
        assert_eq!(fake.reviews().reviews[0].comments, vec![comment]);
    }

    #[test]
    fn fake_store_bounds_inline_comments_per_review() {
        let mut store = FakeReviewStore::default();
        let id = store.seed_pending(pr(7), SHA, "m");
        let node = format!("PRR_fake{id}");
        for _ in 0..MAX_PENDING_REVIEW_COMMENTS {
            store.apply(pr(7), &inline(&node)).unwrap();
        }
        assert_eq!(
            store.apply(pr(7), &inline(&node)).unwrap_err().kind,
            RepositoryErrorKind::PermanentFailure
        );
        // Another PR does not see this pending review.
        assert_eq!(
            store
                .apply(pr(8), &RepositoryRequest::ListPendingReviews {})
                .unwrap(),
            RepositoryResponse::PendingReviews(Vec::new())
        );
    }

    /// One projected thread as `REVIEW_THREADS_FILTER` streams it.
    fn thread_line(id: &str, ids: &[u64], resolved: bool, outdated: bool) -> String {
        json!({
            "id": id, "isResolved": resolved, "isOutdated": outdated, "path": "src/lib.rs",
            "line": if outdated { Value::Null } else { json!(3) }, "originalLine": 3,
            "startLine": null, "diffSide": "RIGHT", "subjectType": "LINE", "ids": ids,
            "root": {"databaseId": ids[0], "diffHunk": "@@ -1,3 +1,3 @@\n a\n-b\n+c",
                     "commit": if outdated { Value::Null } else { json!({"oid": SHA}) },
                     "originalCommit": {"oid": SHA}},
        })
        .to_string()
    }

    #[test]
    fn review_thread_requests_are_closed_and_text_never_reaches_argv() {
        let decode = |text: &str| RepositoryRequest::decode_agent_json(text.as_bytes());
        assert_eq!(
            decode(r#"{"kind":"reply_to_review_comment","comment_id":5,"body":"done"}"#).unwrap(),
            RepositoryRequest::ReplyToReviewComment {
                comment_id: 5,
                body: "done".into()
            }
        );
        assert_eq!(
            decode(r#"{"kind":"resolve_review_thread","thread_node_id":"PRRT_k1"}"#).unwrap(),
            RepositoryRequest::ResolveReviewThread {
                thread_node_id: "PRRT_k1".into()
            }
        );
        for rejected in [
            r#"{"kind":"reply_to_review_comment","comment_id":0,"body":"x"}"#,
            r#"{"kind":"reply_to_review_comment","comment_id":5,"body":"  "}"#,
            r#"{"kind":"reply_to_review_comment","comment_id":5,"body":"x","pr":9}"#,
            r#"{"kind":"resolve_review_thread","thread_node_id":"a b"}"#,
            r#"{"kind":"resolve_review_thread","thread_node_id":""}"#,
            // Daemon-only reads never decode from agent JSON.
            r#"{"kind":"list_review_threads"}"#,
            r#"{"kind":"list_comment_replies","comment_id":5}"#,
        ] {
            assert!(decode(rejected).is_err(), "{rejected}");
        }
        let reply = RepositoryRequest::ReplyToReviewComment {
            comment_id: 5,
            body: "Fixed `x` in $(id)".into(),
        };
        let resolve = RepositoryRequest::ResolveReviewThread {
            thread_node_id: "PRRT_k1".into(),
        };
        assert_eq!(reply.effect(), OperationEffect::Write);
        assert_eq!(resolve.effect(), OperationEffect::Write);
        assert_eq!(
            RepositoryRequest::ListReviewThreads {}.effect(),
            OperationEffect::Read
        );
        assert_eq!(
            RepositoryRequest::ListCommentReplies { comment_id: 5 }.effect(),
            OperationEffect::Read
        );

        let slug = RepositoryTarget::new(PathBuf::from("/tmp"), Some("acme/widgets")).unwrap();
        let local = RepositoryTarget::new(PathBuf::from("/tmp"), None).unwrap();
        assert_eq!(
            gh_args(&slug, pr(9), &reply),
            [
                "api",
                "--method",
                "POST",
                "repos/acme/widgets/pulls/9/comments/5/replies",
                "--input",
                "-"
            ]
        );
        let sent: Value = serde_json::from_slice(&gh_input(&reply).unwrap()).unwrap();
        assert_eq!(sent, json!({"body": "Fixed `x` in $(id)"}));
        assert_eq!(
            gh_args(&slug, pr(9), &resolve),
            ["api", "graphql", "--input", "-"]
        );
        let sent: Value = serde_json::from_slice(&gh_input(&resolve).unwrap()).unwrap();
        assert_eq!(sent["variables"], json!({"threadId": "PRRT_k1"}));
        assert_eq!(sent["query"], RESOLVE_THREAD_MUTATION);
        let threads = gh_args(&slug, pr(9), &RepositoryRequest::ListReviewThreads {});
        assert_eq!(threads[..3], ["api", "graphql", "--paginate"]);
        for pair in [
            ["-f", "owner=acme"],
            ["-f", "name=widgets"],
            ["-F", "number=9"],
            ["--jq", REVIEW_THREADS_FILTER],
        ] {
            assert!(
                threads.windows(2).any(|w| w == pair),
                "{pair:?} in {threads:?}"
            );
        }
        let threads = gh_args(&local, pr(9), &RepositoryRequest::ListReviewThreads {});
        assert!(threads.windows(2).any(|w| w == ["-F", "owner={owner}"]));
        assert!(threads.windows(2).any(|w| w == ["-F", "name={repo}"]));
        assert_eq!(
            gh_args(
                &slug,
                pr(9),
                &RepositoryRequest::ListCommentReplies { comment_id: 5 }
            ),
            [
                "api",
                "--paginate",
                "repos/acme/widgets/pulls/9/comments?per_page=100",
                "--jq",
                ".[] | select(.in_reply_to_id == 5)"
            ]
        );
        for request in [&reply, &resolve] {
            assert!(!gh_args(&slug, pr(9), request).join(" ").contains("Fixed"));
        }
    }

    #[test]
    fn review_thread_responses_are_bound_to_the_target_pr() {
        let listing = format!(
            "{}\n{}\n",
            thread_line("PRRT_a", &[11, 12], false, false),
            thread_line("PRRT_b", &[21], true, true)
        );
        let RepositoryResponse::ReviewThreads(threads) = parse_response(
            pr(9),
            &RepositoryRequest::ListReviewThreads {},
            listing.as_bytes(),
        )
        .unwrap() else {
            panic!("not a thread listing")
        };
        assert_eq!(threads.len(), 2);
        assert_eq!(threads[0].node_id, "PRRT_a");
        assert_eq!(threads[0].comment_ids, vec![11, 12]);
        assert_eq!(threads[0].root_comment_id, 11);
        assert_eq!(threads[0].commit_id.as_deref(), Some(SHA));
        assert!(threads[0].diff_hunk.ends_with("+c"));
        assert!(threads[1].resolved && threads[1].outdated);
        assert_eq!(threads[1].line, None);
        assert_eq!(threads[1].commit_id, None);
        // A root that is not the first comment, or a bad commit, fails closed.
        let mut bad: Value =
            serde_json::from_str(&thread_line("PRRT_c", &[31], false, false)).unwrap();
        bad["root"]["databaseId"] = json!(32);
        let bad_commit = thread_line("PRRT_d", &[41], false, false).replace(SHA, "not-a-sha");
        for invalid in [bad.to_string(), bad_commit] {
            assert!(parse_response(
                pr(9),
                &RepositoryRequest::ListReviewThreads {},
                invalid.as_bytes()
            )
            .is_err());
        }

        let reply = RepositoryRequest::ReplyToReviewComment {
            comment_id: 11,
            body: "x".into(),
        };
        let created = |url: &str, in_reply_to: u64| {
            json!({"id": 13, "node_id": "PRRC_13", "in_reply_to_id": in_reply_to,
                   "body": "x <!-- m -->", "pull_request_url": url})
            .to_string()
        };
        let own = "https://api.github.com/repos/acme/widgets/pulls/9";
        assert_eq!(
            parse_response(pr(9), &reply, created(own, 11).as_bytes()).unwrap(),
            RepositoryResponse::ReviewReply(ReviewReplyRecord {
                id: 13,
                node_id: "PRRC_13".into(),
                in_reply_to_id: 11,
                body: "x <!-- m -->".into(),
            })
        );
        let other = "https://api.github.com/repos/acme/widgets/pulls/10";
        assert!(parse_response(pr(9), &reply, created(other, 11).as_bytes()).is_err());
        assert!(parse_response(pr(9), &reply, created(own, 12).as_bytes()).is_err());

        let resolve = RepositoryRequest::ResolveReviewThread {
            thread_node_id: "PRRT_a".into(),
        };
        let resolved = |id: &str, number: u64, done: bool| {
            json!({"data": {"resolveReviewThread": {"thread": {
                "id": id, "isResolved": done, "pullRequest": {"number": number}}}}})
            .to_string()
        };
        assert_eq!(
            parse_response(pr(9), &resolve, resolved("PRRT_a", 9, true).as_bytes()).unwrap(),
            RepositoryResponse::ThreadResolved {
                thread_node_id: "PRRT_a".into()
            }
        );
        for invalid in [
            resolved("PRRT_a", 10, true),
            resolved("PRRT_b", 9, true),
            resolved("PRRT_a", 9, false),
        ] {
            assert!(parse_response(pr(9), &resolve, invalid.as_bytes()).is_err());
        }
    }

    #[test]
    fn review_comments_carry_their_thread_id_and_github_state() {
        let threads = [ReviewThreadRecord {
            node_id: "PRRT_a".into(),
            resolved: true,
            outdated: true,
            path: "src/lib.rs".into(),
            line: Some(3),
            original_line: Some(3),
            start_line: None,
            side: Some("RIGHT".into()),
            subject_type: Some("LINE".into()),
            root_comment_id: 11,
            comment_ids: vec![11, 12],
            commit_id: None,
            original_commit_id: None,
            diff_hunk: String::new(),
        }];
        let comments = json!([
            {"id": 12, "path": "src/lib.rs", "line": 3, "body": "reply"},
            {"id": 99, "path": "src/lib.rs", "line": null, "body": "unthreaded"},
        ])
        .to_string();
        let request = RepositoryRequest::GetReviewComments {
            page: 1,
            per_page: 30,
        };
        let RepositoryResponse::PullRequestRead(page) =
            parse_response_with_threads(pr(9), &request, comments.as_bytes(), &threads).unwrap()
        else {
            panic!("not a read")
        };
        let items = page["items"].as_array().unwrap();
        assert_eq!(items[0]["thread_id"], "PRRT_a");
        assert_eq!(items[0]["resolved"], true);
        // GitHub's own outdated state wins over the current-diff line.
        assert_eq!(items[0]["outdated"], true);
        assert_eq!(items[1]["thread_id"], Value::Null);
        assert_eq!(items[1]["resolved"], Value::Null);
        assert_eq!(items[1]["outdated"], true);
    }
}
