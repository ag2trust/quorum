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
            Self::AddIssueComment { body } => {
                !body.trim().is_empty() && body.len() <= MAX_COMMENT_BODY_BYTES
            }
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
            Self::AddIssueComment { .. } => OperationEffect::Write,
            Self::ReadPrMetadata {}
            | Self::GetPullRequest {}
            | Self::GetDiff { .. }
            | Self::GetFiles { .. }
            | Self::GetReviewComments { .. }
            | Self::GetReviews { .. }
            | Self::GetComments { .. } => OperationEffect::Read,
        }
    }
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedComment {
    pub id: u64,
    pub html_url: String,
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

    async fn run(
        &self,
        target: &RepositoryTarget,
        pr: PullRequestNumber,
        request: &RepositoryRequest,
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
            return Err(RepositoryError::new(classify_gh_failure(&stderr), &stderr));
        }
        parse_response(pr, request, &output.stdout).map_err(|reason| {
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
            let mut args = vec![
                "api".to_string(),
                "--method".to_string(),
                "POST".to_string(),
            ];
            args.push(format!("repos/{repo}/issues/{n}/comments"));
            args.extend(["--input".to_string(), "-".to_string()]);
            args
        }
    }
}

/// The request document streamed to `gh` on stdin. Only a comment has one:
/// its body travels as a JSON string, byte for byte.
fn gh_input(request: &RepositoryRequest) -> Option<Vec<u8>> {
    match request {
        RepositoryRequest::AddIssueComment { body } => {
            Some(json!({ "body": body }).to_string().into_bytes())
        }
        _ => None,
    }
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

fn parse_response(
    pr: PullRequestNumber,
    request: &RepositoryRequest,
    stdout: &[u8],
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
            parse_list::<GhReviewComment>(stdout)
                .map(|v| v.into_iter().map(review_comment_item).collect()),
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
    }
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

fn review_comment_item(comment: GhReviewComment) -> Item {
    let mut clipped = false;
    let short = MAX_SHORT_FIELD_BYTES;
    let value = json!({
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
        // GitHub drops the current-diff anchor of an outdated comment.
        "outdated": comment.line.is_none() && comment.subject_type.as_deref() != Some("file"),
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
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub enum FakeOutcome {
    Succeed(RepositoryResponse),
    Fail(RepositoryErrorKind),
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
        }
    }

    pub fn calls(&self) -> Vec<FakeCall> {
        self.calls.lock().unwrap().clone()
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
        Box::pin(async move {
            match outcome {
                FakeOutcome::Succeed(response) => Ok(response),
                FakeOutcome::Fail(kind) => Err(RepositoryError::new(kind, "fake outcome")),
            }
        })
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
}
