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

// The executor that consumes this boundary lands in a follow-up slice; the
// contract tests below exercise the runtime API directly.
#![allow(dead_code)]

use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use super::{publication_github_token, run_bounded_gh_command, BoundedGhError};
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
        serde_json::from_str(text).map_err(|error| RequestDecodeError(bounded(&error.to_string())))
    }

    pub fn effect(&self) -> OperationEffect {
        match self {
            Self::ReadPrMetadata {} => OperationEffect::Read,
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
    /// A transport/5xx/rate-limit style failure after spawn.
    TransientFailure,
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

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn delivery(&self) -> Delivery {
        match self.kind {
            RepositoryErrorKind::CredentialUnavailable | RepositoryErrorKind::SpawnFailed => {
                Delivery::NotSent
            }
            RepositoryErrorKind::PermanentFailure => Delivery::Rejected,
            RepositoryErrorKind::TimedOut
            | RepositoryErrorKind::ResponseTooLarge
            | RepositoryErrorKind::TransientFailure
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
                RepositoryErrorKind::TimedOut | RepositoryErrorKind::TransientFailure => {
                    OutcomeClass::Retryable
                }
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
        let output = run_bounded_gh_command(command, self.timeout, self.response_byte_limit)
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
        match request {
            RepositoryRequest::ReadPrMetadata {} => parse_pr_metadata(pr, &output.stdout)
                .map(RepositoryResponse::PrMetadata)
                .map_err(|reason| {
                    RepositoryError::new(RepositoryErrorKind::InvalidResponse, &redact(&reason))
                }),
        }
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
/// with the daemon-supplied PR and optional repository slug.
fn gh_args(
    target: &RepositoryTarget,
    pr: PullRequestNumber,
    request: &RepositoryRequest,
) -> Vec<String> {
    let mut args = match request {
        RepositoryRequest::ReadPrMetadata {} => vec![
            "pr".to_string(),
            "view".to_string(),
            pr.get().to_string(),
            "--json".to_string(),
            "number,state,headRefName,headRefOid,baseRefName,isCrossRepository".to_string(),
        ],
    };
    if let Some(slug) = target.gh_repo() {
        args.push("--repo".to_string());
        args.push(slug.to_string());
    }
    args
}

/// Nonzero `gh` exits whose stderr names a transport, 5xx, or rate-limit
/// condition are transient; anything else is a definitive rejection.
fn classify_gh_failure(stderr: &str) -> RepositoryErrorKind {
    const TRANSIENT_MARKERS: &[&str] = &[
        "http 500",
        "http 502",
        "http 503",
        "http 504",
        "rate limit",
        "timeout",
        "timed out",
        "connection",
        "tls handshake",
        "unexpected eof",
        "temporarily unavailable",
        "something went wrong",
    ];
    let lower = stderr.to_ascii_lowercase();
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
            (
                "API rate limit exceeded",
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
}
