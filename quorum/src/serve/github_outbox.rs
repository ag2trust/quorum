//! The daemon's single bounded executor loop for `github_agent_operations`.
//!
//! Lifecycle per daemon:
//! 1. [`OutboxExecutor::run`] first calls
//!    [`github_operations::reconcile_on_start`], so every claim left by a
//!    previous daemon is resolved before this daemon's first claim. A write
//!    whose send may have reached GitHub settles `failed/ambiguous` there and
//!    is never re-sent; reads and never-sent writes are re-queued.
//! 2. Each polling tick claims due operations only while an in-flight permit
//!    is free ([`MAX_IN_FLIGHT`]), so claimed-but-unsettled work never exceeds
//!    that bound and there is no unbounded internal queue.
//! 3. Each claimed operation is decoded into the closed
//!    [`RepositoryRequest`], stamped execution-started, executed through the
//!    [`RepositoryService`] with **no** DB transaction open, and settled or
//!    released for retry. Claim, start, settle, and retry are each one short
//!    `BEGIN IMMEDIATE` on a fresh short-lived connection (core primitives);
//!    nothing holds a connection or read transaction across ticks.
//!
//! Target authority: the repository comes from daemon config
//! ([`RepositoryTarget`]) and the pull request from the operation row's
//! capability-derived `pr_number` snapshot (written by
//! `collaboration_admission::admit_operation` from the live run binding).
//! Nothing in the stored request can name a target.
//!
//! Pending-review steps (create, inline comments, `submit_pending`) run
//! through the same claim/start/settle primitives, in the review sequence
//! order `claim_next` enforces, via [`prepare_review`]: the create body is the
//! attempt's hidden owner marker (after any authored text), inline comments
//! carry their operation marker, and the COMMENT submit appends the owner
//! marker after the authored summary. A possibly-sent result is reconciled
//! with one exact Repository Service read (marker, review ID, PR, launch SHA)
//! and is never re-sent; without marker evidence it settles `ambiguous`. Only
//! a closed pre-send proof records `definitely_unsent`.
//!
//! Shutdown: the shared flag stops new claims. In-flight executions are
//! awaited (not raced) up to a bounded grace; any still running after it are
//! dropped with the runtime. Every state transition is a committed SQLite
//! transaction, so a dropped execution leaves its row `running` and
//! reconcilable on the next start (ambiguous for a started write, re-queued
//! for a read), never double-executed by this daemon.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use quorum_core::collaboration_admission::{
    GithubOperationKind, GithubOperationResponseJson, OperationErrorSummary, RemoteObjectId,
    MAX_OPERATION_ERROR_BYTES, MAX_OPERATION_JSON_BYTES,
};
use quorum_core::error::{QuorumError, Result};
use quorum_core::github_operations::{
    self, ClaimedOperation, OperationClaim, RetryOutcome, Settlement, MAX_OPERATION_CLAIMS,
};
use quorum_core::review_publication_slots::{self, ReviewOwnerMarker, SlotState};
use quorum_core::Connection;
use rusqlite::OptionalExtension;
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::repository_service::{
    CreatedComment, Delivery, DiffSide, OperationEffect, OutcomeClass, PrMetadata, PrState,
    PullRequestNumber, RepositoryError, RepositoryRequest, RepositoryResponse, RepositoryService,
    RepositoryTarget, ReviewRecord, ReviewState, ReviewSubjectType, ReviewThreadRecord,
    DEFAULT_CALL_TIMEOUT, MAX_AGENT_REQUEST_BYTES,
};
use super::{log, now_unix};

/// Maximum operations claimed and unsettled at once (executor concurrency and
/// in-flight queue length; claims happen only while a permit is free).
pub const MAX_IN_FLIGHT: usize = 4;
/// Maximum executions per operation (each claim is one attempt; the core
/// claim/retry primitives settle the row `failed` at this cap).
pub const MAX_EXECUTION_ATTEMPTS: i64 = MAX_OPERATION_CLAIMS;
/// Largest stored request accepted for decode.
pub const MAX_REQUEST_BYTES: usize = MAX_AGENT_REQUEST_BYTES;
/// Largest serialized result retained on the row.
pub const MAX_RESULT_BYTES: usize = MAX_OPERATION_JSON_BYTES;
/// Retry backoff: `RETRY_BASE_SECS * 2^(attempt-1)`, capped.
pub const RETRY_BASE_SECS: i64 = 5;
pub const RETRY_MAX_SECS: i64 = 120;
/// Idle polling interval between claim ticks.
pub const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Bound on awaiting in-flight executions after shutdown is requested: one
/// send plus one reconciliation read at the Repository Service call
/// deadline, plus slack for the settle.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(2 * DEFAULT_CALL_TIMEOUT.as_secs() + 5);

const LOG_SOURCE: &str = "github_outbox";

/// One daemon's outbox executor. Construct once per daemon and drive with
/// [`OutboxExecutor::run`].
pub struct OutboxExecutor {
    db_path: PathBuf,
    target: RepositoryTarget,
    service: Arc<dyn RepositoryService>,
    poll_interval: Duration,
    shutdown_grace: Duration,
}

impl OutboxExecutor {
    pub fn new(
        db_path: PathBuf,
        target: RepositoryTarget,
        service: Arc<dyn RepositoryService>,
    ) -> Self {
        Self {
            db_path,
            target,
            service,
            poll_interval: POLL_INTERVAL,
            shutdown_grace: SHUTDOWN_GRACE,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_timing(mut self, poll_interval: Duration, shutdown_grace: Duration) -> Self {
        self.poll_interval = poll_interval;
        self.shutdown_grace = shutdown_grace;
        self
    }

    /// Reconcile, then claim/execute/settle until `shutdown` is set. Returns
    /// an error only when startup reconciliation fails, in which case no
    /// operation is claimed.
    pub async fn run(self, shutdown: Arc<AtomicBool>) -> Result<()> {
        let report = with_conn(&self.db_path, |conn| {
            github_operations::reconcile_on_start(conn, now_unix())
        })
        .await?;
        if report != github_operations::ReconcileReport::default() {
            log(&format!(
                "github outbox: startup reconciliation requeued={} ambiguous={} failed={} slots={}",
                report.requeued, report.ambiguous, report.failed, report.slots
            ));
        }

        let ctx = Arc::new(ExecContext {
            db_path: self.db_path.clone(),
            target: self.target.clone(),
            service: self.service.clone(),
        });
        let permits = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
        let mut in_flight: JoinSet<()> = JoinSet::new();
        while !shutdown.load(Ordering::SeqCst) {
            while let Some(joined) = in_flight.try_join_next() {
                report_join(joined);
            }
            self.claim_available(&ctx, &permits, &mut in_flight, &shutdown)
                .await;
            // Plain sleep, not a select! race: the flag is re-read each tick.
            tokio::time::sleep(self.poll_interval).await;
        }

        // No new claims from here. Await in-flight work up to the grace; the
        // only raced future is `join_next`, which is cancellation-safe.
        let deadline = tokio::time::Instant::now() + self.shutdown_grace;
        loop {
            match tokio::time::timeout_at(deadline, in_flight.join_next()).await {
                Ok(Some(joined)) => report_join(joined),
                Ok(None) => break,
                Err(_) => {
                    log(&format!(
                        "github outbox: {} execution(s) still in flight at shutdown; \
                         left for startup reconciliation",
                        in_flight.len()
                    ));
                    break;
                }
            }
        }
        Ok(())
    }

    async fn claim_available(
        &self,
        ctx: &Arc<ExecContext>,
        permits: &Arc<Semaphore>,
        in_flight: &mut JoinSet<()>,
        shutdown: &AtomicBool,
    ) {
        while !shutdown.load(Ordering::SeqCst) {
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                return;
            };
            match claim_due(&self.db_path).await {
                Ok(Some(claimed)) => {
                    let ctx = ctx.clone();
                    in_flight.spawn(async move {
                        let _permit = permit;
                        if let Err(error) = execute_claimed(&ctx, claimed).await {
                            log_abnormal(&ctx.db_path, &format!("execution: {error}")).await;
                        }
                    });
                }
                Ok(None) => return,
                Err(error) => {
                    log_abnormal(&self.db_path, &format!("claim: {error}")).await;
                    return;
                }
            }
        }
    }
}

struct ExecContext {
    db_path: PathBuf,
    target: RepositoryTarget,
    service: Arc<dyn RepositoryService>,
}

fn report_join(joined: std::result::Result<(), tokio::task::JoinError>) {
    if let Err(error) = joined {
        log(&format!("github outbox: execution task failed: {error}"));
    }
}

async fn log_abnormal(db_path: &Path, detail: &str) {
    log(&format!("github outbox: {detail}"));
    let detail = detail.to_string();
    let _ = with_conn(db_path, move |conn| {
        quorum_core::errlog::log_error(conn, now_unix(), LOG_SOURCE, &detail);
        Ok(())
    })
    .await;
}

/// Run `f` on a fresh connection in a blocking thread. The connection is
/// closed when `f` returns, so no read transaction outlives one primitive.
async fn with_conn<T, F>(db_path: &Path, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
{
    let db_path = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut conn = quorum_core::db::open(&db_path)?;
        f(&mut conn)
    })
    .await
    .map_err(|error| QuorumError::Io(format!("github outbox DB task failed: {error}")))?
}

/// Claim the next due operation. A single autocommit existence probe keeps
/// idle ticks off the write lock; the claim itself is the core primitive.
async fn claim_due(db_path: &Path) -> Result<Option<ClaimedOperation>> {
    with_conn(db_path, |conn| {
        let queued: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM github_agent_operations
                           WHERE state='queued' AND active=0)",
            [],
            |row| row.get(0),
        )?;
        if !queued {
            return Ok(None);
        }
        github_operations::claim_next(conn, now_unix())
    })
    .await
}

/// What the executor records for one execution result.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Disposition {
    /// Confirmed, with the created GitHub object's id when there is one.
    Succeeded(GithubOperationResponseJson, Option<RemoteObjectId>),
    /// Release for another attempt. `proven_unsent` is the closed transport
    /// proof that nothing was handed to GitHub.
    Retry {
        error_kind: &'static str,
        summary: String,
        proven_unsent: bool,
    },
    /// Terminal failure with no remote effect.
    FailedUnsent {
        error_kind: &'static str,
        summary: String,
    },
    /// Terminal failure whose remote effect is unknown; writes stay
    /// `ambiguous` and are never re-sent.
    Ambiguous {
        error_kind: &'static str,
        summary: String,
    },
}

/// Decode the stored request into the closed vocabulary and check it is the
/// operation its row `kind` names. Anything else fails before any send.
fn decode_stored(
    kind: &str,
    request_json: &str,
) -> std::result::Result<RepositoryRequest, Disposition> {
    if request_json.len() > MAX_REQUEST_BYTES {
        return Err(Disposition::FailedUnsent {
            error_kind: "request_too_large",
            summary: format!("stored request exceeds {MAX_REQUEST_BYTES} bytes"),
        });
    }
    let request =
        RepositoryRequest::decode_agent_json(request_json.as_bytes()).map_err(|error| {
            Disposition::FailedUnsent {
                error_kind: "unsupported_request",
                summary: error.to_string(),
            }
        })?;
    let expected = match request {
        RepositoryRequest::ReadPrMetadata {}
        | RepositoryRequest::GetPullRequest {}
        | RepositoryRequest::GetDiff { .. }
        | RepositoryRequest::GetFiles { .. }
        | RepositoryRequest::GetReviewComments { .. }
        | RepositoryRequest::GetReviews { .. }
        | RepositoryRequest::GetComments { .. } => GithubOperationKind::PullRequestRead,
        RepositoryRequest::AddIssueComment { .. } => GithubOperationKind::AddIssueComment,
        // Executed only by `execute_thread_write`, which first resolves the
        // target thread on the bound PR.
        RepositoryRequest::ReplyToReviewComment { .. } => {
            GithubOperationKind::AddReplyToPullRequestComment
        }
        RepositoryRequest::ResolveReviewThread { .. } => GithubOperationKind::ResolveReviewThread,
        // Pending-review steps are stored as endpoint requests and built by
        // `prepare_review` from daemon slot state; one stored in this closed
        // vocabulary is refused before any send. Housekeeping variants are
        // daemon-only and never decode from a stored agent request.
        RepositoryRequest::CreatePendingReview { .. }
        | RepositoryRequest::AddPendingReviewComment { .. }
        | RepositoryRequest::SubmitPendingReview { .. }
        | RepositoryRequest::ListPendingReviews {}
        | RepositoryRequest::GetReview { .. }
        | RepositoryRequest::DeletePendingReview { .. }
        | RepositoryRequest::ListReviewComments { .. }
        | RepositoryRequest::ListReviewThreads {}
        | RepositoryRequest::ListCommentReplies { .. } => {
            return Err(Disposition::FailedUnsent {
                error_kind: "unsupported_request",
                summary: "pending-review requests are executable only as review steps".to_string(),
            })
        }
    };
    let matches_kind = kind == expected.as_str();
    if !matches_kind {
        return Err(Disposition::FailedUnsent {
            error_kind: "request_kind_mismatch",
            summary: format!("stored request does not match operation kind {kind}"),
        });
    }
    Ok(request)
}

/// Every write carries the operation's hidden marker. A comment without one
/// fails before any send rather than posting an unattributable body.
fn attach_marker(
    request: RepositoryRequest,
    marker: Option<&str>,
) -> std::result::Result<RepositoryRequest, Disposition> {
    match request {
        RepositoryRequest::AddIssueComment { body } => match marker {
            Some(marker) if !marker.is_empty() => Ok(RepositoryRequest::AddIssueComment {
                body: compose_comment_body(&body, marker),
            }),
            _ => Err(Disposition::FailedUnsent {
                error_kind: "missing_marker",
                summary: "write operation has no daemon marker".to_string(),
            }),
        },
        request => Ok(request),
    }
}

/// Fail-safe effect: an operation is a read only when both the row kind and
/// the decoded request say so.
fn effect_of(kind: &str, request: &RepositoryRequest) -> OperationEffect {
    if kind == GithubOperationKind::PullRequestRead.as_str()
        && request.effect() == OperationEffect::Read
    {
        OperationEffect::Read
    } else {
        OperationEffect::Write
    }
}

fn error_kind_token(error: &RepositoryError) -> &'static str {
    use super::repository_service::RepositoryErrorKind as K;
    match error.kind {
        K::CredentialUnavailable => "credential_unavailable",
        K::SpawnFailed => "spawn_failed",
        K::TimedOut => "timed_out",
        K::ResponseTooLarge => "response_too_large",
        K::TransientFailure => "transient_failure",
        K::RateLimited => "rate_limited",
        K::PermanentFailure => "permanent_failure",
        K::InvalidResponse => "invalid_response",
    }
}

/// Map one Repository Service result to the recorded disposition.
fn disposition(
    effect: OperationEffect,
    result: std::result::Result<RepositoryResponse, RepositoryError>,
) -> Disposition {
    let error = match result {
        Ok(response) => {
            let remote_object_id = match &response {
                RepositoryResponse::IssueComment(comment) => Some(comment.id),
                _ => None,
            };
            return succeeded(&response_json(&response), remote_object_id);
        }
        Err(error) => error,
    };
    let error_kind = error_kind_token(&error);
    let summary = error.to_string();
    match error.delivery() {
        // Provably never handed to GitHub (no credential, no child): safe
        // to retry within the attempt cap for reads and writes alike.
        Delivery::NotSent => Disposition::Retry {
            error_kind,
            summary,
            proven_unsent: true,
        },
        Delivery::Rejected => Disposition::FailedUnsent {
            error_kind,
            summary,
        },
        Delivery::PossiblySent => match error.outcome(effect) {
            OutcomeClass::Retryable => Disposition::Retry {
                error_kind,
                summary,
                proven_unsent: false,
            },
            OutcomeClass::NonRetryable | OutcomeClass::PossiblySent => Disposition::Ambiguous {
                error_kind,
                summary,
            },
        },
    }
}

/// A confirmed effect with its bounded recorded result. A result that cannot
/// be recorded is still never re-sent: ambiguous keeps it out of every retry
/// path.
fn succeeded(value: &serde_json::Value, remote_object_id: Option<u64>) -> Disposition {
    let text = value.to_string();
    if text.len() > MAX_RESULT_BYTES {
        return Disposition::Ambiguous {
            error_kind: "result_too_large",
            summary: format!("result exceeds {MAX_RESULT_BYTES} bytes"),
        };
    }
    let remote_object_id = match remote_object_id.map(|id| RemoteObjectId::new(id.to_string())) {
        None => None,
        Some(Ok(id)) => Some(id),
        Some(Err(error)) => {
            return Disposition::Ambiguous {
                error_kind: "invalid_result",
                summary: error.to_string(),
            }
        }
    };
    match GithubOperationResponseJson::new(&text) {
        Ok(json) => Disposition::Succeeded(json, remote_object_id),
        Err(error) => Disposition::Ambiguous {
            error_kind: "invalid_result",
            summary: error.to_string(),
        },
    }
}

fn response_json(response: &RepositoryResponse) -> serde_json::Value {
    match response {
        RepositoryResponse::PrMetadata(PrMetadata {
            number,
            state,
            head_ref,
            head_sha,
            base_ref,
            is_cross_repository,
        }) => serde_json::json!({
            "kind": "pr_metadata",
            "number": number.get(),
            "state": match state {
                PrState::Open => "open",
                PrState::Closed => "closed",
                PrState::Merged => "merged",
            },
            "head_ref": head_ref,
            "head_sha": head_sha,
            "base_ref": base_ref,
            "is_cross_repository": is_cross_repository,
        }),
        RepositoryResponse::PullRequestRead(value) => value.clone(),
        RepositoryResponse::IssueComment(CreatedComment { id, html_url }) => serde_json::json!({
            "kind": "issue_comment",
            "id": id,
            "html_url": html_url,
        }),
        // Rendered without the body, which carries the hidden owner marker.
        RepositoryResponse::Review(review) => review_json(review),
        RepositoryResponse::PendingReviews(reviews) => serde_json::json!({
            "kind": "pending_reviews",
            "reviews": reviews.iter().map(review_json).collect::<Vec<_>>(),
        }),
        RepositoryResponse::ReviewAbsent { review_id } => serde_json::json!({
            "kind": "review_absent",
            "id": review_id,
        }),
        RepositoryResponse::PendingReviewComment(comment) => serde_json::json!({
            "kind": "pending_review_comment",
            "id": comment.id,
            "node_id": comment.node_id,
            "thread_node_id": comment.thread_node_id,
        }),
        RepositoryResponse::ReviewComments(comments) => serde_json::json!({
            "kind": "review_comments",
            "comments": comments
                .iter()
                .map(|comment| serde_json::json!({"id": comment.id, "node_id": comment.node_id}))
                .collect::<Vec<_>>(),
        }),
        RepositoryResponse::ReviewThreads(threads) => serde_json::json!({
            "kind": "review_threads",
            "threads": threads.iter().map(thread_json).collect::<Vec<_>>(),
        }),
        // Rendered without bodies, which carry hidden operation markers.
        RepositoryResponse::ReviewReply(reply) => serde_json::json!({
            "kind": "review_reply",
            "id": reply.id,
            "node_id": reply.node_id,
            "in_reply_to_id": reply.in_reply_to_id,
        }),
        RepositoryResponse::ReviewReplies(replies) => serde_json::json!({
            "kind": "review_replies",
            "ids": replies.iter().map(|reply| reply.id).collect::<Vec<_>>(),
        }),
        RepositoryResponse::ThreadResolved { thread_node_id } => serde_json::json!({
            "kind": "review_thread_resolved",
            "thread_id": thread_node_id,
        }),
    }
}

/// The agent-facing view of one review thread: its node ID, anchor, the
/// revision it applies to, nearby diff context, and GitHub's own resolved
/// and outdated state.
fn thread_json(thread: &ReviewThreadRecord) -> serde_json::Value {
    serde_json::json!({
        "id": thread.node_id,
        "root_comment_id": thread.root_comment_id,
        "path": thread.path,
        "line": thread.line,
        "start_line": thread.start_line,
        "original_line": thread.original_line,
        "side": thread.side,
        "subject_type": thread.subject_type,
        "revision": thread.commit_id,
        "original_revision": thread.original_commit_id,
        "context": thread.diff_hunk,
        "resolved": thread.resolved,
        "outdated": thread.outdated,
    })
}

fn review_json(review: &super::repository_service::ReviewRecord) -> serde_json::Value {
    serde_json::json!({
        "kind": "review",
        "id": review.id,
        "node_id": review.node_id,
        "state": review.state.as_str(),
        "commit_id": review.commit_id,
    })
}

/// The body actually posted for a general comment: the authored Markdown,
/// byte for byte, then a blank line and the operation's hidden marker. The
/// blank line ends any open paragraph, list, quote, or table, and the
/// endpoint refuses a body with an unclosed code fence, so the marker never
/// enters an authored construct.
pub fn compose_comment_body(authored: &str, marker: &str) -> String {
    let separator = if authored.ends_with('\n') {
        "\n"
    } else {
        "\n\n"
    };
    format!("{authored}{separator}{marker}")
}

/// Bounded, NUL-free, non-empty summary text.
fn bounded_summary(text: &str) -> OperationErrorSummary {
    let mut text = text.replace('\0', " ").trim().to_string();
    if text.len() > MAX_OPERATION_ERROR_BYTES {
        let mut end = MAX_OPERATION_ERROR_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    OperationErrorSummary::new(text).unwrap_or_else(|_| {
        OperationErrorSummary::new("github operation failed").expect("static summary is valid")
    })
}

fn retry_delay_secs(attempt: i64) -> i64 {
    let exponent = u32::try_from(attempt.saturating_sub(1).clamp(0, 16)).unwrap_or(16);
    RETRY_BASE_SECS
        .saturating_mul(1_i64 << exponent)
        .min(RETRY_MAX_SECS)
}

/// Execute one claimed operation: decode, mark started, call the service
/// with no DB transaction open, then record the result.
async fn execute_claimed(ctx: &ExecContext, claimed: ClaimedOperation) -> Result<()> {
    let ClaimedOperation { claim, operation } = claimed;
    if is_review_kind(&operation.kind) {
        return execute_review(ctx, claim, operation).await;
    }
    if is_thread_kind(&operation.kind) {
        return execute_thread_write(ctx, claim, operation).await;
    }
    let prepared = PullRequestNumber::new(operation.pr_number)
        .ok_or(Disposition::FailedUnsent {
            error_kind: "invalid_target",
            summary: "operation has no valid pull request".to_string(),
        })
        .and_then(|pr| {
            decode_stored(&operation.kind, &operation.request_json).map(|request| (pr, request))
        })
        .and_then(|(pr, request)| {
            attach_marker(request, operation.github_marker.as_deref()).map(|request| (pr, request))
        });
    let (pr, request) = match prepared {
        Ok(prepared) => prepared,
        Err(disposition) => {
            record(&ctx.db_path, claim, disposition).await?;
            return Ok(());
        }
    };
    let effect = effect_of(&operation.kind, &request);

    let started = with_conn(&ctx.db_path, move |conn| {
        github_operations::mark_execution_started(conn, &claim, now_unix())
    })
    .await?;
    if !started {
        // Stale claim (zero rows on settle too) or a reconciliation-only row
        // whose earlier send may have reached GitHub: never send it again.
        return record_not_resent(&ctx.db_path, claim).await;
    }

    let result = ctx.service.execute(&ctx.target, pr, &request).await;
    record(&ctx.db_path, claim, disposition(effect, result)).await?;
    Ok(())
}

/// Record the clean "never re-sent" negative for a claim whose start was
/// refused: a stale claim, a row cancelled before its send, or a
/// reconciliation-only row whose earlier send may have reached GitHub.
async fn record_not_resent(db_path: &Path, claim: OperationClaim) -> Result<()> {
    record(
        db_path,
        claim,
        Disposition::Ambiguous {
            error_kind: "not_resent",
            summary: "operation send may already have started; not re-sent".to_string(),
        },
    )
    .await
}

// ---------------------------------------------------------------------------
// Pending-review steps
// ---------------------------------------------------------------------------

const REVIEW_WRITE_KIND: &str = GithubOperationKind::PullRequestReviewWrite.as_str();
const REVIEW_COMMENT_KIND: &str = GithubOperationKind::AddCommentToPendingReview.as_str();

fn is_review_kind(kind: &str) -> bool {
    kind == REVIEW_WRITE_KIND || kind == REVIEW_COMMENT_KIND
}

/// Stored `pull_request_review_write` request, as the endpoint admitted it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredReviewWrite {
    method: String,
    #[serde(default, rename = "commitID")]
    commit_id: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    body: Option<String>,
}

/// Stored `add_comment_to_pending_review` request: the anchor validated
/// against the launch-SHA diff at admission, plus the authored body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StoredInlineComment {
    path: String,
    line: u64,
    side: String,
    #[serde(default)]
    start_line: Option<u64>,
    #[serde(default)]
    start_side: Option<String>,
    subject_type: String,
    body: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReviewStep {
    Create,
    Inline,
    Submit,
}

/// One review step ready to send, with the exact identity reconciliation
/// needs. Built from daemon state only; the marker never leaves the daemon.
#[derive(Debug)]
struct ReviewPlan {
    step: ReviewStep,
    request: RepositoryRequest,
    launch_sha: String,
    owner_marker: ReviewOwnerMarker,
    /// Inline: the operation's hidden marker carried by the comment body.
    operation_marker: String,
    /// Inline and submit: the confirmed pending review ID.
    review_id: Option<u64>,
}

fn unsent(error_kind: &'static str, summary: &str) -> Disposition {
    Disposition::FailedUnsent {
        error_kind,
        summary: summary.to_string(),
    }
}

fn parse_side(side: &str) -> Option<DiffSide> {
    match side {
        "LEFT" => Some(DiffSide::Left),
        "RIGHT" => Some(DiffSide::Right),
        _ => None,
    }
}

/// Build the step's request from the stored row and the attempt's slot in one
/// short read (no transaction is held past it). Any refusal here happens
/// before execution starts and is the closed pre-send proof the settle
/// records as unsent.
fn prepare_review(
    conn: &Connection,
    operation: &github_operations::GithubAgentOperation,
) -> Result<std::result::Result<ReviewPlan, Disposition>> {
    let Some(sequence) = operation.review_sequence else {
        return Ok(Err(unsent(
            "invalid_review_step",
            "pending-review operation has no review sequence",
        )));
    };
    let slot = review_publication_slots::for_attempt(conn, &operation.attempt_id)?;
    let Some(slot) =
        slot.filter(|slot| slot.state == SlotState::Owned && slot.pr_number == operation.pr_number)
    else {
        return Ok(Err(unsent(
            "review_slot_lost",
            "the attempt no longer owns the pending-review slot for this PR",
        )));
    };
    let Some(owner_marker) =
        review_publication_slots::owner_marker_for_attempt(conn, &operation.attempt_id)?
    else {
        return Ok(Err(unsent(
            "missing_marker",
            "the attempt has no pending-review owner marker",
        )));
    };
    let attempt_sha: Option<String> = conn
        .query_row(
            "SELECT head_sha FROM github_collaboration_attempts WHERE attempt_id=?1",
            [&operation.attempt_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    // Admission snapshots the reviewer launch SHA in the row's `head_sha`.
    let launch_sha = match (&operation.head_sha, attempt_sha) {
        (Some(row_sha), Some(attempt_sha))
            if *row_sha == attempt_sha
                && operation
                    .reviewer_launch_sha
                    .as_ref()
                    .is_none_or(|sha| *sha == attempt_sha) =>
        {
            attempt_sha
        }
        _ => {
            return Ok(Err(unsent(
                "review_launch_mismatch",
                "the operation and attempt do not share one launch SHA",
            )))
        }
    };
    let confirmed_review_id = || -> Option<u64> {
        if slot.create_send_state
            != quorum_core::collaboration_admission::GithubSendState::Confirmed
        {
            return None;
        }
        slot.pending_review_id.as_deref()?.parse().ok()
    };
    let mut plan = ReviewPlan {
        step: ReviewStep::Create,
        request: RepositoryRequest::ListPendingReviews {},
        launch_sha: launch_sha.clone(),
        owner_marker,
        operation_marker: String::new(),
        review_id: None,
    };
    if operation.kind == REVIEW_COMMENT_KIND {
        let Ok(stored) = serde_json::from_str::<StoredInlineComment>(&operation.request_json)
        else {
            return Ok(Err(unsent(
                "unsupported_request",
                "stored inline comment request is invalid",
            )));
        };
        let (Some(side), Ok(line)) = (parse_side(&stored.side), u32::try_from(stored.line)) else {
            return Ok(Err(unsent(
                "unsupported_request",
                "stored inline comment anchor is invalid",
            )));
        };
        let start_side = match stored.start_side.as_deref().map(parse_side) {
            None => None,
            Some(Some(side)) => Some(side),
            Some(None) => {
                return Ok(Err(unsent(
                    "unsupported_request",
                    "stored inline comment start side is invalid",
                )))
            }
        };
        let start_line = match stored.start_line.map(u32::try_from) {
            None => None,
            Some(Ok(start)) => Some(start),
            Some(Err(_)) => {
                return Ok(Err(unsent(
                    "unsupported_request",
                    "stored inline comment start line is invalid",
                )))
            }
        };
        if stored.subject_type != "LINE" || sequence == 0 {
            return Ok(Err(unsent(
                "unsupported_request",
                "stored inline comment is not a line comment after the create",
            )));
        }
        let Some(marker) = operation.github_marker.clone().filter(|m| !m.is_empty()) else {
            return Ok(Err(unsent(
                "missing_marker",
                "write operation has no daemon marker",
            )));
        };
        let Some(review_id) = confirmed_review_id() else {
            return Ok(Err(unsent(
                "review_not_created",
                "the pending review has no confirmed ID",
            )));
        };
        let Some(review_node_id) = create_node_id(conn, &operation.attempt_id, review_id)? else {
            return Ok(Err(unsent(
                "review_not_created",
                "the pending review has no recorded node ID",
            )));
        };
        plan.step = ReviewStep::Inline;
        plan.request = RepositoryRequest::AddPendingReviewComment {
            review_node_id,
            path: stored.path,
            line,
            side,
            start_line,
            start_side,
            subject_type: ReviewSubjectType::Line,
            body: compose_comment_body(&stored.body, &marker),
        };
        plan.operation_marker = marker;
        plan.review_id = Some(review_id);
        return Ok(Ok(plan));
    }
    let Ok(stored) = serde_json::from_str::<StoredReviewWrite>(&operation.request_json) else {
        return Ok(Err(unsent(
            "unsupported_request",
            "stored pending-review request is invalid",
        )));
    };
    match (stored.method.as_str(), sequence) {
        ("create", 0) => {
            if stored.event.is_some() || stored.commit_id.as_deref() != Some(launch_sha.as_str()) {
                return Ok(Err(unsent(
                    "review_launch_mismatch",
                    "create must name exactly the launch SHA and no event",
                )));
            }
            let marker = plan.owner_marker.as_str();
            let body = match &stored.body {
                Some(authored) => compose_comment_body(authored, marker),
                None => marker.to_string(),
            };
            plan.request = RepositoryRequest::CreatePendingReview {
                commit_id: launch_sha,
                body,
            };
        }
        ("submit_pending", sequence) if sequence > 0 => {
            let (Some("COMMENT"), Some(authored), None) =
                (stored.event.as_deref(), &stored.body, &stored.commit_id)
            else {
                return Ok(Err(unsent(
                    "unsupported_request",
                    "submit_pending must carry event COMMENT and a summary",
                )));
            };
            let Some(review_id) = confirmed_review_id() else {
                return Ok(Err(unsent(
                    "review_not_created",
                    "the pending review has no confirmed ID",
                )));
            };
            plan.step = ReviewStep::Submit;
            plan.request = RepositoryRequest::SubmitPendingReview {
                review_id,
                body: compose_comment_body(authored, plan.owner_marker.as_str()),
            };
            plan.review_id = Some(review_id);
        }
        _ => {
            return Ok(Err(unsent(
                "unsupported_request",
                "stored pending-review step does not match its sequence",
            )))
        }
    }
    Ok(Ok(plan))
}

/// The GraphQL node ID the succeeded create recorded for `review_id`.
fn create_node_id(conn: &Connection, attempt_id: &str, review_id: u64) -> Result<Option<String>> {
    let row: Option<(Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT remote_object_id, response_json FROM github_agent_operations
             WHERE attempt_id=?1 AND review_sequence=0 AND state='succeeded'",
            [attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((Some(remote_id), Some(response))) = row else {
        return Ok(None);
    };
    if remote_id != review_id.to_string() {
        return Ok(None);
    }
    let value: serde_json::Value = match serde_json::from_str(&response) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    if value["id"].as_u64() != Some(review_id) {
        return Ok(None);
    }
    Ok(value["node_id"].as_str().map(str::to_owned))
}

/// The remote ID of exactly the effect `plan` requested, when `response`
/// proves it.
fn confirmed_effect(plan: &ReviewPlan, response: &RepositoryResponse) -> Option<u64> {
    match (plan.step, response) {
        (ReviewStep::Create, RepositoryResponse::Review(review))
            if review.state == ReviewState::Pending
                && review.commit_id.as_deref() == Some(plan.launch_sha.as_str())
                && review.has_marker(plan.owner_marker.as_str()) =>
        {
            Some(review.id)
        }
        // The service already checked the thread belongs to this PR and to
        // the exact pending review node the request named.
        (ReviewStep::Inline, RepositoryResponse::PendingReviewComment(comment)) => Some(comment.id),
        (ReviewStep::Submit, RepositoryResponse::Review(review))
            if Some(review.id) == plan.review_id && review.state == ReviewState::Commented =>
        {
            Some(review.id)
        }
        _ => None,
    }
}

/// A submitted (or created) review found by its exact identity.
fn review_matches(review: &ReviewRecord, plan: &ReviewPlan, state: ReviewState) -> bool {
    review.state == state
        && review.commit_id.as_deref() == Some(plan.launch_sha.as_str())
        && review.has_marker(plan.owner_marker.as_str())
}

/// Look for the step's effect with one exact read. `Some` only on marker
/// evidence for this attempt's review on this PR and launch SHA; any read
/// failure or absence is `None` and the step stays ambiguous.
async fn reconcile(
    ctx: &ExecContext,
    pr: PullRequestNumber,
    plan: &ReviewPlan,
) -> Option<Disposition> {
    let read = match (plan.step, plan.review_id) {
        (ReviewStep::Create, _) => RepositoryRequest::ListPendingReviews {},
        (ReviewStep::Inline, Some(review_id)) => {
            RepositoryRequest::ListReviewComments { review_id }
        }
        (ReviewStep::Submit, Some(review_id)) => RepositoryRequest::GetReview { review_id },
        _ => return None,
    };
    let response = ctx.service.execute(&ctx.target, pr, &read).await.ok()?;
    let (mut value, id) = match (plan.step, response) {
        (ReviewStep::Create, RepositoryResponse::PendingReviews(reviews)) => {
            let mut owned = reviews
                .iter()
                .filter(|review| review.has_marker(plan.owner_marker.as_str()));
            let review = owned.next()?;
            if owned.next().is_some() || !review_matches(review, plan, ReviewState::Pending) {
                return None;
            }
            (review_json(review), review.id)
        }
        (ReviewStep::Inline, RepositoryResponse::ReviewComments(comments)) => {
            let mut marked = comments
                .iter()
                .filter(|comment| comment.body.contains(&plan.operation_marker));
            let comment = marked.next()?;
            if marked.next().is_some() {
                return None;
            }
            (
                serde_json::json!({
                    "kind": "pending_review_comment",
                    "id": comment.id,
                    "node_id": comment.node_id,
                    "thread_node_id": null,
                }),
                comment.id,
            )
        }
        (ReviewStep::Submit, RepositoryResponse::Review(review))
            if Some(review.id) == plan.review_id
                && review_matches(&review, plan, ReviewState::Commented) =>
        {
            (review_json(&review), review.id)
        }
        _ => return None,
    };
    value["reconciled"] = serde_json::json!(true);
    Some(succeeded(&value, Some(id)))
}

/// Execute one claimed pending-review step: build it from daemon state, mark
/// it started, send it with no DB transaction open, and settle. A possibly
/// sent result is reconciled by exact marker before it may settle ambiguous;
/// nothing here re-sends a mutation.
async fn execute_review(
    ctx: &ExecContext,
    claim: OperationClaim,
    operation: github_operations::GithubAgentOperation,
) -> Result<()> {
    let Some(pr) = PullRequestNumber::new(operation.pr_number) else {
        let disposition = unsent("invalid_target", "operation has no valid pull request");
        return record(&ctx.db_path, claim, disposition).await;
    };
    let plan = with_conn(&ctx.db_path, move |conn| prepare_review(conn, &operation)).await?;
    let plan = match plan {
        Ok(plan) => plan,
        Err(disposition) => return record(&ctx.db_path, claim, disposition).await,
    };
    // Revalidates live authority and slot ownership; a create already holds
    // the slot's `ambiguous` send state from its claim transaction.
    let started = with_conn(&ctx.db_path, move |conn| {
        github_operations::mark_execution_started(conn, &claim, now_unix())
    })
    .await?;
    if !started {
        return record_not_resent(&ctx.db_path, claim).await;
    }

    let result = ctx.service.execute(&ctx.target, pr, &plan.request).await;
    let (error_kind, summary) = match result {
        Ok(response) => match confirmed_effect(&plan, &response) {
            Some(id) => {
                let disposition = succeeded(&response_json(&response), Some(id));
                return record(&ctx.db_path, claim, disposition).await;
            }
            None => (
                "invalid_result",
                "repository service result does not prove the requested review effect".to_string(),
            ),
        },
        // Closed transport proof: nothing was handed to GitHub.
        Err(error) if error.delivery() == Delivery::NotSent => {
            let disposition = Disposition::Retry {
                error_kind: error_kind_token(&error),
                summary: error.to_string(),
                proven_unsent: true,
            };
            return record(&ctx.db_path, claim, disposition).await;
        }
        // A GitHub answer after hand-off (even a rejection classified from
        // stderr) is not a pre-send proof: reconcile, never settle unsent.
        Err(error) => (error_kind_token(&error), error.to_string()),
    };
    let disposition = match reconcile(ctx, pr, &plan).await {
        Some(reconciled) => reconciled,
        None => Disposition::Ambiguous {
            error_kind,
            summary,
        },
    };
    record(&ctx.db_path, claim, disposition).await
}

// ---------------------------------------------------------------------------
// Review-thread replies and resolution
// ---------------------------------------------------------------------------

const REPLY_KIND: &str = GithubOperationKind::AddReplyToPullRequestComment.as_str();
const RESOLVE_KIND: &str = GithubOperationKind::ResolveReviewThread.as_str();

fn is_thread_kind(kind: &str) -> bool {
    kind == REPLY_KIND || kind == RESOLVE_KIND
}

/// The bound PR's review threads, from one Repository Service read.
async fn list_threads(
    ctx: &ExecContext,
    pr: PullRequestNumber,
) -> std::result::Result<Vec<ReviewThreadRecord>, RepositoryError> {
    match ctx
        .service
        .execute(&ctx.target, pr, &RepositoryRequest::ListReviewThreads {})
        .await?
    {
        RepositoryResponse::ReviewThreads(threads) => Ok(threads),
        _ => Err(RepositoryError::new(
            super::repository_service::RepositoryErrorKind::InvalidResponse,
            "review thread listing returned another response",
        )),
    }
}

fn reply_json(
    id: u64,
    node_id: &str,
    thread: &ReviewThreadRecord,
    reconciled: bool,
) -> serde_json::Value {
    let mut value = serde_json::json!({
        "kind": "review_reply",
        "id": id,
        "node_id": node_id,
        "in_reply_to_id": thread.root_comment_id,
        "thread": thread_json(thread),
    });
    if reconciled {
        value["reconciled"] = serde_json::json!(true);
    }
    value
}

fn resolved_json(
    thread: &ReviewThreadRecord,
    already_resolved: bool,
    reconciled: bool,
) -> serde_json::Value {
    let mut thread = thread.clone();
    thread.resolved = true;
    let mut value = serde_json::json!({
        "kind": "review_thread_resolved",
        "already_resolved": already_resolved,
        "thread": thread_json(&thread),
    });
    if reconciled {
        value["reconciled"] = serde_json::json!(true);
    }
    value
}

/// After a possibly-sent reply or resolve, look for its effect with one
/// exact read: the reply by its hidden marker among the thread root's
/// replies, the resolution by GitHub's idempotent resolved state. `None`
/// (absence or any read failure) leaves the write ambiguous; nothing here
/// sends the mutation again.
async fn reconcile_thread_write(
    ctx: &ExecContext,
    pr: PullRequestNumber,
    request: &RepositoryRequest,
    thread: &ReviewThreadRecord,
    marker: &str,
) -> Option<Disposition> {
    match request {
        RepositoryRequest::ReplyToReviewComment { comment_id, .. } => {
            let read = RepositoryRequest::ListCommentReplies {
                comment_id: *comment_id,
            };
            let RepositoryResponse::ReviewReplies(replies) =
                ctx.service.execute(&ctx.target, pr, &read).await.ok()?
            else {
                return None;
            };
            let mut marked = replies
                .iter()
                .filter(|reply| !marker.is_empty() && reply.body.contains(marker));
            let reply = marked.next()?;
            if marked.next().is_some() {
                return None;
            }
            Some(succeeded(
                &reply_json(reply.id, &reply.node_id, thread, true),
                Some(reply.id),
            ))
        }
        RepositoryRequest::ResolveReviewThread { thread_node_id } => {
            let threads = list_threads(ctx, pr).await.ok()?;
            let current = threads
                .iter()
                .find(|current| current.node_id == *thread_node_id)?;
            current
                .resolved
                .then(|| succeeded(&resolved_json(current, false, true), None))
        }
        _ => None,
    }
}

/// Execute one claimed thread reply or resolution.
///
/// The target is resolved first, with one read of the bound PR's review
/// threads and before execution starts, so a comment or thread on any other
/// PR (or a general issue comment ID) fails closed with nothing sent. A reply
/// goes to the thread's top-level comment through GitHub's reply endpoint and
/// carries the operation marker; an already-resolved thread satisfies its
/// resolve without a mutation. A possibly-sent result is reconciled by exact
/// marker or resolved state and is otherwise left ambiguous, never re-sent.
async fn execute_thread_write(
    ctx: &ExecContext,
    claim: OperationClaim,
    operation: github_operations::GithubAgentOperation,
) -> Result<()> {
    let prepared = PullRequestNumber::new(operation.pr_number)
        .ok_or_else(|| unsent("invalid_target", "operation has no valid pull request"))
        .and_then(|pr| {
            decode_stored(&operation.kind, &operation.request_json).map(|request| (pr, request))
        });
    let (pr, stored) = match prepared {
        Ok(prepared) => prepared,
        Err(disposition) => return record(&ctx.db_path, claim, disposition).await,
    };
    // A failed lookup is a read failure before any mutation was handed off:
    // retryable within the claim cap or terminal, but always provably unsent.
    let threads = match list_threads(ctx, pr).await {
        Ok(threads) => threads,
        Err(error) => {
            let error_kind = error_kind_token(&error);
            let summary = error.to_string();
            let retryable = error.delivery() == Delivery::NotSent
                || error.outcome(OperationEffect::Read) == OutcomeClass::Retryable;
            let disposition = if retryable {
                Disposition::Retry {
                    error_kind,
                    summary,
                    proven_unsent: true,
                }
            } else {
                Disposition::FailedUnsent {
                    error_kind,
                    summary,
                }
            };
            return record(&ctx.db_path, claim, disposition).await;
        }
    };
    let (thread, request, marker) = match stored {
        RepositoryRequest::ReplyToReviewComment { comment_id, body } => {
            let Some(thread) = threads
                .into_iter()
                .find(|thread| thread.comment_ids.contains(&comment_id))
            else {
                let summary = format!(
                    "comment {comment_id} is not a review comment on pull request #{}",
                    pr.get()
                );
                return record(
                    &ctx.db_path,
                    claim,
                    unsent("review_comment_not_found", &summary),
                )
                .await;
            };
            let Some(marker) = operation.github_marker.clone().filter(|m| !m.is_empty()) else {
                let disposition = unsent("missing_marker", "write operation has no daemon marker");
                return record(&ctx.db_path, claim, disposition).await;
            };
            // GitHub replies only to a thread's top-level comment.
            let request = RepositoryRequest::ReplyToReviewComment {
                comment_id: thread.root_comment_id,
                body: compose_comment_body(&body, &marker),
            };
            (thread, Some(request), marker)
        }
        RepositoryRequest::ResolveReviewThread { thread_node_id } => {
            let Some(thread) = threads
                .into_iter()
                .find(|thread| thread.node_id == thread_node_id)
            else {
                let summary = format!("the review thread is not on pull request #{}", pr.get());
                return record(
                    &ctx.db_path,
                    claim,
                    unsent("review_thread_not_found", &summary),
                )
                .await;
            };
            // Resolution is idempotent: an already-resolved thread is the
            // requested effect, confirmed without a mutation.
            let request = (!thread.resolved)
                .then_some(RepositoryRequest::ResolveReviewThread { thread_node_id });
            (thread, request, String::new())
        }
        _ => {
            let disposition = unsent("request_kind_mismatch", "not a review-thread write");
            return record(&ctx.db_path, claim, disposition).await;
        }
    };

    // Revalidates live authority; a revoked attempt is cancelled unsent.
    let started = with_conn(&ctx.db_path, move |conn| {
        github_operations::mark_execution_started(conn, &claim, now_unix())
    })
    .await?;
    if !started {
        return record_not_resent(&ctx.db_path, claim).await;
    }
    let Some(request) = request else {
        let disposition = succeeded(&resolved_json(&thread, true, false), None);
        return record(&ctx.db_path, claim, disposition).await;
    };

    let result = ctx.service.execute(&ctx.target, pr, &request).await;
    let (error_kind, summary) = match (&request, result) {
        (
            RepositoryRequest::ReplyToReviewComment { comment_id, .. },
            Ok(RepositoryResponse::ReviewReply(reply)),
        ) if reply.in_reply_to_id == *comment_id && reply.body.contains(&marker) => {
            let disposition = succeeded(
                &reply_json(reply.id, &reply.node_id, &thread, false),
                Some(reply.id),
            );
            return record(&ctx.db_path, claim, disposition).await;
        }
        (
            RepositoryRequest::ResolveReviewThread { thread_node_id },
            Ok(RepositoryResponse::ThreadResolved {
                thread_node_id: resolved,
            }),
        ) if resolved == *thread_node_id => {
            let disposition = succeeded(&resolved_json(&thread, false, false), None);
            return record(&ctx.db_path, claim, disposition).await;
        }
        (_, Ok(_)) => (
            "invalid_result",
            "repository service result does not prove the requested thread effect".to_string(),
        ),
        // Closed transport proof: nothing was handed to GitHub.
        (_, Err(error)) if error.delivery() == Delivery::NotSent => {
            let disposition = Disposition::Retry {
                error_kind: error_kind_token(&error),
                summary: error.to_string(),
                proven_unsent: true,
            };
            return record(&ctx.db_path, claim, disposition).await;
        }
        // Any answer after hand-off, even a rejection, is reconciled, never
        // settled unsent.
        (_, Err(error)) => (error_kind_token(&error), error.to_string()),
    };
    let disposition = match reconcile_thread_write(ctx, pr, &request, &thread, &marker).await {
        Some(reconciled) => reconciled,
        None => Disposition::Ambiguous {
            error_kind,
            summary,
        },
    };
    record(&ctx.db_path, claim, disposition).await
}

/// Record a disposition for `claim` in one core primitive. A stale claim is
/// a clean negative (zero rows) and is not an error.
async fn record(db_path: &Path, claim: OperationClaim, disposition: Disposition) -> Result<()> {
    with_conn(db_path, move |conn| {
        let now = now_unix();
        match disposition {
            Disposition::Succeeded(json, remote_object_id) => {
                github_operations::settle(
                    conn,
                    &claim,
                    Settlement::Succeeded {
                        response_json: Some(&json),
                        remote_object_id: remote_object_id.as_ref(),
                    },
                    now,
                )?;
            }
            Disposition::FailedUnsent {
                error_kind,
                summary,
            } => {
                let summary = bounded_summary(&summary);
                github_operations::settle(
                    conn,
                    &claim,
                    Settlement::FailedUnsent {
                        error_kind,
                        error_summary: &summary,
                    },
                    now,
                )?;
            }
            Disposition::Ambiguous {
                error_kind,
                summary,
            } => {
                let summary = bounded_summary(&summary);
                github_operations::settle(
                    conn,
                    &claim,
                    Settlement::Ambiguous {
                        error_kind,
                        error_summary: &summary,
                    },
                    now,
                )?;
            }
            Disposition::Retry {
                error_kind,
                summary,
                proven_unsent,
            } => {
                let summary = bounded_summary(&summary);
                let retry_at = now + retry_delay_secs(claim.attempt);
                let outcome = github_operations::release_for_retry(
                    conn,
                    &claim,
                    retry_at,
                    error_kind,
                    &summary,
                    proven_unsent,
                    now,
                )?;
                if outcome == RetryOutcome::Exhausted {
                    log(&format!(
                        "github outbox: operation row {} exhausted after attempt {}/{}: {error_kind}",
                        claim.id, claim.attempt, MAX_EXECUTION_ATTEMPTS
                    ));
                }
            }
        }
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::github_credentials::{
        FakeCredentialSource, GithubCredentialError, GithubCredentialSource,
    };
    use crate::serve::repository_service::{
        FakeOutcome, FakeRepositoryService, GhRepositoryService, RepositoryErrorKind,
        RepositoryFuture,
    };
    use quorum_core::collaboration_admission::{
        self as admission, ClientRequestId, CollaborationAgent, CollaborationAttemptId,
        CollaborationRole, EnqueueGithubOperationRequest, GithubOperationAdmissionResult,
        GithubOperationId, GithubOperationReadResult, GithubOperationRequestJson,
        GithubOperationState, GithubSendState, RunCapabilityId,
    };
    use quorum_core::github_operations::GithubAgentOperation;
    use std::sync::atomic::AtomicUsize;

    const RUN: &str = "run-outbox";
    const ATTEMPT: &str = "attempt-outbox";
    const AGENT: &str = "Worker";
    const TASK: i64 = 1;
    const PR: i64 = 42;
    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
    const READ_REQUEST: &str = r#"{"kind":"read_pr_metadata"}"#;
    const FAST_POLL: Duration = Duration::from_millis(20);

    struct Harness {
        _dir: tempfile::TempDir,
        db: PathBuf,
        repo_dir: PathBuf,
    }

    impl Harness {
        /// A live worker capability bound to one active collaboration attempt
        /// on PR #42 (the same fixture shape admission tests use).
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("q.db");
            let mut conn = quorum_core::db::open(&db).unwrap();
            let now = now_unix();
            conn.execute(
                "INSERT INTO tasks(
                     id,title,status,assignee,author,created_by,created_at,updated_at,refs
                 ) VALUES (?1,'outbox test','rework',?2,?2,'owner',1,1,?3)",
                rusqlite::params![
                    TASK,
                    AGENT,
                    serde_json::json!({
                        "pr": PR,
                        "runner_retry": {
                            "provider": "codex", "model": "model", "effort": "high",
                            "prompt": "resume exact collaboration turn",
                            "turn_kind": "rework", "continuation_id": "turn-exact",
                            "requested": true
                        }
                    })
                    .to_string()
                ],
            )
            .unwrap();
            quorum_core::capabilities::issue(&mut conn, RUN, TASK, AGENT, "worker", now).unwrap();
            quorum_core::agent_runs::insert(
                &conn, TASK, AGENT, "worker", "model", "high", "codex", now,
            )
            .unwrap();
            conn.execute(
                "INSERT INTO github_collaboration_attempts(
                     attempt_id, task_id, agent, role, pr_number, head_sha,
                     lifecycle_generation, active_run_id, state,
                     created_at, updated_at, expires_at
                 ) VALUES (?1,?2,?3,'worker',?4,NULL,0,?5,'active',?6,?6,?7)",
                rusqlite::params![ATTEMPT, TASK, AGENT, PR, RUN, now, now + 3_600],
            )
            .unwrap();
            let repo_dir = dir.path().to_path_buf();
            Self {
                _dir: dir,
                db,
                repo_dir,
            }
        }

        fn conn(&self) -> Connection {
            quorum_core::db::open(&self.db).unwrap()
        }

        fn target(&self) -> RepositoryTarget {
            RepositoryTarget::new(self.repo_dir.clone(), Some("acme/widgets")).unwrap()
        }

        /// Admit one operation through the real admission boundary.
        fn admit(&self, client: &str, kind: GithubOperationKind, body: &str) -> GithubOperationId {
            let now = now_unix();
            let attempt = CollaborationAttemptId::new(ATTEMPT).unwrap();
            let client = ClientRequestId::new(client).unwrap();
            let operation_id = admission::derive_operation_id(&attempt, &client);
            let marker = admission::derive_github_marker(&attempt, &client, kind);
            let request = EnqueueGithubOperationRequest::new(
                operation_id.clone(),
                client,
                attempt,
                RunCapabilityId::new(RUN).unwrap(),
                TASK,
                CollaborationAgent::new(AGENT).unwrap(),
                CollaborationRole::Worker,
                PR,
                None,
                kind,
                GithubOperationRequestJson::new(body).unwrap(),
                marker,
                now + 1_800,
                now,
                now + 3_600,
            )
            .unwrap();
            let result = admission::admit_operation(&mut self.conn(), RUN, &request, now).unwrap();
            assert!(
                matches!(result, GithubOperationAdmissionResult::Enqueued(_)),
                "{result:?}"
            );
            operation_id
        }

        fn row(&self, id: &GithubOperationId) -> GithubAgentOperation {
            github_operations::get_by_operation_id(&self.conn(), id.as_str())
                .unwrap()
                .unwrap()
        }

        fn read(&self, id: &GithubOperationId) -> GithubOperationReadResult {
            admission::read_operation(
                &self.conn(),
                RUN,
                &CollaborationAttemptId::new(ATTEMPT).unwrap(),
                id,
            )
            .unwrap()
        }

        fn executor(&self, service: Arc<dyn RepositoryService>) -> OutboxExecutor {
            OutboxExecutor::new(self.db.clone(), self.target(), service)
                .with_timing(FAST_POLL, Duration::from_secs(5))
        }

        fn errors(&self) -> i64 {
            self.conn()
                .query_row("SELECT COUNT(*) FROM errors", [], |r| r.get(0))
                .unwrap()
        }
    }

    fn metadata(head_ref: &str) -> RepositoryResponse {
        RepositoryResponse::PrMetadata(PrMetadata {
            number: PullRequestNumber::new(PR).unwrap(),
            state: PrState::Open,
            head_ref: head_ref.to_string(),
            head_sha: SHA.to_string(),
            base_ref: "main".to_string(),
            is_cross_repository: false,
        })
    }

    struct Running {
        shutdown: Arc<AtomicBool>,
        handle: tokio::task::JoinHandle<Result<()>>,
    }

    impl Running {
        fn start(executor: OutboxExecutor) -> Self {
            let shutdown = Arc::new(AtomicBool::new(false));
            let handle = tokio::spawn(executor.run(shutdown.clone()));
            Self { shutdown, handle }
        }

        async fn stop(self) {
            self.shutdown.store(true, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(15), self.handle)
                .await
                .expect("executor stops")
                .unwrap()
                .unwrap();
        }
    }

    async fn wait_for(
        h: &Harness,
        id: &GithubOperationId,
        pred: impl Fn(&GithubAgentOperation) -> bool,
    ) -> GithubAgentOperation {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let row = h.row(id);
            if pred(&row) {
                return row;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out: {row:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn terminal(row: &GithubAgentOperation) -> bool {
        matches!(row.state.as_str(), "succeeded" | "failed")
    }

    #[tokio::test]
    async fn admitted_read_flows_admit_claim_execute_settle_read() {
        let h = Harness::new();
        let id = h.admit(
            "req-read",
            GithubOperationKind::PullRequestRead,
            READ_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Succeed(
            metadata("feature"),
        )]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, terminal).await;
        running.stop().await;

        assert_eq!(row.state, "succeeded");
        assert_eq!(row.send_state, "confirmed");
        assert_eq!(row.attempts, 1);
        assert!(!row.active);
        assert!(row.execution_started_at.is_some());
        let calls = fake.calls();
        assert_eq!(calls.len(), 1, "executed exactly once");
        // Target from daemon config, PR from the row's capability snapshot.
        assert_eq!(calls[0].target, h.target());
        assert_eq!(calls[0].pr.get(), PR as u64);
        assert_eq!(calls[0].request, RepositoryRequest::ReadPrMetadata {});

        let GithubOperationReadResult::Found(status) = h.read(&id) else {
            panic!("operation must be readable by its owning run");
        };
        assert_eq!(status.state, GithubOperationState::Succeeded);
        assert_eq!(status.send_state, GithubSendState::Confirmed);
        let response = status.response_json.expect("bounded result recorded");
        assert!(response.as_str().len() <= MAX_RESULT_BYTES);
        let value: serde_json::Value = serde_json::from_str(response.as_str()).unwrap();
        assert_eq!(value["kind"], "pr_metadata");
        assert_eq!(value["number"], PR);
        assert_eq!(value["head_sha"], SHA);
        assert_eq!(value["state"], "open");
        assert_eq!(h.errors(), 0);
    }

    #[tokio::test]
    async fn credential_source_failure_is_not_sent_and_retried_within_cap() {
        let h = Harness::new();
        let id = h.admit(
            "req-cred",
            GithubOperationKind::PullRequestRead,
            READ_REQUEST,
        );
        // Real service over a failing credential source: no gh child runs.
        let source = Arc::new(FakeCredentialSource::failing(
            GithubCredentialError::LookupFailed {
                reason: "no auth".to_string(),
            },
        ));
        let credentials: Arc<dyn GithubCredentialSource> = source.clone();
        let service = Arc::new(GhRepositoryService::new(credentials));
        let running = Running::start(h.executor(service));
        let row = wait_for(&h, &id, |r| r.state == "queued" && r.attempts == 1).await;
        running.stop().await;

        assert_eq!(source.calls(), 1);
        assert!(!row.active);
        assert_eq!(row.send_state, "not_started");
        assert_eq!(row.error_kind.as_deref(), Some("credential_unavailable"));
        let retry_at = row.next_attempt_at.expect("retry scheduled");
        assert!(retry_at >= row.updated_at + RETRY_BASE_SECS);
        assert_eq!(h.errors(), 0);
    }

    #[tokio::test]
    async fn credential_failure_on_last_attempt_settles_failed_not_sent() {
        let h = Harness::new();
        let id = h.admit(
            "req-cap",
            GithubOperationKind::PullRequestRead,
            READ_REQUEST,
        );
        h.conn()
            .execute(
                "UPDATE github_agent_operations SET attempts=?2 WHERE operation_id=?1",
                rusqlite::params![id.as_str(), MAX_EXECUTION_ATTEMPTS - 1],
            )
            .unwrap();
        let fake = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::credential_unavailable(),
        ]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, terminal).await;
        running.stop().await;
        assert_eq!(row.state, "failed");
        assert_eq!(row.send_state, "not_started");
        assert_eq!(row.attempts, MAX_EXECUTION_ATTEMPTS);
        assert_eq!(fake.calls().len(), 1);
    }

    #[tokio::test]
    async fn read_timeout_is_released_for_retry_not_settled() {
        let h = Harness::new();
        let id = h.admit(
            "req-timeout",
            GithubOperationKind::PullRequestRead,
            READ_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([FakeOutcome::timeout()]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, |r| r.state == "queued" && r.attempts == 1).await;
        running.stop().await;
        assert_eq!(row.error_kind.as_deref(), Some("timed_out"));
        assert_eq!(row.send_state, "not_started");
        assert_eq!(fake.calls().len(), 1, "not retried before its due time");
    }

    #[tokio::test]
    async fn write_timeout_settles_ambiguous_and_is_never_resent() {
        let h = Harness::new();
        let id = h.admit(
            "req-write",
            GithubOperationKind::AddIssueComment,
            r#"{"body":"hi"}"#,
        );
        let claimed = github_operations::claim_next(&mut h.conn(), now_unix())
            .unwrap()
            .unwrap();
        assert!(github_operations::mark_execution_started(
            &mut h.conn(),
            &claimed.claim,
            now_unix()
        )
        .unwrap());
        let result = Err(RepositoryError::new(
            RepositoryErrorKind::TimedOut,
            "deadline",
        ));
        record(
            &h.db,
            claimed.claim,
            disposition(OperationEffect::Write, result),
        )
        .await
        .unwrap();
        let row = h.row(&id);
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str(), row.active),
            ("failed", "ambiguous", false)
        );
        assert_eq!(row.error_kind.as_deref(), Some("timed_out"));
        // Never re-claimed.
        assert!(github_operations::claim_next(&mut h.conn(), now_unix())
            .unwrap()
            .is_none());
    }

    #[test]
    fn disposition_never_retries_possibly_sent_writes() {
        use RepositoryErrorKind::*;
        for kind in [
            TimedOut,
            TransientFailure,
            ResponseTooLarge,
            InvalidResponse,
        ] {
            let got = disposition(OperationEffect::Write, Err(RepositoryError::new(kind, "x")));
            assert!(
                matches!(got, Disposition::Ambiguous { .. }),
                "{kind:?}: {got:?}"
            );
        }
        for kind in [CredentialUnavailable, SpawnFailed] {
            for effect in [OperationEffect::Read, OperationEffect::Write] {
                let got = disposition(effect, Err(RepositoryError::new(kind, "x")));
                assert!(
                    matches!(
                        got,
                        Disposition::Retry {
                            proven_unsent: true,
                            ..
                        }
                    ),
                    "{kind:?}/{effect:?}: {got:?}"
                );
            }
        }
        assert!(matches!(
            disposition(
                OperationEffect::Write,
                Err(RepositoryError::new(PermanentFailure, "x"))
            ),
            Disposition::FailedUnsent { .. }
        ));
        for kind in [TimedOut, TransientFailure] {
            assert!(matches!(
                disposition(OperationEffect::Read, Err(RepositoryError::new(kind, "x"))),
                Disposition::Retry {
                    proven_unsent: false,
                    ..
                }
            ));
        }
        // Effect is a read only when row kind and request both say so.
        let read = RepositoryRequest::ReadPrMetadata {};
        assert_eq!(effect_of("pull_request_read", &read), OperationEffect::Read);
        assert_eq!(
            effect_of("add_issue_comment", &read),
            OperationEffect::Write
        );
    }

    #[tokio::test]
    async fn started_write_across_restart_is_reconciled_ambiguous_not_reexecuted() {
        let h = Harness::new();
        let id = h.admit(
            "req-restart",
            GithubOperationKind::AddIssueComment,
            r#"{"body":"hi"}"#,
        );
        // Previous daemon: claimed and started the send, then died.
        let claimed = github_operations::claim_next(&mut h.conn(), now_unix())
            .unwrap()
            .unwrap();
        assert!(github_operations::mark_execution_started(
            &mut h.conn(),
            &claimed.claim,
            now_unix()
        )
        .unwrap());

        let fake = Arc::new(FakeRepositoryService::scripted([]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, terminal).await;
        // Give the loop several ticks to (wrongly) re-claim it.
        tokio::time::sleep(FAST_POLL * 10).await;
        running.stop().await;

        let row_after = h.row(&id);
        assert_eq!(row_after, row);
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str(), row.active),
            ("failed", "ambiguous", false)
        );
        assert_eq!(row.error_kind.as_deref(), Some("interrupted_ambiguous"));
        assert_eq!(row.attempts, 1);
        assert!(fake.calls().is_empty(), "ambiguous write was re-sent");
        let GithubOperationReadResult::Found(status) = h.read(&id) else {
            panic!("readable");
        };
        assert_eq!(status.send_state, GithubSendState::Ambiguous);
    }

    #[tokio::test]
    async fn oversize_service_response_settles_failed() {
        let h = Harness::new();
        let id = h.admit(
            "req-big",
            GithubOperationKind::PullRequestRead,
            READ_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([FakeOutcome::oversize()]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, terminal).await;
        running.stop().await;
        assert_eq!(row.state, "failed");
        assert_eq!(row.error_kind.as_deref(), Some("response_too_large"));
        assert!(row.response_json.is_none());
        assert_eq!(fake.calls().len(), 1, "oversize is not retried");
    }

    #[tokio::test]
    async fn oversize_result_is_not_recorded_and_settles_failed() {
        let h = Harness::new();
        let id = h.admit(
            "req-huge",
            GithubOperationKind::PullRequestRead,
            READ_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Succeed(
            metadata(&"x".repeat(MAX_RESULT_BYTES)),
        )]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, terminal).await;
        running.stop().await;
        assert_eq!(row.state, "failed");
        assert_eq!(row.error_kind.as_deref(), Some("result_too_large"));
        assert!(row.response_json.is_none());
        assert_eq!(fake.calls().len(), 1);
    }

    #[tokio::test]
    async fn undecodable_or_mismatched_requests_fail_without_sending() {
        let h = Harness::new();
        let bad = h.admit(
            "req-bad",
            GithubOperationKind::PullRequestRead,
            r#"{"kind":"read_pr_metadata","repo":"evil/repo"}"#,
        );
        let mismatched = h.admit(
            "req-mismatch",
            GithubOperationKind::AddIssueComment,
            READ_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([]));
        let running = Running::start(h.executor(fake.clone()));
        let bad_row = wait_for(&h, &bad, terminal).await;
        let mismatched_row = wait_for(&h, &mismatched, terminal).await;
        running.stop().await;
        assert!(fake.calls().is_empty());
        assert_eq!(bad_row.error_kind.as_deref(), Some("unsupported_request"));
        assert_eq!(
            mismatched_row.error_kind.as_deref(),
            Some("request_kind_mismatch")
        );
        for row in [bad_row, mismatched_row] {
            assert_eq!(row.state, "failed");
            assert_eq!(row.send_state, "not_started");
            assert!(row.execution_started_at.is_none());
        }
    }

    /// Blocks every call until released; counts calls.
    struct GatedService {
        calls: AtomicUsize,
        gate: tokio::sync::Semaphore,
    }

    impl RepositoryService for GatedService {
        fn execute<'a>(
            &'a self,
            _target: &'a RepositoryTarget,
            _pr: PullRequestNumber,
            _request: &'a RepositoryRequest,
        ) -> RepositoryFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                self.gate.acquire().await.unwrap().forget();
                Ok(metadata("feature"))
            })
        }
    }

    async fn wait_calls(service: &GatedService, n: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while service.calls.load(Ordering::SeqCst) < n {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn shutdown_mid_flight_settles_in_flight_and_claims_nothing_new() {
        let h = Harness::new();
        let first = h.admit("req-a", GithubOperationKind::PullRequestRead, READ_REQUEST);
        let service = Arc::new(GatedService {
            calls: AtomicUsize::new(0),
            gate: tokio::sync::Semaphore::new(0),
        });
        let running = Running::start(h.executor(service.clone()));
        wait_calls(&service, 1).await;
        running.shutdown.store(true, Ordering::SeqCst);
        // Let the loop observe the flag (it re-reads it every tick), then
        // queue more work: it must not be claimed while draining.
        tokio::time::sleep(FAST_POLL * 5).await;
        let second = h.admit("req-b", GithubOperationKind::PullRequestRead, READ_REQUEST);
        tokio::time::sleep(FAST_POLL * 5).await;
        assert!(!running.handle.is_finished(), "drain awaits in-flight work");
        service.gate.add_permits(1);
        running.stop().await;

        let row = h.row(&first);
        assert_eq!((row.state.as_str(), row.attempts), ("succeeded", 1));
        let row = h.row(&second);
        assert_eq!((row.state.as_str(), row.attempts), ("queued", 0));
        assert_eq!(service.calls.load(Ordering::SeqCst), 1);

        // Next daemon: reconciliation finds nothing claimed; the queued read
        // runs once and the settled one is not re-executed.
        let fake = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Succeed(
            metadata("feature"),
        )]));
        let running = Running::start(h.executor(fake.clone()));
        wait_for(&h, &second, terminal).await;
        running.stop().await;
        assert_eq!(fake.calls().len(), 1);
        assert_eq!(h.row(&first).attempts, 1);
    }

    #[tokio::test]
    async fn shutdown_past_grace_leaves_claim_reconcilable_on_next_start() {
        let h = Harness::new();
        let id = h.admit(
            "req-stuck",
            GithubOperationKind::PullRequestRead,
            READ_REQUEST,
        );
        let service = Arc::new(GatedService {
            calls: AtomicUsize::new(0),
            gate: tokio::sync::Semaphore::new(0),
        });
        let executor = OutboxExecutor::new(h.db.clone(), h.target(), service.clone())
            .with_timing(FAST_POLL, Duration::from_millis(50));
        let running = Running::start(executor);
        wait_calls(&service, 1).await;
        running.stop().await;
        let row = h.row(&id);
        assert_eq!((row.state.as_str(), row.active), ("running", true));

        let fake = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Succeed(
            metadata("feature"),
        )]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, terminal).await;
        running.stop().await;
        // A read may retry: reconciled, re-claimed once, and settled.
        assert_eq!((row.state.as_str(), row.attempts), ("succeeded", 2));
        assert_eq!(fake.calls().len(), 1);
        assert_eq!(service.calls.load(Ordering::SeqCst), 1);
    }

    // -----------------------------------------------------------------
    // Pending-review steps
    // -----------------------------------------------------------------

    const REVIEW_TASK: i64 = 3;
    const REVIEW_RUN: &str = "run-review";
    const REVIEW_ATTEMPT: &str = "attempt-review";
    const REVIEWER: &str = "Reviewer";
    const SCOPE: &str = "scope-test";
    const CREATE: &str =
        r#"{"method":"create","commitID":"0123456789abcdef0123456789abcdef01234567"}"#;
    const INLINE: &str =
        r#"{"path":"src/lib.rs","line":3,"side":"RIGHT","subjectType":"LINE","body":"finding"}"#;
    const SUBMIT: &str =
        r###"{"method":"submit_pending","event":"COMMENT","body":"## Summary\n\nok"}"###;

    /// A live reviewer on PR #42 at launch SHA `SHA` whose attempt owns the
    /// publication slot through the real probe primitives.
    fn review_harness() -> Harness {
        let h = Harness::new();
        let mut conn = h.conn();
        let now = now_unix();
        conn.execute(
            "INSERT INTO tasks(
                 id,title,status,assignee,reviewer,created_by,created_at,updated_at,refs
             ) VALUES (?1,'review test','in-review',?2,?2,'owner',1,1,?3)",
            rusqlite::params![
                REVIEW_TASK,
                REVIEWER,
                serde_json::json!({"pr": PR}).to_string()
            ],
        )
        .unwrap();
        quorum_core::capabilities::issue(
            &mut conn,
            REVIEW_RUN,
            REVIEW_TASK,
            REVIEWER,
            "reviewer",
            now,
        )
        .unwrap();
        quorum_core::agent_runs::insert_reviewer_with_launch(
            &conn,
            REVIEW_TASK,
            REVIEWER,
            "model",
            "high",
            "codex",
            None,
            now,
            None,
            REVIEW_RUN,
            PR,
            SHA,
        )
        .unwrap()
        .unwrap();
        let attempt = admission::CreateCollaborationAttemptRequest::new(
            CollaborationAttemptId::new(REVIEW_ATTEMPT).unwrap(),
            REVIEW_TASK,
            CollaborationAgent::new(REVIEWER).unwrap(),
            CollaborationRole::Reviewer,
            PR,
            Some(admission::ReviewerHeadSha::new(SHA).unwrap()),
            0,
            RunCapabilityId::new(REVIEW_RUN).unwrap(),
            now,
            now + 3_600,
        )
        .unwrap();
        assert!(matches!(
            admission::issue_attempt(&mut conn, &attempt, now).unwrap(),
            admission::CollaborationAttemptAdmissionResult::Created(_)
        ));
        let scope = review_publication_slots::PublisherScope::new(SCOPE).unwrap();
        let review_publication_slots::ProbeStart::Claimed(claim) =
            review_publication_slots::begin_probe(&mut conn, &scope, PR, REVIEW_TASK, now).unwrap()
        else {
            panic!("probe not claimed");
        };
        let owned = review_publication_slots::resolve_probe(
            &mut conn,
            &claim,
            review_publication_slots::ProbeResult::Empty {
                new_attempt_id: REVIEW_ATTEMPT,
            },
            now,
        )
        .unwrap();
        assert!(
            matches!(owned, review_publication_slots::ProbeOutcome::Owned(_)),
            "{owned:?}"
        );
        h
    }

    impl Harness {
        fn admit_review(
            &self,
            client: &str,
            kind: GithubOperationKind,
            body: &str,
        ) -> GithubOperationId {
            let now = now_unix();
            let attempt = CollaborationAttemptId::new(REVIEW_ATTEMPT).unwrap();
            let client = ClientRequestId::new(client).unwrap();
            let operation_id = admission::derive_operation_id(&attempt, &client);
            let marker = admission::derive_github_marker(&attempt, &client, kind);
            let request = EnqueueGithubOperationRequest::new(
                operation_id.clone(),
                client,
                attempt,
                RunCapabilityId::new(REVIEW_RUN).unwrap(),
                REVIEW_TASK,
                CollaborationAgent::new(REVIEWER).unwrap(),
                CollaborationRole::Reviewer,
                PR,
                Some(admission::ReviewerHeadSha::new(SHA).unwrap()),
                kind,
                GithubOperationRequestJson::new(body).unwrap(),
                marker,
                now + 1_800,
                now,
                now + 3_600,
            )
            .unwrap();
            let result =
                admission::admit_operation(&mut self.conn(), REVIEW_RUN, &request, now).unwrap();
            assert!(
                matches!(result, GithubOperationAdmissionResult::Enqueued(_)),
                "{result:?}"
            );
            operation_id
        }

        /// create (0), one inline finding (1), COMMENT submit (2).
        fn admit_review_sequence(&self) -> [GithubOperationId; 3] {
            [
                self.admit_review(
                    "create",
                    GithubOperationKind::PullRequestReviewWrite,
                    CREATE,
                ),
                self.admit_review(
                    "inline",
                    GithubOperationKind::AddCommentToPendingReview,
                    INLINE,
                ),
                self.admit_review(
                    "submit",
                    GithubOperationKind::PullRequestReviewWrite,
                    SUBMIT,
                ),
            ]
        }

        fn owner_marker(&self) -> String {
            review_publication_slots::owner_marker_for_attempt(&self.conn(), REVIEW_ATTEMPT)
                .unwrap()
                .unwrap()
                .as_str()
                .to_string()
        }

        fn slot(&self) -> Option<review_publication_slots::SlotStatus> {
            review_publication_slots::for_attempt(&self.conn(), REVIEW_ATTEMPT).unwrap()
        }
    }

    /// The mutations among `calls`, in order, by variant.
    fn mutations(calls: &[crate::serve::repository_service::FakeCall]) -> Vec<&'static str> {
        calls
            .iter()
            .filter_map(|call| match call.request {
                RepositoryRequest::CreatePendingReview { .. } => Some("create"),
                RepositoryRequest::AddPendingReviewComment { .. } => Some("inline"),
                RepositoryRequest::SubmitPendingReview { .. } => Some("submit"),
                RepositoryRequest::DeletePendingReview { .. } => Some("delete"),
                _ => None,
            })
            .collect()
    }

    fn assert_published_once(
        h: &Harness,
        fake: &FakeRepositoryService,
        ids: &[GithubOperationId; 3],
    ) {
        let owner = h.owner_marker();
        let rows = ids.iter().map(|id| h.row(id)).collect::<Vec<_>>();
        for row in &rows {
            assert_eq!(
                (row.state.as_str(), row.send_state.as_str(), row.active),
                ("succeeded", "confirmed", false),
                "{row:?}"
            );
            assert!(row.remote_object_id.is_some());
        }
        let store = fake.reviews();
        assert_eq!(store.reviews.len(), 1, "exactly one review: {store:?}");
        let review = &store.reviews[0];
        assert_eq!(review.record.state, ReviewState::Commented);
        assert_eq!(review.record.commit_id.as_deref(), Some(SHA));
        assert_eq!(review.record.body, format!("## Summary\n\nok\n\n{owner}"));
        assert_eq!(review.comments.len(), 1, "exactly one inline comment");
        assert_eq!(
            review.comments[0].body,
            format!("finding\n\n{}", rows[1].github_marker.as_deref().unwrap())
        );
        assert_eq!(
            rows[0].remote_object_id.as_deref(),
            Some(review.record.id.to_string().as_str())
        );
        assert_eq!(
            rows[1].remote_object_id.as_deref(),
            Some(review.comments[0].id.to_string().as_str())
        );
        assert_eq!(mutations(&fake.calls()), ["create", "inline", "submit"]);
        // A successful submit releases the slot.
        assert!(h.slot().is_none(), "{:?}", h.slot());
        assert_eq!(h.errors(), 0);
    }

    #[tokio::test]
    async fn review_steps_publish_in_sequence_with_owner_and_operation_markers() {
        let h = review_harness();
        let ids = h.admit_review_sequence();
        let fake = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::Apply,
            FakeOutcome::Apply,
            FakeOutcome::Apply,
        ]));
        let running = Running::start(h.executor(fake.clone()));
        wait_for(&h, &ids[2], terminal).await;
        running.stop().await;

        let calls = fake.calls();
        let owner = h.owner_marker();
        let RepositoryRequest::CreatePendingReview { commit_id, body } = &calls[0].request else {
            panic!("{calls:?}");
        };
        assert_eq!((commit_id.as_str(), body.as_str()), (SHA, owner.as_str()));
        let RepositoryRequest::AddPendingReviewComment {
            review_node_id,
            path,
            line,
            side,
            subject_type,
            ..
        } = &calls[1].request
        else {
            panic!("{calls:?}");
        };
        let review = &fake.reviews().reviews[0].record;
        assert_eq!(review_node_id, &review.node_id);
        assert_eq!(
            (path.as_str(), *line, *side, *subject_type),
            ("src/lib.rs", 3, DiffSide::Right, ReviewSubjectType::Line)
        );
        // The executor can only submit COMMENT (the request has no event
        // field; the service fixes it) on the persisted review ID.
        let RepositoryRequest::SubmitPendingReview { review_id, .. } = &calls[2].request else {
            panic!("{calls:?}");
        };
        assert_eq!(*review_id, review.id);
        assert_published_once(&h, &fake, &ids);
        let create = h.row(&ids[0]);
        let value: serde_json::Value =
            serde_json::from_str(create.response_json.as_deref().unwrap()).unwrap();
        assert!(!value.to_string().contains(&owner), "marker never recorded");
    }

    #[tokio::test]
    async fn lost_create_inline_and_submit_responses_reconcile_by_marker_without_resend() {
        let h = review_harness();
        let ids = h.admit_review_sequence();
        let fake = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::lost_response(),
            FakeOutcome::Apply, // list pending reviews
            FakeOutcome::lost_response(),
            FakeOutcome::Apply, // list the review's comments
            FakeOutcome::ApplyThenLose(RepositoryErrorKind::TransientFailure),
            FakeOutcome::Apply, // get the exact review
        ]));
        let running = Running::start(h.executor(fake.clone()));
        wait_for(&h, &ids[2], terminal).await;
        running.stop().await;

        let reads = fake
            .calls()
            .iter()
            .filter_map(|call| match call.request {
                RepositoryRequest::ListPendingReviews {} => Some("list_pending"),
                RepositoryRequest::ListReviewComments { .. } => Some("list_comments"),
                RepositoryRequest::GetReview { .. } => Some("get_review"),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(reads, ["list_pending", "list_comments", "get_review"]);
        assert_published_once(&h, &fake, &ids);
        for id in &ids {
            let value: serde_json::Value =
                serde_json::from_str(h.row(id).response_json.as_deref().unwrap()).unwrap();
            assert_eq!(value["reconciled"], true, "{value}");
        }
    }

    #[tokio::test]
    async fn possibly_sent_create_without_marker_stays_ambiguous_and_fails_dependents() {
        let h = review_harness();
        let ids = h.admit_review_sequence();
        // The create's effect is unknown and the listing shows no marker.
        let fake = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::ambiguous(),
            FakeOutcome::Apply,
        ]));
        let running = Running::start(h.executor(fake.clone()));
        let create = wait_for(&h, &ids[0], terminal).await;
        let submit = wait_for(&h, &ids[2], terminal).await;
        running.stop().await;

        assert_eq!(
            (create.state.as_str(), create.send_state.as_str()),
            ("failed", "ambiguous")
        );
        assert_eq!(create.error_kind.as_deref(), Some("transient_failure"));
        for row in [h.row(&ids[1]), submit] {
            assert_eq!(
                (row.state.as_str(), row.send_state.as_str()),
                ("failed", "not_started"),
                "{row:?}"
            );
            assert_eq!(row.error_kind.as_deref(), Some("review_predecessor_failed"));
            assert!(row.execution_started_at.is_none());
        }
        // The create may have landed: the slot needs cleanup, still ambiguous.
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::CleanupRequired);
        assert_eq!(
            slot.create_send_state,
            quorum_core::collaboration_admission::GithubSendState::Ambiguous
        );
        assert_eq!(mutations(&fake.calls()), ["create"], "never re-sent");
        assert_eq!(fake.calls().len(), 2);
        assert_eq!(h.errors(), 0);
    }

    #[tokio::test]
    async fn rejected_inline_is_reconciled_then_ambiguous_never_unsent() {
        let h = review_harness();
        let ids = h.admit_review_sequence();
        let fake = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::Apply,
            FakeOutcome::failure(),
            FakeOutcome::Apply, // the review's comments: none carry the marker
        ]));
        let running = Running::start(h.executor(fake.clone()));
        let inline = wait_for(&h, &ids[1], terminal).await;
        let submit = wait_for(&h, &ids[2], terminal).await;
        running.stop().await;

        assert_eq!(h.row(&ids[0]).state, "succeeded");
        assert_eq!(
            (inline.state.as_str(), inline.send_state.as_str()),
            ("failed", "ambiguous")
        );
        assert_eq!(
            submit.error_kind.as_deref(),
            Some("review_predecessor_failed")
        );
        assert_eq!(h.slot().unwrap().state, SlotState::CleanupRequired);
        assert_eq!(mutations(&fake.calls()), ["create", "inline"]);
    }

    #[tokio::test]
    async fn create_failing_before_send_retries_definitely_unsent_on_row_and_slot() {
        let h = review_harness();
        let [create, _, _] = h.admit_review_sequence();
        let fake = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::credential_unavailable(),
        ]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &create, |r| r.state == "queued" && r.attempts == 1).await;
        running.stop().await;

        assert_eq!(row.send_state, "definitely_unsent");
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Owned);
        assert_eq!(
            slot.create_send_state,
            quorum_core::collaboration_admission::GithubSendState::DefinitelyUnsent
        );
        assert_eq!(fake.calls().len(), 1);
        assert!(fake.reviews().reviews.is_empty());
    }

    #[tokio::test]
    async fn review_step_without_owned_slot_is_never_sent() {
        let h = review_harness();
        let [create, inline, _] = h.admit_review_sequence();
        // Cleanup was required before any claim: nothing is claimable.
        assert!(review_publication_slots::require_cleanup(
            &mut h.conn(),
            REVIEW_ATTEMPT,
            now_unix()
        )
        .unwrap());
        let fake = Arc::new(FakeRepositoryService::scripted([]));
        let running = Running::start(h.executor(fake.clone()));
        tokio::time::sleep(FAST_POLL * 10).await;
        running.stop().await;
        assert!(fake.calls().is_empty());
        assert_eq!(h.row(&create).state, "queued");
        assert_eq!(h.row(&inline).state, "queued");

        // A claimed inline whose slot vanished before preparation fails
        // unsent without a send.
        let plan = prepare_review(&h.conn(), &h.row(&inline)).unwrap();
        assert!(
            matches!(
                plan,
                Err(Disposition::FailedUnsent {
                    error_kind: "review_slot_lost",
                    ..
                })
            ),
            "{plan:?}"
        );
    }

    #[test]
    fn bounds_are_explicit() {
        assert_eq!(MAX_IN_FLIGHT, 4);
        assert_eq!(MAX_EXECUTION_ATTEMPTS, 8);
        assert_eq!(MAX_RESULT_BYTES, 64 * 1024);
        assert_eq!(MAX_REQUEST_BYTES, 64 * 1024);
        assert_eq!(retry_delay_secs(1), RETRY_BASE_SECS);
        assert_eq!(retry_delay_secs(2), RETRY_BASE_SECS * 2);
        assert_eq!(retry_delay_secs(MAX_EXECUTION_ATTEMPTS), RETRY_MAX_SECS);
        assert!(bounded_summary(&"é".repeat(5_000)).as_str().len() <= MAX_OPERATION_ERROR_BYTES);
        assert_eq!(bounded_summary("a\0b").as_str(), "a b");
        assert_eq!(bounded_summary("").as_str(), "github operation failed");
    }

    // -----------------------------------------------------------------------
    // Review-thread replies and resolution. Role gating is the endpoint's
    // (covered there); these drive the executor for one admitted row.
    // -----------------------------------------------------------------------

    use crate::serve::repository_service::ReviewReplyRecord;

    const REPLY_REQUEST: &str =
        r#"{"kind":"reply_to_review_comment","comment_id":12,"body":"Fixed in `abc`."}"#;
    const RESOLVE_REQUEST: &str = r#"{"kind":"resolve_review_thread","thread_node_id":"PRRT_a"}"#;

    fn thread(id: &str, ids: &[u64], resolved: bool, outdated: bool) -> ReviewThreadRecord {
        ReviewThreadRecord {
            node_id: id.to_string(),
            resolved,
            outdated,
            path: "src/lib.rs".to_string(),
            line: (!outdated).then_some(3),
            original_line: Some(3),
            start_line: None,
            side: Some("RIGHT".to_string()),
            subject_type: Some("LINE".to_string()),
            root_comment_id: ids[0],
            comment_ids: ids.to_vec(),
            commit_id: (!outdated).then(|| SHA.to_string()),
            original_commit_id: Some(SHA.to_string()),
            diff_hunk: "@@ -1,3 +1,3 @@\n a\n-b\n+c".to_string(),
        }
    }

    fn listing(threads: Vec<ReviewThreadRecord>) -> FakeOutcome {
        FakeOutcome::Succeed(RepositoryResponse::ReviewThreads(threads))
    }

    fn write_calls(fake: &FakeRepositoryService) -> usize {
        fake.calls()
            .iter()
            .filter(|call| call.request.effect() == OperationEffect::Write)
            .count()
    }

    fn result_json(row: &GithubAgentOperation) -> serde_json::Value {
        serde_json::from_str(row.response_json.as_deref().expect("result recorded")).unwrap()
    }

    async fn run_once(h: &Harness, id: &GithubOperationId, fake: Arc<FakeRepositoryService>) {
        let running = Running::start(h.executor(fake));
        wait_for(h, id, terminal).await;
        running.stop().await;
    }

    #[tokio::test]
    async fn reply_targets_the_thread_root_on_the_bound_pr_and_returns_thread_state() {
        let h = Harness::new();
        let id = h.admit(
            "req-reply",
            GithubOperationKind::AddReplyToPullRequestComment,
            REPLY_REQUEST,
        );
        let marker = h.row(&id).github_marker.unwrap();
        let body = compose_comment_body("Fixed in `abc`.", &marker);
        let fake = Arc::new(FakeRepositoryService::scripted([
            listing(vec![
                thread("PRRT_z", &[5], false, false),
                thread("PRRT_a", &[11, 12], false, true),
            ]),
            FakeOutcome::Succeed(RepositoryResponse::ReviewReply(ReviewReplyRecord {
                id: 77,
                node_id: "PRRC_77".into(),
                in_reply_to_id: 11,
                body: body.clone(),
            })),
        ]));
        run_once(&h, &id, fake.clone()).await;

        let row = h.row(&id);
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str()),
            ("succeeded", "confirmed")
        );
        assert_eq!(row.remote_object_id.as_deref(), Some("77"));
        let calls = fake.calls();
        assert_eq!(calls[0].request, RepositoryRequest::ListReviewThreads {});
        // The agent named a reply in the thread; GitHub's reply goes to its root.
        assert_eq!(
            calls[1].request,
            RepositoryRequest::ReplyToReviewComment {
                comment_id: 11,
                body
            }
        );
        assert!(calls.iter().all(|call| call.pr.get() == PR as u64));
        let value = result_json(&row);
        assert_eq!(value["id"], 77);
        assert_eq!(value["in_reply_to_id"], 11);
        let thread = &value["thread"];
        assert_eq!(thread["id"], "PRRT_a");
        assert_eq!(thread["path"], "src/lib.rs");
        assert_eq!(thread["original_revision"], SHA);
        assert_eq!(thread["revision"], serde_json::Value::Null);
        assert_eq!(thread["context"], "@@ -1,3 +1,3 @@\n a\n-b\n+c");
        assert_eq!(thread["resolved"], false);
        assert_eq!(thread["outdated"], true);
        assert!(!row.response_json.unwrap().contains(&marker));
    }

    #[tokio::test]
    async fn reply_to_a_comment_outside_the_bound_pr_threads_sends_nothing() {
        let h = Harness::new();
        let id = h.admit(
            "req-foreign",
            GithubOperationKind::AddReplyToPullRequestComment,
            REPLY_REQUEST,
        );
        // Comment 12 is on another PR (or is a general issue comment): it is
        // in none of the bound PR's threads.
        let fake = Arc::new(FakeRepositoryService::scripted([listing(vec![thread(
            "PRRT_z",
            &[5],
            false,
            false,
        )])]));
        run_once(&h, &id, fake.clone()).await;
        let row = h.row(&id);
        assert_eq!(row.state, "failed");
        assert_eq!(row.error_kind.as_deref(), Some("review_comment_not_found"));
        assert_eq!(row.send_state, "not_started");
        assert!(row.execution_started_at.is_none());
        assert_eq!(write_calls(&fake), 0);
        assert_eq!(h.errors(), 0);
    }

    #[tokio::test]
    async fn lost_reply_is_reconciled_by_marker_and_absence_is_never_resent() {
        let h = Harness::new();
        let found = h.admit(
            "req-lost-found",
            GithubOperationKind::AddReplyToPullRequestComment,
            REPLY_REQUEST,
        );
        let marker = h.row(&found).github_marker.unwrap();
        let fake = Arc::new(FakeRepositoryService::scripted([
            listing(vec![thread("PRRT_a", &[11, 12], false, false)]),
            FakeOutcome::timeout(),
            FakeOutcome::Succeed(RepositoryResponse::ReviewReplies(vec![
                ReviewReplyRecord {
                    id: 70,
                    node_id: "PRRC_70".into(),
                    in_reply_to_id: 11,
                    body: "someone else".into(),
                },
                ReviewReplyRecord {
                    id: 71,
                    node_id: "PRRC_71".into(),
                    in_reply_to_id: 11,
                    body: format!("Fixed.\n\n{marker}"),
                },
            ])),
        ]));
        run_once(&h, &found, fake.clone()).await;
        let row = h.row(&found);
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str()),
            ("succeeded", "confirmed")
        );
        assert_eq!(row.remote_object_id.as_deref(), Some("71"));
        assert_eq!(result_json(&row)["reconciled"], true);
        assert_eq!(write_calls(&fake), 1);
        assert_eq!(
            fake.calls()[2].request,
            RepositoryRequest::ListCommentReplies { comment_id: 11 }
        );

        let absent = h.admit(
            "req-lost-absent",
            GithubOperationKind::AddReplyToPullRequestComment,
            REPLY_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([
            listing(vec![thread("PRRT_a", &[11, 12], false, false)]),
            FakeOutcome::timeout(),
            FakeOutcome::Succeed(RepositoryResponse::ReviewReplies(Vec::new())),
        ]));
        run_once(&h, &absent, fake.clone()).await;
        let row = h.row(&absent);
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str()),
            ("failed", "ambiguous")
        );
        assert_eq!(write_calls(&fake), 1);

        // A later daemon never claims the ambiguous row for another send.
        let restarted = Arc::new(FakeRepositoryService::scripted([]));
        let running = Running::start(h.executor(restarted.clone()));
        tokio::time::sleep(FAST_POLL * 10).await;
        running.stop().await;
        assert!(restarted.calls().is_empty(), "ambiguous reply was re-sent");
        assert_eq!(h.row(&absent), row);
        assert_eq!(h.errors(), 0);
    }

    #[tokio::test]
    async fn reply_started_before_a_crash_is_ambiguous_after_restart_and_never_resent() {
        let h = Harness::new();
        let id = h.admit(
            "req-crash",
            GithubOperationKind::AddReplyToPullRequestComment,
            REPLY_REQUEST,
        );
        let claimed = github_operations::claim_next(&mut h.conn(), now_unix())
            .unwrap()
            .unwrap();
        assert!(github_operations::mark_execution_started(
            &mut h.conn(),
            &claimed.claim,
            now_unix()
        )
        .unwrap());
        let fake = Arc::new(FakeRepositoryService::scripted([]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, terminal).await;
        tokio::time::sleep(FAST_POLL * 10).await;
        running.stop().await;
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str()),
            ("failed", "ambiguous")
        );
        assert!(fake.calls().is_empty(), "interrupted reply was re-sent");
    }

    #[tokio::test]
    async fn resolve_is_bound_to_the_pr_idempotent_and_reconciled_by_state() {
        let h = Harness::new();
        // A thread that is not on the bound PR is refused before any send.
        let foreign = h.admit(
            "req-resolve-foreign",
            GithubOperationKind::ResolveReviewThread,
            RESOLVE_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([listing(vec![thread(
            "PRRT_z",
            &[5],
            false,
            false,
        )])]));
        run_once(&h, &foreign, fake.clone()).await;
        let row = h.row(&foreign);
        assert_eq!(row.state, "failed");
        assert_eq!(row.error_kind.as_deref(), Some("review_thread_not_found"));
        assert_eq!(row.send_state, "not_started");
        assert_eq!(write_calls(&fake), 0);

        // A lost response is confirmed by GitHub's resolved state, once.
        let lost = h.admit(
            "req-resolve-lost",
            GithubOperationKind::ResolveReviewThread,
            RESOLVE_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([
            listing(vec![thread("PRRT_a", &[11], false, true)]),
            FakeOutcome::timeout(),
            listing(vec![thread("PRRT_a", &[11], true, true)]),
        ]));
        run_once(&h, &lost, fake.clone()).await;
        let row = h.row(&lost);
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str()),
            ("succeeded", "confirmed")
        );
        let value = result_json(&row);
        assert_eq!(value["reconciled"], true);
        assert_eq!(value["thread"]["resolved"], true);
        assert_eq!(value["thread"]["outdated"], true);
        assert_eq!(
            fake.calls()[1].request,
            RepositoryRequest::ResolveReviewThread {
                thread_node_id: "PRRT_a".into()
            }
        );
        assert_eq!(write_calls(&fake), 1);

        // An already-resolved thread satisfies the request without a mutation.
        let again = h.admit(
            "req-resolve-again",
            GithubOperationKind::ResolveReviewThread,
            RESOLVE_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([listing(vec![thread(
            "PRRT_a",
            &[11],
            true,
            false,
        )])]));
        run_once(&h, &again, fake.clone()).await;
        let row = h.row(&again);
        assert_eq!(
            (row.state.as_str(), row.send_state.as_str()),
            ("succeeded", "confirmed")
        );
        assert_eq!(result_json(&row)["already_resolved"], true);
        assert_eq!(write_calls(&fake), 0);
        assert_eq!(h.errors(), 0);
    }

    #[tokio::test]
    async fn thread_lookup_outage_retries_provably_unsent() {
        let h = Harness::new();
        let id = h.admit(
            "req-outage",
            GithubOperationKind::AddReplyToPullRequestComment,
            REPLY_REQUEST,
        );
        let fake = Arc::new(FakeRepositoryService::scripted([FakeOutcome::ambiguous()]));
        let running = Running::start(h.executor(fake.clone()));
        let row = wait_for(&h, &id, |row| row.state == "queued" && row.attempts == 1).await;
        running.stop().await;
        // The lookup failed before any mutation was handed off.
        assert_eq!(row.send_state, "not_started");
        assert!(row.execution_started_at.is_none());
        assert_eq!(row.error_kind.as_deref(), Some("transient_failure"));
        assert_eq!(write_calls(&fake), 0);
    }
}
