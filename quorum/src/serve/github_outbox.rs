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
use quorum_core::Connection;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::repository_service::{
    CreatedComment, Delivery, OperationEffect, OutcomeClass, PrMetadata, PrState,
    PullRequestNumber, RepositoryError, RepositoryRequest, RepositoryResponse, RepositoryService,
    RepositoryTarget, DEFAULT_CALL_TIMEOUT, MAX_AGENT_REQUEST_BYTES,
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
/// full Repository Service call deadline plus slack for the settle.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(DEFAULT_CALL_TIMEOUT.as_secs() + 5);

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
            let text = response_json(&response).to_string();
            if text.len() > MAX_RESULT_BYTES {
                // A write that succeeded with an unrecordable result is still
                // never re-sent: ambiguous keeps it out of every retry path.
                return Disposition::Ambiguous {
                    error_kind: "result_too_large",
                    summary: format!("result exceeds {MAX_RESULT_BYTES} bytes"),
                };
            }
            let remote_object_id = match &response {
                RepositoryResponse::IssueComment(comment) => {
                    match RemoteObjectId::new(comment.id.to_string()) {
                        Ok(id) => Some(id),
                        Err(error) => {
                            return Disposition::Ambiguous {
                                error_kind: "invalid_result",
                                summary: error.to_string(),
                            }
                        }
                    }
                }
                _ => None,
            };
            return match GithubOperationResponseJson::new(&text) {
                Ok(json) => Disposition::Succeeded(json, remote_object_id),
                Err(error) => Disposition::Ambiguous {
                    error_kind: "invalid_result",
                    summary: error.to_string(),
                },
            };
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
    }
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
        record(
            &ctx.db_path,
            claim,
            Disposition::Ambiguous {
                error_kind: "not_resent",
                summary: "operation send may already have started; not re-sent".to_string(),
            },
        )
        .await?;
        return Ok(());
    }

    let result = ctx.service.execute(&ctx.target, pr, &request).await;
    record(&ctx.db_path, claim, disposition(effect, result)).await?;
    Ok(())
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
}
