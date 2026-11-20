//! Daemon-owned pending-review publication barrier and orphan-draft cleanup
//! (task #451).
//!
//! The slot state machine lives in
//! [`quorum_core::review_publication_slots`]; this module performs the
//! Repository Service I/O *between* its primitives and never holds a database
//! transaction across a remote call.
//!
//! **Provisioning barrier.** Before any reviewer (first review, re-review, R1,
//! or R2) gets resources, [`ReviewPublication::probe_before_provision`]
//! inserts or claims the `(publisher_scope, PR)` slot in `probing`, commits,
//! reads the publisher's pending reviews, and applies the guarded disposition:
//!
//! - empty listing → [`Barrier::Clear`]: the caller provisions and then
//!   [`ReviewPublication::bind`]s the live reviewer run, which issues its
//!   collaboration attempt and makes it the slot owner in one transaction. A
//!   provision that never reaches the bind gives the claim back with
//!   [`ReviewPublication::abandon`];
//! - exactly one exact-marker draft of a terminal Quorum attempt →
//!   `cleanup_required` and [`Barrier::Wait`]: no reviewer until the cleanup
//!   driver has deleted that draft;
//! - a foreign, multiple, mismatched, or live-attempt draft →
//!   [`Barrier::Blocked`]: nothing is deleted and no reviewer is provisioned;
//! - a failed read → one consumed cycle and [`Barrier::Wait`], so a GitHub
//!   outage can never let a reviewer bypass the barrier.
//!
//! **Cleanup driver.** [`ReviewPublication::run_cleanup`] claims due
//! `cleanup_required` slots one at a time and performs at most a
//! read / exact delete / exact re-read sequence for each claim. Only the
//! draft the core authorizes by exact owner marker, PR, publisher scope,
//! launch SHA, and stored review ID is ever deleted. Backoff, the shared
//! eight-cycle bound, and the one-hour window are enforced by the core
//! primitives; exhaustion is `blocked`.
//!
//! **Authority.** Probing and cleanup are publication housekeeping only: they
//! never post, infer, or substitute for a review verdict, transition a task,
//! request rework, write an `errors` row, or charge a provider/provision
//! budget. A blocked slot surfaces in `quorum status`.
//!
//! **Startup and shutdown.** Crash recovery (`recovery::recover`) runs
//! [`review_publication_slots::reconcile_on_start`] and then
//! [`ReviewPublication::mark_startup_reconciled`]; until then neither the
//! barrier nor the driver claims anything, so a `probing` or
//! `cleanup_running` slot left by a previous daemon is always re-read
//! remotely before any delete or release. Shutdown sets a flag: the driver
//! stops claiming and stops issuing calls for its current claim, which stays
//! `cleanup_running` for the next startup. An in-flight `gh` child is killed
//! and reaped when the daemon aborts the driver after its bounded grace
//! (`kill_on_drop`). Neither path releases a slot or changes lifecycle.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use quorum_core::collaboration_admission::{
    self, CollaborationAttemptId, GithubSendState, ReviewerSlotBinding, RunCapabilityId,
    MAX_GITHUB_MARKER_BYTES,
};
use quorum_core::error::{QuorumError, Result};
use quorum_core::review_publication_slots::{
    self, CleanupClaim, CleanupObservation, CleanupOutcome, PendingReviewCandidate, ProbeClaim,
    ProbeOutcome, ProbeResult, ProbeStart, PublisherScope, SlotState, SlotStatus,
    MAX_PENDING_REVIEW_CANDIDATES,
};
use quorum_core::Connection;

use super::repository_service::{
    PullRequestNumber, RepositoryRequest, RepositoryResponse, RepositoryService, RepositoryTarget,
    ReviewRecord, ReviewState, DEFAULT_CALL_TIMEOUT,
};
use super::{log, now_unix};

/// The self-hosted adapter's opaque publisher namespace. One daemon uses one
/// configured publication credential source per repository database, so every
/// pending review it can see belongs to this one actor namespace. It names no
/// user, account, installation, or key.
pub const PUBLISHER_SCOPE: &str = "repository-service";
/// Idle interval between cleanup claim ticks.
pub const CLEANUP_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Claims processed per tick before sleeping again.
pub const MAX_CLEANUPS_PER_TICK: usize = 4;
/// Bound on awaiting the driver after shutdown is requested: one claim's
/// read, delete, and re-read at the Repository Service call deadline, plus
/// slack for the settle transactions. After it the daemon aborts the driver,
/// which kills and reaps any in-flight `gh` child.
pub const CLEANUP_SHUTDOWN_GRACE: Duration =
    Duration::from_secs(3 * DEFAULT_CALL_TIMEOUT.as_secs() + 5);

/// Bound on how long a provisioning barrier waits for a concurrent driver
/// cycle on its own slot, and the autocommit re-read interval meanwhile.
const INFLIGHT_SETTLE_WAIT: Duration = Duration::from_secs(5);
const INFLIGHT_SETTLE_POLL: Duration = Duration::from_millis(100);

const OWNER_MARKER_PREFIX: &str = "<!-- quorum-review-owner:";
const MARKER_SUFFIX: &str = " -->";
/// Stand-in for a listed review GitHub returned without a usable commit ID.
/// It can never equal a launch SHA, so such a draft is never deleted.
const UNKNOWN_COMMIT: &str = "unknown-commit";
const MAX_COMMIT_SHA_BYTES: usize = 128;

/// What the provisioning barrier decided.
#[derive(Debug)]
pub enum Barrier {
    /// The probe holds the slot and observed no pending review: provision,
    /// then [`ReviewPublication::bind`] or [`ReviewPublication::abandon`].
    Clear(ProbeClaim),
    /// Cleanup, backoff, or another holder must finish first. Retry later;
    /// nothing was provisioned and no budget was charged.
    Wait(String),
    /// The slot is blocked: no reviewer may be provisioned for this PR.
    Blocked(String),
}

/// Daemon handle for the pending-review barrier and cleanup driver.
pub struct ReviewPublication {
    db_path: PathBuf,
    target: RepositoryTarget,
    service: Arc<dyn RepositoryService>,
    scope: PublisherScope,
    startup_reconciled: AtomicBool,
    /// Last logged wait reason per `(task, PR)`, so a waiting provision that
    /// retries every tick logs only when its reason changes. Bounded.
    logged_waits: std::sync::Mutex<std::collections::HashMap<(i64, i64), String>>,
}

impl std::fmt::Debug for ReviewPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReviewPublication")
            .field("db_path", &self.db_path)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl ReviewPublication {
    pub fn new(
        db_path: PathBuf,
        target: RepositoryTarget,
        service: Arc<dyn RepositoryService>,
    ) -> Result<Self> {
        Ok(Self {
            db_path,
            target,
            service,
            scope: PublisherScope::new(PUBLISHER_SCOPE)?,
            startup_reconciled: AtomicBool::new(false),
            logged_waits: Default::default(),
        })
    }

    /// Build the daemon's handle from config over the real `gh` Repository
    /// Service. `None` (logged) when the configured repository is invalid —
    /// the same condition that disables the outbox, so no reviewer can then
    /// publish a pending review either.
    pub fn from_config(config: &super::ServeConfig) -> Option<Arc<Self>> {
        let target = match RepositoryTarget::new(config.repo_dir.clone(), Some(&config.repo)) {
            Ok(target) => target,
            Err(error) => {
                log(&format!(
                    "review cleanup: disabled; configured repository {:?} is invalid: {error}",
                    config.repo
                ));
                return None;
            }
        };
        let service = Arc::new(super::repository_service::GhRepositoryService::new(
            config.merge_executor.publication_credentials(),
        ));
        match Self::new(config.db_path.clone(), target, service) {
            Ok(publication) => Some(Arc::new(publication)),
            Err(error) => {
                log(&format!("review cleanup: disabled: {error}"));
                None
            }
        }
    }

    /// Called by crash recovery once startup slot reconciliation committed.
    pub fn mark_startup_reconciled(&self) {
        self.startup_reconciled.store(true, Ordering::SeqCst);
    }

    fn is_startup_reconciled(&self) -> bool {
        self.startup_reconciled.load(Ordering::SeqCst)
    }

    async fn db<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        with_conn(&self.db_path, f).await
    }

    // -----------------------------------------------------------------------
    // Provisioning barrier
    // -----------------------------------------------------------------------

    /// The barrier every reviewer provision passes before allocating any
    /// reviewer resource. See the module docs for the dispositions. A
    /// [`Barrier::Wait`] is logged here, once per distinct reason.
    pub async fn probe_before_provision(&self, task_id: i64, pr: i64) -> Result<Barrier> {
        let barrier = self.probe(task_id, pr).await?;
        match &barrier {
            Barrier::Wait(reason) => self.log_wait(task_id, pr, reason),
            Barrier::Clear(_) | Barrier::Blocked(_) => {
                self.logged_waits
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&(task_id, pr));
            }
        }
        Ok(barrier)
    }

    fn log_wait(&self, task_id: i64, pr: i64, reason: &str) {
        const MAX_LOGGED_WAITS: usize = 256;
        let mut logged = self
            .logged_waits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if logged.get(&(task_id, pr)).map(String::as_str) == Some(reason) {
            return;
        }
        if logged.len() >= MAX_LOGGED_WAITS {
            logged.clear();
        }
        logged.insert((task_id, pr), reason.to_string());
        log(&format!(
            "review publication: task #{task_id} PR #{pr} reviewer deferred: {reason}"
        ));
    }

    async fn probe(&self, task_id: i64, pr: i64) -> Result<Barrier> {
        if !self.is_startup_reconciled() {
            return Ok(Barrier::Wait(
                "startup publication-slot reconciliation has not completed".into(),
            ));
        }
        let pr_number = PullRequestNumber::new(pr)
            .ok_or_else(|| QuorumError::Usage(format!("invalid review PR #{pr}")))?;
        let mut start = self.begin(task_id, pr).await?;
        if let ProbeStart::Occupied(status) = &start {
            // A previous reviewer's slot (re-review, R1→R2 handoff, restart)
            // is cleaned up inline, at most one bounded cycle per call, so
            // the next reviewer does not wait for the driver's poll.
            if matches!(status.state, SlotState::Owned | SlotState::CleanupRequired) {
                self.cleanup_claimed(Some(pr), &AtomicBool::new(false))
                    .await?;
                start = self.begin(task_id, pr).await?;
            }
        }
        // The background driver may hold this slot's cycle right now; give a
        // short in-flight cycle a bounded chance to settle rather than
        // deferring the reviewer to the next head poll.
        let settle_until = std::time::Instant::now() + INFLIGHT_SETTLE_WAIT;
        while matches!(&start, ProbeStart::Occupied(status) if status.state == SlotState::CleanupRunning)
            && std::time::Instant::now() < settle_until
        {
            tokio::time::sleep(INFLIGHT_SETTLE_POLL).await;
            start = self.begin(task_id, pr).await?;
        }
        let claim = match start {
            ProbeStart::Claimed(claim) => claim,
            ProbeStart::Occupied(status) => return Ok(occupied(&status)),
            ProbeStart::CapacityExceeded => {
                return Ok(Barrier::Wait(
                    "publication-slot capacity is exhausted".into(),
                ))
            }
        };
        self.read_probe(claim, pr_number).await
    }

    /// Claim the probe, or report the occupying slot from an autocommit read.
    async fn begin(&self, task_id: i64, pr: i64) -> Result<ProbeStart> {
        let scope = self.scope.clone();
        self.db(move |conn| {
            let now = now_unix();
            // An autocommit read keeps a waiting provision, retried every
            // tick, off the write lock until the slot could be claimed.
            if let Some(status) = review_publication_slots::get(conn, &scope, pr)? {
                let due = status.next_cleanup_at.is_some_and(|next| next <= now);
                if status.state != SlotState::Probing || status.task_id != task_id || !due {
                    return Ok(ProbeStart::Occupied(status));
                }
            }
            review_publication_slots::begin_probe(conn, &scope, pr, task_id, now)
        })
        .await
    }

    /// The committed probe's remote read and guarded disposition.
    async fn read_probe(&self, claim: ProbeClaim, pr_number: PullRequestNumber) -> Result<Barrier> {
        // The probe claim is committed; the remote read holds no transaction.
        let listing = self
            .service
            .execute(
                &self.target,
                pr_number,
                &RepositoryRequest::ListPendingReviews {},
            )
            .await;
        let reviews = match listing {
            Ok(RepositoryResponse::PendingReviews(reviews)) => Some(reviews),
            Ok(_) | Err(_) => None,
        };
        let outcome = match reviews {
            Some(reviews) if reviews.is_empty() => return Ok(Barrier::Clear(claim)),
            Some(reviews) => {
                let candidates = owned_candidates(&reviews);
                self.db(move |conn| {
                    let borrowed = borrow_candidates(&candidates);
                    review_publication_slots::resolve_probe(
                        conn,
                        &claim,
                        ProbeResult::Pending {
                            candidates: &borrowed,
                        },
                        now_unix(),
                    )
                })
                .await?
            }
            None => {
                self.db(move |conn| {
                    review_publication_slots::resolve_probe(
                        conn,
                        &claim,
                        ProbeResult::Inconclusive,
                        now_unix(),
                    )
                })
                .await?
            }
        };
        Ok(probe_barrier(outcome))
    }

    /// Issue the live reviewer run's collaboration attempt and make it the
    /// owner of the probed slot in one transaction. `false` (logged) when it
    /// could not: the claim is given back, and the reviewer runs without an
    /// owned slot, so it can never publish a pending review.
    pub async fn bind(&self, claim: ProbeClaim, run_id: &str) -> Result<bool> {
        let run_id = RunCapabilityId::new(run_id.to_string())?;
        let attempt_id = CollaborationAttemptId::new(format!("att-{}", uuid::Uuid::new_v4()))?;
        let bound_claim = claim.clone();
        let binding = self
            .db(move |conn| {
                collaboration_admission::bind_reviewer_run_to_probed_slot(
                    conn,
                    &run_id,
                    &attempt_id,
                    super::agent_endpoint::ATTEMPT_RETENTION_SECS,
                    &bound_claim,
                    now_unix(),
                )
            })
            .await;
        let binding = match binding {
            Ok(binding) => binding,
            Err(error) => {
                // The bind rolled back; never leave the claim in flight.
                self.abandon(claim).await;
                return Err(error);
            }
        };
        match binding {
            ReviewerSlotBinding::Owned(_) => Ok(true),
            ReviewerSlotBinding::AttemptRejected(rejection) => {
                log(&format!(
                    "review publication: PR #{} reviewer attempt unavailable ({rejection:?}); \
                     the reviewer cannot publish a pending review",
                    claim.pr_number
                ));
                self.abandon(claim).await;
                Ok(false)
            }
            ReviewerSlotBinding::SlotRejected(outcome) => {
                log(&format!(
                    "review publication: PR #{} slot ownership refused ({outcome:?}); \
                     the reviewer cannot publish a pending review",
                    claim.pr_number
                ));
                self.abandon(claim).await;
                Ok(false)
            }
        }
    }

    /// R1→R2 handoff: end the retiring R1 run's publication authority before
    /// R2's barrier probes. The run capability is revoked, its attempt is
    /// terminalized, and its `owned` slot becomes `cleanup_required` in one
    /// transaction, so the R2 barrier's inline cleanup can claim it at once
    /// (never attaching R2 to R1's draft). Failures are logged: the barrier
    /// then waits and R2 is retried from Phase 5, as before. No verdict,
    /// lifecycle, or budget effect.
    pub async fn retire_for_handoff(&self, r1_run_id: &str) {
        let run_id = r1_run_id.to_string();
        let result = self
            .db(move |conn| {
                review_publication_slots::retire_owner_for_handoff(conn, &run_id, now_unix())
            })
            .await;
        if let Err(error) = result {
            log(&format!(
                "review publication: could not end R1 run {r1_run_id}'s publication \
                 authority before the R2 handoff: {error}"
            ));
        }
    }

    /// Give back a cleared probe claim whose provision never reached a
    /// successful [`Self::bind`]. The slot was never owned, so the core
    /// deletes it and the next provision repeats the fresh probe. Failures
    /// are logged: the in-flight claim is then fenced at the next startup.
    pub async fn abandon(&self, claim: ProbeClaim) {
        let pr = claim.pr_number;
        let result = self
            .db(move |conn| review_publication_slots::release_unbound_probe(conn, &claim))
            .await;
        if let Err(error) = result {
            log(&format!(
                "review publication: PR #{pr} could not give back its probe claim: {error}"
            ));
        }
    }

    // -----------------------------------------------------------------------
    // Cleanup driver
    // -----------------------------------------------------------------------

    /// Claim and drive due cleanup slots until `shutdown` is set. Waits for
    /// startup reconciliation before the first claim.
    pub async fn run_cleanup(self: Arc<Self>, shutdown: Arc<AtomicBool>, poll: Duration) {
        while !shutdown.load(Ordering::SeqCst) {
            if self.is_startup_reconciled() {
                for _ in 0..MAX_CLEANUPS_PER_TICK {
                    if shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    match self.cleanup_once(&shutdown).await {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(error) => {
                            log(&format!("review cleanup: tick failed: {error}"));
                            break;
                        }
                    }
                }
            }
            sleep_unless_shutdown(poll, &shutdown).await;
        }
    }

    /// Claim and drive at most one due cleanup slot. `Ok(false)` when none
    /// was claimable (or shutdown was already requested).
    pub async fn cleanup_once(&self, shutdown: &AtomicBool) -> Result<bool> {
        self.cleanup_claimed(None, shutdown).await
    }

    /// [`Self::cleanup_once`], optionally restricted to this daemon's slot
    /// for one PR (the provisioning barrier cleans exactly what it waits on).
    async fn cleanup_claimed(&self, only_pr: Option<i64>, shutdown: &AtomicBool) -> Result<bool> {
        if shutdown.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let scope = self.scope.clone();
        let Some(claim) = self
            .db(move |conn| match only_pr {
                Some(pr) => {
                    review_publication_slots::claim_cleanup_for(conn, &scope, pr, now_unix())
                }
                None => review_publication_slots::claim_cleanup(conn, now_unix()),
            })
            .await?
        else {
            return Ok(false);
        };
        let pr = claim.pr_number;
        match self.drive(&claim, shutdown).await? {
            None => log(&format!(
                "review cleanup: PR #{pr} shutdown interrupted cycle {}; the slot stays \
                 cleanup_running for startup reconciliation",
                claim.cycle
            )),
            Some(CleanupOutcome::Released) => log(&format!(
                "review cleanup: PR #{pr} publication slot released (cycle {})",
                claim.cycle
            )),
            Some(CleanupOutcome::Blocked(reason)) => log(&format!(
                "review cleanup: PR #{pr} publication slot blocked: {}",
                reason.as_str()
            )),
            Some(CleanupOutcome::Rescheduled { next_cleanup_at }) => log(&format!(
                "review cleanup: PR #{pr} cycle {} inconclusive; next attempt at {next_cleanup_at}",
                claim.cycle
            )),
            Some(CleanupOutcome::Lost) => log(&format!(
                "review cleanup: PR #{pr} cycle {} superseded",
                claim.cycle
            )),
            Some(CleanupOutcome::DeleteAuthorized { .. }) => {
                return Err(QuorumError::Io(
                    "review cleanup: delete authorization left unresolved".into(),
                ))
            }
        }
        Ok(true)
    }

    /// One bounded read → exact delete → exact re-read sequence. `None` when
    /// shutdown interrupted it before the next remote call.
    async fn drive(
        &self,
        claim: &CleanupClaim,
        shutdown: &AtomicBool,
    ) -> Result<Option<CleanupOutcome>> {
        let pr = PullRequestNumber::new(claim.pr_number)
            .ok_or_else(|| QuorumError::Io("publication slot PR is invalid".into()))?;
        let observed = self.observe(pr, claim).await;
        let outcome = self.complete(claim, observed).await?;
        let CleanupOutcome::DeleteAuthorized { review_id } = outcome else {
            return Ok(Some(outcome));
        };
        if shutdown.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let deleted = self.delete_exact(pr, &review_id).await;
        let outcome = self.complete(claim, deleted).await?;
        if matches!(outcome, CleanupOutcome::DeleteAuthorized { .. }) {
            return Err(QuorumError::Io(
                "review cleanup: exact delete observation re-authorized a delete".into(),
            ));
        }
        Ok(Some(outcome))
    }

    /// The first read of a claim: the publisher's pending reviews, or — for a
    /// `confirmed` create whose listing is empty — the exact stored review ID.
    async fn observe(&self, pr: PullRequestNumber, claim: &CleanupClaim) -> Observed {
        let listing = self
            .service
            .execute(&self.target, pr, &RepositoryRequest::ListPendingReviews {})
            .await;
        let reviews = match listing {
            Ok(RepositoryResponse::PendingReviews(reviews)) => reviews,
            Ok(_) | Err(_) => return Observed::Inconclusive,
        };
        if reviews.is_empty() && claim.create_send_state == GithubSendState::Confirmed {
            // Never trust an empty listing for a confirmed create: address
            // the exact persisted ID instead.
            return match claim.pending_review_id.as_deref() {
                Some(review_id) => self.exact_not_pending(pr, review_id).await,
                None => Observed::Inconclusive,
            };
        }
        Observed::Listing(owned_candidates(&reviews))
    }

    /// Delete exactly `review_id`; on any non-proof answer, re-read it.
    async fn delete_exact(&self, pr: PullRequestNumber, review_id: &str) -> Observed {
        let Ok(id) = review_id.parse::<u64>() else {
            return Observed::Inconclusive;
        };
        let deleted = self
            .service
            .execute(
                &self.target,
                pr,
                &RepositoryRequest::DeletePendingReview { review_id: id },
            )
            .await;
        match deleted {
            Ok(RepositoryResponse::Review(review)) if review.id == id => {
                Observed::ExactDeleted(review_id.to_string())
            }
            // 404 after the exact delete: the one create is already gone.
            Ok(RepositoryResponse::ReviewAbsent { review_id: absent }) if absent == id => {
                Observed::ExactDeleted(review_id.to_string())
            }
            Ok(_) | Err(_) => self.exact_not_pending(pr, review_id).await,
        }
    }

    /// Authoritative exact-ID lookup: proof only when the review is absent
    /// or no longer pending.
    async fn exact_not_pending(&self, pr: PullRequestNumber, review_id: &str) -> Observed {
        let Ok(id) = review_id.parse::<u64>() else {
            return Observed::Inconclusive;
        };
        let read = self
            .service
            .execute(
                &self.target,
                pr,
                &RepositoryRequest::GetReview { review_id: id },
            )
            .await;
        match read {
            Ok(RepositoryResponse::Review(review))
                if review.id == id && review.state != ReviewState::Pending =>
            {
                Observed::ExactNotPending(review_id.to_string())
            }
            Ok(RepositoryResponse::ReviewAbsent { review_id: absent }) if absent == id => {
                Observed::ExactNotPending(review_id.to_string())
            }
            Ok(_) | Err(_) => Observed::Inconclusive,
        }
    }

    async fn complete(&self, claim: &CleanupClaim, observed: Observed) -> Result<CleanupOutcome> {
        let claim = claim.clone();
        self.db(move |conn| {
            let now = now_unix();
            match &observed {
                Observed::Listing(candidates) => {
                    let borrowed = borrow_candidates(candidates);
                    review_publication_slots::complete_cleanup(
                        conn,
                        &claim,
                        CleanupObservation::Listing {
                            candidates: &borrowed,
                            // The self-hosted `gh` adapter has no publisher
                            // ordering fence; a generic GET never is one.
                            fenced: false,
                        },
                        now,
                    )
                }
                Observed::ExactDeleted(review_id) => review_publication_slots::complete_cleanup(
                    conn,
                    &claim,
                    CleanupObservation::ExactDeleted { review_id },
                    now,
                ),
                Observed::ExactNotPending(review_id) => review_publication_slots::complete_cleanup(
                    conn,
                    &claim,
                    CleanupObservation::ExactNotPending { review_id },
                    now,
                ),
                Observed::Inconclusive => review_publication_slots::complete_cleanup(
                    conn,
                    &claim,
                    CleanupObservation::Inconclusive,
                    now,
                ),
            }
        })
        .await
    }
}

/// An owned cleanup observation, so it can cross into a blocking DB task.
#[derive(Debug, Clone)]
enum Observed {
    Listing(Vec<OwnedCandidate>),
    ExactDeleted(String),
    ExactNotPending(String),
    Inconclusive,
}

#[derive(Debug, Clone)]
struct OwnedCandidate {
    review_id: String,
    marker: Option<String>,
    commit_sha: String,
}

/// Bounded owned candidates for one listing. A listing longer than the core
/// bound is truncated, which still reports multiple drafts.
fn owned_candidates(reviews: &[ReviewRecord]) -> Vec<OwnedCandidate> {
    reviews
        .iter()
        .take(MAX_PENDING_REVIEW_CANDIDATES)
        .map(|review| OwnedCandidate {
            review_id: review.id.to_string(),
            marker: owner_marker_in(&review.body).map(str::to_owned),
            commit_sha: review
                .commit_id
                .as_deref()
                .filter(|sha| !sha.is_empty() && sha.len() <= MAX_COMMIT_SHA_BYTES)
                .filter(|sha| !sha.contains('\0'))
                .unwrap_or(UNKNOWN_COMMIT)
                .to_string(),
        })
        .collect()
}

fn borrow_candidates(candidates: &[OwnedCandidate]) -> Vec<PendingReviewCandidate<'_>> {
    candidates
        .iter()
        .map(|candidate| PendingReviewCandidate {
            review_id: &candidate.review_id,
            marker: candidate.marker.as_deref(),
            commit_sha: &candidate.commit_sha,
        })
        .collect()
}

/// The one Quorum owner marker a pending-review body carries. A body with no
/// marker, two different markers, or a malformed/oversized marker yields
/// `None` and is therefore treated as foreign (never deleted).
fn owner_marker_in(body: &str) -> Option<&str> {
    let mut found: Option<&str> = None;
    let mut offset = 0;
    while let Some(relative) = body[offset..].find(OWNER_MARKER_PREFIX) {
        let start = offset + relative;
        let end = body[start..].find(MARKER_SUFFIX)? + start + MARKER_SUFFIX.len();
        let marker = &body[start..end];
        if marker.len() > MAX_GITHUB_MARKER_BYTES || marker.contains('\0') {
            return None;
        }
        match found {
            Some(previous) if previous != marker => return None,
            _ => found = Some(marker),
        }
        offset = end;
    }
    found
}

fn occupied(status: &SlotStatus) -> Barrier {
    let detail = format!(
        "publication slot for PR #{} is {} (task #{}, cycle {}{})",
        status.pr_number,
        status.state.as_str(),
        status.task_id,
        status.cleanup_attempts,
        status
            .error_kind
            .as_deref()
            .map(|kind| format!(", {kind}"))
            .unwrap_or_default()
    );
    if status.state == SlotState::Blocked {
        Barrier::Blocked(detail)
    } else {
        Barrier::Wait(detail)
    }
}

fn probe_barrier(outcome: ProbeOutcome) -> Barrier {
    match outcome {
        ProbeOutcome::Blocked(reason) => {
            Barrier::Blocked(format!("publication slot blocked: {}", reason.as_str()))
        }
        ProbeOutcome::CleanupRequired(status) => Barrier::Wait(format!(
            "orphan Quorum pending review on PR #{} must be cleaned up first",
            status.pr_number
        )),
        ProbeOutcome::Rescheduled { next_probe_at } => Barrier::Wait(format!(
            "pending-review probe inconclusive; next probe at {next_probe_at}"
        )),
        ProbeOutcome::Lost => Barrier::Wait("pending-review probe claim was superseded".into()),
        ProbeOutcome::Owned(_) | ProbeOutcome::AttemptRejected => {
            Barrier::Wait("pending-review probe resolved unexpectedly".into())
        }
    }
}

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
    .map_err(|error| QuorumError::Io(format!("review cleanup DB task failed: {error}")))?
}

async fn sleep_unless_shutdown(total: Duration, shutdown: &AtomicBool) {
    let step = Duration::from_millis(100).min(total);
    let mut slept = Duration::ZERO;
    while slept < total && !shutdown.load(Ordering::SeqCst) {
        tokio::time::sleep(step).await;
        slept += step;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::repository_service::{FakeOutcome, FakeRepositoryService, FakeReviewStore};
    use quorum_core::review_publication_slots::SlotBlockReason;

    const TASK: i64 = 31;
    const PR: i64 = 42;
    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
    const OTHER_SHA: &str = "89abcdef0123456789abcdef0123456789abcdef";

    struct Harness {
        _dir: tempfile::TempDir,
        db: PathBuf,
        repo_dir: PathBuf,
    }

    impl Harness {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("q.db");
            let conn = quorum_core::db::open(&db).unwrap();
            conn.execute(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at,refs)
                 VALUES (?1,'review','in-review','owner',1,1,?2)",
                rusqlite::params![TASK, serde_json::json!({ "pr": PR }).to_string()],
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

        fn publication(&self, service: Arc<FakeRepositoryService>) -> ReviewPublication {
            let target =
                RepositoryTarget::new(self.repo_dir.clone(), Some("acme/widgets")).unwrap();
            let publication = ReviewPublication::new(self.db.clone(), target, service).unwrap();
            publication.mark_startup_reconciled();
            publication
        }

        /// A provisioned live reviewer run: capability, reviewer run at the
        /// launch SHA, and task reviewer pointer (what provisioning commits
        /// before it binds).
        fn live_reviewer(&self, run_id: &str, agent: &str, sha: &str) {
            let mut conn = self.conn();
            let now = now_unix();
            conn.execute(
                "UPDATE tasks SET reviewer=?2 WHERE id=?1",
                rusqlite::params![TASK, agent],
            )
            .unwrap();
            quorum_core::capabilities::issue(&mut conn, run_id, TASK, agent, "reviewer", now)
                .unwrap();
            quorum_core::agent_runs::insert_reviewer_with_launch(
                &conn, TASK, agent, "model", "high", "codex", None, now, None, run_id, PR, sha,
            )
            .unwrap()
            .unwrap();
        }

        /// The previous daemon's reviewer dies with it: crash recovery
        /// revokes its capability and ends its run.
        fn retire_reviewer(&self, run_id: &str) {
            let mut conn = self.conn();
            quorum_core::capabilities::revoke(&mut conn, run_id, now_unix()).unwrap();
            conn.execute(
                "UPDATE agent_runs SET ended_at=?2 WHERE review_cap_run_id=?1",
                rusqlite::params![run_id, now_unix()],
            )
            .unwrap();
        }

        fn slot(&self) -> Option<SlotStatus> {
            review_publication_slots::get(
                &self.conn(),
                &PublisherScope::new(PUBLISHER_SCOPE).unwrap(),
                PR,
            )
            .unwrap()
        }

        fn attempt_for_run(&self, run_id: &str) -> String {
            self.conn()
                .query_row(
                    "SELECT attempt_id FROM github_collaboration_attempts WHERE active_run_id=?1",
                    [run_id],
                    |row| row.get(0),
                )
                .unwrap()
        }

        fn marker(&self, attempt_id: &str) -> String {
            review_publication_slots::owner_marker_for_attempt(&self.conn(), attempt_id)
                .unwrap()
                .unwrap()
                .as_str()
                .to_string()
        }

        /// The owned attempt's create reached GitHub but the daemon died
        /// before it learned the result: the create row is `running` with an
        /// `ambiguous` send state, exactly as a crash mid-send leaves it.
        fn crash_during_create(&self, attempt_id: &str, run_id: &str) {
            let now = now_unix();
            let request = quorum_core::github_operations::NewGithubOperation {
                operation_id: "op-create",
                client_request_id: "create",
                attempt_id,
                created_by_run_id: run_id,
                task_id: TASK,
                agent: "Reviewer-A",
                role: "reviewer",
                pr_number: PR,
                head_sha: Some(SHA),
                reviewer_launch_sha: Some(SHA),
                lifecycle_generation: Some(0),
                kind: "pull_request_review_write",
                request_json: r#"{"method":"create"}"#,
                deadline_at: now + 1_800,
                review_sequence: Some(0),
                group_key: None,
                github_marker: None,
                created_at: now,
                expires_at: now + 3_600,
            };
            let mut conn = self.conn();
            assert!(quorum_core::github_operations::enqueue(&mut conn, &request).unwrap());
            let claimed = quorum_core::github_operations::claim_next(&mut conn, now)
                .unwrap()
                .expect("create claimable while the slot is owned");
            quorum_core::github_operations::mark_execution_started(&mut conn, &claimed.claim, now)
                .unwrap();
        }

        /// What a daemon restart does to slots before any reviewer: the
        /// outbox requeues/settles orphaned claims, then crash recovery
        /// revokes stale capabilities and reconciles the slots.
        fn restart(&self, stale_runs: &[&str]) {
            let mut conn = self.conn();
            quorum_core::github_operations::reconcile_on_start(&mut conn, now_unix()).unwrap();
            for run in stale_runs {
                self.retire_reviewer(run);
            }
            review_publication_slots::reconcile_on_start(&mut conn, now_unix()).unwrap();
        }

        fn lifecycle_snapshot(&self) -> (String, i64, i64, i64) {
            let conn = self.conn();
            let (status, rework): (String, i64) = conn
                .query_row(
                    "SELECT status, rework_round FROM tasks WHERE id=?1",
                    [TASK],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            let errors: i64 = conn
                .query_row("SELECT COUNT(*) FROM errors", [], |row| row.get(0))
                .unwrap();
            let verdicts: i64 = conn
                .query_row("SELECT COUNT(*) FROM approvals", [], |row| row.get(0))
                .unwrap();
            (status, rework, errors, verdicts)
        }
    }

    fn pr() -> PullRequestNumber {
        PullRequestNumber::new(PR).unwrap()
    }

    fn mutations(service: &FakeRepositoryService) -> Vec<&'static str> {
        service
            .calls()
            .iter()
            .map(|call| match call.request {
                RepositoryRequest::ListPendingReviews {} => "list",
                RepositoryRequest::DeletePendingReview { .. } => "delete",
                RepositoryRequest::GetReview { .. } => "get",
                RepositoryRequest::CreatePendingReview { .. } => "create",
                RepositoryRequest::SubmitPendingReview { .. } => "submit",
                _ => "other",
            })
            .collect()
    }

    /// First reviewer: empty probe, provision, bind.
    async fn provision_first(h: &Harness, run_id: &str, agent: &str) -> String {
        let service = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]));
        let publication = h.publication(service.clone());
        let Barrier::Clear(claim) = publication.probe_before_provision(TASK, PR).await.unwrap()
        else {
            panic!("an empty listing clears the barrier");
        };
        h.live_reviewer(run_id, agent, SHA);
        assert!(publication.bind(claim, run_id).await.unwrap());
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Owned);
        let attempt = h.attempt_for_run(run_id);
        assert_eq!(slot.attempt_id.as_deref(), Some(attempt.as_str()));
        attempt
    }

    /// Restart → the next reviewer waits; cleanup deletes exactly the orphan;
    /// only then does the barrier clear. Shared by the re-review and R1→R2
    /// cases, which differ only in who the next reviewer is.
    async fn orphan_cleaned_before_next_reviewer(next_run: &str, next_agent: &str) {
        let h = Harness::new();
        let attempt = provision_first(&h, "run-a", "Reviewer-A").await;
        let marker = h.marker(&attempt);
        h.crash_during_create(&attempt, "run-a");
        // The create landed on GitHub; an unrelated PR has its own draft.
        let mut store = FakeReviewStore::default();
        let orphan = store.seed_pending(pr(), SHA, &format!("draft\n\n{marker}"));
        let elsewhere = store.seed_pending(PullRequestNumber::new(PR + 1).unwrap(), SHA, "other");
        let before = h.lifecycle_snapshot();

        h.restart(&["run-a"]);
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::CleanupRequired, "{slot:?}");
        assert_eq!(slot.create_send_state, GithubSendState::Ambiguous);

        // The next reviewer's barrier first deletes exactly the orphan
        // (read -> exact delete), and only then probes fresh and clears.
        let service = Arc::new(
            FakeRepositoryService::scripted([
                FakeOutcome::Apply, // cleanup: list
                FakeOutcome::Apply, // cleanup: exact delete
                FakeOutcome::Apply, // fresh probe: list
            ])
            .with_reviews(store),
        );
        let publication = h.publication(service.clone());
        let barrier = publication.probe_before_provision(TASK, PR).await.unwrap();
        let remote = service.reviews();
        assert!(remote.reviews.iter().all(|r| r.record.id != orphan));
        assert!(
            remote.reviews.iter().any(|r| r.record.id == elsewhere),
            "only the exact-marker draft is deleted"
        );
        let deleted: Vec<u64> = service
            .calls()
            .iter()
            .filter_map(|call| match call.request {
                RepositoryRequest::DeletePendingReview { review_id } => Some(review_id),
                _ => None,
            })
            .collect();
        assert_eq!(deleted, vec![orphan]);
        assert_eq!(mutations(&service), vec!["list", "delete", "list"]);
        let Barrier::Clear(claim) = barrier else {
            panic!("the barrier clears after cleanup");
        };
        h.live_reviewer(next_run, next_agent, SHA);
        assert!(publication.bind(claim, next_run).await.unwrap());
        let next_attempt = h.attempt_for_run(next_run);
        assert_ne!(next_attempt, attempt);
        assert_ne!(h.marker(&next_attempt), marker, "a fresh owner marker");
        assert_eq!(mutations(&service), vec!["list", "delete", "list"]);
        assert_eq!(
            h.lifecycle_snapshot(),
            before,
            "no lifecycle, error, or verdict"
        );
    }

    #[tokio::test]
    async fn orphan_draft_is_deleted_by_exact_marker_after_restart_before_re_review() {
        orphan_cleaned_before_next_reviewer("run-b", "Reviewer-B").await;
    }

    #[tokio::test]
    async fn r1_to_r2_handoff_waits_for_owned_slot_and_cleanup() {
        // R1 still owns the slot while R2 is provisioned: the handoff waits.
        let h = Harness::new();
        let r1_attempt = provision_first(&h, "run-r1", "Reviewer-R1").await;
        let service = Arc::new(FakeRepositoryService::scripted([]));
        let publication = h.publication(service.clone());
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Wait(_)
        ));
        assert!(service.calls().is_empty());
        assert_eq!(
            h.slot().unwrap().attempt_id.as_deref(),
            Some(r1_attempt.as_str())
        );
        // The R2 gate ends live R1's publication authority before it
        // provisions R2 (no restart, R1's run still open): the R2 barrier
        // cleans R1's draft-free slot inline, then probes fresh, and R2 owns
        // the slot under a new marker.
        publication.retire_for_handoff("run-r1").await;
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::CleanupRequired, "{slot:?}");
        let revoked: Option<i64> = h
            .conn()
            .query_row(
                "SELECT revoked_at FROM run_capabilities WHERE run_id='run-r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(revoked.is_some(), "R1's capability is fenced");
        let service = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::Apply, // cleanup: list (empty, create never sent)
            FakeOutcome::Apply, // fresh probe: list
        ]));
        let publication = h.publication(service.clone());
        let Barrier::Clear(claim) = publication.probe_before_provision(TASK, PR).await.unwrap()
        else {
            panic!("the R2 barrier clears once R1's slot is released");
        };
        assert_eq!(mutations(&service), vec!["list", "list"]);
        h.live_reviewer("run-r2", "Reviewer-R2", SHA);
        assert!(publication.bind(claim, "run-r2").await.unwrap());
        let r2_attempt = h.attempt_for_run("run-r2");
        assert_eq!(
            h.slot().unwrap().attempt_id.as_deref(),
            Some(r2_attempt.as_str())
        );
        assert_ne!(h.marker(&r2_attempt), h.marker(&r1_attempt));
        // Then the full restart path for the R2 reviewer.
        orphan_cleaned_before_next_reviewer("run-r2", "Reviewer-R2").await;
    }

    #[tokio::test]
    async fn r1_handoff_cancels_unsent_r1_create_and_r2_never_attaches_to_it() {
        let h = Harness::new();
        let r1_attempt = provision_first(&h, "run-r1", "Reviewer-R1").await;
        // R1 enqueued its create but it was never sent.
        let now = now_unix();
        let request = quorum_core::github_operations::NewGithubOperation {
            operation_id: "op-r1-create",
            client_request_id: "create",
            attempt_id: &r1_attempt,
            created_by_run_id: "run-r1",
            task_id: TASK,
            agent: "Reviewer-R1",
            role: "reviewer",
            pr_number: PR,
            head_sha: Some(SHA),
            reviewer_launch_sha: Some(SHA),
            lifecycle_generation: Some(0),
            kind: "pull_request_review_write",
            request_json: r#"{"method":"create"}"#,
            deadline_at: now + 1_800,
            review_sequence: Some(0),
            group_key: None,
            github_marker: None,
            created_at: now,
            expires_at: now + 3_600,
        };
        assert!(quorum_core::github_operations::enqueue(&mut h.conn(), &request).unwrap());
        let before = h.lifecycle_snapshot();
        let service = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::Apply, // cleanup: list (empty; create never sent)
            FakeOutcome::Apply, // fresh probe: list
        ]));
        let publication = h.publication(service.clone());
        publication.retire_for_handoff("run-r1").await;
        let op_state: String = h
            .conn()
            .query_row(
                "SELECT state FROM github_agent_operations WHERE operation_id='op-r1-create'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(op_state, "cancelled", "R1's unsent create can never send");
        let Barrier::Clear(claim) = publication.probe_before_provision(TASK, PR).await.unwrap()
        else {
            panic!("the R2 barrier clears after R1's slot is cleaned");
        };
        assert_eq!(mutations(&service), vec!["list", "list"]);
        h.live_reviewer("run-r2", "Reviewer-R2", SHA);
        assert!(publication.bind(claim, "run-r2").await.unwrap());
        assert_ne!(h.attempt_for_run("run-r2"), r1_attempt);
        assert_eq!(h.lifecycle_snapshot(), before);
        // Idempotent, and a run without a slot is a clean negative.
        let retired =
            review_publication_slots::retire_owner_for_handoff(&mut h.conn(), "run-r1", now_unix())
                .unwrap();
        assert!(!retired);
    }

    /// Drive a probe-phase GitHub outage until the shared bound blocks the
    /// slot.
    async fn exhaust_probe_outage(h: &Harness) {
        let service = Arc::new(FakeRepositoryService::scripted(
            (0..review_publication_slots::MAX_SLOT_CYCLES).map(|_| FakeOutcome::timeout()),
        ));
        let publication = h.publication(service.clone());
        for cycle in 1..=review_publication_slots::MAX_SLOT_CYCLES {
            h.conn()
                .execute(
                    "UPDATE github_review_publication_slots SET next_cleanup_at=0
                     WHERE state='probing'",
                    [],
                )
                .unwrap();
            let barrier = publication.probe_before_provision(TASK, PR).await.unwrap();
            if cycle < review_publication_slots::MAX_SLOT_CYCLES {
                assert!(matches!(barrier, Barrier::Wait(_)), "cycle {cycle}");
            } else {
                assert!(matches!(barrier, Barrier::Blocked(_)), "cycle {cycle}");
            }
        }
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Blocked);
        assert_eq!(
            slot.error_kind.as_deref(),
            Some(SlotBlockReason::RetryExhausted.as_str())
        );
        assert_eq!(
            service.calls().len(),
            review_publication_slots::MAX_SLOT_CYCLES as usize
        );
    }

    fn park_for_slot(h: &Harness, task_id: i64) {
        assert!(quorum_core::tasks::park(
            &mut h.conn(),
            task_id,
            "R1 reviewer not provisioned: publication slot blocked",
            "in-review",
            now_unix(),
        )
        .unwrap()
        .is_some());
    }

    #[tokio::test]
    async fn owner_retry_of_parked_task_resets_a_blocked_slot_once() {
        let h = Harness::new();
        exhaust_probe_outage(&h).await;
        park_for_slot(&h, TASK);
        // A daemon auto-retry never resets the slot: still blocked.
        assert!(
            quorum_core::tasks::retry_parked(&mut h.conn(), TASK, "daemon", false, now_unix())
                .unwrap()
                .is_some()
        );
        assert_eq!(h.slot().unwrap().state, SlotState::Blocked);
        let service = Arc::new(FakeRepositoryService::scripted([]));
        let publication = h.publication(service.clone());
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Blocked(_)
        ));
        assert!(service.calls().is_empty());

        // The owner's retry after repair: one fresh generation on the same
        // slot, no new slot, marker, attempt, or review.
        park_for_slot(&h, TASK);
        assert!(
            quorum_core::tasks::retry_parked(&mut h.conn(), TASK, "owner", true, now_unix())
                .unwrap()
                .is_some()
        );
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Probing);
        assert_eq!(slot.task_id, TASK);
        assert_eq!(slot.cleanup_attempts, 0);
        assert!(slot.error_kind.is_none());
        assert!(slot.attempt_id.is_none());
        let service = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]));
        let publication = h.publication(service.clone());
        let Barrier::Clear(claim) = publication.probe_before_provision(TASK, PR).await.unwrap()
        else {
            panic!("an empty listing clears the reset slot");
        };
        assert_eq!(claim.cycle, 1, "a fresh bounded generation");
        assert_eq!(mutations(&service), vec!["list"]);
        let slots: i64 = h
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM github_review_publication_slots",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(slots, 1);
        let reviews: usize = service.reviews().reviews.len();
        assert_eq!(reviews, 0, "the retry never creates a review");
    }

    #[tokio::test]
    async fn owner_retry_reset_rebinds_the_waiting_task_and_a_foreign_draft_reblocks() {
        let h = Harness::new();
        exhaust_probe_outage(&h).await;
        // A later task on the same PR is the one parked on the slot.
        const LATER: i64 = 32;
        h.conn()
            .execute(
                "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at,refs)
                 VALUES (?1,'review again','in-review','owner',1,1,?2)",
                rusqlite::params![LATER, serde_json::json!({ "pr": PR }).to_string()],
            )
            .unwrap();
        park_for_slot(&h, LATER);
        assert!(
            quorum_core::tasks::retry_parked(&mut h.conn(), LATER, "owner", true, now_unix())
                .unwrap()
                .is_some()
        );
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Probing);
        assert_eq!(slot.task_id, LATER, "waiting-task pointer rebound");
        // The earlier task no longer owns the slot's next probe.
        let publication = h.publication(Arc::new(FakeRepositoryService::scripted([])));
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Wait(_)
        ));
        // A human left a draft meanwhile: the reset slot re-blocks, no delete.
        let mut store = FakeReviewStore::default();
        let foreign = store.seed_pending(pr(), SHA, "a human's unfinished review");
        let service =
            Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]).with_reviews(store));
        let publication = h.publication(service.clone());
        assert!(matches!(
            publication.probe_before_provision(LATER, PR).await.unwrap(),
            Barrier::Blocked(_)
        ));
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Blocked);
        assert_eq!(
            slot.error_kind.as_deref(),
            Some(SlotBlockReason::ForeignDraft.as_str())
        );
        assert_eq!(mutations(&service), vec!["list"]);
        assert!(service
            .reviews()
            .reviews
            .iter()
            .any(|r| r.record.id == foreign));
    }

    #[tokio::test]
    async fn re_review_during_outage_cannot_bypass_cleanup() {
        let h = Harness::new();
        let attempt = provision_first(&h, "run-a", "Reviewer-A").await;
        let marker = h.marker(&attempt);
        h.crash_during_create(&attempt, "run-a");
        let mut store = FakeReviewStore::default();
        store.seed_pending(pr(), SHA, &marker);
        h.restart(&["run-a"]);

        // GitHub is down: the cleanup read fails and is rescheduled.
        let service =
            Arc::new(FakeRepositoryService::scripted([FakeOutcome::timeout()]).with_reviews(store));
        let publication = h.publication(service.clone());
        let shutdown = AtomicBool::new(false);
        assert!(publication.cleanup_once(&shutdown).await.unwrap());
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::CleanupRequired);
        assert_eq!(slot.error_kind.as_deref(), Some("cleanup_inconclusive"));
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Wait(_)
        ));
        assert_eq!(service.reviews().reviews.len(), 1, "nothing deleted");
        assert_eq!(mutations(&service), vec!["list"]);
    }

    #[tokio::test]
    async fn probe_outage_reschedules_and_never_clears() {
        let h = Harness::new();
        let service = Arc::new(FakeRepositoryService::scripted([FakeOutcome::timeout()]));
        let publication = h.publication(service.clone());
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Wait(_)
        ));
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Probing);
        assert_eq!(slot.error_kind.as_deref(), Some("probe_inconclusive"));
        // Not yet due: the next provision attempt waits without a read.
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Wait(_)
        ));
        assert_eq!(service.calls().len(), 1);
    }

    #[tokio::test]
    async fn foreign_draft_blocks_provisioning_without_deletion() {
        let h = Harness::new();
        let before = h.lifecycle_snapshot();
        let mut store = FakeReviewStore::default();
        let foreign = store.seed_pending(pr(), SHA, "a human's unfinished review");
        let service =
            Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]).with_reviews(store));
        let publication = h.publication(service.clone());
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Blocked(_)
        ));
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Blocked);
        assert_eq!(
            slot.error_kind.as_deref(),
            Some(SlotBlockReason::ForeignDraft.as_str())
        );
        // Every later provision stays blocked without another read.
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Blocked(_)
        ));
        let shutdown = AtomicBool::new(false);
        assert!(!publication.cleanup_once(&shutdown).await.unwrap());
        assert_eq!(mutations(&service), vec!["list"]);
        assert!(service
            .reviews()
            .reviews
            .iter()
            .any(|r| r.record.id == foreign));
        // Visible in status.
        let stats = quorum_core::stats::stats(&h.conn(), now_unix(), 60).unwrap();
        assert_eq!(stats.review_publication_slots.len(), 1);
        assert_eq!(stats.review_publication_slots[0].state, SlotState::Blocked);
        assert_eq!(h.lifecycle_snapshot(), before);
    }

    #[tokio::test]
    async fn cleanup_blocks_on_a_foreign_or_mismatched_draft_without_deleting() {
        for case in ["foreign", "mismatched_sha", "two_markers"] {
            let h = Harness::new();
            let attempt = provision_first(&h, "run-a", "Reviewer-A").await;
            let marker = h.marker(&attempt);
            h.crash_during_create(&attempt, "run-a");
            let mut store = FakeReviewStore::default();
            match case {
                "foreign" => store.seed_pending(pr(), SHA, "unmarked draft"),
                "mismatched_sha" => store.seed_pending(pr(), OTHER_SHA, &marker),
                _ => store.seed_pending(
                    pr(),
                    SHA,
                    &format!("{marker}\n<!-- quorum-review-owner:{} -->", "f".repeat(64)),
                ),
            };
            h.restart(&["run-a"]);
            let service =
                Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]).with_reviews(store));
            let publication = h.publication(service.clone());
            let shutdown = AtomicBool::new(false);
            assert!(publication.cleanup_once(&shutdown).await.unwrap(), "{case}");
            let slot = h.slot().unwrap();
            assert_eq!(slot.state, SlotState::Blocked, "{case}");
            assert_eq!(mutations(&service), vec!["list"], "{case}: never deleted");
            assert_eq!(service.reviews().reviews.len(), 1, "{case}");
            assert!(matches!(
                publication.probe_before_provision(TASK, PR).await.unwrap(),
                Barrier::Blocked(_)
            ));
        }
    }

    #[tokio::test]
    async fn bounded_cleanup_exhaustion_blocks_without_lifecycle_effects() {
        let h = Harness::new();
        let attempt = provision_first(&h, "run-a", "Reviewer-A").await;
        h.crash_during_create(&attempt, "run-a");
        h.restart(&["run-a"]);
        let before = h.lifecycle_snapshot();
        // The create is ambiguous and GitHub shows no draft: an unfenced
        // empty read is only a negative observation, never release.
        let service = Arc::new(FakeRepositoryService::scripted(
            (0..review_publication_slots::MAX_SLOT_CYCLES).map(|_| FakeOutcome::Apply),
        ));
        let publication = h.publication(service.clone());
        let shutdown = AtomicBool::new(false);
        for cycle in 1..=review_publication_slots::MAX_SLOT_CYCLES {
            // Make the backoff due without waiting.
            h.conn()
                .execute(
                    "UPDATE github_review_publication_slots SET next_cleanup_at=0
                     WHERE state='cleanup_required'",
                    [],
                )
                .unwrap();
            assert!(
                publication.cleanup_once(&shutdown).await.unwrap(),
                "cycle {cycle}"
            );
        }
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::Blocked);
        assert_eq!(
            slot.error_kind.as_deref(),
            Some(SlotBlockReason::RetryExhausted.as_str())
        );
        assert!(!publication.cleanup_once(&shutdown).await.unwrap());
        assert_eq!(
            service.calls().len(),
            review_publication_slots::MAX_SLOT_CYCLES as usize,
            "one bounded read per cycle"
        );
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Blocked(_)
        ));
        assert_eq!(h.lifecycle_snapshot(), before);
    }

    #[tokio::test]
    async fn confirmed_create_is_released_only_by_exact_id_proof() {
        let h = Harness::new();
        let attempt = provision_first(&h, "run-a", "Reviewer-A").await;
        h.crash_during_create(&attempt, "run-a");
        h.restart(&["run-a"]);
        // Marker evidence recorded the remote ID on both rows (as the core's
        // reconciliation does): the create is `confirmed`.
        h.conn()
            .execute_batch(
                "UPDATE github_agent_operations SET send_state='confirmed',
                     remote_object_id='9001' WHERE review_sequence=0;
                 UPDATE github_review_publication_slots SET create_send_state='confirmed',
                     pending_review_id='9001';",
            )
            .unwrap();
        assert_eq!(h.slot().unwrap().state, SlotState::CleanupRequired);
        // Empty listing, then the exact ID is gone.
        let service = Arc::new(FakeRepositoryService::scripted([
            FakeOutcome::Apply,
            FakeOutcome::Apply,
        ]));
        let publication = h.publication(service.clone());
        let shutdown = AtomicBool::new(false);
        assert!(publication.cleanup_once(&shutdown).await.unwrap());
        assert!(h.slot().is_none());
        assert_eq!(mutations(&service), vec!["list", "get"]);
        assert!(matches!(
            service.calls()[1].request,
            RepositoryRequest::GetReview { review_id: 9001 }
        ));
    }

    #[tokio::test]
    async fn shutdown_and_restart_reconcile_cleanup_running_before_any_delete() {
        let h = Harness::new();
        let attempt = provision_first(&h, "run-a", "Reviewer-A").await;
        let marker = h.marker(&attempt);
        h.crash_during_create(&attempt, "run-a");
        let mut store = FakeReviewStore::default();
        let orphan = store.seed_pending(pr(), SHA, &marker);
        h.restart(&["run-a"]);

        // A previous daemon claimed cleanup and died mid-cycle.
        let claim = review_publication_slots::claim_cleanup(&mut h.conn(), now_unix())
            .unwrap()
            .unwrap();
        assert_eq!(h.slot().unwrap().state, SlotState::CleanupRunning);
        let service = Arc::new(
            FakeRepositoryService::scripted([FakeOutcome::Apply, FakeOutcome::Apply])
                .with_reviews(store),
        );
        let publication = h.publication(service.clone());
        // Shutdown requested: no new claim and no remote call.
        let shutdown = AtomicBool::new(true);
        assert!(!publication.cleanup_once(&shutdown).await.unwrap());
        assert_eq!(h.slot().unwrap().state, SlotState::CleanupRunning);
        assert!(service.calls().is_empty());

        // Next startup: cleanup_running becomes reconciliation-required, the
        // stale claim is fenced, and the remote state is re-read first.
        review_publication_slots::reconcile_on_start(&mut h.conn(), now_unix()).unwrap();
        let slot = h.slot().unwrap();
        assert_eq!(slot.state, SlotState::CleanupRequired);
        assert_eq!(slot.cleanup_attempts, claim.cycle, "cycle count retained");
        let stale = review_publication_slots::complete_cleanup(
            &mut h.conn(),
            &claim,
            CleanupObservation::Inconclusive,
            now_unix(),
        )
        .unwrap();
        assert_eq!(stale, CleanupOutcome::Lost);
        let shutdown = AtomicBool::new(false);
        assert!(publication.cleanup_once(&shutdown).await.unwrap());
        assert!(h.slot().is_none());
        assert_eq!(mutations(&service), vec!["list", "delete"]);
        assert!(service
            .reviews()
            .reviews
            .iter()
            .all(|r| r.record.id != orphan));
    }

    #[tokio::test]
    async fn barrier_waits_until_startup_reconciliation() {
        let h = Harness::new();
        let service = Arc::new(FakeRepositoryService::scripted([]));
        let target = RepositoryTarget::new(h.repo_dir.clone(), Some("acme/widgets")).unwrap();
        let publication = ReviewPublication::new(h.db.clone(), target, service.clone()).unwrap();
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Wait(_)
        ));
        let shutdown = Arc::new(AtomicBool::new(false));
        let driver = tokio::spawn(
            Arc::new(publication).run_cleanup(shutdown.clone(), Duration::from_millis(10)),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .unwrap()
            .unwrap();
        assert!(h.slot().is_none());
        assert!(service.calls().is_empty());
    }

    #[tokio::test]
    async fn abandoned_probe_is_released_for_a_fresh_probe() {
        let h = Harness::new();
        let service = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]));
        let publication = h.publication(service.clone());
        let Barrier::Clear(claim) = publication.probe_before_provision(TASK, PR).await.unwrap()
        else {
            panic!("empty listing clears");
        };
        publication.abandon(claim).await;
        assert!(h.slot().is_none(), "an unbound probe leaves no slot");
        // The next provision repeats a fresh probe immediately.
        let service = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]));
        let publication = h.publication(service.clone());
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Clear(_)
        ));
        assert_eq!(mutations(&service), vec!["list"]);
    }

    #[tokio::test]
    async fn bind_without_a_live_reviewer_run_writes_nothing_and_gives_the_claim_back() {
        let h = Harness::new();
        let service = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]));
        let publication = h.publication(service.clone());
        let Barrier::Clear(claim) = publication.probe_before_provision(TASK, PR).await.unwrap()
        else {
            panic!("empty listing clears");
        };
        // The capability exists but was revoked before the bind.
        h.live_reviewer("run-dead", "Reviewer-A", SHA);
        h.retire_reviewer("run-dead");
        assert!(!publication.bind(claim, "run-dead").await.unwrap());
        let attempts: i64 = h
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM github_collaboration_attempts",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempts, 0, "the attempt insert rolled back");
        assert!(h.slot().is_none(), "the unbound probe was given back");
    }

    #[tokio::test]
    async fn barrier_waits_boundedly_for_an_inflight_driver_cycle() {
        let h = Harness::new();
        let attempt = provision_first(&h, "run-a", "Reviewer-A").await;
        assert!(!attempt.is_empty());
        h.retire_reviewer("run-a");
        // The background driver holds the slot's cycle.
        let claim = review_publication_slots::claim_cleanup(&mut h.conn(), now_unix())
            .unwrap()
            .unwrap();
        let db = h.db.clone();
        let finisher = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let mut conn = quorum_core::db::open(&db).unwrap();
            review_publication_slots::complete_cleanup(
                &mut conn,
                &claim,
                CleanupObservation::Listing {
                    candidates: &[],
                    fenced: false,
                },
                now_unix(),
            )
            .unwrap()
        });
        let service = Arc::new(FakeRepositoryService::scripted([FakeOutcome::Apply]));
        let publication = h.publication(service.clone());
        assert!(matches!(
            publication.probe_before_provision(TASK, PR).await.unwrap(),
            Barrier::Clear(_)
        ));
        assert_eq!(finisher.await.unwrap(), CleanupOutcome::Released);
        assert_eq!(
            mutations(&service),
            vec!["list"],
            "only the fresh probe read"
        );
    }

    #[test]
    fn owner_marker_extraction_is_exact_and_fails_closed() {
        let marker = format!("<!-- quorum-review-owner:{} -->", "a".repeat(64));
        assert_eq!(owner_marker_in(&marker), Some(marker.as_str()));
        let body = format!("## Summary\n\nok\n\n{marker}");
        assert_eq!(owner_marker_in(&body), Some(marker.as_str()));
        assert_eq!(
            owner_marker_in(&format!("{marker}{marker}")),
            Some(marker.as_str())
        );
        let other = format!("<!-- quorum-review-owner:{} -->", "b".repeat(64));
        assert_eq!(owner_marker_in(&format!("{marker}{other}")), None);
        assert_eq!(owner_marker_in("no marker"), None);
        assert_eq!(owner_marker_in("<!-- quorum-review-owner:abc"), None);
        let huge = format!("<!-- quorum-review-owner:{} -->", "c".repeat(600));
        assert_eq!(owner_marker_in(&huge), None);
    }
}
