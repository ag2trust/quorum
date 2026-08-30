//! #228 approval recovery: merge already-approved PRs from durable state on restart.
//!
//! A `--self-update-drain` restart relaunches `serve` as a fresh instance with
//! an empty roster. The prior instance's reviewer is gone, so its
//! instance-scoped verdict mailbox row is left unconsumed and the approved task
//! gets re-worked. This pass makes restart recovery a pure function of durable,
//! instance-independent state:
//!
//! 1. **Adopt** any unconsumed, attested `approved` verdict mailbox rows into
//!    the durable [`approvals`] table (the stranded live signal), resolving the
//!    task/author from the journal.
//! 2. **Replay** every durable approval through
//!    [`crate::verdict::dispose_approval`] — trust by attestation + SHA-binding,
//!    NOT by roster:
//!    - `Merge`  → merge the PR, close the task, drop the journal + approval.
//!    - `Demote` → head moved; drop the approval and let normal recovery re-review.
//!    - `Reject` → self-review / not attested; drop the approval (crash safety).
//!
//! Runs BEFORE [`super::recovery::recover`] so an approved task is
//! merged+closed (and its journal rows removed) before recovery resets it.

use super::log;
use super::merge::{self, MergeabilityState};
use quorum_core::error::{QuorumError, Result};
use quorum_core::{approvals, journal, mailbox, tasks};
use std::path::Path;
use std::sync::Arc;

/// Outcome counts for logging/assertions.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RecoveryOutcome {
    pub adopted: usize,
    pub merged: usize,
    pub demoted: usize,
    pub rejected: usize,
    /// Approvals left in place for a later startup (e.g. head unknown / merge failed).
    pub deferred: usize,
}

pub(crate) async fn recover(
    db_path: &Path,
    repo_dir: &Path,
    merge_executor: &Arc<dyn merge::MergeExecutor>,
    configured_base_branch: &str,
    checks_timeout_secs: u64,
    checks_poll_secs: u64,
) -> Result<RecoveryOutcome> {
    // ── Phase 1: adopt stranded attested-approved mailbox rows ─────────────
    let mut outcome = RecoveryOutcome {
        adopted: adopt_stranded_verdicts(db_path, repo_dir, merge_executor).await?,
        ..Default::default()
    };

    // ── Phase 2: replay durable approvals from persisted state ─────────────
    let records = {
        let p = db_path.to_path_buf();
        run_blocking(move || {
            let conn = quorum_core::db::open(&p)?;
            approvals::list(&conn)
        })
        .await?
    };

    // Deduplicate by PR: process each PR once (may have R1 + R2 records).
    let mut seen_prs = std::collections::HashSet::new();
    for appr in &records {
        if !seen_prs.insert(appr.pr_number) {
            continue;
        }
        let pr = appr.pr_number;
        let pr_records: Vec<&approvals::Approval> = records
            .iter()
            .filter(|record| record.pr_number == pr)
            .collect();
        // A policy/infrastructure park is owner-gated. Retained approvals are
        // evidence, not an automatic retry loop. Likewise an explicit merge
        // retry is reconciled by the normal daemon tick, which atomically
        // consumes its one-shot marker and performs the stronger live target
        // checks; startup recovery must not race or bypass that authority. A
        // PR-wide approval set must also name one exact task and author. Mixed
        // rows are stale evidence, never authority to close whichever task was
        // encountered first.
        let replay_deferred = {
            let p = db_path.to_path_buf();
            let task_id = appr.task_id;
            let author = appr.author.clone();
            let mixed_authority = author.is_empty()
                || pr_records
                    .iter()
                    .any(|record| record.task_id != task_id || record.author != author);
            run_blocking(move || {
                if mixed_authority {
                    return Ok(true);
                }
                let conn = quorum_core::db::open(&p)?;
                let Some(task) = tasks::get(&conn, task_id)? else {
                    return Ok(true);
                };
                if task.author.as_deref() != Some(author.as_str()) {
                    return Ok(true);
                }
                let merge_retry = task
                    .refs
                    .as_deref()
                    .and_then(|refs| serde_json::from_str::<serde_json::Value>(refs).ok())
                    .and_then(|refs| {
                        refs.get(tasks::MERGE_RETRY_REF)
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    });
                Ok(task.status == "failed" || merge_retry.is_some())
            })
            .await?
        };
        if replay_deferred {
            outcome.deferred += 1;
            continue;
        }
        // SHA-bind against the PR's live head. If we can't determine it, leave
        // the record for a future startup rather than merging blind.
        let repo = repo_dir.to_path_buf();
        let exec = Arc::clone(merge_executor);
        let current_head = tokio::task::spawn_blocking(move || exec.head_sha(pr, &repo))
            .await
            .map_err(|e| QuorumError::Io(format!("head_sha spawn_blocking join: {e}")))?;
        let Some(current_head) = current_head else {
            log(&format!(
                "approval-recovery: PR #{pr} head unknown — deferring approval \
                 (task #{}) to a later startup",
                appr.task_id
            ));
            outcome.deferred += 1;
            continue;
        };

        // Validate each role's approval individually (stale SHA, self-review).
        let mut any_invalid = false;
        let mut any_rejected = false;
        for role_appr in &pr_records {
            match crate::verdict::dispose_approval(
                &role_appr.verdict,
                role_appr.blocking_count,
                &role_appr.reviewer,
                &role_appr.author,
                &role_appr.approved_head_sha,
                &current_head,
            ) {
                crate::verdict::ApprovalDisposition::Merge => {}
                crate::verdict::ApprovalDisposition::Demote(reason) => {
                    log(&format!(
                        "approval-recovery: PR #{pr} {} demoted (task #{}) — {reason}",
                        role_appr.review_role, appr.task_id
                    ));
                    any_invalid = true;
                }
                crate::verdict::ApprovalDisposition::Reject(reason) => {
                    log(&format!(
                        "approval-recovery: PR #{pr} {} rejected (task #{}) — {reason}",
                        role_appr.review_role, appr.task_id
                    ));
                    any_invalid = true;
                    any_rejected = true;
                }
            }
        }

        if any_invalid {
            drop_approval(db_path, pr).await?;
            if any_rejected {
                outcome.rejected += 1;
            } else {
                outcome.demoted += 1;
            }
            continue;
        }

        // The sampled-R2 decision is independent daemon authority. Missing or
        // differently-task-bound state cannot be substituted by having both
        // role rows. Invalidate R1 so normal review recreates the decision;
        // retain an exact R2 row in case the new decision still requires it.
        let sampling_authoritative = {
            let p = db_path.to_path_buf();
            let task_id = appr.task_id;
            let sha = current_head.clone();
            run_blocking(move || {
                let mut conn = quorum_core::db::open(&p)?;
                match quorum_core::review_audits::r2_requirement(&conn, task_id, pr, &sha) {
                    Ok(Some(_)) => Ok(true),
                    Ok(None) | Err(_) => {
                        tasks::invalidate_recovered_sampling_authority(
                            &mut conn,
                            task_id,
                            pr,
                            &sha,
                            super::now_unix(),
                        )?;
                        Ok(false)
                    }
                }
            })
            .await?
        };
        if !sampling_authoritative {
            log(&format!(
                "approval-recovery: PR #{pr} (task #{}) has no exact sampled-R2 decision — invalidated R1 and deferred to fresh review",
                appr.task_id
            ));
            outcome.deferred += 1;
            continue;
        }

        // #191: use the shared next_needed_role predicate (#190) instead of
        // hard-coded dual_approved(). Any role not approved for the current SHA
        // defers to normal review (generic recovery resets to in-review and the
        // tick loop provisions the first missing role).
        let next_role = {
            let p = db_path.to_path_buf();
            let sha = current_head.clone();
            run_blocking(move || {
                let conn = quorum_core::db::open(&p)?;
                super::next_needed_role(&conn, pr, &sha)
            })
            .await?
        };
        if let Some(role) = next_role {
            log(&format!(
                "approval-recovery: PR #{pr} (task #{}) — {role} not approved \
                 for current SHA, deferring to normal review",
                appr.task_id
            ));
            outcome.deferred += 1;
            continue;
        }

        if merge_approved(
            db_path,
            repo_dir,
            merge_executor,
            appr,
            configured_base_branch,
            checks_timeout_secs,
            checks_poll_secs,
        )
        .await?
        {
            outcome.merged += 1;
        } else {
            outcome.deferred += 1;
        }
    }

    if outcome != RecoveryOutcome::default() {
        log(&format!(
            "approval-recovery: adopted={} merged={} demoted={} rejected={} deferred={}",
            outcome.adopted, outcome.merged, outcome.demoted, outcome.rejected, outcome.deferred
        ));
    }
    Ok(outcome)
}

/// Fold unconsumed, attested `approved` verdict mailbox rows into durable
/// approvals. The stranded signal (reviewer gone with the old instance) becomes
/// instance-independent state Phase 2 can act on. The row is only consumed once
/// its approval is durably recorded, so nothing is lost on a mid-pass crash.
async fn adopt_stranded_verdicts(
    db_path: &Path,
    repo_dir: &Path,
    merge_executor: &Arc<dyn merge::MergeExecutor>,
) -> Result<usize> {
    // Pull candidate rows + the journal (for task/author resolution) in one hop.
    let p = db_path.to_path_buf();
    let (candidates, journal_rows) = run_blocking(move || {
        let conn = quorum_core::db::open(&p)?;
        let rows = mailbox::poll_unconsumed(&conn)?;
        let journal_rows = journal::list_in_flight(&conn)?;
        Ok((rows, journal_rows))
    })
    .await?;

    let mut adopted = 0usize;
    for (id, row) in candidates {
        if row.kind != mailbox::MailboxKind::Done {
            continue;
        }
        let Some(pr) = row.pr else { continue };
        // Only an attested zero-blocking approval is adoptable; the #226 gate
        // rejects forged/unattested rows (they demote to changes → skipped).
        let gated = crate::verdict::gate(row.verdict.as_deref(), row.payload.as_deref());
        if gated.verdict.as_deref() != Some("approved") {
            continue;
        }
        // Resolve the authoring worker + task from the journal row that carries
        // this PR (the #178 awaiting-review position). Without it we can't close
        // the task on merge, so leave the row for the normal path.
        let Some(row_task_id) = row.task_id else {
            continue;
        };
        let Some(worker) = journal_rows
            .iter()
            .find(|e| e.role == "worker" && e.task_id == Some(row_task_id) && e.pr == Some(pr))
        else {
            continue;
        };
        let Some(task_id) = worker.task_id else {
            continue;
        };
        let author = worker.agent.clone();
        // Self-review guard at adoption time too (belt and suspenders — Phase 2
        // re-checks): never fold a reviewer==author signal into an approval.
        if row.agent == author {
            log(&format!(
                "approval-recovery: refusing self-review verdict on PR #{pr} \
                 from {} (== author)",
                row.agent
            ));
            continue;
        }
        // A mailbox verdict carries no SHA authority. Recover it only from the
        // one immutable daemon-written launch row for this exact
        // reviewer/task/PR; an absent or ambiguous launch is not adoptable.
        let launch = {
            let p = db_path.to_path_buf();
            let reviewer = row.agent.clone();
            run_blocking(move || {
                let conn = quorum_core::db::open(&p)?;
                quorum_core::agent_runs::review_launch_for_reviewer(&conn, task_id, &reviewer, pr)
            })
            .await?
        };
        let Some(launch) = launch else {
            continue;
        };

        // Live identity is checked outside the database transaction. A moved
        // head leaves the verdict unconsumed for a fresh review; it must never
        // be stamped onto the new head.
        let repo = repo_dir.to_path_buf();
        let exec = Arc::clone(merge_executor);
        let current_head = tokio::task::spawn_blocking(move || exec.head_sha(pr, &repo))
            .await
            .map_err(|e| QuorumError::Io(format!("head_sha spawn_blocking join: {e}")))?;
        let Some(current_head) = current_head else {
            continue; // head unknown — leave the row for a later startup
        };
        if current_head != launch.head_sha {
            continue;
        }
        let record = approvals::Approval {
            pr_number: pr,
            review_role: launch.review_role,
            task_id,
            author,
            reviewer: row.agent.clone(),
            verdict: "approved".to_string(),
            blocking_count: 0,
            approved_head_sha: launch.head_sha,
        };
        let p = db_path.to_path_buf();
        run_blocking(move || {
            let mut conn = quorum_core::db::open(&p)?;
            approvals::record(&mut conn, &record)?;
            mailbox::mark_consumed(&mut conn, id)?;
            Ok(())
        })
        .await?;
        log(&format!(
            "approval-recovery: adopted stranded approval for PR #{pr} \
             (reviewer {}, task #{task_id})",
            row.agent
        ));
        adopted += 1;
    }
    Ok(adopted)
}

/// Merge a durably-approved PR and close its task. Returns `true` on a
/// successful merge (approval + journal cleaned up), `false` if the merge did
/// not happen (conflicting / merge failure) — the caller counts it as deferred.
async fn merge_approved(
    db_path: &Path,
    repo_dir: &Path,
    merge_executor: &Arc<dyn merge::MergeExecutor>,
    appr: &approvals::Approval,
    configured_base_branch: &str,
    checks_timeout_secs: u64,
    checks_poll_secs: u64,
) -> Result<bool> {
    let pr = appr.pr_number;
    let effective_base_branch =
        super::effective_task_base_branch(db_path, appr.task_id, configured_base_branch).await?;

    // Pre-merge conflict check — a superseded base means rework, not merge.
    let repo = repo_dir.to_path_buf();
    let exec = Arc::clone(merge_executor);
    let mergeability = tokio::task::spawn_blocking(move || exec.check_mergeability(pr, &repo))
        .await
        .map_err(|e| QuorumError::Io(format!("mergeability spawn_blocking join: {e}")))?;
    if mergeability == MergeabilityState::Conflicting {
        log(&format!(
            "approval-recovery: PR #{pr} is CONFLICTING — dropping approval so \
             the task returns to rework (task #{})",
            appr.task_id
        ));
        drop_approval(db_path, pr).await?;
        return Ok(false);
    }

    // #174: verify CI checks before merging — a durable approval may
    // survive a crash during merge-wait when checks were still pending.
    {
        let repo = repo_dir.to_path_buf();
        let exec = Arc::clone(merge_executor);
        let timeout = checks_timeout_secs;
        let poll = checks_poll_secs;
        let checks =
            tokio::task::spawn_blocking(move || exec.wait_for_checks(pr, &repo, timeout, poll))
                .await
                .map_err(|e| {
                    QuorumError::Io(format!("wait_for_checks spawn_blocking join: {e}"))
                })?;
        match checks {
            merge::ChecksOutcome::Ready => {}
            merge::ChecksOutcome::Failed { failing_checks } => {
                let names = failing_checks.join(", ");
                log(&format!(
                    "approval-recovery: PR #{pr} CI checks failed ({names}) — \
                     dropping approval (task #{})",
                    appr.task_id
                ));
                drop_approval(db_path, pr).await?;
                return Ok(false);
            }
            merge::ChecksOutcome::TimedOut | merge::ChecksOutcome::Pending { .. } => {
                log(&format!(
                    "approval-recovery: PR #{pr} CI checks not ready — \
                     deferring to tick loop (task #{})",
                    appr.task_id
                ));
                return Ok(false);
            }
        }
    }

    let repo = repo_dir.to_path_buf();
    let exec = Arc::clone(merge_executor);
    let ctx = merge::MergeContext {
        reviewer_name: appr.reviewer.clone(),
        review_task_id: appr.task_id,
        expected_base_branch: effective_base_branch,
        expected_head_sha: appr.approved_head_sha.clone(),
    };

    // A normal live merge records this boundary before its first remote call.
    // Do the same for a recovered task that already reached `merging`: a crash
    // during this startup call must park `attempting` on the next startup, not
    // silently replay it. Older recovered shapes in another lifecycle state
    // retain their legacy disposition until they are migrated by normal flow.
    let (attempt_admitted, review_only) = {
        let p = db_path.to_path_buf();
        let task_id = appr.task_id;
        run_blocking(move || {
            let mut conn = quorum_core::db::open(&p)?;
            let Some(task) = tasks::get(&conn, task_id)? else {
                return Ok((false, false));
            };
            if task.status != "merging" {
                return Ok((false, task.review_only));
            }
            Ok((
                tasks::begin_approved_merge_attempt(&mut conn, task_id, super::now_unix())?,
                task.review_only,
            ))
        })
        .await?
    };
    if !attempt_admitted {
        let p = db_path.to_path_buf();
        let task_id = appr.task_id;
        let still_merging = run_blocking(move || {
            let conn = quorum_core::db::open(&p)?;
            Ok(tasks::get(&conn, task_id)?.is_some_and(|task| task.status == "merging"))
        })
        .await?;
        if still_merging {
            log(&format!(
                "approval-recovery: task #{} lost startup merge admission — deferring fail closed",
                appr.task_id
            ));
            return Ok(false);
        }
    }
    let result = tokio::task::spawn_blocking(move || exec.merge(pr, &repo, &ctx))
        .await
        .map_err(|e| QuorumError::Io(format!("merge spawn_blocking join: {e}")))?;

    if !result.success {
        match result
            .failure_kind
            .unwrap_or(merge::MergeFailureKind::PolicyBlocked)
        {
            merge::MergeFailureKind::StaleAuthority => {
                log(&format!(
                    "approval-recovery: PR #{pr} head moved during final merge boundary \
                     (task #{}) — dropping stale approval",
                    appr.task_id
                ));
                if attempt_admitted {
                    let p = db_path.to_path_buf();
                    let task_id = appr.task_id;
                    let approved_head = appr.approved_head_sha.clone();
                    run_blocking(move || {
                        let mut conn = quorum_core::db::open(&p)?;
                        tasks::invalidate_merge_retry(
                            &mut conn,
                            task_id,
                            pr,
                            tasks::StaleMergeRetryEvidence {
                                roles: &["r1", "r2"],
                                sampling_head: Some(&approved_head),
                            },
                            "PR head moved during startup approval replay",
                            super::now_unix(),
                        )?;
                        Ok(())
                    })
                    .await?;
                } else {
                    drop_approval(db_path, pr).await?;
                }
            }
            merge::MergeFailureKind::PolicyBlocked | merge::MergeFailureKind::PolicyPending => {
                super::require_park_task(
                    db_path,
                    appr.task_id,
                    &format!(
                        "startup approval replay policy blocked PR #{pr}: {}",
                        result.message
                    ),
                    "merging",
                )
                .await?;
                log(&format!(
                    "approval-recovery: PR #{pr} policy blocked (task #{}) — parked with exact-head approvals retained",
                    appr.task_id
                ));
            }
            merge::MergeFailureKind::Retryable => {
                if attempt_admitted {
                    let p = db_path.to_path_buf();
                    let task_id = appr.task_id;
                    let feedback = format!(
                        "Merge of PR #{pr} failed during startup approval replay: {}",
                        result.message
                    );
                    if review_only {
                        let approved_head = appr.approved_head_sha.clone();
                        run_blocking(move || {
                            let mut conn = quorum_core::db::open(&p)?;
                            tasks::invalidate_merge_retry(
                                &mut conn,
                                task_id,
                                pr,
                                tasks::StaleMergeRetryEvidence {
                                    roles: &["r1", "r2"],
                                    sampling_head: Some(&approved_head),
                                },
                                &feedback,
                                super::now_unix(),
                            )?;
                            Ok(())
                        })
                        .await?;
                        super::set_task_body(db_path, task_id, tasks::MERGE_BLOCKED_BODY).await;
                    } else {
                        run_blocking(move || {
                            let mut conn = quorum_core::db::open(&p)?;
                            tasks::rework_approved_merge(
                                &mut conn,
                                task_id,
                                pr,
                                &feedback,
                                super::now_unix(),
                            )?;
                            Ok(())
                        })
                        .await?;
                    }
                    log(&format!(
                        "approval-recovery: PR #{pr} has a worker-fixable merge failure \
                         (task #{}) — invalidated approvals for rework",
                        appr.task_id
                    ));
                } else {
                    log(&format!(
                        "approval-recovery: legacy PR #{pr} merge failure (task #{}) \
                         — dropping approvals for ordinary recovery: {}",
                        appr.task_id, result.message
                    ));
                    drop_approval(db_path, pr).await?;
                }
            }
        }
        return Ok(false);
    }

    let repo = repo_dir.to_path_buf();
    let exec = Arc::clone(merge_executor);
    let merge_commit_sha = tokio::task::spawn_blocking(move || exec.merge_commit_sha(pr, &repo))
        .await
        .map_err(|error| {
            QuorumError::Io(format!("merge commit SHA spawn_blocking join: {error}"))
        })?;
    let Some(merge_commit_sha) = merge_commit_sha else {
        let resume_status = if attempt_admitted {
            "merging"
        } else {
            "in-review"
        };
        let reason = format!(
            "PR #{pr} merged but GitHub merge commit metadata is unavailable; retry after metadata propagation"
        );
        super::require_park_task(db_path, appr.task_id, &reason, resume_status).await?;
        log(&format!("PARKED: task #{}: {reason}", appr.task_id));
        return Ok(false);
    };

    if attempt_admitted {
        let p = db_path.to_path_buf();
        let task_id = appr.task_id;
        run_blocking(move || {
            let mut conn = quorum_core::db::open(&p)?;
            tasks::complete_approved_merge(
                &mut conn,
                task_id,
                pr,
                &merge_commit_sha,
                super::now_unix(),
            )?;
            let agents: Vec<String> = journal::list_in_flight(&conn)?
                .into_iter()
                .filter(|entry| entry.task_id == Some(task_id))
                .map(|entry| entry.agent)
                .collect();
            for agent in agents {
                journal::delete(&mut conn, &agent)?;
            }
            Ok(())
        })
        .await?;
        log(&format!(
            "approval-recovery: PR #{pr} merged from admitted durable approval — closed task #{}",
            appr.task_id
        ));
        return Ok(true);
    }

    let p = db_path.to_path_buf();
    let task_id = appr.task_id;
    let reviewer = appr.reviewer.clone();
    run_blocking(move || {
        let mut conn = quorum_core::db::open(&p)?;
        let now = super::now_unix();
        let note =
            format!("daemon: PR #{pr} merged on restart recovery (approved by {reviewer}, #228)");
        tasks::close_after_merge_with_merge_commit_sha(
            &mut conn,
            task_id,
            &note,
            &merge_commit_sha,
            now,
        )?;
        // Remove journal rows for this task so recovery won't reset now-merged work.
        let agents: Vec<String> = journal::list_in_flight(&conn)?
            .into_iter()
            .filter(|e| e.task_id == Some(task_id))
            .map(|e| e.agent)
            .collect();
        for a in agents {
            journal::delete(&mut conn, &a)?;
        }
        approvals::delete(&mut conn, pr)?;
        Ok(())
    })
    .await?;

    log(&format!(
        "approval-recovery: PR #{pr} merged from durable approval — closed task #{} \
         (reviewer {})",
        appr.task_id, appr.reviewer
    ));

    Ok(true)
}

async fn drop_approval(db_path: &Path, pr: i64) -> Result<()> {
    let p = db_path.to_path_buf();
    run_blocking(move || {
        let mut conn = quorum_core::db::open(&p)?;
        approvals::delete(&mut conn, pr)?;
        Ok(())
    })
    .await
}

async fn run_blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::db;
    use rusqlite::OptionalExtension;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Mock merge executor: records merge/mergeability calls and returns a
    /// configurable head SHA per PR.
    struct MockExec {
        heads: HashMap<i64, String>,
        merged: Mutex<Vec<(i64, String)>>,
        conflicting: bool,
        merge_succeeds: bool,
        failure_kind: merge::MergeFailureKind,
        merge_commit_available: bool,
    }

    impl MockExec {
        fn new(heads: HashMap<i64, String>) -> Self {
            Self {
                heads,
                merged: Mutex::new(Vec::new()),
                conflicting: false,
                merge_succeeds: true,
                failure_kind: merge::MergeFailureKind::Retryable,
                merge_commit_available: true,
            }
        }

        fn failing(mut self, failure_kind: merge::MergeFailureKind) -> Self {
            self.merge_succeeds = false;
            self.failure_kind = failure_kind;
            self
        }

        fn without_merge_commit_sha(mut self) -> Self {
            self.merge_commit_available = false;
            self
        }
    }

    impl merge::MergeExecutor for MockExec {
        fn merge(&self, pr: i64, _repo: &Path, ctx: &merge::MergeContext) -> merge::MergeResult {
            self.merged
                .lock()
                .unwrap()
                .push((pr, ctx.expected_base_branch.clone()));
            merge::MergeResult {
                success: self.merge_succeeds,
                message: String::new(),
                failure_kind: if self.merge_succeeds {
                    None
                } else {
                    Some(self.failure_kind)
                },
            }
        }
        fn check_mergeability(&self, _pr: i64, _repo: &Path) -> MergeabilityState {
            if self.conflicting {
                MergeabilityState::Conflicting
            } else {
                MergeabilityState::Mergeable
            }
        }
        fn head_sha(&self, pr: i64, _repo: &Path) -> Option<String> {
            self.heads.get(&pr).cloned()
        }
        fn merge_commit_sha(&self, pr: i64, _repo: &Path) -> Option<String> {
            self.merge_commit_available
                .then(|| self.heads.get(&pr).map(|_| format!("merge-{pr}")))
                .flatten()
        }
    }

    fn seed_task(conn: &mut rusqlite::Connection, title: &str, worker: &str) -> i64 {
        let tid =
            tasks::create(conn, "boss", title, None, 50, None, None, None, None, 1000).unwrap();
        conn.execute(
            "UPDATE tasks SET status='working', assignee=?2, author=?2 WHERE id=?1",
            rusqlite::params![tid, worker],
        )
        .unwrap();
        tid
    }

    fn seed_journal(conn: &mut rusqlite::Connection, agent: &str, task_id: i64, pr: i64) {
        journal::upsert(
            conn,
            &journal::JournalEntry {
                agent: agent.into(),
                role: "worker".into(),
                task_id: Some(task_id),
                session_id: "sess".into(),
                worktree: Some("/tmp/wt/x".into()),
                branch: Some("feat/x".into()),
                phase: "awaiting-review".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: Some(pr),
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: None,
            },
        )
        .unwrap();
    }

    fn exec_arc(m: MockExec) -> Arc<dyn merge::MergeExecutor> {
        Arc::new(m)
    }

    /// Acceptance #228 (primary): a durable approval by a reviewer NOT in any
    /// live roster, whose approved head matches the PR's current head, is
    /// merged and its task closed by the restarted instance.
    #[tokio::test]
    async fn matching_approval_is_merged_and_task_closed() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "impl", "Bellows-d11");
        assert!(tasks::resolve_target_branch(&mut conn, tid, "develop", 1002).unwrap());
        conn.execute(
            "UPDATE tasks
             SET status='merging', refs=json_set(COALESCE(refs, '{}'), '$.pr', 208)
             WHERE id=?1",
            [tid],
        )
        .unwrap();
        seed_journal(&mut conn, "Bellows-d11", tid, 208);
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r1".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Grommet-d14".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "2c0c833".into(),
            },
        )
        .unwrap();
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r2".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Anvil-d22".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "2c0c833".into(),
            },
        )
        .unwrap();
        quorum_core::review_audits::record_r2_requirement(&mut conn, tid, 208, "2c0c833", true)
            .unwrap();
        drop(conn);

        let concrete = Arc::new(MockExec::new(HashMap::from([(208, "2c0c833".to_string())])));
        let exec: Arc<dyn merge::MergeExecutor> = concrete.clone();
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.merged, 1, "matching approval must merge");
        assert_eq!(
            concrete.merged.lock().unwrap().as_slice(),
            &[(208, "develop".to_string())],
            "restart recovery must pass the immutable task target to merge"
        );
        // Task closed, approval consumed, journal cleaned.
        let conn = db::open(&db_path).unwrap();
        assert_eq!(tasks::get(&conn, tid).unwrap().unwrap().status, "done");
        assert!(approvals::get_for_pr(&conn, 208).unwrap().is_empty());
        assert!(journal::list_in_flight(&conn).unwrap().is_empty());
    }

    #[tokio::test]
    async fn recovered_in_review_approval_records_merge_commit_sha() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "in-review recovery", "Bellows-d11");
        assert!(tasks::resolve_target_branch(&mut conn, tid, "develop", 1002).unwrap());
        conn.execute(
            "UPDATE tasks
             SET status='in-review', refs=json_set(COALESCE(refs, '{}'), '$.pr', 208)
             WHERE id=?1",
            [tid],
        )
        .unwrap();
        seed_journal(&mut conn, "Bellows-d11", tid, 208);
        for (role, reviewer) in [("r1", "Grommet-d14"), ("r2", "Anvil-d22")] {
            approvals::record(
                &mut conn,
                &approvals::Approval {
                    pr_number: 208,
                    review_role: role.into(),
                    task_id: tid,
                    author: "Bellows-d11".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "2c0c833".into(),
                },
            )
            .unwrap();
        }
        quorum_core::review_audits::record_r2_requirement(&mut conn, tid, 208, "2c0c833", true)
            .unwrap();
        drop(conn);

        let exec = exec_arc(MockExec::new(HashMap::from([(208, "2c0c833".to_string())])));
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.merged, 1);
        let conn = db::open(&db_path).unwrap();
        let task = tasks::get(&conn, tid).unwrap().unwrap();
        assert_eq!(task.status, "done");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[tasks::MERGE_COMMIT_SHA_REF], "merge-208");
    }

    #[tokio::test]
    async fn recovered_merge_without_commit_sha_stays_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "metadata retry", "Bellows-d11");
        assert!(tasks::resolve_target_branch(&mut conn, tid, "develop", 1002).unwrap());
        conn.execute(
            "UPDATE tasks
             SET status='merging', refs=json_set(COALESCE(refs, '{}'), '$.pr', 208)
             WHERE id=?1",
            [tid],
        )
        .unwrap();
        seed_journal(&mut conn, "Bellows-d11", tid, 208);
        for (role, reviewer) in [("r1", "Grommet-d14"), ("r2", "Anvil-d22")] {
            approvals::record(
                &mut conn,
                &approvals::Approval {
                    pr_number: 208,
                    review_role: role.into(),
                    task_id: tid,
                    author: "Bellows-d11".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "2c0c833".into(),
                },
            )
            .unwrap();
        }
        quorum_core::review_audits::record_r2_requirement(&mut conn, tid, 208, "2c0c833", true)
            .unwrap();
        drop(conn);

        let exec = exec_arc(
            MockExec::new(HashMap::from([(208, "2c0c833".to_string())])).without_merge_commit_sha(),
        );
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.merged, 0);
        assert_eq!(outcome.deferred, 1);
        let conn = db::open(&db_path).unwrap();
        let task = tasks::get(&conn, tid).unwrap().unwrap();
        assert_eq!(task.status, "failed");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[tasks::PARKED_RESUME_STATUS_REF], "merging");
        assert!(refs[tasks::PARKED_REASON_REF]
            .as_str()
            .unwrap()
            .contains("merge commit metadata is unavailable"));
        assert!(
            refs.get(tasks::MERGE_COMMIT_SHA_REF).is_none(),
            "the task must not be completed with a fabricated merge SHA"
        );
        assert_eq!(approvals::get_for_pr(&conn, 208).unwrap().len(), 2);
        assert_eq!(journal::list_in_flight(&conn).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn missing_sampled_r2_decision_returns_merging_task_to_fresh_r1() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "missing sampling", "Worker-T");
        conn.execute(
            "UPDATE tasks
             SET status='merging', refs=json_set(COALESCE(refs, '{}'), '$.pr', 208)
             WHERE id=?1",
            [tid],
        )
        .unwrap();
        for (role, reviewer) in [("r1", "Reviewer-1"), ("r2", "Reviewer-2")] {
            approvals::record(
                &mut conn,
                &approvals::Approval {
                    pr_number: 208,
                    review_role: role.into(),
                    task_id: tid,
                    author: "Worker-T".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "same-head".into(),
                },
            )
            .unwrap();
        }
        drop(conn);

        let concrete = Arc::new(MockExec::new(HashMap::from([(
            208,
            "same-head".to_string(),
        )])));
        let exec: Arc<dyn merge::MergeExecutor> = concrete.clone();
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.deferred, 1);
        assert!(concrete.merged.lock().unwrap().is_empty());
        let conn = db::open(&db_path).unwrap();
        assert_eq!(tasks::get(&conn, tid).unwrap().unwrap().status, "in-review");
        assert!(approvals::get(&conn, 208, "r1").unwrap().is_none());
        assert!(approvals::get(&conn, 208, "r2").unwrap().is_some());
    }

    #[tokio::test]
    async fn mismatched_sampled_r2_decision_is_deleted_and_cannot_merge() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "mismatched sampling", "Worker-T");
        let other = seed_task(&mut conn, "sampling owner", "Worker-X");
        conn.execute(
            "UPDATE tasks
             SET status='merging', refs=json_set(COALESCE(refs, '{}'), '$.pr', 208)
             WHERE id=?1",
            [tid],
        )
        .unwrap();
        for (role, reviewer) in [("r1", "Reviewer-1"), ("r2", "Reviewer-2")] {
            approvals::record(
                &mut conn,
                &approvals::Approval {
                    pr_number: 208,
                    review_role: role.into(),
                    task_id: tid,
                    author: "Worker-T".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "same-head".into(),
                },
            )
            .unwrap();
        }
        quorum_core::review_audits::record_r2_requirement(&mut conn, other, 208, "same-head", true)
            .unwrap();
        drop(conn);

        let concrete = Arc::new(MockExec::new(HashMap::from([(
            208,
            "same-head".to_string(),
        )])));
        let exec: Arc<dyn merge::MergeExecutor> = concrete.clone();
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.deferred, 1);
        assert!(concrete.merged.lock().unwrap().is_empty());
        let conn = db::open(&db_path).unwrap();
        assert_eq!(tasks::get(&conn, tid).unwrap().unwrap().status, "in-review");
        assert!(approvals::get(&conn, 208, "r1").unwrap().is_none());
        assert!(approvals::get(&conn, 208, "r2").unwrap().is_some());
        assert!(conn
            .query_row(
                "SELECT 1 FROM r2_sampling_decisions WHERE pr_number=208 AND head_sha='same-head'",
                [],
                |_| Ok(()),
            )
            .optional()
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn mixed_task_approval_set_is_deferred_before_any_startup_merge_call() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let unrelated = seed_task(&mut conn, "unrelated", "Worker-X");
        let parked = seed_task(&mut conn, "parked merge", "Worker-T");
        conn.execute(
            "UPDATE tasks SET refs=json_set(COALESCE(refs, '{}'), '$.pr', 208) WHERE id IN (?1, ?2)",
            rusqlite::params![unrelated, parked],
        )
        .unwrap();
        tasks::park(&mut conn, parked, "credential failure", "merging", 1002)
            .unwrap()
            .unwrap();
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r1".into(),
                task_id: unrelated,
                author: "Worker-X".into(),
                reviewer: "Reviewer-1".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "same-head".into(),
            },
        )
        .unwrap();
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r2".into(),
                task_id: parked,
                author: "Worker-T".into(),
                reviewer: "Reviewer-2".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "same-head".into(),
            },
        )
        .unwrap();
        drop(conn);

        let concrete = Arc::new(MockExec::new(HashMap::from([(
            208,
            "same-head".to_string(),
        )])));
        let exec: Arc<dyn merge::MergeExecutor> = concrete.clone();
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.deferred, 1);
        assert!(concrete.merged.lock().unwrap().is_empty());
        let conn = db::open(&db_path).unwrap();
        assert_ne!(
            tasks::get(&conn, unrelated).unwrap().unwrap().status,
            "done"
        );
        assert_eq!(tasks::get(&conn, parked).unwrap().unwrap().status, "failed");
        assert_eq!(approvals::get_for_pr(&conn, 208).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn attempting_marker_prevents_startup_replay_when_policy_park_was_not_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "uncertain merge", "Worker-T");
        conn.execute(
            "UPDATE tasks
             SET status='merging', refs=json_set(COALESCE(refs, '{}'), '$.pr', 208)
             WHERE id=?1",
            [tid],
        )
        .unwrap();
        for (role, reviewer) in [("r1", "Reviewer-1"), ("r2", "Reviewer-2")] {
            approvals::record(
                &mut conn,
                &approvals::Approval {
                    pr_number: 208,
                    review_role: role.into(),
                    task_id: tid,
                    author: "Worker-T".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "same-head".into(),
                },
            )
            .unwrap();
        }
        assert!(tasks::begin_approved_merge_attempt(&mut conn, tid, 1002).unwrap());
        drop(conn);

        let concrete = Arc::new(MockExec::new(HashMap::from([(
            208,
            "same-head".to_string(),
        )])));
        let exec: Arc<dyn merge::MergeExecutor> = concrete.clone();
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.deferred, 1);
        assert!(concrete.merged.lock().unwrap().is_empty());
        let conn = db::open(&db_path).unwrap();
        assert_eq!(tasks::get(&conn, tid).unwrap().unwrap().status, "merging");
        assert_eq!(approvals::get_for_pr(&conn, 208).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn startup_policy_failure_parks_once_with_approvals_retained() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "startup policy failure", "Worker-T");
        assert!(tasks::resolve_target_branch(&mut conn, tid, "main", 1001).unwrap());
        conn.execute(
            "UPDATE tasks
             SET status='merging', refs=json_set(COALESCE(refs, '{}'), '$.pr', 208)
             WHERE id=?1",
            [tid],
        )
        .unwrap();
        for (role, reviewer) in [("r1", "Reviewer-1"), ("r2", "Reviewer-2")] {
            approvals::record(
                &mut conn,
                &approvals::Approval {
                    pr_number: 208,
                    review_role: role.into(),
                    task_id: tid,
                    author: "Worker-T".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "same-head".into(),
                },
            )
            .unwrap();
        }
        quorum_core::review_audits::record_r2_requirement(&mut conn, tid, 208, "same-head", true)
            .unwrap();
        drop(conn);

        let concrete = Arc::new(
            MockExec::new(HashMap::from([(208, "same-head".to_string())]))
                .failing(merge::MergeFailureKind::PolicyBlocked),
        );
        let exec: Arc<dyn merge::MergeExecutor> = concrete.clone();
        let first = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();
        assert_eq!(first.deferred, 1);
        assert_eq!(concrete.merged.lock().unwrap().len(), 1);

        let conn = db::open(&db_path).unwrap();
        let task = tasks::get(&conn, tid).unwrap().unwrap();
        assert_eq!(task.status, "failed");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[tasks::PARKED_REF], true);
        assert_eq!(refs[tasks::PARKED_RESUME_STATUS_REF], "merging");
        assert_eq!(refs[tasks::MERGE_RETRY_REF], tasks::MERGE_RETRY_ATTEMPTING);
        assert_eq!(approvals::get_for_pr(&conn, 208).unwrap().len(), 2);
        drop(conn);

        let second = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();
        assert_eq!(second.deferred, 1);
        assert_eq!(
            concrete.merged.lock().unwrap().len(),
            1,
            "a parked startup failure must not replay without another owner retry"
        );
        let conn = db::open(&db_path).unwrap();
        let parked_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE subject=?1 AND kind='task_parked'",
                [tasks::lease_target(tid)],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parked_events, 1);
        assert_eq!(approvals::get_for_pr(&conn, 208).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn startup_retryable_failure_atomically_enters_rework() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "startup worker fix", "Worker-T");
        assert!(tasks::resolve_target_branch(&mut conn, tid, "main", 1001).unwrap());
        conn.execute(
            "UPDATE tasks
             SET status='merging', refs=json_set(COALESCE(refs, '{}'), '$.pr', 208)
             WHERE id=?1",
            [tid],
        )
        .unwrap();
        for (role, reviewer) in [("r1", "Reviewer-1"), ("r2", "Reviewer-2")] {
            approvals::record(
                &mut conn,
                &approvals::Approval {
                    pr_number: 208,
                    review_role: role.into(),
                    task_id: tid,
                    author: "Worker-T".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "same-head".into(),
                },
            )
            .unwrap();
        }
        quorum_core::review_audits::record_r2_requirement(&mut conn, tid, 208, "same-head", true)
            .unwrap();
        drop(conn);

        let concrete = Arc::new(
            MockExec::new(HashMap::from([(208, "same-head".to_string())]))
                .failing(merge::MergeFailureKind::Retryable),
        );
        let exec: Arc<dyn merge::MergeExecutor> = concrete.clone();
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();
        assert_eq!(outcome.deferred, 1);
        assert_eq!(concrete.merged.lock().unwrap().len(), 1);

        let conn = db::open(&db_path).unwrap();
        let task = tasks::get(&conn, tid).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.rework_round, 1);
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert!(refs.get(tasks::MERGE_RETRY_REF).is_none());
        assert_eq!(refs[tasks::PARKED_REWORK_RETRY_REF], true);
        assert!(refs["remediation_feedback"]
            .as_str()
            .unwrap()
            .contains("startup approval replay"));
        assert!(approvals::get_for_pr(&conn, 208).unwrap().is_empty());
    }

    /// Acceptance #228: a stale approval (PR head moved since approval) is NOT
    /// merged — the approval is dropped so the task returns to normal review.
    #[tokio::test]
    async fn stale_approval_is_not_merged() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "impl", "Bellows-d11");
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r1".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Grommet-d14".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "OLD_sha".into(),
            },
        )
        .unwrap();
        drop(conn);

        // Current head differs from approved head → stale.
        let exec = exec_arc(MockExec::new(HashMap::from([(208, "NEW_sha".to_string())])));
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.merged, 0, "stale approval must not merge");
        assert_eq!(outcome.demoted, 1);
        let conn = db::open(&db_path).unwrap();
        // Task NOT closed — it stays claimed for normal re-review.
        assert_ne!(tasks::get(&conn, tid).unwrap().unwrap().status, "done");
        assert!(approvals::get_for_pr(&conn, 208).unwrap().is_empty());
    }

    /// Acceptance #228: a self-review approval (reviewer == author) is refused,
    /// even with a matching head SHA — never merged.
    #[tokio::test]
    async fn self_review_approval_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "impl", "Bellows-d11");
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r1".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Bellows-d11".into(), // == author
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "abc".into(),
            },
        )
        .unwrap();
        drop(conn);

        let exec = exec_arc(MockExec::new(HashMap::from([(208, "abc".to_string())])));
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.merged, 0);
        assert_eq!(outcome.rejected, 1, "self-review must be rejected");
        let conn = db::open(&db_path).unwrap();
        assert_ne!(tasks::get(&conn, tid).unwrap().unwrap().status, "done");
        assert!(approvals::get_for_pr(&conn, 208).unwrap().is_empty());
    }

    /// Acceptance #228 (the observed live bug): a stranded, attested `approved`
    /// mailbox verdict from a reviewer NOT in the current roster is adopted into
    /// a durable approval and merged — instead of being left unconsumed and the
    /// task re-worked.
    #[tokio::test]
    async fn stranded_mailbox_verdict_is_adopted_and_merged() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "impl", "Bellows-d11");
        seed_journal(&mut conn, "Bellows-d11", tid, 208);
        const HEAD: &str = "2c0c833000000000000000000000000000000000";
        // Pre-seed R2 approval (e.g. from a prior R2 run before restart).
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r2".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Anvil-d22".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: HEAD.into(),
            },
        )
        .unwrap();
        quorum_core::review_audits::record_r2_requirement(&mut conn, tid, 208, HEAD, true).unwrap();
        quorum_core::agent_runs::insert_reviewer_with_launch(
            &conn,
            tid,
            "Grommet-d14",
            "model",
            "high",
            "codex",
            None,
            1001,
            None,
            "review-cap-r1",
            208,
            HEAD,
        )
        .unwrap()
        .expect("immutable R1 launch");
        // The prior R1 reviewer's attested approval, still unconsumed in the mailbox.
        let mid = mailbox::append(
            &mut conn,
            &mailbox::MailboxRow {
                agent: "Grommet-d14".into(),
                kind: mailbox::MailboxKind::Done,
                task_id: Some(tid),
                pr: Some(208),
                verdict: Some("approved".into()),
                feedback: None,
                note: None,
                to_agent: None,
                payload: Some("{\"blocking\":0}".into()),
            },
        )
        .unwrap();
        drop(conn);

        let exec = exec_arc(MockExec::new(HashMap::from([(208, HEAD.to_string())])));
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.adopted, 1, "stranded verdict must be adopted");
        assert_eq!(outcome.merged, 1, "adopted approval must merge");
        let conn = db::open(&db_path).unwrap();
        assert_eq!(tasks::get(&conn, tid).unwrap().unwrap().status, "done");
        // Mailbox row consumed.
        assert!(mailbox::poll_unconsumed(&conn)
            .unwrap()
            .iter()
            .all(|(id, _)| *id != mid));
    }

    #[tokio::test]
    async fn stranded_verdict_cannot_adopt_a_force_pushed_head() {
        const REVIEWED: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const CURRENT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "force pushed", "Bellows-d11");
        seed_journal(&mut conn, "Bellows-d11", tid, 208);
        quorum_core::agent_runs::insert_reviewer_with_launch(
            &conn,
            tid,
            "Grommet-d14",
            "model",
            "high",
            "codex",
            None,
            1001,
            None,
            "review-cap-old-head",
            208,
            REVIEWED,
        )
        .unwrap()
        .expect("immutable review launch");
        let mailbox_id = mailbox::append(
            &mut conn,
            &mailbox::MailboxRow {
                agent: "Grommet-d14".into(),
                kind: mailbox::MailboxKind::Done,
                task_id: Some(tid),
                pr: Some(208),
                verdict: Some("approved".into()),
                feedback: None,
                note: None,
                to_agent: None,
                payload: Some("{\"blocking\":0}".into()),
            },
        )
        .unwrap();
        drop(conn);

        let concrete = Arc::new(MockExec::new(HashMap::from([(208, CURRENT.to_string())])));
        let exec: Arc<dyn merge::MergeExecutor> = concrete.clone();
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.adopted, 0);
        assert_eq!(outcome.merged, 0);
        assert!(concrete.merged.lock().unwrap().is_empty());
        let conn = db::open(&db_path).unwrap();
        assert!(approvals::get_for_pr(&conn, 208).unwrap().is_empty());
        assert!(mailbox::poll_unconsumed(&conn)
            .unwrap()
            .iter()
            .any(|(id, _)| *id == mailbox_id));
    }

    /// Crash-recovery safety: a forged/unattested mailbox verdict (no
    /// `{blocking:0}` payload) is NOT adopted and NOT merged.
    #[tokio::test]
    async fn unattested_mailbox_verdict_is_not_adopted() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "impl", "Bellows-d11");
        seed_journal(&mut conn, "Bellows-d11", tid, 208);
        mailbox::append(
            &mut conn,
            &mailbox::MailboxRow {
                agent: "Grommet-d14".into(),
                kind: mailbox::MailboxKind::Done,
                task_id: None,
                pr: Some(208),
                verdict: Some("approved".into()),
                feedback: None,
                note: None,
                to_agent: None,
                payload: None, // no attestation → forged/unvalidated
            },
        )
        .unwrap();
        drop(conn);

        let exec = exec_arc(MockExec::new(HashMap::from([(208, "2c0c833".to_string())])));
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.adopted, 0, "unattested verdict must not be adopted");
        assert_eq!(outcome.merged, 0);
        let conn = db::open(&db_path).unwrap();
        assert_ne!(tasks::get(&conn, tid).unwrap().unwrap().status, "done");
    }

    /// A conflicting PR (base moved) is not merged even with a valid approval —
    /// the approval is dropped so the task returns to rework.
    #[tokio::test]
    async fn conflicting_pr_is_not_merged() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "impl", "Bellows-d11");
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r1".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Grommet-d14".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "abc".into(),
            },
        )
        .unwrap();
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r2".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Anvil-d22".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "abc".into(),
            },
        )
        .unwrap();
        quorum_core::review_audits::record_r2_requirement(&mut conn, tid, 208, "abc", true)
            .unwrap();
        drop(conn);

        let mut m = MockExec::new(HashMap::from([(208, "abc".to_string())]));
        m.conflicting = true;
        let exec = exec_arc(m);
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.merged, 0);
        assert_eq!(outcome.deferred, 1);
        let conn = db::open(&db_path).unwrap();
        assert_ne!(tasks::get(&conn, tid).unwrap().unwrap().status, "done");
        assert!(approvals::get_for_pr(&conn, 208).unwrap().is_empty());
    }

    /// #191 regression: R1-only approval with matching SHA is deferred (not
    /// merged, not demoted) — R2 is still missing. The approval is preserved
    /// so generic recovery can act on it.
    #[tokio::test]
    async fn r1_only_matching_sha_is_deferred() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "impl", "Bellows-d11");
        seed_journal(&mut conn, "Bellows-d11", tid, 208);
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r1".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Grommet-d14".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "2c0c833".into(),
            },
        )
        .unwrap();
        drop(conn);

        let exec = exec_arc(MockExec::new(HashMap::from([(208, "2c0c833".to_string())])));
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.merged, 0, "R1-only must not merge");
        assert_eq!(outcome.demoted, 0, "R1 matches SHA, not demoted");
        assert_eq!(outcome.deferred, 1, "incomplete review deferred");
        let conn = db::open(&db_path).unwrap();
        assert_ne!(tasks::get(&conn, tid).unwrap().unwrap().status, "done");
        // This legacy fixture has no task-level PR association, so recovery
        // cannot selectively mutate its role row; it still refuses merge.
        assert!(approvals::get(&conn, 208, "r1").unwrap().is_some());
    }

    /// #191: mixed-SHA approvals (R1 for one SHA, R2 for another) cannot
    /// authorize merge — both are demoted/deferred.
    #[tokio::test]
    async fn mixed_sha_approvals_cannot_merge() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = db::open(&db_path).unwrap();
        let tid = seed_task(&mut conn, "impl", "Bellows-d11");
        seed_journal(&mut conn, "Bellows-d11", tid, 208);
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r1".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Grommet-d14".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "sha_old".into(),
            },
        )
        .unwrap();
        approvals::record(
            &mut conn,
            &approvals::Approval {
                pr_number: 208,
                review_role: "r2".into(),
                task_id: tid,
                author: "Bellows-d11".into(),
                reviewer: "Anvil-d22".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "sha_new".into(),
            },
        )
        .unwrap();
        drop(conn);

        // Current head matches R2 but not R1 → R1 is stale → demoted.
        let exec = exec_arc(MockExec::new(HashMap::from([(208, "sha_new".to_string())])));
        let outcome = recover(&db_path, Path::new("/tmp/repo"), &exec, "main", 10, 1)
            .await
            .unwrap();

        assert_eq!(outcome.merged, 0, "mixed-SHA must not merge");
        assert_eq!(outcome.demoted, 1, "stale R1 demoted");
    }
}
