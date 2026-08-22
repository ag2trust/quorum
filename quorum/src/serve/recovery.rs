//! Crash recovery on daemon restart.
//!
//! On startup:
//! 1. Kill all stale process groups (from journal PIDs)
//! 2. Reconstruct validated dormant review/merge-wait/rework Codex workers in place
//!    and retire rows transferred to fresh-retry/graph-block authority
//! 3. Delete stale/retired journal entries and GC only non-recovered worktrees
//! 4. Scan non-terminal tasks and reset them to states the normal tick
//!    loop can handle:
//!    - `working` / `rework` → AgentFailed → open (Phase 6 re-spawns)
//!    - `merging` → AgentFailed → in-review (Phase 5 spawns reviewer)
//!    - `in-review` → left as-is (Phase 5 spawns reviewer)
//!
//! Reconstructed rework turns are marked for the startup coordinator to resume
//! only after exact PR-baseline recovery.

use super::worktree::WorktreeManager;
use super::{log, LifetimeRoster, LiveStats, ServeConfig, SlotProcess, SlotState};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::lifecycle::Event;
use quorum_core::runner_state::{self, ContinuationSlot};
use quorum_core::{approvals, error::QuorumError, error::Result, tasks};
use rusqlite::OptionalExtension;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct DormantRecovery {
    entry: JournalEntry,
    task_id: i64,
    pr: i64,
    provider: super::runner::AgentKind,
    continuation_id: String,
    local_branch: String,
    model: String,
    effort: String,
    agent_run_id: i64,
    token_usage: super::runner::TokenUsage,
    limit_tokens: i64,
    cap_run_id: String,
    rework_feedback: Option<String>,
    needs_rework_claim: bool,
}

#[derive(Debug, Clone)]
enum DormantRecoveryDisposition {
    Reconstruct(Box<DormantRecovery>),
    Retire {
        entry: Box<JournalEntry>,
        reason: &'static str,
    },
}

fn dormant_recovery_error(agent: &str, detail: impl std::fmt::Display) -> QuorumError {
    QuorumError::Io(format!(
        "dormant awaiting-review recovery rejected for '{agent}': {detail}"
    ))
}

fn is_explicit_dormant_recovery_entry(entry: &JournalEntry) -> bool {
    entry.role == "worker"
        && ((entry.phase == "awaiting-review" && entry.pid.is_none())
            || (entry.phase == "resuming-rework" && entry.pid.is_some()))
}

fn validate_dormant_recovery(
    conn: &rusqlite::Connection,
    entry: &JournalEntry,
    now: i64,
    token_limit_basis: crate::serve_config::TokenLimitBasis,
) -> Result<DormantRecoveryDisposition> {
    let invalid = |detail: String| dormant_recovery_error(&entry.agent, detail);
    if !is_explicit_dormant_recovery_entry(entry) {
        return Err(invalid(
            "journal phase/process shape is neither dormant nor an interrupted recovered rework"
                .into(),
        ));
    }
    let task_id = entry
        .task_id
        .filter(|id| *id > 0)
        .ok_or_else(|| invalid("missing task".into()))?;
    let pr = entry
        .pr
        .filter(|pr| *pr > 0)
        .ok_or_else(|| invalid("missing PR".into()))?;
    let worktree = entry
        .worktree
        .as_deref()
        .filter(|path| !path.is_empty())
        .ok_or_else(|| invalid("missing worktree".into()))?;
    let remote_branch = entry
        .branch
        .as_deref()
        .filter(|branch| !branch.is_empty())
        .ok_or_else(|| invalid("missing publication branch".into()))?;
    let local_branch = entry
        .local_branch
        .as_deref()
        .filter(|branch| !branch.is_empty())
        .ok_or_else(|| invalid("missing local branch".into()))?
        .to_string();
    let provider_name = entry
        .provider
        .as_deref()
        .filter(|provider| !provider.is_empty())
        .ok_or_else(|| invalid("missing provider".into()))?;
    let provider = match provider_name {
        "codex" => super::runner::AgentKind::Codex,
        "claude" => super::runner::AgentKind::Claude,
        "grok" => super::runner::AgentKind::Grok,
        other => return Err(invalid(format!("unknown provider '{other}'"))),
    };
    if provider.turn_mode() != super::runner::TurnMode::RespawnPerTurn {
        return Err(invalid(format!(
            "provider '{provider}' requires a persistent process"
        )));
    }
    let continuation_id = entry
        .continuation_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| invalid("missing provider continuation".into()))?
        .to_string();

    let task = tasks::get(conn, task_id)?
        .ok_or_else(|| invalid(format!("task #{task_id} no longer exists")))?;
    if tasks::extract_pr_number(&task.refs) != Some(pr) {
        return Err(invalid(format!(
            "artifact mismatch: task #{task_id} is not bound to PR #{pr}"
        )));
    }
    let refs: serde_json::Value = task
        .refs
        .as_deref()
        .ok_or_else(|| invalid("task refs are missing".into()))
        .and_then(|raw| {
            serde_json::from_str(raw)
                .map_err(|error| invalid(format!("task refs are invalid JSON: {error}")))
        })?;
    let claim_holder: Option<String> = conn
        .query_row(
            "SELECT holder FROM claims
              WHERE target=?1 AND active=1 AND expires_at>?2",
            rusqlite::params![tasks::lease_target(task_id), now],
            |row| row.get(0),
        )
        .optional()?;
    // Late reviewer and approval recovery run before this pass. Preserve a
    // worker only while review/merge authority still needs its exact slot;
    // accepted rework and graph-block transitions instead retire the stale
    // runtime row after every immutable binding below has been revalidated.
    let mut rework_feedback = None;
    let mut needs_rework_claim = false;
    let task_disposition = match task.status.as_str() {
        "in-review" | "merging" => {
            if claim_holder.as_deref() != Some(entry.agent.as_str()) {
                return Err(invalid(format!(
                    "claim mismatch: task #{task_id} has no live lease for this agent"
                )));
            }
            None
        }
        "rework" => {
            let retry_requested = refs
                .get(tasks::PARKED_REWORK_RETRY_REF)
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let feedback = refs
                .get("remediation_feedback")
                .and_then(serde_json::Value::as_str)
                .or_else(|| {
                    refs.get(tasks::CI_REMEDIATION_FEEDBACK_REF)
                        .and_then(serde_json::Value::as_str)
                })
                .map(str::trim)
                .filter(|feedback| !feedback.is_empty())
                .map(str::to_string);
            if retry_requested {
                if feedback.is_none() {
                    return Err(invalid(format!(
                        "stale journal: task #{task_id} entered rework without an authoritative retry"
                    )));
                }
                if task.assignee.is_some() || claim_holder.is_some() {
                    return Err(invalid(format!(
                        "claim mismatch: authoritative rework task #{task_id} retained runtime ownership"
                    )));
                }
                Some("late reviewer changes verdict")
            } else {
                match (task.assignee.as_deref(), claim_holder.as_deref()) {
                    (None, None) => needs_rework_claim = true,
                    (Some(assignee), None) if assignee == entry.agent => needs_rework_claim = true,
                    (Some(assignee), Some(holder))
                        if assignee == entry.agent && holder == entry.agent => {}
                    _ => {
                        return Err(invalid(format!(
                            "claim mismatch: sticky rework task #{task_id} is not owned wholly by this agent"
                        )));
                    }
                }
                rework_feedback = Some(feedback.ok_or_else(|| {
                    invalid(format!(
                        "stale journal: sticky rework task #{task_id} is missing its exact pending turn"
                    ))
                })?);
                None
            }
        }
        "failed" => {
            let graph_blocked: bool = conn.query_row(
                "SELECT COALESCE((
                     SELECT kind FROM events
                      WHERE subject=?1 ORDER BY seq DESC LIMIT 1
                 ), '')='task_graph_blocked'",
                [tasks::lease_target(task_id)],
                |row| row.get(0),
            )?;
            if !graph_blocked || task.assignee.is_some() || claim_holder.is_some() {
                return Err(invalid(format!(
                    "stale journal: task #{task_id} is failed without a settled graph blocker"
                )));
            }
            Some("settled graph blocker")
        }
        status => {
            return Err(invalid(format!(
                "stale journal: task #{task_id} is '{status}' instead of a recoverable review state"
            )));
        }
    };
    let persisted = runner_state::continuation(&refs, ContinuationSlot::Worker, provider_name)
        .ok_or_else(|| {
            invalid("task refs are missing the matching provider continuation".into())
        })?;
    if persisted.id != continuation_id {
        return Err(invalid(
            "journal continuation does not match task refs".into(),
        ));
    }

    let runs = quorum_core::agent_runs::runs_for_task(conn, task_id)?;
    let mut active_runs = runs
        .iter()
        .filter(|run| run.role == "worker" && run.agent == entry.agent && run.ended_at.is_none());
    let active_run = active_runs.next();
    if active_run.is_some() && active_runs.next().is_some() {
        return Err(invalid("multiple active worker run bindings".into()));
    }
    // A failed post-launch journal handoff has already killed and reaped its
    // provider, so that run is truthfully closed. The unchanged PID-less
    // awaiting-review journal and active capability still authorize one exact
    // retry; recover the newest failed handoff as that dormant binding.
    let run = active_run
        .or_else(|| {
            runs.iter()
                .rev()
                .find(|run| run.role == "worker" && run.agent == entry.agent)
                .filter(|run| run.end_reason.as_deref() == Some("journal-handoff-failed"))
        })
        .ok_or_else(|| invalid("missing active or retryable worker run binding".into()))?;
    if run.provider.as_deref() != Some(provider_name) {
        return Err(invalid("worker run provider mismatch".into()));
    }
    let model_provider = super::resolve_worker_provider(&run.model)
        .map_err(|error| invalid(format!("worker run model is invalid: {error}")))?;
    if model_provider != provider {
        return Err(invalid("worker run model/provider mismatch".into()));
    }
    let token_usage = quorum_core::token_usage::usage_for_agent_run(conn, run.id)?
        .map(|usage| -> Result<super::runner::TokenUsage> {
            Ok(super::runner::TokenUsage {
                // The legacy live total is recovered independently from the
                // journal's scalar `cost_tokens`; only durable split buckets
                // are restored here.
                input_tokens: 0,
                uncached_input_tokens: u64::try_from(usage.uncached_input_tokens)
                    .map_err(|_| invalid("negative uncached token snapshot".into()))?,
                cached_input_tokens: u64::try_from(usage.cached_input_tokens)
                    .map_err(|_| invalid("negative cached token snapshot".into()))?,
                cache_write_input_tokens: u64::try_from(usage.cache_write_input_tokens)
                    .map_err(|_| invalid("negative cache-write token snapshot".into()))?,
                output_tokens: u64::try_from(usage.output_tokens)
                    .map_err(|_| invalid("negative output token snapshot".into()))?,
                reasoning_tokens: u64::try_from(usage.reasoning_tokens)
                    .map_err(|_| invalid("negative reasoning token snapshot".into()))?,
            })
        })
        .transpose()?
        .unwrap_or_default();
    let limit_tokens =
        recovered_limit_tokens(conn, &runs, task_id, &entry.agent, token_limit_basis)?;
    let capability =
        quorum_core::capabilities::active_for_agent_task(conn, &entry.agent, task_id, "worker")?
            .ok_or_else(|| invalid("missing active run capability binding".into()))?;
    let capability_count: i64 = conn.query_row(
        "SELECT count(*) FROM run_capabilities
         WHERE agent=?1 AND task_id=?2 AND role='worker' AND revoked_at IS NULL",
        rusqlite::params![entry.agent, task_id],
        |row| row.get(0),
    )?;
    if capability_count != 1 {
        return Err(invalid(format!(
            "expected one active run capability binding, found {capability_count}"
        )));
    }

    let allocation: Option<(String, String)> = conn
        .query_row(
            "SELECT branch,worktree FROM task_branches WHERE task_id=?1",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let allocation_matches =
        allocation
            .as_ref()
            .is_some_and(|(allocated_branch, allocated_worktree)| {
                allocated_branch == &local_branch
                    && allocated_branch == remote_branch
                    && allocated_worktree == worktree
            });
    if let Some((_, allocated_worktree)) = allocation.as_ref() {
        if allocated_worktree == worktree && !allocation_matches {
            return Err(invalid(
                "journal branch does not match its durable task allocation".into(),
            ));
        }
    }
    let target_branch: Option<String> = conn
        .query_row(
            "SELECT head_ref FROM pr_targets WHERE task_id=?1 AND pr_number=?2",
            rusqlite::params![task_id, pr],
            |row| row.get(0),
        )
        .optional()?;
    let target_matches = target_branch.as_deref() == Some(remote_branch);
    if target_branch.is_some() && !target_matches {
        return Err(invalid(
            "journal publication branch does not match the durable PR target".into(),
        ));
    }
    if !allocation_matches && !target_matches {
        return Err(invalid(
            "artifact mismatch: no durable branch/worktree or PR target binds this journal row"
                .into(),
        ));
    }

    let recovery = DormantRecovery {
        entry: entry.clone(),
        task_id,
        pr,
        provider,
        continuation_id,
        local_branch,
        model: run.model.clone(),
        effort: run.effort.clone(),
        agent_run_id: run.id,
        token_usage,
        limit_tokens,
        cap_run_id: capability.run_id,
        rework_feedback,
        needs_rework_claim,
    };
    if entry.phase == "resuming-rework" && recovery.rework_feedback.is_none() {
        return Err(invalid(
            "interrupted recovered rework has no exact pending remediation turn".into(),
        ));
    }
    Ok(match task_disposition {
        Some(reason) => DormantRecoveryDisposition::Retire {
            entry: Box::new(entry.clone()),
            reason,
        },
        None => DormantRecoveryDisposition::Reconstruct(Box::new(recovery)),
    })
}

/// Rebuild an active watchdog total from durable split buckets. The journal's
/// `cost_tokens` remains the historical raw inspection scalar and cannot be
/// reused after a repository changes its token-limit basis.
fn recovered_limit_tokens(
    conn: &rusqlite::Connection,
    runs: &[quorum_core::agent_runs::AgentRun],
    task_id: i64,
    agent: &str,
    basis: crate::serve_config::TokenLimitBasis,
) -> Result<i64> {
    let matching_runs: HashSet<i64> = runs
        .iter()
        .filter(|run| run.role == "worker" && run.agent == agent)
        .map(|run| run.id)
        .collect();
    let mut total = 0_i64;
    for usage_run in quorum_core::token_usage::for_task(conn, task_id)? {
        let Some(agent_run_id) = usage_run.agent_run_id else {
            continue;
        };
        if !matching_runs.contains(&agent_run_id) {
            continue;
        }
        let usage = usage_run.usage;
        let uncached = u64::try_from(usage.uncached_input_tokens)
            .map_err(|_| dormant_recovery_error(agent, "negative uncached token snapshot"))?;
        let cached = u64::try_from(usage.cached_input_tokens)
            .map_err(|_| dormant_recovery_error(agent, "negative cached token snapshot"))?;
        let cache_write = u64::try_from(usage.cache_write_input_tokens)
            .map_err(|_| dormant_recovery_error(agent, "negative cache-write token snapshot"))?;
        let output = u64::try_from(usage.output_tokens)
            .map_err(|_| dormant_recovery_error(agent, "negative output token snapshot"))?;
        let input = match usage_run.provider.as_str() {
            // Claude and Codex report cache writes outside input_tokens.
            "claude" | "codex" => uncached.saturating_add(cached),
            // Grok's normalized raw input includes cache creation tokens.
            "grok" => uncached.saturating_add(cached).saturating_add(cache_write),
            provider => {
                return Err(dormant_recovery_error(
                    agent,
                    format!("unknown token-usage provider '{provider}'"),
                ));
            }
        };
        let turn = match basis {
            crate::serve_config::TokenLimitBasis::Raw => input.saturating_add(output),
            crate::serve_config::TokenLimitBasis::Uncached => uncached.saturating_add(output),
        };
        total = total.saturating_add(i64::try_from(turn).unwrap_or(i64::MAX));
    }
    Ok(total)
}

fn normalize_interrupted_rework_journals(
    conn: &mut rusqlite::Connection,
    recoveries: &mut [DormantRecovery],
) -> Result<()> {
    let tx = quorum_core::db::begin_immediate(conn)?;
    for recovery in recoveries.iter_mut() {
        if recovery.entry.phase != "resuming-rework" {
            continue;
        }
        let changed = tx.execute(
            "UPDATE journal SET phase='awaiting-review',pid=NULL,updated_at=?4
              WHERE agent=?1 AND task_id=?2 AND phase='resuming-rework' AND pid=?3",
            rusqlite::params![
                recovery.entry.agent,
                recovery.task_id,
                recovery.entry.pid,
                super::now_unix(),
            ],
        )?;
        if changed != 1 {
            return Err(dormant_recovery_error(
                &recovery.entry.agent,
                "interrupted rework journal binding changed before normalization",
            ));
        }
        recovery.entry.phase = "awaiting-review".into();
        recovery.entry.pid = None;
    }
    tx.commit()?;
    Ok(())
}

async fn verify_dormant_worktree(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    recovery: &DormantRecovery,
) -> Result<()> {
    let invalid = |detail: String| dormant_recovery_error(&recovery.entry.agent, detail);
    let worktree = PathBuf::from(recovery.entry.worktree.as_deref().unwrap_or_default());
    let canonical_base = std::fs::canonicalize(&config.worktree_base)
        .map_err(|error| invalid(format!("worktree base is unavailable: {error}")))?;
    let canonical_worktree = std::fs::canonicalize(&worktree)
        .map_err(|error| invalid(format!("worktree is unavailable: {error}")))?;
    if canonical_worktree.parent() != Some(canonical_base.as_path()) {
        return Err(invalid(format!(
            "worktree {} is outside the managed worktree base",
            worktree.display()
        )));
    }
    wt_mgr
        .verify_exact_registration(
            &config.repo_dir,
            &canonical_worktree,
            &recovery.local_branch,
        )
        .await
        .map_err(|error| invalid(format!("artifact mismatch: {error}")))?;
    Ok(())
}

async fn install_recovered_rework_claim(db_path: &Path, recovery: &DormantRecovery) -> Result<()> {
    if !recovery.needs_rework_claim {
        return Ok(());
    }
    let path = db_path.to_path_buf();
    let agent = recovery.entry.agent.clone();
    let task_id = recovery.task_id;
    let feedback = recovery
        .rework_feedback
        .clone()
        .ok_or_else(|| dormant_recovery_error(&agent, "missing recovered rework feedback"))?;
    let claimed = tokio::task::spawn_blocking(move || -> Result<Option<tasks::Task>> {
        let mut conn = quorum_core::db::open(&path)?;
        tasks::claim_remediation_rework_with_feedback(
            &mut conn,
            &agent,
            task_id,
            tasks::DEFAULT_LEASE_TTL_SECS,
            super::now_unix(),
            Some(&feedback),
        )
    })
    .await
    .map_err(|error| QuorumError::Io(format!("recovered rework claim join: {error}")))??;
    if claimed.is_none() {
        return Err(dormant_recovery_error(
            &recovery.entry.agent,
            format!("could not re-install sticky lease for task #{task_id}"),
        ));
    }
    Ok(())
}

fn reconstruct_dormant_slots(
    recoveries: &[DormantRecovery],
    workers: &mut Vec<SlotState>,
    lifetime_roster: &mut LifetimeRoster,
) -> Result<()> {
    for recovery in recoveries {
        if let Some(existing) = workers
            .iter()
            .find(|worker| worker.agent_name == recovery.entry.agent)
        {
            let same = existing.task_id == recovery.task_id
                && existing.pr == Some(recovery.pr)
                && existing.worktree_path
                    == Path::new(recovery.entry.worktree.as_deref().unwrap_or_default())
                && existing.branch == recovery.local_branch
                && existing.remote_branch == recovery.entry.branch.as_deref().unwrap_or_default()
                && existing.continuation_id_for_launch() == Some(&recovery.continuation_id)
                && matches!(existing.proc, SlotProcess::Dormant { .. });
            if same {
                continue;
            }
            return Err(dormant_recovery_error(
                &recovery.entry.agent,
                "name is already bound to a different in-memory slot",
            ));
        }
        if workers
            .iter()
            .any(|worker| worker.task_id == recovery.task_id)
        {
            return Err(dormant_recovery_error(
                &recovery.entry.agent,
                format!("task #{} already has another worker slot", recovery.task_id),
            ));
        }
        lifetime_roster.register(&recovery.entry.agent);
        let now = std::time::Instant::now();
        workers.push(SlotState {
            agent_name: recovery.entry.agent.clone(),
            proc: SlotProcess::dormant(recovery.provider, Some(&recovery.continuation_id))
                .map_err(|error| dormant_recovery_error(&recovery.entry.agent, error))?,
            task_id: recovery.task_id,
            session_id: recovery.entry.session_id.clone(),
            model: recovery.model.clone(),
            effort: recovery.effort.clone(),
            worktree_path: PathBuf::from(recovery.entry.worktree.as_deref().unwrap_or_default()),
            branch: recovery.local_branch.clone(),
            remote_branch: recovery.entry.branch.clone().unwrap_or_default(),
            draining: false,
            pending_watchdog_breach: None,
            pr: Some(recovery.pr),
            rework_count: recovery.entry.rework_count.max(0) as u32,
            cost_tokens: recovery.entry.cost_tokens,
            limit_tokens: recovery.limit_tokens,
            token_usage: recovery.token_usage,
            last_terminal_usage: super::runner::TokenUsage::default(),
            last_terminal_cost_usd: None,
            cost_usd: recovery.entry.cost_usd,
            task_started_at: now,
            turn_started_at: now,
            last_event_at: now,
            turn_ended_at: Some(now),
            agent_state: recovery.entry.agent_state.clone(),
            session_log: None,
            live_stats: LiveStats::new(),
            error_turn_count: 0,
            last_error_text: None,
            agent_run_id: Some(recovery.agent_run_id),
            cap_run_id: Some(recovery.cap_run_id.clone()),
            r2_origin: false,
            reviewed_head_sha: None,
            continuation_id: Some(recovery.continuation_id.clone()),
            pending_prompt: recovery.rework_feedback.clone().unwrap_or_default(),
            pending_turn_kind: if recovery.rework_feedback.is_some() {
                "recovered-rework".into()
            } else {
                "awaiting-review".into()
            },
        });
        log(&format!(
            "recovery: reconstructed dormant worker {} for task #{} PR #{}",
            recovery.entry.agent, recovery.task_id, recovery.pr
        ));
    }
    Ok(())
}

fn reserve_dormant_names(
    entries: &[JournalEntry],
    name_pool: &mut super::names::Pool,
    workers: &[SlotState],
) -> Result<()> {
    for entry in entries {
        if workers
            .iter()
            .any(|worker| worker.agent_name == entry.agent)
        {
            continue;
        }
        name_pool.acquire_named(&entry.agent).ok_or_else(|| {
            dormant_recovery_error(&entry.agent, "persisted name is already reserved")
        })?;
    }
    Ok(())
}

fn kill_stale_process_group(pid: Option<i32>) {
    if let Some(pid) = pid {
        unsafe {
            libc::killpg(pid, libc::SIGKILL);
        }
    }
}

/// After killpg, poll `kill(pid, 0)` until the process is gone or timeout expires.
/// Returns `true` if the process is confirmed dead, `false` if still alive at timeout.
async fn await_process_exit(pid: i32, timeout: std::time::Duration) -> bool {
    let start = std::time::Instant::now();
    let poll_interval = std::time::Duration::from_millis(50);

    loop {
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        if !alive {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

fn remove_journaled_decomposition_views(entries: &[JournalEntry]) {
    for entry in entries {
        if !matches!(entry.role.as_str(), "planner" | "classifier") {
            continue;
        }
        let Some(view) = entry.worktree.as_deref().map(std::path::Path::new) else {
            continue;
        };
        let validated = view
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("quorum-planner-"));
        if validated {
            if let Err(error) = std::fs::remove_dir_all(view) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    log(&format!(
                        "recovery: failed to remove stale decomposition view {}: {error}",
                        view.display()
                    ));
                }
            }
        } else {
            log(&format!(
                "recovery: refused unvalidated decomposition view path {}",
                view.display()
            ));
        }
    }
}

pub(crate) async fn recover(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut super::names::Pool,
    workers: &mut Vec<SlotState>,
    lifetime_roster: &mut LifetimeRoster,
) -> Result<()> {
    let db_path = config.db_path.clone();

    // ── Phase 1: Read journal entries and kill stale process groups ─────
    let entries = {
        let p = db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<JournalEntry>> {
            let conn = quorum_core::db::open(&p)?;
            journal::list_in_flight(&conn)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
    }?;

    let dormant_entries = entries
        .iter()
        .filter(|entry| is_explicit_dormant_recovery_entry(entry))
        .cloned()
        .collect::<Vec<_>>();
    // #130: all unjournaled working/rework tasks are orphaned and follow
    // normal recovery (no passive exemption). Journal entries identify
    // daemon-managed agents whose stale processes must be killed.

    if !entries.is_empty() {
        log(&format!(
            "recovery: found {} stale journal entries — killing processes",
            entries.len(),
        ));

        let reap_timeout = std::time::Duration::from_secs(5);
        for entry in &entries {
            kill_stale_process_group(entry.pid);
            if let Some(pid) = entry.pid {
                if !await_process_exit(pid, reap_timeout).await {
                    log(&format!(
                        "recovery: WARNING — pid {} for agent {} still alive after {}s post-SIGKILL",
                        pid,
                        entry.agent,
                        reap_timeout.as_secs(),
                    ));
                }
            }
        }
    }

    // Reserve every candidate identity before validation. A corrupt dormant
    // row is fatal, but retaining its name in the pool makes the fail-safe
    // local too: even a caller that mishandled the error could not recycle the
    // identity for another task in this daemon process.
    reserve_dormant_names(&dormant_entries, name_pool, workers)?;

    // Decomposition planner views are ordinary temporary archives rather
    // than Git worktrees. Their exact path is journaled with the provider
    // process so abrupt daemon death cannot leak the frozen repository view.
    remove_journaled_decomposition_views(&entries);

    let dispositions = {
        let p = db_path.clone();
        let token_limit_basis = config.limits.token_limit_basis;
        tokio::task::spawn_blocking(move || -> Result<Vec<DormantRecoveryDisposition>> {
            let mut conn = quorum_core::db::open(&p)?;
            let tx = quorum_core::db::begin_immediate(&mut conn)?;
            let dispositions = dormant_entries
                .iter()
                .map(|entry| {
                    validate_dormant_recovery(&tx, entry, super::now_unix(), token_limit_basis)
                })
                .collect::<Result<Vec<_>>>()?;
            tx.commit()?;
            Ok(dispositions)
        })
        .await
        .map_err(|error| QuorumError::Io(format!("dormant recovery join: {error}")))??
    };
    let mut recoveries = Vec::new();
    let mut retired = Vec::new();
    for disposition in dispositions {
        match disposition {
            DormantRecoveryDisposition::Reconstruct(recovery) => recoveries.push(*recovery),
            DormantRecoveryDisposition::Retire { entry, reason } => {
                retired.push((*entry, reason));
            }
        }
    }
    let dormant_names = recoveries
        .iter()
        .map(|recovery| recovery.entry.agent.clone())
        .collect::<std::collections::HashSet<_>>();
    for recovery in &recoveries {
        verify_dormant_worktree(config, wt_mgr, recovery).await?;
    }
    for recovery in &recoveries {
        install_recovered_rework_claim(&config.db_path, recovery).await?;
    }
    {
        let path = db_path.clone();
        recoveries = tokio::task::spawn_blocking(move || -> Result<Vec<DormantRecovery>> {
            let mut conn = quorum_core::db::open(&path)?;
            normalize_interrupted_rework_journals(&mut conn, &mut recoveries)?;
            Ok(recoveries)
        })
        .await
        .map_err(|error| {
            QuorumError::Io(format!(
                "interrupted rework journal normalization join failed: {error}"
            ))
        })??;
    }

    // ── Phase 1b: Revoke run capabilities for stale agents (#130) ──────
    if !entries.is_empty() {
        let p = db_path.clone();
        let agents: Vec<String> = entries
            .iter()
            .filter(|entry| !dormant_names.contains(&entry.agent))
            .map(|entry| entry.agent.clone())
            .collect();
        let revoked = tokio::task::spawn_blocking(move || -> usize {
            let mut total = 0;
            if let Ok(mut conn) = quorum_core::db::open(&p) {
                let now = crate::serve::now_unix();
                for agent in &agents {
                    if let Ok(n) =
                        quorum_core::capabilities::revoke_all_for_agent(&mut conn, agent, now)
                    {
                        total += n;
                    }
                }
            }
            total
        })
        .await
        .unwrap_or(0);
        if revoked > 0 {
            log(&format!(
                "recovery: revoked {revoked} stale run capability(ies)"
            ));
        }
    }

    // ── Phase 2: Delete stale journal entries, retaining dormant rows ───
    {
        let p = db_path.clone();
        let stale_agents = entries
            .iter()
            .filter(|entry| !dormant_names.contains(&entry.agent))
            .map(|entry| entry.agent.clone())
            .collect::<Vec<_>>();
        let count = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut conn = quorum_core::db::open(&p)?;
            let mut count = 0;
            for agent in stale_agents {
                count += usize::from(journal::delete(&mut conn, &agent)?);
            }
            Ok(count)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
        .unwrap_or(0);
        if count > 0 {
            log(&format!("recovery: deleted {count} stale journal entries"));
        }
    }

    for (entry, reason) in &retired {
        name_pool.release(&entry.agent);
        log(&format!(
            "recovery: retired dormant worker {} after {reason}",
            entry.agent
        ));
    }

    reconstruct_dormant_slots(&recoveries, workers, lifetime_roster)?;

    // ── Phase 3: GC all non-recovered worktrees ─────────────────────────
    let active_worktrees = recoveries
        .iter()
        .filter_map(|recovery| recovery.entry.worktree.as_deref())
        .collect::<Vec<_>>();
    let removed = wt_mgr
        .gc_orphaned(&config.repo_dir, &config.worktree_base, &active_worktrees)
        .await;
    if !removed.is_empty() {
        log(&format!("recovery: GC'd {} worktree(s)", removed.len()));
    }

    // ── Phase 4: Reset non-terminal tasks to tick-loop-handleable states ─
    // working/rework → AgentFailed → open (Phase 6 re-claims and spawns a fresh worker)
    // merging → AgentFailed → in-review (Phase 5 spawns a reviewer)
    // in-review → left as-is (Phase 5 spawns a reviewer)
    //
    // #130: ordinary working/rework tasks have no passive exemption. Exact
    // validated dormant rework continuations are active recovered authority
    // and must survive until the startup coordinator resumes their turn.
    let recovered_rework_tasks = recoveries
        .iter()
        .filter(|recovery| recovery.rework_feedback.is_some())
        .map(|recovery| recovery.task_id)
        .collect::<std::collections::HashSet<_>>();
    for status in &["working", "rework"] {
        let p = db_path.clone();
        let s = status.to_string();
        let tasks_in_state = tokio::task::spawn_blocking(move || -> Result<Vec<tasks::Task>> {
            let conn = quorum_core::db::open(&p)?;
            tasks::list(&conn, Some(&s), None, None)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
        .unwrap_or_default();

        for task in tasks_in_state {
            if recovered_rework_tasks.contains(&task.id) {
                log(&format!(
                    "recovery: preserving exact dormant rework continuation for task #{}",
                    task.id
                ));
                continue;
            }
            let retry_queued = task
                .refs
                .as_deref()
                .and_then(|refs| serde_json::from_str::<serde_json::Value>(refs).ok())
                .map(|refs| {
                    quorum_core::runner_state::provider_block(&refs).is_some()
                        || refs
                            .get(tasks::PARKED_REWORK_RETRY_REF)
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false)
                        || refs
                            .get(tasks::CI_REMEDIATION_REQUESTED_REF)
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false)
                })
                .unwrap_or(false);
            if retry_queued {
                if task
                    .refs
                    .as_deref()
                    .and_then(|refs| serde_json::from_str::<serde_json::Value>(refs).ok())
                    .and_then(|refs| {
                        refs.get(tasks::CI_REMEDIATION_REQUESTED_REF)
                            .and_then(serde_json::Value::as_bool)
                    })
                    == Some(true)
                {
                    let p = db_path.clone();
                    let tid = task.id;
                    let preserved = tokio::task::spawn_blocking(move || -> Result<bool> {
                        let mut conn = quorum_core::db::open(&p)?;
                        tasks::reset_ci_remediation_for_recovery(
                            &mut conn,
                            tid,
                            crate::serve::now_unix(),
                        )
                    })
                    .await
                    .map_err(|error| {
                        QuorumError::Io(format!(
                            "CI remediation recovery join for task #{tid}: {error}"
                        ))
                    })??;
                    if preserved {
                        log(&format!(
                            "recovery: preserving exact CI remediation for task #{} in rework",
                            task.id
                        ));
                    }
                }
                log(&format!(
                    "recovery: leaving explicitly retried task #{} in {} for worker claim",
                    task.id, task.status
                ));
                continue;
            }
            let p = db_path.clone();
            let tid = task.id;
            let st = *status;
            tokio::task::spawn_blocking(move || {
                if let Ok(mut conn) = quorum_core::db::open(&p) {
                    let now = crate::serve::now_unix();
                    let event = Event::AgentFailed {
                        reason: format!("daemon restart recovery ({st} task)"),
                    };
                    match tasks::apply_event(&mut conn, "daemon", tid, &event, now) {
                        Ok(tr) => {
                            log(&format!(
                                "recovery: {st} task #{tid} -> {} via AgentFailed",
                                tr.task.status,
                            ));
                        }
                        Err(e) => {
                            log(&format!(
                                "recovery: AgentFailed failed for {st} task #{tid}: {e}",
                            ));
                        }
                    }
                }
            })
            .await
            .ok();
        }
    }

    // merging → AgentFailed → in-review (skip tasks with durable approvals
    // — those are in merge-wait and should stay in merging for retry)
    {
        let p = db_path.clone();
        let merging_tasks = tokio::task::spawn_blocking(move || -> Result<Vec<tasks::Task>> {
            let conn = quorum_core::db::open(&p)?;
            tasks::list(&conn, Some("merging"), None, None)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
        .unwrap_or_default();

        for task in merging_tasks {
            let p = db_path.clone();
            let tid = task.id;

            // An owner retry is durable daemon work, even when its retained
            // role evidence is incomplete or stale. Approval recovery defers
            // every marked task, and the live retry reconciler must consume
            // `requested` exactly once so it can validate that evidence and
            // return to the first missing role. Demoting here would strand the
            // marker on an in-review task that neither reconciler can claim.
            let merge_retry_requested = task
                .refs
                .as_deref()
                .and_then(|refs| serde_json::from_str::<serde_json::Value>(refs).ok())
                .and_then(|refs| {
                    refs.get(tasks::MERGE_RETRY_REF)
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .as_deref()
                == Some(tasks::MERGE_RETRY_REQUESTED);
            if merge_retry_requested {
                log(&format!(
                    "recovery: preserving owner-requested merge replay for task #{tid}"
                ));
                continue;
            }

            // #191: stay in merging only when ALL required review roles are
            // approved for the same SHA (genuine merge-wait). Incomplete
            // approval (e.g. R1 only with R2 still required) resets to
            // in-review so the tick loop provisions the first missing role.
            //
            // "Required" must honour a durable sampled R2 skip. `dual_approved`
            // hard-codes "R1 and R2 both approved", so a sampled-out head — which
            // never gets an `r2` approval row — read as incomplete, bounced the
            // task back to in-review, and the tick loop then had nothing to
            // provision and parked it. Evaluate against R1's approved head so a
            // required R2 still gates exactly as before.
            let pr_number = tasks::extract_pr_number(&task.refs);
            let fully_approved = if let Some(pr) = pr_number {
                let p2 = db_path.clone();
                tokio::task::spawn_blocking(move || -> bool {
                    let Ok(conn) = quorum_core::db::open(&p2) else {
                        return false;
                    };
                    let Ok(Some(r1)) = approvals::get(&conn, pr, "r1") else {
                        return false;
                    };
                    if r1.approved_head_sha.is_empty() {
                        return false;
                    }
                    super::all_required_roles_approved(&conn, pr, &r1.approved_head_sha)
                        .unwrap_or(false)
                })
                .await
                .unwrap_or(false)
            } else {
                false
            };
            if fully_approved {
                log(&format!(
                    "recovery: merging task #{tid} fully approved \
                     — preserving merge-wait state"
                ));
                continue;
            }

            tokio::task::spawn_blocking(move || {
                if let Ok(mut conn) = quorum_core::db::open(&p) {
                    let now = crate::serve::now_unix();
                    let event = Event::AgentFailed {
                        reason: "daemon restart recovery (merging task)".into(),
                    };
                    match tasks::apply_event(&mut conn, "daemon", tid, &event, now) {
                        Ok(tr) => {
                            log(&format!(
                                "recovery: merging task #{tid} -> {} via AgentFailed",
                                tr.task.status,
                            ));
                        }
                        Err(e) => {
                            log(&format!(
                                "recovery: AgentFailed failed for merging task #{tid}: {e}",
                            ));
                        }
                    }
                }
            })
            .await
            .ok();
        }
    }

    log("recovery: complete");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct StartupRetryableMerge {
        calls: AtomicUsize,
    }

    impl super::super::merge::MergeExecutor for StartupRetryableMerge {
        fn merge(
            &self,
            _pr: i64,
            _repo_dir: &Path,
            _ctx: &super::super::merge::MergeContext,
        ) -> super::super::merge::MergeResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            super::super::merge::MergeResult {
                success: false,
                message: "head branch must be updated".into(),
                failure_kind: Some(super::super::merge::MergeFailureKind::Retryable),
            }
        }

        fn head_sha(&self, _pr: i64, _repo_dir: &Path) -> Option<String> {
            Some("approved-head".into())
        }
    }

    fn run_git(dir: &Path, args: &[&str]) {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn dormant_test_config(
        db_path: PathBuf,
        repo_dir: PathBuf,
        worktree_base: PathBuf,
    ) -> ServeConfig {
        ServeConfig {
            db_path,
            cap: 1,
            model_profiles: std::collections::BTreeMap::new(),
            routing: crate::serve_config::RoutingPolicy {
                classifier: std::collections::BTreeMap::new(),
                planner: std::collections::BTreeMap::new(),
                arbiter: std::collections::BTreeMap::new(),
                collector: std::collections::BTreeMap::new(),
                worker: std::collections::BTreeMap::new(),
                reviewer: std::collections::BTreeMap::new(),
            },
            repo_dir,
            worktree_base,
            names_file: None,
            agent_bin: None,
            merge_executor: std::sync::Arc::new(super::super::merge::CommandMergeExecutor {
                command: "true".into(),
                checks_cmd: None,
                mergeability_cmd: None,
            }),
            bare_agent: true,
            limits: super::super::CostLimits::default(),
            log_dir: None,
            self_update_drain: false,
            drain_timeout_secs: 1,
            self_repo: None,
            sha_poll_interval_secs: 60,
            merge_checks_timeout_secs: 1,
            merge_checks_poll_secs: 1,
            repo: "owner/repo".into(),
            base_branch: "main".into(),
            self_update_branch: "main".into(),
            exit_when_gone: None,
            required_jobs: Vec::new(),
            master_ci_gate: false,
            master_ci_timeout_secs: 1,
            allowed_tools: None,
            doctor_enabled: false,
            r2_enabled: false,
            r2_target_per_stratum: 0,
            r2_steady_state_p: 0.0,
            max_rework: quorum_core::lifecycle::REWORK_CAP,
            codex_sandbox: "danger-full-access".into(),
            grok: Default::default(),
            pr_target_program: None,
        }
    }

    struct DormantFixture {
        _dir: tempfile::TempDir,
        config: ServeConfig,
        task_id: i64,
        worktree: PathBuf,
    }

    fn dormant_fixture() -> DormantFixture {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let worktree_base = dir.path().join("worktrees");
        let worktree = worktree_base.join("Dormant-t1");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::create_dir_all(&worktree_base).unwrap();
        run_git(&repo, &["init", "-b", "main"]);
        run_git(&repo, &["config", "user.email", "test@example.com"]);
        run_git(&repo, &["config", "user.name", "Test"]);
        run_git(&repo, &["commit", "--allow-empty", "-m", "init"]);
        run_git(&repo, &["branch", "daemon/dormant-t1"]);
        run_git(
            &repo,
            &[
                "worktree",
                "add",
                worktree.to_str().unwrap(),
                "daemon/dormant-t1",
            ],
        );

        let db_path = dir.path().join("quorum.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let now = super::super::now_unix();
        let task_id = tasks::create(
            &mut conn,
            "owner",
            "dormant",
            None,
            0,
            None,
            Some(
                r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#,
            ),
            None,
            None,
            now,
        )
        .unwrap();
        tasks::claim(&mut conn, "Dormant", Some(task_id), &[], 3600, now)
            .unwrap()
            .unwrap();
        tasks::apply_event(
            &mut conn,
            "Dormant",
            task_id,
            &Event::SignaledDone { pr: "901".into() },
            now + 1,
        )
        .unwrap();
        let task = tasks::get(&conn, task_id).unwrap().unwrap();
        let mut refs: serde_json::Value =
            serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        runner_state::set_continuation(
            &mut refs,
            ContinuationSlot::Worker,
            &runner_state::ContinuationIdentity {
                provider: "codex".into(),
                id: "thread-dormant".into(),
            },
        );
        tasks::update_refs_daemon(&mut conn, task_id, &refs.to_string(), now + 2).unwrap();
        conn.execute(
            "INSERT INTO task_branches(task_id,branch,worktree,allocated_by,allocated_at)
             VALUES (?1,'daemon/dormant-t1',?2,'Dormant',?3)",
            rusqlite::params![task_id, worktree.to_string_lossy(), now],
        )
        .unwrap();
        let run_id = quorum_core::agent_runs::insert(
            &conn,
            task_id,
            "Dormant",
            "worker",
            "gpt-5.6-terra",
            "medium",
            "codex",
            now,
        )
        .unwrap();
        assert!(run_id > 0);
        quorum_core::token_usage::record(
            &mut conn,
            Some(run_id),
            "worker",
            &[task_id],
            Some(901),
            "codex",
            "gpt-5.6-terra",
            "medium",
            quorum_core::token_usage::TokenUsage {
                uncached_input_tokens: 80,
                cached_input_tokens: 900,
                cache_write_input_tokens: 10,
                output_tokens: 20,
                reasoning_tokens: 5,
            },
            now + 2,
        )
        .unwrap();
        quorum_core::capabilities::issue(
            &mut conn,
            "cap-dormant",
            task_id,
            "Dormant",
            "worker",
            now,
        )
        .unwrap();
        journal::upsert(
            &mut conn,
            &JournalEntry {
                agent: "Dormant".into(),
                role: "worker".into(),
                task_id: Some(task_id),
                session_id: "session-dormant".into(),
                worktree: Some(worktree.to_string_lossy().into_owned()),
                branch: Some("daemon/dormant-t1".into()),
                phase: "awaiting-review".into(),
                cost_tokens: 123,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: Some(901),
                rework_count: 0,
                provider: Some("codex".into()),
                continuation_id: Some("thread-dormant".into()),
                local_branch: Some("daemon/dormant-t1".into()),
            },
        )
        .unwrap();
        drop(conn);

        DormantFixture {
            _dir: dir,
            config: dormant_test_config(db_path, repo, worktree_base),
            task_id,
            worktree,
        }
    }

    #[tokio::test]
    async fn restart_reconstructs_dormant_identity_and_is_idempotent() {
        let fixture = dormant_fixture();
        let wt_mgr = WorktreeManager::new();
        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();

        recover(
            &fixture.config,
            &wt_mgr,
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.agent_name, "Dormant");
        assert_eq!(worker.task_id, fixture.task_id);
        assert_eq!(worker.pr, Some(901));
        assert_eq!(worker.worktree_path, fixture.worktree);
        assert_eq!(worker.continuation_id_for_launch(), Some("thread-dormant"));
        assert!(matches!(worker.proc, SlotProcess::Dormant { .. }));
        assert_eq!(
            worker.token_usage,
            super::super::runner::TokenUsage {
                input_tokens: 0,
                uncached_input_tokens: 80,
                cached_input_tokens: 900,
                cache_write_input_tokens: 10,
                output_tokens: 20,
                reasoning_tokens: 5,
            },
            "dormant recovery must restore the cumulative detailed snapshot"
        );
        assert!(roster.owns("Dormant"));
        assert!(names.acquire_named("Dormant").is_none());

        recover(
            &fixture.config,
            &wt_mgr,
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();
        assert_eq!(
            workers.len(),
            1,
            "repeated restart must not duplicate the slot"
        );
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        for (table, predicate) in [
            ("journal", "agent='Dormant'"),
            ("claims", "target='task#1' AND active=1"),
            ("agent_runs", "task_id=1 AND role='worker'"),
            ("run_capabilities", "task_id=1 AND role='worker'"),
        ] {
            let count: i64 = conn
                .query_row(
                    &format!("SELECT count(*) FROM {table} WHERE {predicate}"),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "recovery duplicated {table}");
        }
    }

    #[tokio::test]
    async fn restart_reconstructs_grok_worker_with_exact_terminal_session() {
        let fixture = dormant_fixture();
        {
            let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            let task = tasks::get(&conn, fixture.task_id).unwrap().unwrap();
            let mut refs: serde_json::Value =
                serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
            runner_state::set_continuation(
                &mut refs,
                ContinuationSlot::Worker,
                &runner_state::ContinuationIdentity {
                    provider: "grok".into(),
                    id: "grok-terminal-session".into(),
                },
            );
            tasks::update_refs_daemon(
                &mut conn,
                fixture.task_id,
                &refs.to_string(),
                super::super::now_unix(),
            )
            .unwrap();
            conn.execute(
                "UPDATE agent_runs SET model='grok-4.5',effort='high',provider='grok' \
                 WHERE task_id=?1 AND role='worker'",
                [fixture.task_id],
            )
            .unwrap();
            conn.execute(
                "UPDATE journal SET provider='grok',continuation_id='grok-terminal-session' \
                 WHERE task_id=?1 AND role='worker'",
                [fixture.task_id],
            )
            .unwrap();
        }

        let wt_mgr = WorktreeManager::new();
        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        recover(
            &fixture.config,
            &wt_mgr,
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();

        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.process_kind(), super::super::runner::AgentKind::Grok);
        assert_eq!(
            worker.continuation_id_for_launch(),
            Some("grok-terminal-session")
        );
        assert_eq!(worker.model, "grok-4.5");
        assert_eq!(worker.effort, "high");
    }

    #[tokio::test]
    async fn restart_preserves_requested_merge_retry_with_missing_role_evidence() {
        for retain_r1 in [false, true] {
            let fixture = dormant_fixture();
            let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            let now = super::super::now_unix();
            let retry_task = tasks::create(
                &mut conn,
                "owner",
                "requested merge retry",
                None,
                0,
                None,
                Some(r#"{"pr":902,"daemon_merge_retry":"requested"}"#),
                None,
                None,
                now,
            )
            .unwrap();
            conn.execute(
                "UPDATE tasks
                 SET status='merging', author='Worker-Retry', target_branch='main'
                 WHERE id=?1",
                [retry_task],
            )
            .unwrap();
            if retain_r1 {
                approvals::record(
                    &mut conn,
                    &approvals::Approval {
                        pr_number: 902,
                        review_role: "r1".into(),
                        task_id: retry_task,
                        author: "Worker-Retry".into(),
                        reviewer: "Reviewer-R1".into(),
                        verdict: "approved".into(),
                        blocking_count: 0,
                        approved_head_sha: "head-a".into(),
                    },
                )
                .unwrap();
            }
            drop(conn);

            let mut names = super::super::names::Pool::new_generated();
            let mut workers = Vec::new();
            let mut roster = LifetimeRoster::new();
            let wt_mgr = WorktreeManager::new();
            recover(
                &fixture.config,
                &wt_mgr,
                &mut names,
                &mut workers,
                &mut roster,
            )
            .await
            .unwrap();

            let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            let task = tasks::get(&conn, retry_task).unwrap().unwrap();
            assert_eq!(task.status, "merging");
            let refs: serde_json::Value =
                serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
            assert_eq!(
                refs[tasks::MERGE_RETRY_REF],
                tasks::MERGE_RETRY_REQUESTED,
                "restart must not strand requested authority on an in-review task"
            );
            let failed_events: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM events
                     WHERE subject=?1 AND kind='agent_failed'",
                    [tasks::lease_target(retry_task)],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(failed_events, 0);
            let claimed = tasks::claim_merge_retry(&mut conn, now + 1)
                .unwrap()
                .expect("the live reconciler must still be able to consume the retry");
            assert_eq!(claimed.id, retry_task);
        }
    }

    #[tokio::test]
    async fn restart_rebuilds_task_token_ceiling_in_the_current_basis() {
        for (basis, prior_journal_total, expected_total, max, breaches) in [
            (
                crate::serve_config::TokenLimitBasis::Uncached,
                6_590_100,
                281_761,
                500_000,
                false,
            ),
            (
                crate::serve_config::TokenLimitBasis::Raw,
                281_761,
                6_590_100,
                500_000,
                true,
            ),
        ] {
            let mut fixture = dormant_fixture();
            fixture.config.limits.token_limit_basis = basis;
            fixture.config.limits.max_task_tokens = Some(max);
            let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            let run_id: i64 = conn
                .query_row(
                    "SELECT id FROM agent_runs WHERE task_id=?1 AND ended_at IS NULL",
                    [fixture.task_id],
                    |row| row.get(0),
                )
                .unwrap();
            quorum_core::token_usage::record(
                &mut conn,
                Some(run_id),
                "worker",
                &[fixture.task_id],
                Some(901),
                "codex",
                "gpt-5.6-terra",
                "medium",
                quorum_core::token_usage::TokenUsage {
                    uncached_input_tokens: 281_661,
                    cached_input_tokens: 6_308_339,
                    cache_write_input_tokens: 40,
                    output_tokens: 100,
                    reasoning_tokens: 10,
                },
                super::super::now_unix(),
            )
            .unwrap();
            conn.execute(
                "UPDATE journal SET cost_tokens=?1 WHERE agent='Dormant'",
                [prior_journal_total],
            )
            .unwrap();
            drop(conn);

            let mut names = super::super::names::Pool::new_generated();
            let mut workers = Vec::new();
            recover(
                &fixture.config,
                &WorktreeManager::new(),
                &mut names,
                &mut workers,
                &mut LifetimeRoster::new(),
            )
            .await
            .unwrap();
            let worker = workers.first().expect("recovered dormant worker");
            assert_eq!(worker.limit_tokens, expected_total);
            assert_eq!(worker.cost_tokens, prior_journal_total);
            let breach = super::super::check_post_result_limits(
                &fixture.config.limits,
                0,
                worker.limit_tokens,
                Some(0.0),
                0.0,
                worker,
            );
            assert_eq!(breach.is_some(), breaches, "basis {basis}");
        }
    }

    #[tokio::test]
    async fn rejected_overflow_snapshot_does_not_poison_dormant_recovery() {
        let fixture = dormant_fixture();
        let run_id = {
            let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            let run_id: i64 = conn
                .query_row(
                    "SELECT id FROM agent_runs WHERE task_id=?1 AND ended_at IS NULL",
                    [fixture.task_id],
                    |row| row.get(0),
                )
                .unwrap();
            conn.execute(
                "DELETE FROM token_usage_run_tasks WHERE run_id IN
                    (SELECT id FROM token_usage_runs WHERE agent_run_id=?1)",
                [run_id],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM token_usage_runs WHERE agent_run_id=?1",
                [run_id],
            )
            .unwrap();
            run_id
        };

        super::super::record_managed_usage_snapshot(
            &fixture.config.db_path,
            Some(run_id),
            super::super::ManagedUsageRecord {
                task_id: fixture.task_id,
                purpose: "worker".into(),
                pr_number: Some(901),
                provider: "codex".into(),
                model: "gpt-5.6-terra".into(),
                effort: "medium".into(),
                usage: super::super::runner::TokenUsage {
                    cached_input_tokens: u64::MAX,
                    ..Default::default()
                },
            },
        )
        .await;

        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        assert!(
            quorum_core::token_usage::usage_for_agent_run(&conn, run_id)
                .unwrap()
                .is_none(),
            "overflow must not leave a negative durable snapshot"
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM errors WHERE source='token_usage'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            1,
            "the ignored overflow must remain observable"
        );
        drop(conn);

        let wt_mgr = WorktreeManager::new();
        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        recover(
            &fixture.config,
            &wt_mgr,
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .expect("best-effort telemetry overflow must not make recovery fatal");
        assert_eq!(workers.len(), 1);
        assert_eq!(
            workers[0].token_usage,
            super::super::runner::TokenUsage::default(),
            "a rejected snapshot recovers as empty usage, never wrapped negative usage"
        );
    }

    fn assert_recovered_sticky_rework(
        fixture: &DormantFixture,
        workers: &[SlotState],
        expected_feedback: &str,
    ) {
        assert_eq!(workers.len(), 1);
        let worker = &workers[0];
        assert_eq!(worker.agent_name, "Dormant");
        assert_eq!(worker.pending_turn_kind, "recovered-rework");
        assert_eq!(worker.pending_prompt, expected_feedback);
        assert!(matches!(worker.proc, SlotProcess::Dormant { .. }));
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let task = tasks::get(&conn, fixture.task_id).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.assignee.as_deref(), Some("Dormant"));
        let holders: Vec<String> = conn
            .prepare(
                "SELECT holder FROM claims
                 WHERE target=?1 AND active=1 AND expires_at>?2",
            )
            .unwrap()
            .query_map(
                rusqlite::params![
                    tasks::lease_target(fixture.task_id),
                    super::super::now_unix()
                ],
                |row| row.get(0),
            )
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(holders, vec!["Dormant".to_string()]);
    }

    fn enter_merge_conflict_rework(
        conn: &mut quorum_core::Connection,
        task_id: i64,
        feedback: &str,
    ) {
        let now = super::super::now_unix();
        tasks::apply_event(
            conn,
            "Reviewer",
            task_id,
            &Event::ReviewerAttached {
                agent: "Reviewer".into(),
            },
            now + 1,
        )
        .unwrap();
        tasks::apply_event(conn, "Reviewer", task_id, &Event::VerdictApprove, now + 2).unwrap();
        tasks::apply_actionable_rework_event(
            conn,
            "system",
            task_id,
            &Event::MergeConflict,
            feedback,
            now + 3,
        )
        .unwrap();
    }

    fn add_dormant_worker(
        fixture: &DormantFixture,
        agent: &str,
        branch: &str,
        pr: i64,
        continuation: &str,
    ) -> i64 {
        let worktree = fixture.config.worktree_base.join(format!("{agent}-t2"));
        run_git(&fixture.config.repo_dir, &["branch", branch]);
        run_git(
            &fixture.config.repo_dir,
            &["worktree", "add", worktree.to_str().unwrap(), branch],
        );
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let now = super::super::now_unix();
        let task_id = tasks::create(
            &mut conn,
            "owner",
            "second dormant worker",
            None,
            0,
            None,
            Some(
                r#"{"cx_est":3,"cx_size":"M","cx_ready":true,"cx_not_ready_reason":null,"cx_by":"test:v2"}"#,
            ),
            None,
            None,
            now,
        )
        .unwrap();
        tasks::claim(&mut conn, agent, Some(task_id), &[], 3600, now)
            .unwrap()
            .unwrap();
        tasks::apply_event(
            &mut conn,
            agent,
            task_id,
            &Event::SignaledDone { pr: pr.to_string() },
            now + 1,
        )
        .unwrap();
        let task = tasks::get(&conn, task_id).unwrap().unwrap();
        let mut refs: serde_json::Value =
            serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        runner_state::set_continuation(
            &mut refs,
            ContinuationSlot::Worker,
            &runner_state::ContinuationIdentity {
                provider: "codex".into(),
                id: continuation.into(),
            },
        );
        tasks::update_refs_daemon(&mut conn, task_id, &refs.to_string(), now + 2).unwrap();
        conn.execute(
            "INSERT INTO task_branches(task_id,branch,worktree,allocated_by,allocated_at)
             VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![task_id, branch, worktree.to_string_lossy(), agent, now],
        )
        .unwrap();
        quorum_core::agent_runs::insert(
            &conn,
            task_id,
            agent,
            "worker",
            "gpt-5.6-terra",
            "medium",
            "codex",
            now,
        )
        .unwrap();
        quorum_core::capabilities::issue(
            &mut conn,
            &format!("cap-{agent}"),
            task_id,
            agent,
            "worker",
            now,
        )
        .unwrap();
        journal::upsert(
            &mut conn,
            &JournalEntry {
                agent: agent.into(),
                role: "worker".into(),
                task_id: Some(task_id),
                session_id: format!("session-{agent}"),
                worktree: Some(worktree.to_string_lossy().into_owned()),
                branch: Some(branch.into()),
                phase: "awaiting-review".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: Some(pr),
                rework_count: 0,
                provider: Some("codex".into()),
                continuation_id: Some(continuation.into()),
                local_branch: Some(branch.into()),
            },
        )
        .unwrap();
        task_id
    }

    #[cfg(unix)]
    fn process_is_alive(pid: i32) -> bool {
        unsafe { libc::kill(pid, 0) == 0 }
    }

    #[cfg(unix)]
    fn reaped_pid_from_error(error: &str) -> i32 {
        error
            .split("launched pid ")
            .nth(1)
            .and_then(|tail| tail.split_whitespace().next())
            .and_then(|pid| pid.parse().ok())
            .expect("journal handoff error records the synchronously reaped PID")
    }

    #[cfg(unix)]
    fn configure_recovered_journal_handoff_failure(fixture: &mut DormantFixture, codex_body: &str) {
        let head = std::process::Command::new("git")
            .arg("-C")
            .arg(&fixture.config.repo_dir)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert!(head.status.success());
        let head = String::from_utf8(head.stdout).unwrap().trim().to_string();
        {
            let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            enter_merge_conflict_rework(
                &mut conn,
                fixture.task_id,
                "retry the exact turn after journal handoff failure",
            );
            conn.execute_batch(
                "CREATE TRIGGER reject_recovered_journal_handoff
                   BEFORE UPDATE ON journal
                   WHEN NEW.phase='resuming-rework'
                 BEGIN
                   SELECT RAISE(ABORT, 'injected recovered journal failure');
                 END;",
            )
            .unwrap();
        }
        let codex = fixture._dir.path().join("fake-codex");
        write_executable(&codex, codex_body);
        let gh = fixture._dir.path().join("fake-gh");
        write_executable(
            &gh,
            &format!(
                "#!/bin/sh\nprintf '%s\\n' '{{\"headRefName\":\"daemon/dormant-t1\",\"headRefOid\":\"{head}\",\"isCrossRepository\":false,\"baseRefName\":\"main\",\"state\":\"OPEN\"}}'\n"
            ),
        );
        fixture.config.agent_bin = Some(codex.to_string_lossy().into_owned());
        fixture.config.pr_target_program = Some(gh);
    }

    fn begin_unrelated_decomposition_freeze(
        conn: &mut quorum_core::Connection,
        frozen_base_sha: &str,
    ) {
        let source = tasks::create(
            conn,
            "owner",
            "unrelated decomposition source",
            None,
            1,
            None,
            None,
            None,
            None,
            super::super::now_unix(),
        )
        .unwrap();
        quorum_core::decomposition::begin_planning(
            conn,
            &quorum_core::decomposition::BeginPlanning {
                source_task_id: source,
                expected_revision: 1,
                provider: "claude",
                model: "planner",
                frozen_base_sha,
                now: super::super::now_unix() + 1,
            },
        )
        .unwrap()
        .expect("unrelated planning source acquires the freeze");
    }

    #[cfg(unix)]
    fn write_executable(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn frozen_restart_resumes_sticky_rework_before_and_after_replacement_lease() {
        let mut retained_fixtures = Vec::new();
        for lease_installed in [false, true] {
            let mut fixture = dormant_fixture();
            let feedback = "resume the exact frozen restart turn";
            let head = std::process::Command::new("git")
                .arg("-C")
                .arg(&fixture.config.repo_dir)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap();
            assert!(head.status.success());
            let head = String::from_utf8(head.stdout).unwrap().trim().to_string();
            {
                let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
                enter_merge_conflict_rework(&mut conn, fixture.task_id, feedback);
                if lease_installed {
                    tasks::claim_remediation_rework_with_feedback(
                        &mut conn,
                        "Dormant",
                        fixture.task_id,
                        tasks::DEFAULT_LEASE_TTL_SECS,
                        super::super::now_unix(),
                        Some(feedback),
                    )
                    .unwrap()
                    .expect("replacement lease is installed before the crash");
                }
                begin_unrelated_decomposition_freeze(&mut conn, &head);
            }

            let args_path = fixture._dir.path().join(format!(
                "codex-args-{}.txt",
                if lease_installed { "after" } else { "before" }
            ));
            let codex = fixture._dir.path().join("fake-codex");
            write_executable(
                &codex,
                &format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nexec sleep 30\n",
                    args_path.display(),
                ),
            );
            let gh = fixture._dir.path().join("fake-gh");
            write_executable(
                &gh,
                &format!(
                    "#!/bin/sh\nprintf '%s\\n' '{{\"headRefName\":\"daemon/dormant-t1\",\"headRefOid\":\"{head}\",\"isCrossRepository\":false,\"baseRefName\":\"main\",\"state\":\"OPEN\"}}'\n"
                ),
            );
            fixture.config.agent_bin = Some(codex.to_string_lossy().into_owned());
            fixture.config.pr_target_program = Some(gh);
            fixture.config.exit_when_gone =
                Some(fixture._dir.path().join("absent-daemon-sentinel"));

            let daemon_pid = std::process::id() as i64;
            let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            assert_eq!(
                quorum_core::daemon_lock::try_acquire(
                    &mut conn,
                    daemon_pid,
                    super::super::now_unix(),
                    30,
                    |_| false,
                )
                .unwrap(),
                quorum_core::daemon_lock::AcquireResult::Acquired,
            );
            drop(conn);

            let exit = super::super::tick_loop(&fixture.config, daemon_pid)
                .await
                .expect("frozen restart must converge through the startup coordinator");
            assert_eq!(exit, 1, "missing sentinel terminates the test daemon");

            let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            let freeze_active: bool = conn
                .query_row("SELECT freeze_active FROM task_decompositions", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert!(
                freeze_active,
                "the existing worker continuation runs without stealing planning authority"
            );
            let task = tasks::get(&conn, fixture.task_id).unwrap().unwrap();
            assert_eq!(task.status, "rework");
            assert_eq!(task.assignee.as_deref(), Some("Dormant"));
            assert_eq!(
                journal::list_in_flight(&conn).unwrap()[0].phase,
                "working",
                "startup must advance the exact retained journal row"
            );
            drop(conn);
            retained_fixtures.push(fixture);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recovered_rework_disposition_error_exits_tick_loop_once() {
        let mut fixture = dormant_fixture();
        let head = std::process::Command::new("git")
            .arg("-C")
            .arg(&fixture.config.repo_dir)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert!(head.status.success());
        let head = String::from_utf8(head.stdout).unwrap().trim().to_string();
        {
            let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            enter_merge_conflict_rework(
                &mut conn,
                fixture.task_id,
                "resume the exact pending turn",
            );
            conn.execute(
                "UPDATE agent_runs SET effort='' WHERE task_id=?1 AND role='worker'",
                [fixture.task_id],
            )
            .unwrap();
        }

        let gh_calls = fixture._dir.path().join("gh-calls");
        let gh = fixture._dir.path().join("fake-gh");
        write_executable(
            &gh,
            &format!(
                "#!/bin/sh\nprintf x >> '{}'\nprintf '%s\\n' '{{\"headRefName\":\"daemon/dormant-t1\",\"headRefOid\":\"{head}\",\"isCrossRepository\":false,\"baseRefName\":\"main\",\"state\":\"OPEN\"}}'\n",
                gh_calls.display(),
            ),
        );
        fixture.config.pr_target_program = Some(gh);

        let daemon_pid = std::process::id() as i64;
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        quorum_core::daemon_lock::try_acquire(
            &mut conn,
            daemon_pid,
            super::super::now_unix(),
            30,
            |_| false,
        )
        .unwrap();
        drop(conn);

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            super::super::tick_loop(&fixture.config, daemon_pid),
        )
        .await
        .expect("persistent startup disposition error must not retry")
        .expect_err("persistent startup disposition error must exit abnormally");
        assert_eq!(error.exit_code(), 3);
        assert!(
            error.to_string().contains("complete pending turn"),
            "{error}"
        );
        assert_eq!(
            std::fs::read_to_string(&gh_calls).unwrap(),
            "x",
            "startup must perform the external baseline inspection only once"
        );

        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let task = tasks::get(&conn, fixture.task_id).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.assignee.as_deref(), Some("Dormant"));
        assert_eq!(
            journal::list_in_flight(&conn).unwrap()[0].phase,
            "awaiting-review"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn later_recovered_failure_reaps_and_preserves_earlier_exact_turn() {
        let mut fixture = dormant_fixture();
        fixture.config.cap = 2;
        let second_task = add_dormant_worker(
            &fixture,
            "Dormant-Z",
            "daemon/dormant-z-t2",
            902,
            "thread-dormant-z",
        );
        let head = std::process::Command::new("git")
            .arg("-C")
            .arg(&fixture.config.repo_dir)
            .args(["rev-parse", "HEAD"])
            .output()
            .unwrap();
        assert!(head.status.success());
        let head = String::from_utf8(head.stdout).unwrap().trim().to_string();
        {
            let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            enter_merge_conflict_rework(
                &mut conn,
                fixture.task_id,
                "preserve worker A's exact pending turn",
            );
            enter_merge_conflict_rework(
                &mut conn,
                second_task,
                "worker B fails durable disposition",
            );
            conn.execute(
                "UPDATE agent_runs SET effort='' WHERE task_id=?1 AND role='worker'",
                [second_task],
            )
            .unwrap();
        }

        let codex = fixture._dir.path().join("fake-codex");
        write_executable(&codex, "#!/bin/sh\nexec sleep 30\n");
        let gh = fixture._dir.path().join("fake-gh");
        write_executable(
            &gh,
            &format!(
                "#!/bin/sh\ncase \"$*\" in *902*) branch=daemon/dormant-z-t2;; *) branch=daemon/dormant-t1;; esac\nprintf '{{\"headRefName\":\"%s\",\"headRefOid\":\"{head}\",\"isCrossRepository\":false,\"baseRefName\":\"main\",\"state\":\"OPEN\"}}\\n' \"$branch\"\n"
            ),
        );
        fixture.config.agent_bin = Some(codex.to_string_lossy().into_owned());
        fixture.config.pr_target_program = Some(gh);

        let daemon_pid = std::process::id() as i64;
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        quorum_core::daemon_lock::try_acquire(
            &mut conn,
            daemon_pid,
            super::super::now_unix(),
            30,
            |_| false,
        )
        .unwrap();
        drop(conn);

        let error = super::super::tick_loop(&fixture.config, daemon_pid)
            .await
            .expect_err("worker B's durable disposition error must abort startup");
        assert_eq!(error.exit_code(), 3);

        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let staged = journal::list_in_flight(&conn)
            .unwrap()
            .into_iter()
            .find(|entry| entry.agent == "Dormant")
            .unwrap();
        assert_eq!(staged.phase, "resuming-rework");
        let first_pid = staged.pid.expect("worker A staged its launched PID");
        assert!(
            !process_is_alive(first_pid),
            "worker A must be fully reaped"
        );
        drop(conn);

        let mut workers = Vec::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut super::super::names::Pool::new_generated(),
            &mut workers,
            &mut LifetimeRoster::new(),
        )
        .await
        .expect("next restart reconstructs both exact dormant turns");
        let worker_a = workers
            .iter()
            .find(|worker| worker.agent_name == "Dormant")
            .unwrap();
        assert_eq!(
            worker_a.pending_prompt,
            "preserve worker A's exact pending turn"
        );
        assert_eq!(
            worker_a.continuation_id_for_launch(),
            Some("thread-dormant")
        );
        assert!(matches!(worker_a.proc, SlotProcess::Dormant { .. }));
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let normalized = journal::list_in_flight(&conn)
            .unwrap()
            .into_iter()
            .find(|entry| entry.agent == "Dormant")
            .unwrap();
        assert_eq!(normalized.phase, "awaiting-review");
        assert_eq!(normalized.pid, None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn post_launch_journal_failure_is_loud_reaped_and_retryable_once() {
        let mut fixture = dormant_fixture();
        configure_recovered_journal_handoff_failure(
            &mut fixture,
            r#"#!/bin/sh
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":80,"cache_write_input_tokens":10,"output_tokens":5,"reasoning_output_tokens":3}}'
exec sleep 30
"#,
        );

        let daemon_pid = std::process::id() as i64;
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        quorum_core::daemon_lock::try_acquire(
            &mut conn,
            daemon_pid,
            super::super::now_unix(),
            30,
            |_| false,
        )
        .unwrap();
        drop(conn);
        let error = super::super::tick_loop(&fixture.config, daemon_pid)
            .await
            .expect_err("post-launch journal failure must abort startup");
        assert_eq!(error.exit_code(), 3);
        assert!(
            error.to_string().contains("journal handoff failed"),
            "{error}"
        );
        let first_pid = reaped_pid_from_error(&error.to_string());
        assert!(
            !process_is_alive(first_pid),
            "failed launch must be fully reaped"
        );

        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let entry = journal::list_in_flight(&conn).unwrap().remove(0);
        assert_eq!(entry.phase, "awaiting-review");
        assert_eq!(entry.pid, None);
        let live_authority: (i64, i64) = conn
            .query_row(
                "SELECT
                   (SELECT count(*) FROM agent_runs WHERE task_id=?1 AND role='worker' AND ended_at IS NULL),
                   (SELECT count(*) FROM run_capabilities WHERE task_id=?1 AND role='worker' AND revoked_at IS NULL)",
                [fixture.task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            live_authority,
            (0, 1),
            "the reaped provider run is closed while its exact retry capability remains active"
        );
        let failed_run = quorum_core::agent_runs::runs_for_task(&conn, fixture.task_id)
            .unwrap()
            .into_iter()
            .find(|run| run.end_reason.as_deref() == Some("journal-handoff-failed"))
            .expect("the post-insert run is truthfully settled");
        assert_eq!(
            quorum_core::token_usage::usage_for_agent_run(&conn, failed_run.id)
                .unwrap()
                .unwrap(),
            quorum_core::token_usage::TokenUsage {
                uncached_input_tokens: 20,
                cached_input_tokens: 80,
                cache_write_input_tokens: 10,
                output_tokens: 5,
                reasoning_tokens: 3,
            },
            "the failed handoff run retains all terminal token buckets"
        );
        conn.execute_batch("DROP TRIGGER reject_recovered_journal_handoff")
            .unwrap();
        drop(conn);

        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();
        super::super::resume_recovered_dormant_reworks(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
        )
        .await
        .expect("the next restart launches the preserved exact turn once");
        assert_eq!(workers.len(), 1);
        assert!(matches!(workers[0].proc, SlotProcess::Running(_)));
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let authority: (i64, i64) = conn
            .query_row(
                "SELECT
                   (SELECT count(*) FROM agent_runs WHERE task_id=?1 AND role='worker'),
                   (SELECT count(*) FROM agent_runs WHERE task_id=?1 AND role='worker' AND ended_at IS NULL)",
                [fixture.task_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(authority, (3, 1), "restart launches one replacement run");
        drop(conn);
        workers.remove(0).kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn post_launch_journal_failure_bounds_unterminated_stdout_before_reap() {
        let mut fixture = dormant_fixture();
        let chunk = "x".repeat(4096);
        configure_recovered_journal_handoff_failure(
            &mut fixture,
            &format!("#!/bin/sh\nwhile :; do printf '%s' '{chunk}'; done\n"),
        );

        let daemon_pid = std::process::id() as i64;
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        quorum_core::daemon_lock::try_acquire(
            &mut conn,
            daemon_pid,
            super::super::now_unix(),
            30,
            |_| false,
        )
        .unwrap();
        drop(conn);

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            super::super::tick_loop(&fixture.config, daemon_pid),
        )
        .await
        .expect("unterminated provider output must not prevent fatal handoff settlement")
        .expect_err("injected journal handoff failure must remain fatal");
        assert_eq!(error.exit_code(), 3);
        assert!(
            error.to_string().contains("journal handoff failed"),
            "{error}"
        );
        let pid = reaped_pid_from_error(&error.to_string());
        assert!(!process_is_alive(pid), "failed provider must be reaped");

        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let failed_run = quorum_core::agent_runs::runs_for_task(&conn, fixture.task_id)
            .unwrap()
            .into_iter()
            .find(|run| run.end_reason.as_deref() == Some("journal-handoff-failed"))
            .expect("the bounded failure path settles the inserted run");
        assert_eq!(
            quorum_core::token_usage::usage_for_agent_run(&conn, failed_run.id)
                .unwrap()
                .unwrap(),
            quorum_core::token_usage::TokenUsage::default(),
            "malformed non-terminal output cannot fabricate token usage"
        );
        let entry = journal::list_in_flight(&conn).unwrap().remove(0);
        assert_eq!((entry.phase.as_str(), entry.pid), ("awaiting-review", None));
    }

    #[tokio::test]
    async fn restart_recovers_sticky_rework_before_replacement_lease() {
        let fixture = dormant_fixture();
        let feedback = "Preserve the published PR head, merge main into the PR branch, resolve conflicts, commit, and submit without pushing. Never rebase.";
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        enter_merge_conflict_rework(&mut conn, fixture.task_id, feedback);
        drop(conn);

        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();
        assert_recovered_sticky_rework(&fixture, &workers, feedback);

        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();
        assert_recovered_sticky_rework(&fixture, &workers, feedback);
    }

    #[tokio::test]
    async fn restart_recovers_sticky_rework_after_replacement_lease() {
        let fixture = dormant_fixture();
        let feedback = "Preserve the published PR head, merge main into the PR branch, resolve conflicts, commit, and submit without pushing. Never rebase.";
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        enter_merge_conflict_rework(&mut conn, fixture.task_id, feedback);
        tasks::claim_remediation_rework_with_feedback(
            &mut conn,
            "Dormant",
            fixture.task_id,
            tasks::DEFAULT_LEASE_TTL_SECS,
            super::super::now_unix(),
            Some(feedback),
        )
        .unwrap()
        .unwrap();
        drop(conn);

        let mut workers = Vec::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut super::super::names::Pool::new_generated(),
            &mut workers,
            &mut LifetimeRoster::new(),
        )
        .await
        .unwrap();
        assert_recovered_sticky_rework(&fixture, &workers, feedback);
    }

    #[tokio::test]
    async fn restart_recovers_sticky_pre_review_ci_rework() {
        let fixture = dormant_fixture();
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        tasks::apply_checks_failed_with_remediation(
            &mut conn,
            fixture.task_id,
            901,
            "head-sha",
            &["unit".into()],
            "fix failing CI",
            super::super::now_unix(),
        )
        .unwrap();
        drop(conn);

        let mut workers = Vec::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut super::super::names::Pool::new_generated(),
            &mut workers,
            &mut LifetimeRoster::new(),
        )
        .await
        .unwrap();
        assert_recovered_sticky_rework(&fixture, &workers, "fix failing CI");
        assert_eq!(workers[0].pending_prompt, "fix failing CI");
    }

    #[tokio::test]
    async fn restart_recovers_exact_retryable_merge_failure_turn() {
        let fixture = dormant_fixture();
        let feedback = "Merge of PR #901 failed: head branch was modified.\n\nPreserve the published PR head, merge main into the PR branch, resolve conflicts, commit, and submit without pushing. Never rebase.";
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let now = super::super::now_unix();
        tasks::apply_event(
            &mut conn,
            "Reviewer",
            fixture.task_id,
            &Event::ReviewerAttached {
                agent: "Reviewer".into(),
            },
            now + 1,
        )
        .unwrap();
        tasks::apply_event(
            &mut conn,
            "Reviewer",
            fixture.task_id,
            &Event::VerdictApprove,
            now + 2,
        )
        .unwrap();
        tasks::apply_event(
            &mut conn,
            "system",
            fixture.task_id,
            &Event::MergeFailed {
                reason: "head branch was modified".into(),
            },
            now + 3,
        )
        .unwrap();
        tasks::apply_actionable_rework_event(
            &mut conn,
            "Reviewer",
            fixture.task_id,
            &Event::VerdictChanges,
            feedback,
            now + 4,
        )
        .unwrap();
        drop(conn);

        let mut workers = Vec::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut super::super::names::Pool::new_generated(),
            &mut workers,
            &mut LifetimeRoster::new(),
        )
        .await
        .unwrap();
        assert_recovered_sticky_rework(&fixture, &workers, feedback);
    }

    #[tokio::test]
    async fn startup_retryable_approval_replay_survives_following_generic_recovery() {
        let fixture = dormant_fixture();
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        journal::delete(&mut conn, "Dormant").unwrap();
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1",
            [tasks::lease_target(fixture.task_id)],
        )
        .unwrap();
        conn.execute(
            "UPDATE tasks SET status='merging',reviewer='Reviewer-2' WHERE id=?1",
            [fixture.task_id],
        )
        .unwrap();
        for (role, reviewer) in [("r1", "Reviewer-1"), ("r2", "Reviewer-2")] {
            quorum_core::approvals::record(
                &mut conn,
                &quorum_core::approvals::Approval {
                    pr_number: 901,
                    review_role: role.into(),
                    task_id: fixture.task_id,
                    author: "Dormant".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "approved-head".into(),
                },
            )
            .unwrap();
        }
        quorum_core::review_audits::record_r2_requirement(
            &mut conn,
            fixture.task_id,
            901,
            "approved-head",
            true,
        )
        .unwrap();
        drop(conn);

        let concrete = std::sync::Arc::new(StartupRetryableMerge {
            calls: AtomicUsize::new(0),
        });
        let executor: std::sync::Arc<dyn super::super::merge::MergeExecutor> = concrete.clone();
        let outcome = super::super::approvals::recover(
            &fixture.config.db_path,
            &fixture.config.repo_dir,
            &executor,
            &fixture.config.base_branch,
            1,
            1,
        )
        .await
        .unwrap();
        assert_eq!(outcome.deferred, 1);
        assert_eq!(concrete.calls.load(Ordering::SeqCst), 1);

        let mut workers = Vec::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut super::super::names::Pool::new_generated(),
            &mut workers,
            &mut LifetimeRoster::new(),
        )
        .await
        .unwrap();

        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let task = tasks::get(&conn, fixture.task_id).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        assert_eq!(task.rework_round, 1);
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[tasks::PARKED_REWORK_RETRY_REF], true);
        assert!(refs["remediation_feedback"]
            .as_str()
            .unwrap()
            .contains("startup approval replay"));
        assert!(quorum_core::approvals::get_for_pr(&conn, 901)
            .unwrap()
            .is_empty());
        assert_eq!(concrete.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn late_changes_retires_dormant_runtime_into_authoritative_rework_retry() {
        let fixture = dormant_fixture();
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        conn.execute(
            "UPDATE tasks SET status='rework',assignee=NULL,
                 refs=json_set(refs,
                   '$.daemon_rework_retry_requested',json('true'),
                   '$.remediation_feedback','fix the blocking finding')
             WHERE id=?1",
            [fixture.task_id],
        )
        .unwrap();
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1",
            [tasks::lease_target(fixture.task_id)],
        )
        .unwrap();
        drop(conn);

        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();

        assert!(workers.is_empty());
        assert!(!roster.owns("Dormant"));
        assert!(names.acquire_named("Dormant").is_some());
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let task = tasks::get(&conn, fixture.task_id).unwrap().unwrap();
        assert_eq!(task.status, "rework");
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs[tasks::PARKED_REWORK_RETRY_REF], true);
        assert_eq!(refs["remediation_feedback"], "fix the blocking finding");
        assert_eq!(
            runner_state::continuation(&refs, ContinuationSlot::Worker, "codex")
                .unwrap()
                .id,
            "thread-dormant"
        );
        let durable: (i64, i64, i64) = conn
            .query_row(
                "SELECT
                   (SELECT count(*) FROM journal WHERE agent='Dormant'),
                   (SELECT count(*) FROM agent_runs WHERE task_id=?1 AND role='worker'),
                   (SELECT count(*) FROM run_capabilities
                     WHERE task_id=?1 AND role='worker' AND revoked_at IS NULL)",
                [fixture.task_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(durable, (0, 1, 0));
        assert!(!fixture.worktree.exists());
    }

    #[tokio::test]
    async fn pidless_remediation_provision_follows_generic_cleanup() {
        let fixture = dormant_fixture();
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        tasks::apply_event(
            &mut conn,
            "Reviewer",
            fixture.task_id,
            &Event::ReviewerAttached {
                agent: "Reviewer".into(),
            },
            super::super::now_unix() + 3,
        )
        .unwrap();
        tasks::apply_event(
            &mut conn,
            "Reviewer",
            fixture.task_id,
            &Event::VerdictChanges,
            super::super::now_unix() + 4,
        )
        .unwrap();
        tasks::claim_remediation_rework(
            &mut conn,
            "Remediation",
            fixture.task_id,
            3600,
            super::super::now_unix() + 5,
        )
        .unwrap()
        .unwrap();
        journal::delete(&mut conn, "Dormant").unwrap();
        quorum_core::capabilities::issue(
            &mut conn,
            "cap-remediation",
            fixture.task_id,
            "Remediation",
            "worker",
            super::super::now_unix() + 5,
        )
        .unwrap();
        journal::upsert(
            &mut conn,
            &JournalEntry {
                agent: "Remediation".into(),
                role: "worker".into(),
                task_id: Some(fixture.task_id),
                session_id: "session-remediation".into(),
                worktree: Some(fixture.worktree.to_string_lossy().into_owned()),
                branch: Some("daemon/original-pr-head".into()),
                phase: "working".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: Some(901),
                rework_count: 0,
                provider: None,
                continuation_id: None,
                local_branch: Some("daemon/remediation-t1".into()),
            },
        )
        .unwrap();
        drop(conn);

        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();

        assert!(
            workers.is_empty(),
            "pre-spawn rows cannot create a live slot"
        );
        assert!(!roster.owns("Remediation"));
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        let task = tasks::get(&conn, fixture.task_id).unwrap().unwrap();
        assert_eq!(task.status, "open");
        assert!(journal::list_in_flight(&conn)
            .unwrap()
            .iter()
            .all(|entry| entry.agent != "Remediation"));
        let active_capabilities: i64 = conn
            .query_row(
                "SELECT count(*) FROM run_capabilities
                 WHERE run_id='cap-remediation' AND revoked_at IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_capabilities, 0);
        assert!(!fixture.worktree.exists());
    }

    #[tokio::test]
    async fn deferred_merging_reconstructs_dormant_worker_and_preserves_merge_wait() {
        let fixture = dormant_fixture();
        let mut conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        for (role, reviewer) in [("r1", "ReviewerOne"), ("r2", "ReviewerTwo")] {
            approvals::record(
                &mut conn,
                &approvals::Approval {
                    pr_number: 901,
                    review_role: role.into(),
                    task_id: fixture.task_id,
                    author: "Dormant".into(),
                    reviewer: reviewer.into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "head-901".into(),
                },
            )
            .unwrap();
        }
        conn.execute(
            "UPDATE tasks SET status='merging' WHERE id=?1",
            [fixture.task_id],
        )
        .unwrap();
        drop(conn);

        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();

        assert_eq!(workers.len(), 1);
        assert!(matches!(workers[0].proc, SlotProcess::Dormant { .. }));
        assert!(roster.owns("Dormant"));
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        assert_eq!(
            tasks::get(&conn, fixture.task_id).unwrap().unwrap().status,
            "merging"
        );
        assert_eq!(journal::list_in_flight(&conn).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn settled_graph_blocker_retires_dormant_runtime_without_restart_failure() {
        let fixture = dormant_fixture();
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        conn.execute(
            "UPDATE tasks SET status='failed',assignee=NULL WHERE id=?1",
            [fixture.task_id],
        )
        .unwrap();
        conn.execute(
            "UPDATE claims SET active=0 WHERE target=?1",
            [tasks::lease_target(fixture.task_id)],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO events(ts,kind,subject,body,expires_at)
             VALUES (?1,'task_graph_blocked',?2,'graph blocked by Reviewer',?3)",
            rusqlite::params![
                super::super::now_unix(),
                tasks::lease_target(fixture.task_id),
                super::super::now_unix() + 3600
            ],
        )
        .unwrap();
        drop(conn);

        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap();

        assert!(workers.is_empty());
        assert!(!roster.owns("Dormant"));
        assert!(names.acquire_named("Dormant").is_some());
        let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
        assert_eq!(journal::list_in_flight(&conn).unwrap().len(), 0);
        assert_eq!(
            tasks::get(&conn, fixture.task_id).unwrap().unwrap().status,
            "failed"
        );
        assert!(!fixture.worktree.exists());
    }

    #[tokio::test]
    async fn corrupt_dormant_recovery_fails_without_mutating_durable_identity() {
        for (mutation, expected) in [
            (
                "UPDATE journal SET continuation_id=NULL",
                "missing provider continuation",
            ),
            (
                "UPDATE claims SET holder='Other' WHERE active=1",
                "claim mismatch",
            ),
            ("UPDATE tasks SET status='done'", "stale journal"),
            (
                "UPDATE tasks SET refs=json_set(refs,'$.pr',902)",
                "artifact mismatch",
            ),
            (
                "UPDATE journal SET local_branch='wrong-branch'",
                "branch does not match",
            ),
            (
                "UPDATE task_branches SET worktree='/tmp/wrong-worktree'",
                "no durable branch/worktree",
            ),
            (
                "UPDATE tasks SET status='rework',assignee=NULL,
                   refs=json_set(refs,'$.daemon_rework_retry_requested',json('true'));
                 UPDATE claims SET active=0 WHERE active=1",
                "without an authoritative retry",
            ),
            (
                "UPDATE tasks SET status='rework',
                   refs=json_set(refs,
                     '$.daemon_rework_retry_requested',json('true'),
                     '$.remediation_feedback','fix blocker')",
                "retained runtime ownership",
            ),
            (
                "UPDATE tasks SET status='rework',assignee='Dormant';
                 UPDATE claims SET active=0 WHERE active=1",
                "missing its exact pending turn",
            ),
            (
                "UPDATE tasks SET status='rework',assignee='Other';
                 UPDATE claims SET active=0 WHERE active=1",
                "not owned wholly by this agent",
            ),
        ] {
            let fixture = dormant_fixture();
            let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            conn.execute_batch(mutation).unwrap();
            drop(conn);
            let mut names = super::super::names::Pool::new_generated();
            let mut workers = Vec::new();
            let mut roster = LifetimeRoster::new();
            let error = recover(
                &fixture.config,
                &WorktreeManager::new(),
                &mut names,
                &mut workers,
                &mut roster,
            )
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
            assert!(
                workers.is_empty(),
                "corrupt recovery must not create a slot"
            );
            assert!(
                names.acquire_named("Dormant").is_none(),
                "a corrupt dormant identity must remain quarantined from name reuse"
            );
            let conn = quorum_core::db::open(&fixture.config.db_path).unwrap();
            let durable: (i64, i64, i64) = conn
                .query_row(
                    "SELECT
                       (SELECT count(*) FROM journal WHERE agent='Dormant'),
                       (SELECT count(*) FROM agent_runs WHERE task_id=1 AND role='worker'),
                       (SELECT count(*) FROM run_capabilities WHERE task_id=1 AND role='worker')",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(
                durable,
                (1, 1, 1),
                "failure must retain exact identity evidence"
            );
        }
    }

    #[tokio::test]
    async fn replaced_checkout_on_expected_branch_is_not_a_registered_worktree() {
        let fixture = dormant_fixture();
        let worktree = fixture.worktree.to_string_lossy().into_owned();
        run_git(
            &fixture.config.repo_dir,
            &["worktree", "remove", &worktree, "--force"],
        );
        std::fs::create_dir_all(&fixture.worktree).unwrap();
        run_git(&fixture.worktree, &["init", "-b", "daemon/dormant-t1"]);
        run_git(
            &fixture.worktree,
            &["config", "user.email", "test@example.com"],
        );
        run_git(&fixture.worktree, &["config", "user.name", "Replacement"]);
        run_git(
            &fixture.worktree,
            &["commit", "--allow-empty", "-m", "replacement"],
        );

        let mut names = super::super::names::Pool::new_generated();
        let mut workers = Vec::new();
        let mut roster = LifetimeRoster::new();
        let error = recover(
            &fixture.config,
            &WorktreeManager::new(),
            &mut names,
            &mut workers,
            &mut roster,
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("without exact git registration"),
            "unexpected error: {error}"
        );
        assert!(workers.is_empty());
        assert!(names.acquire_named("Dormant").is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dormant_git_inspection_bounds_silent_and_continuous_processes() {
        use std::os::unix::fs::PermissionsExt;

        for (name, body, expected, timeout) in [
            (
                "silent-git",
                "exec sleep 3600",
                "timed out",
                // Leave enough scheduling headroom for the shim to record its
                // PID even while the full suite is running four test binaries.
                std::time::Duration::from_secs(2),
            ),
            (
                "noisy-git",
                "while :; do printf 0123456789; printf abcdefghij >&2; done",
                "exceeded",
                std::time::Duration::from_secs(5),
            ),
        ] {
            let fixture = dormant_fixture();
            let shim = fixture._dir.path().join(name);
            let pid_file = fixture._dir.path().join(format!("{name}.pid"));
            std::fs::write(
                &shim,
                format!("#!/bin/sh\necho $$ > '{}'\n{body}\n", pid_file.display()),
            )
            .unwrap();
            std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
            let wt_mgr =
                WorktreeManager::with_config(shim, std::time::Duration::from_secs(60), timeout);
            let mut names = super::super::names::Pool::new_generated();
            let mut workers = Vec::new();
            let mut roster = LifetimeRoster::new();
            let started = std::time::Instant::now();
            let error = recover(
                &fixture.config,
                &wt_mgr,
                &mut names,
                &mut workers,
                &mut roster,
            )
            .await
            .unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "unexpected {name} error: {error}"
            );
            assert!(
                started.elapsed() < std::time::Duration::from_secs(10),
                "{name} was not bounded"
            );
            let pid = std::fs::read_to_string(&pid_file).unwrap();
            assert!(
                !std::process::Command::new("kill")
                    .args(["-0", pid.trim()])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .unwrap()
                    .success(),
                "{name} subprocess {pid:?} was not reaped"
            );
            assert!(workers.is_empty());
            assert!(names.acquire_named("Dormant").is_none());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn journaled_planner_and_classifier_process_groups_are_reaped_with_frozen_views() {
        use std::os::unix::process::CommandExt;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("recovery.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
             VALUES ('large','open','owner',1,1)",
            [],
        )
        .unwrap();
        let graph_id = quorum_core::decomposition::begin_planning(
            &mut conn,
            &quorum_core::decomposition::BeginPlanning {
                source_task_id: 1,
                expected_revision: 1,
                provider: "claude",
                model: "opus",
                frozen_base_sha: "",
                now: 2,
            },
        )
        .unwrap()
        .unwrap();
        quorum_core::decomposition::record_attempt(
            &mut conn,
            graph_id,
            "provider",
            "before-restart",
            "bounded",
            3,
        )
        .unwrap();

        let mut children = Vec::new();
        let mut view_paths = Vec::new();
        for role in ["planner", "classifier"] {
            let view = tempfile::Builder::new()
                .prefix("quorum-planner-")
                .tempdir()
                .unwrap()
                .keep();
            std::fs::write(view.join("frozen.txt"), role).unwrap();
            let child = std::process::Command::new("sleep")
                .arg("60")
                .process_group(0)
                .spawn()
                .unwrap();
            let pid = child.id() as i32;
            journal::upsert(
                &mut conn,
                &JournalEntry {
                    agent: format!("decomposition-{role}-{graph_id}"),
                    role: role.into(),
                    task_id: Some(1),
                    session_id: format!("{role}-session"),
                    worktree: Some(view.to_string_lossy().into_owned()),
                    branch: None,
                    phase: role.into(),
                    cost_tokens: 0,
                    agent_state: None,
                    cost_usd: 0.0,
                    log_dir: None,
                    pid: Some(pid),
                    pr: None,
                    rework_count: 0,
                    provider: None,
                    continuation_id: None,
                    local_branch: None,
                },
            )
            .unwrap();
            children.push((child, pid));
            view_paths.push(view);
        }
        let entries = journal::list_in_flight(&conn).unwrap();
        assert_eq!(entries.len(), 2);
        for entry in &entries {
            kill_stale_process_group(entry.pid);
        }
        let pids = children.iter().map(|(_, pid)| *pid).collect::<Vec<_>>();
        for (mut child, _) in children {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        for pid in &pids {
            assert!(await_process_exit(*pid, std::time::Duration::from_secs(5)).await);
        }
        remove_journaled_decomposition_views(&entries);
        assert!(view_paths.iter().all(|path| !path.exists()));
        let attempts: i64 = conn
            .query_row(
                "SELECT count(*) FROM decomposition_attempts WHERE graph_id=?1",
                [graph_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            attempts, 1,
            "restart cleanup must not charge a provider attempt"
        );
    }

    #[tokio::test]
    async fn await_process_exit_returns_true_when_process_dies() {
        let mut child = std::process::Command::new("sleep")
            .arg("0.05")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id() as i32;
        std::thread::spawn(move || {
            let _ = child.wait();
        });

        let dead = await_process_exit(pid, std::time::Duration::from_secs(5)).await;
        assert!(dead, "process should have exited within timeout");
    }

    #[tokio::test]
    async fn await_process_exit_returns_false_on_timeout() {
        let mut child = std::process::Command::new("sleep")
            .arg("60")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id() as i32;

        let dead = await_process_exit(pid, std::time::Duration::from_millis(100)).await;
        assert!(!dead, "process should still be alive at timeout");

        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let _ = child.wait();
    }

    #[tokio::test]
    async fn await_process_exit_killed_process_confirms_death() {
        let mut child = std::process::Command::new("sleep")
            .arg("60")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn sleep");
        let pid = child.id() as i32;

        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        std::thread::spawn(move || {
            let _ = child.wait();
        });

        let dead = await_process_exit(pid, std::time::Duration::from_secs(5)).await;
        assert!(dead, "SIGKILL'd process should be confirmed dead");
    }
}
