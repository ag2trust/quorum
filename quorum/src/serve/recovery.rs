//! Crash recovery on daemon restart.
//!
//! On startup:
//! 1. Kill all stale process groups (from journal PIDs)
//! 2. Reconstruct validated dormant review/merge-wait Codex workers in place
//!    and retire rows already transferred to authoritative rework/graph-block state
//! 3. Delete stale/retired journal entries and GC only non-recovered worktrees
//! 4. Scan non-terminal tasks and reset them to states the normal tick
//!    loop can handle:
//!    - `working` / `rework` → AgentFailed → open (Phase 6 re-spawns)
//!    - `merging` → AgentFailed → in-review (Phase 5 spawns reviewer)
//!    - `in-review` → left as-is (Phase 5 spawns reviewer)
//!
//! No provider turn is launched during dormant reconstruction.

use super::worktree::WorktreeManager;
use super::{log, LifetimeRoster, LiveStats, ServeConfig, SlotProcess, SlotState};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::lifecycle::Event;
use quorum_core::runner_state::{self, ContinuationSlot};
use quorum_core::{approvals, error::QuorumError, error::Result, tasks};
use rusqlite::OptionalExtension;
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
    cap_run_id: String,
}

#[derive(Debug, Clone)]
enum DormantRecoveryDisposition {
    Reconstruct(DormantRecovery),
    Retire {
        entry: JournalEntry,
        reason: &'static str,
    },
}

fn dormant_recovery_error(agent: &str, detail: impl std::fmt::Display) -> QuorumError {
    QuorumError::Io(format!(
        "dormant awaiting-review recovery rejected for '{agent}': {detail}"
    ))
}

fn validate_dormant_recovery(
    conn: &rusqlite::Connection,
    entry: &JournalEntry,
    now: i64,
) -> Result<DormantRecoveryDisposition> {
    let invalid = |detail: String| dormant_recovery_error(&entry.agent, detail);
    if entry.role != "worker" || entry.phase != "awaiting-review" || entry.pid.is_some() {
        return Err(invalid(
            "journal phase/process shape is not explicitly dormant".into(),
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
            let feedback_present = refs
                .get("remediation_feedback")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|feedback| !feedback.trim().is_empty());
            if !retry_requested || !feedback_present {
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
    let run = active_runs
        .next()
        .ok_or_else(|| invalid("missing active worker run binding".into()))?;
    if active_runs.next().is_some() {
        return Err(invalid("multiple active worker run bindings".into()));
    }
    if run.provider.as_deref() != Some(provider_name) {
        return Err(invalid("worker run provider mismatch".into()));
    }
    let model_provider = super::resolve_worker_provider(&run.model)
        .map_err(|error| invalid(format!("worker run model is invalid: {error}")))?;
    if model_provider != provider {
        return Err(invalid("worker run model/provider mismatch".into()));
    }
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
        cap_run_id: capability.run_id,
    };
    Ok(match task_disposition {
        Some(reason) => DormantRecoveryDisposition::Retire {
            entry: entry.clone(),
            reason,
        },
        None => DormantRecoveryDisposition::Reconstruct(recovery),
    })
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
            pr: Some(recovery.pr),
            rework_count: recovery.entry.rework_count.max(0) as u32,
            cost_tokens: recovery.entry.cost_tokens,
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
            pending_prompt: String::new(),
            pending_turn_kind: "awaiting-review".into(),
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
        .filter(|entry| {
            entry.role == "worker" && entry.phase == "awaiting-review" && entry.pid.is_none()
        })
        .cloned()
        .collect::<Vec<_>>();
    let dormant_candidate_names = dormant_entries
        .iter()
        .map(|entry| entry.agent.clone())
        .collect::<std::collections::HashSet<_>>();

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

    if let Some(entry) = entries
        .iter()
        .find(|entry| entry.pid.is_none() && !dormant_candidate_names.contains(&entry.agent))
    {
        return Err(QuorumError::Io(format!(
            "recovery rejected pid-less journal row for '{}' in phase '{}'; only explicit dormant awaiting-review workers may omit a PID",
            entry.agent, entry.phase
        )));
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
        tokio::task::spawn_blocking(move || -> Result<Vec<DormantRecoveryDisposition>> {
            let mut conn = quorum_core::db::open(&p)?;
            let tx = quorum_core::db::begin_immediate(&mut conn)?;
            let dispositions = dormant_entries
                .iter()
                .map(|entry| validate_dormant_recovery(&tx, entry, super::now_unix()))
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
            DormantRecoveryDisposition::Reconstruct(recovery) => recoveries.push(recovery),
            DormantRecoveryDisposition::Retire { entry, reason } => {
                retired.push((entry, reason));
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
    // #130: no passive exemption — all working/rework tasks are reset.
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
            exit_when_gone: None,
            required_jobs: Vec::new(),
            master_ci_gate: false,
            master_ci_timeout_secs: 1,
            allowed_tools: None,
            doctor_enabled: false,
            r2_enabled: false,
            r2_target_per_stratum: 0,
            r2_steady_state_p: 0.0,
            codex_sandbox: "danger-full-access".into(),
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
