//! M7 crash recovery: journal-driven resurrection on daemon restart.
//!
//! On startup, reads `list_in_flight()` from the journal. For each entry:
//! - Kills any stale process group (best-effort, via stored PID)
//! - Reclaims the agent name in the pool
//! - Workers: resumes via `claude --resume <session_id>`, feeds a resume turn
//! - Reviewers: tears down (ephemeral — Phase 5 respawns fresh ones)
//!
//! Orphaned worktrees (present on disk but absent from journal) are GC'd.
//! GC is naturally scoped: it only scans this instance's `worktree_base`.

use super::agent::{AgentProc, AgentSpec, ALLOWED_TOOLS};
use super::names::Pool;
use super::session_log::SessionLog;
use super::worktree::WorktreeManager;
use super::{log, LifetimeRoster, PendingReview, ServeConfig, SlotState};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::lifecycle::Event;
use quorum_core::mailbox;
use quorum_core::{error::QuorumError, error::Result, tasks};
use std::path::PathBuf;

fn kill_stale_process_group(pid: Option<i32>) {
    if let Some(pid) = pid {
        unsafe {
            // SIGKILL the entire process group. ESRCH (no such process) is expected
            // when the group already exited after a crash — silently ignored.
            libc::killpg(pid, libc::SIGKILL);
        }
    }
}

pub(crate) fn build_resume_turn(entry: &JournalEntry) -> String {
    let content = match (entry.role.as_str(), entry.phase.as_str()) {
        ("worker", "working") => format!(
            "Your daemon session was interrupted and has been resumed. \
             You were working on task #{task_id} in worktree {wt} on branch {branch}. \
             Continue where you left off.",
            task_id = entry.task_id.unwrap_or(0),
            wt = entry.worktree.as_deref().unwrap_or("(unknown)"),
            branch = entry.branch.as_deref().unwrap_or("(unknown)"),
        ),
        ("worker", "awaiting-review") => format!(
            "Your daemon session was interrupted and has been resumed. \
             You had submitted PR #{pr} for task #{task_id} and were awaiting review. \
             Wait for the review verdict — the daemon will deliver it.",
            pr = entry.pr.unwrap_or(0),
            task_id = entry.task_id.unwrap_or(0),
        ),
        _ => format!(
            "Your daemon session was interrupted and has been resumed. \
             Continue your current work (phase: {phase}).",
            phase = entry.phase,
        ),
    };

    super::agent::user_turn(&content)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn recover(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    workers: &mut Vec<SlotState>,
    _reviewers: &mut [SlotState],
    pending_reviews: &mut Vec<PendingReview>,
    lifetime_roster: &mut LifetimeRoster,
) -> Result<()> {
    let db_path = config.db_path.clone();
    let entries = {
        let p = db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<JournalEntry>> {
            let conn = quorum_core::db::open(&p)?;
            journal::list_in_flight(&conn)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
    }?;

    if !entries.is_empty() {
        log(&format!(
            "recovery: found {} in-flight journal entries",
            entries.len(),
        ));
    }

    // Phase 1: Kill stale process groups
    if !entries.is_empty() {
        for entry in &entries {
            kill_stale_process_group(entry.pid);
        }
        // Brief pause to let processes exit
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Phase 2: Reclaim names + process entries
    let mut active_worktrees: Vec<String> = Vec::new();

    for entry in &entries {
        // Reclaim the name (always succeeds — generated names are tracked on reclaim)
        name_pool.reclaim(&entry.agent);
        // #181: register recovered agent name as owned by this instance so
        // its subsequent mailbox rows are consumable (not left for a sibling).
        lifetime_roster.register(&entry.agent);

        if let Some(ref wt) = entry.worktree {
            active_worktrees.push(wt.clone());
        }

        match entry.role.as_str() {
            "reviewer" => {
                // Reviewers are ephemeral — tear down, don't resume.
                // Phase 5 will spawn fresh ones if workers have PRs.
                log(&format!(
                    "recovery: tearing down stale reviewer {} (task #{:?})",
                    entry.agent, entry.task_id
                ));
                if let Some(ref wt) = entry.worktree {
                    let wt_path = PathBuf::from(wt);
                    wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
                    // Remove from active since we're cleaning it up
                    active_worktrees.retain(|w| w != wt);
                }
                if let Some(ref branch) = entry.branch {
                    wt_mgr.delete_branch(&config.repo_dir, branch).await;
                }
                let p = db_path.clone();
                let agent = entry.agent.clone();
                tokio::task::spawn_blocking(move || {
                    if let Ok(mut conn) = quorum_core::db::open(&p) {
                        let _ = journal::delete(&mut conn, &agent);
                    }
                })
                .await
                .ok();
                name_pool.release(&entry.agent);
            }
            "worker" => {
                // Verify the worktree still exists
                let wt_path = match &entry.worktree {
                    Some(wt) => {
                        let p = PathBuf::from(wt);
                        if !p.exists() {
                            log(&format!(
                                "recovery: worktree missing for worker {} — releasing task",
                                entry.agent
                            ));
                            release_and_cleanup(
                                config,
                                wt_mgr,
                                name_pool,
                                &entry.agent,
                                entry.task_id,
                                entry.branch.as_deref(),
                            )
                            .await;
                            active_worktrees.retain(|w| w != wt);
                            continue;
                        }
                        p
                    }
                    None => {
                        log(&format!(
                            "recovery: no worktree for worker {} — releasing task",
                            entry.agent
                        ));
                        release_and_cleanup(
                            config,
                            wt_mgr,
                            name_pool,
                            &entry.agent,
                            entry.task_id,
                            entry.branch.as_deref(),
                        )
                        .await;
                        continue;
                    }
                };

                // Drain stale mailbox rows for this name (F9)
                {
                    let p = db_path.clone();
                    let name = entry.agent.clone();
                    let stale = tokio::task::spawn_blocking(move || -> Result<usize> {
                        let mut conn = quorum_core::db::open(&p)?;
                        mailbox::consume_all_for_agent(&mut conn, &name)
                    })
                    .await
                    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                    .unwrap_or(0);
                    if stale > 0 {
                        log(&format!(
                            "recovery: consumed {stale} stale mailbox row(s) for {}",
                            entry.agent
                        ));
                    }
                }

                // #178: Worker in `awaiting-review` with a recorded PR means
                // "task pipeline position = review stage." Do NOT respawn a
                // worker process (either the resumed session sits idle and
                // wastes context, or if the CLI can't resume it dies and the
                // task gets reaped to `open`, producing a duplicate PR on
                // re-execution). Instead, register a `PendingReview` so
                // Phase 5 provisions a reviewer directly against the
                // recorded PR. A `--resume` worker is spawned lazily later
                // if the reviewer asks for changes.
                if let (true, Some(pr)) = (entry.phase == "awaiting-review", entry.pr) {
                    // Persist journal (no PID, phase already awaiting-review).
                    let p = db_path.clone();
                    let mut refreshed = entry.clone();
                    refreshed.pid = None;
                    tokio::task::spawn_blocking(move || -> Result<()> {
                        let mut conn = quorum_core::db::open(&p)?;
                        journal::upsert(&mut conn, &refreshed)
                    })
                    .await
                    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                    .ok();

                    pending_reviews.push(PendingReview {
                        agent_name: entry.agent.clone(),
                        task_id: entry.task_id.unwrap_or(0),
                        pr,
                        session_id: entry.session_id.clone(),
                        worktree_path: wt_path,
                        branch: entry.branch.clone().unwrap_or_default(),
                        rework_count: entry.rework_count as u32,
                        cost_tokens: entry.cost_tokens,
                        cost_usd: entry.cost_usd,
                        agent_state: entry.agent_state.clone(),
                        log_dir: entry.log_dir.as_ref().map(PathBuf::from),
                        task_started_at: std::time::Instant::now(),
                    });
                    log(&format!(
                        "recovery: resuming task #{} at REVIEW stage \
                         (worker {}, PR #{}) — awaiting reviewer provision",
                        entry.task_id.unwrap_or(0),
                        entry.agent,
                        pr,
                    ));
                    continue;
                }

                // Spawn with --resume
                let spec = AgentSpec {
                    model: config.model.clone(),
                    effort: config.effort.clone(),
                    session_id: entry.session_id.clone(),
                    worktree: wt_path.clone(),
                    bare: config.bare_agent,
                    resume: true,
                    allowed_tools: ALLOWED_TOOLS.to_string(),
                    env_vars: vec![("QUORUM_REPO".into(), config.repo.clone())],
                };

                match AgentProc::spawn(&spec, config.agent_bin.as_deref()) {
                    Ok(mut proc) => {
                        // Only feed a resume turn to workers in "working" phase.
                        // "awaiting-review" workers are idle (waiting for reviewer verdict).
                        let draining = entry.phase == "working";
                        if draining {
                            let turn = build_resume_turn(entry);
                            if let Err(e) = proc.feed_turn(&turn).await {
                                log(&format!(
                                    "recovery: feed_turn failed for {} — {e}, releasing task",
                                    entry.agent
                                ));
                                proc.kill_and_reap().await;
                                release_and_cleanup(
                                    config,
                                    wt_mgr,
                                    name_pool,
                                    &entry.agent,
                                    entry.task_id,
                                    entry.branch.as_deref(),
                                )
                                .await;
                                continue;
                            }
                        }

                        // Re-open session log if log_dir is known
                        let session_log = entry
                            .log_dir
                            .as_ref()
                            .and_then(|ld| SessionLog::reopen(&PathBuf::from(ld)).ok());

                        // Update journal with new PID
                        let new_pid = proc.pid();
                        {
                            let p = db_path.clone();
                            let mut updated_entry = entry.clone();
                            updated_entry.pid = new_pid;
                            tokio::task::spawn_blocking(move || -> Result<()> {
                                let mut conn = quorum_core::db::open(&p)?;
                                journal::upsert(&mut conn, &updated_entry)
                            })
                            .await
                            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                            .ok();
                        }

                        let now_instant = std::time::Instant::now();
                        workers.push(SlotState {
                            agent_name: entry.agent.clone(),
                            proc,
                            task_id: entry.task_id.unwrap_or(0),
                            session_id: entry.session_id.clone(),
                            worktree_path: wt_path,
                            branch: entry.branch.clone().unwrap_or_default(),
                            draining,
                            pr: entry.pr,
                            rework_count: entry.rework_count as u32,
                            cost_tokens: entry.cost_tokens,
                            cost_usd: entry.cost_usd,
                            task_started_at: now_instant,
                            turn_started_at: now_instant,
                            agent_state: entry.agent_state.clone(),
                            session_log,
                            live_stats: super::LiveStats::new(),
                        });

                        log(&format!(
                            "recovery: resumed worker {} (task #{}, phase={}, pid={:?})",
                            entry.agent,
                            entry.task_id.unwrap_or(0),
                            entry.phase,
                            new_pid,
                        ));
                    }
                    Err(e) => {
                        log(&format!(
                            "recovery: spawn failed for {} — {e}, releasing task",
                            entry.agent
                        ));
                        release_and_cleanup(
                            config,
                            wt_mgr,
                            name_pool,
                            &entry.agent,
                            entry.task_id,
                            entry.branch.as_deref(),
                        )
                        .await;
                    }
                }
            }
            other => {
                log(&format!(
                    "recovery: unknown role '{}' for {} — deleting journal entry",
                    other, entry.agent
                ));
                let p = db_path.clone();
                let agent = entry.agent.clone();
                tokio::task::spawn_blocking(move || {
                    if let Ok(mut conn) = quorum_core::db::open(&p) {
                        let _ = journal::delete(&mut conn, &agent);
                    }
                })
                .await
                .ok();
                name_pool.release(&entry.agent);
            }
        }
    }

    // Phase 3: GC orphaned worktrees
    let active_refs: Vec<&str> = active_worktrees.iter().map(|s| s.as_str()).collect();
    let removed = wt_mgr
        .gc_orphaned(&config.repo_dir, &config.worktree_base, &active_refs)
        .await;
    if !removed.is_empty() {
        log(&format!(
            "recovery: GC'd {} orphaned worktree(s)",
            removed.len()
        ));
    }

    // Phase 4: Rescue orphaned in-review tasks (C7)
    // Tasks stuck in `in-review` with no journal row and no live reviewer are
    // invisible to the journal-driven recovery above. Scan for them and
    // register a PendingReview so Phase 5 provisions a reviewer.
    let journal_task_ids: std::collections::HashSet<i64> = pending_reviews
        .iter()
        .map(|p| p.task_id)
        .chain(workers.iter().map(|w| w.task_id))
        .collect();
    {
        let p = db_path.clone();
        let orphans =
            tokio::task::spawn_blocking(move || -> Result<Vec<quorum_core::tasks::Task>> {
                let conn = quorum_core::db::open(&p)?;
                quorum_core::tasks::list(&conn, Some("in-review"), None, None)
            })
            .await
            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            .unwrap_or_default();

        for task in orphans {
            if journal_task_ids.contains(&task.id) {
                continue;
            }
            let pr_str = quorum_core::tasks::extract_pr_number(&task.refs);
            let Some(pr_num) = pr_str else {
                log(&format!(
                    "recovery: orphaned in-review task #{} has no PR ref — firing AgentFailed",
                    task.id
                ));
                let p2 = db_path.clone();
                let tid = task.id;
                tokio::task::spawn_blocking(move || {
                    if let Ok(mut conn) = quorum_core::db::open(&p2) {
                        let now = crate::serve::now_unix();
                        let event = Event::AgentFailed {
                            reason: "orphaned in-review task with no PR on recovery".into(),
                        };
                        match tasks::apply_event(&mut conn, "daemon", tid, &event, now) {
                            Ok(tr) => {
                                log(&format!(
                                    "recovery: orphan task #{tid} -> {} via AgentFailed",
                                    tr.task.status,
                                ));
                            }
                            Err(e) => {
                                log(&format!(
                                    "recovery: orphan AgentFailed failed for task #{tid}: {e}",
                                ));
                            }
                        }
                    }
                })
                .await
                .ok();
                continue;
            };

            log(&format!(
                "recovery: rescuing orphaned in-review task #{} (PR #{pr_num}) — registering PendingReview",
                task.id,
            ));

            let agent_name = format!("orphan-rescue-{}", task.id);
            name_pool.reclaim(&agent_name);
            lifetime_roster.register(&agent_name);

            let worktree_path = config.worktree_base.join(&agent_name);
            let branch = format!("orphan-rescue-task-{}", task.id);

            let p2 = db_path.clone();
            let entry = JournalEntry {
                agent: agent_name.clone(),
                role: "worker".into(),
                task_id: Some(task.id),
                session_id: String::new(),
                worktree: Some(worktree_path.to_string_lossy().into()),
                branch: Some(branch.clone()),
                phase: "awaiting-review".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: Some(pr_num),
                rework_count: task.rework_round as i32,
            };
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = quorum_core::db::open(&p2)?;
                journal::upsert(&mut conn, &entry)
            })
            .await
            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            .ok();

            pending_reviews.push(PendingReview {
                agent_name,
                task_id: task.id,
                pr: pr_num,
                session_id: String::new(),
                worktree_path,
                branch,
                rework_count: task.rework_round as u32,
                cost_tokens: 0,
                cost_usd: 0.0,
                agent_state: None,
                log_dir: None,
                task_started_at: std::time::Instant::now(),
            });
        }
    }

    // Phase 5: Rescue orphaned merging tasks (M10)
    // A task stuck in `merging` with no journal entry means the daemon died
    // mid-merge (SIGKILL, crash, or force-kill where teardown's AgentFailed was
    // rejected by the old lifecycle). Fire AgentFailed to move it back to
    // `in-review`, then register a PendingReview so Phase 6 provisions a
    // reviewer to re-evaluate the PR state.
    {
        let p = db_path.clone();
        let merging_orphans =
            tokio::task::spawn_blocking(move || -> Result<Vec<quorum_core::tasks::Task>> {
                let conn = quorum_core::db::open(&p)?;
                quorum_core::tasks::list(&conn, Some("merging"), None, None)
            })
            .await
            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            .unwrap_or_default();

        for task in merging_orphans {
            if journal_task_ids.contains(&task.id) {
                continue;
            }

            let pr_str = quorum_core::tasks::extract_pr_number(&task.refs);
            let tid = task.id;

            // Fire AgentFailed to transition merging -> in-review
            let p2 = db_path.clone();
            let transitioned = tokio::task::spawn_blocking(move || -> bool {
                let Ok(mut conn) = quorum_core::db::open(&p2) else {
                    return false;
                };
                let now = crate::serve::now_unix();
                let event = Event::AgentFailed {
                    reason: "orphaned merging task on recovery (M10)".into(),
                };
                match tasks::apply_event(&mut conn, "daemon", tid, &event, now) {
                    Ok(tr) => {
                        log(&format!(
                            "recovery: orphan merging task #{tid} -> {} via AgentFailed",
                            tr.task.status,
                        ));
                        true
                    }
                    Err(e) => {
                        log(&format!(
                            "recovery: orphan AgentFailed failed for merging task #{tid}: {e}",
                        ));
                        false
                    }
                }
            })
            .await
            .unwrap_or(false);

            if !transitioned {
                continue;
            }

            let Some(pr_num) = pr_str else {
                log(&format!(
                    "recovery: orphaned merging task #{tid} has no PR ref — \
                     moved to in-review but cannot register PendingReview",
                ));
                continue;
            };

            log(&format!(
                "recovery: rescuing orphaned merging task #{tid} (PR #{pr_num}) — \
                 registering PendingReview",
            ));

            let agent_name = format!("orphan-merge-rescue-{tid}");
            name_pool.reclaim(&agent_name);
            lifetime_roster.register(&agent_name);

            let worktree_path = config.worktree_base.join(&agent_name);
            let branch = format!("orphan-merge-rescue-task-{tid}");

            let p2 = db_path.clone();
            let entry = JournalEntry {
                agent: agent_name.clone(),
                role: "worker".into(),
                task_id: Some(tid),
                session_id: String::new(),
                worktree: Some(worktree_path.to_string_lossy().into()),
                branch: Some(branch.clone()),
                phase: "awaiting-review".into(),
                cost_tokens: 0,
                agent_state: None,
                cost_usd: 0.0,
                log_dir: None,
                pid: None,
                pr: Some(pr_num),
                rework_count: task.rework_round as i32,
            };
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = quorum_core::db::open(&p2)?;
                journal::upsert(&mut conn, &entry)
            })
            .await
            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            .ok();

            pending_reviews.push(PendingReview {
                agent_name,
                task_id: tid,
                pr: pr_num,
                session_id: String::new(),
                worktree_path,
                branch,
                rework_count: task.rework_round as u32,
                cost_tokens: 0,
                cost_usd: 0.0,
                agent_state: None,
                log_dir: None,
                task_started_at: std::time::Instant::now(),
            });
        }
    }

    log(&format!(
        "recovery: complete — {} worker(s) resumed, {} pending review(s)",
        workers.len(),
        pending_reviews.len(),
    ));

    Ok(())
}

async fn release_and_cleanup(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    agent: &str,
    task_id: Option<i64>,
    branch: Option<&str>,
) {
    let p = config.db_path.clone();
    let a = agent.to_string();
    let tid = task_id;
    tokio::task::spawn_blocking(move || {
        if let Ok(mut conn) = quorum_core::db::open(&p) {
            if let Some(task_id) = tid {
                let now = crate::serve::now_unix();
                let event = Event::AgentFailed {
                    reason: "worktree missing on recovery".into(),
                };
                match tasks::apply_event(&mut conn, &a, task_id, &event, now) {
                    Ok(tr) => {
                        log(&format!(
                            "recovery: lifecycle task #{task_id} -> {} via AgentFailed",
                            tr.task.status,
                        ));
                    }
                    Err(e) => {
                        log(&format!(
                            "recovery: apply_event(AgentFailed) failed for task #{task_id}: {e}"
                        ));
                    }
                }
            }
            if let Err(e) = journal::delete(&mut conn, &a) {
                log(&format!("recovery: journal::delete failed for {a}: {e}"));
            }
        }
    })
    .await
    .ok();
    if let Some(branch) = branch {
        wt_mgr.delete_branch(&config.repo_dir, branch).await;
    }
    name_pool.release(agent);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(agent: &str, role: &str, phase: &str) -> JournalEntry {
        JournalEntry {
            agent: agent.into(),
            role: role.into(),
            task_id: Some(42),
            session_id: "sess-001".into(),
            worktree: Some("/tmp/wt/test".into()),
            branch: Some("feat/test".into()),
            phase: phase.into(),
            cost_tokens: 1000,
            agent_state: None,
            cost_usd: 0.05,
            log_dir: None,
            pid: Some(12345),
            pr: Some(10),
            rework_count: 1,
        }
    }

    #[test]
    fn resume_turn_working_phase() {
        let entry = sample_entry("W1", "worker", "working");
        let turn = build_resume_turn(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&turn).unwrap();
        assert_eq!(parsed["type"], "user");
        assert_eq!(
            parsed["message"]["role"], "user",
            "claude CLI exits 1 on turns without message.role"
        );
        let content = parsed["message"]["content"].as_str().unwrap();
        assert!(content.contains("interrupted and has been resumed"));
        assert!(content.contains("task #42"));
        assert!(content.contains("Continue where you left off"));
    }

    #[test]
    fn resume_turn_awaiting_review_phase() {
        let entry = sample_entry("W1", "worker", "awaiting-review");
        let turn = build_resume_turn(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&turn).unwrap();
        let content = parsed["message"]["content"].as_str().unwrap();
        assert!(content.contains("PR #10"));
        assert!(content.contains("task #42"));
        assert!(content.contains("review verdict"));
    }

    #[test]
    fn resume_turn_unknown_phase() {
        let mut entry = sample_entry("W1", "worker", "custom-phase");
        entry.pr = None;
        let turn = build_resume_turn(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&turn).unwrap();
        let content = parsed["message"]["content"].as_str().unwrap();
        assert!(content.contains("custom-phase"));
    }
}
