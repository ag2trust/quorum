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
use quorum_core::mailbox;
use quorum_core::{error::QuorumError, error::Result};
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

    if entries.is_empty() {
        return Ok(());
    }

    log(&format!(
        "recovery: found {} in-flight journal entries",
        entries.len(),
    ));

    // Phase 1: Kill stale process groups
    for entry in &entries {
        kill_stale_process_group(entry.pid);
    }
    // Brief pause to let processes exit
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

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

    log(&format!(
        "recovery: complete — {} worker(s) resumed",
        workers.len()
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
                let fields = quorum_core::tasks::TaskUpdate {
                    status: Some("open"),
                    body: None,
                    refs: None,
                    verdict: None,
                };
                let _ = quorum_core::tasks::update(&mut conn, &a, task_id, &fields, now);
            }
            let _ = journal::delete(&mut conn, &a);
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
