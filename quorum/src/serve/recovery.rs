//! M7 crash recovery: journal-driven resurrection on daemon restart.
//!
//! On startup, reads `list_in_flight()` from the journal. For each entry:
//! - Kills any stale process group (best-effort, via stored PID)
//! - Reclaims the agent name in the pool
//! - Workers: resumes via `claude --resume <session_id>`, feeds a resume turn
//! - Reviewers: tears down (ephemeral — Phase 5 respawns fresh ones)
//!
//! Orphaned worktrees (present on disk but absent from journal) are GC'd.

use super::agent::{AgentProc, AgentSpec};
use super::names::Pool;
use super::session_log::SessionLog;
use super::worktree::WorktreeManager;
use super::{log, ServeConfig, SlotState};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::mailbox;
use quorum_core::{error::Result, error::QuorumError};
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

    let turn = serde_json::json!({
        "type": "user",
        "message": { "content": content }
    });
    turn.to_string()
}

pub(crate) async fn recover(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    workers: &mut Vec<SlotState>,
    reviewers: &mut Vec<SlotState>,
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
        entries.len()
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
        // Reclaim the name
        if !name_pool.reclaim(&entry.agent) {
            log(&format!(
                "recovery: name {} not in pool — skipping (journal stale?)",
                entry.agent
            ));
            // Clean up the stale journal entry
            let p = db_path.clone();
            let agent = entry.agent.clone();
            tokio::task::spawn_blocking(move || {
                if let Ok(mut conn) = quorum_core::db::open(&p) {
                    let _ = journal::delete(&mut conn, &agent);
                }
            })
            .await
            .ok();
            continue;
        }

        if let Some(ref wt) = entry.worktree {
            active_worktrees.push(wt.clone());
        }

        match entry.role.as_str() {
            "reviewer" => {
                // Reviewers are ephemeral — tear down, don't resume.
                // Phase 5 will spawn fresh ones if workers have PRs.
                log(&format!(
                    "recovery: tearing down stale reviewer {} (task #{:?})",
                    entry.agent,
                    entry.task_id
                ));
                if let Some(ref wt) = entry.worktree {
                    let wt_path = PathBuf::from(wt);
                    wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
                    // Remove from active since we're cleaning it up
                    active_worktrees.retain(|w| w != wt);
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
                            release_and_cleanup(config, name_pool, &entry.agent, entry.task_id)
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
                        release_and_cleanup(config, name_pool, &entry.agent, entry.task_id).await;
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

                // Spawn with --resume
                let spec = AgentSpec {
                    model: config.model.clone(),
                    effort: config.effort.clone(),
                    session_id: entry.session_id.clone(),
                    worktree: wt_path.clone(),
                    bare: config.bare_agent,
                    resume: true,
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
                                    name_pool,
                                    &entry.agent,
                                    entry.task_id,
                                )
                                .await;
                                continue;
                            }
                        }

                        // Re-open session log if log_dir is known
                        let session_log = entry.log_dir.as_ref().and_then(|ld| {
                            SessionLog::reopen(&PathBuf::from(ld)).ok()
                        });

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
                        release_and_cleanup(config, name_pool, &entry.agent, entry.task_id).await;
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
        "recovery: complete — {} worker(s), {} reviewer(s) in flight",
        workers.len(),
        reviewers.len()
    ));

    Ok(())
}

async fn release_and_cleanup(
    config: &ServeConfig,
    name_pool: &mut Pool,
    agent: &str,
    task_id: Option<i64>,
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
