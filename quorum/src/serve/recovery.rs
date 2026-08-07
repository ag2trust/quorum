//! Stateless crash recovery on daemon restart.
//!
//! On startup:
//! 1. Kill all stale process groups (from journal PIDs)
//! 2. Delete all journal entries (stale by definition after a crash)
//! 3. GC all worktrees from the previous run
//! 4. Scan non-terminal tasks and reset them to states the normal tick
//!    loop can handle:
//!    - `working` / `rework` → AgentFailed → open (Phase 6 re-spawns)
//!    - `merging` → AgentFailed → in-review (Phase 5 spawns reviewer)
//!    - `in-review` → left as-is (Phase 5 spawns reviewer)
//!
//! No resume, no special names, no PendingReview reconstruction.

use super::worktree::WorktreeManager;
use super::{log, ServeConfig};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::lifecycle::Event;
use quorum_core::{approvals, error::QuorumError, error::Result, tasks};

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

pub(crate) async fn recover(config: &ServeConfig, wt_mgr: &WorktreeManager) -> Result<()> {
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

    // Decomposition planner views are ordinary temporary archives rather
    // than Git worktrees. Their exact path is journaled with the provider
    // process so abrupt daemon death cannot leak the frozen repository view.
    remove_journaled_decomposition_views(&entries);

    // ── Phase 1b: Revoke run capabilities for stale agents (#130) ──────
    if !entries.is_empty() {
        let p = db_path.clone();
        let agents: Vec<String> = entries.iter().map(|e| e.agent.clone()).collect();
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

    // ── Phase 2: Delete all journal entries ─────────────────────────────
    {
        let p = db_path.clone();
        let count = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut conn = quorum_core::db::open(&p)?;
            journal::delete_all(&mut conn)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
        .unwrap_or(0);
        if count > 0 {
            log(&format!("recovery: deleted {count} stale journal entries"));
        }
    }

    // ── Phase 3: GC all worktrees ──────────────────────────────────────
    let removed = wt_mgr
        .gc_orphaned(&config.repo_dir, &config.worktree_base, &[])
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
