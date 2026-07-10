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
use quorum_core::{error::QuorumError, error::Result, tasks};

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

    // merging → AgentFailed → in-review
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
