//! `quorum serve` — the agent-manager daemon.
//!
//! Builds a tokio runtime and runs an async tick loop that polls the mailbox,
//! spawns/drives agents, and shuts down cleanly on Ctrl-C. See spec §3.

pub mod agent;
pub mod names;
pub mod stream;
pub mod worktree;

use agent::{AgentProc, AgentSpec};
use names::Pool;
use quorum_core::error::{QuorumError, Result};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::mailbox::{self, MailboxKind};
use quorum_core::tasks;
use std::io::Write;
use std::path::PathBuf;
use worktree::WorktreeManager;

fn log(msg: &str) {
    let _ = writeln!(std::io::stderr(), "quorum serve: {msg}");
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Configuration for the daemon, resolved from CLI flags / config file.
pub struct ServeConfig {
    pub db_path: PathBuf,
    pub cap: usize,
    pub repo_dir: PathBuf,
    pub worktree_base: PathBuf,
    pub names_file: PathBuf,
    pub agent_bin: Option<String>,
    pub model: String,
    pub effort: String,
}

pub fn run_serve(config: ServeConfig) -> Result<()> {
    log(&format!("starting (cap={})", config.cap));

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| QuorumError::Io(format!("failed to create tokio runtime: {e}")))?;

    rt.block_on(tick_loop(config))
}

struct SlotState {
    agent_name: String,
    proc: AgentProc,
    task_id: i64,
    session_id: String,
    worktree_path: PathBuf,
    branch: String,
    draining: bool,
}

async fn tick_loop(config: ServeConfig) -> Result<()> {
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|e| QuorumError::Io(format!("failed to register SIGINT handler: {e}")))?;

    // SIGINT sets a flag; shutdown happens between ticks. Racing the signal against
    // tick() in a select! would cancel tick mid-flight at an await point, which can
    // leak a claimed task (claimed in the DB but slot never assigned, so teardown
    // has nothing to release) and orphan the spawned agent process. Ticks are
    // bounded (500ms idle sleep, 5s event timeout), so shutdown latency stays small.
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            sigint.recv().await;
            shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
        });
    }

    let mut name_pool = Pool::load(&config.names_file, config.cap)
        .map_err(|e| QuorumError::Io(format!("names pool: {e}")))?;

    let wt_mgr = WorktreeManager::new();
    let mut slot: Option<SlotState> = None;

    log(&format!("serving (cap={})", config.cap));

    loop {
        if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            log("shutting down (Ctrl-C)");
            if let Some(s) = slot.take() {
                // Work was interrupted, not completed — release the task back to open.
                teardown(&config, &wt_mgr, &mut name_pool, s, TaskOutcome::Release).await;
            }
            return Ok(());
        }
        if let Err(e) = tick(&config, &wt_mgr, &mut name_pool, &mut slot).await {
            log(&format!("tick error: {e}"));
        }
    }
}

async fn tick(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    slot: &mut Option<SlotState>,
) -> Result<()> {
    let db_path = config.db_path.clone();

    // Poll mailbox for Done rows
    let mailbox_rows = {
        let p = db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<(i64, mailbox::MailboxRow)>> {
            let conn = quorum_core::db::open(&p)?;
            mailbox::poll_unconsumed(&conn)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
    }?;

    // Process Done mailbox rows for our agent
    for (id, row) in &mailbox_rows {
        if row.kind == MailboxKind::Done {
            if let Some(ref s) = slot {
                if row.agent == s.agent_name {
                    log(&format!(
                        "agent {} done (pr={:?}, verdict={:?})",
                        s.agent_name, row.pr, row.verdict
                    ));

                    // Mark consumed. On failure, skip teardown and retry next tick —
                    // acting on an unacknowledged row would leave it behind to kill a
                    // future agent that reuses this name.
                    let p = db_path.clone();
                    let mid = *id;
                    let consumed = tokio::task::spawn_blocking(move || -> Result<()> {
                        let mut conn = quorum_core::db::open(&p)?;
                        mailbox::mark_consumed(&mut conn, mid)
                    })
                    .await
                    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?;
                    if let Err(e) = consumed {
                        log(&format!(
                            "mark_consumed failed for mailbox row {id}: {e}; retrying next tick"
                        ));
                        break;
                    }

                    // Teardown
                    if let Some(s) = slot.take() {
                        teardown(config, wt_mgr, name_pool, s, TaskOutcome::Done).await;
                    }
                    break;
                }
            }
        }
    }

    // If we have an active agent that's draining, pump events (bounded per tick)
    if let Some(ref mut s) = slot {
        if s.draining {
            while let Ok(Some(event)) =
                tokio::time::timeout(std::time::Duration::from_secs(5), s.proc.next_event()).await
            {
                match &event {
                    stream::Event::Result { usage, .. } => {
                        let tokens = usage
                            .as_ref()
                            .map_or(0, |u| (u.input_tokens + u.output_tokens) as i64);
                        log(&format!(
                            "agent {} result (tokens={})",
                            s.agent_name, tokens
                        ));

                        // Update journal to awaiting-review
                        let p = db_path.clone();
                        let entry = JournalEntry {
                            agent: s.agent_name.clone(),
                            role: "worker".into(),
                            task_id: Some(s.task_id),
                            session_id: s.session_id.clone(),
                            worktree: Some(s.worktree_path.to_string_lossy().into()),
                            branch: Some(s.branch.clone()),
                            phase: "awaiting-review".into(),
                            expected_signal: Some("done".into()),
                            cost_tokens: tokens,
                        };
                        tokio::task::spawn_blocking(move || -> Result<()> {
                            let mut conn = quorum_core::db::open(&p)?;
                            journal::upsert(&mut conn, &entry)
                        })
                        .await
                        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                        .ok();

                        // Done draining turn 1
                        s.draining = false;
                        break;
                    }
                    stream::Event::Assistant { message } => {
                        if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                            let preview = if content.len() > 80 {
                                let end = content
                                    .char_indices()
                                    .nth(80)
                                    .map_or(content.len(), |(i, _)| i);
                                format!("{}…", &content[..end])
                            } else {
                                content.to_string()
                            };
                            log(&format!("agent {}: {preview}", s.agent_name));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // If no slot occupied and under cap, try to pick up a task
    if slot.is_none() && name_pool.in_use_count() < config.cap {
        let p = db_path.clone();
        let ready_task = tokio::task::spawn_blocking(move || -> Result<Option<tasks::Task>> {
            let conn = quorum_core::db::open(&p)?;
            let open = tasks::list(&conn, Some("open"), None, None)?;
            Ok(open.into_iter().find(|t| t.ready))
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))??;

        if let Some(task) = ready_task {
            if let Some(agent_name) = name_pool.acquire() {
                log(&format!(
                    "spawning agent {} for task #{} ({})",
                    agent_name, task.id, task.title
                ));

                // Claim the task atomically (open → claimed)
                let p = db_path.clone();
                let claim_agent = agent_name.clone();
                let claim_task_id = task.id;
                let claimed =
                    tokio::task::spawn_blocking(move || -> Result<Option<tasks::Task>> {
                        let mut conn = quorum_core::db::open(&p)?;
                        let now = now_unix();
                        tasks::claim(
                            &mut conn,
                            &claim_agent,
                            Some(claim_task_id),
                            &[],
                            tasks::DEFAULT_LEASE_TTL_SECS,
                            now,
                        )
                    })
                    .await
                    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?;

                match claimed {
                    Ok(None) => {
                        log(&format!("task #{} already claimed, skipping", task.id));
                        name_pool.release(&agent_name);
                        return Ok(());
                    }
                    Err(e) => {
                        log(&format!("task #{} claim failed: {e}", task.id));
                        name_pool.release(&agent_name);
                        return Ok(());
                    }
                    Ok(Some(_)) => {}
                }

                let session_id = uuid::Uuid::new_v4().to_string();
                let branch = format!("daemon/{}-t{}", agent_name.to_lowercase(), task.id);
                let wt_path = config
                    .worktree_base
                    .join(format!("{}-t{}", agent_name, task.id));

                // Provision worktree
                match wt_mgr
                    .provision(&config.repo_dir, &branch, &wt_path, "origin/main")
                    .await
                {
                    Ok(_) => {
                        log(&format!("worktree provisioned at {}", wt_path.display()));
                    }
                    Err(e) => {
                        log(&format!("worktree provision failed: {e}"));
                        release_task(&db_path, &agent_name, task.id).await;
                        name_pool.release(&agent_name);
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        return Ok(());
                    }
                }

                // Journal: phase=working
                let p = config.db_path.clone();
                let entry = JournalEntry {
                    agent: agent_name.clone(),
                    role: "worker".into(),
                    task_id: Some(task.id),
                    session_id: session_id.clone(),
                    worktree: Some(wt_path.to_string_lossy().into()),
                    branch: Some(branch.clone()),
                    phase: "working".into(),
                    expected_signal: Some("done".into()),
                    cost_tokens: 0,
                };
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut conn = quorum_core::db::open(&p)?;
                    journal::upsert(&mut conn, &entry)
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                .ok();

                // Spawn agent
                let spec = AgentSpec {
                    model: config.model.clone(),
                    effort: config.effort.clone(),
                    session_id: session_id.clone(),
                    worktree: wt_path.clone(),
                    allowlist: vec![],
                };
                match AgentProc::spawn(&spec, config.agent_bin.as_deref()) {
                    Ok(mut proc) => {
                        // Feed turn 1
                        let body = task.body.as_deref().unwrap_or(&task.title);
                        let turn1 = serde_json::json!({
                            "type": "user",
                            "message": {
                                "content": format!(
                                    "You are agent {}. Task #{}: {}\n\n{}",
                                    agent_name, task.id, task.title, body
                                )
                            }
                        });
                        if let Err(e) = proc.feed_turn(&turn1.to_string()).await {
                            log(&format!("feed_turn failed: {e}"));
                            proc.kill_and_reap().await;
                            release_task(&db_path, &agent_name, task.id).await;
                            name_pool.release(&agent_name);
                            wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
                            return Ok(());
                        }

                        *slot = Some(SlotState {
                            agent_name,
                            proc,
                            task_id: task.id,
                            session_id,
                            worktree_path: wt_path,
                            branch,
                            draining: true,
                        });
                    }
                    Err(e) => {
                        log(&format!("agent spawn failed: {e}"));
                        release_task(&db_path, &agent_name, task.id).await;
                        name_pool.release(&agent_name);
                        wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
                    }
                }
            }
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}

async fn release_task(db_path: &std::path::Path, agent: &str, task_id: i64) {
    let p = db_path.to_path_buf();
    let a = agent.to_string();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        let fields = tasks::TaskUpdate {
            status: Some("open"),
            body: None,
            refs: None,
            verdict: None,
        };
        tasks::update(&mut conn, &a, task_id, &fields, now)?;
        Ok(())
    })
    .await
    .ok();
}

/// Final disposition of the slot's task when tearing down its agent.
enum TaskOutcome {
    /// The agent signaled done — mark the task done.
    Done,
    /// Work was interrupted (e.g. daemon shutdown) — release the task back to open.
    Release,
}

async fn teardown(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    state: SlotState,
    outcome: TaskOutcome,
) {
    let status = match outcome {
        TaskOutcome::Done => "done",
        TaskOutcome::Release => "open",
    };
    log(&format!(
        "tearing down agent {} (task #{} -> {status})",
        state.agent_name, state.task_id
    ));

    // Kill the process and reap to avoid zombies
    state.proc.kill_and_reap().await;

    // Move the task to its final status (claimed → done, or claimed → open on release)
    let p = config.db_path.clone();
    let agent = state.agent_name.clone();
    let task_id = state.task_id;
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        let fields = tasks::TaskUpdate {
            status: Some(status),
            body: None,
            refs: None,
            verdict: None,
        };
        tasks::update(&mut conn, &agent, task_id, &fields, now)?;
        journal::delete(&mut conn, &agent)?;
        Ok(())
    })
    .await
    .ok();

    // Remove worktree
    wt_mgr
        .remove(&config.repo_dir, &state.worktree_path)
        .await
        .ok();

    // Release name
    name_pool.release(&state.agent_name);

    log(&format!("agent {} torn down", state.agent_name));
}
