//! `quorum serve` — the agent-manager daemon.
//!
//! Builds a tokio runtime and runs an async tick loop that polls the mailbox,
//! spawns/drives agents, and shuts down cleanly on Ctrl-C. See spec §3.

pub mod agent;
pub mod merge;
pub mod names;
pub mod reviewer;
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
use std::sync::Arc;
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
    pub merge_executor: Arc<dyn merge::MergeExecutor>,
    /// Pass `--bare` to spawned agents, stripping operator-local hooks,
    /// plugins, memory, and MCP config. Default: true.
    pub bare_agent: bool,
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
    pr: Option<i64>,
    rework_count: u32,
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
    let mut worker: Option<SlotState> = None;
    let mut reviewer: Option<SlotState> = None;

    log(&format!("serving (cap={})", config.cap));

    loop {
        if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            log("shutting down (Ctrl-C)");
            if let Some(r) = reviewer.take() {
                teardown_reviewer(&config, &wt_mgr, &mut name_pool, r).await;
            }
            if let Some(w) = worker.take() {
                teardown_worker(&config, &wt_mgr, &mut name_pool, w, "open").await;
            }
            return Ok(());
        }
        if let Err(e) = tick(&config, &wt_mgr, &mut name_pool, &mut worker, &mut reviewer).await {
            log(&format!("tick error: {e}"));
        }
    }
}

async fn tick(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    worker: &mut Option<SlotState>,
    reviewer: &mut Option<SlotState>,
) -> Result<()> {
    let db_path = config.db_path.clone();

    // ── Phase 1: Poll mailbox ───────────────────────────────────────────
    let mailbox_rows = {
        let p = db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<Vec<(i64, mailbox::MailboxRow)>> {
            let conn = quorum_core::db::open(&p)?;
            mailbox::poll_unconsumed(&conn)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
    }?;

    // ── Phase 2: Process mailbox rows ─────────────────────────────────
    for (id, row) in &mailbox_rows {
        // F3/F9: Consume vestigial non-Done kinds (TaskUpdate, Message).
        // The daemon does not implement handlers for these — agents use the
        // CLI (e.g. `task-update`) which writes directly to the DB.
        if row.kind != MailboxKind::Done {
            log(&format!(
                "consuming unhandled {:?} mailbox row from {} (not processed by daemon)",
                row.kind, row.agent
            ));
            if !consume_mailbox_row(&db_path, *id).await {
                break;
            }
            continue;
        }

        // ── Done row handling ──
        // F13: include note in log when present.
        let note_suffix = row
            .note
            .as_ref()
            .map(|n| format!(", summary={n:?}"))
            .unwrap_or_default();

        // Check reviewer match first (verdict handling)
        if let Some(ref r) = reviewer {
            if row.agent == r.agent_name {
                log(&format!(
                    "reviewer {} done (pr={:?}, verdict={:?}{note_suffix})",
                    r.agent_name, row.pr, row.verdict
                ));

                // F8: action runs BEFORE consume — if the action fails,
                // the row stays unconsumed for diagnostic visibility.
                match row.verdict.as_deref() {
                    Some("approved") => {
                        let pr_num = row.pr.unwrap_or(0);
                        log(&format!("verdict: approved — merging PR #{pr_num}"));
                        let merge_result = {
                            let repo = config.repo_dir.clone();
                            let executor = Arc::clone(&config.merge_executor);
                            tokio::task::spawn_blocking(move || executor.merge(pr_num, &repo))
                                .await
                                .map_err(|e| {
                                    QuorumError::Io(format!("merge spawn_blocking join: {e}"))
                                })?
                        };

                        if merge_result.success {
                            log(&format!("PR #{pr_num} merged — tearing down both agents"));
                            if let Some(r) = reviewer.take() {
                                teardown_reviewer(config, wt_mgr, name_pool, r).await;
                            }
                            if let Some(w) = worker.take() {
                                teardown_worker(config, wt_mgr, name_pool, w, "done").await;
                            }
                        } else {
                            log(&format!(
                                "PR #{pr_num} merge failed: {} — treating as changes",
                                merge_result.message
                            ));
                            if let Some(r) = reviewer.take() {
                                teardown_reviewer(config, wt_mgr, name_pool, r).await;
                            }
                            if let Some(ref mut w) = worker {
                                let rework_msg = format!(
                                    "Merge of PR #{pr_num} failed: {}\n\n\
                                     Fix the issue and push again.",
                                    merge_result.message
                                );
                                let rework_turn = reviewer::build_rework_turn(&rework_msg);
                                if let Err(e) = w.proc.feed_turn(&rework_turn).await {
                                    log(&format!(
                                        "merge-failure rework feed failed: {e} — \
                                         tearing down broken worker"
                                    ));
                                    if let Some(w) = worker.take() {
                                        teardown_worker(config, wt_mgr, name_pool, w, "open")
                                            .await;
                                    }
                                } else {
                                    w.draining = true;
                                    w.pr = None;
                                    w.rework_count += 1;

                                    let p = db_path.clone();
                                    let entry = JournalEntry {
                                        agent: w.agent_name.clone(),
                                        role: "worker".into(),
                                        task_id: Some(w.task_id),
                                        session_id: w.session_id.clone(),
                                        worktree: Some(w.worktree_path.to_string_lossy().into()),
                                        branch: Some(w.branch.clone()),
                                        phase: "working".into(),
                                        expected_signal: Some("done".into()),
                                        cost_tokens: 0,
                                    };
                                    tokio::task::spawn_blocking(move || -> Result<()> {
                                        let mut conn = quorum_core::db::open(&p)?;
                                        journal::upsert(&mut conn, &entry)
                                    })
                                    .await
                                    .map_err(|e| {
                                        QuorumError::Io(format!("spawn_blocking join: {e}"))
                                    })?
                                    .ok();

                                    log(&format!(
                                        "worker {} rework #{} (merge failure)",
                                        w.agent_name, w.rework_count
                                    ));
                                }
                            }
                        }
                    }
                    Some("changes") => {
                        let feedback = row.feedback.as_deref().unwrap_or("Changes requested.");
                        log(&format!(
                            "verdict: changes — feeding rework to worker (feedback: {feedback})"
                        ));

                        if let Some(r) = reviewer.take() {
                            teardown_reviewer(config, wt_mgr, name_pool, r).await;
                        }

                        if let Some(ref mut w) = worker {
                            let rework_turn = reviewer::build_rework_turn(feedback);
                            if let Err(e) = w.proc.feed_turn(&rework_turn).await {
                                log(&format!(
                                    "rework feed_turn failed: {e} — \
                                     tearing down broken worker"
                                ));
                                if let Some(w) = worker.take() {
                                    teardown_worker(config, wt_mgr, name_pool, w, "open").await;
                                }
                            } else {
                                w.draining = true;
                                w.pr = None;
                                w.rework_count += 1;

                                let p = db_path.clone();
                                let entry = JournalEntry {
                                    agent: w.agent_name.clone(),
                                    role: "worker".into(),
                                    task_id: Some(w.task_id),
                                    session_id: w.session_id.clone(),
                                    worktree: Some(w.worktree_path.to_string_lossy().into()),
                                    branch: Some(w.branch.clone()),
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

                                log(&format!(
                                    "worker {} rework #{} started",
                                    w.agent_name, w.rework_count
                                ));
                            }
                        }
                    }
                    _ => {
                        log(&format!(
                            "reviewer {} done without verdict — tearing down reviewer only",
                            row.agent
                        ));
                        if let Some(r) = reviewer.take() {
                            teardown_reviewer(config, wt_mgr, name_pool, r).await;
                        }
                    }
                }

                // F8: consume AFTER the action has completed.
                if !consume_mailbox_row(&db_path, *id).await {
                    break;
                }
                break;
            }
        }

        // Check worker match
        if let Some(ref w) = worker {
            if row.agent == w.agent_name {
                log(&format!(
                    "worker {} done (pr={:?}{note_suffix})",
                    w.agent_name, row.pr,
                ));

                if let Some(pr) = row.pr {
                    if let Some(ref mut w) = worker {
                        w.pr = Some(pr);
                        log(&format!(
                            "worker {} PR #{} ready for review",
                            w.agent_name, pr
                        ));
                    }
                } else {
                    if let Some(w) = worker.take() {
                        teardown_worker(config, wt_mgr, name_pool, w, "done").await;
                    }
                }

                // F8: consume AFTER the action has completed.
                if !consume_mailbox_row(&db_path, *id).await {
                    break;
                }
                break;
            }
        }

        // F9: Done row matches neither worker nor reviewer — consume it to
        // prevent infinite re-polling. With name reuse, a stale verdict left
        // unconsumed could be applied to a NEW agent that acquired the same
        // name (phantom completion).
        log(&format!(
            "consuming unmatched Done row from {} (matches no active agent)",
            row.agent
        ));
        if !consume_mailbox_row(&db_path, *id).await {
            break;
        }
    }

    // ── Phase 3: Drain events from active reviewer ──────────────────────
    if let Some(ref mut r) = reviewer {
        if r.draining {
            drain_events(r, &db_path, "reviewer").await?;
        }
    }

    // ── Phase 4: Drain events from active worker ────────────────────────
    if let Some(ref mut w) = worker {
        if w.draining {
            drain_events(w, &db_path, "worker").await?;
        }
    }

    // ── Phase 4b: Detect dead workers/reviewers ─────────────────────────
    // A crashed or exited agent process leaves the slot pinned: the task is
    // never released, the name/worktree leak, and Phase 5 (which gates on
    // !w.draining) never spawns a reviewer. `next_event`/`drain_events` alone
    // cannot detect this — stdout EOF is a hint but a stuck child can hold
    // its stdout open. `try_wait` is the authoritative signal.
    let worker_dead = worker.as_mut().and_then(|w| match w.proc.try_wait() {
        Ok(Some(status)) => Some(status),
        Ok(None) => None,
        Err(e) => {
            log(&format!("worker {} try_wait error: {e}", w.agent_name));
            None
        }
    });
    if let Some(status) = worker_dead {
        if let Some(dead) = worker.take() {
            log(&format!(
                "worker {} died mid-task (task #{}, status={:?}) — releasing task/name/worktree",
                dead.agent_name, dead.task_id, status
            ));
            teardown_worker(config, wt_mgr, name_pool, dead, "open").await;
        }
    }

    let reviewer_dead = reviewer.as_mut().and_then(|r| match r.proc.try_wait() {
        Ok(Some(status)) => Some(status),
        Ok(None) => None,
        Err(e) => {
            log(&format!("reviewer {} try_wait error: {e}", r.agent_name));
            None
        }
    });
    if let Some(status) = reviewer_dead {
        if let Some(dead) = reviewer.take() {
            log(&format!(
                "reviewer {} died (status={:?}) — releasing name/worktree",
                dead.agent_name, status
            ));
            teardown_reviewer(config, wt_mgr, name_pool, dead).await;
        }
    }

    // ── Phase 5: Spawn reviewer if worker has PR and no reviewer ────────
    if reviewer.is_none() {
        if let Some(ref w) = worker {
            if let Some(pr) = w.pr {
                if !w.draining {
                    spawn_reviewer_for_worker(config, wt_mgr, name_pool, reviewer, pr, w).await?;
                }
            }
        }
    }

    // ── Phase 6: Spawn worker if slot empty and under cap ───────────────
    if worker.is_none() && name_pool.in_use_count() < config.cap {
        spawn_worker(config, wt_mgr, name_pool, worker).await?;
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}

/// Consume a mailbox row. Returns false on failure (caller should break and retry next tick).
async fn consume_mailbox_row(db_path: &std::path::Path, id: i64) -> bool {
    let p = db_path.to_path_buf();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        mailbox::mark_consumed(&mut conn, id)
    })
    .await;

    match result {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            log(&format!(
                "mark_consumed failed for mailbox row {id}: {e}; retrying next tick"
            ));
            false
        }
        Err(e) => {
            log(&format!(
                "mark_consumed join error for mailbox row {id}: {e}"
            ));
            false
        }
    }
}

/// Drain stream events from an agent slot (bounded per tick, 5s timeout).
async fn drain_events(slot: &mut SlotState, db_path: &std::path::Path, role: &str) -> Result<()> {
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_secs(5), slot.proc.next_event()).await
    {
        match &event {
            stream::Event::Result { usage, .. } => {
                let tokens = usage
                    .as_ref()
                    .map_or(0, |u| (u.input_tokens + u.output_tokens) as i64);
                log(&format!(
                    "{role} {} result (tokens={})",
                    slot.agent_name, tokens
                ));

                let p = db_path.to_path_buf();
                let phase = if role == "worker" {
                    "awaiting-review"
                } else {
                    "reviewing"
                };
                let entry = JournalEntry {
                    agent: slot.agent_name.clone(),
                    role: role.into(),
                    task_id: Some(slot.task_id),
                    session_id: slot.session_id.clone(),
                    worktree: Some(slot.worktree_path.to_string_lossy().into()),
                    branch: Some(slot.branch.clone()),
                    phase: phase.into(),
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

                slot.draining = false;
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
                    log(&format!("{role} {}: {preview}", slot.agent_name));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn spawn_reviewer_for_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    reviewer_slot: &mut Option<SlotState>,
    pr: i64,
    worker: &SlotState,
) -> Result<()> {
    let reviewer_name = match name_pool.acquire() {
        Some(n) => n,
        None => {
            log("no name available for reviewer");
            return Ok(());
        }
    };

    // F9: drain stale mailbox rows for this name to prevent phantom verdicts.
    {
        let p = config.db_path.clone();
        let name = reviewer_name.clone();
        let stale = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut conn = quorum_core::db::open(&p)?;
            mailbox::consume_all_for_agent(&mut conn, &name)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
        .unwrap_or(0);
        if stale > 0 {
            log(&format!(
                "consumed {stale} stale mailbox row(s) for {reviewer_name}"
            ));
        }
    }

    log(&format!(
        "spawning reviewer {} for PR #{} (worker {})",
        reviewer_name, pr, worker.agent_name
    ));

    let session_id = uuid::Uuid::new_v4().to_string();
    let branch = reviewer::reviewer_branch(pr, &reviewer_name);
    let wt_path = reviewer::reviewer_worktree_path(&config.worktree_base, pr, &reviewer_name);

    match wt_mgr
        .provision(&config.repo_dir, &branch, &wt_path, "origin/main")
        .await
    {
        Ok(_) => {
            log(&format!(
                "reviewer worktree provisioned at {}",
                wt_path.display()
            ));
        }
        Err(e) => {
            log(&format!("reviewer worktree provision failed: {e}"));
            name_pool.release(&reviewer_name);
            return Ok(());
        }
    }

    // Journal: phase=reviewing, role=reviewer
    let p = config.db_path.clone();
    let entry = JournalEntry {
        agent: reviewer_name.clone(),
        role: "reviewer".into(),
        task_id: Some(worker.task_id),
        session_id: session_id.clone(),
        worktree: Some(wt_path.to_string_lossy().into()),
        branch: Some(branch.clone()),
        phase: "reviewing".into(),
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

    let spec = reviewer::ReviewerSpec {
        pr,
        worker_agent: worker.agent_name.clone(),
        reviewer_name: reviewer_name.clone(),
        task_id: worker.task_id,
        branch: worker.branch.clone(),
    };

    match reviewer::spawn_reviewer(
        &spec,
        &config.model,
        &config.effort,
        &session_id,
        &wt_path,
        config.agent_bin.as_deref(),
        config.bare_agent,
    )
    .await
    {
        Ok(mut proc) => {
            let prompt = reviewer::build_review_prompt(&spec);
            let turn1 = serde_json::json!({
                "type": "user",
                "message": { "content": prompt }
            });
            if let Err(e) = proc.feed_turn(&turn1.to_string()).await {
                log(&format!("reviewer feed_turn failed: {e}"));
                proc.kill_and_reap().await;
                name_pool.release(&reviewer_name);
                wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
                // Delete journal entry for the reviewer
                let p = config.db_path.clone();
                let rn = reviewer_name.clone();
                tokio::task::spawn_blocking(move || {
                    if let Ok(mut conn) = quorum_core::db::open(&p) {
                        let _ = journal::delete(&mut conn, &rn);
                    }
                })
                .await
                .ok();
                return Ok(());
            }

            *reviewer_slot = Some(SlotState {
                agent_name: reviewer_name,
                proc,
                task_id: worker.task_id,
                session_id,
                worktree_path: wt_path,
                branch,
                draining: true,
                pr: Some(pr),
                rework_count: 0,
            });
        }
        Err(e) => {
            log(&format!("reviewer spawn failed: {e}"));
            name_pool.release(&reviewer_name);
            wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
            let p = config.db_path.clone();
            let rn = reviewer_name.clone();
            tokio::task::spawn_blocking(move || {
                if let Ok(mut conn) = quorum_core::db::open(&p) {
                    let _ = journal::delete(&mut conn, &rn);
                }
            })
            .await
            .ok();
        }
    }

    Ok(())
}

async fn spawn_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    worker: &mut Option<SlotState>,
) -> Result<()> {
    let db_path = config.db_path.clone();
    let p = db_path.clone();
    let ready_task = tokio::task::spawn_blocking(move || -> Result<Option<tasks::Task>> {
        let conn = quorum_core::db::open(&p)?;
        let open = tasks::list(&conn, Some("open"), None, None)?;
        Ok(open.into_iter().find(|t| t.ready))
    })
    .await
    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))??;

    let task = match ready_task {
        Some(t) => t,
        None => return Ok(()),
    };

    let agent_name = match name_pool.acquire() {
        Some(n) => n,
        None => return Ok(()),
    };

    // F9: drain stale mailbox rows for this name to prevent phantom verdicts.
    {
        let p = db_path.clone();
        let name = agent_name.clone();
        let stale = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut conn = quorum_core::db::open(&p)?;
            mailbox::consume_all_for_agent(&mut conn, &name)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
        .unwrap_or(0);
        if stale > 0 {
            log(&format!(
                "consumed {stale} stale mailbox row(s) for {agent_name}"
            ));
        }
    }

    log(&format!(
        "spawning agent {} for task #{} ({})",
        agent_name, task.id, task.title
    ));

    // Claim the task atomically (open → claimed)
    let p = db_path.clone();
    let claim_agent = agent_name.clone();
    let claim_task_id = task.id;
    let claimed = tokio::task::spawn_blocking(move || -> Result<Option<tasks::Task>> {
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

    let spec = AgentSpec {
        model: config.model.clone(),
        effort: config.effort.clone(),
        session_id: session_id.clone(),
        worktree: wt_path.clone(),
        allowlist: vec![],
        bare: config.bare_agent,
    };
    match AgentProc::spawn(&spec, config.agent_bin.as_deref()) {
        Ok(mut proc) => {
            let body = task.body.as_deref().unwrap_or(&task.title);
            let turn1 = reviewer::build_worker_turn(&agent_name, task.id, &task.title, body);
            if let Err(e) = proc.feed_turn(&turn1).await {
                log(&format!("feed_turn failed: {e}"));
                proc.kill_and_reap().await;
                release_task(&db_path, &agent_name, task.id).await;
                name_pool.release(&agent_name);
                wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
                return Ok(());
            }

            *worker = Some(SlotState {
                agent_name,
                proc,
                task_id: task.id,
                session_id,
                worktree_path: wt_path,
                branch,
                draining: true,
                pr: None,
                rework_count: 0,
            });
        }
        Err(e) => {
            log(&format!("agent spawn failed: {e}"));
            release_task(&db_path, &agent_name, task.id).await;
            name_pool.release(&agent_name);
            wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
        }
    }

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

/// Tear down a worker agent: kill process, update task, clean up journal/worktree/name.
async fn teardown_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    state: SlotState,
    task_status: &str,
) {
    log(&format!(
        "tearing down worker {} (task #{} -> {task_status})",
        state.agent_name, state.task_id
    ));

    state.proc.kill_and_reap().await;

    let p = config.db_path.clone();
    let agent = state.agent_name.clone();
    let task_id = state.task_id;
    let status = task_status.to_string();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        let fields = tasks::TaskUpdate {
            status: Some(&status),
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

    wt_mgr
        .remove(&config.repo_dir, &state.worktree_path)
        .await
        .ok();

    name_pool.release(&state.agent_name);
    log(&format!("worker {} torn down", state.agent_name));
}

/// Tear down a reviewer agent: kill process, clean up journal/worktree/name (no task update).
async fn teardown_reviewer(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    state: SlotState,
) {
    log(&format!("tearing down reviewer {}", state.agent_name));

    state.proc.kill_and_reap().await;

    let p = config.db_path.clone();
    let agent = state.agent_name.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        journal::delete(&mut conn, &agent)?;
        Ok(())
    })
    .await
    .ok();

    wt_mgr
        .remove(&config.repo_dir, &state.worktree_path)
        .await
        .ok();

    name_pool.release(&state.agent_name);
    log(&format!("reviewer {} torn down", state.agent_name));
}
