//! `quorum serve` — the agent-manager daemon.
//!
//! Builds a tokio runtime and runs an async tick loop that polls the mailbox,
//! spawns/drives agents, and shuts down cleanly on Ctrl-C. See spec §3.

pub mod agent;
pub mod merge;
pub mod names;
pub mod reviewer;
pub mod session_log;
pub mod stream;
pub mod worktree;

use agent::{AgentProc, AgentSpec};
use names::Pool;
use quorum_core::error::{QuorumError, Result};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::mailbox;
use quorum_core::tasks;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use worktree::WorktreeManager;

fn log(msg: &str) {
    let _ = writeln!(std::io::stderr(), "quorum serve: {msg}");
}

/// Extract model and effort overrides from a task's labels JSON.
///
/// Labels like `tier:opus-46` map to model `opus-46`; `effort:high` maps to effort `high`.
/// Returns (model_override, effort_override) — `None` means "use the global config value".
fn labels_to_model_effort(labels_json: Option<&str>) -> (Option<String>, Option<String>) {
    let json = match labels_json {
        Some(s) => s,
        None => return (None, None),
    };
    let arr: Vec<String> = match serde_json::from_str(json) {
        Ok(a) => a,
        Err(_) => return (None, None),
    };
    let mut model = None;
    let mut effort = None;
    for label in &arr {
        if model.is_none() {
            if let Some(val) = label.strip_prefix("tier:") {
                if !val.is_empty() {
                    model = Some(val.to_string());
                }
            }
        }
        if effort.is_none() {
            if let Some(val) = label.strip_prefix("effort:") {
                if !val.is_empty() {
                    effort = Some(val.to_string());
                }
            }
        }
    }
    (model, effort)
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn slot_journal_entry(slot: &SlotState, role: &str, phase: &str) -> JournalEntry {
    JournalEntry {
        agent: slot.agent_name.clone(),
        role: role.into(),
        task_id: Some(slot.task_id),
        session_id: slot.session_id.clone(),
        worktree: Some(slot.worktree_path.to_string_lossy().into()),
        branch: Some(slot.branch.clone()),
        phase: phase.into(),
        cost_tokens: slot.cost_tokens,
        agent_state: slot.agent_state.clone(),
        cost_usd: slot.cost_usd,
        log_dir: slot
            .session_log
            .as_ref()
            .map(|l| l.dir().to_string_lossy().into()),
    }
}

/// Per-turn / per-task ceilings. `None` = unlimited.
/// All limits fail-closed: exceeding kills the agent and releases the task.
#[derive(Debug, Clone, Default)]
pub struct CostLimits {
    pub max_turn_tokens: Option<i64>,
    pub max_task_tokens: Option<i64>,
    pub max_turn_cost_usd: Option<f64>,
    pub max_task_cost_usd: Option<f64>,
    pub max_turn_wall_secs: Option<u64>,
    pub max_task_wall_secs: Option<u64>,
    pub max_rework_rounds: Option<u32>,
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
    pub limits: CostLimits,
    /// Directory for per-agent session logs (stream.jsonl, transcript.md, meta.json).
    pub log_dir: Option<PathBuf>,
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
    cost_tokens: i64,
    cost_usd: f64,
    task_started_at: std::time::Instant,
    turn_started_at: std::time::Instant,
    agent_state: Option<String>,
    session_log: Option<session_log::SessionLog>,
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

    if let Some(ref log_dir) = config.log_dir {
        let max_age = 7 * 24 * 3600; // 7 days
        match session_log::sweep_logs(log_dir, max_age) {
            Ok(0) => {}
            Ok(n) => log(&format!(
                "swept {n} old session log(s) from {}",
                log_dir.display()
            )),
            Err(e) => log(&format!("log sweep warning: {e}")),
        }
    }

    let wt_mgr = WorktreeManager::new();
    let mut workers: Vec<SlotState> = Vec::new();
    let mut reviewers: Vec<SlotState> = Vec::new();

    log(&format!("serving (cap={})", config.cap));

    loop {
        if shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            log("shutting down (Ctrl-C)");
            // Tear down reviewers first, then workers (spec: reviewer before worker).
            for r in reviewers.drain(..) {
                teardown_reviewer(&config, &wt_mgr, &mut name_pool, r).await;
            }
            for w in workers.drain(..) {
                teardown_worker(&config, &wt_mgr, &mut name_pool, w, "open").await;
            }
            return Ok(());
        }
        if let Err(e) = tick(
            &config,
            &wt_mgr,
            &mut name_pool,
            &mut workers,
            &mut reviewers,
        )
        .await
        {
            log(&format!("tick error: {e}"));
        }
    }
}

async fn tick(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    workers: &mut Vec<SlotState>,
    reviewers: &mut Vec<SlotState>,
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
    // Partition by kind: done rows process first (one-per-tick), task_update
    // rows process inline (lightweight), message rows defer to Phase 4c
    // (deliver at idle after draining).
    let mut pending_messages: Vec<(i64, &mailbox::MailboxRow)> = Vec::new();

    for (id, row) in &mailbox_rows {
        // M5: task_update rows — track agent state reactions.
        if row.kind == mailbox::MailboxKind::TaskUpdate {
            if let Some(wi) = workers.iter_mut().position(|w| w.agent_name == row.agent) {
                let state_str = row.note.as_deref().unwrap_or("note");
                workers[wi].agent_state = Some(state_str.to_string());
                log(&format!(
                    "worker {} state: {state_str} (task #{})",
                    workers[wi].agent_name, workers[wi].task_id,
                ));
                let p = db_path.clone();
                let phase = if workers[wi].draining {
                    "working"
                } else {
                    "awaiting-review"
                };
                let entry = slot_journal_entry(&workers[wi], "worker", phase);
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut conn = quorum_core::db::open(&p)?;
                    journal::upsert(&mut conn, &entry)
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                .ok();
            } else {
                log(&format!(
                    "consuming unmatched task_update from {} (no active worker)",
                    row.agent
                ));
            }
            if !consume_mailbox_row(&db_path, *id).await {
                break;
            }
            continue;
        }

        // M5: message rows — collect for delivery at idle (Phase 4c).
        if row.kind == mailbox::MailboxKind::Message {
            pending_messages.push((*id, row));
            continue;
        }

        // kind=done — existing lifecycle processing below.
        let note_suffix = row
            .note
            .as_ref()
            .map(|n| format!(", summary={n:?}"))
            .unwrap_or_default();

        // Check reviewer match first (verdict handling).
        let reviewer_idx = reviewers.iter().position(|r| r.agent_name == row.agent);
        if let Some(ri) = reviewer_idx {
            let reviewer_task_id = reviewers[ri].task_id;
            log(&format!(
                "reviewer {} done (pr={:?}, verdict={:?}{note_suffix})",
                reviewers[ri].agent_name, row.pr, row.verdict
            ));

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
                        let r = reviewers.remove(ri);
                        teardown_reviewer(config, wt_mgr, name_pool, r).await;
                        if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id)
                        {
                            let w = workers.remove(wi);
                            teardown_worker(config, wt_mgr, name_pool, w, "done").await;
                        }
                    } else {
                        log(&format!(
                            "PR #{pr_num} merge failed: {} — treating as changes",
                            merge_result.message
                        ));
                        let r = reviewers.remove(ri);
                        teardown_reviewer(config, wt_mgr, name_pool, r).await;

                        if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id)
                        {
                            let next_round = workers[wi].rework_count + 1;
                            if let Some(max) = config.limits.max_rework_rounds {
                                if next_round > max {
                                    let breach = LimitBreached::ReworkRounds {
                                        count: next_round,
                                        max,
                                    };
                                    log(&format!(
                                        "WATCHDOG: worker {} killed (task #{}) — {}",
                                        workers[wi].agent_name, workers[wi].task_id, breach
                                    ));
                                    let w = workers.remove(wi);
                                    teardown_worker(config, wt_mgr, name_pool, w, "open").await;
                                    if !consume_mailbox_row(&db_path, *id).await {
                                        break;
                                    }
                                    break;
                                }
                            }

                            let rework_msg = format!(
                                "Merge of PR #{pr_num} failed: {}\n\n\
                                 Fix the issue and push again.",
                                merge_result.message
                            );
                            let rework_turn = reviewer::build_rework_turn(&rework_msg);
                            if let Err(e) = workers[wi].proc.feed_turn(&rework_turn).await {
                                log(&format!(
                                    "merge-failure rework feed failed: {e} — \
                                     tearing down broken worker"
                                ));
                                let w = workers.remove(wi);
                                teardown_worker(config, wt_mgr, name_pool, w, "open").await;
                            } else {
                                let w = &mut workers[wi];
                                w.draining = true;
                                w.pr = None;
                                w.rework_count += 1;
                                w.turn_started_at = std::time::Instant::now();

                                if let Some(ref mut sl) = w.session_log {
                                    sl.log_rework(w.rework_count);
                                }

                                let p = db_path.clone();
                                let entry = slot_journal_entry(w, "worker", "working");
                                tokio::task::spawn_blocking(move || -> Result<()> {
                                    let mut conn = quorum_core::db::open(&p)?;
                                    journal::upsert(&mut conn, &entry)
                                })
                                .await
                                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
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

                    let r = reviewers.remove(ri);
                    teardown_reviewer(config, wt_mgr, name_pool, r).await;

                    if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id) {
                        let next_round = workers[wi].rework_count + 1;
                        if let Some(max) = config.limits.max_rework_rounds {
                            if next_round > max {
                                let breach = LimitBreached::ReworkRounds {
                                    count: next_round,
                                    max,
                                };
                                log(&format!(
                                    "WATCHDOG: worker {} killed (task #{}) — {}",
                                    workers[wi].agent_name, workers[wi].task_id, breach
                                ));
                                let w = workers.remove(wi);
                                teardown_worker(config, wt_mgr, name_pool, w, "open").await;
                                if !consume_mailbox_row(&db_path, *id).await {
                                    break;
                                }
                                break;
                            }
                        }

                        let rework_turn = reviewer::build_rework_turn(feedback);
                        if let Err(e) = workers[wi].proc.feed_turn(&rework_turn).await {
                            log(&format!(
                                "rework feed_turn failed: {e} — \
                                 tearing down broken worker"
                            ));
                            let w = workers.remove(wi);
                            teardown_worker(config, wt_mgr, name_pool, w, "open").await;
                        } else {
                            let w = &mut workers[wi];
                            w.draining = true;
                            w.pr = None;
                            w.rework_count += 1;
                            w.turn_started_at = std::time::Instant::now();

                            if let Some(ref mut sl) = w.session_log {
                                sl.log_rework(w.rework_count);
                            }

                            let p = db_path.clone();
                            let entry = slot_journal_entry(w, "worker", "working");
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
                        "reviewer {} done without verdict — tearing down reviewer, \
                         clearing PR (worker must re-signal done to retry)",
                        row.agent
                    ));
                    let r = reviewers.remove(ri);
                    teardown_reviewer(config, wt_mgr, name_pool, r).await;
                    if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id) {
                        workers[wi].pr = None;
                    }
                }
            }

            if !consume_mailbox_row(&db_path, *id).await {
                break;
            }
            break;
        }

        // Check worker match.
        let worker_idx = workers.iter().position(|w| w.agent_name == row.agent);
        if let Some(wi) = worker_idx {
            log(&format!(
                "worker {} done (pr={:?}{note_suffix})",
                workers[wi].agent_name, row.pr,
            ));

            if let Some(pr) = row.pr {
                workers[wi].pr = Some(pr);
                log(&format!(
                    "worker {} PR #{} ready for review",
                    workers[wi].agent_name, pr
                ));
            } else {
                let w = workers.remove(wi);
                teardown_worker(config, wt_mgr, name_pool, w, "done").await;
            }

            if !consume_mailbox_row(&db_path, *id).await {
                break;
            }
            break;
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

    // ── Phase 3: Drain events from active reviewers ────────────────────
    let mut reviewers_to_kill: Vec<usize> = Vec::new();
    for (i, r) in reviewers.iter_mut().enumerate() {
        if !r.draining {
            continue;
        }
        // Wall-clock watchdog (checked each tick, even before result arrives)
        if let Some(breach) = check_wall_clock_limits(&config.limits, r) {
            log(&format!(
                "WATCHDOG: reviewer {} killed — {}",
                r.agent_name, breach
            ));
            reviewers_to_kill.push(i);
            continue;
        }
        if let Some(breach) = drain_events(r, &db_path, "reviewer", &config.limits).await? {
            log(&format!(
                "WATCHDOG: reviewer {} killed — {}",
                r.agent_name, breach
            ));
            reviewers_to_kill.push(i);
        }
    }
    for &i in reviewers_to_kill.iter().rev() {
        let dead = reviewers.remove(i);
        teardown_reviewer(config, wt_mgr, name_pool, dead).await;
    }

    // ── Phase 4: Drain events from active workers ──────────────────────
    let mut workers_to_kill: Vec<usize> = Vec::new();
    for (i, w) in workers.iter_mut().enumerate() {
        if !w.draining {
            continue;
        }
        if let Some(breach) = check_wall_clock_limits(&config.limits, w) {
            log(&format!(
                "WATCHDOG: worker {} killed (task #{}) — {}",
                w.agent_name, w.task_id, breach
            ));
            workers_to_kill.push(i);
            continue;
        }
        if let Some(breach) = drain_events(w, &db_path, "worker", &config.limits).await? {
            log(&format!(
                "WATCHDOG: worker {} killed (task #{}) — {}",
                w.agent_name, w.task_id, breach
            ));
            workers_to_kill.push(i);
        }
    }
    for &i in workers_to_kill.iter().rev() {
        let dead = workers.remove(i);
        teardown_worker(config, wt_mgr, name_pool, dead, "open").await;
    }

    // ── Phase 4b: Detect dead workers/reviewers ────────────────────────
    // A crashed or exited agent process leaves the slot pinned: the task is
    // never released, the name/worktree leak, and Phase 5 (which gates on
    // !w.draining) never spawns a reviewer. `next_event`/`drain_events` alone
    // cannot detect this — stdout EOF is a hint but a stuck child can hold
    // its stdout open. `try_wait` is the authoritative signal.
    let mut dead_workers: Vec<usize> = Vec::new();
    for (i, w) in workers.iter_mut().enumerate() {
        match w.proc.try_wait() {
            Ok(Some(status)) => {
                log(&format!(
                    "worker {} died mid-task (task #{}, status={:?}) — releasing task/name/worktree",
                    w.agent_name, w.task_id, status
                ));
                dead_workers.push(i);
            }
            Ok(None) => {}
            Err(e) => {
                log(&format!("worker {} try_wait error: {e}", w.agent_name));
            }
        }
    }
    // Remove in reverse order to preserve indices.
    for &i in dead_workers.iter().rev() {
        let dead = workers.remove(i);
        teardown_worker(config, wt_mgr, name_pool, dead, "open").await;
    }

    let mut dead_reviewers: Vec<usize> = Vec::new();
    for (i, r) in reviewers.iter_mut().enumerate() {
        match r.proc.try_wait() {
            Ok(Some(status)) => {
                log(&format!(
                    "reviewer {} died (status={:?}) — releasing name/worktree",
                    r.agent_name, status
                ));
                dead_reviewers.push(i);
            }
            Ok(None) => {}
            Err(e) => {
                log(&format!("reviewer {} try_wait error: {e}", r.agent_name));
            }
        }
    }
    for &i in dead_reviewers.iter().rev() {
        let dead = reviewers.remove(i);
        teardown_reviewer(config, wt_mgr, name_pool, dead).await;
    }

    // ── Phase 4c: Deliver queued messages to idle workers (M5) ──────────
    // Messages are delivered "at idle" — only when the worker is between turns
    // (draining=false). If the target is still draining, the message stays
    // unconsumed and retries next tick.
    for (msg_id, msg_row) in &pending_messages {
        let target = match &msg_row.to_agent {
            Some(t) => t,
            None => {
                log(&format!(
                    "consuming message from {} with no to_agent",
                    msg_row.agent
                ));
                consume_mailbox_row(&db_path, *msg_id).await;
                continue;
            }
        };

        let wi = workers.iter().position(|w| w.agent_name == *target);
        match wi {
            Some(wi) if workers[wi].draining => {
                // Target is mid-turn — leave unconsumed, retry next tick.
            }
            Some(wi) => {
                let payload = msg_row.payload.as_deref().unwrap_or("");
                let turn = serde_json::json!({
                    "type": "user",
                    "message": { "content": format!(
                        "MESSAGE from {}: {payload}", msg_row.agent
                    ) }
                });
                match workers[wi].proc.feed_turn(&turn.to_string()).await {
                    Ok(()) => {
                        workers[wi].draining = true;
                        workers[wi].turn_started_at = std::time::Instant::now();
                        log(&format!(
                            "delivered message from {} to {} (task #{})",
                            msg_row.agent, target, workers[wi].task_id,
                        ));
                    }
                    Err(e) => {
                        log(&format!(
                            "message delivery to {} failed: {e} — tearing down broken worker",
                            target,
                        ));
                        let w = workers.remove(wi);
                        teardown_worker(config, wt_mgr, name_pool, w, "open").await;
                    }
                }
                consume_mailbox_row(&db_path, *msg_id).await;
            }
            None => {
                log(&format!("consuming message to {target} (no active worker)"));
                consume_mailbox_row(&db_path, *msg_id).await;
            }
        }
    }

    // ── Phase 5: Spawn reviewers for workers with PRs ──────────────────
    // Each worker that has a PR and no paired reviewer (and is not draining)
    // gets a reviewer spawned. Reviewers don't consume worker capacity.
    // Collect (pr, task_id, index) for workers needing reviewers, so we don't
    // hold an immutable borrow on `workers` across the spawn calls.
    let needs_reviewer: Vec<(i64, i64, usize)> = workers
        .iter()
        .enumerate()
        .filter_map(|(i, w)| {
            if let Some(pr) = w.pr {
                if !w.draining && !reviewers.iter().any(|r| r.task_id == w.task_id) {
                    return Some((pr, w.task_id, i));
                }
            }
            None
        })
        .collect();
    for (pr, _task_id, wi) in needs_reviewer {
        spawn_reviewer_for_worker(config, wt_mgr, name_pool, reviewers, pr, &workers[wi]).await?;
    }

    // ── Phase 6: Spawn workers up to cap ───────────────────────────────
    // Gate on worker count, not total in_use_count() — reviewers must
    // not consume worker capacity (F16).
    while workers.len() < config.cap {
        if !spawn_worker(config, wt_mgr, name_pool, workers).await? {
            break;
        }
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

/// Reason an agent was killed by a watchdog.
enum LimitBreached {
    TurnTokens { turn: i64, max: i64 },
    TaskTokens { total: i64, max: i64 },
    TurnCostUsd { turn: f64, max: f64 },
    TaskCostUsd { total: f64, max: f64 },
    TurnWallSecs { elapsed: u64, max: u64 },
    TaskWallSecs { elapsed: u64, max: u64 },
    ReworkRounds { count: u32, max: u32 },
}

impl std::fmt::Display for LimitBreached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TurnTokens { turn, max } => {
                write!(f, "turn tokens {turn} exceeded limit {max}")
            }
            Self::TaskTokens { total, max } => {
                write!(f, "task tokens {total} exceeded limit {max}")
            }
            Self::TurnCostUsd { turn, max } => {
                write!(f, "turn cost ${turn:.4} exceeded limit ${max:.4}")
            }
            Self::TaskCostUsd { total, max } => {
                write!(f, "task cost ${total:.4} exceeded limit ${max:.4}")
            }
            Self::TurnWallSecs { elapsed, max } => {
                write!(f, "turn wall-clock {elapsed}s exceeded limit {max}s")
            }
            Self::TaskWallSecs { elapsed, max } => {
                write!(f, "task wall-clock {elapsed}s exceeded limit {max}s")
            }
            Self::ReworkRounds { count, max } => {
                write!(f, "rework rounds {count} exceeded limit {max}")
            }
        }
    }
}

/// Check per-turn and cumulative limits after a result event.
fn check_post_result_limits(
    limits: &CostLimits,
    turn_tokens: i64,
    cumulative_tokens: i64,
    turn_cost_usd: Option<f64>,
    cumulative_cost_usd: f64,
    slot: &SlotState,
) -> Option<LimitBreached> {
    if let Some(max) = limits.max_turn_tokens {
        if turn_tokens > max {
            return Some(LimitBreached::TurnTokens {
                turn: turn_tokens,
                max,
            });
        }
    }
    if let Some(max) = limits.max_task_tokens {
        if cumulative_tokens > max {
            return Some(LimitBreached::TaskTokens {
                total: cumulative_tokens,
                max,
            });
        }
    }
    if let Some(max) = limits.max_turn_cost_usd {
        if let Some(turn_cost) = turn_cost_usd {
            if turn_cost > max {
                return Some(LimitBreached::TurnCostUsd {
                    turn: turn_cost,
                    max,
                });
            }
        }
    }
    if let Some(max) = limits.max_task_cost_usd {
        if cumulative_cost_usd > max {
            return Some(LimitBreached::TaskCostUsd {
                total: cumulative_cost_usd,
                max,
            });
        }
    }
    if let Some(max) = limits.max_turn_wall_secs {
        let elapsed = slot.turn_started_at.elapsed().as_secs();
        if elapsed > max {
            return Some(LimitBreached::TurnWallSecs { elapsed, max });
        }
    }
    if let Some(max) = limits.max_task_wall_secs {
        let elapsed = slot.task_started_at.elapsed().as_secs();
        if elapsed > max {
            return Some(LimitBreached::TaskWallSecs { elapsed, max });
        }
    }
    None
}

/// Check wall-clock limits only (called each tick for slots still draining).
fn check_wall_clock_limits(limits: &CostLimits, slot: &SlotState) -> Option<LimitBreached> {
    if let Some(max) = limits.max_turn_wall_secs {
        let elapsed = slot.turn_started_at.elapsed().as_secs();
        if elapsed > max {
            return Some(LimitBreached::TurnWallSecs { elapsed, max });
        }
    }
    if let Some(max) = limits.max_task_wall_secs {
        let elapsed = slot.task_started_at.elapsed().as_secs();
        if elapsed > max {
            return Some(LimitBreached::TaskWallSecs { elapsed, max });
        }
    }
    None
}

/// Drain stream events from an agent slot (bounded per tick, 5s timeout).
/// Returns `Some(LimitBreached)` if a cost/time ceiling was hit.
async fn drain_events(
    slot: &mut SlotState,
    db_path: &std::path::Path,
    role: &str,
    limits: &CostLimits,
) -> Result<Option<LimitBreached>> {
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_secs(5), slot.proc.next_event()).await
    {
        match &event {
            stream::Event::Result {
                usage,
                total_cost_usd,
                ..
            } => {
                let turn_tokens = usage
                    .as_ref()
                    .map_or(0, |u| (u.input_tokens + u.output_tokens) as i64);
                slot.cost_tokens += turn_tokens;
                // total_cost_usd is session-cumulative (running total), not per-turn.
                // Derive per-turn cost as the delta before overwriting the high-water mark.
                let prev_cost = slot.cost_usd;
                if let Some(cost) = total_cost_usd {
                    slot.cost_usd = *cost;
                }
                let turn_cost_usd = total_cost_usd.map(|c| (c - prev_cost).max(0.0));
                log(&format!(
                    "{role} {} result (turn_tokens={}, cumulative={}, cost_usd={:.4})",
                    slot.agent_name, turn_tokens, slot.cost_tokens, slot.cost_usd
                ));

                if let Some(ref mut sl) = slot.session_log {
                    sl.update_cost(slot.cost_tokens, slot.cost_usd);
                    sl.set_phase(if role == "worker" {
                        "awaiting-review"
                    } else {
                        "reviewing"
                    });
                }

                let p = db_path.to_path_buf();
                let phase = if role == "worker" {
                    "awaiting-review"
                } else {
                    "reviewing"
                };
                let entry = slot_journal_entry(slot, role, phase);
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut conn = quorum_core::db::open(&p)?;
                    journal::upsert(&mut conn, &entry)
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                .ok();

                slot.draining = false;

                let breach = check_post_result_limits(
                    limits,
                    turn_tokens,
                    slot.cost_tokens,
                    turn_cost_usd,
                    slot.cost_usd,
                    slot,
                );
                return Ok(breach);
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

        if let Some(ref mut sl) = slot.session_log {
            sl.log_event(&event);
        }
    }
    Ok(None)
}

async fn spawn_reviewer_for_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    reviewers: &mut Vec<SlotState>,
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

    // F4: provision reviewer worktree from the PR head branch (the worker's
    // branch), not origin/main, so the reviewer has the code under review
    // checked out locally.
    match wt_mgr
        .fetch_and_provision(&config.repo_dir, &branch, &wt_path, &worker.branch)
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

    let reviewer_session_log = config.log_dir.as_ref().and_then(|ld| {
        session_log::SessionLog::create(
            ld,
            &reviewer_name,
            "reviewer",
            Some(worker.task_id),
            &session_id,
            &branch,
            now_unix(),
        )
        .ok()
    });

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
        cost_tokens: 0,
        agent_state: None,
        cost_usd: 0.0,
        log_dir: reviewer_session_log
            .as_ref()
            .map(|l| l.dir().to_string_lossy().into()),
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
    };

    match reviewer::spawn_reviewer(
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

            let now_instant = std::time::Instant::now();
            reviewers.push(SlotState {
                agent_name: reviewer_name,
                proc,
                task_id: worker.task_id,
                session_id,
                worktree_path: wt_path,
                branch,
                draining: true,
                pr: Some(pr),
                rework_count: 0,
                cost_tokens: 0,
                cost_usd: 0.0,
                task_started_at: now_instant,
                turn_started_at: now_instant,
                agent_state: None,
                session_log: reviewer_session_log,
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

/// Spawn a worker for the next highest-priority ready task.
/// Returns true if a worker was spawned, false if no ready tasks or names available.
async fn spawn_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    workers: &mut Vec<SlotState>,
) -> Result<bool> {
    let db_path = config.db_path.clone();
    let p = db_path.clone();

    // Collect task_ids already in flight to skip them.
    let in_flight: Vec<i64> = workers.iter().map(|w| w.task_id).collect();

    let ready_task = tokio::task::spawn_blocking(move || -> Result<Option<tasks::Task>> {
        let conn = quorum_core::db::open(&p)?;
        let open = tasks::list(&conn, Some("open"), None, None)?;
        Ok(open
            .into_iter()
            .find(|t| t.ready && !in_flight.contains(&t.id)))
    })
    .await
    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))??;

    let task = match ready_task {
        Some(t) => t,
        None => return Ok(false),
    };

    let agent_name = match name_pool.acquire() {
        Some(n) => n,
        None => return Ok(false),
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
            return Ok(false);
        }
        Err(e) => {
            log(&format!("task #{} claim failed: {e}", task.id));
            name_pool.release(&agent_name);
            return Ok(false);
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
            return Ok(false);
        }
    }

    let worker_session_log = config.log_dir.as_ref().and_then(|ld| {
        session_log::SessionLog::create(
            ld,
            &agent_name,
            "worker",
            Some(task.id),
            &session_id,
            &branch,
            now_unix(),
        )
        .ok()
    });

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
        cost_tokens: 0,
        agent_state: None,
        cost_usd: 0.0,
        log_dir: worker_session_log
            .as_ref()
            .map(|l| l.dir().to_string_lossy().into()),
    };
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        journal::upsert(&mut conn, &entry)
    })
    .await
    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
    .ok();

    let (label_model, label_effort) = labels_to_model_effort(task.labels.as_deref());
    let spec = AgentSpec {
        model: label_model.unwrap_or_else(|| config.model.clone()),
        effort: label_effort.unwrap_or_else(|| config.effort.clone()),
        session_id: session_id.clone(),
        worktree: wt_path.clone(),
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
                return Ok(false);
            }

            let now_instant = std::time::Instant::now();
            workers.push(SlotState {
                agent_name,
                proc,
                task_id: task.id,
                session_id,
                worktree_path: wt_path,
                branch,
                draining: true,
                pr: None,
                rework_count: 0,
                cost_tokens: 0,
                cost_usd: 0.0,
                task_started_at: now_instant,
                turn_started_at: now_instant,
                agent_state: None,
                session_log: worker_session_log,
            });
        }
        Err(e) => {
            log(&format!("agent spawn failed: {e}"));
            release_task(&db_path, &agent_name, task.id).await;
            name_pool.release(&agent_name);
            wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
            return Ok(false);
        }
    }

    Ok(true)
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
    mut state: SlotState,
    task_status: &str,
) {
    log(&format!(
        "tearing down worker {} (task #{} -> {task_status})",
        state.agent_name, state.task_id
    ));

    let verdict = if task_status == "done" {
        Some("done")
    } else {
        None
    };
    if let Some(ref mut sl) = state.session_log {
        sl.finalize(verdict);
    }

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
    mut state: SlotState,
) {
    log(&format!("tearing down reviewer {}", state.agent_name));

    if let Some(ref mut sl) = state.session_log {
        sl.finalize(None);
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_to_model_effort_tier_wins_over_global() {
        let labels = r#"["kind:fix","tier:opus-46"]"#;
        let (model, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(model.as_deref(), Some("opus-46"));
        assert_eq!(effort, None);
    }

    #[test]
    fn labels_to_model_effort_effort_override() {
        let labels = r#"["effort:high","tier:sonnet-5"]"#;
        let (model, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(model.as_deref(), Some("sonnet-5"));
        assert_eq!(effort.as_deref(), Some("high"));
    }

    #[test]
    fn labels_to_model_effort_no_labels() {
        let (model, effort) = labels_to_model_effort(None);
        assert_eq!(model, None);
        assert_eq!(effort, None);
    }

    #[test]
    fn labels_to_model_effort_empty_suffix_ignored() {
        let labels = r#"["tier:","effort:"]"#;
        let (model, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(model, None);
        assert_eq!(effort, None);
    }

    #[test]
    fn labels_to_model_effort_malformed_json() {
        let (model, effort) = labels_to_model_effort(Some("not json"));
        assert_eq!(model, None);
        assert_eq!(effort, None);
    }

    #[test]
    fn labels_to_model_effort_first_tier_wins() {
        let labels = r#"["tier:opus-46","tier:sonnet-5"]"#;
        let (model, _effort) = labels_to_model_effort(Some(labels));
        assert_eq!(model.as_deref(), Some("opus-46"));
    }

    fn make_dummy_slot() -> SlotState {
        use std::time::Instant;
        use tokio::io::{AsyncBufReadExt, BufReader};

        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut child = rt.block_on(async {
            tokio::process::Command::new("true")
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        });
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let reader = BufReader::new(stdout).lines();
        let proc = AgentProc::from_parts(child, stdin, reader);
        let now = Instant::now();
        SlotState {
            agent_name: "Test-1".into(),
            proc,
            task_id: 1,
            session_id: "sess".into(),
            worktree_path: PathBuf::from("/tmp/test"),
            branch: "test-branch".into(),
            draining: false,
            pr: None,
            rework_count: 0,
            cost_tokens: 500,
            cost_usd: 0.01,
            task_started_at: now,
            turn_started_at: now,
            agent_state: None,
            session_log: None,
        }
    }

    #[test]
    fn check_limits_no_limits_returns_none() {
        let limits = CostLimits::default();
        let slot = make_dummy_slot();
        assert!(check_post_result_limits(&limits, 100, 500, Some(0.01), 0.05, &slot).is_none());
    }

    #[test]
    fn check_limits_turn_tokens_exceeded() {
        let limits = CostLimits {
            max_turn_tokens: Some(100),
            ..Default::default()
        };
        let slot = make_dummy_slot();
        let result = check_post_result_limits(&limits, 200, 500, None, 0.0, &slot);
        assert!(result.is_some());
        assert!(result.unwrap().to_string().contains("turn tokens"));
    }

    #[test]
    fn check_limits_turn_tokens_within_limit() {
        let limits = CostLimits {
            max_turn_tokens: Some(500),
            ..Default::default()
        };
        let slot = make_dummy_slot();
        assert!(check_post_result_limits(&limits, 200, 500, None, 0.0, &slot).is_none());
    }

    #[test]
    fn check_limits_task_tokens_exceeded() {
        let limits = CostLimits {
            max_task_tokens: Some(400),
            ..Default::default()
        };
        let slot = make_dummy_slot();
        let result = check_post_result_limits(&limits, 100, 500, None, 0.0, &slot);
        assert!(result.is_some());
        assert!(result.unwrap().to_string().contains("task tokens"));
    }

    #[test]
    fn check_limits_turn_cost_usd_exceeded() {
        let limits = CostLimits {
            max_turn_cost_usd: Some(0.01),
            ..Default::default()
        };
        let slot = make_dummy_slot();
        let result = check_post_result_limits(&limits, 100, 500, Some(0.05), 0.05, &slot);
        assert!(result.is_some());
        assert!(result.unwrap().to_string().contains("turn cost"));
    }

    #[test]
    fn check_limits_task_cost_usd_exceeded() {
        let limits = CostLimits {
            max_task_cost_usd: Some(0.10),
            ..Default::default()
        };
        let slot = make_dummy_slot();
        let result = check_post_result_limits(&limits, 100, 500, None, 0.20, &slot);
        assert!(result.is_some());
        assert!(result.unwrap().to_string().contains("task cost"));
    }

    #[test]
    fn check_limits_turn_cost_usd_ignored_when_absent() {
        let limits = CostLimits {
            max_turn_cost_usd: Some(0.01),
            ..Default::default()
        };
        let slot = make_dummy_slot();
        assert!(check_post_result_limits(&limits, 100, 500, None, 0.0, &slot).is_none());
    }

    #[test]
    fn check_wall_clock_no_limits_returns_none() {
        let limits = CostLimits::default();
        let slot = make_dummy_slot();
        assert!(check_wall_clock_limits(&limits, &slot).is_none());
    }

    #[test]
    fn limit_breached_display_rework() {
        let b = LimitBreached::ReworkRounds { count: 4, max: 3 };
        assert_eq!(b.to_string(), "rework rounds 4 exceeded limit 3");
    }

    #[test]
    fn limit_breached_display_all_variants() {
        let cases: Vec<LimitBreached> = vec![
            LimitBreached::TurnTokens { turn: 100, max: 50 },
            LimitBreached::TaskTokens {
                total: 1000,
                max: 500,
            },
            LimitBreached::TurnCostUsd {
                turn: 0.10,
                max: 0.05,
            },
            LimitBreached::TaskCostUsd {
                total: 1.0,
                max: 0.5,
            },
            LimitBreached::TurnWallSecs {
                elapsed: 120,
                max: 60,
            },
            LimitBreached::TaskWallSecs {
                elapsed: 3600,
                max: 1800,
            },
            LimitBreached::ReworkRounds { count: 4, max: 3 },
        ];
        for c in cases {
            let s = c.to_string();
            assert!(s.contains("exceeded limit"), "bad display: {s}");
        }
    }

    #[test]
    fn cost_limits_default_is_unlimited() {
        let limits = CostLimits::default();
        assert!(limits.max_turn_tokens.is_none());
        assert!(limits.max_task_tokens.is_none());
        assert!(limits.max_turn_cost_usd.is_none());
        assert!(limits.max_task_cost_usd.is_none());
        assert!(limits.max_turn_wall_secs.is_none());
        assert!(limits.max_task_wall_secs.is_none());
        assert!(limits.max_rework_rounds.is_none());
    }
}
