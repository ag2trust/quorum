//! `quorum serve` — the agent-manager daemon.
//!
//! Builds a tokio runtime and runs an async tick loop that polls the mailbox,
//! spawns/drives agents, and shuts down cleanly on Ctrl-C. See spec §3.

pub mod agent;
pub mod approvals;
pub mod merge;
pub mod names;
pub mod recovery;
pub mod render;
pub mod reviewer;
pub mod session_log;
pub mod stream;
pub mod worktree;

use agent::{AgentProc, AgentSpec};
use names::Pool;
use quorum_core::error::{QuorumError, Result};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::lifecycle::{Effect, Event};
use quorum_core::mailbox;
use quorum_core::stats::DaemonLiveStats;
use quorum_core::tasks;
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use worktree::WorktreeManager;

const MAX_POISON_STRIKES: u32 = 3;
const MAX_REVIEWER_PROVISION_STRIKES: u32 = 3;

struct PoisonTracker {
    strikes: HashMap<i64, u32>,
}

impl PoisonTracker {
    fn new() -> Self {
        Self {
            strikes: HashMap::new(),
        }
    }

    fn record_strike(&mut self, task_id: i64) -> u32 {
        let count = self.strikes.entry(task_id).or_insert(0);
        *count += 1;
        *count
    }

    fn clear(&mut self, task_id: i64) {
        self.strikes.remove(&task_id);
    }

    #[cfg(test)]
    fn is_poisoned(&self, task_id: i64) -> bool {
        self.strikes.get(&task_id).copied().unwrap_or(0) >= MAX_POISON_STRIKES
    }

    #[cfg(test)]
    fn strikes(&self, task_id: i64) -> u32 {
        self.strikes.get(&task_id).copied().unwrap_or(0)
    }
}

struct ReviewerProvisionTracker {
    strikes: HashMap<(i64, i64), u32>,
}

impl ReviewerProvisionTracker {
    fn new() -> Self {
        Self {
            strikes: HashMap::new(),
        }
    }

    fn record_strike(&mut self, task_id: i64, pr: i64) -> u32 {
        let count = self.strikes.entry((task_id, pr)).or_insert(0);
        *count += 1;
        *count
    }

    fn clear(&mut self, task_id: i64, pr: i64) {
        self.strikes.remove(&(task_id, pr));
    }

    fn is_exhausted(&self, task_id: i64, pr: i64) -> bool {
        self.strikes.get(&(task_id, pr)).copied().unwrap_or(0) >= MAX_REVIEWER_PROVISION_STRIKES
    }

    #[cfg(test)]
    fn strikes(&self, task_id: i64, pr: i64) -> u32 {
        self.strikes.get(&(task_id, pr)).copied().unwrap_or(0)
    }
}

/// Lifetime roster of agent names this daemon has ever owned.
///
/// Under a multi-instance topology (one shared SQLite mailbox, per-repo daemons)
/// a mailbox row whose `agent` matches no live worker/reviewer usually means
/// "belongs to the OTHER daemon's agent", not "phantom row". Consuming such a
/// row destroys the sibling daemon's lifecycle signal (#181).
///
/// The roster tracks every agent name this daemon has ever spawned or resumed —
/// entries are inserted at spawn/recover and NEVER removed (so recently-torn-down
/// names still register as "ours"). The phantom-row GC guarantee from #133 is
/// preserved WITHIN an instance: our own past-agent rows still get consumed;
/// rows for names we have never owned are left for the owning instance to
/// process (or for TTL/sweep to reap if truly orphaned).
pub(crate) struct LifetimeRoster {
    names: std::collections::HashSet<String>,
    logged_foreign: std::collections::HashSet<String>,
}

impl LifetimeRoster {
    fn new() -> Self {
        Self {
            names: std::collections::HashSet::new(),
            logged_foreign: std::collections::HashSet::new(),
        }
    }

    /// Register an agent name as owned by this daemon (permanent — never removed).
    pub(crate) fn register(&mut self, name: &str) {
        self.names.insert(name.to_string());
    }

    /// True if this daemon has ever owned this agent name.
    pub(crate) fn owns(&self, name: &str) -> bool {
        self.names.contains(name)
    }

    /// Record that we have logged a "foreign row" for this agent (debounce).
    /// Returns true the first time an agent is seen, false on subsequent calls.
    fn log_foreign_once(&mut self, name: &str) -> bool {
        self.logged_foreign.insert(name.to_string())
    }
}

fn query_pr_head_ref(pr: i64, repo_dir: &Path, gh_repo: Option<&str>) -> Option<String> {
    let mut args = vec![
        "pr".to_string(),
        "view".to_string(),
        pr.to_string(),
        "--json".to_string(),
        "headRefName".to_string(),
        "--jq".to_string(),
        ".headRefName".to_string(),
    ];
    if let Some(repo) = gh_repo {
        args.push("--repo".to_string());
        args.push(repo.to_string());
    }
    let output = std::process::Command::new("gh")
        .args(&args)
        .current_dir(repo_dir)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn log(msg: &str) {
    let _ = writeln!(std::io::stderr(), "quorum serve: {msg}");
}

/// Map a tier label suffix to a full Claude model ID.
/// Returns `None` (fall back to global default) for unknown tiers.
fn tier_to_model_id(tier: &str) -> Option<String> {
    match tier {
        "opus-46" => Some("claude-opus-4-6".into()),
        "opus-47" => Some("claude-opus-4-7".into()),
        "opus-48" => Some("claude-opus-4-8".into()),
        "sonnet-5" => Some("claude-sonnet-5".into()),
        unknown => {
            log(&format!(
                "unknown tier label '{unknown}', falling back to global model"
            ));
            None
        }
    }
}

/// Extract model and effort overrides from a task's labels JSON.
///
/// Labels like `tier:opus-46` map to model `claude-opus-4-6`; `effort:high` maps to effort `high`.
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
                    model = tier_to_model_id(val);
                }
            }
        }
        if effort.is_none() {
            if let Some(val) = label.strip_prefix("effort:") {
                if val == "medium" || val == "high" {
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
        pid: slot.proc.pid(),
        pr: slot.pr,
        rework_count: slot.rework_count as i32,
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
}

/// Configuration for the daemon, resolved from CLI flags / config file.
pub struct ServeConfig {
    pub db_path: PathBuf,
    pub cap: usize,
    pub repo_dir: PathBuf,
    pub worktree_base: PathBuf,
    pub names_file: Option<PathBuf>,
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
    /// When true, the daemon drains and exits 75 when its own repo's base branch advances.
    pub self_update_drain: bool,
    /// Seconds to wait for in-flight agents during drain before SIGTERM.
    pub drain_timeout_secs: u64,
    /// Owner/name of the daemon's own repo (e.g. "ag2trust/quorum").
    pub self_repo: Option<String>,
    /// Interval between git ls-remote polls for Trigger B (seconds). Default: 60.
    pub sha_poll_interval_secs: u64,
    /// Seconds to wait for required status checks before merging. Default: 900.
    pub merge_checks_timeout_secs: u64,
    /// Poll interval for status checks (seconds). Default: 30.
    pub merge_checks_poll_secs: u64,
    /// The repo this daemon manages (e.g. "ag2trust/quorum"). Set via `--repo`.
    /// Injected as `QUORUM_REPO` into spawned workers/reviewers.
    pub repo: String,
    /// Base branch name (e.g. "main" or "master") for sha-polling, worktree
    /// provisioning, and merge targeting.
    pub base_branch: String,
    /// When set, serve polls for this file's existence every tick and initiates
    /// shutdown when it disappears (#201: test fixture self-termination).
    pub exit_when_gone: Option<PathBuf>,
}

pub const EXIT_SELF_UPDATE: i32 = 75;

const DAEMON_LOCK_STALE_SECS: i64 = 30;

pub fn run_serve(config: ServeConfig) -> Result<i32> {
    log(&format!(
        "starting (cap={}, repo={})",
        config.cap, config.repo
    ));

    let daemon_pid = std::process::id() as i64;
    let now = now_unix();

    // Acquire the single-daemon-per-DB lock. The check + acquire is atomic
    // (single BEGIN IMMEDIATE) to prevent TOCTOU races between two daemons
    // starting simultaneously.
    {
        let mut conn = quorum_core::db::open(&config.db_path)?;
        match quorum_core::daemon_lock::try_acquire(
            &mut conn,
            daemon_pid,
            now,
            DAEMON_LOCK_STALE_SECS,
            pid_is_alive,
        )? {
            quorum_core::daemon_lock::AcquireResult::Acquired => {}
            quorum_core::daemon_lock::AcquireResult::Held {
                holder_pid,
                heartbeat_age,
            } => {
                return Err(QuorumError::Usage(format!(
                    "another daemon (pid {holder_pid}) is already serving this DB — \
                     heartbeat {heartbeat_age}s ago. Stop it first or wait for it to exit"
                )));
            }
        }
    }

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| QuorumError::Io(format!("failed to create tokio runtime: {e}")))?;

    let result = rt.block_on(tick_loop(&config, daemon_pid));

    // Release the lock on clean shutdown (best-effort).
    if let Ok(conn) = quorum_core::db::open(&config.db_path) {
        let _ = quorum_core::daemon_lock::release(&conn, daemon_pid);
    }

    result
}

fn pid_is_alive(pid: i64) -> bool {
    let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if ret == 0 {
        return true;
    }
    // EPERM means the process exists but we lack permission — still alive.
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

pub(crate) struct LiveStats {
    tool_count: u32,
    now_label: String,
    event_times: VecDeque<std::time::Instant>,
    mid_turn_tokens: i64,
    spawn_epoch: i64,
}

impl LiveStats {
    fn new() -> Self {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            tool_count: 0,
            now_label: String::new(),
            event_times: VecDeque::new(),
            mid_turn_tokens: 0,
            spawn_epoch: epoch,
        }
    }

    fn record_event(&mut self) {
        let now = std::time::Instant::now();
        self.event_times.push_back(now);
        while let Some(front) = self.event_times.front() {
            if now.duration_since(*front).as_secs() > 60 {
                self.event_times.pop_front();
            } else {
                break;
            }
        }
    }

    fn events_per_min(&self) -> f64 {
        let now = std::time::Instant::now();
        self.event_times
            .iter()
            .filter(|t| now.duration_since(**t).as_secs() <= 60)
            .count() as f64
    }

    fn uptime_secs(&self) -> u64 {
        let now_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        (now_epoch - self.spawn_epoch).max(0) as u64
    }
}

pub(crate) struct SlotState {
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
    live_stats: LiveStats,
}

/// A worker task that has already delivered a PR (`done --pr N`) and is
/// awaiting review, but does NOT have a live worker child process.
///
/// #178: On daemon restart, an awaiting-review-with-PR journal entry is
/// resurrected as a `PendingReview` rather than a `--resume`d worker slot,
/// so the daemon provisions a reviewer against the recorded PR instead of
/// respawning a worker (which would either sit idle burning session context
/// or, if the resume failed, get reaped and cause task re-execution and
/// duplicate PRs).
///
/// If the reviewer verdict comes back as `changes` (or any rework path), a
/// `--resume` worker is spawned lazily at that moment from the stored
/// session_id, and the pending review is promoted to a full `SlotState`.
pub(crate) struct PendingReview {
    agent_name: String,
    task_id: i64,
    pr: i64,
    session_id: String,
    worktree_path: PathBuf,
    branch: String,
    rework_count: u32,
    cost_tokens: i64,
    cost_usd: f64,
    agent_state: Option<String>,
    log_dir: Option<PathBuf>,
    task_started_at: std::time::Instant,
}

/// Snapshot the sha of origin's base branch via `git ls-remote`. Returns None on any failure.
fn poll_origin_base_sha(repo_dir: &std::path::Path, base_branch: &str) -> Option<String> {
    let refspec = format!("refs/heads/{}", base_branch);
    let output = std::process::Command::new("git")
        .args(["ls-remote", "origin", &refspec])
        .current_dir(repo_dir)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.split_whitespace().next().map(|s| s.to_string())
}

/// Why the daemon entered drain mode.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DrainSource {
    SelfUpdate,
    Signal,
}

/// Mutable drain state tracked across ticks.
struct DrainState {
    draining: bool,
    drain_source: Option<DrainSource>,
    drain_started_at: Option<std::time::Instant>,
    drain_sha: Option<String>,
    last_sha_poll: Option<std::time::Instant>,
    known_base_sha: Option<String>,
}

impl DrainState {
    fn new() -> Self {
        Self {
            draining: false,
            drain_source: None,
            drain_started_at: None,
            drain_sha: None,
            last_sha_poll: None,
            known_base_sha: None,
        }
    }

    fn start_drain(&mut self, sha: &str) {
        self.start_drain_with_source(sha, DrainSource::SelfUpdate);
    }

    fn start_drain_with_source(&mut self, sha: &str, source: DrainSource) {
        if self.draining {
            return; // debounce: already draining
        }
        self.draining = true;
        self.drain_source = Some(source);
        self.drain_started_at = Some(std::time::Instant::now());
        self.drain_sha = Some(sha.to_string());
        log(&format!("DRAIN: entering drain mode (sha={sha})"));
    }

    fn exit_code(&self) -> i32 {
        match self.drain_source {
            Some(DrainSource::SelfUpdate) => EXIT_SELF_UPDATE,
            _ => 0,
        }
    }

    fn should_poll_sha(&self, interval_secs: u64) -> bool {
        match self.last_sha_poll {
            None => true,
            Some(t) => t.elapsed().as_secs() >= interval_secs,
        }
    }

    fn timed_out(&self, timeout_secs: u64) -> bool {
        self.drain_started_at
            .is_some_and(|t| t.elapsed().as_secs() >= timeout_secs)
    }
}

/// How the tick loop should react to an error returned by [`tick`].
///
/// Most tick errors are transient (a lost claim race, a momentary `SQLITE_BUSY`) and the
/// loop should log and retry on the next tick. But [`QuorumError::SchemaTooNew`] means the
/// on-disk DB was migrated by a *newer* binary than the one running: retrying can never
/// succeed, because this binary fundamentally cannot read the schema. Left unhandled it
/// live-locks — the daemon ticks forever, logging `tick error: db schema version N is
/// newer than this binary (M)` and doing no work.
///
/// The only recovery is to exit with [`EXIT_SELF_UPDATE`] so `serve-supervisor.sh` fetches,
/// rebuilds, and relaunches on a current binary. This reuses the self-update exit path.
/// In-flight agents can't be gracefully drained (teardown writes to the DB, which would also
/// fail against a too-new schema), so they are force-killed before exit and their tasks are
/// re-adopted from the journal on restart.
#[derive(Debug, PartialEq, Eq)]
enum TickErrorAction {
    /// Transient — log and continue to the next tick.
    Continue,
    /// Unrecoverable binary/schema mismatch — exit so the supervisor rebuilds.
    ExitSelfUpdate,
}

fn classify_tick_error(e: &QuorumError) -> TickErrorAction {
    // Exhaustive on purpose — no `_` arm. A new `QuorumError` variant must force a
    // compile-time decision here; silently defaulting an unknown variant to `Continue`
    // would risk re-introducing the exact live-lock this function exists to prevent.
    match e {
        // Unrecoverable: this binary is older than the on-disk schema, and migrations are
        // forward-only. Retrying can never succeed — exit so the supervisor rebuilds.
        QuorumError::SchemaTooNew { .. } => TickErrorAction::ExitSelfUpdate,
        // Transient / retryable: a lost race, momentary lock contention, or a one-off
        // I/O or DB hiccup that a later tick may clear.
        QuorumError::NotHolder
        | QuorumError::Usage(_)
        | QuorumError::BadInput(_)
        | QuorumError::Busy
        | QuorumError::Db(_)
        | QuorumError::Io(_) => TickErrorAction::Continue,
    }
}

async fn tick_loop(config: &ServeConfig, daemon_pid: i64) -> Result<i32> {
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|e| QuorumError::Io(format!("failed to register SIGINT handler: {e}")))?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| QuorumError::Io(format!("failed to register SIGTERM handler: {e}")))?;

    // Signal counter: 0 = running, 1 = drain requested, 2+ = force shutdown.
    // First SIGINT/SIGTERM enters drain mode (in-flight agents finish their turn);
    // second signal forces immediate teardown. Shutdown happens between ticks —
    // racing the signal against tick() in a select! would cancel tick mid-flight
    // at an await point, which can leak a claimed task and orphan agent processes.
    let signal_count = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    {
        let sc = signal_count.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = sigint.recv() => {}
                    _ = sigterm.recv() => {}
                }
                let prev = sc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if prev >= 1 {
                    break; // already draining; second signal recorded, stop listening
                }
            }
        });
    }

    let mut name_pool = match &config.names_file {
        Some(path) => {
            Pool::load(path, config.cap).map_err(|e| QuorumError::Io(format!("names pool: {e}")))?
        }
        None => {
            log("no names file provided — using auto-generated names");
            Pool::new_generated()
        }
    };

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
    let mut pending_reviews: Vec<PendingReview> = Vec::new();
    let mut poison_tracker = PoisonTracker::new();
    let mut reviewer_provision_tracker = ReviewerProvisionTracker::new();
    let mut drain_state = DrainState::new();
    let mut lifetime_roster = LifetimeRoster::new();

    // Snapshot initial main sha for Trigger B baseline
    if config.self_update_drain {
        drain_state.known_base_sha = poll_origin_base_sha(&config.repo_dir, &config.base_branch);
        if let Some(ref sha) = drain_state.known_base_sha {
            log(&format!(
                "self-update-drain: baseline {} sha={}",
                config.base_branch,
                &sha[..12.min(sha.len())]
            ));
        }
    }

    // #228: approval recovery — merge already-approved PRs from durable,
    // instance-independent state BEFORE journal-driven worker resume, so a
    // self-update-drain restart merges the approved PR instead of re-working it.
    // Runs first so approved tasks are closed (and their journal rows dropped)
    // before recovery::recover could resume a worker for them.
    if let Err(e) =
        approvals::recover(&config.db_path, &config.repo_dir, &config.merge_executor).await
    {
        log(&format!("approval recovery failed: {e} — continuing"));
    }

    // M7: crash recovery — resume in-flight agents from journal
    if let Err(e) = recovery::recover(
        config,
        &wt_mgr,
        &mut name_pool,
        &mut workers,
        &mut reviewers,
        &mut pending_reviews,
        &mut lifetime_roster,
    )
    .await
    {
        log(&format!("recovery failed: {e} — starting fresh"));
    }

    log(&format!("serving (cap={})", config.cap));

    // Standalone heartbeat task — refreshes the daemon lock every 10s,
    // independent of tick() duration. Detects lock theft (0-row refresh)
    // and signals the main loop to exit immediately.
    let lock_stolen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let db = config.db_path.clone();
        let pid = daemon_pid;
        let stolen = lock_stolen.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let db2 = db.clone();
                let result =
                    tokio::task::spawn_blocking(move || -> std::result::Result<usize, String> {
                        let conn = quorum_core::db::open(&db2).map_err(|e| e.to_string())?;
                        let now = now_unix();
                        quorum_core::daemon_lock::refresh(&conn, pid, now)
                            .map_err(|e| e.to_string())
                    })
                    .await;
                match result {
                    Ok(Ok(0)) => {
                        log("FATAL: daemon lock stolen — another daemon owns this DB; exiting immediately");
                        stolen.store(true, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                    Ok(Err(e)) => {
                        log(&format!("heartbeat refresh error: {e}"));
                    }
                    Err(e) => {
                        log(&format!("heartbeat spawn_blocking join error: {e}"));
                    }
                    Ok(Ok(_)) => {}
                }
            }
        });
    }

    loop {
        // Check if heartbeat task detected lock theft.
        if lock_stolen.load(std::sync::atomic::Ordering::SeqCst) {
            log("daemon lock stolen — tearing down and exiting");
            for r in reviewers.drain(..) {
                teardown_reviewer(config, &wt_mgr, &mut name_pool, r).await;
            }
            for w in workers.drain(..) {
                teardown_worker(config, &wt_mgr, &mut name_pool, w, "open").await;
            }
            for p in pending_reviews.drain(..) {
                teardown_pending_review(config, &wt_mgr, &mut name_pool, p, "open", None).await;
            }
            return Ok(1);
        }
        let sig = signal_count.load(std::sync::atomic::Ordering::SeqCst);

        // Second signal (or first signal with no in-flight agents): immediate teardown.
        if sig >= 2
            || (sig >= 1
                && workers.is_empty()
                && reviewers.is_empty()
                && pending_reviews.is_empty())
        {
            if sig >= 2 {
                log("force shutdown (second signal)");
            } else {
                log("shutting down (signal, no in-flight agents)");
            }
            for r in reviewers.drain(..) {
                teardown_reviewer(config, &wt_mgr, &mut name_pool, r).await;
            }
            for w in workers.drain(..) {
                teardown_worker(config, &wt_mgr, &mut name_pool, w, "open").await;
            }
            // #178: pending reviews left dangling on force-shutdown — release
            // the task back to open so a future daemon can pick it up cleanly.
            for p in pending_reviews.drain(..) {
                teardown_pending_review(config, &wt_mgr, &mut name_pool, p, "open", None).await;
            }
            return Ok(0);
        }

        // First signal: enter drain mode (let in-flight agents finish).
        if sig >= 1 && !drain_state.draining {
            log("SIGINT: draining \u{2014} in-flight agents will finish; Ctrl+C again to force immediate shutdown");
            drain_state.start_drain_with_source("signal", DrainSource::Signal);
        }

        // #201: sentinel file disappeared — parent test process died.
        // Force-kill all children immediately (no graceful drain — the test is gone).
        if let Some(ref sentinel) = config.exit_when_gone {
            if !sentinel.exists() {
                log("exit-when-gone: sentinel disappeared — parent died, force shutdown");
                for r in reviewers.drain(..) {
                    r.proc.kill_and_reap().await;
                    name_pool.release(&r.agent_name);
                }
                for w in workers.drain(..) {
                    w.proc.kill_and_reap().await;
                    name_pool.release(&w.agent_name);
                }
                return Ok(1);
            }
        }

        // Trigger B: throttled git ls-remote poll for main sha changes
        if config.self_update_drain
            && !drain_state.draining
            && drain_state.should_poll_sha(config.sha_poll_interval_secs)
        {
            drain_state.last_sha_poll = Some(std::time::Instant::now());
            let repo_dir = config.repo_dir.clone();
            let base_branch = config.base_branch.clone();
            if let Some(new_sha) =
                tokio::task::spawn_blocking(move || poll_origin_base_sha(&repo_dir, &base_branch))
                    .await
                    .ok()
                    .flatten()
            {
                match &drain_state.known_base_sha {
                    Some(old) if *old != new_sha => {
                        log(&format!(
                            "DRAIN: origin/{} advanced ({} -> {})",
                            config.base_branch,
                            &old[..12.min(old.len())],
                            &new_sha[..12.min(new_sha.len())]
                        ));
                        drain_state.start_drain(&new_sha);
                    }
                    None => {
                        drain_state.known_base_sha = Some(new_sha);
                    }
                    _ => {}
                }
            }
        }

        // Drain: check timeout and roster empty
        if drain_state.draining {
            if workers.is_empty() && reviewers.is_empty() && pending_reviews.is_empty() {
                let exit = drain_state.exit_code();
                let sha = drain_state.drain_sha.as_deref().unwrap_or("unknown");
                log(&format!(
                    "DRAIN: all agents finished (sha={sha}), exiting {exit}"
                ));
                return Ok(exit);
            }

            if drain_state.timed_out(config.drain_timeout_secs) {
                let exit = drain_state.exit_code();
                log(&format!(
                    "DRAIN: timeout ({}s) — force-killing {} worker(s), {} reviewer(s), \
                     {} pending review(s)",
                    config.drain_timeout_secs,
                    workers.len(),
                    reviewers.len(),
                    pending_reviews.len(),
                ));
                for r in reviewers.drain(..) {
                    teardown_reviewer(config, &wt_mgr, &mut name_pool, r).await;
                }
                for w in workers.drain(..) {
                    teardown_worker(config, &wt_mgr, &mut name_pool, w, "open").await;
                }
                for p in pending_reviews.drain(..) {
                    teardown_pending_review(config, &wt_mgr, &mut name_pool, p, "open", None).await;
                }
                let sha = drain_state.drain_sha.as_deref().unwrap_or("unknown");
                log(&format!("DRAIN: exiting {exit} (sha={sha})"));
                return Ok(exit);
            }
        }

        if let Err(e) = tick(
            config,
            &wt_mgr,
            &mut name_pool,
            &mut workers,
            &mut reviewers,
            &mut pending_reviews,
            &mut poison_tracker,
            &mut reviewer_provision_tracker,
            &mut drain_state,
            &mut lifetime_roster,
        )
        .await
        {
            match classify_tick_error(&e) {
                TickErrorAction::ExitSelfUpdate => {
                    log(&format!(
                        "SCHEMA: {e} — this binary is outdated relative to the on-disk \
                         schema; exiting {EXIT_SELF_UPDATE} so the supervisor rebuilds and \
                         relaunches on a current binary"
                    ));
                    // Force-kill in-flight agents before exiting. They run in their own
                    // process groups (setpgid, no Drop), so a bare return orphans them —
                    // the relaunched daemon would re-adopt the same tasks and race
                    // live-but-unsupervised agents on the same worktrees/branches. We
                    // can't gracefully teardown (that writes to the DB, which also fails
                    // against a too-new schema); just reap the processes and release their
                    // names. Journal recovery reclaims the tasks on restart.
                    for r in reviewers.drain(..) {
                        r.proc.kill_and_reap().await;
                        name_pool.release(&r.agent_name);
                    }
                    for w in workers.drain(..) {
                        w.proc.kill_and_reap().await;
                        name_pool.release(&w.agent_name);
                    }
                    return Ok(EXIT_SELF_UPDATE);
                }
                TickErrorAction::Continue => log(&format!("tick error: {e}")),
            }
        }

        // Heartbeat is refreshed by the standalone heartbeat_task (see above).
    }
}

#[allow(clippy::too_many_arguments)]
async fn tick(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    workers: &mut Vec<SlotState>,
    reviewers: &mut Vec<SlotState>,
    pending_reviews: &mut Vec<PendingReview>,
    poison_tracker: &mut PoisonTracker,
    reviewer_provision_tracker: &mut ReviewerProvisionTracker,
    drain_state: &mut DrainState,
    lifetime_roster: &mut LifetimeRoster,
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
            } else if lifetime_roster.owns(&row.agent) {
                log(&format!(
                    "consuming unmatched task_update from {} (no active worker)",
                    row.agent
                ));
            } else {
                // #181: row belongs to another daemon's agent — leave it.
                if lifetime_roster.log_foreign_once(&row.agent) {
                    log(&format!(
                        "leaving task_update from {} unconsumed (not in this instance's roster)",
                        row.agent
                    ));
                }
                continue;
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

            // #206: gate the verdict before acting on it. An `approved` row
            // without the zero-blocking attestation payload (i.e. one that
            // did not come through the validated CLI, or that attests
            // blocking findings) is demoted to `changes` — the daemon never
            // merges on it.
            let gated = crate::verdict::gate(row.verdict.as_deref(), row.payload.as_deref());
            if let Some(reason) = &gated.demotion_reason {
                log(&format!(
                    "VERDICT GATE: reviewer {} — {}",
                    reviewers[ri].agent_name, reason
                ));
            }

            match gated.verdict.as_deref() {
                Some("approved") => {
                    let Some(pr_num) = row.pr else {
                        log(&format!(
                            "WARN: reviewer {} approved but missing PR number \
                             — skipping merge",
                            reviewers[ri].agent_name
                        ));
                        let r = reviewers.remove(ri);
                        teardown_reviewer(config, wt_mgr, name_pool, r).await;
                        if !consume_mailbox_row(&db_path, *id).await {
                            break;
                        }
                        continue;
                    };

                    // Lifecycle: in-review → merging
                    if fire_event(
                        &db_path,
                        &reviewers[ri].agent_name,
                        reviewer_task_id,
                        &Event::VerdictApprove,
                    )
                    .await
                    .is_none()
                    {
                        log("VerdictApprove transition failed — skipping merge");
                        let r = reviewers.remove(ri);
                        teardown_reviewer(config, wt_mgr, name_pool, r).await;
                        // C3 belt-and-suspenders: clear worker.pr so Phase 5
                        // doesn't spawn another reviewer for a rejected task.
                        if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id)
                        {
                            workers[wi].pr = None;
                        }
                        if !consume_mailbox_row(&db_path, *id).await {
                            break;
                        }
                        continue;
                    }

                    // Pre-merge mergeability check: if the PR is conflicting
                    // (base moved since branch cut), fire MergeFailed and
                    // auto-rework.
                    let mergeability = {
                        let repo = config.repo_dir.clone();
                        let executor = Arc::clone(&config.merge_executor);
                        tokio::task::spawn_blocking(move || {
                            executor.check_mergeability(pr_num, &repo)
                        })
                        .await
                        .map_err(|e| {
                            QuorumError::Io(format!("mergeability spawn_blocking join: {e}"))
                        })?
                    };

                    if mergeability == merge::MergeabilityState::Conflicting {
                        log(&format!("PR #{pr_num} is CONFLICTING — firing MergeFailed"));
                        // merging → in-review
                        fire_event(
                            &db_path,
                            "system",
                            reviewer_task_id,
                            &Event::MergeFailed {
                                reason: format!(
                                    "PR #{pr_num} has conflicts with {}",
                                    config.base_branch
                                ),
                            },
                        )
                        .await;
                        // in-review → rework (lifecycle checks rework cap)
                        let reviewer_name = reviewers[ri].agent_name.clone();
                        let vc = fire_event(
                            &db_path,
                            &reviewer_name,
                            reviewer_task_id,
                            &Event::VerdictChanges,
                        )
                        .await;
                        match vc {
                            Some(ref tr) if tr.task.status == "rework" => {
                                let rework_msg = format!(
                                    "PR #{pr_num} has conflicts with {} \
                                     (a sibling PR likely merged first).\n\n\
                                     Rebase on {}, resolve conflicts, \
                                     and push again.",
                                    config.base_branch, config.base_branch
                                );
                                // Reviewer stays alive (sticky-agent).
                                if let Some(wi) =
                                    workers.iter().position(|w| w.task_id == reviewer_task_id)
                                {
                                    let rework_turn = reviewer::build_rework_turn(
                                        &workers[wi].agent_name,
                                        workers[wi].task_id,
                                        pr_num,
                                        &rework_msg,
                                        workers[wi].cost_usd,
                                        config.limits.max_task_cost_usd,
                                    );
                                    if let Err(e) = workers[wi].proc.feed_turn(&rework_turn).await {
                                        log(&format!(
                                            "conflict rework feed failed: {e} — cleaning up"
                                        ));
                                        let w = workers.remove(wi);
                                        fire_event(
                                            &db_path,
                                            &w.agent_name,
                                            w.task_id,
                                            &Event::AgentFailed {
                                                reason: format!("rework feed failed: {e}"),
                                            },
                                        )
                                        .await;
                                        cleanup_slot(config, wt_mgr, name_pool, w, None).await;
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
                                        .ok();
                                        log(&format!(
                                            "worker {} rework #{} (pre-merge conflict)",
                                            w.agent_name, w.rework_count
                                        ));
                                    }
                                } else if let Some(pi) = pending_reviews
                                    .iter()
                                    .position(|p| p.task_id == reviewer_task_id)
                                {
                                    let pending = pending_reviews.remove(pi);
                                    let next_round = pending.rework_count + 1;
                                    let rework_turn = reviewer::build_rework_turn(
                                        &pending.agent_name,
                                        pending.task_id,
                                        pr_num,
                                        &rework_msg,
                                        pending.cost_usd,
                                        config.limits.max_task_cost_usd,
                                    );
                                    spawn_resume_worker_for_pending(
                                        config,
                                        wt_mgr,
                                        name_pool,
                                        workers,
                                        pending,
                                        &rework_turn,
                                        next_round,
                                    )
                                    .await?;
                                } else {
                                    fire_event(
                                        &db_path,
                                        "daemon",
                                        reviewer_task_id,
                                        &Event::AgentFailed {
                                            reason: "no worker for rework".into(),
                                        },
                                    )
                                    .await;
                                }
                            }
                            Some(_) => {
                                // Rework cap exceeded → failed. Clean up.
                                let r = reviewers.remove(ri);
                                teardown_reviewer(config, wt_mgr, name_pool, r).await;
                                if let Some(wi) =
                                    workers.iter().position(|w| w.task_id == reviewer_task_id)
                                {
                                    let w = workers.remove(wi);
                                    cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                                } else if let Some(pi) = pending_reviews
                                    .iter()
                                    .position(|p| p.task_id == reviewer_task_id)
                                {
                                    let p = pending_reviews.remove(pi);
                                    cleanup_pending(config, wt_mgr, name_pool, p).await;
                                }
                            }
                            None => {
                                // VerdictChanges failed — clean up.
                                let r = reviewers.remove(ri);
                                teardown_reviewer(config, wt_mgr, name_pool, r).await;
                            }
                        }
                        if !consume_mailbox_row(&db_path, *id).await {
                            break;
                        }
                        continue;
                    }

                    log(&format!(
                        "verdict: approved — waiting for checks on PR #{pr_num}"
                    ));
                    let checks_outcome = {
                        let repo = config.repo_dir.clone();
                        let executor = Arc::clone(&config.merge_executor);
                        let timeout = config.merge_checks_timeout_secs;
                        let poll = config.merge_checks_poll_secs;
                        tokio::task::spawn_blocking(move || {
                            executor.wait_for_checks(pr_num, &repo, timeout, poll)
                        })
                        .await
                        .map_err(|e| QuorumError::Io(format!("checks spawn_blocking join: {e}")))?
                    };

                    match checks_outcome {
                        merge::ChecksOutcome::Failed { failing_checks } => {
                            let names = failing_checks.join(", ");
                            log(&format!(
                                "PR #{pr_num} checks failed: {names} — firing MergeFailed"
                            ));
                            // merging → in-review
                            fire_event(
                                &db_path,
                                "system",
                                reviewer_task_id,
                                &Event::MergeFailed {
                                    reason: format!("CI checks failed for PR #{pr_num}: {names}"),
                                },
                            )
                            .await;
                            // in-review → rework (lifecycle checks rework cap)
                            let reviewer_name = reviewers[ri].agent_name.clone();
                            let vc = fire_event(
                                &db_path,
                                &reviewer_name,
                                reviewer_task_id,
                                &Event::VerdictChanges,
                            )
                            .await;
                            match vc {
                                Some(ref tr) if tr.task.status == "rework" => {
                                    let rework_msg = format!(
                                        "CI checks failed for PR #{pr_num}: {names}\n\n\
                                         Fix the failing checks and push again.",
                                    );
                                    // Reviewer stays alive (sticky-agent).
                                    if let Some(wi) =
                                        workers.iter().position(|w| w.task_id == reviewer_task_id)
                                    {
                                        let rework_turn = reviewer::build_rework_turn(
                                            &workers[wi].agent_name,
                                            workers[wi].task_id,
                                            pr_num,
                                            &rework_msg,
                                            workers[wi].cost_usd,
                                            config.limits.max_task_cost_usd,
                                        );
                                        if let Err(e) =
                                            workers[wi].proc.feed_turn(&rework_turn).await
                                        {
                                            log(&format!(
                                                "checks-failure rework feed failed: {e} — cleaning up"
                                            ));
                                            let w = workers.remove(wi);
                                            fire_event(
                                                &db_path,
                                                &w.agent_name,
                                                w.task_id,
                                                &Event::AgentFailed {
                                                    reason: format!("rework feed failed: {e}"),
                                                },
                                            )
                                            .await;
                                            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
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
                                            .ok();
                                            log(&format!(
                                                "worker {} rework #{} (checks failure)",
                                                w.agent_name, w.rework_count
                                            ));
                                        }
                                    } else if let Some(pi) = pending_reviews
                                        .iter()
                                        .position(|p| p.task_id == reviewer_task_id)
                                    {
                                        let pending = pending_reviews.remove(pi);
                                        let next_round = pending.rework_count + 1;
                                        let rework_turn = reviewer::build_rework_turn(
                                            &pending.agent_name,
                                            pending.task_id,
                                            pr_num,
                                            &rework_msg,
                                            pending.cost_usd,
                                            config.limits.max_task_cost_usd,
                                        );
                                        spawn_resume_worker_for_pending(
                                            config,
                                            wt_mgr,
                                            name_pool,
                                            workers,
                                            pending,
                                            &rework_turn,
                                            next_round,
                                        )
                                        .await?;
                                    } else {
                                        fire_event(
                                            &db_path,
                                            "daemon",
                                            reviewer_task_id,
                                            &Event::AgentFailed {
                                                reason: "no worker for rework".into(),
                                            },
                                        )
                                        .await;
                                    }
                                }
                                Some(_) => {
                                    // Rework cap exceeded → failed. Clean up.
                                    let r = reviewers.remove(ri);
                                    teardown_reviewer(config, wt_mgr, name_pool, r).await;
                                    if let Some(wi) =
                                        workers.iter().position(|w| w.task_id == reviewer_task_id)
                                    {
                                        let w = workers.remove(wi);
                                        cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                                    } else if let Some(pi) = pending_reviews
                                        .iter()
                                        .position(|p| p.task_id == reviewer_task_id)
                                    {
                                        let p = pending_reviews.remove(pi);
                                        cleanup_pending(config, wt_mgr, name_pool, p).await;
                                    }
                                }
                                None => {
                                    let r = reviewers.remove(ri);
                                    teardown_reviewer(config, wt_mgr, name_pool, r).await;
                                }
                            }
                            if !consume_mailbox_row(&db_path, *id).await {
                                break;
                            }
                            continue;
                        }
                        merge::ChecksOutcome::TimedOut => {
                            log(&format!(
                                "MERGE BLOCKED: PR #{pr_num} checks timed out after \
                                 {}s — cancelling task",
                                config.merge_checks_timeout_secs
                            ));
                            fire_event(
                                &db_path,
                                "system",
                                reviewer_task_id,
                                &Event::Cancelled {
                                    by: "daemon".into(),
                                },
                            )
                            .await;
                            let r = reviewers.remove(ri);
                            teardown_reviewer(config, wt_mgr, name_pool, r).await;
                            if let Some(wi) =
                                workers.iter().position(|w| w.task_id == reviewer_task_id)
                            {
                                let w = workers.remove(wi);
                                cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                            } else if let Some(pi) = pending_reviews
                                .iter()
                                .position(|p| p.task_id == reviewer_task_id)
                            {
                                let p = pending_reviews.remove(pi);
                                cleanup_pending(config, wt_mgr, name_pool, p).await;
                            }
                            if !consume_mailbox_row(&db_path, *id).await {
                                break;
                            }
                            continue;
                        }
                        merge::ChecksOutcome::Ready => {
                            log(&format!(
                                "checks passed for PR #{pr_num} — proceeding to merge"
                            ));
                        }
                    }

                    // #228: persist the approval durably (instance-independent)
                    // BEFORE merging, so a self-update-drain restart that lands
                    // between approval and merge reconstructs "merge this PR"
                    // from state instead of re-working the task. The record is
                    // deleted right after a successful merge below. Best-effort:
                    // a capture failure must not block the merge.
                    {
                        let reviewer_name = reviewers[ri].agent_name.clone();
                        let author = workers
                            .iter()
                            .find(|w| w.task_id == reviewer_task_id)
                            .map(|w| w.agent_name.clone())
                            .or_else(|| {
                                pending_reviews
                                    .iter()
                                    .find(|p| p.task_id == reviewer_task_id)
                                    .map(|p| p.agent_name.clone())
                            });
                        if let Some(author) = author {
                            let repo = config.repo_dir.clone();
                            let executor = Arc::clone(&config.merge_executor);
                            let head = tokio::task::spawn_blocking(move || {
                                executor.head_sha(pr_num, &repo)
                            })
                            .await
                            .ok()
                            .flatten();
                            if let Some(head) = head {
                                let p = db_path.clone();
                                let record = quorum_core::approvals::Approval {
                                    pr_number: pr_num,
                                    task_id: reviewer_task_id,
                                    author,
                                    reviewer: reviewer_name,
                                    verdict: "approved".to_string(),
                                    blocking_count: 0,
                                    approved_head_sha: head,
                                };
                                tokio::task::spawn_blocking(move || -> Result<()> {
                                    let mut conn = quorum_core::db::open(&p)?;
                                    quorum_core::approvals::record(&mut conn, &record)
                                })
                                .await
                                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                                .ok();
                            }
                        }
                    }

                    let merge_result = {
                        let repo = config.repo_dir.clone();
                        let executor = Arc::clone(&config.merge_executor);
                        let merge_ctx = merge::MergeContext {
                            reviewer_name: reviewers[ri].agent_name.clone(),
                            review_task_id: reviewer_task_id,
                        };
                        tokio::task::spawn_blocking(move || {
                            executor.merge(pr_num, &repo, &merge_ctx)
                        })
                        .await
                        .map_err(|e| QuorumError::Io(format!("merge spawn_blocking join: {e}")))?
                    };

                    // #228: the merge was attempted by this live instance —
                    // whatever the outcome (merged / reworked / parked), the
                    // durable "awaiting merge" record has served its purpose.
                    // Drop it so restart recovery never re-merges a PR this
                    // instance already handled.
                    {
                        let p = db_path.clone();
                        tokio::task::spawn_blocking(move || -> Result<()> {
                            let mut conn = quorum_core::db::open(&p)?;
                            quorum_core::approvals::delete(&mut conn, pr_num)?;
                            Ok(())
                        })
                        .await
                        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                        .ok();
                    }

                    if merge_result.success {
                        log(&format!("PR #{pr_num} merged — firing MergeSucceeded"));
                        fire_event(&db_path, "system", reviewer_task_id, &Event::MergeSucceeded)
                            .await;
                        if config.self_update_drain && config.self_repo.is_some() {
                            let sha = format!("post-merge-pr-{pr_num}");
                            drain_state.start_drain(&sha);
                        }
                        let r = reviewers.remove(ri);
                        teardown_reviewer(config, wt_mgr, name_pool, r).await;
                        if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id)
                        {
                            let w = workers.remove(wi);
                            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                        } else if let Some(pi) = pending_reviews
                            .iter()
                            .position(|p| p.task_id == reviewer_task_id)
                        {
                            let p = pending_reviews.remove(pi);
                            cleanup_pending(config, wt_mgr, name_pool, p).await;
                        }
                        reviewer_provision_tracker.clear(reviewer_task_id, pr_num);
                    } else {
                        let failure_kind = merge_result
                            .failure_kind
                            .unwrap_or(merge::MergeFailureKind::PolicyBlocked);

                        match failure_kind {
                            merge::MergeFailureKind::PolicyBlocked => {
                                log(&format!(
                                    "MERGE BLOCKED: PR #{pr_num} merge failed \
                                     (not worker-fixable): {} — cancelling task",
                                    merge_result.message
                                ));
                                fire_event(
                                    &db_path,
                                    "system",
                                    reviewer_task_id,
                                    &Event::Cancelled {
                                        by: "daemon".into(),
                                    },
                                )
                                .await;
                                let r = reviewers.remove(ri);
                                teardown_reviewer(config, wt_mgr, name_pool, r).await;
                                if let Some(wi) =
                                    workers.iter().position(|w| w.task_id == reviewer_task_id)
                                {
                                    let w = workers.remove(wi);
                                    cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                                } else if let Some(pi) = pending_reviews
                                    .iter()
                                    .position(|p| p.task_id == reviewer_task_id)
                                {
                                    let p = pending_reviews.remove(pi);
                                    cleanup_pending(config, wt_mgr, name_pool, p).await;
                                }
                            }
                            merge::MergeFailureKind::Retryable => {
                                log(&format!(
                                    "PR #{pr_num} merge failed (retryable): {} \
                                     — firing MergeFailed",
                                    merge_result.message
                                ));
                                // merging → in-review
                                fire_event(
                                    &db_path,
                                    "system",
                                    reviewer_task_id,
                                    &Event::MergeFailed {
                                        reason: format!(
                                            "Merge of PR #{pr_num} failed: {}",
                                            merge_result.message
                                        ),
                                    },
                                )
                                .await;
                                // in-review → rework
                                let reviewer_name = reviewers[ri].agent_name.clone();
                                let vc = fire_event(
                                    &db_path,
                                    &reviewer_name,
                                    reviewer_task_id,
                                    &Event::VerdictChanges,
                                )
                                .await;
                                match vc {
                                    Some(ref tr) if tr.task.status == "rework" => {
                                        let rework_msg = format!(
                                            "Merge of PR #{pr_num} failed: {}\n\n\
                                             Rebase on {}, resolve any conflicts, \
                                             and push again.",
                                            merge_result.message, config.base_branch
                                        );
                                        // Reviewer stays alive (sticky-agent).
                                        if let Some(wi) = workers
                                            .iter()
                                            .position(|w| w.task_id == reviewer_task_id)
                                        {
                                            let rework_turn = reviewer::build_rework_turn(
                                                &workers[wi].agent_name,
                                                workers[wi].task_id,
                                                pr_num,
                                                &rework_msg,
                                                workers[wi].cost_usd,
                                                config.limits.max_task_cost_usd,
                                            );
                                            if let Err(e) =
                                                workers[wi].proc.feed_turn(&rework_turn).await
                                            {
                                                log(&format!(
                                                    "merge-failure rework feed failed: {e} — cleaning up"
                                                ));
                                                let w = workers.remove(wi);
                                                fire_event(
                                                    &db_path,
                                                    &w.agent_name,
                                                    w.task_id,
                                                    &Event::AgentFailed {
                                                        reason: format!("rework feed failed: {e}"),
                                                    },
                                                )
                                                .await;
                                                cleanup_slot(config, wt_mgr, name_pool, w, None)
                                                    .await;
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
                                                let entry =
                                                    slot_journal_entry(w, "worker", "working");
                                                tokio::task::spawn_blocking(
                                                    move || -> Result<()> {
                                                        let mut conn = quorum_core::db::open(&p)?;
                                                        journal::upsert(&mut conn, &entry)
                                                    },
                                                )
                                                .await
                                                .ok();
                                                log(&format!(
                                                    "worker {} rework #{} (merge failure)",
                                                    w.agent_name, w.rework_count
                                                ));
                                            }
                                        } else if let Some(pi) = pending_reviews
                                            .iter()
                                            .position(|p| p.task_id == reviewer_task_id)
                                        {
                                            let pending = pending_reviews.remove(pi);
                                            let next_round = pending.rework_count + 1;
                                            let rework_turn = reviewer::build_rework_turn(
                                                &pending.agent_name,
                                                pending.task_id,
                                                pr_num,
                                                &rework_msg,
                                                pending.cost_usd,
                                                config.limits.max_task_cost_usd,
                                            );
                                            spawn_resume_worker_for_pending(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                workers,
                                                pending,
                                                &rework_turn,
                                                next_round,
                                            )
                                            .await?;
                                        } else {
                                            fire_event(
                                                &db_path,
                                                "daemon",
                                                reviewer_task_id,
                                                &Event::AgentFailed {
                                                    reason: "no worker for rework".into(),
                                                },
                                            )
                                            .await;
                                        }
                                    }
                                    Some(_) => {
                                        // Rework cap exceeded → failed. Clean up.
                                        let r = reviewers.remove(ri);
                                        teardown_reviewer(config, wt_mgr, name_pool, r).await;
                                        if let Some(wi) = workers
                                            .iter()
                                            .position(|w| w.task_id == reviewer_task_id)
                                        {
                                            let w = workers.remove(wi);
                                            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                                        } else if let Some(pi) = pending_reviews
                                            .iter()
                                            .position(|p| p.task_id == reviewer_task_id)
                                        {
                                            let p = pending_reviews.remove(pi);
                                            cleanup_pending(config, wt_mgr, name_pool, p).await;
                                        }
                                    }
                                    None => {
                                        let r = reviewers.remove(ri);
                                        teardown_reviewer(config, wt_mgr, name_pool, r).await;
                                    }
                                }
                            }
                        }
                    }
                }
                Some("changes") => {
                    // On a #206 demotion the demotion reason leads, but any
                    // feedback the row carried is appended — never dropped —
                    // so the worker still sees the reviewer's actual notes.
                    let feedback_owned = match (&gated.demotion_reason, &row.feedback) {
                        (Some(reason), Some(fb)) => {
                            format!("{reason}\n\nReviewer feedback:\n{fb}")
                        }
                        (Some(reason), None) => reason.clone(),
                        (None, Some(fb)) => fb.clone(),
                        (None, None) => "Changes requested.".to_string(),
                    };
                    let feedback = feedback_owned.as_str();
                    log(&format!(
                        "verdict: changes — feeding rework to worker (feedback: {feedback})"
                    ));

                    // Mirror the changes verdict to GitHub (best-effort).
                    if let Some(pr_num) = row.pr {
                        let repo = config.repo_dir.clone();
                        let executor = Arc::clone(&config.merge_executor);
                        let fb = feedback.to_string();
                        match tokio::task::spawn_blocking(move || {
                            executor.request_changes(pr_num, &repo, &fb)
                        })
                        .await
                        {
                            Ok(r) if r.success => {
                                log(&format!("posted REQUEST_CHANGES on PR #{pr_num}"));
                            }
                            Ok(r) => log(&format!(
                                "REQUEST_CHANGES on PR #{pr_num} failed (non-blocking): {}",
                                r.message
                            )),
                            Err(e) => log(&format!(
                                "REQUEST_CHANGES spawn_blocking join failed (non-blocking): {e}"
                            )),
                        }
                    }

                    // Fire VerdictChanges lifecycle event (lifecycle enforces rework cap).
                    let reviewer_name = reviewers[ri].agent_name.clone();
                    let vc = fire_event(
                        &db_path,
                        &reviewer_name,
                        reviewer_task_id,
                        &Event::VerdictChanges,
                    )
                    .await;
                    match vc {
                        Some(ref tr) if tr.task.status == "rework" => {
                            // Reviewer stays alive (sticky-agent policy).
                            if let Some(wi) =
                                workers.iter().position(|w| w.task_id == reviewer_task_id)
                            {
                                let rework_pr = workers[wi].pr.unwrap_or_else(|| {
                                    log(&format!(
                                        "WARN: worker {} rework has no PR number",
                                        workers[wi].agent_name
                                    ));
                                    0
                                });
                                let rework_turn = reviewer::build_rework_turn(
                                    &workers[wi].agent_name,
                                    workers[wi].task_id,
                                    rework_pr,
                                    feedback,
                                    workers[wi].cost_usd,
                                    config.limits.max_task_cost_usd,
                                );
                                if let Err(e) = workers[wi].proc.feed_turn(&rework_turn).await {
                                    log(&format!("rework feed_turn failed: {e} — cleaning up"));
                                    let w = workers.remove(wi);
                                    fire_event(
                                        &db_path,
                                        &w.agent_name,
                                        w.task_id,
                                        &Event::AgentFailed {
                                            reason: format!("rework feed failed: {e}"),
                                        },
                                    )
                                    .await;
                                    cleanup_slot(config, wt_mgr, name_pool, w, None).await;
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
                                    .ok();
                                    log(&format!(
                                        "worker {} rework #{} started",
                                        w.agent_name, w.rework_count
                                    ));
                                }
                            } else if let Some(pi) = pending_reviews
                                .iter()
                                .position(|p| p.task_id == reviewer_task_id)
                            {
                                let pending = pending_reviews.remove(pi);
                                let next_round = pending.rework_count + 1;
                                let rework_pr = pending.pr;
                                let rework_turn = reviewer::build_rework_turn(
                                    &pending.agent_name,
                                    pending.task_id,
                                    rework_pr,
                                    feedback,
                                    pending.cost_usd,
                                    config.limits.max_task_cost_usd,
                                );
                                spawn_resume_worker_for_pending(
                                    config,
                                    wt_mgr,
                                    name_pool,
                                    workers,
                                    pending,
                                    &rework_turn,
                                    next_round,
                                )
                                .await?;
                            } else {
                                log("no worker/pending for rework — releasing task");
                                fire_event(
                                    &db_path,
                                    "daemon",
                                    reviewer_task_id,
                                    &Event::AgentFailed {
                                        reason: "no worker for rework".into(),
                                    },
                                )
                                .await;
                            }
                        }
                        Some(_) => {
                            // Rework cap exceeded → failed. Clean up both.
                            let r = reviewers.remove(ri);
                            teardown_reviewer(config, wt_mgr, name_pool, r).await;
                            if let Some(wi) =
                                workers.iter().position(|w| w.task_id == reviewer_task_id)
                            {
                                let w = workers.remove(wi);
                                cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                            } else if let Some(pi) = pending_reviews
                                .iter()
                                .position(|p| p.task_id == reviewer_task_id)
                            {
                                let p = pending_reviews.remove(pi);
                                cleanup_pending(config, wt_mgr, name_pool, p).await;
                            }
                        }
                        None => {
                            let r = reviewers.remove(ri);
                            teardown_reviewer(config, wt_mgr, name_pool, r).await;
                        }
                    }
                }
                _ => {
                    log(&format!(
                        "reviewer {} done without verdict — firing AgentFailed",
                        row.agent
                    ));
                    let r = reviewers.remove(ri);
                    fire_event(
                        &db_path,
                        &r.agent_name,
                        reviewer_task_id,
                        &Event::AgentFailed {
                            reason: "reviewer exited without verdict".into(),
                        },
                    )
                    .await;
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

            // #206 defense-in-depth: verdicts are only actionable from the
            // task's spawned reviewer (matched above). A worker posting one
            // is trying to review its own delivery — surface it loudly.
            if row.verdict.is_some() {
                log(&format!(
                    "INTEGRITY: worker {} posted a verdict ({:?}) on its own \
                     delivery — ignored (#206: the deliverer cannot review)",
                    workers[wi].agent_name, row.verdict
                ));
            }

            if let Some(pr) = row.pr {
                // Fire the appropriate lifecycle event based on whether
                // this is the first done signal or a rework-pushed.
                let event = if workers[wi].rework_count > 0 {
                    Event::ReworkPushed
                } else {
                    Event::SignaledDone { pr: pr.to_string() }
                };
                let tr = fire_event(
                    &db_path,
                    &workers[wi].agent_name,
                    workers[wi].task_id,
                    &event,
                )
                .await;

                match tr {
                    Some(tr) => {
                        workers[wi].pr = Some(pr);

                        // Dispatch lifecycle effects.
                        for effect in &tr.effects {
                            match effect {
                                Effect::ResumeReviewer => {
                                    // C6: tear down existing reviewer so Phase 5
                                    // respawns a fresh one with current PR context.
                                    if let Some(ri) = reviewers
                                        .iter()
                                        .position(|r| r.task_id == workers[wi].task_id)
                                    {
                                        log(&format!(
                                            "ResumeReviewer: tearing down reviewer \
                                             for task #{}",
                                            workers[wi].task_id
                                        ));
                                        let r = reviewers.remove(ri);
                                        teardown_reviewer(config, wt_mgr, name_pool, r).await;
                                    }
                                }
                                other => {
                                    log(&format!(
                                        "WARN: unhandled effect {} at done-signal site",
                                        tasks::effect_name(other)
                                    ));
                                }
                            }
                        }

                        // Worker stays alive (sticky-agent policy).
                        // #178: persist PR to journal so a restart resumes at the
                        // review stage instead of re-executing the task from
                        // scratch.
                        let p = db_path.clone();
                        let entry = slot_journal_entry(&workers[wi], "worker", "awaiting-review");
                        tokio::task::spawn_blocking(move || -> Result<()> {
                            let mut conn = quorum_core::db::open(&p)?;
                            journal::upsert(&mut conn, &entry)
                        })
                        .await
                        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                        .ok();
                        log(&format!(
                            "worker {} PR #{} ready for review",
                            workers[wi].agent_name, pr
                        ));
                    }
                    None => {
                        // C3: transition rejected (e.g. task externally
                        // cancelled). Do NOT set worker.pr — clean up instead.
                        log(&format!(
                            "lifecycle rejected at done signal for worker {} \
                             — cleaning up slot",
                            workers[wi].agent_name
                        ));
                        let w = workers.remove(wi);
                        fire_event(
                            &db_path,
                            &w.agent_name,
                            w.task_id,
                            &Event::AgentFailed {
                                reason: "lifecycle transition rejected at done signal".into(),
                            },
                        )
                        .await;
                        cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                    }
                }
            } else {
                // Done without PR — close directly (no review needed).
                let w = workers.remove(wi);
                let p = db_path.clone();
                let task_id = w.task_id;
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut conn = quorum_core::db::open(&p)?;
                    let now = now_unix();
                    tasks::close_after_merge(&mut conn, task_id, "done without PR", now)?;
                    Ok(())
                })
                .await
                .ok();
                cleanup_slot(config, wt_mgr, name_pool, w, Some("done")).await;
            }

            if !consume_mailbox_row(&db_path, *id).await {
                break;
            }
            break;
        }

        // F9 + #181: Done row matches neither worker nor reviewer.
        //
        // If this daemon has ever owned the agent name, the row is a phantom
        // from a prior turn we own — consume it (F9) so it doesn't re-poll and
        // so a future name-reuse doesn't fire a phantom verdict.
        //
        // If this daemon has NEVER owned the name, the row belongs to another
        // instance's agent (two-instance topology, shared SQLite queue) — leave
        // it for the owning daemon to process (#181). Consuming would destroy
        // the sibling's lifecycle signal.
        if lifetime_roster.owns(&row.agent) {
            log(&format!(
                "consuming unmatched Done row from {} (matches no active agent)",
                row.agent
            ));
            if !consume_mailbox_row(&db_path, *id).await {
                break;
            }
        } else if lifetime_roster.log_foreign_once(&row.agent) {
            log(&format!(
                "leaving Done row from {} unconsumed (not in this instance's roster)",
                row.agent
            ));
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
        fire_event(
            &db_path,
            &dead.agent_name,
            dead.task_id,
            &Event::AgentFailed {
                reason: "reviewer killed by watchdog".into(),
            },
        )
        .await;
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
        fire_event(
            &db_path,
            &dead.agent_name,
            dead.task_id,
            &Event::AgentFailed {
                reason: "worker killed by watchdog".into(),
            },
        )
        .await;
        cleanup_slot(config, wt_mgr, name_pool, dead, None).await;
    }

    // ── Phase 4a-drain: Tear down idle agents during drain ──────────────
    // During drain, agents that have finished their current turn (draining=false)
    // should be torn down immediately — no new reviewers/work will be spawned.
    if drain_state.draining {
        let mut drain_workers: Vec<usize> = Vec::new();
        for (i, w) in workers.iter().enumerate() {
            if !w.draining {
                drain_workers.push(i);
            }
        }
        for &i in drain_workers.iter().rev() {
            let w = workers.remove(i);
            log(&format!(
                "DRAIN: tearing down idle worker {} (task #{})",
                w.agent_name, w.task_id
            ));
            fire_event(
                &db_path,
                &w.agent_name,
                w.task_id,
                &Event::AgentFailed {
                    reason: "daemon draining".into(),
                },
            )
            .await;
            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
        }

        let mut drain_reviewers: Vec<usize> = Vec::new();
        for (i, r) in reviewers.iter().enumerate() {
            if !r.draining {
                drain_reviewers.push(i);
            }
        }
        for &i in drain_reviewers.iter().rev() {
            let r = reviewers.remove(i);
            log(&format!(
                "DRAIN: tearing down idle reviewer {}",
                r.agent_name
            ));
            fire_event(
                &db_path,
                &r.agent_name,
                r.task_id,
                &Event::AgentFailed {
                    reason: "daemon draining".into(),
                },
            )
            .await;
            teardown_reviewer(config, wt_mgr, name_pool, r).await;
        }

        // #178: pending_reviews without a paired reviewer are idle by
        // definition; during drain no new reviewers will be provisioned.
        // Release the task back to `open` so a future daemon picks up at
        // the review stage on restart. (If a reviewer IS paired, wait for
        // its verdict — that path drains normally through the reviewer.)
        let mut drain_pending: Vec<usize> = Vec::new();
        for (i, p) in pending_reviews.iter().enumerate() {
            if !reviewers.iter().any(|r| r.task_id == p.task_id) {
                drain_pending.push(i);
            }
        }
        for &i in drain_pending.iter().rev() {
            let p = pending_reviews.remove(i);
            log(&format!(
                "DRAIN: releasing pending review {} (task #{})",
                p.agent_name, p.task_id
            ));
            fire_event(
                &db_path,
                &p.agent_name,
                p.task_id,
                &Event::AgentFailed {
                    reason: "daemon draining".into(),
                },
            )
            .await;
            cleanup_pending(config, wt_mgr, name_pool, p).await;
        }
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
    for &i in dead_workers.iter().rev() {
        let dead = workers.remove(i);
        let instant_death = dead.cost_tokens == 0;
        if instant_death {
            let strikes = poison_tracker.record_strike(dead.task_id);
            if strikes >= MAX_POISON_STRIKES {
                let task_id = dead.task_id;
                fire_event(
                    &db_path,
                    "daemon",
                    task_id,
                    &Event::Cancelled {
                        by: format!(
                            "daemon: poisoned — worker died {strikes} time(s) without producing output"
                        ),
                    },
                )
                .await;
                cleanup_slot(config, wt_mgr, name_pool, dead, None).await;
                log(&format!(
                    "POISON: task #{task_id} cancelled after {strikes} strikes"
                ));
            } else {
                log(&format!(
                    "POISON: task #{} strike {strikes}/{MAX_POISON_STRIKES}",
                    dead.task_id
                ));
                fire_event(
                    &db_path,
                    &dead.agent_name,
                    dead.task_id,
                    &Event::AgentFailed {
                        reason: "worker process died (instant death)".into(),
                    },
                )
                .await;
                cleanup_slot(config, wt_mgr, name_pool, dead, None).await;
            }
        } else {
            poison_tracker.clear(dead.task_id);
            fire_event(
                &db_path,
                &dead.agent_name,
                dead.task_id,
                &Event::AgentFailed {
                    reason: "worker process died".into(),
                },
            )
            .await;
            cleanup_slot(config, wt_mgr, name_pool, dead, None).await;
        }
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
        fire_event(
            &db_path,
            &dead.agent_name,
            dead.task_id,
            &Event::AgentFailed {
                reason: "reviewer process died".into(),
            },
        )
        .await;
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
                let turn = agent::user_turn(&format!("MESSAGE from {}: {payload}", msg_row.agent));
                match workers[wi].proc.feed_turn(&turn).await {
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
                // #181: only reap messages addressed to agents we've ever owned.
                // Otherwise the row belongs to another instance's worker — leave it.
                if lifetime_roster.owns(target) {
                    log(&format!("consuming message to {target} (no active worker)"));
                    consume_mailbox_row(&db_path, *msg_id).await;
                } else if lifetime_roster.log_foreign_once(target) {
                    log(&format!(
                        "leaving message to {target} unconsumed (not in this instance's roster)"
                    ));
                }
            }
        }
    }

    // ── Phase 5: Spawn reviewers for workers with PRs ──────────────────
    // Each worker that has a PR and no paired reviewer (and is not draining)
    // gets a reviewer spawned. Reviewers don't consume worker capacity.
    // Skip during drain — no new work, let existing agents finish.
    //
    // #178: `pending_reviews` (restart-resurrected awaiting-review slots
    // with no live worker process) are also eligible — Phase 5 provisions
    // reviewers for them the same way as for live workers.
    if !drain_state.draining {
        let needs_reviewer_from_workers: Vec<(i64, i64, usize)> = workers
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
        let mut parked_workers: Vec<usize> = Vec::new();
        for (pr, task_id, wi) in &needs_reviewer_from_workers {
            if reviewer_provision_tracker.is_exhausted(*task_id, *pr) {
                log(&format!(
                    "reviewer provision exhausted for task #{task_id} PR #{pr} \
                     — parking worker"
                ));
                parked_workers.push(*wi);
                continue;
            }
            let counterpart: ReviewCounterpart = (&workers[*wi]).into();
            spawn_reviewer_for_worker(
                config,
                wt_mgr,
                name_pool,
                reviewers,
                reviewer_provision_tracker,
                lifetime_roster,
                *pr,
                counterpart,
            )
            .await?;
        }
        for &wi in parked_workers.iter().rev() {
            let w = workers.remove(wi);
            let pr_label =
                w.pr.map(|n| format!("#{n}"))
                    .unwrap_or_else(|| "unknown".to_string());
            teardown_worker_with_body(
                config,
                wt_mgr,
                name_pool,
                w,
                "cancelled",
                Some(&format!(
                    "{}provision-exhausted | PR {pr_label} | \
                     reviewer provision failed {MAX_REVIEWER_PROVISION_STRIKES} time(s)",
                    tasks::PARKED_BODY_PREFIX
                )),
            )
            .await;
        }

        // Provision reviewers for restart-recovered pending reviews (#178).
        let needs_reviewer_from_pending: Vec<(i64, i64, usize)> = pending_reviews
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                if !reviewers.iter().any(|r| r.task_id == p.task_id) {
                    Some((p.pr, p.task_id, i))
                } else {
                    None
                }
            })
            .collect();
        let mut parked_pending: Vec<usize> = Vec::new();
        for (pr, task_id, pi) in &needs_reviewer_from_pending {
            if reviewer_provision_tracker.is_exhausted(*task_id, *pr) {
                log(&format!(
                    "reviewer provision exhausted for task #{task_id} PR #{pr} \
                     — parking pending review"
                ));
                parked_pending.push(*pi);
                continue;
            }
            let counterpart: ReviewCounterpart = (&pending_reviews[*pi]).into();
            spawn_reviewer_for_worker(
                config,
                wt_mgr,
                name_pool,
                reviewers,
                reviewer_provision_tracker,
                lifetime_roster,
                *pr,
                counterpart,
            )
            .await?;
        }
        for &pi in parked_pending.iter().rev() {
            let p = pending_reviews.remove(pi);
            let pr = p.pr;
            teardown_pending_review(
                config,
                wt_mgr,
                name_pool,
                p,
                "cancelled",
                Some(&format!(
                    "daemon: reviewer provision failed {MAX_REVIEWER_PROVISION_STRIKES} \
                     time(s) for PR #{pr} — parking task"
                )),
            )
            .await;
        }
    }

    // ── Phase 6: Spawn workers up to cap ───────────────────────────────
    // Gate on worker count, not total in_use_count() — reviewers must
    // not consume worker capacity (F16).
    // Skip during drain — no new tasks, let existing agents finish.
    //
    // #178: pending_reviews (restart-recovered awaiting-review slots) are
    // passed in so their task_ids are excluded from claimable tasks — a
    // task whose lease was reaped to `open` while the daemon was down
    // must NOT be re-claimed by a fresh worker if a pending review is
    // handling it (would cause duplicate PRs, the very bug #178 fixes).
    if !drain_state.draining {
        while workers.len() < config.cap {
            if !spawn_worker(
                config,
                wt_mgr,
                name_pool,
                workers,
                pending_reviews,
                poison_tracker,
                lifetime_roster,
            )
            .await?
            {
                break;
            }
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
    TurnCostUsdMissing { max: f64 },
    TaskCostUsd { total: f64, max: f64 },
    TurnWallSecs { elapsed: u64, max: u64 },
    TaskWallSecs { elapsed: u64, max: u64 },
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
            Self::TurnCostUsdMissing { max } => {
                write!(f, "turn cost unavailable (fail-closed); limit ${max:.4}")
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
        match turn_cost_usd {
            Some(turn_cost) if turn_cost > max => {
                return Some(LimitBreached::TurnCostUsd {
                    turn: turn_cost,
                    max,
                });
            }
            None => {
                return Some(LimitBreached::TurnCostUsdMissing { max });
            }
            _ => {}
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
                slot.live_stats.mid_turn_tokens = 0;
                slot.live_stats.record_event();
                write_live_sidecar(slot);

                if let Some(ref mut sl) = slot.session_log {
                    sl.log_event(&event);
                }

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
                if let Some(usage) = message.get("usage") {
                    let input = usage
                        .get("input_tokens")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let output = usage
                        .get("output_tokens")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    if input + output > 0 {
                        slot.live_stats.mid_turn_tokens = input + output;
                    }
                }
                slot.live_stats.record_event();
                write_live_sidecar(slot);
            }
            stream::Event::ToolUse { name, input } => {
                slot.live_stats.tool_count += 1;
                slot.live_stats.now_label = now_label(name, input);
                slot.live_stats.record_event();
                write_live_sidecar(slot);
            }
            _ => {}
        }

        if let Some(ref mut sl) = slot.session_log {
            sl.log_event(&event);
        }
    }
    Ok(None)
}

fn write_live_sidecar(slot: &SlotState) {
    if let Some(ref sl) = slot.session_log {
        let path = sl.dir().join("_daemon_live.json");
        let stats = DaemonLiveStats {
            tools: slot.live_stats.tool_count,
            now: slot.live_stats.now_label.clone(),
            evm: slot.live_stats.events_per_min(),
            up_secs: slot.live_stats.uptime_secs(),
            mid_turn_tok: slot.live_stats.mid_turn_tokens,
            spawn_epoch: slot.live_stats.spawn_epoch,
        };
        if let Ok(json) = serde_json::to_string(&stats) {
            let _ = std::fs::write(&path, json);
        }
    }
}

fn now_label(name: &str, input: &serde_json::Value) -> String {
    let snippet = match name {
        "Bash" => input
            .get("command")
            .and_then(|c| c.as_str())
            .map(|c| c.split_whitespace().take(3).collect::<Vec<_>>().join(" ")),
        "Read" | "Write" | "Edit" => input
            .get("file_path")
            .and_then(|p| p.as_str())
            .map(|p| p.rsplit('/').next().unwrap_or(p).to_string()),
        "Grep" => input
            .get("pattern")
            .and_then(|p| p.as_str())
            .map(|s| s.to_string()),
        "Glob" => input
            .get("pattern")
            .and_then(|p| p.as_str())
            .map(|s| s.to_string()),
        "Agent" => input
            .get("description")
            .and_then(|d| d.as_str())
            .map(|s| s.to_string()),
        _ => None,
    };
    let full = match snippet {
        Some(s) => format!("{name}: {s}"),
        None => name.to_string(),
    };
    if full.chars().count() <= 24 {
        full
    } else {
        let t: String = full.chars().take(23).collect();
        format!("{t}…")
    }
}

/// A minimal view of the reviewer's counterpart — the agent whose PR is
/// under review. Works for both a live worker `SlotState` and a
/// `PendingReview` recovered from journal.
struct ReviewCounterpart<'a> {
    agent_name: &'a str,
    task_id: i64,
    branch: &'a str,
}

impl<'a> From<&'a SlotState> for ReviewCounterpart<'a> {
    fn from(w: &'a SlotState) -> Self {
        Self {
            agent_name: &w.agent_name,
            task_id: w.task_id,
            branch: &w.branch,
        }
    }
}

impl<'a> From<&'a PendingReview> for ReviewCounterpart<'a> {
    fn from(p: &'a PendingReview) -> Self {
        Self {
            agent_name: &p.agent_name,
            task_id: p.task_id,
            branch: &p.branch,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_reviewer_for_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    reviewers: &mut Vec<SlotState>,
    provision_tracker: &mut ReviewerProvisionTracker,
    lifetime_roster: &mut LifetimeRoster,
    pr: i64,
    worker: ReviewCounterpart<'_>,
) -> Result<()> {
    let acquire_result = name_pool.acquire();
    if acquire_result.is_generated() && name_pool.has_file() {
        log(&format!(
            "names pool exhausted, generated fallback reviewer name: {}",
            acquire_result.name()
        ));
    }
    let reviewer_name = acquire_result.into_name();
    // #181: register in the lifetime roster BEFORE any DB work so any concurrent
    // sibling daemon polling us now can see we own this name via future rows.
    lifetime_roster.register(&reviewer_name);

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
    // #180: resolve the correct clone directory from refs.repo — the worker's
    // branch lives on the task's repo remote, not necessarily config.repo_dir.
    // #162: if the worker's branch fails, fall back to the PR's actual head
    // ref from GitHub (covers review tasks where the worker never pushed).
    let task_repo_dir = &config.repo_dir;
    let provision_result = wt_mgr
        .fetch_and_provision(task_repo_dir, &branch, &wt_path, worker.branch)
        .await;
    let provision_ok = match provision_result {
        Ok(_) => true,
        Err(ref e) => {
            log(&format!(
                "reviewer worktree provision failed for branch '{}' in {}: {e} — \
                 trying gh pr view fallback",
                worker.branch,
                task_repo_dir.display()
            ));
            let pr_num = pr;
            let repo_dir_for_gh = task_repo_dir.to_path_buf();
            let gh_repo = config.repo.clone();
            let fallback_ref = tokio::task::spawn_blocking(move || {
                query_pr_head_ref(pr_num, &repo_dir_for_gh, Some(&gh_repo))
            })
            .await
            .ok()
            .flatten();
            if let Some(ref head_ref) = fallback_ref {
                if head_ref != worker.branch {
                    log(&format!(
                        "PR #{pr} head ref from GitHub: '{head_ref}' (worker branch: '{}')",
                        worker.branch
                    ));
                    match wt_mgr
                        .fetch_and_provision(task_repo_dir, &branch, &wt_path, head_ref)
                        .await
                    {
                        Ok(_) => true,
                        Err(e2) => {
                            log(&format!(
                                "reviewer worktree provision failed with fallback ref too: {e2}"
                            ));
                            false
                        }
                    }
                } else {
                    false
                }
            } else {
                log("gh pr view fallback returned no head ref");
                false
            }
        }
    };
    if !provision_ok {
        let task_id = worker.task_id;
        let strikes = provision_tracker.record_strike(task_id, pr);
        log(&format!(
            "reviewer provision strike {strikes}/{MAX_REVIEWER_PROVISION_STRIKES} \
             for task #{task_id} PR #{pr}"
        ));
        if strikes >= MAX_REVIEWER_PROVISION_STRIKES {
            log(&format!(
                "REVIEWER PROVISION EXHAUSTED: parking task #{task_id} after \
                 {strikes} consecutive provision failures for PR #{pr}"
            ));
        }
        name_pool.release(&reviewer_name);
        return Ok(());
    } else {
        provision_tracker.clear(worker.task_id, pr);
    }
    log(&format!(
        "reviewer worktree provisioned at {}",
        wt_path.display()
    ));

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

    // Journal: phase=reviewing, role=reviewer (pid filled after spawn)
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
        pid: None,
        pr: Some(pr),
        rework_count: 0,
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
        worker_agent: worker.agent_name.to_string(),
        reviewer_name: reviewer_name.clone(),
    };

    match reviewer::spawn_reviewer(
        &config.model,
        &config.effort,
        &session_id,
        &wt_path,
        config.agent_bin.as_deref(),
        config.bare_agent,
        vec![("QUORUM_REPO".into(), config.repo.clone())],
    )
    .await
    {
        Ok(mut proc) => {
            let prompt = reviewer::build_review_prompt(&spec);
            let turn1 = agent::user_turn(&prompt);
            if let Err(e) = proc.feed_turn(&turn1).await {
                log(&format!("reviewer feed_turn failed: {e}"));
                proc.kill_and_reap().await;
                name_pool.release(&reviewer_name);
                wt_mgr.remove(task_repo_dir, &wt_path).await.ok();
                wt_mgr.delete_branch(task_repo_dir, &branch).await;
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

            // M7: persist PID immediately so crash recovery can clean up
            let spawn_pid = proc.pid();
            {
                let p = config.db_path.clone();
                let pid_entry = JournalEntry {
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
                    pid: spawn_pid,
                    pr: Some(pr),
                    rework_count: 0,
                };
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut conn = quorum_core::db::open(&p)?;
                    journal::upsert(&mut conn, &pid_entry)
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                .ok();
            }

            fire_event(
                &config.db_path,
                &reviewer_name,
                worker.task_id,
                &Event::ReviewerAttached {
                    agent: reviewer_name.clone(),
                },
            )
            .await;

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
                live_stats: LiveStats::new(),
            });
        }
        Err(e) => {
            log(&format!("reviewer spawn failed: {e}"));
            name_pool.release(&reviewer_name);
            wt_mgr.remove(task_repo_dir, &wt_path).await.ok();
            wt_mgr.delete_branch(task_repo_dir, &branch).await;
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
#[allow(clippy::too_many_arguments)]
async fn spawn_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    workers: &mut Vec<SlotState>,
    pending_reviews: &[PendingReview],
    poison_tracker: &mut PoisonTracker,
    lifetime_roster: &mut LifetimeRoster,
) -> Result<bool> {
    let db_path = config.db_path.clone();
    let p = db_path.clone();

    let mut in_flight: Vec<i64> = workers.iter().map(|w| w.task_id).collect();
    in_flight.extend(pending_reviews.iter().map(|p| p.task_id));
    let poisoned: Vec<i64> = poison_tracker
        .strikes
        .iter()
        .filter(|(_, &s)| s >= MAX_POISON_STRIKES)
        .map(|(&id, _)| id)
        .collect();

    let ready_task = tokio::task::spawn_blocking(move || -> Result<Option<tasks::Task>> {
        let conn = quorum_core::db::open(&p)?;
        let open = tasks::list(&conn, Some("open"), None, None)?;
        let found = open.into_iter().find(|t| {
            if !t.ready || in_flight.contains(&t.id) || poisoned.contains(&t.id) {
                return false;
            }
            if t.review_only {
                return false;
            }
            true
        });
        Ok(found)
    })
    .await
    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))??;

    let task = match ready_task {
        Some(t) => t,
        None => return Ok(false),
    };

    let acquire_result = name_pool.acquire();
    if acquire_result.is_generated() && name_pool.has_file() {
        log(&format!(
            "names pool exhausted, generated fallback name: {}",
            acquire_result.name()
        ));
    }
    let agent_name = acquire_result.into_name();
    // #181: register in the lifetime roster BEFORE any DB work so we know this
    // name belongs to us for the rest of the daemon's life.
    lifetime_roster.register(&agent_name);

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

    let worker_repo_dir = &config.repo_dir;

    let session_id = uuid::Uuid::new_v4().to_string();
    let branch = format!("daemon/{}-t{}", agent_name.to_lowercase(), task.id);
    let wt_path = config
        .worktree_base
        .join(format!("{}-t{}", agent_name, task.id));

    match wt_mgr
        .provision(
            worker_repo_dir,
            &branch,
            &wt_path,
            &format!("origin/{}", config.base_branch),
        )
        .await
    {
        Ok(_) => {
            log(&format!("worktree provisioned at {}", wt_path.display()));
        }
        Err(e) => {
            log(&format!("worktree provision failed: {e}"));
            let strikes = poison_tracker.record_strike(task.id);
            if strikes >= MAX_POISON_STRIKES {
                poison_task(&db_path, &agent_name, task.id, strikes).await;
            } else {
                release_task(&db_path, &agent_name, task.id).await;
            }
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

    // Journal: phase=working (pid filled after spawn)
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
        pid: None,
        pr: None,
        rework_count: 0,
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
        resume: false,
        allowed_tools: agent::ALLOWED_TOOLS.to_string(),
        env_vars: vec![("QUORUM_REPO".into(), config.repo.clone())],
    };
    match AgentProc::spawn(&spec, config.agent_bin.as_deref()) {
        Ok(mut proc) => {
            let body = task.body.as_deref().unwrap_or(&task.title);
            let turn1 = reviewer::build_worker_turn(
                &agent_name,
                task.id,
                &task.title,
                body,
                config.limits.max_task_cost_usd,
            );
            if let Err(e) = proc.feed_turn(&turn1).await {
                log(&format!("feed_turn failed: {e}"));
                proc.kill_and_reap().await;
                let strikes = poison_tracker.record_strike(task.id);
                if strikes >= MAX_POISON_STRIKES {
                    poison_task(&db_path, &agent_name, task.id, strikes).await;
                } else {
                    release_task(&db_path, &agent_name, task.id).await;
                }
                name_pool.release(&agent_name);
                wt_mgr.remove(worker_repo_dir, &wt_path).await.ok();
                wt_mgr.delete_branch(worker_repo_dir, &branch).await;
                return Ok(false);
            }

            // M7: persist PID immediately so crash recovery can clean up
            let spawn_pid = proc.pid();
            {
                let p = db_path.clone();
                let pid_entry = JournalEntry {
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
                    pid: spawn_pid,
                    pr: None,
                    rework_count: 0,
                };
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut conn = quorum_core::db::open(&p)?;
                    journal::upsert(&mut conn, &pid_entry)
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                .ok();
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
                live_stats: LiveStats::new(),
            });
        }
        Err(e) => {
            log(&format!("agent spawn failed: {e}"));
            let strikes = poison_tracker.record_strike(task.id);
            if strikes >= MAX_POISON_STRIKES {
                poison_task(&db_path, &agent_name, task.id, strikes).await;
            } else {
                release_task(&db_path, &agent_name, task.id).await;
            }
            name_pool.release(&agent_name);
            wt_mgr.remove(worker_repo_dir, &wt_path).await.ok();
            wt_mgr.delete_branch(worker_repo_dir, &branch).await;
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

async fn poison_task(db_path: &std::path::Path, agent: &str, task_id: i64, strikes: u32) {
    log(&format!(
        "POISON: task #{task_id} cancelled after {strikes} consecutive instant-death failures"
    ));
    let p = db_path.to_path_buf();
    let a = agent.to_string();
    let body = format!("daemon: poisoned — worker died {strikes} time(s) without producing output");
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        let fields = tasks::TaskUpdate {
            status: Some("cancelled"),
            body: Some(&body),
            refs: None,
            verdict: None,
        };
        tasks::update(&mut conn, &a, task_id, &fields, now)?;
        Ok(())
    })
    .await
    .ok();
}

/// Fire a lifecycle event through `tasks::apply_event`.
/// Returns the transition result (updated task + process-side effects), or None
/// if the transition was invalid (logs the error).
async fn fire_event(
    db_path: &std::path::Path,
    agent: &str,
    task_id: i64,
    event: &Event,
) -> Option<tasks::TransitionResult> {
    let p = db_path.to_path_buf();
    let a = agent.to_string();
    let ev = event.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<tasks::TransitionResult> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        tasks::apply_event(&mut conn, &a, task_id, &ev, now)
    })
    .await;

    match result {
        Ok(Ok(tr)) => {
            let names: Vec<String> = tr.effects.iter().map(tasks::effect_name).collect();
            log(&format!(
                "lifecycle: task #{task_id} -> {} (effects: [{}])",
                tr.task.status,
                names.join(", ")
            ));
            Some(tr)
        }
        Ok(Err(e)) => {
            log(&format!(
                "lifecycle: fire_event failed for task #{task_id}: {e}"
            ));
            None
        }
        Err(e) => {
            log(&format!(
                "lifecycle: fire_event join error for task #{task_id}: {e}"
            ));
            None
        }
    }
}

/// Clean up a worker slot's resources without updating task status.
/// Used when `apply_event` has already transitioned the task state.
async fn cleanup_slot(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    mut state: SlotState,
    finalize_verdict: Option<&str>,
) {
    log(&format!(
        "tearing down worker {} (task #{})",
        state.agent_name, state.task_id
    ));

    if let Some(ref mut sl) = state.session_log {
        sl.finalize(finalize_verdict);
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

    let repo_dir = &config.repo_dir;
    wt_mgr.remove(repo_dir, &state.worktree_path).await.ok();
    wt_mgr.delete_branch(repo_dir, &state.branch).await;

    name_pool.release(&state.agent_name);
}

/// Clean up a pending review's resources without updating task status.
async fn cleanup_pending(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    state: PendingReview,
) {
    log(&format!(
        "cleanup_pending {} (task #{})",
        state.agent_name, state.task_id
    ));

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
    wt_mgr.delete_branch(&config.repo_dir, &state.branch).await;

    name_pool.release(&state.agent_name);
}

/// Tear down a worker agent: kill process, update task, clean up journal/worktree/name.
async fn teardown_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    state: SlotState,
    task_status: &str,
) {
    teardown_worker_with_body(config, wt_mgr, name_pool, state, task_status, None).await;
}

async fn teardown_worker_with_body(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    mut state: SlotState,
    task_status: &str,
    body: Option<&str>,
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

    if task_status == "open" {
        fire_event(
            &config.db_path,
            &state.agent_name,
            state.task_id,
            &Event::AgentFailed {
                reason: "worker teardown (shutdown/cleanup)".into(),
            },
        )
        .await;
        let p = config.db_path.clone();
        let agent = state.agent_name.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&p)?;
            journal::delete(&mut conn, &agent)?;
            Ok(())
        })
        .await
        .ok();
    } else {
        let p = config.db_path.clone();
        let agent = state.agent_name.clone();
        let task_id = state.task_id;
        let status = task_status.to_string();
        let body_owned = body.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&p)?;
            let now = now_unix();
            let fields = tasks::TaskUpdate {
                status: Some(&status),
                body: body_owned.as_deref(),
                refs: None,
                verdict: None,
            };
            tasks::update(&mut conn, &agent, task_id, &fields, now)?;
            journal::delete(&mut conn, &agent)?;
            Ok(())
        })
        .await
        .ok();
    }

    let repo_dir = &config.repo_dir;
    wt_mgr.remove(repo_dir, &state.worktree_path).await.ok();
    wt_mgr.delete_branch(repo_dir, &state.branch).await;

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

    let repo_dir = &config.repo_dir;
    wt_mgr.remove(repo_dir, &state.worktree_path).await.ok();
    wt_mgr.delete_branch(repo_dir, &state.branch).await;

    name_pool.release(&state.agent_name);
    log(&format!("reviewer {} torn down", state.agent_name));
}

/// Tear down a pending review (no live process): update task status, delete
/// journal, remove worktree/branch, release the name. Mirrors
/// `teardown_worker` but skips the process kill (there's no proc).
async fn teardown_pending_review(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    state: PendingReview,
    task_status: &str,
    body: Option<&str>,
) {
    log(&format!(
        "tearing down pending review for {} (task #{} -> {task_status})",
        state.agent_name, state.task_id
    ));

    if task_status == "open" {
        fire_event(
            &config.db_path,
            &state.agent_name,
            state.task_id,
            &Event::AgentFailed {
                reason: "pending review teardown (shutdown/cleanup)".into(),
            },
        )
        .await;
        let p = config.db_path.clone();
        let agent = state.agent_name.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&p)?;
            journal::delete(&mut conn, &agent)?;
            Ok(())
        })
        .await
        .ok();
    } else {
        let p = config.db_path.clone();
        let agent = state.agent_name.clone();
        let task_id = state.task_id;
        let status = task_status.to_string();
        let body_owned = body.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&p)?;
            let now = now_unix();
            let fields = tasks::TaskUpdate {
                status: Some(&status),
                body: body_owned.as_deref(),
                refs: None,
                verdict: None,
            };
            tasks::update(&mut conn, &agent, task_id, &fields, now)?;
            journal::delete(&mut conn, &agent)?;
            Ok(())
        })
        .await
        .ok();
    }

    wt_mgr
        .remove(&config.repo_dir, &state.worktree_path)
        .await
        .ok();
    wt_mgr.delete_branch(&config.repo_dir, &state.branch).await;

    name_pool.release(&state.agent_name);
    log(&format!(
        "pending review for {} torn down",
        state.agent_name
    ));
}

/// Spawn a `--resume` worker from a `PendingReview` and feed it a rework
/// turn, promoting the pending review into a live `SlotState` in `workers`.
///
/// #178: used when a reviewer verdict requires the worker to do more work
/// (changes / conflict rework / checks failure / retryable merge failure)
/// but the daemon restarted and no worker process is currently alive for
/// this task. The session_id from the pending review is what `--resume`
/// picks up, preserving the worker's original context.
///
/// Returns `Ok(true)` on success (pending removed, worker pushed). Returns
/// `Ok(false)` if spawn or feed_turn failed — the pending review is torn
/// down with the task released back to `open` so it can be retried.
#[allow(clippy::too_many_arguments)]
async fn spawn_resume_worker_for_pending(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    workers: &mut Vec<SlotState>,
    pending: PendingReview,
    rework_turn: &str,
    new_rework_count: u32,
) -> Result<bool> {
    log(&format!(
        "spawning --resume worker for pending review {} (task #{}, session {})",
        pending.agent_name, pending.task_id, pending.session_id
    ));

    let spec = AgentSpec {
        model: config.model.clone(),
        effort: config.effort.clone(),
        session_id: pending.session_id.clone(),
        worktree: pending.worktree_path.clone(),
        bare: config.bare_agent,
        resume: true,
        allowed_tools: agent::ALLOWED_TOOLS.to_string(),
        env_vars: vec![("QUORUM_REPO".into(), config.repo.clone())],
    };

    let mut proc = match AgentProc::spawn(&spec, config.agent_bin.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            log(&format!(
                "resume-worker spawn failed for pending {} (task #{}): {e} — \
                 releasing task",
                pending.agent_name, pending.task_id
            ));
            teardown_pending_review(config, wt_mgr, name_pool, pending, "open", None).await;
            return Ok(false);
        }
    };

    if let Err(e) = proc.feed_turn(rework_turn).await {
        log(&format!(
            "resume-worker feed_turn failed for pending {} (task #{}): {e} — \
             releasing task",
            pending.agent_name, pending.task_id
        ));
        proc.kill_and_reap().await;
        teardown_pending_review(config, wt_mgr, name_pool, pending, "open", None).await;
        return Ok(false);
    }

    // Reopen the session log if we have one
    let session_log = pending
        .log_dir
        .as_ref()
        .and_then(|ld| session_log::SessionLog::reopen(ld).ok());

    let now_instant = std::time::Instant::now();
    let mut slot = SlotState {
        agent_name: pending.agent_name.clone(),
        proc,
        task_id: pending.task_id,
        session_id: pending.session_id.clone(),
        worktree_path: pending.worktree_path.clone(),
        branch: pending.branch.clone(),
        draining: true,
        pr: None,
        rework_count: new_rework_count,
        cost_tokens: pending.cost_tokens,
        cost_usd: pending.cost_usd,
        task_started_at: pending.task_started_at,
        turn_started_at: now_instant,
        agent_state: pending.agent_state.clone(),
        session_log,
        live_stats: LiveStats::new(),
    };

    if let Some(ref mut sl) = slot.session_log {
        sl.log_rework(slot.rework_count);
    }

    // Persist journal update to reflect worker is back at working phase.
    let p = config.db_path.clone();
    let pid = slot.proc.pid();
    let entry = JournalEntry {
        agent: slot.agent_name.clone(),
        role: "worker".into(),
        task_id: Some(slot.task_id),
        session_id: slot.session_id.clone(),
        worktree: Some(slot.worktree_path.to_string_lossy().into()),
        branch: Some(slot.branch.clone()),
        phase: "working".into(),
        cost_tokens: slot.cost_tokens,
        agent_state: slot.agent_state.clone(),
        cost_usd: slot.cost_usd,
        log_dir: slot
            .session_log
            .as_ref()
            .map(|l| l.dir().to_string_lossy().into()),
        pid,
        pr: None,
        rework_count: slot.rework_count as i32,
    };
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        journal::upsert(&mut conn, &entry)
    })
    .await
    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
    .ok();

    workers.push(slot);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_to_model_id_opus_46() {
        assert_eq!(
            tier_to_model_id("opus-46").as_deref(),
            Some("claude-opus-4-6")
        );
    }

    #[test]
    fn tier_to_model_id_opus_47() {
        assert_eq!(
            tier_to_model_id("opus-47").as_deref(),
            Some("claude-opus-4-7")
        );
    }

    #[test]
    fn tier_to_model_id_opus_48() {
        assert_eq!(
            tier_to_model_id("opus-48").as_deref(),
            Some("claude-opus-4-8")
        );
    }

    #[test]
    fn tier_to_model_id_sonnet_5() {
        assert_eq!(
            tier_to_model_id("sonnet-5").as_deref(),
            Some("claude-sonnet-5")
        );
    }

    #[test]
    fn tier_to_model_id_unknown_returns_none() {
        assert_eq!(tier_to_model_id("gpt-5"), None);
    }

    #[test]
    fn labels_to_model_effort_tier_maps_to_full_id() {
        let labels = r#"["kind:fix","tier:opus-46"]"#;
        let (model, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(model.as_deref(), Some("claude-opus-4-6"));
        assert_eq!(effort, None);
    }

    #[test]
    fn labels_to_model_effort_effort_override() {
        let labels = r#"["effort:high","tier:sonnet-5"]"#;
        let (model, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(model.as_deref(), Some("claude-sonnet-5"));
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
        assert_eq!(model.as_deref(), Some("claude-opus-4-6"));
    }

    #[test]
    fn labels_to_model_effort_unknown_tier_falls_back() {
        let labels = r#"["tier:unknown-model"]"#;
        let (model, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(model, None);
        assert_eq!(effort, None);
    }

    #[test]
    fn labels_to_model_effort_rejects_invalid_effort() {
        let labels = r#"["effort:low"]"#;
        let (_, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(effort, None, "effort:low must be rejected");

        let labels = r#"["effort:max"]"#;
        let (_, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(effort, None, "effort:max must be rejected");

        let labels = r#"["effort:medium"]"#;
        let (_, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(effort.as_deref(), Some("medium"));

        let labels = r#"["effort:high"]"#;
        let (_, effort) = labels_to_model_effort(Some(labels));
        assert_eq!(effort.as_deref(), Some("high"));
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
            live_stats: LiveStats::new(),
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
    fn check_limits_turn_cost_usd_fail_closed_when_none() {
        let limits = CostLimits {
            max_turn_cost_usd: Some(0.01),
            ..Default::default()
        };
        let slot = make_dummy_slot();
        let result = check_post_result_limits(&limits, 100, 500, None, 0.0, &slot);
        assert!(result.is_some(), "must fail-closed when cost is None");
        let msg = result.unwrap().to_string();
        assert!(
            msg.contains("unavailable") && msg.contains("fail-closed"),
            "expected fail-closed message, got: {msg}"
        );
    }

    #[test]
    fn check_limits_turn_cost_usd_passes_when_no_limit_set() {
        let limits = CostLimits::default();
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
        ];
        for c in cases {
            let s = c.to_string();
            assert!(s.contains("exceeded limit"), "bad display: {s}");
        }
        let missing = LimitBreached::TurnCostUsdMissing { max: 0.01 };
        let s = missing.to_string();
        assert!(
            s.contains("fail-closed"),
            "TurnCostUsdMissing display must say fail-closed: {s}"
        );
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
    }

    #[test]
    fn poison_tracker_new_is_clean() {
        let tracker = PoisonTracker::new();
        assert_eq!(tracker.strikes(42), 0);
        assert!(!tracker.is_poisoned(42));
    }

    #[test]
    fn poison_tracker_records_strikes() {
        let mut tracker = PoisonTracker::new();
        assert_eq!(tracker.record_strike(1), 1);
        assert_eq!(tracker.record_strike(1), 2);
        assert_eq!(tracker.record_strike(1), 3);
        assert_eq!(tracker.strikes(1), 3);
    }

    #[test]
    fn poison_tracker_poisoned_at_threshold() {
        let mut tracker = PoisonTracker::new();
        for _ in 0..MAX_POISON_STRIKES - 1 {
            tracker.record_strike(1);
        }
        assert!(!tracker.is_poisoned(1));
        tracker.record_strike(1);
        assert!(tracker.is_poisoned(1));
    }

    #[test]
    fn poison_tracker_clear_resets() {
        let mut tracker = PoisonTracker::new();
        tracker.record_strike(1);
        tracker.record_strike(1);
        tracker.clear(1);
        assert_eq!(tracker.strikes(1), 0);
        assert!(!tracker.is_poisoned(1));
    }

    #[test]
    fn poison_tracker_independent_tasks() {
        let mut tracker = PoisonTracker::new();
        tracker.record_strike(1);
        tracker.record_strike(1);
        tracker.record_strike(2);
        assert_eq!(tracker.strikes(1), 2);
        assert_eq!(tracker.strikes(2), 1);
        assert!(!tracker.is_poisoned(1));
        assert!(!tracker.is_poisoned(2));
    }

    #[test]
    fn poison_tracker_clear_does_not_affect_other_tasks() {
        let mut tracker = PoisonTracker::new();
        tracker.record_strike(1);
        tracker.record_strike(2);
        tracker.clear(1);
        assert_eq!(tracker.strikes(1), 0);
        assert_eq!(tracker.strikes(2), 1);
    }

    // ── Drain state unit tests ────────────────────────────────────────

    #[test]
    fn drain_state_new_is_not_draining() {
        let ds = DrainState::new();
        assert!(!ds.draining);
        assert!(ds.drain_started_at.is_none());
        assert!(ds.drain_sha.is_none());
    }

    #[test]
    fn drain_state_start_drain_sets_fields() {
        let mut ds = DrainState::new();
        ds.start_drain("abc123");
        assert!(ds.draining);
        assert!(ds.drain_started_at.is_some());
        assert_eq!(ds.drain_sha.as_deref(), Some("abc123"));
    }

    #[test]
    fn drain_state_debounce_second_start_is_noop() {
        let mut ds = DrainState::new();
        ds.start_drain("sha1");
        let first_started = ds.drain_started_at.unwrap();
        let first_sha = ds.drain_sha.clone();
        ds.start_drain("sha2");
        assert_eq!(ds.drain_started_at.unwrap(), first_started);
        assert_eq!(ds.drain_sha, first_sha);
    }

    #[test]
    fn drain_state_should_poll_sha_initially() {
        let ds = DrainState::new();
        assert!(ds.should_poll_sha(60));
    }

    #[test]
    fn drain_state_should_poll_sha_throttled() {
        let mut ds = DrainState::new();
        ds.last_sha_poll = Some(std::time::Instant::now());
        assert!(!ds.should_poll_sha(60));
    }

    #[test]
    fn drain_state_timed_out_false_when_not_draining() {
        let ds = DrainState::new();
        assert!(!ds.timed_out(900));
    }

    #[test]
    fn drain_state_timed_out_false_when_within_timeout() {
        let mut ds = DrainState::new();
        ds.start_drain("sha");
        assert!(!ds.timed_out(900));
    }

    #[test]
    fn drain_state_timed_out_true_when_expired() {
        let mut ds = DrainState::new();
        ds.drain_started_at =
            Some(std::time::Instant::now() - std::time::Duration::from_secs(1000));
        assert!(ds.timed_out(900));
    }

    #[test]
    fn exit_self_update_is_75() {
        assert_eq!(EXIT_SELF_UPDATE, 75);
    }

    // ── Tick error classification (schema-too-new self-update) ───────────

    #[test]
    fn schema_too_new_tick_error_exits_for_self_update() {
        // A newer binary migrated the DB past what this binary understands. Retrying can
        // never succeed — the loop must exit 75 so the supervisor rebuilds/relaunches.
        let e = QuorumError::SchemaTooNew { db: 17, bin: 16 };
        assert_eq!(classify_tick_error(&e), TickErrorAction::ExitSelfUpdate);
    }

    #[test]
    fn transient_tick_errors_continue() {
        // Everything that can plausibly succeed on a later tick must NOT exit the loop.
        assert_eq!(
            classify_tick_error(&QuorumError::Busy),
            TickErrorAction::Continue
        );
        assert_eq!(
            classify_tick_error(&QuorumError::NotHolder),
            TickErrorAction::Continue
        );
        assert_eq!(
            classify_tick_error(&QuorumError::Io("transient".into())),
            TickErrorAction::Continue
        );
    }

    // ── ReviewerProvisionTracker tests (#162) ────────────────────────────

    #[test]
    fn reviewer_provision_tracker_new_is_clean() {
        let tracker = ReviewerProvisionTracker::new();
        assert_eq!(tracker.strikes(1, 42), 0);
        assert!(!tracker.is_exhausted(1, 42));
    }

    #[test]
    fn reviewer_provision_tracker_records_strikes() {
        let mut tracker = ReviewerProvisionTracker::new();
        assert_eq!(tracker.record_strike(1, 42), 1);
        assert_eq!(tracker.record_strike(1, 42), 2);
        assert_eq!(tracker.record_strike(1, 42), 3);
        assert_eq!(tracker.strikes(1, 42), 3);
    }

    #[test]
    fn reviewer_provision_tracker_exhausted_at_threshold() {
        let mut tracker = ReviewerProvisionTracker::new();
        for _ in 0..MAX_REVIEWER_PROVISION_STRIKES - 1 {
            tracker.record_strike(1, 42);
        }
        assert!(!tracker.is_exhausted(1, 42));
        tracker.record_strike(1, 42);
        assert!(tracker.is_exhausted(1, 42));
    }

    #[test]
    fn reviewer_provision_tracker_clear_resets() {
        let mut tracker = ReviewerProvisionTracker::new();
        tracker.record_strike(1, 42);
        tracker.record_strike(1, 42);
        tracker.clear(1, 42);
        assert_eq!(tracker.strikes(1, 42), 0);
        assert!(!tracker.is_exhausted(1, 42));
    }

    #[test]
    fn reviewer_provision_tracker_independent_keys() {
        let mut tracker = ReviewerProvisionTracker::new();
        tracker.record_strike(1, 42);
        tracker.record_strike(1, 42);
        tracker.record_strike(2, 43);
        assert_eq!(tracker.strikes(1, 42), 2);
        assert_eq!(tracker.strikes(2, 43), 1);
        assert!(!tracker.is_exhausted(1, 42));
        assert!(!tracker.is_exhausted(2, 43));
    }

    #[test]
    fn reviewer_provision_tracker_same_task_different_pr() {
        let mut tracker = ReviewerProvisionTracker::new();
        for _ in 0..MAX_REVIEWER_PROVISION_STRIKES {
            tracker.record_strike(1, 42);
        }
        assert!(tracker.is_exhausted(1, 42));
        assert!(
            !tracker.is_exhausted(1, 43),
            "different PR on same task must not be exhausted"
        );
    }

    #[test]
    fn now_label_bash_command() {
        let input = serde_json::json!({"command": "cargo test"});
        assert_eq!(now_label("Bash", &input), "Bash: cargo test");
    }

    #[test]
    fn now_label_read_file() {
        let input = serde_json::json!({"file_path": "/foo/bar/stats.rs"});
        assert_eq!(now_label("Read", &input), "Read: stats.rs");
    }

    #[test]
    fn now_label_truncation() {
        let input = serde_json::json!({"command": "cargo build --all-targets --features bundled"});
        let label = now_label("Bash", &input);
        assert!(label.chars().count() <= 24, "label too long: {label}");
        assert!(label.ends_with('…'));
    }

    #[test]
    fn now_label_unknown_tool() {
        let input = serde_json::json!({});
        assert_eq!(now_label("UnknownTool", &input), "UnknownTool");
    }

    #[test]
    fn live_stats_event_ring_buffer() {
        let mut ls = LiveStats::new();
        for _ in 0..5 {
            ls.record_event();
        }
        assert_eq!(ls.events_per_min(), 5.0);
    }

    #[test]
    fn live_stats_uptime_is_nonnegative() {
        let ls = LiveStats::new();
        assert!(ls.uptime_secs() < 2);
    }
}
