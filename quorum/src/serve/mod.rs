//! `quorum serve` — the agent-manager daemon.
//!
//! Builds a tokio runtime and runs an async tick loop that polls the mailbox,
//! spawns/drives agents, and shuts down cleanly on Ctrl-C. See spec §3.

pub mod agent;
pub mod approvals;
pub mod classifier;
pub mod doctor;
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
use quorum_core::lifecycle::{self, Effect, Event};
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
const MAX_ERROR_RETRIES: u32 = 3;

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

/// Build the daemon-authored branch name for an orphan in-review task.
/// Returns `None` for review-only tasks or tasks with no author — those
/// have externally-authored branches that must be resolved from GitHub.
fn orphan_worker_branch(author: &str, task_id: i64, review_only: bool) -> Option<String> {
    if review_only || author.is_empty() {
        None
    } else {
        Some(format!("daemon/{}-t{}", author.to_lowercase(), task_id))
    }
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

async fn close_agent_run(db_path: &std::path::Path, run_id: Option<i64>, end_reason: &str) {
    if let Some(rid) = run_id {
        let p = db_path.to_path_buf();
        let reason = end_reason.to_string();
        tokio::task::spawn_blocking(move || {
            if let Ok(conn) = quorum_core::db::open(&p) {
                let _ = quorum_core::agent_runs::close(&conn, rid, now_unix(), &reason);
            }
        })
        .await
        .ok();
    }
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
    /// Max seconds a worker/reviewer may sit idle between turns (draining=false)
    /// before the watchdog kills it. Catches zombies that asked a question no one
    /// can answer (e.g. permission denied in dontAsk mode). Default: 300.
    pub idle_timeout_secs: Option<u64>,
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
    /// Per-repo list of CI job names that must have conclusion SUCCESS (not
    /// SKIPPED or absent) on the PR head before merging. Empty = gate off.
    pub required_jobs: Vec<String>,
    /// When true, check the default branch's latest CI run before merging;
    /// hold if red/pending, proceed with a warning after `master_ci_timeout_secs`.
    pub master_ci_gate: bool,
    /// Seconds to wait for the default branch CI to turn green before
    /// proceeding anyway. Default: 300.
    pub master_ci_timeout_secs: u64,
    /// Override the default tool allowlist for spawned agents. When None,
    /// uses `agent::ALLOWED_TOOLS`.
    pub allowed_tools: Option<String>,
    /// When true, the daemon spawns a one-shot doctor agent for tasks stalled
    /// with no active worker/reviewer. Default: false.
    pub doctor_enabled: bool,
    /// R2 review-audit: spawn a second reviewer to adversarially audit R1.
    pub r2_enabled: bool,
    /// R2: minimum samples per (model, effort, cx_bucket) stratum before
    /// switching to steady-state probability. Default: 5.
    pub r2_target_per_stratum: i64,
    /// R2: probability of spawning R2 once stratum target is met. Default: 0.10.
    pub r2_steady_state_p: f64,
    /// R2: when true, R2 misses with confidence>=70 block merge. Default: false.
    #[allow(dead_code)]
    pub r2_blocking: bool,
}

pub const EXIT_SELF_UPDATE: i32 = 75;

const DAEMON_LOCK_STALE_SECS: i64 = 30;
const DRIFT_CHECK_INTERVAL_SECS: u64 = 15 * 60;

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
    /// Set when `draining` flips to false (turn completed). Used by the idle
    /// watchdog to detect zombies sitting between turns indefinitely.
    turn_ended_at: Option<std::time::Instant>,
    agent_state: Option<String>,
    session_log: Option<session_log::SessionLog>,
    live_stats: LiveStats,
    error_turn_count: u32,
    agent_run_id: Option<i64>,
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

/// Run unbacked/twin PR drift detection. Shells out to `gh pr list`,
/// compares against task refs.pr, and emits one-time events. Fail-open on
/// any error (API unavailable, parse failure, etc.).
fn run_drift_check(db_path: &std::path::Path, repo: &str) -> Result<()> {
    let output = std::process::Command::new("gh")
        .args([
            "pr",
            "list",
            "--repo",
            repo,
            "--json",
            "number,title,headRefName",
            "--state",
            "open",
            "--limit",
            "100",
        ])
        .output()
        .map_err(|e| QuorumError::Io(format!("gh pr list: {e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(QuorumError::Io(format!("gh pr list failed: {stderr}")));
    }
    let open_prs: Vec<quorum_core::drift::GhPr> = serde_json::from_slice(&output.stdout)
        .map_err(|e| QuorumError::Io(format!("gh pr list parse: {e}")))?;

    let mut conn = quorum_core::db::open(db_path)?;
    let task_prs = quorum_core::drift::task_pr_refs(&conn)?;
    let all_backed_prs = quorum_core::drift::all_task_pr_refs(&conn)?;
    let task_branches = quorum_core::drift::task_branch_allocations(&conn)?;
    let active_tasks = quorum_core::drift::active_task_ids(&conn)?;
    let drift = quorum_core::drift::detect(
        &open_prs,
        &task_prs,
        &all_backed_prs,
        &task_branches,
        &active_tasks,
    );

    let now = now_unix();

    for u in &drift.unbacked {
        log(&format!(
            "DRIFT: unbacked PR #{} \"{}\" (branch: {})",
            u.number, u.title, u.branch
        ));
    }
    for t in &drift.twins {
        let prs: Vec<String> = t.pr_numbers.iter().map(|n| format!("#{n}")).collect();
        log(&format!(
            "DRIFT: twin PRs for task #{}: {}",
            t.task_id,
            prs.join(", ")
        ));
    }

    quorum_core::drift::emit_drift_events(&mut conn, &drift, now)?;
    Ok(())
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
    let mut poison_tracker = PoisonTracker::new();
    let mut reviewer_provision_tracker = ReviewerProvisionTracker::new();
    let mut drain_state = DrainState::new();
    let mut lifetime_roster = LifetimeRoster::new();
    let mut last_drift_check: Option<std::time::Instant> = None;
    let mut classifier_slot: Option<classifier::ClassifierSlot> = None;
    let mut doctor_slot: Option<doctor::DoctorSlot> = None;
    let mut doctored_tasks: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut auditors: Vec<SlotState> = Vec::new();

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
    // instance-independent state BEFORE stateless recovery, so a
    // self-update-drain restart merges the approved PR instead of re-working it.
    // Runs first so approved tasks are closed (and their journal rows dropped)
    // before recovery::recover resets them to open.
    if let Err(e) =
        approvals::recover(&config.db_path, &config.repo_dir, &config.merge_executor).await
    {
        log(&format!("approval recovery failed: {e} — continuing"));
    }

    // M7: stateless crash recovery — kill stale processes, wipe journal,
    // GC worktrees, and reset non-terminal tasks for the tick loop to handle.
    if let Err(e) = recovery::recover(config, &wt_mgr).await {
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
            if let Some(slot) = classifier_slot.take() {
                slot.proc.kill_and_reap().await;
            }
            for a in auditors.drain(..) {
                R2_META.lock().unwrap().remove(&a.agent_name);
                teardown_reviewer(config, &wt_mgr, &mut name_pool, a, "shutdown").await;
            }
            for r in reviewers.drain(..) {
                teardown_reviewer(config, &wt_mgr, &mut name_pool, r, "shutdown").await;
            }
            for w in workers.drain(..) {
                teardown_worker(config, &wt_mgr, &mut name_pool, w, "open").await;
            }
            return Ok(1);
        }
        let sig = signal_count.load(std::sync::atomic::Ordering::SeqCst);

        // Second signal (or first signal with no in-flight agents): immediate teardown.
        if sig >= 2
            || (sig >= 1 && workers.is_empty() && reviewers.is_empty() && auditors.is_empty())
        {
            if sig >= 2 {
                log("force shutdown (second signal)");
            } else {
                log("shutting down (signal, no in-flight agents)");
            }
            if let Some(slot) = classifier_slot.take() {
                slot.proc.kill_and_reap().await;
            }
            for a in auditors.drain(..) {
                R2_META.lock().unwrap().remove(&a.agent_name);
                teardown_reviewer(config, &wt_mgr, &mut name_pool, a, "shutdown").await;
            }
            for r in reviewers.drain(..) {
                teardown_reviewer(config, &wt_mgr, &mut name_pool, r, "shutdown").await;
            }
            for w in workers.drain(..) {
                teardown_worker(config, &wt_mgr, &mut name_pool, w, "open").await;
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
                if let Some(slot) = classifier_slot.take() {
                    slot.proc.kill_and_reap().await;
                }
                for a in auditors.drain(..) {
                    R2_META.lock().unwrap().remove(&a.agent_name);
                    a.proc.kill_and_reap().await;
                    name_pool.release(&a.agent_name);
                }
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
            if workers.is_empty() && reviewers.is_empty() {
                let exit = drain_state.exit_code();
                let sha = drain_state.drain_sha.as_deref().unwrap_or("unknown");
                log(&format!(
                    "DRAIN: all agents finished (sha={sha}), exiting {exit}"
                ));
                if let Some(slot) = classifier_slot.take() {
                    slot.proc.kill_and_reap().await;
                }
                return Ok(exit);
            }

            if drain_state.timed_out(config.drain_timeout_secs) {
                let exit = drain_state.exit_code();
                log(&format!(
                    "DRAIN: timeout ({}s) — force-killing {} worker(s), {} reviewer(s), {} auditor(s)",
                    config.drain_timeout_secs,
                    workers.len(),
                    reviewers.len(),
                    auditors.len(),
                ));
                if let Some(slot) = classifier_slot.take() {
                    slot.proc.kill_and_reap().await;
                }
                for a in auditors.drain(..) {
                    R2_META.lock().unwrap().remove(&a.agent_name);
                    teardown_reviewer(config, &wt_mgr, &mut name_pool, a, "drain").await;
                }
                for r in reviewers.drain(..) {
                    teardown_reviewer(config, &wt_mgr, &mut name_pool, r, "drain").await;
                }
                for w in workers.drain(..) {
                    teardown_worker(config, &wt_mgr, &mut name_pool, w, "open").await;
                }
                let sha = drain_state.drain_sha.as_deref().unwrap_or("unknown");
                log(&format!("DRAIN: exiting {exit} (sha={sha})"));
                return Ok(exit);
            }
        }

        // ── Drift check: unbacked/twin PR detection (~15 min cadence) ──
        let should_drift_check = match last_drift_check {
            None => true,
            Some(t) => t.elapsed().as_secs() >= DRIFT_CHECK_INTERVAL_SECS,
        };
        if should_drift_check {
            last_drift_check = Some(std::time::Instant::now());
            let db = config.db_path.clone();
            let repo = config.repo.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = run_drift_check(&db, &repo) {
                    log(&format!("drift check failed (fail-open): {e}"));
                }
            })
            .await
            .ok();
        }

        if let Err(e) = tick(
            config,
            &wt_mgr,
            &mut name_pool,
            &mut workers,
            &mut reviewers,
            &mut auditors,
            &mut poison_tracker,
            &mut reviewer_provision_tracker,
            &mut drain_state,
            &mut lifetime_roster,
            &mut classifier_slot,
            &mut doctor_slot,
            &mut doctored_tasks,
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
                    if let Some(slot) = classifier_slot.take() {
                        slot.proc.kill_and_reap().await;
                    }
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
    auditors: &mut Vec<SlotState>,
    poison_tracker: &mut PoisonTracker,
    reviewer_provision_tracker: &mut ReviewerProvisionTracker,
    drain_state: &mut DrainState,
    lifetime_roster: &mut LifetimeRoster,
    classifier_slot: &mut Option<classifier::ClassifierSlot>,
    doctor_slot: &mut Option<doctor::DoctorSlot>,
    doctored_tasks: &mut std::collections::HashSet<i64>,
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

        // Kill rows — hard-terminate the targeted agent immediately.
        if row.kind == mailbox::MailboxKind::Kill {
            if let Some(target) = &row.to_agent {
                let reason = row.note.as_deref().unwrap_or("no reason given");
                let by = &row.agent;

                // Check workers first, then reviewers.
                if let Some(wi) = workers.iter().position(|w| w.agent_name == *target) {
                    log(&format!(
                        "kill: terminating worker {} (task #{}) by {by}: {reason}",
                        workers[wi].agent_name, workers[wi].task_id,
                    ));
                    let w = workers.remove(wi);
                    let task_id = w.task_id;
                    fire_event(
                        &db_path,
                        &w.agent_name,
                        task_id,
                        &Event::AgentFailed {
                            reason: format!("killed by {by}: {reason}"),
                        },
                    )
                    .await;
                    // Emit agent_killed event for the log.
                    emit_kill_event(&db_path, target, by, reason).await;
                    teardown_worker(config, wt_mgr, name_pool, w, "open").await;
                } else if let Some(ri) = reviewers.iter().position(|r| r.agent_name == *target) {
                    log(&format!(
                        "kill: terminating reviewer {} (task #{}) by {by}: {reason}",
                        reviewers[ri].agent_name, reviewers[ri].task_id,
                    ));
                    let r = reviewers.remove(ri);
                    let task_id = r.task_id;
                    fire_event(
                        &db_path,
                        &r.agent_name,
                        task_id,
                        &Event::AgentFailed {
                            reason: format!("killed by {by}: {reason}"),
                        },
                    )
                    .await;
                    emit_kill_event(&db_path, target, by, reason).await;
                    teardown_reviewer(config, wt_mgr, name_pool, r, "killed").await;
                } else if lifetime_roster.owns(target) {
                    log(&format!(
                        "kill: agent {target} not active (already dead/finished)"
                    ));
                } else {
                    log(&format!(
                        "kill: agent {target} not in this instance's roster"
                    ));
                    // Leave unconsumed for another daemon instance.
                    continue;
                }
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

        // R2 auditor done — record audit, tear down, consume. Never affects merge.
        let auditor_idx = auditors.iter().position(|a| a.agent_name == row.agent);
        if let Some(ai) = auditor_idx {
            log(&format!(
                "R2 auditor {} done (pr={:?}, verdict={:?})",
                auditors[ai].agent_name, row.pr, row.verdict
            ));
            handle_r2_done(config, wt_mgr, name_pool, &mut *auditors, ai, row).await;
            if !consume_mailbox_row(&db_path, *id).await {
                break;
            }
            continue;
        }

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
                        teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved").await;
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
                        teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved").await;
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

                    if mergeability == merge::MergeabilityState::AlreadyMerged {
                        log(&format!(
                            "PR #{pr_num} already merged — firing MergeSucceeded"
                        ));
                        fire_event(&db_path, "system", reviewer_task_id, &Event::MergeSucceeded)
                            .await;
                        if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id)
                        {
                            let w = workers.remove(wi);
                            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                        }
                        let r = reviewers.remove(ri);
                        teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved").await;
                        if !consume_mailbox_row(&db_path, *id).await {
                            break;
                        }
                        break;
                    }

                    if mergeability == merge::MergeabilityState::Conflicting {
                        let has_worker = workers.iter().any(|w| w.task_id == reviewer_task_id);

                        if !has_worker {
                            // Review-only: no worker to rebase. Fire MergeFailed
                            // (merging → in-review) and park as merge-blocked;
                            // orphan handler retries when PR becomes MERGEABLE.
                            log(&format!(
                                "PR #{pr_num} is CONFLICTING (review-only) — firing MergeFailed"
                            ));
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
                            log(&format!(
                                "review-only task #{reviewer_task_id}: \
                                 merge blocked (conflict) — parking, not failing"
                            ));
                            set_task_body(&db_path, reviewer_task_id, tasks::MERGE_BLOCKED_BODY)
                                .await;
                            let r = reviewers.remove(ri);
                            teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved")
                                .await;
                        } else {
                            // Non-review-only: fire MergeConflict (merging → rework
                            // directly, skipping the reviewer hop).
                            log(&format!(
                                "PR #{pr_num} is CONFLICTING — firing MergeConflict"
                            ));
                            let mc = fire_event(
                                &db_path,
                                "system",
                                reviewer_task_id,
                                &Event::MergeConflict,
                            )
                            .await;
                            match mc {
                                Some(ref tr) if tr.task.status == "rework" => {
                                    let rework_msg = format!(
                                        "PR #{pr_num} has conflicts with {} \
                                     (a sibling PR likely merged first).\n\n\
                                     Rebase on {}, resolve conflicts, \
                                     and push again.",
                                        config.base_branch, config.base_branch
                                    );
                                    let wi = workers
                                        .iter()
                                        .position(|w| w.task_id == reviewer_task_id)
                                        .unwrap();
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
                                }
                                Some(_) => {
                                    // Rework cap exceeded → failed. Clean up.
                                    let r = reviewers.remove(ri);
                                    teardown_reviewer(
                                        config,
                                        wt_mgr,
                                        name_pool,
                                        r,
                                        "verdict:approved",
                                    )
                                    .await;
                                    if let Some(wi) =
                                        workers.iter().position(|w| w.task_id == reviewer_task_id)
                                    {
                                        let w = workers.remove(wi);
                                        cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                                    }
                                }
                                None => {
                                    // MergeConflict event failed — clean up.
                                    let r = reviewers.remove(ri);
                                    teardown_reviewer(
                                        config,
                                        wt_mgr,
                                        name_pool,
                                        r,
                                        "verdict:approved",
                                    )
                                    .await;
                                }
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

                    const MAX_POLICY_RETRIES: u32 = 3;
                    let mut policy_retry = 0u32;
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

                    // Required jobs gate: if checks are Ready but configured
                    // required jobs are not SUCCESS (e.g. SKIPPED), override to
                    // Failed so the existing rework path handles it.
                    let checks_outcome = if matches!(checks_outcome, merge::ChecksOutcome::Ready)
                        && !config.required_jobs.is_empty()
                    {
                        let rj_outcome = {
                            let repo = config.repo_dir.clone();
                            let executor = Arc::clone(&config.merge_executor);
                            let jobs = config.required_jobs.clone();
                            tokio::task::spawn_blocking(move || {
                                executor.check_required_jobs(pr_num, &repo, &jobs)
                            })
                            .await
                            .map_err(|e| {
                                QuorumError::Io(format!("required_jobs spawn_blocking join: {e}"))
                            })?
                        };
                        match rj_outcome {
                            merge::RequiredJobsOutcome::AllSucceeded => {
                                log("required jobs gate: all required jobs succeeded");
                                checks_outcome
                            }
                            merge::RequiredJobsOutcome::NotReady { issues } => {
                                let failing: Vec<String> = issues
                                    .iter()
                                    .map(|(name, status)| format!("{name} ({status})"))
                                    .collect();
                                log(&format!(
                                    "REQUIRED JOBS GATE: PR #{pr_num} required jobs \
                                     not SUCCESS: {}",
                                    failing.join(", ")
                                ));
                                merge::ChecksOutcome::Failed {
                                    failing_checks: failing,
                                }
                            }
                            merge::RequiredJobsOutcome::Pending { pending_jobs } => {
                                log(&format!(
                                    "REQUIRED JOBS GATE: PR #{pr_num} required jobs \
                                     still pending: {}",
                                    pending_jobs.join(", ")
                                ));
                                merge::ChecksOutcome::TimedOut
                            }
                        }
                    } else {
                        checks_outcome
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
                                    teardown_reviewer(
                                        config,
                                        wt_mgr,
                                        name_pool,
                                        r,
                                        "verdict:approved",
                                    )
                                    .await;
                                    if let Some(wi) =
                                        workers.iter().position(|w| w.task_id == reviewer_task_id)
                                    {
                                        let w = workers.remove(wi);
                                        cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                                    }
                                }
                                None => {
                                    let r = reviewers.remove(ri);
                                    teardown_reviewer(
                                        config,
                                        wt_mgr,
                                        name_pool,
                                        r,
                                        "verdict:approved",
                                    )
                                    .await;
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
                            teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved")
                                .await;
                            if let Some(wi) =
                                workers.iter().position(|w| w.task_id == reviewer_task_id)
                            {
                                let w = workers.remove(wi);
                                cleanup_slot(config, wt_mgr, name_pool, w, None).await;
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
                            .map(|w| w.agent_name.clone());
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
                                    blocking_count: gated.blocking_count.unwrap_or(0) as i64,
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

                    // Master-green gate: hold the merge while the default
                    // branch's latest CI run is red/pending. Proceed with a
                    // warning after the timeout — the PR being merged may
                    // itself be the fix.
                    if config.master_ci_gate {
                        let deadline = std::time::Instant::now()
                            + std::time::Duration::from_secs(config.master_ci_timeout_secs);
                        loop {
                            let branch_status = {
                                let repo = config.repo_dir.clone();
                                let executor = Arc::clone(&config.merge_executor);
                                let branch = config.base_branch.clone();
                                tokio::task::spawn_blocking(move || {
                                    executor.check_default_branch_ci(&repo, &branch)
                                })
                                .await
                                .map_err(|e| {
                                    QuorumError::Io(format!(
                                        "default_branch_ci spawn_blocking join: {e}"
                                    ))
                                })?
                            };
                            match branch_status {
                                merge::DefaultBranchStatus::Green => {
                                    log(&format!(
                                        "master-ci gate: {} CI is green",
                                        config.base_branch
                                    ));
                                    break;
                                }
                                merge::DefaultBranchStatus::Unknown => {
                                    log(&format!(
                                        "master-ci gate: {} CI status unknown \
                                         — proceeding",
                                        config.base_branch
                                    ));
                                    break;
                                }
                                merge::DefaultBranchStatus::Red { ref details } => {
                                    log(&format!(
                                        "MASTER-CI GATE: {} CI is red ({details}) \
                                         — holding merge for PR #{pr_num}",
                                        config.base_branch
                                    ));
                                }
                                merge::DefaultBranchStatus::Pending => {
                                    log(&format!(
                                        "MASTER-CI GATE: {} CI is pending \
                                         — holding merge for PR #{pr_num}",
                                        config.base_branch
                                    ));
                                }
                            }
                            let poll = config.merge_checks_poll_secs;
                            if std::time::Instant::now() + std::time::Duration::from_secs(poll)
                                > deadline
                            {
                                log(&format!(
                                    "MASTER-CI GATE: {} CI still not green after \
                                     {}s — proceeding (the PR may be the fix)",
                                    config.base_branch, config.master_ci_timeout_secs
                                ));
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(poll)).await;
                        }
                    }

                    let merge_result = 'merge_gate: loop {
                        let attempt = {
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
                            .map_err(|e| {
                                QuorumError::Io(format!("merge spawn_blocking join: {e}"))
                            })?
                        };

                        if !attempt.success
                            && attempt.failure_kind == Some(merge::MergeFailureKind::PolicyPending)
                            && policy_retry < MAX_POLICY_RETRIES
                        {
                            policy_retry += 1;
                            log(&format!(
                                "PR #{pr_num} merge policy-pending (attempt {policy_retry}/\
                                 {MAX_POLICY_RETRIES}): {} — re-waiting for checks",
                                attempt.message
                            ));
                            let retry_outcome = {
                                let repo = config.repo_dir.clone();
                                let executor = Arc::clone(&config.merge_executor);
                                let timeout = config.merge_checks_timeout_secs;
                                let poll = config.merge_checks_poll_secs;
                                tokio::task::spawn_blocking(move || {
                                    executor.wait_for_checks(pr_num, &repo, timeout, poll)
                                })
                                .await
                                .map_err(|e| {
                                    QuorumError::Io(format!("checks spawn_blocking join: {e}"))
                                })?
                            };
                            match retry_outcome {
                                merge::ChecksOutcome::Ready => {
                                    log(&format!(
                                        "checks passed for PR #{pr_num} (retry) \
                                         — retrying merge"
                                    ));
                                    continue 'merge_gate;
                                }
                                merge::ChecksOutcome::Failed { .. }
                                | merge::ChecksOutcome::TimedOut => {
                                    break 'merge_gate attempt;
                                }
                            }
                        }

                        break 'merge_gate attempt;
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

                        // R2 audit: capture R1 info before teardown, maybe spawn R2.
                        let r1_name = reviewers[ri].agent_name.clone();
                        let r1_run_id = reviewers[ri].agent_run_id;
                        if config.r2_enabled && !drain_state.draining {
                            maybe_spawn_r2(
                                config,
                                wt_mgr,
                                name_pool,
                                &mut *auditors,
                                lifetime_roster,
                                pr_num,
                                reviewer_task_id,
                                &r1_name,
                                r1_run_id,
                            )
                            .await;
                        }

                        let r = reviewers.remove(ri);
                        teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved").await;
                        if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id)
                        {
                            let w = workers.remove(wi);
                            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                        }
                        reviewer_provision_tracker.clear(reviewer_task_id, pr_num);
                    } else {
                        let failure_kind = merge_result
                            .failure_kind
                            .unwrap_or(merge::MergeFailureKind::PolicyBlocked);

                        match failure_kind {
                            merge::MergeFailureKind::PolicyBlocked
                            | merge::MergeFailureKind::PolicyPending => {
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
                                teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved")
                                    .await;
                                if let Some(wi) =
                                    workers.iter().position(|w| w.task_id == reviewer_task_id)
                                {
                                    let w = workers.remove(wi);
                                    cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                                }
                            }
                            merge::MergeFailureKind::Retryable => {
                                log(&format!(
                                    "PR #{pr_num} merge failed (retryable): {} \
                                     — firing MergeFailed",
                                    merge_result.message
                                ));
                                // merging → in-review (NotifyOwner alert posted by lifecycle)
                                let mf = fire_event(
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

                                let is_review_only =
                                    mf.as_ref().is_some_and(|tr| tr.task.review_only);

                                if is_review_only {
                                    log(&format!(
                                        "review-only task #{reviewer_task_id}: \
                                         merge blocked (retryable) — parking, not failing"
                                    ));
                                    set_task_body(
                                        &db_path,
                                        reviewer_task_id,
                                        tasks::MERGE_BLOCKED_BODY,
                                    )
                                    .await;
                                    let r = reviewers.remove(ri);
                                    teardown_reviewer(
                                        config,
                                        wt_mgr,
                                        name_pool,
                                        r,
                                        "verdict:approved",
                                    )
                                    .await;
                                } else {
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
                                                            reason: format!(
                                                                "rework feed failed: {e}"
                                                            ),
                                                        },
                                                    )
                                                    .await;
                                                    cleanup_slot(
                                                        config, wt_mgr, name_pool, w, None,
                                                    )
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
                                                            let mut conn =
                                                                quorum_core::db::open(&p)?;
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
                                            teardown_reviewer(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                r,
                                                "verdict:approved",
                                            )
                                            .await;
                                            if let Some(wi) = workers
                                                .iter()
                                                .position(|w| w.task_id == reviewer_task_id)
                                            {
                                                let w = workers.remove(wi);
                                                cleanup_slot(config, wt_mgr, name_pool, w, None)
                                                    .await;
                                            }
                                        }
                                        None => {
                                            let r = reviewers.remove(ri);
                                            teardown_reviewer(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                r,
                                                "verdict:approved",
                                            )
                                            .await;
                                        }
                                    }
                                } // end !review_only
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

                    // #90: record the changes verdict in approvals (mirrors approved path).
                    if let Some(pr_num) = row.pr {
                        let reviewer_name = reviewers[ri].agent_name.clone();
                        let author = workers
                            .iter()
                            .find(|w| w.task_id == reviewer_task_id)
                            .map(|w| w.agent_name.clone());
                        if let Some(author) = author {
                            let blocking = gated.blocking_count.unwrap_or(0) as i64;
                            let p = db_path.clone();
                            let record = quorum_core::approvals::Approval {
                                pr_number: pr_num,
                                task_id: reviewer_task_id,
                                author,
                                reviewer: reviewer_name,
                                verdict: "changes".to_string(),
                                blocking_count: blocking,
                                approved_head_sha: String::new(),
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
                            } else {
                                log("no worker for rework — releasing task");
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
                            teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:changes")
                                .await;
                            if let Some(wi) =
                                workers.iter().position(|w| w.task_id == reviewer_task_id)
                            {
                                let w = workers.remove(wi);
                                cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                            }
                        }
                        None => {
                            let r = reviewers.remove(ri);
                            teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:changes")
                                .await;
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
                    teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:none").await;
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
                                    // C6: feed the existing reviewer a re-review
                                    // turn so it keeps its session context.
                                    if let Some(ri) = reviewers
                                        .iter()
                                        .position(|r| r.task_id == workers[wi].task_id)
                                    {
                                        let rereview_turn = reviewer::build_rereview_turn(
                                            &reviewers[ri].agent_name,
                                            pr,
                                            &workers[wi].agent_name,
                                        );
                                        if let Err(e) =
                                            reviewers[ri].proc.feed_turn(&rereview_turn).await
                                        {
                                            log(&format!(
                                                "ResumeReviewer: feed_turn failed \
                                                 for task #{}: {e} — tearing down",
                                                workers[wi].task_id
                                            ));
                                            let r = reviewers.remove(ri);
                                            teardown_reviewer(
                                                config, wt_mgr, name_pool, r, "failed",
                                            )
                                            .await;
                                        } else {
                                            log(&format!(
                                                "ResumeReviewer: fed re-review turn \
                                                 to {} for task #{}",
                                                reviewers[ri].agent_name, workers[wi].task_id
                                            ));
                                            reviewers[ri].turn_started_at =
                                                std::time::Instant::now();
                                        }
                                    }
                                }
                                Effect::SpawnReviewer => {
                                    // Intentionally deferred: reviewer spawning for a
                                    // newly-InReview task is picked up by the Phase 5
                                    // tick reconciler ("Spawn reviewers for workers with
                                    // PRs"), not dispatched inline here. Costs at most
                                    // one tick — not worth a log line on every task.
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
        teardown_reviewer(config, wt_mgr, name_pool, dead, "watchdog").await;
    }

    // ── Phase 3-idle: Kill idle reviewers (same logic as workers) ──────
    let idle_timeout = config.limits.idle_timeout_secs.unwrap_or(300);
    let mut idle_reviewers: Vec<usize> = Vec::new();
    for (i, r) in reviewers.iter().enumerate() {
        if r.draining {
            continue;
        }
        if let Some(ended) = r.turn_ended_at {
            if ended.elapsed().as_secs() > idle_timeout {
                log(&format!(
                    "WATCHDOG: reviewer {} idle {}s (limit {}s) — killing zombie",
                    r.agent_name,
                    ended.elapsed().as_secs(),
                    idle_timeout
                ));
                idle_reviewers.push(i);
            }
        }
    }
    for &i in idle_reviewers.iter().rev() {
        let dead = reviewers.remove(i);
        fire_event(
            &db_path,
            &dead.agent_name,
            dead.task_id,
            &Event::AgentFailed {
                reason: format!(
                    "reviewer idle {}s between turns (limit {}s) — zombie reaped",
                    dead.turn_ended_at.unwrap().elapsed().as_secs(),
                    idle_timeout
                ),
            },
        )
        .await;
        teardown_reviewer(config, wt_mgr, name_pool, dead, "idle").await;
    }

    // ── Phase 3-R2: Drain events from R2 auditors (shadow — no lifecycle) ──
    let mut auditors_to_kill: Vec<usize> = Vec::new();
    for (i, a) in auditors.iter_mut().enumerate() {
        if !a.draining {
            continue;
        }
        if let Some(breach) = check_wall_clock_limits(&config.limits, a) {
            log(&format!("R2 auditor {} killed — {}", a.agent_name, breach));
            auditors_to_kill.push(i);
            continue;
        }
        if let Some(breach) = drain_events(a, &db_path, "auditor", &config.limits).await? {
            log(&format!("R2 auditor {} killed — {}", a.agent_name, breach));
            auditors_to_kill.push(i);
        }
    }
    for &i in auditors_to_kill.iter().rev() {
        let dead = auditors.remove(i);
        teardown_reviewer(config, wt_mgr, name_pool, dead, "watchdog").await;
    }
    // R2 idle watchdog
    {
        let mut idle_auditors: Vec<usize> = Vec::new();
        for (i, a) in auditors.iter().enumerate() {
            if a.draining {
                continue;
            }
            if let Some(ended) = a.turn_ended_at {
                if ended.elapsed().as_secs() > idle_timeout {
                    log(&format!(
                        "R2 auditor {} idle {}s — killing zombie",
                        a.agent_name,
                        ended.elapsed().as_secs()
                    ));
                    idle_auditors.push(i);
                }
            }
        }
        for &i in idle_auditors.iter().rev() {
            let dead = auditors.remove(i);
            R2_META.lock().unwrap().remove(&dead.agent_name);
            teardown_reviewer(config, wt_mgr, name_pool, dead, "idle").await;
        }
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

    // ── Phase 4-idle: Kill workers idle too long between turns ─────────
    // A worker that completed a turn (draining=false) but never gets a new
    // turn is a zombie — e.g. it asked a question in dontAsk mode (#74).
    let mut idle_zombies: Vec<usize> = Vec::new();
    for (i, w) in workers.iter().enumerate() {
        if w.draining || w.error_turn_count > 0 {
            continue;
        }
        if let Some(ended) = w.turn_ended_at {
            if ended.elapsed().as_secs() > idle_timeout {
                log(&format!(
                    "WATCHDOG: worker {} idle {}s on task #{} (limit {}s) — killing zombie",
                    w.agent_name,
                    ended.elapsed().as_secs(),
                    w.task_id,
                    idle_timeout
                ));
                idle_zombies.push(i);
            }
        }
    }
    for &i in idle_zombies.iter().rev() {
        let dead = workers.remove(i);
        fire_event(
            &db_path,
            &dead.agent_name,
            dead.task_id,
            &Event::AgentFailed {
                reason: format!(
                    "worker idle {}s between turns (limit {}s) — zombie reaped",
                    dead.turn_ended_at.unwrap().elapsed().as_secs(),
                    idle_timeout
                ),
            },
        )
        .await;
        cleanup_slot(config, wt_mgr, name_pool, dead, None).await;
    }

    // ── Phase 4-refeed: Auto-refeed workers whose last turn ended with an error ──
    // An error-terminated result (is_error=true) leaves the worker idle with
    // error_turn_count > 0. Re-feed a continuation turn so the agent retries.
    // After MAX_ERROR_RETRIES consecutive errors, fire AgentFailed.
    let mut error_failed: Vec<usize> = Vec::new();
    for (i, w) in workers.iter_mut().enumerate() {
        if w.error_turn_count == 0 || w.draining {
            continue;
        }
        if w.error_turn_count >= MAX_ERROR_RETRIES {
            log(&format!(
                "worker {} exhausted error retries ({}/{}) on task #{} — firing AgentFailed",
                w.agent_name, w.error_turn_count, MAX_ERROR_RETRIES, w.task_id
            ));
            error_failed.push(i);
            continue;
        }
        let turn = agent::user_turn(&format!(
            "Your previous turn was interrupted by a transport/API error (attempt {}/{}). \
             Verify any partial state from your last turn, then continue your task.",
            w.error_turn_count, MAX_ERROR_RETRIES
        ));
        match w.proc.feed_turn(&turn).await {
            Ok(()) => {
                w.draining = true;
                w.turn_started_at = std::time::Instant::now();
                log(&format!(
                    "auto-refeed worker {} after error (attempt {}/{})",
                    w.agent_name, w.error_turn_count, MAX_ERROR_RETRIES
                ));
            }
            Err(e) => {
                log(&format!(
                    "auto-refeed worker {} failed: {e} — marking for AgentFailed",
                    w.agent_name
                ));
                error_failed.push(i);
            }
        }
    }
    for &i in error_failed.iter().rev() {
        let dead = workers.remove(i);
        fire_event(
            &db_path,
            &dead.agent_name,
            dead.task_id,
            &Event::AgentFailed {
                reason: format!(
                    "worker exhausted error retries ({} consecutive error-terminated turns)",
                    dead.error_turn_count
                ),
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
            teardown_reviewer(config, wt_mgr, name_pool, r, "drain").await;
        }

        // R2 auditors: shadow mode — no lifecycle events, just tear down.
        let drain_auditors: Vec<usize> = auditors
            .iter()
            .enumerate()
            .filter(|(_, a)| !a.draining)
            .map(|(i, _)| i)
            .collect();
        for &i in drain_auditors.iter().rev() {
            let a = auditors.remove(i);
            log(&format!("DRAIN: tearing down R2 auditor {}", a.agent_name));
            R2_META.lock().unwrap().remove(&a.agent_name);
            teardown_reviewer(config, wt_mgr, name_pool, a, "drain").await;
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
        teardown_reviewer(config, wt_mgr, name_pool, dead, "crashed").await;
    }

    // Dead R2 auditors — shadow, no lifecycle events.
    let mut dead_auditors: Vec<usize> = Vec::new();
    for (i, a) in auditors.iter_mut().enumerate() {
        if matches!(a.proc.try_wait(), Ok(Some(_))) {
            log(&format!("R2 auditor {} died — cleaning up", a.agent_name));
            dead_auditors.push(i);
        }
    }
    for &i in dead_auditors.iter().rev() {
        let dead = auditors.remove(i);
        R2_META.lock().unwrap().remove(&dead.agent_name);
        teardown_reviewer(config, wt_mgr, name_pool, dead, "crashed").await;
    }

    // ── Phase 4b2: Detect externally-cancelled/terminal tasks ───────────
    // A creator (or another daemon) may cancel/complete a task while our
    // worker is still running on it. Poll the DB for current status of all
    // held tasks; tear down any whose task reached a terminal state.
    {
        let held_task_ids: Vec<i64> = workers
            .iter()
            .map(|w| w.task_id)
            .chain(reviewers.iter().map(|r| r.task_id))
            .collect();
        if !held_task_ids.is_empty() {
            let p = db_path.clone();
            let terminal_tasks: Vec<(i64, String)> = tokio::task::spawn_blocking(move || {
                let conn = quorum_core::db::open(&p)?;
                let mut result = Vec::new();
                for tid in held_task_ids {
                    if let Some(task) = tasks::get(&conn, tid)? {
                        if task
                            .status
                            .parse::<lifecycle::Status>()
                            .is_ok_and(|s| s.is_terminal())
                        {
                            result.push((tid, task.status));
                        }
                    }
                }
                Ok::<_, QuorumError>(result)
            })
            .await
            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            .unwrap_or_default();

            for (tid, status) in terminal_tasks {
                if let Some(wi) = workers.iter().position(|w| w.task_id == tid) {
                    log(&format!(
                        "task #{tid} externally moved to {status} — tearing down worker {}",
                        workers[wi].agent_name,
                    ));
                    let w = workers.remove(wi);
                    cleanup_slot(config, wt_mgr, name_pool, w, None).await;
                }
                if let Some(ri) = reviewers.iter().position(|r| r.task_id == tid) {
                    log(&format!(
                        "task #{tid} externally moved to {status} — tearing down reviewer {}",
                        reviewers[ri].agent_name,
                    ));
                    let r = reviewers.remove(ri);
                    teardown_reviewer(config, wt_mgr, name_pool, r, "cancelled").await;
                }
            }
        }
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
        let mut repo_mismatch_workers: Vec<(usize, String)> = Vec::new();
        let mut pr_closed_workers: Vec<usize> = Vec::new();
        for (pr, task_id, wi) in &needs_reviewer_from_workers {
            let pr_state = {
                let exec = config.merge_executor.clone();
                let pr_num = *pr;
                let repo = config.repo_dir.clone();
                tokio::task::spawn_blocking(move || exec.check_mergeability(pr_num, &repo))
                    .await
                    .unwrap_or(merge::MergeabilityState::Mergeable)
            };
            match pr_state {
                merge::MergeabilityState::AlreadyMerged => {
                    log(&format!(
                        "PR #{pr} already merged — firing PrFoundMerged for task #{task_id}"
                    ));
                    fire_event(&db_path, "system", *task_id, &Event::PrFoundMerged).await;
                    pr_closed_workers.push(*wi);
                    continue;
                }
                merge::MergeabilityState::Closed => {
                    log(&format!(
                        "PR #{pr} closed without merge — firing PrFoundClosed for task #{task_id}"
                    ));
                    fire_event(&db_path, "system", *task_id, &Event::PrFoundClosed).await;
                    pr_closed_workers.push(*wi);
                    continue;
                }
                _ => {}
            }
            // #75: detect cross-repo PR before burning provision strikes
            let task_refs = lookup_task_refs(&db_path, *task_id).await;
            if let Some(task_repo) =
                check_repo_mismatch(&task_refs, &config.repo, config.self_repo.as_deref())
            {
                log(&format!(
                    "REPO MISMATCH: task #{task_id} PR #{pr} belongs to {task_repo}, \
                     not {} — parking immediately",
                    config.repo
                ));
                repo_mismatch_workers.push((*wi, task_repo));
                continue;
            }
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
        for &wi in pr_closed_workers.iter().rev() {
            let w = workers.remove(wi);
            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
        }
        // #75: park repo-mismatch workers without burning strikes
        // Process in reverse index order to avoid index invalidation.
        repo_mismatch_workers.sort_by_key(|b| std::cmp::Reverse(b.0));
        for (wi, task_repo) in repo_mismatch_workers {
            let w = workers.remove(wi);
            let pr_label =
                w.pr.map(|n| format!("#{n}"))
                    .unwrap_or_else(|| "unknown".to_string());
            fire_event(
                &db_path,
                &w.agent_name,
                w.task_id,
                &Event::Cancelled {
                    by: "daemon:parked:repo-mismatch".into(),
                },
            )
            .await;
            let body = format!(
                "{}repo-mismatch | PR {pr_label} belongs to {task_repo}, \
                 not {} — cannot provision reviewer from this daemon",
                tasks::PARKED_BODY_PREFIX,
                config.repo
            );
            set_task_body(&db_path, w.task_id, &body).await;
            notify_provision_failure(
                &db_path,
                w.task_id,
                &format!("repo mismatch ({task_repo} vs {})", config.repo),
                &pr_label,
            )
            .await;
            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
        }
        for &wi in parked_workers.iter().rev() {
            let w = workers.remove(wi);
            let pr_label =
                w.pr.map(|n| format!("#{n}"))
                    .unwrap_or_else(|| "unknown".to_string());
            fire_event(
                &db_path,
                &w.agent_name,
                w.task_id,
                &Event::Cancelled {
                    by: "daemon:provision-exhausted".into(),
                },
            )
            .await;
            set_task_body(
                &db_path,
                w.task_id,
                &format!(
                    "{}provision-exhausted | PR {pr_label} | \
                     reviewer provision failed {MAX_REVIEWER_PROVISION_STRIKES} time(s)",
                    tasks::PARKED_BODY_PREFIX
                ),
            )
            .await;
            notify_provision_failure(
                &db_path,
                w.task_id,
                "reviewer provision exhausted",
                &pr_label,
            )
            .await;
            cleanup_slot(config, wt_mgr, name_pool, w, None).await;
        }

        // ── Phase 5b: Spawn reviewers for orphan in-review tasks ──────
        // After a stateless recovery (or if a worker exited without being
        // tracked), in-review tasks with a PR but no live worker or reviewer
        // need a reviewer provisioned from the DB state alone.
        // (task_id, pr, author, review_only, body, reviewer, refs)
        type OrphanRow = (
            i64,
            i64,
            String,
            bool,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let orphan_in_review: Vec<OrphanRow> = {
            let p = db_path.clone();
            tokio::task::spawn_blocking(move || -> Result<Vec<OrphanRow>> {
                let conn = quorum_core::db::open(&p)?;
                let ir_tasks = tasks::list(&conn, Some("in-review"), None, None)?;
                let mut result = Vec::new();
                for t in ir_tasks {
                    if let Some(pr) = tasks::extract_pr_number(&t.refs) {
                        let author = t.author.unwrap_or_default();
                        result.push((t.id, pr, author, t.review_only, t.body, t.reviewer, t.refs));
                    }
                }
                Ok(result)
            })
            .await
            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            .unwrap_or_default()
        };
        for (task_id, pr, author, review_only, body, reviewer, task_refs) in &orphan_in_review {
            let has_worker = workers.iter().any(|w| w.task_id == *task_id);
            let has_reviewer = reviewers.iter().any(|r| r.task_id == *task_id);
            if has_worker || has_reviewer {
                continue;
            }
            let pr_state = {
                let exec = config.merge_executor.clone();
                let pr_num = *pr;
                let repo = config.repo_dir.clone();
                tokio::task::spawn_blocking(move || exec.check_mergeability(pr_num, &repo))
                    .await
                    .unwrap_or(merge::MergeabilityState::Mergeable)
            };
            match pr_state {
                merge::MergeabilityState::AlreadyMerged => {
                    log(&format!(
                        "PR #{pr} already merged (orphan in-review) — \
                         firing PrFoundMerged for task #{task_id}"
                    ));
                    fire_event(&db_path, "system", *task_id, &Event::PrFoundMerged).await;
                    continue;
                }
                merge::MergeabilityState::Closed => {
                    log(&format!(
                        "PR #{pr} closed without merge (orphan in-review) — \
                         firing PrFoundClosed for task #{task_id}"
                    ));
                    fire_event(&db_path, "system", *task_id, &Event::PrFoundClosed).await;
                    continue;
                }
                merge::MergeabilityState::Conflicting
                    if body.as_deref() == Some(tasks::MERGE_BLOCKED_BODY) =>
                {
                    continue;
                }
                _ => {}
            }

            // Merge-blocked retry: approved review-only task waiting for
            // PR to become mergeable again.
            if body.as_deref() == Some(tasks::MERGE_BLOCKED_BODY) {
                if let Some(reviewer_name) = reviewer.as_deref() {
                    log(&format!(
                        "merge-blocked task #{task_id} PR #{pr}: \
                         PR is now mergeable — retrying merge"
                    ));
                    set_task_body(&db_path, *task_id, "").await;
                    fire_event(&db_path, reviewer_name, *task_id, &Event::VerdictApprove).await;
                } else {
                    log(&format!(
                        "merge-blocked task #{task_id} PR #{pr}: \
                         no reviewer on record — cannot retry merge"
                    ));
                    fire_event(
                        &db_path,
                        "system",
                        *task_id,
                        &Event::AgentFailed {
                            reason: "merge-blocked but no reviewer to re-approve".into(),
                        },
                    )
                    .await;
                }
                continue;
            }

            // #75: detect cross-repo PR before burning provision strikes
            if let Some(other_repo) =
                check_repo_mismatch(task_refs, &config.repo, config.self_repo.as_deref())
            {
                log(&format!(
                    "REPO MISMATCH: orphan in-review task #{task_id} PR #{pr} \
                     belongs to {other_repo}, not {} — parking",
                    config.repo
                ));
                fire_event(
                    &db_path,
                    "daemon",
                    *task_id,
                    &Event::Cancelled {
                        by: "daemon:parked:repo-mismatch".into(),
                    },
                )
                .await;
                let body = format!(
                    "{}repo-mismatch | PR #{pr} belongs to {other_repo}, \
                     not {} — cannot provision reviewer from this daemon",
                    tasks::PARKED_BODY_PREFIX,
                    config.repo
                );
                set_task_body(&db_path, *task_id, &body).await;
                notify_provision_failure(
                    &db_path,
                    *task_id,
                    &format!("repo mismatch ({other_repo} vs {})", config.repo),
                    &format!("#{pr}"),
                )
                .await;
                continue;
            }
            if reviewer_provision_tracker.is_exhausted(*task_id, *pr) {
                log(&format!(
                    "orphan in-review task #{task_id} PR #{pr}: \
                     provision exhausted — parking"
                ));
                fire_event(
                    &db_path,
                    "daemon",
                    *task_id,
                    &Event::Cancelled {
                        by: "daemon:parked:provision-exhausted".into(),
                    },
                )
                .await;
                set_task_body(
                    &db_path,
                    *task_id,
                    &format!(
                        "{}provision-exhausted | PR #{pr} | \
                         reviewer provision failed (orphan in-review)",
                        tasks::PARKED_BODY_PREFIX
                    ),
                )
                .await;
                notify_provision_failure(
                    &db_path,
                    *task_id,
                    "reviewer provision exhausted (orphan in-review)",
                    &format!("#{pr}"),
                )
                .await;
                continue;
            }
            let branch = if let Some(b) = orphan_worker_branch(author, *task_id, *review_only) {
                b
            } else {
                // Review-only tasks (or tasks with no author) have no daemon-authored
                // branch — resolve the PR's head ref from GitHub instead of guessing
                // a malformed daemon/-t<id> branch name.
                let pr_num = *pr;
                let repo_dir = config.repo_dir.clone();
                let gh_repo = config.repo.clone();
                let resolved = tokio::task::spawn_blocking(move || {
                    query_pr_head_ref(pr_num, &repo_dir, Some(&gh_repo))
                })
                .await
                .ok()
                .flatten();
                match resolved {
                    Some(head_ref) => head_ref,
                    None => {
                        log(&format!(
                            "orphan in-review task #{task_id} PR #{pr}: \
                             review-only but could not resolve PR head ref — skipping"
                        ));
                        continue;
                    }
                }
            };
            let counterpart = ReviewCounterpart {
                agent_name: if author.is_empty() {
                    "external"
                } else {
                    author
                },
                task_id: *task_id,
                branch: &branch,
            };
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
    }

    // ── Phase 6: Spawn workers up to cap ───────────────────────────────
    // Gate on worker count, not total in_use_count() — reviewers must
    // not consume worker capacity (F16).
    // Skip during drain — no new tasks, let existing agents finish.
    if !drain_state.draining {
        while workers.len() < config.cap {
            if !spawn_worker(
                config,
                wt_mgr,
                name_pool,
                workers,
                poison_tracker,
                lifetime_roster,
            )
            .await?
            {
                break;
            }
        }
    }

    // ── Phase 7: Task classifier ─────────────────────────────────────
    // 7a: Drain events from in-flight classifier.
    if let Some(slot) = classifier_slot.as_mut() {
        // Check if the classifier process exited.
        let exited = matches!(slot.proc.try_wait(), Ok(Some(_)));

        if let Some(result) = classifier::drain_classifier_events(slot).await {
            match result {
                classifier::ClassifierResult::Done(text) => {
                    if let Some(results) = classifier::parse_response(&text) {
                        let p = db_path.clone();
                        let version = quorum_core::classify::CLASSIFIER_VERSION.to_string();
                        let stored = tokio::task::spawn_blocking(move || -> Result<usize> {
                            let mut conn = quorum_core::db::open(&p)?;
                            let now = now_unix();
                            quorum_core::classify::store_classifications(
                                &mut conn, &results, &version, now,
                            )
                        })
                        .await
                        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                        .unwrap_or(0);

                        if stored > 0 {
                            log(&format!("classifier: stored {stored} classification(s)"));
                        }
                    } else {
                        log("classifier: failed to parse response");
                    }
                    *classifier_slot = None;
                }
                classifier::ClassifierResult::Error(e) => {
                    log(&format!("classifier: {e}"));
                    *classifier_slot = None;
                }
            }
        } else if exited {
            // Process exited without a Result event.
            if !slot.response_text.is_empty() {
                let text = std::mem::take(&mut slot.response_text);
                if let Some(results) = classifier::parse_response(&text) {
                    let p = db_path.clone();
                    let version = quorum_core::classify::CLASSIFIER_VERSION.to_string();
                    let stored = tokio::task::spawn_blocking(move || -> Result<usize> {
                        let mut conn = quorum_core::db::open(&p)?;
                        let now = now_unix();
                        quorum_core::classify::store_classifications(
                            &mut conn, &results, &version, now,
                        )
                    })
                    .await
                    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                    .unwrap_or(0);

                    if stored > 0 {
                        log(&format!("classifier: stored {stored} classification(s)"));
                    }
                } else {
                    log("classifier: process exited without parseable response");
                }
            } else {
                log("classifier: process exited without response");
            }
            *classifier_slot = None;
        }
    }

    // 7b: Spawn classifier if idle and there are unclassified tasks.
    if classifier_slot.is_none() && !drain_state.draining {
        let p = db_path.clone();
        let unclassified = tokio::task::spawn_blocking(move || -> Result<(Vec<quorum_core::classify::TaskForClassification>, Vec<quorum_core::classify::TaskForClassification>)> {
            let conn = quorum_core::db::open(&p)?;
            let tasks = quorum_core::classify::unclassified_tasks(&conn)?;
            if tasks.is_empty() {
                return Ok((vec![], vec![]));
            }
            let all_open = quorum_core::classify::dup_context_tasks(&conn)?;
            let task_ids: Vec<i64> = tasks.iter().map(|t| t.id).collect();
            let dup_context: Vec<_> = all_open
                .into_iter()
                .filter(|t| !task_ids.contains(&t.id))
                .collect();
            Ok((tasks, dup_context))
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?;

        if let Ok((tasks, dup_context)) = unclassified {
            if !tasks.is_empty() {
                let turn = classifier::classifier_turn(&tasks, &dup_context);
                match classifier::spawn_classifier(
                    &tasks,
                    &dup_context,
                    &config.repo_dir,
                    config.agent_bin.as_deref(),
                    config.bare_agent,
                ) {
                    Ok(mut slot) => {
                        if let Err(e) = slot.proc.feed_turn(&turn).await {
                            log(&format!("classifier: feed_turn failed: {e}"));
                        } else {
                            log(&format!("classifier: spawned for {} task(s)", tasks.len()));
                            *classifier_slot = Some(slot);
                        }
                    }
                    Err(e) => {
                        log(&format!("classifier: spawn failed: {e}"));
                    }
                }
            }
        }
    }

    // ── Phase 8: Doctor agent ────────────────────────────────────────
    // 8a: Drain events from in-flight doctor.
    if let Some(slot) = doctor_slot.as_mut() {
        let exited = matches!(slot.proc.try_wait(), Ok(Some(_)));

        if let Some(result) = doctor::drain_doctor_events(slot).await {
            let tid = slot.task_id;
            match result {
                doctor::DoctorResult::Done(text) => {
                    log(&format!(
                        "doctor: finished for task #{tid} ({}b)",
                        text.len()
                    ));
                }
                doctor::DoctorResult::Error(e) => {
                    log(&format!("doctor: error on task #{tid}: {e}"));
                }
            }
            doctored_tasks.insert(tid);
            *doctor_slot = None;
        } else if exited {
            let tid = slot.task_id;
            log(&format!("doctor: process exited for task #{tid}"));
            doctored_tasks.insert(tid);
            *doctor_slot = None;
        }
    }

    // 8b: Spawn doctor if enabled, idle, not draining, and a stalled task exists.
    if config.doctor_enabled && doctor_slot.is_none() && !drain_state.draining {
        let p = db_path.clone();
        let active_worker_task_ids: Vec<i64> = workers.iter().map(|w| w.task_id).collect();
        let active_reviewer_task_ids: Vec<i64> = reviewers.iter().map(|r| r.task_id).collect();
        let already_doctored = doctored_tasks.clone();

        let stalled =
            tokio::task::spawn_blocking(move || -> Result<Option<doctor::EvidenceBundle>> {
                let conn = quorum_core::db::open(&p)?;
                let working = tasks::list(&conn, Some("working"), None, None)?;
                let in_review = tasks::list(&conn, Some("in-review"), None, None)?;
                let candidates: Vec<_> = working.into_iter().chain(in_review).collect();
                for task in candidates {
                    if active_worker_task_ids.contains(&task.id)
                        || active_reviewer_task_ids.contains(&task.id)
                        || already_doctored.contains(&task.id)
                    {
                        continue;
                    }
                    let refs_json: Option<serde_json::Value> = task
                        .refs
                        .as_deref()
                        .and_then(|s| serde_json::from_str(s).ok());
                    let pr = refs_json.as_ref().and_then(|v| v["pr"].as_i64());
                    let worktree_path = refs_json
                        .as_ref()
                        .and_then(|v| v["worktree"].as_str().map(|s| s.to_string()));
                    return Ok(Some(doctor::EvidenceBundle {
                        task_id: task.id,
                        task_title: task.title,
                        task_status: task.status,
                        task_body: task.body,
                        author: task.assignee,
                        pr,
                        worktree_path,
                        repo: String::new(),
                    }));
                }
                Ok(None)
            })
            .await
            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?;

        if let Ok(Some(mut evidence)) = stalled {
            evidence.repo = config.repo.clone();
            let allowed = config
                .allowed_tools
                .as_deref()
                .unwrap_or(agent::ALLOWED_TOOLS);
            let turn = doctor::doctor_turn(&evidence);
            match doctor::spawn_doctor(
                evidence.task_id,
                &config.repo_dir,
                config.agent_bin.as_deref(),
                config.bare_agent,
                allowed,
                &config.repo,
            ) {
                Ok(mut slot) => {
                    if let Err(e) = slot.proc.feed_turn(&turn).await {
                        log(&format!("doctor: feed_turn failed: {e}"));
                    } else {
                        log(&format!("doctor: spawned for task #{}", evidence.task_id));
                        *doctor_slot = Some(slot);
                    }
                }
                Err(e) => {
                    log(&format!("doctor: spawn failed: {e}"));
                }
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
                is_error,
                ..
            } => {
                let error_terminated = is_error.unwrap_or(false);
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
                    "{role} {} result (turn_tokens={}, cumulative={}, cost_usd={:.4}{})",
                    slot.agent_name,
                    turn_tokens,
                    slot.cost_tokens,
                    slot.cost_usd,
                    if error_terminated { ", ERROR" } else { "" }
                ));

                let phase = if error_terminated {
                    "working"
                } else if role == "worker" {
                    "awaiting-review"
                } else {
                    "reviewing"
                };

                if let Some(ref mut sl) = slot.session_log {
                    sl.update_cost(slot.cost_tokens, slot.cost_usd);
                    sl.set_phase(phase);
                }

                let p = db_path.to_path_buf();
                let entry = slot_journal_entry(slot, role, phase);
                tokio::task::spawn_blocking(move || -> Result<()> {
                    let mut conn = quorum_core::db::open(&p)?;
                    journal::upsert(&mut conn, &entry)
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                .ok();

                slot.draining = false;
                slot.turn_ended_at = Some(std::time::Instant::now());
                slot.live_stats.mid_turn_tokens = 0;
                slot.live_stats.record_event();
                write_live_sidecar(slot);

                if error_terminated {
                    slot.error_turn_count += 1;
                } else {
                    slot.error_turn_count = 0;
                }

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
                let content = message.get("content");
                if let Some(blocks) = content.and_then(|c| c.as_array()) {
                    for block in blocks {
                        match block.get("type").and_then(|t| t.as_str()) {
                            Some("text") => {
                                if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                                    let preview = truncate_preview(text);
                                    log(&format!("{role} {}: {preview}", slot.agent_name));
                                }
                            }
                            Some("tool_use") => {
                                if let Some(name) = block.get("name").and_then(|n| n.as_str()) {
                                    let input = block
                                        .get("input")
                                        .cloned()
                                        .unwrap_or(serde_json::Value::Null);
                                    slot.live_stats.tool_count += 1;
                                    slot.live_stats.now_label = now_label(name, &input);
                                }
                            }
                            _ => {}
                        }
                    }
                } else if let Some(text) = content.and_then(|c| c.as_str()) {
                    let preview = truncate_preview(text);
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

fn truncate_preview(text: &str) -> String {
    if text.len() > 80 {
        let end = text.char_indices().nth(80).map_or(text.len(), |(i, _)| i);
        format!("{}…", &text[..end])
    } else {
        text.to_string()
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
/// under review.
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

    let session_id = agent::new_session_id();
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
        vec![
            ("QUORUM_REPO".into(), config.repo.clone()),
            ("QUORUM_AGENT".into(), reviewer_name.clone()),
        ],
        config.allowed_tools.as_deref(),
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

            let reviewer_run_id = {
                let p = config.db_path.clone();
                let name = reviewer_name.clone();
                let m = config.model.clone();
                let e = config.effort.clone();
                let tid = worker.task_id;
                tokio::task::spawn_blocking(move || -> Result<i64> {
                    let conn = quorum_core::db::open(&p)?;
                    quorum_core::agent_runs::insert(
                        &conn,
                        tid,
                        &name,
                        "reviewer",
                        &m,
                        &e,
                        now_unix(),
                    )
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                .ok()
            };

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
                turn_ended_at: None,
                agent_state: None,
                session_log: reviewer_session_log,
                live_stats: LiveStats::new(),
                error_turn_count: 0,
                agent_run_id: reviewer_run_id,
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
    poison_tracker: &mut PoisonTracker,
    lifetime_roster: &mut LifetimeRoster,
) -> Result<bool> {
    let db_path = config.db_path.clone();
    let p = db_path.clone();

    let in_flight: Vec<i64> = workers.iter().map(|w| w.task_id).collect();
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

    let session_id = agent::new_session_id();
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
    let resolved_model = label_model.unwrap_or_else(|| config.model.clone());
    let resolved_effort = label_effort.unwrap_or_else(|| config.effort.clone());
    let spec = AgentSpec {
        model: resolved_model.clone(),
        effort: resolved_effort.clone(),
        session_id: session_id.clone(),
        worktree: wt_path.clone(),
        bare: config.bare_agent,
        allowed_tools: config
            .allowed_tools
            .clone()
            .unwrap_or_else(|| agent::ALLOWED_TOOLS.to_string()),
        env_vars: vec![
            ("QUORUM_REPO".into(), config.repo.clone()),
            ("QUORUM_AGENT".into(), agent_name.clone()),
        ],
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

            let worker_run_id = {
                let p = db_path.clone();
                let name = agent_name.clone();
                let m = resolved_model.clone();
                let e = resolved_effort.clone();
                let tid = task.id;
                tokio::task::spawn_blocking(move || -> Result<i64> {
                    let conn = quorum_core::db::open(&p)?;
                    quorum_core::agent_runs::insert(&conn, tid, &name, "worker", &m, &e, now_unix())
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
                .ok()
            };

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
                turn_ended_at: None,
                agent_state: None,
                session_log: worker_session_log,
                live_stats: LiveStats::new(),
                error_turn_count: 0,
                agent_run_id: worker_run_id,
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
            depends_on: None,
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
            depends_on: None,
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

async fn emit_kill_event(db_path: &std::path::Path, target: &str, by: &str, reason: &str) {
    let p = db_path.to_path_buf();
    let subj = format!("agent:{target}");
    let body = format!("killed by {by}: {reason}");
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        let tx = quorum_core::db::begin_immediate(&mut conn)?;
        quorum_core::events::emit(&tx, "agent_killed", &subj, &body, now)?;
        tx.commit()?;
        Ok(())
    })
    .await
    .ok();
}

async fn set_task_body(db_path: &std::path::Path, task_id: i64, body: &str) {
    let p = db_path.to_path_buf();
    let b = body.to_string();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        tasks::set_body(&mut conn, task_id, &b, now)
    })
    .await;
    match result {
        Ok(Err(e)) => log(&format!("set_task_body failed for task #{task_id}: {e}")),
        Err(e) => log(&format!(
            "set_task_body join error for task #{task_id}: {e}"
        )),
        Ok(Ok(())) => {}
    }
}

/// Post a direct message to a task's creator when the daemon parks a task
/// due to provision failure (exhausted strikes or repo mismatch).
async fn notify_provision_failure(
    db_path: &std::path::Path,
    task_id: i64,
    reason: &str,
    pr_label: &str,
) {
    let p = db_path.to_path_buf();
    let tid = task_id;
    let reason_owned = reason.to_string();
    let pr_label_owned = pr_label.to_string();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        let body = format!(
            "task #{tid} parked: {reason_owned} | PR {pr_label_owned} — \
             review linkage lost, manual re-queue may be needed"
        );
        quorum_core::feed::post(
            &mut conn,
            "daemon",
            "critical",
            None,
            &body,
            None,
            Some("owner"),
            86400,
            now,
        )?;
        Ok(())
    })
    .await;
    match result {
        Ok(Err(e)) => log(&format!(
            "notify_provision_failure failed for task #{task_id}: {e}"
        )),
        Err(e) => log(&format!(
            "notify_provision_failure join error for task #{task_id}: {e}"
        )),
        Ok(Ok(())) => log(&format!(
            "notified creator of task #{task_id} about provision failure"
        )),
    }
}

/// Check if a task's refs.repo mismatches all repos this daemon can provision from.
/// Returns `Some(task_repo)` on mismatch, `None` if matching or unknown.
fn check_repo_mismatch(
    task_refs: &Option<String>,
    daemon_repo: &str,
    self_repo: Option<&str>,
) -> Option<String> {
    let task_repo = tasks::extract_repo(task_refs)?;
    if task_repo == daemon_repo {
        return None;
    }
    if let Some(sr) = self_repo {
        if task_repo == sr {
            return None;
        }
    }
    Some(task_repo)
}

/// Look up a task's refs from the DB. Returns None on any failure.
async fn lookup_task_refs(db_path: &std::path::Path, task_id: i64) -> Option<String> {
    let p = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Option<String> {
        let conn = quorum_core::db::open(&p).ok()?;
        let task = tasks::get(&conn, task_id).ok()??;
        task.refs
    })
    .await
    .ok()
    .flatten()
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
    close_agent_run(&config.db_path, state.agent_run_id, "done").await;

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
    let end_reason = if task_status == "open" {
        "failed"
    } else {
        task_status
    };
    close_agent_run(&config.db_path, state.agent_run_id, end_reason).await;

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
                ..Default::default()
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
    end_reason: &str,
) {
    log(&format!("tearing down reviewer {}", state.agent_name));

    if let Some(ref mut sl) = state.session_log {
        sl.finalize(None);
    }

    state.proc.kill_and_reap().await;
    close_agent_run(&config.db_path, state.agent_run_id, end_reason).await;

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

// ── R2 review-audit helpers ──────────────────────────────────────────────

/// R2 metadata stashed alongside the SlotState for audit recording.
struct R2Meta {
    r1_reviewer: String,
    r1_run_id: Option<i64>,
    worker_model: String,
    worker_effort: String,
    cx_bucket: String,
}

/// Map from R2 agent_name → R2Meta. Separate from SlotState to avoid bloating it.
static R2_META: std::sync::LazyLock<std::sync::Mutex<HashMap<String, R2Meta>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[allow(clippy::too_many_arguments)]
async fn maybe_spawn_r2(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    auditors: &mut Vec<SlotState>,
    lifetime_roster: &mut LifetimeRoster,
    pr: i64,
    task_id: i64,
    r1_reviewer: &str,
    r1_run_id: Option<i64>,
) {
    // Query stratum data from DB.
    let p = config.db_path.clone();
    let tid = task_id;
    let target = config.r2_target_per_stratum;
    let prob = config.r2_steady_state_p;
    let default_model = config.model.clone();
    let default_effort = config.effort.clone();

    let sample_result =
        tokio::task::spawn_blocking(move || -> Result<Option<(String, String, String)>> {
            let conn = quorum_core::db::open(&p)?;
            let stratum = quorum_core::review_audits::task_stratum(
                &conn,
                tid,
                &default_model,
                &default_effort,
            )?;
            let counts = quorum_core::review_audits::stratum_counts(&conn)?;
            // ponytail: use task_id as seed — deterministic per-task, varies across tasks
            let should = quorum_core::review_audits::should_sample(
                &counts, &stratum, target, prob, tid as u64,
            );
            if should {
                let (model, effort, cx) = stratum;
                Ok(Some((model, effort, cx)))
            } else {
                Ok(None)
            }
        })
        .await;

    let Some(Ok(Some((model, effort, cx)))) = sample_result.ok() else {
        return;
    };

    log(&format!(
        "R2: spawning auditor for PR #{pr} (stratum: {model}/{effort}/{cx})"
    ));

    let acquire_result = name_pool.acquire();
    let r2_name = acquire_result.into_name();
    lifetime_roster.register(&r2_name);

    // Drain stale mailbox rows.
    {
        let p = config.db_path.clone();
        let name = r2_name.clone();
        let _ = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut conn = quorum_core::db::open(&p)?;
            mailbox::consume_all_for_agent(&mut conn, &name)
        })
        .await;
    }

    let session_id = agent::new_session_id();
    let branch = reviewer::reviewer_branch(pr, &r2_name);
    let wt_path = reviewer::reviewer_worktree_path(&config.worktree_base, pr, &r2_name);

    // Provision worktree from origin/main (post-merge, the PR branch is gone).
    let provision_ok = wt_mgr
        .fetch_and_provision(&config.repo_dir, &branch, &wt_path, &config.base_branch)
        .await
        .is_ok();
    if !provision_ok {
        log(&format!(
            "R2: worktree provision failed for PR #{pr} — skipping"
        ));
        name_pool.release(&r2_name);
        return;
    }

    // Record agent_run with sub_role='r2'.
    let agent_run_id = {
        let p = config.db_path.clone();
        let name = r2_name.clone();
        let m = model.clone();
        let e = effort.clone();
        tokio::task::spawn_blocking(move || -> Result<i64> {
            let conn = quorum_core::db::open(&p)?;
            quorum_core::agent_runs::insert_r2(&conn, task_id, &name, &m, &e, now_unix())
        })
        .await
        .ok()
        .and_then(|r| r.ok())
    };

    // Stash R2 metadata for later audit recording.
    R2_META.lock().unwrap().insert(
        r2_name.clone(),
        R2Meta {
            r1_reviewer: r1_reviewer.to_string(),
            r1_run_id,
            worker_model: model.clone(),
            worker_effort: effort.clone(),
            cx_bucket: cx,
        },
    );

    // Journal + spawn.
    let spec = reviewer::R2AuditSpec {
        pr,
        r1_reviewer: r1_reviewer.to_string(),
        r2_name: r2_name.clone(),
    };
    let prompt = reviewer::build_r2_audit_prompt(&spec);

    let session_log = config.log_dir.as_ref().and_then(|d| {
        session_log::SessionLog::create(
            d,
            &r2_name,
            "auditor",
            Some(task_id),
            &session_id,
            &branch,
            now_unix(),
        )
        .ok()
    });

    match reviewer::spawn_reviewer(
        &model,
        &effort,
        &session_id,
        &wt_path,
        config.agent_bin.as_deref(),
        config.bare_agent,
        vec![],
        config.allowed_tools.as_deref(),
    )
    .await
    {
        Ok(mut proc) => {
            let _ = proc.feed_turn(&prompt).await;
            let now_inst = std::time::Instant::now();
            let slot = SlotState {
                agent_name: r2_name.clone(),
                proc,
                task_id,
                session_id,
                worktree_path: wt_path,
                branch,
                draining: true,
                pr: Some(pr),
                rework_count: 0,
                cost_tokens: 0,
                cost_usd: 0.0,
                task_started_at: now_inst,
                turn_started_at: now_inst,
                turn_ended_at: None,
                agent_state: None,
                session_log,
                live_stats: LiveStats::new(),
                error_turn_count: 0,
                agent_run_id,
            };
            // Journal the R2 as a reviewer in "auditing" phase.
            {
                let p = config.db_path.clone();
                let entry = slot_journal_entry(&slot, "reviewer", "auditing");
                tokio::task::spawn_blocking(move || {
                    if let Ok(mut conn) = quorum_core::db::open(&p) {
                        let _ = journal::upsert(&mut conn, &entry);
                    }
                })
                .await
                .ok();
            }
            auditors.push(slot);
            log(&format!("R2: auditor {r2_name} spawned for PR #{pr}"));
        }
        Err(e) => {
            log(&format!("R2: failed to spawn auditor: {e}"));
            R2_META.lock().unwrap().remove(&r2_name);
            close_agent_run(&config.db_path, agent_run_id, "spawn-failed").await;
            wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
            name_pool.release(&r2_name);
        }
    }
}

async fn handle_r2_done(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    auditors: &mut Vec<SlotState>,
    ai: usize,
    row: &quorum_core::mailbox::MailboxRow,
) {
    let a = auditors.remove(ai);
    let r2_name = a.agent_name.clone();
    let r2_run_id = a.agent_run_id;

    // Parse R2 findings from the note/payload.
    let (missed_count, overcaught_count, r2_verdict) = parse_r2_payload(row);

    // Retrieve stashed R2 metadata.
    let meta = R2_META.lock().unwrap().remove(&r2_name);

    if let Some(meta) = meta {
        let audit = quorum_core::review_audits::ReviewAudit {
            task_id: a.task_id,
            pr_number: row.pr.unwrap_or(0),
            r1_run_id: meta.r1_run_id.unwrap_or(0),
            r2_run_id: r2_run_id.unwrap_or(0),
            r1_reviewer: meta.r1_reviewer,
            r2_reviewer: r2_name.clone(),
            model: meta.worker_model,
            effort: meta.worker_effort,
            cx_bucket: meta.cx_bucket,
            missed_count,
            overcaught_count,
            r1_verdict: "approved".into(),
            r2_verdict: r2_verdict.clone(),
            created_at: now_unix(),
        };
        let p = config.db_path.clone();
        let _ = tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = quorum_core::db::open(&p)?;
            quorum_core::review_audits::insert(&conn, &audit)?;
            Ok(())
        })
        .await;
        log(&format!(
            "R2: audit recorded for PR #{} — missed={}, overcaught={}, verdict={:?}",
            row.pr.unwrap_or(0),
            missed_count,
            overcaught_count,
            r2_verdict
        ));
    }

    teardown_reviewer(config, wt_mgr, name_pool, a, "r2-done").await;
}

fn parse_r2_payload(row: &quorum_core::mailbox::MailboxRow) -> (i64, i64, Option<String>) {
    let verdict = row.verdict.clone();
    // Try to parse structured findings from note or payload.
    let text = row.note.as_deref().or(row.payload.as_deref()).unwrap_or("");
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
        let missed = v
            .get("missed")
            .and_then(|a| a.as_array())
            .map(|a| a.len() as i64)
            .unwrap_or(0);
        let overcaught = v
            .get("overcaught")
            .and_then(|a| a.as_array())
            .map(|a| a.len() as i64)
            .unwrap_or(0);
        let v_from_json = v
            .get("verdict")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        return (missed, overcaught, verdict.or(v_from_json));
    }
    // Fallback: use blocking count from verdict gate if available.
    (0, 0, verdict)
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
            turn_ended_at: None,
            agent_state: None,
            session_log: None,
            live_stats: LiveStats::new(),
            error_turn_count: 0,
            agent_run_id: None,
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
        assert!(limits.idle_timeout_secs.is_none());
    }

    #[test]
    fn idle_timeout_detects_zombie_slot() {
        let mut slot = make_dummy_slot();
        // Simulate: turn ended 400s ago, not draining, no errors
        slot.draining = false;
        slot.error_turn_count = 0;
        slot.turn_ended_at = Some(std::time::Instant::now() - std::time::Duration::from_secs(400));

        let timeout = 300u64;
        let is_zombie = !slot.draining
            && slot.error_turn_count == 0
            && slot
                .turn_ended_at
                .is_some_and(|t| t.elapsed().as_secs() > timeout);
        assert!(is_zombie);
    }

    #[test]
    fn idle_timeout_ignores_draining_slot() {
        let mut slot = make_dummy_slot();
        slot.draining = true;
        slot.turn_ended_at = Some(std::time::Instant::now() - std::time::Duration::from_secs(400));

        let timeout = 300u64;
        let is_zombie = !slot.draining
            && slot.error_turn_count == 0
            && slot
                .turn_ended_at
                .is_some_and(|t| t.elapsed().as_secs() > timeout);
        assert!(!is_zombie);
    }

    #[test]
    fn idle_timeout_ignores_fresh_idle_slot() {
        let mut slot = make_dummy_slot();
        slot.draining = false;
        slot.turn_ended_at = Some(std::time::Instant::now());

        let timeout = 300u64;
        let is_zombie = !slot.draining
            && slot.error_turn_count == 0
            && slot
                .turn_ended_at
                .is_some_and(|t| t.elapsed().as_secs() > timeout);
        assert!(!is_zombie);
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
    fn check_repo_mismatch_detects_cross_repo() {
        let refs = Some(r#"{"pr":318,"repo":"ag2trust/quorum"}"#.to_string());
        let result = check_repo_mismatch(&refs, "ag2trust/ag2trust", None);
        assert_eq!(result, Some("ag2trust/quorum".to_string()));
    }

    #[test]
    fn check_repo_mismatch_same_repo_returns_none() {
        let refs = Some(r#"{"pr":318,"repo":"ag2trust/quorum"}"#.to_string());
        assert_eq!(check_repo_mismatch(&refs, "ag2trust/quorum", None), None);
    }

    #[test]
    fn check_repo_mismatch_self_repo_returns_none() {
        let refs = Some(r#"{"pr":318,"repo":"ag2trust/quorum"}"#.to_string());
        assert_eq!(
            check_repo_mismatch(&refs, "ag2trust/ag2trust", Some("ag2trust/quorum")),
            None
        );
    }

    #[test]
    fn check_repo_mismatch_no_repo_returns_none() {
        let refs = Some(r#"{"pr":318}"#.to_string());
        assert_eq!(check_repo_mismatch(&refs, "ag2trust/ag2trust", None), None);
    }

    #[test]
    fn check_repo_mismatch_none_refs_returns_none() {
        assert_eq!(check_repo_mismatch(&None, "ag2trust/ag2trust", None), None);
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
    fn assistant_content_array_extracts_tool_use() {
        let message = serde_json::json!({
            "content": [
                {"type": "text", "text": "Let me check that."},
                {"type": "tool_use", "name": "Bash", "input": {"command": "cargo test"}},
                {"type": "tool_use", "name": "Read", "input": {"file_path": "/foo/bar.rs"}}
            ]
        });
        let mut tool_count: u32 = 0;
        let mut last_now = String::new();
        if let Some(blocks) = message.get("content").and_then(|c| c.as_array()) {
            for block in blocks {
                if block.get("type").and_then(|t| t.as_str()) == Some("tool_use") {
                    if let Some(name) = block.get("name").and_then(|n| n.as_str()) {
                        let input = block
                            .get("input")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        tool_count += 1;
                        last_now = now_label(name, &input);
                    }
                }
            }
        }
        assert_eq!(tool_count, 2);
        assert_eq!(last_now, "Read: bar.rs");
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

    #[test]
    fn orphan_worker_branch_daemon_authored() {
        assert_eq!(
            orphan_worker_branch("Anvil", 42, false),
            Some("daemon/anvil-t42".into())
        );
    }

    #[test]
    fn orphan_worker_branch_review_only_returns_none() {
        assert_eq!(orphan_worker_branch("", 15, true), None);
    }

    #[test]
    fn orphan_worker_branch_empty_author_returns_none() {
        assert_eq!(orphan_worker_branch("", 15, false), None);
    }

    #[test]
    fn orphan_worker_branch_review_only_with_author_returns_none() {
        assert_eq!(orphan_worker_branch("Anvil", 15, true), None);
    }
}
