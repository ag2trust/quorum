//! `quorum serve` — the agent-manager daemon.
//!
//! Builds a tokio runtime and runs an async tick loop that polls the mailbox,
//! spawns/drives agents, and shuts down cleanly on Ctrl-C. See spec §3.

pub mod agent;
pub mod approvals;
pub mod classifier;
#[allow(dead_code)]
pub mod codex_agent;
#[allow(dead_code)]
pub mod codex_stream;
pub mod collector;
pub mod doctor;
pub mod merge;
pub mod names;
pub mod recovery;
pub mod render;
pub mod reviewer;
pub mod runner;
pub mod session_log;
pub mod stream;
pub mod worktree;

use agent::{AgentProc, AgentSpec};
use names::Pool;
use quorum_core::error::{QuorumError, Result};
use quorum_core::journal::{self, JournalEntry};
use quorum_core::lifecycle::{self, Effect, Event};
use quorum_core::mailbox;
use quorum_core::pr_targets;
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
const MAX_TOTAL_REVIEWER_RUNS: i64 = 12;

const REQUIRED_REVIEW_ROLES: &[&str] = &["r1", "r2"];

#[derive(Debug, Clone)]
enum ReviewRole {
    R1,
    R2 {
        r1_reviewer: String,
        r1_run_id: Option<i64>,
    },
}

impl ReviewRole {
    fn as_str(&self) -> &'static str {
        match self {
            ReviewRole::R1 => "r1",
            ReviewRole::R2 { .. } => "r2",
        }
    }

    fn is_r2(&self) -> bool {
        matches!(self, ReviewRole::R2 { .. })
    }
}

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

/// Determine the next review role needed for a PR, checking durable approvals.
/// Returns `None` if all required roles are approved for the given `head_sha`.
fn next_needed_role(
    conn: &quorum_core::Connection,
    pr_number: i64,
    head_sha: &str,
) -> Result<Option<&'static str>> {
    for &role in REQUIRED_REVIEW_ROLES {
        match quorum_core::approvals::get(conn, pr_number, role)? {
            Some(a)
                if a.verdict == "approved"
                    && a.blocking_count == 0
                    && !a.approved_head_sha.is_empty()
                    && a.approved_head_sha == head_sha =>
            {
                continue;
            }
            _ => return Ok(Some(role)),
        }
    }
    Ok(None)
}

/// Check whether provision attempts for a (task, PR, role) are exhausted.
fn is_provision_exhausted(
    conn: &quorum_core::Connection,
    task_id: i64,
    pr: i64,
    role: &str,
    head_sha: &str,
) -> Result<bool> {
    let attempts =
        quorum_core::provision_attempts::get_attempts(conn, task_id, pr, role, head_sha)?;
    Ok(attempts >= MAX_REVIEWER_PROVISION_STRIKES as i64)
}

/// Check whether total reviewer runs for a task exceed the safety cap.
fn is_reviewer_cap_exceeded(conn: &quorum_core::Connection, task_id: i64) -> Result<bool> {
    let total = quorum_core::provision_attempts::total_reviewer_runs(conn, task_id)?;
    Ok(total >= MAX_TOTAL_REVIEWER_RUNS)
}

/// Lifetime roster of agent names this daemon has ever owned.
///
/// Tracks every agent name this daemon has spawned or resumed — entries are
/// inserted at spawn/recover and NEVER removed. Used to distinguish F9
/// phantom rows (from our own past agents) from passive/interactive agent
/// submissions. daemon_lock (invariant 11) guarantees single daemon per DB,
/// so non-roster names are always passive agents, never a sibling daemon.
pub(crate) struct LifetimeRoster {
    names: std::collections::HashSet<String>,
}

impl LifetimeRoster {
    fn new() -> Self {
        Self {
            names: std::collections::HashSet::new(),
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
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
struct PrTarget {
    pr: i64,
    head_ref: String,
    head_sha: String,
    is_fork: bool,
}

fn resolve_pr_target(pr: i64, repo_dir: &Path, gh_repo: Option<&str>) -> Option<PrTarget> {
    let mut args = vec![
        "pr".to_string(),
        "view".to_string(),
        pr.to_string(),
        "--json".to_string(),
        "headRefName,headRefOid,isCrossRepository".to_string(),
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
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let head_ref = json.get("headRefName")?.as_str()?.to_string();
    let head_sha = json.get("headRefOid")?.as_str()?.to_string();
    let is_fork = json
        .get("isCrossRepository")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if head_ref.is_empty() || head_sha.is_empty() {
        return None;
    }
    Some(PrTarget {
        pr,
        head_ref,
        head_sha,
        is_fork,
    })
}

fn log(msg: &str) {
    let _ = writeln!(std::io::stderr(), "quorum serve: {msg}");
}

/// Fire off a detached post-merge review-analytics collection for `pr_num`.
///
/// The daemon already fired `MergeSucceeded` before this runs — the task is
/// `done` and the reviewer verdict is final. The collector is analytics-only:
/// it never mutates lifecycle, verdicts, or GitHub. Failures land in the
/// `review_collection_runs` table (loud, observable, retryable) and MUST NOT
/// touch the completed task. See serve/collector.rs.
fn spawn_post_merge_collector(config: &ServeConfig, pr_num: i64, task_id: i64) {
    let request = collector::CollectionRequest::new(
        pr_num,
        Some(task_id),
        // ServeConfig::repo is the "owner/name" slug this daemon manages —
        // pass it explicitly so `gh api` targets the right repo when the
        // daemon's cwd is a worktree or unrelated dir.
        if config.repo.is_empty() {
            None
        } else {
            Some(config.repo.clone())
        },
        config.db_path.clone(),
        config.repo_dir.clone(),
        config.agent_bin.clone(),
        config.bare_agent,
    );
    collector::spawn_detached(request);
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

/// Resolve PR target from GitHub, persist on success, fall back to
/// persisted target when GitHub is unavailable.
fn resolve_or_load_pr_target(
    pr: i64,
    task_id: i64,
    db_path: &Path,
    repo_dir: &Path,
    gh_repo: Option<&str>,
) -> Option<PrTarget> {
    if let Some(target) = resolve_pr_target(pr, repo_dir, gh_repo) {
        if let Ok(mut conn) = quorum_core::db::open(db_path) {
            let _ = pr_targets::upsert(
                &mut conn,
                task_id,
                target.pr,
                &target.head_ref,
                &target.head_sha,
                target.is_fork,
            );
        }
        return Some(target);
    }
    let conn = quorum_core::db::open(db_path).ok()?;
    let stored = pr_targets::get(&conn, task_id, pr).ok()??;
    Some(PrTarget {
        pr: stored.pr_number,
        head_ref: stored.head_ref,
        head_sha: stored.head_sha,
        is_fork: stored.is_fork,
    })
}

/// Map a tier label suffix to a full Claude model ID.
/// Returns `None` (fall back to global default) for unknown tiers.
pub fn tier_to_model_id_pub(tier: &str) -> Option<String> {
    tier_to_model_id(tier)
}

fn tier_to_model_id(tier: &str) -> Option<String> {
    match tier {
        "opus-46" => Some("claude-opus-4-6".into()),
        "opus-47" => Some("claude-opus-4-7".into()),
        "opus-48" => Some("claude-opus-4-8".into()),
        "sonnet-5" => Some("claude-sonnet-5".into()),
        "o3" => Some("o3".into()),
        "o4-mini" => Some("o4-mini".into()),
        unknown => {
            log(&format!(
                "unknown tier label '{unknown}', falling back to global model"
            ));
            None
        }
    }
}

fn runner_kind_to_agent_kind(kind: crate::serve_config::RunnerKind) -> runner::AgentKind {
    match kind {
        crate::serve_config::RunnerKind::Claude => runner::AgentKind::Claude,
        crate::serve_config::RunnerKind::Codex => runner::AgentKind::Codex,
    }
}

/// Known OpenAI/Codex model prefixes. A model matches if it equals a prefix
/// or starts with `<prefix>-`.
const CODEX_MODEL_PREFIXES: &[&str] = &["o3", "o4-mini", "gpt-4.1", "gpt-5"];

/// Resolve the provider (AgentKind) from a fully-qualified model ID.
///
/// Claude models start with `claude-`. OpenAI/Codex models match
/// `CODEX_MODEL_PREFIXES`. Unknown models are rejected before spawning.
pub fn resolve_provider(model: &str) -> Result<runner::AgentKind> {
    if model.starts_with("claude-") {
        return Ok(runner::AgentKind::Claude);
    }
    for &prefix in CODEX_MODEL_PREFIXES {
        if model == prefix || model.starts_with(&format!("{prefix}-")) {
            return Ok(runner::AgentKind::Codex);
        }
    }
    Err(QuorumError::Usage(format!(
        "unknown model '{model}': cannot resolve provider \
         (expected claude-* or known OpenAI model)"
    )))
}

fn resolve_worker_provider(
    model: &str,
    has_model_override: bool,
    is_retry: bool,
    default_runner: crate::serve_config::RunnerKind,
) -> Result<runner::AgentKind> {
    if is_retry || has_model_override {
        resolve_provider(model)
    } else {
        Ok(runner_kind_to_agent_kind(default_runner))
    }
}

fn resolve_remediation_provider(
    recorded_provider: Option<&str>,
    recorded_model: Option<String>,
    config_model: &str,
    config_runner: crate::serve_config::RunnerKind,
) -> (String, runner::AgentKind) {
    let model = recorded_model.unwrap_or_else(|| config_model.to_string());
    let kind = recorded_provider
        .and_then(|provider| match provider {
            "claude" => Some(runner::AgentKind::Claude),
            "codex" => Some(runner::AgentKind::Codex),
            _ => None,
        })
        .or_else(|| resolve_provider(&model).ok())
        .unwrap_or_else(|| runner_kind_to_agent_kind(config_runner));
    (model, kind)
}

const MODEL_TIERS: &[&str] = &[
    "claude-sonnet-5",
    "claude-opus-4-6",
    "claude-opus-4-7",
    "claude-opus-4-8",
];

fn model_rank(model: &str) -> Option<usize> {
    MODEL_TIERS.iter().position(|&m| m == model)
}

fn escalated_reviewer_model(worker_model: &str, config_model: &str) -> String {
    use crate::serve::runner::AgentKind;
    let worker_is_claude = AgentKind::for_model(worker_model) == AgentKind::Claude;
    let config_is_claude = AgentKind::for_model(config_model) == AgentKind::Claude;

    // Both non-Claude (e.g. Codex): no tier escalation available, use
    // config model as-is so the reviewer inherits the daemon's provider.
    if !worker_is_claude && !config_is_claude {
        return config_model.to_string();
    }

    let worker_rank = model_rank(worker_model).unwrap_or(0);
    let config_rank = model_rank(config_model).unwrap_or(0);
    let escalated = (worker_rank + 1).min(MODEL_TIERS.len() - 1);
    let final_rank = escalated.max(config_rank);
    MODEL_TIERS[final_rank].to_string()
}

fn extract_cx_est(refs: &Option<String>) -> Option<i64> {
    let s = refs.as_deref()?;
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    v.get("cx_est")?.as_i64()
}

fn extract_complexity_label(labels_json: Option<&str>) -> Option<u8> {
    let arr: Vec<String> = serde_json::from_str(labels_json?).ok()?;
    arr.iter().find_map(|l| {
        l.strip_prefix("complexity:")
            .and_then(|v| v.parse::<u8>().ok())
            .filter(|&v| (1..=5).contains(&v))
    })
}

fn effort_rank(effort: &str) -> u8 {
    match effort {
        "medium" => 1,
        "high" => 2,
        _ => 0,
    }
}

use quorum_core::complexity::DEFAULT_RECOMMENDATIONS as SUGGESTED_DEFAULTS;

fn suggested_for(
    cx: u8,
    overrides: &std::collections::HashMap<String, String>,
) -> (String, String) {
    if let Some(val) = overrides.get(&cx.to_string()) {
        if let Some((tier, effort)) = val.split_once('/') {
            if let Some(model) = tier_to_model_id(tier) {
                if effort == "medium" || effort == "high" {
                    return (model, effort.to_string());
                }
            }
        }
    }
    SUGGESTED_DEFAULTS
        .iter()
        .find(|(level, _, _)| *level == cx)
        .map(|(_, model, effort)| (model.to_string(), effort.to_string()))
        .unwrap_or_else(|| ("claude-opus-4-6".into(), "medium".into()))
}

/// #172: clamp a resolved (model, effort) up to the configured floor for worker
/// spawn. `min_model` is a full model id (e.g. "claude-opus-4-7"); `min_effort`
/// is "medium"|"high". A `None` field imposes no floor on that dimension — the
/// resolved value stands in as the missing companion, so the combined
/// `is_model_effort_below` comparison only fires on the configured dimension.
/// Never lowers a pair already at/above the floor.
fn apply_model_effort_floor(
    resolved_model: &str,
    resolved_effort: &str,
    min_model: Option<&str>,
    min_effort: Option<&str>,
) -> (String, String) {
    if min_model.is_none() && min_effort.is_none() {
        return (resolved_model.to_string(), resolved_effort.to_string());
    }
    let floor_model = min_model.unwrap_or(resolved_model);
    let floor_effort = min_effort.unwrap_or(resolved_effort);
    if is_model_effort_below(resolved_model, resolved_effort, floor_model, floor_effort) {
        (floor_model.to_string(), floor_effort.to_string())
    } else {
        (resolved_model.to_string(), resolved_effort.to_string())
    }
}

fn is_model_effort_below(
    resolved_model: &str,
    resolved_effort: &str,
    suggested_model: &str,
    suggested_effort: &str,
) -> bool {
    let r_rank = model_rank(resolved_model).unwrap_or(0);
    let s_rank = model_rank(suggested_model).unwrap_or(0);
    if r_rank < s_rank {
        return true;
    }
    if r_rank == s_rank && effort_rank(resolved_effort) < effort_rank(suggested_effort) {
        return true;
    }
    false
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

/// Durably enqueue a post-merge interpret retry job (#127). Best-effort — a
/// write failure only means the row is not durably captured for retry; the
/// primary analytics path (`spawn_post_merge_collector`, #125) still runs.
/// Reconcile at next startup rebuilds any missing job from task state.
async fn enqueue_interpret_job(db_path: &std::path::Path, pr_num: i64, task_id: i64, repo: &str) {
    let p = db_path.to_path_buf();
    let repo_opt = if repo.is_empty() {
        None
    } else {
        Some(repo.to_string())
    };
    let outcome = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        quorum_core::review_interpret_jobs::enqueue(
            &mut conn,
            pr_num,
            task_id,
            repo_opt.as_deref(),
            collector::COLLECTOR_VERSION,
        )
    })
    .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => log(&format!("interpret: enqueue for PR #{pr_num} failed: {e}")),
        Err(e) => log(&format!(
            "interpret: enqueue spawn_blocking join failed for PR #{pr_num}: {e}"
        )),
    }
}

/// One tick's post-merge collector-queue work packed for the spawn_blocking
/// boundary. `converged` counts jobs whose successful run just landed;
/// `next` is the job (if any) whose attempt was reserved and now needs an
/// out-of-tokio spawn; `just_dead_lettered` is populated when this tick's
/// reservation caused a row to cross the cap — the operator log line fires
/// exactly once per PR that dies, not once per tick per PR.
struct InterpretTickOutcome {
    converged: usize,
    next: Option<quorum_core::review_interpret_jobs::InterpretJob>,
    just_dead_lettered: Option<(i64, i64, i64)>,
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
    #[allow(dead_code)]
    pub runner_kind: crate::serve_config::RunnerKind,
    pub repo_dir: PathBuf,
    pub worktree_base: PathBuf,
    pub names_file: Option<PathBuf>,
    pub agent_bin: Option<String>,
    pub model: String,
    pub effort: String,
    pub merge_executor: Arc<dyn merge::MergeExecutor>,
    /// Pass `--bare` to spawned agents, stripping operator-local hooks,
    /// plugins, memory, and MCP config. Default: false (inherit operator login).
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
    // R2 is mandatory (#159) — no sampling config needed.
    /// Per-complexity suggested model/effort overrides (keys "1".."5", values "tier/effort").
    pub suggested_models: std::collections::HashMap<String, String>,
    /// #172: minimum worker model floor as a full model id (e.g. "claude-opus-4-7"),
    /// or None for no floor. Validated + tier→id converted at config load.
    pub min_model: Option<String>,
    /// #172: minimum worker effort floor ("medium"|"high"), or None for no floor.
    pub min_effort: Option<String>,
    /// Codex sandbox mode (default: "danger-full-access").
    pub codex_sandbox: String,
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

    // Abandoned spawn_blocking threads (e.g. wait_for_checks interrupted by
    // drain) keep the runtime alive on implicit drop. Shut down with a short
    // grace period so the process can actually exit.
    rt.shutdown_timeout(std::time::Duration::from_secs(1));

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
    proc: runner::RunnerProc,
    task_id: i64,
    session_id: String,
    /// Exact provider selection for this run. Continuations must never
    /// silently drift to daemon-global defaults.
    model: String,
    effort: String,
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
    last_error_text: Option<String>,
    agent_run_id: Option<i64>,
    /// Daemon-issued run capability id (#130). Used for revocation on teardown.
    cap_run_id: Option<String>,
    /// True when this reviewer slot is an R2 pre-merge reviewer. Survives
    /// across rework so the daemon routes re-submissions back to R2.
    r2_origin: bool,
    /// PR head SHA at reviewer spawn time. Used to detect stale approvals
    /// when the author pushes new commits between review and merge.
    reviewed_head_sha: Option<String>,
    /// Codex thread identity for continuation. Set from the first
    /// `thread.started` event, persisted to task refs before use.
    codex_thread_id: Option<String>,
    pending_prompt: String,
    pending_turn_kind: String,
}

#[derive(Debug, Clone)]
struct CodexRetryTurn {
    model: String,
    effort: String,
    prompt: String,
    turn_kind: String,
    thread_id: Option<String>,
}

fn codex_retry_turn(refs: Option<&str>) -> Option<CodexRetryTurn> {
    let refs: serde_json::Value = serde_json::from_str(refs?).ok()?;
    if !refs.get("codex_retry_requested")?.as_bool()? {
        return None;
    }
    Some(CodexRetryTurn {
        model: refs.get("codex_retry_model")?.as_str()?.to_string(),
        effort: refs.get("codex_retry_effort")?.as_str()?.to_string(),
        prompt: refs.get("codex_retry_prompt")?.as_str()?.to_string(),
        turn_kind: refs.get("codex_retry_turn_kind")?.as_str()?.to_string(),
        thread_id: refs
            .get("codex_retry_thread_id")
            .and_then(|value| value.as_str())
            .map(str::to_string),
    })
}

fn worker_done_event(rework_count: u32, pr: i64) -> Event {
    if rework_count > 0 {
        Event::ReworkPushed
    } else {
        Event::SignaledDone { pr: pr.to_string() }
    }
}

fn daemon_rework_retry_requested(refs: Option<&str>) -> bool {
    refs.and_then(|refs| serde_json::from_str::<serde_json::Value>(refs).ok())
        .and_then(|refs| {
            refs.get(tasks::PARKED_REWORK_RETRY_REF)
                .and_then(|value| value.as_bool())
        })
        .unwrap_or(false)
}

fn retry_slot_rework_count(
    rework_round: i64,
    retry: Option<&CodexRetryTurn>,
    daemon_retry: bool,
) -> u32 {
    if daemon_retry
        || retry
            .map(|retry| retry.turn_kind == "rework")
            .unwrap_or(false)
    {
        u32::try_from(rework_round.max(1)).unwrap_or(u32::MAX)
    } else {
        0
    }
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

/// Resolves when a drain deadline should interrupt long-running work inside
/// tick(). Two modes:
/// - Already draining: fires after `drain_remaining` elapses.
/// - Not yet draining: fires as soon as signal_count >= 1 (signal arrival).
///
/// In both modes, fires immediately on signal_count >= 2 (force shutdown).
async fn drain_interrupt(
    signal_count: std::sync::Arc<std::sync::atomic::AtomicU8>,
    drain_remaining: Option<std::time::Duration>,
) {
    if let Some(remaining) = drain_remaining {
        let deadline = tokio::time::Instant::now() + remaining;
        while tokio::time::Instant::now() < deadline {
            if signal_count.load(std::sync::atomic::Ordering::SeqCst) >= 2 {
                return;
            }
            let left = deadline - tokio::time::Instant::now();
            tokio::time::sleep(left.min(std::time::Duration::from_millis(100))).await;
        }
    } else {
        loop {
            if signal_count.load(std::sync::atomic::Ordering::SeqCst) >= 1 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
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
    let mut drain_state = DrainState::new();
    let mut lifetime_roster = LifetimeRoster::new();
    let mut last_drift_check: Option<std::time::Instant> = None;
    let mut classifier_slot: Option<classifier::ClassifierSlot> = None;
    let mut classifier_consec_errors: u32 = 0;
    let mut classifier_backoff_until: Option<std::time::Instant> = None;
    let mut doctor_slot: Option<doctor::DoctorSlot> = None;
    let mut doctored_tasks: std::collections::HashSet<i64> = std::collections::HashSet::new();
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
    if let Err(e) = approvals::recover(
        &config.db_path,
        &config.repo_dir,
        &config.merge_executor,
        config.merge_checks_timeout_secs,
        config.merge_checks_poll_secs,
    )
    .await
    {
        log(&format!("approval recovery failed: {e} — continuing"));
    }

    // M7: stateless crash recovery — kill stale processes, wipe journal,
    // GC worktrees, and reset non-terminal tasks for the tick loop to handle.
    if let Err(e) = recovery::recover(config, &wt_mgr).await {
        log(&format!("recovery failed: {e} — starting fresh"));
    }

    // #127/#157: report dead-lettered interpret jobs at startup. Historical
    // terminal tasks are NOT scanned — jobs come only from the MergeSucceeded
    // enqueue path. Already-enqueued rows (from prior merges) remain in the
    // queue for the tick drain to process normally.
    {
        let p = config.db_path.clone();
        let outcome = tokio::task::spawn_blocking(
            move || -> Result<Vec<quorum_core::review_interpret_jobs::InterpretJob>> {
                let conn = quorum_core::db::open(&p)?;
                quorum_core::review_interpret_jobs::over_cap(&conn)
            },
        )
        .await
        .map_err(|e| QuorumError::Io(format!("interpret startup join: {e}")))?;
        match outcome {
            Ok(dead) => {
                for job in &dead {
                    log(&format!(
                        "interpret: DEAD-LETTER PR #{} (task #{}, attempts={}): {}",
                        job.pr_number,
                        job.task_id,
                        job.attempts,
                        job.last_error.as_deref().unwrap_or("(no error recorded)")
                    ));
                }
            }
            Err(e) => log(&format!("interpret startup check failed: {e} — continuing")),
        }
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
        if sig >= 2 || (sig >= 1 && workers.is_empty() && reviewers.is_empty()) {
            if sig >= 2 {
                log("force shutdown (second signal)");
            } else {
                log("shutting down (signal, no in-flight agents)");
            }
            if let Some(slot) = classifier_slot.take() {
                slot.proc.kill_and_reap().await;
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
                    "DRAIN: timeout ({}s) — force-killing {} worker(s), {} reviewer(s)",
                    config.drain_timeout_secs,
                    workers.len(),
                    reviewers.len(),
                ));
                if let Some(slot) = classifier_slot.take() {
                    slot.proc.kill_and_reap().await;
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
            &mut poison_tracker,
            &mut drain_state,
            &mut lifetime_roster,
            &mut classifier_slot,
            &mut classifier_consec_errors,
            &mut classifier_backoff_until,
            &mut doctor_slot,
            &mut doctored_tasks,
            &signal_count,
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
    poison_tracker: &mut PoisonTracker,
    drain_state: &mut DrainState,
    lifetime_roster: &mut LifetimeRoster,
    classifier_slot: &mut Option<classifier::ClassifierSlot>,
    classifier_consec_errors: &mut u32,
    classifier_backoff_until: &mut Option<std::time::Instant>,
    doctor_slot: &mut Option<doctor::DoctorSlot>,
    doctored_tasks: &mut std::collections::HashSet<i64>,
    signal_count: &std::sync::Arc<std::sync::atomic::AtomicU8>,
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
                // #130: no passive agent handling — unmatched task_updates
                // are consumed as phantoms.
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
                    // Passive agent — daemon can't terminate it, but
                    // consume the row (single daemon per DB, invariant 11).
                    log(&format!(
                        "kill: agent {target} not managed by daemon — consuming row"
                    ));
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

                    // #159/#190: mandatory dual-review gate. When R1 (non-R2)
                    // approves, store durable approval and provision R2 via the
                    // unified path. If R2 spawn fails, the durable approval
                    // persists and Phase 5 retries on next tick with bounded
                    // attempt tracking.
                    if !reviewers[ri].r2_origin && !drain_state.draining {
                        let r1_reviewer = reviewers[ri].agent_name.clone();
                        let r1_run_id = reviewers[ri].agent_run_id;
                        let author = workers
                            .iter()
                            .find(|w| w.task_id == reviewer_task_id)
                            .map(|w| w.agent_name.clone())
                            .unwrap_or_default();
                        let head_sha = {
                            let repo = config.repo_dir.clone();
                            let executor = Arc::clone(&config.merge_executor);
                            tokio::task::spawn_blocking(move || executor.head_sha(pr_num, &repo))
                                .await
                                .ok()
                                .flatten()
                                .unwrap_or_default()
                        };
                        // Record R1's durable approval.
                        {
                            let p = db_path.clone();
                            let tid = reviewer_task_id;
                            let blocking = gated.blocking_count.unwrap_or(0) as i64;
                            let sha = head_sha.clone();
                            let r1_rev = r1_reviewer.clone();
                            let auth = author.clone();
                            tokio::task::spawn_blocking(move || -> Result<()> {
                                let mut conn = quorum_core::db::open(&p)?;
                                quorum_core::approvals::record(
                                    &mut conn,
                                    &quorum_core::approvals::Approval {
                                        pr_number: pr_num,
                                        review_role: "r1".to_string(),
                                        task_id: tid,
                                        author: auth,
                                        reviewer: r1_rev,
                                        verdict: "approved".to_string(),
                                        blocking_count: blocking,
                                        approved_head_sha: sha,
                                    },
                                )
                            })
                            .await
                            .ok();
                        }

                        // R2 audit: record completed R1 review for the stratum.
                        record_r2_audit(
                            config,
                            &reviewers[ri].agent_name,
                            reviewers[ri].agent_run_id,
                            reviewers[ri].task_id,
                            row.pr,
                            gated.verdict.as_deref(),
                        )
                        .await;

                        // Build counterpart from worker if available, otherwise
                        // resolve from task author or PR head ref.
                        let worker_cp_owned: Option<(String, i64, String)> = if let Some(w) =
                            workers.iter().find(|w| w.task_id == reviewer_task_id)
                        {
                            Some((w.agent_name.clone(), w.task_id, w.branch.clone()))
                        } else {
                            let db_branch = {
                                let p = db_path.clone();
                                let tid = reviewer_task_id;
                                tokio::task::spawn_blocking(move || -> Option<String> {
                                    let conn = quorum_core::db::open(&p).ok()?;
                                    let t = tasks::get(&conn, tid).ok()??;
                                    let a = t.author.unwrap_or_default();
                                    orphan_worker_branch(&a, tid, t.review_only)
                                })
                                .await
                                .ok()
                                .flatten()
                            };
                            if let Some(branch) = db_branch {
                                Some(("external".to_string(), reviewer_task_id, branch))
                            } else {
                                let pr_val = pr_num;
                                let tid = reviewer_task_id;
                                let dbp = db_path.clone();
                                let repo_dir = config.repo_dir.clone();
                                let gh_repo = config.repo.clone();
                                let resolved = tokio::task::spawn_blocking(move || {
                                    resolve_or_load_pr_target(
                                        pr_val,
                                        tid,
                                        &dbp,
                                        &repo_dir,
                                        Some(&gh_repo),
                                    )
                                })
                                .await
                                .ok()
                                .flatten();
                                resolved.map(|target| {
                                    ("external".to_string(), reviewer_task_id, target.head_ref)
                                })
                            }
                        };

                        if let Some((cp_agent, cp_tid, cp_branch)) = worker_cp_owned {
                            let worker_cp = ReviewCounterpart {
                                agent_name: &cp_agent,
                                task_id: cp_tid,
                                branch: &cp_branch,
                            };
                            let role = ReviewRole::R2 {
                                r1_reviewer: r1_reviewer.clone(),
                                r1_run_id,
                            };

                            let pre_count = reviewers.len();
                            provision_reviewer(
                                config,
                                wt_mgr,
                                name_pool,
                                reviewers,
                                lifetime_roster,
                                pr_num,
                                worker_cp,
                                &role,
                                &head_sha,
                            )
                            .await
                            .ok();
                            let r2_added = reviewers.len() > pre_count;

                            if r2_added {
                                log(&format!(
                                    "R2 GATE: PR #{pr_num} — mandatory R2 review spawned, \
                                     tearing down R1 reviewer {}",
                                    r1_reviewer
                                ));
                            } else {
                                log(&format!(
                                    "R2 GATE: R2 spawn failed for PR #{pr_num} \
                                     — R1 approval stored, Phase 5 will retry"
                                ));
                            }
                            let r = reviewers.remove(ri);
                            teardown_reviewer(
                                config,
                                wt_mgr,
                                name_pool,
                                r,
                                if r2_added {
                                    "r2-pending"
                                } else {
                                    "r2-spawn-failed"
                                },
                            )
                            .await;
                            if !consume_mailbox_row(&db_path, *id).await {
                                break;
                            }
                            continue;
                        } else {
                            log(&format!(
                                "R2 GATE: PR #{pr_num} — could not resolve branch for R2 \
                                 counterpart, R1 approval stored, Phase 5 will retry"
                            ));
                            let r = reviewers.remove(ri);
                            teardown_reviewer(config, wt_mgr, name_pool, r, "r2-no-branch").await;
                            if !consume_mailbox_row(&db_path, *id).await {
                                break;
                            }
                            continue;
                        }
                    }

                    // #174: check if task is already merging (merge-wait
                    // retry via unconsumed mailbox row). Skip VerdictApprove
                    // entirely to avoid rejected transitions and unbounded
                    // diagnostic writes.
                    let already_merging = {
                        let p = db_path.clone();
                        tokio::task::spawn_blocking(move || -> bool {
                            quorum_core::db::open(&p)
                                .ok()
                                .and_then(|conn| tasks::get(&conn, reviewer_task_id).ok().flatten())
                                .map(|t| t.status == "merging")
                                .unwrap_or(false)
                        })
                        .await
                        .unwrap_or(false)
                    };
                    if already_merging {
                        log("merge-wait retry: task already merging — proceeding to merge gate");
                    } else {
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
                            teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved")
                                .await;
                            if let Some(wi) =
                                workers.iter().position(|w| w.task_id == reviewer_task_id)
                            {
                                workers[wi].pr = None;
                            }
                            if !consume_mailbox_row(&db_path, *id).await {
                                break;
                            }
                            continue;
                        }
                        // #174: persist R2 approval NOW (before merge gate)
                        // so it survives a restart during merge-wait. Uses
                        // the current head SHA which is the diff R2 reviewed.
                        // Only runs once — merge-wait retries take the
                        // already_merging branch above and skip this.
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
                                    let role = if reviewers[ri].r2_origin { "r2" } else { "r1" };
                                    let record = quorum_core::approvals::Approval {
                                        pr_number: pr_num,
                                        review_role: role.to_string(),
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
                                    .ok();
                                }
                            }
                        }
                    }

                    // R2 audit: record completed R2 review for the stratum.
                    if reviewers[ri].r2_origin {
                        record_r2_audit(
                            config,
                            &reviewers[ri].agent_name,
                            reviewers[ri].agent_run_id,
                            reviewers[ri].task_id,
                            row.pr,
                            gated.verdict.as_deref(),
                        )
                        .await;
                    }

                    // Stale-SHA gate: if the reviewer recorded a head SHA at
                    // spawn time, verify the PR head hasn't moved. A stale
                    // approval cannot authorize a changed diff.
                    if let Some(ref expected_sha) = reviewers[ri].reviewed_head_sha {
                        let current_sha = {
                            let repo = config.repo_dir.clone();
                            let executor = Arc::clone(&config.merge_executor);
                            tokio::task::spawn_blocking(move || executor.head_sha(pr_num, &repo))
                                .await
                                .ok()
                                .flatten()
                        };
                        if let Some(ref current) = current_sha {
                            if current != expected_sha {
                                log(&format!(
                                    "STALE SHA: reviewer {} approved head {} but current \
                                     head is {} — firing MergeFailed for rework",
                                    reviewers[ri].agent_name, expected_sha, current
                                ));
                                fire_event(
                                    &db_path,
                                    "system",
                                    reviewer_task_id,
                                    &Event::MergeFailed {
                                        reason: format!(
                                            "PR #{pr_num} head moved since review \
                                             (approved {}, now {})",
                                            &expected_sha[..8.min(expected_sha.len())],
                                            &current[..8.min(current.len())]
                                        ),
                                    },
                                )
                                .await;
                                let r = reviewers.remove(ri);
                                teardown_reviewer(config, wt_mgr, name_pool, r, "stale-sha").await;
                                if !consume_mailbox_row(&db_path, *id).await {
                                    break;
                                }
                                continue;
                            }
                        }
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
                        // #125 fires the collector immediately (best-effort).
                        // #127 also durably enqueues so the tick loop retries
                        // with backoff and cap; a successful run deletes the
                        // job.
                        spawn_post_merge_collector(config, pr_num, reviewer_task_id);
                        enqueue_interpret_job(&db_path, pr_num, reviewer_task_id, &config.repo)
                            .await;
                        if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id)
                        {
                            let w = workers.remove(wi);
                            cleanup_slot(config, wt_mgr, name_pool, w, None, "merged").await;
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
                                    if let Some(wi) =
                                        workers.iter().position(|w| w.task_id == reviewer_task_id)
                                    {
                                        let rework_prompt = reviewer::build_rework_prompt(
                                            &workers[wi].agent_name,
                                            workers[wi].task_id,
                                            pr_num,
                                            &rework_msg,
                                            workers[wi].cost_usd,
                                            config.limits.max_task_cost_usd,
                                        );
                                        if let Err(e) = feed_worker_turn(
                                            &mut workers[wi],
                                            &rework_prompt,
                                            config,
                                        )
                                        .await
                                        {
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
                                            cleanup_slot(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                w,
                                                None,
                                                "agent_failed",
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
                                    } else {
                                        // #175: no live worker — spawn remediation
                                        log(&format!(
                                            "no worker for rework on task #{reviewer_task_id} \
                                             (pre-merge conflict) — spawning remediation worker"
                                        ));
                                        if pr_num > 0 && !drain_state.draining {
                                            let spawn_ok = spawn_remediation_worker(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                workers,
                                                lifetime_roster,
                                                reviewer_task_id,
                                                pr_num,
                                                &rework_msg,
                                            )
                                            .await;
                                            if !spawn_ok {
                                                log(&format!(
                                                    "remediation worker spawn failed for task \
                                                     #{reviewer_task_id} — firing AgentFailed"
                                                ));
                                                fire_event(
                                                    &db_path,
                                                    "daemon",
                                                    reviewer_task_id,
                                                    &Event::AgentFailed {
                                                        reason: "remediation worker spawn failed"
                                                            .into(),
                                                    },
                                                )
                                                .await;
                                            }
                                        } else {
                                            log(&format!(
                                                "no PR or draining — cannot spawn remediation \
                                                 for task #{reviewer_task_id}"
                                            ));
                                            fire_event(
                                                &db_path,
                                                "daemon",
                                                reviewer_task_id,
                                                &Event::AgentFailed {
                                                    reason: "no worker and no PR for rework".into(),
                                                },
                                            )
                                            .await;
                                        }
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
                                        cleanup_slot(
                                            config,
                                            wt_mgr,
                                            name_pool,
                                            w,
                                            None,
                                            "rework_cap",
                                        )
                                        .await;
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
                    let mut drain_interrupted = false;
                    let checks_outcome = {
                        let repo = config.repo_dir.clone();
                        let executor = Arc::clone(&config.merge_executor);
                        let timeout = config.merge_checks_timeout_secs;
                        let poll = config.merge_checks_poll_secs;
                        let handle = tokio::task::spawn_blocking(move || {
                            executor.wait_for_checks(pr_num, &repo, timeout, poll)
                        });
                        let drain_remaining = if drain_state.draining {
                            Some(
                                std::time::Duration::from_secs(config.drain_timeout_secs)
                                    .saturating_sub(
                                        drain_state.drain_started_at.unwrap().elapsed(),
                                    ),
                            )
                        } else {
                            None
                        };
                        let sc = std::sync::Arc::clone(signal_count);
                        tokio::select! {
                            result = handle => {
                                result.map_err(|e| QuorumError::Io(format!("checks spawn_blocking join: {e}")))?
                            }
                            _ = drain_interrupt(sc, drain_remaining) => {
                                drain_interrupted = true;
                                merge::ChecksOutcome::TimedOut
                            }
                        }
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
                                let pending_list = pending_jobs.join(", ");
                                log(&format!(
                                    "REQUIRED JOBS GATE: PR #{pr_num} required jobs \
                                     still pending: {pending_list}",
                                ));
                                merge::ChecksOutcome::Pending {
                                    reason: format!("required jobs still pending: {pending_list}"),
                                }
                            }
                        }
                    } else {
                        checks_outcome
                    };

                    // Convert non-drain TimedOut to Pending: a timeout
                    // means checks haven't finished yet, not that they
                    // failed. Merge-wait retries instead of reworking.
                    let checks_outcome = match checks_outcome {
                        merge::ChecksOutcome::TimedOut if !drain_interrupted => {
                            merge::ChecksOutcome::Pending {
                                reason: format!(
                                    "CI checks timed out after {}s for PR #{pr_num}",
                                    config.merge_checks_timeout_secs
                                ),
                            }
                        }
                        other => other,
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
                                        let rework_prompt = reviewer::build_rework_prompt(
                                            &workers[wi].agent_name,
                                            workers[wi].task_id,
                                            pr_num,
                                            &rework_msg,
                                            workers[wi].cost_usd,
                                            config.limits.max_task_cost_usd,
                                        );
                                        if let Err(e) = feed_worker_turn(
                                            &mut workers[wi],
                                            &rework_prompt,
                                            config,
                                        )
                                        .await
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
                                            cleanup_slot(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                w,
                                                None,
                                                "agent_failed",
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
                                        // #175: no live worker — spawn remediation
                                        log(&format!(
                                            "no worker for rework on task #{reviewer_task_id} \
                                             (checks failure) — spawning remediation worker"
                                        ));
                                        if pr_num > 0 && !drain_state.draining {
                                            let spawn_ok = spawn_remediation_worker(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                workers,
                                                lifetime_roster,
                                                reviewer_task_id,
                                                pr_num,
                                                &rework_msg,
                                            )
                                            .await;
                                            if !spawn_ok {
                                                log(&format!(
                                                    "remediation worker spawn failed for task \
                                                     #{reviewer_task_id} — firing AgentFailed"
                                                ));
                                                fire_event(
                                                    &db_path,
                                                    "daemon",
                                                    reviewer_task_id,
                                                    &Event::AgentFailed {
                                                        reason: "remediation worker spawn failed"
                                                            .into(),
                                                    },
                                                )
                                                .await;
                                            }
                                        } else {
                                            log(&format!(
                                                "no PR or draining — cannot spawn remediation \
                                                 for task #{reviewer_task_id}"
                                            ));
                                            fire_event(
                                                &db_path,
                                                "daemon",
                                                reviewer_task_id,
                                                &Event::AgentFailed {
                                                    reason: "no worker and no PR for rework".into(),
                                                },
                                            )
                                            .await;
                                        }
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
                                        cleanup_slot(
                                            config,
                                            wt_mgr,
                                            name_pool,
                                            w,
                                            None,
                                            "rework_cap",
                                        )
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
                            if !consume_mailbox_row(&db_path, *id).await {
                                break;
                            }
                            continue;
                        }
                        merge::ChecksOutcome::TimedOut => {
                            // Only drain-interrupted TimedOut reaches here
                            // (non-drain converted to Pending above). Leave
                            // the mailbox row unconsumed and the task in
                            // "merging" state — the outer loop will handle
                            // drain shutdown, and adoption recovery on
                            // restart will re-process the approval.
                            log(&format!(
                                "drain interrupted merge-checks for PR #{pr_num} \
                                 — preserving state for restart recovery"
                            ));
                            return Ok(());
                        }
                        merge::ChecksOutcome::Pending { reason } => {
                            // Recheck mergeability — PR may have become
                            // conflicting during the checks wait.
                            let post_timeout_mergeability = {
                                let repo = config.repo_dir.clone();
                                let executor = Arc::clone(&config.merge_executor);
                                tokio::task::spawn_blocking(move || {
                                    executor.check_mergeability(pr_num, &repo)
                                })
                                .await
                                .map_err(|e| {
                                    QuorumError::Io(format!(
                                        "mergeability spawn_blocking join: {e}"
                                    ))
                                })?
                            };

                            if post_timeout_mergeability == merge::MergeabilityState::Conflicting {
                                // Conflict appeared during checks wait — fire
                                // MergeConflict (merging → rework directly).
                                log(&format!(
                                    "PR #{pr_num} became CONFLICTING during checks \
                                     wait — firing MergeConflict"
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
                                             (detected after checks timeout).\n\n\
                                             Rebase on {}, resolve conflicts, \
                                             and push again.",
                                            config.base_branch, config.base_branch
                                        );
                                        if let Some(wi) = workers
                                            .iter()
                                            .position(|w| w.task_id == reviewer_task_id)
                                        {
                                            let rework_prompt = reviewer::build_rework_prompt(
                                                &workers[wi].agent_name,
                                                workers[wi].task_id,
                                                pr_num,
                                                &rework_msg,
                                                workers[wi].cost_usd,
                                                config.limits.max_task_cost_usd,
                                            );
                                            if let Err(e) = feed_worker_turn(
                                                &mut workers[wi],
                                                &rework_prompt,
                                                config,
                                            )
                                            .await
                                            {
                                                log(&format!(
                                                    "timeout-conflict rework feed failed: \
                                                     {e} — cleaning up"
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
                                                cleanup_slot(
                                                    config,
                                                    wt_mgr,
                                                    name_pool,
                                                    w,
                                                    None,
                                                    "agent_failed",
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
                                                        let mut conn = quorum_core::db::open(&p)?;
                                                        journal::upsert(&mut conn, &entry)
                                                    },
                                                )
                                                .await
                                                .ok();
                                                log(&format!(
                                                    "worker {} rework #{} \
                                                     (timeout + conflict)",
                                                    w.agent_name, w.rework_count
                                                ));
                                            }
                                        } else {
                                            // #175: no live worker — spawn remediation
                                            log(&format!(
                                                "no worker for rework on task #{reviewer_task_id} \
                                                 (timeout + conflict) — spawning remediation worker"
                                            ));
                                            if pr_num > 0 && !drain_state.draining {
                                                let spawn_ok = spawn_remediation_worker(
                                                    config,
                                                    wt_mgr,
                                                    name_pool,
                                                    workers,
                                                    lifetime_roster,
                                                    reviewer_task_id,
                                                    pr_num,
                                                    &rework_msg,
                                                )
                                                .await;
                                                if !spawn_ok {
                                                    log(&format!(
                                                        "remediation worker spawn failed for task \
                                                         #{reviewer_task_id} — firing AgentFailed"
                                                    ));
                                                    fire_event(
                                                        &db_path,
                                                        "daemon",
                                                        reviewer_task_id,
                                                        &Event::AgentFailed {
                                                            reason:
                                                                "remediation worker spawn failed"
                                                                    .into(),
                                                        },
                                                    )
                                                    .await;
                                                }
                                            } else {
                                                log(&format!(
                                                    "no PR or draining — cannot spawn remediation \
                                                     for task #{reviewer_task_id}"
                                                ));
                                                fire_event(
                                                    &db_path,
                                                    "daemon",
                                                    reviewer_task_id,
                                                    &Event::AgentFailed {
                                                        reason: "no worker and no PR for rework"
                                                            .into(),
                                                    },
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                    Some(_) => {
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
                                            cleanup_slot(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                w,
                                                None,
                                                "rework_cap",
                                            )
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
                                if !consume_mailbox_row(&db_path, *id).await {
                                    break;
                                }
                                continue;
                            }

                            // #174: Durable merge-wait. Checks are still
                            // running — leave the mailbox row unconsumed so
                            // the next tick retries. No approval write here:
                            // R1/R2 approvals from normal flow are already
                            // SHA-bound to the reviewed diff; writing here
                            // would re-bind to current head (force-push drift)
                            // and let approval-recovery merge without CI.
                            log(&format!("merge wait: {reason} — retrying next tick"));
                            break;
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
                                let role = if reviewers[ri].r2_origin { "r2" } else { "r1" };
                                let record = quorum_core::approvals::Approval {
                                    pr_number: pr_num,
                                    review_role: role.to_string(),
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
                            if signal_count.load(std::sync::atomic::Ordering::SeqCst) >= 1 {
                                log("master-ci gate: drain signal — returning to outer loop");
                                return Ok(());
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(poll)).await;
                        }
                    }

                    // Recheck drain after master CI gate (may have been signaled)
                    if signal_count.load(std::sync::atomic::Ordering::SeqCst) >= 1 {
                        log("drain detected after master-ci gate — returning to outer loop");
                        return Ok(());
                    }

                    // #153: final mergeability recheck immediately before merge.
                    // The window between the initial check and here includes the
                    // checks wait + approval persistence + master-ci gate — base
                    // can advance and create conflicts during any of those.
                    {
                        let pre_merge_state = {
                            let repo = config.repo_dir.clone();
                            let executor = Arc::clone(&config.merge_executor);
                            tokio::task::spawn_blocking(move || {
                                executor.check_mergeability(pr_num, &repo)
                            })
                            .await
                            .map_err(|e| {
                                QuorumError::Io(format!(
                                    "pre-merge mergeability spawn_blocking join: {e}"
                                ))
                            })?
                        };
                        if pre_merge_state == merge::MergeabilityState::Conflicting {
                            log(&format!(
                                "PR #{pr_num} is CONFLICTING at merge time \
                                 — firing MergeConflict"
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
                                         (detected at merge time).\n\n\
                                         Rebase on {}, resolve conflicts, \
                                         and push again.",
                                        config.base_branch, config.base_branch
                                    );
                                    if let Some(wi) =
                                        workers.iter().position(|w| w.task_id == reviewer_task_id)
                                    {
                                        let rework_prompt = reviewer::build_rework_prompt(
                                            &workers[wi].agent_name,
                                            workers[wi].task_id,
                                            pr_num,
                                            &rework_msg,
                                            workers[wi].cost_usd,
                                            config.limits.max_task_cost_usd,
                                        );
                                        if let Err(e) = feed_worker_turn(
                                            &mut workers[wi],
                                            &rework_prompt,
                                            config,
                                        )
                                        .await
                                        {
                                            log(&format!(
                                                "pre-merge conflict rework feed failed: \
                                                 {e} — cleaning up"
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
                                            cleanup_slot(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                w,
                                                None,
                                                "agent_failed",
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
                                            let entry = slot_journal_entry(w, "worker", "working");
                                            tokio::task::spawn_blocking(move || -> Result<()> {
                                                let mut conn = quorum_core::db::open(&p)?;
                                                journal::upsert(&mut conn, &entry)
                                            })
                                            .await
                                            .ok();
                                            log(&format!(
                                                "worker {} rework #{} \
                                                 (pre-merge conflict recheck)",
                                                w.agent_name, w.rework_count
                                            ));
                                        }
                                    } else {
                                        // #175: no live worker — spawn remediation
                                        log(&format!(
                                            "no worker for rework on task #{reviewer_task_id} \
                                             (pre-merge conflict) — spawning remediation worker"
                                        ));
                                        if pr_num > 0 && !drain_state.draining {
                                            let spawn_ok = spawn_remediation_worker(
                                                config,
                                                wt_mgr,
                                                name_pool,
                                                workers,
                                                lifetime_roster,
                                                reviewer_task_id,
                                                pr_num,
                                                &rework_msg,
                                            )
                                            .await;
                                            if !spawn_ok {
                                                log(&format!(
                                                    "remediation worker spawn failed for task \
                                                     #{reviewer_task_id} — firing AgentFailed"
                                                ));
                                                fire_event(
                                                    &db_path,
                                                    "daemon",
                                                    reviewer_task_id,
                                                    &Event::AgentFailed {
                                                        reason: "remediation worker spawn failed"
                                                            .into(),
                                                    },
                                                )
                                                .await;
                                            }
                                        } else {
                                            log(&format!(
                                                "no PR or draining — cannot spawn remediation \
                                                 for task #{reviewer_task_id}"
                                            ));
                                            fire_event(
                                                &db_path,
                                                "daemon",
                                                reviewer_task_id,
                                                &Event::AgentFailed {
                                                    reason: "no worker and no PR for rework".into(),
                                                },
                                            )
                                            .await;
                                        }
                                    }
                                }
                                Some(_) => {
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
                                        cleanup_slot(
                                            config,
                                            wt_mgr,
                                            name_pool,
                                            w,
                                            None,
                                            "rework_cap",
                                        )
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
                            if !consume_mailbox_row(&db_path, *id).await {
                                break;
                            }
                            continue;
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
                                let handle = tokio::task::spawn_blocking(move || {
                                    executor.wait_for_checks(pr_num, &repo, timeout, poll)
                                });
                                let drain_remaining = if drain_state.draining {
                                    Some(
                                        std::time::Duration::from_secs(config.drain_timeout_secs)
                                            .saturating_sub(
                                                drain_state.drain_started_at.unwrap().elapsed(),
                                            ),
                                    )
                                } else {
                                    None
                                };
                                let sc = std::sync::Arc::clone(signal_count);
                                tokio::select! {
                                    result = handle => {
                                        result.map_err(|e| {
                                            QuorumError::Io(format!("checks spawn_blocking join: {e}"))
                                        })?
                                    }
                                    _ = drain_interrupt(sc, drain_remaining) => {
                                        log("policy-retry wait_for_checks interrupted: drain deadline");
                                        return Ok(());
                                    }
                                }
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
                                | merge::ChecksOutcome::TimedOut
                                | merge::ChecksOutcome::Pending { .. } => {
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
                        // #125 fires the collector immediately (best-effort).
                        // #127 also durably enqueues so the tick loop retries
                        // with backoff and cap; a successful run deletes the
                        // job.
                        spawn_post_merge_collector(config, pr_num, reviewer_task_id);
                        enqueue_interpret_job(&db_path, pr_num, reviewer_task_id, &config.repo)
                            .await;
                        if config.self_update_drain && config.self_repo.is_some() {
                            let sha = format!("post-merge-pr-{pr_num}");
                            drain_state.start_drain(&sha);
                        }

                        let r = reviewers.remove(ri);
                        teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved").await;
                        if let Some(wi) = workers.iter().position(|w| w.task_id == reviewer_task_id)
                        {
                            let w = workers.remove(wi);
                            cleanup_slot(config, wt_mgr, name_pool, w, None, "merged").await;
                        }
                        {
                            let p = db_path.clone();
                            let tid = reviewer_task_id;
                            tokio::task::spawn_blocking(move || {
                                if let Ok(mut conn) = quorum_core::db::open(&p) {
                                    let _ = quorum_core::provision_attempts::clear_for_task(
                                        &mut conn, tid,
                                    );
                                }
                            })
                            .await
                            .ok();
                        }
                    } else {
                        let failure_kind = merge_result
                            .failure_kind
                            .unwrap_or(merge::MergeFailureKind::PolicyBlocked);

                        match failure_kind {
                            merge::MergeFailureKind::PolicyBlocked
                            | merge::MergeFailureKind::PolicyPending => {
                                log(&format!(
                                    "MERGE BLOCKED: PR #{pr_num} merge failed \
                                     (not worker-fixable): {} — parking task",
                                    merge_result.message
                                ));
                                park_task(
                                    &db_path,
                                    reviewer_task_id,
                                    &format!(
                                        "merge policy blocked PR #{pr_num}: {}",
                                        merge_result.message
                                    ),
                                    "merging",
                                )
                                .await;
                                let r = reviewers.remove(ri);
                                teardown_reviewer(config, wt_mgr, name_pool, r, "verdict:approved")
                                    .await;
                                if let Some(wi) =
                                    workers.iter().position(|w| w.task_id == reviewer_task_id)
                                {
                                    let w = workers.remove(wi);
                                    cleanup_slot(config, wt_mgr, name_pool, w, None, "parked")
                                        .await;
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
                                                let rework_prompt = reviewer::build_rework_prompt(
                                                    &workers[wi].agent_name,
                                                    workers[wi].task_id,
                                                    pr_num,
                                                    &rework_msg,
                                                    workers[wi].cost_usd,
                                                    config.limits.max_task_cost_usd,
                                                );
                                                if let Err(e) = feed_worker_turn(
                                                    &mut workers[wi],
                                                    &rework_prompt,
                                                    config,
                                                )
                                                .await
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
                                                        config,
                                                        wt_mgr,
                                                        name_pool,
                                                        w,
                                                        None,
                                                        "agent_failed",
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
                                                // #175: no live worker — spawn remediation
                                                log(&format!(
                                                    "no worker for rework on task #{reviewer_task_id} \
                                                     (merge failure) — spawning remediation worker"
                                                ));
                                                if pr_num > 0 && !drain_state.draining {
                                                    let spawn_ok = spawn_remediation_worker(
                                                        config,
                                                        wt_mgr,
                                                        name_pool,
                                                        workers,
                                                        lifetime_roster,
                                                        reviewer_task_id,
                                                        pr_num,
                                                        &rework_msg,
                                                    )
                                                    .await;
                                                    if !spawn_ok {
                                                        log(&format!(
                                                            "remediation worker spawn failed for task \
                                                             #{reviewer_task_id} — firing AgentFailed"
                                                        ));
                                                        fire_event(
                                                            &db_path,
                                                            "daemon",
                                                            reviewer_task_id,
                                                            &Event::AgentFailed {
                                                                reason: "remediation worker spawn failed".into(),
                                                            },
                                                        )
                                                        .await;
                                                    }
                                                } else {
                                                    log(&format!(
                                                        "no PR or draining — cannot spawn remediation \
                                                         for task #{reviewer_task_id}"
                                                    ));
                                                    fire_event(
                                                        &db_path,
                                                        "daemon",
                                                        reviewer_task_id,
                                                        &Event::AgentFailed {
                                                            reason:
                                                                "no worker and no PR for rework"
                                                                    .into(),
                                                        },
                                                    )
                                                    .await;
                                                }
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
                                                cleanup_slot(
                                                    config,
                                                    wt_mgr,
                                                    name_pool,
                                                    w,
                                                    None,
                                                    "rework_cap",
                                                )
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

                    // R2 audit: record completed R2 review.
                    if reviewers[ri].r2_origin {
                        record_r2_audit(
                            config,
                            &reviewers[ri].agent_name,
                            reviewers[ri].agent_run_id,
                            reviewers[ri].task_id,
                            row.pr,
                            Some("changes"),
                        )
                        .await;
                    }

                    // Task #124: the daemon no longer mirrors the reviewer's
                    // changes verdict into a duplicate generic GitHub
                    // REQUEST_CHANGES review. Reviewer agents own their own
                    // GitHub review interactions (inline comments, review
                    // summaries, `gh pr review --request-changes`) — the PR is
                    // the source of truth for findings + author pushback.
                    // The submit feedback is still fed to the warm worker as
                    // rework-turn context below.

                    // #90/#159: record the changes verdict in approvals (mirrors approved path).
                    if let Some(pr_num) = row.pr {
                        let reviewer_name = reviewers[ri].agent_name.clone();
                        let author = workers
                            .iter()
                            .find(|w| w.task_id == reviewer_task_id)
                            .map(|w| w.agent_name.clone())
                            .unwrap_or_default();
                        let role = if reviewers[ri].r2_origin { "r2" } else { "r1" };
                        let blocking = gated.blocking_count.unwrap_or(0) as i64;
                        let p = db_path.clone();
                        let record = quorum_core::approvals::Approval {
                            pr_number: pr_num,
                            review_role: role.to_string(),
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

                        // #159: on changes verdict, invalidate both approvals
                        // (rework will produce a new head, staling any prior approval).
                        let p = db_path.clone();
                        tokio::task::spawn_blocking(move || -> Result<()> {
                            let mut conn = quorum_core::db::open(&p)?;
                            quorum_core::approvals::delete(&mut conn, pr_num)?;
                            Ok(())
                        })
                        .await
                        .ok();
                    }

                    // #159: verify GitHub has a REQUEST_CHANGES review from this
                    // reviewer. The reviewer is encouraged to post one, but the
                    // daemon verifies and backstops.
                    if let Some(pr_num) = row.pr {
                        let repo = config.repo_dir.clone();
                        let executor = Arc::clone(&config.merge_executor);
                        let feedback_for_gh = feedback_owned.clone();
                        tokio::task::spawn_blocking(move || {
                            executor.ensure_changes_requested(pr_num, &repo, &feedback_for_gh);
                        })
                        .await
                        .ok();
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
                                let rework_prompt = reviewer::build_rework_prompt(
                                    &workers[wi].agent_name,
                                    workers[wi].task_id,
                                    rework_pr,
                                    feedback,
                                    workers[wi].cost_usd,
                                    config.limits.max_task_cost_usd,
                                );
                                if let Err(e) =
                                    feed_worker_turn(&mut workers[wi], &rework_prompt, config).await
                                {
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
                                    cleanup_slot(
                                        config,
                                        wt_mgr,
                                        name_pool,
                                        w,
                                        None,
                                        "agent_failed",
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
                                // #159: no live worker — spawn a remediation
                                // worker with the existing PR and blocking
                                // findings instead of failing the task.
                                log(&format!(
                                    "no worker for rework on task #{reviewer_task_id} — \
                                     spawning remediation worker"
                                ));
                                let rework_pr = row.pr.unwrap_or(0);
                                if rework_pr > 0 && !drain_state.draining {
                                    let spawn_ok = spawn_remediation_worker(
                                        config,
                                        wt_mgr,
                                        name_pool,
                                        workers,
                                        lifetime_roster,
                                        reviewer_task_id,
                                        rework_pr,
                                        feedback,
                                    )
                                    .await;
                                    if !spawn_ok {
                                        log(&format!(
                                            "remediation worker spawn failed for task \
                                             #{reviewer_task_id} — firing AgentFailed"
                                        ));
                                        fire_event(
                                            &db_path,
                                            "daemon",
                                            reviewer_task_id,
                                            &Event::AgentFailed {
                                                reason: "remediation worker spawn failed".into(),
                                            },
                                        )
                                        .await;
                                    }
                                } else {
                                    log(&format!(
                                        "no PR or draining — cannot spawn remediation \
                                         for task #{reviewer_task_id}"
                                    ));
                                    fire_event(
                                        &db_path,
                                        "daemon",
                                        reviewer_task_id,
                                        &Event::AgentFailed {
                                            reason: "no worker and no PR for rework".into(),
                                        },
                                    )
                                    .await;
                                }
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
                                cleanup_slot(config, wt_mgr, name_pool, w, None, "rework_cap")
                                    .await;
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

            // #101: resolve PR — prefer explicit row.pr, fall back to
            // task refs so a done-without-`--pr` doesn't silently close
            // a task whose refs already carry a PR number.
            let effective_pr = if row.pr.is_some() {
                Ok(row.pr)
            } else {
                let p = db_path.clone();
                let tid = workers[wi].task_id;
                let result = tokio::task::spawn_blocking(move || -> Result<Option<i64>> {
                    let conn = quorum_core::db::open(&p)?;
                    let task = tasks::get(&conn, tid)?;
                    Ok(task.and_then(|t| tasks::extract_pr_number(&t.refs)))
                })
                .await;
                match result {
                    Ok(Ok(pr)) => {
                        if let Some(n) = pr {
                            log(&format!(
                                "BACKFILL: worker {} done without --pr but task #{} refs \
                                 carry pr#{} — routing to review flow",
                                workers[wi].agent_name, workers[wi].task_id, n
                            ));
                        }
                        Ok(pr)
                    }
                    Ok(Err(e)) => Err(format!("DB error loading refs for task #{tid}: {e}")),
                    Err(e) => Err(format!("spawn_blocking join error for task #{tid}: {e}")),
                }
            };

            // DB error during refs lookup — log loudly, leave mailbox row
            // unconsumed so next tick retries. Never fall through to direct-close.
            let effective_pr = match effective_pr {
                Ok(pr) => pr,
                Err(msg) => {
                    log(&format!(
                        "ERROR: refs backfill lookup failed for worker {} — \
                         skipping done processing (will retry): {msg}",
                        workers[wi].agent_name
                    ));
                    break;
                }
            };

            if let Some(pr) = effective_pr {
                // Fire the appropriate lifecycle event based on whether
                // this is the first done signal or a rework-pushed.
                let event = worker_done_event(workers[wi].rework_count, pr);
                let tr = fire_event_result(
                    &db_path,
                    &workers[wi].agent_name,
                    workers[wi].task_id,
                    &event,
                )
                .await;

                match tr {
                    Ok(tr) => {
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
                                            &config.effort,
                                        );
                                        if let Err(e) =
                                            reviewers[ri].proc.feed_turn(&rereview_turn).await
                                        {
                                            let reason = if reviewers[ri].proc.is_codex() {
                                                "codex_rereview"
                                            } else {
                                                "failed"
                                            };
                                            log(&format!(
                                                "ResumeReviewer: feed_turn failed \
                                                 for task #{}: {e} — tearing down ({reason})",
                                                workers[wi].task_id
                                            ));
                                            let r = reviewers.remove(ri);
                                            teardown_reviewer(config, wt_mgr, name_pool, r, reason)
                                                .await;
                                        } else {
                                            log(&format!(
                                                "ResumeReviewer: fed re-review turn \
                                                 to {} for task #{}",
                                                reviewers[ri].agent_name, workers[wi].task_id
                                            ));
                                            reviewers[ri].turn_started_at =
                                                std::time::Instant::now();
                                            // Update reviewed head SHA for stale
                                            // approval detection on re-review.
                                            if reviewers[ri].r2_origin {
                                                let repo = config.repo_dir.clone();
                                                let executor = Arc::clone(&config.merge_executor);
                                                reviewers[ri].reviewed_head_sha =
                                                    tokio::task::spawn_blocking(move || {
                                                        executor.head_sha(pr, &repo)
                                                    })
                                                    .await
                                                    .ok()
                                                    .flatten();
                                            }
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
                    Err(rejection_cause) => {
                        // C3: transition rejected (e.g. task externally
                        // cancelled, authority mismatch). Routes through the
                        // recovery budget as a deterministic failure — branch
                        // preserved for diagnosis.
                        log(&format!(
                            "lifecycle rejected at done signal for worker {} \
                             — deterministic failure, branch preserved: {rejection_cause}",
                            workers[wi].agent_name
                        ));
                        let w = workers.remove(wi);
                        fire_event(
                            &db_path,
                            &w.agent_name,
                            w.task_id,
                            &Event::AgentFailed {
                                reason: format!(
                                    "lifecycle transition rejected at done signal: \
                                     {rejection_cause}"
                                ),
                            },
                        )
                        .await;
                        cleanup_slot_inner(
                            config,
                            wt_mgr,
                            name_pool,
                            w,
                            None,
                            false,
                            "agent_failed",
                        )
                        .await;
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
                cleanup_slot(config, wt_mgr, name_pool, w, Some("done"), "done").await;
            }

            if !consume_mailbox_row(&db_path, *id).await {
                break;
            }
            break;
        }

        // F9: Done row matches neither worker nor reviewer.
        // #130: no passive agent handling — all unmatched Done rows are
        // consumed as phantoms regardless of roster ownership.
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
        cleanup_slot(config, wt_mgr, name_pool, dead, None, "crashed").await;
    }

    // ── Phase 4-idle: Kill workers idle too long between turns ─────────
    // A worker that completed a turn (draining=false) but never gets a new
    // turn is a zombie — e.g. it asked a question in dontAsk mode (#74).
    // Phase-aware: workers whose task is already in-review or merging are
    // legitimately idle (awaiting review/merge) — release gracefully without
    // firing AgentFailed (#176).
    let mut idle_workers: Vec<usize> = Vec::new();
    for (i, w) in workers.iter().enumerate() {
        if w.draining || w.error_turn_count > 0 {
            continue;
        }
        if let Some(ended) = w.turn_ended_at {
            if ended.elapsed().as_secs() > idle_timeout {
                idle_workers.push(i);
            }
        }
    }
    for &i in idle_workers.iter().rev() {
        let task_id = workers[i].task_id;
        let idle_secs = workers[i].turn_ended_at.unwrap().elapsed().as_secs();

        // Look up current task status to distinguish legitimate waits from zombies.
        let p = db_path.clone();
        let task_status: Option<String> = tokio::task::spawn_blocking(move || -> Option<String> {
            let conn = quorum_core::db::open(&p).ok()?;
            let task = tasks::get(&conn, task_id).ok()??;
            Some(task.status.clone())
        })
        .await
        .ok()
        .flatten();

        let dead = workers.remove(i);

        match task_status.as_deref() {
            Some("in-review") | Some("merging") => {
                // Worker submitted its work and is awaiting review/merge.
                // Release gracefully — no AgentFailed, no task state change.
                let end_reason = match task_status.as_deref() {
                    Some("in-review") => "submitted",
                    Some("merging") => "awaiting_merge",
                    _ => unreachable!(),
                };
                log(&format!(
                    "worker {} idle {}s on task #{} (status: {}) — releasing submitted slot",
                    dead.agent_name,
                    idle_secs,
                    dead.task_id,
                    task_status.as_deref().unwrap_or("unknown"),
                ));
                cleanup_slot(config, wt_mgr, name_pool, dead, None, end_reason).await;
            }
            _ => {
                // Genuine zombie — fire AgentFailed.
                log(&format!(
                    "WATCHDOG: worker {} idle {}s on task #{} (limit {}s) — killing zombie",
                    dead.agent_name, idle_secs, dead.task_id, idle_timeout
                ));
                fire_event(
                    &db_path,
                    &dead.agent_name,
                    dead.task_id,
                    &Event::AgentFailed {
                        reason: format!(
                            "worker idle {}s between turns (limit {}s) — zombie reaped",
                            idle_secs, idle_timeout
                        ),
                    },
                )
                .await;
                cleanup_slot(config, wt_mgr, name_pool, dead, None, "idle_reaped").await;
            }
        }
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
        let raw_prompt = format!(
            "Your previous turn was interrupted by a transport/API error (attempt {}/{}). \
             Verify any partial state from your last turn, then continue your task.",
            w.error_turn_count, MAX_ERROR_RETRIES
        );
        match feed_worker_turn(w, &raw_prompt, config).await {
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
        if dead.proc.is_codex() {
            let reason = dead
                .last_error_text
                .as_deref()
                .unwrap_or("Codex provider/protocol failure")
                .to_string();
            log(&format!(
                "parking task #{} after bounded Codex failure: {reason}",
                dead.task_id
            ));
            if let Err(error) = persist_codex_provider_block(
                &db_path,
                dead.task_id,
                &reason,
                &CodexRetryTurn {
                    model: dead.model.clone(),
                    effort: dead.effort.clone(),
                    prompt: dead.pending_prompt.clone(),
                    turn_kind: dead.pending_turn_kind.clone(),
                    thread_id: dead.codex_thread_id.clone(),
                },
            )
            .await
            {
                log(&format!(
                    "FATAL: cannot durably park task #{}; retaining slot: {error}",
                    dead.task_id
                ));
                workers.insert(i, dead);
                continue;
            }
            cleanup_slot(config, wt_mgr, name_pool, dead, None, "provider_blocked").await;
            continue;
        }
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
        cleanup_slot(config, wt_mgr, name_pool, dead, None, "error_retries").await;
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
            cleanup_slot(config, wt_mgr, name_pool, w, None, "drain").await;
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
        if dead.proc.is_codex() {
            let p = db_path.clone();
            let task_id = dead.task_id;
            let still_working = tokio::task::spawn_blocking(move || -> bool {
                quorum_core::db::open(&p)
                    .ok()
                    .and_then(|conn| tasks::get(&conn, task_id).ok().flatten())
                    .is_some_and(|task| task.status == "working")
            })
            .await
            .unwrap_or(false);
            if still_working {
                let reason = dead
                    .last_error_text
                    .as_deref()
                    .unwrap_or("Codex process exited before task submission")
                    .to_string();
                if let Err(error) = persist_codex_provider_block(
                    &db_path,
                    dead.task_id,
                    &reason,
                    &CodexRetryTurn {
                        model: dead.model.clone(),
                        effort: dead.effort.clone(),
                        prompt: dead.pending_prompt.clone(),
                        turn_kind: dead.pending_turn_kind.clone(),
                        thread_id: dead.codex_thread_id.clone(),
                    },
                )
                .await
                {
                    log(&format!(
                        "FATAL: cannot durably park task #{}; retaining slot: {error}",
                        dead.task_id
                    ));
                    workers.insert(i, dead);
                    continue;
                }
                cleanup_slot(config, wt_mgr, name_pool, dead, None, "provider_blocked").await;
                continue;
            }
        }
        let instant_death = dead.cost_tokens == 0;
        if instant_death {
            let strikes = poison_tracker.record_strike(dead.task_id);
            if strikes >= MAX_POISON_STRIKES {
                let task_id = dead.task_id;
                park_task(
                    &db_path,
                    task_id,
                    &format!("worker died {strikes} time(s) without producing output"),
                    "open",
                )
                .await;
                cleanup_slot(config, wt_mgr, name_pool, dead, None, "parked").await;
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
                cleanup_slot(config, wt_mgr, name_pool, dead, None, "crashed").await;
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
            cleanup_slot(config, wt_mgr, name_pool, dead, None, "crashed").await;
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
                    cleanup_slot(config, wt_mgr, name_pool, w, None, "external").await;
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
                let raw_prompt = format!("MESSAGE from {}: {payload}", msg_row.agent);
                match feed_worker_turn(&mut workers[wi], &raw_prompt, config).await {
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
                // No active worker for this target. Consume — single daemon
                // per DB (invariant 11), no sibling to defer to.
                log(&format!("consuming message to {target} (no active worker)"));
                consume_mailbox_row(&db_path, *msg_id).await;
            }
        }
    }

    // ── Phase 4d: Renew task leases for active workers (#130) ───────────
    // The daemon explicitly renews the exact lease for each active worker's
    // task. External writes (sync, post, etc.) no longer auto-renew leases.
    {
        let p = db_path.clone();
        let active_pairs: Vec<(String, i64)> = workers
            .iter()
            .map(|w| (w.agent_name.clone(), w.task_id))
            .collect();
        if !active_pairs.is_empty() {
            tokio::task::spawn_blocking(move || {
                if let Ok(conn) = quorum_core::db::open(&p) {
                    let now = now_unix();
                    for (agent, task_id) in &active_pairs {
                        let _ = quorum_core::agents::renew_task_lease(&conn, agent, *task_id, now);
                    }
                }
            })
            .await
            .ok();
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
            // #190: determine which role is needed and check durable exhaustion.
            let head_sha = {
                let repo = config.repo_dir.clone();
                let executor = Arc::clone(&config.merge_executor);
                let pr_num = *pr;
                tokio::task::spawn_blocking(move || executor.head_sha(pr_num, &repo))
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_default()
            };
            let provision_decision = {
                let p = db_path.clone();
                let tid = *task_id;
                let pr_num = *pr;
                let sha = head_sha.clone();
                tokio::task::spawn_blocking(move || -> Result<Option<&'static str>> {
                    let conn = quorum_core::db::open(&p)?;
                    if is_reviewer_cap_exceeded(&conn, tid)? {
                        return Ok(None);
                    }
                    let role = next_needed_role(&conn, pr_num, &sha)?;
                    if let Some(r) = role {
                        if is_provision_exhausted(&conn, tid, pr_num, r, &sha)? {
                            return Ok(None);
                        }
                    }
                    Ok(role)
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            };
            match provision_decision {
                Ok(None) => {
                    log(&format!(
                        "reviewer provision exhausted for task #{task_id} PR #{pr} \
                         — parking worker"
                    ));
                    parked_workers.push(*wi);
                    continue;
                }
                Ok(Some(role_str)) => {
                    let role = if role_str == "r2" {
                        // Look up R1 reviewer info from durable approval
                        let r1_info = {
                            let p = db_path.clone();
                            let pr_num = *pr;
                            tokio::task::spawn_blocking(move || -> Option<(String, Option<i64>)> {
                                let conn = quorum_core::db::open(&p).ok()?;
                                let a = quorum_core::approvals::get(&conn, pr_num, "r1").ok()??;
                                Some((a.reviewer, None))
                            })
                            .await
                            .ok()
                            .flatten()
                            .unwrap_or(("unknown".into(), None))
                        };
                        ReviewRole::R2 {
                            r1_reviewer: r1_info.0,
                            r1_run_id: r1_info.1,
                        }
                    } else {
                        ReviewRole::R1
                    };
                    let counterpart: ReviewCounterpart = (&workers[*wi]).into();
                    provision_reviewer(
                        config,
                        wt_mgr,
                        name_pool,
                        reviewers,
                        lifetime_roster,
                        *pr,
                        counterpart,
                        &role,
                        &head_sha,
                    )
                    .await?;
                }
                Err(e) => {
                    log(&format!(
                        "provision decision error for task #{task_id} PR #{pr}: {e}"
                    ));
                }
            }
        }
        for &wi in pr_closed_workers.iter().rev() {
            let w = workers.remove(wi);
            cleanup_slot(config, wt_mgr, name_pool, w, None, "pr_closed").await;
        }
        // #75: park repo-mismatch workers without burning strikes
        // Process in reverse index order to avoid index invalidation.
        repo_mismatch_workers.sort_by_key(|b| std::cmp::Reverse(b.0));
        for (wi, task_repo) in repo_mismatch_workers {
            let w = workers.remove(wi);
            let pr_label =
                w.pr.map(|n| format!("#{n}"))
                    .unwrap_or_else(|| "unknown".to_string());
            park_task(
                &db_path,
                w.task_id,
                &format!(
                    "repo mismatch: PR {pr_label} belongs to {task_repo}, not {}",
                    config.repo
                ),
                "in-review",
            )
            .await;
            notify_provision_failure(
                &db_path,
                w.task_id,
                &format!("repo mismatch ({task_repo} vs {})", config.repo),
                &pr_label,
            )
            .await;
            cleanup_slot(config, wt_mgr, name_pool, w, None, "parked").await;
        }
        for &wi in parked_workers.iter().rev() {
            let w = workers.remove(wi);
            let pr_label =
                w.pr.map(|n| format!("#{n}"))
                    .unwrap_or_else(|| "unknown".to_string());
            park_task(
                &db_path,
                w.task_id,
                &format!(
                    "reviewer provision exhausted for PR {pr_label} after \
                     {MAX_REVIEWER_PROVISION_STRIKES} attempts"
                ),
                "in-review",
            )
            .await;
            notify_provision_failure(
                &db_path,
                w.task_id,
                "reviewer provision exhausted",
                &pr_label,
            )
            .await;
            cleanup_slot(config, wt_mgr, name_pool, w, None, "parked").await;
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
                park_task(
                    &db_path,
                    *task_id,
                    &format!(
                        "repo mismatch: PR #{pr} belongs to {other_repo}, not {}",
                        config.repo
                    ),
                    "in-review",
                )
                .await;
                notify_provision_failure(
                    &db_path,
                    *task_id,
                    &format!("repo mismatch ({other_repo} vs {})", config.repo),
                    &format!("#{pr}"),
                )
                .await;
                continue;
            }
            // #190: determine which role is needed and check durable exhaustion.
            let head_sha = {
                let repo = config.repo_dir.clone();
                let executor = Arc::clone(&config.merge_executor);
                let pr_num = *pr;
                tokio::task::spawn_blocking(move || executor.head_sha(pr_num, &repo))
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_default()
            };
            let provision_decision = {
                let p = db_path.clone();
                let tid = *task_id;
                let pr_num = *pr;
                let sha = head_sha.clone();
                tokio::task::spawn_blocking(move || -> Result<Option<&'static str>> {
                    let conn = quorum_core::db::open(&p)?;
                    if is_reviewer_cap_exceeded(&conn, tid)? {
                        return Ok(None);
                    }
                    let role = next_needed_role(&conn, pr_num, &sha)?;
                    if let Some(r) = role {
                        if is_provision_exhausted(&conn, tid, pr_num, r, &sha)? {
                            return Ok(None);
                        }
                    }
                    Ok(role)
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            };
            match provision_decision {
                Ok(None) => {
                    log(&format!(
                        "orphan in-review task #{task_id} PR #{pr}: \
                         provision exhausted — parking"
                    ));
                    park_task(
                        &db_path,
                        *task_id,
                        &format!("reviewer provision exhausted for orphan PR #{pr}"),
                        "in-review",
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
                Ok(Some(role_str)) => {
                    let role = if role_str == "r2" {
                        let r1_info = {
                            let p = db_path.clone();
                            let pr_num = *pr;
                            tokio::task::spawn_blocking(move || -> Option<(String, Option<i64>)> {
                                let conn = quorum_core::db::open(&p).ok()?;
                                let a = quorum_core::approvals::get(&conn, pr_num, "r1").ok()??;
                                Some((a.reviewer, None))
                            })
                            .await
                            .ok()
                            .flatten()
                            .unwrap_or(("unknown".into(), None))
                        };
                        ReviewRole::R2 {
                            r1_reviewer: r1_info.0,
                            r1_run_id: r1_info.1,
                        }
                    } else {
                        ReviewRole::R1
                    };
                    let branch = if let Some(b) =
                        orphan_worker_branch(author, *task_id, *review_only)
                    {
                        b
                    } else {
                        let pr_num = *pr;
                        let tid = *task_id;
                        let dbp = config.db_path.clone();
                        let repo_dir = config.repo_dir.clone();
                        let gh_repo = config.repo.clone();
                        let resolved = tokio::task::spawn_blocking(move || {
                            resolve_or_load_pr_target(pr_num, tid, &dbp, &repo_dir, Some(&gh_repo))
                        })
                        .await
                        .ok()
                        .flatten();
                        match resolved {
                            Some(target) => target.head_ref,
                            None => {
                                log(&format!(
                                    "orphan in-review task #{task_id} PR #{pr}: \
                                         could not resolve PR head ref — skipping"
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
                    provision_reviewer(
                        config,
                        wt_mgr,
                        name_pool,
                        reviewers,
                        lifetime_roster,
                        *pr,
                        counterpart,
                        &role,
                        &head_sha,
                    )
                    .await?;
                }
                Err(e) => {
                    log(&format!(
                        "orphan provision decision error for task #{task_id} PR #{pr}: {e}"
                    ));
                }
            }
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
                    *classifier_consec_errors = 0;
                    *classifier_backoff_until = None;
                    *classifier_slot = None;
                }
                classifier::ClassifierResult::Error(e) => {
                    if classifier::is_auth_error(&e) {
                        log(&format!(
                            "classifier: {e} — if using subscription auth, \
                             ensure no_bare_agent = true (the default)"
                        ));
                    } else {
                        log(&format!("classifier: {e}"));
                    }
                    *classifier_consec_errors = classifier_consec_errors.saturating_add(1);
                    let delay =
                        std::cmp::min(30 * (1u64 << (*classifier_consec_errors - 1).min(4)), 300);
                    *classifier_backoff_until =
                        Some(std::time::Instant::now() + std::time::Duration::from_secs(delay));
                    log(&format!(
                        "classifier: backoff {delay}s after {} consecutive error(s)",
                        *classifier_consec_errors
                    ));
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
                    *classifier_consec_errors = 0;
                    *classifier_backoff_until = None;
                } else {
                    log("classifier: process exited without parseable response");
                    *classifier_consec_errors = classifier_consec_errors.saturating_add(1);
                    let delay =
                        std::cmp::min(30 * (1u64 << (*classifier_consec_errors - 1).min(4)), 300);
                    *classifier_backoff_until =
                        Some(std::time::Instant::now() + std::time::Duration::from_secs(delay));
                }
            } else {
                log("classifier: process exited without response");
                *classifier_consec_errors = classifier_consec_errors.saturating_add(1);
                let delay =
                    std::cmp::min(30 * (1u64 << (*classifier_consec_errors - 1).min(4)), 300);
                *classifier_backoff_until =
                    Some(std::time::Instant::now() + std::time::Duration::from_secs(delay));
                log(&format!(
                    "classifier: backoff {delay}s after {} consecutive error(s)",
                    *classifier_consec_errors
                ));
            }
            *classifier_slot = None;
        }
    }

    // 7b: Spawn classifier if idle and there are unclassified tasks.
    // Skip if in backoff from consecutive errors.
    if let Some(until) = *classifier_backoff_until {
        if std::time::Instant::now() < until {
            // Still in backoff — skip spawn this tick.
        } else {
            *classifier_backoff_until = None;
        }
    }
    if classifier_slot.is_none() && !drain_state.draining && classifier_backoff_until.is_none() {
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

    // ── Phase 7.5: Post-merge collector retry queue (#127) ─────────────
    // Complements #125's fire-and-forget `spawn_post_merge_collector`. Every
    // merged PR is durably enqueued at merge time. Here we (a) sweep jobs
    // whose collector run has since recorded a success at the current version
    // (converged — delete the row), (b) spawn one retry for the highest-
    // priority ready job (attempts asc, past backoff, under cap), and (c)
    // log dead-lettered rows so operators see poison PRs instead of silent
    // starvation. Never touches task lifecycle: analytics-only, retry-only.
    if !drain_state.draining {
        let p = db_path.clone();
        let now_ts = now_unix();
        let outcome = tokio::task::spawn_blocking(move || -> Result<InterpretTickOutcome> {
            let mut conn = quorum_core::db::open(&p)?;
            let version = collector::COLLECTOR_VERSION;
            // 7.5a: converge — delete any job whose successful run at
            // the current version has landed since we last swept.
            let mut converged = 0usize;
            for job in quorum_core::review_interpret_jobs::list_all(&conn)? {
                if let Some(run) = quorum_core::review_findings::get_run(&conn, job.pr_number)? {
                    if matches!(run.status, quorum_core::review_findings::RunStatus::Success)
                        && run.collector_version == version
                    {
                        quorum_core::review_interpret_jobs::delete(&mut conn, job.pr_number)?;
                        converged += 1;
                    }
                }
            }
            // 7.5b: pick one ready job. `list_ready` orders by
            // (attempts asc, oldest last_attempt_at asc), so a fresh
            // job always beats a retry and a poison job cannot starve
            // later work. `list_ready` also excludes rows at MAX_ATTEMPTS
            // and applies the initial grace + per-attempt backoff (see
            // review_interpret_jobs::list_ready).
            let ready = quorum_core::review_interpret_jobs::list_ready(&conn, now_ts)?;
            let next = ready.into_iter().next();
            // If we're about to spawn, reserve the attempt now so that
            // this same job cannot be re-picked next tick if the spawn
            // races ahead of its run-record write. Reserving = mark_error
            // with a placeholder — the run-record itself (success or
            // failed) is the ground truth; the queue row's last_error
            // is only informational. The returned attempt count lets us
            // log a one-time DEAD-LETTER line at the exact tick a row
            // crosses the cap (dead-lettered rows are then excluded from
            // future list_ready calls, so the log never fires again for
            // the same PR).
            let mut just_dead_lettered = None;
            if let Some(job) = &next {
                let attempts = quorum_core::review_interpret_jobs::mark_error(
                    &mut conn,
                    job.pr_number,
                    "retry-spawn-in-flight",
                )?;
                if attempts >= quorum_core::review_interpret_jobs::MAX_ATTEMPTS {
                    just_dead_lettered = Some((job.pr_number, job.task_id, attempts));
                }
            }
            Ok(InterpretTickOutcome {
                converged,
                next,
                just_dead_lettered,
            })
        })
        .await
        .map_err(|e| QuorumError::Io(format!("interpret sweep join: {e}")))??;

        if outcome.converged > 0 {
            log(&format!(
                "interpret: {} job(s) converged (successful run at {})",
                outcome.converged,
                collector::COLLECTOR_VERSION
            ));
        }
        if let Some((pr, task_id, attempts)) = outcome.just_dead_lettered {
            log(&format!(
                "interpret: DEAD-LETTER PR #{pr} (task #{task_id}) — \
                 {attempts} failed attempts, giving up (row preserved for \
                 operator visibility)"
            ));
        }
        if let Some(job) = outcome.next {
            log(&format!(
                "interpret: retrying PR #{} (task #{}, attempt {})",
                job.pr_number,
                job.task_id,
                job.attempts + 1
            ));
            // Reuse #125's collector spawn: it writes review_collection_runs
            // on both success and failure, so the next tick's converge step
            // catches successes and the mark_error above blocks re-picking
            // until the backoff window elapses.
            let request = collector::CollectionRequest::new(
                job.pr_number,
                Some(job.task_id),
                job.repo.clone().or_else(|| {
                    if config.repo.is_empty() {
                        None
                    } else {
                        Some(config.repo.clone())
                    }
                }),
                config.db_path.clone(),
                config.repo_dir.clone(),
                config.agent_bin.clone(),
                config.bare_agent,
            );
            collector::spawn_detached(request);
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

/// Persist a Codex thread_id to task refs for continuation across restart/rework.
/// Feed a worker turn (rework, error-retry, or message). Claude uses stdin
/// feed_turn; Codex kills the exited process and spawns a new one with
/// thread-based continuation.
async fn feed_worker_turn(
    slot: &mut SlotState,
    raw_prompt: &str,
    config: &ServeConfig,
) -> std::io::Result<()> {
    if slot.proc.is_codex() {
        if should_replace_pending_prompt(raw_prompt) {
            slot.pending_prompt = raw_prompt.to_string();
            slot.pending_turn_kind = if slot.pr.is_some() {
                "rework".into()
            } else {
                "continuation".into()
            };
        }
        let (model, effort) = codex_continuation_identity(slot)?;
        let mut env_vars: Vec<(String, String)> = vec![
            ("QUORUM_REPO".into(), config.repo.clone()),
            ("QUORUM_AGENT".into(), slot.agent_name.clone()),
        ];
        if let Some(ref rid) = slot.cap_run_id {
            env_vars.push(("QUORUM_RUN_ID".into(), rid.clone()));
        }

        let new_proc = if let Some(ref tid) = slot.codex_thread_id {
            codex_agent::CodexProc::spawn_resume(
                tid,
                model,
                effort,
                &config.codex_sandbox,
                &slot.worktree_path,
                raw_prompt,
                &env_vars,
                config.agent_bin.as_deref(),
            )?
        } else {
            codex_agent::CodexProc::spawn(
                &codex_agent::CodexSpec {
                    model: model.to_string(),
                    effort: effort.to_string(),
                    sandbox: config.codex_sandbox.clone(),
                    worktree: slot.worktree_path.clone(),
                    prompt: raw_prompt.to_string(),
                    env_vars,
                },
                config.agent_bin.as_deref(),
            )?
        };

        let old = std::mem::replace(&mut slot.proc, runner::RunnerProc::Codex(new_proc));
        tokio::spawn(async move { old.kill_and_reap().await });
        Ok(())
    } else {
        let turn = agent::user_turn(raw_prompt);
        slot.proc.feed_turn(&turn).await
    }
}

fn should_replace_pending_prompt(prompt: &str) -> bool {
    !prompt.starts_with("Your previous turn was interrupted by a transport/API error")
}

fn codex_continuation_identity(slot: &SlotState) -> std::io::Result<(&str, &str)> {
    if slot.model.is_empty() || slot.effort.is_empty() {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Codex continuation missing its resolved model/effort identity",
        ))
    } else {
        Ok((&slot.model, &slot.effort))
    }
}

async fn persist_codex_thread_id(db_path: &std::path::Path, task_id: i64, thread_id: &str) {
    let p = db_path.to_path_buf();
    let tid = thread_id.to_string();
    let task_id_val = task_id;
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        let task = tasks::get(&conn, task_id_val)?;
        if let Some(task) = task {
            let mut refs: serde_json::Value = task
                .refs
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(serde_json::json!({}));
            refs["codex_thread_id"] = serde_json::Value::String(tid);
            let refs_str = refs.to_string();
            tasks::update_refs_daemon(&mut conn, task_id_val, &refs_str, now_unix())?;
        }
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => log(&format!(
            "persist_codex_thread_id failed for task #{task_id}: {e}"
        )),
        Err(e) => log(&format!(
            "persist_codex_thread_id join error for task #{task_id}: {e}"
        )),
    }
}

async fn persist_codex_provider_block(
    db_path: &std::path::Path,
    task_id: i64,
    reason: &str,
    retry: &CodexRetryTurn,
) -> Result<()> {
    let p = db_path.to_path_buf();
    let reason = reason.to_string();
    let retry = retry.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        if let Some(task) = tasks::get(&conn, task_id)? {
            let mut refs: serde_json::Value = task
                .refs
                .as_deref()
                .and_then(|refs| serde_json::from_str(refs).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            refs["codex_provider_blocked"] = serde_json::Value::Bool(true);
            refs["codex_provider_error"] = serde_json::Value::String(reason);
            refs["codex_retry_model"] = serde_json::Value::String(retry.model);
            refs["codex_retry_effort"] = serde_json::Value::String(retry.effort);
            refs["codex_retry_prompt"] = serde_json::Value::String(retry.prompt);
            refs["codex_retry_turn_kind"] = serde_json::Value::String(retry.turn_kind);
            match retry.thread_id {
                Some(thread_id) => {
                    refs["codex_retry_thread_id"] = serde_json::Value::String(thread_id);
                }
                None => {
                    if let Some(object) = refs.as_object_mut() {
                        object.remove("codex_retry_thread_id");
                    }
                }
            }
            tasks::update_refs_daemon(&mut conn, task_id, &refs.to_string(), now_unix())?;
        }
        Ok(())
    })
    .await;
    result.map_err(|error| QuorumError::Io(format!("provider-block join failed: {error}")))?
}

async fn codex_thread_id_from_refs(db_path: &std::path::Path, task_id: i64) -> Option<String> {
    let p = db_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Option<String> {
        let conn = quorum_core::db::open(&p).ok()?;
        let task = tasks::get(&conn, task_id).ok()??;
        let refs: serde_json::Value = task
            .refs
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())?;
        refs.get("codex_thread_id")
            .or_else(|| refs.get("codex_retry_thread_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    })
    .await
    .ok()
    .flatten()
}

/// Drain stream events from an agent slot (bounded per tick, 5s timeout).
/// Returns `Some(LimitBreached)` if a cost/time ceiling was hit.
///
/// Raw provider lines are preserved verbatim in stream.jsonl; the daemon
/// consumes only normalized `AgentEvent`s via the runner boundary.
async fn drain_events(
    slot: &mut SlotState,
    db_path: &std::path::Path,
    role: &str,
    limits: &CostLimits,
) -> Result<Option<LimitBreached>> {
    while let Ok(Some(raw_line)) =
        tokio::time::timeout(std::time::Duration::from_secs(5), slot.proc.next_raw_line()).await
    {
        let events = runner::normalize_line(slot.proc.kind(), &raw_line);

        if let Some(ref mut sl) = slot.session_log {
            sl.log_raw_and_normalized(&raw_line, &events);
        }

        for agent_event in &events {
            match agent_event {
                runner::AgentEvent::ThreadStarted { thread_id } => {
                    slot.codex_thread_id = Some(thread_id.clone());
                    persist_codex_thread_id(db_path, slot.task_id, thread_id).await;
                }
                runner::AgentEvent::TurnCompleted { usage, cost_usd } => {
                    let turn_tokens =
                        usage.map_or(0, |u| (u.input_tokens + u.output_tokens) as i64);
                    slot.cost_tokens += turn_tokens;
                    // cost_usd is session-cumulative (running total), not per-turn.
                    let prev_cost = slot.cost_usd;
                    if let Some(cost) = cost_usd {
                        slot.cost_usd = *cost;
                    }
                    let turn_cost_usd = cost_usd.map(|c| (c - prev_cost).max(0.0));
                    log(&format!(
                        "{role} {} result (turn_tokens={}, cumulative={}, cost_usd={:.4})",
                        slot.agent_name, turn_tokens, slot.cost_tokens, slot.cost_usd,
                    ));

                    let phase = if role == "worker" {
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
                    slot.error_turn_count = 0;
                    slot.last_error_text = None;

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

                runner::AgentEvent::TurnFailed {
                    message,
                    usage,
                    cost_usd,
                } => {
                    let turn_tokens =
                        usage.map_or(0, |u| (u.input_tokens + u.output_tokens) as i64);
                    slot.cost_tokens += turn_tokens;
                    let prev_cost = slot.cost_usd;
                    if let Some(cost) = cost_usd {
                        slot.cost_usd = *cost;
                    }
                    let turn_cost_usd = cost_usd.map(|c| (c - prev_cost).max(0.0));
                    log(&format!(
                        "{role} {} result (turn_tokens={}, cumulative={}, cost_usd={:.4}, ERROR)",
                        slot.agent_name, turn_tokens, slot.cost_tokens, slot.cost_usd,
                    ));

                    if let Some(ref mut sl) = slot.session_log {
                        sl.update_cost(slot.cost_tokens, slot.cost_usd);
                        sl.set_phase("working");
                    }

                    let p = db_path.to_path_buf();
                    let entry = slot_journal_entry(slot, role, "working");
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
                    slot.error_turn_count += 1;
                    let bounded: String = message.chars().take(120).collect();
                    slot.last_error_text = Some(bounded);
                    write_live_sidecar(slot);

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

                runner::AgentEvent::AssistantText { .. } => {
                    slot.live_stats.record_event();
                    write_live_sidecar(slot);
                }

                runner::AgentEvent::Activity { summary, .. } => {
                    slot.live_stats.tool_count += 1;
                    slot.live_stats.now_label = truncate_now_label(summary);
                    slot.live_stats.record_event();
                    write_live_sidecar(slot);
                }

                runner::AgentEvent::MidTurnUsage { tokens } => {
                    slot.live_stats.mid_turn_tokens = *tokens;
                    slot.live_stats.record_event();
                    write_live_sidecar(slot);
                }
            }
        }
    }
    Ok(None)
}

fn truncate_now_label(s: &str) -> String {
    if s.chars().count() <= 24 {
        s.to_string()
    } else {
        let t: String = s.chars().take(23).collect();
        format!("{t}…")
    }
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
            error_count: slot.error_turn_count,
            error_text: slot.last_error_text.clone(),
        };
        if let Ok(json) = serde_json::to_string(&stats) {
            let _ = std::fs::write(&path, json);
        }
    }
}

/// A minimal view of the reviewer's counterpart — the agent whose PR is
/// under review. `branch` is a hint for logging; provisioning resolves the
/// authoritative ref from GitHub (#189).
#[allow(dead_code)]
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

fn reviewer_name_exclusions(
    worker_name: &str,
    role: &ReviewRole,
    prior_runs: &[quorum_core::agent_runs::AgentRun],
) -> std::collections::HashSet<String> {
    let mut excluded = prior_runs
        .iter()
        .filter(|run| run.role == "worker" || run.role == "reviewer")
        .map(|run| run.agent.clone())
        .collect::<std::collections::HashSet<_>>();
    excluded.insert(worker_name.to_string());
    if let ReviewRole::R2 { r1_reviewer, .. } = role {
        excluded.insert(r1_reviewer.clone());
    }
    excluded
}

/// Unified reviewer provisioning (#190). Replaces the separate
/// `spawn_reviewer_for_worker` (R1) and `spawn_r2_reviewer` (R2) paths.
/// Role, prompt, model policy, and audit recording are parameterized;
/// worktree provisioning, capability issuance, process lifecycle, and
/// durable attempt tracking are shared.
#[allow(clippy::too_many_arguments)]
async fn provision_reviewer(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    reviewers: &mut Vec<SlotState>,
    lifetime_roster: &mut LifetimeRoster,
    pr: i64,
    worker: ReviewCounterpart<'_>,
    role: &ReviewRole,
    head_sha: &str,
) -> Result<()> {
    // Reviewer identity is task-scoped, not slot-scoped. A completed worker or
    // reviewer has already left `Pool::in_use`, so consult the durable run
    // history before acquiring a name. This also protects stateless recovery.
    let prior_runs = {
        let p = config.db_path.clone();
        let task_id = worker.task_id;
        tokio::task::spawn_blocking(move || -> Result<Vec<quorum_core::agent_runs::AgentRun>> {
            let conn = quorum_core::db::open(&p)?;
            quorum_core::agent_runs::runs_for_task(&conn, task_id)
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))??
    };
    let excluded = reviewer_name_exclusions(worker.agent_name, role, &prior_runs);
    let acquire_result = name_pool.acquire_excluding(&excluded);
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

    // #189/#201: resolve PR target from GitHub (authoritative head ref, SHA,
    // fork status). Persists on success; falls back to persisted target, then
    // worker branch convention when GitHub is unavailable.
    let task_repo_dir = &config.repo_dir;
    let pr_num = pr;
    let tid = worker.task_id;
    let dbp = config.db_path.clone();
    let repo_dir_for_gh = task_repo_dir.to_path_buf();
    let gh_repo = config.repo.clone();
    let pr_target = tokio::task::spawn_blocking(move || {
        resolve_or_load_pr_target(pr_num, tid, &dbp, &repo_dir_for_gh, Some(&gh_repo))
    })
    .await
    .ok()
    .flatten();
    let (provision_result, sha_to_verify) = if let Some(ref target) = pr_target {
        let result = if target.is_fork {
            wt_mgr
                .fetch_pr_and_provision(task_repo_dir, &branch, &wt_path, target.pr)
                .await
        } else {
            wt_mgr
                .fetch_and_provision(task_repo_dir, &branch, &wt_path, &target.head_ref)
                .await
        };
        (result, Some(target.head_sha.as_str()))
    } else {
        log(&format!(
            "PR #{pr} target not resolved from GitHub — falling back to worker branch '{}'",
            worker.branch
        ));
        let result = wt_mgr
            .fetch_and_provision(task_repo_dir, &branch, &wt_path, worker.branch)
            .await;
        (result, None)
    };
    let provision_ok = match provision_result {
        Ok(_) => {
            if let Some(expected_sha) = sha_to_verify {
                match wt_mgr.verify_head_sha(&wt_path, expected_sha).await {
                    Ok(()) => true,
                    Err(e) => {
                        log(&format!(
                            "reviewer worktree HEAD SHA mismatch for PR #{pr}: {e}"
                        ));
                        wt_mgr.remove(task_repo_dir, &wt_path).await.ok();
                        wt_mgr.delete_branch(task_repo_dir, &branch).await;
                        false
                    }
                }
            } else {
                true
            }
        }
        Err(e) => {
            log(&format!(
                "reviewer worktree provision failed for PR #{pr}: {e}"
            ));
            false
        }
    };
    if !provision_ok {
        let task_id = worker.task_id;
        let role_str = role.as_str().to_string();
        let sha = head_sha.to_string();
        let strikes = {
            let p = config.db_path.clone();
            tokio::task::spawn_blocking(move || -> i64 {
                quorum_core::db::open(&p)
                    .ok()
                    .and_then(|mut conn| {
                        quorum_core::provision_attempts::record_attempt(
                            &mut conn, task_id, pr, &role_str, &sha,
                        )
                        .ok()
                    })
                    .unwrap_or(0)
            })
            .await
            .unwrap_or(0)
        };
        log(&format!(
            "{role_label} provision strike {strikes}/{MAX_REVIEWER_PROVISION_STRIKES} \
             for task #{task_id} PR #{pr}",
            role_label = role.as_str().to_uppercase()
        ));
        if strikes >= MAX_REVIEWER_PROVISION_STRIKES as i64 {
            log(&format!(
                "REVIEWER PROVISION EXHAUSTED: parking task #{task_id} after \
                 {strikes} consecutive {} provision failures for PR #{pr}",
                role.as_str().to_uppercase()
            ));
        }
        name_pool.release(&reviewer_name);
        return Ok(());
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

    let reviewer_model = {
        let p = config.db_path.clone();
        let tid = worker.task_id;
        let cfg_model = config.model.clone();
        tokio::task::spawn_blocking(move || -> String {
            let worker_model = quorum_core::db::open(&p)
                .ok()
                .and_then(|conn| {
                    quorum_core::agent_runs::worker_model(&conn, tid)
                        .ok()
                        .flatten()
                })
                .unwrap_or_else(|| cfg_model.clone());
            escalated_reviewer_model(&worker_model, &cfg_model)
        })
        .await
        .unwrap_or_else(|_| config.model.clone())
    };
    log(&format!(
        "reviewer model escalated to {reviewer_model} for task {}",
        worker.task_id
    ));

    // #130: issue the run capability BEFORE spawning so the reviewer inherits
    // QUORUM_RUN_ID in its environment.
    let cap_run_id = uuid::Uuid::new_v4().to_string();
    {
        let p = config.db_path.clone();
        let rid = cap_run_id.clone();
        let name = reviewer_name.clone();
        let tid = worker.task_id;
        let issue_res = tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&p)?;
            quorum_core::capabilities::issue(&mut conn, &rid, tid, &name, "reviewer", now_unix())
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?;
        if let Err(e) = issue_res {
            log(&format!(
                "reviewer capability issue failed for task {} agent {}: {e} — reviewer will fall back to compat auth",
                worker.task_id, reviewer_name
            ));
        }
    }

    // Build the role-appropriate prompt BEFORE spawn — Codex takes it as a
    // CLI argument while Claude receives it via stdin after spawn.
    let reviewer_kind = runner::AgentKind::for_model(&reviewer_model);
    let prompt = match role {
        ReviewRole::R1 => {
            let spec = reviewer::ReviewerSpec {
                pr,
                worker_agent: worker.agent_name.to_string(),
                reviewer_name: reviewer_name.clone(),
            };
            reviewer::build_review_prompt_for_kind(reviewer_kind, &spec, &config.effort)
        }
        ReviewRole::R2 { r1_reviewer, .. } => {
            let spec = reviewer::R2ReviewSpec {
                pr,
                worker_agent: worker.agent_name.to_string(),
                r1_reviewer: r1_reviewer.clone(),
                r2_name: reviewer_name.clone(),
            };
            reviewer::build_r2_review_prompt_for_kind(reviewer_kind, &spec, &config.effort)
        }
    };

    match reviewer::spawn_reviewer(
        &reviewer_model,
        &config.effort,
        &session_id,
        &wt_path,
        config.agent_bin.as_deref(),
        config.bare_agent,
        vec![
            ("QUORUM_REPO".into(), config.repo.clone()),
            ("QUORUM_AGENT".into(), reviewer_name.clone()),
            ("QUORUM_RUN_ID".into(), cap_run_id.clone()),
        ],
        config.allowed_tools.as_deref(),
        &config.codex_sandbox,
        &prompt,
    ) {
        Ok(mut proc) => {
            // Claude: feed the first turn via stdin. Codex: prompt was a CLI arg.
            if !proc.is_codex() {
                let turn1 = agent::user_turn(&prompt);
                if let Err(e) = proc.feed_turn(&turn1).await {
                    log(&format!(
                        "{}: reviewer feed_turn failed: {e}",
                        role.as_str().to_uppercase()
                    ));
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

            // Persist the run before attaching the reviewer. The run history is
            // the durable identity-exclusion source, so attachment must never
            // become visible without it. A crash after this insert is safe:
            // recovery excludes the name even if ReviewerAttached never fired.
            let reviewer_provider = resolve_provider(&reviewer_model)
                .map(|k| k.to_string())
                .unwrap_or_else(|_| "claude".to_string());
            let reviewer_run_result = {
                let p = config.db_path.clone();
                let name = reviewer_name.clone();
                let m = reviewer_model.clone();
                let e = config.effort.clone();
                let prov = reviewer_provider;
                let tid = worker.task_id;
                let is_r2 = role.is_r2();
                tokio::task::spawn_blocking(move || -> Result<i64> {
                    let conn = quorum_core::db::open(&p)?;
                    if is_r2 {
                        quorum_core::agent_runs::insert_r2(
                            &conn,
                            tid,
                            &name,
                            &m,
                            &e,
                            &prov,
                            now_unix(),
                        )
                    } else {
                        quorum_core::agent_runs::insert(
                            &conn,
                            tid,
                            &name,
                            "reviewer",
                            &m,
                            &e,
                            &prov,
                            now_unix(),
                        )
                    }
                })
                .await
                .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))
            };
            let reviewer_run_id = match reviewer_run_result {
                Ok(Ok(run_id)) => run_id,
                Ok(Err(e)) | Err(e) => {
                    log(&format!(
                        "{}: reviewer run persistence failed before attachment: {e}",
                        role.as_str().to_uppercase()
                    ));
                    proc.kill_and_reap().await;
                    name_pool.release(&reviewer_name);
                    wt_mgr.remove(task_repo_dir, &wt_path).await.ok();
                    wt_mgr.delete_branch(task_repo_dir, &branch).await;
                    let p = config.db_path.clone();
                    let rn = reviewer_name.clone();
                    let failed_cap_run_id = cap_run_id.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Ok(mut conn) = quorum_core::db::open(&p) {
                            let _ = journal::delete(&mut conn, &rn);
                            let _ = quorum_core::capabilities::revoke(
                                &mut conn,
                                &failed_cap_run_id,
                                now_unix(),
                            );
                        }
                    })
                    .await
                    .ok();
                    let task_id = worker.task_id;
                    let role_str = role.as_str().to_string();
                    let sha = head_sha.to_string();
                    let strikes = {
                        let p = config.db_path.clone();
                        tokio::task::spawn_blocking(move || -> i64 {
                            quorum_core::db::open(&p)
                                .ok()
                                .and_then(|mut conn| {
                                    quorum_core::provision_attempts::record_attempt(
                                        &mut conn, task_id, pr, &role_str, &sha,
                                    )
                                    .ok()
                                })
                                .unwrap_or(0)
                        })
                        .await
                        .unwrap_or(0)
                    };
                    log(&format!(
                        "{role_label} provision strike {strikes}/{MAX_REVIEWER_PROVISION_STRIKES} \
                         for task #{task_id} PR #{pr}",
                        role_label = role.as_str().to_uppercase()
                    ));
                    if strikes >= MAX_REVIEWER_PROVISION_STRIKES as i64 {
                        log(&format!(
                            "REVIEWER PROVISION EXHAUSTED: parking task #{task_id} after \
                             {strikes} consecutive {} provision failures for PR #{pr}",
                            role.as_str().to_uppercase()
                        ));
                    }
                    return Err(e);
                }
            };

            // Provisioning is successful only after the reviewer run is
            // durable. Clearing after worktree creation would reset strikes
            // for a persistent run-insert failure on every daemon tick.
            let p = config.db_path.clone();
            let tid = worker.task_id;
            let role_str = role.as_str().to_string();
            tokio::task::spawn_blocking(move || {
                if let Ok(mut conn) = quorum_core::db::open(&p) {
                    let _ = quorum_core::provision_attempts::clear(&mut conn, tid, pr, &role_str);
                }
            })
            .await
            .ok();

            fire_event(
                &config.db_path,
                &reviewer_name,
                worker.task_id,
                &Event::ReviewerAttached {
                    agent: reviewer_name.clone(),
                },
            )
            .await;

            // R2: stash metadata for audit recording when the reviewer finishes.
            if let ReviewRole::R2 {
                r1_reviewer,
                r1_run_id,
            } = role
            {
                let p = config.db_path.clone();
                let tid = worker.task_id;
                let dm = config.model.clone();
                let de = config.effort.clone();
                let r1_rev = r1_reviewer.clone();
                let r2n = reviewer_name.clone();
                let r1_rid = *r1_run_id;
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(conn) = quorum_core::db::open(&p) {
                        let stratum =
                            quorum_core::review_audits::task_stratum(&conn, tid, &dm, &de)
                                .unwrap_or_else(|_| (dm, de, "untagged".into()));
                        R2_META.lock().unwrap().insert(
                            r2n,
                            R2Meta {
                                r1_reviewer: r1_rev,
                                r1_run_id: r1_rid,
                                worker_model: stratum.0,
                                worker_effort: stratum.1,
                                cx_bucket: stratum.2,
                            },
                        );
                    }
                })
                .await;
            }

            // Capture head SHA at spawn time for stale-approval detection.
            let spawn_head_sha = if role.is_r2() {
                Some(head_sha.to_string()).filter(|s| !s.is_empty())
            } else {
                None
            };

            let now_instant = std::time::Instant::now();
            reviewers.push(SlotState {
                agent_name: reviewer_name.clone(),
                proc,
                task_id: worker.task_id,
                session_id,
                model: reviewer_model,
                effort: config.effort.clone(),
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
                last_error_text: None,
                agent_run_id: Some(reviewer_run_id),
                cap_run_id: Some(cap_run_id),
                r2_origin: role.is_r2(),
                reviewed_head_sha: spawn_head_sha,
                codex_thread_id: None,
                pending_prompt: String::new(),
                pending_turn_kind: if role.is_r2() {
                    "r2-review".into()
                } else {
                    "review".into()
                },
            });

            if role.is_r2() {
                log(&format!(
                    "R2: pre-merge reviewer {reviewer_name} spawned for PR #{pr}"
                ));
            } else {
                log(&format!(
                    "R1: reviewer {reviewer_name} spawned for PR #{pr}"
                ));
            }
        }
        Err(e) => {
            log(&format!(
                "{}: failed to spawn reviewer: {e}",
                role.as_str().to_uppercase()
            ));
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
        let mut available = tasks::list(&conn, Some("open"), None, None)?;
        available.extend(
            tasks::list(&conn, Some("rework"), None, None)?
                .into_iter()
                .filter(|task| {
                    codex_retry_turn(task.refs.as_deref()).is_some()
                        || daemon_rework_retry_requested(task.refs.as_deref())
                }),
        );
        let found = available.into_iter().find(|t| {
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

    let mut task = match ready_task {
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
    let retrying_rework = task.status == "rework";
    let claimed = tokio::task::spawn_blocking(move || -> Result<Option<tasks::Task>> {
        let mut conn = quorum_core::db::open(&p)?;
        let now = now_unix();
        if retrying_rework {
            tasks::claim_provider_retry_rework(
                &mut conn,
                &claim_agent,
                claim_task_id,
                tasks::DEFAULT_LEASE_TTL_SECS,
                now,
            )
        } else {
            tasks::claim(
                &mut conn,
                &claim_agent,
                Some(claim_task_id),
                &[],
                tasks::DEFAULT_LEASE_TTL_SECS,
                now,
            )
        }
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
        Ok(Some(claimed_task)) => {
            task = claimed_task;
        }
    }

    let worker_repo_dir = &config.repo_dir;

    // Branch keyed to task + original author, not current assignee — a rework
    // re-claim by a different agent continues the original branch instead of
    // forking a duplicate PR (#340).
    let branch_agent = task.author.as_deref().unwrap_or(&agent_name);
    let session_id = agent::new_session_id();
    let branch = format!("daemon/{}-t{}", branch_agent.to_lowercase(), task.id);
    let wt_path = config
        .worktree_base
        .join(format!("{}-t{}", branch_agent, task.id));

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
    let has_label_model = label_model.is_some();
    let resolved_model = label_model.unwrap_or_else(|| config.model.clone());
    let resolved_effort = label_effort.unwrap_or_else(|| config.effort.clone());

    // Mismatch alert: complexity label > cx_est from refs > skip
    let cx_source = extract_complexity_label(task.labels.as_deref())
        .map(|cx| (cx, "label"))
        .or_else(|| {
            extract_cx_est(&task.refs)
                .and_then(|v| u8::try_from(v).ok())
                .map(|cx| (cx, "cx_est"))
        });
    if let Some((cx, source)) = cx_source {
        let (sug_model, sug_effort) = suggested_for(cx, &config.suggested_models);
        if is_model_effort_below(&resolved_model, &resolved_effort, &sug_model, &sug_effort) {
            let alert_body = format!(
                "model/effort mismatch: task #{} \"{}\" (creator: {}) — \
                 complexity {} (source: {}), using {}/{}, suggested {}/{}",
                task.id,
                task.title,
                task.author.as_deref().unwrap_or("unknown"),
                cx,
                source,
                resolved_model,
                resolved_effort,
                sug_model,
                sug_effort,
            );
            log(&format!("mismatch alert: {alert_body}"));
            let p = db_path.clone();
            let tid = task.id;
            let body = alert_body.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                let mut conn = quorum_core::db::open(&p)?;
                let now = now_unix();
                quorum_core::feed::post(
                    &mut conn,
                    "daemon",
                    "alert",
                    None,
                    &body,
                    None,
                    Some("owner"),
                    86400,
                    now,
                )?;
                quorum_core::tasks::add_note(&mut conn, "daemon", tid, &body, now)?;
                Ok(())
            })
            .await
            .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?
            .ok();
        }
    }

    // #172: enforce the min model/effort floor for worker spawn. Applied AFTER the
    // mismatch alert (so the alert reflects the label-resolved values) but BEFORE the
    // AgentSpec build (so the spawn uses the floored values). Reviewers float above
    // implicitly via escalated_reviewer_model. Separate clamp — not part of flag>file
    // >default resolution.
    let (floored_model, floored_effort) = apply_model_effort_floor(
        &resolved_model,
        &resolved_effort,
        config.min_model.as_deref(),
        config.min_effort.as_deref(),
    );
    if floored_model != resolved_model || floored_effort != resolved_effort {
        log(&format!(
            "model/effort floor: task #{} bumped {resolved_model}/{resolved_effort} -> {floored_model}/{floored_effort}",
            task.id
        ));
    }
    let retry_turn = codex_retry_turn(task.refs.as_deref());
    let (resolved_model, resolved_effort) = retry_turn
        .as_ref()
        .map(|retry| (retry.model.clone(), retry.effort.clone()))
        .unwrap_or((floored_model, floored_effort));

    // #130: issue run capability for this worker (before spawn so env var is available).
    // A silent issue failure would leave the worker holding a QUORUM_RUN_ID pointing at
    // no row — every capability-validated submit would then exit 2 and only the compat
    // (agent-name) path could save it. Log loudly so the operator sees the degrade.
    let cap_run_id = uuid::Uuid::new_v4().to_string();
    {
        let p = db_path.clone();
        let rid = cap_run_id.clone();
        let name = agent_name.clone();
        let tid = task.id;
        let issue_res = tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&p)?;
            quorum_core::capabilities::issue(&mut conn, &rid, tid, &name, "worker", now_unix())
        })
        .await
        .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?;
        if let Err(e) = issue_res {
            log(&format!(
                "worker capability issue failed for task {} agent {agent_name}: {e} — worker will fall back to compat auth",
                task.id
            ));
        }
    }

    let worker_env_vars = vec![
        ("QUORUM_REPO".into(), config.repo.clone()),
        ("QUORUM_AGENT".into(), agent_name.clone()),
        ("QUORUM_RUN_ID".into(), cap_run_id.clone()),
    ];

    let body = task.body.as_deref().unwrap_or(&task.title);
    let prompt_text = retry_turn.as_ref().map_or_else(
        || {
            reviewer::build_worker_prompt(
                &agent_name,
                task.id,
                &task.title,
                body,
                config.limits.max_task_cost_usd,
            )
        },
        |retry| retry.prompt.clone(),
    );

    let resolved_kind = resolve_worker_provider(
        &resolved_model,
        has_label_model,
        retry_turn.is_some(),
        config.runner_kind,
    )?;

    // Codex cannot fabricate USD cost — reject rather than silently spawn
    // with no cost enforcement.
    if resolved_kind == runner::AgentKind::Codex
        && (config.limits.max_task_cost_usd.is_some() || config.limits.max_turn_cost_usd.is_some())
    {
        log(&format!(
            "task #{} routes to Codex but USD cost limits are configured — \
             Codex cannot report cost; rejecting to avoid silent over-spend",
            task.id
        ));
        if let Some(retry) = &retry_turn {
            // Rework retry: park so the operator can intervene without
            // losing the durable turn. Do NOT call release_task — it
            // would set status=open and break the rework state contract.
            persist_codex_provider_block(
                &db_path,
                task.id,
                "Codex+USD cost limit incompatible — daemon has max_*_cost_usd configured",
                retry,
            )
            .await
            .ok();
        } else {
            // Initial open task: poison — this won't resolve on retry.
            poison_task(&db_path, &agent_name, task.id, MAX_POISON_STRIKES).await;
        }
        name_pool.release(&agent_name);
        wt_mgr.remove(&config.repo_dir, &wt_path).await.ok();
        wt_mgr.delete_branch(&config.repo_dir, &branch).await;
        return Ok(false);
    }

    let spawn_result: std::io::Result<runner::RunnerProc> = match resolved_kind {
        runner::AgentKind::Claude => {
            let spec = AgentSpec {
                kind: runner::AgentKind::Claude,
                model: resolved_model.clone(),
                effort: resolved_effort.clone(),
                session_id: session_id.clone(),
                worktree: wt_path.clone(),
                bare: config.bare_agent,
                allowed_tools: config
                    .allowed_tools
                    .clone()
                    .unwrap_or_else(|| agent::ALLOWED_TOOLS.to_string()),
                env_vars: worker_env_vars,
            };
            AgentProc::spawn(&spec, config.agent_bin.as_deref()).map(runner::RunnerProc::Claude)
        }
        runner::AgentKind::Codex => {
            if let Some(thread_id) = retry_turn
                .as_ref()
                .and_then(|retry| retry.thread_id.as_deref())
            {
                codex_agent::CodexProc::spawn_resume(
                    thread_id,
                    &resolved_model,
                    &resolved_effort,
                    &config.codex_sandbox,
                    &wt_path,
                    &prompt_text,
                    &worker_env_vars,
                    config.agent_bin.as_deref(),
                )
                .map(runner::RunnerProc::Codex)
            } else {
                let spec = codex_agent::CodexSpec {
                    model: resolved_model.clone(),
                    effort: resolved_effort.clone(),
                    sandbox: config.codex_sandbox.clone(),
                    worktree: wt_path.clone(),
                    prompt: prompt_text.clone(),
                    env_vars: worker_env_vars,
                };
                codex_agent::CodexProc::spawn(&spec, config.agent_bin.as_deref())
                    .map(runner::RunnerProc::Codex)
            }
        }
    };

    match spawn_result {
        Ok(mut proc) => {
            // Claude: feed the first turn via stdin. Codex: prompt was a CLI arg.
            if !proc.is_codex() {
                let turn1 = agent::user_turn(&prompt_text);
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

            let provider_str = resolved_kind.to_string();
            let worker_run_id = {
                let p = db_path.clone();
                let name = agent_name.clone();
                let m = resolved_model.clone();
                let e = resolved_effort.clone();
                let prov = provider_str.clone();
                let tid = task.id;
                tokio::task::spawn_blocking(move || -> Result<i64> {
                    let conn = quorum_core::db::open(&p)?;
                    quorum_core::agent_runs::insert(
                        &conn,
                        tid,
                        &name,
                        "worker",
                        &m,
                        &e,
                        &prov,
                        now_unix(),
                    )
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
                model: resolved_model,
                effort: resolved_effort,
                worktree_path: wt_path,
                branch,
                draining: true,
                pr: task
                    .refs
                    .as_deref()
                    .and_then(|refs| serde_json::from_str::<serde_json::Value>(refs).ok())
                    .and_then(|refs| refs.get("pr").and_then(|pr| pr.as_i64())),
                rework_count: retry_slot_rework_count(
                    task.rework_round,
                    retry_turn.as_ref(),
                    daemon_rework_retry_requested(task.refs.as_deref()),
                ),
                cost_tokens: 0,
                cost_usd: 0.0,
                task_started_at: now_instant,
                turn_started_at: now_instant,
                turn_ended_at: None,
                agent_state: None,
                session_log: worker_session_log,
                live_stats: LiveStats::new(),
                error_turn_count: 0,
                last_error_text: None,
                agent_run_id: worker_run_id,
                cap_run_id: Some(cap_run_id),
                r2_origin: false,
                reviewed_head_sha: None,
                codex_thread_id: None,
                pending_prompt: prompt_text,
                pending_turn_kind: retry_turn
                    .as_ref()
                    .map(|retry| retry.turn_kind.clone())
                    .unwrap_or_else(|| "initial".into()),
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
    let reason = format!("worker {agent} died {strikes} time(s) without producing output");
    park_task(db_path, task_id, &reason, "open").await;
}

async fn park_task(db_path: &std::path::Path, task_id: i64, reason: &str, resume_status: &str) {
    log(&format!("PARKED: task #{task_id}: {reason}"));
    let p = db_path.to_path_buf();
    let reason = reason.to_string();
    let resume_status = resume_status.to_string();
    match tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&p)?;
        tasks::park(&mut conn, task_id, &reason, &resume_status, now_unix())?;
        Ok(())
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log(&format!("FATAL: failed to park task #{task_id}: {error}")),
        Err(error) => log(&format!(
            "FATAL: park task #{task_id} join failure: {error}"
        )),
    }
}

/// Fire a lifecycle event, returning the result or an error description.
/// On failure, persists a structured diagnostic to errors/notes/alerts.
async fn fire_event_result(
    db_path: &std::path::Path,
    agent: &str,
    task_id: i64,
    event: &Event,
) -> std::result::Result<tasks::TransitionResult, String> {
    let p = db_path.to_path_buf();
    let a = agent.to_string();
    let ev = event.clone();
    let ev_debug = format!("{:?}", event);
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
            Ok(tr)
        }
        Ok(Err(e)) => {
            let msg = e.to_string();
            log(&format!(
                "lifecycle: fire_event failed for task #{task_id}: {msg}"
            ));
            persist_lifecycle_diagnostic(db_path, agent, task_id, &ev_debug, &msg).await;
            Err(msg)
        }
        Err(e) => {
            let msg = format!("join error: {e}");
            log(&format!(
                "lifecycle: fire_event join error for task #{task_id}: {msg}"
            ));
            persist_lifecycle_diagnostic(db_path, agent, task_id, &ev_debug, &msg).await;
            Err(msg)
        }
    }
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
    fire_event_result(db_path, agent, task_id, event).await.ok()
}

/// Gather task context from the DB and persist a structured lifecycle diagnostic.
async fn persist_lifecycle_diagnostic(
    db_path: &std::path::Path,
    agent: &str,
    task_id: i64,
    event_desc: &str,
    error_msg: &str,
) {
    let p = db_path.to_path_buf();
    let actor = agent.to_string();
    let ev = event_desc.to_string();
    let err = error_msg.to_string();
    tokio::task::spawn_blocking(move || {
        let Ok(mut conn) = quorum_core::db::open(&p) else {
            return;
        };
        let now = now_unix();
        quorum_core::errlog::persist_lifecycle_diagnostic(
            &mut conn, now, &actor, task_id, &ev, &err,
        );
    })
    .await
    .ok();
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
    state: SlotState,
    finalize_verdict: Option<&str>,
    end_reason: &str,
) {
    cleanup_slot_inner(
        config,
        wt_mgr,
        name_pool,
        state,
        finalize_verdict,
        true,
        end_reason,
    )
    .await;
}

async fn cleanup_slot_inner(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    mut state: SlotState,
    finalize_verdict: Option<&str>,
    delete_branch: bool,
    end_reason: &str,
) {
    log(&format!(
        "tearing down worker {} (task #{}{})",
        state.agent_name,
        state.task_id,
        if delete_branch {
            ""
        } else {
            ", branch preserved"
        },
    ));

    if let Some(ref mut sl) = state.session_log {
        sl.finalize(finalize_verdict);
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
    if delete_branch {
        wt_mgr.delete_branch(repo_dir, &state.branch).await;
    }

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

    // #130: revoke run capability
    if let Some(ref rid) = state.cap_run_id {
        let p = config.db_path.clone();
        let rid = rid.clone();
        tokio::task::spawn_blocking(move || {
            if let Ok(mut conn) = quorum_core::db::open(&p) {
                let _ = quorum_core::capabilities::revoke(&mut conn, &rid, now_unix());
            }
        })
        .await
        .ok();
    }

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

    // #130: revoke run capability
    if let Some(ref rid) = state.cap_run_id {
        let p = config.db_path.clone();
        let rid = rid.clone();
        tokio::task::spawn_blocking(move || {
            if let Ok(mut conn) = quorum_core::db::open(&p) {
                let _ = quorum_core::capabilities::revoke(&mut conn, &rid, now_unix());
            }
        })
        .await
        .ok();
    }

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

/// Record an R2 audit row from stashed metadata (best-effort).
async fn record_r2_audit(
    config: &ServeConfig,
    r2_name: &str,
    r2_run_id: Option<i64>,
    task_id: i64,
    pr: Option<i64>,
    verdict: Option<&str>,
) {
    let meta = R2_META.lock().unwrap().remove(r2_name);
    let Some(meta) = meta else { return };
    let audit = quorum_core::review_audits::ReviewAudit {
        task_id,
        pr_number: pr.unwrap_or(0),
        r1_run_id: meta.r1_run_id.unwrap_or(0),
        r2_run_id: r2_run_id.unwrap_or(0),
        r1_reviewer: meta.r1_reviewer,
        r2_reviewer: r2_name.to_string(),
        model: meta.worker_model,
        effort: meta.worker_effort,
        cx_bucket: meta.cx_bucket,
        missed_count: 0,
        overcaught_count: 0,
        r1_verdict: "approved".into(),
        r2_verdict: verdict.map(|s| s.to_string()),
        created_at: now_unix(),
    };
    let p = config.db_path.clone();
    let _ = tokio::task::spawn_blocking(move || -> Result<()> {
        let conn = quorum_core::db::open(&p)?;
        quorum_core::review_audits::insert(&conn, &audit)?;
        Ok(())
    })
    .await;
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

// ── Remediation worker (#159) ────────────────────────────────────────────

/// Spawn a remediation worker for a task in rework with no live worker.
/// The worker gets the existing PR, branch, blocking findings, and task body.
/// Returns true if a worker was successfully added to the workers vec.
#[allow(clippy::too_many_arguments)]
async fn spawn_remediation_worker(
    config: &ServeConfig,
    wt_mgr: &WorktreeManager,
    name_pool: &mut Pool,
    workers: &mut Vec<SlotState>,
    lifetime_roster: &mut LifetimeRoster,
    task_id: i64,
    pr: i64,
    feedback: &str,
) -> bool {
    let db_path = &config.db_path;

    // Fetch task body + author for context and branch resolution.
    let (task_body, task_author, task_review_only) = {
        let p = db_path.clone();
        let tid = task_id;
        tokio::task::spawn_blocking(move || -> (String, String, bool) {
            let conn = quorum_core::db::open(&p).ok();
            let t = conn
                .as_ref()
                .and_then(|c| tasks::get(c, tid).ok().flatten());
            match t {
                Some(task) => (
                    task.body.unwrap_or_default(),
                    task.author.unwrap_or_default(),
                    task.review_only,
                ),
                None => (String::new(), String::new(), false),
            }
        })
        .await
        .unwrap_or((String::new(), String::new(), false))
    };

    // #189/#201: resolve PR target from GitHub, persist on success, fall
    // back to persisted target when GitHub is unavailable.
    let pr_target = {
        let pr_val = pr;
        let tid = task_id;
        let dbp = db_path.clone();
        let repo_dir = config.repo_dir.clone();
        let gh_repo = config.repo.clone();
        tokio::task::spawn_blocking(move || {
            resolve_or_load_pr_target(pr_val, tid, &dbp, &repo_dir, Some(&gh_repo))
        })
        .await
        .ok()
        .flatten()
    };
    let fallback_branch = if pr_target.is_none() {
        let fb = orphan_worker_branch(&task_author, task_id, task_review_only);
        if fb.is_none() {
            log(&format!(
                "remediation: cannot resolve PR #{pr} target — cannot spawn worker"
            ));
            return false;
        }
        fb
    } else {
        None
    };

    let agent_name = name_pool.acquire().into_name();
    lifetime_roster.register(&agent_name);

    // Acquire the remediation lease BEFORE any other write that could invoke
    // sweep. Without this, an intervening sweep_on_write sees a rework task
    // with no active claim and reaps it to open (#199).
    {
        let p = db_path.clone();
        let name = agent_name.clone();
        let tid = task_id;
        let claimed = tokio::task::spawn_blocking(move || -> Result<bool> {
            let mut conn = quorum_core::db::open(&p)?;
            Ok(tasks::claim_remediation_rework(
                &mut conn,
                &name,
                tid,
                tasks::DEFAULT_LEASE_TTL_SECS,
                now_unix(),
            )?
            .is_some())
        })
        .await
        .ok()
        .and_then(|r| r.ok())
        .unwrap_or(false);
        if !claimed {
            log(&format!(
                "remediation: claim failed for task #{task_id} — task no longer in rework"
            ));
            name_pool.release(&agent_name);
            return false;
        }
    }

    // Drain stale mailbox rows.
    {
        let p = db_path.clone();
        let name = agent_name.clone();
        let _ = tokio::task::spawn_blocking(move || -> Result<usize> {
            let mut conn = quorum_core::db::open(&p)?;
            mailbox::consume_all_for_agent(&mut conn, &name)
        })
        .await;
    }

    log(&format!(
        "spawning remediation worker {} for task #{task_id} PR #{pr}",
        agent_name
    ));

    let session_id = agent::new_session_id();
    let branch = pr_target
        .as_ref()
        .map(|t| t.head_ref.clone())
        .or_else(|| fallback_branch.clone())
        .unwrap();
    let wt_path = config
        .worktree_base
        .join(format!("{}-t{}", agent_name, task_id));

    let task_repo_dir = &config.repo_dir;
    let (provision_result, sha_to_verify) = if let Some(ref target) = pr_target {
        let result = if target.is_fork {
            wt_mgr
                .fetch_pr_and_provision(task_repo_dir, &branch, &wt_path, target.pr)
                .await
        } else {
            wt_mgr
                .fetch_and_provision(task_repo_dir, &branch, &wt_path, &target.head_ref)
                .await
        };
        (result, Some(target.head_sha.as_str()))
    } else {
        let result = wt_mgr
            .fetch_and_provision(task_repo_dir, &branch, &wt_path, &branch)
            .await;
        (result, None)
    };
    let provision_ok = match provision_result {
        Ok(_) => {
            if let Some(expected_sha) = sha_to_verify {
                wt_mgr.verify_head_sha(&wt_path, expected_sha).await.is_ok()
            } else {
                true
            }
        }
        Err(_) => false,
    };
    if !provision_ok {
        log(&format!(
            "remediation: worktree provision failed for PR #{pr} — giving up"
        ));
        // Release the lease installed by claim_remediation_rework.
        {
            let p = db_path.clone();
            let name = agent_name.clone();
            let tid = task_id;
            let _ = tokio::task::spawn_blocking(move || {
                let mut conn = quorum_core::db::open(&p)?;
                tasks::release_remediation_lease(&mut conn, &name, tid, now_unix())
            })
            .await;
        }
        name_pool.release(&agent_name);
        return false;
    }

    // Author was already set by claim_remediation_rework.

    let worker_session_log = config.log_dir.as_ref().and_then(|ld| {
        session_log::SessionLog::create(
            ld,
            &agent_name,
            "worker",
            Some(task_id),
            &session_id,
            &branch,
            now_unix(),
        )
        .ok()
    });

    // Journal entry.
    {
        let p = db_path.clone();
        let entry = JournalEntry {
            agent: agent_name.clone(),
            role: "worker".into(),
            task_id: Some(task_id),
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
            pr: Some(pr),
            rework_count: 0,
        };
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&p)?;
            journal::upsert(&mut conn, &entry)
        })
        .await
        .ok();
    }

    let cap_run_id = uuid::Uuid::new_v4().to_string();
    {
        let p = db_path.clone();
        let rid = cap_run_id.clone();
        let name = agent_name.clone();
        let tid = task_id;
        let _ = tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = quorum_core::db::open(&p)?;
            quorum_core::capabilities::issue(&mut conn, &rid, tid, &name, "worker", now_unix())
        })
        .await;
    }

    let prompt = reviewer::build_remediation_turn(
        &agent_name,
        task_id,
        pr,
        feedback,
        &task_body,
        config.limits.max_task_cost_usd,
    );

    let remediation_env = vec![
        ("QUORUM_REPO".into(), config.repo.clone()),
        ("QUORUM_AGENT".into(), agent_name.clone()),
        ("QUORUM_RUN_ID".into(), cap_run_id.clone()),
    ];

    // Recover the original worker's provider and model from agent_runs so
    // remediation cannot silently switch providers (e.g. tier:o3 task reworked
    // as Claude because the daemon default is Claude).
    let (remediation_model, remediation_kind) = {
        let p = db_path.clone();
        let tid = task_id;
        let cfg_model = config.model.clone();
        let cfg_kind = config.runner_kind;
        tokio::task::spawn_blocking(move || -> (String, runner::AgentKind) {
            let conn = match quorum_core::db::open(&p) {
                Ok(c) => c,
                Err(_) => return (cfg_model, runner_kind_to_agent_kind(cfg_kind)),
            };
            let provider = quorum_core::agent_runs::worker_provider(&conn, tid)
                .ok()
                .flatten();
            let model = quorum_core::agent_runs::worker_model(&conn, tid)
                .ok()
                .flatten();
            resolve_remediation_provider(provider.as_deref(), model, &cfg_model, cfg_kind)
        })
        .await
        .unwrap_or_else(|_| {
            (
                config.model.clone(),
                runner_kind_to_agent_kind(config.runner_kind),
            )
        })
    };
    let remediation_effort = config.effort.clone();

    // Codex cannot fabricate USD cost — fail-closed.
    if remediation_kind == runner::AgentKind::Codex
        && (config.limits.max_task_cost_usd.is_some() || config.limits.max_turn_cost_usd.is_some())
    {
        log(&format!(
            "remediation task #{task_id} routes to Codex but USD cost limits \
             are configured — parking to avoid silent over-spend"
        ));
        let park_prompt = prompt.clone();
        persist_codex_provider_block(
            db_path,
            task_id,
            "Codex+USD cost limit incompatible — daemon has max_*_cost_usd configured",
            &CodexRetryTurn {
                model: remediation_model.clone(),
                effort: remediation_effort.clone(),
                prompt: park_prompt,
                turn_kind: "rework".into(),
                thread_id: codex_thread_id_from_refs(db_path, task_id).await,
            },
        )
        .await
        .ok();
        name_pool.release(&agent_name);
        wt_mgr.remove(task_repo_dir, &wt_path).await.ok();
        wt_mgr.delete_branch(task_repo_dir, &branch).await;
        return false;
    }

    // For Codex rework: look up persisted thread_id for continuation.
    let codex_thread_id = if remediation_kind == runner::AgentKind::Codex {
        codex_thread_id_from_refs(db_path, task_id).await
    } else {
        None
    };

    let spawn_result: std::io::Result<runner::RunnerProc> = match remediation_kind {
        runner::AgentKind::Claude => agent::AgentProc::spawn(
            &agent::AgentSpec {
                kind: runner::AgentKind::Claude,
                model: remediation_model.clone(),
                effort: remediation_effort.clone(),
                session_id: session_id.clone(),
                worktree: wt_path.clone(),
                bare: config.bare_agent,
                allowed_tools: config
                    .allowed_tools
                    .as_deref()
                    .unwrap_or(agent::ALLOWED_TOOLS)
                    .to_string(),
                env_vars: remediation_env,
            },
            config.agent_bin.as_deref(),
        )
        .map(runner::RunnerProc::Claude),
        runner::AgentKind::Codex => if let Some(ref tid) = codex_thread_id {
            codex_agent::CodexProc::spawn_resume(
                tid,
                &remediation_model,
                &remediation_effort,
                &config.codex_sandbox,
                &wt_path,
                &prompt,
                &remediation_env,
                config.agent_bin.as_deref(),
            )
        } else {
            log("remediation: no persisted thread_id — starting fresh Codex exec");
            codex_agent::CodexProc::spawn(
                &codex_agent::CodexSpec {
                    model: remediation_model.clone(),
                    effort: remediation_effort.clone(),
                    sandbox: config.codex_sandbox.clone(),
                    worktree: wt_path.clone(),
                    prompt: prompt.clone(),
                    env_vars: remediation_env,
                },
                config.agent_bin.as_deref(),
            )
        }
        .map(runner::RunnerProc::Codex),
    };

    match spawn_result {
        Ok(mut proc) => {
            if !proc.is_codex() {
                let turn1 = agent::user_turn(&prompt);
                if let Err(e) = proc.feed_turn(&turn1).await {
                    log(&format!("remediation feed_turn failed: {e}"));
                    proc.kill_and_reap().await;
                    // Release the lease installed by claim_remediation_rework.
                    {
                        let p = db_path.clone();
                        let name = agent_name.clone();
                        let tid = task_id;
                        let _ = tokio::task::spawn_blocking(move || {
                            let mut conn = quorum_core::db::open(&p)?;
                            tasks::release_remediation_lease(&mut conn, &name, tid, now_unix())
                        })
                        .await;
                    }
                    name_pool.release(&agent_name);
                    wt_mgr.remove(task_repo_dir, &wt_path).await.ok();
                    wt_mgr.delete_branch(task_repo_dir, &branch).await;
                    return false;
                }
            }

            let remediation_provider_str = remediation_kind.to_string();
            let worker_run_id = {
                let p = db_path.clone();
                let name = agent_name.clone();
                let m = remediation_model.clone();
                let e = remediation_effort.clone();
                let prov = remediation_provider_str;
                let tid = task_id;
                tokio::task::spawn_blocking(move || -> Result<i64> {
                    let conn = quorum_core::db::open(&p)?;
                    quorum_core::agent_runs::insert(
                        &conn,
                        tid,
                        &name,
                        "worker",
                        &m,
                        &e,
                        &prov,
                        now_unix(),
                    )
                })
                .await
                .ok()
                .and_then(|r| r.ok())
            };

            let now_instant = std::time::Instant::now();
            workers.push(SlotState {
                agent_name: agent_name.clone(),
                proc,
                task_id,
                session_id,
                model: remediation_model.clone(),
                effort: remediation_effort.clone(),
                worktree_path: wt_path,
                branch,
                draining: true,
                pr: Some(pr),
                rework_count: 1,
                cost_tokens: 0,
                cost_usd: 0.0,
                task_started_at: now_instant,
                turn_started_at: now_instant,
                turn_ended_at: None,
                agent_state: None,
                session_log: worker_session_log,
                live_stats: LiveStats::new(),
                error_turn_count: 0,
                last_error_text: None,
                agent_run_id: worker_run_id,
                cap_run_id: Some(cap_run_id),
                r2_origin: false,
                reviewed_head_sha: None,
                codex_thread_id,
                pending_prompt: prompt,
                pending_turn_kind: "rework".into(),
            });

            log(&format!(
                "remediation worker {} spawned for task #{task_id} PR #{pr}",
                agent_name
            ));
            true
        }
        Err(e) => {
            log(&format!("remediation worker spawn failed: {e}"));
            // Release the lease installed by claim_remediation_rework.
            {
                let p = db_path.clone();
                let name = agent_name.clone();
                let tid = task_id;
                let _ = tokio::task::spawn_blocking(move || {
                    let mut conn = quorum_core::db::open(&p)?;
                    tasks::release_remediation_lease(&mut conn, &name, tid, now_unix())
                })
                .await;
            }
            name_pool.release(&agent_name);
            wt_mgr.remove(task_repo_dir, &wt_path).await.ok();
            wt_mgr.delete_branch(task_repo_dir, &branch).await;
            false
        }
    }
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
    fn tier_to_model_id_o3() {
        assert_eq!(tier_to_model_id("o3").as_deref(), Some("o3"));
    }

    #[test]
    fn tier_to_model_id_o4_mini() {
        assert_eq!(tier_to_model_id("o4-mini").as_deref(), Some("o4-mini"));
    }

    #[test]
    fn resolve_provider_claude_models() {
        assert_eq!(
            resolve_provider("claude-opus-4-6").unwrap(),
            runner::AgentKind::Claude
        );
        assert_eq!(
            resolve_provider("claude-opus-4-7").unwrap(),
            runner::AgentKind::Claude
        );
        assert_eq!(
            resolve_provider("claude-opus-4-8").unwrap(),
            runner::AgentKind::Claude
        );
        assert_eq!(
            resolve_provider("claude-sonnet-5").unwrap(),
            runner::AgentKind::Claude
        );
    }

    #[test]
    fn resolve_provider_codex_models() {
        assert_eq!(resolve_provider("o3").unwrap(), runner::AgentKind::Codex);
        assert_eq!(
            resolve_provider("o4-mini").unwrap(),
            runner::AgentKind::Codex
        );
        assert_eq!(
            resolve_provider("gpt-4.1").unwrap(),
            runner::AgentKind::Codex
        );
        assert_eq!(resolve_provider("gpt-5").unwrap(), runner::AgentKind::Codex);
        assert_eq!(
            resolve_provider("gpt-5-turbo").unwrap(),
            runner::AgentKind::Codex
        );
    }

    #[test]
    fn resolve_provider_default_claude_model() {
        assert_eq!(
            resolve_provider("claude-opus-4-6").unwrap(),
            runner::AgentKind::Claude
        );
    }

    #[test]
    fn resolve_provider_unknown_model_rejected() {
        let err = resolve_provider("llama-3").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown model"), "got: {msg}");
    }

    #[test]
    fn resolve_provider_persists_via_agent_runs() {
        let dir = tempfile::tempdir().unwrap();
        let conn = quorum_core::db::open(&dir.path().join("q.db")).unwrap();

        let kind = resolve_provider("claude-opus-4-7").unwrap();
        quorum_core::agent_runs::insert(
            &conn,
            1,
            "w1",
            "worker",
            "claude-opus-4-7",
            "high",
            &kind.to_string(),
            100,
        )
        .unwrap();

        let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].provider.as_deref(), Some("claude"));

        let codex_kind = resolve_provider("o3").unwrap();
        quorum_core::agent_runs::insert(
            &conn,
            1,
            "w2",
            "worker",
            "o3",
            "high",
            &codex_kind.to_string(),
            200,
        )
        .unwrap();

        let runs = quorum_core::agent_runs::runs_for_task(&conn, 1).unwrap();
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[1].provider.as_deref(), Some("codex"));
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

    #[test]
    fn model_rank_known_models() {
        assert_eq!(model_rank("claude-sonnet-5"), Some(0));
        assert_eq!(model_rank("claude-opus-4-6"), Some(1));
        assert_eq!(model_rank("claude-opus-4-7"), Some(2));
        assert_eq!(model_rank("claude-opus-4-8"), Some(3));
        assert_eq!(model_rank("unknown-model"), None);
    }

    #[test]
    fn escalated_reviewer_default_worker_steps_up() {
        assert_eq!(
            escalated_reviewer_model("claude-opus-4-6", "claude-opus-4-6"),
            "claude-opus-4-7"
        );
    }

    #[test]
    fn escalated_reviewer_top_tier_caps() {
        assert_eq!(
            escalated_reviewer_model("claude-opus-4-8", "claude-opus-4-6"),
            "claude-opus-4-8"
        );
    }

    #[test]
    fn escalated_reviewer_config_higher_wins() {
        assert_eq!(
            escalated_reviewer_model("claude-sonnet-5", "claude-opus-4-7"),
            "claude-opus-4-7"
        );
    }

    #[test]
    fn escalated_reviewer_unknown_worker_uses_rank_zero() {
        assert_eq!(
            escalated_reviewer_model("unknown-model", "claude-opus-4-6"),
            "claude-opus-4-6"
        );
    }

    // R2 model-routing: Sonnet worker → Opus 4.6 reviewer
    #[test]
    fn r2_model_routing_sonnet_to_opus_46() {
        assert_eq!(
            escalated_reviewer_model("claude-sonnet-5", "claude-sonnet-5"),
            "claude-opus-4-6"
        );
    }

    // R2 model-routing: Opus 4.6 worker → Opus 4.7 reviewer
    #[test]
    fn r2_model_routing_opus_46_to_opus_47() {
        assert_eq!(
            escalated_reviewer_model("claude-opus-4-6", "claude-sonnet-5"),
            "claude-opus-4-7"
        );
    }

    // R2 model-routing: top-tier worker caps at top tier
    #[test]
    fn r2_model_routing_top_tier_capped() {
        assert_eq!(
            escalated_reviewer_model("claude-opus-4-8", "claude-sonnet-5"),
            "claude-opus-4-8"
        );
    }

    // R2 model-routing: floor config raises below-escalation reviewer
    #[test]
    fn r2_model_routing_floor_overrides_escalation() {
        assert_eq!(
            escalated_reviewer_model("claude-sonnet-5", "claude-opus-4-7"),
            "claude-opus-4-7",
            "config floor must override natural escalation when higher"
        );
    }

    // #196: Codex model escalation — both non-Claude returns config as-is
    #[test]
    fn escalated_reviewer_codex_both_returns_config() {
        assert_eq!(
            escalated_reviewer_model("o4-mini", "o4-mini"),
            "o4-mini",
            "both non-Claude: return config model as-is"
        );
        assert_eq!(
            escalated_reviewer_model("gpt-4o", "gpt-5.6-codex"),
            "gpt-5.6-codex",
            "both non-Claude: return config model"
        );
    }

    // #196: mixed Claude worker + Codex config still escalates within Claude tiers
    #[test]
    fn escalated_reviewer_claude_worker_codex_config() {
        assert_eq!(
            escalated_reviewer_model("claude-opus-4-6", "o4-mini"),
            "claude-opus-4-7",
            "Claude worker with non-Claude config: escalate within Claude tiers"
        );
    }

    // #196: Codex worker + Claude config uses Claude tier escalation
    #[test]
    fn escalated_reviewer_codex_worker_claude_config() {
        assert_eq!(
            escalated_reviewer_model("o4-mini", "claude-opus-4-7"),
            "claude-opus-4-7",
            "non-Claude worker with Claude config: config floor wins"
        );
    }

    // #196: recovery retains provider — deterministic model resolution
    #[test]
    fn escalated_reviewer_model_is_deterministic() {
        let model_a = escalated_reviewer_model("claude-opus-4-6", "claude-opus-4-6");
        let model_b = escalated_reviewer_model("claude-opus-4-6", "claude-opus-4-6");
        assert_eq!(model_a, model_b, "same inputs must yield same output");
        assert_eq!(
            runner::AgentKind::for_model(&model_a),
            runner::AgentKind::Claude
        );

        let codex_a = escalated_reviewer_model("o4-mini", "o4-mini");
        let codex_b = escalated_reviewer_model("o4-mini", "o4-mini");
        assert_eq!(
            codex_a, codex_b,
            "Codex: same inputs must yield same output"
        );
        assert_eq!(
            runner::AgentKind::for_model(&codex_a),
            runner::AgentKind::Codex
        );
    }

    #[test]
    fn escalated_reviewer_short_alias_sonnet() {
        let result = escalated_reviewer_model("sonnet", "sonnet");
        assert_eq!(
            result, "claude-opus-4-6",
            "short alias 'sonnet' must escalate within Claude tiers, not pass through as Codex"
        );
        assert_eq!(
            runner::AgentKind::for_model(&result),
            runner::AgentKind::Claude,
        );
    }

    #[test]
    fn extract_cx_est_from_refs() {
        assert_eq!(extract_cx_est(&Some(r#"{"cx_est":4}"#.into())), Some(4));
        assert_eq!(
            extract_cx_est(&Some(r#"{"cx_est":5,"pr":99}"#.into())),
            Some(5)
        );
        assert_eq!(extract_cx_est(&Some(r#"{"pr":42}"#.into())), None);
        assert_eq!(extract_cx_est(&None), None);
        assert_eq!(extract_cx_est(&Some("not json".into())), None);
        assert_eq!(
            extract_cx_est(&Some(r#"{"cx_est":"bad"}"#.into())),
            None,
            "non-integer cx_est must return None"
        );
    }

    #[test]
    fn extract_complexity_label_valid() {
        assert_eq!(
            extract_complexity_label(Some(r#"["complexity:3"]"#)),
            Some(3)
        );
        assert_eq!(
            extract_complexity_label(Some(r#"["complexity:5","tier:opus-46"]"#)),
            Some(5)
        );
    }

    #[test]
    fn extract_complexity_label_out_of_range() {
        assert_eq!(extract_complexity_label(Some(r#"["complexity:0"]"#)), None);
        assert_eq!(extract_complexity_label(Some(r#"["complexity:6"]"#)), None);
        assert_eq!(
            extract_complexity_label(Some(r#"["complexity:bad"]"#)),
            None
        );
        assert_eq!(extract_complexity_label(None), None);
    }

    #[test]
    fn suggested_for_defaults() {
        let empty = std::collections::HashMap::new();
        assert_eq!(
            suggested_for(1, &empty),
            ("claude-sonnet-5".into(), "medium".into())
        );
        assert_eq!(
            suggested_for(4, &empty),
            ("claude-opus-4-7".into(), "high".into())
        );
        assert_eq!(
            suggested_for(5, &empty),
            ("claude-opus-4-8".into(), "high".into())
        );
    }

    #[test]
    fn suggested_for_override() {
        let mut m = std::collections::HashMap::new();
        m.insert("3".into(), "opus-48/high".into());
        assert_eq!(
            suggested_for(3, &m),
            ("claude-opus-4-8".into(), "high".into())
        );
        // non-overridden key still uses default
        assert_eq!(
            suggested_for(1, &m),
            ("claude-sonnet-5".into(), "medium".into())
        );
    }

    #[test]
    fn is_model_effort_below_detects_mismatch() {
        assert!(is_model_effort_below(
            "claude-opus-4-6",
            "medium",
            "claude-opus-4-7",
            "high"
        ));
        assert!(is_model_effort_below(
            "claude-opus-4-7",
            "medium",
            "claude-opus-4-7",
            "high"
        ));
        assert!(is_model_effort_below(
            "claude-sonnet-5",
            "high",
            "claude-opus-4-6",
            "medium"
        ));
    }

    #[test]
    fn is_model_effort_below_no_mismatch_when_at_or_above() {
        assert!(!is_model_effort_below(
            "claude-opus-4-7",
            "high",
            "claude-opus-4-7",
            "high"
        ));
        assert!(!is_model_effort_below(
            "claude-opus-4-8",
            "medium",
            "claude-opus-4-7",
            "high"
        ));
        assert!(!is_model_effort_below(
            "claude-opus-4-7",
            "high",
            "claude-opus-4-6",
            "medium"
        ));
    }

    #[test]
    fn effort_rank_ordering() {
        assert!(effort_rank("medium") < effort_rank("high"));
        assert_eq!(effort_rank("unknown"), 0);
    }

    #[test]
    fn floor_bumps_below_to_floor() {
        // #172: resolved below floor → clamped up to floor (both dims).
        let (m, e) = apply_model_effort_floor(
            "claude-sonnet-5",
            "medium",
            Some("claude-opus-4-7"),
            Some("high"),
        );
        assert_eq!(m, "claude-opus-4-7");
        assert_eq!(e, "high");
    }

    #[test]
    fn floor_leaves_at_floor_unchanged() {
        let (m, e) = apply_model_effort_floor(
            "claude-opus-4-7",
            "high",
            Some("claude-opus-4-7"),
            Some("high"),
        );
        assert_eq!(m, "claude-opus-4-7");
        assert_eq!(e, "high");
    }

    #[test]
    fn floor_never_lowers_above_floor() {
        // Higher tier at lower effort is still above floor — must NOT be touched.
        let (m, e) = apply_model_effort_floor(
            "claude-opus-4-8",
            "medium",
            Some("claude-opus-4-7"),
            Some("high"),
        );
        assert_eq!(m, "claude-opus-4-8");
        assert_eq!(e, "medium");
    }

    #[test]
    fn floor_effort_tiebreak_same_model() {
        // #172: same model, effort below floor → effort bumped, model unchanged.
        let (m, e) = apply_model_effort_floor("claude-opus-4-7", "medium", None, Some("high"));
        assert_eq!(m, "claude-opus-4-7");
        assert_eq!(e, "high");
    }

    #[test]
    fn floor_none_is_identity() {
        // Regression guard: no floor → spawn values identical to today.
        let (m, e) = apply_model_effort_floor("claude-sonnet-5", "medium", None, None);
        assert_eq!(m, "claude-sonnet-5");
        assert_eq!(e, "medium");
    }

    #[test]
    fn floor_reviewer_floats_above_floored_worker() {
        // #172: a task asking for sonnet-5 with a floor of opus-47 spawns the worker
        // at opus-47, and the reviewer escalates strictly above it (opus-48).
        let (worker_model, _) = apply_model_effort_floor(
            "claude-sonnet-5",
            "high",
            Some("claude-opus-4-7"),
            Some("high"),
        );
        assert_eq!(worker_model, "claude-opus-4-7");
        let reviewer = escalated_reviewer_model(&worker_model, "claude-sonnet-5");
        assert_eq!(reviewer, "claude-opus-4-8");
        assert!(model_rank(&reviewer) > model_rank(&worker_model));
    }

    fn test_db() -> (rusqlite::Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let conn = quorum_core::db::open(&dir.path().join("q.db")).unwrap();
        (conn, dir)
    }

    #[test]
    fn mismatch_alert_posts_feed_and_note() {
        let (mut conn, _dir) = test_db();
        let now = 1000;
        // Create a task to attach the note to
        quorum_core::tasks::create(
            &mut conn,
            "tester",
            "test task",
            None,
            0,
            None,
            None,
            None,
            None,
            now,
        )
        .unwrap();
        let tasks = quorum_core::tasks::list(&conn, None, None, None).unwrap();
        let tid = tasks[0].id;

        let body =
            "model/effort mismatch: task #1 cx=4 using opus-4-6/medium, suggested opus-4-7/high";
        quorum_core::feed::post(
            &mut conn,
            "daemon",
            "alert",
            None,
            body,
            None,
            Some("owner"),
            86400,
            now,
        )
        .unwrap();
        quorum_core::tasks::add_note(&mut conn, "daemon", tid, body, now).unwrap();

        // Verify alert message in feed (read as "owner" — the recipient)
        let msgs = quorum_core::feed::read(
            &mut conn,
            "owner",
            None,
            None,
            quorum_core::feed::ReadFilter::All,
            10,
            now,
        )
        .unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].kind, "alert");
        assert!(msgs[0].body.contains("mismatch"));

        // Verify task note
        let detail = quorum_core::tasks::get_with_notes(&conn, tid)
            .unwrap()
            .unwrap();
        assert_eq!(detail.notes.len(), 1);
        assert!(detail.notes[0].body.contains("mismatch"));
    }

    #[test]
    fn cx_label_mismatch_detected_cx_est_no_upgrade() {
        // cx_est=5, no labels -> resolved stays at config default, NOT upgraded
        let labels: Option<&str> = None;
        let (label_model, label_effort) = labels_to_model_effort(labels);
        let config_model = "claude-opus-4-6";
        let config_effort = "medium";
        let resolved_model = label_model.unwrap_or_else(|| config_model.into());
        let resolved_effort = label_effort.unwrap_or_else(|| config_effort.into());

        // Verify: resolved is config default (revert proof)
        assert_eq!(resolved_model, "claude-opus-4-6");
        assert_eq!(resolved_effort, "medium");

        // Mismatch should be detected
        let empty = std::collections::HashMap::new();
        let (sug_model, sug_effort) = suggested_for(5, &empty);
        assert!(
            is_model_effort_below(&resolved_model, &resolved_effort, &sug_model, &sug_effort),
            "cx 5 on opus-4-6/medium should trigger mismatch alert"
        );
    }

    #[test]
    fn cx_label_4_explicit_tier_high_no_alert() {
        let labels = Some(r#"["tier:opus-47","effort:high","complexity:4"]"#);
        let (label_model, label_effort) = labels_to_model_effort(labels);
        let resolved_model = label_model.unwrap_or_else(|| "claude-opus-4-6".into());
        let resolved_effort = label_effort.unwrap_or_else(|| "medium".into());

        let empty = std::collections::HashMap::new();
        let (sug_model, sug_effort) = suggested_for(4, &empty);
        assert!(
            !is_model_effort_below(&resolved_model, &resolved_effort, &sug_model, &sug_effort),
            "explicit tier:opus-47 effort:high should not trigger mismatch for cx 4"
        );
    }

    #[test]
    fn cx_2_default_no_alert() {
        let empty = std::collections::HashMap::new();
        let (sug_model, sug_effort) = suggested_for(2, &empty);
        assert!(
            !is_model_effort_below("claude-opus-4-6", "medium", &sug_model, &sug_effort),
            "cx 2 on opus-4-6/medium matches suggestion, no alert"
        );
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
        let proc = runner::RunnerProc::Claude(AgentProc::from_parts(child, stdin, reader));
        let now = Instant::now();
        SlotState {
            agent_name: "Test-1".into(),
            proc,
            task_id: 1,
            session_id: "sess".into(),
            model: "test-model".into(),
            effort: "high".into(),
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
            last_error_text: None,
            agent_run_id: None,
            cap_run_id: None,
            r2_origin: false,
            reviewed_head_sha: None,
            codex_thread_id: None,
            pending_prompt: "test prompt".into(),
            pending_turn_kind: "initial".into(),
        }
    }

    #[test]
    fn codex_continuation_requires_exact_run_identity() {
        let mut slot = make_dummy_slot();
        assert_eq!(
            codex_continuation_identity(&slot).unwrap(),
            ("test-model", "high")
        );
        slot.model.clear();
        assert_eq!(
            codex_continuation_identity(&slot).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        slot.model = "test-model".into();
        slot.effort.clear();
        assert_eq!(
            codex_continuation_identity(&slot).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn provider_retry_restores_exact_rework_turn_and_thread() {
        let feedback = "Fix stale-SHA handling; preserve this exact feedback.";
        let refs = serde_json::json!({
            "pr": 419,
            "codex_retry_requested": true,
            "codex_retry_model": "gpt-5.6-codex",
            "codex_retry_effort": "high",
            "codex_retry_prompt": feedback,
            "codex_retry_turn_kind": "rework",
            "codex_retry_thread_id": "thread-exact"
        })
        .to_string();
        let retry = codex_retry_turn(Some(&refs)).unwrap();
        assert_eq!(retry.prompt, feedback);
        assert_eq!(retry.turn_kind, "rework");
        assert_eq!(retry.thread_id.as_deref(), Some("thread-exact"));
        let args = codex_agent::resume_args(
            retry.thread_id.as_deref().unwrap(),
            &retry.model,
            &retry.effort,
            &retry.prompt,
        );
        assert_eq!(args[0..3], ["exec", "resume", "thread-exact"]);
        assert_eq!(args.last().unwrap(), feedback);
        assert!(!args.iter().any(|arg| arg.contains("You are agent")));
    }

    #[test]
    fn provider_retry_rejects_incomplete_durable_turn() {
        let refs = r#"{"codex_retry_requested":true,"codex_retry_model":"gpt"}"#;
        assert!(codex_retry_turn(Some(refs)).is_none());
    }

    #[test]
    fn transport_refeed_does_not_erase_rework_feedback() {
        let feedback = "REVIEW FAILED — fix the stale approval bug";
        assert!(should_replace_pending_prompt(feedback));
        assert!(!should_replace_pending_prompt(
            "Your previous turn was interrupted by a transport/API error (attempt 1/3)."
        ));
    }

    #[test]
    fn retry_metadata_survives_provider_event_and_db_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("retry.db");
        let refs = serde_json::json!({
            "pr": 419,
            "codex_retry_requested": true,
            "codex_retry_model": "gpt-exact",
            "codex_retry_effort": "high",
            "codex_retry_prompt": "exact rework feedback",
            "codex_retry_turn_kind": "rework",
            "codex_retry_thread_id": "thread-exact"
        })
        .to_string();
        {
            let mut conn = quorum_core::db::open(&db_path).unwrap();
            tasks::create(
                &mut conn,
                "owner",
                "retry",
                None,
                0,
                None,
                Some(&refs),
                None,
                None,
                10,
            )
            .unwrap();
        }

        let events = runner::normalize_codex_line(
            r#"{"type":"thread.started","thread_id":"replacement-thread"}"#,
        );
        assert!(matches!(
            events.as_slice(),
            [runner::AgentEvent::ThreadStarted { .. }]
        ));

        // Simulated daemon crash/restart: reopen the DB after provider output.
        let conn = quorum_core::db::open(&db_path).unwrap();
        let task = tasks::get(&conn, 1).unwrap().unwrap();
        let restored = codex_retry_turn(task.refs.as_deref()).unwrap();
        assert_eq!(restored.prompt, "exact rework feedback");
        assert_eq!(restored.thread_id.as_deref(), Some("thread-exact"));
        assert_eq!(restored.model, "gpt-exact");
        assert_eq!(restored.effort, "high");
    }

    #[test]
    fn rework_retry_reconstructs_rework_pushed_and_sticky_reviewer_effect() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("rework-retry.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let task_id = tasks::create(
            &mut conn, "owner", "retry", None, 0, None, None, None, None, 10,
        )
        .unwrap();
        tasks::claim(&mut conn, "worker", Some(task_id), &[], 3600, 11).unwrap();
        tasks::apply_event(
            &mut conn,
            "worker",
            task_id,
            &Event::SignaledDone { pr: "419".into() },
            12,
        )
        .unwrap();
        tasks::apply_event(
            &mut conn,
            "reviewer",
            task_id,
            &Event::ReviewerAttached {
                agent: "reviewer".into(),
            },
            13,
        )
        .unwrap();
        let rework = tasks::apply_event(&mut conn, "reviewer", task_id, &Event::VerdictChanges, 14)
            .unwrap()
            .task;
        assert_eq!(rework.status, "rework");

        let refs = serde_json::json!({
            "pr": 419,
            "codex_provider_blocked": true,
            "codex_provider_error": "quota",
            "codex_retry_model": "gpt-exact",
            "codex_retry_effort": "high",
            "codex_retry_prompt": "fix reviewer blocker",
            "codex_retry_turn_kind": "rework",
            "codex_retry_thread_id": "thread-r2"
        })
        .to_string();
        tasks::update_refs_daemon(&mut conn, task_id, &refs, 15).unwrap();
        let queued = tasks::retry_provider_blocked(&mut conn, task_id, "operator", 16)
            .unwrap()
            .unwrap();
        assert_eq!(queued.status, "rework");
        assert!(queued.assignee.is_none());
        let retry = codex_retry_turn(queued.refs.as_deref()).unwrap();
        let reconstructed_count = retry_slot_rework_count(queued.rework_round, Some(&retry), false);
        let event = worker_done_event(reconstructed_count, 419);
        assert!(matches!(event, Event::ReworkPushed));

        let reclaimed =
            tasks::claim_provider_retry_rework(&mut conn, "retry-worker", task_id, 3600, 17)
                .unwrap()
                .unwrap();
        assert_eq!(reclaimed.status, "rework");
        let transitioned =
            tasks::apply_event(&mut conn, "retry-worker", task_id, &event, 18).unwrap();
        assert_eq!(transitioned.task.status, "in-review");
        assert!(
            transitioned.effects.contains(&Effect::ResumeReviewer),
            "R2/sticky reviewer must be resumed after retried rework"
        );
    }

    #[test]
    fn initial_worker_retry_still_signals_done() {
        assert!(matches!(
            worker_done_event(0, 419),
            Event::SignaledDone { .. }
        ));
    }

    #[tokio::test]
    async fn provider_block_persistence_fails_loud_before_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let impossible = dir.path().join("missing-parent").join("quorum.db");
        let error = persist_codex_provider_block(
            &impossible,
            1,
            "quota",
            &CodexRetryTurn {
                model: "gpt-test".into(),
                effort: "high".into(),
                prompt: "continue exact work".into(),
                turn_kind: "rework".into(),
                thread_id: Some("thread-1".into()),
            },
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("provider-block")
                || error.to_string().contains("unable to open"),
            "unexpected durable-write error: {error}"
        );
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

    // ── next_needed_role / provision exhaustion tests (#190) ────────────

    #[test]
    fn next_needed_role_returns_r1_when_no_approvals() {
        let dir = tempfile::tempdir().unwrap();
        let conn = quorum_core::db::open(&dir.path().join("q.db")).unwrap();
        let role = next_needed_role(&conn, 42, "abc123").unwrap();
        assert_eq!(role, Some("r1"));
    }

    #[test]
    fn next_needed_role_returns_r2_when_r1_approved() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = quorum_core::db::open(&dir.path().join("q.db")).unwrap();
        quorum_core::approvals::record(
            &mut conn,
            &quorum_core::approvals::Approval {
                pr_number: 42,
                review_role: "r1".into(),
                task_id: 10,
                author: "worker".into(),
                reviewer: "rev1".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "abc123".into(),
            },
        )
        .unwrap();
        let role = next_needed_role(&conn, 42, "abc123").unwrap();
        assert_eq!(role, Some("r2"));
    }

    #[test]
    fn next_needed_role_returns_none_when_both_approved() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = quorum_core::db::open(&dir.path().join("q.db")).unwrap();
        for role in &["r1", "r2"] {
            quorum_core::approvals::record(
                &mut conn,
                &quorum_core::approvals::Approval {
                    pr_number: 42,
                    review_role: role.to_string(),
                    task_id: 10,
                    author: "worker".into(),
                    reviewer: format!("rev-{role}"),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    approved_head_sha: "abc123".into(),
                },
            )
            .unwrap();
        }
        let role = next_needed_role(&conn, 42, "abc123").unwrap();
        assert_eq!(role, None);
    }

    #[test]
    fn next_needed_role_returns_r1_when_sha_changed() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = quorum_core::db::open(&dir.path().join("q.db")).unwrap();
        quorum_core::approvals::record(
            &mut conn,
            &quorum_core::approvals::Approval {
                pr_number: 42,
                review_role: "r1".into(),
                task_id: 10,
                author: "worker".into(),
                reviewer: "rev1".into(),
                verdict: "approved".into(),
                blocking_count: 0,
                approved_head_sha: "old_sha".into(),
            },
        )
        .unwrap();
        let role = next_needed_role(&conn, 42, "new_sha").unwrap();
        assert_eq!(role, Some("r1"), "stale SHA must invalidate approval");
    }

    #[test]
    fn reviewer_cap_exceeded_at_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let conn = quorum_core::db::open(&dir.path().join("q.db")).unwrap();
        for i in 0..MAX_TOTAL_REVIEWER_RUNS {
            quorum_core::agent_runs::insert(
                &conn,
                1,
                &format!("rev-{i}"),
                "reviewer",
                "model",
                "high",
                "claude",
                100 + i,
            )
            .unwrap();
        }
        assert!(
            is_reviewer_cap_exceeded(&conn, 1).unwrap(),
            "should be exceeded at {MAX_TOTAL_REVIEWER_RUNS} runs"
        );
    }

    #[test]
    fn reviewer_cap_not_exceeded_below_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let conn = quorum_core::db::open(&dir.path().join("q.db")).unwrap();
        quorum_core::agent_runs::insert(
            &conn, 1, "rev-0", "reviewer", "model", "high", "claude", 100,
        )
        .unwrap();
        assert!(!is_reviewer_cap_exceeded(&conn, 1).unwrap());
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
    fn runner_tool_summary_via_normalize() {
        let line = r#"{"type":"tool_use","name":"Bash","input":{"command":"cargo test"}}"#;
        let events = runner::normalize_claude_line(line);
        assert_eq!(events.len(), 1);
        match &events[0] {
            runner::AgentEvent::Activity { summary, .. } => {
                assert!(summary.contains("Bash"), "summary={summary}");
                assert!(summary.contains("cargo test"), "summary={summary}");
            }
            other => panic!("expected Activity, got {other:?}"),
        }
    }

    #[test]
    fn runner_tool_summary_truncation() {
        let line = r#"{"type":"tool_use","name":"Bash","input":{"command":"cargo build --all-targets --features bundled"}}"#;
        let events = runner::normalize_claude_line(line);
        match &events[0] {
            runner::AgentEvent::Activity { summary, .. } => {
                assert!(summary.chars().count() <= 24, "summary too long: {summary}");
            }
            other => panic!("expected Activity, got {other:?}"),
        }
    }

    #[test]
    fn assistant_content_array_extracts_tool_use_via_runner() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Let me check that."},{"type":"tool_use","name":"Bash","input":{"command":"cargo test"}},{"type":"tool_use","name":"Read","input":{"file_path":"/foo/bar.rs"}}]}}"#;
        let events = runner::normalize_claude_line(line);
        let tool_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, runner::AgentEvent::Activity { .. }))
            .collect();
        assert_eq!(tool_events.len(), 2);
        match &tool_events[1] {
            runner::AgentEvent::Activity { summary, .. } => {
                assert!(summary.contains("Read"), "summary={summary}");
                assert!(summary.contains("bar.rs"), "summary={summary}");
            }
            _ => unreachable!(),
        }
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

    // ── PrTarget / resolve_pr_target ─────────────────────────────────────

    fn parse_pr_target_json(pr: i64, json: &str) -> Option<PrTarget> {
        let v: serde_json::Value = serde_json::from_str(json).ok()?;
        let head_ref = v.get("headRefName")?.as_str()?.to_string();
        let head_sha = v.get("headRefOid")?.as_str()?.to_string();
        let is_fork = v
            .get("isCrossRepository")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if head_ref.is_empty() || head_sha.is_empty() {
            return None;
        }
        Some(PrTarget {
            pr,
            head_ref,
            head_sha,
            is_fork,
        })
    }

    #[test]
    fn pr_target_same_repo() {
        let json = r#"{"headRefName":"daemon/lever-t189","headRefOid":"abc123","isCrossRepository":false}"#;
        let t = parse_pr_target_json(42, json).unwrap();
        assert_eq!(t.pr, 42);
        assert_eq!(t.head_ref, "daemon/lever-t189");
        assert_eq!(t.head_sha, "abc123");
        assert!(!t.is_fork);
    }

    #[test]
    fn pr_target_fork() {
        let json = r#"{"headRefName":"fix-bug","headRefOid":"def456","isCrossRepository":true}"#;
        let t = parse_pr_target_json(99, json).unwrap();
        assert!(t.is_fork);
        assert_eq!(t.head_ref, "fix-bug");
    }

    #[test]
    fn pr_target_missing_sha_returns_none() {
        let json = r#"{"headRefName":"branch","headRefOid":"","isCrossRepository":false}"#;
        assert!(parse_pr_target_json(1, json).is_none());
    }

    #[test]
    fn pr_target_missing_ref_returns_none() {
        let json = r#"{"headRefName":"","headRefOid":"abc123","isCrossRepository":false}"#;
        assert!(parse_pr_target_json(1, json).is_none());
    }

    #[test]
    fn pr_target_missing_fork_field_defaults_false() {
        let json = r#"{"headRefName":"branch","headRefOid":"abc123"}"#;
        let t = parse_pr_target_json(1, json).unwrap();
        assert!(!t.is_fork);
    }

    #[test]
    fn pr_target_revived_pr_uses_github_ref() {
        // Simulates task #202 reviving PR #3779 whose real head is t200, not t202
        let json = r#"{"headRefName":"daemon/alloy-hvjv-t200","headRefOid":"deadbeef","isCrossRepository":false}"#;
        let t = parse_pr_target_json(3779, json).unwrap();
        assert_eq!(t.head_ref, "daemon/alloy-hvjv-t200");
        assert_eq!(t.head_sha, "deadbeef");
    }

    // ── resolve_or_load_pr_target (persisted fallback) ───────────────────

    fn open_test_db(dir: &std::path::Path) -> quorum_core::Connection {
        quorum_core::db::open(&dir.join("q.db")).unwrap()
    }

    #[test]
    fn persisted_target_reused_when_gh_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = open_test_db(dir.path());
        pr_targets::upsert(&mut conn, 10, 42, "daemon/lever-t10", "abc123", false).unwrap();
        drop(conn);

        // gh will fail (fake repo_dir), so fallback kicks in
        let result = resolve_or_load_pr_target(42, 10, &db_path, dir.path(), Some("fake/repo"));
        let t = result.unwrap();
        assert_eq!(t.pr, 42);
        assert_eq!(t.head_ref, "daemon/lever-t10");
        assert_eq!(t.head_sha, "abc123");
        assert!(!t.is_fork);
    }

    #[test]
    fn persisted_fork_target_reused() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = open_test_db(dir.path());
        pr_targets::upsert(&mut conn, 10, 42, "fix-bug", "def456", true).unwrap();
        drop(conn);

        let t = resolve_or_load_pr_target(42, 10, &db_path, dir.path(), Some("fake/repo")).unwrap();
        assert!(t.is_fork);
        assert_eq!(t.head_ref, "fix-bug");
    }

    #[test]
    fn persisted_target_wrong_pr_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = open_test_db(dir.path());
        pr_targets::upsert(&mut conn, 10, 42, "branch", "sha1", false).unwrap();
        drop(conn);

        // PR 99 doesn't match persisted PR 42
        assert!(
            resolve_or_load_pr_target(99, 10, &db_path, dir.path(), Some("fake/repo")).is_none()
        );
    }

    #[test]
    fn no_persisted_target_no_gh_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let _conn = open_test_db(dir.path());

        // No persisted target, gh will fail
        assert!(
            resolve_or_load_pr_target(42, 10, &db_path, dir.path(), Some("fake/repo")).is_none()
        );
    }

    #[test]
    fn persisted_target_stale_head_still_returned() {
        // Stale head_sha is returned — SHA verification happens downstream
        // at worktree checkout, not here
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = open_test_db(dir.path());
        pr_targets::upsert(&mut conn, 10, 42, "branch", "old_sha", false).unwrap();
        drop(conn);

        let t = resolve_or_load_pr_target(42, 10, &db_path, dir.path(), Some("fake/repo")).unwrap();
        assert_eq!(t.head_sha, "old_sha");
    }

    #[test]
    fn review_only_no_gh_no_persisted_returns_none() {
        // Review-only tasks: orphan_worker_branch returns None AND no persisted
        // target → no worker spawned (the fail-safe)
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let _conn = open_test_db(dir.path());

        assert!(orphan_worker_branch("Anvil", 15, true).is_none());
        assert!(
            resolve_or_load_pr_target(42, 15, &db_path, dir.path(), Some("fake/repo")).is_none()
        );
    }

    #[test]
    fn review_only_with_persisted_target_succeeds() {
        // Review-only tasks with a persisted target CAN spawn
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = open_test_db(dir.path());
        pr_targets::upsert(&mut conn, 15, 42, "external/pr-branch", "sha999", false).unwrap();
        drop(conn);

        assert!(orphan_worker_branch("Anvil", 15, true).is_none());
        let t = resolve_or_load_pr_target(42, 15, &db_path, dir.path(), Some("fake/repo")).unwrap();
        assert_eq!(t.head_ref, "external/pr-branch");
    }

    #[test]
    fn persisted_target_updated_on_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("q.db");
        let mut conn = open_test_db(dir.path());
        pr_targets::upsert(&mut conn, 10, 42, "old-branch", "sha1", false).unwrap();
        pr_targets::upsert(&mut conn, 10, 42, "new-branch", "sha2", true).unwrap();
        drop(conn);

        let t = resolve_or_load_pr_target(42, 10, &db_path, dir.path(), Some("fake/repo")).unwrap();
        assert_eq!(t.head_ref, "new-branch");
        assert_eq!(t.head_sha, "sha2");
        assert!(t.is_fork);
    }

    // ── per-task provider routing (#195/#203) ────────────────────────────

    #[test]
    fn retry_turn_forces_provider_resolution_from_model() {
        // A Codex retry_turn with model "o3" must route to Codex even when
        // the daemon default is Claude (has_label_model=false).
        let retry_model = "o3";
        let kind = resolve_provider(retry_model).unwrap();
        assert_eq!(kind, runner::AgentKind::Codex);

        // And Claude retry models stay Claude.
        let claude_kind = resolve_provider("claude-opus-4-6").unwrap();
        assert_eq!(claude_kind, runner::AgentKind::Claude);
    }

    #[test]
    fn simultaneous_claude_and_codex_tasks_resolve_independently() {
        let claude_labels = Some(r#"["tier:opus-47"]"#);
        let codex_labels = Some(r#"["tier:o3"]"#);
        let (cm, _) = labels_to_model_effort(claude_labels);
        let (xm, _) = labels_to_model_effort(codex_labels);
        let ck = resolve_provider(cm.as_deref().unwrap()).unwrap();
        let xk = resolve_provider(xm.as_deref().unwrap()).unwrap();
        assert_eq!(ck, runner::AgentKind::Claude);
        assert_eq!(xk, runner::AgentKind::Codex);
    }

    #[test]
    fn no_model_label_falls_back_to_daemon_default() {
        let (m, _) = labels_to_model_effort(None);
        assert!(m.is_none(), "no label must produce no model override");
        let kind = runner_kind_to_agent_kind(crate::serve_config::RunnerKind::Claude);
        assert_eq!(kind, runner::AgentKind::Claude);
        let kind = runner_kind_to_agent_kind(crate::serve_config::RunnerKind::Codex);
        assert_eq!(kind, runner::AgentKind::Codex);
    }

    #[test]
    fn deterministic_provider_matrix_records_worker_r1_and_r2_routes() {
        struct Case {
            name: &'static str,
            default_runner: crate::serve_config::RunnerKind,
            config_model: &'static str,
            worker_model: &'static str,
            has_override: bool,
            expected: [&'static str; 3],
        }

        let cases = [
            Case {
                name: "claude-default",
                default_runner: crate::serve_config::RunnerKind::Claude,
                config_model: "claude-opus-4-6",
                worker_model: "claude-opus-4-6",
                has_override: false,
                expected: ["claude", "claude", "claude"],
            },
            Case {
                name: "all-codex",
                default_runner: crate::serve_config::RunnerKind::Codex,
                config_model: "o3",
                worker_model: "o3",
                has_override: false,
                expected: ["codex", "codex", "codex"],
            },
            Case {
                name: "mixed-codex-worker-claude-reviewers",
                default_runner: crate::serve_config::RunnerKind::Claude,
                config_model: "claude-opus-4-6",
                worker_model: "o3",
                has_override: true,
                expected: ["codex", "claude", "claude"],
            },
        ];

        for case in cases {
            let dir = tempfile::tempdir().unwrap();
            let db_path = dir.path().join(format!("{}.db", case.name));
            let mut conn = quorum_core::db::open(&db_path).unwrap();
            let task_id = tasks::create(
                &mut conn, "owner", case.name, None, 0, None, None, None, None, 10,
            )
            .unwrap();

            let worker_kind = resolve_worker_provider(
                case.worker_model,
                case.has_override,
                false,
                case.default_runner,
            )
            .unwrap();
            let reviewer_model = escalated_reviewer_model(case.worker_model, case.config_model);
            let reviewer_kind = resolve_provider(&reviewer_model).unwrap();

            quorum_core::agent_runs::insert(
                &conn,
                task_id,
                "worker",
                "worker",
                case.worker_model,
                "high",
                &worker_kind.to_string(),
                11,
            )
            .unwrap();
            quorum_core::agent_runs::insert(
                &conn,
                task_id,
                "r1",
                "reviewer",
                &reviewer_model,
                "high",
                &reviewer_kind.to_string(),
                12,
            )
            .unwrap();
            quorum_core::agent_runs::insert_r2(
                &conn,
                task_id,
                "r2",
                &reviewer_model,
                "high",
                &reviewer_kind.to_string(),
                13,
            )
            .unwrap();

            let runs = quorum_core::agent_runs::runs_for_task(&conn, task_id).unwrap();
            assert_eq!(
                runs.len(),
                3,
                "{} should record all lifecycle runs",
                case.name
            );
            assert_eq!(runs[0].role, "worker");
            assert_eq!(runs[1].role, "reviewer");
            assert_eq!(runs[1].sub_role, None);
            assert_eq!(runs[2].sub_role.as_deref(), Some("r2"));
            assert_eq!(
                [
                    runs[0].provider.as_deref().unwrap(),
                    runs[1].provider.as_deref().unwrap(),
                    runs[2].provider.as_deref().unwrap(),
                ],
                case.expected,
                "{} provider route changed",
                case.name
            );
        }
    }

    #[test]
    fn remediation_reopens_with_original_worker_provider_and_fresh_reviews() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("rework-provider.db");
        let task_id;
        {
            let mut conn = quorum_core::db::open(&db_path).unwrap();
            task_id = tasks::create(
                &mut conn,
                "owner",
                "mixed task",
                None,
                0,
                None,
                None,
                None,
                None,
                10,
            )
            .unwrap();
            quorum_core::agent_runs::insert(
                &conn, task_id, "worker-a", "worker", "o3", "high", "codex", 11,
            )
            .unwrap();
            quorum_core::agent_runs::insert(
                &conn,
                task_id,
                "r1-changes",
                "reviewer",
                "claude-opus-4-6",
                "high",
                "claude",
                12,
            )
            .unwrap();
        }

        // A daemon restart closes and reopens the sole source of truth.
        let conn = quorum_core::db::open(&db_path).unwrap();
        let recorded_provider = quorum_core::agent_runs::worker_provider(&conn, task_id).unwrap();
        let recorded_model = quorum_core::agent_runs::worker_model(&conn, task_id).unwrap();
        let (model, kind) = resolve_remediation_provider(
            recorded_provider.as_deref(),
            recorded_model,
            "claude-opus-4-6",
            crate::serve_config::RunnerKind::Claude,
        );
        assert_eq!(model, "o3");
        assert_eq!(
            kind,
            runner::AgentKind::Codex,
            "rework must not fall back to the daemon's Claude default"
        );

        quorum_core::agent_runs::insert(
            &conn,
            task_id,
            "worker-a",
            "worker",
            &model,
            "high",
            &kind.to_string(),
            13,
        )
        .unwrap();
        quorum_core::agent_runs::insert(
            &conn,
            task_id,
            "r1-fresh",
            "reviewer",
            "claude-opus-4-6",
            "high",
            "claude",
            14,
        )
        .unwrap();
        quorum_core::agent_runs::insert_r2(
            &conn,
            task_id,
            "r2-fresh",
            "claude-opus-4-6",
            "high",
            "claude",
            15,
        )
        .unwrap();

        let runs = quorum_core::agent_runs::runs_for_task(&conn, task_id).unwrap();
        assert_eq!(runs.len(), 5);
        assert_eq!(
            runs.iter()
                .filter(|run| run.role == "worker")
                .map(|run| (
                    run.agent.as_str(),
                    run.model.as_str(),
                    run.provider.as_deref()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("worker-a", "o3", Some("codex")),
                ("worker-a", "o3", Some("codex")),
            ],
            "changes must resume the same worker identity, model, and provider"
        );
        assert_eq!(runs[3].agent, "r1-fresh");
        assert_eq!(runs[4].sub_role.as_deref(), Some("r2"));
    }

    #[test]
    fn codex_usd_guard_condition_fires_on_task_cost() {
        let limits = CostLimits {
            max_task_cost_usd: Some(10.0),
            ..Default::default()
        };
        let kind = runner::AgentKind::Codex;
        let should_reject = kind == runner::AgentKind::Codex && limits.max_task_cost_usd.is_some();
        assert!(should_reject);
    }

    #[test]
    fn codex_usd_guard_condition_fires_on_turn_cost() {
        let limits = CostLimits {
            max_turn_cost_usd: Some(2.0),
            ..Default::default()
        };
        let kind = runner::AgentKind::Codex;
        let should_reject = kind == runner::AgentKind::Codex && limits.max_turn_cost_usd.is_some();
        assert!(should_reject);
    }

    #[test]
    fn codex_usd_guard_allows_token_and_wall_limits() {
        let limits = CostLimits {
            max_task_tokens: Some(100_000),
            max_turn_wall_secs: Some(600),
            ..Default::default()
        };
        let kind = runner::AgentKind::Codex;
        let should_reject = kind == runner::AgentKind::Codex
            && (limits.max_task_cost_usd.is_some() || limits.max_turn_cost_usd.is_some());
        assert!(
            !should_reject,
            "token/wall limits must not trigger USD guard"
        );
    }

    #[test]
    fn claude_never_rejected_by_codex_usd_guard() {
        let limits = CostLimits {
            max_task_cost_usd: Some(10.0),
            max_turn_cost_usd: Some(2.0),
            ..Default::default()
        };
        let kind = runner::AgentKind::Claude;
        let should_reject = kind == runner::AgentKind::Codex
            && (limits.max_task_cost_usd.is_some() || limits.max_turn_cost_usd.is_some());
        assert!(!should_reject);
    }

    #[tokio::test]
    async fn codex_usd_rejection_parks_task_durably() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("usd-guard.db");
        {
            let mut conn = quorum_core::db::open(&db_path).unwrap();
            tasks::create(
                &mut conn,
                "owner",
                "codex task",
                None,
                0,
                None,
                None,
                None,
                None,
                10,
            )
            .unwrap();
        }
        let retry = CodexRetryTurn {
            model: "o3".into(),
            effort: "high".into(),
            prompt: "continue exact work".into(),
            turn_kind: "rework".into(),
            thread_id: Some("thread-99".into()),
        };
        persist_codex_provider_block(&db_path, 1, "Codex+USD cost limit incompatible", &retry)
            .await
            .unwrap();
        let conn = quorum_core::db::open(&db_path).unwrap();
        let task = tasks::get(&conn, 1).unwrap().unwrap();
        let refs: serde_json::Value = serde_json::from_str(task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["codex_provider_blocked"].as_bool(), Some(true));
        assert!(
            refs["codex_provider_error"]
                .as_str()
                .unwrap()
                .contains("USD"),
            "error must mention USD"
        );
        assert_eq!(refs["codex_retry_model"].as_str(), Some("o3"));
        assert_eq!(
            refs["codex_retry_thread_id"].as_str(),
            Some("thread-99"),
            "thread identity must survive parking"
        );
    }

    #[tokio::test]
    async fn codex_thread_id_from_refs_reads_both_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("thread.db");
        {
            let mut conn = quorum_core::db::open(&db_path).unwrap();
            tasks::create(
                &mut conn, "owner", "t1", None, 0, None, None, None, None, 10,
            )
            .unwrap();
            // Set codex_retry_thread_id via refs
            let refs = r#"{"codex_retry_thread_id":"tid-abc"}"#;
            tasks::update_refs_daemon(&mut conn, 1, refs, now_unix()).unwrap();
        }
        let tid = codex_thread_id_from_refs(&db_path, 1).await;
        assert_eq!(tid.as_deref(), Some("tid-abc"));
    }

    #[tokio::test]
    async fn codex_thread_id_from_refs_prefers_primary_key() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("thread2.db");
        {
            let mut conn = quorum_core::db::open(&db_path).unwrap();
            tasks::create(
                &mut conn, "owner", "t1", None, 0, None, None, None, None, 10,
            )
            .unwrap();
            let refs = r#"{"codex_thread_id":"primary","codex_retry_thread_id":"fallback"}"#;
            tasks::update_refs_daemon(&mut conn, 1, refs, now_unix()).unwrap();
        }
        let tid = codex_thread_id_from_refs(&db_path, 1).await;
        assert_eq!(tid.as_deref(), Some("primary"));
    }
}
