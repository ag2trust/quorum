//! Bounded task-decomposition planner provider and protocol boundary.

// This foundation module is exercised directly by its contract tests. The daemon coordinator
// integration will consume the runtime API in the next implementation slice.
#![allow(dead_code)]

use super::agent::{self, AgentProc, AgentSpec};
use super::codex_agent::{CodexProc, CodexSpec};
use super::runner::{AgentEvent, AgentKind, CapturedOutput, RunnerFailure, RunnerProc};
use super::session_log::{
    ProviderLifecyclePhase, SanitizedCommandKind, SanitizedCompletionOutcome, SanitizedField,
    SanitizedProvider, SanitizedProviderFailureKind, SanitizedRejectionKind, SanitizedSessionEvent,
    SanitizedSummaryOutcome, SanitizedTerminalStatus, SanitizedToolKind, SessionLog,
    TurnLifecyclePhase, MAX_SANITIZED_RECORDS_PER_SESSION,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const CODEX_PLANNER_MODEL: &str = "gpt-5.6-sol";
pub const CLAUDE_PLANNER_MODEL: &str = "claude-opus-4-6";
pub const PLANNER_EFFORT: &str = "high";
pub const PLANNER_TIMEOUT: Duration = Duration::from_secs(600);
pub const MAX_STDOUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_PROMPT_BYTES: usize = 128 * 1024;
const MAX_FAILURE_SUMMARY_BYTES: usize = 2048;
const MAX_FAILURE_REASON_BYTES: usize = 256;
const DIAGNOSTIC_SAMPLE_LINES: usize = 2;
const WRITABLE_PATH_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(1);
pub const WORKER_WRITABILITY_GUIDANCE: &str = "Worker guidance: only the assigned worktree and repository are writable. This defense-in-depth guidance does not itself enforce that boundary.";
const MAX_TEXT_BYTES: usize = 8 * 1024;
const MAX_LIST_ITEMS: usize = 32;
const MAX_REJECTION_SUMMARIES: usize = 3;
pub(super) const MAX_REJECTION_SUMMARY_BYTES: usize = 1024;
// A clean terminal provider line followed by no accepted `submit_plan` emits
// one terminal response and four closure records. Provider failures and their
// final outcome share this fixed reserve.
const PLANNER_SANITIZED_CLOSURE_RESERVE: usize = 5;

/// Process-local admission gate for filesystem-backed write-path resolution.
///
/// The resolver runs on at most one dedicated OS thread, never Tokio's shared
/// blocking pool. A timed-out filesystem call retains this single slot until
/// it really returns; later requests fail closed without spawning or queueing
/// more work.
#[derive(Clone, Default)]
pub struct WritablePathResolver {
    active: Arc<AtomicBool>,
}

struct WritablePathResolutionGuard(Arc<AtomicBool>);

impl Drop for WritablePathResolutionGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl WritablePathResolver {
    async fn resolve_with<F>(
        &self,
        repo_root: std::path::PathBuf,
        writable_paths: Vec<String>,
        resolution_timeout: Duration,
        resolver: F,
    ) -> bool
    where
        F: FnOnce(std::path::PathBuf, Vec<String>) -> bool + Send + 'static,
    {
        if self
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }

        let guard = WritablePathResolutionGuard(Arc::clone(&self.active));
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("quorum-write-path-resolver".into())
            .spawn(move || {
                let _guard = guard;
                let _ = result_tx.send(resolver(repo_root, writable_paths));
            });
        if spawned.is_err() {
            return false;
        }

        matches!(
            tokio::time::timeout(resolution_timeout, result_rx).await,
            Ok(Ok(true))
        )
    }

    #[cfg(test)]
    fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "outcome", rename_all = "lowercase", deny_unknown_fields)]
pub enum PlannerResponse {
    Plan {
        tasks: Vec<ProposedTask>,
    },
    Blocker {
        category: String,
        evidence: Vec<String>,
        required_decision: String,
        why_no_safe_split: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedTask {
    pub key: String,
    pub title: String,
    #[serde(default)]
    pub implementation_delta: String,
    #[serde(default)]
    pub affected_paths: Vec<String>,
    pub observable_outcome: String,
    /// Explicit child file contract. Writes and contextual references are
    /// distinct so downstream enforcement need not infer intent from prose.
    #[serde(default)]
    pub deliverables: quorum_core::decomposition::ChildDeliverables,
    pub acceptance_criteria: Vec<String>,
    pub source_constraints: Vec<String>,
    pub verification_expectations: Vec<String>,
    #[serde(default)]
    pub non_goals: Vec<String>,
    #[serde(default)]
    pub prerequisites: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanningSource<'a> {
    pub task_id: i64,
    pub revision: i64,
    pub title: &'a str,
    pub body: Option<&'a str>,
    pub dependencies: &'a [i64],
}

/// The two closed response shapes a planner may produce.
///
/// The planner prompt and the `submit_plan` MCP tool description must state the
/// same shapes; both render this one text so the two surfaces cannot drift.
pub const RESPONSE_SHAPES: &str = concat!(
    "The object must be exactly one of these closed shapes: do not omit, rename, or add fields.\n",
    r#"PLAN={"outcome":"plan","tasks":[{"key":"<lowercase-ascii-key>","title":"<title>","implementation_delta":"<new-code-or-documentation-change>","affected_paths":["<repo-relative-path-or-narrow-pattern>"],"observable_outcome":"<observable-outcome>","deliverables":[{"kind":"write","path":"<repo-relative-path>"},{"kind":"read_only_reference","path":"<contextual-path>"}],"acceptance_criteria":["<criterion>"],"source_constraints":["<constraint>"],"verification_expectations":["<verification>"],"non_goals":["<preserved-or-explicitly-excluded-behavior>"],"prerequisites":["<task-key-or-source:positive-id>"]}]}"#,
    "\n",
    r#"BLOCKER={"outcome":"blocker","category":"<ambiguous_scope|missing_decision|external_constraint|no_safe_split>","evidence":["<evidence>"],"required_decision":"<decision>","why_no_safe_split":"<reason>"}"#,
    "\n",
    "`outcome` must be exactly `plan` or `blocker`; use PLAN only when it has 2-8 tasks.",
);

/// Build one bounded, closed-book planner turn. Retry context is deliberately
/// limited to structured rejection summaries; provider transcripts and prior
/// continuation identities never enter a later attempt.
pub fn build_prompt(source: &PlanningSource<'_>, rejection_summaries: &[String]) -> String {
    let source_json = serde_json::to_string(source).expect("planning source serializes");
    let retry_json = serde_json::to_string(
        &rejection_summaries
            .iter()
            .take(MAX_REJECTION_SUMMARIES)
            .map(|summary| truncate_utf8(summary, MAX_REJECTION_SUMMARY_BYTES).to_owned())
            .collect::<Vec<_>>(),
    )
    .expect("rejection summaries serialize");
    format!(
        "You are Quorum's repository-grounded implementation-boundary planner. Produce one \
         closed DAG of 2-8 independently deliverable implementation tasks, each size S or M. \
         Identify concrete implementation deltas and split at real code or ownership seams; do \
         not turn each desired product outcome into a separate task. Preserved behavior, \
         compatibility requirements, and regression-only expectations belong in acceptance \
         criteria or non_goals, not standalone implementation tasks. Multiple files may belong \
         to one child when they implement one coherent seam; split a child when it combines \
         independently deliverable changes across layers or components. The execution-size \
         rubric is: S = focused/local; M = bounded coherent work; L = broad cross-component \
         coherent delivery; XL = compound work needing decomposition. Do not estimate human time. \
         Inspect the repository only to ground the plan: start with source-named paths and symbols, \
         use targeted Grep or Glob, read focused excerpts, follow observed calls at most one hop, \
         and stop once each child has a concrete delta and path set. Use at most 5 Grep/Glob calls \
         and 10 Read calls. Do not browse unrelated directories or read whole large files. \
         Preserve every source constraint; carry each source requirement, constraint, and \
         verbatim value forward faithfully in the child that owns it. A separate plan-review \
         Arbiter judges that faithfulness, so preserve meaning and load-bearing detail rather \
         than echoing byte-exact literals. Every source requirement must be represented by a child delta, criterion, \
         constraint, verification expectation, or non-goal. Do not create synthetic integration \
         work, recursive planning, no-op/regression-only tasks, or unrelated scope. Dependencies \
         must be real delivery prerequisites and may \
         reference another task key or source:<dependency-id>. Every PLAN task's \
         `source_constraints` must include this worker-facing guidance: \
         \"{WORKER_WRITABILITY_GUIDANCE}\". The daemon adds it deterministically, so it is \
         guidance rather than an enforcement claim. For \
         each task, declare every file-level deliverable in `deliverables`: use \
         `write` only for requested changes and `read_only_reference` only for context. \
         Report by calling the `submit_plan` tool exactly once with the PLAN or BLOCKER \
         object as its `response` argument. The tool answers with the daemon's own \
         validation errors: fix the reported defect and call `submit_plan` again in the same \
         turn. `already_submitted` means your first plan was accepted and stands, so stop. \
         Never print the plan as text: written output is not read, and a turn that ends \
         without an accepted `submit_plan` call is a failed attempt. {RESPONSE_SHAPES} \
         PRIOR_REJECTIONS contains at most {MAX_REJECTION_SUMMARIES} summaries, each truncated \
         to {MAX_REJECTION_SUMMARY_BYTES} bytes. On retry, use only those summaries to correct \
         the cited semantic defect; do not discuss them or request more context.\n\nSOURCE={source_json}\n\nPRIOR_REJECTIONS={retry_json}"
    )
}

/// Add the worker-facing defense-in-depth guidance to every generated child,
/// independent of the planner's response.
pub fn with_worker_writability_guidance(source_constraints: &[String]) -> Vec<String> {
    let mut constraints = source_constraints.to_vec();
    if !contains_worker_writability_guidance(source_constraints) {
        constraints.push(WORKER_WRITABILITY_GUIDANCE.into());
    }
    constraints
}

fn contains_worker_writability_guidance(source_constraints: &[String]) -> bool {
    source_constraints
        .iter()
        .any(|constraint| constraint == WORKER_WRITABILITY_GUIDANCE)
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

/// Recheck proposal dependencies against the authoritative source snapshot.
/// Shape/cycle/text validation runs first, in `validate_semantics`.
pub async fn validate_for_source(
    tasks: &[ProposedTask],
    source_dependency_ids: &[i64],
    repo_root: &Path,
    path_resolver: &WritablePathResolver,
) -> Result<(), PlannerParseError> {
    validate_plan_tasks(tasks)?;
    validate_for_source_with_resolver(
        tasks,
        source_dependency_ids,
        repo_root,
        path_resolver,
        WRITABLE_PATH_RESOLUTION_TIMEOUT,
        |repo_root, paths| {
            paths.iter().all(|path| {
                quorum_core::decomposition::classify_writable_deliverable_path_blocking(
                    &repo_root, path,
                ) == quorum_core::decomposition::WritableDeliverablePath::Permitted
            })
        },
    )
    .await
}

async fn validate_for_source_with_resolver<F>(
    tasks: &[ProposedTask],
    source_dependency_ids: &[i64],
    repo_root: &Path,
    path_resolver: &WritablePathResolver,
    resolution_timeout: Duration,
    resolver: F,
) -> Result<(), PlannerParseError>
where
    F: FnOnce(std::path::PathBuf, Vec<String>) -> bool + Send + 'static,
{
    let allowed: HashSet<i64> = source_dependency_ids.iter().copied().collect();
    let mut writable_paths = Vec::new();
    for task in tasks {
        for path in task.deliverables.writable_paths() {
            if quorum_core::decomposition::writable_deliverable_is_lexically_external(
                repo_root, path,
            ) {
                return escaping_write(task, path);
            }
            writable_paths.push(path.to_owned());
        }
        for prerequisite in &task.prerequisites {
            if let Some(raw) = prerequisite.strip_prefix("source:") {
                let id = raw
                    .parse::<i64>()
                    .map_err(|_| PlannerParseError::Semantic("invalid source dependency".into()))?;
                if !allowed.contains(&id) {
                    return semantic("proposal references a dependency outside the source");
                }
            }
        }
        let synthetic = format!("{} {}", task.title, task.observable_outcome).to_lowercase();
        if ["integration task", "integrate all", "merge all siblings"]
            .iter()
            .any(|phrase| synthetic.contains(phrase))
        {
            return semantic("synthetic integration work is not permitted");
        }
    }
    if writable_paths.is_empty() {
        return Ok(());
    }
    let permitted = path_resolver
        .resolve_with(
            repo_root.to_path_buf(),
            writable_paths,
            resolution_timeout,
            resolver,
        )
        .await;
    if permitted {
        Ok(())
    } else {
        semantic(
            "a writable deliverable resolves outside the managed repository; use an \
             in-repository write path or declare external context as read_only_reference",
        )
    }
}

fn escaping_write<T>(task: &ProposedTask, path: &str) -> Result<T, PlannerParseError> {
    const MAX_PATH_BYTES: usize = 512;
    const ELLIPSIS: &str = "…";
    let path = if path.len() > MAX_PATH_BYTES {
        format!(
            "{}{}",
            truncate_utf8(path, MAX_PATH_BYTES - ELLIPSIS.len()),
            ELLIPSIS
        )
    } else {
        path.to_owned()
    };
    let message = format!(
        "child {} writable deliverable `{path}` escapes the managed repository; use an \
         in-repository write path or declare external context as read_only_reference",
        task.key
    );
    debug_assert!(message.len() <= MAX_REJECTION_SUMMARY_BYTES);
    semantic(&message)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerParseError {
    Provider(String),
    Semantic(String),
}

impl std::fmt::Display for PlannerParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider(s) => write!(f, "provider failure: {s}"),
            Self::Semantic(s) => write!(f, "semantic rejection: {s}"),
        }
    }
}

/// Rehydrate one durably accepted `submit_plan` submission.
///
/// The endpoint already applied `validate_semantics` and `validate_for_source`
/// before storing the response, so this only decodes the stored bytes; it is
/// the same closed-type deserialization `rehydrate_accepted_proposal` performs
/// for a durable proposal. Provider text is never a source here.
pub(super) fn rehydrate_submitted_response(
    text: &str,
) -> Result<PlannerResponse, PlannerParseError> {
    serde_json::from_str(text).map_err(|error| {
        PlannerParseError::Provider(format!("invalid accepted plan submission: {error}"))
    })
}

/// Rehydrate the durable task-list form accepted by the planner coordinator.
/// This shares `ProposedTask` with the live response parser, so a persisted
/// proposal receives the complete closed-plan semantic validation before any
/// downstream phase resumes.
pub fn parse_accepted_proposal(text: &str) -> Result<Vec<ProposedTask>, PlannerParseError> {
    let tasks = rehydrate_accepted_proposal(text)?;
    validate_plan_tasks(&tasks)?;
    Ok(tasks)
}

/// Decode a durable proposal before semantic revalidation. Compatibility
/// defaults let the coordinator recover proposals written before required
/// fields were introduced; `validate_plan_tasks` rejects those defaults and
/// returns the graph to planning before classification or materialization.
pub(super) fn rehydrate_accepted_proposal(
    text: &str,
) -> Result<Vec<ProposedTask>, PlannerParseError> {
    let tasks: Vec<ProposedTask> = serde_json::from_str(text).map_err(|error| {
        PlannerParseError::Provider(format!("invalid accepted proposal: {error}"))
    })?;
    Ok(tasks)
}

pub(crate) fn validate_semantics(response: &PlannerResponse) -> Result<(), PlannerParseError> {
    match response {
        PlannerResponse::Blocker {
            category,
            evidence,
            required_decision,
            why_no_safe_split,
        } => {
            const CATEGORIES: &[&str] = &[
                "ambiguous_scope",
                "missing_decision",
                "external_constraint",
                "no_safe_split",
            ];
            if !CATEGORIES.contains(&category.as_str()) {
                return semantic("unsupported blocker category");
            }
            validate_list("blocker evidence", evidence, 1)?;
            validate_text("required decision", required_decision)?;
            validate_text("why no safe split", why_no_safe_split)?;
        }
        PlannerResponse::Plan { tasks } => validate_plan_tasks(tasks)?,
    }
    Ok(())
}

pub(super) fn validate_plan_tasks(tasks: &[ProposedTask]) -> Result<(), PlannerParseError> {
    if !(2..=8).contains(&tasks.len()) {
        return semantic("plan must contain between 2 and 8 tasks");
    }
    let mut keys = HashSet::new();
    for task in tasks {
        validate_key(&task.key)?;
        if !keys.insert(task.key.as_str()) {
            return semantic("task keys must be unique");
        }
        validate_text("title", &task.title)?;
        validate_text("implementation delta", &task.implementation_delta)?;
        validate_list("affected paths", &task.affected_paths, 1)?;
        validate_text("observable outcome", &task.observable_outcome)?;
        validate_deliverables(&task.deliverables)?;
        validate_list("acceptance criteria", &task.acceptance_criteria, 1)?;
        validate_list("source constraints", &task.source_constraints, 1)?;
        if task.source_constraints.len() == MAX_LIST_ITEMS
            && !contains_worker_writability_guidance(&task.source_constraints)
        {
            return semantic(
                "source constraints at maximum size must include worker writability guidance",
            );
        }
        validate_list(
            "verification expectations",
            &task.verification_expectations,
            1,
        )?;
        validate_list("non-goals", &task.non_goals, 1)?;
        if task.prerequisites.len() > MAX_LIST_ITEMS {
            return semantic("too many prerequisites");
        }
    }
    for task in tasks {
        for prerequisite in &task.prerequisites {
            validate_text("prerequisite", prerequisite)?;
            if prerequisite == &task.key {
                return semantic("task cannot depend on itself");
            }
            if !keys.contains(prerequisite.as_str()) && !valid_source_dependency(prerequisite) {
                return semantic("prerequisite must be a task key or source:<positive-id>");
            }
        }
    }
    reject_cycles(tasks)
}

fn validate_deliverables(
    deliverables: &quorum_core::decomposition::ChildDeliverables,
) -> Result<(), PlannerParseError> {
    if deliverables.0.is_empty() || deliverables.0.len() > MAX_LIST_ITEMS {
        return semantic(&format!(
            "deliverables must contain 1..={MAX_LIST_ITEMS} items"
        ));
    }
    for deliverable in &deliverables.0 {
        let path = match deliverable {
            quorum_core::decomposition::ChildDeliverable::Write { path }
            | quorum_core::decomposition::ChildDeliverable::ReadOnlyReference { path } => path,
        };
        validate_text("deliverable path", path)?;
    }
    Ok(())
}

fn semantic<T>(message: &str) -> Result<T, PlannerParseError> {
    Err(PlannerParseError::Semantic(message.into()))
}

fn validate_text(label: &str, value: &str) -> Result<(), PlannerParseError> {
    if value.trim().is_empty() {
        return semantic(&format!("{label} must not be empty"));
    }
    if value.len() > MAX_TEXT_BYTES {
        return semantic(&format!("{label} exceeds {MAX_TEXT_BYTES} bytes"));
    }
    Ok(())
}

fn validate_list(label: &str, values: &[String], minimum: usize) -> Result<(), PlannerParseError> {
    if values.len() < minimum || values.len() > MAX_LIST_ITEMS {
        return semantic(&format!(
            "{label} must contain {minimum}..={MAX_LIST_ITEMS} items"
        ));
    }
    for value in values {
        validate_text(label, value)?;
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<(), PlannerParseError> {
    if key.is_empty()
        || key.len() > 64
        || !key
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return semantic("task key must be 1-64 lowercase ASCII letters, digits, '-' or '_'");
    }
    Ok(())
}

fn valid_source_dependency(value: &str) -> bool {
    value
        .strip_prefix("source:")
        .and_then(|id| id.parse::<i64>().ok())
        .is_some_and(|id| id > 0)
}

fn reject_cycles(tasks: &[ProposedTask]) -> Result<(), PlannerParseError> {
    let by_key: HashMap<&str, &ProposedTask> = tasks.iter().map(|t| (t.key.as_str(), t)).collect();
    fn visit<'a>(
        key: &'a str,
        by_key: &HashMap<&'a str, &'a ProposedTask>,
        visiting: &mut HashSet<&'a str>,
        visited: &mut HashSet<&'a str>,
    ) -> bool {
        if visited.contains(key) {
            return false;
        }
        if !visiting.insert(key) {
            return true;
        }
        if let Some(task) = by_key.get(key) {
            for dependency in &task.prerequisites {
                if by_key.contains_key(dependency.as_str())
                    && visit(dependency, by_key, visiting, visited)
                {
                    return true;
                }
            }
        }
        visiting.remove(key);
        visited.insert(key);
        false
    }
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    if tasks
        .iter()
        .any(|task| visit(&task.key, &by_key, &mut visiting, &mut visited))
    {
        return semantic("plan contains a dependency cycle");
    }
    Ok(())
}

pub struct PlannerSlot {
    pub proc: RunnerProc,
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub usage: super::runner::TokenUsage,
    started_at: tokio::time::Instant,
    stdout_bytes: usize,
    codex_terminal_candidate: bool,
    diagnostics: PlannerDiagnostics,
    session_log: Option<SessionLog>,
    sanitized_record_count: usize,
}

#[derive(Default)]
struct PlannerDiagnostics {
    lines: u64,
    event_types: BTreeMap<&'static str, u64>,
    beginning: Vec<String>,
    end: VecDeque<String>,
    terminal_response_seen: bool,
    read_boundary_truncated: bool,
}

impl PlannerDiagnostics {
    fn observe_line(&mut self, provider: AgentKind, raw: &str) {
        self.lines = self.lines.saturating_add(1);
        let event_type = safe_event_type(provider, raw);
        *self.event_types.entry(event_type).or_default() += 1;
        self.terminal_response_seen |=
            matches!(event_type, "result" | "turn.completed" | "turn.failed");

        // Samples deliberately retain only a structural description. Provider
        // payload strings (including tool output and assistant text) can contain
        // inherited credentials, so they never enter a durable diagnostic.
        let sample = format!("line={} event={event_type} bytes={}", self.lines, raw.len());
        if self.beginning.len() < DIAGNOSTIC_SAMPLE_LINES {
            self.beginning.push(sample.clone());
        }
        if self.end.len() == DIAGNOSTIC_SAMPLE_LINES {
            self.end.pop_front();
        }
        self.end.push_back(sample);
    }

    fn note_read_boundary_truncation(&mut self) {
        self.read_boundary_truncated = true;
    }
}

fn safe_event_type(provider: AgentKind, raw: &str) -> &'static str {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return if raw.trim().is_empty() {
            "blank"
        } else {
            "malformed-json"
        };
    };
    let event_type = value.get("type").and_then(serde_json::Value::as_str);
    match (provider, event_type) {
        (AgentKind::Codex, Some("thread.started")) => "thread.started",
        (AgentKind::Codex, Some("turn.started")) => "turn.started",
        (AgentKind::Codex, Some("turn.completed")) => "turn.completed",
        (AgentKind::Codex, Some("turn.failed")) => "turn.failed",
        (AgentKind::Codex, Some("error")) => "error",
        (AgentKind::Codex, Some("item.started" | "item.completed")) => {
            match (
                event_type,
                value
                    .get("item")
                    .and_then(|item| item.get("type"))
                    .and_then(serde_json::Value::as_str),
            ) {
                (Some("item.started"), Some("agent_message")) => "item.started/agent_message",
                (Some("item.started"), Some("command_execution")) => {
                    "item.started/command_execution"
                }
                (Some("item.started"), Some("file_change")) => "item.started/file_change",
                (Some("item.started"), Some("mcp_call")) => "item.started/mcp_call",
                (Some("item.started"), Some("error")) => "item.started/error",
                (Some("item.completed"), Some("agent_message")) => "item.completed/agent_message",
                (Some("item.completed"), Some("command_execution")) => {
                    "item.completed/command_execution"
                }
                (Some("item.completed"), Some("file_change")) => "item.completed/file_change",
                (Some("item.completed"), Some("mcp_call")) => "item.completed/mcp_call",
                (Some("item.completed"), Some("error")) => "item.completed/error",
                (Some("item.started"), _) => "item.started/other",
                _ => "item.completed/other",
            }
        }
        (AgentKind::Claude, Some("assistant")) => "assistant",
        (AgentKind::Claude, Some("tool_use")) => "tool_use",
        (AgentKind::Claude, Some("result")) => "result",
        (AgentKind::Claude, Some("system")) => "system",
        (AgentKind::Grok, Some("end")) => "end",
        (_, Some(_)) => "other-json-event",
        (_, None) => "json-without-type",
    }
}

impl PlannerSlot {
    pub fn pid(&self) -> Option<i32> {
        self.proc.pid()
    }

    pub fn start_session_log(
        &mut self,
        log_dir: &Path,
        agent: &str,
        task_id: i64,
        session_id: &str,
        branch: &str,
        started_at: i64,
    ) -> std::io::Result<()> {
        self.session_log = Some(SessionLog::create(
            log_dir,
            agent,
            "planner",
            Some(task_id),
            session_id,
            branch,
            started_at,
        )?);
        self.sanitized_record_count = 0;
        self.log_sanitized_events(&[
            SanitizedSessionEvent::ProviderLifecycle {
                provider: sanitized_provider(self.proc.kind()),
                phase: ProviderLifecyclePhase::Started,
            },
            SanitizedSessionEvent::TurnLifecycle {
                turn: 1,
                phase: TurnLifecyclePhase::Started,
            },
        ]);
        Ok(())
    }

    pub fn log_dir(&self) -> Option<&Path> {
        self.session_log.as_ref().map(|log| log.dir())
    }

    fn log_sanitized_events(&mut self, events: &[SanitizedSessionEvent]) {
        self.log_sanitized_events_with_reserve(events, true);
    }

    fn log_sanitized_closure_events(&mut self, events: &[SanitizedSessionEvent]) {
        self.log_sanitized_events_with_reserve(events, false);
    }

    fn log_sanitized_events_with_reserve(
        &mut self,
        events: &[SanitizedSessionEvent],
        reserve_closure_capacity: bool,
    ) {
        if let Some(session_log) = self.session_log.as_mut() {
            for event in events {
                if reserve_closure_capacity
                    && self.sanitized_record_count
                        >= MAX_SANITIZED_RECORDS_PER_SESSION
                            .saturating_sub(PLANNER_SANITIZED_CLOSURE_RESERVE)
                {
                    break;
                }
                if session_log.log_sanitized_event(event) {
                    self.sanitized_record_count = self.sanitized_record_count.saturating_add(1);
                }
            }
        }
    }

    fn log_sanitized_line(&mut self, raw: &str) {
        if self.session_log.is_none() {
            return;
        }
        let event_type = safe_event_type(self.proc.kind(), raw);
        let details = SanitizedField::from_json_text(raw);
        let provider = sanitized_provider(self.proc.kind());
        let event = match event_type {
            "thread.started" => SanitizedSessionEvent::ProviderLifecycle {
                provider,
                phase: ProviderLifecyclePhase::Ready,
            },
            "turn.started" => SanitizedSessionEvent::TurnLifecycle {
                turn: 1,
                phase: TurnLifecyclePhase::Started,
            },
            "turn.completed" | "result" | "end" => SanitizedSessionEvent::TerminalResponse {
                status: terminal_status(event_type, raw),
                response: details,
            },
            "turn.failed"
            | "error"
            | "item.started/error"
            | "item.completed/error"
            | "malformed-json" => SanitizedSessionEvent::ProviderFailure {
                provider,
                kind: SanitizedProviderFailureKind::Protocol,
                details,
            },
            "item.started/command_execution" | "item.completed/command_execution" => {
                SanitizedSessionEvent::CommandSummary {
                    command: SanitizedCommandKind::Shell,
                    outcome: summary_outcome(event_type),
                    details,
                }
            }
            "item.started/file_change" | "item.completed/file_change" => {
                SanitizedSessionEvent::CommandSummary {
                    command: SanitizedCommandKind::Write,
                    outcome: summary_outcome(event_type),
                    details,
                }
            }
            "tool_use" | "item.started/mcp_call" | "item.completed/mcp_call" => {
                SanitizedSessionEvent::ToolSummary {
                    tool: SanitizedToolKind::Other,
                    outcome: summary_outcome(event_type),
                    details,
                }
            }
            _ => SanitizedSessionEvent::ToolSummary {
                tool: SanitizedToolKind::Other,
                outcome: summary_outcome(event_type),
                details,
            },
        };
        if matches!(event, SanitizedSessionEvent::TerminalResponse { .. }) {
            self.log_sanitized_closure_events(&[event]);
        } else {
            self.log_sanitized_events(&[event]);
        }
    }

    fn log_provider_failure(&mut self, reason: &str) {
        if self.session_log.is_none() {
            return;
        }
        self.log_sanitized_closure_events(&[SanitizedSessionEvent::ProviderFailure {
            provider: sanitized_provider(self.proc.kind()),
            kind: provider_failure_kind(reason),
            details: SanitizedField::from_text(reason),
        }]);
    }

    fn log_missing_submission(&mut self) {
        if self.session_log.is_none() {
            return;
        }
        self.log_sanitized_closure_events(&[
            SanitizedSessionEvent::SemanticRejection {
                kind: SanitizedRejectionKind::MissingSubmission,
                details: SanitizedField::from_text("planner exited without submit_plan"),
            },
            SanitizedSessionEvent::ProviderFailure {
                provider: sanitized_provider(self.proc.kind()),
                kind: SanitizedProviderFailureKind::Other,
                details: SanitizedField::from_text("planner exited without submit_plan"),
            },
            SanitizedSessionEvent::ProviderLifecycle {
                provider: sanitized_provider(self.proc.kind()),
                phase: ProviderLifecyclePhase::Stopped,
            },
            SanitizedSessionEvent::Completion {
                outcome: SanitizedCompletionOutcome::Failed,
            },
        ]);
    }

    fn log_completion(&mut self) {
        if self.session_log.is_none() {
            return;
        }
        self.log_sanitized_closure_events(&[
            SanitizedSessionEvent::ProviderLifecycle {
                provider: sanitized_provider(self.proc.kind()),
                phase: ProviderLifecyclePhase::Stopped,
            },
            SanitizedSessionEvent::Completion {
                outcome: SanitizedCompletionOutcome::Completed,
            },
        ]);
    }

    fn log_failed_completion(&mut self) {
        if self.session_log.is_none() {
            return;
        }
        self.log_sanitized_closure_events(&[
            SanitizedSessionEvent::ProviderLifecycle {
                provider: sanitized_provider(self.proc.kind()),
                phase: ProviderLifecyclePhase::Stopped,
            },
            SanitizedSessionEvent::Completion {
                outcome: SanitizedCompletionOutcome::Failed,
            },
        ]);
    }

    /// Finalize this attempt's session log at most once.
    ///
    /// Taking the log makes finalization ownership explicit: normal terminal
    /// handling and forced reaping cannot both rewrite its metadata.
    pub fn finalize_session_log(&mut self) {
        if let Some(mut session_log) = self.session_log.take() {
            session_log.finalize(None);
        }
    }

    /// Kill this planner attempt, retain its terminal diagnostics, and return
    /// its complete token usage for the caller's durable accounting.
    pub async fn kill_and_reap(mut self) -> super::runner::TokenUsage {
        let session_log = self.session_log.take();
        let kind = self.proc.kind();
        let output = self.proc.kill_and_reap().await;
        for captured in &output {
            let CapturedOutput::Stdout(raw) = captured else {
                continue;
            };
            for event in super::runner::normalize_line(kind, raw) {
                match event {
                    AgentEvent::TurnCompleted {
                        usage: Some(usage), ..
                    }
                    | AgentEvent::TurnFailed {
                        usage: Some(usage), ..
                    } => self.usage.saturating_add_assign(usage),
                    _ => {}
                }
            }
        }
        // Teardown may uncover provider bytes which never crossed
        // `poll_planner`. They are diagnostic-only and must not bypass the
        // planner's sanitized poll boundary into its durable session log.
        if let Some(mut session_log) = session_log {
            session_log.finalize(None);
        }
        self.usage
    }
}

fn sanitized_provider(provider: AgentKind) -> SanitizedProvider {
    match provider {
        AgentKind::Claude => SanitizedProvider::Claude,
        AgentKind::Codex => SanitizedProvider::Codex,
        AgentKind::Grok => SanitizedProvider::Grok,
    }
}

fn summary_outcome(event_type: &str) -> SanitizedSummaryOutcome {
    if event_type.starts_with("item.started") {
        SanitizedSummaryOutcome::Started
    } else if event_type.ends_with("/error") {
        SanitizedSummaryOutcome::Failed
    } else {
        SanitizedSummaryOutcome::Succeeded
    }
}

fn terminal_status(event_type: &str, raw: &str) -> SanitizedTerminalStatus {
    if event_type == "result"
        && serde_json::from_str::<serde_json::Value>(raw)
            .ok()
            .and_then(|value| value.get("is_error").and_then(serde_json::Value::as_bool))
            == Some(true)
    {
        SanitizedTerminalStatus::Error
    } else {
        SanitizedTerminalStatus::Success
    }
}

fn provider_failure_kind(reason: &str) -> SanitizedProviderFailureKind {
    let reason = reason.to_ascii_lowercase();
    if reason.contains("auth") || reason.contains("credential") {
        SanitizedProviderFailureKind::Authentication
    } else if reason.contains("timed out") {
        SanitizedProviderFailureKind::Timeout
    } else if reason.contains("protocol") || reason.contains("stdout") {
        SanitizedProviderFailureKind::Protocol
    } else if reason.contains("read failed") || reason.contains("transport") {
        SanitizedProviderFailureKind::Transport
    } else if reason.contains("exited") || reason.contains("status") {
        SanitizedProviderFailureKind::Exit
    } else {
        SanitizedProviderFailureKind::Other
    }
}

/// How one planner provider process ended its turn.
///
/// This is deliberately not an outcome. The provider reports only whether it
/// reached a clean terminal state; the plan itself comes from the durable
/// `submit_plan` submission, which [`planner_outcome`] pairs with this report.
pub enum PlannerTurnEnd {
    /// The turn reached its terminal provider state with no provider-level
    /// defect. It says nothing about whether a plan was submitted.
    Complete,
    /// Bounded operator diagnostic for a provider-level failure.
    Failed(String),
}

/// The durable outcome of one planner attempt.
pub enum PlannerPoll {
    Done(PlannerResponse),
    ProviderFailed(String),
    /// Retained for the Arbiter proposal-rejection path (design §4). Semantic
    /// defects in a submitted plan are now returned to the planner in-turn by
    /// the endpoint, so the planner attempt itself never produces this.
    SemanticRejected(String),
}

/// Resolve one planner attempt from its durable `submit_plan` submission.
///
/// `submitted` is `planner_submissions::accepted_response` for this run. A
/// submission is authoritative however the process then ended: it was accepted
/// by the daemon's own endpoint after full validation, so a provider that
/// crashed or timed out after submitting still delivered a plan. Provider
/// stdout is operator diagnostics only and is never consulted for the plan.
pub fn planner_outcome(
    slot: &mut PlannerSlot,
    end: PlannerTurnEnd,
    submitted: Option<&str>,
) -> PlannerPoll {
    if let Some(response) = submitted {
        return match rehydrate_submitted_response(response) {
            Ok(response) => {
                // The daemon accepted this submission before the provider
                // ended, so it remains the attempt outcome even after a
                // provider crash or timeout.
                slot.log_completion();
                PlannerPoll::Done(response)
            }
            Err(error) => {
                let reason = error.to_string();
                slot.log_provider_failure(&reason);
                slot.log_failed_completion();
                PlannerPoll::ProviderFailed(provider_failure_summary(slot, &reason, "exact"))
            }
        };
    }
    PlannerPoll::ProviderFailed(match end {
        PlannerTurnEnd::Failed(summary) => {
            slot.log_failed_completion();
            summary
        }
        PlannerTurnEnd::Complete => {
            slot.log_missing_submission();
            provider_failure_summary(slot, "planner exited without submit_plan", "exact")
        }
    })
}

/// The managed run envelope of one planner turn. Present only once the daemon
/// has issued the run's `planner` capability; the four names it produces are
/// exactly `runner::AGENT_MCP_ENV_VARS`, which is what the stdio `submit_plan`
/// MCP child reads to reach the daemon endpoint.
#[derive(Debug, Clone)]
pub struct PlannerRunEnvelope {
    pub run_id: String,
    pub agent: String,
    /// Repository slug (`owner/repo`), matching `QUORUM_REPO` everywhere else.
    pub repo: String,
    /// Agent endpoint socket path.
    pub endpoint: PathBuf,
}

impl PlannerRunEnvelope {
    /// Fail loudly rather than spawn a planner whose tool cannot authenticate:
    /// an empty name would reach `agent-mcp` as a usage error only after the
    /// provider had already burned its turn.
    fn environment(&self) -> std::io::Result<Vec<(String, String)>> {
        let environment = vec![
            ("QUORUM_REPO".to_string(), self.repo.clone()),
            ("QUORUM_AGENT".to_string(), self.agent.clone()),
            ("QUORUM_RUN_ID".to_string(), self.run_id.clone()),
            (
                "QUORUM_AGENT_ENDPOINT".to_string(),
                self.endpoint.display().to_string(),
            ),
        ];
        if let Some((name, _)) = environment.iter().find(|(_, value)| value.is_empty()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("planner run envelope is missing {name}"),
            ));
        }
        Ok(environment)
    }
}

/// Spawn only the provider selected by the durable role assignment. There is
/// no fallback or model substitution.
///
/// `run` is `Some` once the caller has issued this run's `planner` capability:
/// the envelope is placed in the planner process environment and the
/// `submit_plan` MCP server is attached. `None` keeps the historical tool-less
/// planner surface.
///
/// One attempt has exactly one identity: when an envelope is present its
/// `run_id` — minted by `agent::new_session_id()` and already bound to both the
/// run capability and `task_decompositions.planner_session_id` — is also the
/// provider session id, so no second id is minted here.
#[allow(clippy::too_many_arguments)]
pub async fn spawn_planner(
    provider: AgentKind,
    model: &str,
    effort: &str,
    repo: &Path,
    prompt: &str,
    bare: bool,
    provider_bin: Option<&str>,
    run: Option<&PlannerRunEnvelope>,
) -> std::io::Result<PlannerSlot> {
    spawn_planner_with_timeout(
        provider,
        model,
        effort,
        repo,
        prompt,
        bare,
        provider_bin,
        run,
        PLANNER_TIMEOUT,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn spawn_planner_with_timeout(
    provider: AgentKind,
    model: &str,
    effort: &str,
    repo: &Path,
    prompt: &str,
    bare: bool,
    provider_bin: Option<&str>,
    run: Option<&PlannerRunEnvelope>,
    turn_timeout: Duration,
) -> std::io::Result<PlannerSlot> {
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "planner prompt exceeds 128 KiB",
        ));
    }
    let env_vars = match run {
        Some(envelope) => envelope.environment()?,
        None => Vec::new(),
    };
    let agent_mcp = run.map(|_| crate::serve::runner::AGENT_MCP_SERVER);
    let started_at = tokio::time::Instant::now();
    let deadline = started_at + turn_timeout;
    let proc =
        match provider {
            AgentKind::Codex => {
                let spec = CodexSpec {
                    model: model.into(),
                    effort: effort.into(),
                    sandbox: "read-only".into(),
                    worktree: repo.to_path_buf(),
                    prompt: prompt.into(),
                    env_vars,
                };
                RunnerProc::Codex(CodexProc::spawn_planner(&spec, provider_bin, agent_mcp)?)
            }
            AgentKind::Grok => return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Grok decomposition planner refused: managed Grok lifecycle roles are not enabled",
            )),
            AgentKind::Claude => {
                let spec = AgentSpec {
                    kind: AgentKind::Claude,
                    model: model.into(),
                    effort: effort.into(),
                    session_id: run
                        .map(|envelope| envelope.run_id.clone())
                        .unwrap_or_else(agent::new_session_id),
                    worktree: repo.to_path_buf(),
                    bare,
                    allowed_tools: "Read,Glob,Grep".into(),
                    env_vars,
                };
                let mut proc = AgentProc::spawn_planner(&spec, provider_bin, agent_mcp)?;
                if let Err(error) = proc
                    .feed_turn_until(&agent::user_turn(prompt), deadline)
                    .await
                {
                    let _ = proc.kill_and_reap().await;
                    return Err(error);
                }
                RunnerProc::Claude(proc)
            }
        };
    Ok(PlannerSlot {
        proc,
        provider: provider.to_string(),
        model: model.into(),
        effort: effort.into(),
        usage: super::runner::TokenUsage::default(),
        started_at,
        stdout_bytes: 0,
        codex_terminal_candidate: false,
        diagnostics: PlannerDiagnostics::default(),
        session_log: None,
        sanitized_record_count: 0,
    })
}

fn provider_failure(slot: &mut PlannerSlot, reason: &str, byte_count_kind: &str) -> PlannerTurnEnd {
    slot.log_provider_failure(reason);
    PlannerTurnEnd::Failed(provider_failure_summary(slot, reason, byte_count_kind))
}

/// Build the bounded, payload-free operator diagnostic for one failure. The
/// samples describe stream structure only; provider payload strings never
/// enter it.
fn provider_failure_summary(slot: &PlannerSlot, reason: &str, byte_count_kind: &str) -> String {
    let bounded_reason = truncate_utf8(reason, MAX_FAILURE_REASON_BYTES);
    let reason_truncated = bounded_reason.len() != reason.len();
    let beginning = &slot.diagnostics.beginning;
    let end = slot.diagnostics.end.iter().collect::<Vec<_>>();
    let samples_truncated = slot.diagnostics.read_boundary_truncated
        || slot.diagnostics.lines as usize > DIAGNOSTIC_SAMPLE_LINES.saturating_mul(2);
    let diagnostic = serde_json::json!({
        "failure": bounded_reason,
        "failure_reason_truncated": reason_truncated,
        "planner_diagnostic": {
            "provider": match slot.proc.kind() {
                AgentKind::Claude => "claude",
                AgentKind::Codex => "codex",
                AgentKind::Grok => "grok",
            },
            "stdout_bytes_observed": slot.stdout_bytes,
            "stdout_byte_count_kind": byte_count_kind,
            "stdout_lines": slot.diagnostics.lines,
            "event_types": slot.diagnostics.event_types,
            "terminal_response_seen": slot.diagnostics.terminal_response_seen,
            "samples": {
                "beginning": beginning,
                "end": end,
                "payloads_redacted": true,
                "truncated": samples_truncated,
                "read_boundary_truncated": slot.diagnostics.read_boundary_truncated,
            }
        }
    });
    let mut summary = serde_json::to_string(&diagnostic).expect("planner diagnostic serializes");
    if summary.len() > MAX_FAILURE_SUMMARY_BYTES {
        const SUFFIX: &str = "... [planner diagnostic truncated]";
        let prefix = truncate_utf8(
            &summary,
            MAX_FAILURE_SUMMARY_BYTES.saturating_sub(SUFFIX.len()),
        );
        summary = format!("{prefix}{SUFFIX}");
    }
    summary
}

fn planner_exit_failure(slot: &mut PlannerSlot, failure: RunnerFailure) -> PlannerTurnEnd {
    provider_failure(
        slot,
        &format!(
            "planner provider exited without a terminal response ({}): {}",
            failure.disposition(),
            failure.detail()
        ),
        "exact",
    )
}

fn stderr_capture_was_truncated(output: &[CapturedOutput]) -> bool {
    output.iter().any(|line| {
        matches!(
            line,
            CapturedOutput::StderrTruncated { .. } | CapturedOutput::StderrBytesTruncated { .. }
        )
    })
}

/// Drain a bounded amount of output and report how the turn ended. Timeout and
/// output violations are provider failures; the caller must kill and reap the
/// returned terminal slot.
///
/// Assistant text is observed only as a structural diagnostic sample. This
/// function cannot produce a plan: the plan comes from the run's durable
/// `submit_plan` submission, which the caller pairs with this report through
/// [`planner_outcome`].
pub async fn poll_planner(slot: &mut PlannerSlot) -> Option<PlannerTurnEnd> {
    if slot.started_at.elapsed() >= PLANNER_TIMEOUT {
        return Some(provider_failure(slot, "planner timed out", "lower-bound"));
    }
    let remaining = PLANNER_TIMEOUT.saturating_sub(slot.started_at.elapsed());
    let poll_for = remaining.min(Duration::from_secs(2));
    let mut stdout_complete = false;
    loop {
        // Reserve the byte previously charged for the line terminator. More
        // importantly, enforce the remaining allowance while bytes are read;
        // waiting for a newline would permit an unbounded internal allocation.
        let line_limit = MAX_STDOUT_BYTES
            .saturating_sub(slot.stdout_bytes)
            .saturating_sub(1);
        let raw = match tokio::time::timeout(poll_for, slot.proc.next_raw_line_bounded(line_limit))
            .await
        {
            Err(_) => break,
            Ok(Ok(Some(raw))) => raw,
            Ok(Ok(None)) => {
                stdout_complete = true;
                break;
            }
            Ok(Err(error)) => {
                slot.diagnostics.note_read_boundary_truncation();
                let exceeded = error.to_string().contains("exceeded");
                if exceeded {
                    slot.stdout_bytes = MAX_STDOUT_BYTES.saturating_add(1);
                }
                let reason = if exceeded {
                    format!("planner stdout exceeded {} KiB", MAX_STDOUT_BYTES / 1024)
                } else {
                    format!("planner stdout read failed: {error}")
                };
                return Some(provider_failure(
                    slot,
                    &reason,
                    if exceeded {
                        "lower-bound"
                    } else {
                        "completed-lines-only"
                    },
                ));
            }
        };
        let events = match slot.proc.kind() {
            AgentKind::Claude => super::runner::normalize_claude_line(&raw),
            AgentKind::Codex => super::runner::normalize_codex_line(&raw),
            AgentKind::Grok => super::runner::normalize_grok_line(&raw),
        };
        if slot.session_log.is_some() {
            slot.log_sanitized_line(&raw);
        }
        slot.stdout_bytes = slot.stdout_bytes.saturating_add(raw.len() + 1);
        slot.diagnostics.observe_line(slot.proc.kind(), &raw);
        for event in super::runner::normalize_line(slot.proc.kind(), &raw) {
            match event {
                AgentEvent::TurnCompleted {
                    usage: Some(usage), ..
                }
                | AgentEvent::TurnFailed {
                    usage: Some(usage), ..
                } => slot.usage.saturating_add_assign(usage),
                _ => {}
            }
        }
        if slot.stdout_bytes > MAX_STDOUT_BYTES {
            return Some(provider_failure(
                slot,
                &format!("planner stdout exceeded {} KiB", MAX_STDOUT_BYTES / 1024),
                "exact-through-last-line",
            ));
        }
        if slot.codex_terminal_candidate {
            return Some(provider_failure(
                slot,
                "planner provider emitted output after terminal response",
                "exact-through-last-line",
            ));
        }
        if slot.proc.kind() == AgentKind::Codex {
            if let Some(failure) = slot.proc.observed_planner_live_failure() {
                return Some(provider_failure(
                    slot,
                    &format!(
                        "planner provider protocol failed ({})",
                        failure.disposition()
                    ),
                    "exact-through-last-line",
                ));
            }
        }
        if slot.proc.kind() == AgentKind::Claude {
            // The result event ends the turn. Its `result` text is deliberately
            // not bound: the plan is whatever this run submitted through
            // `submit_plan`, and the transcript is diagnostics only.
            if let Some(super::stream::Event::Result { is_error, .. }) =
                super::stream::parse_line(&raw)
            {
                if is_error.unwrap_or(false) {
                    return Some(provider_failure(
                        slot,
                        "planner provider returned an error",
                        "exact-through-last-line",
                    ));
                }
                return Some(PlannerTurnEnd::Complete);
            }
        }
        for event in events {
            match event {
                AgentEvent::TurnFailed { .. } => {
                    return Some(provider_failure(
                        slot,
                        "planner provider turn failed",
                        "exact-through-last-line",
                    ));
                }
                AgentEvent::TurnCompleted { .. } => {
                    // Codex's terminal event is provisional: its clean exit and
                    // final stderr/protocol evidence are checked below before the
                    // turn counts as complete.
                    if slot.proc.kind() == AgentKind::Codex {
                        slot.codex_terminal_candidate = true;
                    } else {
                        return Some(PlannerTurnEnd::Complete);
                    }
                }
                _ => {}
            }
        }
        if slot.started_at.elapsed() >= PLANNER_TIMEOUT {
            return Some(provider_failure(slot, "planner timed out", "lower-bound"));
        }
    }
    let status = match slot.proc.try_wait() {
        Ok(status) => status,
        Err(error) => {
            return Some(provider_failure(
                slot,
                &format!("planner process status unavailable: {error}"),
                "completed-lines-only",
            ));
        }
    };
    if slot.proc.kind() == AgentKind::Codex && slot.codex_terminal_candidate && stdout_complete {
        let status = status?;
        let evidence = slot.proc.finalize_pre_authoritative_evidence().await;
        if !status.success() {
            if let Some(failure) = slot.proc.observed_strict_pre_authoritative_failure() {
                return Some(provider_failure(
                    slot,
                    &format!(
                        "planner provider exited unsuccessfully after terminal response ({}) at {status}: {}",
                        failure.disposition(),
                        failure.detail()
                    ),
                    "exact",
                ));
            }
            return Some(provider_failure(
                slot,
                &format!(
                    "planner provider exited unsuccessfully after terminal response: {status}"
                ),
                "exact",
            ));
        }
        if stderr_capture_was_truncated(&evidence) {
            return Some(provider_failure(
                slot,
                "planner stderr exceeded bounded diagnostic capture",
                "exact",
            ));
        }
        if let Some(failure) = slot.proc.observed_planner_terminal_failure() {
            return Some(provider_failure(
                slot,
                &format!(
                    "planner provider terminal evidence failed ({})",
                    failure.disposition()
                ),
                "exact",
            ));
        }
        return Some(PlannerTurnEnd::Complete);
    }
    if let Some(status) = status {
        let evidence = slot.proc.finalize_pre_authoritative_evidence().await;
        if stderr_capture_was_truncated(&evidence) {
            return Some(provider_failure(
                slot,
                "planner stderr exceeded bounded diagnostic capture",
                "exact",
            ));
        }
        if let Some(failure) = slot.proc.classify_pre_authoritative_exit(status) {
            return Some(planner_exit_failure(slot, failure));
        }
        return Some(provider_failure(
            slot,
            "planner exited without a terminal response",
            "exact",
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // The no-read provider test fills a pipe with a near-limit prompt. Allow
    // normal CI scheduling delay while retaining a finite failure boundary.
    const TEST_STDIN_FEED_TIMEOUT: Duration = Duration::from_secs(15);
    const TEST_BOUNDARY_TIMEOUT: Duration = Duration::from_secs(15);

    #[cfg(unix)]
    fn executable_script(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let runner = dir.join(name);
        std::fs::write(&runner, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        runner
    }

    /// The exact validation a submitted plan receives: closed deserialization
    /// followed by `validate_semantics`. The endpoint applies these two steps
    /// to every `submit_plan` call, and they are the only validation a plan
    /// now passes through — no text parser remains.
    fn validate_submitted(text: &str) -> Result<PlannerResponse, PlannerParseError> {
        let response = rehydrate_submitted_response(text)?;
        validate_semantics(&response)?;
        Ok(response)
    }

    /// The envelope produces exactly the names the stdio MCP child reads, in
    /// the order `runner::AGENT_MCP_ENV_VARS` pins.
    #[test]
    fn planner_run_envelope_produces_the_agent_mcp_env_vars() {
        let envelope = PlannerRunEnvelope {
            run_id: "run-capability".into(),
            agent: "Planner-test".into(),
            repo: "owner/repo".into(),
            endpoint: PathBuf::from("/tmp/quorum-agent.sock"),
        };
        let environment = envelope.environment().expect("complete envelope");
        let names: Vec<&str> = environment.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, crate::serve::runner::AGENT_MCP_ENV_VARS);
        assert_eq!(
            environment
                .iter()
                .map(|(_, value)| value.as_str())
                .collect::<Vec<_>>(),
            [
                "owner/repo",
                "Planner-test",
                "run-capability",
                "/tmp/quorum-agent.sock"
            ]
        );
    }

    /// An incomplete envelope must fail before the provider spawns: an empty
    /// name would only surface as an `agent-mcp` usage error after the planner
    /// had already burned its turn.
    #[test]
    fn planner_run_envelope_rejects_an_incomplete_name() {
        for missing in ["run_id", "agent", "repo"] {
            let mut envelope = PlannerRunEnvelope {
                run_id: "run-capability".into(),
                agent: "Planner-test".into(),
                repo: "owner/repo".into(),
                endpoint: PathBuf::from("/tmp/quorum-agent.sock"),
            };
            match missing {
                "run_id" => envelope.run_id.clear(),
                "agent" => envelope.agent.clear(),
                _ => envelope.repo.clear(),
            }
            let error = envelope
                .environment()
                .expect_err("incomplete envelope must be refused");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }

    /// End-to-end spawn wiring: an envelope must reach the planner process
    /// environment and bring the MCP override with it.
    #[cfg(unix)]
    #[tokio::test]
    async fn spawn_planner_with_envelope_carries_run_env_and_mcp_override() {
        let dir = tempfile::tempdir().unwrap();
        let capture = dir.path().join("observed");
        let runner = executable_script(
            dir.path(),
            "codex",
            &format!(
                "printf 'run=%s\\nrepo=%s\\nendpoint=%s\\nargs=%s\\n' \
                 \"$QUORUM_RUN_ID\" \"$QUORUM_REPO\" \"$QUORUM_AGENT_ENDPOINT\" \"$*\" > '{0}.tmp'\n\
                 mv '{0}.tmp' '{0}'",
                capture.display()
            ),
        );
        let envelope = PlannerRunEnvelope {
            run_id: "run-capability".into(),
            agent: "Planner-test".into(),
            repo: "owner/repo".into(),
            endpoint: PathBuf::from("/tmp/quorum-agent.sock"),
        };

        let slot = spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir.path(),
            "bounded prompt",
            false,
            runner.to_str(),
            Some(&envelope),
        )
        .await
        .unwrap();
        for _ in 0..200 {
            if capture.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let observed = std::fs::read_to_string(&capture)
            .expect("planner process did not report its run envelope");
        slot.kill_and_reap().await;

        assert!(observed.contains("run=run-capability\n"), "{observed}");
        assert!(observed.contains("repo=owner/repo\n"), "{observed}");
        assert!(
            observed.contains("endpoint=/tmp/quorum-agent.sock\n"),
            "{observed}"
        );
        assert!(observed.contains("mcp_servers.quorum="), "{observed}");
        assert!(observed.contains("-s read-only"), "{observed}");
    }

    #[cfg(unix)]
    async fn poll_to_terminal(slot: &mut PlannerSlot) -> PlannerTurnEnd {
        tokio::time::timeout(TEST_BOUNDARY_TIMEOUT, async {
            loop {
                if let Some(outcome) = poll_planner(slot).await {
                    break outcome;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("planner did not reach a terminal outcome")
    }

    #[cfg(unix)]
    async fn spawn_fake_codex(dir: &Path, stdout: &str) -> PlannerSlot {
        let stdout_path = dir.join("stdout.jsonl");
        std::fs::write(&stdout_path, stdout).unwrap();
        let runner = executable_script(
            dir,
            "codex",
            &format!("exec /bin/cat '{}'", stdout_path.display()),
        );
        spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir,
            "bounded prompt",
            false,
            runner.to_str(),
            None,
        )
        .await
        .unwrap()
    }

    #[cfg(unix)]
    async fn spawn_fake_claude(dir: &Path, stdout: &str) -> PlannerSlot {
        let stdout_path = dir.join("stdout.jsonl");
        std::fs::write(&stdout_path, stdout).unwrap();
        let runner = executable_script(
            dir,
            "claude",
            &format!("exec /bin/cat '{}'", stdout_path.display()),
        );
        spawn_planner(
            AgentKind::Claude,
            CLAUDE_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir,
            "bounded prompt",
            false,
            runner.to_str(),
            None,
        )
        .await
        .unwrap()
    }

    #[cfg(unix)]
    fn claude_stream(events: &[serde_json::Value]) -> String {
        let mut stream = events
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        stream.push_str("\n{\"type\":\"result\",\"result\":\"\",\"is_error\":false}\n");
        stream
    }

    #[cfg(unix)]
    async fn spawn_fake_codex_with_stderr(dir: &Path, stdout: &str, stderr: &str) -> PlannerSlot {
        let stdout_path = dir.join("stdout.jsonl");
        let stderr_path = dir.join("stderr.txt");
        std::fs::write(&stdout_path, stdout).unwrap();
        std::fs::write(&stderr_path, stderr).unwrap();
        let runner = executable_script(
            dir,
            "codex",
            &format!(
                "/bin/cat '{}' >&2\nexec /bin/cat '{}'",
                stderr_path.display(),
                stdout_path.display()
            ),
        );
        spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir,
            "bounded prompt",
            false,
            runner.to_str(),
            None,
        )
        .await
        .unwrap()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn no_poll_reap_does_not_persist_buffered_provider_stream() {
        let dir = tempfile::tempdir().unwrap();
        let raw = r#"{"type":"turn.failed","error":{"message":"provider failed"}}"#;
        let mut slot = spawn_fake_codex(dir.path(), &format!("{raw}\n")).await;
        slot.start_session_log(
            dir.path(),
            "decomposition-planner-test",
            42,
            "session",
            "frozen-base",
            1,
        )
        .unwrap();
        let log_dir = slot.log_dir().unwrap().to_path_buf();

        tokio::time::timeout(TEST_BOUNDARY_TIMEOUT, async {
            while slot.proc.try_wait().unwrap().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("planner provider did not exit");

        slot.kill_and_reap().await;
        let stream = std::fs::read_to_string(log_dir.join("stream.jsonl")).unwrap();
        assert!(!stream.contains(raw));
        assert!(stream.contains(r#""event":"provider_lifecycle""#));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_planner_logs_only_sanitized_progress_and_redacts_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let credential = "sk-planner-provider-payload-secret";
        let prompt = format!("credential-shaped prompt value: {credential}");
        let output = format!(
            "{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
            serde_json::json!({"type":"thread.started","thread_id":"fixture-thread"}),
            serde_json::json!({"type":"turn.started"}),
            serde_json::json!({
                "type":"item.started",
                "item":{"type":"command_execution","id":"command-1","command":format!("echo {credential}"),"status":"in_progress"}
            }),
            serde_json::json!({
                "type":"item.completed",
                "item":{"type":"command_execution","id":"command-1","command":"echo redacted","aggregated_output":credential,"exit_code":0,"status":"completed"}
            }),
            serde_json::json!({
                "type":"item.completed",
                "item":{"type":"mcp_call","id":"tool-1","tool_output":credential}
            }),
            serde_json::json!({
                "type":"item.completed",
                "item":{"type":"agent_message","id":"message-1","text":credential}
            }),
            serde_json::json!({"type":"turn.completed"}),
        );
        let stdout_path = dir.path().join("stdout.jsonl");
        std::fs::write(&stdout_path, output).unwrap();
        let runner = executable_script(
            dir.path(),
            "codex",
            &format!("exec /bin/cat '{}'", stdout_path.display()),
        );
        let mut slot = spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir.path(),
            &prompt,
            false,
            runner.to_str(),
            None,
        )
        .await
        .unwrap();
        slot.start_session_log(
            dir.path(),
            "decomposition-planner-test",
            42,
            "session",
            "frozen-base",
            1,
        )
        .unwrap();
        let log_dir = slot.log_dir().unwrap().to_path_buf();

        let turn_end = poll_to_terminal(&mut slot).await;
        let PlannerPoll::ProviderFailed(summary) = planner_outcome(&mut slot, turn_end, None)
        else {
            panic!("a planner without submit_plan must fail");
        };

        let stream = std::fs::read_to_string(log_dir.join("stream.jsonl")).unwrap();
        let transcript = std::fs::read_to_string(log_dir.join("transcript.md")).unwrap();
        for durable_surface in [&stream, &transcript, &summary] {
            assert!(
                !durable_surface.contains(credential),
                "credential leaked into durable planner evidence: {durable_surface}"
            );
        }
        assert!(
            !stream.contains("aggregated_output"),
            "raw provider JSON reached the sanitized stream: {stream}"
        );

        let event_kinds = stream
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["event"].clone())
            .collect::<Vec<_>>();
        for expected in [
            "provider_lifecycle",
            "turn_lifecycle",
            "command_summary",
            "tool_summary",
            "terminal_response",
            "provider_failure",
            "semantic_rejection",
            "completion",
        ] {
            assert!(
                event_kinds.iter().any(|kind| kind == expected),
                "missing sanitized {expected} event: {stream}"
            );
        }
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn planner_log_reserves_terminal_and_closure_events_after_long_progress() {
        let dir = tempfile::tempdir().unwrap();
        let mut output = (0..300)
            .map(|index| {
                serde_json::json!({
                    "type": "item.completed",
                    "item": {
                        "type": "command_execution",
                        "id": format!("command-{index}"),
                        "status": "completed",
                    },
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        output.push('\n');
        output.push_str(&serde_json::json!({"type": "turn.completed"}).to_string());
        output.push('\n');

        let mut slot = spawn_fake_codex(dir.path(), &output).await;
        slot.start_session_log(
            dir.path(),
            "decomposition-planner-test",
            42,
            "session",
            "frozen-base",
            1,
        )
        .unwrap();
        let log_dir = slot.log_dir().unwrap().to_path_buf();

        let turn_end = poll_to_terminal(&mut slot).await;
        assert!(matches!(turn_end, PlannerTurnEnd::Complete));
        assert!(matches!(
            planner_outcome(&mut slot, turn_end, None),
            PlannerPoll::ProviderFailed(_)
        ));

        let events = std::fs::read_to_string(log_dir.join("stream.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events.len(), MAX_SANITIZED_RECORDS_PER_SESSION);
        for expected in [
            "terminal_response",
            "semantic_rejection",
            "provider_failure",
            "completion",
        ] {
            assert!(
                events.iter().any(|event| event["event"] == expected),
                "missing {expected} after long planner progress"
            );
        }
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event"] == "completion")
                .count(),
            1
        );
        assert_eq!(
            events.last().unwrap()["event"],
            "completion",
            "completion must remain the final durable record"
        );
        assert_eq!(events.last().unwrap()["outcome"], "failed");
        assert!(events.iter().any(|event| {
            event["event"] == "provider_lifecycle" && event["phase"] == "stopped"
        }));

        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn planner_log_reserves_provider_failure_after_long_progress() {
        let dir = tempfile::tempdir().unwrap();
        let mut output = (0..300)
            .map(|index| {
                serde_json::json!({
                    "type": "item.completed",
                    "item": {
                        "type": "command_execution",
                        "id": format!("command-{index}"),
                        "status": "completed",
                    },
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        output.push('\n');
        output.push_str(
            &serde_json::json!({
                "type": "turn.failed",
                "error": {"message": "provider failed after long progress"},
            })
            .to_string(),
        );
        output.push('\n');

        let mut slot = spawn_fake_codex(dir.path(), &output).await;
        slot.start_session_log(
            dir.path(),
            "decomposition-planner-test",
            42,
            "session",
            "frozen-base",
            1,
        )
        .unwrap();
        let log_dir = slot.log_dir().unwrap().to_path_buf();

        let turn_end = poll_to_terminal(&mut slot).await;
        assert!(matches!(turn_end, PlannerTurnEnd::Failed(_)));
        assert!(matches!(
            planner_outcome(&mut slot, turn_end, None),
            PlannerPoll::ProviderFailed(_)
        ));

        let events = std::fs::read_to_string(log_dir.join("stream.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(events.len() <= MAX_SANITIZED_RECORDS_PER_SESSION);
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event"] == "provider_failure")
                .count(),
            1,
            "the reserved provider-failure record must survive the progress cap"
        );
        let completions = events
            .iter()
            .filter(|event| event["event"] == "completion")
            .collect::<Vec<_>>();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0]["outcome"], "failed");
        assert_eq!(events.last().unwrap()["event"], "completion");

        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn planner_failure_is_unchanged_when_session_logging_is_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let output = concat!(
            "{\"type\":\"thread.started\",\"thread_id\":\"fixture-thread\"}\n",
            "{\"type\":\"turn.failed\",\"error\":{\"message\":\"provider failed\"}}\n"
        );
        let mut without_log = spawn_fake_codex(dir.path(), output).await;
        let without_log = poll_to_terminal(&mut without_log).await;
        let without_log = failure_json(&without_log);

        let mut with_log = spawn_fake_codex(dir.path(), output).await;
        with_log
            .start_session_log(
                dir.path(),
                "decomposition-planner-test",
                42,
                "session",
                "frozen-base",
                2,
            )
            .unwrap();
        let with_log = poll_to_terminal(&mut with_log).await;
        let with_log = failure_json(&with_log);

        assert_eq!(without_log, with_log);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn session_logs_finalize_once_for_all_terminal_paths() {
        for terminal in [
            "success",
            "provider-failure",
            "semantic-rejection",
            "timeout",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut slot = spawn_fake_codex(dir.path(), "").await;
            slot.start_session_log(
                dir.path(),
                "decomposition-planner-test",
                42,
                terminal,
                "frozen-base",
                1,
            )
            .unwrap();
            let log_dir = slot.log_dir().unwrap().to_path_buf();

            // The terminal coordinator path finalizes before moving `proc`
            // into generic usage cleanup. Taking the log makes a repeated
            // call a no-op rather than a second metadata write.
            slot.finalize_session_log();
            assert!(slot.log_dir().is_none());
            slot.finalize_session_log();
            let _ = slot.proc.kill_and_reap().await;

            let meta: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(log_dir.join("meta.json")).unwrap())
                    .unwrap();
            assert!(meta["end_time"].is_i64(), "{terminal}");
        }

        for terminal in ["shutdown-drain", "forced-reap"] {
            let dir = tempfile::tempdir().unwrap();
            let mut slot = spawn_fake_codex(dir.path(), "").await;
            slot.start_session_log(
                dir.path(),
                "decomposition-planner-test",
                42,
                terminal,
                "frozen-base",
                1,
            )
            .unwrap();
            let log_dir = slot.log_dir().unwrap().to_path_buf();

            let _ = slot.kill_and_reap().await;

            let meta: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(log_dir.join("meta.json")).unwrap())
                    .unwrap();
            assert!(meta["end_time"].is_i64(), "{terminal}");
        }
    }

    fn failure_json(outcome: &PlannerTurnEnd) -> serde_json::Value {
        let PlannerTurnEnd::Failed(summary) = outcome else {
            panic!("expected provider failure");
        };
        assert!(
            summary.len() <= MAX_FAILURE_SUMMARY_BYTES,
            "durable planner diagnostic was {} bytes",
            summary.len()
        );
        serde_json::from_str(summary).expect("planner failure remains inspectable JSON")
    }

    fn task(key: &str, prerequisites: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "key": key,
            "title": format!("Implement {key}"),
            "implementation_delta": format!("change the {key} implementation seam"),
            "affected_paths": [format!("src/{key}.rs")],
            "observable_outcome": format!("{key} works"),
            "deliverables": [{"kind": "write", "path": format!("src/{key}.rs")}],
            "acceptance_criteria": ["behavior is covered"],
            "source_constraints": ["preserve atomicity"],
            "verification_expectations": ["focused tests pass"],
            "non_goals": ["do not change adjacent behavior"],
            "prerequisites": prerequisites,
        })
    }

    fn writable_deliverables(path: &str) -> quorum_core::decomposition::ChildDeliverables {
        quorum_core::decomposition::ChildDeliverables(vec![
            quorum_core::decomposition::ChildDeliverable::Write { path: path.into() },
        ])
    }

    #[test]
    fn accepts_closed_plan_and_blocker() {
        let plan = serde_json::json!({"outcome":"plan","tasks":[task("core", &[]), task("daemon", &["core", "source:7"])]});
        assert!(matches!(
            validate_submitted(&plan.to_string()),
            Ok(PlannerResponse::Plan { .. })
        ));
        let blocker = serde_json::json!({
            "outcome":"blocker", "category":"missing_decision", "evidence":["two incompatible outcomes are requested"],
            "required_decision":"choose one outcome", "why_no_safe_split":"both children would mutate the same contract"
        });
        assert!(matches!(
            validate_submitted(&blocker.to_string()),
            Ok(PlannerResponse::Blocker { .. })
        ));
    }

    #[test]
    fn parses_writable_and_read_only_deliverables_without_conflating_them() {
        let plan = serde_json::json!({
            "outcome": "plan",
            "tasks": [
                task("core", &[]),
                {
                    "key": "daemon",
                    "title": "Implement daemon boundary",
                    "implementation_delta": "change the daemon boundary",
                    "affected_paths": ["quorum/src/serve/mod.rs"],
                    "observable_outcome": "daemon boundary works",
                    "deliverables": [
                        {"kind": "write", "path": "quorum/src/serve/mod.rs"},
                        {"kind": "read_only_reference", "path": "quorum-core/src/decomposition.rs"}
                    ],
                    "acceptance_criteria": ["boundary is covered"],
                    "source_constraints": ["preserve atomicity"],
                    "verification_expectations": ["focused tests pass"],
                    "non_goals": ["do not change decomposition storage"],
                    "prerequisites": ["core"]
                }
            ]
        });
        let PlannerResponse::Plan { tasks } = validate_submitted(&plan.to_string()).unwrap() else {
            panic!("expected plan");
        };
        assert_eq!(
            tasks[1].deliverables.writable_paths().collect::<Vec<_>>(),
            ["quorum/src/serve/mod.rs"]
        );
        assert_eq!(
            tasks[1].deliverables.reference_paths().collect::<Vec<_>>(),
            ["quorum-core/src/decomposition.rs"]
        );
    }

    #[test]
    fn accepted_proposal_json_rehydrates_structured_deliverables() {
        let accepted = serde_json::json!([
            {
                "key": "core",
                "title": "Implement core boundary",
                "implementation_delta": "change the core boundary",
                "affected_paths": ["quorum-core/src/decomposition.rs"],
                "observable_outcome": "core boundary works",
                "deliverables": [
                    {"kind": "write", "path": "quorum-core/src/decomposition.rs"},
                    {"kind": "read_only_reference", "path": "docs/decomposition.md"}
                ],
                "acceptance_criteria": ["boundary is covered"],
                "source_constraints": ["preserve atomicity"],
                "verification_expectations": ["focused tests pass"],
                "non_goals": ["do not change daemon behavior"],
                "prerequisites": []
            },
            task("other", &[])
        ]);
        let tasks = parse_accepted_proposal(&accepted.to_string()).unwrap();
        assert_eq!(
            tasks[0].deliverables.writable_paths().collect::<Vec<_>>(),
            ["quorum-core/src/decomposition.rs"]
        );
        assert_eq!(
            tasks[0].deliverables.reference_paths().collect::<Vec<_>>(),
            ["docs/decomposition.md"]
        );
    }

    #[test]
    fn wrappers_unknown_fields_and_malformed_json_are_provider_failures() {
        for value in [
            "```json\n{}\n```".to_string(),
            r#"{"outcome":"plan","tasks":[],"extra":true}"#.into(),
            r#"{"outcome":"plan"} trailing"#.into(),
        ] {
            assert!(matches!(
                validate_submitted(&value),
                Err(PlannerParseError::Provider(_))
            ));
        }
    }

    #[test]
    fn invalid_blocker_and_invalid_graph_are_semantic_rejections() {
        let blocker = r#"{"outcome":"blocker","category":"magic","evidence":[],"required_decision":"x","why_no_safe_split":"y"}"#;
        assert!(matches!(
            validate_submitted(blocker),
            Err(PlannerParseError::Semantic(_))
        ));
        let cycle =
            serde_json::json!({"outcome":"plan","tasks":[task("a", &["b"]),task("b", &["a"])]});
        assert!(matches!(
            validate_submitted(&cycle.to_string()),
            Err(PlannerParseError::Semantic(_))
        ));
    }

    #[test]
    fn plan_rejects_missing_repository_grounding_fields() {
        for field in ["implementation_delta", "affected_paths", "non_goals"] {
            let mut first = task("a", &[]);
            first.as_object_mut().unwrap().remove(field);
            let plan = serde_json::json!({
                "outcome": "plan",
                "tasks": [first, task("b", &["a"])]
            });
            assert!(matches!(
                validate_submitted(&plan.to_string()),
                Err(PlannerParseError::Semantic(_))
            ));
        }
    }

    #[test]
    fn durable_legacy_plan_defaults_are_rejected_before_resume() {
        let legacy = serde_json::json!([
            {
                "key": "a", "title": "a", "observable_outcome": "a works",
                "deliverables": [{"kind": "write", "path": "src/a.rs"}],
                "acceptance_criteria": ["covered"], "source_constraints": ["atomic"],
                "verification_expectations": ["tests"], "prerequisites": []
            },
            {
                "key": "b", "title": "b", "observable_outcome": "b works",
                "deliverables": [{"kind": "write", "path": "src/b.rs"}],
                "acceptance_criteria": ["covered"], "source_constraints": ["atomic"],
                "verification_expectations": ["tests"], "prerequisites": ["a"]
            }
        ]);
        let tasks: Vec<ProposedTask> = serde_json::from_value(legacy).unwrap();
        assert!(tasks
            .iter()
            .all(|task| task.implementation_delta.is_empty()));
        assert!(matches!(
            validate_plan_tasks(&tasks),
            Err(PlannerParseError::Semantic(message))
                if message.contains("implementation delta must not be empty")
        ));
    }

    #[test]
    fn maximum_source_constraints_without_worker_guidance_are_semantically_rejected() {
        let mut maximum = task("maximum", &[]);
        let constraints: Vec<String> = (0..MAX_LIST_ITEMS)
            .map(|index| format!("constraint {index}"))
            .collect();
        maximum["source_constraints"] = serde_json::to_value(constraints).unwrap();
        let plan = serde_json::json!({
            "outcome": "plan",
            "tasks": [maximum, task("other", &[])],
        });
        assert_eq!(
            validate_submitted(&plan.to_string()),
            Err(PlannerParseError::Semantic(
                "source constraints at maximum size must include worker writability guidance"
                    .into(),
            ))
        );
    }

    #[test]
    fn durable_maximum_source_constraints_without_worker_guidance_are_rejected() {
        let mut maximum = task("maximum", &[]);
        let constraints: Vec<String> = (0..MAX_LIST_ITEMS)
            .map(|index| format!("constraint {index}"))
            .collect();
        maximum["source_constraints"] = serde_json::to_value(constraints).unwrap();
        let accepted = serde_json::json!([maximum, task("other", &[])]);
        assert_eq!(
            parse_accepted_proposal(&accepted.to_string()),
            Err(PlannerParseError::Semantic(
                "source constraints at maximum size must include worker writability guidance"
                    .into(),
            ))
        );
    }

    #[test]
    fn polling_result_preserves_independent_failure_budgets() {
        assert!(matches!(
            validate_submitted("not json"),
            Err(PlannerParseError::Provider(_))
        ));
        assert!(matches!(
            validate_submitted(r#"{"outcome":"plan","tasks":[]}"#),
            Err(PlannerParseError::Semantic(_))
        ));
    }

    #[test]
    fn provider_models_are_fixed_frontier_high() {
        assert_eq!(CODEX_PLANNER_MODEL, "gpt-5.6-sol");
        assert_eq!(CLAUDE_PLANNER_MODEL, "claude-opus-4-6");
        assert_eq!(PLANNER_EFFORT, "high");
    }

    #[tokio::test]
    async fn oversized_prompt_fails_before_provider_spawn() {
        let prompt = "x".repeat(MAX_PROMPT_BYTES + 1);
        let error = spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            Path::new("."),
            &prompt,
            false,
            Some("provider-must-not-run"),
            None,
        )
        .await
        .err()
        .expect("oversized prompt must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn planner_stdin_feed_timeout_kills_and_reaps_no_read_provider() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("pid");
        let runner = dir.path().join("claude");
        std::fs::write(
            &runner,
            format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nexec sleep 30\n",
                pid_path.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();
        let prompt = "x".repeat(MAX_PROMPT_BYTES - 1024);

        let error = match spawn_planner_with_timeout(
            AgentKind::Claude,
            CLAUDE_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir.path(),
            &prompt,
            false,
            runner.to_str(),
            None,
            TEST_STDIN_FEED_TIMEOUT,
        )
        .await
        {
            Ok(slot) => {
                slot.kill_and_reap().await;
                panic!("a no-read provider unexpectedly accepted the bounded prompt")
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        let pid: i32 = std::fs::read_to_string(pid_path).unwrap().parse().unwrap();
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "planner was not reaped");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn no_newline_stdout_is_rejected_at_the_read_boundary() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let runner = dir.path().join("claude");
        let chunk = "x".repeat(8192);
        std::fs::write(
            &runner,
            format!("#!/bin/sh\nIFS= read -r _turn\nwhile :; do printf '%s' '{chunk}'; done\n"),
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut slot = spawn_planner(
            AgentKind::Claude,
            CLAUDE_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir.path(),
            "bounded prompt",
            false,
            runner.to_str(),
            None,
        )
        .await
        .unwrap();
        let pid = slot.pid().unwrap();
        let outcome = tokio::time::timeout(TEST_BOUNDARY_TIMEOUT, async {
            loop {
                if let Some(outcome) = poll_planner(&mut slot).await {
                    break outcome;
                }
            }
        })
        .await
        .expect("oversized unterminated line must fail promptly");
        assert!(matches!(
            outcome,
            PlannerTurnEnd::Failed(ref message) if message.contains("stdout exceeded")
        ));
        let diagnostic = failure_json(&outcome);
        assert_eq!(
            diagnostic["planner_diagnostic"]["stdout_bytes_observed"],
            MAX_STDOUT_BYTES + 1
        );
        assert_eq!(
            diagnostic["planner_diagnostic"]["stdout_byte_count_kind"],
            "lower-bound"
        );
        assert_eq!(
            diagnostic["planner_diagnostic"]["samples"]["read_boundary_truncated"],
            true
        );

        slot.kill_and_reap().await;
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "planner was not reaped");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_terminal_response_reaches_existing_plan_validation_with_exact_profile() {
        let dir = tempfile::tempdir().unwrap();
        let args_path = dir.path().join("args");
        let response = serde_json::json!({
            "outcome": "plan",
            "tasks": [task("core", &[]), task("daemon", &["core"])]
        });
        let output = format!(
            "{}\n{}\n",
            serde_json::json!({
                "type": "item.completed",
                "item": {"type": "agent_message", "id": "message-1", "text": response.to_string()}
            }),
            serde_json::json!({"type": "turn.completed"})
        );
        let output_path = dir.path().join("stdout.jsonl");
        std::fs::write(&output_path, output).unwrap();
        let fake = executable_script(
            dir.path(),
            "codex",
            &format!(
                "printf '%s\\n' \"$@\" > '{}'\nexec /bin/cat '{}'",
                args_path.display(),
                output_path.display()
            ),
        );
        let mut slot = spawn_planner(
            AgentKind::Codex,
            "gpt-5.6-luna",
            "xhigh",
            dir.path(),
            "exact bounded prompt",
            false,
            fake.to_str(),
            None,
        )
        .await
        .unwrap();
        let outcome = poll_to_terminal(&mut slot).await;
        assert!(matches!(outcome, PlannerTurnEnd::Complete));
        assert!(
            slot.proc.try_wait().unwrap().is_some(),
            "Codex terminal evidence preceded provider exit"
        );
        slot.kill_and_reap().await;

        let args = std::fs::read_to_string(args_path).unwrap();
        let args: Vec<&str> = args.lines().collect();
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--model", "gpt-5.6-luna"]));
        assert!(args
            .windows(2)
            .any(|pair| pair == ["-c", "model_reasoning_effort=xhigh"]));
        assert_eq!(args.last(), Some(&"exact bounded prompt"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_planner_survives_exploration_stdout_before_terminal_response() {
        let dir = tempfile::tempdir().unwrap();
        let response = serde_json::json!({
            "outcome": "plan",
            "tasks": [task("core", &[]), task("daemon", &["core"])]
        });
        let exploration = serde_json::json!({
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "id": "tool-1",
                "aggregated_output": "x".repeat(1024),
            }
        });
        let mut output = format!("{}\n", exploration).repeat(210);
        assert!(output.len() > 200 * 1024);
        output.push_str(&format!(
            "{}\n{}\n",
            serde_json::json!({
                "type": "item.completed",
                "item": {"type": "agent_message", "id": "message-1", "text": response.to_string()}
            }),
            serde_json::json!({"type": "turn.completed"})
        ));

        let mut slot = spawn_fake_codex(dir.path(), &output).await;
        let outcome = poll_to_terminal(&mut slot).await;
        assert!(matches!(outcome, PlannerTurnEnd::Complete));
        slot.kill_and_reap().await;
    }

    /// The regression this cutover exists for, inverted: a provider that prints
    /// a flawless plan and exits cleanly has still not reported one. Text is
    /// never an outcome, and the failure keeps the payload out of durable
    /// evidence.
    #[cfg(unix)]
    #[tokio::test]
    async fn perfect_json_text_without_submit_plan_is_a_provider_failure() {
        let dir = tempfile::tempdir().unwrap();
        let response = serde_json::json!({
            "outcome": "plan",
            "tasks": [task("core", &[]), task("daemon", &["core"])]
        });
        // Deserializing this text would produce a valid plan; nothing does.
        assert!(matches!(
            validate_submitted(&response.to_string()),
            Ok(PlannerResponse::Plan { .. })
        ));
        let output = format!(
            "{}\n{}\n",
            serde_json::json!({
                "type": "item.completed",
                "item": {"type": "agent_message", "id": "message-1", "text": response.to_string()}
            }),
            serde_json::json!({"type": "turn.completed"})
        );

        let mut slot = spawn_fake_codex(dir.path(), &output).await;
        let turn_end = poll_to_terminal(&mut slot).await;
        assert!(matches!(turn_end, PlannerTurnEnd::Complete));
        let outcome = planner_outcome(&mut slot, turn_end, None);
        let PlannerPoll::ProviderFailed(summary) = outcome else {
            panic!("a turn without an accepted submission must not produce a plan");
        };
        assert!(summary.contains("without submit_plan"), "{summary}");
        assert!(
            !summary.contains("observable_outcome"),
            "durable failure retained provider payload text"
        );
        let diagnostic: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            diagnostic["planner_diagnostic"]["terminal_response_seen"],
            true
        );
        slot.kill_and_reap().await;
    }

    /// The durable submission is the outcome, and it stands however the process
    /// then ended: it was accepted by the daemon's own endpoint after full
    /// validation, so a provider that died after submitting still delivered a
    /// plan.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_accepted_submission_is_the_only_source_of_a_plan() {
        let dir = tempfile::tempdir().unwrap();
        let submitted = serde_json::json!({
            "outcome": "plan",
            "tasks": [task("core", &[]), task("daemon", &["core"])]
        })
        .to_string();
        // Text that contradicts the submission is never consulted.
        let output = format!(
            "{}\n{}\n",
            serde_json::json!({
                "type": "item.completed",
                "item": {"type": "agent_message", "id": "message-1", "text": "narration only"}
            }),
            serde_json::json!({"type": "turn.completed"})
        );

        let mut slot = spawn_fake_codex(dir.path(), &output).await;
        let turn_end = poll_to_terminal(&mut slot).await;
        assert!(matches!(
            planner_outcome(&mut slot, turn_end, Some(&submitted)),
            PlannerPoll::Done(PlannerResponse::Plan { ref tasks })
                if tasks.len() == 2 && tasks[1].prerequisites == ["core"]
        ));
        assert!(matches!(
            planner_outcome(
                &mut slot,
                PlannerTurnEnd::Failed("planner timed out".into()),
                Some(&submitted),
            ),
            PlannerPoll::Done(PlannerResponse::Plan { .. })
        ));
        assert!(matches!(
            planner_outcome(
                &mut slot,
                PlannerTurnEnd::Failed("planner timed out".into()),
                None,
            ),
            PlannerPoll::ProviderFailed(ref summary) if summary == "planner timed out"
        ));
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn accepted_submission_after_provider_failure_logs_completed_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let submitted = serde_json::json!({
            "outcome": "plan",
            "tasks": [task("core", &[]), task("daemon", &["core"])]
        })
        .to_string();
        let output = format!(
            "{}\n",
            serde_json::json!({
                "type": "turn.failed",
                "error": {"message": "provider crashed after submit_plan"}
            })
        );
        let mut slot = spawn_fake_codex(dir.path(), &output).await;
        slot.start_session_log(
            dir.path(),
            "decomposition-planner-test",
            42,
            "session",
            "frozen-base",
            1,
        )
        .unwrap();
        let log_dir = slot.log_dir().unwrap().to_path_buf();

        let turn_end = poll_to_terminal(&mut slot).await;
        assert!(matches!(
            planner_outcome(&mut slot, turn_end, Some(&submitted)),
            PlannerPoll::Done(PlannerResponse::Plan { .. })
        ));

        let stream = std::fs::read_to_string(log_dir.join("stream.jsonl")).unwrap();
        let events = stream
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(events
            .iter()
            .any(|event| event["event"] == "provider_failure"));
        let completions = events
            .iter()
            .filter(|event| event["event"] == "completion")
            .collect::<Vec<_>>();
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0]["outcome"], "completed");

        slot.kill_and_reap().await;
    }

    /// Claude's terminal branch, which used to select the final assistant
    /// message: a full narration transcript now ends the turn cleanly and
    /// carries no plan, so the attempt fails on the missing submission alone.
    #[cfg(unix)]
    #[tokio::test]
    async fn claude_terminal_result_completes_the_turn_without_reading_its_text() {
        let dir = tempfile::tempdir().unwrap();
        let response = serde_json::json!({
            "outcome": "plan",
            "tasks": [task("core", &[]), task("daemon", &["core"])]
        });
        let output = claude_stream(&[
            serde_json::json!({"type": "assistant", "message": {"content": "Let me inspect the task."}}),
            serde_json::json!({"type": "tool_use", "name": "Read", "input": {"file_path": "src/core.rs"}}),
            // The final assistant message is a complete, valid plan. It is not
            // an outcome: only an accepted `submit_plan` call is.
            serde_json::json!({"type": "assistant", "message": {"content": response.to_string()}}),
        ]);

        let mut slot = spawn_fake_claude(dir.path(), &output).await;
        let turn_end = poll_to_terminal(&mut slot).await;
        assert!(matches!(turn_end, PlannerTurnEnd::Complete));
        let PlannerPoll::ProviderFailed(summary) = planner_outcome(&mut slot, turn_end, None)
        else {
            panic!("Claude assistant text must not produce a plan");
        };
        assert!(summary.contains("without submit_plan"), "{summary}");
        assert!(
            !summary.contains("observable_outcome"),
            "durable failure retained provider payload text"
        );
        slot.kill_and_reap().await;
    }

    /// An errored terminal result is still a provider failure: the turn never
    /// reached a clean end, so its own diagnostic is what the attempt reports.
    #[cfg(unix)]
    #[tokio::test]
    async fn claude_errored_terminal_result_fails_the_turn() {
        let dir = tempfile::tempdir().unwrap();
        let output = format!(
            "{}\n{}\n",
            serde_json::json!({"type": "assistant", "message": {"content": "Let me inspect the task."}}),
            serde_json::json!({"type": "result", "result": "", "is_error": true}),
        );

        let mut slot = spawn_fake_claude(dir.path(), &output).await;
        assert!(matches!(
            poll_to_terminal(&mut slot).await,
            PlannerTurnEnd::Failed(ref summary) if summary.contains("provider returned an error")
        ));
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_planner_accepts_changed_unclassified_stderr_before_clean_terminal() {
        let response = serde_json::json!({
            "outcome": "plan",
            "tasks": [task("core", &[]), task("daemon", &["core"])]
        });
        let stdout = format!(
            "{}\n{}\n{}\n",
            serde_json::json!({"type":"thread.started","thread_id":"fixture-thread"}),
            serde_json::json!({
                "type": "item.completed",
                "item": {"type": "agent_message", "id": "message-1", "text": response.to_string()}
            }),
            serde_json::json!({"type":"turn.completed"}),
        );
        for stderr in [
            "Reading additional input from stdin...\n",
            "Codex changed its informational input notice.\n",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut slot = spawn_fake_codex_with_stderr(dir.path(), &stdout, stderr).await;
            let outcome = poll_to_terminal(&mut slot).await;
            assert!(matches!(outcome, PlannerTurnEnd::Complete));
            slot.kill_and_reap().await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_planner_unclassified_stderr_still_fails_without_clean_terminal() {
        let stderr = "Codex changed its informational input notice.\n";
        let cases = [
            (
                "turn-failed",
                "{\"type\":\"turn.failed\",\"error\":{\"message\":\"provider failed\"}}\n",
                "planner provider protocol failed (unclassified)",
            ),
            (
                "error-event",
                "{\"type\":\"error\",\"message\":\"provider failed\"}\n",
                "planner provider protocol failed (unclassified)",
            ),
            (
                "malformed",
                "not-json\n",
                "planner provider protocol failed",
            ),
            (
                "early-eof",
                "{\"type\":\"thread.started\",\"thread_id\":\"fixture-thread\"}\n",
                "Codex stderr did not match a bounded provider signal",
            ),
        ];
        for (name, stdout, expected) in cases {
            let dir = tempfile::tempdir().unwrap();
            let mut slot = spawn_fake_codex_with_stderr(dir.path(), stdout, stderr).await;
            let outcome = poll_to_terminal(&mut slot).await;
            let diagnostic = failure_json(&outcome);
            assert!(
                diagnostic["failure"].as_str().unwrap().contains(expected),
                "{name} discarded its authoritative bounded failure evidence: {diagnostic}",
            );
            slot.kill_and_reap().await;
        }

        let dir = tempfile::tempdir().unwrap();
        let stdout_path = dir.path().join("stdout.jsonl");
        let stderr_path = dir.path().join("stderr.txt");
        std::fs::write(
            &stdout_path,
            "{\"type\":\"thread.started\",\"thread_id\":\"fixture-thread\"}\n",
        )
        .unwrap();
        std::fs::write(&stderr_path, stderr).unwrap();
        let runner = executable_script(
            dir.path(),
            "codex",
            &format!(
                "/bin/cat '{}' >&2\n/bin/cat '{}'\nexit 7",
                stderr_path.display(),
                stdout_path.display()
            ),
        );
        let mut slot = spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir.path(),
            "bounded prompt",
            false,
            runner.to_str(),
            None,
        )
        .await
        .unwrap();
        let outcome = poll_to_terminal(&mut slot).await;
        let diagnostic = failure_json(&outcome);
        assert!(diagnostic["failure"]
            .as_str()
            .unwrap()
            .contains("Codex stderr did not match a bounded provider signal"));
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_planner_rejects_truncated_stderr_after_terminal() {
        let response = serde_json::json!({"outcome":"plan","tasks":[task("core", &[])]});
        let stdout = format!(
            "{}\n{}\n",
            serde_json::json!({
                "type": "item.completed",
                "item": {"type": "agent_message", "id": "message-1", "text": response.to_string()}
            }),
            serde_json::json!({"type":"turn.completed"}),
        );
        let dir = tempfile::tempdir().unwrap();
        let stderr = "x".repeat(16 * 1024 + 1);
        let mut slot = spawn_fake_codex_with_stderr(dir.path(), &stdout, &stderr).await;
        let outcome = poll_to_terminal(&mut slot).await;
        assert!(matches!(
            outcome,
            PlannerTurnEnd::Failed(ref summary) if summary.contains("stderr exceeded bounded")
        ));
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_terminal_candidate_requires_clean_exit_and_final_evidence() {
        let response = serde_json::json!({
            "outcome": "plan",
            "tasks": [task("core", &[]), task("daemon", &["core"])]
        });
        let terminal_output = format!(
            "{}\n{}\n",
            serde_json::json!({
                "type": "item.completed",
                "item": {"type": "agent_message", "id": "message-1", "text": response.to_string()}
            }),
            serde_json::json!({"type": "turn.completed"})
        );
        let cases = [
            ("nonzero-exit", "exit 7"),
            (
                "nonzero-exit-with-informational-stderr",
                "printf '%s\\n' 'Codex changed its informational input notice.' >&2; exit 7",
            ),
            (
                "trailing-stdout",
                "printf '%s\\n' '{\"type\":\"error\",\"message\":\"fatal trailing error\"}'",
            ),
            (
                "trailing-stderr",
                "printf '%s\\n' 'error: unexpected argument --future' >&2",
            ),
        ];
        for (name, trailer) in cases {
            let dir = tempfile::tempdir().unwrap();
            let output_path = dir.path().join("stdout.jsonl");
            std::fs::write(&output_path, &terminal_output).unwrap();
            let runner = executable_script(
                dir.path(),
                "codex",
                &format!("/bin/cat '{}'\n{trailer}", output_path.display()),
            );
            let mut slot = spawn_planner(
                AgentKind::Codex,
                CODEX_PLANNER_MODEL,
                PLANNER_EFFORT,
                dir.path(),
                "bounded prompt",
                false,
                runner.to_str(),
                None,
            )
            .await
            .unwrap();
            let outcome = poll_to_terminal(&mut slot).await;
            assert!(
                matches!(outcome, PlannerTurnEnd::Failed(_)),
                "{name} acquired planner authority"
            );
            if name == "trailing-stderr" {
                let diagnostic = failure_json(&outcome);
                assert!(diagnostic["failure"]
                    .as_str()
                    .unwrap()
                    .contains("non-failover"));
            }
            if name == "nonzero-exit-with-informational-stderr" {
                let diagnostic = failure_json(&outcome);
                let failure = diagnostic["failure"].as_str().unwrap();
                assert!(failure.contains("unclassified"));
                assert!(failure.contains("Codex stderr did not match a bounded provider signal"));
                assert!(
                    !failure.contains("Codex changed its informational input notice."),
                    "durable failure retained raw provider stderr"
                );
            }
            slot.kill_and_reap().await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_malformed_error_and_missing_terminal_streams_fail_without_a_plan() {
        let cases = [
            ("malformed", "not-json\n"),
            (
                "provider-error",
                "{\"type\":\"error\",\"message\":\"provider exploded\"}\n",
            ),
            (
                "missing-terminal",
                "{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"id\":\"m1\",\"text\":\"{}\"}}\n",
            ),
        ];
        for (name, stdout) in cases {
            let dir = tempfile::tempdir().unwrap();
            let mut slot = spawn_fake_codex(dir.path(), stdout).await;
            let outcome = poll_to_terminal(&mut slot).await;
            assert!(
                matches!(outcome, PlannerTurnEnd::Failed(_)),
                "{name} stream created planner authority"
            );
            let diagnostic = failure_json(&outcome);
            assert_eq!(diagnostic["planner_diagnostic"]["provider"], "codex");
            assert_eq!(
                diagnostic["planner_diagnostic"]["samples"]["payloads_redacted"],
                true
            );
            assert!(
                !diagnostic
                    .to_string()
                    .contains("provider-credential-must-not-persist"),
                "{name} persisted provider payload text"
            );
            slot.kill_and_reap().await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn repeated_protocol_events_retain_bounded_head_tail_and_counts() {
        let dir = tempfile::tempdir().unwrap();
        let output = "{\"type\":\"turn.started\"}\n".repeat(8_000);
        let mut slot = spawn_fake_codex(dir.path(), &output).await;
        let pid = slot.pid().unwrap();
        let outcome = poll_to_terminal(&mut slot).await;
        let diagnostic = failure_json(&outcome);
        let planner = &diagnostic["planner_diagnostic"];
        assert!(planner["event_types"]["turn.started"].as_u64().unwrap() > 1_000);
        assert_eq!(planner["terminal_response_seen"], false);
        assert_eq!(planner["samples"]["truncated"], true);
        assert_eq!(
            planner["samples"]["beginning"].as_array().unwrap().len(),
            DIAGNOSTIC_SAMPLE_LINES
        );
        assert_eq!(
            planner["samples"]["end"].as_array().unwrap().len(),
            DIAGNOSTIC_SAMPLE_LINES
        );
        assert!(planner["samples"]["beginning"][0]
            .as_str()
            .unwrap()
            .contains("event=turn.started"));
        slot.kill_and_reap().await;
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "planner was not reaped");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_utf8_read_failure_retains_text_safe_bounded_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let runner = executable_script(dir.path(), "codex", "printf '\\377\\n'");
        let mut slot = spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir.path(),
            "bounded prompt",
            false,
            runner.to_str(),
            None,
        )
        .await
        .unwrap();
        let outcome = poll_to_terminal(&mut slot).await;
        assert!(matches!(
            outcome,
            PlannerTurnEnd::Failed(ref summary) if summary.contains("stdout read failed")
        ));
        let diagnostic = failure_json(&outcome);
        assert_eq!(
            diagnostic["planner_diagnostic"]["stdout_byte_count_kind"],
            "completed-lines-only"
        );
        assert_eq!(
            diagnostic["planner_diagnostic"]["samples"]["read_boundary_truncated"],
            true
        );
        slot.kill_and_reap().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_unterminated_and_stdout_bounds_fail_and_reap_without_a_plan() {
        let dir = tempfile::tempdir().unwrap();
        // A single large assistant message is no longer measured against a
        // response bound — nothing reads it. Only the stream's own terminal
        // contract and the stdout ceiling bound the turn.
        let unterminated_text = "x".repeat(64 * 1024 + 1);
        let output = format!(
            "{}\n",
            serde_json::json!({
                "type": "item.completed",
                "item": {"type": "agent_message", "id": "message-1", "text": unterminated_text}
            })
        );
        let mut slot = spawn_fake_codex(dir.path(), &output).await;
        let outcome = poll_to_terminal(&mut slot).await;
        assert!(matches!(
            outcome,
            PlannerTurnEnd::Failed(ref message)
                if message.contains("exited without a terminal response")
        ));
        let response_diagnostic = failure_json(&outcome);
        assert_eq!(
            response_diagnostic["planner_diagnostic"]["event_types"]
                ["item.completed/agent_message"],
            1
        );
        assert_eq!(
            response_diagnostic["planner_diagnostic"]["terminal_response_seen"],
            false
        );
        slot.kill_and_reap().await;

        let dir = tempfile::tempdir().unwrap();
        let runner = executable_script(dir.path(), "codex", "while :; do printf '%08192d' 0; done");
        let mut slot = spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            dir.path(),
            "bounded prompt",
            false,
            runner.to_str(),
            None,
        )
        .await
        .unwrap();
        let pid = slot.pid().unwrap();
        let outcome = poll_to_terminal(&mut slot).await;
        assert!(matches!(
            outcome,
            PlannerTurnEnd::Failed(ref message) if message.contains("stdout exceeded")
        ));
        let stdout_diagnostic = failure_json(&outcome);
        assert_eq!(
            stdout_diagnostic["planner_diagnostic"]["stdout_bytes_observed"],
            MAX_STDOUT_BYTES + 1
        );
        slot.kill_and_reap().await;
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "planner was not reaped");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_timeout_and_cancellation_reap_the_process_group() {
        for timed_out in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let runner = executable_script(dir.path(), "codex", "exec sleep 30");
            let mut slot = spawn_planner(
                AgentKind::Codex,
                CODEX_PLANNER_MODEL,
                PLANNER_EFFORT,
                dir.path(),
                "bounded prompt",
                false,
                runner.to_str(),
                None,
            )
            .await
            .unwrap();
            let pid = slot.pid().unwrap();
            if timed_out {
                slot.started_at = tokio::time::Instant::now() - PLANNER_TIMEOUT;
                let outcome = poll_planner(&mut slot).await.expect("timeout is terminal");
                assert!(matches!(
                    outcome,
                    PlannerTurnEnd::Failed(ref message) if message.contains("timed out")
                ));
                let diagnostic = failure_json(&outcome);
                assert_eq!(diagnostic["planner_diagnostic"]["stdout_bytes_observed"], 0);
                assert_eq!(
                    diagnostic["planner_diagnostic"]["stdout_byte_count_kind"],
                    "lower-bound"
                );
                assert_eq!(
                    diagnostic["planner_diagnostic"]["terminal_response_seen"],
                    false
                );
            }
            slot.kill_and_reap().await;
            assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "planner was not reaped");
        }
    }

    #[tokio::test]
    async fn source_validation_rejects_foreign_dependencies_and_synthetic_integration() {
        let path_resolver = WritablePathResolver::default();
        let foreign = ProposedTask {
            key: "a".into(),
            title: "Implement a".into(),
            implementation_delta: "change dependency handling".into(),
            affected_paths: vec!["src/a.rs".into()],
            observable_outcome: "a works".into(),
            deliverables: quorum_core::decomposition::ChildDeliverables(vec![
                quorum_core::decomposition::ChildDeliverable::Write {
                    path: "src/a.rs".into(),
                },
            ]),
            acceptance_criteria: vec!["covered".into()],
            source_constraints: vec!["atomic".into()],
            verification_expectations: vec!["tests".into()],
            non_goals: vec!["no unrelated changes".into()],
            prerequisites: vec!["source:9".into()],
        };
        assert!(matches!(
            validate_for_source(&[foreign], &[7], Path::new("."), &path_resolver,).await,
            Err(PlannerParseError::Semantic(_))
        ));
        let synthetic = ProposedTask {
            key: "integration".into(),
            title: "Integration task".into(),
            implementation_delta: "merge sibling changes".into(),
            affected_paths: vec!["src/integration.rs".into()],
            observable_outcome: "merge all siblings".into(),
            deliverables: quorum_core::decomposition::ChildDeliverables(vec![
                quorum_core::decomposition::ChildDeliverable::Write {
                    path: "src/integration.rs".into(),
                },
            ]),
            acceptance_criteria: vec!["covered".into()],
            source_constraints: vec!["atomic".into()],
            verification_expectations: vec!["tests".into()],
            non_goals: vec!["no unrelated changes".into()],
            prerequisites: vec![],
        };
        assert!(matches!(
            validate_for_source(&[synthetic], &[], Path::new("."), &path_resolver,).await,
            Err(PlannerParseError::Semantic(_))
        ));
    }

    #[tokio::test]
    async fn source_validation_inspects_only_requested_writes() {
        let path_resolver = WritablePathResolver::default();
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo");
        let sibling = root.path().join("sibling");
        std::fs::create_dir(&repo).unwrap();
        std::fs::create_dir(&sibling).unwrap();
        let external = sibling.join("context.rs").to_string_lossy().into_owned();
        let task = ProposedTask {
            key: "bounded".into(),
            title: "Implement bounded change".into(),
            implementation_delta: "change the bounded implementation".into(),
            affected_paths: vec!["src/in_repo.rs".into()],
            observable_outcome: "bounded change works".into(),
            deliverables: quorum_core::decomposition::ChildDeliverables(vec![
                quorum_core::decomposition::ChildDeliverable::Write {
                    path: "src/in_repo.rs".into(),
                },
                quorum_core::decomposition::ChildDeliverable::ReadOnlyReference { path: external },
                quorum_core::decomposition::ChildDeliverable::ReadOnlyReference {
                    path: "../sibling/other-context.rs".into(),
                },
            ]),
            acceptance_criteria: vec!["covered".into()],
            source_constraints: vec!["bounded".into()],
            verification_expectations: vec!["tests".into()],
            non_goals: vec!["do not change external references".into()],
            prerequisites: vec![],
        };

        assert_eq!(
            validate_for_source_with_resolver(
                &[task],
                &[],
                &repo,
                &path_resolver,
                WRITABLE_PATH_RESOLUTION_TIMEOUT,
                |repo_root, paths| {
                    paths.iter().all(|path| {
                        quorum_core::decomposition::classify_writable_deliverable_path_blocking(
                            &repo_root, path,
                        ) == quorum_core::decomposition::WritableDeliverablePath::Permitted
                    })
                },
            )
            .await,
            Ok(())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn source_validation_rejects_every_repository_escape_shape() {
        let path_resolver = WritablePathResolver::default();
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo");
        let sibling = root.path().join("sibling");
        std::fs::create_dir(&repo).unwrap();
        std::fs::create_dir(&sibling).unwrap();
        std::os::unix::fs::symlink(&sibling, repo.join("external-link")).unwrap();

        for path in [
            "../sibling/output.rs".to_string(),
            "nested/../../sibling/output.rs".to_string(),
            sibling.join("output.rs").to_string_lossy().into_owned(),
            "external-link/new/output.rs".to_string(),
        ] {
            let task = ProposedTask {
                key: "escaping".into(),
                title: "Implement escaping change".into(),
                implementation_delta: "write the declared output".into(),
                affected_paths: vec![path.clone()],
                observable_outcome: "escaping change works".into(),
                deliverables: quorum_core::decomposition::ChildDeliverables(vec![
                    quorum_core::decomposition::ChildDeliverable::Write { path: path.clone() },
                ]),
                acceptance_criteria: vec!["covered".into()],
                source_constraints: vec!["bounded".into()],
                verification_expectations: vec!["tests".into()],
                non_goals: vec!["do not write external paths".into()],
                prerequisites: vec![],
            };
            assert!(
                matches!(
                    validate_for_source_with_resolver(
                        &[task],
                        &[],
                        &repo,
                        &path_resolver,
                        WRITABLE_PATH_RESOLUTION_TIMEOUT,
                        |repo_root, paths| {
                            paths.iter().all(|path| {
                                quorum_core::decomposition::classify_writable_deliverable_path_blocking(
                                    &repo_root, path,
                                ) == quorum_core::decomposition::WritableDeliverablePath::Permitted
                            })
                        },
                    )
                    .await,
                    Err(PlannerParseError::Semantic(ref error))
                        if error.contains("writable deliverable")
                            && error.contains("managed repository")
                            && error.contains("read_only_reference")
                ),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn source_validation_times_out_blocking_filesystem_resolution() {
        let path_resolver = WritablePathResolver::default();
        let repo = tempfile::tempdir().unwrap();
        let task = ProposedTask {
            key: "bounded".into(),
            title: "Implement bounded change".into(),
            observable_outcome: "bounded change works".into(),
            deliverables: quorum_core::decomposition::ChildDeliverables(vec![
                quorum_core::decomposition::ChildDeliverable::Write {
                    path: "src/in_repo.rs".into(),
                },
            ]),
            acceptance_criteria: vec!["covered".into()],
            source_constraints: vec!["bounded".into()],
            verification_expectations: vec!["tests".into()],
            prerequisites: vec![],
            ..Default::default()
        };
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let started = std::time::Instant::now();
        let outcome = validate_for_source_with_resolver(
            &[task],
            &[],
            repo.path(),
            &path_resolver,
            Duration::from_millis(25),
            move |_, _| {
                release_rx.recv().unwrap();
                true
            },
        )
        .await;
        release_tx.send(()).unwrap();

        assert!(
            matches!(outcome, Err(PlannerParseError::Semantic(ref error))
                if error == "a writable deliverable resolves outside the managed repository; use an in-repository write path or declare external context as read_only_reference")
        );
        assert!(started.elapsed() < Duration::from_millis(500));
        tokio::time::timeout(Duration::from_secs(1), async {
            while path_resolver.is_active() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("released resolver exits");
    }

    #[test]
    fn repeated_resolution_timeouts_do_not_queue_or_starve_blocking_pool() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let path_resolver = WritablePathResolver::default();
            let repo = tempfile::tempdir().unwrap();
            let db_path = repo.path().join("quorum.db");
            drop(quorum_core::db::open(&db_path).unwrap());
            let task = ProposedTask {
                key: "bounded".into(),
                title: "Implement bounded change".into(),
                observable_outcome: "bounded change works".into(),
                deliverables: quorum_core::decomposition::ChildDeliverables(vec![
                    quorum_core::decomposition::ChildDeliverable::Write {
                        path: "src/in_repo.rs".into(),
                    },
                ]),
                acceptance_criteria: vec!["covered".into()],
                source_constraints: vec!["bounded".into()],
                verification_expectations: vec!["tests".into()],
                prerequisites: vec![],
                ..Default::default()
            };
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let calls_in_resolver = Arc::clone(&calls);
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let first = validate_for_source_with_resolver(
                std::slice::from_ref(&task),
                &[],
                repo.path(),
                &path_resolver,
                Duration::from_millis(10),
                move |_, _| {
                    calls_in_resolver.fetch_add(1, Ordering::SeqCst);
                    release_rx.recv().unwrap();
                    true
                },
            )
            .await;
            assert!(matches!(first, Err(PlannerParseError::Semantic(_))));

            for _ in 0..16 {
                let outcome = validate_for_source_with_resolver(
                    std::slice::from_ref(&task),
                    &[],
                    repo.path(),
                    &path_resolver,
                    Duration::from_millis(10),
                    |_, _| panic!("occupied resolver slot queued another job"),
                )
                .await;
                assert!(matches!(outcome, Err(PlannerParseError::Semantic(_))));
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(path_resolver.is_active());

            let db_style_work = tokio::time::timeout(
                Duration::from_secs(1),
                tokio::task::spawn_blocking(move || {
                    let conn = quorum_core::db::open(&db_path).unwrap();
                    conn.query_row("SELECT 42", [], |row| row.get::<_, i64>(0))
                        .unwrap()
                }),
            )
            .await
            .expect("resolver must not starve Tokio's blocking pool")
            .expect("DB-style blocking work joins");
            assert_eq!(db_style_work, 42);

            release_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), async {
                while path_resolver.is_active() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("released resolver exits");
        });
    }

    #[tokio::test]
    async fn source_validation_rejects_absolute_external_path_before_resolver() {
        let path_resolver = WritablePathResolver::default();
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo");
        let external = root.path().join("unavailable-mount/output.rs");
        std::fs::create_dir(&repo).unwrap();
        let task = ProposedTask {
            key: "external".into(),
            title: "Implement external change".into(),
            observable_outcome: "external change works".into(),
            deliverables: quorum_core::decomposition::ChildDeliverables(vec![
                quorum_core::decomposition::ChildDeliverable::Write {
                    path: external.to_string_lossy().into_owned(),
                },
            ]),
            acceptance_criteria: vec!["covered".into()],
            source_constraints: vec!["bounded".into()],
            verification_expectations: vec!["tests".into()],
            prerequisites: vec![],
            ..Default::default()
        };

        let outcome = validate_for_source_with_resolver(
            &[task],
            &[],
            &repo,
            &path_resolver,
            Duration::from_secs(1),
            |_, _| panic!("lexically external path reached filesystem resolver"),
        )
        .await;
        assert!(matches!(
            outcome,
            Err(PlannerParseError::Semantic(ref error))
                if error.contains("child external writable deliverable")
                    && error.contains("unavailable-mount/output.rs")
                    && error.contains("read_only_reference")
        ));
    }

    #[test]
    fn planner_prompt_declares_the_exact_closed_plan_and_blocker_shapes() {
        let dependencies = vec![3, 4];
        let source = PlanningSource {
            task_id: 7,
            revision: 2,
            title: "large outcome",
            body: Some("preserve atomicity"),
            dependencies: &dependencies,
        };
        let prompt = build_prompt(&source, &[]);
        assert!(prompt.contains("do not omit, rename, or add fields"));
        assert!(prompt.contains(r#""implementation_delta":"<new-code-or-documentation-change>""#));
        assert!(prompt.contains(r#""affected_paths":["<repo-relative-path-or-narrow-pattern>"]"#));
        assert!(prompt.contains(r#""deliverables":[{"kind":"write""#));
        assert!(prompt.contains(r#""non_goals":["<preserved-or-explicitly-excluded-behavior>"]"#));
        assert!(!prompt.contains("preserved_literals"));
        assert!(prompt.contains(
            r#"BLOCKER={"outcome":"blocker","category":"<ambiguous_scope|missing_decision|external_constraint|no_safe_split>","evidence":["<evidence>"],"required_decision":"<decision>","why_no_safe_split":"<reason>"}"#
        ));
        assert!(prompt.contains("`outcome` must be exactly `plan` or `blocker`"));
        assert!(prompt.contains("S = focused/local; M = bounded coherent work"));
        assert!(prompt.contains("Use at most 5 Grep/Glob calls and 10 Read calls"));
        assert!(prompt.contains("Arbiter judges that faithfulness"));
        assert!(prompt.contains(&format!(
            "Every PLAN task's `source_constraints` must include this worker-facing guidance: \"{WORKER_WRITABILITY_GUIDANCE}\""
        )));
        assert!(prompt.contains("The daemon adds it deterministically"));
    }

    /// The prompt must send the plan through the tool, not the transcript. The
    /// old "one JSON object" instruction is the failure mode this batch removed:
    /// conversational providers prefixed prose and lost a whole attempt.
    #[test]
    fn planner_prompt_instructs_the_submit_plan_tool_and_forbids_a_text_plan() {
        let dependencies = vec![3, 4];
        let source = PlanningSource {
            task_id: 7,
            revision: 2,
            title: "large outcome",
            body: Some("preserve atomicity"),
            dependencies: &dependencies,
        };
        let prompt = build_prompt(&source, &[]);
        assert!(!prompt.contains("Return exactly one valid JSON object"));
        assert!(!prompt.contains("Use no markdown or commentary"));
        assert!(prompt.contains(
            "Report by calling the `submit_plan` tool exactly once with the PLAN or BLOCKER \
             object as its `response` argument"
        ));
        assert!(prompt
            .contains("fix the reported defect and call `submit_plan` again in the same turn"));
        assert!(prompt.contains("`already_submitted` means your first plan was accepted"));
        assert!(prompt.contains("Never print the plan as text"));
        // The closed shapes stay in the prompt as well as the tool schema.
        assert!(prompt.contains(RESPONSE_SHAPES));
    }

    #[tokio::test]
    async fn literal_dense_proposal_passes_structural_validation_without_a_byte_exact_gate() {
        // Regression for #48/#58: the byte-exact `validate_source_literals` gate
        // is gone. `validate_for_source` no longer receives the source text and
        // never rejects a structurally valid proposal for failing to echo any
        // backtick-delimited span verbatim. A literal-dense source now reaches
        // the Arbiter instead of exhausting the proposal budget here.
        let path_resolver = WritablePathResolver::default();
        let proposal = vec![
            ProposedTask {
                key: "core".into(),
                title: "Change core seam".into(),
                implementation_delta: "change the core implementation seam".into(),
                affected_paths: vec!["src/core.rs".into()],
                observable_outcome: "core works".into(),
                deliverables: writable_deliverables("src/core.rs"),
                acceptance_criteria: vec!["covered".into()],
                source_constraints: vec!["preserve behavior".into()],
                verification_expectations: vec!["tests pass".into()],
                non_goals: vec!["no unrelated changes".into()],
                prerequisites: vec![],
            },
            ProposedTask {
                key: "verify".into(),
                title: "Verify core seam".into(),
                implementation_delta: "add focused core verification".into(),
                affected_paths: vec!["tests/core.rs".into()],
                observable_outcome: "core verification works".into(),
                deliverables: writable_deliverables("tests/core.rs"),
                acceptance_criteria: vec!["verification is covered".into()],
                source_constraints: vec!["preserve behavior".into()],
                verification_expectations: vec!["tests pass".into()],
                non_goals: vec!["no unrelated changes".into()],
                prerequisites: vec!["core".into()],
            },
        ];
        // The proposal echoes none of the source's 14 backtick spans verbatim;
        // under the retired gate this would have been a semantic rejection.
        let outcome = validate_for_source_with_resolver(
            &proposal,
            &[],
            Path::new("."),
            &path_resolver,
            WRITABLE_PATH_RESOLUTION_TIMEOUT,
            |_repo_root, _paths| true,
        )
        .await;
        assert!(outcome.is_ok(), "{outcome:?}");
    }

    #[test]
    fn planner_prompt_bounds_retry_feedback_and_instructs_its_use() {
        let dependencies = vec![3, 4];
        let source = PlanningSource {
            task_id: 7,
            revision: 2,
            title: "large outcome",
            body: Some("preserve atomicity"),
            dependencies: &dependencies,
        };
        let prompt = build_prompt(
            &source,
            &[
                "cycle detected".into(),
                "bad field".into(),
                "x".repeat(MAX_REJECTION_SUMMARY_BYTES + 1),
                "must not be included".into(),
            ],
        );
        assert!(prompt.len() < MAX_PROMPT_BYTES);
        assert!(prompt.contains("cycle detected"));
        assert!(!prompt.contains(&"x".repeat(MAX_REJECTION_SUMMARY_BYTES + 1)));
        assert!(!prompt.contains("must not be included"));
        assert!(prompt.contains(&format!(
            "PRIOR_REJECTIONS contains at most {MAX_REJECTION_SUMMARIES} summaries, each truncated to {MAX_REJECTION_SUMMARY_BYTES} bytes"
        )));
        assert!(prompt
            .contains("On retry, use only those summaries to correct the cited semantic defect"));
    }

    #[test]
    fn worker_writability_guidance_is_added_without_trusting_the_planner() {
        let constraints = vec!["preserve atomicity".into()];
        let with_guidance = with_worker_writability_guidance(&constraints);
        assert_eq!(with_guidance.len(), 2);
        assert_eq!(with_guidance[0], "preserve atomicity");
        assert_eq!(with_guidance[1], WORKER_WRITABILITY_GUIDANCE);

        let mut maximum_with_guidance: Vec<String> = (0..MAX_LIST_ITEMS - 1)
            .map(|index| format!("constraint {index}"))
            .collect();
        maximum_with_guidance.push(WORKER_WRITABILITY_GUIDANCE.into());
        assert_eq!(
            with_worker_writability_guidance(&maximum_with_guidance),
            maximum_with_guidance
        );

        let already_present = vec![WORKER_WRITABILITY_GUIDANCE.into()];
        assert_eq!(
            with_worker_writability_guidance(&already_present),
            already_present
        );
    }

    #[test]
    fn retry_summary_truncation_preserves_utf8_at_the_byte_limit() {
        let summary = "😀".repeat(300);
        let truncated = truncate_utf8(&summary, MAX_REJECTION_SUMMARY_BYTES);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.len() <= MAX_REJECTION_SUMMARY_BYTES);
        assert_eq!(truncated, "😀".repeat(256));
    }
}
