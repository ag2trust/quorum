//! Bounded task-decomposition planner provider and protocol boundary.

// This foundation module is exercised directly by its contract tests. The daemon coordinator
// integration will consume the runtime API in the next implementation slice.
#![allow(dead_code)]

use super::agent::{self, AgentProc, AgentSpec};
use super::runner::{AgentEvent, AgentKind, RunnerProc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

pub const CODEX_PLANNER_MODEL: &str = "gpt-5.6-sol";
pub const CLAUDE_PLANNER_MODEL: &str = "claude-opus-4-6";
pub const PLANNER_EFFORT: &str = "high";
pub const PLANNER_TIMEOUT: Duration = Duration::from_secs(600);
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024;
pub const MAX_STDOUT_BYTES: usize = 128 * 1024;
pub const MAX_PROMPT_BYTES: usize = 128 * 1024;
const MAX_TEXT_BYTES: usize = 8 * 1024;
const MAX_LIST_ITEMS: usize = 32;
const MAX_REJECTION_SUMMARIES: usize = 3;
pub(super) const MAX_REJECTION_SUMMARY_BYTES: usize = 1024;

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
    pub acceptance_criteria: Vec<String>,
    pub source_constraints: Vec<String>,
    pub verification_expectations: Vec<String>,
    #[serde(default)]
    pub non_goals: Vec<String>,
    #[serde(default)]
    pub preserved_literals: Vec<String>,
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
         Preserve every source constraint. Copy exact required text, literals, commands, schemas, \
         labels, tags, messages, identifiers, and other verbatim source material byte-for-byte \
         without normalization or paraphrase. Put each such value in preserved_literals on at \
         least one child. Every source requirement must be represented by a child delta, criterion, \
         constraint, verification expectation, or non-goal. Do not create synthetic integration \
         work, recursive planning, no-op/regression-only tasks, or unrelated scope. Dependencies \
         must be real delivery prerequisites and may \
         reference another task key or source:<dependency-id>. Return exactly one valid JSON \
         object, with no markdown or commentary. The object must be exactly one of these closed \
         shapes: do not omit, rename, or add fields.\n\
         PLAN={{\"outcome\":\"plan\",\"tasks\":[{{\"key\":\"<lowercase-ascii-key>\",\"title\":\"<title>\",\"implementation_delta\":\"<new-code-or-documentation-change>\",\"affected_paths\":[\"<repo-relative-path-or-narrow-pattern>\"],\"observable_outcome\":\"<observable-outcome>\",\"acceptance_criteria\":[\"<criterion>\"],\"source_constraints\":[\"<constraint>\"],\"verification_expectations\":[\"<verification>\"],\"non_goals\":[\"<preserved-or-explicitly-excluded-behavior>\"],\"preserved_literals\":[\"<exact-source-literal>\"],\"prerequisites\":[\"<task-key-or-source:positive-id>\"]}}]}}\n\
         BLOCKER={{\"outcome\":\"blocker\",\"category\":\"<ambiguous_scope|missing_decision|external_constraint|no_safe_split>\",\"evidence\":[\"<evidence>\"],\"required_decision\":\"<decision>\",\"why_no_safe_split\":\"<reason>\"}}\n\
         `outcome` must be exactly `plan` or `blocker`; use PLAN only when it has 2-8 tasks. \
         PRIOR_REJECTIONS contains at most {MAX_REJECTION_SUMMARIES} summaries, each truncated \
         to {MAX_REJECTION_SUMMARY_BYTES} bytes. On retry, use only those summaries to correct \
         the cited semantic defect; do not discuss them or request more context.\n\nSOURCE={source_json}\n\nPRIOR_REJECTIONS={retry_json}"
    )
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
/// Shape/cycle/text validation has already run in `parse_response`.
pub fn validate_for_source(
    tasks: &[ProposedTask],
    source_dependency_ids: &[i64],
    source_title: &str,
    source_body: Option<&str>,
) -> Result<(), PlannerParseError> {
    let allowed: HashSet<i64> = source_dependency_ids.iter().copied().collect();
    for task in tasks {
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
    let source_text = format!("{source_title}\n{}", source_body.unwrap_or_default());
    let preserved: HashSet<&str> = tasks
        .iter()
        .flat_map(|task| task.preserved_literals.iter().map(String::as_str))
        .collect();
    for literal in &preserved {
        if !source_text.contains(literal) {
            return semantic("preserved literal must match source bytes exactly");
        }
    }
    for (index, literal) in required_source_literals(&source_text)
        .into_iter()
        .enumerate()
    {
        if !preserved.contains(literal.as_str()) {
            return semantic(&format!(
                "missing byte-exact source literal at source marker {}",
                index + 1
            ));
        }
    }
    Ok(())
}

/// Extract source-marked literals that can be checked without asking a model
/// to decide which spelling is authoritative. Markdown inline/fenced code is
/// explicit literal syntax. Quoted values immediately associated with the
/// words literal, label, tag, or message receive the same protection.
fn required_source_literals(source: &str) -> Vec<String> {
    let mut literals = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = source[cursor..].find('`') {
        let start = cursor + relative;
        let fence = if source[start..].starts_with("```") {
            3
        } else {
            1
        };
        let content_start = start + fence;
        let Some(end_relative) = source[content_start..].find(if fence == 3 { "```" } else { "`" })
        else {
            break;
        };
        let end = content_start + end_relative;
        if end > content_start {
            literals.push(source[content_start..end].to_string());
        }
        cursor = end + fence;
    }

    let lower = source.to_ascii_lowercase();
    for keyword in ["literal", "label", "tag", "message"] {
        let mut search_from = 0;
        while let Some(relative) = lower[search_from..].find(keyword) {
            let after_keyword = search_from + relative + keyword.len();
            let bytes = source.as_bytes();
            let mut open = after_keyword;
            if bytes.get(open) == Some(&b'"') {
                open += 1;
            }
            while bytes.get(open).is_some_and(u8::is_ascii_whitespace) {
                open += 1;
            }
            if matches!(bytes.get(open), Some(b':') | Some(b'=')) {
                open += 1;
                while bytes.get(open).is_some_and(u8::is_ascii_whitespace) {
                    open += 1;
                }
            }
            if bytes.get(open) != Some(&b'"') || open.saturating_sub(after_keyword) > 24 {
                search_from = after_keyword;
                continue;
            }
            let value_start = open + 1;
            let Some(close_relative) = source[value_start..].find('"') else {
                break;
            };
            let value_end = value_start + close_relative;
            // A quoted label/tag/message never spans lines; a newline before the
            // closing quote means the opening quote was unpaired. Skip it so a
            // stray quote cannot manufacture an unsatisfiable required literal.
            if source[value_start..value_end].contains('\n') {
                search_from = value_start;
                continue;
            }
            if value_end > value_start {
                literals.push(source[value_start..value_end].to_string());
            }
            search_from = value_end + 1;
        }
    }
    literals.sort();
    literals.dedup();
    literals
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

pub fn parse_response(text: &str) -> Result<PlannerResponse, PlannerParseError> {
    if text.len() > MAX_RESPONSE_BYTES {
        return Err(PlannerParseError::Provider(
            "response exceeds 64 KiB".into(),
        ));
    }
    let trimmed = text.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(PlannerParseError::Provider(
            "response must be exactly one JSON object without wrappers".into(),
        ));
    }
    let response: PlannerResponse = serde_json::from_str(trimmed)
        .map_err(|e| PlannerParseError::Provider(format!("invalid closed JSON: {e}")))?;
    validate_semantics(&response)?;
    Ok(response)
}

fn validate_semantics(response: &PlannerResponse) -> Result<(), PlannerParseError> {
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
        PlannerResponse::Plan { tasks } => {
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
                validate_list("acceptance criteria", &task.acceptance_criteria, 1)?;
                validate_list("source constraints", &task.source_constraints, 1)?;
                validate_list(
                    "verification expectations",
                    &task.verification_expectations,
                    1,
                )?;
                validate_list("non-goals", &task.non_goals, 1)?;
                validate_list("preserved literals", &task.preserved_literals, 0)?;
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
                    if !keys.contains(prerequisite.as_str())
                        && !valid_source_dependency(prerequisite)
                    {
                        return semantic("prerequisite must be a task key or source:<positive-id>");
                    }
                }
            }
            reject_cycles(tasks)?;
        }
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
    pub response_text: String,
    started_at: tokio::time::Instant,
    stdout_bytes: usize,
}

impl PlannerSlot {
    pub fn pid(&self) -> Option<i32> {
        self.proc.pid()
    }

    pub async fn kill_and_reap(self) {
        let _ = self.proc.kill_and_reap().await;
    }
}

pub enum PlannerPoll {
    Done(PlannerResponse),
    ProviderFailed(String),
    SemanticRejected(String),
}

/// Spawn only the provider selected by the durable role assignment. There is
/// no fallback or model substitution.
pub async fn spawn_planner(
    provider: AgentKind,
    model: &str,
    effort: &str,
    repo: &Path,
    prompt: &str,
    bare: bool,
    provider_bin: Option<&str>,
) -> std::io::Result<PlannerSlot> {
    spawn_planner_with_timeout(
        provider,
        model,
        effort,
        repo,
        prompt,
        bare,
        provider_bin,
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
    turn_timeout: Duration,
) -> std::io::Result<PlannerSlot> {
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "planner prompt exceeds 128 KiB",
        ));
    }
    let started_at = tokio::time::Instant::now();
    let deadline = started_at + turn_timeout;
    let proc = match provider {
        AgentKind::Codex => return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Codex decomposition planner refused: no portable launch boundary can isolate provider transport from model-generated network and filesystem access",
        )),
        AgentKind::Grok => return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Grok decomposition planner refused: managed Grok lifecycle roles are not enabled",
        )),
        AgentKind::Claude => {
            let spec = AgentSpec {
                kind: AgentKind::Claude,
                model: model.into(),
                effort: effort.into(),
                session_id: agent::new_session_id(),
                worktree: repo.to_path_buf(),
                bare,
                allowed_tools: "Read,Glob,Grep".into(),
                env_vars: vec![],
            };
            let mut proc = AgentProc::spawn_planner(&spec, provider_bin)?;
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
        response_text: String::new(),
        started_at,
        stdout_bytes: 0,
    })
}

/// Drain a bounded amount of output. Timeout and output violations are
/// provider failures; the caller must kill and reap the returned terminal slot.
pub async fn poll_planner(slot: &mut PlannerSlot) -> Option<PlannerPoll> {
    if slot.started_at.elapsed() >= PLANNER_TIMEOUT {
        return Some(PlannerPoll::ProviderFailed("planner timed out".into()));
    }
    let remaining = PLANNER_TIMEOUT.saturating_sub(slot.started_at.elapsed());
    let poll_for = remaining.min(Duration::from_secs(2));
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
            Ok(Ok(None)) => break,
            Ok(Err(_)) => {
                return Some(PlannerPoll::ProviderFailed(format!(
                    "planner stdout exceeded {} KiB",
                    MAX_STDOUT_BYTES / 1024
                )));
            }
        };
        slot.stdout_bytes = slot.stdout_bytes.saturating_add(raw.len() + 1);
        if slot.stdout_bytes > MAX_STDOUT_BYTES {
            return Some(PlannerPoll::ProviderFailed(format!(
                "planner stdout exceeded {} KiB",
                MAX_STDOUT_BYTES / 1024
            )));
        }
        if slot.proc.kind() == AgentKind::Claude {
            if let Some(super::stream::Event::Result {
                result, is_error, ..
            }) = super::stream::parse_line(&raw)
            {
                if is_error.unwrap_or(false) {
                    return Some(PlannerPoll::ProviderFailed(
                        "planner provider returned an error".into(),
                    ));
                }
                let text = super::stream::result_text(&result);
                if !text.is_empty() {
                    slot.response_text = text;
                }
                return Some(parsed_poll(&slot.response_text));
            }
        }
        for event in match slot.proc.kind() {
            AgentKind::Claude => super::runner::normalize_claude_line(&raw),
            AgentKind::Codex => super::runner::normalize_codex_line(&raw),
            AgentKind::Grok => super::runner::normalize_grok_line(&raw),
        } {
            match event {
                AgentEvent::TurnFailed { .. } => {
                    return Some(PlannerPoll::ProviderFailed(
                        "planner provider turn failed".into(),
                    ));
                }
                AgentEvent::TurnCompleted { .. } => {
                    return Some(parsed_poll(&slot.response_text));
                }
                AgentEvent::AssistantText { text } => {
                    if slot.response_text.len().saturating_add(text.len()) > MAX_RESPONSE_BYTES {
                        return Some(PlannerPoll::ProviderFailed(
                            "planner response exceeded 64 KiB".into(),
                        ));
                    }
                    slot.response_text.push_str(&text);
                }
                _ => {}
            }
        }
        if slot.started_at.elapsed() >= PLANNER_TIMEOUT {
            return Some(PlannerPoll::ProviderFailed("planner timed out".into()));
        }
    }
    if matches!(slot.proc.try_wait(), Ok(Some(_))) {
        return Some(PlannerPoll::ProviderFailed(
            "planner exited without a terminal response".into(),
        ));
    }
    None
}

fn parsed_poll(text: &str) -> PlannerPoll {
    match parse_response(text) {
        Ok(response) => PlannerPoll::Done(response),
        Err(PlannerParseError::Provider(error)) => PlannerPoll::ProviderFailed(error),
        Err(PlannerParseError::Semantic(error)) => PlannerPoll::SemanticRejected(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The no-read provider test fills a pipe with a near-limit prompt. Allow
    // normal CI scheduling delay while retaining a finite failure boundary.
    const TEST_STDIN_FEED_TIMEOUT: Duration = Duration::from_secs(15);
    const TEST_BOUNDARY_TIMEOUT: Duration = Duration::from_secs(15);

    fn task(key: &str, prerequisites: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "key": key,
            "title": format!("Implement {key}"),
            "implementation_delta": format!("change the {key} implementation seam"),
            "affected_paths": [format!("src/{key}.rs")],
            "observable_outcome": format!("{key} works"),
            "acceptance_criteria": ["behavior is covered"],
            "source_constraints": ["preserve atomicity"],
            "verification_expectations": ["focused tests pass"],
            "non_goals": ["do not change adjacent behavior"],
            "preserved_literals": [],
            "prerequisites": prerequisites,
        })
    }

    #[test]
    fn accepts_closed_plan_and_blocker() {
        let plan = serde_json::json!({"outcome":"plan","tasks":[task("core", &[]), task("daemon", &["core", "source:7"])]});
        assert!(matches!(
            parse_response(&plan.to_string()),
            Ok(PlannerResponse::Plan { .. })
        ));
        let blocker = serde_json::json!({
            "outcome":"blocker", "category":"missing_decision", "evidence":["two incompatible outcomes are requested"],
            "required_decision":"choose one outcome", "why_no_safe_split":"both children would mutate the same contract"
        });
        assert!(matches!(
            parse_response(&blocker.to_string()),
            Ok(PlannerResponse::Blocker { .. })
        ));
    }

    #[test]
    fn wrappers_unknown_fields_and_malformed_json_are_provider_failures() {
        for value in [
            "```json\n{}\n```".to_string(),
            r#"{"outcome":"plan","tasks":[],"extra":true}"#.into(),
            r#"{"outcome":"plan"} trailing"#.into(),
        ] {
            assert!(matches!(
                parse_response(&value),
                Err(PlannerParseError::Provider(_))
            ));
        }
    }

    #[test]
    fn invalid_blocker_and_invalid_graph_are_semantic_rejections() {
        let blocker = r#"{"outcome":"blocker","category":"magic","evidence":[],"required_decision":"x","why_no_safe_split":"y"}"#;
        assert!(matches!(
            parse_response(blocker),
            Err(PlannerParseError::Semantic(_))
        ));
        let cycle =
            serde_json::json!({"outcome":"plan","tasks":[task("a", &["b"]),task("b", &["a"])]});
        assert!(matches!(
            parse_response(&cycle.to_string()),
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
                parse_response(&plan.to_string()),
                Err(PlannerParseError::Semantic(_))
            ));
        }
    }

    #[test]
    fn polling_result_preserves_independent_failure_budgets() {
        assert!(matches!(
            parsed_poll("not json"),
            PlannerPoll::ProviderFailed(_)
        ));
        assert!(matches!(
            parsed_poll(r#"{"outcome":"plan","tasks":[]}"#),
            PlannerPoll::SemanticRejected(_)
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
            PlannerPoll::ProviderFailed(ref message) if message.contains("stdout exceeded")
        ));

        slot.kill_and_reap().await;
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "planner was not reaped");
    }

    #[tokio::test]
    async fn codex_planner_fails_closed_before_real_binary_launch() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("invoked");
        let fake = dir.path().join("codex");
        std::fs::write(&fake, format!("#!/bin/sh\ntouch '{}'\n", marker.display())).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let error = spawn_planner(
            AgentKind::Codex,
            CODEX_PLANNER_MODEL,
            PLANNER_EFFORT,
            Path::new("."),
            "bounded prompt",
            false,
            fake.to_str(),
        )
        .await
        .err()
        .expect("Codex planner must fail closed");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(
            !marker.exists(),
            "refused planner must not execute provider binary"
        );
        if let Ok(output) = std::process::Command::new("which").arg("codex").output() {
            if output.status.success() {
                let real = String::from_utf8_lossy(&output.stdout).trim().to_string();
                let real_error = spawn_planner(
                    AgentKind::Codex,
                    CODEX_PLANNER_MODEL,
                    PLANNER_EFFORT,
                    Path::new("."),
                    "attempt network, quorum, database, and coordination access",
                    false,
                    Some(real.as_str()),
                )
                .await
                .err()
                .expect("real Codex binary must also be refused before launch");
                assert_eq!(real_error.kind(), std::io::ErrorKind::PermissionDenied);
            }
        }
    }

    #[test]
    fn source_validation_rejects_foreign_dependencies_and_synthetic_integration() {
        let foreign = ProposedTask {
            key: "a".into(),
            title: "Implement a".into(),
            observable_outcome: "a works".into(),
            acceptance_criteria: vec!["covered".into()],
            source_constraints: vec!["atomic".into()],
            verification_expectations: vec!["tests".into()],
            prerequisites: vec!["source:9".into()],
            ..Default::default()
        };
        assert!(matches!(
            validate_for_source(&[foreign], &[7], "source", None),
            Err(PlannerParseError::Semantic(_))
        ));
        let synthetic = ProposedTask {
            key: "integration".into(),
            title: "Integration task".into(),
            observable_outcome: "merge all siblings".into(),
            acceptance_criteria: vec!["covered".into()],
            source_constraints: vec!["atomic".into()],
            verification_expectations: vec!["tests".into()],
            prerequisites: vec![],
            ..Default::default()
        };
        assert!(matches!(
            validate_for_source(&[synthetic], &[], "source", None),
            Err(PlannerParseError::Semantic(_))
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
        assert!(prompt.contains(r#""non_goals":["<preserved-or-explicitly-excluded-behavior>"]"#));
        assert!(prompt.contains(r#""preserved_literals":["<exact-source-literal>"]"#));
        assert!(prompt.contains(
            r#"BLOCKER={"outcome":"blocker","category":"<ambiguous_scope|missing_decision|external_constraint|no_safe_split>","evidence":["<evidence>"],"required_decision":"<decision>","why_no_safe_split":"<reason>"}"#
        ));
        assert!(prompt.contains("`outcome` must be exactly `plan` or `blocker`"));
        assert!(prompt.contains("S = focused/local; M = bounded coherent work"));
        assert!(prompt.contains("Use at most 5 Grep/Glob calls and 10 Read calls"));
        assert!(prompt.contains("labels, tags, messages, identifiers"));
    }

    #[test]
    fn source_validation_requires_marked_literals_byte_for_byte() {
        let title = "Keep label \"review-ready\"";
        let source = "Keep `type:feature`, tag \"security\", literal \"EXACT\", and message \"Merge ready\" exactly.";
        let mut proposed = ProposedTask {
            key: "routing".into(),
            title: "Change routing".into(),
            implementation_delta: "change one routing seam".into(),
            affected_paths: vec!["src/routing.rs".into()],
            observable_outcome: "routing works".into(),
            acceptance_criteria: vec!["covered".into()],
            source_constraints: vec!["preserve literals".into()],
            verification_expectations: vec!["tests pass".into()],
            non_goals: vec!["no unrelated changes".into()],
            preserved_literals: vec![
                "type:feature".into(),
                "security".into(),
                "EXACT".into(),
                "Merge ready".into(),
                "review-ready".into(),
            ],
            prerequisites: vec![],
        };
        assert!(validate_for_source(&[proposed.clone()], &[], title, Some(source)).is_ok());
        proposed.preserved_literals[3] = "merge ready".into();
        assert!(matches!(
            validate_for_source(&[proposed], &[], title, Some(source)),
            Err(PlannerParseError::Semantic(message))
                if message.contains("match source bytes exactly")
        ));
    }

    #[test]
    fn unpaired_quote_does_not_manufacture_required_literal() {
        let source = "A stray label \"unclosed\ntag \"three\" stays protected.";
        let literals = required_source_literals(source);
        assert_eq!(literals, vec!["three".to_string()]);
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
    fn retry_summary_truncation_preserves_utf8_at_the_byte_limit() {
        let summary = "😀".repeat(300);
        let truncated = truncate_utf8(&summary, MAX_REJECTION_SUMMARY_BYTES);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.len() <= MAX_REJECTION_SUMMARY_BYTES);
        assert_eq!(truncated, "😀".repeat(256));
    }
}
