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
pub const MAX_STDOUT_BYTES: usize = 256 * 1024;
pub const MAX_PROMPT_BYTES: usize = 128 * 1024;
const MAX_TEXT_BYTES: usize = 8 * 1024;
const MAX_LIST_ITEMS: usize = 32;

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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedTask {
    pub key: String,
    pub title: String,
    pub observable_outcome: String,
    pub acceptance_criteria: Vec<String>,
    pub source_constraints: Vec<String>,
    pub verification_expectations: Vec<String>,
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
            .take(3)
            .map(|summary| summary.chars().take(1024).collect::<String>())
            .collect::<Vec<_>>(),
    )
    .expect("rejection summaries serialize");
    format!(
        "You are Quorum's bounded decomposition planner. Split the source outcome into one \
         closed DAG of 2-8 independently deliverable implementation tasks, each size S or M. \
         Preserve every source constraint. Do not create synthetic integration work, recursive \
         planning, or unrelated scope. Dependencies must be real delivery prerequisites and may \
         reference another task key or source:<dependency-id>. Return exactly one closed JSON \
         object matching the PLAN/BLOCKER protocol; no markdown or commentary.\n\nSOURCE={source_json}\n\nPRIOR_REJECTIONS={retry_json}"
    )
}

/// Recheck proposal dependencies against the authoritative source snapshot.
/// Shape/cycle/text validation has already run in `parse_response`.
pub fn validate_for_source(
    tasks: &[ProposedTask],
    source_dependency_ids: &[i64],
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
    Ok(())
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
                validate_text("observable outcome", &task.observable_outcome)?;
                validate_list("acceptance criteria", &task.acceptance_criteria, 1)?;
                validate_list("source constraints", &task.source_constraints, 1)?;
                validate_list(
                    "verification expectations",
                    &task.verification_expectations,
                    1,
                )?;
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
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "planner prompt exceeds 128 KiB",
        ));
    }
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
            if let Err(error) = proc.feed_turn(&agent::user_turn(prompt)).await {
                let _ = proc.kill_and_reap().await;
                return Err(error);
            }
            RunnerProc::Claude(proc)
        }
    };
    Ok(PlannerSlot {
        proc,
        response_text: String::new(),
        started_at: tokio::time::Instant::now(),
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
                return Some(PlannerPoll::ProviderFailed(
                    "planner stdout exceeded 256 KiB".into(),
                ));
            }
        };
        slot.stdout_bytes = slot.stdout_bytes.saturating_add(raw.len() + 1);
        if slot.stdout_bytes > MAX_STDOUT_BYTES {
            return Some(PlannerPoll::ProviderFailed(
                "planner stdout exceeded 256 KiB".into(),
            ));
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

    fn task(key: &str, prerequisites: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "key": key,
            "title": format!("Implement {key}"),
            "observable_outcome": format!("{key} works"),
            "acceptance_criteria": ["behavior is covered"],
            "source_constraints": ["preserve atomicity"],
            "verification_expectations": ["focused tests pass"],
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
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
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
        };
        assert!(matches!(
            validate_for_source(&[foreign], &[7]),
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
        };
        assert!(matches!(
            validate_for_source(&[synthetic], &[]),
            Err(PlannerParseError::Semantic(_))
        ));
    }

    #[test]
    fn planner_prompt_is_bounded_and_contains_only_structured_retry_summaries() {
        let dependencies = vec![3, 4];
        let source = PlanningSource {
            task_id: 7,
            revision: 2,
            title: "large outcome",
            body: Some("preserve atomicity"),
            dependencies: &dependencies,
        };
        let prompt = build_prompt(&source, &["cycle detected".into(), "x".repeat(5000)]);
        assert!(prompt.len() < MAX_PROMPT_BYTES);
        assert!(prompt.contains("cycle detected"));
        assert!(!prompt.contains(&"x".repeat(1025)));
        assert!(!prompt.contains("transcript"));
    }
}
