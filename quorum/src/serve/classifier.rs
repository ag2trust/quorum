//! Daemon classifier phase — spawns a headless agent to batch-classify tasks.

use super::agent::{AgentProc, AgentSpec};
use super::stream;
use quorum_core::classify::{self, ClassifierResponse, TaskClassification, TaskForClassification};
use std::path::Path;

pub const CLASSIFIER_MODEL: &str = "claude-haiku-4-5-20251001";
pub const CLASSIFIER_EFFORT: &str = "low";

/// In-flight classifier state, persisted across daemon ticks.
#[allow(dead_code)]
pub struct ClassifierSlot {
    pub proc: AgentProc,
    pub pending_task_ids: Vec<i64>,
    pub response_text: String,
}

/// Build the spec for a classifier agent. `bare` must follow the daemon's
/// `no_bare_agent` setting (same as worker/reviewer spawns): on machines
/// using subscription auth a `--bare` agent has no credentials, so every
/// classifier turn fails "Not logged in · Please run /login" and the daemon
/// respawn-loops (observed live 2026-07-10, right after the session-id fix).
pub fn classifier_spec(repo_dir: &Path, bare: bool) -> AgentSpec {
    AgentSpec {
        model: CLASSIFIER_MODEL.to_string(),
        effort: CLASSIFIER_EFFORT.to_string(),
        session_id: super::agent::new_session_id(),
        worktree: repo_dir.to_path_buf(),
        bare,
        allowed_tools: String::new(),
        env_vars: vec![],
    }
}

/// Spawn a headless classifier agent for a batch of tasks.
pub fn spawn_classifier(
    tasks: &[TaskForClassification],
    _dup_context: &[TaskForClassification],
    repo_dir: &Path,
    agent_bin: Option<&str>,
    bare: bool,
) -> std::io::Result<ClassifierSlot> {
    let pending_task_ids: Vec<i64> = tasks.iter().map(|t| t.id).collect();

    let spec = classifier_spec(repo_dir, bare);

    let proc = AgentProc::spawn(&spec, agent_bin)?;

    Ok(ClassifierSlot {
        proc,
        pending_task_ids,
        response_text: String::new(),
    })
}

/// Build the user turn for the classifier prompt.
pub fn classifier_turn(
    tasks: &[TaskForClassification],
    dup_context: &[TaskForClassification],
) -> String {
    let prompt = classify::build_prompt(tasks, dup_context);
    super::agent::user_turn(&prompt)
}

/// Drain events from the classifier agent (non-blocking, bounded).
/// Returns `Some(response_text)` when the agent produces a Result event.
pub async fn drain_classifier_events(slot: &mut ClassifierSlot) -> Option<ClassifierResult> {
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), slot.proc.next_event()).await
    {
        match &event {
            stream::Event::Result {
                result, is_error, ..
            } => {
                if is_error.unwrap_or(false) {
                    return Some(ClassifierResult::Error(
                        "classifier agent returned an error".into(),
                    ));
                }
                let text = stream::result_text(result);
                if !text.is_empty() {
                    slot.response_text = text;
                }
                return Some(ClassifierResult::Done(slot.response_text.clone()));
            }
            stream::Event::Assistant { message } => {
                // #127: real stream-json content is a typed block array — the
                // prior `.as_str()`-only branch dropped every non-string
                // content shape, leaving the classifier response empty and
                // the daemon respawn-looping. Shared helper handles both.
                if let Some(text) = stream::assistant_text(message) {
                    slot.response_text.push_str(&text);
                }
            }
            _ => {}
        }
    }
    None
}

pub enum ClassifierResult {
    Done(String),
    Error(String),
}

/// Parse the classifier response text into structured results.
pub fn parse_response(text: &str) -> Option<Vec<TaskClassification>> {
    // Try to find JSON in the response (model might wrap in markdown fences)
    let json_text = extract_json(text)?;
    let resp: ClassifierResponse = serde_json::from_str(json_text).ok()?;
    Some(resp.tasks)
}

fn extract_json(text: &str) -> Option<&str> {
    let trimmed = text.trim();

    // Direct JSON object
    if trimmed.starts_with('{') {
        return Some(trimmed);
    }

    // Strip markdown code fences
    if let Some(start) = trimmed.find("```json") {
        let after = &trimmed[start + 7..];
        if let Some(end) = after.find("```") {
            return Some(after[..end].trim());
        }
    }
    if let Some(start) = trimmed.find("```") {
        let after = &trimmed[start + 3..];
        if let Some(end) = after.find("```") {
            let inner = after[..end].trim();
            if inner.starts_with('{') {
                return Some(inner);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifier_spec_threads_bare_flag() {
        assert!(!classifier_spec(Path::new("."), false).bare);
        assert!(classifier_spec(Path::new("."), true).bare);
    }

    #[test]
    fn extract_json_direct() {
        let text = r#"{"tasks": []}"#;
        assert_eq!(extract_json(text), Some(r#"{"tasks": []}"#));
    }

    #[test]
    fn extract_json_with_fences() {
        let text = "```json\n{\"tasks\": []}\n```";
        assert_eq!(extract_json(text), Some("{\"tasks\": []}"));
    }

    #[test]
    fn extract_json_with_whitespace() {
        let text = "  \n  {\"tasks\": []}  \n  ";
        assert_eq!(extract_json(text), Some("{\"tasks\": []}"));
    }

    #[test]
    fn parse_response_valid() {
        let text = r#"{"tasks": [{"task_id": 1, "cx_est": 3, "cx_flags": [], "cx_tags": [], "cx_dup_of": []}]}"#;
        let results = parse_response(text).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].task_id, 1);
        assert_eq!(results[0].cx_est, 3);
    }

    #[test]
    fn parse_response_invalid() {
        assert!(parse_response("not json at all").is_none());
    }
}
