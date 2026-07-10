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

/// Spawn a headless classifier agent for a batch of tasks.
pub fn spawn_classifier(
    tasks: &[TaskForClassification],
    _dup_context: &[TaskForClassification],
    repo_dir: &Path,
    agent_bin: Option<&str>,
) -> std::io::Result<ClassifierSlot> {
    let pending_task_ids: Vec<i64> = tasks.iter().map(|t| t.id).collect();

    let session_id = format!("classifier-{}", pending_task_ids.first().unwrap_or(&0));

    let spec = AgentSpec {
        model: CLASSIFIER_MODEL.to_string(),
        effort: CLASSIFIER_EFFORT.to_string(),
        session_id,
        worktree: repo_dir.to_path_buf(),
        bare: true,
        allowed_tools: String::new(),
        env_vars: vec![],
    };

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
                let text = result
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| result.to_string());
                if !text.is_empty() {
                    slot.response_text = text;
                }
                return Some(ClassifierResult::Done(slot.response_text.clone()));
            }
            stream::Event::Assistant { message } => {
                if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                    slot.response_text.push_str(content);
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
