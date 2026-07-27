//! Daemon classifier phase — spawns a headless agent to batch-classify tasks.

use super::agent::{AgentProc, AgentSpec};
use super::codex_agent::{CodexProc, CodexSpec};
use super::runner::{AgentEvent, AgentKind, RunnerProc};
use quorum_core::classify::{self, ClassifierResponse, TaskClassification, TaskForClassification};
use std::path::Path;

pub const CLASSIFIER_MODEL: &str = "claude-haiku-4-5-20251001";
pub const CLASSIFIER_EFFORT: &str = "low";

pub fn classifier_kind(model: &str) -> std::io::Result<AgentKind> {
    AgentKind::for_model(model)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
}

/// In-flight classifier state, persisted across daemon ticks.
#[allow(dead_code)]
pub struct ClassifierSlot {
    pub proc: RunnerProc,
    pub pending_task_ids: Vec<i64>,
    pub response_text: String,
}

/// Build the spec for a classifier agent. `bare` must follow the daemon's
/// `no_bare_agent` setting (same as worker/reviewer spawns): on machines
/// using subscription auth a `--bare` agent has no credentials, so every
/// classifier turn fails "Not logged in · Please run /login" and the daemon
/// respawn-loops (observed live 2026-07-10, right after the session-id fix).
pub fn classifier_spec(repo_dir: &Path, bare: bool) -> AgentSpec {
    classifier_spec_for(repo_dir, bare, CLASSIFIER_MODEL, CLASSIFIER_EFFORT)
}

pub fn classifier_spec_for(repo_dir: &Path, bare: bool, model: &str, effort: &str) -> AgentSpec {
    AgentSpec {
        kind: AgentKind::Claude,
        model: model.to_string(),
        effort: effort.to_string(),
        session_id: super::agent::new_session_id(),
        worktree: repo_dir.to_path_buf(),
        bare,
        allowed_tools: String::new(),
        env_vars: vec![],
    }
}

/// Spawn using the provider resolved from `model`. Provider resolution is
/// authoritative: an unknown model is rejected and a failed spawn is returned
/// directly; neither condition falls back to the other runner.
#[allow(clippy::too_many_arguments)]
pub fn spawn_classifier_configured(
    tasks: &[TaskForClassification],
    dup_context: &[TaskForClassification],
    repo_dir: &Path,
    agent_bin: Option<&str>,
    bare: bool,
    model: &str,
    effort: &str,
    codex_sandbox: &str,
    recommendations: &str,
) -> std::io::Result<ClassifierSlot> {
    let kind = classifier_kind(model)?;
    let pending_task_ids = tasks.iter().map(|t| t.id).collect();
    let proc = match kind {
        AgentKind::Claude => {
            let spec = classifier_spec_for(repo_dir, bare, model, effort);
            AgentProc::spawn(&spec, agent_bin).map(RunnerProc::Claude)?
        }
        AgentKind::Codex => {
            let spec = CodexSpec {
                model: model.to_string(),
                effort: effort.to_string(),
                sandbox: codex_sandbox.to_string(),
                worktree: repo_dir.to_path_buf(),
                prompt: classify::build_prompt_with_recommendations(
                    tasks,
                    dup_context,
                    recommendations,
                ),
                env_vars: vec![],
            };
            CodexProc::spawn(&spec, agent_bin).map(RunnerProc::Codex)?
        }
    };
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

/// Build a classifier turn using the active provider's routing policy.
pub fn classifier_turn_with_recommendations(
    tasks: &[TaskForClassification],
    dup_context: &[TaskForClassification],
    recommendations: &str,
) -> String {
    let prompt = classify::build_prompt_with_recommendations(tasks, dup_context, recommendations);
    super::agent::user_turn(&prompt)
}

/// Drain events from the classifier agent (non-blocking, bounded).
/// Returns `Some(response_text)` when the agent produces a Result event.
pub async fn drain_classifier_events(slot: &mut ClassifierSlot) -> Option<ClassifierResult> {
    while let Ok(Some(raw)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), slot.proc.next_raw_line()).await
    {
        if slot.proc.kind() == AgentKind::Claude {
            if let Some(super::stream::Event::Result {
                result, is_error, ..
            }) = super::stream::parse_line(&raw)
            {
                let text = super::stream::result_text(&result);
                if is_error.unwrap_or(false) {
                    let detail = if text.is_empty() {
                        "classifier agent returned an error".into()
                    } else {
                        format!("classifier agent error: {}", truncate_error(&text, 300))
                    };
                    return Some(ClassifierResult::Error(detail));
                }
                if !text.is_empty() {
                    slot.response_text = text;
                }
                return Some(ClassifierResult::Done(slot.response_text.clone()));
            }
        }
        let events = match slot.proc.kind() {
            AgentKind::Claude => super::runner::normalize_claude_line(&raw),
            AgentKind::Codex => super::runner::normalize_codex_line(&raw),
        };
        for event in events {
            match event {
                AgentEvent::TurnFailed { message, .. } => {
                    let detail = if message.is_empty() {
                        "classifier agent returned an error".into()
                    } else {
                        format!("classifier agent error: {}", truncate_error(&message, 300))
                    };
                    return Some(ClassifierResult::Error(detail));
                }
                AgentEvent::TurnCompleted { .. } => {
                    return Some(ClassifierResult::Done(slot.response_text.clone()));
                }
                AgentEvent::AssistantText { text } => {
                    slot.response_text.push_str(&text);
                }
                _ => {}
            }
        }
    }
    None
}

pub enum ClassifierResult {
    Done(String),
    Error(String),
}

fn truncate_error(s: &str, max_chars: usize) -> String {
    let mut chars = s.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

/// True when the error text looks like a Claude CLI authentication failure.
pub fn is_auth_error(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("not logged in") || lower.contains("please run /login")
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
    fn configured_spec_preserves_model_and_effort() {
        let spec = classifier_spec_for(Path::new("."), false, "claude-test", "medium");
        assert_eq!(spec.kind, AgentKind::Claude);
        assert_eq!(spec.model, "claude-test");
        assert_eq!(spec.effort, "medium");
    }

    #[test]
    fn configured_provider_is_resolved_from_model_without_fallback() {
        assert_eq!(classifier_kind("gpt-5.6-terra").unwrap(), AgentKind::Codex);
        assert_eq!(
            classifier_kind("claude-haiku-4-5-20251001").unwrap(),
            AgentKind::Claude
        );
        assert_eq!(
            classifier_kind("unknown").unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn configured_codex_classifier_invokes_exact_model_and_effort() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let args_log = temp.path().join("args.log");
        let runner = temp.path().join("codex");
        std::fs::write(
            &runner,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" > '{}'\n\
                 printf '%s\\n' '{{\"type\":\"thread.started\",\"thread_id\":\"classifier-thread\"}}'\n\
                 printf '%s\\n' '{{\"type\":\"turn.completed\",\"usage\":{{\"input_tokens\":1,\"output_tokens\":1}}}}'\n",
                args_log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();

        let tasks = vec![TaskForClassification {
            id: 7,
            title: "classify me".into(),
            body: None,
        }];
        let mut slot = spawn_classifier_configured(
            &tasks,
            &[],
            temp.path(),
            runner.to_str(),
            false,
            "gpt-5.6-terra",
            "medium",
            "danger-full-access",
            "   1 → gpt-5.6-luna / medium",
        )
        .unwrap();
        while slot.proc.next_raw_line().await.is_some() {}
        slot.proc.kill_and_reap().await;

        let args = std::fs::read_to_string(args_log).unwrap();
        assert!(args.contains("exec --json"), "{args}");
        assert!(args.contains("--model gpt-5.6-terra"), "{args}");
        assert!(args.contains("-c model_reasoning_effort=medium"), "{args}");
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

    #[test]
    fn is_auth_error_detects_login_message() {
        assert!(is_auth_error("Not logged in · Please run /login"));
        assert!(is_auth_error("error: not logged in"));
        assert!(!is_auth_error("rate limit exceeded"));
    }

    #[test]
    fn truncate_error_respects_limit() {
        assert_eq!(truncate_error("short", 10), "short");
        let long = "a".repeat(400);
        let t = truncate_error(&long, 300);
        assert!(t.ends_with('…'));
        assert_eq!(t.chars().count(), 301); // 300 + '…'
    }

    #[test]
    fn truncate_error_safe_on_multibyte() {
        // "·" is 2 bytes in UTF-8; must not panic on boundary
        let s = "Not logged in · Please run /login".repeat(20);
        let t = truncate_error(&s, 30);
        assert!(t.ends_with('…'));
        assert_eq!(t.chars().count(), 31);
    }
}
