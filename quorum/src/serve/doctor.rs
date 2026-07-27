//! Doctor agent — config-gated troubleshooter for non-deterministic stalls.
//!
//! When `doctor_enabled = true`, the daemon spawns a one-shot doctor agent for
//! tasks that are stalled with no active worker or reviewer and no quorum-side
//! error. The doctor investigates via quorum CLI commands and posts findings
//! to the mailbox + a report file. Default OFF — the deterministic ladder
//! (stir → reap → terminal+notify) is the complete behavior when disabled.

use super::agent::{self, AgentProc, AgentSpec};
use super::stream;
use std::path::{Path, PathBuf};

pub const DOCTOR_MODEL: &str = "claude-sonnet-4-20250514";
pub const DOCTOR_EFFORT: &str = "medium";

/// In-flight doctor state, persisted across ticks.
pub struct DoctorSlot {
    pub proc: AgentProc,
    pub task_id: i64,
    pub response_text: String,
}

/// Evidence bundle passed to the doctor's prompt.
pub struct EvidenceBundle {
    pub task_id: i64,
    pub task_title: String,
    pub task_status: String,
    pub task_body: Option<String>,
    pub author: Option<String>,
    pub pr: Option<i64>,
    pub worktree_path: Option<String>,
    pub repo: String,
}

/// Build the doctor's user turn from an evidence bundle.
pub fn doctor_turn(evidence: &EvidenceBundle) -> String {
    let mut prompt = String::with_capacity(2048);

    prompt.push_str("You are a doctor agent — a one-shot troubleshooter for a stalled task.\n\n");
    prompt.push_str("## Situation\n\n");
    prompt.push_str(&format!(
        "Task #{} is stalled in status `{}` with no active worker or reviewer.\n",
        evidence.task_id, evidence.task_status
    ));
    prompt.push_str(&format!("Title: {}\n", evidence.task_title));
    if let Some(body) = &evidence.task_body {
        let end = body
            .char_indices()
            .map(|(i, _)| i)
            .nth(500)
            .unwrap_or(body.len());
        let truncated = &body[..end];
        prompt.push_str(&format!("Body (truncated): {truncated}\n"));
    }
    if let Some(author) = &evidence.author {
        prompt.push_str(&format!("Author: {author}\n"));
    }
    if let Some(pr) = evidence.pr {
        prompt.push_str(&format!("PR: #{pr}\n"));
    }
    if let Some(wt) = &evidence.worktree_path {
        prompt.push_str(&format!("Worktree: {wt}\n"));
    }
    prompt.push_str(&format!("Repo: {}\n", evidence.repo));

    prompt.push_str("\n## Your job\n\n");
    prompt.push_str(&format!(
        "1. Run `quorum task-get --task-id {} --json` to get current task state.\n",
        evidence.task_id
    ));
    prompt.push_str("2. Run `quorum status --json` to see daemon/agent state.\n");
    prompt.push_str("3. Check if there's a worktree or branch still present.\n");
    prompt.push_str("4. Look at recent quorum events for this task.\n");
    prompt.push_str("5. Diagnose why the task is stuck (common causes: worktree gone, ");
    prompt.push_str("branch deleted, PR closed externally, agent crashed without cleanup).\n\n");

    prompt.push_str("## Output contract\n\n");
    prompt.push_str("- Post your findings via: `quorum post --agent Doctor --body-file <path>`\n");
    prompt.push_str("- If you can fix the stall, do so via quorum commands ");
    prompt.push_str("(task-update, react, post). Do NOT edit code.\n");
    prompt.push_str("- Write a report to: ~/.quorum/reports/ as ");
    prompt.push_str(&format!(
        "`$(date +%Y-%m-%d)-task{}.md`\n",
        evidence.task_id
    ));
    prompt.push_str("- This is a single turn — do your investigation and report, then stop.\n");

    agent::user_turn(&prompt)
}

/// Spawn a doctor agent. Runs in repo_dir (not a worktree — read-only investigation).
pub fn spawn_doctor(
    task_id: i64,
    repo_dir: &Path,
    agent_bin: Option<&str>,
    bare: bool,
    allowed_tools: &str,
    repo: &str,
) -> std::io::Result<DoctorSlot> {
    let spec = AgentSpec {
        kind: super::runner::AgentKind::Claude,
        model: DOCTOR_MODEL.to_string(),
        effort: DOCTOR_EFFORT.to_string(),
        session_id: agent::new_session_id(),
        worktree: repo_dir.to_path_buf(),
        bare,
        allowed_tools: allowed_tools.to_string(),
        env_vars: vec![
            ("QUORUM_REPO".into(), repo.into()),
            ("QUORUM_AGENT".into(), "Doctor".into()),
        ],
        stderr_log: None,
    };

    let proc = AgentProc::spawn(&spec, agent_bin)?;

    Ok(DoctorSlot {
        proc,
        task_id,
        response_text: String::new(),
    })
}

/// Drain events from the doctor (non-blocking, bounded). Returns Some when done.
pub async fn drain_doctor_events(slot: &mut DoctorSlot) -> Option<DoctorResult> {
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_secs(2), slot.proc.next_event()).await
    {
        match &event {
            stream::Event::Result {
                result, is_error, ..
            } => {
                if is_error.unwrap_or(false) {
                    return Some(DoctorResult::Error("doctor agent returned an error".into()));
                }
                let text = result
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| result.to_string());
                if !text.is_empty() {
                    slot.response_text = text;
                }
                return Some(DoctorResult::Done(slot.response_text.clone()));
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

pub enum DoctorResult {
    Done(String),
    Error(String),
}

/// Reports dir under quorum home (~/.quorum/reports/).
/// Called by the doctor agent via Bash, not directly from daemon code.
#[allow(dead_code)]
pub fn ensure_reports_dir() -> std::io::Result<PathBuf> {
    let base = crate::paths::home_dir().map_err(|e| std::io::Error::other(e.to_string()))?;
    let dir = base.join("reports");
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doctor_turn_contains_task_id() {
        let evidence = EvidenceBundle {
            task_id: 42,
            task_title: "fix the widget".into(),
            task_status: "working".into(),
            task_body: Some("needs investigation".into()),
            author: Some("Bolt-1".into()),
            pr: Some(99),
            worktree_path: Some("/tmp/wt".into()),
            repo: "test/repo".into(),
        };
        let turn = doctor_turn(&evidence);
        assert!(turn.contains("42"), "turn should reference task id");
        assert!(turn.contains("fix the widget"), "turn should include title");
        assert!(turn.contains("working"), "turn should include status");
    }

    #[test]
    fn doctor_turn_handles_missing_optionals() {
        let evidence = EvidenceBundle {
            task_id: 1,
            task_title: "test".into(),
            task_status: "in-review".into(),
            task_body: None,
            author: None,
            pr: None,
            worktree_path: None,
            repo: "test/repo".into(),
        };
        let turn = doctor_turn(&evidence);
        assert!(turn.contains("Task #1"), "should include task id");
        assert!(!turn.contains("Author:"), "no author when None");
    }

    #[test]
    fn ensure_reports_dir_creates_path() {
        let dir = ensure_reports_dir();
        assert!(dir.is_ok());
        assert!(dir.unwrap().ends_with("reports"));
    }
}
