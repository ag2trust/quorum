//! Reviewer spawn + verdict handling.
//!
//! An ephemeral reviewer agent is spawned in a throwaway worktree at the PR's
//! head. It runs review and signals back via `quorum done --verdict
//! approved|changes`. The reviewer does NOT merge — merge is the daemon's job
//! (via MergeExecutor). The daemon consumes the verdict mailbox row and either
//! merges + tears down both agents (approved) or feeds a rework turn to the
//! warm worker (changes).

use super::agent::{AgentProc, AgentSpec};
use std::path::{Path, PathBuf};

pub struct ReviewerSpec {
    pub pr: i64,
    pub worker_agent: String,
    pub reviewer_name: String,
}

pub fn build_review_prompt(spec: &ReviewerSpec) -> String {
    format!(
        "You are reviewer agent {}. Review PR #{} opened by worker {}.\n\n\
         Run the project's code review process on this PR. When done:\n\
         - If approved: run: quorum done --agent {} --pr {} --verdict approved\n\
         - If changes needed: run: quorum done --agent {} --pr {} --verdict changes --feedback \"<your feedback>\"\n\n\
         Do NOT merge the PR yourself — the daemon handles merging.\n\
         Do NOT mark the task done yourself — the daemon handles task lifecycle.",
        spec.reviewer_name,
        spec.pr,
        spec.worker_agent,
        spec.reviewer_name,
        spec.pr,
        spec.reviewer_name,
        spec.pr,
    )
}

pub fn reviewer_worktree_path(base: &Path, pr: i64, reviewer_name: &str) -> PathBuf {
    base.join(format!("pr-{}-{}", pr, reviewer_name))
}

pub fn reviewer_branch(pr: i64, reviewer_name: &str) -> String {
    format!("review/pr-{}-{}", pr, reviewer_name.to_lowercase())
}

pub async fn spawn_reviewer(
    model: &str,
    effort: &str,
    session_id: &str,
    worktree_path: &Path,
    agent_bin: Option<&str>,
    bare: bool,
) -> std::io::Result<AgentProc> {
    let agent_spec = AgentSpec {
        model: model.to_string(),
        effort: effort.to_string(),
        session_id: session_id.to_string(),
        worktree: worktree_path.to_path_buf(),
        bare,
        resume: false,
    };
    AgentProc::spawn(&agent_spec, agent_bin)
}

pub fn build_worker_turn(agent_name: &str, task_id: i64, title: &str, body: &str) -> String {
    let turn = serde_json::json!({
        "type": "user",
        "message": {
            "content": format!(
                "You are agent {agent}. Task #{task_id}: {title}\n\n\
                 {body}\n\n\
                 When your work is complete:\n\
                 1. Push your branch and open a PR with: gh pr create\n\
                 2. Signal completion with the PR number: quorum done --agent {agent} --pr <PR_NUMBER>\n\
                 3. Post progress notes by writing text to a temp file, then: quorum task-update --task-id {task_id} --agent {agent} --note-file <path>\n\n\
                 Do NOT mark the task done yourself — the daemon handles task lifecycle.",
                agent = agent_name,
                task_id = task_id,
                title = title,
                body = body,
            )
        }
    });
    turn.to_string()
}

pub fn build_rework_turn(feedback: &str) -> String {
    let turn = serde_json::json!({
        "type": "user",
        "message": {
            "content": format!(
                "REVIEW FAILED — the reviewer requested changes. Fix the following feedback and push again:\n\n{}",
                feedback
            )
        }
    });
    turn.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn review_prompt_contains_agent_names_and_pr() {
        let spec = ReviewerSpec {
            pr: 42,
            worker_agent: "Worker-1".into(),
            reviewer_name: "Reviewer-1".into(),
        };
        let prompt = build_review_prompt(&spec);
        assert!(prompt.contains("PR #42"));
        assert!(prompt.contains("Worker-1"));
        assert!(prompt.contains("Reviewer-1"));
        assert!(prompt.contains("--verdict approved"));
        assert!(prompt.contains("--verdict changes"));
        assert!(
            !prompt.contains("merge the PR, then"),
            "reviewer prompt must NOT instruct the reviewer to merge"
        );
        assert!(prompt.contains("Do NOT merge the PR yourself"));
    }

    #[test]
    fn reviewer_worktree_path_format() {
        let base = PathBuf::from("/tmp/wt");
        let path = reviewer_worktree_path(&base, 55, "Rev-1");
        assert_eq!(path, PathBuf::from("/tmp/wt/pr-55-Rev-1"));
    }

    #[test]
    fn reviewer_branch_format() {
        let branch = reviewer_branch(55, "Rev-1");
        assert_eq!(branch, "review/pr-55-rev-1");
    }

    #[test]
    fn rework_turn_contains_feedback_and_review_failed() {
        let turn = build_rework_turn("Fix error handling in main.rs");
        assert!(turn.contains("REVIEW FAILED"));
        assert!(turn.contains("Fix error handling in main.rs"));
        let parsed: serde_json::Value = serde_json::from_str(&turn).unwrap();
        assert_eq!(parsed["type"], "user");
    }

    #[test]
    fn worker_turn_contains_agent_and_task() {
        let turn = build_worker_turn("Agent-1", 99, "Fix the bug", "Detailed body text");
        assert!(turn.contains("Agent-1"));
        assert!(turn.contains("Task #99"));
        assert!(turn.contains("Fix the bug"));
        assert!(turn.contains("Detailed body text"));
        let parsed: serde_json::Value = serde_json::from_str(&turn).unwrap();
        assert_eq!(parsed["type"], "user");
    }

    #[test]
    fn worker_turn_contains_pr_done_contract() {
        let turn = build_worker_turn("W-1", 42, "title", "body");
        assert!(
            turn.contains("gh pr create"),
            "worker template must instruct agent to open a PR"
        );
        assert!(
            turn.contains("quorum done --agent W-1 --pr"),
            "worker template must instruct agent to signal done with PR number"
        );
        assert!(
            turn.contains("quorum task-update --task-id 42 --agent W-1 --note-file"),
            "worker template must instruct agent to post progress notes"
        );
        assert!(
            turn.contains("Do NOT mark the task done yourself"),
            "worker template must warn against manual task-done"
        );
    }

    /// Extracts every `quorum <subcommand> --<flag>` from all turn-template
    /// strings and validates each subcommand and flag against the clap Command
    /// tree. Catches drift between what templates tell agents to run and what
    /// the binary actually accepts.
    #[test]
    fn turn_template_cli_invocations_match_clap_surface() {
        use clap::CommandFactory;

        let spec = ReviewerSpec {
            pr: 1,
            worker_agent: "W".into(),
            reviewer_name: "R".into(),
        };
        let templates: &[(&str, String)] = &[
            ("worker", build_worker_turn("A", 1, "t", "b")),
            ("reviewer", build_review_prompt(&spec)),
            ("rework", build_rework_turn("fix it")),
        ];

        let clap_cmd = crate::cli::Cli::command();

        // Extract the text to scan: JSON turn templates embed the content
        // inside {"message":{"content":"..."}}, with literal newlines escaped
        // as \n. Parse JSON and pull the content string so we get real lines.
        fn extract_scannable_text(raw: &str) -> String {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) {
                if let Some(s) = v["message"]["content"].as_str() {
                    return s.to_string();
                }
            }
            raw.to_string()
        }

        let mut invocations_checked = 0usize;
        for (template_name, text) in templates {
            let scannable = extract_scannable_text(text);
            for line in scannable.lines() {
                let Some(pos) = line.find("quorum ") else {
                    continue;
                };
                let rest = &line[pos + "quorum ".len()..];
                let tokens: Vec<&str> = rest.split_whitespace().collect();
                if tokens.is_empty() {
                    continue;
                }

                let subcommand = tokens[0];
                let sub = clap_cmd.find_subcommand(subcommand);
                assert!(
                    sub.is_some(),
                    "template '{template_name}' references unknown subcommand \
                     'quorum {subcommand}'"
                );
                let sub = sub.unwrap();

                for token in &tokens[1..] {
                    if let Some(flag) = token.strip_prefix("--") {
                        let has_flag = sub.get_arguments().any(|a| a.get_long() == Some(flag));
                        assert!(
                            has_flag,
                            "template '{template_name}' references unknown flag \
                             '--{flag}' on 'quorum {subcommand}'"
                        );
                    }
                }
                invocations_checked += 1;
            }
        }

        assert!(
            invocations_checked > 0,
            "no CLI invocations found in any turn template — if templates \
             no longer embed quorum commands, update or remove this test"
        );
    }
}
