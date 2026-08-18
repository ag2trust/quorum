//! Re-review prompt composition.

use super::review_cycle_context::ReviewCycleContext;
use super::review_ledger;
use super::reviewer::{
    graph_review_contract, COMPLETE_REVIEW_CONTRACT, REVIEWER_VERIFICATION_BOUNDARY,
    REVIEW_FINDING_CONTRACT,
};

/// Context shared by sticky re-review turns and replacement reviewer prompts.
pub(super) fn review_round_contract(pr: i64, review_cycle: ReviewCycleContext) -> String {
    format!(
        "{}\n{}",
        review_cycle.prompt_contract(),
        review_ledger::required_cumulative_disposition_contract(pr)
    )
}

#[cfg(test)]
pub fn build_rereview_turn(
    reviewer_name: &str,
    pr: i64,
    worker_agent: &str,
    effort: &str,
) -> String {
    build_rereview_turn_with_context(
        reviewer_name,
        pr,
        worker_agent,
        effort,
        None,
        // This test-only compatibility helper builds a re-review, whose first
        // possible persisted value is one completed transition.
        ReviewCycleContext::from_persisted_rework_round(1, quorum_core::lifecycle::REWORK_CAP),
    )
}

pub fn build_rereview_turn_with_context(
    reviewer_name: &str,
    pr: i64,
    worker_agent: &str,
    effort: &str,
    graph_context: Option<&str>,
    review_cycle: ReviewCycleContext,
) -> String {
    super::agent::user_turn(&format!(
        "The author ({worker}) pushed rework for PR #{pr}. Re-review the updated diff.\n\n\
         Verify the branch actually advanced (new commits since prior review) — approving \
         an unchanged diff over prior blocking findings is forbidden.\n\n\
         Invoke the builtin `review` skill (via the Skill tool) at effort level {effort} \
         for the review methodology. If the builtin skill is unavailable, read the full \
         PR diff and surrounding code and check the repo CLAUDE.md invariants.\n\n\
         Verify prior fixes by reading the prior review thread on the PR. Then re-audit the \
         full current diff and relevant sibling paths; do not narrowly inspect only the last \
         remediation commit.\n\n\
         {review_round_contract}\n\
         {complete_review_contract}\n\
         {finding_contract}\n\
         The PR is the source of truth for this review:\n\
         - Read the prior review thread on the PR. For each earlier finding, resolve it on \
         the PR — mark it fixed, downgrade it, or reaffirm it — so a later reader can \
         determine fixed / accepted / overridden / unaddressed outcomes. Do not silently \
         drop a prior blocker.\n\
         - Post new findings to the PR (inline where a specific file/line is involved, \
         summary comment for cross-cutting findings) and reply to author pushback there.\n\
         - Encouraged GitHub operations: normal PR comments, inline comments, and review summary \
         comments.\n\
         - Forbidden GitHub operations: formal `gh pr review --approve`, `gh pr review \
         --request-changes`, and `gh pr merge` — the daemon posts the formal review from \
         your verdict as the merge account and owns merge.\n\n\
         {verification_boundary}\n\
         Review contract (#206 — the verdict MUST match your own findings):\n\
         - Zero blocking findings: run: quorum submit --agent {name} --pr {pr} \
         --verdict approved --blocking 0\n\
         - One or more blocking findings: write a short blocker summary to a temp file, then \
         run: quorum submit --agent {name} --pr {pr} --verdict changes --blocking <count> \
         --feedback-file <path>\n\
         - The feedback file is a lifecycle-signal summary; the authoritative \
         findings must already be on the PR.\n\n\
         Do NOT merge the PR yourself — the daemon handles merging.\n\
         Do NOT run `gh pr review --approve` — the daemon posts the formal GitHub \
         approval as the merge account after your verdict.\n\
         Do NOT mark the task done yourself — the daemon handles task lifecycle.{graph_contract}",
        worker = worker_agent,
        name = reviewer_name,
        pr = pr,
        complete_review_contract = COMPLETE_REVIEW_CONTRACT,
        finding_contract = REVIEW_FINDING_CONTRACT,
        verification_boundary = REVIEWER_VERIFICATION_BOUNDARY,
        graph_contract = graph_review_contract(reviewer_name, pr, graph_context),
        review_round_contract = review_round_contract(pr, review_cycle),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rereview_turn_requires_bounded_cumulative_ledger() {
        let turn = build_rereview_turn("Rev-1", 491, "Worker-1", "high");

        assert!(turn.contains("Required cumulative cross-round review ledger"));
        assert!(turn.contains("### Prior BLOCKING findings"));
        assert!(turn.contains("Author remedy/response"));
        assert!(turn.contains("Current reviewer disposition"));
        assert!(turn.contains("### Newly discovered findings"));
        assert!(turn.contains("TRUNCATED: additional ledger history omitted; read PR #491"));
        assert!(turn.contains("PR discussion remains authoritative"));
    }
}
