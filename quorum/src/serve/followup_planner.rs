//! Restricted follow-up planning turn.
//!
//! This module only builds bounded model input. The daemon owns assessment
//! state, response parsing, intent staging, and every GitHub mutation.

use quorum_core::error::{QuorumError, Result};
use quorum_core::review_followup_assessments::ReviewFollowupAssessment;
use quorum_core::review_followup_issues::ExistingIssue;
use quorum_core::review_followups::ReviewFollowupArtifact;

pub const MAX_FOLLOWUP_PLANNER_PROMPT_BYTES: usize = 128 * 1024;

pub fn build_prompt(
    assessment: &ReviewFollowupAssessment,
    source_title: &str,
    source_body: Option<&str>,
    artifacts: &[ReviewFollowupArtifact],
    existing_issues: &[ExistingIssue],
) -> Result<String> {
    let artifact_values = artifacts
        .iter()
        .map(|artifact| {
            Ok(serde_json::json!({
                "id": artifact.id().ok_or_else(|| QuorumError::Usage(
                    "follow-up planner artifact has no durable id".into()
                ))?,
                "pr_number": artifact.pr_number(),
                "technical_impact": artifact.technical_impact().as_str(),
                "scope_relationship": artifact.scope_relationship().as_str(),
                "concern": artifact.concern(),
                "non_blocking_reason": artifact.non_blocking_reason(),
                "affected_behavior": artifact.affected_behavior(),
                "desired_outcome": artifact.desired_outcome(),
                "verification_expectations": artifact.verification_expectations().as_slice(),
                "evidence": artifact.evidence_ids().as_slice().iter().map(|evidence| {
                    serde_json::json!({"kind": evidence.kind().as_str(), "id": evidence.id()})
                }).collect::<Vec<_>>(),
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let context = serde_json::json!({
        "assessment": {
            "id": assessment.id(),
            "scope_kind": assessment.scope_kind().as_str(),
            "scope_id": assessment.scope_id(),
            "source_task_id": assessment.source_task_id(),
        },
        "source_task": {"title": source_title, "body": source_body},
        "artifacts": artifact_values,
        "existing_issues": existing_issues,
    });
    let context = serde_json::to_string_pretty(&context).map_err(|error| {
        QuorumError::Io(format!("serialize follow-up planner context: {error}"))
    })?;
    let prompt = format!(
        r#"You are Quorum's post-merge follow-up planner. Assess every supplied artifact exactly once.

Return one raw JSON object only: no markdown, commentary, or tool calls. The exact schema is:
{{
  "outcome": "assessment",
  "decisions": [
    {{"decision":"create","artifact_ids":[1],"reason":"...","issue":{{"title":"...","issue_type":"bug|enhancement|documentation|tests|cleanup","observable_outcome":"...","acceptance_criteria":["..."],"source_constraints":["..."],"verification_expectations":["..."]}}}},
    {{"decision":"link","artifact_ids":[2],"reason":"...","existing_issue_number":123,"existing_issue_url":"exact URL from existing_issues"}},
    {{"decision":"dismiss","artifact_ids":[3],"reason":"...","category":"invalid|obsolete|already_resolved|out_of_product"}},
    {{"decision":"defer","artifact_ids":[4],"reason":"...","required_decision":"..."}}
  ]
}}

Rules:
- Cover the complete immutable artifact set; each artifact id appears exactly once.
- Deduplicate artifacts with the same root outcome into one decision.
- Link only when the supplied issue inventory already tracks the same observable outcome; copy its number and URL exactly.
- Create only execution-ready issues. Use one type and describe observable acceptance and verification.
- Dismiss only with concrete evidence that the artifact is invalid, obsolete, already resolved, or outside the product.
- Defer only when a named owner/product decision is genuinely required.
- Do not revisit whether the merged PR should have been blocked. These are already classified post-merge follow-ups.

Authoritative bounded context:
{context}"#
    );
    if prompt.len() > MAX_FOLLOWUP_PLANNER_PROMPT_BYTES || prompt.contains('\0') {
        return Err(QuorumError::Usage(
            "follow-up planner prompt exceeds its 128 KiB bound".into(),
        ));
    }
    Ok(prompt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::review_followup_assessments::{FollowupAssessmentState, FollowupScopeKind};

    #[test]
    fn prompt_declares_closed_response_and_no_tool_authority() {
        let assessment = ReviewFollowupAssessment::new(
            1,
            "followup:task:2".into(),
            FollowupScopeKind::Task,
            2,
            2,
            FollowupAssessmentState::Pending,
            false,
            true,
            0,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            1,
            1,
        )
        .unwrap();
        let prompt = build_prompt(&assessment, "source", None, &[], &[]).unwrap();
        assert!(prompt.contains("Return one raw JSON object only"));
        assert!(prompt.contains("each artifact id appears exactly once"));
        assert!(prompt.contains("copy its number and URL exactly"));
    }
}
