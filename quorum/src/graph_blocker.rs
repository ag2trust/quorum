//! Closed graph-blocker verdict payload shared by the CLI and daemon.

use serde::{Deserialize, Serialize};

pub const MAX_GRAPH_BLOCKER_JSON_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Feedback {
    pub category: String,
    pub affected_task: i64,
    pub violated_assigned_boundary: String,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub run_id: String,
    pub feedback: Feedback,
}

pub fn parse_feedback(raw: &str) -> Result<Feedback, String> {
    if raw.len() > MAX_GRAPH_BLOCKER_JSON_BYTES {
        return Err(format!(
            "--feedback-json exceeds {MAX_GRAPH_BLOCKER_JSON_BYTES} bytes"
        ));
    }
    if raw.contains('\0') {
        return Err("--feedback-json contains an embedded NUL".into());
    }
    let feedback: Feedback =
        serde_json::from_str(raw).map_err(|error| format!("invalid --feedback-json: {error}"))?;
    validate(&feedback)?;
    Ok(feedback)
}

pub fn parse_envelope(raw: &str) -> Result<Envelope, String> {
    if raw.len() > MAX_GRAPH_BLOCKER_JSON_BYTES || raw.contains('\0') {
        return Err("invalid bounded graph-blocker mailbox payload".into());
    }
    let envelope: Envelope = serde_json::from_str(raw)
        .map_err(|error| format!("invalid graph-blocker mailbox payload: {error}"))?;
    if envelope.run_id.is_empty() || envelope.run_id.contains('\0') {
        return Err("invalid graph-blocker run capability".into());
    }
    validate(&envelope.feedback)?;
    Ok(envelope)
}

pub fn encode(run_id: String, feedback: Feedback) -> Result<String, String> {
    let raw = serde_json::to_string(&Envelope { run_id, feedback })
        .map_err(|error| format!("graph-blocker serialization failed: {error}"))?;
    if raw.len() > MAX_GRAPH_BLOCKER_JSON_BYTES {
        return Err(format!(
            "graph-blocker payload exceeds {MAX_GRAPH_BLOCKER_JSON_BYTES} bytes"
        ));
    }
    Ok(raw)
}

fn validate(feedback: &Feedback) -> Result<(), String> {
    let category_ok =
        feedback.category == quorum_core::decomposition::GRAPH_BLOCKER_CATEGORY_BOUNDARY_VIOLATION;
    if !category_ok
        || feedback.affected_task <= 0
        || feedback.violated_assigned_boundary.trim().is_empty()
        || feedback.violated_assigned_boundary.len() > 1024
        || feedback.evidence.is_empty()
        || feedback.evidence.len() > 8
        || feedback
            .evidence
            .iter()
            .any(|item| item.trim().is_empty() || item.len() > 1024 || item.contains('\0'))
        || feedback.category.contains('\0')
        || feedback.violated_assigned_boundary.contains('\0')
    {
        return Err(
            "graph-blocker feedback requires category 'boundary-violation', a positive affected_task, a bounded assigned boundary, and 1-8 concrete evidence items"
                .into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> &'static str {
        r#"{"category":"boundary-violation","affected_task":7,"violated_assigned_boundary":"only edit parser","evidence":["diff adds schema migration outside assigned parser scope"]}"#
    }

    #[test]
    fn closed_feedback_round_trips_with_capability() {
        let feedback = parse_feedback(valid()).unwrap();
        let encoded = encode("cap-7".into(), feedback.clone()).unwrap();
        assert_eq!(
            parse_envelope(&encoded).unwrap(),
            Envelope {
                run_id: "cap-7".into(),
                feedback
            }
        );
    }

    #[test]
    fn rejects_unknown_category_field_nul_and_oversize() {
        assert!(parse_feedback(&valid().replace("boundary-violation", "scope-concern")).is_err());
        assert!(parse_feedback(&valid().replace("}", ",\"extra\":true}")).is_err());
        assert!(parse_feedback(&valid().replace("only edit parser", "only\0parser")).is_err());
        assert!(parse_feedback(&"x".repeat(MAX_GRAPH_BLOCKER_JSON_BYTES + 1)).is_err());
    }
}
