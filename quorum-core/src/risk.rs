//! Bounded execution-surface signals emitted alongside task classification.
//!
//! Risk flags deliberately describe coupling and contract surfaces, not the
//! reasoning difficulty measured by [`crate::complexity`]. A flagged L/cx4
//! root task routes to decomposition; flags also give the planner source seams
//! without changing child classification or other dispatch shapes.

use serde::{Deserialize, Serialize};

pub const MAX_RISK_FLAGS: usize = 7;
pub const MAX_RISK_EVIDENCE_BYTES: usize = 280;

/// Closed set of execution-surface signals the classifier may report.
///
/// `Unknown` permits the daemon to deserialize an untrusted provider response
/// and reject it at the shared validation boundary instead of accepting an
/// arbitrary string. It is never a valid persisted flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskFlagName {
    GrammarOrParser,
    VersionedFormat,
    SchemaAndValidator,
    ManyConsumers,
    CrossLayer,
    PublicContract,
    Underspecified,
    #[serde(other)]
    Unknown,
}

impl RiskFlagName {
    pub fn is_known(self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

/// One bounded risk signal and the classifier's task-specific evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskFlag {
    pub flag: RiskFlagName,
    pub evidence: String,
}

/// (flag name, rubric line). Keep these signals about coupling/surface only.
pub const RUBRIC: [(RiskFlagName, &str); MAX_RISK_FLAGS] = [
    (
        RiskFlagName::GrammarOrParser,
        "new syntax, keyword, delimiter/sentinel parsing, or tokenizer change",
    ),
    (
        RiskFlagName::VersionedFormat,
        "change to a persisted or compiled format with a version boundary",
    ),
    (
        RiskFlagName::SchemaAndValidator,
        "schema change coupled with semantic-validation change",
    ),
    (
        RiskFlagName::ManyConsumers,
        "three or more consumer sites must change together",
    ),
    (
        RiskFlagName::CrossLayer,
        "touches storage, semantics/lifecycle, and presentation/output in one change",
    ),
    (
        RiskFlagName::PublicContract,
        "serialization, API, CLI, or graph output contract change",
    ),
    (
        RiskFlagName::Underspecified,
        "open semantic questions in the task text (for example, product-level only, TBD, or as appropriate)",
    ),
];

/// Render the closed risk rubric for the classifier prompt.
pub fn rubric_lines() -> String {
    RUBRIC
        .iter()
        .map(|(flag, description)| format!("   - {} — {description}", flag_name(*flag)))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn flag_name(flag: RiskFlagName) -> &'static str {
    match flag {
        RiskFlagName::GrammarOrParser => "grammar_or_parser",
        RiskFlagName::VersionedFormat => "versioned_format",
        RiskFlagName::SchemaAndValidator => "schema_and_validator",
        RiskFlagName::ManyConsumers => "many_consumers",
        RiskFlagName::CrossLayer => "cross_layer",
        RiskFlagName::PublicContract => "public_contract",
        RiskFlagName::Underspecified => "underspecified",
        RiskFlagName::Unknown => "unknown",
    }
}

/// Read deserializable risk flags from task refs for prompt context. Legacy
/// rows without the key, and malformed JSON, intentionally read as no flags.
pub fn risk_flags(refs: &str) -> Vec<RiskFlag> {
    serde_json::from_str::<serde_json::Value>(refs)
        .ok()
        .and_then(|refs| refs.get("cx_risk_flags").cloned())
        .and_then(|flags| serde_json::from_value(flags).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn risk_flags_reads_valid_refs_and_defaults_for_legacy_or_malformed_refs() {
        let flags = risk_flags(
            r#"{"cx_risk_flags":[{"flag":"grammar_or_parser","evidence":"The task adds a delimiter parser."}]}"#,
        );
        assert_eq!(flags.len(), 1);
        assert_eq!(flags[0].flag, RiskFlagName::GrammarOrParser);
        assert_eq!(flags[0].evidence, "The task adds a delimiter parser.");
        assert!(risk_flags(r#"{"cx_est":3}"#).is_empty());
        assert!(risk_flags("not JSON").is_empty());
    }
}
