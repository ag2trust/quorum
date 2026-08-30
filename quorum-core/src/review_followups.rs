//! Closed, bounded domain types for immutable post-merge review follow-ups.
//!
//! This module deliberately contains no persistence or scheduling behavior. The
//! constructors are the shared validation boundary for later collector, storage,
//! and inspection code: persisted strings must become closed enums, JSON arrays
//! must be parsed into bounded values, and disposition relationships must agree.

use crate::error::{QuorumError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

pub const MAX_FOLLOWUP_ARTIFACTS: usize = 32;
pub const MAX_FOLLOWUP_TEXT_BYTES: usize = 8 * 1024;
pub const MAX_FOLLOWUP_ARTIFACT_JSON_BYTES: usize = 64 * 1024;
pub const MAX_VERIFICATION_EXPECTATIONS: usize = 8;
/// A concrete evidence list can cite every bounded finding in one interpretation.
pub const MAX_FOLLOWUP_EVIDENCE_IDS: usize = 128;

macro_rules! closed_text_enum {
    ($name:ident, $label:literal, {$($variant:ident => $value:literal),+ $(,)?}) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = QuorumError;

            fn from_str(value: &str) -> Result<Self> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(QuorumError::Usage(format!(
                        "invalid {}: {value}", $label
                    ))),
                }
            }
        }
    };
}

closed_text_enum!(TechnicalImpact, "technical impact", {
    Critical => "critical",
    Major => "major",
    Minor => "minor",
    Nit => "nit",
});

closed_text_enum!(ScopeRelationship, "scope relationship", {
    PreExisting => "pre_existing",
    OutOfScope => "out_of_scope",
    ThreatModelExpansion => "threat_model_expansion",
    DefenseInDepth => "defense_in_depth",
    FutureRequirement => "future_requirement",
    DesignDebt => "design_debt",
});

closed_text_enum!(FollowupDisposition, "follow-up disposition", {
    Created => "created",
    Linked => "linked",
    Dismissed => "dismissed",
    Deferred => "deferred",
});

closed_text_enum!(FollowupBatchState, "follow-up batch state", {
    Collected => "collected",
    Assessing => "assessing",
    Resolved => "resolved",
});

closed_text_enum!(EvidenceKind, "follow-up evidence kind", {
    Review => "review",
    ReviewComment => "review_comment",
    IssueComment => "issue_comment",
});

fn validate_required_text(field: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.contains('\0') || value.len() > MAX_FOLLOWUP_TEXT_BYTES {
        return Err(QuorumError::Usage(format!(
            "invalid bounded follow-up {field}"
        )));
    }
    Ok(())
}

fn validate_optional_id(field: &str, value: Option<i64>) -> Result<()> {
    if value.is_some_and(|id| id <= 0) {
        return Err(QuorumError::Usage(format!(
            "follow-up {field} must be positive"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewFollowupEvidenceId {
    kind: EvidenceKind,
    id: i64,
}

impl ReviewFollowupEvidenceId {
    pub fn new(kind: EvidenceKind, id: i64) -> Result<Self> {
        if id <= 0 {
            return Err(QuorumError::Usage(
                "follow-up evidence id must be positive".into(),
            ));
        }
        Ok(Self { kind, id })
    }

    pub fn kind(&self) -> EvidenceKind {
        self.kind
    }

    pub fn id(&self) -> i64 {
        self.id
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEvidenceId {
    kind: String,
    id: i64,
}

/// One or more unique, concrete GitHub record IDs, bounded before and after JSON parsing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct FollowupEvidenceIds(Vec<ReviewFollowupEvidenceId>);

impl FollowupEvidenceIds {
    pub fn new(values: Vec<ReviewFollowupEvidenceId>) -> Result<Self> {
        if values.is_empty() || values.len() > MAX_FOLLOWUP_EVIDENCE_IDS {
            return Err(QuorumError::Usage(
                "follow-up evidence ids must contain one through 128 entries".into(),
            ));
        }
        let unique = values
            .iter()
            .map(|value| (value.kind, value.id))
            .collect::<HashSet<_>>();
        if unique.len() != values.len() {
            return Err(QuorumError::Usage(
                "follow-up evidence ids must be unique".into(),
            ));
        }
        let encoded = serde_json::to_vec(&values).map_err(|error| {
            QuorumError::Io(format!(
                "serialize follow-up evidence for validation: {error}"
            ))
        })?;
        if encoded.len() > MAX_FOLLOWUP_ARTIFACT_JSON_BYTES {
            return Err(QuorumError::Usage(
                "follow-up evidence JSON exceeds bounded size".into(),
            ));
        }
        Ok(Self(values))
    }

    pub fn from_json(json: &str) -> Result<Self> {
        if json.len() > MAX_FOLLOWUP_ARTIFACT_JSON_BYTES || json.contains('\0') {
            return Err(QuorumError::Usage(
                "follow-up evidence JSON exceeds bounded size".into(),
            ));
        }
        let raw: Vec<RawEvidenceId> = serde_json::from_str(json).map_err(|error| {
            QuorumError::Usage(format!("malformed follow-up evidence JSON: {error}"))
        })?;
        let values = raw
            .into_iter()
            .map(|value| ReviewFollowupEvidenceId::new(value.kind.parse()?, value.id))
            .collect::<Result<Vec<_>>>()?;
        Self::new(values)
    }

    pub fn as_slice(&self) -> &[ReviewFollowupEvidenceId] {
        &self.0
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|error| QuorumError::Io(format!("serialize follow-up evidence: {error}")))
    }
}

/// One through eight non-empty, bounded expectations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct VerificationExpectations(Vec<String>);

impl VerificationExpectations {
    pub fn new(values: Vec<String>) -> Result<Self> {
        if values.is_empty() || values.len() > MAX_VERIFICATION_EXPECTATIONS {
            return Err(QuorumError::Usage(
                "follow-up verification expectations must contain one through eight entries".into(),
            ));
        }
        for value in &values {
            validate_required_text("verification expectation", value)?;
        }
        let encoded = serde_json::to_vec(&values).map_err(|error| {
            QuorumError::Io(format!(
                "serialize follow-up verification for validation: {error}"
            ))
        })?;
        if encoded.len() > MAX_FOLLOWUP_ARTIFACT_JSON_BYTES {
            return Err(QuorumError::Usage(
                "follow-up verification JSON exceeds bounded size".into(),
            ));
        }
        Ok(Self(values))
    }

    pub fn from_json(json: &str) -> Result<Self> {
        if json.len() > MAX_FOLLOWUP_ARTIFACT_JSON_BYTES || json.contains('\0') {
            return Err(QuorumError::Usage(
                "follow-up verification JSON exceeds bounded size".into(),
            ));
        }
        let values: Vec<String> = serde_json::from_str(json).map_err(|error| {
            QuorumError::Usage(format!("malformed follow-up verification JSON: {error}"))
        })?;
        Self::new(values)
    }

    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|error| {
            QuorumError::Io(format!(
                "serialize follow-up verification expectations: {error}"
            ))
        })
    }
}

/// A disposition and its relationship columns, validated as one indivisible value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactDisposition {
    kind: FollowupDisposition,
    reason: String,
    linked_task_id: Option<i64>,
    created_task_id: Option<i64>,
}

impl ArtifactDisposition {
    pub fn new(
        kind: FollowupDisposition,
        reason: String,
        linked_task_id: Option<i64>,
        created_task_id: Option<i64>,
    ) -> Result<Self> {
        validate_required_text("disposition reason", &reason)?;
        validate_optional_id("linked task id", linked_task_id)?;
        validate_optional_id("created task id", created_task_id)?;
        let relationships_match = match kind {
            FollowupDisposition::Created => created_task_id.is_some() && linked_task_id.is_none(),
            FollowupDisposition::Linked => linked_task_id.is_some() && created_task_id.is_none(),
            FollowupDisposition::Dismissed | FollowupDisposition::Deferred => {
                linked_task_id.is_none() && created_task_id.is_none()
            }
        };
        if !relationships_match {
            return Err(QuorumError::Usage(
                "follow-up disposition has inconsistent task relationships".into(),
            ));
        }
        Ok(Self {
            kind,
            reason,
            linked_task_id,
            created_task_id,
        })
    }

    pub fn from_stored(
        disposition: Option<&str>,
        reason: Option<String>,
        linked_task_id: Option<i64>,
        created_task_id: Option<i64>,
    ) -> Result<Option<Self>> {
        match (disposition, reason) {
            (None, None) if linked_task_id.is_none() && created_task_id.is_none() => Ok(None),
            (Some(kind), Some(reason)) => Ok(Some(Self::new(
                kind.parse()?,
                reason,
                linked_task_id,
                created_task_id,
            )?)),
            _ => Err(QuorumError::Usage(
                "follow-up disposition columns are incomplete or inconsistent".into(),
            )),
        }
    }

    pub fn kind(&self) -> FollowupDisposition {
        self.kind
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn linked_task_id(&self) -> Option<i64> {
        self.linked_task_id
    }

    pub fn created_task_id(&self) -> Option<i64> {
        self.created_task_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewFollowupBatch {
    pr_number: i64,
    task_id: i64,
    graph_id: Option<i64>,
    source_task_id: i64,
    collector_version: String,
    artifact_count: usize,
    state: FollowupBatchState,
    created_at: i64,
    updated_at: i64,
}

impl ReviewFollowupBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pr_number: i64,
        task_id: i64,
        graph_id: Option<i64>,
        source_task_id: i64,
        collector_version: String,
        artifact_count: usize,
        state: FollowupBatchState,
        created_at: i64,
        updated_at: i64,
    ) -> Result<Self> {
        if pr_number <= 0 || task_id <= 0 || source_task_id <= 0 {
            return Err(QuorumError::Usage(
                "follow-up batch relationship ids must be positive".into(),
            ));
        }
        validate_optional_id("batch graph id", graph_id)?;
        validate_required_text("collector version", &collector_version)?;
        if artifact_count > MAX_FOLLOWUP_ARTIFACTS {
            return Err(QuorumError::Usage(
                "follow-up batch exceeds 32 artifacts".into(),
            ));
        }
        if created_at < 0 || updated_at < created_at {
            return Err(QuorumError::Usage(
                "follow-up batch timestamps are inconsistent".into(),
            ));
        }
        Ok(Self {
            pr_number,
            task_id,
            graph_id,
            source_task_id,
            collector_version,
            artifact_count,
            state,
            created_at,
            updated_at,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_stored(
        pr_number: i64,
        task_id: i64,
        graph_id: Option<i64>,
        source_task_id: i64,
        collector_version: String,
        artifact_count: i64,
        state: &str,
        created_at: i64,
        updated_at: i64,
    ) -> Result<Self> {
        let artifact_count = usize::try_from(artifact_count).map_err(|_| {
            QuorumError::Usage("follow-up batch artifact count cannot be negative".into())
        })?;
        Self::new(
            pr_number,
            task_id,
            graph_id,
            source_task_id,
            collector_version,
            artifact_count,
            state.parse()?,
            created_at,
            updated_at,
        )
    }

    pub fn pr_number(&self) -> i64 {
        self.pr_number
    }
    pub fn task_id(&self) -> i64 {
        self.task_id
    }
    pub fn graph_id(&self) -> Option<i64> {
        self.graph_id
    }
    pub fn source_task_id(&self) -> i64 {
        self.source_task_id
    }
    pub fn collector_version(&self) -> &str {
        &self.collector_version
    }
    pub fn artifact_count(&self) -> usize {
        self.artifact_count
    }
    pub fn state(&self) -> FollowupBatchState {
        self.state
    }
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
    pub fn updated_at(&self) -> i64 {
        self.updated_at
    }
}

#[derive(Serialize)]
struct ArtifactPayload<'a> {
    technical_impact: TechnicalImpact,
    scope_relationship: ScopeRelationship,
    concern: &'a str,
    non_blocking_reason: &'a str,
    affected_behavior: &'a str,
    desired_outcome: &'a str,
    verification_expectations: &'a VerificationExpectations,
    evidence: &'a FollowupEvidenceIds,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewFollowupArtifact {
    id: Option<i64>,
    pr_number: i64,
    ordinal: i64,
    technical_impact: TechnicalImpact,
    scope_relationship: ScopeRelationship,
    concern: String,
    non_blocking_reason: String,
    affected_behavior: String,
    desired_outcome: String,
    verification_expectations: VerificationExpectations,
    evidence_ids: FollowupEvidenceIds,
    disposition: Option<ArtifactDisposition>,
    created_at: i64,
    updated_at: i64,
}

impl ReviewFollowupArtifact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Option<i64>,
        pr_number: i64,
        ordinal: i64,
        technical_impact: TechnicalImpact,
        scope_relationship: ScopeRelationship,
        concern: String,
        non_blocking_reason: String,
        affected_behavior: String,
        desired_outcome: String,
        verification_expectations: VerificationExpectations,
        evidence_ids: FollowupEvidenceIds,
        disposition: Option<ArtifactDisposition>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<Self> {
        validate_optional_id("artifact id", id)?;
        if pr_number <= 0 || ordinal < 0 {
            return Err(QuorumError::Usage(
                "follow-up artifact relationship ids and ordinal are invalid".into(),
            ));
        }
        validate_required_text("concern", &concern)?;
        validate_required_text("non-blocking reason", &non_blocking_reason)?;
        validate_required_text("affected behavior", &affected_behavior)?;
        validate_required_text("desired outcome", &desired_outcome)?;
        if created_at < 0 || updated_at < created_at {
            return Err(QuorumError::Usage(
                "follow-up artifact timestamps are inconsistent".into(),
            ));
        }
        let payload = ArtifactPayload {
            technical_impact,
            scope_relationship,
            concern: &concern,
            non_blocking_reason: &non_blocking_reason,
            affected_behavior: &affected_behavior,
            desired_outcome: &desired_outcome,
            verification_expectations: &verification_expectations,
            evidence: &evidence_ids,
        };
        let encoded = serde_json::to_vec(&payload).map_err(|error| {
            QuorumError::Io(format!(
                "serialize follow-up artifact for validation: {error}"
            ))
        })?;
        if encoded.len() > MAX_FOLLOWUP_ARTIFACT_JSON_BYTES {
            return Err(QuorumError::Usage(
                "follow-up artifact exceeds bounded JSON size".into(),
            ));
        }
        Ok(Self {
            id,
            pr_number,
            ordinal,
            technical_impact,
            scope_relationship,
            concern,
            non_blocking_reason,
            affected_behavior,
            desired_outcome,
            verification_expectations,
            evidence_ids,
            disposition,
            created_at,
            updated_at,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_stored(
        id: i64,
        pr_number: i64,
        ordinal: i64,
        technical_impact: &str,
        scope_relationship: &str,
        concern: String,
        non_blocking_reason: String,
        affected_behavior: String,
        desired_outcome: String,
        verification_expectations_json: &str,
        evidence_ids_json: &str,
        disposition: Option<&str>,
        disposition_reason: Option<String>,
        linked_task_id: Option<i64>,
        created_task_id: Option<i64>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<Self> {
        Self::new(
            Some(id),
            pr_number,
            ordinal,
            technical_impact.parse()?,
            scope_relationship.parse()?,
            concern,
            non_blocking_reason,
            affected_behavior,
            desired_outcome,
            VerificationExpectations::from_json(verification_expectations_json)?,
            FollowupEvidenceIds::from_json(evidence_ids_json)?,
            ArtifactDisposition::from_stored(
                disposition,
                disposition_reason,
                linked_task_id,
                created_task_id,
            )?,
            created_at,
            updated_at,
        )
    }

    pub fn id(&self) -> Option<i64> {
        self.id
    }
    pub fn pr_number(&self) -> i64 {
        self.pr_number
    }
    pub fn ordinal(&self) -> i64 {
        self.ordinal
    }
    pub fn technical_impact(&self) -> TechnicalImpact {
        self.technical_impact
    }
    pub fn scope_relationship(&self) -> ScopeRelationship {
        self.scope_relationship
    }
    pub fn concern(&self) -> &str {
        &self.concern
    }
    pub fn non_blocking_reason(&self) -> &str {
        &self.non_blocking_reason
    }
    pub fn affected_behavior(&self) -> &str {
        &self.affected_behavior
    }
    pub fn desired_outcome(&self) -> &str {
        &self.desired_outcome
    }
    pub fn verification_expectations(&self) -> &VerificationExpectations {
        &self.verification_expectations
    }
    pub fn evidence_ids(&self) -> &FollowupEvidenceIds {
        &self.evidence_ids
    }
    pub fn disposition(&self) -> Option<&ArtifactDisposition> {
        self.disposition.as_ref()
    }
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
    pub fn updated_at(&self) -> i64 {
        self.updated_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verification() -> VerificationExpectations {
        VerificationExpectations::new(vec!["focused test passes".into()]).unwrap()
    }

    fn evidence() -> FollowupEvidenceIds {
        FollowupEvidenceIds::new(vec![ReviewFollowupEvidenceId::new(
            EvidenceKind::ReviewComment,
            42,
        )
        .unwrap()])
        .unwrap()
    }

    fn artifact(disposition: Option<ArtifactDisposition>) -> Result<ReviewFollowupArtifact> {
        ReviewFollowupArtifact::new(
            None,
            7,
            0,
            TechnicalImpact::Major,
            ScopeRelationship::OutOfScope,
            "concrete failure".into(),
            "safe outside this change".into(),
            "review collection".into(),
            "reject invalid input".into(),
            verification(),
            evidence(),
            disposition,
            100,
            100,
        )
    }

    #[test]
    fn closed_enums_accept_only_documented_values() {
        assert_eq!(
            "critical".parse::<TechnicalImpact>().unwrap(),
            TechnicalImpact::Critical
        );
        assert_eq!(
            "threat_model_expansion"
                .parse::<ScopeRelationship>()
                .unwrap(),
            ScopeRelationship::ThreatModelExpansion
        );
        assert_eq!(
            "deferred".parse::<FollowupDisposition>().unwrap(),
            FollowupDisposition::Deferred
        );
        assert!("severe".parse::<TechnicalImpact>().is_err());
        assert!("adjacent".parse::<ScopeRelationship>().is_err());
        assert!("ignored".parse::<FollowupDisposition>().is_err());
    }

    #[test]
    fn json_array_boundaries_reject_malformed_and_oversized_values() {
        assert!(VerificationExpectations::from_json("not-json").is_err());
        assert!(VerificationExpectations::from_json(r#"{"expectation":"test"}"#).is_err());
        assert!(FollowupEvidenceIds::from_json(
            r#"[{"kind":"review_comment","id":1,"extra":true}]"#
        )
        .is_err());
        assert!(VerificationExpectations::new(vec![
            "test".into();
            MAX_VERIFICATION_EXPECTATIONS + 1
        ])
        .is_err());

        let too_many_evidence = (1..=(MAX_FOLLOWUP_EVIDENCE_IDS as i64 + 1))
            .map(|id| ReviewFollowupEvidenceId::new(EvidenceKind::Review, id).unwrap())
            .collect();
        assert!(FollowupEvidenceIds::new(too_many_evidence).is_err());
    }

    #[test]
    fn invalid_evidence_ids_are_rejected() {
        assert!(ReviewFollowupEvidenceId::new(EvidenceKind::Review, 0).is_err());
        assert!(FollowupEvidenceIds::from_json(r#"[{"kind":"commit","id":1}]"#).is_err());
        assert!(FollowupEvidenceIds::from_json(
            r#"[{"kind":"review","id":1},{"kind":"review","id":1}]"#
        )
        .is_err());
    }

    #[test]
    fn oversized_text_and_total_artifact_json_are_rejected() {
        assert!(
            VerificationExpectations::new(vec!["x".repeat(MAX_FOLLOWUP_TEXT_BYTES + 1)]).is_err()
        );

        let large_expectations =
            VerificationExpectations::new(vec!["x".repeat(7_900); MAX_VERIFICATION_EXPECTATIONS])
                .unwrap();
        let result = ReviewFollowupArtifact::new(
            None,
            7,
            0,
            TechnicalImpact::Major,
            ScopeRelationship::DesignDebt,
            "c".repeat(1_000),
            "r".repeat(1_000),
            "b".repeat(1_000),
            "o".repeat(1_000),
            large_expectations,
            evidence(),
            None,
            1,
            1,
        );
        assert!(result.is_err());

        let oversized_concern = ReviewFollowupArtifact::new(
            None,
            7,
            0,
            TechnicalImpact::Minor,
            ScopeRelationship::DesignDebt,
            "x".repeat(MAX_FOLLOWUP_TEXT_BYTES + 1),
            "reason".into(),
            "behavior".into(),
            "outcome".into(),
            verification(),
            evidence(),
            None,
            1,
            1,
        );
        assert!(oversized_concern.is_err());
    }

    #[test]
    fn disposition_relationships_must_be_consistent() {
        assert!(ArtifactDisposition::new(
            FollowupDisposition::Created,
            "new work".into(),
            Some(8),
            None,
        )
        .is_err());
        assert!(ArtifactDisposition::new(
            FollowupDisposition::Linked,
            "already tracked".into(),
            None,
            None,
        )
        .is_err());
        assert!(ArtifactDisposition::new(
            FollowupDisposition::Dismissed,
            "invalid concern".into(),
            None,
            Some(9),
        )
        .is_err());
        assert!(ArtifactDisposition::from_stored(None, None, Some(8), None).is_err());

        let created = ArtifactDisposition::new(
            FollowupDisposition::Created,
            "new work".into(),
            None,
            Some(9),
        )
        .unwrap();
        assert!(artifact(Some(created)).is_ok());
    }

    #[test]
    fn stored_artifact_revalidates_enums_json_and_relationships() {
        let make = |impact: &str, verification_json: &str, disposition: Option<&str>| {
            ReviewFollowupArtifact::from_stored(
                1,
                7,
                0,
                impact,
                "out_of_scope",
                "concern".into(),
                "reason".into(),
                "behavior".into(),
                "outcome".into(),
                verification_json,
                r#"[{"kind":"review_comment","id":42}]"#,
                disposition,
                disposition.map(|_| "decision".into()),
                None,
                None,
                100,
                100,
            )
        };
        assert!(make("unknown", r#"["test"]"#, None).is_err());
        assert!(make("minor", "not-json", None).is_err());
        assert!(make("minor", r#"["test"]"#, Some("linked")).is_err());
        assert!(make("minor", r#"["test"]"#, Some("deferred")).is_ok());
    }

    #[test]
    fn batch_rejects_invalid_relationships_counts_and_state() {
        assert!(ReviewFollowupBatch::new(
            0,
            2,
            None,
            2,
            "v1".into(),
            0,
            FollowupBatchState::Collected,
            1,
            1,
        )
        .is_err());
        assert!(ReviewFollowupBatch::new(
            1,
            2,
            None,
            2,
            "v1".into(),
            MAX_FOLLOWUP_ARTIFACTS + 1,
            FollowupBatchState::Collected,
            1,
            1,
        )
        .is_err());
        assert!(
            ReviewFollowupBatch::from_stored(1, 2, None, 2, "v1".into(), 0, "pending", 1, 1,)
                .is_err()
        );
    }

    #[test]
    fn bounded_arrays_roundtrip_through_json() {
        let verification = verification();
        assert_eq!(
            VerificationExpectations::from_json(&verification.to_json().unwrap()).unwrap(),
            verification
        );
        let evidence = evidence();
        assert_eq!(
            FollowupEvidenceIds::from_json(&evidence.to_json().unwrap()).unwrap(),
            evidence
        );
    }
}
