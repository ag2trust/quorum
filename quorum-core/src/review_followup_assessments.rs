//! Closed domain types for dormant review follow-up assessment storage.
//!
//! The types in this module map every column in the assessment and membership
//! tables while keeping persisted enum and boolean values closed. There is
//! deliberately no read, write, planning, disposition, or lifecycle API here;
//! later activation work must add those authority boundaries explicitly.

use crate::error::{QuorumError, Result};
use crate::review_followups::MAX_FOLLOWUP_TEXT_BYTES;
use std::fmt;
use std::str::FromStr;

macro_rules! closed_text_enum {
    ($name:ident, $label:literal, {$($variant:ident => $value:literal),+ $(,)?}) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

closed_text_enum!(FollowupScopeKind, "follow-up assessment scope kind", {
    Task => "task",
    Graph => "graph",
});

closed_text_enum!(FollowupAssessmentState, "follow-up assessment state", {
    Pending => "pending",
    Planning => "planning",
    ProviderBackoff => "provider-backoff",
    Held => "held",
    Completed => "completed",
});

fn validate_optional_text(field: &str, value: Option<&str>) -> Result<()> {
    if value.is_some_and(|value| {
        value.is_empty() || value.contains('\0') || value.len() > MAX_FOLLOWUP_TEXT_BYTES
    }) {
        return Err(QuorumError::Usage(format!(
            "invalid bounded follow-up assessment {field}"
        )));
    }
    Ok(())
}

/// Canonical SQLite authority target for one assessment scope.
pub fn assessment_target(scope_kind: FollowupScopeKind, scope_id: i64) -> Result<String> {
    if scope_id <= 0 {
        return Err(QuorumError::Usage(
            "follow-up assessment scope id must be positive".into(),
        ));
    }
    Ok(format!("followup:{}:{scope_id}", scope_kind.as_str()))
}

/// One fully typed stored row from `review_followup_assessments`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewFollowupAssessment {
    id: i64,
    target: String,
    scope_kind: FollowupScopeKind,
    scope_id: i64,
    source_task_id: i64,
    state: FollowupAssessmentState,
    active: bool,
    proposal_attempts: usize,
    provider_failures: usize,
    planner_provider: Option<String>,
    planner_model: Option<String>,
    planner_assignment_id: Option<i64>,
    base_sha: Option<String>,
    hold_code: Option<String>,
    hold_summary: Option<String>,
    created_at: i64,
    updated_at: i64,
}

impl ReviewFollowupAssessment {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: i64,
        target: String,
        scope_kind: FollowupScopeKind,
        scope_id: i64,
        source_task_id: i64,
        state: FollowupAssessmentState,
        active: bool,
        proposal_attempts: usize,
        provider_failures: usize,
        planner_provider: Option<String>,
        planner_model: Option<String>,
        planner_assignment_id: Option<i64>,
        base_sha: Option<String>,
        hold_code: Option<String>,
        hold_summary: Option<String>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<Self> {
        if id <= 0 || source_task_id <= 0 {
            return Err(QuorumError::Usage(
                "follow-up assessment relationship ids must be positive".into(),
            ));
        }
        if planner_assignment_id.is_some_and(|id| id <= 0) {
            return Err(QuorumError::Usage(
                "follow-up assessment planner assignment id must be positive".into(),
            ));
        }
        if target != assessment_target(scope_kind, scope_id)? {
            return Err(QuorumError::Usage(
                "follow-up assessment target does not match its scope".into(),
            ));
        }
        for (field, value) in [
            ("planner provider", planner_provider.as_deref()),
            ("planner model", planner_model.as_deref()),
            ("base SHA", base_sha.as_deref()),
            ("hold code", hold_code.as_deref()),
            ("hold summary", hold_summary.as_deref()),
        ] {
            validate_optional_text(field, value)?;
        }
        if created_at < 0 || updated_at < created_at {
            return Err(QuorumError::Usage(
                "follow-up assessment timestamps are inconsistent".into(),
            ));
        }
        Ok(Self {
            id,
            target,
            scope_kind,
            scope_id,
            source_task_id,
            state,
            active,
            proposal_attempts,
            provider_failures,
            planner_provider,
            planner_model,
            planner_assignment_id,
            base_sha,
            hold_code,
            hold_summary,
            created_at,
            updated_at,
        })
    }

    /// Reconstruct a row from SQLite, rejecting open-ended text enums, invalid
    /// boolean sentinels, and negative counters before they reach callers.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored(
        id: i64,
        target: String,
        scope_kind: &str,
        scope_id: i64,
        source_task_id: i64,
        state: &str,
        active: i64,
        proposal_attempts: i64,
        provider_failures: i64,
        planner_provider: Option<String>,
        planner_model: Option<String>,
        planner_assignment_id: Option<i64>,
        base_sha: Option<String>,
        hold_code: Option<String>,
        hold_summary: Option<String>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<Self> {
        let active = match active {
            0 => false,
            1 => true,
            _ => {
                return Err(QuorumError::Usage(
                    "invalid follow-up assessment active sentinel".into(),
                ))
            }
        };
        let proposal_attempts = usize::try_from(proposal_attempts).map_err(|_| {
            QuorumError::Usage("follow-up proposal attempts cannot be negative".into())
        })?;
        let provider_failures = usize::try_from(provider_failures).map_err(|_| {
            QuorumError::Usage("follow-up provider failures cannot be negative".into())
        })?;
        Self::new(
            id,
            target,
            scope_kind.parse()?,
            scope_id,
            source_task_id,
            state.parse()?,
            active,
            proposal_attempts,
            provider_failures,
            planner_provider,
            planner_model,
            planner_assignment_id,
            base_sha,
            hold_code,
            hold_summary,
            created_at,
            updated_at,
        )
    }

    pub fn id(&self) -> i64 {
        self.id
    }
    pub fn target(&self) -> &str {
        &self.target
    }
    pub fn scope_kind(&self) -> FollowupScopeKind {
        self.scope_kind
    }
    pub fn scope_id(&self) -> i64 {
        self.scope_id
    }
    pub fn source_task_id(&self) -> i64 {
        self.source_task_id
    }
    pub fn state(&self) -> FollowupAssessmentState {
        self.state
    }
    pub fn active(&self) -> bool {
        self.active
    }
    pub fn proposal_attempts(&self) -> usize {
        self.proposal_attempts
    }
    pub fn provider_failures(&self) -> usize {
        self.provider_failures
    }
    pub fn planner_provider(&self) -> Option<&str> {
        self.planner_provider.as_deref()
    }
    pub fn planner_model(&self) -> Option<&str> {
        self.planner_model.as_deref()
    }
    pub fn planner_assignment_id(&self) -> Option<i64> {
        self.planner_assignment_id
    }
    pub fn base_sha(&self) -> Option<&str> {
        self.base_sha.as_deref()
    }
    pub fn hold_code(&self) -> Option<&str> {
        self.hold_code.as_deref()
    }
    pub fn hold_summary(&self) -> Option<&str> {
        self.hold_summary.as_deref()
    }
    pub fn created_at(&self) -> i64 {
        self.created_at
    }
    pub fn updated_at(&self) -> i64 {
        self.updated_at
    }
}

/// One fully typed immutable membership row from
/// `review_followup_assessment_artifacts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReviewFollowupAssessmentArtifact {
    assessment_id: i64,
    artifact_id: i64,
}

impl ReviewFollowupAssessmentArtifact {
    pub fn new(assessment_id: i64, artifact_id: i64) -> Result<Self> {
        if assessment_id <= 0 || artifact_id <= 0 {
            return Err(QuorumError::Usage(
                "follow-up assessment membership ids must be positive".into(),
            ));
        }
        Ok(Self {
            assessment_id,
            artifact_id,
        })
    }

    pub fn assessment_id(self) -> i64 {
        self.assessment_id
    }

    pub fn artifact_id(self) -> i64 {
        self.artifact_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored() -> ReviewFollowupAssessment {
        ReviewFollowupAssessment::from_stored(
            1,
            "followup:graph:7".into(),
            "graph",
            7,
            3,
            "provider-backoff",
            1,
            2,
            1,
            Some("openai".into()),
            Some("planner".into()),
            Some(9),
            Some("0123456789abcdef".into()),
            Some("provider-failure".into()),
            Some("retry after the bounded backoff".into()),
            10,
            11,
        )
        .unwrap()
    }

    #[test]
    fn stored_assessment_maps_every_column_to_closed_values() {
        let value = stored();
        assert_eq!(value.id(), 1);
        assert_eq!(value.target(), "followup:graph:7");
        assert_eq!(value.scope_kind(), FollowupScopeKind::Graph);
        assert_eq!(value.scope_id(), 7);
        assert_eq!(value.source_task_id(), 3);
        assert_eq!(value.state(), FollowupAssessmentState::ProviderBackoff);
        assert!(value.active());
        assert_eq!(value.proposal_attempts(), 2);
        assert_eq!(value.provider_failures(), 1);
        assert_eq!(value.planner_provider(), Some("openai"));
        assert_eq!(value.planner_model(), Some("planner"));
        assert_eq!(value.planner_assignment_id(), Some(9));
        assert_eq!(value.base_sha(), Some("0123456789abcdef"));
        assert_eq!(value.hold_code(), Some("provider-failure"));
        assert_eq!(
            value.hold_summary(),
            Some("retry after the bounded backoff")
        );
        assert_eq!(value.created_at(), 10);
        assert_eq!(value.updated_at(), 11);
    }

    #[test]
    fn stored_assessment_rejects_open_or_inconsistent_values() {
        assert!("repository".parse::<FollowupScopeKind>().is_err());
        assert!("retrying".parse::<FollowupAssessmentState>().is_err());

        let mut value = stored();
        value.target = "followup:task:7".into();
        assert!(ReviewFollowupAssessment::new(
            value.id,
            value.target,
            value.scope_kind,
            value.scope_id,
            value.source_task_id,
            value.state,
            value.active,
            value.proposal_attempts,
            value.provider_failures,
            value.planner_provider,
            value.planner_model,
            value.planner_assignment_id,
            value.base_sha,
            value.hold_code,
            value.hold_summary,
            value.created_at,
            value.updated_at,
        )
        .is_err());

        let make = |active, attempts| {
            ReviewFollowupAssessment::from_stored(
                1,
                "followup:task:7".into(),
                "task",
                7,
                7,
                "pending",
                active,
                attempts,
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
        };
        assert!(make(2, 0).is_err());
        assert!(make(0, -1).is_err());
    }

    #[test]
    fn membership_requires_positive_relationship_ids() {
        let membership = ReviewFollowupAssessmentArtifact::new(2, 3).unwrap();
        assert_eq!(membership.assessment_id(), 2);
        assert_eq!(membership.artifact_id(), 3);
        assert!(ReviewFollowupAssessmentArtifact::new(0, 3).is_err());
        assert!(ReviewFollowupAssessmentArtifact::new(2, -1).is_err());
    }
}
