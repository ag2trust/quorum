//! Closed task-level model tier vocabulary.
//!
//! Task label validation and daemon dispatch both use this table so a tier can
//! never be accepted without resolving to the exact model selected at spawn.

/// A task-level `tier:` label and the full model ID it selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelTier {
    pub tier: &'static str,
    pub model_id: &'static str,
}

/// The complete, closed vocabulary accepted in task `tier:` labels.
pub const MODEL_TIERS: &[ModelTier] = &[
    ModelTier {
        tier: "sonnet-5",
        model_id: "claude-sonnet-5",
    },
    ModelTier {
        tier: "opus-46",
        model_id: "claude-opus-4-6",
    },
    ModelTier {
        tier: "opus-47",
        model_id: "claude-opus-4-7",
    },
    ModelTier {
        tier: "opus-48",
        model_id: "claude-opus-4-8",
    },
    ModelTier {
        tier: "luna",
        model_id: "gpt-5.6-luna",
    },
    ModelTier {
        tier: "terra",
        model_id: "gpt-5.6-terra",
    },
    ModelTier {
        tier: "sol",
        model_id: "gpt-5.6-sol",
    },
];

/// Resolve a task tier suffix to its exact model ID.
pub fn model_id_for_tier(tier: &str) -> Option<&'static str> {
    MODEL_TIERS
        .iter()
        .find(|entry| entry.tier == tier)
        .map(|entry| entry.model_id)
}

/// Render the closed vocabulary for usage errors.
pub fn known_tiers() -> String {
    MODEL_TIERS
        .iter()
        .map(|entry| entry.tier)
        .collect::<Vec<_>>()
        .join("|")
}

/// Resolve a Claude-only model floor tier to its exact model ID.
pub fn claude_model_id_for_tier(tier: &str) -> Option<&'static str> {
    model_id_for_tier(tier).filter(|model_id| model_id.starts_with("claude-"))
}

/// Render the Claude-only vocabulary accepted by `min_model`.
pub fn known_claude_tiers() -> String {
    MODEL_TIERS
        .iter()
        .filter(|entry| entry.model_id.starts_with("claude-"))
        .map(|entry| entry.tier)
        .collect::<Vec<_>>()
        .join("|")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_supported_tier_resolves_to_its_exact_model_id() {
        let expected = [
            ("sonnet-5", "claude-sonnet-5"),
            ("opus-46", "claude-opus-4-6"),
            ("opus-47", "claude-opus-4-7"),
            ("opus-48", "claude-opus-4-8"),
            ("luna", "gpt-5.6-luna"),
            ("terra", "gpt-5.6-terra"),
            ("sol", "gpt-5.6-sol"),
        ];
        assert_eq!(MODEL_TIERS.len(), expected.len());
        for (tier, model_id) in expected {
            assert_eq!(model_id_for_tier(tier), Some(model_id));
        }
    }

    #[test]
    fn legacy_and_unknown_tiers_are_not_resolvable() {
        for tier in ["o3", "o4-mini", "unknown"] {
            assert_eq!(model_id_for_tier(tier), None, "{tier}");
        }
    }

    #[test]
    fn claude_floor_vocabulary_excludes_codex_tiers() {
        assert_eq!(claude_model_id_for_tier("opus-47"), Some("claude-opus-4-7"));
        for tier in ["luna", "terra", "sol", "unknown"] {
            assert_eq!(claude_model_id_for_tier(tier), None, "{tier}");
        }
        assert_eq!(known_claude_tiers(), "sonnet-5|opus-46|opus-47|opus-48");
    }
}
