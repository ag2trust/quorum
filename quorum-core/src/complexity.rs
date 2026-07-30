//! Single source of truth for the 1-5 complexity rubric and default
//! model/effort recommendations. Used by the classifier prompt, the
//! cheatsheet, and regression tests.

/// (level, short label, description, time estimate)
pub const RUBRIC: [(u8, &str, &str, &str); 5] = [
    (1, "Trivial", "config tweak, typo fix, simple rename", ""),
    (
        2,
        "Simple",
        "single-file change, clear spec",
        "< 15 min agent work",
    ),
    (
        3,
        "Moderate",
        "multi-file change, some design decisions",
        "15-30 min",
    ),
    (
        4,
        "Complex",
        "cross-cutting change, multiple components",
        "30-60 min",
    ),
    (
        5,
        "Very complex",
        "architectural change, new subsystem",
        "> 60 min",
    ),
];

/// Provider whose operational routing policy supplies recommendations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecommendationProvider {
    Claude,
    Codex,
}

/// Claude default (level, model_id, effort) recommendations.
/// Daemon `suggested_models` config overrides these when set.
pub const CLAUDE_RECOMMENDATIONS: [(u8, &str, &str); 5] = [
    (1, "claude-sonnet-5", "medium"),
    (2, "claude-opus-4-6", "medium"),
    (3, "claude-opus-4-6", "high"),
    (4, "claude-opus-4-7", "high"),
    (5, "claude-opus-4-8", "high"),
];

/// Codex default (level, model_id, effort) recommendations.
/// This is Quorum's operational routing policy, not a cross-vendor benchmark.
pub const CODEX_RECOMMENDATIONS: [(u8, &str, &str); 5] = [
    (1, "gpt-5.6-luna", "high"),
    (2, "gpt-5.6-terra", "high"),
    (3, "gpt-5.6-terra", "high"),
    (4, "gpt-5.6-sol", "high"),
    (5, "gpt-5.6-sol", "high"),
];

/// Return the complete built-in recommendation ladder for one provider.
pub fn recommendations_for(
    provider: RecommendationProvider,
) -> &'static [(u8, &'static str, &'static str); 5] {
    match provider {
        RecommendationProvider::Claude => &CLAUDE_RECOMMENDATIONS,
        RecommendationProvider::Codex => &CODEX_RECOMMENDATIONS,
    }
}

/// Render the rubric as lines for embedding in prompts or help text.
/// Format: `  - N: Label — description[, time]`
pub fn rubric_lines() -> String {
    RUBRIC
        .iter()
        .map(|(level, label, desc, time)| {
            if time.is_empty() {
                format!("   - {level}: {label} — {desc}")
            } else {
                format!("   - {level}: {label} — {desc}, {time}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Render one provider's recommendation table for agent-facing guidance.
/// Each line: `  N → model / effort`
pub fn recommendation_lines(provider: RecommendationProvider) -> String {
    recommendations_for(provider)
        .iter()
        .map(|(level, model, effort)| format!("   {level} → {model} / {effort}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Render both built-in routing ladders for provider-neutral help.
pub fn all_recommendation_lines() -> String {
    format!(
        "  Claude:\n{}\n  Codex:\n{}",
        recommendation_lines(RecommendationProvider::Claude),
        recommendation_lines(RecommendationProvider::Codex),
    )
}

/// Short inline rubric for cheatsheet (one-line per level).
pub fn rubric_inline() -> String {
    RUBRIC
        .iter()
        .map(|(level, _label, desc, _time)| format!("{level}={desc}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rubric_covers_1_through_5() {
        for i in 1..=5u8 {
            assert!(
                RUBRIC.iter().any(|(l, _, _, _)| *l == i),
                "rubric missing level {i}"
            );
        }
    }

    #[test]
    fn claude_recommendations_match_the_complete_legacy_ladder() {
        assert_eq!(
            CLAUDE_RECOMMENDATIONS,
            [
                (1, "claude-sonnet-5", "medium"),
                (2, "claude-opus-4-6", "medium"),
                (3, "claude-opus-4-6", "high"),
                (4, "claude-opus-4-7", "high"),
                (5, "claude-opus-4-8", "high"),
            ]
        );
    }

    #[test]
    fn codex_recommendations_match_the_complete_routing_policy() {
        assert_eq!(
            CODEX_RECOMMENDATIONS,
            [
                (1, "gpt-5.6-luna", "high"),
                (2, "gpt-5.6-terra", "high"),
                (3, "gpt-5.6-terra", "high"),
                (4, "gpt-5.6-sol", "high"),
                (5, "gpt-5.6-sol", "high"),
            ]
        );
    }

    #[test]
    fn rubric_level_4_is_cross_cutting() {
        let (_, _, desc, _) = RUBRIC.iter().find(|(l, _, _, _)| *l == 4).unwrap();
        assert!(
            desc.contains("cross-cutting") && desc.contains("multiple components"),
            "level 4 must say cross-cutting/multiple components, got: {desc}"
        );
    }

    #[test]
    fn rubric_level_5_is_architectural() {
        let (_, _, desc, _) = RUBRIC.iter().find(|(l, _, _, _)| *l == 5).unwrap();
        assert!(
            desc.contains("architectural") && desc.contains("new subsystem"),
            "level 5 must say architectural/new subsystem, got: {desc}"
        );
    }

    #[test]
    fn rubric_lines_renders_all_levels() {
        let text = rubric_lines();
        for i in 1..=5 {
            assert!(text.contains(&format!("- {i}:")), "missing level {i}");
        }
    }

    #[test]
    fn recommendation_lines_renders_all_levels() {
        let text = recommendation_lines(RecommendationProvider::Claude);
        for i in 1..=5 {
            assert!(text.contains(&format!("{i} →")), "missing level {i}");
        }
    }

    #[test]
    fn provider_selection_never_mixes_ladders() {
        let claude = recommendation_lines(RecommendationProvider::Claude);
        let codex = recommendation_lines(RecommendationProvider::Codex);
        assert!(claude.contains("claude-opus-4-8"));
        assert!(!claude.contains("gpt-5.6"));
        assert!(codex.contains("gpt-5.6-sol"));
        assert!(!codex.contains("claude-"));
    }
}
