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

/// Default (level, model_id, effort) recommendations.
/// Daemon `suggested_models` config overrides these when set.
pub const DEFAULT_RECOMMENDATIONS: [(u8, &str, &str); 5] = [
    (1, "claude-sonnet-5", "medium"),
    (2, "claude-opus-4-6", "medium"),
    (3, "claude-opus-4-6", "high"),
    (4, "claude-opus-4-7", "high"),
    (5, "claude-opus-4-8", "high"),
];

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

/// Render the recommendation table for agent-facing help.
/// Each line: `  N → model / effort`
pub fn recommendation_lines() -> String {
    DEFAULT_RECOMMENDATIONS
        .iter()
        .map(|(level, model, effort)| format!("   {level} → {model} / {effort}"))
        .collect::<Vec<_>>()
        .join("\n")
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
    fn recommendations_cover_1_through_5() {
        for i in 1..=5u8 {
            assert!(
                DEFAULT_RECOMMENDATIONS.iter().any(|(l, _, _)| *l == i),
                "recommendations missing level {i}"
            );
        }
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
        let text = recommendation_lines();
        for i in 1..=5 {
            assert!(text.contains(&format!("{i} →")), "missing level {i}");
        }
    }
}
