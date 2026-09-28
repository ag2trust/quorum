//! Single source of truth for the 1-5 complexity rubric.

/// (level, short label, description, reserved legacy field)
pub const RUBRIC: [(u8, &str, &str, &str); 5] = [
    (
        1,
        "Trivial",
        "mechanical change with no meaningful reasoning choice",
        "",
    ),
    (
        2,
        "Simple",
        "established pattern with one clear implementation path",
        "",
    ),
    (
        3,
        "Moderate",
        "bounded design choices across known behavior and invariants",
        "",
    ),
    (
        4,
        "Complex",
        "subtle interaction among multiple invariants or failure modes",
        "",
    ),
    (
        5,
        "Very complex",
        "new architectural boundary or subsystem with novel interface tradeoffs",
        "",
    ),
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
    fn rubric_level_4_is_about_interacting_correctness_constraints() {
        let (_, _, desc, _) = RUBRIC.iter().find(|(l, _, _, _)| *l == 4).unwrap();
        assert!(
            desc.contains("multiple invariants") && desc.contains("failure modes"),
            "level 4 must describe interacting correctness constraints, got: {desc}"
        );
    }

    #[test]
    fn rubric_level_5_is_architectural() {
        let (_, _, desc, _) = RUBRIC.iter().find(|(l, _, _, _)| *l == 5).unwrap();
        assert!(
            desc.contains("architectural boundary") && desc.contains("novel interface tradeoffs"),
            "level 5 must describe novel architectural reasoning, got: {desc}"
        );
    }

    #[test]
    fn complexity_rubric_does_not_use_execution_surface_proxies() {
        let text = rubric_lines();
        for proxy in ["single-file", "multi-file", "multiple components"] {
            assert!(
                !text.contains(proxy),
                "complexity rubric must not use size proxy {proxy}: {text}"
            );
        }
        for (_, surface_phrase) in crate::risk::RUBRIC {
            assert!(
                !text.contains(surface_phrase),
                "complexity rubric must not use risk surface phrase {surface_phrase}: {text}"
            );
        }

        let risk_text = crate::risk::rubric_lines();
        for reasoning_language in [
            "meaningful reasoning choice",
            "established pattern",
            "implementation path",
            "design choices",
            "multiple invariants",
            "failure modes",
            "architectural boundary",
            "novel interface tradeoffs",
        ] {
            assert!(
                !risk_text.contains(reasoning_language),
                "risk rubric must not use reasoning-difficulty language {reasoning_language}: {risk_text}"
            );
        }
    }

    #[test]
    fn rubric_lines_renders_all_levels() {
        let text = rubric_lines();
        for i in 1..=5 {
            assert!(text.contains(&format!("- {i}:")), "missing level {i}");
        }
    }
}
