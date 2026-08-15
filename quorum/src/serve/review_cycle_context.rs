//! Typed lifecycle state used to calibrate a re-review prompt.
//!
//! `rework_round` is persisted by the lifecycle and counts completed
//! changes-to-rework transitions. It is not a review ordinal: the initial
//! review happens before any such transition.

use quorum_core::lifecycle::REWORK_CAP;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewCycleContext {
    /// The raw persisted count, retained so unexpected database values remain
    /// visible rather than being silently normalized at the prompt boundary.
    pub rework_round: i64,
    /// The lifecycle-owned cap used to derive `final_opportunity`.
    pub rework_cap: u32,
    /// Whether this re-review is at or beyond the last permitted remediation
    /// transition.
    pub final_opportunity: bool,
}

impl ReviewCycleContext {
    pub fn from_persisted_rework_round(rework_round: i64) -> Self {
        let rework_cap = REWORK_CAP;
        Self {
            rework_round,
            rework_cap,
            final_opportunity: rework_round >= i64::from(rework_cap),
        }
    }

    /// Bounded wording for a re-review prompt. Deliberately describes
    /// transitions rather than numbering reviews, since the initial review is
    /// not a rework round.
    pub fn prompt_contract(self) -> String {
        if self.rework_round < 0 {
            return format!(
                "## Review-cycle context\n\nThe persisted rework count ({}) is invalid; this re-review is not marked as the final opportunity.\n",
                self.rework_round
            );
        }

        let final_opportunity = if self.final_opportunity {
            "This is the final review opportunity: another changes verdict cannot enter rework."
        } else {
            "This is not the final review opportunity."
        };
        format!(
            "## Review-cycle context\n\nThe task has {} completed changes-to-rework transitions (lifecycle cap {}). {final_opportunity}\n",
            self.rework_round, self.rework_cap
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_opportunity_uses_the_lifecycle_cap_at_all_boundaries() {
        for (round, final_opportunity) in [
            (0, false),
            (i64::from(REWORK_CAP) - 1, false),
            (i64::from(REWORK_CAP), true),
            (i64::from(REWORK_CAP) + 1, true),
            (-1, false),
        ] {
            let context = ReviewCycleContext::from_persisted_rework_round(round);
            assert_eq!(context.rework_round, round);
            assert_eq!(context.rework_cap, REWORK_CAP);
            assert_eq!(context.final_opportunity, final_opportunity);
        }
    }

    #[test]
    fn prompt_contract_describes_transitions_not_review_ordinals() {
        let initial = ReviewCycleContext::from_persisted_rework_round(0).prompt_contract();
        let final_round = ReviewCycleContext::from_persisted_rework_round(i64::from(REWORK_CAP))
            .prompt_contract();
        let invalid = ReviewCycleContext::from_persisted_rework_round(-1).prompt_contract();

        assert!(initial.contains("0 completed changes-to-rework transitions"));
        assert!(initial.contains("not the final"));
        assert!(final_round.contains("final review opportunity"));
        assert!(invalid.contains("invalid"));
        for prompt in [initial, final_round, invalid] {
            assert!(!prompt.contains("review round"));
        }
    }
}
