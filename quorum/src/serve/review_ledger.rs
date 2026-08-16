//! Bounded cross-round review-ledger contract.
//!
//! Quorum does not currently have a reliable structured extraction of GitHub
//! review threads. Re-review prompts therefore require the reviewer to publish
//! a standardized cumulative disposition section on the authoritative PR.

/// Maximum prior BLOCKING findings represented in the cumulative ledger.
pub const MAX_PRIOR_BLOCKING_ENTRIES: usize = 16;
/// Maximum newly discovered findings represented in the cumulative ledger.
pub const MAX_NEW_FINDING_ENTRIES: usize = 16;
/// Maximum Unicode scalar values in each reviewer-authored ledger field,
/// including the field-truncation suffix.
pub const MAX_LEDGER_FIELD_CHARS: usize = 500;

/// Require the reliable-extraction fallback for a re-review.
///
/// This is intentionally fixed prompt text apart from the PR number and
/// numeric limits. It provides navigation requirements, never synthesized
/// finding state.
pub fn required_cumulative_disposition_contract(pr: i64) -> String {
    format!(
        "## Required cumulative cross-round review ledger\n\n\
         No reliable structured review-thread ledger was supplied by the daemon for this turn. \
         Before submitting your verdict, read the relevant PR threads and publish the following \
         standardized cumulative disposition section in the review summary for the current SHA. \
         The PR discussion remains authoritative. This required ledger is bounded navigation \
         context, not a replacement for the relevant PR threads or your independent review. Do \
         not infer resolution from a pushed commit or the author's claimed remedy or response. \
         You own and must independently determine every current disposition.\n\n\
         `### Prior BLOCKING findings`\n\
         List prior BLOCKING findings in first-appearance PR chronology, oldest first, using at \
         most {MAX_PRIOR_BLOCKING_ENTRIES} entries. Each entry must contain exactly these fields:\n\
         - `Finding: <stable PR thread/comment reference and bounded description>`\n\
         - `Author remedy/response: <bounded claim or response; use \"none found\">`\n\
         - `Current reviewer disposition: <fixed | reaffirmed | downgraded/follow-up | \
         overridden/accepted | unresolved>`\n\
         - `Reviewer basis: <bounded independent verification or rationale>`\n\
         `fixed` requires your verification; an author claim or pushed commit alone is never \
         enough. Do not silently drop a prior blocker.\n\n\
         `### Newly discovered findings`\n\
         List findings first discovered in this review separately, in discovery order, using at \
         most {MAX_NEW_FINDING_ENTRIES} entries. Each entry must contain exactly these fields:\n\
         - `Finding: <stable PR thread/comment reference and bounded description>`\n\
         - `Merge disposition: <BLOCKING | FOLLOW-UP>`\n\
         - `Reviewer basis: <bounded evidence and rationale>`\n\
         - `Late-blocker explanation: <why a new BLOCKING finding in unchanged behavior was not \
         reasonably discoverable in the prior complete audit; otherwise \"not applicable\">`\n\n\
         Every field is limited to {MAX_LEDGER_FIELD_CHARS} Unicode scalar values, including any \
         truncation suffix. Truncate only at a scalar boundary and end a truncated field with \
         `... [field truncated; read PR #{pr} thread]`, keeping the whole field within the limit. \
         If either section has more entries than its limit, retain the first entries in the \
         ordering above and append exactly `TRUNCATED: additional ledger history omitted; read PR \
         #{pr} discussion for authoritative history.` The marker is not a ledger entry. Omitted \
         findings remain part of the authoritative PR discussion and, when currently BLOCKING, \
         still count in `--blocking`.\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_contract_is_bounded_and_pr_authoritative() {
        let contract = required_cumulative_disposition_contract(491);

        assert!(contract.contains("No reliable structured review-thread ledger was supplied"));
        assert!(contract.contains("standardized cumulative disposition section"));
        assert!(contract.contains("PR discussion remains authoritative"));
        assert!(contract.contains("bounded navigation context"));
        assert!(contract.contains("not a replacement for the relevant PR threads"));
        assert!(contract.contains("Do not infer resolution from a pushed commit"));
        assert!(
            contract.contains("You own and must independently determine every current disposition")
        );
        assert!(contract.contains(&format!("at most {MAX_PRIOR_BLOCKING_ENTRIES} entries")));
        assert!(contract.contains(&format!("at most {MAX_NEW_FINDING_ENTRIES} entries")));
        assert!(contract.contains(&format!(
            "limited to {MAX_LEDGER_FIELD_CHARS} Unicode scalar values"
        )));
        assert!(contract.contains(
            "TRUNCATED: additional ledger history omitted; read PR #491 discussion for authoritative history."
        ));
        assert!(contract.contains("[field truncated; read PR #491 thread]"));
    }

    #[test]
    fn fallback_contract_requires_all_ledger_fields_and_closed_dispositions() {
        let contract = required_cumulative_disposition_contract(77);

        for field in [
            "### Prior BLOCKING findings",
            "Finding:",
            "Author remedy/response:",
            "Current reviewer disposition:",
            "Reviewer basis:",
            "### Newly discovered findings",
            "Merge disposition:",
            "Late-blocker explanation:",
        ] {
            assert!(contract.contains(field), "missing ledger field: {field}");
        }
        for disposition in [
            "fixed",
            "reaffirmed",
            "downgraded/follow-up",
            "overridden/accepted",
            "unresolved",
        ] {
            assert!(
                contract.contains(disposition),
                "missing disposition: {disposition}"
            );
        }
        assert!(contract.contains("author claim or pushed commit alone is never enough"));
        assert!(contract.contains("new BLOCKING finding in unchanged behavior"));
        assert!(contract.contains("not reasonably discoverable in the prior complete audit"));
        assert!(contract.contains("still count in `--blocking`"));
    }
}
