//! Review-verdict discipline (#206).
//!
//! PR #198 merged over two blocking findings because the verdict was a free
//! string the reviewer chose separately from the review it wrote — nothing
//! bound the findings to the verdict. This module makes the contract
//! executable at two boundaries:
//!
//! - CLI ([`validate`]): `--verdict approved` requires an explicit
//!   `--blocking 0` attestation (the count of blocking findings in the
//!   review); any nonzero count is refused — a blocking finding REQUIRES
//!   `--verdict changes`.
//! - Daemon ([`gate`]): an `approved` mailbox row that lacks the attestation
//!   payload (written by any path other than the validated CLI) is demoted to
//!   `changes` instead of merged.

/// CLI-boundary validation of the verdict/blocking/feedback argument bundle.
///
/// `changes_requires_feedback` is true for `quorum submit` (daemon flow — the
/// feedback becomes the worker's rework turn) and false for
/// `quorum task-update` (passive flow — feedback travels via review comments
/// and task notes).
pub fn validate(
    verdict: Option<&str>,
    blocking: Option<u32>,
    feedback: Option<&str>,
    changes_requires_feedback: bool,
) -> Result<(), String> {
    match verdict {
        None => {
            if blocking.is_some() {
                return Err("--blocking is a review attestation and requires --verdict".to_string());
            }
            Ok(())
        }
        Some("approved") | Some("approve") => match blocking {
            Some(0) => Ok(()),
            Some(n) => Err(format!(
                "cannot approve with {n} blocking finding(s) — a blocking finding \
                 requires --verdict changes with the findings as feedback"
            )),
            None => Err(
                "an approve verdict requires the attestation --blocking 0 (the count of \
                 BLOCKING findings in your review; any blocking finding requires \
                 --verdict changes instead)"
                    .to_string(),
            ),
        },
        Some("changes") => {
            if blocking == Some(0) {
                return Err(
                    "a changes verdict with --blocking 0 contradicts the contract — zero \
                     blocking findings means --verdict approved --blocking 0; if something \
                     must be fixed before merge, count it as blocking"
                        .to_string(),
                );
            }
            if changes_requires_feedback && feedback.is_none_or(|f| f.trim().is_empty()) {
                return Err(
                    "--verdict changes requires --feedback-file (or compatibility \
                     --feedback) describing what must be fixed"
                        .to_string(),
                );
            }
            Ok(())
        }
        // Unknown spellings carry no attestation semantics here: the `done`
        // path enum-checks them before calling validate; the `task-update`
        // path defers spelling validation to quorum-core's verdict handling.
        Some(_) => Ok(()),
    }
}

/// Maximum UTF-8 byte length for a non-authoritative review-draft summary.
/// This is deliberately small continuation context, not a second findings ledger.
pub const MAX_REVIEW_DRAFT_FEEDBACK_BYTES: usize = 8 * 1024;

/// Validate the structurally separate `quorum review-draft` payload. A draft
/// records only a positive blocking count and a bounded continuation summary;
/// it does not carry a verdict and must not enter the authoritative gate.
pub fn validate_review_draft(blocking: u32, feedback: &str) -> Result<(), String> {
    if blocking == 0 {
        return Err("review-draft requires a positive --blocking count".to_string());
    }
    if feedback.trim().is_empty() {
        return Err("review-draft requires a non-empty --feedback-file summary".to_string());
    }
    if feedback.len() > MAX_REVIEW_DRAFT_FEEDBACK_BYTES {
        return Err(format!(
            "review-draft feedback exceeds the {} byte limit",
            MAX_REVIEW_DRAFT_FEEDBACK_BYTES
        ));
    }
    Ok(())
}

/// Serialized attestation for the mailbox `payload` column: `{"blocking":N}`.
/// `None` when no attestation was supplied.
pub fn attestation_payload(blocking: Option<u32>) -> Option<String> {
    blocking.map(|n| serde_json::json!({ "blocking": n }).to_string())
}

/// Outcome of gating a mailbox verdict at the daemon boundary.
#[derive(Debug)]
pub struct GatedVerdict {
    /// The verdict the daemon should act on (possibly demoted).
    pub verdict: Option<String>,
    /// Set when an `approved` verdict was demoted to `changes`; explains why
    /// and doubles as the rework feedback.
    pub demotion_reason: Option<String>,
    /// Attested blocking-finding count from the payload, if present and valid.
    pub blocking_count: Option<u32>,
}

/// Daemon-boundary gate: `approved` is only actionable with a payload
/// attesting zero blocking findings. Anything else that claims `approved`
/// (missing payload, unparseable payload, nonzero count) is demoted to
/// `changes` so the daemon never merges on it. `changes` and non-verdicts
/// pass through untouched.
pub fn gate(verdict: Option<&str>, payload: Option<&str>) -> GatedVerdict {
    let parsed_blocking = payload
        .and_then(|p| serde_json::from_str::<serde_json::Value>(p).ok())
        .and_then(|v| v.get("blocking").and_then(|b| b.as_u64()))
        .map(|n| n as u32);

    // Both spellings are approval claims ("approve" is the passive-flow
    // spelling); canonicalize so a raw mailbox row can't slip past the gate
    // into the daemon's "done without verdict" catch-all.
    let is_approval = matches!(verdict, Some("approved") | Some("approve"));
    if !is_approval {
        return GatedVerdict {
            verdict: verdict.map(str::to_string),
            demotion_reason: None,
            blocking_count: parsed_blocking,
        };
    }

    match parsed_blocking {
        Some(0) => GatedVerdict {
            verdict: Some("approved".to_string()),
            demotion_reason: None,
            blocking_count: Some(0),
        },
        Some(n) => GatedVerdict {
            verdict: Some("changes".to_string()),
            demotion_reason: Some(format!(
                "approved verdict demoted to changes: the reviewer attested {n} BLOCKING \
                 finding(s) — a blocking finding cannot be approved over (#206). \
                 Re-signal done to trigger a fresh review after addressing the findings."
            )),
            blocking_count: Some(n),
        },
        None => {
            // Covers both a missing payload and a malformed one (bad JSON or
            // no numeric `blocking` key) — name which so the log is accurate.
            let what = if payload.is_none() {
                "missing"
            } else {
                "invalid"
            };
            GatedVerdict {
                verdict: Some("changes".to_string()),
                demotion_reason: Some(format!(
                    "approved verdict demoted to changes: {what} zero-blocking-findings \
                     attestation (--blocking 0). The verdict did not come through the \
                     validated CLI path (#206). Re-signal done to trigger a fresh review."
                )),
                blocking_count: None,
            }
        }
    }
}

/// #228: what a restarted daemon must do with a durable approval record,
/// decided purely from persisted, instance-independent state (never the
/// in-memory reviewer roster). See [`dispose_approval`].
#[derive(Debug, PartialEq, Eq)]
pub enum ApprovalDisposition {
    /// Valid attested approval, reviewer ≠ author, and the approved head SHA
    /// still matches the PR's live head → merge, then close the task.
    Merge,
    /// The approval was for a superseded diff (head SHA moved) → return the
    /// task to review; do NOT merge a diff no reviewer approved.
    Demote(String),
    /// The record is not a trustworthy approval (attestation-less/forged, or a
    /// self-review) → refuse. Preserves crash-recovery safety on a true crash.
    Reject(String),
}

/// #228 recovery gate: decide what to do with a durable approval record on
/// restart, from persisted state alone. This is what replaces roster-scoped
/// verdict consumption for the merge path — a restarted instance with an empty
/// roster can still act on its own prior instance's approval, while a
/// genuinely foreign/forged/stale row is refused.
///
/// Trust is granted iff (all must hold):
/// - the record is a well-formed attested approval (#226 `{blocking:0}`
///   contract: `verdict == "approved"` AND `blocking_count == 0`), AND
/// - `reviewer != author` (no self-review), AND
/// - `approved_head_sha == current_head_sha` (the reviewer's approval binds to
///   the exact commit; a moved head auto-invalidates — subsumes #206 defect C).
pub fn dispose_approval(
    verdict: &str,
    blocking_count: i64,
    reviewer: &str,
    author: &str,
    approved_head_sha: &str,
    current_head_sha: &str,
) -> ApprovalDisposition {
    // 1. Attestation: only an approved verdict with zero attested blocking
    //    findings is an approval at all. Anything else is forged/garbage.
    if verdict != "approved" || blocking_count != 0 {
        return ApprovalDisposition::Reject(format!(
            "not an attested zero-blocking approval (verdict={verdict:?}, \
             blocking={blocking_count}) — refusing to merge on it (#228/#206)"
        ));
    }
    // 2. Self-review: a reviewer may never approve their own authored PR.
    if reviewer == author {
        return ApprovalDisposition::Reject(format!(
            "self-review refused: reviewer '{reviewer}' is the PR author (#228)"
        ));
    }
    // 3. SHA-binding: the approval is for a specific commit. If the PR head has
    //    moved since, the approved diff is superseded — return to review.
    if approved_head_sha != current_head_sha {
        return ApprovalDisposition::Demote(format!(
            "approved head {approved_head_sha} != current head {current_head_sha} \
             — diff superseded since approval; returning to review (#228)"
        ));
    }
    ApprovalDisposition::Merge
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- validate: the #198 regression contract ---

    #[test]
    fn approved_without_blocking_attestation_is_refused() {
        let err = validate(Some("approved"), None, None, true).unwrap_err();
        assert!(
            err.contains("--blocking 0"),
            "error must tell the reviewer how to attest: {err}"
        );
    }

    /// #198 regression: a review with blocking findings must not be able to
    /// signal `approved` — the exact sequence that shipped #205's bugs.
    #[test]
    fn approved_with_nonzero_blocking_findings_is_refused() {
        let err = validate(Some("approved"), Some(2), None, true).unwrap_err();
        assert!(
            err.contains("changes"),
            "error must redirect to --verdict changes: {err}"
        );
    }

    #[test]
    fn approved_with_zero_blocking_attestation_passes() {
        assert!(validate(Some("approved"), Some(0), None, true).is_ok());
    }

    #[test]
    fn approve_passive_spelling_gets_same_contract() {
        assert!(validate(Some("approve"), None, None, false).is_err());
        assert!(validate(Some("approve"), Some(0), None, false).is_ok());
    }

    /// Review #226 finding 3: attesting zero blocking findings while
    /// requesting changes contradicts the derived-verdict contract — zero
    /// blocking means approved. (`--blocking` omitted stays allowed: the
    /// passive flow doesn't attest on changes.)
    #[test]
    fn changes_with_zero_blocking_attestation_is_refused() {
        let err = validate(Some("changes"), Some(0), Some("nits only"), true).unwrap_err();
        assert!(
            err.contains("approved"),
            "error must redirect to --verdict approved: {err}"
        );
        assert!(validate(Some("changes"), None, Some("fix X"), true).is_ok());
    }

    #[test]
    fn changes_requires_feedback_in_daemon_flow() {
        let err = validate(Some("changes"), Some(1), None, true).unwrap_err();
        assert!(err.contains("--feedback"), "got: {err}");
        assert!(validate(
            Some("changes"),
            Some(1),
            Some("fix the LIKE escaping"),
            true
        )
        .is_ok());
    }

    #[test]
    fn changes_feedback_optional_in_passive_flow() {
        assert!(validate(Some("changes"), None, None, false).is_ok());
    }

    #[test]
    fn changes_with_blank_feedback_is_refused_in_daemon_flow() {
        assert!(validate(Some("changes"), None, Some("   "), true).is_err());
    }

    #[test]
    fn no_verdict_passes_without_attestation() {
        // Plain worker `done --pr N`.
        assert!(validate(None, None, None, true).is_ok());
    }

    #[test]
    fn blocking_without_verdict_is_refused() {
        assert!(validate(None, Some(0), None, true).is_err());
    }

    #[test]
    fn review_draft_requires_positive_bounded_nonempty_summary() {
        assert!(validate_review_draft(1, "Need a second pass on the error path.").is_ok());
        assert!(validate_review_draft(0, "Need a second pass.").is_err());
        assert!(validate_review_draft(1, " \n ").is_err());
        assert!(
            validate_review_draft(1, &"x".repeat(MAX_REVIEW_DRAFT_FEEDBACK_BYTES + 1)).is_err()
        );
    }

    // --- attestation payload ---

    #[test]
    fn attestation_payload_roundtrip() {
        assert_eq!(
            attestation_payload(Some(0)).as_deref(),
            Some("{\"blocking\":0}")
        );
        assert_eq!(
            attestation_payload(Some(3)).as_deref(),
            Some("{\"blocking\":3}")
        );
        assert_eq!(attestation_payload(None), None);
    }

    // --- gate: daemon-boundary demotion ---

    #[test]
    fn gate_passes_attested_approved() {
        let g = gate(Some("approved"), Some("{\"blocking\":0}"));
        assert_eq!(g.verdict.as_deref(), Some("approved"));
        assert!(g.demotion_reason.is_none());
        assert_eq!(g.blocking_count, Some(0));
    }

    /// Review #226 finding 1 (CONFIRMED): a raw mailbox row using the
    /// passive-flow spelling `approve` must not slip past the gate into the
    /// daemon's "done without verdict" catch-all — it is an approval claim
    /// and gets the same attestation treatment, canonicalized to `approved`.
    #[test]
    fn gate_treats_approve_spelling_as_approval_claim() {
        let g = gate(Some("approve"), None);
        assert_eq!(
            g.verdict.as_deref(),
            Some("changes"),
            "unattested 'approve' must demote, not fall through"
        );
        assert!(g.demotion_reason.is_some());

        let g = gate(Some("approve"), Some("{\"blocking\":0}"));
        assert_eq!(
            g.verdict.as_deref(),
            Some("approved"),
            "attested 'approve' must canonicalize to 'approved'"
        );
        assert!(g.demotion_reason.is_none());
    }

    #[test]
    fn gate_demotes_unattested_approved_to_changes() {
        let g = gate(Some("approved"), None);
        assert_eq!(g.verdict.as_deref(), Some("changes"));
        assert!(g.demotion_reason.is_some());
    }

    #[test]
    fn gate_demotes_approved_with_nonzero_blocking() {
        let g = gate(Some("approved"), Some("{\"blocking\":2}"));
        assert_eq!(g.verdict.as_deref(), Some("changes"));
        let reason = g.demotion_reason.unwrap();
        assert!(
            reason.contains('2'),
            "reason should cite the count: {reason}"
        );
    }

    #[test]
    fn gate_demotes_approved_with_garbage_payload() {
        let g = gate(Some("approved"), Some("not json"));
        assert_eq!(g.verdict.as_deref(), Some("changes"));
        let reason = g.demotion_reason.unwrap();
        assert!(
            reason.contains("invalid"),
            "a malformed payload must be reported as invalid, not missing: {reason}"
        );
    }

    #[test]
    fn gate_demotion_reason_says_missing_when_no_payload() {
        let g = gate(Some("approved"), None);
        let reason = g.demotion_reason.unwrap();
        assert!(
            reason.contains("missing"),
            "an absent payload must be reported as missing: {reason}"
        );
    }

    #[test]
    fn gate_passes_changes_through_untouched() {
        let g = gate(Some("changes"), None);
        assert_eq!(g.verdict.as_deref(), Some("changes"));
        assert!(g.demotion_reason.is_none());
        assert_eq!(g.blocking_count, None);
    }

    #[test]
    fn gate_changes_carries_blocking_count() {
        let g = gate(Some("changes"), Some("{\"blocking\":3}"));
        assert_eq!(g.verdict.as_deref(), Some("changes"));
        assert_eq!(g.blocking_count, Some(3));
    }

    #[test]
    fn gate_passes_non_verdict_through() {
        let g = gate(None, None);
        assert_eq!(g.verdict, None);
        assert!(g.demotion_reason.is_none());
        assert_eq!(g.blocking_count, None);
    }

    // --- dispose_approval: the #228 restart-recovery trust gate ---

    /// Acceptance #228: an attested approval by a reviewer (≠ author) whose
    /// approved head SHA matches the PR's current head → MERGE. Crucially the
    /// reviewer here need NOT be in any live roster — the decision is purely a
    /// function of the durable record + the PR's current head.
    #[test]
    fn dispose_merges_matching_attested_approval() {
        let d = dispose_approval(
            "approved",
            0,
            "Grommet-d14",
            "Bellows-d11",
            "2c0c833",
            "2c0c833",
        );
        assert_eq!(d, ApprovalDisposition::Merge);
    }

    /// Acceptance #228: a stale approval (PR head moved since approval) must
    /// NOT merge — it demotes back to review.
    #[test]
    fn dispose_demotes_stale_approval_when_head_moved() {
        let d = dispose_approval(
            "approved",
            0,
            "Grommet-d14",
            "Bellows-d11",
            "old_sha",
            "new_sha",
        );
        match d {
            ApprovalDisposition::Demote(reason) => assert!(reason.contains("superseded")),
            other => panic!("expected Demote, got {other:?}"),
        }
    }

    /// Acceptance #228: a self-review (reviewer == author) approval is refused
    /// even when perfectly attested and SHA-matched.
    #[test]
    fn dispose_rejects_self_review() {
        let d = dispose_approval("approved", 0, "Bellows-d11", "Bellows-d11", "abc", "abc");
        match d {
            ApprovalDisposition::Reject(reason) => assert!(reason.contains("self-review")),
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    /// Acceptance #228: crash-recovery safety — a forged/attestation-less row
    /// (verdict not approved, or nonzero blocking) is refused, so a true crash
    /// can never merge on a row that never passed the #226 gate.
    #[test]
    fn dispose_rejects_non_attested_verdict() {
        // Not an approval verdict at all.
        let d = dispose_approval("changes", 0, "R", "A", "abc", "abc");
        assert!(matches!(d, ApprovalDisposition::Reject(_)));
        // Approval spelling but nonzero blocking findings — forged attestation.
        let d = dispose_approval("approved", 2, "R", "A", "abc", "abc");
        assert!(matches!(d, ApprovalDisposition::Reject(_)));
    }

    /// A SHA mismatch is checked AFTER attestation/self-review, but a forged
    /// row with a mismatched head is still rejected (not merged), preserving
    /// safety regardless of ordering.
    #[test]
    fn dispose_rejects_forged_even_with_head_mismatch() {
        let d = dispose_approval("approve", 0, "R", "A", "old", "new");
        // "approve" (passive spelling) is NOT the canonical attested verdict
        // stored in the durable record — records always store "approved".
        assert!(matches!(d, ApprovalDisposition::Reject(_)));
    }
}
