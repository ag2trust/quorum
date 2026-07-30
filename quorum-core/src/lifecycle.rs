//! Task lifecycle state machine — pure transition table, no DB, no I/O.
//!
//! Single-task model: one row walks `open → working → in-review → merging → done`
//! with a rework loop (`in-review ⇄ rework`). Terminals: `done`, `failed`, `cancelled`.

use std::fmt;
use std::str::FromStr;

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    Open,
    Working,
    InReview,
    Rework,
    Merging,
    Done,
    Failed,
    Cancelled,
}

impl Status {
    pub fn is_terminal(self) -> bool {
        matches!(self, Status::Done | Status::Failed | Status::Cancelled)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Status::Open => "open",
            Status::Working => "working",
            Status::InReview => "in-review",
            Status::Rework => "rework",
            Status::Merging => "merging",
            Status::Done => "done",
            Status::Failed => "failed",
            Status::Cancelled => "cancelled",
        })
    }
}

impl FromStr for Status {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "open" => Ok(Status::Open),
            "working" => Ok(Status::Working),
            "in-review" => Ok(Status::InReview),
            "rework" => Ok(Status::Rework),
            "merging" => Ok(Status::Merging),
            "done" => Ok(Status::Done),
            "failed" => Ok(Status::Failed),
            "cancelled" => Ok(Status::Cancelled),
            other => Err(format!("unknown status: {other}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Event — the complete input vocabulary
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Claimed {
        agent: String,
    },
    SignaledDone {
        pr: String,
    },
    ReviewerAttached {
        agent: String,
    },
    VerdictApprove,
    VerdictChanges,
    ChecksFailed {
        checks: Vec<String>,
    },
    ReworkPushed,
    MergeSucceeded,
    MergeFailed {
        reason: String,
    },
    MergeConflict,
    PrFoundMerged,
    PrFoundClosed,
    LeaseExpired,
    AgentFailed {
        reason: String,
    },
    /// Daemon-controlled teardown. This records suspension without treating
    /// the managed run as failed or advancing the task lifecycle.
    ControlledShutdown,
    Cancelled {
        by: String,
    },
}

// ---------------------------------------------------------------------------
// Effect — declarative side-effects, executed by callers in later PRs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    SetAuthor { agent: String },
    SetReviewer { agent: String },
    SpawnReviewer,
    ResumeReviewer,
    ResumeWorker,
    MergePr { pr: String },
    IncrementReworkRound,
    NotifyOwner { reason: String },
    ReleaseLease,
    ClearAuthor,
    PostFindingsNote,
}

// ---------------------------------------------------------------------------
// TaskView — read-only snapshot the transition function needs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TaskView {
    pub status: Status,
    pub author: Option<String>,
    pub reviewer: Option<String>,
    pub rework_round: u32,
    pub pr: Option<String>,
    pub review_only: bool,
}

// ---------------------------------------------------------------------------
// InvalidTransition
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidTransition {
    pub status: Status,
    pub event: Event,
    pub reason: String,
}

impl fmt::Display for InvalidTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid transition: {:?} in status {} — {}",
            self.event, self.status, self.reason
        )
    }
}

impl std::error::Error for InvalidTransition {}

pub const REWORK_CAP: u32 = 5;

// ---------------------------------------------------------------------------
// transition — the exhaustive match
// ---------------------------------------------------------------------------

pub fn transition(t: &TaskView, e: &Event) -> Result<(Status, Vec<Effect>), InvalidTransition> {
    let reject = |reason: &str| -> Result<(Status, Vec<Effect>), InvalidTransition> {
        Err(InvalidTransition {
            status: t.status,
            event: e.clone(),
            reason: reason.to_string(),
        })
    };

    match (&t.status, e) {
        // ---- Open ----
        (Status::Open, Event::Claimed { agent }) => Ok((
            Status::Working,
            vec![Effect::SetAuthor {
                agent: agent.clone(),
            }],
        )),
        (Status::Open, Event::Cancelled { by }) => Ok((
            Status::Cancelled,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: format!("cancelled: {by}"),
                },
            ],
        )),
        (Status::Open, Event::LeaseExpired) => reject("no lease to expire in open"),
        (Status::Open, Event::SignaledDone { .. }) => reject("cannot signal done from open"),
        (Status::Open, Event::ReviewerAttached { .. }) => reject("no reviewer in open"),
        (Status::Open, Event::VerdictApprove) => reject("no verdict in open"),
        (Status::Open, Event::VerdictChanges) => reject("no verdict in open"),
        (Status::Open, Event::ChecksFailed { .. }) => reject("no PR checks in open"),
        (Status::Open, Event::ReworkPushed) => reject("no rework from open"),
        (Status::Open, Event::MergeSucceeded) => reject("no merge from open"),
        (Status::Open, Event::MergeFailed { .. }) => reject("no merge from open"),
        (Status::Open, Event::MergeConflict) => reject("no merge from open"),
        (Status::Open, Event::PrFoundMerged) => reject("no PR from open"),
        (Status::Open, Event::PrFoundClosed) => reject("no PR from open"),
        (Status::Open, Event::AgentFailed { .. }) => reject("no agent in open"),
        (Status::Open, Event::ControlledShutdown) => Ok((Status::Open, vec![])),

        // ---- Working ----
        (Status::Working, Event::SignaledDone { .. }) => {
            Ok((Status::InReview, vec![Effect::SpawnReviewer]))
        }
        (Status::Working, Event::AgentFailed { reason }) => {
            let mut effects = vec![Effect::ReleaseLease];
            // Preserve author when task has an open PR — the work survives the worker
            if t.pr.is_none() {
                effects.push(Effect::ClearAuthor);
            }
            effects.push(Effect::NotifyOwner {
                reason: reason.clone(),
            });
            Ok((Status::Open, effects))
        }
        (Status::Working, Event::LeaseExpired) => {
            let mut effects = vec![Effect::ReleaseLease];
            if t.pr.is_none() {
                effects.push(Effect::ClearAuthor);
            }
            Ok((Status::Open, effects))
        }
        (Status::Working, Event::Cancelled { by }) => Ok((
            Status::Cancelled,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: format!("cancelled: {by}"),
                },
            ],
        )),
        (Status::Working, Event::Claimed { .. }) => reject("already claimed"),
        (Status::Working, Event::ReviewerAttached { .. }) => reject("not in review"),
        (Status::Working, Event::VerdictApprove) => reject("not in review"),
        (Status::Working, Event::VerdictChanges) => reject("not in review"),
        (Status::Working, Event::ChecksFailed { .. }) => reject("not in review"),
        (Status::Working, Event::ReworkPushed) => reject("not in rework"),
        (Status::Working, Event::MergeSucceeded) => reject("not merging"),
        (Status::Working, Event::MergeFailed { .. }) => reject("not merging"),
        (Status::Working, Event::MergeConflict) => reject("not merging"),
        (Status::Working, Event::PrFoundMerged) => reject("not in review"),
        (Status::Working, Event::PrFoundClosed) => reject("not in review"),
        (Status::Working, Event::ControlledShutdown) => Ok((Status::Working, vec![])),

        // ---- InReview ----
        (Status::InReview, Event::ReviewerAttached { agent }) => {
            if t.author.as_deref() == Some(agent.as_str()) {
                return reject("reviewer must differ from author");
            }
            Ok((
                Status::InReview,
                vec![Effect::SetReviewer {
                    agent: agent.clone(),
                }],
            ))
        }
        (Status::InReview, Event::VerdictApprove) => Ok((
            Status::Merging,
            vec![Effect::MergePr {
                pr: t.pr.clone().unwrap_or_default(),
            }],
        )),
        (Status::InReview, Event::VerdictChanges | Event::ChecksFailed { .. }) => {
            if t.rework_round >= REWORK_CAP {
                return Ok((
                    Status::Failed,
                    vec![
                        Effect::NotifyOwner {
                            reason: format!("rework cap ({REWORK_CAP}) exceeded"),
                        },
                        Effect::ReleaseLease,
                    ],
                ));
            }
            Ok((
                Status::Rework,
                vec![
                    Effect::ReleaseLease,
                    Effect::IncrementReworkRound,
                    Effect::ResumeWorker,
                ],
            ))
        }
        (Status::InReview, Event::AgentFailed { reason }) => Ok((
            Status::InReview,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: reason.clone(),
                },
                Effect::SpawnReviewer,
            ],
        )),
        (Status::InReview, Event::LeaseExpired) => Ok((
            Status::InReview,
            vec![Effect::ReleaseLease, Effect::SpawnReviewer],
        )),
        (Status::InReview, Event::Cancelled { by }) => Ok((
            Status::Cancelled,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: format!("cancelled: {by}"),
                },
            ],
        )),
        (Status::InReview, Event::Claimed { .. }) => reject("in review, not claimable"),
        (Status::InReview, Event::SignaledDone { .. }) => reject("already in review"),
        (Status::InReview, Event::ReworkPushed) => reject("not in rework"),
        (Status::InReview, Event::MergeSucceeded) => reject("not merging"),
        (Status::InReview, Event::MergeFailed { .. }) => reject("not merging"),
        (Status::InReview, Event::MergeConflict) => reject("not merging"),
        (Status::InReview, Event::PrFoundMerged) => Ok((Status::Done, vec![Effect::ReleaseLease])),
        (Status::InReview, Event::PrFoundClosed) => Ok((
            Status::Failed,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "PR closed externally without merging".into(),
                },
            ],
        )),
        (Status::InReview, Event::ControlledShutdown) => Ok((Status::InReview, vec![])),

        // ---- Rework ----
        (Status::Rework, Event::ReworkPushed) => {
            Ok((Status::InReview, vec![Effect::ResumeReviewer]))
        }
        (Status::Rework, Event::AgentFailed { reason }) => {
            if t.review_only {
                // A lost remediation worker must not hand the task back to
                // review: the replacement reviewer re-judges the unchanged PR
                // head and its changes verdict burns a rework round with zero
                // remediation applied. Park instead (Failed + daemon_parked
                // refs, written by the storage layer); the daemon's head check
                // resumes straight to in-review when the worker did push, and
                // `task-retry` covers the rest.
                Ok((
                    Status::Failed,
                    vec![
                        Effect::ReleaseLease,
                        Effect::NotifyOwner {
                            reason: format!(
                                "remediation worker lost ({reason}); parked — \
                                 resume with `quorum task-retry`"
                            ),
                        },
                    ],
                ))
            } else {
                Ok((
                    Status::Open,
                    vec![
                        Effect::ReleaseLease,
                        Effect::NotifyOwner {
                            reason: reason.clone(),
                        },
                    ],
                ))
            }
        }
        (Status::Rework, Event::LeaseExpired) => {
            if t.review_only {
                // Same park-not-bounce contract as AgentFailed above.
                Ok((
                    Status::Failed,
                    vec![
                        Effect::ReleaseLease,
                        Effect::NotifyOwner {
                            reason: "remediation lease expired; parked — \
                                     resume with `quorum task-retry`"
                                .into(),
                        },
                    ],
                ))
            } else {
                Ok((Status::Open, vec![Effect::ReleaseLease]))
            }
        }
        (Status::Rework, Event::Cancelled { by }) => Ok((
            Status::Cancelled,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: format!("cancelled: {by}"),
                },
            ],
        )),
        (Status::Rework, Event::Claimed { .. }) => reject("in rework, not claimable"),
        (Status::Rework, Event::SignaledDone { .. }) => reject("must push rework, not signal done"),
        (Status::Rework, Event::ReviewerAttached { .. }) => reject("not in review"),
        (Status::Rework, Event::VerdictApprove) => reject("not in review"),
        (Status::Rework, Event::VerdictChanges) => reject("not in review"),
        (Status::Rework, Event::ChecksFailed { .. }) => reject("not in review"),
        (Status::Rework, Event::MergeSucceeded) => reject("not merging"),
        (Status::Rework, Event::MergeFailed { .. }) => reject("not merging"),
        (Status::Rework, Event::MergeConflict) => reject("not merging"),
        (Status::Rework, Event::PrFoundMerged) => reject("not in review"),
        (Status::Rework, Event::PrFoundClosed) => reject("not in review"),
        (Status::Rework, Event::ControlledShutdown) => Ok((Status::Rework, vec![])),

        // ---- Merging ----
        (Status::Merging, Event::MergeSucceeded) => Ok((Status::Done, vec![Effect::ReleaseLease])),
        (Status::Merging, Event::MergeFailed { reason }) => Ok((
            Status::InReview,
            vec![
                Effect::NotifyOwner {
                    reason: reason.clone(),
                },
                Effect::ResumeReviewer,
            ],
        )),
        (Status::Merging, Event::MergeConflict) => {
            if t.rework_round >= REWORK_CAP {
                return Ok((
                    Status::Failed,
                    vec![
                        Effect::NotifyOwner {
                            reason: format!("rework cap ({REWORK_CAP}) exceeded"),
                        },
                        Effect::ReleaseLease,
                    ],
                ));
            }
            Ok((
                Status::Rework,
                vec![
                    Effect::ReleaseLease,
                    Effect::IncrementReworkRound,
                    Effect::ResumeWorker,
                ],
            ))
        }
        (Status::Merging, Event::Cancelled { by }) => Ok((
            Status::Cancelled,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: format!("cancelled: {by}"),
                },
            ],
        )),
        (Status::Merging, Event::AgentFailed { reason }) => Ok((
            Status::InReview,
            vec![
                Effect::NotifyOwner {
                    reason: format!("agent failed during merge: {reason}"),
                },
                Effect::ResumeReviewer,
            ],
        )),
        (Status::Merging, Event::Claimed { .. }) => reject("merging in progress"),
        (Status::Merging, Event::SignaledDone { .. }) => reject("merging in progress"),
        (Status::Merging, Event::ReviewerAttached { .. }) => reject("merging in progress"),
        (Status::Merging, Event::VerdictApprove) => reject("merging in progress"),
        (Status::Merging, Event::VerdictChanges) => reject("merging in progress"),
        (Status::Merging, Event::ChecksFailed { .. }) => reject("merging in progress"),
        (Status::Merging, Event::ReworkPushed) => reject("merging in progress"),
        (Status::Merging, Event::PrFoundMerged) => reject("merging in progress"),
        (Status::Merging, Event::PrFoundClosed) => reject("merging in progress"),
        (Status::Merging, Event::LeaseExpired) => reject("merging in progress"),
        (Status::Merging, Event::ControlledShutdown) => Ok((Status::Merging, vec![])),

        // ---- Done (terminal) ----
        (Status::Done, Event::ControlledShutdown) => reject("task is done"),
        (Status::Done, _) => reject("task is done"),

        // ---- Failed (terminal) ----
        (Status::Failed, Event::ControlledShutdown) => reject("task has failed"),
        (Status::Failed, _) => reject("task has failed"),

        // ---- Cancelled (terminal) ----
        (Status::Cancelled, Event::ControlledShutdown) => reject("task is cancelled"),
        (Status::Cancelled, _) => reject("task is cancelled"),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn view(status: Status) -> TaskView {
        TaskView {
            status,
            author: None,
            reviewer: None,
            rework_round: 0,
            pr: None,
            review_only: false,
        }
    }

    fn view_with_author(status: Status, author: &str) -> TaskView {
        TaskView {
            status,
            author: Some(author.to_string()),
            reviewer: None,
            rework_round: 0,
            pr: Some("123".to_string()),
            review_only: false,
        }
    }

    fn assert_ok(t: &TaskView, e: &Event, expected_status: Status, expected_effects: &[Effect]) {
        let (next, effects) = transition(t, e)
            .unwrap_or_else(|err| panic!("expected Ok, got InvalidTransition: {err}"));
        assert_eq!(next, expected_status, "wrong next status for {e:?}");
        assert_eq!(effects, expected_effects, "wrong effects for {e:?}");
    }

    fn assert_invalid(t: &TaskView, e: &Event) {
        assert!(
            transition(t, e).is_err(),
            "expected InvalidTransition for {:?} in {:?}",
            e,
            t.status,
        );
    }

    // -----------------------------------------------------------------------
    // Status: Display + FromStr round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn status_display_roundtrip() {
        let all = [
            Status::Open,
            Status::Working,
            Status::InReview,
            Status::Rework,
            Status::Merging,
            Status::Done,
            Status::Failed,
            Status::Cancelled,
        ];
        for s in all {
            let text = s.to_string();
            let parsed: Status = text.parse().unwrap();
            assert_eq!(parsed, s);
        }
    }

    #[test]
    fn status_parse_unknown() {
        assert!("bogus".parse::<Status>().is_err());
    }

    #[test]
    fn status_is_terminal() {
        assert!(!Status::Open.is_terminal());
        assert!(!Status::Working.is_terminal());
        assert!(!Status::InReview.is_terminal());
        assert!(!Status::Rework.is_terminal());
        assert!(!Status::Merging.is_terminal());
        assert!(Status::Done.is_terminal());
        assert!(Status::Failed.is_terminal());
        assert!(Status::Cancelled.is_terminal());
    }

    #[test]
    fn controlled_shutdown_preserves_every_nonterminal_status_and_rejects_terminals() {
        // This match intentionally has no wildcard: adding a Status requires
        // declaring its controlled-shutdown contract here before tests compile.
        let expected = |status| match status {
            Status::Open => Ok(Status::Open),
            Status::Working => Ok(Status::Working),
            Status::InReview => Ok(Status::InReview),
            Status::Rework => Ok(Status::Rework),
            Status::Merging => Ok(Status::Merging),
            Status::Done => Err(()),
            Status::Failed => Err(()),
            Status::Cancelled => Err(()),
        };
        let all_statuses = [
            Status::Open,
            Status::Working,
            Status::InReview,
            Status::Rework,
            Status::Merging,
            Status::Done,
            Status::Failed,
            Status::Cancelled,
        ];

        for status in all_statuses {
            match expected(status) {
                Ok(next_status) => {
                    let (next, effects) = transition(&view(status), &Event::ControlledShutdown)
                        .expect("controlled shutdown must be accepted for non-terminal status");
                    assert_eq!(next, next_status);
                    assert!(
                        effects.is_empty(),
                        "controlled shutdown must have no effects"
                    );
                }
                Err(()) => assert_invalid(&view(status), &Event::ControlledShutdown),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Open
    // -----------------------------------------------------------------------

    #[test]
    fn open_claimed() {
        let t = view(Status::Open);
        assert_ok(
            &t,
            &Event::Claimed { agent: "W1".into() },
            Status::Working,
            &[Effect::SetAuthor { agent: "W1".into() }],
        );
    }

    #[test]
    fn open_cancelled() {
        let t = view(Status::Open);
        assert_ok(
            &t,
            &Event::Cancelled { by: "boss".into() },
            Status::Cancelled,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "cancelled: boss".into(),
                },
            ],
        );
    }

    #[test]
    fn open_rejects_all_others() {
        let t = view(Status::Open);
        let invalid_events = [
            Event::LeaseExpired,
            Event::SignaledDone { pr: "1".into() },
            Event::ReviewerAttached { agent: "R1".into() },
            Event::VerdictApprove,
            Event::VerdictChanges,
            Event::ChecksFailed {
                checks: vec!["test".into()],
            },
            Event::ReworkPushed,
            Event::MergeSucceeded,
            Event::MergeFailed { reason: "x".into() },
            Event::MergeConflict,
            Event::PrFoundMerged,
            Event::PrFoundClosed,
            Event::AgentFailed { reason: "x".into() },
        ];
        for e in &invalid_events {
            assert_invalid(&t, e);
        }
    }

    // -----------------------------------------------------------------------
    // Working
    // -----------------------------------------------------------------------

    #[test]
    fn working_signaled_done() {
        let mut t = view(Status::Working);
        t.author = Some("W1".into());
        assert_ok(
            &t,
            &Event::SignaledDone { pr: "42".into() },
            Status::InReview,
            &[Effect::SpawnReviewer],
        );
    }

    #[test]
    fn working_agent_failed() {
        let t = view(Status::Working);
        assert_ok(
            &t,
            &Event::AgentFailed {
                reason: "oom".into(),
            },
            Status::Open,
            &[
                Effect::ReleaseLease,
                Effect::ClearAuthor,
                Effect::NotifyOwner {
                    reason: "oom".into(),
                },
            ],
        );
    }

    #[test]
    fn working_lease_expired() {
        let t = view(Status::Working);
        assert_ok(
            &t,
            &Event::LeaseExpired,
            Status::Open,
            &[Effect::ReleaseLease, Effect::ClearAuthor],
        );
    }

    #[test]
    fn working_agent_failed_with_pr_preserves_author() {
        let mut t = view(Status::Working);
        t.pr = Some("42".into());
        t.author = Some("W1".into());
        assert_ok(
            &t,
            &Event::AgentFailed {
                reason: "idle".into(),
            },
            Status::Open,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "idle".into(),
                },
            ],
        );
    }

    #[test]
    fn working_lease_expired_with_pr_preserves_author() {
        let mut t = view(Status::Working);
        t.pr = Some("42".into());
        t.author = Some("W1".into());
        assert_ok(
            &t,
            &Event::LeaseExpired,
            Status::Open,
            &[Effect::ReleaseLease],
        );
    }

    #[test]
    fn working_cancelled() {
        let t = view(Status::Working);
        assert_ok(
            &t,
            &Event::Cancelled { by: "boss".into() },
            Status::Cancelled,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "cancelled: boss".into(),
                },
            ],
        );
    }

    #[test]
    fn working_rejects_all_others() {
        let t = view(Status::Working);
        let invalid_events = [
            Event::Claimed { agent: "W2".into() },
            Event::ReviewerAttached { agent: "R1".into() },
            Event::VerdictApprove,
            Event::VerdictChanges,
            Event::ChecksFailed {
                checks: vec!["test".into()],
            },
            Event::ReworkPushed,
            Event::MergeSucceeded,
            Event::MergeFailed { reason: "x".into() },
            Event::MergeConflict,
            Event::PrFoundMerged,
            Event::PrFoundClosed,
        ];
        for e in &invalid_events {
            assert_invalid(&t, e);
        }
    }

    // -----------------------------------------------------------------------
    // InReview
    // -----------------------------------------------------------------------

    #[test]
    fn in_review_reviewer_attached() {
        let t = view_with_author(Status::InReview, "W1");
        assert_ok(
            &t,
            &Event::ReviewerAttached { agent: "R1".into() },
            Status::InReview,
            &[Effect::SetReviewer { agent: "R1".into() }],
        );
    }

    #[test]
    fn in_review_reviewer_equals_author_rejected() {
        let t = view_with_author(Status::InReview, "W1");
        assert_invalid(&t, &Event::ReviewerAttached { agent: "W1".into() });
    }

    #[test]
    fn in_review_verdict_approve() {
        let mut t = view_with_author(Status::InReview, "W1");
        t.pr = Some("42".into());
        assert_ok(
            &t,
            &Event::VerdictApprove,
            Status::Merging,
            &[Effect::MergePr { pr: "42".into() }],
        );
    }

    #[test]
    fn in_review_verdict_changes_normal() {
        let t = view_with_author(Status::InReview, "W1");
        assert_ok(
            &t,
            &Event::VerdictChanges,
            Status::Rework,
            &[
                Effect::ReleaseLease,
                Effect::IncrementReworkRound,
                Effect::ResumeWorker,
            ],
        );
    }

    #[test]
    fn in_review_checks_failed_uses_rework_path_without_reviewer() {
        let t = view_with_author(Status::InReview, "W1");
        assert_ok(
            &t,
            &Event::ChecksFailed {
                checks: vec!["fmt".into()],
            },
            Status::Rework,
            &[
                Effect::ReleaseLease,
                Effect::IncrementReworkRound,
                Effect::ResumeWorker,
            ],
        );
    }

    #[test]
    fn in_review_verdict_changes_review_only_reworks() {
        // #159: review_only tasks now go through rework (remediation workers).
        let mut t = view_with_author(Status::InReview, "W1");
        t.review_only = true;
        assert_ok(
            &t,
            &Event::VerdictChanges,
            Status::Rework,
            &[
                Effect::ReleaseLease,
                Effect::IncrementReworkRound,
                Effect::ResumeWorker,
            ],
        );
    }

    #[test]
    fn in_review_verdict_changes_rework_cap_exceeded() {
        let mut t = view_with_author(Status::InReview, "W1");
        t.rework_round = REWORK_CAP;
        assert_ok(
            &t,
            &Event::VerdictChanges,
            Status::Failed,
            &[
                Effect::NotifyOwner {
                    reason: format!("rework cap ({REWORK_CAP}) exceeded"),
                },
                Effect::ReleaseLease,
            ],
        );
    }

    #[test]
    fn in_review_agent_failed() {
        let t = view_with_author(Status::InReview, "W1");
        assert_ok(
            &t,
            &Event::AgentFailed {
                reason: "crash".into(),
            },
            Status::InReview,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "crash".into(),
                },
                Effect::SpawnReviewer,
            ],
        );
    }

    #[test]
    fn in_review_lease_expired() {
        let t = view_with_author(Status::InReview, "W1");
        assert_ok(
            &t,
            &Event::LeaseExpired,
            Status::InReview,
            &[Effect::ReleaseLease, Effect::SpawnReviewer],
        );
    }

    #[test]
    fn in_review_cancelled() {
        let t = view_with_author(Status::InReview, "W1");
        assert_ok(
            &t,
            &Event::Cancelled { by: "boss".into() },
            Status::Cancelled,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "cancelled: boss".into(),
                },
            ],
        );
    }

    #[test]
    fn in_review_pr_found_merged() {
        let t = view_with_author(Status::InReview, "W1");
        assert_ok(
            &t,
            &Event::PrFoundMerged,
            Status::Done,
            &[Effect::ReleaseLease],
        );
    }

    #[test]
    fn in_review_pr_found_closed() {
        let t = view_with_author(Status::InReview, "W1");
        assert_ok(
            &t,
            &Event::PrFoundClosed,
            Status::Failed,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "PR closed externally without merging".into(),
                },
            ],
        );
    }

    #[test]
    fn in_review_rejects_all_others() {
        let t = view_with_author(Status::InReview, "W1");
        let invalid_events = [
            Event::Claimed { agent: "W2".into() },
            Event::SignaledDone { pr: "1".into() },
            Event::ReworkPushed,
            Event::MergeSucceeded,
            Event::MergeFailed { reason: "x".into() },
            Event::MergeConflict,
        ];
        for e in &invalid_events {
            assert_invalid(&t, e);
        }
    }

    // -----------------------------------------------------------------------
    // Rework
    // -----------------------------------------------------------------------

    #[test]
    fn rework_pushed() {
        let t = view(Status::Rework);
        assert_ok(
            &t,
            &Event::ReworkPushed,
            Status::InReview,
            &[Effect::ResumeReviewer],
        );
    }

    #[test]
    fn rework_agent_failed() {
        let t = view(Status::Rework);
        assert_ok(
            &t,
            &Event::AgentFailed {
                reason: "crash".into(),
            },
            Status::Open,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "crash".into(),
                },
            ],
        );
    }

    #[test]
    fn rework_lease_expired() {
        let t = view(Status::Rework);
        assert_ok(
            &t,
            &Event::LeaseExpired,
            Status::Open,
            &[Effect::ReleaseLease],
        );
    }

    // ── Review-only rework recovery (table-driven) ─────────────────
    // Implementation tasks recover to Open (worker requeue). review_only
    // tasks park (Failed + daemon_parked refs): bouncing to InReview would
    // re-review the unchanged PR head and burn a rework round per bounce.

    #[test]
    fn rework_recovery_destinations_by_review_only() {
        struct Case {
            review_only: bool,
            event: Event,
            expected_status: Status,
            label: &'static str,
        }
        let cases = [
            Case {
                review_only: false,
                event: Event::AgentFailed { reason: "x".into() },
                expected_status: Status::Open,
                label: "impl+AgentFailed→Open",
            },
            Case {
                review_only: true,
                event: Event::AgentFailed { reason: "x".into() },
                expected_status: Status::Failed,
                label: "review_only+AgentFailed→Failed(park)",
            },
            Case {
                review_only: false,
                event: Event::LeaseExpired,
                expected_status: Status::Open,
                label: "impl+LeaseExpired→Open",
            },
            Case {
                review_only: true,
                event: Event::LeaseExpired,
                expected_status: Status::Failed,
                label: "review_only+LeaseExpired→Failed(park)",
            },
        ];
        for case in &cases {
            let mut t = view(Status::Rework);
            t.review_only = case.review_only;
            t.pr = Some("42".into());
            let (next, _) =
                transition(&t, &case.event).unwrap_or_else(|e| panic!("{}: {e}", case.label));
            assert_eq!(next, case.expected_status, "{}", case.label);
        }
    }

    #[test]
    fn rework_agent_failed_review_only_parks_without_reviewer() {
        let mut t = view(Status::Rework);
        t.review_only = true;
        t.pr = Some("42".into());
        t.author = Some("W1".into());
        let (status, effects) = transition(
            &t,
            &Event::AgentFailed {
                reason: "crash".into(),
            },
        )
        .unwrap();
        // Park, never bounce: a replacement reviewer on the unchanged head
        // would burn a rework round with zero remediation applied.
        assert_eq!(status, Status::Failed);
        assert!(effects.contains(&Effect::ReleaseLease));
        assert!(
            !effects.contains(&Effect::SpawnReviewer),
            "remediation death must not spawn a reviewer"
        );
        assert!(
            !effects.contains(&Effect::IncrementReworkRound),
            "infra failure must not consume a rework round"
        );
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { reason } if reason.contains("crash"))));
    }

    #[test]
    fn rework_lease_expired_review_only_parks_without_reviewer() {
        let mut t = view(Status::Rework);
        t.review_only = true;
        t.pr = Some("42".into());
        let (status, effects) = transition(&t, &Event::LeaseExpired).unwrap();
        assert_eq!(status, Status::Failed);
        assert!(effects.contains(&Effect::ReleaseLease));
        assert!(!effects.contains(&Effect::SpawnReviewer));
        assert!(!effects.contains(&Effect::IncrementReworkRound));
    }

    #[test]
    fn rework_cancelled() {
        let t = view(Status::Rework);
        assert_ok(
            &t,
            &Event::Cancelled { by: "boss".into() },
            Status::Cancelled,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "cancelled: boss".into(),
                },
            ],
        );
    }

    #[test]
    fn rework_rejects_all_others() {
        let t = view(Status::Rework);
        let invalid_events = [
            Event::Claimed { agent: "W2".into() },
            Event::SignaledDone { pr: "1".into() },
            Event::ReviewerAttached { agent: "R1".into() },
            Event::VerdictApprove,
            Event::VerdictChanges,
            Event::MergeSucceeded,
            Event::MergeFailed { reason: "x".into() },
            Event::MergeConflict,
            Event::PrFoundMerged,
            Event::PrFoundClosed,
        ];
        for e in &invalid_events {
            assert_invalid(&t, e);
        }
    }

    // -----------------------------------------------------------------------
    // Merging
    // -----------------------------------------------------------------------

    #[test]
    fn merging_succeeded() {
        let t = view(Status::Merging);
        assert_ok(
            &t,
            &Event::MergeSucceeded,
            Status::Done,
            &[Effect::ReleaseLease],
        );
    }

    #[test]
    fn merging_failed() {
        let t = view(Status::Merging);
        assert_ok(
            &t,
            &Event::MergeFailed {
                reason: "conflict".into(),
            },
            Status::InReview,
            &[
                Effect::NotifyOwner {
                    reason: "conflict".into(),
                },
                Effect::ResumeReviewer,
            ],
        );
    }

    #[test]
    fn merging_cancelled() {
        let t = view(Status::Merging);
        assert_ok(
            &t,
            &Event::Cancelled { by: "boss".into() },
            Status::Cancelled,
            &[
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: "cancelled: boss".into(),
                },
            ],
        );
    }

    #[test]
    fn merging_agent_failed() {
        let t = view(Status::Merging);
        assert_ok(
            &t,
            &Event::AgentFailed {
                reason: "worker teardown".into(),
            },
            Status::InReview,
            &[
                Effect::NotifyOwner {
                    reason: "agent failed during merge: worker teardown".into(),
                },
                Effect::ResumeReviewer,
            ],
        );
    }

    #[test]
    fn merging_conflict_below_cap() {
        let t = view(Status::Merging);
        assert_ok(
            &t,
            &Event::MergeConflict,
            Status::Rework,
            &[
                Effect::ReleaseLease,
                Effect::IncrementReworkRound,
                Effect::ResumeWorker,
            ],
        );
    }

    #[test]
    fn merging_conflict_at_cap() {
        let mut t = view(Status::Merging);
        t.rework_round = REWORK_CAP;
        assert_ok(
            &t,
            &Event::MergeConflict,
            Status::Failed,
            &[
                Effect::NotifyOwner {
                    reason: format!("rework cap ({REWORK_CAP}) exceeded"),
                },
                Effect::ReleaseLease,
            ],
        );
    }

    #[test]
    fn merging_rejects_all_others() {
        let t = view(Status::Merging);
        let invalid_events = [
            Event::Claimed { agent: "W2".into() },
            Event::SignaledDone { pr: "1".into() },
            Event::ReviewerAttached { agent: "R1".into() },
            Event::VerdictApprove,
            Event::VerdictChanges,
            Event::ReworkPushed,
            Event::PrFoundMerged,
            Event::PrFoundClosed,
            Event::LeaseExpired,
        ];
        for e in &invalid_events {
            assert_invalid(&t, e);
        }
    }

    // -----------------------------------------------------------------------
    // Terminals: Done, Failed, Cancelled reject everything
    // -----------------------------------------------------------------------

    #[test]
    fn done_rejects_everything() {
        let t = view(Status::Done);
        let all_events = all_sample_events();
        for e in &all_events {
            assert_invalid(&t, e);
        }
    }

    #[test]
    fn failed_rejects_everything() {
        let t = view(Status::Failed);
        let all_events = all_sample_events();
        for e in &all_events {
            assert_invalid(&t, e);
        }
    }

    #[test]
    fn cancelled_rejects_everything() {
        let t = view(Status::Cancelled);
        let all_events = all_sample_events();
        for e in &all_events {
            assert_invalid(&t, e);
        }
    }

    // -----------------------------------------------------------------------
    // End-to-end walk: open → working → in-review → rework → in-review → merging → done
    // -----------------------------------------------------------------------

    #[test]
    fn full_lifecycle_walk() {
        let mut t = view(Status::Open);

        // open → working
        let (next, effects) = transition(&t, &Event::Claimed { agent: "W1".into() }).unwrap();
        assert_eq!(next, Status::Working);
        assert_eq!(effects, vec![Effect::SetAuthor { agent: "W1".into() }]);
        t.status = next;
        t.author = Some("W1".into());

        // working → in-review
        let (next, effects) = transition(&t, &Event::SignaledDone { pr: "42".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::SpawnReviewer]);
        t.status = next;
        t.pr = Some("42".into());

        // reviewer attaches
        let (next, effects) =
            transition(&t, &Event::ReviewerAttached { agent: "R1".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::SetReviewer { agent: "R1".into() }]);
        t.reviewer = Some("R1".into());

        // changes → rework
        let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Rework);
        assert_eq!(
            effects,
            vec![
                Effect::ReleaseLease,
                Effect::IncrementReworkRound,
                Effect::ResumeWorker,
            ]
        );
        t.status = next;
        t.rework_round += 1;

        // rework pushed → back to in-review
        let (next, effects) = transition(&t, &Event::ReworkPushed).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::ResumeReviewer]);
        t.status = next;

        // approve → merging
        let (next, effects) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        assert_eq!(effects, vec![Effect::MergePr { pr: "42".into() }]);
        t.status = next;

        // merge succeeded → done
        let (next, effects) = transition(&t, &Event::MergeSucceeded).unwrap();
        assert_eq!(next, Status::Done);
        assert_eq!(effects, vec![Effect::ReleaseLease]);
        assert!(next.is_terminal());
    }

    // -----------------------------------------------------------------------
    // Guard: rework round exactly at cap
    // -----------------------------------------------------------------------

    #[test]
    fn rework_round_at_cap_minus_one_allowed() {
        let mut t = view_with_author(Status::InReview, "W1");
        t.rework_round = REWORK_CAP - 1;
        let (next, _) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Rework);
    }

    #[test]
    fn rework_round_at_cap_fails() {
        let mut t = view_with_author(Status::InReview, "W1");
        t.rework_round = REWORK_CAP;
        let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Failed);
        assert!(effects.contains(&Effect::ReleaseLease));
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
    }

    #[test]
    fn rework_round_above_cap_also_fails() {
        let mut t = view_with_author(Status::InReview, "W1");
        t.rework_round = REWORK_CAP + 5;
        let (next, _) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Failed);
    }

    // -----------------------------------------------------------------------
    // Guard: review_only + changes → failed (not rework)
    // -----------------------------------------------------------------------

    #[test]
    fn review_only_changes_at_zero_rounds_reworks() {
        // #159: review_only + changes → rework (remediation workers).
        let mut t = view_with_author(Status::InReview, "W1");
        t.review_only = true;
        t.rework_round = 0;
        let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Rework);
        assert!(effects.contains(&Effect::IncrementReworkRound));
        assert!(effects.contains(&Effect::ResumeWorker));
    }

    // -----------------------------------------------------------------------
    // Guard: review-only merge failure stays in-review (not failed)
    // -----------------------------------------------------------------------

    #[test]
    fn review_only_merge_failed_stays_in_review() {
        let mut t = view_with_author(Status::Merging, "W1");
        t.review_only = true;
        let (next, effects) = transition(
            &t,
            &Event::MergeFailed {
                reason: "conflict".into(),
            },
        )
        .unwrap();
        assert_eq!(next, Status::InReview);
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
        assert!(effects.contains(&Effect::ResumeReviewer));
        assert!(!effects.contains(&Effect::ReleaseLease));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn all_sample_events() -> Vec<Event> {
        vec![
            Event::Claimed { agent: "W1".into() },
            Event::SignaledDone { pr: "1".into() },
            Event::ReviewerAttached { agent: "R1".into() },
            Event::VerdictApprove,
            Event::VerdictChanges,
            Event::ReworkPushed,
            Event::MergeSucceeded,
            Event::MergeFailed { reason: "x".into() },
            Event::MergeConflict,
            Event::PrFoundMerged,
            Event::PrFoundClosed,
            Event::LeaseExpired,
            Event::AgentFailed { reason: "x".into() },
            Event::Cancelled { by: "boss".into() },
        ]
    }

    // ===================================================================
    // Walk tests — multi-step scenario traces
    // ===================================================================

    // MergeFailed → re-review → approve → done
    #[test]
    fn walk_merge_failed_re_review_done() {
        let mut t = view(Status::Open);

        // open → working
        let (next, _) = transition(&t, &Event::Claimed { agent: "W1".into() }).unwrap();
        t.status = next;
        t.author = Some("W1".into());

        // working → in-review
        let (next, _) = transition(&t, &Event::SignaledDone { pr: "99".into() }).unwrap();
        t.status = next;
        t.pr = Some("99".into());

        // reviewer attaches
        let (next, _) = transition(&t, &Event::ReviewerAttached { agent: "R1".into() }).unwrap();
        t.status = next;
        t.reviewer = Some("R1".into());

        // approve → merging
        let (next, effects) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        assert!(effects.contains(&Effect::MergePr { pr: "99".into() }));
        t.status = next;

        // merge fails → back to in-review
        let (next, effects) = transition(
            &t,
            &Event::MergeFailed {
                reason: "conflict".into(),
            },
        )
        .unwrap();
        assert_eq!(next, Status::InReview);
        assert!(effects.contains(&Effect::ResumeReviewer));
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
        t.status = next;

        // re-approve → merging again
        let (next, _) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        t.status = next;

        // merge succeeds → done
        let (next, _) = transition(&t, &Event::MergeSucceeded).unwrap();
        assert_eq!(next, Status::Done);
        assert!(next.is_terminal());
    }

    // Rework → Open (lease expired) → re-claim preserves PR/branch
    #[test]
    fn walk_rework_open_reclaim_preserves_pr() {
        let mut t = view(Status::Open);

        // open → working → in-review
        let (next, _) = transition(&t, &Event::Claimed { agent: "W1".into() }).unwrap();
        t.status = next;
        t.author = Some("W1".into());

        let (next, _) = transition(&t, &Event::SignaledDone { pr: "55".into() }).unwrap();
        t.status = next;
        t.pr = Some("55".into());

        // reviewer + changes → rework
        let (next, _) = transition(&t, &Event::ReviewerAttached { agent: "R1".into() }).unwrap();
        t.status = next;
        t.reviewer = Some("R1".into());

        let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Rework);
        assert!(effects.contains(&Effect::IncrementReworkRound));
        t.status = next;
        t.rework_round += 1;

        // rework agent's lease expires → back to Open
        let (next, effects) = transition(&t, &Event::LeaseExpired).unwrap();
        assert_eq!(next, Status::Open);
        assert!(effects.contains(&Effect::ReleaseLease));
        t.status = next;

        // PR and rework_round survive the Open transition (TaskView state persists)
        assert_eq!(t.pr.as_deref(), Some("55"));
        assert_eq!(t.rework_round, 1);

        // re-claim from Open
        let (next, effects) = transition(&t, &Event::Claimed { agent: "W2".into() }).unwrap();
        assert_eq!(next, Status::Working);
        assert_eq!(effects, vec![Effect::SetAuthor { agent: "W2".into() }]);
        t.status = next;
        t.author = Some("W2".into());

        // the PR is still there
        assert_eq!(t.pr.as_deref(), Some("55"));
        assert_eq!(t.rework_round, 1);
    }

    // close_after_merge from InReview (AgentFailed during merge → InReview,
    // then approve again → merge succeeds)
    #[test]
    fn walk_close_after_merge_from_in_review() {
        let mut t = view(Status::Open);
        t.status = Status::InReview;
        t.author = Some("W1".into());
        t.reviewer = Some("R1".into());
        t.pr = Some("77".into());

        // approve → merging
        let (next, _) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        t.status = next;

        // agent fails during merge → back to in-review
        let (next, effects) = transition(
            &t,
            &Event::AgentFailed {
                reason: "timeout".into(),
            },
        )
        .unwrap();
        assert_eq!(next, Status::InReview);
        assert!(effects.contains(&Effect::ResumeReviewer));
        t.status = next;

        // re-approve → merging
        let (next, _) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        t.status = next;

        // merge succeeds
        let (next, _) = transition(&t, &Event::MergeSucceeded).unwrap();
        assert_eq!(next, Status::Done);
    }

    // close_after_merge from Merging (direct path)
    #[test]
    fn walk_close_after_merge_from_merging() {
        let mut t = view(Status::Merging);
        t.pr = Some("88".into());

        let (next, effects) = transition(&t, &Event::MergeSucceeded).unwrap();
        assert_eq!(next, Status::Done);
        assert!(effects.contains(&Effect::ReleaseLease));
        assert!(next.is_terminal());
    }

    // Reviewer replacement after expiry in InReview (regression coverage for H1)
    #[test]
    fn walk_reviewer_replacement_after_expiry() {
        let mut t = view(Status::Open);
        t.status = Status::InReview;
        t.author = Some("W1".into());
        t.pr = Some("42".into());

        // first reviewer attaches
        let (next, effects) =
            transition(&t, &Event::ReviewerAttached { agent: "R1".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::SetReviewer { agent: "R1".into() }]);
        t.status = next;
        t.reviewer = Some("R1".into());

        // reviewer's lease expires → stays InReview, spawns new reviewer
        let (next, effects) = transition(&t, &Event::LeaseExpired).unwrap();
        assert_eq!(next, Status::InReview, "InReview must be sticky on expiry");
        assert!(effects.contains(&Effect::ReleaseLease));
        assert!(effects.contains(&Effect::SpawnReviewer));
        t.status = next;

        // new reviewer attaches (replacing the old one)
        let (next, effects) =
            transition(&t, &Event::ReviewerAttached { agent: "R2".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::SetReviewer { agent: "R2".into() }]);
        t.reviewer = Some("R2".into());

        // new reviewer approves
        let (next, _) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
    }

    // Reviewer AgentFailed triggers replacement (second H1 regression path)
    #[test]
    fn walk_reviewer_replacement_after_agent_failed() {
        let mut t = view(Status::InReview);
        t.author = Some("W1".into());
        t.reviewer = Some("R1".into());
        t.pr = Some("42".into());

        // reviewer agent fails → stays InReview, spawns new reviewer
        let (next, effects) = transition(
            &t,
            &Event::AgentFailed {
                reason: "crash".into(),
            },
        )
        .unwrap();
        assert_eq!(
            next,
            Status::InReview,
            "InReview must be sticky on agent failure"
        );
        assert!(effects.contains(&Effect::ReleaseLease));
        assert!(effects.contains(&Effect::SpawnReviewer));
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
        t.status = next;

        // replacement reviewer attaches
        let (next, effects) =
            transition(&t, &Event::ReviewerAttached { agent: "R2".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::SetReviewer { agent: "R2".into() }]);
        t.reviewer = Some("R2".into());

        // finishes the review
        let (next, _) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
    }

    // Multiple rework rounds, then cap hit
    #[test]
    fn walk_rework_loop_hits_cap() {
        let mut t = view(Status::Open);
        t.status = Status::InReview;
        t.author = Some("W1".into());
        t.reviewer = Some("R1".into());
        t.pr = Some("10".into());

        for round in 0..REWORK_CAP {
            // VerdictChanges → Rework
            let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
            assert_eq!(next, Status::Rework, "round {round} should go to Rework");
            assert!(effects.contains(&Effect::IncrementReworkRound));
            t.status = next;
            t.rework_round += 1;

            // ReworkPushed → InReview
            let (next, _) = transition(&t, &Event::ReworkPushed).unwrap();
            assert_eq!(next, Status::InReview);
            t.status = next;
        }

        assert_eq!(t.rework_round, REWORK_CAP);

        // one more VerdictChanges at cap → Failed
        let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Failed);
        assert!(next.is_terminal());
        assert!(effects.contains(&Effect::ReleaseLease));
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
    }

    // MergeConflict → rework → push → re-review → approve → merge → done
    #[test]
    fn walk_merge_conflict_rework_done() {
        let mut t = view(Status::Open);

        // open → working → in-review → merging
        let (next, _) = transition(&t, &Event::Claimed { agent: "W1".into() }).unwrap();
        t.status = next;
        t.author = Some("W1".into());

        let (next, _) = transition(&t, &Event::SignaledDone { pr: "99".into() }).unwrap();
        t.status = next;
        t.pr = Some("99".into());

        let (next, _) = transition(&t, &Event::ReviewerAttached { agent: "R1".into() }).unwrap();
        t.status = next;
        t.reviewer = Some("R1".into());

        let (next, _) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        t.status = next;

        // MergeConflict → rework (skips reviewer hop)
        let (next, effects) = transition(&t, &Event::MergeConflict).unwrap();
        assert_eq!(next, Status::Rework);
        assert_eq!(
            effects,
            vec![
                Effect::ReleaseLease,
                Effect::IncrementReworkRound,
                Effect::ResumeWorker,
            ]
        );
        assert!(!effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
        assert!(!effects.contains(&Effect::ResumeReviewer));
        t.status = next;
        t.rework_round += 1;

        // rework pushed → back to in-review
        let (next, _) = transition(&t, &Event::ReworkPushed).unwrap();
        assert_eq!(next, Status::InReview);
        t.status = next;

        // re-approve → merging → done
        let (next, _) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        t.status = next;

        let (next, _) = transition(&t, &Event::MergeSucceeded).unwrap();
        assert_eq!(next, Status::Done);
    }

    // R2 pre-merge gate: R1 approves → R2 attaches as replacement reviewer
    // → R2 requests changes → rework → push → R2 resumes → R2 approves → merge → done.
    // This proves the existing lifecycle transitions support the R2 flow without new states.
    #[test]
    fn walk_r2_pre_merge_gate_full_cycle() {
        let mut t = view(Status::Open);

        // open → working
        let (next, _) = transition(&t, &Event::Claimed { agent: "W1".into() }).unwrap();
        t.status = next;
        t.author = Some("W1".into());

        // working → in-review (spawns R1)
        let (next, _) = transition(&t, &Event::SignaledDone { pr: "42".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        t.status = next;
        t.pr = Some("42".into());

        // R1 attaches
        let (next, _) = transition(&t, &Event::ReviewerAttached { agent: "R1".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        t.reviewer = Some("R1".into());

        // R1 approves → Merging (in the daemon, R2 gate intercepts BEFORE this
        // transition fires; the lifecycle doesn't see VerdictApprove for R1.
        // Instead, R2 replaces R1 as the reviewer.)
        // R2 attaches (replacing R1 — daemon tears down R1 first)
        let (next, effects) =
            transition(&t, &Event::ReviewerAttached { agent: "R2".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::SetReviewer { agent: "R2".into() }]);
        t.reviewer = Some("R2".into());

        // R2 requests changes → rework
        let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Rework);
        assert!(effects.contains(&Effect::IncrementReworkRound));
        assert!(effects.contains(&Effect::ResumeWorker));
        t.status = next;
        t.rework_round += 1;

        // Worker pushes rework → back to in-review (ResumeReviewer = R2)
        let (next, effects) = transition(&t, &Event::ReworkPushed).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::ResumeReviewer]);
        t.status = next;

        // R2 approves on re-review → merging
        let (next, effects) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        assert!(effects.contains(&Effect::MergePr { pr: "42".into() }));
        t.status = next;

        // Merge succeeds → done
        let (next, _) = transition(&t, &Event::MergeSucceeded).unwrap();
        assert_eq!(next, Status::Done);
        assert!(next.is_terminal());
    }

    // R2 approves without rework → direct merge
    #[test]
    fn walk_r2_approves_immediately_permits_merge() {
        let mut t = view(Status::InReview);
        t.author = Some("W1".into());
        t.pr = Some("99".into());

        // R2 attaches (after R1 was torn down by daemon)
        let (next, _) = transition(&t, &Event::ReviewerAttached { agent: "R2".into() }).unwrap();
        assert_eq!(next, Status::InReview);
        t.reviewer = Some("R2".into());

        // R2 approves → merging (no rework needed)
        let (next, effects) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        assert!(effects.contains(&Effect::MergePr { pr: "99".into() }));
    }

    // After R2-requested rework, ReworkPushed yields ResumeReviewer (not
    // SpawnReviewer). The daemon feeds this back to R2, not R1.
    #[test]
    fn walk_r2_rework_routes_back_to_reviewer_not_spawn() {
        let mut t = view(Status::InReview);
        t.author = Some("W1".into());
        t.reviewer = Some("R2".into());
        t.pr = Some("42".into());

        // R2 requests changes
        let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Rework);
        assert!(effects.contains(&Effect::ResumeWorker));
        assert!(!effects.contains(&Effect::SpawnReviewer));
        t.status = next;
        t.rework_round += 1;

        // Worker pushes rework
        let (next, effects) = transition(&t, &Event::ReworkPushed).unwrap();
        assert_eq!(next, Status::InReview);
        assert_eq!(effects, vec![Effect::ResumeReviewer]);
        assert!(!effects.contains(&Effect::SpawnReviewer));
    }

    // Stale-SHA scenario: after R2 approves, MergeFailed fires (daemon detects
    // head moved). This puts the task back to InReview with ResumeReviewer.
    #[test]
    fn walk_stale_sha_fires_merge_failed() {
        let mut t = view(Status::InReview);
        t.author = Some("W1".into());
        t.reviewer = Some("R2".into());
        t.pr = Some("42".into());

        // R2 approves → merging
        let (next, _) = transition(&t, &Event::VerdictApprove).unwrap();
        assert_eq!(next, Status::Merging);
        t.status = next;

        // Daemon detects stale SHA → MergeFailed
        let (next, effects) = transition(
            &t,
            &Event::MergeFailed {
                reason: "PR #42 head moved since review".into(),
            },
        )
        .unwrap();
        assert_eq!(next, Status::InReview);
        assert!(effects.contains(&Effect::ResumeReviewer));
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
    }

    // MergeConflict at rework cap → Failed
    #[test]
    fn walk_merge_conflict_at_cap_fails() {
        let mut t = view(Status::Merging);
        t.author = Some("W1".into());
        t.reviewer = Some("R1".into());
        t.pr = Some("99".into());
        t.rework_round = REWORK_CAP;

        let (next, effects) = transition(&t, &Event::MergeConflict).unwrap();
        assert_eq!(next, Status::Failed);
        assert!(next.is_terminal());
        assert!(effects.contains(&Effect::ReleaseLease));
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
        assert!(!effects.contains(&Effect::ResumeWorker));
    }

    // MergeFailed (non-conflict) still goes to InReview with reviewer
    #[test]
    fn walk_merge_failed_non_conflict_unchanged() {
        let mut t = view(Status::Merging);
        t.author = Some("W1".into());
        t.reviewer = Some("R1".into());
        t.pr = Some("99".into());

        let (next, effects) = transition(
            &t,
            &Event::MergeFailed {
                reason: "branch protection".into(),
            },
        )
        .unwrap();
        assert_eq!(next, Status::InReview);
        assert!(effects.contains(&Effect::ResumeReviewer));
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::NotifyOwner { .. })));
    }

    // ── Phase-aware idle reaping contract (#176) ─────────────────────────
    // These tests document the lifecycle invariants that the daemon's
    // phase-aware idle reaper relies on.

    #[test]
    fn idle_zombie_working_agent_failed_resets_to_open() {
        // A genuinely idle worker (task still in Working, no PR yet) fires
        // AgentFailed, which resets the task to Open for re-claim.
        let t = view(Status::Working);
        assert_ok(
            &t,
            &Event::AgentFailed {
                reason: "worker idle 300s — zombie reaped".into(),
            },
            Status::Open,
            &[
                Effect::ReleaseLease,
                Effect::ClearAuthor,
                Effect::NotifyOwner {
                    reason: "worker idle 300s — zombie reaped".into(),
                },
            ],
        );
    }

    #[test]
    fn legitimate_idle_in_review_agent_failed_would_spawn_reviewer() {
        // If AgentFailed WERE fired for an in-review task (which the
        // phase-aware reaper avoids), it would stay in-review but
        // unnecessarily spawn a replacement reviewer. This test documents
        // why the daemon skips AgentFailed for in-review workers: the
        // side effects (SpawnReviewer, NotifyOwner) are unwanted for a
        // worker that legitimately submitted and is awaiting review.
        let t = view_with_author(Status::InReview, "W1");
        let (next, effects) = transition(
            &t,
            &Event::AgentFailed {
                reason: "should not fire for legitimate idle".into(),
            },
        )
        .unwrap();
        assert_eq!(next, Status::InReview);
        assert!(
            effects.contains(&Effect::SpawnReviewer),
            "AgentFailed in in-review spawns a reviewer — this is why the daemon skips it"
        );
    }

    #[test]
    fn legitimate_idle_merging_agent_failed_would_regress_to_in_review() {
        // If AgentFailed WERE fired during merging, the task regresses to
        // in-review. The phase-aware reaper avoids this by not firing
        // AgentFailed for merging workers.
        let t = view_with_author(Status::Merging, "W1");
        let (next, effects) = transition(
            &t,
            &Event::AgentFailed {
                reason: "should not fire for legitimate idle".into(),
            },
        )
        .unwrap();
        assert_eq!(next, Status::InReview);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::NotifyOwner { .. })),
            "AgentFailed in merging notifies owner — this is why the daemon skips it"
        );
    }
}

// ===========================================================================
// Property / fuzz tests (proptest)
// ===========================================================================

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_event() -> impl Strategy<Value = Event> {
        prop_oneof![
            Just(Event::Claimed { agent: "W1".into() }),
            Just(Event::SignaledDone { pr: "42".into() }),
            Just(Event::ReviewerAttached { agent: "R1".into() }),
            Just(Event::VerdictApprove),
            Just(Event::VerdictChanges),
            Just(Event::ChecksFailed {
                checks: vec!["test".into()]
            }),
            Just(Event::ReworkPushed),
            Just(Event::MergeSucceeded),
            Just(Event::MergeFailed {
                reason: "conflict".into()
            }),
            Just(Event::MergeConflict),
            Just(Event::LeaseExpired),
            Just(Event::AgentFailed {
                reason: "crash".into()
            }),
            Just(Event::PrFoundMerged),
            Just(Event::PrFoundClosed),
            Just(Event::Cancelled { by: "boss".into() }),
        ]
    }

    fn arb_event_seq(max_len: usize) -> impl Strategy<Value = Vec<Event>> {
        prop::collection::vec(arb_event(), 1..=max_len)
    }

    /// Apply a sequence of events to a fresh Open task, tracking state as the
    /// daemon would. Returns the final TaskView and the history of
    /// (pre_status, event, post_status) for accepted transitions.
    fn simulate(events: &[Event]) -> (TaskView, Vec<(Status, Event, Status)>) {
        let mut t = TaskView {
            status: Status::Open,
            author: None,
            reviewer: None,
            rework_round: 0,
            pr: None,
            review_only: false,
        };
        let mut history = Vec::new();

        for event in events {
            let pre = t.status;
            if let Ok((next, effects)) = transition(&t, event) {
                history.push((pre, event.clone(), next));

                for eff in &effects {
                    match eff {
                        Effect::SetAuthor { agent } => t.author = Some(agent.clone()),
                        Effect::SetReviewer { agent } => t.reviewer = Some(agent.clone()),
                        Effect::IncrementReworkRound => t.rework_round += 1,
                        Effect::ReleaseLease => {}
                        _ => {}
                    }
                }
                // Track PR from SignaledDone
                if let Event::SignaledDone { pr } = event {
                    t.pr = Some(pr.clone());
                }
                t.status = next;
            }
        }

        (t, history)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        /// Once a task reaches a terminal state, every subsequent event is rejected.
        #[test]
        fn terminals_absorb(events in arb_event_seq(30)) {
            let (final_view, history) = simulate(&events);

            // Find the first transition into a terminal state
            let terminal_idx = history.iter().position(|(_, _, next)| next.is_terminal());

            if let Some(idx) = terminal_idx {
                let terminal_status = history[idx].2;
                // Every transition after the terminal one must not exist in history
                // (i.e., the simulate loop skipped them because transition() returned Err)
                prop_assert!(
                    history[idx + 1..].is_empty(),
                    "terminal {} absorbed a transition: {:?}",
                    terminal_status,
                    &history[idx + 1..],
                );
                // Also verify the final status is that terminal
                prop_assert_eq!(final_view.status, terminal_status);
            }
        }

        /// rework_round is monotonically non-decreasing across the lifecycle.
        #[test]
        fn rework_round_monotonic(events in arb_event_seq(40)) {
            let mut t = TaskView {
                status: Status::Open,
                author: None,
                reviewer: None,
                rework_round: 0,
                pr: None,
                review_only: false,
            };
            let mut prev_round = 0u32;

            for event in &events {
                if let Ok((next, effects)) = transition(&t, event) {
                    for eff in &effects {
                        match eff {
                            Effect::SetAuthor { agent } => t.author = Some(agent.clone()),
                            Effect::SetReviewer { agent } => t.reviewer = Some(agent.clone()),
                            Effect::IncrementReworkRound => t.rework_round += 1,
                            _ => {}
                        }
                    }
                    if let Event::SignaledDone { pr } = event {
                        t.pr = Some(pr.clone());
                    }
                    t.status = next;

                    prop_assert!(
                        t.rework_round >= prev_round,
                        "rework_round went from {} to {}",
                        prev_round,
                        t.rework_round
                    );
                    prev_round = t.rework_round;
                }
            }
        }

        /// A state bearing a PR (pr.is_some()) can only transition to Open from
        /// Rework (AgentFailed/LeaseExpired). InReview and Merging must never
        /// drop to Open.
        #[test]
        fn pr_bearing_to_open_only_from_rework(events in arb_event_seq(40)) {
            let (_, history) = simulate(&events);

            let mut pr_set = false;
            for (pre, event, next) in &history {
                if let Event::SignaledDone { .. } = event {
                    pr_set = true;
                }
                if pr_set && *next == Status::Open {
                    prop_assert!(
                        *pre == Status::Rework || *pre == Status::Working,
                        "PR-bearing task went Open from {:?} (event {:?}), expected only Rework or Working",
                        pre, event
                    );
                }
            }
        }

        /// Cancelled is reachable from every non-terminal state.
        #[test]
        fn cancelled_reachable_from_all_non_terminals(status_idx in 0..5usize) {
            let statuses = [
                Status::Open,
                Status::Working,
                Status::InReview,
                Status::Rework,
                Status::Merging,
            ];
            let status = statuses[status_idx];
            let t = TaskView {
                status,
                author: Some("W1".into()),
                reviewer: Some("R1".into()),
                rework_round: 0,
                pr: Some("1".into()),
                review_only: false,
            };
            let result = transition(&t, &Event::Cancelled { by: "x".into() });
            prop_assert!(result.is_ok(), "Cancelled rejected from {:?}", status);
            let (next, _) = result.unwrap();
            prop_assert_eq!(next, Status::Cancelled);
        }

        /// IncrementReworkRound only appears on an actionable rework event.
        #[test]
        fn increment_rework_only_on_rework_events(events in arb_event_seq(40)) {
            let mut t = TaskView {
                status: Status::Open,
                author: None,
                reviewer: None,
                rework_round: 0,
                pr: None,
                review_only: false,
            };

            for event in &events {
                if let Ok((next, effects)) = transition(&t, event) {
                    if effects.contains(&Effect::IncrementReworkRound) {
                        prop_assert!(
                            matches!(
                                event,
                                Event::VerdictChanges
                                    | Event::ChecksFailed { .. }
                                    | Event::MergeConflict
                            ),
                            "IncrementReworkRound from event {:?}", event
                        );
                        prop_assert_eq!(
                            next, Status::Rework,
                            "IncrementReworkRound but next state is {:?}", next
                        );
                    }
                    for eff in &effects {
                        match eff {
                            Effect::SetAuthor { agent } => t.author = Some(agent.clone()),
                            Effect::SetReviewer { agent } => t.reviewer = Some(agent.clone()),
                            Effect::IncrementReworkRound => t.rework_round += 1,
                            _ => {}
                        }
                    }
                    if let Event::SignaledDone { pr } = event {
                        t.pr = Some(pr.clone());
                    }
                    t.status = next;
                }
            }
        }

        /// Every transition into Failed or Cancelled carries a NotifyOwner effect.
        #[test]
        fn terminal_failed_or_cancelled_always_notifies(events in arb_event_seq(30)) {
            let mut t = TaskView {
                status: Status::Open,
                author: None,
                reviewer: None,
                rework_round: 0,
                pr: None,
                review_only: false,
            };

            for event in &events {
                if let Ok((next, effects)) = transition(&t, event) {
                    if next == Status::Failed || next == Status::Cancelled {
                        prop_assert!(
                            effects.iter().any(|e| matches!(e, Effect::NotifyOwner { .. })),
                            "transition {:?} -> {:?} on {:?} lacks NotifyOwner; effects: {:?}",
                            t.status, next, event, effects
                        );
                    }
                    for eff in &effects {
                        match eff {
                            Effect::SetAuthor { agent } => t.author = Some(agent.clone()),
                            Effect::SetReviewer { agent } => t.reviewer = Some(agent.clone()),
                            Effect::IncrementReworkRound => t.rework_round += 1,
                            _ => {}
                        }
                    }
                    if let Event::SignaledDone { pr } = event {
                        t.pr = Some(pr.clone());
                    }
                    t.status = next;
                }
            }
        }

        /// Valid transitions never produce an empty effects list — every accepted
        /// event has at least one side-effect.
        #[test]
        fn no_empty_effects_on_accepted_transition(events in arb_event_seq(30)) {
            let mut t = TaskView {
                status: Status::Open,
                author: None,
                reviewer: None,
                rework_round: 0,
                pr: None,
                review_only: false,
            };

            for event in &events {
                if let Ok((next, effects)) = transition(&t, event) {
                    prop_assert!(
                        !effects.is_empty(),
                        "empty effects for {:?} → {:?} on event {:?}",
                        t.status, next, event
                    );
                    for eff in &effects {
                        match eff {
                            Effect::SetAuthor { agent } => t.author = Some(agent.clone()),
                            Effect::SetReviewer { agent } => t.reviewer = Some(agent.clone()),
                            Effect::IncrementReworkRound => t.rework_round += 1,
                            _ => {}
                        }
                    }
                    if let Event::SignaledDone { pr } = event {
                        t.pr = Some(pr.clone());
                    }
                    t.status = next;
                }
            }
        }
    }
}
