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
    Claimed { agent: String },
    SignaledDone { pr: String },
    ReviewerAttached { agent: String },
    VerdictApprove,
    VerdictChanges,
    ReworkPushed,
    MergeSucceeded,
    MergeFailed { reason: String },
    PrFoundMerged,
    PrFoundClosed,
    LeaseExpired,
    AgentFailed { reason: String },
    Cancelled { by: String },
}

// ---------------------------------------------------------------------------
// Effect — declarative side-effects, executed by callers in later PRs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    SetAuthor { agent: String },
    SetReviewer { agent: String },
    SpawnWorker,
    SpawnReviewer,
    ResumeReviewer,
    ResumeWorker,
    MergePr { pr: String },
    IncrementReworkRound,
    NotifyOwner { reason: String },
    ReleaseLease,
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

pub const REWORK_CAP: u32 = 3;

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
        (Status::Open, Event::Cancelled { .. }) => {
            Ok((Status::Cancelled, vec![Effect::ReleaseLease]))
        }
        (Status::Open, Event::LeaseExpired) => reject("no lease to expire in open"),
        (Status::Open, Event::SignaledDone { .. }) => reject("cannot signal done from open"),
        (Status::Open, Event::ReviewerAttached { .. }) => reject("no reviewer in open"),
        (Status::Open, Event::VerdictApprove) => reject("no verdict in open"),
        (Status::Open, Event::VerdictChanges) => reject("no verdict in open"),
        (Status::Open, Event::ReworkPushed) => reject("no rework from open"),
        (Status::Open, Event::MergeSucceeded) => reject("no merge from open"),
        (Status::Open, Event::MergeFailed { .. }) => reject("no merge from open"),
        (Status::Open, Event::PrFoundMerged) => reject("no PR from open"),
        (Status::Open, Event::PrFoundClosed) => reject("no PR from open"),
        (Status::Open, Event::AgentFailed { .. }) => reject("no agent in open"),

        // ---- Working ----
        (Status::Working, Event::SignaledDone { .. }) => {
            Ok((Status::InReview, vec![Effect::SpawnReviewer]))
        }
        (Status::Working, Event::AgentFailed { reason }) => Ok((
            Status::Open,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: reason.clone(),
                },
            ],
        )),
        (Status::Working, Event::LeaseExpired) => Ok((Status::Open, vec![Effect::ReleaseLease])),
        (Status::Working, Event::Cancelled { .. }) => {
            Ok((Status::Cancelled, vec![Effect::ReleaseLease]))
        }
        (Status::Working, Event::Claimed { .. }) => reject("already claimed"),
        (Status::Working, Event::ReviewerAttached { .. }) => reject("not in review"),
        (Status::Working, Event::VerdictApprove) => reject("not in review"),
        (Status::Working, Event::VerdictChanges) => reject("not in review"),
        (Status::Working, Event::ReworkPushed) => reject("not in rework"),
        (Status::Working, Event::MergeSucceeded) => reject("not merging"),
        (Status::Working, Event::MergeFailed { .. }) => reject("not merging"),
        (Status::Working, Event::PrFoundMerged) => reject("not in review"),
        (Status::Working, Event::PrFoundClosed) => reject("not in review"),

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
        (Status::InReview, Event::VerdictChanges) => {
            if t.review_only {
                return Ok((
                    Status::Failed,
                    vec![Effect::PostFindingsNote, Effect::ReleaseLease],
                ));
            }
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
                vec![Effect::IncrementReworkRound, Effect::ResumeWorker],
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
        (Status::InReview, Event::Cancelled { .. }) => {
            Ok((Status::Cancelled, vec![Effect::ReleaseLease]))
        }
        (Status::InReview, Event::Claimed { .. }) => reject("in review, not claimable"),
        (Status::InReview, Event::SignaledDone { .. }) => reject("already in review"),
        (Status::InReview, Event::ReworkPushed) => reject("not in rework"),
        (Status::InReview, Event::MergeSucceeded) => reject("not merging"),
        (Status::InReview, Event::MergeFailed { .. }) => reject("not merging"),
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

        // ---- Rework ----
        (Status::Rework, Event::ReworkPushed) => {
            Ok((Status::InReview, vec![Effect::ResumeReviewer]))
        }
        (Status::Rework, Event::AgentFailed { reason }) => Ok((
            Status::Open,
            vec![
                Effect::ReleaseLease,
                Effect::NotifyOwner {
                    reason: reason.clone(),
                },
            ],
        )),
        (Status::Rework, Event::LeaseExpired) => Ok((Status::Open, vec![Effect::ReleaseLease])),
        (Status::Rework, Event::Cancelled { .. }) => {
            Ok((Status::Cancelled, vec![Effect::ReleaseLease]))
        }
        (Status::Rework, Event::Claimed { .. }) => reject("in rework, not claimable"),
        (Status::Rework, Event::SignaledDone { .. }) => reject("must push rework, not signal done"),
        (Status::Rework, Event::ReviewerAttached { .. }) => reject("not in review"),
        (Status::Rework, Event::VerdictApprove) => reject("not in review"),
        (Status::Rework, Event::VerdictChanges) => reject("not in review"),
        (Status::Rework, Event::MergeSucceeded) => reject("not merging"),
        (Status::Rework, Event::MergeFailed { .. }) => reject("not merging"),
        (Status::Rework, Event::PrFoundMerged) => reject("not in review"),
        (Status::Rework, Event::PrFoundClosed) => reject("not in review"),

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
        (Status::Merging, Event::Cancelled { .. }) => {
            Ok((Status::Cancelled, vec![Effect::ReleaseLease]))
        }
        (Status::Merging, Event::Claimed { .. }) => reject("merging in progress"),
        (Status::Merging, Event::SignaledDone { .. }) => reject("merging in progress"),
        (Status::Merging, Event::ReviewerAttached { .. }) => reject("merging in progress"),
        (Status::Merging, Event::VerdictApprove) => reject("merging in progress"),
        (Status::Merging, Event::VerdictChanges) => reject("merging in progress"),
        (Status::Merging, Event::ReworkPushed) => reject("merging in progress"),
        (Status::Merging, Event::PrFoundMerged) => reject("merging in progress"),
        (Status::Merging, Event::PrFoundClosed) => reject("merging in progress"),
        (Status::Merging, Event::LeaseExpired) => reject("merging in progress"),
        (Status::Merging, Event::AgentFailed { .. }) => reject("merging in progress"),

        // ---- Done (terminal) ----
        (Status::Done, _) => reject("task is done"),

        // ---- Failed (terminal) ----
        (Status::Failed, _) => reject("task has failed"),

        // ---- Cancelled (terminal) ----
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
            &[Effect::ReleaseLease],
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
            Event::ReworkPushed,
            Event::MergeSucceeded,
            Event::MergeFailed { reason: "x".into() },
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
            &[Effect::ReleaseLease],
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
            Event::ReworkPushed,
            Event::MergeSucceeded,
            Event::MergeFailed { reason: "x".into() },
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
            &[Effect::IncrementReworkRound, Effect::ResumeWorker],
        );
    }

    #[test]
    fn in_review_verdict_changes_review_only_fails() {
        let mut t = view_with_author(Status::InReview, "W1");
        t.review_only = true;
        assert_ok(
            &t,
            &Event::VerdictChanges,
            Status::Failed,
            &[Effect::PostFindingsNote, Effect::ReleaseLease],
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
            &[Effect::ReleaseLease],
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

    #[test]
    fn rework_cancelled() {
        let t = view(Status::Rework);
        assert_ok(
            &t,
            &Event::Cancelled { by: "boss".into() },
            Status::Cancelled,
            &[Effect::ReleaseLease],
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
            &[Effect::ReleaseLease],
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
            Event::AgentFailed { reason: "x".into() },
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
            vec![Effect::IncrementReworkRound, Effect::ResumeWorker]
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
    fn review_only_changes_at_zero_rounds_still_fails() {
        let mut t = view_with_author(Status::InReview, "W1");
        t.review_only = true;
        t.rework_round = 0;
        let (next, effects) = transition(&t, &Event::VerdictChanges).unwrap();
        assert_eq!(next, Status::Failed);
        assert!(effects.contains(&Effect::PostFindingsNote));
        assert!(effects.contains(&Effect::ReleaseLease));
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
            Event::PrFoundMerged,
            Event::PrFoundClosed,
            Event::LeaseExpired,
            Event::AgentFailed { reason: "x".into() },
            Event::Cancelled { by: "boss".into() },
        ]
    }
}
