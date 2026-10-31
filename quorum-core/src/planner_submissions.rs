//! Storage for the planner `submit_plan` MCP tool (task #1 of the
//! planner-submit-plan-mcp batch).
//!
//! A row is created on the first `SubmitPlan` call for a planner run.
//! Acceptance is guarded so the first accepted submission stands: a second
//! `record_accepted` call for the same run reports `AlreadySubmitted` rather
//! than overwriting the stored response. Each rejected call increments
//! `rejections`; once the count reaches [`MAX_PLAN_SUBMIT_REJECTIONS`],
//! further submissions are refused without validating.
//!
//! `planner_submission_calls` is a separate per-call ledger: [`record_call`]
//! appends one row per `submit_plan` call, accepted or refused, and
//! [`call_summary`] reads the per-run totals. It stores only the run, sequence,
//! receipt time, accepted flag, and a fixed [`RejectionCategory`], never plan
//! text or validation messages. The ledger neither reads nor writes
//! `planner_submissions.rejections`, so the rejection budget is unaffected.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::collections::BTreeMap;

/// Bound on rejected `submit_plan` calls per planner run before the endpoint
/// refuses further submissions without validating.
pub const MAX_PLAN_SUBMIT_REJECTIONS: i64 = 5;

/// Why a `submit_plan` call was refused. The stored strings are the closed set
/// enforced by the `planner_submission_calls.rejection_category` CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RejectionCategory {
    Validation,
    BudgetExhausted,
    AlreadySubmitted,
    Unauthorized,
    UnsupportedVersion,
    Timeout,
    Internal,
}

impl RejectionCategory {
    pub const ALL: [Self; 7] = [
        Self::Validation,
        Self::BudgetExhausted,
        Self::AlreadySubmitted,
        Self::Unauthorized,
        Self::UnsupportedVersion,
        Self::Timeout,
        Self::Internal,
    ];

    /// The value stored in `rejection_category`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Validation => "validation",
            Self::BudgetExhausted => "budget_exhausted",
            Self::AlreadySubmitted => "already_submitted",
            Self::Unauthorized => "unauthorized",
            Self::UnsupportedVersion => "unsupported_version",
            Self::Timeout => "timeout",
            Self::Internal => "internal",
        }
    }

    /// Parse a stored value. `None` for anything outside the fixed set.
    pub fn from_stored(stored: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == stored)
    }
}

/// The outcome of one `submit_plan` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallOutcome {
    Accepted,
    Rejected(RejectionCategory),
}

/// Per-run totals from the `submit_plan` call ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallSummary {
    pub total: i64,
    pub accepted: i64,
    /// Every [`RejectionCategory`] is present, with `0` when it never occurred.
    pub rejected: BTreeMap<RejectionCategory, i64>,
}

/// Outcome of attempting to record an accepted plan submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// This call's response is now the stored, accepted response for the run.
    Accepted,
    /// A response was already accepted for this run; this call's response was
    /// not recorded.
    AlreadySubmitted,
    /// The run's rejection budget is exhausted; this call was not recorded.
    BudgetExhausted,
}

/// Record an accepted plan submission for `run_id`. Runs inside the caller's
/// `BEGIN IMMEDIATE` transaction. Creates the row if missing.
///
/// Once `rejections >= MAX_PLAN_SUBMIT_REJECTIONS`, returns `BudgetExhausted`
/// without writing `response_json`. Otherwise, a guarded
/// `UPDATE ... WHERE response_json IS NULL` records the response only if none
/// is stored yet: `Accepted` on the first call for a run, `AlreadySubmitted`
/// on any later call (the first submission stands).
pub fn record_accepted(
    tx: &Transaction,
    run_id: &str,
    graph_id: i64,
    response_json: &str,
    now: i64,
) -> Result<SubmitOutcome> {
    tx.execute(
        "INSERT OR IGNORE INTO planner_submissions(run_id, graph_id) VALUES (?1, ?2)",
        params![run_id, graph_id],
    )?;

    let rejections: i64 = tx.query_row(
        "SELECT rejections FROM planner_submissions WHERE run_id = ?1",
        params![run_id],
        |row| row.get(0),
    )?;
    if rejections >= MAX_PLAN_SUBMIT_REJECTIONS {
        return Ok(SubmitOutcome::BudgetExhausted);
    }

    let accepted: Option<String> = tx
        .query_row(
            "UPDATE planner_submissions
             SET response_json = ?1, accepted_at = ?2
             WHERE run_id = ?3 AND response_json IS NULL
             RETURNING run_id",
            params![response_json, now, run_id],
            |row| row.get(0),
        )
        .optional()?;

    Ok(if accepted.is_some() {
        SubmitOutcome::Accepted
    } else {
        SubmitOutcome::AlreadySubmitted
    })
}

/// Record a rejected plan submission for `run_id`. Runs inside the caller's
/// transaction. Creates the row if missing; increments `rejections`. Returns
/// the resulting count.
///
/// The increment is guarded by `response_json IS NULL`: validation runs outside
/// the write transaction, so a call whose plan was rejected can commit after
/// another connection accepted a plan for the same run. Counting that rejection
/// would make the stored counter describe a run that is already settled, so the
/// guard leaves an accepted row untouched and reports its existing count.
pub fn record_rejection(tx: &Transaction, run_id: &str, graph_id: i64) -> Result<i64> {
    tx.execute(
        "INSERT OR IGNORE INTO planner_submissions(run_id, graph_id) VALUES (?1, ?2)",
        params![run_id, graph_id],
    )?;
    let incremented: Option<i64> = tx
        .query_row(
            "UPDATE planner_submissions
             SET rejections = rejections + 1
             WHERE run_id = ?1 AND response_json IS NULL
             RETURNING rejections",
            params![run_id],
            |row| row.get(0),
        )
        .optional()?;
    match incremented {
        Some(rejections) => Ok(rejections),
        None => Ok(tx.query_row(
            "SELECT rejections FROM planner_submissions WHERE run_id = ?1",
            params![run_id],
            |row| row.get(0),
        )?),
    }
}

/// Read-only. `true` when the run's rejection count has reached
/// [`MAX_PLAN_SUBMIT_REJECTIONS`]. Unknown runs are not exhausted.
pub fn budget_exhausted(conn: &Connection, run_id: &str) -> Result<bool> {
    let rejections: Option<i64> = conn
        .query_row(
            "SELECT rejections FROM planner_submissions WHERE run_id = ?1",
            params![run_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(rejections.unwrap_or(0) >= MAX_PLAN_SUBMIT_REJECTIONS)
}

/// Read-only. The accepted response JSON for `run_id`, if any has been
/// recorded. `None` for an unknown run or one with no accepted response yet.
pub fn accepted_response(conn: &Connection, run_id: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT response_json FROM planner_submissions WHERE run_id = ?1",
        params![run_id],
        |row| row.get(0),
    )
    .optional()
    .map(Option::flatten)
    .map_err(Into::into)
}

/// Append one `submit_plan` call to the ledger inside the caller's transaction,
/// returning the allocated per-run `seq` (starting at 1). Performs no commit;
/// callers use `BEGIN IMMEDIATE` so `seq` allocation is serialized.
///
/// Returns `Ok(None)` without writing when `run_id` matches no
/// `run_capabilities` row, so an unknown caller-supplied string never enters
/// the ledger. Any issued capability qualifies, whatever its role and whether
/// or not it is revoked. Storage errors propagate unchanged.
pub fn record_call(
    tx: &Transaction,
    run_id: &str,
    outcome: CallOutcome,
    now: i64,
) -> Result<Option<i64>> {
    let (accepted, category) = match outcome {
        CallOutcome::Accepted => (1_i64, None),
        CallOutcome::Rejected(category) => (0_i64, Some(category.as_str())),
    };
    tx.query_row(
        "INSERT INTO planner_submission_calls
             (run_id, seq, received_at, accepted, rejection_category)
         SELECT capability.run_id,
                (SELECT COALESCE(MAX(seq), 0) + 1 FROM planner_submission_calls
                 WHERE run_id = capability.run_id),
                ?2, ?3, ?4
         FROM run_capabilities AS capability
         WHERE capability.run_id = ?1
         RETURNING seq",
        params![run_id, now, accepted, category],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

/// Read-only. Ledger totals for `run_id`; zeros for a run with no calls. A row
/// that does not decode to a valid outcome is a storage error, never skipped.
/// The error names no stored value, so a corrupt cell cannot reach a log line.
pub fn call_summary(conn: &Connection, run_id: &str) -> Result<CallSummary> {
    let mut summary = CallSummary {
        total: 0,
        accepted: 0,
        rejected: RejectionCategory::ALL.map(|c| (c, 0)).into(),
    };
    let mut stmt = conn.prepare(
        "SELECT accepted, rejection_category, COUNT(*) FROM planner_submission_calls
         WHERE run_id = ?1
         GROUP BY accepted, rejection_category",
    )?;
    let mut rows = stmt.query(params![run_id])?;
    while let Some(row) = rows.next()? {
        let accepted: i64 = row.get(0)?;
        let category: Option<String> = row.get(1)?;
        let count: i64 = row.get(2)?;
        match (accepted, category.as_deref()) {
            (1, None) => summary.accepted += count,
            (0, Some(stored)) => {
                let category =
                    RejectionCategory::from_stored(stored).ok_or_else(undecodable_ledger_row)?;
                *summary.rejected.entry(category).or_default() += count;
            }
            _ => return Err(undecodable_ledger_row().into()),
        }
        summary.total += count;
    }
    Ok(summary)
}

fn undecodable_ledger_row() -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        1,
        rusqlite::types::Type::Text,
        "undecodable planner_submission_calls row".into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn issue_capability(c: &mut Connection, run_id: &str, role: &str) {
        crate::capabilities::issue(c, run_id, 1, "planner-agent", role, 1).unwrap();
    }

    fn record(c: &mut Connection, run_id: &str, outcome: CallOutcome, now: i64) -> Option<i64> {
        let tx = db::begin_immediate(c).unwrap();
        let seq = record_call(&tx, run_id, outcome, now).unwrap();
        tx.commit().unwrap();
        seq
    }

    fn ledger_rows(c: &Connection, run_id: &str) -> Vec<(i64, i64, i64, Option<String>)> {
        c.prepare(
            "SELECT seq, received_at, accepted, rejection_category
             FROM planner_submission_calls WHERE run_id = ?1 ORDER BY seq",
        )
        .unwrap()
        .query_map([run_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
    }

    /// A rejection then an acceptance gets seq 1 and 2 with the right flag and
    /// category, and the summary counts agree.
    #[test]
    fn records_calls_in_sequence_and_summarizes_them() {
        let (_d, mut c) = open_tmp();
        issue_capability(&mut c, "run-a", "planner");
        issue_capability(&mut c, "run-b", "planner");

        let validation = CallOutcome::Rejected(RejectionCategory::Validation);
        assert_eq!(record(&mut c, "run-a", validation, 10), Some(1));
        assert_eq!(record(&mut c, "run-a", CallOutcome::Accepted, 20), Some(2));
        let exhausted = CallOutcome::Rejected(RejectionCategory::BudgetExhausted);
        assert_eq!(record(&mut c, "run-a", exhausted, 30), Some(3));
        // `seq` is per run, not global.
        assert_eq!(record(&mut c, "run-b", validation, 40), Some(1));

        assert_eq!(
            ledger_rows(&c, "run-a"),
            vec![
                (1, 10, 0, Some("validation".to_string())),
                (2, 20, 1, None),
                (3, 30, 0, Some("budget_exhausted".to_string())),
            ]
        );

        let summary = call_summary(&c, "run-a").unwrap();
        assert_eq!(summary.total, 3);
        assert_eq!(summary.accepted, 1);
        for category in RejectionCategory::ALL {
            let expected = match category {
                RejectionCategory::Validation | RejectionCategory::BudgetExhausted => 1,
                _ => 0,
            };
            assert_eq!(summary.rejected[&category], expected, "{category:?}");
        }
    }

    #[test]
    fn call_summary_is_zero_for_a_run_with_no_calls() {
        let (_d, c) = open_tmp();
        let summary = call_summary(&c, "no-such-run").unwrap();
        assert_eq!(summary.total, 0);
        assert_eq!(summary.accepted, 0);
        assert_eq!(summary.rejected.len(), RejectionCategory::ALL.len());
        assert!(summary.rejected.values().all(|count| *count == 0));
    }

    #[test]
    fn record_call_writes_no_row_for_an_unknown_run() {
        let (_d, mut c) = open_tmp();
        issue_capability(&mut c, "known", "planner");
        assert_eq!(
            record(&mut c, "unknown", CallOutcome::Accepted, 10),
            None,
            "an unknown run_id must report that nothing was written"
        );
        let rows: i64 = c
            .query_row("SELECT COUNT(*) FROM planner_submission_calls", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn record_call_accepts_any_issued_capability_including_revoked() {
        let (_d, mut c) = open_tmp();
        for (run_id, role) in [
            ("worker-run", "worker"),
            ("reviewer-run", "reviewer"),
            ("planner-run", "planner"),
        ] {
            issue_capability(&mut c, run_id, role);
            assert_eq!(record(&mut c, run_id, CallOutcome::Accepted, 10), Some(1));
        }
        let unauthorized = CallOutcome::Rejected(RejectionCategory::Unauthorized);
        issue_capability(&mut c, "revoked-run", "worker");
        assert!(crate::capabilities::revoke(&mut c, "revoked-run", 5).unwrap());
        assert_eq!(record(&mut c, "revoked-run", unauthorized, 10), Some(1));
    }

    /// The transaction belongs to the caller: rolling it back discards the row.
    #[test]
    fn record_call_does_not_commit_the_callers_transaction() {
        let (_d, mut c) = open_tmp();
        issue_capability(&mut c, "run-tx", "planner");
        let tx = db::begin_immediate(&mut c).unwrap();
        assert_eq!(
            record_call(&tx, "run-tx", CallOutcome::Accepted, 10).unwrap(),
            Some(1)
        );
        tx.rollback().unwrap();
        assert!(ledger_rows(&c, "run-tx").is_empty());
    }

    #[test]
    fn every_rejection_category_round_trips_through_the_check() {
        let (_d, mut c) = open_tmp();
        issue_capability(&mut c, "run-all", "planner");
        for (index, category) in RejectionCategory::ALL.into_iter().enumerate() {
            assert_eq!(
                RejectionCategory::from_stored(category.as_str()),
                Some(category)
            );
            let seq = record(&mut c, "run-all", CallOutcome::Rejected(category), 10);
            assert_eq!(seq, Some(index as i64 + 1));
        }
        let summary = call_summary(&c, "run-all").unwrap();
        assert_eq!(summary.total, RejectionCategory::ALL.len() as i64);
        assert!(summary.rejected.values().all(|count| *count == 1));
        assert_eq!(RejectionCategory::from_stored("Validation"), None);
    }

    #[test]
    fn schema_refuses_rows_outside_the_outcome_contract() {
        let (_d, c) = open_tmp();
        let insert = |seq: i64, accepted: i64, category: Option<&str>| {
            c.execute(
                "INSERT INTO planner_submission_calls
                     (run_id, seq, received_at, accepted, rejection_category)
                 VALUES ('run-check', ?1, 1, ?2, ?3)",
                params![seq, accepted, category],
            )
        };
        let is_check_violation = |result: rusqlite::Result<usize>| {
            matches!(
                result,
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_CHECK
            )
        };
        assert!(is_check_violation(insert(1, 0, Some("not-a-category"))));
        assert!(is_check_violation(insert(1, 1, Some("validation"))));
        assert!(is_check_violation(insert(1, 0, None)));
        assert!(is_check_violation(insert(1, 2, None)));
        // The contract's two valid shapes still insert.
        insert(1, 1, None).unwrap();
        insert(2, 0, Some("internal")).unwrap();
    }

    /// The ledger has no column able to hold plan or validation message text.
    #[test]
    fn ledger_columns_are_exactly_the_payload_free_set() {
        let (_d, c) = open_tmp();
        let columns: Vec<String> = c
            .prepare("SELECT name FROM pragma_table_info('planner_submission_calls') ORDER BY cid")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            columns,
            [
                "run_id",
                "seq",
                "received_at",
                "accepted",
                "rejection_category"
            ]
        );
    }

    /// Rows that bypass the CHECK must surface as errors, not vanish from counts.
    #[test]
    fn call_summary_reports_rows_it_cannot_decode() {
        for (accepted, category) in [
            (0_i64, Some("not-a-category")),
            (0, None),
            (1, Some("validation")),
            (2, None),
        ] {
            let (_d, c) = open_tmp();
            c.execute_batch("PRAGMA ignore_check_constraints=ON")
                .unwrap();
            c.execute(
                "INSERT INTO planner_submission_calls
                     (run_id, seq, received_at, accepted, rejection_category)
                 VALUES ('run-bad', 1, 1, ?1, ?2)",
                params![accepted, category],
            )
            .unwrap();
            let error = call_summary(&c, "run-bad").unwrap_err();
            assert_eq!(error.exit_code(), 3);
            assert!(
                !error.to_string().contains("not-a-category"),
                "stored values stay out of the error: {error}"
            );
        }
    }

    #[test]
    fn ledger_leaves_rejection_budget_and_submission_rows_alone() {
        let (_d, mut c) = open_tmp();
        issue_capability(&mut c, "run-budget", "planner");
        let validation = CallOutcome::Rejected(RejectionCategory::Validation);
        for now in 0..(MAX_PLAN_SUBMIT_REJECTIONS + 1) {
            record(&mut c, "run-budget", validation, now);
        }
        assert!(!budget_exhausted(&c, "run-budget").unwrap());
        let submissions: i64 = c
            .query_row("SELECT COUNT(*) FROM planner_submissions", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(submissions, 0);

        // And the budget's own writes add nothing to the ledger.
        let tx = c.transaction().unwrap();
        record_rejection(&tx, "run-budget", 1).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            call_summary(&c, "run-budget").unwrap().total,
            MAX_PLAN_SUBMIT_REJECTIONS + 1
        );
    }

    /// Writers on separate connections serialize through `BEGIN IMMEDIATE`: every
    /// call lands exactly once and each run's `seq` stays contiguous from 1.
    #[test]
    fn concurrent_writers_keep_seq_contiguous_per_run() {
        use std::sync::{Arc, Barrier};

        const WRITERS: usize = 8;
        const CALLS_PER_WRITER: usize = 25;
        const ROUNDS: usize = 10;

        for _ in 0..ROUNDS {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("ledger-race.db");
            {
                let mut c = db::open(&path).unwrap();
                issue_capability(&mut c, "run-x", "planner");
                issue_capability(&mut c, "run-y", "planner");
            }

            let barrier = Arc::new(Barrier::new(WRITERS));
            let handles: Vec<_> = (0..WRITERS)
                .map(|writer| {
                    let path = path.clone();
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let mut c = db::open(&path).unwrap();
                        let run_id = if writer % 2 == 0 { "run-x" } else { "run-y" };
                        barrier.wait();
                        for now in 0..CALLS_PER_WRITER {
                            let tx = db::begin_immediate(&mut c).unwrap();
                            let seq = record_call(&tx, run_id, CallOutcome::Accepted, now as i64)
                                .unwrap();
                            assert!(seq.is_some());
                            tx.commit().unwrap();
                        }
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap();
            }

            let c = db::open(&path).unwrap();
            let expected_calls = (WRITERS / 2 * CALLS_PER_WRITER) as i64;
            for run_id in ["run-x", "run-y"] {
                let seqs: Vec<i64> = ledger_rows(&c, run_id)
                    .into_iter()
                    .map(|row| row.0)
                    .collect();
                assert_eq!(seqs, (1..=expected_calls).collect::<Vec<_>>());
                assert_eq!(call_summary(&c, run_id).unwrap().accepted, expected_calls);
            }
        }
    }

    #[test]
    fn accepts_once_then_reports_already_submitted() {
        let (_d, mut c) = open_tmp();
        let tx = c.transaction().unwrap();
        let outcome = record_accepted(&tx, "run-1", 10, "{\"kind\":\"plan\"}", 100).unwrap();
        assert_eq!(outcome, SubmitOutcome::Accepted);
        tx.commit().unwrap();

        // graph_id and accepted_at are stored, not just response_json.
        let (graph_id, accepted_at): (i64, i64) = c
            .query_row(
                "SELECT graph_id, accepted_at FROM planner_submissions WHERE run_id = ?1",
                ["run-1"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(graph_id, 10);
        assert_eq!(accepted_at, 100);

        let tx = c.transaction().unwrap();
        let outcome = record_accepted(&tx, "run-1", 10, "{\"kind\":\"different\"}", 200).unwrap();
        assert_eq!(outcome, SubmitOutcome::AlreadySubmitted);
        tx.commit().unwrap();

        let stored = accepted_response(&c, "run-1").unwrap();
        assert_eq!(stored, Some("{\"kind\":\"plan\"}".to_string()));

        // accepted_at is not overwritten by the rejected second call.
        let accepted_at_after: i64 = c
            .query_row(
                "SELECT accepted_at FROM planner_submissions WHERE run_id = ?1",
                ["run-1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(accepted_at_after, 100);
    }

    /// Guards the `record_accepted` budget comparison itself (`rejections >=
    /// MAX_PLAN_SUBMIT_REJECTIONS`), independent of `budget_exhausted`'s own
    /// 4-vs-5 boundary test. Mutating the threshold to anything in `1..=5`
    /// (e.g. `>= 1`, `> 0`) must fail one of these two cases: a run with
    /// exactly one rejection, and a run with exactly four (one below
    /// exhaustion), both still accept a valid submission.
    #[test]
    fn accepts_below_rejection_threshold() {
        let (_d, mut c) = open_tmp();

        let tx = c.transaction().unwrap();
        record_rejection(&tx, "run-3", 30).unwrap();
        tx.commit().unwrap();
        let tx = c.transaction().unwrap();
        let outcome = record_accepted(&tx, "run-3", 30, "{\"kind\":\"plan\"}", 400).unwrap();
        tx.commit().unwrap();
        assert_eq!(outcome, SubmitOutcome::Accepted);
        assert_eq!(
            accepted_response(&c, "run-3").unwrap(),
            Some("{\"kind\":\"plan\"}".to_string())
        );

        for _ in 0..4 {
            let tx = c.transaction().unwrap();
            record_rejection(&tx, "run-4", 40).unwrap();
            tx.commit().unwrap();
        }
        let tx = c.transaction().unwrap();
        let outcome = record_accepted(&tx, "run-4", 40, "{\"kind\":\"plan\"}", 500).unwrap();
        tx.commit().unwrap();
        assert_eq!(outcome, SubmitOutcome::Accepted);
        assert_eq!(
            accepted_response(&c, "run-4").unwrap(),
            Some("{\"kind\":\"plan\"}".to_string())
        );
    }

    #[test]
    fn rejections_count_and_exhaust_budget() {
        let (_d, mut c) = open_tmp();

        for expected in 1..=4 {
            let tx = c.transaction().unwrap();
            let count = record_rejection(&tx, "run-2", 20).unwrap();
            tx.commit().unwrap();
            assert_eq!(count, expected);
        }
        assert!(!budget_exhausted(&c, "run-2").unwrap());

        let tx = c.transaction().unwrap();
        let count = record_rejection(&tx, "run-2", 20).unwrap();
        tx.commit().unwrap();
        assert_eq!(count, 5);
        assert!(budget_exhausted(&c, "run-2").unwrap());

        let tx = c.transaction().unwrap();
        let outcome = record_accepted(&tx, "run-2", 20, "{\"kind\":\"plan\"}", 300).unwrap();
        assert_eq!(outcome, SubmitOutcome::BudgetExhausted);
        tx.commit().unwrap();

        assert_eq!(accepted_response(&c, "run-2").unwrap(), None);
    }

    /// The rejection counter is a record of an unsettled run: once a plan is
    /// accepted, a late rejection committing behind it must not bump the count.
    #[test]
    fn rejections_are_not_counted_after_acceptance() {
        let (_d, mut c) = open_tmp();
        let tx = c.transaction().unwrap();
        record_rejection(&tx, "run-5", 50).unwrap();
        record_accepted(&tx, "run-5", 50, "{\"kind\":\"plan\"}", 600).unwrap();
        tx.commit().unwrap();

        let tx = c.transaction().unwrap();
        let count = record_rejection(&tx, "run-5", 50).unwrap();
        tx.commit().unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            accepted_response(&c, "run-5").unwrap(),
            Some("{\"kind\":\"plan\"}".to_string())
        );
    }

    #[test]
    fn accepted_response_is_none_for_unknown_run() {
        let (_d, c) = open_tmp();
        assert_eq!(accepted_response(&c, "no-such-run").unwrap(), None);
    }
}
