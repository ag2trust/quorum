//! Storage for the planner `submit_plan` MCP tool (task #1 of the
//! planner-submit-plan-mcp batch).
//!
//! A row is created on the first `SubmitPlan` call for a planner run.
//! Acceptance is guarded so the first accepted submission stands: a second
//! `record_accepted` call for the same run reports `AlreadySubmitted` rather
//! than overwriting the stored response. Each rejected call increments
//! `rejections`; once the count reaches [`MAX_PLAN_SUBMIT_REJECTIONS`],
//! further submissions are refused without validating.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, Transaction};

/// Bound on rejected `submit_plan` calls per planner run before the endpoint
/// refuses further submissions without validating.
pub const MAX_PLAN_SUBMIT_REJECTIONS: i64 = 5;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
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
