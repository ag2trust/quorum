//! Durable post-merge review-interpretation queue (#127).
//!
//! When a PR merges, the daemon enqueues a row here BEFORE dropping the
//! reviewer/worker slot. The row survives daemon crashes and restarts. A
//! persistent slot in the tick loop drains one job at a time, spawning a
//! headless interpreter agent; on success the findings are stored idempotently
//! and the row is deleted. Failures increment `attempts` and populate
//! `last_error` so the same row is retried on the next tick.
//!
//! Historical tasks are NOT backfilled (#157): only PRs that merge under the
//! current daemon flow get jobs. Daemon startup reports dead-lettered rows but
//! does not scan terminal tasks to infer missing jobs.
//!
//! Independence from lifecycle: this queue never mutates the task's `done`
//! status. Interpretation is post-hoc analysis; a repeated failure only leaves
//! the finding unstored — it does NOT reopen, fail, or otherwise touch the
//! completed task.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

/// Maximum retry budget for a poison job before it's demoted to the dead-letter
/// state. At this count `list_ready` excludes the row so the drain loop moves
/// on to the next pending job instead of respawning the same failing collector
/// every ~2 ticks. The row stays in the table so an operator can still see
/// `last_error` via `over_cap`; it never gets silently deleted.
pub const MAX_ATTEMPTS: i64 = 5;

/// Linear backoff base in seconds. Between retries, a job with `attempts=k`
/// waits `BACKOFF_BASE_SECS * k` seconds before it's eligible for another
/// pick. Attempts 1..5 wait 60s, 120s, 180s, 240s, 300s — ~15min total
/// before dead-lettering.
pub const BACKOFF_BASE_SECS: i64 = 60;

/// Post-enqueue grace window for a freshly-enqueued (attempts=0) job. #125's
/// `spawn_post_merge_collector` already fires an immediate collector at
/// MergeSucceeded time; without this grace the tick's Phase 7.5 drain would
/// respawn a redundant second collector for the same PR ~2s later, doubling
/// cost on every successful merge. The grace waits long enough for the
/// primary spawn to write `review_collection_runs` (success or failed) —
/// once that lands, the tick sweep deletes the queue row entirely. If no
/// run row appears after the grace (crashed / lost), the retry drain
/// takes over.
///
/// Sized above the classifier's 180s hard timeout in `serve::collector`
/// so a normally-slow (but eventually-successful) collector still resolves
/// via the primary path.
pub const INITIAL_GRACE_SECS: i64 = 240;

/// A pending or in-flight interpretation job. One row per PR (primary key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InterpretJob {
    pub pr_number: i64,
    pub task_id: i64,
    /// GitHub `owner/name` — daemons scoped to a single repo can pass None.
    pub repo: Option<String>,
    /// Interpreter version at enqueue time. Re-interpretation triggered by a
    /// version bump keeps the last-attempted version here.
    pub interpreter_version: String,
    pub attempts: i64,
    pub last_attempt_at: Option<i64>,
    pub last_error: Option<String>,
    pub created_at: i64,
}

fn row_from_sql(r: &rusqlite::Row<'_>) -> rusqlite::Result<InterpretJob> {
    Ok(InterpretJob {
        pr_number: r.get(0)?,
        task_id: r.get(1)?,
        repo: r.get(2)?,
        interpreter_version: r.get(3)?,
        attempts: r.get(4)?,
        last_attempt_at: r.get(5)?,
        last_error: r.get(6)?,
        created_at: r.get(7)?,
    })
}

/// Enqueue a job for the given PR at the given interpreter version.
/// Idempotent by primary key — a second enqueue for the same PR does NOT
/// clobber the attempt counter or last_error, only refreshes task_id, repo,
/// and the interpreter version (a version bump legitimately re-targets the
/// same PR).
pub fn enqueue(
    conn: &mut Connection,
    pr_number: i64,
    task_id: i64,
    repo: Option<&str>,
    interpreter_version: &str,
) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO review_interpret_jobs
            (pr_number, task_id, repo, interpreter_version, attempts,
             last_attempt_at, last_error, created_at)
         VALUES (?1, ?2, ?3, ?4, 0, NULL, NULL, ?5)
         ON CONFLICT(pr_number) DO UPDATE SET
             task_id = excluded.task_id,
             repo = excluded.repo,
             interpreter_version = excluded.interpreter_version",
        params![pr_number, task_id, repo, interpreter_version, now],
    )?;
    tx.commit()?;
    Ok(())
}

/// List every job row (pending + dead-lettered), oldest first. Used by the
/// tick sweep to check per-PR run status; the drain uses [`list_ready`].
pub fn list_all(conn: &Connection) -> Result<Vec<InterpretJob>> {
    let mut stmt = conn.prepare(
        "SELECT pr_number, task_id, repo, interpreter_version, attempts,
                last_attempt_at, last_error, created_at
         FROM review_interpret_jobs ORDER BY created_at, pr_number",
    )?;
    let rows = stmt.query_map([], row_from_sql)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Pick candidate jobs eligible to run *now*: below the attempt cap and past
/// their per-attempt backoff. Ordered so a poison job cannot wedge the queue:
///
/// * `attempts ASC` — a fresh (attempts=0) job is always picked before a
///   retry, so a permanently-failing head does not starve later work.
/// * `last_attempt_at ASC NULLS FIRST` — among equal-attempt rows, the
///   longest-waiting one goes next.
/// * `created_at ASC, pr_number ASC` — stable tiebreaker.
///
/// A row at [`MAX_ATTEMPTS`] is EXCLUDED (see `over_cap`). This is the
/// load-bearing fail-safe: without the exclusion the daemon respawns the
/// same failing collector every ~2 ticks indefinitely.
pub fn list_ready(conn: &Connection, now_unix: i64) -> Result<Vec<InterpretJob>> {
    // The eligibility clause reads as:
    //  - Never past the retry cap (attempts < MAX_ATTEMPTS).
    //  - A never-attempted (attempts=0) job waits INITIAL_GRACE_SECS after
    //    created_at, so the primary #125 spawn has time to write its run
    //    record. Without this the drain would immediately spawn a redundant
    //    second collector for every merged PR.
    //  - A retried (attempts>0) job waits BACKOFF_BASE_SECS * attempts after
    //    its last attempt — linear backoff.
    let mut stmt = conn.prepare(
        "SELECT pr_number, task_id, repo, interpreter_version, attempts,
                last_attempt_at, last_error, created_at
         FROM review_interpret_jobs
         WHERE attempts < ?1
           AND (
                (attempts = 0 AND created_at + ?4 <= ?3)
                OR (attempts > 0 AND (last_attempt_at IS NULL
                                      OR last_attempt_at + (?2 * attempts) <= ?3))
           )
         ORDER BY attempts ASC, last_attempt_at ASC, created_at ASC, pr_number ASC",
    )?;
    let rows = stmt.query_map(
        params![
            MAX_ATTEMPTS,
            BACKOFF_BASE_SECS,
            now_unix,
            INITIAL_GRACE_SECS,
        ],
        row_from_sql,
    )?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Dead-letter view: rows at [`MAX_ATTEMPTS`]. Excluded from `list_ready` so
/// they don't burn agent spawns; surfaced at startup / tick so operators see
/// the poison PR instead of silent starvation. Never mutates state.
pub fn over_cap(conn: &Connection) -> Result<Vec<InterpretJob>> {
    let mut stmt = conn.prepare(
        "SELECT pr_number, task_id, repo, interpreter_version, attempts,
                last_attempt_at, last_error, created_at
         FROM review_interpret_jobs
         WHERE attempts >= ?1
         ORDER BY last_attempt_at DESC, pr_number",
    )?;
    let rows = stmt.query_map(params![MAX_ATTEMPTS], row_from_sql)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Fetch one job by PR, if present.
pub fn get(conn: &Connection, pr_number: i64) -> Result<Option<InterpretJob>> {
    let row = conn
        .query_row(
            "SELECT pr_number, task_id, repo, interpreter_version, attempts,
                    last_attempt_at, last_error, created_at
             FROM review_interpret_jobs WHERE pr_number = ?1",
            params![pr_number],
            row_from_sql,
        )
        .optional()?;
    Ok(row)
}

/// Record an attempt (success or failure): increment `attempts`, stamp
/// `last_attempt_at`, store `last_error`. Never deletes the row —
/// `list_ready` stops handing it out once `attempts` reaches [`MAX_ATTEMPTS`]
/// (dead-lettered — still visible via `over_cap`).
///
/// Returns the new attempt count so the caller can emit a loud dead-letter
/// log at the exact tick the row crosses the cap. Also used as a
/// pre-spawn "reservation" — the tick that spawns a retry marks the attempt
/// first, so the same job cannot be picked twice in one tick if the
/// collector races ahead of its run-record write.
pub fn mark_error(conn: &mut Connection, pr_number: i64, error: &str) -> Result<i64> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "UPDATE review_interpret_jobs
         SET attempts = attempts + 1,
             last_attempt_at = ?2,
             last_error = ?3
         WHERE pr_number = ?1",
        params![pr_number, now, error],
    )?;
    let attempts: i64 = tx
        .query_row(
            "SELECT attempts FROM review_interpret_jobs WHERE pr_number = ?1",
            params![pr_number],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    tx.commit()?;
    Ok(attempts)
}

/// Delete a completed job. Called after findings are durably stored.
pub fn delete(conn: &mut Connection, pr_number: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let n = tx.execute(
        "DELETE FROM review_interpret_jobs WHERE pr_number = ?1",
        params![pr_number],
    )?;
    tx.commit()?;
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::review_findings::{self, ReviewFinding};

    const TEST_VERSION: &str = "v1";

    fn test_conn() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open(&dir.path().join("q.db")).unwrap();
        (conn, dir)
    }

    fn sample_finding(pr: i64) -> ReviewFinding {
        ReviewFinding {
            id: 0,
            pr_number: pr,
            task_id: Some(50),
            reviewer: "rev".into(),
            kind: "blocking".into(),
            author_pushback: false,
            pushback_accepted: None,
            severity: Some("major".into()),
            text: "example".into(),
            source_endpoint: "pulls".into(),
            addressed_status: None,
            evidence: Vec::new(),
            collector_model: Some("haiku-4.5".into()),
            collector_version: Some(TEST_VERSION.into()),
        }
    }

    fn seed_done_task_with_pr(conn: &mut Connection, task_id: i64, pr: i64) {
        let refs = format!(r#"{{"pr": {pr}}}"#);
        conn.execute(
            "INSERT INTO tasks (id, title, status, priority, created_by, created_at, updated_at, refs)
             VALUES (?1, ?2, 'done', 50, 'boss', 1000, 1001, ?3)",
            params![task_id, format!("task-{task_id}"), refs],
        )
        .unwrap();
    }

    #[test]
    fn enqueue_get_delete_roundtrip() {
        let (mut conn, _dir) = test_conn();
        assert!(get(&conn, 500).unwrap().is_none());
        enqueue(&mut conn, 500, 42, Some("ag2trust/quorum"), "v1").unwrap();
        let got = get(&conn, 500).unwrap().unwrap();
        assert_eq!(got.pr_number, 500);
        assert_eq!(got.task_id, 42);
        assert_eq!(got.repo.as_deref(), Some("ag2trust/quorum"));
        assert_eq!(got.interpreter_version, "v1");
        assert_eq!(got.attempts, 0);
        assert!(delete(&mut conn, 500).unwrap());
        assert!(get(&conn, 500).unwrap().is_none());
    }

    #[test]
    fn enqueue_is_idempotent_on_duplicate() {
        let (mut conn, _dir) = test_conn();
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();
        mark_error(&mut conn, 500, "first attempt failed").unwrap();
        // Re-enqueue must NOT reset attempts/last_error — a merge-hook double-fire
        // does not erase the failure-observability we've built up.
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();
        let got = get(&conn, 500).unwrap().unwrap();
        assert_eq!(got.attempts, 1);
        assert_eq!(got.last_error.as_deref(), Some("first attempt failed"));
    }

    #[test]
    fn mark_error_increments_attempts() {
        let (mut conn, _dir) = test_conn();
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();
        mark_error(&mut conn, 500, "haiku timeout").unwrap();
        mark_error(&mut conn, 500, "parse failure").unwrap();
        let got = get(&conn, 500).unwrap().unwrap();
        assert_eq!(got.attempts, 2);
        assert_eq!(got.last_error.as_deref(), Some("parse failure"));
        assert!(got.last_attempt_at.is_some());
    }

    /// Negative-path (#157): historical terminal tasks must NOT produce
    /// interpret jobs. The queue only receives rows from the live
    /// MergeSucceeded path — there is no startup scan of task history.
    #[test]
    fn historical_done_tasks_do_not_create_jobs() {
        let (mut conn, _dir) = test_conn();
        seed_done_task_with_pr(&mut conn, 100, 500);
        seed_done_task_with_pr(&mut conn, 101, 501);
        seed_done_task_with_pr(&mut conn, 102, 502);
        // No enqueue calls — simulates daemon startup with pre-existing
        // terminal tasks. The queue must remain empty.
        assert!(list_all(&conn).unwrap().is_empty());
        assert!(list_ready(&conn, 9_999_999_999).unwrap().is_empty());
        assert!(over_cap(&conn).unwrap().is_empty());
    }

    /// Acceptance (#127): re-running interpretation for the same PR does
    /// not duplicate findings — `replace_for_pr` is idempotent even under
    /// repeated success paths.
    #[test]
    fn rerun_replaces_rather_than_duplicates() {
        let (mut conn, _dir) = test_conn();
        seed_done_task_with_pr(&mut conn, 100, 500);
        review_findings::replace_for_pr(
            &mut conn,
            500,
            &[sample_finding(500), sample_finding(500)],
        )
        .unwrap();
        assert_eq!(review_findings::list_for_pr(&conn, 500).unwrap().len(), 2);
        review_findings::replace_for_pr(&mut conn, 500, &[sample_finding(500)]).unwrap();
        assert_eq!(review_findings::list_for_pr(&conn, 500).unwrap().len(), 1);
    }

    /// list_ready respects the per-attempt backoff — a job that just failed
    /// is NOT immediately eligible; after the backoff window elapses it is.
    #[test]
    fn list_ready_respects_backoff_window() {
        let (mut conn, _dir) = test_conn();
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();

        mark_error(&mut conn, 500, "boom").unwrap();
        let job = get(&conn, 500).unwrap().unwrap();
        let just_failed = job.last_attempt_at.unwrap();
        assert!(list_ready(&conn, just_failed).unwrap().is_empty());
        let mid = just_failed + BACKOFF_BASE_SECS / 2;
        assert!(list_ready(&conn, mid).unwrap().is_empty());
        let ready_at = just_failed + BACKOFF_BASE_SECS * job.attempts;
        assert_eq!(list_ready(&conn, ready_at).unwrap().len(), 1);
    }

    /// Grace window on fresh (attempts=0) jobs prevents Phase 7.5 from
    /// double-spawning the collector that #125 already fires at merge time.
    /// Before INITIAL_GRACE_SECS elapses: not ready. After: ready.
    #[test]
    fn list_ready_honors_initial_grace_for_fresh_jobs() {
        let (mut conn, _dir) = test_conn();
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();
        let created = get(&conn, 500).unwrap().unwrap().created_at;
        // Immediately after enqueue — not ready (grace window).
        assert!(list_ready(&conn, created).unwrap().is_empty());
        // Halfway through the window — still not ready.
        assert!(list_ready(&conn, created + INITIAL_GRACE_SECS / 2)
            .unwrap()
            .is_empty());
        // At the boundary — ready (crash-safety kicks in).
        assert_eq!(
            list_ready(&conn, created + INITIAL_GRACE_SECS)
                .unwrap()
                .len(),
            1
        );
    }

    /// Fail-safe: a poison job (permanently failing) must NOT wedge the queue.
    /// A fresh job enqueued after the poison is picked first — `attempts asc`
    /// dominates `created_at`.
    #[test]
    fn poison_job_does_not_starve_fresh_jobs() {
        let (mut conn, _dir) = test_conn();
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();
        for _ in 0..3 {
            mark_error(&mut conn, 500, "unparseable haiku output").unwrap();
        }
        enqueue(&mut conn, 501, 43, None, "v1").unwrap();
        // Look far into the future so both jobs are past their eligibility
        // window (backoff for poison, grace for fresh).
        let ready = list_ready(&conn, 9_999_999_999).unwrap();
        assert!(!ready.is_empty());
        assert_eq!(
            ready[0].pr_number, 501,
            "fresh job must be picked before a poison job — otherwise the queue starves"
        );
    }

    /// Fail-safe: a job at cap is EXCLUDED from list_ready but preserved for
    /// operator visibility via `over_cap`.
    #[test]
    fn attempts_at_cap_are_excluded_from_ready_but_visible_in_over_cap() {
        let (mut conn, _dir) = test_conn();
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();
        for _ in 0..MAX_ATTEMPTS {
            mark_error(&mut conn, 500, "gh api 404: PR removed").unwrap();
        }
        let job = get(&conn, 500).unwrap().unwrap();
        assert_eq!(job.attempts, MAX_ATTEMPTS);
        assert!(list_ready(&conn, 9_999_999_999).unwrap().is_empty());
        let dead = over_cap(&conn).unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].pr_number, 500);
        assert_eq!(
            dead[0].last_error.as_deref(),
            Some("gh api 404: PR removed")
        );
        assert!(get(&conn, 500).unwrap().is_some());
    }

    /// Ordering: given two attempted jobs at the same attempt count, the
    /// one with the older last_attempt_at goes first (longest-waiting served).
    #[test]
    fn list_ready_orders_equal_attempts_by_oldest_last_attempt() {
        let (mut conn, _dir) = test_conn();
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();
        enqueue(&mut conn, 501, 43, None, "v1").unwrap();
        conn.execute(
            "UPDATE review_interpret_jobs SET attempts = 1, last_attempt_at = 100
             WHERE pr_number = 500",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE review_interpret_jobs SET attempts = 1, last_attempt_at = 200
             WHERE pr_number = 501",
            [],
        )
        .unwrap();
        let ready = list_ready(&conn, 1_000_000).unwrap();
        assert_eq!(ready.len(), 2);
        assert_eq!(
            ready[0].pr_number, 500,
            "oldest last_attempt_at must be picked first (been waiting longest)"
        );
    }

    /// mark_error returns the new attempt count so the daemon can log a
    /// one-time dead-letter transition the tick a job crosses the cap.
    #[test]
    fn mark_error_returns_new_attempt_count() {
        let (mut conn, _dir) = test_conn();
        enqueue(&mut conn, 500, 42, None, "v1").unwrap();
        assert_eq!(mark_error(&mut conn, 500, "one").unwrap(), 1);
        assert_eq!(mark_error(&mut conn, 500, "two").unwrap(), 2);
    }
}
