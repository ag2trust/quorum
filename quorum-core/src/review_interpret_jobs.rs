//! Durable post-merge review-interpretation queue (#127).
//!
//! When a PR merges, the daemon enqueues a row here BEFORE dropping the
//! reviewer/worker slot. The row survives daemon crashes and restarts — a
//! restart runs [`reconcile`] to catch any merged PRs that missed their
//! MergeSucceeded hook (e.g. daemon killed between merge and enqueue). A
//! persistent slot in the tick loop drains one job at a time, spawning a
//! headless interpreter agent; on success the findings are stored idempotently
//! and the row is deleted. Failures increment `attempts` and populate
//! `last_error` so the same row is retried on the next tick.
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
    let mut stmt = conn.prepare(
        "SELECT pr_number, task_id, repo, interpreter_version, attempts,
                last_attempt_at, last_error, created_at
         FROM review_interpret_jobs
         WHERE attempts < ?1
           AND (last_attempt_at IS NULL
                OR last_attempt_at + (?2 * attempts) <= ?3)
         ORDER BY attempts ASC, last_attempt_at ASC, created_at ASC, pr_number ASC",
    )?;
    let rows = stmt.query_map(
        params![MAX_ATTEMPTS, BACKOFF_BASE_SECS, now_unix],
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

/// Reconcile: find every `done` task whose `refs.pr` points at a PR that
/// LACKS a successful [`review_findings::get_run`] at the given collector
/// version, and enqueue a retry job for it (idempotent — an existing job
/// for that PR is left alone). Returns the number of newly-enqueued jobs.
///
/// This is the crash-safety backstop: if the daemon merged a PR and died
/// before its detached collector wrote its success row, the next startup
/// catches it. It also covers a version bump — a merged PR whose only run
/// is at an older version gets a fresh retry queued.
///
/// A PR whose most recent run is `failed` at the current version is treated
/// like "no successful run" — enqueue and let the drain retry with backoff.
pub fn reconcile(conn: &mut Connection, collector_version: &str) -> Result<usize> {
    let mut stmt = conn.prepare(
        "SELECT id, refs FROM tasks
         WHERE status = 'done' AND refs IS NOT NULL",
    )?;
    let rows: Vec<(i64, String)> = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut candidates: Vec<(i64, i64)> = Vec::new();
    for (task_id, refs_json) in rows {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&refs_json) {
            if let Some(pr) = v.get("pr").and_then(|p| {
                p.as_i64()
                    .or_else(|| p.as_str().and_then(|s| s.parse().ok()))
            }) {
                candidates.push((task_id, pr));
            }
        }
    }
    drop(stmt);

    let mut enqueued = 0usize;
    for (task_id, pr) in candidates {
        if let Some(run) = crate::review_findings::get_run(conn, pr)? {
            if matches!(run.status, crate::review_findings::RunStatus::Success)
                && run.collector_version == collector_version
            {
                continue;
            }
        }
        // enqueue is a no-op on existing (ON CONFLICT keeps attempts/last_error),
        // so we only count truly new inserts.
        let existed = get(conn, pr)?.is_some();
        enqueue(conn, pr, task_id, None, collector_version)?;
        if !existed {
            enqueued += 1;
        }
    }
    Ok(enqueued)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::review_findings::{self, CollectionRun, ReviewFinding, RunStatus};

    /// Version stamp used across these tests (mirrors the daemon's real
    /// `serve::collector::COLLECTOR_VERSION` but decoupled so quorum-core
    /// stays independent of the binary crate).
    const TEST_VERSION: &str = "v1";

    fn success_run(pr: i64) -> CollectionRun {
        CollectionRun {
            pr_number: pr,
            task_id: Some(50),
            status: RunStatus::Success,
            error: None,
            collector_model: "haiku-4.5".into(),
            collector_version: TEST_VERSION.into(),
            findings_count: 0,
            attempted_at: 100,
            completed_at: Some(101),
        }
    }

    fn failed_run(pr: i64) -> CollectionRun {
        CollectionRun {
            pr_number: pr,
            task_id: Some(50),
            status: RunStatus::Failed,
            error: Some("classifier timeout".into()),
            collector_model: "haiku-4.5".into(),
            collector_version: TEST_VERSION.into(),
            findings_count: 0,
            attempted_at: 100,
            completed_at: Some(101),
        }
    }

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

    #[test]
    fn reconcile_enqueues_merged_task_without_run() {
        let (mut conn, _dir) = test_conn();
        seed_done_task_with_pr(&mut conn, 100, 500);
        let n = reconcile(&mut conn, TEST_VERSION).unwrap();
        assert_eq!(n, 1);
        let job = get(&conn, 500).unwrap().unwrap();
        assert_eq!(job.task_id, 100);
        assert_eq!(job.interpreter_version, TEST_VERSION);
    }

    #[test]
    fn reconcile_skips_task_with_successful_run_at_current_version() {
        let (mut conn, _dir) = test_conn();
        seed_done_task_with_pr(&mut conn, 100, 500);
        review_findings::record_run(&conn, &success_run(500)).unwrap();
        let n = reconcile(&mut conn, TEST_VERSION).unwrap();
        assert_eq!(n, 0);
        assert!(get(&conn, 500).unwrap().is_none());
    }

    #[test]
    fn reconcile_enqueues_when_run_is_at_older_version() {
        let (mut conn, _dir) = test_conn();
        seed_done_task_with_pr(&mut conn, 100, 500);
        // A run recorded at an OLD version — reconciler must enqueue at the
        // current version because a version bump legitimately re-targets
        // the same PR.
        let mut old_run = success_run(500);
        old_run.collector_version = "old-version".into();
        review_findings::record_run(&conn, &old_run).unwrap();
        let n = reconcile(&mut conn, "new-version").unwrap();
        assert_eq!(n, 1);
        let job = get(&conn, 500).unwrap().unwrap();
        assert_eq!(job.interpreter_version, "new-version");
    }

    /// A failed run at the current version is treated like "no successful
    /// run" — the reconciler enqueues so the drain retries with backoff.
    #[test]
    fn reconcile_enqueues_when_only_run_is_failed() {
        let (mut conn, _dir) = test_conn();
        seed_done_task_with_pr(&mut conn, 100, 500);
        review_findings::record_run(&conn, &failed_run(500)).unwrap();
        let n = reconcile(&mut conn, TEST_VERSION).unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn reconcile_skips_non_done_tasks() {
        let (mut conn, _dir) = test_conn();
        // Seed an open task with refs.pr — must NOT be enqueued (interpretation
        // is post-merge only).
        conn.execute(
            "INSERT INTO tasks (id, title, status, priority, created_by, created_at, updated_at, refs)
             VALUES (100, 'open task', 'open', 50, 'boss', 1000, 1001, '{\"pr\": 500}')",
            [],
        )
        .unwrap();
        let n = reconcile(&mut conn, "v1").unwrap();
        assert_eq!(n, 0);
    }

    /// Acceptance (#127): if the daemon crashes after MergeSucceeded but
    /// BEFORE enqueue, a restart reconstructs the pending job purely from
    /// task state — no live signal required. Task `done` status is not touched.
    #[test]
    fn crash_after_merge_before_enqueue_recovers_via_reconcile() {
        let (mut conn, _dir) = test_conn();
        // Simulate the post-merge world: task is done, PR is set, but no
        // job row exists (daemon died before enqueue) and no findings yet.
        seed_done_task_with_pr(&mut conn, 100, 500);
        assert!(get(&conn, 500).unwrap().is_none());
        let status_before: String = conn
            .query_row("SELECT status FROM tasks WHERE id = 100", [], |r| r.get(0))
            .unwrap();

        // Restart-time reconcile catches the missed PR.
        let n = reconcile(&mut conn, TEST_VERSION).unwrap();
        assert_eq!(n, 1);

        // Task lifecycle is NOT mutated by reconciliation.
        let status_after: String = conn
            .query_row("SELECT status FROM tasks WHERE id = 100", [], |r| r.get(0))
            .unwrap();
        assert_eq!(status_before, status_after);
        assert_eq!(status_after, "done");
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
        let now = 1_000_000;
        assert_eq!(list_ready(&conn, now).unwrap().len(), 1);

        mark_error(&mut conn, 500, "boom").unwrap();
        let job = get(&conn, 500).unwrap().unwrap();
        let just_failed = job.last_attempt_at.unwrap();
        assert!(list_ready(&conn, just_failed).unwrap().is_empty());
        let mid = just_failed + BACKOFF_BASE_SECS / 2;
        assert!(list_ready(&conn, mid).unwrap().is_empty());
        let ready_at = just_failed + BACKOFF_BASE_SECS * job.attempts;
        assert_eq!(list_ready(&conn, ready_at).unwrap().len(), 1);
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
        let ready = list_ready(&conn, 1_000_000_000).unwrap();
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
