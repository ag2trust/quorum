//! Durable reviewer-provision attempt tracking (#190).
//!
//! Each (task, PR, role) triple gets a bounded attempt budget. Attempts persist
//! across daemon restarts — the in-memory `ReviewerProvisionTracker` only tracked
//! per-instance, allowing unbounded retries after restarts.
//!
//! A new head SHA resets the budget (the prior failures were for a different diff).
//!
//! A strike may carry the provider error that caused it. The stored error always
//! describes the most recent strike, so it survives a restart alongside the count.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};

/// Byte bound on a stored provider error.
pub const MAX_PROVIDER_ERROR_BYTES: usize = 512;

/// Record a provision attempt. Returns the new cumulative count.
/// If `head_sha` differs from the stored SHA, the counter resets to 1.
/// Clears any stored provider error: this strike has none.
pub fn record_attempt(
    conn: &mut Connection,
    task_id: i64,
    pr_number: i64,
    role: &str,
    head_sha: &str,
) -> Result<i64> {
    record_strike(conn, task_id, pr_number, role, head_sha, None)
}

/// Record a provision attempt caused by a provider failure, storing the bounded
/// `error` in the same transaction as the strike. Counter semantics match
/// [`record_attempt`]. Returns the new cumulative count.
pub fn record_provider_failure(
    conn: &mut Connection,
    task_id: i64,
    pr_number: i64,
    role: &str,
    head_sha: &str,
    error: &str,
) -> Result<i64> {
    let error = bound_provider_error(error);
    record_strike(conn, task_id, pr_number, role, head_sha, Some(&error))
}

/// Remove embedded NUL and cap at [`MAX_PROVIDER_ERROR_BYTES`], cutting only
/// between characters.
fn bound_provider_error(error: &str) -> String {
    let mut bounded = String::with_capacity(error.len().min(MAX_PROVIDER_ERROR_BYTES));
    for ch in error.chars().filter(|ch| *ch != '\0') {
        if bounded.len() + ch.len_utf8() > MAX_PROVIDER_ERROR_BYTES {
            break;
        }
        bounded.push(ch);
    }
    bounded
}

fn record_strike(
    conn: &mut Connection,
    task_id: i64,
    pr_number: i64,
    role: &str,
    head_sha: &str,
    provider_error: Option<&str>,
) -> Result<i64> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;

    let existing: Option<(i64, String)> = tx
        .query_row(
            "SELECT attempts, head_sha FROM reviewer_provision_attempts \
             WHERE task_id = ?1 AND pr_number = ?2 AND role = ?3",
            params![task_id, pr_number, role],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;

    let new_count = match existing {
        Some((count, stored_sha)) if stored_sha == head_sha => count + 1,
        _ => 1,
    };

    tx.execute(
        "INSERT INTO reviewer_provision_attempts
            (task_id, pr_number, role, head_sha, attempts, last_attempt_at,
             last_provider_error)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(task_id, pr_number, role) DO UPDATE SET
             head_sha = excluded.head_sha,
             attempts = excluded.attempts,
             last_attempt_at = excluded.last_attempt_at,
             last_provider_error = excluded.last_provider_error",
        params![
            task_id,
            pr_number,
            role,
            head_sha,
            new_count,
            now,
            provider_error
        ],
    )?;
    tx.commit()?;
    Ok(new_count)
}

/// Get current attempt count for a (task, PR, role) at a given SHA.
/// Returns 0 if no record exists or the SHA doesn't match.
pub fn get_attempts(
    conn: &Connection,
    task_id: i64,
    pr_number: i64,
    role: &str,
    head_sha: &str,
) -> Result<i64> {
    let row: Option<(i64, String)> = conn
        .query_row(
            "SELECT attempts, head_sha FROM reviewer_provision_attempts \
             WHERE task_id = ?1 AND pr_number = ?2 AND role = ?3",
            params![task_id, pr_number, role],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    match row {
        Some((count, stored_sha)) if stored_sha == head_sha => Ok(count),
        _ => Ok(0),
    }
}

/// Stored provider error for a (task, PR) whose provision budget is exhausted at
/// `head_sha`. `None` when no role has reached [`MAX_REVIEWER_PROVISION_STRIKES`]
/// at that head, or its most recent strike carried no provider error. If several
/// roles qualify, the most recently struck one wins.
pub fn exhausted_provider_error(
    conn: &Connection,
    task_id: i64,
    pr_number: i64,
    head_sha: &str,
) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT last_provider_error FROM reviewer_provision_attempts \
             WHERE task_id = ?1 AND pr_number = ?2 AND head_sha = ?3 \
               AND attempts >= ?4 AND last_provider_error IS NOT NULL \
             ORDER BY last_attempt_at DESC, role ASC LIMIT 1",
            params![task_id, pr_number, head_sha, MAX_REVIEWER_PROVISION_STRIKES],
            |r| r.get(0),
        )
        .optional()?)
}

/// Clear provision attempts for a specific (task, PR, role).
pub fn clear(conn: &mut Connection, task_id: i64, pr_number: i64, role: &str) -> Result<()> {
    let tx = begin_immediate(conn)?;
    tx.execute(
        "DELETE FROM reviewer_provision_attempts \
         WHERE task_id = ?1 AND pr_number = ?2 AND role = ?3",
        params![task_id, pr_number, role],
    )?;
    tx.commit()?;
    Ok(())
}

/// Clear all provision attempts for a task.
pub fn clear_for_task(conn: &mut Connection, task_id: i64) -> Result<()> {
    let tx = begin_immediate(conn)?;
    tx.execute(
        "DELETE FROM reviewer_provision_attempts WHERE task_id = ?1",
        params![task_id],
    )?;
    tx.commit()?;
    Ok(())
}

/// Lifetime cap on reviewer runs per task. At the cap the daemon parks the task
/// instead of spawning another reviewer.
///
/// [`total_reviewer_runs`] is the count. A `turn-error-unclassified` run that
/// ended within [`REVIEWER_NO_WORK_MAX_SECS`] with a recorded PR and head does
/// not consume it; the per-head strike bound covers those runs.
pub const MAX_TOTAL_REVIEWER_RUNS: i64 = 12;
/// Maximum `ended_at - spawned_at` span, in unix seconds, of a reviewer run
/// treated as having ended before review work. `agent_runs.spawned_at` and
/// `ended_at` are both unix seconds.
pub const REVIEWER_NO_WORK_MAX_SECS: i64 = 60;
/// Consecutive reviewer provision failures at one head SHA before the daemon stops retrying.
pub const MAX_REVIEWER_PROVISION_STRIKES: u32 = 3;

/// Highest recorded provision-attempt count for a task (any PR/role). Used by
/// read-only status to explain a task that has no live reviewer.
pub fn max_attempts_for_task(conn: &Connection, task_id: i64) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COALESCE(MAX(attempts), 0) FROM reviewer_provision_attempts WHERE task_id = ?1",
        params![task_id],
        |r| r.get(0),
    )?)
}

/// Reviewer runs that consume [`MAX_TOTAL_REVIEWER_RUNS`] for `task_id`.
///
/// Excludes a run only when it durably ended before review work: `end_reason`
/// is `turn-error-unclassified`, `ended_at` is set, `ended_at - spawned_at` is
/// at most [`REVIEWER_NO_WORK_MAX_SECS`], and both `review_pr` and
/// `review_head_sha` are set. `fail_reviewer_if_owner` lets a racing verdict
/// win, so this close cannot hold a verdict, and the head requirement keeps
/// the exemption on runs the per-head strike bound can cover. `end_reason IS`
/// is null-safe, so a missing reason still counts. Completed reviews, every
/// other end reason, live runs, slower unclassified closes, and rows missing
/// either launch column (including historical ones) still count.
pub fn total_reviewer_runs(conn: &Connection, task_id: i64) -> Result<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM agent_runs \
         WHERE task_id = ?1 AND role = 'reviewer' \
           AND NOT ( \
             end_reason IS 'turn-error-unclassified' \
             AND ended_at IS NOT NULL \
             AND ended_at - spawned_at <= ?2 \
             AND review_pr IS NOT NULL \
             AND review_head_sha IS NOT NULL \
           )",
        params![task_id, REVIEWER_NO_WORK_MAX_SECS],
        |r| r.get(0),
    )?;
    Ok(count)
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
    fn record_and_get_round_trip() {
        let (_d, mut c) = open_tmp();
        let count = record_attempt(&mut c, 10, 42, "r1", "abc123").unwrap();
        assert_eq!(count, 1);

        let got = get_attempts(&c, 10, 42, "r1", "abc123").unwrap();
        assert_eq!(got, 1);

        let count2 = record_attempt(&mut c, 10, 42, "r1", "abc123").unwrap();
        assert_eq!(count2, 2);

        let got2 = get_attempts(&c, 10, 42, "r1", "abc123").unwrap();
        assert_eq!(got2, 2);
    }

    #[test]
    fn new_sha_resets_counter() {
        let (_d, mut c) = open_tmp();
        record_attempt(&mut c, 10, 42, "r1", "sha1").unwrap();
        record_attempt(&mut c, 10, 42, "r1", "sha1").unwrap();
        assert_eq!(get_attempts(&c, 10, 42, "r1", "sha1").unwrap(), 2);

        let count = record_attempt(&mut c, 10, 42, "r1", "sha2").unwrap();
        assert_eq!(count, 1);
        assert_eq!(get_attempts(&c, 10, 42, "r1", "sha2").unwrap(), 1);
        assert_eq!(get_attempts(&c, 10, 42, "r1", "sha1").unwrap(), 0);
    }

    #[test]
    fn different_roles_independent() {
        let (_d, mut c) = open_tmp();
        record_attempt(&mut c, 10, 42, "r1", "abc").unwrap();
        record_attempt(&mut c, 10, 42, "r1", "abc").unwrap();
        record_attempt(&mut c, 10, 42, "r2", "abc").unwrap();

        assert_eq!(get_attempts(&c, 10, 42, "r1", "abc").unwrap(), 2);
        assert_eq!(get_attempts(&c, 10, 42, "r2", "abc").unwrap(), 1);
    }

    #[test]
    fn clear_removes_specific() {
        let (_d, mut c) = open_tmp();
        record_attempt(&mut c, 10, 42, "r1", "abc").unwrap();
        record_attempt(&mut c, 10, 42, "r2", "abc").unwrap();

        clear(&mut c, 10, 42, "r1").unwrap();
        assert_eq!(get_attempts(&c, 10, 42, "r1", "abc").unwrap(), 0);
        assert_eq!(get_attempts(&c, 10, 42, "r2", "abc").unwrap(), 1);
    }

    #[test]
    fn clear_for_task_removes_all() {
        let (_d, mut c) = open_tmp();
        record_attempt(&mut c, 10, 42, "r1", "abc").unwrap();
        record_attempt(&mut c, 10, 42, "r2", "abc").unwrap();
        record_attempt(&mut c, 10, 99, "r1", "def").unwrap();

        clear_for_task(&mut c, 10).unwrap();
        assert_eq!(get_attempts(&c, 10, 42, "r1", "abc").unwrap(), 0);
        assert_eq!(get_attempts(&c, 10, 42, "r2", "abc").unwrap(), 0);
        assert_eq!(get_attempts(&c, 10, 99, "r1", "def").unwrap(), 0);
    }

    #[test]
    fn missing_record_returns_zero() {
        let (_d, c) = open_tmp();
        assert_eq!(get_attempts(&c, 999, 888, "r1", "anything").unwrap(), 0);
    }

    /// Raw column read: unlike the helper, this sees errors below the strike cap.
    fn stored_error(c: &Connection, task_id: i64, pr: i64, role: &str) -> Option<String> {
        c.query_row(
            "SELECT last_provider_error FROM reviewer_provision_attempts \
             WHERE task_id = ?1 AND pr_number = ?2 AND role = ?3",
            params![task_id, pr, role],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn provider_failure_shares_the_attempt_counter() {
        let (_d, mut c) = open_tmp();
        assert_eq!(record_attempt(&mut c, 10, 42, "r1", "sha1").unwrap(), 1);
        assert_eq!(
            record_provider_failure(&mut c, 10, 42, "r1", "sha1", "boom").unwrap(),
            2
        );
        assert_eq!(get_attempts(&c, 10, 42, "r1", "sha1").unwrap(), 2);
        assert_eq!(stored_error(&c, 10, 42, "r1").as_deref(), Some("boom"));
    }

    #[test]
    fn new_head_resets_count_and_clears_prior_error() {
        let (_d, mut c) = open_tmp();
        record_provider_failure(&mut c, 10, 42, "r1", "sha1", "old").unwrap();
        record_provider_failure(&mut c, 10, 42, "r1", "sha1", "old").unwrap();

        assert_eq!(record_attempt(&mut c, 10, 42, "r1", "sha2").unwrap(), 1);
        assert_eq!(stored_error(&c, 10, 42, "r1"), None);

        record_provider_failure(&mut c, 10, 42, "r1", "sha2", "two").unwrap();
        assert_eq!(
            record_provider_failure(&mut c, 10, 42, "r1", "sha3", "three").unwrap(),
            1
        );
        assert_eq!(stored_error(&c, 10, 42, "r1").as_deref(), Some("three"));
    }

    #[test]
    fn plain_strike_clears_provider_error_at_the_same_head() {
        let (_d, mut c) = open_tmp();
        for _ in 0..MAX_REVIEWER_PROVISION_STRIKES {
            record_provider_failure(&mut c, 10, 42, "r1", "sha1", "boom").unwrap();
        }
        assert_eq!(
            exhausted_provider_error(&c, 10, 42, "sha1").unwrap(),
            Some("boom".to_string())
        );

        record_attempt(&mut c, 10, 42, "r1", "sha1").unwrap();
        assert_eq!(
            get_attempts(&c, 10, 42, "r1", "sha1").unwrap(),
            i64::from(MAX_REVIEWER_PROVISION_STRIKES) + 1
        );
        assert_eq!(stored_error(&c, 10, 42, "r1"), None);
        assert_eq!(exhausted_provider_error(&c, 10, 42, "sha1").unwrap(), None);
    }

    #[test]
    fn exhausted_error_requires_matching_head_and_the_strike_cap() {
        let (_d, mut c) = open_tmp();
        for _ in 1..MAX_REVIEWER_PROVISION_STRIKES {
            record_provider_failure(&mut c, 10, 42, "r1", "sha1", "boom").unwrap();
        }
        assert!(stored_error(&c, 10, 42, "r1").is_some());
        assert_eq!(
            exhausted_provider_error(&c, 10, 42, "sha1").unwrap(),
            None,
            "one strike below the cap stays hidden"
        );

        record_provider_failure(&mut c, 10, 42, "r1", "sha1", "boom").unwrap();
        assert_eq!(
            exhausted_provider_error(&c, 10, 42, "sha1").unwrap(),
            Some("boom".to_string())
        );
        assert_eq!(exhausted_provider_error(&c, 10, 42, "other").unwrap(), None);
        assert_eq!(exhausted_provider_error(&c, 10, 99, "sha1").unwrap(), None);
        assert_eq!(exhausted_provider_error(&c, 11, 42, "sha1").unwrap(), None);
    }

    #[test]
    fn exhausted_error_finds_any_role_and_prefers_the_latest_strike() {
        let (_d, mut c) = open_tmp();
        for _ in 0..MAX_REVIEWER_PROVISION_STRIKES {
            record_attempt(&mut c, 10, 42, "r1", "sha1").unwrap();
            record_provider_failure(&mut c, 10, 42, "r2", "sha1", "r2-error").unwrap();
        }
        assert_eq!(
            exhausted_provider_error(&c, 10, 42, "sha1").unwrap(),
            Some("r2-error".to_string()),
            "an exhausted role without an error must not mask one that has it"
        );

        for _ in 0..MAX_REVIEWER_PROVISION_STRIKES {
            record_provider_failure(&mut c, 10, 42, "r1", "sha1", "r1-error").unwrap();
        }
        c.execute(
            "UPDATE reviewer_provision_attempts SET last_attempt_at = \
             CASE role WHEN 'r1' THEN 100 ELSE 200 END",
            [],
        )
        .unwrap();
        assert_eq!(
            exhausted_provider_error(&c, 10, 42, "sha1").unwrap(),
            Some("r2-error".to_string())
        );
        c.execute(
            "UPDATE reviewer_provision_attempts SET last_attempt_at = 300 WHERE role = 'r1'",
            [],
        )
        .unwrap();
        assert_eq!(
            exhausted_provider_error(&c, 10, 42, "sha1").unwrap(),
            Some("r1-error".to_string())
        );
    }

    #[test]
    fn stored_error_truncates_on_a_utf8_boundary() {
        let (_d, mut c) = open_tmp();
        // A 4-byte scalar straddling the limit must be dropped whole.
        let error = format!("{}{}tail", "a".repeat(MAX_PROVIDER_ERROR_BYTES - 1), '😀');
        record_provider_failure(&mut c, 10, 42, "r1", "sha1", &error).unwrap();
        assert_eq!(
            stored_error(&c, 10, 42, "r1").unwrap(),
            "a".repeat(MAX_PROVIDER_ERROR_BYTES - 1)
        );

        let exact = "é".repeat(MAX_PROVIDER_ERROR_BYTES / 2);
        record_provider_failure(&mut c, 10, 42, "r1", "sha1", &exact).unwrap();
        assert_eq!(stored_error(&c, 10, 42, "r1").unwrap(), exact);

        let oversized = "x".repeat(MAX_PROVIDER_ERROR_BYTES * 4);
        record_provider_failure(&mut c, 10, 42, "r1", "sha1", &oversized).unwrap();
        assert_eq!(
            stored_error(&c, 10, 42, "r1").unwrap().len(),
            MAX_PROVIDER_ERROR_BYTES
        );
    }

    #[test]
    fn stored_error_strips_nul_and_binds_sql_text() {
        let (_d, mut c) = open_tmp();
        record_provider_failure(&mut c, 10, 42, "r1", "sha1", "a\0b\0").unwrap();
        let hex: String = c
            .query_row(
                "SELECT hex(last_provider_error) FROM reviewer_provision_attempts",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(hex, "6162", "no NUL may reach the stored bytes");

        // Stripping happens before the bound, so NUL never consumes the budget.
        let padded = format!("{}{}", "\0".repeat(MAX_PROVIDER_ERROR_BYTES), "kept");
        record_provider_failure(&mut c, 10, 42, "r1", "sha1", &padded).unwrap();
        assert_eq!(stored_error(&c, 10, 42, "r1").as_deref(), Some("kept"));

        let hostile = "'; DROP TABLE reviewer_provision_attempts; --";
        record_provider_failure(&mut c, 10, 42, "r1", "sha1", hostile).unwrap();
        assert_eq!(stored_error(&c, 10, 42, "r1").as_deref(), Some(hostile));
    }

    struct ReviewerRunKind {
        label: &'static str,
        role: &'static str,
        /// `None` leaves `ended_at` NULL. Otherwise `spawned_at + ended_offset`.
        ended_offset: Option<i64>,
        end_reason: Option<&'static str>,
        review_pr: Option<i64>,
        review_head: Option<&'static str>,
        counts: bool,
    }

    fn insert_reviewer_run(
        conn: &Connection,
        task_id: i64,
        name: &str,
        spawned_at: i64,
        kind: &ReviewerRunKind,
    ) {
        let ended_at = kind.ended_offset.map(|offset| spawned_at + offset);
        conn.execute(
            "INSERT INTO agent_runs
                (task_id, agent_name, role, model, effort, spawned_at, ended_at, end_reason,
                 review_pr, review_head_sha)
             VALUES (?1, ?2, ?3, 'm', 'e', ?4, ?5, ?6, ?7, ?8)",
            params![
                task_id,
                name,
                kind.role,
                spawned_at,
                ended_at,
                kind.end_reason,
                kind.review_pr,
                kind.review_head,
            ],
        )
        .unwrap();
    }

    #[test]
    fn total_reviewer_runs_excludes_only_short_unclassified_work_with_a_head() {
        let (_d, c) = open_tmp();
        let spawned = 1_700_000_000_i64;
        let head = "0123456789abcdef0123456789abcdef01234567";
        let unclassified = Some("turn-error-unclassified");
        let kinds = [
            ReviewerRunKind {
                label: "instant unclassified with head",
                role: "reviewer",
                ended_offset: Some(0),
                end_reason: unclassified,
                review_pr: Some(4),
                review_head: Some(head),
                counts: false,
            },
            ReviewerRunKind {
                label: "unclassified at the no-work threshold",
                role: "reviewer",
                ended_offset: Some(REVIEWER_NO_WORK_MAX_SECS),
                end_reason: unclassified,
                review_pr: Some(4),
                review_head: Some(head),
                counts: false,
            },
            ReviewerRunKind {
                label: "unclassified one second past the threshold",
                role: "reviewer",
                ended_offset: Some(REVIEWER_NO_WORK_MAX_SECS + 1),
                end_reason: unclassified,
                review_pr: Some(4),
                review_head: Some(head),
                counts: true,
            },
            ReviewerRunKind {
                label: "completed review",
                role: "reviewer",
                ended_offset: Some(1),
                end_reason: Some("completed"),
                review_pr: Some(4),
                review_head: Some(head),
                counts: true,
            },
            ReviewerRunKind {
                label: "other end reason",
                role: "reviewer",
                ended_offset: Some(0),
                end_reason: Some("continuation-unavailable"),
                review_pr: Some(4),
                review_head: Some(head),
                counts: true,
            },
            ReviewerRunKind {
                label: "live run with null ended_at",
                role: "reviewer",
                ended_offset: None,
                end_reason: None,
                review_pr: Some(4),
                review_head: Some(head),
                counts: true,
            },
            ReviewerRunKind {
                label: "unclassified reason but still live",
                role: "reviewer",
                ended_offset: None,
                end_reason: unclassified,
                review_pr: Some(4),
                review_head: Some(head),
                counts: true,
            },
            ReviewerRunKind {
                label: "short unclassified missing review_pr",
                role: "reviewer",
                ended_offset: Some(0),
                end_reason: unclassified,
                review_pr: None,
                review_head: Some(head),
                counts: true,
            },
            ReviewerRunKind {
                label: "short unclassified missing review_head_sha",
                role: "reviewer",
                ended_offset: Some(0),
                end_reason: unclassified,
                review_pr: Some(4),
                review_head: None,
                counts: true,
            },
            ReviewerRunKind {
                label: "historical unclassified row missing both launch columns",
                role: "reviewer",
                ended_offset: Some(0),
                end_reason: unclassified,
                review_pr: None,
                review_head: None,
                counts: true,
            },
            ReviewerRunKind {
                label: "ended row with a null end_reason",
                role: "reviewer",
                ended_offset: Some(0),
                end_reason: None,
                review_pr: Some(4),
                review_head: Some(head),
                counts: true,
            },
            ReviewerRunKind {
                label: "worker run",
                role: "worker",
                ended_offset: Some(0),
                end_reason: unclassified,
                review_pr: Some(4),
                review_head: Some(head),
                counts: false,
            },
        ];

        let mixed_task = 100_i64;
        let mut expected_mixed = 0_i64;
        for (index, kind) in kinds.iter().enumerate() {
            let task_id = (index as i64) + 1;
            insert_reviewer_run(&c, task_id, kind.label, spawned, kind);
            assert_eq!(
                total_reviewer_runs(&c, task_id).unwrap(),
                i64::from(kind.counts),
                "{}",
                kind.label
            );
            insert_reviewer_run(&c, mixed_task, &format!("mixed-{index}"), spawned, kind);
            if kind.counts {
                expected_mixed += 1;
            }
        }

        assert_eq!(total_reviewer_runs(&c, mixed_task).unwrap(), expected_mixed);
        let exempt = ReviewerRunKind {
            label: "burst",
            role: "reviewer",
            ended_offset: Some(0),
            end_reason: unclassified,
            review_pr: Some(4),
            review_head: Some(head),
            counts: false,
        };
        for n in 0..40 {
            insert_reviewer_run(&c, mixed_task, &format!("burst-{n}"), spawned, &exempt);
        }
        assert_eq!(
            total_reviewer_runs(&c, mixed_task).unwrap(),
            expected_mixed,
            "a burst of instant provider failures must not consume the lifetime cap"
        );
        assert!(expected_mixed < MAX_TOTAL_REVIEWER_RUNS);
        assert_eq!(total_reviewer_runs(&c, 999).unwrap(), 0);
    }
}
