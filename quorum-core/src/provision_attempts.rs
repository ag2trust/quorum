//! Durable reviewer-provision attempt tracking (#190).
//!
//! Each (task, PR, role) triple gets a bounded attempt budget. Attempts persist
//! across daemon restarts — the in-memory `ReviewerProvisionTracker` only tracked
//! per-instance, allowing unbounded retries after restarts.
//!
//! A new head SHA resets the budget (the prior failures were for a different diff).

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};

/// Record a provision attempt. Returns the new cumulative count.
/// If `head_sha` differs from the stored SHA, the counter resets to 1.
pub fn record_attempt(
    conn: &mut Connection,
    task_id: i64,
    pr_number: i64,
    role: &str,
    head_sha: &str,
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
            (task_id, pr_number, role, head_sha, attempts, last_attempt_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(task_id, pr_number, role) DO UPDATE SET
             head_sha = excluded.head_sha,
             attempts = excluded.attempts,
             last_attempt_at = excluded.last_attempt_at",
        params![task_id, pr_number, role, head_sha, new_count, now],
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

/// Count total reviewer agent_runs for a task.
pub fn total_reviewer_runs(conn: &Connection, task_id: i64) -> Result<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM agent_runs WHERE task_id = ?1 AND role = 'reviewer'",
        params![task_id],
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
}
