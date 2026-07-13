//! R2 review-audit records: per-audit results from the second-reviewer shadow pass.
//!
//! Each row captures one R2 adversarial audit of an R1 verdict. Stratum key =
//! (model, effort, complexity) from the worker's agent_run + task labels. R2 runs
//! are shadow-only (do not block merges) until the blocking toggle is flipped.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ReviewAudit {
    pub id: i64,
    pub task_id: i64,
    pub pr_number: i64,
    pub r1_agent_run_id: i64,
    pub r2_agent_run_id: i64,
    pub r1_agent_name: String,
    pub r2_agent_name: String,
    pub stratum_model: String,
    pub stratum_effort: String,
    pub stratum_complexity: String,
    pub missed_count: Option<i64>,
    pub overcaught_count: Option<i64>,
    pub r2_verdict: Option<String>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
}

/// Insert a new audit row when R2 is spawned (before results are known).
/// Returns the row id.
#[allow(clippy::too_many_arguments)]
pub fn create(
    conn: &mut Connection,
    task_id: i64,
    pr_number: i64,
    r1_agent_run_id: i64,
    r2_agent_run_id: i64,
    r1_agent_name: &str,
    r2_agent_name: &str,
    stratum_model: &str,
    stratum_effort: &str,
    stratum_complexity: &str,
) -> Result<i64> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO review_audits
            (task_id, pr_number, r1_agent_run_id, r2_agent_run_id,
             r1_agent_name, r2_agent_name,
             stratum_model, stratum_effort, stratum_complexity, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            task_id,
            pr_number,
            r1_agent_run_id,
            r2_agent_run_id,
            r1_agent_name,
            r2_agent_name,
            stratum_model,
            stratum_effort,
            stratum_complexity,
            now,
        ],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(id)
}

/// Complete an audit row with R2's results.
pub fn complete(
    conn: &mut Connection,
    audit_id: i64,
    missed_count: i64,
    overcaught_count: i64,
    r2_verdict: &str,
) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "UPDATE review_audits
         SET missed_count = ?1, overcaught_count = ?2, r2_verdict = ?3, completed_at = ?4
         WHERE id = ?5",
        params![missed_count, overcaught_count, r2_verdict, now, audit_id],
    )?;
    tx.commit()?;
    Ok(())
}

/// Count completed audits per stratum. Used by the sampling policy to decide
/// whether to spawn R2.
pub fn stratum_count(
    conn: &Connection,
    model: &str,
    effort: &str,
    complexity: &str,
) -> Result<i64> {
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM review_audits
         WHERE stratum_model = ?1 AND stratum_effort = ?2 AND stratum_complexity = ?3",
        params![model, effort, complexity],
        |r| r.get(0),
    )?;
    Ok(count)
}

/// Find the audit row for a given R2 agent_run_id (used when R2 signals done).
pub fn find_by_r2_run(conn: &Connection, r2_agent_run_id: i64) -> Result<Option<i64>> {
    let id: Option<i64> = conn
        .query_row(
            "SELECT id FROM review_audits WHERE r2_agent_run_id = ?1",
            params![r2_agent_run_id],
            |r| r.get(0),
        )
        .optional()?;
    Ok(id)
}

/// Find the audit row for a given R2 agent name (used when R2 signals done
/// and we only have the agent name from the mailbox).
pub fn find_by_r2_agent(conn: &Connection, r2_agent_name: &str) -> Result<Option<(i64, i64)>> {
    let row: Option<(i64, i64)> = conn
        .query_row(
            "SELECT id, r2_agent_run_id FROM review_audits WHERE r2_agent_name = ?1 AND completed_at IS NULL",
            params![r2_agent_name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(row)
}

use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    #[test]
    fn create_and_complete_round_trip() {
        let (_d, mut c) = open_tmp();
        let audit_id = create(
            &mut c,
            1,
            100,
            10,
            20,
            "R1-agent",
            "R2-agent",
            "claude-opus-4-6",
            "high",
            "3",
        )
        .unwrap();
        assert!(audit_id > 0);

        // Before completion, missed/overcaught/verdict are NULL.
        let (missed, overcaught, verdict): (Option<i64>, Option<i64>, Option<String>) = c
            .query_row(
                "SELECT missed_count, overcaught_count, r2_verdict FROM review_audits WHERE id = ?1",
                params![audit_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert!(missed.is_none());
        assert!(overcaught.is_none());
        assert!(verdict.is_none());

        complete(&mut c, audit_id, 2, 1, "changes").unwrap();

        let (missed, overcaught, verdict): (i64, i64, String) = c
            .query_row(
                "SELECT missed_count, overcaught_count, r2_verdict FROM review_audits WHERE id = ?1",
                params![audit_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(missed, 2);
        assert_eq!(overcaught, 1);
        assert_eq!(verdict, "changes");
    }

    #[test]
    fn stratum_count_returns_all_rows() {
        let (_d, mut c) = open_tmp();
        assert_eq!(
            stratum_count(&c, "claude-opus-4-6", "high", "3").unwrap(),
            0
        );

        create(
            &mut c,
            1,
            100,
            10,
            20,
            "R1",
            "R2a",
            "claude-opus-4-6",
            "high",
            "3",
        )
        .unwrap();
        create(
            &mut c,
            2,
            101,
            11,
            21,
            "R1",
            "R2b",
            "claude-opus-4-6",
            "high",
            "3",
        )
        .unwrap();
        // Different stratum
        create(
            &mut c,
            3,
            102,
            12,
            22,
            "R1",
            "R2c",
            "claude-sonnet-5",
            "medium",
            "2",
        )
        .unwrap();

        assert_eq!(
            stratum_count(&c, "claude-opus-4-6", "high", "3").unwrap(),
            2
        );
        assert_eq!(
            stratum_count(&c, "claude-sonnet-5", "medium", "2").unwrap(),
            1
        );
    }

    #[test]
    fn find_by_r2_agent_returns_incomplete_only() {
        let (_d, mut c) = open_tmp();
        let id1 = create(
            &mut c, 1, 100, 10, 20, "R1", "R2-find", "model", "effort", "c",
        )
        .unwrap();

        let found = find_by_r2_agent(&c, "R2-find").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().0, id1);

        complete(&mut c, id1, 0, 0, "approved").unwrap();
        let found = find_by_r2_agent(&c, "R2-find").unwrap();
        assert!(found.is_none());
    }
}
