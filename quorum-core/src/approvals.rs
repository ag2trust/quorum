//! Durable, instance-independent approval records (#228, #159).
//!
//! When a reviewer posts a verdict, the daemon records it here — keyed by
//! (PR, review_role), with the `approved_head_sha` the reviewer signed off on.
//! Unlike an instance-scoped mailbox row (which a `--self-update-drain` restart
//! discards because the prior reviewer is not in the new instance's roster),
//! this record survives any restart and carries no instance identity. On
//! startup the daemon replays these records and — purely from persisted state —
//! merges the PR (both R1+R2 approved for same head), returns it to review
//! (head moved or missing verdict), or refuses it (self-review / not attested).
//!
//! Merge is authorized only when R1 is approved, the daemon-owned sampled-R2
//! decision is present for the exact task/PR/head, every role it requires is
//! approved with blocking=0 for that head, and no retained row is rejected.
//!
//! Deleted on the terminal transition (merge / demote / reject) so the table
//! only ever holds live verdict records.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Approval {
    pub pr_number: i64,
    /// Review role: "r1" or "r2".
    pub review_role: String,
    pub task_id: i64,
    /// Agent that authored the PR (the worker).
    pub author: String,
    /// Agent that produced the verdict (the reviewer).
    pub reviewer: String,
    /// Attested verdict — "approved" or "changes".
    pub verdict: String,
    /// Attested blocking-finding count (#226 contract; 0 for a real approval).
    pub blocking_count: i64,
    /// The PR head commit the reviewer reviewed — bound at record time so a
    /// later force-push auto-invalidates the approval.
    pub approved_head_sha: String,
}

fn row_from_sql(r: &rusqlite::Row<'_>) -> rusqlite::Result<Approval> {
    Ok(Approval {
        pr_number: r.get(0)?,
        review_role: r.get(1)?,
        task_id: r.get(2)?,
        author: r.get(3)?,
        reviewer: r.get(4)?,
        verdict: r.get(5)?,
        blocking_count: r.get(6)?,
        approved_head_sha: r.get(7)?,
    })
}

const SELECT_COLS: &str =
    "pr_number, review_role, task_id, author, reviewer, verdict, blocking_count, approved_head_sha";

/// Upsert the durable verdict for a (PR, role). Last writer wins (a re-review
/// after rework overwrites the prior head SHA).
pub fn record(conn: &mut Connection, a: &Approval) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO approvals
            (pr_number, review_role, task_id, author, reviewer, verdict, blocking_count, approved_head_sha, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(pr_number, review_role) DO UPDATE SET
             task_id = excluded.task_id,
             author = excluded.author,
             reviewer = excluded.reviewer,
             verdict = excluded.verdict,
             blocking_count = excluded.blocking_count,
             approved_head_sha = excluded.approved_head_sha,
             created_at = excluded.created_at",
        params![
            a.pr_number,
            a.review_role,
            a.task_id,
            a.author,
            a.reviewer,
            a.verdict,
            a.blocking_count,
            a.approved_head_sha,
            now,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

/// Fetch the durable verdict for a (PR, role), if any.
pub fn get(conn: &Connection, pr_number: i64, review_role: &str) -> Result<Option<Approval>> {
    let row = conn
        .query_row(
            &format!(
                "SELECT {SELECT_COLS} FROM approvals WHERE pr_number = ?1 AND review_role = ?2"
            ),
            params![pr_number, review_role],
            row_from_sql,
        )
        .optional()?;
    Ok(row)
}

/// Fetch both R1 and R2 verdicts for a PR (empty vec if none).
pub fn get_for_pr(conn: &Connection, pr_number: i64) -> Result<Vec<Approval>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {SELECT_COLS} FROM approvals WHERE pr_number = ?1 ORDER BY review_role"
    ))?;
    let rows = stmt.query_map(params![pr_number], row_from_sql)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Check whether dual approval is complete: both R1 and R2 approved for the
/// same head SHA. Returns the common head SHA on success.
pub fn dual_approved(conn: &Connection, pr_number: i64) -> Result<Option<String>> {
    let verdicts = get_for_pr(conn, pr_number)?;
    let r1 = verdicts.iter().find(|a| a.review_role == "r1");
    let r2 = verdicts.iter().find(|a| a.review_role == "r2");
    match (r1, r2) {
        (Some(a1), Some(a2))
            if a1.verdict == "approved"
                && a2.verdict == "approved"
                && a1.blocking_count == 0
                && a2.blocking_count == 0
                && !a1.approved_head_sha.is_empty()
                && a1.approved_head_sha == a2.approved_head_sha =>
        {
            Ok(Some(a1.approved_head_sha.clone()))
        }
        _ => Ok(None),
    }
}

/// True if any durable approval exists for the given task (any role).
pub fn has_for_task(conn: &Connection, task_id: i64) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM approvals WHERE task_id = ?1",
        params![task_id],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}

/// List all durable verdicts (recovery replays every one on startup).
pub fn list(conn: &Connection) -> Result<Vec<Approval>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {SELECT_COLS} FROM approvals ORDER BY pr_number, review_role"
    ))?;
    let rows = stmt.query_map([], row_from_sql)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Delete all durable verdicts for a PR (terminal transition). Returns the
/// number of rows removed.
pub fn delete(conn: &mut Connection, pr_number: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let n = tx.execute(
        "DELETE FROM approvals WHERE pr_number = ?1",
        params![pr_number],
    )?;
    tx.commit()?;
    Ok(n > 0)
}

/// Delete a specific (PR, role) verdict. Used when invalidating a stale
/// approval after head changes.
pub fn delete_role(conn: &mut Connection, pr_number: i64, review_role: &str) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let n = tx.execute(
        "DELETE FROM approvals WHERE pr_number = ?1 AND review_role = ?2",
        params![pr_number, review_role],
    )?;
    tx.commit()?;
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn test_conn() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open(&dir.path().join("q.db")).unwrap();
        (conn, dir)
    }

    fn sample(pr: i64) -> Approval {
        Approval {
            pr_number: pr,
            review_role: "r1".into(),
            task_id: 80,
            author: "Bellows-d11".into(),
            reviewer: "Grommet-d14".into(),
            verdict: "approved".into(),
            blocking_count: 0,
            approved_head_sha: "2c0c8336f863".into(),
        }
    }

    fn sample_r2(pr: i64) -> Approval {
        Approval {
            pr_number: pr,
            review_role: "r2".into(),
            task_id: 80,
            author: "Bellows-d11".into(),
            reviewer: "Anvil-d22".into(),
            verdict: "approved".into(),
            blocking_count: 0,
            approved_head_sha: "2c0c8336f863".into(),
        }
    }

    #[test]
    fn record_get_delete_roundtrip() {
        let (mut conn, _dir) = test_conn();
        assert!(get(&conn, 208, "r1").unwrap().is_none());

        record(&mut conn, &sample(208)).unwrap();
        let got = get(&conn, 208, "r1").unwrap().unwrap();
        assert_eq!(got, sample(208));

        assert!(delete(&mut conn, 208).unwrap());
        assert!(get(&conn, 208, "r1").unwrap().is_none());
        assert!(!delete(&mut conn, 208).unwrap());
    }

    #[test]
    fn record_upserts_on_rereview() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();

        let mut updated = sample(208);
        updated.approved_head_sha = "deadbeef".into();
        updated.reviewer = "Anvil-d22".into();
        record(&mut conn, &updated).unwrap();

        let got = get(&conn, 208, "r1").unwrap().unwrap();
        assert_eq!(got.approved_head_sha, "deadbeef");
        assert_eq!(got.reviewer, "Anvil-d22");
        assert_eq!(get_for_pr(&conn, 208).unwrap().len(), 1);
    }

    #[test]
    fn dual_review_both_roles() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        record(&mut conn, &sample_r2(208)).unwrap();

        let all = get_for_pr(&conn, 208).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].review_role, "r1");
        assert_eq!(all[1].review_role, "r2");

        let sha = dual_approved(&conn, 208).unwrap();
        assert_eq!(sha, Some("2c0c8336f863".into()));
    }

    #[test]
    fn dual_approved_fails_when_heads_differ() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        let mut r2 = sample_r2(208);
        r2.approved_head_sha = "different".into();
        record(&mut conn, &r2).unwrap();

        assert_eq!(dual_approved(&conn, 208).unwrap(), None);
    }

    #[test]
    fn dual_approved_fails_when_one_is_changes() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        let mut r2 = sample_r2(208);
        r2.verdict = "changes".into();
        r2.blocking_count = 2;
        record(&mut conn, &r2).unwrap();

        assert_eq!(dual_approved(&conn, 208).unwrap(), None);
    }

    #[test]
    fn dual_approved_fails_when_r2_missing() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        assert_eq!(dual_approved(&conn, 208).unwrap(), None);
    }

    #[test]
    fn delete_role_removes_single() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        record(&mut conn, &sample_r2(208)).unwrap();
        assert!(delete_role(&mut conn, 208, "r1").unwrap());
        assert!(get(&conn, 208, "r1").unwrap().is_none());
        assert!(get(&conn, 208, "r2").unwrap().is_some());
    }

    #[test]
    fn list_returns_all_ordered() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        record(&mut conn, &sample(84)).unwrap();
        record(&mut conn, &sample_r2(208)).unwrap();
        let all = list(&conn).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].pr_number, 84);
        assert_eq!(all[1].pr_number, 208);
        assert_eq!(all[1].review_role, "r1");
        assert_eq!(all[2].pr_number, 208);
        assert_eq!(all[2].review_role, "r2");
    }

    #[test]
    fn changes_verdict_records_blocking_count() {
        let (mut conn, _dir) = test_conn();
        let a = Approval {
            pr_number: 300,
            review_role: "r1".into(),
            task_id: 90,
            author: "Worker-w1".into(),
            reviewer: "Reviewer-r1".into(),
            verdict: "changes".into(),
            blocking_count: 3,
            approved_head_sha: String::new(),
        };
        record(&mut conn, &a).unwrap();
        let got = get(&conn, 300, "r1").unwrap().unwrap();
        assert_eq!(got.verdict, "changes");
        assert_eq!(got.blocking_count, 3);
    }

    #[test]
    fn approval_record_is_instance_independent() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        drop(conn);
        let dir2 = _dir;
        let conn2 = db::open(&dir2.path().join("q.db")).unwrap();
        assert_eq!(get(&conn2, 208, "r1").unwrap().unwrap(), sample(208));
    }
}
