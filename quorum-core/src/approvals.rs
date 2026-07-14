//! Durable, instance-independent approval records (#228).
//!
//! When a reviewer posts an attested `approved` verdict, the daemon records it
//! here — keyed by PR, with the `approved_head_sha` the reviewer signed off on.
//! Unlike an instance-scoped mailbox row (which a `--self-update-drain` restart
//! discards because the prior reviewer is not in the new instance's roster),
//! this record survives any restart and carries no instance identity. On
//! startup the daemon replays these records and — purely from persisted state —
//! merges the PR (head still matches), returns it to review (head moved), or
//! refuses it (self-review / not attested). See `serve::approvals::recover`.
//!
//! Deleted on the terminal transition (merge / demote / reject) so the table
//! only ever holds live "approved, awaiting merge" records.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Approval {
    /// The approved PR number — primary key (one live approval per PR).
    pub pr_number: i64,
    /// The implementation task the PR closes — recovery closes this on merge.
    pub task_id: i64,
    /// Agent that authored the PR (the worker). Recovery refuses `reviewer == author`.
    pub author: String,
    /// Agent that produced the approved verdict (the reviewer).
    pub reviewer: String,
    /// Canonical attested verdict — always `"approved"` for a stored record.
    pub verdict: String,
    /// Attested blocking-finding count (#226 contract; `0` for a real approval).
    pub blocking_count: i64,
    /// The PR head commit the reviewer approved — bound at record time so a
    /// later force-push auto-invalidates the approval.
    pub approved_head_sha: String,
}

fn row_from_sql(r: &rusqlite::Row<'_>) -> rusqlite::Result<Approval> {
    Ok(Approval {
        pr_number: r.get(0)?,
        task_id: r.get(1)?,
        author: r.get(2)?,
        reviewer: r.get(3)?,
        verdict: r.get(4)?,
        blocking_count: r.get(5)?,
        approved_head_sha: r.get(6)?,
    })
}

/// Upsert the durable approval for a PR. Last writer wins (a re-review after
/// rework overwrites the prior head SHA).
pub fn record(conn: &mut Connection, a: &Approval) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO approvals
            (pr_number, task_id, author, reviewer, verdict, blocking_count, approved_head_sha, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(pr_number) DO UPDATE SET
             task_id = excluded.task_id,
             author = excluded.author,
             reviewer = excluded.reviewer,
             verdict = excluded.verdict,
             blocking_count = excluded.blocking_count,
             approved_head_sha = excluded.approved_head_sha,
             created_at = excluded.created_at",
        params![
            a.pr_number,
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

/// Fetch the durable approval for a PR, if any.
pub fn get(conn: &Connection, pr_number: i64) -> Result<Option<Approval>> {
    let row = conn
        .query_row(
            "SELECT pr_number, task_id, author, reviewer, verdict, blocking_count, approved_head_sha
             FROM approvals WHERE pr_number = ?1",
            params![pr_number],
            row_from_sql,
        )
        .optional()?;
    Ok(row)
}

/// List all durable approvals (recovery replays every one on startup).
pub fn list(conn: &Connection) -> Result<Vec<Approval>> {
    let mut stmt = conn.prepare(
        "SELECT pr_number, task_id, author, reviewer, verdict, blocking_count, approved_head_sha
         FROM approvals ORDER BY pr_number",
    )?;
    let rows = stmt.query_map([], row_from_sql)?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Delete the durable approval for a PR (terminal transition). Returns whether
/// a row was removed.
pub fn delete(conn: &mut Connection, pr_number: i64) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let n = tx.execute(
        "DELETE FROM approvals WHERE pr_number = ?1",
        params![pr_number],
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
            task_id: 80,
            author: "Bellows-d11".into(),
            reviewer: "Grommet-d14".into(),
            verdict: "approved".into(),
            blocking_count: 0,
            approved_head_sha: "2c0c8336f863".into(),
        }
    }

    #[test]
    fn record_get_delete_roundtrip() {
        let (mut conn, _dir) = test_conn();
        assert!(get(&conn, 208).unwrap().is_none());

        record(&mut conn, &sample(208)).unwrap();
        let got = get(&conn, 208).unwrap().unwrap();
        assert_eq!(got, sample(208));

        assert!(delete(&mut conn, 208).unwrap());
        assert!(get(&conn, 208).unwrap().is_none());
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

        let got = get(&conn, 208).unwrap().unwrap();
        assert_eq!(got.approved_head_sha, "deadbeef");
        assert_eq!(got.reviewer, "Anvil-d22");
        // Still exactly one row for the PR.
        assert_eq!(list(&conn).unwrap().len(), 1);
    }

    #[test]
    fn list_returns_all_ordered() {
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        record(&mut conn, &sample(84)).unwrap();
        let all = list(&conn).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].pr_number, 84);
        assert_eq!(all[1].pr_number, 208);
    }

    #[test]
    fn changes_verdict_records_blocking_count() {
        let (mut conn, _dir) = test_conn();
        let a = Approval {
            pr_number: 300,
            task_id: 90,
            author: "Worker-w1".into(),
            reviewer: "Reviewer-r1".into(),
            verdict: "changes".into(),
            blocking_count: 3,
            approved_head_sha: String::new(),
        };
        record(&mut conn, &a).unwrap();
        let got = get(&conn, 300).unwrap().unwrap();
        assert_eq!(got.verdict, "changes");
        assert_eq!(got.blocking_count, 3);
    }

    #[test]
    fn approval_record_is_instance_independent() {
        // The whole point of #228: the record survives with no instance
        // identity, so a restarted instance (new/empty roster) reads it back
        // unchanged. There is no instance_id column to gate on.
        let (mut conn, _dir) = test_conn();
        record(&mut conn, &sample(208)).unwrap();
        // Re-open the DB (simulates a fresh daemon process/instance).
        drop(conn);
        let dir2 = _dir;
        let conn2 = db::open(&dir2.path().join("q.db")).unwrap();
        assert_eq!(get(&conn2, 208).unwrap().unwrap(), sample(208));
    }
}
