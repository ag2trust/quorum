//! Durable record of daemon-created worker-work recover refs.
//!
//! Managed workers can write any ref in the shared repository, so the daemon
//! never trusts a `refs/quorum-recover/...` ref by name alone. Every ref it
//! creates is recorded here (exact name, SHA, task, kind); resume only uses a
//! recorded row whose ref still names the recorded SHA.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};

/// Text bound for agent/reason columns.
const FIELD_MAX: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverRefRow {
    pub id: i64,
    pub refname: String,
    pub task_id: i64,
    pub sha: String,
    /// `Some(parent)` for a daemon snapshot commit of uncommitted state;
    /// `None` for pinned worker commits.
    pub parent_sha: Option<String>,
    pub tracked_only: bool,
    pub agent: String,
    pub reason: String,
    pub created_at: i64,
}

pub struct NewRecoverRef<'a> {
    pub refname: &'a str,
    pub task_id: i64,
    pub sha: &'a str,
    pub parent_sha: Option<&'a str>,
    pub tracked_only: bool,
    pub agent: &'a str,
    pub reason: &'a str,
}

fn bounded(value: &str) -> &str {
    if value.len() <= FIELD_MAX {
        return value;
    }
    let mut end = FIELD_MAX;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

const COLS: &str = "id, refname, task_id, sha, parent_sha, tracked_only, agent, reason, created_at";

fn row_to(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecoverRefRow> {
    Ok(RecoverRefRow {
        id: row.get(0)?,
        refname: row.get(1)?,
        task_id: row.get(2)?,
        sha: row.get(3)?,
        parent_sha: row.get(4)?,
        tracked_only: row.get::<_, i64>(5)? != 0,
        agent: row.get(6)?,
        reason: row.get(7)?,
        created_at: row.get(8)?,
    })
}

/// Record one daemon-created ref. Idempotent on the exact (refname, sha);
/// a different SHA under an already-recorded name is refused (returns false).
pub fn record(conn: &mut Connection, new: &NewRecoverRef<'_>, now: i64) -> Result<bool> {
    let tx = crate::db::begin_immediate(conn)?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT sha FROM recover_refs WHERE refname=?1",
            params![new.refname],
            |row| row.get(0),
        )
        .optional()?;
    let recorded = match existing {
        Some(sha) => sha == new.sha,
        None => {
            tx.execute(
                "INSERT INTO recover_refs(refname, task_id, sha, kind, parent_sha, tracked_only,
                                          agent, reason, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    new.refname,
                    new.task_id,
                    new.sha,
                    if new.parent_sha.is_some() {
                        "snapshot"
                    } else {
                        "commits"
                    },
                    new.parent_sha,
                    i64::from(new.tracked_only),
                    bounded(new.agent),
                    bounded(new.reason),
                    now,
                ],
            )?;
            true
        }
    };
    tx.commit()?;
    Ok(recorded)
}

/// Recorded refs for one task, newest first.
pub fn for_task(conn: &Connection, task_id: i64) -> Result<Vec<RecoverRefRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM recover_refs WHERE task_id=?1 ORDER BY id DESC"
    ))?;
    let rows = stmt
        .query_map(params![task_id], row_to)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Rows of tasks that reached done/cancelled, oldest first, bounded.
pub fn terminal_rows(conn: &Connection, limit: i64) -> Result<Vec<RecoverRefRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {} FROM recover_refs r JOIN tasks t ON t.id = r.task_id
         WHERE t.status IN ('done', 'cancelled') ORDER BY r.id ASC LIMIT ?1",
        COLS.split(", ")
            .map(|c| format!("r.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))?;
    let rows = stmt
        .query_map(params![limit], row_to)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Delete one row by exact refname and SHA. Returns whether a row was removed.
pub fn delete(conn: &mut Connection, refname: &str, sha: &str) -> Result<bool> {
    let tx = crate::db::begin_immediate(conn)?;
    let removed = tx.execute(
        "DELETE FROM recover_refs WHERE refname=?1 AND sha=?2",
        params![refname, sha],
    )?;
    tx.commit()?;
    Ok(removed > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, conn)
    }

    fn new<'a>(refname: &'a str, task_id: i64, sha: &'a str) -> NewRecoverRef<'a> {
        NewRecoverRef {
            refname,
            task_id,
            sha,
            parent_sha: Some("p"),
            tracked_only: false,
            agent: "anvil",
            reason: "idle_reaped",
        }
    }

    #[test]
    fn record_is_idempotent_and_refuses_sha_replacement() {
        let (_d, mut conn) = open();
        assert!(record(&mut conn, &new("refs/quorum-recover/t1/1/a/r", 1, "s1"), 5).unwrap());
        assert!(record(&mut conn, &new("refs/quorum-recover/t1/1/a/r", 1, "s1"), 6).unwrap());
        assert!(!record(&mut conn, &new("refs/quorum-recover/t1/1/a/r", 1, "s2"), 7).unwrap());
        let rows = for_task(&conn, 1).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sha, "s1");
        assert_eq!(rows[0].parent_sha.as_deref(), Some("p"));
        assert!(for_task(&conn, 2).unwrap().is_empty());
    }

    #[test]
    fn for_task_is_newest_first_and_delete_is_exact() {
        let (_d, mut conn) = open();
        record(&mut conn, &new("refs/quorum-recover/t1/1/a/r", 1, "s1"), 5).unwrap();
        record(&mut conn, &new("refs/quorum-recover/t1/2/a/r", 1, "s2"), 6).unwrap();
        let rows = for_task(&conn, 1).unwrap();
        assert_eq!(rows[0].sha, "s2");
        assert!(!delete(&mut conn, "refs/quorum-recover/t1/2/a/r", "other").unwrap());
        assert!(delete(&mut conn, "refs/quorum-recover/t1/2/a/r", "s2").unwrap());
        assert_eq!(for_task(&conn, 1).unwrap().len(), 1);
    }

    #[test]
    fn schema_rejects_snapshot_without_parent() {
        let (_d, conn) = open();
        assert!(conn
            .execute(
                "INSERT INTO recover_refs(refname, task_id, sha, kind, parent_sha, agent, reason,
                                          created_at)
                 VALUES ('r', 1, 's', 'snapshot', NULL, 'a', 'r', 1)",
                [],
            )
            .is_err());
    }
}
