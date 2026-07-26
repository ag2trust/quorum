//! Durable PR target persistence (#201).
//!
//! Stores the authoritative PR target tuple (head ref, head SHA, fork status)
//! resolved from GitHub. When GitHub is temporarily unavailable during
//! remediation/recovery, persisted targets prevent review-only tasks from
//! being stranded (orphan_worker_branch returns None for those).

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedPrTarget {
    pub pr_number: i64,
    pub head_ref: String,
    pub head_sha: String,
    pub is_fork: bool,
    pub resolved_at: i64,
}

/// Persist (or update) the authoritative PR target for a task.
pub fn upsert(
    conn: &mut Connection,
    task_id: i64,
    pr_number: i64,
    head_ref: &str,
    head_sha: &str,
    is_fork: bool,
) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO pr_targets (task_id, pr_number, head_ref, head_sha, is_fork, resolved_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(task_id) DO UPDATE SET
             pr_number   = excluded.pr_number,
             head_ref    = excluded.head_ref,
             head_sha    = excluded.head_sha,
             is_fork     = excluded.is_fork,
             resolved_at = excluded.resolved_at",
        params![task_id, pr_number, head_ref, head_sha, is_fork as i64, now],
    )?;
    tx.commit()?;
    Ok(())
}

/// Load a persisted target for a task, but only if it matches the expected PR.
pub fn get(conn: &Connection, task_id: i64, pr_number: i64) -> Result<Option<PersistedPrTarget>> {
    conn.query_row(
        "SELECT pr_number, head_ref, head_sha, is_fork, resolved_at
         FROM pr_targets WHERE task_id = ?1 AND pr_number = ?2",
        params![task_id, pr_number],
        |r| {
            Ok(PersistedPrTarget {
                pr_number: r.get(0)?,
                head_ref: r.get(1)?,
                head_sha: r.get(2)?,
                is_fork: r.get::<_, i64>(3)? != 0,
                resolved_at: r.get(4)?,
            })
        },
    )
    .optional()
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
    fn upsert_and_get_same_repo() {
        let (_d, mut c) = open_tmp();
        upsert(&mut c, 10, 42, "daemon/lever-t10", "abc123", false).unwrap();

        let got = get(&c, 10, 42).unwrap().unwrap();
        assert_eq!(got.pr_number, 42);
        assert_eq!(got.head_ref, "daemon/lever-t10");
        assert_eq!(got.head_sha, "abc123");
        assert!(!got.is_fork);
    }

    #[test]
    fn upsert_and_get_fork() {
        let (_d, mut c) = open_tmp();
        upsert(&mut c, 10, 42, "fix-bug", "def456", true).unwrap();

        let got = get(&c, 10, 42).unwrap().unwrap();
        assert!(got.is_fork);
        assert_eq!(got.head_ref, "fix-bug");
    }

    #[test]
    fn upsert_updates_existing() {
        let (_d, mut c) = open_tmp();
        upsert(&mut c, 10, 42, "old-branch", "sha1", false).unwrap();
        upsert(&mut c, 10, 42, "new-branch", "sha2", true).unwrap();

        let got = get(&c, 10, 42).unwrap().unwrap();
        assert_eq!(got.head_ref, "new-branch");
        assert_eq!(got.head_sha, "sha2");
        assert!(got.is_fork);
    }

    #[test]
    fn get_wrong_pr_returns_none() {
        let (_d, mut c) = open_tmp();
        upsert(&mut c, 10, 42, "branch", "sha1", false).unwrap();

        assert!(get(&c, 10, 99).unwrap().is_none());
    }

    #[test]
    fn get_missing_task_returns_none() {
        let (_d, c) = open_tmp();
        assert!(get(&c, 999, 42).unwrap().is_none());
    }

    #[test]
    fn different_tasks_independent() {
        let (_d, mut c) = open_tmp();
        upsert(&mut c, 10, 42, "branch-a", "sha-a", false).unwrap();
        upsert(&mut c, 20, 43, "branch-b", "sha-b", true).unwrap();

        let a = get(&c, 10, 42).unwrap().unwrap();
        let b = get(&c, 20, 43).unwrap().unwrap();
        assert_eq!(a.head_ref, "branch-a");
        assert_eq!(b.head_ref, "branch-b");
        assert!(!a.is_fork);
        assert!(b.is_fork);
    }

    #[test]
    fn pr_change_on_same_task_replaces() {
        let (_d, mut c) = open_tmp();
        upsert(&mut c, 10, 42, "branch-1", "sha1", false).unwrap();
        upsert(&mut c, 10, 99, "branch-2", "sha2", false).unwrap();

        assert!(get(&c, 10, 42).unwrap().is_none());
        let got = get(&c, 10, 99).unwrap().unwrap();
        assert_eq!(got.head_ref, "branch-2");
    }
}
