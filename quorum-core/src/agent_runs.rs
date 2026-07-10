//! Agent-performance capture: one row per daemon-spawned agent process.

use crate::error::Result;
use rusqlite::{params, Connection};

/// Insert a new run row at spawn time. Returns the row id.
pub fn insert(
    conn: &Connection,
    task_id: i64,
    agent_name: &str,
    role: &str,
    model: &str,
    effort: &str,
    spawned_at: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO agent_runs (task_id, agent_name, role, model, effort, spawned_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![task_id, agent_name, role, model, effort, spawned_at],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Close an open run row at teardown/terminal.
pub fn close(conn: &Connection, run_id: i64, ended_at: i64, end_reason: &str) -> Result<()> {
    conn.execute(
        "UPDATE agent_runs SET ended_at = ?1, end_reason = ?2 WHERE id = ?3",
        params![ended_at, end_reason, run_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    #[test]
    fn insert_and_close_round_trip() {
        let (_d, mut c) = open_tmp();
        let tid = crate::tasks::create(
            &mut c,
            "boss",
            "test-task",
            None,
            0,
            None,
            None,
            None,
            None,
            100,
        )
        .unwrap();
        let run_id = insert(&c, tid, "Alice", "worker", "claude-opus-4-6", "high", 100).unwrap();
        assert!(run_id > 0);

        close(&c, run_id, 200, "done").unwrap();

        let (ended, reason): (i64, String) = c
            .query_row(
                "SELECT ended_at, end_reason FROM agent_runs WHERE id = ?1",
                params![run_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(ended, 200);
        assert_eq!(reason, "done");
    }

    #[test]
    fn role_check_constraint_rejects_invalid() {
        let (_d, c) = open_tmp();
        let result = c.execute(
            "INSERT INTO agent_runs (task_id, agent_name, role, model, effort, spawned_at)
             VALUES (1, 'A', 'invalid', 'model', 'high', 100)",
            [],
        );
        assert!(result.is_err());
    }

    #[test]
    fn multiple_runs_per_task() {
        let (_d, c) = open_tmp();
        let r1 = insert(&c, 1, "Alice", "worker", "model-a", "high", 100).unwrap();
        let r2 = insert(&c, 1, "Bob", "reviewer", "model-b", "medium", 200).unwrap();
        assert_ne!(r1, r2);

        let count: i64 = c
            .query_row(
                "SELECT count(*) FROM agent_runs WHERE task_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }
}
