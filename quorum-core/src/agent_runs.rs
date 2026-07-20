//! Agent-performance capture: one row per daemon-spawned agent process.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentRun {
    pub id: i64,
    pub agent: String,
    pub role: String,
    pub sub_role: Option<String>,
    pub model: String,
    pub effort: String,
    pub spawned_at: i64,
    pub ended_at: Option<i64>,
    pub end_reason: Option<String>,
}

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

/// Insert an R2 audit run (sub_role='r2'). Returns the row id.
pub fn insert_r2(
    conn: &Connection,
    task_id: i64,
    agent_name: &str,
    model: &str,
    effort: &str,
    spawned_at: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO agent_runs (task_id, agent_name, role, model, effort, spawned_at, sub_role)
         VALUES (?1, ?2, 'reviewer', ?3, ?4, ?5, 'r2')",
        params![task_id, agent_name, model, effort, spawned_at],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Return the model used by the first worker agent_run for a task, if any.
pub fn worker_model(conn: &Connection, task_id: i64) -> Result<Option<String>> {
    let model = conn
        .query_row(
            "SELECT model FROM agent_runs \
             WHERE task_id = ?1 AND role = 'worker' \
             ORDER BY spawned_at ASC LIMIT 1",
            params![task_id],
            |r| r.get(0),
        )
        .optional()?;
    Ok(model)
}

/// All runs for a task, ordered by id.
pub fn runs_for_task(conn: &Connection, task_id: i64) -> Result<Vec<AgentRun>> {
    let mut stmt = conn.prepare(
        "SELECT id, agent_name, role, sub_role, model, effort, spawned_at, ended_at, end_reason \
         FROM agent_runs WHERE task_id = ?1 ORDER BY id ASC",
    )?;
    let runs = stmt
        .query_map(params![task_id], |r| {
            Ok(AgentRun {
                id: r.get(0)?,
                agent: r.get(1)?,
                role: r.get(2)?,
                sub_role: r.get(3)?,
                model: r.get(4)?,
                effort: r.get(5)?,
                spawned_at: r.get(6)?,
                ended_at: r.get(7)?,
                end_reason: r.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(runs)
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

    #[test]
    fn worker_model_returns_first_worker() {
        let (_d, c) = open_tmp();
        assert_eq!(worker_model(&c, 999).unwrap(), None);

        insert(&c, 1, "Alice", "worker", "claude-opus-4-6", "high", 100).unwrap();
        insert(&c, 1, "Bob", "reviewer", "claude-opus-4-8", "medium", 200).unwrap();
        insert(&c, 1, "Carol", "worker", "claude-opus-4-7", "high", 300).unwrap();

        assert_eq!(
            worker_model(&c, 1).unwrap().as_deref(),
            Some("claude-opus-4-6")
        );
    }

    #[test]
    fn end_reason_distinguishes_cleanup_causes() {
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

        let reasons = [
            "submitted",
            "awaiting_merge",
            "idle_reaped",
            "crashed",
            "agent_failed",
            "merged",
            "done",
        ];
        let mut run_ids = Vec::new();
        for (i, &reason) in reasons.iter().enumerate() {
            let rid = insert(
                &c,
                tid,
                &format!("Agent-{i}"),
                "worker",
                "model",
                "high",
                100 + i as i64,
            )
            .unwrap();
            close(&c, rid, 200 + i as i64, reason).unwrap();
            run_ids.push(rid);
        }

        for (rid, &expected) in run_ids.iter().zip(reasons.iter()) {
            let actual: String = c
                .query_row(
                    "SELECT end_reason FROM agent_runs WHERE id = ?1",
                    params![rid],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                actual, expected,
                "run {rid} should have end_reason={expected}"
            );
        }
    }
}
