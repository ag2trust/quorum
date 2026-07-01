//! Daemon journal: crash-recovery state for in-flight agents (workers and reviewers).
//!
//! The daemon upserts on every lifecycle transition so a restart can resurrect agents
//! via `claude --resume <session-id>`. Keyed by agent name (one process per name at any
//! time). Deleted on terminal transitions (merge/cancel/fail). See spec §19.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, Row};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JournalEntry {
    pub agent: String,
    pub role: String,
    pub task_id: Option<i64>,
    pub session_id: String,
    pub worktree: Option<String>,
    pub branch: Option<String>,
    pub phase: String,
    pub expected_signal: Option<String>,
    pub cost_tokens: i64,
}

fn entry_from_row(r: &Row<'_>) -> rusqlite::Result<JournalEntry> {
    Ok(JournalEntry {
        agent: r.get(0)?,
        role: r.get(1)?,
        task_id: r.get(2)?,
        session_id: r.get(3)?,
        worktree: r.get(4)?,
        branch: r.get(5)?,
        phase: r.get(6)?,
        expected_signal: r.get(7)?,
        cost_tokens: r.get(8)?,
    })
}

pub fn upsert(conn: &mut Connection, entry: &JournalEntry) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO journal (agent, role, task_id, session_id, worktree, branch, phase, expected_signal, cost_tokens, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(agent) DO UPDATE SET
             role = excluded.role,
             task_id = excluded.task_id,
             session_id = excluded.session_id,
             worktree = excluded.worktree,
             branch = excluded.branch,
             phase = excluded.phase,
             expected_signal = excluded.expected_signal,
             cost_tokens = excluded.cost_tokens,
             updated_at = excluded.updated_at",
        params![
            entry.agent,
            entry.role,
            entry.task_id,
            entry.session_id,
            entry.worktree,
            entry.branch,
            entry.phase,
            entry.expected_signal,
            entry.cost_tokens,
            now,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn list_in_flight(conn: &Connection) -> Result<Vec<JournalEntry>> {
    let mut stmt = conn.prepare(
        "SELECT agent, role, task_id, session_id, worktree, branch, phase, expected_signal, cost_tokens
         FROM journal
         ORDER BY agent",
    )?;
    let rows = stmt.query_map([], entry_from_row)?;
    let mut result = Vec::new();
    for r in rows {
        result.push(r?);
    }
    Ok(result)
}

pub fn delete(conn: &mut Connection, agent: &str) -> Result<bool> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute("DELETE FROM journal WHERE agent = ?1", params![agent])?;
    tx.commit()?;
    Ok(changed > 0)
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

    fn sample_entry(agent: &str) -> JournalEntry {
        JournalEntry {
            agent: agent.into(),
            role: "worker".into(),
            task_id: Some(42),
            session_id: "sess-001".into(),
            worktree: Some("/tmp/wt/agent-1".into()),
            branch: Some("feat/thing".into()),
            phase: "working".into(),
            expected_signal: Some("done".into()),
            cost_tokens: 1000,
        }
    }

    #[test]
    fn upsert_list_delete() {
        let (mut conn, _dir) = test_conn();

        let entry = sample_entry("Agent-1");
        upsert(&mut conn, &entry).unwrap();

        let in_flight = list_in_flight(&conn).unwrap();
        assert_eq!(in_flight.len(), 1);
        assert_eq!(in_flight[0].agent, "Agent-1");
        assert_eq!(in_flight[0].role, "worker");
        assert_eq!(in_flight[0].task_id, Some(42));
        assert_eq!(in_flight[0].session_id, "sess-001");
        assert_eq!(in_flight[0].phase, "working");
        assert_eq!(in_flight[0].cost_tokens, 1000);

        let deleted = delete(&mut conn, "Agent-1").unwrap();
        assert!(deleted);
        let in_flight = list_in_flight(&conn).unwrap();
        assert!(in_flight.is_empty());
    }

    #[test]
    fn upsert_overwrites_existing() {
        let (mut conn, _dir) = test_conn();

        let mut entry = sample_entry("Agent-1");
        upsert(&mut conn, &entry).unwrap();

        entry.phase = "awaiting-review".into();
        entry.cost_tokens = 5000;
        upsert(&mut conn, &entry).unwrap();

        let in_flight = list_in_flight(&conn).unwrap();
        assert_eq!(in_flight.len(), 1);
        assert_eq!(in_flight[0].phase, "awaiting-review");
        assert_eq!(in_flight[0].cost_tokens, 5000);
    }

    #[test]
    fn multiple_agents() {
        let (mut conn, _dir) = test_conn();

        upsert(&mut conn, &sample_entry("Alpha")).unwrap();
        upsert(&mut conn, &sample_entry("Beta")).unwrap();

        let in_flight = list_in_flight(&conn).unwrap();
        assert_eq!(in_flight.len(), 2);
        assert_eq!(in_flight[0].agent, "Alpha");
        assert_eq!(in_flight[1].agent, "Beta");

        let deleted = delete(&mut conn, "Alpha").unwrap();
        assert!(deleted);
        let in_flight = list_in_flight(&conn).unwrap();
        assert_eq!(in_flight.len(), 1);
        assert_eq!(in_flight[0].agent, "Beta");
    }

    #[test]
    fn delete_nonexistent_returns_false() {
        let (mut conn, _dir) = test_conn();
        let deleted = delete(&mut conn, "Ghost").unwrap();
        assert!(!deleted);
    }
}
