//! Daemon journal: crash-recovery state for in-flight agents (workers and reviewers).
//!
//! The daemon upserts on every lifecycle transition so a restart can resurrect agents
//! via `claude --resume <session-id>`. Keyed by agent name (one process per name at any
//! time). Deleted on terminal transitions (merge/cancel/fail). See spec §19.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct JournalEntry {
    pub agent: String,
    pub role: String,
    pub task_id: Option<i64>,
    pub session_id: String,
    pub worktree: Option<String>,
    pub branch: Option<String>,
    pub phase: String,
    pub cost_tokens: i64,
    pub agent_state: Option<String>,
    pub cost_usd: f64,
    pub log_dir: Option<String>,
}

fn entry_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<JournalEntry> {
    Ok(JournalEntry {
        agent: r.get(0)?,
        role: r.get(1)?,
        task_id: r.get(2)?,
        session_id: r.get(3)?,
        worktree: r.get(4)?,
        branch: r.get(5)?,
        phase: r.get(6)?,
        cost_tokens: r.get(7)?,
        agent_state: r.get(8)?,
        cost_usd: r.get(9)?,
        log_dir: r.get(10)?,
    })
}

pub fn upsert(conn: &mut Connection, entry: &JournalEntry) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO journal (agent, role, task_id, session_id, worktree, branch, phase, cost_tokens, agent_state, cost_usd, log_dir, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(agent) DO UPDATE SET
             role = excluded.role,
             task_id = excluded.task_id,
             session_id = excluded.session_id,
             worktree = excluded.worktree,
             branch = excluded.branch,
             phase = excluded.phase,
             cost_tokens = excluded.cost_tokens,
             agent_state = excluded.agent_state,
             cost_usd = excluded.cost_usd,
             log_dir = excluded.log_dir,
             updated_at = excluded.updated_at",
        params![
            entry.agent,
            entry.role,
            entry.task_id,
            entry.session_id,
            entry.worktree,
            entry.branch,
            entry.phase,
            entry.cost_tokens,
            entry.agent_state,
            entry.cost_usd,
            entry.log_dir,
            now,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn list_in_flight(conn: &Connection) -> Result<Vec<JournalEntry>> {
    let mut stmt = conn.prepare(
        "SELECT agent, role, task_id, session_id, worktree, branch, phase, cost_tokens, agent_state, cost_usd, log_dir
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
            cost_tokens: 1000,
            agent_state: None,
            cost_usd: 0.0,
            log_dir: None,
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
    fn agent_state_persists_and_updates() {
        let (mut conn, _dir) = test_conn();

        let mut entry = sample_entry("Agent-1");
        entry.agent_state = Some("blocked".into());
        upsert(&mut conn, &entry).unwrap();

        let entries = list_in_flight(&conn).unwrap();
        assert_eq!(entries[0].agent_state.as_deref(), Some("blocked"));

        entry.agent_state = Some("needs-info".into());
        upsert(&mut conn, &entry).unwrap();

        let entries = list_in_flight(&conn).unwrap();
        assert_eq!(entries[0].agent_state.as_deref(), Some("needs-info"));

        entry.agent_state = None;
        upsert(&mut conn, &entry).unwrap();

        let entries = list_in_flight(&conn).unwrap();
        assert_eq!(entries[0].agent_state, None);
    }

    #[test]
    fn delete_nonexistent_returns_false() {
        let (mut conn, _dir) = test_conn();
        let deleted = delete(&mut conn, "Ghost").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn cost_tokens_accumulates_across_turns_and_rework() {
        let (mut conn, _dir) = test_conn();

        // Turn 1: initial work costs 1000 tokens
        let mut entry = sample_entry("Agent-1");
        entry.cost_tokens = 1000;
        entry.phase = "awaiting-review".into();
        upsert(&mut conn, &entry).unwrap();

        let entries = list_in_flight(&conn).unwrap();
        assert_eq!(entries[0].cost_tokens, 1000);

        // Rework: cumulative cost preserved (not reset to 0)
        entry.phase = "working".into();
        upsert(&mut conn, &entry).unwrap();

        let entries = list_in_flight(&conn).unwrap();
        assert_eq!(entries[0].cost_tokens, 1000, "rework must not reset cost");

        // Turn 2: additional 500 tokens, cumulative = 1500
        entry.cost_tokens = 1500;
        entry.phase = "awaiting-review".into();
        upsert(&mut conn, &entry).unwrap();

        let entries = list_in_flight(&conn).unwrap();
        assert_eq!(entries[0].cost_tokens, 1500);
    }
}
