//! Daemon journal: crash-recovery state for in-flight agents (workers and reviewers).
//!
//! The daemon upserts on every lifecycle transition so a restart can identify stale
//! processes to kill. Keyed by agent name (one process per name at any time). Deleted
//! on terminal transitions (merge/cancel/fail). See spec §19.

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
    pub pid: Option<i32>,
    pub pr: Option<i64>,
    pub rework_count: i32,
    pub provider: Option<String>,
    pub continuation_id: Option<String>,
    pub local_branch: Option<String>,
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
        pid: r.get(11)?,
        pr: r.get(12)?,
        rework_count: r.get(13)?,
        provider: r.get(14)?,
        continuation_id: r.get(15)?,
        local_branch: r.get(16)?,
    })
}

pub fn upsert(conn: &mut Connection, entry: &JournalEntry) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO journal (agent, role, task_id, session_id, worktree, branch, phase, cost_tokens, agent_state, cost_usd, log_dir, pid, pr, rework_count, provider, continuation_id, local_branch, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
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
             pid = excluded.pid,
             pr = excluded.pr,
             rework_count = excluded.rework_count,
             provider = excluded.provider,
             continuation_id = excluded.continuation_id,
             local_branch = excluded.local_branch,
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
            entry.pid,
            entry.pr,
            entry.rework_count,
            entry.provider,
            entry.continuation_id,
            entry.local_branch,
            now,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn list_in_flight(conn: &Connection) -> Result<Vec<JournalEntry>> {
    let mut stmt = conn.prepare(
        "SELECT agent, role, task_id, session_id, worktree, branch, phase, cost_tokens, agent_state, cost_usd, log_dir, pid, pr, rework_count, provider, continuation_id, local_branch
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

pub fn delete_all(conn: &mut Connection) -> Result<usize> {
    let tx = begin_immediate(conn)?;
    let changed = tx.execute("DELETE FROM journal", [])?;
    tx.commit()?;
    Ok(changed)
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
            pid: None,
            pr: None,
            rework_count: 0,
            provider: None,
            continuation_id: None,
            local_branch: None,
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
    fn delete_all_clears_journal() {
        let (mut conn, _dir) = test_conn();

        upsert(&mut conn, &sample_entry("Alpha")).unwrap();
        upsert(&mut conn, &sample_entry("Beta")).unwrap();
        upsert(&mut conn, &sample_entry("Gamma")).unwrap();

        let count = delete_all(&mut conn).unwrap();
        assert_eq!(count, 3);

        let remaining = list_in_flight(&conn).unwrap();
        assert!(remaining.is_empty());
    }

    #[test]
    fn delete_all_empty_returns_zero() {
        let (mut conn, _dir) = test_conn();
        let count = delete_all(&mut conn).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn m7_fields_persist_and_update() {
        let (mut conn, _dir) = test_conn();

        let mut entry = sample_entry("Agent-1");
        entry.pid = Some(12345);
        entry.pr = Some(42);
        entry.rework_count = 2;
        upsert(&mut conn, &entry).unwrap();

        let entries = list_in_flight(&conn).unwrap();
        assert_eq!(entries[0].pid, Some(12345));
        assert_eq!(entries[0].pr, Some(42));
        assert_eq!(entries[0].rework_count, 2);

        entry.pid = Some(99999);
        entry.pr = None;
        entry.rework_count = 3;
        upsert(&mut conn, &entry).unwrap();

        let entries = list_in_flight(&conn).unwrap();
        assert_eq!(entries[0].pid, Some(99999));
        assert_eq!(entries[0].pr, None);
        assert_eq!(entries[0].rework_count, 3);
    }

    #[test]
    fn dormant_awaiting_review_identity_persists_and_updates() {
        let (mut conn, _dir) = test_conn();
        let mut entry = sample_entry("Dormant");
        entry.phase = "awaiting-review".into();
        entry.provider = Some("codex".into());
        entry.continuation_id = Some("thread-1".into());
        entry.local_branch = Some("daemon/dormant-t42".into());
        upsert(&mut conn, &entry).unwrap();

        let stored = list_in_flight(&conn).unwrap().pop().unwrap();
        assert_eq!(stored.provider.as_deref(), Some("codex"));
        assert_eq!(stored.continuation_id.as_deref(), Some("thread-1"));
        assert_eq!(stored.local_branch.as_deref(), Some("daemon/dormant-t42"));

        entry.continuation_id = Some("thread-2".into());
        entry.local_branch = Some("daemon/local-rework".into());
        upsert(&mut conn, &entry).unwrap();
        let stored = list_in_flight(&conn).unwrap().pop().unwrap();
        assert_eq!(stored.continuation_id.as_deref(), Some("thread-2"));
        assert_eq!(stored.local_branch.as_deref(), Some("daemon/local-rework"));
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
