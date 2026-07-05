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
    pub pid: Option<i32>,
    pub pr: Option<i64>,
    pub rework_count: i32,
    /// #190: identifies the daemon instance that owns this row. Recovery filters on
    /// this so a restart never kills/reclaims a sibling instance's live workers.
    /// `None` only on pre-v16 rows from an older binary — recovery adopts them if
    /// their `worktree` lives under this instance's `worktree_base` (see
    /// `adopt_null_instance_rows`).
    pub instance_id: Option<String>,
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
        instance_id: r.get(14)?,
    })
}

pub fn upsert(conn: &mut Connection, entry: &JournalEntry) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "INSERT INTO journal (agent, role, task_id, session_id, worktree, branch, phase, cost_tokens, agent_state, cost_usd, log_dir, pid, pr, rework_count, instance_id, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
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
             instance_id = excluded.instance_id,
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
            entry.instance_id,
            now,
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn list_in_flight(conn: &Connection) -> Result<Vec<JournalEntry>> {
    let mut stmt = conn.prepare(
        "SELECT agent, role, task_id, session_id, worktree, branch, phase, cost_tokens, agent_state, cost_usd, log_dir, pid, pr, rework_count, instance_id
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

/// #190: List only journal entries owned by `instance_id`. Rows written by another
/// daemon instance are left for the owning instance's recovery to reclaim — the
/// third leg of the instance-scoping family (#181 mailbox, #178 resume, #190 recovery).
///
/// This is the ONLY read the daemon's `recover()` should use — the unscoped
/// [`list_in_flight`] is retained for `quorum status` and diagnostics.
pub fn list_in_flight_for_instance(
    conn: &Connection,
    instance_id: &str,
) -> Result<Vec<JournalEntry>> {
    let mut stmt = conn.prepare(
        "SELECT agent, role, task_id, session_id, worktree, branch, phase, cost_tokens, agent_state, cost_usd, log_dir, pid, pr, rework_count, instance_id
         FROM journal
         WHERE instance_id = ?1
         ORDER BY agent",
    )?;
    let rows = stmt.query_map(params![instance_id], entry_from_row)?;
    let mut result = Vec::new();
    for r in rows {
        result.push(r?);
    }
    Ok(result)
}

/// #190 transitional: on first startup after v15→v16 upgrade, adopt NULL-instance
/// rows whose `worktree` lives under this instance's `worktree_base`. Once adopted,
/// they participate in scoped recovery via [`list_in_flight_for_instance`]. Bounded
/// by the LIKE prefix so it can NEVER touch a sibling's rows.
///
/// Returns the number of rows adopted. Safe to call every startup: after the first
/// run this instance's rows are all stamped; NULLs left in the table belong to
/// worktrees outside our base (nothing adopted).
pub fn adopt_null_instance_rows(
    conn: &mut Connection,
    instance_id: &str,
    worktree_base: &str,
) -> Result<usize> {
    let tx = begin_immediate(conn)?;
    // Anchor the LIKE to a directory-boundary prefix so `/tmp/wt-a` never matches
    // rows whose worktree lives under `/tmp/wt-ab`. We match either
    // `<base>/<anything>` or an exact `<base>` value.
    let prefix = format!("{}/%", worktree_base.trim_end_matches('/'));
    let exact = worktree_base.trim_end_matches('/').to_string();
    let n = tx.execute(
        "UPDATE journal SET instance_id = ?1
         WHERE instance_id IS NULL
           AND worktree IS NOT NULL
           AND (worktree LIKE ?2 OR worktree = ?3)",
        params![instance_id, prefix, exact],
    )?;
    tx.commit()?;
    Ok(n)
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
            pid: None,
            pr: None,
            rework_count: 0,
            instance_id: Some("/tmp/wt".into()),
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
    fn list_in_flight_for_instance_scopes_by_instance_id() {
        // #190: sibling instances must not see each other's rows.
        let (mut conn, _dir) = test_conn();

        let mut a = sample_entry("Aardvark-a");
        a.instance_id = Some("/tmp/wt-a".into());
        a.worktree = Some("/tmp/wt-a/task-1".into());
        upsert(&mut conn, &a).unwrap();

        let mut b = sample_entry("Beluga-b");
        b.instance_id = Some("/tmp/wt-b".into());
        b.worktree = Some("/tmp/wt-b/task-2".into());
        upsert(&mut conn, &b).unwrap();

        let for_a = list_in_flight_for_instance(&conn, "/tmp/wt-a").unwrap();
        assert_eq!(for_a.len(), 1);
        assert_eq!(for_a[0].agent, "Aardvark-a");

        let for_b = list_in_flight_for_instance(&conn, "/tmp/wt-b").unwrap();
        assert_eq!(for_b.len(), 1);
        assert_eq!(for_b[0].agent, "Beluga-b");

        // Unscoped list still sees both (used by `quorum status`).
        let all = list_in_flight(&conn).unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn list_in_flight_for_instance_excludes_null_and_foreign() {
        let (mut conn, _dir) = test_conn();

        let mut ours = sample_entry("Ours");
        ours.instance_id = Some("/tmp/wt-a".into());
        upsert(&mut conn, &ours).unwrap();

        let mut null = sample_entry("Null");
        null.instance_id = None;
        upsert(&mut conn, &null).unwrap();

        let mut foreign = sample_entry("Foreign");
        foreign.instance_id = Some("/tmp/wt-b".into());
        upsert(&mut conn, &foreign).unwrap();

        let for_a = list_in_flight_for_instance(&conn, "/tmp/wt-a").unwrap();
        assert_eq!(for_a.len(), 1);
        assert_eq!(for_a[0].agent, "Ours");
    }

    #[test]
    fn adopt_null_rows_matches_worktree_prefix() {
        let (mut conn, _dir) = test_conn();

        // NULL row under our base — should be adopted.
        let mut mine = sample_entry("Mine");
        mine.instance_id = None;
        mine.worktree = Some("/tmp/wt-a/task-1".into());
        upsert(&mut conn, &mine).unwrap();

        // NULL row under a sibling's base — MUST NOT be adopted.
        let mut sibling = sample_entry("Sibling");
        sibling.instance_id = None;
        sibling.worktree = Some("/tmp/wt-b/task-2".into());
        upsert(&mut conn, &sibling).unwrap();

        // Already-stamped row — untouched.
        let mut stamped = sample_entry("Stamped");
        stamped.instance_id = Some("/tmp/other".into());
        stamped.worktree = Some("/tmp/wt-a/task-3".into());
        upsert(&mut conn, &stamped).unwrap();

        let n = adopt_null_instance_rows(&mut conn, "/tmp/wt-a", "/tmp/wt-a").unwrap();
        assert_eq!(n, 1, "only the NULL row under our base is adopted");

        let for_a = list_in_flight_for_instance(&conn, "/tmp/wt-a").unwrap();
        let agents: Vec<_> = for_a.iter().map(|e| e.agent.as_str()).collect();
        assert_eq!(agents, ["Mine"]);

        // Sibling still NULL — untouched.
        let all = list_in_flight(&conn).unwrap();
        let sibling_row = all.iter().find(|e| e.agent == "Sibling").unwrap();
        assert!(sibling_row.instance_id.is_none());
    }

    #[test]
    fn adopt_null_rows_prefix_boundary_safe() {
        // A base of "/tmp/wt-a" must not match rows under "/tmp/wt-ab".
        let (mut conn, _dir) = test_conn();

        let mut a = sample_entry("A");
        a.instance_id = None;
        a.worktree = Some("/tmp/wt-a/task".into());
        upsert(&mut conn, &a).unwrap();

        let mut ab = sample_entry("AB");
        ab.instance_id = None;
        ab.worktree = Some("/tmp/wt-ab/task".into());
        upsert(&mut conn, &ab).unwrap();

        let n = adopt_null_instance_rows(&mut conn, "/tmp/wt-a", "/tmp/wt-a").unwrap();
        assert_eq!(n, 1);
        let all = list_in_flight(&conn).unwrap();
        let ab_row = all.iter().find(|e| e.agent == "AB").unwrap();
        assert!(
            ab_row.instance_id.is_none(),
            "wt-ab must not be adopted by wt-a"
        );
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
