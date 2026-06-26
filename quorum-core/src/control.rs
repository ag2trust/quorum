//! Non-expiring control state — the emergency stop primitive.
//!
//! A `stop` is a "everybody halt" signal that can't TTL or get buried (unlike feed/event
//! rows which all expire by design). One row per **scope**: either `global` (every agent
//! halts) or `agent:<id>` (only that agent halts). Both can be active simultaneously — an
//! agent is stopped if **either** applies to it.
//!
//! Lifecycle is explicit: [`stop`] sets, [`resume`] clears (and atomically emits a
//! `stop_cleared` event so a halted agent that polls gets an affirmative resume signal,
//! not just an absent field). There is no TTL and no sweep path — the row stays until
//! someone explicitly resumes it. (Issue #6.)
//!
//! Stopped ≠ exited. Per the project contract (lives in the agent's instructions, not the
//! binary): on stop, an agent does no work but keeps cheaply polling for the resume signal;
//! on stop_cleared, it resumes. A process-exit stop can't self-resume.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::Serialize;

/// The `global` scope string. Targeted scopes use `agent:<id>` via [`agent_scope`].
pub const GLOBAL: &str = "global";

/// Build the scope string for a per-agent stop.
pub fn agent_scope(agent: &str) -> String {
    format!("agent:{agent}")
}

/// A live stop row.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Stop {
    /// `global` or `agent:<id>`.
    pub scope: String,
    pub reason: String,
    /// Who issued the stop.
    pub by: String,
    pub since: i64,
}

const COLS: &str = "scope, reason, by, since";

fn row_to_stop(r: &Row) -> rusqlite::Result<Stop> {
    Ok(Stop {
        scope: r.get(0)?,
        reason: r.get(1)?,
        by: r.get(2)?,
        since: r.get(3)?,
    })
}

fn scope_for(agent: Option<&str>) -> String {
    match agent {
        Some(a) => agent_scope(a),
        None => GLOBAL.to_string(),
    }
}

/// Set a stop for `scope` (`None` = global, `Some(a)` = `agent:<a>`). Idempotent: setting
/// twice on the same scope **replaces** reason/by/since — useful for refining a reason or
/// re-asserting an old stop. Returns the row that landed.
pub fn stop(
    conn: &mut Connection,
    agent: Option<&str>,
    reason: &str,
    by: &str,
    now: i64,
) -> Result<Stop> {
    let scope = scope_for(agent);
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    crate::agents::touch(&tx, by, now)?;
    tx.execute(
        "INSERT INTO control(scope, reason, by, since) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(scope) DO UPDATE SET reason=excluded.reason, by=excluded.by, since=excluded.since",
        params![scope, reason, by, now],
    )?;
    let s = tx.query_row(
        &format!("SELECT {COLS} FROM control WHERE scope=?1"),
        params![scope],
        row_to_stop,
    )?;
    tx.commit()?;
    Ok(s)
}

/// Clear the stop for `scope` (matching what [`stop`] would set). Returns the row that was
/// cleared, or `None` if no stop was active on that scope (no-op clear is not an error;
/// caller decides the surface — CLI exits 1 if there was nothing to clear).
///
/// On a successful clear, emits a `stop_cleared` event to the event log so a halted agent
/// that polls gets an affirmative resume signal (the issue's "one-shot stop_cleared"
/// requirement). The event and the delete commit atomically.
pub fn resume(
    conn: &mut Connection,
    agent: Option<&str>,
    by: &str,
    now: i64,
) -> Result<Option<Stop>> {
    let scope = scope_for(agent);
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    crate::agents::touch(&tx, by, now)?;
    let cleared = tx
        .query_row(
            &format!("SELECT {COLS} FROM control WHERE scope=?1"),
            params![scope],
            row_to_stop,
        )
        .optional()?;
    if cleared.is_some() {
        tx.execute("DELETE FROM control WHERE scope=?1", params![scope])?;
        // Emit an explicit resume signal. `subject = scope` makes it filterable via
        // `quorum log --refs global` or `quorum log --refs agent:<id>`.
        crate::events::emit(
            &tx,
            "stop_cleared",
            &scope,
            &format!("cleared by {by}"),
            now,
        )?;
    }
    tx.commit()?;
    Ok(cleared)
}

/// List every active stop (read-only).
pub fn list(conn: &Connection) -> Result<Vec<Stop>> {
    let mut stmt = conn.prepare(&format!("SELECT {COLS} FROM control ORDER BY scope ASC"))?;
    let stops = stmt
        .query_map([], row_to_stop)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(stops)
}

/// Whether `agent` is currently stopped, and if so by what. Prefers `global` over the
/// agent-specific row when both are live (callers typically only care that a halt is in
/// effect; the specific row exists for diagnosis).
///
/// Consumers in #8 (the `sync` agent-tick command) surface this as the first field of
/// the tick — an agent that learns it's stopped does nothing but keep cheap-polling for
/// the resume signal.
pub fn is_stopped(conn: &Connection, agent: &str) -> Result<Option<Stop>> {
    if let Some(g) = conn
        .query_row(
            &format!("SELECT {COLS} FROM control WHERE scope=?1"),
            params![GLOBAL],
            row_to_stop,
        )
        .optional()?
    {
        return Ok(Some(g));
    }
    let s = conn
        .query_row(
            &format!("SELECT {COLS} FROM control WHERE scope=?1"),
            params![agent_scope(agent)],
            row_to_stop,
        )
        .optional()?;
    Ok(s)
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
    fn stop_global_then_resume_clears_and_emits_event() {
        let (_d, mut c) = open_tmp();
        let s = stop(&mut c, None, "deploy in flight", "cto", 100).unwrap();
        assert_eq!(s.scope, "global");
        assert_eq!(s.reason, "deploy in flight");
        assert_eq!(s.by, "cto");
        assert_eq!(list(&c).unwrap().len(), 1);

        let cleared = resume(&mut c, None, "cto", 200).unwrap().unwrap();
        assert_eq!(cleared.scope, "global");
        // List is empty post-resume.
        assert!(list(&c).unwrap().is_empty());
        // A `stop_cleared` event was emitted to the event log on `global`.
        let evs = crate::events::list(&c, 0, Some(GLOBAL), 10, 200).unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, "stop_cleared");
        assert!(evs[0].body.contains("cto"));
    }

    #[test]
    fn stop_targeted_only_affects_named_agent() {
        let (_d, mut c) = open_tmp();
        stop(&mut c, Some("Alice"), "rate-limited", "cto", 100).unwrap();
        // Alice is stopped (by her own scope); Bob is not.
        let a = is_stopped(&c, "Alice").unwrap().unwrap();
        assert_eq!(a.scope, "agent:Alice");
        assert!(is_stopped(&c, "Bob").unwrap().is_none());
    }

    #[test]
    fn global_and_targeted_can_coexist_global_wins_on_query() {
        // Per the issue: "Targeted + global can both be active; agent is stopped if either
        // applies to it." When both apply, prefer the global (broader signal) on the
        // is_stopped query.
        let (_d, mut c) = open_tmp();
        stop(&mut c, None, "all hands halt", "cto", 100).unwrap();
        stop(&mut c, Some("Alice"), "and you specifically", "cto", 100).unwrap();
        // Alice sees the GLOBAL stop (broader signal).
        let a = is_stopped(&c, "Alice").unwrap().unwrap();
        assert_eq!(a.scope, "global");
        // Bob also sees the global stop (he has no targeted row).
        let b = is_stopped(&c, "Bob").unwrap().unwrap();
        assert_eq!(b.scope, "global");
        // Both rows visible via `list`.
        assert_eq!(list(&c).unwrap().len(), 2);
    }

    #[test]
    fn resume_matches_scope_global_does_not_clear_targeted() {
        let (_d, mut c) = open_tmp();
        stop(&mut c, None, "g", "cto", 100).unwrap();
        stop(&mut c, Some("Alice"), "a", "cto", 100).unwrap();
        // Resume global → Alice's targeted stop remains.
        resume(&mut c, None, "cto", 200).unwrap();
        let a = is_stopped(&c, "Alice").unwrap().unwrap();
        assert_eq!(a.scope, "agent:Alice");
        // Bob is no longer stopped (only the global affected him).
        assert!(is_stopped(&c, "Bob").unwrap().is_none());
    }

    #[test]
    fn resume_on_nothing_is_clean_none_no_event() {
        let (_d, mut c) = open_tmp();
        let cleared = resume(&mut c, None, "cto", 100).unwrap();
        assert!(cleared.is_none(), "no-op resume returns None");
        // No phantom stop_cleared event when nothing was cleared.
        let evs = crate::events::list(&c, 0, Some(GLOBAL), 10, 100).unwrap();
        assert!(evs.is_empty());
    }

    #[test]
    fn stop_twice_on_same_scope_replaces_reason() {
        let (_d, mut c) = open_tmp();
        let first = stop(&mut c, None, "v1", "cto-a", 100).unwrap();
        let second = stop(&mut c, None, "v2 (refined)", "cto-b", 200).unwrap();
        assert_eq!(first.scope, second.scope);
        assert_eq!(list(&c).unwrap().len(), 1, "still one row per scope");
        let cur = list(&c).unwrap().into_iter().next().unwrap();
        assert_eq!(cur.reason, "v2 (refined)");
        assert_eq!(cur.by, "cto-b");
        assert_eq!(cur.since, 200);
    }

    #[test]
    fn stop_survives_sweep_unlike_messages_and_events() {
        // Control state is non-expiring by design. The sweeper does NOT touch the control
        // table. Verify by running sweep with `now` far in the future — every expiring row
        // would be reaped, but the stop must survive.
        let (_d, mut c) = open_tmp();
        stop(&mut c, None, "indefinite halt", "cto", 100).unwrap();
        // Run sweep at a `now` past any reasonable TTL.
        let way_future = 100 + 365 * 24 * 3600;
        crate::sweep::sweep_all(&c, way_future).unwrap();
        let s = is_stopped(&c, "anyone").unwrap();
        assert!(s.is_some(), "control state must NOT be swept");
    }

    #[test]
    fn list_returns_global_first_then_agent_alphabetical() {
        // ASCII sort: 'a' (agent:...) > 'g' (global), so ORDER BY scope gives agent:Alice,
        // agent:Bob, global. The test pins the ordering so callers can render deterministically.
        let (_d, mut c) = open_tmp();
        stop(&mut c, Some("Bob"), "b", "cto", 100).unwrap();
        stop(&mut c, None, "g", "cto", 100).unwrap();
        stop(&mut c, Some("Alice"), "a", "cto", 100).unwrap();
        let stops = list(&c).unwrap();
        let scopes: Vec<&str> = stops.iter().map(|s| s.scope.as_str()).collect();
        assert_eq!(scopes, vec!["agent:Alice", "agent:Bob", "global"]);
    }
}
