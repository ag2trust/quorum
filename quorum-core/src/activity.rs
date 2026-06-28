//! Optional PostToolUse activity hook (issue #101) — EXPERIMENTAL, stats-only.
//!
//! Bridges Claude Code session UUIDs to the agent's creative name (e.g.
//! `Velcro-m4D`) so a `PostToolUse` hook firing with only a `session_id` can be
//! resolved into a meaningful agent identity for the operator dashboard.
//!
//! **Load-bearing invariant: this module is parallel + observational only.**
//! No other code path (claim/routing/sign-off/retirement) reads
//! `agent_sessions` or `activity_events`. The hook is opt-in; absence or
//! failure changes nothing. Owner direction (#101): "stats collection only,
//! no workflow impact." A registration call that errors is silently dropped at
//! the CLI boundary (`activity` subcommand is fail-open per the issue).
//!
//! ## Tables
//! - `agent_sessions(session_id PK, agent_name, registered_at, expires_at)` —
//!   TTL'd map from Claude session UUID → agent name.
//! - `activity_events(seq, ts, session_id, agent_name, tool, expires_at)` —
//!   TTL'd per-tool-use event log. `agent_name` is resolved at insert time;
//!   NULL when the session isn't registered yet (the row is still recorded so
//!   late-registration retroactive resolution stays possible).
//!
//! ## TTLs
//! - Sessions: 48h (a long-running session can stay registered through the
//!   full work day; expired sessions just stop resolving — the activity row
//!   still inserts with `agent_name=NULL`).
//! - Activity events: 24h (stats are recency-biased; older rows are dropped).

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

/// How long a `session_id → agent_name` registration is honored. 48h covers a
/// full work day plus overnight; long enough that hub-onboard only needs to
/// register once per session, short enough that stale rows don't accumulate.
pub const SESSION_TTL_SECS: i64 = 48 * 60 * 60;

/// How long an activity event row is kept. Stats are recency-biased — the
/// dashboard cares about "what tool did agent X last use, and how recently" —
/// so a 24h window is plenty. Older rows are reaped by the standard sweeper.
pub const ACTIVITY_TTL_SECS: i64 = 24 * 60 * 60;

/// Per-agent activity summary surfaced in the `quorum status` experimental
/// section. One row per agent that has fired the hook in the last
/// `ACTIVITY_TTL_SECS`. `agent_name=None` aggregates all unresolved
/// (session-not-registered) events under a single "unknown" bucket.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ActivityView {
    /// `Some(name)` for resolved rows, `None` for "unknown" (unresolved bucket).
    pub agent_name: Option<String>,
    pub events_in_window: i64,
    /// Seconds since the most-recent tool-use event for this agent.
    pub last_tool_age_secs: i64,
    /// Name of the last tool used (for at-a-glance "what was the agent doing").
    pub last_tool: String,
}

/// Register a Claude session UUID against an agent name. Idempotent on
/// (`session_id`): re-registration extends the TTL and updates the name if it
/// changed (an agent renaming mid-session — rare but legal).
pub fn register_session(
    conn: &Connection,
    session_id: &str,
    agent_name: &str,
    now: i64,
) -> Result<()> {
    let expires_at = now + SESSION_TTL_SECS;
    conn.execute(
        "INSERT INTO agent_sessions(session_id, agent_name, registered_at, expires_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(session_id) DO UPDATE SET
             agent_name = excluded.agent_name,
             expires_at = excluded.expires_at",
        params![session_id, agent_name, now, expires_at],
    )?;
    Ok(())
}

/// Record one tool-use event. Resolves `session_id → agent_name` against the
/// `agent_sessions` table; if the session isn't registered (or its TTL has
/// lapsed), `agent_name` is stored as NULL — the row still goes in so the stats
/// surface can show an "unknown" count, and a late-registration agent can
/// retroactively map their activity by re-querying.
///
/// **Fail-open at the CLI boundary**: the `quorum activity` subcommand must
/// catch any error here and exit 0 (per the issue's hard rule "MUST NOT affect
/// any existing workflow"). The function itself returns the underlying
/// `Result` so callers / tests can observe the success/failure.
pub fn record_activity(conn: &Connection, session_id: &str, tool: &str, now: i64) -> Result<()> {
    // Resolve session → name from live (un-expired) rows only.
    let agent_name: Option<String> = conn
        .query_row(
            "SELECT agent_name FROM agent_sessions
             WHERE session_id = ?1 AND expires_at > ?2",
            params![session_id, now],
            |r| r.get(0),
        )
        .optional()?;
    let expires_at = now + ACTIVITY_TTL_SECS;
    conn.execute(
        "INSERT INTO activity_events(ts, session_id, agent_name, tool, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![now, session_id, agent_name, tool, expires_at],
    )?;
    Ok(())
}

/// Per-agent activity summary for the operator dashboard. Bounded to events in
/// the last `ACTIVITY_TTL_SECS` window (matches the TTL — older rows have been
/// reaped by `sweep`). Unresolved rows (NULL `agent_name`) aggregate under a
/// single `agent_name=None` "unknown" bucket if any exist.
///
/// Ordered by `last_tool_age_secs` ascending — most-recently-active first, the
/// natural read order for "who's doing what right now."
pub fn activity_summary(conn: &Connection, now: i64) -> Result<Vec<ActivityView>> {
    // The correlated subquery picks `last_tool` for each agent group. ORDER BY
    // ts DESC alone is ambiguous when multiple tool-uses share a `now` second
    // (hot path — hook can fire several times per CLI tick), so we tie-break on
    // `seq DESC` to honor insertion order — the *truly* last tool wins.
    let mut stmt = conn.prepare(
        "SELECT agent_name, COUNT(*) AS events, MAX(ts) AS last_ts,
                (SELECT tool FROM activity_events ae2
                 WHERE COALESCE(ae2.agent_name,'__unknown__')
                     = COALESCE(activity_events.agent_name,'__unknown__')
                   AND ae2.expires_at > ?1
                 ORDER BY ae2.ts DESC, ae2.seq DESC LIMIT 1) AS last_tool
         FROM activity_events
         WHERE expires_at > ?1
         GROUP BY COALESCE(agent_name, '__unknown__')
         ORDER BY last_ts DESC",
    )?;
    let rows = stmt
        .query_map(params![now], |r| {
            let agent_name: Option<String> = r.get(0)?;
            let events: i64 = r.get(1)?;
            let last_ts: i64 = r.get(2)?;
            let last_tool: String = r.get(3)?;
            Ok(ActivityView {
                agent_name,
                events_in_window: events,
                last_tool_age_secs: (now - last_ts).max(0),
                last_tool,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
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
    fn register_session_idempotent_renames_on_re_register() {
        let (_d, c) = open_tmp();
        register_session(&c, "sess-1", "Alpha", 100).unwrap();
        register_session(&c, "sess-1", "Bravo", 200).unwrap();
        let (name, expires): (String, i64) = c
            .query_row(
                "SELECT agent_name, expires_at FROM agent_sessions WHERE session_id = ?1",
                params!["sess-1"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(name, "Bravo", "re-register must update the name");
        assert_eq!(
            expires,
            200 + SESSION_TTL_SECS,
            "TTL must extend on re-register"
        );
    }

    #[test]
    fn record_activity_resolves_session_to_name() {
        let (_d, c) = open_tmp();
        register_session(&c, "sess-1", "Alpha", 100).unwrap();
        record_activity(&c, "sess-1", "Read", 150).unwrap();
        let agent: Option<String> = c
            .query_row(
                "SELECT agent_name FROM activity_events WHERE session_id = ?1",
                params!["sess-1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(agent.as_deref(), Some("Alpha"));
    }

    #[test]
    fn record_activity_unregistered_session_stores_null_name() {
        let (_d, c) = open_tmp();
        record_activity(&c, "sess-ghost", "Bash", 100).unwrap();
        let agent: Option<String> = c
            .query_row(
                "SELECT agent_name FROM activity_events WHERE session_id = ?1",
                params!["sess-ghost"],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            agent.is_none(),
            "unregistered session must yield NULL agent_name"
        );
    }

    #[test]
    fn record_activity_expired_session_treats_as_unregistered() {
        let (_d, c) = open_tmp();
        register_session(&c, "sess-old", "Alpha", 100).unwrap();
        // Far past the SESSION_TTL_SECS expiry.
        let now = 100 + SESSION_TTL_SECS + 1;
        record_activity(&c, "sess-old", "Read", now).unwrap();
        let agent: Option<String> = c
            .query_row(
                "SELECT agent_name FROM activity_events WHERE session_id = ?1",
                params!["sess-old"],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            agent.is_none(),
            "expired session must NOT resolve to a name"
        );
    }

    #[test]
    fn activity_summary_aggregates_per_agent_newest_first() {
        let (_d, c) = open_tmp();
        register_session(&c, "s-alpha", "Alpha", 100).unwrap();
        register_session(&c, "s-bravo", "Bravo", 100).unwrap();
        // Alpha did 3 tool-uses, last at ts=300 (Bash).
        record_activity(&c, "s-alpha", "Read", 100).unwrap();
        record_activity(&c, "s-alpha", "Edit", 200).unwrap();
        record_activity(&c, "s-alpha", "Bash", 300).unwrap();
        // Bravo did 1 tool-use at ts=500 (Read) — more recent than Alpha's last.
        record_activity(&c, "s-bravo", "Read", 500).unwrap();
        let summary = activity_summary(&c, 1000).unwrap();
        assert_eq!(summary.len(), 2);
        // Newest-first: Bravo (last=500, age=500) before Alpha (last=300, age=700).
        assert_eq!(summary[0].agent_name.as_deref(), Some("Bravo"));
        assert_eq!(summary[0].events_in_window, 1);
        assert_eq!(summary[0].last_tool, "Read");
        assert_eq!(summary[0].last_tool_age_secs, 500);
        assert_eq!(summary[1].agent_name.as_deref(), Some("Alpha"));
        assert_eq!(summary[1].events_in_window, 3);
        assert_eq!(summary[1].last_tool, "Bash");
        assert_eq!(summary[1].last_tool_age_secs, 700);
    }

    #[test]
    fn activity_summary_picks_truly_last_tool_on_same_second() {
        // Hot-path: PostToolUse hook fires several tools within the same `now`
        // second. The last_tool subquery must tiebreak on insertion order
        // (seq DESC) so the most-recently-recorded tool wins, not whichever
        // row SQLite happens to scan first.
        let (_d, c) = open_tmp();
        register_session(&c, "s", "Alpha", 100).unwrap();
        record_activity(&c, "s", "Read", 100).unwrap();
        record_activity(&c, "s", "Edit", 100).unwrap();
        record_activity(&c, "s", "Bash", 100).unwrap();
        let summary = activity_summary(&c, 200).unwrap();
        assert_eq!(summary.len(), 1);
        assert_eq!(
            summary[0].last_tool, "Bash",
            "tiebreaker must honor insertion order — Bash is the truly-last call"
        );
    }

    #[test]
    fn activity_summary_buckets_unresolved_under_single_unknown_row() {
        let (_d, c) = open_tmp();
        // Two unregistered sessions firing tools → one "unknown" row in the summary.
        record_activity(&c, "ghost-1", "Read", 100).unwrap();
        record_activity(&c, "ghost-2", "Edit", 200).unwrap();
        let summary = activity_summary(&c, 300).unwrap();
        assert_eq!(summary.len(), 1);
        assert!(summary[0].agent_name.is_none());
        assert_eq!(summary[0].events_in_window, 2);
    }

    #[test]
    fn activity_summary_drops_expired_events() {
        let (_d, c) = open_tmp();
        register_session(&c, "s-old", "Old", 100).unwrap();
        // Activity at ts=100, expires at 100 + ACTIVITY_TTL_SECS.
        record_activity(&c, "s-old", "Read", 100).unwrap();
        // Query past expiry → no rows.
        let now = 100 + ACTIVITY_TTL_SECS + 1;
        let summary = activity_summary(&c, now).unwrap();
        assert!(
            summary.is_empty(),
            "expired events must not surface in summary"
        );
    }
}
