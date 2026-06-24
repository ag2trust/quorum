//! Agent identity & presence.
//!
//! There is no `register`/`heartbeat` in v1 — an agent row is auto-created and its
//! `last_seen` bumped by [`touch`], which every write-taking command calls inside its own
//! transaction. Presence is *derived* (`online` = recently active) and display-only; it
//! never drives claim eviction (claims are lease-only).

use crate::error::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

/// An agent is `online` if it acted within this many seconds. Default; Phase 6 config overrides.
pub const ONLINE_WINDOW_SECS: i64 = 300;

/// A row in the roster view.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentView {
    pub id: String,
    pub last_seen: i64,
    pub online: bool,
}

/// Auto-create the agent (if new) and bump its `last_seen` to `now`.
///
/// Takes `&Connection`; callers holding a `Transaction` pass it directly (deref coercion),
/// so the presence bump joins the caller's atomic write. Pure reads must NOT call this.
pub fn touch(conn: &Connection, id: &str, now: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO agents(id, first_seen, last_seen) VALUES (?1, ?2, ?2)
         ON CONFLICT(id) DO UPDATE SET last_seen = excluded.last_seen",
        params![id, now],
    )?;
    Ok(())
}

/// All agents with derived online/offline, ordered by id. Read-only (no presence bump).
pub fn roster(conn: &Connection, now: i64, online_window: i64) -> Result<Vec<AgentView>> {
    let mut stmt = conn
        .prepare("SELECT id, last_seen, (?1 - last_seen) < ?2 AS online FROM agents ORDER BY id")?;
    let rows = stmt.query_map(params![now, online_window], |r| {
        Ok(AgentView {
            id: r.get(0)?,
            last_seen: r.get(1)?,
            online: r.get::<_, i64>(2)? != 0,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
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
    fn touch_creates_and_roster_shows_online() {
        let (_d, c) = open_tmp();
        touch(&c, "Alice", 1000).unwrap();
        let r = roster(&c, 1100, ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(
            r,
            vec![AgentView {
                id: "Alice".into(),
                last_seen: 1000,
                online: true
            }]
        );
    }

    #[test]
    fn stale_agent_is_offline() {
        let (_d, c) = open_tmp();
        touch(&c, "Bob", 1000).unwrap();
        // now far past the online window
        let r = roster(&c, 1000 + ONLINE_WINDOW_SECS + 1, ONLINE_WINDOW_SECS).unwrap();
        assert!(!r[0].online);
    }

    #[test]
    fn second_touch_updates_last_seen_not_first_seen() {
        let (_d, c) = open_tmp();
        touch(&c, "Cy", 1000).unwrap();
        touch(&c, "Cy", 2000).unwrap();
        let (first, last): (i64, i64) = c
            .query_row(
                "SELECT first_seen, last_seen FROM agents WHERE id='Cy'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(first, 1000);
        assert_eq!(last, 2000);
    }

    #[test]
    fn roster_empty_on_fresh_db() {
        let (_d, c) = open_tmp();
        assert!(roster(&c, 1000, ONLINE_WINDOW_SECS).unwrap().is_empty());
    }
}
