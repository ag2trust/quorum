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

/// Auto-create the agent (if new), bump its `last_seen` to `now`, AND **auto-renew every
/// active lease the agent holds** (#55).
///
/// Folding lease-renew into `touch` makes "an agent that's working through quorum keeps
/// its work" automatic: every `--agent`-identified command (claim, task-claim, task-update,
/// post, read with ack, sync::tick, …) calls `touch` inside its own `BEGIN IMMEDIATE`
/// transaction, so the renew rides on the existing presence bump with **no new write
/// surface** and no extra round-trip. Only true silence (no `--agent` touch for > TTL)
/// lapses the lease → reaper returns the task to `open` (lost-agent recovery, unchanged).
///
/// **Renew shape:** `expires_at = MAX(expires_at, now + DEFAULT_LEASE_TTL_SECS)` on every
/// row matching `holder=id AND active=1 AND expires_at > now`. The `MAX` is monotonic
/// (an agent that explicitly claimed with a longer TTL keeps the longer TTL); the
/// `expires_at > now` guard explicitly excludes lapsed leases (the lost-agent path is
/// owned by the reaper, not `touch` — a returning agent does NOT silently resurrect a
/// task the reaper has reclaimed or is about to reclaim).
///
/// Takes `&Connection`; callers holding a `Transaction` pass it directly (deref coercion),
/// so both writes join the caller's atomic txn. Pure reads must NOT call this.
pub fn touch(conn: &Connection, id: &str, now: i64) -> Result<()> {
    // 1. Presence bump (always — auto-create row if first time).
    conn.execute(
        "INSERT INTO agents(id, first_seen, last_seen) VALUES (?1, ?2, ?2)
         ON CONFLICT(id) DO UPDATE SET last_seen = excluded.last_seen",
        params![id, now],
    )?;
    // 2. Auto-renew this agent's live leases (#55). Monotonic via MAX so an explicit long
    //    TTL is never shortened. Lapsed (expires_at <= now) rows are deliberately not
    //    touched — they belong to the reaper.
    conn.execute(
        "UPDATE claims
         SET expires_at = MAX(expires_at, ?2 + ?3)
         WHERE holder = ?1 AND active = 1 AND expires_at > ?2",
        params![id, now, crate::tasks::DEFAULT_LEASE_TTL_SECS],
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
            // column 2 is the derived 0/1 of `(now - last_seen) < window`, not a stored column
            online: r.get::<_, i64>(2)? != 0,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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

    // -- Auto-renew on touch (#55) --------------------------------------------------------

    /// Helper: stamp an `active=1` claim row directly (skip the higher-level `claims::claim`
    /// path so we can pin pre-touch state without entangling these tests with claim's own
    /// logic). Returns nothing — caller queries by holder/target if needed.
    fn stamp_claim(conn: &Connection, holder: &str, target: &str, expires_at: i64, now: i64) {
        conn.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1,?2,?3,?4,1)",
            params![target, holder, now, expires_at],
        )
        .unwrap();
    }

    fn lease_expires(conn: &Connection, holder: &str, target: &str) -> Option<i64> {
        conn.query_row(
            "SELECT expires_at FROM claims WHERE holder=?1 AND target=?2 AND active=1",
            params![holder, target],
            |r| r.get(0),
        )
        .ok()
    }

    #[test]
    fn touch_auto_renews_active_lease_to_now_plus_default_ttl() {
        let (_d, c) = open_tmp();
        // A holds an active lease expiring at 1100 (60s from now). After touching at now=1100
        // — wait, 1100 is exactly expiry; pick 1090 so the lease is still live (`> now`).
        stamp_claim(&c, "A", "pr#1", 1100, 1000);
        touch(&c, "A", 1090).unwrap();
        let new_exp = lease_expires(&c, "A", "pr#1").unwrap();
        assert_eq!(
            new_exp,
            1090 + crate::tasks::DEFAULT_LEASE_TTL_SECS,
            "lease must extend to now + DEFAULT_LEASE_TTL_SECS"
        );
    }

    #[test]
    fn touch_renew_is_monotonic_does_not_shorten_long_lease() {
        let (_d, c) = open_tmp();
        // A claimed `pr#1` with a 24h TTL (`expires_at = 1000 + 86400`).
        let long_exp = 1000 + 24 * 3600;
        stamp_claim(&c, "A", "pr#1", long_exp, 1000);
        // 5 min later A touches. now + DEFAULT_LEASE_TTL_SECS (= 1300 + 3600 = 4900) is
        // WAY less than 24h. Monotonic MAX must preserve the longer existing expiry.
        touch(&c, "A", 1300).unwrap();
        let new_exp = lease_expires(&c, "A", "pr#1").unwrap();
        assert_eq!(
            new_exp, long_exp,
            "monotonic MAX must keep the longer existing expiry, not shorten it"
        );
    }

    #[test]
    fn touch_does_not_resurrect_lapsed_lease() {
        let (_d, c) = open_tmp();
        // A's lease expired at 1100. now=2000 (way past). Touch must NOT extend it — the
        // reaper owns the lost-agent recovery path.
        stamp_claim(&c, "A", "pr#1", 1100, 1000);
        touch(&c, "A", 2000).unwrap();
        let new_exp = lease_expires(&c, "A", "pr#1").unwrap();
        assert_eq!(
            new_exp, 1100,
            "touch must not extend a lapsed lease (lost-agent recovery is the reaper's)"
        );
    }

    #[test]
    fn touch_only_renews_callers_own_leases() {
        let (_d, c) = open_tmp();
        // A and B each hold one live lease at 1100 (60s).
        stamp_claim(&c, "A", "pr#1", 1100, 1000);
        stamp_claim(&c, "B", "pr#2", 1100, 1000);
        // A touches at 1050 — only A's expires_at should advance.
        touch(&c, "A", 1050).unwrap();
        let a_exp = lease_expires(&c, "A", "pr#1").unwrap();
        let b_exp = lease_expires(&c, "B", "pr#2").unwrap();
        assert_eq!(
            a_exp,
            1050 + crate::tasks::DEFAULT_LEASE_TTL_SECS,
            "A's lease must extend"
        );
        assert_eq!(b_exp, 1100, "B's lease must NOT change on A's touch");
    }

    #[test]
    fn touch_does_not_renew_inactive_lease() {
        let (_d, c) = open_tmp();
        // A had an active lease that was later released (active=0). expires_at is still in
        // the future but the row is logically dead. Touch must NOT re-activate or extend it.
        c.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1,?2,?3,?4,0)",
            params!["pr#1", "A", 1000, 1100],
        )
        .unwrap();
        touch(&c, "A", 1050).unwrap();
        let exp: i64 = c
            .query_row(
                "SELECT expires_at FROM claims WHERE holder='A' AND target='pr#1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(exp, 1100, "inactive lease must stay untouched");
    }

    #[test]
    fn touch_renews_multiple_active_leases_in_one_call() {
        let (_d, c) = open_tmp();
        // A holds three live leases (typical of an agent juggling claims + a task). Single
        // touch must extend all three with one UPDATE — that's the design's "no per-row
        // overhead" guarantee.
        stamp_claim(&c, "A", "pr#1", 1100, 1000);
        stamp_claim(&c, "A", "pr#2", 1200, 1000);
        stamp_claim(&c, "A", "task#5", 1500, 1000);
        touch(&c, "A", 1050).unwrap();
        let want = 1050 + crate::tasks::DEFAULT_LEASE_TTL_SECS;
        for target in ["pr#1", "pr#2", "task#5"] {
            let exp = lease_expires(&c, "A", target).unwrap();
            assert_eq!(exp, want, "{target} not extended");
        }
    }
}
