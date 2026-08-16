//! Agent identity & presence.
//!
//! There is no `register`/`heartbeat` in v1 — an agent row is auto-created and its
//! `last_seen` bumped by [`touch`], which every write-taking command calls inside its own
//! transaction. Presence is *derived* (`online` = recently active) and display-only; it
//! never drives claim eviction (claims are lease-only).

use crate::error::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

/// An agent is `online` if it acted within this many seconds, OR if it holds an active claim
/// (see [`roster`]). Bumped 300→900 (#100) so idle agents don't flicker offline between
/// 5-min work-loop ticks. Phase 6 config overrides.
pub const ONLINE_WINDOW_SECS: i64 = 900;

/// Retirement budget defaults (issue #97). An agent crosses into `retiring` when EITHER:
/// - cumulative `total_active_secs` across completed tasks ≥ [`DEFAULT_RETIRE_AFTER_ACTIVE_SECS`], or
/// - cumulative `tasks_completed` ≥ [`DEFAULT_RETIRE_AFTER_TASKS`].
///
/// The seconds bound is the load-bearing one (closest proxy for context-consumed); the tasks
/// bound is a backstop for short-but-many-task agents. Both tunable via config; these
/// constants are the single source of truth (same pattern as [`ONLINE_WINDOW_SECS`]).
///
/// 5400 sec ≈ 90 min and 8 tasks land deliberately conservative — we want agents to retire
/// **before** context-compaction degrades them, not after.
pub const DEFAULT_RETIRE_AFTER_ACTIVE_SECS: i64 = 5400;
pub const DEFAULT_RETIRE_AFTER_TASKS: i64 = 8;

/// Wall-clock lifetime budget (issue #97 extension). An agent that has been alive (since
/// `first_seen`) for longer than this retires regardless of task load. Prevents the
/// "idle ticking for hours burning context tokens" failure mode observed 2026-06-28 where
/// agents ran 6-14 hours because work trickling in reset the cumulative-idle counter.
/// 3600 sec = 1 hour — hard cap on session lifetime.
pub const DEFAULT_RETIRE_AFTER_WALL_SECS: i64 = 3600;

/// Retirement state machine (issue #97). String-typed so it round-trips cleanly through
/// SQLite TEXT and JSON without a custom serde adapter; the three variants form the entire
/// state space.
pub const RETIRE_STATUS_ACTIVE: &str = "active";
pub const RETIRE_STATUS_RETIRING: &str = "retiring";
pub const RETIRE_STATUS_RETIRED: &str = "retired";

/// A row in the roster view.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentView {
    pub id: String,
    pub last_seen: i64,
    pub online: bool,
}

/// Auto-create the agent (if new) and bump its `last_seen` to `now`.
///
/// **Presence-only** — does NOT renew leases (#130). External writes (post, read,
/// task-update) update presence but never extend task leases. The daemon renews
/// only the exact task lease for the exact active run via [`renew_task_lease`].
///
/// Takes `&Connection`; callers holding a `Transaction` pass it directly (deref coercion),
/// so both writes join the caller's atomic txn. Pure reads must NOT call this.
pub fn touch(conn: &Connection, id: &str, now: i64) -> Result<()> {
    // Presence bump (always — auto-create row if first time).
    //    Session reset (issue #125): when the agent has been offline for >= ONLINE_WINDOW_SECS,
    //    this is a new session — reset first_seen + retire_status so the wall-clock budget
    //    measures SESSION age, not NAME age. A retired agent that continues ticking (within
    //    window, processing the retire signal) keeps its retired state — the offline gap is
    //    the sole signal for "returning name = new session."
    conn.execute(
        "INSERT INTO agents(id, first_seen, last_seen) VALUES (?1, ?2, ?2)
         ON CONFLICT(id) DO UPDATE SET
           last_seen = excluded.last_seen,
           first_seen = CASE
             WHEN (?2 - agents.last_seen) >= ?3
             THEN excluded.first_seen
             ELSE agents.first_seen
           END,
           retire_status = CASE
             WHEN (?2 - agents.last_seen) >= ?3
             THEN 'active'
             ELSE agents.retire_status
           END,
           retired_at = CASE
             WHEN (?2 - agents.last_seen) >= ?3
             THEN NULL
             ELSE agents.retired_at
           END",
        params![id, now, ONLINE_WINDOW_SECS],
    )?;
    Ok(())
}

/// Renew the lease on a specific task for a specific agent. **Daemon-only** —
/// called from the tick loop for the exact active run. Monotonic via MAX so an
/// explicit long TTL is never shortened. Lapsed leases are not touched (reaper
/// owns the recovery path).
pub fn renew_task_lease(conn: &Connection, agent: &str, task_id: i64, now: i64) -> Result<()> {
    let target = format!("task#{task_id}");
    conn.execute(
        "UPDATE claims
         SET expires_at = MAX(expires_at, ?2 + ?3)
         WHERE holder = ?1 AND target = ?4 AND active = 1 AND expires_at > ?2",
        params![agent, now, crate::tasks::DEFAULT_LEASE_TTL_SECS, target],
    )?;
    Ok(())
}

/// Persist the agent's declared tier (e.g. `"tier:opus-46"`). Called from `sync::tick`
/// when `--match-label tier:*` is present, inside the same write transaction as `touch`.
/// Passing `None` is a no-op (does not clear a previously stored tier).
pub fn set_tier(conn: &Connection, id: &str, tier: Option<&str>) -> Result<()> {
    if let Some(t) = tier {
        conn.execute("UPDATE agents SET tier = ?1 WHERE id = ?2", params![t, id])?;
    }
    Ok(())
}

/// Read the agent's current retire-state row. Returns `(retire_status, retired_at)`. A
/// missing row surfaces as the active default — sync calls this after `touch`, so a row
/// always exists in the live path; the fallback only fires for ad-hoc test queries against
/// an empty store.
pub fn retire_state(conn: &Connection, id: &str) -> Result<(String, Option<i64>)> {
    let row = conn
        .query_row(
            "SELECT retire_status, retired_at FROM agents WHERE id = ?1",
            params![id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?)),
        )
        .ok();
    Ok(row.unwrap_or_else(|| (RETIRE_STATUS_ACTIVE.to_string(), None)))
}

/// Read `first_seen` for the agent. Returns `None` if the agent row doesn't exist yet
/// (first tick before `touch` has run). Used by `sync::gather_with_budget` for the
/// wall-clock retirement check.
pub fn first_seen(conn: &Connection, id: &str) -> Result<Option<i64>> {
    let row = conn
        .query_row(
            "SELECT first_seen FROM agents WHERE id = ?1",
            params![id],
            |r| r.get::<_, i64>(0),
        )
        .ok();
    Ok(row)
}

/// Session-aware read of retirement state and first_seen (issue #125). When the agent is
/// returning from retirement or an offline gap, returns the values that `touch` WILL write
/// (active/None/now) rather than the stale persisted values. This lets `gather_with_budget`
/// (read phase) agree with the `touch` reset (write phase) that follows it in `tick`.
pub fn session_aware_retire_info(
    conn: &Connection,
    id: &str,
    now: i64,
) -> Result<(String, Option<i64>, Option<i64>)> {
    let row = conn
        .query_row(
            "SELECT retire_status, retired_at, first_seen, last_seen FROM agents WHERE id = ?1",
            params![id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            },
        )
        .ok();
    match row {
        Some((status, retired_at, fs, ls)) => {
            if (now - ls) >= ONLINE_WINDOW_SECS {
                Ok((RETIRE_STATUS_ACTIVE.to_string(), None, Some(now)))
            } else {
                Ok((status, retired_at, Some(fs)))
            }
        }
        None => Ok((RETIRE_STATUS_ACTIVE.to_string(), None, None)),
    }
}

/// Promote the agent to `retiring`. No-op if already retiring or retired (the state machine
/// is forward-only: active → retiring → retired). Called from `sync::tick` once the
/// load-score budget is exceeded, inside the existing write txn.
pub fn mark_retiring(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE agents
         SET retire_status = ?2
         WHERE id = ?1 AND retire_status = ?3",
        params![id, RETIRE_STATUS_RETIRING, RETIRE_STATUS_ACTIVE],
    )?;
    Ok(())
}

/// Promote the agent to `retired` and stamp `retired_at`. The "already retired"
/// short-circuit preserves the original `retired_at` so the dashboard's "retired N ago"
/// stays stable across syncs.
pub fn mark_retired(conn: &Connection, id: &str, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE agents
         SET retire_status = ?2,
             retired_at    = ?3
         WHERE id = ?1 AND retire_status != ?2",
        params![id, RETIRE_STATUS_RETIRED, now],
    )?;
    Ok(())
}

/// All agents with derived online/offline, ordered by id. Read-only (no presence bump).
/// An agent is online if it acted within `online_window` seconds OR holds an active claim
/// (#100 — claim-holders are by definition working).
pub fn roster(conn: &Connection, now: i64, online_window: i64) -> Result<Vec<AgentView>> {
    roster_with_limit(conn, now, online_window, None)
}

/// Bounded roster for polling UIs.  This deliberately orders by recent activity so a
/// dashboard does not repeatedly serialize every persistent historical agent.
pub fn roster_limited(
    conn: &Connection,
    now: i64,
    online_window: i64,
    limit: i64,
) -> Result<Vec<AgentView>> {
    roster_with_limit(conn, now, online_window, Some(limit))
}

fn roster_with_limit(
    conn: &Connection,
    now: i64,
    online_window: i64,
    limit: Option<i64>,
) -> Result<Vec<AgentView>> {
    let order_and_limit = if limit.is_some() {
        "ORDER BY a.last_seen DESC, a.id ASC LIMIT ?3"
    } else {
        "ORDER BY a.id"
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT a.id, a.last_seen,
                CASE WHEN (?1 - a.last_seen) < ?2
                       OR EXISTS (SELECT 1 FROM claims c
                                  WHERE c.holder = a.id AND c.active = 1 AND c.expires_at > ?1)
                     THEN 1 ELSE 0 END AS online
         FROM agents a {order_and_limit}"
    ))?;
    match limit {
        Some(limit) => stmt
            .query_map(params![now, online_window, limit], |r| {
                Ok(AgentView {
                    id: r.get(0)?,
                    last_seen: r.get(1)?,
                    online: r.get::<_, i64>(2)? != 0,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into),
        None => stmt
            .query_map(params![now, online_window], |r| {
                Ok(AgentView {
                    id: r.get(0)?,
                    last_seen: r.get(1)?,
                    online: r.get::<_, i64>(2)? != 0,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into),
    }
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
    fn stale_agent_without_claim_is_offline() {
        let (_d, c) = open_tmp();
        touch(&c, "Bob", 1000).unwrap();
        let r = roster(&c, 1000 + ONLINE_WINDOW_SECS + 1, ONLINE_WINDOW_SECS).unwrap();
        assert!(!r[0].online);
    }

    #[test]
    fn claim_holder_is_online_even_when_last_seen_stale() {
        let (_d, c) = open_tmp();
        touch(&c, "Eve", 1000).unwrap();
        stamp_claim(&c, "Eve", "task#42", 5000, 1000);
        // last_seen is 1000, now is 3000 → 2000s stale, well past the window.
        // But Eve holds an active claim (expires 5000 > 3000) → online.
        let r = roster(&c, 3000, ONLINE_WINDOW_SECS).unwrap();
        assert!(
            r[0].online,
            "claim-holder must be online regardless of last_seen age"
        );
    }

    #[test]
    fn expired_claim_does_not_keep_stale_agent_online() {
        let (_d, c) = open_tmp();
        touch(&c, "Fay", 1000).unwrap();
        stamp_claim(&c, "Fay", "task#42", 2000, 1000);
        // Claim expired at 2000, now is 3000. No active claim + stale → offline.
        let r = roster(&c, 3000, ONLINE_WINDOW_SECS).unwrap();
        assert!(
            !r[0].online,
            "expired claim must not keep a stale agent online"
        );
    }

    #[test]
    fn second_touch_updates_last_seen_not_first_seen() {
        let (_d, c) = open_tmp();
        touch(&c, "Cy", 1000).unwrap();
        // Gap must be < ONLINE_WINDOW_SECS so session reset doesn't fire.
        touch(&c, "Cy", 1500).unwrap();
        let (first, last): (i64, i64) = c
            .query_row(
                "SELECT first_seen, last_seen FROM agents WHERE id='Cy'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(first, 1000);
        assert_eq!(last, 1500);
    }

    #[test]
    fn roster_empty_on_fresh_db() {
        let (_d, c) = open_tmp();
        assert!(roster(&c, 1000, ONLINE_WINDOW_SECS).unwrap().is_empty());
    }

    // -- set_tier (#82) -------------------------------------------------------------------

    #[test]
    fn set_tier_persists_and_none_is_noop() {
        let (_d, c) = open_tmp();
        touch(&c, "X", 1000).unwrap();
        set_tier(&c, "X", Some("tier:opus-46")).unwrap();
        let t: Option<String> = c
            .query_row("SELECT tier FROM agents WHERE id='X'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t, Some("tier:opus-46".to_string()));
        // None does not clear.
        set_tier(&c, "X", None).unwrap();
        let t2: Option<String> = c
            .query_row("SELECT tier FROM agents WHERE id='X'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t2, Some("tier:opus-46".to_string()));
    }

    // -- Touch no longer renews leases (#130) -----------------------------------------------
    // touch() is presence-only. Lease renewal is daemon-owned via renew_task_lease().

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
    fn touch_does_not_renew_leases() {
        let (_d, c) = open_tmp();
        stamp_claim(&c, "A", "task#1", 1100, 1000);
        touch(&c, "A", 1050).unwrap();
        let exp = lease_expires(&c, "A", "task#1").unwrap();
        assert_eq!(exp, 1100, "touch must NOT renew leases (#130)");
    }

    // -- renew_task_lease (daemon-owned) (#130) -------------------------------------------

    #[test]
    fn renew_task_lease_extends_active_lease() {
        let (_d, c) = open_tmp();
        stamp_claim(&c, "A", "task#42", 1100, 1000);
        renew_task_lease(&c, "A", 42, 1090).unwrap();
        let new_exp = lease_expires(&c, "A", "task#42").unwrap();
        assert_eq!(
            new_exp,
            1090 + crate::tasks::DEFAULT_LEASE_TTL_SECS,
            "renew_task_lease must extend to now + DEFAULT_LEASE_TTL_SECS"
        );
    }

    #[test]
    fn renew_task_lease_is_monotonic() {
        let (_d, c) = open_tmp();
        let long_exp = 1000 + 24 * 3600;
        stamp_claim(&c, "A", "task#42", long_exp, 1000);
        renew_task_lease(&c, "A", 42, 1300).unwrap();
        let new_exp = lease_expires(&c, "A", "task#42").unwrap();
        assert_eq!(new_exp, long_exp, "monotonic MAX must keep longer expiry");
    }

    #[test]
    fn renew_task_lease_does_not_resurrect_lapsed() {
        let (_d, c) = open_tmp();
        stamp_claim(&c, "A", "task#42", 1100, 1000);
        renew_task_lease(&c, "A", 42, 2000).unwrap();
        let exp = lease_expires(&c, "A", "task#42").unwrap();
        assert_eq!(exp, 1100, "must not extend a lapsed lease");
    }

    #[test]
    fn renew_task_lease_scoped_to_specific_task() {
        let (_d, c) = open_tmp();
        stamp_claim(&c, "A", "task#1", 1100, 1000);
        stamp_claim(&c, "A", "task#2", 1100, 1000);
        renew_task_lease(&c, "A", 1, 1050).unwrap();
        let exp1 = lease_expires(&c, "A", "task#1").unwrap();
        let exp2 = lease_expires(&c, "A", "task#2").unwrap();
        assert_eq!(exp1, 1050 + crate::tasks::DEFAULT_LEASE_TTL_SECS);
        assert_eq!(exp2, 1100, "must NOT renew other task leases");
    }

    // -- Retirement state machine (issue #97) ---------------------------------------------

    #[test]
    fn fresh_agent_defaults_to_active_with_no_retired_at() {
        let (_d, c) = open_tmp();
        touch(&c, "Newbie", 100).unwrap();
        let (st, ts) = retire_state(&c, "Newbie").unwrap();
        assert_eq!(st, RETIRE_STATUS_ACTIVE);
        assert!(ts.is_none());
    }

    #[test]
    fn retire_state_missing_row_returns_active_default() {
        // Caller invariant: sync's read path queries an agent that may not yet have a
        // row (the touch comes inside the write txn AFTER the read). The fallback
        // keeps that path simple instead of forcing a guarded touch on every read.
        let (_d, c) = open_tmp();
        let (st, ts) = retire_state(&c, "Ghost").unwrap();
        assert_eq!(st, RETIRE_STATUS_ACTIVE);
        assert!(ts.is_none());
    }

    #[test]
    fn mark_retiring_then_retired_progresses_state_forward_only() {
        let (_d, c) = open_tmp();
        touch(&c, "X", 100).unwrap();
        mark_retiring(&c, "X").unwrap();
        assert_eq!(retire_state(&c, "X").unwrap().0, RETIRE_STATUS_RETIRING);
        // Calling mark_retiring again is a no-op (idempotent, no demote).
        mark_retiring(&c, "X").unwrap();
        assert_eq!(retire_state(&c, "X").unwrap().0, RETIRE_STATUS_RETIRING);
        // Promote to retired with a stamped timestamp.
        mark_retired(&c, "X", 200).unwrap();
        let (st, ts) = retire_state(&c, "X").unwrap();
        assert_eq!(st, RETIRE_STATUS_RETIRED);
        assert_eq!(ts, Some(200));
    }

    #[test]
    fn mark_retired_is_idempotent_and_does_not_reset_timestamp() {
        // The "retired N ago" dashboard cell relies on retired_at being frozen at the
        // original transition, not refreshed every sync.
        let (_d, c) = open_tmp();
        touch(&c, "Y", 100).unwrap();
        mark_retired(&c, "Y", 200).unwrap();
        mark_retired(&c, "Y", 999).unwrap();
        assert_eq!(retire_state(&c, "Y").unwrap().1, Some(200));
    }

    #[test]
    fn mark_retiring_cannot_demote_from_retired() {
        // Forward-only: a retired agent stays retired even if mark_retiring is called
        // (defensive — sync never does this, but the guard makes the helper safe to
        // call from any future code path without a state-check).
        let (_d, c) = open_tmp();
        touch(&c, "Z", 100).unwrap();
        mark_retired(&c, "Z", 200).unwrap();
        mark_retiring(&c, "Z").unwrap();
        assert_eq!(retire_state(&c, "Z").unwrap().0, RETIRE_STATUS_RETIRED);
    }

    // -- Session reset on returning agent (issue #125) ------------------------------------

    #[test]
    fn touch_resets_session_for_retired_agent_after_offline_gap() {
        // A retired agent that returns after an offline gap must start a fresh session:
        // first_seen reset to now, retire_status back to active, retired_at cleared.
        let (_d, c) = open_tmp();
        touch(&c, "Rex", 1000).unwrap();
        mark_retired(&c, "Rex", 1500).unwrap();
        // Rex returns after being offline for > ONLINE_WINDOW_SECS.
        touch(&c, "Rex", 1000 + ONLINE_WINDOW_SECS + 1).unwrap();
        let (first, last): (i64, i64) = c
            .query_row(
                "SELECT first_seen, last_seen FROM agents WHERE id='Rex'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let expected = 1000 + ONLINE_WINDOW_SECS + 1;
        assert_eq!(
            first, expected,
            "first_seen must reset to now on returning retired agent"
        );
        assert_eq!(last, expected);
        let (st, ts) = retire_state(&c, "Rex").unwrap();
        assert_eq!(
            st, RETIRE_STATUS_ACTIVE,
            "retire_status must reset to active"
        );
        assert!(ts.is_none(), "retired_at must be cleared");
    }

    #[test]
    fn retired_agent_still_ticking_within_window_stays_retired() {
        // A retired agent that continues ticking (processing the retire signal, about
        // to sign off) must NOT have its retirement reset — only the offline gap triggers
        // session reset, not retire_status alone.
        let (_d, c) = open_tmp();
        touch(&c, "Steady", 1000).unwrap();
        mark_retired(&c, "Steady", 1500).unwrap();
        // Steady ticks once more within the online window (processing retire signal).
        touch(&c, "Steady", 1000 + ONLINE_WINDOW_SECS - 1).unwrap();
        let (st, _) = retire_state(&c, "Steady").unwrap();
        assert_eq!(
            st, RETIRE_STATUS_RETIRED,
            "retired agent ticking within window must stay retired"
        );
    }

    #[test]
    fn touch_resets_session_for_offline_agent() {
        // An agent that went offline (last_seen older than ONLINE_WINDOW_SECS) and comes
        // back is starting a new session — first_seen must reset so the wall-clock budget
        // counts from the new session, not the original name creation.
        let (_d, c) = open_tmp();
        touch(&c, "Ollie", 1000).unwrap();
        // Ollie returns after being offline for longer than the online window.
        touch(&c, "Ollie", 1000 + ONLINE_WINDOW_SECS + 1).unwrap();
        let first: i64 = c
            .query_row("SELECT first_seen FROM agents WHERE id='Ollie'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            first,
            1000 + ONLINE_WINDOW_SECS + 1,
            "first_seen must reset when agent returns after offline gap"
        );
    }

    #[test]
    fn touch_does_not_reset_session_for_active_online_agent() {
        // An agent that is still online (touched recently) must NOT have first_seen reset
        // — the session is continuous, not a new one.
        let (_d, c) = open_tmp();
        touch(&c, "Connie", 1000).unwrap();
        touch(&c, "Connie", 1000 + ONLINE_WINDOW_SECS - 1).unwrap();
        let first: i64 = c
            .query_row("SELECT first_seen FROM agents WHERE id='Connie'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            first, 1000,
            "first_seen must NOT reset for continuously online agent"
        );
    }

    #[test]
    fn touch_resets_retiring_agent_that_went_offline() {
        // An agent in "retiring" state that went offline and comes back should also get
        // a session reset — a new session shouldn't inherit the old session's drain state.
        let (_d, c) = open_tmp();
        touch(&c, "Drain", 1000).unwrap();
        mark_retiring(&c, "Drain").unwrap();
        // Drain goes offline and returns after the window.
        touch(&c, "Drain", 1000 + ONLINE_WINDOW_SECS + 1).unwrap();
        let (st, _) = retire_state(&c, "Drain").unwrap();
        assert_eq!(
            st, RETIRE_STATUS_ACTIVE,
            "retiring agent returning after offline must reset"
        );
        let first: i64 = c
            .query_row("SELECT first_seen FROM agents WHERE id='Drain'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(first, 1000 + ONLINE_WINDOW_SECS + 1);
    }
}
