//! Atomic claims — the load-bearing concurrency primitive.
//!
//! A claim grants exclusive hold of a `target` (e.g. `pr#2459`) for a lease. Exactly one
//! holder per target is guaranteed by the partial unique index `UNIQUE(target) WHERE
//! active=1`, enforced inside a `BEGIN IMMEDIATE` transaction — so N concurrent processes
//! racing the same target produce exactly one winner.
//!
//! Eviction is lease-only: a claim dies when `expires_at <= now`. The next `claim` on that
//! target reaps the dead row inside its own transaction before inserting (self-healing).

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

/// An active claim.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Claim {
    pub id: i64,
    pub target: String,
    pub holder: String,
    pub expires_at: i64,
}

/// Result of attempting a [`claim`].
#[derive(Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    Won(Claim),
    Lost { holder: String, expires_at: i64 },
}

/// How to identify a claim to release.
pub enum ClaimSelector {
    Target(String),
    Id(i64),
}

/// Outcome of a [`release`].
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ReleaseOutcome {
    /// True if an active claim was actually deactivated; false if there was nothing to release
    /// (idempotent — already expired/released/never held).
    pub released: bool,
}

fn is_unique_violation(e: &rusqlite::Error) -> bool {
    // Match only the UNIQUE *extended* code. All NOT NULL columns are always supplied, so the
    // only constraint an INSERT can hit today is the partial unique index — but matching the
    // extended code means any future CHECK/NOT NULL violation fails loud (exit 3) instead of
    // being misread as a lost race.
    matches!(e, rusqlite::Error::SqliteFailure(f, _)
        if f.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE)
}

/// Atomically claim `target` for `agent` with a `ttl`-second lease.
///
/// Inside one `BEGIN IMMEDIATE` transaction: bump presence, sweep, reap an expired holder on
/// this target, then insert. Returns [`ClaimOutcome::Won`] for the single winner, or
/// [`ClaimOutcome::Lost`] (with the live holder) for everyone else — Lost is NOT an error.
pub fn claim(
    conn: &mut Connection,
    agent: &str,
    target: &str,
    ttl: i64,
    now: i64,
) -> Result<ClaimOutcome> {
    // Bounded retry: a unique violation normally means a *live* holder (→ Lost). The only way
    // the re-SELECT finds no live holder is a boundary corpse reap should have cleared; retry
    // then wins. With reap using `<= now` this is unreachable, but the retry keeps any future
    // boundary slip from surfacing as an errlog'd exit 3.
    for _ in 0..3 {
        let tx = begin_immediate(conn)?;
        crate::agents::touch(&tx, agent, now)?;
        crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
        // A claim is dead iff `expires_at <= now` (consistent with the read-filter's
        // `expires_at > now`). Reap the dead holder so its row stops blocking the unique index.
        tx.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at <= ?2",
            params![target, now],
        )?;
        let expires_at = now + ttl;
        let ins = tx.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1,?2,?3,?4,1)",
            params![target, agent, now, expires_at],
        );
        match ins {
            Ok(_) => {
                let id = tx.last_insert_rowid();
                crate::events::emit(
                    &tx,
                    "claim_taken",
                    target,
                    &format!("claimed by {agent}"),
                    now,
                )?;
                tx.commit().map_err(map_sql_err)?;
                return Ok(ClaimOutcome::Won(Claim {
                    id,
                    target: target.to_string(),
                    holder: agent.to_string(),
                    expires_at,
                }));
            }
            Err(ref e) if is_unique_violation(e) => {
                // Read the live holder (still holding the write lock → no race).
                let live: Option<(String, i64)> = tx
                    .query_row(
                        "SELECT holder, expires_at FROM claims
                         WHERE target=?1 AND active=1 AND expires_at > ?2",
                        params![target, now],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                match live {
                    Some((holder, exp)) => {
                        // Commit the presence/sweep work; a lost claim is not an error.
                        tx.commit().map_err(map_sql_err)?;
                        return Ok(ClaimOutcome::Lost {
                            holder,
                            expires_at: exp,
                        });
                    }
                    None => {
                        // Boundary corpse blocked the index but is already dead per the
                        // read-filter. Roll back and retry; reap clears it next pass.
                        drop(tx);
                        continue;
                    }
                }
            }
            Err(e) => return Err(map_sql_err(e)),
        }
    }
    Err(QuorumError::Busy)
}

/// Release a claim. Fails loud ([`QuorumError::NotHolder`]) if a different agent holds the
/// live claim; idempotent (`released: false`) if there's nothing active to release.
pub fn release(
    conn: &mut Connection,
    agent: &str,
    sel: &ClaimSelector,
    now: i64,
) -> Result<ReleaseOutcome> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let found: Option<(i64, String)> = match sel {
        ClaimSelector::Target(t) => tx
            .query_row(
                "SELECT id, holder FROM claims WHERE target=?1 AND active=1 AND expires_at > ?2",
                params![t, now],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?,
        ClaimSelector::Id(id) => tx
            .query_row(
                "SELECT id, holder FROM claims WHERE id=?1 AND active=1 AND expires_at > ?2",
                params![id, now],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?,
    };
    match found {
        None => {
            tx.commit().map_err(map_sql_err)?;
            Ok(ReleaseOutcome { released: false })
        }
        Some((_, holder)) if holder != agent => {
            tx.commit().map_err(map_sql_err)?;
            Err(QuorumError::NotHolder)
        }
        Some((id, _)) => {
            // Capture the target before we deactivate so the event names what was released.
            let target: String =
                tx.query_row("SELECT target FROM claims WHERE id=?1", params![id], |r| {
                    r.get(0)
                })?;
            tx.execute("UPDATE claims SET active=0 WHERE id=?1", params![id])?;
            crate::events::emit(
                &tx,
                "claim_released",
                &target,
                &format!("released by {agent}"),
                now,
            )?;
            tx.commit().map_err(map_sql_err)?;
            Ok(ReleaseOutcome { released: true })
        }
    }
}

/// Extend a claim's lease. Fails loud if `agent` is not the current active, unexpired holder.
pub fn renew(
    conn: &mut Connection,
    agent: &str,
    claim_id: i64,
    ttl: i64,
    now: i64,
) -> Result<Claim> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let n = tx.execute(
        "UPDATE claims SET expires_at=?1 WHERE id=?2 AND holder=?3 AND active=1 AND expires_at > ?4",
        params![now + ttl, claim_id, agent, now],
    )?;
    if n == 0 {
        tx.commit().map_err(map_sql_err)?;
        return Err(QuorumError::NotHolder);
    }
    let claim = tx.query_row(
        "SELECT id, target, holder, expires_at FROM claims WHERE id=?1",
        params![claim_id],
        |r| {
            Ok(Claim {
                id: r.get(0)?,
                target: r.get(1)?,
                holder: r.get(2)?,
                expires_at: r.get(3)?,
            })
        },
    )?;
    crate::events::emit(
        &tx,
        "claim_renewed",
        &claim.target,
        &format!("renewed by {agent} (expires {})", claim.expires_at),
        now,
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(claim)
}

/// List active, unexpired claims, optionally filtered to one target. Read-only.
///
/// **Task leases are excluded** (`target NOT LIKE 'task#%'`, #58): a `task-claim` takes its
/// renewable lease in this same `claims` table under the reserved `task#<id>` target, but
/// those are internal to the task queue — they belong to `task-list`/`task-get`, not the
/// arbitrary-lock surface. So `claims` lists only the locks an agent took with `quorum claim`
/// (`pr#…`, free-form targets). The `task#` prefix is reserved for the queue; don't claim it
/// directly. This is a behavior-contract clarification, not a removal — no command goes away.
pub fn list(conn: &Connection, target: Option<&str>, now: i64) -> Result<Vec<Claim>> {
    let map = |r: &rusqlite::Row| {
        Ok(Claim {
            id: r.get(0)?,
            target: r.get(1)?,
            holder: r.get(2)?,
            expires_at: r.get(3)?,
        })
    };
    let claims = match target {
        Some(t) => {
            let mut stmt = conn.prepare(
                "SELECT id, target, holder, expires_at FROM claims
                 WHERE target=?1 AND active=1 AND expires_at > ?2
                   AND target NOT LIKE 'task#%' ORDER BY target",
            )?;
            let v = stmt
                .query_map(params![t, now], map)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            v
        }
        None => {
            let mut stmt = conn.prepare(
                "SELECT id, target, holder, expires_at FROM claims
                 WHERE active=1 AND expires_at > ?1
                   AND target NOT LIKE 'task#%' ORDER BY target",
            )?;
            let v = stmt
                .query_map(params![now], map)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            v
        }
    };
    Ok(claims)
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
    fn first_claim_wins_second_loses() {
        let (_d, mut c) = open_tmp();
        let won = claim(&mut c, "A", "pr#1", 100, 1000).unwrap();
        assert!(matches!(won, ClaimOutcome::Won(_)));
        let lost = claim(&mut c, "B", "pr#1", 100, 1000).unwrap();
        assert_eq!(
            lost,
            ClaimOutcome::Lost {
                holder: "A".into(),
                expires_at: 1100
            }
        );
    }

    #[test]
    fn reap_on_claim_after_expiry() {
        let (_d, mut c) = open_tmp();
        claim(&mut c, "A", "pr#1", 100, 1000).unwrap(); // expires at 1100
        let won = claim(&mut c, "B", "pr#1", 100, 2000).unwrap(); // past expiry → reaped
        assert!(matches!(won, ClaimOutcome::Won(c) if c.holder == "B"));
    }

    #[test]
    fn claim_at_exact_expiry_boundary_wins() {
        // Regression for the now == expires_at boundary: dead iff expires_at <= now, so the
        // new claimant must WIN (never become an errlog'd exit 3).
        let (_d, mut c) = open_tmp();
        claim(&mut c, "A", "pr#1", 100, 1000).unwrap(); // expires at 1100
        let won = claim(&mut c, "B", "pr#1", 100, 1100).unwrap(); // now == 1100 == expiry
        assert!(matches!(won, ClaimOutcome::Won(cl) if cl.holder == "B"));
    }

    #[test]
    fn expired_holder_cannot_renew_and_loses_target() {
        let (_d, mut c) = open_tmp();
        let won = claim(&mut c, "A", "pr#1", 100, 1000).unwrap(); // expires 1100
        let id = match won {
            ClaimOutcome::Won(cl) => cl.id,
            _ => unreachable!(),
        };
        // Past expiry, A cannot renew its dead lease...
        let err = renew(&mut c, "A", id, 100, 1200).unwrap_err();
        assert!(matches!(err, QuorumError::NotHolder));
        // ...and B reaps + wins the target.
        let won_b = claim(&mut c, "B", "pr#1", 100, 1200).unwrap();
        assert!(matches!(won_b, ClaimOutcome::Won(cl) if cl.holder == "B"));
    }

    #[test]
    fn claim_bumps_presence() {
        let (_d, mut c) = open_tmp();
        claim(&mut c, "A", "pr#1", 100, 1000).unwrap();
        let r = crate::agents::roster(&c, 1000, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].id, "A");
    }

    #[test]
    fn release_by_holder_then_reclaimable() {
        let (_d, mut c) = open_tmp();
        claim(&mut c, "A", "pr#1", 100, 1000).unwrap();
        let out = release(&mut c, "A", &ClaimSelector::Target("pr#1".into()), 1000).unwrap();
        assert!(out.released);
        let won = claim(&mut c, "B", "pr#1", 100, 1000).unwrap();
        assert!(matches!(won, ClaimOutcome::Won(_)));
    }

    #[test]
    fn release_by_nonholder_fails_loud() {
        let (_d, mut c) = open_tmp();
        claim(&mut c, "A", "pr#1", 100, 1000).unwrap();
        let err = release(&mut c, "B", &ClaimSelector::Target("pr#1".into()), 1000).unwrap_err();
        assert!(matches!(err, QuorumError::NotHolder));
    }

    #[test]
    fn release_nothing_is_idempotent() {
        let (_d, mut c) = open_tmp();
        let out = release(&mut c, "A", &ClaimSelector::Target("pr#1".into()), 1000).unwrap();
        assert!(!out.released);
    }

    #[test]
    fn renew_extends_and_guards_holder() {
        let (_d, mut c) = open_tmp();
        let won = claim(&mut c, "A", "pr#1", 100, 1000).unwrap();
        let id = match won {
            ClaimOutcome::Won(c) => c.id,
            _ => unreachable!(),
        };
        let renewed = renew(&mut c, "A", id, 500, 1050).unwrap();
        assert_eq!(renewed.expires_at, 1550);
        let err = renew(&mut c, "B", id, 500, 1050).unwrap_err();
        assert!(matches!(err, QuorumError::NotHolder));
    }

    #[test]
    fn renew_emits_claim_renewed_event() {
        let (_d, mut c) = open_tmp();
        let won = claim(&mut c, "A", "pr#1", 100, 1000).unwrap();
        let id = match won {
            ClaimOutcome::Won(c) => c.id,
            _ => unreachable!(),
        };
        renew(&mut c, "A", id, 500, 1050).unwrap();
        let evs = crate::events::list(&c, 0, Some("pr#1"), 10, 1050).unwrap();
        let renewed: Vec<_> = evs.iter().filter(|e| e.kind == "claim_renewed").collect();
        assert_eq!(renewed.len(), 1);
        assert!(renewed[0].body.contains("renewed by A"));
    }

    #[test]
    fn list_hides_expired() {
        let (_d, mut c) = open_tmp();
        claim(&mut c, "A", "pr#1", 100, 1000).unwrap(); // expires 1100
        assert_eq!(list(&c, None, 1050).unwrap().len(), 1);
        assert_eq!(list(&c, None, 2000).unwrap().len(), 0);
    }

    #[test]
    fn list_excludes_task_leases() {
        // A task-claim takes its lease in this same table under the reserved `task#<id>`
        // target (#58). Those are internal to the queue and must not surface via `claims`;
        // an arbitrary `pr#` lock still does.
        let (_d, mut c) = open_tmp();
        claim(&mut c, "A", "pr#7", 100, 1000).unwrap(); // arbitrary lock -> visible
        let id =
            crate::tasks::create(&mut c, "boss", "t", None, 0, None, None, None, 1000).unwrap();
        crate::tasks::claim(&mut c, "A", Some(id), &[], 100, 1000).unwrap(); // task lease -> hidden
        let all = list(&c, None, 1050).unwrap();
        assert_eq!(all.len(), 1, "only the arbitrary lock should list");
        assert_eq!(all[0].target, "pr#7");
        // Even an explicit `--target task#<id>` via claims returns nothing — the `task#`
        // namespace is owned by the queue, not the arbitrary-lock surface.
        assert!(list(&c, Some(&format!("task#{id}")), 1050)
            .unwrap()
            .is_empty());
    }
}
