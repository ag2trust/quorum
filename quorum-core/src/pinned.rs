//! Non-expiring pinned notices (issue #78).
//!
//! A pin is a durable standing notice that every agent sees on every `sync` / onboard,
//! regardless of message cursor or TTL. Use cases: CTO standing guidelines, "MIGRATION
//! IN PROGRESS — do X", coordination conventions every new session must read.
//!
//! Conceptually parallels [`control`]: no `expires_at` column, sweeper does NOT touch the
//! `pinned` table. The only lifecycle is explicit — [`pin`] inserts, [`unpin`] removes by
//! `id`. Author is recorded; `unpin` is creator-only (matches the rest of quorum's
//! agent-string authority model).
//!
//! Distinct from the messages feed by design: pins live in their own table so they cannot
//! be ack'd away by the cursor advance, and the messages feed isn't polluted by
//! never-expiring rows. See `sync::Snapshot::pinned`.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::Serialize;

/// A live pinned notice.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Pin {
    pub id: i64,
    pub ts: i64,
    pub author: String,
    pub body: String,
}

const COLS: &str = "id, ts, author, body";

fn row_to_pin(r: &Row) -> rusqlite::Result<Pin> {
    Ok(Pin {
        id: r.get(0)?,
        ts: r.get(1)?,
        author: r.get(2)?,
        body: r.get(3)?,
    })
}

/// Insert a pinned notice. Returns the row that landed (id assigned by AUTOINCREMENT).
/// Touches `author` so presence is bumped on the same write txn — matches every other
/// mutator in the crate.
pub fn pin(conn: &mut Connection, author: &str, body: &str, now: i64) -> Result<Pin> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    crate::agents::touch(&tx, author, now)?;
    tx.execute(
        "INSERT INTO pinned(ts, author, body) VALUES (?1, ?2, ?3)",
        params![now, author, body],
    )?;
    let id = tx.last_insert_rowid();
    let p = tx.query_row(
        &format!("SELECT {COLS} FROM pinned WHERE id=?1"),
        params![id],
        row_to_pin,
    )?;
    tx.commit()?;
    Ok(p)
}

/// Outcome of an [`unpin`] attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum UnpinResult {
    /// Pin removed. Returns the row that was cleared.
    Removed(Pin),
    /// No pin exists at `id`.
    NotFound,
    /// Pin exists but `by` is not the original author.
    NotCreator { author: String },
}

/// Remove a pinned notice by `id`. Creator-only: the `by` arg must equal the pin's
/// `author` (quorum's authority model is the `--agent` string; no external auth). Returns
/// a discriminated outcome so the CLI can pick the right exit code without re-querying.
pub fn unpin(conn: &mut Connection, id: i64, by: &str, now: i64) -> Result<UnpinResult> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    crate::agents::touch(&tx, by, now)?;
    let row = tx
        .query_row(
            &format!("SELECT {COLS} FROM pinned WHERE id=?1"),
            params![id],
            row_to_pin,
        )
        .optional()?;
    let outcome = match row {
        None => UnpinResult::NotFound,
        Some(p) if p.author != by => UnpinResult::NotCreator { author: p.author },
        Some(p) => {
            tx.execute("DELETE FROM pinned WHERE id=?1", params![id])?;
            UnpinResult::Removed(p)
        }
    };
    tx.commit()?;
    Ok(outcome)
}

/// List every active pin, oldest first (insertion order = ascending `id`). Read-only.
pub fn list(conn: &Connection) -> Result<Vec<Pin>> {
    let mut stmt = conn.prepare(&format!("SELECT {COLS} FROM pinned ORDER BY id ASC"))?;
    let pins = stmt
        .query_map([], row_to_pin)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(pins)
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
    fn pin_and_list_roundtrip() {
        let (_d, mut c) = open_tmp();
        let p = pin(&mut c, "cto", "MIGRATION IN PROGRESS — use new flow", 100).unwrap();
        assert!(p.id > 0);
        assert_eq!(p.author, "cto");
        assert_eq!(p.body, "MIGRATION IN PROGRESS — use new flow");
        assert_eq!(p.ts, 100);

        let listed = list(&c).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], p);
    }

    #[test]
    fn list_returns_oldest_first() {
        let (_d, mut c) = open_tmp();
        let a = pin(&mut c, "cto", "first", 100).unwrap();
        let b = pin(&mut c, "cto", "second", 200).unwrap();
        let c_pin = pin(&mut c, "cto", "third", 300).unwrap();
        let listed = list(&c).unwrap();
        // Oldest first = insertion order = ascending id.
        assert_eq!(
            listed.iter().map(|p| p.id).collect::<Vec<_>>(),
            vec![a.id, b.id, c_pin.id]
        );
    }

    #[test]
    fn unpin_by_creator_removes() {
        let (_d, mut c) = open_tmp();
        let p = pin(&mut c, "cto", "old notice", 100).unwrap();
        let outcome = unpin(&mut c, p.id, "cto", 200).unwrap();
        match outcome {
            UnpinResult::Removed(cleared) => assert_eq!(cleared, p),
            other => panic!("expected Removed, got {other:?}"),
        }
        assert!(list(&c).unwrap().is_empty());
    }

    #[test]
    fn unpin_by_non_creator_refuses() {
        let (_d, mut c) = open_tmp();
        let p = pin(&mut c, "cto", "guarded", 100).unwrap();
        let outcome = unpin(&mut c, p.id, "intruder", 200).unwrap();
        match outcome {
            UnpinResult::NotCreator { author } => assert_eq!(author, "cto"),
            other => panic!("expected NotCreator, got {other:?}"),
        }
        // Pin still present.
        assert_eq!(list(&c).unwrap().len(), 1);
    }

    #[test]
    fn unpin_on_missing_id_is_clean_notfound() {
        let (_d, mut c) = open_tmp();
        let outcome = unpin(&mut c, 99999, "cto", 100).unwrap();
        assert_eq!(outcome, UnpinResult::NotFound);
    }

    #[test]
    fn pins_survive_sweep_unlike_messages_and_events() {
        // Mirror of control's invariant test: pins are non-expiring by design. Sweep
        // running far in the future must not touch them.
        let (_d, mut c) = open_tmp();
        pin(&mut c, "cto", "permanent notice", 100).unwrap();
        let way_future = 100 + 365 * 24 * 3600;
        crate::sweep::sweep_all(&c, way_future).unwrap();
        assert_eq!(list(&c).unwrap().len(), 1, "pinned table must NOT be swept");
    }

    #[test]
    fn pin_bumps_author_presence() {
        // Like every mutator, pin touches the author so `last_seen` advances.
        let (_d, mut c) = open_tmp();
        pin(&mut c, "cto", "x", 100).unwrap();
        let agents = crate::agents::roster(&c, 100, 600).unwrap();
        let cto = agents
            .iter()
            .find(|a| a.id == "cto")
            .expect("cto registered");
        assert_eq!(cto.last_seen, 100);
    }

    #[test]
    fn unpin_bumps_caller_presence_even_on_notfound() {
        // unpin's caller is `by` — we touch them regardless of outcome so attempted
        // unpins still count as a tick. Matches the touch-then-do pattern.
        let (_d, mut c) = open_tmp();
        unpin(&mut c, 1, "operator", 100).unwrap();
        let agents = crate::agents::roster(&c, 100, 600).unwrap();
        let op = agents
            .iter()
            .find(|a| a.id == "operator")
            .expect("operator registered");
        assert_eq!(op.last_seen, 100);
    }
}
