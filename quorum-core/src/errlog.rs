//! Best-effort logging of *abnormal* failures to the `errors` table.
//!
//! Only genuine failures go here (DB errors, post-timeout BUSY, bad input, migration
//! refusal) — never a normal lost-race / not-holder (those are expected operation). Logging
//! is best-effort: a failure to log must never mask or replace the original error.

use rusqlite::{params, Connection};

/// Logged errors are reclaimed after this long. Default; Phase 6 config overrides.
pub const ERROR_TTL_SECS: i64 = 7 * 24 * 3600;

/// Append an error row. Best-effort: any failure here is swallowed.
pub fn log_error(conn: &Connection, now: i64, source: &str, detail: &str) {
    let _ = conn.execute(
        "INSERT INTO errors(ts, source, detail, expires_at) VALUES (?1,?2,?3,?4)",
        params![now, source, detail, now + ERROR_TTL_SECS],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_error_inserts_row() {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        log_error(&c, 100, "claim", "database busy after timeout");
        let (source, detail, expires): (String, String, i64) = c
            .query_row("SELECT source, detail, expires_at FROM errors", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!(source, "claim");
        assert_eq!(detail, "database busy after timeout");
        assert_eq!(expires, 100 + ERROR_TTL_SECS);
    }
}
