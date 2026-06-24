//! Monotonic-ish wall-clock source. All TTLs are unix seconds.
//!
//! Tests inject explicit `now`/`expires_at` values rather than sleeping, so this is the
//! only place real time enters the system.

/// Current unix time in seconds.
pub fn now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    #[test]
    fn now_is_after_2020() {
        // 1_577_836_800 = 2020-01-01T00:00:00Z
        assert!(super::now() > 1_577_836_800);
    }
}
