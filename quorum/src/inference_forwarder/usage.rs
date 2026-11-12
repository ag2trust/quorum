//! Per-run token accounting for the inference forwarder.
//!
//! Counts come only from the upstream's own `usage` reports. The ledger lives
//! in this process's memory: it survives daemon restarts, and is lost when
//! the forwarder itself restarts, after which a live run counts from zero.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// Longest server-sent event line examined for usage. Usage events are a few
/// hundred bytes; a longer line is skipped rather than buffered.
const MAX_EVENT_LINE_BYTES: usize = 64 * 1024;

#[derive(Debug)]
struct RunUsage {
    tokens: u64,
    last_seen: Instant,
}

/// Result of asking whether a run may start another request.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Admission {
    Admitted,
    /// The run has used at least its cap.
    CapReached,
    /// The ledger tracks its maximum number of runs and this one is new.
    Full,
}

/// Tokens used by each run this process has served, keyed by run capability.
#[derive(Debug)]
pub(super) struct Ledger {
    cap: Option<u64>,
    capacity: usize,
    runs: Mutex<HashMap<String, RunUsage>>,
}

impl Ledger {
    pub(super) fn new(cap: Option<u64>, capacity: usize) -> Self {
        Self {
            cap,
            capacity,
            runs: Mutex::default(),
        }
    }

    fn runs(&self) -> std::sync::MutexGuard<'_, HashMap<String, RunUsage>> {
        // No code path panics while holding the lock; recover the map anyway.
        self.runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record that `run` is asking for a request and decide whether its cap
    /// allows one. The check is made at admission only: requests already in
    /// flight when the cap is crossed finish, and are counted.
    pub(super) fn admit(&self, run: &str) -> Admission {
        let mut runs = self.runs();
        if !runs.contains_key(run) {
            if runs.len() >= self.capacity {
                return Admission::Full;
            }
            runs.insert(
                run.to_string(),
                RunUsage {
                    tokens: 0,
                    last_seen: Instant::now(),
                },
            );
        }
        let Some(usage) = runs.get_mut(run) else {
            return Admission::Full;
        };
        usage.last_seen = Instant::now();
        match self.cap {
            Some(cap) if usage.tokens >= cap => Admission::CapReached,
            _ => Admission::Admitted,
        }
    }

    /// Add `tokens` to a run's total and return the new total.
    pub(super) fn charge(&self, run: &str, tokens: u64) -> u64 {
        let mut runs = self.runs();
        let now = Instant::now();
        let usage = runs.entry(run.to_string()).or_insert(RunUsage {
            tokens: 0,
            last_seen: now,
        });
        usage.tokens = usage.tokens.saturating_add(tokens);
        usage.last_seen = now;
        usage.tokens
    }

    /// Up to `limit` tracked runs, least recently seen first.
    pub(super) fn least_recent(&self, limit: usize) -> Vec<String> {
        let runs = self.runs();
        let mut entries: Vec<(&String, Instant)> = runs
            .iter()
            .map(|(run, usage)| (run, usage.last_seen))
            .collect();
        entries.sort_by_key(|(_, seen)| *seen);
        entries
            .into_iter()
            .take(limit)
            .map(|(run, _)| run.clone())
            .collect()
    }

    /// Stop tracking a run whose capability the daemon no longer accepts.
    pub(super) fn forget(&self, run: &str) {
        self.runs().remove(run);
    }
}

/// Token counts reported for one response.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Counts {
    input: u64,
    cache_creation: u64,
    cache_read: u64,
    output: u64,
}

impl Counts {
    /// All input the request was billed for: uncached, cache-written, and
    /// cache-read tokens.
    pub(super) fn input_total(&self) -> u64 {
        self.input
            .saturating_add(self.cache_creation)
            .saturating_add(self.cache_read)
    }

    pub(super) fn output_total(&self) -> u64 {
        self.output
    }

    pub(super) fn total(&self) -> u64 {
        self.input_total().saturating_add(self.output)
    }
}

/// Reads `usage` out of one upstream response, streamed or whole.
#[derive(Debug, Default)]
pub(super) struct Meter {
    counts: Counts,
    charged: u64,
    line: Vec<u8>,
    skipping_line: bool,
}

impl Meter {
    pub(super) fn counts(&self) -> Counts {
        self.counts
    }

    /// Apply one `usage` object and return the tokens not yet charged. Each
    /// field replaces the previous value: streamed usage is cumulative.
    pub(super) fn observe(&mut self, usage: &Value) -> u64 {
        let field = |name: &str| usage.get(name).and_then(Value::as_u64);
        if let Some(value) = field("input_tokens") {
            self.counts.input = value;
        }
        if let Some(value) = field("cache_creation_input_tokens") {
            self.counts.cache_creation = value;
        }
        if let Some(value) = field("cache_read_input_tokens") {
            self.counts.cache_read = value;
        }
        if let Some(value) = field("output_tokens") {
            self.counts.output = value;
        }
        let total = self.counts.total();
        let delta = total.saturating_sub(self.charged);
        self.charged = self.charged.max(total);
        delta
    }

    /// Feed the next bytes of a server-sent event stream and return the
    /// tokens newly reported by any complete usage events within them.
    pub(super) fn feed_events(&mut self, mut chunk: &[u8]) -> u64 {
        let mut delta = 0_u64;
        while let Some(end) = chunk.iter().position(|byte| *byte == b'\n') {
            let (head, rest) = chunk.split_at(end);
            chunk = &rest[1..];
            if self.skipping_line {
                self.skipping_line = false;
            } else if self.line.is_empty() {
                delta = delta.saturating_add(self.observe_line(head));
            } else if self.line.len() + head.len() <= MAX_EVENT_LINE_BYTES {
                self.line.extend_from_slice(head);
                let line = std::mem::take(&mut self.line);
                delta = delta.saturating_add(self.observe_line(&line));
            }
            self.line.clear();
        }
        if !self.skipping_line {
            if self.line.len() + chunk.len() > MAX_EVENT_LINE_BYTES {
                self.line.clear();
                self.skipping_line = true;
            } else {
                self.line.extend_from_slice(chunk);
            }
        }
        delta
    }

    fn observe_line(&mut self, line: &[u8]) -> u64 {
        let Some(data) = line.strip_prefix(b"data:") else {
            return 0;
        };
        if data.len() > MAX_EVENT_LINE_BYTES || !contains(data, b"\"usage\"") {
            return 0;
        }
        let Ok(event) = serde_json::from_slice::<Value>(data) else {
            return 0;
        };
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => event
                .get("message")
                .and_then(|message| message.get("usage"))
                .map_or(0, |usage| self.observe(usage)),
            Some("message_delta") => event.get("usage").map_or(0, |usage| self.observe(usage)),
            _ => 0,
        }
    }
}

pub(super) fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ledger_enforces_the_cap_at_admission() {
        let ledger = Ledger::new(Some(100), 8);
        assert_eq!(ledger.admit("run-a"), Admission::Admitted);
        assert_eq!(ledger.charge("run-a", 99), 99);
        assert_eq!(ledger.admit("run-a"), Admission::Admitted);
        assert_eq!(ledger.charge("run-a", 1), 100);
        assert_eq!(ledger.admit("run-a"), Admission::CapReached);
        // Another run has its own count.
        assert_eq!(ledger.admit("run-b"), Admission::Admitted);
        assert_eq!(ledger.charge("run-b", u64::MAX), u64::MAX);
        assert_eq!(ledger.charge("run-b", 5), u64::MAX);

        let uncapped = Ledger::new(None, 8);
        uncapped.charge("run-a", u64::MAX);
        assert_eq!(uncapped.admit("run-a"), Admission::Admitted);
    }

    #[test]
    fn ledger_is_bounded_and_releases_only_forgotten_runs() {
        let ledger = Ledger::new(Some(10), 2);
        assert_eq!(ledger.admit("run-a"), Admission::Admitted);
        ledger.charge("run-a", 10);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(ledger.admit("run-b"), Admission::Admitted);
        assert_eq!(ledger.admit("run-c"), Admission::Full);
        // A tracked run is still served, and still capped, while full.
        assert_eq!(ledger.admit("run-a"), Admission::CapReached);
        std::thread::sleep(std::time::Duration::from_millis(5));
        ledger.admit("run-a");
        assert_eq!(ledger.least_recent(1), vec!["run-b".to_string()]);
        ledger.forget("run-b");
        assert_eq!(ledger.admit("run-c"), Admission::Admitted);
        assert_eq!(ledger.admit("run-a"), Admission::CapReached);
    }

    #[test]
    fn whole_response_usage_counts_every_input_kind() {
        let mut meter = Meter::default();
        let delta = meter.observe(&json!({
            "input_tokens": 10,
            "cache_creation_input_tokens": 20,
            "cache_read_input_tokens": 30,
            "output_tokens": 5,
            "cache_creation": {"ephemeral_5m_input_tokens": 20},
            "service_tier": "standard"
        }));
        assert_eq!(delta, 65);
        assert_eq!(meter.counts().input_total(), 60);
        assert_eq!(meter.counts().output_total(), 5);
    }

    fn stream() -> Vec<u8> {
        concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"usage\":",
            "{\"input_tokens\":25,\"cache_read_input_tokens\":100,\"output_tokens\":1}}}\n\n",
            "event: ping\ndata: {\"type\":\"ping\"}\n\n",
            "event: content_block_delta\r\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":",
            "{\"type\":\"text_delta\",\"text\":\"\\\"usage\\\":{\\\"output_tokens\\\":999999}\"}}\r\n\r\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},",
            "\"usage\":{\"output_tokens\":40}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        )
        .as_bytes()
        .to_vec()
    }

    #[test]
    fn streamed_usage_is_cumulative_and_independent_of_chunking() {
        let bytes = stream();
        for size in [1, 2, 7, 64, bytes.len()] {
            let mut meter = Meter::default();
            let mut total = 0;
            for chunk in bytes.chunks(size) {
                total += meter.feed_events(chunk);
            }
            // 25 uncached + 100 cache-read input, 40 output. The text delta
            // that merely mentions usage is not a usage event.
            assert_eq!(total, 165, "chunk size {size}");
            assert_eq!(meter.counts().input_total(), 125);
            assert_eq!(meter.counts().output_total(), 40);
        }
    }

    #[test]
    fn usage_reported_before_a_truncated_stream_is_kept() {
        let bytes = stream();
        let cut = bytes
            .windows(13)
            .position(|window| window == b"event: ping\nd")
            .unwrap();
        let mut meter = Meter::default();
        assert_eq!(meter.feed_events(&bytes[..cut]), 126);
    }

    #[test]
    fn overlong_lines_are_skipped_without_losing_later_usage() {
        let mut meter = Meter::default();
        let mut bytes = b"data: {\"type\":\"content_block_delta\",\"text\":\"".to_vec();
        bytes.extend(std::iter::repeat_n(b'x', 3 * MAX_EVENT_LINE_BYTES));
        bytes.extend_from_slice(b"\"}\n\n");
        bytes.extend_from_slice(
            b"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}\n\n",
        );
        let mut total = 0;
        for chunk in bytes.chunks(4096) {
            total += meter.feed_events(chunk);
            assert!(meter.line.len() <= MAX_EVENT_LINE_BYTES);
        }
        assert_eq!(total, 7);
    }
}
