//! Per-run token accounting for the inference forwarder.
//!
//! A request is admitted only if the run's cap can cover the most it could
//! cost: its `max_tokens` plus an estimate of its input. That amount is
//! reserved while the request is in flight, so concurrent requests cannot
//! together pass the cap, and is replaced by the upstream's own `usage`
//! report when one arrives. A request that ends without a final report,
//! because the caller hung up, the stream broke, or the upstream went silent,
//! is charged its reservation.
//!
//! The ledger lives in this process's memory: it survives daemon restarts,
//! and is lost when the forwarder itself restarts, after which a live run
//! counts from zero.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Longest server-sent event line examined for usage. Usage events are a few
/// hundred bytes; a longer line is skipped rather than buffered.
const MAX_EVENT_LINE_BYTES: usize = 64 * 1024;

/// Input tokens assumed for a request body before the upstream reports the
/// real figure: one per two bytes, which over-estimates ordinary text and
/// code roughly twofold and inline base64 media by far more.
pub(super) fn estimated_input_tokens(body_bytes: usize) -> u64 {
    (body_bytes as u64).div_ceil(2)
}

#[derive(Debug)]
struct RunUsage {
    /// Tokens charged for finished requests.
    settled: u64,
    /// Tokens held for requests still in flight.
    reserved: u64,
    last_seen: Instant,
}

impl RunUsage {
    fn new() -> Self {
        Self {
            settled: 0,
            reserved: 0,
            last_seen: Instant::now(),
        }
    }

    fn committed(&self) -> u64 {
        self.settled.saturating_add(self.reserved)
    }
}

/// Result of asking whether a run may start another request.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Admission {
    Admitted,
    /// The run's settled and reserved tokens already reach its cap.
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

    /// Start tracking `run` and refuse it early if nothing is left of its
    /// cap. This runs before the request body is read; `reserve` makes the
    /// binding decision once the request's size is known.
    pub(super) fn admit(&self, run: &str) -> Admission {
        let mut runs = self.runs();
        if !runs.contains_key(run) {
            if runs.len() >= self.capacity {
                return Admission::Full;
            }
            runs.insert(run.to_string(), RunUsage::new());
        }
        let Some(usage) = runs.get_mut(run) else {
            return Admission::Full;
        };
        usage.last_seen = Instant::now();
        match self.cap {
            Some(cap) if usage.committed() >= cap => Admission::CapReached,
            _ => Admission::Admitted,
        }
    }

    /// Hold `tokens` for one request, or `None` if the run's settled and
    /// reserved tokens plus this request would pass its cap. The check and
    /// the hold happen under one lock, so concurrent requests cannot each
    /// take the same remaining allowance.
    pub(super) fn reserve(self: &Arc<Self>, run: &str, tokens: u64) -> Option<Reservation> {
        let mut runs = self.runs();
        let usage = runs.entry(run.to_string()).or_insert_with(RunUsage::new);
        usage.last_seen = Instant::now();
        if let Some(cap) = self.cap {
            if usage.committed().saturating_add(tokens) > cap {
                return None;
            }
        }
        usage.reserved = usage.reserved.saturating_add(tokens);
        Some(Reservation {
            ledger: Arc::clone(self),
            run: run.to_string(),
            held: tokens,
            open: true,
        })
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

/// Tokens held against a run for one in-flight request. Dropping it without
/// an explicit outcome charges the whole hold: the safe default for any path
/// that loses track of what the upstream did.
#[derive(Debug)]
pub(super) struct Reservation {
    ledger: Arc<Ledger>,
    run: String,
    held: u64,
    open: bool,
}

impl Reservation {
    /// Replace the hold with a better bound, once the upstream has reported
    /// the request's real input. May raise the hold above the estimate.
    pub(super) fn hold(&mut self, tokens: u64) {
        let mut runs = self.ledger.runs();
        let usage = runs.entry(self.run.clone()).or_insert_with(RunUsage::new);
        usage.reserved = usage
            .reserved
            .saturating_sub(self.held)
            .saturating_add(tokens);
        self.held = tokens;
    }

    fn close(&mut self, charge: u64) -> u64 {
        self.open = false;
        let mut runs = self.ledger.runs();
        let usage = runs.entry(self.run.clone()).or_insert_with(RunUsage::new);
        usage.reserved = usage.reserved.saturating_sub(self.held);
        usage.settled = usage.settled.saturating_add(charge);
        usage.last_seen = Instant::now();
        usage.settled
    }

    /// The upstream reported final usage: charge exactly that. Returns the
    /// run's settled total.
    pub(super) fn settle(mut self, actual: u64) -> u64 {
        self.close(actual)
    }

    /// The request never reached the upstream, or the upstream refused it
    /// without doing billable work: charge nothing.
    pub(super) fn release(mut self) -> u64 {
        self.close(0)
    }

    /// The outcome is unknown: charge the whole hold.
    pub(super) fn keep(mut self) -> u64 {
        let held = self.held;
        self.close(held)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.open {
            let held = self.held;
            self.close(held);
        }
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
    input_known: bool,
    final_seen: bool,
    line: Vec<u8>,
    skipping_line: bool,
}

impl Meter {
    pub(super) fn counts(&self) -> Counts {
        self.counts
    }

    /// Whether the upstream has reported this request's input tokens.
    pub(super) fn input_known(&self) -> bool {
        self.input_known
    }

    /// Whether the upstream has reported the response's closing usage: the
    /// whole response's `usage`, or a stream's `message_delta` usage.
    pub(super) fn final_seen(&self) -> bool {
        self.final_seen
    }

    /// Apply one `usage` object. Each field replaces the previous value:
    /// streamed usage is cumulative. Returns whether it carried any count.
    fn apply(&mut self, usage: &Value) -> bool {
        let field = |name: &str| usage.get(name).and_then(Value::as_u64);
        let mut any = false;
        if let Some(value) = field("input_tokens") {
            self.counts.input = value;
            self.input_known = true;
            any = true;
        }
        if let Some(value) = field("cache_creation_input_tokens") {
            self.counts.cache_creation = value;
            any = true;
        }
        if let Some(value) = field("cache_read_input_tokens") {
            self.counts.cache_read = value;
            any = true;
        }
        if let Some(value) = field("output_tokens") {
            self.counts.output = value;
            any = true;
        }
        any
    }

    /// Apply the `usage` of a whole, non-streamed response.
    pub(super) fn observe_final(&mut self, usage: &Value) {
        if self.apply(usage) && self.input_known {
            self.final_seen = true;
        }
    }

    /// Feed the next bytes of a server-sent event stream. Returns whether
    /// any complete usage event within them changed what is known.
    pub(super) fn feed_events(&mut self, mut chunk: &[u8]) -> bool {
        let mut changed = false;
        while let Some(end) = chunk.iter().position(|byte| *byte == b'\n') {
            let (head, rest) = chunk.split_at(end);
            chunk = &rest[1..];
            if self.skipping_line {
                self.skipping_line = false;
            } else if self.line.is_empty() {
                changed |= self.observe_line(head);
            } else if self.line.len() + head.len() <= MAX_EVENT_LINE_BYTES {
                self.line.extend_from_slice(head);
                let line = std::mem::take(&mut self.line);
                changed |= self.observe_line(&line);
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
        changed
    }

    fn observe_line(&mut self, line: &[u8]) -> bool {
        let Some(data) = line.strip_prefix(b"data:") else {
            return false;
        };
        if data.len() > MAX_EVENT_LINE_BYTES || !contains(data, b"\"usage\"") {
            return false;
        }
        let Ok(event) = serde_json::from_slice::<Value>(data) else {
            return false;
        };
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => event
                .get("message")
                .and_then(|message| message.get("usage"))
                .is_some_and(|usage| self.apply(usage)),
            Some("message_delta") => {
                let applied = event.get("usage").is_some_and(|usage| self.apply(usage));
                self.final_seen |= applied;
                applied
            }
            _ => false,
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

    fn ledger(cap: Option<u64>, capacity: usize) -> Arc<Ledger> {
        Arc::new(Ledger::new(cap, capacity))
    }

    #[test]
    fn reservations_count_against_the_cap_until_reconciled() {
        let ledger = ledger(Some(100), 8);
        assert_eq!(ledger.admit("run-a"), Admission::Admitted);
        // Two in-flight requests may hold the whole cap; a third may not.
        let first = ledger.reserve("run-a", 60).unwrap();
        let second = ledger.reserve("run-a", 40).unwrap();
        assert!(ledger.reserve("run-a", 1).is_none());
        assert_eq!(ledger.admit("run-a"), Admission::CapReached);
        // Another run has its own allowance.
        let other = ledger.reserve("run-b", 100).unwrap();
        assert!(ledger.reserve("run-b", 1).is_none());
        assert_eq!(other.release(), 0);

        // Reconciling to actual usage frees what was not used.
        assert_eq!(first.settle(10), 10);
        assert!(ledger.reserve("run-a", 51).is_none());
        let third = ledger.reserve("run-a", 50).unwrap();
        // A request that failed before the upstream costs nothing.
        assert_eq!(third.release(), 10);
        // An unknown outcome costs the whole hold.
        assert_eq!(second.keep(), 50);
        assert!(ledger.reserve("run-a", 51).is_none());
        // A hold dropped without an outcome is charged in full too.
        drop(ledger.reserve("run-a", 50).unwrap());
        assert_eq!(ledger.admit("run-a"), Admission::CapReached);
        assert!(ledger.reserve("run-a", 1).is_none());
        assert!(ledger.reserve("run-a", 0).is_some());
    }

    #[test]
    fn a_hold_follows_the_reported_input() {
        let ledger = ledger(Some(100), 8);
        let mut held = ledger.reserve("run-a", 80).unwrap();
        held.hold(30);
        let mut other = ledger.reserve("run-a", 70).unwrap();
        // Real input above the estimate raises the hold, even past the cap.
        other.hold(90);
        assert!(ledger.reserve("run-a", 1).is_none());
        assert_eq!(held.keep(), 30);
        assert_eq!(other.settle(5), 35);
        assert!(ledger.reserve("run-a", 65).is_some());
    }

    #[test]
    fn concurrent_reservations_never_pass_the_cap() {
        let ledger = ledger(Some(1000), 8);
        let granted: usize = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..32)
                .map(|_| {
                    let ledger = &ledger;
                    scope.spawn(move || ledger.reserve("run-a", 100).map(std::mem::forget))
                })
                .collect();
            handles
                .into_iter()
                .filter(|_| true)
                .map(|handle| usize::from(handle.join().unwrap().is_some()))
                .sum()
        });
        assert_eq!(granted, 10);
    }

    #[test]
    fn uncapped_runs_are_tracked_but_never_refused() {
        let ledger = ledger(None, 8);
        let held = ledger.reserve("run-a", u64::MAX).unwrap();
        ledger.reserve("run-a", u64::MAX).unwrap().release();
        assert_eq!(held.settle(7), 7);
        assert_eq!(ledger.admit("run-a"), Admission::Admitted);
    }

    #[test]
    fn ledger_is_bounded_and_releases_only_forgotten_runs() {
        let ledger = ledger(Some(10), 2);
        assert_eq!(ledger.admit("run-a"), Admission::Admitted);
        ledger.reserve("run-a", 10).unwrap().keep();
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
    fn input_is_estimated_from_the_body_size() {
        assert_eq!(estimated_input_tokens(0), 0);
        assert_eq!(estimated_input_tokens(1), 1);
        assert_eq!(estimated_input_tokens(4096), 2048);
    }

    #[test]
    fn whole_response_usage_counts_every_input_kind() {
        let mut meter = Meter::default();
        assert!(!meter.final_seen());
        meter.observe_final(&json!({
            "input_tokens": 10,
            "cache_creation_input_tokens": 20,
            "cache_read_input_tokens": 30,
            "output_tokens": 5,
            "cache_creation": {"ephemeral_5m_input_tokens": 20},
            "service_tier": "standard"
        }));
        assert!(meter.final_seen());
        assert_eq!(meter.counts().total(), 65);
        assert_eq!(meter.counts().input_total(), 60);
        assert_eq!(meter.counts().output_total(), 5);
        // A usage object with no counts is not a final report.
        let mut empty = Meter::default();
        empty.observe_final(&json!({"service_tier": "standard"}));
        empty.observe_final(&json!({"output_tokens": 5}));
        assert!(!empty.final_seen());
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
            let mut changes = 0;
            for chunk in bytes.chunks(size) {
                changes += usize::from(meter.feed_events(chunk));
            }
            // 25 uncached + 100 cache-read input, 40 output. The text delta
            // that merely mentions usage is not a usage event.
            assert_eq!(meter.counts().total(), 165, "chunk size {size}");
            assert_eq!(meter.counts().input_total(), 125);
            assert_eq!(meter.counts().output_total(), 40);
            assert!(meter.final_seen());
            assert!((1..=2).contains(&changes), "chunk size {size}");
        }
    }

    #[test]
    fn a_truncated_stream_reports_input_but_no_final_usage() {
        let bytes = stream();
        let cut = bytes
            .windows(13)
            .position(|window| window == b"event: ping\nd")
            .unwrap();
        let mut meter = Meter::default();
        assert!(meter.feed_events(&bytes[..cut]));
        assert!(meter.input_known());
        assert!(!meter.final_seen());
        assert_eq!(meter.counts().input_total(), 125);
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
        for chunk in bytes.chunks(4096) {
            meter.feed_events(chunk);
            assert!(meter.line.len() <= MAX_EVENT_LINE_BYTES);
        }
        assert_eq!(meter.counts().output_total(), 7);
        assert!(meter.final_seen());
    }
}
