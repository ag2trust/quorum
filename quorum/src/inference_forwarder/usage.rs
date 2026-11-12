//! Per-run token accounting for the inference forwarder.
//!
//! A request is admitted only if the run's cap can cover the most it could
//! cost: its `max_tokens` plus an upper bound on its input. That amount is
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

/// Input tokens the upstream adds that no request byte accounts for: the
/// instructions it prepends when tools are declared (a few hundred tokens)
/// and its framing of each message.
const FRAMING_TOKENS: u64 = 1024;

/// Upper bound on a request's input tokens, assumed until the upstream
/// reports the real figure. Text cannot cost more than one token per byte,
/// and the request policy admits no content whose cost is independent of its
/// size (images and documents are priced by dimensions and pages). Ordinary
/// text and code come in around a quarter of this.
pub(super) fn estimated_input_tokens(body_bytes: usize) -> u64 {
    (body_bytes as u64).saturating_add(FRAMING_TOKENS)
}

#[derive(Debug)]
struct RunUsage {
    /// Tokens charged for finished requests.
    settled: u64,
    /// Tokens held for requests still in flight.
    reserved: u64,
    /// Requests being served for this run right now.
    in_flight: usize,
    last_seen: Instant,
}

impl RunUsage {
    fn new() -> Self {
        Self {
            settled: 0,
            reserved: 0,
            in_flight: 0,
            last_seen: Instant::now(),
        }
    }

    fn committed(&self) -> u64 {
        self.settled.saturating_add(self.reserved)
    }
}

/// Why a run may not start another request.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Refusal {
    /// The run already has its maximum number of requests in flight.
    Busy,
    /// The run's settled and reserved tokens already reach its cap.
    CapReached,
    /// The ledger tracks its maximum number of runs and this one is new.
    Full,
}

/// Tokens used by each run this process has served, keyed by run capability.
#[derive(Debug)]
pub(super) struct Ledger {
    cap: Option<u64>,
    max_in_flight: usize,
    capacity: usize,
    runs: Mutex<HashMap<String, RunUsage>>,
}

impl Ledger {
    pub(super) fn new(cap: Option<u64>, max_in_flight: usize, capacity: usize) -> Self {
        Self {
            cap,
            max_in_flight,
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

    /// Take one of `run`'s in-flight request slots, or say why it may not
    /// have one. This runs before the request body is read, so a run can
    /// make the forwarder buffer only as many bodies as it has slots, and
    /// refuses early a run with nothing left of its cap; `reserve` makes the
    /// binding cap decision once the request's size is known.
    pub(super) fn enter(self: &Arc<Self>, run: &str) -> Result<Slot, Refusal> {
        let mut runs = self.runs();
        if !runs.contains_key(run) {
            if runs.len() >= self.capacity {
                return Err(Refusal::Full);
            }
            runs.insert(run.to_string(), RunUsage::new());
        }
        let Some(usage) = runs.get_mut(run) else {
            return Err(Refusal::Full);
        };
        usage.last_seen = Instant::now();
        if usage.in_flight >= self.max_in_flight {
            return Err(Refusal::Busy);
        }
        if self.cap.is_some_and(|cap| usage.committed() >= cap) {
            return Err(Refusal::CapReached);
        }
        usage.in_flight += 1;
        Ok(Slot {
            ledger: Arc::clone(self),
            run: run.to_string(),
        })
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

    /// Up to `limit` tracked runs with nothing in flight, least recently
    /// seen first.
    pub(super) fn least_recent_idle(&self, limit: usize) -> Vec<String> {
        let runs = self.runs();
        let mut entries: Vec<(&String, Instant)> = runs
            .iter()
            .filter(|(_, usage)| usage.in_flight == 0 && usage.reserved == 0)
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

/// One of a run's in-flight request slots, held until the request's response
/// has been delivered or abandoned.
#[derive(Debug)]
pub(super) struct Slot {
    ledger: Arc<Ledger>,
    run: String,
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(usage) = self.ledger.runs().get_mut(&self.run) {
            usage.in_flight = usage.in_flight.saturating_sub(1);
            usage.last_seen = Instant::now();
        }
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
    pub(super) fn held(&self) -> u64 {
        self.held
    }

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
    /// Sum of the per-iteration usage the upstream itemized, when it did.
    iterations: Option<u64>,
    input_known: bool,
    final_seen: bool,
    line: Vec<u8>,
    skipping_line: bool,
}

impl Meter {
    pub(super) fn counts(&self) -> Counts {
        self.counts
    }

    /// Everything the upstream billed for this response. When the upstream
    /// did more than produce the returned message (compacting the
    /// conversation, or retrying on another model) it itemizes each step
    /// under `usage.iterations`, and the top-level counts cover the returned
    /// message only; the larger of the two is charged.
    pub(super) fn billed(&self) -> u64 {
        self.counts.total().max(self.iterations.unwrap_or(0))
    }

    /// Whether the upstream itemized this response's steps.
    pub(super) fn itemized(&self) -> bool {
        self.iterations.is_some()
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
        if let Some(iterations) = usage.get("iterations").and_then(Value::as_array) {
            let sum = iterations.iter().fold(0_u64, |sum, step| {
                [
                    "input_tokens",
                    "cache_creation_input_tokens",
                    "cache_read_input_tokens",
                    "output_tokens",
                ]
                .iter()
                .filter_map(|name| step.get(name).and_then(Value::as_u64))
                .fold(sum, u64::saturating_add)
            });
            // Itemized usage only grows over a response.
            self.iterations = Some(self.iterations.unwrap_or(0).max(sum));
            any = true;
        }
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
        Arc::new(Ledger::new(cap, 2, capacity))
    }

    #[test]
    fn reservations_count_against_the_cap_until_reconciled() {
        let ledger = ledger(Some(100), 8);
        // Two in-flight requests may hold the whole cap; a third may not.
        let first = ledger.reserve("run-a", 60).unwrap();
        let second = ledger.reserve("run-a", 40).unwrap();
        assert!(ledger.reserve("run-a", 1).is_none());
        assert_eq!(ledger.enter("run-a").unwrap_err(), Refusal::CapReached);
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
        assert_eq!(ledger.enter("run-a").unwrap_err(), Refusal::CapReached);
        assert!(ledger.reserve("run-a", 1).is_none());
        assert!(ledger.reserve("run-a", 0).is_some());
    }

    #[test]
    fn each_run_has_its_own_in_flight_slots() {
        let ledger = ledger(None, 8);
        let first = ledger.enter("run-a").unwrap();
        let second = ledger.enter("run-a").unwrap();
        assert_eq!(ledger.enter("run-a").unwrap_err(), Refusal::Busy);
        // Another run is unaffected.
        let other = ledger.enter("run-b").unwrap();
        drop(first);
        let third = ledger.enter("run-a").unwrap();
        assert_eq!(ledger.enter("run-a").unwrap_err(), Refusal::Busy);
        // Forgetting a run mid-request leaves its slots harmless to drop.
        ledger.forget("run-a");
        drop((second, third, other));
        drop(ledger.enter("run-a").unwrap());
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
        drop(ledger.enter("run-a").unwrap());
    }

    #[test]
    fn ledger_is_bounded_and_releases_only_forgotten_runs() {
        let ledger = ledger(Some(10), 2);
        drop(ledger.enter("run-a").unwrap());
        ledger.reserve("run-a", 10).unwrap().keep();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let busy = ledger.enter("run-b").unwrap();
        assert_eq!(ledger.enter("run-c").unwrap_err(), Refusal::Full);
        // A tracked run is still served, and still capped, while full.
        assert_eq!(ledger.enter("run-a").unwrap_err(), Refusal::CapReached);
        // Only runs with nothing in flight are offered for release.
        assert_eq!(ledger.least_recent_idle(8), vec!["run-a".to_string()]);
        drop(busy);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let _ = ledger.enter("run-a");
        assert_eq!(ledger.least_recent_idle(1), vec!["run-b".to_string()]);
        ledger.forget("run-b");
        drop(ledger.enter("run-c").unwrap());
        assert_eq!(ledger.enter("run-a").unwrap_err(), Refusal::CapReached);
    }

    #[test]
    fn input_is_estimated_from_the_body_size() {
        // Never less than one token per byte, whatever the size.
        for bytes in [0, 1, 4096, 32 * 1024 * 1024] {
            assert_eq!(estimated_input_tokens(bytes), bytes as u64 + FRAMING_TOKENS);
        }
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

    #[test]
    fn itemized_steps_are_charged_over_the_top_level_counts() {
        // The upstream compacted the conversation, then answered. The
        // top-level counts cover the answer only.
        let mut meter = Meter::default();
        meter.observe_final(&json!({
            "input_tokens": 23000,
            "output_tokens": 1000,
            "iterations": [
                {"type": "compaction", "input_tokens": 180000, "output_tokens": 3500},
                {"type": "message", "input_tokens": 23000, "output_tokens": 1000,
                 "cache_read_input_tokens": 500}
            ]
        }));
        assert!(meter.final_seen());
        assert!(meter.itemized());
        assert_eq!(meter.counts().total(), 24000);
        assert_eq!(meter.billed(), 208000);
        // A step list that covers less than the top-level counts never
        // lowers the charge, and no step list changes nothing.
        let mut meter = Meter::default();
        meter.observe_final(&json!({"input_tokens": 10, "output_tokens": 5, "iterations": []}));
        assert_eq!(meter.billed(), 15);
        let mut meter = Meter::default();
        meter.observe_final(&json!({"input_tokens": 10, "output_tokens": 5}));
        assert!(!meter.itemized());
        assert_eq!(meter.billed(), 15);
        // Streamed: itemized usage arrives with the closing usage.
        let mut meter = Meter::default();
        meter.feed_events(
            concat!(
                "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":20,\"output_tokens\":1}}}\n\n",
                "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":9,\"iterations\":[",
                "{\"type\":\"compaction\",\"input_tokens\":300,\"output_tokens\":40},",
                "{\"type\":\"message\",\"input_tokens\":20,\"output_tokens\":9}]}}\n\n",
            )
            .as_bytes(),
        );
        assert!(meter.final_seen() && meter.itemized());
        assert_eq!(meter.billed(), 369);
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
