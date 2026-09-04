//! Shared, bounded timing budget for process-oriented integration tests.
//!
//! `QUORUM_TEST_TIMING_SCALE` scales process deadlines from the normal budget
//! (1) up to ten times that budget. `preflight.sh` and its timing collector
//! preserve this environment for Cargo test binaries, so a loaded macOS host
//! can run `QUORUM_TEST_TIMING_SCALE=3 rtk proxy ./preflight.sh` without
//! changing what the tests assert.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

pub const TEST_TIMING_SCALE_ENV: &str = "QUORUM_TEST_TIMING_SCALE";

const DEFAULT_SCALE: f64 = 1.0;
const MAX_SCALE: f64 = 10.0;
static SCALE: OnceLock<f64> = OnceLock::new();

/// Return a bounded duration for an integration-test process deadline.
pub fn budget(base: Duration) -> Duration {
    base.mul_f64(scale())
}

/// Return a deadline derived from the shared integration-test timing budget.
pub fn deadline(base: Duration) -> Instant {
    Instant::now() + budget(base)
}

fn scale() -> f64 {
    *SCALE.get_or_init(|| match std::env::var(TEST_TIMING_SCALE_ENV) {
        Ok(value) => parse_scale(&value)
            .unwrap_or_else(|error| panic!("invalid {TEST_TIMING_SCALE_ENV}={value:?}: {error}")),
        Err(std::env::VarError::NotPresent) => DEFAULT_SCALE,
        Err(error) => panic!("could not read {TEST_TIMING_SCALE_ENV}: {error}"),
    })
}

fn parse_scale(value: &str) -> Result<f64, &'static str> {
    let scale = value
        .parse::<f64>()
        .map_err(|_| "expected a number from 1 through 10")?;
    if !scale.is_finite() || !(DEFAULT_SCALE..=MAX_SCALE).contains(&scale) {
        return Err("expected a finite number from 1 through 10");
    }
    Ok(scale)
}

#[cfg(test)]
mod tests {
    use super::parse_scale;

    #[test]
    fn accepts_bounded_scale() {
        assert_eq!(parse_scale("1"), Ok(1.0));
        assert_eq!(parse_scale("2.5"), Ok(2.5));
        assert_eq!(parse_scale("10"), Ok(10.0));
    }

    #[test]
    fn rejects_unbounded_scale() {
        for value in ["0", "0.5", "11", "NaN", "infinite"] {
            assert!(parse_scale(value).is_err(), "accepted {value:?}");
        }
    }
}
