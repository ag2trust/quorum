//! Bounded, in-memory cooldown for model profiles that a managed run recently
//! classified as provider- or profile-unavailable.
//!
//! The durable `routing_attempts` evidence is scoped to one responsibility, so
//! a fresh reviewer responsibility would otherwise launch straight onto a
//! profile that another reviewer just proved unavailable. The cooldown carries
//! that observation across responsibilities for a short, fixed window. It is
//! deliberately not durable: a daemon restart at worst launches one more probe
//! of the blocked profile, and the durable strike budget still bounds the total
//! number of attempts.
//!
//! The window is opened by the first classified failure and is not extended by
//! later failures while it is live. A profile is therefore re-probed at most
//! once per window, and the cooldown's own route skips (which flow through the
//! same classification funnel) cannot keep a profile cooling forever.

use quorum_core::role_assignments::{ModelProfile, ValidatedPool};
use quorum_core::routing_attempts::RouteExclusions;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How long a profile is skipped after a classified unavailability failure.
const PROFILE_COOLDOWN: Duration = Duration::from_secs(120);
/// Ceiling on that window regardless of configuration or test override.
const PROFILE_COOLDOWN_MAX: Duration = Duration::from_secs(300);
/// Hard bound on tracked profiles; expired entries are pruned first.
pub(super) const PROFILE_COOLDOWN_CAPACITY: usize = 64;
/// At most one deferral log line per interval across all reviewer work.
const DEFERRAL_LOG_INTERVAL: Duration = Duration::from_secs(60);
#[cfg(debug_assertions)]
const MIN_TEST_PROFILE_COOLDOWN: Duration = Duration::from_millis(10);

/// Test-only override for the cooldown window, bounded the same way as
/// `reviewer_respawn_backoff_base` so a fixture cannot disable the cooldown or
/// extend it.
#[cfg(debug_assertions)]
pub(super) fn profile_cooldown_duration() -> Duration {
    let default_ms = PROFILE_COOLDOWN.as_millis() as u64;
    let millis = std::env::var("QUORUM_TEST_PROFILE_COOLDOWN_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_ms)
        .clamp(MIN_TEST_PROFILE_COOLDOWN.as_millis() as u64, default_ms);
    Duration::from_millis(millis).min(PROFILE_COOLDOWN_MAX)
}

#[cfg(not(debug_assertions))]
pub(super) fn profile_cooldown_duration() -> Duration {
    PROFILE_COOLDOWN.min(PROFILE_COOLDOWN_MAX)
}

/// What reviewer provisioning must do with its assigned profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CooldownPlan {
    /// The assigned profile is not cooling down; launch it as usual.
    Run,
    /// The assigned profile is cooling down and at least one other profile in
    /// the validated pool is neither cooling down nor excluded by the
    /// responsibility's routing evidence. Route through the alternate path.
    SkipAssigned,
    /// No eligible profile remains. Do not spawn before `until`.
    Defer { until: Instant },
}

/// Per-profile cooldown deadlines owned by the serve loop.
pub(super) struct ProfileCooldown {
    until: HashMap<String, Instant>,
    last_deferral_log: Option<Instant>,
}

impl ProfileCooldown {
    pub(super) fn new() -> Self {
        Self {
            until: HashMap::new(),
            last_deferral_log: None,
        }
    }

    /// Open a cooldown window for `profile_id` and return its deadline. A live
    /// window is kept as is. The map never exceeds
    /// [`PROFILE_COOLDOWN_CAPACITY`]: expired entries are pruned first, then the
    /// entry closest to expiry is evicted.
    pub(super) fn record(&mut self, profile_id: &str, now: Instant) -> Instant {
        self.until.retain(|_, until| *until > now);
        if let Some(until) = self.until.get(profile_id) {
            return *until;
        }
        if self.until.len() >= PROFILE_COOLDOWN_CAPACITY {
            if let Some(oldest) = self
                .until
                .iter()
                .min_by_key(|(_, until)| **until)
                .map(|(id, _)| id.clone())
            {
                self.until.remove(&oldest);
            }
        }
        let until = now + profile_cooldown_duration();
        self.until.insert(profile_id.to_string(), until);
        until
    }

    /// Whether `profile_id` must still be skipped at `now`.
    pub(super) fn is_cooling(&self, profile_id: &str, now: Instant) -> bool {
        self.until.get(profile_id).is_some_and(|until| *until > now)
    }

    fn deadline(&self, profile_id: &str, now: Instant) -> Option<Instant> {
        self.until
            .get(profile_id)
            .copied()
            .filter(|until| *until > now)
    }

    /// Decide how to route a reviewer whose assignment names `assigned`.
    ///
    /// `pool` is the assignment's validated pool and `exclusions` is the
    /// responsibility's durable routing evidence. The skip is recorded as a
    /// profile-scoped exclusion, so any other non-excluded, non-cooling profile
    /// is reachable by the alternate-route walk regardless of its order.
    pub(super) fn plan(
        &self,
        assigned: &ModelProfile,
        pool: &ValidatedPool,
        exclusions: &RouteExclusions,
        now: Instant,
    ) -> CooldownPlan {
        let Some(assigned_until) = self.deadline(&assigned.id, now) else {
            return CooldownPlan::Run;
        };
        let mut until = assigned_until;
        for weighted in &pool.profiles {
            let profile = &weighted.profile;
            if profile.id == assigned.id || exclusions.excludes(profile) {
                continue;
            }
            match self.deadline(&profile.id, now) {
                None => return CooldownPlan::SkipAssigned,
                Some(deadline) => until = until.min(deadline),
            }
        }
        CooldownPlan::Defer { until }
    }

    /// Whether a deferral may be logged at `now`; bounds deferral log volume to
    /// one line per [`DEFERRAL_LOG_INTERVAL`].
    pub(super) fn take_deferral_log_slot(&mut self, now: Instant) -> bool {
        match self.last_deferral_log {
            Some(last) if now.saturating_duration_since(last) < DEFERRAL_LOG_INTERVAL => false,
            _ => {
                self.last_deferral_log = Some(now);
                true
            }
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.until.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::role_assignments::WeightedProfile;

    fn profile(id: &str, provider: &str) -> ModelProfile {
        ModelProfile {
            id: id.into(),
            provider: provider.into(),
            runner: provider.into(),
            model: format!("{id}-model"),
            effort: "high".into(),
        }
    }

    fn pool(profiles: &[(&str, &str)]) -> ValidatedPool {
        ValidatedPool {
            pool_key: "reviewer.3.r1".into(),
            policy_generation: "generation-1".into(),
            profiles: profiles
                .iter()
                .map(|(id, provider)| WeightedProfile {
                    profile: profile(id, provider),
                    percent: 50,
                })
                .collect(),
        }
    }

    #[test]
    fn duration_is_positive_and_capped() {
        let duration = profile_cooldown_duration();
        assert!(duration > Duration::ZERO);
        assert!(duration <= PROFILE_COOLDOWN_MAX);
    }

    #[test]
    fn cooldown_expires_and_no_longer_affects_selection() {
        let mut cooldown = ProfileCooldown::new();
        let now = Instant::now();
        let until = cooldown.record("a", now);
        assert_eq!(until, now + profile_cooldown_duration());
        assert!(cooldown.is_cooling("a", now));
        assert!(cooldown.is_cooling("a", until - Duration::from_millis(1)));
        assert!(!cooldown.is_cooling("a", until), "dead at until <= now");
        assert!(!cooldown.is_cooling("b", now));

        let pool = pool(&[("a", "codex"), ("b", "claude")]);
        let exclusions = RouteExclusions::default();
        assert_eq!(
            cooldown.plan(&profile("a", "codex"), &pool, &exclusions, until),
            CooldownPlan::Run
        );
    }

    #[test]
    fn live_window_is_not_extended_by_a_repeated_failure() {
        let mut cooldown = ProfileCooldown::new();
        let now = Instant::now();
        let first = cooldown.record("a", now);
        let later = now + Duration::from_millis(5);
        assert_eq!(cooldown.record("a", later), first);
        // After expiry a new failure opens a fresh window.
        assert_eq!(
            cooldown.record("a", first),
            first + profile_cooldown_duration()
        );
    }

    #[test]
    fn capacity_bound_holds_and_evicts_the_entry_closest_to_expiry() {
        let mut cooldown = ProfileCooldown::new();
        let start = Instant::now();
        for index in 0..(PROFILE_COOLDOWN_CAPACITY + 16) {
            cooldown.record(
                &format!("profile-{index}"),
                start + Duration::from_micros(index as u64),
            );
            assert!(cooldown.len() <= PROFILE_COOLDOWN_CAPACITY);
        }
        assert_eq!(cooldown.len(), PROFILE_COOLDOWN_CAPACITY);
        let probe = start + Duration::from_micros((PROFILE_COOLDOWN_CAPACITY + 16) as u64);
        // The oldest windows were evicted; the newest survive.
        assert!(!cooldown.is_cooling("profile-0", probe));
        assert!(!cooldown.is_cooling("profile-15", probe));
        assert!(cooldown.is_cooling("profile-16", probe));
        assert!(cooldown.is_cooling(
            &format!("profile-{}", PROFILE_COOLDOWN_CAPACITY + 15),
            probe
        ));
    }

    #[test]
    fn expired_entries_are_pruned_before_eviction() {
        let mut cooldown = ProfileCooldown::new();
        let start = Instant::now();
        for index in 0..PROFILE_COOLDOWN_CAPACITY {
            cooldown.record(&format!("old-{index}"), start);
        }
        cooldown.record("live", start + Duration::from_millis(1));
        assert_eq!(cooldown.len(), PROFILE_COOLDOWN_CAPACITY);
        // Every old window is expired at this point: one record prunes them
        // all instead of evicting the live entry.
        let after = start + profile_cooldown_duration();
        cooldown.record("fresh", after);
        assert_eq!(cooldown.len(), 2);
        assert!(cooldown.is_cooling("live", after));
        assert!(cooldown.is_cooling("fresh", after));
    }

    #[test]
    fn plan_skips_a_cooling_assignment_for_an_eligible_alternate() {
        let mut cooldown = ProfileCooldown::new();
        let now = Instant::now();
        cooldown.record("a", now);
        let pool = pool(&[("a", "codex"), ("b", "claude")]);
        assert_eq!(
            cooldown.plan(
                &profile("a", "codex"),
                &pool,
                &RouteExclusions::default(),
                now
            ),
            CooldownPlan::SkipAssigned
        );
    }

    #[test]
    fn plan_defers_until_the_earliest_relevant_cooldown_expires() {
        let mut cooldown = ProfileCooldown::new();
        let now = Instant::now();
        let a_until = cooldown.record("a", now);
        let b_until = cooldown.record("b", now + Duration::from_millis(1));
        assert!(b_until > a_until);
        let pool = pool(&[("a", "codex"), ("b", "claude")]);
        let assigned = profile("b", "claude");
        assert_eq!(
            cooldown.plan(&assigned, &pool, &RouteExclusions::default(), now),
            CooldownPlan::Defer { until: a_until }
        );
        // Once the earliest window lapses the alternate is eligible again.
        assert_eq!(
            cooldown.plan(&assigned, &pool, &RouteExclusions::default(), a_until),
            CooldownPlan::SkipAssigned
        );
        assert_eq!(
            cooldown.plan(&assigned, &pool, &RouteExclusions::default(), b_until),
            CooldownPlan::Run
        );
    }

    #[test]
    fn deferral_logs_are_rate_limited() {
        let mut cooldown = ProfileCooldown::new();
        let now = Instant::now();
        assert!(cooldown.take_deferral_log_slot(now));
        assert!(!cooldown.take_deferral_log_slot(now + Duration::from_secs(1)));
        assert!(cooldown.take_deferral_log_slot(now + DEFERRAL_LOG_INTERVAL));
    }
}
