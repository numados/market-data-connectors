//! Resynchronization retry and quarantine policies.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::model::Symbol;

/// Decision returned by a `ResyncPolicy` before initiating an order book resync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResyncDecision {
    /// Proceed immediately with snapshot fetch or resubscription.
    Proceed,
    /// Wait until the specified instant before retrying.
    Wait(Instant),
    /// Symbol is quarantined due to repeated failures until the specified instant.
    Quarantine(Instant),
}

/// Category of resync failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResyncFailure {
    /// HTTP status code error (e.g. 429, 500).
    HttpError(u16),
    /// Snapshot arrived with sequence behind buffered deltas.
    StaleSnapshot,
    /// Transport or network connection error.
    Transport(String),
    /// Response parsing or JSON deserialization error.
    Parse(String),
    /// Buffer overflowed while waiting for snapshot.
    BufferOverflow,
}

/// Strategy deciding when to retry or quarantine order book resynchronizations.
pub trait ResyncPolicy: Send + 'static {
    /// Notifies the policy that a resync has started for `symbol`.
    fn on_started(&mut self, symbol: &Symbol, now: Instant);

    /// Notifies the policy that a resync has succeeded for `symbol`.
    fn on_completed(&mut self, symbol: &Symbol, now: Instant);

    /// Notifies the policy that a resync attempt failed for `symbol`.
    fn on_failed(&mut self, symbol: &Symbol, error: &ResyncFailure, now: Instant);

    /// Decides whether a resync can proceed or must wait/quarantine.
    fn decide(&mut self, symbol: &Symbol, now: Instant) -> ResyncDecision;
}

/// Default resync policy enforcing a per-symbol failure cooldown.
///
/// Healthy venues recover immediately (no cooldown on initial failure or success).
/// Cooldown is stamped on failure only.
#[derive(Debug, Clone)]
pub struct CooldownPolicy {
    cooldown: Duration,
    retry_deadlines: HashMap<Symbol, Instant>,
}

impl CooldownPolicy {
    /// Creates a new `CooldownPolicy` with the given retry cooldown duration.
    pub fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            retry_deadlines: HashMap::new(),
        }
    }

    /// Returns the configured cooldown duration.
    pub fn cooldown(&self) -> Duration {
        self.cooldown
    }
}

impl ResyncPolicy for CooldownPolicy {
    fn on_started(&mut self, _symbol: &Symbol, _now: Instant) {}

    fn on_completed(&mut self, symbol: &Symbol, _now: Instant) {
        self.retry_deadlines.remove(symbol);
    }

    fn on_failed(&mut self, symbol: &Symbol, _error: &ResyncFailure, now: Instant) {
        let deadline = now.checked_add(self.cooldown).unwrap_or(now);
        self.retry_deadlines.insert(symbol.clone(), deadline);
    }

    fn decide(&mut self, symbol: &Symbol, now: Instant) -> ResyncDecision {
        if let Some(&deadline) = self.retry_deadlines.get(symbol) {
            if now < deadline {
                return ResyncDecision::Wait(deadline);
            }
            self.retry_deadlines.remove(symbol);
        }
        ResyncDecision::Proceed
    }
}

/// Internal per-symbol resync lifecycle tracking for `TimeoutQuarantinePolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuarantineTracking {
    AwaitingSnapshot { started_at: Instant },
    Quarantined { until: Instant },
}

/// Resync policy enforcing a snapshot arrival timeout followed by quarantine.
///
/// If a snapshot does not arrive within `timeout` (default 5s), the symbol is quarantined
/// for `quarantine` (default 30s) during which no retries are allowed. A valid snapshot
/// lifts quarantine early.
#[derive(Debug, Clone)]
pub struct TimeoutQuarantinePolicy {
    timeout: Duration,
    quarantine: Duration,
    tracking: HashMap<Symbol, QuarantineTracking>,
}

impl Default for TimeoutQuarantinePolicy {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            quarantine: Duration::from_secs(30),
            tracking: HashMap::new(),
        }
    }
}

impl TimeoutQuarantinePolicy {
    /// Creates a new `TimeoutQuarantinePolicy` with the given snapshot timeout and quarantine duration.
    pub fn new(timeout: Duration, quarantine: Duration) -> Self {
        Self {
            timeout,
            quarantine,
            tracking: HashMap::new(),
        }
    }

    /// Returns the configured snapshot timeout duration.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns the configured quarantine duration.
    pub fn quarantine(&self) -> Duration {
        self.quarantine
    }
}

impl ResyncPolicy for TimeoutQuarantinePolicy {
    fn on_started(&mut self, symbol: &Symbol, now: Instant) {
        self.tracking.insert(
            symbol.clone(),
            QuarantineTracking::AwaitingSnapshot { started_at: now },
        );
    }

    fn on_completed(&mut self, symbol: &Symbol, _now: Instant) {
        self.tracking.remove(symbol);
    }

    fn on_failed(&mut self, symbol: &Symbol, _error: &ResyncFailure, now: Instant) {
        let until = now.checked_add(self.quarantine).unwrap_or(now);
        self.tracking
            .insert(symbol.clone(), QuarantineTracking::Quarantined { until });
    }

    fn decide(&mut self, symbol: &Symbol, now: Instant) -> ResyncDecision {
        if let Some(&track) = self.tracking.get(symbol) {
            match track {
                QuarantineTracking::Quarantined { until } => {
                    if now < until {
                        return ResyncDecision::Quarantine(until);
                    }
                    self.tracking.remove(symbol);
                    ResyncDecision::Proceed
                }
                QuarantineTracking::AwaitingSnapshot { started_at } => {
                    let deadline = started_at.checked_add(self.timeout).unwrap_or(started_at);
                    if now >= deadline {
                        let until = now.checked_add(self.quarantine).unwrap_or(now);
                        self.tracking
                            .insert(symbol.clone(), QuarantineTracking::Quarantined { until });
                        ResyncDecision::Quarantine(until)
                    } else {
                        ResyncDecision::Wait(deadline)
                    }
                }
            }
        } else {
            ResyncDecision::Proceed
        }
    }
}

impl TimeoutQuarantinePolicy {
    /// Returns when the current snapshot wait for `symbol` began, if one is tracked.
    pub fn awaiting_since(&self, symbol: &Symbol) -> Option<Instant> {
        match self.tracking.get(symbol) {
            Some(QuarantineTracking::AwaitingSnapshot { started_at }) => Some(*started_at),
            _ => None,
        }
    }

    /// Returns the instant until which `symbol` is held, if it is recorded in
    /// quarantine (whether or not that instant has passed yet).
    pub fn quarantined_until(&self, symbol: &Symbol) -> Option<Instant> {
        match self.tracking.get(symbol) {
            Some(QuarantineTracking::Quarantined { until }) => Some(*until),
            _ => None,
        }
    }

    /// Holds `symbol` until `until` regardless of the configured quarantine
    /// duration, for callers that decide the hold length themselves.
    pub fn hold_until(&mut self, symbol: &Symbol, until: Instant) {
        self.tracking
            .insert(symbol.clone(), QuarantineTracking::Quarantined { until });
    }

    /// Returns true if `symbol` is currently recorded in quarantine.
    pub fn is_quarantined(&self, symbol: &Symbol, now: Instant) -> bool {
        match self.tracking.get(symbol) {
            Some(QuarantineTracking::Quarantined { until }) => now < *until,
            _ => false,
        }
    }
}

/// Decision returned by a [`RejectionPolicy`] after a venue rejects a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionDecision {
    /// Retry is allowed again at the given instant.
    Cooldown(Instant),
    /// Repeated rejections: hold the symbol until the given instant.
    Quarantine(Instant),
}

/// Strategy deciding how long to hold a symbol after the venue rejects its subscription.
pub trait RejectionPolicy: Send + 'static {
    /// Records a rejection for `symbol` and returns how long to hold it.
    fn on_rejected(&mut self, symbol: &Symbol, now: Instant) -> RejectionDecision;

    /// Records a successful subscription for `symbol`, clearing its rejection history.
    fn on_accepted(&mut self, symbol: &Symbol);
}

/// Escalating rejection policy: rejections below `escalate_after` cool down for
/// `cooldown`; the `escalate_after`-th consecutive rejection (and every one after)
/// quarantines for `quarantine`. A successful subscription clears the count.
#[derive(Debug, Clone)]
pub struct EscalatingRejectionPolicy {
    cooldown: Duration,
    quarantine: Duration,
    escalate_after: u32,
    strikes: HashMap<Symbol, u32>,
}

impl EscalatingRejectionPolicy {
    /// Creates a policy; `escalate_after` is clamped to at least 1.
    pub fn new(cooldown: Duration, quarantine: Duration, escalate_after: u32) -> Self {
        Self {
            cooldown,
            quarantine,
            escalate_after: escalate_after.max(1),
            strikes: HashMap::new(),
        }
    }

    /// Returns the quarantine duration applied on escalation.
    pub fn quarantine(&self) -> Duration {
        self.quarantine
    }
}

impl RejectionPolicy for EscalatingRejectionPolicy {
    fn on_rejected(&mut self, symbol: &Symbol, now: Instant) -> RejectionDecision {
        let strikes = self.strikes.entry(symbol.clone()).or_insert(0);
        *strikes = strikes.saturating_add(1);
        if *strikes >= self.escalate_after {
            RejectionDecision::Quarantine(now.checked_add(self.quarantine).unwrap_or(now))
        } else {
            RejectionDecision::Cooldown(now.checked_add(self.cooldown).unwrap_or(now))
        }
    }

    fn on_accepted(&mut self, symbol: &Symbol) {
        self.strikes.remove(symbol);
    }
}

/// Reconnect backoff policy for connection retry scheduling.
pub trait BackoffPolicy: Send + Sync + 'static {
    /// Computes the delay after `attempt` earlier retries (0 for the first retry, which waits `initial`).
    fn next_delay(&mut self, attempt: u32) -> Duration;

    /// Resets policy state on successful connection.
    fn reset(&mut self);
}

/// Standard exponential reconnect backoff with configurable initial delay, maximum delay cap, and factor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExponentialBackoff {
    /// Initial delay on first reconnect attempt.
    pub initial: Duration,
    /// Maximum upper bound on delay.
    pub max: Duration,
    /// Exponential multiplication factor (default: 2).
    pub factor: u32,
}

impl ExponentialBackoff {
    /// Creates a new `ExponentialBackoff` with exponential factor 2.
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            factor: 2,
        }
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(15))
    }
}

impl BackoffPolicy for ExponentialBackoff {
    fn next_delay(&mut self, attempt: u32) -> Duration {
        let mult = 1u32.checked_shl(attempt.min(10)).unwrap_or(1024);
        (self.initial * mult).min(self.max)
    }

    fn reset(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cooldown_policy_proceeds_initially() {
        let mut policy = CooldownPolicy::new(Duration::from_secs(5));
        let sym = Symbol::from("BTCUSDT");
        let now = Instant::now();

        assert_eq!(policy.decide(&sym, now), ResyncDecision::Proceed);
    }

    #[test]
    fn test_cooldown_stamped_on_failure_only() {
        let mut policy = CooldownPolicy::new(Duration::from_secs(5));
        let sym = Symbol::from("BTCUSDT");
        let now = Instant::now();

        policy.on_failed(&sym, &ResyncFailure::StaleSnapshot, now);
        let expected_deadline = now + Duration::from_secs(5);

        assert_eq!(
            policy.decide(&sym, now + Duration::from_secs(2)),
            ResyncDecision::Wait(expected_deadline)
        );

        assert_eq!(
            policy.decide(&sym, now + Duration::from_secs(5)),
            ResyncDecision::Proceed
        );
    }

    #[test]
    fn test_cooldown_cleared_on_completed() {
        let mut policy = CooldownPolicy::new(Duration::from_secs(5));
        let sym = Symbol::from("BTCUSDT");
        let now = Instant::now();

        policy.on_failed(&sym, &ResyncFailure::HttpError(429), now);
        policy.on_completed(&sym, now + Duration::from_secs(1));

        assert_eq!(
            policy.decide(&sym, now + Duration::from_secs(2)),
            ResyncDecision::Proceed
        );
    }

    #[test]
    fn test_timeout_quarantine_policy_lifecycle() {
        let mut policy =
            TimeoutQuarantinePolicy::new(Duration::from_secs(5), Duration::from_secs(30));
        let sym = Symbol::from("BTC/USD");
        let t0 = Instant::now();

        // 1. Initial state: Proceed
        assert_eq!(policy.decide(&sym, t0), ResyncDecision::Proceed);

        // 2. Resync started at t0
        policy.on_started(&sym, t0);

        // At t0 + 2s: still waiting for snapshot
        assert_eq!(
            policy.decide(&sym, t0 + Duration::from_secs(2)),
            ResyncDecision::Wait(t0 + Duration::from_secs(5))
        );

        // At t0 + 5s: timeout expires -> transitions to quarantine for 30s
        let q_until = t0 + Duration::from_secs(5) + Duration::from_secs(30);
        assert_eq!(
            policy.decide(&sym, t0 + Duration::from_secs(5)),
            ResyncDecision::Quarantine(q_until)
        );

        // At t0 + 20s: still in quarantine
        assert_eq!(
            policy.decide(&sym, t0 + Duration::from_secs(20)),
            ResyncDecision::Quarantine(q_until)
        );

        // 3. Valid snapshot arrives at t0 + 25s while in quarantine -> lifts quarantine early!
        policy.on_completed(&sym, t0 + Duration::from_secs(25));
        assert_eq!(
            policy.decide(&sym, t0 + Duration::from_secs(25)),
            ResyncDecision::Proceed
        );

        // 4. Test explicit failure -> quarantined immediately
        let t1 = t0 + Duration::from_secs(100);
        policy.on_failed(&sym, &ResyncFailure::Transport("reset".into()), t1);
        assert_eq!(
            policy.decide(&sym, t1 + Duration::from_secs(10)),
            ResyncDecision::Quarantine(t1 + Duration::from_secs(30))
        );

        // Quarantine expires at t1 + 30s
        assert_eq!(
            policy.decide(&sym, t1 + Duration::from_secs(30)),
            ResyncDecision::Proceed
        );
    }

    #[test]
    fn test_exponential_backoff_progression_and_cap() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(0), Duration::from_millis(500));
        assert_eq!(backoff.next_delay(1), Duration::from_millis(1000));
        assert_eq!(backoff.next_delay(2), Duration::from_millis(2000));
        assert_eq!(backoff.next_delay(3), Duration::from_millis(4000));
        assert_eq!(backoff.next_delay(4), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(100), Duration::from_secs(4));
    }

    #[test]
    fn test_exponential_backoff_default_compatibility() {
        let mut backoff = ExponentialBackoff::default();
        let initial = backoff.initial;
        assert_eq!(backoff.next_delay(0), initial, "first retry waits initial");
        assert_eq!(backoff.next_delay(1), initial * 2);
        assert_eq!(backoff.next_delay(2), initial * 4);
        assert_eq!(backoff.next_delay(20), backoff.max);
    }

    #[test]
    fn test_first_retry_attempt_counter_maps_to_initial() {
        // Call sites hold a 1-based reconnect counter and pass `counter - 1`.
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(8));
        let delays: Vec<_> = (1u32..=6)
            .map(|counter| backoff.next_delay(counter.saturating_sub(1)))
            .collect();
        assert_eq!(delays[0], Duration::from_millis(500));
        assert_eq!(delays[1], Duration::from_millis(1000));
        assert_eq!(delays[5], Duration::from_secs(8));
    }

    #[test]
    fn test_rejection_policy_escalates_from_cooldown_to_quarantine() {
        let mut policy =
            EscalatingRejectionPolicy::new(Duration::from_secs(5), Duration::from_secs(60), 2);
        let sym = Symbol::from("BTC/USD");
        let t0 = Instant::now();

        assert_eq!(
            policy.on_rejected(&sym, t0),
            RejectionDecision::Cooldown(t0 + Duration::from_secs(5))
        );
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(
            policy.on_rejected(&sym, t1),
            RejectionDecision::Quarantine(t1 + Duration::from_secs(60))
        );
    }

    #[test]
    fn test_rejection_policy_acceptance_clears_history_per_symbol() {
        let mut policy =
            EscalatingRejectionPolicy::new(Duration::from_secs(5), Duration::from_secs(60), 2);
        let a = Symbol::from("BTC/USD");
        let b = Symbol::from("ETH/USD");
        let t0 = Instant::now();

        policy.on_rejected(&a, t0);
        policy.on_rejected(&b, t0);
        policy.on_accepted(&a);

        assert!(matches!(
            policy.on_rejected(&a, t0),
            RejectionDecision::Cooldown(_)
        ));
        assert!(matches!(
            policy.on_rejected(&b, t0),
            RejectionDecision::Quarantine(_)
        ));
    }
}
