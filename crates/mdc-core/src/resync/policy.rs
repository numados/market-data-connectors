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
}
