//! Deterministic per-symbol synchronization state machine for Coinbase Exchange.

use std::time::{Duration, Instant};

/// Per-product order book synchronization lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Awaiting an order book snapshot from `level2_batch` subscription.
    AwaitingSnapshot {
        /// Instant when the snapshot wait commenced.
        started_at: Instant,
    },
    /// In synchronization; processing continuous level updates.
    InSync,
    /// Quarantined after repeated snapshot timeout.
    Quarantined {
        /// Instant until which the symbol remains in quarantine.
        until: Instant,
    },
}

/// Outcome of applying an order book snapshot to the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// Snapshot was accepted, transitioning the symbol to `InSync`.
    Accepted,
}

/// Outcome of applying an order book update to the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// Update was accepted on the synchronized hot path.
    Accepted,
    /// Update was dropped because the symbol is not in `InSync` state.
    DroppedNotSynchronized,
}

/// Outcome of polling the state machine timers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    /// No timer state changes.
    None,
    /// Snapshot wait exceeded timeout; transitioned to quarantine.
    TimeoutQuarantined {
        /// Instant until which quarantine lasts.
        until: Instant,
    },
    /// Quarantine duration elapsed; ready for resubscription.
    QuarantineExpiredTriggerResubscribe,
}

use mdc_core::Symbol;
use mdc_core::resync::{ResyncDecision, ResyncPolicy, TimeoutQuarantinePolicy};

/// Per-product synchronization controller.
#[derive(Debug, Clone)]
pub struct SymbolFsm {
    symbol: Symbol,
    in_sync: bool,
    policy: TimeoutQuarantinePolicy,
}

impl SymbolFsm {
    /// Creates a new `SymbolFsm` starting in `AwaitingSnapshot` with the given timeouts.
    pub fn new(started_at: Instant, timeout: Duration, quarantine: Duration) -> Self {
        let symbol = Symbol::from("product");
        let mut policy = TimeoutQuarantinePolicy::new(timeout, quarantine);
        policy.on_started(&symbol, started_at);
        Self {
            symbol,
            in_sync: false,
            policy,
        }
    }

    fn begin_wait(&mut self, now: Instant) {
        self.in_sync = false;
        self.policy.on_started(&self.symbol, now);
    }

    /// Returns the current synchronization state.
    ///
    /// Derived from the timeout-quarantine policy, which owns the wait and
    /// quarantine timestamps; only the in-sync flag lives here.
    pub fn state(&self) -> SyncState {
        if self.in_sync {
            SyncState::InSync
        } else if let Some(until) = self.policy.quarantined_until(&self.symbol) {
            SyncState::Quarantined { until }
        } else if let Some(started_at) = self.policy.awaiting_since(&self.symbol) {
            SyncState::AwaitingSnapshot { started_at }
        } else {
            // Unreachable by construction: every non-synced path records
            // `on_started`. Fail safe as a fresh wait.
            SyncState::AwaitingSnapshot {
                started_at: Instant::now(),
            }
        }
    }

    /// Returns true if the symbol is in the `InSync` state.
    pub fn is_in_sync(&self) -> bool {
        self.in_sync
    }

    /// Handles arrival of an order book snapshot.
    pub fn on_snapshot(&mut self) -> SnapshotOutcome {
        self.in_sync = true;
        self.policy.on_completed(&self.symbol, Instant::now());
        SnapshotOutcome::Accepted
    }

    /// Handles arrival of an incremental order book update.
    pub fn on_update(&self) -> UpdateOutcome {
        if self.in_sync {
            UpdateOutcome::Accepted
        } else {
            UpdateOutcome::DroppedNotSynchronized
        }
    }

    /// Requests an explicit resync.
    ///
    /// Returns `true` if a transition to `AwaitingSnapshot` occurred and a new resubscribe should
    /// be triggered. Returns `false` if the symbol is already `AwaitingSnapshot` within timeout or
    /// in quarantine to prevent resubscribe storms.
    pub fn request_resync(&mut self, now: Instant) -> bool {
        match self.state() {
            SyncState::InSync => {
                self.begin_wait(now);
                true
            }
            SyncState::AwaitingSnapshot { .. } => match self.policy.decide(&self.symbol, now) {
                ResyncDecision::Wait(_) => false,
                _ => {
                    self.begin_wait(now);
                    true
                }
            },
            SyncState::Quarantined { .. } => false,
        }
    }

    /// Polls periodic timers (snapshot timeout and quarantine expiry).
    pub fn poll(&mut self, now: Instant) -> PollOutcome {
        match self.state() {
            SyncState::InSync => PollOutcome::None,
            SyncState::AwaitingSnapshot { .. } => match self.policy.decide(&self.symbol, now) {
                ResyncDecision::Quarantine(until) => PollOutcome::TimeoutQuarantined { until },
                _ => PollOutcome::None,
            },
            SyncState::Quarantined { .. } => match self.policy.decide(&self.symbol, now) {
                ResyncDecision::Proceed => {
                    self.begin_wait(now);
                    PollOutcome::QuarantineExpiredTriggerResubscribe
                }
                _ => PollOutcome::None,
            },
        }
    }

    /// Explicitly resets the symbol (e.g. upon socket reconnect).
    pub fn reset(&mut self, now: Instant) {
        self.begin_wait(now);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_fsm_happy_path() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(t0, Duration::from_secs(5), Duration::from_secs(30));
        assert!(matches!(fsm.state(), SyncState::AwaitingSnapshot { .. }));

        // Updates while awaiting snapshot are dropped
        assert_eq!(fsm.on_update(), UpdateOutcome::DroppedNotSynchronized);

        // Snapshot arrives -> InSync
        assert_eq!(fsm.on_snapshot(), SnapshotOutcome::Accepted);
        assert!(fsm.is_in_sync());

        // Update while InSync -> Accepted
        assert_eq!(fsm.on_update(), UpdateOutcome::Accepted);
    }

    #[test]
    fn test_fsm_resync_storm_prevention() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(t0, Duration::from_secs(5), Duration::from_secs(30));
        fsm.on_snapshot();
        assert!(fsm.is_in_sync());

        // First resync request -> true
        assert!(fsm.request_resync(t0));
        assert!(matches!(fsm.state(), SyncState::AwaitingSnapshot { .. }));

        // Second resync request within 5s -> false (storm prevented)
        assert!(!fsm.request_resync(t0 + Duration::from_secs(2)));

        // Resync request after 5s timeout -> true
        assert!(fsm.request_resync(t0 + Duration::from_secs(6)));
    }

    #[test]
    fn test_fsm_timeout_quarantine_lifecycle() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(t0, Duration::from_secs(5), Duration::from_secs(30));

        // Poll at 2s -> None
        assert_eq!(fsm.poll(t0 + Duration::from_secs(2)), PollOutcome::None);

        // Poll at 5s -> TimeoutQuarantined
        let q_until = t0 + Duration::from_secs(5) + Duration::from_secs(30);
        assert_eq!(
            fsm.poll(t0 + Duration::from_secs(5)),
            PollOutcome::TimeoutQuarantined { until: q_until }
        );
        assert!(matches!(fsm.state(), SyncState::Quarantined { .. }));

        // Updates while quarantined are dropped
        assert_eq!(fsm.on_update(), UpdateOutcome::DroppedNotSynchronized);

        // Poll at 35s -> QuarantineExpiredTriggerResubscribe
        assert_eq!(
            fsm.poll(t0 + Duration::from_secs(35)),
            PollOutcome::QuarantineExpiredTriggerResubscribe
        );
        assert!(matches!(fsm.state(), SyncState::AwaitingSnapshot { .. }));

        // Snapshot arriving during quarantine immediately lifts quarantine
        fsm.poll(t0 + Duration::from_secs(5));
        assert_eq!(fsm.on_snapshot(), SnapshotOutcome::Accepted);
        assert!(fsm.is_in_sync());
    }

    #[test]
    fn test_state_is_derived_from_policy_timestamps() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(t0, Duration::from_secs(5), Duration::from_secs(30));
        assert_eq!(fsm.state(), SyncState::AwaitingSnapshot { started_at: t0 });

        let t_timeout = t0 + Duration::from_secs(5);
        assert!(matches!(
            fsm.poll(t_timeout),
            PollOutcome::TimeoutQuarantined { .. }
        ));
        assert_eq!(
            fsm.state(),
            SyncState::Quarantined {
                until: t_timeout + Duration::from_secs(30)
            }
        );

        // Snapshot lifts quarantine immediately; the policy forgets the symbol.
        fsm.on_snapshot();
        assert_eq!(fsm.state(), SyncState::InSync);
        assert_eq!(
            fsm.poll(t_timeout + Duration::from_secs(60)),
            PollOutcome::None
        );
    }
}
