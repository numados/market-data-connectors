//! Deterministic per-symbol synchronization state machine for Kraken WebSocket v2.

use std::time::{Duration, Instant};

use mdc_core::Symbol;
use mdc_core::resync::{
    EscalatingRejectionPolicy, RejectionDecision, RejectionPolicy, ResyncDecision, ResyncPolicy,
    TimeoutQuarantinePolicy,
};

/// Per-symbol order book synchronization lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Awaiting a valid, checksum-verified order book snapshot.
    AwaitingSnapshot {
        /// Instant when the snapshot wait commenced.
        started_at: Instant,
    },
    /// In synchronization; processing continuous checksum-verified updates.
    InSync,
    /// Quarantined after repeated failure or snapshot timeout.
    Quarantined {
        /// Instant until which the symbol remains in quarantine.
        until: Instant,
    },
}

/// Outcome of applying an order book snapshot to the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// Snapshot was valid and accepted, transitioning the symbol to `InSync`.
    Accepted,
    /// Snapshot arrived without a valid checksum; dropped, remaining in current state.
    RejectedBadChecksum,
}

/// Outcome of applying an order book update to the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// Update was accepted on the synchronized hot path.
    Accepted,
    /// Update was dropped because the symbol is not in `InSync` state.
    DroppedNotSynchronized,
    /// Checksum verification failed; triggers alert, resync, and transition to `AwaitingSnapshot`.
    MismatchTriggerResync {
        /// Expected checksum from message.
        expected: u32,
        /// Checksum calculated locally.
        calculated: u32,
    },
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

/// Outcome of recording a subscription rejection for a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionOutcome {
    /// First rejection: retry is allowed again once `until` passes.
    CoolingDown {
        /// Instant at which the symbol may be resubscribed.
        until: Instant,
    },
    /// Repeated rejections: the symbol is held in quarantine until `until`.
    Quarantined {
        /// Instant until which the symbol remains in quarantine.
        until: Instant,
    },
}

/// Per-symbol synchronization controller.
///
/// The timeout-quarantine policy owns every wait and hold timestamp and the
/// rejection policy owns escalation history; the controller only tracks
/// whether the book is synchronized and maps policy decisions to outcomes.
pub struct SymbolFsm {
    symbol: Symbol,
    in_sync: bool,
    policy: TimeoutQuarantinePolicy,
    rejection: Box<dyn RejectionPolicy>,
}

impl std::fmt::Debug for SymbolFsm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolFsm")
            .field("in_sync", &self.in_sync)
            .field("state", &self.state())
            .finish()
    }
}

impl SymbolFsm {
    /// Creates a `SymbolFsm` for `symbol` starting in `AwaitingSnapshot` with the
    /// default rejection policy (first rejection cools down for `timeout`, the
    /// second quarantines for `quarantine`).
    pub fn new(
        symbol: Symbol,
        started_at: Instant,
        timeout: Duration,
        quarantine: Duration,
    ) -> Self {
        Self::with_rejection_policy(
            symbol,
            started_at,
            timeout,
            quarantine,
            Box::new(EscalatingRejectionPolicy::new(timeout, quarantine, 2)),
        )
    }

    /// Creates a `SymbolFsm` for `symbol` with an injected rejection policy.
    /// The policy observes `symbol` in every callback.
    pub fn with_rejection_policy(
        symbol: Symbol,
        started_at: Instant,
        timeout: Duration,
        quarantine: Duration,
        rejection: Box<dyn RejectionPolicy>,
    ) -> Self {
        let mut policy = TimeoutQuarantinePolicy::new(timeout, quarantine);
        policy.on_started(&symbol, started_at);
        Self {
            symbol,
            in_sync: false,
            policy,
            rejection,
        }
    }

    fn begin_wait(&mut self, now: Instant) {
        self.in_sync = false;
        self.policy.on_started(&self.symbol, now);
    }

    /// Returns the current synchronization state, derived from the policy.
    /// A rejection cooldown reports as `Quarantined` until it expires.
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
    pub fn on_snapshot(&mut self, is_valid_checksum: bool) -> SnapshotOutcome {
        if is_valid_checksum {
            self.in_sync = true;
            self.policy.on_completed(&self.symbol, Instant::now());
            self.rejection.on_accepted(&self.symbol);
            SnapshotOutcome::Accepted
        } else {
            SnapshotOutcome::RejectedBadChecksum
        }
    }

    /// Handles arrival of an order book update.
    pub fn on_update(
        &mut self,
        is_valid_checksum: bool,
        expected: u32,
        calculated: u32,
        now: Instant,
    ) -> UpdateOutcome {
        if !self.in_sync {
            return UpdateOutcome::DroppedNotSynchronized;
        }
        if is_valid_checksum {
            UpdateOutcome::Accepted
        } else {
            self.begin_wait(now);
            UpdateOutcome::MismatchTriggerResync {
                expected,
                calculated,
            }
        }
    }

    /// Polls periodic timers (resync timeout and quarantine expiry).
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

    /// Requests an explicit resync, transitioning the symbol back to `AwaitingSnapshot`.
    pub fn request_resync(&mut self, now: Instant) {
        self.begin_wait(now);
    }

    /// Explicitly resets the symbol (e.g. upon socket reconnect).
    pub fn reset(&mut self, now: Instant) {
        self.begin_wait(now);
    }

    /// Records a venue subscription rejection and holds the symbol as the
    /// rejection policy decides; the poll loop resubscribes once the hold ends.
    pub fn on_subscription_rejected(&mut self, now: Instant) -> RejectionOutcome {
        self.in_sync = false;
        match self.rejection.on_rejected(&self.symbol, now) {
            RejectionDecision::Cooldown(until) => {
                self.policy.hold_until(&self.symbol, until);
                RejectionOutcome::CoolingDown { until }
            }
            RejectionDecision::Quarantine(until) => {
                self.policy.hold_until(&self.symbol, until);
                RejectionOutcome::Quarantined { until }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_fsm_happy_path() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(
            Symbol::from("BTC/USD"),
            t0,
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        assert!(matches!(fsm.state(), SyncState::AwaitingSnapshot { .. }));

        // Updates while awaiting snapshot are dropped
        assert_eq!(
            fsm.on_update(true, 100, 100, t0),
            UpdateOutcome::DroppedNotSynchronized
        );

        // Invalid snapshot rejected
        assert_eq!(fsm.on_snapshot(false), SnapshotOutcome::RejectedBadChecksum);
        assert!(!fsm.is_in_sync());

        // Valid snapshot accepted -> InSync
        assert_eq!(fsm.on_snapshot(true), SnapshotOutcome::Accepted);
        assert!(fsm.is_in_sync());

        // Update with valid checksum accepted
        assert_eq!(
            fsm.on_update(true, 200, 200, t0 + Duration::from_secs(1)),
            UpdateOutcome::Accepted
        );
    }

    #[test]
    fn test_fsm_checksum_mismatch_triggers_resync() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(
            Symbol::from("BTC/USD"),
            t0,
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        fsm.on_snapshot(true);
        assert!(fsm.is_in_sync());

        let t1 = t0 + Duration::from_secs(2);
        let outcome = fsm.on_update(false, 999, 888, t1);
        assert_eq!(
            outcome,
            UpdateOutcome::MismatchTriggerResync {
                expected: 999,
                calculated: 888
            }
        );
        assert!(matches!(
            fsm.state(),
            SyncState::AwaitingSnapshot { started_at } if started_at == t1
        ));
    }

    #[test]
    fn test_fsm_timeout_and_quarantine_lifecycle() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(
            Symbol::from("BTC/USD"),
            t0,
            Duration::from_secs(5),
            Duration::from_secs(30),
        );

        // Poll at t0 + 2s -> None
        assert_eq!(fsm.poll(t0 + Duration::from_secs(2)), PollOutcome::None);

        // Poll at t0 + 5s -> TimeoutQuarantined until t0 + 35s
        let q_until = t0 + Duration::from_secs(5) + Duration::from_secs(30);
        assert_eq!(
            fsm.poll(t0 + Duration::from_secs(5)),
            PollOutcome::TimeoutQuarantined { until: q_until }
        );
        assert!(matches!(fsm.state(), SyncState::Quarantined { .. }));

        // Updates while quarantined are dropped
        assert_eq!(
            fsm.on_update(true, 100, 100, t0 + Duration::from_secs(10)),
            UpdateOutcome::DroppedNotSynchronized
        );

        // Poll at t0 + 20s -> None
        assert_eq!(fsm.poll(t0 + Duration::from_secs(20)), PollOutcome::None);

        // Poll at t0 + 35s -> QuarantineExpiredTriggerResubscribe -> AwaitingSnapshot
        assert_eq!(
            fsm.poll(t0 + Duration::from_secs(35)),
            PollOutcome::QuarantineExpiredTriggerResubscribe
        );
        assert!(matches!(fsm.state(), SyncState::AwaitingSnapshot { .. }));
    }

    #[test]
    fn test_fsm_valid_snapshot_lifts_quarantine_early() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(
            Symbol::from("BTC/USD"),
            t0,
            Duration::from_secs(5),
            Duration::from_secs(30),
        );

        // Timeout into quarantine
        fsm.poll(t0 + Duration::from_secs(5));
        assert!(matches!(fsm.state(), SyncState::Quarantined { .. }));

        // Snapshot arrives during quarantine -> InSync immediately!
        assert_eq!(fsm.on_snapshot(true), SnapshotOutcome::Accepted);
        assert!(fsm.is_in_sync());
    }

    #[test]
    fn test_first_rejection_cools_down_then_retry_is_allowed() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::with_rejection_policy(
            Symbol::from("BTC/USD"),
            t0,
            Duration::from_secs(5),
            Duration::from_secs(30),
            Box::new(EscalatingRejectionPolicy::new(
                Duration::from_secs(2),
                Duration::from_secs(30),
                2,
            )),
        );
        fsm.on_snapshot(true);

        let until = t0 + Duration::from_secs(2);
        assert_eq!(
            fsm.on_subscription_rejected(t0),
            RejectionOutcome::CoolingDown { until }
        );
        assert!(!fsm.is_in_sync());
        assert_eq!(
            fsm.on_update(true, 1, 1, t0),
            UpdateOutcome::DroppedNotSynchronized
        );

        // Held during the cooldown, resubscribe triggered when it ends.
        assert_eq!(fsm.poll(t0 + Duration::from_secs(1)), PollOutcome::None);
        assert_eq!(
            fsm.poll(until),
            PollOutcome::QuarantineExpiredTriggerResubscribe
        );
        assert!(matches!(fsm.state(), SyncState::AwaitingSnapshot { .. }));
    }

    #[test]
    fn test_repeated_rejection_escalates_to_quarantine() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(
            Symbol::from("BTC/USD"),
            t0,
            Duration::from_secs(5),
            Duration::from_secs(30),
        );

        assert!(matches!(
            fsm.on_subscription_rejected(t0),
            RejectionOutcome::CoolingDown { .. }
        ));
        let t1 = t0 + Duration::from_secs(6);
        assert_eq!(
            fsm.poll(t1),
            PollOutcome::QuarantineExpiredTriggerResubscribe
        );
        assert_eq!(
            fsm.on_subscription_rejected(t1),
            RejectionOutcome::Quarantined {
                until: t1 + Duration::from_secs(30)
            }
        );
        assert_eq!(
            fsm.state(),
            SyncState::Quarantined {
                until: t1 + Duration::from_secs(30)
            }
        );
    }

    #[test]
    fn test_valid_snapshot_clears_rejection_history() {
        let t0 = Instant::now();
        let mut fsm = SymbolFsm::new(
            Symbol::from("BTC/USD"),
            t0,
            Duration::from_secs(5),
            Duration::from_secs(30),
        );
        fsm.on_subscription_rejected(t0);
        assert_eq!(fsm.on_snapshot(true), SnapshotOutcome::Accepted);
        assert!(matches!(
            fsm.on_subscription_rejected(t0),
            RejectionOutcome::CoolingDown { .. }
        ));
    }
}
