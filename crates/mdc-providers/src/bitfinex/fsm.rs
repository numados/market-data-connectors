//! Deterministic per-channel synchronization state machine for Bitfinex WebSocket v2.

use std::time::{Duration, Instant};

/// Per-symbol/channel order book synchronization lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Awaiting a fresh, complete order book snapshot from the server.
    AwaitingSnapshot {
        /// Instant when the snapshot wait began.
        started_at: Instant,
    },
    /// In synchronization; processing continuous updates verified by CRC32 checksums.
    InSync,
    /// Quarantined after snapshot wait timeout elapsed without a valid snapshot.
    Quarantined {
        /// Instant until which the symbol remains in quarantine.
        until: Instant,
    },
}

/// Outcome of applying an order book snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// Snapshot accepted, transitioning the symbol to `InSync`.
    Accepted,
}

/// Outcome of applying an order book update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// Update accepted on the synchronized hot path.
    Accepted,
    /// Update dropped because the symbol is not in `InSync` state.
    DroppedNotSynchronized,
}

/// Outcome of periodic state machine timer inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    /// No timer state changes.
    None,
    /// Snapshot wait exceeded timeout; transitioned to quarantine.
    TimeoutQuarantined {
        /// Instant until which quarantine lasts.
        until: Instant,
    },
    /// Quarantine duration elapsed; ready for fresh resubscription.
    QuarantineExpiredTriggerResubscribe,
}

use mdc_core::resync::{ResyncDecision, ResyncPolicy, TimeoutQuarantinePolicy};
use mdc_core::Symbol;

/// Per-channel synchronization controller.
#[derive(Debug, Clone)]
pub struct ChannelFsm {
    symbol: Symbol,
    state: SyncState,
    policy: TimeoutQuarantinePolicy,
}

impl ChannelFsm {
    /// Creates a new `ChannelFsm` starting in `AwaitingSnapshot`.
    pub fn new(started_at: Instant, timeout: Duration, quarantine: Duration) -> Self {
        let symbol = Symbol::from("channel");
        let mut policy = TimeoutQuarantinePolicy::new(timeout, quarantine);
        policy.on_started(&symbol, started_at);
        Self {
            symbol,
            state: SyncState::AwaitingSnapshot { started_at },
            policy,
        }
    }

    /// Returns the current synchronization state.
    pub fn state(&self) -> SyncState {
        self.state
    }

    /// Returns true if the channel is currently in sync.
    pub fn is_in_sync(&self) -> bool {
        matches!(self.state, SyncState::InSync)
    }

    /// Handles arrival of an order book snapshot.
    pub fn on_snapshot(&mut self) -> SnapshotOutcome {
        self.state = SyncState::InSync;
        self.policy.on_completed(&self.symbol, Instant::now());
        SnapshotOutcome::Accepted
    }

    /// Handles an order book update on this channel.
    pub fn on_update(&self) -> UpdateOutcome {
        match self.state {
            SyncState::InSync => UpdateOutcome::Accepted,
            SyncState::AwaitingSnapshot { .. } | SyncState::Quarantined { .. } => {
                UpdateOutcome::DroppedNotSynchronized
            }
        }
    }

    /// Triggers a resynchronization due to CRC32 checksum mismatch.
    pub fn on_checksum_mismatch(&mut self, now: Instant) {
        self.state = SyncState::AwaitingSnapshot { started_at: now };
        self.policy.on_started(&self.symbol, now);
    }

    /// Resets the FSM back to awaiting snapshot (e.g. on manual resync or reconnect).
    pub fn reset_awaiting_snapshot(&mut self, now: Instant) {
        self.state = SyncState::AwaitingSnapshot { started_at: now };
        self.policy.on_started(&self.symbol, now);
    }

    /// Evaluates timer transitions (timeout to quarantine, quarantine expiry).
    pub fn poll(&mut self, now: Instant) -> PollOutcome {
        match self.state {
            SyncState::InSync => PollOutcome::None,
            SyncState::AwaitingSnapshot { .. } => {
                match self.policy.decide(&self.symbol, now) {
                    ResyncDecision::Quarantine(until) => {
                        self.state = SyncState::Quarantined { until };
                        PollOutcome::TimeoutQuarantined { until }
                    }
                    _ => PollOutcome::None,
                }
            }
            SyncState::Quarantined { .. } => {
                match self.policy.decide(&self.symbol, now) {
                    ResyncDecision::Proceed => {
                        self.state = SyncState::AwaitingSnapshot { started_at: now };
                        self.policy.on_started(&self.symbol, now);
                        PollOutcome::QuarantineExpiredTriggerResubscribe
                    }
                    _ => PollOutcome::None,
                }
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
        let mut fsm = ChannelFsm::new(t0, Duration::from_secs(5), Duration::from_secs(30));
        assert!(!fsm.is_in_sync());
        assert_eq!(fsm.on_update(), UpdateOutcome::DroppedNotSynchronized);

        assert_eq!(fsm.on_snapshot(), SnapshotOutcome::Accepted);
        assert!(fsm.is_in_sync());
        assert_eq!(fsm.on_update(), UpdateOutcome::Accepted);
    }

    #[test]
    fn test_fsm_checksum_mismatch_and_timeout_quarantine() {
        let t0 = Instant::now();
        let timeout = Duration::from_secs(5);
        let quarantine = Duration::from_secs(30);
        let mut fsm = ChannelFsm::new(t0, timeout, quarantine);
        fsm.on_snapshot();
        assert!(fsm.is_in_sync());

        // Checksum mismatch
        let t1 = t0 + Duration::from_secs(1);
        fsm.on_checksum_mismatch(t1);
        assert!(!fsm.is_in_sync());
        assert_eq!(fsm.on_update(), UpdateOutcome::DroppedNotSynchronized);

        // Before timeout
        assert_eq!(fsm.poll(t1 + Duration::from_secs(2)), PollOutcome::None);

        // At timeout -> Quarantine
        let t_timeout = t1 + timeout;
        match fsm.poll(t_timeout) {
            PollOutcome::TimeoutQuarantined { until } => {
                assert_eq!(until, t_timeout + quarantine);
            }
            other => panic!("expected TimeoutQuarantined, got {other:?}"),
        }
        assert!(matches!(fsm.state(), SyncState::Quarantined { .. }));

        // While in quarantine -> updates still dropped
        assert_eq!(fsm.on_update(), UpdateOutcome::DroppedNotSynchronized);

        // Quarantine expires
        let t_expired = t_timeout + quarantine;
        assert_eq!(
            fsm.poll(t_expired),
            PollOutcome::QuarantineExpiredTriggerResubscribe
        );
        assert!(matches!(fsm.state(), SyncState::AwaitingSnapshot { .. }));
    }
}
