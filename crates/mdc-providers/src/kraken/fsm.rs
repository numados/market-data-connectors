//! Deterministic per-symbol synchronization state machine for Kraken WebSocket v2.

use std::time::{Duration, Instant};

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

/// Per-symbol synchronization controller.
#[derive(Debug, Clone)]
pub struct SymbolFsm {
    state: SyncState,
    timeout: Duration,
    quarantine: Duration,
}

impl SymbolFsm {
    /// Creates a new `SymbolFsm` starting in `AwaitingSnapshot` with the given timeouts.
    pub fn new(started_at: Instant, timeout: Duration, quarantine: Duration) -> Self {
        Self {
            state: SyncState::AwaitingSnapshot { started_at },
            timeout,
            quarantine,
        }
    }

    /// Returns the current synchronization state.
    pub fn state(&self) -> SyncState {
        self.state
    }

    /// Returns true if the symbol is in the `InSync` state.
    pub fn is_in_sync(&self) -> bool {
        matches!(self.state, SyncState::InSync)
    }

    /// Handles arrival of an order book snapshot.
    pub fn on_snapshot(&mut self, is_valid_checksum: bool) -> SnapshotOutcome {
        if is_valid_checksum {
            self.state = SyncState::InSync;
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
        match self.state {
            SyncState::InSync => {
                if is_valid_checksum {
                    UpdateOutcome::Accepted
                } else {
                    self.state = SyncState::AwaitingSnapshot { started_at: now };
                    UpdateOutcome::MismatchTriggerResync {
                        expected,
                        calculated,
                    }
                }
            }
            SyncState::AwaitingSnapshot { .. } | SyncState::Quarantined { .. } => {
                UpdateOutcome::DroppedNotSynchronized
            }
        }
    }

    /// Polls periodic timers (resync timeout and quarantine expiry).
    pub fn poll(&mut self, now: Instant) -> PollOutcome {
        match self.state {
            SyncState::AwaitingSnapshot { started_at } => {
                let deadline = started_at.checked_add(self.timeout).unwrap_or(started_at);
                if now >= deadline {
                    let until = now.checked_add(self.quarantine).unwrap_or(now);
                    self.state = SyncState::Quarantined { until };
                    PollOutcome::TimeoutQuarantined { until }
                } else {
                    PollOutcome::None
                }
            }
            SyncState::Quarantined { until } => {
                if now >= until {
                    self.state = SyncState::AwaitingSnapshot { started_at: now };
                    PollOutcome::QuarantineExpiredTriggerResubscribe
                } else {
                    PollOutcome::None
                }
            }
            SyncState::InSync => PollOutcome::None,
        }
    }

    /// Requests an explicit resync, transitioning the symbol back to `AwaitingSnapshot`.
    pub fn request_resync(&mut self, now: Instant) {
        self.state = SyncState::AwaitingSnapshot { started_at: now };
    }

    /// Explicitly resets the symbol (e.g. upon socket reconnect).
    pub fn reset(&mut self, now: Instant) {
        self.state = SyncState::AwaitingSnapshot { started_at: now };
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
        let mut fsm = SymbolFsm::new(t0, Duration::from_secs(5), Duration::from_secs(30));
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
        let mut fsm = SymbolFsm::new(t0, Duration::from_secs(5), Duration::from_secs(30));

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
        let mut fsm = SymbolFsm::new(t0, Duration::from_secs(5), Duration::from_secs(30));

        // Timeout into quarantine
        fsm.poll(t0 + Duration::from_secs(5));
        assert!(matches!(fsm.state(), SyncState::Quarantined { .. }));

        // Snapshot arrives during quarantine -> InSync immediately!
        assert_eq!(fsm.on_snapshot(true), SnapshotOutcome::Accepted);
        assert!(fsm.is_in_sync());
    }
}
