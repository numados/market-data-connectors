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

/// Per-product synchronization controller.
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
    pub fn on_snapshot(&mut self) -> SnapshotOutcome {
        self.state = SyncState::InSync;
        SnapshotOutcome::Accepted
    }

    /// Handles arrival of an incremental order book update.
    pub fn on_update(&self) -> UpdateOutcome {
        match self.state {
            SyncState::InSync => UpdateOutcome::Accepted,
            SyncState::AwaitingSnapshot { .. } | SyncState::Quarantined { .. } => {
                UpdateOutcome::DroppedNotSynchronized
            }
        }
    }

    /// Requests an explicit resync.
    ///
    /// Returns `true` if a transition to `AwaitingSnapshot` occurred and a new resubscribe should
    /// be triggered. Returns `false` if the symbol is already `AwaitingSnapshot` within timeout or
    /// in quarantine to prevent resubscribe storms.
    pub fn request_resync(&mut self, now: Instant) -> bool {
        match self.state {
            SyncState::InSync => {
                self.state = SyncState::AwaitingSnapshot { started_at: now };
                true
            }
            SyncState::AwaitingSnapshot { started_at } => {
                if now.saturating_duration_since(started_at) >= self.timeout {
                    self.state = SyncState::AwaitingSnapshot { started_at: now };
                    true
                } else {
                    false
                }
            }
            SyncState::Quarantined { .. } => false,
        }
    }

    /// Polls periodic timers (snapshot timeout and quarantine expiry).
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
}
