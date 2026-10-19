//! Per-symbol timestamp-gating state machine for Bitstamp order book reconciliation.

use std::time::{Duration, Instant};

use mdc_core::model::Symbol;
use mdc_core::resync::{CooldownPolicy, ResyncDecision, ResyncFailure, ResyncPolicy};

/// Synchronization state of a symbol on a Bitstamp connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Awaiting authoritative REST snapshot; incoming deltas are dropped.
    AwaitingSnapshot,
    /// In sync: gate timestamp is `gate_micros`. Only deltas with `microtimestamp > gate_micros` are emitted.
    InSync {
        /// Microsecond timestamp of the authoritative snapshot.
        gate_micros: u64,
    },
}

/// Action to take on an incoming order book diff based on its microtimestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffDecision {
    /// Delta arrived while awaiting snapshot: buffer it for post-snapshot replay.
    Buffer,
    /// Microtimestamp is strictly greater than snapshot gate: stream delta downstream.
    Emit,
    /// Microtimestamp is less than or equal to snapshot gate: drop delta.
    Drop,
}

/// State machine tracking the synchronization status and timestamp gate for a single symbol.
pub struct SymbolFsm {
    symbol: Symbol,
    state: SyncState,
    cooldown: Box<dyn ResyncPolicy>,
}

impl SymbolFsm {
    /// Creates a new `SymbolFsm` for `symbol` with a custom resync policy.
    pub fn with_policy(symbol: Symbol, policy: Box<dyn ResyncPolicy>) -> Self {
        Self {
            symbol,
            state: SyncState::AwaitingSnapshot,
            cooldown: policy,
        }
    }

    /// Creates a new `SymbolFsm` for `symbol` with the specified failure cooldown.
    pub fn new(symbol: Symbol, cooldown: Duration) -> Self {
        Self::with_policy(symbol, Box::new(CooldownPolicy::new(cooldown)))
    }

    /// Current synchronization state.
    pub fn state(&self) -> SyncState {
        self.state
    }

    /// Evaluates an incoming diff's microtimestamp against the current gate.
    pub fn check_diff(&self, microtimestamp: u64) -> DiffDecision {
        match self.state {
            SyncState::AwaitingSnapshot => DiffDecision::Buffer,
            SyncState::InSync { gate_micros } => {
                if microtimestamp > gate_micros {
                    DiffDecision::Emit
                } else {
                    DiffDecision::Drop
                }
            }
        }
    }

    /// Records receipt of an authoritative REST snapshot at `microtimestamp`.
    /// Never rolls back an existing timestamp gate.
    pub fn on_snapshot(&mut self, microtimestamp: u64, now: Instant) {
        match self.state {
            SyncState::AwaitingSnapshot => {
                self.state = SyncState::InSync {
                    gate_micros: microtimestamp,
                };
                self.cooldown.on_completed(&self.symbol, now);
            }
            SyncState::InSync { gate_micros } => {
                if microtimestamp > gate_micros {
                    self.state = SyncState::InSync {
                        gate_micros: microtimestamp,
                    };
                    self.cooldown.on_completed(&self.symbol, now);
                }
            }
        }
    }

    /// Records failure of a REST snapshot retrieval attempt.
    pub fn on_snapshot_failed(&mut self, failure: &ResyncFailure, now: Instant) {
        self.state = SyncState::AwaitingSnapshot;
        self.cooldown.on_failed(&self.symbol, failure, now);
    }

    /// Triggers a resynchronization, resetting the state to `AwaitingSnapshot`.
    pub fn trigger_resync(&mut self, _now: Instant) {
        self.state = SyncState::AwaitingSnapshot;
    }

    /// Checks if a snapshot request is permitted under the failure cooldown policy.
    pub fn can_request_snapshot(&mut self, now: Instant) -> bool {
        matches!(
            self.cooldown.decide(&self.symbol, now),
            ResyncDecision::Proceed
        )
    }

    /// Marks that a snapshot request has been initiated.
    pub fn mark_snapshot_requested(&mut self, now: Instant) {
        self.cooldown.on_started(&self.symbol, now);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_fsm_initial_state_buffers_diffs() {
        let fsm = SymbolFsm::new(Symbol::from("btcusd"), Duration::from_secs(5));
        assert_eq!(fsm.state(), SyncState::AwaitingSnapshot);

        assert_eq!(fsm.check_diff(100), DiffDecision::Buffer);
        assert_eq!(fsm.check_diff(1700000000000000), DiffDecision::Buffer);
    }

    #[test]
    fn test_fsm_snapshot_gates_diffs() {
        let mut fsm = SymbolFsm::new(Symbol::from("btcusd"), Duration::from_secs(5));
        let now = Instant::now();
        let snap_time = 1700000000100000;

        fsm.on_snapshot(snap_time, now);
        assert_eq!(
            fsm.state(),
            SyncState::InSync {
                gate_micros: snap_time
            }
        );

        // Older than snapshot -> Drop
        assert_eq!(fsm.check_diff(snap_time - 1), DiffDecision::Drop);
        // Exactly equal to snapshot -> Drop (strict >)
        assert_eq!(fsm.check_diff(snap_time), DiffDecision::Drop);
        // Newer than snapshot -> Emit
        assert_eq!(fsm.check_diff(snap_time + 1), DiffDecision::Emit);
        assert_eq!(fsm.check_diff(snap_time + 1000), DiffDecision::Emit);
    }

    #[test]
    fn test_fsm_cooldown_throttling() {
        let mut fsm = SymbolFsm::new(Symbol::from("btcusd"), Duration::from_secs(5));
        let start = Instant::now();

        // Initial request permitted
        assert!(fsm.can_request_snapshot(start));
        fsm.mark_snapshot_requested(start);

        // Snapshot fails
        fsm.on_snapshot_failed(&ResyncFailure::HttpError(500), start);

        // 1 second later: in cooldown
        assert!(!fsm.can_request_snapshot(start + Duration::from_secs(1)));
        // 4 seconds later: still in cooldown
        assert!(!fsm.can_request_snapshot(start + Duration::from_secs(4)));
        // 5 seconds later: cooldown expired, allowed to request again
        assert!(fsm.can_request_snapshot(start + Duration::from_secs(5)));
    }

    #[test]
    fn test_fsm_trigger_resync_resets_gate() {
        let mut fsm = SymbolFsm::new(Symbol::from("btcusd"), Duration::from_secs(5));
        let now = Instant::now();
        let snap_time = 1700000000100000;

        fsm.on_snapshot(snap_time, now);
        assert_eq!(fsm.check_diff(snap_time + 1), DiffDecision::Emit);

        fsm.trigger_resync(now);
        assert_eq!(fsm.state(), SyncState::AwaitingSnapshot);
        // Now diffs are buffered again
        assert_eq!(fsm.check_diff(snap_time + 1), DiffDecision::Buffer);
    }

    #[test]
    fn test_fsm_snapshot_no_rollback_on_older_timestamp() {
        let mut fsm = SymbolFsm::new(Symbol::from("btcusd"), Duration::from_secs(5));
        let now = Instant::now();
        let newer_snap = 1700000000200000;
        let older_snap = 1700000000100000;

        fsm.on_snapshot(newer_snap, now);
        assert_eq!(
            fsm.state(),
            SyncState::InSync {
                gate_micros: newer_snap
            }
        );

        // Delayed/stale REST snapshot with older timestamp arrives -> must NOT roll back gate!
        fsm.on_snapshot(older_snap, now);
        assert_eq!(
            fsm.state(),
            SyncState::InSync {
                gate_micros: newer_snap
            }
        );
        // Microtimestamp between older and newer is still dropped (not emitted)
        assert_eq!(fsm.check_diff(older_snap + 50), DiffDecision::Drop);
        // Newer than the active gate emits
        assert_eq!(fsm.check_diff(newer_snap + 1), DiffDecision::Emit);
    }

    #[test]
    fn test_fsm_with_custom_policy_injection() {
        struct ZeroCooldownPolicy;
        impl ResyncPolicy for ZeroCooldownPolicy {
            fn on_started(&mut self, _symbol: &Symbol, _now: Instant) {}
            fn on_completed(&mut self, _symbol: &Symbol, _now: Instant) {}
            fn on_failed(&mut self, _symbol: &Symbol, _error: &ResyncFailure, _now: Instant) {}
            fn decide(&mut self, _symbol: &Symbol, _now: Instant) -> ResyncDecision {
                ResyncDecision::Proceed
            }
        }

        let mut fsm = SymbolFsm::with_policy(Symbol::from("btcusd"), Box::new(ZeroCooldownPolicy));
        let now = Instant::now();
        fsm.on_snapshot_failed(&ResyncFailure::HttpError(500), now);
        // Custom policy allows snapshot immediately despite failure
        assert!(fsm.can_request_snapshot(now));
    }
}
