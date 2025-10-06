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
    /// Microtimestamp is strictly greater than snapshot gate: stream delta downstream.
    Emit,
    /// Microtimestamp is less than or equal to snapshot gate (or awaiting snapshot): drop delta.
    Drop,
}

/// State machine tracking the synchronization status and timestamp gate for a single symbol.
#[derive(Debug, Clone)]
pub struct SymbolFsm {
    symbol: Symbol,
    state: SyncState,
    cooldown: CooldownPolicy,
}

impl SymbolFsm {
    /// Creates a new `SymbolFsm` for `symbol` with the specified failure cooldown.
    pub fn new(symbol: Symbol, cooldown: Duration) -> Self {
        Self {
            symbol: symbol.clone(),
            state: SyncState::AwaitingSnapshot,
            cooldown: CooldownPolicy::new(cooldown),
        }
    }

    /// Current synchronization state.
    pub fn state(&self) -> SyncState {
        self.state
    }

    /// Evaluates an incoming diff's microtimestamp against the current gate.
    pub fn check_diff(&self, microtimestamp: u64) -> DiffDecision {
        match self.state {
            SyncState::AwaitingSnapshot => DiffDecision::Drop,
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
    pub fn on_snapshot(&mut self, microtimestamp: u64, now: Instant) {
        self.state = SyncState::InSync {
            gate_micros: microtimestamp,
        };
        self.cooldown.on_completed(&self.symbol, now);
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
    fn test_fsm_initial_state_drops_diffs() {
        let fsm = SymbolFsm::new(Symbol::from("btcusd"), Duration::from_secs(5));
        assert_eq!(fsm.state(), SyncState::AwaitingSnapshot);

        assert_eq!(fsm.check_diff(100), DiffDecision::Drop);
        assert_eq!(fsm.check_diff(1700000000000000), DiffDecision::Drop);
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
        // Now diffs are dropped again
        assert_eq!(fsm.check_diff(snap_time + 1), DiffDecision::Drop);
    }
}
