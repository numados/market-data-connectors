//! Deterministic per-symbol order book synchronization state machine for Huobi (HTX).

use std::time::Instant;

use mdc_core::model::{
    DeliveryStamp, IntegrityAlert, L2Delta, MarketEvent, SequenceRange, Symbol, SymbolTag,
    UnixNanos, VenueId,
};
use mdc_core::resync::{
    CooldownPolicy, DeltaDecision, ResyncDecision, ResyncError, ResyncFailure, ResyncPolicy,
    SnapshotOutcome, SyncState,
};

use crate::huobi::config::HuobiConfig;
use crate::huobi::normalize::normalize_snapshot;
use crate::huobi::raw::{HuobiConnectionScratch, MbpHeader};

/// Per-symbol FSM coordinating [`mdc_core::resync::ResyncController`] and [`CooldownPolicy`].
#[derive(Debug)]
pub struct SymbolFsm {
    symbol: Symbol,
    tag: SymbolTag,
    controller: mdc_core::resync::ResyncController,
    cooldown: CooldownPolicy,
}

impl SymbolFsm {
    /// Creates a new `SymbolFsm` for the given symbol.
    pub fn new(symbol: Symbol, tag: SymbolTag, config: &HuobiConfig) -> Self {
        #[allow(clippy::expect_used)] // capacity clamped to >= 1
        let controller =
            mdc_core::resync::ResyncController::new(config.resync_buffer_capacity.max(1))
                .expect("resync_buffer_capacity clamped to >= 1");
        Self {
            symbol,
            tag,
            controller,
            cooldown: CooldownPolicy::new(config.resync_timeout),
        }
    }

    /// Returns the symbol.
    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Returns the symbol tag.
    pub fn tag(&self) -> SymbolTag {
        self.tag
    }

    /// Returns current synchronization state.
    pub fn state(&self) -> SyncState {
        self.controller.state()
    }

    /// Returns true if synchronized.
    pub fn is_in_sync(&self) -> bool {
        self.state() == SyncState::InSync
    }

    /// Evaluates the sequence range of an incoming delta.
    pub fn check_delta(&mut self, range: SequenceRange) -> DeltaDecision {
        self.controller.check_delta(range)
    }

    /// Buffers an owned delta during initial synchronization or resync.
    ///
    /// # Errors
    /// Returns [`mdc_core::resync::BufferError`] if the buffer overflows or controller is in an invalid state.
    pub fn buffer_delta(
        &mut self,
        event: MarketEvent<L2Delta>,
    ) -> Result<(), mdc_core::resync::BufferError> {
        self.controller.buffer(event)
    }

    /// Applies an authoritative snapshot reply (`rep`).
    pub fn on_snapshot(
        &mut self,
        header: &MbpHeader,
        scratch: &HuobiConnectionScratch,
        ts: Option<u64>,
        delivery: DeliveryStamp,
        received_ts: UnixNanos,
        now: Instant,
    ) -> Result<SnapshotOutcome, ResyncError> {
        let (exchange_ts, snapshot) = normalize_snapshot(header, scratch, ts);
        let event = MarketEvent {
            venue: VenueId::Huobi,
            symbol: self.symbol.clone(),
            exchange_ts: Some(exchange_ts),
            received_ts,
            tag: self.tag,
            delivery,
            source_sequence: Some(header.seq_num),
            kind: snapshot,
        };
        let outcome = self.controller.on_snapshot(event)?;
        if matches!(outcome, SnapshotOutcome::Synchronized { .. }) {
            self.cooldown.on_completed(&self.symbol, now);
        }
        Ok(outcome)
    }

    /// Triggers resynchronization on sequence gap or operator request.
    pub fn trigger_resync(&mut self, now: Instant) -> IntegrityAlert {
        self.cooldown.on_started(&self.symbol, now);
        self.controller.reset();
        IntegrityAlert::ResyncStarted
    }

    /// Checks if a snapshot request may be attempted according to the cooldown policy.
    pub fn can_request_snapshot(&mut self, now: Instant) -> bool {
        matches!(
            self.cooldown.decide(&self.symbol, now),
            ResyncDecision::Proceed
        )
    }

    /// Records a snapshot request failure or stale snapshot.
    pub fn record_snapshot_failure(&mut self, now: Instant) {
        self.cooldown
            .on_failed(&self.symbol, &ResyncFailure::StaleSnapshot, now);
    }

    /// Resets the FSM on disconnect.
    pub fn reset(&mut self, config: &HuobiConfig) {
        #[allow(clippy::expect_used)] // capacity clamped to >= 1
        let controller =
            mdc_core::resync::ResyncController::new(config.resync_buffer_capacity.max(1))
                .expect("resync_buffer_capacity clamped to >= 1");
        self.controller = controller;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::huobi::raw::{HuobiConnectionScratch, MbpHeader};
    use mdc_core::{PriceLevel, RunId};
    use rust_decimal::Decimal;

    #[test]
    fn test_fsm_initial_state_buffering() {
        let config = HuobiConfig::default();
        let sym = Symbol::from("btcusdt");
        let fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);
        assert_eq!(fsm.state(), SyncState::BufferingDiffs);
        assert!(!fsm.is_in_sync());
    }

    #[test]
    fn test_fsm_snapshot_and_delta_flow() {
        let config = HuobiConfig::default();
        let sym = Symbol::from("btcusdt");
        let mut fsm = SymbolFsm::new(sym.clone(), SymbolTag::default(), &config);

        let mut scratch = HuobiConnectionScratch::with_capacity(4);
        scratch
            .bids
            .push(PriceLevel::new(Decimal::from(26000), Decimal::from(1)));
        scratch
            .asks
            .push(PriceLevel::new(Decimal::from(26001), Decimal::from(1)));

        let snap_header = MbpHeader {
            seq_num: 100,
            prev_seq_num: None,
        };

        let now = Instant::now();
        let outcome = fsm
            .on_snapshot(
                &snap_header,
                &scratch,
                Some(1700000000000),
                DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1),
                UnixNanos::from_millis(1700000000000),
                now,
            )
            .unwrap();

        assert!(matches!(outcome, SnapshotOutcome::Synchronized { .. }));
        assert_eq!(fsm.state(), SyncState::InSync);
        assert!(fsm.is_in_sync());

        // Delta check: contiguous update [101, 102]
        let decision = fsm.check_delta(SequenceRange::new(101, 102));
        assert_eq!(decision, DeltaDecision::Emit);

        // Gap check: gap to 105 (expected 103)
        let gap_decision = fsm.check_delta(SequenceRange::new(105, 106));
        assert!(matches!(gap_decision, DeltaDecision::GapDetected { .. }));
    }
}
