//! Deterministic per-symbol order book synchronization state machine for Gate.io.

use std::time::Instant;

use mdc_core::model::{
    DeliveryStamp, IntegrityAlert, L2Delta, MarketEvent, SequenceRange, Symbol, SymbolTag,
    UnixNanos, VenueId,
};
use mdc_core::resync::{
    CooldownPolicy, DeltaDecision, ResyncDecision, ResyncError, ResyncFailure, ResyncPolicy,
    SnapshotOutcome, SyncState,
};

use crate::gateio::config::GateioConfig;
use crate::gateio::normalize::normalize_snapshot;
use crate::gateio::raw::{GateioConnectionScratch, ObuHeader};

/// Per-symbol FSM coordinating `ResyncController` and `CooldownPolicy`.
#[derive(Debug)]
pub struct SymbolFsm {
    symbol: Symbol,
    tag: SymbolTag,
    controller: mdc_core::resync::ResyncController,
    cooldown: CooldownPolicy,
}

impl SymbolFsm {
    /// Creates a new `SymbolFsm` for the given symbol.
    pub fn new(symbol: Symbol, tag: SymbolTag, config: &GateioConfig) -> Self {
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

    /// Returns the tag.
    pub fn tag(&self) -> SymbolTag {
        self.tag
    }

    /// Returns the current synchronization state (`BufferingDiffs` or `InSync`).
    pub fn state(&self) -> SyncState {
        self.controller.state()
    }

    /// Returns true if synchronized.
    pub fn is_in_sync(&self) -> bool {
        self.state() == SyncState::InSync
    }

    /// Evaluates sequence range of incoming delta on the hot path.
    pub fn check_delta(&mut self, range: SequenceRange) -> DeltaDecision {
        self.controller.check_delta(range)
    }

    /// Buffers an owned delta during initial synchronization or resync.
    pub fn buffer_delta(&mut self, event: MarketEvent<L2Delta>) {
        let _ = self.controller.buffer(event);
    }

    /// Applies an authoritative snapshot (`full: true`).
    pub fn on_snapshot(
        &mut self,
        header: &ObuHeader<'_>,
        scratch: &GateioConnectionScratch,
        delivery: DeliveryStamp,
        received_ts: UnixNanos,
        now: Instant,
    ) -> Result<SnapshotOutcome, ResyncError> {
        let (exchange_ts, snapshot) = normalize_snapshot(header, scratch);
        let event = MarketEvent {
            venue: VenueId::Gateio,
            symbol: self.symbol.clone(),
            exchange_ts: Some(exchange_ts),
            received_ts,
            tag: self.tag,
            delivery,
            source_sequence: Some(header.last_id),
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

    /// Checks if resubscription may be attempted according to the cooldown policy.
    pub fn can_resubscribe(&mut self, now: Instant) -> bool {
        matches!(
            self.cooldown.decide(&self.symbol, now),
            ResyncDecision::Proceed
        )
    }

    /// Records a resubscription attempt failure or retry trigger.
    pub fn record_resubscribe_failure(&mut self, now: Instant) {
        self.cooldown
            .on_failed(&self.symbol, &ResyncFailure::StaleSnapshot, now);
    }

    /// Resets the FSM on disconnect.
    pub fn reset(&mut self, config: &GateioConfig) {
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
    use crate::gateio::raw::{GateioConnectionScratch, ObuHeader};
    use mdc_core::{PriceLevel, RunId};
    use rust_decimal::Decimal;
    use std::time::Duration;

    #[test]
    fn test_fsm_initial_state_buffering() {
        let config = GateioConfig::default();
        let sym = Symbol::from("BTC_USDT");
        let fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);
        assert_eq!(fsm.state(), SyncState::BufferingDiffs);
        assert!(!fsm.is_in_sync());
    }

    #[test]
    fn test_fsm_snapshot_and_delta_flow() {
        let config = GateioConfig::default();
        let sym = Symbol::from("BTC_USDT");
        let mut fsm = SymbolFsm::new(sym.clone(), SymbolTag::default(), &config);

        let mut scratch = GateioConnectionScratch::with_capacity(4);
        scratch
            .bids
            .push(PriceLevel::new(Decimal::from(50000), Decimal::from(1)));
        scratch
            .asks
            .push(PriceLevel::new(Decimal::from(50001), Decimal::from(1)));

        let snap_header = ObuHeader {
            t: 1700000000000,
            full: true,
            s: "ob.BTC_USDT.400",
            first_id: 0,
            last_id: 100,
        };

        let now = Instant::now();
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1);
        let outcome = fsm
            .on_snapshot(&snap_header, &scratch, delivery, crate::now_nanos(), now)
            .unwrap();
        assert!(matches!(outcome, SnapshotOutcome::Synchronized { .. }));
        assert_eq!(fsm.state(), SyncState::InSync);
        assert!(fsm.is_in_sync());

        // Contiguous delta
        let decision = fsm.check_delta(SequenceRange::new(101, 102));
        assert_eq!(decision, DeltaDecision::Emit);

        // Gap delta
        let decision_gap = fsm.check_delta(SequenceRange::new(105, 106));
        assert!(matches!(decision_gap, DeltaDecision::GapDetected { .. }));

        // Resync trigger
        let alert = fsm.trigger_resync(now);
        assert_eq!(alert, IntegrityAlert::ResyncStarted);
        assert_eq!(fsm.state(), SyncState::BufferingDiffs);
    }

    #[test]
    fn test_fsm_cooldown_throttling() {
        let config = GateioConfig {
            resync_timeout: Duration::from_secs(5),
            ..GateioConfig::default()
        };
        let sym = Symbol::from("BTC_USDT");
        let mut fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);

        let now = Instant::now();
        assert!(fsm.can_resubscribe(now));

        fsm.record_resubscribe_failure(now);
        // Immediately after failure, cooldown should prevent resubscribe
        assert!(!fsm.can_resubscribe(now));

        // After cooldown elapses, resubscribe is permitted
        let later = now + Duration::from_secs(6);
        assert!(fsm.can_resubscribe(later));
    }
}
