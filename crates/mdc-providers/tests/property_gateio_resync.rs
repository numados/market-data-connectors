#![cfg(feature = "gateio")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 3 Property tests for Gate.io order book synchronization and sequence continuity.

use std::time::{Duration, Instant};

use mdc_core::PriceLevel;
use mdc_core::model::{DeliveryStamp, RunId, SequenceRange, Symbol, SymbolTag};
use mdc_core::resync::{DeltaDecision, SnapshotOutcome, SyncState};
use mdc_providers::gateio::GateioConfig;
use mdc_providers::gateio::fsm::SymbolFsm;
use mdc_providers::gateio::raw::{GateioConnectionScratch, ObuHeader};
use proptest::prelude::*;
use rust_decimal::Decimal;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_contiguous_deltas_after_socket_snapshot(
        snap_id in 100u64..10_000,
        num_deltas in 1usize..30,
    ) {
        let config = GateioConfig::default();
        let sym = Symbol::from("BTC_USDT");
        let mut fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);
        prop_assert_eq!(fsm.state(), SyncState::BufferingDiffs);

        let mut scratch = GateioConnectionScratch::with_capacity(16);
        scratch.bids.push(PriceLevel::new(Decimal::from(50000), Decimal::from(1)));
        scratch.asks.push(PriceLevel::new(Decimal::from(50001), Decimal::from(1)));

        let snap_header = ObuHeader {
            t: 1700000000000,
            full: true,
            s: "ob.BTC_USDT.400",
            first_id: 0,
            last_id: snap_id,
        };

        let now = Instant::now();
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1);
        let outcome = fsm.on_snapshot(&snap_header, &scratch, delivery, mdc_providers::now_nanos(), now).unwrap();
        let is_sync = matches!(outcome, SnapshotOutcome::Synchronized { .. });
        prop_assert!(is_sync);
        prop_assert_eq!(fsm.state(), SyncState::InSync);

        // Feed contiguous deltas
        let mut curr_seq = snap_id;
        for _ in 0..num_deltas {
            let next_seq = curr_seq + 3;
            let range = SequenceRange::new(curr_seq + 1, next_seq);
            let decision = fsm.check_delta(range);
            prop_assert_eq!(decision, DeltaDecision::Emit);
            curr_seq = next_seq;
        }
    }

    #[test]
    fn property_gap_triggers_resync(
        snap_id in 100u64..10_000,
        gap_size in 2u64..50,
    ) {
        let config = GateioConfig::default();
        let sym = Symbol::from("BTC_USDT");
        let mut fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);

        let mut scratch = GateioConnectionScratch::with_capacity(16);
        scratch.bids.push(PriceLevel::new(Decimal::from(50000), Decimal::from(1)));
        scratch.asks.push(PriceLevel::new(Decimal::from(50001), Decimal::from(1)));

        let snap_header = ObuHeader {
            t: 1700000000000,
            full: true,
            s: "ob.BTC_USDT.400",
            first_id: 0,
            last_id: snap_id,
        };

        let now = Instant::now();
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1);
        let outcome = fsm.on_snapshot(&snap_header, &scratch, delivery, mdc_providers::now_nanos(), now).unwrap();
        let is_sync = matches!(outcome, SnapshotOutcome::Synchronized { .. });
        prop_assert!(is_sync);

        // Inject gap: expected first is snap_id + 1, but received is snap_id + gap_size
        let gapped_range = SequenceRange::new(snap_id + gap_size, snap_id + gap_size + 2);
        let decision = fsm.check_delta(gapped_range);
        let is_gap = matches!(decision, DeltaDecision::GapDetected { .. });
        prop_assert!(is_gap);

        // State machine must transition to BufferingDiffs
        prop_assert_eq!(fsm.state(), SyncState::BufferingDiffs);

        // Subsequent update is buffered
        let next_range = SequenceRange::new(snap_id + gap_size + 3, snap_id + gap_size + 4);
        let decision_buffering = fsm.check_delta(next_range);
        prop_assert_eq!(decision_buffering, DeltaDecision::Buffer);

        // Fresh snapshot anchors to InSync again
        let fresh_snap_header = ObuHeader {
            t: 1700000001000,
            full: true,
            s: "ob.BTC_USDT.400",
            first_id: 0,
            last_id: snap_id + gap_size + 4,
        };
        let resync_outcome = fsm.on_snapshot(&fresh_snap_header, &scratch, delivery, mdc_providers::now_nanos(), now).unwrap();
        let is_resync_ok = matches!(resync_outcome, SnapshotOutcome::Synchronized { .. });
        prop_assert!(is_resync_ok);
        prop_assert_eq!(fsm.state(), SyncState::InSync);
    }

    #[test]
    fn property_one_sided_updates_never_manufacture_gap(
        snap_id in 100u64..5_000,
        steps in 1usize..20,
    ) {
        let config = GateioConfig::default();
        let sym = Symbol::from("BTC_USDT");
        let mut fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);

        let mut scratch = GateioConnectionScratch::with_capacity(16);
        scratch.bids.push(PriceLevel::new(Decimal::from(50000), Decimal::from(1)));
        scratch.asks.push(PriceLevel::new(Decimal::from(50001), Decimal::from(1)));

        let snap_header = ObuHeader {
            t: 1700000000000,
            full: true,
            s: "ob.BTC_USDT.400",
            first_id: 0,
            last_id: snap_id,
        };

        let now = Instant::now();
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1);
        fsm.on_snapshot(&snap_header, &scratch, delivery, mdc_providers::now_nanos(), now).unwrap();

        let mut curr_seq = snap_id;
        for i in 0..steps {
            // Contiguous sequence ranges regardless of whether update is bid-only or ask-only
            let next_seq = curr_seq + 1;
            let range = SequenceRange::new(curr_seq + 1, next_seq);
            let decision = fsm.check_delta(range);
            prop_assert_eq!(decision, DeltaDecision::Emit);
            curr_seq = next_seq;

            // One-sided scratch configuration
            if i % 2 == 0 {
                scratch.bids.clear();
                scratch.asks.clear();
                scratch.asks.push(PriceLevel::new(Decimal::from(50002), Decimal::from(1)));
                prop_assert!(scratch.bids.is_empty());
                prop_assert_eq!(scratch.asks.len(), 1);
            } else {
                scratch.bids.clear();
                scratch.asks.clear();
                scratch.bids.push(PriceLevel::new(Decimal::from(49999), Decimal::from(1)));
                prop_assert!(scratch.asks.is_empty());
                prop_assert_eq!(scratch.bids.len(), 1);
            }
        }
    }

    #[test]
    fn property_cooldown_throttling_durations(
        timeout_secs in 1u64..30,
    ) {
        let config = GateioConfig {
            resync_timeout: Duration::from_secs(timeout_secs),
            ..GateioConfig::default()
        };
        let sym = Symbol::from("BTC_USDT");
        let mut fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);

        let start = Instant::now();
        prop_assert!(fsm.can_resubscribe(start));

        fsm.record_resubscribe_failure(start);
        prop_assert!(!fsm.can_resubscribe(start));

        let before_timeout = start + Duration::from_millis(timeout_secs * 1000 - 1);
        prop_assert!(!fsm.can_resubscribe(before_timeout));

        let after_timeout = start + Duration::from_secs(timeout_secs + 1);
        prop_assert!(fsm.can_resubscribe(after_timeout));
    }
}
