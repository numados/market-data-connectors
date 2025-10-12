#![cfg(feature = "huobi")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 3 Property tests for Huobi order book synchronization and sequence continuity.

use std::time::Instant;

use mdc_core::PriceLevel;
use mdc_core::model::{
    DeliveryStamp, L2Delta, MarketEvent, RunId, SequenceRange, Symbol, SymbolTag, UnixNanos,
    VenueId,
};
use mdc_core::resync::{DeltaDecision, SnapshotOutcome, SyncState};
use mdc_providers::huobi::HuobiConfig;
use mdc_providers::huobi::fsm::SymbolFsm;
use mdc_providers::huobi::raw::{HuobiConnectionScratch, MbpHeader};
use proptest::prelude::*;
use rust_decimal::Decimal;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_contiguous_deltas_after_snapshot(
        snap_id in 100u64..10_000,
        num_deltas in 1usize..30,
    ) {
        let config = HuobiConfig::default();
        let sym = Symbol::from("btcusdt");
        let mut fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);
        prop_assert_eq!(fsm.state(), SyncState::BufferingDiffs);

        let mut scratch = HuobiConnectionScratch::with_capacity(16);
        scratch.bids.push(PriceLevel::new(Decimal::from(26000), Decimal::from(1)));
        scratch.asks.push(PriceLevel::new(Decimal::from(26001), Decimal::from(1)));

        let snap_header = MbpHeader {
            seq_num: snap_id,
            prev_seq_num: None,
        };

        let now = Instant::now();
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1);
        let outcome = fsm
            .on_snapshot(&snap_header, &scratch, Some(1700000000000), delivery, mdc_providers::now_nanos(), now)
            .unwrap();
        let is_sync = matches!(outcome, SnapshotOutcome::Synchronized { .. });
        prop_assert!(is_sync);
        prop_assert_eq!(fsm.state(), SyncState::InSync);

        // Feed contiguous deltas: prevSeqNum == current, seqNum == current + step
        let mut curr_seq = snap_id;
        for _ in 0..num_deltas {
            let next_seq = curr_seq + 3;
            // In Huobi: SequenceRange is [prevSeqNum + 1, seqNum]
            let range = SequenceRange::new(curr_seq + 1, next_seq);
            let decision = fsm.check_delta(range);
            prop_assert_eq!(decision, DeltaDecision::Emit);
            curr_seq = next_seq;
        }
    }

    #[test]
    fn property_gap_detected_on_skipped_sequences(
        snap_id in 100u64..10_000,
        gap in 1u64..100,
    ) {
        let config = HuobiConfig::default();
        let sym = Symbol::from("btcusdt");
        let mut fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);

        let scratch = HuobiConnectionScratch::with_capacity(4);
        let snap_header = MbpHeader {
            seq_num: snap_id,
            prev_seq_num: None,
        };

        let now = Instant::now();
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1);
        let _ = fsm.on_snapshot(&snap_header, &scratch, Some(1700000000000), delivery, mdc_providers::now_nanos(), now).unwrap();
        prop_assert_eq!(fsm.state(), SyncState::InSync);

        // Introduce gap: prevSeqNum = snap_id + gap
        let skipped_prev = snap_id + gap;
        let next_seq = skipped_prev + 2;
        let range = SequenceRange::new(skipped_prev + 1, next_seq);
        let decision = fsm.check_delta(range);
        let is_gap = matches!(decision, DeltaDecision::GapDetected { .. });
        prop_assert!(is_gap);
    }

    #[test]
    fn property_old_and_duplicate_deltas_dropped(
        snap_id in 100u64..10_000,
        sub in 1u64..50,
    ) {
        let config = HuobiConfig::default();
        let sym = Symbol::from("btcusdt");
        let mut fsm = SymbolFsm::new(sym, SymbolTag::default(), &config);

        let scratch = HuobiConnectionScratch::with_capacity(4);
        let snap_header = MbpHeader {
            seq_num: snap_id,
            prev_seq_num: None,
        };

        let now = Instant::now();
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1);
        let _ = fsm.on_snapshot(&snap_header, &scratch, Some(1700000000000), delivery, mdc_providers::now_nanos(), now).unwrap();

        // Delta strictly older than or equal to current snapshot
        let old_seq = snap_id.saturating_sub(sub);
        let range = SequenceRange::new(old_seq.saturating_sub(2), old_seq);
        let decision = fsm.check_delta(range);
        prop_assert_eq!(decision, DeltaDecision::Drop);
    }

    #[test]
    fn property_buffering_and_snapshot_reconciliation(
        snap_id in 100u64..10_000,
        num_buffered in 1usize..10,
    ) {
        let config = HuobiConfig::default();
        let sym = Symbol::from("btcusdt");
        let mut fsm = SymbolFsm::new(sym.clone(), SymbolTag::default(), &config);
        prop_assert_eq!(fsm.state(), SyncState::BufferingDiffs);

        // Buffer deltas that will align with future snapshot
        let mut curr = snap_id;
        for i in 0..num_buffered {
            let next = curr + 1;
            let range = SequenceRange::new(curr + 1, next);
            let event = MarketEvent {
                venue: VenueId::Huobi,
                symbol: sym.clone(),
                exchange_ts: Some(UnixNanos::from_millis(1700000000000 + i as u64)),
                received_ts: mdc_providers::now_nanos(),
                tag: SymbolTag::default(),
                delivery: DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, i as u64 + 1),
                source_sequence: Some(next),
                kind: L2Delta {
                    sequence: Some(range),
                    bids: vec![],
                    asks: vec![],
                },
            };
            fsm.buffer_delta(event);
            curr = next;
        }

        let scratch = HuobiConnectionScratch::with_capacity(4);
        let snap_header = MbpHeader {
            seq_num: snap_id,
            prev_seq_num: None,
        };

        let now = Instant::now();
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 100);
        let outcome = fsm
            .on_snapshot(&snap_header, &scratch, Some(1700000000000), delivery, mdc_providers::now_nanos(), now)
            .unwrap();

        match outcome {
            SnapshotOutcome::Synchronized { replay, .. } => {
                prop_assert_eq!(replay.len(), num_buffered);
                prop_assert_eq!(fsm.state(), SyncState::InSync);
            }
            _ => prop_assert!(false, "Expected Synchronized outcome"),
        }
    }
}
