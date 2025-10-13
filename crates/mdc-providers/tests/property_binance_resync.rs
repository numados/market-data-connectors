#![cfg(feature = "binance")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Property-based testing for Binance snapshot reconciliation and sequence continuity.

use mdc_core::resync::{DeltaDecision, ResyncController, SyncState};
use mdc_core::{
    DeliveryStamp, IntegrityAlert, L2Delta, L2Snapshot, MarketEvent, PriceLevel, RunId,
    SequenceRange, Symbol, UnixNanos, VenueId,
};
use proptest::prelude::*;
use rust_decimal::Decimal;

fn dummy_delta(first: u64, last: u64) -> MarketEvent<L2Delta> {
    MarketEvent {
        exchange_ts: None,
        received_ts: UnixNanos::from_millis(100),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
        source_sequence: Some(last),
        kind: L2Delta {
            sequence: Some(SequenceRange::new(first, last)),
            bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
            asks: vec![PriceLevel::new(Decimal::from(101), Decimal::from(1))],
        },
    }
}

fn dummy_snapshot(last_update_id: u64) -> MarketEvent<L2Snapshot> {
    MarketEvent {
        exchange_ts: None,
        received_ts: UnixNanos::from_millis(100),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
        source_sequence: Some(last_update_id),
        kind: L2Snapshot {
            last_update_id: Some(last_update_id),
            bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
            asks: vec![PriceLevel::new(Decimal::from(101), Decimal::from(1))],
        },
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_contiguous_deltas_reconcile_successfully(
        snap_id in 100u64..10_000,
        deltas_before in 0usize..10,
        deltas_after in 1usize..20,
    ) {
        let mut controller = ResyncController::new(100).expect("valid controller");
        prop_assert_eq!(controller.state(), SyncState::BufferingDiffs);

        // Buffer some old deltas that arrive before or around snapshot
        let mut current_id = snap_id.saturating_sub(deltas_before as u64 * 5);
        for _ in 0..deltas_before {
            let next_id = current_id + 5;
            let range = SequenceRange::new(current_id + 1, next_id);
            if range.last <= snap_id {
                let decision = controller.check_delta(range);
                prop_assert_eq!(decision, DeltaDecision::Buffer);
                let _ = controller.buffer(dummy_delta(range.first, range.last));
            }
            current_id = next_id;
        }

        // Buffer the bridging delta covering snap_id + 1
        let bridge_first = snap_id.saturating_sub(2);
        let bridge_last = snap_id + 3;
        let bridge_range = SequenceRange::new(bridge_first, bridge_last);
        let decision = controller.check_delta(bridge_range);
        prop_assert_eq!(decision, DeltaDecision::Buffer);
        let _ = controller.buffer(dummy_delta(bridge_range.first, bridge_range.last));

        // Buffer subsequent contiguous deltas
        let mut last_id = bridge_last;
        for _ in 0..deltas_after {
            let next_id = last_id + 5;
            let range = SequenceRange::new(last_id + 1, next_id);
            let decision = controller.check_delta(range);
            prop_assert_eq!(decision, DeltaDecision::Buffer);
            let _ = controller.buffer(dummy_delta(range.first, range.last));
            last_id = next_id;
        }

        // Now deliver snapshot
        let outcome = controller.on_snapshot(dummy_snapshot(snap_id)).expect("reconcile snapshot");
        match outcome {
            mdc_core::SnapshotOutcome::Synchronized { replay, .. } => {
                prop_assert_eq!(controller.state(), SyncState::InSync);
                prop_assert_eq!(controller.buffered_count(), 0);
                prop_assert_eq!(replay.len(), 1 + deltas_after);
                prop_assert_eq!(controller.tracker().current(), Some(last_id));
            }
            other => prop_assert!(false, "Expected Synchronized, got {:?}", other),
        }

        // Subsequent contiguous delta is directly emitted
        let next_range = SequenceRange::new(last_id + 1, last_id + 10);
        let decision = controller.check_delta(next_range);
        prop_assert_eq!(decision, DeltaDecision::Emit);
        prop_assert_eq!(controller.tracker().current(), Some(last_id + 10));
    }

    #[test]
    fn property_gap_in_stream_detected(
        start_id in 100u64..10_000,
        gap_size in 2u64..100,
    ) {
        let mut controller = ResyncController::new(100).expect("valid controller");
        let bridge_range = SequenceRange::new(start_id, start_id + 5);
        let _ = controller.buffer(dummy_delta(bridge_range.first, bridge_range.last));

        let _ = controller.on_snapshot(dummy_snapshot(start_id + 2)).expect("snapshot");
        prop_assert_eq!(controller.state(), SyncState::InSync);

        // Next expected is start_id + 6
        let gapped_first = start_id + 5 + gap_size;
        let gapped_last = gapped_first + 5;
        let decision = controller.check_delta(SequenceRange::new(gapped_first, gapped_last));

        match decision {
            DeltaDecision::GapDetected { alert: IntegrityAlert::GapDetected { expected, received } } => {
                prop_assert_eq!(expected, start_id + 6);
                prop_assert_eq!(received, gapped_first);
            }
            other => prop_assert!(false, "Expected GapDetected, got {:?}", other),
        }
    }

    #[test]
    fn property_stale_snapshot_rejected(
        earliest_buffered_first in 100u64..1000,
        stale_offset in 5u64..50,
    ) {
        let mut controller = ResyncController::new(100).expect("valid controller");
        // Buffer delta starting at earliest_buffered_first
        let _ = controller.buffer(dummy_delta(earliest_buffered_first, earliest_buffered_first + 10));

        // Snapshot with lastUpdateId + 1 < earliest_buffered_first
        let stale_id = earliest_buffered_first.saturating_sub(stale_offset);
        let outcome = controller.on_snapshot(dummy_snapshot(stale_id)).expect("stale snapshot check");

        match outcome {
            mdc_core::SnapshotOutcome::StaleSnapshot { snapshot_id, first_buffered } => {
                prop_assert_eq!(snapshot_id, stale_id);
                prop_assert_eq!(first_buffered, earliest_buffered_first);
                prop_assert_eq!(controller.state(), SyncState::BufferingDiffs);
                prop_assert_eq!(controller.buffered_count(), 1);
            }
            other => prop_assert!(false, "Expected StaleSnapshot, got {:?}", other),
        }
    }
}
