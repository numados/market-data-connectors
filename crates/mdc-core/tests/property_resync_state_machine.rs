//! L3 State-machine testing for ResyncController and reconciliation invariants.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use mdc_core::model::{
    DeliveryStamp, L2Delta, L2Snapshot, MarketEvent, PriceLevel, RunId, SequenceRange, Symbol,
    UnixNanos, VenueId,
};
use mdc_core::resync::{DeltaDecision, ResyncController, SnapshotOutcome, SyncState};
use proptest::prelude::*;
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
enum ResyncOp {
    LiveDelta { first: u64, last: u64 },
    ApplySnapshot { last_update_id: u64 },
    Reset,
}

fn make_delta(first: u64, last: u64) -> MarketEvent<L2Delta> {
    MarketEvent {
        exchange_ts: Some(UnixNanos(1_000)),
        received_ts: UnixNanos(1_000),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
        source_sequence: None,
        kind: L2Delta::new(
            vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
            vec![PriceLevel::new(Decimal::from(101), Decimal::from(1))],
            Some(SequenceRange::new(first, last)),
        ),
    }
}

fn make_snapshot(last_update_id: u64) -> MarketEvent<L2Snapshot> {
    MarketEvent {
        exchange_ts: Some(UnixNanos(1_000)),
        received_ts: UnixNanos(1_000),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
        source_sequence: None,
        kind: L2Snapshot::new(
            vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
            vec![PriceLevel::new(Decimal::from(101), Decimal::from(10))],
            Some(last_update_id),
        ),
    }
}

use mdc_core::resync::BufferError;

fn arb_clustered_scenario() -> impl Strategy<Value = (usize, Vec<ResyncOp>)> {
    (1usize..50, 100u64..10_000).prop_flat_map(|(max_buffer, base)| {
        let op_strat = prop_oneof![
            // Contiguous or slight delta
            (-5i64..20i64, 0u64..3).prop_map(move |(offset, span)| {
                let first = (base as i64 + offset).max(1) as u64;
                ResyncOp::LiveDelta {
                    first,
                    last: first + span,
                }
            }),
            // Gapped delta (to reach gap-during-replay or gap detection)
            (-5i64..20i64, 0u64..3).prop_map(move |(offset, span)| {
                let first = (base as i64 + offset + 7).max(1) as u64;
                ResyncOp::LiveDelta {
                    first,
                    last: first + span,
                }
            }),
            // Snapshot clustered around the base
            (-10i64..20i64).prop_map(move |offset| {
                let last_update_id = (base as i64 + offset).max(1) as u64;
                ResyncOp::ApplySnapshot { last_update_id }
            }),
            Just(ResyncOp::Reset),
        ];
        (Just(max_buffer), proptest::collection::vec(op_strat, 1..60))
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn property_resync_controller_invariants(
        (max_buffer, ops) in arb_clustered_scenario(),
    ) {
        let mut controller = ResyncController::new(max_buffer).expect("valid max_buffer");
        let mut prev_emitted_last: Option<u64> = None;

        for op in ops {
            match op {
                ResyncOp::LiveDelta { first, last } => {
                    let range = SequenceRange::new(first, last);
                    let decision = controller.check_delta(range);

                    match decision {
                        DeltaDecision::Buffer => {
                            prop_assert_eq!(controller.state(), SyncState::BufferingDiffs);
                            let delta = make_delta(first, last);
                            let res = controller.buffer(delta);
                            if let Err(BufferError::Overflow(overflow)) = res {
                                prop_assert_eq!(overflow.dropped, max_buffer);
                                prop_assert_eq!(controller.buffered_count(), 0);
                            }
                        }
                        DeltaDecision::Emit => {
                            prop_assert_eq!(controller.state(), SyncState::InSync);
                            if let Some(prev) = prev_emitted_last {
                                // Monotonic progression
                                prop_assert!(first <= prev + 1);
                                prop_assert!(last > prev);
                            }
                            prev_emitted_last = Some(last);
                        }
                        DeltaDecision::Drop => {
                            prop_assert_eq!(controller.state(), SyncState::InSync);
                        }
                        DeltaDecision::GapDetected { .. } => {
                            // After gap, state must transition to BufferingDiffs
                            prop_assert_eq!(controller.state(), SyncState::BufferingDiffs);
                            prop_assert_eq!(controller.tracker().current(), None);
                            prev_emitted_last = None;
                        }
                    }
                }
                ResyncOp::ApplySnapshot { last_update_id } => {
                    let snapshot = make_snapshot(last_update_id);
                    let was_in_sync = controller.state() == SyncState::InSync;
                    let outcome = controller.on_snapshot(snapshot).expect("valid snapshot");

                    match outcome {
                        SnapshotOutcome::Synchronized { snapshot, replay } => {
                            prop_assert!(!was_in_sync);
                            prop_assert_eq!(controller.state(), SyncState::InSync);
                            prop_assert!(controller.tracker().current().is_some());
                            prop_assert_eq!(controller.buffered_count(), 0);

                            // Invariant: replay deltas must be contiguous from snapshot ID
                            let mut anchor = snapshot.kind.last_update_id.expect("has last_update_id");
                            for r in replay {
                                let seq = r.kind.sequence.expect("has sequence");
                                prop_assert!(seq.first <= anchor + 1);
                                prop_assert!(seq.last >= anchor);
                                anchor = seq.last;
                            }
                            prev_emitted_last = Some(anchor);
                        }
                        SnapshotOutcome::StaleSnapshot { snapshot_id, first_buffered } => {
                            prop_assert!(!was_in_sync);
                            prop_assert_eq!(controller.state(), SyncState::BufferingDiffs);
                            prop_assert!(first_buffered > snapshot_id + 1);

                            // Invariant: all retained deltas must have last > snapshot_id
                            for d in controller.buffered_deltas() {
                                let seq = d.kind.sequence.expect("has sequence");
                                prop_assert!(seq.last > snapshot_id);
                            }
                            if let Some(first_d) = controller.buffered_deltas().front() {
                                let seq = first_d.kind.sequence.expect("has sequence");
                                prop_assert_eq!(seq.first, first_buffered);
                            }
                        }
                        SnapshotOutcome::Ignored => {
                            prop_assert!(was_in_sync);
                        }
                    }
                }
                ResyncOp::Reset => {
                    controller.reset();
                    prop_assert_eq!(controller.state(), SyncState::BufferingDiffs);
                    prop_assert_eq!(controller.buffered_count(), 0);
                    prop_assert_eq!(controller.tracker().current(), None);
                    prev_emitted_last = None;
                }
            }

            // Invariants checked after EVERY step:
            // 1. Buffer never exceeds max_buffer
            prop_assert!(controller.buffered_count() <= max_buffer);

            // 2. InSync implies tracker is initialized and buffer is empty
            if controller.state() == SyncState::InSync {
                prop_assert!(controller.tracker().current().is_some());
                prop_assert_eq!(controller.buffered_count(), 0);
            }
        }
    }
}

#[test]
fn test_resync_controller_max_buffer_one() {
    let mut controller = ResyncController::new(1).expect("valid max_buffer 1");
    assert_eq!(controller.max_buffer(), 1);
    let d1 = make_delta(100, 105);
    assert!(controller.buffer(d1).is_ok());
    assert_eq!(controller.buffered_count(), 1);

    // Second delta overflows buffer of size 1 and purges
    let d2 = make_delta(106, 110);
    let err = controller.buffer(d2).unwrap_err();
    match err {
        BufferError::Overflow(overflow) => {
            assert_eq!(overflow.dropped, 1);
        }
        other => panic!("expected Overflow, got {other:?}"),
    }
    assert_eq!(controller.buffered_count(), 0);
}

#[test]
fn test_resync_controller_rejects_zero_max_buffer() {
    use mdc_core::ConfigError;
    assert!(matches!(
        ResyncController::new(0),
        Err(ConfigError::InvalidMaxBuffer)
    ));
}
