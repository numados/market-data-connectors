//! Criterion benchmarks for mdc-core integrity and resync hot path.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_core::integrity::SequenceTracker;
use mdc_core::model::{
    DeliveryStamp, L2Delta, L2Snapshot, MarketEvent, PriceLevel, RunId, SequenceRange, Symbol,
    UnixNanos, VenueId,
};
use mdc_core::resync::{ResyncController, SyncState};
use rust_decimal::Decimal;

fn bench_sequence_tracker_check(c: &mut Criterion) {
    let mut tracker = SequenceTracker::new();
    tracker.reset_to(100);
    let mut seq = 100u64;

    c.bench_function("sequence_tracker_check", |b| {
        b.iter(|| {
            seq = seq.wrapping_add(1);
            let range = SequenceRange::new(seq, seq);
            let check = tracker.check(black_box(range));
            black_box(check);
        })
    });
}

fn bench_resync_controller_check_delta_in_sync(c: &mut Criterion) {
    let mut controller = ResyncController::new(5000).expect("valid capacity");
    let snap = MarketEvent {
        exchange_ts: Some(UnixNanos(1_000)),
        received_ts: UnixNanos(1_000),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
        source_sequence: None,
        kind: L2Snapshot::new(vec![], vec![], Some(100)),
    };
    let _ = controller.on_snapshot(snap);
    assert_eq!(controller.state(), SyncState::InSync);

    let mut seq = 100u64;
    c.bench_function("resync_controller_check_delta_in_sync", |b| {
        b.iter(|| {
            seq = seq.wrapping_add(1);
            let range = SequenceRange::new(seq, seq);
            let decision = controller.check_delta(black_box(range));
            black_box(decision);
        })
    });
}

fn bench_resync_controller_on_snapshot_5000(c: &mut Criterion) {
    let template_deltas: Vec<MarketEvent<L2Delta>> = (1..=5000)
        .map(|i| MarketEvent {
            exchange_ts: Some(UnixNanos(1_000)),
            received_ts: UnixNanos(1_000),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: 0,
            delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, i),
            source_sequence: None,
            kind: L2Delta::new(
                vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
                vec![PriceLevel::new(Decimal::from(101), Decimal::from(1))],
                Some(SequenceRange::new(i, i)),
            ),
        })
        .collect();

    c.bench_function("resync_controller_on_snapshot_5000_deltas", |b| {
        b.iter(|| {
            let mut controller = ResyncController::new(6000).expect("valid capacity");
            for d in &template_deltas {
                let _ = controller.buffer(d.clone());
            }
            let snap = MarketEvent {
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
                    Some(black_box(100)),
                ),
            };
            let outcome = controller.on_snapshot(black_box(snap));
            let _ = black_box(outcome);
        })
    });
}

criterion_group!(
    benches,
    bench_sequence_tracker_check,
    bench_resync_controller_check_delta_in_sync,
    bench_resync_controller_on_snapshot_5000
);
criterion_main!(benches);
