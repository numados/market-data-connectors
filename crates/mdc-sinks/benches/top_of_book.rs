#![allow(clippy::expect_used, clippy::unwrap_used)]

//! Criterion benchmarks for TopOfBookProjector and L2Book.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_core::{
    DeliveryStamp, L2Delta, L2Snapshot, MarketDataKind, MarketEvent, PriceLevel, RunId, Symbol,
    UnixNanos, VenueId,
};
use mdc_sinks::{L2Book, TopOfBookProjector};
use rust_decimal::Decimal;

fn build_1000_level_snapshot() -> L2Snapshot {
    let mut bids = Vec::with_capacity(500);
    let mut asks = Vec::with_capacity(500);

    for i in 0..500 {
        bids.push(PriceLevel::new(
            Decimal::from(50_000 - i),
            Decimal::from(i + 1),
        ));
        asks.push(PriceLevel::new(
            Decimal::from(50_001 + i),
            Decimal::from(i + 1),
        ));
    }

    L2Snapshot::new(bids, asks, Some(1000))
}

fn bench_l2_book_delta_apply_1000_levels(c: &mut Criterion) {
    let snapshot = build_1000_level_snapshot();
    let mut book = L2Book::new(Some(1000));
    book.apply_snapshot(&snapshot).expect("snapshot applied");

    let delta = L2Delta {
        sequence: None,
        bids: vec![PriceLevel::new(Decimal::from(50_000), Decimal::from(10))],
        asks: vec![PriceLevel::new(Decimal::from(50_001), Decimal::from(20))],
    };

    // Warm-up to ensure scratch buffers have capacity
    book.apply_delta(&delta).expect("warmup delta applied");

    let allocs = allocation_counter::measure(|| {
        book.apply_delta(&delta).expect("delta applied");
    });
    assert_eq!(
        allocs.count_total, 0,
        "L2Book::apply_delta must perform 0 allocations after warm-up on 1000-level book"
    );

    c.bench_function("sinks/book/apply_delta_1000_levels_zero_alloc", |b| {
        b.iter(|| {
            book.apply_delta(black_box(&delta)).expect("applied");
        });
    });
}

fn bench_top_of_book_in_sync_delta_1000_levels(c: &mut Criterion) {
    let snapshot = build_1000_level_snapshot();
    let sym = Symbol::from("BTCUSDT");
    let mut proj = TopOfBookProjector::new_l2(sym.clone(), 1, Some(1000));

    let snap_ev = MarketEvent {
        exchange_ts: Some(UnixNanos(1_000_000)),
        received_ts: UnixNanos(1_000_100),
        venue: VenueId::Binance,
        symbol: sym.clone(),
        tag: 1,
        delivery: DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1),
        source_sequence: None,
        kind: MarketDataKind::L2Snapshot(snapshot),
    };
    proj.handle_event(&snap_ev);

    let resync_ev = MarketEvent {
        exchange_ts: None,
        received_ts: UnixNanos(1_000_200),
        venue: VenueId::Binance,
        symbol: sym.clone(),
        tag: 1,
        delivery: DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 2),
        source_sequence: None,
        kind: MarketDataKind::IntegrityAlert(mdc_core::IntegrityAlert::ResyncCompleted {
            last_update_id: Some(1000),
        }),
    };
    proj.handle_event(&resync_ev);
    assert!(proj.is_in_sync());

    let delta = L2Delta {
        sequence: None,
        bids: vec![PriceLevel::new(Decimal::from(50_000), Decimal::from(15))],
        asks: vec![PriceLevel::new(Decimal::from(50_001), Decimal::from(25))],
    };
    let delta_ev = MarketEvent {
        exchange_ts: Some(UnixNanos(1_001_000)),
        received_ts: UnixNanos(1_001_100),
        venue: VenueId::Binance,
        symbol: sym,
        tag: 1,
        delivery: DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 3),
        source_sequence: None,
        kind: MarketDataKind::L2Delta(delta),
    };

    // Warm-up
    proj.handle_event(&delta_ev);

    let allocs = allocation_counter::measure(|| {
        proj.handle_event(&delta_ev);
    });
    assert_eq!(
        allocs.count_total, 0,
        "TopOfBookProjector::handle_event must perform 0 allocations per in-sync delta"
    );

    c.bench_function(
        "sinks/top_of_book/in_sync_delta_1000_levels_zero_alloc",
        |b| {
            b.iter(|| {
                proj.handle_event(black_box(&delta_ev));
            });
        },
    );
}

criterion_group!(
    benches,
    bench_l2_book_delta_apply_1000_levels,
    bench_top_of_book_in_sync_delta_1000_levels
);
criterion_main!(benches);
