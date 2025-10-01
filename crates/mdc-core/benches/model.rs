//! Criterion benchmarks for mdc-core model hot path.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_core::model::{L2Snapshot, PriceLevel, Symbol, UnixNanos};
use rust_decimal::Decimal;

fn bench_symbol_creation(c: &mut Criterion) {
    c.bench_function("symbol_inline_creation", |b| {
        b.iter(|| {
            let s = Symbol::from(black_box("BTCUSDT"));
            black_box(s);
        })
    });
}

fn bench_unix_nanos_now(c: &mut Criterion) {
    c.bench_function("unix_nanos_now", |b| {
        b.iter(|| {
            let ts = UnixNanos::now();
            black_box(ts);
        })
    });
}

fn bench_snapshot_serialization(c: &mut Criterion) {
    let mut bids = Vec::with_capacity(1000);
    let mut asks = Vec::with_capacity(1000);

    for i in 0..1000 {
        bids.push(PriceLevel::new(
            Decimal::from(50_000 - i),
            Decimal::from(i + 1),
        ));
        asks.push(PriceLevel::new(
            Decimal::from(50_001 + i),
            Decimal::from(i + 1),
        ));
    }

    let snapshot = L2Snapshot::new(bids, asks, Some(12345678));

    c.bench_function("snapshot_1000_levels_serde_json", |b| {
        b.iter(|| {
            let json =
                serde_json::to_string(black_box(&snapshot)).unwrap_or_else(|_| panic!("serialize"));
            black_box(json);
        })
    });
}

criterion_group!(
    benches,
    bench_symbol_creation,
    bench_unix_nanos_now,
    bench_snapshot_serialization
);
criterion_main!(benches);
