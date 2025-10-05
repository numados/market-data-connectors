#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::binance::bench_support::Connection;
use mdc_testkit::{NoopStreamingSink, load_fixture};

fn parse_depth_update(c: &mut Criterion) {
    let fixture_str = load_fixture("crates/mdc-providers/tests/fixtures/binance/depth_update.json")
        .expect("load fixture");
    let bytes = fixture_str.as_bytes();

    let mut conn = Connection::new(NoopStreamingSink);
    conn.handle_frame(bytes); // warm-up: scratch buffers reach capacity

    let allocs = allocation_counter::measure(|| {
        conn.handle_frame(bytes);
    });
    assert_eq!(
        allocs.count_total, 0,
        "streaming path must not allocate after warm-up"
    );

    c.bench_function("binance/depth_update/streaming_zero_alloc", |b| {
        b.iter(|| {
            conn.handle_frame(black_box(bytes));
        });
    });
}

criterion_group!(benches, parse_depth_update);
criterion_main!(benches);
