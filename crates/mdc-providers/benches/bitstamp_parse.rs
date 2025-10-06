#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::bitstamp::bench_support::Connection;
use mdc_testkit::{NoopStreamingSink, load_fixture};

fn parse_depth_update(c: &mut Criterion) {
    let upd_str = load_fixture("crates/mdc-providers/tests/fixtures/bitstamp/depth_update.json")
        .expect("load update fixture");

    let upd_bytes = upd_str.as_bytes();

    let mut conn = Connection::new(NoopStreamingSink);
    // Warm-up: process update to warm pre-allocated scratch buffers
    conn.handle_frame(upd_bytes);
    conn.handle_frame(upd_bytes);

    let allocs = allocation_counter::measure(|| {
        conn.handle_frame(upd_bytes);
    });
    assert_eq!(
        allocs.count_total, 0,
        "streaming path must not allocate after warm-up"
    );

    c.bench_function("bitstamp/depth_update/streaming_zero_alloc", |b| {
        b.iter(|| {
            conn.handle_frame(black_box(upd_bytes));
        });
    });
}

criterion_group!(benches, parse_depth_update);
criterion_main!(benches);
