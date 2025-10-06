#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::bitfinex::bench_support::Connection;
use mdc_testkit::{NoopStreamingSink, load_fixture};

fn parse_depth_update(c: &mut Criterion) {
    let snap_str =
        load_fixture("crates/mdc-providers/tests/fixtures/bitfinex/order_book_snapshot.json")
            .expect("load snapshot fixture");
    let upd_str = load_fixture("crates/mdc-providers/tests/fixtures/bitfinex/depth_update.json")
        .expect("load update fixture");

    let snap_bytes = snap_str.as_bytes();
    let upd_bytes = upd_str.as_bytes();

    let mut conn = Connection::new(NoopStreamingSink);
    // Warm-up: initialize shadow book with snapshot and warm up scratch buffers
    conn.handle_frame(snap_bytes);
    conn.handle_frame(upd_bytes);

    let allocs = allocation_counter::measure(|| {
        conn.handle_frame(upd_bytes);
    });
    assert_eq!(
        allocs.count_total, 0,
        "streaming path must not allocate after warm-up"
    );

    c.bench_function("bitfinex/depth_update/streaming_zero_alloc", |b| {
        b.iter(|| {
            conn.handle_frame(black_box(upd_bytes));
        });
    });
}

criterion_group!(benches, parse_depth_update);
criterion_main!(benches);
