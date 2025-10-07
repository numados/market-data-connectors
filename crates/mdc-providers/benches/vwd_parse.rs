#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::vwd::bench_support::Connection;
use mdc_testkit::{NoopStreamingSink, load_fixture};

fn parse_snapquote(c: &mut Criterion) {
    let snap_str = load_fixture("crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json")
        .expect("load snapshot fixture");

    let snap_bytes = snap_str.as_bytes();

    let mut conn = Connection::new(NoopStreamingSink);
    // Warm-up: process snapshot to warm pre-allocated scratch buffers
    conn.handle_frame(snap_bytes);
    conn.handle_frame(snap_bytes);

    let allocs = allocation_counter::measure(|| {
        conn.handle_frame(snap_bytes);
    });
    assert_eq!(
        allocs.count_total, 0,
        "streaming path must not allocate after warm-up"
    );

    c.bench_function("vwd/snapquote/streaming_zero_alloc", |b| {
        b.iter(|| {
            conn.handle_frame(black_box(snap_bytes));
        });
    });
}

criterion_group!(benches, parse_snapquote);
criterion_main!(benches);
