#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::huobi::bench_support::Connection;
use mdc_testkit::{NoopStreamingSink, load_fixture, load_fixture_bytes};

fn parse_depth_update(c: &mut Criterion) {
    let snap_str =
        load_fixture("crates/mdc-providers/tests/fixtures/huobi/order_book_snapshot.json")
            .expect("load snapshot fixture");
    let upd_str = load_fixture("crates/mdc-providers/tests/fixtures/huobi/depth_update.json")
        .expect("load update fixture");

    let snap_bytes = snap_str.as_bytes();
    let upd_bytes = upd_str.as_bytes();

    let mut conn = Connection::new(NoopStreamingSink);
    // Warm-up: initialize with snapshot and warm up scratch buffers
    conn.handle_frame(snap_bytes);
    conn.handle_frame(upd_bytes);

    let allocs = allocation_counter::measure(|| {
        conn.handle_frame(upd_bytes);
    });
    assert_eq!(
        allocs.count_total, 0,
        "streaming path must not allocate after warm-up"
    );

    c.bench_function("huobi/depth_update/streaming_zero_alloc", |b| {
        b.iter(|| {
            conn.handle_frame(black_box(upd_bytes));
        });
    });
}

fn parse_depth_update_gzip(c: &mut Criterion) {
    let snap_str =
        load_fixture("crates/mdc-providers/tests/fixtures/huobi/order_book_snapshot.json")
            .expect("load snapshot fixture");
    let upd_gz =
        load_fixture_bytes("crates/mdc-providers/tests/fixtures/huobi/depth_update.json.gz")
            .expect("load gzip update fixture");

    let snap_bytes = snap_str.as_bytes();

    let mut conn = Connection::new(NoopStreamingSink);
    conn.handle_frame(snap_bytes);
    conn.handle_gzip_frame(&upd_gz);

    let allocs = allocation_counter::measure(|| {
        conn.handle_gzip_frame(&upd_gz);
    });
    assert_eq!(
        allocs.count_total, 0,
        "streaming path with gzip decompression must not allocate after warm-up"
    );

    c.bench_function("huobi/depth_update_gzip/streaming_zero_alloc", |b| {
        b.iter(|| {
            conn.handle_gzip_frame(black_box(&upd_gz));
        });
    });
}

criterion_group!(benches, parse_depth_update, parse_depth_update_gzip);
criterion_main!(benches);
