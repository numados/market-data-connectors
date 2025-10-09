#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::frankfurter::bench_support::Connection;
use mdc_testkit::{NoopStreamingSink, load_fixture};

fn parse_frankfurter_rates(c: &mut Criterion) {
    let rates_str =
        load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json")
            .expect("load rates fixture");

    let rates_bytes = rates_str.as_bytes();

    let mut conn = Connection::new(NoopStreamingSink);
    // Warm-up: process frame to warm pre-allocated scratch buffers
    conn.handle_frame(rates_bytes);
    conn.handle_frame(rates_bytes);

    let allocs = allocation_counter::measure(|| {
        conn.handle_frame(rates_bytes);
    });
    assert_eq!(
        allocs.count_total, 0,
        "streaming path must not allocate after warm-up"
    );

    c.bench_function("frankfurter/rates/streaming_zero_alloc", |b| {
        b.iter(|| {
            conn.handle_frame(black_box(rates_bytes));
        });
    });
}

criterion_group!(benches, parse_frankfurter_rates);
criterion_main!(benches);
