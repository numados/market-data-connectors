#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::binance::{CombinedStream, RawPayload, normalize_depth};
use mdc_testkit::load_fixture;

fn bench_binance_parse(c: &mut Criterion) {
    let fixture = load_fixture("crates/mdc-providers/tests/fixtures/binance/depth_update.json")
        .expect("load fixture");

    c.bench_function("binance_parse_and_normalize_depth", |b| {
        b.iter(|| {
            let parsed: CombinedStream<'_> =
                serde_json::from_str(black_box(&fixture)).expect("parse depth");
            if let RawPayload::Depth(ref depth) = parsed.data {
                let _ = normalize_depth(black_box(depth));
            }
        });
    });
}

criterion_group!(benches, bench_binance_parse);
criterion_main!(benches);
