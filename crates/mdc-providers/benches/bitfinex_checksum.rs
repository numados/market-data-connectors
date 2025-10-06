#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::bitfinex::checksum::BitfinexChecksumBook;
use mdc_providers::bitfinex::raw::RawBookLevel;

fn bench_checksum_250_levels(c: &mut Criterion) {
    let mut book = BitfinexChecksumBook::new(25);

    // Populate 250 bids and 250 asks
    let mut levels = Vec::new();
    for i in 0..250 {
        let p_bid = format!("{}", 40_000 - i);
        let q_bid = format!("1.{:04}", i % 10000);
        let json_bid = format!("[{}, 2, \"{}\"]", p_bid, q_bid);
        levels.push(json_bid);

        let p_ask = format!("{}", 50_000 + i);
        let q_ask = format!("-1.{:04}", i % 10000);
        let json_ask = format!("[{}, 2, \"{}\"]", p_ask, q_ask);
        levels.push(json_ask);
    }

    for json in &levels {
        let lvl: RawBookLevel<'_> = serde_json::from_str(json).unwrap();
        book.apply(&lvl).unwrap();
    }

    // Warm-up and assert zero allocations
    let allocs = allocation_counter::measure(|| {
        let _ = book.checksum();
    });
    assert_eq!(
        allocs.count_total, 0,
        "Bitfinex CRC32 checksum computation must not allocate on the heap"
    );

    c.bench_function("bitfinex/checksum/top25_over_250_levels", |b| {
        b.iter(|| {
            black_box(book.checksum());
        });
    });
}

criterion_group!(benches, bench_checksum_250_levels);
criterion_main!(benches);
