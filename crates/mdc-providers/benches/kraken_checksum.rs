#![allow(clippy::expect_used, clippy::unwrap_used)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mdc_providers::kraken::checksum::{KrakenChecksumBook, RawLevelOwned};
use rust_decimal::Decimal;
use smallstr::SmallString;

fn bench_checksum_1000_levels(c: &mut Criterion) {
    let mut book = KrakenChecksumBook::new(1000);

    let asks: Vec<RawLevelOwned> = (0..1000)
        .map(|i| {
            let p_str = format!("{}.{:02}", 50_000 + i, i % 100);
            let q_str = format!("1.{:04}", i % 10000);
            RawLevelOwned {
                price: Decimal::from_str_exact(&p_str).unwrap(),
                volume: Decimal::from_str_exact(&q_str).unwrap(),
                price_raw: SmallString::from(p_str.as_str()),
                volume_raw: SmallString::from(q_str.as_str()),
            }
        })
        .collect();

    let bids: Vec<RawLevelOwned> = (0..1000)
        .map(|i| {
            let p_str = format!("{}.{:02}", 40_000 - i, i % 100);
            let q_str = format!("2.{:04}", i % 10000);
            RawLevelOwned {
                price: Decimal::from_str_exact(&p_str).unwrap(),
                volume: Decimal::from_str_exact(&q_str).unwrap(),
                price_raw: SmallString::from(p_str.as_str()),
                volume_raw: SmallString::from(q_str.as_str()),
            }
        })
        .collect();

    book.apply_owned(&asks, &bids);

    let allocs = allocation_counter::measure(|| {
        let _ = book.checksum();
    });
    assert_eq!(
        allocs.count_total, 0,
        "Kraken CRC32 checksum computation must not allocate"
    );

    c.bench_function("kraken/checksum/top10_over_1000_levels", |b| {
        b.iter(|| {
            black_box(book.checksum());
        });
    });
}

criterion_group!(benches, bench_checksum_1000_levels);
criterion_main!(benches);
