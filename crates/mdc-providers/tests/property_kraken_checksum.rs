#![cfg(feature = "kraken")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Property-based testing for Kraken CRC32 checksum sensitivity and update order invariance.

use mdc_providers::kraken::checksum::KrakenChecksumBook;
use mdc_providers::kraken::raw::RawBookData;
use proptest::prelude::*;

fn make_book_json(asks: &[(String, String)], bids: &[(String, String)]) -> String {
    let asks_json = asks
        .iter()
        .map(|(p, q)| format!(r#"{{"price":{p},"qty":{q}}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let bids_json = bids
        .iter()
        .map(|(p, q)| format!(r#"{{"price":{p},"qty":{q}}}"#))
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"symbol":"BTC/USD","asks":[{asks_json}],"bids":[{bids_json}]}}"#)
}

type BookSideLevels = Vec<(String, String)>;

// Generates sorted unique prices and positive quantities for asks and bids
fn arb_book_levels() -> impl Strategy<Value = (BookSideLevels, BookSideLevels)> {
    (5usize..15, 5usize..15).prop_flat_map(|(num_asks, num_bids)| {
        let asks_strat = proptest::collection::vec((10_000u32..50_000, 1u32..999), num_asks)
            .prop_map(|pairs| {
                let mut prices: Vec<u32> = pairs.iter().map(|(p, _)| *p).collect();
                prices.sort_unstable();
                prices.dedup();
                prices
                    .into_iter()
                    .zip(pairs.into_iter().map(|(_, q)| q))
                    .map(|(p, q)| {
                        (
                            format!("{}.{:02}", p + 50_000, p % 97),
                            format!("{}.{:04}", q / 10 + 1, q % 100),
                        )
                    })
                    .collect::<Vec<_>>()
            });

        let bids_strat = proptest::collection::vec((10_000u32..50_000, 1u32..999), num_bids)
            .prop_map(|pairs| {
                let mut prices: Vec<u32> = pairs.iter().map(|(p, _)| *p).collect();
                prices.sort_unstable_by(|a, b| b.cmp(a));
                prices.dedup();
                prices
                    .into_iter()
                    .zip(pairs.into_iter().map(|(_, q)| q))
                    .map(|(p, q)| {
                        (
                            format!("{}.{:02}", p, p % 97),
                            format!("{}.{:04}", q / 10 + 1, q % 100),
                        )
                    })
                    .collect::<Vec<_>>()
            });

        (asks_strat, bids_strat)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn property_single_level_mutation_changes_checksum(
        (asks, bids) in arb_book_levels(),
        mutation_side in 0u8..2,
        mutation_type in 0u8..3,
    ) {
        let base_json = make_book_json(&asks, &bids);
        let raw_base: RawBookData<'_> = serde_json::from_str(&base_json).expect("valid base book json");
        let mut original_book = KrakenChecksumBook::new(100);
        original_book.apply(&raw_base.asks, &raw_base.bids).expect("apply base book");
        let original_checksum = original_book.checksum();

        // Perform mutation on top 10 levels
        let mut mutated_asks = asks.clone();
        let mut mutated_bids = bids.clone();

        if mutation_side == 0 && !mutated_asks.is_empty() {
            let idx = 0; // Top ask
            match mutation_type {
                0 => {
                    // Price digit mutation
                    let (p, q) = &mutated_asks[idx];
                    let mutated_price = format!("{}1", p);
                    mutated_asks[idx] = (mutated_price, q.clone());
                }
                1 => {
                    // Qty digit mutation
                    let (p, q) = &mutated_asks[idx];
                    let mutated_qty = format!("{}5", q);
                    mutated_asks[idx] = (p.clone(), mutated_qty);
                }
                _ => {
                    // Level removal
                    mutated_asks.remove(idx);
                }
            }
        } else if !mutated_bids.is_empty() {
            let idx = 0; // Top bid
            match mutation_type {
                0 => {
                    // Price digit mutation
                    let (p, q) = &mutated_bids[idx];
                    let mutated_price = format!("{}9", p);
                    mutated_bids[idx] = (mutated_price, q.clone());
                }
                1 => {
                    // Qty digit mutation
                    let (p, q) = &mutated_bids[idx];
                    let mutated_qty = format!("{}7", q);
                    mutated_bids[idx] = (p.clone(), mutated_qty);
                }
                _ => {
                    // Level removal
                    mutated_bids.remove(idx);
                }
            }
        }

        let mutated_json = make_book_json(&mutated_asks, &mutated_bids);
        let raw_mutated: RawBookData<'_> = serde_json::from_str(&mutated_json).expect("valid mutated book json");
        let mut mutated_book = KrakenChecksumBook::new(100);
        mutated_book.apply(&raw_mutated.asks, &raw_mutated.bids).expect("apply mutated book");
        let mutated_checksum = mutated_book.checksum();

        prop_assert_ne!(
            original_checksum,
            mutated_checksum,
            "Checksum collision detected between original and single-level mutated book!"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn property_update_order_invariance(
        (asks, bids) in arb_book_levels(),
        upd1_qty in 1u32..99,
        upd2_qty in 1u32..99,
    ) {
        let base_json = make_book_json(&asks, &bids);
        let raw_base: RawBookData<'_> = serde_json::from_str(&base_json).expect("valid base book json");

        let mut book_sequential = KrakenChecksumBook::new(100);
        book_sequential.apply(&raw_base.asks, &raw_base.bids).expect("apply base to sequential");

        let mut book_batched = KrakenChecksumBook::new(100);
        book_batched.apply(&raw_base.asks, &raw_base.bids).expect("apply base to batched");

        // Two updates targeting top 2 asks and bids with new quantities
        let update_ask_1 = (asks[0].0.clone(), format!("{}.5000", upd1_qty));
        let update_bid_1 = (bids[0].0.clone(), format!("{}.2500", upd2_qty));

        let update_ask_2 = (asks[1].0.clone(), format!("{}.1111", upd2_qty));
        let update_bid_2 = (bids[1].0.clone(), format!("{}.2222", upd1_qty));

        // Apply sequentially: update 1 then update 2
        let upd1_json = make_book_json(
            std::slice::from_ref(&update_ask_1),
            std::slice::from_ref(&update_bid_1),
        );
        let raw_upd1: RawBookData<'_> = serde_json::from_str(&upd1_json).expect("valid upd1 json");
        book_sequential.apply(&raw_upd1.asks, &raw_upd1.bids).expect("apply upd1");

        let upd2_json = make_book_json(
            std::slice::from_ref(&update_ask_2),
            std::slice::from_ref(&update_bid_2),
        );
        let raw_upd2: RawBookData<'_> = serde_json::from_str(&upd2_json).expect("valid upd2 json");
        book_sequential.apply(&raw_upd2.asks, &raw_upd2.bids).expect("apply upd2");

        // Apply in single batched message:
        let batch_json = make_book_json(
            &[update_ask_1, update_ask_2],
            &[update_bid_1, update_bid_2],
        );
        let raw_batch: RawBookData<'_> = serde_json::from_str(&batch_json).expect("valid batch json");
        book_batched.apply(&raw_batch.asks, &raw_batch.bids).expect("apply batch");

        prop_assert_eq!(
            book_sequential.checksum(),
            book_batched.checksum(),
            "Sequential updates vs batched updates must result in identical checksum"
        );
    }
}
