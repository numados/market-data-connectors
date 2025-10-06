#![cfg(feature = "bitfinex")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Property-based testing for Bitfinex CRC32 interleaved checksum sensitivity and update order invariance.

use mdc_providers::bitfinex::checksum::BitfinexChecksumBook;
use mdc_providers::bitfinex::raw::RawBookLevel;
use proptest::prelude::*;

type BookSideLevels = Vec<(String, u32, String)>;

// Generates sorted unique prices, counts, and amounts for bids and asks.
// In Bitfinex, bid amounts are positive (e.g. "0.5") and ask amounts are negative (e.g. "-0.5").
fn arb_bitfinex_levels() -> impl Strategy<Value = (BookSideLevels, BookSideLevels)> {
    (5usize..30, 5usize..30).prop_flat_map(|(num_bids, num_asks)| {
        let bids_strat =
            proptest::collection::vec((10_000u32..50_000, 1u32..10, 1u32..999), num_bids).prop_map(
                |triplets| {
                    let mut prices: Vec<u32> = triplets.iter().map(|(p, _, _)| *p).collect();
                    prices.sort_unstable_by(|a, b| b.cmp(a)); // Bids descending
                    prices.dedup();
                    prices
                        .into_iter()
                        .zip(triplets.into_iter().map(|(_, c, a)| (c, a)))
                        .map(|(p, (count, amount))| {
                            (
                                format!("{}.{:02}", p, p % 97),
                                count,
                                format!("{}.{:04}", amount / 10 + 1, amount % 100),
                            )
                        })
                        .collect::<Vec<_>>()
                },
            );

        let asks_strat =
            proptest::collection::vec((10_000u32..50_000, 1u32..10, 1u32..999), num_asks).prop_map(
                |triplets| {
                    let mut prices: Vec<u32> = triplets.iter().map(|(p, _, _)| *p).collect();
                    prices.sort_unstable(); // Asks ascending
                    prices.dedup();
                    prices
                        .into_iter()
                        .zip(triplets.into_iter().map(|(_, c, a)| (c, a)))
                        .map(|(p, (count, amount))| {
                            (
                                format!("{}.{:02}", p + 50_000, p % 97),
                                count,
                                format!("-{}.{:04}", amount / 10 + 1, amount % 100), // Negative amount for asks
                            )
                        })
                        .collect::<Vec<_>>()
                },
            );

        (bids_strat, asks_strat)
    })
}

fn apply_levels(book: &mut BitfinexChecksumBook, levels: &[(String, u32, String)]) {
    for (p, c, a) in levels {
        let json = format!(r#"["{p}", {c}, "{a}"]"#);
        let raw: RawBookLevel<'_> = serde_json::from_str(&json).expect("valid raw book level");
        let _ = book.apply(&raw);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn property_single_level_mutation_changes_checksum(
        (bids, asks) in arb_bitfinex_levels(),
        mutation_side in 0u8..2,
        mutation_type in 0u8..3,
    ) {
        let mut original_book = BitfinexChecksumBook::new(25);
        apply_levels(&mut original_book, &bids);
        apply_levels(&mut original_book, &asks);
        let original_checksum = original_book.checksum();

        let mut mutated_bids = bids.clone();
        let mut mutated_asks = asks.clone();

        if mutation_side == 0 && !mutated_bids.is_empty() {
            let idx = 0; // Top bid
            match mutation_type {
                0 => {
                    // Price digit mutation
                    let (p, c, a) = &mutated_bids[idx];
                    let mutated_p = format!("{}9", p);
                    mutated_bids[idx] = (mutated_p, *c, a.clone());
                }
                1 => {
                    // Amount mutation
                    let (p, c, a) = &mutated_bids[idx];
                    let mutated_a = format!("{}5", a);
                    mutated_bids[idx] = (p.clone(), *c, mutated_a);
                }
                _ => {
                    // Level removal (count 0)
                    let (p, _, a) = &mutated_bids[idx];
                    mutated_bids[idx] = (p.clone(), 0, a.clone());
                }
            }
        } else if !mutated_asks.is_empty() {
            let idx = 0; // Top ask
            match mutation_type {
                0 => {
                    // Price digit mutation
                    let (p, c, a) = &mutated_asks[idx];
                    let mutated_p = format!("{}1", p);
                    mutated_asks[idx] = (mutated_p, *c, a.clone());
                }
                1 => {
                    // Amount mutation
                    let (p, c, a) = &mutated_asks[idx];
                    let mutated_a = format!("{}8", a);
                    mutated_asks[idx] = (p.clone(), *c, mutated_a);
                }
                _ => {
                    // Level removal (count 0)
                    let (p, _, a) = &mutated_asks[idx];
                    mutated_asks[idx] = (p.clone(), 0, a.clone());
                }
            }
        }

        let mut mutated_book = BitfinexChecksumBook::new(25);
        apply_levels(&mut mutated_book, &mutated_bids);
        apply_levels(&mut mutated_book, &mutated_asks);
        let mutated_checksum = mutated_book.checksum();

        prop_assert_ne!(
            original_checksum,
            mutated_checksum,
            "Checksum collision detected between original and mutated book!"
        );
    }

    #[test]
    fn property_update_order_invariance(
        (bids, asks) in arb_bitfinex_levels(),
    ) {
        // Book A: apply bids first, then asks
        let mut book_a = BitfinexChecksumBook::new(25);
        apply_levels(&mut book_a, &bids);
        apply_levels(&mut book_a, &asks);

        // Book B: apply asks first, then bids
        let mut book_b = BitfinexChecksumBook::new(25);
        apply_levels(&mut book_b, &asks);
        apply_levels(&mut book_b, &bids);

        prop_assert_eq!(
            book_a.checksum(),
            book_b.checksum(),
            "Insertion order of distinct price levels must not affect checksum"
        );
    }
}
