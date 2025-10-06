//! Local shadow order book maintaining exact string tokens for Bitfinex CRC32 validation.

use std::collections::BTreeMap;

use mdc_core::integrity::{BitfinexChecksum, ChecksumStrategy, RawLevelRef};
use rust_decimal::Decimal;

use crate::bitfinex::raw::{RawBookLevel, RawBookLevelOwned};

/// Verdict of a checksum verification against a received value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumVerdict {
    /// Checksum matches the calculated value.
    Valid,
    /// Checksum differs from the calculated value.
    Mismatch {
        /// Checksum received from venue message.
        expected: i32,
        /// Checksum calculated from local shadow book.
        calculated: i32,
    },
}

/// Local shadow order book for Bitfinex WebSocket v2.
///
/// Stores bids (descending) and asks (ascending) in `BTreeMap`s, retaining the exact raw JSON
/// strings for price and amount so that CRC32 checksums match Bitfinex's calculation
/// without rounding or decimal formatting drift.
#[derive(Debug, Clone)]
pub struct BitfinexChecksumBook {
    depth: usize,
    bids: BTreeMap<Decimal, RawBookLevelOwned>,
    asks: BTreeMap<Decimal, RawBookLevelOwned>,
}

impl Default for BitfinexChecksumBook {
    fn default() -> Self {
        Self::new(25)
    }
}

impl BitfinexChecksumBook {
    /// Creates a new empty `BitfinexChecksumBook` with the configured checksum depth (default 25).
    pub fn new(depth: usize) -> Self {
        Self {
            depth,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// Clears all levels from both sides of the book.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Returns the maximum checksum level inspection depth.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Returns true if the book contains no levels on either side.
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Returns the number of bid levels currently tracked.
    pub fn bid_count(&self) -> usize {
        self.bids.len()
    }

    /// Returns the number of ask levels currently tracked.
    pub fn ask_count(&self) -> usize {
        self.asks.len()
    }

    /// Applies a raw price level update or removal to the shadow book.
    ///
    /// Bitfinex rules:
    /// - `count > 0`: insert or update level. Side is Bid if `amount > 0`, Ask if `amount < 0`.
    /// - `count == 0`: delete price level. Side is Bid if `amount > 0` (or `1`), Ask if `amount < 0` (or `-1`).
    pub fn apply(&mut self, level: &RawBookLevel<'_>) -> Result<(), rust_decimal::Error> {
        let level_owned = RawBookLevelOwned::from_raw(level)?;
        let price = level_owned.price;

        if level.count == 0 {
            if level_owned.amount.is_sign_positive() {
                self.bids.remove(&price);
            } else {
                self.asks.remove(&price);
            }
        } else if level_owned.amount.is_sign_positive() {
            self.bids.insert(price, level_owned);
        } else {
            self.asks.insert(price, level_owned);
        }

        Ok(())
    }

    /// Computes the signed 32-bit CRC32 checksum over the top 25 levels of bids and asks.
    pub fn checksum(&self) -> i32 {
        let max_depth = self.depth.min(25);
        let mut top_bids = [RawLevelRef::new(Decimal::ZERO, Decimal::ZERO, "", ""); 25];
        let mut bid_count = 0;
        for l in self.bids.values().rev().take(max_depth) {
            top_bids[bid_count] = RawLevelRef::new(
                l.price,
                l.amount,
                l.price_raw.as_str(),
                l.amount_raw.as_str(),
            );
            bid_count += 1;
        }

        let mut top_asks = [RawLevelRef::new(Decimal::ZERO, Decimal::ZERO, "", ""); 25];
        let mut ask_count = 0;
        for l in self.asks.values().take(max_depth) {
            top_asks[ask_count] = RawLevelRef::new(
                l.price,
                l.amount,
                l.price_raw.as_str(),
                l.amount_raw.as_str(),
            );
            ask_count += 1;
        }

        let strategy = BitfinexChecksum::new(self.depth);
        strategy.compute(&top_bids[..bid_count], &top_asks[..ask_count]) as i32
    }

    /// Verifies the local book checksum against a venue-reported `cs` checksum.
    pub fn verify(&self, received: i32) -> ChecksumVerdict {
        let calculated = self.checksum();
        if calculated == received {
            ChecksumVerdict::Valid
        } else {
            ChecksumVerdict::Mismatch {
                expected: received,
                calculated,
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::Value;

    const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/bitfinex/");

    #[test]
    fn test_checksum_book_reproduces_all_vectors() {
        let vectors_str = std::fs::read_to_string(format!("{FIXTURE_DIR}checksum_vectors.json"))
            .expect("failed to load checksum_vectors.json");
        let vectors: Value = serde_json::from_str(&vectors_str).expect("invalid json");

        for (idx, vec_item) in vectors.as_array().unwrap().iter().enumerate() {
            let desc = vec_item["description"].as_str().unwrap();
            let mut book = BitfinexChecksumBook::new(25);

            // Apply snapshot
            let snapshot = vec_item["snapshot"].as_array().unwrap();
            for level_val in snapshot {
                let lvl_str = level_val.to_string();
                let lvl: RawBookLevel<'_> = serde_json::from_str(&lvl_str)
                    .unwrap_or_else(|e| panic!("failed on {desc}: {e}"));
                book.apply(&lvl).unwrap();
            }

            // Apply updates
            let updates = vec_item["updates"].as_array().unwrap();
            for update_val in updates {
                let upd_str = update_val.to_string();
                let lvl: RawBookLevel<'_> = serde_json::from_str(&upd_str)
                    .unwrap_or_else(|e| panic!("failed on {desc}: {e}"));
                book.apply(&lvl).unwrap();
            }

            let expected_checksum = vec_item["expected_checksum"].as_i64().unwrap() as i32;
            let calculated = book.checksum();
            assert_eq!(
                calculated, expected_checksum,
                "vector {idx} ('{desc}') failed: expected {expected_checksum}, calculated {calculated}"
            );
            assert_eq!(book.verify(expected_checksum), ChecksumVerdict::Valid);
        }
    }

    #[test]
    fn test_checksum_mismatch_detected() {
        let mut book = BitfinexChecksumBook::new(25);
        let lvl1: RawBookLevel<'_> = serde_json::from_str(r#"[41698, 3, "0.71"]"#).unwrap();
        let lvl2: RawBookLevel<'_> = serde_json::from_str(r#"[41699, 2, "-0.5"]"#).unwrap();
        book.apply(&lvl1).unwrap();
        book.apply(&lvl2).unwrap();

        let verdict = book.verify(12345678);
        match verdict {
            ChecksumVerdict::Mismatch {
                expected,
                calculated,
            } => {
                assert_eq!(expected, 12345678);
                assert_ne!(calculated, 12345678);
            }
            ChecksumVerdict::Valid => panic!("expected mismatch"),
        }
    }

    #[test]
    fn test_delete_level_for_both_sides() {
        let mut book = BitfinexChecksumBook::new(25);
        let bid: RawBookLevel<'_> = serde_json::from_str(r#"[100, 1, "1.0"]"#).unwrap();
        let ask: RawBookLevel<'_> = serde_json::from_str(r#"[101, 1, "-1.0"]"#).unwrap();
        book.apply(&bid).unwrap();
        book.apply(&ask).unwrap();
        assert_eq!(book.bid_count(), 1);
        assert_eq!(book.ask_count(), 1);

        // Delete bid
        let del_bid: RawBookLevel<'_> = serde_json::from_str(r#"[100, 0, "1"]"#).unwrap();
        book.apply(&del_bid).unwrap();
        assert_eq!(book.bid_count(), 0);
        assert_eq!(book.ask_count(), 1);

        // Delete ask
        let del_ask: RawBookLevel<'_> = serde_json::from_str(r#"[101, 0, "-1"]"#).unwrap();
        book.apply(&del_ask).unwrap();
        assert_eq!(book.bid_count(), 0);
        assert_eq!(book.ask_count(), 0);
        assert!(book.is_empty());
    }
}
