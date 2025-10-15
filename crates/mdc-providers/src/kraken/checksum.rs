//! Local shadow order book maintaining exact string tokens for CRC32 validation.

use std::collections::BTreeMap;

use mdc_core::integrity::{ChecksumStrategy, KrakenChecksum, RawLevelRef};
use mdc_core::{PriceLevel, Side};
use rust_decimal::Decimal;
use smallstr::SmallString;
use thiserror::Error;

use crate::kraken::raw::RawLevel;

/// Error encountered when parsing numeric fields of a raw price level.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum LevelParseError {
    /// Invalid price decimal representation.
    #[error("failed to parse level price '{token}': {reason}")]
    InvalidPrice {
        /// Raw token that failed parsing.
        token: String,
        /// Underlying parser error description.
        reason: String,
    },
    /// Invalid volume decimal representation.
    #[error("failed to parse level volume '{token}': {reason}")]
    InvalidVolume {
        /// Raw token that failed parsing.
        token: String,
        /// Underlying parser error description.
        reason: String,
    },
}

/// Owned price level retaining exact JSON number tokens within an inline buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLevelOwned {
    /// Parsed price decimal.
    pub price: Decimal,
    /// Parsed volume decimal.
    pub volume: Decimal,
    /// Raw token string for price (inline up to 24 bytes).
    pub price_raw: SmallString<[u8; 24]>,
    /// Raw token string for volume (inline up to 24 bytes).
    pub volume_raw: SmallString<[u8; 24]>,
}

struct RawLevelOwnedVisitor;

impl<'de> serde::de::Visitor<'de> for RawLevelOwnedVisitor {
    type Value = RawLevelOwned;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a level map with price and qty numbers")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: serde::de::MapAccess<'de>,
    {
        let mut price_str: Option<&'de serde_json::value::RawValue> = None;
        let mut qty_str: Option<&'de serde_json::value::RawValue> = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "price" => price_str = Some(map.next_value()?),
                "qty" => qty_str = Some(map.next_value()?),
                _ => {
                    let _: serde::de::IgnoredAny = map.next_value()?;
                }
            }
        }

        let p_raw = price_str.ok_or_else(|| serde::de::Error::missing_field("price"))?;
        let q_raw = qty_str.ok_or_else(|| serde::de::Error::missing_field("qty"))?;

        let price_s = p_raw.get();
        let qty_s = q_raw.get();

        let price = Decimal::from_str_exact(price_s).map_err(serde::de::Error::custom)?;
        let volume = Decimal::from_str_exact(qty_s).map_err(serde::de::Error::custom)?;

        Ok(RawLevelOwned {
            price,
            volume,
            price_raw: SmallString::from(price_s),
            volume_raw: SmallString::from(qty_s),
        })
    }
}

impl<'de> serde::de::Deserialize<'de> for RawLevelOwned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_map(RawLevelOwnedVisitor)
    }
}

/// Verdict of a checksum verification against a received value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumVerdict {
    /// Checksum matches the calculated value.
    Valid,
    /// Checksum differs from the calculated value.
    Mismatch {
        /// Checksum received from venue.
        expected: u32,
        /// Checksum calculated from local shadow book.
        calculated: u32,
    },
    /// No checksum was provided in the message.
    NotProvided,
}

/// Local shadow order book for Kraken WebSocket v2.
///
/// Stores bids and asks sorted in `BTreeMap`s, retaining the exact raw JSON
/// strings for price and volume so that CRC32 checksums match Kraken's calculation
use std::sync::Arc;

/// Local shadow order book maintaining exact string tokens for CRC32 validation
/// without rounding or decimal formatting drift.
#[derive(Clone)]
pub struct KrakenChecksumBook {
    depth: usize,
    bids: BTreeMap<Decimal, RawLevelOwned>,
    asks: BTreeMap<Decimal, RawLevelOwned>,
    strategy: Arc<dyn ChecksumStrategy>,
}

impl std::fmt::Debug for KrakenChecksumBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KrakenChecksumBook")
            .field("depth", &self.depth)
            .field("bids_len", &self.bids.len())
            .field("asks_len", &self.asks.len())
            .finish()
    }
}

impl KrakenChecksumBook {
    /// Creates a new empty `KrakenChecksumBook` with the configured subscribed depth.
    pub fn new(depth: usize) -> Self {
        Self::with_strategy(depth, Arc::new(KrakenChecksum::new(10)))
    }

    /// Creates a new empty `KrakenChecksumBook` with the configured depth and pluggable checksum strategy.
    pub fn with_strategy(depth: usize, strategy: Arc<dyn ChecksumStrategy>) -> Self {
        Self {
            depth,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            strategy,
        }
    }

    /// Clears all levels from both sides of the book.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Returns the subscribed depth limit.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Returns true if the book contains no levels on either side.
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Returns the number of bid levels currently in the book.
    pub fn bids_len(&self) -> usize {
        self.bids.len()
    }

    /// Returns the number of ask levels currently in the book.
    pub fn asks_len(&self) -> usize {
        self.asks.len()
    }

    /// Applies pre-parsed `RawLevelOwned` updates to the shadow book with zero allocations when overwriting.
    pub fn apply_owned(&mut self, asks: &[RawLevelOwned], bids: &[RawLevelOwned]) {
        for raw in asks {
            if raw.volume.is_zero() {
                self.asks.remove(&raw.price);
            } else {
                self.asks.insert(raw.price, raw.clone());
            }
        }
        while self.asks.len() > self.depth {
            self.asks.pop_last();
        }

        for raw in bids {
            if raw.volume.is_zero() {
                self.bids.remove(&raw.price);
            } else {
                self.bids.insert(raw.price, raw.clone());
            }
        }
        while self.bids.len() > self.depth {
            self.bids.pop_first();
        }
    }

    /// Applies one message's price level updates to the book.
    /// Applies incremental updates to the checksum book, tracking levels evicted by depth truncation.
    pub fn apply_with_evictions(
        &mut self,
        asks: &[RawLevel<'_>],
        bids: &[RawLevel<'_>],
        evicted_asks: &mut Vec<Decimal>,
        evicted_bids: &mut Vec<Decimal>,
    ) -> Result<(), LevelParseError> {
        evicted_asks.clear();
        evicted_bids.clear();

        // 1. Apply asks
        for raw in asks {
            let price_str = raw.price.get();
            let price =
                Decimal::from_str_exact(price_str).map_err(|e| LevelParseError::InvalidPrice {
                    token: price_str.to_owned(),
                    reason: e.to_string(),
                })?;

            let qty_str = raw.qty.get();
            let volume =
                Decimal::from_str_exact(qty_str).map_err(|e| LevelParseError::InvalidVolume {
                    token: qty_str.to_owned(),
                    reason: e.to_string(),
                })?;

            if volume.is_zero() {
                self.asks.remove(&price);
            } else {
                self.asks.insert(
                    price,
                    RawLevelOwned {
                        price,
                        volume,
                        price_raw: SmallString::from(price_str),
                        volume_raw: SmallString::from(qty_str),
                    },
                );
            }
        }

        // Truncate asks to depth: asks are sorted ascending, so pop highest prices.
        while self.asks.len() > self.depth {
            if let Some((price, _)) = self.asks.pop_last() {
                evicted_asks.push(price);
            }
        }

        // 2. Apply bids
        for raw in bids {
            let price_str = raw.price.get();
            let price =
                Decimal::from_str_exact(price_str).map_err(|e| LevelParseError::InvalidPrice {
                    token: price_str.to_owned(),
                    reason: e.to_string(),
                })?;

            let qty_str = raw.qty.get();
            let volume =
                Decimal::from_str_exact(qty_str).map_err(|e| LevelParseError::InvalidVolume {
                    token: qty_str.to_owned(),
                    reason: e.to_string(),
                })?;

            if volume.is_zero() {
                self.bids.remove(&price);
            } else {
                self.bids.insert(
                    price,
                    RawLevelOwned {
                        price,
                        volume,
                        price_raw: SmallString::from(price_str),
                        volume_raw: SmallString::from(qty_str),
                    },
                );
            }
        }

        // Truncate bids to depth: bids are sorted ascending, so pop lowest prices.
        while self.bids.len() > self.depth {
            if let Some((price, _)) = self.bids.pop_first() {
                evicted_bids.push(price);
            }
        }

        Ok(())
    }

    /// Applies incremental asks and bids updates to the checksum book.
    ///
    /// Asks are updated first, then bids. A level with quantity 0 is removed.
    /// After updates are applied on a side, the side is truncated to `depth`:
    /// highest prices are dropped from asks, lowest prices are dropped from bids.
    pub fn apply(
        &mut self,
        asks: &[RawLevel<'_>],
        bids: &[RawLevel<'_>],
    ) -> Result<(), LevelParseError> {
        let mut evicted_asks = Vec::new();
        let mut evicted_bids = Vec::new();
        self.apply_with_evictions(asks, bids, &mut evicted_asks, &mut evicted_bids)
    }

    /// Computes the order book checksum using the configured checksum strategy.
    pub fn checksum(&self) -> u32 {
        let max_depth = self.strategy.depth();
        if max_depth <= 10 {
            let mut top_asks = [RawLevelRef::new(Decimal::ZERO, Decimal::ZERO, "", ""); 10];
            let mut ask_count = 0;
            for (i, l) in self.asks.values().take(max_depth).enumerate() {
                top_asks[i] = RawLevelRef::new(l.price, l.volume, &l.price_raw, &l.volume_raw);
                ask_count += 1;
            }

            let mut top_bids = [RawLevelRef::new(Decimal::ZERO, Decimal::ZERO, "", ""); 10];
            let mut bid_count = 0;
            for (i, l) in self.bids.values().rev().take(max_depth).enumerate() {
                top_bids[i] = RawLevelRef::new(l.price, l.volume, &l.price_raw, &l.volume_raw);
                bid_count += 1;
            }

            self.strategy
                .compute(&top_bids[..bid_count], &top_asks[..ask_count])
        } else {
            let top_asks: Vec<_> = self
                .asks
                .values()
                .take(max_depth)
                .map(|l| RawLevelRef::new(l.price, l.volume, &l.price_raw, &l.volume_raw))
                .collect();
            let top_bids: Vec<_> = self
                .bids
                .values()
                .rev()
                .take(max_depth)
                .map(|l| RawLevelRef::new(l.price, l.volume, &l.price_raw, &l.volume_raw))
                .collect();
            self.strategy.compute(&top_bids, &top_asks)
        }
    }

    /// Compares the calculated checksum against an optional received value.
    pub fn verify(&self, received: Option<u32>) -> ChecksumVerdict {
        match received {
            Some(expected) => {
                let calculated = self.checksum();
                if expected == calculated {
                    ChecksumVerdict::Valid
                } else {
                    ChecksumVerdict::Mismatch {
                        expected,
                        calculated,
                    }
                }
            }
            None => ChecksumVerdict::NotProvided,
        }
    }

    /// Returns an iterator over the top `n` price levels for the specified side.
    ///
    /// Bids are returned highest price first; asks are returned lowest price first.
    pub fn top(&self, side: Side, n: usize) -> Vec<PriceLevel> {
        match side {
            Side::Ask => self
                .asks
                .values()
                .take(n)
                .map(|l| PriceLevel::new(l.price, l.volume))
                .collect(),
            Side::Bid => self
                .bids
                .values()
                .rev()
                .take(n)
                .map(|l| PriceLevel::new(l.price, l.volume))
                .collect(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::kraken::raw::RawMessage;

    const SNAPSHOT_JSON: &str =
        include_str!("../../tests/fixtures/kraken/order_book_snapshot.json");
    const UPDATE_JSON: &str = include_str!("../../tests/fixtures/kraken/depth_update.json");
    const DELETE_JSON: &str =
        include_str!("../../tests/fixtures/kraken/depth_update_delete_level.json");
    const VECTORS_JSON: &str = include_str!("../../tests/fixtures/kraken/checksum_vectors.json");

    #[test]
    fn test_checksum_book_snapshot_reproduction() {
        let msg: RawMessage<'_> =
            serde_json::from_str(SNAPSHOT_JSON).expect("deserialize snapshot");
        if let RawMessage::Book { data, .. } = msg {
            let mut book = KrakenChecksumBook::new(1000);
            book.apply(&data[0].asks, &data[0].bids)
                .expect("apply snapshot");
            assert_eq!(book.checksum(), 3310070434);
            assert_eq!(book.verify(data[0].checksum), ChecksumVerdict::Valid);
        } else {
            panic!("expected Book message");
        }
    }

    #[test]
    fn test_checksum_book_update_and_delete() {
        let mut book = KrakenChecksumBook::new(1000);

        // Apply snapshot
        let snap_msg: RawMessage<'_> =
            serde_json::from_str(SNAPSHOT_JSON).expect("deserialize snapshot");
        if let RawMessage::Book { data, .. } = snap_msg {
            book.apply(&data[0].asks, &data[0].bids)
                .expect("apply snapshot");
        }

        // Apply update
        let upd_msg: RawMessage<'_> =
            serde_json::from_str(UPDATE_JSON).expect("deserialize update");
        if let RawMessage::Book { data, .. } = upd_msg {
            book.apply(&data[0].asks, &data[0].bids)
                .expect("apply update");
            assert_eq!(book.checksum(), 38355977);
            assert_eq!(book.verify(data[0].checksum), ChecksumVerdict::Valid);
        }

        // Apply delete level
        let del_msg: RawMessage<'_> =
            serde_json::from_str(DELETE_JSON).expect("deserialize delete");
        if let RawMessage::Book { data, .. } = del_msg {
            book.apply(&data[0].asks, &data[0].bids)
                .expect("apply delete");
            assert_eq!(book.checksum(), 1166728830);
            assert_eq!(book.verify(data[0].checksum), ChecksumVerdict::Valid);
        }
    }

    #[test]
    fn test_checksum_book_depth_truncation() {
        let mut book = KrakenChecksumBook::new(2);

        let snap_msg: RawMessage<'_> =
            serde_json::from_str(SNAPSHOT_JSON).expect("deserialize snapshot");
        if let RawMessage::Book { data, .. } = snap_msg {
            book.apply(&data[0].asks, &data[0].bids)
                .expect("apply snapshot");
            assert_eq!(book.asks_len(), 2);
            assert_eq!(book.bids_len(), 2);

            let top_asks = book.top(Side::Ask, 5);
            assert_eq!(top_asks.len(), 2);
            assert_eq!(top_asks[0].price.to_string(), "45285.2");
            assert_eq!(top_asks[1].price.to_string(), "45286.4");

            let top_bids = book.top(Side::Bid, 5);
            assert_eq!(top_bids.len(), 2);
            assert_eq!(top_bids[0].price.to_string(), "45283.5");
            assert_eq!(top_bids[1].price.to_string(), "45283.4");
        }
    }

    #[test]
    fn test_checksum_book_reproduces_all_vectors() {
        #[derive(serde::Deserialize)]
        struct VectorEntry<'a> {
            #[serde(borrow)]
            asks: Vec<crate::kraken::raw::RawLevel<'a>>,
            #[serde(borrow)]
            bids: Vec<crate::kraken::raw::RawLevel<'a>>,
            checksum: u32,
        }

        let vectors: Vec<VectorEntry<'_>> =
            serde_json::from_str(VECTORS_JSON).expect("deserialize vectors");

        for v in vectors {
            let mut book = KrakenChecksumBook::new(1000);
            book.apply(&v.asks, &v.bids).expect("apply vector");
            assert_eq!(book.checksum(), v.checksum);
            assert_eq!(book.verify(Some(v.checksum)), ChecksumVerdict::Valid);
        }
    }

    #[test]
    fn test_checksum_book_mismatch_detection() {
        let mut book = KrakenChecksumBook::new(10);
        let snap_msg: RawMessage<'_> =
            serde_json::from_str(SNAPSHOT_JSON).expect("deserialize snapshot");
        if let RawMessage::Book { data, .. } = snap_msg {
            book.apply(&data[0].asks, &data[0].bids)
                .expect("apply snapshot");
            let wrong_checksum = 12345678;
            assert_eq!(
                book.verify(Some(wrong_checksum)),
                ChecksumVerdict::Mismatch {
                    expected: wrong_checksum,
                    calculated: 3310070434,
                }
            );
            assert_eq!(book.verify(None), ChecksumVerdict::NotProvided);
        }
    }

    #[test]
    fn test_checksum_policy_fail_closed_mapping() {
        use mdc_core::ChecksumPolicy;

        let verdict = ChecksumVerdict::NotProvided;

        let is_valid_required = match verdict {
            ChecksumVerdict::Valid => true,
            ChecksumVerdict::Mismatch { .. } => false,
            ChecksumVerdict::NotProvided => match ChecksumPolicy::Required {
                ChecksumPolicy::Required => false,
                ChecksumPolicy::Optional => true,
            },
        };
        assert!(
            !is_valid_required,
            "Required checksum policy must fail closed on missing checksum"
        );

        let is_valid_optional = match verdict {
            ChecksumVerdict::Valid => true,
            ChecksumVerdict::Mismatch { .. } => false,
            ChecksumVerdict::NotProvided => match ChecksumPolicy::Optional {
                ChecksumPolicy::Required => false,
                ChecksumPolicy::Optional => true,
            },
        };
        assert!(
            is_valid_optional,
            "Optional checksum policy accepts missing checksum"
        );
    }
}
