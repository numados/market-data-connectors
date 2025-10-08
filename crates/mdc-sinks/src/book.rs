//! In-memory Level 2 order book with validation and sorted price level tracking.

use mdc_core::{L2Delta, L2Snapshot, PriceLevel};
use rust_decimal::Decimal;

/// Errors when validating or applying updates to an [`L2Book`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BookError {
    /// Non-positive price level encountered.
    #[error("non-positive price level: {0}")]
    NonPositivePrice(Decimal),
    /// Negative volume/quantity encountered.
    #[error("negative volume: {0}")]
    NegativeVolume(Decimal),
    /// Crossed book (highest bid >= lowest ask).
    #[error("crossed book: best bid {best_bid} >= best ask {best_ask}")]
    CrossedBook {
        /// Highest bid price.
        best_bid: Decimal,
        /// Lowest ask price.
        best_ask: Decimal,
    },
    /// Order book depth exceeded the configured limit.
    #[error("order book depth {depth} exceeds max depth {max_depth}")]
    MaxDepthExceeded {
        /// Actual depth after update.
        depth: usize,
        /// Configured limit.
        max_depth: usize,
    },
}

/// An in-memory Level 2 order book for a single symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Book {
    /// Bids sorted in descending order by price (highest bid first).
    pub(crate) bids: Vec<PriceLevel>,
    /// Asks sorted in ascending order by price (lowest ask first).
    pub(crate) asks: Vec<PriceLevel>,
    /// Maximum allowed depth on either side.
    pub(crate) max_depth: Option<usize>,
    /// Last update identifier, if tracked.
    pub(crate) last_update_id: Option<u64>,
}

impl L2Book {
    /// Creates a new empty [`L2Book`] with an optional maximum depth cap.
    pub fn new(max_depth: Option<usize>) -> Self {
        Self {
            bids: Vec::new(),
            asks: Vec::new(),
            max_depth,
            last_update_id: None,
        }
    }

    /// Returns a slice of bids ordered descending by price.
    pub fn bids(&self) -> &[PriceLevel] {
        &self.bids
    }

    /// Returns a slice of asks ordered ascending by price.
    pub fn asks(&self) -> &[PriceLevel] {
        &self.asks
    }

    /// Returns the highest bid level if present.
    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids.first().copied()
    }

    /// Returns the lowest ask level if present.
    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks.first().copied()
    }

    /// Returns `true` if both sides of the book are empty.
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Clears all levels from the order book.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_update_id = None;
    }

    /// Replaces the entire book with a new authoritative [`L2Snapshot`].
    pub fn apply_snapshot(&mut self, snapshot: &L2Snapshot) -> Result<(), BookError> {
        let mut new_bids = Vec::with_capacity(snapshot.bids.len());
        let mut new_asks = Vec::with_capacity(snapshot.asks.len());

        for level in &snapshot.bids {
            validate_level(level)?;
            if level.volume > Decimal::ZERO {
                new_bids.push(*level);
            }
        }

        for level in &snapshot.asks {
            validate_level(level)?;
            if level.volume > Decimal::ZERO {
                new_asks.push(*level);
            }
        }

        // Bids: descending by price
        new_bids.sort_by_key(|a| std::cmp::Reverse(a.price));
        new_bids.dedup_by(|a, b| a.price == b.price);

        // Asks: ascending by price
        new_asks.sort_by_key(|a| a.price);
        new_asks.dedup_by(|a, b| a.price == b.price);

        if let Some(max_depth) = self.max_depth {
            if new_bids.len() > max_depth {
                return Err(BookError::MaxDepthExceeded {
                    depth: new_bids.len(),
                    max_depth,
                });
            }
            if new_asks.len() > max_depth {
                return Err(BookError::MaxDepthExceeded {
                    depth: new_asks.len(),
                    max_depth,
                });
            }
        }

        if let (Some(b), Some(a)) = (new_bids.first(), new_asks.first()) {
            if b.price >= a.price {
                return Err(BookError::CrossedBook {
                    best_bid: b.price,
                    best_ask: a.price,
                });
            }
        }

        self.bids = new_bids;
        self.asks = new_asks;
        self.last_update_id = snapshot.last_update_id;

        Ok(())
    }

    /// Applies an incremental [`L2Delta`] update to the order book.
    pub fn apply_delta(&mut self, delta: &L2Delta) -> Result<(), BookError> {
        let mut new_bids = self.bids.clone();
        let mut new_asks = self.asks.clone();

        for level in &delta.bids {
            validate_level(level)?;
            apply_side_update(&mut new_bids, level, true);
        }

        for level in &delta.asks {
            validate_level(level)?;
            apply_side_update(&mut new_asks, level, false);
        }

        if let Some(max_depth) = self.max_depth {
            if new_bids.len() > max_depth {
                return Err(BookError::MaxDepthExceeded {
                    depth: new_bids.len(),
                    max_depth,
                });
            }
            if new_asks.len() > max_depth {
                return Err(BookError::MaxDepthExceeded {
                    depth: new_asks.len(),
                    max_depth,
                });
            }
        }

        if let (Some(b), Some(a)) = (new_bids.first(), new_asks.first()) {
            if b.price >= a.price {
                return Err(BookError::CrossedBook {
                    best_bid: b.price,
                    best_ask: a.price,
                });
            }
        }

        self.bids = new_bids;
        self.asks = new_asks;
        if let Some(range) = delta.sequence {
            self.last_update_id = Some(range.last);
        }

        Ok(())
    }
}

fn validate_level(level: &PriceLevel) -> Result<(), BookError> {
    if level.price <= Decimal::ZERO {
        return Err(BookError::NonPositivePrice(level.price));
    }
    if level.volume < Decimal::ZERO {
        return Err(BookError::NegativeVolume(level.volume));
    }
    Ok(())
}

fn apply_side_update(levels: &mut Vec<PriceLevel>, level: &PriceLevel, is_bid: bool) {
    if is_bid {
        // Bids sorted descending
        match levels.binary_search_by(|probe| probe.price.cmp(&level.price).reverse()) {
            Ok(idx) => {
                if level.volume == Decimal::ZERO {
                    levels.remove(idx);
                } else {
                    levels[idx].volume = level.volume;
                }
            }
            Err(idx) => {
                if level.volume > Decimal::ZERO {
                    levels.insert(idx, *level);
                }
            }
        }
    } else {
        // Asks sorted ascending
        match levels.binary_search_by(|probe| probe.price.cmp(&level.price)) {
            Ok(idx) => {
                if level.volume == Decimal::ZERO {
                    levels.remove(idx);
                } else {
                    levels[idx].volume = level.volume;
                }
            }
            Err(idx) => {
                if level.volume > Decimal::ZERO {
                    levels.insert(idx, *level);
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_and_delta() {
        let mut book = L2Book::new(Some(5));
        let snap = L2Snapshot {
            last_update_id: Some(10),
            bids: vec![
                PriceLevel::new(Decimal::from(100), Decimal::from(1)),
                PriceLevel::new(Decimal::from(99), Decimal::from(2)),
            ],
            asks: vec![
                PriceLevel::new(Decimal::from(101), Decimal::from(1)),
                PriceLevel::new(Decimal::from(102), Decimal::from(3)),
            ],
        };

        assert!(book.apply_snapshot(&snap).is_ok());
        assert_eq!(book.best_bid().unwrap().price, Decimal::from(100));
        assert_eq!(book.best_ask().unwrap().price, Decimal::from(101));

        // Apply delta: update top bid, remove top ask, insert new ask
        let delta = L2Delta {
            sequence: None,
            bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(5))],
            asks: vec![
                PriceLevel::new(Decimal::from(101), Decimal::ZERO),
                PriceLevel::new(Decimal::new(1015, 1), Decimal::from(2)), // 101.5
            ],
        };

        assert!(book.apply_delta(&delta).is_ok());
        assert_eq!(book.best_bid().unwrap().volume, Decimal::from(5));
        assert_eq!(book.best_ask().unwrap().price, Decimal::new(1015, 1));
    }

    #[test]
    fn test_crossed_book_rejected() {
        let mut book = L2Book::new(None);
        let snap = L2Snapshot {
            last_update_id: None,
            bids: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
            asks: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
        };

        let err = book.apply_snapshot(&snap).unwrap_err();
        assert!(matches!(err, BookError::CrossedBook { .. }));
    }

    #[test]
    fn test_depth_cap_enforced() {
        let mut book = L2Book::new(Some(2));
        let snap = L2Snapshot {
            last_update_id: None,
            bids: vec![
                PriceLevel::new(Decimal::from(100), Decimal::from(1)),
                PriceLevel::new(Decimal::from(99), Decimal::from(1)),
                PriceLevel::new(Decimal::from(98), Decimal::from(1)),
            ],
            asks: vec![PriceLevel::new(Decimal::from(101), Decimal::from(1))],
        };

        let err = book.apply_snapshot(&snap).unwrap_err();
        assert!(matches!(err, BookError::MaxDepthExceeded { .. }));
    }
}
