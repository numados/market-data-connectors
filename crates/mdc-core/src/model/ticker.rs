//! Top of book ticker (best bid / best ask).

use super::price_level::PriceLevel;
use serde::{Deserialize, Serialize};

/// Best bid and best ask price levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookTicker {
    /// Current best bid price level.
    pub best_bid: PriceLevel,
    /// Current best ask price level.
    pub best_ask: PriceLevel,
    /// Venue update id when provided.
    pub update_id: Option<u64>,
}

impl BookTicker {
    /// Creates a new `BookTicker`.
    pub const fn new(best_bid: PriceLevel, best_ask: PriceLevel, update_id: Option<u64>) -> Self {
        Self {
            best_bid,
            best_ask,
            update_id,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn book_ticker_serde() {
        let ticker = BookTicker::new(
            PriceLevel::new(Decimal::from(100), Decimal::from(1)),
            PriceLevel::new(Decimal::from(101), Decimal::from(2)),
            Some(555),
        );

        let json = serde_json::to_string(&ticker).expect("serialize");
        let decoded: BookTicker = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, ticker);
    }
}
