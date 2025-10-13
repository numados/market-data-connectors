//! Order book price level with fixed-point decimal arithmetic.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// A single level in an order book. `volume == 0` denotes level removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLevel {
    /// Price of the book level.
    pub price: Decimal,
    /// Quantity or volume available at this price level; zero indicates removal.
    pub volume: Decimal,
}

impl PriceLevel {
    /// Creates a new `PriceLevel`.
    pub const fn new(price: Decimal, volume: Decimal) -> Self {
        Self { price, volume }
    }

    /// Returns `true` if this level signals removal of the price level (volume is zero).
    pub fn is_remove(&self) -> bool {
        self.volume.is_zero()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn price_level_removal() {
        let active = PriceLevel::new(Decimal::from(100), Decimal::from(5));
        assert!(!active.is_remove());

        let removal = PriceLevel::new(Decimal::from(100), Decimal::ZERO);
        assert!(removal.is_remove());
    }

    #[test]
    fn decimal_precision_roundtrip() {
        let small = Decimal::from_str("0.00000001").expect("parse decimal");
        let large = Decimal::from_str("123456789.123456789").expect("parse decimal");

        let level = PriceLevel::new(small, large);
        assert_eq!(level.price.to_string(), "0.00000001");
        assert_eq!(level.volume.to_string(), "123456789.123456789");

        let json = serde_json::to_string(&level).expect("serialize");
        let decoded: PriceLevel = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, level);
    }
}
