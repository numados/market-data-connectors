//! Dated reference midpoint rate.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Dated reference midpoint; no executed size or order book claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceRate {
    pub mid: Decimal,
}

impl ReferenceRate {
    /// Creates a new `ReferenceRate`.
    pub const fn new(mid: Decimal) -> Self {
        Self { mid }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn reference_rate_serde() {
        let rate = ReferenceRate::new(Decimal::from(105));
        let json = serde_json::to_string(&rate).expect("serialize");
        let decoded: ReferenceRate = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, rate);
    }
}
