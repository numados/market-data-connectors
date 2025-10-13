//! Order book side.

use serde::{Deserialize, Serialize};

/// Order book side. Exhaustive: consumers must handle every side variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// Buy side of the order book.
    Bid,
    /// Sell side of the order book.
    Ask,
}

impl Side {
    /// Returns the opposite side.
    pub const fn opposite(self) -> Self {
        match self {
            Self::Bid => Self::Ask,
            Self::Ask => Self::Bid,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip() {
        assert_eq!(
            serde_json::to_string(&Side::Bid).expect("serialize"),
            "\"bid\""
        );
        assert_eq!(
            serde_json::to_string(&Side::Ask).expect("serialize"),
            "\"ask\""
        );

        assert_eq!(
            serde_json::from_str::<Side>("\"bid\"").expect("deserialize"),
            Side::Bid
        );
        assert_eq!(
            serde_json::from_str::<Side>("\"ask\"").expect("deserialize"),
            Side::Ask
        );
    }

    #[test]
    fn opposite() {
        assert_eq!(Side::Bid.opposite(), Side::Ask);
        assert_eq!(Side::Ask.opposite(), Side::Bid);
    }
}
