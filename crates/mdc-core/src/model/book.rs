//! Order book delta and snapshot models.

use super::{price_level::PriceLevel, sequence::SequenceRange};
use serde::{Deserialize, Serialize};

/// Incremental order book delta. Applied on top of the most recent `L2Snapshot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L2Delta {
    /// Changed bid price levels.
    pub bids: Vec<PriceLevel>,
    /// Changed ask price levels.
    pub asks: Vec<PriceLevel>,
    /// Venue sequence information, `None` for venues without sequencing (Bitstamp, LMAX).
    pub sequence: Option<SequenceRange>,
}

impl L2Delta {
    /// Creates an `L2Delta`.
    pub fn new(
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        sequence: Option<SequenceRange>,
    ) -> Self {
        Self {
            bids,
            asks,
            sequence,
        }
    }
}

/// Authoritative full order book snapshot. Replaces any previous local book state for the symbol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L2Snapshot {
    /// Complete list of bid price levels ordered by price.
    pub bids: Vec<PriceLevel>,
    /// Complete list of ask price levels ordered by price.
    pub asks: Vec<PriceLevel>,
    /// Sequence the snapshot is synchronized to, when the venue provides one.
    pub last_update_id: Option<u64>,
}

impl L2Snapshot {
    /// Creates an `L2Snapshot`.
    pub fn new(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>, last_update_id: Option<u64>) -> Self {
        Self {
            bids,
            asks,
            last_update_id,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn delta_and_snapshot_serde_roundtrip() {
        let delta = L2Delta::new(
            vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
            vec![PriceLevel::new(Decimal::from(101), Decimal::from(2))],
            Some(SequenceRange::new(10, 11)),
        );
        let delta_json = serde_json::to_string(&delta).expect("serialize delta");
        let decoded_delta: L2Delta = serde_json::from_str(&delta_json).expect("deserialize delta");
        assert_eq!(decoded_delta, delta);

        let snapshot = L2Snapshot::new(
            vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
            vec![PriceLevel::new(Decimal::from(101), Decimal::from(20))],
            Some(1000),
        );
        let snapshot_json = serde_json::to_string(&snapshot).expect("serialize snapshot");
        let decoded_snapshot: L2Snapshot =
            serde_json::from_str(&snapshot_json).expect("deserialize snapshot");
        assert_eq!(decoded_snapshot, snapshot);
    }
}
