//! Sequence range for order book updates.

use serde::{Deserialize, Serialize};

/// Inclusive range of exchange sequence or update IDs covered by an event.
/// Venues with a single sequence number per message set `first == last`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceRange {
    pub first: u64,
    pub last: u64,
}

impl SequenceRange {
    /// Creates a sequence range from `first` to `last`.
    pub const fn new(first: u64, last: u64) -> Self {
        Self { first, last }
    }

    /// Creates a single-value sequence range (`first == last`).
    pub const fn single(seq: u64) -> Self {
        Self {
            first: seq,
            last: seq,
        }
    }

    /// Returns `true` if this range covers exactly one sequence ID.
    pub const fn is_single(&self) -> bool {
        self.first == self.last
    }

    /// Returns the number of sequence IDs in this range, assuming `last >= first`.
    pub const fn count(&self) -> u64 {
        if self.last >= self.first {
            self.last - self.first + 1
        } else {
            0
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn single_sequence() {
        let single = SequenceRange::single(42);
        assert!(single.is_single());
        assert_eq!(single.count(), 1);
        assert_eq!(single.first, 42);
        assert_eq!(single.last, 42);
    }

    #[test]
    fn range_sequence() {
        let range = SequenceRange::new(100, 105);
        assert!(!range.is_single());
        assert_eq!(range.count(), 6);
    }

    #[test]
    fn serde_roundtrip() {
        let range = SequenceRange::new(10, 20);
        let json = serde_json::to_string(&range).expect("serialize");
        let decoded: SequenceRange = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, range);
    }
}
