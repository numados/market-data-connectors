//! Sequence continuity tracking for incremental order book updates.

use crate::model::SequenceRange;

/// Outcome of checking an incoming sequence range against the current tracker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceCheck {
    /// Range is contiguous or overlapping with current state; tracker advanced to `range.last`.
    Valid,
    /// Range is entirely older than or equal to current state (`range.last <= current`); drop silently.
    OldDataIgnored,
    /// Gap detected (`range.first > current + 1`); updates were lost.
    GapDetected {
        /// Expected next sequence ID (`current + 1`).
        expected: u64,
        /// Actual first sequence ID received in the range.
        received: u64,
    },
    /// No authoritative snapshot has been applied yet; data must be buffered and snapshot requested.
    NotSynchronized,
}

/// Tracks the latest applied sequence or update ID and validates incoming update ranges.
///
/// Designed for venues using incremental delta sequences (e.g. Binance `U`/`u`, Gate.io, Huobi).
/// Pure state machine with zero allocations and no I/O.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SequenceTracker {
    last: Option<u64>,
}

impl SequenceTracker {
    /// Creates an uninitialized sequence tracker.
    pub const fn new() -> Self {
        Self { last: None }
    }

    /// Anchors the tracker to an authoritative snapshot update ID.
    pub fn reset_to(&mut self, snapshot_last_update_id: u64) {
        self.last = Some(snapshot_last_update_id);
    }

    /// Clears the tracker state, returning to uninitialized (`NotSynchronized`).
    pub fn clear(&mut self) {
        self.last = None;
    }

    /// Returns the current last applied sequence ID, if initialized.
    pub fn current(&self) -> Option<u64> {
        self.last
    }

    /// Classifies an incoming sequence range and advances the tracker if valid.
    ///
    /// Classification rules:
    /// 1. `current == None` -> [`SequenceCheck::NotSynchronized`]
    /// 2. `range.last <= current` -> [`SequenceCheck::OldDataIgnored`]
    /// 3. `range.first > current + 1` -> [`SequenceCheck::GapDetected`]
    /// 4. Otherwise -> [`SequenceCheck::Valid`], advancing `current` to `range.last`.
    ///
    /// Rule 4 deliberately accepts overlapping ranges (`first <= current + 1 <= last`),
    /// which is required by exchange snapshot-reconciliation contracts (e.g. Binance)
    /// where the first delta after snapshot application overlaps with the snapshot's `last_update_id`.
    pub fn check(&mut self, range: SequenceRange) -> SequenceCheck {
        let Some(current) = self.last else {
            return SequenceCheck::NotSynchronized;
        };

        if range.last <= current {
            return SequenceCheck::OldDataIgnored;
        }

        let expected = current.saturating_add(1);
        if range.first > expected {
            return SequenceCheck::GapDetected {
                expected,
                received: range.first,
            };
        }

        self.last = Some(range.last);
        SequenceCheck::Valid
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn uninitialized_returns_not_synchronized() {
        let mut tracker = SequenceTracker::new();
        assert_eq!(tracker.current(), None);
        assert_eq!(
            tracker.check(SequenceRange::new(1, 1)),
            SequenceCheck::NotSynchronized
        );
    }

    #[test]
    fn reset_to_anchors_snapshot() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(100);
        assert_eq!(tracker.current(), Some(100));
    }

    #[test]
    fn clear_resets_to_uninitialized() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(100);
        tracker.clear();
        assert_eq!(tracker.current(), None);
        assert_eq!(
            tracker.check(SequenceRange::new(101, 101)),
            SequenceCheck::NotSynchronized
        );
    }

    #[test]
    fn contiguous_delta_is_valid_and_advances() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(100);

        assert_eq!(
            tracker.check(SequenceRange::new(101, 105)),
            SequenceCheck::Valid
        );
        assert_eq!(tracker.current(), Some(105));

        assert_eq!(
            tracker.check(SequenceRange::new(106, 106)),
            SequenceCheck::Valid
        );
        assert_eq!(tracker.current(), Some(106));
    }

    #[test]
    fn overlapping_delta_after_snapshot_is_valid() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(100);

        // Binance rule: first delta can overlap snapshot ID (e.g. [98, 102])
        assert_eq!(
            tracker.check(SequenceRange::new(98, 102)),
            SequenceCheck::Valid
        );
        assert_eq!(tracker.current(), Some(102));

        // Exact match on first (first == current)
        assert_eq!(
            tracker.check(SequenceRange::new(102, 105)),
            SequenceCheck::Valid
        );
        assert_eq!(tracker.current(), Some(105));
    }

    #[test]
    fn old_data_ignored_when_last_le_current() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(100);

        // Entirely older
        assert_eq!(
            tracker.check(SequenceRange::new(80, 90)),
            SequenceCheck::OldDataIgnored
        );
        assert_eq!(tracker.current(), Some(100));

        // Exactly equal to current
        assert_eq!(
            tracker.check(SequenceRange::new(95, 100)),
            SequenceCheck::OldDataIgnored
        );
        assert_eq!(tracker.current(), Some(100));
    }

    #[test]
    fn gap_detected_leaves_current_unchanged() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(100);

        assert_eq!(
            tracker.check(SequenceRange::new(102, 105)),
            SequenceCheck::GapDetected {
                expected: 101,
                received: 102,
            }
        );
        assert_eq!(tracker.current(), Some(100));
    }
}
