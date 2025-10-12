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
    /// Sequence space exhausted after `u64::MAX`; no further sequence updates are valid.
    SequenceExhausted,
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

    /// Returns `true` if the sequence tracker is exhausted after `u64::MAX`.
    pub const fn is_exhausted(&self) -> bool {
        matches!(self.last, Some(u64::MAX))
    }

    /// Classifies an incoming sequence range and advances the tracker if valid.
    ///
    /// Classification rules:
    /// 1. `current == None` -> [`SequenceCheck::NotSynchronized`]
    /// 2. `current == u64::MAX` -> [`SequenceCheck::SequenceExhausted`]
    /// 3. `range.last <= current` -> [`SequenceCheck::OldDataIgnored`]
    /// 4. `range.first > current + 1` -> [`SequenceCheck::GapDetected`]
    /// 5. Otherwise -> [`SequenceCheck::Valid`], advancing `current` to `range.last`.
    ///
    /// Rule 5 deliberately accepts overlapping ranges (`first <= current + 1 <= last`),
    /// which is required by exchange snapshot-reconciliation contracts (e.g. Binance)
    /// where the first delta after snapshot application overlaps with the snapshot's `last_update_id`.
    pub fn check(&mut self, range: SequenceRange) -> SequenceCheck {
        let Some(current) = self.last else {
            return SequenceCheck::NotSynchronized;
        };

        if current == u64::MAX {
            return SequenceCheck::SequenceExhausted;
        }

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

/// Outcome of checking a sequence number against a connection-wide sequence tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobalSequenceCheck {
    /// First sequence number received; tracker anchored at `received + 1`.
    First,
    /// Contiguous sequence number received; tracker advanced to `received + 1`.
    Valid,
    /// Sequence number is older than expected (`received < expected`); envelope dropped silently.
    OldDataIgnored,
    /// Gap detected (`received > expected`); envelopes were lost. Re-anchors tracker to `received + 1`.
    GapDetected {
        /// Expected sequence number.
        expected: u64,
        /// Actual sequence number received.
        received: u64,
    },
    /// Sequence space exhausted after `u64::MAX`; no further sequence updates are valid.
    SequenceExhausted,
}

/// Tracks connection-wide monotonic sequence numbers (e.g. Coinbase Advanced Trade).
///
/// Pure state machine with zero heap allocations and no I/O.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GlobalSequenceTracker {
    next: Option<u64>,
    exhausted: bool,
}

impl GlobalSequenceTracker {
    /// Creates an uninitialized global sequence tracker.
    pub const fn new() -> Self {
        Self {
            next: None,
            exhausted: false,
        }
    }

    /// Resets the tracker to an uninitialized state.
    pub fn reset(&mut self) {
        self.next = None;
        self.exhausted = false;
    }

    /// Returns the expected next sequence number, if initialized and not exhausted.
    pub fn next(&self) -> Option<u64> {
        self.next
    }

    /// Returns `true` if the sequence tracker is exhausted after `u64::MAX`.
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Checks an incoming sequence number and advances the tracker accordingly.
    ///
    /// Rules:
    /// - Exhausted (`exhausted == true`) -> returns [`GlobalSequenceCheck::SequenceExhausted`].
    /// - Uninitialized (`next == None`) -> returns [`GlobalSequenceCheck::First`]. If `n == u64::MAX`, transitions to exhausted; otherwise sets `next = n + 1`.
    /// - `n == next` -> returns [`GlobalSequenceCheck::Valid`]. If `n == u64::MAX`, transitions to exhausted; otherwise sets `next = n + 1`.
    /// - `n < next` -> returns [`GlobalSequenceCheck::OldDataIgnored`], leaving `next` unchanged.
    /// - `n > next` -> returns [`GlobalSequenceCheck::GapDetected`]. If `n == u64::MAX`, transitions to exhausted; otherwise sets `next = n + 1`.
    pub fn check(&mut self, n: u64) -> GlobalSequenceCheck {
        if self.exhausted {
            return GlobalSequenceCheck::SequenceExhausted;
        }

        match self.next {
            None => {
                if n == u64::MAX {
                    self.exhausted = true;
                    self.next = None;
                } else {
                    self.next = Some(n + 1);
                }
                GlobalSequenceCheck::First
            }
            Some(expected) => match n.cmp(&expected) {
                std::cmp::Ordering::Equal => {
                    if expected == u64::MAX {
                        self.exhausted = true;
                        self.next = None;
                    } else {
                        self.next = Some(expected + 1);
                    }
                    GlobalSequenceCheck::Valid
                }
                std::cmp::Ordering::Less => GlobalSequenceCheck::OldDataIgnored,
                std::cmp::Ordering::Greater => {
                    if n == u64::MAX {
                        self.exhausted = true;
                        self.next = None;
                    } else {
                        self.next = Some(n + 1);
                    }
                    GlobalSequenceCheck::GapDetected {
                        expected,
                        received: n,
                    }
                }
            },
        }
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

    #[test]
    fn global_sequence_tracker_lifecycle() {
        let mut tracker = GlobalSequenceTracker::new();
        assert_eq!(tracker.next(), None);

        // First message with sequence 0
        assert_eq!(tracker.check(0), GlobalSequenceCheck::First);
        assert_eq!(tracker.next(), Some(1));

        // Contiguous message with sequence 1
        assert_eq!(tracker.check(1), GlobalSequenceCheck::Valid);
        assert_eq!(tracker.next(), Some(2));

        // Duplicate/old message with sequence 1
        assert_eq!(tracker.check(1), GlobalSequenceCheck::OldDataIgnored);
        assert_eq!(tracker.next(), Some(2));

        // Duplicate/old message with sequence 0
        assert_eq!(tracker.check(0), GlobalSequenceCheck::OldDataIgnored);
        assert_eq!(tracker.next(), Some(2));

        // Gap message with sequence 5 (expected 2)
        assert_eq!(
            tracker.check(5),
            GlobalSequenceCheck::GapDetected {
                expected: 2,
                received: 5,
            }
        );
        // Tracker re-anchors to 5 + 1 = 6
        assert_eq!(tracker.next(), Some(6));

        // Contiguous message after re-anchor
        assert_eq!(tracker.check(6), GlobalSequenceCheck::Valid);
        assert_eq!(tracker.next(), Some(7));

        // Reset clears back to uninitialized
        tracker.reset();
        assert_eq!(tracker.next(), None);
        assert_eq!(tracker.check(42), GlobalSequenceCheck::First);
        assert_eq!(tracker.next(), Some(43));
    }

    #[test]
    fn sequence_tracker_exhaustion() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(u64::MAX - 1);
        assert!(!tracker.is_exhausted());

        // Frame advancing to u64::MAX is valid
        assert_eq!(
            tracker.check(SequenceRange::new(u64::MAX, u64::MAX)),
            SequenceCheck::Valid
        );
        assert_eq!(tracker.current(), Some(u64::MAX));
        assert!(tracker.is_exhausted());

        // Any subsequent frame classifies as SequenceExhausted, never Valid
        assert_eq!(
            tracker.check(SequenceRange::new(u64::MAX, u64::MAX)),
            SequenceCheck::SequenceExhausted
        );
        assert_eq!(
            tracker.check(SequenceRange::new(0, 1)),
            SequenceCheck::SequenceExhausted
        );

        // Reset to u64::MAX immediately triggers exhaustion on next frame
        tracker.reset_to(u64::MAX);
        assert!(tracker.is_exhausted());
        assert_eq!(
            tracker.check(SequenceRange::new(100, 100)),
            SequenceCheck::SequenceExhausted
        );
    }

    #[test]
    fn global_sequence_tracker_exhaustion() {
        let mut tracker = GlobalSequenceTracker::new();

        // Exhaustion from first frame
        assert_eq!(tracker.check(u64::MAX), GlobalSequenceCheck::First);
        assert!(tracker.is_exhausted());
        assert_eq!(tracker.next(), None);
        assert_eq!(
            tracker.check(u64::MAX),
            GlobalSequenceCheck::SequenceExhausted
        );
        assert_eq!(tracker.check(0), GlobalSequenceCheck::SequenceExhausted);

        // Reset clears exhaustion
        tracker.reset();
        assert!(!tracker.is_exhausted());
        assert_eq!(tracker.check(u64::MAX - 1), GlobalSequenceCheck::First);
        assert_eq!(tracker.next(), Some(u64::MAX));
        assert!(!tracker.is_exhausted());

        // Valid frame matching u64::MAX transitions to exhausted
        assert_eq!(tracker.check(u64::MAX), GlobalSequenceCheck::Valid);
        assert!(tracker.is_exhausted());
        assert_eq!(tracker.next(), None);

        // Further frames are exhausted
        assert_eq!(
            tracker.check(u64::MAX),
            GlobalSequenceCheck::SequenceExhausted
        );
        assert_eq!(tracker.check(1), GlobalSequenceCheck::SequenceExhausted);

        // Gap arriving at u64::MAX transitions to exhausted
        tracker.reset();
        assert_eq!(tracker.check(10), GlobalSequenceCheck::First);
        assert_eq!(
            tracker.check(u64::MAX),
            GlobalSequenceCheck::GapDetected {
                expected: 11,
                received: u64::MAX,
            }
        );
        assert!(tracker.is_exhausted());
        assert_eq!(
            tracker.check(u64::MAX),
            GlobalSequenceCheck::SequenceExhausted
        );
    }
}
