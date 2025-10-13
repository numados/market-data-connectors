//! Sequence continuity tracking for incremental order book updates.

use crate::model::SequenceRange;

/// Default number of consecutive frames in lower epoch required to classify a sequence reset.
pub const DEFAULT_RESET_THRESHOLD: u32 = 2;

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
    /// Mid-connection venue counter reset detected; local book is invalid and must be resynced via snapshot.
    SequenceReset {
        /// Current sequence before reset.
        current: u64,
        /// First sequence received in the reset frame.
        reset_to: u64,
    },
}

/// Tracks the latest applied sequence or update ID and validates incoming update ranges.
///
/// Designed for venues using incremental delta sequences (e.g. Binance `U`/`u`, Gate.io, Huobi).
/// Pure state machine with zero allocations and no I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceTracker {
    last: Option<u64>,
    reset_threshold: u32,
    reset_count: u32,
    last_candidate: Option<u64>,
    is_reset: bool,
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceTracker {
    /// Creates an uninitialized sequence tracker with default reset threshold.
    pub const fn new() -> Self {
        Self {
            last: None,
            reset_threshold: DEFAULT_RESET_THRESHOLD,
            reset_count: 0,
            last_candidate: None,
            is_reset: false,
        }
    }

    /// Configures the consecutive monotonic frame threshold required to classify a sequence reset.
    pub const fn with_reset_threshold(mut self, threshold: u32) -> Self {
        self.reset_threshold = if threshold == 0 { 1 } else { threshold };
        self
    }

    /// Returns `true` if a sequence reset was detected and the tracker is awaiting resynchronization.
    pub const fn is_reset(&self) -> bool {
        self.is_reset
    }

    /// Anchors the tracker to an authoritative snapshot update ID.
    pub fn reset_to(&mut self, snapshot_last_update_id: u64) {
        self.last = Some(snapshot_last_update_id);
        self.reset_count = 0;
        self.last_candidate = None;
        self.is_reset = false;
    }

    /// Clears the tracker state, returning to uninitialized (`NotSynchronized`).
    pub fn clear(&mut self) {
        self.last = None;
        self.reset_count = 0;
        self.last_candidate = None;
        self.is_reset = false;
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
    /// 2. `is_reset == true` -> [`SequenceCheck::SequenceReset`]
    /// 3. `current == u64::MAX` -> [`SequenceCheck::SequenceExhausted`]
    /// 4. `range.last <= current`:
    ///    - If monotonic progression in lower numbers reaches threshold -> [`SequenceCheck::SequenceReset`]
    ///    - Otherwise -> [`SequenceCheck::OldDataIgnored`]
    /// 5. `range.first > current + 1` -> [`SequenceCheck::GapDetected`]
    /// 6. Otherwise -> [`SequenceCheck::Valid`], advancing `current` to `range.last`.
    ///
    /// Rule 6 deliberately accepts overlapping ranges (`first <= current + 1 <= last`),
    /// which is required by exchange snapshot-reconciliation contracts (e.g. Binance)
    /// where the first delta after snapshot application overlaps with the snapshot's `last_update_id`.
    pub fn check(&mut self, range: SequenceRange) -> SequenceCheck {
        let Some(current) = self.last else {
            return SequenceCheck::NotSynchronized;
        };

        if self.is_reset {
            return SequenceCheck::SequenceReset {
                current,
                reset_to: range.first,
            };
        }

        if current == u64::MAX {
            return SequenceCheck::SequenceExhausted;
        }

        if range.last < current {
            let is_monotonic_progression = match self.last_candidate {
                Some(prev) => range.last >= prev,
                None => true,
            };

            if is_monotonic_progression {
                self.reset_count = self.reset_count.saturating_add(1);
                self.last_candidate = Some(range.last);

                if self.reset_count >= self.reset_threshold {
                    self.is_reset = true;
                    return SequenceCheck::SequenceReset {
                        current,
                        reset_to: range.first,
                    };
                }
            } else {
                self.reset_count = 1;
                self.last_candidate = Some(range.last);
            }

            return SequenceCheck::OldDataIgnored;
        }

        if range.last == current {
            return SequenceCheck::OldDataIgnored;
        }

        self.reset_count = 0;
        self.last_candidate = None;

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
    /// Mid-connection sequence counter reset detected; local book is invalid and must be resynced via snapshot.
    SequenceReset {
        /// Expected sequence number before reset.
        expected: u64,
        /// Sequence received in the reset frame.
        reset_to: u64,
    },
}

/// Tracks connection-wide monotonic sequence numbers (e.g. Coinbase Advanced Trade).
///
/// Pure state machine with zero heap allocations and no I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalSequenceTracker {
    next: Option<u64>,
    exhausted: bool,
    reset_threshold: u32,
    reset_count: u32,
    last_candidate: Option<u64>,
    is_reset: bool,
}

impl Default for GlobalSequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalSequenceTracker {
    /// Creates an uninitialized global sequence tracker with default reset threshold.
    pub const fn new() -> Self {
        Self {
            next: None,
            exhausted: false,
            reset_threshold: DEFAULT_RESET_THRESHOLD,
            reset_count: 0,
            last_candidate: None,
            is_reset: false,
        }
    }

    /// Configures the consecutive monotonic frame threshold required to classify a sequence reset.
    pub const fn with_reset_threshold(mut self, threshold: u32) -> Self {
        self.reset_threshold = if threshold == 0 { 1 } else { threshold };
        self
    }

    /// Returns `true` if a sequence reset was detected and the tracker is awaiting resynchronization.
    pub const fn is_reset(&self) -> bool {
        self.is_reset
    }

    /// Resets the tracker to an uninitialized state.
    pub fn reset(&mut self) {
        self.next = None;
        self.exhausted = false;
        self.reset_count = 0;
        self.last_candidate = None;
        self.is_reset = false;
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
    /// - Reset (`is_reset == true`) -> returns [`GlobalSequenceCheck::SequenceReset`].
    /// - Uninitialized (`next == None`) -> returns [`GlobalSequenceCheck::First`]. If `n == u64::MAX`, transitions to exhausted; otherwise sets `next = n + 1`.
    /// - `n == next` -> returns [`GlobalSequenceCheck::Valid`]. If `n == u64::MAX`, transitions to exhausted; otherwise sets `next = n + 1`.
    /// - `n < next`:
    ///   - If monotonic progression in lower numbers reaches threshold -> returns [`GlobalSequenceCheck::SequenceReset`].
    ///   - Otherwise -> returns [`GlobalSequenceCheck::OldDataIgnored`], leaving `next` unchanged.
    /// - `n > next` -> returns [`GlobalSequenceCheck::GapDetected`]. If `n == u64::MAX`, transitions to exhausted; otherwise sets `next = n + 1`.
    pub fn check(&mut self, n: u64) -> GlobalSequenceCheck {
        if self.exhausted {
            return GlobalSequenceCheck::SequenceExhausted;
        }

        if self.is_reset {
            return GlobalSequenceCheck::SequenceReset {
                expected: self.next.unwrap_or(0),
                reset_to: n,
            };
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
                    self.reset_count = 0;
                    self.last_candidate = None;
                    if expected == u64::MAX {
                        self.exhausted = true;
                        self.next = None;
                    } else {
                        self.next = Some(expected + 1);
                    }
                    GlobalSequenceCheck::Valid
                }
                std::cmp::Ordering::Less => {
                    let is_monotonic_progression = match self.last_candidate {
                        Some(prev) => n >= prev,
                        None => true,
                    };

                    if is_monotonic_progression {
                        self.reset_count = self.reset_count.saturating_add(1);
                        self.last_candidate = Some(n);

                        if self.reset_count >= self.reset_threshold {
                            self.is_reset = true;
                            return GlobalSequenceCheck::SequenceReset {
                                expected,
                                reset_to: n,
                            };
                        }
                    } else {
                        self.reset_count = 1;
                        self.last_candidate = Some(n);
                    }

                    GlobalSequenceCheck::OldDataIgnored
                }
                std::cmp::Ordering::Greater => {
                    self.reset_count = 0;
                    self.last_candidate = None;
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

/// Strategy for validating sequence ranges and managing sequence state.
pub trait SequenceRule: Send + 'static {
    /// Classifies an incoming sequence range against the current tracker state.
    fn check(&mut self, range: SequenceRange) -> SequenceCheck;
    /// Anchors the rule to an authoritative snapshot update ID.
    fn reset_to(&mut self, anchor: u64);
    /// Clears the rule state back to uninitialized.
    fn clear(&mut self);
    /// Returns the current last applied sequence ID, if initialized.
    fn current(&self) -> Option<u64>;
}

impl SequenceRule for SequenceTracker {
    fn check(&mut self, range: SequenceRange) -> SequenceCheck {
        self.check(range)
    }

    fn reset_to(&mut self, anchor: u64) {
        self.reset_to(anchor);
    }

    fn clear(&mut self) {
        self.clear();
    }

    fn current(&self) -> Option<u64> {
        self.current()
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

    #[test]
    fn sequence_tracker_reset_detection_within_threshold() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(1_000_000);

        // Frame 1 in reset epoch (lower counter): first candidate
        assert_eq!(
            tracker.check(SequenceRange::new(1, 10)),
            SequenceCheck::OldDataIgnored
        );
        assert!(!tracker.is_reset());

        // Frame 2 in reset epoch with monotonic progression (11 >= 10): threshold reached!
        assert_eq!(
            tracker.check(SequenceRange::new(11, 20)),
            SequenceCheck::SequenceReset {
                current: 1_000_000,
                reset_to: 11,
            }
        );
        assert!(tracker.is_reset());

        // Subsequent frames are locked in SequenceReset until snapshot reset_to
        assert_eq!(
            tracker.check(SequenceRange::new(21, 30)),
            SequenceCheck::SequenceReset {
                current: 1_000_000,
                reset_to: 21,
            }
        );

        // Fresh snapshot anchors the new epoch
        tracker.reset_to(25);
        assert!(!tracker.is_reset());
        assert_eq!(tracker.current(), Some(25));
        assert_eq!(
            tracker.check(SequenceRange::new(26, 30)),
            SequenceCheck::Valid
        );
        assert_eq!(tracker.current(), Some(30));
    }

    #[test]
    fn sequence_tracker_non_monotonic_old_data_does_not_trigger_reset() {
        let mut tracker = SequenceTracker::new().with_reset_threshold(3);
        tracker.reset_to(1_000_000);

        // Frame 1: candidate at 50
        assert_eq!(
            tracker.check(SequenceRange::new(50, 50)),
            SequenceCheck::OldDataIgnored
        );
        // Frame 2: regressive older frame at 20 (not monotonic)
        assert_eq!(
            tracker.check(SequenceRange::new(20, 20)),
            SequenceCheck::OldDataIgnored
        );
        assert!(!tracker.is_reset());
    }

    #[test]
    fn global_sequence_tracker_reset_detection_within_threshold() {
        let mut tracker = GlobalSequenceTracker::new();
        assert_eq!(tracker.check(500_000), GlobalSequenceCheck::First);
        assert_eq!(tracker.check(500_001), GlobalSequenceCheck::Valid);
        assert_eq!(tracker.next(), Some(500_002));

        // Frame 1 in lower epoch
        assert_eq!(tracker.check(1), GlobalSequenceCheck::OldDataIgnored);
        assert!(!tracker.is_reset());

        // Frame 2 in lower epoch with monotonic progression (2 >= 1): triggers SequenceReset!
        assert_eq!(
            tracker.check(2),
            GlobalSequenceCheck::SequenceReset {
                expected: 500_002,
                reset_to: 2,
            }
        );
        assert!(tracker.is_reset());

        // Subsequent frames remain locked
        assert_eq!(
            tracker.check(3),
            GlobalSequenceCheck::SequenceReset {
                expected: 500_002,
                reset_to: 3,
            }
        );

        // Reset clears the tracker
        tracker.reset();
        assert!(!tracker.is_reset());
        assert_eq!(tracker.check(5), GlobalSequenceCheck::First);
        assert_eq!(tracker.next(), Some(6));
    }
}
