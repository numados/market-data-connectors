//! Sequence continuity tracking for incremental order book updates.

use crate::model::SequenceRange;

/// Default number of consecutive frames in lower epoch required to classify a sequence reset.
pub const DEFAULT_RESET_THRESHOLD: u32 = 2;

/// Default retransmission window, in sequence units behind the accepted tail.
///
/// Frames at most this far behind the tail are treated as re-deliveries of
/// already-accepted data and are never evidence of a counter reset.
pub const DEFAULT_RETRANSMIT_WINDOW: u64 = 1024;

/// Reset-candidate bookkeeping shared by [`SequenceTracker`] and
/// [`GlobalSequenceTracker`].
///
/// # Reset rule
///
/// A frame at or behind the accepted tail is either a re-delivery of data that
/// was already accepted or the first sign of a venue counter reset. Only the
/// second may advance a reset candidate. A stale frame is *eligible* (may be
/// a reset candidate) when it is:
///
/// - below the epoch floor (the anchor or first value accepted in this epoch), or
/// - more than `window` units behind the tail.
///
/// Anything else lies inside the span that was already accepted and is ignored
/// without touching candidate state, so retransmission runs of any length (for
/// example frames 99 then 100 when 101 is expected) can never reach the reset
/// threshold on their own.
///
/// Eligible frames count as progress when strictly increasing; an exact
/// duplicate of the previous candidate is ignored; a decrease restarts the
/// count at one. Once a candidate run is active, a frame equal to the tail
/// (the *equality edge*: the new epoch climbing to the accepted tail) counts as
/// further progress. With no active candidate a tail frame is a plain
/// duplicate. Ambiguity is inherent when the tail is within `window` of the
/// floor; there only values below the floor can start a candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResetDetector {
    threshold: u32,
    window: u64,
    floor: Option<u64>,
    count: u32,
    last_candidate: Option<u64>,
    latched: bool,
}

impl ResetDetector {
    const fn new() -> Self {
        Self {
            threshold: DEFAULT_RESET_THRESHOLD,
            window: DEFAULT_RETRANSMIT_WINDOW,
            floor: None,
            count: 0,
            last_candidate: None,
            latched: false,
        }
    }

    const fn with_threshold(mut self, threshold: u32) -> Self {
        self.threshold = if threshold == 0 { 1 } else { threshold };
        self
    }

    const fn with_window(mut self, window: u64) -> Self {
        self.window = window;
        self
    }

    /// Drops candidate state and the reset latch, keeping the epoch floor.
    fn clear_candidates(&mut self) {
        self.count = 0;
        self.last_candidate = None;
    }

    /// Starts a new epoch whose already-accepted span begins at `floor`.
    fn anchor(&mut self, floor: u64) {
        self.floor = Some(floor);
        self.clear_candidates();
        self.latched = false;
    }

    /// Returns to the uninitialized state.
    fn clear(&mut self) {
        self.floor = None;
        self.clear_candidates();
        self.latched = false;
    }

    /// Records the first accepted value of an epoch if no floor is set yet.
    fn anchor_if_unset(&mut self, floor: u64) {
        if self.floor.is_none() {
            self.floor = Some(floor);
        }
    }

    fn is_eligible(&self, value: u64, tail: u64) -> bool {
        self.floor.is_some_and(|floor| value < floor) || value.saturating_add(self.window) < tail
    }

    fn progress(&mut self, value: u64) -> bool {
        self.count = self.count.saturating_add(1);
        self.last_candidate = Some(value);
        if self.count >= self.threshold {
            self.latched = true;
        }
        self.latched
    }

    /// Observes a frame whose end `value` is at or behind `tail`. Returns
    /// `true` when the reset threshold has just been reached.
    fn observe_stale(&mut self, value: u64, tail: u64) -> bool {
        if value == tail {
            return match self.last_candidate {
                Some(prev) if value > prev => self.progress(value),
                _ => false,
            };
        }
        if !self.is_eligible(value, tail) {
            return false;
        }
        match self.last_candidate {
            Some(prev) if value == prev => false,
            Some(prev) if value < prev => {
                self.count = 0;
                self.progress(value)
            }
            _ => self.progress(value),
        }
    }
}

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
    detector: ResetDetector,
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
            detector: ResetDetector::new(),
        }
    }

    /// Configures the consecutive monotonic frame threshold required to classify a sequence reset.
    pub const fn with_reset_threshold(mut self, threshold: u32) -> Self {
        self.detector = self.detector.with_threshold(threshold);
        self
    }

    /// Configures how far behind the tail a frame may be and still count as a re-delivery.
    pub const fn with_retransmit_window(mut self, window: u64) -> Self {
        self.detector = self.detector.with_window(window);
        self
    }

    /// Returns `true` if a sequence reset was detected and the tracker is awaiting resynchronization.
    pub const fn is_reset(&self) -> bool {
        self.detector.latched
    }

    /// Anchors the tracker to an authoritative snapshot update ID.
    pub fn reset_to(&mut self, snapshot_last_update_id: u64) {
        self.last = Some(snapshot_last_update_id);
        self.detector.anchor(snapshot_last_update_id);
    }

    /// Clears the tracker state, returning to uninitialized (`NotSynchronized`).
    pub fn clear(&mut self) {
        self.last = None;
        self.detector.clear();
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
    /// # Classification rules:
    /// 1. `current == None` -> [`SequenceCheck::NotSynchronized`]
    /// 2. `is_reset == true` -> [`SequenceCheck::OldDataIgnored`] (reset latch active until `reset_to`)
    /// 3. `current == u64::MAX` -> [`SequenceCheck::SequenceExhausted`]
    /// 4. `range.last <= current` (stale or duplicate data): classified by the shared reset rule
    ///    (see `ResetDetector`). Re-delivery of already-accepted data never advances a reset
    ///    candidate; a genuine lower-epoch run reaching the threshold -> [`SequenceCheck::SequenceReset`].
    ///    The frame itself is never applied: otherwise -> [`SequenceCheck::OldDataIgnored`].
    /// 5. `range.first > current + 1` -> [`SequenceCheck::GapDetected`]
    /// 6. Otherwise -> [`SequenceCheck::Valid`], advancing `current` to `range.last` and clearing
    ///    any reset candidate.
    ///
    /// Rule 6 deliberately accepts overlapping ranges (`first <= current + 1 <= last`),
    /// which is required by exchange snapshot-reconciliation contracts (e.g. Binance)
    /// where the first delta after snapshot application overlaps with the snapshot's `last_update_id`.
    pub fn check(&mut self, range: SequenceRange) -> SequenceCheck {
        let Some(current) = self.last else {
            return SequenceCheck::NotSynchronized;
        };

        if self.detector.latched {
            return SequenceCheck::OldDataIgnored;
        }

        if current == u64::MAX {
            return SequenceCheck::SequenceExhausted;
        }

        if range.last <= current {
            if self.detector.observe_stale(range.last, current) {
                return SequenceCheck::SequenceReset {
                    current,
                    reset_to: range.first,
                };
            }
            return SequenceCheck::OldDataIgnored;
        }

        self.detector.clear_candidates();

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
    detector: ResetDetector,
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
            detector: ResetDetector::new(),
        }
    }

    /// Configures the consecutive monotonic frame threshold required to classify a sequence reset.
    pub const fn with_reset_threshold(mut self, threshold: u32) -> Self {
        self.detector = self.detector.with_threshold(threshold);
        self
    }

    /// Configures how far behind the tail a frame may be and still count as a re-delivery.
    pub const fn with_retransmit_window(mut self, window: u64) -> Self {
        self.detector = self.detector.with_window(window);
        self
    }

    /// Returns `true` if a sequence reset was detected and the tracker is awaiting resynchronization.
    pub const fn is_reset(&self) -> bool {
        self.detector.latched
    }

    /// Resets the tracker to an uninitialized state.
    pub fn reset(&mut self) {
        self.next = None;
        self.exhausted = false;
        self.detector.clear();
    }

    /// Resets the tracker to an authoritative sequence number, clearing any reset latch
    /// and establishing a new epoch baseline.
    pub fn reset_to(&mut self, seq: u64) {
        if seq == u64::MAX {
            self.exhausted = true;
            self.next = None;
        } else {
            self.exhausted = false;
            self.next = Some(seq + 1);
        }
        self.detector.anchor(seq);
    }

    /// Returns the expected next sequence number, if initialized and not exhausted.
    pub fn next(&self) -> Option<u64> {
        self.next
    }

    /// Returns `true` if the sequence tracker is exhausted after `u64::MAX`.
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    fn advance_to(&mut self, n: u64) {
        if n == u64::MAX {
            self.exhausted = true;
            self.next = None;
        } else {
            self.next = Some(n + 1);
        }
    }

    /// Checks an incoming sequence number and advances the tracker accordingly.
    ///
    /// # Progression Rules:
    /// 1. Exhausted (`exhausted == true`) -> [`GlobalSequenceCheck::SequenceExhausted`].
    /// 2. Reset (`is_reset == true`) -> [`GlobalSequenceCheck::OldDataIgnored`] (latched until `reset` or `reset_to`).
    /// 3. Uninitialized (`next == None`) -> [`GlobalSequenceCheck::First`]. Anchors the epoch floor at `n`.
    ///    If `n == u64::MAX`, transitions to exhausted; otherwise sets `next = n + 1`.
    /// 4. `n == next` -> [`GlobalSequenceCheck::Valid`]: the in-order frame ends any lower-epoch
    ///    candidate run (a stray stale frame followed by live data is not a reset).
    /// 5. `n < next` (stale or duplicate): classified by the shared reset rule (see `ResetDetector`).
    ///    Re-delivery of already-accepted data never advances a reset candidate; a genuine
    ///    lower-epoch run reaching the threshold -> [`GlobalSequenceCheck::SequenceReset`].
    ///    The equality edge is `n == next - 1` (the accepted tail): with an active candidate run it
    ///    counts as progress, otherwise it is a duplicate. Otherwise -> [`GlobalSequenceCheck::OldDataIgnored`].
    /// 6. `n > next`: clears candidate state and returns [`GlobalSequenceCheck::GapDetected`].
    ///    If `n == u64::MAX`, transitions to exhausted; otherwise sets `next = n + 1`.
    pub fn check(&mut self, n: u64) -> GlobalSequenceCheck {
        if self.exhausted {
            return GlobalSequenceCheck::SequenceExhausted;
        }

        if self.detector.latched {
            return GlobalSequenceCheck::OldDataIgnored;
        }

        let Some(expected) = self.next else {
            self.detector.anchor_if_unset(n);
            self.advance_to(n);
            return GlobalSequenceCheck::First;
        };

        match n.cmp(&expected) {
            std::cmp::Ordering::Equal => {
                self.detector.clear_candidates();
                self.advance_to(n);
                GlobalSequenceCheck::Valid
            }
            std::cmp::Ordering::Less => {
                // `expected >= 1`, so the tail never underflows.
                if self.detector.observe_stale(n, expected - 1) {
                    GlobalSequenceCheck::SequenceReset {
                        expected,
                        reset_to: n,
                    }
                } else {
                    GlobalSequenceCheck::OldDataIgnored
                }
            }
            std::cmp::Ordering::Greater => {
                self.detector.clear_candidates();
                self.advance_to(n);
                GlobalSequenceCheck::GapDetected {
                    expected,
                    received: n,
                }
            }
        }
    }
}

/// Requirement policy governing whether sequence numbering is mandatory on venue messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SequencePolicy {
    /// Sequence numbers are strictly required on integrity-bearing channels.
    /// Messages omitting a sequence number fail closed (emit `ResyncStarted` and drop data).
    #[default]
    Required,
    /// Sequence numbers are optional.
    Optional,
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
        let mut tracker = SequenceTracker::new().with_reset_threshold(3);
        tracker.reset_to(100);

        // Entirely older (candidate 1)
        assert_eq!(
            tracker.check(SequenceRange::new(80, 90)),
            SequenceCheck::OldDataIgnored
        );
        assert_eq!(tracker.current(), Some(100));

        // Exactly equal to current (candidate 2, threshold 3 not yet reached)
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

        // Subsequent frames are latched in OldDataIgnored until snapshot reset_to (no alert repetition)
        assert_eq!(
            tracker.check(SequenceRange::new(21, 30)),
            SequenceCheck::OldDataIgnored
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
    fn sequence_tracker_duplicates_do_not_advance_candidate_count() {
        let mut tracker = SequenceTracker::new().with_reset_threshold(2);
        tracker.reset_to(1_000_000);

        // Frame 1: candidate at (1, 10)
        assert_eq!(
            tracker.check(SequenceRange::new(1, 10)),
            SequenceCheck::OldDataIgnored
        );
        assert!(!tracker.is_reset());

        // Duplicate frames at (1, 10) must NOT advance candidate count or trigger reset
        for _ in 0..10 {
            assert_eq!(
                tracker.check(SequenceRange::new(1, 10)),
                SequenceCheck::OldDataIgnored
            );
            assert!(!tracker.is_reset());
        }

        // Strict progression to (11, 20) triggers reset
        assert_eq!(
            tracker.check(SequenceRange::new(11, 20)),
            SequenceCheck::SequenceReset {
                current: 1_000_000,
                reset_to: 11,
            }
        );
        assert!(tracker.is_reset());
    }

    #[test]
    fn sequence_tracker_equality_edge_triggers_reset() {
        let mut tracker = SequenceTracker::new().with_reset_threshold(2);
        tracker.reset_to(100);

        // Frame 1 in lower epoch: candidate 1
        assert_eq!(
            tracker.check(SequenceRange::new(50, 90)),
            SequenceCheck::OldDataIgnored
        );
        assert!(!tracker.is_reset());

        // Frame 2 climbs to exactly current (100): equality edge must count as candidate
        assert_eq!(
            tracker.check(SequenceRange::new(91, 100)),
            SequenceCheck::SequenceReset {
                current: 100,
                reset_to: 91,
            }
        );
        assert!(tracker.is_reset());

        // Frame 3 trying to silently continue is dropped as latched OldDataIgnored
        assert_eq!(
            tracker.check(SequenceRange::new(101, 105)),
            SequenceCheck::OldDataIgnored
        );
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

        // Frame 2 in lower epoch with monotonic progression (2 > 1): triggers SequenceReset!
        assert_eq!(
            tracker.check(2),
            GlobalSequenceCheck::SequenceReset {
                expected: 500_002,
                reset_to: 2,
            }
        );
        assert!(tracker.is_reset());

        // Subsequent frames are latched in OldDataIgnored until reset/reset_to
        assert_eq!(tracker.check(3), GlobalSequenceCheck::OldDataIgnored);

        // Reset clears the tracker
        tracker.reset();
        assert!(!tracker.is_reset());
        assert_eq!(tracker.check(5), GlobalSequenceCheck::First);
        assert_eq!(tracker.next(), Some(6));
    }

    #[test]
    fn global_sequence_tracker_duplicates_do_not_advance_candidate_count() {
        let mut tracker = GlobalSequenceTracker::new().with_reset_threshold(2);
        assert_eq!(tracker.check(10_000), GlobalSequenceCheck::First);
        assert_eq!(tracker.next(), Some(10_001));

        // Frame 1 in lower epoch
        assert_eq!(tracker.check(5), GlobalSequenceCheck::OldDataIgnored);
        assert!(!tracker.is_reset());

        // Duplicates of candidate must NOT advance candidate count
        for _ in 0..10 {
            assert_eq!(tracker.check(5), GlobalSequenceCheck::OldDataIgnored);
            assert!(!tracker.is_reset());
        }

        // Strict progression to 6 triggers reset
        assert_eq!(
            tracker.check(6),
            GlobalSequenceCheck::SequenceReset {
                expected: 10_001,
                reset_to: 6,
            }
        );
        assert!(tracker.is_reset());
    }

    #[test]
    fn global_sequence_tracker_equality_edge_triggers_reset() {
        let mut tracker = GlobalSequenceTracker::new().with_reset_threshold(2);
        assert_eq!(tracker.check(99), GlobalSequenceCheck::First);
        assert_eq!(tracker.next(), Some(100));

        // Lower epoch candidate
        assert_eq!(tracker.check(50), GlobalSequenceCheck::OldDataIgnored);
        assert!(!tracker.is_reset());

        // Equality edge: n reaches current (99 < expected 100) and must trigger reset
        assert_eq!(
            tracker.check(99),
            GlobalSequenceCheck::SequenceReset {
                expected: 100,
                reset_to: 99,
            }
        );
        assert!(tracker.is_reset());

        // Subsequent frame 100 is latched in OldDataIgnored, preventing silent continuity
        assert_eq!(tracker.check(100), GlobalSequenceCheck::OldDataIgnored);

        // Epoch recovery via reset_to
        tracker.reset_to(99);
        assert!(!tracker.is_reset());
        assert_eq!(tracker.next(), Some(100));
        assert_eq!(tracker.check(100), GlobalSequenceCheck::Valid);
    }

    #[test]
    fn global_multi_frame_retransmission_never_resets() {
        let mut tracker = GlobalSequenceTracker::new();
        assert_eq!(tracker.check(50), GlobalSequenceCheck::First);
        for n in 51..=100 {
            assert_eq!(tracker.check(n), GlobalSequenceCheck::Valid);
        }
        assert_eq!(tracker.next(), Some(101));

        // Retransmitted run 99, 100 is strictly increasing but already accepted.
        assert_eq!(tracker.check(99), GlobalSequenceCheck::OldDataIgnored);
        assert_eq!(tracker.check(100), GlobalSequenceCheck::OldDataIgnored);
        assert!(!tracker.is_reset());
        assert_eq!(tracker.check(101), GlobalSequenceCheck::Valid);
    }

    #[test]
    fn symbol_stale_then_tail_duplicate_is_not_progression() {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(50);
        assert_eq!(
            tracker.check(SequenceRange::new(51, 100)),
            SequenceCheck::Valid
        );

        assert_eq!(
            tracker.check(SequenceRange::new(99, 99)),
            SequenceCheck::OldDataIgnored
        );
        assert_eq!(
            tracker.check(SequenceRange::new(100, 100)),
            SequenceCheck::OldDataIgnored
        );
        assert!(!tracker.is_reset());
    }

    #[test]
    fn deep_lower_epoch_run_resets_beyond_window_even_above_floor() {
        // Epoch floor is 0, so only distance from the tail separates replay from reset.
        let mut tracker = GlobalSequenceTracker::new().with_retransmit_window(10);
        assert_eq!(tracker.check(0), GlobalSequenceCheck::First);
        for n in 1..=100 {
            assert_eq!(tracker.check(n), GlobalSequenceCheck::Valid);
        }
        // Within the window: replay.
        assert_eq!(tracker.check(95), GlobalSequenceCheck::OldDataIgnored);
        assert_eq!(tracker.check(96), GlobalSequenceCheck::OldDataIgnored);
        assert!(!tracker.is_reset());
        // Beyond the window: counter restart.
        assert_eq!(tracker.check(3), GlobalSequenceCheck::OldDataIgnored);
        assert_eq!(
            tracker.check(4),
            GlobalSequenceCheck::SequenceReset {
                expected: 101,
                reset_to: 4,
            }
        );
    }

    #[test]
    fn in_order_frame_ends_a_stray_stale_candidate() {
        let mut tracker = GlobalSequenceTracker::new();
        assert_eq!(tracker.check(10_000), GlobalSequenceCheck::First);
        assert_eq!(tracker.check(5), GlobalSequenceCheck::OldDataIgnored);
        // The next live frame proves the stale one was stray, not an epoch change.
        assert_eq!(tracker.check(10_001), GlobalSequenceCheck::Valid);
        assert_eq!(tracker.check(6), GlobalSequenceCheck::OldDataIgnored);
        assert!(!tracker.is_reset());
    }

    #[test]
    fn trackers_agree_on_equality_edge() {
        // Per-symbol: tail is `current`; global: tail is `next - 1`.
        let mut symbol = SequenceTracker::new();
        symbol.reset_to(100);
        let mut global = GlobalSequenceTracker::new();
        assert_eq!(global.check(100), GlobalSequenceCheck::First);

        // Tail frame without an active candidate is a duplicate in both.
        assert_eq!(
            symbol.check(SequenceRange::new(100, 100)),
            SequenceCheck::OldDataIgnored
        );
        assert_eq!(global.check(100), GlobalSequenceCheck::OldDataIgnored);

        // With an active lower-epoch candidate it counts as progress in both.
        assert_eq!(
            symbol.check(SequenceRange::new(1, 50)),
            SequenceCheck::OldDataIgnored
        );
        assert_eq!(global.check(50), GlobalSequenceCheck::OldDataIgnored);
        assert!(matches!(
            symbol.check(SequenceRange::new(51, 100)),
            SequenceCheck::SequenceReset { .. }
        ));
        assert!(matches!(
            global.check(100),
            GlobalSequenceCheck::SequenceReset { .. }
        ));
    }
}
