//! L3 Property-based tests for SequenceTracker continuity, gap detection, and overlap invariants.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use mdc_core::integrity::{SequenceCheck, SequenceTracker};
use mdc_core::model::SequenceRange;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn property_contiguous_stream_always_valid(
        start in 1u64..1_000_000,
        deltas in proptest::collection::vec(1u32..50, 1..50),
    ) {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(start);

        let mut expected_current = start;
        for delta_len in deltas {
            let first = expected_current + 1;
            let last = first + delta_len as u64 - 1;
            let range = SequenceRange::new(first, last);

            let check = tracker.check(range);
            prop_assert_eq!(check, SequenceCheck::Valid);
            prop_assert_eq!(tracker.current(), Some(last));
            expected_current = last;
        }
    }

    #[test]
    fn property_old_data_ignored_preserves_current(
        anchor in 100u64..1_000_000,
        sub in 0u64..100,
        span in 0u64..50,
    ) {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(anchor);

        let last = anchor.saturating_sub(sub);
        let first = last.saturating_sub(span);
        let range = SequenceRange::new(first, last);

        let check = tracker.check(range);
        prop_assert_eq!(check, SequenceCheck::OldDataIgnored);
        prop_assert_eq!(tracker.current(), Some(anchor));
    }

    #[test]
    fn property_gap_detected_preserves_current(
        current in 1u64..1_000_000,
        gap in 2u64..10_000,
        len in 0u64..50,
    ) {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(current);

        let first = current + gap;
        let last = first + len;
        let range = SequenceRange::new(first, last);

        let check = tracker.check(range);
        prop_assert_eq!(
            check,
            SequenceCheck::GapDetected {
                expected: current + 1,
                received: first,
            }
        );
        prop_assert_eq!(tracker.current(), Some(current));
    }

    #[test]
    fn property_overlap_allowed_advances_current(
        current in 100u64..1_000_000,
        under in 0u64..50,
        over in 1u64..50,
    ) {
        let mut tracker = SequenceTracker::new();
        tracker.reset_to(current);

        let first = current.saturating_sub(under);
        let last = current + over;
        let range = SequenceRange::new(first, last);

        let check = tracker.check(range);
        prop_assert_eq!(check, SequenceCheck::Valid);
        prop_assert_eq!(tracker.current(), Some(last));
    }

    #[test]
    fn property_reset_epoch_triggers_sequence_reset(
        anchor in 10_000u64..1_000_000,
        threshold in 1u32..5,
        start_lower in 1u64..1000,
        steps in proptest::collection::vec(1u32..20, 5..10),
    ) {
        let mut tracker = SequenceTracker::new().with_reset_threshold(threshold);
        tracker.reset_to(anchor);

        let mut curr = start_lower;
        for (idx, step) in steps.iter().enumerate() {
            let next_last = curr + *step as u64;
            let range = SequenceRange::new(curr, next_last);
            let check = tracker.check(range);

            if ((idx + 1) as u32) < threshold {
                prop_assert_eq!(check, SequenceCheck::OldDataIgnored);
                prop_assert!(!tracker.is_reset());
            } else {
                prop_assert_eq!(
                    check,
                    SequenceCheck::SequenceReset {
                        current: anchor,
                        reset_to: curr,
                    }
                );
                prop_assert!(tracker.is_reset());
            }
            curr = next_last + 1;
        }

        // Post-alert overlap does NOT re-anchor without snapshot
        let overlap = SequenceRange::new(anchor - 10, anchor + 10);
        prop_assert_eq!(
            tracker.check(overlap),
            SequenceCheck::SequenceReset {
                current: anchor,
                reset_to: anchor - 10,
            }
        );

        // Re-anchor permitted only post-snapshot reset_to
        tracker.reset_to(curr);
        prop_assert!(!tracker.is_reset());
        prop_assert_eq!(tracker.current(), Some(curr));
        prop_assert_eq!(
            tracker.check(SequenceRange::new(curr + 1, curr + 5)),
            SequenceCheck::Valid
        );
    }

    #[test]
    fn property_global_sequence_reset_epoch(
        anchor in 10_000u64..1_000_000,
        threshold in 1u32..5,
        start_lower in 1u64..1000,
        steps in proptest::collection::vec(1u32..20, 5..10),
    ) {
        use mdc_core::integrity::{GlobalSequenceCheck, GlobalSequenceTracker};

        let mut tracker = GlobalSequenceTracker::new().with_reset_threshold(threshold);
        prop_assert_eq!(tracker.check(anchor), GlobalSequenceCheck::First);
        let expected = anchor + 1;
        prop_assert_eq!(tracker.next(), Some(expected));

        let mut curr = start_lower;
        for (idx, step) in steps.iter().enumerate() {
            let next_val = curr + *step as u64;
            let check = tracker.check(next_val);

            if ((idx + 1) as u32) < threshold {
                prop_assert_eq!(check, GlobalSequenceCheck::OldDataIgnored);
                prop_assert!(!tracker.is_reset());
            } else {
                prop_assert_eq!(
                    check,
                    GlobalSequenceCheck::SequenceReset {
                        expected,
                        reset_to: next_val,
                    }
                );
                prop_assert!(tracker.is_reset());
            }
            curr = next_val;
        }

        // Post-alert higher sequence does NOT re-anchor without reset
        prop_assert_eq!(
            tracker.check(expected + 10),
            GlobalSequenceCheck::SequenceReset {
                expected,
                reset_to: expected + 10,
            }
        );

        // Tracker accepts new epoch only via reset
        tracker.reset();
        prop_assert!(!tracker.is_reset());
        prop_assert_eq!(tracker.check(curr + 1), GlobalSequenceCheck::First);
    }

    #[test]
    fn property_global_sequence_pre_anchor_and_exhaustion(
        start in 1u64..u64::MAX - 1,
    ) {
        use mdc_core::integrity::{GlobalSequenceCheck, GlobalSequenceTracker};

        let mut tracker = GlobalSequenceTracker::new();
        // Pre-anchor invariants
        prop_assert_eq!(tracker.next(), None);
        prop_assert!(!tracker.is_reset());
        prop_assert!(!tracker.is_exhausted());

        // First check anchors tracker
        prop_assert_eq!(tracker.check(start), GlobalSequenceCheck::First);
        prop_assert_eq!(tracker.next(), Some(start + 1));
        prop_assert!(!tracker.is_exhausted());

        // Test boundary exhaustion at u64::MAX
        let mut max_tracker = GlobalSequenceTracker::new();
        prop_assert_eq!(max_tracker.check(u64::MAX), GlobalSequenceCheck::First);
        prop_assert!(max_tracker.is_exhausted());
        prop_assert_eq!(max_tracker.next(), None);
        prop_assert_eq!(max_tracker.check(1), GlobalSequenceCheck::SequenceExhausted);
    }

    #[test]
    fn property_global_sequence_contiguous_and_gaps(
        start in 1u64..1_000_000,
        ops in proptest::collection::vec(0u64..10, 1..40),
    ) {
        use mdc_core::integrity::{GlobalSequenceCheck, GlobalSequenceTracker};

        let mut tracker = GlobalSequenceTracker::new().with_reset_threshold(100);
        prop_assert_eq!(tracker.check(start), GlobalSequenceCheck::First);
        let mut expected = start + 1;

        for step in ops {
            match step {
                0 => {
                    // Contiguous
                    let check = tracker.check(expected);
                    prop_assert_eq!(check, GlobalSequenceCheck::Valid);
                    expected += 1;
                    prop_assert_eq!(tracker.next(), Some(expected));
                }
                1..=5 => {
                    // Gap
                    let gapped = expected + step;
                    let check = tracker.check(gapped);
                    prop_assert_eq!(
                        check,
                        GlobalSequenceCheck::GapDetected {
                            expected,
                            received: gapped,
                        }
                    );
                    expected = gapped + 1;
                    prop_assert_eq!(tracker.next(), Some(expected));
                }
                _ => {
                    // Old data
                    if expected > 10 {
                        let old = expected - 2;
                        let check = tracker.check(old);
                        prop_assert_eq!(check, GlobalSequenceCheck::OldDataIgnored);
                        prop_assert_eq!(tracker.next(), Some(expected));
                    }
                }
            }
        }
    }
}
