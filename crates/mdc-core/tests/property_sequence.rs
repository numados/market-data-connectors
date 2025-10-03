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
}
