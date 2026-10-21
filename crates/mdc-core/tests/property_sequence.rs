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
            } else if ((idx + 1) as u32) == threshold {
                prop_assert_eq!(
                    check,
                    SequenceCheck::SequenceReset {
                        current: anchor,
                        reset_to: curr,
                    }
                );
                prop_assert!(tracker.is_reset());
            } else {
                prop_assert_eq!(check, SequenceCheck::OldDataIgnored);
                prop_assert!(tracker.is_reset());
            }
            curr = next_last + 1;
        }

        // Post-alert overlap does NOT re-anchor without snapshot; latched as OldDataIgnored (no alert repetition)
        let overlap = SequenceRange::new(anchor - 10, anchor + 10);
        prop_assert_eq!(
            tracker.check(overlap),
            SequenceCheck::OldDataIgnored
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
            } else if ((idx + 1) as u32) == threshold {
                prop_assert_eq!(
                    check,
                    GlobalSequenceCheck::SequenceReset {
                        expected,
                        reset_to: next_val,
                    }
                );
                prop_assert!(tracker.is_reset());
            } else {
                prop_assert_eq!(check, GlobalSequenceCheck::OldDataIgnored);
                prop_assert!(tracker.is_reset());
            }
            curr = next_val;
        }

        // Post-alert higher sequence does NOT re-anchor without reset; latched as OldDataIgnored
        prop_assert_eq!(
            tracker.check(expected + 10),
            GlobalSequenceCheck::OldDataIgnored
        );

        // Tracker accepts new epoch via reset_to
        tracker.reset_to(curr);
        prop_assert!(!tracker.is_reset());
        prop_assert_eq!(tracker.next(), Some(curr + 1));
        prop_assert_eq!(tracker.check(curr + 1), GlobalSequenceCheck::Valid);
    }

    #[test]
    fn property_duplicate_storm_never_yields_false_reset(
        anchor in 1000u64..100_000,
        threshold in 2u32..5,
        candidate in 1u64..500,
        dup_count in 2usize..20,
    ) {
        let mut tracker = SequenceTracker::new().with_reset_threshold(threshold);
        tracker.reset_to(anchor);

        // Initial candidate in lower epoch
        let range = SequenceRange::new(candidate, candidate);
        let check = tracker.check(range);
        prop_assert_eq!(check, SequenceCheck::OldDataIgnored);
        prop_assert!(!tracker.is_reset());

        // Repeated duplicate retransmissions must never advance candidate count or trigger reset
        for _ in 0..dup_count {
            let dup_check = tracker.check(range);
            prop_assert_eq!(dup_check, SequenceCheck::OldDataIgnored);
            prop_assert!(!tracker.is_reset(), "Duplicate retransmission caused false sequence reset!");
        }
    }

    #[test]
    fn property_equality_edge_yields_reset_not_silent_continuity(
        anchor in 100_000u64..1_000_000,
        threshold in 2u32..5,
    ) {
        let mut tracker = SequenceTracker::new().with_reset_threshold(threshold);
        tracker.reset_to(anchor);

        // Feed threshold - 1 candidates strictly ascending
        let step = (anchor - 1) / threshold as u64;
        let mut curr = 1u64;
        for _ in 1..threshold {
            let next_last = curr + step.max(1);
            let check = tracker.check(SequenceRange::new(curr, next_last));
            prop_assert_eq!(check, SequenceCheck::OldDataIgnored);
            prop_assert!(!tracker.is_reset());
            curr = next_last;
        }

        // Final candidate reaches exactly anchor (equality edge)
        let edge_check = tracker.check(SequenceRange::new(curr + 1, anchor));
        prop_assert_eq!(
            edge_check,
            SequenceCheck::SequenceReset {
                current: anchor,
                reset_to: curr + 1,
            }
        );
        prop_assert!(tracker.is_reset());

        // Stream attempting to continue past anchor must NOT be classified as Valid
        let post_edge = tracker.check(SequenceRange::new(anchor + 1, anchor + 5));
        prop_assert_ne!(post_edge, SequenceCheck::Valid, "Equality edge permitted silent continuity!");
        prop_assert_eq!(post_edge, SequenceCheck::OldDataIgnored);
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
    #[test]
    fn property_global_retransmit_storm_never_resets(
        start in 2_000u64..100_000,
        accepted in 20u64..200,
        threshold in 2u32..5,
        picks in proptest::collection::vec(0u64..20, 2..30),
    ) {
        use mdc_core::integrity::{GlobalSequenceCheck, GlobalSequenceTracker};

        let mut tracker = GlobalSequenceTracker::new().with_reset_threshold(threshold);
        prop_assert_eq!(tracker.check(start), GlobalSequenceCheck::First);
        for n in start + 1..start + accepted {
            prop_assert_eq!(tracker.check(n), GlobalSequenceCheck::Valid);
        }
        let tail = start + accepted - 1;

        // Any run of re-delivered, already-accepted frames, in any order and
        // including strictly increasing climbs to the tail.
        let mut run: Vec<u64> = picks.iter().map(|p| tail - p.min(&(accepted - 1))).collect();
        run.sort_unstable();
        for n in run {
            prop_assert_eq!(tracker.check(n), GlobalSequenceCheck::OldDataIgnored);
            prop_assert!(!tracker.is_reset(), "retransmission caused a false reset");
        }
        prop_assert_eq!(tracker.check(tail + 1), GlobalSequenceCheck::Valid);
    }

    #[test]
    fn property_symbol_retransmit_storm_never_resets(
        anchor in 2_000u64..100_000,
        accepted in 20u64..200,
        threshold in 2u32..5,
        picks in proptest::collection::vec(0u64..20, 2..30),
    ) {
        let mut tracker = SequenceTracker::new().with_reset_threshold(threshold);
        tracker.reset_to(anchor);
        let tail = anchor + accepted;
        prop_assert_eq!(
            tracker.check(SequenceRange::new(anchor + 1, tail)),
            SequenceCheck::Valid
        );

        let mut run: Vec<u64> = picks.iter().map(|p| tail - p.min(&(accepted - 1))).collect();
        run.sort_unstable();
        for n in run {
            prop_assert_eq!(
                tracker.check(SequenceRange::new(n, n)),
                SequenceCheck::OldDataIgnored
            );
            prop_assert!(!tracker.is_reset(), "retransmission caused a false reset");
        }
    }

    #[test]
    fn property_trackers_agree_on_reset_epoch_classification(
        anchor in 10_000u64..1_000_000,
        threshold in 1u32..5,
        start_lower in 1u64..1000,
        steps in proptest::collection::vec(1u32..20, 5..10),
    ) {
        use mdc_core::integrity::{GlobalSequenceCheck, GlobalSequenceTracker};

        let mut symbol = SequenceTracker::new().with_reset_threshold(threshold);
        symbol.reset_to(anchor);
        let mut global = GlobalSequenceTracker::new().with_reset_threshold(threshold);
        prop_assert_eq!(global.check(anchor), GlobalSequenceCheck::First);

        let mut curr = start_lower;
        for step in steps {
            let n = curr + step as u64;
            let s = symbol.check(SequenceRange::new(n, n));
            let g = global.check(n);
            prop_assert_eq!(
                matches!(s, SequenceCheck::SequenceReset { .. }),
                matches!(g, GlobalSequenceCheck::SequenceReset { .. })
            );
            prop_assert_eq!(symbol.is_reset(), global.is_reset());
            curr = n;
        }
    }

}
