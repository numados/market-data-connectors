#![cfg(feature = "coinbase-advanced")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 3 Property tests for Coinbase Advanced Trade connection-wide sequence tracking and FSM.

use std::time::{Duration, Instant};

use mdc_core::integrity::{GlobalSequenceCheck, GlobalSequenceTracker};
use mdc_core::{IntegrityAlert, Symbol, SymbolTag};
use mdc_providers::coinbase_advanced::fsm::CoinbaseAdvancedFsm;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_tracker_contiguous_sequence(
        start_seq in 0u64..10_000,
        len in 1usize..50,
    ) {
        let mut tracker = GlobalSequenceTracker::new();

        // First message
        let first_check = tracker.check(start_seq);
        prop_assert_eq!(first_check, GlobalSequenceCheck::First);
        prop_assert_eq!(tracker.next(), Some(start_seq + 1));

        // Subsequent contiguous messages
        for i in 1..=len {
            let seq = start_seq + i as u64;
            let check = tracker.check(seq);
            prop_assert_eq!(check, GlobalSequenceCheck::Valid);
            prop_assert_eq!(tracker.next(), Some(seq + 1));
        }
    }

    #[test]
    fn property_tracker_old_and_duplicate_dropped(
        start_seq in 10u64..10_000,
        older_offset in 1u64..10,
    ) {
        let mut tracker = GlobalSequenceTracker::new();
        tracker.check(start_seq);

        let next = tracker.next().unwrap();

        // Duplicate of current
        let dup = tracker.check(start_seq);
        prop_assert_eq!(dup, GlobalSequenceCheck::OldDataIgnored);
        prop_assert_eq!(tracker.next(), Some(next));

        // Older sequence
        let old = tracker.check(start_seq.saturating_sub(older_offset));
        prop_assert_eq!(old, GlobalSequenceCheck::OldDataIgnored);
        prop_assert_eq!(tracker.next(), Some(next));
    }

    #[test]
    fn property_tracker_gap_and_reanchor(
        start_seq in 0u64..10_000,
        gap in 1u64..100,
        subsequent_len in 1usize..20,
    ) {
        let mut tracker = GlobalSequenceTracker::new();
        tracker.check(start_seq);

        let expected = start_seq + 1;
        let jumped_seq = expected + gap;

        // Gap check
        let gap_check = tracker.check(jumped_seq);
        prop_assert_eq!(
            gap_check,
            GlobalSequenceCheck::GapDetected {
                expected,
                received: jumped_seq,
            }
        );
        // Tracker must re-anchor to jumped_seq + 1
        prop_assert_eq!(tracker.next(), Some(jumped_seq + 1));

        // Subsequent contiguous sequence must now be Valid
        for i in 1..=subsequent_len {
            let seq = jumped_seq + i as u64;
            let check = tracker.check(seq);
            prop_assert_eq!(check, GlobalSequenceCheck::Valid);
            prop_assert_eq!(tracker.next(), Some(seq + 1));
        }
    }

    #[test]
    fn property_fsm_gap_invalidates_all_products(
        num_products in 1usize..10,
        start_seq in 0u64..1_000,
        gap in 2u64..50,
    ) {
        let symbols: Vec<(Symbol, SymbolTag)> = (0..num_products)
            .map(|i| (Symbol::from(format!("PROD-{i}")), i as SymbolTag))
            .collect();

        let mut fsm = CoinbaseAdvancedFsm::new(symbols.clone(), Duration::from_secs(5));

        // All products initially awaiting snapshot
        for (sym, _) in &symbols {
            prop_assert!(!fsm.is_in_sync(sym));
        }

        // Complete snapshot for each
        for (sym, _) in &symbols {
            let alert = fsm.on_snapshot(sym);
            prop_assert_eq!(
                alert,
                Some(IntegrityAlert::ResyncCompleted {
                    last_update_id: None,
                })
            );
            prop_assert!(fsm.is_in_sync(sym));
        }

        // Anchor sequence
        fsm.check_sequence(start_seq);

        // Gap injection
        let expected = start_seq + 1;
        let jumped = expected + gap;
        let now = Instant::now();
        let (should_resub, alerts) = fsm.on_gap(expected, jumped, now);

        prop_assert!(should_resub.is_immediate());
        prop_assert_eq!(alerts.len(), num_products);

        // All products must be invalidated (AwaitingSnapshot)
        for (sym, _) in &symbols {
            prop_assert!(!fsm.is_in_sync(sym));
        }

        for (sym, tag, [gap_alert, start_alert]) in alerts {
            prop_assert!(symbols.iter().any(|(s, t)| s == &sym && t == &tag));
            prop_assert_eq!(
                gap_alert,
                IntegrityAlert::GapDetected {
                    expected,
                    received: jumped,
                }
            );
            prop_assert_eq!(start_alert, IntegrityAlert::ResyncStarted);
        }
    }

    #[test]
    fn property_tracker_exhaustion(
        start_seq in (u64::MAX - 20)..=u64::MAX,
    ) {
        let mut tracker = GlobalSequenceTracker::new();
        let check = tracker.check(start_seq);
        if start_seq == u64::MAX {
            prop_assert_eq!(check, GlobalSequenceCheck::First);
            prop_assert!(tracker.is_exhausted());
            prop_assert_eq!(tracker.check(u64::MAX), GlobalSequenceCheck::SequenceExhausted);
            prop_assert_eq!(tracker.check(0), GlobalSequenceCheck::SequenceExhausted);
        } else {
            prop_assert_eq!(check, GlobalSequenceCheck::First);
            prop_assert!(!tracker.is_exhausted());

            // Advance to u64::MAX
            let mut curr = start_seq + 1;
            while curr < u64::MAX {
                prop_assert_eq!(tracker.check(curr), GlobalSequenceCheck::Valid);
                prop_assert!(!tracker.is_exhausted());
                curr += 1;
            }
            // Final sequence u64::MAX
            prop_assert_eq!(tracker.check(u64::MAX), GlobalSequenceCheck::Valid);
            prop_assert!(tracker.is_exhausted());

            // After u64::MAX, all further checks return SequenceExhausted
            prop_assert_eq!(tracker.check(u64::MAX), GlobalSequenceCheck::SequenceExhausted);
            prop_assert_eq!(tracker.check(0), GlobalSequenceCheck::SequenceExhausted);
            prop_assert_eq!(tracker.check(curr), GlobalSequenceCheck::SequenceExhausted);
        }
    }

    #[test]
    fn property_tracker_duplicate_storm_never_yields_false_reset(
        start_seq in 1000u64..100_000,
        threshold in 2u32..5,
        candidate in 1u64..500,
        dup_count in 2usize..20,
    ) {
        let mut tracker = GlobalSequenceTracker::new().with_reset_threshold(threshold);
        tracker.check(start_seq);
        let expected = start_seq + 1;
        prop_assert_eq!(tracker.next(), Some(expected));

        // First candidate in lower epoch
        let check = tracker.check(candidate);
        prop_assert_eq!(check, GlobalSequenceCheck::OldDataIgnored);
        prop_assert!(!tracker.is_reset());

        // Repeated duplicates must not trigger false reset
        for _ in 0..dup_count {
            let dup = tracker.check(candidate);
            prop_assert_eq!(dup, GlobalSequenceCheck::OldDataIgnored);
            prop_assert!(!tracker.is_reset(), "Duplicate frame caused false reset!");
        }
    }

    #[test]
    fn property_tracker_equality_edge_and_epoch_recovery(
        start_seq in 100_000u64..1_000_000,
        threshold in 2u32..5,
    ) {
        let mut tracker = GlobalSequenceTracker::new().with_reset_threshold(threshold);
        tracker.check(start_seq);
        let expected = start_seq + 1;

        // Feed threshold - 1 strictly ascending candidates in lower epoch
        let step = (start_seq - 1) / threshold as u64;
        let mut curr = 1u64;
        for _ in 1..threshold {
            let check = tracker.check(curr);
            prop_assert_eq!(check, GlobalSequenceCheck::OldDataIgnored);
            prop_assert!(!tracker.is_reset());
            curr += step.max(1);
        }

        // Final candidate reaches exactly current (start_seq < expected)
        let edge_check = tracker.check(start_seq);
        prop_assert_eq!(
            edge_check,
            GlobalSequenceCheck::SequenceReset {
                expected,
                reset_to: start_seq,
            }
        );
        prop_assert!(tracker.is_reset());

        // Silent continuity is blocked
        let post_edge = tracker.check(expected);
        prop_assert_ne!(post_edge, GlobalSequenceCheck::Valid);
        prop_assert_eq!(post_edge, GlobalSequenceCheck::OldDataIgnored);

        // Epoch recovery via reset_to
        tracker.reset_to(start_seq);
        prop_assert!(!tracker.is_reset());
        prop_assert_eq!(tracker.next(), Some(expected));
        prop_assert_eq!(tracker.check(expected), GlobalSequenceCheck::Valid);
    }
}
