#![cfg(feature = "bitstamp")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 3 Property tests for Bitstamp microsecond timestamp gating and resync FSM.

use std::time::{Duration, Instant};

use mdc_core::Symbol;
use mdc_core::resync::ResyncFailure;
use mdc_providers::bitstamp::fsm::{DiffDecision, SymbolFsm, SyncState};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_timestamp_gate_emits_strictly_greater(
        snap_micros in 1_000_000_000_000u64..2_000_000_000_000u64,
        diff_offsets in prop::collection::vec(-10_000i64..10_000i64, 1..50),
    ) {
        let sym = Symbol::from("btcusd");
        let mut fsm = SymbolFsm::new(sym, Duration::from_secs(5));
        let now = Instant::now();

        // Feed snapshot
        fsm.on_snapshot(snap_micros, now);
        prop_assert_eq!(fsm.state(), SyncState::InSync { gate_micros: snap_micros });

        for offset in diff_offsets {
            let diff_micros = if offset < 0 {
                snap_micros.saturating_sub(offset.unsigned_abs())
            } else {
                snap_micros.saturating_add(offset as u64)
            };

            let decision = fsm.check_diff(diff_micros);
            if diff_micros > snap_micros {
                prop_assert_eq!(decision, DiffDecision::Emit);
            } else {
                prop_assert_eq!(decision, DiffDecision::Drop);
            }
        }
    }

    #[test]
    fn property_awaiting_snapshot_buffers_all_diffs(
        diff_micros in any::<u64>(),
    ) {
        let sym = Symbol::from("btcusd");
        let fsm = SymbolFsm::new(sym, Duration::from_secs(5));
        prop_assert_eq!(fsm.state(), SyncState::AwaitingSnapshot);

        prop_assert_eq!(fsm.check_diff(diff_micros), DiffDecision::Buffer);
    }

    #[test]
    fn property_resync_trigger_buffers_subsequent_diffs(
        snap_micros in 1_000_000_000_000u64..2_000_000_000_000u64,
        newer_offset in 1u64..10_000u64,
    ) {
        let sym = Symbol::from("btcusd");
        let mut fsm = SymbolFsm::new(sym, Duration::from_secs(5));
        let now = Instant::now();

        fsm.on_snapshot(snap_micros, now);
        let newer_micros = snap_micros + newer_offset;
        prop_assert_eq!(fsm.check_diff(newer_micros), DiffDecision::Emit);

        // Resync triggered
        fsm.trigger_resync(now);
        prop_assert_eq!(fsm.state(), SyncState::AwaitingSnapshot);

        // Previously emitted diff now buffered
        prop_assert_eq!(fsm.check_diff(newer_micros), DiffDecision::Buffer);
    }

    #[test]
    fn property_snapshot_failure_cooldown(
        cooldown_secs in 1u64..60u64,
        elapsed_millis in 0u64..120_000u64,
    ) {
        let sym = Symbol::from("btcusd");
        let cooldown = Duration::from_secs(cooldown_secs);
        let mut fsm = SymbolFsm::new(sym, cooldown);
        let t0 = Instant::now();

        // First attempt allowed
        prop_assert!(fsm.can_request_snapshot(t0));
        fsm.mark_snapshot_requested(t0);

        // Snapshot fails
        fsm.on_snapshot_failed(&ResyncFailure::HttpError(500), t0);

        let t_check = t0 + Duration::from_millis(elapsed_millis);
        let elapsed = Duration::from_millis(elapsed_millis);

        if elapsed < cooldown {
            prop_assert!(!fsm.can_request_snapshot(t_check));
        } else {
            prop_assert!(fsm.can_request_snapshot(t_check));
        }
    }
}
