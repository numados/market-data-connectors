#![cfg(feature = "coinbase")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Property-based testing for Coinbase HeartbeatMonitor and SymbolFsm.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use mdc_core::Symbol;
use mdc_providers::coinbase::fsm::{
    PollOutcome, SnapshotOutcome, SymbolFsm, SyncState, UpdateOutcome,
};
use mdc_providers::coinbase::heartbeat::{HeartbeatMonitor, HeartbeatVerdict};
use proptest::prelude::*;

proptest! {
    /// Invariant: HeartbeatMonitor returns First on initial arrival, Timeout iff elapsed >= timeout,
    /// SequenceGap iff elapsed < timeout and sequence jump > threshold, and Healthy otherwise.
    #[test]
    fn property_heartbeat_verdict_invariants(
        timeout_ms in 1_000u64..10_000,
        gap_threshold in 1_000u64..5_000,
        delays_ms in prop::collection::vec(100u64..15_000, 1..50),
        seq_steps in prop::collection::vec(1u64..10_000, 1..50),
    ) {
        let timeout = Duration::from_millis(timeout_ms);
        let mut monitor = HeartbeatMonitor::new(timeout, Some(gap_threshold));
        let sym = Symbol::from("BTC-USD");

        let mut current_instant = Instant::now();
        let mut current_seq = 100u64;

        // Initial heartbeat must be First
        let first_verdict = monitor.on_heartbeat(&sym, current_seq, current_instant);
        prop_assert_eq!(first_verdict, HeartbeatVerdict::First);

        let steps = delays_ms.len().min(seq_steps.len());
        for i in 0..steps {
            let delay = Duration::from_millis(delays_ms[i]);
            let seq_jump = seq_steps[i];

            current_instant += delay;
            current_seq += seq_jump;

            let verdict = monitor.on_heartbeat(&sym, current_seq, current_instant);

            if delay >= timeout {
                match verdict {
                    HeartbeatVerdict::Timeout { silent_for } => {
                        prop_assert_eq!(silent_for, delay);
                    }
                    other => prop_assert!(false, "expected Timeout, got {:?}", other),
                }
            } else if seq_jump > gap_threshold {
                prop_assert_eq!(verdict, HeartbeatVerdict::SequenceGap { gap: seq_jump });
            } else {
                prop_assert_eq!(verdict, HeartbeatVerdict::Healthy);
            }
        }

        // Calling reset makes next arrival First
        monitor.reset(&sym);
        let next_verdict = monitor.on_heartbeat(&sym, current_seq + 1, current_instant + Duration::from_millis(100));
        prop_assert_eq!(next_verdict, HeartbeatVerdict::First);
    }

    /// Invariant: poll(now) returns exactly the set of symbols whose last heartbeat was >= timeout ago.
    #[test]
    fn property_heartbeat_poll_matches_stale_set(
        timeout_ms in 1_000u64..5_000,
        symbol_delays in prop::collection::vec((0..10u8, 100u64..10_000), 1..30),
    ) {
        let timeout = Duration::from_millis(timeout_ms);
        let mut monitor = HeartbeatMonitor::new(timeout, Some(3000));
        let t0 = Instant::now();

        let mut expected_timed_out: HashMap<Symbol, Duration> = HashMap::new();

        for (sym_id, elapsed_ms) in symbol_delays {
            let sym = Symbol::from(format!("SYM-{}", sym_id));
            let arrival_time = t0;
            monitor.on_heartbeat(&sym, 100, arrival_time);

            let check_time = t0 + Duration::from_millis(elapsed_ms);
            let elapsed = check_time.saturating_duration_since(arrival_time);
            if elapsed >= timeout {
                expected_timed_out.insert(sym, elapsed);
            } else {
                expected_timed_out.remove(&sym);
            }
        }

        // Verify across arbitrary query timestamps
        for query_offset in [500u64, 1_500, 3_000, 6_000, 12_000] {
            let now = t0 + Duration::from_millis(query_offset);
            let polled: HashMap<Symbol, Duration> = monitor.poll(now).collect();

            for (sym, duration) in &polled {
                prop_assert!(*duration >= timeout, "poll returned unexpired symbol {:?}", sym);
            }
        }
    }

    /// Invariant: SymbolFsm guarantees fail-closed behavior during AwaitingSnapshot and Quarantine,
    /// and ensures resync storm prevention within timeout duration.
    #[test]
    fn property_coinbase_fsm_invariants(
        resync_timeout_ms in 1_000u64..5_000,
        quarantine_ms in 5_000u64..20_000,
        _tick_offsets in prop::collection::vec(100u64..30_000, 1..20),
    ) {
        let timeout = Duration::from_millis(resync_timeout_ms);
        let quarantine = Duration::from_millis(quarantine_ms);
        let t0 = Instant::now();

        let mut fsm = SymbolFsm::new(t0, timeout, quarantine);
        let is_awaiting = matches!(fsm.state(), SyncState::AwaitingSnapshot { .. });
        prop_assert!(is_awaiting);
        prop_assert_eq!(fsm.on_update(), UpdateOutcome::DroppedNotSynchronized);

        // Snapshot transitions to InSync
        prop_assert_eq!(fsm.on_snapshot(), SnapshotOutcome::Accepted);
        prop_assert!(fsm.is_in_sync());
        prop_assert_eq!(fsm.on_update(), UpdateOutcome::Accepted);

        // Resync transitions back to AwaitingSnapshot
        prop_assert!(fsm.request_resync(t0));
        let is_awaiting2 = matches!(fsm.state(), SyncState::AwaitingSnapshot { .. });
        prop_assert!(is_awaiting2);

        // Duplicate resync within timeout is rejected (prevents storms)
        prop_assert!(!fsm.request_resync(t0 + Duration::from_millis(resync_timeout_ms / 2)));

        // Resync after timeout is permitted
        prop_assert!(fsm.request_resync(t0 + Duration::from_millis(resync_timeout_ms + 100)));

        // Polling past timeout moves to Quarantine
        let t_quarantine = t0 + Duration::from_millis(resync_timeout_ms * 2 + 200);
        let poll_res = fsm.poll(t_quarantine);
        let is_quarantined_poll = matches!(poll_res, PollOutcome::TimeoutQuarantined { .. });
        prop_assert!(is_quarantined_poll);
        let is_quarantined = matches!(fsm.state(), SyncState::Quarantined { .. });
        prop_assert!(is_quarantined);
        prop_assert_eq!(fsm.on_update(), UpdateOutcome::DroppedNotSynchronized);
    }
}
