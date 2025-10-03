//! L3 Property-based tests for EmbargoTracker rate-limit monotonicity and safety caps.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use mdc_core::integrity::{EMBARGO_SAFETY_CAP, EmbargoTracker, SnapshotPermit};
use proptest::prelude::*;

#[derive(Debug, Clone)]
struct RateLimitEvent {
    status: u16,
    offset_secs: u64,
    retry_after: Option<u64>,
}

fn arb_rate_limit_event() -> impl Strategy<Value = RateLimitEvent> {
    (
        prop_oneof![Just(429u16), Just(418u16), Just(500u16)],
        0u64..10_000,
        proptest::option::of(0u64..200_000),
    )
        .prop_map(|(status, offset_secs, retry_after)| RateLimitEvent {
            status,
            offset_secs,
            retry_after,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn property_embargo_deadline_monotonicity(
        events in proptest::collection::vec(arb_rate_limit_event(), 1..30),
    ) {
        let t0 = Instant::now();
        let mut tracker = EmbargoTracker::new();
        let mut prev_deadline = t0;

        for ev in events {
            let event_time = t0 + Duration::from_secs(ev.offset_secs);
            let retry = ev.retry_after.map(Duration::from_secs);

            tracker.on_rate_limited(ev.status, retry, event_time);
            let current_deadline = tracker.embargo_until().expect("deadline must be present");

            // Monotonic: deadline must never regress backwards
            prop_assert!(current_deadline >= prev_deadline);

            // Safety cap: deadline must not exceed event_time + EMBARGO_SAFETY_CAP
            let max_possible = event_time + EMBARGO_SAFETY_CAP;
            if current_deadline > max_possible {
                // If it exceeded max_possible, it must be because an earlier event set a later deadline
                prop_assert!(prev_deadline > event_time);
            }

            prev_deadline = current_deadline;
        }
    }

    #[test]
    fn property_permit_consistency(
        events in proptest::collection::vec(arb_rate_limit_event(), 1..10),
        probe_offset in 0u64..200_000,
    ) {
        let t0 = Instant::now();
        let mut tracker = EmbargoTracker::new();

        for ev in events {
            let event_time = t0 + Duration::from_secs(ev.offset_secs);
            let retry = ev.retry_after.map(Duration::from_secs);
            tracker.on_rate_limited(ev.status, retry, event_time);
        }

        let probe_time = t0 + Duration::from_secs(probe_offset);
        let deadline = tracker.embargo_until();

        match deadline {
            Some(until) => {
                if probe_time < until {
                    prop_assert!(tracker.is_embargoed(probe_time));
                    prop_assert_eq!(
                        tracker.permit(probe_time),
                        SnapshotPermit::Embargoed { until }
                    );
                } else {
                    prop_assert!(!tracker.is_embargoed(probe_time));
                    prop_assert_eq!(tracker.permit(probe_time), SnapshotPermit::Allowed);
                }
            }
            None => {
                prop_assert!(!tracker.is_embargoed(probe_time));
                prop_assert_eq!(tracker.permit(probe_time), SnapshotPermit::Allowed);
            }
        }
    }
}
