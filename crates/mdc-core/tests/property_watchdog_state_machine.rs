//! L3 State-machine testing for SilenceWatchdog with virtual time.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum WatchdogOp {
    DataFrame,
    Tick(Duration),
}

fn arb_watchdog_op() -> impl Strategy<Value = WatchdogOp> {
    prop_oneof![
        Just(WatchdogOp::DataFrame),
        (100u64..2_000).prop_map(|ms| WatchdogOp::Tick(Duration::from_millis(ms))),
    ]
}

#[derive(Debug, PartialEq, Eq)]
enum ModelPhase {
    Waiting,
    Pinged,
}

struct WatchdogModel {
    ping_after: Duration,
    reconnect_after: Duration,
    last_data: Instant,
    ping_sent_at: Option<Instant>,
    phase: ModelPhase,
}

impl WatchdogModel {
    fn new(ping_after: Duration, reconnect_after: Duration, now: Instant) -> Self {
        Self {
            ping_after,
            reconnect_after,
            last_data: now,
            ping_sent_at: None,
            phase: ModelPhase::Waiting,
        }
    }

    fn on_data(&mut self, now: Instant) {
        self.last_data = now;
        self.ping_sent_at = None;
        self.phase = ModelPhase::Waiting;
    }

    fn step(&mut self, now: Instant) -> WatchdogAction {
        match self.phase {
            ModelPhase::Waiting
                if now.saturating_duration_since(self.last_data) >= self.ping_after =>
            {
                self.phase = ModelPhase::Pinged;
                self.ping_sent_at = Some(now);
                WatchdogAction::SendPing
            }
            ModelPhase::Pinged => {
                let ping_at = self.ping_sent_at.unwrap_or(self.last_data);
                if now.saturating_duration_since(ping_at) >= self.reconnect_after {
                    self.phase = ModelPhase::Waiting;
                    self.ping_sent_at = None;
                    WatchdogAction::Reconnect {
                        silent_for: now.saturating_duration_since(self.last_data),
                    }
                } else {
                    WatchdogAction::Healthy
                }
            }
            _ => WatchdogAction::Healthy,
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn property_watchdog_state_machine_matches_reference_model(
        ping_secs in 1u64..10,
        reconnect_secs in 1u64..10,
        ops in proptest::collection::vec(arb_watchdog_op(), 1..100),
    ) {
        let ping_after = Duration::from_secs(ping_secs);
        let reconnect_after = Duration::from_secs(reconnect_secs);
        let t0 = Instant::now();
        let mut now = t0;

        let mut sut = SilenceWatchdog::new(ping_after, reconnect_after, now);
        let mut model = WatchdogModel::new(ping_after, reconnect_after, now);

        let mut pings_in_episode = 0;

        for op in ops {
            match op {
                WatchdogOp::DataFrame => {
                    sut.on_data_frame(now);
                    model.on_data(now);
                    pings_in_episode = 0;
                }
                WatchdogOp::Tick(dt) => {
                    now += dt;
                    let actual = sut.poll(now);
                    let expected = model.step(now);

                    // Check exact action equivalence
                    match (actual, expected) {
                        (WatchdogAction::Healthy, WatchdogAction::Healthy) => {}
                        (WatchdogAction::SendPing, WatchdogAction::SendPing) => {
                            pings_in_episode += 1;
                            // Invariant: at most 1 ping before reconnect
                            prop_assert_eq!(pings_in_episode, 1);
                        }
                        (
                            WatchdogAction::Reconnect { silent_for: act },
                            WatchdogAction::Reconnect { silent_for: exp },
                        ) => {
                            prop_assert_eq!(act, exp);
                            // Invariant: Reconnect requires at least ping_after + reconnect_after silence
                            prop_assert!(act >= ping_after + reconnect_after);
                            pings_in_episode = 0;
                        }
                        (a, e) => {
                            panic!("Action mismatch at {now:?}: sut={a:?}, model={e:?}");
                        }
                    }
                }
            }
        }
    }
}
