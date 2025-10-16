//! L3 State-machine testing for SilenceWatchdog with virtual time.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::{Duration, Instant};

use mdc_core::integrity::{SilenceWatchdog, WatchdogAction};
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum WatchdogOp {
    DataFrame,
    Tick(Duration),
    TickBackwards(Duration),
}

fn arb_watchdog_op() -> impl Strategy<Value = WatchdogOp> {
    prop_oneof![
        Just(WatchdogOp::DataFrame),
        // Normal ticks including zero duration
        (0u64..2_000).prop_map(|ms| WatchdogOp::Tick(Duration::from_millis(ms))),
        // Non-monotonic backward ticks simulating clock skew or NTP backward adjustments
        (0u64..500).prop_map(|ms| WatchdogOp::TickBackwards(Duration::from_millis(ms))),
    ]
}

/// Independent interval-arithmetic oracle for watchdog verification.
/// Derives expected actions purely from timestamp delta comparisons rather than a mirrored FSM.
struct IntervalArithmeticOracle {
    ping_after: Duration,
    reconnect_after: Duration,
    last_data_at: Instant,
    ping_at: Option<Instant>,
}

impl IntervalArithmeticOracle {
    fn new(ping_after: Duration, reconnect_after: Duration, now: Instant) -> Self {
        Self {
            ping_after,
            reconnect_after,
            last_data_at: now,
            ping_at: None,
        }
    }

    fn on_data(&mut self, now: Instant) {
        self.last_data_at = now;
        self.ping_at = None;
    }

    fn evaluate(&mut self, now: Instant) -> WatchdogAction {
        if let Some(ping_ts) = self.ping_at {
            let elapsed_since_ping = now.saturating_duration_since(ping_ts);
            if elapsed_since_ping >= self.reconnect_after {
                self.ping_at = None;
                WatchdogAction::Reconnect {
                    silent_for: now.saturating_duration_since(self.last_data_at),
                }
            } else {
                WatchdogAction::Healthy
            }
        } else {
            let elapsed_silence = now.saturating_duration_since(self.last_data_at);
            if elapsed_silence >= self.ping_after {
                self.ping_at = Some(now);
                WatchdogAction::SendPing
            } else {
                WatchdogAction::Healthy
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn property_watchdog_state_machine_matches_reference_model(
        ping_secs in 0u64..10,
        reconnect_secs in 0u64..10,
        ops in proptest::collection::vec(arb_watchdog_op(), 1..100),
    ) {
        let ping_after = Duration::from_secs(ping_secs);
        let reconnect_after = Duration::from_secs(reconnect_secs);
        let t0 = Instant::now();
        let mut now = t0;

        let mut sut = SilenceWatchdog::new(ping_after, reconnect_after, now);
        let mut oracle = IntervalArithmeticOracle::new(ping_after, reconnect_after, now);

        let mut pings_in_episode = 0;

        for op in ops {
            match op {
                WatchdogOp::DataFrame => {
                    sut.on_data_frame(now);
                    oracle.on_data(now);
                    pings_in_episode = 0;
                }
                WatchdogOp::Tick(dt) => {
                    now += dt;
                    let actual = sut.poll(now);
                    let expected = oracle.evaluate(now);

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
                            pings_in_episode = 0;
                        }
                        (a, e) => {
                            panic!("Action mismatch at {now:?}: sut={a:?}, oracle={e:?}");
                        }
                    }
                }
                WatchdogOp::TickBackwards(dt) => {
                    // Non-monotonic clock step (saturating backward step to prevent underflow before t0)
                    now = now.checked_sub(dt).unwrap_or(t0);
                    let actual = sut.poll(now);
                    let expected = oracle.evaluate(now);

                    match (actual, expected) {
                        (WatchdogAction::Healthy, WatchdogAction::Healthy) => {}
                        (WatchdogAction::SendPing, WatchdogAction::SendPing) => {
                            pings_in_episode += 1;
                            prop_assert_eq!(pings_in_episode, 1);
                        }
                        (
                            WatchdogAction::Reconnect { silent_for: act },
                            WatchdogAction::Reconnect { silent_for: exp },
                        ) => {
                            prop_assert_eq!(act, exp);
                            pings_in_episode = 0;
                        }
                        (a, e) => {
                            panic!("Action mismatch on non-monotonic jump at {now:?}: sut={a:?}, oracle={e:?}");
                        }
                    }
                }
            }
        }
    }
}
