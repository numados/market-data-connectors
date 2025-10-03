//! Connection silence watchdog detecting dead or stalled sockets.

use std::time::{Duration, Instant};

/// Action recommended by [`SilenceWatchdog::poll`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchdogAction {
    /// Connection is within acceptable latency thresholds; no action required.
    Healthy,
    /// No data frame received for `ping_after`; an application or protocol ping should be sent.
    SendPing,
    /// No data frame received after ping for `reconnect_after`; connection should be dropped and rebuilt.
    Reconnect {
        /// Total elapsed duration of silence since the last inbound data frame.
        silent_for: Duration,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Waiting,
    Pinged { at: Instant },
}

/// Detects dead or stalled connections by measuring elapsed time since the last inbound data frame.
///
/// Pure state machine with injected timestamps (`now: Instant`). Only genuine data frames
/// (or heartbeat channels treated as data by the venue) reset the watchdog. Low-level ping/pong
/// exchanges do not reset it, ensuring that quiet connections with stalled market data streams
/// are deterministically reconnected and re-synchronized.
#[derive(Debug, Clone, Copy)]
pub struct SilenceWatchdog {
    ping_after: Duration,
    reconnect_after: Duration,
    last_data_at: Instant,
    phase: Phase,
}

impl SilenceWatchdog {
    /// Creates a new silence watchdog initialized at the given `now` timestamp.
    #[must_use]
    pub const fn new(ping_after: Duration, reconnect_after: Duration, now: Instant) -> Self {
        Self {
            ping_after,
            reconnect_after,
            last_data_at: now,
            phase: Phase::Waiting,
        }
    }

    /// Returns the duration of silence after which a ping is triggered.
    #[must_use]
    pub const fn ping_after(&self) -> Duration {
        self.ping_after
    }

    /// Returns the grace duration after ping before reconnection is triggered.
    #[must_use]
    pub const fn reconnect_after(&self) -> Duration {
        self.reconnect_after
    }

    /// Returns the timestamp of the last recorded data frame.
    #[must_use]
    pub const fn last_data_at(&self) -> Instant {
        self.last_data_at
    }

    /// Notifies the watchdog that an inbound data frame (Text or Binary) has been received.
    ///
    /// Must be called for data frames before parsing. Low-level WebSocket ping/pong frames
    /// must not call this method.
    pub fn on_data_frame(&mut self, now: Instant) {
        self.last_data_at = now;
        self.phase = Phase::Waiting;
    }

    /// Evaluates the current state against `now` and returns any required watchdog action.
    ///
    /// Designed to be called periodically (e.g. every ~250 ms) on an async timer.
    pub fn poll(&mut self, now: Instant) -> WatchdogAction {
        match self.phase {
            Phase::Waiting
                if now.saturating_duration_since(self.last_data_at) >= self.ping_after =>
            {
                self.phase = Phase::Pinged { at: now };
                WatchdogAction::SendPing
            }
            Phase::Pinged { at } if now.saturating_duration_since(at) >= self.reconnect_after => {
                self.phase = Phase::Waiting;
                WatchdogAction::Reconnect {
                    silent_for: now.saturating_duration_since(self.last_data_at),
                }
            }
            _ => WatchdogAction::Healthy,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_is_healthy() {
        let t0 = Instant::now();
        let mut watchdog = SilenceWatchdog::new(Duration::from_secs(5), Duration::from_secs(5), t0);

        assert_eq!(watchdog.poll(t0), WatchdogAction::Healthy);
        assert_eq!(
            watchdog.poll(t0 + Duration::from_secs(4)),
            WatchdogAction::Healthy
        );
    }

    #[test]
    fn triggers_ping_at_ping_after() {
        let t0 = Instant::now();
        let mut watchdog = SilenceWatchdog::new(Duration::from_secs(5), Duration::from_secs(5), t0);

        // At 5s exactly -> SendPing
        assert_eq!(
            watchdog.poll(t0 + Duration::from_secs(5)),
            WatchdogAction::SendPing
        );

        // Before reconnect_after -> Healthy (already pinged, waiting for reconnect timeout)
        assert_eq!(
            watchdog.poll(t0 + Duration::from_secs(9)),
            WatchdogAction::Healthy
        );
    }

    #[test]
    fn triggers_reconnect_after_grace_period() {
        let t0 = Instant::now();
        let mut watchdog = SilenceWatchdog::new(Duration::from_secs(5), Duration::from_secs(5), t0);

        let ping_time = t0 + Duration::from_secs(5);
        assert_eq!(watchdog.poll(ping_time), WatchdogAction::SendPing);

        // At ping_time + 5s (total 10s silent) -> Reconnect
        let reconnect_time = ping_time + Duration::from_secs(5);
        assert_eq!(
            watchdog.poll(reconnect_time),
            WatchdogAction::Reconnect {
                silent_for: Duration::from_secs(10),
            }
        );
    }

    #[test]
    fn data_frame_cancels_pending_ping() {
        let t0 = Instant::now();
        let mut watchdog = SilenceWatchdog::new(Duration::from_secs(5), Duration::from_secs(5), t0);

        // Data frame at 4s resets last_data_at
        watchdog.on_data_frame(t0 + Duration::from_secs(4));

        // At 5s (1s after data frame) -> Healthy
        assert_eq!(
            watchdog.poll(t0 + Duration::from_secs(5)),
            WatchdogAction::Healthy
        );

        // At 9s (5s after data frame) -> SendPing
        assert_eq!(
            watchdog.poll(t0 + Duration::from_secs(9)),
            WatchdogAction::SendPing
        );
    }

    #[test]
    fn data_frame_cancels_pending_reconnect() {
        let t0 = Instant::now();
        let mut watchdog = SilenceWatchdog::new(Duration::from_secs(5), Duration::from_secs(5), t0);

        let ping_time = t0 + Duration::from_secs(5);
        assert_eq!(watchdog.poll(ping_time), WatchdogAction::SendPing);

        // Data frame arrives at 8s (3s after ping, before 5s reconnect timeout)
        let data_time = t0 + Duration::from_secs(8);
        watchdog.on_data_frame(data_time);

        // At 10s (2s after data frame) -> Healthy, reconnect is aborted
        assert_eq!(
            watchdog.poll(t0 + Duration::from_secs(10)),
            WatchdogAction::Healthy
        );

        // Next ping triggers at 8 + 5 = 13s
        assert_eq!(
            watchdog.poll(t0 + Duration::from_secs(13)),
            WatchdogAction::SendPing
        );
    }

    #[test]
    fn pong_frame_does_not_reset_watchdog() {
        // Architectural invariant: incoming Ping/Pong frames do NOT reset the watchdog.
        // If a venue answers TCP pongs but sends no market data, the watchdog must reconnect.
        let t0 = Instant::now();
        let mut watchdog = SilenceWatchdog::new(Duration::from_secs(5), Duration::from_secs(5), t0);

        let ping_time = t0 + Duration::from_secs(5);
        assert_eq!(watchdog.poll(ping_time), WatchdogAction::SendPing);

        // A pong frame arrives at 6s; handler deliberately does NOT call on_data_frame.
        // At 10s, watchdog correctly triggers Reconnect.
        let reconnect_time = t0 + Duration::from_secs(10);
        assert_eq!(
            watchdog.poll(reconnect_time),
            WatchdogAction::Reconnect {
                silent_for: Duration::from_secs(10),
            }
        );
    }
}
