//! Wall-clock timestamps in nanoseconds since Unix epoch.

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Nanoseconds since the Unix epoch (wall clock).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UnixNanos(pub u64);

impl UnixNanos {
    /// Captures the current wall-clock time as `UnixNanos`.
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Self(duration.as_nanos().min(u64::MAX as u128) as u64)
    }

    /// Creates a `UnixNanos` from seconds since the Unix epoch.
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    /// Creates a `UnixNanos` from milliseconds since the Unix epoch.
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms.saturating_mul(1_000_000))
    }

    /// Creates a `UnixNanos` from microseconds since the Unix epoch.
    pub const fn from_micros(us: u64) -> Self {
        Self(us.saturating_mul(1_000))
    }

    /// Returns the raw nanosecond timestamp as a `u64`.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Returns elapsed duration since an earlier timestamp, saturating to `Duration::ZERO` on clock skew.
    pub fn saturating_elapsed_since(self, earlier: UnixNanos) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_conversions() {
        let ts_ms = UnixNanos::from_millis(1_670_000_000_000);
        assert_eq!(ts_ms.0, 1_670_000_000_000_000_000);
        assert_eq!(ts_ms.as_nanos(), 1_670_000_000_000_000_000);

        let ts_us = UnixNanos::from_micros(1_670_000_000_000_000);
        assert_eq!(ts_us.0, 1_670_000_000_000_000_000);
    }

    #[test]
    fn saturating_elapsed_since() {
        let t1 = UnixNanos(1_000_000_000);
        let t2 = UnixNanos(1_500_000_000);

        assert_eq!(
            t2.saturating_elapsed_since(t1),
            Duration::from_nanos(500_000_000)
        );
        assert_eq!(t1.saturating_elapsed_since(t2), Duration::ZERO);
    }

    #[test]
    fn now_is_non_decreasing() {
        let t1 = UnixNanos::now();
        let t2 = UnixNanos::now();
        assert!(t2 >= t1);
    }
}
