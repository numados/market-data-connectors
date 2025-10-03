//! Rate-limit embargo tracker for HTTP 429 and 418 responses.

use std::time::{Duration, Instant};

/// Default embargo duration for HTTP 429 (Too Many Requests) when no `Retry-After` header is provided.
pub const EMBARGO_FALLBACK_429: Duration = Duration::from_secs(60);

/// Default embargo duration for HTTP 418 (I'm a teapot / IP Ban) when no `Retry-After` header is provided.
pub const EMBARGO_FALLBACK_418: Duration = Duration::from_secs(300);

/// Maximum safety cap on embargo duration (24 hours).
pub const EMBARGO_SAFETY_CAP: Duration = Duration::from_secs(24 * 60 * 60);

/// Status of requesting permission to issue a REST snapshot call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotPermit {
    /// REST requests are permitted immediately.
    Allowed,
    /// Requests are restricted until the given instant due to active rate-limiting.
    Embargoed {
        /// Timestamp until which snapshot requests must be suspended.
        until: Instant,
    },
}

/// Tracks venue-wide rate-limit embargo state across all connections of a venue.
///
/// Shared across connections (e.g. wrapped in `Arc<Mutex<EmbargoTracker>>`) because IP-level
/// rate limits affect all sockets and HTTP clients from the same origin.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EmbargoTracker {
    embargo_until: Option<Instant>,
}

impl EmbargoTracker {
    /// Creates a new, un-embargoed tracker.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            embargo_until: None,
        }
    }

    /// Returns `true` if snapshot requests are currently embargoed at `now`.
    #[must_use]
    pub fn is_embargoed(&self, now: Instant) -> bool {
        match self.embargo_until {
            Some(until) => now < until,
            None => false,
        }
    }

    /// Returns the current permit status at `now`.
    #[must_use]
    pub fn permit(&self, now: Instant) -> SnapshotPermit {
        match self.embargo_until {
            Some(until) if now < until => SnapshotPermit::Embargoed { until },
            _ => SnapshotPermit::Allowed,
        }
    }

    /// Returns the active embargo deadline, if any.
    #[must_use]
    pub fn embargo_until(&self) -> Option<Instant> {
        self.embargo_until
    }

    /// Records a rate-limiting response (typically HTTP 429 or 418).
    ///
    /// - `status`: HTTP status code (e.g. 429 or 418).
    /// - `retry_after`: Parsed duration from the `Retry-After` header, if present.
    /// - `now`: Current timestamp.
    ///
    /// The embargo deadline is extended forward only; a subsequent response with a shorter
    /// duration never shortens an existing deadline. Durations are capped at [`EMBARGO_SAFETY_CAP`].
    pub fn on_rate_limited(&mut self, status: u16, retry_after: Option<Duration>, now: Instant) {
        let fallback = if status == 418 {
            EMBARGO_FALLBACK_418
        } else {
            EMBARGO_FALLBACK_429
        };

        let wait = retry_after.unwrap_or(fallback).min(EMBARGO_SAFETY_CAP);
        let deadline = now + wait;

        self.embargo_until = Some(match self.embargo_until {
            Some(current) => current.max(deadline),
            None => deadline,
        });
    }

    /// Clears any active embargo immediately (e.g. on operator override or reset).
    pub fn clear(&mut self) {
        self.embargo_until = None;
    }
}

/// Parses an HTTP `Retry-After` header value into a [`Duration`].
///
/// Only numeric seconds format is parsed (e.g. `"120"`). Date-based formats
/// and invalid/negative numbers return `None`, falling back to standard venue defaults.
#[must_use]
pub fn parse_retry_after(header: Option<&str>) -> Option<Duration> {
    let raw = header?.trim();
    if raw.is_empty() {
        return None;
    }
    let secs = raw.parse::<u64>().ok()?;
    Some(Duration::from_secs(secs))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_retry_after_vectors() {
        assert_eq!(parse_retry_after(Some("13")), Some(Duration::from_secs(13)));
        assert_eq!(parse_retry_after(Some(" 7 ")), Some(Duration::from_secs(7)));
        assert_eq!(parse_retry_after(None), None);
        assert_eq!(parse_retry_after(Some("soon")), None);
        assert_eq!(parse_retry_after(Some("-5")), None);
        assert_eq!(parse_retry_after(Some("")), None);
        assert_eq!(
            parse_retry_after(Some("259200")),
            Some(Duration::from_secs(259200))
        );
    }

    #[test]
    fn default_state_is_allowed() {
        let tracker = EmbargoTracker::new();
        let now = Instant::now();
        assert!(!tracker.is_embargoed(now));
        assert_eq!(tracker.permit(now), SnapshotPermit::Allowed);
        assert_eq!(tracker.embargo_until(), None);
    }

    #[test]
    fn rate_limited_429_uses_fallback_when_no_header() {
        let mut tracker = EmbargoTracker::new();
        let now = Instant::now();

        tracker.on_rate_limited(429, None, now);
        let expected_until = now + EMBARGO_FALLBACK_429;

        assert!(tracker.is_embargoed(now));
        assert_eq!(
            tracker.permit(now),
            SnapshotPermit::Embargoed {
                until: expected_until
            }
        );

        // After fallback expires -> Allowed
        assert!(!tracker.is_embargoed(expected_until));
        assert_eq!(tracker.permit(expected_until), SnapshotPermit::Allowed);
    }

    #[test]
    fn rate_limited_418_uses_fallback_when_no_header() {
        let mut tracker = EmbargoTracker::new();
        let now = Instant::now();

        tracker.on_rate_limited(418, None, now);
        let expected_until = now + EMBARGO_FALLBACK_418;

        assert_eq!(tracker.embargo_until(), Some(expected_until));
        assert!(tracker.is_embargoed(now));
    }

    #[test]
    fn rate_limited_honors_retry_after_header() {
        let mut tracker = EmbargoTracker::new();
        let now = Instant::now();

        tracker.on_rate_limited(429, Some(Duration::from_secs(15)), now);
        assert_eq!(
            tracker.permit(now),
            SnapshotPermit::Embargoed {
                until: now + Duration::from_secs(15)
            }
        );
    }

    #[test]
    fn rate_limited_caps_at_safety_cap() {
        let mut tracker = EmbargoTracker::new();
        let now = Instant::now();

        // 3 days header -> capped to 24h
        tracker.on_rate_limited(429, Some(Duration::from_secs(3 * 24 * 60 * 60)), now);
        assert_eq!(tracker.embargo_until(), Some(now + EMBARGO_SAFETY_CAP));
    }

    #[test]
    fn forward_only_extension() {
        let mut tracker = EmbargoTracker::new();
        let now = Instant::now();

        // First 429: wait 100s
        tracker.on_rate_limited(429, Some(Duration::from_secs(100)), now);
        let first_deadline = now + Duration::from_secs(100);
        assert_eq!(tracker.embargo_until(), Some(first_deadline));

        // Second response 10s later with wait 10s: must NOT shorten existing deadline
        tracker.on_rate_limited(
            429,
            Some(Duration::from_secs(10)),
            now + Duration::from_secs(10),
        );
        assert_eq!(tracker.embargo_until(), Some(first_deadline));

        // Third response 10s later with wait 200s: extends deadline forward
        tracker.on_rate_limited(
            429,
            Some(Duration::from_secs(200)),
            now + Duration::from_secs(10),
        );
        assert_eq!(
            tracker.embargo_until(),
            Some(now + Duration::from_secs(210))
        );
    }

    #[test]
    fn clear_resets_embargo() {
        let mut tracker = EmbargoTracker::new();
        let now = Instant::now();

        tracker.on_rate_limited(429, None, now);
        assert!(tracker.is_embargoed(now));

        tracker.clear();
        assert!(!tracker.is_embargoed(now));
        assert_eq!(tracker.permit(now), SnapshotPermit::Allowed);
    }
}
