//! In-band integrity alerts and failure classifications.

use serde::{Deserialize, Serialize};

/// Integrity state transitions, delivered in-band and in order with market data events.
/// Exhaustive by design: consumers must explicitly handle every integrity transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "alert", rename_all = "snake_case")]
pub enum IntegrityAlert {
    /// Missing sequence segment; the local book must be treated as invalid until `ResyncCompleted`.
    GapDetected { expected: u64, received: u64 },
    /// CRC32 checksum mismatch against the venue-published checksum (Kraken, Bitfinex).
    ChecksumMismatch { expected: u32, calculated: u32 },
    /// Connection-level silence: no data frame for the configured window; a reconnect follows.
    SilenceTimeout { elapsed_ms: u64 },
    /// Venue heartbeat missing or heartbeat sequence gap (Coinbase).
    HeartbeatTimeout { elapsed_ms: u64 },
    /// Resynchronization started (buffering deltas, snapshot requested). Emitted once per attempt.
    ResyncStarted,
    /// Book is contiguous again from `last_update_id` onward; follows snapshot and delta replay.
    ResyncCompleted { last_update_id: Option<u64> },
    /// Resync gave up for now. Book stays invalid; a retry follows after `retry_in_ms`.
    Quarantined { retry_in_ms: u64 },
    /// The data source itself failed (polling error, invalid quote). Book stays invalid until next delivery.
    SourceError { kind: SourceErrorKind },
    /// The connection carrying this symbol is lost. Emitted once per symbol at the moment socket is dropped.
    ConnectionLost { reason: DisconnectKind },
}

/// Cause category when a data source fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SourceErrorKind {
    Transport,
    HttpStatus(u16),
    Parse,
    MissingQuote,
    InvalidQuote,
}

/// Reason a connection carrying market data was terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DisconnectKind {
    Closed,
    TransportError,
    SilenceTimeout,
    VenueRequested,
    Paused,
    Shutdown,
    Panic,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn integrity_alert_serde_roundtrip() {
        let alerts = vec![
            IntegrityAlert::GapDetected {
                expected: 10,
                received: 12,
            },
            IntegrityAlert::ChecksumMismatch {
                expected: 12345,
                calculated: 67890,
            },
            IntegrityAlert::SilenceTimeout { elapsed_ms: 5000 },
            IntegrityAlert::HeartbeatTimeout { elapsed_ms: 10000 },
            IntegrityAlert::ResyncStarted,
            IntegrityAlert::ResyncCompleted {
                last_update_id: Some(100),
            },
            IntegrityAlert::ResyncCompleted {
                last_update_id: None,
            },
            IntegrityAlert::Quarantined { retry_in_ms: 30000 },
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::HttpStatus(429),
            },
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::TransportError,
            },
        ];

        for alert in alerts {
            let json = serde_json::to_string(&alert).expect("serialize alert");
            let decoded: IntegrityAlert = serde_json::from_str(&json).expect("deserialize alert");
            assert_eq!(decoded, alert);
        }
    }
}
