//! In-band integrity alerts and failure classifications.

use serde::{Deserialize, Serialize};

/// Integrity state transitions, delivered in-band and in order with market data events.
/// Exhaustive by design: consumers must explicitly handle every integrity transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "alert", rename_all = "snake_case")]
pub enum IntegrityAlert {
    /// Missing sequence segment; the local book must be treated as invalid until `ResyncCompleted`.
    GapDetected {
        /// Expected sequence ID.
        expected: u64,
        /// Received sequence ID that did not match expected.
        received: u64,
    },
    /// CRC32 checksum mismatch against the venue-published checksum (Kraken, Bitfinex).
    ChecksumMismatch {
        /// Expected checksum published by the venue.
        expected: u32,
        /// Calculated checksum computed over the local order book.
        calculated: u32,
    },
    /// Connection-level silence: no data frame for the configured window; a reconnect follows.
    SilenceTimeout {
        /// Milliseconds elapsed without receiving any frame.
        elapsed_ms: u64,
    },
    /// Venue heartbeat missing or heartbeat sequence gap (Coinbase).
    HeartbeatTimeout {
        /// Milliseconds elapsed without receiving a venue heartbeat.
        elapsed_ms: u64,
    },
    /// Resynchronization started (buffering deltas, snapshot requested). Emitted once per attempt.
    ResyncStarted,
    /// Book is contiguous again from `last_update_id` onward; follows snapshot and delta replay.
    ResyncCompleted {
        /// Sequence ID that the resynchronized book is aligned to, if known.
        last_update_id: Option<u64>,
    },
    /// Resync gave up for now. Book stays invalid; a retry follows after `retry_in_ms`.
    Quarantined {
        /// Delay in milliseconds before attempting resync again.
        retry_in_ms: u64,
    },
    /// The data source itself failed (polling error, invalid quote). Book stays invalid until next delivery.
    SourceError {
        /// Cause category of the source error.
        kind: SourceErrorKind,
    },
    /// The connection carrying this symbol is lost. Emitted once per symbol at the moment socket is dropped.
    ConnectionLost {
        /// Reason the connection terminated.
        reason: DisconnectKind,
    },
    /// Downstream sink failed to accept an event. Emission stops and the connection is closed.
    SinkFailure,
}

/// Cause category when a data source fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SourceErrorKind {
    /// Network or I/O failure communicating with the source.
    Transport,
    /// HTTP error status code returned by the source endpoint.
    HttpStatus(u16),
    /// Failure parsing or deserializing the venue payload.
    Parse,
    /// Expected quote or book data missing from the response payload.
    MissingQuote,
    /// Quote data received violated semantic validation invariants.
    InvalidQuote,
}

/// Reason a connection carrying market data was terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DisconnectKind {
    /// Normal connection closure by peer or client.
    Closed,
    /// Underlying transport or socket error.
    TransportError,
    /// Disconnection due to silence timeout without data or heartbeats.
    SilenceTimeout,
    /// Disconnection explicitly requested or signaled by the venue.
    VenueRequested,
    /// Connection suspended or paused administratively.
    Paused,
    /// Normal connector shutdown initiated by consumer.
    Shutdown,
    /// Connection task terminated unexpectedly due to a panic.
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
            IntegrityAlert::SinkFailure,
        ];

        for alert in alerts {
            let json = serde_json::to_string(&alert).expect("serialize alert");
            let decoded: IntegrityAlert = serde_json::from_str(&json).expect("deserialize alert");
            assert_eq!(decoded, alert);
        }
    }
}
