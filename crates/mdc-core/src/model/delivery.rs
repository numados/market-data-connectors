//! Connector run identity and delivery ordering stamps.
//!
//! Delivery identity is established by a unique [`RunId`] per connector run and
//! [`DeliveryStamp`] ordering each emitted event within and across connection generations.

use serde::{Deserialize, Serialize};

/// Opaque 16-byte session token identifying a connector run lifetime.
///
/// Generated once per connector run execution (or injected by builder for tests)
/// to ensure delivery uniqueness across connector lifecycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunId(pub [u8; 16]);

impl RunId {
    /// Creates a `RunId` from raw 16 bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Creates a `RunId` from a 128-bit unsigned integer in big-endian byte order.
    pub const fn from_u128(val: u128) -> Self {
        Self(val.to_be_bytes())
    }

    /// Creates a `RunId` derived from a nanosecond timestamp.
    pub const fn from_nanos(nanos: u128) -> Self {
        Self(nanos.to_be_bytes())
    }

    /// Creates a `RunId` mixing a 64-bit nanosecond timestamp with a 64-bit process nonce or sequence counter.
    pub const fn from_nanos_and_nonce(nanos: u64, nonce: u64) -> Self {
        let mut bytes = [0u8; 16];
        let n_bytes = nanos.to_be_bytes();
        let c_bytes = nonce.to_be_bytes();
        let mut i = 0;
        while i < 8 {
            bytes[i] = n_bytes[i];
            bytes[i + 8] = c_bytes[i];
            i += 1;
        }
        Self(bytes)
    }

    /// Returns the raw 16 bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Metadata ordering local event delivery within a connector run.
///
/// `DeliveryStamp` orders events within a single connector generation, assigning
/// strictly increasing monotonic ordinals at actual emission time. It carries a
/// connection identifier distinguishing simultaneous connections/buckets within a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryStamp {
    /// Connector run session token.
    pub run: RunId,
    /// Connection identifier distinguishing simultaneous connections/buckets within a run.
    #[serde(default)]
    pub connection_id: u32,
    /// Connection generation, incremented on each supervised reconnect.
    pub connection_generation: u64,
    /// Strictly increasing ordinal within this connection generation.
    pub ordinal: u64,
}

impl DeliveryStamp {
    /// Creates a new `DeliveryStamp` with default `connection_id: 0`.
    pub const fn new(run: RunId, connection_generation: u64, ordinal: u64) -> Self {
        Self {
            run,
            connection_id: 0,
            connection_generation,
            ordinal,
        }
    }

    /// Creates a new `DeliveryStamp` with an explicit `connection_id`.
    pub const fn with_connection_id(
        run: RunId,
        connection_id: u32,
        connection_generation: u64,
        ordinal: u64,
    ) -> Self {
        Self {
            run,
            connection_id,
            connection_generation,
            ordinal,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn run_id_and_delivery_stamp() {
        let run = RunId::from_bytes([1; 16]);
        assert_eq!(run.as_bytes(), &[1; 16]);

        let run_u128 = RunId::from_u128(42);
        assert_eq!(run_u128.as_bytes()[15], 42);

        let run_nanos = RunId::from_nanos(1_000_000_000);
        assert_eq!(run_nanos.as_bytes()[12], 0x3b); // 1_000_000_000 in be: byte 12 is 0x3b

        let run_mixed = RunId::from_nanos_and_nonce(1_000_000_000, 42);
        assert_eq!(&run_mixed.as_bytes()[0..8], &1_000_000_000u64.to_be_bytes());
        assert_eq!(&run_mixed.as_bytes()[8..16], &42u64.to_be_bytes());

        let stamp = DeliveryStamp::new(run, 2, 42);
        assert_eq!(stamp.run, run);
        assert_eq!(stamp.connection_id, 0);
        assert_eq!(stamp.connection_generation, 2);
        assert_eq!(stamp.ordinal, 42);

        let stamp_conn = DeliveryStamp::with_connection_id(run, 7, 2, 42);
        assert_eq!(stamp_conn.connection_id, 7);

        let json = serde_json::to_string(&stamp_conn).expect("serialize");
        let decoded: DeliveryStamp = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, stamp_conn);

        // Backwards compatibility: deserialize old JSON without connection_id
        let legacy_json = format!(
            r#"{{"run":{},"connection_generation":2,"ordinal":42}}"#,
            serde_json::to_string(&stamp.run).expect("serialize run")
        );
        let decoded_legacy: DeliveryStamp =
            serde_json::from_str(&legacy_json).expect("deserialize legacy");
        assert_eq!(decoded_legacy.connection_id, 0);
        assert_eq!(decoded_legacy.connection_generation, 2);
        assert_eq!(decoded_legacy.ordinal, 42);
    }
}
