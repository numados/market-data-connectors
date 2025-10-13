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

    /// Returns the raw 16 bytes.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Metadata ordering local event delivery within a connector run.
///
/// `DeliveryStamp` orders events within a single connector generation, assigning
/// strictly increasing monotonic ordinals at actual emission time. It is unique
/// and strictly ordered across the lifetime of a connector run. It is not a durable trade
/// identity or cross-connection total order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryStamp {
    /// Connector run session token.
    pub run: RunId,
    /// Connection generation, incremented on each supervised reconnect.
    pub connection_generation: u64,
    /// Strictly increasing ordinal within this connection generation.
    pub ordinal: u64,
}

impl DeliveryStamp {
    /// Creates a new `DeliveryStamp`.
    pub const fn new(run: RunId, connection_generation: u64, ordinal: u64) -> Self {
        Self {
            run,
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

        let stamp = DeliveryStamp::new(run, 2, 42);
        assert_eq!(stamp.run, run);
        assert_eq!(stamp.connection_generation, 2);
        assert_eq!(stamp.ordinal, 42);

        let json = serde_json::to_string(&stamp).expect("serialize");
        let decoded: DeliveryStamp = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, stamp);
    }
}
