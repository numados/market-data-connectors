//! Connector run identity and delivery ordering stamps.

use serde::{Deserialize, Serialize};

/// Opaque caller-supplied 16-byte session token identifying a connector lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunId(pub [u8; 16]);

impl RunId {
    /// Creates a `RunId` from raw 16 bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
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
/// strictly increasing ordinals at actual emission time. It is not a durable trade
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

        let stamp = DeliveryStamp::new(run, 2, 42);
        assert_eq!(stamp.run, run);
        assert_eq!(stamp.connection_generation, 2);
        assert_eq!(stamp.ordinal, 42);

        let json = serde_json::to_string(&stamp).expect("serialize");
        let decoded: DeliveryStamp = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, stamp);
    }
}
