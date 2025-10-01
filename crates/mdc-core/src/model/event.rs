//! Canonical market data event envelope and payload enum.

use super::{
    alert::IntegrityAlert,
    book::{L2Delta, L2Snapshot},
    delivery::DeliveryStamp,
    reference::ReferenceRate,
    symbol::{Symbol, SymbolTag},
    ticker::BookTicker,
    timestamp::UnixNanos,
    trade::Trade,
    venue::VenueId,
};
use serde::{Deserialize, Serialize};

/// Sum-type covering all normalized market data events and integrity alerts.
/// Exhaustive: downstream consumers must explicitly handle every variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum MarketDataKind {
    L2Delta(L2Delta),
    L2Snapshot(L2Snapshot),
    Trade(Trade),
    BookTicker(BookTicker),
    ReferenceRate(ReferenceRate),
    IntegrityAlert(IntegrityAlert),
}

/// Uniform envelope for all normalized market events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketEvent<T> {
    /// Venue-reported exchange timestamp, when the payload carries one.
    pub exchange_ts: Option<UnixNanos>,
    /// Ingestion wall-clock time captured immediately upon socket frame receipt, before decompression/parsing.
    pub received_ts: UnixNanos,
    /// Exchange venue.
    pub venue: VenueId,
    /// Trading symbol as the venue spells it.
    pub symbol: Symbol,
    /// Consumer-assigned numeric identifier for the symbol.
    pub tag: SymbolTag,
    /// Delivery stamp for local in-run event ordering.
    pub delivery: DeliveryStamp,
    /// Scoped sequence number published by the venue, if available.
    pub source_sequence: Option<u64>,
    /// Event payload.
    pub kind: T,
}

impl<T> MarketEvent<T> {
    /// Maps the inner event payload using a transforming closure, preserving all envelope metadata.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> MarketEvent<U> {
        MarketEvent {
            exchange_ts: self.exchange_ts,
            received_ts: self.received_ts,
            venue: self.venue,
            symbol: self.symbol,
            tag: self.tag,
            delivery: self.delivery,
            source_sequence: self.source_sequence,
            kind: f(self.kind),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::model::delivery::RunId;

    #[test]
    fn event_map_preserves_metadata() {
        let event = MarketEvent {
            exchange_ts: Some(UnixNanos::from_millis(1000)),
            received_ts: UnixNanos::from_millis(1005),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: 1,
            delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 10),
            source_sequence: Some(500),
            kind: 42,
        };

        let mapped = event.map(|k| k * 2);
        assert_eq!(mapped.kind, 84);
        assert_eq!(mapped.exchange_ts, Some(UnixNanos::from_millis(1000)));
        assert_eq!(mapped.received_ts, UnixNanos::from_millis(1005));
        assert_eq!(mapped.venue, VenueId::Binance);
        assert_eq!(mapped.symbol.as_str(), "BTCUSDT");
        assert_eq!(mapped.tag, 1);
        assert_eq!(mapped.delivery.ordinal, 10);
        assert_eq!(mapped.source_sequence, Some(500));
    }

    #[test]
    fn event_serde_roundtrip() {
        let event = MarketEvent {
            exchange_ts: None,
            received_ts: UnixNanos::from_millis(123456789),
            venue: VenueId::Kraken,
            symbol: Symbol::from("ETH/USD"),
            tag: 100,
            delivery: DeliveryStamp::new(RunId::from_bytes([7; 16]), 1, 1),
            source_sequence: None,
            kind: MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncStarted),
        };

        let json = serde_json::to_string(&event).expect("serialize");
        let decoded: MarketEvent<MarketDataKind> =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, event);
    }
}
