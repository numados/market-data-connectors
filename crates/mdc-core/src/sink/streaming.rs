//! Zero-copy streaming sink contract and event context.

use rust_decimal::Decimal;
use std::fmt::Debug;

use crate::model::{
    BookTicker, DeliveryStamp, IntegrityAlert, ReferenceRate, SequenceRange, Side, Symbol,
    SymbolTag, Trade, UnixNanos, VenueId,
};

/// Type of order book emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookKind {
    /// Full order book snapshot.
    Snapshot,
    /// Incremental level updates.
    Delta,
}

/// Venue raw price and volume token strings for checksum calculation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawLevelText<'a> {
    /// Exact price string received from the exchange.
    pub price: &'a str,
    /// Exact volume/quantity string received from the exchange.
    pub volume: &'a str,
}

/// Metadata context passed alongside each market data stream callback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventContext<'a> {
    /// Source venue identifier.
    pub venue: VenueId,
    /// Market symbol.
    pub symbol: &'a Symbol,
    /// Consumer-defined opaque symbol tag.
    pub tag: SymbolTag,
    /// Venue-reported exchange timestamp, if provided.
    pub exchange_ts: Option<UnixNanos>,
    /// Ingestion timestamp captured immediately upon socket frame receipt.
    pub received_ts: UnixNanos,
    /// Connection identifier, stable across the lifetime of a single transport socket.
    pub connection_id: u32,
    /// MRS delivery stamp for auditability and replay tracking.
    pub delivery: DeliveryStamp,
    /// Optional venue-scoped sequence number.
    pub source_sequence: Option<u64>,
}

/// Hot-path zero-copy streaming sink interface.
///
/// Providers emit price levels individually during book deserialization without allocating
/// intermediate `Vec` or `MarketEvent` instances. One `StreamingSink` instance is typically
/// owned per connection task (`&mut self`).
pub trait StreamingSink: Send + 'static {
    /// Error returned when the sink cannot accept a streaming callback.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Signals the start of an order book update (snapshot or delta).
    fn begin_book(&mut self, ctx: &EventContext<'_>, kind: BookKind) -> Result<(), Self::Error>;

    /// Delivers an individual price level.
    fn level(&mut self, side: Side, price: Decimal, volume: Decimal) -> Result<(), Self::Error>;

    /// Delivers an individual price level with raw tokens for checksum-verifying venues.
    fn level_raw(
        &mut self,
        side: Side,
        price: Decimal,
        volume: Decimal,
        raw: RawLevelText<'_>,
    ) -> Result<(), Self::Error> {
        let _ = raw;
        self.level(side, price, volume)
    }

    /// Signals the completion of an order book update with an optional sequence range.
    fn end_book(
        &mut self,
        ctx: &EventContext<'_>,
        seq: Option<SequenceRange>,
    ) -> Result<(), Self::Error>;

    /// Delivers a normalized public trade.
    fn trade(&mut self, ctx: &EventContext<'_>, trade: &Trade) -> Result<(), Self::Error>;

    /// Delivers a top-of-book ticker.
    fn book_ticker(
        &mut self,
        ctx: &EventContext<'_>,
        ticker: &BookTicker,
    ) -> Result<(), Self::Error>;

    /// Delivers a reference interest or foreign exchange rate.
    fn reference_rate(
        &mut self,
        ctx: &EventContext<'_>,
        rate: &ReferenceRate,
    ) -> Result<(), Self::Error>;

    /// Delivers an in-band integrity alert ordered with market data.
    fn alert(&mut self, ctx: &EventContext<'_>, alert: IntegrityAlert) -> Result<(), Self::Error>;
}
