//! Adapter converting a `MarketDataSink` into a `StreamingSink`.

use rust_decimal::Decimal;

use crate::model::{
    BookTicker, IntegrityAlert, L2Delta, L2Snapshot, MarketDataKind, MarketEvent, PriceLevel,
    ReferenceRate, SequenceRange, Side, Trade,
};
use crate::sink::MarketDataSink;
use crate::sink::streaming::{BookKind, EventContext, StreamingSink};

/// Adapts an owned `MarketDataSink` into a `StreamingSink`.
///
/// Collects individual price level calls between `begin_book` and `end_book` into
/// owned vectors before constructing canonical `MarketEvent` instances and forwarding
/// them downstream.
#[derive(Debug, Clone)]
pub struct Owned<S> {
    sink: S,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
    active_kind: Option<BookKind>,
}

impl<S> Owned<S> {
    /// Creates a new `Owned` adapter wrapping the given market data sink.
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            bids: Vec::new(),
            asks: Vec::new(),
            active_kind: None,
        }
    }

    /// Returns a reference to the inner sink.
    pub fn inner(&self) -> &S {
        &self.sink
    }

    /// Consumes the adapter, returning the inner sink.
    pub fn into_inner(self) -> S {
        self.sink
    }
}

impl<S: MarketDataSink> StreamingSink for Owned<S> {
    type Error = S::Error;

    fn begin_book(&mut self, _ctx: &EventContext<'_>, kind: BookKind) -> Result<(), Self::Error> {
        self.bids.clear();
        self.asks.clear();
        self.active_kind = Some(kind);
        Ok(())
    }

    fn level(&mut self, side: Side, price: Decimal, volume: Decimal) -> Result<(), Self::Error> {
        let level = PriceLevel::new(price, volume);
        match side {
            Side::Bid => self.bids.push(level),
            Side::Ask => self.asks.push(level),
        }
        Ok(())
    }

    fn end_book(
        &mut self,
        ctx: &EventContext<'_>,
        seq: Option<SequenceRange>,
    ) -> Result<(), Self::Error> {
        let kind = self.active_kind.take().unwrap_or(BookKind::Delta);
        let bids = std::mem::take(&mut self.bids);
        let asks = std::mem::take(&mut self.asks);

        let event_kind = match kind {
            BookKind::Snapshot => {
                let last_update_id = seq.map(|s| s.last);
                MarketDataKind::L2Snapshot(L2Snapshot {
                    bids,
                    asks,
                    last_update_id,
                })
            }
            BookKind::Delta => MarketDataKind::L2Delta(L2Delta {
                bids,
                asks,
                sequence: seq,
            }),
        };

        let event = MarketEvent {
            venue: ctx.venue,
            symbol: ctx.symbol.clone(),
            tag: ctx.tag,
            exchange_ts: ctx.exchange_ts,
            received_ts: ctx.received_ts,
            delivery: ctx.delivery,
            source_sequence: ctx.source_sequence,
            kind: event_kind,
        };

        self.sink.on_event(event)
    }

    fn trade(&mut self, ctx: &EventContext<'_>, trade: &Trade) -> Result<(), Self::Error> {
        let event = MarketEvent {
            venue: ctx.venue,
            symbol: ctx.symbol.clone(),
            tag: ctx.tag,
            exchange_ts: ctx.exchange_ts,
            received_ts: ctx.received_ts,
            delivery: ctx.delivery,
            source_sequence: ctx.source_sequence,
            kind: MarketDataKind::Trade(trade.clone()),
        };
        self.sink.on_event(event)
    }

    fn book_ticker(
        &mut self,
        ctx: &EventContext<'_>,
        ticker: &BookTicker,
    ) -> Result<(), Self::Error> {
        let event = MarketEvent {
            venue: ctx.venue,
            symbol: ctx.symbol.clone(),
            tag: ctx.tag,
            exchange_ts: ctx.exchange_ts,
            received_ts: ctx.received_ts,
            delivery: ctx.delivery,
            source_sequence: ctx.source_sequence,
            kind: MarketDataKind::BookTicker(*ticker),
        };
        self.sink.on_event(event)
    }

    fn reference_rate(
        &mut self,
        ctx: &EventContext<'_>,
        rate: &ReferenceRate,
    ) -> Result<(), Self::Error> {
        let event = MarketEvent {
            venue: ctx.venue,
            symbol: ctx.symbol.clone(),
            tag: ctx.tag,
            exchange_ts: ctx.exchange_ts,
            received_ts: ctx.received_ts,
            delivery: ctx.delivery,
            source_sequence: ctx.source_sequence,
            kind: MarketDataKind::ReferenceRate(*rate),
        };
        self.sink.on_event(event)
    }

    fn alert(&mut self, ctx: &EventContext<'_>, alert: IntegrityAlert) -> Result<(), Self::Error> {
        let event = MarketEvent {
            venue: ctx.venue,
            symbol: ctx.symbol.clone(),
            tag: ctx.tag,
            exchange_ts: ctx.exchange_ts,
            received_ts: ctx.received_ts,
            delivery: ctx.delivery,
            source_sequence: ctx.source_sequence,
            kind: MarketDataKind::IntegrityAlert(alert),
        };
        self.sink.on_event(event)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::model::{DeliveryStamp, RunId, SymbolTag, UnixNanos, VenueId};
    use crate::sink::VecSink;
    use std::sync::Arc;

    #[test]
    fn test_owned_adapter_snapshot_and_delta() {
        let vec_sink = Arc::new(VecSink::new());
        let mut owned = Owned::new(vec_sink.clone());

        let symbol = crate::model::Symbol::from("BTCUSDT");
        let delivery = DeliveryStamp::new(RunId::from_bytes([1; 16]), 0, 1);
        let ctx = EventContext {
            venue: VenueId::Binance,
            symbol: &symbol,
            tag: SymbolTag::default(),
            exchange_ts: None,
            received_ts: UnixNanos::from_millis(100),
            connection_id: 1,
            delivery,
            source_sequence: None,
        };

        let price_100_5 = Decimal::from_str_exact("100.5").expect("valid decimal");
        let price_101_0 = Decimal::from_str_exact("101.0").expect("valid decimal");
        let price_100_6 = Decimal::from_str_exact("100.6").expect("valid decimal");
        let qty_2 = Decimal::from_str_exact("2.0").expect("valid decimal");
        let qty_1_5 = Decimal::from_str_exact("1.5").expect("valid decimal");
        let qty_3 = Decimal::from_str_exact("3.0").expect("valid decimal");

        // Snapshot
        owned
            .begin_book(&ctx, BookKind::Snapshot)
            .expect("begin_book");
        owned.level(Side::Bid, price_100_5, qty_2).expect("level");
        owned.level(Side::Ask, price_101_0, qty_1_5).expect("level");
        owned
            .end_book(&ctx, Some(SequenceRange::new(10, 10)))
            .expect("end_book");

        // Delta
        owned.begin_book(&ctx, BookKind::Delta).expect("begin_book");
        owned.level(Side::Bid, price_100_6, qty_3).expect("level");
        owned
            .end_book(&ctx, Some(SequenceRange::new(11, 12)))
            .expect("end_book");

        let events = vec_sink.events();
        assert_eq!(events.len(), 2);

        match &events[0].kind {
            MarketDataKind::L2Snapshot(snap) => {
                assert_eq!(snap.last_update_id, Some(10));
                assert_eq!(snap.bids.len(), 1);
                assert_eq!(snap.asks.len(), 1);
                assert_eq!(snap.bids[0].price, price_100_5);
            }
            _ => panic!("Expected L2Snapshot"),
        }

        match &events[1].kind {
            MarketDataKind::L2Delta(delta) => {
                assert_eq!(delta.sequence, Some(SequenceRange::new(11, 12)));
                assert_eq!(delta.bids.len(), 1);
                assert_eq!(delta.asks.len(), 0);
            }
            _ => panic!("Expected L2Delta"),
        }
    }
}
