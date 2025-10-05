//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Coinbase.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, Side, StreamingSink, Symbol, SymbolTag,
    UnixNanos, VenueId,
};
use serde::de::DeserializeSeed;

use crate::coinbase::normalize::parse_rfc3339_nanos;
use crate::coinbase::raw::{CoinbaseConnectionScratch, CoinbaseFrameHeader, CoinbaseFrameSeed};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: CoinbaseConnectionScratch,
    sink: S,
    run_id: RunId,
    symbol: Symbol,
    tag: SymbolTag,
    delivery_ordinal: u64,
}

impl<S: StreamingSink> Connection<S> {
    /// Creates a new connection with pre-allocated scratch capacity.
    pub fn new(sink: S) -> Self {
        Self {
            scratch: CoinbaseConnectionScratch::with_capacity(256),
            sink,
            run_id: RunId::from_bytes([1; 16]),
            symbol: Symbol::from("BTC-USD"),
            tag: 0,
            delivery_ordinal: 0,
        }
    }

    /// Handles a raw JSON frame slice with zero heap allocations after warm-up.
    pub fn handle_frame(&mut self, frame: &[u8]) {
        let frame_str = match std::str::from_utf8(frame) {
            Ok(s) => s,
            Err(_) => return,
        };

        let mut deserializer = serde_json::Deserializer::from_str(frame_str);
        let header = match CoinbaseFrameSeed::new(&mut self.scratch).deserialize(&mut deserializer)
        {
            Ok(h) => h,
            Err(_) => return,
        };

        match header {
            CoinbaseFrameHeader::Snapshot { .. } => {
                self.delivery_ordinal += 1;
                let ctx = EventContext {
                    venue: VenueId::Coinbase,
                    symbol: &self.symbol,
                    tag: self.tag,
                    exchange_ts: None,
                    received_ts: UnixNanos::now(),
                    connection_id: 1,
                    delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
                    source_sequence: None,
                };

                let _ = self.sink.begin_book(&ctx, BookKind::Snapshot);
                for level in &self.scratch.bids {
                    let _ = self.sink.level(Side::Bid, level.price, level.volume);
                }
                for level in &self.scratch.asks {
                    let _ = self.sink.level(Side::Ask, level.price, level.volume);
                }
                let _ = self.sink.end_book(&ctx, None);
            }
            CoinbaseFrameHeader::L2Update { time, .. } => {
                let exchange_ts = parse_rfc3339_nanos(time).ok();
                self.delivery_ordinal += 1;
                let ctx = EventContext {
                    venue: VenueId::Coinbase,
                    symbol: &self.symbol,
                    tag: self.tag,
                    exchange_ts,
                    received_ts: UnixNanos::now(),
                    connection_id: 1,
                    delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
                    source_sequence: None,
                };

                let _ = self.sink.begin_book(&ctx, BookKind::Delta);
                for level in &self.scratch.bids {
                    let _ = self.sink.level(Side::Bid, level.price, level.volume);
                }
                for level in &self.scratch.asks {
                    let _ = self.sink.level(Side::Ask, level.price, level.volume);
                }
                let _ = self.sink.end_book(&ctx, None);
            }
            _ => {}
        }
    }
}
