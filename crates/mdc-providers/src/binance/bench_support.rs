//! Benchmark support harness demonstrating hot-path zero-heap frame processing.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, SequenceRange, Side, StreamingSink, Symbol,
    SymbolTag, UnixNanos, VenueId,
};
use serde::de::DeserializeSeed;

use crate::binance::raw::{CombinedDepthSeed, ConnectionScratchBuffers};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: ConnectionScratchBuffers,
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
            scratch: ConnectionScratchBuffers::with_capacity(128),
            sink,
            run_id: RunId::from_u128(1),
            symbol: Symbol::from("BTCUSDT"),
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
        let header = match CombinedDepthSeed::new(&mut self.scratch).deserialize(&mut deserializer)
        {
            Ok(h) => h,
            Err(_) => return,
        };

        self.delivery_ordinal += 1;
        let ctx = EventContext {
            venue: VenueId::Binance,
            symbol: &self.symbol,
            tag: self.tag,
            exchange_ts: Some(UnixNanos::from_millis(header.depth.event_time_ms)),
            received_ts: crate::now_nanos(),
            connection_id: 1,
            delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
            source_sequence: Some(header.depth.last_update_id),
        };

        let _ = self.sink.begin_book(&ctx, BookKind::Delta);
        for level in &self.scratch.bids {
            let _ = self.sink.level(Side::Bid, level.price, level.volume);
        }
        for level in &self.scratch.asks {
            let _ = self.sink.level(Side::Ask, level.price, level.volume);
        }
        let range = SequenceRange::new(header.depth.first_update_id, header.depth.last_update_id);
        let _ = self.sink.end_book(&ctx, Some(range));
    }
}
