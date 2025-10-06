//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Gate.io.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, SequenceRange, Side, StreamingSink, Symbol,
    SymbolTag, UnixNanos, VenueId,
};
use serde::de::DeserializeSeed;

use crate::gateio::raw::{GateioConnectionScratch, ObuSeed, RawEnvelope, extract_symbol};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: GateioConnectionScratch,
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
            scratch: GateioConnectionScratch::with_capacity(512),
            sink,
            run_id: RunId::from_bytes([1; 16]),
            symbol: Symbol::from("BTC_USDT"),
            tag: SymbolTag::default(),
            delivery_ordinal: 0,
        }
    }

    /// Handles a raw JSON frame slice with zero heap allocations after warm-up.
    pub fn handle_frame(&mut self, frame: &[u8]) {
        let frame_str = match std::str::from_utf8(frame) {
            Ok(s) => s,
            Err(_) => return,
        };

        let env: RawEnvelope<'_> = match serde_json::from_str(frame_str) {
            Ok(e) => e,
            Err(_) => return,
        };

        let Some(res_val) = env.result else {
            return;
        };

        let mut deserializer = serde_json::Deserializer::from_str(res_val.get());
        let header = match ObuSeed::new(&mut self.scratch).deserialize(&mut deserializer) {
            Ok(h) => h,
            Err(_) => return,
        };

        let sym_str = extract_symbol(header.s);
        let sym = if sym_str == self.symbol.as_str() {
            &self.symbol
        } else {
            return;
        };

        self.delivery_ordinal += 1;
        let ctx = EventContext {
            venue: VenueId::Gateio,
            symbol: sym,
            tag: self.tag,
            exchange_ts: Some(UnixNanos::from_millis(header.t)),
            received_ts: UnixNanos::now(),
            connection_id: 1,
            delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
            source_sequence: Some(header.last_id),
        };

        let kind = if header.full {
            BookKind::Snapshot
        } else {
            BookKind::Delta
        };
        let _ = self.sink.begin_book(&ctx, kind);
        for level in &self.scratch.bids {
            let _ = self.sink.level(Side::Bid, level.price, level.volume);
        }
        for level in &self.scratch.asks {
            let _ = self.sink.level(Side::Ask, level.price, level.volume);
        }
        let seq = if header.full {
            None
        } else {
            Some(SequenceRange::new(header.first_id, header.last_id))
        };
        let _ = self.sink.end_book(&ctx, seq);
    }
}
