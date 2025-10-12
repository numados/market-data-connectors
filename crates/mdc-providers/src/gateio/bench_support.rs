//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Gate.io.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, SequenceRange, Side, StreamingSink, Symbol,
    SymbolTag, UnixNanos, VenueId,
};
use serde::de::DeserializeSeed;

use crate::gateio::raw::{GateioConnectionScratch, GateioDepthFrameSeed, extract_symbol};

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

        let mut deserializer = serde_json::Deserializer::from_str(frame_str);
        let header =
            match GateioDepthFrameSeed::new(&mut self.scratch).deserialize(&mut deserializer) {
                Ok(h) => h,
                Err(_) => return,
            };

        let sym_str = extract_symbol(header.obu.s);
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
            exchange_ts: Some(UnixNanos::from_millis(header.obu.t)),
            received_ts: crate::now_nanos(),
            connection_id: 1,
            delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
            source_sequence: Some(header.obu.last_id),
        };

        let kind = if header.obu.full {
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
        let seq = if header.obu.full {
            None
        } else {
            Some(SequenceRange::new(header.obu.first_id, header.obu.last_id))
        };
        let _ = self.sink.end_book(&ctx, seq);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mdc_testkit::{NoopStreamingSink, load_fixture};

    #[test]
    fn test_handle_frame_zero_alloc() {
        let snap_str =
            load_fixture("crates/mdc-providers/tests/fixtures/gateio/order_book_snapshot.json")
                .unwrap();
        let upd_str =
            load_fixture("crates/mdc-providers/tests/fixtures/gateio/depth_update.json").unwrap();
        let snap_bytes = snap_str.as_bytes();
        let upd_bytes = upd_str.as_bytes();

        let mut conn = Connection::new(NoopStreamingSink);
        // Warm-up: initialize scratch buffers
        conn.handle_frame(snap_bytes);
        conn.handle_frame(upd_bytes);

        let allocs = allocation_counter::measure(|| {
            conn.handle_frame(upd_bytes);
        });
        assert_eq!(
            allocs.count_total, 0,
            "streaming path must not allocate after warm-up"
        );
    }
}
