//! Benchmark support harness demonstrating hot-path zero-heap frame processing for LMAX Digital.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, Side, StreamingSink, Symbol, SymbolTag, VenueId,
};
use serde::de::DeserializeSeed;

use crate::lmax::normalize::parse_rfc3339_nanos;
use crate::lmax::raw::{LmaxBookFrameSeed, LmaxConnectionScratch, LmaxFrameKind, peek_frame_kind};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: LmaxConnectionScratch,
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
            scratch: LmaxConnectionScratch::with_capacity(512),
            sink,
            run_id: RunId::from_bytes([1; 16]),
            symbol: Symbol::from("btc-usd"),
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

        if peek_frame_kind(frame) != LmaxFrameKind::OrderBook {
            return;
        }

        let mut deserializer = serde_json::Deserializer::from_str(frame_str);
        let header = match LmaxBookFrameSeed::new(&mut self.scratch).deserialize(&mut deserializer)
        {
            Ok(h) => h,
            Err(_) => return,
        };

        if header.instrument_id != self.symbol.as_str() {
            return;
        }

        let exchange_ts = parse_rfc3339_nanos(header.timestamp).ok();
        self.delivery_ordinal += 1;
        let ctx = EventContext {
            venue: VenueId::Lmax,
            symbol: &self.symbol,
            tag: self.tag,
            exchange_ts,
            received_ts: crate::now_nanos(),
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mdc_testkit::{NoopStreamingSink, load_fixture};

    #[test]
    fn test_handle_frame_zero_alloc() {
        let snap_str =
            load_fixture("crates/mdc-providers/tests/fixtures/lmax/order_book_snapshot.json")
                .unwrap();
        let snap_bytes = snap_str.as_bytes();

        let mut conn = Connection::new(NoopStreamingSink);
        // Warm-up: initialize scratch buffers
        conn.handle_frame(snap_bytes);
        conn.handle_frame(snap_bytes);

        let allocs = allocation_counter::measure(|| {
            conn.handle_frame(snap_bytes);
        });
        assert_eq!(
            allocs.count_total, 0,
            "streaming path must not allocate after warm-up"
        );
    }
}
