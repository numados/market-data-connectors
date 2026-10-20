//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Bitstamp.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, Side, StreamingSink, Symbol, SymbolTag,
    UnixNanos, VenueId,
};
use serde::de::DeserializeSeed;

use crate::bitstamp::raw::{BitstampConnectionScratch, BitstampDiffFrameSeed, extract_symbol};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: BitstampConnectionScratch,
    sink: S,
    run_id: RunId,
    symbol: Symbol,
    tag: SymbolTag,
    delivery_ordinal: u64,
}

impl<S: StreamingSink> Connection<S> {
    /// Creates a new connection with pre-allocated scratch capacity.
    pub fn new(sink: S) -> Self {
        Self::with_run_id(sink, RunId::from_nanos(crate::now_nanos().0 as u128))
    }

    /// Creates a new connection with a specific `RunId`.
    pub fn with_run_id(sink: S, run_id: RunId) -> Self {
        Self {
            scratch: BitstampConnectionScratch::with_capacity(512),
            sink,
            run_id,
            symbol: Symbol::from("btcusd"),
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
            match BitstampDiffFrameSeed::new(&mut self.scratch).deserialize(&mut deserializer) {
                Ok(h) => h,
                Err(_) => return,
            };

        let sym_str = extract_symbol(header.channel);
        let sym = if sym_str == self.symbol.as_str() {
            &self.symbol
        } else {
            return;
        };

        self.delivery_ordinal += 1;
        let ctx = EventContext {
            venue: VenueId::Bitstamp,
            symbol: sym,
            tag: self.tag,
            exchange_ts: Some(UnixNanos::from_micros(header.microtimestamp)),
            received_ts: crate::now_nanos(),
            connection_id: 1,
            delivery: DeliveryStamp::with_connection_id(self.run_id, 1, 1, self.delivery_ordinal),
            source_sequence: None,
        };

        if self.sink.begin_book(&ctx, BookKind::Delta).is_err() {
            return;
        }
        for level in &self.scratch.bids {
            if self
                .sink
                .level(Side::Bid, level.price, level.volume)
                .is_err()
            {
                return;
            }
        }
        for level in &self.scratch.asks {
            if self
                .sink
                .level(Side::Ask, level.price, level.volume)
                .is_err()
            {
                return;
            }
        }
        if self.sink.end_book(&ctx, None).is_err() {
            return;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mdc_testkit::{NoopStreamingSink, load_fixture};

    #[test]
    fn test_handle_frame_zero_alloc() {
        let upd_str =
            load_fixture("crates/mdc-providers/tests/fixtures/bitstamp/depth_update.json").unwrap();
        let upd_bytes = upd_str.as_bytes();

        let mut conn = Connection::new(NoopStreamingSink);
        // Warm-up: initialize scratch buffers
        conn.handle_frame(upd_bytes);
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
