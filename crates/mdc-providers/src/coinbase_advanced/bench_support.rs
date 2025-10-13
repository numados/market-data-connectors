//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Coinbase Advanced.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, Side, StreamingSink, Symbol, SymbolTag, VenueId,
};
use serde::de::DeserializeSeed;

use crate::coinbase_advanced::normalize::parse_rfc3339_nanos;
use crate::coinbase_advanced::raw::{
    CoinbaseAdvancedFrameSeed, CoinbaseAdvancedScratch, L2EventKind,
};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: CoinbaseAdvancedScratch,
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
            scratch: CoinbaseAdvancedScratch::with_capacity(1_024),
            sink,
            run_id: RunId::from_u128(1),
            symbol: Symbol::from("BTC-USD"),
            tag: SymbolTag::default(),
            delivery_ordinal: 0,
        }
    }

    /// Handles an uncompressed JSON frame slice with zero heap allocations after warm-up.
    pub fn handle_raw_frame(&mut self, frame: &[u8]) {
        let frame_str = match std::str::from_utf8(frame) {
            Ok(s) => s,
            Err(_) => return,
        };

        let mut deserializer = serde_json::Deserializer::from_str(frame_str);
        let header = match CoinbaseAdvancedFrameSeed::new(&mut self.scratch)
            .deserialize(&mut deserializer)
        {
            Ok(h) => h,
            Err(_) => return,
        };

        if header.product_id != Some(self.symbol.as_str()) {
            return;
        }

        self.delivery_ordinal = self.delivery_ordinal.saturating_add(1);
        let ctx = EventContext {
            venue: VenueId::CoinbaseAdvanced,
            symbol: &self.symbol,
            tag: self.tag,
            exchange_ts: header.timestamp.and_then(|ts| parse_rfc3339_nanos(ts).ok()),
            received_ts: crate::now_nanos(),
            connection_id: 1,
            delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
            source_sequence: header.sequence_num,
        };

        match header.l2_kind {
            Some(L2EventKind::Snapshot) => {
                let _ = self.sink.begin_book(&ctx, BookKind::Snapshot);
                for level in &self.scratch.bids {
                    let _ = self.sink.level(Side::Bid, level.price, level.volume);
                }
                for level in &self.scratch.asks {
                    let _ = self.sink.level(Side::Ask, level.price, level.volume);
                }
                let _ = self.sink.end_book(&ctx, None);
            }
            Some(L2EventKind::Update) => {
                let _ = self.sink.begin_book(&ctx, BookKind::Delta);
                for level in &self.scratch.bids {
                    let _ = self.sink.level(Side::Bid, level.price, level.volume);
                }
                for level in &self.scratch.asks {
                    let _ = self.sink.level(Side::Ask, level.price, level.volume);
                }
                let _ = self.sink.end_book(&ctx, None);
            }
            None => {}
        }
    }

    /// Handles an uncompressed JSON frame slice (alias for [`Self::handle_raw_frame`]).
    pub fn handle_frame(&mut self, frame: &[u8]) {
        self.handle_raw_frame(frame);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mdc_testkit::NoopStreamingSink;

    #[test]
    fn test_handle_frame_zero_alloc() {
        let upd_raw = include_bytes!("../../tests/fixtures/coinbase_advanced/depth_update.json");
        let snap_raw =
            include_bytes!("../../tests/fixtures/coinbase_advanced/order_book_snapshot.json");

        let mut conn = Connection::new(NoopStreamingSink);

        // Warm-up: initialize scratch buffers
        conn.handle_frame(snap_raw);
        conn.handle_frame(upd_raw);

        // Assert 0 allocations on streaming path
        let allocs_snap = allocation_counter::measure(|| {
            conn.handle_frame(snap_raw);
        });
        assert_eq!(
            allocs_snap.count_total, 0,
            "snapshot streaming path must not allocate after warm-up"
        );

        let allocs_upd = allocation_counter::measure(|| {
            conn.handle_frame(upd_raw);
        });
        assert_eq!(
            allocs_upd.count_total, 0,
            "update streaming path must not allocate after warm-up"
        );
    }
}
