//! Benchmark support harness demonstrating hot-path zero-heap frame processing for VWD.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, Side, StreamingSink, SymbolTag, VenueId,
};
use serde::de::DeserializeSeed;

use crate::vwd::config::{VwdConfig, VwdCredentials, VwdSymbol};
use crate::vwd::normalize::normalize_symbol_from_scratch;
use crate::vwd::raw::{VwdScratch, VwdScratchSeed};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: VwdScratch,
    config: VwdConfig,
    sink: S,
    run_id: RunId,
    tag: SymbolTag,
    delivery_ordinal: u64,
}

impl<S: StreamingSink> Connection<S> {
    /// Creates a new connection with pre-allocated scratch capacity.
    pub fn new(sink: S) -> Self {
        #[allow(clippy::expect_used)]
        let config = VwdConfig::builder()
            .account_path("bench")
            .credentials(VwdCredentials::new("bench", "bench"))
            .add_symbol(VwdSymbol::with_inverse("EURUSD"))
            .build()
            .expect("valid bench config");

        Self {
            scratch: VwdScratch::with_capacity(32),
            config,
            sink,
            run_id: RunId::from_bytes([1; 16]),
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
        if VwdScratchSeed::new(&mut self.scratch)
            .deserialize(&mut deserializer)
            .is_err()
        {
            return;
        }

        let now = crate::now_nanos();

        for sym in &self.config.symbols {
            self.delivery_ordinal += 1;
            if let Ok(norm) =
                normalize_symbol_from_scratch(sym, &self.scratch, self.config.significant_digits)
            {
                let ctx = EventContext {
                    venue: VenueId::Vwd,
                    symbol: &sym.symbol,
                    tag: self.tag,
                    exchange_ts: norm.exchange_ts,
                    received_ts: now,
                    connection_id: 1,
                    delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
                    source_sequence: None,
                };

                let _ = self.sink.begin_book(&ctx, BookKind::Snapshot);
                let _ = self
                    .sink
                    .level(Side::Bid, norm.bid, self.config.indicative_size);
                let _ = self
                    .sink
                    .level(Side::Ask, norm.ask, self.config.indicative_size);
                let _ = self.sink.end_book(&ctx, None);
            }
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
        let snap_str =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json")
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
