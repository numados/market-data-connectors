//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Coinbase Advanced.
//!
//! The harness drives the production [`dispatch_frame`](crate::coinbase_advanced::connection)
//! path, so the measured and allocation-checked code is the code the connector runs. Only the
//! connection-wide sequence check is skipped, so the same frame can be replayed.

use std::time::Duration;

use mdc_core::{RunId, StreamingSink, Symbol, SymbolTag};
use serde::de::DeserializeSeed;

use crate::coinbase_advanced::connection::dispatch_frame;
use crate::coinbase_advanced::fsm::CoinbaseAdvancedFsm;
use crate::coinbase_advanced::raw::{CoinbaseAdvancedFrameSeed, CoinbaseAdvancedScratch};
use crate::sink_util::ConnectionSink;

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: CoinbaseAdvancedScratch,
    fsm: CoinbaseAdvancedFsm,
    sink: ConnectionSink<S>,
}

impl<S: StreamingSink> Connection<S> {
    /// Creates a new connection with pre-allocated scratch capacity.
    pub fn new(sink: S) -> Self {
        let symbols = vec![(Symbol::from("BTC-USD"), SymbolTag::default())];
        Self {
            scratch: CoinbaseAdvancedScratch::with_capacity(1_024),
            fsm: CoinbaseAdvancedFsm::new(symbols, Duration::from_secs(5)),
            sink: ConnectionSink::new(sink, RunId::from_u128(1), 1),
        }
    }

    /// Handles an uncompressed JSON frame slice with zero heap allocations after warm-up.
    pub fn handle_raw_frame(&mut self, frame: &[u8]) {
        let Ok(text) = std::str::from_utf8(frame) else {
            return;
        };

        let mut deserializer = serde_json::Deserializer::from_str(text);
        let Ok(header) =
            CoinbaseAdvancedFrameSeed::new(&mut self.scratch).deserialize(&mut deserializer)
        else {
            return;
        };

        dispatch_frame(
            1,
            false,
            &header,
            text,
            crate::now_nanos(),
            &self.scratch,
            &mut self.fsm,
            &mut self.sink,
        );
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

    #[test]
    fn test_harness_publishes_through_production_dispatch() {
        use mdc_core::BookKind;
        use mdc_testkit::{RecordedEvent, RecordingSink};

        let snap_raw =
            include_bytes!("../../tests/fixtures/coinbase_advanced/order_book_snapshot.json");
        let upd_raw = include_bytes!("../../tests/fixtures/coinbase_advanced/depth_update.json");

        let sink = RecordingSink::new();
        let mut conn = Connection::new(sink.clone());
        conn.handle_frame(snap_raw);
        conn.handle_frame(upd_raw);

        let kinds: Vec<_> = sink
            .events()
            .into_iter()
            .filter_map(|e| match e {
                RecordedEvent::BeginBook { kind, .. } => Some(kind),
                _ => None,
            })
            .collect();
        assert_eq!(kinds, vec![BookKind::Snapshot, BookKind::Delta]);
    }
}
