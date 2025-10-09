//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Frankfurter.

use mdc_core::{
    DeliveryStamp, EventContext, ReferenceRate, RunId, StreamingSink, SymbolTag, UnixNanos, VenueId,
};
use serde::de::DeserializeSeed;

use crate::frankfurter::config::FrankfurterConfig;
use crate::frankfurter::normalize::{NormalizedBatchRate, normalize_batch_from_scratch};
use crate::frankfurter::raw::{FrankfurterScratch, FrankfurterScratchSeed};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: FrankfurterScratch,
    normalized_buf: Vec<NormalizedBatchRate>,
    config: FrankfurterConfig,
    sink: S,
    run_id: RunId,
    tag: SymbolTag,
    delivery_ordinal: u64,
}

impl<S: StreamingSink> Connection<S> {
    /// Creates a new connection with pre-allocated scratch capacity.
    pub fn new(sink: S) -> Self {
        Self {
            scratch: FrankfurterScratch::with_capacity(32),
            normalized_buf: Vec::with_capacity(32),
            config: FrankfurterConfig::default(),
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
        if FrankfurterScratchSeed::new(&mut self.scratch)
            .deserialize(&mut deserializer)
            .is_err()
        {
            return;
        }

        if normalize_batch_from_scratch(
            &self.config.base_currency,
            &self.config.quote_currencies,
            &self.scratch,
            &mut self.normalized_buf,
        )
        .is_err()
        {
            return;
        }

        let now = UnixNanos::now();

        for item in &self.normalized_buf {
            self.delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::FrankfurterEcb,
                symbol: &item.symbol,
                tag: self.tag,
                exchange_ts: Some(item.exchange_ts),
                received_ts: now,
                connection_id: 1,
                delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
                source_sequence: None,
            };

            let rate = ReferenceRate::new(item.mid);
            let _ = self.sink.reference_rate(&ctx, &rate);
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
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json")
                .unwrap();
        let snap_bytes = snap_str.as_bytes();

        let mut conn = Connection::new(NoopStreamingSink);
        // Warm-up
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
