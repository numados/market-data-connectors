//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Huobi.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RunId, SequenceRange, Side, StreamingSink, Symbol,
    SymbolTag, UnixNanos, VenueId,
};
use serde::de::DeserializeSeed;

use crate::huobi::gzip::GzipDecoder;
use crate::huobi::raw::{HuobiConnectionScratch, HuobiDepthFrameSeed, extract_symbol_from_channel};

/// Harness connection maintaining pre-allocated scratch buffers for benchmarking.
pub struct Connection<S> {
    scratch: HuobiConnectionScratch,
    decoder: GzipDecoder,
    sink: S,
    run_id: RunId,
    symbol: Symbol,
    tag: SymbolTag,
    delivery_ordinal: u64,
}

impl<S: StreamingSink> Connection<S> {
    /// Creates a new connection with pre-allocated scratch and gzip decoder capacity.
    pub fn new(sink: S) -> Self {
        Self::with_run_id(sink, RunId::from_nanos(crate::now_nanos().0 as u128))
    }

    /// Creates a new connection with a specific `RunId`.
    pub fn with_run_id(sink: S, run_id: RunId) -> Self {
        Self {
            scratch: HuobiConnectionScratch::with_capacity(1_024),
            decoder: GzipDecoder::with_capacity(64 * 1024),
            sink,
            run_id,
            symbol: Symbol::from("btcusdt"),
            tag: SymbolTag::default(),
            delivery_ordinal: 0,
        }
    }

    /// Handles a gzip-compressed binary WebSocket frame with zero heap allocations after warm-up.
    pub fn handle_gzip_frame(&mut self, gz_bytes: &[u8]) {
        let decompressed = match self.decoder.decompress(gz_bytes) {
            Ok(d) => d,
            Err(_) => return,
        };
        Self::dispatch_frame(
            &mut self.scratch,
            &mut self.sink,
            &self.symbol,
            self.tag,
            &mut self.delivery_ordinal,
            self.run_id,
            decompressed,
        );
    }

    /// Handles an uncompressed JSON frame slice with zero heap allocations after warm-up.
    pub fn handle_raw_frame(&mut self, frame: &[u8]) {
        Self::dispatch_frame(
            &mut self.scratch,
            &mut self.sink,
            &self.symbol,
            self.tag,
            &mut self.delivery_ordinal,
            self.run_id,
            frame,
        );
    }

    /// Handles an uncompressed JSON frame slice (alias for [`Self::handle_raw_frame`]).
    pub fn handle_frame(&mut self, frame: &[u8]) {
        self.handle_raw_frame(frame);
    }

    fn dispatch_frame(
        scratch: &mut HuobiConnectionScratch,
        sink: &mut S,
        symbol: &Symbol,
        tag: SymbolTag,
        delivery_ordinal: &mut u64,
        run_id: RunId,
        frame: &[u8],
    ) {
        let frame_str = match std::str::from_utf8(frame) {
            Ok(s) => s,
            Err(_) => return,
        };

        let mut deserializer = serde_json::Deserializer::from_str(frame_str);
        let header = match HuobiDepthFrameSeed::new(scratch).deserialize(&mut deserializer) {
            Ok(h) => h,
            Err(_) => return,
        };

        let (sym_str, is_snapshot) = if let Some(ch) = header.ch {
            (extract_symbol_from_channel(ch), false)
        } else if let Some(rep) = header.rep {
            (extract_symbol_from_channel(rep), true)
        } else {
            return;
        };

        if sym_str != Some(symbol.as_str()) {
            return;
        }

        *delivery_ordinal = delivery_ordinal.saturating_add(1);
        let ctx = EventContext {
            venue: VenueId::Huobi,
            symbol,
            tag,
            exchange_ts: header.ts.map(UnixNanos::from_millis),
            received_ts: crate::now_nanos(),
            connection_id: 1,
            delivery: DeliveryStamp::new(run_id, 1, *delivery_ordinal),
            source_sequence: Some(header.mbp.seq_num),
        };

        if is_snapshot {
            let _ = sink.begin_book(&ctx, BookKind::Snapshot);
            for level in &scratch.bids {
                let _ = sink.level(Side::Bid, level.price, level.volume);
            }
            for level in &scratch.asks {
                let _ = sink.level(Side::Ask, level.price, level.volume);
            }
            let _ = sink.end_book(&ctx, None);
        } else {
            let range = header
                .mbp
                .prev_seq_num
                .map(|p| SequenceRange::new(p.saturating_add(1), header.mbp.seq_num));

            let _ = sink.begin_book(&ctx, BookKind::Delta);
            for level in &scratch.bids {
                let _ = sink.level(Side::Bid, level.price, level.volume);
            }
            for level in &scratch.asks {
                let _ = sink.level(Side::Ask, level.price, level.volume);
            }
            let _ = sink.end_book(&ctx, range);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use mdc_testkit::NoopStreamingSink;

    #[test]
    fn test_handle_frame_zero_alloc() {
        let upd_raw = include_bytes!("../../tests/fixtures/huobi/depth_update.json");
        let upd_gz = include_bytes!("../../tests/fixtures/huobi/depth_update.json.gz");
        let snap_gz = include_bytes!("../../tests/fixtures/huobi/order_book_snapshot.json.gz");

        let mut conn = Connection::new(NoopStreamingSink);

        // Warm-up: initialize scratch buffers and decompressor state
        conn.handle_gzip_frame(snap_gz);
        conn.handle_gzip_frame(upd_gz);
        conn.handle_raw_frame(upd_raw);

        // Assert 0 allocations on raw JSON streaming path
        let allocs_raw = allocation_counter::measure(|| {
            conn.handle_raw_frame(upd_raw);
        });
        assert_eq!(
            allocs_raw.count_total, 0,
            "raw streaming path must not allocate after warm-up"
        );

        // Assert 0 allocations on gzip binary streaming path
        let allocs_gz = allocation_counter::measure(|| {
            conn.handle_gzip_frame(upd_gz);
        });
        assert_eq!(
            allocs_gz.count_total, 0,
            "gzip streaming path must not allocate after warm-up"
        );
    }
}
