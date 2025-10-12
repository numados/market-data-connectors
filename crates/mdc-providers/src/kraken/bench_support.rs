//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Kraken.

use mdc_core::{
    BookKind, DeliveryStamp, EventContext, RawLevelText, RunId, Side, StreamingSink, Symbol,
    SymbolTag, VenueId,
};

use crate::kraken::checksum::KrakenChecksumBook;
use crate::kraken::normalize::parse_rfc3339_nanos;
use crate::kraken::raw::{KrakenConnectionScratch, KrakenDepthSeed};
use serde::de::DeserializeSeed;

/// Harness connection maintaining pre-allocated scratch buffers and shadow book for benchmarking.
pub struct Connection<S> {
    scratch: KrakenConnectionScratch,
    book: KrakenChecksumBook,
    sink: S,
    run_id: RunId,
    symbol: Symbol,
    tag: SymbolTag,
    delivery_ordinal: u64,
}

impl<S: StreamingSink> Connection<S> {
    /// Creates a new connection with pre-allocated scratch capacity and initialized shadow book.
    pub fn new(sink: S) -> Self {
        Self {
            scratch: KrakenConnectionScratch::with_capacity(128),
            book: KrakenChecksumBook::new(1000),
            sink,
            run_id: RunId::from_bytes([1; 16]),
            symbol: Symbol::from("BTC/USD"),
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
        let header = match KrakenDepthSeed::new(&mut self.scratch).deserialize(&mut deserializer) {
            Ok(h) => h,
            Err(_) => return,
        };

        let exchange_ts = header.timestamp.and_then(|ts| parse_rfc3339_nanos(ts).ok());

        self.delivery_ordinal += 1;
        let ctx = EventContext {
            venue: VenueId::Kraken,
            symbol: &self.symbol,
            tag: self.tag,
            exchange_ts,
            received_ts: crate::now_nanos(),
            connection_id: 1,
            delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
            source_sequence: None,
        };

        self.book
            .apply_owned(&self.scratch.asks, &self.scratch.bids);
        let _ = self.book.verify(header.checksum);

        let kind = if header.kind == "snapshot" {
            BookKind::Snapshot
        } else {
            BookKind::Delta
        };

        let _ = self.sink.begin_book(&ctx, kind);
        for raw in &self.scratch.bids {
            let _ = self.sink.level_raw(
                Side::Bid,
                raw.price,
                raw.volume,
                RawLevelText {
                    price: raw.price_raw.as_str(),
                    volume: raw.volume_raw.as_str(),
                },
            );
        }
        for raw in &self.scratch.asks {
            let _ = self.sink.level_raw(
                Side::Ask,
                raw.price,
                raw.volume,
                RawLevelText {
                    price: raw.price_raw.as_str(),
                    volume: raw.volume_raw.as_str(),
                },
            );
        }
        let _ = self.sink.end_book(&ctx, None);
    }

    /// Returns a reference to the local shadow book.
    pub fn book(&self) -> &KrakenChecksumBook {
        &self.book
    }

    /// Returns a mutable reference to the local shadow book.
    pub fn book_mut(&mut self) -> &mut KrakenChecksumBook {
        &mut self.book
    }
}
