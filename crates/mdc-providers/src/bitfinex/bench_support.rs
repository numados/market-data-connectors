//! Benchmark support harness demonstrating hot-path zero-heap frame processing for Bitfinex.

use mdc_core::model::{DeliveryStamp, RunId, Side, Symbol, SymbolTag, UnixNanos, VenueId};
use mdc_core::sink::{BookKind, EventContext, RawLevelText, StreamingSink};

use crate::bitfinex::checksum::BitfinexChecksumBook;
use crate::bitfinex::normalize::normalize_book_level;
use crate::bitfinex::raw::{
    BitfinexConnectionScratch, RawBookLevelOwned, RawChannelFrame, RawFrame, RawPayload,
};

/// Harness connection maintaining pre-allocated scratch buffers and shadow book for benchmarking.
pub struct Connection<S> {
    scratch: BitfinexConnectionScratch,
    book: BitfinexChecksumBook,
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
            scratch: BitfinexConnectionScratch::new(),
            book: BitfinexChecksumBook::new(25),
            sink,
            run_id: RunId::from_u128(1),
            symbol: Symbol::from("tBTCUSD"),
            tag: 0,
            delivery_ordinal: 0,
        }
    }

    /// Access the underlying shadow checksum book.
    pub fn book_mut(&mut self) -> &mut BitfinexChecksumBook {
        &mut self.book
    }

    /// Handles a raw JSON frame slice with zero heap allocations after warm-up.
    pub fn handle_frame(&mut self, frame: &[u8]) {
        let frame_str = match std::str::from_utf8(frame) {
            Ok(s) => s,
            Err(_) => return,
        };

        let parsed = match RawFrame::parse(frame_str) {
            Ok(p) => p,
            Err(_) => return,
        };

        if let RawFrame::Channel(RawChannelFrame { payload, ts, .. }) = parsed {
            self.delivery_ordinal += 1;
            let ctx = EventContext {
                venue: VenueId::Bitfinex,
                symbol: &self.symbol,
                tag: self.tag,
                exchange_ts: ts.map(UnixNanos::from_millis),
                received_ts: crate::now_nanos(),
                connection_id: 1,
                delivery: DeliveryStamp::new(self.run_id, 1, self.delivery_ordinal),
                source_sequence: None,
            };

            match payload {
                RawPayload::BookUpdate(level) => {
                    let _ = self.book.apply(&level);
                    if let Ok((side, pl)) = normalize_book_level(&level) {
                        let _ = self.sink.begin_book(&ctx, BookKind::Delta);
                        let raw_text = RawLevelText {
                            price: level.price_str(),
                            volume: level.amount_str(),
                        };
                        let _ = self.sink.level_raw(side, pl.price, pl.volume, raw_text);
                        let _ = self.sink.end_book(&ctx, None);
                    }
                }
                RawPayload::BookSnapshot(levels) => {
                    self.book.clear();
                    self.scratch.clear();
                    for l in &levels {
                        let _ = self.book.apply(l);
                        if let Ok(owned) = RawBookLevelOwned::from_raw(l) {
                            self.scratch.levels.push(owned);
                        }
                    }

                    let _ = self.sink.begin_book(&ctx, BookKind::Snapshot);
                    for l in &self.scratch.levels {
                        let side = if l.amount.is_sign_positive() {
                            Side::Bid
                        } else {
                            Side::Ask
                        };
                        let raw_text = RawLevelText {
                            price: l.price_raw.as_str(),
                            volume: l.amount_raw.as_str(),
                        };
                        let _ = self.sink.level_raw(side, l.price, l.amount.abs(), raw_text);
                    }
                    let _ = self.sink.end_book(&ctx, None);
                }
                _ => {}
            }
        }
    }
}
