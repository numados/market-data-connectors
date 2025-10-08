//! Top-of-book market quote projector over Level 2 order books and native tickers.

use mdc_core::{
    IntegrityAlert, MarketDataKind, MarketEvent, QuoteMode, Symbol, SymbolTag, UnixNanos, VenueId,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::book::{BookError, L2Book};

/// Reason why a market quote is currently unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    /// Not yet synchronized or awaiting initial snapshot / resync completion.
    NotSynchronized,
    /// At least one side of the order book is empty (withdrawal).
    EmptySide,
    /// Order book is crossed (best bid >= best ask).
    CrossedBook,
    /// Configured maximum order book depth cap exceeded.
    ExceededMaxDepth,
    /// Invalid non-positive price or negative volume.
    InvalidPriceOrVolume,
    /// Invalidation triggered by an in-band integrity alert.
    IntegrityAlert(String),
}

/// Projected top-of-book market quote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuoteProjection {
    /// Valid top-of-book quote available.
    Available {
        /// Exchange venue.
        venue: VenueId,
        /// Trading symbol.
        symbol: Symbol,
        /// Consumer tag.
        tag: SymbolTag,
        /// Effective timestamp: older side's retained effective timestamp (exchange or receipt).
        effective_ts: UnixNanos,
        /// Wall-clock ingestion receipt timestamp.
        received_ts: UnixNanos,
        /// Best bid price.
        bid: Decimal,
        /// Best ask price.
        ask: Decimal,
        /// Best bid size/volume.
        bid_size: Decimal,
        /// Best ask size/volume.
        ask_size: Decimal,
    },
    /// Top-of-book quote is unavailable.
    Unavailable {
        /// Exchange venue.
        venue: VenueId,
        /// Trading symbol.
        symbol: Symbol,
        /// Consumer tag.
        tag: SymbolTag,
        /// Wall-clock ingestion receipt timestamp.
        received_ts: UnixNanos,
        /// Reason for unavailability.
        reason: UnavailableReason,
    },
}

/// Projector maintaining top-of-book market quotes driven by canonical [`MarketEvent`] streams.
#[derive(Debug, Clone)]
pub struct TopOfBookProjector {
    mode: QuoteMode,
    symbol: Symbol,
    tag: SymbolTag,
    venue: Option<VenueId>,
    book: L2Book,
    candidate_book: L2Book,
    in_sync: bool,
    bid_effective_ts: Option<UnixNanos>,
    ask_effective_ts: Option<UnixNanos>,
    candidate_bid_ts: Option<UnixNanos>,
    candidate_ask_ts: Option<UnixNanos>,
    last_projection: Option<QuoteProjection>,
}

impl TopOfBookProjector {
    /// Creates a new [`TopOfBookProjector`] driven by Level 2 order book snapshots and deltas.
    pub fn new_l2(symbol: Symbol, tag: SymbolTag, max_depth: Option<usize>) -> Self {
        Self {
            mode: QuoteMode::L2Projection,
            symbol,
            tag,
            venue: None,
            book: L2Book::new(max_depth),
            candidate_book: L2Book::new(max_depth),
            in_sync: false,
            bid_effective_ts: None,
            ask_effective_ts: None,
            candidate_bid_ts: None,
            candidate_ask_ts: None,
            last_projection: None,
        }
    }

    /// Creates a new [`TopOfBookProjector`] driven by native venue [`BookTicker`] events.
    pub fn new_ticker(symbol: Symbol, tag: SymbolTag) -> Self {
        Self {
            mode: QuoteMode::NativeTicker,
            symbol,
            tag,
            venue: None,
            book: L2Book::new(None),
            candidate_book: L2Book::new(None),
            in_sync: true,
            bid_effective_ts: None,
            ask_effective_ts: None,
            candidate_bid_ts: None,
            candidate_ask_ts: None,
            last_projection: None,
        }
    }

    /// Returns `true` if the projector is currently synchronized and healthy.
    pub fn is_in_sync(&self) -> bool {
        self.in_sync
    }

    /// Returns a reference to the active Level 2 order book, if in L2 projection mode.
    pub fn book(&self) -> &L2Book {
        &self.book
    }

    /// Returns the latest emitted [`QuoteProjection`], if any.
    pub fn current_projection(&self) -> Option<&QuoteProjection> {
        self.last_projection.as_ref()
    }

    /// Ingests a canonical market event and returns any resulting [`QuoteProjection`].
    pub fn handle_event(&mut self, event: &MarketEvent<MarketDataKind>) -> Option<QuoteProjection> {
        if event.symbol != self.symbol {
            return None;
        }

        self.venue = Some(event.venue);
        self.tag = event.tag;

        match self.mode {
            QuoteMode::NativeTicker => self.handle_ticker_event(event),
            QuoteMode::L2Projection => self.handle_l2_event(event),
            QuoteMode::ReferenceRate => None,
        }
    }

    fn handle_ticker_event(
        &mut self,
        event: &MarketEvent<MarketDataKind>,
    ) -> Option<QuoteProjection> {
        let venue = event.venue;
        let symbol = event.symbol.clone();
        let tag = event.tag;
        let received_ts = event.received_ts;
        let effective_ts = event.exchange_ts.unwrap_or(received_ts);

        match &event.kind {
            MarketDataKind::IntegrityAlert(alert) => match alert {
                IntegrityAlert::ResyncCompleted { .. } => {
                    self.in_sync = true;
                    None
                }
                _ => {
                    self.in_sync = false;
                    let proj = QuoteProjection::Unavailable {
                        venue,
                        symbol,
                        tag,
                        received_ts,
                        reason: UnavailableReason::IntegrityAlert(format!("{alert:?}")),
                    };
                    self.last_projection = Some(proj.clone());
                    Some(proj)
                }
            },

            MarketDataKind::BookTicker(ticker) => {
                if !self.in_sync {
                    let proj = QuoteProjection::Unavailable {
                        venue,
                        symbol,
                        tag,
                        received_ts,
                        reason: UnavailableReason::NotSynchronized,
                    };
                    self.last_projection = Some(proj.clone());
                    return Some(proj);
                }

                if ticker.best_bid.price <= Decimal::ZERO
                    || ticker.best_bid.volume < Decimal::ZERO
                    || ticker.best_ask.price <= Decimal::ZERO
                    || ticker.best_ask.volume < Decimal::ZERO
                {
                    let proj = QuoteProjection::Unavailable {
                        venue,
                        symbol,
                        tag,
                        received_ts,
                        reason: UnavailableReason::InvalidPriceOrVolume,
                    };
                    self.last_projection = Some(proj.clone());
                    return Some(proj);
                }

                if ticker.best_bid.volume == Decimal::ZERO
                    || ticker.best_ask.volume == Decimal::ZERO
                {
                    let proj = QuoteProjection::Unavailable {
                        venue,
                        symbol,
                        tag,
                        received_ts,
                        reason: UnavailableReason::EmptySide,
                    };
                    self.last_projection = Some(proj.clone());
                    return Some(proj);
                }

                if ticker.best_bid.price >= ticker.best_ask.price {
                    let proj = QuoteProjection::Unavailable {
                        venue,
                        symbol,
                        tag,
                        received_ts,
                        reason: UnavailableReason::CrossedBook,
                    };
                    self.last_projection = Some(proj.clone());
                    return Some(proj);
                }

                let proj = QuoteProjection::Available {
                    venue,
                    symbol,
                    tag,
                    effective_ts,
                    received_ts,
                    bid: ticker.best_bid.price,
                    ask: ticker.best_ask.price,
                    bid_size: ticker.best_bid.volume,
                    ask_size: ticker.best_ask.volume,
                };
                self.last_projection = Some(proj.clone());
                Some(proj)
            }

            _ => None,
        }
    }

    fn handle_l2_event(&mut self, event: &MarketEvent<MarketDataKind>) -> Option<QuoteProjection> {
        let venue = event.venue;
        let symbol = event.symbol.clone();
        let tag = event.tag;
        let received_ts = event.received_ts;
        let event_ts = event.exchange_ts.unwrap_or(received_ts);

        match &event.kind {
            MarketDataKind::IntegrityAlert(alert) => match alert {
                IntegrityAlert::ResyncCompleted { .. } => {
                    // Release candidate book for use
                    if self.candidate_book.is_empty() {
                        self.in_sync = false;
                        let proj = QuoteProjection::Unavailable {
                            venue,
                            symbol,
                            tag,
                            received_ts,
                            reason: UnavailableReason::NotSynchronized,
                        };
                        self.last_projection = Some(proj.clone());
                        return Some(proj);
                    }

                    self.book = self.candidate_book.clone();
                    self.bid_effective_ts = self.candidate_bid_ts;
                    self.ask_effective_ts = self.candidate_ask_ts;
                    self.in_sync = true;

                    let proj = self.compute_available_quote(venue, symbol, tag, received_ts);
                    self.last_projection = Some(proj.clone());
                    Some(proj)
                }

                _ => {
                    self.in_sync = false;
                    self.candidate_book.clear();
                    self.candidate_bid_ts = None;
                    self.candidate_ask_ts = None;
                    let proj = QuoteProjection::Unavailable {
                        venue,
                        symbol,
                        tag,
                        received_ts,
                        reason: UnavailableReason::IntegrityAlert(format!("{alert:?}")),
                    };
                    self.last_projection = Some(proj.clone());
                    Some(proj)
                }
            },

            MarketDataKind::L2Snapshot(snapshot) => {
                match self.candidate_book.apply_snapshot(snapshot) {
                    Ok(()) => {
                        self.candidate_bid_ts = Some(event_ts);
                        self.candidate_ask_ts = Some(event_ts);

                        if self.in_sync {
                            // If already in sync, snapshot updates active book directly
                            self.book = self.candidate_book.clone();
                            self.bid_effective_ts = Some(event_ts);
                            self.ask_effective_ts = Some(event_ts);
                            let proj =
                                self.compute_available_quote(venue, symbol, tag, received_ts);
                            self.last_projection = Some(proj.clone());
                            Some(proj)
                        } else {
                            // Candidate book staged, awaits ResyncCompleted
                            None
                        }
                    }
                    Err(e) => {
                        self.in_sync = false;
                        self.candidate_book.clear();
                        let proj = QuoteProjection::Unavailable {
                            venue,
                            symbol,
                            tag,
                            received_ts,
                            reason: book_error_to_unavailable(e),
                        };
                        self.last_projection = Some(proj.clone());
                        Some(proj)
                    }
                }
            }

            MarketDataKind::L2Delta(delta) => {
                if !self.in_sync {
                    // Update candidate book during resync
                    let _ = self.candidate_book.apply_delta(delta);
                    return None;
                }

                let old_best_bid = self.book.best_bid();
                let old_best_ask = self.book.best_ask();

                let mut staged_book = self.book.clone();
                match staged_book.apply_delta(delta) {
                    Ok(()) => {
                        let new_best_bid = staged_book.best_bid();
                        let new_best_ask = staged_book.best_ask();

                        if new_best_bid != old_best_bid {
                            self.bid_effective_ts = Some(event_ts);
                        }
                        if new_best_ask != old_best_ask {
                            self.ask_effective_ts = Some(event_ts);
                        }

                        self.book = staged_book;
                        self.candidate_book = self.book.clone();

                        let proj = self.compute_available_quote(venue, symbol, tag, received_ts);
                        self.last_projection = Some(proj.clone());
                        Some(proj)
                    }
                    Err(e) => {
                        self.in_sync = false;
                        let proj = QuoteProjection::Unavailable {
                            venue,
                            symbol,
                            tag,
                            received_ts,
                            reason: book_error_to_unavailable(e),
                        };
                        self.last_projection = Some(proj.clone());
                        Some(proj)
                    }
                }
            }

            _ => None,
        }
    }

    fn compute_available_quote(
        &self,
        venue: VenueId,
        symbol: Symbol,
        tag: SymbolTag,
        received_ts: UnixNanos,
    ) -> QuoteProjection {
        let (Some(best_bid), Some(best_ask)) = (self.book.best_bid(), self.book.best_ask()) else {
            return QuoteProjection::Unavailable {
                venue,
                symbol,
                tag,
                received_ts,
                reason: UnavailableReason::EmptySide,
            };
        };

        if best_bid.price >= best_ask.price {
            return QuoteProjection::Unavailable {
                venue,
                symbol,
                tag,
                received_ts,
                reason: UnavailableReason::CrossedBook,
            };
        }

        // Retain older side's effective timestamp
        let effective_ts = match (self.bid_effective_ts, self.ask_effective_ts) {
            (Some(b_ts), Some(a_ts)) => b_ts.min(a_ts),
            (Some(b_ts), None) => b_ts,
            (None, Some(a_ts)) => a_ts,
            (None, None) => received_ts,
        };

        QuoteProjection::Available {
            venue,
            symbol,
            tag,
            effective_ts,
            received_ts,
            bid: best_bid.price,
            ask: best_ask.price,
            bid_size: best_bid.volume,
            ask_size: best_ask.volume,
        }
    }
}

fn book_error_to_unavailable(err: BookError) -> UnavailableReason {
    match err {
        BookError::NonPositivePrice(_) | BookError::NegativeVolume(_) => {
            UnavailableReason::InvalidPriceOrVolume
        }
        BookError::CrossedBook { .. } => UnavailableReason::CrossedBook,
        BookError::MaxDepthExceeded { .. } => UnavailableReason::ExceededMaxDepth,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use mdc_core::{DeliveryStamp, L2Delta, L2Snapshot, PriceLevel, RunId};

    fn make_event(
        kind: MarketDataKind,
        ex_ts: Option<u64>,
        rec_ts: u64,
    ) -> MarketEvent<MarketDataKind> {
        MarketEvent {
            exchange_ts: ex_ts.map(UnixNanos),
            received_ts: UnixNanos(rec_ts),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: 1,
            delivery: DeliveryStamp::new(RunId::from_bytes([1; 16]), 1, 1),
            source_sequence: None,
            kind,
        }
    }

    #[test]
    fn test_top_of_book_lifecycle_and_freshness() {
        let sym = Symbol::from("BTCUSDT");
        let mut proj = TopOfBookProjector::new_l2(sym, 1, Some(10));

        assert!(!proj.is_in_sync());

        // Snapshot arrives: staged into candidate, not yet released
        let snap = L2Snapshot {
            last_update_id: Some(100),
            bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
            asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(2))],
        };
        let snap_ev = make_event(MarketDataKind::L2Snapshot(snap), Some(1000), 1005);
        assert!(proj.handle_event(&snap_ev).is_none());
        assert!(!proj.is_in_sync());

        // ResyncCompleted releases candidate state
        let comp_ev = make_event(
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
                last_update_id: Some(100),
            }),
            None,
            1010,
        );
        let q = proj.handle_event(&comp_ev).expect("released quote");
        match q {
            QuoteProjection::Available {
                bid,
                ask,
                bid_size,
                ask_size,
                effective_ts,
                ..
            } => {
                assert_eq!(bid, Decimal::from(100));
                assert_eq!(ask, Decimal::from(105));
                assert_eq!(bid_size, Decimal::from(1));
                assert_eq!(ask_size, Decimal::from(2));
                assert_eq!(effective_ts, UnixNanos(1000));
            }
            _ => panic!("Expected Available quote"),
        }

        // Deeper delta at price 98 (does not change best bid 100) -> effective_ts is NOT advanced!
        let delta_deep = L2Delta {
            sequence: None,
            bids: vec![PriceLevel::new(Decimal::from(98), Decimal::from(5))],
            asks: vec![],
        };
        let delta_ev1 = make_event(MarketDataKind::L2Delta(delta_deep), Some(1200), 1205);
        let q2 = proj.handle_event(&delta_ev1).expect("quote");
        match q2 {
            QuoteProjection::Available { effective_ts, .. } => {
                assert_eq!(
                    effective_ts,
                    UnixNanos(1000),
                    "Deeper update must not advance top freshness"
                );
            }
            _ => panic!("Expected Available"),
        }

        // Top bid changes from 100 to 101 at ts 1300
        let delta_top = L2Delta {
            sequence: None,
            bids: vec![PriceLevel::new(Decimal::from(101), Decimal::from(3))],
            asks: vec![],
        };
        let delta_ev2 = make_event(MarketDataKind::L2Delta(delta_top), Some(1300), 1305);
        let q3 = proj.handle_event(&delta_ev2).expect("quote");
        match q3 {
            QuoteProjection::Available {
                bid,
                bid_size,
                effective_ts,
                ..
            } => {
                assert_eq!(bid, Decimal::from(101));
                assert_eq!(bid_size, Decimal::from(3));
                // Bid effective ts is 1300, ask effective ts is 1000 -> older is 1000
                assert_eq!(
                    effective_ts,
                    UnixNanos(1000),
                    "Freshness preserves older side age"
                );
            }
            _ => panic!("Expected Available"),
        }

        // Integrity alert invalidates projection
        let alert_ev = make_event(
            MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
                expected: 10,
                received: 12,
            }),
            None,
            1400,
        );
        let q4 = proj.handle_event(&alert_ev).expect("unavailable");
        match q4 {
            QuoteProjection::Unavailable { reason, .. } => {
                assert!(matches!(reason, UnavailableReason::IntegrityAlert(_)));
            }
            _ => panic!("Expected Unavailable"),
        }
        assert!(!proj.is_in_sync());
    }

    #[test]
    fn test_empty_side_emits_withdrawal() {
        let sym = Symbol::from("BTCUSDT");
        let mut proj = TopOfBookProjector::new_l2(sym, 1, None);

        let snap = L2Snapshot {
            last_update_id: None,
            bids: vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
            asks: vec![PriceLevel::new(Decimal::from(105), Decimal::from(1))],
        };
        proj.handle_event(&make_event(
            MarketDataKind::L2Snapshot(snap),
            Some(100),
            100,
        ));
        proj.handle_event(&make_event(
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
                last_update_id: None,
            }),
            None,
            105,
        ));

        // Remove only ask
        let delta = L2Delta {
            sequence: None,
            bids: vec![],
            asks: vec![PriceLevel::new(Decimal::from(105), Decimal::ZERO)],
        };
        let q = proj
            .handle_event(&make_event(MarketDataKind::L2Delta(delta), Some(110), 110))
            .expect("withdrawal");

        match q {
            QuoteProjection::Unavailable { reason, .. } => {
                assert_eq!(reason, UnavailableReason::EmptySide);
            }
            _ => panic!("Expected Unavailable EmptySide"),
        }
    }
}
