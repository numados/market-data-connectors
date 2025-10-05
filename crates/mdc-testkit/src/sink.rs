//! Streaming sink implementations for testing and benchmarking.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use mdc_core::{
    BookKind, BookTicker, EventContext, IntegrityAlert, ReferenceRate, SequenceRange, Side,
    StreamingSink, Symbol, Trade,
};
use rust_decimal::Decimal;

/// No-op streaming sink that discards all market data and alerts.
///
/// Allocates nothing and executes with minimal overhead; suitable for zero-heap benchmarks.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopStreamingSink;

impl StreamingSink for NoopStreamingSink {
    type Error = Infallible;

    #[inline(always)]
    fn begin_book(&mut self, _ctx: &EventContext<'_>, _kind: BookKind) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn level(&mut self, _side: Side, _price: Decimal, _volume: Decimal) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn end_book(
        &mut self,
        _ctx: &EventContext<'_>,
        _seq: Option<SequenceRange>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn trade(&mut self, _ctx: &EventContext<'_>, _trade: &Trade) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn book_ticker(
        &mut self,
        _ctx: &EventContext<'_>,
        _ticker: &BookTicker,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn reference_rate(
        &mut self,
        _ctx: &EventContext<'_>,
        _rate: &ReferenceRate,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline(always)]
    fn alert(
        &mut self,
        _ctx: &EventContext<'_>,
        _alert: IntegrityAlert,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Recorded event or callback from a `RecordingSink`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedEvent {
    /// Start of an order book snapshot or delta.
    BeginBook {
        /// Symbol being updated.
        symbol: Symbol,
        /// Type of book update.
        kind: BookKind,
    },
    /// An individual price level.
    Level {
        /// Book side.
        side: Side,
        /// Price level.
        price: Decimal,
        /// Volume/quantity.
        volume: Decimal,
    },
    /// End of an order book snapshot or delta.
    EndBook {
        /// Symbol being updated.
        symbol: Symbol,
        /// Sequence range covered.
        seq: Option<SequenceRange>,
    },
    /// Public trade.
    Trade {
        /// Symbol.
        symbol: Symbol,
        /// Trade data.
        trade: Trade,
    },
    /// Top-of-book ticker.
    BookTicker {
        /// Symbol.
        symbol: Symbol,
        /// Ticker data.
        ticker: BookTicker,
    },
    /// Reference rate.
    ReferenceRate {
        /// Symbol.
        symbol: Symbol,
        /// Reference rate.
        rate: ReferenceRate,
    },
    /// Integrity alert.
    Alert {
        /// Symbol.
        symbol: Symbol,
        /// Integrity alert.
        alert: IntegrityAlert,
    },
}

/// Thread-safe recording sink that collects all streaming callbacks in arrival order.
#[derive(Debug, Clone, Default)]
pub struct RecordingSink {
    events: Arc<Mutex<Vec<RecordedEvent>>>,
}

impl RecordingSink {
    /// Creates a new, empty `RecordingSink`.
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a snapshot of all recorded events in arrival order.
    pub fn events(&self) -> Vec<RecordedEvent> {
        let guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.clone()
    }

    /// Returns all integrity alerts recorded across all symbols.
    pub fn alerts(&self) -> Vec<IntegrityAlert> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                RecordedEvent::Alert { alert, .. } => Some(alert),
                _ => None,
            })
            .collect()
    }

    /// Returns all integrity alerts recorded for a specific symbol.
    pub fn alerts_for(&self, symbol: &str) -> Vec<IntegrityAlert> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                RecordedEvent::Alert {
                    symbol: s, alert, ..
                } if s.as_str() == symbol => Some(alert),
                _ => None,
            })
            .collect()
    }

    /// Clears all recorded events.
    pub fn clear(&self) {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.clear();
    }

    /// Returns the number of events recorded.
    pub fn len(&self) -> usize {
        let guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.len()
    }

    /// Returns `true` if no events have been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl StreamingSink for RecordingSink {
    type Error = Infallible;

    fn begin_book(&mut self, ctx: &EventContext<'_>, kind: BookKind) -> Result<(), Self::Error> {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(RecordedEvent::BeginBook {
            symbol: ctx.symbol.clone(),
            kind,
        });
        Ok(())
    }

    fn level(&mut self, side: Side, price: Decimal, volume: Decimal) -> Result<(), Self::Error> {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(RecordedEvent::Level {
            side,
            price,
            volume,
        });
        Ok(())
    }

    fn end_book(
        &mut self,
        ctx: &EventContext<'_>,
        seq: Option<SequenceRange>,
    ) -> Result<(), Self::Error> {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(RecordedEvent::EndBook {
            symbol: ctx.symbol.clone(),
            seq,
        });
        Ok(())
    }

    fn trade(&mut self, ctx: &EventContext<'_>, trade: &Trade) -> Result<(), Self::Error> {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(RecordedEvent::Trade {
            symbol: ctx.symbol.clone(),
            trade: trade.clone(),
        });
        Ok(())
    }

    fn book_ticker(
        &mut self,
        ctx: &EventContext<'_>,
        ticker: &BookTicker,
    ) -> Result<(), Self::Error> {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(RecordedEvent::BookTicker {
            symbol: ctx.symbol.clone(),
            ticker: *ticker,
        });
        Ok(())
    }

    fn reference_rate(
        &mut self,
        ctx: &EventContext<'_>,
        rate: &ReferenceRate,
    ) -> Result<(), Self::Error> {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(RecordedEvent::ReferenceRate {
            symbol: ctx.symbol.clone(),
            rate: *rate,
        });
        Ok(())
    }

    fn alert(&mut self, ctx: &EventContext<'_>, alert: IntegrityAlert) -> Result<(), Self::Error> {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(RecordedEvent::Alert {
            symbol: ctx.symbol.clone(),
            alert,
        });
        Ok(())
    }
}
