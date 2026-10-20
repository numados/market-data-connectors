//! Streaming sink implementations for testing and benchmarking.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use mdc_core::{
    BookKind, BookTicker, EventContext, IntegrityAlert, ReferenceRate, SequenceRange, Side,
    StreamingSink, Symbol, Trade,
};
use rust_decimal::Decimal;

use crate::barrier::wait_until;

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
    stamps: Arc<Mutex<Vec<mdc_core::DeliveryStamp>>>,
    notify: Arc<tokio::sync::Notify>,
}

impl RecordingSink {
    /// Creates a new, empty `RecordingSink`.
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            stamps: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(tokio::sync::Notify::new()),
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

    /// Returns a snapshot of all recorded delivery stamps in arrival order.
    pub fn delivery_stamps(&self) -> Vec<mdc_core::DeliveryStamp> {
        let guard = self
            .stamps
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

    /// Asynchronously waits until at least `count` events have been recorded, returning all recorded events.
    pub async fn wait_for_events(&self, count: usize) -> Vec<RecordedEvent> {
        wait_until(&self.notify, || {
            let events = self.events();
            (events.len() >= count).then_some(events)
        })
        .await
    }

    /// Asynchronously waits until at least `count` alerts have been recorded, returning all recorded alerts.
    pub async fn wait_for_alerts(&self, count: usize) -> Vec<IntegrityAlert> {
        wait_until(&self.notify, || {
            let alerts = self.alerts();
            (alerts.len() >= count).then_some(alerts)
        })
        .await
    }

    /// Asynchronously waits until at least `count` alerts have been recorded for a specific symbol.
    pub async fn wait_for_alerts_for(&self, symbol: &str, count: usize) -> Vec<IntegrityAlert> {
        wait_until(&self.notify, || {
            let alerts = self.alerts_for(symbol);
            (alerts.len() >= count).then_some(alerts)
        })
        .await
    }

    /// Asynchronously waits until an event matching `predicate` is recorded and returns it.
    pub async fn wait_for_event_matching<F>(&self, predicate: F) -> RecordedEvent
    where
        F: Fn(&RecordedEvent) -> bool,
    {
        wait_until(&self.notify, || self.events().into_iter().find(&predicate)).await
    }

    /// Asynchronously waits until an alert matching `predicate` is recorded and returns it.
    pub async fn wait_for_alert_matching<F>(&self, predicate: F) -> IntegrityAlert
    where
        F: Fn(&IntegrityAlert) -> bool,
    {
        wait_until(&self.notify, || self.alerts().into_iter().find(&predicate)).await
    }

    /// Asynchronously waits until at least `count` delivery stamps have been recorded.
    pub async fn wait_for_delivery_stamps(&self, count: usize) -> Vec<mdc_core::DeliveryStamp> {
        wait_until(&self.notify, || {
            let stamps = self.delivery_stamps();
            (stamps.len() >= count).then_some(stamps)
        })
        .await
    }

    /// Asynchronously waits until at least `count` trades have been recorded.
    pub async fn wait_for_trades(&self, count: usize) -> Vec<mdc_core::Trade> {
        wait_until(&self.notify, || {
            let trades: Vec<_> = self
                .events()
                .into_iter()
                .filter_map(|e| match e {
                    RecordedEvent::Trade { trade, .. } => Some(trade),
                    _ => None,
                })
                .collect();
            (trades.len() >= count).then_some(trades)
        })
        .await
    }

    /// Clears all recorded events and delivery stamps.
    pub fn clear(&self) {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.clear();
        let mut stamp_guard = self
            .stamps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stamp_guard.clear();
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
        let mut stamp_guard = self
            .stamps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stamp_guard.push(ctx.delivery);
        drop(guard);
        drop(stamp_guard);
        self.notify.notify_waiters();
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
        drop(guard);
        self.notify.notify_waiters();
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
        drop(guard);
        self.notify.notify_waiters();
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
        let mut stamp_guard = self
            .stamps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stamp_guard.push(ctx.delivery);
        drop(guard);
        drop(stamp_guard);
        self.notify.notify_waiters();
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
        let mut stamp_guard = self
            .stamps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stamp_guard.push(ctx.delivery);
        drop(guard);
        drop(stamp_guard);
        self.notify.notify_waiters();
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
        let mut stamp_guard = self
            .stamps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stamp_guard.push(ctx.delivery);
        drop(guard);
        drop(stamp_guard);
        self.notify.notify_waiters();
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
        let mut stamp_guard = self
            .stamps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        stamp_guard.push(ctx.delivery);
        drop(guard);
        drop(stamp_guard);
        self.notify.notify_waiters();
        Ok(())
    }
}

/// Target streaming callback where a fault should be injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultInjectionPoint {
    /// Never inject a fault (acts as normal recording sink).
    Never,
    /// Fail immediately on `begin_book`.
    BeginBook,
    /// Fail on the Nth `level` (1-indexed).
    Level(usize),
    /// Fail on `end_book`.
    EndBook,
    /// Fail on `trade`.
    Trade,
    /// Fail on `book_ticker`.
    BookTicker,
    /// Fail on `reference_rate`.
    ReferenceRate,
    /// Fail on `alert`.
    Alert,
    /// Fail on the Nth callback overall (1-indexed across all callback types).
    NthCallback(usize),
}

/// Error type returned by [`FaultInjectingSink`].
#[derive(Debug, Clone, thiserror::Error)]
#[error("Fault injected by test sink at {point:?}")]
pub struct InjectedSinkError {
    /// The injection point that triggered this error.
    pub point: FaultInjectionPoint,
}

/// A streaming sink that injects an error at a configured callback point
/// and records all events accepted before and after the failure.
#[derive(Debug, Clone)]
pub struct FaultInjectingSink {
    inner: RecordingSink,
    fault_point: FaultInjectionPoint,
    level_count: Arc<Mutex<usize>>,
    callback_count: Arc<Mutex<usize>>,
    invocations: Arc<AtomicUsize>,
    tripped: Arc<Mutex<bool>>,
}

impl FaultInjectingSink {
    /// Creates a new `FaultInjectingSink` with the specified injection point.
    pub fn new(fault_point: FaultInjectionPoint) -> Self {
        Self {
            inner: RecordingSink::new(),
            fault_point,
            level_count: Arc::new(Mutex::new(0)),
            callback_count: Arc::new(Mutex::new(0)),
            invocations: Arc::new(AtomicUsize::new(0)),
            tripped: Arc::new(Mutex::new(false)),
        }
    }

    /// Creates a new `FaultInjectingSink` that fails on the `n`-th callback overall.
    pub fn fail_at_callback(n: usize) -> Self {
        Self::new(FaultInjectionPoint::NthCallback(n))
    }

    /// Returns all events recorded by the inner sink.
    pub fn events(&self) -> Vec<RecordedEvent> {
        self.inner.events()
    }

    /// Returns all alerts recorded by the inner sink.
    pub fn alerts(&self) -> Vec<IntegrityAlert> {
        self.inner.alerts()
    }

    /// Returns all alerts recorded for a specific symbol.
    pub fn alerts_for(&self, symbol: &str) -> Vec<IntegrityAlert> {
        self.inner.alerts_for(symbol)
    }

    /// Returns all delivery stamps recorded by the inner sink.
    pub fn delivery_stamps(&self) -> Vec<mdc_core::DeliveryStamp> {
        self.inner.delivery_stamps()
    }

    /// Returns `true` if the configured fault has tripped.
    pub fn is_tripped(&self) -> bool {
        *self.tripped.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Returns the number of data and alert callbacks attempted, excluding the
    /// terminal `SinkFailure` alert that connectors emit after a failure.
    pub fn callback_invocations(&self) -> usize {
        self.invocations.load(Ordering::SeqCst)
    }

    /// Returns the number of level invocations attempted.
    pub fn level_invocations(&self) -> usize {
        *self.level_count.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Asynchronously waits until at least `count` events have been recorded.
    pub async fn wait_for_events(&self, count: usize) -> Vec<RecordedEvent> {
        self.inner.wait_for_events(count).await
    }

    /// Asynchronously waits until at least `count` alerts have been recorded.
    pub async fn wait_for_alerts(&self, count: usize) -> Vec<IntegrityAlert> {
        self.inner.wait_for_alerts(count).await
    }

    /// Asynchronously waits until at least `count` alerts have been recorded for a specific symbol.
    pub async fn wait_for_alerts_for(&self, symbol: &str, count: usize) -> Vec<IntegrityAlert> {
        self.inner.wait_for_alerts_for(symbol, count).await
    }

    fn check_callback_fault(&self) -> Result<(), InjectedSinkError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        if let FaultInjectionPoint::NthCallback(n) = self.fault_point {
            let mut count = self
                .callback_count
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            *count += 1;
            if *count == n {
                let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
                if !*tripped {
                    *tripped = true;
                    return Err(InjectedSinkError {
                        point: FaultInjectionPoint::NthCallback(n),
                    });
                }
            }
        }
        Ok(())
    }
}

impl StreamingSink for FaultInjectingSink {
    type Error = InjectedSinkError;

    fn begin_book(&mut self, ctx: &EventContext<'_>, kind: BookKind) -> Result<(), Self::Error> {
        self.check_callback_fault()?;
        if self.fault_point == FaultInjectionPoint::BeginBook {
            let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
            if !*tripped {
                *tripped = true;
                return Err(InjectedSinkError {
                    point: FaultInjectionPoint::BeginBook,
                });
            }
        }
        let _ = self.inner.begin_book(ctx, kind);
        Ok(())
    }

    fn level(&mut self, side: Side, price: Decimal, volume: Decimal) -> Result<(), Self::Error> {
        self.check_callback_fault()?;
        if let FaultInjectionPoint::Level(n) = self.fault_point {
            let mut count = self.level_count.lock().unwrap_or_else(|p| p.into_inner());
            *count += 1;
            if *count == n {
                let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
                if !*tripped {
                    *tripped = true;
                    return Err(InjectedSinkError {
                        point: FaultInjectionPoint::Level(n),
                    });
                }
            }
        }
        let _ = self.inner.level(side, price, volume);
        Ok(())
    }

    fn level_raw(
        &mut self,
        side: Side,
        price: Decimal,
        volume: Decimal,
        raw: mdc_core::RawLevelText<'_>,
    ) -> Result<(), Self::Error> {
        self.check_callback_fault()?;
        if let FaultInjectionPoint::Level(n) = self.fault_point {
            let mut count = self.level_count.lock().unwrap_or_else(|p| p.into_inner());
            *count += 1;
            if *count == n {
                let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
                if !*tripped {
                    *tripped = true;
                    return Err(InjectedSinkError {
                        point: FaultInjectionPoint::Level(n),
                    });
                }
            }
        }
        let _ = self.inner.level_raw(side, price, volume, raw);
        Ok(())
    }

    fn end_book(
        &mut self,
        ctx: &EventContext<'_>,
        seq: Option<SequenceRange>,
    ) -> Result<(), Self::Error> {
        self.check_callback_fault()?;
        if self.fault_point == FaultInjectionPoint::EndBook {
            let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
            if !*tripped {
                *tripped = true;
                return Err(InjectedSinkError {
                    point: FaultInjectionPoint::EndBook,
                });
            }
        }
        let _ = self.inner.end_book(ctx, seq);
        Ok(())
    }

    fn trade(&mut self, ctx: &EventContext<'_>, trade: &Trade) -> Result<(), Self::Error> {
        self.check_callback_fault()?;
        if self.fault_point == FaultInjectionPoint::Trade {
            let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
            if !*tripped {
                *tripped = true;
                return Err(InjectedSinkError {
                    point: FaultInjectionPoint::Trade,
                });
            }
        }
        let _ = self.inner.trade(ctx, trade);
        Ok(())
    }

    fn book_ticker(
        &mut self,
        ctx: &EventContext<'_>,
        ticker: &BookTicker,
    ) -> Result<(), Self::Error> {
        self.check_callback_fault()?;
        if self.fault_point == FaultInjectionPoint::BookTicker {
            let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
            if !*tripped {
                *tripped = true;
                return Err(InjectedSinkError {
                    point: FaultInjectionPoint::BookTicker,
                });
            }
        }
        let _ = self.inner.book_ticker(ctx, ticker);
        Ok(())
    }

    fn reference_rate(
        &mut self,
        ctx: &EventContext<'_>,
        rate: &ReferenceRate,
    ) -> Result<(), Self::Error> {
        self.check_callback_fault()?;
        if self.fault_point == FaultInjectionPoint::ReferenceRate {
            let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
            if !*tripped {
                *tripped = true;
                return Err(InjectedSinkError {
                    point: FaultInjectionPoint::ReferenceRate,
                });
            }
        }
        let _ = self.inner.reference_rate(ctx, rate);
        Ok(())
    }

    fn alert(&mut self, ctx: &EventContext<'_>, alert: IntegrityAlert) -> Result<(), Self::Error> {
        if alert != IntegrityAlert::SinkFailure {
            self.check_callback_fault()?;
        }
        if self.fault_point == FaultInjectionPoint::Alert {
            let mut tripped = self.tripped.lock().unwrap_or_else(|p| p.into_inner());
            if !*tripped {
                *tripped = true;
                return Err(InjectedSinkError {
                    point: FaultInjectionPoint::Alert,
                });
            }
        }
        let _ = self.inner.alert(ctx, alert);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use mdc_core::{DeliveryStamp, RunId, SymbolTag, UnixNanos, VenueId};

    fn make_ctx(sym: &Symbol, delivery: DeliveryStamp) -> EventContext<'_> {
        EventContext {
            venue: VenueId::Binance,
            symbol: sym,
            tag: SymbolTag::default(),
            exchange_ts: None,
            received_ts: UnixNanos(100),
            connection_id: 1,
            delivery,
            source_sequence: None,
        }
    }

    #[test]
    fn fault_injecting_sink_triggers_at_points() {
        let sym = Symbol::from("BTCUSDT");
        let run = RunId::from_u128(1);
        let stamp = DeliveryStamp::new(run, 1, 1);
        let ctx = make_ctx(&sym, stamp);

        // Fail at begin_book
        let mut sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        assert!(sink.begin_book(&ctx, BookKind::Snapshot).is_err());
        assert!(sink.is_tripped());

        // Fail at level 2
        let mut sink = FaultInjectingSink::new(FaultInjectionPoint::Level(2));
        assert!(sink.begin_book(&ctx, BookKind::Snapshot).is_ok());
        assert!(sink.level(Side::Bid, Decimal::ONE, Decimal::ONE).is_ok());
        assert!(!sink.is_tripped());
        assert!(sink.level(Side::Ask, Decimal::ONE, Decimal::ONE).is_err());
        assert!(sink.is_tripped());

        // Fail at end_book
        let mut sink = FaultInjectingSink::new(FaultInjectionPoint::EndBook);
        assert!(sink.begin_book(&ctx, BookKind::Delta).is_ok());
        assert!(sink.end_book(&ctx, None).is_err());
        assert!(sink.is_tripped());

        // Fail at trade
        let mut sink = FaultInjectingSink::new(FaultInjectionPoint::Trade);
        let trade = Trade {
            identity: None,
            price: Decimal::ONE,
            volume: Decimal::ONE,
            aggressor: Side::Bid,
        };
        assert!(sink.trade(&ctx, &trade).is_err());
        assert!(sink.is_tripped());
    }

    #[test]
    fn recording_sink_records_delivery_stamps() {
        let sym = Symbol::from("BTCUSDT");
        let run = RunId::from_u128(1);
        let stamp1 = DeliveryStamp::new(run, 1, 1);
        let stamp2 = DeliveryStamp::new(run, 1, 2);

        let mut sink = RecordingSink::new();
        let ctx1 = make_ctx(&sym, stamp1);
        sink.begin_book(&ctx1, BookKind::Snapshot).unwrap();

        let ctx2 = make_ctx(&sym, stamp2);
        sink.alert(&ctx2, IntegrityAlert::ResyncStarted).unwrap();

        let stamps = sink.delivery_stamps();
        assert_eq!(stamps.len(), 2);
        assert_eq!(stamps[0], stamp1);
        assert_eq!(stamps[1], stamp2);
    }

    #[test]
    fn test_fault_injection_nth_callback() {
        let sym = Symbol::from("BTCUSDT");
        let run = RunId::from_u128(1);
        let stamp = DeliveryStamp::new(run, 1, 1);
        let ctx = make_ctx(&sym, stamp);

        // Fail at the 3rd callback
        let mut sink = FaultInjectingSink::fail_at_callback(3);
        assert!(sink.begin_book(&ctx, BookKind::Snapshot).is_ok());
        assert!(!sink.is_tripped());
        assert!(sink.level(Side::Bid, Decimal::ONE, Decimal::ONE).is_ok());
        assert!(!sink.is_tripped());
        assert!(sink.end_book(&ctx, None).is_err());
        assert!(sink.is_tripped());

        // Subsequent calls succeed so error alert can be recorded
        assert!(sink.alert(&ctx, IntegrityAlert::SinkFailure).is_ok());
        assert_eq!(sink.alerts(), vec![IntegrityAlert::SinkFailure]);
    }

    #[tokio::test]
    async fn test_recording_sink_readiness_barriers() {
        let sink = RecordingSink::new();
        let sym = Symbol::from("BTCUSDT");
        let run = RunId::from_u128(1);
        let stamp = DeliveryStamp::new(run, 1, 1);

        let mut sink_writer = sink.clone();
        tokio::spawn(async move {
            let ctx = make_ctx(&sym, stamp);
            sink_writer.begin_book(&ctx, BookKind::Snapshot).unwrap();
            sink_writer
                .level(Side::Bid, Decimal::ONE, Decimal::ONE)
                .unwrap();
            sink_writer
                .alert(&ctx, IntegrityAlert::ResyncStarted)
                .unwrap();
        });

        let alerts = sink.wait_for_alerts(1).await;
        assert_eq!(alerts, vec![IntegrityAlert::ResyncStarted]);

        let alert = sink
            .wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncStarted))
            .await;
        assert_eq!(alert, IntegrityAlert::ResyncStarted);

        let event = sink
            .wait_for_event_matching(|e| {
                matches!(
                    e,
                    RecordedEvent::BeginBook {
                        kind: BookKind::Snapshot,
                        ..
                    }
                )
            })
            .await;
        assert!(matches!(
            event,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        ));

        let stamps = sink.wait_for_delivery_stamps(2).await;
        assert_eq!(stamps.len(), 2);

        let events = sink.wait_for_events(3).await;
        assert_eq!(events.len(), 3);
    }
}
