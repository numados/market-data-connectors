//! Closure adapter sink for functional event processing.

use super::MarketDataSink;
use crate::model::{MarketDataKind, MarketEvent};
use std::error::Error;

/// Sink that delegates each market event to a provided closure.
pub struct FnSink<F> {
    f: F,
}

impl<F> FnSink<F> {
    /// Creates a new `FnSink` wrapping the given closure.
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F, E> MarketDataSink for FnSink<F>
where
    F: Fn(MarketEvent<MarketDataKind>) -> Result<(), E> + Send + Sync + 'static,
    E: Error + Send + Sync + 'static,
{
    type Error = E;

    fn on_event(&self, event: MarketEvent<MarketDataKind>) -> Result<(), Self::Error> {
        (self.f)(event)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::model::{DeliveryStamp, ReferenceRate, RunId, Symbol, UnixNanos, VenueId};
    use rust_decimal::Decimal;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn fn_sink_executes_closure() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let sink = FnSink::new(move |_event: MarketEvent<MarketDataKind>| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok::<(), std::io::Error>(())
        });

        let event = MarketEvent {
            exchange_ts: None,
            received_ts: UnixNanos::now(),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: 1,
            delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
            source_sequence: None,
            kind: MarketDataKind::ReferenceRate(ReferenceRate::new(Decimal::from(100))),
        };

        sink.on_event(event).expect("on_event");
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn fn_sink_propagates_error() {
        #[derive(Debug, thiserror::Error, PartialEq, Eq)]
        #[error("custom sink error")]
        struct CustomError;

        let sink = FnSink::new(|_event| Err(CustomError));

        let event = MarketEvent {
            exchange_ts: None,
            received_ts: UnixNanos::now(),
            venue: VenueId::Kraken,
            symbol: Symbol::from("ETHUSD"),
            tag: 2,
            delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
            source_sequence: None,
            kind: MarketDataKind::ReferenceRate(ReferenceRate::new(Decimal::from(1))),
        };

        let result = sink.on_event(event);
        assert_eq!(result.unwrap_err(), CustomError);
    }
}
