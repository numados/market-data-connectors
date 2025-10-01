//! Vector-backed recording sink for testing and offline inspection.

use super::MarketDataSink;
use crate::model::{MarketDataKind, MarketEvent};
use std::convert::Infallible;
use std::sync::Mutex;

/// In-memory sink recording events into a thread-safe `Vec`.
/// Intended for unit tests, offline conformance runners, and debug instrumentation.
#[derive(Debug, Default)]
pub struct VecSink {
    events: Mutex<Vec<MarketEvent<MarketDataKind>>>,
}

impl VecSink {
    /// Creates a new, empty `VecSink`.
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    /// Returns a cloned copy of all recorded events in arrival order.
    pub fn events(&self) -> Vec<MarketEvent<MarketDataKind>> {
        let guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.clone()
    }

    /// Clears all recorded events from the sink.
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

impl MarketDataSink for VecSink {
    type Error = Infallible;

    fn on_event(&self, event: MarketEvent<MarketDataKind>) -> Result<(), Self::Error> {
        let mut guard = self
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(event);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::model::{DeliveryStamp, ReferenceRate, RunId, Symbol, UnixNanos, VenueId};
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use std::thread;

    fn dummy_event(ordinal: u64) -> MarketEvent<MarketDataKind> {
        MarketEvent {
            exchange_ts: None,
            received_ts: UnixNanos::from_millis(ordinal),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: ordinal,
            delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, ordinal),
            source_sequence: Some(ordinal),
            kind: MarketDataKind::ReferenceRate(ReferenceRate::new(Decimal::from(ordinal))),
        }
    }

    #[test]
    fn vec_sink_records_and_clears() {
        let sink = VecSink::new();
        assert!(sink.is_empty());

        sink.on_event(dummy_event(1)).expect("send 1");
        sink.on_event(dummy_event(2)).expect("send 2");

        assert_eq!(sink.len(), 2);
        let recorded = sink.events();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].tag, 1);
        assert_eq!(recorded[1].tag, 2);

        sink.clear();
        assert!(sink.is_empty());
    }

    #[test]
    fn vec_sink_thread_safety() {
        let sink = Arc::new(VecSink::new());
        let mut handles = Vec::new();

        for t in 0..4 {
            let sink_clone = Arc::clone(&sink);
            handles.push(thread::spawn(move || {
                for i in 0..25 {
                    sink_clone
                        .on_event(dummy_event(t * 25 + i))
                        .expect("on_event");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread join");
        }

        assert_eq!(sink.len(), 100);
    }
}
