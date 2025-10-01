//! Downstream consumer sink contracts and reference implementations.

pub mod fn_sink;
pub mod vec_sink;

pub use fn_sink::FnSink;
pub use vec_sink::VecSink;

use crate::model::{MarketDataKind, MarketEvent};

/// Push interface for downstream market data consumption.
///
/// Implemented by channel senders, ring buffer writers, or in-process order book builders.
/// `on_event` runs synchronously on the connector driver task and must be non-blocking.
pub trait MarketDataSink: Send + Sync + 'static {
    /// Error returned when the sink cannot accept an event.
    ///
    /// Sink errors are fatal for the producing connector connection task.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Delivers an event to the downstream sink.
    fn on_event(&self, event: MarketEvent<MarketDataKind>) -> Result<(), Self::Error>;
}
