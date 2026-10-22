//! Ordered reduction of accepted events and delivery faults into display
//! state.
//!
//! The reducer is pure with respect to the terminal: it owns one
//! [`TopOfBookProjector`] per configured symbol, reduces every accepted event
//! exactly once in receive order, and produces immutable display snapshots on
//! demand. It implements no second sequence, resync, or book-integrity
//! algorithm — integrity control belongs to the projector and the provider.

use std::collections::{HashMap, VecDeque};

use mdc_core::{IntegrityAlert, MarketDataKind, MarketEvent, Symbol, SymbolTag};
use mdc_sinks::loss_intolerant::DeliveryFault;
use mdc_sinks::top_of_book::{QuoteProjection, TopOfBookProjector};

use crate::config::{ALERT_HISTORY, SessionConfig};
use crate::display::{
    AlertRecord, DisplaySnapshot, DisplayStatus, DisplayedQuote, LevelView, QuoteView,
    SymbolDisplay,
};
use crate::provider::SessionBounds;

/// Per-symbol reducer state.
struct SymbolState {
    symbol: Symbol,
    status: DisplayStatus,
    projector: TopOfBookProjector,
    retained: Option<QuoteView>,
    unavailability: Option<String>,
}

/// Ordered reducer over the loss-intolerant stream.
pub struct Reducer {
    symbols: Vec<SymbolState>,
    lookup: HashMap<Symbol, usize>,
    provider_label: &'static str,
    session: DisplayStatus,
    failure: Option<String>,
    alerts: VecDeque<AlertRecord>,
    alerts_evicted: usize,
}

impl Reducer {
    /// Builds a reducer for a validated session configuration.
    pub fn new(config: &SessionConfig, bounds: SessionBounds) -> Self {
        let symbols: Vec<SymbolState> = config
            .symbols
            .iter()
            .enumerate()
            .map(|(index, symbol)| SymbolState {
                symbol: symbol.clone(),
                status: DisplayStatus::Loading,
                projector: TopOfBookProjector::new_l2(
                    symbol.clone(),
                    index as SymbolTag,
                    Some(bounds.projector_depth),
                ),
                retained: None,
                unavailability: None,
            })
            .collect();
        let lookup = symbols
            .iter()
            .enumerate()
            .map(|(index, state)| (state.symbol.clone(), index))
            .collect();
        Self {
            symbols,
            lookup,
            provider_label: config.provider.label(),
            session: DisplayStatus::Loading,
            failure: None,
            alerts: VecDeque::new(),
            alerts_evicted: 0,
        }
    }

    /// Reduces one accepted market event in receive order.
    pub fn apply_event(&mut self, event: &MarketEvent<MarketDataKind>) {
        let Some(&index) = self.lookup.get(&event.symbol) else {
            return;
        };
        let projection = self.symbols[index].projector.handle_event(event);
        match &event.kind {
            MarketDataKind::IntegrityAlert(alert) => {
                self.apply_alert(index, alert, event, projection);
            }
            _ => self.apply_projection(index, projection),
        }
    }

    /// Reduces the terminal delivery fault after the accepted FIFO prefix.
    pub fn apply_fault(&mut self, fault: &DeliveryFault) {
        self.session = DisplayStatus::Failed;
        self.failure = Some(fault.reason.to_string());
    }

    /// Applies stream end. `stop_requested` distinguishes operator stop from
    /// unexpected EOF; a fault already recorded keeps precedence.
    pub fn apply_eof(&mut self, stop_requested: bool) {
        if self.session == DisplayStatus::Failed {
            return;
        }
        if stop_requested {
            self.session = DisplayStatus::Stopped;
        } else {
            self.session = DisplayStatus::Failed;
            self.failure = Some("stream ended unexpectedly".to_string());
        }
    }

    /// Marks the session failed with a sanitized orchestration cause.
    pub fn apply_session_failure(&mut self, cause: String) {
        self.session = DisplayStatus::Failed;
        if self.failure.is_none() {
            self.failure = Some(cause);
        }
    }

    /// Builds an immutable display snapshot of the current state.
    pub fn snapshot(&self) -> DisplaySnapshot {
        let symbols = self
            .symbols
            .iter()
            .map(|state| SymbolDisplay {
                symbol: state.symbol.clone(),
                status: state.status,
                quote: state.retained.as_ref().map(|quote| DisplayedQuote {
                    quote: quote.clone(),
                    historical: state.status != DisplayStatus::InSync
                        || self.session == DisplayStatus::Failed,
                }),
                bids: state
                    .projector
                    .book()
                    .bids()
                    .iter()
                    .map(|level| LevelView {
                        price: level.price,
                        size: level.volume,
                    })
                    .collect(),
                asks: state
                    .projector
                    .book()
                    .asks()
                    .iter()
                    .map(|level| LevelView {
                        price: level.price,
                        size: level.volume,
                    })
                    .collect(),
                unavailability: state.unavailability.clone(),
            })
            .collect();
        DisplaySnapshot {
            provider: self.provider_label,
            session: self.session,
            failure: self.failure.clone(),
            symbols,
            alerts: self.alerts.clone(),
            alerts_evicted: self.alerts_evicted,
        }
    }

    fn apply_alert(
        &mut self,
        index: usize,
        alert: &IntegrityAlert,
        event: &MarketEvent<MarketDataKind>,
        projection: Option<QuoteProjection>,
    ) {
        self.push_alert(AlertRecord {
            symbol: Some(event.symbol.clone()),
            summary: alert_summary(alert),
            received_ts: event.received_ts,
        });
        let state = &mut self.symbols[index];
        match alert {
            IntegrityAlert::ResyncStarted => {
                state.status = DisplayStatus::AwaitingSnapshot;
                state.unavailability = None;
            }
            IntegrityAlert::ResyncCompleted { .. } => {
                // In sync only when the projector releases a valid candidate;
                // an invalid or one-sided candidate is unavailable, and no
                // projection keeps awaiting (candidate still buffered).
                match projection {
                    Some(QuoteProjection::Available { .. }) => {
                        state.status = DisplayStatus::InSync;
                        state.unavailability = None;
                    }
                    Some(QuoteProjection::Unavailable { ref reason, .. }) => {
                        state.status = DisplayStatus::Unavailable;
                        state.unavailability = Some(format!("{reason:?}"));
                    }
                    None => {
                        state.status = DisplayStatus::AwaitingSnapshot;
                        state.unavailability = None;
                    }
                }
            }
            IntegrityAlert::ConnectionLost {
                reason: mdc_core::DisconnectKind::Paused,
            } => {
                state.status = DisplayStatus::Paused;
                state.unavailability = None;
            }
            other => {
                state.status = DisplayStatus::Unavailable;
                state.unavailability = Some(alert_summary(other));
            }
        }
        self.retain_quote(index, projection);
    }

    /// Applies a data-event projection: `Available` promotes to `InSync`,
    /// `Unavailable` demotes with its reason, `None` leaves status unchanged
    /// (candidate buffering, trades, reference rates).
    fn apply_projection(&mut self, index: usize, projection: Option<QuoteProjection>) {
        let state = &mut self.symbols[index];
        match projection {
            Some(QuoteProjection::Available { .. }) => {
                state.status = DisplayStatus::InSync;
                state.unavailability = None;
            }
            Some(QuoteProjection::Unavailable { ref reason, .. }) => {
                state.status = DisplayStatus::Unavailable;
                state.unavailability = Some(format!("{reason:?}"));
            }
            None => {}
        }
        self.retain_quote(index, projection);
    }

    /// Retains the last valid projection; retained values become historical
    /// whenever the symbol is not `InSync` (snapshot-time marking).
    fn retain_quote(&mut self, index: usize, projection: Option<QuoteProjection>) {
        if let Some(QuoteProjection::Available {
            bid,
            ask,
            bid_size,
            ask_size,
            effective_ts,
            received_ts,
            ..
        }) = projection.as_ref()
        {
            self.symbols[index].retained = Some(QuoteView {
                bid: *bid,
                ask: *ask,
                bid_size: *bid_size,
                ask_size: *ask_size,
                spread: ask.checked_sub(*bid),
                effective_ts: *effective_ts,
                received_ts: *received_ts,
            });
        }
    }

    fn push_alert(&mut self, record: AlertRecord) {
        if self.alerts.len() >= ALERT_HISTORY {
            self.alerts.pop_front();
            self.alerts_evicted += 1;
        }
        self.alerts.push_back(record);
    }
}

/// Sanitized, human-readable alert summary. Contains venue-state words and
/// numbers only; never URLs, configuration, or credentials.
pub fn alert_summary(alert: &IntegrityAlert) -> String {
    match alert {
        IntegrityAlert::GapDetected { expected, received } => {
            format!("gap detected (expected {expected}, received {received})")
        }
        IntegrityAlert::ChecksumMismatch {
            expected,
            calculated,
        } => {
            format!("checksum mismatch (expected {expected}, calculated {calculated})")
        }
        IntegrityAlert::SilenceTimeout { elapsed_ms } => {
            format!("silence timeout ({elapsed_ms} ms)")
        }
        IntegrityAlert::HeartbeatTimeout { elapsed_ms } => {
            format!("heartbeat timeout ({elapsed_ms} ms)")
        }
        IntegrityAlert::ResyncStarted => "resync started".to_string(),
        IntegrityAlert::ResyncCompleted { last_update_id } => match last_update_id {
            Some(id) => format!("resync completed (aligned to {id})"),
            None => "resync completed".to_string(),
        },
        IntegrityAlert::Quarantined { retry_in_ms } => {
            format!("resync quarantined (retry in {retry_in_ms} ms)")
        }
        IntegrityAlert::SourceError { kind } => format!("source error ({kind:?})"),
        IntegrityAlert::ConnectionLost { reason } => format!("connection lost ({reason:?})"),
        IntegrityAlert::SinkFailure => "sink failure".to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
