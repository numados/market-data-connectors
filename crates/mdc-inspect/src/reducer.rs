//! Ordered reduction of accepted events and delivery faults into display
//! state.
//!
//! The reducer is pure with respect to the terminal: it owns one
//! [`TopOfBookProjector`] per configured L2 symbol and separate retained state
//! for reference-rate symbols, reduces every accepted event exactly once in
//! receive order, and produces immutable display snapshots on demand. It
//! implements no second sequence, resync, or book-integrity algorithm —
//! integrity control belongs to the projector and the provider.

use std::collections::{HashMap, VecDeque};

use mdc_core::{IntegrityAlert, MarketDataKind, MarketEvent, Symbol, SymbolTag, UnixNanos};
use mdc_sinks::loss_intolerant::DeliveryFault;
use mdc_sinks::top_of_book::{QuoteProjection, TopOfBookProjector};

use crate::config::{ALERT_HISTORY, SessionConfig};
use crate::display::{
    AlertRecord, DisplaySnapshot, DisplayStatus, DisplayedQuote, DisplayedRate, LevelView,
    QuoteView, RateView, SymbolDisplay,
};
use crate::provider::SessionBounds;

/// Data-mode-specific state owned by one configured symbol.
///
/// Reference rates do not have a book projector. Keeping the modes separate
/// prevents rate alerts from being interpreted as L2 integrity events.
enum SymbolData {
    Book {
        projector: Box<TopOfBookProjector>,
        retained: Option<QuoteView>,
    },
    Rate {
        retained: Option<RateView>,
    },
}

/// Per-symbol reducer state.
struct SymbolState {
    symbol: Symbol,
    status: DisplayStatus,
    data: SymbolData,
    unavailability: Option<String>,
}

/// Ordered reducer over the loss-intolerant stream.
pub struct Reducer {
    symbols: Vec<SymbolState>,
    lookup: HashMap<Symbol, usize>,
    provider_label: &'static str,
    presentation: crate::display::ProviderPresentation,
    session: DisplayStatus,
    failure: Option<String>,
    alerts: VecDeque<AlertRecord>,
    alerts_evicted: usize,
}

impl Reducer {
    /// Builds a reducer for a validated session configuration.
    pub fn new(config: &SessionConfig, bounds: SessionBounds) -> Self {
        let reference =
            config.provider.presentation().mode == crate::display::DisplayMode::ReferenceRate;
        let symbols: Vec<SymbolState> = config
            .symbols
            .iter()
            .enumerate()
            .map(|(index, symbol)| {
                let display_symbol = if reference {
                    config
                        .base
                        .as_ref()
                        .map(|base| Symbol::from(format!("{base}{symbol}").as_str()))
                        .unwrap_or_else(|| symbol.clone())
                } else {
                    symbol.clone()
                };
                let data = if reference {
                    SymbolData::Rate { retained: None }
                } else {
                    SymbolData::Book {
                        projector: Box::new(TopOfBookProjector::new_l2(
                            display_symbol.clone(),
                            index as SymbolTag,
                            Some(bounds.projector_depth),
                        )),
                        retained: None,
                    }
                };
                SymbolState {
                    symbol: display_symbol,
                    status: DisplayStatus::Loading,
                    data,
                    unavailability: None,
                }
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
            presentation: config.provider.presentation(),
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
        let Some(state) = self.symbols.get_mut(index) else {
            return;
        };
        match &event.kind {
            MarketDataKind::ReferenceRate(_) => self.apply_rate(index, event),
            MarketDataKind::IntegrityAlert(alert)
                if matches!(&state.data, SymbolData::Rate { .. }) =>
            {
                self.apply_rate_alert(index, alert, event);
            }
            MarketDataKind::IntegrityAlert(alert) => {
                let projection = match &mut state.data {
                    SymbolData::Book { projector, .. } => projector.handle_event(event),
                    SymbolData::Rate { .. } => None,
                };
                self.apply_alert(index, alert, event, projection);
            }
            _ => {
                let projection = match &mut state.data {
                    SymbolData::Book { projector, .. } => projector.handle_event(event),
                    SymbolData::Rate { .. } => None,
                };
                self.apply_projection(index, projection);
            }
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
        if matches!(self.session, DisplayStatus::Failed | DisplayStatus::Stopped) {
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
        if matches!(self.session, DisplayStatus::Failed | DisplayStatus::Stopped) {
            return;
        }
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
            .map(|state| {
                let status = match self.session {
                    DisplayStatus::Stopped | DisplayStatus::Failed => self.session,
                    _ => state.status,
                };
                let historical = status != DisplayStatus::InSync;
                let (quote, rate, bids, asks) = match &state.data {
                    SymbolData::Book {
                        projector,
                        retained,
                    } => (
                        retained.as_ref().map(|quote| DisplayedQuote {
                            quote: quote.clone(),
                            historical,
                        }),
                        None,
                        projector
                            .book()
                            .bids()
                            .iter()
                            .map(|level| LevelView {
                                price: level.price,
                                size: level.volume,
                            })
                            .collect(),
                        projector
                            .book()
                            .asks()
                            .iter()
                            .map(|level| LevelView {
                                price: level.price,
                                size: level.volume,
                            })
                            .collect(),
                    ),
                    SymbolData::Rate { retained } => (
                        None,
                        retained.as_ref().map(|rate| DisplayedRate {
                            rate: rate.clone(),
                            historical,
                        }),
                        Vec::new(),
                        Vec::new(),
                    ),
                };
                SymbolDisplay {
                    symbol: state.symbol.clone(),
                    status,
                    quote,
                    rate,
                    bids,
                    asks,
                    unavailability: state.unavailability.clone(),
                }
            })
            .collect();
        DisplaySnapshot {
            provider: self.provider_label,
            presentation: self.presentation,
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
        let Some(state) = self.symbols.get_mut(index) else {
            return;
        };
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
        self.update_session_status();
    }

    fn apply_rate_alert(
        &mut self,
        index: usize,
        alert: &IntegrityAlert,
        event: &MarketEvent<MarketDataKind>,
    ) {
        self.push_alert(AlertRecord {
            symbol: Some(event.symbol.clone()),
            summary: alert_summary(alert),
            received_ts: event.received_ts,
        });
        let Some(state) = self.symbols.get_mut(index) else {
            return;
        };
        let summary = alert_summary(alert);
        let SymbolData::Rate { retained } = &mut state.data else {
            return;
        };
        match alert {
            IntegrityAlert::ResyncStarted => {
                state.status = DisplayStatus::AwaitingSnapshot;
            }
            IntegrityAlert::ResyncCompleted { .. } => {
                if retained.is_some() {
                    state.status = DisplayStatus::InSync;
                    state.unavailability = None;
                } else {
                    state.status = DisplayStatus::AwaitingSnapshot;
                }
            }
            IntegrityAlert::ConnectionLost {
                reason: mdc_core::DisconnectKind::Paused,
            } => {
                state.status = DisplayStatus::Paused;
                state.unavailability = None;
                if let Some(rate) = retained {
                    rate.diagnostic = Some(summary);
                }
            }
            _ => {
                state.status = DisplayStatus::Unavailable;
                state.unavailability = Some(summary.clone());
                if let Some(rate) = retained {
                    rate.diagnostic = Some(summary);
                }
            }
        }
        self.update_session_status();
    }

    fn apply_rate(&mut self, index: usize, event: &MarketEvent<MarketDataKind>) {
        let Some(state) = self.symbols.get_mut(index) else {
            return;
        };
        let MarketDataKind::ReferenceRate(rate) = &event.kind else {
            return;
        };
        let diagnostic = match &state.data {
            SymbolData::Rate { retained } => {
                retained.as_ref().and_then(|rate| rate.diagnostic.clone())
            }
            SymbolData::Book { .. } => None,
        }
        .or_else(|| state.unavailability.clone());
        let SymbolData::Rate { retained } = &mut state.data else {
            return;
        };
        let observation_ts = event.exchange_ts.unwrap_or(event.received_ts);
        let day = observation_ts.0 / 86_400_000_000_000;
        *retained = Some(RateView {
            provider: self.provider_label,
            pair: event.symbol.clone(),
            midpoint: rate.mid,
            observation_ts,
            observation_date: UnixNanos(day * 86_400_000_000_000),
            received_ts: event.received_ts,
            diagnostic,
        });
        state.status = DisplayStatus::InSync;
        state.unavailability = None;
        self.update_session_status();
    }

    /// Applies a data-event projection: `Available` promotes to `InSync`,
    /// `Unavailable` demotes with its reason, and `None` leaves status
    /// unchanged while a candidate is buffered or an event has no projection.
    fn apply_projection(&mut self, index: usize, projection: Option<QuoteProjection>) {
        let Some(state) = self.symbols.get_mut(index) else {
            return;
        };
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
        self.update_session_status();
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
            let Some(state) = self.symbols.get_mut(index) else {
                return;
            };
            if let SymbolData::Book { retained, .. } = &mut state.data {
                *retained = Some(QuoteView {
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
    }

    fn update_session_status(&mut self) {
        if matches!(self.session, DisplayStatus::Failed | DisplayStatus::Stopped) {
            return;
        }
        self.session = if self
            .symbols
            .iter()
            .any(|state| state.status == DisplayStatus::InSync)
        {
            DisplayStatus::InSync
        } else if self
            .symbols
            .iter()
            .any(|state| state.status == DisplayStatus::Unavailable)
        {
            DisplayStatus::Unavailable
        } else if self
            .symbols
            .iter()
            .any(|state| state.status == DisplayStatus::AwaitingSnapshot)
        {
            DisplayStatus::AwaitingSnapshot
        } else if self
            .symbols
            .iter()
            .any(|state| state.status == DisplayStatus::Paused)
        {
            DisplayStatus::Paused
        } else {
            DisplayStatus::Loading
        };
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
