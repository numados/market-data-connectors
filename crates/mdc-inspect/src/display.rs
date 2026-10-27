//! Provider-neutral display state: statuses, historical-value representation,
//! coalesced snapshots, and UI focus/scroll state.

use std::collections::VecDeque;
use std::time::Duration;

use mdc_core::{ProviderCapabilities, Symbol, UnixNanos};

use crate::diagnostics::DiagnosticsSnapshot;

/// Provider-declared presentation semantics, independent of observed events.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DisplayMode {
    /// Native order book with projected top of book.
    #[default]
    OrderBook,
    /// Dated reference observations without executable sides or depth.
    ReferenceRate,
    /// Synthetic one-level indicative FX book.
    SyntheticBook,
}

/// Immutable metadata supplied by the selected provider adapter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderPresentation {
    /// View semantics, valid before the first observation arrives.
    pub mode: DisplayMode,
    /// Existing provider declaration; absence means capabilities are unknown.
    pub capabilities: Option<&'static ProviderCapabilities>,
}
use rust_decimal::Decimal;

/// Canonical display status set (PRD 31 §3.5 requirement 72).
///
/// Session-level and per-symbol states share one enum; the session status is
/// the aggregate view, and each symbol tracks its own status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisplayStatus {
    /// No configured or active session.
    Empty,
    /// Start accepted, no initial integrity event yet.
    Loading,
    /// Initial synchronization or resynchronization is active.
    AwaitingSnapshot,
    /// The projector exposes an available current quote.
    InSync,
    /// An integrity alert or projection error invalidates current data.
    Unavailable,
    /// `ConnectionLost { reason: DisconnectKind::Paused }` was observed.
    Paused,
    /// The operator stopped the session and teardown completed.
    Stopped,
    /// Delivery, connector, join, or session orchestration failed.
    Failed,
}

impl DisplayStatus {
    /// Short text label, used in addition to any color styling.
    pub const fn label(self) -> &'static str {
        match self {
            DisplayStatus::Empty => "empty",
            DisplayStatus::Loading => "loading",
            DisplayStatus::AwaitingSnapshot => "awaiting snapshot",
            DisplayStatus::InSync => "in sync",
            DisplayStatus::Unavailable => "unavailable",
            DisplayStatus::Paused => "paused",
            DisplayStatus::Stopped => "stopped",
            DisplayStatus::Failed => "failed",
        }
    }
}

/// A reference-rate observation with exact decimal and receipt timing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateView {
    /// Provider that supplied the observation.
    pub provider: &'static str,
    /// Provider pair symbol.
    pub pair: Symbol,
    /// Exact reference midpoint.
    pub midpoint: Decimal,
    /// Observation timestamp from the provider event.
    pub observation_ts: UnixNanos,
    /// UTC midnight corresponding to the observation date.
    pub observation_date: UnixNanos,
    /// Receipt timestamp captured by the connector.
    pub received_ts: UnixNanos,
    /// Latest bounded diagnostic for this pair.
    pub diagnostic: Option<String>,
}

impl RateView {
    /// Elapsed receipt time at the renderer's clock, never market latency.
    pub fn receipt_age_at(&self, now: UnixNanos) -> Duration {
        Duration::from_nanos(now.0.saturating_sub(self.received_ts.0))
    }

    /// UTC calendar date derived from the observation timestamp.
    pub fn observation_date_utc(&self) -> String {
        let mut days = self.observation_ts.0 / 86_400_000_000_000;
        let mut year = 1970u64;
        let leap = |year: u64| year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        loop {
            let year_days = if leap(year) { 366 } else { 365 };
            if days < year_days {
                break;
            }
            days -= year_days;
            year += 1;
        }
        let months = [
            31,
            if leap(year) { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut month = 1;
        for month_days in months {
            if days < month_days {
                break;
            }
            days -= month_days;
            month += 1;
        }
        format!("{year:04}-{month:02}-{:02}", days + 1)
    }
}

/// A retained reference-rate observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplayedRate {
    /// The retained observation.
    pub rate: RateView,
    /// True after an invalidating alert or terminal failure.
    pub historical: bool,
}

/// A projected top-of-book value with exact decimals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteView {
    /// Best bid price.
    pub bid: Decimal,
    /// Best ask price.
    pub ask: Decimal,
    /// Best bid size.
    pub bid_size: Decimal,
    /// Best ask size.
    pub ask_size: Decimal,
    /// Checked `ask - bid` subtraction; `None` when invalid.
    pub spread: Option<Decimal>,
    /// Effective timestamp of the older side.
    pub effective_ts: UnixNanos,
    /// Ingestion receipt timestamp.
    pub received_ts: UnixNanos,
}

/// A retained quote together with its validity marking.
///
/// Retained values are historical whenever the owning symbol is not
/// [`DisplayStatus::InSync`]; historical values never share the styling or
/// label of a current quote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplayedQuote {
    /// The retained value.
    pub quote: QuoteView,
    /// `true` when the value is not currently valid.
    pub historical: bool,
}

/// One price level in the depth view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelView {
    /// Level price.
    pub price: Decimal,
    /// Level size.
    pub size: Decimal,
}

/// Per-symbol display state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolDisplay {
    /// Canonical symbol.
    pub symbol: Symbol,
    /// Symbol status.
    pub status: DisplayStatus,
    /// Retained quote, historical unless status is `InSync`.
    pub quote: Option<DisplayedQuote>,
    /// Reference-rate observation, present only for rate providers.
    pub rate: Option<DisplayedRate>,
    /// Bid levels, best first, bounded by the projector depth.
    pub bids: Vec<LevelView>,
    /// Ask levels, best first, bounded by the projector depth.
    pub asks: Vec<LevelView>,
    /// Human-readable unavailability cause when status is `Unavailable`.
    pub unavailability: Option<String>,
}

/// One retained integrity-alert record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlertRecord {
    /// Symbol the alert belongs to, when known.
    pub symbol: Option<Symbol>,
    /// Sanitized summary of the alert.
    pub summary: String,
    /// Ingestion receipt timestamp.
    pub received_ts: UnixNanos,
}

/// Immutable coalesced display snapshot.
///
/// The session publishes whole snapshots behind an `Arc` on a latest-value
/// boundary; the renderer never queues snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplaySnapshot {
    /// Provider label.
    pub provider: &'static str,
    /// Provider-selected view semantics and declared capability metadata.
    pub presentation: ProviderPresentation,
    /// Session-level aggregate status.
    pub session: DisplayStatus,
    /// Sanitized terminal failure cause when the session failed.
    pub failure: Option<String>,
    /// Per-symbol state in configuration order.
    pub symbols: Vec<SymbolDisplay>,
    /// Retained alert history, oldest first.
    pub alerts: VecDeque<AlertRecord>,
    /// Alerts evicted from the bounded history.
    pub alerts_evicted: usize,
}

/// Which pane owns keyboard focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    /// Symbol tab selector.
    Symbols,
    /// Level 2 depth pane.
    Book,
    /// Integrity alert pane.
    Alerts,
    /// Diagnostics pane.
    Diagnostics,
}

/// Focus, tab selection, and scroll state.
///
/// Pure UI state: it never alters reducer state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiState {
    /// Focused pane.
    pub focus: Focus,
    /// Selected symbol tab index.
    pub selected_symbol: usize,
    /// Level 2 pane scroll offset.
    pub book_scroll: usize,
    /// Alert pane scroll offset.
    pub alerts_scroll: usize,
    /// Diagnostics pane scroll offset.
    pub diagnostics_scroll: usize,
    /// Help overlay visibility.
    pub help_open: bool,
}

impl UiState {
    /// Initial UI state.
    pub const fn new() -> Self {
        Self {
            focus: Focus::Symbols,
            selected_symbol: 0,
            book_scroll: 0,
            alerts_scroll: 0,
            diagnostics_scroll: 0,
            help_open: false,
        }
    }

    /// Keeps selection and pane offsets within the currently rendered rows.
    pub(crate) fn clamp(&mut self, snapshot: &DisplaySnapshot, diagnostics: &DiagnosticsSnapshot) {
        self.selected_symbol = self
            .selected_symbol
            .min(snapshot.symbols.len().saturating_sub(1));

        let book_rows = snapshot
            .symbols
            .get(self.selected_symbol)
            .map(|symbol| symbol.bids.len().saturating_add(symbol.asks.len()))
            .unwrap_or(0)
            .max(1);
        self.book_scroll = self.book_scroll.min(book_rows.saturating_sub(1));

        let alert_rows = list_row_count(snapshot.alerts.len(), snapshot.alerts_evicted);
        self.alerts_scroll = self.alerts_scroll.min(alert_rows.saturating_sub(1));

        let diagnostic_rows = list_row_count(diagnostics.records.len(), diagnostics.evicted);
        self.diagnostics_scroll = self
            .diagnostics_scroll
            .min(diagnostic_rows.saturating_sub(1));
    }
}

fn list_row_count(items: usize, evicted: usize) -> usize {
    if evicted > 0 {
        items.saturating_add(1).max(1)
    } else {
        items.max(1)
    }
}

impl Default for UiState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
