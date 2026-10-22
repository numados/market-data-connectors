//! Provider-neutral display state: statuses, historical-value representation,
//! coalesced snapshots, and UI focus/scroll state.

use std::collections::VecDeque;

use mdc_core::{Symbol, UnixNanos};
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
}

impl Default for UiState {
    fn default() -> Self {
        Self::new()
    }
}
