//! Calendar and receipt-clock behavior independent of provider polling.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::diagnostics::{DiagnosticRecord, DiagnosticsSnapshot};

fn observation(timestamp: UnixNanos) -> RateView {
    RateView {
        provider: "rates",
        pair: Symbol::from("EURUSD"),
        midpoint: Decimal::ONE,
        observation_ts: timestamp,
        observation_date: timestamp,
        received_ts: UnixNanos::from_secs(10),
        diagnostic: None,
    }
}

#[test]
fn observation_date_handles_epoch_leap_centuries_and_timestamp_limit() {
    for (timestamp, expected) in [
        (UnixNanos(0), "1970-01-01"),
        (UnixNanos::from_secs(951_782_400), "2000-02-29"),
        (UnixNanos::from_secs(4_107_542_400), "2100-03-01"),
        (UnixNanos(u64::MAX), "2554-07-21"),
    ] {
        assert_eq!(observation(timestamp).observation_date_utc(), expected);
    }
}

#[test]
fn receipt_age_uses_the_supplied_clock_and_saturates_before_receipt() {
    let rate = observation(UnixNanos(0));
    assert_eq!(
        rate.receipt_age_at(UnixNanos::from_secs(13)),
        Duration::from_secs(3)
    );
    assert_eq!(
        rate.receipt_age_at(UnixNanos::from_secs(19)),
        Duration::from_secs(9)
    );
    assert_eq!(rate.receipt_age_at(UnixNanos::from_secs(9)), Duration::ZERO);
}

fn symbol(name: &str, bids: usize, asks: usize) -> SymbolDisplay {
    let level = || LevelView {
        price: Decimal::ONE,
        size: Decimal::ONE,
    };
    SymbolDisplay {
        symbol: Symbol::from(name),
        status: DisplayStatus::InSync,
        quote: None,
        rate: None,
        bids: (0..bids).map(|_| level()).collect(),
        asks: (0..asks).map(|_| level()).collect(),
        unavailability: None,
    }
}

#[test]
fn clamp_uses_current_rows_and_retains_placeholder_rows() {
    let snapshot = DisplaySnapshot {
        provider: "test",
        presentation: ProviderPresentation::default(),
        session: DisplayStatus::InSync,
        failure: None,
        symbols: vec![symbol("BTCUSDT", 1, 1), symbol("ETHUSDT", 2, 1)],
        alerts: VecDeque::from([AlertRecord {
            symbol: None,
            summary: "gap".to_string(),
            received_ts: UnixNanos(0),
        }]),
        alerts_evicted: 1,
    };
    let diagnostics = DiagnosticsSnapshot {
        records: vec![DiagnosticRecord {
            level: tracing::Level::INFO,
            target: "test".to_string(),
            name: "event".to_string(),
            fields: Vec::new(),
        }],
        evicted: 1,
    };
    let mut ui = UiState {
        focus: Focus::Symbols,
        selected_symbol: usize::MAX,
        book_scroll: usize::MAX,
        alerts_scroll: usize::MAX,
        diagnostics_scroll: usize::MAX,
        help_open: false,
    };

    ui.clamp(&snapshot, &diagnostics);

    assert_eq!(ui.selected_symbol, 1);
    assert_eq!(ui.book_scroll, 2);
    assert_eq!(ui.alerts_scroll, 1);
    assert_eq!(ui.diagnostics_scroll, 1);
}

#[test]
fn clamp_bounds_empty_panes_to_their_placeholder_rows() {
    let snapshot = DisplaySnapshot {
        provider: "test",
        presentation: ProviderPresentation::default(),
        session: DisplayStatus::Empty,
        failure: None,
        symbols: vec![symbol("BTCUSDT", 0, 0)],
        alerts: VecDeque::new(),
        alerts_evicted: 0,
    };
    let mut ui = UiState {
        focus: Focus::Symbols,
        selected_symbol: usize::MAX,
        book_scroll: usize::MAX,
        alerts_scroll: usize::MAX,
        diagnostics_scroll: usize::MAX,
        help_open: false,
    };

    ui.clamp(&snapshot, &DiagnosticsSnapshot::default());

    assert_eq!(ui.selected_symbol, 0);
    assert_eq!(ui.book_scroll, 0);
    assert_eq!(ui.alerts_scroll, 0);
    assert_eq!(ui.diagnostics_scroll, 0);
}
