//! Ratatui `TestBackend` rendering tests: all session states, historical
//! labels, help overlay, and narrow layouts (requirement 144).
//! Text labels are asserted in addition to any styling.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::VecDeque;

use mdc_core::{Symbol, UnixNanos};
use ratatui::{Terminal, backend::TestBackend};
use rust_decimal::Decimal;

use super::*;
use crate::diagnostics::DiagnosticRecord;
use crate::display::{AlertRecord, DisplayedQuote, LevelView, QuoteView, SymbolDisplay};

fn view(symbol: Symbol, status: DisplayStatus, quote: Option<DisplayedQuote>) -> SymbolDisplay {
    SymbolDisplay {
        symbol,
        status,
        quote,
        rate: None,
        bids: vec![LevelView {
            price: Decimal::from(100),
            size: Decimal::ONE,
        }],
        asks: vec![LevelView {
            price: Decimal::from(101),
            size: Decimal::ONE,
        }],
        unavailability: None,
    }
}

fn in_sync_quote() -> DisplayedQuote {
    DisplayedQuote {
        quote: QuoteView {
            bid: Decimal::from(100),
            ask: Decimal::from(101),
            bid_size: Decimal::ONE,
            ask_size: Decimal::TWO,
            spread: Some(Decimal::ONE),
            effective_ts: UnixNanos(1),
            received_ts: UnixNanos(2),
        },
        historical: false,
    }
}

fn snapshot(session: DisplayStatus, symbols: Vec<SymbolDisplay>) -> DisplaySnapshot {
    DisplaySnapshot {
        provider: "Binance Spot",
        session,
        failure: None,
        symbols,
        alerts: VecDeque::new(),
        alerts_evicted: 0,
    }
}

fn render(
    width: u16,
    height: u16,
    snapshot: &DisplaySnapshot,
    ui: &UiState,
    diagnostics: &DiagnosticsSnapshot,
) -> String {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| draw_main(frame, snapshot, ui, diagnostics))
        .unwrap();
    buffer_text(terminal.backend().buffer())
}

fn buffer_text(buffer: &ratatui::buffer::Buffer) -> String {
    let mut text = String::new();
    let mut last_y = 0;
    for (index, cell) in buffer.content.iter().enumerate() {
        let y = index as u16 / buffer.area.width;
        if y != last_y {
            text.push('\n');
            last_y = y;
        }
        text.push_str(cell.symbol());
    }
    text
}

fn empty_ui() -> UiState {
    UiState::new()
}

fn empty_diagnostics() -> DiagnosticsSnapshot {
    DiagnosticsSnapshot::default()
}

#[test]
fn renders_provider_header_and_status_labels() {
    let snap = snapshot(
        DisplayStatus::InSync,
        vec![view(
            Symbol::from("BTCUSDT"),
            DisplayStatus::InSync,
            Some(in_sync_quote()),
        )],
    );
    let text = render(100, 30, &snap, &empty_ui(), &empty_diagnostics());
    assert!(text.contains("mdc-inspect"), "{text}");
    assert!(text.contains("provider: Binance Spot"), "{text}");
    assert!(text.contains("session: in sync"), "{text}");
    assert!(text.contains("BTCUSDT"), "{text}");
    assert!(text.contains("bid"), "{text}");
    assert!(text.contains("spread 1"), "{text}");
    assert!(
        !text.contains("HISTORICAL"),
        "current quote must not be historical"
    );
}

#[test]
fn renders_historical_label_for_retained_quotes() {
    let mut historical = in_sync_quote();
    historical.historical = true;
    let snap = snapshot(
        DisplayStatus::Unavailable,
        vec![view(
            Symbol::from("BTCUSDT"),
            DisplayStatus::Unavailable,
            Some(historical),
        )],
    );
    let text = render(100, 30, &snap, &empty_ui(), &empty_diagnostics());
    assert!(text.contains("HISTORICAL"), "{text}");
    assert!(text.contains("unavailable"), "{text}");
}

#[test]
fn renders_all_session_states_with_labels() {
    for state in [
        DisplayStatus::Loading,
        DisplayStatus::AwaitingSnapshot,
        DisplayStatus::Unavailable,
        DisplayStatus::Paused,
        DisplayStatus::Stopped,
        DisplayStatus::Failed,
    ] {
        let snap = snapshot(state, vec![view(Symbol::from("BTCUSDT"), state, None)]);
        let text = render(100, 30, &snap, &empty_ui(), &empty_diagnostics());
        assert!(
            text.contains(state.label()),
            "missing label for {state:?}: {text}"
        );
    }
}

#[test]
fn renders_failure_line() {
    let mut snap = snapshot(DisplayStatus::Failed, vec![]);
    snap.failure = Some("channel capacity exceeded".to_string());
    let text = render(100, 30, &snap, &empty_ui(), &empty_diagnostics());
    assert!(text.contains("failure:"), "{text}");
    assert!(text.contains("capacity exceeded"), "{text}");
}

#[test]
fn renders_alerts_with_eviction_counter() {
    let mut snap = snapshot(
        DisplayStatus::InSync,
        vec![view(
            Symbol::from("BTCUSDT"),
            DisplayStatus::InSync,
            Some(in_sync_quote()),
        )],
    );
    snap.alerts.push_back(AlertRecord {
        symbol: Some(Symbol::from("BTCUSDT")),
        summary: "gap detected (expected 2, received 5)".to_string(),
        received_ts: UnixNanos(7),
    });
    snap.alerts_evicted = 3;
    let text = render(100, 30, &snap, &empty_ui(), &empty_diagnostics());
    assert!(text.contains("gap detected"), "{text}");
    assert!(text.contains("3 older alerts evicted"), "{text}");
}

#[test]
fn renders_diagnostics_and_evictions() {
    let diagnostics = DiagnosticsSnapshot {
        records: vec![DiagnosticRecord {
            level: tracing::Level::INFO,
            target: "mdc".to_string(),
            name: "up".to_string(),
            fields: vec![("venue".to_string(), "binance".to_string())],
        }],
        evicted: 2,
    };
    let snap = snapshot(DisplayStatus::InSync, vec![]);
    let text = render(100, 30, &snap, &empty_ui(), &diagnostics);
    assert!(text.contains("venue=binance"), "{text}");
    assert!(text.contains("2 older diagnostics evicted"), "{text}");
}

#[test]
fn narrow_terminal_stacks_without_panicking() {
    let snap = snapshot(
        DisplayStatus::InSync,
        vec![view(
            Symbol::from("BTCUSDT"),
            DisplayStatus::InSync,
            Some(in_sync_quote()),
        )],
    );
    let text = render(40, 20, &snap, &empty_ui(), &empty_diagnostics());
    assert!(text.contains("BTCUSDT"), "{text}");
}

#[test]
fn zero_sized_area_does_not_panic() {
    let snap = snapshot(DisplayStatus::Empty, vec![]);
    let text = render(0, 0, &snap, &empty_ui(), &empty_diagnostics());
    assert_eq!(text.trim(), "");
}

#[test]
fn help_overlay_lists_required_keys_and_no_excluded_controls() {
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(draw_help).unwrap();
    let text = buffer_text(terminal.backend().buffer());
    for required in [
        "Enter",
        "stop",
        "Tab",
        "Left/Right",
        "help",
        "quit",
        "Ctrl-C",
    ] {
        assert!(text.contains(required), "help missing {required}: {text}");
    }
    for excluded in ["resync", "reconnect", "pause", "resume", "poll"] {
        assert!(
            !text.to_lowercase().contains(excluded),
            "help advertises {excluded}: {text}"
        );
    }
}

#[test]
fn setup_screen_renders_picker_input_and_errors() {
    let mut setup = crate::setup::SetupState::new();
    for c in "BTCUSDT".chars() {
        let _ = setup.handle_key(&crate::setup::key_for_test(c));
    }
    let _ = setup.handle_key(&crate::setup::key_for_test('\n'));
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal.draw(|frame| draw_setup(frame, &setup)).unwrap();
    let text = buffer_text(terminal.backend().buffer());
    assert!(text.contains("Provider"), "{text}");
    assert!(text.contains("BTCUSDT"), "{text}");
}
