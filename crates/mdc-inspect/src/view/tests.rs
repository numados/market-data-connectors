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
        presentation: Default::default(),
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
    assert!(text.contains("c: configure"), "{text}");
    assert!(text.contains("Enter: start"), "{text}");
    assert!(!text.contains("s: stop"), "{text}");
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
fn reference_view_is_selected_before_any_rate_arrives() {
    for status in [
        DisplayStatus::Loading,
        DisplayStatus::AwaitingSnapshot,
        DisplayStatus::Unavailable,
    ] {
        let mut snap = snapshot(status, vec![view(Symbol::from("EURUSD"), status, None)]);
        snap.provider = "Frankfurter/ECB";
        snap.presentation.mode = DisplayMode::ReferenceRate;
        let text = render(120, 40, &snap, &empty_ui(), &empty_diagnostics());
        assert!(text.contains("Reference rate"), "{text}");
        for forbidden in ["Top of book", "Level 2", "spread", "bid ", "ask "] {
            assert!(!text.contains(forbidden), "unexpected {forbidden}: {text}");
        }
    }
}

#[test]
fn six_character_native_symbols_are_not_synthetic() {
    let snap = snapshot(
        DisplayStatus::InSync,
        vec![view(
            Symbol::from("BTCUSD"),
            DisplayStatus::InSync,
            Some(in_sync_quote()),
        )],
    );
    let text = render(120, 40, &snap, &empty_ui(), &empty_diagnostics());
    assert!(!text.contains("synthetic"), "{text}");
    assert!(text.contains("Level 2"), "{text}");
}

#[test]
fn retained_book_levels_are_labelled_historical_after_stop_or_invalidation() {
    for (session, symbol) in [
        (DisplayStatus::Unavailable, DisplayStatus::Unavailable),
        (DisplayStatus::Stopped, DisplayStatus::InSync),
        (DisplayStatus::Failed, DisplayStatus::InSync),
    ] {
        let snap = snapshot(session, vec![view(Symbol::from("BTCUSDT"), symbol, None)]);
        let text = render(120, 40, &snap, &empty_ui(), &empty_diagnostics());
        assert!(text.contains("HISTORICAL"), "{text}");
    }
}

#[test]
fn scroll_offsets_move_all_three_viewports() {
    let mut symbol = view(Symbol::from("BTCUSDT"), DisplayStatus::InSync, None);
    symbol.bids = (0..20)
        .map(|index| LevelView {
            price: Decimal::from(11000 + index),
            size: Decimal::ONE,
        })
        .collect();
    let mut snap = snapshot(DisplayStatus::InSync, vec![symbol]);
    snap.alerts = (0..20)
        .map(|index| AlertRecord {
            symbol: None,
            summary: format!("alert-{index:02}"),
            received_ts: UnixNanos(1),
        })
        .collect();
    let diagnostics = DiagnosticsSnapshot {
        records: (0..20)
            .map(|index| DiagnosticRecord {
                level: tracing::Level::INFO,
                target: "mdc".to_string(),
                name: format!("record-{index:02}"),
                fields: Vec::new(),
            })
            .collect(),
        evicted: 0,
    };
    let mut ui = empty_ui();
    let initial = render(120, 40, &snap, &ui, &diagnostics);
    for first in ["11000", "alert-00", "record-00"] {
        assert!(initial.contains(first), "{initial}");
    }
    ui.book_scroll = 1;
    ui.alerts_scroll = 1;
    ui.diagnostics_scroll = 1;
    let scrolled = render(120, 40, &snap, &ui, &diagnostics);
    for first in ["11000", "alert-00", "record-00"] {
        assert!(
            !scrolled.contains(first),
            "{first} stayed visible: {scrolled}"
        );
    }
    for second in ["11001", "alert-01", "record-01"] {
        assert!(scrolled.contains(second), "{scrolled}");
    }
}

#[test]
fn narrow_terminal_keeps_depth_and_every_other_pane() {
    let snap = snapshot(
        DisplayStatus::InSync,
        vec![view(
            Symbol::from("BTCUSDT"),
            DisplayStatus::InSync,
            Some(in_sync_quote()),
        )],
    );
    let text = render(60, 45, &snap, &empty_ui(), &empty_diagnostics());
    for heading in ["Level 2", "Top of book", "Integrity alerts", "Diagnostics"] {
        assert!(text.contains(heading), "missing {heading}: {text}");
    }
}

#[test]
fn rates_use_calendar_dates_and_the_render_clock_without_new_market_events() {
    use crate::display::{DisplayedRate, RateView};
    let mut symbol = view(Symbol::from("EURUSD"), DisplayStatus::InSync, None);
    symbol.bids.clear();
    symbol.asks.clear();
    symbol.rate = Some(DisplayedRate {
        rate: RateView {
            provider: "Frankfurter/ECB",
            pair: Symbol::from("EURUSD"),
            midpoint: "1.08900".parse().unwrap(),
            observation_ts: UnixNanos::from_secs(1_704_067_200),
            observation_date: UnixNanos::from_secs(1_704_067_200),
            received_ts: UnixNanos::from_secs(10),
            diagnostic: Some("source error (HttpStatus(500))".to_string()),
        },
        historical: false,
    });
    let mut snap = snapshot(DisplayStatus::InSync, vec![symbol]);
    snap.presentation.mode = DisplayMode::ReferenceRate;
    let render_at = |now| {
        let mut terminal = Terminal::new(TestBackend::new(140, 40)).unwrap();
        terminal
            .draw(|frame| {
                draw_main_at(
                    frame,
                    &snap,
                    &empty_ui(),
                    &empty_diagnostics(),
                    UnixNanos::from_secs(now),
                )
            })
            .unwrap();
        buffer_text(terminal.backend().buffer())
    };
    let early = render_at(13);
    let later = render_at(19);
    assert!(early.contains("receipt age 3s"), "{early}");
    assert!(later.contains("receipt age 9s"), "{later}");
    for text in [&early, &later] {
        for required in ["2024-01-01", "EURUSD", "1.08900", "HttpStatus(500)"] {
            assert!(text.contains(required), "{text}");
        }
        for forbidden in ["bid ", "ask ", "spread", "Level 2", "stale", "latency"] {
            assert!(!text.contains(forbidden), "{text}");
        }
    }
}

#[test]
fn synthetic_disclosure_depends_on_provider_mode_not_symbol_length() {
    let mut snap = snapshot(
        DisplayStatus::InSync,
        vec![view(
            Symbol::from("EURUSD"),
            DisplayStatus::InSync,
            Some(in_sync_quote()),
        )],
    );
    snap.provider = "VWD/Infront";
    snap.presentation.mode = DisplayMode::SyntheticBook;
    let text = render(160, 40, &snap, &empty_ui(), &empty_diagnostics());
    assert!(text.contains("synthetic one-level indicative"), "{text}");
    assert!(text.contains("capabilities: unknown"), "{text}");
}

#[cfg(feature = "binance")]
#[test]
fn static_capability_values_are_rendered_from_the_provider_declaration() {
    let mut snap = snapshot(DisplayStatus::Loading, vec![]);
    snap.presentation = crate::provider::ProviderKind::Binance.presentation();
    let text = render(160, 40, &snap, &empty_ui(), &empty_diagnostics());
    assert!(text.contains("capabilities: quotes"), "{text}");
    assert!(text.contains("SequenceNumbers"), "{text}");
    assert!(
        text.contains(&format!(
            "symbols/connection: {}",
            snap.presentation
                .capabilities
                .unwrap()
                .bounds
                .max_symbols_per_connection
        )),
        "{text}"
    );
    assert!(!text.contains("capabilities: unknown"), "{text}");
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
