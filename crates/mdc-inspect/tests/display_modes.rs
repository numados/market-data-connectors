//! Public display-state and rendering coverage for books, native tickers, and rates.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mdc_core::{
    BookTicker, DeliveryStamp, MarketDataKind, MarketEvent, PriceLevel, RunId, Symbol, UnixNanos,
    VenueId,
};
use mdc_inspect::diagnostics::DiagnosticsSnapshot;
use mdc_inspect::display::{
    DisplaySnapshot, DisplayStatus, DisplayedQuote, QuoteView, SymbolDisplay, UiState,
};
use mdc_inspect::view::draw_main_at;
use ratatui::{Terminal, backend::TestBackend};
use rust_decimal::Decimal;
use std::collections::VecDeque;

#[cfg(any(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
    feature = "coinbase-advanced",
    feature = "lmax",
    feature = "vwd"
))]
#[path = "display_modes/book.rs"]
mod book;

#[cfg(feature = "frankfurter")]
#[path = "display_modes/rates.rs"]
mod rates;

fn decimal(value: &str) -> Decimal {
    value.parse().expect("valid decimal")
}

fn event(
    symbol: &str,
    kind: MarketDataKind,
    exchange_ts: Option<u64>,
    received_ts: u64,
    ordinal: u64,
) -> MarketEvent<MarketDataKind> {
    MarketEvent {
        exchange_ts: exchange_ts.map(UnixNanos),
        received_ts: UnixNanos(received_ts),
        venue: VenueId::Binance,
        symbol: Symbol::from(symbol),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([7; 16]), 1, ordinal),
        source_sequence: None,
        kind,
    }
}

#[test]
fn native_ticker_projection_reaches_exact_display_quote() {
    let symbol = Symbol::from("EURUSD");
    let mut projector = mdc_sinks::TopOfBookProjector::new_ticker(symbol.clone(), 0);
    assert!(projector.is_in_sync());

    let projection = projector
        .handle_event(&event(
            symbol.as_str(),
            MarketDataKind::BookTicker(BookTicker::new(
                PriceLevel::new(decimal("100.123400"), decimal("2.500")),
                PriceLevel::new(decimal("100.567800"), decimal("3.250")),
                Some(42),
            )),
            Some(2_000),
            2_005,
            1,
        ))
        .expect("native ticker projection");
    let mdc_sinks::QuoteProjection::Available {
        bid,
        ask,
        bid_size,
        ask_size,
        effective_ts,
        received_ts,
        ..
    } = projection
    else {
        panic!("native ticker must project an available quote");
    };
    assert_eq!(bid, decimal("100.123400"));
    assert_eq!(ask, decimal("100.567800"));
    assert_eq!(bid_size, decimal("2.500"));
    assert_eq!(ask_size, decimal("3.250"));
    assert_eq!(effective_ts, UnixNanos(2_000));
    assert_eq!(received_ts, UnixNanos(2_005));

    let snapshot = DisplaySnapshot {
        provider: "native ticker test",
        presentation: Default::default(),
        session: DisplayStatus::InSync,
        failure: None,
        symbols: vec![SymbolDisplay {
            symbol,
            status: DisplayStatus::InSync,
            quote: Some(DisplayedQuote {
                quote: QuoteView {
                    bid,
                    ask,
                    bid_size,
                    ask_size,
                    spread: ask.checked_sub(bid),
                    effective_ts,
                    received_ts,
                },
                historical: false,
            }),
            rate: None,
            bids: Vec::new(),
            asks: Vec::new(),
            unavailability: None,
        }],
        alerts: VecDeque::new(),
        alerts_evicted: 0,
    };
    let text = render(&snapshot, UnixNanos(2_100));
    assert!(
        text.contains("bid 100.123400 x 2.500  ask 100.567800 x 3.250  spread 0.444400"),
        "{text}"
    );
}

fn render(snapshot: &DisplaySnapshot, now: UnixNanos) -> String {
    let mut terminal = Terminal::new(TestBackend::new(160, 40)).expect("test terminal");
    terminal
        .draw(|frame| {
            draw_main_at(
                frame,
                snapshot,
                &UiState::new(),
                &DiagnosticsSnapshot::default(),
                now,
            )
        })
        .expect("render display snapshot");
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
