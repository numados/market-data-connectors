//! Public display-state and rendering coverage for books, native tickers, and rates.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::VecDeque;
use std::str::FromStr;

use mdc_core::{
    DeliveryStamp, MarketDataKind, MarketEvent, ReferenceRate, RunId, Symbol, UnixNanos, VenueId,
};
use mdc_inspect::config::SessionConfig;
use mdc_inspect::display::{DisplaySnapshot, DisplayStatus, UiState};
use mdc_inspect::provider::{ProviderKind, SessionBounds};
use mdc_inspect::reducer::Reducer;
use rust_decimal::Decimal;

fn rate_event(symbol: &str) -> MarketEvent<MarketDataKind> {
    MarketEvent {
        exchange_ts: Some(UnixNanos::from_millis(1_704_067_200_000)),
        received_ts: UnixNanos::from_secs(1_704_067_200),
        venue: VenueId::FrankfurterEcb,
        symbol: Symbol::from(symbol),
        tag: 0,
        delivery: DeliveryStamp {
            run: RunId::from_bytes([0; 16]),
            connection_id: 1,
            connection_generation: 1,
            ordinal: 1,
        },
        source_sequence: None,
        kind: MarketDataKind::ReferenceRate(ReferenceRate::new(
            Decimal::from_str("1.089").expect("decimal"),
        )),
    }
}

#[cfg(feature = "frankfurter")]
#[test]
fn reference_rate_state_keeps_exact_midpoint_and_date() {
    let config = SessionConfig::new(
        ProviderKind::Frankfurter,
        &["EUR".to_string(), "USD".to_string()],
    )
    .expect("valid rate config");
    let mut reducer = Reducer::new(
        &config,
        SessionBounds {
            max_symbols: 100,
            projector_depth: 1,
            affected_connection: 1,
        },
    );
    reducer.apply_event(&rate_event("EURUSD"));
    let symbol = &reducer.snapshot().symbols[0];
    let rate = symbol.rate.as_ref().expect("rate state");
    assert_eq!(rate.rate.midpoint.to_string(), "1.089");
    assert_eq!(
        rate.rate.observation_date,
        UnixNanos::from_millis(1_704_067_200_000)
    );
    assert!(symbol.quote.is_none());
    assert!(symbol.bids.is_empty());
    assert!(symbol.asks.is_empty());
}

#[test]
fn display_snapshot_shape_remains_book_compatible() {
    let snapshot = DisplaySnapshot {
        provider: "test",
        session: DisplayStatus::Loading,
        failure: None,
        symbols: Vec::new(),
        alerts: VecDeque::new(),
        alerts_evicted: 0,
    };
    assert_eq!(snapshot.session, DisplayStatus::Loading);
    let _ = UiState::new();
}
