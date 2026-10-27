use super::*;
use mdc_core::ReferenceRate;
use mdc_inspect::config::SessionConfig;
use mdc_inspect::display::DisplayMode;
use mdc_inspect::provider::ProviderKind;
use mdc_inspect::reducer::Reducer;

fn rate_event(symbol: &str) -> MarketEvent<MarketDataKind> {
    MarketEvent {
        exchange_ts: Some(UnixNanos::from_millis(1_704_067_200_000)),
        received_ts: UnixNanos::from_secs(1_704_067_200),
        venue: VenueId::FrankfurterEcb,
        symbol: Symbol::from(symbol),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
        source_sequence: None,
        kind: MarketDataKind::ReferenceRate(ReferenceRate::new(decimal("1.089"))),
    }
}

#[test]
fn reference_rate_state_and_display_keep_exact_midpoint_and_date() {
    let config = SessionConfig::new(
        ProviderKind::Frankfurter,
        &["EUR".to_string(), "USD".to_string()],
    )
    .expect("valid rate config");
    let mut reducer = Reducer::new(&config, ProviderKind::Frankfurter.session_bounds());
    reducer.apply_event(&rate_event("EURUSD"));

    let snapshot = reducer.snapshot();
    let symbol = &snapshot.symbols[0];
    let rate = symbol.rate.as_ref().expect("rate state");
    assert_eq!(snapshot.presentation.mode, DisplayMode::ReferenceRate);
    assert_eq!(snapshot.session, DisplayStatus::InSync);
    assert_eq!(symbol.status, DisplayStatus::InSync);
    assert_eq!(rate.rate.midpoint.to_string(), "1.089");
    assert_eq!(
        rate.rate.observation_date,
        UnixNanos::from_millis(1_704_067_200_000)
    );
    assert_eq!(rate.rate.observation_date_utc(), "2024-01-01");
    assert!(!rate.historical);
    assert!(symbol.quote.is_none());
    assert!(symbol.bids.is_empty());
    assert!(symbol.asks.is_empty());

    let text = render(&snapshot, UnixNanos::from_secs(1_704_067_201));
    assert!(text.contains("Reference rate"), "{text}");
    assert!(text.contains("midpoint 1.089"), "{text}");
    assert!(text.contains("observation date UTC 2024-01-01"), "{text}");
    assert!(!text.contains("Top of book"), "{text}");
}
