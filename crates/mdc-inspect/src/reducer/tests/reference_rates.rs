//! Reference-rate state transitions at the reducer boundary.
use super::*;
use mdc_core::ReferenceRate;

#[cfg(feature = "frankfurter")]
fn frankfurter_config(symbols: &[&str]) -> SessionConfig {
    let raw: Vec<String> = symbols.iter().map(|symbol| (*symbol).to_string()).collect();
    SessionConfig::new(ProviderKind::Frankfurter, &raw).expect("valid Frankfurter config")
}

#[cfg(feature = "frankfurter")]
fn rate_event(
    symbol: &str,
    midpoint: &str,
    observation_ts: UnixNanos,
    received_ts: UnixNanos,
    ordinal: u64,
) -> MarketEvent<MarketDataKind> {
    MarketEvent {
        exchange_ts: Some(observation_ts),
        received_ts,
        venue: VenueId::FrankfurterEcb,
        symbol: Symbol::from(symbol),
        tag: 0,
        delivery: DeliveryStamp {
            run: RunId::from_bytes([0u8; 16]),
            connection_id: 0,
            connection_generation: 1,
            ordinal,
        },
        source_sequence: None,
        kind: MarketDataKind::ReferenceRate(ReferenceRate::new(d(midpoint))),
    }
}

#[cfg(feature = "frankfurter")]
fn rate_alert(symbol: &str, alert: IntegrityAlert, ordinal: u64) -> MarketEvent<MarketDataKind> {
    event(symbol, MarketDataKind::IntegrityAlert(alert), ordinal)
}

#[cfg(feature = "frankfurter")]
#[test]
fn reference_rate_trace_keeps_resync_out_of_the_book_projector() {
    let mut reducer = Reducer::new(&frankfurter_config(&["EUR", "GBP"]), bounds(1));
    reducer.apply_event(&rate_alert("EURGBP", IntegrityAlert::ResyncStarted, 1));
    let awaiting = reducer.snapshot();
    assert_eq!(awaiting.session, DisplayStatus::AwaitingSnapshot);
    assert_eq!(awaiting.symbols[0].status, DisplayStatus::AwaitingSnapshot);
    assert!(awaiting.symbols[0].rate.is_none());
    assert!(awaiting.symbols[0].quote.is_none());
    assert!(awaiting.symbols[0].bids.is_empty() && awaiting.symbols[0].asks.is_empty());

    let observation_ts = UnixNanos(1_774_396_800_000_000_000);
    reducer.apply_event(&rate_event(
        "EURGBP",
        "0.8623",
        observation_ts,
        UnixNanos(7_000),
        2,
    ));
    let current = reducer.snapshot();
    let rate = current.symbols[0].rate.as_ref().expect("rate retained");
    assert_eq!(current.session, DisplayStatus::InSync);
    assert!(!rate.historical);
    assert_eq!(rate.rate.midpoint, d("0.8623"));
    assert_eq!(rate.rate.observation_ts, observation_ts);
    assert_eq!(rate.rate.observation_date, observation_ts);
    assert_eq!(rate.rate.received_ts, UnixNanos(7_000));
    assert!(current.symbols[0].quote.is_none());

    reducer.apply_event(&rate_alert(
        "EURGBP",
        IntegrityAlert::ResyncCompleted {
            last_update_id: None,
        },
        3,
    ));
    let completed = reducer.snapshot();
    assert_eq!(completed.session, DisplayStatus::InSync);
    assert_eq!(completed.symbols[0].status, DisplayStatus::InSync);
    assert!(!completed.symbols[0].rate.as_ref().expect("rate").historical);
    assert!(completed.symbols[0].bids.is_empty() && completed.symbols[0].asks.is_empty());
}

#[cfg(feature = "frankfurter")]
#[test]
fn reference_rate_repeated_poll_replaces_exact_observation() {
    let mut reducer = Reducer::new(&frankfurter_config(&["EUR", "GBP"]), bounds(1));
    reducer.apply_event(&rate_event(
        "EURGBP",
        "0.8623",
        UnixNanos(1_774_396_800_000_000_000),
        UnixNanos(7_000),
        1,
    ));
    reducer.apply_event(&rate_alert(
        "EURGBP",
        IntegrityAlert::ResyncCompleted {
            last_update_id: None,
        },
        2,
    ));
    reducer.apply_event(&rate_event(
        "EURGBP",
        "0.900100",
        UnixNanos(1_774_483_200_000_000_000),
        UnixNanos(8_000),
        3,
    ));
    reducer.apply_event(&rate_alert(
        "EURGBP",
        IntegrityAlert::ResyncCompleted {
            last_update_id: None,
        },
        4,
    ));

    let snapshot = reducer.snapshot();
    let rate = snapshot.symbols[0].rate.as_ref().expect("latest rate");
    assert_eq!(rate.rate.midpoint, d("0.900100"));
    assert_eq!(
        rate.rate.observation_ts,
        UnixNanos(1_774_483_200_000_000_000)
    );
    assert_eq!(
        rate.rate.observation_date,
        UnixNanos(1_774_483_200_000_000_000)
    );
    assert_eq!(rate.rate.received_ts, UnixNanos(8_000));
    assert!(!rate.historical);
}

#[cfg(feature = "frankfurter")]
#[test]
fn reference_rate_source_failure_is_historical_until_recovery() {
    let mut reducer = Reducer::new(&frankfurter_config(&["EUR", "GBP"]), bounds(1));
    reducer.apply_event(&rate_event(
        "EURGBP",
        "0.8623",
        UnixNanos(1_774_396_800_000_000_000),
        UnixNanos(7_000),
        1,
    ));
    reducer.apply_event(&rate_alert(
        "EURGBP",
        IntegrityAlert::ResyncCompleted {
            last_update_id: None,
        },
        2,
    ));
    reducer.apply_event(&rate_alert(
        "EURGBP",
        IntegrityAlert::SourceError {
            kind: mdc_core::SourceErrorKind::HttpStatus(500),
        },
        3,
    ));
    let failed = reducer.snapshot();
    let failed_rate = failed.symbols[0].rate.as_ref().expect("historical rate");
    assert_eq!(failed.session, DisplayStatus::Unavailable);
    assert_eq!(failed.symbols[0].status, DisplayStatus::Unavailable);
    assert!(failed_rate.historical);
    assert_eq!(
        failed_rate.rate.diagnostic.as_deref(),
        Some("source error (HttpStatus(500))")
    );

    reducer.apply_event(&rate_alert("EURGBP", IntegrityAlert::ResyncStarted, 4));
    assert_eq!(reducer.snapshot().session, DisplayStatus::AwaitingSnapshot);
    reducer.apply_event(&rate_event(
        "EURGBP",
        "0.9001",
        UnixNanos(1_774_483_200_000_000_000),
        UnixNanos(8_000),
        5,
    ));
    reducer.apply_event(&rate_alert(
        "EURGBP",
        IntegrityAlert::ResyncCompleted {
            last_update_id: None,
        },
        6,
    ));
    let recovered = reducer.snapshot();
    let recovered_rate = recovered.symbols[0].rate.as_ref().expect("recovered rate");
    assert_eq!(recovered.session, DisplayStatus::InSync);
    assert!(!recovered_rate.historical);
    assert_eq!(recovered_rate.rate.midpoint, d("0.9001"));
    assert_eq!(
        recovered_rate.rate.diagnostic.as_deref(),
        Some("source error (HttpStatus(500))")
    );
}

#[cfg(feature = "frankfurter")]
#[test]
fn rate_aggregate_handles_paused_and_terminal_states() {
    let mut reducer = Reducer::new(&frankfurter_config(&["EUR", "GBP"]), bounds(1));
    reducer.apply_event(&rate_alert(
        "EURGBP",
        IntegrityAlert::ConnectionLost {
            reason: DisconnectKind::Paused,
        },
        1,
    ));
    let paused = reducer.snapshot();
    assert_eq!(paused.session, DisplayStatus::Paused);
    assert_eq!(paused.symbols[0].status, DisplayStatus::Paused);

    reducer.apply_event(&rate_event(
        "EURGBP",
        "0.8623",
        UnixNanos(1_774_396_800_000_000_000),
        UnixNanos(7_000),
        2,
    ));
    assert_eq!(reducer.snapshot().session, DisplayStatus::InSync);
    reducer.apply_eof(true);
    let stopped = reducer.snapshot();
    assert_eq!(stopped.session, DisplayStatus::Stopped);
    assert!(stopped.symbols[0].rate.as_ref().expect("rate").historical);

    reducer.apply_event(&rate_event(
        "EURGBP",
        "0.9001",
        UnixNanos(1_774_483_200_000_000_000),
        UnixNanos(8_000),
        3,
    ));
    let terminal = reducer.snapshot();
    assert_eq!(terminal.session, DisplayStatus::Stopped);
    assert!(terminal.symbols[0].rate.as_ref().expect("rate").historical);
}
