//! Reducer unit tests: ordered transitions, per-symbol isolation, exact
//! decimal state, historical labelling, and alert-history bounds.

use mdc_core::{
    DeliveryStamp, DisconnectKind, IntegrityAlert, L2Snapshot, MarketDataKind, MarketEvent,
    PriceLevel, RunId, Symbol, UnixNanos, VenueId,
};
use mdc_sinks::loss_intolerant::{DeliveryFault, DeliveryFaultReason};
use rust_decimal::Decimal;
use std::str::FromStr;

use super::Reducer;
use crate::config::SessionConfig;
use crate::display::DisplayStatus;
use crate::provider::{ProviderKind, SessionBounds};

fn d(value: &str) -> Decimal {
    Decimal::from_str(value).expect("valid decimal")
}

fn level(price: &str, volume: &str) -> PriceLevel {
    PriceLevel::new(d(price), d(volume))
}

fn bounds(depth: usize) -> SessionBounds {
    SessionBounds {
        max_symbols: 25,
        projector_depth: depth,
        affected_connection: 1,
    }
}

fn config(symbols: &[&str]) -> SessionConfig {
    let raw: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
    SessionConfig::new(ProviderKind::Test, &raw).expect("valid config")
}

fn event(symbol: &str, kind: MarketDataKind, ordinal: u64) -> MarketEvent<MarketDataKind> {
    MarketEvent {
        exchange_ts: Some(UnixNanos(1_000)),
        received_ts: UnixNanos(2_000 + ordinal),
        venue: VenueId::Binance,
        symbol: Symbol::from(symbol),
        tag: 0,
        delivery: DeliveryStamp {
            run: RunId::from_bytes([0u8; 16]),
            connection_id: 0,
            connection_generation: 1,
            ordinal,
        },
        source_sequence: None,
        kind,
    }
}

fn snapshot(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> MarketDataKind {
    MarketDataKind::L2Snapshot(L2Snapshot {
        bids,
        asks,
        last_update_id: Some(1),
    })
}

fn balanced_snapshot() -> MarketDataKind {
    snapshot(
        vec![level("100.500", "10"), level("100.400", "20")],
        vec![level("100.600", "5"), level("100.700", "8")],
    )
}

fn resync_completed() -> MarketDataKind {
    MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
        last_update_id: Some(2),
    })
}

#[cfg(feature = "frankfurter")]
mod reference_rates;

fn reduce(mut reducer: Reducer, items: &[MarketEvent<MarketDataKind>]) -> Reducer {
    for item in items {
        reducer.apply_event(item);
    }
    reducer
}

fn status_of(reducer: &Reducer, symbol: &str) -> DisplayStatus {
    reducer
        .snapshot()
        .symbols
        .iter()
        .find(|s| s.symbol.as_str() == symbol)
        .map(|s| s.status)
        .unwrap_or(DisplayStatus::Empty)
}

#[test]
fn initial_state_is_loading_per_symbol() {
    let reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(100));
    let snap = reducer.snapshot();
    assert_eq!(snap.session, DisplayStatus::Loading);
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::Loading);
    assert!(snap.symbols[0].quote.is_none());
}

#[test]
fn snapshot_then_resync_completed_produces_in_sync_and_exact_decimals() {
    // The projector buffers the initial snapshot as a resync candidate and
    // releases it on ResyncCompleted, matching the provider contract.
    let mut reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(100));
    reducer.apply_event(&event("BTCUSDT", balanced_snapshot(), 1));
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::Loading);
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 2));
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::InSync);
    let symbol = &reducer.snapshot().symbols[0];
    let quote = symbol.quote.as_ref().expect("quote retained");
    assert!(!quote.historical);
    assert_eq!(quote.quote.bid, d("100.500"));
    assert_eq!(quote.quote.ask, d("100.600"));
    assert_eq!(quote.quote.bid_size, d("10"));
    assert_eq!(quote.quote.ask_size, d("5"));
    assert_eq!(quote.quote.spread, Some(d("0.100")));
    assert_eq!(symbol.bids.len(), 2);
    assert_eq!(symbol.asks.len(), 2);
}

#[test]
fn steady_snapshot_after_gap_resyncs_directly() {
    // After an invalidating alert the projector is not resyncing, so a fresh
    // snapshot applies directly and restores InSync without a handshake.
    let reducer = reduce(
        Reducer::new(&config(&["BTCUSDT"]), bounds(100)),
        &[
            event(
                "BTCUSDT",
                MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
                    expected: 1,
                    received: 4,
                }),
                1,
            ),
            event(
                "BTCUSDT",
                snapshot(vec![level("300.000", "7")], vec![level("300.100", "3")]),
                2,
            ),
        ],
    );
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::InSync);
    let snap = reducer.snapshot();
    let quote = snap.symbols[0].quote.as_ref().expect("quote retained");
    assert_eq!(quote.quote.bid, d("300.000"));
}

#[test]
fn gap_makes_values_unavailable_and_retains_historical_quote() {
    let reducer = reduce(
        Reducer::new(&config(&["BTCUSDT"]), bounds(100)),
        &[
            event("BTCUSDT", balanced_snapshot(), 1),
            event("BTCUSDT", resync_completed(), 2),
            event(
                "BTCUSDT",
                MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
                    expected: 2,
                    received: 5,
                }),
                3,
            ),
        ],
    );
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::Unavailable);
    let symbol = &reducer.snapshot().symbols[0];
    let quote = symbol.quote.as_ref().expect("last quote retained");
    assert!(quote.historical, "retained quote must be historical");
    assert_eq!(quote.quote.bid, d("100.500"));
    assert!(symbol.unavailability.as_deref().unwrap().contains("gap"));
}

#[test]
fn resync_passes_through_awaiting_and_recovery_restores_in_sync() {
    let events = [
        event("BTCUSDT", balanced_snapshot(), 1),
        event("BTCUSDT", resync_completed(), 2),
        event(
            "BTCUSDT",
            MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
                expected: 3,
                received: 9,
            }),
            3,
        ),
        event(
            "BTCUSDT",
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncStarted),
            4,
        ),
        // Post-gap snapshot buffers as a resync candidate.
        event(
            "BTCUSDT",
            snapshot(vec![level("200.100", "1")], vec![level("200.200", "2")]),
            5,
        ),
        // Completion releases the candidate and restores InSync.
        event(
            "BTCUSDT",
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
                last_update_id: Some(12),
            }),
            6,
        ),
    ];
    let mut reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(100));
    for (index, item) in events.iter().enumerate() {
        reducer.apply_event(item);
        let status = status_of(&reducer, "BTCUSDT");
        match index {
            0 => assert_eq!(status, DisplayStatus::Loading, "step {index}"),
            1 => assert_eq!(status, DisplayStatus::InSync, "step {index}"),
            2 => assert_eq!(status, DisplayStatus::Unavailable, "step {index}"),
            3 | 4 => assert_eq!(
                status,
                DisplayStatus::AwaitingSnapshot,
                "resync keeps values unavailable until completion (step {index})"
            ),
            _ => {}
        }
    }
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::InSync);
    let symbol = &reducer.snapshot().symbols[0];
    let quote = symbol.quote.as_ref().expect("recovered quote");
    assert!(!quote.historical);
    assert_eq!(quote.quote.bid, d("200.100"));
    // Alert history preserves gap and recovery ordering.
    let snap = reducer.snapshot();
    let summaries: Vec<&str> = snap
        .alerts
        .iter()
        .map(|record| record.summary.as_str())
        .collect();
    assert_eq!(
        summaries,
        vec![
            "resync completed (aligned to 2)",
            "gap detected (expected 3, received 9)",
            "resync started",
            "resync completed (aligned to 12)",
        ]
    );
}

#[test]
fn resync_completed_without_snapshot_is_unavailable() {
    // Completion with no candidate book cannot release anything: the
    // projector reports NotSynchronized, which is an unavailable state.
    let reducer = reduce(
        Reducer::new(&config(&["BTCUSDT"]), bounds(100)),
        &[
            event(
                "BTCUSDT",
                MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncStarted),
                1,
            ),
            event(
                "BTCUSDT",
                MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
                    last_update_id: None,
                }),
                2,
            ),
        ],
    );
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::Unavailable);
}

#[test]
fn invalidating_alerts_make_data_unavailable() {
    let alerts = [
        IntegrityAlert::ChecksumMismatch {
            expected: 1,
            calculated: 2,
        },
        IntegrityAlert::SilenceTimeout { elapsed_ms: 30_000 },
        IntegrityAlert::HeartbeatTimeout { elapsed_ms: 30_000 },
        IntegrityAlert::Quarantined { retry_in_ms: 500 },
        IntegrityAlert::SourceError {
            kind: mdc_core::SourceErrorKind::Transport,
        },
        IntegrityAlert::ConnectionLost {
            reason: DisconnectKind::TransportError,
        },
        IntegrityAlert::SinkFailure,
    ];
    for alert in alerts {
        let reducer = reduce(
            Reducer::new(&config(&["BTCUSDT"]), bounds(100)),
            &[
                event("BTCUSDT", balanced_snapshot(), 1),
                event("BTCUSDT", resync_completed(), 2),
                event("BTCUSDT", MarketDataKind::IntegrityAlert(alert), 3),
            ],
        );
        assert_eq!(
            status_of(&reducer, "BTCUSDT"),
            DisplayStatus::Unavailable,
            "alert {alert:?} must invalidate current data"
        );
    }
}

#[test]
fn paused_connection_is_observational() {
    let reducer = reduce(
        Reducer::new(&config(&["BTCUSDT"]), bounds(100)),
        &[event(
            "BTCUSDT",
            MarketDataKind::IntegrityAlert(IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::Paused,
            }),
            1,
        )],
    );
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::Paused);
    let symbol = &reducer.snapshot().symbols[0];
    assert!(symbol.quote.is_none());
}

#[test]
fn crossed_book_snapshot_is_unavailable() {
    let reducer = reduce(
        Reducer::new(&config(&["BTCUSDT"]), bounds(100)),
        &[event(
            "BTCUSDT",
            snapshot(vec![level("101.000", "1")], vec![level("100.000", "1")]),
            1,
        )],
    );
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::Unavailable);
}

#[test]
fn empty_side_snapshot_is_unavailable_after_release() {
    // A one-sided snapshot buffers as a candidate (still loading) and
    // surfaces EmptySide when ResyncCompleted releases it.
    let mut reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(100));
    reducer.apply_event(&event(
        "BTCUSDT",
        snapshot(vec![level("100.0", "1")], vec![]),
        1,
    ));
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::Loading);
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 2));
    assert_eq!(status_of(&reducer, "BTCUSDT"), DisplayStatus::Unavailable);
}

#[test]
fn symbols_are_isolated() {
    let reducer = reduce(
        Reducer::new(&config(&["AAAUSDT", "BBBUSDT"]), bounds(100)),
        &[
            event("AAAUSDT", balanced_snapshot(), 1),
            event("AAAUSDT", resync_completed(), 2),
        ],
    );
    assert_eq!(status_of(&reducer, "AAAUSDT"), DisplayStatus::InSync);
    assert_eq!(status_of(&reducer, "BBBUSDT"), DisplayStatus::Loading);
    let symbols = reducer.snapshot().symbols;
    assert!(
        symbols
            .iter()
            .any(|s| s.symbol.as_str() == "AAAUSDT" && s.quote.is_some())
    );
    assert!(
        symbols
            .iter()
            .any(|s| s.symbol.as_str() == "BBBUSDT" && s.quote.is_none())
    );
}

#[test]
fn alert_history_is_bounded_and_reports_evictions() {
    let mut reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(100));
    for sequence in 0..300u64 {
        reducer.apply_event(&event(
            "BTCUSDT",
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncStarted),
            sequence,
        ));
    }
    let snap = reducer.snapshot();
    assert_eq!(snap.alerts.len(), 256);
    assert_eq!(snap.alerts_evicted, 44);
}

#[test]
fn delivery_fault_fails_session_and_marks_quotes_historical() {
    let mut reducer = reduce(
        Reducer::new(&config(&["BTCUSDT"]), bounds(100)),
        &[
            event("BTCUSDT", balanced_snapshot(), 1),
            event("BTCUSDT", resync_completed(), 2),
        ],
    );
    reducer.apply_fault(&DeliveryFault {
        stamp: DeliveryStamp::new(RunId::from_bytes([0u8; 16]), 1, 1),
        reason: DeliveryFaultReason::CapacityExceeded { capacity: 4096 },
        affected_connection: 1,
    });
    let snap = reducer.snapshot();
    assert_eq!(snap.session, DisplayStatus::Failed);
    assert!(snap.failure.is_some());
    let quote = snap.symbols[0].quote.as_ref().expect("quote retained");
    assert!(quote.historical);
}

#[test]
fn requested_stop_marks_retained_quote_historical() {
    let mut reducer = reduce(
        Reducer::new(&config(&["BTCUSDT"]), bounds(100)),
        &[
            event("BTCUSDT", balanced_snapshot(), 1),
            event("BTCUSDT", resync_completed(), 2),
        ],
    );
    reducer.apply_eof(true);
    let snapshot = reducer.snapshot();
    assert_eq!(snapshot.session, DisplayStatus::Stopped);
    assert!(
        snapshot.symbols[0]
            .quote
            .as_ref()
            .expect("quote")
            .historical
    );
}

#[test]
fn unexpected_eof_fails_but_requested_stop_stops() {
    let mut stopped = Reducer::new(&config(&["BTCUSDT"]), bounds(100));
    stopped.apply_eof(true);
    assert_eq!(stopped.snapshot().session, DisplayStatus::Stopped);

    let mut unexpected = Reducer::new(&config(&["BTCUSDT"]), bounds(100));
    unexpected.apply_eof(false);
    let snap = unexpected.snapshot();
    assert_eq!(snap.session, DisplayStatus::Failed);
    assert_eq!(snap.failure.as_deref(), Some("stream ended unexpectedly"));
}

#[test]
fn depth_cap_bounds_projector_book() {
    let reducer = reduce(
        Reducer::new(&config(&["BTCUSDT"]), bounds(2)),
        &[event(
            "BTCUSDT",
            snapshot(
                vec![level("1.0", "1"), level("2.0", "1"), level("3.0", "1")],
                vec![level("9.0", "1"), level("8.0", "1"), level("7.0", "1")],
            ),
            1,
        )],
    );
    let snap = reducer.snapshot();
    assert!(snap.symbols[0].bids.len() <= 2);
    assert!(snap.symbols[0].asks.len() <= 2);
}
