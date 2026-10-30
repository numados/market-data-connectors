//! Specific projection failures remain observable until recovery begins.

use super::*;

#[test]
fn new_resync_clears_previous_projection_failure() {
    let mut reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(1));
    reducer.apply_event(&event("BTCUSDT", balanced_snapshot(), 1));
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 2));
    reducer.apply_event(&event(
        "BTCUSDT",
        MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncStarted),
        3,
    ));
    let snapshot = reducer.snapshot();
    assert_eq!(snapshot.symbols[0].status, DisplayStatus::AwaitingSnapshot);
    assert!(snapshot.symbols[0].unavailability.is_none());
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 4));
    assert_eq!(
        reducer.snapshot().symbols[0].unavailability.as_deref(),
        Some("NotSynchronized")
    );
}

#[test]
fn recovered_symbol_does_not_change_its_peer() {
    let mut reducer = Reducer::new(&config(&["BTCUSDT", "ETHUSDT"]), bounds(1));
    reducer.apply_event(&event("BTCUSDT", balanced_snapshot(), 1));
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 2));
    let valid = snapshot(vec![level("100", "1")], vec![level("101", "2")]);
    reducer.apply_event(&event("ETHUSDT", valid.clone(), 3));
    reducer.apply_event(&event("ETHUSDT", resync_completed(), 4));
    let before = reducer.snapshot();
    assert_eq!(
        before.symbols[0].unavailability.as_deref(),
        Some("ExceededMaxDepth")
    );
    assert_eq!(before.symbols[1].status, DisplayStatus::InSync);
    reducer.apply_event(&event("BTCUSDT", valid, 5));
    let recovered = reducer.snapshot();
    assert_eq!(recovered.symbols[0].status, DisplayStatus::InSync);
    assert!(recovered.symbols[0].unavailability.is_none());
    assert!(recovered.symbols[0].quote.is_some());
    assert_eq!(recovered.symbols[1], before.symbols[1]);
}

#[test]
fn new_projection_failure_replaces_previous_cause() {
    let mut reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(1));
    reducer.apply_event(&event("BTCUSDT", balanced_snapshot(), 1));
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 2));
    let crossed = snapshot(vec![level("102", "1")], vec![level("101", "2")]);
    reducer.apply_event(&event("BTCUSDT", crossed, 3));
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 4));
    assert_eq!(
        reducer.snapshot().symbols[0].unavailability.as_deref(),
        Some("CrossedBook")
    );
}

#[test]
fn native_alert_is_not_pinned_as_a_projection_failure() {
    let mut reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(1));
    reducer.apply_event(&event("BTCUSDT", balanced_snapshot(), 1));
    reducer.apply_event(&event(
        "BTCUSDT",
        MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
            expected: 10,
            received: 12,
        }),
        2,
    ));
    assert_eq!(
        reducer.snapshot().symbols[0].unavailability.as_deref(),
        Some("gap detected (expected 10, received 12)")
    );
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 3));
    assert_eq!(
        reducer.snapshot().symbols[0].unavailability.as_deref(),
        Some("NotSynchronized")
    );
}

#[test]
fn depth_failure_survives_resync_completion() {
    let mut reducer = Reducer::new(&config(&["BTCUSDT"]), bounds(1));
    reducer.apply_event(&event("BTCUSDT", balanced_snapshot(), 1));
    assert_eq!(
        reducer.snapshot().symbols[0].unavailability.as_deref(),
        Some("ExceededMaxDepth")
    );
    reducer.apply_event(&event("BTCUSDT", resync_completed(), 2));
    let snapshot = reducer.snapshot();
    let symbol = &snapshot.symbols[0];
    assert_eq!(symbol.status, DisplayStatus::Unavailable);
    assert_eq!(symbol.unavailability.as_deref(), Some("ExceededMaxDepth"));
    assert!(symbol.bids.is_empty() && symbol.asks.is_empty() && symbol.quote.is_none());
}
