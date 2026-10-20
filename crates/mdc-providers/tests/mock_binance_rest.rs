#![cfg(feature = "binance")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock REST integration tests for Binance connector.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mdc_core::resync::{ResyncController, SyncState};
use mdc_core::{
    DeliveryStamp, EmbargoTracker, L2Delta, MarketEvent, PriceLevel, RunId, SequenceRange,
    SnapshotOutcome, Symbol, UnixNanos, VenueId,
};
use mdc_providers::binance::normalize::normalize_snapshot;
use mdc_providers::binance::rest::{BinanceRestClient, RestError};
use mdc_testkit::{MockRest, load_fixture};
use rust_decimal::Decimal;

#[tokio::test]
async fn test_rest_snapshot_success() {
    let mock = MockRest::start().await;
    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");

    mock.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    let embargo = Arc::new(Mutex::new(EmbargoTracker::new()));
    let client = BinanceRestClient::new(
        mock.url().expect("valid mock url"),
        embargo,
        Duration::from_secs(5),
    )
    .expect("create client");

    let now = Instant::now();
    let snapshot = client
        .fetch_depth("BTCUSDT", now)
        .await
        .expect("fetch depth");

    assert_eq!(snapshot.last_update_id, 1027024);
    assert!(!snapshot.bids.is_empty());
    assert!(!snapshot.asks.is_empty());

    let normalized = normalize_snapshot(&snapshot);
    assert_eq!(normalized.last_update_id, Some(1027024));
    assert_eq!(normalized.bids.len(), snapshot.bids.len());
    assert_eq!(normalized.asks.len(), snapshot.asks.len());
}

#[tokio::test]
async fn test_rest_rate_limit_429_sets_embargo() {
    let mock = MockRest::start().await;
    mock.mock_binance_depth_status("BTCUSDT", 429, Some(2))
        .await;

    let embargo = Arc::new(Mutex::new(EmbargoTracker::new()));
    let client = BinanceRestClient::new(
        mock.url().expect("valid mock url"),
        Arc::clone(&embargo),
        Duration::from_secs(5),
    )
    .expect("create client");

    let t0 = Instant::now();
    let err = client.fetch_depth("BTCUSDT", t0).await.unwrap_err();
    assert!(matches!(err, RestError::RateLimited(429)));

    // Immediate second call should be blocked locally by the embargo without network request
    let t1 = t0 + Duration::from_millis(500);
    let err2 = client.fetch_depth("BTCUSDT", t1).await.unwrap_err();
    assert!(matches!(err2, RestError::Embargoed(_)));

    // After Retry-After duration has passed, embargo permits requests again
    let t2 = t0 + Duration::from_secs(3);
    // The embargo is now clear; this call will proceed to the network (and get 429 again)
    let err3 = client.fetch_depth("BTCUSDT", t2).await.unwrap_err();
    assert!(matches!(err3, RestError::RateLimited(429)));
}

#[tokio::test]
async fn test_rest_delayed_429_embargo_runs_from_response_receipt() {
    const RETRY_AFTER: Duration = Duration::from_secs(2);
    const SERVER_DELAY: Duration = Duration::from_millis(400);

    let mock = MockRest::start().await;
    mock.mock_binance_depth_status_delayed(
        "BTCUSDT",
        429,
        Some(RETRY_AFTER.as_secs()),
        SERVER_DELAY,
    )
    .await;

    let embargo = Arc::new(Mutex::new(EmbargoTracker::new()));
    let client = BinanceRestClient::new(
        mock.url().expect("valid mock url"),
        Arc::clone(&embargo),
        Duration::from_secs(5),
    )
    .expect("create client");

    let t0 = Instant::now();
    let err = client.fetch_depth("BTCUSDT", t0).await.unwrap_err();
    let returned = Instant::now();
    assert!(matches!(err, RestError::RateLimited(429)));

    let until = embargo
        .lock()
        .expect("embargo lock")
        .embargo_until()
        .expect("429 must set an embargo");
    // The window starts when the 429 is received, not when the request was issued.
    assert!(
        until >= t0 + SERVER_DELAY + RETRY_AFTER,
        "embargo must outlast server delay plus Retry-After, got {:?} from request start",
        until - t0
    );
    assert!(
        until <= returned + RETRY_AFTER,
        "embargo must not exceed Retry-After from receipt"
    );

    // Still embargoed just before the deadline, allowed at it.
    let err2 = client
        .fetch_depth("BTCUSDT", until - Duration::from_millis(1))
        .await
        .unwrap_err();
    assert!(matches!(err2, RestError::Embargoed(u) if u == until));
}

#[tokio::test]
async fn test_rest_rate_limit_418_ip_ban() {
    let mock = MockRest::start().await;
    mock.mock_binance_depth_status("ETHUSDT", 418, Some(60))
        .await;

    let embargo = Arc::new(Mutex::new(EmbargoTracker::new()));
    let client = BinanceRestClient::new(
        mock.url().expect("valid mock url"),
        Arc::clone(&embargo),
        Duration::from_secs(5),
    )
    .expect("create client");

    let t0 = Instant::now();
    let err = client.fetch_depth("ETHUSDT", t0).await.unwrap_err();
    assert!(matches!(err, RestError::RateLimited(418)));

    // Embargo is active for 60 seconds
    let t1 = t0 + Duration::from_secs(10);
    let err2 = client.fetch_depth("ETHUSDT", t1).await.unwrap_err();
    assert!(matches!(err2, RestError::Embargoed(_)));
}

#[tokio::test]
async fn test_rest_snapshot_reconciliation_flow() {
    let mock = MockRest::start().await;
    let snapshot_json =
        r#"{"lastUpdateId":1000,"bids":[["25000.00","1.0"]],"asks":[["25001.00","2.0"]]}"#;
    mock.mock_binance_depth_ok("BTCUSDT", snapshot_json).await;

    let embargo = Arc::new(Mutex::new(EmbargoTracker::new()));
    let client = BinanceRestClient::new(
        mock.url().expect("valid mock url"),
        embargo,
        Duration::from_secs(5),
    )
    .expect("create client");

    let mut controller = ResyncController::new(100).expect("valid controller");
    assert_eq!(controller.state(), SyncState::BufferingDiffs);

    // Buffer delta that bridges snapshot lastUpdateId (990..1010)
    let delta = MarketEvent {
        exchange_ts: None,
        received_ts: UnixNanos::from_millis(100),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
        source_sequence: Some(1010),
        kind: L2Delta {
            sequence: Some(SequenceRange::new(990, 1010)),
            bids: vec![PriceLevel::new(Decimal::from(25000), Decimal::from(2))],
            asks: vec![PriceLevel::new(Decimal::from(25001), Decimal::from(3))],
        },
    };
    controller.buffer(delta).expect("buffer delta");

    // Fetch snapshot
    let raw_snap = client
        .fetch_depth("BTCUSDT", Instant::now())
        .await
        .expect("fetch snapshot");
    let norm_snap = normalize_snapshot(&raw_snap);
    let snap_event = MarketEvent {
        exchange_ts: None,
        received_ts: UnixNanos::from_millis(100),
        venue: VenueId::Binance,
        symbol: Symbol::from("BTCUSDT"),
        tag: 0,
        delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 2),
        source_sequence: norm_snap.last_update_id,
        kind: norm_snap,
    };

    let outcome = controller
        .on_snapshot(snap_event)
        .expect("reconcile snapshot");
    match outcome {
        SnapshotOutcome::Synchronized { replay, .. } => {
            assert_eq!(controller.state(), SyncState::InSync);
            assert_eq!(replay.len(), 1);
            assert_eq!(controller.tracker().current(), Some(1010));
        }
        other => panic!("Expected Synchronized, got {other:?}"),
    }
}
