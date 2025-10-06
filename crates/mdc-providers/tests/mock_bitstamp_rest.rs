#![cfg(feature = "bitstamp")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock REST integration tests for Bitstamp connector.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mdc_core::integrity::EmbargoTracker;
use mdc_providers::bitstamp::normalize::normalize_snapshot;
use mdc_providers::bitstamp::rest::{BitstampRestClient, RestError};
use mdc_testkit::{MockRest, load_fixture};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

const SNAPSHOT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/bitstamp/order_book_snapshot.json";

#[tokio::test]
async fn test_rest_snapshot_success() {
    let mock = MockRest::start().await;
    let snapshot_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");

    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(snapshot_json, "application/json"))
        .mount(mock.inner())
        .await;

    let embargo = Arc::new(Mutex::new(EmbargoTracker::new()));
    let client = BitstampRestClient::new(
        mock.url().expect("valid mock url"),
        embargo,
        Duration::from_secs(5),
    )
    .expect("create client");

    let now = Instant::now();
    let snapshot = client
        .fetch_order_book("btcusd", now)
        .await
        .expect("fetch order book");

    assert_eq!(snapshot.microtimestamp, 1700000000100000);
    assert!(!snapshot.bids.is_empty());
    assert!(!snapshot.asks.is_empty());

    let mut scratch = mdc_providers::bitstamp::BitstampConnectionScratch::with_capacity(64);
    for b in &snapshot.bids {
        scratch.bids.push(mdc_core::PriceLevel::new(b[0], b[1]));
    }
    for a in &snapshot.asks {
        scratch.asks.push(mdc_core::PriceLevel::new(a[0], a[1]));
    }
    let (ts, normalized) = normalize_snapshot(snapshot.microtimestamp, &scratch);
    assert_eq!(ts, mdc_core::UnixNanos::from_micros(1700000000100000));
    assert_eq!(normalized.last_update_id, None);
    assert_eq!(normalized.bids.len(), snapshot.bids.len());
    assert_eq!(normalized.asks.len(), snapshot.asks.len());
}

#[tokio::test]
async fn test_rest_rate_limit_429_sets_embargo() {
    let mock = MockRest::start().await;

    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("retry-after", "2")
                .set_body_string(r#"{"error": "Rate limit exceeded"}"#),
        )
        .mount(mock.inner())
        .await;

    let embargo = Arc::new(Mutex::new(EmbargoTracker::new()));
    let client = BitstampRestClient::new(
        mock.url().expect("valid mock url"),
        Arc::clone(&embargo),
        Duration::from_secs(5),
    )
    .expect("create client");

    let t0 = Instant::now();
    let err = client.fetch_order_book("btcusd", t0).await.unwrap_err();
    assert!(matches!(err, RestError::RateLimited(429)));

    // Immediate second call should be blocked locally by the embargo without making network request
    let t1 = t0 + Duration::from_millis(500);
    let err2 = client.fetch_order_book("btcusd", t1).await.unwrap_err();
    assert!(matches!(err2, RestError::Embargoed(_)));

    // After Retry-After duration (2s) has passed, embargo allows request through again
    let t2 = t0 + Duration::from_secs(3);
    let err3 = client.fetch_order_book("btcusd", t2).await.unwrap_err();
    assert!(matches!(err3, RestError::RateLimited(429)));
}

#[tokio::test]
async fn test_rest_http_500_error() {
    let mock = MockRest::start().await;

    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(
            ResponseTemplate::new(500).set_body_string(r#"{"error": "Internal server error"}"#),
        )
        .mount(mock.inner())
        .await;

    let embargo = Arc::new(Mutex::new(EmbargoTracker::new()));
    let client = BitstampRestClient::new(
        mock.url().expect("valid mock url"),
        embargo,
        Duration::from_secs(5),
    )
    .expect("create client");

    let now = Instant::now();
    let err = client.fetch_order_book("btcusd", now).await.unwrap_err();
    assert!(matches!(err, RestError::HttpStatus(500)));
}
