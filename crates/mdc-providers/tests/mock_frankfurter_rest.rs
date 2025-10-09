#![cfg(feature = "frankfurter")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

use std::str::FromStr;
use std::time::Duration;

use mdc_core::{DisconnectKind, IntegrityAlert, SourceErrorKind};
use mdc_providers::frankfurter::{FrankfurterConfig, FrankfurterConnector};
use mdc_testkit::{MockRest, RecordedEvent, RecordingSink, load_fixture};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

const RATES_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json";
const IDENTITY_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/frankfurter/identity_row.json";
const MISSING_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/frankfurter/missing_pair.json";
const INVALID_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/frankfurter/invalid_rate.json";
const CONFLICT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/frankfurter/conflicting_pair.json";

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

#[tokio::test]
async fn test_rest_rates_success_and_ordering() {
    let mock = MockRest::start().await;
    let body = load_fixture(RATES_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .and(path("/v2/providers/ecb/rates"))
        .and(query_param("base", "EUR"))
        .and(query_param("quotes", "GBP,USD"))
        .and(header("accept", "application/json"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["GBP".to_string(), "USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();
    assert!(!events.is_empty(), "Events should be recorded");

    // Check ResyncStarted on startup
    let resync_started_gbp = events.iter().any(|e| match e {
        RecordedEvent::Alert { symbol, alert } => {
            symbol.as_str() == "EURGBP" && *alert == IntegrityAlert::ResyncStarted
        }
        _ => false,
    });
    assert!(resync_started_gbp, "Expected EURGBP ResyncStarted");

    // Check ReferenceRate emitted in canonical order (EURGBP before EURUSD)
    let rate_gbp = events.iter().find(|e| match e {
        RecordedEvent::ReferenceRate { symbol, rate } => {
            symbol.as_str() == "EURGBP" && rate.mid == d("0.8623")
        }
        _ => false,
    });
    assert!(rate_gbp.is_some(), "Expected EURGBP rate 0.8623");

    let rate_usd = events.iter().find(|e| match e {
        RecordedEvent::ReferenceRate { symbol, rate } => {
            symbol.as_str() == "EURUSD" && rate.mid == d("1.089")
        }
        _ => false,
    });
    assert!(rate_usd.is_some(), "Expected EURUSD rate 1.089");

    // Check ResyncCompleted emitted for both symbols
    let completed_gbp = events.iter().any(|e| match e {
        RecordedEvent::Alert { symbol, alert } => {
            symbol.as_str() == "EURGBP"
                && *alert
                    == IntegrityAlert::ResyncCompleted {
                        last_update_id: None,
                    }
        }
        _ => false,
    });
    assert!(completed_gbp, "Expected EURGBP ResyncCompleted");
}

#[tokio::test]
async fn test_rest_identity_row_skipped() {
    let mock = MockRest::start().await;
    let body = load_fixture(IDENTITY_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .and(path("/v2/providers/ecb/rates"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["GBP".to_string(), "USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    // Verify identity row (EUREUR) was NOT emitted
    let has_eureur = events.iter().any(|e| match e {
        RecordedEvent::ReferenceRate { symbol, .. } => symbol.as_str() == "EUREUR",
        _ => false,
    });
    assert!(!has_eureur, "Identity row EUREUR must be skipped");
}

#[tokio::test]
async fn test_rest_missing_pair_rejects_batch() {
    let mock = MockRest::start().await;
    let body = load_fixture(MISSING_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["GBP".to_string(), "USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    // No ReferenceRate should be published for partial batch
    let has_rate = events
        .iter()
        .any(|e| matches!(e, RecordedEvent::ReferenceRate { .. }));
    assert!(!has_rate, "Partial batch must NOT emit any ReferenceRate");

    // Must emit SourceError(MissingQuote)
    let has_missing_quote = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::MissingQuote
            }
        ),
        _ => false,
    });
    assert!(has_missing_quote, "Expected MissingQuote alert");
}

#[tokio::test]
async fn test_rest_invalid_rate_rejects_batch() {
    let mock = MockRest::start().await;
    let body = load_fixture(INVALID_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["GBP".to_string(), "USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();
    let has_rate = events
        .iter()
        .any(|e| matches!(e, RecordedEvent::ReferenceRate { .. }));
    assert!(!has_rate, "Invalid rate must reject entire batch");

    let has_invalid_quote = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::InvalidQuote
            }
        ),
        _ => false,
    });
    assert!(has_invalid_quote, "Expected InvalidQuote alert");
}

#[tokio::test]
async fn test_rest_conflicting_pair_rejects_batch() {
    let mock = MockRest::start().await;
    let body = load_fixture(CONFLICT_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();
    let has_rate = events
        .iter()
        .any(|e| matches!(e, RecordedEvent::ReferenceRate { .. }));
    assert!(!has_rate, "Duplicate conflicting pair must reject batch");

    let has_invalid_quote = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::InvalidQuote
            }
        ),
        _ => false,
    });
    assert!(
        has_invalid_quote,
        "Expected InvalidQuote alert for duplicate pair"
    );
}

#[tokio::test]
async fn test_rest_http_429_retry_after() {
    let mock = MockRest::start().await;

    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "1")
                .set_body_string("Too Many Requests"),
        )
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();
    let has_429 = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::HttpStatus(429)
            }
        ),
        _ => false,
    });
    assert!(has_429, "Expected HttpStatus(429) alert");
}

#[tokio::test]
async fn test_rest_http_500_server_error() {
    let mock = MockRest::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(120)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();
    let has_500 = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::HttpStatus(500)
            }
        ),
        _ => false,
    });
    assert!(has_500, "Expected HttpStatus(500) alert");
}

#[tokio::test]
async fn test_rest_fail_then_recover_resync_completed() {
    let mock = MockRest::start().await;
    let body = load_fixture(RATES_FIXTURE).expect("load fixture");

    // First request fails with 500, subsequent succeeds with 200
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Error"))
        .up_to_n_times(1)
        .mount(mock.inner())
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["GBP".to_string(), "USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    // Wait enough time for fail then recovery retry
    tokio::time::sleep(Duration::from_millis(1200)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    // Verify 500 occurred first
    let has_500 = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::HttpStatus(500)
            }
        ),
        _ => false,
    });
    assert!(has_500, "First attempt should fail with 500");

    // Verify recovery succeeded with ReferenceRate and ResyncCompleted
    let has_rate = events
        .iter()
        .any(|e| matches!(e, RecordedEvent::ReferenceRate { .. }));
    assert!(has_rate, "Recovery should emit ReferenceRate");

    let has_completed = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => {
            matches!(alert, IntegrityAlert::ResyncCompleted { .. })
        }
        _ => false,
    });
    assert!(has_completed, "Recovery should emit ResyncCompleted");
}

#[tokio::test]
async fn test_rest_control_pause_resume() {
    let mock = MockRest::start().await;
    let body = load_fixture(RATES_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (handle, runner) = connector.run(sink.clone(), cancel.child_token());

    // Let 1 poll execute
    tokio::time::sleep(Duration::from_millis(80)).await;
    let count_before = sink.events().len();
    assert!(count_before > 0, "Initial poll must have executed");

    // Pause
    handle.pause().await.expect("pause");
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Verify ConnectionLost(Paused) was emitted
    let has_paused_alert = sink.events().iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::Paused
            }
        ),
        _ => false,
    });
    assert!(has_paused_alert, "Pause should emit ConnectionLost(Paused)");

    let count_during_pause = sink.events().len();

    // Resume
    handle.resume().await.expect("resume");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count_after_resume = sink.events().len();
    assert!(
        count_after_resume > count_during_pause,
        "Events should resume after resume"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_rest_control_set_symbols() {
    let mock = MockRest::start().await;
    let rates_body = load_fixture(RATES_FIXTURE).expect("load fixture");
    let usd_only_body = load_fixture(MISSING_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .and(query_param("quotes", "GBP,USD"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(rates_body, "application/json"))
        .mount(mock.inner())
        .await;

    Mock::given(method("GET"))
        .and(query_param("quotes", "USD"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(usd_only_body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["GBP".to_string(), "USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (handle, runner) = connector.run(sink.clone(), cancel.child_token());

    // Wait for first poll with GBP,USD
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Update symbols to USD only (removes GBP)
    handle
        .set_symbols(vec!["USD".to_string()])
        .await
        .expect("set_symbols");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check that EURGBP received ConnectionLost(Closed)
    let has_closed_gbp = sink.events().iter().any(|e| match e {
        RecordedEvent::Alert { symbol, alert } => {
            symbol.as_str() == "EURGBP"
                && matches!(
                    alert,
                    IntegrityAlert::ConnectionLost {
                        reason: DisconnectKind::Closed
                    }
                )
        }
        _ => false,
    });
    assert!(
        has_closed_gbp,
        "Removed EURGBP must receive ConnectionLost(Closed)"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_rest_repeated_poll_does_not_repeat_resync_completed() {
    let mock = MockRest::start().await;
    let body = load_fixture(RATES_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["GBP".to_string(), "USD".to_string()])
        .poll_interval(Duration::from_millis(40))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    // Allow multiple polls to execute
    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    // Count ResyncCompleted alerts for EURGBP
    let completed_count = events
        .iter()
        .filter(|e| match e {
            RecordedEvent::Alert { symbol, alert } => {
                symbol.as_str() == "EURGBP"
                    && matches!(alert, IntegrityAlert::ResyncCompleted { .. })
            }
            _ => false,
        })
        .count();

    assert_eq!(
        completed_count, 1,
        "ResyncCompleted should only be emitted once on initial delivery, not on repeated polls"
    );

    // Verify multiple ReferenceRate events were emitted
    let rates_count = events
        .iter()
        .filter(|e| match e {
            RecordedEvent::ReferenceRate { symbol, .. } => symbol.as_str() == "EURGBP",
            _ => false,
        })
        .count();
    assert!(
        rates_count >= 2,
        "Multiple rate observations should be delivered across polls"
    );
}

#[tokio::test]
async fn test_rest_unrequested_pair_rejects_batch() {
    let mock = MockRest::start().await;
    let body = load_fixture(RATES_FIXTURE).expect("load fixture"); // Contains GBP and USD

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    // Config ONLY requests USD (so GBP in the payload is unrequested)
    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    // No ReferenceRate should be published
    let has_rate = events
        .iter()
        .any(|e| matches!(e, RecordedEvent::ReferenceRate { .. }));
    assert!(!has_rate, "Unrequested pair must reject entire batch");

    // Must emit SourceError(InvalidQuote)
    let has_invalid_quote = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::InvalidQuote
            }
        ),
        _ => false,
    });
    assert!(
        has_invalid_quote,
        "Expected InvalidQuote alert for unrequested pair"
    );
}

#[tokio::test]
async fn test_rest_oversized_body_rejected() {
    let mock = MockRest::start().await;
    let large_body = "x".repeat(200);

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(large_body))
        .mount(mock.inner())
        .await;

    let config = FrankfurterConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .base_currency("EUR")
        .quote_currencies(vec!["USD".to_string()])
        .poll_interval(Duration::from_millis(50))
        .body_cap_bytes(50) // Small body cap
        .build()
        .expect("build config");

    let connector = FrankfurterConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();
    let has_transport_error = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::Transport
            }
        ),
        _ => false,
    });
    assert!(
        has_transport_error,
        "Oversized body must trigger Transport error"
    );
}
