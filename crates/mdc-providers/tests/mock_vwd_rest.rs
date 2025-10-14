#![cfg(feature = "vwd")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

use std::str::FromStr;
use std::time::Duration;

use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, Side, SourceErrorKind};
use mdc_providers::vwd::config::{VwdConfig, VwdCredentials, VwdSymbol};
use mdc_providers::vwd::connector::VwdConnector;
use mdc_testkit::{MockRest, RecordedEvent, RecordingSink, load_fixture};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, ResponseTemplate};

const SNAPSHOT_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json";
const INVERSE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/vwd/snapquote_inverse_only.json";
const MISSING_ASK_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/vwd/snapquote_missing_ask.json";
const EMPTY_ARRAY_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/vwd/empty_array.json";

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

#[tokio::test]
async fn test_rest_snapshot_success() {
    let mock = MockRest::start().await;
    let body = load_fixture(SNAPSHOT_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .and(path(
            "/test-account/JSON/QUOTE/SNAPQUOTE/EURUSD.XRATE.SPOT~X,GBPUSD.XRATE.SPOT~X",
        ))
        .and(header_exists("authorization"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = VwdConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .account_path("test-account")
        .credentials(VwdCredentials::new("user", "pass"))
        .add_symbol(VwdSymbol::direct("EURUSD"))
        .add_symbol(VwdSymbol::direct("GBPUSD"))
        .poll_interval(Duration::from_millis(50))
        .emit_book_ticker(true)
        .build()
        .expect("build config");

    let connector = VwdConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();
    assert!(!events.is_empty(), "Events should be recorded");

    // Check that EURUSD snapshot is recorded
    let has_eurusd_begin = events.iter().any(|e| match e {
        RecordedEvent::BeginBook { symbol, kind } => {
            symbol.as_str() == "EURUSD" && *kind == BookKind::Snapshot
        }
        _ => false,
    });
    assert!(has_eurusd_begin, "Expected EURUSD Snapshot begin");

    let has_eurusd_bid = events.iter().any(|e| match e {
        RecordedEvent::Level {
            side,
            price,
            volume,
        } => *side == Side::Bid && *price == d("1.0842") && *volume == d("200000000"),
        _ => false,
    });
    assert!(has_eurusd_bid, "Expected EURUSD bid level");

    let has_eurusd_ask = events.iter().any(|e| match e {
        RecordedEvent::Level {
            side,
            price,
            volume,
        } => *side == Side::Ask && *price == d("1.0844") && *volume == d("200000000"),
        _ => false,
    });
    assert!(has_eurusd_ask, "Expected EURUSD ask level");

    // Check that GBPUSD snapshot is recorded
    let has_gbpusd_bid = events.iter().any(|e| match e {
        RecordedEvent::Level { side, price, .. } => *side == Side::Bid && *price == d("1.2950"),
        _ => false,
    });
    assert!(has_gbpusd_bid, "Expected GBPUSD bid level");

    // Check ResyncCompleted alert was emitted on first recovery
    let has_resync_completed = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => {
            matches!(alert, IntegrityAlert::ResyncCompleted { .. })
        }
        _ => false,
    });
    assert!(has_resync_completed, "Expected ResyncCompleted alert");

    // Check BookTicker was emitted
    let has_ticker = events.iter().any(|e| match e {
        RecordedEvent::BookTicker { symbol, ticker } => {
            symbol.as_str() == "EURUSD"
                && ticker.best_bid.price == d("1.0842")
                && ticker.best_ask.price == d("1.0844")
        }
        _ => false,
    });
    assert!(has_ticker, "Expected BookTicker event");
}

#[tokio::test]
async fn test_rest_auth_failure_401() {
    let mock = MockRest::start().await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(401))
        .mount(mock.inner())
        .await;

    let config = VwdConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .account_path("acct")
        .credentials(VwdCredentials::new("bad_user", "bad_pass"))
        .add_symbol(VwdSymbol::direct("EURUSD"))
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = VwdConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    // Must not emit book levels
    let has_levels = events
        .iter()
        .any(|e| matches!(e, RecordedEvent::Level { .. }));
    assert!(!has_levels, "No levels should be emitted on auth error");

    // Must emit SourceError { HttpStatus(401) }
    let has_source_error = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::HttpStatus(401),
            }
        ),
        _ => false,
    });
    assert!(has_source_error, "Expected SourceError HttpStatus(401)");
}

#[tokio::test]
async fn test_rest_fail_closed_recovery_flow() {
    let mock = MockRest::start().await;
    let body = load_fixture(SNAPSHOT_FIXTURE).expect("load fixture");

    // 1st request -> 200 OK
    // 2nd request -> 500 Error
    // 3rd request -> 200 OK
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body.clone(), "application/json"))
        .up_to_n_times(1)
        .mount(mock.inner())
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .mount(mock.inner())
        .await;

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = VwdConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .account_path("acct")
        .credentials(VwdCredentials::new("user", "pass"))
        .add_symbol(VwdSymbol::direct("EURUSD"))
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = VwdConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    // Allow multiple cycles to run
    tokio::time::sleep(Duration::from_millis(250)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    // Verify ordering:
    // 1. Snapshot + ResyncCompleted
    // 2. SourceError(HttpStatus(500))
    // 3. Snapshot + ResyncCompleted
    let alerts: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            RecordedEvent::Alert { alert, .. } => Some(*alert),
            _ => None,
        })
        .collect();

    assert!(
        alerts.len() >= 3,
        "Expected at least 3 alerts, found: {:?}",
        alerts
    );

    assert!(matches!(alerts[0], IntegrityAlert::ResyncCompleted { .. }));
    assert!(matches!(
        alerts[1],
        IntegrityAlert::SourceError {
            kind: SourceErrorKind::HttpStatus(500)
        }
    ));
    assert!(matches!(alerts[2], IntegrityAlert::ResyncCompleted { .. }));
}

#[tokio::test]
async fn test_rest_inverse_quote_handling() {
    let mock = MockRest::start().await;
    let body = load_fixture(INVERSE_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = VwdConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .account_path("acct")
        .credentials(VwdCredentials::new("user", "pass"))
        .add_symbol(VwdSymbol::with_inverse("EURUSD"))
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = VwdConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    let has_inverse_bid = events.iter().any(|e| match e {
        RecordedEvent::Level { side, price, .. } => *side == Side::Bid && *price == d("1.08420"),
        _ => false,
    });
    assert!(has_inverse_bid, "Expected inverted bid 1.08420");

    let has_inverse_ask = events.iter().any(|e| match e {
        RecordedEvent::Level { side, price, .. } => *side == Side::Ask && *price == d("1.08440"),
        _ => false,
    });
    assert!(has_inverse_ask, "Expected inverted ask 1.08440");
}

#[tokio::test]
async fn test_rest_symbol_specific_errors() {
    let mock = MockRest::start().await;
    let body = load_fixture(MISSING_ASK_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = VwdConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .account_path("acct")
        .credentials(VwdCredentials::new("user", "pass"))
        .add_symbol(VwdSymbol::direct("EURUSD"))
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = VwdConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

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
        "Expected InvalidQuote alert for missing ask"
    );
}

#[tokio::test]
async fn test_rest_empty_array_error() {
    let mock = MockRest::start().await;
    let body = load_fixture(EMPTY_ARRAY_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = VwdConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .account_path("acct")
        .credentials(VwdCredentials::new("user", "pass"))
        .add_symbol(VwdSymbol::direct("EURUSD"))
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = VwdConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.cancel();
    let _ = runner.await;

    let events = sink.events();

    let has_missing_quote = events.iter().any(|e| match e {
        RecordedEvent::Alert { alert, .. } => matches!(
            alert,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::MissingQuote
            }
        ),
        _ => false,
    });
    assert!(
        has_missing_quote,
        "Expected MissingQuote alert for empty array response"
    );
}

#[tokio::test]
async fn test_rest_control_pause_resume() {
    let mock = MockRest::start().await;
    let body = load_fixture(SNAPSHOT_FIXTURE).expect("load fixture");

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(mock.inner())
        .await;

    let config = VwdConfig::builder()
        .base_url(mock.url().expect("valid mock url"))
        .account_path("acct")
        .credentials(VwdCredentials::new("user", "pass"))
        .add_symbol(VwdSymbol::direct("EURUSD"))
        .poll_interval(Duration::from_millis(50))
        .build()
        .expect("build config");

    let connector = VwdConnector::builder(config)
        .build()
        .expect("build connector");
    let sink = RecordingSink::new();
    let cancel = CancellationToken::new();

    let (handle, runner) = connector.run(sink.clone(), cancel.child_token());

    // Let 1 poll execute
    tokio::time::sleep(Duration::from_millis(80)).await;
    let count_before = sink.events().len();
    assert!(count_before > 0, "Initial poll must have executed");

    // Pause polling
    handle.pause().await.expect("pause");
    tokio::time::sleep(Duration::from_millis(120)).await;
    let count_during_pause = sink.events().len();
    assert_eq!(
        count_during_pause,
        count_before + 1,
        "Only the pause alert should be emitted while paused"
    );
    let has_paused = sink.events().iter().any(|ev| {
        matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ConnectionLost {
                    reason: DisconnectKind::Paused,
                },
                ..
            }
        )
    });
    assert!(has_paused, "Pause must emit ConnectionLost {{ reason: DisconnectKind::Paused }}");

    // Resume polling
    handle.resume().await.expect("resume");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let count_after_resume = sink.events().len();
    assert!(
        count_after_resume > count_during_pause,
        "Events should resume being emitted after resume"
    );

    cancel.cancel();
    let _ = runner.await;
}
