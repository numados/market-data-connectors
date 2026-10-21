#![cfg(feature = "all")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Integration tests for bounded I/O, task ownership, and configuration validation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use mdc_core::Symbol;
use mdc_core::error::MdcError;
use mdc_core::integrity::{ChecksumStrategy, RawLevelRef};
use mdc_core::resync::{ResyncDecision, ResyncFailure, ResyncPolicy};
use mdc_providers::binance::config::BinanceConfig;
use mdc_providers::binance::connector::BinanceConnectorBuilder;
use mdc_providers::bitstamp::config::BitstampConfig;
use mdc_providers::bitstamp::connector::BitstampConnectorBuilder;
use mdc_providers::coinbase::config::CoinbaseConfig;
use mdc_providers::coinbase::connector::CoinbaseConnectorBuilder;
use mdc_providers::coinbase_advanced::config::CoinbaseAdvancedConfig;
use mdc_providers::coinbase_advanced::connector::CoinbaseAdvancedConnectorBuilder;
use mdc_providers::gateio::config::GateioConfig;
use mdc_providers::gateio::connector::GateioConnectorBuilder;
use mdc_providers::huobi::config::HuobiConfig;
use mdc_providers::huobi::connector::HuobiConnectorBuilder;
use mdc_providers::kraken::checksum::{KrakenChecksumBook, RawLevelOwned};
use mdc_providers::kraken::config::KrakenConfig;
use mdc_providers::kraken::connector::KrakenConnectorBuilder;
use mdc_providers::lmax::config::LmaxConfig;
use mdc_providers::lmax::connector::LmaxConnectorBuilder;
use mdc_testkit::MockRest;
use mdc_testkit::sink::RecordingSink;
use mdc_testkit::ws::MockWsServer;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

/// Dropping the control handle closes the command channel. The connector's command fan-out and
/// per-connection forwarders must wind down on `Closed` without spinning, stopping the
/// connector, or blocking a later shutdown.
#[tokio::test(flavor = "current_thread")]
async fn test_dropping_control_handle_leaves_connector_serving_and_shutdown_clean() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let config =
        BinanceConfig::for_test(ws.ws_url().expect("ws url"), rest.url().expect("rest url"));
    let (connector, handle) = BinanceConnectorBuilder::new(config)
        .symbols(["BTCUSDT"])
        .streaming_sink(RecordingSink::new())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.clone();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_handshakes(1).await;
    drop(handle);

    // Let the fan-out and forwarders observe the closed channel. On a current-thread runtime a
    // spinning forwarder would starve this timer and the runner assertions below.
    // Pacing wait: bounded real-time yield so closed-channel handling has run; no readiness signal exists.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !runner.is_finished(),
        "connector must keep serving after the handle is dropped"
    );

    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), runner)
        .await
        .expect("shutdown must complete promptly")
        .expect("connector task must not panic");
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_token_cancellation_aborts_inflight_snapshot_request() {
    let mock_rest = MockRest::start().await;
    let mock_ws = MockWsServer::spawn().await.expect("spawn mock ws");

    // Configure mock REST with a 3-second delay
    Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .and(query_param("symbol", "BTCUSDT"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(3))
                .set_body_string(r#"{"lastUpdateId":100,"bids":[],"asks":[]}"#),
        )
        .mount(mock_rest.inner())
        .await;

    let mut config = BinanceConfig::default();
    config.ws_base_url = mock_ws.ws_url().expect("ws url");
    config.rest_base_url = mock_rest.url().expect("rest url");
    config.rest_timeout = Duration::from_millis(500);

    let sink = RecordingSink::new();
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(["BTCUSDT"])
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.clone();

    let start = Instant::now();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Cancel only once the delayed snapshot request is actually in flight.
    mock_rest.wait_for_requests(1).await;
    cancel.cancel();

    // The runner must abort promptly, far under the 3-second REST delay
    let joined = tokio::time::timeout(Duration::from_millis(500), runner)
        .await
        .expect("Connector did not abort promptly on cancellation");
    let outcome = joined.expect("connector task must not panic");
    assert!(
        outcome.is_ok(),
        "cancellation must end the run cleanly: {outcome:?}"
    );
    assert!(start.elapsed() < Duration::from_secs(2));
}

/// Repeated resync triggers while a snapshot request is in flight must coalesce into the one
/// HTTP request, driven through the real connector (commands, spawn and coalescing included).
#[tokio::test]
async fn test_duplicate_snapshot_triggers_coalesce_to_one_http_request() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    // The delayed response keeps the first request in flight while triggers pile up.
    Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .and(query_param("symbol", "BTCUSDT"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(300))
                .set_body_string(
                    r#"{"lastUpdateId":1000,"bids":[["100.0","1.0"]],"asks":[["101.0","1.0"]]}"#,
                ),
        )
        .expect(1)
        .mount(rest.inner())
        .await;

    let config =
        BinanceConfig::for_test(ws.ws_url().expect("ws url"), rest.url().expect("rest url"));
    let sink = RecordingSink::new();
    let (connector, handle) = BinanceConnectorBuilder::new(config)
        .symbols(["BTCUSDT"])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.clone();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_handshakes(1).await;
    rest.wait_for_requests(1).await;

    // Triggers while the first request is still in flight.
    for _ in 0..3 {
        handle
            .request_resync(Symbol::from("BTCUSDT"))
            .await
            .expect("resync command");
    }
    handle
        .request_resync_all()
        .await
        .expect("resync-all command");

    // A diff bridging the snapshot lets the in-flight response complete the resync.
    let _ = ws.broadcast_text(
        r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1600000000000,"s":"BTCUSDT","U":999,"u":1002,"b":[["100.0","2.0"]],"a":[]}}"#
            .to_string(),
    );
    sink.wait_for_alert_matching(|a| matches!(a, mdc_core::IntegrityAlert::ResyncCompleted { .. }))
        .await;

    assert_eq!(
        rest.inner()
            .received_requests()
            .await
            .map_or(0, |requests| requests.len()),
        1,
        "all triggers must coalesce onto the single in-flight request"
    );
    rest.inner().verify().await;

    cancel.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("shutdown must complete promptly")
        .expect("connector task must not panic");
    assert!(
        outcome.is_ok(),
        "cancellation must end the run cleanly: {outcome:?}"
    );
}

#[tokio::test]
async fn test_config_validation_rejects_zero_and_oversized_values() {
    // 1. Binance symbols_per_connection == 0
    let mut binance_cfg = BinanceConfig::default();
    binance_cfg.symbols_per_connection = 0;
    let res = BinanceConnectorBuilder::new(binance_cfg)
        .symbols(["BTCUSDT"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 2. Binance resync_buffer_capacity == 0
    let mut binance_cfg = BinanceConfig::default();
    binance_cfg.resync_buffer_capacity = 0;
    let res = BinanceConnectorBuilder::new(binance_cfg)
        .symbols(["BTCUSDT"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 3. Kraken symbols_per_connection == 0
    let mut kraken_cfg = KrakenConfig::default();
    kraken_cfg.symbols_per_connection = 0;
    let res = KrakenConnectorBuilder::new(kraken_cfg)
        .symbols(["BTC/USD"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 4. Kraken resubscribe_batch_size == 0
    let mut kraken_cfg = KrakenConfig::default();
    kraken_cfg.resubscribe_batch_size = 0;
    let res = KrakenConnectorBuilder::new(kraken_cfg)
        .symbols(["BTC/USD"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 5. Coinbase products_per_connection == 0
    let mut coinbase_cfg = CoinbaseConfig::default();
    coinbase_cfg.products_per_connection = 0;
    let res = CoinbaseConnectorBuilder::new(coinbase_cfg)
        .symbols(["BTC-USD"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 6. Gate.io symbols_per_connection == 0
    let mut gateio_cfg = GateioConfig::default();
    gateio_cfg.symbols_per_connection = 0;
    let res = GateioConnectorBuilder::new(gateio_cfg)
        .symbols(["BTC_USDT"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 7. Gate.io invalid orderbook_levels
    let mut gateio_cfg = GateioConfig::default();
    gateio_cfg.orderbook_levels = 15;
    let res = GateioConnectorBuilder::new(gateio_cfg)
        .symbols(["BTC_USDT"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 8. Bitstamp symbols_per_connection == 0
    let mut bitstamp_cfg = BitstampConfig::default();
    bitstamp_cfg.symbols_per_connection = 0;
    let res = BitstampConnectorBuilder::new(bitstamp_cfg)
        .symbols(["btcusd"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 9. Huobi symbols_per_connection == 0
    let mut huobi_cfg = HuobiConfig::default();
    huobi_cfg.symbols_per_connection = 0;
    let res = HuobiConnectorBuilder::new(huobi_cfg)
        .symbols(["btcusdt"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 10. Coinbase Advanced symbols_per_connection == 0
    let mut cb_adv_cfg = CoinbaseAdvancedConfig::default();
    cb_adv_cfg.symbols_per_connection = 0;
    let res = CoinbaseAdvancedConnectorBuilder::new(cb_adv_cfg)
        .symbols(["BTC-USD"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 11. LMAX symbols_per_connection == 0
    let mut lmax_cfg = LmaxConfig::default();
    lmax_cfg.symbols_per_connection = 0;
    let res = LmaxConnectorBuilder::new(lmax_cfg)
        .symbols(["EUR/USD"])
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));

    // 12. Empty symbols list validation on Binance
    let res = BinanceConnectorBuilder::new(BinanceConfig::default())
        .streaming_sink(RecordingSink::new())
        .build();
    assert!(matches!(res, Err(MdcError::Config(_))));
}

#[derive(Debug, Clone)]
struct CustomTestPolicy {
    decide_called: Arc<AtomicBool>,
}

impl ResyncPolicy for CustomTestPolicy {
    fn decide(&mut self, _symbol: &Symbol, _now: Instant) -> ResyncDecision {
        self.decide_called.store(true, Ordering::SeqCst);
        ResyncDecision::Proceed
    }

    fn on_started(&mut self, _symbol: &Symbol, _now: Instant) {}

    fn on_completed(&mut self, _symbol: &Symbol, _now: Instant) {}

    fn on_failed(&mut self, _symbol: &Symbol, _error: &ResyncFailure, _now: Instant) {}
}

#[derive(Debug)]
struct CustomTestChecksum {
    checksum_called: Arc<AtomicBool>,
}

impl ChecksumStrategy for CustomTestChecksum {
    fn compute(&self, _bids: &[RawLevelRef<'_>], _asks: &[RawLevelRef<'_>]) -> u32 {
        self.checksum_called.store(true, Ordering::SeqCst);
        12345678
    }

    fn depth(&self) -> usize {
        10
    }
}

#[tokio::test]
async fn test_policy_injection_changes_observable_behavior() {
    // 1. Resync policy injection on Binance
    let decide_called = Arc::new(AtomicBool::new(false));
    let policy_called = Arc::clone(&decide_called);

    let (connector, _handle) = BinanceConnectorBuilder::new(BinanceConfig::default())
        .symbols(["BTCUSDT"])
        .resync_policy(CustomTestPolicy {
            decide_called: policy_called,
        })
        .streaming_sink(RecordingSink::new())
        .build()
        .expect("build connector with custom policy");

    // The factory created the policy which wrapped our AtomicBool
    let mut created_policy = connector.resync_policy_factory()();
    let decision = created_policy.decide(&Symbol::from("BTCUSDT"), Instant::now());
    assert_eq!(decision, ResyncDecision::Proceed);
    assert!(decide_called.load(Ordering::SeqCst));

    // 2. Checksum strategy injection on Kraken
    let checksum_called = Arc::new(AtomicBool::new(false));
    let checksum_strategy: Arc<dyn ChecksumStrategy> = Arc::new(CustomTestChecksum {
        checksum_called: Arc::clone(&checksum_called),
    });

    let (connector, _handle) = KrakenConnectorBuilder::new(KrakenConfig::default())
        .symbols(["BTC/USD"])
        .checksum_strategy(Arc::clone(&checksum_strategy))
        .streaming_sink(RecordingSink::new())
        .build()
        .expect("build kraken connector with custom checksum");

    assert!(connector.config().checksum_strategy.is_some());
    let mut book = KrakenChecksumBook::with_strategy(10, checksum_strategy);
    let owned = RawLevelOwned {
        price: rust_decimal::Decimal::new(100, 0),
        volume: rust_decimal::Decimal::new(1, 0),
        price_raw: smallstr::SmallString::from("100"),
        volume_raw: smallstr::SmallString::from("1"),
    };
    book.apply_owned(&[], &[owned]);
    let cs = book.checksum();
    assert_eq!(cs, 12345678);
    assert!(checksum_called.load(Ordering::SeqCst));
}
