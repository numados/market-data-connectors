#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Downstream sink fail-closed fault injection integration tests.
//!
//! Verifies that downstream sink errors in `begin_book`, `level`, `end_book`, `trade`,
//! `book_ticker`, and `reference_rate` immediately halt the connector task, emit `IntegrityAlert::SinkFailure`,
//! and prevent further emissions.

#[cfg(feature = "binance")]
mod binance_faults {
    use std::time::Duration;

    use mdc_core::{IntegrityAlert, Symbol};
    use mdc_providers::binance::{BinanceConfig, BinanceConnectorBuilder};
    use mdc_testkit::{
        FaultInjectingSink, FaultInjectionPoint, MockRest, MockWsServer, WsAction,
    };
    use tokio_util::sync::CancellationToken;

    const SNAPSHOT_JSON: &str =
        r#"{"lastUpdateId":100,"bids":[["25000.00","1.0"]],"asks":[["25001.00","2.0"]]}"#;
    const DEPTH_JSON: &str =
        r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1600000000000,"s":"BTCUSDT","U":95,"u":105,"b":[["25000.00","1.5"]],"a":[["25001.00","2.5"]]}}"#;
    const TICKER_JSON: &str =
        r#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25000.00","B":"1.20","a":"25000.10","A":"2.40"}}"#;
    const TRADE_JSON: &str =
        r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,"p":"25000.00","q":"0.10","b":88,"a":50,"T":1672515782136,"m":true,"M":true}}"#;

    #[tokio::test]
    async fn test_sink_fault_on_begin_book() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let rest = MockRest::start().await;
        rest.mock_binance_depth_ok("BTCUSDT", SNAPSHOT_JSON).await;

        ws.push_script(vec![WsAction::SendText(DEPTH_JSON.to_string())]);

        let mut config = BinanceConfig::default();
        config.ws_base_url = ws.ws_url().expect("ws url");
        config.rest_base_url = rest.url().expect("rest url");
        config.initial_backoff = Duration::from_millis(20);

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let (connector, _handle) = BinanceConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTCUSDT")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        // Await runner termination on sink fault (should terminate without hang)
        let res = tokio::time::timeout(Duration::from_millis(1000), runner).await;
        assert!(res.is_ok(), "connector task should terminate after sink error");

        assert!(sink.is_tripped(), "configured fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts.iter().any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_sink_fault_on_level() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let rest = MockRest::start().await;
        rest.mock_binance_depth_ok("BTCUSDT", SNAPSHOT_JSON).await;

        ws.push_script(vec![WsAction::SendText(DEPTH_JSON.to_string())]);

        let mut config = BinanceConfig::default();
        config.ws_base_url = ws.ws_url().expect("ws url");
        config.rest_base_url = rest.url().expect("rest url");
        config.initial_backoff = Duration::from_millis(20);

        let sink = FaultInjectingSink::new(FaultInjectionPoint::Level(1));
        let (connector, _handle) = BinanceConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTCUSDT")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        let res = tokio::time::timeout(Duration::from_millis(1000), runner).await;
        assert!(res.is_ok(), "connector task should terminate after sink error");

        assert!(sink.is_tripped(), "configured fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts.iter().any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_sink_fault_on_end_book() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let rest = MockRest::start().await;
        rest.mock_binance_depth_ok("BTCUSDT", SNAPSHOT_JSON).await;

        ws.push_script(vec![WsAction::SendText(DEPTH_JSON.to_string())]);

        let mut config = BinanceConfig::default();
        config.ws_base_url = ws.ws_url().expect("ws url");
        config.rest_base_url = rest.url().expect("rest url");
        config.initial_backoff = Duration::from_millis(20);

        let sink = FaultInjectingSink::new(FaultInjectionPoint::EndBook);
        let (connector, _handle) = BinanceConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTCUSDT")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        let res = tokio::time::timeout(Duration::from_millis(1000), runner).await;
        assert!(res.is_ok(), "connector task should terminate after sink error");

        assert!(sink.is_tripped(), "configured fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts.iter().any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_sink_fault_on_book_ticker() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let rest = MockRest::start().await;
        rest.mock_binance_depth_ok("BTCUSDT", SNAPSHOT_JSON).await;

        let mut config = BinanceConfig::default();
        config.ws_base_url = ws.ws_url().expect("ws url");
        config.rest_base_url = rest.url().expect("rest url");
        config.initial_backoff = Duration::from_millis(20);

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BookTicker);
        let (connector, _handle) = BinanceConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTCUSDT")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        // Let WebSocket connect
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Broadcast book ticker that will trigger fault
        ws.broadcast_text(TICKER_JSON);

        let res = tokio::time::timeout(Duration::from_millis(1000), runner).await;
        assert!(res.is_ok(), "connector task should terminate after sink error");

        assert!(sink.is_tripped(), "configured fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts.iter().any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_sink_fault_on_trade() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let rest = MockRest::start().await;
        rest.mock_binance_depth_ok("BTCUSDT", SNAPSHOT_JSON).await;

        let mut config = BinanceConfig::default();
        config.ws_base_url = ws.ws_url().expect("ws url");
        config.rest_base_url = rest.url().expect("rest url");
        config.initial_backoff = Duration::from_millis(20);
        config.streams.trade = true;

        let sink = FaultInjectingSink::new(FaultInjectionPoint::Trade);
        let (connector, _handle) = BinanceConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTCUSDT")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        // Let WebSocket connect
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Broadcast trade that will trigger fault
        ws.broadcast_text(TRADE_JSON);

        let res = tokio::time::timeout(Duration::from_millis(1000), runner).await;
        assert!(res.is_ok(), "connector task should terminate after sink error");

        assert!(sink.is_tripped(), "configured fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts.iter().any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );

        cancel.cancel();
    }
}

#[cfg(feature = "frankfurter")]
mod frankfurter_faults {
    use std::time::Duration;

    use mdc_core::IntegrityAlert;
    use mdc_providers::frankfurter::{FrankfurterConfig, FrankfurterConnector};
    use mdc_testkit::{FaultInjectingSink, FaultInjectionPoint, MockRest, load_fixture};
    use tokio_util::sync::CancellationToken;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, ResponseTemplate};

    const RATES_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json";

    #[tokio::test]
    async fn test_sink_fault_on_reference_rate() {
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
        let sink = FaultInjectingSink::new(FaultInjectionPoint::ReferenceRate);
        let cancel = CancellationToken::new();

        let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());

        let res = tokio::time::timeout(Duration::from_millis(1000), runner).await;
        assert!(res.is_ok(), "connector task should terminate after sink error");

        assert!(sink.is_tripped(), "configured fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts.iter().any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );

        cancel.cancel();
    }
}
