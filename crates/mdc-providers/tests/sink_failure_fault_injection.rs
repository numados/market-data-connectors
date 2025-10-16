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
    use mdc_testkit::{FaultInjectingSink, FaultInjectionPoint, MockRest, MockWsServer, WsAction};
    use tokio_util::sync::CancellationToken;

    const SNAPSHOT_JSON: &str =
        r#"{"lastUpdateId":100,"bids":[["25000.00","1.0"]],"asks":[["25001.00","2.0"]]}"#;
    const DEPTH_JSON: &str = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1600000000000,"s":"BTCUSDT","U":95,"u":105,"b":[["25000.00","1.5"]],"a":[["25001.00","2.5"]]}}"#;
    const TICKER_JSON: &str = r#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25000.00","B":"1.20","a":"25000.10","A":"2.40"}}"#;
    const TRADE_JSON: &str = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,"p":"25000.00","q":"0.10","b":88,"a":50,"T":1672515782136,"m":true,"M":true}}"#;

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
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );

        assert!(
            sink.is_tripped(),
            "configured fault point should be tripped"
        );
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
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
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );

        assert!(
            sink.is_tripped(),
            "configured fault point should be tripped"
        );
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
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
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );

        assert!(
            sink.is_tripped(),
            "configured fault point should be tripped"
        );
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
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

        // Wait for WebSocket handshake
        ws.wait_for_handshakes(1).await;

        // Broadcast book ticker that will trigger fault
        let _ = ws.broadcast_text(TICKER_JSON);

        let res = tokio::time::timeout(Duration::from_millis(1000), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );

        assert!(
            sink.is_tripped(),
            "configured fault point should be tripped"
        );
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
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

        // Wait for WebSocket handshake
        ws.wait_for_handshakes(1).await;

        // Broadcast trade that will trigger fault
        let _ = ws.broadcast_text(TRADE_JSON);

        let res = tokio::time::timeout(Duration::from_millis(1000), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );

        assert!(
            sink.is_tripped(),
            "configured fault point should be tripped"
        );
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
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
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );

        assert!(
            sink.is_tripped(),
            "configured fault point should be tripped"
        );
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );

        cancel.cancel();
    }
}

#[cfg(feature = "coinbase")]
mod coinbase_faults {
    use std::time::Duration;

    use mdc_core::{IntegrityAlert, Symbol};
    use mdc_providers::coinbase::{CoinbaseConfig, CoinbaseConnectorBuilder};
    use mdc_testkit::{
        FaultInjectingSink, FaultInjectionPoint, MockWsServer, WsAction, load_fixture,
    };
    use tokio_util::sync::CancellationToken;

    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/coinbase/order_book_snapshot.json";

    #[tokio::test]
    async fn test_sink_fault_on_coinbase_snapshot() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let snapshot = load_fixture(SNAPSHOT_FIXTURE).expect("load fixture");

        ws.push_script(vec![
            WsAction::WaitClientMessage,
            WsAction::SendText(snapshot),
        ]);

        let mut config = CoinbaseConfig::default();
        config.ws_base_url = ws.ws_url().expect("ws url").to_string();

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let (connector, _handle) = CoinbaseConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTC-USD")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );
        assert!(sink.is_tripped(), "fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );
        cancel.cancel();
    }
}

#[cfg(feature = "kraken")]
mod kraken_faults {
    use std::time::Duration;

    use mdc_core::{IntegrityAlert, Symbol};
    use mdc_providers::kraken::{KrakenConfig, KrakenConnectorBuilder};
    use mdc_testkit::{
        FaultInjectingSink, FaultInjectionPoint, MockWsServer, WsAction, load_fixture,
    };
    use tokio_util::sync::CancellationToken;

    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/kraken/order_book_snapshot.json";

    #[tokio::test]
    async fn test_sink_fault_on_kraken_snapshot() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let snapshot = load_fixture(SNAPSHOT_FIXTURE).expect("load fixture");
        ws.push_script(vec![
            WsAction::WaitClientMessage,
            WsAction::SendText(snapshot),
        ]);

        let mut config = KrakenConfig::default();
        config.ws_base_url = ws.ws_url().expect("ws url");
        config.initial_backoff = Duration::from_millis(20);

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let (connector, _handle) = KrakenConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTC/USD")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );
        assert!(sink.is_tripped(), "fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );
        cancel.cancel();
    }
}

#[cfg(feature = "bitfinex")]
mod bitfinex_faults {
    use std::time::Duration;

    use mdc_core::model::IntegrityAlert;
    use mdc_providers::bitfinex::connector::BitfinexConnector;
    use mdc_testkit::{
        FaultInjectingSink, FaultInjectionPoint, MockWsServer, WsAction, load_fixture,
    };
    use tokio_util::sync::CancellationToken;

    const CONF_OK_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/bitfinex/conf_ok.json";
    const SUBSCRIBED_BOOK_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/bitfinex/subscribed_book.json";
    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/bitfinex/order_book_snapshot.json";

    #[tokio::test]
    async fn test_sink_fault_on_bitfinex_snapshot() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let conf_ok = load_fixture(CONF_OK_FIXTURE).expect("fixture");
        let sub_book = load_fixture(SUBSCRIBED_BOOK_FIXTURE).expect("fixture");
        let snap = load_fixture(SNAPSHOT_FIXTURE).expect("fixture");

        ws.push_script(vec![
            WsAction::WaitClientMessage, // conf
            WsAction::SendText(conf_ok),
            WsAction::WaitClientMessage, // subscribe
            WsAction::SendText(sub_book),
            WsAction::SendText(snap),
        ]);

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let connector = BitfinexConnector::builder()
            .ws_url(ws.ws_url().expect("ws url").to_string())
            .symbol("tBTCUSD")
            .enable_depth(true)
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let (_handle, runner) = connector.run(sink.clone(), cancel_child);
        let run_task = tokio::spawn(runner);

        let res = tokio::time::timeout(Duration::from_millis(1500), run_task).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );
        assert!(sink.is_tripped(), "fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );
        cancel.cancel();
    }
}

#[cfg(feature = "bitstamp")]
mod bitstamp_faults {
    use std::time::Duration;

    use mdc_core::{IntegrityAlert, Symbol};
    use mdc_providers::bitstamp::{BitstampConfig, BitstampConnectorBuilder};
    use mdc_testkit::{
        FaultInjectingSink, FaultInjectionPoint, MockRest, MockWsServer, load_fixture,
    };
    use tokio_util::sync::CancellationToken;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, ResponseTemplate};

    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/bitstamp/order_book_snapshot.json";

    #[tokio::test]
    async fn test_sink_fault_on_bitstamp_snapshot() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let rest = MockRest::start().await;
        let snap = load_fixture(SNAPSHOT_FIXTURE).expect("fixture");

        Mock::given(method("GET"))
            .and(path("/order_book/btcusd/"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(snap, "application/json"))
            .mount(rest.inner())
            .await;

        let mut config = BitstampConfig::default();
        config.ws_url = ws.ws_url().expect("ws url");
        config.rest_url = rest.url().expect("valid rest url");

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let (connector, _handle) = BitstampConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("btcusd")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );
        assert!(sink.is_tripped(), "fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );
        cancel.cancel();
    }
}

#[cfg(feature = "gateio")]
mod gateio_faults {
    use std::time::Duration;

    use mdc_core::{IntegrityAlert, Symbol};
    use mdc_providers::gateio::{GateioConfig, GateioConnectorBuilder};
    use mdc_testkit::{
        FaultInjectingSink, FaultInjectionPoint, MockWsServer, WsAction, load_fixture,
    };
    use tokio_util::sync::CancellationToken;

    const SUB_ACK_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/gateio/subscribe_ack.json";
    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/gateio/order_book_snapshot.json";

    #[tokio::test]
    async fn test_sink_fault_on_gateio_snapshot() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let sub_ack = load_fixture(SUB_ACK_FIXTURE).expect("fixture");
        let snap = load_fixture(SNAPSHOT_FIXTURE).expect("fixture");
        ws.push_script(vec![
            WsAction::WaitClientMessage,
            WsAction::SendText(sub_ack),
            WsAction::SendText(snap),
        ]);

        let mut config = GateioConfig::default();
        config.ws_url = ws.ws_url().expect("ws url");

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let (connector, _handle) = GateioConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("BTC_USDT")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );
        assert!(sink.is_tripped(), "fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );
        cancel.cancel();
    }
}

#[cfg(feature = "huobi")]
mod huobi_faults {
    use std::io::Write;
    use std::time::Duration;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use mdc_core::{IntegrityAlert, Symbol};
    use mdc_providers::huobi::{HuobiConfig, HuobiConnectorBuilder};
    use mdc_testkit::{FaultInjectingSink, FaultInjectionPoint, MockWsServer, load_fixture};
    use tokio_util::sync::CancellationToken;

    const SUB_ACK_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/huobi/sub_ack.json";
    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/huobi/order_book_snapshot.json";

    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).expect("gzip write");
        encoder.finish().expect("gzip finish")
    }

    #[tokio::test]
    async fn test_sink_fault_on_huobi_snapshot() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let mut config = HuobiConfig::default();
        config.ws_url = ws.ws_url().expect("ws url");

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let (connector, _handle) = HuobiConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("btcusdt")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        let req_msg = ws
            .wait_for_client_message_matching(|m| m.contains("\"req\""))
            .await;
        let v: serde_json::Value = serde_json::from_str(&req_msg).expect("valid json req");
        let req_id = v["id"].as_str().expect("req id").to_string();

        let ack_json = load_fixture(SUB_ACK_FIXTURE).expect("fixture");
        let _ = ws.broadcast_binary(gzip_compress(ack_json.as_bytes()));

        let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
        let mut snap_val: serde_json::Value =
            serde_json::from_str(&snap_raw).expect("parse snapshot");
        snap_val["id"] = serde_json::Value::String(req_id);
        snap_val["data"]["seqNum"] = serde_json::json!(100);
        let _ = ws.broadcast_binary(gzip_compress(snap_val.to_string().as_bytes()));

        let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );
        assert!(sink.is_tripped(), "fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );
        cancel.cancel();
    }
}

#[cfg(feature = "lmax")]
mod lmax_faults {
    use std::time::Duration;

    use mdc_core::{IntegrityAlert, Symbol};
    use mdc_providers::lmax::{ControlPacing, LmaxConfig, LmaxConnectorBuilder};
    use mdc_testkit::{
        FaultInjectingSink, FaultInjectionPoint, MockWsServer, WsAction, load_fixture,
    };
    use tokio_util::sync::CancellationToken;

    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/lmax/order_book_snapshot.json";

    #[tokio::test]
    async fn test_sink_fault_on_lmax_snapshot() {
        let ws = MockWsServer::spawn().await.expect("spawn mock ws");
        let snap = load_fixture(SNAPSHOT_FIXTURE).expect("fixture");
        ws.push_script(vec![WsAction::WaitClientMessage, WsAction::SendText(snap)]);

        let mut config = LmaxConfig::default();
        config.ws_url = ws.ws_url().expect("ws url");
        config.control_pacing = ControlPacing {
            initial_delay: Duration::from_millis(10),
            min_command_interval: Duration::from_millis(10),
            too_many_requests_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
        };

        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let (connector, _handle) = LmaxConnectorBuilder::new(config)
            .symbols(vec![Symbol::from("btc-usd")])
            .streaming_sink(sink.clone())
            .build()
            .expect("build connector");

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let runner = tokio::spawn(async move { connector.run(cancel_child).await });

        let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );
        assert!(sink.is_tripped(), "fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );
        cancel.cancel();
    }
}

#[cfg(feature = "vwd")]
mod vwd_faults {
    use std::time::Duration;

    use mdc_core::IntegrityAlert;
    use mdc_providers::vwd::config::{VwdConfig, VwdCredentials, VwdSymbol};
    use mdc_providers::vwd::connector::VwdConnector;
    use mdc_testkit::{FaultInjectingSink, FaultInjectionPoint, MockRest, load_fixture};
    use tokio_util::sync::CancellationToken;
    use wiremock::matchers::{header_exists, method, path};
    use wiremock::{Mock, ResponseTemplate};

    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json";

    #[tokio::test]
    async fn test_sink_fault_on_vwd_snapshot() {
        let mock = MockRest::start().await;
        let body = load_fixture(SNAPSHOT_FIXTURE).expect("load fixture");

        Mock::given(method("GET"))
            .and(path(
                "/test-account/JSON/QUOTE/SNAPQUOTE/EURUSD.XRATE.SPOT~X",
            ))
            .and(header_exists("authorization"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.into_bytes(), "application/json"),
            )
            .mount(mock.inner())
            .await;

        let config = VwdConfig::builder()
            .base_url(mock.url().expect("valid mock url"))
            .account_path("test-account")
            .credentials(VwdCredentials::new("user", "pass"))
            .add_symbol(VwdSymbol::direct("EURUSD"))
            .poll_interval(Duration::from_millis(30))
            .build()
            .expect("build config");

        let connector = VwdConnector::builder(config)
            .build()
            .expect("build connector");
        let sink = FaultInjectingSink::new(FaultInjectionPoint::BeginBook);
        let cancel = CancellationToken::new();

        let (_handle, runner) = connector.run(sink.clone(), cancel.child_token());
        let res = tokio::time::timeout(Duration::from_millis(1500), runner).await;
        assert!(
            res.is_ok(),
            "connector task should terminate after sink error"
        );
        assert!(sink.is_tripped(), "fault point should be tripped");
        let alerts = sink.alerts();
        assert!(
            alerts
                .iter()
                .any(|a| matches!(a, IntegrityAlert::SinkFailure)),
            "expected SinkFailure alert, got: {alerts:?}"
        );
        cancel.cancel();
    }
}
