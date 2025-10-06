#![cfg(feature = "bitstamp")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Bitstamp connector.

use std::str::FromStr;
use std::time::Duration;

use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, Side, Symbol};
use mdc_providers::bitstamp::{BitstampConfig, BitstampConnectorBuilder, BitstampStreams};
use mdc_testkit::{MockRest, MockWsServer, RecordedEvent, RecordingSink, load_fixture};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

const SNAPSHOT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/bitstamp/order_book_snapshot.json";
const UPDATE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/bitstamp/depth_update.json";
const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/bitstamp/trade.json";
const RECONNECT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/bitstamp/request_reconnect.json";

#[tokio::test]
async fn test_ws_subscribe_diff_order_book_channel() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusd"), Symbol::from("ethusd")];

    let sink = RecordingSink::new();
    let (connector, _handle) = BitstampConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    let received = ws.received_messages();
    assert!(
        received.len() >= 2,
        "Expected at least 2 subscribe frames, got {}",
        received.len()
    );

    let mut channels: Vec<String> = Vec::new();
    for msg in &received {
        let v: serde_json::Value = serde_json::from_str(msg).expect("valid json subscribe");
        assert_eq!(v["event"], "bts:subscribe");
        if let Some(ch) = v["data"]["channel"].as_str() {
            channels.push(ch.to_string());
        }
    }

    assert!(channels.contains(&"diff_order_book_btcusd".to_string()));
    assert!(channels.contains(&"diff_order_book_ethusd".to_string()));

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_subscribe_with_trades_stream() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.streams = BitstampStreams {
        depth: true,
        trades: true,
    };

    let symbols = vec![Symbol::from("btcusd")];

    let sink = RecordingSink::new();
    let (connector, _handle) = BitstampConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    let received = ws.received_messages();
    assert!(received.len() >= 2);

    let mut channels: Vec<String> = Vec::new();
    for msg in &received {
        let v: serde_json::Value = serde_json::from_str(msg).expect("valid json");
        assert_eq!(v["event"], "bts:subscribe");
        if let Some(ch) = v["data"]["channel"].as_str() {
            channels.push(ch.to_string());
        }
    }

    assert!(channels.contains(&"diff_order_book_btcusd".to_string()));
    assert!(channels.contains(&"live_trades_btcusd".to_string()));

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_snapshot_and_time_gated_deltas() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    // Snapshot microtimestamp in fixture is 1700000000100000
    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(snap_json, "application/json"))
        .mount(rest.inner())
        .await;

    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.rest_url = rest.url().expect("valid rest url");

    let sink = RecordingSink::new();
    let (connector, _handle) = BitstampConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for snapshot reconciliation to complete
    tokio::time::sleep(Duration::from_millis(150)).await;

    let events = sink.events();
    // Verify ResyncStarted, BeginBook(Snapshot), Level, EndBook, ResyncCompleted
    let has_snapshot = events.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        )
    });
    assert!(has_snapshot, "Authoritative snapshot must be processed");

    let has_completed = events.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncCompleted { .. },
                ..
            }
        )
    });
    assert!(has_completed, "ResyncCompleted alert must be emitted");

    sink.clear();

    // 1. Send an older/stale diff (microtimestamp <= snapshot microtimestamp 1700000000100000)
    let stale_diff = serde_json::json!({
        "event": "data",
        "channel": "diff_order_book_btcusd",
        "data": {
            "timestamp": "1700000000",
            "microtimestamp": "1700000000100000", // Exactly equal to snapshot -> must drop
            "bids": [["50000.00", "1.00000000"]],
            "asks": []
        }
    })
    .to_string();

    ws.broadcast_text(stale_diff);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        sink.events().is_empty(),
        "Diff with microtimestamp <= snapshot time must be dropped"
    );

    // 2. Send a newer diff (microtimestamp 1700000000123456 > snapshot 1700000000100000)
    ws.broadcast_text(upd_json);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_events = sink.events();
    let has_delta = new_events.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Delta,
                ..
            }
        )
    });
    assert!(
        has_delta,
        "Newer diff (> snapshot) must be emitted as Delta"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_request_reconnect_triggers_reconnect() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let reconnect_json = load_fixture(RECONNECT_FIXTURE).expect("load reconnect fixture");

    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(snap_json, "application/json"))
        .mount(rest.inner())
        .await;

    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.rest_url = rest.url().expect("valid rest url");

    let sink = RecordingSink::new();
    let (connector, _handle) = BitstampConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Send bts:request_reconnect
    ws.broadcast_text(reconnect_json);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let events = sink.events();
    let has_conn_lost = events.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ConnectionLost {
                    reason: DisconnectKind::VenueRequested,
                },
                ..
            }
        )
    });
    assert!(
        has_conn_lost,
        "bts:request_reconnect must trigger ConnectionLost(VenueRequested)"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_trade_normalization() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let trade_json = load_fixture(TRADE_FIXTURE).expect("load trade fixture");

    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(snap_json, "application/json"))
        .mount(rest.inner())
        .await;

    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.rest_url = rest.url().expect("valid rest url");
    config.streams = BitstampStreams {
        depth: true,
        trades: true,
    };

    let sink = RecordingSink::new();
    let (connector, _handle) = BitstampConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(150)).await;

    sink.clear();

    // Send trade frame
    ws.broadcast_text(trade_json);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    let trade_event = events.iter().find_map(|e| match e {
        RecordedEvent::Trade { trade, .. } => Some(trade),
        _ => None,
    });

    assert!(trade_event.is_some(), "Trade event must be emitted");
    let trade = trade_event.unwrap();
    assert_eq!(trade.aggressor, Side::Bid); // type: 0 -> Bid
    assert_eq!(trade.price, Decimal::from_str("50000.00").unwrap());
    assert_eq!(trade.volume, Decimal::from_str("0.01000000").unwrap());
    assert!(
        trade.identity.is_some(),
        "NativeTradeIdentity must be populated"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_connector_handle_control_pause_resume() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");

    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(snap_json, "application/json"))
        .mount(rest.inner())
        .await;

    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.rest_url = rest.url().expect("valid rest url");

    let sink = RecordingSink::new();
    let (connector, handle) = BitstampConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Send Pause command
    handle.pause().await.expect("send pause");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    let has_paused = events.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ConnectionLost {
                    reason: DisconnectKind::Paused,
                },
                ..
            }
        )
    });
    assert!(has_paused, "Pause must emit ConnectionLost(Paused)");

    // Send Resume command
    handle.resume().await.expect("send resume");
    tokio::time::sleep(Duration::from_millis(50)).await;

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_connector_handle_request_resync() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");

    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(snap_json, "application/json"))
        .mount(rest.inner())
        .await;

    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.rest_url = rest.url().expect("valid rest url");

    let sink = RecordingSink::new();
    let (connector, handle) = BitstampConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(150)).await;

    sink.clear();

    // Trigger explicit resync
    handle
        .request_resync(Symbol::from("btcusd"))
        .await
        .expect("request resync");

    tokio::time::sleep(Duration::from_millis(150)).await;

    let events = sink.events();
    let has_resync_started = events.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncStarted,
                ..
            }
        )
    });
    assert!(
        has_resync_started,
        "request_resync must emit ResyncStarted alert"
    );

    let has_snapshot = events.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        )
    });
    assert!(has_snapshot, "Fresh snapshot must be processed");

    cancel.cancel();
    let _ = runner.await;
}
