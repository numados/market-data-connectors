#![cfg(feature = "coinbase")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Coinbase Exchange WebSocket connector.

use std::time::Duration;

use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, Side, Symbol};
use mdc_providers::coinbase::{CoinbaseConfig, CoinbaseConnectorBuilder};
use mdc_testkit::{MockWsServer, RecordedEvent, RecordingSink, load_fixture};
use tokio_util::sync::CancellationToken;

const SNAPSHOT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/coinbase/order_book_snapshot.json";
const UPDATE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/coinbase/depth_update.json";
const HEARTBEAT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/coinbase/heartbeat_or_ping.json";
const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/coinbase/trade.json";

#[tokio::test]
async fn test_ws_subscribe_message_and_matches_flag() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url").to_string();

    let symbols = vec![Symbol::from("BTC-USD"), Symbol::from("ETH-USD")];

    let sink = RecordingSink::new();
    let (connector, _handle) = CoinbaseConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    let received = ws.wait_for_client_messages(1).await;
    assert!(!received.is_empty(), "Expected subscribe frame");

    let v: serde_json::Value = serde_json::from_str(&received[0]).expect("valid json subscribe");
    assert_eq!(v["type"], "subscribe");
    assert_eq!(v["product_ids"], serde_json::json!(["BTC-USD", "ETH-USD"]));
    assert_eq!(
        v["channels"],
        serde_json::json!(["level2_batch", "heartbeat"])
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_subscribe_message_with_matches_enabled() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url").to_string();
    config.streams.matches = true;

    let sink = RecordingSink::new();
    let (connector, _handle) = CoinbaseConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC-USD")])
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    let received = ws.wait_for_client_messages(1).await;
    assert!(!received.is_empty());

    let v: serde_json::Value = serde_json::from_str(&received[0]).expect("valid json subscribe");
    assert_eq!(
        v["channels"],
        serde_json::json!(["level2_batch", "heartbeat", "matches"])
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_snapshot_and_delta_ordered_delivery() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    let mut config = CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url").to_string();

    let sink = RecordingSink::new();
    let (connector, _handle) = CoinbaseConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC-USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for connect / subscription
    ws.wait_for_client_messages(1).await;
    sink.wait_for_alerts(1).await;

    // Send snapshot
    let _ = ws.broadcast_text(&snap_json);
    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    // Send update
    let _ = ws.broadcast_text(&upd_json);
    sink.wait_for_event_matching(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Delta,
                ..
            }
        )
    })
    .await;

    let events = sink.events();
    let mut has_resync_started = false;
    let mut has_snapshot = false;
    let mut has_resync_completed = false;
    let mut has_delta = false;

    for ev in events {
        match ev {
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncStarted,
                ..
            } => {
                has_resync_started = true;
            }
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncCompleted { .. },
                ..
            } => {
                has_resync_completed = true;
            }
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            } => {
                has_snapshot = true;
            }
            RecordedEvent::BeginBook {
                kind: BookKind::Delta,
                ..
            } => {
                has_delta = true;
            }
            _ => {}
        }
    }

    assert!(has_resync_started, "Must emit ResyncStarted on connect");
    assert!(has_snapshot, "Must emit L2Snapshot from valid snapshot");
    assert!(
        has_resync_completed,
        "Must emit ResyncCompleted after valid snapshot"
    );
    assert!(has_delta, "Must emit L2Delta from update in InSync");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_heartbeat_silence_timeout_resubscribes_single_product() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");

    let mut config = CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url").to_string();
    config.heartbeat_timeout = Duration::from_millis(250); // fast timeout for test

    let sink = RecordingSink::new();
    let (connector, _handle) = CoinbaseConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC-USD"), Symbol::from("ETH-USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;
    sink.wait_for_alerts(2).await;

    // Send snapshot for BTC-USD
    let _ = ws.broadcast_text(&snap_json);
    // Send snapshot for ETH-USD
    let eth_snap = snap_json.replace("BTC-USD", "ETH-USD");
    let _ = ws.broadcast_text(&eth_snap);
    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    // Send initial heartbeats for both symbols
    let hb_init_btc = serde_json::json!({
        "type": "heartbeat",
        "sequence": 99,
        "last_trade_id": 1,
        "product_id": "BTC-USD",
        "time": "2014-11-07T08:19:28.464459Z"
    });
    let hb_init_eth = serde_json::json!({
        "type": "heartbeat",
        "sequence": 49,
        "last_trade_id": 1,
        "product_id": "ETH-USD",
        "time": "2014-11-07T08:19:28.464459Z"
    });
    let _ = ws.broadcast_text(hb_init_btc.to_string());
    let _ = ws.broadcast_text(hb_init_eth.to_string());

    // Now continuously send heartbeats ONLY for BTC-USD, leaving ETH-USD silent
    for seq in 100..105 {
        let hb = serde_json::json!({
            "type": "heartbeat",
            "sequence": seq,
            "last_trade_id": 1,
            "product_id": "BTC-USD",
            "time": "2014-11-07T08:19:28.464459Z"
        });
        let _ = ws.broadcast_text(hb.to_string());
        tokio::time::sleep(Duration::from_millis(60)).await;
    }

    // Wait until ETH-USD triggers resubscribe after silence timeout
    ws.wait_for_client_message_matching(|m| m.contains("unsubscribe") && m.contains("ETH-USD"))
        .await;

    let received = ws.received_messages();
    // Verify an unsubscribe and subscribe was sent for ETH-USD
    let mut eth_unsub = false;
    let mut eth_sub = false;
    let mut btc_resub = false;

    for msg in &received {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
            if v["type"] == "unsubscribe" && v["product_ids"] == serde_json::json!(["ETH-USD"]) {
                eth_unsub = true;
            }
            if v["type"] == "subscribe" && v["product_ids"] == serde_json::json!(["ETH-USD"]) {
                eth_sub = true;
            }
            if (v["type"] == "unsubscribe" || v["type"] == "subscribe")
                && v["product_ids"] == serde_json::json!(["BTC-USD"])
            {
                btc_resub = true;
            }
        }
    }

    assert!(eth_unsub, "Expected unsubscribe for timed out ETH-USD");
    assert!(eth_sub, "Expected subscribe for timed out ETH-USD");
    assert!(!btc_resub, "Healthy BTC-USD must not be resubscribed");

    // Verify HeartbeatTimeout alert was emitted for ETH-USD
    let events = sink.events();
    let has_eth_timeout = events.iter().any(|ev| match ev {
        RecordedEvent::Alert {
            symbol,
            alert: IntegrityAlert::HeartbeatTimeout { .. },
            ..
        } => symbol.as_str() == "ETH-USD",
        _ => false,
    });
    assert!(
        has_eth_timeout,
        "Expected HeartbeatTimeout alert for ETH-USD"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_heartbeat_sequence_jump_resubscribes() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");

    let mut config = CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url").to_string();
    config.sequence_gap_threshold = Some(3000);

    let sink = RecordingSink::new();
    let (connector, _handle) = CoinbaseConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC-USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;
    sink.wait_for_alerts(1).await;

    // Send snapshot
    let _ = ws.broadcast_text(&snap_json);
    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    // Send normal heartbeat from fixture (seq 90)
    let hb_json = load_fixture(HEARTBEAT_FIXTURE).expect("load heartbeat fixture");
    let _ = ws.broadcast_text(&hb_json);

    // Send heartbeat with sequence jump 4000 (jump = 3900 > 3000 threshold)
    let hb2 = serde_json::json!({
        "type": "heartbeat",
        "sequence": 4000,
        "last_trade_id": 10,
        "product_id": "BTC-USD",
        "time": "2014-11-07T08:19:29.464459Z"
    });
    let _ = ws.broadcast_text(hb2.to_string());
    ws.wait_for_client_message_matching(|m| m.contains("unsubscribe") && m.contains("BTC-USD"))
        .await;

    let received = ws.received_messages();
    let mut has_unsub = false;
    let mut has_sub = false;

    for msg in &received {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
            if v["type"] == "unsubscribe" && v["product_ids"] == serde_json::json!(["BTC-USD"]) {
                has_unsub = true;
            }
            if v["type"] == "subscribe" && v["product_ids"] == serde_json::json!(["BTC-USD"]) {
                has_sub = true;
            }
        }
    }

    assert!(has_unsub, "Sequence jump must trigger unsubscribe");
    assert!(has_sub, "Sequence jump must trigger subscribe");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_drop_updates_while_awaiting_snapshot() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    let mut config = CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url").to_string();

    let sink = RecordingSink::new();
    let (connector, _handle) = CoinbaseConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC-USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;
    sink.wait_for_alerts(1).await;

    // Send update BEFORE snapshot
    let _ = ws.broadcast_text(&upd_json);
    tokio::task::yield_now().await;

    let events = sink.events();
    let has_delta = events.iter().any(|ev| {
        matches!(
            ev,
            RecordedEvent::BeginBook {
                kind: BookKind::Delta,
                ..
            }
        )
    });

    assert!(!has_delta, "Updates while AwaitingSnapshot must be dropped");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_matches_trade_stream() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let trade_json = load_fixture(TRADE_FIXTURE).expect("load trade fixture");

    let mut config = CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url").to_string();
    config.streams.matches = true;

    let sink = RecordingSink::new();
    let (connector, _handle) = CoinbaseConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC-USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;
    sink.wait_for_alerts(1).await;

    let _ = ws.broadcast_text(&trade_json);
    let trades = sink.wait_for_trades(1).await;

    assert_eq!(trades.len(), 1, "Must emit trade event");
    let t = &trades[0];
    // Maker side in fixture is "sell" -> Taker is buyer -> aggressor is Bid
    assert_eq!(t.aggressor, Side::Bid);
    assert_eq!(t.price.to_string(), "400.23");
    assert_eq!(t.volume.to_string(), "5.23512");
    let id = t.identity.as_ref().expect("trade identity");
    assert_eq!(id.namespace.as_str(), "coinbase:exchange");
    assert_eq!(id.value.as_str(), "10");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_control_pause_resume_cancel() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url").to_string();

    let sink = RecordingSink::new();
    let (connector, handle) = CoinbaseConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC-USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;
    sink.wait_for_alerts(1).await;

    // Pause connector
    handle.pause().await.expect("pause command");
    let paused_alert = sink
        .wait_for_alert_matching(|a| {
            matches!(
                a,
                IntegrityAlert::ConnectionLost {
                    reason: DisconnectKind::Paused
                }
            )
        })
        .await;
    assert_eq!(
        paused_alert,
        IntegrityAlert::ConnectionLost {
            reason: DisconnectKind::Paused
        }
    );

    // Resume connector
    handle.resume().await.expect("resume command");
    sink.wait_for_alerts(3).await;

    cancel.cancel();
    let _ = runner.await;
}
