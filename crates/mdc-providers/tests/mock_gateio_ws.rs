#![cfg(feature = "gateio")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Gate.io WebSocket connector.

use std::time::Duration;

use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, SequenceRange, Side, Symbol, UnixNanos};
use mdc_providers::gateio::{GateioConfig, GateioConnectorBuilder, GateioStreams};
use mdc_testkit::{MockWsServer, RecordedEvent, RecordingSink, load_fixture};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

const SNAPSHOT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/gateio/order_book_snapshot.json";
const UPDATE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/gateio/depth_update.json";
const BID_ONLY_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/gateio/depth_update_bid_only.json";
const ASK_ONLY_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/gateio/depth_update_ask_only.json";
const PING_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/gateio/heartbeat_or_ping.json";
const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/gateio/trade.json";
const SUB_ACK_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/gateio/subscribe_ack.json";

#[tokio::test]
async fn test_ws_handshake_and_obu_subscription() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = GateioConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC_USDT")];
    let sink = RecordingSink::new();

    let (connector, _handle) = GateioConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    let received = ws.received_messages();
    assert!(!received.is_empty(), "Expected subscribe frame");

    let v: serde_json::Value = serde_json::from_str(&received[0]).expect("valid json subscribe");
    assert_eq!(v["channel"], "spot.obu");
    assert_eq!(v["event"], "subscribe");
    assert_eq!(v["payload"], serde_json::json!(["ob.BTC_USDT.400"]));

    let now_secs = UnixNanos::now().as_nanos() / 1_000_000_000;
    let req_time = v["time"].as_u64().expect("timestamp as u64");
    assert!(req_time.abs_diff(now_secs) <= 5);

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_spot_ping_answered_with_spot_pong() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = GateioConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC_USDT")];
    let sink = RecordingSink::new();

    let (connector, _handle) = GateioConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send spot.ping from server
    let ping_json = load_fixture(PING_FIXTURE).expect("load ping fixture");
    ws.broadcast_text(&ping_json);

    tokio::time::sleep(Duration::from_millis(60)).await;

    let received = ws.received_messages();
    assert!(
        received.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["channel"] == "spot.pong" && v["time"] == 1700000030
            } else {
                false
            }
        }),
        "Expected spot.pong response echoing ping time 1700000030, got: {received:?}"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_snapshot_and_delta_lifecycle() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = GateioConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC_USDT")];
    let sink = RecordingSink::new();

    let (connector, _handle) = GateioConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send sub ack
    let ack_json = load_fixture(SUB_ACK_FIXTURE).expect("load sub ack fixture");
    ws.broadcast_text(&ack_json);

    // Send snapshot
    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    ws.broadcast_text(&snap_json);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send incremental delta
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");
    ws.broadcast_text(&upd_json);

    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();

    // Verify snapshot
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        )),
        "Expected BeginBook Snapshot"
    );

    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncCompleted {
                    last_update_id: Some(123456)
                },
                ..
            }
        )),
        "Expected ResyncCompleted alert"
    );

    // Verify delta
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::BeginBook {
                kind: BookKind::Delta,
                ..
            }
        )),
        "Expected BeginBook Delta"
    );

    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::EndBook {
                seq: Some(SequenceRange {
                    first: 123457,
                    last: 123460
                }),
                ..
            }
        )),
        "Expected EndBook with [123457, 123460]"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_gap_injection_triggers_resubscribe() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = GateioConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.resync_timeout = Duration::from_millis(100);

    let symbols = vec![Symbol::from("BTC_USDT")];
    let sink = RecordingSink::new();

    let (connector, _handle) = GateioConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send snapshot (u=100)
    let snap_frame = serde_json::json!({
        "time": 1700000000,
        "channel": "spot.obu",
        "event": "update",
        "result": {
            "t": 1700000000000_i64,
            "full": true,
            "s": "ob.BTC_USDT.400",
            "u": 100,
            "b": [["50000", "0.1"]],
            "a": [["50001", "0.1"]]
        }
    })
    .to_string();
    ws.broadcast_text(&snap_frame);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Clear received messages to check resubscribe frames specifically
    let pre_gap_count = ws.received_messages().len();

    // Send gapped update (expected 101, but U=105)
    let gap_frame = serde_json::json!({
        "time": 1700000000,
        "channel": "spot.obu",
        "event": "update",
        "result": {
            "t": 1700000000100_i64,
            "s": "ob.BTC_USDT.400",
            "U": 105,
            "u": 106,
            "b": [["50000", "0.2"]]
        }
    })
    .to_string();
    ws.broadcast_text(&gap_frame);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check sink received GapDetected
    let events = sink.events();
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::GapDetected {
                    expected: 101,
                    received: 105
                },
                ..
            }
        )),
        "Expected GapDetected alert"
    );

    // Check client sent unsubscribe then subscribe
    let post_gap_msgs = &ws.received_messages()[pre_gap_count..];
    assert!(
        post_gap_msgs.iter().any(|m| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(m) {
                v["channel"] == "spot.obu" && v["event"] == "unsubscribe"
            } else {
                false
            }
        }),
        "Expected unsubscribe frame after gap, got: {post_gap_msgs:?}"
    );

    assert!(
        post_gap_msgs.iter().any(|m| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(m) {
                v["channel"] == "spot.obu" && v["event"] == "subscribe"
            } else {
                false
            }
        }),
        "Expected subscribe frame after gap, got: {post_gap_msgs:?}"
    );

    // Server sends fresh snapshot to re-anchor
    let fresh_snap_frame = serde_json::json!({
        "time": 1700000001,
        "channel": "spot.obu",
        "event": "update",
        "result": {
            "t": 1700000001000_i64,
            "full": true,
            "s": "ob.BTC_USDT.400",
            "u": 200,
            "b": [["50000", "0.3"]],
            "a": [["50001", "0.3"]]
        }
    })
    .to_string();
    ws.broadcast_text(&fresh_snap_frame);

    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncCompleted {
                    last_update_id: Some(200)
                },
                ..
            }
        )),
        "Expected ResyncCompleted after fresh snapshot"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_one_sided_updates() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = GateioConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC_USDT")];
    let sink = RecordingSink::new();

    let (connector, _handle) = GateioConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Snapshot u=123460
    let snap = serde_json::json!({
        "time": 1700000000,
        "channel": "spot.obu",
        "event": "update",
        "result": {
            "t": 1700000000000_i64,
            "full": true,
            "s": "ob.BTC_USDT.400",
            "u": 123460,
            "b": [["50000", "0.1"]],
            "a": [["50001", "0.1"]]
        }
    })
    .to_string();
    ws.broadcast_text(&snap);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Bid-only update [123461, 123462]
    let bid_only = load_fixture(BID_ONLY_FIXTURE).expect("load bid only");
    ws.broadcast_text(&bid_only);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Ask-only update [123463, 123464]
    let ask_only = load_fixture(ASK_ONLY_FIXTURE).expect("load ask only");
    ws.broadcast_text(&ask_only);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::EndBook {
                seq: Some(SequenceRange {
                    first: 123461,
                    last: 123462
                }),
                ..
            }
        )),
        "Expected bid-only update processed"
    );

    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::EndBook {
                seq: Some(SequenceRange {
                    first: 123463,
                    last: 123464
                }),
                ..
            }
        )),
        "Expected ask-only update processed"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_public_trades() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = GateioConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.streams = GateioStreams {
        depth: false,
        trades: true,
    };

    let symbols = vec![Symbol::from("BTC_USDT")];
    let sink = RecordingSink::new();

    let (connector, _handle) = GateioConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let received = ws.received_messages();
    assert!(
        received.iter().any(|m| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(m) {
                v["channel"] == "spot.trades" && v["event"] == "subscribe"
            } else {
                false
            }
        }),
        "Expected spot.trades subscribe frame"
    );

    let trade_json = load_fixture(TRADE_FIXTURE).expect("load trade fixture");
    ws.broadcast_text(&trade_json);

    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    assert!(
        events.iter().any(|ev| {
            if let RecordedEvent::Trade { trade, .. } = ev {
                trade.aggressor == Side::Ask
                    && trade.price == Decimal::from(50000)
                    && trade.identity.as_ref().map(|id| id.value.as_str()) == Some("987654321")
            } else {
                false
            }
        }),
        "Expected public trade with native identity"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_control_pause_resume_cancel() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = GateioConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC_USDT")];
    let sink = RecordingSink::new();

    let (connector, handle) = GateioConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Pause
    handle.pause().await.expect("pause command");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ConnectionLost {
                    reason: DisconnectKind::Shutdown
                },
                ..
            }
        )),
        "Expected ConnectionLost on pause"
    );

    // Resume
    handle.resume().await.expect("resume command");
    tokio::time::sleep(Duration::from_millis(50)).await;

    cancel.cancel();
    let _ = runner.await;
}
