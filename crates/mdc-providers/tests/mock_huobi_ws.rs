#![cfg(feature = "huobi")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Huobi (HTX) WebSocket connector.

use std::io::Write;
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;
use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, SequenceRange, Side, Symbol};
use mdc_providers::huobi::{HuobiConfig, HuobiConnectorBuilder, HuobiStreams};
use mdc_testkit::{MockWsServer, RecordedEvent, RecordingSink, load_fixture};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

const SNAPSHOT_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/huobi/order_book_snapshot.json";
const UPDATE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/huobi/depth_update.json";
const PING_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/huobi/heartbeat_or_ping.json";
const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/huobi/trade.json";
const SUB_ACK_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/huobi/sub_ack.json";

fn gzip_compress(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

#[tokio::test]
async fn test_ws_handshake_and_subscriptions() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.streams = HuobiStreams {
        mbp: true,
        trades: true,
    };

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    let received = ws.received_messages();
    assert!(!received.is_empty(), "Expected subscription frames");

    // Check MBP sub frame
    assert!(
        received.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["sub"] == "market.btcusdt.mbp.400"
            } else {
                false
            }
        }),
        "Expected MBP subscribe frame, got: {received:?}"
    );

    // Check trades sub frame
    assert!(
        received.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["sub"] == "market.btcusdt.trade.detail"
            } else {
                false
            }
        }),
        "Expected trade subscribe frame, got: {received:?}"
    );

    // Check initial snapshot request frame
    assert!(
        received.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["req"] == "market.btcusdt.mbp.400" && v["id"].as_str().is_some()
            } else {
                false
            }
        }),
        "Expected in-band snapshot request frame, got: {received:?}"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_server_ping_answered_with_pong() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send binary gzip ping
    let ping_json = load_fixture(PING_FIXTURE).expect("load ping fixture");
    let ping_gz = gzip_compress(ping_json.as_bytes());
    let _ = ws.broadcast_binary(ping_gz);

    tokio::time::sleep(Duration::from_millis(60)).await;

    let received = ws.received_messages();
    assert!(
        received.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["pong"] == 1700000000000_i64
            } else {
                false
            }
        }),
        "Expected pong response echoing ping timestamp 1700000000000, got: {received:?}"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_snapshot_and_delta_lifecycle() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Find the snapshot request ID sent by client
    let req_id = ws
        .received_messages()
        .iter()
        .find_map(|msg| {
            let v = serde_json::from_str::<serde_json::Value>(msg).ok()?;
            if v["req"] == "market.btcusdt.mbp.400" {
                v["id"].as_str().map(|s| s.to_string())
            } else {
                None
            }
        })
        .expect("snapshot request id found");

    // Send sub ack
    let ack_json = load_fixture(SUB_ACK_FIXTURE).expect("load sub ack");
    let _ = ws.broadcast_binary(gzip_compress(ack_json.as_bytes()));

    // Send snapshot response with matching ID
    let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
    let mut snap_val: serde_json::Value = serde_json::from_str(&snap_raw).expect("parse snapshot");
    snap_val["id"] = serde_json::Value::String(req_id);
    snap_val["data"]["seqNum"] = serde_json::json!(100);
    let _ = ws.broadcast_binary(gzip_compress(snap_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send contiguous incremental delta (prevSeqNum: 100, seqNum: 101)
    let delta_raw = load_fixture(UPDATE_FIXTURE).expect("load update");
    let mut delta_val: serde_json::Value = serde_json::from_str(&delta_raw).expect("parse update");
    delta_val["tick"]["prevSeqNum"] = serde_json::json!(100);
    delta_val["tick"]["seqNum"] = serde_json::json!(101);
    let _ = ws.broadcast_binary(gzip_compress(delta_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();

    // Verify ResyncStarted
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncStarted,
                ..
            }
        )),
        "Expected ResyncStarted alert"
    );

    // Verify Snapshot BeginBook
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

    // Verify Snapshot EndBook
    assert!(
        events
            .iter()
            .any(|ev| matches!(ev, RecordedEvent::EndBook { seq: None, .. })),
        "Expected EndBook Snapshot"
    );

    // Verify ResyncCompleted
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncCompleted {
                    last_update_id: Some(100),
                },
                ..
            }
        )),
        "Expected ResyncCompleted alert"
    );

    // Verify Delta BeginBook
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

    // Verify Delta EndBook
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::EndBook {
                seq: Some(SequenceRange {
                    first: 101,
                    last: 101
                }),
                ..
            }
        )),
        "Expected EndBook Delta with [101, 101]"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_delta_buffering_before_snapshot() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let req_id = ws
        .received_messages()
        .iter()
        .find_map(|msg| {
            let v = serde_json::from_str::<serde_json::Value>(msg).ok()?;
            if v["req"] == "market.btcusdt.mbp.400" {
                v["id"].as_str().map(|s| s.to_string())
            } else {
                None
            }
        })
        .expect("snapshot request id found");

    // First send delta: seqNum = 101, prevSeqNum = 100 -> must be buffered
    let delta_raw = load_fixture(UPDATE_FIXTURE).expect("load update");
    let mut delta_val: serde_json::Value = serde_json::from_str(&delta_raw).expect("parse update");
    delta_val["tick"]["prevSeqNum"] = serde_json::json!(100);
    delta_val["tick"]["seqNum"] = serde_json::json!(101);
    let _ = ws.broadcast_binary(gzip_compress(delta_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    // At this point, no books should have been emitted
    let events_before_snap = sink.events();
    assert!(
        !events_before_snap
            .iter()
            .any(|ev| matches!(ev, RecordedEvent::BeginBook { .. })),
        "No book should be emitted while buffering diffs"
    );

    // Now send snapshot: seqNum = 100
    let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
    let mut snap_val: serde_json::Value = serde_json::from_str(&snap_raw).expect("parse snapshot");
    snap_val["id"] = serde_json::Value::String(req_id);
    snap_val["data"]["seqNum"] = serde_json::json!(100);
    let _ = ws.broadcast_binary(gzip_compress(snap_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Both snapshot and buffered delta should have been applied
    let events = sink.events();
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        )),
        "Expected snapshot BeginBook"
    );

    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::EndBook {
                seq: Some(SequenceRange {
                    first: 101,
                    last: 101
                }),
                ..
            }
        )),
        "Expected buffered delta EndBook [101, 101]"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_gap_detected_triggers_resync() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let req_id = ws
        .received_messages()
        .iter()
        .find_map(|msg| {
            let v = serde_json::from_str::<serde_json::Value>(msg).ok()?;
            if v["req"] == "market.btcusdt.mbp.400" {
                v["id"].as_str().map(|s| s.to_string())
            } else {
                None
            }
        })
        .expect("snapshot request id found");

    // Establish snapshot at seqNum 100
    let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
    let mut snap_val: serde_json::Value = serde_json::from_str(&snap_raw).expect("parse snapshot");
    snap_val["id"] = serde_json::Value::String(req_id);
    snap_val["data"]["seqNum"] = serde_json::json!(100);
    let _ = ws.broadcast_binary(gzip_compress(snap_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg_count_before_gap = ws.received_messages().len();

    // Send gapped delta: prevSeqNum 105 != expected 100 + 1 (101)
    let delta_raw = load_fixture(UPDATE_FIXTURE).expect("load update");
    let mut delta_val: serde_json::Value = serde_json::from_str(&delta_raw).expect("parse update");
    delta_val["tick"]["prevSeqNum"] = serde_json::json!(105);
    delta_val["tick"]["seqNum"] = serde_json::json!(106);
    let _ = ws.broadcast_binary(gzip_compress(delta_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Check sink received GapDetected alert
    let events = sink.events();
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::GapDetected {
                    expected: 101,
                    received: 106,
                },
                ..
            }
        )),
        "Expected GapDetected alert"
    );

    // Check new snapshot request was queued / sent by client
    let new_msgs = &ws.received_messages()[msg_count_before_gap..];
    assert!(
        new_msgs.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["req"] == "market.btcusdt.mbp.400"
            } else {
                false
            }
        }),
        "Expected new snapshot request after gap, got: {new_msgs:?}"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_stale_snapshot_handling() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let req_id = ws
        .received_messages()
        .iter()
        .find_map(|msg| {
            let v = serde_json::from_str::<serde_json::Value>(msg).ok()?;
            if v["req"] == "market.btcusdt.mbp.400" {
                v["id"].as_str().map(|s| s.to_string())
            } else {
                None
            }
        })
        .expect("snapshot request id found");

    // First buffer delta starting at seqNum 101 (prevSeqNum 100)
    let delta_raw = load_fixture(UPDATE_FIXTURE).expect("load update");
    let mut delta_val: serde_json::Value = serde_json::from_str(&delta_raw).expect("parse update");
    delta_val["tick"]["prevSeqNum"] = serde_json::json!(100);
    delta_val["tick"]["seqNum"] = serde_json::json!(101);
    let _ = ws.broadcast_binary(gzip_compress(delta_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Snapshot arrives with seqNum 90 (older than buffered delta's first sequence 101) -> stale
    let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
    let mut snap_val: serde_json::Value = serde_json::from_str(&snap_raw).expect("parse snapshot");
    snap_val["id"] = serde_json::Value::String(req_id);
    snap_val["data"]["seqNum"] = serde_json::json!(90);
    let _ = ws.broadcast_binary(gzip_compress(snap_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    // Stale snapshot must not emit ResyncCompleted
    assert!(
        !events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncCompleted { .. },
                ..
            }
        )),
        "Stale snapshot must not complete resync"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_multi_symbol_snapshot_serialization() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusdt"), Symbol::from("ethusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Check that at most ONE snapshot request is in flight initially
    let initial_reqs: Vec<_> = ws
        .received_messages()
        .iter()
        .filter_map(|msg| {
            let v = serde_json::from_str::<serde_json::Value>(msg).ok()?;
            if v.get("req").is_some() {
                Some(v)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        initial_reqs.len(),
        1,
        "Expected exactly 1 snapshot request in flight initially, got: {initial_reqs:?}"
    );

    let active_req = &initial_reqs[0];
    let active_id = active_req["id"].as_str().expect("req id").to_string();
    let active_ch = active_req["req"].as_str().expect("channel");

    // Reply to the first snapshot request
    let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
    let mut snap_val: serde_json::Value = serde_json::from_str(&snap_raw).expect("parse snapshot");
    snap_val["id"] = serde_json::Value::String(active_id);
    snap_val["rep"] = serde_json::Value::String(active_ch.to_string());
    snap_val["data"]["seqNum"] = serde_json::json!(100);
    let _ = ws.broadcast_binary(gzip_compress(snap_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Now the second snapshot request should have been dispatched
    let all_reqs: Vec<_> = ws
        .received_messages()
        .iter()
        .filter_map(|msg| {
            let v = serde_json::from_str::<serde_json::Value>(msg).ok()?;
            if v.get("req").is_some() {
                Some(v)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        all_reqs.len(),
        2,
        "Expected both snapshot requests sent sequentially, got: {all_reqs:?}"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_trades_stream() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.streams = HuobiStreams {
        mbp: false,
        trades: true,
    };

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Broadcast trade binary frame
    let trade_json = load_fixture(TRADE_FIXTURE).expect("load trade");
    let _ = ws.broadcast_binary(gzip_compress(trade_json.as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    let trade_opt = events.iter().find_map(|ev| {
        if let RecordedEvent::Trade { trade, .. } = ev {
            Some((trade.price, trade.volume, trade.aggressor))
        } else {
            None
        }
    });

    assert!(trade_opt.is_some(), "Expected Trade event in sink");
    let (p, s, side) = trade_opt.unwrap();
    assert_eq!(p, Decimal::from_str_exact("26000.1").unwrap());
    assert_eq!(s, Decimal::from_str_exact("0.01").unwrap());
    assert_eq!(side, Side::Bid);

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_manual_resync_command() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let req_id = ws
        .received_messages()
        .iter()
        .find_map(|msg| {
            let v = serde_json::from_str::<serde_json::Value>(msg).ok()?;
            if v["req"] == "market.btcusdt.mbp.400" {
                v["id"].as_str().map(|s| s.to_string())
            } else {
                None
            }
        })
        .expect("snapshot request id found");

    // Reply to initial snapshot
    let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
    let mut snap_val: serde_json::Value = serde_json::from_str(&snap_raw).expect("parse snapshot");
    snap_val["id"] = serde_json::Value::String(req_id);
    snap_val["data"]["seqNum"] = serde_json::json!(100);
    let _ = ws.broadcast_binary(gzip_compress(snap_val.to_string().as_bytes()));

    tokio::time::sleep(Duration::from_millis(50)).await;

    let msg_count = ws.received_messages().len();

    // Trigger manual resync via handle
    handle
        .request_resync(Symbol::from("btcusdt"))
        .await
        .expect("command resync");

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Check sink received ResyncStarted
    let events = sink.events();
    let resync_starts = events
        .iter()
        .filter(|ev| {
            matches!(
                ev,
                RecordedEvent::Alert {
                    alert: IntegrityAlert::ResyncStarted,
                    ..
                }
            )
        })
        .count();
    assert!(
        resync_starts >= 2,
        "Expected at least 2 ResyncStarted alerts (initial + manual resync)"
    );

    // Check new snapshot request was sent
    let new_msgs = &ws.received_messages()[msg_count..];
    assert!(
        new_msgs.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["req"] == "market.btcusdt.mbp.400"
            } else {
                false
            }
        }),
        "Expected new snapshot request after manual resync"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_cancellation_emits_connection_lost() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = HuobiConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("btcusdt")];
    let sink = RecordingSink::new();

    let (connector, _handle) = HuobiConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancel token
    cancel.cancel();
    let run_res = runner.await;
    assert!(run_res.is_ok(), "Runner should exit cleanly");

    let events = sink.events();
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ConnectionLost {
                    reason: DisconnectKind::Shutdown,
                },
                ..
            }
        )),
        "Expected ConnectionLost alert with DisconnectKind::Shutdown"
    );
}
