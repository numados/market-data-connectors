#![cfg(feature = "kraken")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Kraken WebSocket v2 connector.

use std::time::Duration;

use mdc_core::{BookKind, IntegrityAlert, Symbol};
use mdc_providers::kraken::{KrakenConfig, KrakenConnectorBuilder};
use mdc_testkit::{MockWsServer, RecordedEvent, RecordingSink, load_fixture};
use tokio_util::sync::CancellationToken;

const SNAPSHOT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/kraken/order_book_snapshot.json";
const UPDATE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/kraken/depth_update.json";
const HEARTBEAT_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/kraken/heartbeat_or_ping.json";

#[tokio::test]
async fn test_ws_subscribe_message_and_chunking() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.resubscribe_batch_size = 2; // Test chunking with batch size 2

    let symbols = vec![
        Symbol::from("BTC/USD"),
        Symbol::from("ETH/USD"),
        Symbol::from("SOL/USD"),
    ];

    let sink = RecordingSink::new();
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(80)).await;

    let received = ws.received_messages();
    assert_eq!(received.len(), 2, "Expected 2 chunked subscribe frames");

    let v1: serde_json::Value = serde_json::from_str(&received[0]).expect("valid json batch 1");
    assert_eq!(v1["method"], "subscribe");
    assert_eq!(v1["params"]["channel"], "book");
    assert_eq!(v1["params"]["depth"], 1000);
    assert_eq!(
        v1["params"]["symbol"],
        serde_json::json!(["BTC/USD", "ETH/USD"])
    );

    let v2: serde_json::Value = serde_json::from_str(&received[1]).expect("valid json batch 2");
    assert_eq!(v2["params"]["symbol"], serde_json::json!(["SOL/USD"]));

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_snapshot_and_update_lifecycle() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");

    let sink = RecordingSink::new();
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send valid snapshot
    ws.broadcast_text(&snap_json);
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send valid update
    ws.broadcast_text(&upd_json);
    tokio::time::sleep(Duration::from_millis(60)).await;

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
    assert!(has_delta, "Must emit L2Delta from valid update in InSync");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_checksum_mismatch_resubscribes() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    // Replace checksum with mismatched value
    let mut bad_upd: serde_json::Value =
        serde_json::from_str(&upd_json).expect("valid update json");
    bad_upd["data"][0]["checksum"] = serde_json::json!(999_999_999);
    let bad_upd_str = bad_upd.to_string();

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");

    let sink = RecordingSink::new();
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send valid snapshot to enter InSync
    ws.broadcast_text(&snap_json);
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send corrupt checksum update
    ws.broadcast_text(&bad_upd_str);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Check alerts
    let alerts = sink.alerts_for("BTC/USD");
    let has_mismatch = alerts.iter().any(|a| {
        matches!(
            a,
            IntegrityAlert::ChecksumMismatch {
                expected: 999_999_999,
                ..
            }
        )
    });
    assert!(has_mismatch, "Must emit ChecksumMismatch alert");

    // Check mock server received unsubscribe followed by subscribe
    let messages = ws.received_messages();
    let has_unsub = messages.iter().any(|m| {
        let v: Result<serde_json::Value, _> = serde_json::from_str(m);
        v.map(|j| {
            j["method"] == "unsubscribe" && j["params"]["symbol"] == serde_json::json!(["BTC/USD"])
        })
        .unwrap_or(false)
    });
    let has_sub = messages.iter().skip(1).any(|m| {
        let v: Result<serde_json::Value, _> = serde_json::from_str(m);
        v.map(|j| {
            j["method"] == "subscribe" && j["params"]["symbol"] == serde_json::json!(["BTC/USD"])
        })
        .unwrap_or(false)
    });
    assert!(has_unsub, "Must send in-place unsubscribe for BTC/USD");
    assert!(has_sub, "Must send in-place resubscribe for BTC/USD");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_resubscribe_timeout_quarantine() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    let mut bad_upd: serde_json::Value =
        serde_json::from_str(&upd_json).expect("valid update json");
    bad_upd["data"][0]["checksum"] = serde_json::json!(888_888_888);
    let bad_upd_str = bad_upd.to_string();

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.resubscribe_timeout = Duration::from_millis(80);
    config.quarantine_duration = Duration::from_millis(150);

    let sink = RecordingSink::new();
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send valid snapshot -> InSync
    ws.broadcast_text(&snap_json);
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send mismatch -> triggers resubscribe, transitioning to AwaitingSnapshot
    ws.broadcast_text(&bad_upd_str);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Server deliberately does NOT send snapshot back
    // Wait past resubscribe_timeout (80ms) and timer tick (100ms) to trigger quarantine
    tokio::time::sleep(Duration::from_millis(250)).await;

    let alerts = sink.alerts_for("BTC/USD");
    let has_quarantine = alerts
        .iter()
        .any(|a| matches!(a, IntegrityAlert::Quarantined { .. }));
    assert!(
        has_quarantine,
        "Must emit Quarantined alert after resubscribe timeout"
    );

    // Wait past quarantine_duration (150ms) and timer tick to re-trigger resubscribe
    tokio::time::sleep(Duration::from_millis(300)).await;

    let resync_starts = alerts
        .iter()
        .filter(|a| matches!(a, IntegrityAlert::ResyncStarted))
        .count();
    let new_alerts = sink.alerts_for("BTC/USD");
    let new_resync_starts = new_alerts
        .iter()
        .filter(|a| matches!(a, IntegrityAlert::ResyncStarted))
        .count();
    assert!(
        new_resync_starts > resync_starts,
        "Must emit ResyncStarted upon quarantine expiry"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_snapshot_during_quarantine_lifts_quarantine() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.resubscribe_timeout = Duration::from_millis(60);
    config.quarantine_duration = Duration::from_secs(10); // Long quarantine

    let sink = RecordingSink::new();
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Initial subscribe without snapshot -> times out into quarantine
    tokio::time::sleep(Duration::from_millis(180)).await;

    let alerts = sink.alerts_for("BTC/USD");
    assert!(
        alerts
            .iter()
            .any(|a| matches!(a, IntegrityAlert::Quarantined { .. })),
        "Symbol must enter quarantine"
    );

    // Send valid snapshot DURING quarantine
    ws.broadcast_text(&snap_json);
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send valid update -> must be accepted immediately!
    sink.clear();
    ws.broadcast_text(&upd_json);
    tokio::time::sleep(Duration::from_millis(60)).await;

    let events = sink.events();
    let has_delta = events.iter().any(|e| {
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
        "Valid snapshot during quarantine must immediately lift quarantine and accept deltas"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_batch_chunking_mismatch() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.resubscribe_batch_size = 30; // 30 symbols per batch

    // 40 symbols
    let symbols: Vec<Symbol> = (0..40)
        .map(|i| Symbol::from(format!("SYM{i}/USD")))
        .collect();

    let sink = RecordingSink::new();
    let (connector, handle) = KrakenConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(80)).await;

    // Operator requests ResyncAll across all 40 symbols
    handle
        .request_resync_all()
        .await
        .expect("request resync all");
    tokio::time::sleep(Duration::from_millis(150)).await;

    let received = ws.received_messages();
    let unsubs: Vec<_> = received
        .iter()
        .filter(|m| {
            let v: Result<serde_json::Value, _> = serde_json::from_str(m);
            v.map(|j| j["method"] == "unsubscribe").unwrap_or(false)
        })
        .collect();

    assert_eq!(
        unsubs.len(),
        2,
        "40 symbols must be chunked into 2 unsubscribe batches (30 + 10)"
    );

    let v1: serde_json::Value = serde_json::from_str(unsubs[0]).unwrap();
    let v2: serde_json::Value = serde_json::from_str(unsubs[1]).unwrap();
    assert_eq!(v1["params"]["symbol"].as_array().unwrap().len(), 30);
    assert_eq!(v2["params"]["symbol"].as_array().unwrap().len(), 10);

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_heartbeat_silence_watchdog() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let heartbeat_json = load_fixture(HEARTBEAT_FIXTURE).expect("load heartbeat fixture");

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.watchdog_ping_after = Duration::from_millis(120);
    config.watchdog_reconnect_after = Duration::from_millis(250);

    let sink = RecordingSink::new();
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Send heartbeats every 40ms for 200ms (longer than watchdog_ping_after 120ms)
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(40)).await;
        ws.broadcast_text(&heartbeat_json);
    }

    assert_eq!(
        ws.ping_count(),
        0,
        "Heartbeats must reset silence watchdog and prevent pings"
    );

    // Stop sending heartbeats and wait > ping_after + timer interval
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        ws.ping_count() >= 1,
        "Watchdog must send ping when heartbeat ceases"
    );

    cancel.cancel();
    let _ = runner.await;
}
