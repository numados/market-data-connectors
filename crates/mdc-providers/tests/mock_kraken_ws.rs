#![cfg(feature = "kraken")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Kraken WebSocket v2 connector.

use std::time::Duration;

use mdc_core::{BookKind, IntegrityAlert, SourceErrorKind, Symbol};
use mdc_providers::kraken::{KrakenConfig, KrakenConnectorBuilder};
use mdc_testkit::{MockWsServer, RecordedEvent, RecordingSink, WsAction, load_fixture};
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

    ws.wait_for_client_messages(2).await;

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

    ws.wait_for_client_messages(1).await;

    // Send valid snapshot
    let _ = ws.broadcast_text(&snap_json);
    ws.sync_barrier().await;

    // Send valid update
    let _ = ws.broadcast_text(&upd_json);
    ws.sync_barrier().await;

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

    ws.wait_for_client_messages(1).await;

    // Send valid snapshot to enter InSync
    let _ = ws.broadcast_text(&snap_json);
    ws.sync_barrier().await;

    // Send corrupt checksum update
    let _ = ws.broadcast_text(&bad_upd_str);
    ws.sync_barrier().await;

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

    // The in-place resubscribe is flushed on the next timer tick: subscribe, unsubscribe, subscribe.
    ws.wait_for_client_messages(3).await;

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

    ws.wait_for_client_messages(1).await;

    // Send valid snapshot -> InSync
    let _ = ws.broadcast_text(&snap_json);
    ws.sync_barrier().await;

    // Send mismatch -> triggers resubscribe, transitioning to AwaitingSnapshot
    let _ = ws.broadcast_text(&bad_upd_str);
    ws.sync_barrier().await;

    // Server deliberately does NOT send snapshot back; the resubscribe timeout
    // (80ms, checked on the 100ms timer tick) must drive the symbol into quarantine.
    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::Quarantined { .. }))
        .await;

    let alerts = sink.alerts_for("BTC/USD");
    let resync_starts = alerts
        .iter()
        .filter(|a| matches!(a, IntegrityAlert::ResyncStarted))
        .count();

    // Quarantine expiry (150ms, next timer tick) must announce a new resync.
    let new_alerts = sink.wait_for_alerts_for("BTC/USD", alerts.len() + 1).await;
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
    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::Quarantined { .. }))
        .await;

    // Send valid snapshot DURING quarantine
    let _ = ws.broadcast_text(&snap_json);
    ws.sync_barrier().await;

    // Send valid update -> must be accepted immediately!
    sink.clear();
    let _ = ws.broadcast_text(&upd_json);
    ws.sync_barrier().await;

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

    ws.wait_for_client_messages(1).await;

    // Operator requests ResyncAll across all 40 symbols
    handle
        .request_resync_all()
        .await
        .expect("request resync all");
    // 2 initial subscribes + 2 unsubscribe + 2 subscribe batches.
    ws.wait_for_client_messages(6).await;

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
        // Pacing wait: the watchdog is wall-clock driven, so heartbeats must arrive
        // spaced inside its window; no readiness signal can replace the spacing.
        tokio::time::sleep(Duration::from_millis(40)).await;
        let _ = ws.broadcast_text(&heartbeat_json);
    }

    assert_eq!(
        ws.ping_count(),
        0,
        "Heartbeats must reset silence watchdog and prevent pings"
    );

    // Stop sending heartbeats and wait for watchdog ping
    let ping_count = ws.wait_for_pings(1).await;

    assert!(
        ping_count >= 1,
        "Watchdog must send ping when heartbeat ceases"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_checksumless_update_fails_closed() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    // Remove checksum from the update payload
    let mut bad_upd: serde_json::Value =
        serde_json::from_str(&upd_json).expect("valid update json");
    bad_upd["data"][0]
        .as_object_mut()
        .unwrap()
        .remove("checksum");
    let bad_upd_str = bad_upd.to_string();

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    assert_eq!(config.checksum_policy, mdc_core::ChecksumPolicy::Required);

    let sink = RecordingSink::new();
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    // Send valid snapshot -> InSync
    let _ = ws.broadcast_text(&snap_json);
    ws.sync_barrier().await;

    let books_after_snap: Vec<_> = sink
        .events()
        .into_iter()
        .filter_map(|e| match e {
            RecordedEvent::BeginBook { symbol, kind } if symbol.as_str() == "BTC/USD" => Some(kind),
            _ => None,
        })
        .collect();
    assert_eq!(books_after_snap.len(), 1, "Snapshot must be applied");
    assert_eq!(books_after_snap[0], BookKind::Snapshot);

    // Send checksum-less update -> must fail closed
    let _ = ws.broadcast_text(&bad_upd_str);
    ws.sync_barrier().await;

    // Verify: no book delta was emitted
    let books_after_update: Vec<_> = sink
        .events()
        .into_iter()
        .filter_map(|e| match e {
            RecordedEvent::BeginBook { symbol, kind } if symbol.as_str() == "BTC/USD" => Some(kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        books_after_update.len(),
        1,
        "No book delta may be emitted when checksum is absent under Required policy"
    );

    // Verify: alerts contain ChecksumMismatch and ResyncStarted
    let alerts = sink.alerts_for("BTC/USD");
    let has_mismatch = alerts
        .iter()
        .any(|a| matches!(a, IntegrityAlert::ChecksumMismatch { .. }));
    let has_resync = alerts
        .iter()
        .any(|a| matches!(a, IntegrityAlert::ResyncStarted));
    assert!(
        has_mismatch,
        "Must emit ChecksumMismatch alert for absent checksum"
    );
    assert!(has_resync, "Must emit ResyncStarted alert to recover book");

    cancel.cancel();
    let _ = runner.await;
}

const REJECT_BTC: &str =
    r#"{"method":"subscribe","success":false,"error":"Subscription rejected","symbol":"BTC/USD"}"#;

/// First rejection cools down and retries; a repeated rejection quarantines with an alert.
#[tokio::test]
async fn test_rejection_cools_down_then_repeated_rejection_quarantines() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::SendText(REJECT_BTC.to_string()),
        WsAction::WaitClientMessage,
        WsAction::WaitClientMessage,
        WsAction::SendText(REJECT_BTC.to_string()),
    ]);

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rejection_cooldown = Duration::from_millis(100);
    config.quarantine_duration = Duration::from_secs(60);
    config.resubscribe_timeout = Duration::from_secs(60);

    let sink = RecordingSink::new();
    let (connector, _handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    let quarantined = sink
        .wait_for_alert_matching(|a| matches!(a, IntegrityAlert::Quarantined { .. }))
        .await;
    assert_eq!(
        quarantined,
        IntegrityAlert::Quarantined {
            retry_in_ms: 60_000
        }
    );

    // Cooldown (no quarantine alert), retry via ResyncStarted, then quarantine.
    assert_eq!(
        sink.alerts_for("BTC/USD"),
        vec![
            IntegrityAlert::ResyncStarted,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::Parse
            },
            IntegrityAlert::ResyncStarted,
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::Parse
            },
            IntegrityAlert::Quarantined {
                retry_in_ms: 60_000
            },
        ]
    );

    // subscribe, then the retry's unsubscribe + subscribe.
    let methods: Vec<String> = ws
        .received_messages()
        .iter()
        .map(|m| {
            serde_json::from_str::<serde_json::Value>(m).expect("json")["method"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    assert_eq!(methods, vec!["subscribe", "unsubscribe", "subscribe"]);

    cancel.cancel();
    let _ = runner.await;
}

/// A resync requested while the connection is in reconnect backoff is executed
/// after the reconnect instead of being discarded; the backoff is not shortened.
#[tokio::test]
async fn test_resync_during_backoff_executes_after_reconnect() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    // The first connect attempt fails, which puts the connection into backoff.
    ws.refuse_next_handshakes(1);

    let mut config = KrakenConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.initial_backoff = Duration::from_millis(400);
    config.max_backoff = Duration::from_millis(400);

    let sink = RecordingSink::new();
    let (connector, handle) = KrakenConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTC/USD")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_connections(1).await;
    let refused_at = std::time::Instant::now();
    // Buffered until the connection reaches its backoff wait; deferred there.
    handle.request_resync_all().await.expect("resync command");

    ws.wait_for_handshakes(1).await;
    assert!(
        refused_at.elapsed() >= Duration::from_millis(350),
        "operator resync must not shorten the backoff"
    );
    ws.wait_for_client_message_matching(|m| m.contains("\"unsubscribe\""))
        .await;

    // Connect-time ResyncStarted plus the deferred command's ResyncStarted.
    let started = sink
        .alerts_for("BTC/USD")
        .iter()
        .filter(|a| matches!(a, IntegrityAlert::ResyncStarted))
        .count();
    assert_eq!(started, 2, "connect-time and deferred resync");

    cancel.cancel();
    let _ = runner.await;
}
