#![cfg(feature = "coinbase-advanced")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Coinbase Advanced Trade connector.

use std::time::Duration;

use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, Side, Symbol};
use mdc_providers::coinbase_advanced::{
    CoinbaseAdvancedConfig, CoinbaseAdvancedConnectorBuilder, CoinbaseAdvancedStreams,
};
use mdc_testkit::{MockWsServer, RecordedEvent, RecordingSink, load_fixture};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

const SNAPSHOT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/coinbase_advanced/order_book_snapshot.json";
const UPDATE_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/coinbase_advanced/depth_update.json";
const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/coinbase_advanced/trade.json";

#[tokio::test]
async fn test_ws_handshake_and_subscriptions() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.streams = CoinbaseAdvancedStreams {
        level2: true,
        heartbeats: true,
        market_trades: true,
    };

    let symbols = vec![Symbol::from("BTC-USD"), Symbol::from("ETH-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(3).await;

    let received = ws.received_messages();
    assert!(!received.is_empty(), "Expected subscription frames");

    // Check level2 subscription frame
    assert!(
        received.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["type"] == "subscribe"
                    && v["channel"] == "level2"
                    && v["product_ids"] == serde_json::json!(["BTC-USD", "ETH-USD"])
            } else {
                false
            }
        }),
        "Expected level2 subscription frame, got: {received:?}"
    );

    // Check heartbeats subscription frame
    assert!(
        received.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["type"] == "subscribe"
                    && v["channel"] == "heartbeats"
                    && v["product_ids"] == serde_json::json!(["BTC-USD", "ETH-USD"])
            } else {
                false
            }
        }),
        "Expected heartbeats subscription frame, got: {received:?}"
    );

    // Check market_trades subscription frame
    assert!(
        received.iter().any(|msg| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(msg) {
                v["type"] == "subscribe"
                    && v["channel"] == "market_trades"
                    && v["product_ids"] == serde_json::json!(["BTC-USD", "ETH-USD"])
            } else {
                false
            }
        }),
        "Expected market_trades subscription frame, got: {received:?}"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_snapshot_and_delta_lifecycle() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for initial ResyncStarted alert
    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncStarted))
        .await;

    // Check initial ResyncStarted alert
    let alerts_before = sink.alerts_for("BTC-USD");
    assert!(
        alerts_before
            .iter()
            .any(|a| matches!(a, IntegrityAlert::ResyncStarted)),
        "Must emit ResyncStarted on connect"
    );

    // Broadcast snapshot fixture (sequence 1)
    let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
    let _ = ws.broadcast_text(snap_raw);

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    let events = sink.events();
    let has_begin_snapshot = events.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                symbol,
                kind: BookKind::Snapshot
            } if symbol.as_str() == "BTC-USD"
        )
    });
    assert!(has_begin_snapshot, "Expected BeginBook(Snapshot)");

    let alerts_after_snap = sink.alerts_for("BTC-USD");
    assert!(
        alerts_after_snap
            .iter()
            .any(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. })),
        "Must emit ResyncCompleted on snapshot"
    );

    // Broadcast incremental update fixture (sequence 2)
    let upd_raw = load_fixture(UPDATE_FIXTURE).expect("load update");
    let _ = ws.broadcast_text(upd_raw);

    sink.wait_for_event_matching(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                symbol,
                kind: BookKind::Delta
            } if symbol.as_str() == "BTC-USD"
        )
    })
    .await;

    let events_after_upd = sink.events();
    let has_begin_delta = events_after_upd.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                symbol,
                kind: BookKind::Delta
            } if symbol.as_str() == "BTC-USD"
        )
    });
    assert!(has_begin_delta, "Expected BeginBook(Delta)");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_gap_injection_invalidates_all_products_and_resubscribes() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.resync_timeout = Duration::from_millis(100);

    let symbols = vec![Symbol::from("BTC-USD"), Symbol::from("ETH-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    sink.wait_for_alerts(2).await;

    // Send snapshot for BTC-USD (seq 1)
    let snap_btc = serde_json::json!({
        "channel": "l2_data",
        "sequence_num": 1,
        "timestamp": "2023-02-09T20:32:50.000000Z",
        "events": [{
            "type": "snapshot",
            "product_id": "BTC-USD",
            "updates": [{
                "side": "bid",
                "event_time": "2023-02-09T20:32:50.000000Z",
                "price_level": "21900.00",
                "new_quantity": "1.00"
            }]
        }]
    });
    let _ = ws.broadcast_text(snap_btc.to_string());

    // Send snapshot for ETH-USD (seq 2)
    let snap_eth = serde_json::json!({
        "channel": "l2_data",
        "sequence_num": 2,
        "timestamp": "2023-02-09T20:32:51.000000Z",
        "events": [{
            "type": "snapshot",
            "product_id": "ETH-USD",
            "updates": [{
                "side": "bid",
                "event_time": "2023-02-09T20:32:51.000000Z",
                "price_level": "1600.00",
                "new_quantity": "5.00"
            }]
        }]
    });
    let _ = ws.broadcast_text(snap_eth.to_string());

    sink.wait_for_alerts(4).await;

    // Verify both are synchronized
    assert!(
        sink.alerts_for("BTC-USD")
            .iter()
            .any(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
    );
    assert!(
        sink.alerts_for("ETH-USD")
            .iter()
            .any(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
    );

    // Inject sequence gap: seq 2 -> 5 (gap: expected 3, received 5)
    let gap_frame = serde_json::json!({
        "channel": "l2_data",
        "sequence_num": 5,
        "timestamp": "2023-02-09T20:32:55.000000Z",
        "events": [{
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [{
                "side": "bid",
                "event_time": "2023-02-09T20:32:55.000000Z",
                "price_level": "21950.00",
                "new_quantity": "1.50"
            }]
        }]
    });
    let _ = ws.broadcast_text(gap_frame.to_string());

    sink.wait_for_alert_matching(|a| {
        matches!(
            a,
            IntegrityAlert::GapDetected {
                expected: 3,
                received: 5
            }
        )
    })
    .await;
    ws.wait_for_client_message_matching(|m| m.contains("unsubscribe"))
        .await;
    ws.wait_for_client_message_matching(|m| m.contains("subscribe"))
        .await;

    // Both BTC-USD and ETH-USD must have GapDetected and ResyncStarted alerts
    let btc_alerts = sink.alerts_for("BTC-USD");
    assert!(
        btc_alerts.iter().any(|a| matches!(
            a,
            IntegrityAlert::GapDetected {
                expected: 3,
                received: 5
            }
        )),
        "Expected GapDetected for BTC-USD"
    );

    let eth_alerts = sink.alerts_for("ETH-USD");
    assert!(
        eth_alerts.iter().any(|a| matches!(
            a,
            IntegrityAlert::GapDetected {
                expected: 3,
                received: 5
            }
        )),
        "Expected GapDetected for ETH-USD"
    );

    // Verify client sent unsubscribe + subscribe messages
    let msgs = ws.received_messages();
    assert!(
        msgs.iter().any(|m| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(m) {
                v["type"] == "unsubscribe" && v["channel"] == "level2"
            } else {
                false
            }
        }),
        "Expected unsubscribe for level2"
    );
    assert!(
        msgs.iter().any(|m| {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(m) {
                v["type"] == "subscribe" && v["channel"] == "level2"
            } else {
                false
            }
        }),
        "Expected resubscribe for level2"
    );

    // Delta arriving before snapshot must be dropped
    let pre_snap_events_count = sink.events().len();
    let delta_frame = serde_json::json!({
        "channel": "l2_data",
        "sequence_num": 6,
        "timestamp": "2023-02-09T20:32:56.000000Z",
        "events": [{
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [{
                "side": "bid",
                "event_time": "2023-02-09T20:32:56.000000Z",
                "price_level": "21950.00",
                "new_quantity": "2.00"
            }]
        }]
    });
    let _ = ws.broadcast_text(delta_frame.to_string());
    tokio::time::sleep(Duration::from_millis(20)).await;

    // No new BeginBook event should have been added
    let delta_added = sink.events()[pre_snap_events_count..].iter().any(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Delta,
                ..
            }
        )
    });
    assert!(!delta_added, "Delta before snapshot must be suppressed");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_stale_and_duplicate_sequence_dropped() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncStarted))
        .await;

    // Send snapshot (seq 1)
    let snap_raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
    let _ = ws.broadcast_text(snap_raw);

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    let count_after_snap = sink.events().len();

    // Send duplicate sequence (seq 1)
    let dup_frame = serde_json::json!({
        "channel": "l2_data",
        "sequence_num": 1,
        "timestamp": "2023-02-09T20:32:50.000000Z",
        "events": [{
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [{
                "side": "bid",
                "event_time": "2023-02-09T20:32:50.000000Z",
                "price_level": "21900.00",
                "new_quantity": "1.00"
            }]
        }]
    });
    let _ = ws.broadcast_text(dup_frame.to_string());

    // Send older sequence (seq 0)
    let old_frame = serde_json::json!({
        "channel": "l2_data",
        "sequence_num": 0,
        "timestamp": "2023-02-09T20:32:49.000000Z",
        "events": [{
            "type": "update",
            "product_id": "BTC-USD",
            "updates": [{
                "side": "bid",
                "event_time": "2023-02-09T20:32:49.000000Z",
                "price_level": "21900.00",
                "new_quantity": "1.00"
            }]
        }]
    });
    let _ = ws.broadcast_text(old_frame.to_string());

    tokio::time::sleep(Duration::from_millis(20)).await;

    // No new events should have been processed
    assert_eq!(
        sink.events().len(),
        count_after_snap,
        "Duplicate and old sequences must be dropped"
    );

    // Send valid next sequence (seq 2)
    let upd_raw = load_fixture(UPDATE_FIXTURE).expect("load update");
    let _ = ws.broadcast_text(upd_raw);

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
    assert!(
        sink.events().len() > count_after_snap,
        "Valid next sequence must be processed"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_heartbeat_keeps_watchdog_alive() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.silence_timeout = Duration::from_millis(600);

    let symbols = vec![Symbol::from("BTC-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    // Send heartbeats every 150ms for 750ms total (> 600ms silence timeout)
    for i in 1..=5 {
        let hb = serde_json::json!({
            "channel": "heartbeats",
            "sequence_num": i,
            "timestamp": "2023-02-09T20:32:50.000000Z",
            "events": [{
                "current_time": "2023-02-09T20:32:50.000000Z",
                "heartbeat_counter": i
            }]
        });
        let _ = ws.broadcast_text(hb.to_string());
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // Must still have exactly 1 connection (no watchdog reconnect)
    assert_eq!(ws.connection_count(), 1);

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_market_trades_channel() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.streams.market_trades = true;

    let symbols = vec![Symbol::from("BTC-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    // Broadcast trade fixture
    let trade_raw = load_fixture(TRADE_FIXTURE).expect("load trade");
    let _ = ws.broadcast_text(trade_raw);

    sink.wait_for_trades(1).await;

    let events = sink.events();
    let trade_opt = events.iter().find_map(|e| match e {
        RecordedEvent::Trade { symbol, trade } if symbol.as_str() == "BTC-USD" => {
            Some(trade.clone())
        }
        _ => None,
    });

    assert!(trade_opt.is_some(), "Expected Trade event");
    let trade = trade_opt.unwrap();
    assert_eq!(trade.price, Decimal::from_str_exact("21925.00").unwrap());
    assert_eq!(trade.volume, Decimal::from_str_exact("0.001").unwrap());
    assert_eq!(trade.aggressor, Side::Ask);

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_operator_commands_resync_and_pause() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.resync_timeout = Duration::from_millis(10);

    let symbols = vec![Symbol::from("BTC-USD"), Symbol::from("ETH-USD")];
    let sink = RecordingSink::new();

    let (connector, handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    sink.wait_for_alerts(2).await;

    // Operator: request_resync for BTC-USD
    handle
        .request_resync(Symbol::from("BTC-USD"))
        .await
        .expect("resync single");
    sink.wait_for_alerts(3).await;

    let btc_alerts = sink.alerts_for("BTC-USD");
    assert!(
        btc_alerts
            .iter()
            .filter(|a| matches!(a, IntegrityAlert::ResyncStarted))
            .count()
            >= 2,
        "Expected multiple ResyncStarted for BTC-USD after operator resync"
    );

    // Operator: request_resync_all
    handle.request_resync_all().await.expect("resync all");
    sink.wait_for_alerts(5).await;

    let eth_alerts = sink.alerts_for("ETH-USD");
    assert!(
        eth_alerts
            .iter()
            .filter(|a| matches!(a, IntegrityAlert::ResyncStarted))
            .count()
            >= 2,
        "Expected multiple ResyncStarted for ETH-USD after operator resync_all"
    );

    // Operator: pause and resume
    handle.pause().await.expect("pause");
    sink.wait_for_alert_matching(|a| {
        matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::Paused
            }
        )
    })
    .await;

    let pre_pause_count = sink.events().len();
    let upd_raw = load_fixture(UPDATE_FIXTURE).expect("load update");
    let _ = ws.broadcast_text(upd_raw.clone());
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(
        sink.events().len(),
        pre_pause_count,
        "No events should be processed while paused"
    );

    handle.resume().await.expect("resume");
    ws.wait_for_connections(2).await;

    // Send update with sequence 10
    let upd_seq10 = serde_json::json!({
        "channel": "l2_data",
        "sequence_num": 10,
        "timestamp": "2023-02-09T20:32:50.000000Z",
        "events": [{
            "type": "snapshot",
            "product_id": "BTC-USD",
            "updates": [{
                "side": "bid",
                "event_time": "2023-02-09T20:32:50.000000Z",
                "price_level": "21900.00",
                "new_quantity": "1.00"
            }]
        }]
    });
    let _ = ws.broadcast_text(upd_seq10.to_string());
    sink.wait_for_event_matching(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        )
    })
    .await;

    assert!(
        sink.events().len() > pre_pause_count,
        "Events must be processed after resume"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_cancellation_emits_shutdown_alert() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC-USD"), Symbol::from("ETH-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_connections(1).await;
    ws.wait_for_client_messages(1).await;

    // Trigger cancellation
    cancel.cancel();
    let res = runner.await.expect("task join");
    assert!(res.is_ok());

    let btc_alerts = sink.alerts_for("BTC-USD");
    assert!(
        btc_alerts.iter().any(|a| matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::Shutdown
            }
        )),
        "Expected ConnectionLost(Shutdown) for BTC-USD"
    );

    let eth_alerts = sink.alerts_for("ETH-USD");
    assert!(
        eth_alerts.iter().any(|a| matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::Shutdown
            }
        )),
        "Expected ConnectionLost(Shutdown) for ETH-USD"
    );
}

#[tokio::test]
async fn test_ws_sequenceless_l2_data_fails_closed() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    // Remove sequence_num from update payload
    let mut bad_upd: serde_json::Value =
        serde_json::from_str(&upd_json).expect("valid update json");
    bad_upd.as_object_mut().unwrap().remove("sequence_num");
    let bad_upd_str = bad_upd.to_string();

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    assert_eq!(config.sequence_policy, mdc_core::SequencePolicy::Required);

    let symbols = vec![Symbol::from("BTC-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    // Send valid snapshot -> InSync
    let _ = ws.broadcast_text(&snap_json);
    sink.wait_for_event_matching(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        )
    })
    .await;

    let books_after_snap: Vec<_> = sink
        .events()
        .into_iter()
        .filter_map(|e| match e {
            RecordedEvent::BeginBook { symbol, kind } if symbol.as_str() == "BTC-USD" => Some(kind),
            _ => None,
        })
        .collect();
    assert_eq!(books_after_snap.len(), 1, "Snapshot must be applied");
    assert_eq!(books_after_snap[0], BookKind::Snapshot);

    // Send sequence-less l2_data frame -> must fail closed
    let _ = ws.broadcast_text(&bad_upd_str);
    sink.wait_for_alerts(3).await;

    // Verify: no book delta was emitted
    let books_after_update: Vec<_> = sink
        .events()
        .into_iter()
        .filter_map(|e| match e {
            RecordedEvent::BeginBook { symbol, kind } if symbol.as_str() == "BTC-USD" => Some(kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        books_after_update.len(),
        1,
        "No book delta may be emitted when sequence_num is absent under Required policy"
    );

    // Verify: alerts contain ResyncStarted
    let alerts = sink.alerts_for("BTC-USD");
    let has_resync = alerts
        .iter()
        .any(|a| matches!(a, IntegrityAlert::ResyncStarted));
    assert!(
        has_resync,
        "Must emit ResyncStarted alert for absent sequence"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_multi_event_envelope_dispatches_per_event() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");

    let symbols = vec![Symbol::from("BTC-USD"), Symbol::from("ETH-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    // Send an envelope containing two snapshots: BTC-USD and ETH-USD
    let multi_event_payload = serde_json::json!({
        "channel": "l2_data",
        "client_id": "",
        "timestamp": "2023-06-23T00:00:00.123456Z",
        "sequence_num": 1,
        "events": [
            {
                "type": "snapshot",
                "product_id": "BTC-USD",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2023-06-23T00:00:00.123456Z",
                        "price_level": "30000.00",
                        "new_quantity": "1.50"
                    },
                    {
                        "side": "offer",
                        "event_time": "2023-06-23T00:00:00.123456Z",
                        "price_level": "30005.00",
                        "new_quantity": "2.00"
                    }
                ]
            },
            {
                "type": "snapshot",
                "product_id": "ETH-USD",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2023-06-23T00:00:00.123456Z",
                        "price_level": "1900.00",
                        "new_quantity": "10.00"
                    },
                    {
                        "side": "offer",
                        "event_time": "2023-06-23T00:00:00.123456Z",
                        "price_level": "1905.00",
                        "new_quantity": "12.00"
                    }
                ]
            }
        ]
    });

    let _ = ws.broadcast_text(multi_event_payload.to_string());
    sink.wait_for_event_matching(|e| {
        matches!(
            e,
            RecordedEvent::EndBook { symbol, .. } if symbol.as_str() == "ETH-USD"
        )
    })
    .await;

    // Verify: separate books emitted for BTC and ETH
    let events = sink.events();
    let mut btc_levels = Vec::new();
    let mut eth_levels = Vec::new();
    let mut current_sym: Option<Symbol> = None;

    for e in events {
        match e {
            RecordedEvent::BeginBook { symbol, .. } => {
                current_sym = Some(symbol);
            }
            RecordedEvent::Level {
                side,
                price,
                volume,
            } => {
                if let Some(ref sym) = current_sym {
                    if sym.as_str() == "BTC-USD" {
                        btc_levels.push((side, price, volume));
                    } else if sym.as_str() == "ETH-USD" {
                        eth_levels.push((side, price, volume));
                    }
                }
            }
            RecordedEvent::EndBook { .. } => {
                current_sym = None;
            }
            _ => {}
        }
    }

    assert_eq!(
        btc_levels,
        vec![
            (
                Side::Bid,
                Decimal::from(30000),
                Decimal::from_str_exact("1.50").unwrap()
            ),
            (
                Side::Ask,
                Decimal::from(30005),
                Decimal::from_str_exact("2.00").unwrap()
            ),
        ],
        "BTC-USD must contain only BTC levels"
    );

    assert_eq!(
        eth_levels,
        vec![
            (
                Side::Bid,
                Decimal::from(1900),
                Decimal::from_str_exact("10.00").unwrap()
            ),
            (
                Side::Ask,
                Decimal::from(1905),
                Decimal::from_str_exact("12.00").unwrap()
            ),
        ],
        "ETH-USD must contain only ETH levels"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_sequence_reset_recovery_one_alert_per_episode() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = CoinbaseAdvancedConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.reset_threshold = 2;

    let symbols = vec![Symbol::from("BTC-USD")];
    let sink = RecordingSink::new();

    let (connector, _handle) = CoinbaseAdvancedConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    // Send initial snapshot with sequence 100
    let initial_snap = r#"{
        "channel": "l2_data",
        "client_id": "",
        "timestamp": "2023-02-09T20:32:50.000000000Z",
        "sequence_num": 100,
        "events": [
            {
                "type": "snapshot",
                "product_id": "BTC-USD",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2023-02-09T20:32:50.000000000Z",
                        "price_level": "21900.00",
                        "new_quantity": "1.00000000"
                    }
                ]
            }
        ]
    }"#;
    let _ = ws.broadcast_text(initial_snap.to_string());
    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    assert!(
        sink.alerts_for("BTC-USD")
            .iter()
            .any(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. })),
        "Must be InSync after snapshot"
    );

    // Inject sequence reset: candidate 1 at sequence 10
    let reset_frame_1 = r#"{
        "channel": "l2_data",
        "client_id": "",
        "timestamp": "2023-02-09T20:32:51.000000000Z",
        "sequence_num": 10,
        "events": [
            {
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2023-02-09T20:32:51.000000000Z",
                        "price_level": "21900.00",
                        "new_quantity": "1.50000000"
                    }
                ]
            }
        ]
    }"#;
    let _ = ws.broadcast_text(reset_frame_1.to_string());

    // Reset frame 2 at sequence 11: reaches threshold 2! Triggers SequenceReset and forces reconnect
    let reset_frame_2 = r#"{
        "channel": "l2_data",
        "client_id": "",
        "timestamp": "2023-02-09T20:32:52.000000000Z",
        "sequence_num": 11,
        "events": [
            {
                "type": "update",
                "product_id": "BTC-USD",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2023-02-09T20:32:52.000000000Z",
                        "price_level": "21900.00",
                        "new_quantity": "2.00000000"
                    }
                ]
            }
        ]
    }"#;
    let _ = ws.broadcast_text(reset_frame_2.to_string());

    // Wait for frame_loop exit, reconnect backoff and second connection handshake
    ws.wait_for_connections(2).await;
    ws.wait_for_client_messages(4).await;

    // Connection reconnects. Send fresh snapshot on the new epoch (sequence 12)
    let recovery_snap = r#"{
        "channel": "l2_data",
        "client_id": "",
        "timestamp": "2023-02-09T20:32:53.000000000Z",
        "sequence_num": 12,
        "events": [
            {
                "type": "snapshot",
                "product_id": "BTC-USD",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2023-02-09T20:32:53.000000000Z",
                        "price_level": "21900.00",
                        "new_quantity": "3.00000000"
                    }
                ]
            }
        ]
    }"#;
    let _ = ws.broadcast_text(recovery_snap.to_string());
    sink.wait_for_alerts(6).await;

    let alerts = sink.alerts_for("BTC-USD");
    let started = alerts
        .iter()
        .filter(|a| matches!(a, IntegrityAlert::ResyncStarted))
        .count();
    assert_eq!(
        started, 2,
        "one ResyncStarted for the initial sync and exactly one for the reset episode, got: {alerts:?}"
    );
    let gap_alerts: Vec<_> = alerts
        .iter()
        .filter(|a| {
            matches!(
                a,
                IntegrityAlert::GapDetected {
                    expected: 101,
                    received: 11
                }
            )
        })
        .collect();

    assert_eq!(
        gap_alerts.len(),
        1,
        "Exactly one GapDetected alert for the reset must be emitted per episode, got: {gap_alerts:?}"
    );

    // Assert that books resynchronized after recovery snapshot
    let completed_alerts: Vec<_> = alerts
        .iter()
        .filter(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .collect();
    assert_eq!(
        completed_alerts.len(),
        2,
        "Expected 2 ResyncCompleted alerts (initial + post-reset recovery)"
    );

    cancel.cancel();
    let _ = runner.await;
}
