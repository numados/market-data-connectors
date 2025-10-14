#![cfg(feature = "binance")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Binance connector.

use std::time::Duration;

use mdc_core::{IntegrityAlert, Symbol};
use mdc_providers::binance::{BinanceConfig, BinanceConnectorBuilder};
use mdc_testkit::{MockRest, MockWsServer, RecordedEvent, RecordingSink, WsAction, load_fixture};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn test_ws_handshake_and_stream_subscription() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");

    let sink = RecordingSink::new();
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(60)).await;

    let paths = ws.handshake_paths();
    assert_eq!(paths.len(), 1);
    assert_eq!(
        paths[0],
        "/stream?streams=btcusdt@depth@100ms/btcusdt@bookTicker"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_silence_watchdog_ping() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.watchdog_ping_after = Duration::from_millis(30);
    config.watchdog_reconnect_after = Duration::from_millis(200);

    let sink = RecordingSink::new();
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait past ping_after (30ms)
    tokio::time::sleep(Duration::from_millis(60)).await;

    // The watchdog should have sent at least one ping
    assert!(
        ws.ping_count() >= 1,
        "expected at least 1 ping, got {}",
        ws.ping_count()
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_silence_watchdog_forces_reconnect() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.watchdog_ping_after = Duration::from_millis(20);
    config.watchdog_reconnect_after = Duration::from_millis(50);
    config.initial_backoff = Duration::from_millis(20);

    let sink = RecordingSink::new();
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait past reconnect_after (50ms) + backoff (20ms) + connect (30ms)
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert!(
        ws.connection_count() >= 2,
        "expected >= 2 connections, got {}",
        ws.connection_count()
    );

    // Verify SilenceTimeout and ConnectionLost alerts were recorded
    let alerts = sink.alerts();
    assert!(
        alerts.iter().any(
            |a| matches!(a, IntegrityAlert::SilenceTimeout { elapsed_ms } if *elapsed_ms >= 50)
        ),
        "expected SilenceTimeout alert, got alerts: {alerts:?}"
    );
    assert!(
        alerts.iter().any(|a| matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: mdc_core::DisconnectKind::SilenceTimeout
            }
        )),
        "expected ConnectionLost(SilenceTimeout), got alerts: {alerts:?}"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_server_close_reconnects() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    // First connection sends a close frame immediately
    ws.push_script(vec![WsAction::Close(Some(1000))]);

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.initial_backoff = Duration::from_millis(20);

    let sink = RecordingSink::new();
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for first close and second connect
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(ws.connection_count() >= 2, "reconnected after close");

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_control_pause_resume_cancel() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot_json =
        load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
            .expect("load snapshot fixture");
    rest.mock_binance_depth_ok("BTCUSDT", &snapshot_json).await;

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.initial_backoff = Duration::from_millis(20);

    let sink = RecordingSink::new();
    let (connector, handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Pause connector
    handle.pause().await.expect("pause connector");
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Broadcast a ticker while paused
    let ticker_json = r#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25000.00","B":"1.20","a":"25000.10","A":"2.40"}}"#;
    ws.broadcast_text(ticker_json);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify no ticker was emitted while paused
    let events_during_pause = sink.events();
    let ticker_count = events_during_pause
        .iter()
        .filter(|e| matches!(e, RecordedEvent::BookTicker { .. }))
        .count();
    assert_eq!(ticker_count, 0, "no tickers emitted while paused");

    // Resume connector
    handle.resume().await.expect("resume connector");
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Broadcast another ticker while resumed
    ws.broadcast_text(ticker_json);
    tokio::time::sleep(Duration::from_millis(60)).await;

    let events_after_resume = sink.events();
    let ticker_count = events_after_resume
        .iter()
        .filter(|e| matches!(e, RecordedEvent::BookTicker { .. }))
        .count();
    assert_eq!(ticker_count, 1, "ticker emitted after resume");

    // Cancel and verify clean termination
    cancel.cancel();
    let res = tokio::time::timeout(Duration::from_millis(200), runner).await;
    assert!(res.is_ok(), "task completed upon cancellation");
}

#[tokio::test]
async fn test_ws_gap_injection_triggers_resync() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    // Snapshot with lastUpdateId = 100
    let snapshot_json =
        r#"{"lastUpdateId":100,"bids":[["25000.00","1.0"]],"asks":[["25001.00","2.0"]]}"#;
    rest.mock_binance_depth_ok("BTCUSDT", snapshot_json).await;

    // Depth update 1: U=95, u=105 (bridges snapshot 100)
    let d1 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1600000000000,"s":"BTCUSDT","U":95,"u":105,"b":[["25000.00","1.5"]],"a":[["25001.00","2.5"]]}}"#;
    // Depth update 2: U=106, u=110 (contiguous)
    let d2 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1600000000100,"s":"BTCUSDT","U":106,"u":110,"b":[["25000.00","1.6"]],"a":[["25001.00","2.6"]]}}"#;
    // Depth update 3: U=120, u=125 (GAP! expected 111, received 120)
    let d3 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1600000000200,"s":"BTCUSDT","U":120,"u":125,"b":[["25000.00","1.7"]],"a":[["25001.00","2.7"]]}}"#;

    ws.push_script(vec![
        WsAction::SendText(d1.to_string()),
        WsAction::Sleep(Duration::from_millis(50)),
        WsAction::SendText(d2.to_string()),
        WsAction::Sleep(Duration::from_millis(50)),
        WsAction::SendText(d3.to_string()),
    ]);

    let mut config = BinanceConfig::default();
    config.ws_base_url = ws.ws_url().expect("ws url");
    config.rest_base_url = rest.url().expect("rest url");
    config.initial_backoff = Duration::from_millis(20);

    let sink = RecordingSink::new();
    let (connector, _handle) = BinanceConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("BTCUSDT")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for all scripted updates and resync reconciliation to occur
    tokio::time::sleep(Duration::from_millis(250)).await;

    let alerts = sink.alerts_for("BTCUSDT");
    // Verify alert progression: ResyncStarted -> ResyncCompleted -> GapDetected -> ResyncStarted
    assert!(
        alerts.len() >= 3,
        "expected at least 3 alerts, got {alerts:?}"
    );
    assert_eq!(alerts[0], IntegrityAlert::ResyncStarted);
    assert_eq!(
        alerts[1],
        IntegrityAlert::ResyncCompleted {
            last_update_id: Some(100)
        }
    );
    assert!(
        matches!(
            alerts[2],
            IntegrityAlert::GapDetected {
                expected: 111,
                received: 120
            }
        ),
        "expected GapDetected with 111/120, got {:?}",
        alerts[2]
    );
    assert_eq!(alerts[3], IntegrityAlert::ResyncStarted);

    cancel.cancel();
    let _ = runner.await;
}
