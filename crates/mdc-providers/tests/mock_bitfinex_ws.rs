#![cfg(feature = "bitfinex")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Bitfinex WebSocket v2 connector.

use std::time::Duration;

use mdc_core::BookKind;
use mdc_core::model::{IntegrityAlert, Side};
use mdc_providers::bitfinex::connector::BitfinexConnector;
use mdc_testkit::{MockWsServer, RecordedEvent, RecordingSink, load_fixture};
use tokio_util::sync::CancellationToken;

const CONF_OK_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/bitfinex/conf_ok.json";
const SUBSCRIBED_BOOK_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/bitfinex/subscribed_book.json";
const SNAPSHOT_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/bitfinex/order_book_snapshot.json";
const UPDATE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/bitfinex/depth_update.json";
const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/bitfinex/trade.json";

#[tokio::test]
async fn test_ws_handshake_conf_flags_and_subscribes() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let sink = RecordingSink::new();
    let connector = BitfinexConnector::builder()
        .ws_url(ws.ws_url().expect("ws url").to_string())
        .symbol("tBTCUSD")
        .enable_depth(true)
        .enable_trades(true)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let (_handle, runner) = connector.run(sink, cancel_child);
    let run_task = tokio::spawn(runner);

    tokio::time::sleep(Duration::from_millis(60)).await;

    let received = ws.received_messages();
    assert!(
        !received.is_empty(),
        "Mock WS should have received messages"
    );

    // Frame 0 must be conf negotiation with flags 163840
    let v0: serde_json::Value = serde_json::from_str(&received[0]).expect("valid json conf");
    assert_eq!(v0["event"], "conf");
    assert_eq!(v0["flags"], 163840);

    // Later frames should include book and trades subscriptions
    let has_book_sub = received.iter().any(|msg| {
        let v: serde_json::Value = serde_json::from_str(msg).unwrap_or_default();
        v["event"] == "subscribe" && v["channel"] == "book" && v["symbol"] == "tBTCUSD"
    });
    let has_trades_sub = received.iter().any(|msg| {
        let v: serde_json::Value = serde_json::from_str(msg).unwrap_or_default();
        v["event"] == "subscribe" && v["channel"] == "trades" && v["symbol"] == "tBTCUSD"
    });

    assert!(has_book_sub, "Connector must subscribe to book");
    assert!(has_trades_sub, "Connector must subscribe to trades");

    cancel.cancel();
    let _ = run_task.await;
}

#[tokio::test]
async fn test_ws_snapshot_and_delta_lifecycle() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let conf_ok = load_fixture(CONF_OK_FIXTURE).expect("load conf_ok fixture");
    let sub_book = load_fixture(SUBSCRIBED_BOOK_FIXTURE).expect("load subscribed_book fixture");
    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");
    let upd_json = load_fixture(UPDATE_FIXTURE).expect("load update fixture");

    let sink = RecordingSink::new();
    let connector = BitfinexConnector::builder()
        .ws_url(ws.ws_url().expect("ws url").to_string())
        .symbol("tBTCUSD")
        .enable_depth(true)
        .enable_trades(false)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let (_handle, runner) = connector.run(sink.clone(), cancel_child);
    let run_task = tokio::spawn(runner);

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send conf ack and book subscription confirmation
    ws.broadcast_text(&conf_ok);
    ws.broadcast_text(&sub_book);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send order book snapshot
    ws.broadcast_text(&snap_json);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send depth update
    ws.broadcast_text(&upd_json);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let events = sink.events();
    let mut has_resync_started = false;
    let mut has_snapshot = false;
    let mut has_resync_completed = false;
    let mut has_delta = false;

    for ev in &events {
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
    assert!(
        has_snapshot,
        "Must emit L2Snapshot from order_book_snapshot"
    );
    assert!(
        has_resync_completed,
        "Must emit ResyncCompleted after valid snapshot"
    );
    assert!(has_delta, "Must emit L2Delta from depth_update");

    // ResyncCompleted must appear strictly after EndBook of snapshot.
    let end_book_pos = events
        .iter()
        .position(|ev| matches!(ev, RecordedEvent::EndBook { .. }))
        .expect("snapshot EndBook must be present");
    let resync_completed_pos = events
        .iter()
        .position(|ev| {
            matches!(
                ev,
                RecordedEvent::Alert {
                    alert: IntegrityAlert::ResyncCompleted { .. },
                    ..
                }
            )
        })
        .expect("ResyncCompleted must be present");

    assert!(
        resync_completed_pos > end_book_pos,
        "ResyncCompleted (pos {resync_completed_pos}) must be emitted strictly after EndBook (pos {end_book_pos})"
    );

    // Verify all delivery stamps are strictly monotonic
    let stamps = sink.delivery_stamps();
    assert!(!stamps.is_empty());
    for w in stamps.windows(2) {
        if w[0].connection_generation == w[1].connection_generation {
            assert!(
                w[1].ordinal > w[0].ordinal,
                "Delivery ordinal must be strictly monotonic: {} <= {}",
                w[1].ordinal,
                w[0].ordinal
            );
        }
    }

    cancel.cancel();
    let _ = run_task.await;
}

#[tokio::test]
async fn test_ws_trade_execution_event() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let conf_ok = load_fixture(CONF_OK_FIXTURE).expect("load conf_ok");
    let trade_sub_ack = r#"{"event":"subscribed","channel":"trades","chanId":17470,"symbol":"tBTCUSD","pair":"BTCUSD"}"#;
    let trade_json = load_fixture(TRADE_FIXTURE).expect("load trade fixture");

    let sink = RecordingSink::new();
    let connector = BitfinexConnector::builder()
        .ws_url(ws.ws_url().expect("ws url").to_string())
        .symbol("tBTCUSD")
        .enable_depth(false)
        .enable_trades(true)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let (_handle, runner) = connector.run(sink.clone(), cancel_child);
    let run_task = tokio::spawn(runner);

    tokio::time::sleep(Duration::from_millis(60)).await;

    ws.broadcast_text(&conf_ok);
    ws.broadcast_text(trade_sub_ack);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send trade event
    ws.broadcast_text(&trade_json);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let trades: Vec<_> = sink
        .events()
        .into_iter()
        .filter_map(|ev| match ev {
            RecordedEvent::Trade { trade, .. } => Some(trade),
            _ => None,
        })
        .collect();
    assert!(!trades.is_empty(), "Expected normalized trade event");
    let t = &trades[0];
    assert_eq!(t.aggressor, Side::Bid);
    assert_eq!(t.identity.as_ref().unwrap().namespace.as_str(), "bitfinex");
    assert_eq!(t.identity.as_ref().unwrap().value.as_str(), "401597395");

    cancel.cancel();
    let _ = run_task.await;
}

#[tokio::test]
async fn test_ws_checksum_mismatch_triggers_in_place_resubscribe() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let conf_ok = load_fixture(CONF_OK_FIXTURE).expect("load conf_ok fixture");
    let sub_book = load_fixture(SUBSCRIBED_BOOK_FIXTURE).expect("load subscribed_book fixture");
    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");

    let sink = RecordingSink::new();
    let connector = BitfinexConnector::builder()
        .ws_url(ws.ws_url().expect("ws url").to_string())
        .symbol("tBTCUSD")
        .enable_depth(true)
        .enable_trades(false)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let (_handle, runner) = connector.run(sink.clone(), cancel_child);
    let run_task = tokio::spawn(runner);

    tokio::time::sleep(Duration::from_millis(60)).await;

    ws.broadcast_text(&conf_ok);
    ws.broadcast_text(&sub_book);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Enter InSync with snapshot
    ws.broadcast_text(&snap_json);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Broadcast checksum frame with bogus checksum value 999999999
    let bad_cs_msg = r#"[266343, "cs", 999999999, 1614604253634]"#;
    ws.broadcast_text(bad_cs_msg);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check alerts
    let alerts = sink.alerts_for("tBTCUSD");
    let has_mismatch = alerts.iter().any(|a| {
        matches!(
            a,
            IntegrityAlert::ChecksumMismatch {
                expected: 999999999,
                ..
            }
        )
    });
    assert!(has_mismatch, "Must emit ChecksumMismatch alert");

    // Check mock server received unsubscribe for 266343 and new subscribe
    let messages = ws.received_messages();
    let has_unsub = messages.iter().any(|m| {
        let v: Result<serde_json::Value, _> = serde_json::from_str(m);
        v.map(|j| j["event"] == "unsubscribe" && j["chanId"] == 266343)
            .unwrap_or(false)
    });
    let has_resub = messages.iter().skip(1).any(|m| {
        let v: Result<serde_json::Value, _> = serde_json::from_str(m);
        v.map(|j| j["event"] == "subscribe" && j["channel"] == "book" && j["symbol"] == "tBTCUSD")
            .unwrap_or(false)
    });

    assert!(
        has_unsub,
        "Must send in-place unsubscribe for chanId 266343"
    );
    assert!(has_resub, "Must send in-place resubscribe for tBTCUSD");

    cancel.cancel();
    let _ = run_task.await;
}

#[tokio::test]
async fn test_ws_channel_leak_guard_forces_reconnect() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let conf_ok = load_fixture(CONF_OK_FIXTURE).expect("load conf_ok fixture");

    let sink = RecordingSink::new();
    let connector = BitfinexConnector::builder()
        .ws_url(ws.ws_url().expect("ws url").to_string())
        .symbol("tBTCUSD") // Only 1 symbol configured
        .enable_depth(true)
        .enable_trades(false)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let (_handle, runner) = connector.run(sink, cancel_child);
    let run_task = tokio::spawn(runner);

    tokio::time::sleep(Duration::from_millis(60)).await;

    ws.broadcast_text(&conf_ok);

    // Send 3 distinct channel confirmations (> 2 * 1 symbols)
    let leak1 = r#"{"event":"subscribed","channel":"book","chanId":101,"symbol":"tBTCUSD"}"#;
    let leak2 = r#"{"event":"subscribed","channel":"book","chanId":102,"symbol":"tBTCUSD"}"#;
    let leak3 = r#"{"event":"subscribed","channel":"book","chanId":103,"symbol":"tBTCUSD"}"#;

    ws.broadcast_text(leak1);
    ws.broadcast_text(leak2);
    ws.broadcast_text(leak3);

    // The watchdog timer runs every 250ms and checks channel leak guard
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Leak guard breaks the message loop, causing reconnect.
    // Therefore connection_count on mock WS should increase to at least 2.
    assert!(
        ws.connection_count() >= 2,
        "Channel leak guard must force connection reconnect"
    );

    cancel.cancel();
    let _ = run_task.await;
}

#[tokio::test]
async fn test_ws_control_pause_resume_cancel() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let sink = RecordingSink::new();
    let connector = BitfinexConnector::builder()
        .ws_url(ws.ws_url().expect("ws url").to_string())
        .symbol("tBTCUSD")
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let (handle, runner) = connector.run(sink, cancel_child);
    let run_task = tokio::spawn(runner);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send Pause command
    handle.pause().await.expect("send pause");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send Resume command
    handle.resume().await.expect("send resume");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Cancel gracefully
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_millis(500), run_task).await;
    assert!(
        result.is_ok(),
        "Connector must shut down cleanly on cancellation"
    );
}

#[tokio::test]
async fn test_ws_silence_watchdog_ping_and_reconnect() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let sink = RecordingSink::new();
    let connector = BitfinexConnector::builder()
        .ws_url(ws.ws_url().expect("ws url").to_string())
        .symbol("tBTCUSD")
        .watchdog_ping_after(Duration::from_millis(60))
        .watchdog_reconnect_after(Duration::from_millis(100))
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let (_handle, runner) = connector.run(sink.clone(), cancel_child);
    let run_task = tokio::spawn(runner);

    // Initial connection
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(ws.connection_count(), 1);

    // Keep server silent past 60ms ping + 100ms grace period + timer intervals
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Silence watchdog should have triggered a reconnect
    assert!(
        ws.connection_count() >= 2,
        "Silence watchdog must trigger reconnect when no data frames arrive"
    );

    let alerts = sink.alerts();
    assert!(
        alerts.iter().any(
            |a| matches!(a, IntegrityAlert::SilenceTimeout { elapsed_ms } if *elapsed_ms >= 100)
        ),
        "Must emit SilenceTimeout alert with measured silence elapsed, got alerts: {alerts:?}"
    );
    assert!(
        alerts.iter().any(|a| matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: mdc_core::DisconnectKind::SilenceTimeout
            }
        )),
        "Must emit ConnectionLost(SilenceTimeout), got alerts: {alerts:?}"
    );

    cancel.cancel();
    let _ = run_task.await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_ws_current_thread_runtime_compatibility() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let sink = RecordingSink::new();
    let connector = BitfinexConnector::builder()
        .ws_url(ws.ws_url().expect("ws url").to_string())
        .symbol("tBTCUSD")
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let (_handle, runner) = connector.run(sink, cancel_child);

    // Spawn on current_thread
    let run_task = tokio::spawn(runner);
    tokio::time::sleep(Duration::from_millis(50)).await;

    cancel.cancel();
    let result = run_task.await.expect("task join");
    assert!(
        result.is_ok(),
        "Must run and cleanly terminate on current_thread runtime"
    );
}
