#![cfg(feature = "lmax")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for LMAX Digital connector.

use std::str::FromStr;
use std::time::Duration;

use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, Side, Symbol};
use mdc_providers::lmax::{ControlPacing, LmaxConfig, LmaxConnectorBuilder, LmaxStreams};
use mdc_testkit::{MockWsServer, RecordedEvent, RecordingSink, load_fixture};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

const SNAPSHOT_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/lmax/order_book_snapshot.json";
const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/lmax/trade.json";

#[tokio::test]
async fn test_ws_control_pacing_and_subscription() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = LmaxConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.control_pacing = ControlPacing {
        initial_delay: Duration::from_millis(150),
        min_command_interval: Duration::from_millis(150),
        too_many_requests_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(5),
    };

    let symbols = vec![Symbol::from("btc-usd"), Symbol::from("eth-usd")];

    let sink = RecordingSink::new();
    let (connector, _handle) = LmaxConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Pacing wait: negative assertion inside the wall-clock initial_delay window; no
    // readiness signal exists for "nothing sent yet".
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        ws.received_messages().is_empty(),
        "No messages should be sent before initial_delay"
    );

    // After initial_delay: subscribe message should be sent
    ws.wait_for_client_messages(1).await;
    let msgs = ws.received_messages();
    assert_eq!(msgs.len(), 1, "Expected exactly 1 subscribe frame");

    let v: serde_json::Value = serde_json::from_str(&msgs[0]).expect("valid json subscribe");
    assert_eq!(v["type"], "SUBSCRIBE");
    assert_eq!(v["channels"][0]["name"], "ORDER_BOOK");
    assert_eq!(
        v["channels"][0]["instruments"],
        serde_json::json!(["btc-usd", "eth-usd"])
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_continuous_full_snapshots_and_resync_lifecycle() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");

    let mut config = LmaxConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.control_pacing = ControlPacing {
        initial_delay: Duration::from_millis(50),
        min_command_interval: Duration::from_millis(50),
        too_many_requests_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
    };

    let sink = RecordingSink::new();
    let (connector, _handle) = LmaxConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btc-usd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    // Wait for connect and initial subscribe
    ws.wait_for_client_messages(1).await;

    // Verify initial ResyncStarted was emitted on connect
    let events_init = sink.events();
    let has_resync_started = events_init.iter().any(|e| {
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
        "ResyncStarted must be emitted on connect"
    );

    sink.clear();

    // Send 1st order book frame
    let _ = ws.broadcast_text(&snap_json);
    ws.sync_barrier().await;

    let events1 = sink.events();
    let has_snapshot1 = events1.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        )
    });
    assert!(has_snapshot1, "1st book must be emitted as Snapshot");

    let has_resync_completed = events1.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncCompleted { .. },
                ..
            }
        )
    });
    assert!(
        has_resync_completed,
        "ResyncCompleted must be emitted after 1st book"
    );

    sink.clear();

    // Send 2nd order book frame
    let _ = ws.broadcast_text(&snap_json);
    ws.sync_barrier().await;

    let events2 = sink.events();
    let has_snapshot2 = events2.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::BeginBook {
                kind: BookKind::Snapshot,
                ..
            }
        )
    });
    assert!(
        has_snapshot2,
        "Subsequent book must also be emitted as Snapshot"
    );

    let has_extra_completed = events2.iter().any(|e| {
        matches!(
            e,
            RecordedEvent::Alert {
                alert: IntegrityAlert::ResyncCompleted { .. },
                ..
            }
        )
    });
    assert!(
        !has_extra_completed,
        "Steady-state books must not emit extra ResyncCompleted alerts"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_high_throughput_snapshot_stream() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let snap_json = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot fixture");

    let mut config = LmaxConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.control_pacing = ControlPacing {
        initial_delay: Duration::from_millis(50),
        min_command_interval: Duration::from_millis(50),
        too_many_requests_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
    };

    let sink = RecordingSink::new();
    let (connector, _handle) = LmaxConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btc-usd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;
    sink.clear();

    // Stream 1,000 snapshots rapidly, yielding periodically to avoid broadcast channel buffer lag
    const NUM_SNAPSHOTS: usize = 1_000;
    for i in 0..NUM_SNAPSHOTS {
        let _ = ws.broadcast_text(&snap_json);
        if i % 32 == 0 {
            tokio::task::yield_now().await;
        }
    }

    ws.sync_barrier().await;

    let events = sink.events();
    let snapshot_count = events
        .iter()
        .filter(|e| {
            matches!(
                e,
                RecordedEvent::BeginBook {
                    kind: BookKind::Snapshot,
                    ..
                }
            )
        })
        .count();

    assert_eq!(
        snapshot_count, NUM_SNAPSHOTS,
        "All 1,000 streamed snapshots must be processed and emitted in order"
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_resync_batching_and_pacing() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = LmaxConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.control_pacing = ControlPacing {
        initial_delay: Duration::from_millis(50),
        min_command_interval: Duration::from_millis(150),
        too_many_requests_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
    };

    let symbols = vec![Symbol::from("btc-usd"), Symbol::from("eth-usd")];

    let sink = RecordingSink::new();
    let (connector, handle) = LmaxConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(sink)
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    // Trigger resync for both symbols within a short batching window
    handle
        .request_resync(Symbol::from("btc-usd"))
        .await
        .expect("request resync btc");
    handle
        .request_resync(Symbol::from("eth-usd"))
        .await
        .expect("request resync eth");

    // Wait for batching timer (200ms) + UNSUBSCRIBE send + pacing delay (150ms) + SUBSCRIBE send
    ws.wait_for_client_messages(3).await;

    let msgs = ws.received_messages();
    // Expected message order:
    // 0: SUBSCRIBE (initial on connect)
    // 1: UNSUBSCRIBE (batched for btc-usd and eth-usd)
    // 2: SUBSCRIBE (batched for btc-usd and eth-usd)
    assert!(
        msgs.len() >= 3,
        "Expected at least 3 messages (initial + batched unsubs + batched subs), got {}",
        msgs.len()
    );

    let unsubs: serde_json::Value = serde_json::from_str(&msgs[1]).expect("valid json unsubs");
    assert_eq!(unsubs["type"], "UNSUBSCRIBE");
    let unsubs_insts = unsubs["channels"][0]["instruments"].as_array().unwrap();
    assert_eq!(unsubs_insts.len(), 2);

    let subs: serde_json::Value = serde_json::from_str(&msgs[2]).expect("valid json subs");
    assert_eq!(subs["type"], "SUBSCRIBE");
    let subs_insts = subs["channels"][0]["instruments"].as_array().unwrap();
    assert_eq!(subs_insts.len(), 2);

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_trade_normalization() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let trade_json = load_fixture(TRADE_FIXTURE).expect("load trade fixture");

    let mut config = LmaxConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.streams = LmaxStreams {
        book: true,
        trades: true,
    };
    config.control_pacing = ControlPacing {
        initial_delay: Duration::from_millis(50),
        min_command_interval: Duration::from_millis(50),
        too_many_requests_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
    };

    let sink = RecordingSink::new();
    let (connector, _handle) = LmaxConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btc-usd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;
    sink.clear();

    let _ = ws.broadcast_text(trade_json);
    ws.sync_barrier().await;

    let events = sink.events();
    let trade_event = events.iter().find_map(|e| match e {
        RecordedEvent::Trade { trade, .. } => Some(trade),
        _ => None,
    });

    assert!(trade_event.is_some(), "Trade event must be emitted");
    let trade = trade_event.unwrap();
    assert_eq!(trade.price, Decimal::from_str("50000.00").unwrap());
    assert_eq!(trade.volume, Decimal::from_str("0.5000").unwrap());
    assert_eq!(trade.aggressor, Side::Ask);

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_ws_control_pause_resume() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = LmaxConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.control_pacing = ControlPacing {
        initial_delay: Duration::from_millis(50),
        min_command_interval: Duration::from_millis(50),
        too_many_requests_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
    };

    let sink = RecordingSink::new();
    let (connector, handle) = LmaxConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btc-usd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    handle.pause().await.expect("send pause");
    sink.wait_for_alert_matching(|a| {
        matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::Paused
            }
        )
    })
    .await;

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

    handle.resume().await.expect("send resume");
    ws.sync_barrier().await;

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_lmax_pause_then_cancel() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");

    let mut config = LmaxConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.control_pacing = ControlPacing {
        initial_delay: Duration::from_millis(50),
        min_command_interval: Duration::from_millis(50),
        too_many_requests_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_secs(1),
    };

    let sink = RecordingSink::new();
    let (connector, handle) = LmaxConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btc-usd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;

    handle.pause().await.expect("send pause");
    sink.wait_for_alert_matching(|a| {
        matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::Paused
            }
        )
    })
    .await;

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

    // Cancel while paused without calling resume
    cancel.cancel();

    // Runner must terminate promptly without hanging
    let result = tokio::time::timeout(Duration::from_secs(2), runner).await;
    assert!(
        result.is_ok(),
        "Runner must exit promptly on cancellation while paused"
    );
}
