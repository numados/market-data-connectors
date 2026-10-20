#![cfg(feature = "bitstamp")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Level 4 Mock WebSocket integration tests for Bitstamp connector.

use std::str::FromStr;
use std::time::Duration;

use mdc_core::{BookKind, DisconnectKind, IntegrityAlert, Side, SourceErrorKind, Symbol};
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

    ws.wait_for_client_messages(2).await;

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

    ws.wait_for_client_messages(2).await;

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
    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

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

    let _ = ws.broadcast_text(stale_diff);
    ws.sync_barrier().await;

    assert!(
        sink.events().is_empty(),
        "Diff with microtimestamp <= snapshot time must be dropped"
    );

    // 2. Send a newer diff (microtimestamp 1700000000123456 > snapshot 1700000000100000)
    let _ = ws.broadcast_text(upd_json);
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

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    // Send bts:request_reconnect
    let _ = ws.broadcast_text(reconnect_json);
    sink.wait_for_alert_matching(|a| {
        matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::VenueRequested,
            }
        )
    })
    .await;

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

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    sink.clear();

    // Send trade frame
    let _ = ws.broadcast_text(trade_json);
    sink.wait_for_trades(1).await;

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

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    // Send Pause command
    handle.pause().await.expect("send pause");
    sink.wait_for_alert_matching(|a| {
        matches!(
            a,
            IntegrityAlert::ConnectionLost {
                reason: DisconnectKind::Paused,
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

    // Send Resume command
    handle.resume().await.expect("send resume");
    ws.sync_barrier().await;

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

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    sink.clear();

    // Trigger explicit resync
    handle
        .request_resync(Symbol::from("btcusd"))
        .await
        .expect("request resync");

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

fn bitstamp_diff(microtimestamp: u64, bid: &str) -> String {
    serde_json::json!({
        "event": "data",
        "channel": "diff_order_book_btcusd",
        "data": {
            "timestamp": "1700000000",
            "microtimestamp": microtimestamp.to_string(),
            "bids": [[bid, "1.00000000"]],
            "asks": []
        }
    })
    .to_string()
}

/// Buffer overflow while a REST snapshot is in flight must orphan that
/// request: its late response is discarded and a fresh snapshot completes.
#[tokio::test]
async fn test_overflow_discards_in_flight_snapshot_and_requests_fresh_one() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let stale = r#"{"timestamp":"1700000000","microtimestamp":"1700000000100000","bids":[["40000.00","1.0"]],"asks":[["40001.00","1.0"]]}"#;
    let fresh = r#"{"timestamp":"1700000001","microtimestamp":"1700000000900000","bids":[["50000.00","1.0"]],"asks":[["50001.00","1.0"]]}"#;
    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(stale, "application/json")
                .set_delay(Duration::from_millis(300)),
        )
        .up_to_n_times(1)
        .mount(rest.inner())
        .await;
    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(fresh, "application/json"))
        .mount(rest.inner())
        .await;

    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().expect("ws url");
    config.rest_url = rest.url().expect("valid rest url");
    config.buffer_capacity = 1;

    let sink = RecordingSink::new();
    let (connector, _handle) = BitstampConnectorBuilder::new(config)
        .symbols(vec![Symbol::from("btcusd")])
        .streaming_sink(sink.clone())
        .build()
        .expect("build connector");

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(1).await;
    // First diff is buffered and triggers the (slow) snapshot request.
    let _ = ws.broadcast_text(bitstamp_diff(1_700_000_000_200_000, "1.00"));
    rest.wait_for_requests(1).await;
    // Second diff overflows capacity 1 while the request is in flight.
    let _ = ws.broadcast_text(bitstamp_diff(1_700_000_000_300_000, "2.00"));

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    assert_eq!(rest.wait_for_requests(2).await, 2, "one fresh request");

    let events = sink.events();
    let snapshot_bids: Vec<Decimal> = events
        .iter()
        .filter_map(|e| match e {
            RecordedEvent::Level {
                side: Side::Bid,
                price,
                ..
            } => Some(*price),
            _ => None,
        })
        .collect();
    assert_eq!(
        snapshot_bids,
        vec![Decimal::from_str("50000.00").unwrap()],
        "only the fresh snapshot may be published"
    );
    let completed = sink
        .alerts()
        .into_iter()
        .filter(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .count();
    assert_eq!(completed, 1);

    cancel.cancel();
    let _ = runner.await;
}

/// Counts the snapshot (REST) requests received so far.
async fn snapshot_requests(rest: &MockRest) -> usize {
    rest.inner()
        .received_requests()
        .await
        .expect("request recording enabled")
        .len()
}

fn event_summary(events: &[RecordedEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            RecordedEvent::BeginBook { kind, .. } => Some(format!("{kind:?}")),
            RecordedEvent::Level { price, .. } => Some(format!("level {price}")),
            _ => None,
        })
        .collect()
}

/// Diffs that arrive while the snapshot request is slow are buffered, not published. When the
/// snapshot lands the stale diff is dropped and the newer ones replay in order after it; one REST
/// request serves the whole interleaving and nothing already published is superseded.
#[tokio::test]
async fn test_delayed_snapshot_interleaving_replays_buffered_diffs_in_order() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let snapshot = r#"{"timestamp":"1700000000","microtimestamp":"1700000000100000","bids":[["40000.00","1.0"]],"asks":[["40001.00","1.0"]]}"#;
    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(snapshot, "application/json")
                .set_delay(Duration::from_millis(300)),
        )
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

    ws.wait_for_client_messages(1).await;
    rest.wait_for_requests(1).await;

    // While the request is in flight: one diff older than the snapshot, then two newer ones.
    let _ = ws.broadcast_text(bitstamp_diff(1_700_000_000_050_000, "1.00"));
    let _ = ws.broadcast_text(bitstamp_diff(1_700_000_000_200_000, "2.00"));
    let _ = ws.broadcast_text(bitstamp_diff(1_700_000_000_300_000, "3.00"));
    ws.sync_barrier().await;
    assert!(
        sink.events()
            .iter()
            .all(|e| !matches!(e, RecordedEvent::BeginBook { .. })),
        "nothing may be published before the snapshot lands"
    );

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    // A live diff after synchronisation continues the same stream.
    let _ = ws.broadcast_text(bitstamp_diff(1_700_000_000_400_000, "4.00"));
    sink.wait_for_events(sink.len() + 1).await;
    ws.sync_barrier().await;

    assert_eq!(
        event_summary(&sink.events()),
        vec![
            "Snapshot",
            "level 40000.00",
            "level 40001.00",
            "Delta",
            "level 2.00",
            "Delta",
            "level 3.00",
            "Delta",
            "level 4.00",
        ],
        "snapshot first, stale diff dropped, buffered diffs replayed in order, then live"
    );
    assert_eq!(
        snapshot_requests(&rest).await,
        1,
        "exactly one snapshot request"
    );
    let started = sink
        .alerts()
        .into_iter()
        .filter(|a| matches!(a, IntegrityAlert::ResyncStarted))
        .count();
    assert_eq!(started, 1, "no second resync episode");

    cancel.cancel();
    let _ = runner.await;
}

/// An undecodable diff on a synchronised book invalidates it: nothing is published for the bad
/// frame, a parse error and a resync are announced, and a fresh snapshot restores the stream.
#[tokio::test]
async fn test_malformed_diff_invalidates_book_and_resyncs() {
    let ws = MockWsServer::spawn().await.expect("spawn mock ws");
    let rest = MockRest::start().await;

    let first = r#"{"timestamp":"1700000000","microtimestamp":"1700000000100000","bids":[["40000.00","1.0"]],"asks":[["40001.00","1.0"]]}"#;
    let second = r#"{"timestamp":"1700000001","microtimestamp":"1700000000900000","bids":[["50000.00","1.0"]],"asks":[["50001.00","1.0"]]}"#;
    for (body, once) in [(first, true), (second, false)] {
        let mock = Mock::given(method("GET"))
            .and(path("/order_book/btcusd/"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"));
        if once {
            mock.up_to_n_times(1).mount(rest.inner()).await;
        } else {
            mock.mount(rest.inner()).await;
        }
    }

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

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;
    sink.clear();

    let malformed = serde_json::json!({
        "event": "data",
        "channel": "diff_order_book_btcusd",
        "data": {
            "timestamp": "1700000000",
            "microtimestamp": "1700000000200000",
            "bids": [["not-a-price", "1.0"]],
            "asks": []
        }
    })
    .to_string();
    let _ = ws.broadcast_text(malformed);

    sink.wait_for_alert_matching(|a| matches!(a, IntegrityAlert::ResyncCompleted { .. }))
        .await;

    assert_eq!(
        sink.alerts(),
        vec![
            IntegrityAlert::SourceError {
                kind: SourceErrorKind::Parse
            },
            IntegrityAlert::ResyncStarted,
            IntegrityAlert::ResyncCompleted {
                last_update_id: None
            },
        ]
    );
    assert_eq!(
        event_summary(&sink.events()),
        vec!["Snapshot", "level 50000.00", "level 50001.00"],
        "the malformed diff publishes nothing; recovery comes from the fresh snapshot"
    );
    assert_eq!(snapshot_requests(&rest).await, 2, "one re-snapshot");

    // The stream continues from the fresh snapshot.
    let _ = ws.broadcast_text(bitstamp_diff(1_700_000_001_000_000, "51000.00"));
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

    cancel.cancel();
    let _ = runner.await;
}
