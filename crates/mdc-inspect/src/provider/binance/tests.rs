//! Binance adapter component tests against `MockWsServer` and `MockRest`:
//! the full subscribe → snapshot → deltas → gap → resync → recovered display
//! path, overflow, and cancellation through the real session runner.

#![cfg(feature = "binance")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use mdc_testkit::{MockRest, MockWsServer, WsAction};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

use crate::config::SessionConfig;
use crate::display::DisplayStatus;
use crate::provider::binance::BinanceAdapter;
use crate::provider::{ProviderKind, SessionBounds};
use crate::session::{SessionOutcome, run_session};

fn config() -> SessionConfig {
    SessionConfig::new(ProviderKind::Binance, &["BTCUSDT".to_string()]).unwrap()
}

fn bounds() -> SessionBounds {
    SessionBounds {
        max_symbols: 25,
        projector_depth: 1000,
        affected_connection: 1,
    }
}

/// Spawns the real session over the mock servers and returns the display
/// receiver plus the session-join handle.
async fn spawn_session(
    ws: &MockWsServer,
    rest: &MockRest,
    capacity: usize,
) -> (
    watch::Receiver<Arc<crate::display::DisplaySnapshot>>,
    tokio::task::JoinHandle<SessionOutcome>,
    CancellationToken,
) {
    spawn_session_with(ws, rest, capacity, Duration::from_millis(10)).await
}

async fn spawn_session_with(
    ws: &MockWsServer,
    rest: &MockRest,
    capacity: usize,
    resync_cooldown: Duration,
) -> (
    watch::Receiver<Arc<crate::display::DisplaySnapshot>>,
    tokio::task::JoinHandle<SessionOutcome>,
    CancellationToken,
) {
    let adapter = BinanceAdapter::with_test_timing(
        ws.ws_url().unwrap(),
        rest.url().unwrap(),
        resync_cooldown,
    );
    let root = CancellationToken::new();
    let session_root = root.clone();
    let (tx, rx) = watch::channel(Arc::new(crate::display::DisplaySnapshot {
        provider: "Binance Spot",
        presentation: Default::default(),
        session: DisplayStatus::Empty,
        failure: None,
        symbols: Vec::new(),
        alerts: Default::default(),
        alerts_evicted: 0,
    }));
    let handle = tokio::spawn(async move {
        run_session(
            Arc::new(adapter),
            config(),
            bounds(),
            capacity,
            session_root,
            tx,
        )
        .await
    });
    (rx, handle, root)
}

async fn wait_for_status(
    rx: &mut watch::Receiver<Arc<crate::display::DisplaySnapshot>>,
    status: DisplayStatus,
) -> Arc<crate::display::DisplaySnapshot> {
    wait_until(rx, |snapshot| {
        snapshot.session == status || snapshot.symbols.iter().any(|s| s.status == status)
    })
    .await
}

async fn wait_until(
    rx: &mut watch::Receiver<Arc<crate::display::DisplaySnapshot>>,
    predicate: impl Fn(&crate::display::DisplaySnapshot) -> bool,
) -> Arc<crate::display::DisplaySnapshot> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = rx.borrow_and_update().clone();
        if predicate(&snapshot) {
            return snapshot;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!(
                "timed out; last session={:?}, symbols={:?}, alerts={:?}",
                snapshot.session,
                snapshot
                    .symbols
                    .iter()
                    .map(|s| (s.symbol.as_str(), s.status, s.unavailability.clone()))
                    .collect::<Vec<_>>(),
                snapshot
                    .alerts
                    .iter()
                    .map(|a| a.summary.clone())
                    .collect::<Vec<_>>(),
            );
        }
        match tokio::time::timeout(remaining, rx.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => panic!("display channel closed while waiting: {error}"),
            Err(_) => panic!(
                "timed out; last session={:?}, symbols={:?}, alerts={:?}",
                snapshot.session,
                snapshot
                    .symbols
                    .iter()
                    .map(|s| (s.symbol.as_str(), s.status, s.unavailability.clone()))
                    .collect::<Vec<_>>(),
                snapshot
                    .alerts
                    .iter()
                    .map(|a| a.summary.clone())
                    .collect::<Vec<_>>(),
            ),
        }
    }
}

fn depth_response(last_update_id: u64, bid: &str, ask: &str) -> String {
    format!(
        r#"{{"lastUpdateId":{last_update_id},"bids":[["{bid}","1.0"]],"asks":[["{ask}","2.0"]]}}"#
    )
}

fn depth_response_levels(last_update_id: u64, bids: &str, asks: &str) -> String {
    format!(r#"{{"lastUpdateId":{last_update_id},"bids":[{bids}],"asks":[{asks}]}}"#)
}

fn depth_delta(first: u64, final_seq: u64, bid: &str, ask: &str) -> String {
    depth_delta_with_sizes(first, final_seq, bid, "1.5", ask, "2.5")
}

fn depth_delta_with_sizes(
    first: u64,
    final_seq: u64,
    bid: &str,
    bid_size: &str,
    ask: &str,
    ask_size: &str,
) -> String {
    format!(
        r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1600000000000,"s":"BTCUSDT","U":{first},"u":{final_seq},"b":[["{bid}","{bid_size}"]],"a":[["{ask}","{ask_size}"]]}}}}"#
    )
}

#[tokio::test]
async fn gap_window_is_unavailable_until_delayed_recovery_snapshot() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;

    let first = depth_response_levels(
        100,
        r#"["25000.00","1.0"],["24999.00","2.0"]"#,
        r#"["25001.00","2.0"],["25002.00","3.0"]"#,
    );
    let second = depth_response_levels(
        122,
        r#"["26000.00","1.0"],["25999.00","2.0"]"#,
        r#"["26001.00","2.0"],["26002.00","3.0"]"#,
    );
    Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(first, "application/json"))
        .up_to_n_times(1)
        .mount(rest.inner())
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(350))
                .set_body_raw(second, "application/json"),
        )
        .mount(rest.inner())
        .await;

    ws.push_script(Vec::new());
    let (mut rx, handle, root) = spawn_session_with(
        &ws,
        &rest,
        crate::config::CHANNEL_CAPACITY,
        Duration::from_millis(250),
    )
    .await;
    ws.wait_for_handshakes(1).await;
    let initial = wait_for_status(&mut rx, DisplayStatus::InSync).await;
    assert_eq!(
        initial.symbols[0]
            .quote
            .as_ref()
            .unwrap()
            .quote
            .bid
            .to_string(),
        "25000.00"
    );

    assert_eq!(
        ws.broadcast_text(depth_delta(101, 105, "25000.10", "25001.10"))
            .unwrap(),
        1
    );
    ws.sync_barrier().await;
    assert_eq!(
        ws.broadcast_text(depth_delta_with_sizes(
            106, 110, "24999.00", "3.0", "25002.00", "4.0",
        ))
        .unwrap(),
        1
    );
    ws.sync_barrier().await;
    assert_eq!(
        ws.broadcast_text(depth_delta(120, 125, "26000.10", "26001.10"))
            .unwrap(),
        1
    );

    // The request barrier proves that the delayed recovery response is in flight.
    rest.wait_for_requests(2).await;
    let gap_window = wait_until(&mut rx, |snapshot| {
        snapshot.symbols.iter().any(|symbol| {
            matches!(
                symbol.status,
                DisplayStatus::AwaitingSnapshot | DisplayStatus::Unavailable
            )
        })
    })
    .await;
    let gap_symbol = &gap_window.symbols[0];
    assert!(matches!(
        gap_symbol.status,
        DisplayStatus::AwaitingSnapshot | DisplayStatus::Unavailable
    ));
    let historical = gap_symbol.quote.as_ref().expect("retained pre-gap quote");
    assert!(historical.historical);
    assert_eq!(historical.quote.bid.to_string(), "25000.10");
    assert_eq!(historical.quote.ask.to_string(), "25001.00");
    assert_eq!(
        gap_symbol
            .bids
            .iter()
            .map(|level| (level.price.to_string(), level.size.to_string()))
            .collect::<Vec<_>>(),
        vec![
            ("25000.10".to_string(), "1.5".to_string()),
            ("25000.00".to_string(), "1.0".to_string()),
            ("24999.00".to_string(), "3.0".to_string()),
        ]
    );
    assert_eq!(
        gap_symbol
            .asks
            .iter()
            .map(|level| (level.price.to_string(), level.size.to_string()))
            .collect::<Vec<_>>(),
        vec![
            ("25001.00".to_string(), "2.0".to_string()),
            ("25001.10".to_string(), "2.5".to_string()),
            ("25002.00".to_string(), "4.0".to_string()),
        ]
    );

    let recovered = wait_for_status(&mut rx, DisplayStatus::InSync).await;
    let symbol = &recovered.symbols[0];
    let quote = symbol.quote.as_ref().expect("recovered quote");
    assert!(!quote.historical);
    assert_eq!(quote.quote.bid.to_string(), "26000.10");
    assert_eq!(quote.quote.ask.to_string(), "26001.00");
    assert_eq!(quote.quote.bid_size.to_string(), "1.5");
    assert_eq!(quote.quote.ask_size.to_string(), "2.0");
    assert_eq!(
        symbol
            .bids
            .iter()
            .map(|level| (level.price.to_string(), level.size.to_string()))
            .collect::<Vec<_>>(),
        vec![
            ("26000.10".to_string(), "1.5".to_string()),
            ("26000.00".to_string(), "1.0".to_string()),
            ("25999.00".to_string(), "2.0".to_string()),
        ]
    );
    assert_eq!(
        symbol
            .asks
            .iter()
            .map(|level| (level.price.to_string(), level.size.to_string()))
            .collect::<Vec<_>>(),
        vec![
            ("26001.00".to_string(), "2.0".to_string()),
            ("26001.10".to_string(), "2.5".to_string()),
            ("26002.00".to_string(), "3.0".to_string()),
        ]
    );

    let summaries: Vec<String> = recovered
        .alerts
        .iter()
        .map(|record| record.summary.clone())
        .collect();
    let gap_position = summaries
        .iter()
        .position(|summary| summary.contains("gap detected"))
        .expect("gap alert retained");
    assert_eq!(
        summaries.get(gap_position + 1).map(String::as_str),
        Some("resync started")
    );
    assert_eq!(
        summaries.get(gap_position + 2).map(String::as_str),
        Some("resync completed (aligned to 122)")
    );

    root.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("session reaped within the shutdown bound")
        .unwrap();
    assert_eq!(outcome.status, DisplayStatus::Stopped);
}

#[tokio::test]
async fn direct_burst_overflow_fails_visibly() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;

    let first = depth_response(100, "25000.00", "25001.00");
    let mock = Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(first, "application/json"));
    mock.mount(rest.inner()).await;

    // A single burst of contiguous deltas larger than the tiny capacity:
    // the connector task feeds them faster than the session drains them.
    let mut script = vec![WsAction::Sleep(Duration::from_millis(80))];
    for index in 0..12u64 {
        let first_seq = 101 + index * 5;
        script.push(WsAction::SendText(depth_delta(
            first_seq,
            first_seq + 4,
            "25000.10",
            "25001.10",
        )));
    }
    ws.push_script(script);

    let (mut rx, handle, root) = spawn_session(&ws, &rest, 2).await;
    let failed = wait_until(&mut rx, |snapshot| {
        snapshot.session == DisplayStatus::Failed
    })
    .await;
    assert!(
        failed
            .failure
            .as_deref()
            .unwrap_or("")
            .contains("capacity exceeded"),
        "expected a capacity fault, got {:?}",
        failed.failure
    );

    root.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("session reaped")
        .unwrap();
    assert_eq!(outcome.status, DisplayStatus::Failed);
}

#[tokio::test]
async fn explicit_cancellation_reaps_session_and_stream() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;

    let first = depth_response(100, "25000.00", "25001.00");
    let mock = Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(first, "application/json"));
    mock.mount(rest.inner()).await;

    ws.push_script(vec![WsAction::Sleep(Duration::from_millis(60))]);

    let (mut rx, handle, root) = spawn_session(&ws, &rest, crate::config::CHANNEL_CAPACITY).await;

    wait_until(&mut rx, |snapshot| {
        snapshot
            .symbols
            .iter()
            .any(|symbol| symbol.status != DisplayStatus::Loading)
    })
    .await;

    root.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("owned session reaped within the bound")
        .unwrap();
    assert_eq!(outcome.status, DisplayStatus::Stopped);
}
