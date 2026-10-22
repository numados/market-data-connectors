//! Binance adapter component tests against `MockWsServer` and `MockRest`:
//! the full subscribe → snapshot → deltas → gap → resync → recovered display
//! path, overflow, and cancellation through the real session runner.

#![cfg(feature = "binance")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use mdc_core::Symbol;
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
    rx: &watch::Receiver<Arc<crate::display::DisplaySnapshot>>,
    status: DisplayStatus,
) -> Arc<crate::display::DisplaySnapshot> {
    wait_until(rx, |snapshot| {
        snapshot.session == status || snapshot.symbols.iter().any(|s| s.status == status)
    })
    .await
}

async fn wait_until(
    rx: &watch::Receiver<Arc<crate::display::DisplaySnapshot>>,
    predicate: impl Fn(&crate::display::DisplaySnapshot) -> bool,
) -> Arc<crate::display::DisplaySnapshot> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = rx.borrow().clone();
        if predicate(&snapshot) {
            return snapshot;
        }
        if tokio::time::Instant::now() >= deadline {
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
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn depth_response(last_update_id: u64, bid: &str, ask: &str) -> String {
    format!(
        r#"{{"lastUpdateId":{last_update_id},"bids":[["{bid}","1.0"]],"asks":[["{ask}","2.0"]]}}"#
    )
}

fn depth_delta(first: u64, final_seq: u64, bid: &str, ask: &str) -> String {
    format!(
        r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1600000000000,"s":"BTCUSDT","U":{first},"u":{final_seq},"b":[["{bid}","1.5"]],"a":[["{ask}","2.5"]]}}}}"#
    )
}

#[tokio::test]
async fn gap_resync_recovery_flows_to_display() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;

    // First snapshot (bridges d1), then the post-gap fast re-snapshot.
    let first = depth_response(100, "25000.00", "25001.00");
    let second = depth_response(122, "26000.00", "26001.00");
    let mock_once = Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(first, "application/json"))
        .up_to_n_times(1);
    mock_once.mount(rest.inner()).await;
    let mock_recurring = Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(second, "application/json"));
    mock_recurring.mount(rest.inner()).await;

    // d1 bridges snapshot 100; d2 contiguous; d3 gaps (expected 111).
    let d1 = depth_delta(95, 105, "25000.10", "25001.10");
    let d2 = depth_delta(106, 110, "25000.20", "25001.20");
    let d3 = depth_delta(120, 125, "26000.10", "26001.10");
    ws.push_script(vec![
        WsAction::SendText(d1),
        WsAction::Sleep(Duration::from_millis(50)),
        WsAction::SendText(d2),
        WsAction::Sleep(Duration::from_millis(50)),
        WsAction::SendText(d3),
    ]);

    let (rx, handle, root) = spawn_session(&ws, &rest, crate::config::CHANNEL_CAPACITY).await;

    // The gap appears in the retained alert ring; recovery follows.
    wait_until(&rx, |snapshot| {
        snapshot
            .alerts
            .iter()
            .any(|record| record.summary.contains("gap detected"))
    })
    .await;
    // Recovery: resync completes and the display returns to InSync with
    // values from the post-gap snapshot and deltas.
    let recovered = wait_for_status(&rx, DisplayStatus::InSync).await;

    let symbol = &recovered.symbols[0];
    assert_eq!(symbol.symbol, Symbol::from("BTCUSDT"));
    let quote = symbol.quote.as_ref().expect("recovered quote");
    assert!(!quote.historical);
    // The recovery delta raises the best bid above the snapshot's bid while
    // the snapshot's ask level remains best (deltas only touch named levels).
    assert_eq!(quote.quote.bid.to_string(), "26000.10");
    assert_eq!(quote.quote.ask.to_string(), "26001.00");

    // Alert history retains gap and recovery ordering.
    let summaries: Vec<String> = recovered
        .alerts
        .iter()
        .map(|record| record.summary.clone())
        .collect();
    let gap_position = summaries
        .iter()
        .rposition(|summary| summary.contains("gap detected"))
        .expect("gap alert retained");
    let started_position = summaries
        .iter()
        .rposition(|summary| summary.contains("resync started"))
        .expect("resync started retained");
    let completed_position = summaries
        .iter()
        .rposition(|summary| summary.contains("resync completed"))
        .expect("resync completed retained");
    assert!(gap_position < started_position && started_position < completed_position);

    // Operator stop cancels, drains, and reaps within the bound.
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

    let (rx, handle, root) = spawn_session(&ws, &rest, 2).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let failed = loop {
        let snapshot = rx.borrow().clone();
        if snapshot.session == DisplayStatus::Failed {
            break snapshot;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for visible failure"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
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

    let (rx, handle, root) = spawn_session(&ws, &rest, crate::config::CHANNEL_CAPACITY).await;

    // Wait until the connector actually connected before cancelling.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = rx.borrow().clone();
        if snapshot
            .symbols
            .iter()
            .any(|s| s.status != DisplayStatus::Loading)
        {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "no progress");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    root.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("owned session reaped within the bound")
        .unwrap();
    assert_eq!(outcome.status, DisplayStatus::Stopped);
}
