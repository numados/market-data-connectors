#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Offline adapter → session → display tests with venue wire fixtures.

use std::sync::Arc;
use std::time::Duration;

use mdc_inspect::config::{CHANNEL_CAPACITY, SessionConfig};
use mdc_inspect::display::{DisplaySnapshot, DisplayStatus};
use mdc_inspect::provider::bitfinex::BitfinexAdapter;
use mdc_inspect::provider::coinbase::CoinbaseAdapter;
use mdc_inspect::provider::gateio::GateioAdapter;
use mdc_inspect::provider::kraken::KrakenAdapter;
use mdc_inspect::provider::{ProviderKind, SessionAdapter};
use mdc_inspect::session::{SessionOutcome, run_session};
use mdc_testkit::{MockRest, MockWsServer, WsAction, load_fixture};
use rust_decimal::Decimal;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const WAIT_BOUND: Duration = Duration::from_secs(10);

#[path = "mock_ws_wave1/subscriptions.rs"]
mod subscriptions;
#[path = "mock_ws_wave1/wire_frames.rs"]
mod wire_frames;

fn fixture(venue: &str, name: &str) -> String {
    load_fixture(format!(
        "crates/mdc-providers/tests/fixtures/{venue}/{name}.json"
    ))
    .expect("venue fixture")
}

fn gateio_fixture(name: &str) -> String {
    fixture("gateio", name).replace("ob.BTC_USDT.400", "ob.BTC_USDT.50")
}

async fn spawn_session(
    ws: &MockWsServer,
    adapter: Arc<dyn SessionAdapter>,
    provider: ProviderKind,
    symbol: &str,
) -> (
    watch::Receiver<Arc<DisplaySnapshot>>,
    JoinHandle<SessionOutcome>,
    CancellationToken,
) {
    let config = SessionConfig::new(provider, &[symbol.to_string()]).unwrap();
    let root = CancellationToken::new();
    let session_root = root.clone();
    let (tx, rx) = watch::channel(Arc::new(DisplaySnapshot {
        provider: provider.label(),
        presentation: Default::default(),
        session: DisplayStatus::Empty,
        failure: None,
        symbols: Vec::new(),
        alerts: Default::default(),
        alerts_evicted: 0,
    }));
    let handle = tokio::spawn(async move {
        run_session(
            adapter,
            config,
            provider.session_bounds(),
            CHANNEL_CAPACITY,
            session_root,
            tx,
        )
        .await
    });
    tokio::time::timeout(WAIT_BOUND, ws.wait_for_handshakes(1))
        .await
        .expect("local WebSocket handshake");
    (rx, handle, root)
}

async fn wait_until(
    rx: &mut watch::Receiver<Arc<DisplaySnapshot>>,
    predicate: impl Fn(&DisplaySnapshot) -> bool,
) -> Arc<DisplaySnapshot> {
    let deadline = tokio::time::Instant::now() + WAIT_BOUND;
    loop {
        let snapshot = rx.borrow_and_update().clone();
        if predicate(&snapshot) {
            return snapshot;
        }
        assert_ne!(snapshot.session, DisplayStatus::Failed, "{snapshot:#?}");
        match tokio::time::timeout_at(deadline, rx.changed()).await {
            Ok(Ok(())) => {}
            result => panic!("display wait ended ({result:?}); last snapshot: {snapshot:#?}"),
        }
    }
}

fn prices(values: [&str; 4]) -> [Decimal; 4] {
    values.map(|value| value.parse().expect("exact decimal"))
}

async fn wait_for_quote(
    rx: &mut watch::Receiver<Arc<DisplaySnapshot>>,
    symbol: &str,
    expected: [&str; 4],
) -> Arc<DisplaySnapshot> {
    let expected = prices(expected);
    let snapshot = wait_until(rx, |snapshot| {
        snapshot.symbols.first().is_some_and(|display| {
            display.symbol.as_str() == symbol
                && display.status == DisplayStatus::InSync
                && display.quote.as_ref().is_some_and(|value| {
                    let quote = &value.quote;
                    !value.historical
                        && [quote.bid, quote.ask, quote.bid_size, quote.ask_size] == expected
                })
        })
    })
    .await;
    assert_eq!(snapshot.symbols.len(), 1);
    assert_eq!(snapshot.session, DisplayStatus::InSync);
    let display = &snapshot.symbols[0];
    assert_eq!(display.symbol.as_str(), symbol);
    assert_eq!(display.status, DisplayStatus::InSync);
    assert!(display.unavailability.is_none());
    let value = display.quote.as_ref().expect("current quote");
    assert!(!value.historical);
    let quote = &value.quote;
    assert_eq!(
        [quote.bid, quote.ask, quote.bid_size, quote.ask_size],
        expected
    );
    snapshot
}

async fn wait_for_fault(
    rx: &mut watch::Receiver<Arc<DisplaySnapshot>>,
    symbol: &str,
    summary: &str,
) -> Arc<DisplaySnapshot> {
    let snapshot = wait_until(rx, |snapshot| {
        snapshot.symbols.first().is_some_and(|display| {
            display.symbol.as_str() == symbol
                && matches!(
                    display.status,
                    DisplayStatus::Unavailable | DisplayStatus::AwaitingSnapshot
                )
        }) && snapshot.alerts.iter().any(|alert| {
            alert.symbol.as_ref().map(|value| value.as_str()) == Some(symbol)
                && alert.summary.contains(summary)
        })
    })
    .await;
    assert!(snapshot.failure.is_none());
    assert!(
        snapshot.symbols[0]
            .quote
            .as_ref()
            .expect("retained quote after fault")
            .historical
    );
    snapshot
}

async fn wait_for_resubscribe(ws: &MockWsServer, initial_messages: usize) {
    // One unsubscribe and one subscribe must reach the server before it replies.
    tokio::time::timeout(
        WAIT_BOUND,
        ws.wait_for_client_messages(initial_messages + 2),
    )
    .await
    .expect("in-place resubscription");
}

fn assert_recovered(snapshot: &DisplaySnapshot, fault: &str) {
    let fault_position = snapshot
        .alerts
        .iter()
        .rposition(|alert| alert.summary.contains(fault))
        .expect("fault retained in alert history");
    let started_position = snapshot
        .alerts
        .iter()
        .rposition(|alert| alert.summary == "resync started")
        .expect("resync started retained");
    let completed_position = snapshot
        .alerts
        .iter()
        .rposition(|alert| alert.summary.starts_with("resync completed"))
        .expect("resync completed retained");
    assert!(fault_position < started_position && started_position < completed_position);
}

async fn stop_session(
    root: CancellationToken,
    handle: JoinHandle<SessionOutcome>,
    rest: &MockRest,
) {
    root.cancel();
    let outcome = tokio::time::timeout(WAIT_BOUND, handle)
        .await
        .expect("session reaped within shutdown bound")
        .expect("session joined");
    assert_eq!(outcome.status, DisplayStatus::Stopped);
    assert!(outcome.failure.is_none());
    // These four venues obtain snapshots over WebSocket, not REST.
    assert!(rest.inner().received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn kraken_checksum_mismatch_recovers_to_display() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    let snapshot = fixture("kraken", "order_book_snapshot");
    let update = fixture("kraken", "depth_update");
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::SendText(snapshot.clone()),
        WsAction::SendText(update.clone()),
    ]);
    let adapter = KrakenAdapter::with_endpoints(ws.ws_url().unwrap(), rest.url().unwrap());
    let (mut rx, handle, root) =
        spawn_session(&ws, Arc::new(adapter), ProviderKind::Kraken, "BTC/USD").await;

    // The update changes only the best bid's size, not either best price.
    wait_for_quote(
        &mut rx,
        "BTC/USD",
        ["45283.5", "45285.2", "0.20000000", "0.00100000"],
    )
    .await;
    subscriptions::kraken(&ws, false).await;

    let bad_update = update.replace("\"checksum\": 38355977", "\"checksum\": 999999999");
    assert_ne!(bad_update, update, "checksum fault must change the fixture");
    ws.broadcast_text(bad_update).unwrap();
    let fault = "checksum mismatch (expected 999999999, calculated 38355977)";
    wait_for_fault(&mut rx, "BTC/USD", fault).await;
    wait_for_resubscribe(&ws, 1).await;

    ws.broadcast_text(snapshot).unwrap();
    let recovered = wait_for_quote(
        &mut rx,
        "BTC/USD",
        ["45283.5", "45285.2", "0.10000000", "0.00100000"],
    )
    .await;
    assert_recovered(&recovered, fault);
    subscriptions::kraken(&ws, true).await;
    stop_session(root, handle, &rest).await;
}

#[tokio::test]
async fn coinbase_heartbeat_sequence_jump_recovers_to_display() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    let snapshot = fixture("coinbase", "order_book_snapshot");
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::SendText(snapshot.clone()),
        WsAction::SendText(fixture("coinbase", "depth_update")),
        WsAction::SendText(fixture("coinbase", "heartbeat_or_ping")),
    ]);
    let adapter = CoinbaseAdapter::with_endpoints(ws.ws_url().unwrap(), rest.url().unwrap());
    let (mut rx, handle, root) =
        spawn_session(&ws, Arc::new(adapter), ProviderKind::Coinbase, "BTC-USD").await;

    // The delta adds a better bid and deletes 10102.55, exposing 10103.00.
    wait_for_quote(
        &mut rx,
        "BTC-USD",
        ["10101.80000000", "10103.00", "0.162567", "2.50000000"],
    )
    .await;
    subscriptions::coinbase(&ws, false).await;

    ws.broadcast_text(
        r#"{"type":"heartbeat","sequence":4000,"last_trade_id":10,"product_id":"BTC-USD","time":"2014-11-07T08:19:29.464459Z"}"#,
    )
    .unwrap();
    // The fixture seeds sequence 90; a jump to 4000 exceeds the default 3000 threshold.
    // Coinbase represents this verdict as HeartbeatTimeout with elapsed_ms = 0.
    let fault = "heartbeat timeout (0 ms)";
    wait_for_fault(&mut rx, "BTC-USD", fault).await;
    wait_for_resubscribe(&ws, 1).await;

    ws.broadcast_text(snapshot).unwrap();
    let recovered = wait_for_quote(
        &mut rx,
        "BTC-USD",
        ["10101.10", "10102.55", "0.45054140", "0.57753524"],
    )
    .await;
    assert_recovered(&recovered, fault);
    subscriptions::coinbase(&ws, true).await;
    stop_session(root, handle, &rest).await;
}

#[tokio::test]
async fn bitfinex_checksum_mismatch_recovers_to_display() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    let subscribed = fixture("bitfinex", "subscribed_book");
    let snapshot = fixture("bitfinex", "order_book_snapshot");
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::WaitClientMessage,
        WsAction::SendText(fixture("bitfinex", "conf_ok")),
        WsAction::SendText(subscribed.clone()),
        WsAction::SendText(snapshot.clone()),
        WsAction::SendText(fixture("bitfinex", "depth_update")),
    ]);
    let adapter = BitfinexAdapter::with_endpoints(ws.ws_url().unwrap(), rest.url().unwrap());
    let (mut rx, handle, root) =
        spawn_session(&ws, Arc::new(adapter), ProviderKind::Bitfinex, "tBTCUSD").await;

    wait_for_quote(&mut rx, "tBTCUSD", ["41698", "41699", "0.61", "0.5"]).await;
    subscriptions::bitfinex(&ws, false).await;
    ws.broadcast_text(r#"[266343, "cs", 999999999, 1614604253634]"#)
        .unwrap();
    let fault = "checksum mismatch (expected 999999999,";
    wait_for_fault(&mut rx, "tBTCUSD", fault).await;
    wait_for_resubscribe(&ws, 2).await;

    ws.broadcast_text(fixture("bitfinex", "unsubscribed"))
        .unwrap();
    ws.broadcast_text(subscribed).unwrap();
    ws.broadcast_text(snapshot).unwrap();
    let recovered = wait_for_quote(&mut rx, "tBTCUSD", ["41698", "41699", "0.71", "0.5"]).await;
    assert_recovered(&recovered, fault);
    subscriptions::bitfinex(&ws, true).await;
    stop_session(root, handle, &rest).await;
}

#[tokio::test]
async fn gateio_sequence_gap_recovers_to_display() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::SendText(gateio_fixture("subscribe_ack")),
        WsAction::SendText(
            r#"{"time":1700000000,"channel":"spot.obu","event":"update","result":{"t":1700000000000,"full":true,"s":"ob.BTC_USDT.50","u":100,"b":[["50000","0.1"]],"a":[["50001","0.1"]]}}"#
                .to_string(),
        ),
    ]);
    let adapter = GateioAdapter::with_endpoints(ws.ws_url().unwrap(), rest.url().unwrap());
    let (mut rx, handle, root) =
        spawn_session(&ws, Arc::new(adapter), ProviderKind::Gateio, "BTC_USDT").await;

    wait_for_quote(&mut rx, "BTC_USDT", ["50000", "50001", "0.1", "0.1"]).await;
    subscriptions::gateio(&ws, false).await;
    ws.broadcast_text(
        r#"{"time":1700000000,"channel":"spot.obu","event":"update","result":{"t":1700000000100,"s":"ob.BTC_USDT.50","U":105,"u":106,"b":[["50000","0.2"]]}}"#,
    )
    .unwrap();
    let fault = "gap detected (expected 101, received 105)";
    wait_for_fault(&mut rx, "BTC_USDT", fault).await;
    wait_for_resubscribe(&ws, 1).await;

    // The full snapshot re-anchors at 123456; the fixture delta is contiguous.
    ws.broadcast_text(gateio_fixture("order_book_snapshot"))
        .unwrap();
    ws.broadcast_text(gateio_fixture("depth_update")).unwrap();
    // Deleting 50000 exposes the existing 49990 bid, not the inserted 49980.
    let recovered = wait_for_quote(&mut rx, "BTC_USDT", ["49990", "50001", "0.5", "0.25"]).await;
    assert_recovered(&recovered, fault);
    assert!(
        recovered
            .alerts
            .iter()
            .any(|alert| alert.summary == "resync completed (aligned to 123456)")
    );
    subscriptions::gateio(&ws, true).await;
    stop_session(root, handle, &rest).await;
}
