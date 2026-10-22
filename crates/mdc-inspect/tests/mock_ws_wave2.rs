#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use mdc_inspect::config::{CHANNEL_CAPACITY, SessionConfig};
use mdc_inspect::display::{DisplaySnapshot, DisplayStatus, SymbolDisplay};
use mdc_inspect::provider::bitstamp::BitstampAdapter;
use mdc_inspect::provider::coinbase_advanced::CoinbaseAdvancedAdapter;
use mdc_inspect::provider::huobi::HuobiAdapter;
use mdc_inspect::provider::lmax::LmaxAdapter;
use mdc_inspect::provider::{ProviderKind, SessionAdapter};
use mdc_inspect::session::{SessionOutcome, run_session};
use mdc_providers::huobi::GzipDecoder;
use mdc_testkit::{MockRest, MockWsServer, WsAction, load_fixture, load_fixture_bytes};
use rust_decimal::Decimal;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

const WAIT_BOUND: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

fn spawn_session(
    adapter: impl SessionAdapter,
    provider: ProviderKind,
    symbols: &[&str],
) -> (
    watch::Receiver<Arc<DisplaySnapshot>>,
    JoinHandle<SessionOutcome>,
    CancellationToken,
) {
    let config = SessionConfig::new(
        provider,
        &symbols
            .iter()
            .map(|symbol| symbol.to_string())
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let root = CancellationToken::new();
    let session_root = root.clone();
    let (tx, rx) = watch::channel(Arc::new(DisplaySnapshot {
        provider: provider.label(),
        session: DisplayStatus::Empty,
        failure: None,
        symbols: Vec::new(),
        alerts: Default::default(),
        alerts_evicted: 0,
    }));
    let handle = tokio::spawn(async move {
        run_session(
            Arc::new(adapter),
            config,
            provider.session_bounds(),
            CHANNEL_CAPACITY,
            session_root,
            tx,
        )
        .await
    });
    (rx, handle, root)
}

async fn wait_until(
    rx: &watch::Receiver<Arc<DisplaySnapshot>>,
    predicate: impl Fn(&DisplaySnapshot) -> bool,
) -> Arc<DisplaySnapshot> {
    let deadline = tokio::time::Instant::now() + WAIT_BOUND;
    let mut poll = tokio::time::interval(POLL_INTERVAL);
    loop {
        poll.tick().await;
        let snapshot = rx.borrow().clone();
        if predicate(&snapshot) {
            return snapshot;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for display state; last snapshot={snapshot:?}"
        );
    }
}

async fn wait_for_status(
    rx: &watch::Receiver<Arc<DisplaySnapshot>>,
    status: DisplayStatus,
) -> Arc<DisplaySnapshot> {
    wait_until(rx, |snapshot| {
        snapshot.session == status
            || (!snapshot.symbols.is_empty()
                && snapshot
                    .symbols
                    .iter()
                    .all(|symbol| symbol.status == status))
    })
    .await
}

async fn wait_for_client_message(
    ws: &MockWsServer,
    after: usize,
    predicate: impl Fn(&str) -> bool,
) -> String {
    let deadline = tokio::time::Instant::now() + WAIT_BOUND;
    let mut poll = tokio::time::interval(POLL_INTERVAL);
    loop {
        poll.tick().await;
        let messages = ws.received_messages();
        if let Some(message) = messages.iter().skip(after).rev().find(|msg| predicate(msg)) {
            return message.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for client frame after {after}; received={messages:?}"
        );
    }
}

fn symbol<'a>(snapshot: &'a DisplaySnapshot, name: &str) -> &'a SymbolDisplay {
    snapshot
        .symbols
        .iter()
        .find(|symbol| symbol.symbol.as_str() == name)
        .unwrap()
}

fn assert_current(
    snapshot: &DisplaySnapshot,
    name: &str,
    bid: &str,
    ask: &str,
    bid_size: &str,
    ask_size: &str,
) {
    let symbol = symbol(snapshot, name);
    assert_eq!(symbol.status, DisplayStatus::InSync);
    let quote = symbol.quote.as_ref().expect("current quote");
    assert!(!quote.historical);
    assert_eq!(quote.quote.bid, bid.parse::<Decimal>().unwrap());
    assert_eq!(quote.quote.ask, ask.parse::<Decimal>().unwrap());
    assert_eq!(quote.quote.bid_size, bid_size.parse::<Decimal>().unwrap());
    assert_eq!(quote.quote.ask_size, ask_size.parse::<Decimal>().unwrap());
}

fn assert_invalidated(snapshot: &DisplaySnapshot) {
    for symbol in &snapshot.symbols {
        assert!(matches!(
            symbol.status,
            DisplayStatus::Unavailable | DisplayStatus::AwaitingSnapshot
        ));
        assert!(
            symbol
                .quote
                .as_ref()
                .expect("retained prior quote")
                .historical
        );
    }
}

fn has_alert(snapshot: &DisplaySnapshot, name: &str, summary: &str) -> bool {
    snapshot.alerts.iter().any(|record| {
        record
            .symbol
            .as_ref()
            .is_some_and(|symbol| symbol.as_str() == name)
            && record.summary == summary
    })
}

fn assert_recovery_order(snapshot: &DisplaySnapshot, name: &str, fault: &str) {
    let summaries: Vec<_> = snapshot
        .alerts
        .iter()
        .filter(|record| {
            record
                .symbol
                .as_ref()
                .is_some_and(|symbol| symbol.as_str() == name)
        })
        .map(|record| record.summary.as_str())
        .collect();
    let fault = summaries
        .iter()
        .rposition(|summary| *summary == fault)
        .expect("fault retained");
    let started = summaries
        .iter()
        .rposition(|summary| *summary == "resync started")
        .unwrap();
    let completed = summaries
        .iter()
        .rposition(|summary| summary.starts_with("resync completed"))
        .unwrap();
    assert!(
        fault < started && started < completed,
        "alerts={summaries:?}"
    );
}

async fn stop_session(
    root: CancellationToken,
    handle: JoinHandle<SessionOutcome>,
    rx: &watch::Receiver<Arc<DisplaySnapshot>>,
) {
    root.cancel();
    let outcome = tokio::time::timeout(WAIT_BOUND, handle)
        .await
        .expect("session reaped within shutdown bound")
        .unwrap();
    assert_eq!(outcome.status, DisplayStatus::Stopped);
    assert_eq!(outcome.failure, None);
    assert_eq!(rx.borrow().session, DisplayStatus::Stopped);
}

// Correlated Huobi replies need the live request ID and the adapter's MBP depth.
// Use one stored DEFLATE block for adjusted gzip fixtures, without another dependency.
fn huobi_frame(fixture: &str, replacements: &[(&str, &str)]) -> Vec<u8> {
    let bytes = load_fixture_bytes(format!(
        "crates/mdc-providers/tests/fixtures/huobi/{fixture}.json.gz"
    ))
    .unwrap();
    if replacements.is_empty() {
        return bytes;
    }
    let mut decoder = GzipDecoder::new();
    let mut json = String::from_utf8(decoder.decompress(&bytes).unwrap().to_vec()).unwrap();
    for (from, to) in replacements {
        assert!(json.contains(from), "fixture missing {from:?}");
        json = json.replace(from, to);
    }
    let payload = json.as_bytes();
    let length = u16::try_from(payload.len()).expect("fixture fits one stored block");
    let mut gzip = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff, 1];
    gzip.extend_from_slice(&length.to_le_bytes());
    gzip.extend_from_slice(&(!length).to_le_bytes());
    gzip.extend_from_slice(payload);
    let mut crc = !0_u32;
    for byte in payload {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    gzip.extend_from_slice(&(!crc).to_le_bytes());
    gzip.extend_from_slice(&u32::from(length).to_le_bytes());
    gzip
}

fn huobi_snapshot(request: &str, sequence: &str) -> Vec<u8> {
    let id = request
        .split("\"id\":\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    huobi_frame(
        "order_book_snapshot",
        &[
            ("snap-1", id),
            ("mbp.400", "mbp.150"),
            ("100020146800", sequence),
        ],
    )
}

#[tokio::test]
async fn bitstamp_invalid_delta_recovers_from_fresh_rest_snapshot() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    let first =
        load_fixture("crates/mdc-providers/tests/fixtures/bitstamp/order_book_snapshot.json")
            .unwrap();
    let fresh = r#"{"timestamp":"1700000001","microtimestamp":"1700000000900000","bids":[["50000.00","1.0"]],"asks":[["50001.00","1.0"]]}"#;
    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(first, "application/json"))
        .up_to_n_times(1)
        .mount(rest.inner())
        .await;
    // A slow fresh snapshot exposes the invalidated state across display coalescing.
    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(fresh, "application/json")
                .set_delay(Duration::from_millis(300)),
        )
        .mount(rest.inner())
        .await;

    let (rx, handle, root) = spawn_session(
        BitstampAdapter::with_endpoints(ws.ws_url().unwrap(), rest.url().unwrap()),
        ProviderKind::Bitstamp,
        &["btcusd"],
    );
    let initial = wait_for_status(&rx, DisplayStatus::InSync).await;
    assert_current(
        &initial,
        "btcusd",
        "50000.00",
        "50001.00",
        "0.50000000",
        "0.30000000",
    );

    // Equal snapshot microtimestamp is stale; the following newer fixture is accepted.
    ws.broadcast_text(r#"{"event":"data","channel":"diff_order_book_btcusd","data":{"timestamp":"1700000000","microtimestamp":"1700000000100000","bids":[["50000.00","1.00000000"]],"asks":[]}}"#).unwrap();
    ws.broadcast_text(
        load_fixture("crates/mdc-providers/tests/fixtures/bitstamp/depth_update.json").unwrap(),
    )
    .unwrap();
    let updated = wait_until(&rx, |snapshot| {
        symbol(snapshot, "btcusd")
            .quote
            .as_ref()
            .is_some_and(|quote| {
                !quote.historical
                    && quote.quote.bid_size == "0.60000000".parse::<Decimal>().unwrap()
            })
    })
    .await;
    assert_current(
        &updated,
        "btcusd",
        "50000.00",
        "50001.00",
        "0.60000000",
        "0.30000000",
    );
    assert_eq!(symbol(&updated, "btcusd").asks.len(), 3);

    ws.broadcast_text(r#"{"event":"data","channel":"diff_order_book_btcusd","data":{"timestamp":"1700000000","microtimestamp":"1700000000200000","bids":[["not-a-price","1.0"]],"asks":[]}}"#).unwrap();
    let fault = wait_until(&rx, |snapshot| {
        has_alert(snapshot, "btcusd", "source error (Parse)")
            && symbol(snapshot, "btcusd").status == DisplayStatus::AwaitingSnapshot
    })
    .await;
    assert_invalidated(&fault);
    assert_eq!(
        symbol(&fault, "btcusd").quote.as_ref().unwrap().quote,
        symbol(&updated, "btcusd").quote.as_ref().unwrap().quote
    );

    let recovered = wait_until(&rx, |snapshot| {
        let symbol = symbol(snapshot, "btcusd");
        symbol.status == DisplayStatus::InSync
            && symbol
                .quote
                .as_ref()
                .is_some_and(|quote| quote.quote.bid_size == Decimal::ONE)
    })
    .await;
    assert_current(&recovered, "btcusd", "50000.00", "50001.00", "1.0", "1.0");
    assert_eq!(symbol(&recovered, "btcusd").bids.len(), 1);
    assert_eq!(symbol(&recovered, "btcusd").asks.len(), 1);
    assert_recovery_order(&recovered, "btcusd", "source error (Parse)");
    assert_eq!(rest.inner().received_requests().await.unwrap().len(), 2);
    stop_session(root, handle, &rx).await;
}

#[tokio::test]
async fn huobi_gzip_prev_sequence_gap_is_visible_and_recovers() {
    let ws = MockWsServer::spawn().await.unwrap();
    // The server's application ping is a verbatim gzip fixture, not a text frame.
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::WaitClientMessage,
        WsAction::SendBinary(huobi_frame("heartbeat_or_ping", &[])),
    ]);
    let unused_rest = ws.ws_url().unwrap();
    let (rx, handle, root) = spawn_session(
        HuobiAdapter::with_endpoints(ws.ws_url().unwrap(), unused_rest),
        ProviderKind::Huobi,
        &["btcusdt"],
    );
    let request = wait_for_client_message(&ws, 0, |message| {
        message.contains("\"req\":\"market.btcusdt.mbp.150\"")
    })
    .await;
    ws.broadcast_binary(huobi_snapshot(&request, "100"))
        .unwrap();
    wait_for_client_message(&ws, 0, |message| message.contains("\"pong\":1700000000000")).await;
    let initial = wait_for_status(&rx, DisplayStatus::InSync).await;
    assert_current(&initial, "btcusdt", "26000.1", "26000.2", "0.5", "0.3");

    ws.broadcast_binary(huobi_frame(
        "depth_update",
        &[
            ("mbp.400", "mbp.150"),
            ("100020146794", "100"),
            ("100020146795", "101"),
        ],
    ))
    .unwrap();
    let updated = wait_until(&rx, |snapshot| {
        symbol(snapshot, "btcusdt")
            .quote
            .as_ref()
            .is_some_and(|quote| {
                !quote.historical && quote.quote.ask == "26000.3".parse::<Decimal>().unwrap()
            })
    })
    .await;
    assert_current(&updated, "btcusdt", "26000.1", "26000.3", "0.5", "0.8");

    let before_gap = ws.received_messages().len();
    ws.broadcast_binary(huobi_frame(
        "depth_update",
        &[
            ("mbp.400", "mbp.150"),
            ("100020146794", "105"),
            ("100020146795", "106"),
        ],
    ))
    .unwrap();
    let gap_summary = "gap detected (expected 102, received 106)";
    let fault = wait_until(&rx, |snapshot| {
        has_alert(snapshot, "btcusdt", gap_summary)
            && symbol(snapshot, "btcusdt").status == DisplayStatus::AwaitingSnapshot
    })
    .await;
    assert_invalidated(&fault);
    assert_eq!(
        symbol(&fault, "btcusdt").quote.as_ref().unwrap().quote,
        symbol(&updated, "btcusdt").quote.as_ref().unwrap().quote
    );

    let request = wait_for_client_message(&ws, before_gap, |message| {
        message.contains("\"req\":\"market.btcusdt.mbp.150\"")
    })
    .await;
    ws.broadcast_binary(huobi_snapshot(&request, "106"))
        .unwrap();
    let recovered = wait_until(&rx, |snapshot| {
        has_alert(snapshot, "btcusdt", "resync completed (aligned to 106)")
            && symbol(snapshot, "btcusdt").status == DisplayStatus::InSync
    })
    .await;
    assert_current(&recovered, "btcusdt", "26000.1", "26000.2", "0.5", "0.3");
    assert_recovery_order(&recovered, "btcusdt", gap_summary);
    stop_session(root, handle, &rx).await;
}

#[tokio::test]
async fn coinbase_advanced_connection_gap_invalidates_all_products_and_recovers() {
    let ws = MockWsServer::spawn().await.unwrap();
    let snapshot = load_fixture(
        "crates/mdc-providers/tests/fixtures/coinbase_advanced/order_book_snapshot.json",
    )
    .unwrap();
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::WaitClientMessage,
        WsAction::SendText(snapshot.clone()),
        WsAction::SendText(
            snapshot
                .replace("BTC-USD", "ETH-USD")
                .replace("\"sequence_num\": 1", "\"sequence_num\": 2"),
        ),
    ]);
    let unused_rest = ws.ws_url().unwrap();
    let (rx, handle, root) = spawn_session(
        CoinbaseAdvancedAdapter::with_endpoints(ws.ws_url().unwrap(), unused_rest),
        ProviderKind::CoinbaseAdvanced,
        &["BTC-USD", "ETH-USD"],
    );
    let initial = wait_for_status(&rx, DisplayStatus::InSync).await;
    for name in ["BTC-USD", "ETH-USD"] {
        assert_current(
            &initial,
            name,
            "21921.73",
            "21925.00",
            "0.06317902",
            "0.15000000",
        );
    }
    let before_gap = ws.received_messages().len();
    ws.broadcast_text(r#"{"channel":"l2_data","sequence_num":5,"timestamp":"2023-02-09T20:32:55.000000Z","events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2023-02-09T20:32:55.000000Z","price_level":"21950.00","new_quantity":"1.50"}]}]}"#).unwrap();
    let gap_summary = "gap detected (expected 3, received 5)";
    let fault = wait_until(&rx, |snapshot| {
        snapshot
            .symbols
            .iter()
            .all(|symbol| symbol.status == DisplayStatus::AwaitingSnapshot)
            && ["BTC-USD", "ETH-USD"]
                .iter()
                .all(|name| has_alert(snapshot, name, gap_summary))
    })
    .await;
    assert_invalidated(&fault);
    for name in ["BTC-USD", "ETH-USD"] {
        assert_eq!(
            symbol(&fault, name).quote.as_ref().unwrap().quote,
            symbol(&initial, name).quote.as_ref().unwrap().quote
        );
    }
    wait_for_client_message(&ws, before_gap, |message| {
        message.contains("\"type\":\"unsubscribe\"") && message.contains("\"channel\":\"level2\"")
    })
    .await;
    wait_for_client_message(&ws, before_gap, |message| {
        message.contains("\"type\":\"subscribe\"") && message.contains("\"channel\":\"level2\"")
    })
    .await;

    // An update while awaiting snapshots cannot make the old book current again.
    ws.broadcast_text(
        load_fixture("crates/mdc-providers/tests/fixtures/coinbase_advanced/depth_update.json")
            .unwrap()
            .replace("\"sequence_num\": 2", "\"sequence_num\": 6"),
    )
    .unwrap();
    tokio::time::timeout(WAIT_BOUND, ws.sync_barrier())
        .await
        .unwrap();
    ws.broadcast_text(snapshot.replace("\"sequence_num\": 1", "\"sequence_num\": 7"))
        .unwrap();
    let partially_recovered = wait_until(&rx, |snapshot| {
        symbol(snapshot, "BTC-USD").status == DisplayStatus::InSync
            && symbol(snapshot, "ETH-USD").status == DisplayStatus::AwaitingSnapshot
    })
    .await;
    assert!(
        symbol(&partially_recovered, "ETH-USD")
            .quote
            .as_ref()
            .unwrap()
            .historical
    );
    ws.broadcast_text(
        snapshot
            .replace("BTC-USD", "ETH-USD")
            .replace("\"sequence_num\": 1", "\"sequence_num\": 8"),
    )
    .unwrap();
    let recovered = wait_for_status(&rx, DisplayStatus::InSync).await;
    for name in ["BTC-USD", "ETH-USD"] {
        assert_current(
            &recovered,
            name,
            "21921.73",
            "21925.00",
            "0.06317902",
            "0.15000000",
        );
        assert_recovery_order(&recovered, name, gap_summary);
        assert!(
            symbol(&recovered, name)
                .quote
                .as_ref()
                .unwrap()
                .quote
                .received_ts
                > symbol(&initial, name)
                    .quote
                    .as_ref()
                    .unwrap()
                    .quote
                    .received_ts
        );
    }
    stop_session(root, handle, &rx).await;
}

#[tokio::test]
async fn lmax_transport_loss_marks_prior_quote_historical_until_fresh_snapshot() {
    let ws = MockWsServer::spawn().await.unwrap();
    let snapshot =
        load_fixture("crates/mdc-providers/tests/fixtures/lmax/order_book_snapshot.json").unwrap();
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::SendText(snapshot.clone()),
    ]);
    let unused_rest = ws.ws_url().unwrap();
    let (rx, handle, root) = spawn_session(
        LmaxAdapter::with_endpoints(ws.ws_url().unwrap(), unused_rest),
        ProviderKind::Lmax,
        &["btc-usd"],
    );
    let initial = wait_for_status(&rx, DisplayStatus::InSync).await;
    assert_current(
        &initial, "btc-usd", "50000.00", "50001.00", "1.5000", "0.7500",
    );

    let before_close = ws.received_messages().len();
    ws.broadcast_close().unwrap();
    let fault_summary = "connection lost (Closed)";
    let fault = wait_until(&rx, |snapshot| {
        has_alert(snapshot, "btc-usd", fault_summary)
            && matches!(
                symbol(snapshot, "btc-usd").status,
                DisplayStatus::Unavailable | DisplayStatus::AwaitingSnapshot
            )
    })
    .await;
    assert_invalidated(&fault);
    assert_eq!(
        symbol(&fault, "btc-usd").quote.as_ref().unwrap().quote,
        symbol(&initial, "btc-usd").quote.as_ref().unwrap().quote
    );

    wait_for_client_message(&ws, before_close, |message| {
        message.contains("\"type\":\"SUBSCRIBE\"")
    })
    .await;
    assert_eq!(ws.connection_count(), 2);
    let awaiting = wait_for_status(&rx, DisplayStatus::AwaitingSnapshot).await;
    assert_invalidated(&awaiting);
    ws.broadcast_text(snapshot).unwrap();
    let recovered = wait_for_status(&rx, DisplayStatus::InSync).await;
    assert_current(
        &recovered, "btc-usd", "50000.00", "50001.00", "1.5000", "0.7500",
    );
    assert_recovery_order(&recovered, "btc-usd", fault_summary);
    assert!(
        symbol(&recovered, "btc-usd")
            .quote
            .as_ref()
            .unwrap()
            .quote
            .received_ts
            > symbol(&initial, "btc-usd")
                .quote
                .as_ref()
                .unwrap()
                .quote
                .received_ts
    );
    stop_session(root, handle, &rx).await;
}
