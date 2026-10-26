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
        if matches!(status, DisplayStatus::Stopped | DisplayStatus::Failed) {
            snapshot.session == status
        } else {
            !snapshot.symbols.is_empty()
                && snapshot
                    .symbols
                    .iter()
                    .all(|symbol| symbol.status == status)
        }
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

#[path = "mock_ws_wave2/scenarios.rs"]
mod scenarios;
