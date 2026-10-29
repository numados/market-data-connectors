//! Received book retention is independent of declared venue depth.
use super::*;
use std::fmt::Write;

async fn initial_book(rx: &watch::Receiver<Arc<DisplaySnapshot>>) -> Arc<DisplaySnapshot> {
    wait_until(rx, |snapshot| {
        snapshot
            .alerts
            .iter()
            .any(|alert| alert.summary.starts_with("resync completed"))
    })
    .await
}

fn assert_retained(snapshot: &DisplaySnapshot, name: &str, depth: usize) {
    let book = symbol(snapshot, name);
    assert_eq!(
        book.status,
        DisplayStatus::InSync,
        "{:?}",
        book.unavailability
    );
    assert!(book.unavailability.is_none());
    assert_eq!(book.bids.len(), depth);
    assert_eq!(book.asks.len(), depth);
    assert_current(snapshot, name, "1000000", "2000000", "1", "1");
    assert_eq!(book.bids.last().unwrap().price, Decimal::from(998_501));
    assert_eq!(book.asks.last().unwrap().price, Decimal::from(2_001_499));
}

#[tokio::test]
async fn depth_bitstamp_retains_received_levels() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    let (bids, asks) = depth_levels::arrays(0..1500, "1", true);
    let frame =
        format!(r#"{{"microtimestamp":"1700000000100000","bids":[{bids}],"asks":[{asks}]}}"#);
    Mock::given(method("GET"))
        .and(path("/order_book/btcusd/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(frame, "application/json"))
        .mount(rest.inner())
        .await;
    let (rx, task, cancel) = spawn_session(
        BitstampAdapter::with_endpoints(ws.ws_url().unwrap(), rest.url().unwrap()),
        ProviderKind::Bitstamp,
        &["btcusd"],
    );
    let initial = initial_book(&rx).await;
    assert_retained(&initial, "btcusd", 1500);
    subscriptions::bitstamp(&ws).await;

    let (bids, asks) = depth_levels::arrays(0..101, "0", true);
    ws.broadcast_text(format!(r#"{{"event":"data","channel":"diff_order_book_btcusd","data":{{"microtimestamp":"1700000000200000","bids":[{bids}],"asks":[{asks}]}}}}"#)).unwrap();
    let removed = wait_until(&rx, |snapshot| {
        let book = symbol(snapshot, "btcusd");
        book.bids.len() == 1399 || book.unavailability.is_some()
    })
    .await;
    assert_current(&removed, "btcusd", "999899", "2000101", "1", "1");
    assert_eq!(symbol(&removed, "btcusd").asks.len(), 1399);
    assert_eq!(
        symbol(&removed, "btcusd").bids.last().unwrap().price,
        Decimal::from(998_501)
    );

    ws.broadcast_text(r#"{"event":"data","channel":"diff_order_book_btcusd","data":{"microtimestamp":"1700000000300000","bids":[["1000000","3"]],"asks":[["2000000","4"]]}}"#).unwrap();
    let inserted = wait_until(&rx, |snapshot| {
        symbol(snapshot, "btcusd").bids.len() == 1400
    })
    .await;
    assert_current(&inserted, "btcusd", "1000000", "2000000", "3", "4");
    assert_eq!(symbol(&inserted, "btcusd").asks.len(), 1400);
    stop_session(cancel, task, &rx).await;
}

fn advanced_frame(
    sequence: u64,
    kind: &str,
    range: std::ops::Range<usize>,
    sizes: [&str; 2],
) -> String {
    let mut updates = String::new();
    for (bid, ask) in depth_levels::prices(range) {
        if !updates.is_empty() {
            updates.push(',');
        }
        write!(updates, r#"{{"side":"bid","event_time":"2024-01-01T00:00:00Z","price_level":"{bid}","new_quantity":"{}"}},{{"side":"offer","event_time":"2024-01-01T00:00:00Z","price_level":"{ask}","new_quantity":"{}"}}"#, sizes[0], sizes[1]).unwrap();
    }
    format!(
        r#"{{"channel":"l2_data","sequence_num":{sequence},"timestamp":"2024-01-01T00:00:00Z","events":[{{"type":"{kind}","product_id":"BTC-USD","updates":[{updates}]}}]}}"#
    )
}

#[tokio::test]
async fn depth_advanced_retains_received_levels() {
    let ws = MockWsServer::spawn().await.unwrap();
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::WaitClientMessage,
        WsAction::SendText(advanced_frame(1, "snapshot", 0..1500, ["1", "1"])),
    ]);
    let (rx, task, cancel) = spawn_session(
        CoinbaseAdvancedAdapter::with_endpoints(ws.ws_url().unwrap(), ws.ws_url().unwrap()),
        ProviderKind::CoinbaseAdvanced,
        &["BTC-USD"],
    );
    let initial = initial_book(&rx).await;
    assert_retained(&initial, "BTC-USD", 1500);

    ws.broadcast_text(advanced_frame(2, "update", 0..101, ["0", "0"]))
        .unwrap();
    let removed = wait_until(&rx, |snapshot| {
        let book = symbol(snapshot, "BTC-USD");
        book.bids.len() == 1399 || book.unavailability.is_some()
    })
    .await;
    assert_current(&removed, "BTC-USD", "999899", "2000101", "1", "1");
    assert_eq!(symbol(&removed, "BTC-USD").asks.len(), 1399);
    assert_eq!(
        symbol(&removed, "BTC-USD").asks.last().unwrap().price,
        Decimal::from(2_001_499)
    );

    ws.broadcast_text(advanced_frame(3, "update", 0..1, ["3", "4"]))
        .unwrap();
    let inserted = wait_until(&rx, |snapshot| {
        symbol(snapshot, "BTC-USD").bids.len() == 1400
    })
    .await;
    assert_current(&inserted, "BTC-USD", "1000000", "2000000", "3", "4");
    assert_eq!(symbol(&inserted, "BTC-USD").asks.len(), 1400);
    stop_session(cancel, task, &rx).await;
}

fn lmax_frame(range: std::ops::Range<usize>) -> String {
    let mut bids = String::new();
    let mut asks = String::new();
    for (bid, ask) in depth_levels::prices(range) {
        if !bids.is_empty() {
            bids.push(',');
            asks.push(',');
        }
        write!(bids, r#"{{"price":"{bid}","quantity":"1"}}"#).unwrap();
        write!(asks, r#"{{"price":"{ask}","quantity":"1"}}"#).unwrap();
    }
    format!(
        r#"{{"instrument_id":"btc-usd","timestamp":"2026-01-01T00:00:00Z","bids":[{bids}],"asks":[{asks}]}}"#
    )
}

#[tokio::test]
async fn depth_lmax_replaces_received_snapshots() {
    let ws = MockWsServer::spawn().await.unwrap();
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::SendText(lmax_frame(0..1500)),
    ]);
    let (rx, task, cancel) = spawn_session(
        LmaxAdapter::with_endpoints(ws.ws_url().unwrap(), ws.ws_url().unwrap()),
        ProviderKind::Lmax,
        &["btc-usd"],
    );
    let initial = initial_book(&rx).await;
    assert_retained(&initial, "btc-usd", 1500);
    subscriptions::lmax(&ws).await;

    ws.broadcast_text(lmax_frame(1500..1502)).unwrap();
    let replaced = wait_until(&rx, |snapshot| symbol(snapshot, "btc-usd").bids.len() == 2).await;
    assert_current(&replaced, "btc-usd", "998500", "2001500", "1", "1");
    assert_eq!(symbol(&replaced, "btc-usd").asks.len(), 2);
    stop_session(cancel, task, &rx).await;
}

fn huobi_update(sequence: u64, previous: u64, bids: &str, asks: &str) -> Vec<u8> {
    gzip_payload(format!(r#"{{"ch":"market.btcusdt.mbp.150","ts":1700000000300,"tick":{{"seqNum":{sequence},"prevSeqNum":{previous},"bids":[{bids}],"asks":[{asks}]}}}}"#).as_bytes())
}

#[tokio::test]
async fn depth_huobi_retains_incremental_levels() {
    let ws = MockWsServer::spawn().await.unwrap();
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::WaitClientMessage,
        WsAction::SendBinary(huobi_frame("heartbeat_or_ping", &[])),
    ]);
    let (rx, task, cancel) = spawn_session(
        HuobiAdapter::with_endpoints(ws.ws_url().unwrap(), ws.ws_url().unwrap()),
        ProviderKind::Huobi,
        &["btcusdt"],
    );
    let request = wait_for_client_message(&ws, 0, |message| {
        message.contains("\"req\":\"market.btcusdt.mbp.150\"")
    })
    .await;
    let id = huobi_request_id(&request);
    let (bids, asks) = depth_levels::arrays(0..150, "1", false);
    let initial_frame = format!(
        r#"{{"id":"{id}","rep":"market.btcusdt.mbp.150","status":"ok","ts":1700000000200,"data":{{"seqNum":100,"bids":[{bids}],"asks":[{asks}]}}}}"#
    );
    ws.broadcast_binary(gzip_payload(initial_frame.as_bytes()))
        .unwrap();
    let initial = initial_book(&rx).await;
    assert_current(&initial, "btcusdt", "1000000", "2000000", "1", "1");
    subscriptions::huobi(&ws).await;

    ws.broadcast_binary(huobi_update(101, 100, "[1000001,1]", "[1999999,1]"))
        .unwrap();
    let grown = wait_until(&rx, |snapshot| {
        let book = symbol(snapshot, "btcusdt");
        book.bids.len() == 151 || book.unavailability.as_deref() == Some("ExceededMaxDepth")
    })
    .await;
    let book = symbol(&grown, "btcusdt");
    assert_eq!(
        book.status,
        DisplayStatus::InSync,
        "{:?}",
        book.unavailability
    );
    assert_eq!(book.bids.len(), 151);
    assert_eq!(book.asks.len(), 151);
    assert_eq!(book.bids.last().unwrap().price, Decimal::from(999_851));
    assert_eq!(book.asks.last().unwrap().price, Decimal::from(2_000_149));

    let (bids, asks) = depth_levels::arrays(0..149, "0", false);
    ws.broadcast_binary(huobi_update(
        102,
        101,
        &format!("{bids},[1000001,0]"),
        &format!("{asks},[1999999,0]"),
    ))
    .unwrap();
    let removed = wait_until(&rx, |snapshot| symbol(snapshot, "btcusdt").bids.len() == 1).await;
    assert_current(&removed, "btcusdt", "999851", "2000149", "1", "1");
    assert_eq!(symbol(&removed, "btcusdt").asks.len(), 1);
    assert!(symbol(&removed, "btcusdt").unavailability.is_none());
    stop_session(cancel, task, &rx).await;
}
