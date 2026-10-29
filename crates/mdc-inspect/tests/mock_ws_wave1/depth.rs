//! Incremental received depth can exceed the requested snapshot depth.
use super::*;
use std::fmt::Write;

fn gate_frame(sequence: u64, full: bool, bids: &str, asks: &str) -> String {
    let header = if full {
        r#""full":true,"#.to_string()
    } else {
        format!(r#""U":{sequence},"#)
    };
    format!(
        r#"{{"time":1700000000,"channel":"spot.obu","event":"update","result":{{"t":1700000000000,{header}"s":"ob.BTC_USDT.50","u":{sequence},"b":[{bids}],"a":[{asks}]}}}}"#
    )
}

#[tokio::test]
async fn depth_gateio_retains_incremental_levels() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    let (bids, asks) = depth_levels::arrays(0..50, "1", true);
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::SendText(gateio_fixture("subscribe_ack")),
        WsAction::SendText(gate_frame(100, true, &bids, &asks)),
    ]);
    let adapter = GateioAdapter::with_endpoints(ws.ws_url().unwrap(), rest.url().unwrap());
    let (mut rx, task, cancel) =
        spawn_session(&ws, Arc::new(adapter), ProviderKind::Gateio, "BTC_USDT").await;
    wait_for_quote(&mut rx, "BTC_USDT", ["1000000", "2000000", "1", "1"]).await;
    subscriptions::gateio(&ws, false).await;

    ws.broadcast_text(gate_frame(
        101,
        false,
        r#"["1000001","1"]"#,
        r#"["1999999","1"]"#,
    ))
    .unwrap();
    let grown = wait_until(&mut rx, |snapshot| {
        snapshot.symbols.first().is_some_and(|book| {
            book.bids.len() == 51 || book.unavailability.as_deref() == Some("ExceededMaxDepth")
        })
    })
    .await;
    let book = grown.symbols.first().unwrap();
    assert_eq!(
        book.status,
        DisplayStatus::InSync,
        "{:?}",
        book.unavailability
    );
    assert_eq!(book.bids.len(), 51);
    assert_eq!(book.asks.len(), 51);
    assert_eq!(book.bids.last().unwrap().price, Decimal::from(999_951));
    assert_eq!(book.asks.last().unwrap().price, Decimal::from(2_000_049));

    let (bids, asks) = depth_levels::arrays(0..49, "0", true);
    ws.broadcast_text(gate_frame(
        102,
        false,
        &format!(r#"{bids},["1000001","0"]"#),
        &format!(r#"{asks},["1999999","0"]"#),
    ))
    .unwrap();
    let removed = wait_for_quote(&mut rx, "BTC_USDT", ["999951", "2000049", "1", "1"]).await;
    let book = removed.symbols.first().unwrap();
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.asks.len(), 1);
    assert!(book.unavailability.is_none());
    stop_session(cancel, task, &rest).await;
}

fn bitfinex_snapshot(depth: usize) -> String {
    let mut levels = String::new();
    for (bid, ask) in depth_levels::prices(0..depth) {
        if !levels.is_empty() {
            levels.push(',');
        }
        write!(levels, "[{bid},1,1],[{ask},1,-1]").unwrap();
    }
    format!("[266343,[{levels}],1614604253634]")
}

fn bitfinex_update(price: usize, count: u32, amount: i32) -> String {
    format!("[266343,[{price},{count},{amount}],1614604253700]")
}

#[tokio::test]
async fn depth_bitfinex_retains_incremental_levels() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    ws.push_script(vec![
        WsAction::WaitClientMessage,
        WsAction::WaitClientMessage,
        WsAction::SendText(fixture("bitfinex", "conf_ok")),
        WsAction::SendText(fixture("bitfinex", "subscribed_book")),
        WsAction::SendText(bitfinex_snapshot(250)),
    ]);
    let adapter = BitfinexAdapter::with_endpoints(ws.ws_url().unwrap(), rest.url().unwrap());
    let (mut rx, task, cancel) =
        spawn_session(&ws, Arc::new(adapter), ProviderKind::Bitfinex, "tBTCUSD").await;
    wait_for_quote(&mut rx, "tBTCUSD", ["1000000", "2000000", "1", "1"]).await;
    subscriptions::bitfinex(&ws, false).await;

    ws.broadcast_text(bitfinex_update(1_000_001, 1, 1)).unwrap();
    ws.broadcast_text(bitfinex_update(1_999_999, 1, -1))
        .unwrap();
    let grown = wait_until(&mut rx, |snapshot| {
        snapshot.symbols.first().is_some_and(|book| {
            (book.bids.len() == 251 && book.asks.len() == 251)
                || book.unavailability.as_deref() == Some("ExceededMaxDepth")
        })
    })
    .await;
    let book = grown.symbols.first().unwrap();
    assert_eq!(
        book.status,
        DisplayStatus::InSync,
        "{:?}",
        book.unavailability
    );
    assert!(book.unavailability.is_none());
    assert_eq!(book.bids.len(), 251);
    assert_eq!(book.asks.len(), 251);
    assert_eq!(book.bids.last().unwrap().price, Decimal::from(999_751));
    assert_eq!(book.asks.last().unwrap().price, Decimal::from(2_000_249));
    let quote = book.quote.as_ref().unwrap();
    assert!(!quote.historical);
    assert_eq!(quote.quote.bid, Decimal::from(1_000_001));
    assert_eq!(quote.quote.ask, Decimal::from(1_999_999));

    ws.broadcast_text(bitfinex_update(1_000_001, 0, 1)).unwrap();
    ws.broadcast_text(bitfinex_update(1_999_999, 0, -1))
        .unwrap();
    for (index, (bid, ask)) in depth_levels::prices(0..249).enumerate() {
        ws.broadcast_text(bitfinex_update(bid, 0, 1)).unwrap();
        ws.broadcast_text(bitfinex_update(ask, 0, -1)).unwrap();
        // Drain bounded batches rather than overflow the mock's broadcast queue.
        if index % 32 == 31 {
            tokio::time::timeout(WAIT_BOUND, ws.sync_barrier())
                .await
                .expect("deletion batch consumed before sending the next");
        }
    }
    let removed = wait_for_quote(&mut rx, "tBTCUSD", ["999751", "2000249", "1", "1"]).await;
    let book = removed.symbols.first().unwrap();
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.asks.len(), 1);
    assert!(book.unavailability.is_none());
    stop_session(cancel, task, &rest).await;
}
