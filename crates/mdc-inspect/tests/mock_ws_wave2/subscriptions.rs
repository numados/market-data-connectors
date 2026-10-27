use mdc_testkit::MockWsServer;

use super::wire_frames::{assert_allowed, assert_resubscribe, recorded};

pub(super) async fn bitstamp(ws: &MockWsServer) {
    const SUBSCRIBE: &str =
        r#"{"event":"bts:subscribe","data":{"channel":"diff_order_book_btcusd"}}"#;
    let messages = recorded(ws).await;
    assert_allowed(&messages, &[SUBSCRIBE], &[SUBSCRIBE]);
}

pub(super) async fn huobi(ws: &MockWsServer) {
    const SUBSCRIBE: &str = r#"{"sub":"market.btcusdt.mbp.150","id":"sub-mbp-btcusdt"}"#;
    const REQUEST: &str = r#"{"req":"market.btcusdt.mbp.150","id":"snap-btcusdt-1"}"#;
    const PONG: &str = r#"{"pong":1700000000000}"#;
    let messages: Vec<_> = recorded(ws)
        .await
        .into_iter()
        .map(|message| {
            if let Some(counter) = message
                .strip_prefix(r#"{"req":"market.btcusdt.mbp.150","id":"snap-btcusdt-"#)
                .and_then(|value| value.strip_suffix("\"}"))
            {
                assert!(counter.parse::<u64>().expect("Huobi snapshot counter") > 0);
                REQUEST.to_string()
            } else {
                message
            }
        })
        .collect();
    assert_allowed(
        &messages,
        &[SUBSCRIBE, REQUEST, PONG],
        &[SUBSCRIBE, REQUEST, PONG],
    );
}

pub(super) async fn coinbase_advanced(ws: &MockWsServer, recovered: bool) {
    const SUBSCRIBE: &str =
        r#"{"channel":"level2","product_ids":["BTC-USD","ETH-USD"],"type":"subscribe"}"#;
    const HEARTBEATS: &str =
        r#"{"channel":"heartbeats","product_ids":["BTC-USD","ETH-USD"],"type":"subscribe"}"#;
    const UNSUBSCRIBE: &str =
        r#"{"channel":"level2","product_ids":["BTC-USD","ETH-USD"],"type":"unsubscribe"}"#;
    let messages = recorded(ws).await;
    assert_allowed(
        &messages,
        &[SUBSCRIBE, HEARTBEATS],
        &[SUBSCRIBE, HEARTBEATS, UNSUBSCRIBE],
    );
    if recovered {
        assert_resubscribe(&messages, UNSUBSCRIBE, SUBSCRIBE);
    }
}

pub(super) async fn lmax(ws: &MockWsServer) {
    const SUBSCRIBE: &str =
        r#"{"type":"SUBSCRIBE","channels":[{"name":"ORDER_BOOK","instruments":["btc-usd"]}]}"#;
    let messages = recorded(ws).await;
    assert_allowed(&messages, &[SUBSCRIBE], &[SUBSCRIBE]);
}
