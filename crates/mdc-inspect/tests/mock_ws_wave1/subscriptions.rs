use mdc_testkit::MockWsServer;

use super::wire_frames::{assert_allowed, assert_resubscribe, recorded};

pub(super) async fn kraken(ws: &MockWsServer, recovered: bool) {
    const SUBSCRIBE: &str =
        r#"{"method":"subscribe","params":{"channel":"book","depth":1000,"symbol":["BTC/USD"]}}"#;
    const UNSUBSCRIBE: &str =
        r#"{"method":"unsubscribe","params":{"channel":"book","depth":1000,"symbol":["BTC/USD"]}}"#;
    let messages = recorded(ws).await;
    assert_allowed(&messages, &[SUBSCRIBE], &[SUBSCRIBE, UNSUBSCRIBE]);
    if recovered {
        assert_resubscribe(&messages, UNSUBSCRIBE, SUBSCRIBE);
    }
}

pub(super) async fn coinbase(ws: &MockWsServer, recovered: bool) {
    const INITIAL: &str =
        r#"{"channels":["level2_batch","heartbeat"],"product_ids":["BTC-USD"],"type":"subscribe"}"#;
    const SUBSCRIBE: &str =
        r#"{"channels":["level2_batch"],"product_ids":["BTC-USD"],"type":"subscribe"}"#;
    const UNSUBSCRIBE: &str =
        r#"{"channels":["level2_batch"],"product_ids":["BTC-USD"],"type":"unsubscribe"}"#;
    let messages = recorded(ws).await;
    assert_allowed(&messages, &[INITIAL], &[INITIAL, SUBSCRIBE, UNSUBSCRIBE]);
    if recovered {
        assert_resubscribe(&messages, UNSUBSCRIBE, SUBSCRIBE);
    }
}

pub(super) async fn bitfinex(ws: &MockWsServer, recovered: bool) {
    const CONF: &str = r#"{"event":"conf","flags":163840}"#;
    const SUBSCRIBE: &str = r#"{"channel":"book","event":"subscribe","freq":"F0","len":250,"prec":"P0","symbol":"tBTCUSD"}"#;
    const UNSUBSCRIBE: &str = r#"{"chanId":266343,"event":"unsubscribe"}"#;
    let messages = recorded(ws).await;
    assert_allowed(
        &messages,
        &[CONF, SUBSCRIBE],
        &[CONF, SUBSCRIBE, UNSUBSCRIBE],
    );
    if recovered {
        assert_resubscribe(&messages, UNSUBSCRIBE, SUBSCRIBE);
    }
}

pub(super) async fn gateio(ws: &MockWsServer, recovered: bool) {
    const SUBSCRIBE: &str =
        r#"{"time":0,"channel":"spot.obu","event":"subscribe","payload":["ob.BTC_USDT.50"]}"#;
    const UNSUBSCRIBE: &str =
        r#"{"time":0,"channel":"spot.obu","event":"unsubscribe","payload":["ob.BTC_USDT.50"]}"#;
    let messages: Vec<_> = recorded(ws)
        .await
        .into_iter()
        .map(|message| {
            let (time, remainder) = message
                .strip_prefix(r#"{"time":"#)
                .and_then(|value| value.split_once(','))
                .expect("Gate.io frame has a leading Unix timestamp");
            time.parse::<u64>().expect("Gate.io Unix seconds");
            format!("{{\"time\":0,{remainder}")
        })
        .collect();
    assert_allowed(&messages, &[SUBSCRIBE], &[SUBSCRIBE, UNSUBSCRIBE]);
    if recovered {
        assert_resubscribe(&messages, UNSUBSCRIBE, SUBSCRIBE);
    }
}
