//! Per-venue recovery scenarios using the shared offline harness.
use super::*;

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
    subscriptions::bitstamp(&ws).await;
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
    subscriptions::bitstamp(&ws).await;
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
    subscriptions::huobi(&ws).await;
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
    subscriptions::huobi(&ws).await;
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
    subscriptions::coinbase_advanced(&ws, false).await;
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
    subscriptions::coinbase_advanced(&ws, true).await;

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
    subscriptions::lmax(&ws).await;
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
    subscriptions::lmax(&ws).await;
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
