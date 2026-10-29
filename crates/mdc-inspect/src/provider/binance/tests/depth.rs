//! Native full-diff growth must not be capped by the initial REST snapshot count.
use super::*;
use rust_decimal::Decimal;
use wiremock::matchers::query_param;

#[tokio::test]
async fn retained_book_outgrows_rest_snapshot() {
    let ws = MockWsServer::spawn().await.unwrap();
    let rest = MockRest::start().await;
    let (bids, asks) = depth_levels::arrays(0..1000, "1", true);
    Mock::given(method("GET"))
        .and(path("/api/v3/depth"))
        .and(query_param("symbol", "BTCUSDT"))
        .and(query_param("limit", "1000"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(depth_response_levels(100, &bids, &asks), "application/json"),
        )
        .expect(1)
        .mount(rest.inner())
        .await;
    ws.push_script(Vec::new());
    let (mut rx, task, cancel) = spawn_session(&ws, &rest, crate::config::CHANNEL_CAPACITY).await;
    tokio::time::timeout(Duration::from_secs(10), ws.wait_for_handshakes(1))
        .await
        .unwrap();
    let initial = wait_for_status(&mut rx, DisplayStatus::InSync).await;
    let book = initial.symbols.first().unwrap();
    assert_eq!(book.bids.len(), 1000);
    assert_eq!(book.asks.len(), 1000);
    let quote = book.quote.as_ref().unwrap();
    assert!(!quote.historical);
    assert_eq!(quote.quote.bid, Decimal::from(1_000_000));
    assert_eq!(quote.quote.ask, Decimal::from(2_000_000));

    ws.broadcast_text(depth_delta_with_sizes(
        101, 101, "999000", "3", "2001000", "4",
    ))
    .unwrap();
    let grown = wait_until(&mut rx, |snapshot| {
        snapshot.symbols.first().is_some_and(|book| {
            book.bids.len() == 1001 || book.unavailability.as_deref() == Some("ExceededMaxDepth")
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
    assert_eq!(book.bids.len(), 1001);
    assert_eq!(book.asks.len(), 1001);
    assert_eq!(book.bids.last().unwrap().price, Decimal::from(999_000));
    assert_eq!(book.asks.last().unwrap().price, Decimal::from(2_001_000));
    assert!(!book.quote.as_ref().unwrap().historical);

    let (bids, asks) = depth_levels::arrays(0..1000, "0", true);
    ws.broadcast_text(depth_delta_levels(102, 102, &bids, &asks))
        .unwrap();
    let removed = wait_until(&mut rx, |snapshot| {
        snapshot
            .symbols
            .first()
            .is_some_and(|book| book.bids.len() == 1)
    })
    .await;
    let book = removed.symbols.first().unwrap();
    assert_eq!(book.status, DisplayStatus::InSync);
    assert!(book.unavailability.is_none());
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.asks.len(), 1);
    let quote = book.quote.as_ref().unwrap();
    assert!(!quote.historical);
    assert_eq!(quote.quote.bid, Decimal::from(999_000));
    assert_eq!(quote.quote.ask, Decimal::from(2_001_000));
    assert_eq!(quote.quote.bid_size, Decimal::from(3));
    assert_eq!(quote.quote.ask_size, Decimal::from(4));
    // Earlier published state is immutable, not retroactively marked historical.
    assert_eq!(initial.symbols.first().unwrap().bids.len(), 1000);
    assert!(
        !initial
            .symbols
            .first()
            .unwrap()
            .quote
            .as_ref()
            .unwrap()
            .historical
    );

    cancel.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.status, DisplayStatus::Stopped);
    assert!(outcome.failure.is_none());
    rest.inner().verify().await;
}
