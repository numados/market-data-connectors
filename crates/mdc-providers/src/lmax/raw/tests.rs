use super::*;
use mdc_testkit::load_fixture;

const SNAPSHOT_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/lmax/order_book_snapshot.json";
const EMPTY_SIDE_FIXTURE: &str =
    "crates/mdc-providers/tests/fixtures/lmax/order_book_snapshot_empty_side.json";
const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/lmax/trade.json";
const SUBSCRIPTIONS_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/lmax/subscriptions.json";
const ERROR_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/lmax/error_too_many_requests.json";

#[test]
fn test_peek_frame_kind() {
    let snap_str = load_fixture(SNAPSHOT_FIXTURE).unwrap();
    let empty_side_str = load_fixture(EMPTY_SIDE_FIXTURE).unwrap();
    let trade_str = load_fixture(TRADE_FIXTURE).unwrap();
    let subs_str = load_fixture(SUBSCRIPTIONS_FIXTURE).unwrap();
    let err_str = load_fixture(ERROR_FIXTURE).unwrap();

    assert_eq!(
        peek_frame_kind(snap_str.as_bytes()),
        LmaxFrameKind::OrderBook
    );
    assert_eq!(
        peek_frame_kind(empty_side_str.as_bytes()),
        LmaxFrameKind::OrderBook
    );
    assert_eq!(peek_frame_kind(trade_str.as_bytes()), LmaxFrameKind::Trade);
    assert_eq!(peek_frame_kind(subs_str.as_bytes()), LmaxFrameKind::Control);
    assert_eq!(peek_frame_kind(err_str.as_bytes()), LmaxFrameKind::Control);
    assert_eq!(peek_frame_kind(b"{}"), LmaxFrameKind::Unknown);
}

#[test]
fn test_parse_order_book_snapshot_fixture() {
    let snap_str = load_fixture(SNAPSHOT_FIXTURE).unwrap();
    let mut scratch = LmaxConnectionScratch::with_capacity(16);

    let mut de = serde_json::Deserializer::from_str(&snap_str);
    let header = LmaxBookFrameSeed::new(&mut scratch)
        .deserialize(&mut de)
        .unwrap();

    assert_eq!(header.instrument_id, "btc-usd");
    assert_eq!(header.timestamp, "2026-01-01T00:00:00.123Z");
    assert_eq!(scratch.bids.len(), 2);
    assert_eq!(scratch.asks.len(), 2);
    assert_eq!(scratch.bids[0].price, Decimal::from(50000));
    assert_eq!(scratch.asks[0].price, Decimal::from(50001));
}

#[test]
fn test_parse_empty_side_fixture() {
    let snap_str = load_fixture(EMPTY_SIDE_FIXTURE).unwrap();
    let mut scratch = LmaxConnectionScratch::with_capacity(16);

    let mut de = serde_json::Deserializer::from_str(&snap_str);
    let header = LmaxBookFrameSeed::new(&mut scratch)
        .deserialize(&mut de)
        .unwrap();

    assert_eq!(header.instrument_id, "btc-usd");
    assert_eq!(scratch.bids.len(), 1);
    assert_eq!(scratch.asks.len(), 0);
}

#[test]
fn test_parse_trade_fixture() {
    let trade_str = load_fixture(TRADE_FIXTURE).unwrap();
    let trade: RawTrade<'_> = serde_json::from_str(&trade_str).unwrap();

    assert_eq!(trade.instrument_id, "btc-usd");
    assert_eq!(trade.timestamp, "2026-01-01T00:00:00.456Z");
    assert_eq!(trade.last_price, Decimal::from(50000));
    assert_eq!(
        trade.last_quantity,
        Decimal::from_str_exact("0.5000").unwrap()
    );
}

#[test]
fn test_parse_subscriptions_fixture() {
    let subs_str = load_fixture(SUBSCRIPTIONS_FIXTURE).unwrap();
    let ctrl: RawControl<'_> = serde_json::from_str(&subs_str).unwrap();

    assert_eq!(ctrl.kind, "SUBSCRIPTIONS");
    assert!(ctrl.error_code.is_none());
    let channels = ctrl.channels.unwrap();
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].name, "ORDER_BOOK");
    assert_eq!(channels[0].instruments, vec!["btc-usd", "eth-usd"]);
}

#[test]
fn test_parse_error_too_many_requests_fixture() {
    let err_str = load_fixture(ERROR_FIXTURE).unwrap();
    let ctrl: RawControl<'_> = serde_json::from_str(&err_str).unwrap();

    assert_eq!(ctrl.kind, "ERROR");
    assert_eq!(ctrl.error_code, Some("TOO_MANY_REQUESTS"));
    assert_eq!(
        ctrl.error_message,
        Some("The threshold for requests has been exceeded")
    );
}

#[test]
fn test_book_frame_seed_zero_alloc() {
    let snap_str = load_fixture(SNAPSHOT_FIXTURE).unwrap();
    let mut scratch = LmaxConnectionScratch::with_capacity(16);

    // Warm up
    let mut de = serde_json::Deserializer::from_str(&snap_str);
    let _ = LmaxBookFrameSeed::new(&mut scratch).deserialize(&mut de);

    let allocs = allocation_counter::measure(|| {
        let mut de = serde_json::Deserializer::from_str(&snap_str);
        let _ = LmaxBookFrameSeed::new(&mut scratch).deserialize(&mut de);
    });

    assert_eq!(
        allocs.count_total, 0,
        "LmaxBookFrameSeed must not allocate after warm-up"
    );
}

#[test]
fn typed_book_dispatch_is_order_independent() {
    let modern = include_str!("../../../tests/fixtures/lmax/order_book_snapshot_typed.json");
    assert_eq!(peek_frame_kind(modern.as_bytes()), LmaxFrameKind::OrderBook);
    for frame in [
        r#"{"type":"ORDER_BOOK","bids":[]}"#,
        r#"{"bids":[],"type":"ORDER_BOOK"}"#,
        r#"{"status":"OPEN","type":"ORDER_BOOK","bids":[]}"#,
        r#"{"status":{"type":"ERROR"},"bids":[],"type":"ORDER_BOOK"}"#,
    ] {
        assert_eq!(
            peek_frame_kind(frame.as_bytes()),
            LmaxFrameKind::OrderBook,
            "{frame}"
        );
    }
}

#[test]
fn typed_book_dispatch_and_decode_allocate_nothing_after_warmup() {
    let frame = include_str!("../../../tests/fixtures/lmax/order_book_snapshot_typed.json");
    let mut scratch = LmaxConnectionScratch::with_capacity(4);
    let mut de = serde_json::Deserializer::from_str(frame);
    LmaxBookFrameSeed::new(&mut scratch)
        .deserialize(&mut de)
        .unwrap();
    let allocs = allocation_counter::measure(|| {
        assert_eq!(peek_frame_kind(frame.as_bytes()), LmaxFrameKind::OrderBook);
        let mut de = serde_json::Deserializer::from_str(frame);
        LmaxBookFrameSeed::new(&mut scratch)
            .deserialize(&mut de)
            .unwrap();
    });
    assert_eq!(
        allocs.count_total, 0,
        "type/status metadata must be skipped without allocation"
    );
}

#[test]
fn control_and_invalid_type_values_never_dispatch_as_typed_books() {
    for frame in [
        r#"{"type":"SUBSCRIPTIONS","channels":[{"name":"ORDER_BOOK"}]}"#,
        r#"{"type":"ERROR","error_code":"TOO_MANY_REQUESTS"}"#,
        r#"{"type":"FUTURE_CONTROL"}"#,
    ] {
        assert_eq!(peek_frame_kind(frame.as_bytes()), LmaxFrameKind::Control);
    }
    for frame in [
        r#"{"type":null}"#,
        r#"{"type":42}"#,
        r#"{"type":{"nested":"ORDER_BOOK"}}"#,
        r#"{"type":"#,
    ] {
        assert_eq!(
            peek_frame_kind(frame.as_bytes()),
            LmaxFrameKind::Unknown,
            "{frame}"
        );
    }
}
