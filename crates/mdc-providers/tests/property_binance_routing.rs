#![cfg(feature = "binance")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Property-based routing and deserialization invariance tests for Binance payloads.

use mdc_core::{PriceLevel, SequenceRange, Side};
use mdc_providers::binance::normalize::{
    normalize_book_ticker, normalize_depth, normalize_snapshot, normalize_trade,
};
use mdc_providers::binance::raw::{
    CombinedStream, ConnectionScratchBuffers, RawDepthSnapshot, RawPayload,
};
use proptest::prelude::*;
use rust_decimal::Decimal;

prop_compose! {
    fn arb_decimal()(val in 1i64..10_000_000, scale in 0u32..6) -> Decimal {
        Decimal::new(val, scale)
    }
}

prop_compose! {
    fn arb_symbol()(s in "[A-Z]{3,8}") -> String {
        s
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_depth_update_routing_and_normalization(
        symbol in arb_symbol(),
        first_id in 1u64..100_000,
        span in 1u64..100,
        bid_p in arb_decimal(),
        bid_v in arb_decimal(),
        ask_p in arb_decimal(),
        ask_v in arb_decimal(),
    ) {
        let last_id = first_id + span;
        let json_str = format!(
            r#"{{"e":"depthUpdate","E":1600000000000,"s":"{symbol}","U":{first_id},"u":{last_id},"b":[["{bid_p}","{bid_v}"]],"a":[["{ask_p}","{ask_v}"]]}}"#
        );

        let mut bytes = json_str.into_bytes();
        let payload: RawPayload<'_> = simd_json::from_slice(&mut bytes).expect("deserialize depthUpdate");

        match payload {
            RawPayload::Depth(update) => {
                prop_assert_eq!(update.symbol, symbol.as_str());
                prop_assert_eq!(update.first_update_id, first_id);
                prop_assert_eq!(update.last_update_id, last_id);

                let mut scratch = ConnectionScratchBuffers::with_capacity(16);
                scratch.fill_from_depth(&update);
                prop_assert_eq!(&scratch.bids, &[PriceLevel::new(bid_p, bid_v)]);
                prop_assert_eq!(&scratch.asks, &[PriceLevel::new(ask_p, ask_v)]);

                let (_ts, delta) = normalize_depth(&update);
                prop_assert_eq!(delta.sequence, Some(SequenceRange::new(first_id, last_id)));
                prop_assert_eq!(delta.bids, vec![PriceLevel::new(bid_p, bid_v)]);
                prop_assert_eq!(delta.asks, vec![PriceLevel::new(ask_p, ask_v)]);
            }
            other => prop_assert!(false, "Expected Depth, got {:?}", other),
        }
    }

    #[test]
    fn property_book_ticker_routing_and_normalization(
        symbol in arb_symbol(),
        update_id in 1u64..1_000_000,
        bid_p in arb_decimal(),
        bid_v in arb_decimal(),
        ask_p in arb_decimal(),
        ask_v in arb_decimal(),
    ) {
        let json_str = format!(
            r#"{{"u":{update_id},"s":"{symbol}","b":"{bid_p}","B":"{bid_v}","a":"{ask_p}","A":"{ask_v}"}}"#
        );

        let mut bytes = json_str.into_bytes();
        let payload: RawPayload<'_> = simd_json::from_slice(&mut bytes).expect("deserialize bookTicker");

        match payload {
            RawPayload::BookTicker(ticker) => {
                prop_assert_eq!(ticker.symbol, symbol.as_str());
                prop_assert_eq!(ticker.update_id, update_id);

                let normalized = normalize_book_ticker(&ticker);
                prop_assert_eq!(normalized.best_bid, PriceLevel::new(bid_p, bid_v));
                prop_assert_eq!(normalized.best_ask, PriceLevel::new(ask_p, ask_v));
                prop_assert_eq!(normalized.update_id, Some(update_id));
            }
            other => prop_assert!(false, "Expected BookTicker, got {:?}", other),
        }
    }

    #[test]
    fn property_trade_routing_and_normalization(
        symbol in arb_symbol(),
        trade_id in 1u64..1_000_000,
        price in arb_decimal(),
        qty in arb_decimal(),
        buyer_order_id in 1u64..100_000,
        seller_order_id in 100_001u64..200_000,
        buyer_is_maker in proptest::bool::ANY,
    ) {
        let json_str = format!(
            r#"{{"e":"trade","E":1672531200000,"s":"{symbol}","t":{trade_id},"p":"{price}","q":"{qty}","b":{buyer_order_id},"a":{seller_order_id},"T":1672531200000,"m":{buyer_is_maker},"M":true}}"#
        );

        let mut bytes = json_str.into_bytes();
        let payload: RawPayload<'_> = simd_json::from_slice(&mut bytes).expect("deserialize trade");

        match payload {
            RawPayload::Trade(trade) => {
                prop_assert_eq!(trade.symbol, symbol.as_str());
                prop_assert_eq!(trade.trade_id, trade_id);

                let (_ts, normalized) = normalize_trade(&trade).expect("normalize trade");
                prop_assert_eq!(normalized.price, price);
                prop_assert_eq!(normalized.volume, qty);
                // When buyer is maker, aggressor is Ask; when buyer is taker, aggressor is Bid
                let expected_aggressor = if buyer_is_maker { Side::Ask } else { Side::Bid };
                prop_assert_eq!(normalized.aggressor, expected_aggressor);
            }
            other => prop_assert!(false, "Expected Trade, got {:?}", other),
        }
    }

    #[test]
    fn property_combined_stream_envelope(
        stream_name in "[a-z0-9_@]{5,20}",
        symbol in arb_symbol(),
        update_id in 1u64..100_000,
    ) {
        let inner_json = format!(
            r#"{{"u":{update_id},"s":"{symbol}","b":"100.0","B":"1.0","a":"101.0","A":"2.0"}}"#
        );
        let envelope = format!(r#"{{"stream":"{stream_name}","data":{inner_json}}}"#);

        let mut bytes = envelope.into_bytes();
        let combined: CombinedStream<'_> = simd_json::from_slice(&mut bytes).expect("deserialize CombinedStream");

        prop_assert_eq!(combined.stream, stream_name.as_str());
        match combined.data {
            RawPayload::BookTicker(ticker) => {
                prop_assert_eq!(ticker.symbol, symbol.as_str());
                prop_assert_eq!(ticker.update_id, update_id);
            }
            other => prop_assert!(false, "Expected BookTicker inside envelope, got {:?}", other),
        }
    }

    #[test]
    fn property_snapshot_normalization(
        last_update_id in 1u64..1_000_000,
        bid_p in arb_decimal(),
        bid_v in arb_decimal(),
        ask_p in arb_decimal(),
        ask_v in arb_decimal(),
    ) {
        let json_str = format!(
            r#"{{"lastUpdateId":{last_update_id},"bids":[["{bid_p}","{bid_v}"]],"asks":[["{ask_p}","{ask_v}"]]}}"#
        );

        let mut bytes = json_str.into_bytes();
        let raw_snapshot: RawDepthSnapshot = simd_json::from_slice(&mut bytes).expect("deserialize snapshot");
        prop_assert_eq!(raw_snapshot.last_update_id, last_update_id);

        let snapshot = normalize_snapshot(&raw_snapshot);
        prop_assert_eq!(snapshot.last_update_id, Some(last_update_id));
        prop_assert_eq!(snapshot.bids, vec![PriceLevel::new(bid_p, bid_v)]);
        prop_assert_eq!(snapshot.asks, vec![PriceLevel::new(ask_p, ask_v)]);
    }
}
