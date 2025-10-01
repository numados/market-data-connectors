//! L3 Property-based serde round-trip verification for canonical market events.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use mdc_core::model::*;
use proptest::prelude::*;
use rust_decimal::Decimal;

fn arb_venue_id() -> impl Strategy<Value = VenueId> {
    prop_oneof![
        Just(VenueId::Binance),
        Just(VenueId::Coinbase),
        Just(VenueId::CoinbaseAdvanced),
        Just(VenueId::Kraken),
        Just(VenueId::Gateio),
        Just(VenueId::Bitstamp),
        Just(VenueId::Bitfinex),
        Just(VenueId::Lmax),
        Just(VenueId::Vwd),
        Just(VenueId::Huobi),
        Just(VenueId::FrankfurterEcb),
    ]
}

fn arb_side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Bid), Just(Side::Ask)]
}

fn arb_decimal() -> impl Strategy<Value = Decimal> {
    (any::<i64>(), 0u32..8).prop_map(|(num, scale)| Decimal::new(num, scale))
}

fn arb_price_level() -> impl Strategy<Value = PriceLevel> {
    (arb_decimal(), arb_decimal()).prop_map(|(price, volume)| PriceLevel::new(price, volume))
}

fn arb_sequence_range() -> impl Strategy<Value = SequenceRange> {
    (any::<u64>(), any::<u32>())
        .prop_map(|(first, delta)| SequenceRange::new(first, first.saturating_add(delta as u64)))
}

fn arb_symbol() -> impl Strategy<Value = Symbol> {
    "[A-Z0-9/_-]{1,16}".prop_map(|s| Symbol::from(s.as_str()))
}

fn arb_trade() -> impl Strategy<Value = Trade> {
    (
        proptest::option::of((
            "[a-z0-9_-]{1,32}".prop_map(|s| TradeNamespace::new(&s).expect("valid ns")),
            "[a-zA-Z0-9_-]{1,64}".prop_map(|s| TradeId::new(&s).expect("valid tid")),
        )),
        arb_decimal(),
        arb_decimal(),
        arb_side(),
    )
        .prop_map(|(id_opt, price, volume, aggressor)| {
            let identity = id_opt.map(|(ns, tid)| NativeTradeIdentity::new(ns, tid));
            Trade::new(identity, price, volume, aggressor)
        })
}

fn arb_disconnect_kind() -> impl Strategy<Value = DisconnectKind> {
    prop_oneof![
        Just(DisconnectKind::Closed),
        Just(DisconnectKind::TransportError),
        Just(DisconnectKind::SilenceTimeout),
        Just(DisconnectKind::VenueRequested),
        Just(DisconnectKind::Paused),
        Just(DisconnectKind::Shutdown),
        Just(DisconnectKind::Panic),
    ]
}

fn arb_source_error_kind() -> impl Strategy<Value = SourceErrorKind> {
    prop_oneof![
        Just(SourceErrorKind::Transport),
        (100u16..600).prop_map(SourceErrorKind::HttpStatus),
        Just(SourceErrorKind::Parse),
        Just(SourceErrorKind::MissingQuote),
        Just(SourceErrorKind::InvalidQuote),
    ]
}

fn arb_integrity_alert() -> impl Strategy<Value = IntegrityAlert> {
    prop_oneof![
        (any::<u64>(), any::<u64>())
            .prop_map(|(expected, received)| IntegrityAlert::GapDetected { expected, received }),
        (any::<u32>(), any::<u32>()).prop_map(|(expected, calculated)| {
            IntegrityAlert::ChecksumMismatch {
                expected,
                calculated,
            }
        }),
        any::<u64>().prop_map(|elapsed_ms| IntegrityAlert::SilenceTimeout { elapsed_ms }),
        any::<u64>().prop_map(|elapsed_ms| IntegrityAlert::HeartbeatTimeout { elapsed_ms }),
        Just(IntegrityAlert::ResyncStarted),
        proptest::option::of(any::<u64>())
            .prop_map(|last_update_id| IntegrityAlert::ResyncCompleted { last_update_id }),
        any::<u64>().prop_map(|retry_in_ms| IntegrityAlert::Quarantined { retry_in_ms }),
        arb_source_error_kind().prop_map(|kind| IntegrityAlert::SourceError { kind }),
        arb_disconnect_kind().prop_map(|reason| IntegrityAlert::ConnectionLost { reason }),
    ]
}

fn arb_market_data_kind() -> impl Strategy<Value = MarketDataKind> {
    prop_oneof![
        (
            proptest::collection::vec(arb_price_level(), 0..5),
            proptest::collection::vec(arb_price_level(), 0..5),
            proptest::option::of(arb_sequence_range())
        )
            .prop_map(|(bids, asks, seq)| {
                MarketDataKind::L2Delta(L2Delta::new(bids, asks, seq))
            }),
        (
            proptest::collection::vec(arb_price_level(), 0..5),
            proptest::collection::vec(arb_price_level(), 0..5),
            proptest::option::of(any::<u64>())
        )
            .prop_map(|(bids, asks, last_id)| {
                MarketDataKind::L2Snapshot(L2Snapshot::new(bids, asks, last_id))
            }),
        arb_trade().prop_map(MarketDataKind::Trade),
        (
            arb_price_level(),
            arb_price_level(),
            proptest::option::of(any::<u64>())
        )
            .prop_map(|(bid, ask, uid)| MarketDataKind::BookTicker(BookTicker::new(bid, ask, uid))),
        arb_decimal().prop_map(|mid| MarketDataKind::ReferenceRate(ReferenceRate::new(mid))),
        arb_integrity_alert().prop_map(MarketDataKind::IntegrityAlert),
    ]
}

fn arb_market_event() -> impl Strategy<Value = MarketEvent<MarketDataKind>> {
    (
        proptest::option::of(any::<u64>().prop_map(UnixNanos)),
        any::<u64>().prop_map(UnixNanos),
        arb_venue_id(),
        arb_symbol(),
        any::<u64>(),
        (any::<[u8; 16]>(), any::<u64>(), any::<u64>()).prop_map(|(run, generation, ord)| {
            DeliveryStamp::new(RunId::from_bytes(run), generation, ord)
        }),
        proptest::option::of(any::<u64>()),
        arb_market_data_kind(),
    )
        .prop_map(
            |(exchange_ts, received_ts, venue, symbol, tag, delivery, source_sequence, kind)| {
                MarketEvent {
                    exchange_ts,
                    received_ts,
                    venue,
                    symbol,
                    tag,
                    delivery,
                    source_sequence,
                    kind,
                }
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn property_market_event_json_roundtrip(event in arb_market_event()) {
        let serialized = serde_json::to_string(&event).expect("serialize MarketEvent");
        let deserialized: MarketEvent<MarketDataKind> =
            serde_json::from_str(&serialized).expect("deserialize MarketEvent");
        prop_assert_eq!(deserialized, event);
    }

    #[test]
    fn property_price_level_decimal_roundtrip(level in arb_price_level()) {
        let serialized = serde_json::to_string(&level).expect("serialize PriceLevel");
        let deserialized: PriceLevel =
            serde_json::from_str(&serialized).expect("deserialize PriceLevel");
        prop_assert_eq!(deserialized, level);
    }
}
