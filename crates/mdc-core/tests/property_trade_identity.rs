#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Property tests for NativeTradeIdentity, TradeNamespace, TradeId, DeliveryStamp, and capabilities.

use mdc_core::model::{
    CapabilityBounds, CapabilityError, ConsumerRequirements, DeliveryStamp, DiscoveryCapability,
    IdentityScope, IntegrityCapability, MarketEvent, NativeTradeIdentity, ProviderCapabilities,
    QuoteMode, ReplayCapability, RunId, Side, SideSemantics, Symbol, TimestampCapability, Trade,
    TradeCapability, TradeId, TradeIdentityError, TradeNamespace, UnixNanos, VenueId,
};
use proptest::prelude::*;
use rust_decimal::Decimal;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_trade_id_length_boundary(
        valid_len in 1usize..=128,
        overflow_len in 129usize..=256,
    ) {
        let valid_str = "x".repeat(valid_len);
        let id_res = TradeId::new(&valid_str);
        prop_assert!(id_res.is_ok());
        let id = id_res.unwrap();
        prop_assert_eq!(id.as_str(), valid_str.as_str());

        let overflow_str = "x".repeat(overflow_len);
        let overflow_res = TradeId::new(&overflow_str);
        let is_too_long = matches!(
            overflow_res,
            Err(TradeIdentityError::TooLong { max: 128, actual }) if actual == overflow_len
        );
        prop_assert!(is_too_long);
    }

    #[test]
    fn property_trade_namespace_length_boundary(
        valid_len in 1usize..=64,
        overflow_len in 65usize..=128,
    ) {
        let valid_str = "n".repeat(valid_len);
        let ns_res = TradeNamespace::new(&valid_str);
        prop_assert!(ns_res.is_ok());
        let ns = ns_res.unwrap();
        prop_assert_eq!(ns.as_str(), valid_str.as_str());

        let overflow_str = "n".repeat(overflow_len);
        let overflow_res = TradeNamespace::new(&overflow_str);
        let is_too_long = matches!(
            overflow_res,
            Err(TradeIdentityError::TooLong { max: 64, actual }) if actual == overflow_len
        );
        prop_assert!(is_too_long);
    }

    #[test]
    fn property_empty_identifiers_rejected(
        _dummy in 0u8..1,
    ) {
        prop_assert_eq!(TradeId::new(""), Err(TradeIdentityError::Empty));
        prop_assert_eq!(TradeNamespace::new(""), Err(TradeIdentityError::Empty));
    }

    #[test]
    fn property_non_ascii_rejected(
        non_ascii_char in "[^\x00-\x7F]",
    ) {
        let s = format!("trade_{non_ascii_char}");
        prop_assert_eq!(TradeId::new(&s), Err(TradeIdentityError::NonAscii));
        prop_assert_eq!(TradeNamespace::new(&s), Err(TradeIdentityError::NonAscii));
    }

    #[test]
    fn property_control_characters_rejected(
        ctrl_byte in 0u8..32u8,
    ) {
        let ctrl_char = ctrl_byte as char;
        let s = format!("id_{ctrl_char}");
        prop_assert_eq!(TradeId::new(&s), Err(TradeIdentityError::ControlCharacter));
        prop_assert_eq!(TradeNamespace::new(&s), Err(TradeIdentityError::ControlCharacter));
    }

    #[test]
    fn property_distinct_trade_identities_not_equal(
        id1 in "[a-zA-Z0-9]{1,32}",
        id2 in "[a-zA-Z0-9]{33,64}",
        price in 1u32..100_000,
        volume in 1u32..1_000,
    ) {
        let ns = TradeNamespace::new("binance").unwrap();
        let tid1 = TradeId::new(&id1).unwrap();
        let tid2 = TradeId::new(&id2).unwrap();

        let identity1 = NativeTradeIdentity::new(ns.clone(), tid1);
        let identity2 = NativeTradeIdentity::new(ns, tid2);
        prop_assert_ne!(&identity1, &identity2);

        let trade1 = Trade::new(
            Some(identity1),
            Decimal::from(price),
            Decimal::from(volume),
            Side::Bid,
        );
        let trade2 = Trade::new(
            Some(identity2),
            Decimal::from(price),
            Decimal::from(volume),
            Side::Bid,
        );
        prop_assert_ne!(trade1, trade2);
    }

    #[test]
    fn property_event_map_preserves_envelope_provenance(
        exchange_ts in 1_000_000_000u64..2_000_000_000,
        received_ts in 1_000_000_000u64..2_000_000_000,
        ordinal in 1u64..1_000_000,
        generation in 1u64..100,
        tag in 0u64..1_000,
    ) {
        let run_id = RunId::from_bytes([0x42; 16]);
        let delivery = DeliveryStamp::new(run_id, generation, ordinal);

        let original_event = MarketEvent {
            exchange_ts: Some(UnixNanos(exchange_ts)),
            received_ts: UnixNanos(received_ts),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag,
            delivery,
            source_sequence: Some(42),
            kind: "payload_a",
        };

        let mapped = original_event.clone().map(|s| format!("{s}_transformed"));

        prop_assert_eq!(mapped.exchange_ts, original_event.exchange_ts);
        prop_assert_eq!(mapped.received_ts, original_event.received_ts);
        prop_assert_eq!(mapped.venue, original_event.venue);
        prop_assert_eq!(mapped.symbol, original_event.symbol);
        prop_assert_eq!(mapped.tag, original_event.tag);
        prop_assert_eq!(mapped.delivery, original_event.delivery);
        prop_assert_eq!(mapped.source_sequence, original_event.source_sequence);
        prop_assert_eq!(mapped.kind, "payload_a_transformed");
    }

    #[test]
    fn property_capabilities_check_requirements_exhaustive(
        require_trades in any::<bool>(),
        require_side in any::<bool>(),
    ) {
        let verified_caps = ProviderCapabilities {
            quote_modes: vec![QuoteMode::NativeTicker, QuoteMode::L2Projection],
            trades: TradeCapability::Verified {
                identity_scope: IdentityScope::PerMarket,
                namespace_scheme: "exchange".to_string(),
                side_semantics: SideSemantics::AggressorTaker,
            },
            timestamps: TimestampCapability::ExchangeMilliseconds,
            integrity: IntegrityCapability::SequenceNumbers,
            replay: ReplayCapability::None,
            bounds: CapabilityBounds::default(),
            discovery: DiscoveryCapability::from(true),
            rest_quote: true.into(),
        };

        let req = ConsumerRequirements {
            quote_mode: QuoteMode::NativeTicker,
            require_stable_trades: require_trades,
            require_verified_side: require_side,
        };

        prop_assert!(verified_caps.check_requirements(&req).is_ok());

        // Unsupported trades provider
        let unsupported_caps = ProviderCapabilities {
            quote_modes: vec![QuoteMode::NativeTicker],
            trades: TradeCapability::Unsupported,
            timestamps: TimestampCapability::ExchangeMilliseconds,
            integrity: IntegrityCapability::SequenceNumbers,
            replay: ReplayCapability::None,
            bounds: CapabilityBounds::default(),
            discovery: DiscoveryCapability::from(false),
            rest_quote: false.into(),
        };

        if require_trades {
            let res = unsupported_caps.check_requirements(&req);
            let is_unstable = matches!(res, Err(CapabilityError::UnstableTrades { .. }));
            prop_assert!(is_unstable);
        }
    }
}
