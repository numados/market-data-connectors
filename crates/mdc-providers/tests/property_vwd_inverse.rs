#![cfg(feature = "vwd")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mdc_providers::vwd::config::VwdSymbol;

use mdc_providers::vwd::normalize::{
    normalize_symbol_from_scratch, normalize_symbol_quote, round_to_significant_digits,
};
use mdc_providers::vwd::raw::{ParsedVwdQuote, RawQuote, RawRow, RawSnapQuote, VwdScratch};
use proptest::prelude::*;
use rust_decimal::Decimal;
use serde_json::value::RawValue;
use smallstr::SmallString;

fn arb_positive_decimal() -> impl Strategy<Value = Decimal> {
    (1u32..10_000_000u32, 0u32..6u32)
        .prop_map(|(mantissa, scale)| Decimal::new(mantissa as i64, scale))
        .prop_filter("strictly positive", |d| *d > Decimal::ZERO)
}

fn arb_bid_ask() -> impl Strategy<Value = (Decimal, Decimal)> {
    (arb_positive_decimal(), arb_positive_decimal()).prop_map(|(a, b)| {
        if a < b {
            (a, b)
        } else if a > b {
            (b, a)
        } else {
            (a, a + Decimal::new(1, 4))
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn property_inverse_preserves_ordering((bid, ask) in arb_bid_ask()) {
        prop_assume!(bid > Decimal::ZERO);
        prop_assume!(ask > bid);

        // Given inverse market where raw quote has (bid, ask)
        // Synthesizing direct market gives:
        // direct_bid = 1 / ask
        // direct_ask = 1 / bid
        let direct_bid = Decimal::ONE.checked_div(ask);
        let direct_ask = Decimal::ONE.checked_div(bid);

        if let (Some(db), Some(da)) = (direct_bid, direct_ask) {
            let rounded_bid = round_to_significant_digits(db, 6).unwrap();
            let rounded_ask = round_to_significant_digits(da, 6).unwrap();

            // Direct bid must be <= direct ask
            prop_assert!(rounded_bid <= rounded_ask, "inverted bid {rounded_bid} must be <= ask {rounded_ask}");
        }
    }

    #[test]
    fn property_scale_preserves_ordering(
        (bid, ask) in arb_bid_ask(),
        scale_pct in 1u32..500u32,
    ) {
        let scale = Decimal::new(scale_pct as i64, 2); // 0.01 .. 5.00
        let scaled_bid = bid * scale;
        let scaled_ask = ask * scale;
        prop_assert!(scaled_bid < scaled_ask, "scaled bid {scaled_bid} must be < ask {scaled_ask}");
    }

    #[test]
    fn property_scratch_normalization_matches_raw_quote(
        (bid, ask) in arb_bid_ask(),
    ) {
        let sym = VwdSymbol::with_inverse("EURUSD");
        let bid_str = bid.to_string();
        let ask_str = ask.to_string();

        let raw_bid_rv = RawValue::from_string(bid_str.clone()).unwrap();
        let raw_ask_rv = RawValue::from_string(ask_str.clone()).unwrap();

        let raw_rows = vec![
            RawRow {
                snap_quote: Some(RawSnapQuote {
                    instrument_key: Some("EURUSD.XRATE.SPOT~X"),
                    bid: Some(RawQuote {
                        price: Some(&raw_bid_rv),
                        date_time: Some("2026-09-29T10:00:00.000+02:00"),
                    }),
                    ask: Some(RawQuote {
                        price: Some(&raw_ask_rv),
                        date_time: Some("2026-09-29T10:00:01.000+02:00"),
                    }),
                    last: None,
                }),
            },
        ];

        let mut scratch = VwdScratch::with_capacity(4);
        scratch.quotes.push(ParsedVwdQuote {
            instrument_key: SmallString::from("EURUSD.XRATE.SPOT~X"),
            bid_price: Some(bid),
            bid_time: Some(SmallString::from("2026-09-29T10:00:00.000+02:00")),
            ask_price: Some(ask),
            ask_time: Some(SmallString::from("2026-09-29T10:00:01.000+02:00")),
        });

        let norm_raw = normalize_symbol_quote(&sym, &raw_rows, 6).unwrap();
        let norm_scratch = normalize_symbol_from_scratch(&sym, &scratch, 6).unwrap();

        prop_assert_eq!(norm_raw, norm_scratch);
    }
}
