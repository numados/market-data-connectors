#![cfg(feature = "frankfurter")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mdc_providers::frankfurter::normalize::{
    days_from_civil, days_in_month, is_leap_year, parse_date_to_utc_midnight,
};
use proptest::prelude::*;
use rust_decimal::Decimal;
use std::str::FromStr;

prop_compose! {
    fn arb_valid_civil_date()(year in 1970i64..2100i64, month in 1u32..=12u32)
        (day in 1u32..=days_in_month(year, month), year in Just(year), month in Just(month))
        -> (i64, u32, u32) {
        (year, month, day)
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_valid_date_converts_to_monotonic_utc_midnight((year, month, day) in arb_valid_civil_date()) {
        let date_str = format!("{year:04}-{month:02}-{day:02}");
        let ts = parse_date_to_utc_midnight(&date_str);
        prop_assert!(ts.is_ok(), "failed to parse valid date: {}", date_str);

        let nanos = ts.unwrap().as_nanos();
        let days = days_from_civil(year, month, day);
        prop_assert!(days >= 0);
        prop_assert_eq!(nanos, (days as u64) * 86_400 * 1_000_000_000);
    }

    #[test]
    fn prop_consecutive_days_differ_by_exact_day_nanos((year, month, day) in arb_valid_civil_date()) {
        let d1 = days_from_civil(year, month, day);
        let max_d = days_in_month(year, month);

        let (next_y, next_m, next_d) = if day < max_d {
            (year, month, day + 1)
        } else if month < 12 {
            (year, month + 1, 1)
        } else {
            (year + 1, 1, 1)
        };

        let d2 = days_from_civil(next_y, next_m, next_d);
        prop_assert_eq!(d2, d1 + 1);

        let s1 = format!("{year:04}-{month:02}-{day:02}");
        let s2 = format!("{next_y:04}-{next_m:02}-{next_d:02}");
        let t1 = parse_date_to_utc_midnight(&s1).unwrap().as_nanos();
        let t2 = parse_date_to_utc_midnight(&s2).unwrap().as_nanos();
        prop_assert_eq!(t2 - t1, 86_400 * 1_000_000_000);
    }

    #[test]
    fn prop_leap_year_february_29(year in 1970i64..2400i64) {
        let s = format!("{year:04}-02-29");
        let res = parse_date_to_utc_midnight(&s);
        if is_leap_year(year) {
            prop_assert!(res.is_ok(), "leap year {} must accept Feb 29", year);
        } else {
            prop_assert!(res.is_err(), "non-leap year {} must reject Feb 29", year);
        }
    }

    #[test]
    fn prop_invalid_day_bounds_always_rejected(
        year in 1970i64..2100i64,
        month in 1u32..=12u32,
    ) {
        // Day 0
        let s0 = format!("{year:04}-{month:02}-00");
        prop_assert!(parse_date_to_utc_midnight(&s0).is_err());

        // Day > max
        let max_d = days_in_month(year, month);
        let s_over = format!("{year:04}-{month:02}-{:02}", max_d + 1);
        prop_assert!(parse_date_to_utc_midnight(&s_over).is_err());
    }

    #[test]
    fn prop_exact_decimal_rate_tokens(
        mantissa in 1u64..10_000_000u64,
        scale in 0u32..10u32,
    ) {
        let expected = Decimal::new(mantissa as i64, scale);
        let token = expected.to_string();
        let parsed = Decimal::from_str(&token);
        prop_assert_eq!(parsed, Ok(expected));
    }

    #[test]
    fn prop_scientific_notation_exact_representability(
        mantissa in 1u32..100u32,
        exp in 1u32..8u32,
    ) {
        let token = format!("{mantissa}e-{exp}");
        let parsed = Decimal::from_str(&token);
        prop_assert!(parsed.is_ok());
        let dec = parsed.unwrap();
        prop_assert!(dec > Decimal::ZERO);
    }
}
