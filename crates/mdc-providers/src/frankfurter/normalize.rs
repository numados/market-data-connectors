//! Normalization, date-to-midnight civil conversion, and batch validation for Frankfurter ECB rates.

use mdc_core::{MdcError, SourceErrorKind, Symbol, UnixNanos};
use rust_decimal::Decimal;
use smallstr::SmallString;

use crate::frankfurter::raw::FrankfurterScratch;

/// Normalized reference rate entry ready for emission to a streaming sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedBatchRate {
    /// Canonical 6-letter currency pair symbol (e.g. `"EURUSD"`).
    pub symbol: Symbol,
    /// Midpoint exchange rate.
    pub mid: Decimal,
    /// UTC midnight timestamp of the observation date.
    pub exchange_ts: UnixNanos,
}

/// Builds a canonical currency pair symbol from 3-letter base and quote codes (e.g. `"EUR"` + `"USD"` -> `"EURUSD"`).
pub fn build_pair_symbol(base: &str, quote: &str) -> Option<Symbol> {
    if base.len() == 3 && quote.len() == 3 && base.is_ascii() && quote.is_ascii() {
        let mut sym = SmallString::new();
        sym.push_str(base);
        sym.push_str(quote);
        Some(sym)
    } else {
        None
    }
}

/// Checks whether a year is a leap year in the Gregorian calendar.
pub const fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Returns the number of days in a given Gregorian month (1-indexed).
pub const fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Computes the number of days since the Unix epoch (1970-01-01) for a Gregorian date.
///
/// Uses Howard Hinnant's civil day count algorithm.
pub const fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = year - (month <= 2) as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

/// Converts a date string in `YYYY-MM-DD` format to a conservative UTC midnight [`UnixNanos`].
///
/// Returns an error if the string is malformed, has invalid month/day, or precedes the Unix epoch.
pub fn parse_date_to_utc_midnight(date_str: &str) -> Result<UnixNanos, MdcError> {
    let bytes = date_str.as_bytes();
    if bytes.len() != 10 {
        return Err(MdcError::Protocol(format!(
            "invalid date string length for YYYY-MM-DD: {date_str}"
        )));
    }

    if bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(MdcError::Protocol(format!(
            "malformed date separators in YYYY-MM-DD: {date_str}"
        )));
    }

    let parse_digits = |start: usize, len: usize| -> Result<u32, MdcError> {
        let mut val = 0u32;
        for i in 0..len {
            let b = bytes[start + i];
            if !b.is_ascii_digit() {
                return Err(MdcError::Protocol(format!(
                    "non-digit character in date: {date_str}"
                )));
            }
            val = val * 10 + (b - b'0') as u32;
        }
        Ok(val)
    };

    let year = parse_digits(0, 4)? as i64;
    let month = parse_digits(5, 2)?;
    let day = parse_digits(8, 2)?;

    if year < 1970 {
        return Err(MdcError::Protocol(format!(
            "observation date precedes Unix epoch (1970): {date_str}"
        )));
    }

    if !(1..=12).contains(&month) {
        return Err(MdcError::Protocol(format!(
            "invalid month {month} in date: {date_str}"
        )));
    }

    let max_days = days_in_month(year, month);
    if day == 0 || day > max_days {
        return Err(MdcError::Protocol(format!(
            "invalid day {day} for month {month}/{year} in date: {date_str}"
        )));
    }

    let days = days_from_civil(year, month, day);
    if days < 0 {
        return Err(MdcError::Protocol(format!(
            "civil days negative for date: {date_str}"
        )));
    }

    let secs = (days as u64)
        .checked_mul(86_400)
        .ok_or_else(|| MdcError::Protocol("overflow converting days to seconds".into()))?;
    let nanos = secs
        .checked_mul(1_000_000_000)
        .ok_or_else(|| MdcError::Protocol("overflow converting seconds to nanoseconds".into()))?;

    Ok(UnixNanos(nanos))
}

/// Validates and normalizes all rate records in `scratch.rates` against configured currencies.
///
/// Enforces batch normalization invariants:
/// - Skips `base == quote` identity rows.
/// - Requires every configured non-identity quote code to appear exactly once.
/// - Rejects response if any unknown, unrequested, duplicate, or nonpositive rate is present.
/// - Rejects response if date parsing fails.
/// - Sorts normalized rates in canonical symbol order.
pub fn normalize_batch_from_scratch(
    base: &str,
    configured_quotes: &[String],
    scratch: &FrankfurterScratch,
    out: &mut Vec<NormalizedBatchRate>,
) -> Result<(), SourceErrorKind> {
    out.clear();

    if scratch.rates.is_empty() {
        return Err(SourceErrorKind::MissingQuote);
    }

    let is_configured_non_identity = |code: &str| -> bool {
        code != base && configured_quotes.iter().any(|q| q.as_str() == code)
    };

    for row in &scratch.rates {
        // Skip base == quote identity rows
        if row.base.as_str() == row.quote.as_str() {
            continue;
        }

        // Validate base matches
        if row.base.as_str() != base {
            return Err(SourceErrorKind::InvalidQuote);
        }

        let quote_code = row.quote.as_str();

        // Must be in the configured non-identity quote set
        if !is_configured_non_identity(quote_code) {
            return Err(SourceErrorKind::InvalidQuote);
        }

        // Must have a positive, exact rate
        let rate_val = match row.rate {
            Some(r) if r > Decimal::ZERO => r,
            _ => return Err(SourceErrorKind::InvalidQuote),
        };

        // Parse date
        let exchange_ts =
            parse_date_to_utc_midnight(row.date.as_str()).map_err(|_| SourceErrorKind::Parse)?;

        // Build canonical symbol
        let symbol = build_pair_symbol(base, quote_code).ok_or(SourceErrorKind::InvalidQuote)?;

        // Duplicate check: ensure this symbol hasn't already been added
        if out.iter().any(|item| item.symbol == symbol) {
            return Err(SourceErrorKind::InvalidQuote);
        }

        out.push(NormalizedBatchRate {
            symbol,
            mid: rate_val,
            exchange_ts,
        });
    }

    let expected_count = configured_quotes
        .iter()
        .filter(|q| q.as_str() != base)
        .count();

    // Ensure every configured non-identity quote appeared exactly once
    if out.len() != expected_count {
        return Err(SourceErrorKind::MissingQuote);
    }

    // Sort in canonical alphabetical symbol order
    out.sort_by(|a, b| a.symbol.as_str().cmp(b.symbol.as_str()));

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::frankfurter::raw::FrankfurterScratchSeed;
    use mdc_testkit::load_fixture;
    use serde::de::DeserializeSeed;
    use std::str::FromStr;

    #[test]
    fn test_date_to_utc_midnight_known_dates() {
        // 1970-01-01 is epoch 0
        let epoch = parse_date_to_utc_midnight("1970-01-01").unwrap();
        assert_eq!(epoch.as_nanos(), 0);

        // 1970-01-02 is 86,400 seconds * 1e9 nanos
        let day1 = parse_date_to_utc_midnight("1970-01-02").unwrap();
        assert_eq!(day1.as_nanos(), 86_400 * 1_000_000_000);

        // 2026-03-25
        let ts = parse_date_to_utc_midnight("2026-03-25").unwrap();
        // 2026-03-25 is 20,537 days since 1970-01-01
        assert_eq!(ts.as_nanos(), 20_537 * 86_400 * 1_000_000_000);
    }

    #[test]
    fn test_leap_year_handling() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2023));

        // 2024 is leap year, 2024-02-29 is valid
        assert!(parse_date_to_utc_midnight("2024-02-29").is_ok());

        // 2023 is not leap year, 2023-02-29 is invalid
        assert!(parse_date_to_utc_midnight("2023-02-29").is_err());
    }

    #[test]
    fn test_invalid_dates_rejected() {
        assert!(parse_date_to_utc_midnight("1969-12-31").is_err()); // Precedes 1970
        assert!(parse_date_to_utc_midnight("2026-13-01").is_err()); // Month 13
        assert!(parse_date_to_utc_midnight("2026-04-31").is_err()); // April has 30 days
        assert!(parse_date_to_utc_midnight("invalid-dt").is_err());
        assert!(parse_date_to_utc_midnight("2026/03/25").is_err());
    }

    #[test]
    fn test_normalize_valid_batch_with_identity_row() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/identity_row.json")
                .unwrap();
        let mut scratch = FrankfurterScratch::with_capacity(16);
        let mut d = serde_json::Deserializer::from_str(&json);
        FrankfurterScratchSeed::new(&mut scratch)
            .deserialize(&mut d)
            .unwrap();

        let quotes = vec!["GBP".to_string(), "USD".to_string()];
        let mut out = Vec::new();
        normalize_batch_from_scratch("EUR", &quotes, &scratch, &mut out).unwrap();

        // 2 non-identity quotes sorted in canonical order: EURGBP, EURUSD
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].symbol.as_str(), "EURGBP");
        assert_eq!(out[0].mid, Decimal::from_str("0.8623").unwrap());
        assert_eq!(out[1].symbol.as_str(), "EURUSD");
        assert_eq!(out[1].mid, Decimal::from_str("1.089").unwrap());
    }

    #[test]
    fn test_normalize_missing_pair_fails() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/missing_pair.json")
                .unwrap();
        let mut scratch = FrankfurterScratch::with_capacity(16);
        let mut d = serde_json::Deserializer::from_str(&json);
        FrankfurterScratchSeed::new(&mut scratch)
            .deserialize(&mut d)
            .unwrap();

        let quotes = vec!["GBP".to_string(), "USD".to_string()];
        let mut out = Vec::new();
        let res = normalize_batch_from_scratch("EUR", &quotes, &scratch, &mut out);
        assert_eq!(res, Err(SourceErrorKind::MissingQuote));
    }

    #[test]
    fn test_normalize_invalid_rate_fails() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/invalid_rate.json")
                .unwrap();
        let mut scratch = FrankfurterScratch::with_capacity(16);
        let mut d = serde_json::Deserializer::from_str(&json);
        FrankfurterScratchSeed::new(&mut scratch)
            .deserialize(&mut d)
            .unwrap();

        let quotes = vec!["GBP".to_string(), "USD".to_string()];
        let mut out = Vec::new();
        let res = normalize_batch_from_scratch("EUR", &quotes, &scratch, &mut out);
        assert_eq!(res, Err(SourceErrorKind::InvalidQuote));
    }

    #[test]
    fn test_normalize_conflicting_pair_fails() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/conflicting_pair.json")
                .unwrap();
        let mut scratch = FrankfurterScratch::with_capacity(16);
        let mut d = serde_json::Deserializer::from_str(&json);
        FrankfurterScratchSeed::new(&mut scratch)
            .deserialize(&mut d)
            .unwrap();

        let quotes = vec!["USD".to_string()];
        let mut out = Vec::new();
        let res = normalize_batch_from_scratch("EUR", &quotes, &scratch, &mut out);
        assert_eq!(res, Err(SourceErrorKind::InvalidQuote));
    }

    #[test]
    fn test_normalize_unrequested_pair_fails() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/frankfurter/reference_rates.json")
                .unwrap();
        let mut scratch = FrankfurterScratch::with_capacity(16);
        let mut d = serde_json::Deserializer::from_str(&json);
        FrankfurterScratchSeed::new(&mut scratch)
            .deserialize(&mut d)
            .unwrap();

        // Config only requests USD, but fixture contains GBP as well
        let quotes = vec!["USD".to_string()];
        let mut out = Vec::new();
        let res = normalize_batch_from_scratch("EUR", &quotes, &scratch, &mut out);
        assert_eq!(res, Err(SourceErrorKind::InvalidQuote));
    }
}
