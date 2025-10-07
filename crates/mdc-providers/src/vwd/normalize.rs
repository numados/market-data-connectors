//! Quote normalization, symbol key construction, and timestamp parsing for VWD.

use std::str::FromStr;

use mdc_core::{MdcError, SourceErrorKind, Symbol, UnixNanos};
use rust_decimal::Decimal;

use crate::vwd::config::VwdSymbol;
use crate::vwd::raw::RawRow;

/// Normalized quote containing calculated bid/ask levels and provenance timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedQuote {
    /// Canonical symbol (e.g. `"EURUSD"`).
    pub symbol: Symbol,
    /// Normalized best bid price.
    pub bid: Decimal,
    /// Normalized best ask price.
    pub ask: Decimal,
    /// Minimum of used bid and ask timestamps, or `None` if either is missing.
    pub exchange_ts: Option<UnixNanos>,
    /// Whether this quote was synthesized from an inverse pair.
    pub is_inverse: bool,
}

use smallstr::SmallString;

/// Builds the direct VWD instrument key for a 6-letter FX symbol (e.g. `"EURUSD.XRATE.SPOT~X"`).
pub fn build_instrument_key(symbol: &str) -> Option<SmallString<[u8; 32]>> {
    if symbol.len() == 6 && symbol.is_ascii() {
        let mut key = SmallString::new();
        key.push_str(symbol);
        key.push_str(".XRATE.SPOT~X");
        Some(key)
    } else {
        None
    }
}

/// Builds the inverse VWD instrument key for a 6-letter FX symbol (e.g. `"USDEUR.XRATE.SPOT~X"`).
pub fn build_inverse_instrument_key(symbol: &str) -> Option<SmallString<[u8; 32]>> {
    if symbol.len() == 6 && symbol.is_ascii() {
        let mut key = SmallString::new();
        key.push_str(&symbol[3..6]);
        key.push_str(&symbol[0..3]);
        key.push_str(".XRATE.SPOT~X");
        Some(key)
    } else {
        None
    }
}

/// Normalizes a configured symbol from a set of SNAPQUOTE response rows.
pub fn normalize_symbol_quote(
    vwd_sym: &VwdSymbol,
    rows: &[RawRow<'_>],
    significant_digits: u32,
) -> Result<NormalizedQuote, SourceErrorKind> {
    let direct_key =
        build_instrument_key(vwd_sym.symbol.as_str()).ok_or(SourceErrorKind::InvalidQuote)?;

    // 1. Check for direct quote
    if let Some(row) = rows.iter().find(|r| {
        r.snap_quote
            .as_ref()
            .and_then(|q| q.instrument_key)
            .is_some_and(|k| k == direct_key.as_str())
    }) {
        let q = row
            .snap_quote
            .as_ref()
            .ok_or(SourceErrorKind::InvalidQuote)?;
        let raw_bid = q.bid.as_ref().ok_or(SourceErrorKind::InvalidQuote)?;
        let raw_ask = q.ask.as_ref().ok_or(SourceErrorKind::InvalidQuote)?;

        let bid_str = raw_bid.price.ok_or(SourceErrorKind::InvalidQuote)?.get();
        let ask_str = raw_ask.price.ok_or(SourceErrorKind::InvalidQuote)?.get();

        let mut bid = Decimal::from_str(bid_str).map_err(|_| SourceErrorKind::InvalidQuote)?;
        let mut ask = Decimal::from_str(ask_str).map_err(|_| SourceErrorKind::InvalidQuote)?;

        if bid <= Decimal::ZERO || ask <= Decimal::ZERO {
            return Err(SourceErrorKind::InvalidQuote);
        }

        if let Some(scale) = vwd_sym.scale {
            bid *= scale;
            ask *= scale;
        }

        let exchange_ts = extract_exchange_ts(raw_bid.date_time, raw_ask.date_time);

        return Ok(NormalizedQuote {
            symbol: vwd_sym.symbol.clone(),
            bid,
            ask,
            exchange_ts,
            is_inverse: false,
        });
    }

    // 2. Check for inverse quote if allowed
    if vwd_sym.allow_inverse {
        let inv_key = build_inverse_instrument_key(vwd_sym.symbol.as_str())
            .ok_or(SourceErrorKind::InvalidQuote)?;

        if let Some(row) = rows.iter().find(|r| {
            r.snap_quote
                .as_ref()
                .and_then(|q| q.instrument_key)
                .is_some_and(|k| k == inv_key.as_str())
        }) {
            let q = row
                .snap_quote
                .as_ref()
                .ok_or(SourceErrorKind::InvalidQuote)?;
            let raw_inv_bid = q.bid.as_ref().ok_or(SourceErrorKind::InvalidQuote)?;
            let raw_inv_ask = q.ask.as_ref().ok_or(SourceErrorKind::InvalidQuote)?;

            let inv_bid_str = raw_inv_bid
                .price
                .ok_or(SourceErrorKind::InvalidQuote)?
                .get();
            let inv_ask_str = raw_inv_ask
                .price
                .ok_or(SourceErrorKind::InvalidQuote)?
                .get();

            let inv_bid =
                Decimal::from_str(inv_bid_str).map_err(|_| SourceErrorKind::InvalidQuote)?;
            let inv_ask =
                Decimal::from_str(inv_ask_str).map_err(|_| SourceErrorKind::InvalidQuote)?;

            if inv_bid <= Decimal::ZERO || inv_ask <= Decimal::ZERO {
                return Err(SourceErrorKind::InvalidQuote);
            }

            // Invert: bid = 1 / inv_ask, ask = 1 / inv_bid
            let inverted_bid = Decimal::ONE
                .checked_div(inv_ask)
                .ok_or(SourceErrorKind::InvalidQuote)?;
            let inverted_ask = Decimal::ONE
                .checked_div(inv_bid)
                .ok_or(SourceErrorKind::InvalidQuote)?;

            let mut bid = round_to_significant_digits(inverted_bid, significant_digits)
                .map_err(|_| SourceErrorKind::InvalidQuote)?;
            let mut ask = round_to_significant_digits(inverted_ask, significant_digits)
                .map_err(|_| SourceErrorKind::InvalidQuote)?;

            if let Some(scale) = vwd_sym.scale {
                bid *= scale;
                ask *= scale;
            }

            let exchange_ts = extract_exchange_ts(raw_inv_bid.date_time, raw_inv_ask.date_time);

            return Ok(NormalizedQuote {
                symbol: vwd_sym.symbol.clone(),
                bid,
                ask,
                exchange_ts,
                is_inverse: true,
            });
        }
    }

    Err(SourceErrorKind::MissingQuote)
}

/// Normalizes a configured symbol from a parsed scratch buffer.
pub fn normalize_symbol_from_scratch(
    vwd_sym: &VwdSymbol,
    scratch: &crate::vwd::raw::VwdScratch,
    significant_digits: u32,
) -> Result<NormalizedQuote, SourceErrorKind> {
    let direct_key =
        build_instrument_key(vwd_sym.symbol.as_str()).ok_or(SourceErrorKind::InvalidQuote)?;

    // 1. Check for direct quote
    if let Some(q) = scratch
        .quotes
        .iter()
        .find(|q| q.instrument_key.as_str() == direct_key.as_str())
    {
        let mut bid = q.bid_price.ok_or(SourceErrorKind::InvalidQuote)?;
        let mut ask = q.ask_price.ok_or(SourceErrorKind::InvalidQuote)?;

        if bid <= Decimal::ZERO || ask <= Decimal::ZERO {
            return Err(SourceErrorKind::InvalidQuote);
        }

        if let Some(scale) = vwd_sym.scale {
            bid *= scale;
            ask *= scale;
        }

        let exchange_ts = extract_exchange_ts(q.bid_time.as_deref(), q.ask_time.as_deref());

        return Ok(NormalizedQuote {
            symbol: vwd_sym.symbol.clone(),
            bid,
            ask,
            exchange_ts,
            is_inverse: false,
        });
    }

    // 2. Check for inverse quote if allowed
    if vwd_sym.allow_inverse {
        let inv_key = build_inverse_instrument_key(vwd_sym.symbol.as_str())
            .ok_or(SourceErrorKind::InvalidQuote)?;

        if let Some(q) = scratch
            .quotes
            .iter()
            .find(|q| q.instrument_key.as_str() == inv_key.as_str())
        {
            let inv_bid = q.bid_price.ok_or(SourceErrorKind::InvalidQuote)?;
            let inv_ask = q.ask_price.ok_or(SourceErrorKind::InvalidQuote)?;

            if inv_bid <= Decimal::ZERO || inv_ask <= Decimal::ZERO {
                return Err(SourceErrorKind::InvalidQuote);
            }

            let inverted_bid = Decimal::ONE
                .checked_div(inv_ask)
                .ok_or(SourceErrorKind::InvalidQuote)?;
            let inverted_ask = Decimal::ONE
                .checked_div(inv_bid)
                .ok_or(SourceErrorKind::InvalidQuote)?;

            let mut bid = round_to_significant_digits(inverted_bid, significant_digits)
                .map_err(|_| SourceErrorKind::InvalidQuote)?;
            let mut ask = round_to_significant_digits(inverted_ask, significant_digits)
                .map_err(|_| SourceErrorKind::InvalidQuote)?;

            if let Some(scale) = vwd_sym.scale {
                bid *= scale;
                ask *= scale;
            }

            let exchange_ts = extract_exchange_ts(q.bid_time.as_deref(), q.ask_time.as_deref());

            return Ok(NormalizedQuote {
                symbol: vwd_sym.symbol.clone(),
                bid,
                ask,
                exchange_ts,
                is_inverse: true,
            });
        }
    }

    Err(SourceErrorKind::MissingQuote)
}

/// Extracts exchange timestamp as minimum of bid and ask dateTime.
///
/// If either side lacks a valid timestamp, returns `None`. Provenance from
/// unrelated fields (such as `last.dateTime`) is explicitly ignored.
fn extract_exchange_ts(bid_dt: Option<&str>, ask_dt: Option<&str>) -> Option<UnixNanos> {
    let b_str = bid_dt?;
    let a_str = ask_dt?;

    let b_nanos = parse_rfc3339_nanos(b_str).ok()?;
    let a_nanos = parse_rfc3339_nanos(a_str).ok()?;

    Some(b_nanos.min(a_nanos))
}

/// Rounds a positive `Decimal` to `sig_digits` significant digits using exact base-10 arithmetic.
pub fn round_to_significant_digits(val: Decimal, sig_digits: u32) -> Result<Decimal, MdcError> {
    if sig_digits == 0 {
        return Err(MdcError::Protocol("significant digits must be > 0".into()));
    }
    if val <= Decimal::ZERO {
        return Err(MdcError::Protocol(
            "value must be positive for significant digits rounding".into(),
        ));
    }

    // Determine order of magnitude k where 10^k <= val < 10^(k+1)
    let mut k: i32 = 0;
    let ten = Decimal::from(10);
    if val >= Decimal::ONE {
        let mut cur = val;
        while cur >= ten {
            cur /= ten;
            k += 1;
        }
    } else {
        let mut cur = val;
        while cur < Decimal::ONE {
            cur *= ten;
            k -= 1;
        }
    }

    // Target scale (number of decimal places) = sig_digits - 1 - k
    let target_scale = (sig_digits as i32) - 1 - k;
    if target_scale >= 0 {
        let scale = (target_scale as u32).min(28);
        Ok(val.round_dp(scale))
    } else {
        let mut factor = Decimal::ONE;
        for _ in 0..(-target_scale) {
            factor = factor.checked_mul(ten).ok_or_else(|| {
                MdcError::Protocol("overflow in significant digits factor".into())
            })?;
        }
        let rounded = (val / factor).round();
        Ok(rounded * factor)
    }
}

/// Computes the number of days since Unix epoch (1970-01-01) for a Gregorian date.
const fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = year - (month <= 2) as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

/// Parses an RFC 3339 timestamp string into `UnixNanos`.
///
/// Supported formats: `YYYY-MM-DDTHH:MM:SS[.fffffffff]Z` or with numeric offset `+HH:MM` / `-HH:MM`.
pub fn parse_rfc3339_nanos(s: &str) -> Result<UnixNanos, MdcError> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return Err(MdcError::Protocol(format!(
            "timestamp too short for RFC 3339: {s}"
        )));
    }

    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || (bytes[10] != b'T' && bytes[10] != b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(MdcError::Protocol(format!(
            "malformed RFC 3339 separators: {s}"
        )));
    }

    let parse_2 = |idx: usize| -> Result<u32, MdcError> {
        let d1 = bytes[idx].wrapping_sub(b'0');
        let d2 = bytes[idx + 1].wrapping_sub(b'0');
        if d1 > 9 || d2 > 9 {
            return Err(MdcError::Protocol(format!("non-digit in timestamp: {s}")));
        }
        Ok((d1 as u32) * 10 + (d2 as u32))
    };

    let parse_4 = |idx: usize| -> Result<i64, MdcError> {
        let d1 = bytes[idx].wrapping_sub(b'0');
        let d2 = bytes[idx + 1].wrapping_sub(b'0');
        let d3 = bytes[idx + 2].wrapping_sub(b'0');
        let d4 = bytes[idx + 3].wrapping_sub(b'0');
        if d1 > 9 || d2 > 9 || d3 > 9 || d4 > 9 {
            return Err(MdcError::Protocol(format!("non-digit in timestamp: {s}")));
        }
        Ok((d1 as i64) * 1000 + (d2 as i64) * 100 + (d3 as i64) * 10 + (d4 as i64))
    };

    let year = parse_4(0)?;
    let month = parse_2(5)?;
    let day = parse_2(8)?;
    let hour = parse_2(11)?;
    let minute = parse_2(14)?;
    let second = parse_2(17)?;

    if month == 0 || month > 12 || day == 0 || day > 31 || hour > 23 || minute > 59 || second > 60 {
        return Err(MdcError::Protocol(format!("date/time out of range: {s}")));
    }

    let days = days_from_civil(year, month, day);
    let mut total_secs =
        days * 86400 + (hour as i64) * 3600 + (minute as i64) * 60 + (second as i64);

    let mut nanos = 0u64;
    let mut idx = 19;

    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        let mut frac_val = 0u64;
        let mut frac_digits = 0;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            if frac_digits < 9 {
                frac_val = frac_val * 10 + (bytes[idx] - b'0') as u64;
                frac_digits += 1;
            }
            idx += 1;
        }
        while frac_digits < 9 {
            frac_val *= 10;
            frac_digits += 1;
        }
        nanos = frac_val;
    }

    if idx >= bytes.len() {
        return Err(MdcError::Protocol(format!(
            "missing timezone in RFC 3339: {s}"
        )));
    }

    match bytes[idx] {
        b'Z' | b'z' => {}
        b'+' | b'-' => {
            let sign = if bytes[idx] == b'-' { -1i64 } else { 1i64 };
            idx += 1;
            if idx + 5 > bytes.len() || bytes[idx + 2] != b':' {
                return Err(MdcError::Protocol(format!(
                    "malformed timezone offset: {s}"
                )));
            }
            let off_h = parse_2(idx)? as i64;
            let off_m = parse_2(idx + 3)? as i64;
            let offset_secs = sign * (off_h * 3600 + off_m * 60);
            total_secs -= offset_secs;
        }
        _ => {
            return Err(MdcError::Protocol(format!(
                "invalid timezone delimiter: {s}"
            )));
        }
    }

    if total_secs < 0 {
        return Err(MdcError::Protocol(format!(
            "timestamp before Unix epoch: {s}"
        )));
    }

    let total_nanos = (total_secs as u64)
        .checked_mul(1_000_000_000)
        .and_then(|ns| ns.checked_add(nanos))
        .ok_or_else(|| MdcError::Protocol(format!("nanosecond overflow in timestamp: {s}")))?;

    Ok(UnixNanos(total_nanos))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::vwd::raw::parse_snap_quotes;
    use mdc_testkit::load_fixture;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn test_key_builders() {
        assert_eq!(
            build_instrument_key("EURUSD").as_deref(),
            Some("EURUSD.XRATE.SPOT~X")
        );
        assert_eq!(
            build_inverse_instrument_key("EURUSD").as_deref(),
            Some("USDEUR.XRATE.SPOT~X")
        );
        assert!(build_instrument_key("INVALID").is_none());
        assert!(build_instrument_key("EUR").is_none());
    }

    #[test]
    fn test_parse_rfc3339_nanos_with_offset() {
        let ts_str = "2026-09-29T10:00:00.000+02:00";
        let nanos = parse_rfc3339_nanos(ts_str).unwrap();
        // 10:00 +02:00 is 08:00:00 UTC
        let utc_str = "2026-09-29T08:00:00.000Z";
        let expected = parse_rfc3339_nanos(utc_str).unwrap();
        assert_eq!(nanos, expected);
    }

    #[test]
    fn test_round_to_significant_digits() {
        // 1.0842345 to 6 digits -> 1.08423
        assert_eq!(
            round_to_significant_digits(d("1.0842345"), 6).unwrap(),
            d("1.08423")
        );
        // 0.92217123 to 6 digits -> 0.922171
        assert_eq!(
            round_to_significant_digits(d("0.92217123"), 6).unwrap(),
            d("0.922171")
        );
        // 0.001234567 to 6 digits -> 0.00123457
        assert_eq!(
            round_to_significant_digits(d("0.001234567"), 6).unwrap(),
            d("0.00123457")
        );
        // 123.45678 to 6 digits -> 123.457
        assert_eq!(
            round_to_significant_digits(d("123.45678"), 6).unwrap(),
            d("123.457")
        );
        // 1234567 to 6 digits -> 1234570
        assert_eq!(
            round_to_significant_digits(d("1234567"), 6).unwrap(),
            d("1234570")
        );
    }

    #[test]
    fn test_normalize_direct_quote() {
        let json = load_fixture("crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json")
            .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        let sym = VwdSymbol {
            symbol: Symbol::from("EURUSD"),
            allow_inverse: false,
            scale: None,
        };

        let norm = normalize_symbol_quote(&sym, &rows, 6).unwrap();
        assert_eq!(norm.symbol.as_str(), "EURUSD");
        assert_eq!(norm.bid, d("1.0842"));
        assert_eq!(norm.ask, d("1.0844"));
        assert!(!norm.is_inverse);
        assert_eq!(
            norm.exchange_ts,
            Some(parse_rfc3339_nanos("2026-09-29T10:00:00.000+02:00").unwrap())
        );
    }

    #[test]
    fn test_normalize_direct_quote_with_scale() {
        let json = load_fixture("crates/mdc-providers/tests/fixtures/vwd/order_book_snapshot.json")
            .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        let sym = VwdSymbol {
            symbol: Symbol::from("EURUSD"),
            allow_inverse: false,
            scale: Some(d("0.01")),
        };

        let norm = normalize_symbol_quote(&sym, &rows, 6).unwrap();
        assert_eq!(norm.bid, d("0.010842"));
        assert_eq!(norm.ask, d("0.010844"));
    }

    #[test]
    fn test_normalize_inverse_quote() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/snapquote_inverse_only.json")
                .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        let sym = VwdSymbol {
            symbol: Symbol::from("EURUSD"),
            allow_inverse: true,
            scale: None,
        };

        let norm = normalize_symbol_quote(&sym, &rows, 6).unwrap();
        assert_eq!(norm.symbol.as_str(), "EURUSD");
        assert!(norm.is_inverse);
        // inv_ask = 0.92234 -> 1 / 0.92234 = 1.0841988... -> 1.08420
        assert_eq!(norm.bid, d("1.08420"));
        // inv_bid = 0.92217 -> 1 / 0.92217 = 1.0843987... -> 1.08440
        assert_eq!(norm.ask, d("1.08440"));
        assert!(norm.bid < norm.ask);
    }

    #[test]
    fn test_normalize_missing_ask_returns_invalid_quote() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/snapquote_missing_ask.json")
                .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        let sym = VwdSymbol {
            symbol: Symbol::from("EURUSD"),
            allow_inverse: false,
            scale: None,
        };

        let err = normalize_symbol_quote(&sym, &rows, 6).unwrap_err();
        assert_eq!(err, SourceErrorKind::InvalidQuote);
    }

    #[test]
    fn test_normalize_zero_price_returns_invalid_quote() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/snapquote_zero_price.json")
                .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        let sym = VwdSymbol {
            symbol: Symbol::from("EURUSD"),
            allow_inverse: false,
            scale: None,
        };

        let err = normalize_symbol_quote(&sym, &rows, 6).unwrap_err();
        assert_eq!(err, SourceErrorKind::InvalidQuote);
    }

    #[test]
    fn test_normalize_missing_side_time_results_in_none_exchange_ts() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/missing_side_time.json").unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        let sym = VwdSymbol {
            symbol: Symbol::from("EURUSD"),
            allow_inverse: false,
            scale: None,
        };

        let norm = normalize_symbol_quote(&sym, &rows, 6).unwrap();
        assert_eq!(norm.exchange_ts, None);
    }

    #[test]
    fn test_normalize_unrelated_fresh_last_time_is_ignored() {
        let json =
            load_fixture("crates/mdc-providers/tests/fixtures/vwd/unrelated_fresh_last_time.json")
                .unwrap();
        let rows = parse_snap_quotes(&json).unwrap();
        let sym = VwdSymbol {
            symbol: Symbol::from("EURUSD"),
            allow_inverse: false,
            scale: None,
        };

        let norm = normalize_symbol_quote(&sym, &rows, 6).unwrap();
        // Provenance must be min of bid (09:59:00) and ask (09:59:01), NOT last (10:05:00)
        let expected = parse_rfc3339_nanos("2026-09-29T09:59:00.000+02:00").unwrap();
        assert_eq!(norm.exchange_ts, Some(expected));
    }
}
