//! Normalization from raw LMAX Digital WebSocket payloads into canonical domain models.

use mdc_core::{L2Snapshot, MdcError, Side, Trade, UnixNanos};

use crate::lmax::raw::{LmaxConnectionScratch, RawTrade};

/// Computes days since Unix epoch (1970-01-01) for a civil Gregorian date.
const fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
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

    let year = parse_digits(&bytes[0..4])? as i64;
    let month = parse_digits(&bytes[5..7])? as u32;
    let day = parse_digits(&bytes[8..10])? as u32;
    let hour = parse_digits(&bytes[11..13])?;
    let minute = parse_digits(&bytes[14..16])?;
    let second = parse_digits(&bytes[17..19])?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(MdcError::Protocol(format!(
            "invalid calendar date: {year:04}-{month:02}-{day:02}"
        )));
    }
    if hour > 23 || minute > 59 || second > 60 {
        return Err(MdcError::Protocol(format!(
            "invalid time of day: {hour:02}:{minute:02}:{second:02}"
        )));
    }

    let days = days_from_civil(year, month, day);
    let seconds_in_day = hour * 3600 + minute * 60 + second;
    let mut total_seconds = days * 86400 + seconds_in_day as i64;

    let mut idx = 19;
    let mut subsecond_nanos = 0u64;

    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        let frac_start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        let frac_len = idx - frac_start;
        if frac_len > 0 {
            let mut val = 0u64;
            let take = frac_len.min(9);
            for &b in &bytes[frac_start..frac_start + take] {
                val = val * 10 + (b - b'0') as u64;
            }
            for _ in take..9 {
                val *= 10;
            }
            subsecond_nanos = val;
        }
    }

    if idx < bytes.len() {
        match bytes[idx] {
            b'Z' | b'z' => {}
            b'+' | b'-' => {
                let sign: i64 = if bytes[idx] == b'-' { 1 } else { -1 };
                idx += 1;
                if idx + 5 <= bytes.len() && bytes[idx + 2] == b':' {
                    let off_h = parse_digits(&bytes[idx..idx + 2])? as i64;
                    let off_m = parse_digits(&bytes[idx + 3..idx + 5])? as i64;
                    let offset_sec = (off_h * 3600 + off_m * 60) * sign;
                    total_seconds += offset_sec;
                }
            }
            _ => {
                return Err(MdcError::Protocol(format!(
                    "unexpected timezone character: {}",
                    bytes[idx] as char
                )));
            }
        }
    }

    if total_seconds < 0 {
        return Err(MdcError::Protocol(format!(
            "timestamp before Unix epoch: {s}"
        )));
    }

    let nanos = (total_seconds as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(subsecond_nanos);

    Ok(UnixNanos(nanos))
}

fn parse_digits(bytes: &[u8]) -> Result<u64, MdcError> {
    let mut val = 0u64;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return Err(MdcError::Protocol("expected ASCII digit".into()));
        }
        val = val * 10 + (b - b'0') as u64;
    }
    Ok(val)
}

/// Normalizes an authoritative LMAX order book snapshot into canonical `(UnixNanos, L2Snapshot)`.
pub fn normalize_book(
    timestamp: &str,
    scratch: &LmaxConnectionScratch,
) -> Result<(UnixNanos, L2Snapshot), MdcError> {
    let exchange_ts = parse_rfc3339_nanos(timestamp)?;
    let snapshot = L2Snapshot {
        last_update_id: None,
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };
    Ok((exchange_ts, snapshot))
}

/// Normalizes a public executed trade into canonical `(UnixNanos, Trade)`.
///
/// LMAX Digital does not explicitly tag aggressor side; by convention and legacy precedent,
/// negative `last_quantity` is mapped to `Side::Bid`, otherwise `Side::Ask`.
pub fn normalize_trade(raw: &RawTrade<'_>) -> Result<(UnixNanos, Trade), MdcError> {
    let exchange_ts = parse_rfc3339_nanos(raw.timestamp)?;
    let aggressor = if raw.last_quantity.is_sign_negative() {
        Side::Bid
    } else {
        Side::Ask
    };

    let trade = Trade {
        identity: None,
        price: raw.last_price,
        volume: raw.last_quantity.abs(),
        aggressor,
    };

    Ok((exchange_ts, trade))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::lmax::raw::{LmaxBookFrameSeed, LmaxConnectionScratch, RawTrade};
    use mdc_testkit::load_fixture;
    use rust_decimal::Decimal;
    use serde::de::DeserializeSeed;

    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/lmax/order_book_snapshot.json";
    const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/lmax/trade.json";

    #[test]
    fn test_parse_rfc3339_nanos() {
        let ts = parse_rfc3339_nanos("2026-01-01T00:00:00.123Z").unwrap();
        assert_eq!(ts.as_nanos(), 1767225600123000000);

        let ts2 = parse_rfc3339_nanos("2026-01-01T00:00:00Z").unwrap();
        assert_eq!(ts2.as_nanos(), 1767225600000000000);

        let ts3 = parse_rfc3339_nanos("2026-01-01T01:00:00+01:00").unwrap();
        assert_eq!(ts3.as_nanos(), 1767225600000000000);

        assert!(parse_rfc3339_nanos("invalid").is_err());
    }

    #[test]
    fn test_normalize_book() {
        let snap_str = load_fixture(SNAPSHOT_FIXTURE).unwrap();
        let mut scratch = LmaxConnectionScratch::with_capacity(16);

        let mut de = serde_json::Deserializer::from_str(&snap_str);
        let header = LmaxBookFrameSeed::new(&mut scratch)
            .deserialize(&mut de)
            .unwrap();

        let (ts, snap) = normalize_book(header.timestamp, &scratch).unwrap();
        assert_eq!(ts.as_nanos(), 1767225600123000000);
        assert_eq!(snap.last_update_id, None);
        assert_eq!(snap.bids.len(), 2);
        assert_eq!(snap.asks.len(), 2);
    }

    #[test]
    fn test_normalize_trade() {
        let trade_str = load_fixture(TRADE_FIXTURE).unwrap();
        let raw: RawTrade<'_> = serde_json::from_str(&trade_str).unwrap();

        let (ts, trade) = normalize_trade(&raw).unwrap();
        assert_eq!(ts.as_nanos(), 1767225600456000000);
        assert_eq!(trade.price, Decimal::from(50000));
        assert_eq!(trade.volume, Decimal::from_str_exact("0.5000").unwrap());
        assert_eq!(trade.aggressor, Side::Ask);
        assert!(trade.identity.is_none());

        // Test negative quantity -> Side::Bid
        let raw_sell = RawTrade {
            instrument_id: "btc-usd",
            timestamp: "2026-01-01T00:00:00.000Z",
            last_quantity: Decimal::from_str_exact("-0.2500").unwrap(),
            last_price: Decimal::from(50000),
        };
        let (_, trade_sell) = normalize_trade(&raw_sell).unwrap();
        assert_eq!(trade_sell.aggressor, Side::Bid);
        assert_eq!(
            trade_sell.volume,
            Decimal::from_str_exact("0.2500").unwrap()
        );
    }
}
