//! Normalization from raw Kraken WebSocket v2 payloads into canonical domain models.

use mdc_core::{
    L2Delta, L2Snapshot, MdcError, NativeTradeIdentity, PriceLevel, Side, Trade, TradeId,
    TradeNamespace, UnixNanos,
};

use crate::kraken::checksum::KrakenChecksumBook;
use crate::kraken::raw::RawTrade;

/// Formats a `u64` into an ASCII string buffer without heap allocation.
fn format_u64_to_buf(mut n: u64, buf: &mut [u8; 20]) -> &str {
    if n == 0 {
        buf[0] = b'0';
        return match std::str::from_utf8(&buf[..1]) {
            Ok(s) => s,
            Err(_) => "0",
        };
    }
    let mut i = 20;
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    match std::str::from_utf8(&buf[i..]) {
        Ok(s) => s,
        Err(_) => "0",
    }
}

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

    // Expect: YYYY-MM-DDTHH:MM:SS
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

    // Offset parsing
    if idx < bytes.len() {
        if bytes[idx] == b'Z' || bytes[idx] == b'z' {
            // UTC
        } else if bytes[idx] == b'+' || bytes[idx] == b'-' {
            let is_negative = bytes[idx] == b'-';
            idx += 1;
            if idx + 4 < bytes.len() {
                let off_h = parse_2(idx)? as i64;
                let off_m = if bytes[idx + 2] == b':' {
                    parse_2(idx + 3)? as i64
                } else {
                    parse_2(idx + 2)? as i64
                };
                let off_secs = off_h * 3600 + off_m * 60;
                if is_negative {
                    total_secs += off_secs;
                } else {
                    total_secs -= off_secs;
                }
            }
        }
    }

    if total_secs < 0 {
        return Err(MdcError::Protocol(format!("pre-epoch timestamp: {s}")));
    }

    let total_nanos = (total_secs as u64)
        .checked_mul(1_000_000_000)
        .and_then(|n| n.checked_add(nanos))
        .ok_or_else(|| MdcError::Protocol(format!("nanosecond timestamp overflow: {s}")))?;

    Ok(UnixNanos(total_nanos))
}

/// Normalizes the entire local shadow book into a canonical `L2Snapshot`.
pub fn normalize_snapshot(book: &KrakenChecksumBook) -> L2Snapshot {
    let bids = book.top(Side::Bid, book.depth());
    let asks = book.top(Side::Ask, book.depth());
    L2Snapshot {
        bids,
        asks,
        last_update_id: None,
    }
}

/// Normalizes updated price levels into a canonical `L2Delta`.
pub fn normalize_delta(bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> L2Delta {
    L2Delta {
        bids,
        asks,
        sequence: None,
    }
}

/// Normalizes a raw public trade into canonical `Trade` and trade timestamp.
///
/// Kraken `side` indicates the taker/aggressor side:
/// - `"buy"`: buyer was aggressor (`Side::Bid`).
/// - `"sell"`: seller was aggressor (`Side::Ask`).
pub fn normalize_trade(raw: &RawTrade<'_>) -> Result<(UnixNanos, Trade), MdcError> {
    let trade_ts = parse_rfc3339_nanos(raw.timestamp)?;
    let ns = TradeNamespace::new("kraken:spot")
        .map_err(|e| MdcError::Protocol(format!("invalid trade namespace: {e}")))?;

    let mut tid_buf = [0u8; 20];
    let tid_str = format_u64_to_buf(raw.trade_id, &mut tid_buf);
    let tid =
        TradeId::new(tid_str).map_err(|e| MdcError::Protocol(format!("invalid trade id: {e}")))?;

    let identity = NativeTradeIdentity::new(ns, tid);
    let aggressor = match raw.side {
        "buy" => Side::Bid,
        "sell" => Side::Ask,
        other => {
            return Err(MdcError::Protocol(format!("unknown trade side '{other}'")));
        }
    };

    let trade = Trade {
        price: raw.price,
        volume: raw.qty,
        aggressor,
        identity: Some(identity),
    };

    Ok((trade_ts, trade))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn test_parse_rfc3339_nanos() {
        let ts_str = "2023-10-06T17:35:55.440295Z";
        let nanos = parse_rfc3339_nanos(ts_str).expect("parse rfc3339");
        assert_eq!(nanos.as_nanos(), 1696613755440295000);

        let epoch = parse_rfc3339_nanos("1970-01-01T00:00:00Z").expect("parse epoch");
        assert_eq!(epoch.as_nanos(), 0);

        let with_offset =
            parse_rfc3339_nanos("2023-10-06T19:35:55.440295+02:00").expect("parse offset");
        assert_eq!(with_offset.as_nanos(), 1696613755440295000);
    }

    #[test]
    fn test_normalize_trade() {
        let raw = RawTrade {
            symbol: "BTC/USD",
            side: "buy",
            price: Decimal::from_str_exact("45285.2").expect("valid price"),
            qty: Decimal::from_str_exact("0.001").expect("valid qty"),
            ord_type: Some("market"),
            trade_id: 12345678,
            timestamp: "2023-10-06T17:35:55.440295Z",
        };

        let (ts, trade) = normalize_trade(&raw).expect("normalize trade");
        assert_eq!(ts.as_nanos(), 1696613755440295000);
        assert_eq!(trade.aggressor, Side::Bid);
        assert_eq!(trade.price.to_string(), "45285.2");
        assert_eq!(trade.volume.to_string(), "0.001");
        let id = trade.identity.expect("trade identity");
        assert_eq!(id.namespace.as_str(), "kraken:spot");
        assert_eq!(id.value.as_str(), "12345678");
    }
}
