//! Normalization from raw Coinbase Advanced Trade payloads into canonical domain models.

use mdc_core::{
    L2Delta, L2Snapshot, MdcError, NativeTradeIdentity, PriceLevel, Side, Trade, TradeId,
    TradeNamespace, UnixNanos,
};

use crate::coinbase_advanced::raw::{CoinbaseAdvancedScratch, RawL2Event, RawTrade};

/// Computes days since Unix epoch (1970-01-01) for a civil Gregorian date.
const fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

/// Parses an RFC 3339 timestamp string into [`UnixNanos`].
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

    let parse_2 = |idx: usize| -> Option<u32> {
        let b0 = bytes[idx];
        let b1 = bytes[idx + 1];
        if b0.is_ascii_digit() && b1.is_ascii_digit() {
            Some(((b0 - b'0') as u32) * 10 + (b1 - b'0') as u32)
        } else {
            None
        }
    };

    let parse_4 = |idx: usize| -> Option<i64> {
        let b0 = bytes[idx];
        let b1 = bytes[idx + 1];
        let b2 = bytes[idx + 2];
        let b3 = bytes[idx + 3];
        if b0.is_ascii_digit() && b1.is_ascii_digit() && b2.is_ascii_digit() && b3.is_ascii_digit()
        {
            Some(
                ((b0 - b'0') as i64) * 1000
                    + ((b1 - b'0') as i64) * 100
                    + ((b2 - b'0') as i64) * 10
                    + (b3 - b'0') as i64,
            )
        } else {
            None
        }
    };

    let year = parse_4(0).ok_or_else(|| MdcError::Protocol("invalid year".to_string()))?;
    let month = parse_2(5).ok_or_else(|| MdcError::Protocol("invalid month".to_string()))?;
    let day = parse_2(8).ok_or_else(|| MdcError::Protocol("invalid day".to_string()))?;
    let hour = parse_2(11).ok_or_else(|| MdcError::Protocol("invalid hour".to_string()))?;
    let min = parse_2(14).ok_or_else(|| MdcError::Protocol("invalid minute".to_string()))?;
    let sec = parse_2(17).ok_or_else(|| MdcError::Protocol("invalid second".to_string()))?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return Err(MdcError::Protocol(format!(
            "calendar component out of range: {s}"
        )));
    }

    let days = days_from_civil(year, month, day);
    let mut secs = days * 86400 + (hour as i64) * 3600 + (min as i64) * 60 + (sec as i64);

    let mut nanos: u32 = 0;
    let mut cursor = 19;

    if cursor < bytes.len() && bytes[cursor] == b'.' {
        cursor += 1;
        let mut frac_nanos = 0u32;
        let mut frac_digits = 0;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            if frac_digits < 9 {
                frac_nanos = frac_nanos * 10 + (bytes[cursor] - b'0') as u32;
                frac_digits += 1;
            }
            cursor += 1;
        }
        while frac_digits < 9 {
            frac_nanos *= 10;
            frac_digits += 1;
        }
        nanos = frac_nanos;
    }

    // Parse timezone
    if cursor < bytes.len() {
        match bytes[cursor] {
            b'Z' | b'z' => {}
            b'+' | b'-' => {
                let sign = if bytes[cursor] == b'+' { 1i64 } else { -1i64 };
                cursor += 1;
                if cursor + 5 <= bytes.len() {
                    let tz_hour = parse_2(cursor)
                        .ok_or_else(|| MdcError::Protocol("invalid tz hour".to_string()))?;
                    let tz_min = parse_2(cursor + 3)
                        .ok_or_else(|| MdcError::Protocol("invalid tz min".to_string()))?;
                    let offset_secs = (tz_hour as i64) * 3600 + (tz_min as i64) * 60;
                    secs -= sign * offset_secs;
                }
            }
            _ => {
                return Err(MdcError::Protocol(format!("unrecognized timezone: {s}")));
            }
        }
    }

    if secs < 0 {
        return Ok(UnixNanos(0));
    }

    let total_nanos = (secs as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(nanos as u64);

    Ok(UnixNanos(total_nanos))
}

/// Normalizes a parsed [`RawL2Event`] snapshot into canonical [`L2Snapshot`].
pub fn normalize_snapshot_from_event(
    event: &RawL2Event<'_>,
    envelope_ts: Option<&str>,
) -> Result<(UnixNanos, L2Snapshot), MdcError> {
    let exchange_ts = match envelope_ts {
        Some(ts_str) => parse_rfc3339_nanos(ts_str)?,
        None => crate::now_nanos(),
    };

    let mut bids = Vec::with_capacity(event.updates.len());
    let mut asks = Vec::with_capacity(event.updates.len());

    for upd in &event.updates {
        if upd.new_quantity > rust_decimal::Decimal::ZERO {
            let level = PriceLevel::new(upd.price_level, upd.new_quantity);
            if upd.side == "bid" {
                bids.push(level);
            } else if upd.side == "offer" {
                asks.push(level);
            }
        }
    }

    let snapshot = L2Snapshot {
        last_update_id: None,
        bids,
        asks,
    };

    Ok((exchange_ts, snapshot))
}

/// Normalizes scratch buffers populated during frame deserialization into canonical [`L2Snapshot`].
pub fn normalize_snapshot_from_scratch(
    scratch: &CoinbaseAdvancedScratch,
    envelope_ts: Option<&str>,
) -> Result<(UnixNanos, L2Snapshot), MdcError> {
    let exchange_ts = match envelope_ts {
        Some(ts_str) => parse_rfc3339_nanos(ts_str)?,
        None => crate::now_nanos(),
    };

    let snapshot = L2Snapshot {
        last_update_id: None,
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };

    Ok((exchange_ts, snapshot))
}

/// Normalizes scratch buffers populated during frame deserialization into canonical [`L2Delta`].
pub fn normalize_delta_from_scratch(
    scratch: &CoinbaseAdvancedScratch,
    envelope_ts: Option<&str>,
) -> Result<(UnixNanos, L2Delta), MdcError> {
    let exchange_ts = match envelope_ts {
        Some(ts_str) => parse_rfc3339_nanos(ts_str)?,
        None => crate::now_nanos(),
    };

    let delta = L2Delta {
        sequence: None,
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };

    Ok((exchange_ts, delta))
}

/// Normalizes a parsed [`RawTrade`] into canonical [`Trade`].
///
/// In Coinbase Advanced Trade, `side` reflects the maker order side:
/// - `"BUY"`: maker was buyer -> aggressor/taker was seller -> [`Side::Ask`]
/// - `"SELL"`: maker was seller -> aggressor/taker was buyer -> [`Side::Bid`]
pub fn normalize_trade(trade: &RawTrade<'_>) -> Result<(UnixNanos, Trade), MdcError> {
    let exchange_ts = parse_rfc3339_nanos(trade.time)?;

    let aggressor = if trade.side.eq_ignore_ascii_case("BUY") {
        Side::Ask
    } else {
        Side::Bid
    };

    let namespace = TradeNamespace::new("coinbase_advanced")
        .map_err(|e| MdcError::Protocol(format!("failed to create trade namespace: {e}")))?;
    let trade_id = TradeId::new(trade.trade_id)
        .map_err(|e| MdcError::Protocol(format!("failed to create trade id: {e}")))?;

    let identity = NativeTradeIdentity::new(namespace, trade_id);

    let canonical_trade = Trade {
        identity: Some(identity),
        price: trade.price,
        volume: trade.size,
        aggressor,
    };

    Ok((exchange_ts, canonical_trade))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn test_parse_rfc3339_nanos() {
        let ts = parse_rfc3339_nanos("2023-02-09T20:32:50.714964855Z").expect("parse rfc3339");
        assert_eq!(ts.as_nanos(), 1675974770714964855);

        let ts2 = parse_rfc3339_nanos("1970-01-01T00:00:00Z").expect("epoch");
        assert_eq!(ts2.as_nanos(), 0);
    }

    #[test]
    fn test_normalize_trade_aggressor_mapping() {
        let buy_trade = RawTrade {
            trade_id: "1001",
            product_id: "BTC-USD",
            price: Decimal::from(21000),
            size: Decimal::from_str_exact("0.5").unwrap(),
            side: "BUY",
            time: "2023-02-09T20:32:53.000000000Z",
        };
        let (ts, t) = normalize_trade(&buy_trade).expect("normalize trade");
        assert_eq!(t.aggressor, Side::Ask);
        assert_eq!(t.price, Decimal::from(21000));
        assert_eq!(t.volume, Decimal::from_str_exact("0.5").unwrap());
        assert!(ts.as_nanos() > 0);

        let sell_trade = RawTrade {
            trade_id: "1002",
            product_id: "BTC-USD",
            price: Decimal::from(21000),
            size: Decimal::from(1),
            side: "SELL",
            time: "2023-02-09T20:32:53.000000000Z",
        };
        let (_, t2) = normalize_trade(&sell_trade).expect("normalize trade");
        assert_eq!(t2.aggressor, Side::Bid);
    }
}
