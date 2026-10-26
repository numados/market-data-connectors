//! Normalization of Bitfinex raw payloads into canonical `mdc-core` domain models.

use mdc_core::model::{
    NativeTradeIdentity, PriceLevel, Side, Trade, TradeId, TradeNamespace, UnixNanos,
};
use rust_decimal::Decimal;

use crate::bitfinex::raw::{RawBookLevel, RawTradeEntry};

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

/// Maps a raw trade entry into a canonical `Trade` domain model.
pub fn normalize_trade(
    trade: &RawTradeEntry<'_>,
    _received_ts: UnixNanos,
) -> Result<Trade, rust_decimal::Error> {
    let raw_amount = trade.parse_amount()?;
    let price = trade.parse_price()?;

    // In Bitfinex trades, positive amount represents buy execution (bid aggressor),
    // negative amount represents sell execution (ask aggressor).
    let aggressor = if raw_amount.is_sign_negative() {
        Side::Ask
    } else {
        Side::Bid
    };

    let volume = raw_amount.abs();
    let mut id_buf = [0u8; 20];
    let id_str = format_u64_to_buf(trade.id, &mut id_buf);
    #[allow(clippy::expect_used)]
    let namespace = TradeNamespace::new("bitfinex").expect("static namespace 'bitfinex' is valid");
    #[allow(clippy::expect_used)]
    let trade_id = TradeId::new(id_str).expect("numeric trade id is valid ASCII");
    let identity = Some(NativeTradeIdentity::new(namespace, trade_id));

    Ok(Trade {
        identity,
        price,
        volume,
        aggressor,
    })
}

/// Extracts normalized canonical `(Side, PriceLevel)` from a raw book level.
pub fn normalize_book_level(
    level: &RawBookLevel<'_>,
) -> Result<(Side, PriceLevel), rust_decimal::Error> {
    let raw_amount = level.parse_amount()?;
    let price = level.parse_price()?;

    let (side, volume) = if level.count == 0 {
        // Count 0 denotes removal of price level
        let s = if raw_amount.is_sign_positive() {
            Side::Bid
        } else {
            Side::Ask
        };
        (s, Decimal::ZERO)
    } else {
        let s = if raw_amount.is_sign_positive() {
            Side::Bid
        } else {
            Side::Ask
        };
        (s, raw_amount.abs())
    };

    Ok((side, PriceLevel::new(price, volume)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str_exact(s).unwrap()
    }

    #[test]
    fn test_normalize_trade_aggressor_mapping() {
        let json_buy = r#"[401597395, 1574694478808, "0.005", "7245.3"]"#;
        let raw_buy: RawTradeEntry<'_> = serde_json::from_str(json_buy).unwrap();
        let trade_buy = normalize_trade(&raw_buy, UnixNanos::from_millis(100)).unwrap();
        assert_eq!(trade_buy.aggressor, Side::Bid);
        assert_eq!(trade_buy.volume, dec("0.005"));
        assert_eq!(trade_buy.price, dec("7245.3"));
        assert_eq!(
            trade_buy.identity.as_ref().unwrap().value.as_str(),
            "401597395"
        );

        let json_sell = r#"[401597396, 1574694478900, "-0.010", "7245.0"]"#;
        let raw_sell: RawTradeEntry<'_> = serde_json::from_str(json_sell).unwrap();
        let trade_sell = normalize_trade(&raw_sell, UnixNanos::from_millis(200)).unwrap();
        assert_eq!(trade_sell.aggressor, Side::Ask);
        assert_eq!(trade_sell.volume, dec("0.010"));
        assert_eq!(trade_sell.price, dec("7245.0"));
    }

    #[test]
    fn test_normalize_book_level_update_and_removal() {
        // Active bid update
        let json_bid = r#"["41698", 2, "0.61"]"#;
        let raw_bid: RawBookLevel<'_> = serde_json::from_str(json_bid).unwrap();
        let (side, level) = normalize_book_level(&raw_bid).unwrap();
        assert_eq!(side, Side::Bid);
        assert_eq!(level.price, dec("41698"));
        assert_eq!(level.volume, dec("0.61"));

        // Active ask update
        let json_ask = r#"["41699", 3, "-0.50"]"#;
        let raw_ask: RawBookLevel<'_> = serde_json::from_str(json_ask).unwrap();
        let (side, level) = normalize_book_level(&raw_ask).unwrap();
        assert_eq!(side, Side::Ask);
        assert_eq!(level.price, dec("41699"));
        assert_eq!(level.volume, dec("0.50"));

        // Bid removal
        let json_rem_bid = r#"["41698", 0, "1"]"#;
        let raw_rem_bid: RawBookLevel<'_> = serde_json::from_str(json_rem_bid).unwrap();
        let (side, level) = normalize_book_level(&raw_rem_bid).unwrap();
        assert_eq!(side, Side::Bid);
        assert_eq!(level.price, dec("41698"));
        assert_eq!(level.volume, Decimal::ZERO);

        // Ask removal
        let json_rem_ask = r#"["41699", 0, "-1"]"#;
        let raw_rem_ask: RawBookLevel<'_> = serde_json::from_str(json_rem_ask).unwrap();
        let (side, level) = normalize_book_level(&raw_rem_ask).unwrap();
        assert_eq!(side, Side::Ask);
        assert_eq!(level.price, dec("41699"));
        assert_eq!(level.volume, Decimal::ZERO);
    }
}
