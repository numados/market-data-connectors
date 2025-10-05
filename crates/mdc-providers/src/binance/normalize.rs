//! Normalization from raw Binance payloads into canonical `mdc-core` domain models.

use crate::binance::raw::{RawBookTicker, RawDepthSnapshot, RawDepthUpdate, RawTrade};
use mdc_core::{
    BookTicker, L2Delta, L2Snapshot, MdcError, NativeTradeIdentity, PriceLevel, SequenceRange,
    Trade, TradeId, TradeNamespace, UnixNanos,
};

/// Normalizes a raw depth update into canonical `L2Delta` and exchange timestamp.
pub fn normalize_depth(raw: &RawDepthUpdate<'_>) -> (UnixNanos, L2Delta) {
    let exchange_ts = UnixNanos::from_millis(raw.event_time_ms);
    let mut bids = Vec::with_capacity(raw.bids.len());
    let mut asks = Vec::with_capacity(raw.asks.len());

    for &[price, volume] in &raw.bids {
        bids.push(PriceLevel::new(price, volume));
    }
    for &[price, volume] in &raw.asks {
        asks.push(PriceLevel::new(price, volume));
    }

    let delta = L2Delta {
        bids,
        asks,
        sequence: Some(SequenceRange::new(raw.first_update_id, raw.last_update_id)),
    };

    (exchange_ts, delta)
}

/// Normalizes a raw top-of-book ticker into canonical `BookTicker`.
///
/// Binance `@bookTicker` carries no event timestamp (`exchange_ts` is always `None`).
pub fn normalize_book_ticker(raw: &RawBookTicker<'_>) -> BookTicker {
    let best_bid = PriceLevel::new(raw.best_bid_price, raw.best_bid_qty);
    let best_ask = PriceLevel::new(raw.best_ask_price, raw.best_ask_qty);
    BookTicker::new(best_bid, best_ask, Some(raw.update_id))
}

/// Normalizes a raw public trade into canonical `Trade` and trade timestamp.
///
/// Maps buyer-maker flag:
/// - `is_buyer_maker == true`: Maker is buyer -> Aggressor is seller (`Side::Ask`).
/// - `is_buyer_maker == false`: Maker is seller -> Aggressor is buyer (`Side::Bid`).
pub fn normalize_trade(raw: &RawTrade<'_>) -> Result<(UnixNanos, Trade), MdcError> {
    let trade_ts = UnixNanos::from_millis(raw.trade_time_ms);
    let ns = TradeNamespace::new("binance:spot")
        .map_err(|e| MdcError::Protocol(format!("invalid trade namespace: {e}")))?;
    let tid_str = raw.trade_id.to_string();
    let tid =
        TradeId::new(&tid_str).map_err(|e| MdcError::Protocol(format!("invalid trade id: {e}")))?;

    let identity = NativeTradeIdentity::new(ns, tid);
    let aggressor = Trade::aggressor_from_buyer_maker(raw.is_buyer_maker);
    let trade = Trade::new(Some(identity), raw.price, raw.qty, aggressor);

    Ok((trade_ts, trade))
}

/// Normalizes a raw REST depth snapshot into canonical `L2Snapshot`.
pub fn normalize_snapshot(raw: &RawDepthSnapshot) -> L2Snapshot {
    let mut bids = Vec::with_capacity(raw.bids.len());
    let mut asks = Vec::with_capacity(raw.asks.len());

    for &[price, volume] in &raw.bids {
        bids.push(PriceLevel::new(price, volume));
    }
    for &[price, volume] in &raw.asks {
        asks.push(PriceLevel::new(price, volume));
    }

    L2Snapshot {
        bids,
        asks,
        last_update_id: Some(raw.last_update_id),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use mdc_core::Side;
    use rust_decimal::Decimal;

    fn d(s: &str) -> Decimal {
        Decimal::from_str_exact(s).expect("valid decimal string")
    }

    #[test]
    fn test_normalize_depth_update() {
        let raw = RawDepthUpdate {
            event_time_ms: 1_672_515_782_136,
            symbol: "BTCUSDT",
            first_update_id: 100,
            last_update_id: 105,
            bids: vec![[d("20000.5"), d("1.5")]],
            asks: vec![[d("20001.0"), d("2.0")]],
        };

        let (ts, delta) = normalize_depth(&raw);
        assert_eq!(ts, UnixNanos::from_millis(1_672_515_782_136));
        assert_eq!(delta.sequence, Some(SequenceRange::new(100, 105)));
        assert_eq!(delta.bids.len(), 1);
        assert_eq!(delta.asks.len(), 1);
        assert_eq!(delta.bids[0].price, d("20000.5"));
        assert_eq!(delta.bids[0].volume, d("1.5"));
        assert_eq!(delta.asks[0].price, d("20001.0"));
        assert_eq!(delta.asks[0].volume, d("2.0"));
    }

    #[test]
    fn test_normalize_book_ticker() {
        let raw = RawBookTicker {
            update_id: 555_666,
            symbol: "BTCUSDT",
            best_bid_price: d("25.35"),
            best_bid_qty: d("31.21"),
            best_ask_price: d("25.36"),
            best_ask_qty: d("40.66"),
        };

        let ticker = normalize_book_ticker(&raw);
        assert_eq!(ticker.update_id, Some(555_666));
        assert_eq!(ticker.best_bid.price, d("25.35"));
        assert_eq!(ticker.best_bid.volume, d("31.21"));
        assert_eq!(ticker.best_ask.price, d("25.36"));
        assert_eq!(ticker.best_ask.volume, d("40.66"));
    }

    #[test]
    fn test_normalize_trade_aggressor_mapping() {
        let raw_buyer_maker = RawTrade {
            event_time_ms: 1000,
            trade_time_ms: 1001,
            symbol: "BTCUSDT",
            trade_id: 42,
            price: d("100"),
            qty: d("1"),
            is_buyer_maker: true,
        };

        let (ts, trade) = normalize_trade(&raw_buyer_maker).expect("normalize trade");
        assert_eq!(ts, UnixNanos::from_millis(1001));
        assert_eq!(trade.aggressor, Side::Ask);
        assert_eq!(trade.price, d("100"));
        assert_eq!(trade.volume, d("1"));
        let id = trade.identity.expect("trade identity");
        assert_eq!(id.namespace.as_str(), "binance:spot");
        assert_eq!(id.value.as_str(), "42");

        let raw_seller_maker = RawTrade {
            is_buyer_maker: false,
            ..raw_buyer_maker
        };
        let (_, trade2) = normalize_trade(&raw_seller_maker).expect("normalize trade");
        assert_eq!(trade2.aggressor, Side::Bid);
    }

    #[test]
    fn test_normalize_snapshot() {
        let raw = RawDepthSnapshot {
            last_update_id: 999_888,
            bids: vec![[d("50000"), d("10")]],
            asks: vec![[d("50001"), d("12")]],
        };

        let snap = normalize_snapshot(&raw);
        assert_eq!(snap.last_update_id, Some(999_888));
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.asks.len(), 1);
        assert_eq!(snap.bids[0].price, d("50000"));
        assert_eq!(snap.bids[0].volume, d("10"));
    }
}
