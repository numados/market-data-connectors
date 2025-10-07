//! Normalization of Huobi raw payloads into canonical `mdc-core` domain models.

use mdc_core::{
    L2Delta, L2Snapshot, MdcError, NativeTradeIdentity, SequenceRange, Side, Trade, TradeId,
    TradeNamespace, UnixNanos,
};

use crate::huobi::raw::{HuobiConnectionScratch, MbpHeader, RawTradeItem};

/// Normalizes an authoritative order book snapshot (`rep`) into canonical `(UnixNanos, L2Snapshot)`.
pub fn normalize_snapshot(
    header: &MbpHeader,
    scratch: &HuobiConnectionScratch,
    ts: Option<u64>,
) -> (UnixNanos, L2Snapshot) {
    let exchange_ts = UnixNanos::from_millis(ts.unwrap_or(0));
    let snapshot = L2Snapshot {
        last_update_id: Some(header.seq_num),
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };
    (exchange_ts, snapshot)
}

/// Normalizes an incremental Market-By-Price delta update into canonical `(UnixNanos, L2Delta)`.
///
/// Under the Huobi MBP protocol:
/// - `prevSeqNum = p`
/// - `seqNum = s`
///
/// The valid contiguous sequence range is `[p + 1, s]`.
pub fn normalize_delta(
    header: &MbpHeader,
    scratch: &HuobiConnectionScratch,
    ts: u64,
) -> (UnixNanos, L2Delta) {
    let exchange_ts = UnixNanos::from_millis(ts);
    let sequence = header
        .prev_seq_num
        .map(|prev| SequenceRange::new(prev.saturating_add(1), header.seq_num));

    let delta = L2Delta {
        sequence,
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };
    (exchange_ts, delta)
}

/// Normalizes a public trade item into canonical `(UnixNanos, Trade)`.
pub fn normalize_trade(item: &RawTradeItem<'_>) -> Result<(UnixNanos, Trade), MdcError> {
    let exchange_ts = UnixNanos::from_millis(item.ts);

    let side = match item.direction {
        "buy" => Side::Bid,
        "sell" => Side::Ask,
        other => {
            return Err(MdcError::Protocol(format!(
                "invalid trade aggressor side: {other}"
            )));
        }
    };

    let price = crate::huobi::raw::decimal_from_f64(item.price)
        .ok_or_else(|| MdcError::Protocol(format!("invalid trade price: {}", item.price)))?;
    let amount = crate::huobi::raw::decimal_from_f64(item.amount)
        .ok_or_else(|| MdcError::Protocol(format!("invalid trade amount: {}", item.amount)))?;

    let id_str = item.trade_id.to_small_str();
    let trade_id = TradeId::new(id_str.as_str())
        .map_err(|e| MdcError::Protocol(format!("invalid trade id: {e}")))?;
    let namespace = TradeNamespace::new("huobi")
        .map_err(|e| MdcError::Protocol(format!("invalid trade namespace: {e}")))?;
    let identity = NativeTradeIdentity::new(namespace, trade_id);

    let trade = Trade::new(Some(identity), price, amount, side);
    Ok((exchange_ts, trade))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::huobi::raw::{MbpSeed, RawEnvelope, RawTradeTick};
    use rust_decimal::Decimal;
    use serde::de::DeserializeSeed;

    #[test]
    fn test_normalize_snapshot() {
        let json = include_str!("../../tests/fixtures/huobi/order_book_snapshot.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        let mut scratch = HuobiConnectionScratch::with_capacity(4);
        let mut de = serde_json::Deserializer::from_str(env.data.unwrap().get());
        let header = MbpSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        let (ts, snap) = normalize_snapshot(&header, &scratch, env.ts);
        assert_eq!(ts, UnixNanos::from_millis(1700000000200));
        assert_eq!(snap.last_update_id, Some(100020146800));
        assert_eq!(snap.bids.len(), 2);
        assert_eq!(snap.asks.len(), 2);
    }

    #[test]
    fn test_normalize_delta() {
        let json = include_str!("../../tests/fixtures/huobi/depth_update.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        let mut scratch = HuobiConnectionScratch::with_capacity(4);
        let mut de = serde_json::Deserializer::from_str(env.tick.unwrap().get());
        let header = MbpSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        let (ts, delta) = normalize_delta(&header, &scratch, env.ts.unwrap());
        assert_eq!(ts, UnixNanos::from_millis(1700000000100));
        assert_eq!(
            delta.sequence,
            Some(SequenceRange::new(100020146795, 100020146795))
        );
        assert_eq!(delta.bids.len(), 2);
        assert_eq!(delta.asks.len(), 2);
    }

    #[test]
    fn test_normalize_trade() {
        let json = include_str!("../../tests/fixtures/huobi/trade.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        let trade_tick: RawTradeTick<'_> = serde_json::from_str(env.tick.unwrap().get()).unwrap();

        let (ts, trade) = normalize_trade(&trade_tick.data[0]).unwrap();
        assert_eq!(ts, UnixNanos::from_millis(1700000000050));
        assert_eq!(trade.aggressor, Side::Bid);
        assert_eq!(trade.price, Decimal::from_str_exact("26000.1").unwrap());
        assert_eq!(trade.volume, Decimal::from_str_exact("0.01").unwrap());

        let id = trade.identity.unwrap();
        assert_eq!(id.namespace.as_str(), "huobi");
        assert_eq!(id.value.as_str(), "102043494568");
    }
}
