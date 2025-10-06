//! Normalization of Gate.io raw payloads into canonical `mdc-core` domain models.

use mdc_core::{
    L2Delta, L2Snapshot, MdcError, NativeTradeIdentity, SequenceRange, Side, Trade, TradeId,
    TradeNamespace, UnixNanos,
};

use crate::gateio::raw::{GateioConnectionScratch, ObuHeader, RawTrade, parse_create_time_ms};

/// Normalizes an authoritative order book snapshot into canonical `(UnixNanos, L2Snapshot)`.
pub fn normalize_snapshot(
    header: &ObuHeader<'_>,
    scratch: &GateioConnectionScratch,
) -> (UnixNanos, L2Snapshot) {
    let exchange_ts = UnixNanos::from_millis(header.t);
    let snapshot = L2Snapshot {
        last_update_id: Some(header.last_id),
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };
    (exchange_ts, snapshot)
}

/// Normalizes an incremental order book delta update into canonical `(UnixNanos, L2Delta)`.
pub fn normalize_depth(
    header: &ObuHeader<'_>,
    scratch: &GateioConnectionScratch,
) -> (UnixNanos, L2Delta) {
    let exchange_ts = UnixNanos::from_millis(header.t);
    let delta = L2Delta {
        sequence: Some(SequenceRange::new(header.first_id, header.last_id)),
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };
    (exchange_ts, delta)
}

/// Normalizes a public trade into canonical `(UnixNanos, Trade)`.
pub fn normalize_trade(raw: &RawTrade<'_>) -> Result<(UnixNanos, Trade), MdcError> {
    let exchange_ts = parse_create_time_ms(raw.create_time_ms)
        .map_err(|e| MdcError::Protocol(format!("invalid trade timestamp: {e}")))?;

    let side = match raw.side {
        "buy" => Side::Bid,
        "sell" => Side::Ask,
        other => {
            return Err(MdcError::Protocol(format!(
                "invalid trade aggressor side: {other}"
            )));
        }
    };

    let id_str = raw.id.to_small_str();
    let trade_id = TradeId::new(id_str.as_str())
        .map_err(|e| MdcError::Protocol(format!("invalid trade id: {e}")))?;
    let namespace = TradeNamespace::new("gateio")
        .map_err(|e| MdcError::Protocol(format!("invalid trade namespace: {e}")))?;
    let identity = NativeTradeIdentity::new(namespace, trade_id);

    let trade = Trade::new(Some(identity), raw.price, raw.amount, side);
    Ok((exchange_ts, trade))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::gateio::raw::{ObuSeed, RawEnvelope, RawTrade};
    use rust_decimal::Decimal;
    use serde::de::DeserializeSeed;

    #[test]
    fn test_normalize_snapshot() {
        let data = include_str!("../../tests/fixtures/gateio/order_book_snapshot.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        let mut scratch = GateioConnectionScratch::with_capacity(16);
        let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
        let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        let (ts, snapshot) = normalize_snapshot(&header, &scratch);

        assert_eq!(ts, UnixNanos::from_millis(1_700_000_000_000));
        assert_eq!(snapshot.last_update_id, Some(123456));
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
    }

    #[test]
    fn test_normalize_depth() {
        let data = include_str!("../../tests/fixtures/gateio/depth_update.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        let mut scratch = GateioConnectionScratch::with_capacity(16);
        let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
        let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        let (ts, delta) = normalize_depth(&header, &scratch);

        assert_eq!(ts, UnixNanos::from_millis(1_700_000_000_100));
        assert_eq!(delta.sequence, Some(SequenceRange::new(123457, 123460)));
        assert_eq!(delta.bids.len(), 2);
        assert_eq!(delta.asks.len(), 1);
    }

    #[test]
    fn test_normalize_trade() {
        let data = include_str!("../../tests/fixtures/gateio/trade.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        let raw_trade: RawTrade<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();

        let (ts, trade) = normalize_trade(&raw_trade).unwrap();

        assert_eq!(ts, UnixNanos(1_700_000_000_123_456_000));
        assert_eq!(trade.aggressor, Side::Ask);
        assert_eq!(trade.price, Decimal::from(50000));
        assert_eq!(trade.volume, Decimal::from_str_exact("0.01").unwrap());

        let id = trade.identity.unwrap();
        assert_eq!(id.namespace.as_str(), "gateio");
        assert_eq!(id.value.as_str(), "987654321");
    }
}
