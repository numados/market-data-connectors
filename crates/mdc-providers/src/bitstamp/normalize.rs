//! Normalization of Bitstamp raw payloads into canonical `mdc-core` domain models.

use mdc_core::{
    L2Delta, L2Snapshot, MdcError, NativeTradeIdentity, Side, Trade, TradeId, TradeNamespace,
    UnixNanos,
};

use crate::bitstamp::raw::{BitstampConnectionScratch, RawTrade};

/// Normalizes an authoritative order book snapshot into canonical `(UnixNanos, L2Snapshot)`.
pub fn normalize_snapshot(
    microtimestamp: u64,
    scratch: &BitstampConnectionScratch,
) -> (UnixNanos, L2Snapshot) {
    let exchange_ts = UnixNanos::from_micros(microtimestamp);
    let snapshot = L2Snapshot {
        last_update_id: None,
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };
    (exchange_ts, snapshot)
}

/// Normalizes an incremental order book delta update into canonical `(UnixNanos, L2Delta)`.
pub fn normalize_depth(
    microtimestamp: u64,
    scratch: &BitstampConnectionScratch,
) -> (UnixNanos, L2Delta) {
    let exchange_ts = UnixNanos::from_micros(microtimestamp);
    let delta = L2Delta {
        sequence: None,
        bids: scratch.bids.clone(),
        asks: scratch.asks.clone(),
    };
    (exchange_ts, delta)
}

/// Normalizes a public trade into canonical `(UnixNanos, Trade)`.
pub fn normalize_trade(raw: &RawTrade) -> Result<(UnixNanos, Trade), MdcError> {
    let exchange_ts = UnixNanos::from_micros(raw.microtimestamp);

    let side = match raw.side {
        0 => Side::Bid,
        1 => Side::Ask,
        other => {
            return Err(MdcError::Protocol(format!(
                "invalid Bitstamp trade aggressor side: {other}"
            )));
        }
    };

    let id_str = raw.id.to_string();
    let trade_id =
        TradeId::new(&id_str).map_err(|e| MdcError::Protocol(format!("invalid trade id: {e}")))?;
    let namespace = TradeNamespace::new("bitstamp")
        .map_err(|e| MdcError::Protocol(format!("invalid trade namespace: {e}")))?;
    let identity = NativeTradeIdentity::new(namespace, trade_id);

    let trade = Trade::new(Some(identity), raw.price_str, raw.amount_str, side);
    Ok((exchange_ts, trade))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::bitstamp::raw::{
        BitstampConnectionScratch, BitstampDiffFrameSeed, RawEnvelope, RawTrade,
    };
    use rust_decimal::Decimal;
    use serde::de::DeserializeSeed;

    #[test]
    fn test_normalize_snapshot() {
        let mut scratch = BitstampConnectionScratch::with_capacity(16);
        scratch.bids.push(mdc_core::PriceLevel::new(
            Decimal::from(50000),
            Decimal::from(1),
        ));
        scratch.asks.push(mdc_core::PriceLevel::new(
            Decimal::from(50001),
            Decimal::from(1),
        ));

        let (ts, snap) = normalize_snapshot(1700000000100000, &scratch);
        assert_eq!(ts, UnixNanos::from_micros(1700000000100000));
        assert_eq!(snap.last_update_id, None);
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.asks.len(), 1);
    }

    #[test]
    fn test_normalize_depth() {
        let data = include_str!("../../tests/fixtures/bitstamp/depth_update.json");
        let mut scratch = BitstampConnectionScratch::with_capacity(16);
        let mut de = serde_json::Deserializer::from_str(data);
        let header = BitstampDiffFrameSeed::new(&mut scratch)
            .deserialize(&mut de)
            .unwrap();

        let (ts, delta) = normalize_depth(header.microtimestamp, &scratch);
        assert_eq!(ts, UnixNanos::from_micros(1700000000123456));
        assert_eq!(delta.sequence, None);
        assert_eq!(delta.bids.len(), 1);
        assert_eq!(delta.asks.len(), 1);
        assert_eq!(delta.bids[0].price, Decimal::from(50000));
        assert_eq!(
            delta.asks[0].price,
            Decimal::from_str_exact("50001.50").unwrap()
        );
    }

    #[test]
    fn test_normalize_trade() {
        let data = include_str!("../../tests/fixtures/bitstamp/trade.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        let raw: RawTrade = serde_json::from_str(env.data.unwrap().get()).unwrap();

        let (ts, trade) = normalize_trade(&raw).unwrap();
        assert_eq!(ts, UnixNanos::from_micros(1700000000123456));
        assert_eq!(trade.aggressor, Side::Bid);
        assert_eq!(trade.price, Decimal::from(50000));
        assert_eq!(trade.volume, Decimal::from_str_exact("0.01000000").unwrap());

        let id = trade.identity.unwrap();
        assert_eq!(id.namespace.as_str(), "bitstamp");
        assert_eq!(id.value.as_str(), "12345678");
    }
}
