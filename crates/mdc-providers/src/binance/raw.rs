//! Raw exchange message schemas and zero-copy deserialization for Binance Spot.

use mdc_core::PriceLevel;
use rust_decimal::Decimal;
use serde::de::{DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;

/// Lightweight combined WebSocket stream envelope borrowing the raw data payload.
#[derive(Debug, Deserialize)]
pub struct StreamEnvelope<'a> {
    pub stream: &'a str,
    #[serde(borrow)]
    pub data: &'a serde_json::value::RawValue,
}

/// Combined WebSocket stream envelope: `{"stream":"btcusdt@depth@100ms","data":{...}}`.
#[derive(Debug, Deserialize)]
pub struct CombinedStream<'a> {
    pub stream: &'a str,
    #[serde(borrow)]
    pub data: RawPayload<'a>,
}

/// Untagged enum over possible Binance stream payloads.
/// Ordered by arrival frequency: BookTicker -> Depth -> Trade.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum RawPayload<'a> {
    #[serde(borrow)]
    BookTicker(RawBookTicker<'a>),
    #[serde(borrow)]
    Depth(RawDepthUpdate<'a>),
    #[serde(borrow)]
    Trade(RawTrade<'a>),
}

/// Raw depth diff update payload (`@depth@100ms`).
#[derive(Debug, Deserialize)]
pub struct RawDepthUpdate<'a> {
    #[serde(rename = "E")]
    pub event_time_ms: u64,
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub last_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<[Decimal; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[Decimal; 2]>,
}

/// Raw top-of-book ticker payload (`@bookTicker`).
#[derive(Debug, Deserialize)]
pub struct RawBookTicker<'a> {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "b")]
    pub best_bid_price: Decimal,
    #[serde(rename = "B")]
    pub best_bid_qty: Decimal,
    #[serde(rename = "a")]
    pub best_ask_price: Decimal,
    #[serde(rename = "A")]
    pub best_ask_qty: Decimal,
}

/// Raw public trade payload (`@trade`).
#[derive(Debug, Deserialize)]
pub struct RawTrade<'a> {
    #[serde(rename = "E")]
    pub event_time_ms: u64,
    #[serde(rename = "T")]
    pub trade_time_ms: u64,
    #[serde(rename = "s")]
    pub symbol: &'a str,
    #[serde(rename = "t")]
    pub trade_id: u64,
    #[serde(rename = "p")]
    pub price: Decimal,
    #[serde(rename = "q")]
    pub qty: Decimal,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

/// REST depth snapshot response (`/api/v3/depth?limit=1000`).
#[derive(Debug, Deserialize)]
pub struct RawDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<[Decimal; 2]>,
    pub asks: Vec<[Decimal; 2]>,
}

/// Deserializer seed that fills pre-allocated scratch vectors with `PriceLevel` entries.
///
/// Avoids allocating intermediate `Vec<[Decimal; 2]>` on the streaming hot path.
pub struct LevelsSeed<'buf> {
    buf: &'buf mut Vec<PriceLevel>,
}

impl<'buf> LevelsSeed<'buf> {
    /// Creates a new `LevelsSeed` targeting the given scratch vector.
    pub fn new(buf: &'buf mut Vec<PriceLevel>) -> Self {
        buf.clear();
        Self { buf }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for LevelsSeed<'buf> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(LevelsVisitor(self.buf))
    }
}

struct LevelsVisitor<'buf>(&'buf mut Vec<PriceLevel>);

impl<'de, 'buf> Visitor<'de> for LevelsVisitor<'buf> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of [price, qty] string pairs")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some([price, volume]) = seq.next_element::<[Decimal; 2]>()? {
            self.0.push(PriceLevel::new(price, volume));
        }
        Ok(())
    }
}

/// Header metadata of a depth update message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthUpdateHeader<'a> {
    pub event_time_ms: u64,
    pub symbol: &'a str,
    pub first_update_id: u64,
    pub last_update_id: u64,
}

/// Zero-allocation deserializer seed for raw depth updates.
pub struct DepthUpdateSeed<'buf> {
    pub scratch: &'buf mut ConnectionScratchBuffers,
}

impl<'buf> DepthUpdateSeed<'buf> {
    /// Creates a new `DepthUpdateSeed` targeting the given scratch buffers.
    pub fn new(scratch: &'buf mut ConnectionScratchBuffers) -> Self {
        scratch.clear();
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for DepthUpdateSeed<'buf> {
    type Value = DepthUpdateHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(DepthUpdateVisitor {
            scratch: self.scratch,
        })
    }
}

struct DepthUpdateVisitor<'buf> {
    scratch: &'buf mut ConnectionScratchBuffers,
}

impl<'de, 'buf> Visitor<'de> for DepthUpdateVisitor<'buf> {
    type Value = DepthUpdateHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Binance depth update JSON object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: serde::de::MapAccess<'de>,
    {
        let mut event_time_ms = None;
        let mut symbol = None;
        let mut first_update_id = None;
        let mut last_update_id = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "E" => event_time_ms = Some(map.next_value::<u64>()?),
                "s" => symbol = Some(map.next_value::<&'de str>()?),
                "U" => first_update_id = Some(map.next_value::<u64>()?),
                "u" => last_update_id = Some(map.next_value::<u64>()?),
                "b" => {
                    map.next_value_seed(LevelsSeed::new(&mut self.scratch.bids))?;
                }
                "a" => {
                    map.next_value_seed(LevelsSeed::new(&mut self.scratch.asks))?;
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let event_time_ms = event_time_ms.ok_or_else(|| serde::de::Error::missing_field("E"))?;
        let symbol = symbol.ok_or_else(|| serde::de::Error::missing_field("s"))?;
        let first_update_id =
            first_update_id.ok_or_else(|| serde::de::Error::missing_field("U"))?;
        let last_update_id = last_update_id.ok_or_else(|| serde::de::Error::missing_field("u"))?;

        Ok(DepthUpdateHeader {
            event_time_ms,
            symbol,
            first_update_id,
            last_update_id,
        })
    }
}

/// Header metadata of a combined stream depth update envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CombinedDepthHeader<'a> {
    pub stream: &'a str,
    pub depth: DepthUpdateHeader<'a>,
}

/// Zero-allocation deserializer seed for a combined stream depth envelope.
pub struct CombinedDepthSeed<'buf> {
    pub scratch: &'buf mut ConnectionScratchBuffers,
}

impl<'buf> CombinedDepthSeed<'buf> {
    /// Creates a new `CombinedDepthSeed` targeting the given scratch buffers.
    pub fn new(scratch: &'buf mut ConnectionScratchBuffers) -> Self {
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for CombinedDepthSeed<'buf> {
    type Value = CombinedDepthHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(CombinedDepthVisitor {
            scratch: self.scratch,
        })
    }
}

struct CombinedDepthVisitor<'buf> {
    scratch: &'buf mut ConnectionScratchBuffers,
}

impl<'de, 'buf> Visitor<'de> for CombinedDepthVisitor<'buf> {
    type Value = CombinedDepthHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Binance combined stream depth update envelope")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: serde::de::MapAccess<'de>,
    {
        let mut stream = None;
        let mut depth = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "stream" => stream = Some(map.next_value::<&'de str>()?),
                "data" => {
                    depth = Some(map.next_value_seed(DepthUpdateSeed::new(self.scratch))?);
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let stream = stream.ok_or_else(|| serde::de::Error::missing_field("stream"))?;
        let depth = depth.ok_or_else(|| serde::de::Error::missing_field("data"))?;

        Ok(CombinedDepthHeader { stream, depth })
    }
}

/// Zero-copy scratch buffer pair for price level deserialization.
#[derive(Debug, Default)]
pub struct ConnectionScratchBuffers {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

impl ConnectionScratchBuffers {
    /// Creates a new scratch buffer with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
        }
    }

    /// Clears both bid and ask scratch vectors while retaining capacity.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Populates scratch buffers from a raw depth update.
    pub fn fill_from_depth(&mut self, depth: &RawDepthUpdate<'_>) {
        self.clear();
        for &[price, volume] in &depth.bids {
            self.bids.push(PriceLevel::new(price, volume));
        }
        for &[price, volume] in &depth.asks {
            self.asks.push(PriceLevel::new(price, volume));
        }
    }

    /// Populates scratch buffers from a raw depth snapshot.
    pub fn fill_from_snapshot(&mut self, snap: &RawDepthSnapshot) {
        self.clear();
        for &[price, volume] in &snap.bids {
            self.bids.push(PriceLevel::new(price, volume));
        }
        for &[price, volume] in &snap.asks {
            self.asks.push(PriceLevel::new(price, volume));
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use mdc_testkit::load_fixture;

    fn d(s: &str) -> Decimal {
        Decimal::from_str_exact(s).expect("valid decimal string")
    }

    #[test]
    fn test_parse_depth_update_fixture() {
        let fixture = load_fixture("crates/mdc-providers/tests/fixtures/binance/depth_update.json")
            .expect("load fixture");
        let parsed: CombinedStream<'_> =
            serde_json::from_str(&fixture).expect("parse combined stream");

        assert_eq!(parsed.stream, "btcusdt@depth@100ms");
        match parsed.data {
            RawPayload::Depth(depth) => {
                assert_eq!(depth.symbol, "BTCUSDT");
                assert_eq!(depth.event_time_ms, 1672515782136);
                assert_eq!(depth.first_update_id, 157);
                assert_eq!(depth.last_update_id, 160);
                assert_eq!(depth.bids.len(), 2);
                assert_eq!(depth.asks.len(), 2);
                assert_eq!(depth.bids[0][0], d("0.00240000"));
                assert_eq!(depth.bids[0][1], d("10.00000000"));
                assert_eq!(depth.asks[0][0], d("0.00260000"));
                assert_eq!(depth.asks[0][1], d("100.00000000"));
                assert_eq!(depth.asks[1][0], d("0.00270000"));
                assert_eq!(depth.asks[1][1], d("50.00000000"));
            }
            _ => panic!("Expected RawPayload::Depth"),
        }
    }

    #[test]
    fn test_parse_book_ticker_fixture() {
        let fixture = load_fixture("crates/mdc-providers/tests/fixtures/binance/book_ticker.json")
            .expect("load fixture");
        let parsed: CombinedStream<'_> =
            serde_json::from_str(&fixture).expect("parse combined stream");

        assert_eq!(parsed.stream, "btcusdt@bookTicker");
        match parsed.data {
            RawPayload::BookTicker(ticker) => {
                assert_eq!(ticker.symbol, "BTCUSDT");
                assert_eq!(ticker.update_id, 400900217);
                assert_eq!(ticker.best_bid_price, d("25.35190000"));
                assert_eq!(ticker.best_bid_qty, d("31.21000000"));
                assert_eq!(ticker.best_ask_price, d("25.36520000"));
                assert_eq!(ticker.best_ask_qty, d("40.66000000"));
            }
            _ => panic!("Expected RawPayload::BookTicker"),
        }
    }

    #[test]
    fn test_parse_trade_fixture() {
        let fixture = load_fixture("crates/mdc-providers/tests/fixtures/binance/trade.json")
            .expect("load fixture");
        let parsed: CombinedStream<'_> =
            serde_json::from_str(&fixture).expect("parse combined stream");

        assert_eq!(parsed.stream, "btcusdt@trade");
        match parsed.data {
            RawPayload::Trade(trade) => {
                assert_eq!(trade.symbol, "BTCUSDT");
                assert_eq!(trade.event_time_ms, 1672515782136);
                assert_eq!(trade.trade_time_ms, 1672515782136);
                assert_eq!(trade.trade_id, 12345);
                assert_eq!(trade.price, d("0.00100000"));
                assert_eq!(trade.qty, d("100.00000000"));
                assert!(trade.is_buyer_maker);
            }
            _ => panic!("Expected RawPayload::Trade"),
        }
    }

    #[test]
    fn test_parse_snapshot_fixture() {
        let fixture =
            load_fixture("crates/mdc-providers/tests/fixtures/binance/order_book_snapshot.json")
                .expect("load fixture");
        let snapshot: RawDepthSnapshot =
            serde_json::from_str(&fixture).expect("parse depth snapshot");

        assert_eq!(snapshot.last_update_id, 1027024);
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 2);
        assert_eq!(snapshot.bids[0][0], d("4.00000000"));
        assert_eq!(snapshot.bids[0][1], d("431.00000000"));
        assert_eq!(snapshot.asks[0][0], d("4.00000200"));
        assert_eq!(snapshot.asks[0][1], d("12.00000000"));
    }

    #[test]
    fn test_parse_empty_sides_fixture() {
        let fixture = load_fixture(
            "crates/mdc-providers/tests/fixtures/binance/depth_update_empty_sides.json",
        )
        .expect("load fixture");
        let parsed: CombinedStream<'_> = serde_json::from_str(&fixture).expect("parse empty sides");

        match parsed.data {
            RawPayload::Depth(depth) => {
                assert_eq!(depth.symbol, "BTCUSDT");
                assert_eq!(depth.first_update_id, 161);
                assert_eq!(depth.last_update_id, 165);
                assert!(depth.bids.is_empty());
                assert!(depth.asks.is_empty());
            }
            _ => panic!("Expected RawPayload::Depth"),
        }
    }

    #[test]
    fn test_scratch_buffers_reuse() {
        let mut scratch = ConnectionScratchBuffers::with_capacity(64);
        let depth = RawDepthUpdate {
            event_time_ms: 1000,
            symbol: "BTCUSDT",
            first_update_id: 1,
            last_update_id: 2,
            bids: vec![[d("100"), d("1")]],
            asks: vec![[d("101"), d("2")]],
        };

        scratch.fill_from_depth(&depth);
        assert_eq!(scratch.bids.len(), 1);
        assert_eq!(scratch.asks.len(), 1);
        assert_eq!(scratch.bids[0].price, d("100"));
        assert_eq!(scratch.asks[0].price, d("101"));

        scratch.clear();
        assert!(scratch.bids.is_empty());
        assert!(scratch.asks.is_empty());
    }
}
