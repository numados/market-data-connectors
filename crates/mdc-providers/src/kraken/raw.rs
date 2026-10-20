use std::fmt;

use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::value::RawValue;

use crate::kraken::checksum::RawLevelOwned;

/// Raw price level borrowing exact JSON number tokens for checksum computation.
#[derive(Debug, Deserialize)]
pub struct RawLevel<'a> {
    /// Exact raw JSON number token for price, e.g. `"45283.5"`.
    #[serde(borrow)]
    pub price: &'a RawValue,
    /// Exact raw JSON number token for quantity, e.g. `"0.10000000"`.
    #[serde(borrow)]
    pub qty: &'a RawValue,
}

/// Raw order book entry in `data` array of `book` channel messages.
#[derive(Debug, Deserialize)]
pub struct RawBookData<'a> {
    /// Currency pair symbol, e.g. `"BTC/USD"`.
    pub symbol: &'a str,
    /// Bid levels.
    #[serde(borrow, default)]
    pub bids: Vec<RawLevel<'a>>,
    /// Ask levels.
    #[serde(borrow, default)]
    pub asks: Vec<RawLevel<'a>>,
    /// CRC32 checksum for top 10 bids and asks.
    pub checksum: Option<u32>,
    /// Update timestamp (RFC 3339), present on updates.
    pub timestamp: Option<&'a str>,
}

/// Raw public trade payload on `trade` channel.
#[derive(Debug, Deserialize)]
pub struct RawTrade<'a> {
    /// Currency pair symbol, e.g. `"BTC/USD"`.
    pub symbol: &'a str,
    /// Aggressor side: `"buy"` or `"sell"`.
    pub side: &'a str,
    /// Trade price.
    pub price: Decimal,
    /// Trade quantity.
    pub qty: Decimal,
    /// Order type, e.g. `"market"` or `"limit"`.
    #[serde(default)]
    pub ord_type: Option<&'a str>,
    /// Unique trade identifier.
    pub trade_id: u64,
    /// Execution timestamp (RFC 3339).
    pub timestamp: &'a str,
}

/// Primary tagged envelope for Kraken WebSocket v2 messages.
#[derive(Debug)]
pub enum RawMessage<'a> {
    /// Order book snapshot or update.
    Book {
        /// Message kind: `"snapshot"` or `"update"`.
        kind: &'a str,
        /// Order book data per symbol.
        data: Vec<RawBookData<'a>>,
    },
    /// Public trade executions.
    Trade {
        /// Trades array.
        data: Vec<RawTrade<'a>>,
    },
    /// Connection heartbeat emitted approximately once per second.
    Heartbeat,
    /// Connection or engine status message.
    Status,
    /// Method acknowledgment response (subscribe, unsubscribe, etc.).
    MethodAck(RawMethodAck<'a>),
    /// Unhandled channel or method response without a `channel` tag.
    Other,
}

impl<'a> RawMessage<'a> {
    /// Parses a raw JSON string into a `RawMessage` borrowing directly from the input.
    pub fn parse(s: &'a str) -> Result<Self, serde_json::Error> {
        #[derive(Deserialize)]
        struct EnvelopeHeader<'a> {
            channel: Option<&'a str>,
            method: Option<&'a str>,
        }

        let header: EnvelopeHeader<'a> = serde_json::from_str(s)?;
        match header.channel {
            Some("book") => {
                #[derive(Deserialize)]
                struct BookWrapper<'a> {
                    #[serde(rename = "type")]
                    kind: &'a str,
                    #[serde(borrow)]
                    data: Vec<RawBookData<'a>>,
                }
                let w: BookWrapper<'a> = serde_json::from_str(s)?;
                Ok(RawMessage::Book {
                    kind: w.kind,
                    data: w.data,
                })
            }
            Some("trade") => {
                #[derive(Deserialize)]
                struct TradeWrapper<'a> {
                    #[serde(borrow)]
                    data: Vec<RawTrade<'a>>,
                }
                let w: TradeWrapper<'a> = serde_json::from_str(s)?;
                Ok(RawMessage::Trade { data: w.data })
            }
            Some("heartbeat") => Ok(RawMessage::Heartbeat),
            Some("status") => Ok(RawMessage::Status),
            None if header.method.is_some() => {
                let ack: RawMethodAck<'a> = serde_json::from_str(s)?;
                Ok(RawMessage::MethodAck(ack))
            }
            _ => Ok(RawMessage::Other),
        }
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for RawMessage<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = <&'de RawValue>::deserialize(deserializer)?;
        RawMessage::parse(raw.get()).map_err(serde::de::Error::custom)
    }
}

/// Method acknowledgment payload (e.g. subscribe / unsubscribe responses).
#[derive(Debug, Deserialize)]
pub struct RawMethodAck<'a> {
    /// Method name, e.g. `"subscribe"` or `"unsubscribe"`.
    pub method: &'a str,
    /// Indicates whether request succeeded.
    pub success: bool,
    /// Success result details.
    #[serde(default)]
    pub result: Option<RawAckResult<'a>>,
    /// Error message when `success == false`.
    #[serde(default)]
    pub error: Option<&'a str>,
    /// Symbol associated with request, if provided at top-level.
    #[serde(default)]
    pub symbol: Option<&'a str>,
    /// Request receipt timestamp (RFC 3339).
    #[serde(default)]
    pub time_in: Option<&'a str>,
    /// Response transmit timestamp (RFC 3339).
    #[serde(default)]
    pub time_out: Option<&'a str>,
}

/// Acknowledgment result payload.
#[derive(Debug, Deserialize)]
pub struct RawAckResult<'a> {
    /// Channel name.
    #[serde(default)]
    pub channel: Option<&'a str>,
    /// Depth level.
    #[serde(default)]
    pub depth: Option<usize>,
    /// Whether snapshot was requested.
    #[serde(default)]
    pub snapshot: Option<bool>,
    /// Symbol associated with subscription.
    #[serde(default)]
    pub symbol: Option<&'a str>,
}

/// Connection scratch buffers for Kraken depth parsing.
#[derive(Debug, Default)]
pub struct KrakenConnectionScratch {
    /// Scratch buffer for bid price levels.
    pub bids: Vec<RawLevelOwned>,
    /// Scratch buffer for ask price levels.
    pub asks: Vec<RawLevelOwned>,
}

impl KrakenConnectionScratch {
    /// Creates a new `KrakenConnectionScratch` with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            bids: Vec::with_capacity(cap),
            asks: Vec::with_capacity(cap),
        }
    }

    /// Clears all scratch buffers while retaining allocated capacity.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

/// Deserializer seed that fills pre-allocated scratch vectors with `RawLevelOwned` entries.
pub struct KrakenLevelsSeed<'buf> {
    buf: &'buf mut Vec<RawLevelOwned>,
}

impl<'buf> KrakenLevelsSeed<'buf> {
    /// Creates a new `KrakenLevelsSeed` targeting the given scratch vector.
    pub fn new(buf: &'buf mut Vec<RawLevelOwned>) -> Self {
        buf.clear();
        Self { buf }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for KrakenLevelsSeed<'buf> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(KrakenLevelsVisitor(self.buf))
    }
}

struct KrakenLevelsVisitor<'buf>(&'buf mut Vec<RawLevelOwned>);

impl<'de, 'buf> Visitor<'de> for KrakenLevelsVisitor<'buf> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of price/qty level objects")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(level) = seq.next_element::<RawLevelOwned>()? {
            self.0.push(level);
        }
        Ok(())
    }
}

/// Header metadata of a Kraken order book update or snapshot message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KrakenBookDataHeader<'a> {
    /// Message type: `"snapshot"` or `"update"`.
    pub kind: &'a str,
    /// Trading symbol, e.g. `"BTC/USD"`.
    pub symbol: &'a str,
    /// CRC32 checksum from venue.
    pub checksum: Option<u32>,
    /// RFC 3339 timestamp for updates.
    pub timestamp: Option<&'a str>,
}

/// Zero-allocation deserializer seed for Kraken book frames targeting scratch buffers.
pub struct KrakenDepthSeed<'buf> {
    /// Reference to connection scratch buffers.
    pub scratch: &'buf mut KrakenConnectionScratch,
}

impl<'buf> KrakenDepthSeed<'buf> {
    /// Creates a new `KrakenDepthSeed` targeting the given scratch buffers.
    pub fn new(scratch: &'buf mut KrakenConnectionScratch) -> Self {
        scratch.clear();
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for KrakenDepthSeed<'buf> {
    type Value = KrakenBookDataHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(KrakenDepthOuterVisitor {
            scratch: self.scratch,
        })
    }
}

struct KrakenDepthOuterVisitor<'buf> {
    scratch: &'buf mut KrakenConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for KrakenDepthOuterVisitor<'buf> {
    type Value = KrakenBookDataHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Kraken book WebSocket frame object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut kind = None;
        let mut header = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "type" => kind = Some(map.next_value::<&'de str>()?),
                "data" => {
                    header = Some(map.next_value_seed(KrakenDataArraySeed {
                        scratch: self.scratch,
                    })?);
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let kind = kind.ok_or_else(|| serde::de::Error::missing_field("type"))?;
        let (symbol, checksum, timestamp) =
            header.ok_or_else(|| serde::de::Error::missing_field("data"))?;

        Ok(KrakenBookDataHeader {
            kind,
            symbol,
            checksum,
            timestamp,
        })
    }
}

struct KrakenDataArraySeed<'buf> {
    scratch: &'buf mut KrakenConnectionScratch,
}

impl<'de, 'buf> DeserializeSeed<'de> for KrakenDataArraySeed<'buf> {
    type Value = (&'de str, Option<u32>, Option<&'de str>);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(KrakenDataArrayVisitor {
            scratch: self.scratch,
        })
    }
}

struct KrakenDataArrayVisitor<'buf> {
    scratch: &'buf mut KrakenConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for KrakenDataArrayVisitor<'buf> {
    type Value = (&'de str, Option<u32>, Option<&'de str>);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a 1-element array containing order book data")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let res = seq.next_element_seed(KrakenSingleDataSeed {
            scratch: self.scratch,
        })?;
        res.ok_or_else(|| serde::de::Error::custom("expected at least 1 element in data array"))
    }
}

struct KrakenSingleDataSeed<'buf> {
    scratch: &'buf mut KrakenConnectionScratch,
}

impl<'de, 'buf> DeserializeSeed<'de> for KrakenSingleDataSeed<'buf> {
    type Value = (&'de str, Option<u32>, Option<&'de str>);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(KrakenSingleDataVisitor {
            scratch: self.scratch,
        })
    }
}

struct KrakenSingleDataVisitor<'buf> {
    scratch: &'buf mut KrakenConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for KrakenSingleDataVisitor<'buf> {
    type Value = (&'de str, Option<u32>, Option<&'de str>);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an order book data object with bids, asks, symbol")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut symbol = None;
        let mut checksum = None;
        let mut timestamp = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "symbol" => symbol = Some(map.next_value::<&'de str>()?),
                "checksum" => checksum = map.next_value::<Option<u32>>()?,
                "timestamp" => timestamp = map.next_value::<Option<&'de str>>()?,
                "bids" => {
                    map.next_value_seed(KrakenLevelsSeed::new(&mut self.scratch.bids))?;
                }
                "asks" => {
                    map.next_value_seed(KrakenLevelsSeed::new(&mut self.scratch.asks))?;
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let symbol = symbol.ok_or_else(|| serde::de::Error::missing_field("symbol"))?;
        Ok((symbol, checksum, timestamp))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const SNAPSHOT_JSON: &str =
        include_str!("../../tests/fixtures/kraken/order_book_snapshot.json");
    const UPDATE_JSON: &str = include_str!("../../tests/fixtures/kraken/depth_update.json");
    const DELETE_JSON: &str =
        include_str!("../../tests/fixtures/kraken/depth_update_delete_level.json");
    const HEARTBEAT_JSON: &str = include_str!("../../tests/fixtures/kraken/heartbeat_or_ping.json");
    const SUB_ACK_JSON: &str = include_str!("../../tests/fixtures/kraken/subscribe_ack.json");
    const SUB_ERR_JSON: &str = include_str!("../../tests/fixtures/kraken/subscribe_ack_error.json");
    const TRADE_JSON: &str = include_str!("../../tests/fixtures/kraken/trade.json");

    #[test]
    fn test_parse_snapshot_fixture() {
        let msg: RawMessage<'_> =
            serde_json::from_str(SNAPSHOT_JSON).expect("deserialize snapshot");
        match msg {
            RawMessage::Book { kind, data } => {
                assert_eq!(kind, "snapshot");
                assert_eq!(data.len(), 1);
                let book = &data[0];
                assert_eq!(book.symbol, "BTC/USD");
                assert_eq!(book.checksum, Some(3310070434));
                assert_eq!(book.bids.len(), 10);
                assert_eq!(book.asks.len(), 10);
                assert_eq!(book.bids[0].price.get(), "45283.5");
                assert_eq!(book.bids[0].qty.get(), "0.10000000");
                assert_eq!(book.asks[0].price.get(), "45285.2");
                assert_eq!(book.asks[0].qty.get(), "0.00100000");
            }
            other => panic!("expected RawMessage::Book, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_update_fixture() {
        let msg: RawMessage<'_> = serde_json::from_str(UPDATE_JSON).expect("deserialize update");
        match msg {
            RawMessage::Book { kind, data } => {
                assert_eq!(kind, "update");
                assert_eq!(data.len(), 1);
                let book = &data[0];
                assert_eq!(book.symbol, "BTC/USD");
                assert_eq!(book.checksum, Some(38355977));
                assert_eq!(book.timestamp, Some("2023-10-06T17:35:55.440295Z"));
                assert_eq!(book.bids.len(), 1);
                assert_eq!(book.asks.len(), 0);
                assert_eq!(book.bids[0].price.get(), "45283.5");
                assert_eq!(book.bids[0].qty.get(), "0.20000000");
            }
            other => panic!("expected RawMessage::Book, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_delete_level_fixture() {
        let msg: RawMessage<'_> =
            serde_json::from_str(DELETE_JSON).expect("deserialize delete level");
        match msg {
            RawMessage::Book { kind, data } => {
                assert_eq!(kind, "update");
                assert_eq!(data.len(), 1);
                let book = &data[0];
                assert_eq!(book.symbol, "BTC/USD");
                assert_eq!(book.checksum, Some(1166728830));
                assert_eq!(book.bids.len(), 1);
                assert_eq!(book.bids[0].price.get(), "45283.5");
                assert_eq!(book.bids[0].qty.get(), "0.0");
            }
            other => panic!("expected RawMessage::Book, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_heartbeat_fixture() {
        let msg: RawMessage<'_> =
            serde_json::from_str(HEARTBEAT_JSON).expect("deserialize heartbeat");
        assert!(matches!(msg, RawMessage::Heartbeat));
    }

    #[test]
    fn test_parse_trade_fixture() {
        let msg: RawMessage<'_> = serde_json::from_str(TRADE_JSON).expect("deserialize trade");
        match msg {
            RawMessage::Trade { data } => {
                assert_eq!(data.len(), 1);
                let t = &data[0];
                assert_eq!(t.symbol, "BTC/USD");
                assert_eq!(t.side, "buy");
                assert_eq!(t.price.to_string(), "45285.2");
                assert_eq!(t.qty.to_string(), "0.001");
                assert_eq!(t.trade_id, 12345678);
                assert_eq!(t.timestamp, "2023-10-06T17:35:55.440295Z");
            }
            other => panic!("expected RawMessage::Trade, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_subscribe_ack_fixture() {
        let ack: RawMethodAck<'_> =
            serde_json::from_str(SUB_ACK_JSON).expect("deserialize subscribe ack");
        assert_eq!(ack.method, "subscribe");
        assert!(ack.success);
        let result = ack.result.expect("ack result");
        assert_eq!(result.channel, Some("book"));
        assert_eq!(result.depth, Some(1000));
        assert_eq!(result.symbol, Some("BTC/USD"));
    }

    #[test]
    fn test_parse_subscribe_error_ack_fixture() {
        let ack: RawMethodAck<'_> =
            serde_json::from_str(SUB_ERR_JSON).expect("deserialize error ack");
        assert_eq!(ack.method, "subscribe");
        assert!(!ack.success);
        assert_eq!(ack.error, Some("Subscription rejected: symbol not found"));
        assert_eq!(ack.symbol, Some("INVALID/USD"));
    }

    #[test]
    fn test_parse_dispatches_subscribe_error_ack() {
        match RawMessage::parse(SUB_ERR_JSON).expect("parse error ack") {
            RawMessage::MethodAck(ack) => {
                assert_eq!(ack.method, "subscribe");
                assert!(!ack.success);
                assert_eq!(ack.symbol, Some("INVALID/USD"));
                assert_eq!(ack.error, Some("Subscription rejected: symbol not found"));
            }
            other => panic!("expected RawMessage::MethodAck, got {other:?}"),
        }
    }

    #[test]
    fn test_raw_value_single_pass_borrows_substring() {
        // Assert RawValue.get() points to the exact slice within the JSON string
        let msg: RawMessage<'_> = serde_json::from_str(UPDATE_JSON).expect("deserialize");
        if let RawMessage::Book { data, .. } = msg {
            let price_str = data[0].bids[0].price.get();
            let json_start = UPDATE_JSON.as_ptr() as usize;
            let price_start = price_str.as_ptr() as usize;
            assert!(price_start >= json_start);
            assert!(price_start < json_start + UPDATE_JSON.len());
        } else {
            panic!("expected Book message");
        }
    }
}
