//! Raw wire formats and zero-allocation deserializers for Bitstamp WebSocket and REST feeds.

use std::fmt;

use mdc_core::{PriceLevel, UnixNanos};
use rust_decimal::Decimal;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Extracts currency pair symbol from Bitstamp channel name (e.g. `"diff_order_book_btcusd"` -> `"btcusd"`).
#[inline]
pub fn extract_symbol(channel: &str) -> &str {
    match channel.rsplit_once('_') {
        Some((_, pair)) => pair,
        None => channel,
    }
}

/// Errors during microtimestamp parsing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParseTimestampError {
    /// Microtimestamp string format was invalid.
    #[error("invalid microtimestamp format")]
    InvalidFormat,
}

/// Parses a string representation of microsecond timestamp into [`UnixNanos`].
pub fn parse_microtimestamp(s: &str) -> Result<UnixNanos, ParseTimestampError> {
    let micros: u64 = s.parse().map_err(|_| ParseTimestampError::InvalidFormat)?;
    Ok(UnixNanos::from_micros(micros))
}

/// Deserializes a `u64` from either a string or integer value.
pub fn u64_from_str<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct StrOrNumVisitor;

    impl<'de> Visitor<'de> for StrOrNumVisitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a string or integer microtimestamp")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            v.parse().map_err(de::Error::custom)
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(v)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if v >= 0 {
                Ok(v as u64)
            } else {
                Err(de::Error::custom("negative microtimestamp"))
            }
        }
    }

    deserializer.deserialize_any(StrOrNumVisitor)
}

/// Top-level raw envelope for all messages on the Bitstamp WebSocket v2 feed.
#[derive(Debug, Deserialize)]
pub struct RawEnvelope<'a> {
    /// Event name (e.g. `"data"`, `"trade"`, `"bts:subscription_succeeded"`, `"bts:request_reconnect"`, `"bts:error"`).
    pub event: &'a str,
    /// Channel name (e.g. `"diff_order_book_btcusd"`, `"live_trades_btcusd"`).
    #[serde(default)]
    pub channel: &'a str,
    /// Raw payload value.
    #[serde(borrow, default)]
    pub data: Option<&'a RawValue>,
}

/// Client subscription message sent to Bitstamp WebSocket.
#[derive(Debug, Serialize)]
pub struct SubscribeMessage<'a> {
    /// Event name (`"bts:subscribe"`).
    pub event: &'static str,
    /// Subscription data payload with target channel.
    pub data: SubscribeData<'a>,
}

/// Data payload of client subscription message.
#[derive(Debug, Serialize)]
pub struct SubscribeData<'a> {
    /// Target channel (e.g. `"diff_order_book_btcusd"`).
    pub channel: &'a str,
}

impl<'a> SubscribeMessage<'a> {
    /// Creates a subscription message for the given channel.
    pub fn new(channel: &'a str) -> Self {
        Self {
            event: "bts:subscribe",
            data: SubscribeData { channel },
        }
    }
}

/// Raw order book diff payload (`data` inside `"event": "data"`).
#[derive(Debug, Deserialize, Clone)]
pub struct RawDiffData {
    /// Unix timestamp in microseconds.
    #[serde(deserialize_with = "u64_from_str")]
    pub microtimestamp: u64,
    /// Bid level updates `[price, size]`.
    #[serde(default)]
    pub bids: Vec<[Decimal; 2]>,
    /// Ask level updates `[price, size]`.
    #[serde(default)]
    pub asks: Vec<[Decimal; 2]>,
}

/// Raw REST order book snapshot body (`GET /api/v2/order_book/{pair}/`).
#[derive(Debug, Deserialize, Clone)]
pub struct RawOrderBookSnapshot {
    /// Unix timestamp in microseconds.
    #[serde(deserialize_with = "u64_from_str")]
    pub microtimestamp: u64,
    /// Authoritative bid levels `[price, size]`.
    #[serde(default)]
    pub bids: Vec<[Decimal; 2]>,
    /// Authoritative ask levels `[price, size]`.
    #[serde(default)]
    pub asks: Vec<[Decimal; 2]>,
}

/// Raw trade event (`data` inside `"event": "trade"`).
#[derive(Debug, Deserialize, Clone)]
pub struct RawTrade {
    /// Trade unique ID.
    pub id: u64,
    /// Trade amount string for exact decimal parsing.
    pub amount_str: Decimal,
    /// Trade price string for exact decimal parsing.
    pub price_str: Decimal,
    /// Trade side: `0` for Buy (Bid aggressor), `1` for Sell (Ask aggressor).
    #[serde(rename = "type")]
    pub side: u8,
    /// Microsecond timestamp.
    #[serde(deserialize_with = "u64_from_str")]
    pub microtimestamp: u64,
    /// Buyer order ID if present.
    #[serde(default)]
    pub buy_order_id: Option<u64>,
    /// Seller order ID if present.
    #[serde(default)]
    pub sell_order_id: Option<u64>,
}

/// Reusable connection scratch buffers for price levels, avoiding heap allocations on the hot path.
#[derive(Debug, Default)]
pub struct BitstampConnectionScratch {
    /// Bids scratch buffer.
    pub bids: Vec<PriceLevel>,
    /// Asks scratch buffer.
    pub asks: Vec<PriceLevel>,
}

impl BitstampConnectionScratch {
    /// Creates scratch buffers with specified initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
        }
    }

    /// Clears buffers while retaining capacity.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

/// Seed for deserializing price level arrays into a reusable `Vec<PriceLevel>`.
pub struct LevelsSeed<'buf> {
    buf: &'buf mut Vec<PriceLevel>,
}

impl<'buf> LevelsSeed<'buf> {
    /// Creates a seed that clears and writes into `buf`.
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
        deserializer.deserialize_seq(LevelsVisitor { buf: self.buf })
    }
}

struct LevelsVisitor<'buf> {
    buf: &'buf mut Vec<PriceLevel>,
}

impl<'de, 'buf> Visitor<'de> for LevelsVisitor<'buf> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of [price, size] level pairs")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some([price, volume]) = seq.next_element::<[Decimal; 2]>()? {
            self.buf.push(PriceLevel::new(price, volume));
        }
        Ok(())
    }
}

/// Header metadata for zero-allocation parsed Bitstamp `diff_order_book_*` WebSocket frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitstampDiffFrameHeader<'de> {
    /// Event name (`"data"`).
    pub event: &'de str,
    /// Channel name (`"diff_order_book_btcusd"`).
    pub channel: &'de str,
    /// Microsecond timestamp.
    pub microtimestamp: u64,
}

/// Zero-allocation deserializer seed for Bitstamp `diff_order_book_*` WebSocket frames targeting scratch buffers.
pub struct BitstampDiffFrameSeed<'buf> {
    scratch: &'buf mut BitstampConnectionScratch,
}

impl<'buf> BitstampDiffFrameSeed<'buf> {
    /// Creates a new seed targeting pre-allocated scratch buffers.
    pub fn new(scratch: &'buf mut BitstampConnectionScratch) -> Self {
        scratch.clear();
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for BitstampDiffFrameSeed<'buf> {
    type Value = BitstampDiffFrameHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(BitstampDiffFrameVisitor {
            scratch: self.scratch,
        })
    }
}

struct BitstampDiffFrameVisitor<'buf> {
    scratch: &'buf mut BitstampConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for BitstampDiffFrameVisitor<'buf> {
    type Value = BitstampDiffFrameHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Bitstamp diff_order_book WebSocket frame map")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut event = None;
        let mut channel = None;
        let mut microtimestamp = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "event" => event = Some(map.next_value::<&'de str>()?),
                "channel" => channel = Some(map.next_value::<&'de str>()?),
                "data" => {
                    let micros = map.next_value_seed(DiffDataSeed {
                        scratch: self.scratch,
                    })?;
                    microtimestamp = Some(micros);
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let event = event.ok_or_else(|| de::Error::missing_field("event"))?;
        let channel = channel.ok_or_else(|| de::Error::missing_field("channel"))?;
        let microtimestamp =
            microtimestamp.ok_or_else(|| de::Error::missing_field("data.microtimestamp"))?;

        Ok(BitstampDiffFrameHeader {
            event,
            channel,
            microtimestamp,
        })
    }
}

struct DiffDataSeed<'buf> {
    scratch: &'buf mut BitstampConnectionScratch,
}

impl<'de, 'buf> DeserializeSeed<'de> for DiffDataSeed<'buf> {
    type Value = u64;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(DiffDataVisitor {
            scratch: self.scratch,
        })
    }
}

struct DiffDataVisitor<'buf> {
    scratch: &'buf mut BitstampConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for DiffDataVisitor<'buf> {
    type Value = u64;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Bitstamp diff order book data map")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut microtimestamp = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "microtimestamp" => {
                    let micros = map.next_value_seed(MicrotimestampSeed)?;
                    microtimestamp = Some(micros);
                }
                "bids" => {
                    map.next_value_seed(LevelsSeed::new(&mut self.scratch.bids))?;
                }
                "asks" => {
                    map.next_value_seed(LevelsSeed::new(&mut self.scratch.asks))?;
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        microtimestamp.ok_or_else(|| de::Error::missing_field("microtimestamp"))
    }
}

struct MicrotimestampSeed;

impl<'de> DeserializeSeed<'de> for MicrotimestampSeed {
    type Value = u64;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        u64_from_str(deserializer)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const SNAPSHOT_FIXTURE: &str =
        include_str!("../../tests/fixtures/bitstamp/order_book_snapshot.json");
    const DEPTH_FIXTURE: &str = include_str!("../../tests/fixtures/bitstamp/depth_update.json");
    const REMOVE_FIXTURE: &str =
        include_str!("../../tests/fixtures/bitstamp/depth_update_remove_level.json");
    const TRADE_FIXTURE: &str = include_str!("../../tests/fixtures/bitstamp/trade.json");
    const SUB_ACK_FIXTURE: &str =
        include_str!("../../tests/fixtures/bitstamp/subscription_succeeded.json");
    const RECONNECT_FIXTURE: &str =
        include_str!("../../tests/fixtures/bitstamp/request_reconnect.json");

    #[test]
    fn test_extract_symbol() {
        assert_eq!(extract_symbol("diff_order_book_btcusd"), "btcusd");
        assert_eq!(extract_symbol("live_trades_ethusd"), "ethusd");
        assert_eq!(extract_symbol("btcusd"), "btcusd");
    }

    #[test]
    fn test_parse_microtimestamp() {
        let ts = parse_microtimestamp("1700000000123456").unwrap();
        assert_eq!(ts.as_nanos(), 1_700_000_000_123_456_000);

        let err = parse_microtimestamp("invalid");
        assert_eq!(err, Err(ParseTimestampError::InvalidFormat));
    }

    #[test]
    fn test_parse_order_book_snapshot_fixture() {
        let snap: RawOrderBookSnapshot = serde_json::from_str(SNAPSHOT_FIXTURE).unwrap();
        assert_eq!(snap.microtimestamp, 1700000000100000);
        assert_eq!(snap.bids.len(), 2);
        assert_eq!(snap.asks.len(), 2);
        assert_eq!(snap.bids[0][0], Decimal::from(50000));
        assert_eq!(snap.asks[0][0], Decimal::from(50001));
    }

    #[test]
    fn test_parse_depth_update_fixture() {
        let env: RawEnvelope<'_> = serde_json::from_str(DEPTH_FIXTURE).unwrap();
        assert_eq!(env.event, "data");
        assert_eq!(env.channel, "diff_order_book_btcusd");

        let diff: RawDiffData = serde_json::from_str(env.data.unwrap().get()).unwrap();
        assert_eq!(diff.microtimestamp, 1700000000123456);
        assert_eq!(diff.bids.len(), 1);
        assert_eq!(diff.asks.len(), 1);
        assert_eq!(diff.bids[0][0], Decimal::from(50000));
        assert_eq!(
            diff.asks[0][0],
            Decimal::from_str_exact("50001.50").unwrap()
        );
    }

    #[test]
    fn test_parse_depth_update_remove_level_fixture() {
        let env: RawEnvelope<'_> = serde_json::from_str(REMOVE_FIXTURE).unwrap();
        let diff: RawDiffData = serde_json::from_str(env.data.unwrap().get()).unwrap();
        assert_eq!(diff.microtimestamp, 1700000000200000);
        assert_eq!(diff.bids.len(), 1);
        assert_eq!(diff.bids[0][1], Decimal::ZERO);
        assert!(diff.asks.is_empty());
    }

    #[test]
    fn test_parse_trade_fixture() {
        let env: RawEnvelope<'_> = serde_json::from_str(TRADE_FIXTURE).unwrap();
        assert_eq!(env.event, "trade");
        assert_eq!(env.channel, "live_trades_btcusd");

        let trade: RawTrade = serde_json::from_str(env.data.unwrap().get()).unwrap();
        assert_eq!(trade.id, 12345678);
        assert_eq!(
            trade.amount_str,
            Decimal::from_str_exact("0.01000000").unwrap()
        );
        assert_eq!(trade.price_str, Decimal::from(50000));
        assert_eq!(trade.side, 0); // Buy
        assert_eq!(trade.microtimestamp, 1700000000123456);
        assert_eq!(trade.buy_order_id, Some(1122334455));
        assert_eq!(trade.sell_order_id, Some(9988776655));
    }

    #[test]
    fn test_parse_subscription_succeeded_fixture() {
        let env: RawEnvelope<'_> = serde_json::from_str(SUB_ACK_FIXTURE).unwrap();
        assert_eq!(env.event, "bts:subscription_succeeded");
        assert_eq!(env.channel, "diff_order_book_btcusd");
    }

    #[test]
    fn test_parse_request_reconnect_fixture() {
        let env: RawEnvelope<'_> = serde_json::from_str(RECONNECT_FIXTURE).unwrap();
        assert_eq!(env.event, "bts:request_reconnect");
        assert_eq!(env.channel, "");
    }

    #[test]
    fn test_diff_frame_seed_zero_alloc() {
        let mut scratch = BitstampConnectionScratch::with_capacity(16);
        // warm up
        let mut de = serde_json::Deserializer::from_str(DEPTH_FIXTURE);
        let _ = BitstampDiffFrameSeed::new(&mut scratch)
            .deserialize(&mut de)
            .unwrap();

        let allocs = allocation_counter::measure(|| {
            let mut de = serde_json::Deserializer::from_str(DEPTH_FIXTURE);
            let header = BitstampDiffFrameSeed::new(&mut scratch)
                .deserialize(&mut de)
                .unwrap();
            assert_eq!(header.microtimestamp, 1700000000123456);
            assert_eq!(scratch.bids.len(), 1);
            assert_eq!(scratch.asks.len(), 1);
        });
        assert_eq!(
            allocs.count_total, 0,
            "streaming diff parsing must not allocate"
        );
    }
}
