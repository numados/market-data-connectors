//! Raw exchange message schemas and zero-allocation deserialization for Gate.io WebSocket v4.

use std::fmt;

use mdc_core::PriceLevel;
use mdc_core::model::UnixNanos;
use rust_decimal::Decimal;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use smallstr::SmallString;

/// Parses `create_time_ms` string (e.g. `"1700000000123.456"`) into exact [`UnixNanos`].
///
/// Handles millisecond timestamps with fractional decimal components without floating-point errors.
pub fn parse_create_time_ms(s: &str) -> Result<UnixNanos, ParseTimestampError> {
    let (ms_str, frac_str) = match s.split_once('.') {
        Some((ms, frac)) => (ms, Some(frac)),
        None => (s, None),
    };
    let ms: u64 = ms_str
        .parse()
        .map_err(|_| ParseTimestampError::InvalidFormat)?;
    let mut nanos_offset = 0u64;
    if let Some(frac) = frac_str {
        if frac.is_empty() {
            return Err(ParseTimestampError::InvalidFormat);
        }
        let trimmed = if frac.len() > 6 { &frac[..6] } else { frac };
        let val: u64 = trimmed
            .parse()
            .map_err(|_| ParseTimestampError::InvalidFormat)?;
        let factor = 10u64.pow(6 - trimmed.len() as u32);
        nanos_offset = val.saturating_mul(factor);
    }
    let total_nanos = ms.saturating_mul(1_000_000).saturating_add(nanos_offset);
    Ok(UnixNanos(total_nanos))
}

/// Errors during timestamp parsing.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParseTimestampError {
    /// Timestamp string format was invalid.
    #[error("invalid timestamp format")]
    InvalidFormat,
}

/// Extracts currency pair symbol from payload identifier (e.g. `"ob.BTC_USDT.400"` -> `"BTC_USDT"`).
#[inline]
pub fn extract_symbol(s: &str) -> &str {
    if let Some(rest) = s.strip_prefix("ob.") {
        if let Some((sym, _levels)) = rest.split_once('.') {
            return sym;
        }
    }
    s
}

/// Top-level raw envelope for all messages on the Gate.io WebSocket feed.
#[derive(Debug, Deserialize)]
pub struct RawEnvelope<'a> {
    /// Channel name (e.g. `"spot.obu"`, `"spot.trades"`, `"spot.ping"`).
    pub channel: &'a str,
    /// Event name if present (e.g. `"subscribe"`, `"unsubscribe"`, `"update"`).
    #[serde(default)]
    pub event: Option<&'a str>,
    /// Server timestamp in seconds.
    pub time: u64,
    /// Server timestamp in milliseconds if present.
    #[serde(default)]
    pub time_ms: Option<u64>,
    /// Raw unparsed result payload.
    #[serde(borrow, default)]
    pub result: Option<&'a RawValue>,
    /// Raw unparsed error payload if present.
    #[serde(borrow, default)]
    pub error: Option<&'a RawValue>,
}

/// Raw order book update (`spot.obu`) result object with owned level vectors.
#[derive(Debug, Deserialize, Clone)]
pub struct RawObu<'a> {
    /// Event timestamp in milliseconds.
    pub t: u64,
    /// True if this is an authoritative snapshot.
    #[serde(default)]
    pub full: bool,
    /// Payload name (e.g. `"ob.BTC_USDT.400"` or symbol name).
    pub s: &'a str,
    /// First sequence ID of the update range (absent or 0 in snapshots).
    #[serde(rename = "U", default)]
    pub first_id: u64,
    /// Last sequence ID of the update range or snapshot sequence ID.
    pub u: u64,
    /// Bid levels `[price, size]`.
    #[serde(default)]
    pub b: Vec<[Decimal; 2]>,
    /// Ask levels `[price, size]`.
    #[serde(default)]
    pub a: Vec<[Decimal; 2]>,
}

/// Raw public trade (`spot.trades`) result item.
#[derive(Debug, Deserialize, Clone)]
pub struct RawTrade<'a> {
    /// Trade ID reported as integer or string.
    #[serde(borrow)]
    pub id: RawTradeId<'a>,
    /// Creation timestamp string with fractional milliseconds (e.g. `"1700000000123.456"`).
    pub create_time_ms: &'a str,
    /// Trade side: `"buy"` or `"sell"`.
    pub side: &'a str,
    /// Currency pair (e.g. `"BTC_USDT"`).
    pub currency_pair: &'a str,
    /// Trade volume/amount.
    pub amount: Decimal,
    /// Execution price.
    pub price: Decimal,
}

/// Raw trade identifier, accommodating either numeric or string representation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum RawTradeId<'a> {
    /// Numeric ID.
    Num(u64),
    /// String ID.
    Str(&'a str),
}

impl<'a> RawTradeId<'a> {
    /// Formats the ID into an inline string buffer without heap allocation.
    pub fn to_small_str(&self) -> SmallString<[u8; 32]> {
        match self {
            Self::Num(n) => {
                let mut buf = SmallString::new();
                use std::fmt::Write;
                let _ = write!(buf, "{n}");
                buf
            }
            Self::Str(s) => SmallString::from(*s),
        }
    }
}

/// Subscription acknowledgment result object.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct RawSubAckResult<'a> {
    /// Status: `"success"` or `"fail"`.
    pub status: &'a str,
}

/// Subscription error payload.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct RawSubAckError<'a> {
    /// Error code.
    pub code: i64,
    /// Error message.
    pub message: &'a str,
}

/// Client heartbeat response (`spot.pong`).
#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct SpotPong {
    /// Ping timestamp echoed back to Gate.io.
    pub time: u64,
    /// Channel name `"spot.pong"`.
    pub channel: &'static str,
}

impl SpotPong {
    /// Creates a pong reply with the specified timestamp.
    pub fn new(time: u64) -> Self {
        Self {
            time,
            channel: "spot.pong",
        }
    }
}

/// Client subscription request frame.
#[derive(Debug, Serialize, Clone)]
pub struct SubscribeRequest<'a> {
    /// Current Unix timestamp in seconds.
    pub time: u64,
    /// Channel name (e.g. `"spot.obu"` or `"spot.trades"`).
    pub channel: &'a str,
    /// Event name (`"subscribe"` or `"unsubscribe"`).
    pub event: &'a str,
    /// Payload list, e.g. `["ob.BTC_USDT.400"]`.
    pub payload: Vec<&'a str>,
}

/// Reused scratch buffers for zero-allocation parsing of Gate.io order book frames.
#[derive(Debug, Default)]
pub struct GateioConnectionScratch {
    /// Bids scratch buffer.
    pub bids: Vec<PriceLevel>,
    /// Asks scratch buffer.
    pub asks: Vec<PriceLevel>,
}

impl GateioConnectionScratch {
    /// Creates a scratch buffer pre-allocated to hold `capacity` levels per side.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
        }
    }

    /// Clears both scratch buffers.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

/// Metadata header for an order book update or snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObuHeader<'a> {
    /// Event timestamp in milliseconds.
    pub t: u64,
    /// True if full snapshot.
    pub full: bool,
    /// Payload name or symbol.
    pub s: &'a str,
    /// First sequence ID ($U$).
    pub first_id: u64,
    /// Last sequence ID ($u$).
    pub last_id: u64,
}

/// Deserializer seed for level arrays: directly pushes `PriceLevel` entries into a scratch buffer.
pub struct LevelsSeed<'buf> {
    buf: &'buf mut Vec<PriceLevel>,
}

impl<'buf> LevelsSeed<'buf> {
    /// Creates a new `LevelsSeed` targeting `buf`.
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
        formatter.write_str("a sequence of [price, size] string pairs")
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

/// Zero-allocation deserializer seed for Gate.io `spot.obu` update result payloads.
pub struct ObuSeed<'buf> {
    scratch: &'buf mut GateioConnectionScratch,
}

impl<'buf> ObuSeed<'buf> {
    /// Creates a new `ObuSeed` using the provided connection scratch buffers.
    pub fn new(scratch: &'buf mut GateioConnectionScratch) -> Self {
        scratch.clear();
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for ObuSeed<'buf> {
    type Value = ObuHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ObuVisitor {
            scratch: self.scratch,
        })
    }
}

struct ObuVisitor<'buf> {
    scratch: &'buf mut GateioConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for ObuVisitor<'buf> {
    type Value = ObuHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a spot.obu result map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut t: Option<u64> = None;
        let mut full: bool = false;
        let mut s: Option<&'de str> = None;
        let mut first_id: u64 = 0;
        let mut last_id: Option<u64> = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "t" => t = Some(map.next_value()?),
                "full" => full = map.next_value()?,
                "s" => s = Some(map.next_value()?),
                "U" => first_id = map.next_value()?,
                "u" => last_id = Some(map.next_value()?),
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

        let t = t.ok_or_else(|| serde::de::Error::missing_field("t"))?;
        let s = s.ok_or_else(|| serde::de::Error::missing_field("s"))?;
        let last_id = last_id.ok_or_else(|| serde::de::Error::missing_field("u"))?;

        Ok(ObuHeader {
            t,
            full,
            s,
            first_id,
            last_id,
        })
    }
}

/// Header metadata for zero-allocation parsed Gate.io `spot.obu` depth frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateioDepthFrameHeader<'de> {
    /// Channel name (`"spot.obu"`).
    pub channel: &'de str,
    /// Event name (`"update"` or `"subscribe"`).
    pub event: Option<&'de str>,
    /// Server timestamp in seconds.
    pub time: u64,
    /// Server timestamp in milliseconds if present.
    pub time_ms: Option<u64>,
    /// Obu order book header containing metadata (t, s, full, first_id, last_id).
    pub obu: ObuHeader<'de>,
}

/// Zero-allocation deserializer seed for Gate.io `spot.obu` WebSocket frames targeting scratch buffers.
pub struct GateioDepthFrameSeed<'buf> {
    scratch: &'buf mut GateioConnectionScratch,
}

impl<'buf> GateioDepthFrameSeed<'buf> {
    /// Creates a new seed targeting pre-allocated scratch buffers.
    pub fn new(scratch: &'buf mut GateioConnectionScratch) -> Self {
        scratch.clear();
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for GateioDepthFrameSeed<'buf> {
    type Value = GateioDepthFrameHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(GateioDepthFrameVisitor {
            scratch: self.scratch,
        })
    }
}

struct GateioDepthFrameVisitor<'buf> {
    scratch: &'buf mut GateioConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for GateioDepthFrameVisitor<'buf> {
    type Value = GateioDepthFrameHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Gate.io spot.obu WebSocket frame map")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut channel = None;
        let mut event = None;
        let mut time = 0;
        let mut time_ms = None;
        let mut obu = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "channel" => channel = Some(map.next_value::<&'de str>()?),
                "event" => event = Some(map.next_value::<&'de str>()?),
                "time" => time = map.next_value::<u64>()?,
                "time_ms" => time_ms = Some(map.next_value::<u64>()?),
                "result" => {
                    let h = map.next_value_seed(ObuSeed::new(self.scratch))?;
                    obu = Some(h);
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let channel = channel.unwrap_or("spot.obu");
        let obu = obu.ok_or_else(|| serde::de::Error::missing_field("result"))?;

        Ok(GateioDepthFrameHeader {
            channel,
            event,
            time,
            time_ms,
            obu,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_symbol() {
        assert_eq!(extract_symbol("ob.BTC_USDT.400"), "BTC_USDT");
        assert_eq!(extract_symbol("ob.ETH_USDT.50"), "ETH_USDT");
        assert_eq!(extract_symbol("BTC_USDT"), "BTC_USDT");
    }

    #[test]
    fn test_parse_create_time_ms() {
        let ts = parse_create_time_ms("1700000000123.456").unwrap();
        assert_eq!(ts.as_nanos(), 1_700_000_000_123_456_000);

        let ts2 = parse_create_time_ms("1700000000123").unwrap();
        assert_eq!(ts2.as_nanos(), 1_700_000_000_123_000_000);

        let ts3 = parse_create_time_ms("1700000000123.5").unwrap();
        assert_eq!(ts3.as_nanos(), 1_700_000_000_123_500_000);

        assert!(parse_create_time_ms("invalid").is_err());
        assert!(parse_create_time_ms("123.").is_err());
    }

    #[test]
    fn test_parse_snapshot_fixture() {
        let data = include_str!("../../tests/fixtures/gateio/order_book_snapshot.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        assert_eq!(env.channel, "spot.obu");
        assert_eq!(env.event, Some("update"));

        let obu: RawObu<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
        assert!(obu.full);
        assert_eq!(obu.s, "ob.BTC_USDT.400");
        assert_eq!(obu.u, 123456);
        assert_eq!(obu.b.len(), 2);
        assert_eq!(obu.a.len(), 2);
    }

    #[test]
    fn test_parse_snapshot_seed_zero_alloc() {
        let data = include_str!("../../tests/fixtures/gateio/order_book_snapshot.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        let mut scratch = GateioConnectionScratch::with_capacity(16);

        let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
        let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        assert!(header.full);
        assert_eq!(header.s, "ob.BTC_USDT.400");
        assert_eq!(header.last_id, 123456);
        assert_eq!(scratch.bids.len(), 2);
        assert_eq!(scratch.asks.len(), 2);
        assert_eq!(scratch.bids[0].price, Decimal::from(50000));
        assert_eq!(scratch.asks[0].price, Decimal::from(50001));
    }

    #[test]
    fn test_parse_depth_update_fixture() {
        let data = include_str!("../../tests/fixtures/gateio/depth_update.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        assert_eq!(env.channel, "spot.obu");
        assert_eq!(env.event, Some("update"));

        let obu: RawObu<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
        assert!(!obu.full);
        assert_eq!(obu.first_id, 123457);
        assert_eq!(obu.u, 123460);
        assert_eq!(obu.b.len(), 2);
        assert_eq!(obu.a.len(), 1);
    }

    #[test]
    fn test_parse_depth_update_seed_zero_alloc() {
        let data = include_str!("../../tests/fixtures/gateio/depth_update.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        let mut scratch = GateioConnectionScratch::with_capacity(16);

        let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
        let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        assert!(!header.full);
        assert_eq!(header.first_id, 123457);
        assert_eq!(header.last_id, 123460);
        assert_eq!(scratch.bids.len(), 2);
        assert_eq!(scratch.asks.len(), 1);
    }

    #[test]
    fn test_parse_depth_update_bid_only() {
        let data = include_str!("../../tests/fixtures/gateio/depth_update_bid_only.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        let mut scratch = GateioConnectionScratch::with_capacity(16);

        let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
        let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        assert_eq!(scratch.bids.len(), 1);
        assert!(scratch.asks.is_empty());
        assert_eq!(header.first_id, 123461);
        assert_eq!(header.last_id, 123462);
    }

    #[test]
    fn test_parse_depth_update_ask_only() {
        let data = include_str!("../../tests/fixtures/gateio/depth_update_ask_only.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        let mut scratch = GateioConnectionScratch::with_capacity(16);

        let mut de = serde_json::Deserializer::from_str(env.result.unwrap().get());
        let header = ObuSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        assert!(scratch.bids.is_empty());
        assert_eq!(scratch.asks.len(), 1);
        assert_eq!(header.first_id, 123463);
        assert_eq!(header.last_id, 123464);
    }

    #[test]
    fn test_parse_heartbeat_or_ping_fixture() {
        let data = include_str!("../../tests/fixtures/gateio/heartbeat_or_ping.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        assert_eq!(env.channel, "spot.ping");
        assert_eq!(env.time, 1700000030);
        assert!(env.event.is_none());
        assert!(env.result.is_none());

        let pong = SpotPong::new(env.time);
        let serialized = serde_json::to_string(&pong).unwrap();
        assert_eq!(serialized, r#"{"time":1700000030,"channel":"spot.pong"}"#);
    }

    #[test]
    fn test_parse_trade_fixture() {
        let data = include_str!("../../tests/fixtures/gateio/trade.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        assert_eq!(env.channel, "spot.trades");
        assert_eq!(env.event, Some("update"));

        let trade: RawTrade<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
        assert_eq!(trade.id.to_small_str().as_str(), "987654321");
        assert_eq!(trade.currency_pair, "BTC_USDT");
        assert_eq!(trade.side, "sell");
        assert_eq!(trade.price, Decimal::from(50000));
        assert_eq!(trade.amount, Decimal::from_str_exact("0.01").unwrap());
    }

    #[test]
    fn test_parse_subscribe_ack_fixture() {
        let data = include_str!("../../tests/fixtures/gateio/subscribe_ack.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        assert_eq!(env.channel, "spot.obu");
        assert_eq!(env.event, Some("subscribe"));

        let ack: RawSubAckResult<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
        assert_eq!(ack.status, "success");
    }

    #[test]
    fn test_parse_subscribe_ack_error_fixture() {
        let data = include_str!("../../tests/fixtures/gateio/subscribe_ack_error.json");
        let env: RawEnvelope<'_> = serde_json::from_str(data).unwrap();
        assert_eq!(env.channel, "spot.obu");
        assert_eq!(env.event, Some("subscribe"));

        let ack: RawSubAckResult<'_> = serde_json::from_str(env.result.unwrap().get()).unwrap();
        assert_eq!(ack.status, "fail");

        let err: RawSubAckError<'_> = serde_json::from_str(env.error.unwrap().get()).unwrap();
        assert_eq!(err.code, 2);
        assert_eq!(err.message, "invalid symbol");
    }
}
