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
#[derive(Debug)]
pub struct RawEnvelope<'a> {
    /// Channel name (e.g. `"spot.obu"`, `"spot.trades"`, `"spot.ping"`).
    pub channel: &'a str,
    /// Event name if present (e.g. `"subscribe"`, `"unsubscribe"`, `"update"`).
    pub event: Option<&'a str>,
    /// Server timestamp in seconds, derived from `time_ms` when `time` is absent.
    pub time: u64,
    /// Server timestamp in milliseconds if present.
    pub time_ms: Option<u64>,
    /// Raw unparsed result payload.
    pub result: Option<&'a RawValue>,
    /// Raw unparsed error payload if present.
    pub error: Option<&'a RawValue>,
}

// Keep the public normalized envelope unchanged while accepting either wire timestamp.
#[derive(Deserialize)]
struct RawEnvelopeFields<'a> {
    channel: &'a str,
    #[serde(default)]
    event: Option<&'a str>,
    #[serde(default, deserialize_with = "optional_seconds")]
    time: Option<u64>,
    #[serde(default)]
    time_ms: Option<u64>,
    #[serde(borrow, default)]
    result: Option<&'a RawValue>,
    #[serde(borrow, default)]
    error: Option<&'a RawValue>,
}

impl<'de: 'a, 'a> Deserialize<'de> for RawEnvelope<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let fields = RawEnvelopeFields::<'a>::deserialize(deserializer)?;
        let time = server_seconds(fields.time, fields.time_ms)
            .ok_or_else(|| serde::de::Error::missing_field("time or time_ms"))?;
        Ok(Self {
            channel: fields.channel,
            event: fields.event,
            time,
            time_ms: fields.time_ms,
            result: fields.result,
            error: fields.error,
        })
    }
}

fn optional_seconds<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    u64::deserialize(deserializer).map(Some)
}

fn server_seconds(time: Option<u64>, time_ms: Option<u64>) -> Option<u64> {
    time.or_else(|| time_ms.map(|millis| millis / 1000))
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
    /// Server timestamp in seconds; zero if neither envelope timestamp is present.
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
        let mut time = None;
        let mut time_ms = None;
        let mut obu = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "channel" => channel = Some(map.next_value::<&'de str>()?),
                "event" => event = Some(map.next_value::<&'de str>()?),
                "time" => time = Some(map.next_value::<u64>()?),
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
            time: server_seconds(time, time_ms).unwrap_or_default(),
            time_ms,
            obu,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
