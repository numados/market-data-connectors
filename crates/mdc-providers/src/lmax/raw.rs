//! Raw wire format schemas, zero-allocation deserializers, and fast key-scan dispatcher for LMAX Digital.

use std::fmt;

use mdc_core::PriceLevel;
use rust_decimal::Decimal;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

/// High-level frame category detected by [`peek_frame_kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LmaxFrameKind {
    /// Subscription or error control message (`SUBSCRIBE`, `SUBSCRIPTIONS`, `ERROR`).
    Control,
    /// Authoritative `ORDER_BOOK` frame, or a legacy untyped frame with `"bids"`.
    OrderBook,
    /// Public trade execution containing a top-level `"last_price"` key.
    Trade,
    /// Unrecognized message payload.
    Unknown,
}

/// Peeks top-level JSON keys to classify an incoming LMAX payload without full deserialization.
pub fn peek_frame_kind(bytes: &[u8]) -> LmaxFrameKind {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escape = false;
    let mut i = 0;
    let len = bytes.len();

    while i < len {
        let b = bytes[i];
        if escape {
            escape = false;
            i += 1;
            continue;
        }
        if b == b'\\' && in_string {
            escape = true;
            i += 1;
            continue;
        }
        if b == b'"' {
            in_string = !in_string;
            if in_string && depth == 1 {
                let key_start = i + 1;
                let mut j = key_start;
                while j < len && bytes[j] != b'"' {
                    if bytes[j] == b'\\' {
                        j += 2;
                    } else {
                        j += 1;
                    }
                }
                if j < len {
                    let key = &bytes[key_start..j];
                    let mut k = j + 1;
                    while k < len
                        && (bytes[k] == b' '
                            || bytes[k] == b'\t'
                            || bytes[k] == b'\r'
                            || bytes[k] == b'\n')
                    {
                        k += 1;
                    }
                    if k < len && bytes[k] == b':' {
                        if key == b"type" {
                            let Some(value) = bytes.get(k.saturating_add(1)..) else {
                                return LmaxFrameKind::Unknown;
                            };
                            let mut de = serde_json::Deserializer::from_slice(value);
                            return match <&str>::deserialize(&mut de) {
                                Ok("ORDER_BOOK") => LmaxFrameKind::OrderBook,
                                Ok(_) => LmaxFrameKind::Control,
                                Err(_) => LmaxFrameKind::Unknown,
                            };
                        }
                        if key == b"bids" {
                            return LmaxFrameKind::OrderBook;
                        }
                        if key == b"last_price" {
                            return LmaxFrameKind::Trade;
                        }
                    }
                }
                i = j;
                in_string = false;
            }
            i += 1;
            continue;
        }
        if !in_string {
            match b {
                b'{' => depth += 1,
                b'}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }

    LmaxFrameKind::Unknown
}

/// Raw control payload from LMAX Digital WebSocket.
#[derive(Debug, Deserialize)]
pub struct RawControl<'a> {
    /// Control message type (e.g. `"SUBSCRIPTIONS"`, `"ERROR"`).
    #[serde(rename = "type")]
    pub kind: &'a str,
    /// Error code if present (e.g. `"TOO_MANY_REQUESTS"`).
    #[serde(default)]
    pub error_code: Option<&'a str>,
    /// Human-readable error message.
    #[serde(default)]
    pub error_message: Option<&'a str>,
    /// Channels subscribed if this is a `SUBSCRIPTIONS` ack.
    #[serde(default)]
    pub channels: Option<Vec<RawSubscribedChannel<'a>>>,
}

/// Channel subscription list inside `SUBSCRIPTIONS` acknowledgment.
#[derive(Debug, Deserialize)]
pub struct RawSubscribedChannel<'a> {
    /// Name of channel (e.g. `"ORDER_BOOK"`).
    pub name: &'a str,
    /// Instruments confirmed subscribed.
    #[serde(default)]
    pub instruments: Vec<&'a str>,
}

/// Channel request entry inside a [`SubscribeMessage`].
#[derive(Debug, Serialize)]
pub struct SubscribeChannel<'a> {
    /// Target channel name (e.g. `"ORDER_BOOK"`).
    pub name: &'a str,
    /// Target instrument identifiers.
    pub instruments: &'a [String],
}

/// Client control request frame (`SUBSCRIBE` or `UNSUBSCRIBE`).
#[derive(Debug, Serialize)]
pub struct SubscribeMessage<'a> {
    /// Control action (`"SUBSCRIBE"` or `"UNSUBSCRIBE"`).
    #[serde(rename = "type")]
    pub kind: &'a str,
    /// Channel specifications.
    pub channels: [SubscribeChannel<'a>; 1],
}

impl<'a> SubscribeMessage<'a> {
    /// Constructs a `SUBSCRIBE` frame for the given channel and instruments.
    pub fn subscribe(channel: &'a str, instruments: &'a [String]) -> Self {
        Self {
            kind: "SUBSCRIBE",
            channels: [SubscribeChannel {
                name: channel,
                instruments,
            }],
        }
    }

    /// Constructs an `UNSUBSCRIBE` frame for the given channel and instruments.
    pub fn unsubscribe(channel: &'a str, instruments: &'a [String]) -> Self {
        Self {
            kind: "UNSUBSCRIBE",
            channels: [SubscribeChannel {
                name: channel,
                instruments,
            }],
        }
    }
}

/// Reusable connection-level scratch buffer for parsing order book levels without allocation.
#[derive(Debug, Default)]
pub struct LmaxConnectionScratch {
    /// Pre-allocated bids buffer.
    pub bids: Vec<PriceLevel>,
    /// Pre-allocated asks buffer.
    pub asks: Vec<PriceLevel>,
}

impl LmaxConnectionScratch {
    /// Creates a new scratch buffer with pre-allocated capacity for both sides.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
        }
    }

    /// Clears both bid and ask level buffers while retaining capacity.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

/// Deserializer seed for reading a list of `{"price":"...", "quantity":"..."}` objects into `Vec<PriceLevel>`.
pub struct LmaxLevelsSeed<'a>(pub &'a mut Vec<PriceLevel>);

impl<'de> DeserializeSeed<'de> for LmaxLevelsSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for LmaxLevelsSeed<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of price-quantity level objects")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(level) = seq.next_element::<RawLevel>()? {
            self.0.push(PriceLevel::new(level.price, level.quantity));
        }
        Ok(())
    }
}

/// Internal helper for deserializing a single level object.
#[derive(Debug, Deserialize)]
struct RawLevel {
    price: Decimal,
    quantity: Decimal,
}

/// Header extracted from an authoritative `ORDER_BOOK` frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LmaxBookHeader<'a> {
    /// Instrument identifier (e.g. `"btc-usd"`).
    pub instrument_id: &'a str,
    /// RFC 3339 timestamp string.
    pub timestamp: &'a str,
}

/// Deserializer seed for single-pass decoding of an LMAX `ORDER_BOOK` frame into pre-allocated scratch.
pub struct LmaxBookFrameSeed<'a> {
    scratch: &'a mut LmaxConnectionScratch,
}

impl<'a> LmaxBookFrameSeed<'a> {
    /// Creates a new seed wrapping `scratch`.
    pub fn new(scratch: &'a mut LmaxConnectionScratch) -> Self {
        Self { scratch }
    }
}

impl<'de> DeserializeSeed<'de> for LmaxBookFrameSeed<'_> {
    type Value = LmaxBookHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for LmaxBookFrameSeed<'_> {
    type Value = LmaxBookHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an LMAX order book object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut instrument_id: Option<&'de str> = None;
        let mut timestamp: Option<&'de str> = None;
        self.scratch.clear();

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "instrument_id" => {
                    instrument_id = Some(map.next_value::<&'de str>()?);
                }
                "timestamp" => {
                    timestamp = Some(map.next_value::<&'de str>()?);
                }
                "bids" => {
                    map.next_value_seed(LmaxLevelsSeed(&mut self.scratch.bids))?;
                }
                "asks" => {
                    map.next_value_seed(LmaxLevelsSeed(&mut self.scratch.asks))?;
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let instrument_id =
            instrument_id.ok_or_else(|| serde::de::Error::missing_field("instrument_id"))?;
        let timestamp = timestamp.ok_or_else(|| serde::de::Error::missing_field("timestamp"))?;

        Ok(LmaxBookHeader {
            instrument_id,
            timestamp,
        })
    }
}

/// Raw public trade frame from LMAX Digital WebSocket.
#[derive(Debug, Deserialize)]
pub struct RawTrade<'a> {
    /// Instrument identifier (e.g. `"btc-usd"`).
    pub instrument_id: &'a str,
    /// RFC 3339 timestamp string.
    pub timestamp: &'a str,
    /// Executed quantity.
    pub last_quantity: Decimal,
    /// Execution price.
    pub last_price: Decimal,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
