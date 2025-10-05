//! Raw exchange message schemas and zero-allocation deserialization for Coinbase Exchange.

use std::fmt;

use mdc_core::PriceLevel;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

/// Raw channel subscription item in `subscriptions` messages.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RawChannelSubscription<'a> {
    /// Channel name, e.g. `"level2_batch"` or `"heartbeat"`.
    pub name: &'a str,
    /// Product IDs subscribed to this channel.
    #[serde(default)]
    pub product_ids: Vec<&'a str>,
}

/// Raw message envelope representing all public messages from Coinbase Exchange WebSocket feed.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum RawMessage<'a> {
    /// Full order book snapshot emitted immediately upon subscribing to `level2_batch`.
    #[serde(rename = "snapshot")]
    Snapshot {
        /// Product currency pair, e.g. `"BTC-USD"`.
        product_id: &'a str,
        /// Bid levels `[price, size]`.
        bids: Vec<[Decimal; 2]>,
        /// Ask levels `[price, size]`.
        asks: Vec<[Decimal; 2]>,
    },
    /// Incremental level updates batched at ~50ms intervals.
    #[serde(rename = "l2update")]
    L2Update {
        /// Product currency pair, e.g. `"BTC-USD"`.
        product_id: &'a str,
        /// Venue-reported RFC 3339 timestamp.
        time: &'a str,
        /// Array of level changes: `[side ("buy"|"sell"), price, size]`.
        #[serde(borrow)]
        changes: Vec<(&'a str, Decimal, Decimal)>,
    },
    /// Periodic heartbeat emitted approximately once per second per product.
    #[serde(rename = "heartbeat")]
    Heartbeat {
        /// Product currency pair.
        product_id: &'a str,
        /// Product-scoped monotonically increasing sequence counter.
        sequence: u64,
        /// Last executed trade identifier.
        last_trade_id: u64,
        /// Venue-reported RFC 3339 timestamp.
        time: &'a str,
    },
    /// Match (trade execution) payload.
    #[serde(rename = "match")]
    Match {
        /// Product currency pair.
        product_id: &'a str,
        /// Venue unique trade ID.
        trade_id: u64,
        /// Sequence number of the match.
        sequence: u64,
        /// Execution timestamp in RFC 3339.
        time: &'a str,
        /// Trade volume/size.
        size: Decimal,
        /// Trade execution price.
        price: Decimal,
        /// Maker order side: `"buy"` or `"sell"`.
        side: &'a str,
    },
    /// Subscription acknowledgment frame.
    #[serde(rename = "subscriptions")]
    Subscriptions {
        /// Subscribed channels.
        #[serde(default)]
        channels: Vec<RawChannelSubscription<'a>>,
    },
    /// Error response from exchange.
    #[serde(rename = "error")]
    Error {
        /// Error description.
        message: &'a str,
        /// Optional detailed reason.
        #[serde(default)]
        reason: Option<&'a str>,
    },
    /// Unhandled or unknown message types.
    #[serde(other)]
    Other,
}

/// Pre-allocated scratch buffers for zero-allocation parsing of Coinbase book frames.
#[derive(Debug, Default)]
pub struct CoinbaseConnectionScratch {
    /// Bids scratch buffer (populated with `PriceLevel` entries on snapshot, and `"buy"` changes on `l2update`).
    pub bids: Vec<PriceLevel>,
    /// Asks scratch buffer (populated with `PriceLevel` entries on snapshot, and `"sell"` changes on `l2update`).
    pub asks: Vec<PriceLevel>,
}

impl CoinbaseConnectionScratch {
    /// Creates a new `CoinbaseConnectionScratch` with pre-allocated capacity.
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

/// Deserializer seed that fills pre-allocated scratch vectors with `PriceLevel` entries from `[price, size]` pairs.
pub struct CoinbaseLevelsSeed<'buf> {
    buf: &'buf mut Vec<PriceLevel>,
}

impl<'buf> CoinbaseLevelsSeed<'buf> {
    /// Creates a new `CoinbaseLevelsSeed` targeting the given scratch vector.
    pub fn new(buf: &'buf mut Vec<PriceLevel>) -> Self {
        buf.clear();
        Self { buf }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for CoinbaseLevelsSeed<'buf> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CoinbaseLevelsVisitor(self.buf))
    }
}

struct CoinbaseLevelsVisitor<'buf>(&'buf mut Vec<PriceLevel>);

impl<'de, 'buf> Visitor<'de> for CoinbaseLevelsVisitor<'buf> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of [price, size] level pairs")
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

/// Deserializer seed that fills pre-allocated scratch vectors with `[side, price, size]` changes from `l2update`.
pub struct CoinbaseChangesSeed<'buf> {
    bids: &'buf mut Vec<PriceLevel>,
    asks: &'buf mut Vec<PriceLevel>,
}

impl<'buf> CoinbaseChangesSeed<'buf> {
    /// Creates a new `CoinbaseChangesSeed` targeting scratch bids and asks buffers.
    pub fn new(bids: &'buf mut Vec<PriceLevel>, asks: &'buf mut Vec<PriceLevel>) -> Self {
        bids.clear();
        asks.clear();
        Self { bids, asks }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for CoinbaseChangesSeed<'buf> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CoinbaseChangesVisitor {
            bids: self.bids,
            asks: self.asks,
        })
    }
}

struct CoinbaseChangesVisitor<'buf> {
    bids: &'buf mut Vec<PriceLevel>,
    asks: &'buf mut Vec<PriceLevel>,
}

impl<'de, 'buf> Visitor<'de> for CoinbaseChangesVisitor<'buf> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of [side, price, size] level change triples")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some((side, price, size)) = seq.next_element::<(&'de str, Decimal, Decimal)>()? {
            if side == "buy" {
                self.bids.push(PriceLevel::new(price, size));
            } else if side == "sell" {
                self.asks.push(PriceLevel::new(price, size));
            }
        }
        Ok(())
    }
}

/// Header metadata and variant for zero-allocation parsed Coinbase frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoinbaseFrameHeader<'a> {
    /// Snapshot message. Scratch buffers contain bids and asks.
    Snapshot {
        /// Currency pair symbol.
        product_id: &'a str,
    },
    /// Incremental level updates. Scratch buffers contain bids (`"buy"`) and asks (`"sell"`).
    L2Update {
        /// Currency pair symbol.
        product_id: &'a str,
        /// Timestamp string.
        time: &'a str,
    },
    /// Heartbeat signal.
    Heartbeat {
        /// Currency pair symbol.
        product_id: &'a str,
        /// Sequence counter.
        sequence: u64,
        /// Last trade ID.
        last_trade_id: u64,
        /// Timestamp string.
        time: &'a str,
    },
    /// Public trade match.
    Match {
        /// Currency pair symbol.
        product_id: &'a str,
        /// Trade ID.
        trade_id: u64,
        /// Sequence number.
        sequence: u64,
        /// Timestamp string.
        time: &'a str,
        /// Trade size.
        size: Decimal,
        /// Trade price.
        price: Decimal,
        /// Maker order side.
        side: &'a str,
    },
    /// Subscriptions acknowledgment frame.
    Subscriptions,
    /// Error frame.
    Error {
        /// Error message.
        message: &'a str,
        /// Optional error reason.
        reason: Option<&'a str>,
    },
    /// Unhandled frame.
    Other,
}

/// Zero-allocation deserializer seed for Coinbase WebSocket frames targeting scratch buffers.
pub struct CoinbaseFrameSeed<'buf> {
    scratch: &'buf mut CoinbaseConnectionScratch,
}

impl<'buf> CoinbaseFrameSeed<'buf> {
    /// Creates a new `CoinbaseFrameSeed` targeting the provided scratch buffers.
    pub fn new(scratch: &'buf mut CoinbaseConnectionScratch) -> Self {
        scratch.clear();
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for CoinbaseFrameSeed<'buf> {
    type Value = CoinbaseFrameHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(CoinbaseFrameVisitor {
            scratch: self.scratch,
        })
    }
}

struct CoinbaseFrameVisitor<'buf> {
    scratch: &'buf mut CoinbaseConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for CoinbaseFrameVisitor<'buf> {
    type Value = CoinbaseFrameHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Coinbase WebSocket frame object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut msg_type: Option<&'de str> = None;
        let mut product_id: Option<&'de str> = None;
        let mut time: Option<&'de str> = None;
        let mut sequence: Option<u64> = None;
        let mut last_trade_id: Option<u64> = None;
        let mut trade_id: Option<u64> = None;
        let mut size: Option<Decimal> = None;
        let mut price: Option<Decimal> = None;
        let mut side: Option<&'de str> = None;
        let mut message: Option<&'de str> = None;
        let mut reason: Option<&'de str> = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "type" => msg_type = Some(map.next_value::<&'de str>()?),
                "product_id" => product_id = Some(map.next_value::<&'de str>()?),
                "time" => time = Some(map.next_value::<&'de str>()?),
                "sequence" => sequence = Some(map.next_value::<u64>()?),
                "last_trade_id" => last_trade_id = Some(map.next_value::<u64>()?),
                "trade_id" => trade_id = Some(map.next_value::<u64>()?),
                "size" => size = Some(map.next_value::<Decimal>()?),
                "price" => price = Some(map.next_value::<Decimal>()?),
                "side" => side = Some(map.next_value::<&'de str>()?),
                "message" => message = Some(map.next_value::<&'de str>()?),
                "reason" => reason = Some(map.next_value::<&'de str>()?),
                "bids" => {
                    map.next_value_seed(CoinbaseLevelsSeed::new(&mut self.scratch.bids))?;
                }
                "asks" => {
                    map.next_value_seed(CoinbaseLevelsSeed::new(&mut self.scratch.asks))?;
                }
                "changes" => {
                    map.next_value_seed(CoinbaseChangesSeed::new(
                        &mut self.scratch.bids,
                        &mut self.scratch.asks,
                    ))?;
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let msg_type = match msg_type {
            Some(t) => t,
            None => return Ok(CoinbaseFrameHeader::Other),
        };

        match msg_type {
            "snapshot" => {
                let product_id =
                    product_id.ok_or_else(|| serde::de::Error::missing_field("product_id"))?;
                Ok(CoinbaseFrameHeader::Snapshot { product_id })
            }
            "l2update" => {
                let product_id =
                    product_id.ok_or_else(|| serde::de::Error::missing_field("product_id"))?;
                let time = time.ok_or_else(|| serde::de::Error::missing_field("time"))?;
                Ok(CoinbaseFrameHeader::L2Update { product_id, time })
            }
            "heartbeat" => {
                let product_id =
                    product_id.ok_or_else(|| serde::de::Error::missing_field("product_id"))?;
                let sequence =
                    sequence.ok_or_else(|| serde::de::Error::missing_field("sequence"))?;
                let last_trade_id = last_trade_id
                    .ok_or_else(|| serde::de::Error::missing_field("last_trade_id"))?;
                let time = time.ok_or_else(|| serde::de::Error::missing_field("time"))?;
                Ok(CoinbaseFrameHeader::Heartbeat {
                    product_id,
                    sequence,
                    last_trade_id,
                    time,
                })
            }
            "match" => {
                let product_id =
                    product_id.ok_or_else(|| serde::de::Error::missing_field("product_id"))?;
                let trade_id =
                    trade_id.ok_or_else(|| serde::de::Error::missing_field("trade_id"))?;
                let sequence =
                    sequence.ok_or_else(|| serde::de::Error::missing_field("sequence"))?;
                let time = time.ok_or_else(|| serde::de::Error::missing_field("time"))?;
                let size = size.ok_or_else(|| serde::de::Error::missing_field("size"))?;
                let price = price.ok_or_else(|| serde::de::Error::missing_field("price"))?;
                let side = side.ok_or_else(|| serde::de::Error::missing_field("side"))?;
                Ok(CoinbaseFrameHeader::Match {
                    product_id,
                    trade_id,
                    sequence,
                    time,
                    size,
                    price,
                    side,
                })
            }
            "subscriptions" => Ok(CoinbaseFrameHeader::Subscriptions),
            "error" => {
                let message = message.unwrap_or("");
                Ok(CoinbaseFrameHeader::Error { message, reason })
            }
            _ => Ok(CoinbaseFrameHeader::Other),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const SNAPSHOT_JSON: &str =
        include_str!("../../tests/fixtures/coinbase/order_book_snapshot.json");
    const L2UPDATE_JSON: &str = include_str!("../../tests/fixtures/coinbase/depth_update.json");
    const TRADE_JSON: &str = include_str!("../../tests/fixtures/coinbase/trade.json");
    const HEARTBEAT_JSON: &str =
        include_str!("../../tests/fixtures/coinbase/heartbeat_or_ping.json");
    const SUBSCRIPTIONS_JSON: &str =
        include_str!("../../tests/fixtures/coinbase/subscriptions.json");
    const ERROR_JSON: &str = include_str!("../../tests/fixtures/coinbase/error.json");

    #[test]
    fn test_parse_snapshot_fixture_typed() {
        let msg: RawMessage<'_> =
            serde_json::from_str(SNAPSHOT_JSON).expect("deserialize snapshot");
        match msg {
            RawMessage::Snapshot {
                product_id,
                bids,
                asks,
            } => {
                assert_eq!(product_id, "BTC-USD");
                assert_eq!(bids.len(), 2);
                assert_eq!(asks.len(), 2);
                assert_eq!(bids[0][0].to_string(), "10101.10");
                assert_eq!(bids[0][1].to_string(), "0.45054140");
                assert_eq!(asks[0][0].to_string(), "10102.55");
                assert_eq!(asks[0][1].to_string(), "0.57753524");
            }
            other => panic!("expected RawMessage::Snapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_snapshot_seed_zero_alloc() {
        let mut scratch = CoinbaseConnectionScratch::with_capacity(32);
        let mut deserializer = serde_json::Deserializer::from_str(SNAPSHOT_JSON);
        let header = CoinbaseFrameSeed::new(&mut scratch)
            .deserialize(&mut deserializer)
            .expect("deserialize frame");

        assert_eq!(
            header,
            CoinbaseFrameHeader::Snapshot {
                product_id: "BTC-USD"
            }
        );
        assert_eq!(scratch.bids.len(), 2);
        assert_eq!(scratch.asks.len(), 2);
        assert_eq!(scratch.bids[0].price.to_string(), "10101.10");
        assert_eq!(scratch.bids[0].volume.to_string(), "0.45054140");
        assert_eq!(scratch.asks[0].price.to_string(), "10102.55");
        assert_eq!(scratch.asks[0].volume.to_string(), "0.57753524");
    }

    #[test]
    fn test_parse_l2update_fixture_typed() {
        let msg: RawMessage<'_> =
            serde_json::from_str(L2UPDATE_JSON).expect("deserialize l2update");
        match msg {
            RawMessage::L2Update {
                product_id,
                time,
                changes,
            } => {
                assert_eq!(product_id, "BTC-USD");
                assert_eq!(time, "2019-08-14T20:42:27.265Z");
                assert_eq!(changes.len(), 2);
                assert_eq!(changes[0].0, "buy");
                assert_eq!(changes[0].1.to_string(), "10101.80000000");
                assert_eq!(changes[0].2.to_string(), "0.162567");
                assert_eq!(changes[1].0, "sell");
                assert_eq!(changes[1].1.to_string(), "10102.55000000");
                assert_eq!(changes[1].2.to_string(), "0.0");
            }
            other => panic!("expected RawMessage::L2Update, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_l2update_seed_zero_alloc() {
        let mut scratch = CoinbaseConnectionScratch::with_capacity(32);
        let mut deserializer = serde_json::Deserializer::from_str(L2UPDATE_JSON);
        let header = CoinbaseFrameSeed::new(&mut scratch)
            .deserialize(&mut deserializer)
            .expect("deserialize frame");

        assert_eq!(
            header,
            CoinbaseFrameHeader::L2Update {
                product_id: "BTC-USD",
                time: "2019-08-14T20:42:27.265Z",
            }
        );
        assert_eq!(scratch.bids.len(), 1);
        assert_eq!(scratch.asks.len(), 1);
        assert_eq!(scratch.bids[0].price.to_string(), "10101.80000000");
        assert_eq!(scratch.bids[0].volume.to_string(), "0.162567");
        assert_eq!(scratch.asks[0].price.to_string(), "10102.55000000");
        assert_eq!(scratch.asks[0].volume.to_string(), "0.0");
    }

    #[test]
    fn test_parse_trade_fixture() {
        let msg: RawMessage<'_> = serde_json::from_str(TRADE_JSON).expect("deserialize trade");
        match msg {
            RawMessage::Match {
                product_id,
                trade_id,
                sequence,
                time,
                size,
                price,
                side,
            } => {
                assert_eq!(product_id, "BTC-USD");
                assert_eq!(trade_id, 10);
                assert_eq!(sequence, 50);
                assert_eq!(time, "2014-11-07T08:19:27.028459Z");
                assert_eq!(size.to_string(), "5.23512");
                assert_eq!(price.to_string(), "400.23");
                assert_eq!(side, "sell");
            }
            other => panic!("expected RawMessage::Match, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_heartbeat_fixture() {
        let msg: RawMessage<'_> =
            serde_json::from_str(HEARTBEAT_JSON).expect("deserialize heartbeat");
        match msg {
            RawMessage::Heartbeat {
                product_id,
                sequence,
                last_trade_id,
                time,
            } => {
                assert_eq!(product_id, "BTC-USD");
                assert_eq!(sequence, 90);
                assert_eq!(last_trade_id, 20);
                assert_eq!(time, "2014-11-07T08:19:28.464459Z");
            }
            other => panic!("expected RawMessage::Heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_subscriptions_fixture() {
        let msg: RawMessage<'_> =
            serde_json::from_str(SUBSCRIPTIONS_JSON).expect("deserialize subscriptions");
        match msg {
            RawMessage::Subscriptions { channels } => {
                assert_eq!(channels.len(), 2);
                assert_eq!(channels[0].name, "level2_batch");
                assert_eq!(channels[0].product_ids, vec!["BTC-USD"]);
                assert_eq!(channels[1].name, "heartbeat");
                assert_eq!(channels[1].product_ids, vec!["BTC-USD"]);
            }
            other => panic!("expected RawMessage::Subscriptions, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_error_fixture() {
        let msg: RawMessage<'_> = serde_json::from_str(ERROR_JSON).expect("deserialize error");
        match msg {
            RawMessage::Error { message, reason } => {
                assert_eq!(message, "Failed to subscribe");
                assert_eq!(reason, Some("product_ids must be specified"));
            }
            other => panic!("expected RawMessage::Error, got {other:?}"),
        }
    }
}
