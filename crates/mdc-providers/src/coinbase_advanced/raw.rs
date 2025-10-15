//! Raw JSON wire format models and zero-allocation deserialization for Coinbase Advanced Trade.

use std::collections::HashMap;
use std::fmt;

use mdc_core::{PriceLevel, Symbol};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::{
    DeserializeSeed, Deserializer, Error as DeError, IgnoredAny, MapAccess, SeqAccess, Visitor,
};
use serde_json::value::RawValue;

/// Raw message envelope representing all public messages from Coinbase Advanced Trade WebSocket feed.
#[derive(Debug, Deserialize)]
pub struct RawEnvelope<'a> {
    /// Channel name, e.g. `"l2_data"`, `"heartbeats"`, `"market_trades"`, `"subscriptions"`.
    pub channel: &'a str,
    /// Client ID if provided.
    #[serde(default)]
    pub client_id: Option<&'a str>,
    /// Venue envelope timestamp in RFC 3339 format.
    #[serde(default)]
    pub timestamp: Option<&'a str>,
    /// Connection-wide monotonically increasing sequence number.
    #[serde(default)]
    pub sequence_num: Option<u64>,
    /// Raw unparsed event elements.
    #[serde(borrow, default)]
    pub events: Vec<&'a RawValue>,
}

/// Raw Level 2 event payload (`channel == "l2_data"`).
#[derive(Debug, Deserialize)]
pub struct RawL2Event<'a> {
    /// Event type: `"snapshot"` or `"update"`.
    #[serde(rename = "type")]
    pub kind: &'a str,
    /// Product ID, e.g. `"BTC-USD"`.
    pub product_id: &'a str,
    /// Array of price level updates.
    #[serde(borrow, default)]
    pub updates: Vec<RawL2Update<'a>>,
}

/// An individual Level 2 price level update.
#[derive(Debug, Deserialize)]
pub struct RawL2Update<'a> {
    /// Book side: `"bid"` or `"offer"`.
    pub side: &'a str,
    /// Time of the event / level update in RFC 3339.
    pub event_time: &'a str,
    /// Price level.
    pub price_level: Decimal,
    /// New quantity (volume). `0` indicates removal.
    pub new_quantity: Decimal,
}

/// Raw heartbeat event payload (`channel == "heartbeats"`).
#[derive(Debug, Deserialize)]
pub struct RawHeartbeatEvent<'a> {
    /// Current server time in RFC 3339.
    #[serde(default)]
    pub current_time: Option<&'a str>,
    /// Monotonic heartbeat counter.
    #[serde(default)]
    pub heartbeat_counter: Option<u64>,
}

/// Raw message envelope representing market trades from Coinbase Advanced Trade WebSocket feed.
#[derive(Debug, Deserialize)]
pub struct RawTradesEnvelope<'a> {
    /// Channel name.
    pub channel: &'a str,
    /// Connection-wide monotonically increasing sequence number.
    #[serde(default)]
    pub sequence_num: Option<u64>,
    /// Trade events array.
    #[serde(borrow, default)]
    pub events: Vec<RawTradeEvent<'a>>,
}

/// Raw public trade execution event payload (`channel == "market_trades"`).
#[derive(Debug, Deserialize)]
pub struct RawTradeEvent<'a> {
    /// Event type: `"snapshot"` or `"update"`.
    #[serde(rename = "type")]
    pub kind: &'a str,
    /// Array of executed trades.
    #[serde(borrow, default)]
    pub trades: Vec<RawTrade<'a>>,
}

/// An individual executed trade.
#[derive(Debug, Deserialize)]
pub struct RawTrade<'a> {
    /// Venue unique trade ID.
    pub trade_id: &'a str,
    /// Product ID, e.g. `"BTC-USD"`.
    pub product_id: &'a str,
    /// Execution price.
    pub price: Decimal,
    /// Executed volume/size.
    pub size: Decimal,
    /// Maker order side: `"BUY"` or `"SELL"`.
    pub side: &'a str,
    /// Trade execution timestamp in RFC 3339.
    pub time: &'a str,
}

/// Raw subscription acknowledgment payload (`channel == "subscriptions"`).
#[derive(Debug, Deserialize)]
pub struct RawSubscriptionsEvent<'a> {
    /// Map of channel names to lists of product IDs.
    #[serde(borrow, default)]
    pub subscriptions: HashMap<&'a str, Vec<&'a str>>,
}

/// Raw error payload.
#[derive(Debug, Deserialize)]
pub struct RawErrorEnvelope<'a> {
    /// Type string, e.g. `"error"`.
    #[serde(rename = "type", default)]
    pub kind: Option<&'a str>,
    /// Error message.
    #[serde(default)]
    pub message: Option<&'a str>,
    /// Channel string if present.
    #[serde(default)]
    pub channel: Option<&'a str>,
}

/// Single parsed Level 2 event within a multi-event envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedL2Event {
    /// Product ID for this event.
    pub product_id: Symbol,
    /// Kind of L2 event (snapshot or incremental update).
    pub kind: L2EventKind,
    /// Start index into `scratch.bids`.
    pub bids_start: usize,
    /// End index into `scratch.bids`.
    pub bids_end: usize,
    /// Start index into `scratch.asks`.
    pub asks_start: usize,
    /// End index into `scratch.asks`.
    pub asks_end: usize,
}

/// Pre-allocated scratch buffers for zero-allocation parsing of Coinbase Advanced book frames.
#[derive(Debug, Default)]
pub struct CoinbaseAdvancedScratch {
    /// Bids scratch buffer.
    pub bids: Vec<PriceLevel>,
    /// Asks scratch buffer.
    pub asks: Vec<PriceLevel>,
    /// Parsed event slices for multi-event envelopes.
    pub events: Vec<ParsedL2Event>,
}

impl CoinbaseAdvancedScratch {
    /// Creates a new scratch instance with pre-allocated level capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
            events: Vec::with_capacity(8),
        }
    }

    /// Clears bid, ask, and event scratch vectors, retaining underlying memory capacity.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.events.clear();
    }
}

/// Parsed metadata for an L2 frame when deserialized directly into scratch buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2EventKind {
    /// Snapshot frame.
    Snapshot,
    /// Incremental delta update frame.
    Update,
}

/// Header metadata returned by [`CoinbaseAdvancedFrameSeed`].
#[derive(Debug)]
pub struct FrameHeader<'a> {
    /// Channel name.
    pub channel: &'a str,
    /// Sequence number if present.
    pub sequence_num: Option<u64>,
    /// Envelope timestamp string in RFC 3339 if present.
    pub timestamp: Option<&'a str>,
    /// Product ID for L2 events.
    pub product_id: Option<&'a str>,
    /// Kind of L2 event (snapshot or update).
    pub l2_kind: Option<L2EventKind>,
}

/// Deserializer seed that streams L2 updates directly into [`CoinbaseAdvancedScratch`].
pub struct CoinbaseAdvancedFrameSeed<'a> {
    scratch: &'a mut CoinbaseAdvancedScratch,
}

impl<'a> CoinbaseAdvancedFrameSeed<'a> {
    /// Creates a new frame seed with the given scratch buffers.
    pub fn new(scratch: &'a mut CoinbaseAdvancedScratch) -> Self {
        Self { scratch }
    }
}

impl<'de> DeserializeSeed<'de> for CoinbaseAdvancedFrameSeed<'_> {
    type Value = FrameHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(FrameVisitor {
            scratch: self.scratch,
        })
    }
}

struct FrameVisitor<'a> {
    scratch: &'a mut CoinbaseAdvancedScratch,
}

impl<'de> Visitor<'de> for FrameVisitor<'_> {
    type Value = FrameHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Coinbase Advanced Trade JSON envelope")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut channel: Option<&'de str> = None;
        let mut sequence_num: Option<u64> = None;
        let mut timestamp: Option<&'de str> = None;
        let mut product_id: Option<&'de str> = None;
        let mut l2_kind: Option<L2EventKind> = None;

        self.scratch.clear();

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "channel" => {
                    channel = Some(map.next_value()?);
                }
                "sequence_num" => {
                    sequence_num = map.next_value()?;
                }
                "timestamp" => {
                    timestamp = map.next_value()?;
                }
                "events" => {
                    // Stream parse events array
                    let (p_id, k) = map.next_value_seed(EventsArraySeed {
                        scratch: self.scratch,
                    })?;
                    if product_id.is_none() {
                        product_id = p_id;
                    }
                    if l2_kind.is_none() {
                        l2_kind = k;
                    }
                }
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                }
            }
        }

        let ch = channel.ok_or_else(|| DeError::missing_field("channel"))?;
        Ok(FrameHeader {
            channel: ch,
            sequence_num,
            timestamp,
            product_id,
            l2_kind,
        })
    }
}

struct EventsArraySeed<'a> {
    scratch: &'a mut CoinbaseAdvancedScratch,
}

impl<'de> DeserializeSeed<'de> for EventsArraySeed<'_> {
    type Value = (Option<&'de str>, Option<L2EventKind>);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(EventsArrayVisitor {
            scratch: self.scratch,
        })
    }
}

struct EventsArrayVisitor<'a> {
    scratch: &'a mut CoinbaseAdvancedScratch,
}

impl<'de> Visitor<'de> for EventsArrayVisitor<'_> {
    type Value = (Option<&'de str>, Option<L2EventKind>);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an events array")
    }

    fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
    where
        S: SeqAccess<'de>,
    {
        let mut first_product: Option<&'de str> = None;
        let mut first_kind: Option<L2EventKind> = None;

        while let Some((p_id, k)) = seq.next_element_seed(SingleEventSeed {
            scratch: self.scratch,
        })? {
            if first_product.is_none() {
                first_product = p_id;
            }
            if first_kind.is_none() {
                first_kind = k;
            }
        }

        Ok((first_product, first_kind))
    }
}

struct SingleEventSeed<'a> {
    scratch: &'a mut CoinbaseAdvancedScratch,
}

impl<'de> DeserializeSeed<'de> for SingleEventSeed<'_> {
    type Value = (Option<&'de str>, Option<L2EventKind>);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(SingleEventVisitor {
            scratch: self.scratch,
        })
    }
}

struct SingleEventVisitor<'a> {
    scratch: &'a mut CoinbaseAdvancedScratch,
}

impl<'de> Visitor<'de> for SingleEventVisitor<'_> {
    type Value = (Option<&'de str>, Option<L2EventKind>);

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a single event object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut product_id: Option<&'de str> = None;
        let mut event_kind: Option<L2EventKind> = None;
        let bids_start = self.scratch.bids.len();
        let asks_start = self.scratch.asks.len();

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "type" => {
                    let k: &'de str = map.next_value()?;
                    if k == "snapshot" {
                        event_kind = Some(L2EventKind::Snapshot);
                    } else if k == "update" {
                        event_kind = Some(L2EventKind::Update);
                    }
                }
                "product_id" => {
                    product_id = Some(map.next_value()?);
                }
                "updates" => {
                    map.next_value_seed(UpdatesSeed {
                        scratch: self.scratch,
                    })?;
                }
                _ => {
                    let _ = map.next_value::<IgnoredAny>()?;
                }
            }
        }

        if let (Some(p_id), Some(k)) = (product_id, event_kind) {
            let bids_end = self.scratch.bids.len();
            let asks_end = self.scratch.asks.len();
            self.scratch.events.push(ParsedL2Event {
                product_id: Symbol::from(p_id),
                kind: k,
                bids_start,
                bids_end,
                asks_start,
                asks_end,
            });
        }

        Ok((product_id, event_kind))
    }
}

struct UpdatesSeed<'a> {
    scratch: &'a mut CoinbaseAdvancedScratch,
}

impl<'de> DeserializeSeed<'de> for UpdatesSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(UpdatesVisitor {
            scratch: self.scratch,
        })
    }
}

struct UpdatesVisitor<'a> {
    scratch: &'a mut CoinbaseAdvancedScratch,
}

impl<'de> Visitor<'de> for UpdatesVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of level updates")
    }

    fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
    where
        S: SeqAccess<'de>,
    {
        while let Some(upd) = seq.next_element::<RawL2Update<'de>>()? {
            let level = PriceLevel::new(upd.price_level, upd.new_quantity);
            if upd.side == "bid" {
                self.scratch.bids.push(level);
            } else if upd.side == "offer" {
                self.scratch.asks.push(level);
            }
        }
        Ok(())
    }
}

/// Builds a JSON subscribe message frame for a channel and product IDs.
pub fn build_subscribe_frame(channel: &str, products: &[&str]) -> String {
    let payload = serde_json::json!({
        "type": "subscribe",
        "product_ids": products,
        "channel": channel,
    });
    payload.to_string()
}

/// Builds a JSON unsubscribe message frame for a channel and product IDs.
pub fn build_unsubscribe_frame(channel: &str, products: &[&str]) -> String {
    let payload = serde_json::json!({
        "type": "unsubscribe",
        "product_ids": products,
        "channel": channel,
    });
    payload.to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use mdc_testkit::load_fixture;

    const SNAPSHOT_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/coinbase_advanced/order_book_snapshot.json";
    const UPDATE_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/coinbase_advanced/depth_update.json";
    const PING_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/coinbase_advanced/heartbeat_or_ping.json";
    const TRADE_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/coinbase_advanced/trade.json";
    const SUB_FIXTURE: &str =
        "crates/mdc-providers/tests/fixtures/coinbase_advanced/subscriptions.json";
    const ERROR_FIXTURE: &str = "crates/mdc-providers/tests/fixtures/coinbase_advanced/error.json";

    #[test]
    fn test_parse_snapshot_fixture() {
        let raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
        let env: RawEnvelope<'_> = serde_json::from_str(&raw).expect("parse envelope");

        assert_eq!(env.channel, "l2_data");
        assert_eq!(env.sequence_num, Some(1));
        assert_eq!(env.timestamp, Some("2023-02-09T20:32:50.714964855Z"));
        assert_eq!(env.events.len(), 1);

        let event: RawL2Event<'_> =
            serde_json::from_str(env.events[0].get()).expect("parse l2 event");
        assert_eq!(event.kind, "snapshot");
        assert_eq!(event.product_id, "BTC-USD");
        assert_eq!(event.updates.len(), 2);

        assert_eq!(event.updates[0].side, "bid");
        assert_eq!(
            event.updates[0].price_level,
            Decimal::from_str_exact("21921.73").unwrap()
        );
        assert_eq!(
            event.updates[0].new_quantity,
            Decimal::from_str_exact("0.06317902").unwrap()
        );

        assert_eq!(event.updates[1].side, "offer");
        assert_eq!(
            event.updates[1].price_level,
            Decimal::from_str_exact("21925.00").unwrap()
        );
        assert_eq!(
            event.updates[1].new_quantity,
            Decimal::from_str_exact("0.15000000").unwrap()
        );
    }

    #[test]
    fn test_parse_update_fixture() {
        let raw = load_fixture(UPDATE_FIXTURE).expect("load update");
        let env: RawEnvelope<'_> = serde_json::from_str(&raw).expect("parse envelope");

        assert_eq!(env.channel, "l2_data");
        assert_eq!(env.sequence_num, Some(2));
        assert_eq!(env.events.len(), 1);

        let event: RawL2Event<'_> =
            serde_json::from_str(env.events[0].get()).expect("parse l2 event");
        assert_eq!(event.kind, "update");
        assert_eq!(event.product_id, "BTC-USD");
        assert_eq!(event.updates.len(), 2);

        assert_eq!(event.updates[0].side, "bid");
        assert_eq!(
            event.updates[0].new_quantity,
            Decimal::from_str_exact("0.08000000").unwrap()
        );

        assert_eq!(event.updates[1].side, "offer");
        assert_eq!(event.updates[1].new_quantity, Decimal::ZERO);
    }

    #[test]
    fn test_parse_heartbeat_fixture() {
        let raw = load_fixture(PING_FIXTURE).expect("load heartbeat");
        let env: RawEnvelope<'_> = serde_json::from_str(&raw).expect("parse envelope");

        assert_eq!(env.channel, "heartbeats");
        assert_eq!(env.sequence_num, Some(3));
        assert_eq!(env.events.len(), 1);

        let hb: RawHeartbeatEvent<'_> =
            serde_json::from_str(env.events[0].get()).expect("parse heartbeat");
        assert_eq!(hb.heartbeat_counter, Some(42));
    }

    #[test]
    fn test_parse_trade_fixture() {
        let raw = load_fixture(TRADE_FIXTURE).expect("load trade");
        let env: RawEnvelope<'_> = serde_json::from_str(&raw).expect("parse envelope");

        assert_eq!(env.channel, "market_trades");
        assert_eq!(env.sequence_num, Some(4));
        assert_eq!(env.events.len(), 1);

        let event: RawTradeEvent<'_> =
            serde_json::from_str(env.events[0].get()).expect("parse trade event");
        assert_eq!(event.kind, "update");
        assert_eq!(event.trades.len(), 1);

        let trade = &event.trades[0];
        assert_eq!(trade.trade_id, "12345");
        assert_eq!(trade.product_id, "BTC-USD");
        assert_eq!(trade.price, Decimal::from_str_exact("21925.00").unwrap());
        assert_eq!(trade.size, Decimal::from_str_exact("0.001").unwrap());
        assert_eq!(trade.side, "BUY");
    }

    #[test]
    fn test_parse_subscriptions_fixture() {
        let raw = load_fixture(SUB_FIXTURE).expect("load sub");
        let env: RawEnvelope<'_> = serde_json::from_str(&raw).expect("parse envelope");

        assert_eq!(env.channel, "subscriptions");
        assert_eq!(env.sequence_num, Some(0));
        assert_eq!(env.events.len(), 1);

        let sub: RawSubscriptionsEvent<'_> =
            serde_json::from_str(env.events[0].get()).expect("parse sub event");
        assert!(sub.subscriptions.contains_key("level2"));
        assert!(sub.subscriptions.contains_key("heartbeats"));
    }

    #[test]
    fn test_parse_error_fixture() {
        let raw = load_fixture(ERROR_FIXTURE).expect("load error");
        let err: RawErrorEnvelope<'_> = serde_json::from_str(&raw).expect("parse error");
        assert_eq!(err.kind, Some("error"));
        assert_eq!(err.message, Some("Invalid product_id"));
    }

    #[test]
    fn test_frame_seed_stream_deserialization() {
        let raw = load_fixture(SNAPSHOT_FIXTURE).expect("load snapshot");
        let mut scratch = CoinbaseAdvancedScratch::with_capacity(16);

        let mut de = serde_json::Deserializer::from_str(&raw);
        let header = CoinbaseAdvancedFrameSeed::new(&mut scratch)
            .deserialize(&mut de)
            .expect("deserialize frame");

        assert_eq!(header.channel, "l2_data");
        assert_eq!(header.sequence_num, Some(1));
        assert_eq!(header.product_id, Some("BTC-USD"));
        assert_eq!(header.l2_kind, Some(L2EventKind::Snapshot));
        assert_eq!(scratch.bids.len(), 1);
        assert_eq!(scratch.asks.len(), 1);
        assert_eq!(
            scratch.bids[0].price,
            Decimal::from_str_exact("21921.73").unwrap()
        );
        assert_eq!(
            scratch.asks[0].price,
            Decimal::from_str_exact("21925.00").unwrap()
        );
    }

    #[test]
    fn test_build_frames() {
        let sub = build_subscribe_frame("level2", &["BTC-USD", "ETH-USD"]);
        let v: serde_json::Value = serde_json::from_str(&sub).unwrap();
        assert_eq!(v["type"], "subscribe");
        assert_eq!(v["channel"], "level2");
        assert_eq!(v["product_ids"], serde_json::json!(["BTC-USD", "ETH-USD"]));

        let unsub = build_unsubscribe_frame("level2", &["BTC-USD"]);
        let v: serde_json::Value = serde_json::from_str(&unsub).unwrap();
        assert_eq!(v["type"], "unsubscribe");
        assert_eq!(v["channel"], "level2");
        assert_eq!(v["product_ids"], serde_json::json!(["BTC-USD"]));
    }
}
