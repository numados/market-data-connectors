//! Raw exchange message schemas and zero-allocation deserialization for Huobi (HTX).

use std::fmt;

use mdc_core::PriceLevel;
use rust_decimal::Decimal;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use smallstr::SmallString;

/// Extracts currency pair symbol from a Huobi topic channel.
///
/// Supported patterns:
/// - `"market.{symbol}.mbp.{levels}"` -> `"{symbol}"`
/// - `"market.{symbol}.trade.detail"` -> `"{symbol}"`
#[inline]
pub fn extract_symbol_from_channel(ch: &str) -> Option<&str> {
    let rest = ch.strip_prefix("market.")?;
    if let Some((sym, _)) = rest.split_once(".mbp.") {
        Some(sym)
    } else if let Some((sym, _)) = rest.split_once(".trade.detail") {
        Some(sym)
    } else {
        rest.split('.').next()
    }
}

/// Extracts MBP levels from a channel (e.g. `"market.btcusdt.mbp.400"` -> `Some(400)`).
#[inline]
pub fn extract_levels_from_channel(ch: &str) -> Option<u32> {
    let rest = ch.strip_prefix("market.")?;
    let (_, levels_str) = rest.split_once(".mbp.")?;
    levels_str.parse().ok()
}

/// Builds a JSON subscription frame for incremental Market-By-Price (`mbp.{levels}`).
pub fn build_sub_mbp_frame(symbol: &str, levels: u32, id: &str) -> String {
    let topic = format!("market.{symbol}.mbp.{levels}");
    let req = SubRequest { sub: &topic, id };
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Builds a JSON unsubscription frame for incremental Market-By-Price (`mbp.{levels}`).
pub fn build_unsub_mbp_frame(symbol: &str, levels: u32, id: Option<&str>) -> String {
    let topic = format!("market.{symbol}.mbp.{levels}");
    let req = UnsubRequest { unsub: &topic, id };
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Builds a JSON subscription frame for public trades (`market.{symbol}.trade.detail`).
pub fn build_sub_trades_frame(symbol: &str, id: &str) -> String {
    let topic = format!("market.{symbol}.trade.detail");
    let req = SubRequest { sub: &topic, id };
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Builds a JSON request frame to fetch an in-band MBP order book snapshot.
pub fn build_req_snapshot_frame(symbol: &str, levels: u32, req_id: &str) -> String {
    let topic = format!("market.{symbol}.mbp.{levels}");
    let req = ReqSnapshotRequest {
        req: &topic,
        id: req_id,
    };
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&req).unwrap()
}

/// Builds a JSON pong response frame answering Huobi's application ping.
pub fn build_pong_frame(ping_ts: u64) -> String {
    let pong = PongFrame { pong: ping_ts };
    #[allow(clippy::unwrap_used)]
    serde_json::to_string(&pong).unwrap()
}

/// Outgoing subscription request payload.
#[derive(Debug, Serialize)]
pub struct SubRequest<'a> {
    pub sub: &'a str,
    pub id: &'a str,
}

/// Outgoing unsubscription request payload.
#[derive(Debug, Serialize)]
pub struct UnsubRequest<'a> {
    pub unsub: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<&'a str>,
}

/// Outgoing snapshot request payload (`req`).
#[derive(Debug, Serialize)]
pub struct ReqSnapshotRequest<'a> {
    pub req: &'a str,
    pub id: &'a str,
}

/// Outgoing application-level pong response.
#[derive(Debug, Serialize)]
pub struct PongFrame {
    pub pong: u64,
}

/// Top-level raw message envelope for Huobi WebSocket frames.
#[derive(Debug, Deserialize)]
pub struct RawEnvelope<'a> {
    /// Data push topic (e.g. `"market.btcusdt.mbp.400"`).
    #[serde(default)]
    pub ch: Option<&'a str>,

    /// Snapshot reply topic (e.g. `"market.btcusdt.mbp.400"`).
    #[serde(default)]
    pub rep: Option<&'a str>,

    /// Application heartbeat ping timestamp from server.
    #[serde(default)]
    pub ping: Option<u64>,

    /// Heartbeat pong timestamp.
    #[serde(default)]
    pub pong: Option<u64>,

    /// Status indicator (`"ok"` or `"error"`).
    #[serde(default)]
    pub status: Option<&'a str>,

    /// Topic acknowledged in subscription confirmation.
    #[serde(default)]
    pub subbed: Option<&'a str>,

    /// Error code if status is error (e.g. `"bad-request"`).
    #[serde(default, rename = "err-code")]
    pub err_code: Option<&'a str>,

    /// Error message if status is error.
    #[serde(default, rename = "err-msg")]
    pub err_msg: Option<&'a str>,

    /// Client request correlation ID.
    #[serde(default)]
    pub id: Option<&'a str>,

    /// Server-side message timestamp in milliseconds.
    #[serde(default)]
    pub ts: Option<u64>,

    /// Incremental tick payload.
    #[serde(borrow, default)]
    pub tick: Option<&'a RawValue>,

    /// Snapshot reply data payload.
    #[serde(borrow, default)]
    pub data: Option<&'a RawValue>,
}

/// High-level message classification based on Huobi dispatch rules.
///
/// Dispatch order: `ping` -> `rep` -> `ch` -> `subbed`/`status`.
#[derive(Debug, PartialEq, Eq)]
pub enum HuobiMessageKind<'a> {
    /// Server keepalive ping requiring immediate pong response.
    Ping(u64),

    /// In-band snapshot reply matching a prior `req`.
    SnapshotReply {
        id: &'a str,
        rep: &'a str,
        status: &'a str,
        ts: Option<u64>,
    },

    /// Market-By-Price incremental update tick.
    MbpTick { ch: &'a str, ts: u64 },

    /// Public trade detail push tick.
    TradeTick { ch: &'a str, ts: u64 },

    /// Subscription confirmation.
    SubAck {
        id: &'a str,
        subbed: &'a str,
        status: &'a str,
    },

    /// Error response.
    Error {
        id: Option<&'a str>,
        err_code: Option<&'a str>,
        err_msg: Option<&'a str>,
    },

    /// Other or unhandled channel / payload.
    Other,
}

impl<'a> RawEnvelope<'a> {
    /// Classifies this envelope according to the documented dispatch order.
    pub fn message_kind(&self) -> HuobiMessageKind<'a> {
        if let Some(ping) = self.ping {
            return HuobiMessageKind::Ping(ping);
        }

        if let (Some(rep), Some(id), Some(status)) = (self.rep, self.id, self.status) {
            return HuobiMessageKind::SnapshotReply {
                id,
                rep,
                status,
                ts: self.ts,
            };
        }

        if let (Some(ch), Some(ts)) = (self.ch, self.ts) {
            if ch.contains(".mbp.") {
                return HuobiMessageKind::MbpTick { ch, ts };
            }
            if ch.ends_with(".trade.detail") {
                return HuobiMessageKind::TradeTick { ch, ts };
            }
        }

        if let (Some(subbed), Some(id), Some(status)) = (self.subbed, self.id, self.status) {
            return HuobiMessageKind::SubAck { id, subbed, status };
        }

        if self.status == Some("error") || self.err_code.is_some() {
            return HuobiMessageKind::Error {
                id: self.id,
                err_code: self.err_code,
                err_msg: self.err_msg,
            };
        }

        HuobiMessageKind::Other
    }
}

/// Scratch buffers for connection level deserialization.
#[derive(Debug, Default)]
pub struct HuobiConnectionScratch {
    /// Bid price levels.
    pub bids: Vec<PriceLevel>,
    /// Ask price levels.
    pub asks: Vec<PriceLevel>,
}

impl HuobiConnectionScratch {
    /// Creates a new scratch buffer pre-allocated for `capacity` levels per side.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
        }
    }

    /// Clears both bid and ask buffers.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

/// Metadata header for an MBP message (update or snapshot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MbpHeader {
    /// Current sequence number.
    pub seq_num: u64,
    /// Previous sequence number (`None` in snapshots).
    pub prev_seq_num: Option<u64>,
}

/// Owned MBP model used in tests and fallbacks.
#[derive(Debug, Deserialize, Clone)]
pub struct RawMbp {
    #[serde(rename = "seqNum")]
    pub seq_num: u64,
    #[serde(rename = "prevSeqNum", default)]
    pub prev_seq_num: Option<u64>,
    #[serde(default, deserialize_with = "levels_from_f64")]
    pub bids: Vec<PriceLevel>,
    #[serde(default, deserialize_with = "levels_from_f64")]
    pub asks: Vec<PriceLevel>,
}

/// Converts an f64 price or volume into a normalized [`Decimal`] honoring Huobi's ≤ 8 decimals precision.
#[inline]
pub fn decimal_from_f64(val: f64) -> Option<Decimal> {
    Decimal::from_f64_retain(val).map(|d| d.round_dp(8).normalize())
}

/// Deserializes a sequence of `[price, amount]` floating-point pairs into `Vec<PriceLevel>`.
pub fn levels_from_f64<'de, D>(deserializer: D) -> Result<Vec<PriceLevel>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OwnedLevelsVisitor;

    impl<'de> Visitor<'de> for OwnedLevelsVisitor {
        type Value = Vec<PriceLevel>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a sequence of [price, amount] number pairs")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut levels = Vec::new();
            while let Some([price_f, amount_f]) = seq.next_element::<[f64; 2]>()? {
                let price = decimal_from_f64(price_f)
                    .ok_or_else(|| serde::de::Error::custom(format!("invalid price: {price_f}")))?;
                let amount = decimal_from_f64(amount_f).ok_or_else(|| {
                    serde::de::Error::custom(format!("invalid amount: {amount_f}"))
                })?;
                levels.push(PriceLevel::new(price, amount));
            }
            Ok(levels)
        }
    }

    deserializer.deserialize_seq(OwnedLevelsVisitor)
}

/// Deserializer seed for level arrays that writes directly into a caller-supplied vector.
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
        formatter.write_str("a sequence of [price, amount] number pairs")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some([price_f, amount_f]) = seq.next_element::<[f64; 2]>()? {
            let price = decimal_from_f64(price_f)
                .ok_or_else(|| serde::de::Error::custom(format!("invalid price: {price_f}")))?;
            let amount = decimal_from_f64(amount_f)
                .ok_or_else(|| serde::de::Error::custom(format!("invalid amount: {amount_f}")))?;
            self.0.push(PriceLevel::new(price, amount));
        }
        Ok(())
    }
}

/// Zero-allocation deserializer seed for Huobi MBP `tick` or snapshot `data` payloads.
pub struct MbpSeed<'buf> {
    scratch: &'buf mut HuobiConnectionScratch,
}

impl<'buf> MbpSeed<'buf> {
    /// Creates a new `MbpSeed` using the provided connection scratch buffers.
    pub fn new(scratch: &'buf mut HuobiConnectionScratch) -> Self {
        scratch.clear();
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for MbpSeed<'buf> {
    type Value = MbpHeader;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(MbpVisitor {
            scratch: self.scratch,
        })
    }
}

struct MbpVisitor<'buf> {
    scratch: &'buf mut HuobiConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for MbpVisitor<'buf> {
    type Value = MbpHeader;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Huobi MBP JSON object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut seq_num = None;
        let mut prev_seq_num = None;

        while let Some(key) = map.next_key::<&str>()? {
            match key {
                "seqNum" => {
                    seq_num = Some(map.next_value::<u64>()?);
                }
                "prevSeqNum" => {
                    prev_seq_num = Some(map.next_value::<u64>()?);
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

        let seq_num = seq_num.ok_or_else(|| serde::de::Error::missing_field("seqNum"))?;

        Ok(MbpHeader {
            seq_num,
            prev_seq_num,
        })
    }
}

/// Parsed metadata for a zero-allocation MBP WebSocket frame (tick or snapshot reply).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HuobiDepthFrameHeader<'de> {
    pub ch: Option<&'de str>,
    pub rep: Option<&'de str>,
    pub ts: Option<u64>,
    pub id: Option<&'de str>,
    pub mbp: MbpHeader,
}

/// Zero-allocation deserializer seed for top-level Huobi MBP frames targeting scratch buffers.
pub struct HuobiDepthFrameSeed<'buf> {
    scratch: &'buf mut HuobiConnectionScratch,
}

impl<'buf> HuobiDepthFrameSeed<'buf> {
    /// Creates a new `HuobiDepthFrameSeed` targeting pre-allocated scratch buffers.
    pub fn new(scratch: &'buf mut HuobiConnectionScratch) -> Self {
        scratch.clear();
        Self { scratch }
    }
}

impl<'de, 'buf> DeserializeSeed<'de> for HuobiDepthFrameSeed<'buf> {
    type Value = HuobiDepthFrameHeader<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(HuobiDepthFrameVisitor {
            scratch: self.scratch,
        })
    }
}

struct HuobiDepthFrameVisitor<'buf> {
    scratch: &'buf mut HuobiConnectionScratch,
}

impl<'de, 'buf> Visitor<'de> for HuobiDepthFrameVisitor<'buf> {
    type Value = HuobiDepthFrameHeader<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Huobi MBP WebSocket frame object")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut ch = None;
        let mut rep = None;
        let mut ts = None;
        let mut id = None;
        let mut mbp = None;

        while let Some(key) = map.next_key::<&'de str>()? {
            match key {
                "ch" => ch = Some(map.next_value::<&'de str>()?),
                "rep" => rep = Some(map.next_value::<&'de str>()?),
                "ts" => ts = Some(map.next_value::<u64>()?),
                "id" => id = Some(map.next_value::<&'de str>()?),
                "tick" => {
                    let h = map.next_value_seed(MbpSeed::new(self.scratch))?;
                    mbp = Some(h);
                }
                "data" => {
                    let h = map.next_value_seed(MbpSeed::new(self.scratch))?;
                    mbp = Some(h);
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }

        let mbp =
            mbp.ok_or_else(|| serde::de::Error::custom("missing tick or data field in MBP frame"))?;

        Ok(HuobiDepthFrameHeader {
            ch,
            rep,
            ts,
            id,
            mbp,
        })
    }
}

/// Raw public trade push tick (`market.{symbol}.trade.detail`).
#[derive(Debug, Deserialize, Clone)]
pub struct RawTradeTick<'a> {
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub ts: Option<u64>,
    #[serde(borrow)]
    pub data: Vec<RawTradeItem<'a>>,
}

/// Raw trade record inside `RawTradeTick.data`.
#[derive(Debug, Deserialize, Clone)]
pub struct RawTradeItem<'a> {
    #[serde(default)]
    pub id: Option<RawTradeId<'a>>,
    #[serde(rename = "tradeId")]
    pub trade_id: RawTradeId<'a>,
    pub ts: u64,
    pub amount: f64,
    pub price: f64,
    pub direction: &'a str,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_symbol_from_channel() {
        assert_eq!(
            extract_symbol_from_channel("market.btcusdt.mbp.400"),
            Some("btcusdt")
        );
        assert_eq!(
            extract_symbol_from_channel("market.ethusdt.mbp.150"),
            Some("ethusdt")
        );
        assert_eq!(
            extract_symbol_from_channel("market.btcusdt.trade.detail"),
            Some("btcusdt")
        );
        assert_eq!(extract_symbol_from_channel("invalid_topic"), None);
    }

    #[test]
    fn test_extract_levels_from_channel() {
        assert_eq!(
            extract_levels_from_channel("market.btcusdt.mbp.400"),
            Some(400)
        );
        assert_eq!(
            extract_levels_from_channel("market.btcusdt.mbp.150"),
            Some(150)
        );
        assert_eq!(
            extract_levels_from_channel("market.btcusdt.trade.detail"),
            None
        );
    }

    #[test]
    fn test_parse_heartbeat_or_ping_fixture() {
        let json = include_str!("../../tests/fixtures/huobi/heartbeat_or_ping.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(env.message_kind(), HuobiMessageKind::Ping(1700000000000));
    }

    #[test]
    fn test_parse_sub_ack_fixture() {
        let json = include_str!("../../tests/fixtures/huobi/sub_ack.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(
            env.message_kind(),
            HuobiMessageKind::SubAck {
                id: "id1",
                subbed: "market.btcusdt.mbp.400",
                status: "ok",
            }
        );
    }

    #[test]
    fn test_parse_error_fixture() {
        let json = include_str!("../../tests/fixtures/huobi/error.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(
            env.message_kind(),
            HuobiMessageKind::Error {
                id: Some("id1"),
                err_code: Some("bad-request"),
                err_msg: Some("Invalid topic"),
            }
        );
    }

    #[test]
    fn test_parse_depth_update_fixture() {
        let json = include_str!("../../tests/fixtures/huobi/depth_update.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(
            env.message_kind(),
            HuobiMessageKind::MbpTick {
                ch: "market.btcusdt.mbp.400",
                ts: 1700000000100,
            }
        );

        let tick_raw = env.tick.unwrap();
        let mut scratch = HuobiConnectionScratch::with_capacity(10);
        let mut de = serde_json::Deserializer::from_str(tick_raw.get());
        let header = MbpSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        assert_eq!(header.seq_num, 100020146795);
        assert_eq!(header.prev_seq_num, Some(100020146794));
        assert_eq!(scratch.bids.len(), 2);
        assert_eq!(scratch.asks.len(), 2);

        // 26000.1, 0.5
        assert_eq!(
            scratch.bids[0].price,
            Decimal::from_str_exact("26000.1").unwrap()
        );
        assert_eq!(
            scratch.bids[0].volume,
            Decimal::from_str_exact("0.5").unwrap()
        );
        // 26000.2, 0.0 (deletion)
        assert_eq!(
            scratch.asks[0].price,
            Decimal::from_str_exact("26000.2").unwrap()
        );
        assert_eq!(scratch.asks[0].volume, Decimal::ZERO);
    }

    #[test]
    fn test_parse_order_book_snapshot_fixture() {
        let json = include_str!("../../tests/fixtures/huobi/order_book_snapshot.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(
            env.message_kind(),
            HuobiMessageKind::SnapshotReply {
                id: "snap-1",
                rep: "market.btcusdt.mbp.400",
                status: "ok",
                ts: Some(1700000000200),
            }
        );

        let data_raw = env.data.unwrap();
        let mut scratch = HuobiConnectionScratch::with_capacity(10);
        let mut de = serde_json::Deserializer::from_str(data_raw.get());
        let header = MbpSeed::new(&mut scratch).deserialize(&mut de).unwrap();

        assert_eq!(header.seq_num, 100020146800);
        assert_eq!(header.prev_seq_num, None);
        assert_eq!(scratch.bids.len(), 2);
        assert_eq!(scratch.asks.len(), 2);
    }

    #[test]
    fn test_parse_trade_fixture() {
        let json = include_str!("../../tests/fixtures/huobi/trade.json");
        let env: RawEnvelope<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(
            env.message_kind(),
            HuobiMessageKind::TradeTick {
                ch: "market.btcusdt.trade.detail",
                ts: 1700000000050,
            }
        );

        let tick_raw = env.tick.unwrap();
        let trade_tick: RawTradeTick<'_> = serde_json::from_str(tick_raw.get()).unwrap();
        assert_eq!(trade_tick.data.len(), 1);
        let item = &trade_tick.data[0];
        assert_eq!(item.trade_id.to_small_str().as_str(), "102043494568");
        assert_eq!(item.ts, 1700000000050);
        assert_eq!(item.direction, "buy");
    }

    #[test]
    fn test_build_frames() {
        let sub = build_sub_mbp_frame("btcusdt", 400, "req-1");
        assert!(sub.contains("\"sub\":\"market.btcusdt.mbp.400\""));
        assert!(sub.contains("\"id\":\"req-1\""));

        let unsub = build_unsub_mbp_frame("btcusdt", 400, Some("req-2"));
        assert!(unsub.contains("\"unsub\":\"market.btcusdt.mbp.400\""));

        let snap = build_req_snapshot_frame("btcusdt", 400, "snap-1");
        assert!(snap.contains("\"req\":\"market.btcusdt.mbp.400\""));

        let pong = build_pong_frame(1700000000000);
        assert_eq!(pong, "{\"pong\":1700000000000}");
    }

    #[test]
    fn test_decimal_from_f64_boundary() {
        // Documented 8-decimal precision boundary: 26000.12345678
        let d = decimal_from_f64(26000.12345678).unwrap();
        assert_eq!(d, Decimal::from_str_exact("26000.12345678").unwrap());

        let d2 = decimal_from_f64(26000.1).unwrap();
        assert_eq!(d2, Decimal::from_str_exact("26000.1").unwrap());

        let zero = decimal_from_f64(0.0).unwrap();
        assert_eq!(zero, Decimal::ZERO);
    }
}
