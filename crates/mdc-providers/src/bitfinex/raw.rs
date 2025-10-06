//! Wire format models and custom positional sequence visitor for Bitfinex WebSocket v2.

use std::fmt;

use rust_decimal::Decimal;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use smallstr::SmallString;

/// Parses a decimal token string, handling standard format and scientific notation (e.g. `1e-8`).
pub fn parse_decimal_token(s: &str) -> Result<Decimal, rust_decimal::Error> {
    Decimal::from_str_exact(s).or_else(|_| Decimal::from_scientific(s))
}

/// Raw price level borrowing exact JSON number tokens for checksum computation.
#[derive(Debug, Clone, Copy)]
pub struct RawBookLevel<'a> {
    /// Exact raw JSON number token for price, e.g. `"41698"` or `"0.030291"`.
    pub price: &'a RawValue,
    /// Number of orders at that price level (0 indicates level deletion).
    pub count: u32,
    /// Total amount available at that price level (positive = bid, negative = ask).
    pub amount: &'a RawValue,
}

impl<'a> RawBookLevel<'a> {
    /// Returns the raw price token with enclosing quotes trimmed if any.
    #[inline]
    pub fn price_str(&self) -> &'a str {
        self.price.get().trim_matches('"')
    }

    /// Returns the raw amount token with enclosing quotes trimmed if any.
    #[inline]
    pub fn amount_str(&self) -> &'a str {
        self.amount.get().trim_matches('"')
    }

    /// Parses price as a Decimal.
    pub fn parse_price(&self) -> Result<Decimal, rust_decimal::Error> {
        parse_decimal_token(self.price_str())
    }

    /// Parses amount as a Decimal.
    pub fn parse_amount(&self) -> Result<Decimal, rust_decimal::Error> {
        parse_decimal_token(self.amount_str())
    }
}

impl<'de> Deserialize<'de> for RawBookLevel<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = RawBookLevel<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a 3-element price level array [price, count, amount]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let price = seq
                    .next_element::<&'de RawValue>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &"price"))?;
                let count = seq
                    .next_element::<u32>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &"count"))?;
                let amount = seq
                    .next_element::<&'de RawValue>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &"amount"))?;
                Ok(RawBookLevel {
                    price,
                    count,
                    amount,
                })
            }
        }
        deserializer.deserialize_seq(V)
    }
}

/// Owned price level retaining exact JSON number tokens within inline string buffers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawBookLevelOwned {
    /// Parsed price decimal.
    pub price: Decimal,
    /// Number of orders at that price level (0 indicates level deletion).
    pub count: u32,
    /// Parsed volume/amount decimal (signed: positive = bid, negative = ask).
    pub amount: Decimal,
    /// Raw token string for price (inline up to 24 bytes).
    pub price_raw: SmallString<[u8; 24]>,
    /// Raw token string for amount (inline up to 24 bytes).
    pub amount_raw: SmallString<[u8; 24]>,
}

impl RawBookLevelOwned {
    /// Converts a borrowed `RawBookLevel` into an owned representation.
    pub fn from_raw(raw: &RawBookLevel<'_>) -> Result<Self, rust_decimal::Error> {
        let price_s = raw.price_str();
        let amount_s = raw.amount_str();
        let price = parse_decimal_token(price_s)?;
        let amount = parse_decimal_token(amount_s)?;
        Ok(Self {
            price,
            count: raw.count,
            amount,
            price_raw: SmallString::from(price_s),
            amount_raw: SmallString::from(amount_s),
        })
    }
}

/// Connection scratch buffer retaining capacity across WebSocket frames.
#[derive(Debug, Default)]
pub struct BitfinexConnectionScratch {
    /// Reusable buffer for raw book levels during snapshots and updates.
    pub levels: Vec<RawBookLevelOwned>,
}

impl BitfinexConnectionScratch {
    /// Creates a new scratch buffer with pre-allocated capacity for standard 250 depth levels.
    pub fn new() -> Self {
        Self {
            levels: Vec::with_capacity(250),
        }
    }

    /// Clears the scratch buffer while preserving allocated memory.
    pub fn clear(&mut self) {
        self.levels.clear();
    }
}

/// Raw public trade entry: `[id, mts, amount, price]`.
#[derive(Debug, Clone, Copy)]
pub struct RawTradeEntry<'a> {
    /// Unique trade identifier.
    pub id: u64,
    /// Execution timestamp in milliseconds.
    pub mts: u64,
    /// Executed amount (positive = buy / bid aggressor, negative = sell / ask aggressor).
    pub amount: &'a RawValue,
    /// Executed price.
    pub price: &'a RawValue,
}

impl<'a> RawTradeEntry<'a> {
    /// Returns the raw amount token.
    #[inline]
    pub fn amount_str(&self) -> &'a str {
        self.amount.get().trim_matches('"')
    }

    /// Returns the raw price token.
    #[inline]
    pub fn price_str(&self) -> &'a str {
        self.price.get().trim_matches('"')
    }

    /// Parses amount as a Decimal.
    pub fn parse_amount(&self) -> Result<Decimal, rust_decimal::Error> {
        parse_decimal_token(self.amount_str())
    }

    /// Parses price as a Decimal.
    pub fn parse_price(&self) -> Result<Decimal, rust_decimal::Error> {
        parse_decimal_token(self.price_str())
    }
}

impl<'de> Deserialize<'de> for RawTradeEntry<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = RawTradeEntry<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a 4-element trade array [id, mts, amount, price]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let id = seq
                    .next_element::<u64>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &"id"))?;
                let mts = seq
                    .next_element::<u64>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &"mts"))?;
                let amount = seq
                    .next_element::<&'de RawValue>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(2, &"amount"))?;
                let price = seq
                    .next_element::<&'de RawValue>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(3, &"price"))?;
                Ok(RawTradeEntry {
                    id,
                    mts,
                    amount,
                    price,
                })
            }
        }
        deserializer.deserialize_seq(V)
    }
}

/// Control and event notifications from Bitfinex WebSocket API.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "event")]
pub enum RawEvent<'a> {
    /// Information event emitted immediately upon connection.
    #[serde(rename = "info")]
    Info {
        /// API version number.
        version: Option<u32>,
        /// Server connection ID.
        #[serde(rename = "serverId")]
        server_id: Option<&'a str>,
    },
    /// Response to a `conf` event flag request.
    #[serde(rename = "conf")]
    Conf {
        /// Status string, typically `"OK"`.
        status: Option<&'a str>,
        /// Negotiated flags integer.
        flags: Option<u64>,
    },
    /// Channel subscription confirmation.
    #[serde(rename = "subscribed")]
    Subscribed {
        /// Subscribed channel name, e.g. `"book"` or `"trades"`.
        channel: &'a str,
        /// Connection-unique channel ID assigned by the venue.
        #[serde(rename = "chanId")]
        chan_id: u32,
        /// Requested symbol, e.g. `"tBTCUSD"`.
        symbol: Option<&'a str>,
        /// Trading pair, e.g. `"BTCUSD"`.
        pair: Option<&'a str>,
        /// Order book precision (for book channel), e.g. `"P0"`.
        prec: Option<&'a str>,
        /// Update frequency, e.g. `"F0"`.
        freq: Option<&'a str>,
        /// Book depth length, e.g. `"250"`.
        len: Option<&'a str>,
    },
    /// Channel unsubscription confirmation.
    #[serde(rename = "unsubscribed")]
    Unsubscribed {
        /// Status string, typically `"OK"`.
        status: Option<&'a str>,
        /// Channel ID that was unsubscribed.
        #[serde(rename = "chanId")]
        chan_id: u32,
    },
    /// Error notification.
    #[serde(rename = "error")]
    Error {
        /// Descriptive error message.
        msg: &'a str,
        /// Numeric error code (e.g. 10300 for duplicate subscription).
        code: u32,
    },
    /// Unhandled event notification.
    #[serde(other)]
    Other,
}

/// Payload within a positional channel message sequence.
#[derive(Debug)]
pub enum RawPayload<'a> {
    /// Heartbeat message: `[chanId, "hb", ts]`.
    Heartbeat,
    /// Order book checksum message: `[chanId, "cs", checksum, ts]`.
    Checksum(i32),
    /// Complete order book snapshot: `[chanId, [[price, count, amount], ...], ts]`.
    BookSnapshot(Vec<RawBookLevel<'a>>),
    /// Single order book update or removal: `[chanId, [price, count, amount], ts]`.
    BookUpdate(RawBookLevel<'a>),
    /// Single public trade execution: `[chanId, "te" | "tu", [id, mts, amount, price], ts]`.
    Trade {
        /// Message type indicator (`"te"` for execution, `"tu"` for update).
        kind: &'a str,
        /// Executed trade entry.
        trade: RawTradeEntry<'a>,
    },
    /// Initial trade snapshot emitted upon subscription.
    TradeSnapshot {
        /// Message type indicator (`"te"` or `"tu"`).
        kind: &'a str,
        /// List of recent trade executions.
        trades: Vec<RawTradeEntry<'a>>,
    },
}

/// Positional channel frame: `[chanId, payload, ts?]`.
#[derive(Debug)]
pub struct RawChannelFrame<'a> {
    /// Channel ID assigned by Bitfinex.
    pub chan_id: u32,
    /// Channel data payload.
    pub payload: RawPayload<'a>,
    /// Venue timestamp in milliseconds (present when TIMESTAMP flag 32768 is active).
    pub ts: Option<u64>,
}

impl<'de> Deserialize<'de> for RawChannelFrame<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = RawChannelFrame<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Bitfinex channel array [chanId, payload, ...]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let chan_id = seq
                    .next_element::<u32>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &"chanId"))?;

                let sec_raw = seq
                    .next_element::<&'de RawValue>()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &"payload"))?;

                let sec_str = sec_raw.get().trim();
                let bytes = sec_str.as_bytes();

                if bytes.starts_with(b"\"") {
                    let tag: &'de str =
                        serde_json::from_str(sec_str).map_err(serde::de::Error::custom)?;
                    match tag {
                        "hb" => {
                            let ts = seq.next_element::<u64>()?;
                            Ok(RawChannelFrame {
                                chan_id,
                                payload: RawPayload::Heartbeat,
                                ts,
                            })
                        }
                        "cs" => {
                            let checksum = seq
                                .next_element::<i32>()?
                                .ok_or_else(|| serde::de::Error::missing_field("checksum"))?;
                            let ts = seq.next_element::<u64>()?;
                            Ok(RawChannelFrame {
                                chan_id,
                                payload: RawPayload::Checksum(checksum),
                                ts,
                            })
                        }
                        kind @ ("te" | "tu" | "fte" | "ftu") => {
                            let data_raw = seq
                                .next_element::<&'de RawValue>()?
                                .ok_or_else(|| serde::de::Error::missing_field("trade_data"))?;
                            let data_str = data_raw.get().trim();
                            let ts = seq.next_element::<u64>()?;

                            let inner = data_str.strip_prefix('[').unwrap_or("").trim_start();
                            if inner.starts_with('[') {
                                let trades: Vec<RawTradeEntry<'de>> =
                                    serde_json::from_str(data_str)
                                        .map_err(serde::de::Error::custom)?;
                                Ok(RawChannelFrame {
                                    chan_id,
                                    payload: RawPayload::TradeSnapshot { kind, trades },
                                    ts,
                                })
                            } else {
                                let trade: RawTradeEntry<'de> = serde_json::from_str(data_str)
                                    .map_err(serde::de::Error::custom)?;
                                Ok(RawChannelFrame {
                                    chan_id,
                                    payload: RawPayload::Trade { kind, trade },
                                    ts,
                                })
                            }
                        }
                        _ => {
                            while seq.next_element::<&'de RawValue>()?.is_some() {}
                            Ok(RawChannelFrame {
                                chan_id,
                                payload: RawPayload::Heartbeat,
                                ts: None,
                            })
                        }
                    }
                } else if bytes.starts_with(b"[") {
                    let ts = seq.next_element::<u64>()?;
                    let inner = sec_str.strip_prefix('[').unwrap_or("").trim_start();
                    if inner.starts_with('[') {
                        let levels: Vec<RawBookLevel<'de>> =
                            serde_json::from_str(sec_str).map_err(serde::de::Error::custom)?;
                        Ok(RawChannelFrame {
                            chan_id,
                            payload: RawPayload::BookSnapshot(levels),
                            ts,
                        })
                    } else {
                        let level: RawBookLevel<'de> =
                            serde_json::from_str(sec_str).map_err(serde::de::Error::custom)?;
                        Ok(RawChannelFrame {
                            chan_id,
                            payload: RawPayload::BookUpdate(level),
                            ts,
                        })
                    }
                } else {
                    Err(serde::de::Error::custom(
                        "unexpected second element in channel frame",
                    ))
                }
            }
        }
        deserializer.deserialize_seq(V)
    }
}

/// Top-level frame received from Bitfinex WebSocket connection.
#[derive(Debug)]
pub enum RawFrame<'a> {
    /// Server notification or response event.
    Event(RawEvent<'a>),
    /// Channel data message.
    Channel(RawChannelFrame<'a>),
}

impl<'a> RawFrame<'a> {
    /// Parses a raw WebSocket message frame directly from a string slice.
    pub fn parse(s: &'a str) -> Result<Self, serde_json::Error> {
        let trimmed = s.trim_start();
        if trimmed.starts_with('{') {
            let event: RawEvent<'a> = serde_json::from_str(s)?;
            Ok(RawFrame::Event(event))
        } else if trimmed.starts_with('[') {
            let channel: RawChannelFrame<'a> = serde_json::from_str(s)?;
            Ok(RawFrame::Channel(channel))
        } else {
            Err(serde::de::Error::custom(
                "Bitfinex frame must start with '{' or '['",
            ))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/bitfinex/");

    fn load_fixture(filename: &str) -> String {
        std::fs::read_to_string(format!("{FIXTURE_DIR}{filename}"))
            .unwrap_or_else(|e| panic!("failed to load fixture {filename}: {e}"))
    }

    #[test]
    fn test_parse_info_fixture() {
        let json = load_fixture("info.json");
        let frame = RawFrame::parse(&json).expect("failed to parse info.json");
        match frame {
            RawFrame::Event(RawEvent::Info { version, server_id }) => {
                assert_eq!(version, Some(2));
                assert_eq!(server_id, Some("9b1deb4d-3b7d-4b82-9694-1a35bf4004c1"));
            }
            other => panic!("expected Info event, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_conf_ok_fixture() {
        let json = load_fixture("conf_ok.json");
        let frame = RawFrame::parse(&json).expect("failed to parse conf_ok.json");
        match frame {
            RawFrame::Event(RawEvent::Conf { status, flags }) => {
                assert_eq!(status, Some("OK"));
                assert_eq!(flags, Some(163840));
            }
            other => panic!("expected Conf event, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_subscribed_book_fixture() {
        let json = load_fixture("subscribed_book.json");
        let frame = RawFrame::parse(&json).expect("failed to parse subscribed_book.json");
        match frame {
            RawFrame::Event(RawEvent::Subscribed {
                channel,
                chan_id,
                symbol,
                prec,
                len,
                ..
            }) => {
                assert_eq!(channel, "book");
                assert_eq!(chan_id, 266343);
                assert_eq!(symbol, Some("tBTCUSD"));
                assert_eq!(prec, Some("P0"));
                assert_eq!(len, Some("250"));
            }
            other => panic!("expected Subscribed event, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_unsubscribed_fixture() {
        let json = load_fixture("unsubscribed.json");
        let frame = RawFrame::parse(&json).expect("failed to parse unsubscribed.json");
        match frame {
            RawFrame::Event(RawEvent::Unsubscribed { status, chan_id }) => {
                assert_eq!(status, Some("OK"));
                assert_eq!(chan_id, 266343);
            }
            other => panic!("expected Unsubscribed event, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_error_dup_fixture() {
        let json = load_fixture("error_dup.json");
        let frame = RawFrame::parse(&json).expect("failed to parse error_dup.json");
        match frame {
            RawFrame::Event(RawEvent::Error { msg, code }) => {
                assert_eq!(msg, "subscribe: dup");
                assert_eq!(code, 10300);
            }
            other => panic!("expected Error event, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_heartbeat_fixture() {
        let json = load_fixture("heartbeat_or_ping.json");
        let frame = RawFrame::parse(&json).expect("failed to parse heartbeat_or_ping.json");
        match frame {
            RawFrame::Channel(RawChannelFrame {
                chan_id,
                payload: RawPayload::Heartbeat,
                ts,
            }) => {
                assert_eq!(chan_id, 266343);
                assert_eq!(ts, Some(1614604260000));
            }
            other => panic!("expected Heartbeat channel frame, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_trade_fixture() {
        let json = load_fixture("trade.json");
        let frame = RawFrame::parse(&json).expect("failed to parse trade.json");
        match frame {
            RawFrame::Channel(RawChannelFrame {
                chan_id,
                payload: RawPayload::Trade { kind, trade },
                ts,
            }) => {
                assert_eq!(chan_id, 17470);
                assert_eq!(kind, "te");
                assert_eq!(trade.id, 401597395);
                assert_eq!(trade.mts, 1574694478808);
                assert_eq!(
                    trade.parse_amount().unwrap(),
                    Decimal::from_str_exact("0.005").unwrap()
                );
                assert_eq!(
                    trade.parse_price().unwrap(),
                    Decimal::from_str_exact("7245.3").unwrap()
                );
                assert_eq!(ts, Some(1574694478900));
            }
            other => panic!("expected Trade channel frame, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_depth_update_fixture() {
        let json = load_fixture("depth_update.json");
        let frame = RawFrame::parse(&json).expect("failed to parse depth_update.json");
        match frame {
            RawFrame::Channel(RawChannelFrame {
                chan_id,
                payload: RawPayload::BookUpdate(level),
                ts,
            }) => {
                assert_eq!(chan_id, 266343);
                assert_eq!(level.price_str(), "41698");
                assert_eq!(level.count, 2);
                assert_eq!(level.amount_str(), "0.61");
                assert_eq!(level.parse_price().unwrap(), Decimal::from(41698));
                assert_eq!(
                    level.parse_amount().unwrap(),
                    Decimal::from_str_exact("0.61").unwrap()
                );
                assert_eq!(ts, Some(1614604253700));
            }
            other => panic!("expected BookUpdate channel frame, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_depth_update_remove_fixture() {
        let json = load_fixture("depth_update_remove.json");
        let frame = RawFrame::parse(&json).expect("failed to parse depth_update_remove.json");
        match frame {
            RawFrame::Channel(RawChannelFrame {
                chan_id,
                payload: RawPayload::BookUpdate(level),
                ts,
            }) => {
                assert_eq!(chan_id, 266343);
                assert_eq!(level.price_str(), "41697");
                assert_eq!(level.count, 0);
                assert_eq!(level.amount_str(), "1");
                assert_eq!(ts, Some(1614604253800));
            }
            other => panic!("expected BookUpdate remove channel frame, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_depth_update_scientific_fixture() {
        let json = load_fixture("depth_update_scientific.json");
        let frame = RawFrame::parse(&json).expect("failed to parse depth_update_scientific.json");
        match frame {
            RawFrame::Channel(RawChannelFrame {
                chan_id,
                payload: RawPayload::BookUpdate(level),
                ts,
            }) => {
                assert_eq!(chan_id, 266343);
                assert_eq!(level.price_str(), "0.030291");
                assert_eq!(level.count, 1);
                assert_eq!(level.amount_str(), "1e-8");
                assert_eq!(
                    level.parse_amount().unwrap(),
                    Decimal::from_str_exact("0.00000001").unwrap()
                );
                assert_eq!(ts, Some(1614604253850));
            }
            other => panic!("expected BookUpdate scientific channel frame, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_checksum_fixture() {
        let json = load_fixture("checksum.json");
        let frame = RawFrame::parse(&json).expect("failed to parse checksum.json");
        match frame {
            RawFrame::Channel(RawChannelFrame {
                chan_id,
                payload: RawPayload::Checksum(cs),
                ts,
            }) => {
                assert_eq!(chan_id, 266343);
                assert_eq!(cs, -1768345891);
                assert_eq!(ts, Some(1614604253900));
            }
            other => panic!("expected Checksum channel frame, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_order_book_snapshot_fixture() {
        let json = load_fixture("order_book_snapshot.json");
        let frame = RawFrame::parse(&json).expect("failed to parse order_book_snapshot.json");
        match frame {
            RawFrame::Channel(RawChannelFrame {
                chan_id,
                payload: RawPayload::BookSnapshot(levels),
                ts,
            }) => {
                assert_eq!(chan_id, 266343);
                assert_eq!(levels.len(), 4);
                assert_eq!(levels[0].price_str(), "41698");
                assert_eq!(levels[0].count, 3);
                assert_eq!(levels[0].amount_str(), "0.71");
                assert_eq!(levels[2].price_str(), "41699");
                assert_eq!(levels[2].count, 2);
                assert_eq!(levels[2].amount_str(), "-0.5");
                assert_eq!(ts, Some(1614604253634));
            }
            other => panic!("expected BookSnapshot channel frame, got {other:?}"),
        }
    }

    #[test]
    fn test_scratch_buffer_retains_capacity() {
        let mut scratch = BitfinexConnectionScratch::new();
        assert!(scratch.levels.capacity() >= 250);

        let json = load_fixture("order_book_snapshot.json");
        let frame = RawFrame::parse(&json).unwrap();
        if let RawFrame::Channel(RawChannelFrame {
            payload: RawPayload::BookSnapshot(levels),
            ..
        }) = frame
        {
            for l in levels {
                scratch
                    .levels
                    .push(RawBookLevelOwned::from_raw(&l).unwrap());
            }
        }
        assert_eq!(scratch.levels.len(), 4);
        scratch.clear();
        assert_eq!(scratch.levels.len(), 0);
        assert!(scratch.levels.capacity() >= 250);
    }
}
