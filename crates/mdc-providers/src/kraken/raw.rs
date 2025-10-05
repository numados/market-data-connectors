//! Raw message schemas and borrowing deserialization for Kraken WebSocket v2.

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;

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
    /// Unhandled channel or method response without a `channel` tag.
    Other,
}

impl<'a> RawMessage<'a> {
    /// Parses a raw JSON string into a `RawMessage` borrowing directly from the input.
    pub fn parse(s: &'a str) -> Result<Self, serde_json::Error> {
        #[derive(Deserialize)]
        struct ChannelHeader<'a> {
            channel: Option<&'a str>,
        }

        let header: ChannelHeader<'a> = serde_json::from_str(s)?;
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
