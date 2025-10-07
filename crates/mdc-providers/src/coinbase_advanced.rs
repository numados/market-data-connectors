//! Coinbase Advanced Trade WebSocket market data connector.
//!
//! # Overview
//! Connects to Coinbase Advanced Trade WebSocket feed (`wss://advanced-trade-ws.coinbase.com`),
//! ingesting public Level 2 order book updates (`channel: "level2"` -> received as `"l2_data"`),
//! tracking connection-wide sequence numbers across all envelopes, maintaining silence watchdog
//! via continuous heartbeats (`channel: "heartbeats"`), and optionally streaming trades (`channel: "market_trades"`).
//!
//! - **WebSocket URL**: `wss://advanced-trade-ws.coinbase.com` (configurable)
//! - **Limits**: Up to 25 products per connection (default).
//! - **Integrity**: Global connection-wide sequence counter (`sequence_num` on every envelope).
//!   A gap on any message invalidates all products on the connection and triggers a connection-wide resubscription.
//! - **Keepalive**: `heartbeats` channel is subscribed automatically on every connection to keep the watchdog quiet.
//! - **Credentials**: None required for public market data channels.
//!
//! # Example
//! ```rust
//! use mdc_providers::coinbase_advanced::{CoinbaseAdvancedConfig, CoinbaseAdvancedConfigBuilder};
//!
//! let config = CoinbaseAdvancedConfig::default();
//! assert_eq!(config.symbols_per_connection, 25);
//! ```

pub mod config;
pub mod normalize;
pub mod raw;

pub use config::{
    CoinbaseAdvancedConfig, CoinbaseAdvancedConfigBuilder, CoinbaseAdvancedStreams, ConfigError,
    DEFAULT_WS_URL,
};
pub use normalize::{
    normalize_delta_from_scratch, normalize_snapshot_from_event, normalize_snapshot_from_scratch,
    normalize_trade, parse_rfc3339_nanos,
};
pub use raw::{
    CoinbaseAdvancedFrameSeed, CoinbaseAdvancedScratch, FrameHeader, L2EventKind, RawEnvelope,
    RawErrorEnvelope, RawHeartbeatEvent, RawL2Event, RawL2Update, RawSubscriptionsEvent, RawTrade,
    RawTradeEvent, build_subscribe_frame, build_unsubscribe_frame,
};
