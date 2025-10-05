//! Coinbase Exchange WebSocket market data connector.
//!
//! # Overview
//! Connects to Coinbase Exchange (formerly GDAX) WebSocket feed, ingesting batched level 2 order
//! book updates (`level2_batch`), periodic heartbeats with product sequence numbers (`heartbeat`),
//! and optional public match executions (`matches`).
//!
//! Because `level2_batch` carries no per-message sequence numbers, book integrity is monitored via
//! a timer-driven `HeartbeatMonitor`: sequence jumps above threshold or silent intervals trigger an
//! in-place resubscription of the product's `level2_batch` channel, yielding a fresh order book snapshot.
//!
//! - **WebSocket Base URL**: `wss://ws-feed.exchange.coinbase.com`
//! - **Limits**: Up to 25 products per connection (default).
//! - **Integrity**: Heartbeat timeout (default 5s) and sequence gap threshold (default 3,000).
//! - **Resync**: Per-product in-place resubscription (`unsubscribe` + `subscribe` for `level2_batch`).
//! - **Credentials**: None required for public `level2_batch`, `heartbeat`, and `matches` channels.

pub mod normalize;
pub mod raw;

pub use normalize::{normalize_delta, normalize_snapshot, normalize_trade, parse_rfc3339_nanos};
pub use raw::{
    CoinbaseChangesSeed, CoinbaseConnectionScratch, CoinbaseFrameHeader, CoinbaseFrameSeed,
    CoinbaseLevelsSeed, RawChannelSubscription, RawMessage,
};
