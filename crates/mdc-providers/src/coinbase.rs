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
//!
//! # Example
//! ```rust
//! use std::sync::Arc;
//! use mdc_core::{Symbol, VecSink};
//! use mdc_providers::coinbase::{CoinbaseConfig, CoinbaseConnectorBuilder};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = CoinbaseConfig::default();
//! let symbols = vec![Symbol::from("BTC-USD")];
//! let sink = Arc::new(VecSink::new());
//! let (connector, handle) = CoinbaseConnectorBuilder::new(config)
//!     .symbols(symbols)
//!     .sink(sink)
//!     .build()?;
//! # Ok(())
//! # }
//! ```

pub mod bench_support;
pub mod catalog;
pub mod config;
pub mod connection;
pub mod connector;
pub mod control;
pub mod fsm;
pub mod heartbeat;
pub mod normalize;
pub mod raw;

pub use catalog::{
    DEFAULT_COINBASE_CATALOG_ENDPOINT, coinbase_catalog_client, parse_coinbase_catalog,
};
pub use config::{CoinbaseConfig, CoinbaseStreams};
pub use connection::{
    build_single_subscribe_payload, build_subscribe_payload, build_unsubscribe_payload,
    run_connection_task,
};
pub use connector::{COINBASE_CAPABILITIES, CoinbaseConnector, CoinbaseConnectorBuilder};
pub use control::{Command, ConnectorHandle, ControlError};
pub use fsm::{PollOutcome, SnapshotOutcome, SymbolFsm, SyncState, UpdateOutcome};
pub use heartbeat::{HeartbeatMonitor, HeartbeatVerdict};
pub use normalize::{normalize_delta, normalize_snapshot, normalize_trade, parse_rfc3339_nanos};
pub use raw::{
    CoinbaseChangesSeed, CoinbaseConnectionScratch, CoinbaseFrameHeader, CoinbaseFrameSeed,
    CoinbaseLevelsSeed, RawChannelSubscription, RawMessage,
};
