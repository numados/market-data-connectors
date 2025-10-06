//! Bitstamp WebSocket v2 and REST market data connector.
//!
//! # Overview
//! Connects to Bitstamp WebSocket v2 API (`wss://ws.bitstamp.net`), ingesting
//! microsecond timestamped order book incremental diffs (`diff_order_book_{pair}`)
//! and reconciling against REST snapshots (`GET /api/v2/order_book/{pair}/`) using
//! strict microtimestamp gating ($> T$).
//!
//! - **WebSocket Base URL**: `wss://ws.bitstamp.net` (configurable)
//! - **REST Base URL**: `https://www.bitstamp.net/api/v2` (configurable)
//! - **Limits**: Default 25 symbols per connection bucket; REST rate limit 400 requests / 10s.
//! - **Integrity**: Microsecond timestamp gating against REST order book snapshots. Diffs arriving
//!   before a snapshot are dropped and trigger a snapshot; diffs with `microtimestamp <= T` are dropped.
//! - **Keepalive**: Connection watchdog and venue `bts:request_reconnect` handling.
//! - **Credentials**: None required (public market data feeds).
//!
//! # Example
//! ```rust
//! use std::sync::Arc;
//! use mdc_core::{Symbol, VecSink};
//! use mdc_providers::bitstamp::{BitstampConfig, BitstampConnectorBuilder};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = BitstampConfig::default();
//! let symbols = vec![Symbol::from("btcusd")];
//! let sink = Arc::new(VecSink::new());
//! let (connector, handle) = BitstampConnectorBuilder::new(config)
//!     .symbols(symbols)
//!     .sink(sink)
//!     .build()?;
//! # Ok(())
//! # }
//! ```

pub mod bench_support;
pub mod config;
pub mod connection;
pub mod connector;
pub mod control;
pub mod fsm;
pub mod normalize;
pub mod raw;
pub mod rest;

pub use config::{BitstampConfig, BitstampStreams, ConfigError};
pub use connection::{
    build_diff_order_book_channel, build_live_trades_channel, build_subscribe_frame,
    run_connection_task,
};
pub use connector::{BitstampConnector, BitstampConnectorBuilder};
pub use control::{Command, ConnectorHandle, ControlError};
pub use fsm::{DiffDecision, SymbolFsm, SyncState};
pub use normalize::{normalize_depth, normalize_snapshot, normalize_trade};
pub use raw::{
    BitstampConnectionScratch, BitstampDiffFrameHeader, BitstampDiffFrameSeed, LevelsSeed,
    ParseTimestampError, RawDiffData, RawEnvelope, RawOrderBookSnapshot, RawTrade, SubscribeData,
    SubscribeMessage, extract_symbol, parse_microtimestamp, u64_from_str,
};
pub use rest::{BitstampRestClient, RestError};
