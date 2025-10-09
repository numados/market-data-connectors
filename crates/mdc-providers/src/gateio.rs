//! Gate.io WebSocket v4 market data connector.
//!
//! # Overview
//! Connects to Gate.io WebSocket v4 API (`wss://api.gateio.ws/ws/v4/`), ingesting
//! order book updates (`spot.obu`) with socket-delivered full snapshots (`full: true`)
//! and contiguous sequence ($U/u$) deltas, answering server-level keepalive pings (`spot.ping`),
//! and providing in-place symbol resubscription upon sequence gaps.
//!
//! - **WebSocket Base URL**: `wss://api.gateio.ws/ws/v4/` (configurable)
//! - **Limits**: Up to 25 symbols per connection bucket (default).
//! - **Integrity**: Strict $[U, u]$ sequence continuity anchored by initial in-socket
//!   order book snapshots (`full: true`). Sequence gaps trigger in-band
//!   `IntegrityAlert::GapDetected` followed by in-place resubscription (`unsubscribe` + `subscribe`).
//! - **Keepalive**: Application-level `spot.ping` from Gate.io answered immediately with `spot.pong`.
//! - **Credentials**: None required (public market data feeds).
//!
//! # Example
//! ```rust
//! use std::sync::Arc;
//! use mdc_core::{Symbol, VecSink};
//! use mdc_providers::gateio::{GateioConfig, GateioConnectorBuilder};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = GateioConfig::default();
//! let symbols = vec![Symbol::from("BTC_USDT")];
//! let sink = Arc::new(VecSink::new());
//! let (connector, handle) = GateioConnectorBuilder::new(config)
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
pub mod normalize;
pub mod raw;
pub mod rest_quote;

pub use catalog::{DEFAULT_GATEIO_CATALOG_ENDPOINT, gateio_catalog_client, parse_gateio_catalog};
pub use config::{ConfigError, GateioConfig, GateioStreams};
pub use connection::{
    build_obu_payload, build_obu_subscribe_frame, build_obu_unsubscribe_frame,
    build_trades_subscribe_frame, current_unix_secs, run_connection_task,
};
pub use connector::{GATEIO_CAPABILITIES, GateioConnector, GateioConnectorBuilder};
pub use control::{Command, ConnectorHandle, ControlError};
pub use fsm::SymbolFsm;
pub use normalize::{normalize_depth, normalize_snapshot, normalize_trade};
pub use raw::{
    GateioConnectionScratch, LevelsSeed, ObuHeader, ObuSeed, ParseTimestampError, RawEnvelope,
    RawObu, RawSubAckError, RawSubAckResult, RawTrade, RawTradeId, SpotPong, SubscribeRequest,
    extract_symbol, parse_create_time_ms,
};
pub use rest_quote::{
    DEFAULT_GATEIO_QUOTE_ENDPOINT, gateio_quote_url, gateio_rest_quote_client, parse_gateio_quote,
};
