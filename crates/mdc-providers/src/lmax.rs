//! LMAX Digital WebSocket market data connector.
//!
//! # Overview
//! Connects to LMAX Digital public market data WebSocket API (`wss://public-data-api.london-digital.lmax.com/v1/web-socket`),
//! ingesting continuous order book snapshots on every message (`ORDER_BOOK`) at high throughput
//! (~2,000 msg/s).
//!
//! - **WebSocket URL**: `wss://public-data-api.london-digital.lmax.com/v1/web-socket` (configurable)
//! - **Throughput**: ~2,000 msg/s with zero heap allocation on the streaming path.
//! - **Integrity**: Continuous full snapshots on every frame (`L2Snapshot`). Silence watchdog for disconnect detection.
//! - **Control Pacing**: 1s initial delay before subscription, paced resubscribes, and backoff on `TOO_MANY_REQUESTS`.
//! - **Credentials**: None required (public market data feeds).
//!
//! # Example
//! ```rust
//! use std::sync::Arc;
//! use mdc_core::{Symbol, VecSink};
//! use mdc_providers::lmax::{LmaxConfig, LmaxConnectorBuilder};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = LmaxConfig::default();
//! let symbols = vec![Symbol::from("btc-usd")];
//! let sink = Arc::new(VecSink::new());
//! let (connector, handle) = LmaxConnectorBuilder::new(config)
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
pub mod normalize;
pub mod raw;

pub use config::{ConfigError, ControlPacing, LmaxConfig, LmaxStreams};
pub use connection::{build_subscribe_frame, build_unsubscribe_frame, run_connection_task};
pub use connector::{LmaxConnector, LmaxConnectorBuilder};
pub use control::{Command, ConnectorHandle, ControlError};
pub use normalize::{normalize_book, normalize_trade, parse_rfc3339_nanos};
pub use raw::{
    LmaxBookFrameSeed, LmaxBookHeader, LmaxConnectionScratch, LmaxFrameKind, LmaxLevelsSeed,
    RawControl, RawSubscribedChannel, RawTrade, SubscribeChannel, SubscribeMessage,
    peek_frame_kind,
};
