//! Binance Spot market data connector.
//!
//! # Overview
//! Connects to Binance WebSocket combined streams and REST depth snapshot API,
//! implementing deterministic snapshot reconciliation, silence watchdog monitoring,
//! and rate-limit embargo handling.
//!
//! - **WebSocket Base URL**: `wss://stream.binance.com:443`
//! - **REST Base URL**: `https://api.binance.com`
//! - **Limits**: Up to 25 symbols per connection bucket (configurable; hard limit 1,024 streams per socket).
//! - **Integrity**: Strict sequence verification on depth updates ($[U, u]$ contiguous with previous $u$).
//!   Out-of-order deltas or gaps trigger an in-band `IntegrityAlert::GapDetected` followed by snapshot
//!   resynchronization.
//! - **Credentials**: None required (public market data feeds).
//!
//! # Example
//! ```rust
//! use std::sync::Arc;
//! use mdc_core::{Symbol, VecSink};
//! use mdc_providers::binance::{BinanceConfig, BinanceConnectorBuilder};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = BinanceConfig::default();
//! let symbols = vec![Symbol::from("BTCUSDT")];
//! let sink = Arc::new(VecSink::new());
//! let (connector, handle) = BinanceConnectorBuilder::new(config)
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
pub mod rest;

pub use config::{BinanceConfig, BinanceStreams};
pub use connection::{build_combined_stream_url, run_connection_task};
pub use connector::{BinanceConnector, BinanceConnectorBuilder};
pub use control::{Command, ConnectorHandle, ControlError};
pub use normalize::{normalize_book_ticker, normalize_depth, normalize_snapshot, normalize_trade};
pub use raw::{
    CombinedStream, ConnectionScratchBuffers, LevelsSeed, RawBookTicker, RawDepthSnapshot,
    RawDepthUpdate, RawPayload, RawTrade,
};
pub use rest::{BinanceRestClient, RestError};
