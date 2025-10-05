//! Kraken WebSocket v2 market data connector.
//!
//! # Overview
//! Connects to Kraken WebSocket v2 API, validating depth order book messages
//! with CRC32 checksums over the top 10 price levels, managing in-place resubscriptions
//! upon checksum mismatch or silence watchdog triggers, and fail-closed quarantine policies.
//!
//! - **WebSocket Base URL**: `wss://ws.kraken.com/v2`
//! - **Limits**: Up to 300 symbols per connection (default), chunked at 30 per subscribe frame.
//! - **Integrity**: Continuous CRC32 checksum validation over top 10 asks and bids.
//!   Checksum mismatches trigger an in-band `IntegrityAlert::ChecksumMismatch` followed by
//!   an immediate in-place resubscription.
//! - **Credentials**: None required for public `book` and `trade` channels.
//!
//! # Example
//! ```rust
//! use std::sync::Arc;
//! use mdc_core::{Symbol, VecSink};
//! use mdc_providers::kraken::{KrakenConfig, KrakenConnectorBuilder};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = KrakenConfig::default();
//! let symbols = vec![Symbol::from("BTC/USD")];
//! let sink = Arc::new(VecSink::new());
//! let (connector, handle) = KrakenConnectorBuilder::new(config)
//!     .symbols(symbols)
//!     .sink(sink)
//!     .build()?;
//! # Ok(())
//! # }
//! ```

pub mod bench_support;
pub mod checksum;
pub mod config;
pub mod connection;
pub mod connector;
pub mod control;
pub mod fsm;
pub mod normalize;
pub mod raw;

pub use checksum::{ChecksumVerdict, KrakenChecksumBook, LevelParseError, RawLevelOwned};
pub use config::{KrakenConfig, KrakenStreams};
pub use connection::{
    build_subscribe_payload, build_unsubscribe_payload, chunk_symbols, run_connection_task,
};
pub use connector::{KrakenConnector, KrakenConnectorBuilder};
pub use control::{Command, ConnectorHandle, ControlError};
pub use fsm::{PollOutcome, SnapshotOutcome, SymbolFsm, SyncState, UpdateOutcome};
pub use normalize::{normalize_delta, normalize_snapshot, normalize_trade, parse_rfc3339_nanos};
pub use raw::{
    KrakenBookDataHeader, KrakenConnectionScratch, KrakenDepthSeed, KrakenLevelsSeed, RawAckResult,
    RawBookData, RawLevel, RawMessage, RawMethodAck, RawTrade,
};
