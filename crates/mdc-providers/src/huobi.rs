//! Huobi (HTX) Market-By-Price WebSocket market data connector.
//!
//! # Overview
//! Connects to Huobi (HTX) WebSocket API (`wss://api.huobi.pro/feed`), ingesting
//! gzip-compressed binary WebSocket frames, answering server keepalive pings (`{"ping": ts}`),
//! requesting order book snapshots in-band over WebSocket (`req`), and tracking
//! strict Market-By-Price incremental sequence continuity (`prevSeqNum` + 1 ..= `seqNum`).
//!
//! - **WebSocket Base URL**: `wss://api.huobi.pro/feed` (configurable)
//! - **Limits**: Up to 25 symbols per connection bucket (default).
//! - **Integrity**: Strict `prevSeqNum` + 1 ..= `seqNum` continuity anchored by in-band
//!   WebSocket snapshots (`req` -> `rep`). Gaps trigger `IntegrityAlert::GapDetected`
//!   and serialized snapshot reconciliation.
//! - **Keepalive**: Application-level `{"ping": ts}` answered immediately with `{"pong": ts}`.
//! - **Decompression**: Zero-allocation gzip decompression reusing a scratch buffer across frames.
//! - **Credentials**: None required (public market data feeds).
//!
//! # Example
//! ```rust
//! use std::sync::Arc;
//! use mdc_core::{Symbol, VecSink};
//! use mdc_providers::huobi::{HuobiConfig, HuobiConnectorBuilder};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = HuobiConfig::default();
//! let symbols = vec![Symbol::from("btcusdt")];
//! let sink = Arc::new(VecSink::new());
//! let (connector, handle) = HuobiConnectorBuilder::new(config)
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
pub mod gzip;
pub mod normalize;
pub mod raw;

pub use config::{ConfigError, HuobiConfig, HuobiConfigBuilder, HuobiStreams};
pub use connection::run_connection_task;
pub use connector::{HuobiConnector, HuobiConnectorBuilder};
pub use control::{Command, ConnectorHandle, ControlError};
pub use fsm::SymbolFsm;
pub use gzip::{GzipDecoder, GzipError};
pub use normalize::{normalize_delta, normalize_snapshot, normalize_trade};
pub use raw::{
    HuobiConnectionScratch, HuobiDepthFrameHeader, HuobiDepthFrameSeed, HuobiMessageKind,
    LevelsSeed, MbpHeader, MbpSeed, RawEnvelope, RawMbp, RawTradeId, RawTradeItem, RawTradeTick,
    build_pong_frame, build_req_snapshot_frame, build_sub_mbp_frame, build_sub_trades_frame,
    build_unsub_mbp_frame, extract_levels_from_channel, extract_symbol_from_channel,
};
