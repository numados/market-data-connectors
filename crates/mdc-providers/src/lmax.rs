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

pub mod config;
pub mod normalize;
pub mod raw;

pub use config::{ConfigError, ControlPacing, LmaxConfig, LmaxStreams};
pub use normalize::{normalize_book, normalize_trade, parse_rfc3339_nanos};
pub use raw::{
    LmaxBookFrameSeed, LmaxBookHeader, LmaxConnectionScratch, LmaxFrameKind, LmaxLevelsSeed,
    RawControl, RawSubscribedChannel, RawTrade, SubscribeChannel, SubscribeMessage,
    peek_frame_kind,
};
