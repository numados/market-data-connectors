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

pub mod config;
pub mod normalize;
pub mod raw;

pub use config::{ConfigError, GateioConfig, GateioStreams};
pub use normalize::{normalize_depth, normalize_snapshot, normalize_trade};
pub use raw::{
    GateioConnectionScratch, LevelsSeed, ObuHeader, ObuSeed, ParseTimestampError, RawEnvelope,
    RawObu, RawSubAckError, RawSubAckResult, RawTrade, RawTradeId, SpotPong, SubscribeRequest,
    extract_symbol, parse_create_time_ms,
};
