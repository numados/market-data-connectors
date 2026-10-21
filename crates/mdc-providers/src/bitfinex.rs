//! Bitfinex WebSocket v2 market data connector and interleaved CRC32 validation.
//!
//! # Overview
//! Connects to Bitfinex WebSocket v2 API (`wss://api-pub.bitfinex.com/ws/2`),
//! managing connection-scoped channel ID mappings (`chanId`), order book state with
//! raw string tokens for exact IEEE 802.3 CRC32 checksum validation over the top 25
//! bids and asks interleaved, in-place channel resubscription on mismatch, and channel
//! leak detection.
//!
//! - **WebSocket Base URL**: `wss://api-pub.bitfinex.com/ws/2` (configurable)
//! - **Limits**: Default 12 symbols per connection (up to 30 allowed by venue).
//! - **Integrity**: CRC32 checksum validation over top 25 bids (descending) and asks (ascending,
//!   formatted with negative amounts) interleaved: `bid_0:price:amount:ask_0:price:amount...`.
//!   Mismatches trigger in-band `IntegrityAlert::ChecksumMismatch` followed by in-place
//!   channel unsubscription and resubscription.
//! - **Credentials**: None required for public `book` and `trades` channels.
//!
//! # Example
//! ```rust
//! use mdc_core::model::Symbol;
//! use mdc_core::sink::{Owned, VecSink};
//! use mdc_providers::bitfinex::BitfinexConnector;
//! use tokio_util::sync::CancellationToken;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let connector = BitfinexConnector::builder()
//!     .symbol(Symbol::from("tBTCUSD"))
//!     .orderbook_depth(250)?
//!     .build()?;
//! let sink = Owned::new(VecSink::new());
//! let cancel = CancellationToken::new();
//! let (handle, join_handle) = connector.spawn(sink, cancel);
//! # Ok(())
//! # }
//! ```

pub mod bench_support;
pub mod catalog;
pub mod checksum;
pub mod config;
pub mod connection;
pub mod connector;
pub mod control;
pub mod fsm;
pub mod normalize;
pub mod raw;
pub mod rest_quote;

pub use catalog::{
    DEFAULT_BITFINEX_CATALOG_ENDPOINT, bitfinex_catalog_client, parse_bitfinex_catalog,
};
pub use checksum::{BitfinexChecksumBook, ChecksumVerdict};
pub use config::{BOOK_DEPTHS, BitfinexConfig, BitfinexStreams, ConfigError, validate_book_depth};
pub use connection::run_connection_loop;
pub use connector::{BITFINEX_CAPABILITIES, BitfinexConnector, BitfinexConnectorBuilder};
pub use control::{BitfinexHandle, Command};
pub use fsm::{ChannelFsm, PollOutcome, SnapshotOutcome, SyncState, UpdateOutcome};
pub use normalize::{normalize_book_level, normalize_trade};
pub use raw::{
    BitfinexConnectionScratch, RawBookLevel, RawBookLevelOwned, RawChannelFrame, RawEvent,
    RawFrame, RawPayload, RawTradeEntry, parse_decimal_token,
};
pub use rest_quote::{
    DEFAULT_BITFINEX_QUOTE_ENDPOINT, bitfinex_quote_url, bitfinex_rest_quote_client,
    parse_bitfinex_quote,
};
