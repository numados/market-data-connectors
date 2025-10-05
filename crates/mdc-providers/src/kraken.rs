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
//! ```rust,no_run
//! use std::sync::Arc;
//! use mdc_core::{Symbol, VecSink};
//! use mdc_providers::kraken::KrakenChecksumBook;
//!
//! let book = KrakenChecksumBook::new(1000);
//! assert_eq!(book.checksum(), 0);
//! ```

pub mod checksum;
pub mod normalize;
pub mod raw;

pub use checksum::{ChecksumVerdict, KrakenChecksumBook, LevelParseError, RawLevelOwned};
pub use normalize::{normalize_delta, normalize_snapshot, normalize_trade, parse_rfc3339_nanos};
pub use raw::{RawAckResult, RawBookData, RawLevel, RawMessage, RawMethodAck, RawTrade};
