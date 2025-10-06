//! Bitfinex WebSocket v2 market data connector and interleaved CRC32 validation.

pub mod checksum;
pub mod normalize;
pub mod raw;

pub use checksum::{BitfinexChecksumBook, ChecksumVerdict};
pub use normalize::{normalize_book_level, normalize_trade};
pub use raw::{
    BitfinexConnectionScratch, RawBookLevel, RawBookLevelOwned, RawChannelFrame, RawEvent,
    RawFrame, RawPayload, RawTradeEntry, parse_decimal_token,
};

