//! Binance Spot market data connector.

pub mod normalize;
pub mod raw;

pub use normalize::{normalize_book_ticker, normalize_depth, normalize_snapshot, normalize_trade};
pub use raw::{
    CombinedStream, ConnectionScratchBuffers, LevelsSeed, RawBookTicker, RawDepthSnapshot,
    RawDepthUpdate, RawPayload, RawTrade,
};
