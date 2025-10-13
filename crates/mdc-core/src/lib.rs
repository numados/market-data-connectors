//! Core abstractions and domain models for market data connectors.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod catalog;
pub mod error;
pub mod integrity;
pub mod model;
pub mod rest_quote;
pub mod resync;
pub mod sink;

pub use catalog::*;
pub use error::{ArcError, MdcError};
#[cfg(test)]
pub use integrity::clean_token;
pub use integrity::{
    BitfinexChecksum, ChecksumPolicy, ChecksumStrategy, DEFAULT_RESET_THRESHOLD,
    EMBARGO_FALLBACK_418, EMBARGO_FALLBACK_429, EMBARGO_SAFETY_CAP, EmbargoTracker,
    GlobalSequenceCheck, GlobalSequenceTracker, KrakenChecksum, NoChecksum, RawLevelRef,
    SequenceCheck, SequencePolicy, SequenceRule, SequenceTracker, SilenceWatchdog, SnapshotPermit,
    WatchdogAction, for_each_cleaned_byte, parse_retry_after, remove_decimal_and_leading_zeros,
};
pub use model::*;
pub use rest_quote::*;
pub use resync::{
    BufferError, BufferOverflow, ConfigError, CooldownPolicy, DeltaDecision, ResyncController,
    ResyncDecision, ResyncError, ResyncFailure, ResyncPolicy, SnapshotOutcome, SyncState,
    TimeoutQuarantinePolicy,
};
pub use sink::{
    BookKind, EventContext, FnSink, MarketDataSink, Owned, RawLevelText, StreamingSink, VecSink,
};
