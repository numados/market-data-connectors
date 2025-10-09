//! Core abstractions and domain models for market data connectors.

#![forbid(unsafe_code)]

pub mod catalog;
pub mod error;
pub mod integrity;
pub mod model;
pub mod rest_quote;
pub mod resync;
pub mod sink;

pub use catalog::*;
pub use error::MdcError;
pub use integrity::{
    BitfinexChecksum, ChecksumStrategy, EMBARGO_FALLBACK_418, EMBARGO_FALLBACK_429,
    EMBARGO_SAFETY_CAP, EmbargoTracker, GlobalSequenceCheck, GlobalSequenceTracker, KrakenChecksum,
    NoChecksum, RawLevelRef, SequenceCheck, SequenceTracker, SilenceWatchdog, SnapshotPermit,
    WatchdogAction, clean_token, parse_retry_after, remove_decimal_and_leading_zeros,
};
pub use model::*;
pub use rest_quote::*;
pub use resync::{
    BufferOverflow, CooldownPolicy, DeltaDecision, ResyncController, ResyncDecision, ResyncError,
    ResyncFailure, ResyncPolicy, SnapshotOutcome, SyncState, TimeoutQuarantinePolicy,
};
pub use sink::{
    BookKind, EventContext, FnSink, MarketDataSink, Owned, RawLevelText, StreamingSink, VecSink,
};
