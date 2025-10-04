//! Core abstractions and domain models for market data connectors.

#![forbid(unsafe_code)]

pub mod error;
pub mod integrity;
pub mod model;
pub mod resync;
pub mod sink;

pub use error::MdcError;
pub use integrity::{
    EMBARGO_FALLBACK_418, EMBARGO_FALLBACK_429, EMBARGO_SAFETY_CAP, EmbargoTracker, SequenceCheck,
    SequenceTracker, SilenceWatchdog, SnapshotPermit, WatchdogAction, parse_retry_after,
};
pub use model::*;
pub use resync::{
    BufferOverflow, CooldownPolicy, DeltaDecision, ResyncController, ResyncDecision, ResyncError,
    ResyncFailure, ResyncPolicy, SnapshotOutcome, SyncState,
};
pub use sink::{
    BookKind, EventContext, FnSink, MarketDataSink, Owned, RawLevelText, StreamingSink, VecSink,
};
