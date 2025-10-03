//! Market data integrity state machines and protocol continuity tracking.

pub mod embargo;
pub mod sequence;
pub mod watchdog;

pub use embargo::{
    EMBARGO_FALLBACK_418, EMBARGO_FALLBACK_429, EMBARGO_SAFETY_CAP, EmbargoTracker, SnapshotPermit,
    parse_retry_after,
};
pub use sequence::{SequenceCheck, SequenceTracker};
pub use watchdog::{SilenceWatchdog, WatchdogAction};
