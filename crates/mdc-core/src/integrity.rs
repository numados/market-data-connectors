//! Market data integrity state machines and protocol continuity tracking.

pub mod checksum;
pub mod embargo;
pub mod sequence;
pub mod watchdog;

pub use checksum::{
    ChecksumStrategy, KrakenChecksum, NoChecksum, RawLevelRef, clean_token,
    remove_decimal_and_leading_zeros,
};
pub use embargo::{
    EMBARGO_FALLBACK_418, EMBARGO_FALLBACK_429, EMBARGO_SAFETY_CAP, EmbargoTracker, SnapshotPermit,
    parse_retry_after,
};
pub use sequence::{SequenceCheck, SequenceTracker};
pub use watchdog::{SilenceWatchdog, WatchdogAction};
