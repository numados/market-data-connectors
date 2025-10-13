//! Market data integrity state machines and protocol continuity tracking.

pub mod checksum;
pub mod embargo;
pub mod sequence;
pub mod watchdog;

#[cfg(test)]
pub use checksum::clean_token;
pub use checksum::{
    BitfinexChecksum, ChecksumStrategy, KrakenChecksum, NoChecksum, RawLevelRef,
    for_each_cleaned_byte, remove_decimal_and_leading_zeros,
};
pub use embargo::{
    EMBARGO_FALLBACK_418, EMBARGO_FALLBACK_429, EMBARGO_SAFETY_CAP, EmbargoTracker, SnapshotPermit,
    parse_retry_after,
};
pub use sequence::{
    DEFAULT_RESET_THRESHOLD, GlobalSequenceCheck, GlobalSequenceTracker, SequenceCheck,
    SequenceRule, SequenceTracker,
};
pub use watchdog::{SilenceWatchdog, WatchdogAction};
