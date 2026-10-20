//! Order book resynchronization and reconciliation state machine.

pub mod controller;
pub mod policy;

pub use controller::{
    BufferError, BufferOverflow, ConfigError, DeltaDecision, ResyncController, ResyncError,
    SnapshotOutcome, SyncState,
};
pub use policy::{
    BackoffPolicy, CooldownPolicy, EscalatingRejectionPolicy, ExponentialBackoff,
    RejectionDecision, RejectionPolicy, ResyncDecision, ResyncFailure, ResyncPolicy,
    TimeoutQuarantinePolicy,
};
