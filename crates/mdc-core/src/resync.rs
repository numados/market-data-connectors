//! Order book resynchronization and reconciliation state machine.

pub mod controller;
pub mod policy;

pub use controller::{
    BufferOverflow, DeltaDecision, ResyncController, ResyncError, SnapshotOutcome, SyncState,
};
pub use policy::{
    CooldownPolicy, ResyncDecision, ResyncFailure, ResyncPolicy, TimeoutQuarantinePolicy,
};
