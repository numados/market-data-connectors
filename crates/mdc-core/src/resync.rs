//! Order book resynchronization and reconciliation state machine.

pub mod controller;

pub use controller::{
    BufferOverflow, DeltaDecision, ResyncController, ResyncError, SnapshotOutcome, SyncState,
};
