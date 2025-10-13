//! Order book resynchronization controller and snapshot reconciliation state machine.

use std::collections::VecDeque;

use crate::integrity::{SequenceCheck, SequenceTracker};
use crate::model::{IntegrityAlert, L2Delta, L2Snapshot, MarketEvent, SequenceRange};

/// Synchronization state of a symbol on a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// Waiting for authoritative snapshot; incoming deltas are buffered.
    BufferingDiffs,
    /// In sync with exchange sequence; deltas can be processed directly.
    InSync,
}

/// Action to take on an incoming delta based on its sequence range alone.
///
/// Allows the in-sync hot path to evaluate and stream price levels without
/// constructing or allocating an owned [`MarketEvent<L2Delta>`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaDecision {
    /// In sync and contiguous: stream levels directly to sink (zero allocation).
    Emit,
    /// Older than current book state: drop silently without action.
    Drop,
    /// Connection is buffering: materialize owned delta and pass to [`ResyncController::buffer`].
    Buffer,
    /// Sequence gap detected: continuity broken. The caller must emit the alert,
    /// trigger resync, and request a snapshot.
    GapDetected {
        /// In-band integrity alert describing the gap.
        alert: IntegrityAlert,
    },
}

/// Indicates that the delta buffer exceeded its capacity and was purged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferOverflow {
    /// Number of buffered deltas that were dropped upon purge.
    pub dropped: usize,
}

/// Result of evaluating an authoritative REST snapshot against buffered deltas.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// Snapshot successfully reconciled with buffered deltas.
    ///
    /// The caller must emit events in exact order:
    /// 1. `snapshot`
    /// 2. Each delta in `replay`
    /// 3. [`IntegrityAlert::ResyncCompleted`]
    Synchronized {
        /// The authoritative snapshot.
        snapshot: MarketEvent<L2Snapshot>,
        /// Replayed deltas that arrived while waiting for the snapshot.
        replay: Vec<MarketEvent<L2Delta>>,
    },
    /// Snapshot is older than the buffered deltas; remain in [`SyncState::BufferingDiffs`]
    /// and re-request a snapshot after cooldown.
    StaleSnapshot {
        /// Sequence ID of the snapshot that was too old.
        snapshot_id: u64,
        /// First sequence ID present in the buffered deltas.
        first_buffered: u64,
    },
    /// Snapshot arrived while already in [`SyncState::InSync`]; late duplicate, safely ignored.
    Ignored,
}

/// Errors occurring during snapshot reconciliation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResyncError {
    /// Snapshot lacks a `last_update_id` on a sequenced feed.
    #[error("snapshot without last_update_id cannot anchor a sequenced feed")]
    UnsequencedSnapshot,
    /// Delta lacks a `sequence` range on a sequenced feed.
    #[error("delta without sequence range on a sequenced feed")]
    UnsequencedDelta,
}

/// Manages order book reconciliation and delta buffering for one symbol on one connection.
///
/// Implements the two-step evaluation flow:
/// 1. Fast range check via [`Self::check_delta`] (zero heap allocation in [`SyncState::InSync`]).
/// 2. Bounded buffering via [`Self::buffer`] when waiting for a snapshot.
/// 3. Authoritative reconciliation via [`Self::on_snapshot`].
#[derive(Debug)]
pub struct ResyncController {
    state: SyncState,
    max_buffer: usize,
    buffer: VecDeque<MarketEvent<L2Delta>>,
    tracker: SequenceTracker,
}

impl ResyncController {
    /// Creates a new resync controller starting in [`SyncState::BufferingDiffs`].
    pub fn new(max_buffer: usize) -> Self {
        Self {
            state: SyncState::BufferingDiffs,
            max_buffer,
            buffer: VecDeque::with_capacity(max_buffer.min(1024)),
            tracker: SequenceTracker::new(),
        }
    }

    /// Returns the current synchronization state.
    pub fn state(&self) -> SyncState {
        self.state
    }

    /// Returns the maximum capacity of the in-flight delta buffer.
    pub fn max_buffer(&self) -> usize {
        self.max_buffer
    }

    /// Returns the number of deltas currently held in the buffer.
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Returns a reference to the underlying [`SequenceTracker`].
    pub fn tracker(&self) -> &SequenceTracker {
        &self.tracker
    }

    /// Resets the controller state to [`SyncState::BufferingDiffs`], clearing the buffer and tracker.
    pub fn reset(&mut self) {
        self.state = SyncState::BufferingDiffs;
        self.buffer.clear();
        self.tracker.clear();
    }

    /// Step 1: Evaluates an incoming delta by its sequence range alone without heap allocation.
    pub fn check_delta(&mut self, range: SequenceRange) -> DeltaDecision {
        match self.state {
            SyncState::BufferingDiffs => DeltaDecision::Buffer,
            SyncState::InSync => match self.tracker.check(range) {
                SequenceCheck::Valid => DeltaDecision::Emit,
                SequenceCheck::OldDataIgnored => DeltaDecision::Drop,
                SequenceCheck::GapDetected { expected, received } => {
                    self.state = SyncState::BufferingDiffs;
                    self.tracker.clear();
                    self.buffer.clear();
                    DeltaDecision::GapDetected {
                        alert: IntegrityAlert::GapDetected { expected, received },
                    }
                }
                SequenceCheck::SequenceExhausted => {
                    self.state = SyncState::BufferingDiffs;
                    self.tracker.clear();
                    self.buffer.clear();
                    DeltaDecision::GapDetected {
                        alert: IntegrityAlert::GapDetected {
                            expected: u64::MAX,
                            received: range.first,
                        },
                    }
                }
                SequenceCheck::SequenceReset { current, reset_to } => {
                    self.state = SyncState::BufferingDiffs;
                    self.tracker.clear();
                    self.buffer.clear();
                    DeltaDecision::GapDetected {
                        alert: IntegrityAlert::GapDetected {
                            expected: current.saturating_add(1),
                            received: reset_to,
                        },
                    }
                }
                SequenceCheck::NotSynchronized => {
                    self.state = SyncState::BufferingDiffs;
                    DeltaDecision::Buffer
                }
            },
        }
    }

    /// Step 2: Buffers an owned delta while in [`SyncState::BufferingDiffs`].
    ///
    /// If buffer capacity is exceeded, the buffer is purged and [`BufferOverflow`]
    /// is returned in `Err`, prompting the caller to restart the snapshot request.
    pub fn buffer(&mut self, event: MarketEvent<L2Delta>) -> Result<(), BufferOverflow> {
        debug_assert_eq!(
            self.state,
            SyncState::BufferingDiffs,
            "buffer called while InSync"
        );

        if self.buffer.len() >= self.max_buffer {
            let dropped = self.buffer.len();
            self.buffer.clear();
            return Err(BufferOverflow { dropped });
        }

        self.buffer.push_back(event);
        Ok(())
    }

    /// Reconciles an authoritative REST snapshot with the buffered deltas.
    ///
    /// # Algorithm
    /// 1. If already [`SyncState::InSync`], return [`SnapshotOutcome::Ignored`].
    /// 2. Validate that `snapshot` has `last_update_id` ($L$).
    /// 3. Verify all buffered deltas have sequence ranges.
    /// 4. If the first delta after $L$ has $first > L + 1$, the snapshot is stale relative to the buffer.
    ///    Return [`SnapshotOutcome::StaleSnapshot`]; buffer remains intact.
    /// 5. Pop all deltas with $last \le L$.
    /// 6. Initialize `SequenceTracker` with $L$, replay remaining deltas through tracker,
    ///    transition to [`SyncState::InSync`], and return [`SnapshotOutcome::Synchronized`].
    pub fn on_snapshot(
        &mut self,
        snapshot: MarketEvent<L2Snapshot>,
    ) -> Result<SnapshotOutcome, ResyncError> {
        if self.state == SyncState::InSync {
            return Ok(SnapshotOutcome::Ignored);
        }

        let snapshot_id = snapshot
            .kind
            .last_update_id
            .ok_or(ResyncError::UnsequencedSnapshot)?;

        // Verify all buffered deltas are sequenced
        for delta in &self.buffer {
            if delta.kind.sequence.is_none() {
                return Err(ResyncError::UnsequencedDelta);
            }
        }

        // Find the first delta that extends past the snapshot
        let first_retained_idx = self.buffer.iter().position(|d| match d.kind.sequence {
            Some(seq) => seq.last > snapshot_id,
            None => false,
        });

        if let Some(idx) = first_retained_idx {
            if let Some(first_delta) = self.buffer.get(idx) {
                if let Some(seq) = first_delta.kind.sequence {
                    let expected_max_first = snapshot_id.saturating_add(1);
                    if seq.first > expected_max_first {
                        return Ok(SnapshotOutcome::StaleSnapshot {
                            snapshot_id,
                            first_buffered: seq.first,
                        });
                    }
                }
            }
        }

        // Drop deltas older than or equal to snapshot_id
        while let Some(front) = self.buffer.front() {
            if let Some(seq) = front.kind.sequence {
                if seq.last <= snapshot_id {
                    self.buffer.pop_front();
                    continue;
                }
            }
            break;
        }

        // Anchor tracker to snapshot
        self.tracker.reset_to(snapshot_id);

        let mut replay = Vec::with_capacity(self.buffer.len());
        while let Some(delta) = self.buffer.pop_front() {
            let seq = delta.kind.sequence.ok_or(ResyncError::UnsequencedDelta)?;
            match self.tracker.check(seq) {
                SequenceCheck::Valid => replay.push(delta),
                SequenceCheck::OldDataIgnored => {}
                SequenceCheck::GapDetected { .. }
                | SequenceCheck::NotSynchronized
                | SequenceCheck::SequenceExhausted
                | SequenceCheck::SequenceReset { .. } => {
                    // Continuity broken in buffered deltas. Discard older deltas prior to gap,
                    // keep this delta and subsequent deltas, clear tracker, and request fresh snapshot.
                    self.buffer.push_front(delta);
                    self.tracker.clear();
                    self.state = SyncState::BufferingDiffs;
                    return Ok(SnapshotOutcome::StaleSnapshot {
                        snapshot_id,
                        first_buffered: seq.first,
                    });
                }
            }
        }

        self.state = SyncState::InSync;
        Ok(SnapshotOutcome::Synchronized { snapshot, replay })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::model::{DeliveryStamp, PriceLevel, RunId, Symbol, UnixNanos, VenueId};
    use rust_decimal::Decimal;

    fn make_delta(first: u64, last: u64) -> MarketEvent<L2Delta> {
        let delta = L2Delta::new(
            vec![PriceLevel::new(Decimal::from(100), Decimal::from(1))],
            vec![PriceLevel::new(Decimal::from(101), Decimal::from(1))],
            Some(SequenceRange::new(first, last)),
        );
        MarketEvent {
            exchange_ts: Some(UnixNanos(1_000)),
            received_ts: UnixNanos(1_000),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: 0,
            delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
            source_sequence: None,
            kind: delta,
        }
    }

    fn make_snapshot(last_update_id: Option<u64>) -> MarketEvent<L2Snapshot> {
        let snapshot = L2Snapshot::new(
            vec![PriceLevel::new(Decimal::from(100), Decimal::from(10))],
            vec![PriceLevel::new(Decimal::from(101), Decimal::from(10))],
            last_update_id,
        );
        MarketEvent {
            exchange_ts: Some(UnixNanos(1_000)),
            received_ts: UnixNanos(1_000),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: 0,
            delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
            source_sequence: None,
            kind: snapshot,
        }
    }

    #[test]
    fn happy_path_reconciliation_and_transition_to_insync() {
        let mut controller = ResyncController::new(100);
        assert_eq!(controller.state(), SyncState::BufferingDiffs);

        // Delta 1 arrives while buffering
        let d1_range = SequenceRange::new(100, 105);
        assert_eq!(controller.check_delta(d1_range), DeltaDecision::Buffer);
        controller.buffer(make_delta(100, 105)).expect("buffer d1");

        // Delta 2 arrives
        let d2_range = SequenceRange::new(106, 110);
        assert_eq!(controller.check_delta(d2_range), DeltaDecision::Buffer);
        controller.buffer(make_delta(106, 110)).expect("buffer d2");

        assert_eq!(controller.buffered_count(), 2);

        // Snapshot arrives with last_update_id = 102 (overlaps d1 [100, 105])
        let snapshot = make_snapshot(Some(102));
        let outcome = controller.on_snapshot(snapshot).expect("on_snapshot");

        match outcome {
            SnapshotOutcome::Synchronized {
                snapshot: snap,
                replay,
            } => {
                assert_eq!(snap.kind.last_update_id, Some(102));
                assert_eq!(replay.len(), 2);
                assert_eq!(replay[0].kind.sequence, Some(SequenceRange::new(100, 105)));
                assert_eq!(replay[1].kind.sequence, Some(SequenceRange::new(106, 110)));
            }
            other => panic!("expected Synchronized, got {other:?}"),
        }

        assert_eq!(controller.state(), SyncState::InSync);
        assert_eq!(controller.buffered_count(), 0);
        assert_eq!(controller.tracker().current(), Some(110));

        // InSync: next contiguous delta emits without heap allocation
        assert_eq!(
            controller.check_delta(SequenceRange::new(111, 115)),
            DeltaDecision::Emit
        );
        assert_eq!(controller.tracker().current(), Some(115));

        // InSync: old delta is dropped
        assert_eq!(
            controller.check_delta(SequenceRange::new(110, 115)),
            DeltaDecision::Drop
        );

        // InSync: gap triggers alert and transitions back to BufferingDiffs
        let gap_decision = controller.check_delta(SequenceRange::new(118, 120));
        assert_eq!(
            gap_decision,
            DeltaDecision::GapDetected {
                alert: IntegrityAlert::GapDetected {
                    expected: 116,
                    received: 118
                }
            }
        );
        assert_eq!(controller.state(), SyncState::BufferingDiffs);
        assert_eq!(controller.tracker().current(), None);
    }

    #[test]
    fn stale_snapshot_rejection_leaves_buffer_intact() {
        let mut controller = ResyncController::new(100);

        // Buffered delta covers [105, 110]
        controller.buffer(make_delta(105, 110)).expect("buffer");

        // Snapshot arrives with last_update_id = 100.
        // Gap exists between 100 and 105 (first_buffered > 100 + 1)
        let stale_snap = make_snapshot(Some(100));
        let outcome = controller.on_snapshot(stale_snap).expect("on_snapshot");

        assert_eq!(
            outcome,
            SnapshotOutcome::StaleSnapshot {
                snapshot_id: 100,
                first_buffered: 105,
            }
        );
        assert_eq!(controller.state(), SyncState::BufferingDiffs);
        assert_eq!(controller.buffered_count(), 1);

        // Now authoritative snapshot arrives with last_update_id = 105
        let good_snap = make_snapshot(Some(105));
        let good_outcome = controller.on_snapshot(good_snap).expect("on_snapshot");

        match good_outcome {
            SnapshotOutcome::Synchronized { replay, .. } => {
                assert_eq!(replay.len(), 1);
            }
            other => panic!("expected Synchronized, got {other:?}"),
        }
        assert_eq!(controller.state(), SyncState::InSync);
    }

    #[test]
    fn duplicate_snapshot_in_sync_is_ignored() {
        let mut controller = ResyncController::new(100);
        let snap1 = make_snapshot(Some(100));
        let outcome1 = controller.on_snapshot(snap1).expect("snap1");
        assert!(matches!(outcome1, SnapshotOutcome::Synchronized { .. }));
        assert_eq!(controller.state(), SyncState::InSync);

        // Late duplicate snapshot
        let snap2 = make_snapshot(Some(100));
        let outcome2 = controller.on_snapshot(snap2).expect("snap2");
        assert_eq!(outcome2, SnapshotOutcome::Ignored);
    }

    #[test]
    fn buffer_overflow_purges_buffer() {
        let mut controller = ResyncController::new(3);

        controller.buffer(make_delta(1, 2)).expect("1");
        controller.buffer(make_delta(3, 4)).expect("2");
        controller.buffer(make_delta(5, 6)).expect("3");
        assert_eq!(controller.buffered_count(), 3);

        // 4th delta exceeds capacity 3 -> Err(BufferOverflow { dropped: 3 })
        let overflow = controller
            .buffer(make_delta(7, 8))
            .expect_err("should overflow");
        assert_eq!(overflow, BufferOverflow { dropped: 3 });
        assert_eq!(controller.buffered_count(), 0);
    }

    #[test]
    fn unsequenced_snapshot_returns_error() {
        let mut controller = ResyncController::new(10);
        let unsequenced_snap = make_snapshot(None);
        let err = controller
            .on_snapshot(unsequenced_snap)
            .expect_err("should fail");
        assert_eq!(err, ResyncError::UnsequencedSnapshot);
    }

    #[test]
    fn unsequenced_delta_returns_error() {
        let mut controller = ResyncController::new(10);
        let unsequenced_delta = MarketEvent {
            exchange_ts: Some(UnixNanos(1_000)),
            received_ts: UnixNanos(1_000),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: 0,
            delivery: DeliveryStamp::new(RunId::from_bytes([0; 16]), 1, 1),
            source_sequence: None,
            kind: L2Delta::new(vec![], vec![], None),
        };
        controller.buffer(unsequenced_delta).expect("buffered");

        let snap = make_snapshot(Some(100));
        let err = controller.on_snapshot(snap).expect_err("should fail");
        assert_eq!(err, ResyncError::UnsequencedDelta);
    }

    #[test]
    fn reset_clears_state_and_buffer() {
        let mut controller = ResyncController::new(10);
        controller.buffer(make_delta(1, 2)).expect("buffer");
        let snap = make_snapshot(Some(2));
        controller.on_snapshot(snap).expect("snap");
        assert_eq!(controller.state(), SyncState::InSync);

        controller.reset();
        assert_eq!(controller.state(), SyncState::BufferingDiffs);
        assert_eq!(controller.buffered_count(), 0);
        assert_eq!(controller.tracker().current(), None);
    }
}
