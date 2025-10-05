#![cfg(feature = "kraken")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Property-based state machine testing for Kraken per-symbol synchronization FSM.

use std::time::{Duration, Instant};

use mdc_providers::kraken::fsm::{
    PollOutcome, SnapshotOutcome, SymbolFsm, SyncState, UpdateOutcome,
};
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};

const RESYNC_TIMEOUT_MS: u64 = 5_000;
const QUARANTINE_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefSyncState {
    AwaitingSnapshot { started_at: u64 },
    InSync,
    Quarantined { until: u64 },
}

#[derive(Debug, Clone)]
pub struct RefModel {
    pub current_time_ms: u64,
    pub state: RefSyncState,
    pub resubscribe_count: usize,
    pub delta_count: usize,
}

#[derive(Debug, Clone)]
pub enum FsmTransition {
    Snapshot { valid: bool },
    Update { valid: bool },
    Tick(u64), // dt in ms
    Disconnect,
}

pub struct FsmStateMachineTest;

impl ReferenceStateMachine for FsmStateMachineTest {
    type State = RefModel;
    type Transition = FsmTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RefModel {
            current_time_ms: 0,
            state: RefSyncState::AwaitingSnapshot { started_at: 0 },
            resubscribe_count: 0,
            delta_count: 0,
        })
        .boxed()
    }

    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        prop_oneof![
            // Snapshot arrival (80% valid, 20% bad checksum)
            prop::bool::weighted(0.8).prop_map(|valid| FsmTransition::Snapshot { valid }),
            // Update arrival (90% valid, 10% bad checksum)
            prop::bool::weighted(0.9).prop_map(|valid| FsmTransition::Update { valid }),
            // Time ticks: small steps (100ms..2s), or step crossing 5s or 30s
            prop_oneof![
                (100u64..2_000).prop_map(FsmTransition::Tick),
                (5_000u64..6_000).prop_map(FsmTransition::Tick),
                (30_000u64..35_000).prop_map(FsmTransition::Tick),
            ],
            // Connection disconnect / reset
            Just(FsmTransition::Disconnect),
        ]
        .boxed()
    }

    fn apply(mut model: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            FsmTransition::Snapshot { valid } => {
                if *valid {
                    // Valid snapshot accepted from ANY state, including quarantine
                    model.state = RefSyncState::InSync;
                }
            }
            FsmTransition::Update { valid } => {
                if matches!(model.state, RefSyncState::InSync) {
                    if *valid {
                        model.delta_count += 1;
                    } else {
                        // Checksum mismatch triggers resync
                        model.state = RefSyncState::AwaitingSnapshot {
                            started_at: model.current_time_ms,
                        };
                        model.resubscribe_count += 1;
                    }
                }
            }
            FsmTransition::Tick(dt) => {
                model.current_time_ms += dt;
                match model.state {
                    RefSyncState::AwaitingSnapshot { started_at } => {
                        if model.current_time_ms >= started_at + RESYNC_TIMEOUT_MS {
                            model.state = RefSyncState::Quarantined {
                                until: model.current_time_ms + QUARANTINE_MS,
                            };
                        }
                    }
                    RefSyncState::Quarantined { until } => {
                        if model.current_time_ms >= until {
                            model.state = RefSyncState::AwaitingSnapshot {
                                started_at: model.current_time_ms,
                            };
                            model.resubscribe_count += 1;
                        }
                    }
                    RefSyncState::InSync => {}
                }
            }
            FsmTransition::Disconnect => {
                model.state = RefSyncState::AwaitingSnapshot {
                    started_at: model.current_time_ms,
                };
            }
        }
        model
    }
}

pub struct Sut {
    pub base_instant: Instant,
    pub current_time_ms: u64,
    pub fsm: SymbolFsm,
    pub delta_count: usize,
    pub resubscribe_count: usize,
}

impl StateMachineTest for FsmStateMachineTest {
    type SystemUnderTest = Sut;
    type Reference = Self;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        let base_instant = Instant::now();
        let fsm = SymbolFsm::new(
            base_instant,
            Duration::from_millis(RESYNC_TIMEOUT_MS),
            Duration::from_millis(QUARANTINE_MS),
        );
        Sut {
            base_instant,
            current_time_ms: ref_state.current_time_ms,
            fsm,
            delta_count: 0,
            resubscribe_count: 0,
        }
    }

    fn apply(
        mut sut: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        let now = sut.base_instant + Duration::from_millis(sut.current_time_ms);
        match transition {
            FsmTransition::Snapshot { valid } => {
                let outcome = sut.fsm.on_snapshot(valid);
                if valid {
                    assert_eq!(outcome, SnapshotOutcome::Accepted);
                } else {
                    assert_eq!(outcome, SnapshotOutcome::RejectedBadChecksum);
                }
            }
            FsmTransition::Update { valid } => {
                let expected = 12345;
                let calculated = if valid { 12345 } else { 54321 };
                let outcome = sut.fsm.on_update(valid, expected, calculated, now);
                match outcome {
                    UpdateOutcome::Accepted => {
                        // Invariant 1: No delta emitted outside InSync
                        assert!(
                            sut.fsm.is_in_sync(),
                            "Invariant violated: Delta accepted outside InSync!"
                        );
                        sut.delta_count += 1;
                    }
                    UpdateOutcome::MismatchTriggerResync { .. } => {
                        sut.resubscribe_count += 1;
                    }
                    UpdateOutcome::DroppedNotSynchronized => {
                        assert!(
                            !sut.fsm.is_in_sync(),
                            "Invariant violated: Dropped update while InSync!"
                        );
                    }
                }
            }
            FsmTransition::Tick(dt) => {
                sut.current_time_ms += dt;
                let tick_now = sut.base_instant + Duration::from_millis(sut.current_time_ms);
                match sut.fsm.poll(tick_now) {
                    PollOutcome::TimeoutQuarantined { .. } => {
                        assert!(
                            matches!(sut.fsm.state(), SyncState::Quarantined { .. }),
                            "Poll returned TimeoutQuarantined but state is not Quarantined"
                        );
                    }
                    PollOutcome::QuarantineExpiredTriggerResubscribe => {
                        sut.resubscribe_count += 1;
                    }
                    PollOutcome::None => {}
                }
            }
            FsmTransition::Disconnect => {
                sut.fsm.reset(now);
            }
        }

        // Compare state and counts with reference model
        assert_eq!(
            sut.delta_count, ref_state.delta_count,
            "Delta count mismatch: SUT={}, Ref={}",
            sut.delta_count, ref_state.delta_count
        );
        assert_eq!(
            sut.resubscribe_count, ref_state.resubscribe_count,
            "Resubscribe count mismatch: SUT={}, Ref={}",
            sut.resubscribe_count, ref_state.resubscribe_count
        );

        sut
    }

    fn check_invariants(
        sut: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        match (&ref_state.state, sut.fsm.state()) {
            (RefSyncState::InSync, SyncState::InSync) => {}
            (RefSyncState::AwaitingSnapshot { .. }, SyncState::AwaitingSnapshot { .. }) => {}
            (RefSyncState::Quarantined { .. }, SyncState::Quarantined { .. }) => {}
            (expected, actual) => {
                panic!(
                    "State mismatch between Ref ({:?}) and SUT ({:?})",
                    expected, actual
                );
            }
        }
    }
}

prop_state_machine! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_kraken_fsm_state_machine(sequential 1..50 => FsmStateMachineTest);
}
