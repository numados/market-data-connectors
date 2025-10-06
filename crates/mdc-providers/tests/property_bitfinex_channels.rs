#![cfg(feature = "bitfinex")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

//! Property-based state machine testing for Bitfinex channel synchronization FSM
//! and channel-id lifecycle tracking.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use mdc_core::model::Symbol;
use mdc_providers::bitfinex::fsm::{ChannelFsm, PollOutcome, SyncState, UpdateOutcome};
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};

const SNAPSHOT_TIMEOUT_MS: u64 = 5_000;
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
    pub accepted_updates: usize,
    pub dropped_updates: usize,
}

#[derive(Debug, Clone)]
pub enum FsmTransition {
    Snapshot,
    Update,
    ChecksumMismatch,
    Tick(u64), // dt in ms
    Reset,
}

pub struct BitfinexChannelFsmTest;

impl ReferenceStateMachine for BitfinexChannelFsmTest {
    type State = RefModel;
    type Transition = FsmTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RefModel {
            current_time_ms: 0,
            state: RefSyncState::AwaitingSnapshot { started_at: 0 },
            resubscribe_count: 0,
            accepted_updates: 0,
            dropped_updates: 0,
        })
        .boxed()
    }

    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        prop_oneof![
            // Snapshot arrival
            Just(FsmTransition::Snapshot),
            // Update arrival
            Just(FsmTransition::Update),
            // Checksum mismatch
            Just(FsmTransition::ChecksumMismatch),
            // Time ticks: small steps (100ms..2s), or step crossing 5s or 30s
            prop_oneof![
                (100u64..2_000).prop_map(FsmTransition::Tick),
                (5_000u64..6_000).prop_map(FsmTransition::Tick),
                (30_000u64..35_000).prop_map(FsmTransition::Tick),
            ],
            // Connection reset
            Just(FsmTransition::Reset),
        ]
        .boxed()
    }

    fn apply(mut model: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            FsmTransition::Snapshot => {
                model.state = RefSyncState::InSync;
            }
            FsmTransition::Update => {
                if matches!(model.state, RefSyncState::InSync) {
                    model.accepted_updates += 1;
                } else {
                    model.dropped_updates += 1;
                }
            }
            FsmTransition::ChecksumMismatch => {
                if matches!(model.state, RefSyncState::InSync) {
                    model.state = RefSyncState::AwaitingSnapshot {
                        started_at: model.current_time_ms,
                    };
                    model.resubscribe_count += 1;
                }
            }
            FsmTransition::Tick(dt) => {
                model.current_time_ms += dt;
                match model.state {
                    RefSyncState::AwaitingSnapshot { started_at } => {
                        if model.current_time_ms >= started_at + SNAPSHOT_TIMEOUT_MS {
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
            FsmTransition::Reset => {
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
    pub fsm: ChannelFsm,
    pub resubscribe_count: usize,
    pub accepted_updates: usize,
    pub dropped_updates: usize,
}

impl StateMachineTest for BitfinexChannelFsmTest {
    type SystemUnderTest = Sut;
    type Reference = Self;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        let base_instant = Instant::now();
        let fsm = ChannelFsm::new(
            base_instant,
            Duration::from_millis(SNAPSHOT_TIMEOUT_MS),
            Duration::from_millis(QUARANTINE_MS),
        );
        Sut {
            base_instant,
            current_time_ms: ref_state.current_time_ms,
            fsm,
            resubscribe_count: 0,
            accepted_updates: 0,
            dropped_updates: 0,
        }
    }

    fn apply(
        mut sut: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        let now = sut.base_instant + Duration::from_millis(sut.current_time_ms);
        match transition {
            FsmTransition::Snapshot => {
                sut.fsm.on_snapshot();
            }
            FsmTransition::Update => match sut.fsm.on_update() {
                UpdateOutcome::Accepted => {
                    assert!(
                        sut.fsm.is_in_sync(),
                        "Invariant violated: Update accepted outside InSync!"
                    );
                    sut.accepted_updates += 1;
                }
                UpdateOutcome::DroppedNotSynchronized => {
                    assert!(
                        !sut.fsm.is_in_sync(),
                        "Invariant violated: Update dropped while InSync!"
                    );
                    sut.dropped_updates += 1;
                }
            },
            FsmTransition::ChecksumMismatch => {
                if sut.fsm.is_in_sync() {
                    sut.fsm.on_checksum_mismatch(now);
                    sut.resubscribe_count += 1;
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
            FsmTransition::Reset => {
                sut.fsm.reset_awaiting_snapshot(now);
            }
        }

        assert_eq!(
            sut.accepted_updates, ref_state.accepted_updates,
            "Accepted updates mismatch"
        );
        assert_eq!(
            sut.dropped_updates, ref_state.dropped_updates,
            "Dropped updates mismatch"
        );
        assert_eq!(
            sut.resubscribe_count, ref_state.resubscribe_count,
            "Resubscribe count mismatch"
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
    fn property_bitfinex_fsm_state_machine(sequential 1..50 => BitfinexChannelFsmTest);
}

// Additional property: channel re-mapping and stale channel rejection
proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn property_channel_id_remap_rejects_stale_frames(
        symbols in proptest::collection::vec("[a-z]{3,6}", 1..5),
        remaps in proptest::collection::vec(1u32..10_000, 1..10),
    ) {
        let sym_objs: Vec<Symbol> = symbols.iter().map(|s| Symbol::from(s.as_str())).collect();
        let mut chan_to_symbol: HashMap<u32, Symbol> = HashMap::new();
        let mut symbol_to_chan: HashMap<Symbol, u32> = HashMap::new();

        // Initial subscriptions
        for (i, sym) in sym_objs.iter().enumerate() {
            let chan_id = (i as u32) + 1;
            chan_to_symbol.insert(chan_id, sym.clone());
            symbol_to_chan.insert(sym.clone(), chan_id);
        }

        // Remap events: simulating resubscribe giving new channel IDs
        for (step, &new_chan_id) in remaps.iter().enumerate() {
            let target_sym = &sym_objs[step % sym_objs.len()];
            let old_chan_id = symbol_to_chan.get(target_sym).copied().unwrap();

            // Unsubscribe old
            chan_to_symbol.remove(&old_chan_id);

            // Subscribe new with distinct channel id
            let unique_chan_id = (step as u32 + 1) * 100_000 + new_chan_id;
            chan_to_symbol.insert(unique_chan_id, target_sym.clone());
            symbol_to_chan.insert(target_sym.clone(), unique_chan_id);

            // Invariant 1: Late frames on old_chan_id are dropped (unmapped)
            prop_assert!(
                !chan_to_symbol.contains_key(&old_chan_id),
                "Old channel ID must not be present in active channel map"
            );

            // Invariant 2: Current channel map size equals number of distinct symbols
            prop_assert_eq!(
                chan_to_symbol.len(),
                sym_objs.len(),
                "Active channel count must exactly match subscribed symbols"
            );

            // Invariant 3: symbol_to_chan points to unique_chan_id
            prop_assert_eq!(
                symbol_to_chan.get(target_sym),
                Some(&unique_chan_id),
                "Symbol to channel mapping must point to new channel ID"
            );
        }
    }
}
