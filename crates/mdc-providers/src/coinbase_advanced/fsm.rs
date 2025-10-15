//! State machine coordinating connection-wide sequence tracking and per-product sync state for Coinbase Advanced Trade.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use mdc_core::{GlobalSequenceCheck, GlobalSequenceTracker, IntegrityAlert, Symbol, SymbolTag};

/// Order book synchronization state of an individual product.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductSyncState {
    /// Awaiting authoritative Level 2 snapshot; updates are suppressed.
    AwaitingSnapshot,
    /// Synchronized; incremental Level 2 updates are emitted.
    InSync,
}

/// Schedule for sending resubscribe messages after a sequence gap or resync trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResyncSchedule {
    /// Resubscribe immediately (cooldown elapsed or first trigger).
    Immediate,
    /// Cooldown active; resubscribe should be scheduled after `delay`.
    Delayed(Duration),
}

impl ResyncSchedule {
    /// Returns true if resubscribe should be sent immediately.
    pub fn is_immediate(&self) -> bool {
        matches!(self, Self::Immediate)
    }

    /// Returns the delay before resubscribe should be attempted, if delayed.
    pub fn delay(&self) -> Option<Duration> {
        match self {
            Self::Immediate => None,
            Self::Delayed(d) => Some(*d),
        }
    }
}

/// State machine tracking connection-wide sequence and per-product order book synchronization.
#[derive(Debug)]
pub struct CoinbaseAdvancedFsm {
    tracker: GlobalSequenceTracker,
    products: HashMap<Symbol, (SymbolTag, ProductSyncState)>,
    last_resync_all: Option<Instant>,
    cooldown: Duration,
}

impl CoinbaseAdvancedFsm {
    /// Creates a new FSM with the given symbols and resync cooldown.
    pub fn new(symbols: Vec<(Symbol, SymbolTag)>, cooldown: Duration) -> Self {
        let mut products = HashMap::with_capacity(symbols.len());
        for (sym, tag) in symbols {
            products.insert(sym, (tag, ProductSyncState::AwaitingSnapshot));
        }

        Self {
            tracker: GlobalSequenceTracker::new(),
            products,
            last_resync_all: None,
            cooldown,
        }
    }

    /// Resets the FSM on connection establishment or disconnect.
    pub fn reset(&mut self) {
        self.tracker.reset();
        self.last_resync_all = None;
        for (_, state) in self.products.values_mut() {
            *state = ProductSyncState::AwaitingSnapshot;
        }
    }

    /// Records that a connection-wide resync was dispatched (updating cooldown timer).
    pub fn mark_resync_all(&mut self, now: Instant) {
        self.last_resync_all = Some(now);
    }

    /// Checks an incoming envelope sequence number against the connection-wide tracker.
    pub fn check_sequence(&mut self, seq: u64) -> GlobalSequenceCheck {
        self.tracker.check(seq)
    }

    /// Returns `true` if the given product is currently in sync.
    pub fn is_in_sync(&self, symbol: &Symbol) -> bool {
        self.products
            .get(symbol)
            .map(|(_, state)| *state == ProductSyncState::InSync)
            .unwrap_or(false)
    }

    /// Returns the symbol tag for a product, if registered.
    pub fn tag(&self, symbol: &Symbol) -> Option<SymbolTag> {
        self.products.get(symbol).map(|(tag, _)| *tag)
    }

    /// Returns all registered symbols.
    pub fn symbols(&self) -> impl Iterator<Item = &Symbol> {
        self.products.keys()
    }

    /// Marks a product as synchronized upon receipt of a Level 2 snapshot.
    ///
    /// Transitions product to [`ProductSyncState::InSync`] and returns [`IntegrityAlert::ResyncCompleted`].
    pub fn on_snapshot(&mut self, symbol: &Symbol) -> Option<IntegrityAlert> {
        if let Some((_, state)) = self.products.get_mut(symbol) {
            *state = ProductSyncState::InSync;
            Some(IntegrityAlert::ResyncCompleted {
                last_update_id: None,
            })
        } else {
            None
        }
    }

    /// Handles a detected connection-wide sequence gap.
    ///
    /// Returns `(schedule, alerts)` where:
    /// - `schedule`: `Immediate` if cooldown permits sending a resubscribe message, or `Delayed(Duration)` with retry delay.
    /// - `alerts`: pairs of `(GapDetected, ResyncStarted)` for every product on the connection.
    pub fn on_gap(
        &mut self,
        expected: u64,
        received: u64,
        now: Instant,
    ) -> (
        ResyncSchedule,
        Vec<(Symbol, SymbolTag, [IntegrityAlert; 2])>,
    ) {
        let schedule = match self.last_resync_all {
            Some(last) => {
                let elapsed = now.duration_since(last);
                if elapsed >= self.cooldown {
                    self.last_resync_all = Some(now);
                    ResyncSchedule::Immediate
                } else {
                    ResyncSchedule::Delayed(self.cooldown.saturating_sub(elapsed))
                }
            }
            None => {
                self.last_resync_all = Some(now);
                ResyncSchedule::Immediate
            }
        };

        let mut alerts = Vec::with_capacity(self.products.len());
        for (sym, (tag, state)) in &mut self.products {
            *state = ProductSyncState::AwaitingSnapshot;
            alerts.push((
                sym.clone(),
                *tag,
                [
                    IntegrityAlert::GapDetected { expected, received },
                    IntegrityAlert::ResyncStarted,
                ],
            ));
        }

        (schedule, alerts)
    }

    /// Triggers a connection-wide resynchronization.
    pub fn trigger_resync_all(
        &mut self,
        now: Instant,
    ) -> (ResyncSchedule, Vec<(Symbol, SymbolTag, IntegrityAlert)>) {
        let schedule = match self.last_resync_all {
            Some(last) => {
                let elapsed = now.duration_since(last);
                if elapsed >= self.cooldown {
                    self.last_resync_all = Some(now);
                    ResyncSchedule::Immediate
                } else {
                    ResyncSchedule::Delayed(self.cooldown.saturating_sub(elapsed))
                }
            }
            None => {
                self.last_resync_all = Some(now);
                ResyncSchedule::Immediate
            }
        };

        let mut alerts = Vec::with_capacity(self.products.len());
        for (sym, (tag, state)) in &mut self.products {
            *state = ProductSyncState::AwaitingSnapshot;
            alerts.push((sym.clone(), *tag, IntegrityAlert::ResyncStarted));
        }

        (schedule, alerts)
    }

    /// Triggers manual resynchronization for a single product.
    pub fn trigger_manual_resync(
        &mut self,
        symbol: &Symbol,
    ) -> Option<(SymbolTag, IntegrityAlert)> {
        if let Some((tag, state)) = self.products.get_mut(symbol) {
            *state = ProductSyncState::AwaitingSnapshot;
            Some((*tag, IntegrityAlert::ResyncStarted))
        } else {
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_fsm_initial_state_awaiting_snapshot() {
        let sym = Symbol::from("BTC-USD");
        let fsm = CoinbaseAdvancedFsm::new(
            vec![(sym.clone(), SymbolTag::default())],
            Duration::from_secs(5),
        );

        assert!(!fsm.is_in_sync(&sym));
        assert_eq!(fsm.tag(&sym), Some(SymbolTag::default()));
    }

    #[test]
    fn test_fsm_snapshot_transitions_to_insync() {
        let sym = Symbol::from("BTC-USD");
        let mut fsm = CoinbaseAdvancedFsm::new(
            vec![(sym.clone(), SymbolTag::default())],
            Duration::from_secs(5),
        );

        let alert = fsm.on_snapshot(&sym).expect("alert on snapshot");
        assert_eq!(
            alert,
            IntegrityAlert::ResyncCompleted {
                last_update_id: None
            }
        );
        assert!(fsm.is_in_sync(&sym));
    }

    #[test]
    fn test_fsm_gap_invalidates_all_products() {
        let sym1 = Symbol::from("BTC-USD");
        let sym2 = Symbol::from("ETH-USD");
        let mut fsm = CoinbaseAdvancedFsm::new(
            vec![
                (sym1.clone(), SymbolTag::default()),
                (sym2.clone(), SymbolTag::default()),
            ],
            Duration::from_secs(5),
        );

        // Transition both to InSync
        fsm.on_snapshot(&sym1);
        fsm.on_snapshot(&sym2);
        assert!(fsm.is_in_sync(&sym1));
        assert!(fsm.is_in_sync(&sym2));

        // Inject gap: expected 1, received 3
        let now = Instant::now();
        let (should_send, alerts) = fsm.on_gap(1, 3, now);
        assert!(should_send.is_immediate());
        assert_eq!(alerts.len(), 2);
        assert!(!fsm.is_in_sync(&sym1));
        assert!(!fsm.is_in_sync(&sym2));

        // Cooldown prevents immediate resubscribe repeat and returns remaining delay
        let (should_repeat, _) = fsm.on_gap(4, 6, now);
        assert!(!should_repeat.is_immediate());
        assert!(should_repeat.delay().is_some());

        // reset() clears cooldown so subsequent gap is immediate
        fsm.reset();
        let (should_after_reset, _) = fsm.on_gap(7, 9, now);
        assert!(should_after_reset.is_immediate());
    }
}
