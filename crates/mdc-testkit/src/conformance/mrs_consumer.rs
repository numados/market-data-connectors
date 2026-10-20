//! Conformance harness and acceptance assertions for Market Rate Service (MRS) consumers.
//!
//! Enforces downstream consumer contract requirements:
//! - Strictly increasing delivery ordinals within a connection generation.
//! - Loss-intolerant FIFO delivery with reserved terminal fault slot.
//! - Verified immutable trade identities and aggressor side semantics.
//! - TopOfBook quote projection withholding until `ResyncCompleted`.
//! - Older-side timestamp retention and clean unavailability on delivery fault.

use std::collections::{HashMap, HashSet};

use mdc_core::{
    ConsumerRequirements, IntegrityAlert, MarketDataKind, MarketEvent, NativeTradeIdentity,
    ProviderCapabilities, QuoteMode, SideSemantics, Symbol, SymbolTag, Trade, TradeCapability,
    UnixNanos,
};
use mdc_sinks::{DeliveryFault, LossIntolerantStream, QuoteProjection, TopOfBookProjector};
use rust_decimal::Decimal;

/// Statistics recorded by the [`MrsConsumerHarness`] during stream processing.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MrsConsumerStats {
    /// Number of ordinary market events successfully delivered.
    pub delivered_events: usize,
    /// Number of valid quotes released to the consumer.
    pub available_quotes: usize,
    /// Number of quote unavailability / withdrawal events observed.
    pub unavailable_quotes: usize,
    /// Number of terminal delivery faults observed.
    pub delivery_faults: usize,
    /// Number of verified public execution trades accepted.
    pub accepted_trades: usize,
}

/// Conformance validator simulating an MRS source ingestion worker.
pub struct MrsConsumerHarness {
    requirements: ConsumerRequirements,
    capabilities: Option<ProviderCapabilities>,
    projectors: HashMap<Symbol, TopOfBookProjector>,
    reference_rate_symbols: HashSet<Symbol>,
    seen_trades: HashMap<(Symbol, NativeTradeIdentity), Trade>,
    stale_symbols: HashSet<Symbol>,
    awaiting_recovery_snapshot: HashSet<Symbol>,
    last_ordinals: HashMap<u64, u64>,
    stats: MrsConsumerStats,
}

impl MrsConsumerHarness {
    /// Creates a new MRS consumer conformance harness with the specified requirements.
    pub fn new(requirements: ConsumerRequirements) -> Self {
        Self {
            requirements,
            capabilities: None,
            projectors: HashMap::new(),
            reference_rate_symbols: HashSet::new(),
            seen_trades: HashMap::new(),
            stale_symbols: HashSet::new(),
            awaiting_recovery_snapshot: HashSet::new(),
            last_ordinals: HashMap::new(),
            stats: MrsConsumerStats::default(),
        }
    }

    /// Attaches provider capabilities to this consumer harness.
    pub fn with_capabilities(mut self, capabilities: ProviderCapabilities) -> Self {
        self.capabilities = Some(capabilities);
        self
    }

    /// Registers a symbol with its corresponding top-of-book quote projector.
    pub fn register_symbol(&mut self, symbol: Symbol, tag: SymbolTag, max_depth: Option<usize>) {
        match self.requirements.quote_mode {
            QuoteMode::NativeTicker => {
                let proj = TopOfBookProjector::new_ticker(symbol.clone(), tag);
                self.projectors.insert(symbol, proj);
            }
            QuoteMode::L2Projection => {
                let proj = TopOfBookProjector::new_l2(symbol.clone(), tag, max_depth);
                self.projectors.insert(symbol, proj);
            }
            QuoteMode::ReferenceRate => {
                self.reference_rate_symbols.insert(symbol);
            }
        }
    }

    /// Returns the accumulated statistics.
    pub fn stats(&self) -> &MrsConsumerStats {
        &self.stats
    }

    /// Validates a delivered market event against MRS contracts.
    pub fn ingest_event(
        &mut self,
        event: &MarketEvent<MarketDataKind>,
    ) -> Result<Option<QuoteProjection>, String> {
        let generation = event.delivery.connection_generation;
        let ord = event.delivery.ordinal;

        // Ordinal must strictly increase within connection generation
        if let Some(&last) = self.last_ordinals.get(&generation) {
            if ord <= last {
                return Err(format!(
                    "Delivery ordinal violation on gen {generation}: received {ord} <= last {last}"
                ));
            }
        }
        self.last_ordinals.insert(generation, ord);
        self.stats.delivered_events += 1;

        // Stale symbol isolation: no trade, book_ticker, or delta allowed while stale
        if self.stale_symbols.contains(&event.symbol) {
            match &event.kind {
                MarketDataKind::Trade(_)
                | MarketDataKind::BookTicker(_)
                | MarketDataKind::L2Delta(_) => {
                    return Err(format!(
                        "Violation: received market data {:?} for symbol {} while stale (before snapshot/resync)",
                        event.kind, event.symbol
                    ));
                }
                _ => {}
            }
        }

        // Integrity alert tracking
        if let MarketDataKind::IntegrityAlert(alert) = &event.kind {
            match alert {
                IntegrityAlert::ResyncCompleted { .. } => {
                    // Recovery is only complete once a snapshot rebuilt the book.
                    if self.awaiting_recovery_snapshot.contains(&event.symbol) {
                        return Err(format!(
                            "ResyncCompleted for {} without an intervening recovery snapshot",
                            event.symbol
                        ));
                    }
                    self.stale_symbols.remove(&event.symbol);
                }
                IntegrityAlert::GapDetected { .. } | IntegrityAlert::ChecksumMismatch { .. } => {
                    self.awaiting_recovery_snapshot.insert(event.symbol.clone());
                    self.stale_symbols.insert(event.symbol.clone());
                }
                IntegrityAlert::ResyncStarted => {
                    if self.requirements.quote_mode == QuoteMode::L2Projection {
                        self.awaiting_recovery_snapshot.insert(event.symbol.clone());
                    }
                    self.stale_symbols.insert(event.symbol.clone());
                }
                IntegrityAlert::SilenceTimeout { .. }
                | IntegrityAlert::HeartbeatTimeout { .. }
                | IntegrityAlert::ConnectionLost { .. }
                | IntegrityAlert::SourceError { .. }
                | IntegrityAlert::Quarantined { .. }
                | IntegrityAlert::SinkFailure => {
                    self.stale_symbols.insert(event.symbol.clone());
                }
            }
        }

        // Snapshot clears stale marker for that symbol
        if let MarketDataKind::L2Snapshot(_) = &event.kind {
            self.stale_symbols.remove(&event.symbol);
            self.awaiting_recovery_snapshot.remove(&event.symbol);
        }

        // ReferenceRate verification rules
        if let MarketDataKind::ReferenceRate(rate) = &event.kind {
            if self.requirements.quote_mode != QuoteMode::ReferenceRate {
                return Err(format!(
                    "ReferenceRate received for {} but quote_mode is {:?}",
                    event.symbol, self.requirements.quote_mode
                ));
            }
            if rate.mid <= Decimal::ZERO {
                return Err(format!(
                    "Invalid non-positive reference rate {} for {}",
                    rate.mid, event.symbol
                ));
            }
            self.stats.available_quotes += 1;
            return Ok(None);
        }

        if self.requirements.quote_mode == QuoteMode::ReferenceRate {
            match &event.kind {
                MarketDataKind::BookTicker(_)
                | MarketDataKind::L2Snapshot(_)
                | MarketDataKind::L2Delta(_) => {
                    return Err(format!(
                        "Book/Ticker event received for {} but quote_mode is ReferenceRate",
                        event.symbol
                    ));
                }
                _ => {}
            }
        }

        // Trade verification rules
        if let MarketDataKind::Trade(trade) = &event.kind {
            if self.requirements.require_stable_trades && trade.identity.is_none() {
                return Err(format!(
                    "Trade rejected: missing stable native trade identity for symbol {}",
                    event.symbol
                ));
            }
            if self.requirements.require_verified_side {
                // Fail closed: without declared capabilities the side cannot be verified.
                let Some(caps) = &self.capabilities else {
                    return Err(format!(
                        "Trade rejected for symbol {}: consumer requires verified taker side but no provider capabilities were supplied",
                        event.symbol
                    ));
                };
                if !matches!(
                    caps.trades,
                    TradeCapability::Verified {
                        side_semantics: SideSemantics::AggressorTaker,
                        ..
                    }
                ) {
                    return Err(format!(
                        "Trade rejected for symbol {}: consumer requires verified taker side but provider trades capability is {:?}",
                        event.symbol, caps.trades
                    ));
                }
            }
            if let Some(id) = &trade.identity {
                if let Some(existing) = self.seen_trades.get(&(event.symbol.clone(), id.clone())) {
                    if existing.price != trade.price
                        || existing.volume != trade.volume
                        || existing.aggressor != trade.aggressor
                    {
                        return Err(format!(
                            "Trade identity reuse conflict for symbol {}: trade id {:?} has differing data: existing {:?} vs new {:?}",
                            event.symbol, id, existing, trade
                        ));
                    }
                } else {
                    self.seen_trades
                        .insert((event.symbol.clone(), id.clone()), trade.clone());
                }
            }
            self.stats.accepted_trades += 1;
        }

        // Quote projection tracking
        let projection = if let Some(proj) = self.projectors.get_mut(&event.symbol) {
            let q_opt = proj.handle_event(event);
            if let Some(ref q) = q_opt {
                match q {
                    QuoteProjection::Available { .. } => self.stats.available_quotes += 1,
                    QuoteProjection::Unavailable { .. } => self.stats.unavailable_quotes += 1,
                }
            }
            q_opt
        } else {
            None
        };

        Ok(projection)
    }

    /// Handles a terminal delivery fault, asserting that all active projectors transition to Unavailable.
    pub fn ingest_fault(&mut self, fault: &DeliveryFault) {
        self.stats.delivery_faults += 1;

        // Invalidate all active projectors and assert unavailable transition
        for (sym, proj) in self.projectors.iter_mut() {
            let fault_event = MarketEvent {
                exchange_ts: None,
                received_ts: UnixNanos(0),
                venue: mdc_core::VenueId::Binance,
                symbol: sym.clone(),
                tag: 0,
                delivery: fault.stamp,
                source_sequence: None,
                kind: MarketDataKind::IntegrityAlert(IntegrityAlert::ConnectionLost {
                    reason: mdc_core::DisconnectKind::Closed,
                }),
            };
            let q = proj.handle_event(&fault_event);
            assert!(
                !proj.is_in_sync(),
                "Projector for {sym} must be out of sync after terminal delivery fault"
            );
            if let Some(QuoteProjection::Unavailable { .. }) = q {
                self.stats.unavailable_quotes += 1;
            }
        }
    }

    /// Drains a [`LossIntolerantStream`] until EOF and verifies all invariants.
    pub async fn run_stream(
        &mut self,
        stream: &mut LossIntolerantStream,
    ) -> Result<MrsConsumerStats, String> {
        while let Some(res) = stream.recv().await {
            match res {
                Ok(event) => {
                    self.ingest_event(&event)?;
                }
                Err(fault) => {
                    self.ingest_fault(&fault);
                }
            }
        }

        Ok(self.stats.clone())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use mdc_core::{
        BookTicker, CapabilityBounds, DeliveryStamp, DiscoveryCapability, IdentityScope,
        IntegrityCapability, L2Snapshot, PriceLevel, ReferenceRate, ReplayCapability,
        RestQuoteCapability, RunId, Side, TimestampCapability, TradeId, TradeNamespace, VenueId,
    };
    use mdc_sinks::{DeliveryFault, DeliveryFaultReason};

    fn make_event<T>(sym: &str, generation: u64, ord: u64, kind: T) -> MarketEvent<T> {
        MarketEvent {
            exchange_ts: None,
            received_ts: UnixNanos(100),
            venue: VenueId::Binance,
            symbol: Symbol::from(sym),
            tag: 0,
            delivery: DeliveryStamp::new(RunId::from_u128(1), generation, ord),
            source_sequence: None,
            kind,
        }
    }

    #[test]
    fn test_ingest_fault_transitions_to_unavailable() {
        let req = ConsumerRequirements {
            quote_mode: QuoteMode::NativeTicker,
            require_stable_trades: false,
            require_verified_side: false,
        };
        let mut harness = MrsConsumerHarness::new(req);
        let sym = Symbol::from("BTCUSDT");
        harness.register_symbol(sym.clone(), 0, None);

        // Feed an initial ticker to make quote available
        let ticker = BookTicker::new(
            PriceLevel::new(Decimal::from(100), Decimal::from(1)),
            PriceLevel::new(Decimal::from(101), Decimal::from(1)),
            Some(1),
        );
        let ev = make_event("BTCUSDT", 1, 1, MarketDataKind::BookTicker(ticker));
        let q = harness.ingest_event(&ev).unwrap();
        assert!(matches!(q, Some(QuoteProjection::Available { .. })));

        // Ingest fault
        let fault = DeliveryFault {
            stamp: DeliveryStamp::new(RunId::from_u128(1), 1, 2),
            reason: DeliveryFaultReason::ReceiverClosed,
            affected_connection: 1,
        };
        harness.ingest_fault(&fault);
        assert_eq!(harness.stats().delivery_faults, 1);
        assert_eq!(harness.stats().unavailable_quotes, 1);
    }

    #[test]
    fn test_reference_rate_mode_validation() {
        let req = ConsumerRequirements {
            quote_mode: QuoteMode::ReferenceRate,
            require_stable_trades: false,
            require_verified_side: false,
        };
        let mut harness = MrsConsumerHarness::new(req);
        harness.register_symbol(Symbol::from("EURUSD"), 0, None);

        // Valid positive rate
        let ev_ok = make_event(
            "EURUSD",
            1,
            1,
            MarketDataKind::ReferenceRate(ReferenceRate::new(Decimal::from(105))),
        );
        assert!(harness.ingest_event(&ev_ok).is_ok());
        assert_eq!(harness.stats().available_quotes, 1);

        // Zero or negative rate rejected
        let ev_neg = make_event(
            "EURUSD",
            1,
            2,
            MarketDataKind::ReferenceRate(ReferenceRate::new(Decimal::ZERO)),
        );
        assert!(harness.ingest_event(&ev_neg).is_err());

        // BookTicker rejected in ReferenceRate mode
        let ticker = BookTicker::new(
            PriceLevel::new(Decimal::from(100), Decimal::from(1)),
            PriceLevel::new(Decimal::from(101), Decimal::from(1)),
            Some(2),
        );
        let ev_ticker = make_event("EURUSD", 1, 3, MarketDataKind::BookTicker(ticker));
        assert!(harness.ingest_event(&ev_ticker).is_err());
    }

    #[test]
    fn test_trade_conformance_and_conflict_detection() {
        let req = ConsumerRequirements {
            quote_mode: QuoteMode::NativeTicker,
            require_stable_trades: true,
            require_verified_side: true,
        };
        let caps = ProviderCapabilities {
            quote_modes: vec![QuoteMode::NativeTicker],
            trades: TradeCapability::Verified {
                identity_scope: IdentityScope::PerMarket,
                namespace_scheme: "binance".to_string(),
                side_semantics: SideSemantics::AggressorTaker,
            },
            timestamps: TimestampCapability::ExchangeMilliseconds,
            integrity: IntegrityCapability::SequenceNumbers,
            replay: ReplayCapability::None,
            bounds: CapabilityBounds::default(),
            discovery: DiscoveryCapability::unsupported("n/a"),
            rest_quote: RestQuoteCapability::unsupported("n/a"),
        };

        let mut harness = MrsConsumerHarness::new(req).with_capabilities(caps);
        harness.register_symbol(Symbol::from("BTCUSDT"), 0, None);

        let id = NativeTradeIdentity::new(
            TradeNamespace::new("binance").unwrap(),
            TradeId::new("1001").unwrap(),
        );
        let trade = Trade::new(
            Some(id.clone()),
            Decimal::from(50000),
            Decimal::from(1),
            Side::Bid,
        );

        // First trade accepted
        let ev1 = make_event("BTCUSDT", 1, 1, MarketDataKind::Trade(trade.clone()));
        assert!(harness.ingest_event(&ev1).is_ok());

        // Duplicate with identical data is accepted (idempotent replay)
        let ev2 = make_event("BTCUSDT", 1, 2, MarketDataKind::Trade(trade.clone()));
        assert!(harness.ingest_event(&ev2).is_ok());

        // Duplicate with conflicting data rejected
        let trade_conflict =
            Trade::new(Some(id), Decimal::from(50001), Decimal::from(1), Side::Bid);
        let ev3 = make_event("BTCUSDT", 1, 3, MarketDataKind::Trade(trade_conflict));
        assert!(harness.ingest_event(&ev3).is_err());

        // Missing trade identity rejected when require_stable_trades: true
        let anonymous_trade = Trade::new(None, Decimal::from(50000), Decimal::from(1), Side::Bid);
        let ev4 = make_event("BTCUSDT", 1, 4, MarketDataKind::Trade(anonymous_trade));
        assert!(harness.ingest_event(&ev4).is_err());
    }

    #[test]
    fn test_stale_symbol_isolation() {
        let req = ConsumerRequirements {
            quote_mode: QuoteMode::NativeTicker,
            require_stable_trades: false,
            require_verified_side: false,
        };
        let mut harness = MrsConsumerHarness::new(req);
        harness.register_symbol(Symbol::from("BTCUSDT"), 0, None);

        // Gap alert marks symbol as stale
        let gap_ev = make_event(
            "BTCUSDT",
            1,
            1,
            MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
                expected: 10,
                received: 12,
            }),
        );
        assert!(harness.ingest_event(&gap_ev).is_ok());

        // Trade while stale is rejected
        let trade = Trade::new(None, Decimal::from(50000), Decimal::from(1), Side::Bid);
        let trade_ev = make_event("BTCUSDT", 1, 2, MarketDataKind::Trade(trade));
        assert!(harness.ingest_event(&trade_ev).is_err());

        // Snapshot clears stale marker
        let snap = L2Snapshot::new(
            vec![PriceLevel::new(Decimal::from(100), Decimal::ONE)],
            vec![PriceLevel::new(Decimal::from(101), Decimal::ONE)],
            Some(100),
        );
        let snap_ev = make_event("BTCUSDT", 1, 3, MarketDataKind::L2Snapshot(snap));
        assert!(harness.ingest_event(&snap_ev).is_ok());

        // Now trade is accepted
        let trade2 = Trade::new(None, Decimal::from(50000), Decimal::from(1), Side::Bid);
        let trade2_ev = make_event("BTCUSDT", 1, 4, MarketDataKind::Trade(trade2));
        assert!(harness.ingest_event(&trade2_ev).is_ok());
    }

    #[test]
    fn test_verified_side_fails_closed_without_capabilities() {
        let req = ConsumerRequirements {
            quote_mode: QuoteMode::NativeTicker,
            require_stable_trades: false,
            require_verified_side: true,
        };
        let mut harness = MrsConsumerHarness::new(req);
        harness.register_symbol(Symbol::from("BTCUSDT"), 0, None);

        let trade = Trade::new(None, Decimal::from(50000), Decimal::from(1), Side::Bid);
        let err = harness
            .ingest_event(&make_event("BTCUSDT", 1, 1, MarketDataKind::Trade(trade)))
            .unwrap_err();
        assert!(err.contains("no provider capabilities"), "{err}");
    }

    #[test]
    fn test_gap_then_resync_completed_without_snapshot_is_rejected() {
        let req = ConsumerRequirements {
            quote_mode: QuoteMode::L2Projection,
            require_stable_trades: false,
            require_verified_side: false,
        };
        let mut harness = MrsConsumerHarness::new(req);
        harness.register_symbol(Symbol::from("BTCUSDT"), 0, Some(10));

        let gap = MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
            expected: 10,
            received: 12,
        });
        harness
            .ingest_event(&make_event("BTCUSDT", 1, 1, gap))
            .unwrap();

        let completed = MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: Some(12),
        });
        let err = harness
            .ingest_event(&make_event("BTCUSDT", 1, 2, completed))
            .unwrap_err();
        assert!(
            err.contains("without an intervening recovery snapshot"),
            "{err}"
        );
    }

    #[test]
    fn test_gap_snapshot_resync_completed_is_accepted() {
        let req = ConsumerRequirements {
            quote_mode: QuoteMode::L2Projection,
            require_stable_trades: false,
            require_verified_side: false,
        };
        let mut harness = MrsConsumerHarness::new(req);
        harness.register_symbol(Symbol::from("BTCUSDT"), 0, Some(10));

        let gap = MarketDataKind::IntegrityAlert(IntegrityAlert::GapDetected {
            expected: 10,
            received: 12,
        });
        harness
            .ingest_event(&make_event("BTCUSDT", 1, 1, gap))
            .unwrap();
        let snap = L2Snapshot::new(
            vec![PriceLevel::new(Decimal::from(100), Decimal::ONE)],
            vec![PriceLevel::new(Decimal::from(101), Decimal::ONE)],
            Some(12),
        );
        harness
            .ingest_event(&make_event(
                "BTCUSDT",
                1,
                2,
                MarketDataKind::L2Snapshot(snap),
            ))
            .unwrap();
        let completed = MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
            last_update_id: Some(12),
        });
        harness
            .ingest_event(&make_event("BTCUSDT", 1, 3, completed))
            .unwrap();
    }
}
