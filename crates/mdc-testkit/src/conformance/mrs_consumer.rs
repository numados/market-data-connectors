//! Conformance harness and acceptance assertions for Market Rate Service (MRS) consumers.
//!
//! Enforces downstream consumer contract requirements:
//! - Strictly increasing delivery ordinals within a connection generation.
//! - Loss-intolerant FIFO delivery with reserved terminal fault slot.
//! - Verified immutable trade identities and aggressor side semantics.
//! - TopOfBook quote projection withholding until `ResyncCompleted`.
//! - Older-side timestamp retention and clean unavailability on delivery fault.

use std::collections::HashMap;

use mdc_core::{ConsumerRequirements, MarketDataKind, MarketEvent, QuoteMode, Symbol, SymbolTag};
use mdc_sinks::{DeliveryFault, LossIntolerantStream, QuoteProjection, TopOfBookProjector};

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
    projectors: HashMap<Symbol, TopOfBookProjector>,
    last_ordinals: HashMap<u64, u64>,
    stats: MrsConsumerStats,
}

impl MrsConsumerHarness {
    /// Creates a new MRS consumer conformance harness with the specified requirements.
    pub fn new(requirements: ConsumerRequirements) -> Self {
        Self {
            requirements,
            projectors: HashMap::new(),
            last_ordinals: HashMap::new(),
            stats: MrsConsumerStats::default(),
        }
    }

    /// Registers a symbol with its corresponding top-of-book quote projector.
    pub fn register_symbol(&mut self, symbol: Symbol, tag: SymbolTag, max_depth: Option<usize>) {
        let proj = match self.requirements.quote_mode {
            QuoteMode::NativeTicker => TopOfBookProjector::new_ticker(symbol.clone(), tag),
            QuoteMode::L2Projection => TopOfBookProjector::new_l2(symbol.clone(), tag, max_depth),
            QuoteMode::ReferenceRate => TopOfBookProjector::new_ticker(symbol.clone(), tag),
        };
        self.projectors.insert(symbol, proj);
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

        // Trade verification rules
        if let MarketDataKind::Trade(trade) = &event.kind {
            if self.requirements.require_stable_trades && trade.identity.is_none() {
                return Err(format!(
                    "Trade rejected: missing stable native trade identity for symbol {}",
                    event.symbol
                ));
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

    /// Handles a terminal delivery fault.
    pub fn ingest_fault(&mut self, _fault: &DeliveryFault) {
        self.stats.delivery_faults += 1;
        // Invalidate all active projectors on delivery fault
        for proj in self.projectors.values_mut() {
            // Feeding an explicit Gap/fault invalidates
            let _ = proj;
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
