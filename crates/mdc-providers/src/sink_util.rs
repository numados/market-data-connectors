//! Downstream sink management, delivery ordering, and fail-closed sink error propagation.

use mdc_core::{
    BookKind, BookTicker, DeliveryStamp, EventContext, IntegrityAlert, RawLevelText, ReferenceRate,
    RunId, SequenceRange, Side, StreamingSink, Symbol, SymbolTag, Trade, UnixNanos, VenueId,
};
use rust_decimal::Decimal;
use tracing::warn;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Error indicating that the downstream streaming sink failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Downstream streaming sink failed")]
pub struct SinkError;

/// Wrapper around a [`StreamingSink`] enforcing delivery stamp ordering, unique ordinals,
/// and fail-closed sink failure propagation.
pub struct ConnectionSink<S> {
    sink: S,
    run_id: RunId,
    connection_id: u32,
    connection_generation: u64,
    delivery_ordinal: u64,
    failed: bool,
    generation_allocator: Option<Arc<AtomicU64>>,
    active_venue: Option<VenueId>,
    active_symbol: Option<Symbol>,
    active_tag: SymbolTag,
    active_exchange_ts: Option<UnixNanos>,
    active_received_ts: Option<UnixNanos>,
    active_source_sequence: Option<u64>,
}

#[allow(dead_code)]
impl<S: StreamingSink> ConnectionSink<S> {
    /// Creates a new `ConnectionSink`.
    pub fn new(sink: S, run_id: RunId, connection_id: u32) -> Self {
        Self {
            sink,
            run_id,
            connection_id,
            connection_generation: 0,
            delivery_ordinal: 0,
            failed: false,
            generation_allocator: None,
            active_venue: None,
            active_symbol: None,
            active_tag: SymbolTag::default(),
            active_exchange_ts: None,
            active_received_ts: None,
            active_source_sequence: None,
        }
    }

    /// Injects a run-scoped generation allocator shared across all connections/buckets.
    pub fn with_generation_allocator(mut self, alloc: Arc<AtomicU64>) -> Self {
        self.generation_allocator = Some(alloc);
        self
    }

    /// Increments the connection generation on each physical socket (re)connection.
    pub fn next_generation(&mut self) {
        if let Some(alloc) = &self.generation_allocator {
            self.connection_generation = alloc.fetch_add(1, Ordering::Relaxed) + 1;
        } else {
            self.connection_generation = self.connection_generation.saturating_add(1);
        }
    }

    /// Returns the current connection generation.
    pub fn generation(&self) -> u64 {
        self.connection_generation
    }

    /// Returns the active RunId.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the connection ID.
    pub fn connection_id(&self) -> u32 {
        self.connection_id
    }

    /// Returns whether the sink has entered the failed state.
    pub fn is_failed(&self) -> bool {
        self.failed
    }

    /// Returns the next monotonic delivery stamp for this connection.
    pub fn next_stamp(&mut self) -> DeliveryStamp {
        self.delivery_ordinal = self.delivery_ordinal.saturating_add(1);
        DeliveryStamp::with_connection_id(
            self.run_id,
            self.connection_id,
            self.connection_generation,
            self.delivery_ordinal,
        )
    }

    /// Best-effort discard helper for alert emissions on teardown paths.
    ///
    /// This is the sole permitted best-effort discard on sink emissions.
    pub fn best_effort_alert(&mut self, ctx: &EventContext<'_>, alert: IntegrityAlert) {
        let _ = self.sink.alert(ctx, alert);
    }

    /// Best-effort alert emission with context generation for teardown paths.
    #[allow(clippy::too_many_arguments)]
    pub fn best_effort_emit_alert(
        &mut self,
        venue: VenueId,
        symbol: &Symbol,
        tag: SymbolTag,
        exchange_ts: Option<UnixNanos>,
        received_ts: UnixNanos,
        source_sequence: Option<u64>,
        alert: IntegrityAlert,
    ) {
        if self.failed {
            return;
        }
        let ctx = self.next_context(
            venue,
            symbol,
            tag,
            exchange_ts,
            received_ts,
            source_sequence,
        );
        self.best_effort_alert(&ctx, alert);
    }

    /// Constructs an [`EventContext`] with a fresh monotonic delivery stamp.
    pub fn next_context<'a>(
        &mut self,
        venue: VenueId,
        symbol: &'a Symbol,
        tag: SymbolTag,
        exchange_ts: Option<UnixNanos>,
        received_ts: UnixNanos,
        source_sequence: Option<u64>,
    ) -> EventContext<'a> {
        let delivery = self.next_stamp();
        EventContext {
            venue,
            symbol,
            tag,
            exchange_ts,
            received_ts,
            connection_id: self.connection_id,
            delivery,
            source_sequence,
        }
    }

    fn handle_sink_error<E: std::error::Error>(&mut self, error: &E, op: &str) {
        self.failed = true;
        warn!(
            connection_id = self.connection_id,
            generation = self.connection_generation,
            op,
            error = %error,
            "Downstream streaming sink failed; propagating SinkFailure and closing connection"
        );
        if let (Some(venue), Some(symbol)) = (self.active_venue, self.active_symbol.clone()) {
            let delivery = self.next_stamp();
            let fail_ctx = EventContext {
                venue,
                symbol: &symbol,
                tag: self.active_tag,
                exchange_ts: self.active_exchange_ts,
                received_ts: self.active_received_ts.unwrap_or_else(crate::now_nanos),
                connection_id: self.connection_id,
                delivery,
                source_sequence: self.active_source_sequence,
            };
            self.best_effort_alert(&fail_ctx, IntegrityAlert::SinkFailure);
        }
    }

    /// Emits `begin_book` to the downstream sink.
    pub fn begin_book(&mut self, ctx: &EventContext<'_>, kind: BookKind) -> Result<(), SinkError> {
        if self.failed {
            return Err(SinkError);
        }
        self.active_venue = Some(ctx.venue);
        self.active_symbol = Some(ctx.symbol.clone());
        self.active_tag = ctx.tag;
        self.active_exchange_ts = ctx.exchange_ts;
        self.active_received_ts = Some(ctx.received_ts);
        self.active_source_sequence = ctx.source_sequence;

        if let Err(e) = self.sink.begin_book(ctx, kind) {
            self.handle_sink_error(&e, "begin_book");
            return Err(SinkError);
        }
        Ok(())
    }

    /// Emits a single price level to the downstream sink.
    pub fn level(&mut self, side: Side, price: Decimal, volume: Decimal) -> Result<(), SinkError> {
        if self.failed {
            return Err(SinkError);
        }
        if let Err(e) = self.sink.level(side, price, volume) {
            self.handle_sink_error(&e, "level");
            return Err(SinkError);
        }
        Ok(())
    }

    /// Emits a raw price level to the downstream sink.
    pub fn level_raw(
        &mut self,
        side: Side,
        price: Decimal,
        volume: Decimal,
        raw: RawLevelText<'_>,
    ) -> Result<(), SinkError> {
        if self.failed {
            return Err(SinkError);
        }
        if let Err(e) = self.sink.level_raw(side, price, volume, raw) {
            self.handle_sink_error(&e, "level_raw");
            return Err(SinkError);
        }
        Ok(())
    }

    /// Emits `end_book` to the downstream sink.
    pub fn end_book(
        &mut self,
        ctx: &EventContext<'_>,
        seq: Option<SequenceRange>,
    ) -> Result<(), SinkError> {
        if self.failed {
            return Err(SinkError);
        }
        self.active_venue = Some(ctx.venue);
        self.active_symbol = Some(ctx.symbol.clone());
        self.active_tag = ctx.tag;
        self.active_exchange_ts = ctx.exchange_ts;
        self.active_received_ts = Some(ctx.received_ts);
        self.active_source_sequence = ctx.source_sequence;

        if let Err(e) = self.sink.end_book(ctx, seq) {
            self.handle_sink_error(&e, "end_book");
            return Err(SinkError);
        }
        self.active_symbol = None;
        Ok(())
    }

    /// Emits a normalized public trade to the downstream sink.
    pub fn trade(&mut self, ctx: &EventContext<'_>, trade: &Trade) -> Result<(), SinkError> {
        if self.failed {
            return Err(SinkError);
        }
        self.active_venue = Some(ctx.venue);
        self.active_symbol = Some(ctx.symbol.clone());
        self.active_tag = ctx.tag;
        self.active_exchange_ts = ctx.exchange_ts;
        self.active_received_ts = Some(ctx.received_ts);
        self.active_source_sequence = ctx.source_sequence;

        if let Err(e) = self.sink.trade(ctx, trade) {
            self.handle_sink_error(&e, "trade");
            return Err(SinkError);
        }
        self.active_symbol = None;
        Ok(())
    }

    /// Emits a book ticker to the downstream sink.
    pub fn book_ticker(
        &mut self,
        ctx: &EventContext<'_>,
        ticker: &BookTicker,
    ) -> Result<(), SinkError> {
        if self.failed {
            return Err(SinkError);
        }
        self.active_venue = Some(ctx.venue);
        self.active_symbol = Some(ctx.symbol.clone());
        self.active_tag = ctx.tag;
        self.active_exchange_ts = ctx.exchange_ts;
        self.active_received_ts = Some(ctx.received_ts);
        self.active_source_sequence = ctx.source_sequence;

        if let Err(e) = self.sink.book_ticker(ctx, ticker) {
            self.handle_sink_error(&e, "book_ticker");
            return Err(SinkError);
        }
        self.active_symbol = None;
        Ok(())
    }

    /// Emits a reference rate to the downstream sink.
    pub fn reference_rate(
        &mut self,
        ctx: &EventContext<'_>,
        rate: &ReferenceRate,
    ) -> Result<(), SinkError> {
        if self.failed {
            return Err(SinkError);
        }
        self.active_venue = Some(ctx.venue);
        self.active_symbol = Some(ctx.symbol.clone());
        self.active_tag = ctx.tag;
        self.active_exchange_ts = ctx.exchange_ts;
        self.active_received_ts = Some(ctx.received_ts);
        self.active_source_sequence = ctx.source_sequence;

        if let Err(e) = self.sink.reference_rate(ctx, rate) {
            self.handle_sink_error(&e, "reference_rate");
            return Err(SinkError);
        }
        self.active_symbol = None;
        Ok(())
    }

    /// Emits an integrity alert to the downstream sink.
    pub fn alert(
        &mut self,
        ctx: &EventContext<'_>,
        alert: IntegrityAlert,
    ) -> Result<(), SinkError> {
        if self.failed {
            return Err(SinkError);
        }
        self.active_venue = Some(ctx.venue);
        self.active_symbol = Some(ctx.symbol.clone());
        self.active_tag = ctx.tag;
        self.active_exchange_ts = ctx.exchange_ts;
        self.active_received_ts = Some(ctx.received_ts);
        self.active_source_sequence = ctx.source_sequence;

        if let Err(e) = self.sink.alert(ctx, alert) {
            if alert != IntegrityAlert::SinkFailure {
                self.handle_sink_error(&e, "alert");
            } else {
                self.failed = true;
                warn!(
                    connection_id = self.connection_id,
                    generation = self.connection_generation,
                    alert = ?alert,
                    error = %e,
                    "Downstream streaming sink failed on terminal SinkFailure alert emission"
                );
            }
            return Err(SinkError);
        }
        self.active_symbol = None;
        Ok(())
    }

    /// Helper to construct context and emit an alert in one call.
    #[allow(
        clippy::too_many_arguments,
        reason = "Alert emission requires full EventContext parameters plus the alert payload"
    )]
    pub fn emit_alert(
        &mut self,
        venue: VenueId,
        symbol: &Symbol,
        tag: SymbolTag,
        exchange_ts: Option<UnixNanos>,
        received_ts: UnixNanos,
        source_sequence: Option<u64>,
        alert: IntegrityAlert,
    ) -> Result<(), SinkError> {
        let ctx = self.next_context(
            venue,
            symbol,
            tag,
            exchange_ts,
            received_ts,
            source_sequence,
        );
        self.alert(&ctx, alert)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use mdc_testkit::{FaultInjectingSink, FaultInjectionPoint};

    #[test]
    fn test_delivery_stamp_ordering_and_generations() {
        let sym = Symbol::from("BTCUSDT");
        let run_id = RunId::from_u128(12345);
        let sink = mdc_testkit::RecordingSink::new();
        let mut conn_sink = ConnectionSink::new(sink.clone(), run_id, 1);

        assert_eq!(conn_sink.generation(), 0);
        conn_sink.next_generation();
        assert_eq!(conn_sink.generation(), 1);

        let ctx1 = conn_sink.next_context(
            VenueId::Binance,
            &sym,
            SymbolTag::default(),
            None,
            UnixNanos(1),
            None,
        );
        assert_eq!(ctx1.delivery.run, run_id);
        assert_eq!(ctx1.delivery.connection_generation, 1);
        assert_eq!(ctx1.delivery.ordinal, 1);
        conn_sink
            .alert(&ctx1, IntegrityAlert::ResyncStarted)
            .unwrap();

        let ctx2 = conn_sink.next_context(
            VenueId::Binance,
            &sym,
            SymbolTag::default(),
            None,
            UnixNanos(2),
            None,
        );
        assert_eq!(ctx2.delivery.connection_generation, 1);
        assert_eq!(ctx2.delivery.ordinal, 2);
        conn_sink
            .alert(
                &ctx2,
                IntegrityAlert::ResyncCompleted {
                    last_update_id: None,
                },
            )
            .unwrap();

        // Reconnect advances generation
        conn_sink.next_generation();
        assert_eq!(conn_sink.generation(), 2);
        let ctx3 = conn_sink.next_context(
            VenueId::Binance,
            &sym,
            SymbolTag::default(),
            None,
            UnixNanos(3),
            None,
        );
        assert_eq!(ctx3.delivery.connection_generation, 2);
        assert_eq!(ctx3.delivery.ordinal, 3);
        conn_sink
            .alert(&ctx3, IntegrityAlert::ResyncStarted)
            .unwrap();

        let stamps = sink.delivery_stamps();
        assert_eq!(stamps.len(), 3);
        assert_eq!(stamps[0].connection_generation, 1);
        assert_eq!(stamps[0].ordinal, 1);
        assert_eq!(stamps[1].connection_generation, 1);
        assert_eq!(stamps[1].ordinal, 2);
        assert_eq!(stamps[2].connection_generation, 2);
        assert_eq!(stamps[2].ordinal, 3);
    }

    #[test]
    fn test_fail_closed_on_level_error() {
        let sym = Symbol::from("BTCUSDT");
        let run_id = RunId::from_u128(1);
        let fault_sink = FaultInjectingSink::new(FaultInjectionPoint::Level(2));
        let mut conn_sink = ConnectionSink::new(fault_sink.clone(), run_id, 1);
        conn_sink.next_generation();

        let ctx = conn_sink.next_context(
            VenueId::Binance,
            &sym,
            SymbolTag::default(),
            None,
            UnixNanos(1),
            None,
        );
        conn_sink.begin_book(&ctx, BookKind::Snapshot).unwrap();
        conn_sink
            .level(Side::Bid, Decimal::ONE, Decimal::ONE)
            .unwrap();

        // Level 2 fails
        let err = conn_sink.level(Side::Ask, Decimal::ONE, Decimal::ONE);
        assert!(err.is_err());
        assert!(conn_sink.is_failed());

        // Subsequent calls fail immediately without touching sink
        assert!(
            conn_sink
                .level(Side::Bid, Decimal::ONE, Decimal::ONE)
                .is_err()
        );
        assert!(conn_sink.end_book(&ctx, None).is_err());

        // SinkFailure was emitted in-band
        let alerts = fault_sink.alerts();
        assert!(alerts.contains(&IntegrityAlert::SinkFailure));
    }

    #[test]
    fn test_fail_closed_on_alert_error() {
        let sym = Symbol::from("BTCUSDT");
        let run_id = RunId::from_u128(1);
        let fault_sink = FaultInjectingSink::new(FaultInjectionPoint::Alert);
        let mut conn_sink = ConnectionSink::new(fault_sink.clone(), run_id, 1);
        conn_sink.next_generation();

        let ctx = conn_sink.next_context(
            VenueId::Binance,
            &sym,
            SymbolTag::default(),
            None,
            UnixNanos(1),
            None,
        );

        // Initial alert trips the fault point and triggers handle_sink_error with terminal SinkFailure
        let err = conn_sink.alert(&ctx, IntegrityAlert::ResyncStarted);
        assert!(err.is_err());
        assert!(conn_sink.is_failed());

        // Terminal SinkFailure was emitted in-band
        let alerts = fault_sink.alerts();
        assert!(alerts.contains(&IntegrityAlert::SinkFailure));

        // Subsequent operations fail immediately
        assert!(conn_sink.begin_book(&ctx, BookKind::Snapshot).is_err());
        assert!(conn_sink.level(Side::Bid, Decimal::ONE, Decimal::ONE).is_err());
    }

    #[test]
    fn test_run_scoped_generation_allocator() {
        let gen_alloc = Arc::new(AtomicU64::new(0));
        let run_id = RunId::from_u128(42);
        let fault_sink = FaultInjectingSink::new(FaultInjectionPoint::Never);

        let mut sink1 = ConnectionSink::new(fault_sink.clone(), run_id, 1)
            .with_generation_allocator(gen_alloc.clone());
        let mut sink2 = ConnectionSink::new(fault_sink.clone(), run_id, 2)
            .with_generation_allocator(gen_alloc.clone());

        sink1.next_generation();
        assert_eq!(sink1.generation(), 1);

        sink2.next_generation();
        assert_eq!(sink2.generation(), 2);

        sink1.next_generation();
        assert_eq!(sink1.generation(), 3);

        let stamp1 = sink1.next_stamp();
        assert_eq!(stamp1.connection_id, 1);
        assert_eq!(stamp1.connection_generation, 3);
        assert_eq!(stamp1.ordinal, 1);

        let stamp2 = sink2.next_stamp();
        assert_eq!(stamp2.connection_id, 2);
        assert_eq!(stamp2.connection_generation, 2);
        assert_eq!(stamp2.ordinal, 1);
    }
}
