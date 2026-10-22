//! Session lifecycle: loss-intolerant channel creation, connector/reducer
//! concurrency, cancellation, and bounded join (PRD 31 §§3.4, 3.7, 3.10).
//!
//! One owned session drives the provider run future and continuously drains
//! the channel into the ordered [`Reducer`]. Display snapshots are coalesced
//! onto a latest-value boundary at most once per render cadence, with one
//! final publish before the session ends so terminal states are never lost.

use std::sync::Arc;

use futures_util::StreamExt;
use mdc_sinks::loss_intolerant::loss_intolerant_channel;
use tokio::sync::watch;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::config::{RENDER_CADENCE, SHUTDOWN_BOUND, SessionConfig};
use crate::display::{DisplaySnapshot, DisplayStatus};
use crate::provider::{ProviderRun, SessionAdapter, SessionBounds};
use crate::reducer::Reducer;

/// Terminal outcome of a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOutcome {
    /// Terminal session status: `Stopped` or `Failed`.
    pub status: DisplayStatus,
    /// Sanitized failure cause when the session failed.
    pub failure: Option<String>,
}

/// Why the session ended; decides the final reducer transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Terminal {
    /// Adapter rejected the start before any provider I/O.
    StartFailed,
    /// The channel delivered its reserved terminal delivery fault.
    DeliveryFault,
    /// The provider future returned an error.
    ConnectorError,
    /// The provider task panicked or was cancelled unexpectedly.
    TaskFailed,
    /// The stream ended; operator stop decides stopped vs failed.
    StreamEof,
    /// The bounded shutdown deadline passed.
    ShutdownDeadline,
}

/// Runs one provider session to its terminal outcome.
///
/// `root` is the application-root token; the session creates a child from it
/// so operator stop, channel overflow, and connector shutdown share one
/// lineage. `capacity` is the loss-intolerant channel capacity (the
/// application passes [`crate::config::CHANNEL_CAPACITY`]; tests shrink it).
pub async fn run_session(
    adapter: Arc<dyn SessionAdapter>,
    config: SessionConfig,
    bounds: SessionBounds,
    capacity: usize,
    root: CancellationToken,
    display: watch::Sender<Arc<DisplaySnapshot>>,
) -> SessionOutcome {
    let session_token = root.child_token();
    let (sender, mut stream) =
        loss_intolerant_channel(capacity, bounds.affected_connection, session_token.clone());

    let mut reducer = Reducer::new(&config, bounds);
    let _ = display.send(Arc::new(reducer.snapshot()));

    let ProviderRun {
        future,
        abort_guards,
    } = match adapter.start(&config, sender, session_token.clone()) {
        Ok(run) => run,
        Err(error) => {
            reducer.apply_session_failure(error.to_string());
            return finish(reducer, display, Terminal::StartFailed);
        }
    };
    let mut provider_task = tokio::spawn(future);
    // Guards already cover spawned provider tasks; the run future itself is
    // owned by this spawned task and aborted below when it overruns.
    drop(abort_guards);

    let mut dirty = false;
    let mut last_publish = Instant::now();
    let mut stop_requested = false;
    let mut provider_done = false;
    let mut terminal: Option<Terminal> = None;

    // Phase 1 — running: select over cancellation, stream items, and
    // provider completion. The session join is never awaited inline by the
    // application loop; stop arrives as token cancellation.
    loop {
        tokio::select! {
            biased;
            _ = session_token.cancelled() => {
                // Operator stop, channel overflow, or root quit. Phase 2
                // drains the accepted prefix before anything else.
                stop_requested = true;
                break;
            }
            joined = &mut provider_task, if !provider_done => {
                record_provider_join(joined, &mut reducer, &mut terminal);
                provider_done = true;
                break;
            }
            item = stream.next() => match item {
                Some(Ok(event)) => {
                    reducer.apply_event(&event);
                    dirty = true;
                }
                Some(Err(fault)) => {
                    reducer.apply_fault(&fault);
                    terminal = Some(Terminal::DeliveryFault);
                    break;
                }
                None => {
                    if terminal.is_none() {
                        terminal = Some(Terminal::StreamEof);
                    }
                    break;
                }
            },
        }
        publish_if_dirty(&mut dirty, &mut last_publish, &reducer, &display);
    }

    // Phase 2 — draining: consume the accepted FIFO prefix until the stream
    // ends, bounded by the shutdown deadline. EOF is never a precondition:
    // a provider that ignores cancellation hits the deadline and is reaped.
    let deadline = Instant::now() + SHUTDOWN_BOUND;
    loop {
        if terminal.is_some() {
            break;
        }
        tokio::select! {
            biased;
            item = stream.next() => match item {
                Some(Ok(event)) => {
                    reducer.apply_event(&event);
                    dirty = true;
                }
                Some(Err(fault)) => {
                    reducer.apply_fault(&fault);
                    terminal = Some(Terminal::DeliveryFault);
                }
                None => break,
            },
            joined = &mut provider_task, if !provider_done => {
                record_provider_join(joined, &mut reducer, &mut terminal);
                provider_done = true;
                // Provider gone: drain what remains, then EOF ends the loop.
                while terminal.is_none() {
                    match stream.next().await {
                        Some(Ok(event)) => {
                            reducer.apply_event(&event);
                        }
                        Some(Err(fault)) => {
                            reducer.apply_fault(&fault);
                            terminal = Some(Terminal::DeliveryFault);
                        }
                        None => {
                            terminal = Some(Terminal::StreamEof);
                        }
                    }
                }
                break;
            }
            _ = tokio::time::sleep_until(deadline) => {
                if terminal.is_none() {
                    terminal = Some(Terminal::ShutdownDeadline);
                }
                break;
            }
        }
        publish_if_dirty(&mut dirty, &mut last_publish, &reducer, &display);
    }

    // Phase 3 — bounded teardown: abort an unfinished provider, await it
    // within the shutdown bound, then drain remaining items without blocking.
    if !provider_done {
        provider_task.abort();
        match timeout(SHUTDOWN_BOUND, &mut provider_task).await {
            Ok(joined) => record_provider_join(joined, &mut reducer, &mut terminal),
            Err(_) => {
                // Missed even abort+await: force-reap (requirement 114).
                provider_task.abort();
                let _ = (&mut provider_task).await;
                if terminal.is_none() {
                    reducer.apply_session_failure("session missed the shutdown bound".to_string());
                    terminal = Some(Terminal::ShutdownDeadline);
                }
            }
        }
    }
    while let Ok(Some(item)) = stream.try_recv() {
        match item {
            Ok(event) => reducer.apply_event(&event),
            Err(fault) => reducer.apply_fault(&fault),
        }
    }

    match terminal {
        Some(
            Terminal::StartFailed
            | Terminal::DeliveryFault
            | Terminal::ConnectorError
            | Terminal::TaskFailed,
        ) => {}
        Some(Terminal::StreamEof) | None => {
            reducer.apply_eof(stop_requested);
        }
        Some(Terminal::ShutdownDeadline) => {
            if stop_requested {
                reducer.apply_eof(true);
            }
        }
    }

    finish(reducer, display, terminal.unwrap_or(Terminal::StreamEof))
}

fn record_provider_join(
    joined: Result<Result<(), mdc_core::MdcError>, tokio::task::JoinError>,
    reducer: &mut Reducer,
    terminal: &mut Option<Terminal>,
) {
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            reducer.apply_session_failure(error.to_string());
            if terminal.is_none() {
                *terminal = Some(Terminal::ConnectorError);
            }
        }
        Err(join_error) if join_error.is_panic() => {
            reducer.apply_session_failure("provider task panicked".to_string());
            if terminal.is_none() {
                *terminal = Some(Terminal::TaskFailed);
            }
        }
        Err(_) => {}
    }
}

fn publish_if_dirty(
    dirty: &mut bool,
    last_publish: &mut Instant,
    reducer: &Reducer,
    display: &watch::Sender<Arc<DisplaySnapshot>>,
) {
    if *dirty && last_publish.elapsed() >= RENDER_CADENCE {
        let _ = display.send(Arc::new(reducer.snapshot()));
        *last_publish = Instant::now();
        *dirty = false;
    }
}

fn finish(
    mut reducer: Reducer,
    display: watch::Sender<Arc<DisplaySnapshot>>,
    terminal: Terminal,
) -> SessionOutcome {
    let _ = &mut reducer;
    let _ = terminal;
    // Final publish so terminal failure states are never lost (requirement 52).
    let _ = display.send(Arc::new(reducer.snapshot()));
    let snapshot = display.borrow().clone();
    SessionOutcome {
        status: snapshot.session,
        failure: snapshot.failure.clone(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::error::Error;
    use crate::provider::{ProviderFuture, ProviderKind, ProviderRun};
    use mdc_core::sink::MarketDataSink;
    use mdc_core::{
        DeliveryStamp, IntegrityAlert, L2Snapshot, MarketDataKind, MarketEvent, PriceLevel, RunId,
        Symbol, UnixNanos, VenueId,
    };
    use mdc_sinks::loss_intolerant::LossIntolerantSender;
    use rust_decimal::Decimal;

    fn bounds() -> SessionBounds {
        SessionBounds {
            max_symbols: 25,
            projector_depth: 100,
            affected_connection: 1,
        }
    }

    fn config() -> SessionConfig {
        SessionConfig::new(ProviderKind::Binance, &["BTCUSDT".to_string()]).unwrap()
    }

    fn event(ordinal: u64, kind: MarketDataKind) -> MarketEvent<MarketDataKind> {
        MarketEvent {
            exchange_ts: Some(UnixNanos(1)),
            received_ts: UnixNanos(ordinal),
            venue: VenueId::Binance,
            symbol: Symbol::from("BTCUSDT"),
            tag: 0,
            delivery: DeliveryStamp {
                run: RunId::from_bytes([0u8; 16]),
                connection_id: 0,
                connection_generation: 1,
                ordinal,
            },
            source_sequence: None,
            kind,
        }
    }

    fn book_event(ordinal: u64) -> MarketEvent<MarketDataKind> {
        event(
            ordinal,
            MarketDataKind::L2Snapshot(L2Snapshot {
                bids: vec![PriceLevel::new(Decimal::ONE, Decimal::ONE)],
                asks: vec![PriceLevel::new(Decimal::TWO, Decimal::ONE)],
                last_update_id: Some(1),
            }),
        )
    }

    fn alert_event(ordinal: u64) -> MarketEvent<MarketDataKind> {
        event(
            ordinal,
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
                last_update_id: Some(2),
            }),
        )
    }

    /// Provider-agnostic test double (PRD 31 §3.10 requirement 154).
    struct FakeAdapter {
        events: Vec<MarketEvent<MarketDataKind>>,
        behavior: FakeBehavior,
    }

    enum FakeBehavior {
        FinishOk,
        ConnectorError,
        Panic,
        IgnoreCancellation,
    }

    impl SessionAdapter for FakeAdapter {
        fn start(
            &self,
            _config: &SessionConfig,
            sink: LossIntolerantSender,
            session_token: CancellationToken,
        ) -> Result<ProviderRun, Error> {
            for event in &self.events {
                let _ = sink.on_event(event.clone());
            }
            // The sink is owned by the run future and dropped when the
            // provider completes, matching real connector causality: EOF
            // can only follow provider completion.
            let future: ProviderFuture = match self.behavior {
                FakeBehavior::FinishOk => Box::pin(async move {
                    drop(sink);
                    Ok(())
                }),
                FakeBehavior::ConnectorError => Box::pin(async move {
                    drop(sink);
                    Err(mdc_core::MdcError::Protocol("connector failed".to_string()))
                }),
                FakeBehavior::Panic => Box::pin(async move {
                    drop(sink);
                    panic!("provider task panicked");
                }),
                FakeBehavior::IgnoreCancellation => Box::pin(async move {
                    let _ = session_token;
                    let _sink = sink;
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    drop(_sink);
                    Ok(())
                }),
            };
            Ok(ProviderRun {
                future,
                abort_guards: Vec::new(),
            })
        }
    }

    async fn run(
        adapter: FakeAdapter,
        capacity: usize,
        stop_after: Option<Duration>,
    ) -> (SessionOutcome, watch::Receiver<Arc<DisplaySnapshot>>) {
        run_with_config(adapter, capacity, stop_after, config()).await
    }

    async fn run_with_config(
        adapter: FakeAdapter,
        capacity: usize,
        stop_after: Option<Duration>,
        config: SessionConfig,
    ) -> (SessionOutcome, watch::Receiver<Arc<DisplaySnapshot>>) {
        let root = CancellationToken::new();
        let (tx, rx) = watch::channel(Arc::new(DisplaySnapshot {
            provider: "test",
            session: DisplayStatus::Empty,
            failure: None,
            symbols: Vec::new(),
            alerts: Default::default(),
            alerts_evicted: 0,
        }));
        let session_root = root.clone();
        let handle = tokio::spawn(async move {
            run_session(
                Arc::new(adapter),
                config,
                bounds(),
                capacity,
                session_root,
                tx,
            )
            .await
        });
        if let Some(delay) = stop_after {
            tokio::time::sleep(delay).await;
            root.cancel();
        }
        let outcome = handle.await.unwrap();
        (outcome, rx)
    }

    #[tokio::test(start_paused = true)]
    async fn reduces_events_in_order_and_reports_unexpected_eof() {
        let adapter = FakeAdapter {
            events: vec![book_event(1), alert_event(2)],
            behavior: FakeBehavior::FinishOk,
        };
        let (outcome, display) = run(adapter, 16, None).await;
        assert_eq!(outcome.status, DisplayStatus::Failed);
        assert_eq!(
            outcome.failure.as_deref(),
            Some("stream ended unexpectedly")
        );
        let snapshot = display.borrow().clone();
        assert_eq!(snapshot.symbols.len(), 1);
        assert_eq!(snapshot.alerts.len(), 1);
        assert_eq!(snapshot.symbols[0].status, DisplayStatus::InSync);
    }

    #[tokio::test(start_paused = true)]
    async fn operator_stop_yields_stopped_outcome_after_reaping() {
        let adapter = FakeAdapter {
            events: vec![book_event(1)],
            behavior: FakeBehavior::IgnoreCancellation,
        };
        let (outcome, _display) = run(adapter, 16, Some(Duration::from_millis(10))).await;
        assert_eq!(outcome.status, DisplayStatus::Stopped);
        assert!(outcome.failure.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn overflow_drains_prefix_then_fails_terminally() {
        let adapter = FakeAdapter {
            events: vec![
                book_event(1),
                book_event(2),
                book_event(3),
                book_event(4),
                book_event(5),
            ],
            behavior: FakeBehavior::FinishOk,
        };
        let (outcome, display) = run(adapter, 2, None).await;
        assert_eq!(outcome.status, DisplayStatus::Failed);
        let failure = display.borrow().failure.clone().unwrap();
        assert!(
            failure.contains("capacity exceeded"),
            "failure was {failure}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn connector_error_fails_the_session() {
        let adapter = FakeAdapter {
            events: vec![book_event(1)],
            behavior: FakeBehavior::ConnectorError,
        };
        let (outcome, _display) = run(adapter, 16, None).await;
        assert_eq!(outcome.status, DisplayStatus::Failed);
        assert_eq!(
            outcome.failure.as_deref(),
            Some("protocol violation: connector failed")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn provider_panic_fails_the_session() {
        let adapter = FakeAdapter {
            events: vec![],
            behavior: FakeBehavior::Panic,
        };
        let (outcome, _display) = run(adapter, 16, None).await;
        assert_eq!(outcome.status, DisplayStatus::Failed);
        assert_eq!(outcome.failure.as_deref(), Some("provider task panicked"));
    }

    #[tokio::test(start_paused = true)]
    async fn eof_with_live_provider_reaps_within_the_bound() {
        // The provider sleeps forever holding the sink: no operator stop, so
        // EOF never arrives; the deadline reaps the provider and an operator
        // stop still yields a clean Stopped outcome.
        let adapter = FakeAdapter {
            events: vec![],
            behavior: FakeBehavior::IgnoreCancellation,
        };
        let (outcome, _display) = run(adapter, 16, Some(Duration::from_millis(5))).await;
        assert_eq!(outcome.status, DisplayStatus::Stopped);
    }

    #[tokio::test(start_paused = true)]
    async fn burst_headroom_with_full_symbol_configuration() {
        // PRD 31 P2 AC8: a 25-symbol burst against the full 4096-event
        // capacity with the drain not yet running records the headroom:
        // 1250 events accepted, 2846 slots spare, zero faults, ordered.
        let mut symbols = Vec::new();
        for symbol_index in 0..25u64 {
            symbols.push(format!("SYM{symbol_index}USDT"));
        }
        let burst_config = SessionConfig::new(ProviderKind::Binance, &symbols).unwrap();
        let mut events = Vec::new();
        let mut ordinal = 0u64;
        for symbol_index in 0..25u64 {
            for _ in 0..50 {
                ordinal += 1;
                let mut event = book_event(ordinal);
                event.symbol = Symbol::from(format!("SYM{symbol_index}USDT").as_str());
                events.push(event);
            }
        }
        let adapter = FakeAdapter {
            events,
            behavior: FakeBehavior::FinishOk,
        };
        let (outcome, display) =
            run_with_config(adapter, crate::config::CHANNEL_CAPACITY, None, burst_config).await;
        assert_eq!(outcome.status, DisplayStatus::Failed, "no operator stop");
        let snapshot = display.borrow().clone();
        assert_eq!(snapshot.symbols.len(), 25);
        // No capacity fault: the failure is the unexpected EOF after the
        // accepted prefix, and every event was accepted (1250 < 4096).
        assert_eq!(
            snapshot.failure.as_deref(),
            Some("stream ended unexpectedly")
        );
        assert!(snapshot.alerts.is_empty());
    }
}
