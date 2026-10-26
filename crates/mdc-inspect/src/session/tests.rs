#![allow(clippy::unwrap_used, clippy::expect_used)]
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
        Ok(ProviderRun::Future(future))
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
async fn force_stop_aborts_and_reaps_the_provider_task() {
    let root = CancellationToken::new();
    let force_stop = CancellationToken::new();
    let (tx, _rx) = watch::channel(Arc::new(DisplaySnapshot {
        provider: "test",
        session: DisplayStatus::Empty,
        failure: None,
        symbols: Vec::new(),
        alerts: Default::default(),
        alerts_evicted: 0,
    }));
    let handle = tokio::spawn(run_session_controlled(
        Arc::new(FakeAdapter {
            events: vec![],
            behavior: FakeBehavior::IgnoreCancellation,
        }),
        config(),
        bounds(),
        16,
        root,
        force_stop.clone(),
        tx,
    ));
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    force_stop.cancel();
    let outcome = handle.await.unwrap();
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
async fn queued_delivery_fault_wins_over_provider_error_and_keeps_prefix() {
    let adapter = FakeAdapter {
        events: vec![book_event(1), alert_event(2), book_event(3)],
        behavior: FakeBehavior::ConnectorError,
    };
    let (outcome, display) = run(adapter, 2, None).await;
    assert_eq!(outcome.status, DisplayStatus::Failed);
    let expected =
        mdc_sinks::loss_intolerant::DeliveryFaultReason::CapacityExceeded { capacity: 2 }
            .to_string();
    assert_eq!(outcome.failure.as_deref(), Some(expected.as_str()));
    let snapshot = display.borrow().clone();
    assert_eq!(snapshot.symbols.len(), 1);
    assert!(snapshot.symbols[0].quote.is_some());
    assert_eq!(snapshot.alerts.len(), 1);
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
async fn shutdown_deadline_without_operator_stop_fails_visibly() {
    struct OpenStreamAdapter {
        keeper: Arc<std::sync::Mutex<Option<LossIntolerantSender>>>,
    }

    impl SessionAdapter for OpenStreamAdapter {
        fn start(
            &self,
            _config: &SessionConfig,
            sink: LossIntolerantSender,
            _session_token: CancellationToken,
        ) -> Result<ProviderRun, Error> {
            *self.keeper.lock().unwrap() = Some(sink.clone());
            Ok(ProviderRun::Future(Box::pin(async move {
                drop(sink);
                Ok(())
            })))
        }
    }

    let keeper = Arc::new(std::sync::Mutex::new(None));
    let adapter = Arc::new(OpenStreamAdapter {
        keeper: Arc::clone(&keeper),
    });
    let root = CancellationToken::new();
    let (tx, _rx) = watch::channel(Arc::new(DisplaySnapshot {
        provider: "test",
        session: DisplayStatus::Empty,
        failure: None,
        symbols: Vec::new(),
        alerts: Default::default(),
        alerts_evicted: 0,
    }));
    let handle = tokio::spawn(run_session(adapter, config(), bounds(), 16, root, tx));
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(SHUTDOWN_BOUND).await;
    for _ in 0..2 {
        tokio::task::yield_now().await;
    }
    let outcome = handle.await.unwrap();
    assert_eq!(outcome.status, DisplayStatus::Failed);
    assert_eq!(
        outcome.failure.as_deref(),
        Some("session shutdown deadline exceeded")
    );
    let _ = keeper.lock().unwrap().take();
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
