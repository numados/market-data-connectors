use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use mdc_core::MdcError;
use mdc_core::sink::MarketDataSink;
use mdc_core::{L2Delta, L2Snapshot, MarketDataKind, MarketEvent, PriceLevel};
use mdc_sinks::loss_intolerant::{
    DeliveryFaultReason, LossIntolerantSender, loss_intolerant_channel,
};
use rust_decimal::Decimal;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::{alert_event, bounds, config, event};
use crate::display::{DisplaySnapshot, DisplayStatus};
use crate::error::Error;
use crate::provider::{ProviderFuture, ProviderRun, SessionAdapter, SessionBounds};
use crate::reducer::Reducer;
use crate::session::run_session;

const CAPACITY: usize = 3;

fn snapshot_event(
    ordinal: u64,
    bid: u64,
    bid_size: u64,
    ask: u64,
    ask_size: u64,
) -> MarketEvent<MarketDataKind> {
    event(
        ordinal,
        MarketDataKind::L2Snapshot(L2Snapshot {
            bids: vec![PriceLevel::new(Decimal::from(bid), Decimal::from(bid_size))],
            asks: vec![PriceLevel::new(Decimal::from(ask), Decimal::from(ask_size))],
            last_update_id: Some(1),
        }),
    )
}

fn delta_event(
    ordinal: u64,
    bid: u64,
    bid_size: u64,
    ask: u64,
    ask_size: u64,
) -> MarketEvent<MarketDataKind> {
    event(
        ordinal,
        MarketDataKind::L2Delta(L2Delta {
            sequence: None,
            bids: vec![PriceLevel::new(Decimal::from(bid), Decimal::from(bid_size))],
            asks: vec![PriceLevel::new(Decimal::from(ask), Decimal::from(ask_size))],
        }),
    )
}

fn accepted_prefix() -> Vec<MarketEvent<MarketDataKind>> {
    vec![
        snapshot_event(1, 10, 1, 20, 2),
        alert_event(2),
        delta_event(3, 10, 3, 20, 4),
    ]
}

fn rejected_after_prefix() -> MarketEvent<MarketDataKind> {
    snapshot_event(4, 99, 9, 100, 9)
}

fn levels(symbol: &crate::display::SymbolDisplay, bids: bool) -> Vec<(Decimal, Decimal)> {
    let levels = if bids { &symbol.bids } else { &symbol.asks };
    levels
        .iter()
        .map(|level| (level.price, level.size))
        .collect()
}

fn assert_prefix_display(snapshot: &DisplaySnapshot, expected_status: DisplayStatus) {
    assert_eq!(snapshot.symbols.len(), 1);
    let symbol = &snapshot.symbols[0];
    assert_eq!(symbol.symbol.as_str(), "BTCUSDT");
    assert_eq!(symbol.status, expected_status);
    let quote = symbol.quote.as_ref().expect("accepted prefix quote");
    assert_eq!(quote.historical, expected_status != DisplayStatus::InSync);
    assert_eq!(quote.quote.bid, Decimal::from(10));
    assert_eq!(quote.quote.ask, Decimal::from(20));
    assert_eq!(quote.quote.bid_size, Decimal::from(3));
    assert_eq!(quote.quote.ask_size, Decimal::from(4));
    assert_eq!(
        levels(symbol, true),
        vec![(Decimal::from(10), Decimal::from(3))]
    );
    assert_eq!(
        levels(symbol, false),
        vec![(Decimal::from(20), Decimal::from(4))]
    );
}

#[tokio::test]
async fn channel_preserves_exact_prefix_then_one_fault_and_eof() {
    let token = CancellationToken::new();
    let (sender, mut stream) = loss_intolerant_channel(CAPACITY, 1, token.clone());
    let accepted = accepted_prefix();

    for event in &accepted {
        sender
            .on_event(event.clone())
            .expect("prefix event must be accepted");
    }
    let producer_error = sender
        .on_event(rejected_after_prefix())
        .expect_err("post-capacity event must become the terminal fault");
    assert!(token.is_cancelled());
    assert_eq!(stream.affected_connection(), 1);
    assert_eq!(
        producer_error.reason,
        DeliveryFaultReason::CapacityExceeded { capacity: CAPACITY }
    );
    assert_eq!(producer_error.stamp.ordinal, 4);

    let mut reducer = Reducer::new(&config(), bounds());
    let mut received_ordinals = Vec::new();
    for expected in &accepted {
        let item = stream.recv().await.expect("accepted prefix item");
        let received = item.expect("prefix item must not be a fault");
        assert_eq!(received.delivery.ordinal, expected.delivery.ordinal);
        assert_eq!(&received.kind, &expected.kind);
        received_ordinals.push(received.delivery.ordinal);
        reducer.apply_event(&received);
    }
    assert_eq!(received_ordinals, vec![1, 2, 3]);
    let prefix = reducer.snapshot();
    assert_eq!(prefix.session, DisplayStatus::InSync);
    assert_eq!(prefix.alerts.len(), 1);
    assert_eq!(prefix.alerts[0].summary, "resync completed (aligned to 2)");
    assert_prefix_display(&prefix, DisplayStatus::InSync);

    let fault = stream.recv().await.expect("terminal fault");
    let fault = fault.expect_err("fault slot must contain no success event");
    assert_eq!(fault, producer_error);
    assert!(
        stream.recv().await.is_none(),
        "fault must be followed by EOF"
    );
    assert!(
        stream.recv().await.is_none(),
        "EOF must not produce another item"
    );

    reducer.apply_fault(&fault);
    let final_snapshot = reducer.snapshot();
    assert_eq!(final_snapshot.session, DisplayStatus::Failed);
    assert_eq!(
        final_snapshot.failure.as_deref(),
        Some("loss-intolerant channel capacity exceeded (max 3 events)")
    );
    assert_prefix_display(&final_snapshot, DisplayStatus::Failed);
    drop(sender);
}

struct SinkGuard {
    _sink: LossIntolerantSender,
    reaped: Arc<AtomicBool>,
}

impl Drop for SinkGuard {
    fn drop(&mut self) {
        self.reaped.store(true, Ordering::SeqCst);
    }
}

struct OverflowAdapter {
    events: Vec<MarketEvent<MarketDataKind>>,
    accepted: Arc<AtomicUsize>,
    rejected: Arc<AtomicUsize>,
    cancellation_seen: Arc<AtomicBool>,
    reaped: Arc<AtomicBool>,
}

impl SessionAdapter for OverflowAdapter {
    fn bounds(&self) -> SessionBounds {
        bounds()
    }

    fn start(
        &self,
        _config: &crate::config::SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        for event in &self.events {
            match sink.on_event(event.clone()) {
                Ok(()) => {
                    self.accepted.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    self.rejected.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        self.cancellation_seen
            .store(session_token.is_cancelled(), Ordering::SeqCst);
        let guard = SinkGuard {
            _sink: sink,
            reaped: Arc::clone(&self.reaped),
        };
        let future: ProviderFuture = Box::pin(async move {
            let _guard = guard;
            session_token.cancelled().await;
            Err(MdcError::Protocol(
                "provider failed after cancellation".to_string(),
            ))
        });
        Ok(ProviderRun::Future(future))
    }
}

#[tokio::test]
async fn run_session_reduces_only_prefix_and_reaps_cancelled_producer() {
    let accepted = accepted_prefix();
    let mut events = accepted.clone();
    events.push(rejected_after_prefix());
    let accepted_count = Arc::new(AtomicUsize::new(0));
    let rejected_count = Arc::new(AtomicUsize::new(0));
    let cancellation_seen = Arc::new(AtomicBool::new(false));
    let reaped = Arc::new(AtomicBool::new(false));
    let adapter = OverflowAdapter {
        events,
        accepted: Arc::clone(&accepted_count),
        rejected: Arc::clone(&rejected_count),
        cancellation_seen: Arc::clone(&cancellation_seen),
        reaped: Arc::clone(&reaped),
    };
    let (display_tx, display_rx) = watch::channel(Arc::new(DisplaySnapshot {
        provider: "test",
        presentation: Default::default(),
        session: DisplayStatus::Empty,
        failure: None,
        symbols: Vec::new(),
        alerts: Default::default(),
        alerts_evicted: 0,
    }));

    let outcome = run_session(
        Arc::new(adapter),
        config(),
        bounds(),
        CAPACITY,
        CancellationToken::new(),
        display_tx,
    )
    .await;
    let snapshot = { display_rx.borrow().clone() };

    assert_eq!(accepted_count.load(Ordering::SeqCst), CAPACITY);
    assert_eq!(rejected_count.load(Ordering::SeqCst), 1);
    assert!(cancellation_seen.load(Ordering::SeqCst));
    assert!(reaped.load(Ordering::SeqCst));
    assert_eq!(outcome.status, DisplayStatus::Failed);
    assert_eq!(
        outcome.failure.as_deref(),
        Some("loss-intolerant channel capacity exceeded (max 3 events)")
    );
    assert_eq!(snapshot.session, DisplayStatus::Failed);
    assert_eq!(snapshot.failure, outcome.failure);
    assert_eq!(snapshot.alerts.len(), 1);
    assert_eq!(
        snapshot.alerts[0].summary,
        "resync completed (aligned to 2)"
    );
    assert_prefix_display(&snapshot, DisplayStatus::Failed);
}
