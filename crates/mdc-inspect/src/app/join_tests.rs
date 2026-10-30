#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::provider::ProviderRun;
use mdc_core::sink::MarketDataSink;
use mdc_core::{
    DeliveryStamp, IntegrityAlert, L2Snapshot, MarketDataKind, MarketEvent, PriceLevel, RunId,
    Symbol, UnixNanos, VenueId,
};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use rust_decimal::Decimal;

struct StartPanicAdapter;

impl SessionAdapter for StartPanicAdapter {
    fn bounds(&self) -> SessionBounds {
        ProviderKind::Test.session_bounds()
    }

    fn start(
        &self,
        _config: &SessionConfig,
        _sink: LossIntolerantSender,
        _session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        panic!("simulated start panic");
    }
}

struct BookAdapter;

impl SessionAdapter for BookAdapter {
    fn bounds(&self) -> SessionBounds {
        ProviderKind::Test.session_bounds()
    }

    fn start(
        &self,
        _config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        let kinds = [
            MarketDataKind::L2Snapshot(L2Snapshot {
                bids: vec![PriceLevel::new(Decimal::from(100), Decimal::TWO)],
                asks: vec![PriceLevel::new(Decimal::from(101), Decimal::from(3))],
                last_update_id: Some(1),
            }),
            MarketDataKind::IntegrityAlert(IntegrityAlert::ResyncCompleted {
                last_update_id: Some(1),
            }),
        ];
        for (index, kind) in kinds.into_iter().enumerate() {
            sink.on_event(MarketEvent {
                exchange_ts: Some(UnixNanos(1)),
                received_ts: UnixNanos(2),
                venue: VenueId::Binance,
                symbol: Symbol::from("BTCUSDT"),
                tag: 0,
                delivery: DeliveryStamp {
                    run: RunId::from_bytes([0; 16]),
                    connection_id: 1,
                    connection_generation: 1,
                    ordinal: index as u64 + 1,
                },
                source_sequence: None,
                kind,
            })
            .unwrap();
        }
        Ok(ProviderRun::Future(Box::pin(async move {
            let _sink = sink;
            session_token.cancelled().await;
            Ok(())
        })))
    }
}

fn config() -> SessionConfig {
    SessionConfig::new(ProviderKind::Test, &["BTCUSDT".to_string()]).unwrap()
}

fn buffer_text(buffer: &ratatui::buffer::Buffer) -> String {
    buffer
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect::<String>()
}

async fn observe_session(
    adapter: Arc<dyn SessionAdapter>,
    mut on_render: impl FnMut(&App),
) -> (ExitOutcome, Arc<DisplaySnapshot>, String) {
    let observed = Arc::new(Mutex::new(None));
    let observation = Arc::clone(&observed);
    let (input_sender, input_receiver) = tokio::sync::mpsc::channel(1);
    let input = futures_util::stream::unfold(input_receiver, |mut receiver| async {
        receiver.recv().await.map(|event| (event, receiver))
    })
    .boxed();
    let factory: Box<AdapterFactory> = Box::new(move |_| Ok(Arc::clone(&adapter)));
    let result = tokio::time::timeout(
        crate::config::SHUTDOWN_BOUND,
        run_with_input(
            App::new(Arc::new(Mutex::new(DiagnosticsRing::new()))),
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(140, 28)).unwrap(),
            Some(config()),
            &factory,
            input,
            move |terminal, app| {
                on_render(app);
                if app.state() == &AppState::Ended {
                    let snapshot = app.snapshot();
                    terminal.draw(|frame| {
                        crate::view::draw_main_at(
                            frame,
                            &snapshot,
                            app.ui(),
                            &app.diagnostics_snapshot(),
                            UnixNanos(3),
                        );
                    })?;
                    *observation.lock().unwrap() =
                        Some((snapshot, buffer_text(terminal.backend().buffer())));
                    input_sender
                        .try_send(Ok(crossterm::event::Event::Key(
                            crate::setup::key_for_test('q'),
                        )))
                        .unwrap();
                }
                Ok(())
            },
        ),
    )
    .await
    .expect("session outcome was not rendered within the shutdown bound")
    .unwrap();
    let (snapshot, text) = observed.lock().unwrap().take().unwrap();
    (result, snapshot, text)
}

#[tokio::test(start_paused = true)]
async fn outer_session_panic_is_rendered_as_failed() {
    let (result, snapshot, text) = observe_session(Arc::new(StartPanicAdapter), |_| {}).await;

    assert_eq!(result.session.unwrap().status, DisplayStatus::Failed);
    assert_eq!(snapshot.session, DisplayStatus::Failed);
    assert_eq!(snapshot.failure.as_deref(), Some("session task panicked"));
    assert_eq!(snapshot.symbols[0].status, DisplayStatus::Failed);
    assert!(text.contains("session: failed"));
    assert!(text.contains("failure: session task panicked"));
    assert!(!text.contains("simulated start panic"));
}

#[tokio::test(start_paused = true)]
async fn cancelled_outer_session_retains_historical_book() {
    let mut live = None;
    let (result, snapshot, text) = observe_session(Arc::new(BookAdapter), |app| {
        if app.state() == &AppState::Running && app.snapshot().session == DisplayStatus::InSync {
            live = Some(app.snapshot());
            app.session.as_ref().unwrap().abort();
        }
    })
    .await;

    assert_eq!(result.session.unwrap().status, DisplayStatus::Failed);
    assert_eq!(snapshot.session, DisplayStatus::Failed);
    assert_eq!(snapshot.failure.as_deref(), Some("session task cancelled"));
    let symbol = &snapshot.symbols[0];
    assert_eq!(symbol.status, DisplayStatus::Failed);
    let quote = symbol.quote.as_ref().unwrap();
    assert!(quote.historical);
    assert_eq!(quote.quote.bid, Decimal::from(100));
    assert_eq!(quote.quote.ask, Decimal::from(101));
    assert_eq!(symbol.bids[0].price, Decimal::from(100));
    assert_eq!(symbol.asks[0].price, Decimal::from(101));
    assert!(text.contains("session task cancelled"));
    assert!(text.contains("[HISTORICAL]"));
    let live = live.unwrap();
    assert_eq!(live.session, DisplayStatus::InSync);
    assert!(!live.symbols[0].quote.as_ref().unwrap().historical);
}

#[tokio::test(start_paused = true)]
async fn restarted_session_reconciles_its_own_join_failure() {
    let mut app = App::new(Arc::new(Mutex::new(DiagnosticsRing::new())));
    let factory: Box<AdapterFactory> = Box::new(|_| Ok(Arc::new(StartPanicAdapter)));
    app.start_session(config(), &factory).unwrap();
    app.reap_session().await;
    let previous = app.snapshot();
    assert_eq!(previous.session, DisplayStatus::Failed);

    app.start_session(config(), &factory).unwrap();
    assert_eq!(app.snapshot().session, DisplayStatus::Loading);
    assert_eq!(app.snapshot().failure, None);
    app.reap_session().await;
    assert_eq!(app.snapshot().session, DisplayStatus::Failed);
    assert_eq!(
        app.snapshot().failure.as_deref(),
        Some("session task panicked")
    );
    assert_eq!(previous.session, DisplayStatus::Failed);
}
