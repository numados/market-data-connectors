#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::diagnostics::DiagnosticsRing;
use crate::display::{LevelView, ProviderPresentation, SymbolDisplay};
use rust_decimal::Decimal;

fn app() -> App {
    App::new(Arc::new(Mutex::new(DiagnosticsRing::new())))
}

fn factory() -> Box<AdapterFactory> {
    Box::new(|kind| Err(Error::NotYetImplemented(kind.label().to_string())))
}

fn symbol(name: &str, bids: usize, asks: usize) -> SymbolDisplay {
    let level = || LevelView {
        price: Decimal::ONE,
        size: Decimal::ONE,
    };
    SymbolDisplay {
        symbol: mdc_core::Symbol::from(name),
        status: DisplayStatus::InSync,
        quote: None,
        rate: None,
        bids: (0..bids).map(|_| level()).collect(),
        asks: (0..asks).map(|_| level()).collect(),
        unavailability: None,
    }
}

fn display(symbols: Vec<SymbolDisplay>) -> Arc<DisplaySnapshot> {
    Arc::new(DisplaySnapshot {
        provider: "test",
        presentation: ProviderPresentation::default(),
        session: DisplayStatus::InSync,
        failure: None,
        symbols,
        alerts: Default::default(),
        alerts_evicted: 0,
    })
}

#[test]
fn help_visibility_matches_the_rendering_state() {
    let mut app = app();
    let factory = factory();
    for visible in [true, false] {
        app.handle_action(Action::ToggleHelp, &factory);
        assert_eq!(app.help_open(), visible);
        assert_eq!(app.ui().help_open, visible);
    }
}

#[tokio::test(start_paused = true)]
async fn session_start_preserves_open_help() {
    struct IdleAdapter;
    impl SessionAdapter for IdleAdapter {
        fn bounds(&self) -> SessionBounds {
            ProviderKind::Test.session_bounds()
        }

        fn start(
            &self,
            _config: &SessionConfig,
            sink: mdc_sinks::loss_intolerant::LossIntolerantSender,
            session_token: CancellationToken,
        ) -> Result<crate::provider::ProviderRun, Error> {
            Ok(crate::provider::ProviderRun::Future(Box::pin(async move {
                let _sink = sink;
                session_token.cancelled().await;
                Ok(())
            })))
        }
    }

    let mut app = app();
    let factory: Box<AdapterFactory> = Box::new(|_| Ok(Arc::new(IdleAdapter)));
    app.handle_action(Action::FocusNext, &factory);
    app.handle_action(Action::ToggleHelp, &factory);
    app.start_session(
        SessionConfig::new(ProviderKind::Test, &["BTCUSDT".to_string()]).unwrap(),
        &factory,
    )
    .unwrap();
    let help_stayed_open = app.help_open();
    app.handle_action(Action::Stop, &factory);
    app.reap_session().await;

    assert!(help_stayed_open);
    assert!(app.ui().help_open);
    assert_eq!(app.ui().focus, crate::display::Focus::Symbols);
    assert_eq!(app.last_outcome().unwrap().status, DisplayStatus::Stopped);
}

#[test]
fn book_scroll_is_bounded_reversible_and_resets_on_symbol_or_data_shrink() {
    let mut app = app();
    let (sender, receiver) = tokio::sync::watch::channel(display(vec![
        symbol("BTCUSDT", 1, 1),
        symbol("ETHUSDT", 1, 0),
    ]));
    app.display = Some(receiver);
    let factory = factory();

    app.handle_action(Action::FocusNext, &factory);
    app.handle_action(Action::PageDown, &factory);
    assert_eq!(app.ui().book_scroll, 1);
    app.handle_action(Action::ScrollUp, &factory);
    assert_eq!(app.ui().book_scroll, 0);
    app.handle_action(Action::PageDown, &factory);
    app.handle_action(Action::NextSymbol, &factory);
    assert_eq!(app.ui().selected_symbol, 1);
    assert_eq!(app.ui().book_scroll, 0);

    sender.send(display(vec![symbol("BTCUSDT", 0, 0)])).unwrap();
    app.handle_action(Action::ScrollDown, &factory);
    assert_eq!(app.ui().selected_symbol, 0);
    assert_eq!(app.ui().book_scroll, 0);
}
