#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::provider::ProviderRun;

fn factory_unavailable() -> Box<AdapterFactory> {
    Box::new(|kind: ProviderKind| Err(Error::NotYetImplemented(kind.label().to_string())))
}

fn app() -> App {
    App::new(Arc::new(Mutex::new(DiagnosticsRing::new())))
}

#[test]
fn starts_on_setup_screen() {
    let app = app();
    assert_eq!(app.state(), &AppState::Setup);
    assert!(app.snapshot().session == DisplayStatus::Empty);
}

#[test]
fn start_without_adapter_stays_in_setup_with_error() {
    let mut app = app();
    let factory = factory_unavailable();
    app.handle_key_event(
        &crossterm::event::Event::Key(crate::setup::key_for_test('B')),
        &factory,
    );
    // Type a symbol and add it, then start.
    for c in "BTCUSDT".chars() {
        app.handle_key_event(
            &crossterm::event::Event::Key(crate::setup::key_for_test(c)),
            &factory,
        );
    }
    app.handle_key_event(
        &crossterm::event::Event::Key(crate::setup::key_for_test('\n')),
        &factory,
    );
    app.handle_action(Action::Start, &factory);
    assert_eq!(
        app.state(),
        &AppState::Setup,
        "NotYetImplemented keeps setup"
    );
}

#[test]
fn quit_from_setup_exits_immediately() {
    let mut app = app();
    let factory = factory_unavailable();
    app.handle_action(Action::Quit, &factory);
    assert_eq!(app.state(), &AppState::Exiting);
}

#[test]
fn setup_global_help_and_ctrl_c_are_not_swallowed_by_text_entry() {
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

    let mut app = app();
    let factory = factory_unavailable();
    let key = |code, modifiers| {
        crossterm::event::Event::Key(KeyEvent {
            code,
            modifiers,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        })
    };

    app.handle_key_event(&key(KeyCode::Char('?'), KeyModifiers::NONE), &factory);
    assert!(app.help_open());
    app.handle_action(Action::ToggleHelp, &factory);
    for code in ['q', 's', 'c'] {
        app.handle_key_event(&key(KeyCode::Char(code), KeyModifiers::NONE), &factory);
    }
    assert_eq!(app.setup().input(), "qsc");
    app.handle_key_event(&key(KeyCode::Char('c'), KeyModifiers::CONTROL), &factory);
    assert_eq!(app.state(), &AppState::Exiting);
}

#[cfg(feature = "binance")]
#[tokio::test]
async fn repeated_ctrl_c_forces_then_reaps_the_owned_session() {
    struct SleepingAdapter;

    impl SessionAdapter for SleepingAdapter {
        fn bounds(&self) -> SessionBounds {
            SessionBounds {
                max_symbols: 25,
                projector_depth: 100,
                affected_connection: 1,
            }
        }

        fn start(
            &self,
            _config: &SessionConfig,
            sink: mdc_sinks::loss_intolerant::LossIntolerantSender,
            _session_token: CancellationToken,
        ) -> Result<ProviderRun, Error> {
            Ok(ProviderRun::Future(Box::pin(async move {
                let _sink = sink;
                futures_util::future::pending::<Result<(), mdc_core::MdcError>>().await
            })))
        }
    }

    let mut app = app();
    let factory: Box<AdapterFactory> = Box::new(|_| Ok(Arc::new(SleepingAdapter)));
    let config = SessionConfig::new(ProviderKind::Binance, &["BTCUSDT".to_string()]).unwrap();
    app.start_session(config, &factory).unwrap();

    app.handle_action(Action::ForceQuit, &factory);
    assert_eq!(app.state(), &AppState::Stopping);
    assert!(app.root.is_cancelled());
    assert!(!app.force_stop.as_ref().unwrap().is_cancelled());
    assert!(!app.force_stop_requested);
    app.handle_action(Action::ForceQuit, &factory);
    assert!(app.force_stop_requested);
    app.reap_session().await;

    assert_eq!(app.state(), &AppState::Exiting);
    assert_eq!(app.last_outcome().unwrap().status, DisplayStatus::Stopped);
    assert!(app.session.is_none());
}

#[cfg(feature = "binance")]
#[tokio::test(flavor = "current_thread")]
async fn direct_stop_reap_enter_restarts_the_original_configuration() {
    struct CooperativeAdapter {
        starts: Arc<Mutex<Vec<SessionConfig>>>,
    }

    impl SessionAdapter for CooperativeAdapter {
        fn bounds(&self) -> SessionBounds {
            SessionBounds {
                max_symbols: 25,
                projector_depth: 100,
                affected_connection: 1,
            }
        }

        fn start(
            &self,
            config: &SessionConfig,
            sink: mdc_sinks::loss_intolerant::LossIntolerantSender,
            session_token: CancellationToken,
        ) -> Result<ProviderRun, Error> {
            self.starts.lock().unwrap().push(config.clone());
            Ok(ProviderRun::Future(Box::pin(async move {
                let _sink = sink;
                session_token.cancelled().await;
                Ok(())
            })))
        }
    }

    let starts = Arc::new(Mutex::new(Vec::new()));
    let factory: Box<AdapterFactory> = {
        let starts = Arc::clone(&starts);
        Box::new(move |kind| {
            assert_eq!(kind, ProviderKind::Binance);
            Ok(Arc::new(CooperativeAdapter {
                starts: Arc::clone(&starts),
            }))
        })
    };
    let config = SessionConfig::new(ProviderKind::Binance, &["BTCUSDT".to_string()]).unwrap();
    let mut app = app();
    app.handle_action(Action::FocusNext, &factory);
    app.handle_action(Action::PageDown, &factory);

    app.start_session(config.clone(), &factory).unwrap();
    assert_eq!(app.state(), &AppState::Running);
    assert_eq!(app.ui(), &UiState::new());
    assert_eq!(app.setup().selected_provider(), Some(ProviderKind::Binance));
    assert_eq!(app.setup().symbols(), ["BTCUSDT"]);
    assert!(matches!(
        app.start_session(config.clone(), &factory),
        Err(Error::Session(_))
    ));
    app.handle_action(Action::Stop, &factory);
    assert_eq!(app.state(), &AppState::Stopping);
    assert!(matches!(
        app.start_session(config.clone(), &factory),
        Err(Error::Session(_))
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), app.reap_session())
        .await
        .unwrap();
    assert_eq!(app.state(), &AppState::Ended);

    app.handle_action(Action::Start, &factory);
    assert_eq!(app.state(), &AppState::Running);
    app.handle_action(Action::Stop, &factory);
    tokio::time::timeout(std::time::Duration::from_secs(1), app.reap_session())
        .await
        .unwrap();
    assert_eq!(app.state(), &AppState::Ended);

    app.handle_action(Action::Start, &factory_unavailable());
    assert_eq!(app.state(), &AppState::Setup);
    assert!(app.setup().error().is_some());
    assert_eq!(starts.lock().unwrap().as_slice(), &[config.clone(), config]);
}

#[test]
fn right_overshoot_then_left_selects_the_first_symbol() {
    use crate::display::{ProviderPresentation, SymbolDisplay};

    let mut app = app();
    let (_, display) = tokio::sync::watch::channel(Arc::new(DisplaySnapshot {
        provider: "Binance Spot",
        presentation: ProviderPresentation::default(),
        session: DisplayStatus::InSync,
        failure: None,
        symbols: vec![
            SymbolDisplay {
                symbol: mdc_core::Symbol::from("BTCUSDT"),
                status: DisplayStatus::InSync,
                quote: None,
                rate: None,
                bids: Vec::new(),
                asks: Vec::new(),
                unavailability: None,
            },
            SymbolDisplay {
                symbol: mdc_core::Symbol::from("ETHUSDT"),
                status: DisplayStatus::InSync,
                quote: None,
                rate: None,
                bids: Vec::new(),
                asks: Vec::new(),
                unavailability: None,
            },
        ],
        alerts: Default::default(),
        alerts_evicted: 0,
    }));
    app.display = Some(display);
    let factory = factory_unavailable();

    app.handle_action(Action::NextSymbol, &factory);
    app.handle_action(Action::NextSymbol, &factory);
    app.handle_action(Action::PrevSymbol, &factory);

    assert_eq!(app.ui().selected_symbol, 0);
}

#[cfg(feature = "binance")]
#[tokio::test(start_paused = true)]
async fn input_error_reaps_a_running_session_before_returning() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Probe(Arc<AtomicBool>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct SleepingAdapter {
        dropped: Arc<AtomicBool>,
    }
    impl SessionAdapter for SleepingAdapter {
        fn bounds(&self) -> SessionBounds {
            SessionBounds {
                max_symbols: 25,
                projector_depth: 100,
                affected_connection: 1,
            }
        }

        fn start(
            &self,
            _config: &SessionConfig,
            sink: mdc_sinks::loss_intolerant::LossIntolerantSender,
            _session_token: CancellationToken,
        ) -> Result<ProviderRun, Error> {
            let probe = Probe(Arc::clone(&self.dropped));
            Ok(ProviderRun::Future(Box::pin(async move {
                let _probe = probe;
                let _sink = sink;
                futures_util::future::pending::<Result<(), mdc_core::MdcError>>().await
            })))
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let factory: Box<AdapterFactory> = {
        let dropped = Arc::clone(&dropped);
        Box::new(move |_| {
            Ok(Arc::new(SleepingAdapter {
                dropped: Arc::clone(&dropped),
            }))
        })
    };
    let input = futures_util::stream::iter(vec![Err(io::Error::other("input failed"))]);
    let result = run_with_input(
        app(),
        ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap(),
        Some(SessionConfig::new(ProviderKind::Binance, &["BTCUSDT".to_string()]).unwrap()),
        &factory,
        input,
        |_terminal, _app| Ok(()),
    )
    .await;

    assert!(matches!(result, Err(Error::TerminalIo(error)) if error.to_string() == "input failed"));
    assert!(dropped.load(Ordering::SeqCst));
}

#[cfg(feature = "binance")]
#[tokio::test(start_paused = true)]
async fn render_error_reaps_a_running_session_before_returning() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Probe(Arc<AtomicBool>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct SleepingAdapter {
        dropped: Arc<AtomicBool>,
    }
    impl SessionAdapter for SleepingAdapter {
        fn bounds(&self) -> SessionBounds {
            SessionBounds {
                max_symbols: 25,
                projector_depth: 100,
                affected_connection: 1,
            }
        }

        fn start(
            &self,
            _config: &SessionConfig,
            sink: mdc_sinks::loss_intolerant::LossIntolerantSender,
            _session_token: CancellationToken,
        ) -> Result<ProviderRun, Error> {
            let probe = Probe(Arc::clone(&self.dropped));
            Ok(ProviderRun::Future(Box::pin(async move {
                let _probe = probe;
                let _sink = sink;
                futures_util::future::pending::<Result<(), mdc_core::MdcError>>().await
            })))
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let factory: Box<AdapterFactory> = {
        let dropped = Arc::clone(&dropped);
        Box::new(move |_| {
            Ok(Arc::new(SleepingAdapter {
                dropped: Arc::clone(&dropped),
            }))
        })
    };
    let result = run_with_input(
        app(),
        ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap(),
        Some(SessionConfig::new(ProviderKind::Binance, &["BTCUSDT".to_string()]).unwrap()),
        &factory,
        futures_util::stream::empty(),
        |_terminal, _app| Err(io::Error::other("render failed")),
    )
    .await;

    assert!(
        matches!(result, Err(Error::TerminalIo(error)) if error.to_string() == "render failed")
    );
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn initial_draw_is_not_repeated_before_the_first_cadence() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let renders = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&renders);
    let input = futures_util::stream::once(async {
        tokio::time::sleep(RENDER_CADENCE / 2).await;
        Ok(crossterm::event::Event::Key(
            crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char('c'),
                crossterm::event::KeyModifiers::CONTROL,
            ),
        ))
    })
    .boxed();
    run_with_input(
        app(),
        ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap(),
        None,
        &factory_unavailable(),
        input,
        move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(renders.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn input_flood_does_not_trigger_render_per_event() {
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let renders = Arc::new(AtomicUsize::new(0));
    let key = |code, modifiers| {
        crossterm::event::Event::Key(KeyEvent {
            code,
            modifiers,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        })
    };
    let mut events = Vec::new();
    for _ in 0..100 {
        events.push(Ok(key(KeyCode::Char('x'), KeyModifiers::NONE)));
    }
    events.push(Ok(key(KeyCode::Char('c'), KeyModifiers::CONTROL)));
    let factory = factory_unavailable();
    let render_count = Arc::clone(&renders);
    let result = run_with_input(
        app(),
        ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap(),
        None,
        &factory,
        futures_util::stream::iter(events),
        move |_terminal, _app| {
            render_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await;

    assert!(result.is_ok());
    assert!(renders.load(Ordering::SeqCst) < 10);
}

#[cfg(feature = "binance")]
#[tokio::test]
async fn dropping_app_aborts_owned_session() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Probe(Arc<AtomicBool>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct SleepingAdapter {
        dropped: Arc<AtomicBool>,
    }
    impl SessionAdapter for SleepingAdapter {
        fn bounds(&self) -> SessionBounds {
            SessionBounds {
                max_symbols: 25,
                projector_depth: 100,
                affected_connection: 1,
            }
        }

        fn start(
            &self,
            _config: &SessionConfig,
            sink: mdc_sinks::loss_intolerant::LossIntolerantSender,
            _session_token: CancellationToken,
        ) -> Result<ProviderRun, Error> {
            let probe = Probe(Arc::clone(&self.dropped));
            Ok(ProviderRun::Future(Box::pin(async move {
                let _probe = probe;
                let _sink = sink;
                futures_util::future::pending::<Result<(), mdc_core::MdcError>>().await
            })))
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let factory: Box<AdapterFactory> = {
        let dropped = Arc::clone(&dropped);
        Box::new(move |_| {
            Ok(Arc::new(SleepingAdapter {
                dropped: Arc::clone(&dropped),
            }))
        })
    };
    let mut app = app();
    app.start_session(
        SessionConfig::new(ProviderKind::Binance, &["BTCUSDT".to_string()]).unwrap(),
        &factory,
    )
    .unwrap();
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    drop(app);
    tokio::task::yield_now().await;
    assert!(dropped.load(Ordering::SeqCst));
}

#[cfg(feature = "bitstamp")]
#[test]
fn provider_validation_failure_keeps_setup_before_spawn() {
    let mut app = app();
    let factory: Box<AdapterFactory> = Box::new(|kind| {
        assert_eq!(kind, ProviderKind::Bitstamp);
        Ok(Arc::new(
            crate::provider::bitstamp::BitstampAdapter::with_endpoints(
                url::Url::parse("http://invalid-websocket.example").unwrap(),
                url::Url::parse("http://127.0.0.1:1").unwrap(),
            ),
        ))
    });
    let config = SessionConfig::new(ProviderKind::Bitstamp, &["btcusd".to_string()]).unwrap();

    let error = app.start_session(config, &factory).unwrap_err();
    assert!(matches!(error, Error::ProviderConfig { .. }));
    assert_eq!(app.state(), &AppState::Setup);
    assert!(app.session.is_none());
}

#[test]
fn configure_and_help_and_focus_transitions() {
    let mut app = app();
    let factory = factory_unavailable();
    app.handle_action(Action::ToggleHelp, &factory);
    assert!(app.help_open());
    app.handle_action(Action::ToggleHelp, &factory);
    assert!(!app.help_open());
    app.handle_action(Action::Configure, &factory);
    assert_eq!(app.state(), &AppState::Setup);
    app.handle_action(Action::FocusNext, &factory);
    assert_eq!(app.ui().focus, crate::display::Focus::Book);
    app.handle_action(Action::FocusPrev, &factory);
    assert_eq!(app.ui().focus, crate::display::Focus::Symbols);
}

#[test]
fn empty_book_cannot_accumulate_scroll_offsets() {
    let mut app = app();
    let factory = factory_unavailable();
    app.handle_action(Action::FocusNext, &factory);
    app.handle_action(Action::ScrollUp, &factory);
    assert_eq!(app.ui().book_scroll, 0);
    app.handle_action(Action::PageDown, &factory);
    assert_eq!(app.ui().book_scroll, 0);
}

#[test]
fn adapter_trait_objects_are_object_safe_enough_for_factories() {
    // Compile-time shape check: a factory returns adapters.
    fn make() -> Result<Arc<dyn SessionAdapter>, Error> {
        struct NeverAdapter;
        impl SessionAdapter for NeverAdapter {
            fn bounds(&self) -> SessionBounds {
                SessionBounds {
                    max_symbols: 25,
                    projector_depth: 100,
                    affected_connection: 1,
                }
            }

            fn start(
                &self,
                _config: &SessionConfig,
                _sink: mdc_sinks::loss_intolerant::LossIntolerantSender,
                _token: CancellationToken,
            ) -> Result<ProviderRun, Error> {
                Err(Error::NotYetImplemented("never".to_string()))
            }
        }
        Ok(Arc::new(NeverAdapter))
    }
    assert!(make().is_ok());
}
