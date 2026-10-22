//! Top-level application: UI state machine, fixed render cadence, session
//! ownership, and start/stop/quit coordination (PRD 31 §3.6–3.7).
//!
//! The session join is never awaited inline by the input path: stop is a
//! state ([`AppState::Stopping`]) observed through a concurrent select arm,
//! and a second `Ctrl-C` while stopping forces task abort (requirement 152).

use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use crate::action::{Action, map_event};
use crate::config::{CHANNEL_CAPACITY, RENDER_CADENCE, SessionConfig};
use crate::diagnostics::{DiagnosticsRing, DiagnosticsSnapshot};
use crate::display::{DisplaySnapshot, DisplayStatus, UiState};
use crate::error::Error;
use crate::provider::{ProviderKind, SessionAdapter, SessionBounds};
use crate::session::{SessionOutcome, run_session};
use crate::setup::{SetupAction, SetupState};

/// Builds the adapter for a provider kind; injectable in tests.
pub type AdapterFactory =
    dyn Fn(ProviderKind) -> Result<Arc<dyn SessionAdapter>, Error> + Send + Sync;

/// Application state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppState {
    /// Guided configuration screen.
    Setup,
    /// Session active.
    Running,
    /// Stop requested; session draining or being reaped.
    Stopping,
    /// Session reached its terminal outcome.
    Ended,
    /// Application is shutting down.
    Exiting,
}

/// Handle returned to the process entry point after the loop ends.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExitOutcome {
    /// Terminal outcome of the last session, when one ran.
    pub session: Option<SessionOutcome>,
}

/// The interactive application.
pub struct App {
    root: CancellationToken,
    state: AppState,
    setup: SetupState,
    ui: UiState,
    help_open: bool,
    display: Option<watch::Receiver<Arc<DisplaySnapshot>>>,
    session: Option<JoinHandle<SessionOutcome>>,
    session_token: Option<CancellationToken>,
    diagnostics: Arc<Mutex<DiagnosticsRing>>,
    last_outcome: Option<SessionOutcome>,
    exit_requested: bool,
}

impl App {
    /// Builds an application starting on the setup screen.
    pub fn new(diagnostics: Arc<Mutex<DiagnosticsRing>>) -> Self {
        Self {
            root: CancellationToken::new(),
            state: AppState::Setup,
            setup: SetupState::new(),
            ui: UiState::new(),
            help_open: false,
            display: None,
            session: None,
            session_token: None,
            diagnostics,
            last_outcome: None,
            exit_requested: false,
        }
    }

    /// Current application state.
    pub fn state(&self) -> &AppState {
        &self.state
    }

    /// Latest display snapshot, or an idle empty snapshot before any session.
    pub fn snapshot(&self) -> Arc<DisplaySnapshot> {
        match &self.display {
            Some(receiver) => receiver.borrow().clone(),
            None => Arc::new(DisplaySnapshot {
                provider: self
                    .setup
                    .selected_provider()
                    .map(|kind| kind.label())
                    .unwrap_or(""),
                session: DisplayStatus::Empty,
                failure: None,
                symbols: Vec::new(),
                alerts: Default::default(),
                alerts_evicted: 0,
            }),
        }
    }

    /// Diagnostics snapshot for rendering.
    pub fn diagnostics_snapshot(&self) -> DiagnosticsSnapshot {
        match self.diagnostics.lock() {
            Ok(guard) => guard.snapshot(),
            Err(poisoned) => poisoned.into_inner().snapshot(),
        }
    }

    /// Starts a session for the configuration, using the factory to build
    /// the adapter. Failures keep the current state and return the error.
    /// A new session cannot start before the previous one is reaped
    /// (requirement 97).
    pub fn start_session(
        &mut self,
        config: SessionConfig,
        factory: &AdapterFactory,
    ) -> Result<(), Error> {
        if self.session.is_some() {
            return Err(Error::Session("a session is still active".to_string()));
        }
        let adapter = factory(config.provider)?;
        let bounds: SessionBounds = config.provider.session_bounds();
        let session_token = self.root.child_token();
        let (display_tx, display_rx) = watch::channel(Arc::new(DisplaySnapshot {
            provider: config.provider.label(),
            session: DisplayStatus::Loading,
            failure: None,
            symbols: Vec::new(),
            alerts: Default::default(),
            alerts_evicted: 0,
        }));
        let handle = tokio::spawn(run_session(
            adapter,
            config,
            bounds,
            CHANNEL_CAPACITY,
            session_token.clone(),
            display_tx,
        ));
        self.display = Some(display_rx);
        self.session = Some(handle);
        self.session_token = Some(session_token);
        self.state = AppState::Running;
        Ok(())
    }

    /// Feeds a raw input event: text-entry keys go to the setup screen while
    /// it is active; other states use the action mapping.
    pub fn handle_key_event(&mut self, event: &crossterm::event::Event, factory: &AdapterFactory) {
        if self.state == AppState::Setup && !self.help_open {
            if let crossterm::event::Event::Key(key) = event {
                if matches!(self.setup.handle_key(key), SetupAction::Start) {
                    self.handle_action(Action::Start, factory);
                }
                return;
            }
        }
        if let Some(action) = map_event(event) {
            self.handle_action(action, factory);
        }
    }

    /// Handles one action, advancing the state machine.
    pub fn handle_action(&mut self, action: Action, factory: &AdapterFactory) {
        match action {
            Action::ForceQuit => {
                // Quit from every interactive state; while a session is
                // stopping, force task abort (requirement 152).
                if let Some(handle) = self.session.as_ref() {
                    handle.abort();
                }
                if let Some(token) = self.session_token.as_ref() {
                    token.cancel();
                }
                self.root.cancel();
                self.exit_requested = true;
                self.state = AppState::Exiting;
            }
            Action::Quit => match self.state {
                AppState::Running | AppState::Stopping => {
                    if let Some(token) = self.session_token.as_ref() {
                        token.cancel();
                    }
                    self.exit_requested = true;
                    self.state = AppState::Stopping;
                }
                _ => {
                    self.root.cancel();
                    self.exit_requested = true;
                    self.state = AppState::Exiting;
                }
            },
            Action::Start => {
                if matches!(self.state, AppState::Setup | AppState::Ended) {
                    match self.setup.start_config() {
                        Ok(config) => {
                            if let Err(error) = self.start_session(config, factory) {
                                self.setup_note_error(error.to_string());
                            }
                        }
                        Err(error) => self.setup_note_error(error.to_string()),
                    }
                }
            }
            Action::Stop => {
                if self.state == AppState::Running {
                    if let Some(token) = self.session_token.as_ref() {
                        token.cancel();
                    }
                    self.state = AppState::Stopping;
                }
            }
            Action::Configure => {
                if matches!(self.state, AppState::Setup | AppState::Ended) {
                    self.state = AppState::Setup;
                }
            }
            Action::ToggleHelp => self.help_open = !self.help_open,
            Action::FocusNext => self.focus_next(),
            Action::FocusPrev => self.focus_prev(),
            Action::NextSymbol => {
                self.ui.selected_symbol = self.ui.selected_symbol.saturating_add(1)
            }
            Action::PrevSymbol => {
                self.ui.selected_symbol = self.ui.selected_symbol.saturating_sub(1);
            }
            Action::ScrollUp => self.scroll_up(),
            Action::ScrollDown => self.scroll_down(),
            Action::PageUp => {
                for _ in 0..10 {
                    self.scroll_up();
                }
            }
            Action::PageDown => {
                for _ in 0..10 {
                    self.scroll_down();
                }
            }
            Action::Resize(_, _) => {
                // Layout recomputes on the next render.
            }
        }
    }

    fn setup_note_error(&mut self, message: String) {
        // Session-start failures keep the setup screen; the message rides on
        // the screen error line via a synthetic entry.
        self.setup.note_error(message);
    }

    /// Marks the session finished; called by the loop when the join resolves.
    pub fn on_session_finished(&mut self, outcome: SessionOutcome) {
        self.session = None;
        self.session_token = None;
        self.last_outcome = Some(outcome);
        if self.exit_requested {
            self.state = AppState::Exiting;
        } else {
            self.state = AppState::Ended;
        }
    }

    fn focus_next(&mut self) {
        use crate::display::Focus;
        self.ui.focus = match self.ui.focus {
            Focus::Symbols => Focus::Book,
            Focus::Book => Focus::Alerts,
            Focus::Alerts => Focus::Diagnostics,
            Focus::Diagnostics => Focus::Symbols,
        };
    }

    fn focus_prev(&mut self) {
        use crate::display::Focus;
        self.ui.focus = match self.ui.focus {
            Focus::Symbols => Focus::Diagnostics,
            Focus::Book => Focus::Symbols,
            Focus::Alerts => Focus::Book,
            Focus::Diagnostics => Focus::Alerts,
        };
    }

    fn scroll_up(&mut self) {
        use crate::display::Focus;
        match self.ui.focus {
            Focus::Book => self.ui.book_scroll = self.ui.book_scroll.saturating_sub(1),
            Focus::Alerts => self.ui.alerts_scroll = self.ui.alerts_scroll.saturating_sub(1),
            Focus::Diagnostics => {
                self.ui.diagnostics_scroll = self.ui.diagnostics_scroll.saturating_sub(1);
            }
            Focus::Symbols => self.ui.selected_symbol = self.ui.selected_symbol.saturating_sub(1),
        }
    }

    fn scroll_down(&mut self) {
        use crate::display::Focus;
        match self.ui.focus {
            Focus::Book => self.ui.book_scroll += 1,
            Focus::Alerts => self.ui.alerts_scroll += 1,
            Focus::Diagnostics => self.ui.diagnostics_scroll += 1,
            Focus::Symbols => self.ui.selected_symbol += 1,
        }
    }

    /// Setup screen state (rendering).
    pub fn setup(&self) -> &SetupState {
        &self.setup
    }

    /// UI state (rendering).
    pub fn ui(&self) -> &UiState {
        &self.ui
    }

    /// Whether the help overlay is open.
    pub fn help_open(&self) -> bool {
        self.help_open
    }

    /// Whether exit was requested.
    pub fn exit_requested(&self) -> bool {
        self.exit_requested
    }

    /// The finished session outcome, if any.
    pub fn last_outcome(&self) -> Option<&SessionOutcome> {
        self.last_outcome.as_ref()
    }
}

/// Runs the interactive loop until exit. `factory` builds provider adapters;
/// `direct` starts a session immediately (CLI direct-start mode).
pub async fn run(
    mut app: App,
    mut terminal: ratatui::Terminal<impl ratatui::backend::Backend>,
    direct: Option<SessionConfig>,
    factory: &AdapterFactory,
) -> Result<ExitOutcome, Error> {
    if let Some(config) = direct {
        app.start_session(config, factory)?;
    }
    let mut input = crossterm::event::EventStream::new();
    let mut ticker = interval(RENDER_CADENCE);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let snapshot = app.snapshot();
        let diagnostics = app.diagnostics_snapshot();
        terminal
            .draw(|frame| {
                if app.state() == &AppState::Setup {
                    crate::view::draw_setup(frame, app.setup());
                } else {
                    crate::view::draw_main(frame, &snapshot, app.ui(), &diagnostics);
                }
                if app.help_open() {
                    crate::view::draw_help(frame);
                }
            })
            .map_err(|e| Error::Terminal(e.to_string()))?;

        let joined = async {
            match app.session.as_mut() {
                Some(handle) => Some(handle.await),
                None => {
                    futures_util::future::pending::<()>().await;
                    None
                }
            }
        };

        tokio::select! {
            _ = ticker.tick() => {
                // Render cadence tick: loop back to draw.
            }
            result = joined => {
                if let Some(result) = result {
                    let outcome = match result {
                        Ok(outcome) => outcome,
                        Err(join_error) if join_error.is_panic() => SessionOutcome {
                            status: DisplayStatus::Failed,
                            failure: Some("session task panicked".to_string()),
                        },
                        Err(_) => SessionOutcome {
                            status: DisplayStatus::Failed,
                            failure: Some("session task cancelled".to_string()),
                        },
                    };
                    app.on_session_finished(outcome);
                    if app.exit_requested() {
                        return Ok(ExitOutcome { session: app.last_outcome().cloned() });
                    }
                }
            }
            input_event = input.next() => {
                match input_event {
                    Some(Ok(event)) => {
                        app.handle_key_event(&event, factory);
                        if app.state() == &AppState::Exiting {
                            return Ok(ExitOutcome { session: app.last_outcome().cloned() });
                        }
                    }
                    Some(Err(error)) => return Err(Error::Terminal(error.to_string())),
                    None => return Err(Error::Terminal("input stream ended".to_string())),
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
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
    fn scrolling_is_capped_at_zero() {
        let mut app = app();
        let factory = factory_unavailable();
        app.handle_action(Action::FocusNext, &factory);
        app.handle_action(Action::ScrollUp, &factory);
        assert_eq!(app.ui().book_scroll, 0);
        app.handle_action(Action::PageDown, &factory);
        assert_eq!(app.ui().book_scroll, 10);
    }

    #[test]
    fn adapter_trait_objects_are_object_safe_enough_for_factories() {
        // Compile-time shape check: a factory returns adapters.
        fn make() -> Result<Arc<dyn SessionAdapter>, Error> {
            struct NeverAdapter;
            impl SessionAdapter for NeverAdapter {
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
}
