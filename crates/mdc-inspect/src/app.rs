//! Top-level application: UI state machine, fixed render cadence, session
//! ownership, and start/stop/quit coordination (PRD 31 §3.6–3.7).
//!
//! The session join is never awaited inline by the input path: stop is a
//! state ([`AppState::Stopping`]) observed through a concurrent select arm,
//! and a second `Ctrl-C` while stopping forces task abort (requirement 152).

use std::io;
use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;

use crate::action::{Action, map_event};
use crate::config::{CHANNEL_CAPACITY, RENDER_CADENCE, SessionConfig};
use crate::diagnostics::{DiagnosticsRing, DiagnosticsSnapshot};
use crate::display::{DisplaySnapshot, DisplayStatus, UiState};
use crate::error::Error;
use crate::provider::{ProviderKind, SessionAdapter, SessionBounds};
use crate::session::{SessionOutcome, run_session_controlled};
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
    force_stop: Option<CancellationToken>,
    force_stop_requested: bool,
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
            force_stop: None,
            force_stop_requested: false,
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
        adapter.validate(&config)?;
        let bounds: SessionBounds = config.provider.session_bounds();
        let session_token = self.root.child_token();
        let force_stop = CancellationToken::new();
        let (display_tx, display_rx) = watch::channel(Arc::new(DisplaySnapshot {
            provider: config.provider.label(),
            session: DisplayStatus::Loading,
            failure: None,
            symbols: Vec::new(),
            alerts: Default::default(),
            alerts_evicted: 0,
        }));
        let handle = tokio::spawn(run_session_controlled(
            adapter,
            config,
            bounds,
            CHANNEL_CAPACITY,
            session_token.clone(),
            force_stop.clone(),
            display_tx,
        ));
        self.display = Some(display_rx);
        self.session = Some(handle);
        self.session_token = Some(session_token);
        self.force_stop = Some(force_stop);
        self.force_stop_requested = false;
        self.state = AppState::Running;
        Ok(())
    }

    /// Feeds a raw input event. Global control and help actions are handled
    /// before setup text entry; native symbol characters keep their text
    /// semantics while the setup field owns ordinary input.
    pub fn handle_key_event(&mut self, event: &crossterm::event::Event, factory: &AdapterFactory) {
        let mapped = map_event(event);
        if let Some(action @ (Action::ForceQuit | Action::ToggleHelp)) = mapped {
            self.handle_action(action, factory);
            return;
        }
        if self.state == AppState::Setup && !self.help_open {
            if let crossterm::event::Event::Key(key) = event {
                if matches!(self.setup.handle_key(key), SetupAction::Start) {
                    self.handle_action(Action::Start, factory);
                }
                return;
            }
        }
        if let Some(action) = mapped {
            self.handle_action(action, factory);
        }
    }

    /// Handles one action, advancing the state machine.
    pub fn handle_action(&mut self, action: Action, factory: &AdapterFactory) {
        match action {
            Action::ForceQuit => {
                // The first interrupt starts cooperative teardown. A repeat
                // cancels the force token; the session still owns and awaits
                // the provider task before the application exits.
                if self.session.is_some() {
                    if self.state == AppState::Stopping {
                        if let Some(token) = self.force_stop.as_ref() {
                            token.cancel();
                        }
                        self.force_stop_requested = true;
                    } else {
                        if let Some(token) = self.session_token.as_ref() {
                            token.cancel();
                        }
                        self.state = AppState::Stopping;
                    }
                    self.root.cancel();
                    self.exit_requested = true;
                } else {
                    self.root.cancel();
                    self.exit_requested = true;
                    self.state = AppState::Exiting;
                }
            }
            Action::Quit => match self.state {
                AppState::Running | AppState::Stopping => {
                    self.root.cancel();
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
        self.force_stop = None;
        self.force_stop_requested = false;
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

    async fn reap_session(&mut self) {
        let Some(handle) = self.session.take() else {
            return;
        };
        let outcome = session_outcome(handle.await);
        self.on_session_finished(outcome);
    }

    async fn teardown(&mut self) {
        self.root.cancel();
        if self.force_stop_requested {
            if let Some(token) = self.force_stop.as_ref() {
                token.cancel();
            }
        }
        self.reap_session().await;
    }
}

impl Drop for App {
    fn drop(&mut self) {
        self.root.cancel();
        if let Some(token) = self.session_token.as_ref() {
            token.cancel();
        }
        if let Some(token) = self.force_stop.as_ref() {
            token.cancel();
        }
        if let Some(handle) = self.session.as_ref() {
            handle.abort();
        }
    }
}

fn session_outcome(result: Result<SessionOutcome, tokio::task::JoinError>) -> SessionOutcome {
    match result {
        Ok(outcome) => outcome,
        Err(join_error) if join_error.is_panic() => SessionOutcome {
            status: DisplayStatus::Failed,
            failure: Some("session task panicked".to_string()),
        },
        Err(_) => SessionOutcome {
            status: DisplayStatus::Failed,
            failure: Some("session task cancelled".to_string()),
        },
    }
}

/// Runs the interactive loop until exit. `factory` builds provider adapters;
/// `direct` starts a session immediately (CLI direct-start mode).
pub async fn run(
    app: App,
    terminal: ratatui::Terminal<impl ratatui::backend::Backend>,
    direct: Option<SessionConfig>,
    factory: &AdapterFactory,
) -> Result<ExitOutcome, Error> {
    let input = crossterm::event::EventStream::new();
    run_with_input(app, terminal, direct, factory, input, |terminal, app| {
        let snapshot = app.snapshot();
        let diagnostics = app.diagnostics_snapshot();
        terminal.draw(|frame| {
            if app.state() == &AppState::Setup {
                crate::view::draw_setup(frame, app.setup());
            } else {
                crate::view::draw_main(frame, &snapshot, app.ui(), &diagnostics);
            }
            if app.help_open() {
                crate::view::draw_help(frame);
            }
        })?;
        Ok(())
    })
    .await
}

/// Runs the loop with injectable input and rendering seams. The production
/// entry point supplies Crossterm's event stream and the terminal renderer;
/// tests can fail either boundary without activating a real terminal.
pub(crate) async fn run_with_input<B, S, R>(
    mut app: App,
    mut terminal: ratatui::Terminal<B>,
    direct: Option<SessionConfig>,
    factory: &AdapterFactory,
    mut input: S,
    mut render: R,
) -> Result<ExitOutcome, Error>
where
    B: ratatui::backend::Backend,
    S: futures_util::Stream<Item = io::Result<crossterm::event::Event>> + Unpin,
    R: FnMut(&mut ratatui::Terminal<B>, &App) -> io::Result<()>,
{
    if let Some(config) = direct {
        if let Err(error) = app.start_session(config, factory) {
            app.teardown().await;
            return Err(error);
        }
    }

    let mut loop_error = None;
    if let Err(error) = render(&mut terminal, &app) {
        loop_error = Some(Error::Terminal(error.to_string()));
    }

    let mut ticker = interval_at(Instant::now() + RENDER_CADENCE, RENDER_CADENCE);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    while loop_error.is_none() {
        if app.state() == &AppState::Exiting && app.session.is_none() {
            break;
        }

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
                if let Err(error) = render(&mut terminal, &app) {
                    loop_error = Some(Error::Terminal(error.to_string()));
                }
            }
            result = joined => {
                if let Some(result) = result {
                    app.on_session_finished(session_outcome(result));
                    if app.exit_requested() {
                        break;
                    }
                }
            }
            input_event = input.next() => {
                match input_event {
                    Some(Ok(event)) => {
                        app.handle_key_event(&event, factory);
                        if app.state() == &AppState::Exiting && app.session.is_none() {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        loop_error = Some(Error::Terminal(error.to_string()));
                    }
                    None => {
                        loop_error = Some(Error::Terminal("input stream ended".to_string()));
                    }
                }
            }
        }
    }

    app.teardown().await;
    if let Some(error) = loop_error {
        Err(error)
    } else {
        Ok(ExitOutcome {
            session: app.last_outcome().cloned(),
        })
    }
}

#[cfg(test)]
mod tests;
