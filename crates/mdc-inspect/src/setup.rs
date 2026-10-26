//! Guided-configuration screen state: provider picker, symbol text entry
//! with a dedicated text-entry input mode, and validation feedback
//! (PRD 31 §3.2 requirement 15, §3.6 requirement 95).
//!
//! While the text field owns input, `q`, `s`, and `c` are ordinary
//! characters, not commands; commands act only outside text entry.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{ProviderKind, SessionBounds};

/// Outcome of feeding a key to the setup screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetupAction {
    /// Key consumed by the screen; no app action.
    None,
    /// Operator requested session start with the current configuration.
    Start,
}

/// State of the guided configuration screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupState {
    providers: Vec<ProviderKind>,
    selected_provider: usize,
    input: String,
    symbols: Vec<String>,
    error: Option<String>,
}

impl SetupState {
    /// Builds the screen over the compiled provider set.
    pub fn new() -> Self {
        Self {
            providers: ProviderKind::ALL.to_vec(),
            selected_provider: 0,
            input: String::new(),
            symbols: Vec::new(),
            error: None,
        }
    }

    /// The currently selected compiled provider, if any.
    pub fn selected_provider(&self) -> Option<ProviderKind> {
        self.providers.get(self.selected_provider).copied()
    }

    /// Symbol text currently being typed.
    pub fn input(&self) -> &str {
        &self.input
    }

    /// Accepted symbols so far.
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// The last validation error, if any.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Surfaces an application-level error on the screen (session start
    /// failures keep the operator in configuration state).
    pub(crate) fn note_error(&mut self, message: String) {
        self.error = Some(message);
    }

    /// Number of compiled providers (zero when no venue feature is built).
    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }

    /// Feeds one key press. Returns [`SetupAction::Start`] when the operator
    /// presses `Enter` with an empty input and a valid configuration.
    pub fn handle_key(&mut self, key: &KeyEvent) -> SetupAction {
        self.error = None;
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            return SetupAction::None;
        }
        match key.code {
            KeyCode::Up => {
                if self.selected_provider > 0 {
                    self.selected_provider -= 1;
                }
            }
            KeyCode::Down => {
                if self.selected_provider + 1 < self.providers.len() {
                    self.selected_provider += 1;
                }
            }
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Enter => {
                if self.input.trim().is_empty() {
                    // Operator intent: start. The app layer validates and
                    // surfaces any configuration error on the screen.
                    return SetupAction::Start;
                }
                self.add_symbol();
            }
            KeyCode::Delete if !self.symbols.is_empty() => {
                let _ = self.symbols.pop();
            }
            KeyCode::Char(c) => self.input.push(c),
            _ => {}
        }
        SetupAction::None
    }

    fn add_symbol(&mut self) {
        let Some(provider) = self.selected_provider() else {
            self.error = Some("no provider features are compiled into this binary".to_string());
            return;
        };
        let bounds: SessionBounds = provider.session_bounds();
        let trimmed = self.input.trim().to_string();
        if trimmed.is_empty() {
            self.error = Some("symbol must not be empty".to_string());
            return;
        }
        if trimmed.chars().any(|c| c.is_control()) {
            self.error = Some("symbol contains control characters".to_string());
            return;
        }
        if self.symbols.iter().any(|s| s.trim() == trimmed) {
            self.error = Some(format!("duplicate symbol '{trimmed}'"));
            return;
        }
        let base_entries =
            usize::from(provider.presentation().mode == crate::display::DisplayMode::ReferenceRate);
        if self.symbols.len() >= bounds.max_symbols.saturating_add(base_entries) {
            self.error = Some(format!(
                "{} accepts at most {} symbols per session",
                provider.label(),
                bounds.max_symbols
            ));
            return;
        }
        self.symbols.push(trimmed);
        self.input.clear();
    }

    /// Validates the screen into a session configuration.
    pub fn start_config(&self) -> Result<SessionConfig, Error> {
        let Some(provider) = self.selected_provider() else {
            return Err(Error::Config(
                "no provider features are compiled into this binary".to_string(),
            ));
        };
        SessionConfig::new(provider, &self.symbols)
    }
}

impl Default for SetupState {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds a key press for view/screen tests.
#[cfg(test)]
pub(crate) fn key_for_test(c: char) -> KeyEvent {
    KeyEvent {
        code: if c == '\n' {
            KeyCode::Enter
        } else {
            KeyCode::Char(c)
        },
        modifiers: KeyModifiers::NONE,
        kind: crossterm::event::KeyEventKind::Press,
        state: crossterm::event::KeyEventState::NONE,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: crossterm::event::KeyEventKind::Press,
            state: crossterm::event::KeyEventState::NONE,
        }
    }

    #[cfg(feature = "binance")]
    #[test]
    fn text_entry_consumes_command_characters() {
        let mut setup = SetupState::new();
        for c in "BTCUSDT qs c".chars() {
            assert_eq!(setup.handle_key(&key(KeyCode::Char(c))), SetupAction::None);
        }
        assert_eq!(setup.input(), "BTCUSDT qs c");
    }

    #[cfg(feature = "binance")]
    #[test]
    fn enter_adds_symbol_and_clear_input_start_returns_start() {
        let mut setup = SetupState::new();
        for c in "BTCUSDT".chars() {
            let _ = setup.handle_key(&key(KeyCode::Char(c)));
        }
        assert_eq!(setup.handle_key(&key(KeyCode::Enter)), SetupAction::None);
        assert_eq!(setup.symbols(), ["BTCUSDT"]);
        // Empty input + valid configuration → start.
        assert_eq!(setup.handle_key(&key(KeyCode::Enter)), SetupAction::Start);
        assert!(setup.start_config().is_ok());
    }

    #[cfg(feature = "binance")]
    #[test]
    fn empty_symbols_start_request_still_validates_before_any_start() {
        let mut setup = SetupState::new();
        assert_eq!(setup.handle_key(&key(KeyCode::Enter)), SetupAction::Start);
        // The start intent surfaces; validation rejects it in the app layer
        // before any adapter construction or network I/O.
        assert!(setup.start_config().is_err());
    }

    #[cfg(feature = "binance")]
    #[test]
    fn duplicate_symbols_are_rejected_in_place() {
        let mut setup = SetupState::new();
        for c in "BTCUSDT".chars() {
            let _ = setup.handle_key(&key(KeyCode::Char(c)));
        }
        let _ = setup.handle_key(&key(KeyCode::Enter));
        for c in "BTCUSDT".chars() {
            let _ = setup.handle_key(&key(KeyCode::Char(c)));
        }
        assert_eq!(setup.handle_key(&key(KeyCode::Enter)), SetupAction::None);
        assert_eq!(setup.symbols().len(), 1);
        assert!(setup.error().unwrap().contains("duplicate"));
    }

    #[cfg(feature = "frankfurter")]
    #[test]
    fn reference_configuration_accepts_base_and_one_hundred_quotes() {
        let mut setup = SetupState::new();
        setup.selected_provider = setup
            .providers
            .iter()
            .position(|provider| *provider == ProviderKind::Frankfurter)
            .expect("Frankfurter compiled");
        for code in std::iter::once("EUR".to_string()).chain((0..100u8).map(|index| {
            format!(
                "A{}{}",
                char::from(b'A' + index / 26),
                char::from(b'A' + index % 26)
            )
        })) {
            for character in code.chars() {
                setup.handle_key(&key(KeyCode::Char(character)));
            }
            setup.handle_key(&key(KeyCode::Enter));
            assert!(setup.error().is_none(), "{}: {:?}", code, setup.error());
        }
        let config = setup.start_config().expect("full quote configuration");
        assert_eq!(config.base.as_ref().map(|base| base.as_str()), Some("EUR"));
        assert_eq!(config.symbols.len(), 100);
        for character in "ZZZ".chars() {
            setup.handle_key(&key(KeyCode::Char(character)));
        }
        setup.handle_key(&key(KeyCode::Enter));
        assert!(setup.error().is_some());
        assert_eq!(setup.start_config().unwrap().symbols.len(), 100);
    }

    #[test]
    fn backspace_edits_input() {
        let mut setup = SetupState::new();
        let _ = setup.handle_key(&key(KeyCode::Char('a')));
        let _ = setup.handle_key(&key(KeyCode::Char('b')));
        let _ = setup.handle_key(&key(KeyCode::Backspace));
        assert_eq!(setup.input(), "a");
    }
}
