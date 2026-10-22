//! Provider-agnostic UI actions and Crossterm event-to-action mapping
//! (PRD 31 §3.6).
//!
//! Inspect-only: start, stop, configure, focus, tabs, scroll, help, quit.
//! No resync, reconnect, pause, resume, or poll actions exist.

use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

/// A mapped UI action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Start the configured session (`Enter` on the setup screen with an
    /// empty symbol input, or direct-start arguments).
    Start,
    /// Stop the active session (`s`).
    Stop,
    /// Return to the configuration screen (`c`, non-running states only).
    Configure,
    /// Move pane focus forward (`Tab`).
    FocusNext,
    /// Move pane focus backward (`BackTab`).
    FocusPrev,
    /// Next symbol tab (`Right`).
    NextSymbol,
    /// Previous symbol tab (`Left`).
    PrevSymbol,
    /// Scroll up one line (`Up`).
    ScrollUp,
    /// Scroll down one line (`Down`).
    ScrollDown,
    /// Scroll up one page (`PageUp`).
    PageUp,
    /// Scroll down one page (`PageDown`).
    PageDown,
    /// Toggle the help overlay (`?`).
    ToggleHelp,
    /// Terminal resized.
    Resize(u16, u16),
    /// Quit (`q`, outside text-entry mode).
    Quit,
    /// Quit from every interactive state (`Ctrl-C`); while already stopping,
    /// forces task abort.
    ForceQuit,
}

/// Maps a Crossterm event to an action, if any.
///
/// `Ctrl-C` is handled as a raw-mode key event (requirement 91); no Tokio
/// signal handler is installed. Key releases and non-press kinds are ignored.
pub fn map_event(event: &Event) -> Option<Action> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => map_key(key),
        Event::Resize(columns, rows) => Some(Action::Resize(*columns, *rows)),
        _ => None,
    }
}

fn map_key(key: &KeyEvent) -> Option<Action> {
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            KeyCode::Char('c') | KeyCode::Char('C') => Some(Action::ForceQuit),
            _ => None,
        };
    }
    match key.code {
        KeyCode::Enter => Some(Action::Start),
        KeyCode::Tab => Some(Action::FocusNext),
        KeyCode::BackTab => Some(Action::FocusPrev),
        KeyCode::Left => Some(Action::PrevSymbol),
        KeyCode::Right => Some(Action::NextSymbol),
        KeyCode::Up => Some(Action::ScrollUp),
        KeyCode::Down => Some(Action::ScrollDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::Char('?') => Some(Action::ToggleHelp),
        KeyCode::Char('q') => Some(Action::Quit),
        KeyCode::Char('s') => Some(Action::Stop),
        KeyCode::Char('c') => Some(Action::Configure),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crossterm::event::KeyEventState;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> Event {
        Event::Key(KeyEvent {
            code,
            modifiers,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        })
    }

    #[test]
    fn maps_all_required_controls() {
        assert_eq!(
            map_event(&key(KeyCode::Enter, KeyModifiers::NONE)),
            Some(Action::Start)
        );
        assert_eq!(
            map_event(&key(KeyCode::Tab, KeyModifiers::NONE)),
            Some(Action::FocusNext)
        );
        assert_eq!(
            map_event(&key(KeyCode::BackTab, KeyModifiers::SHIFT)),
            Some(Action::FocusPrev)
        );
        assert_eq!(
            map_event(&key(KeyCode::Left, KeyModifiers::NONE)),
            Some(Action::PrevSymbol)
        );
        assert_eq!(
            map_event(&key(KeyCode::Right, KeyModifiers::NONE)),
            Some(Action::NextSymbol)
        );
        assert_eq!(
            map_event(&key(KeyCode::Up, KeyModifiers::NONE)),
            Some(Action::ScrollUp)
        );
        assert_eq!(
            map_event(&key(KeyCode::Down, KeyModifiers::NONE)),
            Some(Action::ScrollDown)
        );
        assert_eq!(
            map_event(&key(KeyCode::PageUp, KeyModifiers::NONE)),
            Some(Action::PageUp)
        );
        assert_eq!(
            map_event(&key(KeyCode::PageDown, KeyModifiers::NONE)),
            Some(Action::PageDown)
        );
        assert_eq!(
            map_event(&key(KeyCode::Char('?'), KeyModifiers::NONE)),
            Some(Action::ToggleHelp)
        );
        assert_eq!(
            map_event(&key(KeyCode::Char('q'), KeyModifiers::NONE)),
            Some(Action::Quit)
        );
        assert_eq!(
            map_event(&key(KeyCode::Char('s'), KeyModifiers::NONE)),
            Some(Action::Stop)
        );
        assert_eq!(
            map_event(&key(KeyCode::Char('c'), KeyModifiers::NONE)),
            Some(Action::Configure)
        );
    }

    #[test]
    fn ctrl_c_is_a_key_event_from_every_state() {
        assert_eq!(
            map_event(&key(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Some(Action::ForceQuit)
        );
        assert_eq!(
            map_event(&key(KeyCode::Char('C'), KeyModifiers::CONTROL)),
            Some(Action::ForceQuit)
        );
    }

    #[test]
    fn resize_maps_and_releases_are_ignored() {
        assert_eq!(
            map_event(&Event::Resize(80, 24)),
            Some(Action::Resize(80, 24))
        );
        let release = Event::Key(KeyEvent {
            code: KeyCode::Char('q'),
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: KeyEventState::NONE,
        });
        assert_eq!(map_event(&release), None);
    }

    #[test]
    fn excluded_provider_controls_have_no_mapping() {
        // No resync / reconnect / pause / resume / poll keys.
        for code in [
            KeyCode::Char('r'),
            KeyCode::Char('p'),
            KeyCode::Char('l'),
            KeyCode::Char('o'),
        ] {
            assert_eq!(map_event(&key(code, KeyModifiers::NONE)), None, "{code:?}");
        }
    }
}
