//! Crossterm terminal activation, idempotent restoration guard, panic hook,
//! and the terminal-lifecycle seam used by tests (PRD 31 §3.7).

use crossterm::ExecutableCommand;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::io::{self, IsTerminal, Stdout};

use crate::error::Error;

const RAW: u8 = 1;
const ALT_SCREEN: u8 = 2;
const CURSOR: u8 = 4;

/// Terminal lifecycle operations, injectable for tests (requirement 141).
pub trait TerminalLifecycle {
    /// Enables raw mode.
    fn enable_raw(&mut self) -> Result<(), Error>;
    /// Disables raw mode.
    fn disable_raw(&mut self) -> Result<(), Error>;
    /// Enters the alternate screen.
    fn enter_alternate(&mut self) -> Result<(), Error>;
    /// Leaves the alternate screen.
    fn leave_alternate(&mut self) -> Result<(), Error>;
    /// Hides the cursor.
    fn hide_cursor(&mut self) -> Result<(), Error>;
    /// Shows the cursor.
    fn show_cursor(&mut self) -> Result<(), Error>;
}

/// Real Crossterm-backed lifecycle operations.
#[derive(Debug, Default)]
pub struct CrosstermLifecycle;

impl TerminalLifecycle for CrosstermLifecycle {
    fn enable_raw(&mut self) -> Result<(), Error> {
        enable_raw_mode().map_err(Error::TerminalIo)
    }

    fn disable_raw(&mut self) -> Result<(), Error> {
        disable_raw_mode().map_err(Error::TerminalIo)
    }

    fn enter_alternate(&mut self) -> Result<(), Error> {
        io::stdout()
            .execute(EnterAlternateScreen)
            .map_err(Error::TerminalIo)?;
        Ok(())
    }

    fn leave_alternate(&mut self) -> Result<(), Error> {
        io::stdout()
            .execute(LeaveAlternateScreen)
            .map_err(Error::TerminalIo)?;
        Ok(())
    }

    fn hide_cursor(&mut self) -> Result<(), Error> {
        io::stdout()
            .execute(crossterm::cursor::Hide)
            .map_err(Error::TerminalIo)?;
        Ok(())
    }

    fn show_cursor(&mut self) -> Result<(), Error> {
        io::stdout()
            .execute(crossterm::cursor::Show)
            .map_err(Error::TerminalIo)?;
        Ok(())
    }
}

/// Activated terminal with an idempotent restoration guard.
///
/// Tracks partial initialization so startup failures restore only completed
/// steps (requirement 100); `Drop` covers panic unwinding (requirement 103);
/// restoration is idempotent because the panic hook and `Drop` may both run
/// (requirement 105).
pub struct TerminalGuard {
    lifecycle: Box<dyn TerminalLifecycle>,
    completed: u8,
    restored: bool,
}

impl TerminalGuard {
    /// Activates the terminal: raw mode, alternate screen, hidden cursor.
    /// A failure restores the steps completed so far before returning.
    pub fn activate(lifecycle: Box<dyn TerminalLifecycle>) -> Result<Self, Error> {
        let mut guard = Self {
            lifecycle,
            completed: 0,
            restored: false,
        };
        let activation = (|| {
            guard.lifecycle.enable_raw()?;
            guard.completed |= RAW;
            guard.lifecycle.enter_alternate()?;
            guard.completed |= ALT_SCREEN;
            guard.lifecycle.hide_cursor()?;
            guard.completed |= CURSOR;
            Ok(())
        })();
        if let Err(error) = activation {
            guard.restore();
            return Err(error);
        }
        Ok(guard)
    }

    /// Restores the terminal exactly once, in reverse activation order:
    /// show cursor, leave alternate screen, disable raw mode.
    pub fn restore(&mut self) {
        if self.restored {
            return;
        }
        self.restored = true;
        if self.completed & CURSOR != 0 {
            let _ = self.lifecycle.show_cursor();
        }
        if self.completed & ALT_SCREEN != 0 {
            let _ = self.lifecycle.leave_alternate();
        }
        if self.completed & RAW != 0 {
            let _ = self.lifecycle.disable_raw();
        }
        self.completed = 0;
    }

    /// Whether restoration already happened.
    pub fn is_restored(&self) -> bool {
        self.restored
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        self.restore();
    }
}

/// Rejects non-TTY invocations before raw mode (requirements 25–26).
pub fn require_interactive_terminal() -> Result<(), Error> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err(Error::NotInteractive);
    }
    Ok(())
}

/// Builds the real Ratatui terminal over stdout.
pub fn ratatui_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>, Error> {
    let backend = CrosstermBackend::new(io::stdout());
    Terminal::new(backend).map_err(Error::TerminalIo)
}

/// Installs a panic hook that restores the terminal for main-thread panics
/// before delegating to the previous hook (requirement 104). Panics on other
/// threads restore nothing: recovered task panics surface through the join
/// error path instead.
pub fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if std::thread::current().name() == Some("main") {
            restore_in_place();
        }
        previous(info);
    }));
}

/// Best-effort in-place restoration used by the panic hook; idempotent.
fn restore_in_place() {
    let _ = io::stdout().execute(crossterm::cursor::Show);
    let _ = io::stdout().execute(LeaveAlternateScreen);
    let _ = disable_raw_mode();
}

#[cfg(test)]
mod tests;
