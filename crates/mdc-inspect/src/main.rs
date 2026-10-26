//! Thin process entry point for the inspector.

use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use clap::Parser;

use mdc_inspect::app::{App, ExitOutcome, run};
use mdc_inspect::cli::{Cli, StartupMode};
use mdc_inspect::config::SessionConfig;
use mdc_inspect::diagnostics::{DiagnosticsRing, DiagnosticsSubscriber};
use mdc_inspect::display::DisplayStatus;
use mdc_inspect::error::Error;
use mdc_inspect::provider::adapter_for;
use mdc_inspect::terminal::{
    CrosstermLifecycle, TerminalGuard, install_panic_hook, ratatui_terminal,
    require_interactive_terminal,
};

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mode = match cli.startup_mode() {
        Ok(mode) => mode,
        Err(error) => {
            eprintln!("error: {error}");
            return ExitCode::FAILURE;
        }
    };
    // Clap output before terminal activation is permitted; nothing may print
    // to stdout or stderr after the alternate screen is entered.
    if let Err(error) = require_interactive_terminal() {
        eprintln!("error: {error}");
        return ExitCode::FAILURE;
    }

    let (ring, subscriber) = DiagnosticsSubscriber::new();
    if tracing::subscriber::set_global_default(subscriber).is_err() {
        eprintln!("error: diagnostics subscriber already installed");
        return ExitCode::FAILURE;
    }
    install_panic_hook();

    let direct = match mode {
        StartupMode::Guided => None,
        StartupMode::Direct(config) => Some(config),
    };

    let factory = |kind| adapter_for(kind);

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: failed to start async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    let result = runtime.block_on(run_tui(direct, Arc::clone(&ring), &factory));

    match result {
        Ok(outcome) => exit_code_for(outcome),
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run_tui(
    direct: Option<SessionConfig>,
    ring: Arc<Mutex<DiagnosticsRing>>,
    factory: &mdc_inspect::app::AdapterFactory,
) -> Result<ExitOutcome, Error> {
    let mut guard = TerminalGuard::activate(Box::new(CrosstermLifecycle))?;
    let terminal = ratatui_terminal()?;
    let app = App::new(ring);
    let outcome = run(app, terminal, direct, factory).await;
    guard.restore();
    outcome
}

fn exit_code_for(outcome: ExitOutcome) -> ExitCode {
    match outcome.session {
        Some(session) if session.status == DisplayStatus::Failed => ExitCode::FAILURE,
        _ => ExitCode::SUCCESS,
    }
}
