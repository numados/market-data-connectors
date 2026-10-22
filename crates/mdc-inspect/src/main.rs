//! Thin process entry point for the inspector.

use std::io::IsTerminal;
use std::process::ExitCode;

use clap::Parser;

use mdc_inspect::cli::{Cli, StartupMode};
use mdc_inspect::error::Error;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mode = match cli.startup_mode() {
        Ok(mode) => mode,
        Err(error) => {
            eprintln!("error: {error}");
            return ExitCode::FAILURE;
        }
    };
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        eprintln!("error: {}", Error::NotInteractive);
        return ExitCode::FAILURE;
    }
    let target = match &mode {
        StartupMode::Guided => "guided configuration".to_string(),
        StartupMode::Direct(config) => format!("{} session", config.provider.label()),
    };
    eprintln!("error: {}", Error::NotYetImplemented(target));
    ExitCode::FAILURE
}
