//! Command-line interface: argument parsing and startup-mode validation.

use clap::Parser;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::ProviderKind;

/// Inspect live market-data providers in a full-screen terminal.
///
/// With no arguments the inspector opens a guided configuration screen.
/// Pass --provider and one or more --symbol values to start directly.
#[derive(Debug, Clone, Parser)]
#[command(name = "mdc-inspect", version, about)]
pub struct Cli {
    /// Provider to inspect (for example "binance").
    #[arg(long, value_name = "PROVIDER")]
    pub provider: Option<String>,

    /// Provider-native symbol; repeat the flag for multiple symbols.
    #[arg(long = "symbol", value_name = "SYMBOL")]
    pub symbols: Vec<String>,
}

/// How the application should start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupMode {
    /// Open the guided configuration screen.
    Guided,
    /// Start a session directly from validated CLI arguments.
    Direct(SessionConfig),
}

impl Cli {
    /// Validates parsed arguments into a startup mode.
    ///
    /// Pure and offline: no network access, no terminal activation, no
    /// provider construction. Clap owns help, version, and usage errors.
    pub fn startup_mode(&self) -> Result<StartupMode, Error> {
        match (&self.provider, self.symbols.as_slice()) {
            (Some(name), symbols) => {
                let provider = ProviderKind::parse(name).map_err(Error::from)?;
                let config = SessionConfig::new(provider, symbols)?;
                Ok(StartupMode::Direct(config))
            }
            (None, []) => Ok(StartupMode::Guided),
            (None, _) => Err(Error::Config(
                "--symbol requires --provider; pass both to start directly".to_string(),
            )),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("mdc-inspect").chain(args.iter().copied()))
            .expect("arguments should parse")
    }

    #[test]
    fn no_arguments_opens_guided_mode() {
        assert!(matches!(cli(&[]).startup_mode(), Ok(StartupMode::Guided)));
    }

    #[cfg(feature = "binance")]
    #[test]
    fn direct_start_builds_a_validated_session_config() {
        let mode = cli(&[
            "--provider",
            "binance",
            "--symbol",
            "BTCUSDT",
            "--symbol",
            "ETHUSDT",
        ])
        .startup_mode()
        .expect("valid direct start");
        let StartupMode::Direct(config) = mode else {
            panic!("expected direct startup, got {mode:?}");
        };
        assert_eq!(config.provider, ProviderKind::Binance);
        assert_eq!(config.symbols.len(), 2);
    }

    #[test]
    fn provider_without_symbol_is_a_configuration_error() {
        let error = cli(&["--provider", "binance"])
            .startup_mode()
            .expect_err("provider without symbols must fail");
        assert!(error.to_string().contains("at least one symbol"));
    }

    #[test]
    fn symbol_without_provider_is_a_configuration_error() {
        let error = cli(&["--symbol", "BTCUSDT"])
            .startup_mode()
            .expect_err("symbol without provider must fail");
        assert!(error.to_string().contains("--symbol requires --provider"));
    }

    #[test]
    fn unknown_provider_is_actionable() {
        let error = cli(&["--provider", "notaprovider", "--symbol", "X"])
            .startup_mode()
            .expect_err("unknown provider must fail");
        assert!(error.to_string().contains("unknown provider"));
    }

    #[cfg(not(feature = "kraken"))]
    #[test]
    fn uncompiled_provider_names_the_missing_feature() {
        let error = cli(&["--provider", "kraken", "--symbol", "BTC/USD"])
            .startup_mode()
            .expect_err("uncompiled provider must fail");
        assert!(error.to_string().contains("--features kraken"));
    }

    #[test]
    fn help_is_handled_by_clap_without_side_effects() {
        let error = Cli::try_parse_from(["mdc-inspect", "--help"]).expect_err("help exits");
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);
    }

    #[test]
    fn version_is_handled_by_clap_without_side_effects() {
        let error = Cli::try_parse_from(["mdc-inspect", "--version"]).expect_err("version exits");
        assert_eq!(error.kind(), ErrorKind::DisplayVersion);
    }
}
