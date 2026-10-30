//! App-local error taxonomy for the inspector.

use std::io;

use crate::provider::ParseProviderError;

/// Errors surfaced by the inspector application.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid session configuration.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// Configuration rejected by a provider's native builder.
    #[error("{provider} provider configuration failed: {source}")]
    ProviderConfig {
        /// Human-facing provider label.
        provider: &'static str,
        /// The provider error, retained for diagnostics and callers.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The requested provider is not compiled into this binary.
    #[error(
        "provider '{provider}' is not compiled into this binary; rebuild with --features {feature}"
    )]
    ProviderUnavailable {
        /// Provider name as supplied by the operator.
        provider: String,
        /// Feature that compiles the provider.
        feature: String,
    },
    /// The requested provider adapter is not implemented in this build.
    #[error("provider '{0}' is not implemented in this build yet")]
    NotYetImplemented(String),
    /// The inspector requires an interactive terminal.
    #[error("mdc-inspect requires an interactive terminal (stdin and stdout)")]
    NotInteractive,
    /// Session orchestration failure with a sanitized cause.
    #[error("session failed: {0}")]
    Session(String),
    /// Terminal activation, restoration, rendering, or input failure.
    #[error("terminal error: {0}")]
    Terminal(String),
    /// Terminal activation, restoration, rendering, or input I/O failure.
    #[error("terminal I/O error: {0}")]
    TerminalIo(#[source] io::Error),
}

impl Error {
    /// Wraps a provider-native validation error while preserving its source.
    #[cfg(any(
        feature = "binance",
        feature = "kraken",
        feature = "coinbase",
        feature = "bitfinex",
        feature = "gateio",
        feature = "bitstamp",
        feature = "huobi",
        feature = "coinbase-advanced",
        feature = "lmax",
        feature = "frankfurter",
        feature = "vwd"
    ))]
    pub(crate) fn provider_config<E>(provider: &'static str, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::ProviderConfig {
            provider,
            source: Box::new(source),
        }
    }
}

impl From<ParseProviderError> for Error {
    fn from(error: ParseProviderError) -> Self {
        match error {
            ParseProviderError::NotCompiled { name, feature } => Error::ProviderUnavailable {
                provider: name,
                feature,
            },
            other => Error::Config(other.to_string()),
        }
    }
}
