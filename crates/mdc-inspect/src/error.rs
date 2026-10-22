//! App-local error taxonomy for the inspector.

use crate::provider::ParseProviderError;

/// Errors surfaced by the inspector application.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid session configuration.
    #[error("invalid configuration: {0}")]
    Config(String),
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
