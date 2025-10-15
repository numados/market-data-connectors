//! Domain error taxonomy for market data connectors.

use crate::model::{IntegrityAlert, Symbol};
use std::error::Error;
use std::time::Duration;
use thiserror::Error;

/// Central error type across market data connector operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MdcError {
    /// Deserialization or schema parsing error.
    #[error("deserialization failed: {0}")]
    Deserialize(#[source] Box<dyn Error + Send + Sync>),

    /// Protocol violation or malformed exchange message.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// Order book sequence or checksum integrity failure.
    #[error("integrity failure on {symbol}: {alert:?}")]
    Integrity {
        /// Symbol that suffered the integrity failure.
        symbol: Symbol,
        /// In-band integrity alert describing the failure.
        alert: IntegrityAlert,
    },

    /// Exchange REST rate limit active with mandated retry cooldown.
    #[error("REST embargo active for {retry_after:?}")]
    RateLimited {
        /// Duration of the mandated retry cooldown embargo.
        retry_after: Duration,
    },

    /// Underlying network or transport communication failure.
    #[error("transport error: {0}")]
    Transport(#[source] Box<dyn Error + Send + Sync>),

    /// Downstream consumer sink rejected an event.
    #[error("downstream sink error: {0}")]
    Sink(#[source] Box<dyn Error + Send + Sync>),

    /// Connector task or operation was cancelled via cancellation token.
    #[error("connector cancelled")]
    Cancelled,

    /// Consumer requirements not supported by provider capabilities.
    #[error("capability error: {0}")]
    Capability(#[from] crate::model::CapabilityError),

    /// Invalid configuration parameter or boundary violation.
    #[error("configuration error: {0}")]
    Config(String),
}

/// Thread-safe, atomically reference-counted error wrapper preserving error source chains.
///
/// Implements `PartialEq` and `Eq` via pointer equality with fallback to string comparison,
/// and delegates `std::error::Error::source` to the wrapped dynamic error.
#[derive(Debug, Clone)]
pub struct ArcError(pub std::sync::Arc<dyn std::error::Error + Send + Sync>);

impl ArcError {
    /// Creates a new `ArcError` wrapping any standard error.
    pub fn new<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self(std::sync::Arc::new(err))
    }

    /// Wraps an existing `Arc<dyn Error + Send + Sync>`.
    pub fn from_arc(arc: std::sync::Arc<dyn std::error::Error + Send + Sync>) -> Self {
        Self(arc)
    }

    /// Borrows the wrapped error trait object.
    pub fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        &*self.0
    }
}

impl std::fmt::Display for ArcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ArcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl PartialEq for ArcError {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0) || self.0.to_string() == other.0.to_string()
    }
}

impl Eq for ArcError {}

impl From<std::sync::Arc<dyn std::error::Error + Send + Sync>> for ArcError {
    fn from(arc: std::sync::Arc<dyn std::error::Error + Send + Sync>) -> Self {
        Self(arc)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn error_display_and_source() {
        #[derive(Debug, thiserror::Error)]
        #[error("inner io")]
        struct InnerError;

        let err = MdcError::Transport(Box::new(InnerError));
        assert_eq!(err.to_string(), "transport error: inner io");
        assert!(err.source().is_some());

        let cancelled = MdcError::Cancelled;
        assert_eq!(cancelled.to_string(), "connector cancelled");
        assert!(cancelled.source().is_none());

        let integrity = MdcError::Integrity {
            symbol: Symbol::from("BTCUSDT"),
            alert: IntegrityAlert::ResyncStarted,
        };
        assert!(
            integrity
                .to_string()
                .contains("integrity failure on BTCUSDT")
        );
    }

    #[test]
    fn arc_error_preserves_source_chain() {
        #[derive(Debug, thiserror::Error)]
        #[error("root cause")]
        struct RootCause;

        #[derive(Debug, thiserror::Error)]
        #[error("intermediate error")]
        struct IntermediateError(#[source] RootCause);

        let err = IntermediateError(RootCause);
        let arc_err = ArcError::new(err);

        assert_eq!(arc_err.to_string(), "intermediate error");
        assert!(arc_err.source().is_some());
        assert_eq!(arc_err.source().unwrap().to_string(), "root cause");

        let cloned = arc_err.clone();
        assert_eq!(arc_err, cloned);
    }
}
