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
        symbol: Symbol,
        alert: IntegrityAlert,
    },

    /// Exchange REST rate limit active with mandated retry cooldown.
    #[error("REST embargo active for {retry_after:?}")]
    RateLimited { retry_after: Duration },

    /// Underlying network or transport communication failure.
    #[error("transport error: {0}")]
    Transport(#[source] Box<dyn Error + Send + Sync>),

    /// Downstream consumer sink rejected an event.
    #[error("downstream sink error: {0}")]
    Sink(#[source] Box<dyn Error + Send + Sync>),

    /// Connector task or operation was cancelled via cancellation token.
    #[error("connector cancelled")]
    Cancelled,
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
}
