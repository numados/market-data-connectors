//! Configuration options for the LMAX Digital market data connector.

use std::time::Duration;
use url::Url;

/// Control message rate limiting and pacing parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPacing {
    /// Delay after WebSocket handshake before sending the initial subscribe message (default: 1s).
    pub initial_delay: Duration,
    /// Minimum interval between subsequent control commands (e.g. unsubscribe / subscribe) (default: 1s).
    pub min_command_interval: Duration,
    /// Initial backoff when `TOO_MANY_REQUESTS` error code is received (default: 1s).
    pub too_many_requests_backoff: Duration,
    /// Maximum backoff ceiling for rate limit backoff (default: 30s).
    pub max_backoff: Duration,
}

impl Default for ControlPacing {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            min_command_interval: Duration::from_secs(1),
            too_many_requests_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
        }
    }
}

/// Active stream selection for LMAX Digital connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LmaxStreams {
    /// Enable `ORDER_BOOK` continuous snapshot stream (default: true).
    pub book: bool,
    /// Enable `TRADE` execution event stream (default: false).
    pub trades: bool,
}

impl Default for LmaxStreams {
    fn default() -> Self {
        Self {
            book: true,
            trades: false,
        }
    }
}

/// Configuration for the LMAX Digital connector.
#[derive(Debug, Clone)]
pub struct LmaxConfig {
    /// WebSocket endpoint URL (default: `wss://public-data-api.london-digital.lmax.com/v1/web-socket`).
    pub ws_url: Url,
    /// Maximum number of symbols assigned per connection task (default: `usize::MAX` / single bucket).
    pub symbols_per_connection: usize,
    /// Stream subscription settings.
    pub streams: LmaxStreams,
    /// Control pacing and rate-limiting configuration.
    pub control_pacing: ControlPacing,
    /// Maximum silence before disconnecting (default: 45s).
    pub silence_timeout: Duration,
    /// Heartbeat ping interval (default: 30s).
    pub ping_interval: Duration,
    /// WebSocket connect (TCP, TLS, and upgrade handshake) timeout. Default: 10 seconds.
    pub connection_timeout: Duration,
}

impl Default for LmaxConfig {
    fn default() -> Self {
        #[allow(clippy::expect_used)]
        let ws_url = Url::parse("wss://public-data-api.london-digital.lmax.com/v1/web-socket")
            .expect("static URL parse");
        Self {
            ws_url,
            symbols_per_connection: usize::MAX,
            streams: LmaxStreams::default(),
            control_pacing: ControlPacing::default(),
            silence_timeout: Duration::from_secs(45),
            ping_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(10),
        }
    }
}

/// Errors occurring during configuration validation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// Symbol list was empty.
    #[error("symbols list cannot be empty")]
    EmptySymbols,
    /// Market data sink was not supplied.
    #[error("market data sink is required")]
    MissingSink,
    /// WebSocket URL scheme was invalid (must be ws:// or wss://).
    #[error("ws_url scheme must be ws or wss: {0}")]
    InvalidWsScheme(String),
    /// At least one stream must be enabled.
    #[error("at least one stream (book or trades) must be enabled")]
    NoStreamsEnabled,
}

impl LmaxConfig {
    /// Validates the configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let ws_scheme = self.ws_url.scheme();
        if ws_scheme != "ws" && ws_scheme != "wss" {
            return Err(ConfigError::InvalidWsScheme(ws_scheme.to_string()));
        }
        if !self.streams.book && !self.streams.trades {
            return Err(ConfigError::NoStreamsEnabled);
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_valid() {
        let config = LmaxConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.symbols_per_connection, usize::MAX);
        assert!(config.streams.book);
        assert!(!config.streams.trades);
    }

    #[test]
    fn test_no_streams_enabled_rejected() {
        let config = LmaxConfig {
            streams: LmaxStreams {
                book: false,
                trades: false,
            },
            ..Default::default()
        };
        assert_eq!(config.validate(), Err(ConfigError::NoStreamsEnabled));
    }

    #[test]
    fn test_invalid_scheme_rejected() {
        let config = LmaxConfig {
            ws_url: Url::parse("http://public-data-api.london-digital.lmax.com").unwrap(),
            ..Default::default()
        };
        assert_eq!(
            config.validate(),
            Err(ConfigError::InvalidWsScheme("http".to_string()))
        );
    }
}
