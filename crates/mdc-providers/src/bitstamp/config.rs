//! Configuration options for the Bitstamp market data connector.

use std::time::Duration;
use url::Url;

/// Bitstamp connector configuration.
#[derive(Debug, Clone)]
pub struct BitstampConfig {
    /// WebSocket feed URL (default: `wss://ws.bitstamp.net`).
    pub ws_url: Url,
    /// REST API base URL (default: `https://www.bitstamp.net/api/v2`).
    pub rest_url: Url,
    /// Maximum number of symbols per WebSocket connection (default: 25).
    pub symbols_per_connection: usize,
    /// Stream subscription settings.
    pub streams: BitstampStreams,
    /// Cooldown period before retrying a failed REST snapshot (default: 5s).
    pub snapshot_retry_cooldown: Duration,
    /// Timeout for REST HTTP requests (default: 5s).
    pub request_timeout: Duration,
    /// Maximum number of deltas to buffer per symbol while awaiting a snapshot (default: 1024).
    pub buffer_capacity: usize,
}

impl Default for BitstampConfig {
    fn default() -> Self {
        #[allow(clippy::expect_used)]
        let ws_url = Url::parse("wss://ws.bitstamp.net").expect("static URL parse");
        #[allow(clippy::expect_used)]
        let rest_url = Url::parse("https://www.bitstamp.net/api/v2/").expect("static URL parse");
        Self {
            ws_url,
            rest_url,
            symbols_per_connection: 25,
            streams: BitstampStreams::default(),
            snapshot_retry_cooldown: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            buffer_capacity: 1024,
        }
    }
}

/// Active stream selection for Bitstamp connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitstampStreams {
    /// Enable `diff_order_book_{pair}` order book updates (default: true).
    pub depth: bool,
    /// Enable `live_trades_{pair}` public trade stream (default: false).
    pub trades: bool,
}

impl Default for BitstampStreams {
    fn default() -> Self {
        Self {
            depth: true,
            trades: false,
        }
    }
}

/// Errors occurring during configuration validation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// Symbol list was empty.
    #[error("symbols list cannot be empty")]
    EmptySymbols,
    /// Streaming or market data sink was not provided.
    #[error("market data sink is required")]
    MissingSink,
    /// WebSocket URL scheme was invalid (must be ws:// or wss://).
    #[error("ws_url scheme must be ws or wss: {0}")]
    InvalidWsScheme(String),
    /// REST URL scheme was invalid (must be http:// or https://).
    #[error("rest_url scheme must be http or https: {0}")]
    InvalidRestScheme(String),
    /// At least one stream must be enabled.
    #[error("at least one stream (depth or trades) must be enabled")]
    NoStreamsEnabled,
}

impl BitstampConfig {
    /// Validates the configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let ws_scheme = self.ws_url.scheme();
        if ws_scheme != "ws" && ws_scheme != "wss" {
            return Err(ConfigError::InvalidWsScheme(ws_scheme.to_string()));
        }
        let rest_scheme = self.rest_url.scheme();
        if rest_scheme != "http" && rest_scheme != "https" {
            return Err(ConfigError::InvalidRestScheme(rest_scheme.to_string()));
        }
        if !self.streams.depth && !self.streams.trades {
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
        let config = BitstampConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.symbols_per_connection, 25);
        assert_eq!(config.buffer_capacity, 1024);
        assert!(config.streams.depth);
        assert!(!config.streams.trades);
    }

    #[test]
    fn test_no_streams_enabled_rejected() {
        let config = BitstampConfig {
            streams: BitstampStreams {
                depth: false,
                trades: false,
            },
            ..Default::default()
        };
        assert_eq!(config.validate(), Err(ConfigError::NoStreamsEnabled));
    }

    #[test]
    fn test_invalid_scheme_rejected() {
        let config = BitstampConfig {
            ws_url: Url::parse("http://ws.bitstamp.net").unwrap(),
            ..Default::default()
        };
        assert_eq!(
            config.validate(),
            Err(ConfigError::InvalidWsScheme("http".to_string()))
        );

        let config2 = BitstampConfig {
            rest_url: Url::parse("ftp://www.bitstamp.net").unwrap(),
            ..Default::default()
        };
        assert_eq!(
            config2.validate(),
            Err(ConfigError::InvalidRestScheme("ftp".to_string()))
        );
    }
}
