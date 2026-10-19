//! Configuration options and builder for the Coinbase Advanced Trade connector.

use std::time::Duration;
use url::Url;

/// Default WebSocket feed URL for Coinbase Advanced Trade.
pub const DEFAULT_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com/";

/// Coinbase Advanced Trade connector configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinbaseAdvancedConfig {
    /// WebSocket feed URL (default: `wss://advanced-trade-ws.coinbase.com`).
    pub ws_url: Url,
    /// Maximum number of products per WebSocket connection (default: 25).
    pub symbols_per_connection: usize,
    /// Active streams selection.
    pub streams: CoinbaseAdvancedStreams,
    /// Cooldown between resubscribe/resync attempts for a product (default: 5s).
    pub resync_timeout: Duration,
    /// Connection handshake timeout (default: 10s).
    pub connection_timeout: Duration,
    /// Silence watchdog timeout before reconnecting (default: 15s).
    pub silence_timeout: Duration,
    /// Sequence enforcement policy (Required or Optional). Default: Required.
    pub sequence_policy: mdc_core::SequencePolicy,
    /// Sequence reset threshold before declaring a sequence reset (default: 2).
    pub reset_threshold: u32,
    /// Injected session run token (generated if not provided).
    pub run_id: Option<mdc_core::RunId>,
}

impl Default for CoinbaseAdvancedConfig {
    fn default() -> Self {
        #[allow(clippy::expect_used)]
        let ws_url = Url::parse(DEFAULT_WS_URL).expect("static URL parse");
        Self {
            ws_url,
            symbols_per_connection: 25,
            streams: CoinbaseAdvancedStreams::default(),
            resync_timeout: Duration::from_secs(5),
            connection_timeout: Duration::from_secs(10),
            silence_timeout: Duration::from_secs(15),
            sequence_policy: mdc_core::SequencePolicy::Required,
            reset_threshold: 2,
            run_id: None,
        }
    }
}

/// Active stream selection for Coinbase Advanced Trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoinbaseAdvancedStreams {
    /// Enable Level 2 order book stream (`channel: "level2"` -> received as `l2_data`). Default: true.
    pub level2: bool,
    /// Enable heartbeat stream (`channel: "heartbeats"`). Default: true.
    pub heartbeats: bool,
    /// Enable public trades stream (`channel: "market_trades"`). Default: false.
    pub market_trades: bool,
}

impl Default for CoinbaseAdvancedStreams {
    fn default() -> Self {
        Self {
            level2: true,
            heartbeats: true,
            market_trades: false,
        }
    }
}

/// Errors occurring during configuration validation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// An invalid URL was supplied.
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    /// No symbols were provided.
    #[error("symbols list cannot be empty")]
    EmptySymbols,
    /// Reset threshold must be greater than zero.
    #[error("reset_threshold must be greater than 0")]
    InvalidResetThreshold,
}

impl CoinbaseAdvancedConfig {
    /// Creates a builder initialized with default configuration values.
    pub fn builder() -> CoinbaseAdvancedConfigBuilder {
        CoinbaseAdvancedConfigBuilder::default()
    }

    /// Validates the configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.reset_threshold == 0 {
            return Err(ConfigError::InvalidResetThreshold);
        }
        Ok(())
    }
}

/// Fluent builder for constructing a [`CoinbaseAdvancedConfig`].
#[derive(Debug, Clone, Default)]
pub struct CoinbaseAdvancedConfigBuilder {
    config: CoinbaseAdvancedConfig,
}

impl CoinbaseAdvancedConfigBuilder {
    /// Sets the WebSocket endpoint URL.
    pub fn ws_url(mut self, url: Url) -> Self {
        self.config.ws_url = url;
        self
    }

    /// Sets the maximum number of products assigned per WebSocket connection.
    pub fn symbols_per_connection(mut self, n: usize) -> Self {
        self.config.symbols_per_connection = n;
        self
    }

    /// Configures stream subscriptions.
    pub fn streams(mut self, streams: CoinbaseAdvancedStreams) -> Self {
        self.config.streams = streams;
        self
    }

    /// Enables or disables the Level 2 order book stream.
    pub fn level2(mut self, enabled: bool) -> Self {
        self.config.streams.level2 = enabled;
        self
    }

    /// Enables or disables the heartbeats stream.
    pub fn heartbeats(mut self, enabled: bool) -> Self {
        self.config.streams.heartbeats = enabled;
        self
    }

    /// Enables or disables the public market trades stream.
    pub fn market_trades(mut self, enabled: bool) -> Self {
        self.config.streams.market_trades = enabled;
        self
    }

    /// Sets the cooldown between resync attempts for a product.
    pub fn resync_timeout(mut self, timeout: Duration) -> Self {
        self.config.resync_timeout = timeout;
        self
    }

    /// Sets the connection handshake timeout.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    /// Sets the silence watchdog timeout.
    pub fn silence_timeout(mut self, timeout: Duration) -> Self {
        self.config.silence_timeout = timeout;
        self
    }

    /// Sets the sequence reset threshold before declaring a sequence reset. Clamps 0 to 1.
    pub fn with_reset_threshold(mut self, threshold: u32) -> Self {
        self.config.reset_threshold = if threshold == 0 { 1 } else { threshold };
        self
    }

    /// Sets the sequence reset threshold before declaring a sequence reset. Clamps 0 to 1.
    pub fn reset_threshold(self, threshold: u32) -> Self {
        self.with_reset_threshold(threshold)
    }

    /// Injects a specific `RunId` session token (primarily for tests).
    pub fn run_id(mut self, run_id: mdc_core::RunId) -> Self {
        self.config.run_id = Some(run_id);
        self
    }

    /// Validates and returns the built [`CoinbaseAdvancedConfig`].
    pub fn build(self) -> Result<CoinbaseAdvancedConfig, ConfigError> {
        self.config.validate()?;
        Ok(self.config)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_valid() {
        let config = CoinbaseAdvancedConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.ws_url.as_str(), DEFAULT_WS_URL);
        assert_eq!(config.symbols_per_connection, 25);
        assert_eq!(config.reset_threshold, 2);
        assert!(config.streams.level2);
        assert!(config.streams.heartbeats);
        assert!(!config.streams.market_trades);
    }

    #[test]
    fn test_zero_reset_threshold_rejected() {
        let config = CoinbaseAdvancedConfig {
            reset_threshold: 0,
            ..Default::default()
        };
        assert_eq!(config.validate(), Err(ConfigError::InvalidResetThreshold));
    }

    #[test]
    fn test_builder_with_reset_threshold_clamps_zero() {
        let config = CoinbaseAdvancedConfig::builder()
            .with_reset_threshold(0)
            .build()
            .unwrap();
        assert_eq!(config.reset_threshold, 1);

        let config_override = CoinbaseAdvancedConfig::builder()
            .with_reset_threshold(5)
            .build()
            .unwrap();
        assert_eq!(config_override.reset_threshold, 5);
    }

    #[test]
    fn test_builder_custom() {
        let custom_url = Url::parse("wss://custom.coinbase.com/ws").unwrap();
        let config = CoinbaseAdvancedConfig::builder()
            .ws_url(custom_url.clone())
            .symbols_per_connection(10)
            .market_trades(true)
            .resync_timeout(Duration::from_secs(3))
            .with_reset_threshold(4)
            .build()
            .unwrap();

        assert_eq!(config.ws_url, custom_url);
        assert_eq!(config.symbols_per_connection, 10);
        assert_eq!(config.reset_threshold, 4);
        assert!(config.streams.market_trades);
        assert_eq!(config.resync_timeout, Duration::from_secs(3));
    }
}
