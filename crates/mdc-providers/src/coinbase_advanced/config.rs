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
    /// Symbols per connection must be greater than zero.
    #[error("symbols_per_connection must be greater than 0")]
    ZeroSymbolsPerConnection,
}

impl CoinbaseAdvancedConfig {
    /// Creates a builder initialized with default configuration values.
    pub fn builder() -> CoinbaseAdvancedConfigBuilder {
        CoinbaseAdvancedConfigBuilder::default()
    }

    /// Validates the configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.symbols_per_connection == 0 {
            return Err(ConfigError::ZeroSymbolsPerConnection);
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
        assert!(config.streams.level2);
        assert!(config.streams.heartbeats);
        assert!(!config.streams.market_trades);
    }

    #[test]
    fn test_zero_symbols_per_connection() {
        let mut config = CoinbaseAdvancedConfig::default();
        config.symbols_per_connection = 0;
        assert_eq!(
            config.validate(),
            Err(ConfigError::ZeroSymbolsPerConnection)
        );
    }

    #[test]
    fn test_builder_custom() {
        let custom_url = Url::parse("wss://custom.coinbase.com/ws").unwrap();
        let config = CoinbaseAdvancedConfig::builder()
            .ws_url(custom_url.clone())
            .symbols_per_connection(10)
            .market_trades(true)
            .resync_timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        assert_eq!(config.ws_url, custom_url);
        assert_eq!(config.symbols_per_connection, 10);
        assert!(config.streams.market_trades);
        assert_eq!(config.resync_timeout, Duration::from_secs(3));
    }
}
