//! Configuration options and builder for the Huobi (HTX) market data connector.

use std::time::Duration;
use url::Url;

/// Default WebSocket feed URL for Huobi MBP streams.
pub const DEFAULT_WS_URL: &str = "wss://api.huobi.pro/feed";

/// Huobi connector configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HuobiConfig {
    /// WebSocket feed URL (default: `wss://api.huobi.pro/feed`).
    pub ws_url: Url,
    /// Maximum number of symbols per WebSocket connection (default: 25).
    pub symbols_per_connection: usize,
    /// MBP order book levels: 150 or 400 (default: 400).
    pub mbp_levels: u32,
    /// Active streams configuration.
    pub streams: HuobiStreams,
    /// Maximum number of deltas buffered during snapshot synchronization (default: 1,000).
    pub resync_buffer_capacity: usize,
    /// Timeout before triggering a resync retry on failure (default: 5s).
    pub resync_timeout: Duration,
    /// Cooldown between snapshot `req` attempts (default: 5s).
    pub snapshot_cooldown: Duration,
    /// Connection handshake timeout (default: 10s).
    pub connection_timeout: Duration,
    /// Silence watchdog timeout before reconnecting (default: 15s).
    pub silence_timeout: Duration,
}

impl Default for HuobiConfig {
    fn default() -> Self {
        #[allow(clippy::expect_used)]
        let ws_url = Url::parse(DEFAULT_WS_URL).expect("static URL parse");
        Self {
            ws_url,
            symbols_per_connection: 25,
            mbp_levels: 400,
            streams: HuobiStreams::default(),
            resync_buffer_capacity: 1000,
            resync_timeout: Duration::from_secs(5),
            snapshot_cooldown: Duration::from_secs(5),
            connection_timeout: Duration::from_secs(10),
            silence_timeout: Duration::from_secs(15),
        }
    }
}

/// Active stream selection for Huobi connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HuobiStreams {
    /// Enable Market-By-Price incremental order book updates (default: true).
    pub mbp: bool,
    /// Enable public trades stream `market.{symbol}.trade.detail` (default: false).
    pub trades: bool,
}

impl Default for HuobiStreams {
    fn default() -> Self {
        Self {
            mbp: true,
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

    /// Specified MBP levels is invalid (only 150 and 400 supported for incremental streams).
    #[error("invalid mbp levels {0}; only 150 and 400 are supported for incremental updates")]
    InvalidMbpLevels(u32),

    /// Invalid URL string.
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
}

impl HuobiConfig {
    /// Returns a new `HuobiConfigBuilder` initialized with defaults.
    pub fn builder() -> HuobiConfigBuilder {
        HuobiConfigBuilder::new()
    }

    /// Validates the configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.mbp_levels != 150 && self.mbp_levels != 400 {
            return Err(ConfigError::InvalidMbpLevels(self.mbp_levels));
        }
        Ok(())
    }
}

/// Fluent builder for [`HuobiConfig`].
#[derive(Debug, Clone)]
pub struct HuobiConfigBuilder {
    config: HuobiConfig,
}

impl Default for HuobiConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl HuobiConfigBuilder {
    /// Creates a new `HuobiConfigBuilder` with default parameters.
    pub fn new() -> Self {
        Self {
            config: HuobiConfig::default(),
        }
    }

    /// Sets the WebSocket endpoint URL.
    pub fn ws_url(mut self, url: Url) -> Self {
        self.config.ws_url = url;
        self
    }

    /// Sets the WebSocket endpoint URL from a string slice.
    pub fn ws_url_str(mut self, url_str: &str) -> Result<Self, ConfigError> {
        self.config.ws_url =
            Url::parse(url_str).map_err(|e| ConfigError::InvalidUrl(e.to_string()))?;
        Ok(self)
    }

    /// Sets the maximum number of symbols per WebSocket connection.
    pub fn symbols_per_connection(mut self, n: usize) -> Self {
        self.config.symbols_per_connection = n;
        self
    }

    /// Sets the MBP depth levels (150 or 400).
    pub fn mbp_levels(mut self, levels: u32) -> Self {
        self.config.mbp_levels = levels;
        self
    }

    /// Enables or disables the MBP order book stream.
    pub fn mbp(mut self, enabled: bool) -> Self {
        self.config.streams.mbp = enabled;
        self
    }

    /// Enables or disables public trades stream.
    pub fn trades(mut self, enabled: bool) -> Self {
        self.config.streams.trades = enabled;
        self
    }

    /// Sets the delta resync buffer capacity.
    pub fn resync_buffer_capacity(mut self, capacity: usize) -> Self {
        self.config.resync_buffer_capacity = capacity;
        self
    }

    /// Sets the resync timeout.
    pub fn resync_timeout(mut self, timeout: Duration) -> Self {
        self.config.resync_timeout = timeout;
        self
    }

    /// Sets the silence watchdog timeout.
    pub fn silence_timeout(mut self, timeout: Duration) -> Self {
        self.config.silence_timeout = timeout;
        self
    }

    /// Builds and validates the `HuobiConfig`.
    pub fn build(self) -> Result<HuobiConfig, ConfigError> {
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
        let config = HuobiConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.mbp_levels, 400);
        assert_eq!(config.symbols_per_connection, 25);
        assert!(config.streams.mbp);
        assert!(!config.streams.trades);
    }

    #[test]
    fn test_builder_custom() {
        let config = HuobiConfig::builder()
            .mbp_levels(150)
            .trades(true)
            .symbols_per_connection(10)
            .build()
            .unwrap();
        assert_eq!(config.mbp_levels, 150);
        assert!(config.streams.trades);
        assert_eq!(config.symbols_per_connection, 10);
    }

    #[test]
    fn test_invalid_mbp_levels() {
        let res = HuobiConfig::builder().mbp_levels(50).build();
        assert_eq!(res, Err(ConfigError::InvalidMbpLevels(50)));
    }
}
