//! Configuration options for the Gate.io market data connector.

use std::time::Duration;
use url::Url;

/// Gate.io connector configuration.
#[derive(Debug, Clone)]
pub struct GateioConfig {
    /// WebSocket feed URL (default: `wss://api.gateio.ws/ws/v4/`).
    pub ws_url: Url,
    /// Maximum number of symbols per WebSocket connection (default: 25).
    pub symbols_per_connection: usize,
    /// Order book depth levels in payload name, e.g. 50 or 400 (default: 400).
    pub orderbook_levels: u32,
    /// Stream subscription settings.
    pub streams: GateioStreams,
    /// Maximum number of diffs buffered during snapshot synchronization (default: 1,000).
    pub resync_buffer_capacity: usize,
    /// Timeout before triggering a resubscribe retry on resync failure (default: 5s).
    pub resync_timeout: Duration,
    /// Silence duration after which the connector sends a Ping frame (default: 30s).
    pub watchdog_ping_after: Duration,
    /// Silence duration after which the connector forces a reconnect (default: 45s).
    pub watchdog_reconnect_after: Duration,
    /// WebSocket connect (TCP, TLS, and upgrade handshake) timeout. Default: 10 seconds.
    pub connection_timeout: Duration,
}

impl Default for GateioConfig {
    fn default() -> Self {
        #[allow(clippy::expect_used)]
        let ws_url = Url::parse("wss://api.gateio.ws/ws/v4/").expect("static URL parse");
        Self {
            ws_url,
            symbols_per_connection: 25,
            orderbook_levels: 400,
            streams: GateioStreams::default(),
            resync_buffer_capacity: 1000,
            resync_timeout: Duration::from_secs(5),
            watchdog_ping_after: Duration::from_secs(30),
            watchdog_reconnect_after: Duration::from_secs(45),
            connection_timeout: Duration::from_secs(10),
        }
    }
}

/// Active stream selection for Gate.io connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateioStreams {
    /// Enable `spot.obu` order book updates (default: true).
    pub depth: bool,
    /// Enable `spot.trades` public trade stream (default: false).
    pub trades: bool,
}

impl Default for GateioStreams {
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
    /// Specified order book levels is invalid (only 50 and 400 supported).
    #[error("invalid orderbook levels {0}; only 50 and 400 are supported by Gate.io")]
    InvalidOrderbookLevels(u32),
}

impl GateioConfig {
    /// Validates the configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.orderbook_levels != 50 && self.orderbook_levels != 400 {
            return Err(ConfigError::InvalidOrderbookLevels(self.orderbook_levels));
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
        let config = GateioConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.symbols_per_connection, 25);
        assert_eq!(config.orderbook_levels, 400);
        assert!(config.streams.depth);
        assert!(!config.streams.trades);
    }

    #[test]
    fn test_invalid_levels_rejected() {
        let config = GateioConfig {
            orderbook_levels: 100,
            ..GateioConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(ConfigError::InvalidOrderbookLevels(100))
        );
        let valid_config = GateioConfig {
            orderbook_levels: 50,
            ..GateioConfig::default()
        };
        assert!(valid_config.validate().is_ok());
    }
}
