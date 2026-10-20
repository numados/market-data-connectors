//! Configuration types and defaults for Bitfinex WebSocket v2 connector.

use std::collections::HashMap;
use std::time::Duration;

use mdc_core::model::{RunId, Symbol, SymbolTag};
use thiserror::Error;

/// Error returned when constructing or validating a `BitfinexConfig`.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// No symbols provided to the connector.
    #[error("at least one symbol must be configured for Bitfinex")]
    EmptySymbols,
    /// Invalid order book depth length requested.
    #[error("invalid order book depth {depth}: Bitfinex allows 1, 25, 100, or 250")]
    InvalidDepth {
        /// The invalid depth requested.
        depth: usize,
    },
}

/// Bitfinex stream subscription flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitfinexStreams {
    /// Enable order book depth channel (`book`). Defaults to `true`.
    pub depth: bool,
    /// Enable public trade execution channel (`trades`). Defaults to `false`.
    pub trades: bool,
}

impl Default for BitfinexStreams {
    fn default() -> Self {
        Self {
            depth: true,
            trades: false,
        }
    }
}

/// Runtime configuration for the Bitfinex WebSocket connector.
#[derive(Debug, Clone)]
pub struct BitfinexConfig {
    /// WebSocket feed URL. Defaults to `"wss://api-pub.bitfinex.com/ws/2"`.
    pub ws_url: String,
    /// List of symbols to subscribe to (e.g. `"tBTCUSD"`).
    pub symbols: Vec<Symbol>,
    /// User-defined tags associated with each symbol.
    pub symbol_tags: HashMap<Symbol, SymbolTag>,
    /// Order book depth length: 1, 25, 100, or 250. Defaults to `250`.
    pub orderbook_depth: usize,
    /// Active data stream selections.
    pub streams: BitfinexStreams,
    /// Watchdog ping trigger duration without data frames. Defaults to 5s.
    pub watchdog_ping_after: Duration,
    /// Watchdog reconnect trigger duration without data frames. Defaults to 10s.
    pub watchdog_reconnect_after: Duration,
    /// Snapshot wait timeout before placing channel into quarantine. Defaults to 5s.
    pub resubscribe_timeout: Duration,
    /// Duration an instrument remains quarantined before re-attempting subscribe. Defaults to 30s.
    pub quarantine_duration: Duration,
    /// Initial reconnect backoff delay. Defaults to 100ms.
    pub reconnect_backoff_initial: Duration,
    /// Maximum reconnect backoff delay. Defaults to 10s.
    pub reconnect_backoff_max: Duration,
    /// Injected session run token (generated if not provided).
    pub run_id: Option<RunId>,
    /// WebSocket connect (TCP, TLS, and upgrade handshake) timeout. Default: 10 seconds.
    pub connection_timeout: Duration,
}

impl Default for BitfinexConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://api-pub.bitfinex.com/ws/2".to_string(),
            symbols: Vec::new(),
            symbol_tags: HashMap::new(),
            orderbook_depth: 250,
            streams: BitfinexStreams::default(),
            watchdog_ping_after: Duration::from_secs(5),
            watchdog_reconnect_after: Duration::from_secs(10),
            resubscribe_timeout: Duration::from_secs(5),
            quarantine_duration: Duration::from_secs(30),
            reconnect_backoff_initial: Duration::from_millis(100),
            reconnect_backoff_max: Duration::from_secs(10),
            run_id: None,
            connection_timeout: Duration::from_secs(10),
        }
    }
}

impl BitfinexConfig {
    /// Validates configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.symbols.is_empty() {
            return Err(ConfigError::EmptySymbols);
        }
        if !matches!(self.orderbook_depth, 1 | 25 | 100 | 250) {
            return Err(ConfigError::InvalidDepth {
                depth: self.orderbook_depth,
            });
        }
        Ok(())
    }
}
