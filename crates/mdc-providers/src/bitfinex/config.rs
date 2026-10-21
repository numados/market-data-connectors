//! Configuration types and defaults for Bitfinex WebSocket v2 connector.

use std::collections::HashMap;
use std::time::Duration;

use mdc_core::MdcError;
use mdc_core::model::{RunId, Symbol, SymbolTag};
use thiserror::Error;

/// Error returned when constructing or validating a `BitfinexConfig`.
///
/// The rules themselves are evaluated by the shared validators in
/// [`crate::config`]; a shared rejection is mapped back to the variant of the
/// rule that failed.
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

/// Order book depths accepted by Bitfinex (`len` parameter of the book channel).
pub const BOOK_DEPTHS: [usize; 4] = [1, 25, 100, 250];

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
    /// Validates configuration parameters, mapping a shared rejection to its [`ConfigError`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.check().map_err(|(legacy, _)| legacy)
    }

    /// Validates configuration parameters, surfacing the shared validator's rejection.
    pub(crate) fn validate_shared(&self) -> Result<(), MdcError> {
        self.check().map_err(|(_, shared)| shared)
    }

    /// Single owner of the rule list; a failure pairs the legacy variant with the shared rejection.
    fn check(&self) -> Result<(), (ConfigError, MdcError)> {
        crate::config::validate_symbol_count(self.symbols.len())
            .map_err(|shared| (ConfigError::EmptySymbols, shared))?;
        validate_book_depth(self.orderbook_depth).map_err(|shared| {
            (
                ConfigError::InvalidDepth {
                    depth: self.orderbook_depth,
                },
                shared,
            )
        })
    }
}

/// Validates a requested book depth against the Bitfinex allow-list.
pub fn validate_book_depth(depth: usize) -> Result<(), MdcError> {
    crate::config::validate_depth_in_set(depth, &BOOK_DEPTHS, "Bitfinex")
}
