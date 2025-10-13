//! Public connector and builder for Bitfinex market data feeds.

use std::future::Future;
use std::time::Duration;

use mdc_core::MdcError;
use mdc_core::catalog::{DiscoveryCapability, ProviderKind};
use mdc_core::model::capabilities::{
    CapabilityBounds, IdentityScope, IntegrityCapability, ProviderCapabilities, QuoteMode,
    ReplayCapability, SideSemantics, TimestampCapability, TradeCapability,
};
use mdc_core::model::{RunId, Symbol, SymbolTag};
use mdc_core::sink::StreamingSink;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::bitfinex::catalog::DEFAULT_BITFINEX_CATALOG_ENDPOINT;
use crate::bitfinex::config::{BitfinexConfig, ConfigError};
use crate::bitfinex::connection::run_connection_loop;
use crate::bitfinex::control::BitfinexHandle;

/// Static capability declaration for Bitfinex Spot.
pub static BITFINEX_CAPABILITIES: std::sync::LazyLock<ProviderCapabilities> =
    std::sync::LazyLock::new(|| ProviderCapabilities {
        quote_modes: vec![QuoteMode::L2Projection],
        trades: TradeCapability::Verified {
            identity_scope: IdentityScope::PerMarket,
            namespace_scheme: "bitfinex.spot.trade".to_string(),
            side_semantics: SideSemantics::AggressorTaker,
        },
        timestamps: TimestampCapability::ExchangeMilliseconds,
        integrity: IntegrityCapability::ChecksumCrc32,
        replay: ReplayCapability::None,
        bounds: CapabilityBounds {
            max_depth: Some(250),
            max_symbols_per_connection: 12,
        },
        discovery: DiscoveryCapability::complete(
            ProviderKind::Bitfinex,
            DEFAULT_BITFINEX_CATALOG_ENDPOINT,
        ),
        rest_quote: mdc_core::rest_quote::RestQuoteCapability::batch(
            100,
            true,
            TimestampCapability::ReceiptOnly,
            crate::bitfinex::rest_quote::DEFAULT_BITFINEX_QUOTE_ENDPOINT,
        ),
    });

/// Builder for constructing a configured [`BitfinexConnector`].
#[derive(Debug, Clone, Default)]
pub struct BitfinexConnectorBuilder {
    config: BitfinexConfig,
}

impl BitfinexConnectorBuilder {
    /// Creates a new builder with production defaults.
    pub fn new() -> Self {
        Self {
            config: BitfinexConfig::default(),
        }
    }

    /// Injects a specific `RunId` session token (primarily for tests).
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.config.run_id = Some(run_id);
        self
    }

    /// Returns the provider capabilities for Bitfinex.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &BITFINEX_CAPABILITIES
    }

    /// Sets the WebSocket endpoint URL.
    pub fn ws_url(mut self, url: impl Into<String>) -> Self {
        self.config.ws_url = url.into();
        self
    }

    /// Adds a market symbol to subscribe to (e.g. `"tBTCUSD"`).
    pub fn symbol(mut self, symbol: impl Into<Symbol>) -> Self {
        let sym = symbol.into();
        if !self.config.symbols.contains(&sym) {
            self.config.symbols.push(sym);
        }
        self
    }

    /// Adds a market symbol with an embedder-chosen opaque tag.
    pub fn symbol_with_tag(mut self, symbol: impl Into<Symbol>, tag: SymbolTag) -> Self {
        let sym = symbol.into();
        self.config.symbol_tags.insert(sym.clone(), tag);
        if !self.config.symbols.contains(&sym) {
            self.config.symbols.push(sym);
        }
        self
    }

    /// Appends multiple market symbols to the subscription list.
    pub fn symbols(mut self, symbols: impl IntoIterator<Item = impl Into<Symbol>>) -> Self {
        for s in symbols {
            let sym = s.into();
            if !self.config.symbols.contains(&sym) {
                self.config.symbols.push(sym);
            }
        }
        self
    }

    /// Sets the requested order book depth: 1, 25, 100, or 250.
    pub fn orderbook_depth(mut self, depth: usize) -> Result<Self, ConfigError> {
        if !matches!(depth, 1 | 25 | 100 | 250) {
            return Err(ConfigError::InvalidDepth { depth });
        }
        self.config.orderbook_depth = depth;
        Ok(self)
    }

    /// Enables or disables the order book depth stream.
    pub fn enable_depth(mut self, enable: bool) -> Self {
        self.config.streams.depth = enable;
        self
    }

    /// Enables or disables the public trades execution stream.
    pub fn enable_trades(mut self, enable: bool) -> Self {
        self.config.streams.trades = enable;
        self
    }

    /// Sets the silence duration before sending a WebSocket Ping.
    pub fn watchdog_ping_after(mut self, duration: Duration) -> Self {
        self.config.watchdog_ping_after = duration;
        self
    }

    /// Sets the silence duration before initiating a reconnect.
    pub fn watchdog_reconnect_after(mut self, duration: Duration) -> Self {
        self.config.watchdog_reconnect_after = duration;
        self
    }

    /// Sets the snapshot wait timeout before putting a channel into quarantine.
    pub fn resubscribe_timeout(mut self, duration: Duration) -> Self {
        self.config.resubscribe_timeout = duration;
        self
    }

    /// Sets the quarantine duration for a channel that failed snapshot restoration.
    pub fn quarantine_duration(mut self, duration: Duration) -> Self {
        self.config.quarantine_duration = duration;
        self
    }

    /// Validates configuration and returns a configured [`BitfinexConnector`].
    pub fn build(self) -> Result<BitfinexConnector, ConfigError> {
        self.config.validate()?;
        Ok(BitfinexConnector {
            config: self.config,
        })
    }
}

/// Standalone Bitfinex WebSocket v2 market data connector.
#[derive(Debug, Clone)]
pub struct BitfinexConnector {
    config: BitfinexConfig,
}

impl BitfinexConnector {
    /// Creates a new builder instance.
    pub fn builder() -> BitfinexConnectorBuilder {
        BitfinexConnectorBuilder::new()
    }

    /// Returns a reference to the active connector configuration.
    pub fn config(&self) -> &BitfinexConfig {
        &self.config
    }

    /// Returns the provider capabilities for Bitfinex.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &BITFINEX_CAPABILITIES
    }

    /// Spawns the connector on the active Tokio runtime.
    pub fn spawn<S: StreamingSink>(
        self,
        sink: S,
        cancel: CancellationToken,
    ) -> (
        BitfinexHandle,
        tokio::task::JoinHandle<Result<(), MdcError>>,
    ) {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let handle = BitfinexHandle::new(cmd_tx);
        let join_handle = tokio::spawn(run_connection_loop(self.config, sink, cmd_rx, cancel));
        (handle, join_handle)
    }

    /// Runs the connector as a future on the calling task (usable on `current_thread` runtimes).
    pub fn run<S: StreamingSink>(
        self,
        sink: S,
        cancel: CancellationToken,
    ) -> (BitfinexHandle, impl Future<Output = Result<(), MdcError>>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let handle = BitfinexHandle::new(cmd_tx);
        let fut = run_connection_loop(self.config, sink, cmd_rx, cancel);
        (handle, fut)
    }
}
