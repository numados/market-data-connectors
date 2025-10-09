use mdc_core::catalog::{DiscoveryCapability, ProviderKind};
use mdc_core::model::capabilities::{
    CapabilityBounds, IdentityScope, IntegrityCapability, ProviderCapabilities, QuoteMode,
    ReplayCapability, SideSemantics, TimestampCapability, TradeCapability,
};
use mdc_core::{MarketDataSink, MdcError, Owned, StreamingSink, Symbol, SymbolTag, VenueId};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::gateio::catalog::DEFAULT_GATEIO_CATALOG_ENDPOINT;
use crate::gateio::config::{ConfigError, GateioConfig, GateioStreams};
use crate::gateio::connection::run_connection_task;
use crate::gateio::control::{Command, ConnectorHandle};

/// Static capability declaration for Gate.io Spot.
pub static GATEIO_CAPABILITIES: std::sync::LazyLock<ProviderCapabilities> =
    std::sync::LazyLock::new(|| ProviderCapabilities {
        quote_modes: vec![QuoteMode::L2Projection],
        trades: TradeCapability::Verified {
            identity_scope: IdentityScope::PerMarket,
            namespace_scheme: "gateio.spot.trade".to_string(),
            side_semantics: SideSemantics::AggressorTaker,
        },
        timestamps: TimestampCapability::ExchangeNanoseconds,
        integrity: IntegrityCapability::SequenceNumbers,
        replay: ReplayCapability::None,
        bounds: CapabilityBounds {
            max_depth: Some(100),
            max_symbols_per_connection: 25,
        },
        discovery: DiscoveryCapability::complete(
            ProviderKind::Gateio,
            DEFAULT_GATEIO_CATALOG_ENDPOINT,
        ),
        rest_quote: false,
    });

/// Gate.io WebSocket v4 market data connector.
pub struct GateioConnector<S> {
    config: GateioConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
}

impl<S: StreamingSink + Clone> GateioConnector<S> {
    /// Creates a builder initialized with the given `GateioConfig`.
    pub fn builder(config: GateioConfig) -> GateioConnectorBuilder<S> {
        GateioConnectorBuilder::new(config)
    }

    /// Returns the provider capabilities for Gate.io.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &GATEIO_CAPABILITIES
    }

    /// Returns a new cloneable `ConnectorHandle` for controlling this connector.
    pub fn handle(&self) -> ConnectorHandle {
        ConnectorHandle::new(self.command_tx.clone())
    }

    /// Returns the venue identifier for Gate.io.
    pub fn venue(&self) -> VenueId {
        VenueId::Gateio
    }

    /// Spawns the connector on the tokio runtime in a background task.
    pub fn spawn(
        self,
        cancel: CancellationToken,
    ) -> (
        ConnectorHandle,
        tokio::task::JoinHandle<Result<(), MdcError>>,
    ) {
        let handle = self.handle();
        let join = tokio::spawn(async move { self.run(cancel).await });
        (handle, join)
    }

    /// Runs the connector until cancelled.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), MdcError> {
        let mut join_set = JoinSet::new();
        let chunks: Vec<Vec<(Symbol, SymbolTag)>> = self
            .symbols
            .chunks(self.config.symbols_per_connection)
            .map(<[(Symbol, SymbolTag)]>::to_vec)
            .collect();

        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<Command>(32);
        let mut command_rx = self.command_rx;
        let cmd_broadcast = broadcast_tx.clone();
        let cancel_cmd = cancel.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel_cmd.cancelled() => break,
                    cmd_opt = command_rx.recv() => {
                        match cmd_opt {
                            Some(cmd) => {
                                let _ = cmd_broadcast.send(cmd);
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        for (i, chunk) in chunks.into_iter().enumerate() {
            let connection_id = (i + 1) as u32;
            let sink = self.sink.clone();
            let config = self.config.clone();
            let cancel_conn = cancel.clone();

            let mut b_rx = broadcast_tx.subscribe();
            let (task_cmd_tx, task_cmd_rx) = mpsc::channel(16);

            let cancel_forward = cancel.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_forward.cancelled() => break,
                        cmd = b_rx.recv() => {
                            if let Ok(c) = cmd {
                                if task_cmd_tx.send(c).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            join_set.spawn(async move {
                run_connection_task(connection_id, chunk, config, sink, task_cmd_rx, cancel_conn)
                    .await;
            });
        }

        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                if !e.is_cancelled() {
                    return Err(MdcError::Transport(Box::new(std::io::Error::other(
                        format!("Gate.io connection task panicked: {e}"),
                    ))));
                }
            }
        }

        Ok(())
    }
}

/// Fluent builder for constructing a `GateioConnector`.
pub struct GateioConnectorBuilder<S> {
    config: GateioConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
}

impl<S> GateioConnectorBuilder<S> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: GateioConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
        }
    }

    /// Returns the provider capabilities for Gate.io.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &GATEIO_CAPABILITIES
    }

    /// Adds a single symbol to subscribe to (with default tag 0).
    pub fn symbol(mut self, symbol: impl Into<Symbol>) -> Self {
        self.symbols.push((symbol.into(), SymbolTag::default()));
        self
    }

    /// Sets the symbols to subscribe to (with default tag 0).
    pub fn symbols<I, T>(mut self, symbols: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Symbol>,
    {
        for s in symbols {
            self.symbols.push((s.into(), SymbolTag::default()));
        }
        self
    }

    /// Adds a symbol with an associated consumer tag.
    pub fn symbol_with_tag(mut self, symbol: impl Into<Symbol>, tag: SymbolTag) -> Self {
        self.symbols.push((symbol.into(), tag));
        self
    }

    /// Overrides the WebSocket base URL.
    pub fn ws_url(mut self, url: Url) -> Self {
        self.config.ws_url = url;
        self
    }

    /// Sets the order book depth levels (50 or 400).
    pub fn orderbook_levels(mut self, levels: u32) -> Result<Self, ConfigError> {
        if levels != 50 && levels != 400 {
            return Err(ConfigError::InvalidOrderbookLevels(levels));
        }
        self.config.orderbook_levels = levels;
        Ok(self)
    }

    /// Configures stream subscriptions.
    pub fn streams(mut self, streams: GateioStreams) -> Self {
        self.config.streams = streams;
        self
    }

    /// Configures the streaming sink for this connector.
    pub fn streaming_sink(mut self, sink: S) -> Self {
        self.sink = Some(sink);
        self
    }
}

impl<M: MarketDataSink + Clone> GateioConnectorBuilder<Owned<M>> {
    /// Configures an owned `MarketDataSink` by automatically wrapping it in an `Owned` adapter.
    pub fn sink(self, sink: M) -> Self {
        Self {
            config: self.config,
            symbols: self.symbols,
            sink: Some(Owned::new(sink)),
        }
    }
}

impl<S: StreamingSink + Clone> GateioConnectorBuilder<S> {
    /// Builds the `GateioConnector` and returns it along with its `ConnectorHandle`.
    pub fn build(self) -> Result<(GateioConnector<S>, ConnectorHandle), MdcError> {
        self.config
            .validate()
            .map_err(|e| MdcError::Protocol(format!("Invalid Gate.io configuration: {e}")))?;

        let sink = self
            .sink
            .ok_or_else(|| MdcError::Protocol("Streaming sink is required".into()))?;

        if self.symbols.is_empty() {
            return Err(MdcError::Protocol(
                "At least one symbol must be subscribed".into(),
            ));
        }

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = GateioConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
