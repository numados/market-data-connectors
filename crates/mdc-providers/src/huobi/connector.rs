use mdc_core::catalog::{DiscoveryCapability, ProviderKind};
use mdc_core::model::capabilities::{
    CapabilityBounds, IdentityScope, IntegrityCapability, ProviderCapabilities, QuoteMode,
    ReplayCapability, SideSemantics, TimestampCapability, TradeCapability,
};
use mdc_core::{MarketDataSink, MdcError, Owned, RunId, StreamingSink, Symbol, SymbolTag, VenueId};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::huobi::catalog::DEFAULT_HUOBI_CATALOG_ENDPOINT;
use crate::huobi::config::{ConfigError, HuobiConfig, HuobiStreams};
use crate::huobi::connection::run_connection_task;
use crate::huobi::control::{Command, ConnectorHandle};

/// Static capability declaration for Huobi Spot.
pub static HUOBI_CAPABILITIES: std::sync::LazyLock<ProviderCapabilities> =
    std::sync::LazyLock::new(|| ProviderCapabilities {
        quote_modes: vec![QuoteMode::L2Projection],
        trades: TradeCapability::Verified {
            identity_scope: IdentityScope::PerMarket,
            namespace_scheme: "huobi.spot.trade".to_string(),
            side_semantics: SideSemantics::AggressorTaker,
        },
        timestamps: TimestampCapability::ExchangeMilliseconds,
        integrity: IntegrityCapability::SequenceNumbers,
        replay: ReplayCapability::None,
        bounds: CapabilityBounds {
            max_depth: Some(150),
            max_symbols_per_connection: 25,
        },
        discovery: DiscoveryCapability::complete(
            ProviderKind::Huobi,
            DEFAULT_HUOBI_CATALOG_ENDPOINT,
        ),
        rest_quote: mdc_core::rest_quote::RestQuoteCapability::single(
            true,
            TimestampCapability::ExchangeMilliseconds,
            crate::huobi::rest_quote::DEFAULT_HUOBI_QUOTE_ENDPOINT,
        ),
    });

/// Huobi (HTX) Market-By-Price WebSocket market data connector.
pub struct HuobiConnector<S> {
    config: HuobiConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    run_id: Option<RunId>,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
}

impl HuobiConnector<()> {
    /// Creates a builder initialized with the given `HuobiConfig`.
    pub fn builder(config: HuobiConfig) -> HuobiConnectorBuilder<()> {
        HuobiConnectorBuilder::new(config)
    }
}

impl<S: StreamingSink + Clone> HuobiConnector<S> {
    /// Returns the provider capabilities for Huobi.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &HUOBI_CAPABILITIES
    }

    /// Returns a new cloneable `ConnectorHandle` for controlling this connector.
    pub fn handle(&self) -> ConnectorHandle {
        ConnectorHandle::new(self.command_tx.clone())
    }

    /// Returns the venue identifier for Huobi.
    pub fn venue(&self) -> VenueId {
        VenueId::Huobi
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
        let run_id = self.run_id.unwrap_or_else(|| {
            let now = crate::now_nanos().0 as u128;
            RunId::from_nanos(now)
        });
        let mut conn_tasks = JoinSet::new();
        let mut aux_tasks = JoinSet::new();
        let chunks: Vec<Vec<(Symbol, SymbolTag)>> = self
            .symbols
            .chunks(self.config.symbols_per_connection)
            .map(<[(Symbol, SymbolTag)]>::to_vec)
            .collect();

        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<Command>(32);
        let mut command_rx = self.command_rx;
        let cmd_broadcast = broadcast_tx.clone();
        let cancel_cmd = cancel.clone();

        aux_tasks.spawn(async move {
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
            aux_tasks.spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_forward.cancelled() => break,
                        cmd = b_rx.recv() => {
                            match cmd {
                                Ok(c) => {
                                    if task_cmd_tx.send(c).await.is_err() {
                                        break;
                                    }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                    tracing::warn!(connection_id, skipped, "Command broadcast lagged; commands dropped");
                                }
                            }
                        }
                    }
                }
            });

            conn_tasks.spawn(async move {
                run_connection_task(
                    connection_id,
                    chunk,
                    config,
                    sink,
                    task_cmd_rx,
                    cancel_conn,
                    run_id,
                )
                .await;
            });
        }

        let mut result = Ok(());
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    while let Some(res) = conn_tasks.join_next().await {
                        if let Err(e) = res {
                            if !e.is_cancelled() {
                                result = Err(MdcError::Protocol(format!(
                                    "connection worker task panicked: {e}"
                                )));
                            }
                        }
                    }
                    break;
                }
                res = conn_tasks.join_next() => {
                    match res {
                        Some(Ok(())) => {
                            if conn_tasks.is_empty() {
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            if !e.is_cancelled() {
                                result = Err(MdcError::Protocol(format!(
                                    "connection worker task panicked: {e}"
                                )));
                            }
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
        conn_tasks.abort_all();
        aux_tasks.abort_all();
        while conn_tasks.join_next().await.is_some() {}
        while aux_tasks.join_next().await.is_some() {}

        result
    }
}

/// Fluent builder for constructing a [`HuobiConnector`].
pub struct HuobiConnectorBuilder<S = ()> {
    config: HuobiConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
    run_id: Option<RunId>,
}

impl HuobiConnectorBuilder<()> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: HuobiConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
            run_id: None,
        }
    }
}

impl<S> HuobiConnectorBuilder<S> {
    /// Explicitly sets the `RunId` for this connector session.
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Returns the provider capabilities for Huobi.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &HUOBI_CAPABILITIES
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

    /// Sets the MBP depth levels (150 or 400).
    pub fn mbp_levels(mut self, levels: u32) -> Result<Self, ConfigError> {
        if levels != 150 && levels != 400 {
            return Err(ConfigError::InvalidMbpLevels(levels));
        }
        self.config.mbp_levels = levels;
        Ok(self)
    }

    /// Configures stream subscriptions.
    pub fn streams(mut self, streams: HuobiStreams) -> Self {
        self.config.streams = streams;
        self
    }

    /// Enables or disables the incremental MBP order book stream.
    pub fn mbp(mut self, enabled: bool) -> Self {
        self.config.streams.mbp = enabled;
        self
    }

    /// Enables or disables the public trades stream.
    pub fn trades(mut self, enabled: bool) -> Self {
        self.config.streams.trades = enabled;
        self
    }

    /// Sets the maximum number of symbols per WebSocket connection.
    pub fn symbols_per_connection(mut self, n: usize) -> Self {
        self.config.symbols_per_connection = n;
        self
    }

    /// Configures the streaming sink for the connector.
    pub fn sink<NewS>(self, sink: NewS) -> HuobiConnectorBuilder<NewS> {
        HuobiConnectorBuilder {
            config: self.config,
            symbols: self.symbols,
            sink: Some(sink),
            run_id: self.run_id,
        }
    }

    /// Configures the streaming sink for the connector (alias for [`Self::sink`]).
    pub fn streaming_sink<NewS>(self, sink: NewS) -> HuobiConnectorBuilder<NewS> {
        self.sink(sink)
    }

    /// Configures an owned [`MarketDataSink`] by wrapping it in an [`Owned`] adapter.
    pub fn owned_sink<O: MarketDataSink>(self, sink: O) -> HuobiConnectorBuilder<Owned<O>> {
        self.sink(Owned::new(sink))
    }

    /// Builds the `HuobiConnector` and its operator `ConnectorHandle`.
    pub fn build(self) -> Result<(HuobiConnector<S>, ConnectorHandle), MdcError> {
        self.config
            .validate()
            .map_err(|e| MdcError::Config(e.to_string()))?;

        crate::config::validate_symbols(&self.symbols)?;
        crate::config::validate_symbols_per_connection(self.config.symbols_per_connection)?;
        crate::config::validate_resync_buffer_capacity(self.config.resync_buffer_capacity)?;

        let sink = self
            .sink
            .ok_or_else(|| MdcError::Config("Streaming sink is required".into()))?;

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = HuobiConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            run_id: self.run_id,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
