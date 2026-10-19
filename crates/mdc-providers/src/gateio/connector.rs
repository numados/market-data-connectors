use mdc_core::catalog::{DiscoveryCapability, ProviderKind};
use mdc_core::model::capabilities::{
    CapabilityBounds, IdentityScope, IntegrityCapability, ProviderCapabilities, QuoteMode,
    ReplayCapability, SideSemantics, TimestampCapability, TradeCapability,
};
use mdc_core::{MarketDataSink, MdcError, Owned, RunId, StreamingSink, Symbol, SymbolTag, VenueId};
use std::sync::Arc;
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
        rest_quote: mdc_core::rest_quote::RestQuoteCapability::single(
            true,
            TimestampCapability::ReceiptOnly,
            crate::gateio::rest_quote::DEFAULT_GATEIO_QUOTE_ENDPOINT,
        ),
    });

/// Thread-safe factory producing per-symbol resynchronization policies.
pub type ResyncPolicyFactory =
    Arc<dyn Fn() -> Box<dyn mdc_core::resync::ResyncPolicy> + Send + Sync>;

/// Gate.io WebSocket v4 market data connector.
pub struct GateioConnector<S> {
    config: GateioConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    run_id: Option<RunId>,
    policy_factory: ResyncPolicyFactory,
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
        let run_id = self.run_id.unwrap_or_else(crate::generate_run_id);
        let generation_allocator = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut conn_tasks = JoinSet::new();
        let mut aux_tasks = JoinSet::new();
        let chunks: Vec<Vec<(Symbol, SymbolTag)>> = self
            .symbols
            .chunks(self.config.symbols_per_connection)
            .map(<[(Symbol, SymbolTag)]>::to_vec)
            .collect();
        let total_connections = chunks.len();
        let mut sink_failure_count = 0;

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
            let gen_alloc = generation_allocator.clone();

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

            let policy_factory = self.policy_factory.clone();
            conn_tasks.spawn(async move {
                run_connection_task(
                    connection_id,
                    chunk,
                    config,
                    sink,
                    task_cmd_rx,
                    cancel_conn,
                    run_id,
                    gen_alloc,
                    policy_factory,
                )
                .await
            });
        }

        let mut result = Ok(());
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    while let Some(res) = conn_tasks.join_next().await {
                        if let Err(e) = res {
                            if !e.is_cancelled() {
                                result = Err(MdcError::Transport(Box::new(std::io::Error::other(
                                    format!("Gate.io connection task panicked: {e}"),
                                ))));
                            }
                        }
                    }
                    break;
                }
                res = conn_tasks.join_next() => {
                    match res {
                        Some(Ok(task_res)) => {
                            if let Err(crate::sink_util::SinkError) = task_res {
                                sink_failure_count += 1;
                            }
                            if conn_tasks.is_empty() {
                                if total_connections > 0 && sink_failure_count == total_connections {
                                    result = Err(MdcError::Sink(Box::new(crate::sink_util::SinkError)));
                                }
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            if !e.is_cancelled() {
                                result = Err(MdcError::Transport(Box::new(std::io::Error::other(
                                    format!("Gate.io connection task panicked: {e}"),
                                ))));
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

/// Fluent builder for constructing a `GateioConnector`.
pub struct GateioConnectorBuilder<S> {
    config: GateioConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
    run_id: Option<RunId>,
    policy_factory: Option<ResyncPolicyFactory>,
}

impl<S> GateioConnectorBuilder<S> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: GateioConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
            run_id: None,
            policy_factory: None,
        }
    }

    /// Injects a custom resynchronization policy prototype cloned for each symbol FSM.
    pub fn resync_policy<P: mdc_core::resync::ResyncPolicy + Clone + Send + Sync + 'static>(
        mut self,
        policy: P,
    ) -> Self {
        self.policy_factory = Some(Arc::new(move || Box::new(policy.clone())));
        self
    }

    /// Injects a custom factory producing resynchronization policies.
    pub fn resync_policy_factory(mut self, factory: ResyncPolicyFactory) -> Self {
        self.policy_factory = Some(factory);
        self
    }

    /// Explicitly sets the `RunId` for this connector session.
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.run_id = Some(run_id);
        self
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
            run_id: self.run_id,
            policy_factory: self.policy_factory,
        }
    }
}

impl<S: StreamingSink + Clone> GateioConnectorBuilder<S> {
    /// Builds the `GateioConnector` and returns it along with its `ConnectorHandle`.
    pub fn build(self) -> Result<(GateioConnector<S>, ConnectorHandle), MdcError> {
        self.config
            .validate()
            .map_err(|e| MdcError::Config(format!("Invalid Gate.io configuration: {e}")))?;

        let sink = self
            .sink
            .ok_or_else(|| MdcError::Config("Streaming sink is required".into()))?;

        crate::config::validate_symbols(&self.symbols)?;
        crate::config::validate_symbols_per_connection(self.config.symbols_per_connection)?;
        crate::config::validate_resync_buffer_capacity(self.config.resync_buffer_capacity)?;

        let policy_factory = self.policy_factory.unwrap_or_else(|| {
            let timeout = self.config.resync_timeout;
            Arc::new(move || {
                Box::new(mdc_core::resync::CooldownPolicy::new(timeout))
            })
        });

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = GateioConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            run_id: self.run_id,
            policy_factory,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
