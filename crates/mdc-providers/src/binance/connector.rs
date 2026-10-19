//! Main Binance Spot connector and fluent builder.

use std::sync::{Arc, Mutex};

use mdc_core::{
    CapabilityBounds, ConsumerRequirements, DiscoveryCapability, EmbargoTracker, IdentityScope,
    IntegrityCapability, MarketDataSink, MdcError, Owned, ProviderCapabilities, QuoteMode,
    ReplayCapability, RunId, SideSemantics, StreamingSink, Symbol, SymbolTag, TimestampCapability,
    TradeCapability, VenueId,
};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::binance::config::BinanceConfig;
use crate::binance::connection::run_connection_task;
use crate::binance::control::{Command, ConnectorHandle};
use crate::binance::rest::BinanceRestClient;

/// Static capability declaration for Binance Spot.
pub static BINANCE_CAPABILITIES: std::sync::LazyLock<ProviderCapabilities> =
    std::sync::LazyLock::new(|| ProviderCapabilities {
        quote_modes: vec![QuoteMode::NativeTicker, QuoteMode::L2Projection],
        trades: TradeCapability::Verified {
            identity_scope: IdentityScope::PerMarket,
            namespace_scheme: "binance.spot.trade".to_string(),
            side_semantics: SideSemantics::AggressorTaker,
        },
        timestamps: TimestampCapability::ExchangeMilliseconds,
        integrity: IntegrityCapability::SequenceNumbers,
        replay: ReplayCapability::None,
        bounds: CapabilityBounds {
            max_depth: Some(5000),
            max_symbols_per_connection: 25,
        },
        discovery: DiscoveryCapability::CompleteCatalog {
            scopes: vec![mdc_core::catalog::CatalogScope::spot(
                mdc_core::catalog::ProviderKind::Binance,
            )],
            pagination: false,
            default_endpoint: crate::binance::catalog::DEFAULT_BINANCE_CATALOG_ENDPOINT.to_string(),
        },
        rest_quote: mdc_core::rest_quote::RestQuoteCapability::single(
            true,
            TimestampCapability::ReceiptOnly,
            crate::binance::rest_quote::DEFAULT_BINANCE_QUOTE_ENDPOINT,
        ),
    });

/// Factory alias for creating fresh resynchronization policies per connection task.
pub type ResyncPolicyFactory =
    Arc<dyn Fn() -> Box<dyn mdc_core::resync::ResyncPolicy> + Send + Sync>;

/// Binance Spot market data connector.
pub struct BinanceConnector<S> {
    config: BinanceConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    rest_client: BinanceRestClient,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    run_id: Option<RunId>,
    resync_policy_factory: ResyncPolicyFactory,
}

impl<S: StreamingSink + Clone> BinanceConnector<S> {
    /// Creates a builder initialized with the given `BinanceConfig`.
    pub fn builder(config: BinanceConfig) -> BinanceConnectorBuilder<S> {
        BinanceConnectorBuilder::new(config)
    }

    /// Returns a new cloneable `ConnectorHandle` for controlling this connector.
    pub fn handle(&self) -> ConnectorHandle {
        ConnectorHandle::new(self.command_tx.clone())
    }

    /// Returns the venue identifier for Binance.
    pub fn venue(&self) -> VenueId {
        VenueId::Binance
    }

    /// Returns the capability declaration for Binance Spot.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &BINANCE_CAPABILITIES
    }

    /// Returns a reference to the active configuration.
    pub fn config(&self) -> &BinanceConfig {
        &self.config
    }

    /// Returns a reference to the configured resync policy factory.
    pub fn resync_policy_factory(&self) -> &ResyncPolicyFactory {
        &self.resync_policy_factory
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

        // Forward commands to connection tasks if multiple buckets exist
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
            let rest_client = self.rest_client.clone();
            let cancel_conn = cancel.clone();
            let policy_factory = self.resync_policy_factory.clone();
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

            conn_tasks.spawn(async move {
                run_connection_task(
                    connection_id,
                    chunk,
                    config,
                    sink,
                    rest_client,
                    task_cmd_rx,
                    cancel_conn,
                    run_id,
                    policy_factory,
                    gen_alloc,
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
                                    format!("Connection task panicked: {e}"),
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
                                    format!("Connection task panicked: {e}"),
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

/// Fluent builder for constructing a `BinanceConnector`.
pub struct BinanceConnectorBuilder<S> {
    config: BinanceConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
    embargo: Option<Arc<Mutex<EmbargoTracker>>>,
    run_id: Option<RunId>,
    resync_policy_factory: Option<ResyncPolicyFactory>,
}

impl<S> BinanceConnectorBuilder<S> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: BinanceConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
            embargo: None,
            run_id: None,
            resync_policy_factory: None,
        }
    }

    /// Configures a pluggable resync policy (cloned per connection task).
    pub fn resync_policy<P: mdc_core::resync::ResyncPolicy + Clone + Sync + 'static>(
        mut self,
        policy: P,
    ) -> Self {
        self.resync_policy_factory = Some(Arc::new(move || Box::new(policy.clone())));
        self
    }

    /// Configures a factory for pluggable resync policies.
    pub fn resync_policy_factory(mut self, factory: ResyncPolicyFactory) -> Self {
        self.resync_policy_factory = Some(factory);
        self
    }

    /// Injects a specific `RunId` session token (primarily for tests).
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.run_id = Some(run_id);
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

    /// Injects an existing shared embargo tracker.
    pub fn embargo(mut self, embargo: Arc<Mutex<EmbargoTracker>>) -> Self {
        self.embargo = Some(embargo);
        self
    }

    /// Configures the streaming sink for this connector.
    pub fn streaming_sink(mut self, sink: S) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Returns the provider capability declaration for Binance Spot.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &BINANCE_CAPABILITIES
    }

    /// Evaluates and enforces consumer requirements against Binance capabilities.
    pub fn requirements(self, req: &ConsumerRequirements) -> Result<Self, MdcError> {
        BINANCE_CAPABILITIES.check_requirements(req)?;
        Ok(self)
    }
}

impl<M: MarketDataSink + Clone> BinanceConnectorBuilder<Owned<M>> {
    /// Configures an owned `MarketDataSink` by automatically wrapping it in an `Owned` adapter.
    pub fn sink(self, sink: M) -> Self {
        Self {
            config: self.config,
            symbols: self.symbols,
            sink: Some(Owned::new(sink)),
            embargo: self.embargo,
            run_id: self.run_id,
            resync_policy_factory: self.resync_policy_factory,
        }
    }
}

impl<S: StreamingSink + Clone> BinanceConnectorBuilder<S> {
    /// Builds the `BinanceConnector` and returns it along with its `ConnectorHandle`.
    pub fn build(self) -> Result<(BinanceConnector<S>, ConnectorHandle), MdcError> {
        let sink = self
            .sink
            .ok_or_else(|| MdcError::Config("Streaming sink is required".into()))?;

        crate::config::validate_symbols(&self.symbols)?;
        crate::config::validate_symbols_per_connection(self.config.symbols_per_connection)?;
        crate::config::validate_resync_buffer_capacity(self.config.resync_buffer_capacity)?;

        let embargo = self
            .embargo
            .unwrap_or_else(|| Arc::new(Mutex::new(EmbargoTracker::new())));

        let rest_client = BinanceRestClient::new(
            self.config.rest_base_url.clone(),
            embargo,
            self.config.rest_timeout,
        )
        .map_err(|e| MdcError::Transport(Box::new(std::io::Error::other(e.to_string()))))?;

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let resync_policy_factory = self.resync_policy_factory.unwrap_or_else(|| {
            let cd = self.config.snapshot_retry_cooldown;
            Arc::new(move || Box::new(mdc_core::resync::CooldownPolicy::new(cd)))
        });

        let connector = BinanceConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            rest_client,
            command_tx,
            command_rx,
            run_id: self.run_id,
            resync_policy_factory,
        };

        Ok((connector, handle))
    }
}
