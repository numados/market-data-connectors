use mdc_core::catalog::{DiscoveryCapability, ProviderKind};
use mdc_core::model::capabilities::{
    CapabilityBounds, IdentityScope, IntegrityCapability, ProviderCapabilities, QuoteMode,
    ReplayCapability, SideSemantics, TimestampCapability, TradeCapability,
};
use mdc_core::{MarketDataSink, MdcError, Owned, RunId, StreamingSink, Symbol, SymbolTag, VenueId};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::supervisor::{
    COMMAND_BROADCAST_CAPACITY, CONNECTION_COMMAND_CAPACITY, SHUTDOWN_GRACE, spawn_command_fanout,
    spawn_command_forwarder, supervise_connections,
};
use url::Url;

use std::sync::Arc;

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

/// Thread-safe factory producing per-symbol resynchronization policies.
pub type ResyncPolicyFactory =
    Arc<dyn Fn() -> Box<dyn mdc_core::resync::ResyncPolicy> + Send + Sync>;

/// Huobi (HTX) Market-By-Price WebSocket market data connector.
pub struct HuobiConnector<S> {
    config: HuobiConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    run_id: Option<RunId>,
    policy_factory: ResyncPolicyFactory,
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
        let run_id = self.run_id.unwrap_or_else(crate::generate_run_id);
        let generation_allocator = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut conn_tasks = JoinSet::new();
        let mut aux_tasks = JoinSet::new();
        let chunks: Vec<Vec<(Symbol, SymbolTag)>> = self
            .symbols
            .chunks(self.config.symbols_per_connection)
            .map(<[(Symbol, SymbolTag)]>::to_vec)
            .collect();
        let total_connections = chunks.len();

        let (broadcast_tx, _) =
            tokio::sync::broadcast::channel::<Command>(COMMAND_BROADCAST_CAPACITY);
        spawn_command_fanout(
            &mut aux_tasks,
            self.command_rx,
            broadcast_tx.clone(),
            cancel.clone(),
        );

        for (i, chunk) in chunks.into_iter().enumerate() {
            let connection_id = (i + 1) as u32;
            let sink = self.sink.clone();
            let config = self.config.clone();
            let cancel_conn = cancel.clone();
            let gen_alloc = generation_allocator.clone();

            let b_rx = broadcast_tx.subscribe();
            let (task_cmd_tx, task_cmd_rx) = mpsc::channel(CONNECTION_COMMAND_CAPACITY);

            spawn_command_forwarder(
                &mut aux_tasks,
                connection_id,
                b_rx,
                task_cmd_tx,
                cancel.clone(),
            );

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

        supervise_connections(
            conn_tasks,
            aux_tasks,
            cancel,
            total_connections,
            SHUTDOWN_GRACE,
        )
        .await
    }
}

/// Fluent builder for constructing a [`HuobiConnector`].
pub struct HuobiConnectorBuilder<S = ()> {
    config: HuobiConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
    run_id: Option<RunId>,
    policy_factory: Option<ResyncPolicyFactory>,
}

impl HuobiConnectorBuilder<()> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: HuobiConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
            run_id: None,
            policy_factory: None,
        }
    }
}

impl<S> HuobiConnectorBuilder<S> {
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
            policy_factory: self.policy_factory,
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

        let policy_factory = self.policy_factory.unwrap_or_else(|| {
            let timeout = self.config.resync_timeout;
            Arc::new(move || Box::new(mdc_core::resync::CooldownPolicy::new(timeout)))
        });

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = HuobiConnector {
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
