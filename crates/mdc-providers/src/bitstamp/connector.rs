use std::sync::{Arc, Mutex};

use mdc_core::catalog::{DiscoveryCapability, ProviderKind};
use mdc_core::integrity::EmbargoTracker;
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

use crate::bitstamp::catalog::DEFAULT_BITSTAMP_CATALOG_ENDPOINT;
use crate::bitstamp::config::{BitstampConfig, BitstampStreams};
use crate::bitstamp::connection::run_connection_task;
use crate::bitstamp::control::{Command, ConnectorHandle};
use crate::bitstamp::rest::BitstampRestClient;

/// Static capability declaration for Bitstamp Spot.
pub static BITSTAMP_CAPABILITIES: std::sync::LazyLock<ProviderCapabilities> =
    std::sync::LazyLock::new(|| ProviderCapabilities {
        quote_modes: vec![QuoteMode::L2Projection],
        trades: TradeCapability::Verified {
            identity_scope: IdentityScope::PerMarket,
            namespace_scheme: "bitstamp.spot.trade".to_string(),
            side_semantics: SideSemantics::AggressorTaker,
        },
        timestamps: TimestampCapability::ExchangeNanoseconds,
        integrity: IntegrityCapability::TimeGated,
        replay: ReplayCapability::None,
        bounds: CapabilityBounds {
            max_depth: Some(100),
            max_symbols_per_connection: 25,
        },
        discovery: DiscoveryCapability::complete(
            ProviderKind::Bitstamp,
            DEFAULT_BITSTAMP_CATALOG_ENDPOINT,
        ),
        rest_quote: mdc_core::rest_quote::RestQuoteCapability::single(
            false,
            TimestampCapability::ExchangeMilliseconds,
            crate::bitstamp::rest_quote::DEFAULT_BITSTAMP_QUOTE_ENDPOINT,
        ),
    });

/// Thread-safe factory producing per-symbol resynchronization policies.
pub type ResyncPolicyFactory =
    Arc<dyn Fn() -> Box<dyn mdc_core::resync::ResyncPolicy> + Send + Sync>;

/// Bitstamp WebSocket v2 and REST market data connector.
pub struct BitstampConnector<S> {
    config: BitstampConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    embargo: Arc<Mutex<EmbargoTracker>>,
    sink: S,
    run_id: Option<RunId>,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    resync_policy_factory: ResyncPolicyFactory,
}

impl<S: StreamingSink + Clone + Send + 'static> BitstampConnector<S> {
    /// Creates a builder initialized with the given `BitstampConfig`.
    pub fn builder(config: BitstampConfig) -> BitstampConnectorBuilder<S> {
        BitstampConnectorBuilder::new(config)
    }

    /// Returns the provider capabilities for Bitstamp.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &BITSTAMP_CAPABILITIES
    }

    /// Returns a new cloneable `ConnectorHandle` for controlling this connector.
    pub fn handle(&self) -> ConnectorHandle {
        ConnectorHandle::new(self.command_tx.clone())
    }

    /// Returns the venue identifier for Bitstamp.
    pub fn venue(&self) -> VenueId {
        VenueId::Bitstamp
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
        let mut conn_tasks = JoinSet::new();
        let mut aux_tasks = JoinSet::new();
        let chunks: Vec<Vec<(Symbol, SymbolTag)>> = self
            .symbols
            .chunks(self.config.symbols_per_connection)
            .map(<[(Symbol, SymbolTag)]>::to_vec)
            .collect();

        let (broadcast_tx, _) =
            tokio::sync::broadcast::channel::<Command>(COMMAND_BROADCAST_CAPACITY);
        spawn_command_fanout(
            &mut aux_tasks,
            self.command_rx,
            broadcast_tx.clone(),
            cancel.clone(),
        );

        let rest_client = BitstampRestClient::new(
            self.config.rest_url.clone(),
            self.embargo.clone(),
            self.config.request_timeout,
        )
        .map_err(|e| MdcError::Transport(Box::new(std::io::Error::other(e.to_string()))))?;

        let run_id = self.run_id.unwrap_or_else(crate::generate_run_id);
        let generation_allocator = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let total_connections = chunks.len();

        for (i, chunk) in chunks.into_iter().enumerate() {
            let connection_id = (i + 1) as u32;
            let sink = self.sink.clone();
            let config = self.config.clone();
            let cancel_conn = cancel.clone();
            let rest = rest_client.clone();
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

            let resync_pf = self.resync_policy_factory.clone();
            conn_tasks.spawn(async move {
                run_connection_task(
                    run_id,
                    connection_id,
                    chunk,
                    config,
                    rest,
                    sink,
                    task_cmd_rx,
                    cancel_conn,
                    gen_alloc,
                    resync_pf,
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

/// Fluent builder for constructing [`BitstampConnector`] instances.
pub struct BitstampConnectorBuilder<S> {
    config: BitstampConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    embargo: Option<Arc<Mutex<EmbargoTracker>>>,
    sink: Option<S>,
    run_id: Option<RunId>,
    resync_policy_factory: Option<ResyncPolicyFactory>,
}

impl<S> BitstampConnectorBuilder<S> {
    /// Creates a new builder with the provided configuration.
    pub fn new(config: BitstampConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            embargo: None,
            sink: None,
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

    /// Sets the maximum number of deltas to buffer per symbol while awaiting a snapshot.
    pub fn buffer_capacity(mut self, capacity: usize) -> Self {
        self.config.buffer_capacity = capacity;
        self
    }

    /// Sets the runtime execution run ID.
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Returns the provider capabilities for Bitstamp.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &BITSTAMP_CAPABILITIES
    }

    /// Sets the list of symbols to ingest.
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

    /// Overrides the shared rate-limit embargo tracker.
    pub fn embargo(mut self, embargo: Arc<Mutex<EmbargoTracker>>) -> Self {
        self.embargo = Some(embargo);
        self
    }

    /// Overrides the WebSocket endpoint URL.
    pub fn ws_url(mut self, ws_url: Url) -> Self {
        self.config.ws_url = ws_url;
        self
    }

    /// Overrides the REST API base URL.
    pub fn rest_url(mut self, rest_url: Url) -> Self {
        self.config.rest_url = rest_url;
        self
    }

    /// Overrides the stream subscriptions.
    pub fn streams(mut self, streams: BitstampStreams) -> Self {
        self.config.streams = streams;
        self
    }

    /// Supplies a zero-allocation streaming sink.
    pub fn streaming_sink(mut self, sink: S) -> Self {
        self.sink = Some(sink);
        self
    }
}

impl<M: MarketDataSink + Clone> BitstampConnectorBuilder<Owned<M>> {
    /// Supplies an owned market event sink.
    pub fn sink(self, sink: M) -> Self {
        Self {
            config: self.config,
            symbols: self.symbols,
            embargo: self.embargo,
            sink: Some(Owned::new(sink)),
            run_id: self.run_id,
            resync_policy_factory: self.resync_policy_factory,
        }
    }
}

impl<S: StreamingSink + Clone + Send + 'static> BitstampConnectorBuilder<S> {
    /// Builds the `BitstampConnector` and returns its control handle.
    pub fn build(self) -> Result<(BitstampConnector<S>, ConnectorHandle), MdcError> {
        self.config
            .validate()
            .map_err(|e| MdcError::Config(e.to_string()))?;

        crate::config::validate_symbols(&self.symbols)?;
        crate::config::validate_symbols_per_connection(self.config.symbols_per_connection)?;
        crate::config::validate_resync_buffer_capacity(self.config.buffer_capacity)?;

        let sink = self
            .sink
            .ok_or_else(|| MdcError::Config("Streaming sink is required".into()))?;

        let embargo = self
            .embargo
            .unwrap_or_else(|| Arc::new(Mutex::new(EmbargoTracker::new())));

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let default_cooldown = self.config.snapshot_retry_cooldown;
        let resync_policy_factory = self.resync_policy_factory.unwrap_or_else(|| {
            Arc::new(move || Box::new(mdc_core::resync::CooldownPolicy::new(default_cooldown)))
        });

        let connector = BitstampConnector {
            config: self.config,
            symbols: self.symbols,
            embargo,
            sink,
            run_id: self.run_id,
            command_tx,
            command_rx,
            resync_policy_factory,
        };

        Ok((connector, handle))
    }
}
