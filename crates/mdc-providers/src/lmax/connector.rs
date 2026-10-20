use mdc_core::{
    CapabilityBounds, ConsumerRequirements, DiscoveryCapability, IntegrityCapability,
    MarketDataSink, MdcError, Owned, ProviderCapabilities, QuoteMode, ReplayCapability,
    RestQuoteCapability, RunId, StreamingSink, Symbol, SymbolTag, TimestampCapability,
    TradeCapability, VenueId,
};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::supervisor::{
    COMMAND_BROADCAST_CAPACITY, CONNECTION_COMMAND_CAPACITY, SHUTDOWN_GRACE, spawn_command_fanout,
    spawn_command_forwarder, supervise_connections,
};
use url::Url;

use crate::lmax::config::{ControlPacing, LmaxConfig, LmaxStreams};
use crate::lmax::connection::run_connection_task;
use crate::lmax::control::{Command, ConnectorHandle};

/// Static capability declaration for LMAX Digital.
pub static LMAX_CAPABILITIES: std::sync::LazyLock<ProviderCapabilities> = std::sync::LazyLock::new(
    || {
        ProviderCapabilities {
        quote_modes: vec![QuoteMode::L2Projection],
        trades: TradeCapability::Unverified {
            reason: "LMAX public streams lack stable native trade identity and verified aggressor side semantics".to_string(),
        },
        timestamps: TimestampCapability::ExchangeMilliseconds,
        integrity: IntegrityCapability::HeartbeatWatchdog,
        replay: ReplayCapability::None,
        bounds: CapabilityBounds {
            max_depth: Some(20),
            max_symbols_per_connection: 26,
        },
        discovery: DiscoveryCapability::unsupported(
            "LMAX does not support public REST symbol discovery",
        ),
        rest_quote: RestQuoteCapability::unsupported(
            "LMAX public streams do not support REST quote snapshots",
        ),
    }
    },
);

/// LMAX Digital WebSocket market data connector.
pub struct LmaxConnector<S> {
    config: LmaxConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    run_id: Option<RunId>,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
}

impl<S: StreamingSink + Clone + Send + 'static> LmaxConnector<S> {
    /// Creates a builder initialized with the given `LmaxConfig`.
    pub fn builder(config: LmaxConfig) -> LmaxConnectorBuilder<S> {
        LmaxConnectorBuilder::new(config)
    }

    /// Returns a new cloneable `ConnectorHandle` for controlling this connector.
    pub fn handle(&self) -> ConnectorHandle {
        ConnectorHandle::new(self.command_tx.clone())
    }

    /// Returns the venue identifier for LMAX Digital.
    pub fn venue(&self) -> VenueId {
        VenueId::Lmax
    }

    /// Returns the capability declaration for LMAX Digital.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &LMAX_CAPABILITIES
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

            conn_tasks.spawn(async move {
                run_connection_task(
                    run_id,
                    connection_id,
                    chunk,
                    config,
                    sink,
                    task_cmd_rx,
                    cancel_conn,
                    gen_alloc,
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

/// Fluent builder for constructing [`LmaxConnector`] instances.
pub struct LmaxConnectorBuilder<S> {
    config: LmaxConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
    run_id: Option<RunId>,
}

impl<S> LmaxConnectorBuilder<S> {
    /// Creates a new builder with the provided configuration.
    pub fn new(config: LmaxConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
            run_id: None,
        }
    }

    /// Sets the runtime execution run ID.
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.run_id = Some(run_id);
        self
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

    /// Overrides the WebSocket endpoint URL.
    pub fn ws_url(mut self, ws_url: Url) -> Self {
        self.config.ws_url = ws_url;
        self
    }

    /// Overrides the stream subscriptions.
    pub fn streams(mut self, streams: LmaxStreams) -> Self {
        self.config.streams = streams;
        self
    }

    /// Overrides control pacing settings.
    pub fn control_pacing(mut self, control_pacing: ControlPacing) -> Self {
        self.config.control_pacing = control_pacing;
        self
    }

    /// Supplies a zero-allocation streaming sink.
    pub fn streaming_sink(mut self, sink: S) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Returns the provider capability declaration for LMAX Digital.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &LMAX_CAPABILITIES
    }

    /// Evaluates and enforces consumer requirements against LMAX capabilities.
    pub fn requirements(self, req: &ConsumerRequirements) -> Result<Self, MdcError> {
        LMAX_CAPABILITIES.check_requirements(req)?;
        Ok(self)
    }
}

impl<M: MarketDataSink + Clone> LmaxConnectorBuilder<Owned<M>> {
    /// Supplies an owned market event sink.
    pub fn sink(self, sink: M) -> Self {
        Self {
            config: self.config,
            symbols: self.symbols,
            sink: Some(Owned::new(sink)),
            run_id: self.run_id,
        }
    }
}

impl<S: StreamingSink + Clone + Send + 'static> LmaxConnectorBuilder<S> {
    /// Builds the `LmaxConnector` and returns its control handle.
    pub fn build(self) -> Result<(LmaxConnector<S>, ConnectorHandle), MdcError> {
        self.config
            .validate()
            .map_err(|e| MdcError::Config(e.to_string()))?;

        crate::config::validate_symbols(&self.symbols)?;
        crate::config::validate_symbols_per_connection(self.config.symbols_per_connection)?;

        let sink = self
            .sink
            .ok_or_else(|| MdcError::Config("Streaming sink is required".into()))?;

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = LmaxConnector {
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
