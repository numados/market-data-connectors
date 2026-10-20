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

use crate::kraken::catalog::DEFAULT_KRAKEN_CATALOG_ENDPOINT;
use crate::kraken::config::KrakenConfig;
use crate::kraken::connection::run_connection_task;
use crate::kraken::control::{Command, ConnectorHandle};

/// Static capability declaration for Kraken Spot.
pub static KRAKEN_CAPABILITIES: std::sync::LazyLock<ProviderCapabilities> =
    std::sync::LazyLock::new(|| ProviderCapabilities {
        quote_modes: vec![QuoteMode::L2Projection],
        trades: TradeCapability::Verified {
            identity_scope: IdentityScope::PerMarket,
            namespace_scheme: "kraken.spot.trade".to_string(),
            side_semantics: SideSemantics::AggressorTaker,
        },
        timestamps: TimestampCapability::ExchangeNanoseconds,
        integrity: IntegrityCapability::ChecksumCrc32,
        replay: ReplayCapability::None,
        bounds: CapabilityBounds {
            max_depth: Some(1000),
            max_symbols_per_connection: 300,
        },
        discovery: DiscoveryCapability::complete(
            ProviderKind::Kraken,
            DEFAULT_KRAKEN_CATALOG_ENDPOINT,
        ),
        rest_quote: mdc_core::rest_quote::RestQuoteCapability::single(
            true,
            TimestampCapability::ReceiptOnly,
            crate::kraken::rest_quote::DEFAULT_KRAKEN_QUOTE_ENDPOINT,
        ),
    });

/// Kraken WebSocket v2 market data connector.
pub struct KrakenConnector<S> {
    config: KrakenConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
}

impl<S: StreamingSink + Clone> KrakenConnector<S> {
    /// Creates a builder initialized with the given `KrakenConfig`.
    pub fn builder(config: KrakenConfig) -> KrakenConnectorBuilder<S> {
        KrakenConnectorBuilder::new(config)
    }

    /// Returns the provider capabilities for Kraken.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &KRAKEN_CAPABILITIES
    }

    /// Returns a reference to the active configuration.
    pub fn config(&self) -> &KrakenConfig {
        &self.config
    }

    /// Returns a new cloneable `ConnectorHandle` for controlling this connector.
    pub fn handle(&self) -> ConnectorHandle {
        ConnectorHandle::new(self.command_tx.clone())
    }

    /// Returns the venue identifier for Kraken.
    pub fn venue(&self) -> VenueId {
        VenueId::Kraken
    }

    /// Runs the connector until cancelled.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), MdcError> {
        let run_id = self.config.run_id.unwrap_or_else(crate::generate_run_id);
        let generation_allocator = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut conn_tasks = JoinSet::new();
        let mut aux_tasks = JoinSet::new();
        let chunks: Vec<Vec<(Symbol, SymbolTag)>> = self
            .symbols
            .chunks(self.config.symbols_per_connection)
            .map(<[(Symbol, SymbolTag)]>::to_vec)
            .collect();
        let total_connections = chunks.len();

        // Forward commands to connection tasks if multiple buckets exist
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
                    connection_id,
                    chunk,
                    config,
                    sink,
                    task_cmd_rx,
                    cancel_conn,
                    run_id,
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

/// Fluent builder for constructing a `KrakenConnector`.
pub struct KrakenConnectorBuilder<S> {
    config: KrakenConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
}

impl<S> KrakenConnectorBuilder<S> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: KrakenConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
        }
    }

    /// Injects a specific `RunId` session token (primarily for tests).
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.config.run_id = Some(run_id);
        self
    }

    /// Returns the provider capabilities for Kraken.
    pub fn capabilities(&self) -> &ProviderCapabilities {
        &KRAKEN_CAPABILITIES
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

    /// Configures the streaming sink for this connector.
    pub fn streaming_sink(mut self, sink: S) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Configures a pluggable checksum calculation strategy.
    pub fn checksum_strategy(
        mut self,
        strategy: std::sync::Arc<dyn mdc_core::integrity::ChecksumStrategy>,
    ) -> Self {
        self.config.checksum_strategy = Some(strategy);
        self
    }
}

impl<M: MarketDataSink + Clone> KrakenConnectorBuilder<Owned<M>> {
    /// Configures an owned `MarketDataSink` by automatically wrapping it in an `Owned` adapter.
    pub fn sink(self, sink: M) -> Self {
        Self {
            config: self.config,
            symbols: self.symbols,
            sink: Some(Owned::new(sink)),
        }
    }
}

impl<S: StreamingSink + Clone> KrakenConnectorBuilder<S> {
    /// Builds the `KrakenConnector` and returns it along with its `ConnectorHandle`.
    pub fn build(self) -> Result<(KrakenConnector<S>, ConnectorHandle), MdcError> {
        let sink = self
            .sink
            .ok_or_else(|| MdcError::Config("Streaming sink is required".into()))?;

        crate::config::validate_symbols(&self.symbols)?;
        crate::config::validate_symbols_per_connection(self.config.symbols_per_connection)?;
        crate::config::validate_resubscribe_batch_size(self.config.resubscribe_batch_size)?;
        crate::config::validate_depth_in_set(
            self.config.depth,
            &[10, 25, 100, 500, 1000],
            "Kraken",
        )?;

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = KrakenConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
