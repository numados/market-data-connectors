//! Main Binance Spot connector and fluent builder.

use std::sync::{Arc, Mutex};

use mdc_core::{
    CapabilityBounds, ConsumerRequirements, DiscoveryCapability, EmbargoTracker, IdentityScope,
    IntegrityCapability, MarketDataSink, MdcError, Owned, ProviderCapabilities, QuoteMode,
    ReplayCapability, SideSemantics, StreamingSink, Symbol, SymbolTag, TimestampCapability,
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

/// Binance Spot market data connector.
pub struct BinanceConnector<S> {
    config: BinanceConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    rest_client: BinanceRestClient,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
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

    /// Runs the connector until cancelled.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), MdcError> {
        let mut join_set = JoinSet::new();
        let chunks: Vec<Vec<(Symbol, SymbolTag)>> = self
            .symbols
            .chunks(self.config.symbols_per_connection)
            .map(<[(Symbol, SymbolTag)]>::to_vec)
            .collect();

        // Forward commands to connection tasks if multiple buckets exist
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
            let rest_client = self.rest_client.clone();
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
                run_connection_task(
                    connection_id,
                    chunk,
                    config,
                    sink,
                    rest_client,
                    task_cmd_rx,
                    cancel_conn,
                )
                .await;
            });
        }

        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                if !e.is_cancelled() {
                    return Err(MdcError::Transport(Box::new(std::io::Error::other(
                        format!("Connection task panicked: {e}"),
                    ))));
                }
            }
        }

        Ok(())
    }
}

/// Fluent builder for constructing a `BinanceConnector`.
pub struct BinanceConnectorBuilder<S> {
    config: BinanceConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
    embargo: Option<Arc<Mutex<EmbargoTracker>>>,
}

impl<S> BinanceConnectorBuilder<S> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: BinanceConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
            embargo: None,
        }
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
        }
    }
}

impl<S: StreamingSink + Clone> BinanceConnectorBuilder<S> {
    /// Builds the `BinanceConnector` and returns it along with its `ConnectorHandle`.
    pub fn build(self) -> Result<(BinanceConnector<S>, ConnectorHandle), MdcError> {
        let sink = self
            .sink
            .ok_or_else(|| MdcError::Protocol("Streaming sink is required".into()))?;

        if self.symbols.is_empty() {
            return Err(MdcError::Protocol(
                "At least one symbol must be subscribed".into(),
            ));
        }

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

        let connector = BinanceConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            rest_client,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
