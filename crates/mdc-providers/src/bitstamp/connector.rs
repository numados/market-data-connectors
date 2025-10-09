use std::sync::{Arc, Mutex};

use mdc_core::catalog::{DiscoveryCapability, ProviderKind};
use mdc_core::integrity::EmbargoTracker;
use mdc_core::model::capabilities::{
    CapabilityBounds, IdentityScope, IntegrityCapability, ProviderCapabilities, QuoteMode,
    ReplayCapability, SideSemantics, TimestampCapability, TradeCapability,
};
use mdc_core::{MarketDataSink, MdcError, Owned, StreamingSink, Symbol, SymbolTag, VenueId};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::bitstamp::catalog::DEFAULT_BITSTAMP_CATALOG_ENDPOINT;
use crate::bitstamp::config::{BitstampConfig, BitstampStreams, ConfigError};
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

/// Bitstamp WebSocket v2 and REST market data connector.
pub struct BitstampConnector<S> {
    config: BitstampConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    embargo: Arc<Mutex<EmbargoTracker>>,
    sink: S,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
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

        let rest_client = BitstampRestClient::new(
            self.config.rest_url.clone(),
            self.embargo.clone(),
            self.config.request_timeout,
        )
        .map_err(|e| MdcError::Transport(Box::new(std::io::Error::other(e.to_string()))))?;

        for (i, chunk) in chunks.into_iter().enumerate() {
            let connection_id = (i + 1) as u32;
            let sink = self.sink.clone();
            let config = self.config.clone();
            let cancel_conn = cancel.clone();
            let rest = rest_client.clone();

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
                    rest,
                    sink,
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
                        format!("Connection task panic: {e}"),
                    ))));
                }
            }
        }

        Ok(())
    }
}

/// Fluent builder for constructing [`BitstampConnector`] instances.
pub struct BitstampConnectorBuilder<S> {
    config: BitstampConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    embargo: Option<Arc<Mutex<EmbargoTracker>>>,
    sink: Option<S>,
}

impl<S> BitstampConnectorBuilder<S> {
    /// Creates a new builder with the provided configuration.
    pub fn new(config: BitstampConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            embargo: None,
            sink: None,
        }
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
        }
    }
}

impl<S: StreamingSink + Clone + Send + 'static> BitstampConnectorBuilder<S> {
    /// Builds the `BitstampConnector` and returns its control handle.
    pub fn build(self) -> Result<(BitstampConnector<S>, ConnectorHandle), ConfigError> {
        self.config.validate()?;

        if self.symbols.is_empty() {
            return Err(ConfigError::EmptySymbols);
        }

        let sink = self.sink.ok_or(ConfigError::MissingSink)?;

        let embargo = self
            .embargo
            .unwrap_or_else(|| Arc::new(Mutex::new(EmbargoTracker::new())));

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = BitstampConnector {
            config: self.config,
            symbols: self.symbols,
            embargo,
            sink,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
