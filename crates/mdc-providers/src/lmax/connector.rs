use mdc_core::{
    CapabilityBounds, ConsumerRequirements, IntegrityCapability, MarketDataSink, MdcError, Owned,
    ProviderCapabilities, QuoteMode, ReplayCapability, StreamingSink, Symbol, SymbolTag,
    TimestampCapability, TradeCapability, VenueId,
};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::lmax::config::{ConfigError, ControlPacing, LmaxConfig, LmaxStreams};
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
        discovery: false,
        rest_quote: false,
    }
    },
);

/// LMAX Digital WebSocket market data connector.
pub struct LmaxConnector<S> {
    config: LmaxConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
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
                        format!("Connection task panic: {e}"),
                    ))));
                }
            }
        }

        Ok(())
    }
}

/// Fluent builder for constructing [`LmaxConnector`] instances.
pub struct LmaxConnectorBuilder<S> {
    config: LmaxConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
}

impl<S> LmaxConnectorBuilder<S> {
    /// Creates a new builder with the provided configuration.
    pub fn new(config: LmaxConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
        }
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
        }
    }
}

impl<S: StreamingSink + Clone + Send + 'static> LmaxConnectorBuilder<S> {
    /// Builds the `LmaxConnector` and returns its control handle.
    pub fn build(self) -> Result<(LmaxConnector<S>, ConnectorHandle), ConfigError> {
        self.config.validate()?;

        if self.symbols.is_empty() {
            return Err(ConfigError::EmptySymbols);
        }

        let sink = self.sink.ok_or(ConfigError::MissingSink)?;

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = LmaxConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
