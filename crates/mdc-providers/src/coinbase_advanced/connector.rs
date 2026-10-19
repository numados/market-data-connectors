//! Main Coinbase Advanced Trade WebSocket market data connector and builder.

use mdc_core::{MarketDataSink, MdcError, Owned, RunId, StreamingSink, Symbol, SymbolTag, VenueId};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::coinbase_advanced::config::{CoinbaseAdvancedConfig, CoinbaseAdvancedStreams};
use crate::coinbase_advanced::connection::run_connection_task;
use crate::coinbase_advanced::control::{Command, ConnectorHandle};

/// Coinbase Advanced Trade WebSocket market data connector.
pub struct CoinbaseAdvancedConnector<S> {
    config: CoinbaseAdvancedConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
}

impl CoinbaseAdvancedConnector<()> {
    /// Creates a builder initialized with the given `CoinbaseAdvancedConfig`.
    pub fn builder(config: CoinbaseAdvancedConfig) -> CoinbaseAdvancedConnectorBuilder<()> {
        CoinbaseAdvancedConnectorBuilder::new(config)
    }
}

impl<S: StreamingSink + Clone> CoinbaseAdvancedConnector<S> {
    /// Returns a new cloneable `ConnectorHandle` for controlling this connector.
    pub fn handle(&self) -> ConnectorHandle {
        ConnectorHandle::new(self.command_tx.clone())
    }

    /// Returns the venue identifier for Coinbase Advanced.
    pub fn venue(&self) -> VenueId {
        VenueId::CoinbaseAdvanced
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
        let run_id = self.config.run_id.unwrap_or_else(crate::generate_run_id);
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

/// Fluent builder for constructing a [`CoinbaseAdvancedConnector`].
pub struct CoinbaseAdvancedConnectorBuilder<S = ()> {
    config: CoinbaseAdvancedConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
}

impl CoinbaseAdvancedConnectorBuilder<()> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: CoinbaseAdvancedConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
        }
    }
}

impl<S> CoinbaseAdvancedConnectorBuilder<S> {
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

    /// Configures stream subscriptions.
    pub fn streams(mut self, streams: CoinbaseAdvancedStreams) -> Self {
        self.config.streams = streams;
        self
    }

    /// Enables or disables the Level 2 order book stream.
    pub fn level2(mut self, enabled: bool) -> Self {
        self.config.streams.level2 = enabled;
        self
    }

    /// Enables or disables the heartbeats stream.
    pub fn heartbeats(mut self, enabled: bool) -> Self {
        self.config.streams.heartbeats = enabled;
        self
    }

    /// Enables or disables the public trades stream.
    pub fn market_trades(mut self, enabled: bool) -> Self {
        self.config.streams.market_trades = enabled;
        self
    }

    /// Sets the maximum number of symbols per WebSocket connection.
    pub fn symbols_per_connection(mut self, n: usize) -> Self {
        self.config.symbols_per_connection = n;
        self
    }

    /// Injects a specific `RunId` session token (primarily for tests).
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.config.run_id = Some(run_id);
        self
    }

    /// Configures the streaming sink for the connector.
    pub fn sink<NewS>(self, sink: NewS) -> CoinbaseAdvancedConnectorBuilder<NewS> {
        CoinbaseAdvancedConnectorBuilder {
            config: self.config,
            symbols: self.symbols,
            sink: Some(sink),
        }
    }

    /// Configures the streaming sink for the connector (alias for [`Self::sink`]).
    pub fn streaming_sink<NewS>(self, sink: NewS) -> CoinbaseAdvancedConnectorBuilder<NewS> {
        self.sink(sink)
    }

    /// Configures an owned [`MarketDataSink`] by wrapping it in an [`Owned`] adapter.
    pub fn owned_sink<O: MarketDataSink>(
        self,
        sink: O,
    ) -> CoinbaseAdvancedConnectorBuilder<Owned<O>> {
        self.sink(Owned::new(sink))
    }

    /// Builds the `CoinbaseAdvancedConnector` and its operator `ConnectorHandle`.
    pub fn build(self) -> Result<(CoinbaseAdvancedConnector<S>, ConnectorHandle), MdcError> {
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

        let connector = CoinbaseAdvancedConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
