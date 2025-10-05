//! Main Coinbase Exchange WebSocket connector and fluent builder.

use mdc_core::{MarketDataSink, MdcError, Owned, StreamingSink, Symbol, SymbolTag, VenueId};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::coinbase::config::CoinbaseConfig;
use crate::coinbase::connection::run_connection_task;
use crate::coinbase::control::{Command, ConnectorHandle};

/// Coinbase Exchange WebSocket market data connector.
pub struct CoinbaseConnector<S> {
    config: CoinbaseConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: S,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
}

impl<S: StreamingSink + Clone> CoinbaseConnector<S> {
    /// Creates a builder initialized with the given `CoinbaseConfig`.
    pub fn builder(config: CoinbaseConfig) -> CoinbaseConnectorBuilder<S> {
        CoinbaseConnectorBuilder::new(config)
    }

    /// Returns a new cloneable `ConnectorHandle` for controlling this connector.
    pub fn handle(&self) -> ConnectorHandle {
        ConnectorHandle::new(self.command_tx.clone())
    }

    /// Returns the venue identifier for Coinbase.
    pub fn venue(&self) -> VenueId {
        VenueId::Coinbase
    }

    /// Runs the connector until cancelled.
    pub async fn run(self, cancel: CancellationToken) -> Result<(), MdcError> {
        let mut join_set = JoinSet::new();
        let chunks: Vec<Vec<(Symbol, SymbolTag)>> = self
            .symbols
            .chunks(self.config.products_per_connection)
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
                        format!("Connection task panicked: {e}"),
                    ))));
                }
            }
        }

        Ok(())
    }
}

/// Fluent builder for constructing a `CoinbaseConnector`.
pub struct CoinbaseConnectorBuilder<S> {
    config: CoinbaseConfig,
    symbols: Vec<(Symbol, SymbolTag)>,
    sink: Option<S>,
}

impl<S> CoinbaseConnectorBuilder<S> {
    /// Creates a new builder with the given configuration.
    pub fn new(config: CoinbaseConfig) -> Self {
        Self {
            config,
            symbols: Vec::new(),
            sink: None,
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

    /// Configures the streaming sink for this connector.
    pub fn streaming_sink(mut self, sink: S) -> Self {
        self.sink = Some(sink);
        self
    }
}

impl<M: MarketDataSink + Clone> CoinbaseConnectorBuilder<Owned<M>> {
    /// Configures an owned `MarketDataSink` by automatically wrapping it in an `Owned` adapter.
    pub fn sink(self, sink: M) -> Self {
        Self {
            config: self.config,
            symbols: self.symbols,
            sink: Some(Owned::new(sink)),
        }
    }
}

impl<S: StreamingSink + Clone> CoinbaseConnectorBuilder<S> {
    /// Builds the `CoinbaseConnector` and returns it along with its `ConnectorHandle`.
    pub fn build(self) -> Result<(CoinbaseConnector<S>, ConnectorHandle), MdcError> {
        let sink = self
            .sink
            .ok_or_else(|| MdcError::Protocol("Streaming sink is required".into()))?;

        if self.symbols.is_empty() {
            return Err(MdcError::Protocol(
                "At least one symbol must be subscribed".into(),
            ));
        }

        let (command_tx, command_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(command_tx.clone());

        let connector = CoinbaseConnector {
            config: self.config,
            symbols: self.symbols,
            sink,
            command_tx,
            command_rx,
        };

        Ok((connector, handle))
    }
}
