//! Operator control commands and handle interface for Bitfinex connector.

use mdc_core::model::Symbol;
use tokio::sync::mpsc;

/// Commands sent to the running Bitfinex connection task.
#[derive(Debug)]
pub enum Command {
    /// Forces an immediate socket drop and reconnect.
    Reconnect,
    /// Requests an explicit resynchronization for a specific symbol or all symbols.
    RequestResync(Option<Symbol>),
    /// Pauses market data processing and disconnects from the exchange.
    Pause,
    /// Resumes market data processing and reconnects to the exchange.
    Resume,
}

/// Handle allowing downstream operators to control the Bitfinex connector lifecycle.
#[derive(Debug, Clone)]
pub struct BitfinexHandle {
    sender: mpsc::Sender<Command>,
}

impl BitfinexHandle {
    /// Creates a new `BitfinexHandle` backed by the given channel.
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Requests an immediate reconnection.
    pub async fn reconnect(&self) -> Result<(), mpsc::error::SendError<Command>> {
        self.sender.send(Command::Reconnect).await
    }

    /// Requests order book resynchronization for all subscribed symbols.
    pub async fn resync_all(&self) -> Result<(), mpsc::error::SendError<Command>> {
        self.sender.send(Command::RequestResync(None)).await
    }

    /// Requests order book resynchronization for a specific symbol.
    pub async fn resync_symbol(
        &self,
        symbol: Symbol,
    ) -> Result<(), mpsc::error::SendError<Command>> {
        self.sender.send(Command::RequestResync(Some(symbol))).await
    }

    /// Pauses connector data stream ingestion.
    pub async fn pause(&self) -> Result<(), mpsc::error::SendError<Command>> {
        self.sender.send(Command::Pause).await
    }

    /// Resumes connector data stream ingestion.
    pub async fn resume(&self) -> Result<(), mpsc::error::SendError<Command>> {
        self.sender.send(Command::Resume).await
    }
}
