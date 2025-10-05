//! Operator control handle and commands for Kraken WebSocket v2 connector.

use mdc_core::Symbol;
use thiserror::Error;
use tokio::sync::mpsc;

/// Error returned when sending an operator control command fails.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ControlError {
    /// Connector task has terminated or channel is closed.
    #[error("connector command channel closed")]
    Disconnected,
}

/// Commands sent to the connector event loop via `ConnectorHandle`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Requests in-place order book resubscription for a single symbol.
    Resync(Symbol),
    /// Requests in-place order book resubscription for all symbols on the connection.
    ResyncAll,
    /// Triggers an immediate reconnection with the specified reason.
    Reconnect(String),
    /// Pauses network connection activity and signals `ConnectionLost { reason: Paused }`.
    Pause,
    /// Resumes network connection activity from a paused state.
    Resume,
}

/// Thread-safe cloneable handle allowing downstream embedders to control the connector.
#[derive(Debug, Clone)]
pub struct ConnectorHandle {
    tx: mpsc::Sender<Command>,
}

impl ConnectorHandle {
    /// Creates a new `ConnectorHandle` from an `mpsc::Sender`.
    pub fn new(tx: mpsc::Sender<Command>) -> Self {
        Self { tx }
    }

    /// Requests an order book in-place resubscription for the given symbol.
    pub async fn request_resync(&self, symbol: Symbol) -> Result<(), ControlError> {
        self.tx
            .send(Command::Resync(symbol))
            .await
            .map_err(|_| ControlError::Disconnected)
    }

    /// Requests order book in-place resubscription across all active symbols.
    pub async fn request_resync_all(&self) -> Result<(), ControlError> {
        self.tx
            .send(Command::ResyncAll)
            .await
            .map_err(|_| ControlError::Disconnected)
    }

    /// Triggers a connection drop and reconnect with backoff.
    pub async fn reconnect(&self, reason: impl Into<String>) -> Result<(), ControlError> {
        self.tx
            .send(Command::Reconnect(reason.into()))
            .await
            .map_err(|_| ControlError::Disconnected)
    }

    /// Pauses connection activity.
    pub async fn pause(&self) -> Result<(), ControlError> {
        self.tx
            .send(Command::Pause)
            .await
            .map_err(|_| ControlError::Disconnected)
    }

    /// Resumes connection activity from a paused state.
    pub async fn resume(&self) -> Result<(), ControlError> {
        self.tx
            .send(Command::Resume)
            .await
            .map_err(|_| ControlError::Disconnected)
    }
}
