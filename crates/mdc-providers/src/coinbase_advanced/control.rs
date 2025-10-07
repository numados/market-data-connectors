//! Control commands and operator handle for the Coinbase Advanced Trade connector.

use mdc_core::Symbol;
use tokio::sync::mpsc;

/// Operator command sent to the Coinbase Advanced connection loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Forces in-place snapshot resynchronization of a specific product.
    ResyncProduct(Symbol),
    /// Forces connection-wide resynchronization across all products.
    ResyncAll,
    /// Forces a WebSocket reconnect.
    Reconnect,
    /// Pauses message processing.
    Pause,
    /// Resumes message processing.
    Resume,
}

/// Errors occurring during connector control operations.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// Connector task has terminated or receiver dropped.
    #[error("connection task channel closed")]
    ChannelClosed,
}

/// Operator handle for interacting with an active Coinbase Advanced Trade connector.
#[derive(Debug, Clone)]
pub struct ConnectorHandle {
    sender: mpsc::Sender<Command>,
}

impl ConnectorHandle {
    /// Creates a new `ConnectorHandle`.
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Requests an immediate snapshot resynchronization for `symbol`.
    pub async fn request_resync(&self, symbol: Symbol) -> Result<(), ControlError> {
        self.sender
            .send(Command::ResyncProduct(symbol))
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Requests an immediate connection-wide resynchronization across all products.
    pub async fn request_resync_all(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::ResyncAll)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Requests an immediate WebSocket reconnect.
    pub async fn reconnect(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Reconnect)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Pauses message processing.
    pub async fn pause(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Pause)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Resumes message processing.
    pub async fn resume(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Resume)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }
}
