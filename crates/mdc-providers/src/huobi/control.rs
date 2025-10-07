//! Control commands and operator handle for the Huobi connector.

use mdc_core::Symbol;
use tokio::sync::mpsc;

/// Operator command sent to the Huobi connection loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Forces in-place snapshot resynchronization of a specific symbol.
    ResyncSymbol(Symbol),
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

/// Operator handle for interacting with an active Huobi connector.
#[derive(Debug, Clone)]
pub struct ConnectorHandle {
    sender: mpsc::Sender<Command>,
}

impl ConnectorHandle {
    /// Creates a new `ConnectorHandle`.
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Requests an immediate resync for `symbol`.
    pub async fn request_resync(&self, symbol: Symbol) -> Result<(), ControlError> {
        self.sender
            .send(Command::ResyncSymbol(symbol))
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

    /// Pauses processing.
    pub async fn pause(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Pause)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Resumes processing.
    pub async fn resume(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Resume)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }
}
