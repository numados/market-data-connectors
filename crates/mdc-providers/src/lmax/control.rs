//! Control commands and operator handle for the LMAX Digital connector.

use mdc_core::Symbol;
use tokio::sync::mpsc;

/// Operator command sent to the LMAX connection task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Requests resubscription (snapshot refresh) for a specific symbol.
    ResyncSymbol(Symbol),
    /// Forces an immediate WebSocket reconnect.
    Reconnect,
    /// Pauses message processing.
    Pause,
    /// Resumes message processing.
    Resume,
}

/// Errors occurring during connector control operations.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// Connection task channel closed.
    #[error("connection task channel closed")]
    ChannelClosed,
}

/// Operator handle for controlling an active LMAX Digital connector.
#[derive(Debug, Clone)]
pub struct ConnectorHandle {
    sender: mpsc::Sender<Command>,
}

impl ConnectorHandle {
    /// Creates a new `ConnectorHandle`.
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Requests a resubscription refresh for `symbol`.
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
