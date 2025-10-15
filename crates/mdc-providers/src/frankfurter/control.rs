//! Control commands and operator handle for the Frankfurter ECB polling connector.

use thiserror::Error;
use tokio::sync::mpsc;

/// Operator command sent to the active Frankfurter polling loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Triggers an immediate poll cycle.
    PollNow,
    /// Pauses the polling loop (cancelling any in-flight request, emitting ConnectionLost).
    Pause,
    /// Resumes the polling loop with a new delivery generation and ResyncStarted.
    Resume,
    /// Updates configured quote currency codes.
    SetSymbols(Vec<String>),
}

/// Errors occurring during connector control operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlError {
    /// Connection task command channel is closed.
    #[error("connection task command channel closed")]
    ChannelClosed,

    /// Proposed symbol list is invalid.
    #[error("invalid symbols configuration: {0}")]
    InvalidSymbols(String),
}

/// Operator handle for controlling an active Frankfurter reference rate connector.
#[derive(Debug, Clone)]
pub struct ConnectorHandle {
    sender: mpsc::Sender<Command>,
}

impl ConnectorHandle {
    /// Creates a new `ConnectorHandle` wrapping the control sender.
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Triggers an immediate poll cycle without waiting for the next scheduled interval.
    pub async fn poll_now(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::PollNow)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Requests an immediate resynchronization poll.
    pub async fn request_resync(&self) -> Result<(), ControlError> {
        self.poll_now().await
    }

    /// Pauses the connector: cancels current request, emits `ConnectionLost`, and suspends polling.
    pub async fn pause(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Pause)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Resumes the connector: increments delivery generation, enters `ResyncStarted`, and polls immediately.
    pub async fn resume(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Resume)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Updates the set of configured quote currency codes for subsequent polls.
    pub async fn set_symbols(&self, quotes: Vec<String>) -> Result<(), ControlError> {
        crate::frankfurter::config::validate_quote_currencies(&quotes)
            .map_err(|e| ControlError::InvalidSymbols(e.to_string()))?;
        self.sender
            .send(Command::SetSymbols(quotes))
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }
}
