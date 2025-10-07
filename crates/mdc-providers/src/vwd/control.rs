//! Control commands and operator handle for the VWD REST polling connector.

use tokio::sync::mpsc;

/// Operator command sent to the VWD polling connection task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Triggers an immediate poll cycle.
    PollNow,
    /// Pauses polling loop.
    Pause,
    /// Resumes polling loop.
    Resume,
}

/// Errors occurring during connector control operations.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// Connection task channel closed.
    #[error("connection task channel closed")]
    ChannelClosed,
}

/// Operator handle for controlling an active VWD polling connector.
#[derive(Debug, Clone)]
pub struct ConnectorHandle {
    sender: mpsc::Sender<Command>,
}

impl ConnectorHandle {
    /// Creates a new `ConnectorHandle`.
    pub fn new(sender: mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Triggers an immediate poll cycle without waiting for the next interval.
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

    /// Pauses the polling loop (no HTTP requests will be dispatched while paused).
    pub async fn pause(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Pause)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }

    /// Resumes the polling loop after being paused.
    pub async fn resume(&self) -> Result<(), ControlError> {
        self.sender
            .send(Command::Resume)
            .await
            .map_err(|_| ControlError::ChannelClosed)
    }
}
