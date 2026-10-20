//! Shared WebSocket connect helper with a bounded handshake.

use std::time::Duration;

use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

/// Why a WebSocket connect attempt failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnectError {
    /// The handshake did not complete within the configured timeout.
    #[error("connect timed out after {0:?}")]
    Timeout(Duration),
    /// The transport or handshake failed.
    #[error(transparent)]
    Ws(Box<tokio_tungstenite::tungstenite::Error>),
}

/// Connects to `url`, failing with [`ConnectError::Timeout`] if the handshake
/// does not complete within `timeout`. Callers race this against their
/// cancellation token.
pub(crate) async fn connect_ws(
    url: &str,
    timeout: Duration,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response), ConnectError> {
    match tokio::time::timeout(timeout, connect_async(url)).await {
        Ok(res) => res.map_err(|e| ConnectError::Ws(Box::new(e))),
        Err(_) => Err(ConnectError::Timeout(timeout)),
    }
}
