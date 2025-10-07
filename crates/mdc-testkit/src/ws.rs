//! In-process deterministic mock WebSocket server.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use url::Url;

/// Scripted actions executed by the mock WebSocket server for an accepted connection.
#[derive(Debug, Clone)]
pub enum WsAction {
    /// Sends a JSON or text message to the client.
    SendText(String),
    /// Sends a binary message to the client.
    SendBinary(Vec<u8>),
    /// Sends a WebSocket ping frame with optional payload.
    SendPing(Vec<u8>),
    /// Closes the connection with an optional close code.
    Close(Option<u16>),
    /// Drops the TCP connection immediately without close handshake.
    DropConnection,
    /// Pauses server execution for the specified duration.
    Sleep(Duration),
    /// Waits until the client sends a message before proceeding.
    WaitClientMessage,
}

/// In-process deterministic mock WebSocket server.
pub struct MockWsServer {
    addr: SocketAddr,
    handshake_paths: Arc<Mutex<Vec<String>>>,
    received_messages: Arc<Mutex<Vec<String>>>,
    connection_count: Arc<AtomicUsize>,
    ping_count: Arc<AtomicUsize>,
    scripts: Arc<Mutex<VecDeque<Vec<WsAction>>>>,
    broadcast_tx: broadcast::Sender<Message>,
    _drop_guard: tokio::sync::oneshot::Sender<()>,
}

impl MockWsServer {
    /// Spawns a new `MockWsServer` listening on `127.0.0.1:0`.
    pub async fn spawn() -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let handshake_paths = Arc::new(Mutex::new(Vec::new()));
        let received_messages = Arc::new(Mutex::new(Vec::new()));
        let connection_count = Arc::new(AtomicUsize::new(0));
        let ping_count = Arc::new(AtomicUsize::new(0));
        let scripts = Arc::new(Mutex::new(VecDeque::new()));
        let (broadcast_tx, _) = broadcast::channel(128);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_paths = Arc::clone(&handshake_paths);
        let server_received = Arc::clone(&received_messages);
        let server_conns = Arc::clone(&connection_count);
        let server_pings = Arc::clone(&ping_count);
        let server_scripts = Arc::clone(&scripts);
        let server_broadcast = broadcast_tx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accept_res = listener.accept() => {
                        let (stream, _) = match accept_res {
                            Ok(conn) => conn,
                            Err(_) => break,
                        };

                        server_conns.fetch_add(1, Ordering::SeqCst);
                        let paths = Arc::clone(&server_paths);
                        let received = Arc::clone(&server_received);
                        let pings = Arc::clone(&server_pings);
                        let scripts = Arc::clone(&server_scripts);
                        let mut broadcast_rx = server_broadcast.subscribe();

                        tokio::spawn(async move {
                            let mut path_capture = String::new();
                            #[allow(clippy::result_large_err)]
                            let ws_res = tokio_tungstenite::accept_hdr_async(
                                stream,
                                |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                                 resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                                    path_capture = req.uri().to_string();
                                    Ok(resp)
                                },
                            )
                            .await;

                            let mut ws_stream = match ws_res {
                                Ok(ws) => ws,
                                Err(_) => return,
                            };

                            if !path_capture.is_empty() {
                                if let Ok(mut p) = paths.lock() {
                                    p.push(path_capture);
                                }
                            }

                            let script = {
                                let mut s = match scripts.lock() {
                                    Ok(guard) => guard,
                                    Err(p) => p.into_inner(),
                                };
                                s.pop_front().unwrap_or_default()
                            };

                            for action in script {
                                match action {
                                    WsAction::SendText(text) => {
                                        if ws_stream.send(Message::Text(text)).await.is_err() {
                                            return;
                                        }
                                    }
                                    WsAction::SendBinary(bytes) => {
                                        if ws_stream.send(Message::Binary(bytes)).await.is_err() {
                                            return;
                                        }
                                    }
                                    WsAction::SendPing(payload) => {
                                        if ws_stream.send(Message::Ping(payload)).await.is_err() {
                                            return;
                                        }
                                    }
                                    WsAction::Close(code) => {
                                        let close_code = code.map(CloseCode::from).unwrap_or(CloseCode::Normal);
                                        let frame = CloseFrame {
                                            code: close_code,
                                            reason: "mock close".into(),
                                        };
                                        let _ = ws_stream.send(Message::Close(Some(frame))).await;
                                        let _ = ws_stream.close(None).await;
                                        return;
                                    }
                                    WsAction::DropConnection => {
                                        return;
                                    }
                                    WsAction::Sleep(dur) => {
                                        tokio::time::sleep(dur).await;
                                    }
                                    WsAction::WaitClientMessage => {
                                        if let Some(Ok(Message::Text(t))) = ws_stream.next().await {
                                            if let Ok(mut r) = received.lock() {
                                                r.push(t.to_string());
                                            }
                                        }
                                    }
                                }
                            }

                            // Ongoing loop servicing broadcast and receiving messages
                            loop {
                                tokio::select! {
                                    msg = ws_stream.next() => {
                                        match msg {
                                            Some(Ok(Message::Text(t))) => {
                                                if let Ok(mut r) = received.lock() {
                                                    r.push(t.to_string());
                                                }
                                            }
                                            Some(Ok(Message::Ping(_))) => {
                                                pings.fetch_add(1, Ordering::SeqCst);
                                            }
                                            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                                            _ => {}
                                        }
                                    }
                                    b_msg = broadcast_rx.recv() => {
                                        if let Ok(m) = b_msg {
                                            if ws_stream.send(m).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });

        Ok(Self {
            addr,
            handshake_paths,
            received_messages,
            connection_count,
            ping_count,
            scripts,
            broadcast_tx,
            _drop_guard: shutdown_tx,
        })
    }

    /// Returns the WebSocket base URL of this mock server.
    pub fn ws_url(&self) -> Result<Url, url::ParseError> {
        Url::parse(&format!("ws://{}", self.addr))
    }

    /// Returns the recorded handshake URI paths across all connections.
    pub fn handshake_paths(&self) -> Vec<String> {
        self.handshake_paths
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Returns all client text messages received across all connections.
    pub fn received_messages(&self) -> Vec<String> {
        self.received_messages
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Returns the total number of TCP connections accepted.
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of WebSocket pings received across all connections.
    pub fn ping_count(&self) -> usize {
        self.ping_count.load(Ordering::SeqCst)
    }

    /// Queues a script of actions to execute sequentially for the next accepted connection.
    pub fn push_script(&self, script: Vec<WsAction>) {
        let mut guard = self.scripts.lock().unwrap_or_else(|p| p.into_inner());
        guard.push_back(script);
    }

    /// Broadcasts a text message to all currently connected clients.
    pub fn broadcast_text(&self, text: impl Into<String>) {
        let _ = self.broadcast_tx.send(Message::Text(text.into()));
    }

    /// Broadcasts a binary frame to all currently connected clients.
    pub fn broadcast_binary(&self, data: impl Into<Vec<u8>>) {
        let _ = self.broadcast_tx.send(Message::Binary(data.into()));
    }

    /// Broadcasts a close frame to all currently connected clients.
    pub fn broadcast_close(&self) {
        let frame = CloseFrame {
            code: CloseCode::Normal,
            reason: "mock broadcast close".into(),
        };
        let _ = self.broadcast_tx.send(Message::Close(Some(frame)));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_ws_server_lifecycle() {
        let server = MockWsServer::spawn().await.expect("spawn server");
        let ws_url = server.ws_url().expect("ws url");
        assert_eq!(server.connection_count(), 0);

        let (mut client, _) = tokio_tungstenite::connect_async(ws_url.as_str())
            .await
            .expect("connect client");

        assert_eq!(server.connection_count(), 1);

        client
            .send(Message::Text("hello from client".into()))
            .await
            .expect("send message");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let msgs = server.received_messages();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0], "hello from client");
    }
}
