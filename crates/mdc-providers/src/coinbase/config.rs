//! Configuration options for the Coinbase Exchange WebSocket connector.

use std::time::Duration;

/// Configuration options for Coinbase Exchange WebSocket connector.
#[derive(Debug, Clone)]
pub struct CoinbaseConfig {
    /// WebSocket endpoint URL. Default: `wss://ws-feed.exchange.coinbase.com`.
    pub ws_base_url: String,
    /// Maximum number of products per physical WebSocket connection. Default: 25.
    pub products_per_connection: usize,
    /// Heartbeat silence timeout before initiating an in-place resubscription. Default: 5s.
    pub heartbeat_timeout: Duration,
    /// Sequence jump threshold between consecutive heartbeats triggering resubscription. Default: Some(3,000).
    pub sequence_gap_threshold: Option<u64>,
    /// Snapshot waiting timeout before transitioning a symbol to quarantine. Default: 5s.
    pub resubscribe_timeout: Duration,
    /// Duration a symbol remains quarantined before attempting fresh resubscription. Default: 30s.
    pub quarantine_duration: Duration,
    /// Silence watchdog ping trigger duration. Default: 5s.
    pub watchdog_ping_after: Duration,
    /// Silence watchdog reconnect trigger duration. Default: 10s.
    pub watchdog_reconnect_after: Duration,
    /// Initial reconnect backoff delay. Default: 500ms.
    pub initial_backoff: Duration,
    /// Maximum reconnect backoff delay. Default: 30s.
    pub max_backoff: Duration,
    /// Channels and streams configuration.
    pub streams: CoinbaseStreams,
    /// Injected session run token (generated if not provided).
    pub run_id: Option<mdc_core::RunId>,
    /// WebSocket connect (TCP, TLS, and upgrade handshake) timeout. Default: 10 seconds.
    pub connection_timeout: Duration,
}

impl CoinbaseConfig {
    /// Validates configuration invariants.
    pub fn validate(&self) -> Result<(), mdc_core::MdcError> {
        if self.streams.depth && !self.streams.heartbeat {
            return Err(mdc_core::MdcError::Config(
                "Coinbase depth stream requires heartbeat stream to be enabled for watchdog integrity".to_string(),
            ));
        }
        Ok(())
    }
}

/// Channel and stream subscription switches for Coinbase Exchange.
#[derive(Debug, Clone)]
pub struct CoinbaseStreams {
    /// Ingest `level2_batch` channel. Default: `true`.
    pub depth: bool,
    /// Ingest `heartbeat` channel. Default: `true`.
    pub heartbeat: bool,
    /// Ingest `matches` (public trades) channel. Default: `false`.
    pub matches: bool,
}

impl Default for CoinbaseStreams {
    fn default() -> Self {
        Self {
            depth: true,
            heartbeat: true,
            matches: false,
        }
    }
}

impl Default for CoinbaseConfig {
    fn default() -> Self {
        Self {
            ws_base_url: "wss://ws-feed.exchange.coinbase.com".to_string(),
            products_per_connection: 25,
            heartbeat_timeout: Duration::from_secs(5),
            sequence_gap_threshold: Some(3000),
            resubscribe_timeout: Duration::from_secs(5),
            quarantine_duration: Duration::from_secs(30),
            watchdog_ping_after: Duration::from_secs(5),
            watchdog_reconnect_after: Duration::from_secs(10),
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            streams: CoinbaseStreams::default(),
            run_id: None,
            connection_timeout: Duration::from_secs(10),
        }
    }
}
