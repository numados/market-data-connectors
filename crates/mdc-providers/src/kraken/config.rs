//! Configuration parameters for Kraken WebSocket v2 connector.

use std::time::Duration;
use url::Url;

/// Active subscription stream selectors for Kraken WebSocket v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KrakenStreams {
    /// Level 2 order book updates (`book` channel). Default: `true`.
    pub depth: bool,
    /// Public trade executions (`trade` channel). Default: `false`.
    pub trade: bool,
}

impl Default for KrakenStreams {
    fn default() -> Self {
        Self {
            depth: true,
            trade: false,
        }
    }
}

/// Configuration options for the Kraken connector.
#[derive(Clone)]
pub struct KrakenConfig {
    /// Base WebSocket URL (default: `wss://ws.kraken.com/v2`).
    pub ws_base_url: Url,
    /// Order book depth requested per symbol ({10, 25, 100, 500, 1000}; default: 1000).
    pub depth: usize,
    /// Maximum number of symbols assigned per WebSocket connection task.
    /// Default: 300 (legacy INSTRUMENTS_PER_CONNECTION).
    pub symbols_per_connection: usize,
    /// Batch size for in-place unsubscribe / subscribe control frames.
    /// Default: 30 (legacy REFRESH_BATCH_SIZE, prevents socket buffer overflow).
    pub resubscribe_batch_size: usize,
    /// Maximum duration to wait for a valid snapshot before entering quarantine.
    /// Default: 5 seconds.
    pub resubscribe_timeout: Duration,
    /// Quarantine duration during which no retries are made.
    /// Default: 30 seconds.
    pub quarantine_duration: Duration,
    /// Stream selectors (book, trade).
    pub streams: KrakenStreams,
    /// Silence duration after which the connector sends a Ping frame.
    /// Default: 10 seconds.
    pub watchdog_ping_after: Duration,
    /// Silence grace duration after ping after which the connector forces a reconnect.
    /// Default: 30 seconds.
    pub watchdog_reconnect_after: Duration,
    /// Checksum verification policy (Required or Optional). Default: Required.
    pub checksum_policy: mdc_core::ChecksumPolicy,
    /// Initial reconnect backoff duration. Default: 1 second.
    pub initial_backoff: Duration,
    /// Maximum reconnect backoff duration. Default: 15 seconds.
    pub max_backoff: Duration,
    /// Injected session run token (generated if not provided).
    pub run_id: Option<mdc_core::RunId>,
    /// Optional pluggable checksum calculation strategy (default: Kraken CRC32 over top 10 levels).
    pub checksum_strategy: Option<std::sync::Arc<dyn mdc_core::integrity::ChecksumStrategy>>,
}

impl std::fmt::Debug for KrakenConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KrakenConfig")
            .field("ws_base_url", &self.ws_base_url)
            .field("depth", &self.depth)
            .field("symbols_per_connection", &self.symbols_per_connection)
            .field("resubscribe_batch_size", &self.resubscribe_batch_size)
            .field("resubscribe_timeout", &self.resubscribe_timeout)
            .field("quarantine_duration", &self.quarantine_duration)
            .field("streams", &self.streams)
            .field("watchdog_ping_after", &self.watchdog_ping_after)
            .field("watchdog_reconnect_after", &self.watchdog_reconnect_after)
            .field("checksum_policy", &self.checksum_policy)
            .field("initial_backoff", &self.initial_backoff)
            .field("max_backoff", &self.max_backoff)
            .field("run_id", &self.run_id)
            .field(
                "checksum_strategy",
                &self.checksum_strategy.as_ref().map(|s| s.depth()),
            )
            .finish()
    }
}

impl Default for KrakenConfig {
    fn default() -> Self {
        #[allow(clippy::unwrap_used, clippy::expect_used)]
        Self {
            ws_base_url: Url::parse("wss://ws.kraken.com/v2").expect("valid default Kraken WS url"),
            depth: 1000,
            symbols_per_connection: 300,
            resubscribe_batch_size: 30,
            resubscribe_timeout: Duration::from_secs(5),
            quarantine_duration: Duration::from_secs(30),
            streams: KrakenStreams::default(),
            checksum_policy: mdc_core::ChecksumPolicy::Required,
            watchdog_ping_after: Duration::from_secs(10),
            watchdog_reconnect_after: Duration::from_secs(30),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(15),
            run_id: None,
            checksum_strategy: None,
        }
    }
}
