//! Configuration parameters for Binance Spot market data connector.

use std::time::Duration;
use url::Url;

/// Stream selection flags for Binance Spot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceStreams {
    /// Depth updates (`@depth@100ms`). Default: `true`.
    pub depth: bool,
    /// Top of book ticker updates (`@bookTicker`). Default: `true`.
    pub book_ticker: bool,
    /// Public execution trades (`@trade`). Default: `false`.
    pub trade: bool,
}

impl Default for BinanceStreams {
    fn default() -> Self {
        Self {
            depth: true,
            book_ticker: true,
            trade: false,
        }
    }
}

/// Configuration options for the Binance connector.
#[derive(Debug, Clone)]
pub struct BinanceConfig {
    /// Base WebSocket URL (default: `wss://stream.binance.com:443`).
    pub ws_base_url: Url,
    /// Base REST URL for order book depth snapshots (default: `https://api.binance.com`).
    pub rest_base_url: Url,
    /// Maximum number of symbols assigned per WebSocket connection task.
    /// Default: 25 (Binance technical limit is 1,024 streams per socket).
    pub symbols_per_connection: usize,
    /// Active WebSocket stream subscriptions.
    pub streams: BinanceStreams,
    /// In-flight delta buffer capacity per symbol during snapshot reconciliation.
    pub resync_buffer_capacity: usize,
    /// Failure retry cooldown per symbol.
    pub snapshot_retry_cooldown: Duration,
    /// Silence duration after which the connector sends a Ping frame.
    pub watchdog_ping_after: Duration,
    /// Silence grace duration after ping after which the connector forces a reconnect.
    pub watchdog_reconnect_after: Duration,
    /// REST request timeout for depth snapshots.
    pub rest_timeout: Duration,
    /// Initial reconnect backoff duration. Default: 1 second.
    pub initial_backoff: Duration,
    /// Maximum reconnect backoff duration. Default: 15 seconds.
    pub max_backoff: Duration,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        #[allow(clippy::unwrap_used, clippy::expect_used)]
        Self {
            ws_base_url: Url::parse("wss://stream.binance.com:443").expect("valid default WS url"),
            rest_base_url: Url::parse("https://api.binance.com").expect("valid default REST url"),
            symbols_per_connection: 25,
            streams: BinanceStreams::default(),
            resync_buffer_capacity: 5_000,
            snapshot_retry_cooldown: Duration::from_secs(5),
            watchdog_ping_after: Duration::from_secs(5),
            watchdog_reconnect_after: Duration::from_secs(5),
            rest_timeout: Duration::from_secs(30),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(15),
        }
    }
}

impl BinanceConfig {
    /// Creates a configuration pointing to mock servers for local testing.
    pub fn for_test(ws_url: Url, rest_url: Url) -> Self {
        Self {
            ws_base_url: ws_url,
            rest_base_url: rest_url,
            symbols_per_connection: 25,
            streams: BinanceStreams::default(),
            resync_buffer_capacity: 5_000,
            snapshot_retry_cooldown: Duration::from_millis(100),
            watchdog_ping_after: Duration::from_secs(5),
            watchdog_reconnect_after: Duration::from_secs(5),
            rest_timeout: Duration::from_secs(5),
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
        }
    }
}
