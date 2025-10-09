//! Frankfurter ECB reference rate provider.
//!
//! # Endpoint & Limits
//! - Primary endpoint: `GET /v2/providers/ecb/rates?base=EUR&quotes=USD,GBP`
//! - Default origin: `https://api.frankfurter.dev`
//! - Limits: 1–100 distinct quote codes, 1 MiB body cap, 10s request timeout, 3600s poll interval.
//! - Single request in flight at any time; public endpoint with no credentials required.
//!
//! # Integrity Mechanism
//! - Dated reference rates emitted as [`MarketDataKind::ReferenceRate`](mdc_core::MarketDataKind::ReferenceRate)
//!   without fabricated order books or trades.
//! - Exact Decimal parsing from JSON numeric tokens directly without `f64` conversions.
//! - Observation date mapped to conservative UTC midnight `exchange_ts`.
//! - Strict batch completeness: missing, conflicting, unrequested, or nonpositive quotes reject the entire batch.
//! - In-band integrity alerts: `ResyncStarted` on startup/recovery, `ResyncCompleted` upon successful batch delivery,
//!   `SourceError` on network/status/validation failure, `ConnectionLost` on pause or symbols removal.
//!
//! # Usage Example
//! ```rust
//! use mdc_providers::frankfurter::{FrankfurterConfig, FrankfurterConnector};
//! use mdc_testkit::NoopStreamingSink;
//! use tokio_util::sync::CancellationToken;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let config = FrankfurterConfig::builder()
//!     .base_currency("EUR")
//!     .quote_currencies(vec!["USD".to_string(), "GBP".to_string()])
//!     .build()
//!     .expect("valid config");
//!
//! let connector = FrankfurterConnector::builder(config)
//!     .build()
//!     .expect("valid connector");
//!
//! let cancel_token = CancellationToken::new();
//! let (handle, join_handle) = connector.run(NoopStreamingSink, cancel_token.clone());
//! cancel_token.cancel();
//! let _ = join_handle.await;
//! # }
//! ```

pub mod bench_support;
pub mod config;
pub mod connection;
pub mod connector;
pub mod control;
pub mod normalize;
pub mod raw;
pub mod rest;

pub use config::{
    ConfigError, DEFAULT_BASE_CURRENCY, DEFAULT_BODY_CAP_BYTES, DEFAULT_FRANKFURTER_BASE_URL,
    DEFAULT_POLL_INTERVAL, DEFAULT_PROVIDER, DEFAULT_REQUEST_TIMEOUT, FrankfurterConfig,
    FrankfurterConfigBuilder, MAX_QUOTE_CODES,
};
pub use connector::{FrankfurterConnector, FrankfurterConnectorBuilder};
pub use control::{Command, ConnectorHandle, ControlError};
pub use normalize::{
    NormalizedBatchRate, build_pair_symbol, days_from_civil, days_in_month, is_leap_year,
    normalize_batch_from_scratch, parse_date_to_utc_midnight,
};
pub use raw::{
    FrankfurterScratch, FrankfurterScratchSeed, MAX_RATES_PER_BATCH, ParsedFrankfurterRate,
    RawRateRow, parse_rates_slice,
};
pub use rest::{FrankfurterRestClient, HttpPollError, PollResponse, build_rates_url};

use mdc_core::{
    CapabilityBounds, DiscoveryCapability, IntegrityCapability, ProviderCapabilities, QuoteMode,
    ReplayCapability, TimestampCapability, TradeCapability,
};

/// Static capability declaration for Frankfurter ECB reference rate provider.
pub static FRANKFURTER_CAPABILITIES: std::sync::LazyLock<ProviderCapabilities> =
    std::sync::LazyLock::new(|| ProviderCapabilities {
        quote_modes: vec![QuoteMode::ReferenceRate],
        trades: TradeCapability::Unsupported,
        timestamps: TimestampCapability::ExchangeMilliseconds,
        integrity: IntegrityCapability::HeartbeatWatchdog,
        replay: ReplayCapability::None,
        bounds: CapabilityBounds {
            max_depth: None,
            max_symbols_per_connection: 100,
        },
        discovery: DiscoveryCapability::unsupported(
            "Frankfurter does not support variable instrument catalog discovery",
        ),
        rest_quote: true.into(),
    });
