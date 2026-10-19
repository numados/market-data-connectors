//! Provider implementations for market data connectors.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

/// Returns the current wall-clock timestamp in nanoseconds since Unix epoch.
pub fn now_nanos() -> mdc_core::UnixNanos {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO);
    mdc_core::UnixNanos(duration.as_nanos().min(u64::MAX as u128) as u64)
}

static RUN_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Generates a unique [`mdc_core::RunId`] mixing current nanosecond timestamp with an atomic process nonce.
pub fn generate_run_id() -> mdc_core::RunId {
    let now = now_nanos().0;
    let nonce = RUN_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    mdc_core::RunId::from_nanos_and_nonce(now, nonce)
}

pub(crate) mod catalog_util;
pub(crate) mod sink_util;

#[cfg(any(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
))]
pub(crate) mod rest_quote_util;

pub mod config;

#[cfg(any(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
    feature = "frankfurter",
    feature = "vwd",
))]
pub mod http;

#[cfg(any(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
    feature = "frankfurter",
    feature = "vwd",
))]
pub mod catalog_client;

#[cfg(any(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
))]
pub mod rest_quote_client;

#[cfg(feature = "binance")]
pub mod binance;

#[cfg(feature = "kraken")]
pub mod kraken;

#[cfg(feature = "coinbase")]
pub mod coinbase;

#[cfg(feature = "bitfinex")]
pub mod bitfinex;

#[cfg(feature = "gateio")]
pub mod gateio;

#[cfg(feature = "bitstamp")]
pub mod bitstamp;

#[cfg(feature = "lmax")]
pub mod lmax;

#[cfg(feature = "vwd")]
pub mod vwd;

#[cfg(feature = "huobi")]
pub mod huobi;

#[cfg(feature = "coinbase-advanced")]
pub mod coinbase_advanced;

#[cfg(feature = "frankfurter")]
pub mod frankfurter;
