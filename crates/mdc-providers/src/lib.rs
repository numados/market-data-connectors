//! Provider implementations for market data connectors.

/// Returns the current wall-clock timestamp in nanoseconds since Unix epoch.
pub fn now_nanos() -> mdc_core::UnixNanos {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO);
    mdc_core::UnixNanos(duration.as_nanos().min(u64::MAX as u128) as u64)
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
