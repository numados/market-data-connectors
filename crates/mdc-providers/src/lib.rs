//! Provider implementations for market data connectors.

pub(crate) mod catalog_util;

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
