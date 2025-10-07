//! Provider implementations for market data connectors.

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
