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
