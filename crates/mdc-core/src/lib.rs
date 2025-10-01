//! Core abstractions and domain models for market data connectors.

#![forbid(unsafe_code)]

pub mod error;
pub mod model;
pub mod sink;

pub use error::MdcError;
pub use model::*;
pub use sink::{FnSink, MarketDataSink, VecSink};
