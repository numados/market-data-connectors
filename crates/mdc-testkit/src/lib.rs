//! In-process deterministic mock servers and test utilities for market data connectors.

pub mod fixture;
pub mod rest;
pub mod sink;
pub mod ws;

pub use fixture::load_fixture;
pub use rest::MockRest;
pub use sink::{NoopStreamingSink, RecordedEvent, RecordingSink};
pub use ws::{MockWsServer, WsAction};
