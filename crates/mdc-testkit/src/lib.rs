//! In-process deterministic mock servers and test utilities for market data connectors.

pub mod conformance;
pub mod fixture;
pub mod rest;
pub mod sink;
pub mod ws;

pub use conformance::{MrsConsumerHarness, MrsConsumerStats};
pub use fixture::{load_fixture, load_fixture_bytes};
pub use rest::MockRest;
pub use sink::{NoopStreamingSink, RecordedEvent, RecordingSink};
pub use ws::{MockWsServer, WsAction};
