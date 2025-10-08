//! Canonical market data models and event structures.

pub mod alert;
pub mod book;
pub mod capabilities;
pub mod delivery;
pub mod event;
pub mod price_level;
pub mod reference;
pub mod sequence;
pub mod side;
pub mod symbol;
pub mod ticker;
pub mod timestamp;
pub mod trade;
pub mod venue;

pub use alert::{DisconnectKind, IntegrityAlert, SourceErrorKind};
pub use book::{L2Delta, L2Snapshot};
pub use capabilities::{
    CapabilityBounds, CapabilityError, ConsumerRequirements, IdentityScope, IntegrityCapability,
    ProviderCapabilities, QuoteMode, ReplayCapability, SideSemantics, TimestampCapability,
    TradeCapability,
};
pub use delivery::{DeliveryStamp, RunId};
pub use event::{MarketDataKind, MarketEvent};
pub use price_level::PriceLevel;
pub use reference::ReferenceRate;
pub use sequence::SequenceRange;
pub use side::Side;
pub use symbol::{Symbol, SymbolTag};
pub use ticker::BookTicker;
pub use timestamp::UnixNanos;
pub use trade::{NativeTradeIdentity, Trade, TradeId, TradeIdentityError, TradeNamespace};
pub use venue::VenueId;
