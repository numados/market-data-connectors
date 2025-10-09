//! Provider capabilities and consumer requirement contracts.

pub use crate::catalog::DiscoveryCapability;
pub use crate::rest_quote::RestQuoteCapability;
use serde::{Deserialize, Serialize};

/// Mode of top-of-book market quote representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteMode {
    /// Native BBO / BookTicker stream directly from the venue.
    NativeTicker,
    /// Synthetic top-of-book projected from a validated Level 2 order book.
    L2Projection,
    /// Periodic indicative reference rate (e.g. daily central bank fix).
    ReferenceRate,
}

/// Scope of trade identity uniqueness guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityScope {
    /// Unique per market/symbol indefinitely.
    PerMarket,
    /// Unique within a venue session or trading day.
    VenueSession,
    /// Globally unique across the entire venue.
    Global,
}

/// Semantic interpretation of executed trade side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideSemantics {
    /// Identifies the aggressor/taker side (standard for MRS trade acceptance).
    AggressorTaker,
    /// Venue only reports maker side.
    MakerOnly,
}

/// Provider execution trade capability profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TradeCapability {
    /// Venue does not offer trade execution events.
    Unsupported,
    /// Trades exist but lack immutable identity or reliable taker side semantics.
    Unverified {
        /// Explanation why trades cannot be accepted as verified.
        reason: String,
    },
    /// Trades are fully verified with immutable native identities and taker side semantics.
    Verified {
        /// Scope of trade ID uniqueness.
        identity_scope: IdentityScope,
        /// Scheme used to form the stable namespace.
        namespace_scheme: String,
        /// Side semantics guarantee.
        side_semantics: SideSemantics,
    },
}

/// Timestamp source and precision capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampCapability {
    /// Venue provides exchange timestamps in nanosecond precision.
    ExchangeNanoseconds,
    /// Venue provides exchange timestamps in millisecond precision.
    ExchangeMilliseconds,
    /// Venue provides no exchange timestamp; ingestion receipt timestamp is authoritative.
    ReceiptOnly,
}

/// Book integrity verification mechanisms supported by the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityCapability {
    /// Incremental sequence numbers (e.g. Binance U/u, Huobi prevSeqNum..seqNum).
    SequenceNumbers,
    /// CRC32 checksum over top book levels (e.g. Kraken, Bitfinex).
    ChecksumCrc32,
    /// Monotonic timestamp gating (e.g. Bitstamp).
    TimeGated,
    /// Periodic heartbeat watchdog keepalive.
    HeartbeatWatchdog,
}

/// Historical replay / backfill capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReplayCapability {
    /// Live feed only; no historical replay supported.
    None,
    /// Documented replay protocol available with cursor lookback bounds.
    DocumentedProtocol {
        /// Protocol identifier.
        name: String,
        /// Maximum lookback in sequence numbers or milliseconds, if bounded.
        max_lookback: Option<u64>,
    },
}

/// Operational bounds and limits for a provider connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityBounds {
    /// Maximum order book depth maintained or supported, if bounded.
    pub max_depth: Option<usize>,
    /// Maximum symbols assigned per WebSocket connection.
    pub max_symbols_per_connection: usize,
}

impl Default for CapabilityBounds {
    fn default() -> Self {
        Self {
            max_depth: None,
            max_symbols_per_connection: 25,
        }
    }
}

/// Full capability record declared by an exchange connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    /// Available quote representation modes.
    pub quote_modes: Vec<QuoteMode>,
    /// Public execution trade stream capability.
    pub trades: TradeCapability,
    /// Timestamp precision profile.
    pub timestamps: TimestampCapability,
    /// Integrity verification mechanism.
    pub integrity: IntegrityCapability,
    /// Replay capability.
    pub replay: ReplayCapability,
    /// Operational capacity limits.
    pub bounds: CapabilityBounds,
    /// Discovery capability profile.
    pub discovery: DiscoveryCapability,
    /// Supports REST quote snapshot polling.
    pub rest_quote: RestQuoteCapability,
}

/// Consumer requirements evaluated before connector startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerRequirements {
    /// Required quote representation mode.
    pub quote_mode: QuoteMode,
    /// Requires verified immutable trade identities.
    pub require_stable_trades: bool,
    /// Requires verified aggressor / taker side semantics.
    pub require_verified_side: bool,
}

/// Errors when a connector does not satisfy consumer requirements.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CapabilityError {
    /// Requested quote mode is not supported by the provider.
    #[error("unsupported quote mode: requested {requested:?}, available {available:?}")]
    UnsupportedQuoteMode {
        /// Requested mode.
        requested: QuoteMode,
        /// Available modes.
        available: Vec<QuoteMode>,
    },
    /// Stable trades required but provider trades capability is insufficient.
    #[error("stable trade identity required but provider trades capability is {actual:?}")]
    UnstableTrades {
        /// Actual trade capability.
        actual: TradeCapability,
    },
    /// Verified aggressor side semantics required but provider does not guarantee it.
    #[error("verified trade side semantics required but provider trades capability is {actual:?}")]
    UnverifiedTradeSide {
        /// Actual trade capability.
        actual: TradeCapability,
    },
}

impl ProviderCapabilities {
    /// Returns `true` if complete instrument catalog discovery is supported.
    pub fn is_discovery_supported(&self) -> bool {
        self.discovery.is_supported()
    }

    /// Returns `true` if REST quote snapshots are supported.
    pub fn is_rest_quote_supported(&self) -> bool {
        self.rest_quote.is_supported()
    }

    /// Evaluates `requirements` against this capability declaration.
    ///
    /// Fails closed with [`CapabilityError`] if any requirement cannot be satisfied.
    pub fn check_requirements(
        &self,
        requirements: &ConsumerRequirements,
    ) -> Result<(), CapabilityError> {
        if !self.quote_modes.contains(&requirements.quote_mode) {
            return Err(CapabilityError::UnsupportedQuoteMode {
                requested: requirements.quote_mode,
                available: self.quote_modes.clone(),
            });
        }

        if requirements.require_stable_trades {
            match &self.trades {
                TradeCapability::Verified { .. } => {}
                _ => {
                    return Err(CapabilityError::UnstableTrades {
                        actual: self.trades.clone(),
                    });
                }
            }
        }

        if requirements.require_verified_side {
            match &self.trades {
                TradeCapability::Verified {
                    side_semantics: SideSemantics::AggressorTaker,
                    ..
                } => {}
                _ => {
                    return Err(CapabilityError::UnverifiedTradeSide {
                        actual: self.trades.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_check_requirements_success() {
        let caps = ProviderCapabilities {
            quote_modes: vec![QuoteMode::NativeTicker, QuoteMode::L2Projection],
            trades: TradeCapability::Verified {
                identity_scope: IdentityScope::PerMarket,
                namespace_scheme: "binance".to_string(),
                side_semantics: SideSemantics::AggressorTaker,
            },
            timestamps: TimestampCapability::ExchangeMilliseconds,
            integrity: IntegrityCapability::SequenceNumbers,
            replay: ReplayCapability::None,
            bounds: CapabilityBounds::default(),
            discovery: DiscoveryCapability::from(false),
            rest_quote: false.into(),
        };

        let req = ConsumerRequirements {
            quote_mode: QuoteMode::NativeTicker,
            require_stable_trades: true,
            require_verified_side: true,
        };

        assert!(caps.check_requirements(&req).is_ok());
    }

    #[test]
    fn test_check_requirements_unsupported_quote_mode() {
        let caps = ProviderCapabilities {
            quote_modes: vec![QuoteMode::L2Projection],
            trades: TradeCapability::Unsupported,
            timestamps: TimestampCapability::ExchangeMilliseconds,
            integrity: IntegrityCapability::SequenceNumbers,
            replay: ReplayCapability::None,
            bounds: CapabilityBounds::default(),
            discovery: DiscoveryCapability::from(false),
            rest_quote: false.into(),
        };

        let req = ConsumerRequirements {
            quote_mode: QuoteMode::NativeTicker,
            require_stable_trades: false,
            require_verified_side: false,
        };

        let err = caps.check_requirements(&req).unwrap_err();
        assert!(matches!(err, CapabilityError::UnsupportedQuoteMode { .. }));
    }

    #[test]
    fn test_check_requirements_unstable_trades() {
        let caps = ProviderCapabilities {
            quote_modes: vec![QuoteMode::L2Projection],
            trades: TradeCapability::Unverified {
                reason: "no unique trade ID".to_string(),
            },
            timestamps: TimestampCapability::ExchangeMilliseconds,
            integrity: IntegrityCapability::SequenceNumbers,
            replay: ReplayCapability::None,
            bounds: CapabilityBounds::default(),
            discovery: DiscoveryCapability::from(false),
            rest_quote: false.into(),
        };

        let req = ConsumerRequirements {
            quote_mode: QuoteMode::L2Projection,
            require_stable_trades: true,
            require_verified_side: false,
        };

        let err = caps.check_requirements(&req).unwrap_err();
        assert!(matches!(err, CapabilityError::UnstableTrades { .. }));
    }
}
