//! Public executed trades and immutable native trade identity.

use super::side::Side;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smallstr::SmallString;
use std::fmt;

/// Validation error when creating a `TradeId` or `TradeNamespace`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TradeIdentityError {
    #[error("trade identifier string cannot be empty")]
    Empty,
    #[error("trade identifier too long: max {max} bytes, got {actual}")]
    TooLong { max: usize, actual: usize },
    #[error("trade identifier must contain only ASCII characters")]
    NonAscii,
    #[error("trade identifier contains ASCII control characters")]
    ControlCharacter,
}

/// Bounded inline namespace for trade identifiers (1–64 ASCII bytes).
/// Immutable, stable across reconnects and restarts.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TradeNamespace(SmallString<[u8; 64]>);

impl TradeNamespace {
    /// Creates and validates a `TradeNamespace`.
    pub fn new(s: &str) -> Result<Self, TradeIdentityError> {
        validate_identifier(s, 64)?;
        Ok(Self(SmallString::from(s)))
    }

    /// Borrows the namespace as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for TradeNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Bounded inline trade identifier (1–128 ASCII bytes).
/// Immutable, retains venue-exact token representation without truncation or float coercion.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TradeId(SmallString<[u8; 128]>);

impl TradeId {
    /// Creates and validates a `TradeId`.
    pub fn new(s: &str) -> Result<Self, TradeIdentityError> {
        validate_identifier(s, 128)?;
        Ok(Self(SmallString::from(s)))
    }

    /// Borrows the trade ID as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for TradeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn validate_identifier(s: &str, max_len: usize) -> Result<(), TradeIdentityError> {
    if s.is_empty() {
        return Err(TradeIdentityError::Empty);
    }
    if s.len() > max_len {
        return Err(TradeIdentityError::TooLong {
            max: max_len,
            actual: s.len(),
        });
    }
    if !s.is_ascii() {
        return Err(TradeIdentityError::NonAscii);
    }
    if s.bytes().any(|b| b.is_ascii_control()) {
        return Err(TradeIdentityError::ControlCharacter);
    }
    Ok(())
}

/// Durable trade identity scoped by namespace and value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NativeTradeIdentity {
    pub namespace: TradeNamespace,
    pub value: TradeId,
}

impl NativeTradeIdentity {
    /// Creates a new `NativeTradeIdentity`.
    pub fn new(namespace: TradeNamespace, value: TradeId) -> Self {
        Self { namespace, value }
    }
}

/// Public executed trade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    pub identity: Option<NativeTradeIdentity>,
    pub price: Decimal,
    pub volume: Decimal,
    /// Side of the aggressor (taker).
    /// Binance `is_buyer_maker == true` implies the buyer is passive and seller is aggressor => `Side::Ask`.
    pub aggressor: Side,
}

impl Trade {
    /// Creates a new `Trade`.
    pub fn new(
        identity: Option<NativeTradeIdentity>,
        price: Decimal,
        volume: Decimal,
        aggressor: Side,
    ) -> Self {
        Self {
            identity,
            price,
            volume,
            aggressor,
        }
    }

    /// Maps buyer-maker flag to aggressor `Side`.
    /// When `is_buyer_maker` is true, the maker was the buyer, so the taker (aggressor) was the seller (`Side::Ask`).
    /// When `is_buyer_maker` is false, the maker was the seller, so the taker was the buyer (`Side::Bid`).
    pub const fn aggressor_from_buyer_maker(is_buyer_maker: bool) -> Side {
        if is_buyer_maker {
            Side::Ask
        } else {
            Side::Bid
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn trade_namespace_validation() {
        assert!(TradeNamespace::new("").is_err());
        assert_eq!(
            TradeNamespace::new("").unwrap_err(),
            TradeIdentityError::Empty
        );

        let valid = TradeNamespace::new("binance-spot-v1").expect("valid");
        assert_eq!(valid.as_str(), "binance-spot-v1");
        assert_eq!(valid.to_string(), "binance-spot-v1");

        let long_64 = "a".repeat(64);
        assert!(TradeNamespace::new(&long_64).is_ok());

        let too_long_65 = "a".repeat(65);
        assert_eq!(
            TradeNamespace::new(&too_long_65).unwrap_err(),
            TradeIdentityError::TooLong {
                max: 64,
                actual: 65
            }
        );

        assert_eq!(
            TradeNamespace::new("ns\0null").unwrap_err(),
            TradeIdentityError::ControlCharacter
        );
        assert_eq!(
            TradeNamespace::new("ns\nline").unwrap_err(),
            TradeIdentityError::ControlCharacter
        );
        assert_eq!(
            TradeNamespace::new("ns_привет").unwrap_err(),
            TradeIdentityError::NonAscii
        );
    }

    #[test]
    fn trade_id_validation() {
        assert_eq!(TradeId::new("").unwrap_err(), TradeIdentityError::Empty);

        let valid = TradeId::new("1234567890").expect("valid");
        assert_eq!(valid.as_str(), "1234567890");

        let long_128 = "x".repeat(128);
        assert!(TradeId::new(&long_128).is_ok());

        let too_long_129 = "x".repeat(129);
        assert_eq!(
            TradeId::new(&too_long_129).unwrap_err(),
            TradeIdentityError::TooLong {
                max: 128,
                actual: 129
            }
        );
    }

    #[test]
    fn buyer_maker_to_aggressor_table() {
        assert_eq!(Trade::aggressor_from_buyer_maker(true), Side::Ask);
        assert_eq!(Trade::aggressor_from_buyer_maker(false), Side::Bid);
    }

    #[test]
    fn trade_serde_roundtrip() {
        let ns = TradeNamespace::new("binance").expect("ns");
        let tid = TradeId::new("99887766").expect("id");
        let identity = NativeTradeIdentity::new(ns, tid);

        let trade = Trade::new(
            Some(identity),
            Decimal::from(50_000),
            Decimal::from(1),
            Side::Ask,
        );

        let json = serde_json::to_string(&trade).expect("serialize");
        let decoded: Trade = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, trade);
    }
}
