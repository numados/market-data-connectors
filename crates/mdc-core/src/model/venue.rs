//! Exchange and data venue identifiers.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Exchange venue identifier. Closed set with non-exhaustive marking for semver evolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum VenueId {
    /// Binance exchange venue.
    Binance,
    /// Legacy Coinbase Exchange / Pro venue.
    Coinbase,
    /// Coinbase Advanced Trade venue.
    CoinbaseAdvanced,
    /// Kraken cryptocurrency exchange.
    Kraken,
    /// Gate.io cryptocurrency exchange.
    Gateio,
    /// Bitstamp cryptocurrency exchange.
    Bitstamp,
    /// Bitfinex cryptocurrency exchange.
    Bitfinex,
    /// LMAX Exchange (FX / Digital).
    Lmax,
    /// Vereinigte Wirtschaftsdienste (VWD) market data feed.
    Vwd,
    /// Huobi / HTX cryptocurrency exchange.
    Huobi,
    /// European Central Bank reference FX feed via Frankfurter API.
    FrankfurterEcb,
}

impl fmt::Display for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Binance => "binance",
            Self::Coinbase => "coinbase",
            Self::CoinbaseAdvanced => "coinbase_advanced",
            Self::Kraken => "kraken",
            Self::Gateio => "gateio",
            Self::Bitstamp => "bitstamp",
            Self::Bitfinex => "bitfinex",
            Self::Lmax => "lmax",
            Self::Vwd => "vwd",
            Self::Huobi => "huobi",
            Self::FrankfurterEcb => "frankfurter_ecb",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn serde_snake_case_roundtrip() {
        let venues = [
            (VenueId::Binance, "\"binance\""),
            (VenueId::Coinbase, "\"coinbase\""),
            (VenueId::CoinbaseAdvanced, "\"coinbase_advanced\""),
            (VenueId::Kraken, "\"kraken\""),
            (VenueId::Gateio, "\"gateio\""),
            (VenueId::Bitstamp, "\"bitstamp\""),
            (VenueId::Bitfinex, "\"bitfinex\""),
            (VenueId::Lmax, "\"lmax\""),
            (VenueId::Vwd, "\"vwd\""),
            (VenueId::Huobi, "\"huobi\""),
            (VenueId::FrankfurterEcb, "\"frankfurter_ecb\""),
        ];

        for (venue, expected_json) in venues {
            let serialized = serde_json::to_string(&venue).expect("serialize venue");
            assert_eq!(serialized, expected_json);
            let deserialized: VenueId =
                serde_json::from_str(&serialized).expect("deserialize venue");
            assert_eq!(deserialized, venue);
        }
    }

    #[test]
    fn display_matches_snake_case() {
        assert_eq!(VenueId::Binance.to_string(), "binance");
        assert_eq!(VenueId::CoinbaseAdvanced.to_string(), "coinbase_advanced");
        assert_eq!(VenueId::FrankfurterEcb.to_string(), "frankfurter_ecb");
    }
}
