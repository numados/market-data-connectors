//! Kraken Spot instrument catalog decoder.

use std::collections::HashMap;

use crate::catalog_util::process_catalog_items;
use mdc_core::catalog::{
    CatalogError, CatalogInstrument, ListingState, NativeInstrumentId, ProviderKind, RawCurrency,
    RawStatus,
};
use mdc_core::model::Symbol;
use serde::Deserialize;

/// Default public Kraken REST endpoint for asset pairs.
pub const DEFAULT_KRAKEN_CATALOG_ENDPOINT: &str = "https://api.kraken.com/0/public/AssetPairs";

#[derive(Deserialize)]
struct RawKrakenResponse {
    error: Option<Vec<String>>,
    result: Option<HashMap<String, RawKrakenPair>>,
}

#[derive(Deserialize)]
struct RawKrakenPair {
    altname: Option<String>,
    wsname: Option<String>,
    base: Option<String>,
    quote: Option<String>,
    status: Option<String>,
}

/// Parses Kraken Spot asset pairs JSON payload into validated `CatalogInstrument` items.
pub fn parse_kraken_catalog(
    payload: &str,
    max_items: u32,
) -> Result<Vec<CatalogInstrument>, CatalogError> {
    let root: RawKrakenResponse =
        serde_json::from_str(payload).map_err(|e| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Kraken,
            reason: format!("failed to parse AssetPairs json: {e}"),
        })?;

    if let Some(errors) = root.error {
        if !errors.is_empty() {
            return Err(CatalogError::InvalidEnvelope {
                provider: ProviderKind::Kraken,
                reason: format!("kraken error envelope: {}", errors.join("; ")),
            });
        }
    }

    let pairs = root.result.ok_or_else(|| CatalogError::InvalidEnvelope {
        provider: ProviderKind::Kraken,
        reason: "missing or invalid 'result' object in response".to_string(),
    })?;

    let mut instruments = Vec::with_capacity(pairs.len());

    for (pair_code, pair_data) in pairs {
        let native_id =
            NativeInstrumentId::new(&pair_code).map_err(|e| CatalogError::MalformedItem {
                provider: ProviderKind::Kraken,
                reason: format!("invalid native instrument id '{pair_code}': {e}"),
            })?;

        // Determine subscription symbol preference: wsname -> altname -> native_id
        let subscription_symbol = pair_data
            .wsname
            .as_deref()
            .filter(|s| s.len() <= 16)
            .map(Symbol::from)
            .or_else(|| {
                pair_data
                    .altname
                    .as_deref()
                    .filter(|s| s.len() <= 16)
                    .map(Symbol::from)
            })
            .or_else(|| {
                if pair_code.len() <= 16 {
                    Some(Symbol::from(pair_code.as_str()))
                } else {
                    None
                }
            });

        let raw_base = pair_data
            .base
            .as_deref()
            .and_then(|s| RawCurrency::new(s).ok());
        let raw_quote = pair_data
            .quote
            .as_deref()
            .and_then(|s| RawCurrency::new(s).ok());

        let raw_status = pair_data
            .status
            .as_deref()
            .and_then(|s| RawStatus::new(s).ok());

        let listing = match pair_data.status.as_deref() {
            Some("online") => ListingState::Listed,
            Some(
                "cancel_only" | "post_only" | "limit_only" | "reduce_only" | "delisted"
                | "maintenance",
            ) => ListingState::Unavailable,
            _ => ListingState::Unknown,
        };

        instruments.push(CatalogInstrument::with_subscription_symbol(
            native_id,
            subscription_symbol,
            raw_base,
            raw_quote,
            listing,
            raw_status,
        ));
    }

    process_catalog_items(ProviderKind::Kraken, instruments, max_items)
}

/// Creates a default HTTP instrument catalog client for Kraken Spot.
pub fn kraken_catalog_client() -> Result<crate::catalog_client::HttpCatalogClient, CatalogError> {
    crate::catalog_client::HttpCatalogClient::new(
        ProviderKind::Kraken,
        DEFAULT_KRAKEN_CATALOG_ENDPOINT,
        parse_kraken_catalog,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kraken_complete() {
        let json = include_str!("../../tests/fixtures/catalog/kraken/complete.json");
        let items = parse_kraken_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 3);

        // Deterministic sort by native_id: SOLUSD, XETHZUSD, XXBTZUSD
        assert_eq!(items[0].native_id.as_str(), "SOLUSD");
        assert_eq!(items[0].listing, ListingState::Listed);
        assert_eq!(
            items[0].subscription_symbol.as_ref().unwrap().as_str(),
            "SOL/USD"
        );

        assert_eq!(items[1].native_id.as_str(), "XETHZUSD");
        assert_eq!(items[1].listing, ListingState::Listed);
        assert_eq!(items[1].raw_base.as_ref().unwrap().as_str(), "XETH");
        assert_eq!(items[1].raw_quote.as_ref().unwrap().as_str(), "ZUSD");
        assert_eq!(
            items[1].subscription_symbol.as_ref().unwrap().as_str(),
            "ETH/USD"
        );

        assert_eq!(items[2].native_id.as_str(), "XXBTZUSD");
        assert_eq!(items[2].listing, ListingState::Listed);
        assert_eq!(items[2].raw_base.as_ref().unwrap().as_str(), "XXBT");
        assert_eq!(items[2].raw_quote.as_ref().unwrap().as_str(), "ZUSD");
        assert_eq!(
            items[2].subscription_symbol.as_ref().unwrap().as_str(),
            "XBT/USD"
        );
    }

    #[test]
    fn test_parse_kraken_inactive() {
        let json = include_str!("../../tests/fixtures/catalog/kraken/inactive.json");
        let items = parse_kraken_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].native_id.as_str(), "AI16ZEUR");
        assert_eq!(items[0].listing, ListingState::Unavailable);
        assert_eq!(items[1].native_id.as_str(), "OLDCOINUSD");
        assert_eq!(items[1].listing, ListingState::Unavailable);
    }

    #[test]
    fn test_parse_kraken_empty() {
        let json = include_str!("../../tests/fixtures/catalog/kraken/empty.json");
        let items = parse_kraken_catalog(json, 100).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_kraken_error() {
        let json = include_str!("../../tests/fixtures/catalog/kraken/error.json");
        let err = parse_kraken_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
    }

    #[test]
    fn test_parse_kraken_malformed() {
        let json = include_str!("../../tests/fixtures/catalog/kraken/malformed.json");
        let err = parse_kraken_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
    }
}
