//! Coinbase Exchange instrument catalog decoder.

use crate::catalog_util::process_catalog_items;
use mdc_core::catalog::{
    CatalogError, CatalogInstrument, ListingState, NativeInstrumentId, ProviderKind, RawCurrency,
    RawStatus,
};
use serde::Deserialize;

/// Default public Coinbase Exchange REST endpoint for products.
pub const DEFAULT_COINBASE_CATALOG_ENDPOINT: &str = "https://api.exchange.coinbase.com/products";

#[derive(Deserialize)]
struct RawCoinbaseProduct {
    id: Option<String>,
    base_currency: Option<String>,
    quote_currency: Option<String>,
    status: Option<String>,
    #[serde(default)]
    trading_disabled: bool,
}

/// Parses Coinbase Exchange products JSON payload into validated `CatalogInstrument` items.
pub fn parse_coinbase_catalog(
    payload: &str,
    max_items: u32,
) -> Result<Vec<CatalogInstrument>, CatalogError> {
    // If it deserializes to an error object with "message", report error envelope
    if let Ok(err_val) = serde_json::from_str::<serde_json::Value>(payload) {
        if let Some(msg) = err_val.get("message").and_then(|m| m.as_str()) {
            return Err(CatalogError::InvalidEnvelope {
                provider: ProviderKind::Coinbase,
                reason: format!("coinbase error envelope: {msg}"),
            });
        }
    }

    let products: Vec<RawCoinbaseProduct> =
        serde_json::from_str(payload).map_err(|e| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Coinbase,
            reason: format!("failed to parse products json array: {e}"),
        })?;

    let mut instruments = Vec::with_capacity(products.len());

    for product in products {
        let id_str = product.id.ok_or_else(|| CatalogError::MalformedItem {
            provider: ProviderKind::Coinbase,
            reason: "missing 'id' property".to_string(),
        })?;

        let native_id =
            NativeInstrumentId::new(&id_str).map_err(|e| CatalogError::MalformedItem {
                provider: ProviderKind::Coinbase,
                reason: format!("invalid native instrument id '{id_str}': {e}"),
            })?;

        let raw_base = product
            .base_currency
            .as_deref()
            .and_then(|s| RawCurrency::new(s).ok());
        let raw_quote = product
            .quote_currency
            .as_deref()
            .and_then(|s| RawCurrency::new(s).ok());

        let raw_status = product
            .status
            .as_deref()
            .and_then(|s| RawStatus::new(s).ok());

        let listing = if product.status.as_deref() == Some("online") && !product.trading_disabled {
            ListingState::Listed
        } else if product.trading_disabled
            || matches!(
                product.status.as_deref(),
                Some(
                    "delisted"
                        | "cancel_only"
                        | "post_only"
                        | "limit_only"
                        | "internal"
                        | "offline"
                )
            )
        {
            ListingState::Unavailable
        } else {
            ListingState::Unknown
        };

        instruments.push(CatalogInstrument::new(
            native_id, raw_base, raw_quote, listing, raw_status,
        ));
    }

    process_catalog_items(ProviderKind::Coinbase, instruments, max_items)
}

/// Creates a default HTTP instrument catalog client for Coinbase Exchange Spot.
pub fn coinbase_catalog_client() -> Result<crate::catalog_client::HttpCatalogClient, CatalogError> {
    crate::catalog_client::HttpCatalogClient::new(
        ProviderKind::Coinbase,
        DEFAULT_COINBASE_CATALOG_ENDPOINT,
        parse_coinbase_catalog,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_coinbase_complete() {
        let json = include_str!("../../tests/fixtures/catalog/coinbase/complete.json");
        let items = parse_coinbase_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 3);

        // Deterministic sort: BTC-USD, ETH-USD, SOL-USD
        assert_eq!(items[0].native_id.as_str(), "BTC-USD");
        assert_eq!(items[0].listing, ListingState::Listed);
        assert_eq!(items[0].raw_base.as_ref().unwrap().as_str(), "BTC");
        assert_eq!(items[0].raw_quote.as_ref().unwrap().as_str(), "USD");
        assert_eq!(
            items[0].subscription_symbol.as_ref().unwrap().as_str(),
            "BTC-USD"
        );

        assert_eq!(items[1].native_id.as_str(), "ETH-USD");
        assert_eq!(items[1].listing, ListingState::Listed);

        assert_eq!(items[2].native_id.as_str(), "SOL-USD");
        assert_eq!(items[2].listing, ListingState::Listed);
    }

    #[test]
    fn test_parse_coinbase_inactive() {
        let json = include_str!("../../tests/fixtures/catalog/coinbase/inactive.json");
        let items = parse_coinbase_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].native_id.as_str(), "NKN-GBP");
        assert_eq!(items[0].listing, ListingState::Unavailable);
        assert_eq!(items[1].native_id.as_str(), "OLD-USD");
        assert_eq!(items[1].listing, ListingState::Unavailable);
    }

    #[test]
    fn test_parse_coinbase_empty() {
        let json = include_str!("../../tests/fixtures/catalog/coinbase/empty.json");
        let items = parse_coinbase_catalog(json, 100).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_coinbase_error() {
        let json = include_str!("../../tests/fixtures/catalog/coinbase/error.json");
        let err = parse_coinbase_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
    }

    #[test]
    fn test_parse_coinbase_malformed() {
        let json = include_str!("../../tests/fixtures/catalog/coinbase/malformed.json");
        let err = parse_coinbase_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::MalformedItem { .. }));
    }
}
