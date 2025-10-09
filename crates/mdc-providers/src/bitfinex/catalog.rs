//! Bitfinex Spot instrument catalog decoder.

use crate::catalog_util::process_catalog_items;
use mdc_core::catalog::{
    CatalogError, CatalogInstrument, ListingState, NativeInstrumentId, ProviderKind,
};
use mdc_core::model::Symbol;

/// Default public Bitfinex REST endpoint for exchange trading pairs configuration.
pub const DEFAULT_BITFINEX_CATALOG_ENDPOINT: &str =
    "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange";

/// Parses Bitfinex Spot exchange pairs JSON payload into validated `CatalogInstrument` items.
pub fn parse_bitfinex_catalog(
    payload: &str,
    max_items: u32,
) -> Result<Vec<CatalogInstrument>, CatalogError> {
    let root: serde_json::Value =
        serde_json::from_str(payload).map_err(|e| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Bitfinex,
            reason: format!("failed to parse json: {e}"),
        })?;

    // Check if error envelope object was returned
    if root.is_object() {
        return Err(CatalogError::InvalidEnvelope {
            provider: ProviderKind::Bitfinex,
            reason: format!("bitfinex error envelope: {payload}"),
        });
    }

    let outer_array = root
        .as_array()
        .ok_or_else(|| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Bitfinex,
            reason: "expected outer JSON array".to_string(),
        })?;

    if outer_array.is_empty() {
        return Err(CatalogError::InvalidEnvelope {
            provider: ProviderKind::Bitfinex,
            reason: "outer array is empty, expected nested array of pairs".to_string(),
        });
    }

    let inner_array = outer_array[0]
        .as_array()
        .ok_or_else(|| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Bitfinex,
            reason: "expected first element of outer array to be array of strings".to_string(),
        })?;

    let mut instruments = Vec::with_capacity(inner_array.len());

    for item in inner_array {
        let pair_str = item.as_str().ok_or_else(|| CatalogError::MalformedItem {
            provider: ProviderKind::Bitfinex,
            reason: format!("expected pair element to be string, got: {item}"),
        })?;

        let native_id =
            NativeInstrumentId::new(pair_str).map_err(|e| CatalogError::MalformedItem {
                provider: ProviderKind::Bitfinex,
                reason: format!("invalid native instrument id '{pair_str}': {e}"),
            })?;

        // Derive checked 't' subscription prefix for trading pairs
        let t_symbol_str = if pair_str.starts_with('t') {
            pair_str.to_string()
        } else {
            format!("t{pair_str}")
        };

        let subscription_symbol = if t_symbol_str.len() <= 16 {
            Some(Symbol::from(t_symbol_str.as_str()))
        } else {
            None
        };

        // All pairs returned in pub:list:pair:exchange are actively listed
        instruments.push(CatalogInstrument::with_subscription_symbol(
            native_id,
            subscription_symbol,
            None,
            None,
            ListingState::Listed,
            None,
        ));
    }

    process_catalog_items(ProviderKind::Bitfinex, instruments, max_items)
}

/// Creates a default HTTP instrument catalog client for Bitfinex Spot.
pub fn bitfinex_catalog_client() -> Result<crate::catalog_client::HttpCatalogClient, CatalogError> {
    crate::catalog_client::HttpCatalogClient::new(
        ProviderKind::Bitfinex,
        DEFAULT_BITFINEX_CATALOG_ENDPOINT,
        parse_bitfinex_catalog,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bitfinex_complete() {
        let json = include_str!("../../tests/fixtures/catalog/bitfinex/complete.json");
        let items = parse_bitfinex_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 4);

        // Deterministic sort: 1INCH:USD, BTCUSD, ETHUSD, SOLUSD
        assert_eq!(items[0].native_id.as_str(), "1INCH:USD");
        assert_eq!(items[0].listing, ListingState::Listed);
        assert_eq!(
            items[0].subscription_symbol.as_ref().unwrap().as_str(),
            "t1INCH:USD"
        );
        assert!(items[0].raw_base.is_none());
        assert!(items[0].raw_quote.is_none());

        assert_eq!(items[1].native_id.as_str(), "BTCUSD");
        assert_eq!(items[1].listing, ListingState::Listed);
        assert_eq!(
            items[1].subscription_symbol.as_ref().unwrap().as_str(),
            "tBTCUSD"
        );

        assert_eq!(items[2].native_id.as_str(), "ETHUSD");
        assert_eq!(items[2].listing, ListingState::Listed);
        assert_eq!(
            items[2].subscription_symbol.as_ref().unwrap().as_str(),
            "tETHUSD"
        );

        assert_eq!(items[3].native_id.as_str(), "SOLUSD");
        assert_eq!(items[3].listing, ListingState::Listed);
        assert_eq!(
            items[3].subscription_symbol.as_ref().unwrap().as_str(),
            "tSOLUSD"
        );
    }

    #[test]
    fn test_parse_bitfinex_empty() {
        let json = include_str!("../../tests/fixtures/catalog/bitfinex/empty.json");
        let items = parse_bitfinex_catalog(json, 100).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_bitfinex_error() {
        let json = include_str!("../../tests/fixtures/catalog/bitfinex/error.json");
        let err = parse_bitfinex_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
    }

    #[test]
    fn test_parse_bitfinex_malformed() {
        let json = include_str!("../../tests/fixtures/catalog/bitfinex/malformed.json");
        let err = parse_bitfinex_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::MalformedItem { .. }));
    }
}
