//! Gate.io Spot instrument catalog decoder.

use crate::catalog_util::process_catalog_items;
use mdc_core::catalog::{
    CatalogError, CatalogInstrument, ListingState, NativeInstrumentId, ProviderKind, RawCurrency,
    RawStatus,
};
use serde::Deserialize;

/// Default public Gate.io REST endpoint for spot currency pairs.
pub const DEFAULT_GATEIO_CATALOG_ENDPOINT: &str =
    "https://api.gateio.ws/api/v4/spot/currency_pairs";

#[derive(Deserialize)]
struct RawGatePair {
    id: Option<String>,
    base: Option<String>,
    quote: Option<String>,
    trade_status: Option<String>,
}

/// Parses Gate.io Spot currency pairs JSON payload into validated `CatalogInstrument` items.
pub fn parse_gateio_catalog(
    payload: &str,
    max_items: u32,
) -> Result<Vec<CatalogInstrument>, CatalogError> {
    // If it deserializes to an error object with "label" or "message", report error envelope
    if let Ok(err_val) = serde_json::from_str::<serde_json::Value>(payload) {
        if let Some(msg) = err_val
            .get("message")
            .or_else(|| err_val.get("label"))
            .and_then(|m| m.as_str())
        {
            return Err(CatalogError::InvalidEnvelope {
                provider: ProviderKind::Gateio,
                reason: format!("gate.io error envelope: {msg}"),
            });
        }
    }

    let pairs: Vec<RawGatePair> =
        serde_json::from_str(payload).map_err(|e| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Gateio,
            reason: format!("failed to parse currency_pairs json array: {e}"),
        })?;

    let mut instruments = Vec::with_capacity(pairs.len());

    for pair in pairs {
        let id_str = pair.id.ok_or_else(|| CatalogError::MalformedItem {
            provider: ProviderKind::Gateio,
            reason: "missing 'id' property".to_string(),
        })?;

        let native_id =
            NativeInstrumentId::new(&id_str).map_err(|e| CatalogError::MalformedItem {
                provider: ProviderKind::Gateio,
                reason: format!("invalid native instrument id '{id_str}': {e}"),
            })?;

        let raw_base = pair.base.as_deref().and_then(|s| RawCurrency::new(s).ok());
        let raw_quote = pair.quote.as_deref().and_then(|s| RawCurrency::new(s).ok());

        let raw_status = pair
            .trade_status
            .as_deref()
            .and_then(|s| RawStatus::new(s).ok());

        let listing = match pair.trade_status.as_deref() {
            Some("tradable") => ListingState::Listed,
            Some("untradable" | "buyable" | "sellable" | "suspended") => ListingState::Unavailable,
            _ => ListingState::Unknown,
        };

        instruments.push(CatalogInstrument::new(
            native_id, raw_base, raw_quote, listing, raw_status,
        ));
    }

    process_catalog_items(ProviderKind::Gateio, instruments, max_items)
}

/// Creates a default HTTP instrument catalog client for Gate.io Spot.
pub fn gateio_catalog_client() -> Result<crate::catalog_client::HttpCatalogClient, CatalogError> {
    crate::catalog_client::HttpCatalogClient::new(
        ProviderKind::Gateio,
        DEFAULT_GATEIO_CATALOG_ENDPOINT,
        parse_gateio_catalog,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_gateio_complete() {
        let json = include_str!("../../tests/fixtures/catalog/gateio/complete.json");
        let items = parse_gateio_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 3);

        // Deterministic sort: BTC_USDT, ETH_USDT, SOL_USDT
        assert_eq!(items[0].native_id.as_str(), "BTC_USDT");
        assert_eq!(items[0].listing, ListingState::Listed);
        assert_eq!(items[0].raw_base.as_ref().unwrap().as_str(), "BTC");
        assert_eq!(items[0].raw_quote.as_ref().unwrap().as_str(), "USDT");
        assert_eq!(
            items[0].subscription_symbol.as_ref().unwrap().as_str(),
            "BTC_USDT"
        );

        assert_eq!(items[1].native_id.as_str(), "ETH_USDT");
        assert_eq!(items[1].listing, ListingState::Listed);

        assert_eq!(items[2].native_id.as_str(), "SOL_USDT");
        assert_eq!(items[2].listing, ListingState::Listed);
    }

    #[test]
    fn test_parse_gateio_inactive() {
        let json = include_str!("../../tests/fixtures/catalog/gateio/inactive.json");
        let items = parse_gateio_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].native_id.as_str(), "SUSPENDED_USDT");
        assert_eq!(items[0].listing, ListingState::Unavailable);
        assert_eq!(items[1].native_id.as_str(), "XAR_USDT");
        assert_eq!(items[1].listing, ListingState::Unavailable);
    }

    #[test]
    fn test_parse_gateio_empty() {
        let json = include_str!("../../tests/fixtures/catalog/gateio/empty.json");
        let items = parse_gateio_catalog(json, 100).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_gateio_error() {
        let json = include_str!("../../tests/fixtures/catalog/gateio/error.json");
        let err = parse_gateio_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
    }

    #[test]
    fn test_parse_gateio_malformed() {
        let json = include_str!("../../tests/fixtures/catalog/gateio/malformed.json");
        let err = parse_gateio_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::MalformedItem { .. }));
    }
}
