//! Huobi/HTX Spot instrument catalog decoder.

use crate::catalog_util::process_catalog_items;
use mdc_core::catalog::{
    CatalogError, CatalogInstrument, ListingState, NativeInstrumentId, ProviderKind, RawCurrency,
    RawStatus,
};
use serde::Deserialize;

/// Default public Huobi/HTX REST endpoint for common symbols.
pub const DEFAULT_HUOBI_CATALOG_ENDPOINT: &str = "https://api.huobi.pro/v1/common/symbols";

#[derive(Deserialize)]
struct RawHuobiResponse {
    status: Option<String>,
    #[serde(rename = "err-code")]
    err_code: Option<String>,
    #[serde(rename = "err-msg")]
    err_msg: Option<String>,
    data: Option<Vec<RawHuobiSymbol>>,
}

#[derive(Deserialize)]
struct RawHuobiSymbol {
    symbol: Option<String>,
    #[serde(rename = "base-currency")]
    base_currency: Option<String>,
    #[serde(rename = "quote-currency")]
    quote_currency: Option<String>,
    state: Option<String>,
}

/// Parses Huobi/HTX Spot symbols JSON payload into validated `CatalogInstrument` items.
pub fn parse_huobi_catalog(
    payload: &str,
    max_items: u32,
) -> Result<Vec<CatalogInstrument>, CatalogError> {
    let root: RawHuobiResponse =
        serde_json::from_str(payload).map_err(|e| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Huobi,
            reason: format!("failed to parse symbols json: {e}"),
        })?;

    if root.status.as_deref() != Some("ok") {
        let err_code = root.err_code.unwrap_or_else(|| "unknown".to_string());
        let err_msg = root.err_msg.unwrap_or_default();
        return Err(CatalogError::InvalidEnvelope {
            provider: ProviderKind::Huobi,
            reason: format!("huobi error envelope: code {err_code}, msg: {err_msg}"),
        });
    }

    let data = root.data.ok_or_else(|| CatalogError::InvalidEnvelope {
        provider: ProviderKind::Huobi,
        reason: "missing 'data' array in response".to_string(),
    })?;

    let mut instruments = Vec::with_capacity(data.len());

    for item in data {
        let symbol_str = item.symbol.ok_or_else(|| CatalogError::MalformedItem {
            provider: ProviderKind::Huobi,
            reason: "missing 'symbol' property".to_string(),
        })?;

        let native_id =
            NativeInstrumentId::new(&symbol_str).map_err(|e| CatalogError::MalformedItem {
                provider: ProviderKind::Huobi,
                reason: format!("invalid native instrument id '{symbol_str}': {e}"),
            })?;

        let raw_base = item
            .base_currency
            .as_deref()
            .and_then(|s| RawCurrency::new(s).ok());
        let raw_quote = item
            .quote_currency
            .as_deref()
            .and_then(|s| RawCurrency::new(s).ok());

        let raw_status = item.state.as_deref().and_then(|s| RawStatus::new(s).ok());

        let listing = match item.state.as_deref() {
            Some("online") => ListingState::Listed,
            Some("offline" | "suspend" | "pre-online") => ListingState::Unavailable,
            _ => ListingState::Unknown,
        };

        instruments.push(CatalogInstrument::new(
            native_id, raw_base, raw_quote, listing, raw_status,
        ));
    }

    process_catalog_items(ProviderKind::Huobi, instruments, max_items)
}

/// Creates a default HTTP instrument catalog client for Huobi Spot.
pub fn huobi_catalog_client() -> Result<crate::catalog_client::HttpCatalogClient, CatalogError> {
    crate::catalog_client::HttpCatalogClient::new(
        ProviderKind::Huobi,
        DEFAULT_HUOBI_CATALOG_ENDPOINT,
        parse_huobi_catalog,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_huobi_complete() {
        let json = include_str!("../../tests/fixtures/catalog/huobi/complete.json");
        let items = parse_huobi_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 3);

        // Deterministic sort: btcusdt, ethusdt, solusdt
        assert_eq!(items[0].native_id.as_str(), "btcusdt");
        assert_eq!(items[0].listing, ListingState::Listed);
        assert_eq!(items[0].raw_base.as_ref().unwrap().as_str(), "btc");
        assert_eq!(items[0].raw_quote.as_ref().unwrap().as_str(), "usdt");
        assert_eq!(
            items[0].subscription_symbol.as_ref().unwrap().as_str(),
            "btcusdt"
        );

        assert_eq!(items[1].native_id.as_str(), "ethusdt");
        assert_eq!(items[1].listing, ListingState::Listed);

        assert_eq!(items[2].native_id.as_str(), "solusdt");
        assert_eq!(items[2].listing, ListingState::Listed);
    }

    #[test]
    fn test_parse_huobi_inactive() {
        let json = include_str!("../../tests/fixtures/catalog/huobi/inactive.json");
        let items = parse_huobi_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].native_id.as_str(), "suspendedpair");
        assert_eq!(items[0].listing, ListingState::Unavailable);
        assert_eq!(items[1].native_id.as_str(), "wnxmbtc");
        assert_eq!(items[1].listing, ListingState::Unavailable);
    }

    #[test]
    fn test_parse_huobi_empty() {
        let json = include_str!("../../tests/fixtures/catalog/huobi/empty.json");
        let items = parse_huobi_catalog(json, 100).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_huobi_error() {
        let json = include_str!("../../tests/fixtures/catalog/huobi/error.json");
        let err = parse_huobi_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
    }

    #[test]
    fn test_parse_huobi_malformed() {
        let json = include_str!("../../tests/fixtures/catalog/huobi/malformed.json");
        let err = parse_huobi_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::MalformedItem { .. }));
    }
}
