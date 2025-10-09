//! Binance Spot instrument catalog decoder.

use crate::catalog_util::process_catalog_items;
use mdc_core::catalog::{
    CatalogError, CatalogInstrument, ListingState, NativeInstrumentId, ProviderKind, RawCurrency,
    RawStatus,
};
use serde::Deserialize;

/// Default public Binance REST endpoint for exchange information.
pub const DEFAULT_BINANCE_CATALOG_ENDPOINT: &str = "https://api.binance.com/api/v3/exchangeInfo";

#[derive(Deserialize)]
struct RawExchangeInfo {
    symbols: Option<Vec<RawSymbolInfo>>,
    code: Option<i64>,
    msg: Option<String>,
}

#[derive(Deserialize)]
struct RawSymbolInfo {
    symbol: Option<String>,
    status: Option<String>,
    #[serde(rename = "baseAsset")]
    base_asset: Option<String>,
    #[serde(rename = "quoteAsset")]
    quote_asset: Option<String>,
}

/// Parses Binance Spot exchange information JSON payload into validated `CatalogInstrument` items.
pub fn parse_binance_catalog(
    payload: &str,
    max_items: u32,
) -> Result<Vec<CatalogInstrument>, CatalogError> {
    let root: RawExchangeInfo =
        serde_json::from_str(payload).map_err(|e| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Binance,
            reason: format!("failed to parse exchangeInfo json: {e}"),
        })?;

    if let Some(code) = root.code {
        let msg = root.msg.unwrap_or_default();
        return Err(CatalogError::InvalidEnvelope {
            provider: ProviderKind::Binance,
            reason: format!("binance error envelope: code {code}, msg: {msg}"),
        });
    }

    let symbols = root.symbols.ok_or_else(|| CatalogError::InvalidEnvelope {
        provider: ProviderKind::Binance,
        reason: "missing 'symbols' array in response".to_string(),
    })?;

    let mut instruments = Vec::with_capacity(symbols.len());

    for item in symbols {
        let symbol_str = item.symbol.ok_or_else(|| CatalogError::MalformedItem {
            provider: ProviderKind::Binance,
            reason: "missing 'symbol' property".to_string(),
        })?;

        let native_id =
            NativeInstrumentId::new(&symbol_str).map_err(|e| CatalogError::MalformedItem {
                provider: ProviderKind::Binance,
                reason: format!("invalid native instrument id '{symbol_str}': {e}"),
            })?;

        let raw_base = item
            .base_asset
            .as_deref()
            .and_then(|s| RawCurrency::new(s).ok());
        let raw_quote = item
            .quote_asset
            .as_deref()
            .and_then(|s| RawCurrency::new(s).ok());

        let raw_status = item.status.as_deref().and_then(|s| RawStatus::new(s).ok());

        let listing = match item.status.as_deref() {
            Some("TRADING") => ListingState::Listed,
            Some(
                "BREAK" | "HALT" | "AUCTION_MATCH" | "CLOSE" | "END_OF_DAY" | "SETTLING"
                | "POST_TRADING" | "PRE_TRADING",
            ) => ListingState::Unavailable,
            _ => ListingState::Unknown,
        };

        instruments.push(CatalogInstrument::new(
            native_id, raw_base, raw_quote, listing, raw_status,
        ));
    }

    process_catalog_items(ProviderKind::Binance, instruments, max_items)
}

/// Creates a default HTTP instrument catalog client for Binance Spot.
pub fn binance_catalog_client() -> Result<crate::catalog_client::HttpCatalogClient, CatalogError> {
    crate::catalog_client::HttpCatalogClient::new(
        ProviderKind::Binance,
        DEFAULT_BINANCE_CATALOG_ENDPOINT,
        parse_binance_catalog,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_binance_complete() {
        let json = include_str!("../../tests/fixtures/catalog/binance/complete.json");
        let items = parse_binance_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 3);

        // Deterministic sort: BTCUSDT, ETHBTC, SOLUSDT
        assert_eq!(items[0].native_id.as_str(), "BTCUSDT");
        assert_eq!(items[0].listing, ListingState::Listed);
        assert_eq!(items[0].raw_base.as_ref().unwrap().as_str(), "BTC");
        assert_eq!(items[0].raw_quote.as_ref().unwrap().as_str(), "USDT");
        assert_eq!(
            items[0].subscription_symbol.as_ref().unwrap().as_str(),
            "BTCUSDT"
        );

        assert_eq!(items[1].native_id.as_str(), "ETHBTC");
        assert_eq!(items[1].listing, ListingState::Listed);

        assert_eq!(items[2].native_id.as_str(), "SOLUSDT");
        assert_eq!(items[2].listing, ListingState::Listed);
    }

    #[test]
    fn test_parse_binance_inactive() {
        let json = include_str!("../../tests/fixtures/catalog/binance/inactive.json");
        let items = parse_binance_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].native_id.as_str(), "BCHSVUSDT");
        assert_eq!(items[0].listing, ListingState::Unavailable);
        assert_eq!(items[1].native_id.as_str(), "QTUMETH");
        assert_eq!(items[1].listing, ListingState::Unavailable);
    }

    #[test]
    fn test_parse_binance_empty() {
        let json = include_str!("../../tests/fixtures/catalog/binance/empty.json");
        let items = parse_binance_catalog(json, 100).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_binance_error() {
        let json = include_str!("../../tests/fixtures/catalog/binance/error.json");
        let err = parse_binance_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
    }

    #[test]
    fn test_parse_binance_malformed() {
        let json = include_str!("../../tests/fixtures/catalog/binance/malformed.json");
        let err = parse_binance_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::MalformedItem { .. }));
    }
}
