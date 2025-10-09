//! Bitstamp Spot instrument catalog decoder.

use crate::catalog_util::process_catalog_items;
use mdc_core::catalog::{
    CatalogError, CatalogInstrument, ListingState, NativeInstrumentId, ProviderKind, RawCurrency,
    RawStatus,
};
use mdc_core::model::Symbol;
use serde::Deserialize;

/// Default public Bitstamp REST endpoint for markets.
pub const DEFAULT_BITSTAMP_CATALOG_ENDPOINT: &str = "https://www.bitstamp.net/api/v2/markets/";

#[derive(Deserialize)]
struct RawBitstampMarket {
    name: Option<String>,
    market_symbol: Option<String>,
    url_symbol: Option<String>,
    base_currency: Option<String>,
    counter_currency: Option<String>,
    market_type: Option<String>,
    trading: Option<String>,
}

/// Parses Bitstamp Spot markets JSON payload into validated `CatalogInstrument` items.
pub fn parse_bitstamp_catalog(
    payload: &str,
    max_items: u32,
) -> Result<Vec<CatalogInstrument>, CatalogError> {
    // If it deserializes to an error object with status: "error", report error envelope
    if let Ok(err_val) = serde_json::from_str::<serde_json::Value>(payload) {
        if let Some(status) = err_val.get("status").and_then(|s| s.as_str()) {
            if status == "error" {
                let reason = err_val
                    .get("reason")
                    .and_then(|r| r.as_str())
                    .unwrap_or("unknown error");
                return Err(CatalogError::InvalidEnvelope {
                    provider: ProviderKind::Bitstamp,
                    reason: format!("bitstamp error envelope: {reason}"),
                });
            }
        }
    }

    let markets: Vec<RawBitstampMarket> =
        serde_json::from_str(payload).map_err(|e| CatalogError::InvalidEnvelope {
            provider: ProviderKind::Bitstamp,
            reason: format!("failed to parse markets json array: {e}"),
        })?;

    let mut instruments = Vec::with_capacity(markets.len());

    for market in markets {
        // Retain only SPOT markets
        if let Some(ref m_type) = market.market_type {
            if !m_type.eq_ignore_ascii_case("SPOT") {
                continue;
            }
        }

        // Native ID preference: market_symbol -> url_symbol -> name
        let native_id_str = market
            .market_symbol
            .or(market.url_symbol)
            .or_else(|| market.name.clone())
            .ok_or_else(|| CatalogError::MalformedItem {
                provider: ProviderKind::Bitstamp,
                reason: "missing 'market_symbol', 'url_symbol', or 'name'".to_string(),
            })?;

        let native_id =
            NativeInstrumentId::new(&native_id_str).map_err(|e| CatalogError::MalformedItem {
                provider: ProviderKind::Bitstamp,
                reason: format!("invalid native instrument id '{native_id_str}': {e}"),
            })?;

        // Subscription symbol preference: name (e.g. "BTC/USD") if <= 16 bytes, else native_id if <= 16
        let subscription_symbol = market
            .name
            .as_deref()
            .filter(|s| s.len() <= 16)
            .map(Symbol::from)
            .or_else(|| {
                if native_id_str.len() <= 16 {
                    Some(Symbol::from(native_id_str.as_str()))
                } else {
                    None
                }
            });

        // Base currency: explicit base_currency or derive from "BASE/QUOTE"
        let base_str = market.base_currency.or_else(|| {
            market
                .name
                .as_deref()
                .and_then(|n| n.find('/').map(|slash| n[..slash].to_string()))
        });
        let raw_base = base_str.as_deref().and_then(|s| RawCurrency::new(s).ok());

        // Counter currency: explicit counter_currency or derive from "BASE/QUOTE"
        let quote_str = market.counter_currency.or_else(|| {
            market
                .name
                .as_deref()
                .and_then(|n| n.find('/').map(|slash| n[slash + 1..].to_string()))
        });
        let raw_quote = quote_str.as_deref().and_then(|s| RawCurrency::new(s).ok());

        let raw_status = market
            .trading
            .as_deref()
            .and_then(|s| RawStatus::new(s).ok());

        let listing = match market.trading.as_deref() {
            Some("Enabled") => ListingState::Listed,
            Some("Disabled" | "Pause" | "Suspended") => ListingState::Unavailable,
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

    process_catalog_items(ProviderKind::Bitstamp, instruments, max_items)
}

/// Creates a default HTTP instrument catalog client for Bitstamp Spot.
pub fn bitstamp_catalog_client() -> Result<crate::catalog_client::HttpCatalogClient, CatalogError> {
    crate::catalog_client::HttpCatalogClient::new(
        ProviderKind::Bitstamp,
        DEFAULT_BITSTAMP_CATALOG_ENDPOINT,
        parse_bitstamp_catalog,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bitstamp_complete() {
        let json = include_str!("../../tests/fixtures/catalog/bitstamp/complete.json");
        let items = parse_bitstamp_catalog(json, 100).unwrap();
        assert_eq!(items.len(), 3);

        // Deterministic sort: btcusd, ethusd, solusd
        assert_eq!(items[0].native_id.as_str(), "btcusd");
        assert_eq!(items[0].listing, ListingState::Listed);
        assert_eq!(items[0].raw_base.as_ref().unwrap().as_str(), "BTC");
        assert_eq!(items[0].raw_quote.as_ref().unwrap().as_str(), "USD");
        assert_eq!(
            items[0].subscription_symbol.as_ref().unwrap().as_str(),
            "BTC/USD"
        );

        assert_eq!(items[1].native_id.as_str(), "ethusd");
        assert_eq!(items[1].listing, ListingState::Listed);

        assert_eq!(items[2].native_id.as_str(), "solusd");
        assert_eq!(items[2].listing, ListingState::Listed);
    }

    #[test]
    fn test_parse_bitstamp_inactive() {
        let json = include_str!("../../tests/fixtures/catalog/bitstamp/inactive.json");
        let items = parse_bitstamp_catalog(json, 100).unwrap();
        // One inactive spot pair, while the futures pair is filtered out by SPOT scope check
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].native_id.as_str(), "testusd");
        assert_eq!(items[0].listing, ListingState::Unavailable);
    }

    #[test]
    fn test_parse_bitstamp_empty() {
        let json = include_str!("../../tests/fixtures/catalog/bitstamp/empty.json");
        let items = parse_bitstamp_catalog(json, 100).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_parse_bitstamp_error() {
        let json = include_str!("../../tests/fixtures/catalog/bitstamp/error.json");
        let err = parse_bitstamp_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
    }

    #[test]
    fn test_parse_bitstamp_malformed() {
        let json = include_str!("../../tests/fixtures/catalog/bitstamp/malformed.json");
        let err = parse_bitstamp_catalog(json, 100).unwrap_err();
        assert!(matches!(err, CatalogError::MalformedItem { .. }));
    }
}
