//! Gate.io REST quote snapshot decoder.

use mdc_core::catalog::ProviderKind;
use mdc_core::model::{Symbol, UnixNanos};
use mdc_core::rest_quote::{
    QuoteQuality, QuoteSnapshot, QuoteUnavailable, RestQuoteError, SymbolQuoteResult,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use url::Url;

use crate::rest_quote_client::HttpRestQuoteClient;
use crate::rest_quote_util::build_url;

/// Default public Gate.io REST base origin.
pub const DEFAULT_GATEIO_QUOTE_ENDPOINT: &str = "https://api.gateio.ws";

#[derive(Deserialize)]
struct RawGateTicker {
    currency_pair: Option<String>,
    highest_bid: Option<String>,
    lowest_ask: Option<String>,
    highest_size: Option<String>,
    lowest_size: Option<String>,
}

#[derive(Deserialize)]
struct RawGateError {
    label: Option<String>,
    message: Option<String>,
}

/// Constructs the Gate.io spot tickers URL for a single requested symbol.
pub fn gateio_quote_url(base: &Url, symbols: &[Symbol]) -> Result<Url, RestQuoteError> {
    let sym = symbols
        .first()
        .ok_or_else(|| RestQuoteError::MalformedItem {
            provider: ProviderKind::Gateio,
            reason: "empty symbols list".to_string(),
        })?;
    let path = format!("api/v4/spot/tickers?currency_pair={}", sym.as_str());
    build_url(ProviderKind::Gateio, base, &path)
}

/// Parses Gate.io spot tickers JSON into a validated `SymbolQuoteResult`.
pub fn parse_gateio_quote(
    provider: ProviderKind,
    symbols: &[Symbol],
    payload: &str,
    received_ts: UnixNanos,
) -> Result<Vec<SymbolQuoteResult>, RestQuoteError> {
    let sym = symbols
        .first()
        .ok_or_else(|| RestQuoteError::MalformedItem {
            provider,
            reason: "empty symbols list".to_string(),
        })?;

    // Check if error object
    if let Ok(err) = serde_json::from_str::<RawGateError>(payload) {
        if let Some(label) = err.label {
            let msg = err.message.unwrap_or_default();
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::ProviderError {
                    code: Some(label),
                    message: msg,
                },
            }]);
        }
    }

    let items: Vec<RawGateTicker> =
        serde_json::from_str(payload).map_err(|e| RestQuoteError::InvalidEnvelope {
            provider,
            reason: format!("failed to parse gateio spot tickers json: {e}"),
        })?;

    let matching = items.iter().find(|item| {
        item.currency_pair
            .as_deref()
            .map(|pair| pair.eq_ignore_ascii_case(sym.as_str()))
            .unwrap_or(false)
    });

    let item = match matching {
        Some(it) => it,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::MissingSymbol,
            }]);
        }
    };

    let bid_str = match &item.highest_bid {
        Some(b) if !b.is_empty() => b,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let ask_str = match &item.lowest_ask {
        Some(a) if !a.is_empty() => a,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let bid_price = match Decimal::from_str_exact(bid_str) {
        Ok(p) => p,
        Err(_) => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let ask_price = match Decimal::from_str_exact(ask_str) {
        Ok(p) => p,
        Err(_) => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let bid_size = item
        .highest_size
        .as_deref()
        .and_then(|s| Decimal::from_str_exact(s).ok());
    let ask_size = item
        .lowest_size
        .as_deref()
        .and_then(|s| Decimal::from_str_exact(s).ok());

    match QuoteSnapshot::try_new(
        QuoteQuality::Firm,
        bid_price,
        bid_size,
        ask_price,
        ask_size,
        None,
        None,
        received_ts,
    ) {
        Ok(quote) => Ok(vec![SymbolQuoteResult::Available {
            symbol: sym.clone(),
            quote,
        }]),
        Err(reason) => Ok(vec![SymbolQuoteResult::Unavailable {
            symbol: sym.clone(),
            reason,
        }]),
    }
}

/// Creates a default HTTP REST quote snapshot client for Gate.io.
pub fn gateio_rest_quote_client() -> Result<HttpRestQuoteClient, RestQuoteError> {
    HttpRestQuoteClient::new(
        ProviderKind::Gateio,
        DEFAULT_GATEIO_QUOTE_ENDPOINT,
        1,
        gateio_quote_url,
        parse_gateio_quote,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        Decimal::from_str_exact(s).unwrap()
    }

    #[test]
    fn test_gateio_quote_url() {
        let base = Url::parse(DEFAULT_GATEIO_QUOTE_ENDPOINT).unwrap();
        let syms = vec![Symbol::from("BTC_USDT")];
        let url = gateio_quote_url(&base, &syms).unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.gateio.ws/api/v4/spot/tickers?currency_pair=BTC_USDT"
        );
    }

    #[test]
    fn test_parse_gateio_valid() {
        let json = include_str!("../../tests/fixtures/rest_quote/gateio/valid.json");
        let syms = vec![Symbol::from("BTC_USDT")];
        let res = parse_gateio_quote(ProviderKind::Gateio, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        let item = &res[0];
        assert!(item.is_available());
        let q = item.quote().unwrap();
        assert_eq!(q.bid_price, d("50000.0"));
        assert_eq!(q.bid_size, Some(d("1.5")));
        assert_eq!(q.ask_price, d("50001.0"));
        assert_eq!(q.ask_size, Some(d("2.0")));
        assert_eq!(q.quality, QuoteQuality::Firm);
    }

    #[test]
    fn test_parse_gateio_crossed() {
        let json = include_str!("../../tests/fixtures/rest_quote/gateio/crossed.json");
        let syms = vec![Symbol::from("BTC_USDT")];
        let res = parse_gateio_quote(ProviderKind::Gateio, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC_USDT"),
                reason: QuoteUnavailable::CrossedBook,
            }
        );
    }

    #[test]
    fn test_parse_gateio_missing() {
        let json = include_str!("../../tests/fixtures/rest_quote/gateio/missing.json");
        let syms = vec![Symbol::from("BTC_USDT")];
        let res = parse_gateio_quote(ProviderKind::Gateio, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC_USDT"),
                reason: QuoteUnavailable::MissingSymbol,
            }
        );
    }

    #[test]
    fn test_parse_gateio_malformed() {
        let json = include_str!("../../tests/fixtures/rest_quote/gateio/malformed.json");
        let syms = vec![Symbol::from("BTC_USDT")];
        let res = parse_gateio_quote(ProviderKind::Gateio, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC_USDT"),
                reason: QuoteUnavailable::NonPositivePrice,
            }
        );
    }

    #[test]
    fn test_parse_gateio_provider_error() {
        let json = include_str!("../../tests/fixtures/rest_quote/gateio/provider_error.json");
        let syms = vec![Symbol::from("BTC_USDT")];
        let res = parse_gateio_quote(ProviderKind::Gateio, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        match &res[0] {
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { code, message },
                ..
            } => {
                assert_eq!(code.as_deref(), Some("INVALID_CURRENCY_PAIR"));
                assert!(message.contains("Currency pair not found"));
            }
            _ => panic!("expected provider error"),
        }
    }
}
