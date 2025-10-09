//! Kraken REST quote snapshot decoder.

use std::collections::BTreeMap;

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

/// Default public Kraken REST base origin.
pub const DEFAULT_KRAKEN_QUOTE_ENDPOINT: &str = "https://api.kraken.com";

#[derive(Deserialize)]
struct RawKrakenResponse {
    error: Option<Vec<String>>,
    result: Option<BTreeMap<String, RawKrakenTicker>>,
}

#[derive(Deserialize)]
struct RawKrakenTicker {
    a: Option<Vec<String>>,
    b: Option<Vec<String>>,
}

/// Constructs the Kraken public Ticker URL for a single requested symbol.
pub fn kraken_quote_url(base: &Url, symbols: &[Symbol]) -> Result<Url, RestQuoteError> {
    let sym = symbols
        .first()
        .ok_or_else(|| RestQuoteError::MalformedItem {
            provider: ProviderKind::Kraken,
            reason: "empty symbols list".to_string(),
        })?;
    let path = format!("0/public/Ticker?pair={}", sym.as_str());
    build_url(ProviderKind::Kraken, base, &path)
}

/// Parses Kraken public Ticker JSON into a validated `SymbolQuoteResult`.
pub fn parse_kraken_quote(
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

    let raw: RawKrakenResponse =
        serde_json::from_str(payload).map_err(|e| RestQuoteError::InvalidEnvelope {
            provider,
            reason: format!("failed to parse kraken ticker json: {e}"),
        })?;

    if let Some(errs) = raw.error {
        if !errs.is_empty() {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::ProviderError {
                    code: None,
                    message: errs.join("; "),
                },
            }]);
        }
    }

    let result = match raw.result {
        Some(r) if !r.is_empty() => r,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::MissingSymbol,
            }]);
        }
    };

    // Match exact symbol or case-insensitively, or take the only result pair if size is 1
    let ticker = result
        .get(sym.as_str())
        .or_else(|| {
            result
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(sym.as_str()))
                .map(|(_, v)| v)
        })
        .or_else(|| {
            if result.len() == 1 {
                result.values().next()
            } else {
                None
            }
        });

    let ticker = match ticker {
        Some(t) => t,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::MissingSymbol,
            }]);
        }
    };

    let b = match &ticker.b {
        Some(vec) if !vec.is_empty() => vec,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let a = match &ticker.a {
        Some(vec) if !vec.is_empty() => vec,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let bid_price = match Decimal::from_str_exact(&b[0]) {
        Ok(p) => p,
        Err(_) => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let ask_price = match Decimal::from_str_exact(&a[0]) {
        Ok(p) => p,
        Err(_) => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let bid_size = b.get(2).and_then(|s| Decimal::from_str_exact(s).ok());
    let ask_size = a.get(2).and_then(|s| Decimal::from_str_exact(s).ok());

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

/// Creates a default HTTP REST quote snapshot client for Kraken.
pub fn kraken_rest_quote_client() -> Result<HttpRestQuoteClient, RestQuoteError> {
    HttpRestQuoteClient::new(
        ProviderKind::Kraken,
        DEFAULT_KRAKEN_QUOTE_ENDPOINT,
        1,
        kraken_quote_url,
        parse_kraken_quote,
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
    fn test_kraken_quote_url() {
        let base = Url::parse(DEFAULT_KRAKEN_QUOTE_ENDPOINT).unwrap();
        let syms = vec![Symbol::from("XXBTZUSD")];
        let url = kraken_quote_url(&base, &syms).unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD"
        );
    }

    #[test]
    fn test_parse_kraken_valid() {
        let json = include_str!("../../tests/fixtures/rest_quote/kraken/valid.json");
        let syms = vec![Symbol::from("XXBTZUSD")];
        let res = parse_kraken_quote(ProviderKind::Kraken, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        let item = &res[0];
        assert!(item.is_available());
        let q = item.quote().unwrap();
        assert_eq!(q.bid_price, d("50000.00000"));
        assert_eq!(q.bid_size, Some(d("1.500")));
        assert_eq!(q.ask_price, d("50001.00000"));
        assert_eq!(q.ask_size, Some(d("2.000")));
        assert_eq!(q.quality, QuoteQuality::Firm);
    }

    #[test]
    fn test_parse_kraken_crossed() {
        let json = include_str!("../../tests/fixtures/rest_quote/kraken/crossed.json");
        let syms = vec![Symbol::from("XXBTZUSD")];
        let res = parse_kraken_quote(ProviderKind::Kraken, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("XXBTZUSD"),
                reason: QuoteUnavailable::CrossedBook,
            }
        );
    }

    #[test]
    fn test_parse_kraken_empty() {
        let json = include_str!("../../tests/fixtures/rest_quote/kraken/empty.json");
        let syms = vec![Symbol::from("XXBTZUSD")];
        let res = parse_kraken_quote(ProviderKind::Kraken, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("XXBTZUSD"),
                reason: QuoteUnavailable::MissingSymbol,
            }
        );
    }

    #[test]
    fn test_parse_kraken_malformed() {
        let json = include_str!("../../tests/fixtures/rest_quote/kraken/malformed.json");
        let syms = vec![Symbol::from("XXBTZUSD")];
        let res = parse_kraken_quote(ProviderKind::Kraken, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("XXBTZUSD"),
                reason: QuoteUnavailable::EmptyBook,
            }
        );
    }

    #[test]
    fn test_parse_kraken_provider_error() {
        let json = include_str!("../../tests/fixtures/rest_quote/kraken/provider_error.json");
        let syms = vec![Symbol::from("XXBTZUSD")];
        let res = parse_kraken_quote(ProviderKind::Kraken, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        match &res[0] {
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { message, .. },
                ..
            } => {
                assert!(message.contains("Unknown asset pair"));
            }
            _ => panic!("expected provider error"),
        }
    }
}
