//! Bitstamp REST quote snapshot decoder.

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

/// Default public Bitstamp REST base origin.
pub const DEFAULT_BITSTAMP_QUOTE_ENDPOINT: &str = "https://www.bitstamp.net";

#[derive(Deserialize)]
struct RawBitstampTicker {
    bid: Option<String>,
    ask: Option<String>,
    timestamp: Option<String>,
    status: Option<String>,
    reason: Option<String>,
    code: Option<String>,
}

/// Constructs the Bitstamp ticker URL for a single requested symbol.
pub fn bitstamp_quote_url(base: &Url, symbols: &[Symbol]) -> Result<Url, RestQuoteError> {
    let sym = symbols
        .first()
        .ok_or_else(|| RestQuoteError::MalformedItem {
            provider: ProviderKind::Bitstamp,
            reason: "empty symbols list".to_string(),
        })?;
    let path = format!("api/v2/ticker/{}/", sym.as_str());
    build_url(ProviderKind::Bitstamp, base, &path)
}

/// Parses Bitstamp ticker JSON into a validated `SymbolQuoteResult`.
pub fn parse_bitstamp_quote(
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

    let raw: RawBitstampTicker =
        serde_json::from_str(payload).map_err(|e| RestQuoteError::InvalidEnvelope {
            provider,
            reason: format!("failed to parse bitstamp ticker json: {e}"),
        })?;

    if raw.status.as_deref() == Some("error") {
        return Ok(vec![SymbolQuoteResult::Unavailable {
            symbol: sym.clone(),
            reason: QuoteUnavailable::ProviderError {
                code: raw.code,
                message: raw
                    .reason
                    .unwrap_or_else(|| "unknown bitstamp error".to_string()),
            },
        }]);
    }

    let bid_str = match raw.bid {
        Some(b) => b,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let ask_str = match raw.ask {
        Some(a) => a,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let bid_price = match Decimal::from_str_exact(&bid_str) {
        Ok(p) => p,
        Err(_) => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let ask_price = match Decimal::from_str_exact(&ask_str) {
        Ok(p) => p,
        Err(_) => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let provider_ts = raw
        .timestamp
        .as_deref()
        .and_then(|s| s.parse::<u64>().ok())
        .map(UnixNanos::from_secs);

    match QuoteSnapshot::try_new(
        QuoteQuality::Firm,
        bid_price,
        None,
        ask_price,
        None,
        provider_ts,
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

/// Creates a default HTTP REST quote snapshot client for Bitstamp.
pub fn bitstamp_rest_quote_client() -> Result<HttpRestQuoteClient, RestQuoteError> {
    HttpRestQuoteClient::new(
        ProviderKind::Bitstamp,
        DEFAULT_BITSTAMP_QUOTE_ENDPOINT,
        1,
        bitstamp_quote_url,
        parse_bitstamp_quote,
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
    fn test_bitstamp_quote_url() {
        let base = Url::parse(DEFAULT_BITSTAMP_QUOTE_ENDPOINT).unwrap();
        let syms = vec![Symbol::from("btcusd")];
        let url = bitstamp_quote_url(&base, &syms).unwrap();
        assert_eq!(
            url.as_str(),
            "https://www.bitstamp.net/api/v2/ticker/btcusd/"
        );
    }

    #[test]
    fn test_parse_bitstamp_valid() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitstamp/valid.json");
        let syms = vec![Symbol::from("btcusd")];
        let res =
            parse_bitstamp_quote(ProviderKind::Bitstamp, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        let item = &res[0];
        assert!(item.is_available());
        let q = item.quote().unwrap();
        assert_eq!(q.bid_price(), d("50000.00"));
        assert_eq!(q.bid_size(), None);
        assert_eq!(q.ask_price(), d("50001.00"));
        assert_eq!(q.ask_size(), None);
        assert_eq!(q.quality(), QuoteQuality::Firm);
        assert_eq!(q.provider_ts(), Some(UnixNanos::from_secs(1700000000)));
    }

    #[test]
    fn test_parse_bitstamp_crossed() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitstamp/crossed.json");
        let syms = vec![Symbol::from("btcusd")];
        let res =
            parse_bitstamp_quote(ProviderKind::Bitstamp, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("btcusd"),
                reason: QuoteUnavailable::CrossedBook,
            }
        );
    }

    #[test]
    fn test_parse_bitstamp_malformed() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitstamp/malformed.json");
        let syms = vec![Symbol::from("btcusd")];
        let res =
            parse_bitstamp_quote(ProviderKind::Bitstamp, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("btcusd"),
                reason: QuoteUnavailable::NonPositivePrice,
            }
        );
    }

    #[test]
    fn test_parse_bitstamp_provider_error() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitstamp/provider_error.json");
        let syms = vec![Symbol::from("btcusd")];
        let res =
            parse_bitstamp_quote(ProviderKind::Bitstamp, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        match &res[0] {
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { code, message },
                ..
            } => {
                assert_eq!(code.as_deref(), Some("API0001"));
                assert!(message.contains("Pair not found"));
            }
            _ => panic!("expected provider error"),
        }
    }
}
