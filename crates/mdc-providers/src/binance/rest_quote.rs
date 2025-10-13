//! Binance Spot REST quote snapshot decoder.

use mdc_core::model::VenueId;
use mdc_core::model::{Symbol, UnixNanos};
use mdc_core::rest_quote::{
    QuoteQuality, QuoteSnapshot, QuoteUnavailable, RestQuoteError, SymbolQuoteResult,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use url::Url;

use crate::rest_quote_client::HttpRestQuoteClient;
use crate::rest_quote_util::build_url;

/// Default public Binance REST base origin.
pub const DEFAULT_BINANCE_QUOTE_ENDPOINT: &str = "https://api.binance.com";

#[derive(Deserialize)]
struct RawBinanceBookTicker {
    symbol: Option<String>,
    #[serde(rename = "bidPrice")]
    bid_price: Option<String>,
    #[serde(rename = "bidQty")]
    bid_qty: Option<String>,
    #[serde(rename = "askPrice")]
    ask_price: Option<String>,
    #[serde(rename = "askQty")]
    ask_qty: Option<String>,
    code: Option<i64>,
    msg: Option<String>,
}

/// Constructs the Binance public bookTicker URL for a single requested symbol.
pub fn binance_quote_url(base: &Url, symbols: &[Symbol]) -> Result<Url, RestQuoteError> {
    let sym = symbols
        .first()
        .ok_or_else(|| RestQuoteError::MalformedItem {
            provider: VenueId::Binance,
            reason: "empty symbols list".to_string(),
        })?;
    let path = format!("api/v3/ticker/bookTicker?symbol={}", sym.as_str());
    build_url(VenueId::Binance, base, &path)
}

/// Parses Binance Spot bookTicker JSON into a validated `SymbolQuoteResult`.
pub fn parse_binance_quote(
    provider: VenueId,
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

    let raw: RawBinanceBookTicker =
        serde_json::from_str(payload).map_err(|e| RestQuoteError::InvalidEnvelope {
            provider,
            reason: format!("failed to parse bookTicker json: {e}"),
        })?;

    if let Some(code) = raw.code {
        let msg = raw.msg.unwrap_or_default();
        return Ok(vec![SymbolQuoteResult::Unavailable {
            symbol: sym.clone(),
            reason: QuoteUnavailable::ProviderError {
                code: Some(code.to_string()),
                message: msg,
            },
        }]);
    }

    let returned_sym = match raw.symbol {
        Some(s) => s,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::MissingSymbol,
            }]);
        }
    };

    if !returned_sym.eq_ignore_ascii_case(sym.as_str()) {
        return Ok(vec![SymbolQuoteResult::Unavailable {
            symbol: sym.clone(),
            reason: QuoteUnavailable::MissingSymbol,
        }]);
    }

    let bid_price_str = match raw.bid_price {
        Some(p) => p,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let ask_price_str = match raw.ask_price {
        Some(p) => p,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let bid_price = match Decimal::from_str_exact(&bid_price_str) {
        Ok(p) => p,
        Err(_) => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let ask_price = match Decimal::from_str_exact(&ask_price_str) {
        Ok(p) => p,
        Err(_) => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let bid_qty = raw
        .bid_qty
        .as_deref()
        .and_then(|s| Decimal::from_str_exact(s).ok());
    let ask_qty = raw
        .ask_qty
        .as_deref()
        .and_then(|s| Decimal::from_str_exact(s).ok());

    match QuoteSnapshot::try_new(
        QuoteQuality::Firm,
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
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

/// Creates a default HTTP REST quote snapshot client for Binance Spot.
pub fn binance_rest_quote_client() -> Result<HttpRestQuoteClient, RestQuoteError> {
    HttpRestQuoteClient::new(
        VenueId::Binance,
        DEFAULT_BINANCE_QUOTE_ENDPOINT,
        1,
        binance_quote_url,
        parse_binance_quote,
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
    fn test_binance_quote_url() {
        let base = Url::parse(DEFAULT_BINANCE_QUOTE_ENDPOINT).unwrap();
        let syms = vec![Symbol::from("BTCUSDT")];
        let url = binance_quote_url(&base, &syms).unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT"
        );
    }

    #[test]
    fn test_parse_binance_valid() {
        let json = include_str!("../../tests/fixtures/rest_quote/binance/valid.json");
        let syms = vec![Symbol::from("BTCUSDT")];
        let res = parse_binance_quote(VenueId::Binance, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        let item = &res[0];
        assert!(item.is_available());
        let q = item.quote().unwrap();
        assert_eq!(q.bid_price(), d("50000.00"));
        assert_eq!(q.bid_size(), Some(d("1.50")));
        assert_eq!(q.ask_price(), d("50001.00"));
        assert_eq!(q.ask_size(), Some(d("2.00")));
        assert_eq!(q.quality(), QuoteQuality::Firm);
    }

    #[test]
    fn test_parse_binance_crossed() {
        let json = include_str!("../../tests/fixtures/rest_quote/binance/crossed.json");
        let syms = vec![Symbol::from("BTCUSDT")];
        let res = parse_binance_quote(VenueId::Binance, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].is_available());
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTCUSDT"),
                reason: QuoteUnavailable::CrossedBook,
            }
        );
    }

    #[test]
    fn test_parse_binance_missing() {
        let json = include_str!("../../tests/fixtures/rest_quote/binance/missing.json");
        let syms = vec![Symbol::from("BTCUSDT")];
        let res = parse_binance_quote(VenueId::Binance, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].is_available());
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTCUSDT"),
                reason: QuoteUnavailable::MissingSymbol,
            }
        );
    }

    #[test]
    fn test_parse_binance_malformed() {
        let json = include_str!("../../tests/fixtures/rest_quote/binance/malformed.json");
        let syms = vec![Symbol::from("BTCUSDT")];
        let res = parse_binance_quote(VenueId::Binance, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].is_available());
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTCUSDT"),
                reason: QuoteUnavailable::NonPositivePrice,
            }
        );
    }

    #[test]
    fn test_parse_binance_provider_error() {
        let json = include_str!("../../tests/fixtures/rest_quote/binance/provider_error.json");
        let syms = vec![Symbol::from("BTCUSDT")];
        let res = parse_binance_quote(VenueId::Binance, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert!(!res[0].is_available());
        match &res[0] {
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { code, message },
                ..
            } => {
                assert_eq!(code.as_deref(), Some("-1121"));
                assert!(message.contains("Invalid symbol"));
            }
            _ => panic!("expected provider error"),
        }
    }
}
