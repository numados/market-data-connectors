//! Coinbase Exchange REST quote snapshot decoder.

use mdc_core::model::VenueId;
use mdc_core::model::{Symbol, UnixNanos};
use mdc_core::rest_quote::{
    QuoteQuality, QuoteSnapshot, QuoteUnavailable, RestQuoteError, SymbolQuoteResult,
};
use serde::Deserialize;
use url::Url;

use crate::rest_quote_client::HttpRestQuoteClient;
use crate::rest_quote_util::{build_url, json_value_to_decimal};

/// Default public Coinbase Exchange REST base origin.
pub const DEFAULT_COINBASE_QUOTE_ENDPOINT: &str = "https://api.exchange.coinbase.com";

#[derive(Deserialize)]
struct RawCoinbaseBookLevel1 {
    sequence: Option<u64>,
    bids: Option<Vec<Vec<serde_json::Value>>>,
    asks: Option<Vec<Vec<serde_json::Value>>>,
    time: Option<String>,
    auction_mode: Option<bool>,
    message: Option<String>,
}

/// Constructs the Coinbase level 1 book URL for a single requested symbol.
pub fn coinbase_quote_url(base: &Url, symbols: &[Symbol]) -> Result<Url, RestQuoteError> {
    let sym = symbols
        .first()
        .ok_or_else(|| RestQuoteError::MalformedItem {
            provider: VenueId::Coinbase,
            reason: "empty symbols list".to_string(),
        })?;
    let path = format!("products/{}/book?level=1", sym.as_str());
    build_url(VenueId::Coinbase, base, &path)
}

/// Parses Coinbase Exchange level 1 book JSON into a validated `SymbolQuoteResult`.
pub fn parse_coinbase_quote(
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

    let raw: RawCoinbaseBookLevel1 =
        serde_json::from_str(payload).map_err(|e| RestQuoteError::InvalidEnvelope {
            provider,
            reason: format!("failed to parse coinbase level 1 book json: {e}"),
        })?;

    if let Some(msg) = raw.message {
        return Ok(vec![SymbolQuoteResult::Unavailable {
            symbol: sym.clone(),
            reason: QuoteUnavailable::ProviderError {
                code: None,
                message: msg,
            },
        }]);
    }

    let bids = match raw.bids {
        Some(b) if !b.is_empty() => b,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let asks = match raw.asks {
        Some(a) if !a.is_empty() => a,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let best_bid_tuple = &bids[0];
    if best_bid_tuple.is_empty() {
        return Ok(vec![SymbolQuoteResult::Unavailable {
            symbol: sym.clone(),
            reason: QuoteUnavailable::EmptyBook,
        }]);
    }

    let best_ask_tuple = &asks[0];
    if best_ask_tuple.is_empty() {
        return Ok(vec![SymbolQuoteResult::Unavailable {
            symbol: sym.clone(),
            reason: QuoteUnavailable::EmptyBook,
        }]);
    }

    let bid_price = match json_value_to_decimal(&best_bid_tuple[0]) {
        Some(p) => p,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let bid_size = best_bid_tuple.get(1).and_then(json_value_to_decimal);

    let ask_price = match json_value_to_decimal(&best_ask_tuple[0]) {
        Some(p) => p,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let ask_size = best_ask_tuple.get(1).and_then(json_value_to_decimal);

    let provider_ts = raw
        .time
        .as_deref()
        .and_then(|t| crate::coinbase::normalize::parse_rfc3339_nanos(t).ok());

    let quality = if raw.auction_mode.unwrap_or(false) {
        QuoteQuality::Indicative
    } else {
        QuoteQuality::Firm
    };

    match QuoteSnapshot::try_new(
        quality,
        bid_price,
        bid_size,
        ask_price,
        ask_size,
        provider_ts,
        raw.sequence,
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

/// Creates a default HTTP REST quote snapshot client for Coinbase Exchange.
pub fn coinbase_rest_quote_client() -> Result<HttpRestQuoteClient, RestQuoteError> {
    HttpRestQuoteClient::new(
        VenueId::Coinbase,
        DEFAULT_COINBASE_QUOTE_ENDPOINT,
        1,
        coinbase_quote_url,
        parse_coinbase_quote,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn d(s: &str) -> Decimal {
        Decimal::from_str_exact(s).unwrap()
    }

    #[test]
    fn test_coinbase_quote_url() {
        let base = Url::parse(DEFAULT_COINBASE_QUOTE_ENDPOINT).unwrap();
        let syms = vec![Symbol::from("BTC-USD")];
        let url = coinbase_quote_url(&base, &syms).unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.exchange.coinbase.com/products/BTC-USD/book?level=1"
        );
    }

    #[test]
    fn test_parse_coinbase_valid() {
        let json = include_str!("../../tests/fixtures/rest_quote/coinbase/valid.json");
        let syms = vec![Symbol::from("BTC-USD")];
        let res = parse_coinbase_quote(VenueId::Coinbase, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        let item = &res[0];
        assert!(item.is_available());
        let q = item.quote().unwrap();
        assert_eq!(q.bid_price(), d("50000.00"));
        assert_eq!(q.bid_size(), Some(d("1.50000000")));
        assert_eq!(q.ask_price(), d("50001.00"));
        assert_eq!(q.ask_size(), Some(d("2.00000000")));
        assert_eq!(q.quality(), QuoteQuality::Firm);
        assert_eq!(q.sequence(), Some(12345678));
        assert!(q.provider_ts().is_some());
    }

    #[test]
    fn test_parse_coinbase_indicative() {
        let json = include_str!("../../tests/fixtures/rest_quote/coinbase/indicative.json");
        let syms = vec![Symbol::from("BTC-USD")];
        let res = parse_coinbase_quote(VenueId::Coinbase, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        let item = &res[0];
        assert!(item.is_available());
        let q = item.quote().unwrap();
        assert_eq!(q.quality(), QuoteQuality::Indicative);
    }

    #[test]
    fn test_parse_coinbase_empty() {
        let json = include_str!("../../tests/fixtures/rest_quote/coinbase/empty.json");
        let syms = vec![Symbol::from("BTC-USD")];
        let res = parse_coinbase_quote(VenueId::Coinbase, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC-USD"),
                reason: QuoteUnavailable::EmptyBook,
            }
        );
    }

    #[test]
    fn test_parse_coinbase_crossed() {
        let json = include_str!("../../tests/fixtures/rest_quote/coinbase/crossed.json");
        let syms = vec![Symbol::from("BTC-USD")];
        let res = parse_coinbase_quote(VenueId::Coinbase, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC-USD"),
                reason: QuoteUnavailable::CrossedBook,
            }
        );
    }

    #[test]
    fn test_parse_coinbase_malformed() {
        let json = include_str!("../../tests/fixtures/rest_quote/coinbase/malformed.json");
        let syms = vec![Symbol::from("BTC-USD")];
        let res = parse_coinbase_quote(VenueId::Coinbase, &syms, json, UnixNanos(100));
        assert!(matches!(res, Err(RestQuoteError::InvalidEnvelope { .. })));
    }

    #[test]
    fn test_parse_coinbase_provider_error() {
        let json = include_str!("../../tests/fixtures/rest_quote/coinbase/provider_error.json");
        let syms = vec![Symbol::from("BTC-USD")];
        let res = parse_coinbase_quote(VenueId::Coinbase, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        match &res[0] {
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { message, .. },
                ..
            } => {
                assert_eq!(message, "NotFound");
            }
            _ => panic!("expected provider error"),
        }
    }
}
