//! Huobi/HTX REST quote snapshot decoder.

use mdc_core::model::VenueId;
use mdc_core::model::{Symbol, UnixNanos};
use mdc_core::rest_quote::{
    QuoteQuality, QuoteSnapshot, QuoteUnavailable, RestQuoteError, SymbolQuoteResult,
};
use serde::Deserialize;
use url::Url;

use crate::rest_quote_client::HttpRestQuoteClient;
use crate::rest_quote_util::{build_url, json_value_to_decimal};

/// Default public Huobi/HTX REST base origin.
pub const DEFAULT_HUOBI_QUOTE_ENDPOINT: &str = "https://api.huobi.pro";

#[derive(Deserialize)]
struct RawHuobiMergedResponse {
    status: Option<String>,
    #[serde(rename = "err-code")]
    err_code: Option<String>,
    #[serde(rename = "err-msg")]
    err_msg: Option<String>,
    ts: Option<u64>,
    tick: Option<RawHuobiTick>,
}

#[derive(Deserialize)]
struct RawHuobiTick {
    id: Option<u64>,
    version: Option<u64>,
    bid: Option<Vec<serde_json::Value>>,
    ask: Option<Vec<serde_json::Value>>,
}

/// Constructs the Huobi detail/merged URL for a single requested symbol.
pub fn huobi_quote_url(base: &Url, symbols: &[Symbol]) -> Result<Url, RestQuoteError> {
    let sym = symbols
        .first()
        .ok_or_else(|| RestQuoteError::MalformedItem {
            provider: VenueId::Huobi,
            reason: "empty symbols list".to_string(),
        })?;
    let path = format!(
        "market/detail/merged?symbol={}",
        sym.as_str().to_lowercase()
    );
    build_url(VenueId::Huobi, base, &path)
}

/// Parses Huobi detail/merged JSON into a validated `SymbolQuoteResult`.
pub fn parse_huobi_quote(
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

    let raw: RawHuobiMergedResponse =
        serde_json::from_str(payload).map_err(|e| RestQuoteError::InvalidEnvelope {
            provider,
            reason: format!("failed to parse huobi merged json: {e}"),
        })?;

    if raw.status.as_deref() != Some("ok") {
        let msg = raw
            .err_msg
            .unwrap_or_else(|| "unknown huobi error".to_string());
        return Ok(vec![SymbolQuoteResult::Unavailable {
            symbol: sym.clone(),
            reason: QuoteUnavailable::ProviderError {
                code: raw.err_code,
                message: msg,
            },
        }]);
    }

    let tick = match raw.tick {
        Some(t) => t,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let bid_tuple = match &tick.bid {
        Some(b) if !b.is_empty() => b,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let ask_tuple = match &tick.ask {
        Some(a) if !a.is_empty() => a,
        _ => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::EmptyBook,
            }]);
        }
    };

    let bid_price = match json_value_to_decimal(&bid_tuple[0]) {
        Some(p) => p,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let bid_size = bid_tuple.get(1).and_then(json_value_to_decimal);

    let ask_price = match json_value_to_decimal(&ask_tuple[0]) {
        Some(p) => p,
        None => {
            return Ok(vec![SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            }]);
        }
    };

    let ask_size = ask_tuple.get(1).and_then(json_value_to_decimal);

    let provider_ts = raw.ts.map(UnixNanos::from_millis);
    let sequence = tick.version.or(tick.id);

    match QuoteSnapshot::try_new(
        QuoteQuality::Firm,
        bid_price,
        bid_size,
        ask_price,
        ask_size,
        provider_ts,
        sequence,
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

/// Creates a default HTTP REST quote snapshot client for Huobi/HTX.
pub fn huobi_rest_quote_client() -> Result<HttpRestQuoteClient, RestQuoteError> {
    HttpRestQuoteClient::new(
        VenueId::Huobi,
        DEFAULT_HUOBI_QUOTE_ENDPOINT,
        1,
        huobi_quote_url,
        parse_huobi_quote,
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
    fn test_huobi_quote_url() {
        let base = Url::parse(DEFAULT_HUOBI_QUOTE_ENDPOINT).unwrap();
        let syms = vec![Symbol::from("BTCUSDT")];
        let url = huobi_quote_url(&base, &syms).unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.huobi.pro/market/detail/merged?symbol=btcusdt"
        );
    }

    #[test]
    fn test_parse_huobi_valid() {
        let json = include_str!("../../tests/fixtures/rest_quote/huobi/valid.json");
        let syms = vec![Symbol::from("btcusdt")];
        let res = parse_huobi_quote(VenueId::Huobi, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        let item = &res[0];
        assert!(item.is_available());
        let q = item.quote().unwrap();
        assert_eq!(q.bid_price(), d("50000.0"));
        assert_eq!(q.bid_size(), Some(d("1.5")));
        assert_eq!(q.ask_price(), d("50001.0"));
        assert_eq!(q.ask_size(), Some(d("2.0")));
        assert_eq!(q.quality(), QuoteQuality::Firm);
        assert_eq!(q.provider_ts(), Some(UnixNanos::from_millis(1700000000123)));
        assert_eq!(q.sequence(), Some(12345678));
    }

    #[test]
    fn test_parse_huobi_crossed() {
        let json = include_str!("../../tests/fixtures/rest_quote/huobi/crossed.json");
        let syms = vec![Symbol::from("btcusdt")];
        let res = parse_huobi_quote(VenueId::Huobi, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("btcusdt"),
                reason: QuoteUnavailable::CrossedBook,
            }
        );
    }

    #[test]
    fn test_parse_huobi_malformed() {
        let json = include_str!("../../tests/fixtures/rest_quote/huobi/malformed.json");
        let syms = vec![Symbol::from("btcusdt")];
        let res = parse_huobi_quote(VenueId::Huobi, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("btcusdt"),
                reason: QuoteUnavailable::EmptyBook,
            }
        );
    }

    #[test]
    fn test_parse_huobi_provider_error() {
        let json = include_str!("../../tests/fixtures/rest_quote/huobi/provider_error.json");
        let syms = vec![Symbol::from("btcusdt")];
        let res = parse_huobi_quote(VenueId::Huobi, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        match &res[0] {
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { code, message },
                ..
            } => {
                assert_eq!(code.as_deref(), Some("invalid-parameter"));
                assert!(message.contains("invalid symbol"));
            }
            _ => panic!("expected provider error"),
        }
    }
}
