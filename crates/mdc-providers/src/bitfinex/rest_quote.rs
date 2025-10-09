//! Bitfinex REST quote snapshot decoder.

use std::collections::{BTreeMap, HashSet};

use mdc_core::catalog::ProviderKind;
use mdc_core::model::{Symbol, UnixNanos};
use mdc_core::rest_quote::{
    QuoteQuality, QuoteSnapshot, QuoteUnavailable, RestQuoteError, SymbolQuoteResult,
};
use url::Url;

use crate::rest_quote_client::HttpRestQuoteClient;
use crate::rest_quote_util::{build_url, json_value_to_decimal};

/// Default public Bitfinex REST base origin.
pub const DEFAULT_BITFINEX_QUOTE_ENDPOINT: &str = "https://api-pub.bitfinex.com";

/// Constructs the Bitfinex public tickers URL for requested symbols.
pub fn bitfinex_quote_url(base: &Url, symbols: &[Symbol]) -> Result<Url, RestQuoteError> {
    if symbols.is_empty() {
        return Err(RestQuoteError::MalformedItem {
            provider: ProviderKind::Bitfinex,
            reason: "empty symbols list".to_string(),
        });
    }
    let comma_separated = symbols
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let path = format!("v2/tickers?symbols={comma_separated}");
    build_url(ProviderKind::Bitfinex, base, &path)
}

/// Parses Bitfinex tickers JSON into validated `SymbolQuoteResult` items.
pub fn parse_bitfinex_quote(
    provider: ProviderKind,
    symbols: &[Symbol],
    payload: &str,
    received_ts: UnixNanos,
) -> Result<Vec<SymbolQuoteResult>, RestQuoteError> {
    if symbols.is_empty() {
        return Ok(Vec::new());
    }

    let raw: serde_json::Value =
        serde_json::from_str(payload).map_err(|e| RestQuoteError::InvalidEnvelope {
            provider,
            reason: format!("failed to parse bitfinex tickers json: {e}"),
        })?;

    // Check for error array: ["error", code, "message"]
    if let Some(arr) = raw.as_array() {
        if arr.first().and_then(|v| v.as_str()) == Some("error") {
            let code = arr.get(1).map(|v| v.to_string());
            let msg = arr
                .get(2)
                .and_then(|v| v.as_str())
                .unwrap_or("unknown bitfinex error")
                .to_string();

            return Ok(symbols
                .iter()
                .map(|sym| SymbolQuoteResult::Unavailable {
                    symbol: sym.clone(),
                    reason: QuoteUnavailable::ProviderError {
                        code: code.clone(),
                        message: msg.clone(),
                    },
                })
                .collect());
        }
    }

    let rows = raw
        .as_array()
        .ok_or_else(|| RestQuoteError::InvalidEnvelope {
            provider,
            reason: "expected array of ticker rows".to_string(),
        })?;

    let requested_set: HashSet<&str> = symbols.iter().map(|s| s.as_str()).collect();
    let mut seen_symbols: HashSet<String> = HashSet::new();
    let mut parsed_results: BTreeMap<String, SymbolQuoteResult> = BTreeMap::new();

    for row_val in rows {
        let row = row_val
            .as_array()
            .ok_or_else(|| RestQuoteError::MalformedItem {
                provider,
                reason: "ticker row is not an array".to_string(),
            })?;

        let sym_str =
            row.first()
                .and_then(|v| v.as_str())
                .ok_or_else(|| RestQuoteError::MalformedItem {
                    provider,
                    reason: "missing symbol string in ticker row".to_string(),
                })?;

        // Reject funding symbols for spot profile
        if sym_str.starts_with('f') {
            return Err(RestQuoteError::UnsupportedScope {
                provider,
                reason: format!("funding symbol '{sym_str}' rejected for spot profile"),
            });
        }

        // Reject unrequested symbols
        if !requested_set.contains(sym_str) {
            return Err(RestQuoteError::MalformedItem {
                provider,
                reason: format!("unrequested symbol '{sym_str}' in response"),
            });
        }

        // Reject duplicate symbols in response
        if !seen_symbols.insert(sym_str.to_string()) {
            return Err(RestQuoteError::MalformedItem {
                provider,
                reason: format!("duplicate symbol '{sym_str}' in response"),
            });
        }

        let sym = Symbol::from(sym_str);

        let bid_price = row.get(1).and_then(json_value_to_decimal);
        let bid_size = row.get(2).and_then(json_value_to_decimal);
        let ask_price = row.get(3).and_then(json_value_to_decimal);
        let ask_size = row.get(4).and_then(json_value_to_decimal);

        let quote_res = match (bid_price, ask_price) {
            (Some(bp), Some(ap)) => {
                match QuoteSnapshot::try_new(
                    QuoteQuality::Firm,
                    bp,
                    bid_size,
                    ap,
                    ask_size,
                    None,
                    None,
                    received_ts,
                ) {
                    Ok(quote) => SymbolQuoteResult::Available {
                        symbol: sym.clone(),
                        quote,
                    },
                    Err(reason) => SymbolQuoteResult::Unavailable {
                        symbol: sym.clone(),
                        reason,
                    },
                }
            }
            _ => SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::NonPositivePrice,
            },
        };

        parsed_results.insert(sym_str.to_string(), quote_res);
    }

    // Preserve exact requested symbol order; missing symbols get MissingSymbol reason
    let mut ordered = Vec::with_capacity(symbols.len());
    for sym in symbols {
        if let Some(res) = parsed_results.remove(sym.as_str()) {
            ordered.push(res);
        } else {
            ordered.push(SymbolQuoteResult::Unavailable {
                symbol: sym.clone(),
                reason: QuoteUnavailable::MissingSymbol,
            });
        }
    }

    Ok(ordered)
}

/// Creates a default HTTP REST quote snapshot client for Bitfinex.
pub fn bitfinex_rest_quote_client() -> Result<HttpRestQuoteClient, RestQuoteError> {
    HttpRestQuoteClient::new(
        ProviderKind::Bitfinex,
        DEFAULT_BITFINEX_QUOTE_ENDPOINT,
        100,
        bitfinex_quote_url,
        parse_bitfinex_quote,
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
    fn test_bitfinex_quote_url() {
        let base = Url::parse(DEFAULT_BITFINEX_QUOTE_ENDPOINT).unwrap();
        let syms = vec![Symbol::from("tBTCUSD"), Symbol::from("tETHUSD")];
        let url = bitfinex_quote_url(&base, &syms).unwrap();
        assert_eq!(
            url.as_str(),
            "https://api-pub.bitfinex.com/v2/tickers?symbols=tBTCUSD,tETHUSD"
        );
    }

    #[test]
    fn test_parse_bitfinex_valid() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitfinex/valid.json");
        let syms = vec![Symbol::from("tBTCUSD"), Symbol::from("tETHUSD")];
        let res =
            parse_bitfinex_quote(ProviderKind::Bitfinex, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 2);

        // First item: tBTCUSD
        assert_eq!(res[0].symbol(), &Symbol::from("tBTCUSD"));
        assert!(res[0].is_available());
        let q1 = res[0].quote().unwrap();
        assert_eq!(q1.bid_price, d("50000.0"));
        assert_eq!(q1.bid_size, Some(d("1.5")));
        assert_eq!(q1.ask_price, d("50001.0"));
        assert_eq!(q1.ask_size, Some(d("2.0")));

        // Second item: tETHUSD
        assert_eq!(res[1].symbol(), &Symbol::from("tETHUSD"));
        assert!(res[1].is_available());
        let q2 = res[1].quote().unwrap();
        assert_eq!(q2.bid_price, d("3000.0"));
        assert_eq!(q2.bid_size, Some(d("5.0")));
        assert_eq!(q2.ask_price, d("3001.0"));
        assert_eq!(q2.ask_size, Some(d("6.0")));
    }

    #[test]
    fn test_parse_bitfinex_stable_order() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitfinex/valid.json");
        // Request in reverse order
        let syms = vec![Symbol::from("tETHUSD"), Symbol::from("tBTCUSD")];
        let res =
            parse_bitfinex_quote(ProviderKind::Bitfinex, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].symbol(), &Symbol::from("tETHUSD"));
        assert_eq!(res[1].symbol(), &Symbol::from("tBTCUSD"));
    }

    #[test]
    fn test_parse_bitfinex_funding_rejected() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitfinex/funding.json");
        let syms = vec![Symbol::from("fUSD")];
        let err =
            parse_bitfinex_quote(ProviderKind::Bitfinex, &syms, json, UnixNanos(100)).unwrap_err();
        assert!(matches!(err, RestQuoteError::UnsupportedScope { .. }));
    }

    #[test]
    fn test_parse_bitfinex_crossed() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitfinex/crossed.json");
        let syms = vec![Symbol::from("tBTCUSD")];
        let res =
            parse_bitfinex_quote(ProviderKind::Bitfinex, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("tBTCUSD"),
                reason: QuoteUnavailable::CrossedBook,
            }
        );
    }

    #[test]
    fn test_parse_bitfinex_missing_symbol() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitfinex/missing.json");
        let syms = vec![Symbol::from("tBTCUSD"), Symbol::from("tETHUSD")];
        let res =
            parse_bitfinex_quote(ProviderKind::Bitfinex, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 2);
        // tBTCUSD was not in the payload
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("tBTCUSD"),
                reason: QuoteUnavailable::MissingSymbol,
            }
        );
        // tETHUSD was in the payload
        assert!(res[1].is_available());
    }

    #[test]
    fn test_parse_bitfinex_malformed() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitfinex/malformed.json");
        let syms = vec![Symbol::from("tBTCUSD")];
        let res =
            parse_bitfinex_quote(ProviderKind::Bitfinex, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("tBTCUSD"),
                reason: QuoteUnavailable::NonPositivePrice,
            }
        );
    }

    #[test]
    fn test_parse_bitfinex_provider_error() {
        let json = include_str!("../../tests/fixtures/rest_quote/bitfinex/provider_error.json");
        let syms = vec![Symbol::from("tBTCUSD")];
        let res =
            parse_bitfinex_quote(ProviderKind::Bitfinex, &syms, json, UnixNanos(100)).unwrap();
        assert_eq!(res.len(), 1);
        match &res[0] {
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { code, message },
                ..
            } => {
                assert_eq!(code.as_deref(), Some("10001"));
                assert!(message.contains("symbol: invalid"));
            }
            _ => panic!("expected provider error"),
        }
    }
}
