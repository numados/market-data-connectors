#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Contract and property verification for REST quote snapshot domain models and traits.

use std::task::{Context, Poll};
use std::time::Duration;

use mdc_core::catalog::ProviderKind;
use mdc_core::model::capabilities::TimestampCapability;
use mdc_core::model::{Symbol, UnixNanos};
use mdc_core::rest_quote::{
    BoxFuture, DEFAULT_MAX_REST_QUOTE_RESPONSE_BYTES, QuoteQuality, QuoteSnapshot,
    QuoteUnavailable, RestQuoteCapability, RestQuoteError, RestQuoteRequest, RestQuoteSnapshot,
    RestQuoteSource, SymbolQuoteResult,
};
use rust_decimal::Decimal;

fn d(s: &str) -> Decimal {
    Decimal::from_str_exact(s).unwrap()
}

#[test]
fn test_quote_snapshot_validation() {
    let recv = UnixNanos(1_000_000);
    let prov = Some(UnixNanos(999_000));

    // Valid quote
    let valid = QuoteSnapshot::try_new(
        QuoteQuality::Firm,
        d("50000.00"),
        Some(d("1.5")),
        d("50001.00"),
        Some(d("2.0")),
        prov,
        Some(42),
        recv,
    );
    assert!(valid.is_ok());
    let q = valid.unwrap();
    assert_eq!(q.quality, QuoteQuality::Firm);
    assert_eq!(q.bid_price, d("50000.00"));
    assert_eq!(q.ask_price, d("50001.00"));
    assert_eq!(q.bid_size, Some(d("1.5")));
    assert_eq!(q.ask_size, Some(d("2.0")));
    assert_eq!(q.provider_ts, prov);
    assert_eq!(q.sequence, Some(42));
    assert_eq!(q.received_ts, recv);

    // Indicative quality
    let indicative = QuoteSnapshot::try_new(
        QuoteQuality::Indicative,
        d("100"),
        None,
        d("101"),
        None,
        None,
        None,
        recv,
    );
    assert!(indicative.is_ok());
    assert_eq!(indicative.unwrap().quality, QuoteQuality::Indicative);

    // Non-positive bid price
    assert_eq!(
        QuoteSnapshot::try_new(
            QuoteQuality::Firm,
            d("0"),
            None,
            d("100"),
            None,
            None,
            None,
            recv
        ),
        Err(QuoteUnavailable::NonPositivePrice)
    );
    assert_eq!(
        QuoteSnapshot::try_new(
            QuoteQuality::Firm,
            d("-10"),
            None,
            d("100"),
            None,
            None,
            None,
            recv
        ),
        Err(QuoteUnavailable::NonPositivePrice)
    );

    // Non-positive ask price
    assert_eq!(
        QuoteSnapshot::try_new(
            QuoteQuality::Firm,
            d("100"),
            None,
            d("0"),
            None,
            None,
            None,
            recv
        ),
        Err(QuoteUnavailable::NonPositivePrice)
    );
    assert_eq!(
        QuoteSnapshot::try_new(
            QuoteQuality::Firm,
            d("100"),
            None,
            d("-5"),
            None,
            None,
            None,
            recv
        ),
        Err(QuoteUnavailable::NonPositivePrice)
    );

    // Crossed book (bid > ask)
    assert_eq!(
        QuoteSnapshot::try_new(
            QuoteQuality::Firm,
            d("105"),
            None,
            d("100"),
            None,
            None,
            None,
            recv
        ),
        Err(QuoteUnavailable::CrossedBook)
    );

    // Locked book (bid == ask)
    assert_eq!(
        QuoteSnapshot::try_new(
            QuoteQuality::Firm,
            d("100"),
            None,
            d("100"),
            None,
            None,
            None,
            recv
        ),
        Err(QuoteUnavailable::CrossedBook)
    );

    // Negative sizes
    assert_eq!(
        QuoteSnapshot::try_new(
            QuoteQuality::Firm,
            d("100"),
            Some(d("-1")),
            d("101"),
            None,
            None,
            None,
            recv
        ),
        Err(QuoteUnavailable::NonPositivePrice)
    );
    assert_eq!(
        QuoteSnapshot::try_new(
            QuoteQuality::Firm,
            d("100"),
            None,
            d("101"),
            Some(d("-0.5")),
            None,
            None,
            recv
        ),
        Err(QuoteUnavailable::NonPositivePrice)
    );
}

#[test]
fn test_symbol_quote_result_accessors() {
    let sym = Symbol::from("BTCUSDT");
    let quote = QuoteSnapshot::try_new(
        QuoteQuality::Firm,
        d("50000"),
        Some(d("1")),
        d("50001"),
        Some(d("1")),
        None,
        None,
        UnixNanos(100),
    )
    .unwrap();

    let avail = SymbolQuoteResult::Available {
        symbol: sym.clone(),
        quote: quote.clone(),
    };
    assert_eq!(avail.symbol(), &sym);
    assert!(avail.is_available());
    assert_eq!(avail.quote(), Some(&quote));

    let unavail = SymbolQuoteResult::Unavailable {
        symbol: sym.clone(),
        reason: QuoteUnavailable::MissingSymbol,
    };
    assert_eq!(unavail.symbol(), &sym);
    assert!(!unavail.is_available());
    assert_eq!(unavail.quote(), None);
}

#[test]
fn test_rest_quote_request_constructors() {
    let single = RestQuoteRequest::single("ETHUSDT", 42);
    assert_eq!(single.symbols, vec![Symbol::from("ETHUSDT")]);
    assert_eq!(single.configuration_generation, 42);
    assert_eq!(
        single.max_response_bytes,
        DEFAULT_MAX_REST_QUOTE_RESPONSE_BYTES
    );

    let batch = RestQuoteRequest::batch(vec!["BTCUSDT", "ETHUSDT"], 101);
    assert_eq!(
        batch.symbols,
        vec![Symbol::from("BTCUSDT"), Symbol::from("ETHUSDT")]
    );
    assert_eq!(batch.configuration_generation, 101);
    assert_eq!(
        batch.max_response_bytes,
        DEFAULT_MAX_REST_QUOTE_RESPONSE_BYTES
    );
}

#[test]
fn test_rest_quote_capability() {
    let unsupp = RestQuoteCapability::unsupported("no quote endpoint");
    assert!(!unsupp.is_supported());

    let single = RestQuoteCapability::single(
        true,
        TimestampCapability::ReceiptOnly,
        "https://api.binance.com",
    );
    assert!(single.is_supported());
    match single {
        RestQuoteCapability::Snapshot {
            max_symbols_per_request,
            batch_mode,
            sizes,
            timestamp_source,
            default_endpoint,
        } => {
            assert_eq!(max_symbols_per_request, 1);
            assert!(!batch_mode);
            assert!(sizes);
            assert_eq!(timestamp_source, TimestampCapability::ReceiptOnly);
            assert_eq!(default_endpoint, "https://api.binance.com");
        }
        _ => panic!("expected snapshot capability"),
    }

    let batch = RestQuoteCapability::batch(
        100,
        true,
        TimestampCapability::ReceiptOnly,
        "https://api-pub.bitfinex.com",
    );
    assert!(batch.is_supported());
    match batch {
        RestQuoteCapability::Snapshot {
            max_symbols_per_request,
            batch_mode,
            ..
        } => {
            assert_eq!(max_symbols_per_request, 100);
            assert!(batch_mode);
        }
        _ => panic!("expected snapshot capability"),
    }

    // From<bool>
    assert!(!RestQuoteCapability::from(false).is_supported());
    assert!(RestQuoteCapability::from(true).is_supported());
}

#[test]
fn test_rest_quote_error_display() {
    let err = RestQuoteError::RateLimit {
        provider: ProviderKind::Binance,
        retry_after: Some(Duration::from_secs(60)),
    };
    assert!(err.to_string().contains("rate limit exceeded on binance"));

    let err2 = RestQuoteError::Timeout {
        provider: ProviderKind::Coinbase,
    };
    assert!(err2.to_string().contains("request timeout"));

    let err3 = RestQuoteError::BatchLimitExceeded {
        provider: ProviderKind::Bitstamp,
        max: 1,
        actual: 5,
    };
    assert!(
        err3.to_string()
            .contains("batch limit exceeded on bitstamp: max 1, actual 5")
    );
}

/// Verification that `RestQuoteSource` trait compiles and executes
/// without Tokio runtime dependencies using standard `BoxFuture`.
struct MockRestQuoteService {
    canned: Result<RestQuoteSnapshot, RestQuoteError>,
}

impl RestQuoteSource for MockRestQuoteService {
    fn fetch<'a>(
        &'a self,
        _request: &'a RestQuoteRequest,
    ) -> BoxFuture<'a, Result<RestQuoteSnapshot, RestQuoteError>> {
        Box::pin(std::future::ready(self.canned.clone()))
    }
}

#[test]
fn test_rest_quote_source_runtime_independent_execution() {
    let sym = Symbol::from("BTCUSDT");
    let quote = QuoteSnapshot::try_new(
        QuoteQuality::Firm,
        d("50000"),
        Some(d("1")),
        d("50001"),
        Some(d("2")),
        Some(UnixNanos(900)),
        Some(123),
        UnixNanos(1000),
    )
    .unwrap();

    let expected_snapshot = RestQuoteSnapshot {
        configuration_generation: 1,
        received_ts: UnixNanos(1000),
        results: vec![SymbolQuoteResult::Available {
            symbol: sym.clone(),
            quote,
        }],
    };

    let service = MockRestQuoteService {
        canned: Ok(expected_snapshot.clone()),
    };

    let request = RestQuoteRequest::single(sym, 1);
    let mut fut = service.fetch(&request);

    // Poll synchronously with a noop waker (no tokio required)
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    let poll_res = fut.as_mut().poll(&mut cx);

    match poll_res {
        Poll::Ready(Ok(snap)) => assert_eq!(snap, expected_snapshot),
        _ => panic!("expected ready quote snapshot"),
    }
}
