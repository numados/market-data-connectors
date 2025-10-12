#![cfg(all(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use mdc_core::catalog::ProviderKind;
use mdc_core::model::Symbol;
use mdc_core::rest_quote::{
    QuoteQuality, QuoteUnavailable, RestQuoteCapability, RestQuoteError, RestQuoteRequest,
    RestQuoteSource, SymbolQuoteResult,
};
use mdc_providers::binance::{BINANCE_CAPABILITIES, binance_rest_quote_client};
use mdc_providers::bitfinex::{BITFINEX_CAPABILITIES, bitfinex_rest_quote_client};
use mdc_providers::bitstamp::{BITSTAMP_CAPABILITIES, bitstamp_rest_quote_client};
use mdc_providers::coinbase::{COINBASE_CAPABILITIES, coinbase_rest_quote_client};
use mdc_providers::gateio::{GATEIO_CAPABILITIES, gateio_rest_quote_client};
use mdc_providers::huobi::{HUOBI_CAPABILITIES, huobi_rest_quote_client};
use mdc_providers::kraken::{KRAKEN_CAPABILITIES, kraken_rest_quote_client};
use mdc_testkit::MockRestQuoteServer;
use mdc_testkit::fixture::load_fixture;
use rust_decimal::Decimal;
use std::str::FromStr;

const FIXTURES_DIR: &str = "crates/mdc-providers/tests/fixtures/rest_quote";

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

// ============================================================================
// 1. Capabilities Matrix Checks
// ============================================================================

#[test]
fn test_all_seven_legacy_providers_declare_rest_quote_capability() {
    let capabilities = [
        (
            &*BINANCE_CAPABILITIES,
            ProviderKind::Binance,
            1,
            false,
            true,
        ),
        (
            &*COINBASE_CAPABILITIES,
            ProviderKind::Coinbase,
            1,
            false,
            true,
        ),
        (&*KRAKEN_CAPABILITIES, ProviderKind::Kraken, 1, false, true),
        (
            &*BITSTAMP_CAPABILITIES,
            ProviderKind::Bitstamp,
            1,
            false,
            false,
        ),
        (&*GATEIO_CAPABILITIES, ProviderKind::Gateio, 1, false, true),
        (&*HUOBI_CAPABILITIES, ProviderKind::Huobi, 1, false, true),
        (
            &*BITFINEX_CAPABILITIES,
            ProviderKind::Bitfinex,
            100,
            true,
            true,
        ),
    ];

    for (cap, provider, max_symbols, batch_mode, sizes) in capabilities {
        assert!(
            cap.is_rest_quote_supported(),
            "REST quote snapshot capability must be supported for {provider:?}"
        );

        match &cap.rest_quote {
            RestQuoteCapability::Snapshot {
                max_symbols_per_request,
                batch_mode: b_mode,
                sizes: s_mode,
                default_endpoint,
                ..
            } => {
                assert_eq!(
                    *max_symbols_per_request, max_symbols,
                    "max_symbols_per_request mismatch for {provider:?}"
                );
                assert_eq!(*b_mode, batch_mode, "batch_mode mismatch for {provider:?}");
                assert_eq!(*s_mode, sizes, "sizes mode mismatch for {provider:?}");
                assert!(
                    default_endpoint.starts_with("https://"),
                    "default_endpoint must be an https URL for {provider:?}"
                );
            }
            RestQuoteCapability::Unsupported { reason } => {
                panic!("expected Snapshot capability for {provider:?}, got Unsupported: {reason}");
            }
        }
    }
}

// ============================================================================
// 2. Loopback WireMock Conformance Across All 7 Venues
// ============================================================================

#[tokio::test]
async fn test_conformance_binance_quote() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/binance/valid.json")).unwrap();
    mock.mock_binance_quote_ok("BTCUSDT", &body).await;

    let client = binance_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTCUSDT"), 42);
    let snapshot = client.fetch(&request).await.unwrap();

    assert_eq!(snapshot.configuration_generation, 42);
    assert_eq!(snapshot.results.len(), 1);

    let res = &snapshot.results[0];
    assert!(res.is_available());
    let q = res.quote().unwrap();
    assert_eq!(q.bid_price(), d("50000.00"));
    assert_eq!(q.bid_size(), Some(d("1.50")));
    assert_eq!(q.ask_price(), d("50001.00"));
    assert_eq!(q.ask_size(), Some(d("2.00")));
    assert_eq!(q.quality(), QuoteQuality::Firm);
}

#[tokio::test]
async fn test_conformance_binance_quote_crossed() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/binance/crossed.json")).unwrap();
    mock.mock_binance_quote_ok("BTCUSDT", &body).await;

    let client = binance_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTCUSDT"), 42);
    let snapshot = client.fetch(&request).await.unwrap();

    assert_eq!(
        snapshot.results[0],
        SymbolQuoteResult::Unavailable {
            symbol: Symbol::from("BTCUSDT"),
            reason: QuoteUnavailable::CrossedBook,
        }
    );
}

#[tokio::test]
async fn test_conformance_coinbase_quote() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/coinbase/valid.json")).unwrap();
    mock.mock_coinbase_quote_ok("BTC-USD", &body).await;

    let client = coinbase_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTC-USD"), 100);
    let snapshot = client.fetch(&request).await.unwrap();

    assert_eq!(snapshot.configuration_generation, 100);
    assert_eq!(snapshot.results.len(), 1);

    let res = &snapshot.results[0];
    assert!(res.is_available());
    let q = res.quote().unwrap();
    assert_eq!(q.bid_price(), d("50000.00"));
    assert_eq!(q.bid_size(), Some(d("1.50")));
    assert_eq!(q.ask_price(), d("50001.00"));
    assert_eq!(q.ask_size(), Some(d("2.00")));
    assert_eq!(q.sequence(), Some(12345678));
    assert_eq!(q.quality(), QuoteQuality::Firm);
}

#[tokio::test]
async fn test_conformance_coinbase_quote_indicative() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/coinbase/indicative.json")).unwrap();
    mock.mock_coinbase_quote_ok("BTC-USD", &body).await;

    let client = coinbase_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTC-USD"), 101);
    let snapshot = client.fetch(&request).await.unwrap();

    let res = &snapshot.results[0];
    assert!(res.is_available());
    let q = res.quote().unwrap();
    assert_eq!(q.quality(), QuoteQuality::Indicative);
}

#[tokio::test]
async fn test_conformance_kraken_quote() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/kraken/valid.json")).unwrap();
    mock.mock_kraken_quote_ok("XXBTZUSD", &body).await;

    let client = kraken_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("XXBTZUSD"), 200);
    let snapshot = client.fetch(&request).await.unwrap();

    assert_eq!(snapshot.configuration_generation, 200);
    assert_eq!(snapshot.results.len(), 1);

    let res = &snapshot.results[0];
    assert!(res.is_available());
    let q = res.quote().unwrap();
    assert_eq!(q.bid_price(), d("50000.00000"));
    assert_eq!(q.bid_size(), Some(d("1.500")));
    assert_eq!(q.ask_price(), d("50001.00000"));
    assert_eq!(q.ask_size(), Some(d("2.000")));
    assert_eq!(q.quality(), QuoteQuality::Firm);
}

#[tokio::test]
async fn test_conformance_bitstamp_quote() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/bitstamp/valid.json")).unwrap();
    mock.mock_bitstamp_quote_ok("btcusd", &body).await;

    let client = bitstamp_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("btcusd"), 300);
    let snapshot = client.fetch(&request).await.unwrap();

    assert_eq!(snapshot.configuration_generation, 300);
    assert_eq!(snapshot.results.len(), 1);

    let res = &snapshot.results[0];
    assert!(res.is_available());
    let q = res.quote().unwrap();
    assert_eq!(q.bid_price(), d("50000.00"));
    assert_eq!(q.bid_size(), None);
    assert_eq!(q.ask_price(), d("50001.00"));
    assert_eq!(q.ask_size(), None);
    assert!(q.provider_ts().is_some());
    assert_eq!(q.quality(), QuoteQuality::Firm);
}

#[tokio::test]
async fn test_conformance_gateio_quote() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/gateio/valid.json")).unwrap();
    mock.mock_gateio_quote_ok("BTC_USDT", &body).await;

    let client = gateio_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTC_USDT"), 400);
    let snapshot = client.fetch(&request).await.unwrap();

    assert_eq!(snapshot.configuration_generation, 400);
    assert_eq!(snapshot.results.len(), 1);

    let res = &snapshot.results[0];
    assert!(res.is_available());
    let q = res.quote().unwrap();
    assert_eq!(q.bid_price(), d("50000.0"));
    assert_eq!(q.bid_size(), Some(d("1.5")));
    assert_eq!(q.ask_price(), d("50001.0"));
    assert_eq!(q.ask_size(), Some(d("2.0")));
    assert_eq!(q.quality(), QuoteQuality::Firm);
}

#[tokio::test]
async fn test_conformance_huobi_quote() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/huobi/valid.json")).unwrap();
    mock.mock_huobi_quote_ok("btcusdt", &body).await;

    let client = huobi_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("btcusdt"), 500);
    let snapshot = client.fetch(&request).await.unwrap();

    assert_eq!(snapshot.configuration_generation, 500);
    assert_eq!(snapshot.results.len(), 1);

    let res = &snapshot.results[0];
    assert!(res.is_available());
    let q = res.quote().unwrap();
    assert_eq!(q.bid_price(), d("50000.00"));
    assert_eq!(q.bid_size(), Some(d("1.50")));
    assert_eq!(q.ask_price(), d("50001.00"));
    assert_eq!(q.ask_size(), Some(d("2.00")));
    assert!(q.provider_ts().is_some());
    assert_eq!(q.quality(), QuoteQuality::Firm);
}

#[tokio::test]
async fn test_conformance_bitfinex_quote_batch() {
    let mock = MockRestQuoteServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/bitfinex/valid.json")).unwrap();
    mock.mock_bitfinex_quote_ok("tBTCUSD,tETHUSD", &body).await;

    let client = bitfinex_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let symbols = vec![Symbol::from("tBTCUSD"), Symbol::from("tETHUSD")];
    let request = RestQuoteRequest {
        symbols,
        configuration_generation: 600,
        max_response_bytes: 8 * 1024 * 1024,
    };
    let snapshot = client.fetch(&request).await.unwrap();

    assert_eq!(snapshot.configuration_generation, 600);
    assert_eq!(snapshot.results.len(), 2);

    let btc = &snapshot.results[0];
    assert!(btc.is_available());
    assert_eq!(btc.symbol(), &Symbol::from("tBTCUSD"));
    let btc_q = btc.quote().unwrap();
    assert_eq!(btc_q.bid_price(), d("50000"));
    assert_eq!(btc_q.bid_size(), Some(d("1.5")));
    assert_eq!(btc_q.ask_price(), d("50001"));
    assert_eq!(btc_q.ask_size(), Some(d("2")));

    let eth = &snapshot.results[1];
    assert!(eth.is_available());
    assert_eq!(eth.symbol(), &Symbol::from("tETHUSD"));
    let eth_q = eth.quote().unwrap();
    assert_eq!(eth_q.bid_price(), d("3000"));
    assert_eq!(eth_q.bid_size(), Some(d("5")));
    assert_eq!(eth_q.ask_price(), d("3001"));
    assert_eq!(eth_q.ask_size(), Some(d("6")));
}

#[tokio::test]
async fn test_conformance_all_providers_error_fixtures() {
    // Binance: missing, malformed, provider_error
    {
        let mock = MockRestQuoteServer::start().await;
        let client = binance_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/binance/missing.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTCUSDT"),
                reason: QuoteUnavailable::MissingSymbol
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = binance_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/binance/malformed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTCUSDT"),
                reason: QuoteUnavailable::NonPositivePrice
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = binance_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(
            &load_fixture(format!("{FIXTURES_DIR}/binance/provider_error.json")).unwrap(),
        )
        .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1))
            .await
            .unwrap();
        assert!(matches!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { .. },
                ..
            }
        ));
    }

    // Coinbase: crossed, empty, malformed, provider_error
    {
        let mock = MockRestQuoteServer::start().await;
        let client = coinbase_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/coinbase/crossed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTC-USD"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC-USD"),
                reason: QuoteUnavailable::CrossedBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = coinbase_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/coinbase/empty.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTC-USD"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC-USD"),
                reason: QuoteUnavailable::EmptyBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = coinbase_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/coinbase/malformed.json")).unwrap())
            .await;
        let err = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTC-USD"), 1))
            .await
            .unwrap_err();
        assert!(matches!(err, RestQuoteError::InvalidEnvelope { .. }));
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = coinbase_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(
            &load_fixture(format!("{FIXTURES_DIR}/coinbase/provider_error.json")).unwrap(),
        )
        .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTC-USD"), 1))
            .await
            .unwrap();
        assert!(matches!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { .. },
                ..
            }
        ));
    }

    // Kraken: crossed, empty, malformed, provider_error
    {
        let mock = MockRestQuoteServer::start().await;
        let client = kraken_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/kraken/crossed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("XXBTZUSD"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("XXBTZUSD"),
                reason: QuoteUnavailable::CrossedBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = kraken_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/kraken/empty.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("XXBTZUSD"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("XXBTZUSD"),
                reason: QuoteUnavailable::MissingSymbol
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = kraken_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/kraken/malformed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("XXBTZUSD"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("XXBTZUSD"),
                reason: QuoteUnavailable::EmptyBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = kraken_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(
            &load_fixture(format!("{FIXTURES_DIR}/kraken/provider_error.json")).unwrap(),
        )
        .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("XXBTZUSD"), 1))
            .await
            .unwrap();
        assert!(matches!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { .. },
                ..
            }
        ));
    }

    // Bitstamp: crossed, malformed, provider_error
    {
        let mock = MockRestQuoteServer::start().await;
        let client = bitstamp_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/bitstamp/crossed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("btcusd"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("btcusd"),
                reason: QuoteUnavailable::CrossedBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = bitstamp_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/bitstamp/malformed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("btcusd"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("btcusd"),
                reason: QuoteUnavailable::NonPositivePrice
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = bitstamp_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(
            &load_fixture(format!("{FIXTURES_DIR}/bitstamp/provider_error.json")).unwrap(),
        )
        .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("btcusd"), 1))
            .await
            .unwrap();
        assert!(matches!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { .. },
                ..
            }
        ));
    }

    // Gate.io: crossed, missing, malformed, provider_error
    {
        let mock = MockRestQuoteServer::start().await;
        let client = gateio_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/gateio/crossed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTC_USDT"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC_USDT"),
                reason: QuoteUnavailable::CrossedBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = gateio_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/gateio/missing.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTC_USDT"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC_USDT"),
                reason: QuoteUnavailable::MissingSymbol
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = gateio_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/gateio/malformed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTC_USDT"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("BTC_USDT"),
                reason: QuoteUnavailable::NonPositivePrice
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = gateio_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(
            &load_fixture(format!("{FIXTURES_DIR}/gateio/provider_error.json")).unwrap(),
        )
        .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("BTC_USDT"), 1))
            .await
            .unwrap();
        assert!(matches!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { .. },
                ..
            }
        ));
    }

    // Huobi: crossed, malformed, provider_error
    {
        let mock = MockRestQuoteServer::start().await;
        let client = huobi_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/huobi/crossed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("btcusdt"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("btcusdt"),
                reason: QuoteUnavailable::CrossedBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = huobi_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/huobi/malformed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("btcusdt"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("btcusdt"),
                reason: QuoteUnavailable::EmptyBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = huobi_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(
            &load_fixture(format!("{FIXTURES_DIR}/huobi/provider_error.json")).unwrap(),
        )
        .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("btcusdt"), 1))
            .await
            .unwrap();
        assert!(matches!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { .. },
                ..
            }
        ));
    }

    // Bitfinex: crossed, funding, missing, malformed, provider_error
    {
        let mock = MockRestQuoteServer::start().await;
        let client = bitfinex_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/bitfinex/crossed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("tBTCUSD"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("tBTCUSD"),
                reason: QuoteUnavailable::CrossedBook
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = bitfinex_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/bitfinex/funding.json")).unwrap())
            .await;
        let err = client
            .fetch(&RestQuoteRequest::single(Symbol::from("fUSD"), 1))
            .await
            .unwrap_err();
        assert!(matches!(err, RestQuoteError::UnsupportedScope { .. }));
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = bitfinex_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/bitfinex/missing.json")).unwrap())
            .await;
        let syms = vec![Symbol::from("tBTCUSD"), Symbol::from("tETHUSD")];
        let req = RestQuoteRequest {
            symbols: syms,
            configuration_generation: 1,
            max_response_bytes: 8 * 1024 * 1024,
        };
        let res = client.fetch(&req).await.unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("tBTCUSD"),
                reason: QuoteUnavailable::MissingSymbol,
            }
        );
        assert!(res.results[1].is_available());

        // Unrequested rows in a batch response reject the response
        let single_req = RestQuoteRequest::single(Symbol::from("tBTCUSD"), 1);
        let err = client.fetch(&single_req).await.unwrap_err();
        assert!(matches!(err, RestQuoteError::MalformedItem { .. }));
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = bitfinex_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(&load_fixture(format!("{FIXTURES_DIR}/bitfinex/malformed.json")).unwrap())
            .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("tBTCUSD"), 1))
            .await
            .unwrap();
        assert_eq!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                symbol: Symbol::from("tBTCUSD"),
                reason: QuoteUnavailable::NonPositivePrice
            }
        );
    }
    {
        let mock = MockRestQuoteServer::start().await;
        let client = bitfinex_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();
        mock.mock_any_ok(
            &load_fixture(format!("{FIXTURES_DIR}/bitfinex/provider_error.json")).unwrap(),
        )
        .await;
        let res = client
            .fetch(&RestQuoteRequest::single(Symbol::from("tBTCUSD"), 1))
            .await
            .unwrap();
        assert!(matches!(
            res.results[0],
            SymbolQuoteResult::Unavailable {
                reason: QuoteUnavailable::ProviderError { .. },
                ..
            }
        ));
    }
}

// ============================================================================
// 3. Batch Limits & Empty Requests
// ============================================================================

#[tokio::test]
async fn test_batch_limit_exceeded_single_symbol_provider() {
    let client = binance_rest_quote_client().unwrap();
    let symbols = vec![Symbol::from("BTCUSDT"), Symbol::from("ETHUSDT")];
    let request = RestQuoteRequest {
        symbols,
        configuration_generation: 1,
        max_response_bytes: 8 * 1024 * 1024,
    };
    let res = client.fetch(&request).await;
    match res {
        Err(RestQuoteError::BatchLimitExceeded {
            provider,
            max,
            actual,
        }) => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(max, 1);
            assert_eq!(actual, 2);
        }
        other => panic!("expected BatchLimitExceeded error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_empty_symbols_returns_empty_snapshot() {
    let client = binance_rest_quote_client().unwrap();
    let request = RestQuoteRequest {
        symbols: Vec::new(),
        configuration_generation: 99,
        max_response_bytes: 8 * 1024 * 1024,
    };
    let snapshot = client.fetch(&request).await.unwrap();
    assert_eq!(snapshot.configuration_generation, 99);
    assert!(snapshot.results.is_empty());
}

// ============================================================================
// 4. Rate Limiting (429 + Retry-After)
// ============================================================================

#[tokio::test]
async fn test_rate_limit_429_with_retry_after() {
    let mock = MockRestQuoteServer::start().await;
    mock.mock_any_status(429, Some(30), r#"{"msg":"too many requests"}"#)
        .await;

    let client = binance_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1);
    let err = client.fetch(&request).await.unwrap_err();

    match err {
        RestQuoteError::RateLimit {
            provider,
            retry_after,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(retry_after, Some(Duration::from_secs(30)));
        }
        other => panic!("expected RateLimit error, got {other:?}"),
    }
}

// ============================================================================
// 5. Timeout Handling
// ============================================================================

#[tokio::test]
async fn test_request_timeout() {
    let mock = MockRestQuoteServer::start().await;
    mock.mock_delayed(Duration::from_millis(200), "{}").await;

    let client = binance_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap()
        .with_timeout(Duration::from_millis(30));

    let request = RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1);
    let err = client.fetch(&request).await.unwrap_err();

    match err {
        RestQuoteError::Timeout { provider } => {
            assert_eq!(provider, ProviderKind::Binance);
        }
        other => panic!("expected Timeout error, got {other:?}"),
    }
}

// ============================================================================
// 6. Response Payload Limit (8 MiB Cap)
// ============================================================================

#[tokio::test]
async fn test_payload_too_large() {
    let mock = MockRestQuoteServer::start().await;
    // 9 MiB payload
    mock.mock_large_payload(9 * 1024 * 1024).await;

    let client = binance_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1);
    let err = client.fetch(&request).await.unwrap_err();

    match err {
        RestQuoteError::ResponseTooLarge {
            provider,
            max,
            actual,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(max, 8 * 1024 * 1024);
            assert!(actual > 8 * 1024 * 1024);
        }
        other => panic!("expected ResponseTooLarge error, got {other:?}"),
    }
}

// ============================================================================
// 7. Foreign-Origin Redirect Disallowed
// ============================================================================

#[tokio::test]
async fn test_foreign_origin_redirect_disallowed() {
    let mock = MockRestQuoteServer::start().await;
    mock.mock_redirect(302, "https://untrusted-foreign-host.com/api/quote")
        .await;

    let client = binance_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1);
    let err = client.fetch(&request).await.unwrap_err();

    match err {
        RestQuoteError::HttpTransport {
            provider,
            status: _,
            message,
            retry_after: _,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert!(
                message.contains("redirect") || message.contains("foreign-origin"),
                "unexpected transport error details: {message}"
            );
        }
        other => panic!("expected HttpTransport error for foreign redirect, got {other:?}"),
    }
}

// ============================================================================
// 8. Cancellation via Future Drop
// ============================================================================

#[tokio::test]
async fn test_future_drop_cancels_cleanly() {
    let mock = MockRestQuoteServer::start().await;
    mock.mock_delayed(Duration::from_millis(500), "{}").await;

    let client = binance_rest_quote_client()
        .unwrap()
        .with_endpoint(&mock.uri())
        .unwrap();

    let request = RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1);
    let fut = client.fetch(&request);

    // Dropping the future cancels the in-flight request
    drop(fut);
}

// ============================================================================
// 9. Current-Thread Runtime Compatibility
// ============================================================================

#[test]
fn test_current_thread_runtime_compatibility() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let mock = MockRestQuoteServer::start().await;
        let body = load_fixture(format!("{FIXTURES_DIR}/binance/valid.json")).unwrap();
        mock.mock_binance_quote_ok("BTCUSDT", &body).await;

        let client = binance_rest_quote_client()
            .unwrap()
            .with_endpoint(&mock.uri())
            .unwrap();

        let request = RestQuoteRequest::single(Symbol::from("BTCUSDT"), 1);
        let snapshot = client.fetch(&request).await.unwrap();
        assert_eq!(snapshot.results.len(), 1);
        assert!(snapshot.results[0].is_available());
    });
}
