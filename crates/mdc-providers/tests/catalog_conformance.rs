#![cfg(all(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
))]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use mdc_core::catalog::{
    CatalogError, CatalogLimits, CatalogRequest, CatalogScope, DiscoveryCapability,
    InstrumentCatalog, ListingState, MarketFamily, ProviderKind,
};
use mdc_providers::binance::{BINANCE_CAPABILITIES, binance_catalog_client};
use mdc_providers::bitfinex::{BITFINEX_CAPABILITIES, bitfinex_catalog_client};
use mdc_providers::bitstamp::{
    BITSTAMP_CAPABILITIES, BitstampConfig, BitstampConnectorBuilder, bitstamp_catalog_client,
};
use mdc_providers::coinbase::{COINBASE_CAPABILITIES, coinbase_catalog_client};
use mdc_providers::gateio::{GATEIO_CAPABILITIES, gateio_catalog_client};
use mdc_providers::huobi::{HUOBI_CAPABILITIES, huobi_catalog_client};
use mdc_providers::kraken::{KRAKEN_CAPABILITIES, kraken_catalog_client};
use mdc_testkit::fixture::load_fixture;
use mdc_testkit::{MockCatalogServer, MockWsServer, RecordingSink};

const FIXTURES_DIR: &str = "crates/mdc-providers/tests/fixtures/catalog";

// ----------------------------------------------------------------------------
// 1. Provider Capabilities Checks
// ----------------------------------------------------------------------------

#[test]
fn test_all_seven_providers_declare_discovery_capability() {
    let capabilities = [
        (&*BINANCE_CAPABILITIES, ProviderKind::Binance),
        (&*KRAKEN_CAPABILITIES, ProviderKind::Kraken),
        (&*COINBASE_CAPABILITIES, ProviderKind::Coinbase),
        (&*BITFINEX_CAPABILITIES, ProviderKind::Bitfinex),
        (&*GATEIO_CAPABILITIES, ProviderKind::Gateio),
        (&*BITSTAMP_CAPABILITIES, ProviderKind::Bitstamp),
        (&*HUOBI_CAPABILITIES, ProviderKind::Huobi),
    ];

    for (cap, provider) in capabilities {
        assert!(
            cap.discovery.is_supported(),
            "discovery capability must be supported for {provider:?}"
        );

        match &cap.discovery {
            DiscoveryCapability::CompleteCatalog {
                scopes,
                pagination,
                default_endpoint,
            } => {
                assert_eq!(
                    *scopes,
                    vec![CatalogScope::spot(provider)],
                    "scopes must match spot for {provider:?}"
                );
                assert!(
                    !pagination,
                    "pagination must be false for initial 7 providers"
                );
                assert!(
                    default_endpoint.starts_with("https://"),
                    "default_endpoint must be https url for {provider:?}"
                );
            }
            DiscoveryCapability::Unsupported { reason } => {
                panic!("expected CompleteCatalog for {provider:?}, got Unsupported: {reason}");
            }
        }
    }
}

// ----------------------------------------------------------------------------
// 2. Loopback WireMock Conformance Across All 7 Venues
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_conformance_binance_catalog() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/binance/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Binance, &body).await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    let request = CatalogRequest::spot(ProviderKind::Binance, 42);
    let catalog = client.fetch(&request).await.unwrap();

    assert_eq!(catalog.scope, CatalogScope::spot(ProviderKind::Binance));
    assert_eq!(catalog.configuration_generation, 42);
    assert_eq!(catalog.instruments.len(), 3);

    // Sorted by native_id
    assert_eq!(catalog.instruments[0].native_id.as_str(), "BTCUSDT");
    assert_eq!(catalog.instruments[0].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[0].raw_base.as_ref().unwrap().as_str(),
        "BTC"
    );
    assert_eq!(
        catalog.instruments[0].raw_quote.as_ref().unwrap().as_str(),
        "USDT"
    );
    assert_eq!(
        catalog.instruments[0]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "BTCUSDT"
    );

    assert_eq!(catalog.instruments[1].native_id.as_str(), "ETHBTC");
    assert_eq!(catalog.instruments[1].listing, ListingState::Listed);

    assert_eq!(catalog.instruments[2].native_id.as_str(), "SOLUSDT");
    assert_eq!(catalog.instruments[2].listing, ListingState::Listed);
}

#[tokio::test]
async fn test_conformance_kraken_catalog() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/kraken/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Kraken, &body).await;

    let client = kraken_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Kraken))
        .unwrap();

    let request = CatalogRequest::spot(ProviderKind::Kraken, 43);
    let catalog = client.fetch(&request).await.unwrap();

    assert_eq!(catalog.scope, CatalogScope::spot(ProviderKind::Kraken));
    assert_eq!(catalog.configuration_generation, 43);
    assert_eq!(catalog.instruments.len(), 3);

    // Dictionary keys preserved as native_id, sorted
    assert_eq!(catalog.instruments[0].native_id.as_str(), "SOLUSD");
    assert_eq!(catalog.instruments[0].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[0]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "SOL/USD"
    );

    assert_eq!(catalog.instruments[1].native_id.as_str(), "XETHZUSD");
    assert_eq!(catalog.instruments[1].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[1].raw_base.as_ref().unwrap().as_str(),
        "XETH"
    );
    assert_eq!(
        catalog.instruments[1].raw_quote.as_ref().unwrap().as_str(),
        "ZUSD"
    );
    assert_eq!(
        catalog.instruments[1]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "ETH/USD"
    );

    assert_eq!(catalog.instruments[2].native_id.as_str(), "XXBTZUSD");
    assert_eq!(catalog.instruments[2].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[2]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "XBT/USD"
    );
}

#[tokio::test]
async fn test_conformance_coinbase_catalog() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/coinbase/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Coinbase, &body).await;

    let client = coinbase_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Coinbase))
        .unwrap();

    let request = CatalogRequest::spot(ProviderKind::Coinbase, 44);
    let catalog = client.fetch(&request).await.unwrap();

    assert_eq!(catalog.scope, CatalogScope::spot(ProviderKind::Coinbase));
    assert_eq!(catalog.instruments.len(), 3);

    assert_eq!(catalog.instruments[0].native_id.as_str(), "BTC-USD");
    assert_eq!(catalog.instruments[0].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[0].raw_base.as_ref().unwrap().as_str(),
        "BTC"
    );
    assert_eq!(
        catalog.instruments[0].raw_quote.as_ref().unwrap().as_str(),
        "USD"
    );

    assert_eq!(catalog.instruments[1].native_id.as_str(), "ETH-USD");
    assert_eq!(catalog.instruments[2].native_id.as_str(), "SOL-USD");
}

#[tokio::test]
async fn test_conformance_bitfinex_catalog() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/bitfinex/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Bitfinex, &body).await;

    let client = bitfinex_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Bitfinex))
        .unwrap();

    let request = CatalogRequest::spot(ProviderKind::Bitfinex, 45);
    let catalog = client.fetch(&request).await.unwrap();

    assert_eq!(catalog.scope, CatalogScope::spot(ProviderKind::Bitfinex));
    assert_eq!(catalog.instruments.len(), 4);

    // Flattened pairs, sorted by native_id, t prefix derived for subscription symbol
    assert_eq!(catalog.instruments[0].native_id.as_str(), "1INCH:USD");
    assert_eq!(catalog.instruments[0].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[0]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "t1INCH:USD"
    );

    assert_eq!(catalog.instruments[1].native_id.as_str(), "BTCUSD");
    assert_eq!(catalog.instruments[1].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[1]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "tBTCUSD"
    );

    assert_eq!(catalog.instruments[2].native_id.as_str(), "ETHUSD");
    assert_eq!(
        catalog.instruments[2]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "tETHUSD"
    );

    assert_eq!(catalog.instruments[3].native_id.as_str(), "SOLUSD");
    assert_eq!(
        catalog.instruments[3]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "tSOLUSD"
    );
}

#[tokio::test]
async fn test_conformance_gateio_catalog() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/gateio/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Gateio, &body).await;

    let client = gateio_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Gateio))
        .unwrap();

    let request = CatalogRequest::spot(ProviderKind::Gateio, 46);
    let catalog = client.fetch(&request).await.unwrap();

    assert_eq!(catalog.scope, CatalogScope::spot(ProviderKind::Gateio));
    assert_eq!(catalog.instruments.len(), 3);

    assert_eq!(catalog.instruments[0].native_id.as_str(), "BTC_USDT");
    assert_eq!(catalog.instruments[0].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[0].raw_base.as_ref().unwrap().as_str(),
        "BTC"
    );
    assert_eq!(
        catalog.instruments[0].raw_quote.as_ref().unwrap().as_str(),
        "USDT"
    );

    assert_eq!(catalog.instruments[1].native_id.as_str(), "ETH_USDT");
    assert_eq!(catalog.instruments[2].native_id.as_str(), "SOL_USDT");
}

#[tokio::test]
async fn test_conformance_bitstamp_catalog() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/bitstamp/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Bitstamp, &body).await;

    let client = bitstamp_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Bitstamp))
        .unwrap();

    let request = CatalogRequest::spot(ProviderKind::Bitstamp, 47);
    let catalog = client.fetch(&request).await.unwrap();

    assert_eq!(catalog.scope, CatalogScope::spot(ProviderKind::Bitstamp));
    assert_eq!(catalog.instruments.len(), 3);

    assert_eq!(catalog.instruments[0].native_id.as_str(), "btcusd");
    assert_eq!(catalog.instruments[0].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[0].raw_base.as_ref().unwrap().as_str(),
        "BTC"
    );
    assert_eq!(
        catalog.instruments[0].raw_quote.as_ref().unwrap().as_str(),
        "USD"
    );
    assert_eq!(
        catalog.instruments[0]
            .subscription_symbol
            .as_ref()
            .unwrap()
            .as_str(),
        "btcusd"
    );

    assert_eq!(catalog.instruments[1].native_id.as_str(), "ethusd");
    assert_eq!(catalog.instruments[2].native_id.as_str(), "solusd");
}

/// Symbols taken from the Bitstamp catalog drive the real connector: each catalog subscription
/// symbol becomes exactly one `diff_order_book_<symbol>` subscribe frame on the wire.
#[tokio::test]
async fn test_bitstamp_catalog_symbols_round_trip_into_subscriptions() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/bitstamp/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Bitstamp, &body).await;
    let client = bitstamp_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Bitstamp))
        .unwrap();
    let catalog = client
        .fetch(&CatalogRequest::spot(ProviderKind::Bitstamp, 47))
        .await
        .unwrap();

    let symbols: Vec<mdc_core::Symbol> = catalog
        .instruments
        .iter()
        .map(|i| mdc_core::Symbol::from(i.subscription_symbol.as_ref().unwrap().as_str()))
        .collect();
    assert_eq!(symbols.len(), 3);

    let ws = MockWsServer::spawn().await.unwrap();
    let mut config = BitstampConfig::default();
    config.ws_url = ws.ws_url().unwrap();
    let (connector, _handle) = BitstampConnectorBuilder::new(config)
        .symbols(symbols)
        .streaming_sink(RecordingSink::new())
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let cancel_child = cancel.child_token();
    let runner = tokio::spawn(async move { connector.run(cancel_child).await });

    ws.wait_for_client_messages(3).await;
    let mut channels: Vec<String> = ws
        .received_messages()
        .iter()
        .map(|m| {
            let v: serde_json::Value = serde_json::from_str(m).unwrap();
            assert_eq!(v["event"], "bts:subscribe");
            v["data"]["channel"].as_str().unwrap().to_string()
        })
        .collect();
    channels.sort();
    assert_eq!(
        channels,
        vec![
            "diff_order_book_btcusd",
            "diff_order_book_ethusd",
            "diff_order_book_solusd"
        ]
    );

    cancel.cancel();
    let _ = runner.await;
}

#[tokio::test]
async fn test_conformance_huobi_catalog() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/huobi/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Huobi, &body).await;

    let client = huobi_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Huobi))
        .unwrap();

    let request = CatalogRequest::spot(ProviderKind::Huobi, 48);
    let catalog = client.fetch(&request).await.unwrap();

    assert_eq!(catalog.scope, CatalogScope::spot(ProviderKind::Huobi));
    assert_eq!(catalog.instruments.len(), 3);

    assert_eq!(catalog.instruments[0].native_id.as_str(), "btcusdt");
    assert_eq!(catalog.instruments[0].listing, ListingState::Listed);
    assert_eq!(
        catalog.instruments[0].raw_base.as_ref().unwrap().as_str(),
        "btc"
    );
    assert_eq!(
        catalog.instruments[0].raw_quote.as_ref().unwrap().as_str(),
        "usdt"
    );

    assert_eq!(catalog.instruments[1].native_id.as_str(), "ethusdt");
    assert_eq!(catalog.instruments[2].native_id.as_str(), "solusdt");
}

// ----------------------------------------------------------------------------
// 3. Inactive / Empty Catalog Handling
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_conformance_empty_catalog() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/binance/empty.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Binance, &body).await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    let catalog = client
        .fetch(&CatalogRequest::spot(ProviderKind::Binance, 1))
        .await
        .unwrap();
    assert!(catalog.instruments.is_empty());
}

#[tokio::test]
async fn test_conformance_inactive_catalog_preserves_unavailable_state() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/kraken/inactive.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Kraken, &body).await;

    let client = kraken_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Kraken))
        .unwrap();

    let catalog = client
        .fetch(&CatalogRequest::spot(ProviderKind::Kraken, 2))
        .await
        .unwrap();
    assert_eq!(catalog.instruments.len(), 2);
    for inst in &catalog.instruments {
        assert_eq!(
            inst.listing,
            ListingState::Unavailable,
            "inactive instruments must be flagged Unavailable"
        );
    }
}

// ----------------------------------------------------------------------------
// 4. Fault Injections: Rate Limits, Timeouts, Body Caps, Scopes
// ----------------------------------------------------------------------------

#[tokio::test]
async fn test_conformance_rate_limit_429_with_retry_after() {
    let mock = MockCatalogServer::start().await;
    mock.mock_catalog_status(
        ProviderKind::Binance,
        429,
        Some(15),
        r#"{"code":-1003,"msg":"Too many requests"}"#,
    )
    .await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    let err = client
        .fetch(&CatalogRequest::spot(ProviderKind::Binance, 10))
        .await
        .unwrap_err();

    match err {
        CatalogError::RateLimit {
            provider,
            retry_after,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(retry_after, Some(Duration::from_secs(15)));
        }
        other => panic!("expected RateLimit, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_conformance_rate_limit_418_ip_ban() {
    let mock = MockCatalogServer::start().await;
    mock.mock_catalog_status(
        ProviderKind::Binance,
        418,
        Some(120),
        r#"{"code":-1003,"msg":"IP banned"}"#,
    )
    .await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    let err = client
        .fetch(&CatalogRequest::spot(ProviderKind::Binance, 11))
        .await
        .unwrap_err();

    match err {
        CatalogError::RateLimit {
            provider,
            retry_after,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(retry_after, Some(Duration::from_secs(120)));
        }
        other => panic!("expected RateLimit, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_conformance_request_timeout() {
    let mock = MockCatalogServer::start().await;
    mock.mock_catalog_delayed(
        ProviderKind::Binance,
        Duration::from_millis(500),
        r#"{"symbols":[]}"#,
    )
    .await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap()
        .with_timeout(Duration::from_millis(50));

    let err = client
        .fetch(&CatalogRequest::spot(ProviderKind::Binance, 12))
        .await
        .unwrap_err();

    match err {
        CatalogError::Timeout { provider } => {
            assert_eq!(provider, ProviderKind::Binance);
        }
        other => panic!("expected Timeout, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_conformance_response_too_large_rejected() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/binance/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Binance, &body).await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    // Set max_response_bytes to 50 bytes (much smaller than the ~1KB fixture)
    let mut request = CatalogRequest::spot(ProviderKind::Binance, 13);
    request.limits.max_response_bytes = 50;

    let err = client.fetch(&request).await.unwrap_err();
    match err {
        CatalogError::ResponseTooLarge {
            provider,
            max,
            actual,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(max, 50);
            assert!(actual > 50);
        }
        other => panic!("expected ResponseTooLarge, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_conformance_scope_mismatch() {
    let client = binance_catalog_client().unwrap();

    // Requesting Kraken scope from Binance client
    let request = CatalogRequest::spot(ProviderKind::Kraken, 14);
    let err = client.fetch(&request).await.unwrap_err();

    match err {
        CatalogError::ScopeMismatch {
            provider,
            requested,
            actual,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(requested.provider, ProviderKind::Kraken);
            assert_eq!(actual.provider, ProviderKind::Binance);
        }
        other => panic!("expected ScopeMismatch, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_conformance_unsupported_market_family() {
    let client = binance_catalog_client().unwrap();

    // Requesting linear futures from spot catalog client
    let request = CatalogRequest {
        scope: CatalogScope {
            provider: ProviderKind::Binance,
            market: MarketFamily::LinearFutures,
            entity: None,
        },
        limits: CatalogLimits::default(),
        configuration_generation: 15,
    };

    let err = client.fetch(&request).await.unwrap_err();
    match err {
        CatalogError::UnsupportedScope { provider, scope } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(scope.market, MarketFamily::LinearFutures);
        }
        other => panic!("expected UnsupportedScope, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_conformance_item_limit_exceeded() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/binance/complete.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Binance, &body).await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    // Binance fixture has 3 items; limit to 2
    let mut request = CatalogRequest::spot(ProviderKind::Binance, 16);
    request.limits.max_items = 2;

    let err = client.fetch(&request).await.unwrap_err();
    match err {
        CatalogError::ItemLimitExceeded {
            provider,
            max,
            actual,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(max, 2);
            assert_eq!(actual, 3);
        }
        other => panic!("expected ItemLimitExceeded, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_conformance_conflicting_duplicate_detected() {
    let mock = MockCatalogServer::start().await;
    // Two symbols with identical name but different base assets
    let conflicting_payload = r#"{
        "timezone": "UTC",
        "serverTime": 1700000000000,
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT"
            },
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "WBTC",
                "quoteAsset": "USDT"
            }
        ]
    }"#;

    mock.mock_catalog_ok(ProviderKind::Binance, conflicting_payload)
        .await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    let err = client
        .fetch(&CatalogRequest::spot(ProviderKind::Binance, 17))
        .await
        .unwrap_err();

    match err {
        CatalogError::ConflictingDuplicate {
            provider,
            native_id,
        } => {
            assert_eq!(provider, ProviderKind::Binance);
            assert_eq!(native_id.as_str(), "BTCUSDT");
        }
        other => panic!("expected ConflictingDuplicate, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_conformance_identical_duplicate_collapsed() {
    let mock = MockCatalogServer::start().await;
    // Two identical items
    let identical_payload = r#"{
        "timezone": "UTC",
        "serverTime": 1700000000000,
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT"
            },
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT"
            }
        ]
    }"#;

    mock.mock_catalog_ok(ProviderKind::Binance, identical_payload)
        .await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    let catalog = client
        .fetch(&CatalogRequest::spot(ProviderKind::Binance, 18))
        .await
        .unwrap();

    // Collapsed to 1 item
    assert_eq!(catalog.instruments.len(), 1);
    assert_eq!(catalog.instruments[0].native_id.as_str(), "BTCUSDT");
}

#[tokio::test]
async fn test_conformance_invalid_envelope_rejected() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/binance/error.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Binance, &body).await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    let err = client
        .fetch(&CatalogRequest::spot(ProviderKind::Binance, 19))
        .await
        .unwrap_err();

    assert!(matches!(err, CatalogError::InvalidEnvelope { .. }));
}

#[tokio::test]
async fn test_conformance_malformed_item_rejected() {
    let mock = MockCatalogServer::start().await;
    let body = load_fixture(format!("{FIXTURES_DIR}/binance/malformed.json")).unwrap();
    mock.mock_catalog_ok(ProviderKind::Binance, &body).await;

    let client = binance_catalog_client()
        .unwrap()
        .with_endpoint(&mock.endpoint_for(ProviderKind::Binance))
        .unwrap();

    let err = client
        .fetch(&CatalogRequest::spot(ProviderKind::Binance, 20))
        .await
        .unwrap_err();

    assert!(matches!(err, CatalogError::MalformedItem { .. }));
}
