#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Contract and property verification for catalog discovery domain models and traits.

use mdc_core::catalog::{
    BoxFuture, CatalogError, CatalogInstrument, CatalogLimits, CatalogRequest, CatalogScope,
    CompleteCatalog, DiscoveryCapability, EntityCode, IdentifierError, InstrumentCatalog,
    ListingState, MarketFamily, NativeInstrumentId, ProviderKind, RawCurrency, RawStatus,
};
use mdc_core::model::{Symbol, UnixNanos, VenueId};
use std::task::{Context, Poll};

#[test]
fn test_native_instrument_id_validation() {
    // Valid lengths
    assert!(NativeInstrumentId::new("BTCUSDT").is_ok());
    assert!(NativeInstrumentId::new("A").is_ok());

    let exact_128 = "a".repeat(128);
    let id_128 = NativeInstrumentId::new(&exact_128);
    assert!(id_128.is_ok());
    assert_eq!(id_128.unwrap().as_str(), exact_128);

    // Empty rejection
    assert_eq!(
        NativeInstrumentId::new("").unwrap_err(),
        IdentifierError::Empty
    );

    // Length limit > 128 rejection
    let over_128 = "a".repeat(129);
    assert_eq!(
        NativeInstrumentId::new(&over_128).unwrap_err(),
        IdentifierError::TooLong {
            max: 128,
            actual: 129,
        }
    );

    // Non-ASCII rejection
    assert_eq!(
        NativeInstrumentId::new("BTC€").unwrap_err(),
        IdentifierError::NonAscii
    );

    // Control characters rejection
    assert_eq!(
        NativeInstrumentId::new("BTC\nUSDT").unwrap_err(),
        IdentifierError::ControlCharacter
    );
    assert_eq!(
        NativeInstrumentId::new("BTC\0USDT").unwrap_err(),
        IdentifierError::ControlCharacter
    );

    // Deserialization & TryFrom checks
    assert!(NativeInstrumentId::try_from("".to_string()).is_err());
    assert!(serde_json::from_str::<NativeInstrumentId>(r#""""#).is_err());
    assert!(serde_json::from_str::<NativeInstrumentId>(r#""BTC\u0000USDT""#).is_err());
    assert!(serde_json::from_str::<NativeInstrumentId>(r#""BTC€""#).is_err());
    let too_long_json = format!(r#""{}""#, "a".repeat(129));
    assert!(serde_json::from_str::<NativeInstrumentId>(&too_long_json).is_err());
}

#[test]
fn test_raw_currency_and_raw_status_validation() {
    // RawCurrency bound 64
    assert!(RawCurrency::new("USDT").is_ok());
    let exact_64 = "C".repeat(64);
    assert!(RawCurrency::new(&exact_64).is_ok());
    assert_eq!(
        RawCurrency::new(&"C".repeat(65)).unwrap_err(),
        IdentifierError::TooLong {
            max: 64,
            actual: 65,
        }
    );
    assert_eq!(RawCurrency::new("").unwrap_err(), IdentifierError::Empty);
    assert_eq!(
        RawCurrency::new("USD$€").unwrap_err(),
        IdentifierError::NonAscii
    );
    assert_eq!(
        RawCurrency::new("USD\n").unwrap_err(),
        IdentifierError::ControlCharacter
    );

    // Deserialization & TryFrom checks for RawCurrency
    assert!(RawCurrency::try_from("".to_string()).is_err());
    assert!(serde_json::from_str::<RawCurrency>(r#""""#).is_err());
    assert!(serde_json::from_str::<RawCurrency>(r#""USD\u0000""#).is_err());
    assert!(serde_json::from_str::<RawCurrency>(r#""USD$€""#).is_err());
    let too_long_curr = format!(r#""{}""#, "C".repeat(65));
    assert!(serde_json::from_str::<RawCurrency>(&too_long_curr).is_err());

    // RawStatus bound 128
    assert!(RawStatus::new("TRADING").is_ok());
    assert!(RawStatus::new(&"S".repeat(128)).is_ok());
    assert_eq!(
        RawStatus::new(&"S".repeat(129)).unwrap_err(),
        IdentifierError::TooLong {
            max: 128,
            actual: 129,
        }
    );
    assert_eq!(RawStatus::new("").unwrap_err(), IdentifierError::Empty);
    assert_eq!(
        RawStatus::new("STATUS\n").unwrap_err(),
        IdentifierError::ControlCharacter
    );
    assert_eq!(
        RawStatus::new("STATUS_€").unwrap_err(),
        IdentifierError::NonAscii
    );

    // Deserialization & TryFrom checks for RawStatus
    assert!(RawStatus::try_from("".to_string()).is_err());
    assert!(serde_json::from_str::<RawStatus>(r#""""#).is_err());
    assert!(serde_json::from_str::<RawStatus>(r#""STAT\u0000""#).is_err());
    assert!(serde_json::from_str::<RawStatus>(r#""STATUS_€""#).is_err());
    let too_long_status = format!(r#""{}""#, "S".repeat(129));
    assert!(serde_json::from_str::<RawStatus>(&too_long_status).is_err());

    // EntityCode bound 64
    assert!(EntityCode::new("US").is_ok());
    assert!(EntityCode::new(&"E".repeat(64)).is_ok());
    assert_eq!(
        EntityCode::new(&"E".repeat(65)).unwrap_err(),
        IdentifierError::TooLong {
            max: 64,
            actual: 65,
        }
    );
    assert_eq!(EntityCode::new("").unwrap_err(), IdentifierError::Empty);
    assert_eq!(
        EntityCode::new("EC\n").unwrap_err(),
        IdentifierError::ControlCharacter
    );
    assert_eq!(
        EntityCode::new("EC_€").unwrap_err(),
        IdentifierError::NonAscii
    );

    // Deserialization & TryFrom checks for EntityCode
    assert!(EntityCode::try_from("".to_string()).is_err());
    assert!(serde_json::from_str::<EntityCode>(r#""""#).is_err());
    assert!(serde_json::from_str::<EntityCode>(r#""EC\u0000""#).is_err());
    assert!(serde_json::from_str::<EntityCode>(r#""EC_€""#).is_err());
    let too_long_ec = format!(r#""{}""#, "E".repeat(65));
    assert!(serde_json::from_str::<EntityCode>(&too_long_ec).is_err());
}

#[test]
fn test_symbol_bound_subscription_resolution() {
    // <= 16 bytes resolves to Some(Symbol)
    let short_id = NativeInstrumentId::new("BTCUSDT").unwrap();
    let instrument = CatalogInstrument::new(
        short_id.clone(),
        Some(RawCurrency::new("BTC").unwrap()),
        Some(RawCurrency::new("USDT").unwrap()),
        ListingState::Listed,
        Some(RawStatus::new("TRADING").unwrap()),
    );
    assert_eq!(
        instrument.subscription_symbol,
        Some(Symbol::from("BTCUSDT"))
    );
    assert_eq!(instrument.native_id, short_id);

    // Exact 16 bytes
    let exact_16_str = "1234567890123456";
    let exact_16_id = NativeInstrumentId::new(exact_16_str).unwrap();
    let inst_16 = CatalogInstrument::new(exact_16_id, None, None, ListingState::Listed, None);
    assert_eq!(
        inst_16.subscription_symbol,
        Some(Symbol::from(exact_16_str))
    );

    // > 16 bytes produces None (discoverable without subscription symbol)
    let long_id_str = "VERYLONGSYMBOLNAME12345";
    let long_id = NativeInstrumentId::new(long_id_str).unwrap();
    let inst_long = CatalogInstrument::new(long_id.clone(), None, None, ListingState::Listed, None);
    assert_eq!(inst_long.subscription_symbol, None);
    assert_eq!(inst_long.native_id.as_str(), long_id_str);

    // with_subscription_symbol override
    let overridden = CatalogInstrument::with_subscription_symbol(
        long_id,
        Some(Symbol::from("OVERRIDE")),
        None,
        None,
        ListingState::Listed,
        None,
    );
    assert_eq!(
        overridden.subscription_symbol,
        Some(Symbol::from("OVERRIDE"))
    );
}

#[test]
fn test_deterministic_sorting_by_native_id() {
    let mut instruments = [
        CatalogInstrument::new(
            NativeInstrumentId::new("ETHUSDT").unwrap(),
            None,
            None,
            ListingState::Listed,
            None,
        ),
        CatalogInstrument::new(
            NativeInstrumentId::new("BTCUSDT").unwrap(),
            None,
            None,
            ListingState::Listed,
            None,
        ),
        CatalogInstrument::new(
            NativeInstrumentId::new("ADAUSDT").unwrap(),
            None,
            None,
            ListingState::Listed,
            None,
        ),
    ];

    instruments.sort_by(|a, b| a.native_id.cmp(&b.native_id));

    assert_eq!(instruments[0].native_id.as_str(), "ADAUSDT");
    assert_eq!(instruments[1].native_id.as_str(), "BTCUSDT");
    assert_eq!(instruments[2].native_id.as_str(), "ETHUSDT");
}

#[test]
fn test_provider_kind_mappings() {
    let providers = [
        (ProviderKind::Binance, VenueId::Binance, "binance"),
        (ProviderKind::Kraken, VenueId::Kraken, "kraken"),
        (ProviderKind::Coinbase, VenueId::Coinbase, "coinbase"),
        (ProviderKind::Bitfinex, VenueId::Bitfinex, "bitfinex"),
        (ProviderKind::Gateio, VenueId::Gateio, "gateio"),
        (ProviderKind::Bitstamp, VenueId::Bitstamp, "bitstamp"),
        (ProviderKind::Huobi, VenueId::Huobi, "huobi"),
    ];

    for (provider, venue, name) in providers {
        assert_eq!(provider.to_venue_id(), venue);
        assert_eq!(provider.as_str(), name);
        assert_eq!(provider.to_string(), name);
    }

    let families = [
        (MarketFamily::Spot, "spot"),
        (MarketFamily::LinearFutures, "linear_futures"),
        (MarketFamily::InverseFutures, "inverse_futures"),
        (MarketFamily::Options, "options"),
    ];
    for (family, name) in families {
        assert_eq!(family.as_str(), name);
        assert_eq!(family.to_string(), name);
    }
}

#[test]
fn test_catalog_serde_roundtrip() {
    let req = CatalogRequest {
        scope: CatalogScope::spot(ProviderKind::Binance),
        limits: CatalogLimits::default(),
        configuration_generation: 42,
    };

    let json = serde_json::to_string(&req).unwrap();
    let deserialized: CatalogRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(req, deserialized);

    let catalog = CompleteCatalog {
        scope: CatalogScope::spot_with_entity(
            ProviderKind::Bitstamp,
            EntityCode::new("US").unwrap(),
        ),
        configuration_generation: 99,
        instruments: vec![CatalogInstrument::new(
            NativeInstrumentId::new("BTC/USD").unwrap(),
            Some(RawCurrency::new("BTC").unwrap()),
            Some(RawCurrency::new("USD").unwrap()),
            ListingState::Listed,
            Some(RawStatus::new("Enabled").unwrap()),
        )],
        received_ts: UnixNanos(1_700_000_000_000_000_000),
    };

    let catalog_json = serde_json::to_string(&catalog).unwrap();
    let deserialized_catalog: CompleteCatalog = serde_json::from_str(&catalog_json).unwrap();
    assert_eq!(catalog, deserialized_catalog);
}

#[test]
fn test_discovery_capability_behavior() {
    let unsupported = DiscoveryCapability::unsupported("no discovery endpoint");
    assert!(!unsupported.is_supported());

    let complete = DiscoveryCapability::CompleteCatalog {
        scopes: vec![CatalogScope::spot(ProviderKind::Kraken)],
        pagination: false,
        default_endpoint: "https://api.kraken.com".to_string(),
    };
    assert!(complete.is_supported());

    // From<bool> conversion
    assert!(!DiscoveryCapability::from(false).is_supported());
    assert!(DiscoveryCapability::from(true).is_supported());
}

/// Verification that `InstrumentCatalog` trait compiles and executes
/// without Tokio runtime dependencies using standard `BoxFuture`.
struct MockCatalogService {
    canned: Result<CompleteCatalog, CatalogError>,
}

impl InstrumentCatalog for MockCatalogService {
    fn fetch<'a>(
        &'a self,
        _request: &'a CatalogRequest,
    ) -> BoxFuture<'a, Result<CompleteCatalog, CatalogError>> {
        Box::pin(std::future::ready(self.canned.clone()))
    }
}

#[test]
fn test_instrument_catalog_runtime_independent_execution() {
    let expected_catalog = CompleteCatalog {
        scope: CatalogScope::spot(ProviderKind::Binance),
        configuration_generation: 1,
        instruments: vec![CatalogInstrument::new(
            NativeInstrumentId::new("BTCUSDT").unwrap(),
            Some(RawCurrency::new("BTC").unwrap()),
            Some(RawCurrency::new("USDT").unwrap()),
            ListingState::Listed,
            Some(RawStatus::new("TRADING").unwrap()),
        )],
        received_ts: UnixNanos(100),
    };

    let service = MockCatalogService {
        canned: Ok(expected_catalog.clone()),
    };

    let request = CatalogRequest::spot(ProviderKind::Binance, 1);
    let mut fut = service.fetch(&request);

    // Poll the future synchronously using a dummy waker (no tokio required)
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    let poll_res = fut.as_mut().poll(&mut cx);

    match poll_res {
        Poll::Ready(Ok(cat)) => assert_eq!(cat, expected_catalog),
        _ => panic!("expected ready catalog result"),
    }
}
