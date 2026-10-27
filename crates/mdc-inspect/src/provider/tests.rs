#![allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(any(feature = "bitstamp", feature = "frankfurter"))]
use std::error::Error as StdError;

use super::*;

fn parse(name: &str) -> Result<ProviderKind, ParseProviderError> {
    ProviderKind::parse(name)
}

#[cfg(feature = "binance")]
#[test]
fn parses_compiled_provider_case_insensitively() {
    assert_eq!(parse("binance"), Ok(ProviderKind::Binance));
    assert_eq!(parse(" BinAnCE "), Ok(ProviderKind::Binance));
}

#[test]
fn registry_bounds_are_supplied_by_each_adapter() {
    for kind in ProviderKind::ALL {
        let adapter = adapter_for(*kind).unwrap();
        assert_eq!(adapter.bounds(), kind.session_bounds());
    }
}

#[cfg(feature = "binance")]
#[test]
fn binance_bounds_come_from_capabilities() {
    let bounds = ProviderKind::Binance.session_bounds();
    assert_eq!(bounds.max_symbols, 25);
    assert_eq!(bounds.projector_depth, 1000);
    assert_eq!(bounds.affected_connection, 1);
    assert!(ProviderKind::Binance.capabilities().is_some());
}

#[test]
fn unknown_provider_names_every_compiled_entry() {
    let error = parse("doesnotexist").unwrap_err();
    if ProviderKind::ALL.is_empty() {
        assert_eq!(error, ParseProviderError::NoneCompiled);
        return;
    }
    let ParseProviderError::Unknown { available, .. } = &error else {
        panic!("expected unknown-provider error, got {error:?}");
    };
    if let Some(kind) = ProviderKind::ALL.first() {
        assert!(available.contains(kind.as_str()));
    }
}

#[cfg(not(feature = "kraken"))]
#[test]
fn known_uncompiled_provider_names_the_feature() {
    let error = parse("kraken").unwrap_err();
    if ProviderKind::ALL.is_empty() {
        assert_eq!(error, ParseProviderError::NoneCompiled);
        return;
    }
    let ParseProviderError::NotCompiled { feature, .. } = &error else {
        panic!("expected not-compiled error, got {error:?}");
    };
    assert_eq!(feature, "kraken");
}

#[cfg(feature = "bitstamp")]
#[test]
fn builder_validation_is_offline_and_retains_provider_source() {
    let adapter = bitstamp::BitstampAdapter::with_endpoints(
        url::Url::parse("http://invalid-websocket.example").unwrap(),
        url::Url::parse("http://127.0.0.1:1").unwrap(),
    );
    let config =
        crate::config::SessionConfig::new(ProviderKind::Bitstamp, &["btcusd".to_string()]).unwrap();

    let error = adapter.validate(&config).unwrap_err();
    assert!(error.to_string().contains("Bitstamp"));
    assert!(StdError::source(&error).is_some());
}

#[cfg(feature = "bitfinex")]
#[test]
fn bitfinex_validation_uses_the_unspawned_builder_path() {
    let adapter = bitfinex::BitfinexAdapter::new();
    let config =
        crate::config::SessionConfig::new(ProviderKind::Bitfinex, &["tBTCUSD".to_string()])
            .unwrap();

    adapter.validate(&config).unwrap();
    assert_eq!(adapter.bounds(), ProviderKind::Bitfinex.session_bounds());
}

#[cfg(feature = "frankfurter")]
#[test]
fn polling_builder_validation_is_offline_and_retains_provider_source() {
    let adapter = frankfurter::FrankfurterAdapter::with_endpoints(
        url::Url::parse("file:///not-an-http-endpoint").unwrap(),
    );
    let config = crate::config::SessionConfig::new(
        ProviderKind::Frankfurter,
        &["EUR".to_string(), "USD".to_string()],
    )
    .unwrap();

    let error = adapter.validate(&config).unwrap_err();
    assert!(error.to_string().contains("Frankfurter/ECB"));
    assert!(StdError::source(&error).is_some());
}

#[cfg(feature = "vwd")]
#[test]
fn vwd_missing_empty_and_whitespace_credentials_are_redacted() {
    use std::collections::HashMap;

    struct TestEnv(HashMap<String, String>);
    impl vwd::EnvLookup for TestEnv {
        fn get(&self, key: &str) -> Option<String> {
            self.0.get(key).cloned()
        }
    }

    let keys = [
        "MDC_INSPECT_VWD_ACCOUNT_PATH",
        "MDC_INSPECT_VWD_USERNAME",
        "MDC_INSPECT_VWD_PASSWORD",
    ];
    for missing in keys {
        for value in [None, Some(String::new()), Some(" \t".to_string())] {
            let mut values = HashMap::from([
                (keys[0].to_string(), "account-secret".to_string()),
                (keys[1].to_string(), "user-secret".to_string()),
                (keys[2].to_string(), "password-secret".to_string()),
            ]);
            match value {
                Some(value) => {
                    values.insert(missing.to_string(), value);
                }
                None => {
                    values.remove(missing);
                }
            }
            let adapter = vwd::VwdAdapter::with_env(
                TestEnv(values),
                url::Url::parse("http://127.0.0.1:1").unwrap(),
            );
            let config =
                crate::config::SessionConfig::new(ProviderKind::Vwd, &["EURUSD".to_string()])
                    .unwrap();
            let error = adapter.validate(&config).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("invalid configuration: {missing}")
            );
            let rendered = format!("{error:?}");
            assert!(!rendered.contains("account-secret"));
            assert!(!rendered.contains("user-secret"));
            assert!(!rendered.contains("password-secret"));
        }
    }
}

#[cfg(any(
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
    feature = "coinbase-advanced",
    feature = "lmax",
    feature = "frankfurter",
    feature = "vwd"
))]
mod remaining_bounds {
    use super::*;
    #[cfg(feature = "kraken")]
    #[test]
    fn kraken_bounds_and_capabilities() {
        let kind = ProviderKind::Kraken;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 300,
                projector_depth: 1000,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_some());
        assert_eq!(
            kind.session_bounds().max_symbols,
            kind.capabilities()
                .unwrap()
                .bounds
                .max_symbols_per_connection
        );
    }
    #[cfg(feature = "coinbase")]
    #[test]
    fn coinbase_bounds_and_capabilities() {
        let kind = ProviderKind::Coinbase;
        let bounds = kind.session_bounds();
        assert_eq!(bounds.max_symbols, 25);
        assert_eq!(bounds.affected_connection, 1);
        assert!(kind.capabilities().is_some());
        assert_eq!(
            kind.session_bounds().max_symbols,
            kind.capabilities()
                .unwrap()
                .bounds
                .max_symbols_per_connection
        );
    }
    #[cfg(feature = "bitfinex")]
    #[test]
    fn bitfinex_bounds_and_capabilities() {
        let kind = ProviderKind::Bitfinex;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 12,
                projector_depth: 250,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_some());
        assert_eq!(
            kind.session_bounds().max_symbols,
            kind.capabilities()
                .unwrap()
                .bounds
                .max_symbols_per_connection
        );
    }
    #[cfg(feature = "gateio")]
    #[test]
    fn gateio_bounds_and_capabilities() {
        let kind = ProviderKind::Gateio;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 25,
                projector_depth: 50,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_some());
        assert_eq!(
            kind.session_bounds().max_symbols,
            kind.capabilities()
                .unwrap()
                .bounds
                .max_symbols_per_connection
        );
    }
    #[cfg(feature = "bitstamp")]
    #[test]
    fn bitstamp_bounds_and_capabilities() {
        let kind = ProviderKind::Bitstamp;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 25,
                projector_depth: 100,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_some());
        assert_eq!(
            kind.session_bounds().max_symbols,
            kind.capabilities()
                .unwrap()
                .bounds
                .max_symbols_per_connection
        );
    }
    #[cfg(feature = "huobi")]
    #[test]
    fn huobi_bounds_and_capabilities() {
        let kind = ProviderKind::Huobi;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 25,
                projector_depth: 150,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_some());
        assert_eq!(
            kind.session_bounds().max_symbols,
            kind.capabilities()
                .unwrap()
                .bounds
                .max_symbols_per_connection
        );
    }
    #[cfg(feature = "coinbase-advanced")]
    #[test]
    fn coinbase_advanced_bounds_and_capabilities() {
        let kind = ProviderKind::CoinbaseAdvanced;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 25,
                projector_depth: 100,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_none());
    }
    #[cfg(feature = "lmax")]
    #[test]
    fn lmax_bounds_and_capabilities() {
        let kind = ProviderKind::Lmax;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 26,
                projector_depth: 20,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_some());
        assert_eq!(
            kind.session_bounds().max_symbols,
            kind.capabilities()
                .unwrap()
                .bounds
                .max_symbols_per_connection
        );
    }
    #[cfg(feature = "frankfurter")]
    #[test]
    fn frankfurter_bounds_and_capabilities() {
        let kind = ProviderKind::Frankfurter;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 100,
                projector_depth: 1,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_some());
        assert_eq!(
            kind.session_bounds().max_symbols,
            kind.capabilities()
                .unwrap()
                .bounds
                .max_symbols_per_connection
        );
    }
    #[cfg(feature = "vwd")]
    #[test]
    fn vwd_bounds_and_capabilities() {
        let kind = ProviderKind::Vwd;
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 25,
                projector_depth: 1,
                affected_connection: 1
            }
        );
        assert!(kind.capabilities().is_none());
    }
}
