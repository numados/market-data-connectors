#![allow(clippy::unwrap_used, clippy::expect_used)]
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
    let ParseProviderError::NotCompiled { feature, .. } = &error else {
        panic!("expected not-compiled error, got {error:?}");
    };
    assert_eq!(feature, "kraken");
}

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
        assert_eq!(
            kind.session_bounds(),
            SessionBounds {
                max_symbols: 25,
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
