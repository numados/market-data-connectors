//! Feature-gating conformance tests for the inspector's provider registry.
//!
//! These are integration tests: they exercise only the public registry API
//! (`ProviderKind`, `ParseProviderError`) and adapt to whichever venue
//! features are compiled into the crate under test. They must stay green
//! with the default feature set, with `--no-default-features`, and with
//! `--all-features`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mdc_inspect::provider::{ParseProviderError, ProviderKind};

/// Wire names known to the inspector, paired with the cargo feature that
/// compiles them, in picker order. Mirrors the registry's private
/// `KNOWN_PROVIDERS` table; feature names map one-to-one to `mdc-providers`.
const VENUE_FEATURES: &[(&str, &str)] = &[
    ("binance", "binance"),
    ("kraken", "kraken"),
    ("coinbase", "coinbase"),
    ("bitfinex", "bitfinex"),
    ("gateio", "gateio"),
    ("bitstamp", "bitstamp"),
    ("huobi", "huobi"),
    ("coinbase-advanced", "coinbase-advanced"),
    ("lmax", "lmax"),
    ("frankfurter", "frankfurter"),
    ("vwd", "vwd"),
];

/// Number of venue features compiled into the crate under test.
fn enabled_venue_count() -> usize {
    [
        cfg!(feature = "binance"),
        cfg!(feature = "kraken"),
        cfg!(feature = "coinbase"),
        cfg!(feature = "bitfinex"),
        cfg!(feature = "gateio"),
        cfg!(feature = "bitstamp"),
        cfg!(feature = "huobi"),
        cfg!(feature = "coinbase-advanced"),
        cfg!(feature = "lmax"),
        cfg!(feature = "frankfurter"),
        cfg!(feature = "vwd"),
    ]
    .iter()
    .filter(|enabled| **enabled)
    .count()
}

/// PRD 32 §4.1 (requirements 1, 6): every compiled variant parses back from
/// its own wire name, case-insensitively, tolerating surrounding whitespace,
/// and returns itself.
#[test]
fn compiled_variants_round_trip_case_insensitively() {
    for kind in ProviderKind::ALL {
        let name = kind.as_str();
        assert!(
            ProviderKind::parse(name).is_ok_and(|parsed| parsed == *kind),
            "exact-name parse of {name}"
        );
        assert!(
            ProviderKind::parse(&name.to_uppercase()).is_ok_and(|parsed| parsed == *kind),
            "uppercase parse of {name}"
        );
        assert!(
            ProviderKind::parse(&format!("  {name}  ")).is_ok_and(|parsed| parsed == *kind),
            "whitespace-wrapped parse of {name}"
        );
        assert!(
            ProviderKind::parse(&format!("\t{}\n", name.to_uppercase()))
                .is_ok_and(|parsed| parsed == *kind),
            "mixed-case whitespace-wrapped parse of {name}"
        );
    }
}

/// PRD 32 §4.1 (requirement 6): `ALL` holds exactly the compiled venue set,
/// no more and no less.
#[test]
fn all_contains_exactly_the_compiled_venue_set() {
    assert_eq!(
        ProviderKind::ALL.len(),
        enabled_venue_count(),
        "ALL must mirror the compiled feature set one-for-one"
    );
}

/// PRD 32 §4.1 (requirements 7, 8): compiled names never read as
/// `NotCompiled`, and a known-but-uncompiled name is reported with its exact
/// missing feature.
#[test]
fn not_compiled_errors_name_the_exact_missing_feature() {
    // Compiled variants must never be reported as not-compiled.
    for kind in ProviderKind::ALL {
        let name = kind.as_str();
        assert!(
            !matches!(
                ProviderKind::parse(name),
                Err(ParseProviderError::NotCompiled { .. })
            ),
            "compiled provider {name} must not be reported NotCompiled"
        );
    }

    // Dynamically pick one known name that is not compiled into this build.
    for (name, feature) in VENUE_FEATURES {
        match ProviderKind::parse(name) {
            Err(ParseProviderError::NotCompiled {
                feature: actual, ..
            }) => {
                assert_eq!(
                    &actual, feature,
                    "missing feature named incorrectly for {name}"
                );
                return;
            }
            Err(ParseProviderError::NoneCompiled) => {
                // Zero-feature build: the registry short-circuits before the
                // per-provider branch and no variant is selectable.
                assert!(ProviderKind::ALL.is_empty());
                return;
            }
            _ => {}
        }
    }

    // Every known name parsed; only a full build reaches this point.
    assert_eq!(
        ProviderKind::ALL.len(),
        VENUE_FEATURES.len(),
        "partial build left a known provider unreported"
    );
}

/// PRD 32 §4.1 (requirement 8): unknown names yield `Unknown` whose message
/// lists every compiled provider name.
#[test]
fn unknown_provider_lists_every_compiled_name() {
    let err = ProviderKind::parse("definitely-not-a-venue").unwrap_err();
    match err {
        ParseProviderError::Unknown {
            name, available, ..
        } => {
            assert_eq!(name, "definitely-not-a-venue");
            for kind in ProviderKind::ALL {
                assert!(
                    available.contains(kind.as_str()),
                    "available list missing {}: {available}",
                    kind.as_str()
                );
            }
        }
        ParseProviderError::NoneCompiled => {
            // A zero-feature build has nothing to list.
            assert!(ProviderKind::ALL.is_empty());
        }
        other => panic!("expected Unknown, got {other:?}"),
    }
}

/// PRD 32 §4.1 (requirement 7) with zero venue features: `ALL` is empty and
/// every lookup short-circuits to `NoneCompiled` (the enum is uninhabited).
#[cfg(not(any(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "bitfinex",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
    feature = "coinbase-advanced",
    feature = "lmax",
    feature = "frankfurter",
    feature = "vwd",
)))]
#[test]
fn zero_venue_features_leave_registry_uninhabited() {
    assert!(ProviderKind::ALL.is_empty());
    assert!(matches!(
        ProviderKind::parse("binance"),
        Err(ParseProviderError::NoneCompiled)
    ));
}

/// PRD 32 §4.1 (requirement 8): a disabled provider name is reported
/// actionably. With binance disabled but other venues enabled the error is
/// `NotCompiled` naming the `binance` feature; in a zero-feature build it is
/// `NoneCompiled`.
#[cfg(not(feature = "binance"))]
#[test]
fn disabled_binance_is_reported_actionably() {
    match ProviderKind::parse("binance") {
        Err(ParseProviderError::NotCompiled { feature, .. }) => {
            assert_eq!(feature, "binance");
        }
        Err(ParseProviderError::NoneCompiled) => {
            assert!(ProviderKind::ALL.is_empty());
        }
        other => panic!("expected a not-compiled error for binance, got {other:?}"),
    }
}

/// PRD 31 §3.10 / PRD 32 §4.1 (requirement 6): every compiled variant has a
/// non-empty label and sane session bounds.
#[test]
fn every_compiled_variant_has_label_and_bounds() {
    for kind in ProviderKind::ALL {
        assert!(
            !kind.label().is_empty(),
            "empty label for {}",
            kind.as_str()
        );
        let bounds = kind.session_bounds();
        assert!(
            bounds.max_symbols >= 1,
            "{} max_symbols must be >= 1",
            kind.as_str()
        );
        assert!(
            bounds.projector_depth >= 1,
            "{} projector_depth must be >= 1",
            kind.as_str()
        );
        assert_eq!(
            bounds.affected_connection,
            1,
            "{} affected_connection must be 1",
            kind.as_str()
        );
    }
}

// PRD 32 §4.8 (requirements 86-88): capability presentation. Providers with a
// static declaration expose `Some`; Coinbase Advanced and VWD expose `None`,
// rendered as `capabilities: unknown`.

#[cfg(feature = "binance")]
#[test]
fn binance_capabilities_declared() {
    assert!(ProviderKind::Binance.capabilities().is_some());
}

#[cfg(feature = "kraken")]
#[test]
fn kraken_capabilities_declared() {
    assert!(ProviderKind::Kraken.capabilities().is_some());
}

#[cfg(feature = "coinbase")]
#[test]
fn coinbase_capabilities_declared() {
    assert!(ProviderKind::Coinbase.capabilities().is_some());
}

#[cfg(feature = "bitfinex")]
#[test]
fn bitfinex_capabilities_declared() {
    assert!(ProviderKind::Bitfinex.capabilities().is_some());
}

#[cfg(feature = "gateio")]
#[test]
fn gateio_capabilities_declared() {
    assert!(ProviderKind::Gateio.capabilities().is_some());
}

#[cfg(feature = "bitstamp")]
#[test]
fn bitstamp_capabilities_declared() {
    assert!(ProviderKind::Bitstamp.capabilities().is_some());
}

#[cfg(feature = "huobi")]
#[test]
fn huobi_capabilities_declared() {
    assert!(ProviderKind::Huobi.capabilities().is_some());
}

#[cfg(feature = "lmax")]
#[test]
fn lmax_capabilities_declared() {
    assert!(ProviderKind::Lmax.capabilities().is_some());
}

#[cfg(feature = "frankfurter")]
#[test]
fn frankfurter_capabilities_declared() {
    assert!(ProviderKind::Frankfurter.capabilities().is_some());
}

#[cfg(feature = "coinbase-advanced")]
#[test]
fn coinbase_advanced_capabilities_unknown() {
    assert!(ProviderKind::CoinbaseAdvanced.capabilities().is_none());
}

#[cfg(feature = "vwd")]
#[test]
fn vwd_capabilities_unknown() {
    assert!(ProviderKind::Vwd.capabilities().is_none());
}
