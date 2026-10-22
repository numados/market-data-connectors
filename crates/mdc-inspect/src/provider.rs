//! Feature-gated provider registry for the inspector.
//!
//! The registry is the compile-time source of truth for which providers this
//! binary can inspect. Each variant exists only when its cargo feature is
//! enabled, and every feature forwards one-to-one to the identically named
//! `mdc-providers` feature.

use mdc_core::ProviderCapabilities;

/// Adapter-supplied session resource bounds (PRD 31 §3.10).
///
/// One owner for the symbol cap, the projector depth, and the channel
/// connection identity across all providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionBounds {
    /// Maximum symbols accepted in one session.
    pub max_symbols: usize,
    /// Level 2 projector maximum depth.
    pub projector_depth: usize,
    /// `affected_connection` value for the loss-intolerant channel.
    pub affected_connection: u32,
}

/// Selectable provider kinds.
///
/// With no venue feature enabled the enum is uninhabited and every lookup
/// fails with an actionable error instead of offering a dead entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderKind {
    /// Binance Spot.
    #[cfg(feature = "binance")]
    Binance,
}

/// Provider names known to the inspector, mapped to the feature that
/// compiles them. Names outside this table are reported as unknown.
const KNOWN_PROVIDERS: &[(&str, &str)] = &[
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

/// Failures when resolving a provider name against the compiled registry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ParseProviderError {
    /// The name matches no provider known to the inspector.
    #[error("unknown provider '{name}'; available: {available}")]
    Unknown {
        /// Name as supplied by the operator.
        name: String,
        /// Compiled provider names.
        available: String,
    },
    /// The provider is known but its feature is not compiled in.
    #[error(
        "provider '{name}' is not compiled into this binary; rebuild with --features {feature}"
    )]
    NotCompiled {
        /// Name as supplied by the operator.
        name: String,
        /// Feature that compiles the provider.
        feature: String,
    },
    /// The binary was compiled without any venue feature.
    #[error(
        "no provider features are compiled into this binary; rebuild with --features <provider>"
    )]
    NoneCompiled,
}

impl ProviderKind {
    /// All variants compiled into this binary, in picker order.
    pub const ALL: &'static [ProviderKind] = &[
        #[cfg(feature = "binance")]
        ProviderKind::Binance,
    ];

    /// Stable wire name, used by the CLI and the registry parser.
    pub fn as_str(self) -> &'static str {
        match self {
            #[cfg(feature = "binance")]
            ProviderKind::Binance => "binance",
        }
    }

    /// Human-facing label for headers and pickers.
    pub fn label(self) -> &'static str {
        match self {
            #[cfg(feature = "binance")]
            ProviderKind::Binance => "Binance Spot",
        }
    }

    /// Static capability declaration, or `None` when the provider exports
    /// none; `None` must be displayed as `capabilities: unknown`.
    pub fn capabilities(self) -> Option<&'static ProviderCapabilities> {
        match self {
            #[cfg(feature = "binance")]
            ProviderKind::Binance => Some(&mdc_providers::binance::BINANCE_CAPABILITIES),
        }
    }

    /// Adapter-supplied session bounds (PRD 31 §3.10 requirement 153).
    pub fn session_bounds(self) -> SessionBounds {
        match self {
            #[cfg(feature = "binance")]
            ProviderKind::Binance => SessionBounds {
                max_symbols: mdc_providers::binance::BINANCE_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                // Matches the Binance REST depth snapshot request limit.
                projector_depth: 1000,
                affected_connection: 1,
            },
        }
    }

    /// Resolves a provider name against the compiled registry.
    pub fn parse(name: &str) -> Result<Self, ParseProviderError> {
        let lowered = name.trim().to_ascii_lowercase();
        if Self::ALL.is_empty() {
            return Err(ParseProviderError::NoneCompiled);
        }
        for kind in Self::ALL {
            if kind.as_str() == lowered {
                return Ok(*kind);
            }
        }
        for (known, feature) in KNOWN_PROVIDERS {
            if *known == lowered {
                return Err(ParseProviderError::NotCompiled {
                    name: lowered,
                    feature: (*feature).to_string(),
                });
            }
        }
        let available = Self::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        Err(ParseProviderError::Unknown {
            name: lowered,
            available,
        })
    }
}

#[cfg(test)]
mod tests {
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
}
