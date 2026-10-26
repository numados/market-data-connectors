//! Feature-gated provider registry for the inspector.
//!
//! The registry is the compile-time source of truth for which providers this
//! binary can inspect. Each variant exists only when its cargo feature is
//! enabled, and every feature forwards one-to-one to the identically named
//! `mdc-providers` feature.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use mdc_core::{MdcError, ProviderCapabilities};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;

use crate::config::SessionConfig;
use crate::error::Error;

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
    /// Kraken.
    #[cfg(feature = "kraken")]
    Kraken,
    /// Coinbase Exchange.
    #[cfg(feature = "coinbase")]
    Coinbase,
    /// Bitfinex.
    #[cfg(feature = "bitfinex")]
    Bitfinex,
    /// Gate.io.
    #[cfg(feature = "gateio")]
    Gateio,
    /// Bitstamp.
    #[cfg(feature = "bitstamp")]
    Bitstamp,
    /// Huobi/HTX.
    #[cfg(feature = "huobi")]
    Huobi,
    /// Coinbase Advanced.
    #[cfg(feature = "coinbase-advanced")]
    CoinbaseAdvanced,
    /// LMAX Digital.
    #[cfg(feature = "lmax")]
    Lmax,
    /// Frankfurter/ECB.
    #[cfg(feature = "frankfurter")]
    Frankfurter,
    /// VWD/Infront.
    #[cfg(feature = "vwd")]
    Vwd,
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
///
/// Placeholder: real definition follows below.
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
        #[cfg(feature = "kraken")]
        ProviderKind::Kraken,
        #[cfg(feature = "coinbase")]
        ProviderKind::Coinbase,
        #[cfg(feature = "bitfinex")]
        ProviderKind::Bitfinex,
        #[cfg(feature = "gateio")]
        ProviderKind::Gateio,
        #[cfg(feature = "bitstamp")]
        ProviderKind::Bitstamp,
        #[cfg(feature = "huobi")]
        ProviderKind::Huobi,
        #[cfg(feature = "coinbase-advanced")]
        ProviderKind::CoinbaseAdvanced,
        #[cfg(feature = "lmax")]
        ProviderKind::Lmax,
        #[cfg(feature = "frankfurter")]
        ProviderKind::Frankfurter,
        #[cfg(feature = "vwd")]
        ProviderKind::Vwd,
    ];

    /// Stable wire name, used by the CLI and the registry parser.
    pub fn as_str(self) -> &'static str {
        match self {
            #[cfg(feature = "binance")]
            ProviderKind::Binance => "binance",
            #[cfg(feature = "kraken")]
            ProviderKind::Kraken => "kraken",
            #[cfg(feature = "coinbase")]
            ProviderKind::Coinbase => "coinbase",
            #[cfg(feature = "bitfinex")]
            ProviderKind::Bitfinex => "bitfinex",
            #[cfg(feature = "gateio")]
            ProviderKind::Gateio => "gateio",
            #[cfg(feature = "bitstamp")]
            ProviderKind::Bitstamp => "bitstamp",
            #[cfg(feature = "huobi")]
            ProviderKind::Huobi => "huobi",
            #[cfg(feature = "coinbase-advanced")]
            ProviderKind::CoinbaseAdvanced => "coinbase-advanced",
            #[cfg(feature = "lmax")]
            ProviderKind::Lmax => "lmax",
            #[cfg(feature = "frankfurter")]
            ProviderKind::Frankfurter => "frankfurter",
            #[cfg(feature = "vwd")]
            ProviderKind::Vwd => "vwd",
        }
    }

    /// Human-facing label for headers and pickers.
    pub fn label(self) -> &'static str {
        match self {
            #[cfg(feature = "binance")]
            ProviderKind::Binance => "Binance Spot",
            #[cfg(feature = "kraken")]
            ProviderKind::Kraken => "Kraken",
            #[cfg(feature = "coinbase")]
            ProviderKind::Coinbase => "Coinbase Exchange",
            #[cfg(feature = "bitfinex")]
            ProviderKind::Bitfinex => "Bitfinex",
            #[cfg(feature = "gateio")]
            ProviderKind::Gateio => "Gate.io",
            #[cfg(feature = "bitstamp")]
            ProviderKind::Bitstamp => "Bitstamp",
            #[cfg(feature = "huobi")]
            ProviderKind::Huobi => "Huobi/HTX",
            #[cfg(feature = "coinbase-advanced")]
            ProviderKind::CoinbaseAdvanced => "Coinbase Advanced",
            #[cfg(feature = "lmax")]
            ProviderKind::Lmax => "LMAX Digital",
            #[cfg(feature = "frankfurter")]
            ProviderKind::Frankfurter => "Frankfurter/ECB",
            #[cfg(feature = "vwd")]
            ProviderKind::Vwd => "VWD/Infront",
        }
    }

    /// Static capability declaration, or `None` when the provider exports
    /// none; `None` must be displayed as `capabilities: unknown`.
    pub fn capabilities(self) -> Option<&'static ProviderCapabilities> {
        match self {
            #[cfg(feature = "binance")]
            ProviderKind::Binance => Some(&mdc_providers::binance::BINANCE_CAPABILITIES),
            #[cfg(feature = "kraken")]
            ProviderKind::Kraken => Some(&mdc_providers::kraken::KRAKEN_CAPABILITIES),
            #[cfg(feature = "coinbase")]
            ProviderKind::Coinbase => Some(&mdc_providers::coinbase::COINBASE_CAPABILITIES),
            #[cfg(feature = "bitfinex")]
            ProviderKind::Bitfinex => Some(&mdc_providers::bitfinex::BITFINEX_CAPABILITIES),
            #[cfg(feature = "gateio")]
            ProviderKind::Gateio => Some(&mdc_providers::gateio::GATEIO_CAPABILITIES),
            #[cfg(feature = "bitstamp")]
            ProviderKind::Bitstamp => Some(&mdc_providers::bitstamp::BITSTAMP_CAPABILITIES),
            #[cfg(feature = "huobi")]
            ProviderKind::Huobi => Some(&mdc_providers::huobi::HUOBI_CAPABILITIES),
            #[cfg(feature = "coinbase-advanced")]
            ProviderKind::CoinbaseAdvanced => None,
            #[cfg(feature = "lmax")]
            ProviderKind::Lmax => Some(&mdc_providers::lmax::connector::LMAX_CAPABILITIES),
            #[cfg(feature = "frankfurter")]
            ProviderKind::Frankfurter => {
                Some(&mdc_providers::frankfurter::FRANKFURTER_CAPABILITIES)
            }
            #[cfg(feature = "vwd")]
            ProviderKind::Vwd => None,
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
            #[cfg(feature = "kraken")]
            ProviderKind::Kraken => SessionBounds {
                max_symbols: mdc_providers::kraken::KRAKEN_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                projector_depth: 1000,
                affected_connection: 1,
            },
            #[cfg(feature = "coinbase")]
            ProviderKind::Coinbase => SessionBounds {
                max_symbols: mdc_providers::coinbase::COINBASE_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                projector_depth: 1000,
                affected_connection: 1,
            },
            #[cfg(feature = "bitfinex")]
            ProviderKind::Bitfinex => SessionBounds {
                max_symbols: mdc_providers::bitfinex::BITFINEX_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                projector_depth: 250,
                affected_connection: 1,
            },
            #[cfg(feature = "gateio")]
            ProviderKind::Gateio => SessionBounds {
                max_symbols: mdc_providers::gateio::GATEIO_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                projector_depth: 50,
                affected_connection: 1,
            },
            #[cfg(feature = "bitstamp")]
            ProviderKind::Bitstamp => SessionBounds {
                max_symbols: mdc_providers::bitstamp::BITSTAMP_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                projector_depth: 100,
                affected_connection: 1,
            },
            #[cfg(feature = "huobi")]
            ProviderKind::Huobi => SessionBounds {
                max_symbols: mdc_providers::huobi::HUOBI_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                projector_depth: 150,
                affected_connection: 1,
            },
            #[cfg(feature = "coinbase-advanced")]
            ProviderKind::CoinbaseAdvanced => SessionBounds {
                max_symbols: 25,
                projector_depth: 100,
                affected_connection: 1,
            },
            #[cfg(feature = "lmax")]
            ProviderKind::Lmax => SessionBounds {
                max_symbols: mdc_providers::lmax::connector::LMAX_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                projector_depth: 20,
                affected_connection: 1,
            },
            #[cfg(feature = "frankfurter")]
            ProviderKind::Frankfurter => SessionBounds {
                max_symbols: mdc_providers::frankfurter::FRANKFURTER_CAPABILITIES
                    .bounds
                    .max_symbols_per_connection,
                projector_depth: 1,
                affected_connection: 1,
            },
            #[cfg(feature = "vwd")]
            ProviderKind::Vwd => SessionBounds {
                max_symbols: 25,
                projector_depth: 1,
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

/// Owned provider run future type (PRD 31 §3.10 requirement 149).
pub type ProviderFuture = Pin<Box<dyn Future<Output = Result<(), MdcError>> + Send>>;

/// A started provider session returned by an adapter.
///
/// Owns the provider run future and any teardown guards. Dropping or
/// aborting the future must terminate the provider; spawned `JoinHandle`s
/// must be wrapped in [`AbortOnDropHandle`] guards so no provider task is
/// ever detached.
pub struct ProviderRun {
    /// Owned provider run future.
    pub future: ProviderFuture,
    /// Abort-on-drop ownership of spawned provider tasks.
    pub abort_guards: Vec<AbortOnDropHandle<()>>,
}

/// Wrapper around a [`tokio::task::JoinHandle`] that aborts the task on
/// drop, so dropped sessions never leak provider tasks.
pub struct AbortOnDropHandle<T>(tokio::task::JoinHandle<T>);

impl<T> AbortOnDropHandle<T> {
    /// Wraps a join handle.
    pub fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(handle)
    }

    /// Aborts the task without consuming the guard.
    pub fn abort(&self) {
        self.0.abort();
    }

    /// Returns `true` when the task has finished.
    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl<T> Future for AbortOnDropHandle<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> std::fmt::Debug for AbortOnDropHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AbortOnDropHandle")
            .field("finished", &self.0.is_finished())
            .finish()
    }
}

/// App-private session-adapter trait: the only seam through which provider
/// connectors are integrated (PRD 31 §3.10 requirement 148).
///
/// Implementations normalize the providers' three lifecycle styles into one
/// owned run future: builder-owned-sink connectors with `run(cancel)`,
/// sink-taking connectors returning an unspawned future, and sink-taking
/// connectors returning an already spawned `JoinHandle`.
pub trait SessionAdapter: Send + Sync + 'static {
    /// Performs offline pre-start validation. Failures keep the setup screen active.
    fn validate(&self, config: &SessionConfig) -> Result<(), Error> {
        config.validate()
    }

    /// Starts the provider session.
    ///
    /// Receives the validated session configuration, the loss-intolerant
    /// sender the provider must feed, and the session child token. Returns
    /// an owned run future plus teardown guards, or an actionable error
    /// before any provider network I/O when configuration is invalid.
    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error>;
}

/// Returns the session adapter for a compiled provider kind.
///
/// Adapter construction performs no network I/O.
pub fn adapter_for(kind: crate::provider::ProviderKind) -> Result<Arc<dyn SessionAdapter>, Error> {
    match kind {
        #[cfg(feature = "binance")]
        ProviderKind::Binance => Ok(Arc::new(binance::BinanceAdapter::new())),
        #[cfg(feature = "kraken")]
        ProviderKind::Kraken => Ok(Arc::new(kraken::KrakenAdapter::new())),
        #[cfg(feature = "coinbase")]
        ProviderKind::Coinbase => Ok(Arc::new(coinbase::CoinbaseAdapter::new())),
        #[cfg(feature = "bitfinex")]
        ProviderKind::Bitfinex => Ok(Arc::new(bitfinex::BitfinexAdapter::new())),
        #[cfg(feature = "gateio")]
        ProviderKind::Gateio => Ok(Arc::new(gateio::GateioAdapter::new())),
        #[cfg(feature = "bitstamp")]
        ProviderKind::Bitstamp => Ok(Arc::new(bitstamp::BitstampAdapter::new())),
        #[cfg(feature = "huobi")]
        ProviderKind::Huobi => Ok(Arc::new(huobi::HuobiAdapter::new())),
        #[cfg(feature = "coinbase-advanced")]
        ProviderKind::CoinbaseAdvanced => {
            Ok(Arc::new(coinbase_advanced::CoinbaseAdvancedAdapter::new()))
        }
        #[cfg(feature = "lmax")]
        ProviderKind::Lmax => Ok(Arc::new(lmax::LmaxAdapter::new())),
        #[cfg(feature = "frankfurter")]
        ProviderKind::Frankfurter => Ok(Arc::new(frankfurter::FrankfurterAdapter::new())),
        #[cfg(feature = "vwd")]
        ProviderKind::Vwd => Ok(Arc::new(vwd::VwdAdapter::new())),
    }
}

/// Frankfurter reference-rate adapter.
#[cfg(feature = "frankfurter")]
pub mod frankfurter;
/// Environment-authenticated VWD adapter.
#[cfg(feature = "vwd")]
pub mod vwd;

/// Binance adapter module (feature-gated).
#[cfg(feature = "binance")]
pub mod binance;
/// Bitfinex session adapter.
#[cfg(feature = "bitfinex")]
pub mod bitfinex;
/// Bitstamp session adapter.
#[cfg(feature = "bitstamp")]
pub mod bitstamp;
/// Coinbase Exchange session adapter.
#[cfg(feature = "coinbase")]
pub mod coinbase;
/// Coinbase Advanced session adapter.
#[cfg(feature = "coinbase-advanced")]
pub mod coinbase_advanced;
/// Gate.io session adapter.
#[cfg(feature = "gateio")]
pub mod gateio;
/// Huobi/HTX session adapter.
#[cfg(feature = "huobi")]
pub mod huobi;
/// Kraken session adapter.
#[cfg(feature = "kraken")]
pub mod kraken;
/// LMAX Digital session adapter.
#[cfg(feature = "lmax")]
pub mod lmax;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
