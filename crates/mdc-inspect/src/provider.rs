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

// Receiver-side budget per book side, not a venue capability or subscription depth.
// Keep every received level within this bound; larger books fail closed.
#[cfg(any(
    feature = "binance",
    feature = "bitfinex",
    feature = "coinbase",
    feature = "bitstamp",
    feature = "coinbase-advanced",
    feature = "lmax",
    feature = "gateio",
    feature = "huobi",
))]
const MAX_RETAINED_BOOK_DEPTH: usize = 50_000;

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
    /// Provider-neutral identity for unit scenarios; never selectable.
    #[cfg(test)]
    Test,
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
            #[cfg(test)]
            Self::Test => "test",
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
            #[cfg(test)]
            Self::Test => "Test provider",
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
            #[cfg(test)]
            Self::Test => None,
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

    /// Presentation semantics and existing static capabilities for this venue.
    pub fn presentation(self) -> crate::display::ProviderPresentation {
        use crate::display::{DisplayMode, ProviderPresentation};
        let mode = match self {
            #[cfg(feature = "frankfurter")]
            Self::Frankfurter => DisplayMode::ReferenceRate,
            #[cfg(feature = "vwd")]
            Self::Vwd => DisplayMode::SyntheticBook,
            #[allow(unreachable_patterns)]
            _ => DisplayMode::OrderBook,
        };
        ProviderPresentation {
            mode,
            capabilities: self.capabilities(),
        }
    }

    /// Adapter-supplied session bounds (PRD 31 §3.10 requirement 153).
    pub fn session_bounds(self) -> SessionBounds {
        match self {
            #[cfg(test)]
            Self::Test => SessionBounds {
                max_symbols: 25,
                projector_depth: 1000,
                affected_connection: 1,
            },
            #[cfg(feature = "binance")]
            ProviderKind::Binance => binance::BinanceAdapter::session_bounds(),
            #[cfg(feature = "kraken")]
            ProviderKind::Kraken => kraken::KrakenAdapter::session_bounds(),
            #[cfg(feature = "coinbase")]
            ProviderKind::Coinbase => coinbase::CoinbaseAdapter::session_bounds(),
            #[cfg(feature = "bitfinex")]
            ProviderKind::Bitfinex => bitfinex::BitfinexAdapter::session_bounds(),
            #[cfg(feature = "gateio")]
            ProviderKind::Gateio => gateio::GateioAdapter::session_bounds(),
            #[cfg(feature = "bitstamp")]
            ProviderKind::Bitstamp => bitstamp::BitstampAdapter::session_bounds(),
            #[cfg(feature = "huobi")]
            ProviderKind::Huobi => huobi::HuobiAdapter::session_bounds(),
            #[cfg(feature = "coinbase-advanced")]
            ProviderKind::CoinbaseAdvanced => {
                coinbase_advanced::CoinbaseAdvancedAdapter::session_bounds()
            }
            #[cfg(feature = "lmax")]
            ProviderKind::Lmax => lmax::LmaxAdapter::session_bounds(),
            #[cfg(feature = "frankfurter")]
            ProviderKind::Frankfurter => frankfurter::FrankfurterAdapter::session_bounds(),
            #[cfg(feature = "vwd")]
            ProviderKind::Vwd => vwd::VwdAdapter::session_bounds(),
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
/// WebSocket adapters return an unspawned future and the session normalizes it
/// into one owned task. Polling adapters already spawn their connector task;
/// those adapters return its join handle directly together with the connector
/// control handle that must remain alive until the task completes.
pub enum ProviderRun {
    /// An unspawned connector future owned by the session.
    Future(ProviderFuture),
    /// A connector task that was spawned by the provider adapter.
    Spawned {
        /// The actual provider task, including abort-on-drop ownership.
        task: AbortOnDropHandle<Result<(), MdcError>>,
        /// Connector control state retained for the task lifetime.
        ownership: Box<dyn Send>,
    },
}

impl ProviderRun {
    /// Wraps an already-spawned provider task and retains its control handle.
    pub fn spawned<T: Send + 'static>(
        task: tokio::task::JoinHandle<Result<(), MdcError>>,
        ownership: T,
    ) -> Self {
        Self::Spawned {
            task: AbortOnDropHandle::new(task),
            ownership: Box::new(ownership),
        }
    }
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
/// owned run: builder-owned-sink connectors with `run(cancel)`, sink-taking
/// connectors returning an unspawned future, and sink-taking connectors
/// returning an already spawned `JoinHandle`.
pub trait SessionAdapter: Send + Sync + 'static {
    /// Returns the adapter-owned bounds used by the session and reducer.
    fn bounds(&self) -> SessionBounds;

    /// Performs offline pre-start validation. Failures keep the setup screen active.
    fn validate(&self, config: &SessionConfig) -> Result<(), Error> {
        config.validate()
    }

    /// Starts the provider session.
    ///
    /// Receives the validated session configuration, the loss-intolerant
    /// sender the provider must feed, and the session child token. Returns
    /// an owned run or an actionable error before any provider network I/O
    /// when configuration is invalid.
    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error>;
}

/// Creates a sink for offline connector-builder validation.
///
/// The channel is never run or connected to a provider. Dropping its stream
/// after construction keeps validation free of provider I/O and task spawns.
#[cfg(any(
    feature = "binance",
    feature = "kraken",
    feature = "coinbase",
    feature = "gateio",
    feature = "bitstamp",
    feature = "huobi",
    feature = "coinbase-advanced",
    feature = "lmax"
))]
pub(crate) fn validation_sink(bounds: SessionBounds) -> LossIntolerantSender {
    let (sink, _stream) = mdc_sinks::loss_intolerant::loss_intolerant_channel(
        1,
        bounds.affected_connection,
        CancellationToken::new(),
    );
    sink
}

/// Returns the session adapter for a compiled provider kind.
///
/// Adapter construction performs no network I/O.
pub fn adapter_for(kind: crate::provider::ProviderKind) -> Result<Arc<dyn SessionAdapter>, Error> {
    match kind {
        #[cfg(test)]
        ProviderKind::Test => Err(Error::NotYetImplemented("unit-test identity".to_string())),
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
mod tests;
