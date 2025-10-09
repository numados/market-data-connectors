//! Complete provider catalog discovery domain models and contracts.
//!
//! Provides types and traits for querying complete exchange trading pair inventories
//! across venues without live streaming subscriptions.

use crate::model::symbol::Symbol;
use crate::model::timestamp::UnixNanos;
use crate::model::venue::VenueId;
use serde::{Deserialize, Serialize};
use smallstr::SmallString;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Bounded boxing for asynchronous trait methods without runtime dependencies.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Validation error when constructing bounded catalog identifiers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentifierError {
    /// Identifier string cannot be empty.
    #[error("catalog identifier string cannot be empty")]
    Empty,
    /// Identifier string exceeded maximum length.
    #[error("catalog identifier too long: max {max} bytes, got {actual}")]
    TooLong {
        /// Maximum allowed bytes.
        max: usize,
        /// Actual bytes provided.
        actual: usize,
    },
    /// Identifier contains non-ASCII characters.
    #[error("catalog identifier must contain only ASCII characters")]
    NonAscii,
    /// Identifier contains ASCII control characters.
    #[error("catalog identifier contains ASCII control characters")]
    ControlCharacter,
}

fn validate_ascii_identifier(s: &str, max: usize) -> Result<(), IdentifierError> {
    if s.is_empty() {
        return Err(IdentifierError::Empty);
    }
    if s.len() > max {
        return Err(IdentifierError::TooLong {
            max,
            actual: s.len(),
        });
    }
    if !s.is_ascii() {
        return Err(IdentifierError::NonAscii);
    }
    if s.chars().any(|c| c.is_ascii_control()) {
        return Err(IdentifierError::ControlCharacter);
    }
    Ok(())
}

/// Bounded inline native instrument identifier (1–128 ASCII bytes).
///
/// Preserves exact exchange-native identifiers without translation or case smashing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NativeInstrumentId(SmallString<[u8; 128]>);

impl NativeInstrumentId {
    /// Creates and validates a `NativeInstrumentId`.
    pub fn new(s: &str) -> Result<Self, IdentifierError> {
        validate_ascii_identifier(s, 128)?;
        Ok(Self(SmallString::from(s)))
    }

    /// Borrows the native instrument ID as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for NativeInstrumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for NativeInstrumentId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for NativeInstrumentId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// Bounded inline raw currency identifier (1–64 ASCII bytes).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RawCurrency(SmallString<[u8; 64]>);

impl RawCurrency {
    /// Creates and validates a `RawCurrency`.
    pub fn new(s: &str) -> Result<Self, IdentifierError> {
        validate_ascii_identifier(s, 64)?;
        Ok(Self(SmallString::from(s)))
    }

    /// Borrows the raw currency as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for RawCurrency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for RawCurrency {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for RawCurrency {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// Bounded inline raw status string (1–128 ASCII bytes).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RawStatus(SmallString<[u8; 128]>);

impl RawStatus {
    /// Creates and validates a `RawStatus`.
    pub fn new(s: &str) -> Result<Self, IdentifierError> {
        validate_ascii_identifier(s, 128)?;
        Ok(Self(SmallString::from(s)))
    }

    /// Borrows the raw status as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for RawStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for RawStatus {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for RawStatus {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// Bounded inline legal/regulatory entity code (1–64 ASCII bytes).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityCode(SmallString<[u8; 64]>);

impl EntityCode {
    /// Creates and validates an `EntityCode`.
    pub fn new(s: &str) -> Result<Self, IdentifierError> {
        validate_ascii_identifier(s, 64)?;
        Ok(Self(SmallString::from(s)))
    }

    /// Borrows the entity code as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for EntityCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for EntityCode {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for EntityCode {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// Supported catalog exchange provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ProviderKind {
    /// Binance Spot / Margin catalog.
    Binance,
    /// Kraken Spot catalog.
    Kraken,
    /// Coinbase Exchange catalog.
    Coinbase,
    /// Bitfinex Spot catalog.
    Bitfinex,
    /// Gate.io Spot catalog.
    Gateio,
    /// Bitstamp Spot catalog.
    Bitstamp,
    /// Huobi/HTX Spot catalog.
    Huobi,
}

impl ProviderKind {
    /// Returns the provider name in lowercase snake_case.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Kraken => "kraken",
            Self::Coinbase => "coinbase",
            Self::Bitfinex => "bitfinex",
            Self::Gateio => "gateio",
            Self::Bitstamp => "bitstamp",
            Self::Huobi => "huobi",
        }
    }

    /// Converts to the corresponding canonical [`VenueId`].
    pub const fn to_venue_id(self) -> VenueId {
        match self {
            Self::Binance => VenueId::Binance,
            Self::Kraken => VenueId::Kraken,
            Self::Coinbase => VenueId::Coinbase,
            Self::Bitfinex => VenueId::Bitfinex,
            Self::Gateio => VenueId::Gateio,
            Self::Bitstamp => VenueId::Bitstamp,
            Self::Huobi => VenueId::Huobi,
        }
    }
}

impl fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Market family for scoping instrument discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MarketFamily {
    /// Spot cash market.
    Spot,
    /// Linear perpetual or dated futures (quoted/settled in stablecoins/fiat).
    LinearFutures,
    /// Inverse perpetual or dated futures (quoted/settled in base coin).
    InverseFutures,
    /// Options contracts.
    Options,
}

impl MarketFamily {
    /// Returns the market family name in lowercase snake_case.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::LinearFutures => "linear_futures",
            Self::InverseFutures => "inverse_futures",
            Self::Options => "options",
        }
    }
}

impl fmt::Display for MarketFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Explicit scope for catalog requests.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CatalogScope {
    /// Target exchange provider.
    pub provider: ProviderKind,
    /// Target market family.
    pub market: MarketFamily,
    /// Optional legal/entity scope (e.g. for Bitstamp entity branches).
    pub entity: Option<EntityCode>,
}

impl CatalogScope {
    /// Creates a spot market scope for a given provider.
    pub fn spot(provider: ProviderKind) -> Self {
        Self {
            provider,
            market: MarketFamily::Spot,
            entity: None,
        }
    }

    /// Creates a spot market scope with an explicit entity code.
    pub fn spot_with_entity(provider: ProviderKind, entity: EntityCode) -> Self {
        Self {
            provider,
            market: MarketFamily::Spot,
            entity: Some(entity),
        }
    }
}

/// Operational bounds for catalog discovery execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogLimits {
    /// Maximum items allowed in a single catalog response.
    pub max_items: u32,
    /// Maximum decoded response bytes allowed.
    pub max_response_bytes: u32,
    /// Maximum pages allowed during catalog traversal.
    pub max_pages: u16,
}

impl Default for CatalogLimits {
    fn default() -> Self {
        Self {
            max_items: 100_000,
            max_response_bytes: 32 * 1024 * 1024, // 32 MiB
            max_pages: 100,
        }
    }
}

/// Request to fetch an instrument catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogRequest {
    /// Target catalog scope.
    pub scope: CatalogScope,
    /// Operational resource limits.
    pub limits: CatalogLimits,
    /// Generation identifier echoed back to detect stale responses.
    pub configuration_generation: u64,
}

impl CatalogRequest {
    /// Creates a default spot catalog request for the given provider.
    pub fn spot(provider: ProviderKind, configuration_generation: u64) -> Self {
        Self {
            scope: CatalogScope::spot(provider),
            limits: CatalogLimits::default(),
            configuration_generation,
        }
    }
}

/// Availability state of an instrument in the provider catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListingState {
    /// Active, tradable instrument.
    Listed,
    /// Recognized inactive, delisted, suspended, or post-only/maintenance instrument.
    Unavailable,
    /// State cannot be definitively resolved from venue metadata.
    Unknown,
}

/// Discovered instrument record in a provider catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogInstrument {
    /// Exact native exchange instrument identifier (e.g. "BTCUSDT", "XXBTZUSD").
    pub native_id: NativeInstrumentId,
    /// Canonical streaming subscription symbol if native ID fits in 16 bytes.
    pub subscription_symbol: Option<Symbol>,
    /// Raw unnormalized base currency reported by the venue.
    pub raw_base: Option<RawCurrency>,
    /// Raw unnormalized quote/counter currency reported by the venue.
    pub raw_quote: Option<RawCurrency>,
    /// Availability state in the provider catalog.
    pub listing: ListingState,
    /// Raw unnormalized venue status token (e.g. "TRADING", "online", "Enabled").
    pub raw_status: Option<RawStatus>,
}

impl CatalogInstrument {
    /// Creates a new `CatalogInstrument`, automatically resolving `subscription_symbol`
    /// from `native_id` if it fits within the 16-byte `Symbol` bound.
    pub fn new(
        native_id: NativeInstrumentId,
        raw_base: Option<RawCurrency>,
        raw_quote: Option<RawCurrency>,
        listing: ListingState,
        raw_status: Option<RawStatus>,
    ) -> Self {
        let subscription_symbol = if native_id.len() <= 16 {
            Some(Symbol::from(native_id.as_str()))
        } else {
            None
        };
        Self {
            native_id,
            subscription_symbol,
            raw_base,
            raw_quote,
            listing,
            raw_status,
        }
    }

    /// Creates a new `CatalogInstrument` with an explicit subscription symbol override
    /// (e.g. Bitfinex `t{PAIR}` or Kraken `wsname`).
    pub fn with_subscription_symbol(
        native_id: NativeInstrumentId,
        subscription_symbol: Option<Symbol>,
        raw_base: Option<RawCurrency>,
        raw_quote: Option<RawCurrency>,
        listing: ListingState,
        raw_status: Option<RawStatus>,
    ) -> Self {
        Self {
            native_id,
            subscription_symbol,
            raw_base,
            raw_quote,
            listing,
            raw_status,
        }
    }
}

/// Complete, validated snapshot of a provider's instrument catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteCatalog {
    /// Request scope for this catalog.
    pub scope: CatalogScope,
    /// Generation identifier echoed from [`CatalogRequest`].
    pub configuration_generation: u64,
    /// Validated instruments, deterministically sorted by `native_id`.
    pub instruments: Vec<CatalogInstrument>,
    /// Receipt timestamp when catalog retrieval completed.
    pub received_ts: UnixNanos,
}

/// Errors occurring during catalog discovery.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    /// HTTP transport failure.
    #[error("http transport error on {provider}: status {status}, message: {message}")]
    HttpTransport {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// HTTP status code.
        status: u16,
        /// Error message.
        message: String,
        /// Optional retry delay recommended by the server.
        retry_after: Option<Duration>,
    },
    /// Rate limit exceeded (HTTP 429 / 418).
    #[error("rate limit exceeded on {provider}")]
    RateLimit {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Optional retry delay recommended by the server.
        retry_after: Option<Duration>,
    },
    /// Request timeout / deadline exceeded.
    #[error("request timeout / deadline exceeded on {provider}")]
    Timeout {
        /// Provider where error occurred.
        provider: ProviderKind,
    },
    /// Request cancelled via cancellation token.
    #[error("request cancelled on {provider}")]
    Cancelled {
        /// Provider where error occurred.
        provider: ProviderKind,
    },
    /// Response body exceeded limits.
    #[error("response body exceeded limit on {provider}: max {max} bytes, actual {actual}")]
    ResponseTooLarge {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Configured maximum bytes.
        max: u32,
        /// Actual bytes encountered.
        actual: usize,
    },
    /// Item count exceeded limits.
    #[error("catalog item count exceeded limit on {provider}: max {max}, actual {actual}")]
    ItemLimitExceeded {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Configured maximum items.
        max: u32,
        /// Actual items parsed.
        actual: u32,
    },
    /// Page count exceeded limits.
    #[error("pagination page count exceeded limit on {provider}: max {max}")]
    PageLimitExceeded {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Configured maximum pages.
        max: u16,
    },
    /// Pagination cursor loop detected.
    #[error("pagination loop detected on {provider}: cursor '{cursor}' already visited")]
    PaginationLoop {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Repeated cursor token.
        cursor: String,
    },
    /// Invalid or unrecognized response envelope.
    #[error("invalid response envelope on {provider}: {reason}")]
    InvalidEnvelope {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Reason description.
        reason: String,
    },
    /// Malformed catalog item.
    #[error("malformed catalog item on {provider}: {reason}")]
    MalformedItem {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Reason description.
        reason: String,
    },
    /// Duplicate native instrument ID with conflicting metadata.
    #[error("conflicting duplicate native id '{native_id}' on {provider}")]
    ConflictingDuplicate {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Duplicate native ID.
        native_id: Box<NativeInstrumentId>,
    },
    /// Scope mismatch between requested and returned catalog.
    #[error("scope mismatch on {provider}: requested {requested:?}, actual {actual:?}")]
    ScopeMismatch {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Requested scope.
        requested: Box<CatalogScope>,
        /// Actual scope detected.
        actual: Box<CatalogScope>,
    },
    /// Requested scope is unsupported by the provider.
    #[error("unsupported catalog scope on {provider}: {scope:?}")]
    UnsupportedScope {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Unsupported scope.
        scope: Box<CatalogScope>,
    },
    /// Discovery capability is unsupported for this provider.
    #[error("discovery is unsupported for {provider}: {reason}")]
    Unsupported {
        /// Provider where error occurred.
        provider: ProviderKind,
        /// Reason description.
        reason: String,
    },
}

/// Discovery capability profile exposed in provider capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DiscoveryCapability {
    /// Discovery is not supported for this provider.
    Unsupported {
        /// Reason why discovery is unsupported.
        reason: String,
    },
    /// Complete catalog discovery is supported.
    CompleteCatalog {
        /// Scopes supported by this catalog provider.
        scopes: Vec<CatalogScope>,
        /// Whether pagination is supported (false for all 7 initial providers).
        pagination: bool,
        /// Default HTTP origin / endpoint.
        default_endpoint: String,
    },
}

impl DiscoveryCapability {
    /// Creates an unsupported discovery capability with a reason.
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported {
            reason: reason.into(),
        }
    }

    /// Creates a complete catalog discovery capability for spot markets on the given provider.
    pub fn complete(provider: ProviderKind, default_endpoint: impl Into<String>) -> Self {
        Self::CompleteCatalog {
            scopes: vec![CatalogScope::spot(provider)],
            pagination: false,
            default_endpoint: default_endpoint.into(),
        }
    }

    /// Returns `true` if discovery is supported.
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::CompleteCatalog { .. })
    }
}

impl From<bool> for DiscoveryCapability {
    fn from(supported: bool) -> Self {
        if supported {
            Self::CompleteCatalog {
                scopes: Vec::new(),
                pagination: false,
                default_endpoint: String::new(),
            }
        } else {
            Self::Unsupported {
                reason: "discovery is unsupported".to_string(),
            }
        }
    }
}

/// Asynchronous provider interface for complete instrument catalog discovery.
pub trait InstrumentCatalog: Send + Sync {
    /// Fetches and validates a complete provider catalog matching the given request.
    fn fetch<'a>(
        &'a self,
        request: &'a CatalogRequest,
    ) -> BoxFuture<'a, Result<CompleteCatalog, CatalogError>>;
}
