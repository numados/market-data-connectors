//! Cold, on-demand REST quote snapshot models, capabilities, and traits.
//!
//! # Overview
//! Defines bounded models and asynchronous provider contracts for fetching on-demand
//! top-of-book (BBO) quote snapshots across exchange REST endpoints without streaming
//! WebSocket subscriptions.

use std::fmt;
use std::time::Duration;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub use crate::catalog::BoxFuture;
use crate::error::ArcError;
use crate::model::capabilities::TimestampCapability;
use crate::model::{Symbol, UnixNanos, VenueId};

/// Default maximum HTTP response body cap for REST quote snapshots (8 MiB).
pub const DEFAULT_MAX_REST_QUOTE_RESPONSE_BYTES: u32 = 8 * 1024 * 1024;

/// Quality assessment of an on-demand quote snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteQuality {
    /// Firm, actionable executable top-of-book quote.
    Firm,
    /// Indicative rate or auction-mode quote without firm execution guarantee.
    Indicative,
}

impl fmt::Display for QuoteQuality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Firm => write!(f, "firm"),
            Self::Indicative => write!(f, "indicative"),
        }
    }
}

/// Reason why an on-demand quote snapshot could not be provided for a symbol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum QuoteUnavailable {
    /// Requested symbol was not present in the provider's response.
    MissingSymbol,
    /// Order book is empty on one or both sides (zero depth).
    EmptyBook,
    /// Bid price is greater than or equal to ask price (crossed/locked book).
    CrossedBook,
    /// Price is zero, negative, or unparseable.
    NonPositivePrice,
    /// Size or volume is negative or unparseable.
    NonPositiveSize,
    /// Venue explicitly returned an error envelope or rejected the market.
    ProviderError {
        /// Optional venue-specific error code.
        code: Option<String>,
        /// Venue-specific error message.
        message: String,
    },
    /// Market is recognized as closed, suspended, or in post-only maintenance.
    MarketClosed,
}

impl fmt::Display for QuoteUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSymbol => write!(f, "missing symbol in response"),
            Self::EmptyBook => write!(f, "empty book side"),
            Self::CrossedBook => write!(f, "crossed or locked book"),
            Self::NonPositivePrice => write!(f, "non-positive price"),
            Self::NonPositiveSize => write!(f, "non-positive size"),
            Self::ProviderError { code, message } => {
                if let Some(c) = code {
                    write!(f, "provider error [{c}]: {message}")
                } else {
                    write!(f, "provider error: {message}")
                }
            }
            Self::MarketClosed => write!(f, "market closed or suspended"),
        }
    }
}

/// Validated top-of-book quote snapshot for a single symbol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawQuoteSnapshot")]
pub struct QuoteSnapshot {
    /// Quality assessment (firm vs indicative).
    quality: QuoteQuality,
    /// Best bid price.
    bid_price: Decimal,
    /// Available volume / size at best bid, if reported by venue.
    bid_size: Option<Decimal>,
    /// Best ask price.
    ask_price: Decimal,
    /// Available volume / size at best ask, if reported by venue.
    ask_size: Option<Decimal>,
    /// Venue-reported exchange timestamp, if provided.
    provider_ts: Option<UnixNanos>,
    /// Venue-reported sequence or update ID, if provided.
    sequence: Option<u64>,
    /// Ingestion timestamp captured at HTTP frame receipt.
    received_ts: UnixNanos,
}

#[derive(Deserialize)]
struct RawQuoteSnapshot {
    quality: QuoteQuality,
    bid_price: Decimal,
    #[serde(default)]
    bid_size: Option<Decimal>,
    ask_price: Decimal,
    #[serde(default)]
    ask_size: Option<Decimal>,
    #[serde(default)]
    provider_ts: Option<UnixNanos>,
    #[serde(default)]
    sequence: Option<u64>,
    received_ts: UnixNanos,
}

impl TryFrom<RawQuoteSnapshot> for QuoteSnapshot {
    type Error = QuoteUnavailable;

    fn try_from(raw: RawQuoteSnapshot) -> Result<Self, Self::Error> {
        Self::try_new(
            raw.quality,
            raw.bid_price,
            raw.bid_size,
            raw.ask_price,
            raw.ask_size,
            raw.provider_ts,
            raw.sequence,
            raw.received_ts,
        )
    }
}

impl QuoteSnapshot {
    /// Constructs and validates a new `QuoteSnapshot`.
    ///
    /// Validates that `bid_price > 0`, `ask_price > 0`, and `bid_price < ask_price`.
    /// Rejects negative sizes with `QuoteUnavailable::NonPositiveSize`.
    /// Normalizes zero sizes to `None` (absent).
    /// Returns `Err(QuoteUnavailable)` if validation fails.
    #[allow(clippy::too_many_arguments)] // Ingests all 8 QuoteSnapshot fields for atomic validation.
    pub fn try_new(
        quality: QuoteQuality,
        bid_price: Decimal,
        bid_size: Option<Decimal>,
        ask_price: Decimal,
        ask_size: Option<Decimal>,
        provider_ts: Option<UnixNanos>,
        sequence: Option<u64>,
        received_ts: UnixNanos,
    ) -> Result<Self, QuoteUnavailable> {
        if bid_price <= Decimal::ZERO || ask_price <= Decimal::ZERO {
            return Err(QuoteUnavailable::NonPositivePrice);
        }
        if bid_price >= ask_price {
            return Err(QuoteUnavailable::CrossedBook);
        }
        let bid_size = match bid_size {
            Some(bs) if bs < Decimal::ZERO => return Err(QuoteUnavailable::NonPositiveSize),
            Some(bs) if bs.is_zero() => None,
            other => other,
        };
        let ask_size = match ask_size {
            Some(asize) if asize < Decimal::ZERO => return Err(QuoteUnavailable::NonPositiveSize),
            Some(asize) if asize.is_zero() => None,
            other => other,
        };

        Ok(Self {
            quality,
            bid_price,
            bid_size,
            ask_price,
            ask_size,
            provider_ts,
            sequence,
            received_ts,
        })
    }

    /// Returns the quality assessment (firm vs indicative).
    pub fn quality(&self) -> QuoteQuality {
        self.quality
    }

    /// Returns the best bid price.
    pub fn bid_price(&self) -> Decimal {
        self.bid_price
    }

    /// Returns available volume / size at best bid, if reported by venue.
    pub fn bid_size(&self) -> Option<Decimal> {
        self.bid_size
    }

    /// Returns the best ask price.
    pub fn ask_price(&self) -> Decimal {
        self.ask_price
    }

    /// Returns available volume / size at best ask, if reported by venue.
    pub fn ask_size(&self) -> Option<Decimal> {
        self.ask_size
    }

    /// Returns the venue-reported exchange timestamp, if provided.
    pub fn provider_ts(&self) -> Option<UnixNanos> {
        self.provider_ts
    }

    /// Returns the venue-reported sequence or update ID, if provided.
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }

    /// Returns the ingestion timestamp captured at HTTP frame receipt.
    pub fn received_ts(&self) -> UnixNanos {
        self.received_ts
    }
}

/// Result of evaluating an on-demand quote snapshot for a requested symbol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SymbolQuoteResult {
    /// Valid quote snapshot is available.
    Available {
        /// Market symbol.
        symbol: Symbol,
        /// Top-of-book quote snapshot.
        quote: QuoteSnapshot,
    },
    /// Quote is unavailable.
    Unavailable {
        /// Market symbol.
        symbol: Symbol,
        /// Reason description.
        reason: QuoteUnavailable,
    },
}

impl SymbolQuoteResult {
    /// Returns the symbol this result applies to.
    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::Available { symbol, .. } | Self::Unavailable { symbol, .. } => symbol,
        }
    }

    /// Returns `true` if this result contains an available quote snapshot.
    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available { .. })
    }

    /// Returns the quote snapshot if available.
    pub fn quote(&self) -> Option<&QuoteSnapshot> {
        match self {
            Self::Available { quote, .. } => Some(quote),
            Self::Unavailable { .. } => None,
        }
    }
}

/// Request to fetch on-demand REST quote snapshots for one or more symbols.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestQuoteRequest {
    /// List of target market symbols to query.
    pub symbols: Vec<Symbol>,
    /// Generation identifier echoed back to detect stale responses.
    pub configuration_generation: u64,
    /// Maximum allowed HTTP response body size in bytes.
    pub max_response_bytes: u32,
}

impl RestQuoteRequest {
    /// Creates a single-symbol REST quote snapshot request with default limits.
    pub fn single(symbol: impl Into<Symbol>, configuration_generation: u64) -> Self {
        Self {
            symbols: vec![symbol.into()],
            configuration_generation,
            max_response_bytes: DEFAULT_MAX_REST_QUOTE_RESPONSE_BYTES,
        }
    }

    /// Creates a multi-symbol batch REST quote snapshot request with default limits.
    pub fn batch<I, S>(symbols: I, configuration_generation: u64) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Symbol>,
    {
        Self {
            symbols: symbols.into_iter().map(Into::into).collect(),
            configuration_generation,
            max_response_bytes: DEFAULT_MAX_REST_QUOTE_RESPONSE_BYTES,
        }
    }
}

/// Complete collection of REST quote snapshot results for a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestQuoteSnapshot {
    /// Generation identifier echoed from [`RestQuoteRequest`].
    pub configuration_generation: u64,
    /// Receipt timestamp when HTTP response retrieval completed.
    pub received_ts: UnixNanos,
    /// Symbol-specific quote outcomes in requested order.
    pub results: Vec<SymbolQuoteResult>,
}

/// Errors occurring during on-demand REST quote snapshot discovery.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RestQuoteError {
    /// Underlying transport or network failure (DNS, connect, body read).
    #[error("transport error on {provider}: {message}")]
    Transport {
        /// Venue where error occurred.
        provider: VenueId,
        /// Error message.
        message: String,
        /// Underlying error cause, if available.
        #[source]
        source: Option<ArcError>,
    },
    /// HTTP status failure returned by server (e.g. 400, 404, 500, 503).
    #[error("http status error on {provider}: status {status}, message: {message}")]
    HttpStatus {
        /// Venue where error occurred.
        provider: VenueId,
        /// HTTP status code.
        status: u16,
        /// Error message.
        message: String,
        /// Optional retry delay recommended by the server.
        retry_after: Option<Duration>,
        /// Underlying error cause, if available.
        #[source]
        source: Option<ArcError>,
    },
    /// Rate limit exceeded (HTTP 429 / 418).
    #[error("rate limit exceeded on {provider}")]
    RateLimit {
        /// Venue where error occurred.
        provider: VenueId,
        /// Optional retry delay recommended by the server.
        retry_after: Option<Duration>,
    },
    /// Request timeout / deadline exceeded.
    #[error("request timeout / deadline exceeded on {provider}")]
    Timeout {
        /// Venue where error occurred.
        provider: VenueId,
    },
    /// Request cancelled via cancellation token.
    #[error("request cancelled on {provider}")]
    Cancelled {
        /// Venue where error occurred.
        provider: VenueId,
    },
    /// Response body exceeded limits.
    #[error("response body exceeded limit on {provider}: max {max} bytes, actual {actual}")]
    ResponseTooLarge {
        /// Venue where error occurred.
        provider: VenueId,
        /// Configured maximum bytes.
        max: u32,
        /// Actual bytes encountered.
        actual: usize,
    },
    /// Invalid or unrecognized response envelope.
    #[error("invalid response envelope on {provider}: {reason}")]
    InvalidEnvelope {
        /// Venue where error occurred.
        provider: VenueId,
        /// Reason description.
        reason: String,
    },
    /// Malformed quote snapshot item.
    #[error("malformed quote item on {provider}: {reason}")]
    MalformedItem {
        /// Venue where error occurred.
        provider: VenueId,
        /// Reason description.
        reason: String,
    },
    /// Unsupported request scope or market family.
    #[error("unsupported scope on {provider}: {reason}")]
    UnsupportedScope {
        /// Venue where error occurred.
        provider: VenueId,
        /// Reason description.
        reason: String,
    },
    /// Requested symbols exceeded the provider's batch limit.
    #[error("batch limit exceeded on {provider}: max {max}, actual {actual}")]
    BatchLimitExceeded {
        /// Venue where error occurred.
        provider: VenueId,
        /// Configured maximum symbols per batch.
        max: usize,
        /// Actual requested symbols.
        actual: usize,
    },
    /// REST quote snapshot capability is unsupported for this provider.
    #[error("rest quote snapshots unsupported for {provider}: {reason}")]
    Unsupported {
        /// Venue where error occurred.
        provider: VenueId,
        /// Reason description.
        reason: String,
    },
}

/// REST quote snapshot capability profile exposed in [`crate::model::ProviderCapabilities`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RestQuoteCapability {
    /// REST quote snapshot is not supported for this provider.
    Unsupported {
        /// Reason why REST quotes are unsupported.
        reason: String,
    },
    /// On-demand REST quote snapshots are supported.
    Snapshot {
        /// Maximum number of symbols allowed in a single REST request.
        max_symbols_per_request: usize,
        /// Whether the endpoint supports querying multiple symbols at once.
        batch_mode: bool,
        /// Whether the endpoint reports top-of-book bid and ask sizes.
        sizes: bool,
        /// Source and precision of timestamps returned by the endpoint.
        timestamp_source: TimestampCapability,
        /// Default REST endpoint URL template or origin.
        default_endpoint: String,
    },
}

impl RestQuoteCapability {
    /// Creates an unsupported REST quote capability with an explanatory reason.
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported {
            reason: reason.into(),
        }
    }

    /// Creates a single-symbol REST quote snapshot capability.
    pub fn single(
        sizes: bool,
        timestamp_source: TimestampCapability,
        default_endpoint: impl Into<String>,
    ) -> Self {
        Self::Snapshot {
            max_symbols_per_request: 1,
            batch_mode: false,
            sizes,
            timestamp_source,
            default_endpoint: default_endpoint.into(),
        }
    }

    /// Creates a batch-capable REST quote snapshot capability.
    pub fn batch(
        max_symbols_per_request: usize,
        sizes: bool,
        timestamp_source: TimestampCapability,
        default_endpoint: impl Into<String>,
    ) -> Self {
        Self::Snapshot {
            max_symbols_per_request,
            batch_mode: true,
            sizes,
            timestamp_source,
            default_endpoint: default_endpoint.into(),
        }
    }

    /// Returns `true` if REST quote snapshots are supported.
    pub fn is_supported(&self) -> bool {
        match self {
            Self::Snapshot {
                default_endpoint, ..
            } => !default_endpoint.is_empty(),
            Self::Unsupported { .. } => false,
        }
    }
}

/// Asynchronous provider interface for on-demand REST quote snapshot discovery.
pub trait RestQuoteSource: Send + Sync {
    /// Fetches on-demand top-of-book quote snapshots matching the given request.
    fn fetch<'a>(
        &'a self,
        request: &'a RestQuoteRequest,
    ) -> BoxFuture<'a, Result<RestQuoteSnapshot, RestQuoteError>>;
}
