//! VWD (Infront) FX REST polling market data connector.
//!
//! # Paid Subscription & Credentials Requirement
//!
//! VWD / Infront is a commercial data provider and has **no public or demo unauthenticated endpoints**.
//! Every request requires customer contract parameters:
//! - An account path segment (`account_path`),
//! - An HTTP Basic username,
//! - An HTTP Basic password.
//!
//! This crate ships **no default credentials** and **never logs or displays credentials in debug output**.
//! An attempt to construct [`VwdConfig`] or [`VwdConnector`] without non-empty credentials returns
//! [`ConfigError::MissingCredentials`] and performs zero network I/O.
//!
//! # Protocol & Integrity
//!
//! - **Endpoint**: `GET /{account_path}/JSON/QUOTE/SNAPQUOTE/{keys}` on `https://rest-interface.vwd.com`.
//! - **Polling Cadence**: Configurable interval (default 5 seconds, ±10% jitter, no overlapping requests).
//! - **Snapshots**: Generates continuous single-level [`L2Snapshot`](mdc_core::BookKind::Snapshot) events with configurable indicative size.
//! - **Fail Closed**: Any transport error, HTTP non-2xx status, JSON parse error, missing quote, or zero price emits
//!   [`IntegrityAlert::SourceError`](mdc_core::IntegrityAlert::SourceError).
//! - **Recovery**: On returning to fresh quotes after a failure, emits [`IntegrityAlert::ResyncCompleted`](mdc_core::IntegrityAlert::ResyncCompleted).
//! - **Provenence**: `exchange_ts` is taken strictly from the minimum of used bid and ask timestamps, never from `last.dateTime`.
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use mdc_providers::vwd::{VwdConfig, VwdCredentials, VwdSymbol};
//!
//! let config = VwdConfig::builder()
//!     .account_path("my-customer-account")
//!     .credentials(VwdCredentials::new("my_user", "my_pass"))
//!     .add_symbol(VwdSymbol::with_inverse("EURUSD"))
//!     .poll_interval(Duration::from_secs(5))
//!     .build()
//!     .expect("valid configuration");
//! ```

pub mod config;
pub mod normalize;
pub mod raw;

pub use config::{
    ConfigError, DEFAULT_INDICATIVE_SIZE, DEFAULT_POLL_INTERVAL, DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_SIGNIFICANT_DIGITS, DEFAULT_VWD_BASE_URL, VwdConfig, VwdConfigBuilder, VwdCredentials,
    VwdSymbol,
};
pub use normalize::{
    NormalizedQuote, build_instrument_key, build_inverse_instrument_key, normalize_symbol_quote,
    parse_rfc3339_nanos, round_to_significant_digits,
};
pub use raw::{RawQuote, RawRow, RawSnapQuote, parse_snap_quotes};
