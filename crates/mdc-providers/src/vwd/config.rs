//! Configuration and credentials for the VWD (Infront) FX REST polling connector.

use std::fmt;
use std::time::Duration;

use mdc_core::Symbol;
use rust_decimal::Decimal;
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;
use url::Url;

/// Default VWD REST base URL.
pub const DEFAULT_VWD_BASE_URL: &str = "https://rest-interface.vwd.com";

/// Default poll interval (5 seconds).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Default request timeout (10 seconds).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Default indicative FX order book volume (200,000,000.00).
pub const DEFAULT_INDICATIVE_SIZE: Decimal = Decimal::from_parts(200_000_000, 0, 0, false, 0);

/// Default significant digits for inverted rates (6 digits).
pub const DEFAULT_SIGNIFICANT_DIGITS: u32 = 6;

/// Configuration errors for VWD connector.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// Required credentials (account path, username, or password) are missing or empty.
    #[error("missing required VWD credentials (account_path, username, or password)")]
    MissingCredentials,

    /// No symbols configured for polling.
    #[error("at least one symbol must be configured for polling")]
    MissingSymbols,

    /// Configured symbol is invalid (must be 6-letter ASCII currency pair, e.g. 'EURUSD').
    #[error("invalid symbol '{0}': must be 6-letter ASCII currency pair")]
    InvalidSymbol(String),

    /// Significant digits configuration must be greater than zero.
    #[error("significant digits must be > 0")]
    InvalidSignificantDigits,

    /// Indicative volume size must be positive.
    #[error("indicative size must be positive")]
    InvalidIndicativeSize,

    /// Poll interval must be greater than zero.
    #[error("poll interval must be greater than zero")]
    InvalidInterval,

    /// Request timeout must be greater than zero.
    #[error("request timeout must be greater than zero")]
    InvalidTimeout,

    /// Malformed base URL.
    #[error("invalid base URL: {0}")]
    InvalidUrl(String),

    /// Scale factor must be positive.
    #[error("scale factor must be positive")]
    InvalidScale,
}

/// Secret authentication credentials for VWD Infront service.
#[derive(Clone)]
pub struct VwdCredentials {
    /// Infront HTTP Basic username.
    pub username: SecretString,
    /// Infront HTTP Basic password.
    pub password: SecretString,
}

impl VwdCredentials {
    /// Creates credentials from plain string slices.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: SecretString::new(username.into().into_boxed_str()),
            password: SecretString::new(password.into().into_boxed_str()),
        }
    }
}

impl fmt::Debug for VwdCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VwdCredentials")
            .field("username", &"[REDACTED]")
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// Per-symbol configuration options for VWD FX pairs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VwdSymbol {
    /// Canonical 6-letter symbol, e.g. `"EURUSD"`.
    pub symbol: Symbol,
    /// Whether to query and compute an inverse quote (e.g. `USDEUR`) if the direct quote is missing.
    pub allow_inverse: bool,
    /// Optional scaling factor applied to bid/ask prices (e.g. `0.01` for legacy `DKKCHF`).
    pub scale: Option<Decimal>,
}

impl VwdSymbol {
    /// Creates a direct symbol without inverse fallback or scaling.
    pub fn direct(symbol: impl Into<Symbol>) -> Self {
        Self {
            symbol: symbol.into(),
            allow_inverse: false,
            scale: None,
        }
    }

    /// Creates a symbol that allows inverse quotes if the direct quote is missing.
    pub fn with_inverse(symbol: impl Into<Symbol>) -> Self {
        Self {
            symbol: symbol.into(),
            allow_inverse: true,
            scale: None,
        }
    }

    /// Creates a symbol with scaling factor.
    pub fn with_scale(symbol: impl Into<Symbol>, scale: Decimal) -> Self {
        Self {
            symbol: symbol.into(),
            allow_inverse: false,
            scale: Some(scale),
        }
    }
}

/// Complete configuration for VWD REST polling connector.
#[derive(Clone)]
pub struct VwdConfig {
    /// Base URL for the VWD REST API.
    pub base_url: Url,
    /// Customer account path segment (e.g. `"customer-account"`).
    pub account_path: SecretString,
    /// Infront HTTP Basic authentication credentials.
    pub credentials: VwdCredentials,
    /// Polling cadence interval (default 5s).
    pub poll_interval: Duration,
    /// HTTP request timeout (default 10s).
    pub request_timeout: Duration,
    /// Indicative volume synthesized for the single-level L2 snapshot (default 200,000,000).
    pub indicative_size: Decimal,
    /// Number of significant figures to round inverse quote calculations (default 6).
    pub significant_digits: u32,
    /// Configured FX currency pairs to poll.
    pub symbols: Vec<VwdSymbol>,
    /// Whether to also emit a synthetic `BookTicker` event alongside each `L2Snapshot`.
    pub emit_book_ticker: bool,
}

impl fmt::Debug for VwdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VwdConfig")
            .field("base_url", &self.base_url.as_str())
            .field("account_path", &"[REDACTED]")
            .field("credentials", &self.credentials)
            .field("poll_interval", &self.poll_interval)
            .field("request_timeout", &self.request_timeout)
            .field("indicative_size", &self.indicative_size)
            .field("significant_digits", &self.significant_digits)
            .field("symbols", &self.symbols)
            .field("emit_book_ticker", &self.emit_book_ticker)
            .finish()
    }
}

impl VwdConfig {
    /// Creates a new builder initialized with defaults.
    pub fn builder() -> VwdConfigBuilder {
        VwdConfigBuilder::default()
    }

    /// Validates configuration parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.account_path.expose_secret().trim().is_empty()
            || self.credentials.username.expose_secret().trim().is_empty()
            || self.credentials.password.expose_secret().trim().is_empty()
        {
            return Err(ConfigError::MissingCredentials);
        }

        if self.symbols.is_empty() {
            return Err(ConfigError::MissingSymbols);
        }

        for sym in &self.symbols {
            let s = sym.symbol.as_str();
            if s.len() != 6 || !s.is_ascii() {
                return Err(ConfigError::InvalidSymbol(s.to_string()));
            }
            if let Some(scale) = sym.scale {
                if scale <= Decimal::ZERO {
                    return Err(ConfigError::InvalidScale);
                }
            }
        }

        if self.significant_digits == 0 {
            return Err(ConfigError::InvalidSignificantDigits);
        }

        if self.indicative_size <= Decimal::ZERO {
            return Err(ConfigError::InvalidIndicativeSize);
        }

        if self.poll_interval.is_zero() {
            return Err(ConfigError::InvalidInterval);
        }

        if self.request_timeout.is_zero() {
            return Err(ConfigError::InvalidTimeout);
        }

        Ok(())
    }
}

/// Builder for constructing a validated [`VwdConfig`].
pub struct VwdConfigBuilder {
    base_url: Option<Url>,
    account_path: Option<SecretString>,
    credentials: Option<VwdCredentials>,
    poll_interval: Duration,
    request_timeout: Duration,
    indicative_size: Decimal,
    significant_digits: u32,
    symbols: Vec<VwdSymbol>,
    emit_book_ticker: bool,
}

impl Default for VwdConfigBuilder {
    fn default() -> Self {
        #[allow(clippy::expect_used)]
        let base_url = Url::parse(DEFAULT_VWD_BASE_URL).expect("default URL must be valid");
        Self {
            base_url: Some(base_url),
            account_path: None,
            credentials: None,
            poll_interval: DEFAULT_POLL_INTERVAL,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            indicative_size: DEFAULT_INDICATIVE_SIZE,
            significant_digits: DEFAULT_SIGNIFICANT_DIGITS,
            symbols: Vec::new(),
            emit_book_ticker: false,
        }
    }
}

impl VwdConfigBuilder {
    /// Sets the base URL.
    pub fn base_url(mut self, url: Url) -> Self {
        self.base_url = Some(url);
        self
    }

    /// Sets the customer account path segment.
    pub fn account_path(mut self, path: impl Into<String>) -> Self {
        self.account_path = Some(SecretString::new(path.into().into_boxed_str()));
        self
    }

    /// Sets the credentials.
    pub fn credentials(mut self, credentials: VwdCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Sets the polling interval.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Sets the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Sets the indicative book volume size.
    pub fn indicative_size(mut self, size: Decimal) -> Self {
        self.indicative_size = size;
        self
    }

    /// Sets the significant digits for inverse quotes.
    pub fn significant_digits(mut self, digits: u32) -> Self {
        self.significant_digits = digits;
        self
    }

    /// Adds a symbol to poll.
    pub fn add_symbol(mut self, symbol: VwdSymbol) -> Self {
        self.symbols.push(symbol);
        self
    }

    /// Sets all symbols to poll.
    pub fn symbols(mut self, symbols: impl IntoIterator<Item = VwdSymbol>) -> Self {
        self.symbols = symbols.into_iter().collect();
        self
    }

    /// Sets whether to emit synthetic `BookTicker` events.
    pub fn emit_book_ticker(mut self, emit: bool) -> Self {
        self.emit_book_ticker = emit;
        self
    }

    /// Validates and builds the `VwdConfig`.
    pub fn build(self) -> Result<VwdConfig, ConfigError> {
        let account_path = self.account_path.ok_or(ConfigError::MissingCredentials)?;
        let credentials = self.credentials.ok_or(ConfigError::MissingCredentials)?;
        let base_url = self
            .base_url
            .ok_or_else(|| ConfigError::InvalidUrl("missing base url".into()))?;

        let config = VwdConfig {
            base_url,
            account_path,
            credentials,
            poll_interval: self.poll_interval,
            request_timeout: self.request_timeout,
            indicative_size: self.indicative_size,
            significant_digits: self.significant_digits,
            symbols: self.symbols,
            emit_book_ticker: self.emit_book_ticker,
        };

        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_config_debug_redacts_secrets() {
        let secret_account = "super_secret_account";
        let secret_user = "secret_user";
        let secret_pass = "secret_password";

        let config = VwdConfig::builder()
            .account_path(secret_account)
            .credentials(VwdCredentials::new(secret_user, secret_pass))
            .add_symbol(VwdSymbol::direct("EURUSD"))
            .build()
            .unwrap();

        let debug_str = format!("{config:?}");
        assert!(!debug_str.contains(secret_account));
        assert!(!debug_str.contains(secret_user));
        assert!(!debug_str.contains(secret_pass));
        assert!(debug_str.contains("[REDACTED]"));
    }

    #[test]
    fn test_missing_credentials_fails_validation() {
        let res = VwdConfig::builder()
            .account_path("")
            .credentials(VwdCredentials::new("user", "pass"))
            .add_symbol(VwdSymbol::direct("EURUSD"))
            .build();
        assert_eq!(res.unwrap_err(), ConfigError::MissingCredentials);

        let res = VwdConfig::builder()
            .account_path("acct")
            .credentials(VwdCredentials::new("", "pass"))
            .add_symbol(VwdSymbol::direct("EURUSD"))
            .build();
        assert_eq!(res.unwrap_err(), ConfigError::MissingCredentials);
    }

    #[test]
    fn test_missing_symbols_fails_validation() {
        let res = VwdConfig::builder()
            .account_path("acct")
            .credentials(VwdCredentials::new("user", "pass"))
            .build();
        assert_eq!(res.unwrap_err(), ConfigError::MissingSymbols);
    }

    #[test]
    fn test_invalid_symbol_fails_validation() {
        let res = VwdConfig::builder()
            .account_path("acct")
            .credentials(VwdCredentials::new("user", "pass"))
            .add_symbol(VwdSymbol::direct("INVALID_PAIR"))
            .build();
        assert_eq!(
            res.unwrap_err(),
            ConfigError::InvalidSymbol("INVALID_PAIR".to_string())
        );
    }

    #[test]
    fn test_valid_builder_with_defaults() {
        let config = VwdConfig::builder()
            .account_path("acct")
            .credentials(VwdCredentials::new("user", "pass"))
            .add_symbol(VwdSymbol::with_inverse("EURUSD"))
            .build()
            .unwrap();

        assert_eq!(config.poll_interval, DEFAULT_POLL_INTERVAL);
        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(config.indicative_size, DEFAULT_INDICATIVE_SIZE);
        assert_eq!(config.significant_digits, DEFAULT_SIGNIFICANT_DIGITS);
        assert!(!config.emit_book_ticker);
        assert_eq!(config.symbols.len(), 1);
        assert!(config.symbols[0].allow_inverse);
    }

    #[test]
    fn test_invalid_scale_fails_validation() {
        let res = VwdConfig::builder()
            .account_path("acct")
            .credentials(VwdCredentials::new("user", "pass"))
            .add_symbol(VwdSymbol::with_scale("EURUSD", Decimal::ZERO))
            .build();
        assert_eq!(res.unwrap_err(), ConfigError::InvalidScale);

        let res = VwdConfig::builder()
            .account_path("acct")
            .credentials(VwdCredentials::new("user", "pass"))
            .add_symbol(VwdSymbol::with_scale("EURUSD", Decimal::from(-1)))
            .build();
        assert_eq!(res.unwrap_err(), ConfigError::InvalidScale);
    }
}
