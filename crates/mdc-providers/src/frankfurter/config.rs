//! Configuration and builder for the Frankfurter ECB reference rate connector.

use std::collections::HashSet;
use std::time::Duration;

use thiserror::Error;
use url::Url;

/// Default Frankfurter API base URL.
pub const DEFAULT_FRANKFURTER_BASE_URL: &str = "https://api.frankfurter.dev";

/// Default fixed provider identifier (`"ecb"`).
pub const DEFAULT_PROVIDER: &str = "ecb";

/// Default base currency code (`"EUR"`).
pub const DEFAULT_BASE_CURRENCY: &str = "EUR";

/// Default polling interval (3600 seconds = 1 hour).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(3600);

/// Default HTTP request timeout (10 seconds).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Default HTTP body size cap (1 MiB = 1,048,576 bytes).
pub const DEFAULT_BODY_CAP_BYTES: usize = 1024 * 1024;

/// Maximum number of distinct quote codes supported in a single configuration.
pub const MAX_QUOTE_CODES: usize = 100;

/// Configuration errors for the Frankfurter connector.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// Base URL is invalid.
    #[error("invalid base URL: {0}")]
    InvalidBaseUrl(String),

    /// Base currency code must be a 3-letter uppercase ASCII code (e.g. 'EUR').
    #[error("invalid base currency '{0}': must be 3 uppercase ASCII letters")]
    InvalidBaseCurrency(String),

    /// At least one quote currency must be configured.
    #[error("at least one quote currency must be configured")]
    MissingQuotes,

    /// Number of configured quotes exceeds maximum limit of 100.
    #[error("too many quote currencies: configured {0}, maximum allowed is 100")]
    TooManyQuotes(usize),

    /// Duplicate quote code detected.
    #[error("duplicate quote currency '{0}'")]
    DuplicateQuote(String),

    /// Invalid quote currency code format (must be 3 uppercase ASCII letters).
    #[error("invalid quote currency '{0}': must be 3 uppercase ASCII letters")]
    InvalidQuote(String),

    /// Poll interval must be greater than zero.
    #[error("poll interval must be greater than zero")]
    InvalidPollInterval,

    /// Request timeout must be greater than zero.
    #[error("request timeout must be greater than zero")]
    InvalidTimeout,

    /// Body size cap must be greater than zero.
    #[error("body size cap must be greater than zero")]
    InvalidBodyCap,
}

/// Bounded configuration for the Frankfurter ECB reference rate connector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrankfurterConfig {
    /// REST API base URL.
    pub base_url: Url,
    /// Fixed provider identifier (always `"ecb"`).
    pub provider: String,
    /// Base currency code (e.g. `"EUR"`).
    pub base_currency: String,
    /// Configured distinct quote codes (1–100 items).
    pub quote_currencies: Vec<String>,
    /// Polling interval.
    pub poll_interval: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Maximum response body size in bytes.
    pub body_cap_bytes: usize,
}

impl Default for FrankfurterConfig {
    fn default() -> Self {
        #[allow(clippy::expect_used)]
        let base_url = Url::parse(DEFAULT_FRANKFURTER_BASE_URL).expect("valid default base URL");
        Self {
            base_url,
            provider: DEFAULT_PROVIDER.to_string(),
            base_currency: DEFAULT_BASE_CURRENCY.to_string(),
            quote_currencies: vec!["USD".to_string(), "GBP".to_string()],
            poll_interval: DEFAULT_POLL_INTERVAL,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            body_cap_bytes: DEFAULT_BODY_CAP_BYTES,
        }
    }
}

impl FrankfurterConfig {
    /// Creates a new builder for configuring a [`FrankfurterConfig`].
    pub fn builder() -> FrankfurterConfigBuilder {
        FrankfurterConfigBuilder::new()
    }

    /// Validates all configuration constraints.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.base_url.scheme() != "http" && self.base_url.scheme() != "https" {
            return Err(ConfigError::InvalidBaseUrl(
                "scheme must be http or https".to_string(),
            ));
        }

        if !is_valid_currency_code(&self.base_currency) {
            return Err(ConfigError::InvalidBaseCurrency(self.base_currency.clone()));
        }

        validate_quote_currencies(&self.quote_currencies)?;

        if self.poll_interval.is_zero() {
            return Err(ConfigError::InvalidPollInterval);
        }

        if self.request_timeout.is_zero() {
            return Err(ConfigError::InvalidTimeout);
        }

        if self.body_cap_bytes == 0 {
            return Err(ConfigError::InvalidBodyCap);
        }

        Ok(())
    }
}

/// Validates a list of quote currency codes (non-empty, <= MAX_QUOTE_CODES, 3-letter ASCII uppercase, no duplicates).
pub fn validate_quote_currencies(quotes: &[String]) -> Result<(), ConfigError> {
    if quotes.is_empty() {
        return Err(ConfigError::MissingQuotes);
    }

    if quotes.len() > MAX_QUOTE_CODES {
        return Err(ConfigError::TooManyQuotes(quotes.len()));
    }

    let mut seen = HashSet::with_capacity(quotes.len());
    for quote in quotes {
        if !is_valid_currency_code(quote) {
            return Err(ConfigError::InvalidQuote(quote.clone()));
        }
        if !seen.insert(quote) {
            return Err(ConfigError::DuplicateQuote(quote.clone()));
        }
    }

    Ok(())
}

/// Checks whether a currency code is a valid 3-letter ASCII uppercase string.
pub fn is_valid_currency_code(code: &str) -> bool {
    code.len() == 3 && code.chars().all(|c| c.is_ascii_uppercase())
}

/// Fluent builder for [`FrankfurterConfig`].
#[derive(Debug, Clone)]
pub struct FrankfurterConfigBuilder {
    config: FrankfurterConfig,
    base_url_error: Option<ConfigError>,
}

impl Default for FrankfurterConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrankfurterConfigBuilder {
    /// Creates a new builder initialized with default settings.
    pub fn new() -> Self {
        Self {
            config: FrankfurterConfig::default(),
            base_url_error: None,
        }
    }

    /// Sets the API base URL.
    pub fn base_url(mut self, url: impl AsRef<str>) -> Self {
        let s = url.as_ref();
        match Url::parse(s) {
            Ok(parsed) => {
                self.config.base_url = parsed;
                self.base_url_error = None;
            }
            Err(e) => {
                self.base_url_error = Some(ConfigError::InvalidBaseUrl(e.to_string()));
            }
        }
        self
    }

    /// Sets the base currency code (default: `"EUR"`).
    pub fn base_currency(mut self, base: impl Into<String>) -> Self {
        self.config.base_currency = base.into();
        self
    }

    /// Sets the full list of quote currency codes.
    pub fn quote_currencies(mut self, quotes: Vec<String>) -> Self {
        self.config.quote_currencies = quotes;
        self
    }

    /// Appends a quote currency code.
    pub fn add_quote_currency(mut self, quote: impl Into<String>) -> Self {
        self.config.quote_currencies.push(quote.into());
        self
    }

    /// Sets the poll interval (default: 3600s).
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.config.poll_interval = interval;
        self
    }

    /// Sets the request timeout (default: 10s).
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Sets the maximum response body size in bytes (default: 1 MiB).
    pub fn body_cap_bytes(mut self, cap: usize) -> Self {
        self.config.body_cap_bytes = cap;
        self
    }

    /// Validates and constructs the [`FrankfurterConfig`].
    pub fn build(self) -> Result<FrankfurterConfig, ConfigError> {
        if let Some(err) = self.base_url_error {
            return Err(err);
        }
        self.config.validate()?;
        Ok(self.config)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = FrankfurterConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.base_currency, "EUR");
        assert_eq!(config.quote_currencies, vec!["USD", "GBP"]);
    }

    #[test]
    fn test_invalid_base_currency_rejected() {
        let config = FrankfurterConfig::builder().base_currency("euro").build();
        assert_eq!(
            config.unwrap_err(),
            ConfigError::InvalidBaseCurrency("euro".into())
        );
    }

    #[test]
    fn test_empty_quotes_rejected() {
        let config = FrankfurterConfig::builder()
            .quote_currencies(vec![])
            .build();
        assert_eq!(config.unwrap_err(), ConfigError::MissingQuotes);
    }

    #[test]
    fn test_duplicate_quotes_rejected() {
        let config = FrankfurterConfig::builder()
            .quote_currencies(vec!["USD".into(), "USD".into()])
            .build();
        assert_eq!(
            config.unwrap_err(),
            ConfigError::DuplicateQuote("USD".into())
        );
    }

    #[test]
    fn test_too_many_quotes_rejected() {
        let quotes = (0..101).map(|i| format!("Q{:02}", i)).collect();
        let config = FrankfurterConfig::builder()
            .quote_currencies(quotes)
            .build();
        assert_eq!(config.unwrap_err(), ConfigError::TooManyQuotes(101));
    }

    #[test]
    fn test_zero_interval_rejected() {
        let config = FrankfurterConfig::builder()
            .poll_interval(Duration::ZERO)
            .build();
        assert_eq!(config.unwrap_err(), ConfigError::InvalidPollInterval);
    }

    #[test]
    fn test_invalid_quote_code_rejected() {
        let res = validate_quote_currencies(&[String::from("usd")]);
        assert_eq!(res.unwrap_err(), ConfigError::InvalidQuote("usd".into()));

        let res = validate_quote_currencies(&[String::from("USDT")]);
        assert_eq!(res.unwrap_err(), ConfigError::InvalidQuote("USDT".into()));
    }
}
