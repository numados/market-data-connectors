//! Bounded application constants and validated session configuration.

use std::time::Duration;

use mdc_core::Symbol;

use crate::error::Error;
use crate::provider::ProviderKind;

/// Loss-intolerant delivery channel capacity (PRD 31 §3.4).
pub const CHANNEL_CAPACITY: usize = 4096;

/// Bounded integrity-alert history entries retained per session.
pub const ALERT_HISTORY: usize = 256;

/// Bounded in-memory diagnostics ring records.
pub const DIAGNOSTICS_RING: usize = 256;

/// Fixed terminal render cadence.
pub const RENDER_CADENCE: Duration = Duration::from_millis(100);

/// Bounded session shutdown wait before the owned task is aborted and reaped.
///
/// Exceeds the provider supervisor's two-second shutdown grace so orderly
/// teardown wins and forced reaping stays the exception.
pub const SHUTDOWN_BOUND: Duration = Duration::from_secs(3);

/// A validated inspector session configuration.
///
/// Configuration is established before a session starts; changing it later
/// requires stopping and reaping the active session first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    /// Selected provider.
    pub provider: ProviderKind,
    /// Provider-native symbols, in operator order, deduplicated after
    /// trimming, bounded by the provider adapter.
    pub symbols: Vec<Symbol>,
}

impl SessionConfig {
    /// Validates raw provider-native symbol entries against the provider's
    /// adapter bounds. Offline: no network, no terminal.
    pub fn new(provider: ProviderKind, raw_symbols: &[String]) -> Result<Self, Error> {
        let bounds = provider.session_bounds();
        let mut symbols = Vec::with_capacity(raw_symbols.len());
        for raw in raw_symbols {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(Error::Config(
                    "symbols must not be empty after trimming".to_string(),
                ));
            }
            if trimmed.chars().any(|c| c.is_control()) {
                return Err(Error::Config(format!(
                    "symbol '{trimmed}' contains terminal control characters"
                )));
            }
            let symbol = Symbol::from(trimmed);
            if symbols.contains(&symbol) {
                return Err(Error::Config(format!(
                    "duplicate symbol '{trimmed}' after trimming"
                )));
            }
            symbols.push(symbol);
        }
        if symbols.is_empty() {
            return Err(Error::Config(
                "at least one symbol is required to start a session".to_string(),
            ));
        }
        if symbols.len() > bounds.max_symbols {
            return Err(Error::Config(format!(
                "{} accepts at most {} symbols per session; got {}",
                provider.label(),
                bounds.max_symbols,
                symbols.len()
            )));
        }
        Ok(Self { provider, symbols })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[cfg(feature = "binance")]
    #[test]
    fn accepts_distinct_symbols_and_preserves_spelling() {
        let config = SessionConfig::new(ProviderKind::Binance, &strings(&["BTCUSDT", " ETHUSDT "]))
            .expect("valid configuration");
        assert_eq!(
            config.symbols,
            vec![Symbol::from("BTCUSDT"), Symbol::from("ETHUSDT")]
        );
    }

    #[cfg(feature = "binance")]
    #[test]
    fn rejects_empty_symbol_list_and_entries() {
        let empty = SessionConfig::new(ProviderKind::Binance, &[]);
        assert!(empty.is_err());
        let blank = SessionConfig::new(ProviderKind::Binance, &strings(&["  "]));
        assert!(blank.is_err());
    }

    #[cfg(feature = "binance")]
    #[test]
    fn rejects_duplicates_after_trimming() {
        let duplicated =
            SessionConfig::new(ProviderKind::Binance, &strings(&["BTCUSDT", " BTCUSDT "]));
        let error = duplicated.expect_err("duplicate symbols must be rejected");
        assert!(error.to_string().contains("duplicate symbol"));
    }

    #[cfg(feature = "binance")]
    #[test]
    fn rejects_control_characters() {
        let controlled = SessionConfig::new(ProviderKind::Binance, &strings(&["BTC\tUSDT"]));
        assert!(controlled.is_err());
    }

    #[cfg(feature = "binance")]
    #[test]
    fn enforces_provider_symbol_cap() {
        let many: Vec<String> = (0..26).map(|index| format!("SYM{index}USDT")).collect();
        let error = SessionConfig::new(ProviderKind::Binance, &many)
            .expect_err("26 symbols must exceed the Binance cap");
        assert!(error.to_string().contains("at most 25"));
    }
}
