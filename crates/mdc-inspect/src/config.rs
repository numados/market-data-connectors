//! Bounded application constants and validated session configuration.

use std::time::Duration;

use mdc_core::Symbol;

use crate::display::DisplayMode;
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
    /// Provider-native symbols, in operator order, trimmed and bounded by
    /// the provider adapter.
    pub symbols: Vec<Symbol>,
    /// Base currency for reference-rate sessions; `symbols` holds the quotes.
    pub base: Option<Symbol>,
}

impl SessionConfig {
    /// Validates complete raw provider-native entries before a session starts.
    pub fn new(provider: ProviderKind, raw_symbols: &[String]) -> Result<Self, Error> {
        let entries = Self::validated_entries(provider, raw_symbols, true)?;
        let (base, symbols) = if provider.presentation().mode == DisplayMode::ReferenceRate {
            let mut entries = entries.into_iter();
            (entries.next(), entries.collect())
        } else {
            (None, entries)
        };
        Ok(Self {
            provider,
            symbols,
            base,
        })
    }

    /// Validates draft or complete provider-native entries and returns their
    /// trimmed canonical spelling.
    pub(crate) fn validated_entries(
        provider: ProviderKind,
        raw_symbols: &[String],
        complete: bool,
    ) -> Result<Vec<Symbol>, Error> {
        let bounds = provider.session_bounds();
        let mode = provider.presentation().mode;
        if raw_symbols.is_empty() {
            if complete {
                return Err(entry_error(
                    provider,
                    "symbols",
                    "<empty>",
                    "at least one symbol is required (provider-native symbols)",
                ));
            }
            return Ok(Vec::new());
        }

        let mut entries: Vec<Symbol> = Vec::with_capacity(raw_symbols.len());
        for (index, raw) in raw_symbols.iter().enumerate() {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(entry_error(
                    provider,
                    "symbols",
                    raw,
                    "symbol must not be empty after trimming",
                ));
            }
            if trimmed.chars().any(char::is_control) {
                return Err(entry_error(
                    provider,
                    "symbols",
                    trimmed,
                    "symbol contains terminal control characters",
                ));
            }

            let field = match mode {
                DisplayMode::ReferenceRate if index == 0 => "base",
                DisplayMode::ReferenceRate => "quote",
                _ => "symbols",
            };
            if mode == DisplayMode::ReferenceRate
                && (trimmed.len() != 3 || !trimmed.bytes().all(|byte| byte.is_ascii_uppercase()))
            {
                return Err(entry_error(
                    provider,
                    field,
                    trimmed,
                    "expected 3 uppercase ASCII letters",
                ));
            }
            if mode == DisplayMode::SyntheticBook
                && (trimmed.len() != 6 || !trimmed.bytes().all(|byte| byte.is_ascii_uppercase()))
            {
                return Err(entry_error(
                    provider,
                    "symbols",
                    trimmed,
                    "expected 6 uppercase ASCII letters (for example EURUSD)",
                ));
            }

            let symbol = Symbol::from(trimmed);
            if let Some(existing) = entries.iter().position(|entry| entry == &symbol) {
                if mode == DisplayMode::ReferenceRate && existing == 0 {
                    return Err(entry_error(
                        provider,
                        "quote",
                        trimmed,
                        &format!("must differ from base '{}'", escaped(trimmed)),
                    ));
                }
                if mode == DisplayMode::ReferenceRate {
                    return Err(Error::Config(format!(
                        "{} duplicate quote '{}' after trimming (field 'quotes'; quotes must be distinct)",
                        provider.label(),
                        escaped(trimmed)
                    )));
                }
                return Err(Error::Config(format!(
                    "{} duplicate symbol '{}' after trimming",
                    provider.label(),
                    escaped(trimmed)
                )));
            }
            entries.push(symbol);
        }

        if mode == DisplayMode::ReferenceRate {
            let quote_count = entries.len().saturating_sub(1);
            if complete && quote_count == 0 {
                return Err(Error::Config(format!(
                    "{} field 'quotes' requires 1–{} quote currencies after base; got 0",
                    provider.label(),
                    bounds.max_symbols
                )));
            }
            if quote_count > bounds.max_symbols {
                return Err(Error::Config(format!(
                    "{} field 'quotes' accepts at most {} quote currencies; got {}",
                    provider.label(),
                    bounds.max_symbols,
                    quote_count
                )));
            }
        } else if entries.len() > bounds.max_symbols {
            return Err(Error::Config(format!(
                "{} field 'symbols' accepts at most {} provider-native symbols; got {}",
                provider.label(),
                bounds.max_symbols,
                entries.len()
            )));
        }
        Ok(entries)
    }

    /// Returns the base before the provider-native symbols.
    pub(crate) fn entries(&self) -> impl Iterator<Item = &Symbol> {
        self.base.iter().chain(self.symbols.iter())
    }

    /// Revalidates public configuration fields at the adapter boundary.
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let raw: Vec<String> = self.entries().map(ToString::to_string).collect();
        let validated = Self::new(self.provider, &raw)?;
        if validated != *self {
            return Err(Error::Config(format!(
                "{} fields 'base' and 'symbols' do not match the provider format",
                self.provider.label()
            )));
        }
        Ok(())
    }
}

fn escaped(value: &str) -> String {
    value.chars().flat_map(char::escape_default).collect()
}

fn entry_error(provider: ProviderKind, field: &str, value: &str, reason: &str) -> Error {
    Error::Config(format!(
        "{} field '{field}' rejected value '{}': {reason}",
        provider.label(),
        escaped(value)
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
