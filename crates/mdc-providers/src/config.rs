//! Shared configuration validation helpers for exchange connector builders.
//!
//! Enforces bounded invariants at configuration and builder boundaries:
//! - `symbols_per_connection > 0`
//! - `resubscribe_batch_size > 0`
//! - `resync_buffer_capacity > 0`
//! - Non-empty symbol subscriptions
//! - Venue-specific depth limits

use mdc_core::{MdcError, Symbol, SymbolTag};

/// Validates that a configured symbol count is non-zero.
pub fn validate_symbol_count(count: usize) -> Result<(), MdcError> {
    if count == 0 {
        return Err(MdcError::Config("symbols list cannot be empty".to_string()));
    }
    Ok(())
}

/// Validates that symbols list is non-empty.
pub fn validate_symbols(symbols: &[(Symbol, SymbolTag)]) -> Result<(), MdcError> {
    validate_symbol_count(symbols.len())
}

/// Validates that `symbols_per_connection` is strictly greater than zero.
pub fn validate_symbols_per_connection(symbols_per_connection: usize) -> Result<(), MdcError> {
    if symbols_per_connection == 0 {
        return Err(MdcError::Config(
            "symbols_per_connection must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

/// Validates that `resubscribe_batch_size` is strictly greater than zero.
pub fn validate_resubscribe_batch_size(batch_size: usize) -> Result<(), MdcError> {
    if batch_size == 0 {
        return Err(MdcError::Config(
            "resubscribe_batch_size must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

/// Validates that `resync_buffer_capacity` / `max_buffer` is strictly greater than zero.
pub fn validate_resync_buffer_capacity(capacity: usize) -> Result<(), MdcError> {
    if capacity == 0 {
        return Err(MdcError::Config(
            "resync_buffer_capacity must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

/// Validates that depth is within the venue's discrete allowed set.
pub fn validate_depth_in_set(depth: usize, allowed: &[usize], venue: &str) -> Result<(), MdcError> {
    if !allowed.contains(&depth) {
        return Err(MdcError::Config(format!(
            "invalid depth {depth} for {venue}; must be one of {allowed:?}"
        )));
    }
    Ok(())
}

/// Validates that depth is within the venue's continuous allowed range `[min, max]`.
pub fn validate_depth_range(
    depth: usize,
    min: usize,
    max: usize,
    venue: &str,
) -> Result<(), MdcError> {
    if depth < min || depth > max {
        return Err(MdcError::Config(format!(
            "invalid depth {depth} for {venue}; must be between {min} and {max}"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_symbols_empty_fails() {
        let empty: Vec<(Symbol, SymbolTag)> = Vec::new();
        assert!(validate_symbols(&empty).is_err());

        let non_empty = vec![(Symbol::from("BTCUSDT"), SymbolTag::default())];
        assert!(validate_symbols(&non_empty).is_ok());
    }

    #[test]
    fn test_validate_symbols_per_connection() {
        assert!(validate_symbols_per_connection(0).is_err());
        assert!(validate_symbols_per_connection(1).is_ok());
        assert!(validate_symbols_per_connection(25).is_ok());
    }

    #[test]
    fn test_validate_resubscribe_batch_size() {
        assert!(validate_resubscribe_batch_size(0).is_err());
        assert!(validate_resubscribe_batch_size(10).is_ok());
    }

    #[test]
    fn test_validate_resync_buffer_capacity() {
        assert!(validate_resync_buffer_capacity(0).is_err());
        assert!(validate_resync_buffer_capacity(1000).is_ok());
    }

    #[test]
    fn test_validate_depth_in_set() {
        let allowed = [10, 25, 100, 500, 1000];
        assert!(validate_depth_in_set(10, &allowed, "Kraken").is_ok());
        assert!(validate_depth_in_set(50, &allowed, "Kraken").is_err());
    }

    #[test]
    fn test_validate_depth_range() {
        assert!(validate_depth_range(100, 1, 1000, "Bitstamp").is_ok());
        assert!(validate_depth_range(0, 1, 1000, "Bitstamp").is_err());
        assert!(validate_depth_range(2000, 1, 1000, "Bitstamp").is_err());
    }
}
