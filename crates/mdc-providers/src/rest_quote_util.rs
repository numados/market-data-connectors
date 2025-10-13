//! Shared helpers and utilities for REST quote snapshot decoders and URL builders.

use mdc_core::model::VenueId;
use mdc_core::rest_quote::RestQuoteError;
use rust_decimal::Decimal;
use url::Url;

/// Safely appends a path and query string to a base `Url`.
pub fn build_url(
    provider: impl Into<VenueId>,
    base: &Url,
    path_and_query: &str,
) -> Result<Url, RestQuoteError> {
    let provider = provider.into();
    let base_trimmed = base.as_str().trim_end_matches('/');
    let clean_path = path_and_query.trim_start_matches('/');
    let full = format!("{base_trimmed}/{clean_path}");
    Url::parse(&full).map_err(|e| RestQuoteError::MalformedItem {
        provider,
        reason: format!("invalid target URL '{full}': {e}"),
    })
}

/// Parses a `serde_json::Value` directly into a `rust_decimal::Decimal` without floating point conversion.
#[allow(dead_code)]
pub fn json_value_to_decimal(val: &serde_json::Value) -> Option<Decimal> {
    match val {
        serde_json::Value::String(s) => Decimal::from_str_exact(s).ok(),
        serde_json::Value::Number(n) => Decimal::from_str_exact(&n.to_string()).ok(),
        _ => None,
    }
}
