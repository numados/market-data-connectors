//! HTTP REST client and query builders for Frankfurter ECB reference rates.

use std::time::Duration;

use bytes::Bytes;
use mdc_core::{MdcError, UnixNanos};
use url::Url;

use crate::frankfurter::config::FrankfurterConfig;

/// Builds the complete request URL for the Frankfurter rates endpoint.
///
/// Example: `https://api.frankfurter.dev/v2/providers/ecb/rates?base=EUR&quotes=USD,GBP`
pub fn build_rates_url(
    base_url: &Url,
    provider: &str,
    base: &str,
    quotes: &[String],
) -> Result<Url, MdcError> {
    let mut url = base_url
        .join(&format!("/v2/providers/{provider}/rates"))
        .map_err(|e| MdcError::Protocol(format!("failed to construct rates URL: {e}")))?;

    let quotes_param = quotes.join(",");
    url.query_pairs_mut()
        .append_pair("base", base)
        .append_pair("quotes", &quotes_param);

    Ok(url)
}

/// Result of an HTTP fetch.
pub struct PollResponse {
    /// Ingestion wall-clock timestamp captured immediately upon response arrival.
    pub received_ts: UnixNanos,
    /// Response body bytes bounded by the configured body cap.
    pub body: Bytes,
}

/// Errors occurring during an HTTP poll attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum HttpPollError {
    /// Underlying transport or network failure.
    Transport,
    /// Non-2xx HTTP status code, with optional `Retry-After` duration.
    HttpStatus {
        status: u16,
        retry_after: Option<Duration>,
    },
    /// Response body exceeded configured body size cap.
    OversizedBody,
    /// Redirection to a foreign origin attempted and rejected.
    ForeignRedirect,
}

/// REST HTTP client executing rate requests against the Frankfurter API.
pub struct FrankfurterRestClient {
    client: reqwest::Client,
    base_url: Url,
    provider: String,
    timeout: Duration,
    body_cap_bytes: usize,
}

impl FrankfurterRestClient {
    /// Creates a new `FrankfurterRestClient` from configuration.
    pub fn new(config: &FrankfurterConfig) -> Result<Self, MdcError> {
        let client = crate::http::build_standard_http_client(config.request_timeout)
            .map_err(|e| MdcError::Transport(Box::new(e)))?;

        Ok(Self {
            client,
            base_url: config.base_url.clone(),
            provider: config.provider.clone(),
            timeout: config.request_timeout,
            body_cap_bytes: config.body_cap_bytes,
        })
    }

    /// Creates a new `FrankfurterRestClient` with a caller-injected `reqwest::Client`.
    pub fn with_client(client: reqwest::Client, config: &FrankfurterConfig) -> Self {
        Self {
            client,
            base_url: config.base_url.clone(),
            provider: config.provider.clone(),
            timeout: config.request_timeout,
            body_cap_bytes: config.body_cap_bytes,
        }
    }

    /// Fetches exchange rates for the given base and quotes.
    pub async fn fetch_rates(
        &self,
        base: &str,
        quotes: &[String],
    ) -> Result<PollResponse, HttpPollError> {
        let url = match build_rates_url(&self.base_url, &self.provider, base, quotes) {
            Ok(u) => u,
            Err(_) => return Err(HttpPollError::Transport),
        };

        match crate::http::execute_http_get(&self.client, url, self.timeout, self.body_cap_bytes)
            .await
        {
            Ok(succ) => Ok(PollResponse {
                received_ts: succ.received_ts,
                body: succ.body,
            }),
            Err(crate::http::HttpError::ForeignRedirect(_)) => Err(HttpPollError::ForeignRedirect),
            Err(crate::http::HttpError::RateLimit {
                status,
                retry_after,
            }) => Err(HttpPollError::HttpStatus {
                status,
                retry_after,
            }),
            Err(crate::http::HttpError::HttpStatus {
                status,
                retry_after,
                ..
            }) => Err(HttpPollError::HttpStatus {
                status,
                retry_after,
            }),
            Err(crate::http::HttpError::ResponseTooLarge { .. }) => {
                Err(HttpPollError::OversizedBody)
            }
            Err(_) => Err(HttpPollError::Transport),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_build_rates_url() {
        let base_url = Url::parse("https://api.frankfurter.dev").unwrap();
        let quotes = vec!["USD".to_string(), "GBP".to_string()];
        let url = build_rates_url(&base_url, "ecb", "EUR", &quotes).unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.frankfurter.dev/v2/providers/ecb/rates?base=EUR&quotes=USD%2CGBP"
        );
    }
}
