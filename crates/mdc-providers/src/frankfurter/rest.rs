//! HTTP REST client and query builders for Frankfurter ECB reference rates.

use std::time::Duration;

use bytes::Bytes;
use mdc_core::{MdcError, UnixNanos, integrity::embargo::parse_retry_after};
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
    body_cap_bytes: usize,
}

impl FrankfurterRestClient {
    /// Creates a new `FrankfurterRestClient` from configuration.
    pub fn new(config: &FrankfurterConfig) -> Result<Self, MdcError> {
        let redirect_policy = reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().is_empty() {
                return attempt.follow();
            }
            let initial = attempt.previous()[0].origin();
            let current = attempt.url().origin();
            if initial == current {
                attempt.follow()
            } else {
                attempt.error("foreign-origin redirect disallowed")
            }
        });

        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .redirect(redirect_policy)
            .build()
            .map_err(|e| MdcError::Transport(Box::new(e)))?;

        Ok(Self {
            client,
            base_url: config.base_url.clone(),
            provider: config.provider.clone(),
            body_cap_bytes: config.body_cap_bytes,
        })
    }

    /// Creates a new `FrankfurterRestClient` with a caller-injected `reqwest::Client`.
    pub fn with_client(client: reqwest::Client, config: &FrankfurterConfig) -> Self {
        Self {
            client,
            base_url: config.base_url.clone(),
            provider: config.provider.clone(),
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

        let response = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|err| {
                if err.is_redirect() {
                    HttpPollError::ForeignRedirect
                } else {
                    HttpPollError::Transport
                }
            })?;

        let received_ts = UnixNanos::now();
        let status = response.status();

        if !status.is_success() {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| parse_retry_after(Some(s)));

            return Err(HttpPollError::HttpStatus {
                status: status.as_u16(),
                retry_after,
            });
        }

        // Check declared Content-Length header against body cap
        if let Some(content_len) = response.content_length() {
            if content_len > self.body_cap_bytes as u64 {
                return Err(HttpPollError::OversizedBody);
            }
        }

        let body = response
            .bytes()
            .await
            .map_err(|_| HttpPollError::Transport)?;

        if body.len() > self.body_cap_bytes {
            return Err(HttpPollError::OversizedBody);
        }

        Ok(PollResponse { received_ts, body })
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
