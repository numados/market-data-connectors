//! Generic HTTP client runtime for provider REST quote snapshot discovery.

use std::time::Duration;

use mdc_core::ArcError;
use mdc_core::integrity::embargo::parse_retry_after;
use mdc_core::model::{Symbol, UnixNanos, VenueId};
use mdc_core::rest_quote::{
    BoxFuture, RestQuoteError, RestQuoteRequest, RestQuoteSnapshot, RestQuoteSource,
    SymbolQuoteResult,
};
use url::Url;

/// Function signature for pure provider REST quote payload decoders.
pub type RestQuoteDecoderFn =
    fn(VenueId, &[Symbol], &str, UnixNanos) -> Result<Vec<SymbolQuoteResult>, RestQuoteError>;

/// Function signature for constructing the provider-specific target URL.
pub type RestQuoteUrlBuilderFn = fn(&Url, &[Symbol]) -> Result<Url, RestQuoteError>;

/// Configuration and runtime state for an HTTP REST quote snapshot client.
#[derive(Clone)]
pub struct HttpRestQuoteClient {
    provider: VenueId,
    default_endpoint: Url,
    endpoint_override: Option<Url>,
    client: reqwest::Client,
    timeout: Duration,
    max_symbols_per_request: usize,
    url_builder: RestQuoteUrlBuilderFn,
    decoder: RestQuoteDecoderFn,
}

impl HttpRestQuoteClient {
    /// Creates a new `HttpRestQuoteClient` for a given venue with endpoint, URL builder, and decoder.
    pub fn new(
        provider: impl Into<VenueId>,
        default_endpoint: &str,
        max_symbols_per_request: usize,
        url_builder: RestQuoteUrlBuilderFn,
        decoder: RestQuoteDecoderFn,
    ) -> Result<Self, RestQuoteError> {
        let provider = provider.into();
        let default_url =
            Url::parse(default_endpoint).map_err(|e| RestQuoteError::Unsupported {
                provider,
                reason: format!("invalid default endpoint URL '{default_endpoint}': {e}"),
            })?;

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
            .redirect(redirect_policy)
            .build()
            .map_err(|e| RestQuoteError::Unsupported {
                provider,
                reason: format!("failed to build reqwest client: {e}"),
            })?;

        Ok(Self {
            provider,
            default_endpoint: default_url,
            endpoint_override: None,
            client,
            timeout: Duration::from_secs(30),
            max_symbols_per_request,
            url_builder,
            decoder,
        })
    }

    /// Overrides the endpoint URL (e.g. for loopback tests or custom API origins).
    pub fn with_endpoint(mut self, endpoint: &str) -> Result<Self, RestQuoteError> {
        let url = Url::parse(endpoint).map_err(|e| RestQuoteError::Unsupported {
            provider: self.provider,
            reason: format!("invalid endpoint override URL '{endpoint}': {e}"),
        })?;
        self.endpoint_override = Some(url);
        Ok(self)
    }

    /// Injects a caller-supplied `reqwest::Client`.
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Overrides the request timeout duration.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the target base URL currently configured for requests.
    pub fn target_url(&self) -> &Url {
        self.endpoint_override
            .as_ref()
            .unwrap_or(&self.default_endpoint)
    }

    /// Returns the provider kind.
    pub fn provider(&self) -> VenueId {
        self.provider
    }

    /// Returns the maximum allowed symbols per request.
    pub fn max_symbols_per_request(&self) -> usize {
        self.max_symbols_per_request
    }
}

impl RestQuoteSource for HttpRestQuoteClient {
    fn fetch<'a>(
        &'a self,
        request: &'a RestQuoteRequest,
    ) -> BoxFuture<'a, Result<RestQuoteSnapshot, RestQuoteError>> {
        Box::pin(async move {
            if request.symbols.is_empty() {
                return Ok(RestQuoteSnapshot {
                    configuration_generation: request.configuration_generation,
                    received_ts: crate::now_nanos(),
                    results: Vec::new(),
                });
            }

            if request.symbols.len() > self.max_symbols_per_request {
                return Err(RestQuoteError::BatchLimitExceeded {
                    provider: self.provider,
                    max: self.max_symbols_per_request,
                    actual: request.symbols.len(),
                });
            }

            let target_url = (self.url_builder)(self.target_url(), &request.symbols)?;

            let response_res = self
                .client
                .get(target_url)
                .header(reqwest::header::ACCEPT, "application/json")
                .timeout(self.timeout)
                .send()
                .await;

            let response = match response_res {
                Ok(resp) => resp,
                Err(err) => {
                    if err.is_timeout() {
                        return Err(RestQuoteError::Timeout {
                            provider: self.provider,
                        });
                    }
                    if err.is_redirect() {
                        return Err(RestQuoteError::Transport {
                            provider: self.provider,
                            message: "foreign-origin redirect disallowed".to_string(),
                            source: Some(ArcError::new(err)),
                        });
                    }
                    return Err(RestQuoteError::Transport {
                        provider: self.provider,
                        message: format!("network transport error: {err}"),
                        source: Some(ArcError::new(err)),
                    });
                }
            };

            let status = response.status();

            if status.as_u16() == 429 || status.as_u16() == 418 {
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| parse_retry_after(Some(s)));

                return Err(RestQuoteError::RateLimit {
                    provider: self.provider,
                    retry_after,
                });
            }

            if !status.is_success() {
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| parse_retry_after(Some(s)));

                let msg = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "failed to read error body".to_string());

                return Err(RestQuoteError::HttpStatus {
                    provider: self.provider,
                    status: status.as_u16(),
                    message: msg,
                    retry_after,
                    source: None,
                });
            }

            if let Some(content_len) = response.content_length() {
                if content_len > request.max_response_bytes as u64 {
                    return Err(RestQuoteError::ResponseTooLarge {
                        provider: self.provider,
                        max: request.max_response_bytes,
                        actual: content_len as usize,
                    });
                }
            }

            let body_bytes = match response.bytes().await {
                Ok(b) => b,
                Err(err) => {
                    if err.is_timeout() {
                        return Err(RestQuoteError::Timeout {
                            provider: self.provider,
                        });
                    }
                    return Err(RestQuoteError::Transport {
                        provider: self.provider,
                        message: format!("failed to read response body: {err}"),
                        source: Some(ArcError::new(err)),
                    });
                }
            };

            if body_bytes.len() > request.max_response_bytes as usize {
                return Err(RestQuoteError::ResponseTooLarge {
                    provider: self.provider,
                    max: request.max_response_bytes,
                    actual: body_bytes.len(),
                });
            }

            let received_ts = crate::now_nanos();

            let payload_str =
                std::str::from_utf8(&body_bytes).map_err(|e| RestQuoteError::InvalidEnvelope {
                    provider: self.provider,
                    reason: format!("response body is not valid UTF-8: {e}"),
                })?;

            let results =
                (self.decoder)(self.provider, &request.symbols, payload_str, received_ts)?;

            Ok(RestQuoteSnapshot {
                configuration_generation: request.configuration_generation,
                received_ts,
                results,
            })
        })
    }
}
