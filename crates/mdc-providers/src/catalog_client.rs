//! Generic HTTP client runtime for provider instrument catalog discovery.

use std::time::Duration;

use mdc_core::ArcError;
use mdc_core::catalog::{
    BoxFuture, CatalogError, CatalogInstrument, CatalogRequest, CompleteCatalog, InstrumentCatalog,
    MarketFamily, ProviderKind,
};
use mdc_core::integrity::embargo::parse_retry_after;
use url::Url;

/// Function signature for pure provider catalog payload decoders.
pub type CatalogDecoderFn = fn(&str, u32) -> Result<Vec<CatalogInstrument>, CatalogError>;

/// Configuration and runtime state for an HTTP instrument catalog client.
#[derive(Clone)]
pub struct HttpCatalogClient {
    provider: ProviderKind,
    default_endpoint: Url,
    endpoint_override: Option<Url>,
    client: reqwest::Client,
    timeout: Duration,
    decoder: CatalogDecoderFn,
}

impl HttpCatalogClient {
    /// Creates a new `HttpCatalogClient` for a given provider with its default endpoint and decoder.
    pub fn new(
        provider: ProviderKind,
        default_endpoint: &str,
        decoder: CatalogDecoderFn,
    ) -> Result<Self, CatalogError> {
        let default_url = Url::parse(default_endpoint).map_err(|e| CatalogError::Unsupported {
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
            .map_err(|e| CatalogError::Unsupported {
                provider,
                reason: format!("failed to build reqwest client: {e}"),
            })?;

        Ok(Self {
            provider,
            default_endpoint: default_url,
            endpoint_override: None,
            client,
            timeout: Duration::from_secs(30),
            decoder,
        })
    }

    /// Overrides the endpoint URL (e.g. for loopback tests or custom API origins).
    pub fn with_endpoint(mut self, endpoint: &str) -> Result<Self, CatalogError> {
        let url = Url::parse(endpoint).map_err(|e| CatalogError::Unsupported {
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

    /// Returns the target URL currently configured for requests.
    pub fn target_url(&self) -> &Url {
        self.endpoint_override
            .as_ref()
            .unwrap_or(&self.default_endpoint)
    }
}

impl InstrumentCatalog for HttpCatalogClient {
    fn fetch<'a>(
        &'a self,
        request: &'a CatalogRequest,
    ) -> BoxFuture<'a, Result<CompleteCatalog, CatalogError>> {
        Box::pin(async move {
            // Validate scope matches this provider
            if request.scope.provider != self.provider {
                return Err(CatalogError::ScopeMismatch {
                    provider: self.provider,
                    requested: Box::new(request.scope.clone()),
                    actual: Box::new(mdc_core::catalog::CatalogScope::spot(self.provider)),
                });
            }

            if request.scope.market != MarketFamily::Spot {
                return Err(CatalogError::UnsupportedScope {
                    provider: self.provider,
                    scope: Box::new(request.scope.clone()),
                });
            }

            let url = self.target_url().clone();

            let response_res = self
                .client
                .get(url)
                .header(reqwest::header::ACCEPT, "application/json")
                .timeout(self.timeout)
                .send()
                .await;

            let response = match response_res {
                Ok(resp) => resp,
                Err(err) => {
                    if err.is_timeout() {
                        return Err(CatalogError::Timeout {
                            provider: self.provider,
                        });
                    }
                    if err.is_redirect() {
                        return Err(CatalogError::Transport {
                            provider: self.provider,
                            message: "foreign-origin redirect disallowed".to_string(),
                            source: Some(ArcError::new(err)),
                        });
                    }
                    return Err(CatalogError::Transport {
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

                return Err(CatalogError::RateLimit {
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

                return Err(CatalogError::HttpStatus {
                    provider: self.provider,
                    status: status.as_u16(),
                    message: msg,
                    retry_after,
                    source: None,
                });
            }

            // Check Content-Length header against limit
            if let Some(content_len) = response.content_length() {
                if content_len > request.limits.max_response_bytes as u64 {
                    return Err(CatalogError::ResponseTooLarge {
                        provider: self.provider,
                        max: request.limits.max_response_bytes,
                        actual: content_len as usize,
                    });
                }
            }

            // Read response bytes while enforcing max response bytes
            let body_bytes = match response.bytes().await {
                Ok(b) => b,
                Err(err) => {
                    if err.is_timeout() {
                        return Err(CatalogError::Timeout {
                            provider: self.provider,
                        });
                    }
                    return Err(CatalogError::Transport {
                        provider: self.provider,
                        message: format!("failed to read response body: {err}"),
                        source: Some(ArcError::new(err)),
                    });
                }
            };

            if body_bytes.len() > request.limits.max_response_bytes as usize {
                return Err(CatalogError::ResponseTooLarge {
                    provider: self.provider,
                    max: request.limits.max_response_bytes,
                    actual: body_bytes.len(),
                });
            }

            let received_ts = crate::now_nanos();

            let payload_str =
                std::str::from_utf8(&body_bytes).map_err(|e| CatalogError::InvalidEnvelope {
                    provider: self.provider,
                    reason: format!("response body is not valid UTF-8: {e}"),
                })?;

            let instruments = (self.decoder)(payload_str, request.limits.max_items)?;

            Ok(CompleteCatalog {
                scope: request.scope.clone(),
                configuration_generation: request.configuration_generation,
                instruments,
                received_ts,
            })
        })
    }
}
