//! Generic HTTP client runtime for provider instrument catalog discovery.

use std::time::Duration;

use mdc_core::catalog::{
    BoxFuture, CatalogError, CatalogInstrument, CatalogRequest, CompleteCatalog, InstrumentCatalog,
    MarketFamily, ProviderKind,
};
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

        let client = crate::http::build_standard_http_client(Duration::from_secs(30))
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

            let fetch_success = crate::http::execute_http_get(
                &self.client,
                url,
                self.timeout,
                request.limits.max_response_bytes as usize,
            )
            .await
            .map_err(|e| e.into_catalog_error(self.provider))?;

            let payload_str =
                std::str::from_utf8(&fetch_success.body).map_err(|e| CatalogError::InvalidEnvelope {
                    provider: self.provider,
                    reason: format!("response body is not valid UTF-8: {e}"),
                })?;

            let instruments = (self.decoder)(payload_str, request.limits.max_items)?;

            Ok(CompleteCatalog {
                scope: request.scope.clone(),
                configuration_generation: request.configuration_generation,
                instruments,
                received_ts: fetch_success.received_ts,
            })
        })
    }
}
