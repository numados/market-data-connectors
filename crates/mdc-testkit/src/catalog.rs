//! In-process mock REST server for testing provider instrument catalog discovery.

use mdc_core::catalog::ProviderKind;
use std::time::Duration;
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Returns the standard REST endpoint path for a provider's instrument catalog.
pub fn catalog_path_for_provider(provider: ProviderKind) -> &'static str {
    match provider {
        ProviderKind::Binance => "/api/v3/exchangeInfo",
        ProviderKind::Kraken => "/0/public/AssetPairs",
        ProviderKind::Coinbase => "/products",
        ProviderKind::Bitfinex => "/v2/conf/pub:list:pair:exchange",
        ProviderKind::Gateio => "/api/v4/spot/currency_pairs",
        ProviderKind::Bitstamp => "/api/v2/markets/",
        ProviderKind::Huobi => "/v1/common/symbols",
        _ => "/",
    }
}

/// In-process mock catalog server wrapping WireMock for testing `InstrumentCatalog` clients.
pub struct MockCatalogServer {
    server: MockServer,
}

impl MockCatalogServer {
    /// Starts a new mock catalog server on loopback (`127.0.0.1:0`).
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        Self { server }
    }

    /// Returns the base URI string.
    pub fn uri(&self) -> String {
        self.server.uri()
    }

    /// Returns the base URL parsed as a `Url`.
    pub fn url(&self) -> Result<Url, url::ParseError> {
        Url::parse(&self.server.uri())
    }

    /// Returns the full endpoint URL for a given provider on this mock server.
    pub fn endpoint_for(&self, provider: ProviderKind) -> String {
        format!(
            "{}{}",
            self.server.uri(),
            catalog_path_for_provider(provider)
        )
    }

    /// Returns a reference to the underlying `MockServer`.
    pub fn inner(&self) -> &MockServer {
        &self.server
    }

    /// Mocks a 200 OK catalog response for the given provider.
    pub async fn mock_catalog_ok(&self, provider: ProviderKind, body: &str) {
        let path_str = catalog_path_for_provider(provider);
        Mock::given(method("GET"))
            .and(path(path_str))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks an HTTP error status response (e.g. 429, 500, 404) with optional Retry-After header.
    pub async fn mock_catalog_status(
        &self,
        provider: ProviderKind,
        status: u16,
        retry_after: Option<u64>,
        body: &str,
    ) {
        let path_str = catalog_path_for_provider(provider);
        let mut resp =
            ResponseTemplate::new(status).set_body_raw(body.to_owned(), "application/json");
        if let Some(ra) = retry_after {
            resp = resp.insert_header("retry-after", ra.to_string());
        }
        Mock::given(method("GET"))
            .and(path(path_str))
            .respond_with(resp)
            .mount(&self.server)
            .await;
    }

    /// Mocks a delayed catalog response to trigger timeouts.
    pub async fn mock_catalog_delayed(&self, provider: ProviderKind, delay: Duration, body: &str) {
        let path_str = catalog_path_for_provider(provider);
        Mock::given(method("GET"))
            .and(path(path_str))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(delay)
                    .set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }
}
