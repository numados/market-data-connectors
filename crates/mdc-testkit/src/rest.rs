//! Mock REST server wrapping WireMock for exchange REST endpoint testing.

use url::Url;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Mock REST server for exchange snapshot and metadata testing.
pub struct MockRest {
    server: MockServer,
}

impl MockRest {
    /// Starts a new in-process mock REST server listening on `127.0.0.1:0`.
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        Self { server }
    }

    /// Returns the base URI string of the mock server.
    pub fn uri(&self) -> String {
        self.server.uri()
    }

    /// Returns the base URL of the mock server.
    pub fn url(&self) -> Result<Url, url::ParseError> {
        Url::parse(&self.server.uri())
    }

    /// Returns a reference to the underlying `MockServer`.
    pub fn inner(&self) -> &MockServer {
        &self.server
    }

    /// Mocks a Binance depth snapshot endpoint returning 200 OK with the given body.
    pub async fn mock_binance_depth_ok(&self, symbol: &str, body: &str) {
        Mock::given(method("GET"))
            .and(path("/api/v3/depth"))
            .and(query_param("symbol", symbol))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(body.to_owned(), "application/json")
                    .insert_header("x-mbx-used-weight-1m", "50"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks a Binance depth snapshot endpoint returning an HTTP error status code with optional Retry-After header.
    pub async fn mock_binance_depth_status(
        &self,
        symbol: &str,
        status: u16,
        retry_after: Option<u64>,
    ) {
        let mut response = ResponseTemplate::new(status)
            .set_body_string(r#"{"code":-1003,"msg":"Too much request rate."}"#)
            .insert_header("x-mbx-used-weight-1m", "6000");

        if let Some(ra) = retry_after {
            response = response.insert_header("retry-after", ra.to_string());
        }

        Mock::given(method("GET"))
            .and(path("/api/v3/depth"))
            .and(query_param("symbol", symbol))
            .respond_with(response)
            .mount(&self.server)
            .await;
    }
}
