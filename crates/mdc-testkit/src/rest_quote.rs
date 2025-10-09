//! In-process mock REST server for testing provider REST quote snapshot retrieval.

use std::time::Duration;
use url::Url;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// In-process mock REST quote server wrapping WireMock for testing `RestQuoteSource` implementations.
pub struct MockRestQuoteServer {
    server: MockServer,
}

impl MockRestQuoteServer {
    /// Starts a new mock REST quote server listening on loopback (`127.0.0.1:0`).
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        Self { server }
    }

    /// Returns the base URI string of the mock server.
    pub fn uri(&self) -> String {
        self.server.uri()
    }

    /// Returns the base URL parsed as a `Url`.
    pub fn url(&self) -> Result<Url, url::ParseError> {
        Url::parse(&self.server.uri())
    }

    /// Returns a reference to the underlying `MockServer`.
    pub fn inner(&self) -> &MockServer {
        &self.server
    }

    /// Mocks a Binance `bookTicker` 200 OK response for the given symbol.
    pub async fn mock_binance_quote_ok(&self, symbol: &str, body: &str) {
        Mock::given(method("GET"))
            .and(path("/api/v3/ticker/bookTicker"))
            .and(query_param("symbol", symbol))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks a Coinbase level 1 book 200 OK response for the given symbol.
    pub async fn mock_coinbase_quote_ok(&self, symbol: &str, body: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/products/{symbol}/book")))
            .and(query_param("level", "1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks a Kraken `Ticker` 200 OK response for the given symbol.
    pub async fn mock_kraken_quote_ok(&self, symbol: &str, body: &str) {
        Mock::given(method("GET"))
            .and(path("/0/public/Ticker"))
            .and(query_param("pair", symbol))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks a Bitstamp `ticker` 200 OK response for the given symbol.
    pub async fn mock_bitstamp_quote_ok(&self, symbol: &str, body: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/api/v2/ticker/{symbol}/")))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks a Gate.io `tickers` 200 OK response for the given symbol.
    pub async fn mock_gateio_quote_ok(&self, symbol: &str, body: &str) {
        Mock::given(method("GET"))
            .and(path("/api/v4/spot/tickers"))
            .and(query_param("currency_pair", symbol))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks a Huobi `merged` 200 OK response for the given symbol.
    pub async fn mock_huobi_quote_ok(&self, symbol: &str, body: &str) {
        Mock::given(method("GET"))
            .and(path("/market/detail/merged"))
            .and(query_param("symbol", symbol.to_lowercase()))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks a Bitfinex `tickers` 200 OK response for the given comma-separated symbols.
    pub async fn mock_bitfinex_quote_ok(&self, symbols: &str, body: &str) {
        Mock::given(method("GET"))
            .and(path("/v2/tickers"))
            .and(query_param("symbols", symbols))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks any incoming GET request with a 200 OK response containing the given body.
    pub async fn mock_any_ok(&self, body: &str) {
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks any incoming GET request with a specified HTTP status code, optional Retry-After header, and body.
    pub async fn mock_any_status(&self, status: u16, retry_after: Option<u64>, body: &str) {
        let mut resp =
            ResponseTemplate::new(status).set_body_raw(body.to_owned(), "application/json");
        if let Some(ra) = retry_after {
            resp = resp.insert_header("retry-after", ra.to_string());
        }
        Mock::given(method("GET"))
            .respond_with(resp)
            .mount(&self.server)
            .await;
    }

    /// Mocks a delayed response to trigger client-side timeout handling.
    pub async fn mock_delayed(&self, delay: Duration, body: &str) {
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(delay)
                    .set_body_raw(body.to_owned(), "application/json"),
            )
            .mount(&self.server)
            .await;
    }

    /// Mocks a response exceeding a given byte size.
    pub async fn mock_large_payload(&self, size_bytes: usize) {
        let padding = "a".repeat(size_bytes);
        let body = format!(r#"{{"data":"{padding}"}}"#);
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
            .mount(&self.server)
            .await;
    }

    /// Mocks a redirect response with the given status (e.g. 301, 302) and `Location` target header.
    pub async fn mock_redirect(&self, status: u16, location: &str) {
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(status).insert_header("location", location))
            .mount(&self.server)
            .await;
    }
}
