//! Mock REST server wrapping WireMock for exchange REST endpoint testing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use url::Url;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::barrier::BARRIER_TIMEOUT;

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

    /// Waits until the server has received at least `count` requests and
    /// returns the total received so far.
    ///
    /// Panics after [`BARRIER_TIMEOUT`] (reporting how many requests did arrive)
    /// instead of hanging the suite.
    pub async fn wait_for_requests(&self, count: usize) -> usize {
        self.wait_for_requests_within(count, BARRIER_TIMEOUT).await
    }

    /// [`Self::wait_for_requests`] with an explicit bound.
    pub async fn wait_for_requests_within(&self, count: usize, bound: Duration) -> usize {
        let received = AtomicUsize::new(0);
        let wait = async {
            loop {
                let seen = self
                    .server
                    .received_requests()
                    .await
                    .map_or(0, |requests| requests.len());
                received.store(seen, Ordering::Relaxed);
                if seen >= count {
                    return seen;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        match tokio::time::timeout(bound, wait).await {
            Ok(seen) => seen,
            Err(_) => panic!(
                "timed out after {bound:?} waiting for {count} REST request(s); received {}",
                received.load(Ordering::Relaxed)
            ),
        }
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
        self.mock_binance_depth_status_delayed(symbol, status, retry_after, Duration::ZERO)
            .await;
    }

    /// Like [`Self::mock_binance_depth_status`], but the server holds the response for `delay`,
    /// so the client observes the rate limit only after that much wall-clock time.
    pub async fn mock_binance_depth_status_delayed(
        &self,
        symbol: &str,
        status: u16,
        retry_after: Option<u64>,
        delay: Duration,
    ) {
        let mut response = ResponseTemplate::new(status)
            .set_body_string(r#"{"code":-1003,"msg":"Too much request rate."}"#)
            .insert_header("x-mbx-used-weight-1m", "6000")
            .set_delay(delay);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[should_panic(expected = "received 0")]
    async fn wait_for_requests_fails_instead_of_hanging() {
        let rest = MockRest::start().await;
        rest.wait_for_requests_within(1, Duration::from_millis(50))
            .await;
    }
}
