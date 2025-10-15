//! Bitstamp REST order book snapshot client with shared rate limit embargo protection.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mdc_core::{EmbargoTracker, SnapshotPermit, parse_retry_after};
use tracing::{debug, warn};
use url::Url;

use crate::bitstamp::raw::RawOrderBookSnapshot;

/// Errors arising during Bitstamp REST snapshot retrieval.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RestError {
    /// Request blocked by active venue rate limit embargo.
    #[error("venue rate limit embargo active until {0:?}")]
    Embargoed(Instant),
    /// HTTP 429 rate limit response received.
    #[error("rate limited with HTTP {0}")]
    RateLimited(u16),
    /// Non-success HTTP status code returned.
    #[error("HTTP error: {0}")]
    HttpStatus(u16),
    /// Transport or networking failure.
    #[error("transport error: {0}")]
    Transport(String),
    /// JSON parsing failure.
    #[error("parse error: {0}")]
    Parse(String),
}

/// Client for fetching order book snapshots from Bitstamp REST API.
#[derive(Debug, Clone)]
pub struct BitstampRestClient {
    client: reqwest::Client,
    base_url: Url,
    embargo: Arc<Mutex<EmbargoTracker>>,
    timeout: Duration,
}

impl BitstampRestClient {
    /// Creates a new `BitstampRestClient` sharing the given embargo tracker.
    pub fn new(
        base_url: Url,
        embargo: Arc<Mutex<EmbargoTracker>>,
        timeout: Duration,
    ) -> Result<Self, RestError> {
        let client = crate::http::build_standard_http_client(timeout)
            .map_err(|e| RestError::Transport(e.to_string()))?;

        Ok(Self {
            client,
            base_url,
            embargo,
            timeout,
        })
    }

    /// Fetches an authoritative order book snapshot (`/api/v2/order_book/{pair}/`) for `pair`.
    pub async fn fetch_order_book(
        &self,
        pair: &str,
        now: Instant,
    ) -> Result<RawOrderBookSnapshot, RestError> {
        // Check embargo before making the request
        {
            let tracker = self
                .embargo
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match tracker.permit(now) {
                SnapshotPermit::Allowed => {}
                SnapshotPermit::Embargoed { until } => {
                    return Err(RestError::Embargoed(until));
                }
            }
        }

        let mut url = self.base_url.clone();
        if !url.path().ends_with('/') {
            let p = format!("{}/", url.path());
            url.set_path(&p);
        }
        let path = format!("order_book/{pair}/");
        url = url
            .join(&path)
            .map_err(|e| RestError::Transport(e.to_string()))?;

        debug!(pair, url = %url, "Requesting Bitstamp REST order book snapshot");

        let response = self
            .client
            .get(url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| RestError::Transport(e.to_string()))?;

        let status = response.status().as_u16();

        // Check for rate limits (HTTP 429)
        if status == 429 {
            let retry_after_header = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok());
            let retry_after = parse_retry_after(retry_after_header);

            let mut tracker = self
                .embargo
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            tracker.on_rate_limited(status, retry_after, now);

            warn!(
                pair,
                status,
                ?retry_after,
                "Bitstamp REST snapshot rate limit hit (HTTP 429)"
            );
            return Err(RestError::RateLimited(status));
        }

        if status != 200 {
            return Err(RestError::HttpStatus(status));
        }

        let bytes = crate::http::read_response_bytes_capped(
            response,
            crate::http::DEFAULT_REST_SNAPSHOT_CAP,
        )
        .await
        .map_err(|e| RestError::Transport(e.to_string()))?;

        let snapshot: RawOrderBookSnapshot =
            serde_json::from_slice(&bytes).map_err(|e| RestError::Parse(e.to_string()))?;

        Ok(snapshot)
    }
}
