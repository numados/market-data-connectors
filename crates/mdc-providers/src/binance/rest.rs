//! Binance REST depth snapshot client with shared rate limit embargo protection.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mdc_core::{EmbargoTracker, SnapshotPermit, parse_retry_after};
use thiserror::Error;
use tracing::{debug, warn};
use url::Url;

use crate::binance::raw::RawDepthSnapshot;

/// Errors arising during REST depth snapshot retrieval.
#[derive(Debug, Clone, Error)]
pub enum RestError {
    /// Request blocked by active venue rate limit embargo.
    #[error("venue rate limit embargo active until {0:?}")]
    Embargoed(Instant),
    /// HTTP 429 or 418 rate limit response received.
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

/// Client for fetching order book depth snapshots from Binance REST API.
#[derive(Debug, Clone)]
pub struct BinanceRestClient {
    client: reqwest::Client,
    base_url: Url,
    embargo: Arc<Mutex<EmbargoTracker>>,
    timeout: Duration,
}

impl BinanceRestClient {
    /// Creates a new `BinanceRestClient` sharing the given embargo tracker.
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

    /// Fetches an authoritative order book depth snapshot (`limit=1000`) for `symbol`.
    pub async fn fetch_depth(
        &self,
        symbol: &str,
        now: Instant,
    ) -> Result<RawDepthSnapshot, RestError> {
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
        url.set_path("/api/v3/depth");
        url.query_pairs_mut()
            .append_pair("symbol", symbol)
            .append_pair("limit", "1000");

        let response = self
            .client
            .get(url)
            .timeout(self.timeout)
            .send()
            .await
            .map_err(|e| RestError::Transport(e.to_string()))?;

        let status = response.status().as_u16();

        // Check for rate limits
        if status == 429 || status == 418 {
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
                symbol,
                status,
                ?retry_after,
                "Binance REST snapshot rate limit hit"
            );
            return Err(RestError::RateLimited(status));
        }

        if status != 200 {
            return Err(RestError::HttpStatus(status));
        }

        if let Some(weight) = response.headers().get("x-mbx-used-weight-1m") {
            if let Ok(w) = weight.to_str() {
                debug!(symbol, weight = w, "Binance REST used weight");
            }
        }

        let bytes =
            crate::http::read_response_bytes_capped(response, crate::http::DEFAULT_REST_SNAPSHOT_CAP)
                .await
                .map_err(|e| RestError::Transport(e.to_string()))?;

        let snapshot: RawDepthSnapshot =
            serde_json::from_slice(&bytes).map_err(|e| RestError::Parse(e.to_string()))?;

        Ok(snapshot)
    }
}
