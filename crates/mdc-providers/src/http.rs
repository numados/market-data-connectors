//! Unified bounded HTTP transport core and streaming capped reader.
//!
//! Provides a single source of truth for:
//! - Streaming bounded response body consumption (`read_response_bytes_capped`).
//! - Capped error body extraction (`read_error_body_capped`).
//! - Foreign-origin redirect blocking.
//! - Unified HTTP error taxonomy mapping to `CatalogError` and `RestQuoteError`.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use mdc_core::ArcError;
use mdc_core::catalog::{CatalogError, ProviderKind};
use mdc_core::integrity::embargo::parse_retry_after;
use mdc_core::model::{UnixNanos, VenueId};
use mdc_core::rest_quote::RestQuoteError;
use url::Url;

/// Default maximum bytes to read for error responses (4 KiB).
pub const DEFAULT_ERROR_BODY_CAP: usize = 4096;

/// Default maximum bytes to read for REST snapshots (10 MiB).
pub const DEFAULT_REST_SNAPSHOT_CAP: usize = 10 * 1024 * 1024;

/// Result of a successful bounded HTTP fetch.
#[derive(Debug, Clone)]
pub struct HttpFetchSuccess {
    /// Ingestion wall-clock timestamp captured immediately upon response arrival.
    pub received_ts: UnixNanos,
    /// Bounded response body bytes.
    pub body: Bytes,
}

/// Error returned when reading a capped streaming HTTP response body.
#[derive(Debug, thiserror::Error)]
pub enum BodyReadError {
    /// The response body exceeded the configured byte limit.
    #[error("response body exceeded limit of {limit} bytes (received {actual} bytes)")]
    TooLarge {
        /// Configured upper limit in bytes.
        limit: usize,
        /// Actual bytes received before or upon termination.
        actual: usize,
    },
    /// The request timed out while streaming chunks.
    #[error("request timed out while streaming response body")]
    Timeout,
    /// Underlying transport or network error while streaming chunks.
    #[error("transport error while reading response body: {0}")]
    Transport(#[source] reqwest::Error),
}

/// Unified HTTP error taxonomy for REST operations.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// The request timed out.
    #[error("request timed out")]
    Timeout,
    /// Redirection to a foreign origin attempted and blocked.
    #[error("foreign-origin redirect disallowed")]
    ForeignRedirect(Option<reqwest::Error>),
    /// Exchange rate limit exceeded (HTTP 429 or 418).
    #[error("rate limited (HTTP {status})")]
    RateLimit {
        /// HTTP status code (429 or 418).
        status: u16,
        /// Optional retry-after cooldown period.
        retry_after: Option<Duration>,
    },
    /// Non-success HTTP status code returned.
    #[error("HTTP error {status}: {message}")]
    HttpStatus {
        /// HTTP status code.
        status: u16,
        /// Bounded error excerpt string.
        message: String,
        /// Optional retry-after cooldown period.
        retry_after: Option<Duration>,
    },
    /// The response body exceeded the configured byte limit.
    #[error("response too large: actual {actual} bytes exceeds limit of {max} bytes")]
    ResponseTooLarge {
        /// Configured upper limit in bytes.
        max: usize,
        /// Actual bytes read when threshold was reached.
        actual: usize,
    },
    /// Low-level transport or socket error.
    #[error("network transport error: {0}")]
    Transport(#[source] reqwest::Error),
}

impl HttpError {
    /// Converts this HTTP error into a canonical [`CatalogError`].
    pub fn into_catalog_error(self, provider: ProviderKind) -> CatalogError {
        match self {
            Self::Timeout => CatalogError::Timeout { provider },
            Self::ForeignRedirect(e) => CatalogError::Transport {
                provider,
                message: "foreign-origin redirect disallowed".to_string(),
                source: e.map(ArcError::new),
            },
            Self::RateLimit { retry_after, .. } => CatalogError::RateLimit {
                provider,
                retry_after,
            },
            Self::HttpStatus {
                status,
                message,
                retry_after,
            } => CatalogError::HttpStatus {
                provider,
                status,
                message,
                retry_after,
                source: None,
            },
            Self::ResponseTooLarge { max, actual } => CatalogError::ResponseTooLarge {
                provider,
                max: max.min(u32::MAX as usize) as u32,
                actual,
            },
            Self::Transport(e) => CatalogError::Transport {
                provider,
                message: format!("network transport error: {e}"),
                source: Some(ArcError::new(e)),
            },
        }
    }

    /// Converts this HTTP error into a canonical [`RestQuoteError`].
    pub fn into_rest_quote_error(self, provider: VenueId) -> RestQuoteError {
        match self {
            Self::Timeout => RestQuoteError::Timeout { provider },
            Self::ForeignRedirect(e) => RestQuoteError::Transport {
                provider,
                message: "foreign-origin redirect disallowed".to_string(),
                source: e.map(ArcError::new),
            },
            Self::RateLimit { retry_after, .. } => RestQuoteError::RateLimit {
                provider,
                retry_after,
            },
            Self::HttpStatus {
                status,
                message,
                retry_after,
            } => RestQuoteError::HttpStatus {
                provider,
                status,
                message,
                retry_after,
                source: None,
            },
            Self::ResponseTooLarge { max, actual } => RestQuoteError::ResponseTooLarge {
                provider,
                max: max.min(u32::MAX as usize) as u32,
                actual,
            },
            Self::Transport(e) => RestQuoteError::Transport {
                provider,
                message: format!("network transport error: {e}"),
                source: Some(ArcError::new(e)),
            },
        }
    }
}

/// Constructs a `reqwest::redirect::Policy` that follows same-origin redirects
/// and blocks foreign-origin redirects.
pub fn make_foreign_redirect_disallowed_policy() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
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
    })
}

/// Builds a standard `reqwest::Client` with foreign redirect protection and timeout.
pub fn build_standard_http_client(timeout: Duration) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .redirect(make_foreign_redirect_disallowed_policy())
        .timeout(timeout)
        .build()
}

/// Reads a streaming HTTP response body into memory, stopping as soon as `limit` bytes is exceeded.
///
/// If `limit` is exceeded, reading stops immediately without buffering additional chunks,
/// preventing unbounded memory consumption.
pub async fn read_response_bytes_capped(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Bytes, BodyReadError> {
    // 1. If declared Content-Length exceeds limit, fail immediately
    if let Some(content_len) = response.content_length() {
        if content_len > limit as u64 {
            return Err(BodyReadError::TooLarge {
                limit,
                actual: content_len as usize,
            });
        }
    }

    // 2. Stream chunks with running byte counter
    let initial_cap = response.content_length().unwrap_or(0).min(limit as u64) as usize;
    let mut buf = BytesMut::with_capacity(initial_cap);

    while let Some(chunk_res) = response.chunk().await.map_err(|e| {
        if e.is_timeout() {
            BodyReadError::Timeout
        } else {
            BodyReadError::Transport(e)
        }
    })? {
        if buf.len().saturating_add(chunk_res.len()) > limit {
            return Err(BodyReadError::TooLarge {
                limit,
                actual: buf.len().saturating_add(chunk_res.len()),
            });
        }
        buf.extend_from_slice(&chunk_res);
    }

    Ok(buf.freeze())
}

/// Reads an error response body capped to a safe excerpt length.
///
/// Returns `Ok(body_str)` on success or when truncated, or `Err(BodyReadError)`
/// if a transport error or timeout occurred during chunk streaming.
pub async fn read_error_body_capped(
    response: reqwest::Response,
    limit: usize,
) -> Result<String, BodyReadError> {
    match read_response_bytes_capped(response, limit).await {
        Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).into_owned()),
        Err(BodyReadError::TooLarge { limit, .. }) => {
            Ok(format!("<error body exceeded {limit} bytes limit>"))
        }
        Err(e @ (BodyReadError::Timeout | BodyReadError::Transport(_))) => Err(e),
    }
}

/// Executes an HTTP GET request with standard redirect protection, timeout, status inspection,
/// and streaming capped body reading.
pub async fn execute_http_get(
    client: &reqwest::Client,
    url: Url,
    timeout: Duration,
    max_response_bytes: usize,
) -> Result<HttpFetchSuccess, HttpError> {
    let response_res = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/json")
        .timeout(timeout)
        .send()
        .await;

    let response = match response_res {
        Ok(resp) => resp,
        Err(err) => {
            if err.is_timeout() {
                return Err(HttpError::Timeout);
            }
            if err.is_redirect() {
                return Err(HttpError::ForeignRedirect(Some(err)));
            }
            return Err(HttpError::Transport(err));
        }
    };

    let received_ts = crate::now_nanos();
    let status = response.status();

    if status.as_u16() == 429 || status.as_u16() == 418 {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| parse_retry_after(Some(s)));

        return Err(HttpError::RateLimit {
            status: status.as_u16(),
            retry_after,
        });
    }

    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| parse_retry_after(Some(s)));

        let message = match read_error_body_capped(response, DEFAULT_ERROR_BODY_CAP).await {
            Ok(msg) => msg,
            Err(BodyReadError::Timeout) => return Err(HttpError::Timeout),
            Err(BodyReadError::Transport(e)) => return Err(HttpError::Transport(e)),
            Err(BodyReadError::TooLarge { limit, .. }) => {
                format!("<error body exceeded {limit} bytes limit>")
            }
        };

        return Err(HttpError::HttpStatus {
            status: status.as_u16(),
            message,
            retry_after,
        });
    }

    let body = match read_response_bytes_capped(response, max_response_bytes).await {
        Ok(b) => b,
        Err(BodyReadError::TooLarge { limit, actual }) => {
            return Err(HttpError::ResponseTooLarge { max: limit, actual });
        }
        Err(BodyReadError::Timeout) => return Err(HttpError::Timeout),
        Err(BodyReadError::Transport(e)) => return Err(HttpError::Transport(e)),
    };

    Ok(HttpFetchSuccess { received_ts, body })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_read_response_bytes_capped_content_length_exceeded() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/big"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![0u8; 100]))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("{}/big", server.uri()))
            .send()
            .await
            .expect("send request");

        let err = read_response_bytes_capped(resp, 50)
            .await
            .expect_err("should fail when content-length > limit");

        match err {
            BodyReadError::TooLarge { limit, actual } => {
                assert_eq!(limit, 50);
                assert_eq!(actual, 100);
            }
            other => panic!("expected TooLarge error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_read_response_bytes_capped_streaming_exceeded() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let port = listener.local_addr().expect("local addr").port();

        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;
                // Send raw chunked response without Content-Length
                let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n40\r\n0123456789012345678901234567890123456789012345678901234567890123\r\n0\r\n\r\n";
                let _ = socket.write_all(resp).await;
            }
        });

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{port}/chunked"))
            .send()
            .await
            .expect("send request");

        let err = read_response_bytes_capped(resp, 50)
            .await
            .expect_err("should abort when streaming bytes > limit");

        match err {
            BodyReadError::TooLarge { limit, actual } => {
                assert_eq!(limit, 50);
                assert_eq!(actual, 64);
            }
            other => panic!("expected TooLarge error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_read_error_body_capped_truncation() {
        let server = MockServer::start().await;
        let large_error = "E".repeat(5000);
        Mock::given(method("GET"))
            .and(path("/err"))
            .respond_with(ResponseTemplate::new(500).set_body_string(large_error))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("{}/err", server.uri()))
            .send()
            .await
            .expect("send request");

        let err_text = read_error_body_capped(resp, 100)
            .await
            .expect("should read error body with truncation placeholder");
        assert!(err_text.contains("<error body exceeded 100 bytes limit>"));
    }

    #[tokio::test]
    async fn test_error_body_transport_failure_preserves_source() {
        use std::error::Error;
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let port = listener.local_addr().expect("local addr").port();

        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;
                // Send 500 status with Content-Length: 100, then abruptly close socket without body
                let resp =
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100\r\n\r\nincomplete";
                let _ = socket.write_all(resp).await;
                drop(socket);
            }
        });

        let client = reqwest::Client::new();
        let url = Url::parse(&format!("http://127.0.0.1:{port}/err_drop")).expect("parse url");
        let err = execute_http_get(&client, url, Duration::from_secs(2), 1024)
            .await
            .expect_err("should fail with transport error");

        match err {
            HttpError::Transport(ref source) => {
                assert!(source.source().is_some() || !source.to_string().is_empty());
                assert!(err.source().is_some());
            }
            other => panic!("expected HttpError::Transport, got {other:?}"),
        }
    }
}
